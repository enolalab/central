//! `POST /v1/commands` — the only generic write path. The body is a
//! bare `CommandEnvelope` (which carries no run id by contract), so the
//! target run travels as the `?run=<uuid>` query parameter; a
//! `CreateRun` command needs no target and the daemon allocates the
//! run id.
//!
//! Public allowlist: scheduler-internal and harness-internal commands
//! (`QueueAttempt`, `StartDispatch`, `AttemptRunning/Unknown`,
//! `RequeueAttempt`, `AttemptFailed`, `ReopenTask`, `DeferRetry`,
//! `NoteFallback`, `RecordExternalRequest`, `RecordObservation`) are
//! never operator rights — the public gateway refuses them with 403
//! even for the owner. They run inside the daemon (scheduler, scan,
//! gateway observation) or through specialized endpoints (`results`,
//! `handover`) that seal and validate first. `RecordWorkerResult` is
//! the one durable harness-adjacent command: owner imports and the
//! attempt-bound bridge may file it, and it parks for verification,
// never passes over the generic gateway.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use central_application::ports::{Clock, IdSource};
use central_application::Runtime;
use central_domain::{Command, CommandEnvelope, RunId};

use super::auth::{app_error_response, authenticate, Auth, AuthScope};
use super::AppState;
use crate::api::errors::{error_response, ApiError};

fn runtime_for(state: &AppState, run_id: RunId) -> Runtime {
    Runtime::new(
        Arc::clone(&state.store),
        Arc::clone(&state.clock) as Arc<dyn Clock>,
        Arc::clone(&state.ids) as Arc<dyn IdSource>,
        Arc::clone(&state.agent),
        run_id,
    )
    // Shares the daemon vault so secret answers dispatch from memory.
    .with_secrets(Arc::clone(&state.secrets))
}

/// Whether `auth` may issue `command` over the public gateway.
/// System/agent commands are not operator rights: 403 for everyone,
/// including the owner. A bridge token may only file its own
/// attempt's worker result.
fn authorize_command(
    auth: &Auth,
    envelope_run: Option<RunId>,
    command: &Command,
) -> Result<(), ApiError> {
    let forbidden_system = || {
        error_response(
            StatusCode::FORBIDDEN,
            "forbidden",
            "scheduler-internal commands are not public operator rights",
        )
    };
    match command {
        Command::QueueAttempt { .. }
        | Command::StartDispatch { .. }
        | Command::AttemptRunning { .. }
        | Command::AttemptUnknown { .. }
        | Command::RequeueAttempt { .. }
        | Command::AttemptFailed { .. }
        | Command::ReopenTask { .. }
        | Command::DeferRetry { .. }
        | Command::NoteFallback { .. }
        | Command::RecordExternalRequest { .. }
        | Command::RecordObservation { .. } => Err(forbidden_system()),
        Command::RecordWorkerResult { attempt, .. } => match &auth.scope {
            AuthScope::Owner | AuthScope::Control => Ok(()),
            AuthScope::Bridge { attempt: bound, .. } if bound == attempt => Ok(()),
            AuthScope::Bridge { .. } => Err(error_response(
                StatusCode::FORBIDDEN,
                "forbidden",
                "bridge tokens file results only for their bound attempt",
            )),
            AuthScope::Read => Err(error_response(
                StatusCode::FORBIDDEN,
                "forbidden",
                "read tokens cannot write",
            )),
        },
        _ => {
            // Every other (user-level) command needs workflow rights;
            // read and bridge scopes never pass here.
            let _ = envelope_run;
            auth.require_control()
        }
    }
}

pub async fn post_command(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
    Json(envelope): Json<CommandEnvelope>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let auth = authenticate(&state, &headers).await?;
    let is_create = matches!(envelope.command, Command::CreateRun { .. });
    let run_id = if is_create {
        state.ids.next_run_id()
    } else {
        let raw = query.get("run").ok_or_else(|| {
            error_response(
                StatusCode::BAD_REQUEST,
                "validation_failed",
                "non-create commands require ?run=<run-id>",
            )
        })?;
        raw.parse::<RunId>().map_err(|_| {
            error_response(
                StatusCode::BAD_REQUEST,
                "validation_failed",
                format!("invalid run id: {raw}"),
            )
        })?
    };
    authorize_command(&auth, Some(run_id), &envelope.command)?;

    let runtime = runtime_for(&state, run_id);
    let receipt = runtime
        .execute(auth.actor.clone(), envelope.clone())
        .await
        .map_err(app_error_response)?;

    // Dispatch is a separate step after the atomic commit (execute
    // itself never dispatches). Resume reopens the restart barrier and
    // Stop drains its cancel intents; every other command leaves
    // dispatch to the explicit dispatch endpoint. dispatch_once keeps
    // its own gate, so this can never send for a parked run.
    let dispatch = if matches!(envelope.command, Command::Resume | Command::Stop) {
        // The command already committed, so a dispatch failure cannot
        // roll it back or turn the receipt into a non-2xx lie. It is
        // reported in the body's `dispatch` field (status "error")
        // alongside the truthful receipt.
        Some(dispatch_field(runtime.dispatch_once(run_id).await))
    } else {
        None
    };

    let receipt = serde_json::to_value(&receipt).map_err(|error| {
        error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            format!("cannot encode receipt: {error}"),
        )
    })?;
    // Wake the daemon scan loop: new state may have made work eligible.
    // The loop still enforces its per-run cadence via should_tick.
    state.scan_notify.notify_one();
    Ok(Json(attach_dispatch(receipt, dispatch)))
}

/// The caller-visible outcome of the post-commit dispatch step.
fn dispatch_field(
    result: Result<
        central_application::contracts::DispatchSummary,
        central_application::contracts::AppError,
    >,
) -> serde_json::Value {
    match result {
        Ok(summary) => serde_json::json!({
            "status": "ok",
            "dispatched": summary.dispatched,
            "unknown": summary.unknown,
            "still_pending": summary.still_pending,
        }),
        Err(error) => serde_json::json!({
            "status": "error",
            "error": error.to_string(),
        }),
    }
}

/// Merge the dispatch outcome into the receipt object, keeping every
/// committed receipt field intact.
fn attach_dispatch(
    mut receipt: serde_json::Value,
    dispatch: Option<serde_json::Value>,
) -> serde_json::Value {
    if let (Some(dispatch), Some(object)) = (dispatch, receipt.as_object_mut()) {
        object.insert("dispatch".to_string(), dispatch);
    }
    receipt
}

pub async fn dispatch(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Path(raw_id): axum::extract::Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let auth = authenticate(&state, &headers).await?;
    auth.require_control()?;
    let run_id = raw_id.parse::<RunId>().map_err(|_| {
        error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            format!("invalid run id: {raw_id}"),
        )
    })?;
    let runtime = runtime_for(&state, run_id);
    let summary = runtime
        .dispatch_once(run_id)
        .await
        .map_err(app_error_response)?;
    state.scan_notify.notify_one();
    Ok(Json(serde_json::json!({
        "run_id": run_id.to_string(),
        "dispatched": summary.dispatched,
        "unknown": summary.unknown,
        "still_pending": summary.still_pending,
    })))
}

pub async fn shutdown(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    let auth = authenticate(&state, &headers).await?;
    auth.require_owner()?;
    if let Ok(mut slot) = state.shutdown.lock() {
        if let Some(sender) = slot.take() {
            let _ = sender.send(());
        }
    }
    Ok(Json(serde_json::json!({ "status": "shutting down" })))
}

#[cfg(test)]
mod tests {
    use central_application::contracts::{AppError, DispatchSummary};

    use super::{attach_dispatch, dispatch_field};

    #[test]
    fn dispatch_failure_is_visible_in_response_field() {
        let field = dispatch_field(Err(AppError::Storage("store exploded".to_string())));
        assert_eq!(field["status"], "error");
        assert!(
            field["error"]
                .as_str()
                .expect("error text")
                .contains("store exploded"),
            "dispatch error must be visible to the caller: {field}"
        );
        // The committed receipt is preserved next to the dispatch field.
        let receipt = serde_json::json!({"command_id": "c1", "revision": 3});
        let merged = attach_dispatch(receipt, Some(field));
        assert_eq!(merged["command_id"], "c1");
        assert_eq!(merged["dispatch"]["status"], "error");
    }

    #[test]
    fn dispatch_success_is_visible_in_response_field() {
        let field = dispatch_field(Ok(DispatchSummary {
            dispatched: 2,
            unknown: 1,
            still_pending: 0,
        }));
        assert_eq!(field["status"], "ok");
        assert_eq!(field["dispatched"], 2);
        assert_eq!(field["unknown"], 1);
    }
}
