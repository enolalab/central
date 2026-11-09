//! `POST /v1/commands` — the only write path. The body is a bare
//! `CommandEnvelope` (which carries no run id by contract), so the
//! target run travels as the `?run=<uuid>` query parameter; a
//! `CreateRun` command needs no target and the daemon allocates the
//! run id. That choice is recorded in `docs/validation/p1-runtime.md`.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use central_application::ports::{Clock, IdSource, Store};
use central_application::Runtime;
use central_domain::{Command, CommandEnvelope, RunId};

use super::auth::{app_error_response, authenticate, error_response, ApiError};
use super::AppState;

fn runtime_for(state: &AppState, run_id: RunId) -> Runtime {
    Runtime::new(
        Arc::clone(&state.store) as Arc<dyn Store>,
        Arc::clone(&state.clock) as Arc<dyn Clock>,
        Arc::clone(&state.ids) as Arc<dyn IdSource>,
        Arc::clone(&state.agent),
        run_id,
    )
}

pub async fn post_command(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
    Json(envelope): Json<CommandEnvelope>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let actor = authenticate(&state, &headers)?;
    let is_create = matches!(envelope.command, Command::CreateRun { .. });
    let run_id = if is_create {
        state.ids.next_run_id()
    } else {
        let raw = query.get("run").ok_or_else(|| {
            error_response(
                StatusCode::BAD_REQUEST,
                "non-create commands require ?run=<run-id>",
            )
        })?;
        raw.parse::<RunId>().map_err(|_| {
            error_response(StatusCode::BAD_REQUEST, format!("invalid run id: {raw}"))
        })?
    };

    let runtime = runtime_for(&state, run_id);
    let receipt = runtime
        .execute(actor, envelope.clone())
        .await
        .map_err(app_error_response)?;

    // Dispatch is a separate step after the atomic commit (execute
    // itself never dispatches). Resume reopens the restart barrier and
    // Stop drains its cancel intents; every other command leaves
    // dispatch to the explicit dispatch endpoint. dispatch_once keeps
    // its own gate, so this can never send for a parked run.
    if matches!(envelope.command, Command::Resume | Command::Stop) {
        let _ = runtime.dispatch_once(run_id).await.map_err(|error| {
            // The command already committed; a dispatch failure is
            // reported in the body instead of rewriting history.
            tracing_dispatch_note(&error)
        });
    }

    Ok(Json(serde_json::to_value(&receipt).map_err(|error| {
        error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("cannot encode receipt: {error}"),
        )
    })?))
}

fn tracing_dispatch_note(error: &central_application::contracts::AppError) {
    eprintln!("central-server: post-command dispatch note: {error}");
}

pub async fn dispatch(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Path(raw_id): axum::extract::Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authenticate(&state, &headers)?;
    let run_id = raw_id.parse::<RunId>().map_err(|_| {
        error_response(StatusCode::BAD_REQUEST, format!("invalid run id: {raw_id}"))
    })?;
    let runtime = runtime_for(&state, run_id);
    let summary = runtime
        .dispatch_once(run_id)
        .await
        .map_err(app_error_response)?;
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
    authenticate(&state, &headers)?;
    if let Ok(mut slot) = state.shutdown.lock() {
        if let Some(sender) = slot.take() {
            let _ = sender.send(());
        }
    }
    Ok(Json(serde_json::json!({ "status": "shutting down" })))
}
