//! Inbox HTTP surface: `GET /v1/runs/{id}/inbox` plus answer, approve,
//! decline and reconcile endpoints.
//!
//! Plan approval never maps to tool approval here: these endpoints only
//! accept canonical inbox keys (`provider:epoch:thread:turn:item:request`)
//! plus the preview hash the user saw. A plan hash is not a key and fails
//! as unknown. Secret answers travel the memory-only vault path — the
//! durable command, receipt and outbox intent carry the random reference
//! only, never the credential. Entries project redacted: secret previews
//! are withheld at the source. Writes need workflow rights (owner or
//! control); agent bridges use the results endpoints, never inbox.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use central_application::contracts::SessionRef;
use central_application::Runtime;
use central_domain::{ContentHash, InboxKey, RunId};
use serde::Deserialize;

use super::auth::{app_error_response, authenticate};
use super::AppState;
use crate::api::errors::{error_response, ApiError};
use crate::api::read_models::project_inbox_entry;

fn runtime_for(state: &AppState, run_id: RunId) -> Runtime {
    Runtime::new(
        Arc::clone(&state.store),
        Arc::clone(&state.clock),
        Arc::clone(&state.ids),
        Arc::clone(&state.agent),
        run_id,
    )
    .with_secrets(Arc::clone(&state.secrets))
}

/// Parse a canonical inbox key: `provider:epoch:thread:turn:item:request`.
/// Thread and epoch ids never contain `:`, so a six-part split is exact.
/// The turn segment may be empty: harness requests without turn scope
/// (opencode permissions) canonicalize with an empty turn id, and
/// refusing them here would block every such approval at the public
/// surface while the gateway still holds the live wire route.
fn parse_key(raw: &str) -> Result<InboxKey, String> {
    let parts: Vec<&str> = raw.split(':').collect();
    if parts.len() != 6
        || parts
            .iter()
            .enumerate()
            .any(|(index, part)| part.is_empty() && index != 3)
    {
        return Err(format!(
            "invalid inbox key {raw:?}; expected provider:epoch:thread:turn:item:request"
        ));
    }
    Ok(InboxKey {
        provider: parts[0].to_string(),
        epoch: parts[1].to_string(),
        thread_id: parts[2].to_string(),
        turn_id: parts[3].to_string(),
        item_id: parts[4].to_string(),
        request_id: parts[5].to_string(),
    })
}

fn parse_hash(raw: &str) -> Result<ContentHash, String> {
    if raw.trim().is_empty() {
        return Err("preview hash must not be empty".to_string());
    }
    Ok(ContentHash::from_hex(raw))
}

fn receipt_json(
    receipt: &central_application::contracts::Receipt,
) -> Result<serde_json::Value, ApiError> {
    serde_json::to_value(receipt).map_err(|error| {
        error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            format!("cannot encode receipt: {error}"),
        )
    })
}

pub async fn get_inbox(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(raw_id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authenticate(&state, &headers).await?;
    let run_id = raw_id.parse::<RunId>().map_err(|_| {
        error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            format!("invalid run id: {raw_id}"),
        )
    })?;
    let run_state = state
        .store
        .load(run_id)
        .await
        .map_err(app_error_response)?
        .ok_or_else(|| {
            error_response(
                StatusCode::NOT_FOUND,
                "not_found",
                format!("run {run_id} not found"),
            )
        })?;
    let mut entries: Vec<serde_json::Value> =
        run_state.inbox.values().map(project_inbox_entry).collect();
    entries.sort_by(|left, right| left["key"].as_str().cmp(&right["key"].as_str()));
    Ok(Json(serde_json::json!({
        "run_id": run_id.to_string(),
        "revision": run_state.revision.0,
        "control": format!("{:?}", run_state.control),
        "entries": entries,
    })))
}

#[derive(Deserialize)]
pub struct KeyBody {
    pub key: String,
    pub preview_hash: String,
}

#[derive(Deserialize)]
pub struct DeclineBody {
    pub key: String,
    pub preview_hash: String,
    pub reason: Option<String>,
}

#[derive(Deserialize)]
pub struct AnswerBody {
    pub key: String,
    pub preview_hash: String,
    pub message: Option<String>,
    /// Secret answer plaintext. Vaulted memory-only; never persisted.
    pub secret: Option<String>,
}

#[derive(Deserialize)]
pub struct ReconcileBody {
    pub session: SessionRef,
}

async fn current_revision(
    state: &AppState,
    run_id: RunId,
) -> Result<central_domain::Revision, ApiError> {
    let run_state = state
        .store
        .load(run_id)
        .await
        .map_err(app_error_response)?
        .ok_or_else(|| {
            error_response(
                StatusCode::NOT_FOUND,
                "not_found",
                format!("run {run_id} not found"),
            )
        })?;
    Ok(run_state.revision)
}

pub async fn post_approve(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(raw_id): Path<String>,
    Json(body): Json<KeyBody>,
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
    let key = parse_key(&body.key)
        .map_err(|message| error_response(StatusCode::BAD_REQUEST, "validation_failed", message))?;
    let preview_hash = parse_hash(&body.preview_hash)
        .map_err(|message| error_response(StatusCode::BAD_REQUEST, "validation_failed", message))?;
    let revision = current_revision(&state, run_id).await?;
    let envelope = central_domain::CommandEnvelope {
        id: state.ids.next_command_id(),
        expected_revision: Some(revision),
        command: central_domain::Command::ApproveAction { key, preview_hash },
    };
    let receipt = runtime_for(&state, run_id)
        .execute(auth.actor, envelope)
        .await
        .map_err(app_error_response)?;
    state.scan_notify.notify_one();
    receipt_json(&receipt).map(Json)
}

pub async fn post_decline(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(raw_id): Path<String>,
    Json(body): Json<DeclineBody>,
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
    let key = parse_key(&body.key)
        .map_err(|message| error_response(StatusCode::BAD_REQUEST, "validation_failed", message))?;
    let preview_hash = parse_hash(&body.preview_hash)
        .map_err(|message| error_response(StatusCode::BAD_REQUEST, "validation_failed", message))?;
    let reason = body.reason.unwrap_or_default();
    if reason.trim().is_empty() {
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            "a decline needs a reason for the harness",
        ));
    }
    let revision = current_revision(&state, run_id).await?;
    let envelope = central_domain::CommandEnvelope {
        id: state.ids.next_command_id(),
        expected_revision: Some(revision),
        command: central_domain::Command::DeclineAction {
            key,
            preview_hash,
            reason,
        },
    };
    let receipt = runtime_for(&state, run_id)
        .execute(auth.actor, envelope)
        .await
        .map_err(app_error_response)?;
    state.scan_notify.notify_one();
    receipt_json(&receipt).map(Json)
}

pub async fn post_answer(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(raw_id): Path<String>,
    Json(body): Json<AnswerBody>,
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
    let key = parse_key(&body.key)
        .map_err(|message| error_response(StatusCode::BAD_REQUEST, "validation_failed", message))?;
    let preview_hash = parse_hash(&body.preview_hash)
        .map_err(|message| error_response(StatusCode::BAD_REQUEST, "validation_failed", message))?;
    let has_message = body
        .message
        .as_ref()
        .is_some_and(|text| !text.trim().is_empty());
    let has_secret = body.secret.as_ref().is_some_and(|text| !text.is_empty());
    if has_message == has_secret {
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            "answer needs exactly one of message or secret",
        ));
    }
    let runtime = runtime_for(&state, run_id);
    let receipt = if let Some(secret) = body.secret {
        if secret.is_empty() {
            return Err(error_response(
                StatusCode::BAD_REQUEST,
                "validation_failed",
                "secret answer must not be empty",
            ));
        }
        // Memory-only vault path: the durable intent carries the random
        // reference only. The vault lives in AppState, shared with dispatch.
        runtime
            .answer_secret(auth.actor, key, preview_hash, secret)
            .await
            .map_err(app_error_response)?
    } else {
        let message = body.message.unwrap_or_default();
        let revision = current_revision(&state, run_id).await?;
        let envelope = central_domain::CommandEnvelope {
            id: state.ids.next_command_id(),
            expected_revision: Some(revision),
            command: central_domain::Command::AnswerQuestion {
                key,
                preview_hash,
                payload: central_domain::AnswerPayload::Plain(message),
            },
        };
        runtime
            .execute(auth.actor, envelope)
            .await
            .map_err(app_error_response)?
    };
    state.scan_notify.notify_one();
    receipt_json(&receipt).map(Json)
}

pub async fn post_reconcile(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(raw_id): Path<String>,
    Json(body): Json<ReconcileBody>,
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
    let summary = runtime_for(&state, run_id)
        .reconcile_inbox(&auth.actor, &body.session)
        .await
        .map_err(app_error_response)?;
    state.scan_notify.notify_one();
    let summary = serde_json::to_value(&summary).map_err(|error| {
        error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            format!("cannot encode reconcile summary: {error}"),
        )
    })?;
    Ok(Json(serde_json::json!({
        "run_id": run_id.to_string(),
        "reconcile": summary,
    })))
}

#[cfg(test)]
mod tests {
    use super::parse_key;

    #[test]
    fn opencode_permission_keys_parse_with_empty_turn() {
        let key = parse_key("opencode:epoch-1:ses_live::per_20e3d3f84001cUaO8tLSu0AuGP:permission")
            .expect("turn-less permission keys are valid");
        assert_eq!(key.provider, "opencode");
        assert_eq!(key.turn_id, "");
        assert_eq!(key.item_id, "per_20e3d3f84001cUaO8tLSu0AuGP");
        assert_eq!(
            key.canonical(),
            "opencode:epoch-1:ses_live::per_20e3d3f84001cUaO8tLSu0AuGP:permission"
        );
    }

    #[test]
    fn malformed_keys_stay_rejected() {
        for raw in [
            "opencode:epoch-1:ses_live:turn-1:per_1",
            "opencode:epoch-1:ses_live:turn-1:per_1:permission:extra",
            ":epoch-1:ses_live:turn-1:per_1:permission",
            "opencode::ses_live:turn-1:per_1:permission",
        ] {
            assert!(parse_key(raw).is_err(), "rejected: {raw}");
        }
    }
}
