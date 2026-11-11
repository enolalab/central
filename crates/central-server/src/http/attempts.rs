//! Attempt reconciliation: `POST /v1/runs/{id}/attempts/{attempt_id}/reconcile`.
//!
//! A passive evidence read: the gateway reports what it knows, Central
//! stores that as a durable observation and returns it. It never starts
//! inference, resends, or rewrites the attempt status — an unknown outcome
//! stays unknown until the user queues a new attempt with a handover.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use central_application::Runtime;
use central_domain::{AttemptId, RunId};

use super::auth::{app_error_response, authenticate};
use super::AppState;
use crate::api::errors::{error_response, ApiError};

fn runtime_for(state: &AppState, run_id: RunId) -> Runtime {
    Runtime::new(
        Arc::clone(&state.store),
        Arc::clone(&state.clock),
        Arc::clone(&state.ids),
        Arc::clone(&state.agent),
        run_id,
    )
}

pub async fn post_reconcile_attempt(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((raw_id, raw_attempt)): Path<(String, String)>,
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
    let attempt_id = raw_attempt.parse::<AttemptId>().map_err(|_| {
        error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            format!("invalid attempt id: {raw_attempt}"),
        )
    })?;
    let evidence = runtime_for(&state, run_id)
        .reconcile_attempt(auth.actor, attempt_id)
        .await
        .map_err(app_error_response)?;
    let evidence = serde_json::to_value(&evidence).map_err(|error| {
        error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            format!("cannot encode reconcile evidence: {error}"),
        )
    })?;
    Ok(Json(serde_json::json!({
        "run_id": run_id.to_string(),
        "evidence": evidence,
    })))
}
