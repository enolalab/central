//! Leader reviews: `POST /v1/runs/{id}/leader/review`.
//!
//! An explicit operator request for the leader to review a task. The
//! daemon queues one leader attempt bound to the run's leader profile;
//! the scan loop dispatches it through the gateway like any other turn,
//! with the same approval/cancel/reconcile machinery. The task keeps
//! its status: the review advises, it never verifies or seals, and its
//! output returns through handover import, never through worker-result
//! import. Owner or control only.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use central_application::Runtime;
use central_domain::{AttemptId, RunId, TaskId};
use serde::Deserialize;

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

/// The leader review body: which task to review and what to ask.
#[derive(Deserialize)]
struct LeaderReviewBody {
    task: String,
    objective: String,
}

pub async fn post_leader_review(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(raw_id): Path<String>,
    body: String,
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
    let submitted: LeaderReviewBody = serde_json::from_str(&body).map_err(|error| {
        error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            format!("invalid leader review: {error}"),
        )
    })?;
    let task = submitted.task.parse::<TaskId>().map_err(|_| {
        error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            format!("invalid task id: {}", submitted.task),
        )
    })?;
    let (receipt, attempt) = runtime_for(&state, run_id)
        .queue_leader_review(auth.actor, task, submitted.objective.trim())
        .await
        .map_err(app_error_response)?;
    state.scan_notify.notify_one();
    let receipt = serde_json::to_value(&receipt).map_err(|error| {
        error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            format!("cannot encode receipt: {error}"),
        )
    })?;
    Ok(Json(serde_json::json!({
        "run_id": run_id.to_string(),
        "receipt": receipt,
        "attempt_id": attempt.to_string(),
    })))
}

/// The finish body: which review attempt to close.
#[derive(Deserialize)]
struct LeaderFinishBody {
    attempt: String,
}

pub async fn post_leader_finish(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(raw_id): Path<String>,
    body: String,
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
    let submitted: LeaderFinishBody = serde_json::from_str(&body).map_err(|error| {
        error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            format!("invalid leader finish: {error}"),
        )
    })?;
    let attempt = submitted.attempt.parse::<AttemptId>().map_err(|_| {
        error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            format!("invalid attempt id: {}", submitted.attempt),
        )
    })?;
    let receipt = runtime_for(&state, run_id)
        .finish_leader_review(auth.actor, attempt)
        .await
        .map_err(app_error_response)?;
    state.scan_notify.notify_one();
    let receipt = serde_json::to_value(&receipt).map_err(|error| {
        error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            format!("cannot encode receipt: {error}"),
        )
    })?;
    Ok(Json(serde_json::json!({
        "run_id": run_id.to_string(),
        "receipt": receipt,
        "attempt_id": attempt.to_string(),
    })))
}
