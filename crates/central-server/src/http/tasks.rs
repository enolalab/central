//! Tasks and attempts: `GET /v1/runs/{id}/tasks`,
//! `GET .../tasks/{task}` and `GET .../attempts/{attempt}`.
//!
//! Status, dependencies, profile/input bindings, verification linkage
//! and the wait reason project here. Frozen attempt bindings (plan
//! hash, profile, session evidence) are history and never rewritten
//! by profile edits.

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use central_domain::{AttemptId, RunId, TaskId};

use super::auth::{app_error_response, authenticate};
use super::AppState;
use crate::api::errors::{error_response, ApiError};
use crate::api::read_models::{project_attempt, project_task};

async fn load_run(state: &AppState, run_id: RunId) -> Result<central_domain::RunState, ApiError> {
    state
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
        })
}

fn parse_run(raw: &str) -> Result<RunId, ApiError> {
    raw.parse::<RunId>().map_err(|_| {
        error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            format!("invalid run id: {raw}"),
        )
    })
}

pub async fn list_tasks(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(raw_id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authenticate(&state, &headers).await?;
    let run_state = load_run(&state, parse_run(&raw_id)?).await?;
    let mut items: Vec<serde_json::Value> = run_state
        .tasks
        .values()
        .map(|task| project_task(&run_state, task))
        .collect();
    items.sort_by(|left, right| left["task_id"].as_str().cmp(&right["task_id"].as_str()));
    Ok(Json(
        serde_json::json!({ "items": items, "next_cursor": None::<String> }),
    ))
}

pub async fn get_task(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((raw_id, raw_task)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authenticate(&state, &headers).await?;
    let run_state = load_run(&state, parse_run(&raw_id)?).await?;
    let task_id = raw_task.parse::<TaskId>().map_err(|_| {
        error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            format!("invalid task id: {raw_task}"),
        )
    })?;
    let task = run_state.tasks.get(&task_id).ok_or_else(|| {
        error_response(
            StatusCode::NOT_FOUND,
            "not_found",
            format!("task {task_id} not found"),
        )
    })?;
    Ok(Json(project_task(&run_state, task)))
}

pub async fn get_attempt(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((raw_id, raw_attempt)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authenticate(&state, &headers).await?;
    let run_state = load_run(&state, parse_run(&raw_id)?).await?;
    let attempt_id = raw_attempt.parse::<AttemptId>().map_err(|_| {
        error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            format!("invalid attempt id: {raw_attempt}"),
        )
    })?;
    let attempt = run_state.attempts.get(&attempt_id).ok_or_else(|| {
        error_response(
            StatusCode::NOT_FOUND,
            "not_found",
            format!("attempt {attempt_id} not found"),
        )
    })?;
    Ok(Json(project_attempt(&run_state, attempt)))
}
