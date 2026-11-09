//! Read models: `GET /v1/runs` and `GET /v1/runs/{id}`.

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use central_domain::RunId;

use super::auth::{app_error_response, authenticate, error_response, ApiError};
use super::AppState;

pub async fn list_runs(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    authenticate(&state, &headers)?;
    let mut runs = Vec::new();
    for run_id in state
        .store
        .list_run_ids()
        .await
        .map_err(app_error_response)?
    {
        let run_state = state
            .store
            .load(run_id)
            .await
            .map_err(app_error_response)?
            .ok_or_else(|| {
                error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("run {run_id} listed but not loadable"),
                )
            })?;
        runs.push(serde_json::json!({
            "run_id": run_id.to_string(),
            "revision": run_state.revision.0,
            "control": format!("{:?}", run_state.control),
            "goal": run_state.goal,
            "repository": run_state.repository,
            "created": run_state.created,
        }));
    }
    Ok(Json(serde_json::json!({ "runs": runs })))
}

pub async fn get_run(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(raw_id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authenticate(&state, &headers)?;
    let run_id = raw_id.parse::<RunId>().map_err(|_| {
        error_response(StatusCode::BAD_REQUEST, format!("invalid run id: {raw_id}"))
    })?;
    let run_state = state
        .store
        .load(run_id)
        .await
        .map_err(app_error_response)?
        .ok_or_else(|| error_response(StatusCode::NOT_FOUND, format!("run {run_id} not found")))?;
    let events = state
        .store
        .events(run_id)
        .await
        .map_err(app_error_response)?;
    Ok(Json(serde_json::json!({
        "state": serde_json::to_value(&run_state).map_err(|error| error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("cannot encode run: {error}"),
        ))?,
        "events": serde_json::to_value(&events).map_err(|error| error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("cannot encode events: {error}"),
        ))?,
        "quality": quality_summary(&run_state),
    })))
}

/// Per-task verification summary (P3 Task 2): repair budget use, dispute
/// count, verified output and pending repair linkage. Read-only projection
/// of durable run state; judging and routing live in the domain.
fn quality_summary(run_state: &central_domain::RunState) -> serde_json::Value {
    let tasks = run_state
        .tasks
        .values()
        .map(|task| {
            serde_json::json!({
                "task_id": task.id.to_string(),
                "status": format!("{:?}", task.status),
                "repair_rounds_used": task.repair_rounds_used,
                "max_repair_rounds": task.max_repair_rounds,
                "disputes": run_state
                    .verification_disputes
                    .get(&task.id)
                    .copied()
                    .unwrap_or(0),
                "verified_output": run_state
                    .verified_outputs
                    .get(&task.id)
                    .map(|hash| hash.to_string()),
                "repair_pending": run_state.repair_base.contains_key(&task.id),
            })
        })
        .collect::<Vec<_>>();
    serde_json::json!({ "tasks": tasks })
}
