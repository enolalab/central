//! Plans: `GET /v1/runs/{id}/plans`, `GET .../plans/{hash}` and
//! `GET .../plans/diff?from=&to=`. Revisions, hashes and criteria are
//! explicit: approval targets an exact hash (never an implicit
//! latest), and the diff flags criteria moves because those need an
//! explanation and user approval before taking effect.

use std::collections::HashMap;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use central_domain::RunId;

use super::auth::{app_error_response, authenticate};
use super::AppState;
use crate::api::errors::{error_response, ApiError};
use crate::api::read_models::{plan_diff, project_plan};

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

pub async fn list_plans(
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
    let run_state = load_run(&state, run_id).await?;
    let mut items: Vec<serde_json::Value> = run_state.plans.values().map(project_plan).collect();
    items.sort_by(|left, right| left["hash"].as_str().cmp(&right["hash"].as_str()));
    Ok(Json(serde_json::json!({
        "items": items,
        "next_cursor": None::<String>,
        "active_plan": run_state.active_plan.as_ref().map(|hash| hash.to_string()),
        "pending_plan": run_state.pending_plan.as_ref().map(|hash| hash.to_string()),
    })))
}

pub async fn get_plan(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((raw_id, hash)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authenticate(&state, &headers).await?;
    let run_id = raw_id.parse::<RunId>().map_err(|_| {
        error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            format!("invalid run id: {raw_id}"),
        )
    })?;
    if hash.trim().is_empty() {
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            "plan hash must not be empty",
        ));
    }
    let run_state = load_run(&state, run_id).await?;
    let key = central_domain::PlanHash::from_hex(&hash);
    let plan = run_state.plans.get(&key).ok_or_else(|| {
        error_response(
            StatusCode::NOT_FOUND,
            "not_found",
            format!("plan {hash} not found"),
        )
    })?;
    Ok(Json(project_plan(plan)))
}

pub async fn diff_plans(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(raw_id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authenticate(&state, &headers).await?;
    let run_id = raw_id.parse::<RunId>().map_err(|_| {
        error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            format!("invalid run id: {raw_id}"),
        )
    })?;
    let from = query.get("from").ok_or_else(|| {
        error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            "diff needs ?from=<plan-hash>&to=<plan-hash>",
        )
    })?;
    let to = query.get("to").ok_or_else(|| {
        error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            "diff needs ?from=<plan-hash>&to=<plan-hash>",
        )
    })?;
    let run_state = load_run(&state, run_id).await?;
    let missing = |hash: &str| {
        error_response(
            StatusCode::NOT_FOUND,
            "not_found",
            format!("plan {hash} not found"),
        )
    };
    let before = run_state
        .plans
        .get(&central_domain::PlanHash::from_hex(from))
        .ok_or_else(|| missing(from))?;
    let after = run_state
        .plans
        .get(&central_domain::PlanHash::from_hex(to))
        .ok_or_else(|| missing(to))?;
    let mut diff = plan_diff(&before.document, &after.document);
    diff["from"] = serde_json::Value::String(from.clone());
    diff["to"] = serde_json::Value::String(to.clone());
    Ok(Json(diff))
}
