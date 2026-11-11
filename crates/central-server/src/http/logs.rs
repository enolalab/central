//! Logs: `GET /v1/runs/{id}/logs`.
//!
//! Bounded, run-scoped evidence reads: harness observations (optionally
//! filtered to one attempt of this run) plus the redacted event tail.
//! There is no live log tail here — live updates flow through the
//! event stream. `limit` caps at 500 rows; absurd windows are 400.

use std::collections::HashMap;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use central_domain::RunId;

use super::auth::{app_error_response, authenticate};
use super::AppState;
use crate::api::dto::parse_limit;
use crate::api::errors::{error_response, ApiError};
use crate::api::read_models::{project_event, redact_text};

pub async fn get_logs(
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
    let limit = parse_limit(query.get("limit").map(|text| text.as_str()))? as usize;
    let offset: usize = query
        .get("offset")
        .map(|text| {
            text.parse::<usize>().map_err(|_| {
                error_response(
                    StatusCode::BAD_REQUEST,
                    "validation_failed",
                    "offset must be a number",
                )
            })
        })
        .transpose()?
        .unwrap_or(0);
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
    // An attempt filter must belong to this run: foreign ids are 404,
    // never an empty page that hides the mistake.
    if let Some(raw_attempt) = query.get("attempt") {
        let attempt_id = raw_attempt
            .parse::<central_domain::AttemptId>()
            .map_err(|_| {
                error_response(
                    StatusCode::BAD_REQUEST,
                    "validation_failed",
                    format!("invalid attempt id: {raw_attempt}"),
                )
            })?;
        if !run_state.attempts.contains_key(&attempt_id) {
            return Err(error_response(
                StatusCode::NOT_FOUND,
                "not_found",
                format!("attempt {attempt_id} is not part of this run"),
            ));
        }
    }
    let attempt_filter = query.get("attempt").cloned();
    let mut entries: Vec<serde_json::Value> = run_state
        .observations
        .iter()
        .filter(|observation| match &attempt_filter {
            None => true,
            Some(raw) => observation.attempt_id.to_string() == *raw,
        })
        .map(|observation| {
            serde_json::json!({
                "attempt_id": observation.attempt_id.to_string(),
                "provider_epoch": observation.provider_epoch,
                "dedup_key": observation.dedup_key,
                "outcome": observation.outcome.as_ref().map(|outcome| format!("{outcome:?}")),
                "detail": redact_text(&observation.detail),
            })
        })
        .collect();
    // Plus the redacted event tail for correlation (bounded by limit).
    let tail = state
        .store
        .events_after(run_id, 0, u32::MAX)
        .await
        .map_err(app_error_response)?;
    let start = tail.len().saturating_sub(limit);
    for row in tail.iter().skip(start) {
        entries.push(project_event(row.seq, &row.event));
    }
    let total = entries.len();
    let entries: Vec<serde_json::Value> = entries.into_iter().skip(offset).take(limit).collect();
    Ok(Json(serde_json::json!({
        "run_id": run_id.to_string(),
        "total": total,
        "entries": entries,
    })))
}
