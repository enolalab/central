//! Read models: `GET /v1/runs` (paginated) and `GET /v1/runs/{id}`
//! (snapshot with durable cursor).
//!
//! The snapshot is read atomically with its cursor
//! (`snapshot_with_cursor`): `last_seq` covers exactly the returned
//! state, so a client streaming from it misses nothing. The state
//! itself is the curated projection (`project_state`), never the raw
//! `RunState`: internal scheduling maps stay server-side. Optional
//! `?known_epoch=` lets a reconnecting client prove it resnapshotted
//! after a restart; a stale epoch is 409 `resync_required`.

use std::collections::HashMap;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use central_domain::RunId;

use super::auth::{app_error_response, authenticate};
use super::AppState;
use crate::api::dto::{decode_cursor, encode_cursor, parse_limit};
use crate::api::errors::{error_response, ApiError};
use crate::api::read_models::project_state;

pub async fn list_runs(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authenticate(&state, &headers).await?;
    let limit = parse_limit(query.get("limit").map(|text| text.as_str()))?;
    let after: usize = match query.get("cursor") {
        None => 0,
        Some(cursor) => decode_cursor(cursor, "runs", "-").map(|pos| pos as usize)?,
    };
    let mut ids = state
        .store
        .list_run_ids()
        .await
        .map_err(app_error_response)?;
    ids.sort_by_key(|left| left.to_string());
    let items: Vec<serde_json::Value> = ids
        .into_iter()
        .skip(after)
        .take(limit as usize)
        .map(|run_id| {
            serde_json::json!({
                "run_id": run_id.to_string(),
            })
        })
        .collect();
    let next_cursor = if items.len() == limit as usize {
        Some(encode_cursor("runs", "-", (after + items.len()) as u64))
    } else {
        None
    };
    Ok(Json(
        serde_json::json!({ "items": items, "next_cursor": next_cursor }),
    ))
}

pub async fn get_run(
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
    let snapshot = state
        .store
        .snapshot_with_cursor(run_id)
        .await
        .map_err(app_error_response)?
        .ok_or_else(|| {
            error_response(
                StatusCode::NOT_FOUND,
                "not_found",
                format!("run {run_id} not found"),
            )
        })?;
    if let Some(known) = query.get("known_epoch") {
        if known != &state.instance_epoch {
            return Err(error_response(
                StatusCode::CONFLICT,
                "resync_required",
                "daemon instance changed since this snapshot; refetch the snapshot, then stream from its cursor",
            ));
        }
    }
    // Full ordered history as projected views (legacy shape kept for
    // the P1-P3 gates; new clients page through `/events` and stream).
    let sequenced = state
        .store
        .events_after(run_id, 0, u32::MAX)
        .await
        .map_err(app_error_response)?;
    let events: Vec<serde_json::Value> = sequenced
        .iter()
        .map(|row| crate::api::read_models::project_event(row.seq, &row.event))
        .collect();
    Ok(Json(serde_json::json!({
        "instance_epoch": state.instance_epoch,
        "revision": snapshot.state.revision.0,
        "last_seq": snapshot.last_seq,
        "state": project_state(&snapshot.state),
        "events": events,
        "quality": quality_summary(&snapshot.state),
    })))
}

/// Per-task verification summary: repair budget use, dispute count,
/// verified output and pending repair linkage. Read-only projection
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
