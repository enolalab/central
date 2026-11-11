//! Artifacts: `GET /v1/runs/{id}/artifacts`,
//! `GET .../artifacts/{artifact}` and `GET .../content`.
//!
//! ID-scoped only: the artifact id must belong to the run, and bytes
//! come from the content-addressed store with the hash re-verified —
//! a metadata row pointing at a missing or re-hashed blob is never a
//! valid artifact. Reads are bounded (`offset`/`limit`, 1 MiB cap).
//! Path segments are ids, never paths: `..`, encoded traversal and
//! absolute paths are refused before any lookup.

use std::collections::HashMap;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use central_domain::{ArtifactId, RunId};

use super::auth::{app_error_response, authenticate};
use super::AppState;
use crate::api::errors::{error_response, ApiError};
use crate::api::read_models::project_artifact;

/// Maximum content bytes served in one response: 1 MiB.
pub const MAX_CONTENT_BYTES: usize = 1024 * 1024;

fn reject_traversal(segment: &str) -> Result<(), ApiError> {
    if segment.contains("..")
        || segment.contains('/')
        || segment.contains('\\')
        || segment.contains('%')
    {
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            "artifact segments are ids, never paths",
        ));
    }
    Ok(())
}

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

pub async fn list_artifacts(
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
    let mut items: Vec<serde_json::Value> =
        run_state.artifacts.values().map(project_artifact).collect();
    items.sort_by(|left, right| left["id"].as_str().cmp(&right["id"].as_str()));
    Ok(Json(
        serde_json::json!({ "items": items, "next_cursor": None::<String> }),
    ))
}

pub async fn get_artifact(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((raw_id, raw_artifact)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authenticate(&state, &headers).await?;
    let run_id = raw_id.parse::<RunId>().map_err(|_| {
        error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            format!("invalid run id: {raw_id}"),
        )
    })?;
    reject_traversal(&raw_artifact)?;
    let artifact_id = raw_artifact.parse::<ArtifactId>().map_err(|_| {
        error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            format!("invalid artifact id: {raw_artifact}"),
        )
    })?;
    let run_state = load_run(&state, run_id).await?;
    let artifact = run_state.artifacts.get(&artifact_id).ok_or_else(|| {
        error_response(
            StatusCode::NOT_FOUND,
            "not_found",
            format!("artifact {artifact_id} not found"),
        )
    })?;
    Ok(Json(project_artifact(artifact)))
}

pub async fn get_artifact_content(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((raw_id, raw_artifact)): Path<(String, String)>,
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
    reject_traversal(&raw_artifact)?;
    if let Some(path) = query.get("path") {
        if path.contains("..") || path.starts_with('/') || path.starts_with('\\') {
            return Err(error_response(
                StatusCode::BAD_REQUEST,
                "validation_failed",
                "artifact content is id-scoped; path escapes are refused",
            ));
        }
    }
    let artifact_id = raw_artifact.parse::<ArtifactId>().map_err(|_| {
        error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            format!("invalid artifact id: {raw_artifact}"),
        )
    })?;
    let run_state = load_run(&state, run_id).await?;
    // ID scoping first: a foreign id is 404 even when the blob exists.
    let record = run_state.artifacts.get(&artifact_id).ok_or_else(|| {
        error_response(
            StatusCode::NOT_FOUND,
            "not_found",
            format!("artifact {artifact_id} not found"),
        )
    })?;
    let bytes = state.artifacts.read(artifact_id).await.map_err(|error| {
        use central_application::contracts::AppError;
        match error {
            AppError::NotFound(_) => error_response(
                StatusCode::NOT_FOUND,
                "not_found",
                format!("artifact {artifact_id} has no committed bytes"),
            ),
            other => app_error_response(other),
        }
    })?;
    // Integrity: the bytes must still hash to the committed reference.
    if bytes.hash != record.hash {
        return Err(error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            format!("artifact {artifact_id} failed its integrity check"),
        ));
    }
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
    let limit: usize = query
        .get("limit")
        .map(|text| {
            text.parse::<usize>().map_err(|_| {
                error_response(
                    StatusCode::BAD_REQUEST,
                    "validation_failed",
                    "limit must be a number",
                )
            })
        })
        .transpose()?
        .unwrap_or(MAX_CONTENT_BYTES)
        .min(MAX_CONTENT_BYTES);
    if limit == 0 {
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            "limit must be at least 1",
        ));
    }
    let window: Vec<u8> = bytes.bytes.into_iter().skip(offset).take(limit).collect();
    // Bytes are binary-safe as hex; UTF-8 text is also shown inline.
    let hex = window
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Ok(Json(serde_json::json!({
        "id": artifact_id.to_string(),
        "hash": record.hash.to_string(),
        "offset": offset,
        "truncated": window.len() == limit,
        "content_hex": hex,
        "content_text": String::from_utf8(window).ok(),
        "provenance": bytes.provenance.map(|provenance| serde_json::json!({
            "source_commit": provenance.source_commit,
            "candidate_commit": provenance.candidate_commit,
            "workspace": provenance.workspace,
        })),
    })))
}
