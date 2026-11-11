//! Owner-only maintenance: `POST /v1/maintenance/export` and
//! `POST /v1/maintenance/gc`. One implementation shared with the CLI:
//! no second write path. Export is a redacted report (never a database
//! backup, never credentials). GC is dry-run-first and explicit:
//! active, unknown-outcome or referenced work is never a candidate,
//! and a non-dry-run call is refused until the dry run was reviewed.

use std::collections::BTreeSet;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use serde::Deserialize;

use super::auth::{app_error_response, authenticate};
use super::AppState;
use crate::api::errors::{error_response, ApiError};

#[derive(Deserialize)]
pub struct GcBody {
    pub dry_run: bool,
    pub referenced: Option<Vec<String>>,
}

pub async fn post_export(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    let auth = authenticate(&state, &headers).await?;
    auth.require_owner()?;
    let report = central_application::maintenance::export_state(&*state.store)
        .await
        .map_err(app_error_response)?;
    Ok(Json(serde_json::json!({
        "runs": report.runs.iter().map(|id| id.to_string()).collect::<Vec<_>>(),
        "client_ids": report.client_ids,
        "profiles": report.profiles,
        "note": report.note,
    })))
}

pub async fn post_gc(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<GcBody>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let auth = authenticate(&state, &headers).await?;
    auth.require_owner()?;
    if !body.dry_run {
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            "GC deletes nothing without an explicit dry run first: repeat with {\"dry_run\": true}",
        ));
    }
    let referenced: BTreeSet<String> = body.referenced.unwrap_or_default().into_iter().collect();
    let candidates = central_application::maintenance::gc_dry_run(&*state.store, &referenced)
        .await
        .map_err(app_error_response)?;
    Ok(Json(serde_json::json!({
        "dry_run": true,
        "deleted": [],
        "candidates": candidates.iter().map(|candidate| serde_json::json!({
            "kind": candidate.kind,
            "id": candidate.id,
            "reason": candidate.reason,
        })).collect::<Vec<_>>(),
    })))
}
