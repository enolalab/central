//! User sync intake: `POST /v1/runs/{id}/sync`.
//!
//! The daemon verifies the commit read-only and queues the intent
//! through the application use case: the CLI never writes queue files
//! directly, and the worktree is never touched — no checkout, stash,
//! pull or push. Needs workflow rights.

use std::path::PathBuf;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use central_domain::RunId;
use serde::Deserialize;

use super::auth::{app_error_response, authenticate};
use super::AppState;
use crate::api::errors::{error_response, ApiError};

#[derive(Deserialize)]
pub struct SyncBody {
    pub commit: String,
    pub repository: Option<PathBuf>,
}

pub async fn post_sync(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(raw_id): Path<String>,
    Json(body): Json<SyncBody>,
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
    let queued = central_application::user_sync::queue_sync(
        &*state.store,
        &*state.sync_intake,
        run_id,
        body.repository,
        &body.commit,
    )
    .await
    .map_err(app_error_response)?;
    state.scan_notify.notify_one();
    Ok(Json(serde_json::json!({
        "status": "queued",
        "run_id": queued.run_id.to_string(),
        "commit": queued.commit,
        "repository": queued.repository,
        "dirty_fingerprint": queued.dirty_fingerprint,
    })))
}
