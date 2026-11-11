//! Daemon-side user sync intake (P4).
//!
//! The CLI no longer writes the sync queue file itself: it calls
//! `POST /v1/runs/{id}/sync` and the daemon verifies the commit
//! read-only and queues the intent through the [`SyncIntake`] port.
//! The worktree is never touched — no checkout, no stash, no pull —
//! and the queue still belongs to the daemon's state directory, picked
//! up by the scan loop.

use std::path::PathBuf;

use central_domain::RunId;

use crate::contracts::AppError;
use crate::ports::{Store, SyncIntake, UserSyncIntent};

/// What the daemon queued for its scan loop to pick up.
#[derive(Clone, Debug)]
pub struct QueuedSync {
    pub run_id: RunId,
    pub commit: String,
    pub repository: String,
    pub dirty_fingerprint: String,
}

/// Verify the commit exists (read-only) and queue it daemon-side.
/// The repository defaults to the run's own; an explicit override must
/// still verify read-only. Nothing here writes to the worktree.
pub async fn queue_sync(
    store: &dyn Store,
    intake: &dyn SyncIntake,
    run_id: RunId,
    repository: Option<PathBuf>,
    commit: &str,
) -> Result<QueuedSync, AppError> {
    if commit.trim().is_empty() {
        return Err(AppError::Validation(
            "sync commit must not be empty".to_string(),
        ));
    }
    let state = store
        .load(run_id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("run {run_id} not found")))?;
    let repo = match repository {
        Some(explicit) => explicit.to_string_lossy().into_owned(),
        None => {
            if state.repository.trim().is_empty() {
                return Err(AppError::Validation(
                    "run has no repository; pass one explicitly".to_string(),
                ));
            }
            state.repository.clone()
        }
    };
    let intent: UserSyncIntent = intake
        .queue_user_commit(run_id, &repo, commit.trim())
        .await?;
    Ok(QueuedSync {
        run_id,
        commit: intent.commit,
        repository: intent.repository,
        dirty_fingerprint: intent.dirty_fingerprint,
    })
}
