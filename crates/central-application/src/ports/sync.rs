//! User-sync intake port (P4).
//!
//! The daemon verifies the user's commit read-only and queues the intent
//! in its own state directory; the CLI never writes queue files
//! directly. The worktree is never mutated: no checkout, stash or pull.

use async_trait::async_trait;
use central_domain::RunId;
use serde::{Deserialize, Serialize};

use crate::contracts::AppError;

/// One queued user commit waiting for the daemon scan loop to pick up.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserSyncIntent {
    pub run: RunId,
    pub commit: String,
    pub repository: String,
    pub dirty_fingerprint: String,
}

#[async_trait]
pub trait SyncIntake: Send + Sync {
    /// Verify the commit exists (read-only probes only) and append one
    /// queue intent. Never mutates the user worktree.
    async fn queue_user_commit(
        &self,
        run: RunId,
        repository: &str,
        commit: &str,
    ) -> Result<UserSyncIntent, AppError>;
}
