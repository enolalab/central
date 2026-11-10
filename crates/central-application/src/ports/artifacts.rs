use async_trait::async_trait;
use central_domain::{ArtifactId, AttemptId, ContentHash, TaskId};

use crate::contracts::AppError;

/// Where a committed artifact came from in the Git workspace flow: the
/// user commit it was snapshotted from, the sealed candidate commit it was
/// built from, and the attempt worktree that produced it. All optional —
/// pre-workspace artifacts carry no provenance — and never affect the
/// content hash: the hash still binds the bytes alone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactProvenance {
    pub source_commit: Option<String>,
    pub candidate_commit: Option<String>,
    pub workspace: Option<String>,
}

/// One immutable artifact to commit. `path` is run-relative and must stay
/// inside the run root; `bytes` are staged to a temp file, hashed, flushed
/// and renamed before any metadata commits. `expected_hash`, when present,
/// must match the staged bytes or the publish aborts with nothing committed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishArtifact {
    pub attempt_id: AttemptId,
    pub task_id: Option<TaskId>,
    pub path: String,
    pub kind: String,
    pub bytes: Vec<u8>,
    pub expected_hash: Option<ContentHash>,
    pub provenance: Option<ArtifactProvenance>,
}

/// Committed reference to an immutable blob.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactRef {
    pub id: ArtifactId,
    pub hash: ContentHash,
}

/// A committed blob read back with its hash re-verified.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactBytes {
    pub id: ArtifactId,
    pub hash: ContentHash,
    pub bytes: Vec<u8>,
    pub provenance: Option<ArtifactProvenance>,
}

/// Content-addressed artifact store. Blobs are immutable after commit: there
/// is no update path, and no delete path for referenced blobs. An interrupted
/// publish leaves at most an orphan blob, which [`ArtifactStore::read`]
/// never treats as committed and [`ArtifactStore::sweep_orphans`] removes
/// explicitly.
#[async_trait]
pub trait ArtifactStore: Send + Sync {
    async fn publish(&self, request: PublishArtifact) -> Result<ArtifactRef, AppError>;
    async fn read(&self, id: ArtifactId) -> Result<ArtifactBytes, AppError>;
    /// Delete blobs no committed metadata references. Returns the number of
    /// blobs removed. Referenced blobs are never deleted.
    async fn sweep_orphans(&self) -> Result<usize, AppError>;
}
