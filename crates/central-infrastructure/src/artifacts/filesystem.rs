//! Immutable content-addressed artifact store (P2 Task 5).
//!
//! Layout under the store root:
//!
//! ```text
//! <root>/blobs/<sha256>      immutable committed blobs, named by content hash
//! <root>/tmp/<uuid>.part     staged bytes, never a committed artifact
//! <root>/meta/<artifact-id>.json  committed metadata pointing at one blob
//! ```
//!
//! Publish stages bytes to a temp file, hashes, flushes (`sync_all`) and
//! only then renames the blob into place before committing the metadata:
//! an interrupted publish leaves at most an orphan blob, which [`read`]
//! never treats as committed. Orphans are removed explicitly by
//! [`sweep_orphans`]; blobs with committed metadata pointing at them are
//! never deleted. A metadata file pointing at a missing or re-hashed blob
//! is never treated as a valid artifact — reads fail instead.
//!
//! [`read`]: ArtifactStore::read
//! [`sweep_orphans`]: ArtifactStore::sweep_orphans

use std::collections::BTreeSet;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use central_application::artifacts::{check_run_relative_path, content_hash, verify_content_hash};
use central_application::contracts::AppError;
use central_application::ports::artifacts::{
    ArtifactBytes, ArtifactRef, ArtifactStore, PublishArtifact,
};
use central_domain::ArtifactId;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Committed metadata for one blob. The blob is found by `hash`, never by
/// trusting the path alone.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ArtifactMeta {
    id: ArtifactId,
    hash: central_domain::ContentHash,
    path: String,
    kind: String,
    task_id: Option<central_domain::TaskId>,
    attempt_id: Option<central_domain::AttemptId>,
    size: u64,
}

/// Filesystem [`ArtifactStore`]. All operations stay under `root`; only
/// run-relative blob paths are accepted.
pub struct FilesystemArtifactStore {
    blobs: PathBuf,
    tmp: PathBuf,
    meta: PathBuf,
    staging: AtomicU64,
}

impl FilesystemArtifactStore {
    /// Open (creating) the store layout under `root`.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, AppError> {
        let root = root.into();
        let blobs = root.join("blobs");
        let tmp = root.join("tmp");
        let meta = root.join("meta");
        for dir in [&blobs, &tmp, &meta] {
            std::fs::create_dir_all(dir).map_err(|error| {
                AppError::Storage(format!(
                    "cannot create artifact dir {}: {error}",
                    dir.display()
                ))
            })?;
        }
        Ok(Self {
            blobs,
            tmp,
            meta,
            staging: AtomicU64::new(0),
        })
    }

    /// Directory holding committed blobs. Exposed for contract tests that
    /// simulate a crash (tampered blob) or count committed files.
    pub fn blobs_dir(&self) -> &Path {
        &self.blobs
    }

    /// Crash simulation: write a blob with no committed metadata, exactly
    /// what an interrupted publish leaves behind. Never part of the commit
    /// path — [`publish`] always commits metadata with the blob.
    ///
    /// [`publish`]: ArtifactStore::publish
    pub fn import_orphan_blob(
        &self,
        bytes: &[u8],
    ) -> Result<central_domain::ContentHash, AppError> {
        let hash = content_hash(bytes);
        let dest = self.blobs.join(hash.as_str());
        if !dest.exists() {
            std::fs::write(&dest, bytes)
                .map_err(|error| AppError::Storage(format!("cannot stage orphan blob: {error}")))?;
        }
        Ok(hash)
    }

    fn stage_bytes(&self, bytes: &[u8]) -> Result<PathBuf, AppError> {
        let staged = self.tmp.join(format!(
            "{}-{}.part",
            Uuid::new_v4(),
            self.staging.fetch_add(1, Ordering::SeqCst)
        ));
        let mut file = std::fs::File::create(&staged)
            .map_err(|error| AppError::Storage(format!("cannot stage artifact bytes: {error}")))?;
        file.write_all(bytes)
            .map_err(|error| AppError::Storage(format!("cannot stage artifact bytes: {error}")))?;
        file.sync_all()
            .map_err(|error| AppError::Storage(format!("cannot flush staged artifact: {error}")))?;
        drop(file);
        Ok(staged)
    }

    fn commit_meta(&self, meta: &ArtifactMeta) -> Result<(), AppError> {
        let dest = self.meta.join(format!("{}.json", meta.id));
        let staged = self.tmp.join(format!("{}.meta-part", Uuid::new_v4()));
        let text = serde_json::to_string(meta).map_err(|error| {
            AppError::Storage(format!("cannot encode artifact metadata: {error}"))
        })?;
        std::fs::write(&staged, text).map_err(|error| {
            AppError::Storage(format!("cannot stage artifact metadata: {error}"))
        })?;
        std::fs::rename(&staged, &dest).map_err(|error| {
            let _ = std::fs::remove_file(&staged);
            AppError::Storage(format!("cannot commit artifact metadata: {error}"))
        })?;
        Ok(())
    }

    fn load_meta(&self, id: ArtifactId) -> Result<ArtifactMeta, AppError> {
        let path = self.meta.join(format!("{id}.json"));
        let text = std::fs::read_to_string(&path)
            .map_err(|_| AppError::NotFound(format!("no committed artifact {id}")))?;
        serde_json::from_str(&text).map_err(|error| {
            AppError::Storage(format!(
                "committed artifact {id} has corrupt metadata: {error}"
            ))
        })
    }
}

#[async_trait]
impl ArtifactStore for FilesystemArtifactStore {
    async fn publish(&self, request: PublishArtifact) -> Result<ArtifactRef, AppError> {
        check_run_relative_path(&request.path)?;
        let staged = self.stage_bytes(&request.bytes)?;
        let hash = content_hash(&request.bytes);
        if let Some(expected) = &request.expected_hash {
            if hash != *expected {
                let _ = std::fs::remove_file(&staged);
                verify_content_hash(&request.bytes, expected)?;
            }
        }
        let dest = self.blobs.join(hash.as_str());
        if dest.exists() {
            // Same content is already committed: the staged copy is redundant.
            let _ = std::fs::remove_file(&staged);
        } else {
            std::fs::rename(&staged, &dest).map_err(|error| {
                let _ = std::fs::remove_file(&staged);
                AppError::Storage(format!("cannot commit artifact blob: {error}"))
            })?;
        }
        let id = ArtifactId::new();
        let meta = ArtifactMeta {
            id,
            hash: hash.clone(),
            path: request.path,
            kind: request.kind,
            task_id: request.task_id,
            attempt_id: Some(request.attempt_id),
            size: request.bytes.len() as u64,
        };
        // The blob without metadata is an orphan by construction: it is
        // never readable as committed (no id points at it) and a later
        // sweep removes it. Never delete the blob here — another publish
        // of the same content may already reference it.
        self.commit_meta(&meta)?;
        Ok(ArtifactRef { id, hash })
    }

    async fn read(&self, id: ArtifactId) -> Result<ArtifactBytes, AppError> {
        let meta = self.load_meta(id)?;
        let blob = self.blobs.join(meta.hash.as_str());
        let bytes = std::fs::read(&blob).map_err(|_| {
            AppError::Storage(format!(
                "committed artifact {id} points at a missing blob; not a valid artifact"
            ))
        })?;
        verify_content_hash(&bytes, &meta.hash).map_err(|_| {
            AppError::Storage(format!(
                "committed artifact {id} fails its hash check; not a valid artifact"
            ))
        })?;
        Ok(ArtifactBytes {
            id,
            hash: meta.hash,
            bytes,
        })
    }

    async fn sweep_orphans(&self) -> Result<usize, AppError> {
        let mut referenced: BTreeSet<String> = BTreeSet::new();
        let entries = std::fs::read_dir(&self.meta).map_err(|error| {
            AppError::Storage(format!("cannot list artifact metadata: {error}"))
        })?;
        for entry in entries {
            let entry = entry.map_err(|error| {
                AppError::Storage(format!("cannot list artifact metadata: {error}"))
            })?;
            if entry
                .file_type()
                .map_err(|error| {
                    AppError::Storage(format!("cannot list artifact metadata: {error}"))
                })?
                .is_file()
            {
                // Fail closed: a corrupt metadata file stops the sweep with
                // nothing deleted, so an unreadable reference can never make
                // its blob look orphaned.
                let text = std::fs::read_to_string(entry.path()).map_err(|error| {
                    AppError::Storage(format!("cannot read artifact metadata: {error}"))
                })?;
                let meta: ArtifactMeta = serde_json::from_str(&text).map_err(|error| {
                    AppError::Storage(format!("corrupt artifact metadata: {error}"))
                })?;
                referenced.insert(meta.hash.as_str().to_string());
            }
        }
        let mut removed = 0;
        let blobs = std::fs::read_dir(&self.blobs)
            .map_err(|error| AppError::Storage(format!("cannot list artifact blobs: {error}")))?;
        for entry in blobs {
            let entry = entry.map_err(|error| {
                AppError::Storage(format!("cannot list artifact blobs: {error}"))
            })?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let is_file = entry
                .file_type()
                .map_err(|error| AppError::Storage(format!("cannot list artifact blobs: {error}")))?
                .is_file();
            if is_file && !referenced.contains(&name) {
                std::fs::remove_file(entry.path()).map_err(|error| {
                    AppError::Storage(format!("cannot remove orphan blob: {error}"))
                })?;
                removed += 1;
            }
        }
        Ok(removed)
    }
}
