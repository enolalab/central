//! Git workspaces, sealed candidates and incremental integration (P3 Task 5).
//!
//! [`GitWorkspaceManager`] implements the [`WorkspaceManager`] port over
//! real git, invoked argv-only with a per-command owned empty hooks dir: no
//! shell strings, no global config mutation, no automatic stash. One
//! detached worktree per attempt; sealed source commits are immutable;
//! verifier scratch outside the worktree never invalidates evidence while a
//! tracked-source write inside it does (the git-level signal behind the
//! `verifier_wrote_tracked_source` / `tainted_evidence` application gate
//! carried over from P3/T2). Integration serializes approved nodes per run
//! onto a temporary branch and moves the good ref only by compare-and-swap
//! after passing baseline + affected + acceptance checks.

pub mod git;
pub mod integration;
pub mod paths;

pub use git::Git;
pub use integration::run_integration_checks;
pub use paths::{canonical_root, contained_path};

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use async_trait::async_trait;
use central_application::artifacts::content_hash;
use central_application::contracts::AppError;
use central_application::integration::gate_promotion;
use central_application::ports::workspace::{
    CandidateRef, InputSnapshot, IntegrationCandidate, IntegrationRequest, PromoteRequest,
    PromotionIntent, PromotionReceipt, SelectedFile, SnapshotRequest, WorkspaceManager,
    WorkspaceRef, WorkspaceRequest,
};
use central_domain::{AttemptId, RunId};
use serde::{Deserialize, Serialize};

/// One live worktree tracked by the manager: the repo it was cut from and
/// the path it lives at. Both stay under manager control until explicit,
/// ownership-checked removal.
#[derive(Clone, Debug)]
struct LiveWorkspace {
    repo: PathBuf,
    worktree: PathBuf,
}

/// One sealed candidate: where its worktree lives so the seal can be
/// re-derived, and the tree/parent it was sealed with.
#[derive(Clone, Debug)]
struct SealedEntry {
    attempt_id: AttemptId,
    repo: PathBuf,
    worktree: PathBuf,
    tree: String,
    parent: String,
}

/// One prepared integration waiting for checks and promotion.
#[derive(Clone, Debug)]
struct ActiveIntegration {
    branch: String,
    worktree: PathBuf,
    repo: PathBuf,
    candidate: String,
    ref_name: String,
    base: String,
}

/// Git-backed [`WorkspaceManager`]. All managed paths live under
/// `state_root` (`hooks/`, `worktrees/`, `intents/`, `sync-queue/`); repos
/// themselves are external and only ever read or worktree-cut, never
/// rewritten except through the CAS-promoted good ref.
pub struct GitWorkspaceManager {
    state_root: PathBuf,
    git: Git,
    worktrees_dir: PathBuf,
    intents_dir: PathBuf,
    live: Mutex<HashMap<AttemptId, LiveWorkspace>>,
    sealed: Mutex<HashMap<String, SealedEntry>>,
    active: Mutex<HashMap<RunId, ActiveIntegration>>,
    counter: AtomicU64,
}

impl GitWorkspaceManager {
    /// Open (creating) the manager state under `state_root`.
    pub fn open(state_root: &Path) -> Result<Self, AppError> {
        let worktrees_dir = state_root.join("worktrees");
        let intents_dir = state_root.join("intents");
        let sync_dir = state_root.join("sync-queue");
        for dir in [&worktrees_dir, &intents_dir, &sync_dir] {
            std::fs::create_dir_all(dir).map_err(|error| {
                AppError::Storage(format!("cannot create workspace dir: {error}"))
            })?;
        }
        let git = Git::new(state_root.join("hooks"))?;
        Ok(Self {
            state_root: state_root.to_path_buf(),
            git,
            worktrees_dir,
            intents_dir,
            live: Mutex::new(HashMap::new()),
            sealed: Mutex::new(HashMap::new()),
            active: Mutex::new(HashMap::new()),
            counter: AtomicU64::new(0),
        })
    }

    fn lock<T>(slot: &Mutex<T>) -> Result<std::sync::MutexGuard<'_, T>, AppError> {
        slot.lock()
            .map_err(|_| AppError::Storage("workspace registry is poisoned".to_string()))
    }

    /// The manager-owned state root (`hooks/`, `worktrees/`, `intents/`,
    /// `sync-queue/` live under it).
    pub fn state_root(&self) -> &Path {
        &self.state_root
    }

    fn worktree_for(&self, attempt: AttemptId) -> PathBuf {
        self.worktrees_dir.join(format!(
            "{}-{}",
            attempt,
            self.counter.fetch_add(1, Ordering::SeqCst)
        ))
    }

    fn live_workspace(&self, workspace: &WorkspaceRef) -> Result<LiveWorkspace, AppError> {
        let live = Self::lock(&self.live)?;
        let entry = live.get(&workspace.attempt_id).ok_or_else(|| {
            AppError::Forbidden(format!(
                "unknown workspace for attempt {}; cleanup and sealing are ownership-checked",
                workspace.attempt_id,
            ))
        })?;
        if entry.worktree.to_string_lossy() != workspace.worktree {
            return Err(AppError::Forbidden(format!(
                "workspace path for attempt {} does not match the owned worktree",
                workspace.attempt_id,
            )));
        }
        Ok(entry.clone())
    }

    fn intent_path(&self, run: RunId, ref_name: &str) -> PathBuf {
        let safe_ref: String = ref_name
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        self.intents_dir
            .join(format!("{run}"))
            .join(format!("{safe_ref}.json"))
    }
}

#[async_trait]
impl WorkspaceManager for GitWorkspaceManager {
    async fn snapshot(&self, request: SnapshotRequest) -> Result<InputSnapshot, AppError> {
        let repo = canonical_root(Path::new(&request.repo_root))?;
        let commit = self.git.rev_parse_commit(&repo, &request.commit).await?;
        let tree = self.git.commit_tree(&repo, &commit).await?;
        let mut selected = Vec::with_capacity(request.selected_dirty_paths.len());
        for raw in &request.selected_dirty_paths {
            let path = contained_path(&repo, raw)?;
            let bytes = std::fs::read(&path).map_err(|error| {
                AppError::Storage(format!("cannot read selected dirty path {raw}: {error}"))
            })?;
            selected.push(SelectedFile {
                path: raw.clone(),
                hash: content_hash(&bytes),
            });
        }
        selected.sort_by(|a, b| a.path.cmp(&b.path));
        let mut binding = format!("commit:{commit}\ntree:{tree}\n");
        for file in &selected {
            binding.push_str(&format!("{}:{}\n", file.path, file.hash));
        }
        Ok(InputSnapshot {
            repo_root: repo.to_string_lossy().into_owned(),
            commit: commit.clone(),
            tree,
            selected,
            snapshot_hash: content_hash(binding.as_bytes()),
        })
    }

    async fn create(&self, request: WorkspaceRequest) -> Result<WorkspaceRef, AppError> {
        let repo = canonical_root(Path::new(&request.input.repo_root))?;
        {
            let live = Self::lock(&self.live)?;
            if live.contains_key(&request.attempt_id) {
                return Err(AppError::Conflict(format!(
                    "attempt {} already owns a worktree; one detached worktree per attempt",
                    request.attempt_id,
                )));
            }
        }
        let worktree = self.worktree_for(request.attempt_id);
        if worktree.exists() {
            return Err(AppError::Conflict(format!(
                "worktree path {} already exists",
                worktree.display()
            )));
        }
        self.git
            .add_worktree_detached(&repo, &worktree, &request.input.commit)
            .await?;
        let canonical_wt = worktree.canonicalize().map_err(|error| {
            AppError::Storage(format!(
                "created worktree is unusable {}: {error}",
                worktree.display()
            ))
        })?;
        // Materialize exactly the snapshotted dirty bytes, refusing drift:
        // the repo file must still hash to the snapshot or the input moved
        // since the snapshot and a fresh snapshot is required.
        for file in &request.input.selected {
            let source = contained_path(&repo, &file.path)?;
            let bytes = std::fs::read(&source).map_err(|error| {
                AppError::Storage(format!(
                    "cannot re-read selected path {}: {error}",
                    file.path
                ))
            })?;
            if content_hash(&bytes) != file.hash {
                let _ = self.git.remove_worktree(&repo, &worktree).await;
                return Err(AppError::Conflict(format!(
                    "dirty input {} changed since the snapshot; take a fresh snapshot",
                    file.path
                )));
            }
            let dest = contained_path(&canonical_wt, &file.path)?;
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent).map_err(|error| {
                    AppError::Storage(format!(
                        "cannot create worktree dirs for {}: {error}",
                        file.path
                    ))
                })?;
            }
            std::fs::write(&dest, &bytes).map_err(|error| {
                AppError::Storage(format!(
                    "cannot materialize selected path {}: {error}",
                    file.path
                ))
            })?;
        }
        Self::lock(&self.live)?.insert(
            request.attempt_id,
            LiveWorkspace {
                repo: repo.clone(),
                worktree: worktree.clone(),
            },
        );
        Ok(WorkspaceRef {
            attempt_id: request.attempt_id,
            worktree: worktree.to_string_lossy().into_owned(),
            base_commit: request.input.commit.clone(),
        })
    }

    async fn seal(&self, workspace: &WorkspaceRef) -> Result<CandidateRef, AppError> {
        let live = self.live_workspace(workspace)?;
        let worktree = PathBuf::from(&workspace.worktree);
        self.git.run(&worktree, &["add", "-A"]).await?;
        // Always a fresh commit (even when empty) so the sealed candidate is
        // a distinct immutable object bound to this attempt.
        let message = format!(
            "central: seal candidate for attempt {}",
            workspace.attempt_id
        );
        self.git
            .run(
                &worktree,
                &[
                    "-c",
                    "user.name=central",
                    "-c",
                    "user.email=central@local",
                    "commit",
                    "--allow-empty",
                    "-m",
                    &message,
                ],
            )
            .await?;
        let commit = self.git.run(&worktree, &["rev-parse", "HEAD"]).await?;
        let parent = self.git.run(&worktree, &["rev-parse", "HEAD^"]).await?;
        let tree = self.git.commit_tree(&live.repo, &commit).await?;
        Self::lock(&self.sealed)?.insert(
            commit.clone(),
            SealedEntry {
                attempt_id: workspace.attempt_id,
                repo: live.repo.clone(),
                worktree: worktree.clone(),
                tree: tree.clone(),
                parent: parent.clone(),
            },
        );
        Ok(CandidateRef {
            attempt_id: workspace.attempt_id,
            commit,
            tree,
            parent,
        })
    }

    async fn verify_seal(&self, candidate: &CandidateRef) -> Result<(), AppError> {
        let entry = Self::lock(&self.sealed)?
            .get(&candidate.commit)
            .cloned()
            .ok_or_else(|| {
                AppError::NotFound(format!(
                    "no sealed candidate {}; reverify against the current seal",
                    candidate.commit,
                ))
            })?;
        if entry.attempt_id != candidate.attempt_id
            || entry.tree != candidate.tree
            || entry.parent != candidate.parent
        {
            return Err(AppError::Conflict(format!(
                "candidate {} does not match its seal; reverify against the current seal",
                candidate.commit,
            )));
        }
        let head = self
            .git
            .run(&entry.worktree, &["rev-parse", "HEAD"])
            .await?;
        // `write-tree` only serializes the index, so an uncommitted
        // working-tree edit would slip past a tree comparison: any
        // porcelain output means the tracked source moved under
        // verification. (Untracked verifier scratch lives outside the
        // worktree and never shows up here.)
        let dirty = self.git.porcelain_status(&entry.worktree).await?;
        let current = self.git.worktree_tree(&entry.worktree).await?;
        if head != candidate.commit || !dirty.is_empty() || current != candidate.tree {
            return Err(AppError::Conflict(format!(
                "tracked source mutated under verification for candidate {}; \
                 the evidence is invalid, never a pass",
                candidate.commit,
            )));
        }
        Ok(())
    }

    async fn prepare_integration(
        &self,
        request: IntegrationRequest,
    ) -> Result<IntegrationCandidate, AppError> {
        if request.candidates.is_empty() {
            return Err(AppError::Validation(
                "integration needs at least one sealed candidate".to_string(),
            ));
        }
        {
            let active = Self::lock(&self.active)?;
            if active.contains_key(&request.run) {
                return Err(AppError::Conflict(format!(
                    "run {} already has an in-flight integration; nodes serialize per run",
                    request.run,
                )));
            }
        }
        // Every candidate must still seal: a mutation under verification is
        // caught here before any merge, and only sealed bytes ever merge.
        for candidate in &request.candidates {
            self.verify_seal(candidate).await?;
        }
        // Copy the repo out of the sealed registry and release the guard
        // before any await: the std MutexGuard is not Send.
        let repo = {
            let sealed = Self::lock(&self.sealed)?;
            sealed
                .get(&request.candidates[0].commit)
                .map(|entry| entry.repo.clone())
                .ok_or_else(|| {
                    AppError::NotFound(format!(
                        "no sealed candidate {}",
                        request.candidates[0].commit
                    ))
                })?
        };
        let base = self.git.rev_parse_commit(&repo, &request.base).await?;
        let branch = format!(
            "central-integration/{}/{}",
            request.run,
            self.counter.fetch_add(1, Ordering::SeqCst)
        );
        let worktree = self.worktrees_dir.join(format!(
            "integration-{}-{}",
            request.run,
            self.counter.load(Ordering::SeqCst)
        ));
        self.git.create_branch(&repo, &branch, &base).await?;
        if let Err(error) = self
            .git
            .add_worktree_branch(&repo, &worktree, &branch)
            .await
        {
            let _ = self.git.delete_branch(&repo, &branch).await;
            return Err(error);
        }
        for candidate in &request.candidates {
            let short = &candidate.commit[..8.min(candidate.commit.len())];
            let message = format!("central: integrate {short}");
            if let Err(error) = self
                .git
                .merge_commit(&worktree, &candidate.commit, &message)
                .await
            {
                let _ = self.git.merge_abort(&worktree).await;
                let _ = self.git.remove_worktree(&repo, &worktree).await;
                let _ = std::fs::remove_dir_all(&worktree);
                let _ = self.git.delete_branch(&repo, &branch).await;
                return Err(AppError::Conflict(format!(
                    "integration of {} onto {base} conflicts ({error}); no out-of-plan \
                     conflict-resolution task is created — the leader proposes a way forward",
                    candidate.commit,
                )));
            }
        }
        let head = self.git.run(&worktree, &["rev-parse", "HEAD"]).await?;
        Self::lock(&self.active)?.insert(
            request.run,
            ActiveIntegration {
                branch: branch.clone(),
                worktree: worktree.clone(),
                repo: repo.clone(),
                candidate: head.clone(),
                ref_name: request.ref_name.clone(),
                base: base.clone(),
            },
        );
        Ok(IntegrationCandidate {
            run: request.run,
            branch,
            base,
            candidate: head,
            worktree: worktree.to_string_lossy().into_owned(),
            ref_name: request.ref_name,
        })
    }

    async fn promote(&self, request: PromoteRequest) -> Result<PromotionReceipt, AppError> {
        let active = Self::lock(&self.active)?
            .get(&request.candidate.run)
            .cloned()
            .ok_or_else(|| {
                AppError::Conflict(format!(
                    "no in-flight integration for run {}; prepare one first",
                    request.candidate.run,
                ))
            })?;
        if active.branch != request.candidate.branch
            || active.candidate != request.candidate.candidate
            || active.ref_name != request.candidate.ref_name
        {
            return Err(AppError::Conflict(format!(
                "integration candidate does not match the prepared one for run {}; \
                 a new candidate and new checks are required",
                request.candidate.run,
            )));
        }
        let actual_parent = self
            .git
            .read_ref(&active.repo, &request.candidate.ref_name)
            .await?;
        if let Err(error) = gate_promotion(
            &request.expected_parent,
            &actual_parent,
            &request.verification,
        ) {
            // The round is over: release the slot and drop the temporary
            // branch/worktree so a new candidate and new checks can start.
            // Nothing was mutated — the good ref never moved.
            self.finish_integration(request.candidate.run, &active)
                .await;
            return Err(error);
        }
        // Intent before mutation: flush the durable record, then CAS.
        let intent = PromotionIntent {
            intent_id: format!(
                "promote-{}-{}",
                request.candidate.run,
                self.counter.fetch_add(1, Ordering::SeqCst)
            ),
            run: request.candidate.run,
            repo_root: active.repo.to_string_lossy().into_owned(),
            ref_name: request.candidate.ref_name.clone(),
            base: active.base.clone(),
            candidate: active.candidate.clone(),
            expected_parent: request.expected_parent.clone(),
        };
        self.persist_intent(&intent)?;
        let after = match self
            .git
            .update_ref_cas(
                &active.repo,
                &request.candidate.ref_name,
                &active.candidate,
                &request.expected_parent,
            )
            .await
        {
            Ok(head) => head,
            Err(error) => {
                // The CAS refused: release the round but keep the persisted
                // intent (recovery reads it), then report the actual ref
                // instead of guessing.
                let actual = self
                    .git
                    .read_ref(&active.repo, &request.candidate.ref_name)
                    .await?;
                self.finish_integration(request.candidate.run, &active)
                    .await;
                return Err(AppError::Conflict(format!(
                    "good ref {} refused the CAS ({error}); it points at {actual} — \
                     a new candidate and new checks are required",
                    request.candidate.ref_name,
                )));
            }
        };
        self.finish_integration(request.candidate.run, &active)
            .await;
        Ok(PromotionReceipt {
            ref_name: request.candidate.ref_name.clone(),
            previous_head: request.expected_parent.clone(),
            new_head: after,
            candidate: active.candidate.clone(),
            intent_id: intent.intent_id.clone(),
        })
    }

    async fn recover_promotion(
        &self,
        run: RunId,
        ref_name: &str,
    ) -> Result<PromotionReceipt, AppError> {
        let intent = self.load_intent(run, ref_name)?;
        let repo = canonical_root(Path::new(&intent.repo_root))?;
        let actual = self.git.read_ref(&repo, ref_name).await?;
        Ok(central_application::integration::recover_receipt(
            &intent, &actual,
        ))
    }

    async fn remove_workspace(&self, workspace: &WorkspaceRef) -> Result<(), AppError> {
        let live = self.live_workspace(workspace)?;
        let worktree = PathBuf::from(&workspace.worktree);
        self.git.remove_worktree(&live.repo, &worktree).await?;
        if worktree.exists() {
            std::fs::remove_dir_all(&worktree).map_err(|error| {
                AppError::Storage(format!(
                    "cannot remove worktree dir {}: {error}",
                    worktree.display()
                ))
            })?;
        }
        Self::lock(&self.live)?.remove(&workspace.attempt_id);
        Ok(())
    }
}

impl GitWorkspaceManager {
    fn persist_intent(&self, intent: &PromotionIntent) -> Result<(), AppError> {
        let path = self.intent_path(intent.run, &intent.ref_name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| AppError::Storage(format!("cannot create intent dir: {error}")))?;
        }
        let text = serde_json::to_string(intent).map_err(|error| {
            AppError::Storage(format!("cannot encode promotion intent: {error}"))
        })?;
        let tmp = path.with_extension("json-part");
        std::fs::write(&tmp, text).map_err(|error| {
            AppError::Storage(format!("cannot stage promotion intent: {error}"))
        })?;
        std::fs::rename(&tmp, &path).map_err(|error| {
            let _ = std::fs::remove_file(&tmp);
            AppError::Storage(format!("cannot commit promotion intent: {error}"))
        })?;
        Ok(())
    }

    fn load_intent(&self, run: RunId, ref_name: &str) -> Result<PromotionIntent, AppError> {
        let path = self.intent_path(run, ref_name);
        let text = std::fs::read_to_string(&path).map_err(|_| {
            AppError::NotFound(format!(
                "no promotion intent for run {run} ref {ref_name}; nothing to recover"
            ))
        })?;
        serde_json::from_str(&text)
            .map_err(|error| AppError::Storage(format!("promotion intent is corrupt: {error}")))
    }

    /// Release the run's slot and delete the temporary branch and worktree.
    /// The intent file stays: it is the durable record recovery reads.
    async fn finish_integration(&self, run: RunId, active: &ActiveIntegration) {
        let _ = Self::lock(&self.active).map(|mut slot| slot.remove(&run));
        let _ = self
            .git
            .remove_worktree(&active.repo, &active.worktree)
            .await;
        if active.worktree.exists() {
            let _ = std::fs::remove_dir_all(&active.worktree);
        }
        let _ = self.git.delete_branch(&active.repo, &active.branch).await;
    }
}

/// A queued user commit: `central sync RUN --commit SHA` records the user's
/// commit for the daemon to pick up without ever touching the user worktree
/// — no checkout, no stash, no pull, only read-only probes (`cat-file -e`,
/// `status --porcelain`). Daemon-side pickup of the queue lands in P3/T6,
/// which owns the scan-loop wiring.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncIntent {
    pub run: RunId,
    pub commit: String,
    pub repository: String,
    pub dirty_fingerprint: String,
}

/// Queue the user's commit for `run`: verify the commit exists (read-only),
/// fingerprint the dirty state, and append one intent line. The worktree is
/// never mutated: callers can assert the porcelain status is identical
/// before and after.
pub fn queue_user_commit(
    state_dir: &Path,
    run: RunId,
    repository: &Path,
    commit: &str,
) -> Result<SyncIntent, AppError> {
    let repo = canonical_root(repository)?;
    let hooks_dir = state_dir.join("hooks");
    std::fs::create_dir_all(&hooks_dir)
        .map_err(|error| AppError::Storage(format!("cannot create owned hooks dir: {error}")))?;
    let argv = |args: &[&str]| -> Result<String, AppError> {
        let output = std::process::Command::new("git")
            .arg("-c")
            .arg(format!("core.hooksPath={}", hooks_dir.to_string_lossy()))
            .args(args)
            .current_dir(&repo)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_TERMINAL_PROMPT", "0")
            .stdin(std::process::Stdio::null())
            .output()
            .map_err(|error| AppError::Transport(format!("cannot spawn git: {error}")))?;
        if !output.status.success() {
            return Err(AppError::Storage(format!(
                "git {} failed: {}",
                args.join(" "),
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    };
    let dirty_before = argv(&["status", "--porcelain"])?;
    let sha = argv(&["rev-parse", "--verify", &format!("{commit}^{{commit}}")])?;
    let dirty_after = argv(&["status", "--porcelain"])?;
    let _ = dirty_before;
    let intent = SyncIntent {
        run,
        commit: sha,
        repository: repo.to_string_lossy().into_owned(),
        dirty_fingerprint: content_hash(dirty_after.as_bytes()).to_string(),
    };
    let queue_dir = state_dir.join("sync-queue");
    std::fs::create_dir_all(&queue_dir)
        .map_err(|error| AppError::Storage(format!("cannot create sync queue: {error}")))?;
    let line = serde_json::to_string(&intent)
        .map_err(|error| AppError::Storage(format!("cannot encode sync intent: {error}")))?;
    let path = queue_dir.join(format!("{run}.jsonl"));
    use std::io::Write as _;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|error| AppError::Storage(format!("cannot open sync queue: {error}")))?;
    writeln!(file, "{line}")
        .map_err(|error| AppError::Storage(format!("cannot queue sync intent: {error}")))?;
    file.sync_all()
        .map_err(|error| AppError::Storage(format!("cannot flush sync queue: {error}")))?;
    Ok(intent)
}
