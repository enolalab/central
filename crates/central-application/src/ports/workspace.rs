//! Workspace port (P3 Task 5): Git workspaces, sealed candidates and
//! incremental integration.
//!
//! One detached worktree per attempt; the source candidate commit is
//! immutable once sealed; promotion moves the good ref only by
//! compare-and-swap after passing checks. Dirty input enters only through
//! an explicit selected snapshot or commit — never an automatic stash.

use async_trait::async_trait;
use central_domain::{AttemptId, ContentHash, PlanHash, RunId};
use serde::{Deserialize, Serialize};

use crate::contracts::AppError;

/// What to snapshot: a repo, the commit to build from, and the exact dirty
/// paths to include. An empty selection means a clean snapshot of the
/// commit; unlisted dirty files never enter the workspace.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotRequest {
    pub repo_root: String,
    pub commit: String,
    pub selected_dirty_paths: Vec<String>,
}

/// One explicitly selected dirty file with the bytes that were snapshotted.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectedFile {
    pub path: String,
    pub hash: ContentHash,
}

/// The immutable input one worktree is built from: the verified commit, its
/// tree, the selected dirty bytes, and the hash binding them together.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputSnapshot {
    pub repo_root: String,
    pub commit: String,
    pub tree: String,
    pub selected: Vec<SelectedFile>,
    pub snapshot_hash: ContentHash,
}

/// Build one detached worktree for one attempt from one snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceRequest {
    pub attempt_id: AttemptId,
    pub input: InputSnapshot,
}

/// A live, manager-owned detached worktree. Cleanup is explicit and
/// ownership-checked; dropping this value cleans up nothing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceRef {
    pub attempt_id: AttemptId,
    pub worktree: String,
    pub base_commit: String,
}

/// An immutable sealed source candidate: the commit, its tree and its
/// parent. `verify_seal` re-derives the tree: a tracked-source write after
/// sealing invalidates the evidence, never a pass.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandidateRef {
    pub attempt_id: AttemptId,
    pub commit: String,
    pub tree: String,
    pub parent: String,
}

/// Serialize approved integration nodes per run: at most one active
/// integration per run; the temporary branch starts at `base` and merges
/// exactly `candidates`. The good ref moves only through [`promote`].
///
/// [`promote`]: WorkspaceManager::promote
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntegrationRequest {
    pub run: RunId,
    pub base: String,
    pub candidates: Vec<CandidateRef>,
    pub approved_plan: PlanHash,
    pub ref_name: String,
}

/// One prepared integration: a temporary branch and its worktree, ready for
/// baseline + affected + acceptance checks. Merging never resolves
/// conflicts by itself: a conflict aborts and routes to the leader.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntegrationCandidate {
    pub run: RunId,
    pub branch: String,
    pub base: String,
    pub candidate: String,
    pub worktree: String,
    pub ref_name: String,
}

/// The check verdict promotion requires. `passed` is false unless baseline,
/// affected and acceptance all passed on the exact candidate commit.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerificationSummary {
    pub passed: bool,
    pub baseline_ok: bool,
    pub affected_ok: bool,
    pub acceptance_ok: bool,
    pub evidence: Vec<String>,
}

/// One approved program+argv pair for a mechanical integration check.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovedCheck {
    pub program: String,
    #[serde(default)]
    pub args: Vec<String>,
}

/// One mechanical integration round, end to end: prepare the temporary
/// branch from pre-sealed candidates, run the approved checks, and
/// promote the good ref by compare-and-swap on pass. Callers seal first
/// (`seal_live`): the registry proves each candidate is an immutable
/// sealed object, and `prepare` re-verifies the tree before merging.
/// `expected_parent: None` reads the current ref first (the CAS still
/// guards the read-then-move window); a caller-supplied parent turns a
/// concurrent move into a hard conflict instead.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutoIntegrationRequest {
    pub run: RunId,
    pub candidates: Vec<CandidateRef>,
    pub base: String,
    pub ref_name: String,
    pub expected_parent: Option<String>,
    pub approved_plan: PlanHash,
    pub baseline: Vec<ApprovedCheck>,
    pub affected: Vec<ApprovedCheck>,
    pub acceptance: Vec<ApprovedCheck>,
}

/// The durable outcome of one mechanical integration round: the sealed
/// candidate, the evidence the checks produced, and the CAS receipt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutoIntegrationReport {
    pub candidate: String,
    pub worktree: String,
    pub evidence: Vec<crate::ports::checks::CheckEvidence>,
    pub summary: VerificationSummary,
    pub receipt: PromotionReceipt,
}

/// Move the good ref to the integration candidate: only when the checks
/// passed, and only when the ref still points at `expected_parent`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromoteRequest {
    pub candidate: IntegrationCandidate,
    pub expected_parent: String,
    pub verification: VerificationSummary,
}

/// The durable intent persisted before the Git mutation. The mutation only
/// runs after this record is flushed; a later crash recovers the outcome
/// from this intent plus the actual ref head.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromotionIntent {
    pub intent_id: String,
    pub run: RunId,
    pub repo_root: String,
    pub ref_name: String,
    pub base: String,
    pub candidate: String,
    pub expected_parent: String,
}

/// Proof the good ref moved: the previous head, the new head and the intent
/// that was persisted before the Git mutation. A lost receipt is recovered
/// by inspecting the actual ref/object IDs, never by re-mutating.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromotionReceipt {
    pub ref_name: String,
    pub previous_head: String,
    pub new_head: String,
    pub candidate: String,
    pub intent_id: String,
}

/// Git workspaces and incremental integration. Every signature is async and
/// returns `Result<_, AppError>` per the workspace contract.
#[async_trait]
pub trait WorkspaceManager: Send + Sync {
    /// Read-only snapshot of a commit plus explicitly selected dirty bytes.
    /// Never stashes, checks out, or otherwise touches the user worktree.
    async fn snapshot(&self, request: SnapshotRequest) -> Result<InputSnapshot, AppError>;
    /// Create one detached worktree for one attempt. A second worktree for
    /// the same attempt conflicts; old workers are never rebased.
    async fn create(&self, request: WorkspaceRequest) -> Result<WorkspaceRef, AppError>;
    /// Commit the worktree as the immutable source candidate for the attempt.
    async fn seal(&self, workspace: &WorkspaceRef) -> Result<CandidateRef, AppError>;
    /// Re-derive the sealed tree: a tracked-source mutation after sealing
    /// conflicts (evidence invalid), verifier scratch outside the worktree
    /// leaves the seal valid. This is the tracked-source-mutation detector
    /// carried over from P3/T2: the git-level signal behind the
    /// `verifier_wrote_tracked_source` / `tainted_evidence` application gate.
    async fn verify_seal(&self, candidate: &CandidateRef) -> Result<(), AppError>;
    /// Serialize one integration per run onto a temporary branch. Merge
    /// conflicts abort with `Conflict` for the leader; no out-of-plan
    /// conflict-resolution task is ever created here.
    async fn prepare_integration(
        &self,
        request: IntegrationRequest,
    ) -> Result<IntegrationCandidate, AppError>;
    /// Persist the intent, then move the good ref by compare-and-swap.
    /// Failed checks refuse without touching the ref; a stale parent
    /// conflicts and the ref stays; recovery reads the actual ref.
    async fn promote(&self, request: PromoteRequest) -> Result<PromotionReceipt, AppError>;
    /// Recover the durable outcome from the intent plus the actual ref and
    /// object IDs: committed or not, without mutating anything.
    async fn recover_promotion(
        &self,
        run: RunId,
        ref_name: &str,
    ) -> Result<PromotionReceipt, AppError>;
    /// Latest user-synced commit for `(run, repo)` from the manager's sync
    /// queue, if any. `None` means nobody synced this repo for this run.
    async fn latest_synced_commit(
        &self,
        run: RunId,
        repo: &str,
    ) -> Result<Option<String>, AppError>;
    /// Explicit, ownership-checked worktree removal. Unknown or foreign
    /// paths refuse; processes spawned for Git ops are reaped by the call.
    async fn remove_workspace(&self, workspace: &WorkspaceRef) -> Result<(), AppError>;
    /// Seal the attempt's live worktree as the immutable source
    /// candidate. The worktree must belong to the attempt (tracked live
    /// by the manager); scratch-backed or unknown attempts refuse instead
    /// of inventing a candidate.
    async fn seal_live(&self, attempt: AttemptId) -> Result<CandidateRef, AppError>;
    /// Run one fully mechanical integration round: seal the attempt's live
    /// worktree, prepare the temporary branch, run the approved checks, and
    /// promote the good ref by compare-and-swap on pass. Fails closed on
    /// every step: an unsealed worktree, a merge conflict, a failed check
    /// or a stale parent never moves the good ref.
    async fn auto_integrate(
        &self,
        request: AutoIntegrationRequest,
    ) -> Result<AutoIntegrationReport, AppError>;
}
