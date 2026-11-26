use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{
    ActorId, ArtifactId, AttemptId, ContentHash, MissingUsage, PendingRevision, PlanDocumentV1,
    PlanHash, ProfileId, Revision, RunId, StaleCandidate, TaskId, TaskKind, Timestamp,
    UsageAuditEntry, UsageLimits, VerifySpec,
};

pub const RUN_STATE_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunControl {
    Running,
    Paused,
    Stopped,
    AwaitingResume,
}

/// Holds are independent of `RunControl`: resuming a run never clears them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum HoldReason {
    BudgetExceeded,
    UsageUnknown,
    PlanImpact,
    ResourceConflict,
}

/// Which agent role a run-level profile selects for. Workers run tasks;
/// the leader reviews parked work on explicit request. The role only
/// decides which profile resolves an attempt — both roles dispatch
/// through the same gateway with the same approval/cancel/reconcile
/// machinery, and usage ledgers never distinguish them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProfileRole {
    Worker,
    Leader,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus {
    Pending,
    Ready,
    Running,
    AwaitingVerification,
    Succeeded,
    Blocked,
    Failed,
    Cancelled,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AttemptStatus {
    Queued,
    Dispatching,
    Running,
    WaitingForApproval,
    WaitingForInput,
    Reconciling,
    UnknownOutcome,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum VerificationOutcome {
    /// The wire contract (`contracts/verification-v1.schema.json`) spells
    /// outcomes lowercase; stored events keep the PascalCase spelling, so
    /// both deserialize while only the canonical form is ever written.
    #[serde(alias = "pass")]
    Pass,
    #[serde(alias = "fail")]
    Fail,
    #[serde(alias = "blocked")]
    Blocked,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Observation {
    pub attempt_id: AttemptId,
    pub provider_epoch: String,
    pub dedup_key: String,
    pub outcome: Option<VerificationOutcome>,
    /// Human-readable harness note stored with the evidence (for
    /// example the `detail` of a reconcile verdict). Never machine
    /// parsed; `#[serde(default)]` keeps older snapshots decodable.
    #[serde(default)]
    pub detail: String,
}

impl Observation {
    /// A complete user-visible assistant message, separate from deltas,
    /// tool output, and commentary. The gateway supplies redacted text.
    pub fn assistant_reply(&self) -> Option<&str> {
        (self.dedup_key.starts_with("assistant.reply:") && !self.detail.trim().is_empty())
            .then_some(self.detail.as_str())
    }

    /// Evidence that one turn was sent for an attempt: keyed by the
    /// harness turn id so a later reconcile can correlate it. The
    /// dispatcher records this through `RecordObservation` right after a
    /// confirmed send; it never fabricates worker results.
    pub fn dispatch_probe(
        attempt_id: AttemptId,
        provider_epoch: impl Into<String>,
        turn_id: impl Into<String>,
    ) -> Self {
        Self {
            attempt_id,
            provider_epoch: provider_epoch.into(),
            dedup_key: turn_id.into(),
            outcome: None,
            detail: String::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlanStatus {
    Proposed,
    Approved,
    Superseded,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PlanRevision {
    pub hash: PlanHash,
    pub document: PlanDocumentV1,
    pub status: PlanStatus,
    pub proposed_by: ActorId,
    pub proposed_at: Timestamp,
    pub approved_by: Option<ActorId>,
    pub approved_at: Option<Timestamp>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Task {
    pub id: TaskId,
    pub plan_hash: PlanHash,
    pub kind: TaskKind,
    pub objective: String,
    pub status: TaskStatus,
    pub depends_on: Vec<TaskId>,
    pub data_depends_on: Vec<TaskId>,
    pub verify: VerifySpec,
    pub profile_id: Option<ProfileId>,
    pub max_repair_rounds: u8,
    pub repair_rounds_used: u8,
}

impl Task {
    /// Whether the task can accept a new attempt once the remaining
    /// gates (plan binding, dependencies, holds, budget) pass: it has
    /// no live attempt outcome yet, so it is `Pending` or `Ready`.
    pub fn is_ready(&self) -> bool {
        matches!(self.status, TaskStatus::Pending | TaskStatus::Ready)
    }
}

/// Why one leader turn was queued: ordinary discussion, explicit
/// planning, or revision of the pending plan.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscussionIntent {
    #[default]
    Discuss,
    Plan,
    Revise,
}

/// One user message and its isolated leader turn, in conversation order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaderDiscussion {
    pub attempt: AttemptId,
    #[serde(default)]
    pub workspace_read_only: bool,
    #[serde(default)]
    pub intent: DiscussionIntent,
    pub user: String,
    pub reply: Option<String>,
    pub proposal: Option<PlanHash>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attempt {
    pub id: AttemptId,
    pub task_id: TaskId,
    pub plan_hash: PlanHash,
    pub status: AttemptStatus,
    pub profile_id: Option<ProfileId>,
    pub created_at: Timestamp,
}

impl Attempt {
    /// The revision the attempt was queued under. Attempts never move
    /// revisions: a revision activates around them, and their results
    /// are quarantined with this binding intact.
    pub fn bound_plan(&self) -> &PlanHash {
        &self.plan_hash
    }
}

/// Where one dispatched attempt is executing, as reported by the harness.
/// Stored separately from [`Attempt`] so the attempt shape stays frozen:
/// the binding is evidence about the outside world, not task progress.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptSession {
    pub provider: String,
    pub instance_epoch: String,
    pub thread_id: String,
}

pub use crate::inbox::{InboxEntry, InboxStatus};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Artifact {
    pub id: ArtifactId,
    pub hash: ContentHash,
    pub kind: String,
    pub path: String,
    pub task_id: Option<TaskId>,
    pub attempt_id: Option<AttemptId>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageLedger {
    pub total_tokens: u64,
    pub total_cost_minor: Option<u64>,
    pub unknown: bool,
    /// Confirmed finals only. Gap resolutions and estimates never land here.
    #[serde(default)]
    pub observed_tokens: u64,
    /// Vendor estimates only; never converted into observed.
    #[serde(default)]
    pub estimated_tokens: u64,
    #[serde(default)]
    pub observed_cost_minor: u64,
    /// Dedup keys of every counted or parked report.
    #[serde(default)]
    pub seen_events: BTreeSet<String>,
    /// Per-stream cumulative high-water marks: replays and late lower
    /// figures never reduce the known totals.
    #[serde(default)]
    pub cumulative_high_water: BTreeMap<String, u64>,
    #[serde(default)]
    pub cumulative_cost_high_water: BTreeMap<String, u64>,
    /// Live cost with no final yet (keyed by live key). Ongoing cost,
    /// never a fabricated total.
    #[serde(default)]
    pub in_flight: BTreeMap<String, MissingUsage>,
    /// Finals that never arrived (keyed by dedup key). Non-empty keeps
    /// the ledger unknown until evidence or audited resolution.
    #[serde(default)]
    pub missing_final: BTreeMap<String, MissingUsage>,
    #[serde(default)]
    pub limits: UsageLimits,
    /// When the current limit window started (set by the first
    /// `SetLimits` that leaves it unset).
    #[serde(default)]
    pub window_started_at: Option<Timestamp>,
    /// Audited user gap resolutions, newest last.
    #[serde(default)]
    pub audit: Vec<UsageAuditEntry>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RunState {
    pub id: RunId,
    pub revision: Revision,
    /// Persisted snapshot schema version.
    pub version: u32,
    pub created: bool,
    pub goal: String,
    pub repository: String,
    pub control: RunControl,
    pub active_plan: Option<PlanHash>,
    pub pending_plan: Option<PlanHash>,
    pub plans: BTreeMap<PlanHash, PlanRevision>,
    pub tasks: BTreeMap<TaskId, Task>,
    pub attempts: BTreeMap<AttemptId, Attempt>,
    pub inbox: BTreeMap<String, InboxEntry>,
    /// Deduplicated harness observations (by `dedup_key`), newest last.
    /// Passive reconcile input: resolving inbox entries reads these, and
    /// the audit trail lives in events. Bounded by decision, not by callers.
    pub observations: Vec<Observation>,
    pub artifacts: BTreeMap<ArtifactId, Artifact>,
    pub usage: UsageLedger,
    pub holds: BTreeSet<HoldReason>,
    /// Verified outputs by task: the content hash the verifier accepted
    /// for that task's sealed candidate. Data dependents open only when
    /// their predecessor has an entry here (P3/T1 reads, P3/T2 writes).
    #[serde(default)]
    pub verified_outputs: BTreeMap<TaskId, ContentHash>,
    /// Exclusive named resources (`port:5432`, `db:test`) held by the
    /// owning attempt. Released only on a confirmed terminal attempt;
    /// unknown-outcome attempts keep their claims so no second attempt
    /// can take the resource while the first is unresolved.
    #[serde(default)]
    pub resource_claims: BTreeMap<String, AttemptId>,
    /// The exact input each attempt was queued with. Queueing never
    /// rewrites an existing entry: new input waits for a new attempt.
    #[serde(default)]
    pub attempt_inputs: BTreeMap<AttemptId, String>,
    /// Harness session bindings for dispatched attempts (reconcile input).
    #[serde(default)]
    pub attempt_sessions: BTreeMap<AttemptId, AttemptSession>,
    /// Disputes recorded per task by `DisputeVerification`. The application
    /// layer gates independent re-reviews on the approved allowance; the
    /// count here is the durable audit of that policy.
    #[serde(default)]
    pub verification_disputes: BTreeMap<TaskId, u8>,
    /// Pending repair base per task: the predecessor attempt whose
    /// candidate failed verification and may anchor one repair attempt.
    /// Written by a `Repair` verdict, consumed by `QueueRepairAttempt`.
    #[serde(default)]
    pub repair_base: BTreeMap<TaskId, AttemptId>,
    /// Safe-retry bookkeeping per attempt (P3/T3 owns it): how many
    /// `DefinitelyNotSent` retries the attempt already used.
    #[serde(default)]
    pub retry_used: BTreeMap<AttemptId, u32>,
    /// Earliest virtual-clock time the next safe retry may send. Queued
    /// attempts at or past this time are due; earlier ones wait.
    #[serde(default)]
    pub retry_not_before: BTreeMap<AttemptId, Timestamp>,
    /// Repair chain: each repair attempt links to the predecessor attempt
    /// whose candidate it repairs. Append-only; a repair never rewrites
    /// the attempt it chains from.
    #[serde(default)]
    pub attempt_predecessor: BTreeMap<AttemptId, AttemptId>,
    /// The immutable revision proposal awaiting user approval, if any.
    /// While it pends, [`RunState::revision_holds`] keeps the impacted
    /// tasks from queueing or dispatching new work; running attempts
    /// continue on their original binding.
    #[serde(default)]
    pub pending_revision: Option<PendingRevision>,
    /// Tasks held by the pending revision proposal: the impacted set
    /// (changed nodes, data descendants and leader semantic nodes).
    /// Queued attempts wait; nothing held is cancelled.
    #[serde(default)]
    pub revision_holds: BTreeSet<TaskId>,
    /// Results produced by attempts bound to a superseded revision,
    /// quarantined with their original provenance. Keyed by attempt;
    /// a late result inserts here without touching the active revision.
    #[serde(default)]
    pub stale_candidates: BTreeMap<AttemptId, StaleCandidate>,
    /// Run-wide default profile for worker attempts without a pinned
    /// profile. Set explicitly by the owner; unpinned tasks dispatch on
    /// this before any fallback, but it never overrides a task pin.
    #[serde(default)]
    pub worker_profile: Option<ProfileId>,
    /// Run-wide profile for leader review attempts, selected the same
    /// way as the worker profile. Leader dispatches use only this
    /// profile (or defer when unset); they never fall back to a worker
    /// profile, so a leader review visibly runs as the chosen agent.
    #[serde(default)]
    pub leader_profile: Option<ProfileId>,
    /// Attempts that are leader reviews, not worker candidates. They
    /// dispatch through the run's leader profile, they never seal a
    /// worker candidate, and their output returns via handover — never
    /// through worker-result import.
    #[serde(default)]
    pub leader_attempts: BTreeSet<AttemptId>,
    #[serde(default)]
    pub leader_discussion: Vec<LeaderDiscussion>,
}

impl RunState {
    pub fn new(id: RunId) -> Self {
        Self {
            id,
            revision: Revision(0),
            version: RUN_STATE_VERSION,
            created: false,
            goal: String::new(),
            repository: String::new(),
            control: RunControl::AwaitingResume,
            active_plan: None,
            pending_plan: None,
            plans: BTreeMap::new(),
            tasks: BTreeMap::new(),
            attempts: BTreeMap::new(),
            inbox: BTreeMap::new(),
            observations: Vec::new(),
            artifacts: BTreeMap::new(),
            usage: UsageLedger::default(),
            holds: BTreeSet::new(),
            verified_outputs: BTreeMap::new(),
            resource_claims: BTreeMap::new(),
            attempt_inputs: BTreeMap::new(),
            attempt_sessions: BTreeMap::new(),
            verification_disputes: BTreeMap::new(),
            repair_base: BTreeMap::new(),
            retry_used: BTreeMap::new(),
            retry_not_before: BTreeMap::new(),
            attempt_predecessor: BTreeMap::new(),
            pending_revision: None,
            revision_holds: BTreeSet::new(),
            stale_candidates: BTreeMap::new(),
            worker_profile: None,
            leader_profile: None,
            leader_attempts: BTreeSet::new(),
            leader_discussion: Vec::new(),
        }
    }

    pub fn is_created(&self) -> bool {
        self.created
    }
}
