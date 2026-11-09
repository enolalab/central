use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{
    ActorId, ArtifactId, AttemptId, ContentHash, PlanDocumentV1, PlanHash, ProfileId, Revision,
    RunId, TaskId, TaskKind, Timestamp, VerifySpec,
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
    Pass,
    Fail,
    Blocked,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Observation {
    pub attempt_id: AttemptId,
    pub provider_epoch: String,
    pub dedup_key: String,
    pub outcome: Option<VerificationOutcome>,
}

impl Observation {
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attempt {
    pub id: AttemptId,
    pub task_id: TaskId,
    pub plan_hash: PlanHash,
    pub status: AttemptStatus,
    pub profile_id: Option<ProfileId>,
    pub created_at: Timestamp,
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
        }
    }

    pub fn is_created(&self) -> bool {
        self.created
    }
}
