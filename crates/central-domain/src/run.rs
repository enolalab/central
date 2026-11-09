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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum InboxStatus {
    Pending,
    Answered,
    Resolved,
    Stale,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InboxEntry {
    pub key: String,
    pub attempt_id: Option<AttemptId>,
    pub status: InboxStatus,
}

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
    pub artifacts: BTreeMap<ArtifactId, Artifact>,
    pub usage: UsageLedger,
    pub holds: BTreeSet<HoldReason>,
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
            artifacts: BTreeMap::new(),
            usage: UsageLedger::default(),
            holds: BTreeSet::new(),
        }
    }

    pub fn is_created(&self) -> bool {
        self.created
    }
}
