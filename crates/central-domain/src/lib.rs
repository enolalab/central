pub mod command;
pub mod decision;
pub mod dependency;
pub mod error;
pub mod event;
pub mod ids;
pub mod inbox;
pub mod plan;
pub mod profile;
pub mod run;
pub mod scheduling;

pub use command::{Actor, ActorKind, Command, CommandEnvelope};
pub use decision::{decide, decide_envelope, Decision};
pub use dependency::{
    blocking_dependencies, dependencies_of, dependency_ready, verified_output, DependencyKind,
    TaskDependency,
};
pub use error::DomainError;
pub use event::{Effect, Event};
pub use ids::{
    ActorId, ArtifactId, AttemptId, CommandId, ContentHash, IntentId, PlanHash, ProfileId,
    Revision, RunId, TaskId, Timestamp,
};
pub use inbox::{
    canonical_key, preview_hash_for, truncate_preview, AnswerPayload, AnswerScope, InboxEntry,
    InboxKey, InboxKind, InboxOutcome, InboxRequest, InboxStatus, VolatileAnswerRef,
    MAX_PREVIEW_CHARS,
};
pub use plan::{plan_hash, validate_plan, PlanDocumentV1, PlanTaskV1, TaskKind, VerifySpec};
pub use profile::{
    apply_layers, compute_profile_hash, CapabilitySupport, Enforcement, ProfileError,
    ProfileLayers, ProfilePatch, ResolvedProfile,
};
pub use run::{
    Artifact, Attempt, AttemptSession, AttemptStatus, HoldReason, Observation, PlanRevision,
    PlanStatus, RunControl, RunState, Task, TaskStatus, UsageLedger, VerificationOutcome,
};
pub use scheduling::{
    covering_attempt, eligible_tasks, eligible_tasks_ignoring_gate, is_confirmed_terminal,
    is_live_status, live_attempt_count, release_settled_claims,
};
