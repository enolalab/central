pub mod budget;
pub mod command;
pub mod decision;
pub mod dependency;
pub mod error;
pub mod event;
pub mod ids;
pub mod inbox;
pub mod plan;
pub mod profile;
pub mod quality;
pub mod retry;
pub mod revision;
pub mod run;
pub mod scheduling;

pub use budget::{
    admission, apply_usage, budget_exceeded, capped_metric, retry_used, sync_budget_holds,
    usage_unknown_hold, Admission, MissingUsage, UsageAuditEntry, UsageFinality, UsageLimits,
    UsageObservation, WorkRequest, WorkRole,
};
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
pub use plan::{
    plan_hash, validate_plan, IntegrationCheck, IntegrationSpec, PlanDocumentV1, PlanTaskV1,
    TaskKind, VerifySpec,
};
pub use profile::{
    apply_layers, compute_profile_hash, CapabilitySupport, Enforcement, ProfileError,
    ProfileLayers, ProfilePatch, ResolvedProfile,
};
pub use quality::{
    check_report, criteria_hash_for, decide_quality, QualityDecision, VerificationReport,
};
pub use retry::{
    classify_rejection, failure_for_rejection, next_retry_delay, Failure, RejectionKind,
    RetryPolicy, DEFAULT_DELAYS_MS, MAX_RETRIES_DEFAULT,
};
pub use revision::{
    active_plan_hash, assess_impact, assess_reuse, criteria_changed, describe_criteria_impact,
    CriteriaImpact, ImpactSet, PendingRevision, ReuseBindings, ReuseDecision, StaleCandidate,
};
pub use run::{
    Artifact, Attempt, AttemptSession, AttemptStatus, DiscussionIntent, HoldReason,
    LeaderDiscussion, Observation, PlanRevision, PlanStatus, ProfileRole, RunControl, RunState,
    Task, TaskStatus, UsageLedger, VerificationOutcome,
};
pub use scheduling::{
    covering_attempt, eligible_tasks, eligible_tasks_ignoring_gate, is_confirmed_terminal,
    is_live_status, live_attempt_count, release_settled_claims, render_input,
};
