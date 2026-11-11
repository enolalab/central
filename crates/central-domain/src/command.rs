use serde::{Deserialize, Serialize};

use crate::{
    quality::VerificationReport, ActorId, AnswerPayload, AttemptId, AttemptSession, CommandId,
    ContentHash, InboxKey, InboxRequest, Observation, PlanDocumentV1, PlanHash, ProfileId,
    ReuseBindings, ReuseDecision, Revision, TaskId, Timestamp, UsageLimits, UsageObservation,
};

/// Internal identity kind. The actor is derived from auth/session mapping,
/// never trusted from a request body claiming to be one of these.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActorKind {
    User,
    Leader,
    Worker,
    Verifier,
    System,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Actor {
    pub id: ActorId,
    pub kind: ActorKind,
}

impl Actor {
    pub fn user(id: ActorId) -> Self {
        Self {
            id,
            kind: ActorKind::User,
        }
    }

    pub fn system(id: ActorId) -> Self {
        Self {
            id,
            kind: ActorKind::System,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Command {
    CreateRun {
        goal: String,
        repository: String,
    },
    ProposePlan {
        plan: PlanDocumentV1,
    },
    ApprovePlan {
        plan_hash: PlanHash,
    },
    Pause,
    Stop,
    Resume,
    /// Persist one normalized harness request before any UI notice. Replay
    /// of the same request is a duplicate, not a second entry. Recording
    /// never triggers inference.
    RecordExternalRequest {
        request: InboxRequest,
    },
    /// Persist one harness observation; passively resolves the Answered
    /// entries of the same attempt. Never fabricates answers.
    RecordObservation {
        observation: Observation,
    },
    /// Answer a pending question. Secret answers carry a random
    /// `VolatileAnswerRef`, never the credential.
    AnswerQuestion {
        key: InboxKey,
        preview_hash: ContentHash,
        payload: AnswerPayload,
    },
    /// Approve a pending action approval. Plan hashes are not inbox keys:
    /// plan approval goes through `ApprovePlan` only.
    ApproveAction {
        key: InboxKey,
        preview_hash: ContentHash,
    },
    /// Decline a pending action approval with a user reason.
    DeclineAction {
        key: InboxKey,
        preview_hash: ContentHash,
        reason: String,
    },
    /// Queue one implementation attempt for a ready task. The scheduler
    /// (never the harness) issues this after the DAG, control, holds,
    /// resource and concurrency gates pass. `max_parallel` travels with
    /// the command so the decision can enforce the bound on fresh state
    /// under the commit CAS, closing the race between concurrent ticks.
    QueueAttempt {
        attempt: AttemptId,
        task: TaskId,
        input: String,
        resources: Vec<String>,
        max_parallel: usize,
    },
    /// Mark a queued attempt as dispatching before any agent call, so a
    /// crash between the call and its receipt reconciles to unknown
    /// instead of silently duplicating the send.
    StartDispatch {
        attempt: AttemptId,
    },
    /// The harness accepted the turn: the attempt is running and bound
    /// to the reported session for later reconciliation.
    AttemptRunning {
        attempt: AttemptId,
        session: AttemptSession,
    },
    /// The send outcome is ambiguous: hold descendants and resources,
    /// never resend blindly.
    AttemptUnknown {
        attempt: AttemptId,
    },
    /// The send definitely never happened: back to queued for retry.
    RequeueAttempt {
        attempt: AttemptId,
    },
    /// The harness definitively refused the turn: terminal, releases
    /// the attempt's resource claims. The task parks as failed too;
    /// recovery is explicit through `ReopenTask` (the retry policy owns
    /// reopening), so a refused send is never silently dropped and never
    /// auto-retried.
    AttemptFailed {
        attempt: AttemptId,
    },
    /// Reopen a task parked as failed by a refused (or retry-exhausted)
    /// send so a retry-policy or user-driven recovery can queue a new
    /// attempt. Refused sends stay recoverable before the P3 gate closes.
    ReopenTask {
        task: TaskId,
    },
    /// Defer one safe retry of a definitely-unsent attempt to a
    /// virtual-clock deadline. Records the retry against the attempt's
    /// bounded budget; an exhausted budget parks the attempt as failed
    /// (recoverable via `ReopenTask`) instead of storming. `max_retries`
    /// travels with the command so the decision enforces the
    /// deployment-configured bound on fresh state under the commit CAS,
    /// like `max_parallel` on `QueueAttempt` — the domain never
    /// re-applies a hardcoded default over it.
    DeferRetry {
        attempt: AttemptId,
        not_before: Timestamp,
        /// Policy bound carried with the command (P3). Command envelopes
        /// written before P3 lack it; the default is the hardcoded bound
        /// those envelopes were decided under (`MAX_RETRIES_DEFAULT` = 3),
        /// so replay means exactly what the writer meant. (P4 replay-compat.)
        #[serde(default = "default_defer_max_retries")]
        max_retries: u32,
    },
    /// Set the run's shared count/token/time limits (plus the optional
    /// user-configured monetary estimate cap). User only; the actor is
    /// recorded as the limits' author for audit.
    SetLimits {
        limits: UsageLimits,
    },
    /// Fold one usage report into the shared ledger: observed totals,
    /// estimates, live in-flight cost, or a missing final. User only.
    ReconcileUsage {
        observation: UsageObservation,
    },
    /// Resolve one missing-final entry by explicit user decision, with
    /// an audit note. Releases the hold without converting anything
    /// estimated or missing into observed usage. User only.
    ResolveUsageGap {
        missing_key: String,
        note: String,
    },
    /// Record a profile-fallback decision durably: which quota/auth-hit
    /// attempt stopped, which new attempt continues, and on what
    /// profile. The notification readers (CLI/UI) consume this event;
    /// the profile itself travels frozen with the new attempt.
    NoteFallback {
        task: TaskId,
        old_attempt: AttemptId,
        new_attempt: AttemptId,
        profile_id: ProfileId,
        reason: String,
    },
    /// Judge one independent verifier verdict against a task awaiting
    /// verification. Only a verifier (or the user, for a same-session
    /// override) may submit; the leader can never force a pass. Pass
    /// succeeds the task on the exact sealed subject, fail retries while
    /// repair rounds remain then escalates to the leader, blocked records
    /// `Blocked` without consuming a round. Criteria change only through
    /// an approved plan revision — a report naming other criteria is
    /// stale. History is append-only: this never rewrites a verdict.
    SubmitVerification {
        task: TaskId,
        attempt: AttemptId,
        report: VerificationReport,
    },
    /// Dispute a verdict awaiting verification. Records the objection for
    /// an independent re-review of the same candidate inside the approved
    /// allowance (otherwise the user decides); the task stays parked for
    /// verification. A dispute never reviews the previous verifier's
    /// report, so no verifier-of-verifier chain can form.
    DisputeVerification {
        task: TaskId,
        reason: String,
    },
    /// Queue one quality-repair attempt linked to the predecessor attempt
    /// whose candidate failed verification. Same scheduling gate as
    /// `QueueAttempt`, plus the repair base the failed verdict recorded:
    /// the predecessor must match, and the base is consumed, so a repair
    /// always chains from its verdict and never re-queues silently.
    QueueRepairAttempt {
        attempt: AttemptId,
        task: TaskId,
        predecessor: AttemptId,
        input: String,
        resources: Vec<String>,
        max_parallel: usize,
    },
    /// Propose an immutable revision of the active plan. The proposal
    /// carries the full replacement document, the reason, an optional
    /// criteria explanation (mandatory when verification criteria move)
    /// and leader-supplied semantic nodes, which can only expand the
    /// computed impact. Committing holds the impacted queued
    /// descendants; running attempts continue on their old binding.
    /// Leader or user; never the harness.
    ProposeRevision {
        plan: PlanDocumentV1,
        reason: String,
        criteria_explanation: Option<String>,
        semantic_affected: Vec<TaskId>,
    },
    /// Activate the pending revision proposal. User only; the target
    /// hash must be the pending revision and the envelope revision
    /// must be current, so a stale tab can never activate a revision
    /// the user did not review. Changed data inputs invalidate
    /// acceptances (verification first, not re-implementation);
    /// order-only changes keep data evidence.
    ApproveRevision {
        plan_hash: PlanHash,
    },
    /// Conclude the reuse assessment of one quarantined stale result.
    /// `current` names the bindings the decision was made against.
    /// `ReuseUnchangedEvidence` is refused unless every binding still
    /// matches; anything else records the routing (verify again,
    /// repair-chain, or discard) without touching new-plan evidence.
    /// User or leader; never the harness.
    AssessReuse {
        attempt: AttemptId,
        decision: ReuseDecision,
        current: ReuseBindings,
    },
    /// Durably record one worker result (P4): the attempt completed with
    /// the sealed candidate and its task parks in `AwaitingVerification`.
    /// The application pre-seals via `seal_candidate` (membership,
    /// containment and hash binding); the domain enforces the state
    /// transition, the actor gate and the criteria binding. Recording
    /// never marks a task `Succeeded` — only an independent verifier
    /// verdict via `SubmitVerification` can do that. Worker role (agent
    /// bridge scoped to the attempt) or user (owner import).
    RecordWorkerResult {
        attempt: AttemptId,
        input_hash: ContentHash,
        candidate_hash: ContentHash,
        criteria_hash: ContentHash,
    },
}

fn default_defer_max_retries() -> u32 {
    crate::MAX_RETRIES_DEFAULT
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CommandEnvelope {
    pub id: CommandId,
    pub expected_revision: Option<Revision>,
    pub command: Command,
}
