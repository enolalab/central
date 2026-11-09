use serde::{Deserialize, Serialize};

use crate::{
    quality::QualityDecision, revision::ReuseDecision, ActorId, AttemptId, ContentHash, InboxKind,
    InboxOutcome, PlanHash, ProfileId, RunId, TaskId, Timestamp, VerificationOutcome,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Event {
    RunCreated {
        run_id: RunId,
        goal: String,
        repository: String,
        by: ActorId,
        at: Timestamp,
    },
    PlanProposed {
        run_id: RunId,
        plan_hash: PlanHash,
        by: ActorId,
        at: Timestamp,
    },
    PlanApproved {
        run_id: RunId,
        plan_hash: PlanHash,
        by: ActorId,
        at: Timestamp,
    },
    RunPaused {
        run_id: RunId,
        by: ActorId,
        at: Timestamp,
    },
    RunStopped {
        run_id: RunId,
        by: ActorId,
        at: Timestamp,
    },
    RunResumed {
        run_id: RunId,
        by: ActorId,
        at: Timestamp,
    },
    /// One harness request persisted before any UI notice. The event names
    /// the canonical key only; the full entry (with its redacted preview)
    /// lives in the snapshot.
    InboxRequestRecorded {
        run_id: RunId,
        key: String,
        kind: InboxKind,
        by: ActorId,
        at: Timestamp,
    },
    /// A pending entry was answered; the response travels as an outbox
    /// intent. The event carries no answer payload: secrets stay memory-only.
    InboxAnswered {
        run_id: RunId,
        key: String,
        outcome: InboxOutcome,
        by: ActorId,
        at: Timestamp,
    },
    /// An answered entry was confirmed by a harness observation.
    InboxResolved {
        run_id: RunId,
        key: String,
        by: ActorId,
        at: Timestamp,
    },
    /// One harness observation persisted; passive reconcile input.
    ObservationRecorded {
        run_id: RunId,
        attempt_id: AttemptId,
        dedup_key: String,
        at: Timestamp,
    },
    /// One attempt queued for a task; carries no input (inputs live in
    /// the snapshot, never in the journal).
    AttemptQueued {
        run_id: RunId,
        attempt_id: AttemptId,
        task: TaskId,
        by: ActorId,
        at: Timestamp,
    },
    /// A queued attempt started dispatching, before any agent call.
    AttemptDispatching {
        run_id: RunId,
        attempt_id: AttemptId,
        by: ActorId,
        at: Timestamp,
    },
    /// A dispatching attempt is confirmed running in a harness session.
    AttemptRunning {
        run_id: RunId,
        attempt_id: AttemptId,
        by: ActorId,
        at: Timestamp,
    },
    /// A dispatch outcome is ambiguous; the attempt holds its claims.
    AttemptUnknown {
        run_id: RunId,
        attempt_id: AttemptId,
        by: ActorId,
        at: Timestamp,
    },
    /// A definitely-unsent attempt returned to queued.
    AttemptRequeued {
        run_id: RunId,
        attempt_id: AttemptId,
        by: ActorId,
        at: Timestamp,
    },
    /// A refused attempt reached its terminal state; claims released.
    AttemptFailed {
        run_id: RunId,
        attempt_id: AttemptId,
        by: ActorId,
        at: Timestamp,
    },
    /// A failed task reopened for recovery after a refused or
    /// retry-exhausted send.
    TaskReopened {
        run_id: RunId,
        task: TaskId,
        by: ActorId,
        at: Timestamp,
    },
    /// One safe retry deferred to a virtual-clock deadline.
    AttemptRetryDeferred {
        run_id: RunId,
        attempt_id: AttemptId,
        not_before: Timestamp,
        retries_used: u32,
        by: ActorId,
        at: Timestamp,
    },
    /// The run's shared limits were set (actor recorded for audit).
    LimitsSet {
        run_id: RunId,
        by: ActorId,
        at: Timestamp,
    },
    /// One usage report folded into the shared ledger.
    UsageReconciled {
        run_id: RunId,
        dedup_key: String,
        by: ActorId,
        at: Timestamp,
    },
    /// One missing-final entry resolved by explicit user decision.
    UsageGapResolved {
        run_id: RunId,
        missing_key: String,
        by: ActorId,
        at: Timestamp,
    },
    /// A quota/auth fallback: the old attempt confirmed stopped, the new
    /// attempt continuing on the fallback profile. This event is the
    /// durable user notification.
    FallbackTaken {
        run_id: RunId,
        task: TaskId,
        old_attempt: AttemptId,
        new_attempt: AttemptId,
        profile: ProfileId,
        by: ActorId,
        at: Timestamp,
    },
    /// One verifier verdict was judged: what was judged (exact subject),
    /// how it routed, and where the task stands. Append-only: later
    /// verdicts add events, never rewrite this one.
    VerificationSubmitted {
        run_id: RunId,
        task: TaskId,
        attempt_id: AttemptId,
        outcome: VerificationOutcome,
        decision: QualityDecision,
        by: ActorId,
        at: Timestamp,
    },
    /// One verdict was disputed with a reason. The task stays parked for
    /// verification; the count audits the approved-allowance policy. The
    /// routing answer is always [`QualityDecision::Disputed`]: the dispute
    /// opens one independent re-review inside the approved allowance, else
    /// the user decides (planned at the application layer).
    VerificationDisputed {
        run_id: RunId,
        task: TaskId,
        reason: String,
        disputes_used: u8,
        decision: QualityDecision,
        by: ActorId,
        at: Timestamp,
    },
    /// One repair attempt queued, linked to the predecessor attempt whose
    /// candidate failed verification.
    RepairAttemptQueued {
        run_id: RunId,
        attempt_id: AttemptId,
        task: TaskId,
        predecessor: AttemptId,
        by: ActorId,
        at: Timestamp,
    },
    /// An immutable revision proposal committed: the base it revises,
    /// the proposed hash and every task it holds, in stable order.
    PlanRevisionProposed {
        run_id: RunId,
        base_plan: PlanHash,
        plan_hash: PlanHash,
        impacted: Vec<TaskId>,
        by: ActorId,
        at: Timestamp,
    },
    /// A revision proposal activated by the user: the base it replaced
    /// and every acceptance its data changes invalidated, in stable order.
    PlanRevisionApproved {
        run_id: RunId,
        base_plan: PlanHash,
        plan_hash: PlanHash,
        invalidated: Vec<TaskId>,
        by: ActorId,
        at: Timestamp,
    },
    /// A result from a superseded revision quarantined with its
    /// original provenance. The active revision is untouched; reuse
    /// waits for an explicit assessment.
    StaleCandidateRecorded {
        run_id: RunId,
        attempt_id: AttemptId,
        task: TaskId,
        bound_plan: PlanHash,
        candidate: ContentHash,
        by: ActorId,
        at: Timestamp,
    },
    /// One quarantined result assessed: what it may become under the
    /// active revision. History is append-only; the assessment never
    /// rewrites the quarantined provenance.
    ReuseAssessed {
        run_id: RunId,
        attempt_id: AttemptId,
        decision: ReuseDecision,
        by: ActorId,
        at: Timestamp,
    },
}

/// Intended side effects for the application layer to persist as outbox
/// intents. Domain decision itself performs zero IO.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Effect {
    CancelAttempt {
        attempt_id: AttemptId,
    },
    /// Send one external answer to the harness. A `Secret` payload carries a
    /// random memory-only reference, never the credential, so the durable
    /// intent is safe to persist; the application resolves the reference at
    /// dispatch and sends nothing when the vault no longer holds it.
    RespondToRequest {
        key: crate::InboxKey,
        payload: crate::AnswerPayload,
        scope: crate::AnswerScope,
        attempt_id: Option<AttemptId>,
    },
}
