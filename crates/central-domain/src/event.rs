use serde::{Deserialize, Serialize};

use crate::{ActorId, AttemptId, InboxKind, InboxOutcome, PlanHash, RunId, TaskId, Timestamp};

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
