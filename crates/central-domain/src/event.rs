use serde::{Deserialize, Serialize};

use crate::{ActorId, AttemptId, PlanHash, RunId, Timestamp};

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
}

/// Intended side effects for the application layer to persist as outbox
/// intents. Domain decision itself performs zero IO.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Effect {
    CancelAttempt { attempt_id: AttemptId },
}
