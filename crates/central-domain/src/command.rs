use serde::{Deserialize, Serialize};

use crate::{
    ActorId, AnswerPayload, AttemptId, AttemptSession, CommandId, ContentHash, InboxKey,
    InboxRequest, Observation, PlanDocumentV1, PlanHash, Revision, TaskId,
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
    /// the attempt's resource claims.
    AttemptFailed {
        attempt: AttemptId,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CommandEnvelope {
    pub id: CommandId,
    pub expected_revision: Option<Revision>,
    pub command: Command,
}
