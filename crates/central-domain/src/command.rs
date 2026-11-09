use serde::{Deserialize, Serialize};

use crate::{ActorId, CommandId, PlanDocumentV1, PlanHash, Revision};

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
    CreateRun { goal: String, repository: String },
    ProposePlan { plan: PlanDocumentV1 },
    ApprovePlan { plan_hash: PlanHash },
    Pause,
    Stop,
    Resume,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CommandEnvelope {
    pub id: CommandId,
    pub expected_revision: Option<Revision>,
    pub command: Command,
}
