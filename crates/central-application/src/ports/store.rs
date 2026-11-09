use async_trait::async_trait;
use central_domain::{ActorId, CommandId, RunId, RunState};

use crate::contracts::{AppError, CommitBatch, OutboxIntent, Receipt};

/// Durable run store. Implementations must commit a `CommitBatch`
/// atomically: snapshot, events, outbox intents and receipt land together
/// or not at all, and the ack is returned only after the commit.
#[async_trait]
pub trait Store: Send + Sync {
    async fn load(&self, id: RunId) -> Result<Option<RunState>, AppError>;
    async fn receipt(&self, actor: ActorId, id: CommandId) -> Result<Option<Receipt>, AppError>;
    async fn commit(&self, batch: CommitBatch) -> Result<Receipt, AppError>;
    async fn pending(&self, id: RunId) -> Result<Vec<OutboxIntent>, AppError>;
}
