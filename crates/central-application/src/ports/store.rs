use async_trait::async_trait;
use central_domain::{ActorId, CommandId, Event, IntentId, Revision, RunId, RunState};

use crate::contracts::{AppError, CommitBatch, OutboxIntent, OutboxStatus, Receipt};

/// Durable run store. Implementations must commit a `CommitBatch`
/// atomically: snapshot, events, outbox intents and receipt land together
/// or not at all, and the ack is returned only after the commit.
#[async_trait]
pub trait Store: Send + Sync {
    async fn load(&self, id: RunId) -> Result<Option<RunState>, AppError>;
    async fn receipt(&self, actor: ActorId, id: CommandId) -> Result<Option<Receipt>, AppError>;
    async fn commit(&self, batch: CommitBatch) -> Result<Receipt, AppError>;
    async fn pending(&self, id: RunId) -> Result<Vec<OutboxIntent>, AppError>;

    /// All run ids known to the store. Used by startup recovery; the
    /// default reports the operation as unsupported rather than
    /// silently recovering nothing.
    async fn list_run_ids(&self) -> Result<Vec<RunId>, AppError> {
        Err(AppError::Storage(
            "list_run_ids is not supported by this store".to_string(),
        ))
    }

    /// Every outbox intent for a run, in any status. Recovery needs the
    /// non-pending rows (for example `Dispatching`) that `pending`
    /// deliberately hides.
    async fn outbox(&self, id: RunId) -> Result<Vec<OutboxIntent>, AppError> {
        let _ = id;
        Err(AppError::Storage(
            "outbox is not supported by this store".to_string(),
        ))
    }

    /// Guarded outbox transition: the intent moves from `expected` to
    /// `next` only if it is still in `expected`. Returns `false` when
    /// the guard fails (already handled elsewhere), never overwrites.
    /// Dispatch code must use this — evidence moves an intent to
    /// `Completed`, ambiguity to `UnknownOutcome` — and never an
    /// unguarded status write.
    async fn transition_outbox(
        &self,
        id: IntentId,
        expected: OutboxStatus,
        next: OutboxStatus,
    ) -> Result<bool, AppError> {
        let _ = (id, expected, next);
        Err(AppError::Storage(
            "transition_outbox is not supported by this store".to_string(),
        ))
    }

    /// Every persisted event for a run, in sequence order. The default
    /// reports the operation as unsupported rather than silently
    /// returning no history.
    async fn events(&self, id: RunId) -> Result<Vec<Event>, AppError> {
        let _ = id;
        Err(AppError::Storage(
            "events is not supported by this store".to_string(),
        ))
    }

    /// Replace a run snapshot under a revision compare-and-swap, with
    /// no receipt or events. Reserved for startup recovery, which is a
    /// system transition, not a user command; the revision is kept so
    /// recovery never forges command history.
    async fn replace_state(
        &self,
        state: &RunState,
        expected_revision: Revision,
    ) -> Result<(), AppError> {
        let _ = (state, expected_revision);
        Err(AppError::Storage(
            "replace_state is not supported by this store".to_string(),
        ))
    }
}
