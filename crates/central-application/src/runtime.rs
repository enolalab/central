use std::sync::Arc;

use central_domain::{decide_envelope, Actor, CommandEnvelope, RunId, RunState};

use crate::contracts::{
    command_payload_hash, AppError, CommitBatch, OutboxIntent, OutboxStatus, Receipt,
};
use crate::ports::{AgentGateway, Clock, IdSource, Store};

/// Application command handler for one run. It validates through the pure
/// domain decision, then commits state, events, outbox intents and the
/// receipt atomically through the store. It never dispatches inline:
/// effects are persisted as pending outbox intents first, so a failed
/// commit can never cause an external send, and the ack follows the commit.
pub struct Runtime {
    store: Arc<dyn Store>,
    clock: Arc<dyn Clock>,
    ids: Arc<dyn IdSource>,
    #[allow(dead_code)] // Held for dispatch_once (P1 Task 4); execute never dispatches.
    agent: Arc<dyn AgentGateway>,
    run_id: RunId,
}

impl Runtime {
    pub fn new(
        store: Arc<dyn Store>,
        clock: Arc<dyn Clock>,
        ids: Arc<dyn IdSource>,
        agent: Arc<dyn AgentGateway>,
        run_id: RunId,
    ) -> Self {
        Self {
            store,
            clock,
            ids,
            agent,
            run_id,
        }
    }

    /// A runtime for a brand-new run whose id comes from the id source.
    pub fn new_run(
        store: Arc<dyn Store>,
        clock: Arc<dyn Clock>,
        ids: Arc<dyn IdSource>,
        agent: Arc<dyn AgentGateway>,
    ) -> Self {
        let run_id = ids.next_run_id();
        Self {
            store,
            clock,
            ids,
            agent,
            run_id,
        }
    }

    pub fn run_id(&self) -> RunId {
        self.run_id
    }

    pub async fn execute(
        &self,
        actor: Actor,
        envelope: CommandEnvelope,
    ) -> Result<Receipt, AppError> {
        let payload_hash = command_payload_hash(&envelope)?;

        // Receipt replay before decide: a duplicate with the same payload
        // returns the first receipt even if re-deciding would now fail;
        // the same command id with a different payload conflicts.
        if let Some(existing) = self.store.receipt(actor.id, envelope.id).await? {
            if existing.payload_hash != payload_hash || existing.command_id != envelope.id {
                return Err(AppError::Conflict(
                    "command id already used with a different payload".to_string(),
                ));
            }
            return Ok(existing);
        }

        let current = self.store.load(self.run_id).await?;
        let state = current.unwrap_or_else(|| RunState::new(self.run_id));
        let expected_revision = state.revision;

        let decision = decide_envelope(&state, &actor, &envelope, self.clock.now())?;

        let intents = decision
            .effects
            .iter()
            .map(|effect| OutboxIntent {
                id: self.ids.next_intent_id(),
                run_id: self.run_id,
                effect: effect.clone(),
                status: OutboxStatus::Pending,
            })
            .collect::<Vec<_>>();

        let receipt = Receipt {
            command_id: envelope.id,
            run_id: self.run_id,
            revision: decision.next.revision,
            result: "applied".to_string(),
            payload_hash: payload_hash.clone(),
        };

        let batch = CommitBatch {
            actor: actor.id,
            command_id: envelope.id,
            payload_hash,
            expected_revision,
            next_state: decision.next,
            events: decision.events,
            intents,
            receipt,
        };

        // Atomic commit; the store re-checks dedup and CAS inside its
        // transaction, so a racing duplicate resolves there, not here.
        self.store.commit(batch).await
    }
}
