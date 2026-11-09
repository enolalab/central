use std::sync::Arc;

use central_domain::{
    decide_envelope, Actor, AttemptStatus, CommandEnvelope, Effect, RunControl, RunId, RunState,
};

use crate::contracts::{
    command_payload_hash, AppError, CommitBatch, DispatchSummary, ExecutionRef, OutboxIntent,
    OutboxStatus, Receipt, RecoverySummary, SessionRef,
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

    /// Startup recovery for every known run. Nonterminal runs are
    /// parked in `AwaitingResume` and attempts/intents left in
    /// `Dispatching` without a receipt become `UnknownOutcome`: a
    /// crash after send but before receipt is never resent blindly.
    /// Recovery performs zero external sends; only an explicit
    /// `Resume` command reopens the dispatch gate. Each run is handled
    /// independently, so one corrupt run surfaces as an error naming
    /// the store failure instead of silently dropping the others.
    pub async fn recover(&self) -> Result<RecoverySummary, AppError> {
        let run_ids = self.store.list_run_ids().await?;
        let mut summary = RecoverySummary {
            runs_seen: run_ids.len(),
            ..RecoverySummary::default()
        };
        for run_id in run_ids {
            let Some(state) = self.store.load(run_id).await? else {
                continue;
            };
            let mut next = state.clone();
            let mut changed = false;

            // An intent and its attempt describe one dispatch, so
            // unknown outcomes are counted per attempt, not per row.
            let mut unknown_attempts = std::collections::BTreeSet::new();
            for intent in self.store.outbox(run_id).await? {
                if intent.status == OutboxStatus::Dispatching
                    && self
                        .store
                        .transition_outbox(
                            intent.id,
                            OutboxStatus::Dispatching,
                            OutboxStatus::UnknownOutcome,
                        )
                        .await?
                {
                    let Effect::CancelAttempt { attempt_id } = intent.effect;
                    unknown_attempts.insert(attempt_id);
                }
            }
            for attempt in next.attempts.values_mut() {
                if attempt.status == AttemptStatus::Dispatching {
                    attempt.status = AttemptStatus::UnknownOutcome;
                    unknown_attempts.insert(attempt.id);
                    changed = true;
                }
            }
            summary.unknown_outcome += unknown_attempts.len();

            let nonterminal = state.created && state.control != RunControl::Stopped;
            if nonterminal && state.control != RunControl::AwaitingResume {
                next.control = RunControl::AwaitingResume;
                changed = true;
            }
            if next.control == RunControl::AwaitingResume && nonterminal {
                summary.awaiting_resume += 1;
            }
            if changed {
                self.store.replace_state(&next, state.revision).await?;
            }
        }
        Ok(summary)
    }

    /// Dispatch pending outbox intents for one run, one guarded
    /// transition at a time. The gate is closed while a run is
    /// `AwaitingResume` or `Paused`: zero external sends happen before
    /// explicit resume. Each intent is independent — the store returns
    /// pending intents ordered by id, and no intent's outcome depends
    /// on that order. Only observed evidence completes an intent; an
    /// ambiguous send becomes `UnknownOutcome` and is never resent.
    pub async fn dispatch_once(&self, run_id: RunId) -> Result<DispatchSummary, AppError> {
        let Some(state) = self.store.load(run_id).await? else {
            return Err(AppError::NotFound(format!("run {run_id} not found")));
        };
        let pending = self.store.pending(run_id).await?;
        let mut summary = DispatchSummary {
            still_pending: pending.len(),
            ..DispatchSummary::default()
        };
        if matches!(
            state.control,
            RunControl::AwaitingResume | RunControl::Paused
        ) {
            return Ok(summary);
        }

        let mut next_state = state.clone();
        let mut state_changed = false;
        for intent in &pending {
            // Claim the intent. If the guard fails another dispatcher
            // already owns it; skip without sending.
            let claimed = self
                .store
                .transition_outbox(intent.id, OutboxStatus::Pending, OutboxStatus::Dispatching)
                .await?;
            if !claimed {
                continue;
            }
            summary.still_pending -= 1;
            match &intent.effect {
                Effect::CancelAttempt { attempt_id } => {
                    let target = ExecutionRef {
                        attempt_id: *attempt_id,
                        session: SessionRef {
                            provider: "central".to_string(),
                            instance_epoch: String::new(),
                            thread_id: String::new(),
                        },
                        turn: None,
                    };
                    match self.agent.cancel(&target).await {
                        Ok(evidence) if evidence.cancelled => {
                            self.store
                                .transition_outbox(
                                    intent.id,
                                    OutboxStatus::Dispatching,
                                    OutboxStatus::Completed,
                                )
                                .await?;
                            summary.dispatched += 1;
                            if let Some(attempt) = next_state.attempts.get_mut(attempt_id) {
                                attempt.status = AttemptStatus::Cancelled;
                                state_changed = true;
                            }
                        }
                        Ok(_) | Err(_) => {
                            // No usable evidence: the cancel may or
                            // may not have landed. Mark unknown, never
                            // complete, never resend.
                            self.store
                                .transition_outbox(
                                    intent.id,
                                    OutboxStatus::Dispatching,
                                    OutboxStatus::UnknownOutcome,
                                )
                                .await?;
                            summary.unknown += 1;
                            if let Some(attempt) = next_state.attempts.get_mut(attempt_id) {
                                attempt.status = AttemptStatus::UnknownOutcome;
                                state_changed = true;
                            }
                        }
                    }
                }
            }
        }
        if state_changed {
            self.store
                .replace_state(&next_state, state.revision)
                .await?;
        }
        Ok(summary)
    }
}
