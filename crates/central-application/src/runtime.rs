use std::sync::Arc;

use central_domain::{
    decide_envelope, Actor, AttemptId, AttemptStatus, Command, CommandEnvelope, Effect, Event,
    ProfileId, ProfileRole, RunControl, RunId, RunState, TaskId,
};

use crate::contracts::{
    command_payload_hash, AppError, CommitBatch, DispatchError, DispatchSummary, ExecutionRef,
    ExternalAnswer, OutboxIntent, OutboxStatus, Receipt, RecoverySummary, SessionRef,
};
use crate::inbox::SecretVault;
use crate::ports::{AgentGateway, Clock, IdSource, Store};
use central_domain::AnswerPayload;

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
    secrets: Arc<SecretVault>,
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
            secrets: Arc::new(SecretVault::new()),
            run_id,
        }
    }

    /// Share a memory-only secret vault with this runtime. Dispatch resolves
    /// secret references through it; a runtime with a fresh (empty) vault
    /// sends nothing and waits for re-entry instead of replaying secrets.
    pub fn with_secrets(self, secrets: Arc<SecretVault>) -> Self {
        Self { secrets, ..self }
    }

    pub(crate) fn store(&self) -> &Arc<dyn Store> {
        &self.store
    }

    pub(crate) fn clock(&self) -> &Arc<dyn Clock> {
        &self.clock
    }

    pub(crate) fn agent(&self) -> &Arc<dyn AgentGateway> {
        &self.agent
    }

    pub(crate) fn ids(&self) -> &Arc<dyn IdSource> {
        &self.ids
    }

    pub(crate) fn secrets(&self) -> &Arc<SecretVault> {
        &self.secrets
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
            secrets: Arc::new(SecretVault::new()),
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

    /// Set (or clear) the run-wide profile for one agent role. The id is
    /// validated against the profile store first, so a typo fails here
    /// with the available profiles instead of deferring every later
    /// dispatch silently.
    pub async fn set_run_profile(
        &self,
        actor: Actor,
        role: ProfileRole,
        profile: Option<ProfileId>,
    ) -> Result<Receipt, AppError> {
        if let Some(id) = &profile {
            if self.store.get_profile(&id.to_string()).await?.is_none() {
                return Err(AppError::NotFound(format!(
                    "profile {id} is unknown; list profiles before binding one to run {}",
                    self.run_id,
                )));
            }
        }
        self.execute(
            actor,
            CommandEnvelope {
                id: self.ids.next_command_id(),
                expected_revision: None,
                command: Command::SetRunProfile { role, profile },
            },
        )
        .await
    }

    /// Queue one leader review turn against a task and return its attempt
    /// id. The prompt names the task, its verify criteria and the latest
    /// sealed candidate (when one exists); the leader advises through
    /// the gateway with the run's leader profile, and the output returns
    /// through handover import, never through worker-result import.
    pub async fn queue_leader_review(
        &self,
        actor: Actor,
        task: TaskId,
        objective: &str,
    ) -> Result<(Receipt, AttemptId), AppError> {
        let objective = objective.trim();
        if objective.is_empty() {
            return Err(AppError::Validation(
                "a leader review needs a non-empty objective".to_string(),
            ));
        }
        let Some(state) = self.store.load(self.run_id).await? else {
            return Err(AppError::NotFound(format!("run {} not found", self.run_id)));
        };
        let task_state = state.tasks.get(&task).ok_or_else(|| {
            AppError::NotFound(format!("task {task} is not part of run {}", self.run_id))
        })?;
        // Bind the latest sealed candidate (if any) so the review names
        // the exact input/candidate/criteria triple it judges; without a
        // seal yet the leader reviews the task as stated.
        let mut sealed: Option<(String, String, String)> = None;
        for event in self.store.events(self.run_id).await? {
            if let Event::WorkerResultRecorded {
                task: event_task,
                input_hash,
                candidate_hash,
                criteria_hash,
                ..
            } = event
            {
                if event_task == task {
                    sealed = Some((
                        input_hash.to_string(),
                        candidate_hash.to_string(),
                        criteria_hash.to_string(),
                    ));
                }
            }
        }
        let criteria = task_state.verify.criteria.join("; ");
        let mut input = format!(
            "Leader review for task {task} (\"{}\").\nTask objective: {}\nVerify criteria: {}.\n",
            objective, task_state.objective, criteria,
        );
        match sealed {
            Some((input_hash, candidate_hash, criteria_hash)) => {
                input.push_str(&format!(
                    "Sealed candidate: {candidate_hash} (input {input_hash}, criteria {criteria_hash}).\n"
                ));
            }
            None => input.push_str("No sealed candidate yet: review the task as stated.\n"),
        }
        input.push_str(
            "Advise with findings and one concrete next step. Do not edit anything: \
             your output returns through handover import.",
        );
        let attempt = self.ids.next_attempt_id();
        let receipt = self
            .execute(
                actor,
                CommandEnvelope {
                    id: self.ids.next_command_id(),
                    expected_revision: None,
                    command: Command::QueueLeaderReview {
                        attempt,
                        task,
                        input,
                    },
                },
            )
            .await?;
        Ok((receipt, attempt))
    }

    /// Close a leader review turn after its output returned. Only the
    /// operator closes reviews; the domain completes the review
    /// attempt (releasing the task cover so the waiting worker can
    /// dispatch) and rides a cancel intent along so a lingering
    /// harness session is reaped. The task itself keeps its status.
    pub async fn finish_leader_review(
        &self,
        actor: Actor,
        attempt: AttemptId,
    ) -> Result<Receipt, AppError> {
        let receipt = self
            .execute(
                actor,
                CommandEnvelope {
                    id: self.ids.next_command_id(),
                    expected_revision: None,
                    command: Command::CompleteLeaderReview { attempt },
                },
            )
            .await?;
        Ok(receipt)
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
                    // Both intent kinds describe one dispatch for an
                    // attempt; a dispatch interrupted mid-flight is unknown,
                    // never resent blindly — including a queued answer.
                    match intent.effect {
                        Effect::CancelAttempt { attempt_id } => {
                            unknown_attempts.insert(attempt_id);
                        }
                        Effect::RespondToRequest { attempt_id, .. } => {
                            if let Some(attempt_id) = attempt_id {
                                unknown_attempts.insert(attempt_id);
                            }
                        }
                    }
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
            // Passive per-run report: findings plus required user actions.
            // Recovery itself never dispatches, answers, promotes or
            // resumes; every entry names an explicit user step instead.
            // Holds are deliberately preserved here, never cleared.
            summary.details.push(crate::recovery::inspect_run(&next));
        }
        Ok(summary)
    }

    /// Whether an intent may be claimed for sending right now. Cancels
    /// follow the run gate above; answers additionally require a Running
    /// run and — for secrets — a live memory-only reference. A `false`
    /// here leaves the intent Pending without claiming it.
    fn sendable_now(&self, state: &RunState, intent: &OutboxIntent) -> bool {
        match &intent.effect {
            Effect::CancelAttempt { .. } => true,
            Effect::RespondToRequest { payload, .. } => {
                if state.control != RunControl::Running {
                    return false;
                }
                match payload {
                    AnswerPayload::Plain(_) => true,
                    AnswerPayload::Secret(reference) => self.secrets.lookup(reference).is_some(),
                }
            }
        }
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
            // Answers only go out while the run is Running, and a secret
            // answer needs its memory-only reference to still resolve.
            // Skipped intents are never claimed, so they stay Pending:
            // answering opens no inference before explicit resume, a
            // stopped run never answers, and a missing secret waits for
            // re-entry instead of sending.
            if !self.sendable_now(&state, intent) {
                continue;
            }
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
                Effect::RespondToRequest {
                    key,
                    payload,
                    scope,
                    attempt_id,
                } => {
                    // The plaintext resolves without opening inference:
                    // secrets come only from the memory-only vault, whose
                    // presence was checked before the claim above.
                    let text = match payload {
                        AnswerPayload::Plain(text) => text.clone(),
                        AnswerPayload::Secret(reference) => {
                            match self.secrets.lookup(reference) {
                                Some(secret) => secret,
                                None => {
                                    // Lost between gate and claim: release
                                    // back to Pending for re-entry.
                                    self.store
                                        .transition_outbox(
                                            intent.id,
                                            OutboxStatus::Dispatching,
                                            OutboxStatus::Pending,
                                        )
                                        .await?;
                                    summary.still_pending += 1;
                                    continue;
                                }
                            }
                        }
                    };
                    let answer = ExternalAnswer {
                        key: crate::contracts::ExternalRequestKey {
                            session: SessionRef {
                                provider: key.provider.clone(),
                                instance_epoch: key.epoch.clone(),
                                thread_id: key.thread_id.clone(),
                            },
                            turn_id: key.turn_id.clone(),
                            item_id: key.item_id.clone(),
                            request_id: key.request_id.clone(),
                        },
                        payload: AnswerPayload::Plain(text),
                        scope: *scope,
                    };
                    match self.agent.answer(answer).await {
                        Ok(()) => {
                            self.store
                                .transition_outbox(
                                    intent.id,
                                    OutboxStatus::Dispatching,
                                    OutboxStatus::Completed,
                                )
                                .await?;
                            summary.dispatched += 1;
                            if let AnswerPayload::Secret(reference) = payload {
                                self.secrets.consume(reference);
                            }
                            // The inbox entry stays Answered until a harness
                            // observation confirms it: resolution is passive.
                        }
                        Err(DispatchError::Rejected(message)) => {
                            // The harness refused the reply (unknown or
                            // already-used key): never resend.
                            let _ = message;
                            self.store
                                .transition_outbox(
                                    intent.id,
                                    OutboxStatus::Dispatching,
                                    OutboxStatus::Failed,
                                )
                                .await?;
                            summary.unknown += 1;
                            if let Some(attempt_id) = attempt_id {
                                if let Some(attempt) = next_state.attempts.get_mut(attempt_id) {
                                    attempt.status = AttemptStatus::UnknownOutcome;
                                    state_changed = true;
                                }
                            }
                        }
                        Err(_) => {
                            self.store
                                .transition_outbox(
                                    intent.id,
                                    OutboxStatus::Dispatching,
                                    OutboxStatus::UnknownOutcome,
                                )
                                .await?;
                            summary.unknown += 1;
                            if let Some(attempt_id) = attempt_id {
                                if let Some(attempt) = next_state.attempts.get_mut(attempt_id) {
                                    attempt.status = AttemptStatus::UnknownOutcome;
                                    state_changed = true;
                                }
                            }
                        }
                    }
                }
                Effect::CancelAttempt { attempt_id } => {
                    // Cancel the attempt's live harness session, never a
                    // blank reference: without the bound provider/epoch/
                    // thread the gateway cannot route or identity-check the
                    // kill, and the agent keeps running after the stop.
                    let session = state
                        .attempt_sessions
                        .get(attempt_id)
                        .map(|binding| SessionRef {
                            provider: binding.provider.clone(),
                            instance_epoch: binding.instance_epoch.clone(),
                            thread_id: binding.thread_id.clone(),
                        })
                        .unwrap_or(SessionRef {
                            provider: "central".to_string(),
                            instance_epoch: String::new(),
                            thread_id: String::new(),
                        });
                    let target = ExecutionRef {
                        attempt_id: *attempt_id,
                        session,
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
                            // The kill is still useful cleanup, but a
                            // terminal status never regresses: a finish
                            // that rode along with its own cancel intent
                            // stays completed.
                            if let Some(attempt) = next_state.attempts.get_mut(attempt_id) {
                                if !central_domain::is_confirmed_terminal(attempt.status) {
                                    attempt.status = AttemptStatus::Cancelled;
                                    state_changed = true;
                                }
                            }
                            // A cancelled attempt is a confirmed
                            // terminal: its resource claims release so a
                            // later tick can schedule the next attempt.
                            // Unknown outcomes keep theirs.
                            state_changed |=
                                central_domain::release_settled_claims(&mut next_state);
                        }
                        Ok(_) | Err(_) => {
                            // No usable evidence: the cancel may or
                            // may not have landed. Mark unknown, never
                            // complete, never resend — unless the
                            // attempt already reached a terminal state
                            // through another path, which never regresses.
                            self.store
                                .transition_outbox(
                                    intent.id,
                                    OutboxStatus::Dispatching,
                                    OutboxStatus::UnknownOutcome,
                                )
                                .await?;
                            summary.unknown += 1;
                            if let Some(attempt) = next_state.attempts.get_mut(attempt_id) {
                                if !central_domain::is_confirmed_terminal(attempt.status) {
                                    attempt.status = AttemptStatus::UnknownOutcome;
                                    state_changed = true;
                                }
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
