//! Attempt dispatch over the agent gateway (P3 Task 1, budget/retry/
//! fallback in P3 Task 3).
//!
//! [`Dispatcher::dispatch_queued`] sends every due `Queued` attempt once:
//! it reloads the run, rechecks the control/hold/plan/capability/budget
//! gate (including the shared admission gate and each attempt's retry
//! backoff deadline), claims the attempt with `StartDispatch` (CAS),
//! calls the gateway, and records the outcome — `Running` with its
//! session binding, `UnknownOutcome` without resending (never retried),
//! a deferred `Queued` with backoff when definitely unsent (bounded to
//! the retry policy), `Failed` when the harness refuses the turn, or a
//! profile-fallback continuation when quota/auth refuses it.
//!
//! No store transaction is ever held across a gateway call: the state is
//! loaded, the gateway is awaited with nothing held, and the outcome is
//! committed afterwards. Two racing dispatchers cannot double-send: the
//! `StartDispatch` compare-and-swap admits exactly one sender per attempt.
//!
//! Budget here means the shared ledger gate: `BudgetExceeded` or
//! `UsageUnknown` holds (plus the live admission check), which close the
//! gate for every role including retries and fallbacks. Capability means
//! the harness still serves the attempt's resolved profile and model.

use std::collections::BTreeMap;
use std::sync::Arc;

use central_domain::{
    admission, classify_rejection, failure_for_rejection, Actor, Admission, AttemptId,
    AttemptSession, Command, CommandEnvelope, RetryPolicy, RunId, Task, TaskId, TaskStatus,
    WorkRequest, WorkRole,
};

use crate::contracts::{AppError, DispatchError, DispatchSummary, OpenSession, SendTurn};
use crate::fallback::{choose_fallback, select_fallback, FallbackOverrides};
use crate::ports::{AgentGateway, Clock, IdSource, Store};
use crate::retries::RetryAction;
use crate::retries::{classify_open, classify_send, retries_used, retry_due, retry_not_before};
use crate::scheduler::{ProfileResolver, DEFAULT_MAX_PARALLEL};
use crate::Runtime;

/// Sends queued attempts through the harness, one guarded attempt at a time.
pub struct Dispatcher {
    store: Arc<dyn Store>,
    clock: Arc<dyn Clock>,
    ids: Arc<dyn IdSource>,
    agent: Arc<dyn AgentGateway>,
    resolver: Arc<ProfileResolver>,
    resources: BTreeMap<TaskId, Vec<String>>,
    fallbacks: Arc<FallbackOverrides>,
    retry_policy: RetryPolicy,
    max_parallel: usize,
    actor: Actor,
}

impl Dispatcher {
    pub fn new(
        store: Arc<dyn Store>,
        clock: Arc<dyn Clock>,
        ids: Arc<dyn IdSource>,
        agent: Arc<dyn AgentGateway>,
        resolver: Arc<ProfileResolver>,
        resources: BTreeMap<TaskId, Vec<String>>,
        actor: Actor,
    ) -> Self {
        Self {
            store,
            clock,
            ids,
            agent,
            resolver,
            resources,
            fallbacks: Arc::new(FallbackOverrides::default()),
            retry_policy: RetryPolicy::default(),
            max_parallel: DEFAULT_MAX_PARALLEL,
            actor,
        }
    }

    /// Share the scheduler's fallback state (chosen profiles plus the
    /// registry the fallback chain resolves against).
    pub fn with_fallbacks(mut self, fallbacks: Arc<FallbackOverrides>) -> Self {
        self.fallbacks = fallbacks;
        self
    }

    /// Bound safe retries (default: 3 retries at 2s/10s/30s).
    pub fn with_retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }

    /// Concurrency bound for attempts queued by fallback handling.
    pub fn with_max_parallel(mut self, max_parallel: usize) -> Self {
        self.max_parallel = max_parallel;
        self
    }

    fn runtime_for(&self, run_id: RunId) -> Runtime {
        Runtime::new(
            Arc::clone(&self.store),
            Arc::clone(&self.clock),
            Arc::clone(&self.ids),
            Arc::clone(&self.agent),
            run_id,
        )
    }

    async fn execute_on(
        &self,
        run_id: RunId,
        command: Command,
    ) -> Result<central_domain::RunState, AppError> {
        let envelope = CommandEnvelope {
            id: self.ids.next_command_id(),
            expected_revision: None,
            command,
        };
        self.runtime_for(run_id)
            .execute(self.actor.clone(), envelope)
            .await?;
        self.store
            .load(run_id)
            .await?
            .ok_or_else(|| AppError::NotFound(format!("run {run_id} vanished mid-dispatch")))
    }

    /// Whether the harness still serves this attempt's resolved profile.
    /// A mismatch defers the attempt: Central never swaps profiles or
    /// models silently to make a dispatch fit.
    async fn capabilities_hold(&self, task: &Task) -> bool {
        let Some(profile) = (self.resolver)(task) else {
            return false;
        };
        let capabilities = match self.agent.capabilities(&profile).await {
            Ok(capabilities) => capabilities,
            Err(_) => return false,
        };
        capabilities
            .features
            .iter()
            .any(|feature| *feature == format!("model:{}", profile.model))
    }

    /// Dispatch every queued attempt of one run. Counts sent (`Running`),
    /// ambiguous (`UnknownOutcome`) and still-queued attempts; attempts
    /// that fail definitively (`Rejected` opens, refused sends) end
    /// `Failed` and are visible in the run state rather than in a count.
    pub async fn dispatch_queued(&self, run_id: RunId) -> Result<DispatchSummary, AppError> {
        let Some(state) = self.store.load(run_id).await? else {
            return Err(AppError::NotFound(format!("run {run_id} not found")));
        };
        let mut queued: Vec<(TaskId, AttemptId)> = Vec::new();
        for attempt in state.attempts.values() {
            if attempt.status == central_domain::AttemptStatus::Queued {
                queued.push((attempt.task_id, attempt.id));
            }
        }
        // Deterministic dispatch order: task first, then attempt.
        queued.sort();
        // The gate is closed while the run is not Running, while any
        // hold is set, or while the shared budget admission holds: running
        // attempts continue, nothing new is sent.
        let budget_held = matches!(
            admission(
                &state,
                &WorkRequest {
                    role: WorkRole::Worker
                },
                self.clock.now()
            ),
            Admission::Held(_)
        );
        if state.control != central_domain::RunControl::Running
            || !state.holds.is_empty()
            || budget_held
        {
            return Ok(DispatchSummary {
                dispatched: 0,
                unknown: 0,
                still_pending: queued.len(),
            });
        }

        let mut summary = DispatchSummary::default();
        for (_, attempt_id) in queued {
            self.dispatch_one(run_id, attempt_id, &mut summary).await?;
        }
        // still_pending is the truth at the end, not an arithmetic
        // residue: recount what is still queued after every outcome.
        let end = self
            .store
            .load(run_id)
            .await?
            .ok_or_else(|| AppError::NotFound(format!("run {run_id} vanished mid-dispatch")))?;
        summary.still_pending = end
            .attempts
            .values()
            .filter(|attempt| attempt.status == central_domain::AttemptStatus::Queued)
            .count();
        Ok(summary)
    }

    /// One guarded send. Every early return leaves the attempt queued
    /// (deferred); only observed gateway evidence moves it elsewhere.
    async fn dispatch_one(
        &self,
        run_id: RunId,
        attempt_id: AttemptId,
        summary: &mut DispatchSummary,
    ) -> Result<(), AppError> {
        // Fresh load: recheck control, holds, plan binding, task state
        // and attempt state right before claiming.
        let Some(state) = self.store.load(run_id).await? else {
            return Err(AppError::NotFound(format!("run {run_id} not found")));
        };
        let attempt = match state.attempts.get(&attempt_id) {
            Some(attempt) if attempt.status == central_domain::AttemptStatus::Queued => {
                attempt.clone()
            }
            // A racing dispatcher moved it first; not ours to send.
            _ => return Ok(()),
        };
        let task = match state.tasks.get(&attempt.task_id) {
            Some(task) => task.clone(),
            None => return Ok(()),
        };
        // A deferred safe retry is not due yet: leave it queued and
        // let a later tick (past the virtual-clock deadline) send it.
        // The end-of-scan recount reports it as still pending, and no
        // real sleep is ever taken.
        if !retry_due(&state, attempt_id, self.clock.now()) {
            return Ok(());
        }
        if state.control != central_domain::RunControl::Running || !state.holds.is_empty() {
            return Ok(());
        }
        if matches!(
            admission(
                &state,
                &WorkRequest {
                    role: WorkRole::Worker
                },
                self.clock.now()
            ),
            Admission::Held(_)
        ) {
            return Ok(());
        }
        if state.active_plan.as_ref() != Some(&task.plan_hash) || task.status != TaskStatus::Running
        {
            return Ok(());
        }
        if !self.capabilities_hold(&task).await {
            return Ok(());
        }
        // The queue-time claim may have been released while the attempt
        // stayed Queued (a racing cancel). No domain transition does this
        // today — the sweep only frees confirmed-terminal owners and Stop
        // leaves Queued attempts untouched — but a send without the claim
        // would break exclusivity, so re-verify ownership here and defer.
        // Queueing stores trimmed keys, so compare trimmed here as well.
        if let Some(required) = self.resources.get(&attempt.task_id) {
            let owns_all = required
                .iter()
                .all(|key| state.resource_claims.get(key.trim()) == Some(&attempt_id));
            if !owns_all {
                return Ok(());
            }
        }
        let input = state
            .attempt_inputs
            .get(&attempt_id)
            .cloned()
            .unwrap_or_default();

        // Claim the send under CAS. A racing dispatcher loses here and
        // never touches the gateway for this attempt.
        match self
            .execute_on(
                run_id,
                Command::StartDispatch {
                    attempt: attempt_id,
                },
            )
            .await
        {
            Ok(_) => {}
            Err(AppError::Conflict(_)) => return Ok(()),
            Err(other) => return Err(other),
        }

        // The gateway calls below hold no store transaction: the claim
        // above is committed, and the outcome commits afterwards.
        let profile = (self.resolver)(&task).ok_or_else(|| {
            AppError::Capability("attempt profile vanished mid-dispatch".to_string())
        })?;
        let session = match self
            .agent
            .open(OpenSession {
                attempt_id,
                profile,
                workspace: workspace_label(run_id, attempt_id),
                resume_from: None,
            })
            .await
        {
            Ok(session) => session,
            Err(error) => {
                return self.on_open_error(run_id, attempt_id, error).await;
            }
        };
        let turn = self
            .agent
            .send(SendTurn {
                session: session.clone(),
                attempt_id,
                input,
                output_schema: None,
                command_id: self.ids.next_command_id(),
            })
            .await;
        self.on_send_outcome(run_id, attempt_id, session, turn, summary)
            .await
    }

    /// An `open` failure happens before any turn exists: quota/auth
    /// failures take the fallback chain, safe transport losses defer a
    /// bounded retry, and capability/request errors are definitive.
    async fn on_open_error(
        &self,
        run_id: RunId,
        attempt_id: AttemptId,
        error: AppError,
    ) -> Result<(), AppError> {
        match classify_open(&error) {
            RetryAction::Fallback => {
                self.run_fallback(run_id, attempt_id, &error.to_string())
                    .await
            }
            RetryAction::RetryNotSent => self.defer_or_fail(run_id, attempt_id).await,
            RetryAction::NeverRetryUnknown | RetryAction::Park => {
                self.fail_attempt(run_id, attempt_id).await
            }
        }
    }

    /// Record a terminal refusal. The task parks as failed too; recovery
    /// is explicit through `ReopenTask`, so the refusal stays recoverable
    /// without ever auto-retrying.
    async fn fail_attempt(&self, run_id: RunId, attempt_id: AttemptId) -> Result<(), AppError> {
        match self
            .execute_on(
                run_id,
                Command::AttemptFailed {
                    attempt: attempt_id,
                },
            )
            .await
        {
            Ok(_) => Ok(()),
            // A stop raced the failure: the attempt is reconciling now,
            // which already owns the outcome.
            Err(AppError::Conflict(_)) => Ok(()),
            Err(other) => Err(other),
        }
    }

    /// Defer one safe retry with virtual-clock backoff, or park the
    /// attempt as failed (recoverably) when the retry budget is
    /// exhausted. Never sleeps: the deadline is enforced by the
    /// dispatch gate on later ticks.
    async fn defer_or_fail(&self, run_id: RunId, attempt_id: AttemptId) -> Result<(), AppError> {
        let Some(state) = self.store.load(run_id).await? else {
            return Err(AppError::NotFound(format!("run {run_id} not found")));
        };
        let used = retries_used(&state, attempt_id);
        let now = self.clock.now();
        let command = match retry_not_before(&self.retry_policy, used, now) {
            Some(not_before) => Command::DeferRetry {
                attempt: attempt_id,
                not_before,
            },
            // Exhausted: park like a refused send; `ReopenTask`
            // recovers it explicitly.
            None => Command::AttemptFailed {
                attempt: attempt_id,
            },
        };
        match self.execute_on(run_id, command).await {
            Ok(_) => Ok(()),
            // A stop raced the deferral: reconciling owns the outcome.
            Err(AppError::Conflict(_)) => Ok(()),
            Err(other) => Err(other),
        }
    }

    /// Quota/auth fallback: confirm the old attempt stopped, then open
    /// one new attempt on the first chain candidate that meets harness
    /// capability without raising permissions, and record the decision
    /// durably as the user notification. A held or exhausted budget
    /// opens zero new attempts; an unresolvable chain parks quietly.
    async fn run_fallback(
        &self,
        run_id: RunId,
        attempt_id: AttemptId,
        reason: &str,
    ) -> Result<(), AppError> {
        let Some(state) = self.store.load(run_id).await? else {
            return Err(AppError::NotFound(format!("run {run_id} not found")));
        };
        let attempt = match state.attempts.get(&attempt_id) {
            Some(attempt) => attempt.clone(),
            None => return Ok(()),
        };
        let task = match state.tasks.get(&attempt.task_id) {
            Some(task) => task.clone(),
            None => return Ok(()),
        };
        let current = match (self.resolver)(&task) {
            Some(profile) => profile,
            // No servable profile: park without touching the chain.
            None => return self.fail_attempt(run_id, attempt_id).await,
        };
        let capabilities = match self.agent.capabilities(&current).await {
            Ok(capabilities) => capabilities,
            Err(_) => return self.fail_attempt(run_id, attempt_id).await,
        };
        // Chain gate first (quota/auth with a usable chain), then
        // the capability/permission selection: a candidate must meet
        // the harness and never raise permissions, or the attempt parks
        // without opening anything.
        let failure = failure_for_rejection(classify_rejection(reason));
        if choose_fallback(&current, &failure, &capabilities).is_none() {
            return self.fail_attempt(run_id, attempt_id).await;
        }
        let registry = self.fallbacks.registry_snapshot();
        let candidate = match select_fallback(&current, &registry, &capabilities) {
            Some(candidate) => candidate,
            None => return self.fail_attempt(run_id, attempt_id).await,
        };
        // Confirmed old stop first: the refused attempt is terminal and
        // releases its claims before anything new opens.
        self.fail_attempt(run_id, attempt_id).await?;
        // No separate budget for fallbacks: the shared gate decides.
        // A held or exhausted budget opens zero new attempts.
        let Some(fresh) = self.store.load(run_id).await? else {
            return Err(AppError::NotFound(format!(
                "run {run_id} vanished mid-fallback"
            )));
        };
        if matches!(
            admission(
                &fresh,
                &WorkRequest {
                    role: WorkRole::Fallback
                },
                self.clock.now()
            ),
            Admission::Held(_)
        ) {
            return Ok(());
        }
        // Freeze the fallback profile for the new attempt, then reopen
        // the parked task and queue its continuation on the old input.
        // Cached inputs are never rewritten: the new attempt reuses the
        // stored input verbatim.
        self.fallbacks.set(task.id, candidate.clone());
        match self
            .execute_on(run_id, Command::ReopenTask { task: task.id })
            .await
        {
            Ok(_) => {}
            Err(AppError::Conflict(_)) => return Ok(()),
            Err(other) => return Err(other),
        }
        let input = fresh
            .attempt_inputs
            .get(&attempt_id)
            .cloned()
            .unwrap_or_default();
        let resources = self.resources.get(&task.id).cloned().unwrap_or_default();
        let new_attempt = self.ids.next_attempt_id();
        match self
            .execute_on(
                run_id,
                Command::QueueAttempt {
                    attempt: new_attempt,
                    task: task.id,
                    input,
                    resources,
                    max_parallel: self.max_parallel,
                },
            )
            .await
        {
            Ok(_) => {}
            Err(AppError::Conflict(_)) => return Ok(()),
            Err(other) => return Err(other),
        }
        match self
            .execute_on(
                run_id,
                Command::NoteFallback {
                    task: task.id,
                    old_attempt: attempt_id,
                    new_attempt,
                    profile_id: candidate.id,
                    reason: format!("quota or auth refusal on attempt {attempt_id}: {reason}"),
                },
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(AppError::Conflict(_)) => Ok(()),
            Err(other) => Err(other),
        }
    }

    /// Record what the send proved. `Ok` means running with a session
    /// binding; ambiguity parks the attempt without resending (never
    /// retried); a definitely-unsent turn defers a bounded retry; a
    /// quota/auth refusal takes the fallback chain; any other refusal
    /// fails the attempt recoverably.
    async fn on_send_outcome(
        &self,
        run_id: RunId,
        attempt_id: AttemptId,
        session: crate::contracts::SessionRef,
        turn: Result<crate::contracts::TurnRef, DispatchError>,
        summary: &mut DispatchSummary,
    ) -> Result<(), AppError> {
        match turn {
            Ok(_) => {
                let binding = AttemptSession {
                    provider: session.provider.clone(),
                    instance_epoch: session.instance_epoch.clone(),
                    thread_id: session.thread_id.clone(),
                };
                match self
                    .execute_on(
                        run_id,
                        Command::AttemptRunning {
                            attempt: attempt_id,
                            session: binding,
                        },
                    )
                    .await
                {
                    Ok(_) => {
                        summary.dispatched += 1;
                        Ok(())
                    }
                    // A stop raced the receipt: the attempt is
                    // reconciling; the session binding is lost with the
                    // failed commit and reconcile must rediscover it.
                    Err(AppError::Conflict(_)) => Ok(()),
                    Err(other) => Err(other),
                }
            }
            Err(DispatchError::OutcomeUnknown(_)) => {
                match self
                    .execute_on(
                        run_id,
                        Command::AttemptUnknown {
                            attempt: attempt_id,
                        },
                    )
                    .await
                {
                    Ok(_) => {
                        summary.unknown += 1;
                        Ok(())
                    }
                    Err(AppError::Conflict(_)) => Ok(()),
                    Err(other) => Err(other),
                }
            }
            Err(DispatchError::DefinitelyNotSent(_)) => {
                self.defer_or_fail(run_id, attempt_id).await
            }
            Err(DispatchError::Rejected(message)) => {
                match classify_send(&DispatchError::Rejected(message.clone())) {
                    RetryAction::Fallback => self.run_fallback(run_id, attempt_id, &message).await,
                    _ => self.fail_attempt(run_id, attempt_id).await,
                }
            }
        }
    }
}

/// Logical workspace label for a dispatch. No directory is created here:
/// isolated worktrees arrive with the workspace integration (P3/T5). The
/// label keeps every attempt addressable until then.
fn workspace_label(run_id: RunId, attempt_id: AttemptId) -> String {
    format!("run/{run_id}/attempt/{attempt_id}")
}
