//! Attempt dispatch over the agent gateway (P3 Task 1).
//!
//! [`Dispatcher::dispatch_queued`] sends every `Queued` attempt once: it
//! reloads the run, rechecks the control/hold/plan/capability/budget gate,
//! claims the attempt with `StartDispatch` (CAS), calls the gateway, and
//! records the outcome — `Running` with its session binding, `UnknownOutcome`
//! without resending, back to `Queued` when definitely unsent, or `Failed`
//! when the harness refuses the turn.
//!
//! No store transaction is ever held across a gateway call: the state is
//! loaded, the gateway is awaited with nothing held, and the outcome is
//! committed afterwards. Two racing dispatchers cannot double-send: the
//! `StartDispatch` compare-and-swap admits exactly one sender per attempt.
//!
//! Budget here means the run holds (`BudgetExceeded`/`UsageUnknown`); the
//! shared ledger and its limits arrive with P3/T3. Capability means the
//! harness still serves the attempt's resolved profile and model; full
//! layer resolution and fallback chains belong to P2/T2 and P3/T3.

use std::sync::Arc;

use central_domain::{
    Actor, AttemptId, AttemptSession, Command, CommandEnvelope, RunId, Task, TaskId, TaskStatus,
};

use crate::contracts::{AppError, DispatchError, DispatchSummary, OpenSession, SendTurn};
use crate::ports::{AgentGateway, Clock, IdSource, Store};
use crate::scheduler::ProfileResolver;
use crate::Runtime;

/// Sends queued attempts through the harness, one guarded attempt at a time.
pub struct Dispatcher {
    store: Arc<dyn Store>,
    clock: Arc<dyn Clock>,
    ids: Arc<dyn IdSource>,
    agent: Arc<dyn AgentGateway>,
    resolver: Arc<ProfileResolver>,
    actor: Actor,
}

impl Dispatcher {
    pub fn new(
        store: Arc<dyn Store>,
        clock: Arc<dyn Clock>,
        ids: Arc<dyn IdSource>,
        agent: Arc<dyn AgentGateway>,
        resolver: Arc<ProfileResolver>,
        actor: Actor,
    ) -> Self {
        Self {
            store,
            clock,
            ids,
            agent,
            resolver,
            actor,
        }
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
        // The gate is closed while the run is not Running or while any
        // hold (budget unknown, budget exceeded, ...) is set: running
        // attempts continue, nothing new is sent.
        if state.control != central_domain::RunControl::Running || !state.holds.is_empty() {
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
        if state.control != central_domain::RunControl::Running || !state.holds.is_empty() {
            return Ok(());
        }
        if state.active_plan.as_ref() != Some(&task.plan_hash) || task.status != TaskStatus::Running
        {
            return Ok(());
        }
        if !self.capabilities_hold(&task).await {
            return Ok(());
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

    /// An `open` failure happens before any turn exists: capability and
    /// request errors are definitive, transport errors retry later.
    async fn on_open_error(
        &self,
        run_id: RunId,
        attempt_id: AttemptId,
        error: AppError,
    ) -> Result<(), AppError> {
        let definitive = matches!(
            error,
            AppError::Capability(_)
                | AppError::Validation(_)
                | AppError::Forbidden(_)
                | AppError::NotFound(_)
        );
        let command = if definitive {
            Command::AttemptFailed {
                attempt: attempt_id,
            }
        } else {
            Command::RequeueAttempt {
                attempt: attempt_id,
            }
        };
        match self.execute_on(run_id, command).await {
            Ok(_) => Ok(()),
            // A stop raced the failure: the attempt is reconciling now,
            // which already owns the outcome.
            Err(AppError::Conflict(_)) => Ok(()),
            Err(other) => Err(other),
        }
    }

    /// Record what the send proved. `Ok` means running with a session
    /// binding; ambiguity parks the attempt without resending; a refused
    /// turn fails it; a definitely-unsent turn goes back to queued.
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
                match self
                    .execute_on(
                        run_id,
                        Command::RequeueAttempt {
                            attempt: attempt_id,
                        },
                    )
                    .await
                {
                    Ok(_) => Ok(()),
                    Err(AppError::Conflict(_)) => Ok(()),
                    Err(other) => Err(other),
                }
            }
            Err(DispatchError::Rejected(_)) => {
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
                    Err(AppError::Conflict(_)) => Ok(()),
                    Err(other) => Err(other),
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
