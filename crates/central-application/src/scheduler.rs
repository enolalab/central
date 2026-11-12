//! DAG scheduler: topo-ready selection with bounded concurrency (P3 Task 1).
//!
//! [`Scheduler::tick`] is the daemon's scan step: it loads one run,
//! selects [`eligible_tasks`](central_domain::scheduling::eligible_tasks)
//! in stable [`TaskId`](central_domain::TaskId) order, queues up to the
//! free concurrency slots through [`Runtime::execute`] (one CAS commit per
//! attempt, so two racing ticks cannot duplicate or overfill), then hands
//! every queued attempt to the [`Dispatcher`](crate::dispatcher::Dispatcher).
//!
//! Queueing decisions recheck control, holds, plan binding, typed
//! dependencies and the named resource config on fresh state for every
//! attempt; dispatch rechecks them again plus harness capabilities. The
//! daemon calls `tick` periodically ([`Scheduler::should_tick`] with the
//! configured cadence) and after new events; scheduling never follows UI
//! animation state.

use std::collections::BTreeMap;
use std::sync::Arc;

use central_domain::{
    admission, eligible_tasks, eligible_tasks_ignoring_gate, live_attempt_count, render_input,
    Actor, Admission, Command, CommandEnvelope, ResolvedProfile, RetryPolicy, RunId, RunState,
    Task, TaskId, WorkRequest, WorkRole,
};

use crate::contracts::{AppError, DispatchSummary};
use crate::dispatcher::Dispatcher;
use crate::fallback::FallbackOverrides;
use crate::ports::{AgentGateway, Clock, IdSource, Store};
use crate::Runtime;

/// Resolves the frozen profile an attempt dispatches with. The scheduler
/// never invents profiles: tests hand one in, production wires the
/// profile layers (P2/T2) here.
pub type ProfileResolver = dyn Fn(&Task) -> Option<ResolvedProfile> + Send + Sync;

/// MVP default: two implementation attempts run side by side.
pub const DEFAULT_MAX_PARALLEL: usize = 2;

/// How often the daemon rescans a run for newly eligible work.
pub const DEFAULT_POLL_INTERVAL_MS: i64 = 5_000;

/// Static scheduling policy: the concurrency bound plus the named
/// exclusive resources (`port:5432`, `db:test`, ...) each task needs.
/// A task with no entry needs no exclusive resource. The safe-retry
/// policy is the plan's configurable default (3 retries at 2s/10s/30s).
#[derive(Clone, Debug)]
pub struct SchedulerConfig {
    pub max_parallel: usize,
    pub resources: BTreeMap<TaskId, Vec<String>>,
    pub poll_interval_ms: i64,
    pub retry_policy: RetryPolicy,
    /// Optional root for per-attempt scratch directories (see
    /// [`Dispatcher::with_attempt_scratch_root`]). `None` keeps dispatch
    /// on inert workspace labels.
    pub attempt_scratch_root: Option<std::path::PathBuf>,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            max_parallel: DEFAULT_MAX_PARALLEL,
            resources: BTreeMap::new(),
            poll_interval_ms: DEFAULT_POLL_INTERVAL_MS,
            retry_policy: RetryPolicy::default(),
            attempt_scratch_root: None,
        }
    }
}

impl SchedulerConfig {
    /// Reject misconfiguration loudly at construction: an empty resource
    /// key or a duplicated key for one task would otherwise fail every
    /// queue attempt at dispatch time.
    pub fn validate(&self) -> Result<(), AppError> {
        self.retry_policy.validate().map_err(AppError::from)?;
        for (task, keys) in &self.resources {
            let mut seen = std::collections::BTreeSet::new();
            for key in keys {
                if key.trim().is_empty() {
                    return Err(AppError::Validation(format!(
                        "scheduler config: task {task} names an empty resource key"
                    )));
                }
                if !seen.insert(key.trim().to_string()) {
                    return Err(AppError::Validation(format!(
                        "scheduler config: task {task} names resource {key} twice"
                    )));
                }
            }
        }
        Ok(())
    }
}

/// What one scheduler tick did: which tasks were queued plus the
/// dispatch outcome (`sent` / `unknown` / `deferred`, mirroring
/// [`DispatchSummary`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ScheduleReport {
    pub queued: Vec<TaskId>,
    pub sent: usize,
    pub unknown: usize,
    pub deferred: usize,
}

/// Scans one run for ready work and dispatches it through the harness.
pub struct Scheduler {
    store: Arc<dyn Store>,
    clock: Arc<dyn Clock>,
    ids: Arc<dyn IdSource>,
    agent: Arc<dyn AgentGateway>,
    config: SchedulerConfig,
    resolver: Arc<ProfileResolver>,
    fallbacks: Arc<FallbackOverrides>,
    actor: Actor,
    workspace: Option<Arc<dyn crate::ports::workspace::WorkspaceManager>>,
}

impl Scheduler {
    pub fn new(
        store: Arc<dyn Store>,
        clock: Arc<dyn Clock>,
        ids: Arc<dyn IdSource>,
        agent: Arc<dyn AgentGateway>,
        config: SchedulerConfig,
        resolver: Arc<ProfileResolver>,
        actor: Actor,
    ) -> Result<Self, AppError> {
        config.validate()?;
        Ok(Self {
            store,
            clock,
            ids,
            agent,
            config,
            resolver,
            fallbacks: Arc::new(FallbackOverrides::default()),
            actor,
            workspace: None,
        })
    }

    /// Attach a Git workspace manager: dispatches for runs targeting a
    /// synced repository get detached worktrees; everything else keeps
    /// the configured scratch behaviour.
    pub fn with_workspace(
        mut self,
        manager: Option<Arc<dyn crate::ports::workspace::WorkspaceManager>>,
    ) -> Self {
        self.workspace = manager;
        self
    }

    /// Register the profile registry the fallback chain resolves
    /// against. Without a registry no fallback can be selected and
    /// quota/auth failures park instead of storming.
    pub fn with_fallback_registry(
        mut self,
        registry: std::collections::BTreeMap<central_domain::ProfileId, ResolvedProfile>,
    ) -> Self {
        self.fallbacks = Arc::new(FallbackOverrides::new(registry));
        self
    }

    /// The effective profile resolver: fallback profiles chosen by
    /// quota/auth handling win over the base resolver for their task,
    /// without rewriting the approved task definition.
    fn effective_resolver(&self) -> Arc<ProfileResolver> {
        let base = Arc::clone(&self.resolver);
        let fallbacks = Arc::clone(&self.fallbacks);
        Arc::new(move |task: &Task| fallbacks.get(task.id).or_else(|| base(task)))
    }

    pub fn config(&self) -> &SchedulerConfig {
        &self.config
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

    fn dispatcher(&self) -> Dispatcher {
        Dispatcher::new(
            Arc::clone(&self.store),
            Arc::clone(&self.clock),
            Arc::clone(&self.ids),
            Arc::clone(&self.agent),
            self.effective_resolver(),
            self.config.resources.clone(),
            self.actor.clone(),
        )
        .with_fallbacks(Arc::clone(&self.fallbacks))
        .with_retry_policy(self.config.retry_policy.clone())
        .with_max_parallel(self.config.max_parallel)
        .with_attempt_scratch_root(self.config.attempt_scratch_root.clone())
        .with_workspace_manager(self.workspace.clone())
    }

    /// Whether the daemon's periodic scan is due.
    pub fn should_tick(
        &self,
        last_tick: central_domain::Timestamp,
        now: central_domain::Timestamp,
    ) -> bool {
        now.0.saturating_sub(last_tick.0) >= self.config.poll_interval_ms
    }

    /// The exact input one attempt starts with: the task objective plus
    /// the verified outputs of its data prerequisites, pinned by hash.
    /// Deterministic for a given snapshot. Delegates to the domain so the
    /// reuse quarantine re-renders the identical string at re-verdict.
    pub fn render_input(state: &RunState, task: &Task) -> String {
        render_input(state, task)
    }

    /// Scan one run and dispatch what is ready. Queueing and dispatch
    /// share nothing but fresh store loads: every attempt is considered
    /// against current state, one CAS commit at a time.
    pub async fn tick(&self, run_id: RunId) -> Result<ScheduleReport, AppError> {
        let mut report = ScheduleReport::default();
        let runtime = self.runtime_for(run_id);
        let now = runtime.clock().now();
        let state = self
            .store
            .load(run_id)
            .await?
            .ok_or_else(|| AppError::NotFound(format!("run {run_id} not found")))?;
        // The shared-budget gate closes alongside the control/holds
        // gate: an unknown usage with cost limits, or an exhausted
        // limit, holds every new attempt (leaders and verifiers
        // included) while running attempts finish untouched.
        let budget_held = matches!(
            admission(
                &state,
                &WorkRequest {
                    role: WorkRole::Worker
                },
                now
            ),
            Admission::Held(_)
        );
        if state.control != central_domain::RunControl::Running
            || !state.holds.is_empty()
            || budget_held
        {
            // The gate is closed (pause/stop/hold/budget/awaiting resume):
            // running attempts continue untouched, nothing new queues.
            // Deferred counts the work the gate is holding back, not zero.
            report.deferred = eligible_tasks_ignoring_gate(&state, now).len();
            return Ok(report);
        }

        let eligible = eligible_tasks(&state, now);
        for task_id in eligible {
            // Fresh state per attempt: a racing tick's commits are
            // visible here, so slots and claims are never oversubscribed
            // beyond one in-flight commit window.
            let fresh = self
                .store
                .load(run_id)
                .await?
                .ok_or_else(|| AppError::NotFound(format!("run {run_id} not found")))?;
            if fresh.control != central_domain::RunControl::Running || !fresh.holds.is_empty() {
                report.deferred += 1;
                continue;
            }
            if matches!(
                admission(
                    &fresh,
                    &WorkRequest {
                        role: WorkRole::Worker
                    },
                    now
                ),
                Admission::Held(_)
            ) {
                report.deferred += 1;
                continue;
            }
            if live_attempt_count(&fresh) >= self.config.max_parallel {
                report.deferred += 1;
                continue;
            }
            let task = fresh
                .tasks
                .get(&task_id)
                .cloned()
                .ok_or_else(|| AppError::NotFound(format!("task {task_id} not found")))?;
            let input = Self::render_input(&fresh, &task);
            let resources = self
                .config
                .resources
                .get(&task_id)
                .cloned()
                .unwrap_or_default();
            let command = Command::QueueAttempt {
                attempt: self.ids.next_attempt_id(),
                task: task_id,
                input,
                resources,
                max_parallel: self.config.max_parallel,
            };
            let envelope = CommandEnvelope {
                id: self.ids.next_command_id(),
                expected_revision: None,
                command,
            };
            match runtime.execute(self.actor.clone(), envelope).await {
                Ok(_) => report.queued.push(task_id),
                // A racing tick won the queue, the plan moved, a
                // dependency closed, or a claim/bound blocked us: defer,
                // never fail the whole scan.
                Err(AppError::Conflict(_)) => report.deferred += 1,
                Err(other) => return Err(other),
            }
        }

        let summary: DispatchSummary = self.dispatcher().dispatch_queued(run_id).await?;
        report.sent = summary.dispatched;
        report.unknown = summary.unknown;
        report.deferred += summary.still_pending;
        Ok(report)
    }
}
