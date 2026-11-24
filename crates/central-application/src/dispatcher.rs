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
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::ports::store::UserProfileRecord;
use central_domain::{
    admission, classify_rejection, failure_for_rejection, Actor, Admission, Attempt, AttemptId,
    AttemptSession, AttemptStatus, Command, CommandEnvelope, ContentHash, IntegrationCheck,
    ProfileId, ResolvedProfile, RetryPolicy, RunId, RunState, Task, TaskId, TaskKind, TaskStatus,
    Timestamp, WorkRequest, WorkRole,
};

use crate::contracts::{AppError, DispatchError, DispatchSummary, OpenSession, SendTurn};
use crate::fallback::{choose_fallback, select_fallback, FallbackOverrides};
use crate::ports::workspace::{
    AutoIntegrationRequest, SnapshotRequest, WorkspaceManager, WorkspaceRequest,
};
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
    /// Optional root for per-attempt scratch directories. When set, every
    /// dispatched attempt gets its own isolated directory and the gateway
    /// session carries that real path as its working directory; without it
    /// the session carries the inert [`workspace_label`]. Scratch is not a
    /// Git worktree and never a sealed candidate: sealing stays on the
    /// workspace integration path.
    attempt_scratch_root: Option<PathBuf>,
    /// Optional Git workspace manager. When set and the run targets an
    /// existing repository with a synced commit, dispatch snapshots that
    /// commit and creates one detached worktree per attempt instead of a
    /// scratch directory.
    workspace: Option<Arc<dyn WorkspaceManager>>,
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
            attempt_scratch_root: None,
            workspace: None,
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

    /// Isolate every dispatched attempt in its own scratch directory under
    /// `root`, and hand that real path to the gateway as the working
    /// directory. `None` (the default) keeps the previous behavior: the
    /// session carries only the inert [`workspace_label`].
    pub fn with_attempt_scratch_root(mut self, root: Option<PathBuf>) -> Self {
        self.attempt_scratch_root = root;
        self
    }

    /// Give dispatched attempts real Git worktrees when the run targets a
    /// synced repository: snapshot the synced commit and create one
    /// detached worktree per attempt. Without a manager the dispatcher
    /// falls back to scratch directories (or the inert label).
    pub fn with_workspace_manager(mut self, manager: Option<Arc<dyn WorkspaceManager>>) -> Self {
        self.workspace = manager;
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

    /// Resolve a stored owner profile into the harness-facing shape.
    /// Same mapping as the daemon registry used to build: the profile
    /// id falls back to a deterministic digest when the stored string
    /// is not a UUID, so hand-written ids still resolve.
    fn profile_from_record(record: &UserProfileRecord) -> ResolvedProfile {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        record.profile_id.hash(&mut hasher);
        let digest = hasher.finish() as u128;
        ResolvedProfile {
            id: record
                .profile_id
                .parse()
                .unwrap_or_else(|_| central_domain::ProfileId::from_u128(digest)),
            provider: record.provider.clone(),
            model: record.model.clone(),
            effort: record.effort.clone(),
            skills: Vec::new(),
            subagents: Vec::new(),
            permissions: Vec::new(),
            fallbacks: Vec::new(),
            hash: ContentHash::from_hex(format!("profile:{}", record.profile_id)),
        }
    }

    /// Look a stored profile up by id and resolve it for dispatch.
    /// A store read failure defers like a missing profile: the next
    /// tick retries instead of dispatching blind.
    async fn lookup_profile(&self, id: &ProfileId) -> Option<ResolvedProfile> {
        let record = self.store.get_profile(&id.to_string()).await.ok()??;
        Some(Self::profile_from_record(&record))
    }

    /// Resolve the exact profile an attempt dispatches with, in order:
    /// leader reviews use only the run's leader profile; pinned tasks
    /// keep their stored profile; unpinned tasks use the run worker
    /// default; anything else falls through to the base resolver (the
    /// fake-agent dev profile in tests, nothing in production).
    /// `None` defers the attempt without touching it: a configured-but-
    /// missing profile waits for the owner to fix it rather than
    /// dispatching on a substituted agent.
    async fn resolve_for_attempt(
        &self,
        state: &RunState,
        attempt: &Attempt,
        task: &Task,
    ) -> Option<ResolvedProfile> {
        if state.leader_attempts.contains(&attempt.id) {
            let id = state.leader_profile.as_ref()?;
            return self.lookup_profile(id).await;
        }
        if let Some(pinned) = task.profile_id.as_ref() {
            return self.lookup_profile(pinned).await;
        }
        if let Some(default) = state.worker_profile.as_ref() {
            return self.lookup_profile(default).await;
        }
        (self.resolver)(state, task, attempt.id)
    }

    /// Whether the harness still serves this attempt's resolved profile.
    /// A mismatch defers the attempt: Central never swaps profiles or
    /// models silently to make a dispatch fit.
    async fn capabilities_hold(&self, state: &RunState, attempt: &Attempt, task: &Task) -> bool {
        let Some(profile) = self.resolve_for_attempt(state, attempt, task).await else {
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
        // A pending revision proposal holds its impacted queued
        // descendants: they wait for the user's decision and are never
        // dispatched on speculation. Running attempts continue; only
        // new sends are held.
        if state.revision_holds.contains(&attempt.task_id) {
            return Ok(());
        }
        // The budget role follows the attempt: leader reviews draw from
        // the shared ledger like any other turn (the role is audit
        // context, not a separate quota).
        let role = if state.leader_attempts.contains(&attempt_id) {
            WorkRole::Leader
        } else {
            WorkRole::Worker
        };
        if matches!(
            admission(&state, &WorkRequest { role }, self.clock.now()),
            Admission::Held(_)
        ) {
            return Ok(());
        }
        // A leader review dispatches against a parked task on purpose:
        // the review advises without moving the task, so the Running
        // gate below is lifted for leader attempts only. The active-plan
        // binding still holds: a review never dispatches for a
        // superseded plan.
        let is_leader_review = state.leader_attempts.contains(&attempt_id);
        if state.active_plan.as_ref() != Some(&task.plan_hash)
            || (task.status != TaskStatus::Running && !is_leader_review)
        {
            return Ok(());
        }
        // Integration tasks never reach a harness: the mechanical path
        // below seals, checks and promotes through the workspace manager
        // instead of opening a gateway session.
        if task.kind == TaskKind::Integration {
            return self
                .dispatch_integration(run_id, &state, &task, attempt_id)
                .await;
        }
        if !self.capabilities_hold(&state, &attempt, &task).await {
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

        // A live turn needs a real, owned working directory — before the
        // claim, while the attempt is still safely Queued. An unusable
        // root defers the dispatch: the directory is never invented and
        // the turn never runs somewhere unowned.
        // When a workspace manager is attached and the run targets an
        // existing repository with a user-synced commit, the attempt gets
        // a real detached worktree (snapshot + create) instead of
        // scratch: only sealed worktrees can ever feed Git integration.
        // A repository without a synced commit defers — Central never
        // invents the base commit.
        let workspace = match &self.workspace {
            Some(manager) => match self.provision_worktree(manager, &state, attempt_id).await? {
                WorktreeSite::Worktree(path) => path,
                WorktreeSite::Defer => return Ok(()),
                WorktreeSite::Scratch => match &self.attempt_scratch_root {
                    Some(root) => match provision_attempt_scratch(root, run_id, attempt_id) {
                        Ok(dir) => dir.to_string_lossy().into_owned(),
                        Err(_) => return Ok(()),
                    },
                    None => workspace_label(run_id, attempt_id),
                },
            },
            None => match &self.attempt_scratch_root {
                Some(root) => match provision_attempt_scratch(root, run_id, attempt_id) {
                    Ok(dir) => dir.to_string_lossy().into_owned(),
                    Err(_) => return Ok(()),
                },
                None => workspace_label(run_id, attempt_id),
            },
        };

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
        let profile = self
            .resolve_for_attempt(&state, &attempt, &task)
            .await
            .ok_or_else(|| {
                AppError::Capability("attempt profile vanished mid-dispatch".to_string())
            })?;
        let session = match self
            .agent
            .open(OpenSession {
                attempt_id,
                profile,
                workspace,
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

    /// One mechanical integration round, no harness involved. The attempt
    /// must already be queued; the CAS claim below admits exactly one
    /// driver, then the workspace manager seals the predecessor
    /// worktrees, prepares the temporary branch, runs the approved
    /// checks and CAS-promotes the good ref. Anything that is not a
    /// passing round defers with backoff (or parks as failed when
    /// retries are exhausted), exactly like the gateway path — and a
    /// pass records the promotion through `RecordIntegration`.
    /// Predecessors come from the task's data dependencies when set,
    /// else its plain dependencies; each must have a completed attempt
    /// with a live worktree, or the dispatch waits for real work.
    async fn dispatch_integration(
        &self,
        run_id: RunId,
        state: &central_domain::RunState,
        task: &Task,
        attempt_id: AttemptId,
    ) -> Result<(), AppError> {
        let Some(manager) = &self.workspace else {
            // No workspace manager configured: without sealed worktrees
            // there is nothing honest to integrate.
            return Ok(());
        };
        let Some(active) = state.active_plan.as_ref() else {
            return Ok(());
        };
        let spec = state
            .plans
            .get(active)
            .and_then(|revision| revision.document.tasks.iter().find(|t| t.id == task.id))
            .and_then(|plan_task| plan_task.integration.clone());
        let Some(spec) = spec else {
            // No approved checks: wait for a revision instead of running
            // unapproved commands.
            return Ok(());
        };
        if spec.good_ref.trim().is_empty() || spec.check_count() == 0 {
            return Ok(());
        }
        if !std::path::Path::new(&state.repository).is_dir() {
            return Ok(());
        }
        let base = match spec.base_ref.clone() {
            Some(base) => base,
            None => match manager
                .latest_synced_commit(state.id, &state.repository)
                .await
            {
                Ok(Some(commit)) => commit,
                // No synced commit (or an unreadable queue): never invent
                // the base — wait for `central sync`.
                _ => return Ok(()),
            },
        };
        // Seal every predecessor's latest completed attempt. Only
        // completed work integrates, and only from a live worktree the
        // manager still owns: anything else waits for real work.
        let predecessors = if task.data_depends_on.is_empty() {
            &task.depends_on
        } else {
            &task.data_depends_on
        };
        if predecessors.is_empty() {
            return Ok(());
        }
        let mut latest: std::collections::BTreeMap<TaskId, AttemptId> =
            std::collections::BTreeMap::new();
        for predecessor in predecessors {
            let mut best: Option<(Timestamp, AttemptId)> = None;
            for record in state.attempts.values() {
                if &record.task_id == predecessor
                    && record.status == AttemptStatus::Completed
                    && best.as_ref().is_none_or(|(at, _)| record.created_at > *at)
                {
                    best = Some((record.created_at, record.id));
                }
            }
            match best {
                Some((_, attempt)) => {
                    latest.insert(*predecessor, attempt);
                }
                None => return Ok(()),
            }
        }
        let mut sealed = Vec::with_capacity(latest.len());
        for attempt in latest.values() {
            match manager.seal_live(*attempt).await {
                Ok(candidate) => sealed.push(candidate),
                Err(_) => return Ok(()),
            }
        }
        if sealed.is_empty() {
            return Ok(());
        }
        // Claim exactly once: a racing dispatcher loses here and never
        // runs the mechanical round twice.
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
        let to_port = |checks: Vec<IntegrationCheck>| {
            checks
                .into_iter()
                .map(|c| crate::ports::workspace::ApprovedCheck {
                    program: c.program,
                    args: c.args,
                })
                .collect::<Vec<_>>()
        };
        let report = match manager
            .auto_integrate(AutoIntegrationRequest {
                run: run_id,
                candidates: sealed,
                base,
                ref_name: spec.good_ref.clone(),
                expected_parent: None,
                approved_plan: active.clone(),
                baseline: to_port(spec.baseline.clone()),
                affected: to_port(spec.affected.clone()),
                acceptance: to_port(spec.acceptance.clone()),
            })
            .await
        {
            Ok(report) => report,
            Err(_) => return self.defer_or_fail(run_id, attempt_id).await,
        };
        let evidence = report
            .evidence
            .iter()
            .map(|got| {
                format!(
                    "{} {} exit={} timeout={} out={} err={}",
                    got.program,
                    got.args.join(" "),
                    got.exit_code,
                    got.timed_out,
                    got.stdout_hash,
                    got.stderr_hash
                )
            })
            .collect::<Vec<_>>();
        self.execute_on(
            run_id,
            Command::RecordIntegration {
                task: task.id,
                attempt: attempt_id,
                candidate_commit: report.candidate,
                previous_head: report.receipt.previous_head,
                new_head: report.receipt.new_head,
                check_evidence: evidence,
            },
        )
        .await
        .map(|_| ())
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
                max_retries: self.retry_policy.max_retries,
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
        // Leader reviews never fall back to a worker chain: a quota/auth
        // refusal on the leader profile fails loudly instead of silently
        // substituting another agent.
        if state.leader_attempts.contains(&attempt_id) {
            return self.fail_attempt(run_id, attempt_id).await;
        }
        let current = match self.resolve_for_attempt(&state, &attempt, &task).await {
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

/// Provision one isolated scratch directory for an attempt:
/// `<root>/<run>/<attempt>`, created on demand. Components are UUIDs, so
/// no separator can smuggle the path outside the root — and the canonical
/// prefix is still verified before trusting it. Anything unusable defers
/// the dispatch (the attempt stays `Queued`): a working directory is
/// never invented, and a live turn never runs somewhere unowned.
///
/// This is explicitly NOT a Git worktree and never a sealed candidate:
/// it isolates live harness filesystem effects. Sealing stays on the
/// workspace integration path (P3/T5).
pub fn provision_attempt_scratch(
    root: &Path,
    run: RunId,
    attempt: AttemptId,
) -> Result<PathBuf, AppError> {
    // The root itself is explicit configuration: create it if needed so a
    // fresh state dir works on first dispatch. Anything that cannot be a
    // directory (a file, a dead symlink, no permission) fails here and
    // defers the dispatch below.
    std::fs::create_dir_all(root).map_err(|error| {
        AppError::Storage(format!(
            "attempt scratch root is unusable {}: {error}",
            root.display()
        ))
    })?;
    let canonical_root = root.canonicalize().map_err(|error| {
        AppError::Storage(format!(
            "attempt scratch root is unusable {}: {error}",
            root.display()
        ))
    })?;
    for component in [run.to_string(), attempt.to_string()] {
        if component.contains('/') || component.contains('\\') || component.contains('\0') {
            return Err(AppError::Storage(format!(
                "attempt scratch component escapes the root: {component}"
            )));
        }
    }
    let dir = canonical_root
        .join(run.to_string())
        .join(attempt.to_string());
    if !dir.starts_with(&canonical_root) {
        return Err(AppError::Storage(
            "attempt scratch escapes the root; refusing".to_string(),
        ));
    }
    std::fs::create_dir_all(&dir).map_err(|error| {
        AppError::Storage(format!(
            "cannot create attempt scratch {}: {error}",
            dir.display()
        ))
    })?;
    Ok(dir)
}

/// Explicit, ownership-checked removal of one provisioned scratch leaf.
/// Removes nothing outside `root`; missing leaves report `false`.
/// Dropping never cleans up implicitly — callers remove explicitly.
pub fn cleanup_attempt_scratch(
    root: &Path,
    run: RunId,
    attempt: AttemptId,
) -> Result<bool, AppError> {
    let canonical_root = root.canonicalize().map_err(|error| {
        AppError::Storage(format!(
            "attempt scratch root is unusable {}: {error}",
            root.display()
        ))
    })?;
    let dir = canonical_root
        .join(run.to_string())
        .join(attempt.to_string());
    if !dir.starts_with(&canonical_root) {
        return Err(AppError::Storage(
            "attempt scratch escapes the root; refusing".to_string(),
        ));
    }
    if !dir.exists() {
        return Ok(false);
    }
    std::fs::remove_dir_all(&dir).map_err(|error| {
        AppError::Storage(format!(
            "cannot remove attempt scratch {}: {error}",
            dir.display()
        ))
    })?;
    Ok(true)
}

/// Logical workspace label for a dispatch. No directory is created here:
/// isolated worktrees arrive with the workspace integration (P3/T5). The
/// label keeps every attempt addressable until then.
fn workspace_label(run_id: RunId, attempt_id: AttemptId) -> String {
    format!("run/{run_id}/attempt/{attempt_id}")
}

/// Where the next turn runs: a detached Git worktree when the run
/// targets a synced repository, else the configured scratch fallback.
/// Anything unusable defers the dispatch instead of inventing a cwd.
enum WorktreeSite {
    Worktree(String),
    Scratch,
    Defer,
}

impl Dispatcher {
    /// Resolve a real working directory for one dispatch. A repository
    /// path that is not a directory is not a repo at all: fall back to
    /// scratch (or the inert label). A real repository without a
    /// user-synced commit, or any snapshot/create failure, defers the
    /// dispatch — the attempt stays `Queued`, nothing is sent and no
    /// base commit is ever invented.
    async fn provision_worktree(
        &self,
        manager: &Arc<dyn WorkspaceManager>,
        state: &central_domain::RunState,
        attempt_id: AttemptId,
    ) -> Result<WorktreeSite, AppError> {
        use std::path::Path as StdPath;
        if !StdPath::new(&state.repository).is_dir() {
            return Ok(WorktreeSite::Scratch);
        }
        let commit = match manager
            .latest_synced_commit(state.id, &state.repository)
            .await
        {
            Ok(Some(commit)) => commit,
            _ => return Ok(WorktreeSite::Defer),
        };
        let snapshot = match manager
            .snapshot(SnapshotRequest {
                repo_root: state.repository.clone(),
                commit,
                selected_dirty_paths: Vec::new(),
            })
            .await
        {
            Ok(snapshot) => snapshot,
            Err(_) => return Ok(WorktreeSite::Defer),
        };
        match manager
            .create(WorkspaceRequest {
                attempt_id,
                input: snapshot,
            })
            .await
        {
            Ok(workspace) => Ok(WorktreeSite::Worktree(workspace.worktree)),
            Err(_) => Ok(WorktreeSite::Defer),
        }
    }
}
