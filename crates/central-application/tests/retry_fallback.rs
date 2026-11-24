//! P3 Task 3: classified retry and profile fallback (TDD: RED first).
//!
//! Safe `DefinitelyNotSent` retries at most 3 times with virtual-clock
//! delays of 2s/10s/30s and never sleep for real; `OutcomeUnknown`
//! never retries; quota/auth failures never storm — they take the
//! user-granted fallback chain (old attempt confirmed stopped, new
//! attempt plus an audited notification) or park when the budget gate
//! is held. A fallback profile must meet harness capability and must
//! never raise permissions.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use central_application::contracts::{
    AppError, CancelObservation, Capabilities, CommitBatch, DispatchError, ExecutionRef,
    ExternalAnswer, OutboxIntent, OutboxStatus, Receipt, ReconcileEvidence, SendTurn, SessionRef,
    TurnRef,
};
use central_application::fallback::select_fallback;
use central_application::ports::{AgentGateway, Clock, IdSource, Store};
use central_application::scheduler::{Scheduler, SchedulerConfig};
use central_application::Runtime;
use central_domain::{
    Actor, ActorId, ActorKind, Attempt, AttemptId, AttemptStatus, Command, CommandEnvelope,
    CommandId, ContentHash, IntentId, Observation, PlanDocumentV1, PlanTaskV1, ProfileId,
    ResolvedProfile, RetryPolicy, Revision, RunId, RunState, TaskId, TaskKind, TaskStatus,
    Timestamp, UsageLimits, UsageObservation, VerifySpec,
};

// ---------- deterministic fakes ----------

struct FakeClock(Arc<AtomicI64>);

impl FakeClock {
    fn advance(&self, delta_ms: i64) {
        self.0.fetch_add(delta_ms, Ordering::SeqCst);
    }
}

impl Clock for FakeClock {
    fn now(&self) -> Timestamp {
        Timestamp(self.0.load(Ordering::SeqCst))
    }
}

struct FakeIds {
    next: AtomicU64,
    attempts: AtomicU64,
}

impl FakeIds {
    fn new() -> Self {
        Self {
            next: AtomicU64::new(0),
            attempts: AtomicU64::new(9_000),
        }
    }
}

impl IdSource for FakeIds {
    fn next_run_id(&self) -> RunId {
        RunId::from_u128(5_000 + self.next.fetch_add(1, Ordering::SeqCst) as u128)
    }
    fn next_intent_id(&self) -> IntentId {
        IntentId::from_u128(6_000 + self.next.fetch_add(1, Ordering::SeqCst) as u128)
    }
    fn next_command_id(&self) -> CommandId {
        CommandId::from_u128(7_000 + self.next.fetch_add(1, Ordering::SeqCst) as u128)
    }
    fn next_attempt_id(&self) -> AttemptId {
        AttemptId::from_u128(self.attempts.fetch_add(1, Ordering::SeqCst) as u128)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SendScript {
    Ok,
    NotSent,
    Unknown,
    Quota,
}

struct FakeGateway {
    sends: AtomicU64,
    script: Mutex<SendScript>,
    models: Mutex<Vec<String>>,
}

impl FakeGateway {
    fn new() -> Self {
        Self {
            sends: AtomicU64::new(0),
            script: Mutex::new(SendScript::Ok),
            models: Mutex::new(Vec::new()),
        }
    }

    fn set_script(&self, script: SendScript) {
        *self.script.lock().expect("lock") = script;
    }

    fn send_count(&self) -> u64 {
        self.sends.load(Ordering::SeqCst)
    }

    fn sent_models(&self) -> Vec<String> {
        self.models.lock().expect("lock").clone()
    }
}

#[async_trait]
impl AgentGateway for FakeGateway {
    async fn capabilities(&self, profile: &ResolvedProfile) -> Result<Capabilities, AppError> {
        if profile.provider != "fake" {
            return Err(AppError::Capability("unknown provider".to_string()));
        }
        Ok(Capabilities {
            provider: "fake".to_string(),
            supports_resume: true,
            features: vec![
                "model:fake-model".to_string(),
                "model:fake-small".to_string(),
            ],
        })
    }

    async fn open(
        &self,
        request: central_application::contracts::OpenSession,
    ) -> Result<SessionRef, AppError> {
        self.models
            .lock()
            .expect("lock")
            .push(request.profile.model.clone());
        Ok(SessionRef {
            provider: "fake".to_string(),
            instance_epoch: "epoch-1".to_string(),
            thread_id: "thread-1".to_string(),
        })
    }

    async fn send(&self, request: SendTurn) -> Result<TurnRef, DispatchError> {
        self.sends.fetch_add(1, Ordering::SeqCst);
        match *self.script.lock().expect("lock") {
            SendScript::Ok => Ok(TurnRef {
                session: request.session,
                turn_id: "turn-1".to_string(),
            }),
            SendScript::NotSent => Err(DispatchError::DefinitelyNotSent(
                "transport dropped before send (scripted)".to_string(),
            )),
            SendScript::Unknown => Err(DispatchError::OutcomeUnknown(
                "lost after send (scripted)".to_string(),
            )),
            SendScript::Quota => Err(DispatchError::Rejected(
                "quota exceeded for model fake-model (scripted)".to_string(),
            )),
        }
    }

    async fn observe(&self, _session: &SessionRef) -> Result<Vec<Observation>, AppError> {
        Ok(Vec::new())
    }

    async fn answer(&self, _answer: ExternalAnswer) -> Result<(), DispatchError> {
        Ok(())
    }

    async fn cancel(&self, _target: &ExecutionRef) -> Result<CancelObservation, AppError> {
        Ok(CancelObservation {
            cancelled: true,
            detail: "cancelled".to_string(),
        })
    }

    async fn reconcile(&self, target: &ExecutionRef) -> Result<ReconcileEvidence, AppError> {
        Ok(ReconcileEvidence {
            attempt_id: target.attempt_id,
            status: AttemptStatus::UnknownOutcome,
            detail: "no evidence".to_string(),
        })
    }
}

// ---------- in-memory store ----------

#[derive(Default)]
struct Inner {
    states: HashMap<RunId, RunState>,
    receipts: HashMap<(ActorId, CommandId), Receipt>,
    intents: HashMap<RunId, Vec<OutboxIntent>>,
}

#[derive(Default)]
struct MemoryStore {
    inner: Mutex<Inner>,
}

impl MemoryStore {
    fn load_sync(&self, run: RunId) -> RunState {
        self.inner
            .lock()
            .expect("lock")
            .states
            .get(&run)
            .cloned()
            .expect("run seeded")
    }
}

#[async_trait]
impl Store for MemoryStore {
    async fn load(&self, id: RunId) -> Result<Option<RunState>, AppError> {
        Ok(self.inner.lock().expect("lock").states.get(&id).cloned())
    }

    async fn receipt(&self, actor: ActorId, id: CommandId) -> Result<Option<Receipt>, AppError> {
        Ok(self
            .inner
            .lock()
            .expect("lock")
            .receipts
            .get(&(actor, id))
            .cloned())
    }

    async fn commit(&self, batch: CommitBatch) -> Result<Receipt, AppError> {
        let mut inner = self.inner.lock().expect("lock");
        if let Some(existing) = inner.receipts.get(&(batch.actor, batch.command_id)) {
            if existing.payload_hash != batch.payload_hash {
                return Err(AppError::Conflict("different payload".to_string()));
            }
            return Ok(existing.clone());
        }
        let current = inner
            .states
            .get(&batch.next_state.id)
            .map(|s| s.revision)
            .unwrap_or(Revision(0));
        if current != batch.expected_revision {
            return Err(AppError::Conflict("revision conflict".to_string()));
        }
        let run = batch.next_state.id;
        inner.states.insert(run, batch.next_state);
        inner.intents.entry(run).or_default().extend(batch.intents);
        inner
            .receipts
            .insert((batch.actor, batch.command_id), batch.receipt.clone());
        Ok(batch.receipt)
    }

    async fn pending(&self, id: RunId) -> Result<Vec<OutboxIntent>, AppError> {
        Ok(self
            .inner
            .lock()
            .expect("lock")
            .intents
            .get(&id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|i| i.status == OutboxStatus::Pending)
            .collect())
    }

    async fn list_run_ids(&self) -> Result<Vec<RunId>, AppError> {
        let mut ids: Vec<RunId> = self
            .inner
            .lock()
            .expect("lock")
            .states
            .keys()
            .copied()
            .collect();
        ids.sort();
        Ok(ids)
    }
}

// ---------- profiles ----------

const PRIMARY_ID: u128 = 21;
const GOOD_ID: u128 = 22;
const EVIL_ID: u128 = 23;

fn primary_profile() -> ResolvedProfile {
    ResolvedProfile {
        id: ProfileId::from_u128(PRIMARY_ID),
        provider: "fake".to_string(),
        model: "fake-model".to_string(),
        effort: "low".to_string(),
        skills: Vec::new(),
        subagents: Vec::new(),
        permissions: vec!["read".to_string(), "write".to_string()],
        // Evil first on purpose: selection must skip it and take good.
        fallbacks: vec![ProfileId::from_u128(EVIL_ID), ProfileId::from_u128(GOOD_ID)],
        hash: ContentHash::from_hex("primary-profile"),
    }
}

fn good_fallback() -> ResolvedProfile {
    ResolvedProfile {
        id: ProfileId::from_u128(GOOD_ID),
        provider: "fake".to_string(),
        model: "fake-small".to_string(),
        effort: "low".to_string(),
        skills: Vec::new(),
        subagents: Vec::new(),
        permissions: vec!["read".to_string()],
        fallbacks: Vec::new(),
        hash: ContentHash::from_hex("good-profile"),
    }
}

fn evil_fallback() -> ResolvedProfile {
    ResolvedProfile {
        id: ProfileId::from_u128(EVIL_ID),
        provider: "fake".to_string(),
        model: "fake-small".to_string(),
        effort: "low".to_string(),
        skills: Vec::new(),
        subagents: Vec::new(),
        // Raises permissions: must never be selected.
        permissions: vec!["read".to_string(), "write".to_string(), "admin".to_string()],
        fallbacks: Vec::new(),
        hash: ContentHash::from_hex("evil-profile"),
    }
}

fn registry() -> BTreeMap<ProfileId, ResolvedProfile> {
    BTreeMap::from([
        (ProfileId::from_u128(GOOD_ID), good_fallback()),
        (ProfileId::from_u128(EVIL_ID), evil_fallback()),
    ])
}

fn capabilities() -> Capabilities {
    Capabilities {
        provider: "fake".to_string(),
        supports_resume: true,
        features: vec![
            "model:fake-model".to_string(),
            "model:fake-small".to_string(),
        ],
    }
}

// ---------- harness ----------

fn user() -> Actor {
    Actor {
        id: ActorId::from_u128(100),
        kind: ActorKind::User,
    }
}

fn system() -> Actor {
    Actor {
        id: ActorId::from_u128(101),
        kind: ActorKind::System,
    }
}

fn verify_spec() -> VerifySpec {
    VerifySpec {
        method: "run the approved check".to_string(),
        criteria: vec!["the approved check exits zero".to_string()],
    }
}

fn plan_task(id: TaskId) -> PlanTaskV1 {
    PlanTaskV1 {
        id,
        kind: TaskKind::Implementation,
        objective: format!("implement task {id}"),
        inputs: Vec::new(),
        scope: Vec::new(),
        out_of_scope: Vec::new(),
        depends_on: Vec::new(),
        data_depends_on: Vec::new(),
        outputs: vec![format!("output-{id}")],
        verify: Some(verify_spec()),
        evidence: Vec::new(),
        profile_id: None,
        permissions: Vec::new(),
        max_repair_rounds: 1,
        requires_approval: Vec::new(),
        handover: String::new(),
        integration: None,
    }
}

struct Harness {
    store: Arc<MemoryStore>,
    clock: Arc<FakeClock>,
    ids: Arc<FakeIds>,
    gateway: Arc<FakeGateway>,
    run_id: RunId,
}

impl Harness {
    fn new() -> Self {
        Self {
            store: Arc::new(MemoryStore::default()),
            clock: Arc::new(FakeClock(Arc::new(AtomicI64::new(1_700_000_000_000)))),
            ids: Arc::new(FakeIds::new()),
            gateway: Arc::new(FakeGateway::new()),
            run_id: RunId::from_u128(10),
        }
    }

    fn runtime(&self) -> Runtime {
        Runtime::new(
            self.store.clone() as Arc<dyn Store>,
            self.clock.clone() as Arc<dyn Clock>,
            self.ids.clone() as Arc<dyn IdSource>,
            self.gateway.clone() as Arc<dyn AgentGateway>,
            self.run_id,
        )
    }

    fn scheduler(&self) -> Scheduler {
        Scheduler::new(
            self.store.clone() as Arc<dyn Store>,
            self.clock.clone() as Arc<dyn Clock>,
            self.ids.clone() as Arc<dyn IdSource>,
            self.gateway.clone() as Arc<dyn AgentGateway>,
            SchedulerConfig::default(),
            Arc::new(|_, _, _| Some(primary_profile())),
            system(),
        )
        .expect("valid scheduler config")
        .with_fallback_registry(registry())
    }

    fn scheduler_with_retry_policy(&self, retry_policy: RetryPolicy) -> Scheduler {
        Scheduler::new(
            self.store.clone() as Arc<dyn Store>,
            self.clock.clone() as Arc<dyn Clock>,
            self.ids.clone() as Arc<dyn IdSource>,
            self.gateway.clone() as Arc<dyn AgentGateway>,
            SchedulerConfig {
                retry_policy,
                ..SchedulerConfig::default()
            },
            Arc::new(|_, _, _| Some(primary_profile())),
            system(),
        )
        .expect("valid scheduler config")
        .with_fallback_registry(registry())
    }

    async fn execute(&self, actor: Actor, command: Command) {
        let envelope = CommandEnvelope {
            id: self.ids.next_command_id(),
            expected_revision: None,
            command,
        };
        self.runtime()
            .execute(actor, envelope)
            .await
            .expect("command commits");
    }

    async fn single_task_run(&self) -> TaskId {
        let task = TaskId::from_u128(1);
        self.execute(
            user(),
            Command::CreateRun {
                goal: "fallback workflow".to_string(),
                repository: "repo".to_string(),
            },
        )
        .await;
        self.execute(
            user(),
            Command::ProposePlan {
                plan: PlanDocumentV1 {
                    version: 1,
                    goal: "fallback workflow".to_string(),
                    tasks: vec![plan_task(task)],
                },
            },
        )
        .await;
        let hash = self.load().pending_plan.clone().expect("pending plan");
        self.execute(user(), Command::ApprovePlan { plan_hash: hash })
            .await;
        task
    }

    fn load(&self) -> RunState {
        self.store.load_sync(self.run_id)
    }

    fn attempts_of(&self, task: TaskId) -> Vec<Attempt> {
        self.load()
            .attempts
            .values()
            .filter(|attempt| attempt.task_id == task)
            .cloned()
            .collect()
    }

    fn task_status(&self, task: TaskId) -> TaskStatus {
        self.load().tasks.get(&task).expect("task").status
    }
}

// ---------- tests ----------

#[tokio::test]
async fn fallback_keeps_permissions_and_budget() {
    // Pure selection first: the evil candidate raises permissions, so the
    // good one wins even though evil is earlier in the chain.
    let chosen = select_fallback(&primary_profile(), &registry(), &capabilities())
        .expect("a fallback must be selected");
    assert_eq!(chosen.id, ProfileId::from_u128(GOOD_ID));
    for permission in &chosen.permissions {
        assert!(
            primary_profile().permissions.contains(permission),
            "fallback must never raise permissions: {permission}"
        );
    }
    // No chain entry may raise permissions or miss capability.
    let mut only_evil = BTreeMap::new();
    only_evil.insert(ProfileId::from_u128(EVIL_ID), evil_fallback());
    assert!(
        select_fallback(&primary_profile(), &only_evil, &capabilities()).is_none(),
        "a permission-raising fallback must never be selected"
    );

    // Through the dispatcher: a held budget opens zero new attempts,
    // even under quota. The first attempt is queued while the gate is
    // open; then the budget exhausts (first unknown usage with cost
    // limits, then spent tokens over the cap). Every later tick must
    // send nothing and queue nothing: the parked attempt is not
    // retried, and no fallback continuation opens. One long-lived
    // scheduler: fallback overrides live in the scheduler, exactly
    // like the daemon.
    let harness = Harness::new();
    let task = harness.single_task_run().await;
    let scheduler = harness.scheduler();
    harness
        .execute(
            user(),
            Command::QueueAttempt {
                attempt: harness.ids.next_attempt_id(),
                task,
                input: "implement task".to_string(),
                resources: Vec::new(),
                max_parallel: 2,
            },
        )
        .await;
    harness
        .execute(
            user(),
            Command::SetLimits {
                limits: UsageLimits {
                    max_tokens: Some(1_000),
                    max_cost_minor: None,
                    max_attempts: None,
                    max_duration_ms: None,
                    extra_caps: Default::default(),
                    updated_by: None,
                },
            },
        )
        .await;
    harness
        .execute(
            user(),
            Command::ReconcileUsage {
                observation: UsageObservation {
                    provider: "fake".to_string(),
                    epoch: "epoch-1".to_string(),
                    turn: "turn-9".to_string(),
                    sequence: 9,
                    tokens: 0,
                    cost_minor: None,
                    cumulative_tokens: None,
                    cumulative_cost_minor: None,
                    finality: central_domain::UsageFinality::MissingFinal,
                    metric: "tokens".to_string(),
                    source: "reported".to_string(),
                },
            },
        )
        .await;
    harness.gateway.set_script(SendScript::Quota);
    scheduler.tick(harness.run_id).await.expect("tick held");
    // Unknown usage with cost limits: nothing sends, nothing queues.
    assert_eq!(harness.attempts_of(task).len(), 1);
    assert_eq!(harness.attempts_of(task)[0].status, AttemptStatus::Queued);
    assert_eq!(harness.gateway.send_count(), 0);
    let new_fallback_attempts = harness.attempts_of(task).len().saturating_sub(1);
    assert_eq!(new_fallback_attempts, 0);

    // The spent-tokens-over-cap variant holds the same way.
    harness
        .execute(
            user(),
            Command::ResolveUsageGap {
                missing_key: "fake/epoch-1/turn-9/9".to_string(),
                note: "gap audited; moving to the spent-budget variant".to_string(),
            },
        )
        .await;
    harness
        .execute(
            user(),
            Command::ReconcileUsage {
                observation: UsageObservation {
                    provider: "fake".to_string(),
                    epoch: "epoch-1".to_string(),
                    turn: "turn-1".to_string(),
                    sequence: 1,
                    tokens: 0,
                    cost_minor: None,
                    cumulative_tokens: Some(1_500),
                    cumulative_cost_minor: None,
                    finality: central_domain::UsageFinality::Observed,
                    metric: "tokens".to_string(),
                    source: "reported".to_string(),
                },
            },
        )
        .await;
    scheduler.tick(harness.run_id).await.expect("tick broke");
    assert_eq!(harness.attempts_of(task).len(), 1);
    assert_eq!(harness.attempts_of(task)[0].status, AttemptStatus::Queued);
    assert_eq!(harness.gateway.send_count(), 0);
    let new_fallback_attempts = harness.attempts_of(task).len().saturating_sub(1);
    assert_eq!(new_fallback_attempts, 0);

    // With budget restored the parked attempt dispatches, hits the
    // same quota, and takes the good fallback: the quota-hit attempt
    // stays Failed (confirmed stopped, never retried), exactly one
    // continuation opens, and it dispatches on the fallback model.
    harness
        .execute(
            user(),
            Command::SetLimits {
                limits: UsageLimits {
                    max_tokens: Some(10_000),
                    max_cost_minor: None,
                    max_attempts: None,
                    max_duration_ms: None,
                    extra_caps: Default::default(),
                    updated_by: None,
                },
            },
        )
        .await;
    scheduler.tick(harness.run_id).await.expect("tick 2");
    let attempts = harness.attempts_of(task);
    assert_eq!(attempts.len(), 2);
    assert_eq!(
        attempts
            .iter()
            .filter(|attempt| attempt.status == AttemptStatus::Failed)
            .count(),
        1,
        "the quota-hit attempt is confirmed stopped, never retried"
    );
    // The continuation dispatches on the good fallback's model: the
    // scheduler froze the selected profile for the new attempt.
    harness.gateway.set_script(SendScript::Ok);
    scheduler.tick(harness.run_id).await.expect("tick 3");
    assert_eq!(
        harness.gateway.sent_models(),
        vec!["fake-model".to_string(), "fake-small".to_string(),]
    );
}

#[tokio::test]
async fn quota_has_no_retry_storm() {
    let harness = Harness::new();
    let task = harness.single_task_run().await;
    harness.gateway.set_script(SendScript::Quota);

    // Tick 1: the first attempt hits quota and the chain opens one
    // fallback attempt. Tick 2 dispatches the continuation on the
    // fallback profile, which has no further chain, so it parks. Ticks
    // 3..5 (with large virtual-clock jumps) must not open anything
    // else: quota never retries. One long-lived scheduler, like the
    // daemon.
    let scheduler = harness.scheduler();
    for _ in 0..5 {
        scheduler.tick(harness.run_id).await.expect("tick");
        harness.clock.advance(60_000);
    }

    let attempts = harness.attempts_of(task);
    assert_eq!(attempts.len(), 2, "quota opens one fallback, never a storm");
    assert!(
        harness.load().retry_used.is_empty(),
        "quota must not consume transport retry budget"
    );
    assert_eq!(harness.gateway.send_count(), 2);
    assert!(
        harness
            .attempts_of(task)
            .iter()
            .all(|attempt| attempt.status == AttemptStatus::Failed),
        "both quota-hit attempts park as failed, recoverable via ReopenTask"
    );

    // And recovery stays possible: reopening reopens the gate.
    harness.execute(user(), Command::ReopenTask { task }).await;
    assert_eq!(harness.task_status(task), TaskStatus::Ready);
}

#[tokio::test]
async fn retry_backoff_uses_virtual_clock() {
    let harness = Harness::new();
    let task = harness.single_task_run().await;
    harness.gateway.set_script(SendScript::NotSent);
    let scheduler = harness.scheduler();

    // First send fails safe-not-sent: the attempt parks Queued with a
    // 2s backoff, not a real sleep.
    scheduler.tick(harness.run_id).await.expect("tick 1");
    assert_eq!(harness.gateway.send_count(), 1);
    assert_eq!(harness.attempts_of(task).len(), 1);

    // Immediate and early ticks never resend.
    scheduler.tick(harness.run_id).await.expect("tick 2");
    harness.clock.advance(1_999);
    scheduler.tick(harness.run_id).await.expect("tick 3");
    assert_eq!(harness.gateway.send_count(), 1);

    // At +2s the first retry goes out and parks with a 10s backoff.
    harness.clock.advance(1);
    scheduler.tick(harness.run_id).await.expect("tick 4");
    assert_eq!(harness.gateway.send_count(), 2);
    scheduler.tick(harness.run_id).await.expect("tick 5");
    harness.clock.advance(9_999);
    scheduler.tick(harness.run_id).await.expect("tick 6");
    assert_eq!(harness.gateway.send_count(), 2);

    // At +10s the second retry goes out with a 30s backoff, then the
    // third and final retry, then exhaustion parks the attempt.
    harness.clock.advance(1);
    scheduler.tick(harness.run_id).await.expect("tick 7");
    assert_eq!(harness.gateway.send_count(), 3);
    harness.clock.advance(29_999);
    scheduler.tick(harness.run_id).await.expect("tick 8");
    assert_eq!(harness.gateway.send_count(), 3);
    harness.clock.advance(1);
    scheduler.tick(harness.run_id).await.expect("tick 9");
    assert_eq!(harness.gateway.send_count(), 4);
    harness.clock.advance(60_000);
    scheduler.tick(harness.run_id).await.expect("tick 10");
    assert_eq!(harness.gateway.send_count(), 4);
    assert_eq!(
        harness.attempts_of(task)[0].status,
        AttemptStatus::Failed,
        "exhausted safe retries park the attempt instead of storming"
    );

    // Exhaustion is recoverable: reopening reopens the task.
    harness.execute(user(), Command::ReopenTask { task }).await;
    assert_eq!(harness.task_status(task), TaskStatus::Ready);
}

#[tokio::test]
async fn outcome_unknown_never_retries() {
    let harness = Harness::new();
    let task = harness.single_task_run().await;
    harness.gateway.set_script(SendScript::Unknown);
    let scheduler = harness.scheduler();

    scheduler.tick(harness.run_id).await.expect("tick 1");
    assert_eq!(harness.gateway.send_count(), 1);
    assert_eq!(
        harness.attempts_of(task)[0].status,
        AttemptStatus::UnknownOutcome
    );

    // Large virtual-clock jumps and repeat ticks must never resend the
    // ambiguous attempt.
    for _ in 0..3 {
        harness.clock.advance(120_000);
        scheduler.tick(harness.run_id).await.expect("tick");
    }
    assert_eq!(harness.gateway.send_count(), 1);
    assert_eq!(
        harness.attempts_of(task)[0].status,
        AttemptStatus::UnknownOutcome
    );
    assert!(harness.load().retry_used.is_empty());
}

#[tokio::test]
async fn configured_max_retries_above_default_allows_fifth_retry() {
    // A deployment-configured bound of 5 must schedule all 5 retries
    // (initial send + 5 = 6 sends). The domain used to hardcode the
    // default 3, silently capping any larger policy at the third retry.
    let harness = Harness::new();
    let task = harness.single_task_run().await;
    harness.gateway.set_script(SendScript::NotSent);
    let scheduler = harness.scheduler_with_retry_policy(RetryPolicy {
        max_retries: 5,
        delays_ms: vec![2_000, 10_000, 30_000, 30_000, 30_000],
    });

    for (step, gap) in [0, 2_000, 10_000, 30_000, 30_000, 30_000]
        .iter()
        .enumerate()
    {
        harness.clock.advance(*gap);
        scheduler.tick(harness.run_id).await.expect("tick");
        assert_eq!(
            harness.gateway.send_count(),
            (step + 1) as u64,
            "send {} must go out under a max_retries=5 policy",
            step + 1
        );
    }
    let state = harness.load();
    assert_eq!(
        state.retry_used.get(&harness.attempts_of(task)[0].id),
        Some(&5),
        "all 5 configured retries must be recorded, not capped at 3"
    );
    assert_eq!(
        harness.attempts_of(task)[0].status,
        AttemptStatus::Failed,
        "the configured bound exhausts after the 5th retry, parking recoverably"
    );

    // Exhaustion stays recoverable: reopening reopens the task.
    harness.execute(user(), Command::ReopenTask { task }).await;
    assert_eq!(harness.task_status(task), TaskStatus::Ready);
}

#[tokio::test]
async fn configured_max_retries_below_default_parks_early() {
    // A tighter bound of 1 must park after the first retry (2 sends
    // total), not ride the default 3.
    let harness = Harness::new();
    let task = harness.single_task_run().await;
    harness.gateway.set_script(SendScript::NotSent);
    let scheduler = harness.scheduler_with_retry_policy(RetryPolicy {
        max_retries: 1,
        delays_ms: vec![2_000],
    });

    scheduler.tick(harness.run_id).await.expect("tick 1");
    assert_eq!(harness.gateway.send_count(), 1);
    harness.clock.advance(2_000);
    scheduler.tick(harness.run_id).await.expect("tick 2");
    assert_eq!(harness.gateway.send_count(), 2);
    assert_eq!(
        harness.attempts_of(task)[0].status,
        AttemptStatus::Failed,
        "a max_retries=1 policy parks after the first retry"
    );

    // A parked attempt never resends, however far the clock jumps.
    harness.clock.advance(120_000);
    scheduler.tick(harness.run_id).await.expect("tick 3");
    assert_eq!(harness.gateway.send_count(), 2);
}
