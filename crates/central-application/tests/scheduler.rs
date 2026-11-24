//! P3 Task 1: DAG scheduling, concurrency and resources (TDD: RED first).
//!
//! The scheduler opens only verified-output dependents, queues at most
//! `max_parallel` live attempts, keeps named port/db resources exclusive,
//! and never duplicates an attempt across ticks. Unknown-outcome branches
//! hold their descendants and resources while independent work proceeds.
//! No test here sleeps: the fake gateway models delay against the shared
//! virtual clock, and concurrency is exercised with real tokio tasks.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use central_application::contracts::{
    AppError, CancelObservation, Capabilities, CommitBatch, DispatchError, ExecutionRef,
    ExternalAnswer, OpenSession, OutboxIntent, OutboxStatus, Receipt, ReconcileEvidence, SendTurn,
    SessionRef, TurnRef,
};
use central_application::ports::{AgentGateway, Clock, IdSource, Store};
use central_application::scheduler::{ScheduleReport, Scheduler, SchedulerConfig};
use central_application::Runtime;
use central_domain::scheduling::eligible_tasks;
use central_domain::{
    Actor, ActorId, ActorKind, Attempt, AttemptId, AttemptStatus, Command, CommandEnvelope,
    CommandId, ContentHash, IntentId, Observation, PlanDocumentV1, PlanTaskV1, ResolvedProfile,
    Revision, RunId, RunState, TaskId, TaskKind, Timestamp, VerifySpec,
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
    Unknown,
    Rejected,
}

struct FakeGateway {
    clock: Arc<AtomicI64>,
    delay_ms: AtomicI64,
    opens: AtomicUsize,
    sends: AtomicUsize,
    cancels: AtomicUsize,
    script: Mutex<SendScript>,
    block_sends: AtomicBool,
    release_sends: AtomicBool,
    workspaces: Mutex<Vec<String>>,
}

impl FakeGateway {
    fn new(clock: Arc<AtomicI64>) -> Self {
        Self {
            clock,
            delay_ms: AtomicI64::new(0),
            opens: AtomicUsize::new(0),
            sends: AtomicUsize::new(0),
            cancels: AtomicUsize::new(0),
            script: Mutex::new(SendScript::Ok),
            block_sends: AtomicBool::new(false),
            release_sends: AtomicBool::new(false),
            workspaces: Mutex::new(Vec::new()),
        }
    }

    fn workspaces(&self) -> Vec<String> {
        self.workspaces.lock().expect("lock").clone()
    }

    fn set_script(&self, script: SendScript) {
        *self.script.lock().expect("lock") = script;
    }

    fn send_count(&self) -> usize {
        self.sends.load(Ordering::SeqCst)
    }

    fn cancel_count(&self) -> usize {
        self.cancels.load(Ordering::SeqCst)
    }

    fn block_until_released(&self) {
        self.block_sends.store(true, Ordering::SeqCst);
    }

    fn release(&self) {
        self.release_sends.store(true, Ordering::SeqCst);
    }

    /// Virtual delay against the shared fake clock plus an optional
    /// rendezvous barrier. Never a real sleep: the loop only yields.
    async fn virtual_delay(&self) {
        let due = self.clock.load(Ordering::SeqCst) + self.delay_ms.load(Ordering::SeqCst);
        let mut spins = 0;
        while self.clock.load(Ordering::SeqCst) < due && spins < 10_000 {
            spins += 1;
            tokio::task::yield_now().await;
        }
        if self.block_sends.load(Ordering::SeqCst) {
            let mut spins = 0;
            while !self.release_sends.load(Ordering::SeqCst) && spins < 100_000 {
                spins += 1;
                tokio::task::yield_now().await;
            }
        }
    }
}

#[async_trait]
impl AgentGateway for FakeGateway {
    async fn capabilities(&self, profile: &ResolvedProfile) -> Result<Capabilities, AppError> {
        if profile.provider != "fake" {
            return Err(AppError::Capability(format!(
                "unknown provider '{}'; this harness only serves 'fake'",
                profile.provider,
            )));
        }
        Ok(Capabilities {
            provider: "fake".to_string(),
            supports_resume: true,
            features: vec!["model:fake-model".to_string()],
        })
    }

    async fn open(&self, request: OpenSession) -> Result<SessionRef, AppError> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        self.workspaces
            .lock()
            .expect("lock")
            .push(request.workspace.clone());
        Ok(SessionRef {
            provider: "fake".to_string(),
            instance_epoch: "epoch-1".to_string(),
            thread_id: "thread-1".to_string(),
        })
    }

    async fn send(&self, request: SendTurn) -> Result<TurnRef, DispatchError> {
        self.sends.fetch_add(1, Ordering::SeqCst);
        self.virtual_delay().await;
        match *self.script.lock().expect("lock") {
            SendScript::Ok => Ok(TurnRef {
                session: request.session,
                turn_id: "turn-1".to_string(),
            }),
            SendScript::Unknown => Err(DispatchError::OutcomeUnknown(
                "lost after send (scripted)".to_string(),
            )),
            SendScript::Rejected => Err(DispatchError::Rejected(
                "harness refused the turn (scripted)".to_string(),
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
        self.cancels.fetch_add(1, Ordering::SeqCst);
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
    events: HashMap<RunId, Vec<central_domain::Event>>,
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

    fn events(&self, run: RunId) -> Vec<central_domain::Event> {
        self.inner
            .lock()
            .expect("lock")
            .events
            .get(&run)
            .cloned()
            .unwrap_or_default()
    }

    fn mutate_state(&self, run: RunId, f: impl FnOnce(&mut RunState)) {
        let mut inner = self.inner.lock().expect("lock");
        let state = inner.states.get_mut(&run).expect("run seeded");
        f(state);
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
        inner.events.entry(run).or_default().extend(batch.events);
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

    async fn outbox(&self, id: RunId) -> Result<Vec<OutboxIntent>, AppError> {
        Ok(self
            .inner
            .lock()
            .expect("lock")
            .intents
            .get(&id)
            .cloned()
            .unwrap_or_default())
    }

    async fn transition_outbox(
        &self,
        id: IntentId,
        expected: OutboxStatus,
        next: OutboxStatus,
    ) -> Result<bool, AppError> {
        let mut inner = self.inner.lock().expect("lock");
        for list in inner.intents.values_mut() {
            for intent in list.iter_mut() {
                if intent.id == id {
                    if intent.status != expected {
                        return Ok(false);
                    }
                    intent.status = next;
                    return Ok(true);
                }
            }
        }
        Err(AppError::NotFound(format!("intent {id} not found")))
    }

    async fn replace_state(
        &self,
        state: &RunState,
        expected_revision: Revision,
    ) -> Result<(), AppError> {
        let mut inner = self.inner.lock().expect("lock");
        let current = inner
            .states
            .get(&state.id)
            .map(|s| s.revision)
            .unwrap_or(Revision(0));
        if current != expected_revision || state.revision != expected_revision {
            return Err(AppError::Conflict("snapshot CAS failed".to_string()));
        }
        inner.states.insert(state.id, state.clone());
        Ok(())
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

fn plan_task(id: TaskId, order: Vec<TaskId>, data: Vec<TaskId>) -> PlanTaskV1 {
    PlanTaskV1 {
        id,
        kind: TaskKind::Implementation,
        objective: format!("implement task {id}"),
        inputs: Vec::new(),
        scope: Vec::new(),
        out_of_scope: Vec::new(),
        depends_on: order,
        data_depends_on: data,
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

fn matching_profile() -> ResolvedProfile {
    ResolvedProfile {
        id: central_domain::ProfileId::from_u128(21),
        provider: "fake".to_string(),
        model: "fake-model".to_string(),
        effort: "low".to_string(),
        skills: Vec::new(),
        subagents: Vec::new(),
        permissions: Vec::new(),
        fallbacks: Vec::new(),
        hash: ContentHash::from_hex("profile-hash"),
    }
}

struct Harness {
    store: Arc<MemoryStore>,
    gateway: Arc<FakeGateway>,
    clock: Arc<FakeClock>,
    ids: Arc<FakeIds>,
    run_id: RunId,
    workspace: Option<Arc<dyn central_application::ports::workspace::WorkspaceManager>>,
}

impl Harness {
    fn new() -> Self {
        let shared = Arc::new(AtomicI64::new(1_700_000_000_000));
        let clock = Arc::new(FakeClock(shared.clone()));
        let gateway = Arc::new(FakeGateway::new(shared));
        let store = Arc::new(MemoryStore::default());
        let ids = Arc::new(FakeIds::new());
        Self {
            store,
            gateway,
            clock,
            ids,
            run_id: RunId::from_u128(10),
            workspace: None,
        }
    }

    fn with_workspace(
        mut self,
        workspace: Arc<dyn central_application::ports::workspace::WorkspaceManager>,
    ) -> Self {
        self.workspace = Some(workspace);
        self
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

    fn scheduler(&self, config: SchedulerConfig) -> Scheduler {
        Scheduler::new(
            self.store.clone() as Arc<dyn Store>,
            self.clock.clone() as Arc<dyn Clock>,
            self.ids.clone() as Arc<dyn IdSource>,
            self.gateway.clone() as Arc<dyn AgentGateway>,
            config,
            Arc::new(|_| Some(matching_profile())),
            system(),
        )
        .expect("valid scheduler config")
        .with_workspace(self.workspace.clone())
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

    /// Plan h1: a and b independent, c needs a's verified output
    /// (order + data dependency on a).
    async fn approved_run(&self) -> (TaskId, TaskId, TaskId) {
        let a = TaskId::from_u128(1);
        let b = TaskId::from_u128(2);
        let c = TaskId::from_u128(3);
        self.execute(
            user(),
            Command::CreateRun {
                goal: "ship the DAG workflow".to_string(),
                repository: "repo".to_string(),
            },
        )
        .await;
        self.execute(
            user(),
            Command::ProposePlan {
                plan: PlanDocumentV1 {
                    version: 1,
                    goal: "ship the DAG workflow".to_string(),
                    tasks: vec![
                        plan_task(a, vec![], vec![]),
                        plan_task(b, vec![], vec![]),
                        plan_task(c, vec![a], vec![a]),
                    ],
                },
            },
        )
        .await;
        let hash = self.load().pending_plan.clone().expect("pending plan");
        self.execute(user(), Command::ApprovePlan { plan_hash: hash })
            .await;
        (a, b, c)
    }

    /// Two independent tasks sharing one named database port.
    async fn exclusive_run(&self) -> (TaskId, TaskId) {
        let m = TaskId::from_u128(4);
        let n = TaskId::from_u128(5);
        self.execute(
            user(),
            Command::CreateRun {
                goal: "guard the shared port".to_string(),
                repository: "repo".to_string(),
            },
        )
        .await;
        self.execute(
            user(),
            Command::ProposePlan {
                plan: PlanDocumentV1 {
                    version: 1,
                    goal: "guard the shared port".to_string(),
                    tasks: vec![plan_task(m, vec![], vec![]), plan_task(n, vec![], vec![])],
                },
            },
        )
        .await;
        let hash = self.load().pending_plan.clone().expect("pending plan");
        self.execute(user(), Command::ApprovePlan { plan_hash: hash })
            .await;
        (m, n)
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

    fn live_attempts(&self) -> usize {
        self.load()
            .attempts
            .values()
            .filter(|attempt| {
                matches!(
                    attempt.status,
                    AttemptStatus::Queued
                        | AttemptStatus::Dispatching
                        | AttemptStatus::Running
                        | AttemptStatus::WaitingForApproval
                        | AttemptStatus::WaitingForInput
                        | AttemptStatus::Reconciling
                )
            })
            .count()
    }

    fn running_count(&self) -> usize {
        self.load()
            .attempts
            .values()
            .filter(|attempt| attempt.status == AttemptStatus::Running)
            .count()
    }

    fn claim_holder(&self, key: &str) -> Option<AttemptId> {
        self.load().resource_claims.get(key).copied()
    }
}

fn default_config() -> SchedulerConfig {
    SchedulerConfig {
        retry_policy: central_domain::RetryPolicy::default(),
        max_parallel: 2,
        resources: BTreeMap::new(),
        poll_interval_ms: 5_000,
        attempt_scratch_root: None,
    }
}

fn port_config(m: TaskId, n: TaskId) -> SchedulerConfig {
    let mut resources = BTreeMap::new();
    resources.insert(m, vec!["port:5432".to_string()]);
    resources.insert(n, vec!["port:5432".to_string()]);
    SchedulerConfig {
        retry_policy: central_domain::RetryPolicy::default(),
        max_parallel: 2,
        resources,
        poll_interval_ms: 5_000,
        attempt_scratch_root: None,
    }
}

// ---------- tests ----------

#[tokio::test]
async fn two_ticks_do_not_duplicate() {
    let harness = Harness::new();
    let (a, b, _c) = harness.approved_run().await;
    let scheduler = harness.scheduler(default_config());

    let first: ScheduleReport = scheduler.tick(harness.run_id).await.expect("tick 1");
    assert_eq!(first.queued, vec![a, b]);
    let second: ScheduleReport = scheduler.tick(harness.run_id).await.expect("tick 2");
    assert!(second.queued.is_empty());

    let a_attempts = harness.attempts_of(a).len();
    assert_eq!(a_attempts, 1);
    assert_eq!(harness.attempts_of(b).len(), 1);
    assert_eq!(harness.live_attempts(), 2);

    let max_parallel = scheduler.config().max_parallel;
    assert_eq!(max_parallel, 2);
}

/// Dispatch with a scratch root provisions one isolated directory per
/// attempt and hands that real path to the gateway — never the repo, never
/// an invented path. Cleanup removes exactly the owned leaf.
#[tokio::test]
async fn dispatch_provisions_isolated_scratch_per_attempt() {
    use central_application::dispatcher::{cleanup_attempt_scratch, provision_attempt_scratch};

    let harness = Harness::new();
    let (a, b, _c) = harness.approved_run().await;
    let root = std::env::temp_dir().join(format!(
        "central-scratch-{}-{}",
        std::process::id(),
        harness.ids.next_attempt_id().uuid().as_u128()
    ));
    let mut config = default_config();
    config.attempt_scratch_root = Some(root.clone());
    let scheduler = harness.scheduler(config);

    let report: ScheduleReport = scheduler.tick(harness.run_id).await.expect("tick");
    assert!(report.sent >= 2, "both attempts dispatch: {report:?}");

    for task in [a, b] {
        let attempts = harness.attempts_of(task);
        assert_eq!(attempts.len(), 1);
        let dir = root
            .join(harness.run_id.to_string())
            .join(attempts[0].id.to_string());
        assert!(dir.is_dir(), "scratch provisioned: {}", dir.display());
    }
    let seen = harness.gateway.workspaces();
    // Canonicalize before comparing: on Windows the recorded paths carry
    // the `\\?\` verbatim prefix while `root` does not. This mirrors the
    // product's own canonical containment comparison.
    let root_canon = std::fs::canonicalize(&root).expect("canonical scratch root");
    let root_canon = root_canon.to_str().expect("scratch root is UTF-8");
    assert!(
        seen.iter().all(|w| w.starts_with(root_canon)),
        "gateway sessions carry real scratch paths: {seen:?}"
    );

    // Direct unit coverage for the refusal paths: nothing can be
    // created under a regular file.
    let sentinel = root.join("sentinel");
    std::fs::write(&sentinel, b"not a dir").expect("sentinel file");
    let first = harness.attempts_of(a)[0].id;
    assert!(provision_attempt_scratch(&sentinel.join("leaf"), harness.run_id, first).is_err());

    // Explicit cleanup removes exactly the owned leaf, then reports false.
    assert!(cleanup_attempt_scratch(&root, harness.run_id, first).expect("cleanup"));
    assert!(!root
        .join(harness.run_id.to_string())
        .join(first.to_string())
        .exists());
    assert!(!cleanup_attempt_scratch(&root, harness.run_id, first).expect("recleanup"));
    std::fs::remove_dir_all(&root).ok();
}

/// An unusable scratch root defers the dispatch: the attempt stays queued,
/// nothing is sent, and no working directory is invented.
#[tokio::test]
async fn unusable_scratch_root_defers_without_sending() {
    let harness = Harness::new();
    let (a, _b, _c) = harness.approved_run().await;
    // A regular file is not a directory root: provisioning must fail.
    let file_root = std::env::temp_dir().join(format!(
        "central-scratch-file-{}-{}",
        std::process::id(),
        harness.ids.next_attempt_id().uuid().as_u128()
    ));
    std::fs::write(&file_root, b"not a dir").expect("sentinel file");
    let mut config = default_config();
    config.attempt_scratch_root = Some(file_root.clone());
    let scheduler = harness.scheduler(config);

    let _report: ScheduleReport = scheduler.tick(harness.run_id).await.expect("tick");
    assert_eq!(harness.gateway.send_count(), 0, "nothing sent");
    let attempts = harness.attempts_of(a);
    assert_eq!(
        attempts.len(),
        1,
        "attempt queued, never claimed past scratch"
    );
    let state = harness.store.load_sync(harness.run_id);
    assert_eq!(
        state.attempts[&attempts[0].id].status,
        AttemptStatus::Queued,
        "unusable root defers, never fails or sends"
    );
    std::fs::remove_file(&file_root).ok();
}

#[tokio::test]
async fn concurrent_ticks_do_not_duplicate() {
    let harness = Harness::new();
    let (a, b, _c) = harness.approved_run().await;
    let scheduler = harness.scheduler(default_config());
    harness.gateway.delay_ms.store(250, Ordering::SeqCst);

    // Hold both dispatches inside the gateway so the two ticks overlap
    // while sending. No sleep is involved: the virtual clock is advanced
    // below and the flag only gates already-started sends.
    harness.gateway.block_until_released();
    let left = scheduler.tick(harness.run_id);
    let right = scheduler.tick(harness.run_id);
    let release = async {
        let mut spins = 0;
        while harness.gateway.send_count() < 2 && spins < 100_000 {
            spins += 1;
            tokio::task::yield_now().await;
        }
        harness.clock.advance(1_000);
        harness.gateway.release();
    };
    let (first, second, _) = tokio::join!(left, right, release);
    first.expect("left tick");
    second.expect("right tick");

    assert_eq!(harness.attempts_of(a).len(), 1);
    assert_eq!(harness.attempts_of(b).len(), 1);
    assert_eq!(harness.gateway.send_count(), 2);
    // Every claim is owned by a live attempt; nothing leaked or doubled.
    let state = harness.load();
    for holder in state.resource_claims.values() {
        assert!(state.attempts.contains_key(holder));
    }
}

#[tokio::test]
async fn dependency_needs_verified_output() {
    let harness = Harness::new();
    let (a, b, c) = harness.approved_run().await;

    // Before a passes, c is blocked on a's verified output while b is ready.
    let eligible = eligible_tasks(&harness.load(), harness.clock.now());
    let c_before_a_pass = c;
    assert!(!eligible.contains(&c_before_a_pass));
    assert!(eligible.contains(&b));

    // An unknown-outcome branch holds its descendants, not its siblings.
    // Queue only a first (one slot), let its send go ambiguous, then read
    // the gate: b is still ready, c is still blocked.
    let mut one_slot = default_config();
    one_slot.max_parallel = 1;
    harness.gateway.set_script(SendScript::Unknown);
    harness
        .scheduler(one_slot)
        .tick(harness.run_id)
        .await
        .expect("tick");
    assert_eq!(harness.attempts_of(a).len(), 1);
    let eligible = eligible_tasks(&harness.load(), harness.clock.now());
    assert!(eligible.contains(&b));
    assert!(!eligible.contains(&c));
    // The independent task still dispatches; the unknown one never resends.
    harness.gateway.set_script(SendScript::Ok);
    harness
        .scheduler(default_config())
        .tick(harness.run_id)
        .await
        .expect("tick b");
    assert_eq!(harness.attempts_of(b).len(), 1);
    assert_eq!(harness.gateway.send_count(), 2);
    let a_status = harness.attempts_of(a)[0].status;
    assert_eq!(a_status, AttemptStatus::UnknownOutcome);

    // Only a verified output (Succeeded + recorded hash) opens c.
    let output = ContentHash::from_hex("aa".repeat(32));
    harness.store.mutate_state(harness.run_id, |state| {
        state.tasks.get_mut(&a).expect("task a").status = central_domain::TaskStatus::Succeeded;
        state.verified_outputs.insert(a, output);
    });
    let eligible = eligible_tasks(&harness.load(), harness.clock.now());
    assert!(eligible.contains(&c));
}

#[tokio::test]
async fn new_input_never_rewrites_a_running_attempt() {
    let harness = Harness::new();
    let (a, _b, _c) = harness.approved_run().await;
    harness
        .scheduler(default_config())
        .tick(harness.run_id)
        .await
        .expect("tick");

    let attempts = harness.attempts_of(a);
    assert_eq!(attempts.len(), 1);
    let stored = harness.load().attempt_inputs.get(&attempts[0].id).cloned();
    assert!(stored.is_some());

    // A second queue for the same task conflicts and leaves the stored
    // input untouched, even with a different payload.
    let envelope = CommandEnvelope {
        id: harness.ids.next_command_id(),
        expected_revision: None,
        command: Command::QueueAttempt {
            attempt: harness.ids.next_attempt_id(),
            task: a,
            input: "a completely different input".to_string(),
            resources: Vec::new(),
            max_parallel: 2,
        },
    };
    let err = harness
        .runtime()
        .execute(system(), envelope)
        .await
        .expect_err("duplicate queue conflicts");
    assert!(matches!(err, AppError::Conflict(_)), "got {err:?}");
    assert_eq!(
        harness.load().attempt_inputs.get(&attempts[0].id).cloned(),
        stored
    );
}

#[tokio::test]
async fn resource_is_exclusive() {
    let harness = Harness::new();
    let (m, n) = harness.exclusive_run().await;
    harness.gateway.set_script(SendScript::Rejected);
    let scheduler = harness.scheduler(port_config(m, n));

    // m wins the named port; n is deferred while the claim is held.
    // m's rejected send then fails the attempt and releases the claim.
    let first = scheduler.tick(harness.run_id).await.expect("tick 1");
    assert_eq!(first.queued, vec![m]);
    assert_eq!(harness.attempts_of(m).len(), 1);
    assert_eq!(harness.attempts_of(n).len(), 0);

    // The freed port lets n queue on the next tick; the two tasks never
    // hold the resource together.
    let second = scheduler.tick(harness.run_id).await.expect("tick 2");
    assert_eq!(second.queued, vec![n]);
    assert_eq!(harness.attempts_of(n).len(), 1);
    assert_eq!(harness.attempts_of(m).len(), 1);
}

#[tokio::test]
async fn unknown_outcome_keeps_resource_claim() {
    let harness = Harness::new();
    let (m, n) = harness.exclusive_run().await;
    harness.gateway.set_script(SendScript::Unknown);
    let scheduler = harness.scheduler(port_config(m, n));

    scheduler.tick(harness.run_id).await.expect("tick 1");
    let unknowns = harness
        .load()
        .attempts
        .values()
        .filter(|attempt| attempt.status == AttemptStatus::UnknownOutcome)
        .count();
    assert_eq!(unknowns, 1);
    assert_eq!(harness.attempts_of(n).len(), 0);

    // The ambiguous send is never resent, and the held claim keeps n
    // deferred even with a healthy gateway.
    harness.gateway.set_script(SendScript::Ok);
    let report = scheduler.tick(harness.run_id).await.expect("tick 2");
    assert_eq!(report.sent, 0);
    assert_eq!(harness.gateway.send_count(), 1);
    assert_eq!(harness.attempts_of(n).len(), 0);
}

#[tokio::test]
async fn pause_keeps_existing_attempt() {
    let harness = Harness::new();
    let (_a, _b, _c) = harness.approved_run().await;
    let scheduler = harness.scheduler(default_config());
    scheduler.tick(harness.run_id).await.expect("tick");
    let running_before = harness.running_count();
    assert_eq!(running_before, 2);

    harness.execute(system(), Command::Pause).await;
    let report = scheduler
        .tick(harness.run_id)
        .await
        .expect("tick while paused");
    assert!(report.queued.is_empty());
    assert_eq!(report.sent, 0);
    let running_after_pause = harness.running_count();
    assert_eq!(running_after_pause, running_before);
}

#[tokio::test]
async fn stop_releases_claims_for_cancelled_attempts() {
    let harness = Harness::new();
    let (a, _b, _c) = harness.approved_run().await;
    let mut config = default_config();
    config.resources.insert(a, vec!["port:5432".to_string()]);
    let scheduler = harness.scheduler(config);
    scheduler.tick(harness.run_id).await.expect("tick");
    assert!(harness.claim_holder("port:5432").is_some());

    harness.execute(system(), Command::Stop).await;
    harness
        .runtime()
        .dispatch_once(harness.run_id)
        .await
        .expect("cancel dispatch");
    assert_eq!(harness.claim_holder("port:5432"), None);
}

#[tokio::test]
async fn capability_mismatch_defers_dispatch() {
    let harness = Harness::new();
    let (_a, _b, _c) = harness.approved_run().await;
    let scheduler = Scheduler::new(
        harness.store.clone() as Arc<dyn Store>,
        harness.clock.clone() as Arc<dyn Clock>,
        harness.ids.clone() as Arc<dyn IdSource>,
        harness.gateway.clone() as Arc<dyn AgentGateway>,
        default_config(),
        Arc::new(|_| {
            Some(ResolvedProfile {
                id: central_domain::ProfileId::from_u128(22),
                provider: "elsewhere".to_string(),
                model: "fake-model".to_string(),
                effort: "low".to_string(),
                skills: Vec::new(),
                subagents: Vec::new(),
                permissions: Vec::new(),
                fallbacks: Vec::new(),
                hash: ContentHash::from_hex("other-profile"),
            })
        }),
        system(),
    )
    .expect("valid scheduler config");
    let report = scheduler.tick(harness.run_id).await.expect("tick");
    assert_eq!(report.queued.len(), 2);
    assert_eq!(report.sent, 0);
    assert_eq!(report.deferred, 2);
    assert_eq!(harness.live_attempts(), 2);
}

#[tokio::test]
async fn dispatch_does_not_send_after_claim_release() {
    // A claim released between queue and dispatch (racing cancel) must
    // not send: dispatch re-verifies ownership right before StartDispatch.
    let harness = Harness::new();
    let (m, n) = harness.exclusive_run().await;
    let attempt = harness.ids.next_attempt_id();
    harness
        .execute(
            system(),
            Command::QueueAttempt {
                attempt,
                task: m,
                input: "implement task m".to_string(),
                resources: vec!["port:5432".to_string()],
                max_parallel: 2,
            },
        )
        .await;
    assert_eq!(harness.claim_holder("port:5432"), Some(attempt));

    // The racing cancel released the claim while the attempt stayed Queued.
    harness.store.mutate_state(harness.run_id, |state| {
        state.resource_claims.remove("port:5432");
    });

    let report = harness
        .scheduler(port_config(m, n))
        .tick(harness.run_id)
        .await
        .expect("tick");
    // n queues and sends normally; m must not send without its claim.
    assert_eq!(report.sent, 1);
    assert_eq!(harness.gateway.send_count(), 1);
    assert_eq!(harness.attempts_of(m).len(), 1);
    assert_eq!(harness.attempts_of(m)[0].status, AttemptStatus::Queued);
    assert!(report.deferred >= 1);
}

#[tokio::test]
async fn paused_tick_reports_held_back_work_as_deferred() {
    // Telemetry honesty: a paused tick must report the work the pause is
    // holding back, not zero.
    let harness = Harness::new();
    let (_a, _b, _c) = harness.approved_run().await;
    harness.execute(system(), Command::Pause).await;
    let report = harness
        .scheduler(default_config())
        .tick(harness.run_id)
        .await
        .expect("tick while paused");
    assert!(report.queued.is_empty());
    assert_eq!(report.sent, 0);
    assert_eq!(report.deferred, 2);
}

#[tokio::test]
async fn shuffled_completion_keeps_invariants() {
    // Verification may arrive in any order; c opens exactly when a's
    // verified output lands, no matter what b does first.
    for order in [[true, false], [false, true]] {
        let harness = Harness::new();
        let (a, b, c) = harness.approved_run().await;
        harness
            .scheduler(default_config())
            .tick(harness.run_id)
            .await
            .expect("tick");
        let output = ContentHash::from_hex("bb".repeat(32));
        let mut a_verified = false;
        let finish = |harness: &Harness, task: TaskId, verified: Option<ContentHash>| {
            harness.store.mutate_state(harness.run_id, |state| {
                let attempt_id = state
                    .attempts
                    .values()
                    .find(|attempt| attempt.task_id == task)
                    .expect("attempt queued")
                    .id;
                state.attempts.get_mut(&attempt_id).expect("attempt").status =
                    AttemptStatus::Completed;
                state.tasks.get_mut(&task).expect("task").status =
                    central_domain::TaskStatus::Succeeded;
                if let Some(hash) = verified {
                    state.verified_outputs.insert(task, hash);
                }
            });
        };
        for b_first in order {
            if b_first {
                finish(&harness, b, None);
            } else {
                finish(&harness, a, Some(output.clone()));
                a_verified = true;
            }
            // Until a's verified output lands, c stays closed whichever
            // order the completions arrive in.
            let eligible = eligible_tasks(&harness.load(), harness.clock.now());
            assert_eq!(eligible.contains(&c), a_verified, "order {order:?}");
        }
        let eligible = eligible_tasks(&harness.load(), harness.clock.now());
        assert!(
            eligible.contains(&c),
            "c opens once a is verified: {order:?}"
        );
        // Queuing c now still yields exactly one attempt over repeat ticks.
        let scheduler = harness.scheduler(default_config());
        scheduler.tick(harness.run_id).await.expect("tick c");
        scheduler.tick(harness.run_id).await.expect("tick c again");
        assert_eq!(harness.attempts_of(c).len(), 1);
    }
}

// ---------- mechanical integration (P3 Task 5 scheduler half) ----------

use central_application::ports::workspace::{
    AutoIntegrationReport, AutoIntegrationRequest, CandidateRef, InputSnapshot,
    IntegrationCandidate, IntegrationRequest, PromoteRequest, PromotionReceipt, SnapshotRequest,
    VerificationSummary, WorkspaceManager, WorkspaceRef, WorkspaceRequest,
};
use central_domain::{IntegrationCheck, IntegrationSpec};

struct FakeWorkspace {
    auto_calls: AtomicUsize,
}

impl FakeWorkspace {
    fn new() -> Self {
        Self {
            auto_calls: AtomicUsize::new(0),
        }
    }

    fn unsupported<T>(what: &str) -> Result<T, AppError> {
        Err(AppError::Transport(format!(
            "fake workspace does not implement {what}"
        )))
    }
}

#[async_trait]
impl WorkspaceManager for FakeWorkspace {
    async fn snapshot(&self, request: SnapshotRequest) -> Result<InputSnapshot, AppError> {
        Ok(InputSnapshot {
            repo_root: request.repo_root,
            commit: request.commit,
            tree: "tree0".to_string(),
            selected: Vec::new(),
            snapshot_hash: central_domain::ContentHash::from_hex("aa"),
        })
    }
    async fn create(&self, request: WorkspaceRequest) -> Result<WorkspaceRef, AppError> {
        Ok(WorkspaceRef {
            attempt_id: request.attempt_id,
            worktree: format!("/tmp/central-fake-worktree-{}", request.attempt_id),
            base_commit: request.input.commit,
        })
    }
    async fn seal(&self, _workspace: &WorkspaceRef) -> Result<CandidateRef, AppError> {
        Self::unsupported("seal")
    }
    async fn verify_seal(&self, _candidate: &CandidateRef) -> Result<(), AppError> {
        Ok(())
    }
    async fn prepare_integration(
        &self,
        _request: IntegrationRequest,
    ) -> Result<IntegrationCandidate, AppError> {
        Self::unsupported("prepare_integration")
    }
    async fn promote(&self, _request: PromoteRequest) -> Result<PromotionReceipt, AppError> {
        Self::unsupported("promote")
    }
    async fn recover_promotion(
        &self,
        _run: RunId,
        _ref_name: &str,
    ) -> Result<PromotionReceipt, AppError> {
        Self::unsupported("recover_promotion")
    }
    async fn latest_synced_commit(
        &self,
        _run: RunId,
        _repo: &str,
    ) -> Result<Option<String>, AppError> {
        Ok(Some("base0".to_string()))
    }
    async fn remove_workspace(&self, _workspace: &WorkspaceRef) -> Result<(), AppError> {
        Ok(())
    }
    async fn seal_live(&self, attempt: AttemptId) -> Result<CandidateRef, AppError> {
        Ok(CandidateRef {
            attempt_id: attempt,
            commit: "c0ffee".to_string(),
            tree: "tree0".to_string(),
            parent: "base0".to_string(),
        })
    }
    async fn auto_integrate(
        &self,
        request: AutoIntegrationRequest,
    ) -> Result<AutoIntegrationReport, AppError> {
        self.auto_calls.fetch_add(1, Ordering::SeqCst);
        Ok(AutoIntegrationReport {
            candidate: request
                .candidates
                .first()
                .map(|c| c.commit.clone())
                .unwrap_or_default(),
            worktree: "/tmp/central-fake-worktree".to_string(),
            evidence: Vec::new(),
            summary: VerificationSummary {
                passed: true,
                baseline_ok: true,
                affected_ok: true,
                acceptance_ok: true,
                evidence: Vec::new(),
            },
            receipt: PromotionReceipt {
                ref_name: request.ref_name,
                previous_head: "base0".to_string(),
                new_head: "deadbeef".to_string(),
                candidate: "c0ffee".to_string(),
                intent_id: "intent-1".to_string(),
            },
        })
    }
}

fn integration_task(worker: TaskId, gate: TaskId, with_spec: bool) -> PlanTaskV1 {
    PlanTaskV1 {
        id: gate,
        kind: TaskKind::Integration,
        objective: "integrate the verified output".to_string(),
        inputs: Vec::new(),
        scope: Vec::new(),
        out_of_scope: Vec::new(),
        depends_on: vec![worker],
        data_depends_on: vec![worker],
        outputs: Vec::new(),
        // A verify block alone never authorizes integration: only the
        // machine-readable spec below does.
        verify: Some(verify_spec()),
        evidence: Vec::new(),
        profile_id: None,
        permissions: Vec::new(),
        max_repair_rounds: 0,
        requires_approval: Vec::new(),
        handover: String::new(),
        integration: with_spec.then(|| IntegrationSpec {
            good_ref: "refs/heads/central-good".to_string(),
            base_ref: None,
            baseline: vec![IntegrationCheck {
                program: "sh".to_string(),
                args: vec!["-c".to_string(), "exit 0".to_string()],
            }],
            affected: Vec::new(),
            acceptance: Vec::new(),
        }),
    }
}

fn integration_plan(worker: TaskId, gate: TaskId, with_spec: bool) -> PlanDocumentV1 {
    PlanDocumentV1 {
        version: 1,
        goal: "integrate the verified worker output".to_string(),
        tasks: vec![
            plan_task(worker, vec![], vec![]),
            integration_task(worker, gate, with_spec),
        ],
    }
}

async fn approved_run_with_repo(harness: &Harness, repo: &str, plan: PlanDocumentV1) {
    harness
        .execute(
            user(),
            Command::CreateRun {
                goal: "integrate".to_string(),
                repository: repo.to_string(),
            },
        )
        .await;
    harness.execute(user(), Command::ProposePlan { plan }).await;
    let hash = harness.load().pending_plan.clone().expect("pending plan");
    harness
        .execute(user(), Command::ApprovePlan { plan_hash: hash })
        .await;
}

/// Scheduler-auto integration: once the worker is verified, the
/// integration task dispatches mechanically — no harness send, no model
/// turn — and the daemon records the CAS promotion as its Succeeded
/// verdict with the new head filed as the verified output.
#[tokio::test]
async fn scheduler_auto_integrates_verified_work() {
    let root =
        std::env::temp_dir().join(format!("central-auto-integration-{}", std::process::id()));
    std::fs::create_dir_all(&root).expect("scratch repo dir");
    let fake = Arc::new(FakeWorkspace::new());
    let harness = Harness::new().with_workspace(fake.clone());
    let worker = TaskId::from_u128(71);
    let gate = TaskId::from_u128(72);
    approved_run_with_repo(
        &harness,
        &root.to_string_lossy(),
        integration_plan(worker, gate, true),
    )
    .await;

    // Worker leg through the fake gateway, then verify it.
    let scheduler = harness.scheduler(default_config());
    scheduler.tick(harness.run_id).await.expect("tick worker");
    assert_eq!(harness.gateway.send_count(), 1);
    let work = harness.attempts_of(worker);
    assert_eq!(work.len(), 1);
    let input_hash = ContentHash::from_hex("aa");
    let candidate = ContentHash::from_hex("bb");
    let criteria = central_domain::quality::criteria_hash_for(&verify_spec());
    harness
        .execute(
            user(),
            Command::RecordWorkerResult {
                attempt: work[0].id,
                input_hash: input_hash.clone(),
                candidate_hash: candidate.clone(),
                criteria_hash: criteria.clone(),
            },
        )
        .await;
    harness
        .execute(
            user(),
            Command::SubmitVerification {
                task: worker,
                attempt: work[0].id,
                report: central_domain::quality::VerificationReport {
                    subject_hash: candidate,
                    criteria_hash: criteria,
                    outcome: central_domain::VerificationOutcome::Pass,
                    evidence_ids: vec![input_hash.to_string()],
                    failed_criteria: Vec::new(),
                    environment: "test".to_string(),
                    check_refs: Vec::new(),
                    verifier_thread: "other-thread".to_string(),
                    verifier_wrote_tracked_source: false,
                    same_session_override: false,
                    findings: "fake verifier passes".to_string(),
                },
            },
        )
        .await;

    // The integration task now dispatches without any harness send.
    scheduler
        .tick(harness.run_id)
        .await
        .expect("tick integration");
    assert_eq!(
        harness.gateway.send_count(),
        1,
        "no model turn for integration"
    );
    let state = harness.load();
    assert_eq!(
        state.tasks[&gate].status,
        central_domain::TaskStatus::Succeeded
    );
    assert_eq!(
        state.verified_outputs[&gate].as_str(),
        "deadbeef",
        "new head filed as the verified output"
    );
    assert!(harness
        .store
        .events(harness.run_id)
        .iter()
        .any(|e| matches!(
            e,
            central_domain::Event::IntegrationPromoted { new_head, .. } if new_head == "deadbeef"
        )));
    assert_eq!(fake.auto_calls.load(Ordering::SeqCst), 1);
    let _ = std::fs::remove_dir_all(&root);
}

/// An integration task without approved checks never dispatches: it
/// waits for a revision instead of running unapproved commands.
#[tokio::test]
async fn integration_without_spec_never_dispatches() {
    let root = std::env::temp_dir().join(format!(
        "central-auto-integration-nospec-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root).expect("scratch repo dir");
    let fake = Arc::new(FakeWorkspace::new());
    let harness = Harness::new().with_workspace(fake);
    let worker = TaskId::from_u128(73);
    let gate = TaskId::from_u128(74);
    approved_run_with_repo(
        &harness,
        &root.to_string_lossy(),
        integration_plan(worker, gate, false),
    )
    .await;

    let scheduler = harness.scheduler(default_config());
    let report = scheduler.tick(harness.run_id).await.expect("tick");
    assert_eq!(report.sent, 1, "only the worker dispatches");
    assert!(harness.attempts_of(gate).is_empty());
    let _ = std::fs::remove_dir_all(&root);
}

/// The scan tick flushes pending outbox intents on the same pass: an
/// operator answer or a stop-requested cancel must reach the harness
/// without a manual dispatch call. Before this, only an explicit
/// POST /v1/runs/{id}/dispatch (or a command-attached dispatch) sent
/// them, so an approval sat Answered until someone kicked dispatch.
#[tokio::test]
async fn scheduler_tick_sends_pending_cancel_without_manual_dispatch() {
    use central_domain::AttemptSession;

    let harness = Harness::new();
    harness
        .execute(
            user(),
            Command::CreateRun {
                goal: "cancel live".to_string(),
                repository: "repo".to_string(),
            },
        )
        .await;
    let attempt = AttemptId::from_u128(4242);
    harness.store.mutate_state(harness.run_id, |state| {
        state.attempts.insert(
            attempt,
            Attempt {
                id: attempt,
                task_id: TaskId::from_u128(4243),
                plan_hash: central_domain::PlanHash::from_hex("h1"),
                status: AttemptStatus::Running,
                profile_id: None,
                created_at: Timestamp(1_700_000_000_000),
            },
        );
        state.attempt_sessions.insert(
            attempt,
            AttemptSession {
                provider: "fake".to_string(),
                instance_epoch: "epoch-1".to_string(),
                thread_id: "thread-1".to_string(),
            },
        );
    });
    harness.execute(user(), Command::Stop).await;
    assert_eq!(harness.gateway.cancel_count(), 0);

    let scheduler = harness.scheduler(default_config());
    let report = scheduler.tick(harness.run_id).await.expect("tick");
    assert_eq!(report.sent, 1, "cancel sent on the scan pass: {report:?}");
    assert_eq!(harness.gateway.cancel_count(), 1);
    let state = harness.load();
    assert_eq!(
        state.attempts[&attempt].status,
        AttemptStatus::Cancelled,
        "cancel confirmed by the harness"
    );
}
