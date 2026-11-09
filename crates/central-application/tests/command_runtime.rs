use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use central_application::contracts::{
    AppError, CancelObservation, Capabilities, CommitBatch, DispatchError, ExecutionRef,
    ExternalAnswer, OpenSession, OutboxIntent, ReconcileEvidence, SendTurn, SessionRef, TurnRef,
};
use central_application::ports::{AgentGateway, Clock, IdSource, Store};
use central_application::Runtime;
use central_domain::{
    Actor, ActorId, ActorKind, Attempt, AttemptId, AttemptStatus, Command, CommandEnvelope,
    CommandId, ContentHash, Event, IntentId, Observation, Revision, RunId, RunState, Timestamp,
};

// ---------- deterministic fakes ----------

struct FakeClock(AtomicI64);

impl Clock for FakeClock {
    fn now(&self) -> Timestamp {
        Timestamp(self.0.load(Ordering::SeqCst))
    }
}

struct FakeIds(AtomicU64);

impl IdSource for FakeIds {
    fn next_run_id(&self) -> RunId {
        RunId::from_u128(5_000 + self.0.fetch_add(1, Ordering::SeqCst) as u128)
    }
    fn next_intent_id(&self) -> IntentId {
        IntentId::from_u128(6_000 + self.0.fetch_add(1, Ordering::SeqCst) as u128)
    }
    fn next_command_id(&self) -> CommandId {
        CommandId::from_u128(7_000 + self.0.fetch_add(1, Ordering::SeqCst) as u128)
    }
}

#[derive(Default)]
struct FakeGateway {
    sends: AtomicUsize,
}

impl FakeGateway {
    fn send_count(&self) -> usize {
        self.sends.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl AgentGateway for FakeGateway {
    async fn capabilities(
        &self,
        _profile: &central_domain::ResolvedProfile,
    ) -> Result<Capabilities, AppError> {
        Ok(Capabilities {
            provider: "fake".to_string(),
            supports_resume: true,
            features: Vec::new(),
        })
    }
    async fn open(&self, _request: OpenSession) -> Result<SessionRef, AppError> {
        Ok(SessionRef {
            provider: "fake".to_string(),
            instance_epoch: "epoch-1".to_string(),
            thread_id: "thread-1".to_string(),
        })
    }
    async fn send(&self, request: SendTurn) -> Result<TurnRef, DispatchError> {
        self.sends.fetch_add(1, Ordering::SeqCst);
        Ok(TurnRef {
            session: request.session,
            turn_id: "turn-1".to_string(),
        })
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

#[derive(Default)]
struct Inner {
    states: HashMap<RunId, RunState>,
    receipts: HashMap<(ActorId, CommandId), (ContentHash, central_application::contracts::Receipt)>,
    events: HashMap<RunId, Vec<Event>>,
    intents: HashMap<RunId, Vec<OutboxIntent>>,
    fail_commit: bool,
}

#[derive(Default)]
struct MemoryStore {
    inner: Mutex<Inner>,
}

impl MemoryStore {
    fn set_fail_commit(&self, fail: bool) {
        self.inner.lock().expect("lock").fail_commit = fail;
    }
    fn seed(&self, state: RunState) {
        self.inner
            .lock()
            .expect("lock")
            .states
            .insert(state.id, state);
    }
    fn events(&self, run: RunId) -> Vec<Event> {
        self.inner
            .lock()
            .expect("lock")
            .events
            .get(&run)
            .cloned()
            .unwrap_or_default()
    }
}

#[async_trait]
impl Store for MemoryStore {
    async fn load(&self, id: RunId) -> Result<Option<RunState>, AppError> {
        Ok(self.inner.lock().expect("lock").states.get(&id).cloned())
    }
    async fn receipt(
        &self,
        actor: ActorId,
        id: CommandId,
    ) -> Result<Option<central_application::contracts::Receipt>, AppError> {
        Ok(self
            .inner
            .lock()
            .expect("lock")
            .receipts
            .get(&(actor, id))
            .map(|(_, receipt)| receipt.clone()))
    }
    async fn commit(
        &self,
        batch: CommitBatch,
    ) -> Result<central_application::contracts::Receipt, AppError> {
        let mut inner = self.inner.lock().expect("lock");
        if let Some((hash, receipt)) = inner.receipts.get(&(batch.actor, batch.command_id)) {
            if *hash != batch.payload_hash {
                return Err(AppError::Conflict(
                    "command id reused with a different payload".to_string(),
                ));
            }
            return Ok(receipt.clone());
        }
        if inner.fail_commit {
            return Err(AppError::Storage("injected commit failure".to_string()));
        }
        let current = inner
            .states
            .get(&batch.next_state.id)
            .map(|state| state.revision)
            .unwrap_or(Revision(0));
        if current != batch.expected_revision {
            return Err(AppError::Conflict("revision conflict".to_string()));
        }
        let run = batch.next_state.id;
        inner.states.insert(run, batch.next_state);
        inner.events.entry(run).or_default().extend(batch.events);
        inner.intents.entry(run).or_default().extend(batch.intents);
        inner.receipts.insert(
            (batch.actor, batch.command_id),
            (batch.payload_hash, batch.receipt.clone()),
        );
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
            .unwrap_or_default())
    }
}

// ---------- helpers ----------

fn user() -> Actor {
    Actor {
        id: ActorId::from_u128(100),
        kind: ActorKind::User,
    }
}

fn run_id() -> RunId {
    RunId::from_u128(10)
}

fn runtime(store: Arc<MemoryStore>, gateway: Arc<FakeGateway>) -> Runtime {
    Runtime::new(
        store as Arc<dyn Store>,
        Arc::new(FakeClock(AtomicI64::new(1_700_000_000_000))),
        Arc::new(FakeIds(AtomicU64::new(0))),
        gateway as Arc<dyn AgentGateway>,
        run_id(),
    )
}

fn create_envelope(id: u128, goal: &str) -> CommandEnvelope {
    CommandEnvelope {
        id: CommandId::from_u128(id),
        expected_revision: None,
        command: Command::CreateRun {
            goal: goal.to_string(),
            repository: "D:/Code/enolalab/central".to_string(),
        },
    }
}

#[tokio::test]
async fn atomic_commit_and_receipt() {
    let store = Arc::new(MemoryStore::default());
    let gateway = Arc::new(FakeGateway::default());
    let runtime = runtime(store.clone(), gateway.clone());

    let first_receipt = runtime
        .execute(user(), create_envelope(900, "Build the durable runtime"))
        .await
        .expect("execute create");

    let state = store.load(run_id()).await.expect("load").expect("run");
    assert_eq!(state.revision, Revision(1));
    assert_eq!(store.events(run_id()).len(), 1);
    assert_eq!(first_receipt.revision, Revision(1));
    assert_eq!(first_receipt.run_id, run_id());

    // Receipt replay happens before decide: re-deciding CreateRun on the
    // created run would fail, so a duplicate must return the first receipt.
    let duplicate_receipt = runtime
        .execute(user(), create_envelope(900, "Build the durable runtime"))
        .await
        .expect("duplicate execute replays receipt");
    assert_eq!(duplicate_receipt, first_receipt);
    assert_eq!(store.events(run_id()).len(), 1);
    let state = store.load(run_id()).await.expect("load").expect("run");
    assert_eq!(state.revision, Revision(1));
}

#[tokio::test]
async fn same_id_different_payload_conflicts() {
    let store = Arc::new(MemoryStore::default());
    let gateway = Arc::new(FakeGateway::default());
    let runtime = runtime(store.clone(), gateway);

    runtime
        .execute(user(), create_envelope(900, "first goal"))
        .await
        .expect("execute create");
    let err = runtime
        .execute(user(), create_envelope(900, "different goal"))
        .await
        .expect_err("same id, different payload");
    assert!(matches!(err, AppError::Conflict(_)), "got {err:?}");
    let state = store.load(run_id()).await.expect("load").expect("run");
    assert_eq!(state.goal, "first goal");
    assert_eq!(state.revision, Revision(1));
}

#[tokio::test]
async fn expected_revision_conflicts() {
    let store = Arc::new(MemoryStore::default());
    let gateway = Arc::new(FakeGateway::default());
    let runtime = runtime(store.clone(), gateway);

    let mut stale_create = create_envelope(900, "goal");
    stale_create.expected_revision = Some(Revision(5));
    let err = runtime
        .execute(user(), stale_create)
        .await
        .expect_err("stale create");
    assert!(matches!(err, AppError::Conflict(_)), "got {err:?}");
    assert_eq!(store.load(run_id()).await.expect("load"), None);

    runtime
        .execute(user(), create_envelope(900, "goal"))
        .await
        .expect("execute create");

    let err = runtime
        .execute(
            user(),
            CommandEnvelope {
                id: CommandId::from_u128(901),
                expected_revision: Some(Revision(0)),
                command: Command::Pause,
            },
        )
        .await
        .expect_err("stale pause");
    assert!(matches!(err, AppError::Conflict(_)), "got {err:?}");
    let state = store.load(run_id()).await.expect("load").expect("run");
    assert_eq!(state.revision, Revision(1));
    assert_eq!(state.control, central_domain::RunControl::Running);
}

#[tokio::test]
async fn failed_commit_never_dispatches() {
    let store = Arc::new(MemoryStore::default());
    let gateway = Arc::new(FakeGateway::default());
    let runtime = runtime(store.clone(), gateway.clone());

    runtime
        .execute(user(), create_envelope(900, "goal"))
        .await
        .expect("execute create");

    // Seed a running attempt so Stop would produce a CancelAttempt effect.
    let mut state = store.load(run_id()).await.expect("load").expect("run");
    let attempt_id = AttemptId::from_u128(42);
    state.attempts.insert(
        attempt_id,
        Attempt {
            id: attempt_id,
            task_id: central_domain::TaskId::from_u128(1),
            plan_hash: central_domain::PlanHash::from_hex("h1"),
            status: AttemptStatus::Running,
            profile_id: None,
            created_at: Timestamp(1_700_000_000_000),
        },
    );
    store.seed(state);

    store.set_fail_commit(true);
    let err = runtime
        .execute(
            user(),
            CommandEnvelope {
                id: CommandId::from_u128(901),
                expected_revision: None,
                command: Command::Stop,
            },
        )
        .await
        .expect_err("failed commit");
    assert!(matches!(err, AppError::Storage(_)), "got {err:?}");
    // No dispatch, no intent, no state change and no receipt on failure.
    assert_eq!(gateway.send_count(), 0);
    assert_eq!(store.pending(run_id()).await.expect("pending").len(), 0);
    assert_eq!(
        store
            .receipt(user().id, CommandId::from_u128(901))
            .await
            .expect("receipt"),
        None
    );
    let state = store.load(run_id()).await.expect("load").expect("run");
    assert_eq!(state.control, central_domain::RunControl::Running);
    assert_eq!(store.events(run_id()).len(), 1);

    // After the failure clears, the same command commits its intent
    // atomically, and execute still does not dispatch it inline.
    store.set_fail_commit(false);
    runtime
        .execute(
            user(),
            CommandEnvelope {
                id: CommandId::from_u128(901),
                expected_revision: None,
                command: Command::Stop,
            },
        )
        .await
        .expect("stop after failure cleared");
    assert_eq!(gateway.send_count(), 0);
    let intents = store.pending(run_id()).await.expect("pending");
    assert_eq!(intents.len(), 1);
    assert_eq!(
        intents[0].status,
        central_application::contracts::OutboxStatus::Pending
    );
}

#[tokio::test]
async fn forbidden_actor_is_rejected_without_commit() {
    let store = Arc::new(MemoryStore::default());
    let gateway = Arc::new(FakeGateway::default());
    let runtime = runtime(store.clone(), gateway);
    let worker = Actor {
        id: ActorId::from_u128(102),
        kind: ActorKind::Worker,
    };
    let err = runtime
        .execute(worker, create_envelope(900, "goal"))
        .await
        .expect_err("worker cannot create a run");
    assert!(matches!(err, AppError::Forbidden(_)), "got {err:?}");
    assert_eq!(store.load(run_id()).await.expect("load"), None);
    assert_eq!(store.events(run_id()).len(), 0);
}
