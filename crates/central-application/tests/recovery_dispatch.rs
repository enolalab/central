//! Recovery and dispatch barrier tests for `Runtime::recover` /
//! `Runtime::dispatch_once` (P1 Task 4). Dispatch must use guarded
//! outbox transitions: evidence completes an intent, an ambiguous send
//! becomes `UnknownOutcome` and is never resent blindly, and nothing is
//! sent while a run waits for explicit resume.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use central_application::contracts::{
    AppError, CancelObservation, Capabilities, CommitBatch, DispatchError, ExecutionRef,
    ExternalAnswer, OpenSession, OutboxIntent, OutboxStatus, Receipt, ReconcileEvidence, SendTurn,
    SessionRef, TurnRef,
};
use central_application::ports::{AgentGateway, Clock, IdSource, Store};
use central_application::Runtime;
use central_domain::{
    decide, Actor, ActorId, ActorKind, Attempt, AttemptId, AttemptStatus, Command, CommandEnvelope,
    CommandId, IntentId, Observation, Revision, RunControl, RunId, RunState, TaskId, Timestamp,
};

// ---------- fakes ----------

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
    cancels: AtomicUsize,
    ambiguous: AtomicBool,
}

impl FakeGateway {
    fn send_count(&self) -> usize {
        self.sends.load(Ordering::SeqCst)
    }
    fn cancel_count(&self) -> usize {
        self.cancels.load(Ordering::SeqCst)
    }
    fn set_ambiguous(&self, value: bool) {
        self.ambiguous.store(value, Ordering::SeqCst);
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
        if self.ambiguous.load(Ordering::SeqCst) {
            return Err(DispatchError::OutcomeUnknown("lost after send".to_string()));
        }
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
        self.cancels.fetch_add(1, Ordering::SeqCst);
        if self.ambiguous.load(Ordering::SeqCst) {
            return Err(AppError::Transport("lost after cancel".to_string()));
        }
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
    fn seed_state(&self, state: RunState) {
        self.inner
            .lock()
            .expect("lock")
            .states
            .insert(state.id, state);
    }
    fn seed_intent(&self, intent: OutboxIntent) {
        self.inner
            .lock()
            .expect("lock")
            .intents
            .entry(intent.run_id)
            .or_default()
            .push(intent);
    }
    fn intent_status(&self, run: RunId, intent: IntentId) -> OutboxStatus {
        self.inner
            .lock()
            .expect("lock")
            .intents
            .get(&run)
            .and_then(|list| list.iter().find(|i| i.id == intent))
            .expect("intent")
            .status
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

fn created_state() -> RunState {
    let state = RunState::new(run_id());
    decide(
        &state,
        &user(),
        &Command::CreateRun {
            goal: "goal".to_string(),
            repository: "repo".to_string(),
        },
        Timestamp(1_700_000_000_000),
    )
    .expect("create")
    .next
}

fn attempt(id: u128, status: AttemptStatus) -> Attempt {
    Attempt {
        id: AttemptId::from_u128(id),
        task_id: TaskId::from_u128(1),
        plan_hash: central_domain::PlanHash::from_hex("h1"),
        status,
        profile_id: None,
        created_at: Timestamp(1_700_000_000_000),
    }
}

async fn stopped_run_with_intent(
    store: &Arc<MemoryStore>,
    runtime: &Runtime,
    attempt_ids: &[u128],
) -> Vec<IntentId> {
    let mut state = created_state();
    for id in attempt_ids {
        state.attempts.insert(
            AttemptId::from_u128(*id),
            attempt(*id, AttemptStatus::Running),
        );
    }
    store.seed_state(state);
    runtime
        .execute(
            user(),
            CommandEnvelope {
                id: CommandId::from_u128(900),
                expected_revision: None,
                command: Command::Stop,
            },
        )
        .await
        .expect("stop");
    store
        .pending(run_id())
        .await
        .expect("pending")
        .iter()
        .map(|i| i.id)
        .collect()
}

// ---------- tests ----------

#[tokio::test]
async fn dispatch_with_evidence_completes_intent() {
    let store = Arc::new(MemoryStore::default());
    let gateway = Arc::new(FakeGateway::default());
    let runtime = runtime(store.clone(), gateway.clone());
    let intents = stopped_run_with_intent(&store, &runtime, &[42]).await;
    assert_eq!(intents.len(), 1);

    let summary = runtime.dispatch_once(run_id()).await.expect("dispatch");
    assert_eq!(summary.dispatched, 1);
    assert_eq!(summary.unknown, 0);
    assert_eq!(gateway.cancel_count(), 1);
    assert_eq!(
        store.intent_status(run_id(), intents[0]),
        OutboxStatus::Completed
    );
    let state = store.load(run_id()).await.expect("load").expect("run");
    assert_eq!(
        state.attempts[&AttemptId::from_u128(42)].status,
        AttemptStatus::Cancelled
    );
    // Completed intents are never redispatched.
    let summary = runtime.dispatch_once(run_id()).await.expect("redispatch");
    assert_eq!(summary.dispatched, 0);
    assert_eq!(gateway.cancel_count(), 1);
}

#[tokio::test]
async fn ambiguous_dispatch_becomes_unknown_and_is_not_resent() {
    let store = Arc::new(MemoryStore::default());
    let gateway = Arc::new(FakeGateway::default());
    gateway.set_ambiguous(true);
    let runtime = runtime(store.clone(), gateway.clone());
    let intents = stopped_run_with_intent(&store, &runtime, &[42]).await;

    let summary = runtime.dispatch_once(run_id()).await.expect("dispatch");
    assert_eq!(summary.unknown, 1);
    assert_eq!(summary.dispatched, 0);
    assert_eq!(
        store.intent_status(run_id(), intents[0]),
        OutboxStatus::UnknownOutcome
    );
    let state = store.load(run_id()).await.expect("load").expect("run");
    assert_eq!(
        state.attempts[&AttemptId::from_u128(42)].status,
        AttemptStatus::UnknownOutcome
    );

    gateway.set_ambiguous(false);
    let summary = runtime.dispatch_once(run_id()).await.expect("redispatch");
    assert_eq!(summary.dispatched, 0);
    assert_eq!(gateway.cancel_count(), 1, "unknown outcome must not resend");
}

#[tokio::test]
async fn dispatch_is_order_insensitive_across_pending_intents() {
    let store = Arc::new(MemoryStore::default());
    let gateway = Arc::new(FakeGateway::default());
    let runtime = runtime(store.clone(), gateway.clone());
    // Intent ids are allocated in attempt order; the store returns
    // pending intents ordered by UUID, which is unrelated to creation
    // order. Every intent must still complete exactly once.
    let intents = stopped_run_with_intent(&store, &runtime, &[43, 41, 42]).await;
    assert_eq!(intents.len(), 3);
    let summary = runtime.dispatch_once(run_id()).await.expect("dispatch");
    assert_eq!(summary.dispatched, 3);
    assert_eq!(gateway.cancel_count(), 3);
    for intent in intents {
        assert_eq!(
            store.intent_status(run_id(), intent),
            OutboxStatus::Completed
        );
    }
}

#[tokio::test]
async fn recovery_marks_dispatching_unknown_and_requires_resume() {
    let store = Arc::new(MemoryStore::default());
    let gateway = Arc::new(FakeGateway::default());
    let runtime = runtime(store.clone(), gateway.clone());

    let mut state = created_state();
    state.attempts.insert(
        AttemptId::from_u128(42),
        attempt(42, AttemptStatus::Dispatching),
    );
    store.seed_state(state);
    let intent_id = IntentId::from_u128(700);
    store.seed_intent(OutboxIntent {
        id: intent_id,
        run_id: run_id(),
        effect: central_domain::Effect::CancelAttempt {
            attempt_id: AttemptId::from_u128(42),
        },
        status: OutboxStatus::Dispatching,
    });
    // A second, still-pending intent must not be sent before resume.
    let pending_id = IntentId::from_u128(701);
    store.seed_intent(OutboxIntent {
        id: pending_id,
        run_id: run_id(),
        effect: central_domain::Effect::CancelAttempt {
            attempt_id: AttemptId::from_u128(42),
        },
        status: OutboxStatus::Pending,
    });

    let summary = runtime.recover().await.expect("recover");
    assert_eq!(summary.runs_seen, 1);
    assert_eq!(summary.awaiting_resume, 1);
    assert_eq!(summary.unknown_outcome, 1);

    let restored = store.load(run_id()).await.expect("load").expect("run");
    assert_eq!(restored.control, RunControl::AwaitingResume);
    assert_eq!(
        restored.attempts[&AttemptId::from_u128(42)].status,
        AttemptStatus::UnknownOutcome
    );
    assert_eq!(
        store.intent_status(run_id(), intent_id),
        OutboxStatus::UnknownOutcome
    );

    // Zero external sends before explicit resume.
    let summary = runtime
        .dispatch_once(run_id())
        .await
        .expect("dispatch gated");
    assert_eq!(summary.dispatched, 0);
    assert_eq!(gateway.send_count(), 0);
    assert_eq!(gateway.cancel_count(), 0);
    assert_eq!(
        store.intent_status(run_id(), pending_id),
        OutboxStatus::Pending
    );

    // Explicit resume reopens the gate and the pending intent dispatches.
    runtime
        .execute(
            user(),
            CommandEnvelope {
                id: CommandId::from_u128(900),
                expected_revision: None,
                command: Command::Resume,
            },
        )
        .await
        .expect("resume");
    let summary = runtime
        .dispatch_once(run_id())
        .await
        .expect("dispatch after resume");
    assert_eq!(summary.dispatched, 1);
    assert_eq!(
        store.intent_status(run_id(), pending_id),
        OutboxStatus::Completed
    );
    // The unknown-outcome intent was never resent.
    assert_eq!(gateway.cancel_count(), 1);
}

#[tokio::test]
async fn recovery_leaves_stopped_runs_stopped() {
    let store = Arc::new(MemoryStore::default());
    let gateway = Arc::new(FakeGateway::default());
    let runtime = runtime(store.clone(), gateway.clone());
    let mut state = created_state();
    state.control = RunControl::Stopped;
    store.seed_state(state);
    let summary = runtime.recover().await.expect("recover");
    assert_eq!(summary.awaiting_resume, 0);
    let restored = store.load(run_id()).await.expect("load").expect("run");
    assert_eq!(restored.control, RunControl::Stopped);
}
