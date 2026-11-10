//! P2 Task 3: durable approval/question inbox (fixture-driven).
//!
//! Pending requests are persisted before any UI notice; answers travel as
//! response intents with receipts and resolve passively through observations.
//! An epoch rotation stales every live entry until a new request is verified
//! — an old accept never applies to a new request, even with identical text.
//! Secret answers live memory-only behind a random [`VolatileAnswerRef`];
//! the durable intent carries the reference only, never the payload or a
//! content-derived hash. A stopped or restart-parked run opens no inference
//! when an answer lands: dispatch stays shut until explicit resume.
//!
//! Fixture shapes come from `tests/fixtures/inbox.json` (distilled from the
//! filtered `spikes/codex-app-server` captures); the sentinel secret below
//! is synthetic and must never appear in any durable artifact.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use central_application::contracts::{
    AppError, CancelObservation, Capabilities, CommitBatch, DispatchError, ExecutionRef,
    ExternalAnswer, ExternalRequest, ExternalRequestKey, OutboxIntent, OutboxStatus, Receipt,
    ReconcileEvidence, SendTurn, SessionRef, TurnRef,
};
use central_application::inbox::{InboxReconcileSummary, SecretVault};
use central_application::ports::{AgentGateway, Clock, IdSource, Store};
use central_application::Runtime;
use central_domain::{
    decide, Actor, ActorId, ActorKind, AnswerPayload, Attempt, AttemptId, AttemptStatus, Command,
    CommandEnvelope, CommandId, ContentHash, DomainError, Event, InboxKey, InboxKind, InboxStatus,
    IntentId, Observation, Revision, RunControl, RunId, RunState, Timestamp,
};
use serde::Deserialize;
use sha2::Digest;

// ---------- fixture loader ----------

#[derive(Debug, Deserialize)]
struct FixtureFile {
    requests: Vec<FixtureRequest>,
}

#[derive(Debug, Deserialize)]
struct FixtureRequest {
    name: String,
    kind: String,
    epoch: String,
    thread_id: String,
    turn_id: String,
    item_id: String,
    request_id: String,
    preview: String,
    is_secret: bool,
}

fn fixtures() -> HashMap<String, FixtureRequest> {
    let path = format!("{}/tests/fixtures/inbox.json", env!("CARGO_MANIFEST_DIR"));
    let text = std::fs::read_to_string(&path).expect("read inbox fixtures");
    let file: FixtureFile = serde_json::from_str(&text).expect("parse inbox fixtures");
    assert!(
        file.requests.len() >= 6,
        "need approval/question/decline/cancel/resolved shapes"
    );
    file.requests
        .into_iter()
        .map(|entry| (entry.name.clone(), entry))
        .collect()
}

fn inbox_key(fixture: &FixtureRequest) -> InboxKey {
    InboxKey {
        provider: "codex".to_string(),
        epoch: fixture.epoch.clone(),
        thread_id: fixture.thread_id.clone(),
        turn_id: fixture.turn_id.clone(),
        item_id: fixture.item_id.clone(),
        request_id: fixture.request_id.clone(),
    }
}

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

struct FakeGateway {
    epoch: Mutex<String>,
    replies: AtomicUsize,
    scripted_requests: Mutex<Vec<ExternalRequest>>,
    scripted_observations: Mutex<Vec<Observation>>,
}

impl FakeGateway {
    fn new(epoch: &str) -> Self {
        Self {
            epoch: Mutex::new(epoch.to_string()),
            replies: AtomicUsize::new(0),
            scripted_requests: Mutex::new(Vec::new()),
            scripted_observations: Mutex::new(Vec::new()),
        }
    }

    fn reply_count(&self) -> usize {
        self.replies.load(Ordering::SeqCst)
    }

    fn set_epoch(&self, epoch: &str) {
        *self.epoch.lock().expect("lock") = epoch.to_string();
    }
}

fn external_key(fixture: &FixtureRequest) -> ExternalRequestKey {
    ExternalRequestKey {
        session: SessionRef {
            provider: "codex".to_string(),
            instance_epoch: fixture.epoch.clone(),
            thread_id: fixture.thread_id.clone(),
        },
        turn_id: fixture.turn_id.clone(),
        item_id: fixture.item_id.clone(),
        request_id: fixture.request_id.clone(),
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
    async fn open(
        &self,
        request: central_application::contracts::OpenSession,
    ) -> Result<SessionRef, AppError> {
        Ok(SessionRef {
            provider: "fake".to_string(),
            instance_epoch: self.epoch.lock().expect("lock").clone(),
            thread_id: format!("thread-for-attempt-{}", request.attempt_id),
        })
    }
    async fn send(&self, request: SendTurn) -> Result<TurnRef, DispatchError> {
        Ok(TurnRef {
            session: request.session,
            turn_id: "turn-1".to_string(),
        })
    }
    async fn observe(&self, _session: &SessionRef) -> Result<Vec<Observation>, AppError> {
        Ok(self.scripted_observations.lock().expect("lock").clone())
    }
    async fn drain_requests(&self, session: &SessionRef) -> Result<Vec<ExternalRequest>, AppError> {
        let mut scripted = self.scripted_requests.lock().expect("lock");
        let matching: Vec<ExternalRequest> = scripted
            .iter()
            .filter(|request| {
                request.key.session.instance_epoch == session.instance_epoch
                    && request.key.session.thread_id == session.thread_id
            })
            .cloned()
            .collect();
        scripted.retain(|request| {
            !(request.key.session.instance_epoch == session.instance_epoch
                && request.key.session.thread_id == session.thread_id)
        });
        Ok(matching)
    }
    async fn answer(&self, _answer: ExternalAnswer) -> Result<(), DispatchError> {
        self.replies.fetch_add(1, Ordering::SeqCst);
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
    receipts: HashMap<(ActorId, CommandId), Receipt>,
    events: HashMap<RunId, Vec<Event>>,
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
        inner.events.entry(run).or_default().extend(batch.events);
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
    async fn events(&self, id: RunId) -> Result<Vec<Event>, AppError> {
        Ok(self
            .inner
            .lock()
            .expect("lock")
            .events
            .get(&id)
            .cloned()
            .unwrap_or_default())
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

fn system() -> Actor {
    Actor {
        id: ActorId::from_u128(101),
        kind: ActorKind::System,
    }
}

fn run_id() -> RunId {
    RunId::from_u128(10)
}

fn harness(
    store: Arc<MemoryStore>,
    gateway: Arc<FakeGateway>,
    secrets: Arc<SecretVault>,
) -> Runtime {
    Runtime::new(
        store as Arc<dyn Store>,
        Arc::new(FakeClock(AtomicI64::new(1_700_000_000_000))),
        Arc::new(FakeIds(AtomicU64::new(0))),
        gateway as Arc<dyn AgentGateway>,
        run_id(),
    )
    .with_secrets(secrets)
}

fn next_command(n: u128, command: Command) -> CommandEnvelope {
    CommandEnvelope {
        id: CommandId::from_u128(n),
        expected_revision: None,
        command,
    }
}

async fn created_runtime() -> (
    Arc<MemoryStore>,
    Arc<FakeGateway>,
    Arc<SecretVault>,
    Runtime,
) {
    let store = Arc::new(MemoryStore::default());
    let gateway = Arc::new(FakeGateway::new("epoch-1"));
    let secrets = Arc::new(SecretVault::new());
    let runtime = harness(store.clone(), gateway.clone(), secrets.clone());
    runtime
        .execute(
            user(),
            next_command(
                900,
                Command::CreateRun {
                    goal: "fixture goal".to_string(),
                    repository: "D:/Code/enolalab/central".to_string(),
                },
            ),
        )
        .await
        .expect("create run");
    // One running worker attempt that approvals/questions attach to.
    let mut state = store.load(run_id()).await.expect("load").expect("run");
    state.attempts.insert(
        AttemptId::from_u128(42),
        Attempt {
            id: AttemptId::from_u128(42),
            task_id: central_domain::TaskId::from_u128(1),
            plan_hash: central_domain::PlanHash::from_hex("h1"),
            status: AttemptStatus::Running,
            profile_id: None,
            created_at: Timestamp(1_700_000_000_000),
        },
    );
    store.seed_state(state);
    (store, gateway, secrets, runtime)
}

fn script_request(gateway: &FakeGateway, fixture: &FixtureRequest, attempt: Option<AttemptId>) {
    let key = external_key(fixture);
    gateway
        .scripted_requests
        .lock()
        .expect("lock")
        .push(ExternalRequest {
            key,
            kind: match fixture.kind.as_str() {
                "Approval" => InboxKind::Approval,
                "Question" => InboxKind::Question,
                other => panic!("unknown kind {other}"),
            },
            preview: fixture.preview.clone(),
            preview_hash: central_domain::preview_hash_for(&fixture.preview),
            attempt_id: attempt,
            is_secret: fixture.is_secret,
        });
}

fn session_for(fixture: &FixtureRequest) -> SessionRef {
    SessionRef {
        provider: "codex".to_string(),
        instance_epoch: fixture.epoch.clone(),
        thread_id: fixture.thread_id.clone(),
    }
}

// ---------- the four brief-named tests ----------

#[tokio::test]
async fn reply_scoped_to_epoch_and_preview() {
    let (store, _gateway, _secrets, runtime) = created_runtime().await;
    let fixtures = fixtures();
    let approval = &fixtures["approval"];
    script_request(&_gateway, approval, Some(AttemptId::from_u128(42)));
    let summary: InboxReconcileSummary = runtime
        .reconcile_inbox(&system(), &session_for(approval))
        .await
        .expect("reconcile records the approval");
    assert_eq!(summary.recorded, 1);

    let state = store.load(run_id()).await.expect("load").expect("run");
    assert_eq!(state.inbox.len(), 1);
    assert_eq!(
        state.attempts[&AttemptId::from_u128(42)].status,
        AttemptStatus::WaitingForApproval
    );

    // Domain shape mirrors the brief: a stale-epoch reply is StaleRequest.
    let stale_key = InboxKey {
        epoch: "epoch-0".to_string(),
        ..inbox_key(approval)
    };
    assert_eq!(
        decide(
            &state,
            &user(),
            &Command::ApproveAction {
                key: stale_key,
                preview_hash: central_domain::preview_hash_for(&approval.preview),
            },
            Timestamp(1_700_000_000_001),
        ),
        Err(DomainError::StaleRequest)
    );

    // Runtime surface maps it to a conflict naming staleness.
    let old_epoch = runtime
        .execute(
            user(),
            next_command(
                910,
                Command::ApproveAction {
                    key: InboxKey {
                        epoch: "epoch-0".to_string(),
                        ..inbox_key(approval)
                    },
                    preview_hash: central_domain::preview_hash_for(&approval.preview),
                },
            ),
        )
        .await;
    assert!(
        matches!(old_epoch, Err(AppError::Conflict(ref message)) if message.contains("stale")),
        "old epoch must be a stale-request conflict, got {old_epoch:?}"
    );

    // Wrong preview hash is stale too: the reply is not for the live text.
    let wrong_preview = runtime
        .execute(
            user(),
            next_command(
                911,
                Command::ApproveAction {
                    key: inbox_key(approval),
                    preview_hash: ContentHash::from_hex("0".repeat(64)),
                },
            ),
        )
        .await;
    assert!(
        matches!(wrong_preview, Err(AppError::Conflict(ref message)) if message.contains("stale")),
        "wrong preview must be a stale-request conflict, got {wrong_preview:?}"
    );

    // Scoped reply succeeds and keeps the immutable preview hash/actor.
    let before = store.load(run_id()).await.expect("load").expect("run");
    let before_entry = before.inbox.values().next().expect("entry").clone();
    runtime
        .execute(
            user(),
            next_command(
                912,
                Command::ApproveAction {
                    key: inbox_key(approval),
                    preview_hash: central_domain::preview_hash_for(&approval.preview),
                },
            ),
        )
        .await
        .expect("scoped approve commits");
    let after = store.load(run_id()).await.expect("load").expect("run");
    let after_entry = after.inbox.values().next().expect("entry").clone();
    assert_eq!(after_entry.status, InboxStatus::Answered);
    assert_eq!(after_entry.preview_hash, before_entry.preview_hash);
    assert_eq!(after_entry.requested_by, before_entry.requested_by);
}

#[tokio::test]
async fn replayed_request_is_one_entry() {
    let (store, gateway, _secrets, runtime) = created_runtime().await;
    let fixtures = fixtures();
    let approval = &fixtures["approval"];
    script_request(&gateway, approval, Some(AttemptId::from_u128(42)));
    runtime
        .reconcile_inbox(&system(), &session_for(approval))
        .await
        .expect("first ingest");
    // The harness replays the same request (same epoch, ids and text).
    script_request(&gateway, approval, Some(AttemptId::from_u128(42)));
    let summary = runtime
        .reconcile_inbox(&system(), &session_for(approval))
        .await
        .expect("replay ingest");
    assert_eq!(summary.duplicates, 1);

    let state = store.load(run_id()).await.expect("load").expect("run");
    assert_eq!(state.inbox.len(), 1);
}

#[tokio::test]
async fn resolved_cannot_accept() {
    let (store, gateway, _secrets, runtime) = created_runtime().await;
    let fixtures = fixtures();
    let approval = &fixtures["approval"];
    script_request(&gateway, approval, Some(AttemptId::from_u128(42)));
    runtime
        .reconcile_inbox(&system(), &session_for(approval))
        .await
        .expect("ingest");
    runtime
        .execute(
            user(),
            next_command(
                920,
                Command::ApproveAction {
                    key: inbox_key(approval),
                    preview_hash: central_domain::preview_hash_for(&approval.preview),
                },
            ),
        )
        .await
        .expect("approve");
    // The harness confirms the answer landed; passive reconcile resolves it.
    gateway
        .scripted_observations
        .lock()
        .expect("lock")
        .push(Observation {
            attempt_id: AttemptId::from_u128(42),
            provider_epoch: approval.epoch.clone(),
            dedup_key: format!("requestApproval:{}", approval.item_id),
            outcome: None,
            detail: String::new(),
        });
    let summary = runtime
        .reconcile_inbox(&system(), &session_for(approval))
        .await
        .expect("reconcile resolves");
    assert_eq!(summary.resolved, 1);
    let state = store.load(run_id()).await.expect("load").expect("run");
    assert_eq!(
        state.inbox.values().next().expect("entry").status,
        InboxStatus::Resolved
    );

    // The queued answer for the legitimate approve still goes out once;
    // resolution is passive and never cancels the user's committed reply.
    runtime
        .dispatch_once(run_id())
        .await
        .expect("dispatch answer");
    assert_eq!(gateway.reply_count(), 1);

    // A late accept of the resolved request is refused, never re-sent.
    let replies_before = gateway.reply_count();
    let late = runtime
        .execute(
            user(),
            next_command(
                921,
                Command::ApproveAction {
                    key: inbox_key(approval),
                    preview_hash: central_domain::preview_hash_for(&approval.preview),
                },
            ),
        )
        .await;
    assert!(
        matches!(late, Err(AppError::Conflict(ref message)) if message.contains("stale")),
        "resolved entry must refuse accepts, got {late:?}"
    );
    runtime.dispatch_once(run_id()).await.expect("dispatch");
    assert_eq!(gateway.reply_count(), replies_before);
}

#[tokio::test]
async fn answer_waits_for_resume() {
    let (store, gateway, _secrets, runtime) = created_runtime().await;
    let fixtures = fixtures();
    let question = &fixtures["question"];
    script_request(&gateway, question, Some(AttemptId::from_u128(42)));
    runtime
        .reconcile_inbox(&system(), &session_for(question))
        .await
        .expect("ingest");

    // Daemon restart parks the run; answering records but sends nothing.
    runtime.recover().await.expect("recover parks the run");
    let parked = store.load(run_id()).await.expect("load").expect("run");
    assert_eq!(parked.control, RunControl::AwaitingResume);
    runtime
        .execute(
            user(),
            next_command(
                930,
                Command::AnswerQuestion {
                    key: inbox_key(question),
                    preview_hash: central_domain::preview_hash_for(&question.preview),
                    payload: AnswerPayload::Plain("Beta".to_string()),
                },
            ),
        )
        .await
        .expect("answer commits while parked");
    let before_resume = gateway.reply_count();
    runtime
        .dispatch_once(run_id())
        .await
        .expect("gated dispatch");
    assert_eq!(before_resume, 0);
    assert_eq!(gateway.reply_count(), 0);

    // Explicit resume reopens the gate and the queued answer goes out once.
    runtime
        .execute(user(), next_command(931, Command::Resume))
        .await
        .expect("resume");
    runtime.dispatch_once(run_id()).await.expect("dispatch");
    let after_resume = gateway.reply_count();
    assert_eq!(after_resume, 1);
    runtime.dispatch_once(run_id()).await.expect("redispatch");
    assert_eq!(gateway.reply_count(), 1);
}

// ---------- reconnect vs restart, secrets, plan separation, stop ----------

#[tokio::test]
async fn restart_reuses_request_id_zero_after_stale() {
    let (store, gateway, _secrets, runtime) = created_runtime().await;
    let fixtures = fixtures();
    let approval = &fixtures["approval"];
    script_request(&gateway, approval, Some(AttemptId::from_u128(42)));
    runtime
        .reconcile_inbox(&system(), &session_for(approval))
        .await
        .expect("ingest");

    // Reconnect under the same epoch: the live entry stays valid.
    let still_live = store.load(run_id()).await.expect("load").expect("run");
    assert_eq!(
        still_live.inbox.values().next().expect("entry").status,
        InboxStatus::Pending
    );

    // Harness restart: same request id 0, new epoch and thread.
    gateway.set_epoch("epoch-2");
    let restarted = &fixtures["restart_approval_same_id"];
    assert_eq!(restarted.request_id, "0");
    script_request(&gateway, restarted, Some(AttemptId::from_u128(42)));
    let summary = runtime
        .reconcile_inbox(&system(), &session_for(restarted))
        .await
        .expect("restart reconcile");
    assert_eq!(summary.marked_stale, 1);
    assert_eq!(summary.recorded, 1);

    let state = store.load(run_id()).await.expect("load").expect("run");
    assert_eq!(state.inbox.len(), 2);
    let old_entry = state
        .inbox
        .values()
        .find(|entry| entry.epoch == "epoch-1")
        .expect("old entry");
    assert_eq!(old_entry.status, InboxStatus::Stale);

    // The old accept is stale even though the preview text is identical;
    // only the verified new request answers.
    let old_accept = runtime
        .execute(
            user(),
            next_command(
                940,
                Command::ApproveAction {
                    key: inbox_key(approval),
                    preview_hash: central_domain::preview_hash_for(&approval.preview),
                },
            ),
        )
        .await;
    assert!(
        matches!(old_accept, Err(AppError::Conflict(ref message)) if message.contains("stale")),
        "old epoch accept must be stale, got {old_accept:?}"
    );
    runtime
        .execute(
            user(),
            next_command(
                941,
                Command::ApproveAction {
                    key: inbox_key(restarted),
                    preview_hash: central_domain::preview_hash_for(&restarted.preview),
                },
            ),
        )
        .await
        .expect("new request answers despite identical text");
}

#[tokio::test]
async fn sentinel_secret_never_persists() {
    let (store, gateway, secrets, runtime) = created_runtime().await;
    let fixtures = fixtures();
    let secret_q = &fixtures["question_secret"];
    // Sentinel stands in for a real credential: random-looking, unique.
    let sentinel = "SENTINEL-SK-9f2c4b7aa01e4d8fb3c6d9e0a2b5f7c4";
    assert!(secret_q.is_secret);

    script_request(&gateway, secret_q, Some(AttemptId::from_u128(42)));
    runtime
        .reconcile_inbox(&system(), &session_for(secret_q))
        .await
        .expect("ingest secret question");

    runtime
        .answer_secret(
            user(),
            inbox_key(secret_q),
            central_domain::preview_hash_for(&secret_q.preview),
            sentinel.to_string(),
        )
        .await
        .expect("secret answer commits");

    // Sweep every durable surface: snapshot, outbox, audit events, receipts
    // and a debug trace. Neither the sentinel nor a content-derived hash of
    // it may appear anywhere.
    let state = store.load(run_id()).await.expect("load").expect("run");
    let snapshot = serde_json::to_string(&state).expect("encode snapshot");
    let intents = store.outbox(run_id()).await.expect("outbox");
    let intents_json = serde_json::to_string(&intents).expect("encode intents");
    let events = store.events(run_id()).await.expect("events");
    let events_json = serde_json::to_string(&events).expect("encode events");
    let trace = format!("{state:?} {intents:?} {events:?}");
    let digest = sha2::Sha256::digest(sentinel.as_bytes());
    let mut sentinel_hash = String::new();
    for byte in digest {
        sentinel_hash.push_str(&format!("{byte:02x}"));
    }
    for surface in [&snapshot, &intents_json, &events_json, &trace] {
        assert!(
            !surface.contains(sentinel),
            "secret payload leaked into a durable surface"
        );
        assert!(
            !surface.contains(&sentinel_hash),
            "content-derived secret hash leaked into a durable surface"
        );
    }

    // The live vault still serves the answer exactly once. The run is
    // Running throughout: no resume is needed and none is taken.
    let live_state = store.load(run_id()).await.expect("load").expect("run");
    assert_eq!(live_state.control, RunControl::Running);
    runtime.dispatch_once(run_id()).await.expect("dispatch");
    assert_eq!(gateway.reply_count(), 1);

    // A fresh process (empty vault, same store) sends nothing: the user
    // must re-enter the secret. No inference opens on the stale reference.
    let cold_gateway = Arc::new(FakeGateway::new("epoch-1"));
    let cold = harness(
        store.clone(),
        cold_gateway.clone(),
        Arc::new(SecretVault::new()),
    );
    let state = store.load(run_id()).await.expect("load").expect("run");
    assert_eq!(
        state.inbox.values().next().expect("entry").status,
        InboxStatus::Answered
    );
    cold.dispatch_once(run_id()).await.expect("cold dispatch");
    assert_eq!(cold_gateway.reply_count(), 0);
    assert_eq!(secrets.len(), 0, "vault is consumed after one send");
}

#[tokio::test]
async fn plan_approval_does_not_map_to_tool_approval() {
    let (_store, _gateway, _secrets, runtime) = created_runtime().await;
    // A tool-approve naming a plan hash is an unknown inbox request, never
    // an approval of anything.
    let err = runtime
        .execute(
            user(),
            next_command(
                960,
                Command::ApproveAction {
                    key: InboxKey {
                        provider: "codex".to_string(),
                        epoch: "epoch-1".to_string(),
                        thread_id: "plan-hash-as-thread".to_string(),
                        turn_id: "turn".to_string(),
                        item_id: "item".to_string(),
                        request_id: "0".to_string(),
                    },
                    preview_hash: ContentHash::from_hex("a".repeat(64)),
                },
            ),
        )
        .await
        .expect_err("plan hash is not an inbox key");
    assert!(
        matches!(err, AppError::NotFound(_)),
        "plan hash must be unknown to the tool inbox, got {err:?}"
    );
}

#[tokio::test]
async fn stopped_run_records_no_reply() {
    let (store, gateway, _secrets, runtime) = created_runtime().await;
    let fixtures = fixtures();
    let approval = &fixtures["cancel_example"];
    script_request(&gateway, approval, Some(AttemptId::from_u128(42)));
    runtime
        .reconcile_inbox(&system(), &session_for(approval))
        .await
        .expect("ingest");

    runtime
        .execute(user(), next_command(970, Command::Stop))
        .await
        .expect("stop");
    let stopped = store.load(run_id()).await.expect("load").expect("run");
    assert_eq!(
        stopped.inbox.values().next().expect("entry").status,
        InboxStatus::Stale
    );

    // Answering a stopped run fails closed; dispatch sends no answer.
    let answer = runtime
        .execute(
            user(),
            next_command(
                971,
                Command::ApproveAction {
                    key: inbox_key(approval),
                    preview_hash: central_domain::preview_hash_for(&approval.preview),
                },
            ),
        )
        .await;
    assert!(
        matches!(answer, Err(AppError::Conflict(ref message)) if message.contains("stale")),
        "stopped run must not accept answers, got {answer:?}"
    );
    runtime.dispatch_once(run_id()).await.expect("dispatch");
    assert_eq!(gateway.reply_count(), 0);
}

#[tokio::test]
async fn unrelated_observation_does_not_resolve_answered_approval() {
    let (store, gateway, _secrets, runtime) = created_runtime().await;
    let fixtures = fixtures();
    let approval = &fixtures["approval"];
    script_request(&gateway, approval, Some(AttemptId::from_u128(42)));
    runtime
        .reconcile_inbox(&system(), &session_for(approval))
        .await
        .expect("ingest");
    runtime
        .execute(
            user(),
            next_command(
                980,
                Command::ApproveAction {
                    key: inbox_key(approval),
                    preview_hash: central_domain::preview_hash_for(&approval.preview),
                },
            ),
        )
        .await
        .expect("approve");

    // An unrelated notification for the same attempt (a progress ping with no
    // item correlation) must not resolve the safety-critical approval: there
    // is no harness evidence for it.
    gateway
        .scripted_observations
        .lock()
        .expect("lock")
        .push(Observation {
            attempt_id: AttemptId::from_u128(42),
            provider_epoch: approval.epoch.clone(),
            dedup_key: "thread/status/changed:progress-ping".to_string(),
            outcome: None,
            detail: String::new(),
        });
    let summary = runtime
        .reconcile_inbox(&system(), &session_for(approval))
        .await
        .expect("reconcile observes the ping");
    assert_eq!(summary.observations, 1);
    assert_eq!(summary.resolved, 0);
    let state = store.load(run_id()).await.expect("load").expect("run");
    assert_eq!(
        state.inbox.values().next().expect("entry").status,
        InboxStatus::Answered,
        "unrelated observation must leave the answered approval unresolved"
    );

    // The matching observation (requestApproval for the same item) resolves it.
    gateway
        .scripted_observations
        .lock()
        .expect("lock")
        .push(Observation {
            attempt_id: AttemptId::from_u128(42),
            provider_epoch: approval.epoch.clone(),
            dedup_key: format!("requestApproval:{}", approval.item_id),
            outcome: None,
            detail: String::new(),
        });
    let summary = runtime
        .reconcile_inbox(&system(), &session_for(approval))
        .await
        .expect("reconcile resolves");
    assert_eq!(summary.resolved, 1);
    let state = store.load(run_id()).await.expect("load").expect("run");
    assert_eq!(
        state.inbox.values().next().expect("entry").status,
        InboxStatus::Resolved
    );
}

#[tokio::test]
async fn conflicting_reuse_counts_as_conflict_not_duplicate() {
    let (store, gateway, _secrets, runtime) = created_runtime().await;
    let fixtures = fixtures();
    let approval = &fixtures["approval"];
    script_request(&gateway, approval, Some(AttemptId::from_u128(42)));
    runtime
        .reconcile_inbox(&system(), &session_for(approval))
        .await
        .expect("ingest");

    // Same key (epoch/thread/turn/item/request) but different text: a protocol
    // violation, never a benign replay.
    let tampered_preview = "rm -rf C:\\fixture\\workspace\\scratch --tampered".to_string();
    gateway
        .scripted_requests
        .lock()
        .expect("lock")
        .push(ExternalRequest {
            key: external_key(approval),
            kind: InboxKind::Approval,
            preview: tampered_preview.clone(),
            preview_hash: central_domain::preview_hash_for(&tampered_preview),
            attempt_id: Some(AttemptId::from_u128(42)),
            is_secret: false,
        });
    let summary = runtime
        .reconcile_inbox(&system(), &session_for(approval))
        .await
        .expect("reconcile conflicting reuse");
    assert_eq!(summary.conflicts, 1);
    assert_eq!(summary.duplicates, 0);
    assert_eq!(summary.recorded, 0);

    // The stored entry is untouched: one entry, original text.
    let state = store.load(run_id()).await.expect("load").expect("run");
    assert_eq!(state.inbox.len(), 1);
    assert_eq!(
        state.inbox.values().next().expect("entry").preview,
        approval.preview
    );
}
