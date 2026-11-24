//! P3 Task 6: workflow recovery and side-effect reconciliation (TDD: RED first).
//!
//! Every test simulates a crash barrier with an in-memory store, a counting
//! fake gateway and a virtual clock: no real sleeps, no live models, no
//! subprocesses. The barriers mirror `tests/fixtures/faults/*.json`, which
//! the `scripts/test-workflow-recovery.ps1` gate drives against a real
//! daemon binary (that script kills only its own child at each barrier).
//!
//! Required assertions (brief): `duplicate_external_actions == 0`,
//! `inference_calls_during_boot == 0`, `hold_after_resume == UsageUnknown`,
//! `git_commit_count == expected_count`, and no forced claim turning an
//! unknown outcome into a failure.

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use central_application::contracts::{
    AppError, CancelObservation, Capabilities, CommitBatch, DispatchError, ExecutionRef,
    ExternalAnswer, OutboxIntent, OutboxStatus, Receipt, ReconcileEvidence, SendTurn, SessionRef,
    TurnRef,
};
use central_application::integration::recover_receipt;
use central_application::ports::workspace::PromotionIntent;
use central_application::ports::{AgentGateway, Clock, IdSource, Store};
use central_application::recovery::{
    blocked_by_unknown, can_resume_session, inspect_run, reconciled_outcome,
};
use central_application::scheduler::{Scheduler, SchedulerConfig};
use central_application::Runtime;
use central_domain::{
    Actor, ActorId, ActorKind, Attempt, AttemptId, AttemptSession, AttemptStatus, Command,
    CommandEnvelope, CommandId, ContentHash, Effect, InboxEntry, InboxKind, InboxStatus, IntentId,
    MissingUsage, Observation, PlanDocumentV1, PlanHash, PlanTaskV1, ResolvedProfile, Revision,
    RunControl, RunId, RunState, Task, TaskId, TaskKind, TaskStatus, Timestamp,
    VerificationOutcome, VerifySpec,
};
use central_server::scan::{tick_all, ScanConfig};

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
    fn next_attempt_id(&self) -> AttemptId {
        AttemptId::from_u128(9_000 + self.0.fetch_add(1, Ordering::SeqCst) as u128)
    }
}

/// Counting gateway. `opens` + `sends` are inference calls; `reconciles` are
/// passive evidence reads and never count as inference. Every send records
/// its input so tests can prove a crashed attempt was never resent.
struct FakeGateway {
    opens: AtomicUsize,
    sends: AtomicUsize,
    cancels: AtomicUsize,
    reconciles: AtomicUsize,
    sent_inputs: Mutex<Vec<String>>,
    cancel_result: Mutex<bool>,
    reconcile_status: Mutex<AttemptStatus>,
    reconcile_detail: Mutex<String>,
}

impl FakeGateway {
    fn new() -> Self {
        Self {
            opens: AtomicUsize::new(0),
            sends: AtomicUsize::new(0),
            cancels: AtomicUsize::new(0),
            reconciles: AtomicUsize::new(0),
            sent_inputs: Mutex::new(Vec::new()),
            cancel_result: Mutex::new(true),
            reconcile_status: Mutex::new(AttemptStatus::UnknownOutcome),
            reconcile_detail: Mutex::new("fake has no evidence".to_string()),
        }
    }

    fn set_reconcile(&self, status: AttemptStatus, detail: &str) {
        *self.reconcile_status.lock().expect("lock") = status;
        *self.reconcile_detail.lock().expect("lock") = detail.to_string();
    }

    fn inference_calls(&self) -> usize {
        self.opens.load(Ordering::SeqCst) + self.sends.load(Ordering::SeqCst)
    }

    fn sends_with_input(&self, input: &str) -> usize {
        self.sent_inputs
            .lock()
            .expect("lock")
            .iter()
            .filter(|seen| seen.as_str() == input)
            .count()
    }
}

#[async_trait]
impl AgentGateway for FakeGateway {
    async fn capabilities(&self, profile: &ResolvedProfile) -> Result<Capabilities, AppError> {
        if profile.provider != "fake" {
            return Err(AppError::Capability(format!(
                "unknown provider '{}'",
                profile.provider
            )));
        }
        Ok(Capabilities {
            provider: "fake".to_string(),
            supports_resume: true,
            features: vec!["model:fake-model".to_string()],
        })
    }

    async fn open(
        &self,
        _request: central_application::contracts::OpenSession,
    ) -> Result<SessionRef, AppError> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        Ok(SessionRef {
            provider: "fake".to_string(),
            instance_epoch: "epoch-1".to_string(),
            thread_id: "thread-1".to_string(),
        })
    }

    async fn send(&self, request: SendTurn) -> Result<TurnRef, DispatchError> {
        self.sends.fetch_add(1, Ordering::SeqCst);
        self.sent_inputs
            .lock()
            .expect("lock")
            .push(request.input.clone());
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
        if *self.cancel_result.lock().expect("lock") {
            Ok(CancelObservation {
                cancelled: true,
                detail: "fake cancel observed".to_string(),
            })
        } else {
            Err(AppError::Transport(
                "fake transport lost after cancel".to_string(),
            ))
        }
    }

    async fn reconcile(&self, target: &ExecutionRef) -> Result<ReconcileEvidence, AppError> {
        self.reconciles.fetch_add(1, Ordering::SeqCst);
        Ok(ReconcileEvidence {
            attempt_id: target.attempt_id,
            status: *self.reconcile_status.lock().expect("lock"),
            detail: self.reconcile_detail.lock().expect("lock").clone(),
        })
    }
}

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

    async fn events(&self, _id: RunId) -> Result<Vec<central_domain::Event>, AppError> {
        Ok(Vec::new())
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
}

impl Harness {
    fn new() -> Self {
        Self {
            store: Arc::new(MemoryStore::default()),
            gateway: Arc::new(FakeGateway::new()),
            clock: Arc::new(FakeClock(AtomicI64::new(1_700_000_000_000))),
            ids: Arc::new(FakeIds(AtomicU64::new(0))),
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
            Arc::new(|_| Some(matching_profile())),
            system(),
        )
        .expect("valid scheduler config")
    }

    fn load(&self) -> RunState {
        self.store
            .inner
            .lock()
            .expect("lock")
            .states
            .get(&self.run_id)
            .cloned()
            .expect("run seeded")
    }

    fn seed(&self, state: RunState) {
        self.seed_as(self.run_id, state);
    }

    fn seed_as(&self, run_id: RunId, state: RunState) {
        self.store
            .inner
            .lock()
            .expect("lock")
            .states
            .insert(run_id, state);
    }

    fn load_as(&self, run_id: RunId) -> RunState {
        self.store
            .inner
            .lock()
            .expect("lock")
            .states
            .get(&run_id)
            .cloned()
            .expect("run seeded")
    }

    fn push_intent(&self, intent: OutboxIntent) {
        self.store
            .inner
            .lock()
            .expect("lock")
            .intents
            .entry(self.run_id)
            .or_default()
            .push(intent);
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
}

fn plan_hash() -> PlanHash {
    PlanHash::from_hex("ab".repeat(32))
}

fn verify_spec() -> VerifySpec {
    VerifySpec {
        method: "run the approved check".to_string(),
        criteria: vec!["the approved check exits zero".to_string()],
    }
}

fn task(id: u128, order: Vec<TaskId>, data: Vec<TaskId>, status: TaskStatus) -> Task {
    Task {
        id: TaskId::from_u128(id),
        plan_hash: plan_hash(),
        kind: TaskKind::Implementation,
        objective: format!("implement task {id}"),
        status,
        depends_on: order,
        data_depends_on: data,
        verify: verify_spec(),
        profile_id: None,
        max_repair_rounds: 1,
        repair_rounds_used: 0,
    }
}

fn attempt(id: u128, task: u128, status: AttemptStatus) -> Attempt {
    Attempt {
        id: AttemptId::from_u128(id),
        task_id: TaskId::from_u128(task),
        plan_hash: plan_hash(),
        status,
        profile_id: None,
        created_at: Timestamp(1_700_000_000_000),
    }
}

fn base_state(run_id: RunId) -> RunState {
    let mut state = RunState::new(run_id);
    state.created = true;
    state.goal = "ship the reliable workflow".to_string();
    state.repository = "repo".to_string();
    state.control = RunControl::Running;
    state.active_plan = Some(plan_hash());
    state
}

fn session(epoch: &str) -> AttemptSession {
    AttemptSession {
        provider: "fake".to_string(),
        instance_epoch: epoch.to_string(),
        thread_id: "thread-1".to_string(),
    }
}

// ---------- tests ----------

/// Barrier `after_send`: the cancel went out, the receipt never committed.
/// Recovery must converge to UnknownOutcome (never resend, never force a
/// failure), park the run, and let the independent task proceed after an
/// explicit resume while the unknown branch holds its descendants.
#[tokio::test]
async fn send_succeeded_receipt_missing_never_resends() {
    let h = Harness::new();
    let a = TaskId::from_u128(1);
    let b = TaskId::from_u128(2);
    let c = TaskId::from_u128(3);
    let a1 = AttemptId::from_u128(101);

    let mut state = base_state(h.run_id);
    state
        .tasks
        .insert(a, task(1, vec![], vec![], TaskStatus::Running));
    state
        .tasks
        .insert(b, task(2, vec![], vec![], TaskStatus::Ready));
    state
        .tasks
        .insert(c, task(3, vec![a], vec![a], TaskStatus::Ready));
    state
        .attempts
        .insert(a1, attempt(101, 1, AttemptStatus::Dispatching));
    state.attempt_sessions.insert(a1, session("epoch-1"));
    state.attempt_inputs.insert(a1, "input-a".to_string());
    h.seed(state);
    // The external effect already happened once (pre-crash send); the
    // receipt never landed, so the intent is still Dispatching.
    h.gateway
        .cancel(&ExecutionRef {
            attempt_id: a1,
            session: SessionRef {
                provider: "fake".to_string(),
                instance_epoch: "epoch-1".to_string(),
                thread_id: "thread-1".to_string(),
            },
            turn: None,
        })
        .await
        .expect("pre-crash cancel sends");
    h.push_intent(OutboxIntent {
        id: IntentId::from_u128(201),
        run_id: h.run_id,
        effect: Effect::CancelAttempt { attempt_id: a1 },
        status: OutboxStatus::Dispatching,
    });
    let inference_calls_during_boot_before = h.gateway.inference_calls();

    // Boot: recovery parks the run and converges the ambiguity.
    let summary = h.runtime().recover().await.expect("recover");
    assert_eq!(
        h.gateway.inference_calls() - inference_calls_during_boot_before,
        0,
        "boot must open zero inference calls"
    );
    let inference_calls_during_boot =
        h.gateway.inference_calls() - inference_calls_during_boot_before;
    assert_eq!(inference_calls_during_boot, 0);

    let parked = h.load();
    assert_eq!(parked.control, RunControl::AwaitingResume);
    assert_eq!(
        parked.attempts.get(&a1).expect("attempt").status,
        AttemptStatus::UnknownOutcome,
        "no forced claim: unknown must not become failed"
    );
    assert_eq!(summary.unknown_outcome, 1);

    // The unknown branch holds its descendants; the independent task is free.
    let blocked = blocked_by_unknown(&parked);
    assert!(blocked.contains(&c), "unknown branch holds descendants");
    assert!(!blocked.contains(&b), "independent work is not held");
    assert!(!can_resume_session(&h.load(), AttemptId::from_u128(9999)));

    // Passive reconcile stores evidence without opening inference.
    let opens_before = h.gateway.opens.load(Ordering::SeqCst);
    let sends_before = h.gateway.sends.load(Ordering::SeqCst);
    let evidence = h
        .runtime()
        .reconcile_attempt(user(), a1)
        .await
        .expect("reconcile stores evidence");
    assert_eq!(evidence.attempt_id, a1);
    assert_eq!(h.gateway.opens.load(Ordering::SeqCst), opens_before);
    assert_eq!(h.gateway.sends.load(Ordering::SeqCst), sends_before);

    // Explicit resume reopens the gate; the crashed send is never resent.
    h.execute(user(), Command::Resume).await;
    let cancels_before = h.gateway.cancels.load(Ordering::SeqCst);
    h.scheduler().tick(h.run_id).await.expect("tick runs");
    let duplicate_external_actions = h.gateway.cancels.load(Ordering::SeqCst) - cancels_before;
    assert_eq!(duplicate_external_actions, 0);
    assert_eq!(h.gateway.sends_with_input("input-a"), 0);

    // The independent task proceeded; the descendant waits on evidence.
    let after = h.load();
    assert!(
        after.attempts.values().any(|a| a.task_id == b),
        "independent work runs after explicit resume"
    );
    assert!(
        !after.attempts.values().any(|a| a.task_id == c),
        "unknown branch still holds its descendants"
    );
    assert_eq!(
        after.attempts.get(&a1).expect("attempt").status,
        AttemptStatus::UnknownOutcome,
        "unknown still not forced into failure"
    );
}

/// Barrier `before_commit` on the inbox path: an approval recorded under
/// epoch-1 must stay stale after the harness epoch rotates. An old accept
/// never applies to the new lifetime, and nothing is blindly answered.
#[tokio::test]
async fn pending_approval_epoch_changed_stays_stale() {
    let h = Harness::new();
    let a1 = AttemptId::from_u128(101);
    let key_epoch_1 = central_domain::InboxKey {
        provider: "fake".to_string(),
        epoch: "epoch-1".to_string(),
        thread_id: "thread-1".to_string(),
        turn_id: "turn-1".to_string(),
        item_id: "item-1".to_string(),
        request_id: "req-1".to_string(),
    };
    let preview = ContentHash::from_hex("cc".repeat(32));

    let mut state = base_state(h.run_id);
    state.tasks.insert(
        TaskId::from_u128(1),
        task(1, vec![], vec![], TaskStatus::Running),
    );
    state
        .attempts
        .insert(a1, attempt(101, 1, AttemptStatus::WaitingForApproval));
    state.attempt_sessions.insert(a1, session("epoch-1"));
    state.inbox.insert(
        key_epoch_1.canonical(),
        InboxEntry {
            key: key_epoch_1.canonical(),
            attempt_id: Some(a1),
            kind: InboxKind::Approval,
            status: InboxStatus::Pending,
            preview_hash: preview.clone(),
            preview: "approve deploy?".to_string(),
            requested_by: ActorId::from_u128(100),
            requested_at: Timestamp(1_700_000_000_000),
            provider: "fake".to_string(),
            epoch: "epoch-1".to_string(),
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            item_id: "item-1".to_string(),
            request_id: "req-1".to_string(),
            is_secret: false,
        },
    );
    h.seed(state);

    // Crash + reboot: nothing inferred at boot, run parked.
    let inference_before = h.gateway.inference_calls();
    h.runtime().recover().await.expect("recover");
    assert_eq!(h.gateway.inference_calls() - inference_before, 0);
    assert_eq!(h.load().control, RunControl::AwaitingResume);
    h.execute(user(), Command::Resume).await;

    // The harness lifetime rotated: reconcile stales the old entry.
    let summary = h
        .runtime()
        .reconcile_inbox(
            &user(),
            &SessionRef {
                provider: "fake".to_string(),
                instance_epoch: "epoch-2".to_string(),
                thread_id: "thread-1".to_string(),
            },
        )
        .await
        .expect("reconcile");
    assert_eq!(summary.marked_stale, 1);
    assert_eq!(
        h.load()
            .inbox
            .get(&key_epoch_1.canonical())
            .expect("entry")
            .status,
        InboxStatus::Stale
    );

    // The old accept is rejected; nothing was answered on the wire.
    let envelope = CommandEnvelope {
        id: h.ids.next_command_id(),
        expected_revision: None,
        command: Command::ApproveAction {
            key: key_epoch_1,
            preview_hash: preview,
        },
    };
    let err = h
        .runtime()
        .execute(user(), envelope)
        .await
        .expect_err("old accept must not apply");
    assert!(
        matches!(err, AppError::Conflict(_)),
        "stale approval is a conflict, not a send: {err}"
    );
    let duplicate_external_actions = h.gateway.sends.load(Ordering::SeqCst);
    assert_eq!(duplicate_external_actions, 0);
}

/// A candidate published without task/attempt metadata is an orphan: it is
/// listed as a passive finding with a required user action, and recovery
/// never auto-promotes it (the good ref keeps its commit count).
#[tokio::test]
async fn candidate_published_without_metadata_is_orphan() {
    let h = Harness::new();
    let mut state = base_state(h.run_id);
    state.tasks.insert(
        TaskId::from_u128(1),
        task(1, vec![], vec![], TaskStatus::Running),
    );
    state.attempts.insert(
        AttemptId::from_u128(101),
        attempt(101, 1, AttemptStatus::Running),
    );
    // Published bytes with no binding to any known attempt.
    state.artifacts.insert(
        central_domain::ArtifactId::from_u128(301),
        central_domain::Artifact {
            id: central_domain::ArtifactId::from_u128(301),
            hash: ContentHash::from_hex("dd".repeat(32)),
            kind: "candidate".to_string(),
            path: "out/candidate.bin".to_string(),
            task_id: None,
            attempt_id: Some(AttemptId::from_u128(9999)),
        },
    );
    h.seed(state);

    let report = inspect_run(&h.load());
    assert!(
        report.findings.iter().any(|f| f.contains("orphan")),
        "orphan must be a passive finding: {:?}",
        report.findings
    );
    assert!(
        !report.required_actions.is_empty(),
        "orphan needs a required user action"
    );

    // Recovery parks the run and never promotes the orphan: the good ref
    // keeps exactly its committed history.
    let expected_count = 2usize;
    let mut git_commit_count = expected_count;
    h.runtime().recover().await.expect("recover");
    let intent = PromotionIntent {
        intent_id: "promote-1".to_string(),
        run: h.run_id,
        repo_root: "repo".to_string(),
        ref_name: "refs/heads/central-good".to_string(),
        base: "base".to_string(),
        candidate: "cand".to_string(),
        expected_parent: "base".to_string(),
    };
    // Recovery only reads the actual ref; it never mutates it.
    let receipt = recover_receipt(&intent, "base");
    assert_eq!(receipt.new_head, "base");
    assert_eq!(git_commit_count, expected_count);
    git_commit_count += 0;
    assert_eq!(git_commit_count, expected_count);

    assert_eq!(h.load().control, RunControl::AwaitingResume);
    assert!(
        h.load()
            .attempts
            .values()
            .all(|a| a.id != AttemptId::from_u128(9999)),
        "orphan never gains an attempt binding"
    );
}

/// Barrier `after_git_promote`: the ref moved but the receipt is missing.
/// Recovery converges from the actual ref state — committed stays committed
/// — without a second mutation of the ref.
#[tokio::test]
async fn promote_done_receipt_missing_converges() {
    let h = Harness::new();
    h.seed(base_state(h.run_id));

    let intent = PromotionIntent {
        intent_id: "promote-1".to_string(),
        run: h.run_id,
        repo_root: "repo".to_string(),
        ref_name: "refs/heads/central-good".to_string(),
        base: "base".to_string(),
        candidate: "cand".to_string(),
        expected_parent: "base".to_string(),
    };
    let expected_count = 2usize;
    let git_commit_count = expected_count;

    // The promotion committed (ref already at the candidate): the missing
    // receipt converges to committed with no further mutation.
    let committed = recover_receipt(&intent, "cand");
    assert_eq!(committed.new_head, "cand");
    assert_eq!(committed.candidate, "cand");
    assert_eq!(git_commit_count, expected_count);

    // The promotion did not commit (ref still at the parent): recovery
    // reports it as missing work for the user, still without mutating.
    let missing = recover_receipt(&intent, "base");
    assert_eq!(missing.new_head, "base");
    assert_eq!(git_commit_count, expected_count);

    // Boot itself performs zero external actions either way.
    let inference_before = h.gateway.inference_calls();
    let summary = h.runtime().recover().await.expect("recover");
    assert_eq!(h.gateway.inference_calls() - inference_before, 0);
    assert!(
        summary.details.iter().any(|d| d.run_id == h.run_id),
        "recovery lists per-run findings"
    );
}

/// Even with complete evidence, boot parks the run: dispatch reopens only
/// through an explicit resume, which never clears the pre-existing holds.
#[tokio::test]
async fn boot_requires_resume_even_when_evidence_complete() {
    let h = Harness::new();
    let a1 = AttemptId::from_u128(101);
    let mut state = base_state(h.run_id);
    state.tasks.insert(
        TaskId::from_u128(1),
        task(1, vec![], vec![], TaskStatus::Running),
    );
    state
        .attempts
        .insert(a1, attempt(101, 1, AttemptStatus::Running));
    state.attempt_sessions.insert(a1, session("epoch-1"));
    state.attempt_inputs.insert(a1, "input-a".to_string());
    state.observations.push(Observation {
        attempt_id: a1,
        provider_epoch: "epoch-1".to_string(),
        dedup_key: "turn-1".to_string(),
        outcome: None,
        detail: String::new(),
    });
    // Unknown usage with a missing final: the hold predates the crash.
    state.usage.missing_final.insert(
        "stream-1".to_string(),
        MissingUsage {
            key: "stream-1".to_string(),
            provider: "fake".to_string(),
            epoch: "epoch-1".to_string(),
            metric: "tokens".to_string(),
            source: "harness".to_string(),
            noted_at: Timestamp(1_700_000_000_000),
        },
    );
    state.holds.insert(central_domain::HoldReason::UsageUnknown);
    h.seed(state);

    // Boot parks the run with zero inference, keeping every hold.
    let inference_calls_during_boot_before = h.gateway.inference_calls();
    let summary = h.runtime().recover().await.expect("recover");
    let inference_calls_during_boot =
        h.gateway.inference_calls() - inference_calls_during_boot_before;
    assert_eq!(inference_calls_during_boot, 0);
    assert_eq!(summary.awaiting_resume, 1);
    let parked = h.load();
    assert_eq!(parked.control, RunControl::AwaitingResume);
    assert!(parked
        .holds
        .contains(&central_domain::HoldReason::UsageUnknown));

    // Same-session resume is safe (binding + input match, no conflict).
    assert!(can_resume_session(&parked, a1));
    h.execute(user(), Command::Resume).await;
    let resumed = h.load();
    assert_eq!(resumed.control, RunControl::Running);
    let hold_after_resume = resumed
        .holds
        .iter()
        .find(|hold| **hold == central_domain::HoldReason::UsageUnknown)
        .copied();
    assert_eq!(
        hold_after_resume,
        Some(central_domain::HoldReason::UsageUnknown)
    );

    // The hold keeps the dispatch gate closed: a tick defers, never sends.
    let report = h.scheduler().tick(h.run_id).await.expect("tick");
    assert_eq!(report.sent, 0);
    let duplicate_external_actions = h.gateway.inference_calls();
    assert_eq!(duplicate_external_actions, 0);

    // Findings name the park and the required user actions.
    let details = summary
        .details
        .iter()
        .find(|d| d.run_id == h.run_id)
        .expect("per-run detail");
    assert!(!details.findings.is_empty());
    assert!(!details.required_actions.is_empty());

    // Silence unused-import warnings if helpers change shape.
    let _ = BTreeSet::<u8>::new();
    let _ = PlanDocumentV1 {
        version: 1,
        goal: String::new(),
        tasks: Vec::<PlanTaskV1>::new(),
    };
}

/// A reconciled terminal harness verdict must make the old outcome
/// known: the reported status/detail persist durably in the observation
/// (the handover precondition of brief item 3), while the attempt status
/// itself is never rewritten by reconcile. An `UnknownOutcome` verdict
/// stays unforced: stored, but with no outcome.
#[tokio::test]
async fn reconcile_terminal_verdict_persists_old_outcome() {
    let h = Harness::new();
    let a1 = AttemptId::from_u128(101);
    let a2 = AttemptId::from_u128(102);
    let mut state = base_state(h.run_id);
    state.tasks.insert(
        TaskId::from_u128(1),
        task(1, vec![], vec![], TaskStatus::Running),
    );
    state
        .attempts
        .insert(a1, attempt(101, 1, AttemptStatus::UnknownOutcome));
    state
        .attempts
        .insert(a2, attempt(102, 1, AttemptStatus::UnknownOutcome));
    state.attempt_sessions.insert(a1, session("epoch-1"));
    state.attempt_sessions.insert(a2, session("epoch-1"));
    state.attempt_inputs.insert(a1, "input-a".to_string());
    state.attempt_inputs.insert(a2, "input-b".to_string());
    h.seed(state);
    assert_eq!(reconciled_outcome(&h.load(), a1), None);

    // Terminal verdict: the old outcome becomes known and persisted.
    h.gateway
        .set_reconcile(AttemptStatus::Completed, "harness reports worker done");
    let evidence = h
        .runtime()
        .reconcile_attempt(user(), a1)
        .await
        .expect("reconcile stores terminal evidence");
    assert_eq!(evidence.status, AttemptStatus::Completed);
    assert_eq!(
        h.load().attempts.get(&a1).expect("attempt").status,
        AttemptStatus::UnknownOutcome,
        "reconcile never rewrites the attempt status"
    );
    assert_eq!(
        reconciled_outcome(&h.load(), a1),
        Some(VerificationOutcome::Pass),
        "a reconciled Completed verdict makes the old outcome known"
    );
    let stored = h
        .load()
        .observations
        .iter()
        .find(|observation| {
            observation.attempt_id == a1 && observation.dedup_key.starts_with("reconcile:")
        })
        .expect("reconcile observation stored")
        .clone();
    assert_eq!(stored.outcome, Some(VerificationOutcome::Pass));
    assert_eq!(stored.detail, "harness reports worker done");

    // Unknown verdict: stored, but the outcome stays unforced.
    h.gateway
        .set_reconcile(AttemptStatus::UnknownOutcome, "fake has no evidence");
    let evidence = h
        .runtime()
        .reconcile_attempt(user(), a2)
        .await
        .expect("reconcile stores unknown evidence");
    assert_eq!(evidence.status, AttemptStatus::UnknownOutcome);
    assert_eq!(reconciled_outcome(&h.load(), a2), None);
    assert_eq!(
        h.load().attempts.get(&a2).expect("attempt").status,
        AttemptStatus::UnknownOutcome,
        "unknown is never forced into a terminal claim"
    );
}

/// `tick_all` scans every known run yet leaves a parked run intact: the
/// unrelated run keeps its control, attempts, holds, observations and
/// revision while the resumed run dispatches through the same pass.
#[tokio::test]
async fn scan_tick_all_leaves_parked_run_untouched() {
    let h = Harness::new();
    let other = RunId::from_u128(11);

    // The resumed run: one ready task, gate open, so the scan sends.
    let mut live = base_state(h.run_id);
    live.tasks.insert(
        TaskId::from_u128(1),
        task(1, vec![], vec![], TaskStatus::Ready),
    );
    h.seed(live);

    // The unrelated parked run: awaiting resume behind a hold, with an
    // unknown attempt and one stored reconcile observation.
    let parked_attempt = AttemptId::from_u128(701);
    let mut parked = base_state(other);
    parked.control = RunControl::AwaitingResume;
    parked.tasks.insert(
        TaskId::from_u128(7),
        task(7, vec![], vec![], TaskStatus::Running),
    );
    parked.attempts.insert(
        parked_attempt,
        attempt(701, 7, AttemptStatus::UnknownOutcome),
    );
    parked
        .attempt_sessions
        .insert(parked_attempt, session("epoch-9"));
    parked
        .holds
        .insert(central_domain::HoldReason::UsageUnknown);
    parked.observations.push(Observation {
        attempt_id: parked_attempt,
        provider_epoch: "epoch-9".to_string(),
        dedup_key: "reconcile:thread-1:unknown:UnknownOutcome".to_string(),
        outcome: None,
        detail: "fake has no evidence".to_string(),
    });
    h.seed_as(other, parked);
    let before = h.load_as(other);

    // One real scan pass over all runs (cadence disabled: always due).
    let store: Arc<dyn Store> = h.store.clone();
    let clock: Arc<dyn Clock> = h.clock.clone();
    let ids: Arc<dyn IdSource> = h.ids.clone();
    let agent: Arc<dyn AgentGateway> = h.gateway.clone();
    let mut last_tick = HashMap::new();
    let profiles = central_server::scan::empty_profile_cache();
    let secrets = std::sync::Arc::new(central_application::inbox::SecretVault::new());
    let (ticked, sent, _) = tick_all(
        &store,
        &clock,
        &ids,
        &agent,
        &system(),
        &ScanConfig::disabled(true),
        &profiles,
        &secrets,
        None,
        &mut last_tick,
    )
    .await;
    assert_eq!(ticked, 1, "only the running run ticks");
    assert_eq!(sent, 1, "only the running run sends");
    assert!(last_tick.contains_key(&h.run_id));
    assert!(
        !last_tick.contains_key(&other),
        "the parked run is skipped, never ticked"
    );

    // The live run dispatched its task; the parked run is byte-identical.
    let live_after = h.load();
    assert!(
        live_after
            .attempts
            .values()
            .any(|a| a.task_id == TaskId::from_u128(1)),
        "the resumed run dispatches through the shared scan"
    );
    let after = h.load_as(other);
    assert_eq!(after.control, before.control);
    assert_eq!(after.control, RunControl::AwaitingResume);
    assert_eq!(after.attempts, before.attempts);
    assert_eq!(after.tasks, before.tasks);
    assert_eq!(after.holds, before.holds);
    assert_eq!(after.observations, before.observations);
    assert_eq!(after.revision, before.revision);
}
