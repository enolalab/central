//! P3 Task 4: revision impact, stale outputs and reuse assessment (TDD: RED first).
//!
//! A revision proposal is immutable: it carries a diff, an impact set
//! (explicitly changed nodes plus data descendants) and an optional
//! leader-supplied semantic expansion that can only grow the machine
//! impact, never shrink it. While the proposal pends, impacted queued
//! descendants are held; independent tasks stay ready. User approval
//! validates the hash (and the envelope revision, the stale-tab guard)
//! before activating. Running attempts keep their old plan binding;
//! their late results are quarantined with the original provenance and
//! never relabeled, and reuse is assessed explicitly: unchanged evidence
//! is reused only when input, candidate, criteria and environment all
//! still match, changed data input schedules verification first, and a
//! criteria change without an explanation is refused.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use central_application::contracts::{
    AppError, CancelObservation, Capabilities, CommitBatch, DispatchError, ExecutionRef,
    ExternalAnswer, OutboxIntent, OutboxStatus, Receipt, ReconcileEvidence, SendTurn, SessionRef,
    TurnRef,
};
use central_application::ports::{AgentGateway, Clock, IdSource, Store};
use central_application::quality_loop::QualityLoop;
use central_application::revisions::{
    criteria_explanation_text, current_bindings, recommend_reuse, reverify_first,
};
use central_application::verification::{criteria_hash as app_criteria_hash, SealedCandidate};
use central_application::Runtime;
use central_domain::{
    assess_impact, assess_reuse, criteria_hash_for, describe_criteria_impact, Actor, ActorId,
    ActorKind, Attempt, AttemptId, AttemptSession, AttemptStatus, Command, CommandEnvelope,
    CommandId, ContentHash, IntentId, Observation, PlanDocumentV1, PlanHash, PlanTaskV1,
    ReuseBindings, ReuseDecision, Revision, RunId, RunState, StaleCandidate, TaskId, TaskKind,
    TaskStatus, Timestamp, VerificationOutcome, VerificationReport, VerifySpec,
};

// ---------- deterministic fakes ----------

struct FakeClock(Arc<AtomicI64>);

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

struct FakeGateway {
    sent: Arc<AtomicUsize>,
}

impl FakeGateway {
    fn new() -> Self {
        Self {
            sent: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl AgentGateway for FakeGateway {
    async fn capabilities(
        &self,
        profile: &central_domain::ResolvedProfile,
    ) -> Result<Capabilities, AppError> {
        if profile.provider != "fake" {
            return Err(AppError::Capability("unknown provider".to_string()));
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
        Ok(SessionRef {
            provider: "fake".to_string(),
            instance_epoch: "epoch-1".to_string(),
            thread_id: "thread-1".to_string(),
        })
    }

    async fn send(&self, request: SendTurn) -> Result<TurnRef, DispatchError> {
        self.sent.fetch_add(1, Ordering::SeqCst);
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

// ---------- harness ----------

fn user() -> Actor {
    Actor {
        id: ActorId::from_u128(100),
        kind: ActorKind::User,
    }
}

fn leader() -> Actor {
    Actor {
        id: ActorId::from_u128(101),
        kind: ActorKind::Leader,
    }
}

fn verifier() -> Actor {
    Actor {
        id: ActorId::from_u128(102),
        kind: ActorKind::Verifier,
    }
}

fn system() -> Actor {
    Actor {
        id: ActorId::from_u128(103),
        kind: ActorKind::System,
    }
}

fn verify_spec() -> VerifySpec {
    VerifySpec {
        method: "run the approved check".to_string(),
        criteria: vec!["the approved check exits zero".to_string()],
    }
}

fn verify_spec_v2() -> VerifySpec {
    VerifySpec {
        method: "run the approved check".to_string(),
        criteria: vec![
            "the approved check exits zero".to_string(),
            "no new warnings".to_string(),
        ],
    }
}

fn plan_task(id: TaskId, order: Vec<TaskId>, data: Vec<TaskId>) -> PlanTaskV1 {
    plan_task_with(
        id,
        order,
        data,
        format!("implement task {id}"),
        verify_spec(),
    )
}

fn plan_task_with(
    id: TaskId,
    order: Vec<TaskId>,
    data: Vec<TaskId>,
    objective: String,
    verify: VerifySpec,
) -> PlanTaskV1 {
    PlanTaskV1 {
        id,
        kind: TaskKind::Implementation,
        objective,
        inputs: Vec::new(),
        scope: Vec::new(),
        out_of_scope: Vec::new(),
        depends_on: order,
        data_depends_on: data,
        outputs: vec![format!("output-{id}")],
        verify: Some(verify),
        evidence: Vec::new(),
        profile_id: None,
        permissions: Vec::new(),
        max_repair_rounds: 1,
        requires_approval: Vec::new(),
        handover: String::new(),
        integration: None,
    }
}

fn matching_profile() -> central_domain::ResolvedProfile {
    central_domain::ResolvedProfile {
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

fn hash_of(content: &str) -> ContentHash {
    ContentHash::from_hex(format!("{:x}", md5ish(content)))
}

// Stable non-crypto hex for test-only labels (never a plan/content hash).
fn md5ish(content: &str) -> u64 {
    let mut acc: u64 = 0xcbf29ce484222325;
    for byte in content.bytes() {
        acc ^= byte as u64;
        acc = acc.wrapping_mul(0x100000001b3);
    }
    acc
}

struct Harness {
    store: Arc<MemoryStore>,
    gateway: Arc<FakeGateway>,
    clock: Arc<FakeClock>,
    ids: Arc<FakeIds>,
    run_id: RunId,
    quality: QualityLoop,
}

impl Harness {
    fn new() -> Self {
        let shared = Arc::new(AtomicI64::new(1_700_000_000_000));
        let clock = Arc::new(FakeClock(shared));
        let gateway = Arc::new(FakeGateway::new());
        let store = Arc::new(MemoryStore::default());
        let ids = Arc::new(FakeIds::new());
        Self {
            store,
            gateway,
            clock,
            ids,
            run_id: RunId::from_u128(10),
            quality: QualityLoop::new(1),
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

    async fn execute(&self, actor: Actor, command: Command) {
        self.try_execute(actor, command)
            .await
            .expect("command commits");
    }

    async fn try_execute(&self, actor: Actor, command: Command) -> Result<Receipt, AppError> {
        let envelope = CommandEnvelope {
            id: self.ids.next_command_id(),
            expected_revision: None,
            command,
        };
        self.runtime().execute(actor, envelope).await
    }

    /// Plan h1: a and b independent, c needs a (order + data dependency).
    async fn approved_run(&self) -> (TaskId, TaskId, TaskId, PlanHash) {
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
            leader(),
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
        self.execute(
            user(),
            Command::ApprovePlan {
                plan_hash: hash.clone(),
            },
        )
        .await;
        (a, b, c, hash)
    }

    /// Queue one attempt for a task and drive it to Running. The queued
    /// input is the scheduler-rendered input so later binding
    /// comparisons see the production shape.
    async fn start_attempt(&self, task: TaskId) -> AttemptId {
        let attempt = self.ids.next_attempt_id();
        let state = self.load();
        let task_state = state.tasks.get(&task).cloned().expect("task");
        let input = central_application::scheduler::Scheduler::render_input(&state, &task_state);
        self.execute(
            system(),
            Command::QueueAttempt {
                attempt,
                task,
                input,
                resources: Vec::new(),
                max_parallel: 2,
            },
        )
        .await;
        self.execute(system(), Command::StartDispatch { attempt })
            .await;
        self.execute(
            system(),
            Command::AttemptRunning {
                attempt,
                session: AttemptSession {
                    provider: "fake".to_string(),
                    instance_epoch: "epoch-1".to_string(),
                    thread_id: "worker-1".to_string(),
                },
            },
        )
        .await;
        attempt
    }

    /// Park the worker's sealed candidate for verification. This is
    /// test scaffolding for the durable worker-done path the runtime
    /// does not own yet: the seal is registered the way production
    /// does, and the parked snapshot commits through the store CAS.
    async fn worker_done(&mut self, attempt: AttemptId, candidate: &ContentHash) {
        let mut state = self.load();
        let task_id = state.attempts.get(&attempt).expect("attempt").task_id;
        let task = state.tasks.get(&task_id).cloned().expect("task");
        let seal = SealedCandidate {
            task_id,
            input_hash: state
                .attempt_inputs
                .get(&attempt)
                .map(|input| central_application::artifacts::content_hash(input.as_bytes()))
                .unwrap_or_else(|| ContentHash::from_hex("")),
            candidate_hash: candidate.clone(),
            criteria_hash: app_criteria_hash(&task),
            max_repair_rounds: task.max_repair_rounds,
        };
        self.quality.note_worker_done(&mut state, seal);
        let expected = state.revision;
        let next_revision = expected.next();
        state.revision = next_revision;
        let command_id = self.ids.next_command_id();
        let payload_hash = ContentHash::from_hex("park-scaffolding");
        self.store
            .commit(CommitBatch {
                actor: user().id,
                command_id,
                payload_hash: payload_hash.clone(),
                expected_revision: expected,
                next_state: state,
                events: Vec::new(),
                intents: Vec::new(),
                receipt: Receipt {
                    command_id,
                    run_id: self.run_id,
                    revision: next_revision,
                    result: "applied".to_string(),
                    payload_hash,
                },
            })
            .await
            .expect("park commits");
    }

    fn report(
        &self,
        subject: &ContentHash,
        criteria: &ContentHash,
        outcome: VerificationOutcome,
    ) -> VerificationReport {
        VerificationReport {
            subject_hash: subject.clone(),
            criteria_hash: criteria.clone(),
            outcome,
            evidence_ids: vec!["evidence-1".to_string()],
            failed_criteria: Vec::new(),
            environment: "env-1".to_string(),
            check_refs: vec!["check-1".to_string()],
            verifier_thread: "verifier-1".to_string(),
            verifier_wrote_tracked_source: false,
            same_session_override: false,
            findings: "looks good".to_string(),
        }
    }

    /// Revision h2 over the approved h1: task a gets a new objective and
    /// stricter criteria (a data-relevant change); c follows untouched.
    fn revised_plan(&self, a: TaskId, b: TaskId, c: TaskId) -> PlanDocumentV1 {
        PlanDocumentV1 {
            version: 1,
            goal: "ship the DAG workflow".to_string(),
            tasks: vec![
                plan_task_with(
                    a,
                    vec![],
                    vec![],
                    format!("implement task {a} with the new output contract"),
                    verify_spec_v2(),
                ),
                plan_task(b, vec![], vec![]),
                plan_task(c, vec![a], vec![a]),
            ],
        }
    }

    fn load(&self) -> RunState {
        self.store.load_sync(self.run_id)
    }

    fn attempt(&self, id: AttemptId) -> Attempt {
        self.load().attempts.get(&id).cloned().expect("attempt")
    }

    fn stale(&self, id: AttemptId) -> StaleCandidate {
        self.load()
            .stale_candidates
            .get(&id)
            .cloned()
            .expect("stale candidate")
    }

    /// Current bindings for a quarantined candidate: the scheduler input
    /// rendered under the active plan, the same candidate bytes, the
    /// current criteria and the pinned environment the re-verification
    /// will run in.
    fn current_for(&self, attempt: AttemptId, environment: &str) -> ReuseBindings {
        let state = self.load();
        let candidate = state
            .stale_candidates
            .get(&attempt)
            .cloned()
            .expect("stale candidate");
        let task = state.tasks.get(&candidate.task_id).cloned().expect("task");
        current_bindings(&state, &task, &candidate, environment).expect("bindings")
    }
}

#[tokio::test]
async fn old_attempt_finishes_without_becoming_new_plan_success() {
    let h = Harness::new();
    let (a, _b, _c, h1) = h.approved_run().await;

    // The old attempt is running under h1 when the revision lands.
    let old = h.start_attempt(a).await;
    let h2_doc = h.revised_plan(a, TaskId::from_u128(2), TaskId::from_u128(3));
    let impact =
        assess_impact(&h.load().plans.get(&h1).expect("h1").document, &h2_doc).expect("impact");
    assert!(impact.changed.contains(&a));
    h.execute(
        leader(),
        Command::ProposeRevision {
            plan: h2_doc,
            reason: "the output contract changed".to_string(),
            criteria_explanation: Some("criteria gain a no-warnings rule".to_string()),
            semantic_affected: Vec::new(),
        },
    )
    .await;
    let h2 = h
        .load()
        .pending_revision
        .clone()
        .expect("pending")
        .plan_hash;
    h.execute(
        user(),
        Command::ApproveRevision {
            plan_hash: h2.clone(),
        },
    )
    .await;

    // The old verdict arrives after activation: the attempt completes on
    // its original binding, the task never becomes a new-plan success,
    // and the result waits for reuse assessment.
    let old_criteria = criteria_hash_for(&verify_spec());
    let report = h.report(
        &hash_of("candidate-a"),
        &old_criteria,
        VerificationOutcome::Pass,
    );
    h.execute(
        verifier(),
        Command::SubmitVerification {
            task: a,
            attempt: old,
            report,
        },
    )
    .await;

    let run = h.load();
    let old_attempt = run.attempts.get(&old).expect("old attempt");
    assert_eq!(old_attempt.bound_plan(), &h1);
    assert_eq!(old_attempt.status, AttemptStatus::Completed);
    assert_eq!(run.active_plan, Some(h2));
    assert_ne!(
        run.tasks.get(&a).expect("task a").status,
        TaskStatus::Succeeded
    );
    let old_candidate = run
        .stale_candidates
        .get(&old)
        .expect("quarantined candidate");
    assert_eq!(old_candidate.bound_plan, h1);
    assert_eq!(old_candidate.reuse, ReuseDecision::NeedsAssessment);
}

#[tokio::test]
async fn hold_only_impacted_queued_tasks() {
    use central_application::dispatcher::Dispatcher;

    let mut h = Harness::new();
    let (a, b, c, _h1) = h.approved_run().await;

    // a succeeds under h1 so c can queue; c waits queued while the
    // revision proposal pends. b stays independent throughout.
    let attempt_a = h.start_attempt(a).await;
    let subject_a = hash_of("candidate-a");
    h.worker_done(attempt_a, &subject_a).await;
    h.execute(
        verifier(),
        Command::SubmitVerification {
            task: a,
            attempt: attempt_a,
            report: h.report(
                &subject_a,
                &criteria_hash_for(&verify_spec()),
                VerificationOutcome::Pass,
            ),
        },
    )
    .await;
    let queued_c = h.ids.next_attempt_id();
    h.execute(
        system(),
        Command::QueueAttempt {
            attempt: queued_c,
            task: c,
            input: format!("implement task {c}"),
            resources: Vec::new(),
            max_parallel: 2,
        },
    )
    .await;

    let h2_doc = h.revised_plan(a, b, c);
    h.execute(
        leader(),
        Command::ProposeRevision {
            plan: h2_doc,
            reason: "the output contract changed".to_string(),
            criteria_explanation: Some("criteria gain a no-warnings rule".to_string()),
            semantic_affected: Vec::new(),
        },
    )
    .await;

    let run = h.load();
    assert!(run.revision_holds.contains(&a));
    assert!(run.revision_holds.contains(&c));
    assert!(!run.revision_holds.contains(&b));
    // The pending proposal invalidates nothing yet.
    assert_eq!(run.verified_outputs.get(&a), Some(&subject_a));
    // The independent task is still ready to queue.
    let b_task = run.tasks.get(&b).expect("task b");
    assert!(b_task.is_ready());
    let eligible = central_domain::eligible_tasks(&run, h.clock.now());
    assert!(eligible.contains(&b));
    assert!(!eligible.contains(&a));
    assert!(!eligible.contains(&c));
    // Held queued work is not dispatched: the dispatcher leaves it
    // queued without touching the gateway.
    let dispatcher = Dispatcher::new(
        h.store.clone() as Arc<dyn Store>,
        h.clock.clone() as Arc<dyn Clock>,
        h.ids.clone() as Arc<dyn IdSource>,
        h.gateway.clone() as Arc<dyn AgentGateway>,
        Arc::new(|_, _, _| Some(matching_profile())),
        BTreeMap::new(),
        system(),
    );
    let summary = dispatcher
        .dispatch_queued(h.run_id)
        .await
        .expect("dispatch");
    assert_eq!(summary.dispatched, 0);
    assert_eq!(summary.still_pending, 1);
    assert_eq!(h.gateway.sent.load(Ordering::SeqCst), 0);
    assert_eq!(h.attempt(queued_c).status, AttemptStatus::Queued);
    // Independent work still queues while the proposal pends.
    h.execute(
        system(),
        Command::QueueAttempt {
            attempt: h.ids.next_attempt_id(),
            task: b,
            input: format!("implement task {b}"),
            resources: Vec::new(),
            max_parallel: 2,
        },
    )
    .await;
}

#[tokio::test]
async fn criteria_change_needs_explanation() {
    let h = Harness::new();
    let (a, b, c, h1) = h.approved_run().await;
    let h2_doc = h.revised_plan(a, b, c);

    // The criteria impact lists what changed and what must re-verify.
    let impact = describe_criteria_impact(&h.load().plans.get(&h1).expect("h1").document, &h2_doc)
        .expect("criteria impact");
    assert_eq!(impact.changed_tasks, vec![a]);
    assert!(impact.reverify_tasks.contains(&a));
    assert!(impact.reverify_tasks.contains(&c));
    assert!(!impact.requirement_impact.trim().is_empty());
    assert!(!impact.verdict_impact.trim().is_empty());
    let text = criteria_explanation_text(&impact);
    assert!(text.contains(&a.to_string()));
    assert!(text.contains(&c.to_string()));

    // Missing explanation is a validation error, not a silent accept.
    let missing = h
        .try_execute(
            leader(),
            Command::ProposeRevision {
                plan: h2_doc.clone(),
                reason: "the output contract changed".to_string(),
                criteria_explanation: None,
                semantic_affected: Vec::new(),
            },
        )
        .await;
    assert!(
        matches!(missing, Err(AppError::Validation(_))),
        "criteria change without explanation must fail validation, got {missing:?}"
    );
    let blank = h
        .try_execute(
            leader(),
            Command::ProposeRevision {
                plan: h2_doc.clone(),
                reason: "the output contract changed".to_string(),
                criteria_explanation: Some("   ".to_string()),
                semantic_affected: Vec::new(),
            },
        )
        .await;
    assert!(
        matches!(blank, Err(AppError::Validation(_))),
        "blank explanation must fail validation, got {blank:?}"
    );

    // With the explanation the proposal commits.
    h.execute(
        leader(),
        Command::ProposeRevision {
            plan: h2_doc,
            reason: "the output contract changed".to_string(),
            criteria_explanation: Some(text),
            semantic_affected: Vec::new(),
        },
    )
    .await;
    assert!(h.load().pending_revision.is_some());
}

#[tokio::test]
async fn late_old_result_cannot_overwrite_active_revision() {
    let h = Harness::new();
    let (a, b, c, h1) = h.approved_run().await;

    // The old attempt runs under h1 and its result is quarantined when
    // h2 (new objective and criteria for a) activates.
    let old = h.start_attempt(a).await;
    h.execute(
        leader(),
        Command::ProposeRevision {
            plan: h.revised_plan(a, b, c),
            reason: "the output contract changed".to_string(),
            criteria_explanation: Some("criteria gain a no-warnings rule".to_string()),
            semantic_affected: Vec::new(),
        },
    )
    .await;
    let h2 = h
        .load()
        .pending_revision
        .clone()
        .expect("pending")
        .plan_hash;
    h.execute(
        user(),
        Command::ApproveRevision {
            plan_hash: h2.clone(),
        },
    )
    .await;

    let old_subject = hash_of("candidate-a");
    let old_criteria = criteria_hash_for(&verify_spec());
    h.execute(
        verifier(),
        Command::SubmitVerification {
            task: a,
            attempt: old,
            report: h.report(&old_subject, &old_criteria, VerificationOutcome::Pass),
        },
    )
    .await;
    assert_eq!(h.stale(old).reuse, ReuseDecision::NeedsAssessment);

    // Assessment routes the stale result to re-verification under the
    // current bindings; the verifier judges the same bytes again.
    let current = h.current_for(old, "env-1");
    assert_eq!(
        recommend_reuse(&h.load(), &h.stale(old), &current),
        ReuseDecision::VerifyAgain
    );
    h.execute(
        user(),
        Command::AssessReuse {
            attempt: old,
            decision: ReuseDecision::VerifyAgain,
            current: current.clone(),
        },
    )
    .await;
    assert!(reverify_first(&h.load()).contains(&a));
    let new_criteria = criteria_hash_for(&verify_spec_v2());
    h.execute(
        verifier(),
        Command::SubmitVerification {
            task: a,
            attempt: old,
            report: h.report(&old_subject, &new_criteria, VerificationOutcome::Pass),
        },
    )
    .await;
    let run = h.load();
    assert_eq!(
        run.tasks.get(&a).expect("task a").status,
        TaskStatus::Succeeded
    );
    assert_eq!(run.verified_outputs.get(&a), Some(&old_subject));

    // A late duplicate of the ORIGINAL verdict must not overwrite the
    // active revision: the task keeps its h2 acceptance with the original
    // provenance intact on the quarantined record.
    h.execute(
        verifier(),
        Command::SubmitVerification {
            task: a,
            attempt: old,
            report: h.report(&old_subject, &old_criteria, VerificationOutcome::Pass),
        },
    )
    .await;
    let run = h.load();
    assert_eq!(run.active_plan, Some(h2));
    assert_eq!(
        run.tasks.get(&a).expect("task a").status,
        TaskStatus::Succeeded
    );
    assert_eq!(run.verified_outputs.get(&a), Some(&old_subject));
    let kept = run.stale_candidates.get(&old).expect("candidate kept");
    assert_eq!(kept.bound_plan, h1);
    assert_eq!(kept.reuse, ReuseDecision::VerifyAgain);
}

#[tokio::test]
async fn order_only_change_keeps_data_evidence() {
    let mut h = Harness::new();
    let (a, b, c, h1) = h.approved_run().await;

    // a succeeds under h1 first: its verified output is the evidence an
    // order-only change must not invalidate.
    let attempt_a = h.start_attempt(a).await;
    let subject_a = hash_of("candidate-a");
    h.worker_done(attempt_a, &subject_a).await;
    h.execute(
        verifier(),
        Command::SubmitVerification {
            task: a,
            attempt: attempt_a,
            report: h.report(
                &subject_a,
                &criteria_hash_for(&verify_spec()),
                VerificationOutcome::Pass,
            ),
        },
    )
    .await;

    // h2 only adds an order dependency of c on b: no data input moves.
    let h2_doc = PlanDocumentV1 {
        version: 1,
        goal: "ship the DAG workflow".to_string(),
        tasks: vec![
            plan_task(a, vec![], vec![]),
            plan_task(b, vec![], vec![]),
            plan_task(c, vec![a, b], vec![a]),
        ],
    };
    let impact =
        assess_impact(&h.load().plans.get(&h1).expect("h1").document, &h2_doc).expect("impact");
    assert!(impact.changed.contains(&c));
    assert!(
        impact.data_descendants.is_empty(),
        "an order-only change has no data descendants, got {:?}",
        impact.data_descendants
    );
    // No criteria changed, so no explanation is required.
    h.execute(
        leader(),
        Command::ProposeRevision {
            plan: h2_doc,
            reason: "sequence c after b".to_string(),
            criteria_explanation: None,
            semantic_affected: Vec::new(),
        },
    )
    .await;
    let h2 = h
        .load()
        .pending_revision
        .clone()
        .expect("pending")
        .plan_hash;
    h.execute(user(), Command::ApproveRevision { plan_hash: h2 })
        .await;

    let run = h.load();
    assert_eq!(
        run.tasks.get(&a).expect("task a").status,
        TaskStatus::Succeeded
    );
    assert_eq!(run.verified_outputs.get(&a), Some(&subject_a));
}

#[tokio::test]
async fn leader_semantic_impact_only_expands() {
    let h = Harness::new();
    let (a, b, c, h1) = h.approved_run().await;
    let h1_doc = h.load().plans.get(&h1).expect("h1").document.clone();

    // Machine impact of the contract change: a changed, c follows data.
    let machine = assess_impact(&h1_doc, &h.revised_plan(a, b, c)).expect("impact");
    assert!(machine.changed.contains(&a));
    assert!(machine.data_descendants.contains(&c));
    assert!(!machine.impacted().contains(&b));

    // The leader adds a semantic concern on b. The merged impact grows
    // to cover b while every machine-known node stays held.
    let expanded = machine.expanded_with([b].into_iter().collect());
    assert!(expanded.impacted().is_superset(&machine.impacted()));
    assert!(expanded.impacted().contains(&b));
    assert_eq!(expanded.changed, machine.changed);
    assert_eq!(expanded.data_descendants, machine.data_descendants);

    // Expanding with nothing changes nothing: the merge never shrinks.
    let same = machine.expanded_with(Default::default());
    assert_eq!(same.impacted(), machine.impacted());

    // End to end: the proposal hold covers the union.
    h.execute(
        leader(),
        Command::ProposeRevision {
            plan: h.revised_plan(a, b, c),
            reason: "the output contract changed".to_string(),
            criteria_explanation: Some("criteria gain a no-warnings rule".to_string()),
            semantic_affected: vec![b],
        },
    )
    .await;
    let run = h.load();
    let pending = run.pending_revision.clone().expect("pending");
    assert!(pending.semantic_affected.contains(&b));
    assert!(run.revision_holds.is_superset(&machine.impacted()));
    assert!(run.revision_holds.contains(&b));
}

#[tokio::test]
async fn conflicting_revision_approval_is_conflict() {
    let h = Harness::new();
    let (a, b, c, _h1) = h.approved_run().await;
    h.execute(
        leader(),
        Command::ProposeRevision {
            plan: h.revised_plan(a, b, c),
            reason: "the output contract changed".to_string(),
            criteria_explanation: Some("criteria gain a no-warnings rule".to_string()),
            semantic_affected: Vec::new(),
        },
    )
    .await;

    // Approving anything but the pending revision hash conflicts: a
    // stale tab can never activate a revision the user did not review.
    let other = PlanHash::from_hex("0".repeat(64));
    let conflict = h
        .try_execute(user(), Command::ApproveRevision { plan_hash: other })
        .await;
    assert!(
        matches!(conflict, Err(AppError::Conflict(_))),
        "conflicting approval must conflict, got {conflict:?}"
    );
    assert!(h.load().pending_revision.is_some());

    // The envelope revision guards the same way: a stale tab retries
    // against a moved run revision and loses.
    let pending_hash = h
        .load()
        .pending_revision
        .clone()
        .expect("pending")
        .plan_hash;
    let stale_envelope = CommandEnvelope {
        id: h.ids.next_command_id(),
        expected_revision: Some(Revision(999_999)),
        command: Command::ApproveRevision {
            plan_hash: pending_hash,
        },
    };
    let stale = h.runtime().execute(user(), stale_envelope).await;
    assert!(
        matches!(stale, Err(AppError::Conflict(_))),
        "stale envelope revision must conflict, got {stale:?}"
    );
}

#[tokio::test]
async fn assess_reuse_records_decision_and_routes_followup() {
    let h = Harness::new();
    let (a, b, c, _h1) = h.approved_run().await;

    // Two old attempts under h1: one for the changed task a, one for
    // the independent task b.
    let old_a = h.start_attempt(a).await;
    let old_b = h.start_attempt(b).await;
    h.execute(
        leader(),
        Command::ProposeRevision {
            plan: h.revised_plan(a, b, c),
            reason: "the output contract changed".to_string(),
            criteria_explanation: Some("criteria gain a no-warnings rule".to_string()),
            semantic_affected: Vec::new(),
        },
    )
    .await;
    let h2 = h
        .load()
        .pending_revision
        .clone()
        .expect("pending")
        .plan_hash;
    h.execute(
        user(),
        Command::ApproveRevision {
            plan_hash: h2.clone(),
        },
    )
    .await;

    let subject_a = hash_of("candidate-a");
    let subject_b = hash_of("candidate-b");
    let old_criteria = criteria_hash_for(&verify_spec());
    for (attempt, subject) in [(old_a, &subject_a), (old_b, &subject_b)] {
        let task = h.load().attempts.get(&attempt).expect("attempt").task_id;
        h.execute(
            verifier(),
            Command::SubmitVerification {
                task,
                attempt,
                report: h.report(subject, &old_criteria, VerificationOutcome::Pass),
            },
        )
        .await;
    }

    // The pure matrix first: retained + failed -> Repair, removed ->
    // Discard, changed bindings -> VerifyAgain, identical -> reuse.
    let candidate_a = h.stale(old_a);
    let changed = h.current_for(old_a, "env-1");
    assert_eq!(
        assess_reuse(&candidate_a, &changed, true, false),
        ReuseDecision::VerifyAgain
    );
    assert_eq!(
        assess_reuse(&candidate_a, &changed, false, false),
        ReuseDecision::Discard
    );
    assert_eq!(
        assess_reuse(&candidate_a, &changed, true, true),
        ReuseDecision::Repair
    );
    let identical = ReuseBindings {
        input_hash: candidate_a.input_hash.clone(),
        candidate_hash: candidate_a.candidate_hash.clone(),
        criteria_hash: candidate_a.criteria_hash.clone(),
        environment: candidate_a.environment.clone(),
    };
    assert_eq!(
        assess_reuse(&candidate_a, &identical, true, false),
        ReuseDecision::ReuseUnchangedEvidence
    );

    // Repair on the changed task anchors a repair base instead of
    // silently re-implementing; the old binding is never relabeled.
    h.execute(
        user(),
        Command::AssessReuse {
            attempt: old_a,
            decision: ReuseDecision::Repair,
            current: changed.clone(),
        },
    )
    .await;
    let run = h.load();
    assert_eq!(
        run.stale_candidates.get(&old_a).expect("assessed").reuse,
        ReuseDecision::Repair
    );
    assert_eq!(
        run.repair_base.get(&a),
        Some(&old_a),
        "repair must chain from the assessed predecessor"
    );
    let repair = h.ids.next_attempt_id();
    h.execute(
        system(),
        Command::QueueRepairAttempt {
            attempt: repair,
            task: a,
            predecessor: old_a,
            input: "repair task a".to_string(),
            resources: Vec::new(),
            max_parallel: 2,
        },
    )
    .await;

    // The independent task is byte-identical under h2: its evidence is
    // reused without re-verification and the task succeeds again.
    let candidate_b = h.stale(old_b);
    let same_b = h.current_for(old_b, "env-1");
    assert_eq!(
        recommend_reuse(&h.load(), &candidate_b, &same_b),
        ReuseDecision::ReuseUnchangedEvidence
    );
    h.execute(
        user(),
        Command::AssessReuse {
            attempt: old_b,
            decision: ReuseDecision::ReuseUnchangedEvidence,
            current: same_b,
        },
    )
    .await;
    let run = h.load();
    assert_eq!(
        run.tasks.get(&b).expect("task b").status,
        TaskStatus::Succeeded
    );
    assert_eq!(run.verified_outputs.get(&b), Some(&subject_b));

    // Reuse with changed bindings is refused: stale evidence can only
    // go through verification again, never a silent accept.
    let bad = h
        .try_execute(
            user(),
            Command::AssessReuse {
                attempt: old_a,
                decision: ReuseDecision::ReuseUnchangedEvidence,
                current: changed,
            },
        )
        .await;
    assert!(
        matches!(bad, Err(AppError::Validation(_))),
        "unchanged-evidence reuse with changed bindings must fail, got {bad:?}"
    );
}

#[tokio::test]
async fn plan_change_schema_covers_revision_proposals() {
    let schema = include_str!("../../../contracts/plan-change-v1.schema.json");
    let value: serde_json::Value =
        serde_json::from_str(schema).expect("plan-change schema is valid JSON");
    for key in ["version", "basePlan", "plan", "reason", "semanticAffected"] {
        assert!(
            value
                .pointer("/required")
                .and_then(|required| required.as_array())
                .is_some_and(|required| required.iter().any(|entry| entry.as_str() == Some(key))),
            "plan-change schema must require {key}"
        );
    }
    let kinds = value
        .pointer("/$defs/planTask/properties/kind/enum")
        .and_then(|kinds| kinds.as_array())
        .expect("plan-change schema embeds the plan task kind enum");
    for kind in [
        "planning",
        "implementation",
        "verification",
        "integration",
        "assessment",
        "proposal",
        "approval",
    ] {
        assert!(
            kinds.iter().any(|entry| entry.as_str() == Some(kind)),
            "plan-change schema must accept task kind {kind}"
        );
    }
}

#[tokio::test]
async fn verify_again_reverdict_rejects_shifted_input() {
    let mut h = Harness::new();
    let (a, b, c, _h1) = h.approved_run().await;

    // c data-depends on a, so c's rendered input pins a's verified
    // output: a succeeds under h1 first, then the old c attempt starts
    // bound to h1.
    let attempt_a = h.start_attempt(a).await;
    let subject_a = hash_of("candidate-a");
    h.worker_done(attempt_a, &subject_a).await;
    h.execute(
        verifier(),
        Command::SubmitVerification {
            task: a,
            attempt: attempt_a,
            report: h.report(
                &subject_a,
                &criteria_hash_for(&verify_spec()),
                VerificationOutcome::Pass,
            ),
        },
    )
    .await;
    let old_c = h.start_attempt(c).await;

    // h2 changes a (objective + criteria): the old c result quarantines
    // and assesses to VerifyAgain under the h2 bindings.
    h.execute(
        leader(),
        Command::ProposeRevision {
            plan: h.revised_plan(a, b, c),
            reason: "the output contract changed".to_string(),
            criteria_explanation: Some("criteria gain a no-warnings rule".to_string()),
            semantic_affected: Vec::new(),
        },
    )
    .await;
    let h2 = h
        .load()
        .pending_revision
        .clone()
        .expect("pending")
        .plan_hash;
    h.execute(
        user(),
        Command::ApproveRevision {
            plan_hash: h2.clone(),
        },
    )
    .await;

    let subject_c = hash_of("candidate-c");
    let old_criteria = criteria_hash_for(&verify_spec());
    h.execute(
        verifier(),
        Command::SubmitVerification {
            task: c,
            attempt: old_c,
            report: h.report(&subject_c, &old_criteria, VerificationOutcome::Pass),
        },
    )
    .await;
    assert_eq!(h.stale(old_c).reuse, ReuseDecision::NeedsAssessment);

    let current = h.current_for(old_c, "env-1");
    assert_eq!(
        recommend_reuse(&h.load(), &h.stale(old_c), &current),
        ReuseDecision::VerifyAgain
    );
    h.execute(
        user(),
        Command::AssessReuse {
            attempt: old_c,
            decision: ReuseDecision::VerifyAgain,
            current: current.clone(),
        },
    )
    .await;

    // The input shifts after the assessment: h3 changes only c's
    // objective, so c's freshly rendered input no longer matches the
    // assessed bindings (criteria stay put, so the report itself is
    // still fresh).
    let h2_doc = h.load().plans.get(&h2).expect("h2").document.clone();
    let mut h3_tasks = h2_doc.tasks.clone();
    for task in &mut h3_tasks {
        if task.id == c {
            task.objective = format!("implement task {c} with a shifted input contract");
        }
    }
    let h3_doc = PlanDocumentV1 {
        version: 1,
        goal: h2_doc.goal.clone(),
        tasks: h3_tasks,
    };
    h.execute(
        leader(),
        Command::ProposeRevision {
            plan: h3_doc,
            reason: "c needs a wider input contract".to_string(),
            criteria_explanation: None,
            semantic_affected: Vec::new(),
        },
    )
    .await;
    let h3 = h
        .load()
        .pending_revision
        .clone()
        .expect("pending")
        .plan_hash;
    h.execute(
        user(),
        Command::ApproveRevision {
            plan_hash: h3.clone(),
        },
    )
    .await;

    // The re-verdict names the assessed bytes under the current criteria
    // and environment — but the input moved, so it must stay quarantined
    // instead of concluding success.
    let fresh_criteria = h.load().tasks.get(&c).expect("task c").verify.clone();
    let fresh_criteria_hash = criteria_hash_for(&fresh_criteria);
    h.execute(
        verifier(),
        Command::SubmitVerification {
            task: c,
            attempt: old_c,
            report: h.report(&subject_c, &fresh_criteria_hash, VerificationOutcome::Pass),
        },
    )
    .await;
    let run = h.load();
    assert_eq!(
        run.tasks.get(&c).expect("task c").status,
        TaskStatus::AwaitingVerification,
        "a re-verdict after an input shift must stay quarantined"
    );
    assert!(
        !run.verified_outputs.contains_key(&c),
        "a re-verdict after an input shift must not publish a verified output"
    );
    assert_eq!(h.stale(old_c).reuse, ReuseDecision::VerifyAgain);
}
