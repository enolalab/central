//! P3 Task 3: shared budget ledger with classified usage (TDD: RED first).
//!
//! One ledger per run covers every role; live `InFlight` entries are
//! ongoing cost without a fabricated final, while `MissingFinal` (or an
//! unsupported metric without a cap) holds only the affected run when
//! that run carries token/money limits. Resume never clears the hold,
//! cumulative replays never double-count or reduce known totals, and a
//! user gap resolution is audited without converting estimated usage
//! into observed usage. No test sleeps: all time comes from the shared
//! virtual clock.

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use central_application::contracts::{
    AppError, CancelObservation, Capabilities, CommitBatch, DispatchError, ExecutionRef,
    ExternalAnswer, OutboxIntent, OutboxStatus, Receipt, ReconcileEvidence, SendTurn, SessionRef,
    TurnRef,
};
use central_application::ports::{AgentGateway, Clock, IdSource, Store};
use central_application::scheduler::{Scheduler, SchedulerConfig};
use central_application::Runtime;
use central_domain::{
    admission, Actor, ActorId, ActorKind, Admission, Attempt, AttemptId, AttemptStatus, Command,
    CommandEnvelope, CommandId, ContentHash, HoldReason, IntentId, Observation, PlanDocumentV1,
    PlanTaskV1, ResolvedProfile, Revision, RunId, RunState, TaskId, TaskKind, Timestamp,
    UsageFinality, UsageLimits, UsageObservation, VerifySpec, WorkRequest, WorkRole,
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

struct FakeGateway;

#[async_trait]
impl AgentGateway for FakeGateway {
    async fn capabilities(&self, profile: &ResolvedProfile) -> Result<Capabilities, AppError> {
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

fn token_limits(max_tokens: u64) -> UsageLimits {
    UsageLimits {
        max_tokens: Some(max_tokens),
        max_cost_minor: None,
        max_attempts: None,
        max_duration_ms: None,
        extra_caps: BTreeSet::new(),
        updated_by: None,
    }
}

fn obs(
    turn: &str,
    sequence: u64,
    finality: UsageFinality,
    cumulative_tokens: Option<u64>,
) -> UsageObservation {
    UsageObservation {
        provider: "fake".to_string(),
        epoch: "epoch-1".to_string(),
        turn: turn.to_string(),
        sequence,
        tokens: 0,
        cost_minor: None,
        cumulative_tokens,
        cumulative_cost_minor: None,
        finality,
        metric: "tokens".to_string(),
        source: "reported".to_string(),
    }
}

struct Harness {
    store: Arc<MemoryStore>,
    clock: Arc<FakeClock>,
    ids: Arc<FakeIds>,
    gateway: Arc<FakeGateway>,
}

impl Harness {
    fn new() -> Self {
        let clock = Arc::new(FakeClock(Arc::new(AtomicI64::new(1_700_000_000_000))));
        Self {
            store: Arc::new(MemoryStore::default()),
            clock,
            ids: Arc::new(FakeIds::new()),
            gateway: Arc::new(FakeGateway),
        }
    }

    fn runtime_for(&self, run: RunId) -> Runtime {
        Runtime::new(
            self.store.clone() as Arc<dyn Store>,
            self.clock.clone() as Arc<dyn Clock>,
            self.ids.clone() as Arc<dyn IdSource>,
            self.gateway.clone() as Arc<dyn AgentGateway>,
            run,
        )
    }

    fn scheduler(&self) -> Scheduler {
        Scheduler::new(
            self.store.clone() as Arc<dyn Store>,
            self.clock.clone() as Arc<dyn Clock>,
            self.ids.clone() as Arc<dyn IdSource>,
            self.gateway.clone() as Arc<dyn AgentGateway>,
            SchedulerConfig::default(),
            Arc::new(|_, _, _| Some(matching_profile())),
            system(),
        )
        .expect("valid scheduler config")
    }

    async fn execute_on(&self, run: RunId, actor: Actor, command: Command) {
        let envelope = CommandEnvelope {
            id: self.ids.next_command_id(),
            expected_revision: None,
            command,
        };
        self.runtime_for(run)
            .execute(actor, envelope)
            .await
            .expect("command commits");
    }

    async fn approved_run_on(&self, run: RunId, tasks: &[TaskId]) {
        self.execute_on(
            run,
            user(),
            Command::CreateRun {
                goal: "budgeted workflow".to_string(),
                repository: "repo".to_string(),
            },
        )
        .await;
        self.execute_on(
            run,
            user(),
            Command::ProposePlan {
                plan: PlanDocumentV1 {
                    version: 1,
                    goal: "budgeted workflow".to_string(),
                    tasks: tasks.iter().map(|id| plan_task(*id)).collect(),
                },
            },
        )
        .await;
        let hash = self.load(run).pending_plan.clone().expect("pending plan");
        self.execute_on(run, user(), Command::ApprovePlan { plan_hash: hash })
            .await;
    }

    fn load(&self, run: RunId) -> RunState {
        self.store.load_sync(run)
    }

    fn admission_of(&self, run: RunId) -> Admission {
        admission(
            &self.load(run),
            &WorkRequest {
                role: WorkRole::Worker,
            },
            self.clock.now(),
        )
    }

    fn attempts_of(&self, run: RunId) -> Vec<Attempt> {
        self.load(run).attempts.values().cloned().collect()
    }
}

// ---------- tests ----------

#[tokio::test]
async fn unknown_usage_holds_only_affected_run() {
    let harness = Harness::new();
    let run_a = RunId::from_u128(10);
    let run_b = RunId::from_u128(11);
    let a = TaskId::from_u128(1);
    let b = TaskId::from_u128(2);
    harness.approved_run_on(run_a, &[a]).await;
    harness.approved_run_on(run_b, &[b]).await;

    // Run A carries token limits and then loses a usage final.
    harness
        .execute_on(
            run_a,
            user(),
            Command::SetLimits {
                limits: token_limits(1_000),
            },
        )
        .await;
    harness
        .execute_on(
            run_a,
            user(),
            Command::ReconcileUsage {
                observation: obs("turn-7", 7, UsageFinality::MissingFinal, None),
            },
        )
        .await;

    let state_a = harness.load(run_a);
    assert!(
        state_a.usage.unknown,
        "missing final must flag usage unknown"
    );
    assert!(state_a.holds.contains(&HoldReason::UsageUnknown));

    let run_a_admission = harness.admission_of(run_a);
    assert_eq!(run_a_admission, Admission::Held(HoldReason::UsageUnknown));
    let run_b_admission = harness.admission_of(run_b);
    assert_eq!(run_b_admission, Admission::Allowed);

    // The gate holds A (running attempts untouched, nothing new queues)
    // while B still dispatches.
    let report_a = harness
        .scheduler()
        .tick(run_a)
        .await
        .expect("tick held run");
    assert!(report_a.queued.is_empty());
    assert_eq!(report_a.sent, 0);
    let report_b = harness
        .scheduler()
        .tick(run_b)
        .await
        .expect("tick free run");
    assert_eq!(report_b.queued, vec![b]);
    assert_eq!(report_b.sent, 1);
    assert_eq!(harness.attempts_of(run_b).len(), 1);
    assert!(harness.attempts_of(run_a).is_empty());
}

#[tokio::test]
async fn resume_does_not_clear_unknown() {
    let harness = Harness::new();
    let run_a = RunId::from_u128(10);
    let a = TaskId::from_u128(1);
    harness.approved_run_on(run_a, &[a]).await;
    harness
        .execute_on(
            run_a,
            user(),
            Command::SetLimits {
                limits: token_limits(1_000),
            },
        )
        .await;
    harness
        .execute_on(
            run_a,
            user(),
            Command::ReconcileUsage {
                observation: obs("turn-7", 7, UsageFinality::MissingFinal, None),
            },
        )
        .await;
    assert!(harness.load(run_a).usage.unknown);

    harness.execute_on(run_a, user(), Command::Pause).await;
    harness.execute_on(run_a, user(), Command::Resume).await;

    // Resume reopens the restart barrier but never clears usage holds.
    let state = harness.load(run_a);
    assert!(state.usage.unknown, "resume must not clear unknown usage");
    assert!(state.holds.contains(&HoldReason::UsageUnknown));
    assert_eq!(
        harness.admission_of(run_a),
        Admission::Held(HoldReason::UsageUnknown)
    );
    let report = harness.scheduler().tick(run_a).await.expect("tick");
    assert!(report.queued.is_empty());
    assert_eq!(report.sent, 0);
}

#[tokio::test]
async fn cumulative_usage_not_double_counted() {
    let harness = Harness::new();
    let run_a = RunId::from_u128(10);
    let a = TaskId::from_u128(1);
    harness.approved_run_on(run_a, &[a]).await;

    // First cumulative report: 120 tokens observed.
    harness
        .execute_on(
            run_a,
            user(),
            Command::ReconcileUsage {
                observation: obs("turn-1", 1, UsageFinality::Observed, Some(120)),
            },
        )
        .await;
    // Exact replay of the same event: dedup, no double count.
    harness
        .execute_on(
            run_a,
            user(),
            Command::ReconcileUsage {
                observation: obs("turn-1", 1, UsageFinality::Observed, Some(120)),
            },
        )
        .await;
    // Older cumulative arriving late: never reduces the known total.
    harness
        .execute_on(
            run_a,
            user(),
            Command::ReconcileUsage {
                observation: obs("turn-2", 2, UsageFinality::Observed, Some(100)),
            },
        )
        .await;

    let state = harness.load(run_a);
    let observed_tokens = state.usage.observed_tokens;
    assert_eq!(observed_tokens, 120);
    assert_eq!(state.usage.total_tokens, 120);

    // A missing final followed by a user gap resolution: audited, but the
    // observed total is untouched and nothing estimated becomes observed.
    let missing = obs("turn-9", 9, UsageFinality::MissingFinal, None);
    let missing_key = missing.dedup_key();
    harness
        .execute_on(
            run_a,
            user(),
            Command::ReconcileUsage {
                observation: missing,
            },
        )
        .await;
    assert!(harness.load(run_a).usage.unknown);
    harness
        .execute_on(
            run_a,
            user(),
            Command::ResolveUsageGap {
                missing_key: missing_key.clone(),
                note: "provider never reported turn-9; capped manually".to_string(),
            },
        )
        .await;
    let state = harness.load(run_a);
    assert_eq!(state.usage.observed_tokens, 120);
    assert!(!state.usage.unknown, "audited gap must release the hold");
    assert_eq!(state.usage.audit.len(), 1);
    assert_eq!(state.usage.audit[0].missing_key, missing_key);
    assert!(
        !state.usage.audit[0].resolved_observed,
        "gap resolution must never convert estimated/missing into observed"
    );

    // Cached reads are never added to the ledger at all.
    let mut cached = obs("turn-3", 3, UsageFinality::Observed, Some(500));
    cached.source = "cache".to_string();
    harness
        .execute_on(
            run_a,
            user(),
            Command::ReconcileUsage {
                observation: cached,
            },
        )
        .await;
    assert_eq!(harness.load(run_a).usage.observed_tokens, 120);
}

#[tokio::test]
async fn terminal_attempts_do_not_block_recovery_queue() {
    // A tight `max_attempts` cap hit only by terminal attempts must not
    // make the reopen recoverability guarantee illusory: after the
    // failed attempt parks and the task reopens, the follow-up queue
    // succeeds because the cap counts live attempts, not history.
    let harness = Harness::new();
    let run = RunId::from_u128(10);
    let task = TaskId::from_u128(1);
    harness.approved_run_on(run, &[task]).await;
    harness
        .execute_on(
            run,
            user(),
            Command::SetLimits {
                limits: UsageLimits {
                    max_tokens: None,
                    max_cost_minor: None,
                    max_attempts: Some(1),
                    max_duration_ms: None,
                    extra_caps: BTreeSet::new(),
                    updated_by: None,
                },
            },
        )
        .await;

    // Queue the one live attempt the cap allows, then fail it.
    let first = harness.ids.next_attempt_id();
    harness
        .execute_on(
            run,
            system(),
            Command::QueueAttempt {
                attempt: first,
                task,
                input: "implement task".to_string(),
                resources: Vec::new(),
                max_parallel: 2,
            },
        )
        .await;
    harness
        .execute_on(run, system(), Command::StartDispatch { attempt: first })
        .await;
    harness
        .execute_on(run, system(), Command::AttemptFailed { attempt: first })
        .await;
    assert_eq!(
        harness
            .load(run)
            .attempts
            .get(&first)
            .expect("attempt")
            .status,
        AttemptStatus::Failed
    );

    // Reopen, then queue the recovery attempt: the gate must let it
    // through even though one terminal attempt exists under a cap of 1.
    harness
        .execute_on(run, user(), Command::ReopenTask { task })
        .await;
    let second = harness.ids.next_attempt_id();
    harness
        .execute_on(
            run,
            system(),
            Command::QueueAttempt {
                attempt: second,
                task,
                input: "implement task".to_string(),
                resources: Vec::new(),
                max_parallel: 2,
            },
        )
        .await;
    let attempts = harness.attempts_of(run);
    assert_eq!(attempts.len(), 2);
    assert_eq!(
        harness
            .load(run)
            .attempts
            .get(&second)
            .expect("attempt")
            .status,
        AttemptStatus::Queued
    );
    // The recovery attempt itself is live, so it now occupies the single
    // slot and anything further holds. The fix is that terminal history
    // alone never blocks the recovery queue.
    assert_eq!(
        harness.admission_of(run),
        Admission::Held(HoldReason::BudgetExceeded)
    );
}
