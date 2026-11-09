//! P2 Task 4: structured leader/result bridge and handover (TDD: RED first).
//!
//! Chat text never mutates a run; only schema-validated leader replies and
//! file imports do. Agent bridge tokens propose/report for one run/attempt
//! and can never approve.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use central_application::contracts::{
    AppError, CancelObservation, Capabilities, CommitBatch, DispatchError, ExecutionRef,
    ExternalAnswer, OutboxIntent, OutboxStatus, Receipt, ReconcileEvidence, SendTurn, SessionRef,
    TurnRef,
};
use central_application::handover::{
    accept_worker_result, build_handover, parse_verification, parse_worker_result, HandoverClaim,
    HandoverEvidence,
};
use central_application::leader::{AgentBridgeToken, ExecutionSegment, LeaderService};
use central_application::ports::{AgentGateway, Clock, IdSource, Store};
use central_application::Runtime;
use central_domain::{
    Actor, ActorId, ActorKind, Attempt, AttemptId, AttemptStatus, Command, CommandEnvelope,
    CommandId, IntentId, Observation, PlanDocumentV1, PlanHash, PlanTaskV1, Revision, RunId,
    RunState, TaskId, TaskKind, Timestamp, VerificationOutcome, VerifySpec,
};

// ---------- fakes (same shape as inbox_recovery.rs) ----------

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

struct FakeGateway;

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

#[derive(Default)]
struct MemoryStore {
    inner: Mutex<MemoryInner>,
}

#[derive(Default)]
struct MemoryInner {
    states: HashMap<RunId, RunState>,
    receipts: HashMap<(ActorId, CommandId), Receipt>,
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
        inner
            .receipts
            .insert((batch.actor, batch.command_id), batch.receipt.clone());
        Ok(batch.receipt)
    }
    async fn pending(&self, _id: RunId) -> Result<Vec<OutboxIntent>, AppError> {
        Ok(Vec::new())
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
    async fn outbox(&self, _id: RunId) -> Result<Vec<OutboxIntent>, AppError> {
        Ok(Vec::new())
    }
    async fn transition_outbox(
        &self,
        _id: IntentId,
        _expected: OutboxStatus,
        _next: OutboxStatus,
    ) -> Result<bool, AppError> {
        Ok(false)
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

// ---------- helpers ----------

fn user() -> Actor {
    Actor {
        id: ActorId::from_u128(100),
        kind: ActorKind::User,
    }
}

fn leader_actor() -> Actor {
    Actor {
        id: ActorId::from_u128(200),
        kind: ActorKind::Leader,
    }
}

fn run_id() -> RunId {
    RunId::from_u128(10)
}

fn setup() -> (Arc<MemoryStore>, LeaderService) {
    let store = Arc::new(MemoryStore::default());
    let runtime = Runtime::new(
        store.clone() as Arc<dyn Store>,
        Arc::new(FakeClock(AtomicI64::new(1_700_000_000_000))),
        Arc::new(FakeIds(AtomicU64::new(0))),
        Arc::new(FakeGateway) as Arc<dyn AgentGateway>,
        run_id(),
    );
    (store, LeaderService::new(runtime, leader_actor()))
}

async fn created_run(store: &Arc<MemoryStore>, service: &LeaderService) {
    service
        .runtime()
        .execute(
            user(),
            CommandEnvelope {
                id: CommandId::from_u128(900),
                expected_revision: None,
                command: Command::CreateRun {
                    goal: "bridge the leader".to_string(),
                    repository: "D:/Code/enolalab/central".to_string(),
                },
            },
        )
        .await
        .expect("create run");
    let _ = store;
}

fn fixture_plan() -> PlanDocumentV1 {
    PlanDocumentV1 {
        version: 1,
        goal: "fixture goal".to_string(),
        tasks: vec![PlanTaskV1 {
            id: TaskId::from_u128(1),
            kind: TaskKind::Implementation,
            objective: "do the thing".to_string(),
            inputs: Vec::new(),
            scope: vec!["src/".to_string()],
            out_of_scope: Vec::new(),
            depends_on: Vec::new(),
            data_depends_on: Vec::new(),
            outputs: vec!["done".to_string()],
            verify: Some(VerifySpec {
                method: "cargo test".to_string(),
                criteria: vec!["green".to_string()],
            }),
            evidence: Vec::new(),
            profile_id: None,
            permissions: Vec::new(),
            max_repair_rounds: 2,
            requires_approval: Vec::new(),
            handover: "none".to_string(),
        }],
    }
}

fn reply_json(plan: &PlanDocumentV1) -> String {
    serde_json::json!({
        "version": 1,
        "message": "proposing the fixture plan",
        "proposal": plan,
    })
    .to_string()
}

fn result_json(attempt: AttemptId) -> String {
    serde_json::json!({
        "version": 1,
        "attemptId": attempt.to_string(),
        "inputHash": "input-h1",
        "candidatePath": "out/candidate.txt",
        "candidateHash": "cc".repeat(32),
        "evidencePaths": [],
        "summary": "finished",
    })
    .to_string()
}

fn seed_attempt(store: &Arc<MemoryStore>, attempt: AttemptId) {
    let mut state = store
        .inner
        .lock()
        .expect("lock")
        .states
        .get(&run_id())
        .cloned()
        .expect("run exists");
    state.attempts.insert(
        attempt,
        Attempt {
            id: attempt,
            task_id: TaskId::from_u128(1),
            plan_hash: PlanHash::from_hex("h1"),
            status: AttemptStatus::Running,
            profile_id: None,
            created_at: Timestamp(1_700_000_000_000),
        },
    );
    store
        .inner
        .lock()
        .expect("lock")
        .states
        .insert(run_id(), state);
}

fn load(store: &Arc<MemoryStore>) -> RunState {
    store
        .inner
        .lock()
        .expect("lock")
        .states
        .get(&run_id())
        .cloned()
        .expect("run exists")
}

// ---------- the four brief-named tests ----------

#[tokio::test]
async fn ok_in_chat_does_not_approve() {
    let (store, service) = setup();
    created_run(&store, &service).await;

    // Plain chat: no mutation, no approval.
    let segment = service
        .message(run_id(), "ok, sounds good".to_string())
        .await
        .expect("chat is accepted as chat");
    assert!(matches!(segment, ExecutionSegment::Chat { .. }));

    // Markdown-fenced JSON is NOT scraped for state changes either.
    let fenced = format!(
        "here is the plan:\n```json\n{}\n```",
        reply_json(&fixture_plan())
    );
    let segment = service
        .message(run_id(), fenced)
        .await
        .expect("fenced text stays chat");
    assert!(matches!(segment, ExecutionSegment::Chat { .. }));

    let run = load(&store);
    assert_eq!(run.active_plan, None);
    assert_eq!(run.pending_plan, None);
}

#[tokio::test]
async fn proposal_needs_user_hash() {
    let (store, service) = setup();
    created_run(&store, &service).await;

    let plan = fixture_plan();
    let segment = service
        .message(run_id(), reply_json(&plan))
        .await
        .expect("schema-valid proposal is accepted");
    let hash = match segment {
        ExecutionSegment::Proposal { hash } => hash,
        ExecutionSegment::Chat { .. } => panic!("expected a proposal segment"),
    };

    // A proposal is pending, never active: only the user hash approves it.
    let run = load(&store);
    assert_eq!(run.active_plan, None);
    assert_eq!(run.pending_plan, Some(hash.clone()));

    // Approving the wrong hash conflicts; it never slides onto another revision.
    let wrong =
        PlanHash::from_hex("deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");
    let envelope = CommandEnvelope {
        id: CommandId::from_u128(911),
        expected_revision: None,
        command: Command::ApprovePlan { plan_hash: wrong },
    };
    let conflict = service.runtime().execute(user(), envelope).await;
    assert!(conflict.is_err());
    assert_eq!(load(&store).active_plan, None);

    // The user approving the exact hash activates the plan.
    let envelope = CommandEnvelope {
        id: CommandId::from_u128(912),
        expected_revision: None,
        command: Command::ApprovePlan { plan_hash: hash },
    };
    service
        .runtime()
        .execute(user(), envelope)
        .await
        .expect("user approves the exact hash");
    assert!(load(&store).active_plan.is_some());
}

#[tokio::test]
async fn handover_separates_claims() {
    let (store, service) = setup();
    created_run(&store, &service).await;
    let predecessor = AttemptId::from_u128(42);
    seed_attempt(&store, predecessor);

    let verified = HandoverClaim {
        text: "worker wrote src/main.rs".to_string(),
        evidence_id: Some("ev-1".to_string()),
    };
    let claim = "and it is definitely the fastest possible implementation".to_string();
    let unverified = HandoverClaim {
        text: claim.clone(),
        evidence_id: None,
    };
    let evidence = HandoverEvidence {
        observations: &[Observation {
            attempt_id: predecessor,
            provider_epoch: "epoch-1".to_string(),
            dedup_key: "ev-1".to_string(),
            outcome: Some(VerificationOutcome::Pass),
        }],
        artifact_hashes: &[],
    };
    let handover = build_handover(
        predecessor,
        "input-snapshot-h1".to_string(),
        vec![verified, unverified],
        Vec::new(),
        &evidence,
        "verify the candidate next".to_string(),
    );
    assert!(handover
        .verified_facts
        .iter()
        .any(|f| f.contains("src/main.rs")));
    assert!(handover.unverified_notes.contains(&claim));
    assert!(!handover.verified_facts.contains(&claim));
    let _ = service;
    let _ = store;
}

#[tokio::test]
async fn agent_token_cannot_approve() {
    let (store, service) = setup();
    created_run(&store, &service).await;

    // The token is scoped to propose/report for exactly this run/attempt.
    let attempt = AttemptId::from_u128(42);
    seed_attempt(&store, attempt);
    let token = AgentBridgeToken::proposal_only(run_id(), attempt);

    // A token approving a plan hash is refused with 403 and mutates nothing.
    let hash =
        PlanHash::from_hex("abcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd");
    let leader_approve = service.approve_with_token(&token, run_id(), &hash);
    assert_eq!(leader_approve.status, 403);
    let run = load(&store);
    assert_eq!(run.active_plan, None);
    assert_eq!(run.pending_plan, None);

    // A token minted for another run cannot propose here either.
    let foreign = AgentBridgeToken::proposal_only(RunId::from_u128(999), attempt);
    let denied = service
        .propose_with_token(&foreign, run_id(), reply_json(&fixture_plan()))
        .await;
    assert!(denied.is_err());
}

// ---------- schema rejection + foreign candidates ----------

#[tokio::test]
async fn malformed_and_unknown_schema_rejected() {
    let (store, service) = setup();
    created_run(&store, &service).await;

    // Truncated JSON that starts like a reply is malformed, not chat.
    let malformed = service
        .message(run_id(), "{\"version\": 1, \"message\": ".to_string())
        .await;
    assert!(malformed.is_err());

    // A well-formed reply under an unknown schema version is rejected.
    let unknown = serde_json::json!({
        "version": 99,
        "message": "from the future",
    })
    .to_string();
    let unknown = service.message(run_id(), unknown).await;
    assert!(unknown.is_err());

    // A worker result under an unknown version is rejected too.
    let bad_result = serde_json::json!({
        "version": 99,
        "attemptId": AttemptId::from_u128(42).to_string(),
        "inputHash": "h",
        "candidatePath": "out/a.txt",
        "candidateHash": "h",
        "evidencePaths": [],
        "summary": "x",
    })
    .to_string();
    assert!(parse_worker_result(&bad_result).is_err());

    // A verification with an unknown outcome is rejected.
    let bad_verification = serde_json::json!({
        "version": 1,
        "subjectHash": "h",
        "criteriaHash": "h",
        "outcome": "maybe",
        "evidenceIds": [],
        "findings": "x",
    })
    .to_string();
    assert!(parse_verification(&bad_verification).is_err());

    let run = load(&store);
    assert_eq!(run.active_plan, None);
    assert_eq!(run.pending_plan, None);
}

#[tokio::test]
async fn foreign_candidate_is_rejected() {
    let (store, service) = setup();
    created_run(&store, &service).await;
    seed_attempt(&store, AttemptId::from_u128(42));

    // A result naming an attempt from another run is refused.
    let foreign = serde_json::json!({
        "version": 1,
        "attemptId": AttemptId::from_u128(777).to_string(),
        "inputHash": "input-h1",
        "candidatePath": "out/candidate.txt",
        "candidateHash": "cc".repeat(32),
        "evidencePaths": [],
        "summary": "finished",
    })
    .to_string();
    let parsed = parse_worker_result(&foreign).expect("schema-valid shape");
    let foreign_candidate = accept_worker_result(&load(&store), &parsed);
    assert!(foreign_candidate.is_err());

    // An escaping candidate path is refused even for a live attempt.
    let escape = serde_json::json!({
        "version": 1,
        "attemptId": AttemptId::from_u128(42).to_string(),
        "inputHash": "input-h1",
        "candidatePath": "../elsewhere/evil.txt",
        "candidateHash": "cc".repeat(32),
        "evidencePaths": [],
        "summary": "finished",
    })
    .to_string();
    assert!(parse_worker_result(&escape).is_err());
    let _ = service;
}

#[tokio::test]
async fn report_token_bound_to_serving_run() {
    let (store, service) = setup();
    created_run(&store, &service).await;
    let attempt = AttemptId::from_u128(42);
    seed_attempt(&store, attempt);

    // The token matches the caller-supplied run, but this service instance
    // serves a different run: the report must be refused.
    let other = LeaderService::new(
        Runtime::new(
            store.clone() as Arc<dyn Store>,
            Arc::new(FakeClock(AtomicI64::new(1_700_000_000_000))),
            Arc::new(FakeIds(AtomicU64::new(0))),
            Arc::new(FakeGateway) as Arc<dyn AgentGateway>,
            RunId::from_u128(999),
        ),
        leader_actor(),
    );
    let token = AgentBridgeToken::report_only(run_id(), attempt);
    let rejected = other
        .report_with_token(&token, run_id(), &result_json(attempt))
        .await;
    assert!(
        rejected.is_err(),
        "report honored for a run this service does not serve"
    );
}

#[tokio::test]
async fn report_member_attempt_is_accepted() {
    let (store, service) = setup();
    created_run(&store, &service).await;
    let attempt = AttemptId::from_u128(42);
    seed_attempt(&store, attempt);

    let token = AgentBridgeToken::report_only(run_id(), attempt);
    let accepted = service
        .report_with_token(&token, run_id(), &result_json(attempt))
        .await
        .expect("member report is accepted");
    assert_eq!(accepted.status, 200);
}

#[tokio::test]
async fn report_unknown_attempt_is_rejected() {
    let (store, service) = setup();
    created_run(&store, &service).await;
    seed_attempt(&store, AttemptId::from_u128(42));

    // Schema-valid report, token-scoped attempt, but the attempt is absent
    // from the loaded run: must be rejected, never 200-accepted.
    let unknown = AttemptId::from_u128(777);
    let token = AgentBridgeToken::report_only(run_id(), unknown);
    let rejected = service
        .report_with_token(&token, run_id(), &result_json(unknown))
        .await;
    assert!(
        rejected.is_err(),
        "report for unknown attempt read as accepted"
    );
}
