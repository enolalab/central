//! P3 Task 2: bounded verification rounds and leader escalation.
//!
//! A worker finishing never marks a task `Succeeded`. A verifier verdict
//! bound to the exact sealed input/candidate/criteria routes to one
//! [`QualityDecision`](central_domain::quality::QualityDecision): two
//! quality-repair rounds, then escalation to the leader as a scheduled,
//! budgeted work item — never a gate bypass and never a direct model call.
//! `Blocked` (environment) never consumes a quality round. The leader can
//! never force a pass. A dispute opens one independent re-review only
//! inside the approved allowance, otherwise it routes to the user; there
//! are no verifier-of-verifier chains.

use std::collections::BTreeMap;

use central_application::handover::{VerificationV1, WorkerResultV1};
use central_application::quality_loop::{
    route_verification, DisputePlan, QualityLoop, VerifierContext,
};
use central_application::verification::{
    criteria_hash, seal_candidate, SealedCandidate, VerificationService,
};
use central_domain::quality::{decide_quality, QualityDecision};
use central_domain::{
    decide, Actor, ActorKind, Attempt, AttemptId, AttemptSession, AttemptStatus, Command,
    ContentHash, PlanHash, RunControl, RunId, RunState, Task, TaskId, TaskKind, TaskStatus,
    Timestamp, VerificationOutcome, VerifySpec,
};

fn hash(hex: &str) -> ContentHash {
    ContentHash::from_hex(hex.to_string())
}

fn input_hash() -> ContentHash {
    hash("1111111111111111111111111111111111111111111111111111111111111111")
}

fn candidate_hash() -> ContentHash {
    hash("2222222222222222222222222222222222222222222222222222222222222222")
}

fn other_hash() -> ContentHash {
    hash("3333333333333333333333333333333333333333333333333333333333333333")
}

fn verifier() -> Actor {
    Actor {
        id: central_domain::ActorId::from_u128(701),
        kind: ActorKind::Verifier,
    }
}

fn leader() -> Actor {
    Actor {
        id: central_domain::ActorId::from_u128(702),
        kind: ActorKind::Leader,
    }
}

fn user() -> Actor {
    Actor {
        id: central_domain::ActorId::from_u128(703),
        kind: ActorKind::User,
    }
}

fn system() -> Actor {
    Actor {
        id: central_domain::ActorId::from_u128(704),
        kind: ActorKind::System,
    }
}

fn verify_spec() -> VerifySpec {
    VerifySpec {
        method: "run the approved check".to_string(),
        criteria: vec!["the approved check exits zero".to_string()],
    }
}

fn plan_hash() -> PlanHash {
    PlanHash::from_hex("plan QUALITY-LOOP")
}

/// One implementation task parked in `AwaitingVerification` with a
/// completed worker attempt bound to a harness session.
fn build_state() -> (RunState, TaskId, AttemptId) {
    let run = RunId::from_u128(7_001);
    let task_id = TaskId::from_u128(7_101);
    let attempt_id = AttemptId::from_u128(7_201);
    let plan = plan_hash();
    let mut state = RunState::new(run);
    state.created = true;
    state.control = RunControl::Running;
    state.active_plan = Some(plan.clone());
    state.tasks.insert(
        task_id,
        Task {
            id: task_id,
            plan_hash: plan.clone(),
            kind: TaskKind::Implementation,
            objective: "implement the quality loop fixture".to_string(),
            status: TaskStatus::AwaitingVerification,
            depends_on: Vec::new(),
            data_depends_on: Vec::new(),
            verify: verify_spec(),
            profile_id: None,
            max_repair_rounds: 2,
            repair_rounds_used: 0,
        },
    );
    state.attempts.insert(
        attempt_id,
        Attempt {
            id: attempt_id,
            task_id,
            plan_hash: plan.clone(),
            status: AttemptStatus::Completed,
            profile_id: None,
            created_at: Timestamp::from_millis(0),
        },
    );
    state.attempt_sessions.insert(
        attempt_id,
        AttemptSession {
            provider: "fake".to_string(),
            instance_epoch: "epoch-1".to_string(),
            thread_id: "worker-thread-a".to_string(),
        },
    );
    (state, task_id, attempt_id)
}

fn worker_result(attempt_id: AttemptId, candidate: ContentHash) -> WorkerResultV1 {
    WorkerResultV1 {
        version: 1,
        attempt_id,
        input_hash: input_hash(),
        candidate_path: "candidate/patch.bin".to_string(),
        candidate_hash: candidate,
        evidence_paths: Vec::new(),
        summary: "worker finished the fixture".to_string(),
    }
}

fn seal(state: &RunState, attempt: AttemptId, candidate: ContentHash) -> SealedCandidate {
    seal_candidate(state, &worker_result(attempt, candidate)).expect("seal")
}

fn fail_verdict(criteria: ContentHash) -> VerificationV1 {
    VerificationV1 {
        version: 1,
        subject_hash: candidate_hash(),
        criteria_hash: criteria,
        outcome: VerificationOutcome::Fail,
        evidence_ids: vec![input_hash().as_str().to_string()],
        findings: "the approved check failed on criterion one".to_string(),
    }
}

fn pass_verdict(criteria: ContentHash) -> VerificationV1 {
    VerificationV1 {
        version: 1,
        subject_hash: candidate_hash(),
        criteria_hash: criteria,
        outcome: VerificationOutcome::Pass,
        evidence_ids: vec![input_hash().as_str().to_string()],
        findings: "the approved check exits zero on a clean scratch".to_string(),
    }
}

fn blocked_verdict(criteria: ContentHash) -> VerificationV1 {
    VerificationV1 {
        version: 1,
        subject_hash: candidate_hash(),
        criteria_hash: criteria,
        outcome: VerificationOutcome::Blocked,
        evidence_ids: vec![input_hash().as_str().to_string()],
        findings: "the check sandbox is unreachable; environment failure".to_string(),
    }
}

fn ctx(thread: &str, failed: Vec<String>) -> VerifierContext {
    VerifierContext {
        verifier_thread: thread.to_string(),
        environment: "fixture-check-env".to_string(),
        check_refs: vec!["check:approved".to_string()],
        failed_criteria: failed,
        verifier_wrote_tracked_source: false,
        tainted_evidence: Vec::new(),
        same_session_override: false,
    }
}

fn fail_ctx(thread: &str) -> VerifierContext {
    ctx(thread, vec!["the approved check exits zero".to_string()])
}

#[allow(clippy::too_many_arguments)]
fn submit(
    quality: &QualityLoop,
    state: &RunState,
    actor: &Actor,
    task: TaskId,
    attempt: AttemptId,
    verdict: &VerificationV1,
    context: &VerifierContext,
    now: i64,
) -> Result<RunState, central_domain::DomainError> {
    let report = quality
        .build_report(state, task, verdict, context)
        .expect("report builds");
    let decision = decide(
        state,
        actor,
        &Command::SubmitVerification {
            task,
            attempt,
            report,
        },
        Timestamp::from_millis(now),
    )?;
    Ok(decision.next)
}

#[test]
fn two_repairs_then_leader() {
    let (mut state, task, attempt) = build_state();
    let criteria = criteria_hash(state.tasks.get(&task).expect("task"));
    let mut quality = QualityLoop::new(1);
    let reseal = seal(&state, attempt, candidate_hash());
    quality.note_worker_done(&mut state, reseal);
    let mut routes = Vec::new();
    for now in [1, 2, 3] {
        let verdict = fail_verdict(criteria.clone());
        routes.push(route_verification(&state, &verdict).expect("routes"));
        state = submit(
            &quality,
            &state,
            &verifier(),
            task,
            attempt,
            &verdict,
            &fail_ctx("verifier-1"),
            now,
        )
        .expect("submit");
    }
    assert_eq!(
        routes,
        vec![
            QualityDecision::Repair { round: 1 },
            QualityDecision::Repair { round: 2 },
            QualityDecision::AskLeader,
        ]
    );
    assert_eq!(state.tasks.get(&task).expect("task").repair_rounds_used, 2);
    // Escalation parks for leader analysis; it never flips to Succeeded.
    assert_eq!(
        state.tasks.get(&task).expect("task").status,
        TaskStatus::AwaitingVerification
    );
}

#[test]
fn blocked_does_not_consume_round() {
    let (mut state, task, attempt) = build_state();
    let criteria = criteria_hash(state.tasks.get(&task).expect("task"));
    let verdict = blocked_verdict(criteria);
    assert_eq!(
        route_verification(&state, &verdict).expect("routes"),
        QualityDecision::Blocked
    );
    let mut quality = QualityLoop::new(1);
    let reseal = seal(&state, attempt, candidate_hash());
    quality.note_worker_done(&mut state, reseal);
    let after_blocked = submit(
        &quality,
        &state,
        &verifier(),
        task,
        attempt,
        &verdict,
        &ctx("verifier-1", Vec::new()),
        1,
    )
    .expect("blocked submits");
    assert_eq!(
        after_blocked
            .tasks
            .get(&task)
            .expect("task")
            .repair_rounds_used,
        0
    );
    assert_eq!(
        after_blocked.tasks.get(&task).expect("task").status,
        TaskStatus::Blocked
    );
    // A blocked verdict is not a quality fail: the next fail still gets round 1.
    assert_eq!(
        decide_quality(
            after_blocked.tasks.get(&task).expect("task"),
            VerificationOutcome::Fail
        ),
        QualityDecision::Repair { round: 1 }
    );
}

#[test]
fn leader_cannot_override_fail() {
    let (mut state, task, attempt) = build_state();
    let criteria = criteria_hash(state.tasks.get(&task).expect("task"));
    let verdict = pass_verdict(criteria);
    let mut loop_service = QualityLoop::new(1);
    let reseal = seal(&state, attempt, candidate_hash());
    loop_service.note_worker_done(&mut state, reseal);
    let report = loop_service
        .build_report(&state, task, &verdict, &ctx("verifier-1", Vec::new()))
        .expect("report builds");
    let leader_force_pass = decide(
        &state,
        &leader(),
        &Command::SubmitVerification {
            task,
            attempt,
            report,
        },
        Timestamp::from_millis(1),
    );
    assert_eq!(
        leader_force_pass.map(|_| ()),
        Err(central_domain::DomainError::Forbidden)
    );
}

#[test]
fn stale_evidence_cannot_pass() {
    // Part 1: the candidate changed after a pass — the old acceptance is
    // stale and the task is no longer Succeeded.
    let (mut state, task, attempt) = build_state();
    let criteria = criteria_hash(state.tasks.get(&task).expect("task"));
    let mut quality = QualityLoop::new(1);
    let reseal = seal(&state, attempt, candidate_hash());
    quality.note_worker_done(&mut state, reseal);
    let pass = pass_verdict(criteria.clone());
    state = submit(
        &quality,
        &state,
        &verifier(),
        task,
        attempt,
        &pass,
        &ctx("verifier-1", Vec::new()),
        1,
    )
    .expect("pass submits");
    assert_eq!(
        state.tasks.get(&task).expect("task").status,
        TaskStatus::Succeeded
    );
    // A new worker candidate seals under a different hash: the task reopens
    // for verification instead of staying Succeeded on the old bytes.
    let reseal = seal(&state, attempt, other_hash());
    quality.note_worker_done(&mut state, reseal);
    let stale_task = state.tasks.get(&task).expect("task").clone();
    assert_ne!(stale_task.status, TaskStatus::Succeeded);
    assert!(!state.verified_outputs.contains_key(&task));

    // Part 2: a verifier that edited tracked source produces invalid
    // evidence, never a pass.
    let (mut state, task, attempt) = build_state();
    let mut quality = QualityLoop::new(1);
    let reseal = seal(&state, attempt, candidate_hash());
    quality.note_worker_done(&mut state, reseal);
    let tainted = ctx("verifier-1", Vec::new());
    let tainted = VerifierContext {
        verifier_wrote_tracked_source: true,
        ..tainted
    };
    let report = quality.build_report(&state, task, &pass_verdict(criteria), &tainted);
    let error = report.expect_err("tracked-source edits are invalid evidence");
    assert!(
        error.to_string().contains("invalid evidence"),
        "unexpected error: {error}"
    );
}

#[test]
fn same_session_verifier_rejected_unless_user_override() {
    let (mut state, task, attempt) = build_state();
    let criteria = criteria_hash(state.tasks.get(&task).expect("task"));
    let verdict = fail_verdict(criteria);
    let mut quality = QualityLoop::new(1);
    let reseal = seal(&state, attempt, candidate_hash());
    quality.note_worker_done(&mut state, reseal);
    // The verifier runs in the worker's own session: refused.
    let same_session = submit(
        &quality,
        &state,
        &verifier(),
        task,
        attempt,
        &verdict,
        &fail_ctx("worker-thread-a"),
        1,
    );
    assert_eq!(
        same_session.map(|_| ()),
        Err(central_domain::DomainError::Forbidden)
    );
    // A per-task user override admits exactly that session, once.
    let override_ctx = VerifierContext {
        same_session_override: true,
        ..fail_ctx("worker-thread-a")
    };
    let report = quality
        .build_report(&state, task, &verdict, &override_ctx)
        .expect("override report builds");
    // Only the user may carry an override; a verifier cannot self-approve it.
    let smuggled = decide(
        &state,
        &verifier(),
        &Command::SubmitVerification {
            task,
            attempt,
            report: report.clone(),
        },
        Timestamp::from_millis(1),
    );
    assert_eq!(
        smuggled.map(|_| ()),
        Err(central_domain::DomainError::Forbidden)
    );
    let admitted = decide(
        &state,
        &user(),
        &Command::SubmitVerification {
            task,
            attempt,
            report,
        },
        Timestamp::from_millis(1),
    )
    .expect("user override admits the session");
    assert_eq!(
        admitted
            .next
            .tasks
            .get(&task)
            .expect("task")
            .repair_rounds_used,
        1
    );
}

#[test]
fn fail_repair_pass_opens_downstream_once() {
    // Task c takes a data dependency on a: it opens only after a's exact
    // sealed candidate passes, and exactly once.
    let (mut state, task_a, attempt_a) = build_state();
    let task_c = TaskId::from_u128(7_102);
    state.tasks.insert(
        task_c,
        Task {
            id: task_c,
            plan_hash: plan_hash(),
            kind: TaskKind::Implementation,
            objective: "consume the verified output".to_string(),
            status: TaskStatus::Pending,
            depends_on: Vec::new(),
            data_depends_on: vec![task_a],
            verify: verify_spec(),
            profile_id: None,
            max_repair_rounds: 2,
            repair_rounds_used: 0,
        },
    );
    assert!(!central_domain::eligible_tasks(&state, Timestamp::from_millis(0)).contains(&task_c));
    let criteria_a = criteria_hash(state.tasks.get(&task_a).expect("task"));
    let mut quality = QualityLoop::new(1);
    let reseal = seal(&state, attempt_a, candidate_hash());
    quality.note_worker_done(&mut state, reseal);
    // fail -> repair -> pass, the positive fixture path.
    state = submit(
        &quality,
        &state,
        &verifier(),
        task_a,
        attempt_a,
        &fail_verdict(criteria_a.clone()),
        &fail_ctx("verifier-1"),
        1,
    )
    .expect("fail submits");
    let repair_id = AttemptId::from_u128(7_202);
    let repair = decide(
        &state,
        &system(),
        &Command::QueueRepairAttempt {
            attempt: repair_id,
            task: task_a,
            predecessor: attempt_a,
            input: "repair with the verifier findings".to_string(),
            resources: Vec::new(),
            max_parallel: 2,
        },
        Timestamp::from_millis(2),
    )
    .expect("repair queues");
    state = repair.next;
    // The repair attempt finishes and seals a new candidate under the same
    // task; the task returns to verification before it can pass.
    state.attempts.get_mut(&repair_id).expect("repair").status = AttemptStatus::Completed;
    state.tasks.get_mut(&task_a).expect("task").status = TaskStatus::AwaitingVerification;
    state = submit(
        &quality,
        &state,
        &verifier(),
        task_a,
        repair_id,
        &pass_verdict(criteria_a),
        &ctx("verifier-2", Vec::new()),
        3,
    )
    .expect("pass submits");
    assert_eq!(
        state.tasks.get(&task_a).expect("task").status,
        TaskStatus::Succeeded
    );
    // Downstream opens exactly once: one queue succeeds, a second is refused
    // because the task is already covered.
    assert!(central_domain::eligible_tasks(&state, Timestamp::from_millis(4)).contains(&task_c));
    let first = decide(
        &state,
        &system(),
        &Command::QueueAttempt {
            attempt: AttemptId::from_u128(7_203),
            task: task_c,
            input: "implement task c".to_string(),
            resources: Vec::new(),
            max_parallel: 2,
        },
        Timestamp::from_millis(4),
    )
    .expect("first downstream queue");
    state = first.next;
    let second = decide(
        &state,
        &system(),
        &Command::QueueAttempt {
            attempt: AttemptId::from_u128(7_204),
            task: task_c,
            input: "implement task c".to_string(),
            resources: Vec::new(),
            max_parallel: 2,
        },
        Timestamp::from_millis(5),
    );
    assert!(second.is_err(), "downstream must open exactly once");
    assert_eq!(
        state
            .attempts
            .values()
            .filter(|attempt| attempt.task_id == task_c)
            .count(),
        1
    );
}

#[test]
fn identical_seals_for_two_tasks_do_not_collide() {
    // Regression for the ledger carry-over: seals keyed by candidate hash
    // alone collide when two tasks seal identical bytes. Seals are keyed
    // per (TaskId, ContentHash); each task judges its own seal.
    let (state, task_a, attempt_a) = build_state();
    let task_b = TaskId::from_u128(7_111);
    let attempt_b = AttemptId::from_u128(7_211);
    let mut state = state;
    state.tasks.insert(
        task_b,
        Task {
            id: task_b,
            plan_hash: plan_hash(),
            kind: TaskKind::Implementation,
            objective: "second task sealing identical bytes".to_string(),
            status: TaskStatus::AwaitingVerification,
            depends_on: Vec::new(),
            data_depends_on: Vec::new(),
            verify: verify_spec(),
            profile_id: None,
            max_repair_rounds: 2,
            repair_rounds_used: 0,
        },
    );
    state.attempts.insert(
        attempt_b,
        Attempt {
            id: attempt_b,
            task_id: task_b,
            plan_hash: plan_hash(),
            status: AttemptStatus::Completed,
            profile_id: None,
            created_at: Timestamp::from_millis(0),
        },
    );
    let criteria = criteria_hash(state.tasks.get(&task_a).expect("task"));
    let mut service = VerificationService::new();
    // Identical bytes sealed for two different tasks: two seals, no collision.
    service.note_worker_done(seal(&state, attempt_a, candidate_hash()));
    service.note_worker_done(seal(&state, attempt_b, candidate_hash()));
    let verdict = pass_verdict(criteria);
    let receipt_a = service
        .accept_for(task_a, &verdict)
        .expect("task a judges its seal");
    assert_eq!(receipt_a.task_id, task_a);
    assert_eq!(receipt_a.candidate_hash, candidate_hash());
    let receipt_b = service
        .accept_for(task_b, &verdict)
        .expect("task b judges its seal");
    assert_eq!(receipt_b.task_id, task_b);
    // The unkeyed lookup is ambiguous now, never silently task a's seal.
    assert!(service.accept(&verdict).is_err());
}

#[test]
fn dispute_within_allowance_schedules_reviewer_else_user() {
    let (mut state, task, attempt) = build_state();
    let criteria = criteria_hash(state.tasks.get(&task).expect("task"));
    let mut quality = QualityLoop::new(1);
    let reseal = seal(&state, attempt, candidate_hash());
    quality.note_worker_done(&mut state, reseal);
    // Exhaust the repair budget so the task awaits leader analysis.
    for now in [1, 2, 3] {
        state = submit(
            &quality,
            &state,
            &verifier(),
            task,
            attempt,
            &fail_verdict(criteria.clone()),
            &fail_ctx("verifier-1"),
            now,
        )
        .expect("submit");
    }
    // Inside the approved allowance: one independent reviewer, bound to the
    // same candidate — never a reviewer of the verifier.
    match quality.plan_dispute(&state, task, "worker shows the check is flaky".to_string()) {
        DisputePlan::IndependentReviewer {
            task: disputed,
            prior_attempt,
            ..
        } => {
            assert_eq!(disputed, task);
            assert_eq!(prior_attempt, attempt);
        }
        DisputePlan::RouteToUser { .. } => panic!("first dispute is inside the allowance"),
    }
    let disputed = decide(
        &state,
        &user(),
        &Command::DisputeVerification {
            task,
            reason: "worker shows the check is flaky".to_string(),
        },
        Timestamp::from_millis(4),
    )
    .expect("dispute commits");
    state = disputed.next;
    assert_eq!(
        state.tasks.get(&task).expect("task").status,
        TaskStatus::AwaitingVerification
    );
    // Allowance spent: a second dispute routes to the user. No chains.
    match quality.plan_dispute(&state, task, "second objection".to_string()) {
        DisputePlan::RouteToUser { task: routed, .. } => assert_eq!(routed, task),
        DisputePlan::IndependentReviewer { .. } => {
            panic!("a second dispute must reach the user, never a verifier chain")
        }
    }
    // A verifier cannot dispute its own verdict.
    let smuggled = decide(
        &state,
        &verifier(),
        &Command::DisputeVerification {
            task,
            reason: "reconsider".to_string(),
        },
        Timestamp::from_millis(5),
    );
    assert_eq!(
        smuggled.map(|_| ()),
        Err(central_domain::DomainError::Forbidden)
    );
}

#[test]
fn leader_escalation_is_scheduled_work() {
    let (mut state, task, attempt) = build_state();
    let criteria = criteria_hash(state.tasks.get(&task).expect("task"));
    let mut quality = QualityLoop::new(1);
    let reseal = seal(&state, attempt, candidate_hash());
    quality.note_worker_done(&mut state, reseal);
    for now in [1, 2, 3] {
        state = submit(
            &quality,
            &state,
            &verifier(),
            task,
            attempt,
            &fail_verdict(criteria.clone()),
            &fail_ctx("verifier-1"),
            now,
        )
        .expect("submit");
    }
    let escalation = quality.leader_escalation(&state, task).expect("escalation");
    assert_eq!(escalation.task, task);
    assert_eq!(escalation.failed_rounds, 2);
    // The escalation is a budgeted, scheduled work item: it travels through
    // the scheduler and the budget gate like any other work. The domain
    // never calls a model and never bypasses a gate for the leader.
    assert!(escalation.requires_scheduler);
    assert!(escalation.requires_budget_gate);
    assert!(!escalation.bypasses_gate);
    assert!(!escalation.calls_model_directly);
}

#[test]
fn repair_attempt_links_predecessor() {
    let (mut state, task, attempt) = build_state();
    let criteria = criteria_hash(state.tasks.get(&task).expect("task"));
    let mut quality = QualityLoop::new(1);
    let reseal = seal(&state, attempt, candidate_hash());
    quality.note_worker_done(&mut state, reseal);
    let state = submit(
        &quality,
        &state,
        &verifier(),
        task,
        attempt,
        &fail_verdict(criteria),
        &fail_ctx("verifier-1"),
        1,
    )
    .expect("fail submits");
    // Ordinary queueing refuses a task that awaits verification: repair has
    // its own linked path, never a silent re-queue.
    let plain = decide(
        &state,
        &system(),
        &Command::QueueAttempt {
            attempt: AttemptId::from_u128(9_001),
            task,
            input: "requeue without linkage".to_string(),
            resources: Vec::new(),
            max_parallel: 2,
        },
        Timestamp::from_millis(2),
    );
    assert!(plain.is_err());
    // A wrong predecessor is refused.
    let wrong = decide(
        &state,
        &system(),
        &Command::QueueRepairAttempt {
            attempt: AttemptId::from_u128(9_002),
            task,
            predecessor: AttemptId::from_u128(9_999),
            input: "repair".to_string(),
            resources: Vec::new(),
            max_parallel: 2,
        },
        Timestamp::from_millis(2),
    );
    assert!(wrong.is_err());
    let repair_id = AttemptId::from_u128(9_003);
    let repair = decide(
        &state,
        &system(),
        &Command::QueueRepairAttempt {
            attempt: repair_id,
            task,
            predecessor: attempt,
            input: "repair with the verifier findings".to_string(),
            resources: Vec::new(),
            max_parallel: 2,
        },
        Timestamp::from_millis(2),
    )
    .expect("repair queues");
    assert_eq!(
        repair.next.attempt_predecessor.get(&repair_id),
        Some(&attempt)
    );
    assert_eq!(
        repair.next.tasks.get(&task).expect("task").status,
        TaskStatus::Running
    );
    // The repair base is consumed: no second repair without a new verdict.
    let again = decide(
        &repair.next,
        &system(),
        &Command::QueueRepairAttempt {
            attempt: AttemptId::from_u128(9_004),
            task,
            predecessor: attempt,
            input: "repair".to_string(),
            resources: Vec::new(),
            max_parallel: 2,
        },
        Timestamp::from_millis(3),
    );
    assert!(again.is_err());
}

#[test]
fn fail_verdict_must_name_failed_criteria() {
    let (mut state, task, attempt) = build_state();
    let criteria = criteria_hash(state.tasks.get(&task).expect("task"));
    let mut quality = QualityLoop::new(1);
    let reseal = seal(&state, attempt, candidate_hash());
    quality.note_worker_done(&mut state, reseal);
    // A fail that names no failed criterion and no evidence is not a
    // verdict the loop can act on.
    let silent = quality.build_report(
        &state,
        task,
        &fail_verdict(criteria),
        &ctx("verifier-1", Vec::new()),
    );
    assert!(silent.is_err());
}

#[test]
fn criteria_change_requires_plan_revision() {
    let (mut state, task, attempt) = build_state();
    let criteria = criteria_hash(state.tasks.get(&task).expect("task"));
    let mut quality = QualityLoop::new(1);
    let reseal = seal(&state, attempt, candidate_hash());
    quality.note_worker_done(&mut state, reseal);
    // One fail commits durable history.
    let first = decide(
        &state,
        &verifier(),
        &Command::SubmitVerification {
            task,
            attempt,
            report: quality
                .build_report(
                    &state,
                    task,
                    &fail_verdict(criteria.clone()),
                    &fail_ctx("verifier-1"),
                )
                .expect("report builds"),
        },
        Timestamp::from_millis(1),
    )
    .expect("fail submits");
    let history = first.events.clone();
    state = first.next;
    // A verdict bound to edited criteria is stale: criteria change only via
    // an approved plan revision, never by rewriting the verdict.
    let edited = VerificationV1 {
        criteria_hash: other_hash(),
        ..fail_verdict(criteria.clone())
    };
    let stale = quality.build_report(&state, task, &edited, &fail_ctx("verifier-1"));
    assert!(stale.is_err());
    // History is append-only: the committed verdict still stands as written.
    assert!(
        history.iter().any(|event| matches!(
            event,
            central_domain::Event::VerificationSubmitted { task: t, .. } if *t == task
        )),
        "committed verdicts are never rewritten"
    );
    let _ = BTreeMap::<TaskId, u8>::new();
}
