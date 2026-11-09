//! P2 Task 5: immutable artifacts and one-task independent verification.
//!
//! A worker finishing (exit zero, attempt Completed) never marks a task
//! `Succeeded`. Only an independent verifier verdict bound to the exact
//! sealed input/candidate/criteria flips a task to `Succeeded`; anything
//! mismatched is stale and must be re-verified.

use central_application::handover::{evidence_for, VerificationV1, WorkerResultV1};
use central_application::ports::checks::CheckEvidence;
use central_application::verification::{criteria_hash, seal_candidate, VerificationService};
use central_domain::{
    Attempt, AttemptId, AttemptStatus, ContentHash, Observation, PlanHash, RunId, RunState, Task,
    TaskId, TaskKind, TaskStatus, Timestamp, VerificationOutcome, VerifySpec,
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

fn empty_hash() -> ContentHash {
    // SHA-256 of the empty string.
    hash("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
}

fn build_state() -> (RunState, TaskId, AttemptId) {
    let run = RunId::from_u128(9_001);
    let task_id = TaskId::from_u128(9_101);
    let attempt_id = AttemptId::from_u128(9_201);
    let plan_hash = PlanHash::from_hex("plan TASK-5");
    let mut state = RunState::new(run);
    state.created = true;
    state.tasks.insert(
        task_id,
        Task {
            id: task_id,
            plan_hash: plan_hash.clone(),
            kind: TaskKind::Implementation,
            objective: "add one fixture function".to_string(),
            status: TaskStatus::Running,
            depends_on: Vec::new(),
            data_depends_on: Vec::new(),
            verify: VerifySpec {
                method: "run the approved check".to_string(),
                criteria: vec!["the approved check exits zero".to_string()],
            },
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
            plan_hash,
            status: AttemptStatus::Completed,
            profile_id: None,
            created_at: Timestamp::from_millis(0),
        },
    );
    (state, task_id, attempt_id)
}

fn worker_result(attempt_id: AttemptId) -> WorkerResultV1 {
    WorkerResultV1 {
        version: 1,
        attempt_id,
        input_hash: input_hash(),
        candidate_path: "candidate/patch.bin".to_string(),
        candidate_hash: candidate_hash(),
        evidence_paths: Vec::new(),
        summary: "worker finished one fixture function".to_string(),
    }
}

fn verdict(
    subject: ContentHash,
    criteria: ContentHash,
    outcome: VerificationOutcome,
    evidence_ids: Vec<String>,
) -> VerificationV1 {
    VerificationV1 {
        version: 1,
        subject_hash: subject,
        criteria_hash: criteria,
        outcome,
        evidence_ids,
        findings: "independent check of the sealed candidate".to_string(),
    }
}

fn pass_verdict(criteria: ContentHash) -> VerificationV1 {
    verdict(
        candidate_hash(),
        criteria,
        VerificationOutcome::Pass,
        vec![input_hash().as_str().to_string()],
    )
}

#[test]
fn exit_zero_is_not_task_success() {
    let (state, task, attempt) = build_state();
    let seal = seal_candidate(&state, &worker_result(attempt)).expect("seal");
    let mut service = VerificationService::new();
    let worker_done = service.note_worker_done(seal);
    assert_ne!(worker_done.task_status, TaskStatus::Succeeded);
    assert_eq!(worker_done.task_status, TaskStatus::AwaitingVerification);

    // An approved check exits zero. That is evidence, never a verdict: the
    // task stays parked until an independent verifier accepts.
    let _evidence = CheckEvidence {
        program: "check".to_string(),
        args: Vec::new(),
        exit_code: 0,
        timed_out: false,
        stdout_hash: empty_hash(),
        stderr_hash: empty_hash(),
        stdout_tail: "ok".to_string(),
        stderr_tail: String::new(),
    };
    assert_eq!(
        service.task_status(task),
        Some(TaskStatus::AwaitingVerification)
    );
    assert_ne!(
        service.task_status(task).expect("tracked"),
        TaskStatus::Succeeded
    );
}

#[test]
fn verifier_uses_sealed_candidate() {
    let (state, task, attempt) = build_state();
    let task_state = state.tasks.get(&task).expect("task");
    let criteria = criteria_hash(task_state);
    let seal = seal_candidate(&state, &worker_result(attempt)).expect("seal");
    let mut service = VerificationService::new();
    service.note_worker_done(seal);

    // A verdict for a subject nobody sealed is refused.
    let unsealed = verdict(
        other_hash(),
        criteria.clone(),
        VerificationOutcome::Pass,
        vec![input_hash().as_str().to_string()],
    );
    assert!(service.accept(&unsealed).is_err());

    // Path escapes are refused at the seal, before any status moves.
    let mut escaped = worker_result(attempt);
    escaped.candidate_path = "../escape.bin".to_string();
    let path_escape = seal_candidate(&state, &escaped);
    assert!(path_escape.is_err());
    assert_eq!(
        service.task_status(task),
        Some(TaskStatus::AwaitingVerification)
    );

    // The sealed candidate with the sealed criteria passes.
    let accepted = service
        .accept(&pass_verdict(criteria))
        .expect("bound pass is accepted");
    assert_eq!(accepted.candidate_hash, candidate_hash());
    assert_eq!(accepted.task_status, TaskStatus::Succeeded);
    let after_pass = service.task_status(task).expect("tracked");
    assert_eq!(after_pass, TaskStatus::Succeeded);
}

#[test]
fn mismatched_input_requires_reverify() {
    let (state, _task, attempt) = build_state();
    let task_state = state.tasks.values().next().expect("task");
    let criteria = criteria_hash(task_state);
    let seal = seal_candidate(&state, &worker_result(attempt)).expect("seal");
    let mut service = VerificationService::new();
    service.note_worker_done(seal);

    // The verifier cites a different input than the sealed one: stale, the
    // candidate must be re-verified against the current input.
    let stale = verdict(
        candidate_hash(),
        criteria.clone(),
        VerificationOutcome::Pass,
        vec![other_hash().as_str().to_string()],
    );
    let refused = service.accept(&stale).expect_err("stale input is refused");
    assert!(
        refused.to_string().contains("reverify"),
        "unexpected error: {refused}"
    );

    // Criteria changed after the seal: stale as well.
    let criteria_changed = verdict(
        candidate_hash(),
        other_hash(),
        VerificationOutcome::Pass,
        vec![input_hash().as_str().to_string()],
    );
    let refused = service
        .accept(&criteria_changed)
        .expect_err("changed criteria are refused");
    assert!(
        refused.to_string().contains("reverify"),
        "unexpected error: {refused}"
    );
}

#[test]
fn fail_consumes_repair_rounds_then_escalates() {
    let (state, task, attempt) = build_state();
    let task_state = state.tasks.get(&task).expect("task");
    let criteria = criteria_hash(task_state);
    let seal = seal_candidate(&state, &worker_result(attempt)).expect("seal");
    let mut service = VerificationService::new();
    service.note_worker_done(seal);

    // Two quality-repair rounds: fail parks the task for retry, it never
    // flips to Succeeded and it never escalates early.
    let fail = || {
        verdict(
            candidate_hash(),
            criteria.clone(),
            VerificationOutcome::Fail,
            vec![input_hash().as_str().to_string()],
        )
    };
    let first = service.accept(&fail()).expect("first fail parks for retry");
    assert_eq!(first.task_status, TaskStatus::AwaitingVerification);
    assert_eq!(first.repair_rounds_used, 1);
    let second = service
        .accept(&fail())
        .expect("second fail parks for retry");
    assert_eq!(second.task_status, TaskStatus::AwaitingVerification);
    assert_eq!(second.repair_rounds_used, 2);

    // Rounds exhausted: the failure escalates instead of retrying forever.
    let third = service.accept(&fail()).expect("third fail escalates");
    assert_eq!(third.task_status, TaskStatus::Failed);

    // Blocked never consumes a quality round and never succeeds.
    let (state, task, attempt) = build_state();
    let task_state = state.tasks.get(&task).expect("task");
    let criteria = criteria_hash(task_state);
    let seal = seal_candidate(&state, &worker_result(attempt)).expect("seal");
    let mut service = VerificationService::new();
    service.note_worker_done(seal);
    let blocked = verdict(
        candidate_hash(),
        criteria,
        VerificationOutcome::Blocked,
        vec![input_hash().as_str().to_string()],
    );
    let receipt = service.accept(&blocked).expect("blocked is recorded");
    assert_eq!(receipt.task_status, TaskStatus::Blocked);
    assert_eq!(receipt.repair_rounds_used, 0);
}

#[test]
fn evidence_for_returns_attempt_scoped_evidence() {
    // T4 left `evidence_for` without a dedicated test; T5 covers it here.
    let (mut state, _task, attempt) = build_state();
    let other_attempt = AttemptId::from_u128(9_202);
    state.observations.push(Observation {
        attempt_id: attempt,
        provider_epoch: "epoch-1".to_string(),
        dedup_key: "obs-for-attempt".to_string(),
        outcome: None,
    });
    state.observations.push(Observation {
        attempt_id: other_attempt,
        provider_epoch: "epoch-1".to_string(),
        dedup_key: "obs-for-other".to_string(),
        outcome: None,
    });
    let own_hash = hash("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let other_hash_value = hash("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    let unowned_hash = hash("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc");
    state.artifacts.insert(
        central_domain::ArtifactId::from_u128(1),
        central_domain::Artifact {
            id: central_domain::ArtifactId::from_u128(1),
            hash: own_hash.clone(),
            kind: "candidate".to_string(),
            path: "candidate/patch.bin".to_string(),
            task_id: None,
            attempt_id: Some(attempt),
        },
    );
    state.artifacts.insert(
        central_domain::ArtifactId::from_u128(2),
        central_domain::Artifact {
            id: central_domain::ArtifactId::from_u128(2),
            hash: other_hash_value,
            kind: "candidate".to_string(),
            path: "candidate/other.bin".to_string(),
            task_id: None,
            attempt_id: Some(other_attempt),
        },
    );
    state.artifacts.insert(
        central_domain::ArtifactId::from_u128(3),
        central_domain::Artifact {
            id: central_domain::ArtifactId::from_u128(3),
            hash: unowned_hash,
            kind: "candidate".to_string(),
            path: "candidate/unowned.bin".to_string(),
            task_id: None,
            attempt_id: None,
        },
    );

    let (observations, hashes) = evidence_for(&state, attempt);
    assert_eq!(
        observations
            .iter()
            .map(|observation| observation.dedup_key.clone())
            .collect::<Vec<_>>(),
        vec!["obs-for-attempt".to_string()]
    );
    assert_eq!(hashes, vec![own_hash]);
}
