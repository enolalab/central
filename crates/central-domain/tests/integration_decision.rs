//! Mechanical Git integration at the domain layer: `RecordIntegration`
//! moves the good ref's receipt into durable history. No git here — the
//! application pre-verifies everything (sealed candidate, checks, CAS);
//! the domain enforces the actor gate, the integration-kind binding and
//! the claimed-attempt state.

// Shared helpers; this binary uses only a subset (other binaries use
// the rest), so unused-helper lints are silenced here only.
#[allow(dead_code)]
#[path = "support/mod.rs"]
mod support;

use central_domain::{
    decide, Actor, AttemptId, AttemptStatus, Command, DomainError, Event, IntegrationCheck,
    IntegrationSpec, PlanDocumentV1, PlanHash, PlanTaskV1, TaskId, TaskKind, TaskStatus,
    VerifySpec,
};

fn integration_task(id: TaskId) -> PlanTaskV1 {
    PlanTaskV1 {
        id,
        kind: TaskKind::Integration,
        objective: "integrate the verified candidate".to_string(),
        inputs: Vec::new(),
        scope: Vec::new(),
        out_of_scope: Vec::new(),
        depends_on: Vec::new(),
        data_depends_on: Vec::new(),
        outputs: Vec::new(),
        verify: None,
        evidence: Vec::new(),
        profile_id: None,
        permissions: Vec::new(),
        max_repair_rounds: 0,
        requires_approval: Vec::new(),
        handover: String::new(),
        integration: Some(IntegrationSpec {
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

fn implementation_task(id: TaskId) -> PlanTaskV1 {
    PlanTaskV1 {
        id,
        kind: TaskKind::Implementation,
        objective: "implement".to_string(),
        inputs: Vec::new(),
        scope: Vec::new(),
        out_of_scope: Vec::new(),
        depends_on: Vec::new(),
        data_depends_on: Vec::new(),
        outputs: Vec::new(),
        verify: Some(VerifySpec {
            method: "check".to_string(),
            criteria: vec!["done".to_string()],
        }),
        evidence: Vec::new(),
        profile_id: None,
        permissions: Vec::new(),
        max_repair_rounds: 1,
        requires_approval: Vec::new(),
        handover: String::new(),
        integration: None,
    }
}

fn approved_two_task_plan() -> (central_domain::RunState, PlanHash, TaskId) {
    let integration = TaskId::from_u128(21);
    let plan = PlanDocumentV1 {
        version: 1,
        goal: "integrate".to_string(),
        tasks: vec![
            implementation_task(support::task_a()),
            integration_task(integration),
        ],
    };
    let state = support::created_run();
    let actor = support::leader();
    let proposed = decide(
        &state,
        &actor,
        &Command::ProposePlan { plan },
        support::now(),
    )
    .expect("propose plan with integration task")
    .next;
    let hash = proposed.pending_plan.clone().expect("pending plan");
    let approved = decide(
        &proposed,
        &support::user(),
        &Command::ApprovePlan {
            plan_hash: hash.clone(),
        },
        support::later(),
    )
    .expect("approve plan")
    .next;
    (approved, hash, integration)
}

fn queue_and_claim(
    state: &central_domain::RunState,
    actor: &Actor,
    attempt: AttemptId,
    task: TaskId,
) -> central_domain::RunState {
    let queued = decide(
        state,
        actor,
        &Command::QueueAttempt {
            attempt,
            task,
            input: "integrate the candidate".to_string(),
            resources: Vec::new(),
            max_parallel: 1,
        },
        support::later(),
    )
    .expect("queue attempt")
    .next;
    decide(
        &queued,
        actor,
        &Command::StartDispatch { attempt },
        support::later(),
    )
    .expect("claim attempt")
    .next
}

fn record(
    state: &central_domain::RunState,
    actor: &Actor,
    task: TaskId,
    attempt: AttemptId,
) -> Result<central_domain::Decision, DomainError> {
    decide(
        state,
        actor,
        &Command::RecordIntegration {
            task,
            attempt,
            candidate_commit: "c0ffee".to_string(),
            previous_head: "ba5e0000".to_string(),
            new_head: "deadbeef".to_string(),
            check_evidence: vec!["baseline:0".to_string()],
        },
        support::later(),
    )
}

#[test]
fn record_integration_succeeds_claimed_integration_task() {
    let (state, _hash, task) = approved_two_task_plan();
    let attempt = AttemptId::from_u128(301);
    let claimed = queue_and_claim(&state, &support::user(), attempt, task);
    let decision = record(&claimed, &support::user(), task, attempt).expect("record integration");
    let after = decision.next;
    assert_eq!(after.attempts[&attempt].status, AttemptStatus::Completed);
    assert_eq!(after.tasks[&task].status, TaskStatus::Succeeded);
    assert_eq!(
        after.verified_outputs[&task].to_string(),
        "deadbeef",
        "merge commit filed as verified output"
    );
    assert!(decision.events.iter().any(|e| matches!(
        e,
        Event::IntegrationPromoted { new_head, .. }
        if new_head == "deadbeef"
    )));
}

#[test]
fn record_integration_refuses_non_integration_task() {
    let (state, _hash, _integration) = approved_two_task_plan();
    let attempt = AttemptId::from_u128(302);
    let claimed = queue_and_claim(&state, &support::user(), attempt, support::task_a());
    let err =
        record(&claimed, &support::user(), support::task_a(), attempt).expect_err("must refuse");
    assert!(
        matches!(err, DomainError::InvalidCommand(_)),
        "wrong kind is a contract violation, not a retry: {err:?}"
    );
}

#[test]
fn record_integration_requires_claimed_attempt() {
    let (state, _hash, task) = approved_two_task_plan();
    // Queued but never claimed: no dispatch happened, nothing may promote.
    let attempt = AttemptId::from_u128(303);
    let queued = decide(
        &state,
        &support::user(),
        &Command::QueueAttempt {
            attempt,
            task,
            input: "integrate the candidate".to_string(),
            resources: Vec::new(),
            max_parallel: 1,
        },
        support::later(),
    )
    .expect("queue")
    .next;
    let err = record(&queued, &support::user(), task, attempt).expect_err("must refuse");
    assert!(
        matches!(err, DomainError::QueueConflict(_)),
        "unclaimed attempt cannot promote: {err:?}"
    );
}

#[test]
fn record_integration_rejects_worker_actor() {
    let (state, _hash, task) = approved_two_task_plan();
    let attempt = AttemptId::from_u128(304);
    let claimed = queue_and_claim(&state, &support::user(), attempt, task);
    let err = record(&claimed, &support::worker(), task, attempt).expect_err("must refuse");
    assert!(
        matches!(err, DomainError::Forbidden),
        "workers never move the good ref: {err:?}"
    );
}

#[test]
fn record_integration_rejects_unknown_attempt() {
    let (state, _hash, task) = approved_two_task_plan();
    let err =
        record(&state, &support::user(), task, AttemptId::from_u128(999)).expect_err("must refuse");
    assert!(
        matches!(err, DomainError::UnknownAttempt(_)),
        "foreign attempt fails closed: {err:?}"
    );
}

#[test]
fn record_integration_rejects_empty_receipt() {
    let (state, _hash, task) = approved_two_task_plan();
    let attempt = AttemptId::from_u128(305);
    let claimed = queue_and_claim(&state, &support::user(), attempt, task);
    let err = decide(
        &claimed,
        &support::user(),
        &Command::RecordIntegration {
            task,
            attempt,
            candidate_commit: String::new(),
            previous_head: "ba5e0000".to_string(),
            new_head: String::new(),
            check_evidence: Vec::new(),
        },
        support::later(),
    )
    .expect_err("must refuse");
    assert!(
        matches!(err, DomainError::InvalidCommand(_)),
        "empty receipt hashes are refused: {err:?}"
    );
}
