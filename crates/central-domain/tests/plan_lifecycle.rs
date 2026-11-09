mod support;

use central_domain::{
    decide, decide_envelope, validate_plan, Attempt, AttemptId, AttemptStatus, Command,
    CommandEnvelope, CommandId, DomainError, PlanDocumentV1, Revision, RunControl, TaskId,
};
use support::{
    approved_run, created_run, leader, now, proposed_run, task_a, task_b, user, valid_plan,
    verifier, worker,
};

#[test]
fn approve_exact_revision() {
    let (state, h1) = proposed_run();
    let envelope = CommandEnvelope {
        id: CommandId::from_u128(900),
        expected_revision: Some(state.revision),
        command: Command::ApprovePlan {
            plan_hash: h1.clone(),
        },
    };
    let approved = decide_envelope(&state, &user(), &envelope, support::later())
        .expect("approve exact revision")
        .next;
    assert_eq!(approved.active_plan, Some(h1.clone()));
    assert_eq!(approved.revision, Revision(state.revision.0 + 1));
    assert!(approved.tasks.contains_key(&task_a()));
    assert!(approved.tasks.contains_key(&task_b()));

    // A stale tab still holding the pre-approval revision is a conflict,
    // checked separately from the target plan hash.
    let stale = decide_envelope(
        &approved,
        &user(),
        &CommandEnvelope {
            id: CommandId::from_u128(901),
            expected_revision: Some(state.revision),
            command: Command::ApprovePlan { plan_hash: h1 },
        },
        support::later(),
    );
    assert_eq!(stale, Err(DomainError::StaleRevision));

    // A wrong target hash is a different failure from a stale revision.
    let (fresh, _) = proposed_run();
    let wrong_hash = central_domain::plan_hash(&{
        let mut changed = valid_plan();
        changed.tasks[0].verify.as_mut().unwrap().criteria = vec!["different criteria".to_string()];
        changed
    });
    let mismatch = decide(
        &fresh,
        &user(),
        &Command::ApprovePlan {
            plan_hash: wrong_hash,
        },
        support::later(),
    );
    assert!(matches!(
        mismatch,
        Err(DomainError::PlanHashMismatch) | Err(DomainError::UnknownPlan(_))
    ));
}

#[test]
fn agent_cannot_approve() {
    let (state, h1) = proposed_run();
    for agent in [leader(), worker(), verifier()] {
        let result = decide(
            &state,
            &agent,
            &Command::ApprovePlan {
                plan_hash: h1.clone(),
            },
            support::later(),
        );
        assert_eq!(result, Err(DomainError::Forbidden));
    }
}

#[test]
fn reject_cycle_or_missing_verify() {
    let mut cyclic = valid_plan();
    cyclic.tasks[0].depends_on = vec![task_b()];
    cyclic.tasks[1].depends_on = vec![task_a()];
    assert_eq!(validate_plan(&cyclic), Err(DomainError::DependencyCycle));

    let state = created_run();
    let proposed = decide(
        &state,
        &leader(),
        &Command::ProposePlan { plan: cyclic },
        now(),
    );
    assert_eq!(proposed, Err(DomainError::DependencyCycle));

    let mut missing_verify = valid_plan();
    missing_verify.tasks[1].verify = None;
    assert!(matches!(
        validate_plan(&missing_verify),
        Err(DomainError::MissingVerification(_))
    ));

    let mut empty_criteria = valid_plan();
    empty_criteria.tasks[0]
        .verify
        .as_mut()
        .unwrap()
        .criteria
        .clear();
    assert!(matches!(
        validate_plan(&empty_criteria),
        Err(DomainError::MissingVerification(_))
    ));
}

#[test]
fn pause_preserves_running_attempt() {
    let (mut state, _) = approved_run();
    let a = AttemptId::from_u128(500);
    state.attempts.insert(
        a,
        Attempt {
            id: a,
            task_id: task_a(),
            plan_hash: state.active_plan.clone().unwrap(),
            status: AttemptStatus::Running,
            profile_id: None,
            created_at: now(),
        },
    );
    let paused = decide(&state, &user(), &Command::Pause, support::later())
        .expect("pause")
        .next;
    assert_eq!(paused.control, RunControl::Paused);
    assert_eq!(paused.attempts[&a].status, AttemptStatus::Running);
}

#[test]
fn empty_plan_rejected() {
    let empty = PlanDocumentV1 {
        version: 1,
        goal: "nothing".to_string(),
        tasks: Vec::new(),
    };
    assert_eq!(validate_plan(&empty), Err(DomainError::EmptyPlan));
}

#[test]
fn duplicate_task_id_rejected() {
    let mut plan = valid_plan();
    plan.tasks[1].id = task_a();
    assert_eq!(
        validate_plan(&plan),
        Err(DomainError::DuplicateTaskId(task_a()))
    );
}

#[test]
fn unknown_dependency_rejected() {
    let mut plan = valid_plan();
    plan.tasks[0].depends_on = vec![TaskId::from_u128(999)];
    assert!(matches!(
        validate_plan(&plan),
        Err(DomainError::MissingDependency { .. })
    ));
}

#[test]
fn criteria_change_changes_hash_and_conflicts() {
    let original = valid_plan();
    let h1 = validate_plan(&original).expect("valid plan");
    let mut changed = original.clone();
    changed.tasks[0].verify.as_mut().unwrap().criteria = vec!["stricter criteria".to_string()];
    let h2 = validate_plan(&changed).expect("changed plan still valid");
    assert_ne!(h1, h2);

    // Approving content that was never proposed cannot reuse the old approval.
    let (state, _) = proposed_run();
    let conflict = decide(
        &state,
        &user(),
        &Command::ApprovePlan {
            plan_hash: h2.clone(),
        },
        support::later(),
    );
    assert!(matches!(
        conflict,
        Err(DomainError::PlanHashMismatch) | Err(DomainError::UnknownPlan(_))
    ));

    // Once the changed plan is proposed, the old hash is stale.
    let (state2, _) = proposed_run();
    let revised = decide(
        &state2,
        &leader(),
        &Command::ProposePlan { plan: changed },
        support::later(),
    )
    .expect("propose revised plan")
    .next;
    let stale_old = decide(
        &revised,
        &user(),
        &Command::ApprovePlan { plan_hash: h1 },
        support::later(),
    );
    assert_eq!(stale_old, Err(DomainError::StaleRevision));
}
