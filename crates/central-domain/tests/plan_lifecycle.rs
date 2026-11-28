mod support;

use central_domain::{
    decide, decide_envelope, validate_plan, Attempt, AttemptId, AttemptStatus, Command,
    CommandEnvelope, CommandId, DiscussionIntent, DomainError, Event, LeaderDiscussion,
    PlanDocumentV1, PlanStatus, Revision, RunControl, RunState, TaskId, TaskStatus,
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
fn reproposing_approved_plan_does_not_downgrade_it() {
    let (state, h1) = approved_run();
    assert_eq!(state.active_plan, Some(h1.clone()));
    assert_eq!(state.plans[&h1].status, PlanStatus::Approved);

    let result = decide(
        &state,
        &leader(),
        &Command::ProposePlan { plan: valid_plan() },
        support::later(),
    );
    assert!(
        matches!(result, Err(DomainError::InvalidTransition(_))),
        "re-proposing the approved plan must be rejected, got {result:?}"
    );
}

#[test]
fn approval_does_not_mutate_existing_non_pending_task() {
    let (mut state, _h1) = approved_run();
    let running_before = {
        let task = state.tasks.get_mut(&task_a()).expect("task A exists");
        task.status = TaskStatus::Running;
        task.repair_rounds_used = 1;
        task.clone()
    };

    let mut revised = valid_plan();
    revised.tasks[0].objective = "Rewritten A objective".to_string();
    revised.tasks[0].verify.as_mut().unwrap().criteria = vec!["Rewritten A criteria".to_string()];
    let new_task_id = TaskId::from_u128(4);
    let mut new_task = revised.tasks[0].clone();
    new_task.id = new_task_id;
    new_task.objective = "Brand new task D".to_string();
    new_task.depends_on = Vec::new();
    new_task.data_depends_on = Vec::new();
    revised.tasks.push(new_task);

    let proposed = decide(
        &state,
        &leader(),
        &Command::ProposePlan { plan: revised },
        support::later(),
    )
    .expect("propose revised plan")
    .next;
    let h2 = proposed.pending_plan.clone().expect("pending revised plan");
    let approved = decide(
        &proposed,
        &user(),
        &Command::ApprovePlan {
            plan_hash: h2.clone(),
        },
        support::later(),
    )
    .expect("approve revised plan")
    .next;

    assert_eq!(approved.active_plan, Some(h2.clone()));
    // The Running task must not silently adopt the new plan's criteria.
    assert_eq!(approved.tasks[&task_a()], running_before);
    // Genuinely new tasks are still inserted as Pending.
    let inserted = &approved.tasks[&new_task_id];
    assert_eq!(inserted.status, TaskStatus::Pending);
    assert_eq!(inserted.plan_hash, h2);
    assert_eq!(inserted.objective, "Brand new task D");
}

#[test]
fn approval_refreshes_pending_task_definition() {
    let (mut state, h1) = approved_run();
    // Task A is Pending after first approval; give it repair history that
    // must survive a refresh (status and repair_rounds_used are preserved).
    {
        let task = state.tasks.get_mut(&task_a()).expect("task A exists");
        assert_eq!(task.status, TaskStatus::Pending);
        assert_eq!(task.plan_hash, h1);
        task.repair_rounds_used = 1;
    }

    let mut revised = valid_plan();
    revised.tasks[0].objective = "Rewritten A objective".to_string();
    revised.tasks[0].verify.as_mut().unwrap().criteria = vec!["Rewritten A criteria".to_string()];
    revised.tasks[0].depends_on = vec![task_b()];
    revised.tasks[0].data_depends_on = vec![task_b()];
    // Task C depends on A (order + data); keep DAG acyclic: B -> A -> C.

    let proposed = decide(
        &state,
        &leader(),
        &Command::ProposePlan { plan: revised },
        support::later(),
    )
    .expect("propose revised plan")
    .next;
    let h2 = proposed.pending_plan.clone().expect("pending revised plan");
    assert_ne!(h1, h2);
    let approved = decide(
        &proposed,
        &user(),
        &Command::ApprovePlan {
            plan_hash: h2.clone(),
        },
        support::later(),
    )
    .expect("approve revised plan")
    .next;

    assert_eq!(approved.active_plan, Some(h2.clone()));
    let refreshed = &approved.tasks[&task_a()];
    assert_eq!(refreshed.status, TaskStatus::Pending);
    assert_eq!(refreshed.repair_rounds_used, 1);
    assert_eq!(refreshed.plan_hash, h2);
    assert_eq!(refreshed.objective, "Rewritten A objective");
    assert_eq!(
        refreshed.verify.criteria,
        vec!["Rewritten A criteria".to_string()]
    );
    assert_eq!(refreshed.depends_on, vec![task_b()]);
    assert_eq!(refreshed.data_depends_on, vec![task_b()]);
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

#[test]
fn leader_discussion_creates_only_a_pending_plan_until_the_user_approves() {
    let mut state = created_run();
    state.leader_profile = Some(central_domain::ProfileId::from_u128(901));
    let plan = valid_plan();
    let hash = validate_plan(&plan).unwrap();
    for (number, reply, proposal, intent) in [
        (
            201,
            "Which file format do you need?",
            None,
            DiscussionIntent::Discuss,
        ),
        (
            202,
            "Here is the agreed plan.",
            Some(plan),
            DiscussionIntent::Plan,
        ),
    ] {
        let attempt = AttemptId::from_u128(number);
        for command in [
            Command::QueueLeaderDiscussion {
                workspace_read_only: false,
                intent,
                attempt,
                message: "Discuss the export".into(),
                input: "Conversation context".into(),
            },
            Command::StartDispatch { attempt },
            Command::AttemptRunning {
                attempt,
                session: central_domain::AttemptSession {
                    provider: "codex".into(),
                    instance_epoch: "epoch".into(),
                    thread_id: "thread".into(),
                },
            },
            Command::CompleteLeaderDiscussion {
                attempt,
                reply: Some(reply.into()),
                proposal,
                error: None,
            },
        ] {
            state = decide(&state, &user(), &command, now()).unwrap().next;
        }
        assert_eq!(state.attempts[&attempt].status, AttemptStatus::Completed);
        assert!(state.tasks.is_empty());
        assert!(state.active_plan.is_none());
        if number == 201 {
            assert!(state.pending_plan.is_none());
        }
    }
    assert_eq!(state.leader_discussion.len(), 2);
    assert_eq!(state.leader_discussion[1].proposal, Some(hash.clone()));
    let approved = decide(
        &state,
        &user(),
        &Command::ApprovePlan {
            plan_hash: hash.clone(),
        },
        now(),
    )
    .unwrap()
    .next;
    assert_eq!(approved.active_plan, Some(hash));
    assert!(approved.tasks.contains_key(&task_a()));
}

#[test]
fn missing_intent_deserializes_as_discuss() {
    let turn: LeaderDiscussion = serde_json::from_value(serde_json::json!({
        "attempt": AttemptId::from_u128(201).to_string(),
        "user": "Discuss the export",
        "reply": null,
        "proposal": null,
        "error": null,
    }))
    .expect("old LeaderDiscussion without intent");
    assert_eq!(turn.intent, DiscussionIntent::Discuss);

    let command: Command = serde_json::from_value(serde_json::json!({
        "QueueLeaderDiscussion": {
            "attempt": AttemptId::from_u128(202).to_string(),
            "message": "Discuss the export",
            "input": "Conversation context",
        }
    }))
    .expect("old QueueLeaderDiscussion without intent");
    assert!(matches!(
        command,
        Command::QueueLeaderDiscussion {
            intent: DiscussionIntent::Discuss,
            ..
        }
    ));
}

fn discussion_run() -> RunState {
    let mut state = created_run();
    state.leader_profile = Some(central_domain::ProfileId::from_u128(901));
    state
}

fn run_discussion_turn(
    state: &RunState,
    number: u128,
    intent: DiscussionIntent,
    proposal: Option<PlanDocumentV1>,
) -> central_domain::Decision {
    let attempt = AttemptId::from_u128(number);
    let mut next = state.clone();
    for command in [
        Command::QueueLeaderDiscussion {
            workspace_read_only: false,
            intent,
            attempt,
            message: "Discuss the export".into(),
            input: "Conversation context".into(),
        },
        Command::StartDispatch { attempt },
        Command::AttemptRunning {
            attempt,
            session: central_domain::AttemptSession {
                provider: "codex".into(),
                instance_epoch: "epoch".into(),
                thread_id: "thread".into(),
            },
        },
    ] {
        next = decide(&next, &user(), &command, now()).unwrap().next;
    }
    decide(
        &next,
        &user(),
        &Command::CompleteLeaderDiscussion {
            attempt,
            reply: Some("Leader reply.".into()),
            proposal,
            error: None,
        },
        now(),
    )
    .expect("complete discussion turn")
}

#[test]
fn discuss_turn_ignores_proposal() {
    let state = discussion_run();
    let decision = run_discussion_turn(&state, 211, DiscussionIntent::Discuss, Some(valid_plan()));
    assert!(decision.next.pending_plan.is_none());
    assert!(decision.next.plans.is_empty());
    assert!(!decision
        .events
        .iter()
        .any(|event| matches!(event, Event::PlanProposed { .. })));
    let turn = decision.next.leader_discussion.last().expect("turn");
    assert_eq!(turn.intent, DiscussionIntent::Discuss);
    assert_eq!(turn.reply.as_deref(), Some("Leader reply."));
    assert_eq!(turn.proposal, None);
}

#[test]
fn revise_turn_supersedes_pending_plan() {
    let state = discussion_run();
    let first = run_discussion_turn(&state, 241, DiscussionIntent::Plan, Some(valid_plan()));
    let h1 = first.next.pending_plan.clone().expect("pending plan");
    let attempt = AttemptId::from_u128(242);
    let mut next = first.next.clone();
    for command in [
        Command::QueueLeaderDiscussion {
            workspace_read_only: false,
            intent: DiscussionIntent::Revise,
            attempt,
            message: "Drop a task and sharpen the criteria".into(),
            input: "Conversation context".into(),
        },
        Command::StartDispatch { attempt },
        Command::AttemptRunning {
            attempt,
            session: central_domain::AttemptSession {
                provider: "codex".into(),
                instance_epoch: "epoch".into(),
                thread_id: "thread".into(),
            },
        },
    ] {
        next = decide(&next, &user(), &command, now()).unwrap().next;
    }
    let mut changed = valid_plan();
    changed.goal = "Revised export goal".to_string();
    let h2 = validate_plan(&changed).expect("changed plan stays valid");
    assert_ne!(h1, h2);
    let done = decide(
        &next,
        &user(),
        &Command::CompleteLeaderDiscussion {
            attempt,
            reply: Some("Revised the plan.".into()),
            proposal: Some(changed),
            error: None,
        },
        now(),
    )
    .expect("complete revise turn")
    .next;
    assert_eq!(done.pending_plan, Some(h2.clone()));
    assert_eq!(done.plans[&h1].status, PlanStatus::Superseded);
    let turn = done.leader_discussion.last().expect("revise turn");
    assert_eq!(turn.intent, DiscussionIntent::Revise);
    assert_eq!(turn.proposal, Some(h2));
}

#[test]
fn plan_turn_creates_pending_plan() {
    let state = discussion_run();
    let plan = valid_plan();
    let hash = validate_plan(&plan).unwrap();
    let decision = run_discussion_turn(&state, 212, DiscussionIntent::Plan, Some(plan));
    assert_eq!(decision.next.pending_plan, Some(hash.clone()));
    assert!(decision
        .events
        .iter()
        .any(|event| matches!(event, Event::PlanProposed { .. })));
    let turn = decision.next.leader_discussion.last().expect("turn");
    assert_eq!(turn.intent, DiscussionIntent::Plan);
    assert_eq!(turn.proposal, Some(hash));
}

#[test]
fn plan_and_revise_are_rejected_when_a_plan_is_active() {
    let (mut state, _) = approved_run();
    state.leader_profile = Some(central_domain::ProfileId::from_u128(901));
    for (number, intent) in [
        (221, DiscussionIntent::Plan),
        (222, DiscussionIntent::Revise),
    ] {
        let result = decide(
            &state,
            &user(),
            &Command::QueueLeaderDiscussion {
                workspace_read_only: false,
                intent,
                attempt: AttemptId::from_u128(number),
                message: "Change the plan".into(),
                input: "Conversation context".into(),
            },
            now(),
        );
        assert!(
            matches!(result, Err(DomainError::InvalidCommand(_))),
            "intent {intent:?} with active plan must be InvalidCommand, got {result:?}"
        );
    }
}

#[test]
fn revise_requires_a_pending_plan() {
    let state = discussion_run();
    let result = decide(
        &state,
        &user(),
        &Command::QueueLeaderDiscussion {
            workspace_read_only: false,
            intent: DiscussionIntent::Revise,
            attempt: AttemptId::from_u128(231),
            message: "Revise the plan".into(),
            input: "Conversation context".into(),
        },
        now(),
    );
    assert!(
        matches!(result, Err(DomainError::InvalidTransition(_))),
        "revise without pending plan must be InvalidTransition, got {result:?}"
    );
}
