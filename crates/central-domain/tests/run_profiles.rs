//! Run-wide agent selection: which provider/model profile the worker
//! and the leader roles resolve through, and queueing leader reviews.
mod support;

use central_domain::{decide, AttemptStatus, Command, DomainError, ProfileId, ProfileRole, TaskId};
use support::{approved_run, created_run, leader, task_a, user, verifier};

fn set_profile(
    state: &central_domain::RunState,
    actor: &central_domain::Actor,
    role: ProfileRole,
    profile: Option<ProfileId>,
) -> Result<central_domain::RunState, DomainError> {
    decide(
        state,
        actor,
        &Command::SetRunProfile { role, profile },
        support::later(),
    )
    .map(|decision| decision.next)
}

fn profile(id: u128) -> ProfileId {
    ProfileId::from_u128(id)
}

#[test]
fn owner_pins_worker_and_leader_profiles_independently() {
    let state = created_run();
    let next = set_profile(&state, &user(), ProfileRole::Worker, Some(profile(11)))
        .expect("set worker profile");
    assert_eq!(next.worker_profile, Some(profile(11)));
    assert_eq!(next.leader_profile, None);

    let next = set_profile(&next, &user(), ProfileRole::Leader, Some(profile(22)))
        .expect("set leader profile");
    assert_eq!(next.worker_profile, Some(profile(11)));
    assert_eq!(next.leader_profile, Some(profile(22)));

    // Clearing restores defer-when-unpinned; the other role is untouched.
    let next = set_profile(&next, &user(), ProfileRole::Worker, None).expect("clear");
    assert_eq!(next.worker_profile, None);
    assert_eq!(next.leader_profile, Some(profile(22)));
}

#[test]
fn only_the_owner_assigns_run_profiles() {
    let state = created_run();
    for actor in [verifier(), leader()] {
        assert_eq!(
            set_profile(&state, &actor, ProfileRole::Worker, Some(profile(11))),
            Err(DomainError::Forbidden),
            "bridges never assign run profiles"
        );
    }
}

#[test]
fn leader_review_queues_without_moving_the_task() {
    let (state, _) = approved_run();
    let attempt = central_domain::AttemptId::from_u128(4242);
    let next = decide(
        &state,
        &user(),
        &Command::QueueLeaderReview {
            attempt,
            task: task_a(),
            input: "review the sealed candidate".to_string(),
        },
        support::later(),
    )
    .expect("queue leader review")
    .next;
    let stored = &next.attempts[&attempt];
    assert_eq!(stored.task_id, task_a());
    assert_eq!(stored.status, AttemptStatus::Queued);
    assert!(next.leader_attempts.contains(&attempt));
    assert_eq!(
        next.attempt_inputs[&attempt],
        "review the sealed candidate".to_string()
    );
    // The parked task keeps its status: the review advises, it never
    // verifies or seals.
    assert_eq!(next.tasks[&task_a()].status, state.tasks[&task_a()].status);
}

#[test]
fn leader_review_rejects_bad_requests() {
    let (state, _) = approved_run();
    let attempt = central_domain::AttemptId::from_u128(4243);
    let queued = |command: Command| decide(&state, &user(), &command, support::later());

    // Unknown task.
    assert!(matches!(
        queued(Command::QueueLeaderReview {
            attempt,
            task: TaskId::from_u128(999),
            input: "look".to_string(),
        }),
        Err(DomainError::UnknownTask(_))
    ));
    // Empty input.
    assert!(matches!(
        queued(Command::QueueLeaderReview {
            attempt,
            task: task_a(),
            input: "  ".to_string(),
        }),
        Err(DomainError::InvalidCommand(_))
    ));
    // Bridge actors cannot queue leader reviews.
    for actor in [verifier(), leader()] {
        assert_eq!(
            decide(
                &state,
                &actor,
                &Command::QueueLeaderReview {
                    attempt,
                    task: task_a(),
                    input: "look".to_string(),
                },
                support::later()
            ),
            Err(DomainError::Forbidden)
        );
    }
    // No duplicate attempt ids.
    let once = queued(Command::QueueLeaderReview {
        attempt,
        task: task_a(),
        input: "look".to_string(),
    })
    .expect("first queue")
    .next;
    assert!(matches!(
        decide(
            &once,
            &user(),
            &Command::QueueLeaderReview {
                attempt,
                task: task_a(),
                input: "look again".to_string(),
            },
            support::later()
        ),
        Err(DomainError::QueueConflict(_))
    ));

    // A stopped run queues nothing.
    let mut stopped = state.clone();
    stopped.control = central_domain::RunControl::Stopped;
    assert!(matches!(
        decide(
            &stopped,
            &user(),
            &Command::QueueLeaderReview {
                attempt: central_domain::AttemptId::from_u128(4244),
                task: task_a(),
                input: "look".to_string(),
            },
            support::later()
        ),
        Err(DomainError::QueueConflict(_))
    ));
}
