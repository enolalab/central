use crate::{
    validate_plan, Actor, ActorKind, AttemptStatus, Command, CommandEnvelope, DomainError, Effect,
    Event, PlanRevision, PlanStatus, RunControl, RunState, Task, TaskStatus, Timestamp,
};

#[derive(Clone, Debug, PartialEq)]
pub struct Decision {
    pub next: RunState,
    pub events: Vec<Event>,
    pub effects: Vec<Effect>,
}

fn require_created(state: &RunState) -> Result<(), DomainError> {
    if state.created {
        Ok(())
    } else {
        Err(DomainError::RunNotCreated)
    }
}

fn require_user(actor: &Actor) -> Result<(), DomainError> {
    if actor.kind == ActorKind::User {
        Ok(())
    } else {
        Err(DomainError::Forbidden)
    }
}

fn require_user_or_system(actor: &Actor) -> Result<(), DomainError> {
    if matches!(actor.kind, ActorKind::User | ActorKind::System) {
        Ok(())
    } else {
        Err(DomainError::Forbidden)
    }
}

fn finish(mut next: RunState, events: Vec<Event>, effects: Vec<Effect>) -> Decision {
    next.revision = next.revision.next();
    Decision {
        next,
        events,
        effects,
    }
}

/// Pure decision function: zero IO, deterministic for a given timestamp.
pub fn decide(
    state: &RunState,
    actor: &Actor,
    command: &Command,
    now: Timestamp,
) -> Result<Decision, DomainError> {
    match command {
        Command::CreateRun { goal, repository } => {
            require_user(actor)?;
            if state.created {
                return Err(DomainError::RunAlreadyCreated);
            }
            if goal.trim().is_empty() {
                return Err(DomainError::InvalidCommand(
                    "goal must not be empty".to_string(),
                ));
            }
            if repository.trim().is_empty() {
                return Err(DomainError::InvalidCommand(
                    "repository must not be empty".to_string(),
                ));
            }
            let mut next = state.clone();
            next.created = true;
            next.goal = goal.clone();
            next.repository = repository.clone();
            next.control = RunControl::Running;
            Ok(finish(
                next.clone(),
                vec![Event::RunCreated {
                    run_id: next.id,
                    goal: goal.clone(),
                    repository: repository.clone(),
                    by: actor.id,
                    at: now,
                }],
                Vec::new(),
            ))
        }

        Command::ProposePlan { plan } => {
            require_created(state)?;
            if !matches!(actor.kind, ActorKind::Leader | ActorKind::User) {
                return Err(DomainError::Forbidden);
            }
            if state.control == RunControl::Stopped {
                return Err(DomainError::InvalidTransition(
                    "cannot propose a plan for a stopped run".to_string(),
                ));
            }
            let hash = validate_plan(plan)?;
            let mut next = state.clone();
            if let Some(previous_hash) = next.pending_plan.take() {
                if previous_hash != hash {
                    if let Some(previous) = next.plans.get_mut(&previous_hash) {
                        previous.status = PlanStatus::Superseded;
                    }
                }
            }
            next.plans.insert(
                hash.clone(),
                PlanRevision {
                    hash: hash.clone(),
                    document: plan.clone(),
                    status: PlanStatus::Proposed,
                    proposed_by: actor.id,
                    proposed_at: now,
                    approved_by: None,
                    approved_at: None,
                },
            );
            next.pending_plan = Some(hash.clone());
            Ok(finish(
                next.clone(),
                vec![Event::PlanProposed {
                    run_id: next.id,
                    plan_hash: hash,
                    by: actor.id,
                    at: now,
                }],
                Vec::new(),
            ))
        }

        Command::ApprovePlan { plan_hash } => {
            require_user(actor)?;
            require_created(state)?;
            let Some(plan) = state.plans.get(plan_hash) else {
                return Err(DomainError::UnknownPlan(plan_hash.clone()));
            };
            match plan.status {
                PlanStatus::Superseded => return Err(DomainError::StaleRevision),
                PlanStatus::Approved => {
                    return Err(DomainError::InvalidTransition(
                        "plan is already approved".to_string(),
                    ));
                }
                PlanStatus::Proposed => {}
            }
            // The target hash is checked separately from envelope revision:
            // approving anything but the current pending proposal conflicts.
            if state.pending_plan.as_ref() != Some(plan_hash) {
                return Err(DomainError::PlanHashMismatch);
            }

            let mut next = state.clone();
            if let Some(old_hash) = next.active_plan.take() {
                if old_hash != *plan_hash {
                    if let Some(old) = next.plans.get_mut(&old_hash) {
                        old.status = PlanStatus::Superseded;
                    }
                }
            }
            let document = {
                let approved = next.plans.get_mut(plan_hash).expect("plan checked above");
                approved.status = PlanStatus::Approved;
                approved.approved_by = Some(actor.id);
                approved.approved_at = Some(now);
                approved.document.clone()
            };
            next.pending_plan = None;
            next.active_plan = Some(plan_hash.clone());
            for plan_task in &document.tasks {
                let verify = plan_task
                    .verify
                    .clone()
                    .expect("approved plans passed validation");
                next.tasks
                    .entry(plan_task.id)
                    .and_modify(|task| {
                        task.plan_hash = plan_hash.clone();
                        task.kind = plan_task.kind.clone();
                        task.objective = plan_task.objective.clone();
                        task.depends_on = plan_task.depends_on.clone();
                        task.data_depends_on = plan_task.data_depends_on.clone();
                        task.verify = verify.clone();
                        task.profile_id = plan_task.profile_id;
                        task.max_repair_rounds = plan_task.max_repair_rounds;
                    })
                    .or_insert_with(|| Task {
                        id: plan_task.id,
                        plan_hash: plan_hash.clone(),
                        kind: plan_task.kind.clone(),
                        objective: plan_task.objective.clone(),
                        status: TaskStatus::Pending,
                        depends_on: plan_task.depends_on.clone(),
                        data_depends_on: plan_task.data_depends_on.clone(),
                        verify: verify.clone(),
                        profile_id: plan_task.profile_id,
                        max_repair_rounds: plan_task.max_repair_rounds,
                        repair_rounds_used: 0,
                    });
            }
            Ok(finish(
                next.clone(),
                vec![Event::PlanApproved {
                    run_id: next.id,
                    plan_hash: plan_hash.clone(),
                    by: actor.id,
                    at: now,
                }],
                Vec::new(),
            ))
        }

        Command::Pause => {
            require_user_or_system(actor)?;
            require_created(state)?;
            if state.control != RunControl::Running {
                return Err(DomainError::InvalidTransition(
                    "only a running run can be paused".to_string(),
                ));
            }
            // Pause closes the dispatch gate only; running attempts finish.
            let mut next = state.clone();
            next.control = RunControl::Paused;
            Ok(finish(
                next.clone(),
                vec![Event::RunPaused {
                    run_id: next.id,
                    by: actor.id,
                    at: now,
                }],
                Vec::new(),
            ))
        }

        Command::Stop => {
            require_user_or_system(actor)?;
            require_created(state)?;
            if state.control == RunControl::Stopped {
                return Err(DomainError::InvalidTransition(
                    "run is already stopped".to_string(),
                ));
            }
            let mut next = state.clone();
            next.control = RunControl::Stopped;
            let mut effects = Vec::new();
            for attempt in next.attempts.values_mut() {
                if matches!(
                    attempt.status,
                    AttemptStatus::Running
                        | AttemptStatus::Dispatching
                        | AttemptStatus::WaitingForApproval
                        | AttemptStatus::WaitingForInput
                ) {
                    attempt.status = AttemptStatus::Reconciling;
                    effects.push(Effect::CancelAttempt {
                        attempt_id: attempt.id,
                    });
                }
            }
            Ok(finish(
                next.clone(),
                vec![Event::RunStopped {
                    run_id: next.id,
                    by: actor.id,
                    at: now,
                }],
                effects,
            ))
        }

        Command::Resume => {
            require_user_or_system(actor)?;
            require_created(state)?;
            if !matches!(
                state.control,
                RunControl::Paused | RunControl::AwaitingResume
            ) {
                return Err(DomainError::InvalidTransition(
                    "only a paused or awaiting-resume run can be resumed".to_string(),
                ));
            }
            // Holds are deliberately untouched: resume never clears
            // BudgetExceeded / UsageUnknown / other holds.
            let mut next = state.clone();
            next.control = RunControl::Running;
            Ok(finish(
                next.clone(),
                vec![Event::RunResumed {
                    run_id: next.id,
                    by: actor.id,
                    at: now,
                }],
                Vec::new(),
            ))
        }
    }
}

/// Envelope path: expected-revision is checked separately here, before the
/// command's own target (for example the plan hash) is checked in `decide`.
pub fn decide_envelope(
    state: &RunState,
    actor: &Actor,
    envelope: &CommandEnvelope,
    now: Timestamp,
) -> Result<Decision, DomainError> {
    if let Some(expected) = envelope.expected_revision {
        if expected != state.revision {
            return Err(DomainError::StaleRevision);
        }
    }
    decide(state, actor, &envelope.command, now)
}
