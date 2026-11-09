use crate::{
    inbox, validate_plan, Actor, ActorKind, AnswerPayload, AttemptStatus, Command, CommandEnvelope,
    DomainError, Effect, Event, InboxEntry, InboxKey, InboxKind, InboxOutcome, InboxStatus,
    PlanRevision, PlanStatus, RunControl, RunState, Task, TaskStatus, Timestamp,
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
            if state.active_plan.as_ref() == Some(&hash)
                || matches!(
                    state.plans.get(&hash).map(|p| p.status),
                    Some(PlanStatus::Approved)
                )
            {
                return Err(DomainError::InvalidTransition(
                    "plan is already approved".to_string(),
                ));
            }
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
                // Pending tasks have no attempt/output to assess, so their
                // definition is refreshed from the newly approved plan
                // (status and repair_rounds_used are preserved). Non-Pending
                // tasks are never mutated: a Running or Succeeded task must
                // not silently adopt new criteria.
                match next.tasks.entry(plan_task.id) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(Task {
                            id: plan_task.id,
                            plan_hash: plan_hash.clone(),
                            kind: plan_task.kind.clone(),
                            objective: plan_task.objective.clone(),
                            status: TaskStatus::Pending,
                            depends_on: plan_task.depends_on.clone(),
                            data_depends_on: plan_task.data_depends_on.clone(),
                            verify,
                            profile_id: plan_task.profile_id,
                            max_repair_rounds: plan_task.max_repair_rounds,
                            repair_rounds_used: 0,
                        });
                    }
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        let task = entry.get_mut();
                        if task.status == TaskStatus::Pending {
                            task.plan_hash = plan_hash.clone();
                            task.kind = plan_task.kind.clone();
                            task.objective = plan_task.objective.clone();
                            task.depends_on = plan_task.depends_on.clone();
                            task.data_depends_on = plan_task.data_depends_on.clone();
                            task.verify = verify;
                            task.profile_id = plan_task.profile_id;
                            task.max_repair_rounds = plan_task.max_repair_rounds;
                        }
                    }
                }
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
            // A stopped run answers nothing: live entries go stale so no
            // reply can be accepted after the stop.
            for entry in next.inbox.values_mut() {
                if matches!(entry.status, InboxStatus::Pending | InboxStatus::Answered) {
                    entry.status = InboxStatus::Stale;
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

        Command::RecordExternalRequest { request } => {
            require_user_or_system(actor)?;
            require_created(state)?;
            if state.control == RunControl::Stopped {
                return Err(DomainError::InvalidTransition(
                    "cannot record external requests for a stopped run".to_string(),
                ));
            }
            let canonical = request.key.canonical();
            if let Some(existing) = state.inbox.get(&canonical) {
                // Same key, same text: a harness replay, not a new entry.
                if existing.preview_hash == request.preview_hash {
                    let next = state.clone();
                    return Ok(finish(next, Vec::new(), Vec::new()));
                }
                // Same key, different text inside one epoch: protocol
                // violation, never an overwrite.
                return Err(DomainError::RequestConflict(canonical));
            }
            let mut next = state.clone();
            next.inbox.insert(
                canonical.clone(),
                InboxEntry {
                    key: canonical.clone(),
                    attempt_id: request.attempt_id,
                    kind: request.kind,
                    status: InboxStatus::Pending,
                    preview_hash: request.preview_hash.clone(),
                    preview: inbox::truncate_preview(&request.preview),
                    requested_by: actor.id,
                    requested_at: now,
                    provider: request.key.provider.clone(),
                    epoch: request.key.epoch.clone(),
                    thread_id: request.key.thread_id.clone(),
                    turn_id: request.key.turn_id.clone(),
                    item_id: request.key.item_id.clone(),
                    request_id: request.key.request_id.clone(),
                    is_secret: request.is_secret,
                },
            );
            // The attempt parks in a waiting state; recording alone never
            // answers and never dispatches.
            if let Some(attempt_id) = request.attempt_id {
                if let Some(attempt) = next.attempts.get_mut(&attempt_id) {
                    if attempt.status == AttemptStatus::Running {
                        attempt.status = match request.kind {
                            InboxKind::Approval => AttemptStatus::WaitingForApproval,
                            InboxKind::Question => AttemptStatus::WaitingForInput,
                        };
                    }
                }
            }
            Ok(finish(
                next.clone(),
                vec![Event::InboxRequestRecorded {
                    run_id: next.id,
                    key: canonical,
                    kind: request.kind,
                    by: actor.id,
                    at: now,
                }],
                Vec::new(),
            ))
        }

        Command::RecordObservation { observation } => {
            require_user_or_system(actor)?;
            require_created(state)?;
            let mut next = state.clone();
            let mut events = Vec::new();
            if !next
                .observations
                .iter()
                .any(|known| known.dedup_key == observation.dedup_key)
            {
                if next.observations.len() >= MAX_OBSERVATIONS {
                    next.observations.remove(0);
                }
                next.observations.push(observation.clone());
                events.push(Event::ObservationRecorded {
                    run_id: next.id,
                    attempt_id: observation.attempt_id,
                    dedup_key: observation.dedup_key.clone(),
                    at: now,
                });
            }
            // Passive reconcile: observations confirm answered entries of the
            // same attempt. Nothing auto-answers; resolution only follows an
            // explicit user reply plus harness evidence.
            let mut resolved_any = false;
            for entry in next.inbox.values_mut() {
                if entry.status == InboxStatus::Answered
                    && entry.attempt_id == Some(observation.attempt_id)
                {
                    entry.status = InboxStatus::Resolved;
                    events.push(Event::InboxResolved {
                        run_id: next.id,
                        key: entry.key.clone(),
                        by: actor.id,
                        at: now,
                    });
                    resolved_any = true;
                }
            }
            if resolved_any {
                if let Some(attempt) = next.attempts.get_mut(&observation.attempt_id) {
                    if matches!(
                        attempt.status,
                        AttemptStatus::WaitingForApproval | AttemptStatus::WaitingForInput
                    ) {
                        attempt.status = AttemptStatus::Running;
                    }
                }
            }
            Ok(finish(next.clone(), events, Vec::new()))
        }

        Command::AnswerQuestion {
            key,
            preview_hash,
            payload,
        } => {
            require_user(actor)?;
            answer_inbox(
                state,
                actor,
                key,
                preview_hash,
                InboxKind::Question,
                InboxOutcome::Answered,
                payload.clone(),
                now,
            )
        }

        Command::ApproveAction { key, preview_hash } => {
            require_user(actor)?;
            answer_inbox(
                state,
                actor,
                key,
                preview_hash,
                InboxKind::Approval,
                InboxOutcome::Approved,
                AnswerPayload::Plain("approved".to_string()),
                now,
            )
        }

        Command::DeclineAction {
            key,
            preview_hash,
            reason,
        } => {
            require_user(actor)?;
            if reason.trim().is_empty() {
                return Err(DomainError::InvalidCommand(
                    "a decline needs a reason for the harness".to_string(),
                ));
            }
            answer_inbox(
                state,
                actor,
                key,
                preview_hash,
                InboxKind::Approval,
                InboxOutcome::Declined,
                AnswerPayload::Plain(format!("declined: {}", reason.trim())),
                now,
            )
        }
    }
}

/// Bound on stored harness observations; oldest drops first.
const MAX_OBSERVATIONS: usize = 4096;

/// Shared reply path: only a live Pending entry answers, scoped to its key
/// (epoch included) and to the exact preview the user saw. The reply becomes
/// a durable outbox intent; dispatch sends it only while the run is Running.
#[allow(clippy::too_many_arguments)]
fn answer_inbox(
    state: &RunState,
    actor: &Actor,
    key: &InboxKey,
    preview_hash: &crate::ContentHash,
    expected_kind: InboxKind,
    outcome: InboxOutcome,
    payload: AnswerPayload,
    now: Timestamp,
) -> Result<Decision, DomainError> {
    require_created(state)?;
    let canonical = key.canonical();
    let entry = match state.inbox.get(&canonical) {
        Some(entry) => entry,
        None => {
            // Same harness request under another epoch (for example a
            // restart that reused the request id): the reply is stale, not
            // unknown. Anything else names nothing at all — a plan hash is
            // never an inbox key.
            let rotated = state.inbox.values().any(|entry| {
                entry.provider == key.provider
                    && entry.thread_id == key.thread_id
                    && entry.turn_id == key.turn_id
                    && entry.item_id == key.item_id
                    && entry.request_id == key.request_id
            });
            if rotated {
                return Err(DomainError::StaleRequest);
            }
            return Err(DomainError::UnknownRequest(canonical.clone()));
        }
    };
    if entry.kind != expected_kind {
        return Err(DomainError::InvalidTransition(match expected_kind {
            InboxKind::Question => "entry is an action approval; answer questions with AnswerQuestion, approve or decline approvals"
                .to_string(),
            InboxKind::Approval => "entry is a question; answer it with AnswerQuestion, approvals use ApproveAction or DeclineAction"
                .to_string(),
        }));
    }
    if entry.status != InboxStatus::Pending
        || entry.epoch != key.epoch
        || entry.preview_hash != *preview_hash
    {
        // Covers answered, resolved and stale entries, epoch rotations and
        // preview mismatches: an old accept never applies, even to the same
        // text under a new key.
        return Err(DomainError::StaleRequest);
    }
    match &payload {
        AnswerPayload::Plain(text) if text.trim().is_empty() => {
            return Err(DomainError::InvalidCommand(
                "an answer must not be empty".to_string(),
            ));
        }
        AnswerPayload::Secret(reference) if reference.as_str().trim().is_empty() => {
            return Err(DomainError::InvalidCommand(
                "a secret answer needs a volatile reference".to_string(),
            ));
        }
        _ => {}
    }
    let mut next = state.clone();
    let stored = next.inbox.get_mut(&canonical).expect("entry checked above");
    stored.status = InboxStatus::Answered;
    Ok(finish(
        next.clone(),
        vec![Event::InboxAnswered {
            run_id: next.id,
            key: canonical.clone(),
            outcome,
            by: actor.id,
            at: now,
        }],
        vec![Effect::RespondToRequest {
            key: key.clone(),
            payload,
            scope: crate::AnswerScope::Once,
            attempt_id: entry.attempt_id,
        }],
    ))
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
