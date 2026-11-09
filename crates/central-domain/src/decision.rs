use std::collections::BTreeSet;

use crate::{
    blocking_dependencies, check_report, covering_attempt, decide_quality, inbox,
    live_attempt_count, release_settled_claims, validate_plan, Actor, ActorKind, AnswerPayload,
    Attempt, AttemptStatus, Command, CommandEnvelope, DomainError, Effect, Event, InboxEntry,
    InboxKey, InboxKind, InboxOutcome, InboxStatus, PlanRevision, PlanStatus, QualityDecision,
    RunControl, RunState, Task, TaskStatus, Timestamp,
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
            // Passive reconcile: observations confirm answered entries with
            // matching harness evidence (same attempt, epoch and item).
            // Nothing auto-answers; resolution only follows an explicit user
            // reply plus an observation correlated to that entry.
            let mut resolved_any = false;
            for entry in next.inbox.values_mut() {
                if entry.status == InboxStatus::Answered
                    && inbox::observation_matches_entry(observation, entry)
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

        Command::QueueAttempt {
            attempt,
            task,
            input,
            resources,
            max_parallel,
        } => queue_attempt(
            state,
            actor,
            *attempt,
            *task,
            input,
            resources,
            *max_parallel,
            now,
        ),

        Command::StartDispatch { attempt } => {
            require_user_or_system(actor)?;
            require_created(state)?;
            let attempt_state = state
                .attempts
                .get(attempt)
                .map(|record| record.status)
                .ok_or(DomainError::QueueConflict(format!(
                    "attempt {attempt} is unknown to this run"
                )))?;
            if attempt_state != AttemptStatus::Queued {
                return Err(DomainError::QueueConflict(format!(
                    "attempt {attempt} is not queued (it is {attempt_state:?})"
                )));
            }
            let mut next = state.clone();
            if let Some(record) = next.attempts.get_mut(attempt) {
                record.status = AttemptStatus::Dispatching;
            }
            Ok(finish(
                next.clone(),
                vec![Event::AttemptDispatching {
                    run_id: next.id,
                    attempt_id: *attempt,
                    by: actor.id,
                    at: now,
                }],
                Vec::new(),
            ))
        }

        Command::AttemptRunning { attempt, session } => {
            require_user_or_system(actor)?;
            require_created(state)?;
            let attempt_state = state
                .attempts
                .get(attempt)
                .map(|record| record.status)
                .ok_or(DomainError::QueueConflict(format!(
                    "attempt {attempt} is unknown to this run"
                )))?;
            if attempt_state != AttemptStatus::Dispatching {
                return Err(DomainError::QueueConflict(format!(
                    "attempt {attempt} never started dispatching (it is {attempt_state:?})"
                )));
            }
            let mut next = state.clone();
            if let Some(record) = next.attempts.get_mut(attempt) {
                record.status = AttemptStatus::Running;
            }
            next.attempt_sessions.insert(*attempt, session.clone());
            Ok(finish(
                next.clone(),
                vec![Event::AttemptRunning {
                    run_id: next.id,
                    attempt_id: *attempt,
                    by: actor.id,
                    at: now,
                }],
                Vec::new(),
            ))
        }

        Command::AttemptUnknown { attempt } => {
            require_user_or_system(actor)?;
            require_created(state)?;
            let attempt_state = state
                .attempts
                .get(attempt)
                .map(|record| record.status)
                .ok_or(DomainError::QueueConflict(format!(
                    "attempt {attempt} is unknown to this run"
                )))?;
            if !matches!(
                attempt_state,
                AttemptStatus::Dispatching | AttemptStatus::Running
            ) {
                return Err(DomainError::QueueConflict(format!(
                    "attempt {attempt} has no ambiguous dispatch (it is {attempt_state:?})"
                )));
            }
            // Claims stay held: nothing is released for unknown outcomes.
            let mut next = state.clone();
            if let Some(record) = next.attempts.get_mut(attempt) {
                record.status = AttemptStatus::UnknownOutcome;
            }
            Ok(finish(
                next.clone(),
                vec![Event::AttemptUnknown {
                    run_id: next.id,
                    attempt_id: *attempt,
                    by: actor.id,
                    at: now,
                }],
                Vec::new(),
            ))
        }

        Command::RequeueAttempt { attempt } => {
            require_user_or_system(actor)?;
            require_created(state)?;
            let attempt_state = state
                .attempts
                .get(attempt)
                .map(|record| record.status)
                .ok_or(DomainError::QueueConflict(format!(
                    "attempt {attempt} is unknown to this run"
                )))?;
            if attempt_state != AttemptStatus::Dispatching {
                return Err(DomainError::QueueConflict(format!(
                    "attempt {attempt} is not waiting on a dispatch (it is {attempt_state:?})"
                )));
            }
            let mut next = state.clone();
            if let Some(record) = next.attempts.get_mut(attempt) {
                record.status = AttemptStatus::Queued;
            }
            Ok(finish(
                next.clone(),
                vec![Event::AttemptRequeued {
                    run_id: next.id,
                    attempt_id: *attempt,
                    by: actor.id,
                    at: now,
                }],
                Vec::new(),
            ))
        }

        Command::AttemptFailed { attempt } => {
            require_user_or_system(actor)?;
            require_created(state)?;
            let attempt_state = state
                .attempts
                .get(attempt)
                .map(|record| record.status)
                .ok_or(DomainError::QueueConflict(format!(
                    "attempt {attempt} is unknown to this run"
                )))?;
            if attempt_state != AttemptStatus::Dispatching {
                return Err(DomainError::QueueConflict(format!(
                    "attempt {attempt} is not waiting on a dispatch (it is {attempt_state:?})"
                )));
            }
            let mut next = state.clone();
            let task_id = next.attempts.get(attempt).expect("checked above").task_id;
            if let Some(record) = next.attempts.get_mut(attempt) {
                record.status = AttemptStatus::Failed;
            }
            if let Some(task) = next.tasks.get_mut(&task_id) {
                task.status = TaskStatus::Failed;
            }
            // A refused send is confirmed finished: release its claims.
            release_settled_claims(&mut next);
            Ok(finish(
                next.clone(),
                vec![Event::AttemptFailed {
                    run_id: next.id,
                    attempt_id: *attempt,
                    by: actor.id,
                    at: now,
                }],
                Vec::new(),
            ))
        }

        Command::SubmitVerification {
            task,
            attempt,
            report,
        } => submit_verification(state, actor, *task, *attempt, report, now),

        Command::DisputeVerification { task, reason } => {
            if !matches!(
                actor.kind,
                ActorKind::User | ActorKind::Worker | ActorKind::Leader
            ) {
                return Err(DomainError::Forbidden);
            }
            require_created(state)?;
            if reason.trim().is_empty() {
                return Err(DomainError::InvalidCommand(
                    "a dispute needs a reason for the next reviewer".to_string(),
                ));
            }
            let task_state = state
                .tasks
                .get(task)
                .ok_or(DomainError::UnknownTask(*task))?;
            if task_state.status != TaskStatus::AwaitingVerification {
                return Err(DomainError::InvalidTransition(format!(
                    "task {task} is not awaiting verification (it is {:?}); \
                     only live verification can be disputed",
                    task_state.status,
                )));
            }
            let mut next = state.clone();
            let disputes_used = next
                .verification_disputes
                .get(task)
                .copied()
                .unwrap_or(0)
                .saturating_add(1);
            next.verification_disputes.insert(*task, disputes_used);
            Ok(finish(
                next.clone(),
                vec![Event::VerificationDisputed {
                    run_id: next.id,
                    task: *task,
                    reason: reason.clone(),
                    disputes_used,
                    decision: QualityDecision::Disputed,
                    by: actor.id,
                    at: now,
                }],
                Vec::new(),
            ))
        }

        Command::QueueRepairAttempt {
            attempt,
            task,
            predecessor,
            input,
            resources,
            max_parallel,
        } => queue_repair_attempt(
            state,
            actor,
            *attempt,
            *task,
            *predecessor,
            input,
            resources,
            *max_parallel,
            now,
        ),
    }
}

/// Judge one independent verifier verdict (P3 Task 2).
///
/// Only a verifier — or the user carrying a same-session override — may
/// submit. The leader, workers and the system are refused: the leader can
/// never force a pass, and a verdict never rewrites history.
fn submit_verification(
    state: &RunState,
    actor: &Actor,
    task: crate::TaskId,
    attempt: crate::AttemptId,
    report: &crate::VerificationReport,
    now: Timestamp,
) -> Result<Decision, DomainError> {
    if !matches!(actor.kind, ActorKind::Verifier | ActorKind::User) {
        return Err(DomainError::Forbidden);
    }
    require_created(state)?;
    let task_state = state
        .tasks
        .get(&task)
        .cloned()
        .ok_or(DomainError::UnknownTask(task))?;
    let attempt_task = state
        .attempts
        .get(&attempt)
        .map(|record| record.task_id)
        .ok_or(DomainError::QueueConflict(format!(
            "attempt {attempt} is unknown to this run"
        )))?;
    if attempt_task != task {
        return Err(DomainError::InvalidCommand(format!(
            "attempt {attempt} does not belong to task {task}"
        )));
    }
    // A Blocked task resumes here: after recovery the next verdict is a
    // fresh verification that preserves the repair rounds the block never
    // consumed. Any other non-open status is a stale verdict.
    if !matches!(
        task_state.status,
        TaskStatus::AwaitingVerification | TaskStatus::Blocked
    ) {
        return Err(DomainError::InvalidTransition(format!(
            "task {task} is not open for verification (it is {:?}); \
             stale verdict, reverify against the current seal",
            task_state.status,
        )));
    }
    check_report(&task_state, report)?;
    // Verifier independence: the default verifier runs in its own session.
    // A verdict from the worker's own session is refused unless the user
    // overrode it for this task — and only a user-carried submission may
    // carry that override.
    if let Some(session) = state.attempt_sessions.get(&attempt) {
        if session.thread_id == report.verifier_thread && !report.same_session_override {
            return Err(DomainError::Forbidden);
        }
    }
    if report.same_session_override && actor.kind != ActorKind::User {
        return Err(DomainError::Forbidden);
    }
    let routing = decide_quality(&task_state, report.outcome);
    let mut next = state.clone();
    // The candidate-producing attempt is done producing: the verdict is
    // the evidence its output exists. Marking it completed also releases
    // its resource claims below, freeing the repair or downstream work.
    if let Some(record) = next.attempts.get_mut(&attempt) {
        record.status = AttemptStatus::Completed;
    }
    match &routing {
        QualityDecision::Accepted => {
            if let Some(task_record) = next.tasks.get_mut(&task) {
                task_record.status = TaskStatus::Succeeded;
            }
            next.verified_outputs
                .insert(task, report.subject_hash.clone());
            next.repair_base.remove(&task);
        }
        QualityDecision::Repair { round } => {
            if let Some(task_record) = next.tasks.get_mut(&task) {
                // A resumption from Blocked reopens the task; a repair
                // verdict never leaves it parked as Blocked.
                task_record.status = TaskStatus::AwaitingVerification;
                task_record.repair_rounds_used = *round;
            }
            // Anchor one repair attempt to this predecessor; the repair
            // queue consumes the base, so a repair always chains from its
            // verdict.
            next.repair_base.insert(task, attempt);
        }
        QualityDecision::Blocked => {
            if let Some(task_record) = next.tasks.get_mut(&task) {
                task_record.status = TaskStatus::Blocked;
            }
            next.repair_base.remove(&task);
        }
        QualityDecision::AskLeader => {
            // Repair budget exhausted: park for leader analysis through the
            // normal scheduled gates. No silent retry, no pass, no bypass.
            // Set explicitly so a resumption from Blocked parks open rather
            // than staying Blocked.
            if let Some(task_record) = next.tasks.get_mut(&task) {
                task_record.status = TaskStatus::AwaitingVerification;
            }
            next.repair_base.remove(&task);
        }
        QualityDecision::Disputed => {}
    }
    release_settled_claims(&mut next);
    Ok(finish(
        next.clone(),
        vec![Event::VerificationSubmitted {
            run_id: next.id,
            task,
            attempt_id: attempt,
            outcome: report.outcome,
            decision: routing,
            by: actor.id,
            at: now,
        }],
        Vec::new(),
    ))
}

/// Queue one quality-repair attempt linked to its predecessor (P3 Task 2).
///
/// The same scheduling gate as [`queue_attempt`]: a running run, no holds,
/// a non-empty input, the active plan, satisfied dependencies, no covering
/// attempt, exclusive resources and a free concurrency slot. Additionally
/// the task must await verification and the predecessor must match the
/// repair base the failed verdict recorded; the base is consumed and the
/// link is stored, so a repair always chains from its verdict.
#[allow(clippy::too_many_arguments)]
fn queue_repair_attempt(
    state: &RunState,
    actor: &Actor,
    attempt: crate::AttemptId,
    task: crate::TaskId,
    predecessor: crate::AttemptId,
    input: &str,
    resources: &[String],
    max_parallel: usize,
    now: Timestamp,
) -> Result<Decision, DomainError> {
    require_user_or_system(actor)?;
    require_created(state)?;
    if state.control != RunControl::Running {
        return Err(DomainError::QueueConflict(format!(
            "the queue gate is closed while the run is {:?}",
            state.control
        )));
    }
    if !state.holds.is_empty() {
        return Err(DomainError::QueueConflict(
            "the queue gate is closed while the run is held".to_string(),
        ));
    }
    if input.trim().is_empty() {
        return Err(DomainError::InvalidCommand(
            "a queued attempt needs a non-empty input".to_string(),
        ));
    }
    let task_state = state
        .tasks
        .get(&task)
        .ok_or(DomainError::UnknownTask(task))?;
    let Some(active) = state.active_plan.as_ref() else {
        return Err(DomainError::QueueConflict(
            "the run has no active plan".to_string(),
        ));
    };
    if task_state.plan_hash != *active {
        return Err(DomainError::QueueConflict(format!(
            "task {task} is bound to a superseded plan"
        )));
    }
    if task_state.status != TaskStatus::AwaitingVerification {
        return Err(DomainError::QueueConflict(format!(
            "task {task} is not awaiting verification (it is {:?})",
            task_state.status
        )));
    }
    match state.repair_base.get(&task) {
        Some(base) if *base == predecessor => {}
        _ => {
            return Err(DomainError::QueueConflict(format!(
                "task {task} has no repair base for predecessor {predecessor}"
            )));
        }
    }
    let predecessor_task = state
        .attempts
        .get(&predecessor)
        .map(|record| record.task_id)
        .ok_or(DomainError::InvalidCommand(format!(
            "predecessor attempt {predecessor} is unknown to this run"
        )))?;
    if predecessor_task != task {
        return Err(DomainError::InvalidCommand(format!(
            "predecessor attempt {predecessor} does not belong to task {task}"
        )));
    }
    if state.attempts.contains_key(&attempt) {
        return Err(DomainError::QueueConflict(format!(
            "attempt {attempt} is already queued"
        )));
    }
    let blocked = blocking_dependencies(task_state, state);
    if !blocked.is_empty() {
        return Err(DomainError::QueueConflict(format!(
            "task {task} still waits on {} dependencies",
            blocked.len()
        )));
    }
    if let Some(covering) = covering_attempt(state, task) {
        return Err(DomainError::QueueConflict(format!(
            "task {task} is already covered by attempt {covering}"
        )));
    }
    let plan_hash = task_state.plan_hash.clone();
    let profile_id = task_state.profile_id;

    let mut next = state.clone();
    release_settled_claims(&mut next);
    let mut seen = BTreeSet::new();
    for key in resources {
        let key = key.trim();
        if key.is_empty() {
            return Err(DomainError::InvalidCommand(
                "a resource claim needs a non-empty key".to_string(),
            ));
        }
        if !seen.insert(key) {
            return Err(DomainError::InvalidCommand(format!(
                "resource {key} is listed twice for task {task}"
            )));
        }
        if let Some(owner) = next.resource_claims.get(key) {
            return Err(DomainError::QueueConflict(format!(
                "resource {key} is held by attempt {owner}"
            )));
        }
    }
    if live_attempt_count(&next) >= max_parallel {
        return Err(DomainError::QueueConflict(format!(
            "the concurrency bound ({max_parallel}) is reached"
        )));
    }

    let task_record = next.tasks.get_mut(&task).expect("task checked above");
    task_record.status = TaskStatus::Running;
    next.attempts.insert(
        attempt,
        Attempt {
            id: attempt,
            task_id: task,
            plan_hash,
            status: AttemptStatus::Queued,
            profile_id,
            created_at: now,
        },
    );
    next.attempt_inputs.insert(attempt, input.to_string());
    for key in resources {
        next.resource_claims.insert(key.trim().to_string(), attempt);
    }
    // Consume the base and store the chain link: this repair repairs its
    // predecessor, and no second repair can reuse the same base.
    next.repair_base.remove(&task);
    next.attempt_predecessor.insert(attempt, predecessor);
    Ok(finish(
        next.clone(),
        vec![Event::RepairAttemptQueued {
            run_id: next.id,
            attempt_id: attempt,
            task,
            predecessor,
            by: actor.id,
            at: now,
        }],
        Vec::new(),
    ))
}

/// Queue one attempt: the full scheduler gate as a pure decision. Every
/// refusal that a later tick could retry maps to [`DomainError::QueueConflict`];
/// only loud misconfiguration (empty input, empty or duplicated resource
/// keys, unknown task) surfaces as a different error.
#[allow(clippy::too_many_arguments)]
fn queue_attempt(
    state: &RunState,
    actor: &Actor,
    attempt: crate::AttemptId,
    task: crate::TaskId,
    input: &str,
    resources: &[String],
    max_parallel: usize,
    now: Timestamp,
) -> Result<Decision, DomainError> {
    require_user_or_system(actor)?;
    require_created(state)?;
    if state.control != RunControl::Running {
        return Err(DomainError::QueueConflict(format!(
            "the queue gate is closed while the run is {:?}",
            state.control
        )));
    }
    if !state.holds.is_empty() {
        return Err(DomainError::QueueConflict(
            "the queue gate is closed while the run is held".to_string(),
        ));
    }
    if input.trim().is_empty() {
        return Err(DomainError::InvalidCommand(
            "a queued attempt needs a non-empty input".to_string(),
        ));
    }
    let task_state = state
        .tasks
        .get(&task)
        .ok_or(DomainError::UnknownTask(task))?;
    let Some(active) = state.active_plan.as_ref() else {
        return Err(DomainError::QueueConflict(
            "the run has no active plan".to_string(),
        ));
    };
    if task_state.plan_hash != *active {
        return Err(DomainError::QueueConflict(format!(
            "task {task} is bound to a superseded plan"
        )));
    }
    if !matches!(task_state.status, TaskStatus::Pending | TaskStatus::Ready) {
        return Err(DomainError::QueueConflict(format!(
            "task {task} is not ready to queue (it is {:?})",
            task_state.status
        )));
    }
    if state.attempts.contains_key(&attempt) {
        return Err(DomainError::QueueConflict(format!(
            "attempt {attempt} is already queued"
        )));
    }
    let blocked = blocking_dependencies(task_state, state);
    if !blocked.is_empty() {
        return Err(DomainError::QueueConflict(format!(
            "task {task} still waits on {} dependencies",
            blocked.len()
        )));
    }
    if let Some(covering) = covering_attempt(state, task) {
        return Err(DomainError::QueueConflict(format!(
            "task {task} is already covered by attempt {covering}"
        )));
    }
    let plan_hash = task_state.plan_hash.clone();
    let profile_id = task_state.profile_id;

    // Sweep claims of confirmed-terminal attempts first so a stale entry
    // can never block a fresh queue; unknown owners keep their claims.
    let mut next = state.clone();
    release_settled_claims(&mut next);
    let mut seen = BTreeSet::new();
    for key in resources {
        let key = key.trim();
        if key.is_empty() {
            return Err(DomainError::InvalidCommand(
                "a resource claim needs a non-empty key".to_string(),
            ));
        }
        if !seen.insert(key) {
            return Err(DomainError::InvalidCommand(format!(
                "resource {key} is listed twice for task {task}"
            )));
        }
        if let Some(owner) = next.resource_claims.get(key) {
            return Err(DomainError::QueueConflict(format!(
                "resource {key} is held by attempt {owner}"
            )));
        }
    }
    if live_attempt_count(&next) >= max_parallel {
        return Err(DomainError::QueueConflict(format!(
            "the concurrency bound ({max_parallel}) is reached"
        )));
    }

    let task_record = next.tasks.get_mut(&task).expect("task checked above");
    task_record.status = TaskStatus::Running;
    next.attempts.insert(
        attempt,
        Attempt {
            id: attempt,
            task_id: task,
            plan_hash,
            status: AttemptStatus::Queued,
            profile_id,
            created_at: now,
        },
    );
    // A second queue for the same task can only happen after the first
    // attempt resolves (the gate above rejects live coverage), and this
    // map is insert-only: existing inputs are never rewritten.
    next.attempt_inputs.insert(attempt, input.to_string());
    for key in resources {
        next.resource_claims.insert(key.trim().to_string(), attempt);
    }
    Ok(finish(
        next.clone(),
        vec![Event::AttemptQueued {
            run_id: next.id,
            attempt_id: attempt,
            task,
            by: actor.id,
            at: now,
        }],
        Vec::new(),
    ))
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
