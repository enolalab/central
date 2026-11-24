use std::collections::BTreeSet;

use crate::revision::content_hash_of;
use crate::{
    admission, apply_usage, assess_impact, blocking_dependencies, check_report, covering_attempt,
    criteria_changed, decide_quality, describe_criteria_impact, inbox, is_confirmed_terminal,
    live_attempt_count, release_settled_claims, render_input, retry_used, sync_budget_holds,
    validate_plan, Actor, ActorKind, Admission, AnswerPayload, Attempt, AttemptId, AttemptStatus,
    Command, CommandEnvelope, ContentHash, DomainError, Effect, Event, HoldReason, InboxEntry,
    InboxKey, InboxKind, InboxOutcome, InboxStatus, PlanDocumentV1, PlanHash, PlanRevision,
    PlanStatus, QualityDecision, ReuseBindings, ReuseDecision, RunControl, RunState,
    StaleCandidate, Task, TaskId, TaskKind, TaskStatus, Timestamp, UsageAuditEntry, VerifySpec,
    WorkRequest, WorkRole,
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
            if state.pending_revision.is_some() {
                return Err(DomainError::InvalidTransition(
                    "a plan revision proposal is already pending; approve or supersede it before proposing an initial plan"
                        .to_string(),
                ));
            }
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
                let verify = task_verify(plan_task);
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

        Command::ReopenTask { task } => reopen_task(state, actor, *task, now),

        Command::DeferRetry {
            attempt,
            not_before,
            max_retries,
        } => defer_retry(state, actor, *attempt, *not_before, *max_retries, now),

        Command::SetLimits { limits } => {
            require_user(actor)?;
            require_created(state)?;
            limits.validate()?;
            let mut next = state.clone();
            let mut limits = limits.clone();
            limits.updated_by = Some(actor.id);
            next.usage.limits = limits;
            if next.usage.window_started_at.is_none() {
                next.usage.window_started_at = Some(now);
            }
            sync_budget_holds(&mut next, now);
            Ok(finish(
                next.clone(),
                vec![Event::LimitsSet {
                    run_id: next.id,
                    by: actor.id,
                    at: now,
                }],
                Vec::new(),
            ))
        }

        Command::ReconcileUsage { observation } => {
            require_user(actor)?;
            require_created(state)?;
            let mut next = state.clone();
            let changed = apply_usage(&mut next.usage, observation, now)?;
            sync_budget_holds(&mut next, now);
            let events = if changed {
                vec![Event::UsageReconciled {
                    run_id: next.id,
                    dedup_key: observation.dedup_key(),
                    by: actor.id,
                    at: now,
                }]
            } else {
                Vec::new()
            };
            Ok(finish(next.clone(), events, Vec::new()))
        }

        Command::ResolveUsageGap { missing_key, note } => {
            require_user(actor)?;
            require_created(state)?;
            if note.trim().is_empty() {
                return Err(DomainError::InvalidCommand(
                    "a gap resolution needs a note for the audit trail".to_string(),
                ));
            }
            let mut next = state.clone();
            let pending = next
                .usage
                .missing_final
                .remove(missing_key.as_str())
                .ok_or_else(|| {
                    DomainError::InvalidCommand(format!(
                        "no missing usage entry for key {missing_key}"
                    ))
                })?;
            // The missing figure stays missing: the audit records what was
            // decided, never a fabricated observed total.
            next.usage.audit.push(UsageAuditEntry {
                missing_key: pending.key.clone(),
                note: note.clone(),
                by: actor.id,
                at: now,
                resolved_observed: false,
            });
            next.usage.unknown = !next.usage.missing_final.is_empty();
            sync_budget_holds(&mut next, now);
            Ok(finish(
                next.clone(),
                vec![Event::UsageGapResolved {
                    run_id: next.id,
                    missing_key: pending.key,
                    by: actor.id,
                    at: now,
                }],
                Vec::new(),
            ))
        }

        Command::NoteFallback {
            task,
            old_attempt,
            new_attempt,
            profile_id,
            reason,
        } => {
            require_user_or_system(actor)?;
            require_created(state)?;
            if reason.trim().is_empty() {
                return Err(DomainError::InvalidCommand(
                    "a fallback needs a reason for the notification".to_string(),
                ));
            }
            let old_task = state.attempts.get(old_attempt).map(|record| record.task_id);
            let new_task = state.attempts.get(new_attempt).map(|record| record.task_id);
            match (old_task, new_task) {
                (Some(old), Some(new))
                    if old == *task && new == *task && old_attempt != new_attempt => {}
                _ => {
                    return Err(DomainError::InvalidCommand(format!(
                        "fallback attempts {old_attempt} -> {new_attempt} do not both belong to task {task}"
                    )));
                }
            }
            let next = state.clone();
            Ok(finish(
                next.clone(),
                vec![Event::FallbackTaken {
                    run_id: next.id,
                    task: *task,
                    old_attempt: *old_attempt,
                    new_attempt: *new_attempt,
                    profile: *profile_id,
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

        Command::ProposeRevision {
            plan,
            reason,
            criteria_explanation,
            semantic_affected,
        } => propose_revision(
            state,
            actor,
            plan,
            reason,
            criteria_explanation,
            semantic_affected,
            now,
        ),

        Command::ApproveRevision { plan_hash } => approve_revision(state, actor, plan_hash, now),

        Command::AssessReuse {
            attempt,
            decision,
            current,
        } => assess_reuse_decision(state, actor, *attempt, *decision, current, now),

        Command::RecordWorkerResult {
            attempt,
            input_hash,
            candidate_hash,
            criteria_hash,
        } => record_worker_result(
            state,
            actor,
            *attempt,
            input_hash,
            candidate_hash,
            criteria_hash,
            now,
        ),
        Command::RecordIntegration {
            task,
            attempt,
            candidate_commit,
            previous_head,
            new_head,
            check_evidence,
        } => record_integration(
            state,
            actor,
            *task,
            *attempt,
            candidate_commit,
            previous_head,
            new_head,
            check_evidence,
            now,
        ),
        Command::SetRunProfile { role, profile } => {
            set_run_profile(state, actor, *role, *profile, now)
        }
        Command::QueueLeaderReview {
            attempt,
            task,
            input,
        } => queue_leader_review(state, actor, *attempt, *task, input, now),
    }
}

/// Durably record one worker result (P4 durable worker-done command).
///
/// Worker role or user only: the leader can never file a result for its
/// own plan, and the verifier/system never complete work. The attempt
/// must be live (queued through running, including unknown outcome after
/// reconcile proves no duplicate send); a terminal attempt is a replay
/// or a foreign result and is refused. The criteria must match the
/// task's current criteria — a result sealed under moved criteria is
/// stale and must be re-sealed, never recorded. Recording parks the task
/// in `AwaitingVerification` and completes the attempt; it never marks
/// `Succeeded`, and a task that already succeeded on identical bytes
/// keeps its acceptance (same-bytes re-record is a no-op park).
fn record_worker_result(
    state: &RunState,
    actor: &Actor,
    attempt: AttemptId,
    input_hash: &ContentHash,
    candidate_hash: &ContentHash,
    criteria_hash: &ContentHash,
    now: Timestamp,
) -> Result<Decision, DomainError> {
    require_created(state)?;
    if !matches!(actor.kind, ActorKind::Worker | ActorKind::User) {
        return Err(DomainError::Forbidden);
    }
    let attempt_state = state
        .attempts
        .get(&attempt)
        .ok_or(DomainError::UnknownAttempt(attempt))?;
    let task_id = attempt_state.task_id;
    let task_state = state
        .tasks
        .get(&task_id)
        .ok_or(DomainError::UnknownTask(task_id))?;
    if !matches!(
        attempt_state.status,
        AttemptStatus::Queued
            | AttemptStatus::Dispatching
            | AttemptStatus::Running
            | AttemptStatus::WaitingForApproval
            | AttemptStatus::WaitingForInput
            | AttemptStatus::Reconciling
            | AttemptStatus::UnknownOutcome
    ) {
        return Err(DomainError::InvalidTransition(format!(
            "attempt {attempt} is {:?}; only a live attempt can record a worker result",
            attempt_state.status,
        )));
    }
    if criteria_hash != &crate::criteria_hash_for(&task_state.verify) {
        return Err(DomainError::InvalidTransition(format!(
            "stale worker result for task {task_id}: criteria changed since the seal; \
             criteria change only through an approved plan revision, re-seal and resubmit",
        )));
    }
    // The input binding itself is sealed by the application layer
    // (`seal_candidate` checks membership before issuing this command);
    // the hashes travel here so the event carries the exact seal.
    let mut next = state.clone();
    if let Some(record) = next.attempts.get_mut(&attempt) {
        record.status = AttemptStatus::Completed;
    }
    if let Some(task_record) = next.tasks.get_mut(&task_id) {
        let same_acceptance = task_record.status == TaskStatus::Succeeded
            && next
                .verified_outputs
                .get(&task_id)
                .is_some_and(|accepted| accepted == candidate_hash);
        if same_acceptance {
            // Same bytes re-recorded: the acceptance still stands.
        } else {
            if task_record.status == TaskStatus::Succeeded {
                next.verified_outputs.remove(&task_id);
            }
            task_record.status = TaskStatus::AwaitingVerification;
        }
    }
    release_settled_claims(&mut next);
    Ok(finish(
        next.clone(),
        vec![Event::WorkerResultRecorded {
            run_id: next.id,
            attempt_id: attempt,
            task: task_id,
            input_hash: input_hash.clone(),
            candidate_hash: candidate_hash.clone(),
            criteria_hash: criteria_hash.clone(),
            by: actor.id,
            at: now,
        }],
        Vec::new(),
    ))
}

/// Materialize a task's verify spec. Declared specs win; integration
/// tasks without one get a synthesized contract derived from their
/// approved checks, since their gate is the mechanical check run rather
/// than an agent verdict. Validation guarantees the spec exists and is
/// non-empty, so the synthesis below cannot invent criteria.
fn task_verify(plan_task: &crate::PlanTaskV1) -> VerifySpec {
    if let Some(verify) = plan_task.verify.clone() {
        return verify;
    }
    let (good_ref, check_count) = plan_task
        .integration
        .as_ref()
        .map(|spec| (spec.good_ref.clone(), spec.check_count()))
        .unwrap_or_default();
    VerifySpec {
        method: "integration".to_string(),
        criteria: vec![format!(
            "promote {good_ref} after {check_count} approved check(s)"
        )],
    }
}

/// Durably record one mechanical Git integration (P3 Task 5).
///
/// User or system only: the daemon driver and the owner endpoint are the
/// only writers; workers and verifiers never move the good ref. The
/// attempt must be Dispatching (claimed for this integration, never sent
/// to a harness) and bound to the active plan; the task must be an
/// integration task. Anything else is a contract violation, not a
/// retryable failure. Recording marks the attempt `Completed` and the
/// task `Succeeded`, files the merge commit as this task's verified
/// output (so dependents build on exactly what promoted), and emits the
/// full CAS receipt as evidence.
/// Set (or clear) the run-wide profile for one agent role. User only.
/// The choice never touches live work: attempts keep the profile they
/// dispatched with; only later dispatches resolve through the new
/// default. Clearing restores defer-when-unpinned behavior.
fn set_run_profile(
    state: &RunState,
    actor: &Actor,
    role: crate::ProfileRole,
    profile: Option<crate::ProfileId>,
    now: Timestamp,
) -> Result<Decision, DomainError> {
    require_user(actor)?;
    require_created(state)?;
    let mut next = state.clone();
    match role {
        crate::ProfileRole::Worker => next.worker_profile = profile,
        crate::ProfileRole::Leader => next.leader_profile = profile,
    }
    Ok(finish(
        next.clone(),
        vec![Event::RunProfileSet {
            run_id: next.id,
            role,
            profile,
            by: actor.id,
            at: now,
        }],
        Vec::new(),
    ))
}

/// Queue one leader review turn against a task. Unlike a worker queue,
/// the task keeps its status: a review advises on parked work without
/// moving it. The review never seals a worker candidate (worker-result
/// import refuses leader attempts); its output returns through handover
/// import. User only.
fn queue_leader_review(
    state: &RunState,
    actor: &Actor,
    attempt: crate::AttemptId,
    task: crate::TaskId,
    input: &str,
    now: Timestamp,
) -> Result<Decision, DomainError> {
    require_user(actor)?;
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
            "a queued leader review needs a non-empty input".to_string(),
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
    if state.revision_holds.contains(&task) {
        return Err(DomainError::QueueConflict(format!(
            "task {task} is held by a pending revision proposal; it queues again after the user decides"
        )));
    }
    if state.attempts.contains_key(&attempt) {
        return Err(DomainError::QueueConflict(format!(
            "attempt {attempt} is already queued"
        )));
    }
    let mut next = state.clone();
    release_settled_claims(&mut next);
    next.attempts.insert(
        attempt,
        Attempt {
            id: attempt,
            task_id: task,
            plan_hash: task_state.plan_hash.clone(),
            status: AttemptStatus::Queued,
            profile_id: None,
            created_at: now,
        },
    );
    // Queueing never rewrites an existing entry: new input waits for a
    // new attempt.
    next.attempt_inputs.insert(attempt, input.to_string());
    next.leader_attempts.insert(attempt);
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

#[allow(clippy::too_many_arguments)]
fn record_integration(
    state: &RunState,
    actor: &Actor,
    task: TaskId,
    attempt: AttemptId,
    candidate_commit: &str,
    previous_head: &str,
    new_head: &str,
    check_evidence: &[String],
    now: Timestamp,
) -> Result<Decision, DomainError> {
    require_created(state)?;
    if !matches!(actor.kind, ActorKind::User | ActorKind::System) {
        return Err(DomainError::Forbidden);
    }
    if candidate_commit.trim().is_empty() || new_head.trim().is_empty() {
        return Err(DomainError::InvalidCommand(
            "integration receipt needs a candidate commit and a new head".to_string(),
        ));
    }
    let record = state
        .attempts
        .get(&attempt)
        .ok_or(DomainError::UnknownAttempt(attempt))?;
    if record.task_id != task {
        return Err(DomainError::QueueConflict(format!(
            "attempt {attempt} does not belong to task {task}"
        )));
    }
    if record.status != AttemptStatus::Dispatching {
        return Err(DomainError::QueueConflict(format!(
            "attempt {attempt} is {:?}, not claimed for integration",
            record.status
        )));
    }
    if let Some(active) = state.active_plan.as_ref() {
        if &record.plan_hash != active {
            return Err(DomainError::QueueConflict(format!(
                "attempt {attempt} is bound to a stale plan; re-queue under the active plan"
            )));
        }
    }
    let task_state = state
        .tasks
        .get(&task)
        .ok_or(DomainError::UnknownTask(task))?;
    if task_state.kind != TaskKind::Integration {
        return Err(DomainError::InvalidCommand(format!(
            "task {task} is {:?}, not an integration task; mechanical promotion is refused",
            task_state.kind
        )));
    }
    let mut next = state.clone();
    if let Some(record) = next.attempts.get_mut(&attempt) {
        record.status = AttemptStatus::Completed;
    }
    if let Some(task_record) = next.tasks.get_mut(&task) {
        task_record.status = TaskStatus::Succeeded;
    }
    next.verified_outputs
        .insert(task, ContentHash::from_hex(new_head));
    release_settled_claims(&mut next);
    Ok(finish(
        next.clone(),
        vec![Event::IntegrationPromoted {
            run_id: next.id,
            task_id: task,
            attempt_id: attempt,
            candidate_commit: candidate_commit.to_string(),
            previous_head: previous_head.to_string(),
            new_head: new_head.to_string(),
            check_evidence: check_evidence.to_vec(),
            by: actor.id,
            at: now,
        }],
        Vec::new(),
    ))
}

/// Propose an immutable revision of the active plan (P3 Task 4).
///
/// The leader or the user proposes the full replacement document with
/// the reason it exists. A revision that moves verification criteria
/// must carry an explanation — without one the proposal is refused and
/// the run is untouched. The impact (changed nodes plus data
/// descendants, expanded only by the leader's semantic nodes) becomes
/// the pending hold set: impacted queued descendants wait for the
/// user's decision, running attempts continue untouched on their old
/// binding. A newer proposal supersedes the pending one, like plans.
#[allow(clippy::too_many_arguments)]
fn propose_revision(
    state: &RunState,
    actor: &Actor,
    plan: &PlanDocumentV1,
    reason: &str,
    criteria_explanation: &Option<String>,
    semantic_affected: &[TaskId],
    now: Timestamp,
) -> Result<Decision, DomainError> {
    require_created(state)?;
    if !matches!(actor.kind, ActorKind::Leader | ActorKind::User) {
        return Err(DomainError::Forbidden);
    }
    if state.control == RunControl::Stopped {
        return Err(DomainError::InvalidTransition(
            "cannot propose a revision for a stopped run".to_string(),
        ));
    }
    let Some(base_plan) = state.active_plan.clone() else {
        return Err(DomainError::InvalidTransition(
            "the run has no active plan; propose an initial plan first".to_string(),
        ));
    };
    if state.pending_plan.is_some() {
        return Err(DomainError::InvalidTransition(
            "an initial plan proposal is already pending; resolve it before proposing a revision"
                .to_string(),
        ));
    }
    if reason.trim().is_empty() {
        return Err(DomainError::InvalidCommand(
            "a revision proposal needs a reason for the user review".to_string(),
        ));
    }
    let hash = validate_plan(plan)?;
    if hash == base_plan {
        return Err(DomainError::InvalidTransition(
            "the revision is identical to the active plan".to_string(),
        ));
    }
    if matches!(
        state.plans.get(&hash).map(|entry| entry.status),
        Some(PlanStatus::Approved)
    ) {
        return Err(DomainError::InvalidTransition(
            "the revised plan is already approved".to_string(),
        ));
    }
    let base_document = state
        .plans
        .get(&base_plan)
        .map(|entry| entry.document.clone())
        .ok_or_else(|| DomainError::UnknownPlan(base_plan.clone()))?;
    if criteria_changed(&base_document, plan)? {
        let explained = criteria_explanation
            .as_deref()
            .is_some_and(|text| !text.trim().is_empty());
        if !explained {
            let changed = describe_criteria_impact(&base_document, plan)?.changed_tasks;
            return Err(DomainError::InvalidCommand(format!(
                "a revision changing verification criteria needs an explanation \
                 covering the requirement and verdict impact plus the reverify tasks \
                 (criteria changed for tasks: {changed:?})"
            )));
        }
    }
    let mut impact = assess_impact(&base_document, plan)?;
    let new_ids: BTreeSet<TaskId> = plan.tasks.iter().map(|task| task.id).collect();
    let mut semantic = BTreeSet::new();
    for node in semantic_affected {
        if !new_ids.contains(node) {
            return Err(DomainError::InvalidCommand(format!(
                "semantic node {node} is not part of the revised plan"
            )));
        }
        semantic.insert(*node);
    }
    impact = impact.expanded_with(semantic.clone());

    let mut next = state.clone();
    if let Some(previous) = next.pending_revision.take() {
        if previous.plan_hash != hash {
            if let Some(entry) = next.plans.get_mut(&previous.plan_hash) {
                entry.status = PlanStatus::Superseded;
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
    let impacted: Vec<TaskId> = impact.impacted().into_iter().collect();
    next.pending_revision = Some(crate::PendingRevision {
        base_plan: base_plan.clone(),
        plan_hash: hash.clone(),
        document: plan.clone(),
        reason: reason.to_string(),
        criteria_explanation: criteria_explanation.clone(),
        impact,
        semantic_affected: semantic,
        proposed_by: actor.id,
        proposed_at: now,
    });
    next.revision_holds = impacted.iter().copied().collect();
    Ok(finish(
        next.clone(),
        vec![Event::PlanRevisionProposed {
            run_id: next.id,
            base_plan,
            plan_hash: hash,
            impacted,
            by: actor.id,
            at: now,
        }],
        Vec::new(),
    ))
}

/// Activate the pending revision proposal (P3 Task 4).
///
/// User only. The target hash must be the pending revision — anything
/// else conflicts, so a stale tab can never activate unreviewed
/// content — and the revision must still be based on the active plan.
/// Every task present in the new document is rebound to it (status and
/// repair budget preserved); tasks whose data inputs the impact names
/// lose an acceptance they held, because verification must come first,
/// never a silent carry-over. Order-only changes invalidate nothing.
/// Running attempts keep their old binding: only tasks move.
fn approve_revision(
    state: &RunState,
    actor: &Actor,
    plan_hash: &PlanHash,
    now: Timestamp,
) -> Result<Decision, DomainError> {
    require_user(actor)?;
    require_created(state)?;
    let Some(pending) = state.pending_revision.clone() else {
        return Err(DomainError::InvalidTransition(
            "no revision proposal is pending".to_string(),
        ));
    };
    if pending.plan_hash != *plan_hash {
        return Err(DomainError::PlanHashMismatch);
    }
    if state.active_plan.as_ref() != Some(&pending.base_plan) {
        return Err(DomainError::InvalidTransition(format!(
            "the revision was proposed against {} but the active plan is now {}; \
             re-propose against the current revision",
            pending.base_plan,
            state
                .active_plan
                .as_ref()
                .map(PlanHash::as_str)
                .unwrap_or("<none>"),
        )));
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
        let approved = next
            .plans
            .get_mut(plan_hash)
            .expect("pending revision is stored");
        approved.status = PlanStatus::Approved;
        approved.approved_by = Some(actor.id);
        approved.approved_at = Some(now);
        approved.document.clone()
    };
    next.active_plan = Some(plan_hash.clone());
    // Rebase before clearing: the invalidation reads the pending impact.
    let invalidated = rebase_tasks_for_revision(&mut next, &document, plan_hash);
    next.pending_revision = None;
    next.revision_holds.clear();
    Ok(finish(
        next.clone(),
        vec![Event::PlanRevisionApproved {
            run_id: next.id,
            base_plan: pending.base_plan,
            plan_hash: plan_hash.clone(),
            invalidated,
            by: actor.id,
            at: now,
        }],
        Vec::new(),
    ))
}

/// Rebind every task present in the newly approved document to the new
/// revision, preserving status and repair budget. Fresh tasks enter
/// `Pending`; tasks the impact names lose a held acceptance (verified
/// output cleared, parked for verification first). Returns the
/// invalidated tasks in stable order.
fn rebase_tasks_for_revision(
    next: &mut RunState,
    document: &PlanDocumentV1,
    plan_hash: &PlanHash,
) -> Vec<TaskId> {
    let impacted: BTreeSet<TaskId> = next
        .pending_revision
        .as_ref()
        .map(|pending| pending.impact.impacted())
        .unwrap_or_default();
    let mut invalidated = Vec::new();
    for plan_task in &document.tasks {
        let verify = task_verify(plan_task);
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
                task.plan_hash = plan_hash.clone();
                task.kind = plan_task.kind.clone();
                task.objective = plan_task.objective.clone();
                task.depends_on = plan_task.depends_on.clone();
                task.data_depends_on = plan_task.data_depends_on.clone();
                task.verify = verify;
                task.profile_id = plan_task.profile_id;
                task.max_repair_rounds = plan_task.max_repair_rounds;
                if impacted.contains(&plan_task.id) && task.status == TaskStatus::Succeeded {
                    task.status = TaskStatus::AwaitingVerification;
                    next.verified_outputs.remove(&plan_task.id);
                    invalidated.push(plan_task.id);
                }
            }
        }
    }
    invalidated
}

/// Conclude the reuse assessment of one quarantined stale result
/// (P3 Task 4).
///
/// User or leader. `NeedsAssessment` is the initial state, never a
/// conclusion. `ReuseUnchangedEvidence` is refused unless every
/// binding still matches the quarantined record — and it restores a
/// parked acceptance so the evidence is reused without
/// re-verification. `Repair` anchors a repair base on the assessed
/// predecessor so follow-up chains from it. `VerifyAgain` parks the
/// result for a verifier verdict under the current bindings.
/// `Discard` of a parked task without newer evidence reopens it for
/// fresh implementation. Nothing here ever rewrites the quarantined
/// provenance, and a task that already succeeded under the active
/// revision is never overwritten.
#[allow(clippy::too_many_arguments)]
fn assess_reuse_decision(
    state: &RunState,
    actor: &Actor,
    attempt: crate::AttemptId,
    decision: ReuseDecision,
    current: &ReuseBindings,
    now: Timestamp,
) -> Result<Decision, DomainError> {
    if !matches!(actor.kind, ActorKind::User | ActorKind::Leader) {
        return Err(DomainError::Forbidden);
    }
    require_created(state)?;
    if decision == ReuseDecision::NeedsAssessment {
        return Err(DomainError::InvalidCommand(
            "a reuse assessment must conclude with VerifyAgain, Repair, Discard or ReuseUnchangedEvidence; NeedsAssessment is the initial state only"
                .to_string(),
        ));
    }
    let candidate = state
        .stale_candidates
        .get(&attempt)
        .cloned()
        .ok_or_else(|| {
            DomainError::InvalidCommand(format!(
                "no quarantined result for attempt {attempt}; only stale results from a superseded revision can be assessed"
            ))
        })?;
    let task_state = state
        .tasks
        .get(&candidate.task_id)
        .cloned()
        .ok_or(DomainError::UnknownTask(candidate.task_id))?;
    let retained = Some(&task_state.plan_hash) == state.active_plan.as_ref();

    let mut next = state.clone();
    match decision {
        ReuseDecision::NeedsAssessment => {
            return Err(DomainError::InvalidCommand(
                "NeedsAssessment is the initial state, not a conclusion".to_string(),
            ));
        }
        ReuseDecision::ReuseUnchangedEvidence => {
            if !candidate.matches(current) {
                return Err(DomainError::InvalidCommand(format!(
                    "stale result for task {} has moving bindings; reuse needs VerifyAgain, not a silent accept",
                    candidate.task_id,
                )));
            }
            if !retained {
                return Err(DomainError::InvalidCommand(format!(
                    "task {} was removed by the revision; its result cannot be reused",
                    candidate.task_id,
                )));
            }
            // A task that already succeeded under the active revision
            // keeps its newer acceptance; otherwise the parked task
            // succeeds on the reused evidence.
            if matches!(
                task_state.status,
                TaskStatus::Pending | TaskStatus::AwaitingVerification
            ) {
                if let Some(task) = next.tasks.get_mut(&candidate.task_id) {
                    task.status = TaskStatus::Succeeded;
                }
                next.verified_outputs
                    .insert(candidate.task_id, candidate.candidate_hash.clone());
            }
        }
        ReuseDecision::VerifyAgain => {
            // The task waits for a verifier verdict under the current
            // bindings; the quarantine judge accepts exactly that.
        }
        ReuseDecision::Repair => {
            // Anchor one repair attempt on the assessed predecessor, so
            // follow-up chains from the quarantined result instead of
            // silently re-implementing. Only a parked task can anchor.
            if task_state.status == TaskStatus::AwaitingVerification {
                next.repair_base.insert(candidate.task_id, attempt);
            }
        }
        ReuseDecision::Discard => {
            // A parked task with no newer acceptance reopens for fresh
            // implementation; anything newer is left alone.
            if task_state.status == TaskStatus::AwaitingVerification
                && !next.verified_outputs.contains_key(&candidate.task_id)
            {
                if let Some(task) = next.tasks.get_mut(&candidate.task_id) {
                    task.status = TaskStatus::Ready;
                }
            }
        }
    }
    if let Some(record) = next.stale_candidates.get_mut(&attempt) {
        record.reuse = decision;
        record.verified_against = Some(current.clone());
    }
    Ok(finish(
        next.clone(),
        vec![Event::ReuseAssessed {
            run_id: next.id,
            attempt_id: attempt,
            decision,
            by: actor.id,
            at: now,
        }],
        Vec::new(),
    ))
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
    let attempt_record = state
        .attempts
        .get(&attempt)
        .expect("attempt membership checked above")
        .clone();
    // An attempt bound to a superseded revision never judges against the
    // active plan: its result is quarantined with the original
    // provenance, and reuse waits for an explicit assessment.
    if state
        .active_plan
        .as_ref()
        .is_some_and(|active| attempt_record.plan_hash != *active)
    {
        return record_stale_result(
            state,
            actor,
            task,
            attempt,
            &attempt_record,
            &task_state,
            report,
            now,
        );
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
    check_verifier_independence(state, actor, attempt, report)?;
    Ok(apply_verdict_routing(
        state.clone(),
        actor,
        task,
        attempt,
        &task_state,
        report,
        now,
    ))
}

/// Verifier independence: the default verifier runs in its own session.
/// A verdict from the worker's own session is refused unless the user
/// overrode it for this task — and only a user-carried submission may
/// carry that override.
fn check_verifier_independence(
    state: &RunState,
    actor: &Actor,
    attempt: crate::AttemptId,
    report: &crate::VerificationReport,
) -> Result<(), DomainError> {
    if let Some(session) = state.attempt_sessions.get(&attempt) {
        if session.thread_id == report.verifier_thread && !report.same_session_override {
            return Err(DomainError::Forbidden);
        }
    }
    if report.same_session_override && actor.kind != ActorKind::User {
        return Err(DomainError::Forbidden);
    }
    Ok(())
}

/// Apply one judged verdict to its task: the shared tail of the live
/// verification path and the quarantine re-verification path. The
/// verdict is the evidence the candidate-producing attempt's output
/// exists, so the attempt completes and its resource claims release,
/// freeing the repair or downstream work.
fn apply_verdict_routing(
    next: RunState,
    actor: &Actor,
    task: crate::TaskId,
    attempt: crate::AttemptId,
    task_state: &Task,
    report: &crate::VerificationReport,
    now: Timestamp,
) -> Decision {
    let routing = decide_quality(task_state, report.outcome);
    let mut next = next;
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
    finish(
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
    )
}

/// Quarantine the result of an attempt bound to a superseded revision
/// (P3 Task 4).
///
/// The result is stored with its original plan binding — never
/// relabeled — and waits for an explicit [`Command::AssessReuse`].
/// The old attempt completes, and a running task parks for
/// verification; a task that already succeeded under the active
/// revision is never overwritten. When the quarantined result was
/// assessed as [`ReuseDecision::VerifyAgain`] and this verdict cites
/// exactly the assessed current bindings, the verdict concludes
/// normally under the current criteria: verification first, not a
/// silent carry-over and not a whole re-implementation.
#[allow(clippy::too_many_arguments)]
fn record_stale_result(
    state: &RunState,
    actor: &Actor,
    task: crate::TaskId,
    attempt: crate::AttemptId,
    attempt_record: &Attempt,
    task_state: &Task,
    report: &crate::VerificationReport,
    now: Timestamp,
) -> Result<Decision, DomainError> {
    let mut next = state.clone();
    if !next.stale_candidates.contains_key(&attempt) {
        let input_hash = next
            .attempt_inputs
            .get(&attempt)
            .map(|input| content_hash_of(input.as_bytes()))
            .unwrap_or_else(|| ContentHash::from_hex(""));
        next.stale_candidates.insert(
            attempt,
            StaleCandidate {
                attempt_id: attempt,
                task_id: task,
                bound_plan: attempt_record.plan_hash.clone(),
                input_hash,
                candidate_hash: report.subject_hash.clone(),
                criteria_hash: report.criteria_hash.clone(),
                environment: report.environment.clone(),
                reuse: ReuseDecision::NeedsAssessment,
                verified_against: None,
            },
        );
    }
    let assessed = next
        .stale_candidates
        .get(&attempt)
        .expect("quarantined above")
        .clone();

    // Verification-first follow-up: the assessment authorized exactly
    // one re-verification of these bytes under the current bindings.
    if task_state.status == TaskStatus::AwaitingVerification
        && assessed.reuse == ReuseDecision::VerifyAgain
        && Some(&task_state.plan_hash) == next.active_plan.as_ref()
    {
        if let Some(current) = assessed.verified_against.clone() {
            // All four bindings must still hold: the input is re-rendered
            // from the current snapshot, so a shift between assessment
            // and re-verdict stays quarantined instead of concluding.
            let fresh_input = content_hash_of(render_input(state, task_state).as_bytes());
            if report.subject_hash == assessed.candidate_hash
                && report.criteria_hash == current.criteria_hash
                && report.environment == current.environment
                && fresh_input == current.input_hash
            {
                check_report(task_state, report)?;
                check_verifier_independence(state, actor, attempt, report)?;
                return Ok(apply_verdict_routing(
                    next, actor, task, attempt, task_state, report, now,
                ));
            }
        }
    }

    if !is_confirmed_terminal(attempt_record.status) {
        if let Some(record) = next.attempts.get_mut(&attempt) {
            record.status = AttemptStatus::Completed;
        }
    }
    if task_state.status == TaskStatus::Running {
        if let Some(task_record) = next.tasks.get_mut(&task) {
            task_record.status = TaskStatus::AwaitingVerification;
        }
    }
    release_settled_claims(&mut next);
    Ok(finish(
        next.clone(),
        vec![Event::StaleCandidateRecorded {
            run_id: next.id,
            attempt_id: attempt,
            task,
            bound_plan: attempt_record.plan_hash.clone(),
            candidate: report.subject_hash.clone(),
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
    if state.revision_holds.contains(&task) {
        return Err(DomainError::QueueConflict(format!(
            "task {task} is held by a pending revision proposal; it repairs after the user decides"
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
    budget_gate(state, WorkRole::Repair, now)?;
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

/// Reopen a task parked as failed so a refused (or retry-exhausted)
/// send stays recoverable. Only a failed task with no live or unknown
/// attempt covering it may reopen; anything else is a conflict a later
/// tick cannot fix by reopening.
fn reopen_task(
    state: &RunState,
    actor: &Actor,
    task: crate::TaskId,
    now: Timestamp,
) -> Result<Decision, DomainError> {
    require_user_or_system(actor)?;
    require_created(state)?;
    let task_state = state
        .tasks
        .get(&task)
        .ok_or(DomainError::UnknownTask(task))?;
    if task_state.status != TaskStatus::Failed {
        return Err(DomainError::InvalidTransition(format!(
            "task {task} is not failed (it is {:?}); only failed tasks reopen",
            task_state.status,
        )));
    }
    if let Some(covering) = covering_attempt(state, task) {
        return Err(DomainError::QueueConflict(format!(
            "task {task} is already covered by attempt {covering}"
        )));
    }
    let mut next = state.clone();
    if let Some(record) = next.tasks.get_mut(&task) {
        record.status = TaskStatus::Ready;
    }
    Ok(finish(
        next.clone(),
        vec![Event::TaskReopened {
            run_id: next.id,
            task,
            by: actor.id,
            at: now,
        }],
        Vec::new(),
    ))
}

/// Defer one safe retry of a definitely-unsent attempt. The retry budget
/// is bounded by the command's carried `max_retries` (the configured
/// policy bound, default 3): an exhausted attempt parks as failed
/// (recoverable via `ReopenTask`, same as a refused send) instead of
/// retrying forever.
fn defer_retry(
    state: &RunState,
    actor: &Actor,
    attempt: crate::AttemptId,
    not_before: Timestamp,
    max_retries: u32,
    now: Timestamp,
) -> Result<Decision, DomainError> {
    require_user_or_system(actor)?;
    require_created(state)?;
    if not_before < now {
        return Err(DomainError::InvalidCommand(
            "a deferred retry must wait until now or later".to_string(),
        ));
    }
    let attempt_state = state
        .attempts
        .get(&attempt)
        .map(|record| record.status)
        .ok_or(DomainError::QueueConflict(format!(
            "attempt {attempt} is unknown to this run"
        )))?;
    if attempt_state != AttemptStatus::Dispatching {
        return Err(DomainError::QueueConflict(format!(
            "attempt {attempt} is not waiting on a dispatch (it is {attempt_state:?})"
        )));
    }
    let used = retry_used(state, attempt);
    if used >= max_retries {
        // Retry budget exhausted: park recoverably, like a refused send.
        let mut next = state.clone();
        let task_id = next.attempts.get(&attempt).expect("checked above").task_id;
        if let Some(record) = next.attempts.get_mut(&attempt) {
            record.status = AttemptStatus::Failed;
        }
        if let Some(task) = next.tasks.get_mut(&task_id) {
            task.status = TaskStatus::Failed;
        }
        release_settled_claims(&mut next);
        return Ok(finish(
            next.clone(),
            vec![Event::AttemptFailed {
                run_id: next.id,
                attempt_id: attempt,
                by: actor.id,
                at: now,
            }],
            Vec::new(),
        ));
    }
    let mut next = state.clone();
    if let Some(record) = next.attempts.get_mut(&attempt) {
        record.status = AttemptStatus::Queued;
    }
    next.retry_used.insert(attempt, used + 1);
    next.retry_not_before.insert(attempt, not_before);
    Ok(finish(
        next.clone(),
        vec![Event::AttemptRetryDeferred {
            run_id: next.id,
            attempt_id: attempt,
            not_before,
            retries_used: used + 1,
            by: actor.id,
            at: now,
        }],
        Vec::new(),
    ))
}

/// The shared-budget gate every queue path enforces under the commit
/// CAS: unknown usage (with cost limits) or an exceeded limit defers
/// the attempt, including leader and verifier attempts.
fn budget_gate(state: &RunState, role: WorkRole, now: Timestamp) -> Result<(), DomainError> {
    match admission(state, &WorkRequest { role }, now) {
        Admission::Allowed => Ok(()),
        Admission::Held(HoldReason::BudgetExceeded) => Err(DomainError::QueueConflict(
            "the shared budget is exhausted; running attempts finish, nothing new queues"
                .to_string(),
        )),
        Admission::Held(HoldReason::UsageUnknown) => Err(DomainError::QueueConflict(
            "usage is unknown and the run carries cost limits; reconcile or resolve the gap first"
                .to_string(),
        )),
        Admission::Held(other) => Err(DomainError::QueueConflict(format!(
            "the run is held ({other:?}); reconcile the hold first"
        ))),
    }
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
    if state.revision_holds.contains(&task) {
        return Err(DomainError::QueueConflict(format!(
            "task {task} is held by a pending revision proposal; it queues again after the user decides"
        )));
    }
    if !matches!(task_state.status, TaskStatus::Pending | TaskStatus::Ready) {
        return Err(DomainError::QueueConflict(format!(
            "task {task} is not ready to queue (it is {:?})",
            task_state.status
        )));
    }
    budget_gate(state, WorkRole::Worker, now)?;
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
