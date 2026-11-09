//! Plan revision proposals and stale-candidate reuse (P3 Task 4).
//!
//! The domain owns the durable transitions (`ProposeRevision`,
//! `ApproveRevision`, `AssessReuse` and the quarantine of old-binding
//! results inside `SubmitVerification`); this module builds the pieces
//! those commands need without ever calling a model:
//!
//! * [`criteria_explanation_text`] renders the requirement/verdict
//!   impact and the reverify list a revision proposal must carry when
//!   verification criteria move.
//! * [`current_bindings`] pins the bindings a quarantined candidate is
//!   assessed against: the scheduler-rendered input under the active
//!   plan, the same candidate bytes, the current criteria and the
//!   pinned environment the re-verification runs in.
//! * [`recommend_reuse`] routes one quarantined candidate through the
//!   domain matrix: removed tasks discard, fully matching bindings
//!   reuse unchanged evidence, failed candidates repair, and anything
//!   else verifies again — never a silent accept, never an up-front
//!   re-implementation.
//! * [`reverify_first`] lists the parked tasks whose stale results
//!   verification must see before any new implementation queues.
//! * [`open_review_tasks`] lists pending proposal/approval/verification/
//!   assessment steps so revision follow-ups surface review work first.

use central_domain::{
    assess_reuse, criteria_hash_for, CriteriaImpact, ReuseBindings, ReuseDecision, RunState,
    StaleCandidate, Task, TaskId,
};

use crate::artifacts::content_hash;
use crate::contracts::AppError;
use crate::scheduler::Scheduler;

/// Render the criteria explanation a revision proposal carries: the
/// requirement impact, the verdict impact and the tasks that must
/// re-verify, so the user reviews one complete statement.
pub fn criteria_explanation_text(impact: &CriteriaImpact) -> String {
    let mut text = String::new();
    text.push_str("Requirement impact: ");
    text.push_str(&impact.requirement_impact);
    text.push('\n');
    text.push_str("Verdict impact: ");
    text.push_str(&impact.verdict_impact);
    text.push('\n');
    text.push_str("Reverify tasks: ");
    let list: Vec<String> = impact
        .reverify_tasks
        .iter()
        .map(ToString::to_string)
        .collect();
    text.push_str(&list.join(", "));
    text
}

/// Pin the current bindings for one quarantined candidate: the exact
/// scheduler input under the active plan, the same candidate bytes,
/// the task's current criteria hash and the pinned environment the
/// re-verification will run in. The caller supplies the environment;
/// everything else is read off the loaded run.
pub fn current_bindings(
    state: &RunState,
    task: &Task,
    candidate: &StaleCandidate,
    environment: &str,
) -> Result<ReuseBindings, AppError> {
    if candidate.task_id != task.id {
        return Err(AppError::Validation(format!(
            "candidate for task {} does not belong to task {}",
            candidate.task_id, task.id,
        )));
    }
    if environment.trim().is_empty() {
        return Err(AppError::Validation(
            "re-verification needs a pinned environment reference".to_string(),
        ));
    }
    Ok(ReuseBindings {
        input_hash: content_hash(Scheduler::render_input(state, task).as_bytes()),
        candidate_hash: candidate.candidate_hash.clone(),
        criteria_hash: criteria_hash_for(&task.verify),
        environment: environment.to_string(),
    })
}

/// Route one quarantined candidate through the domain reuse matrix.
/// A removed task discards; fully matching bindings reuse the
/// evidence; a previously failed candidate repairs; anything else
/// verifies again first.
pub fn recommend_reuse(
    state: &RunState,
    candidate: &StaleCandidate,
    current: &ReuseBindings,
) -> ReuseDecision {
    let retained = state
        .tasks
        .get(&candidate.task_id)
        .is_some_and(|task| Some(&task.plan_hash) == state.active_plan.as_ref());
    let failed_before = state.tasks.get(&candidate.task_id).is_some_and(|task| {
        task.repair_rounds_used > 0 || state.repair_base.contains_key(&task.id)
    });
    assess_reuse(candidate, current, retained, failed_before)
}

/// Tasks parked for verification whose stale results must be judged
/// before any new implementation queues: changed data inputs
/// invalidate acceptances, and the follow-up is verification first.
pub fn reverify_first(state: &RunState) -> Vec<TaskId> {
    let mut tasks: Vec<TaskId> = state
        .stale_candidates
        .values()
        .filter(|candidate| {
            matches!(
                candidate.reuse,
                ReuseDecision::NeedsAssessment | ReuseDecision::VerifyAgain
            )
        })
        .map(|candidate| candidate.task_id)
        .filter(|task| {
            state.tasks.get(task).is_some_and(|record| {
                record.status == central_domain::TaskStatus::AwaitingVerification
            })
        })
        .collect();
    tasks.sort();
    tasks.dedup();
    tasks
}

/// Pending review steps (proposal, approval, verification or
/// assessment kinds): revision follow-ups schedule these before new
/// build work.
pub fn open_review_tasks(state: &RunState) -> Vec<TaskId> {
    let mut tasks: Vec<TaskId> = state
        .tasks
        .values()
        .filter(|task| task.kind.is_review() && task.is_ready())
        .map(|task| task.id)
        .collect();
    tasks.sort();
    tasks
}
