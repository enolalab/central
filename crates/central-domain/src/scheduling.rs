//! Scheduling selection over run snapshots (P3 Task 1).
//!
//! [`eligible_tasks`] is the pure gate the daemon and the scheduler tick
//! share: which tasks may start a new attempt right now. A task is
//! eligible when the run is created and `Running`, no hold closes the
//! gate, the task is `Pending`/`Ready` on the active plan, no live or
//! unknown-outcome attempt already covers it, every order predecessor is
//! `Succeeded`, and every data predecessor is `Succeeded` with a verified
//! output. Ties break by stable [`TaskId`] order (the tasks map is a
//! `BTreeMap`), so two ticks from the same snapshot pick the same order.
//!
//! Pure: no IO, no mutation, deterministic for a given [`RunState`].

use crate::dependency::{blocking_dependencies, dependencies_of, verified_output};
use crate::{
    AttemptId, AttemptStatus, DependencyKind, RunControl, RunState, Task, TaskId, Timestamp,
};

/// The exact input one attempt starts with: the task objective plus the
/// verified outputs of its data prerequisites, pinned by hash.
/// Deterministic for a given snapshot. The scheduler renders this when
/// queueing, and the reuse quarantine re-renders it at re-verdict time so
/// an input shift between assessment and re-verdict cannot conclude as
/// success: both sides share this one function, so they agree by
/// construction.
pub fn render_input(state: &RunState, task: &Task) -> String {
    let mut input = format!("implement task {}: {}", task.id, task.objective);
    for edge in dependencies_of(task) {
        match edge.kind {
            DependencyKind::Order => {
                input.push_str(&format!(
                    "\norder prerequisite: task {} succeeded",
                    edge.predecessor
                ));
            }
            DependencyKind::Data => match verified_output(state, edge.predecessor) {
                Some(hash) => {
                    input.push_str(&format!(
                        "\nverified input: task {} output {}",
                        edge.predecessor, hash
                    ));
                }
                None => {
                    input.push_str(&format!(
                        "\nverified input: task {} (pending verification)",
                        edge.predecessor
                    ));
                }
            },
        }
    }
    input
}

/// Attempts that occupy a concurrency slot: started but not terminally
/// resolved. `UnknownOutcome` is deliberately excluded: an unknown branch
/// holds its descendants and resources, but independent work must still
/// proceed once the run resumes.
pub fn is_live_status(status: AttemptStatus) -> bool {
    matches!(
        status,
        AttemptStatus::Queued
            | AttemptStatus::Dispatching
            | AttemptStatus::Running
            | AttemptStatus::WaitingForApproval
            | AttemptStatus::WaitingForInput
            | AttemptStatus::Reconciling
    )
}

/// A confirmed terminal attempt: the harness outcome is known, so the
/// attempt's resource claims may be released. `UnknownOutcome` is not
/// confirmed — the claim stays held until reconciliation proves the
/// attempt finished (P3/T6 owns that proof).
pub fn is_confirmed_terminal(status: AttemptStatus) -> bool {
    matches!(
        status,
        AttemptStatus::Completed | AttemptStatus::Failed | AttemptStatus::Cancelled
    )
}

/// The non-terminal attempt covering a task, if any. Covers live attempts
/// and `UnknownOutcome`: a task with an unresolved attempt never opens a
/// second one, and new input never rewrites the running attempt's input.
pub fn covering_attempt(state: &RunState, task: TaskId) -> Option<AttemptId> {
    state
        .attempts
        .values()
        .filter(|attempt| attempt.task_id == task)
        .find(|attempt| {
            is_live_status(attempt.status) || attempt.status == AttemptStatus::UnknownOutcome
        })
        .map(|attempt| attempt.id)
}

/// Live attempt count: the concurrency-slot usage of this snapshot.
pub fn live_attempt_count(state: &RunState) -> usize {
    state
        .attempts
        .values()
        .filter(|attempt| is_live_status(attempt.status))
        .count()
}

/// Release claims whose owner reached a confirmed terminal attempt.
/// Unknown-outcome owners keep their claims. Returns whether anything
/// was released.
pub fn release_settled_claims(state: &mut RunState) -> bool {
    let settled: Vec<AttemptId> = state
        .resource_claims
        .values()
        .copied()
        .filter(|owner| {
            state
                .attempts
                .get(owner)
                .is_some_and(|attempt| is_confirmed_terminal(attempt.status))
        })
        .collect();
    if settled.is_empty() {
        return false;
    }
    state
        .resource_claims
        .retain(|_, owner| !settled.contains(owner));
    true
}

/// Tasks that may start a new attempt now, in stable [`TaskId`] order.
/// The timestamp is accepted for future activation windows and is not
/// consulted yet; passing it keeps the daemon call shape stable.
pub fn eligible_tasks(state: &RunState, now: Timestamp) -> Vec<TaskId> {
    if !state.created || state.control != RunControl::Running {
        return Vec::new();
    }
    if !state.holds.is_empty() {
        return Vec::new();
    }
    eligible_tasks_ignoring_gate(state, now)
}

/// Tasks that would be eligible if the run gate (control/holds) were
/// open. Used for honest paused/held telemetry: a closed gate holds back
/// real work, and the deferred figure must count that held-back work
/// rather than report zero.
///
/// A pending revision proposal holds its impacted tasks regardless of
/// the gate: held tasks never appear here, so neither the scheduler
/// tick nor the dispatcher can queue or send new work for them while
/// the user decides. Queued attempts wait; running attempts continue.
pub fn eligible_tasks_ignoring_gate(state: &RunState, now: Timestamp) -> Vec<TaskId> {
    let _ = now;
    if !state.created {
        return Vec::new();
    }
    let Some(active) = state.active_plan.as_ref() else {
        return Vec::new();
    };
    state
        .tasks
        .values()
        .filter(|task| {
            task.plan_hash == *active
                && matches!(
                    task.status,
                    crate::TaskStatus::Pending | crate::TaskStatus::Ready
                )
                && !state.revision_holds.contains(&task.id)
                && covering_attempt(state, task.id).is_none()
                && blocking_dependencies(task, state).is_empty()
        })
        .map(|task| task.id)
        .collect()
}
