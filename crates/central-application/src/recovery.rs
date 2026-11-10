//! Workflow recovery and side-effect reconciliation (P3 Task 6).
//!
//! [`inspect_run`] is the passive boot report for one run: ownership,
//! session bindings, inbox, history and artifacts are only read, never
//! touched. [`Runtime::reconcile_attempt`] stores gateway reconciliation
//! evidence durably without starting inference: it never calls
//! `open`/`send`/`answer`. Resuming the *same* session is only safe when
//! [`can_resume_session`] holds (binding, input and profile match with no
//! conflicting evidence); otherwise the old outcome must be known first and
//! a new attempt continues with a handover. [`blocked_by_unknown`] is the
//! set of descendants an unknown branch holds; independent work is free to
//! run after an explicit run resume unless a global budget hold closes the
//! gate.

use std::collections::{BTreeMap, BTreeSet};

use central_domain::{
    Actor, AttemptId, AttemptStatus, Command, InboxStatus, RunState, TaskId, VerificationOutcome,
};

use crate::contracts::{AppError, ExecutionRef, ReconcileEvidence, RunRecovery, SessionRef};
use crate::Runtime;

/// Attempts whose outcome is not yet known. Their descendants wait and
/// their resource claims stay held; nothing here is terminal.
fn is_unknown(status: AttemptStatus) -> bool {
    matches!(
        status,
        AttemptStatus::UnknownOutcome | AttemptStatus::Reconciling
    )
}

/// Attempts that still own a live harness binding.
fn is_live(status: AttemptStatus) -> bool {
    matches!(
        status,
        AttemptStatus::Running
            | AttemptStatus::WaitingForApproval
            | AttemptStatus::WaitingForInput
            | AttemptStatus::Reconciling
            | AttemptStatus::UnknownOutcome
    )
}

/// Passive boot report for one run. Reads ownership, session history,
/// inbox, observations and artifacts; performs zero external calls and
/// stores nothing. Every finding names an explicit user action — recovery
/// itself never dispatches, answers, promotes or resumes.
pub fn inspect_run(state: &RunState) -> RunRecovery {
    let mut report = RunRecovery {
        run_id: state.id,
        findings: Vec::new(),
        required_actions: Vec::new(),
    };

    for attempt in state.attempts.values() {
        if is_unknown(attempt.status) {
            report.findings.push(format!(
                "attempt {} of task {}: outcome unknown; its descendants and resources are held",
                attempt.id, attempt.task_id,
            ));
            report.required_actions.push(format!(
                "reconcile attempt {} (`central reconcile --run {} --attempt {}`), then queue a new attempt with a handover once the old outcome is known",
                attempt.id, state.id, attempt.id,
            ));
        } else if attempt.status == AttemptStatus::Dispatching {
            report.findings.push(format!(
                "attempt {} of task {}: still dispatching at boot; it will converge to unknown on recover",
                attempt.id, attempt.task_id,
            ));
            report.required_actions.push(format!(
                "let boot recovery converge attempt {} before any resume",
                attempt.id,
            ));
        }
        if matches!(
            attempt.status,
            AttemptStatus::Running
                | AttemptStatus::WaitingForApproval
                | AttemptStatus::WaitingForInput
        ) {
            match state.attempt_sessions.get(&attempt.id) {
                Some(binding) => report.findings.push(format!(
                    "attempt {} runs in session {}/{}/{}: reconcile before resume",
                    attempt.id, binding.provider, binding.instance_epoch, binding.thread_id,
                )),
                None => report.findings.push(format!(
                    "attempt {} is {:?} with no harness session binding",
                    attempt.id, attempt.status,
                )),
            }
        }
    }

    for entry in state.inbox.values() {
        if matches!(entry.status, InboxStatus::Pending | InboxStatus::Answered) {
            report.findings.push(format!(
                "inbox {} is {:?} (epoch {}): an old accept never applies after a rotation",
                entry.key, entry.status, entry.epoch,
            ));
        }
    }
    let live_inbox = state
        .inbox
        .values()
        .filter(|entry| matches!(entry.status, InboxStatus::Pending | InboxStatus::Answered))
        .count();
    if live_inbox > 0 {
        report.required_actions.push(format!(
            "review {live_inbox} live inbox entries and answer through the current epoch only"
        ));
    }

    for artifact in state.artifacts.values() {
        let bound = artifact
            .attempt_id
            .is_some_and(|attempt_id| state.attempts.contains_key(&attempt_id));
        if !bound {
            report.findings.push(format!(
                "artifact {} ({}) references no known attempt: orphan, never auto-promoted",
                artifact.id, artifact.path,
            ));
            report.required_actions.push(format!(
                "decide the fate of orphan artifact {} explicitly; recovery will not promote it",
                artifact.id,
            ));
        }
    }

    if !state.usage.missing_final.is_empty() {
        report.findings.push(format!(
            "usage ledger misses {} finals: the UsageUnknown hold stays until ResolveUsageGap",
            state.usage.missing_final.len(),
        ));
        report.required_actions.push(
            "resolve each missing-final entry with `central` usage resolution (audit, never fabricated), then resume".to_string(),
        );
    }
    if !state.holds.is_empty() {
        let holds: Vec<String> = state.holds.iter().map(|hold| format!("{hold:?}")).collect();
        report.findings.push(format!(
            "holds preserved across the restart: {}",
            holds.join(", ")
        ));
    }
    if state.control == central_domain::RunControl::AwaitingResume {
        report.required_actions.push(format!(
            "resume the run explicitly (`central resume --run {}`); nothing dispatches before that",
            state.id,
        ));
    }
    report
}

/// Tasks held by an unknown branch: the transitive order/data descendants
/// of tasks with an unknown-outcome attempt. The unknown tasks themselves
/// are not listed; independent tasks are never listed.
pub fn blocked_by_unknown(state: &RunState) -> BTreeSet<TaskId> {
    let mut roots = BTreeSet::new();
    for attempt in state.attempts.values() {
        if is_unknown(attempt.status) {
            roots.insert(attempt.task_id);
        }
    }
    if roots.is_empty() {
        return BTreeSet::new();
    }
    // Transitive closure over order + data edges, parent -> child.
    let mut children: BTreeMap<TaskId, Vec<TaskId>> = BTreeMap::new();
    for task in state.tasks.values() {
        for parent in task.depends_on.iter().chain(task.data_depends_on.iter()) {
            children.entry(*parent).or_default().push(task.id);
        }
    }
    let mut blocked = BTreeSet::new();
    let mut stack: Vec<TaskId> = roots.iter().copied().collect();
    while let Some(task_id) = stack.pop() {
        if let Some(next) = children.get(&task_id) {
            for child in next {
                if !roots.contains(child) && blocked.insert(*child) {
                    stack.push(*child);
                }
            }
        }
    }
    blocked
}

/// Whether resuming the *same* harness session is safe: a session binding
/// is recorded, the attempt input is still stored, the attempt is not
/// terminal, the frozen profile still matches the task, and no observation
/// from a different epoch contradicts the binding. Anything else needs a
/// new attempt with a handover after the old outcome is known.
pub fn can_resume_session(state: &RunState, attempt_id: AttemptId) -> bool {
    let Some(attempt) = state.attempts.get(&attempt_id) else {
        return false;
    };
    if !is_live(attempt.status) {
        return false;
    }
    let Some(binding) = state.attempt_sessions.get(&attempt_id) else {
        return false;
    };
    if !state.attempt_inputs.contains_key(&attempt_id) {
        return false;
    }
    if let Some(task) = state.tasks.get(&attempt.task_id) {
        if task.profile_id != attempt.profile_id {
            return false;
        }
    }
    !state.observations.iter().any(|observation| {
        observation.attempt_id == attempt_id && observation.provider_epoch != binding.instance_epoch
    })
}

/// The old outcome of one attempt as the stored reconcile evidence
/// knows it: the outcome of the latest reconcile observation for the
/// attempt, or `None` when no reconcile ever reported a terminal
/// verdict. This is the durable answer to brief item 3's handover
/// precondition — a new attempt continues with a handover only after
/// this returns `Some` for the predecessor.
pub fn reconciled_outcome(state: &RunState, attempt_id: AttemptId) -> Option<VerificationOutcome> {
    state
        .observations
        .iter()
        .rfind(|observation| {
            observation.attempt_id == attempt_id && observation.dedup_key.starts_with("reconcile:")
        })
        .and_then(|observation| observation.outcome)
}

/// Map a reported harness status onto a persisted verification
/// outcome. Terminal verdicts converge the evidence (`Completed` is
/// work done, `Failed` is work failed, `Cancelled` unblocks nothing so
/// it reads as `Blocked`); every other status — including
/// `UnknownOutcome` — stays unforced (`None`), never a claim.
fn reconcile_outcome(status: AttemptStatus) -> Option<VerificationOutcome> {
    match status {
        AttemptStatus::Completed => Some(VerificationOutcome::Pass),
        AttemptStatus::Failed => Some(VerificationOutcome::Fail),
        AttemptStatus::Cancelled => Some(VerificationOutcome::Blocked),
        _ => None,
    }
}

impl Runtime {
    /// Passively reconcile one attempt: ask the gateway what it knows,
    /// store that evidence as a durable observation, and report it. The
    /// reported status/detail persist in the observation (terminal
    /// verdicts map onto an outcome via [`reconciled_outcome`]; unknown
    /// stays unforced), so a later handover can prove the old outcome
    /// is known. Never starts inference — no `open`, `send` or `answer`
    /// happens here — and never rewrites the attempt status: an unknown
    /// outcome stays unknown until the user queues a new attempt with a
    /// handover.
    pub async fn reconcile_attempt(
        &self,
        actor: Actor,
        attempt_id: AttemptId,
    ) -> Result<ReconcileEvidence, AppError> {
        let Some(state) = self.store().load(self.run_id()).await? else {
            return Err(AppError::NotFound(format!(
                "run {} not found",
                self.run_id()
            )));
        };
        if !state.attempts.contains_key(&attempt_id) {
            return Err(AppError::NotFound(format!(
                "attempt {attempt_id} not found in run {}",
                self.run_id()
            )));
        }
        let session = state
            .attempt_sessions
            .get(&attempt_id)
            .map(|binding| SessionRef {
                provider: binding.provider.clone(),
                instance_epoch: binding.instance_epoch.clone(),
                thread_id: binding.thread_id.clone(),
            })
            .unwrap_or(SessionRef {
                provider: "unknown".to_string(),
                instance_epoch: String::new(),
                thread_id: String::new(),
            });
        let target = ExecutionRef {
            attempt_id,
            session: session.clone(),
            turn: None,
        };
        // Passive read only: the gateway reports, Central never instructs.
        let evidence = self.agent().reconcile(&target).await?;
        // The key carries the reported verdict, so a changed verdict
        // stores a new observation while repeating the same verdict
        // stays idempotent; the outcome mapping keeps unknown unforced.
        let observation = central_domain::Observation {
            attempt_id,
            provider_epoch: session.instance_epoch.clone(),
            dedup_key: format!(
                "reconcile:{}:{attempt_id}:{:?}",
                session.thread_id, evidence.status
            ),
            outcome: reconcile_outcome(evidence.status),
            detail: evidence.detail.clone(),
        };
        let envelope = central_domain::CommandEnvelope {
            id: self.ids().next_command_id(),
            expected_revision: None,
            command: Command::RecordObservation { observation },
        };
        // `RecordObservation` is passive by contract: it stores the
        // evidence and resolves answered inbox entries, never answers.
        self.execute(actor, envelope).await?;
        Ok(evidence)
    }
}
