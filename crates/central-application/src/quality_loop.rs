//! Bounded verification rounds and leader escalation (P3 Task 2).
//!
//! [`QualityLoop`] is the application boundary around the durable quality
//! path: it keys seals per task, binds verifier verdicts to the exact
//! sealed input/candidate/criteria, enforces verifier independence, and
//! routes disputes and leader escalations without ever calling a model.
//!
//! The durable transitions themselves live in the domain (`SubmitVerification`,
//! `DisputeVerification`, `QueueRepairAttempt`): this loop builds the
//! reports those commands commit and describes the work they schedule.
//! Repair creates a new attempt linked to its predecessor; leader
//! escalation is a scheduled, budgeted work item that travels through the
//! scheduler and budget gates like any other work — the loop performs no
//! inference and bypasses no gate. A dispute opens one independent
//! re-review of the same candidate only inside the approved allowance;
//! otherwise it routes to the user. Verification reports never spawn
//! verifier-of-verifier rounds: a dispute re-verifies the candidate, never
//! the previous verifier's report.

use std::collections::HashMap;

use central_domain::{
    decide_quality, AttemptId, ContentHash, DomainError, QualityDecision, RunState, TaskId,
    TaskStatus, VerificationOutcome, VerificationReport,
};

use crate::contracts::AppError;
use crate::handover::VerificationV1;
use crate::verification::{criteria_hash, SealedCandidate, VerificationService, WorkerDone};

/// Route one verifier verdict to its quality decision.
///
/// The verdict binds to the single task awaiting verification whose
/// criteria hash it cites; anything else is stale (no task cites those
/// criteria) or ambiguous (several do — reverify with a task binding).
/// Routing itself is the domain budget rule: two repairs, then the leader.
pub fn route_verification(
    state: &RunState,
    verdict: &VerificationV1,
) -> Result<QualityDecision, DomainError> {
    let mut candidates = Vec::new();
    for task in state.tasks.values() {
        if task.status == TaskStatus::AwaitingVerification
            && criteria_hash(task) == verdict.criteria_hash
        {
            candidates.push(task);
        }
    }
    match candidates.as_slice() {
        [] => Err(DomainError::InvalidTransition(format!(
            "stale verdict for subject {}: no task awaits verification under \
             those criteria; reverify against the current seal",
            verdict.subject_hash,
        ))),
        [only] => Ok(decide_quality(only, verdict.outcome)),
        _ => Err(DomainError::InvalidTransition(format!(
            "ambiguous verdict for subject {}: {} tasks await verification \
             under those criteria; reverify with an explicit task binding",
            verdict.subject_hash,
            candidates.len(),
        ))),
    }
}

/// Verifier-side context the `VerificationV1` envelope does not carry: the
/// session the verifier ran in, the pinned environment/check references the
/// verdict rests on, the failed criteria a fail verdict must name, and the
/// tracked-source signal that invalidates evidence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifierContext {
    /// Harness thread the verifier ran in (independence check).
    pub verifier_thread: String,
    /// Pinned environment reference the checks ran in.
    pub environment: String,
    /// Approved checks the verdict rests on.
    pub check_refs: Vec<String>,
    /// Criteria the candidate failed; required for fail verdicts.
    pub failed_criteria: Vec<String>,
    /// The verifier wrote to tracked source while judging: its evidence is
    /// invalid, never a pass.
    pub verifier_wrote_tracked_source: bool,
    /// Evidence ids known to come from verifier-mutated source.
    pub tainted_evidence: Vec<String>,
    /// One-shot per-task user override admitting a same-session verifier.
    pub same_session_override: bool,
}

/// Where a dispute goes: one independent re-review of the same candidate
/// while the approved allowance covers it, else the user decides. A dispute
/// never reviews the previous verifier's report, so no chain can form: the
/// allowance admits at most one re-review before the user owns the call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DisputePlan {
    IndependentReviewer {
        task: TaskId,
        prior_attempt: AttemptId,
        reason: String,
    },
    RouteToUser {
        task: TaskId,
        reason: String,
    },
}

/// Leader escalation as a scheduled, budgeted work item.
///
/// The leader analysis travels through the scheduler and the budget gate
/// like any other work: this descriptor requires both and bypasses
/// neither. It performs no inference itself — constructing it never calls
/// a model — and it never rewrites the failed verdict it follows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaderEscalation {
    pub task: TaskId,
    pub failed_rounds: u8,
    pub candidate_hash: ContentHash,
    pub findings: String,
    pub requires_scheduler: bool,
    pub requires_budget_gate: bool,
    pub bypasses_gate: bool,
    pub calls_model_directly: bool,
}

/// Application boundary for the durable quality path.
pub struct QualityLoop {
    service: VerificationService,
    dispute_allowance: u8,
    escalations: HashMap<TaskId, LeaderEscalation>,
}

impl QualityLoop {
    /// The loop with its dispute policy: how many independent re-reviews
    /// one task's verification may open before disputes route to the user.
    /// An allowance of one admits a single re-review, so no
    /// verifier-of-verifier chain can form.
    pub fn new(dispute_allowance: u8) -> Self {
        Self {
            service: VerificationService::new(),
            dispute_allowance,
            escalations: HashMap::new(),
        }
    }

    pub fn service(&self) -> &VerificationService {
        &self.service
    }

    /// Record a sealed worker candidate and park its task for verification.
    ///
    /// When the task already succeeded on a different candidate, the new
    /// bytes reopen it: a candidate changed after a pass is stale, and the
    /// old acceptance no longer covers the task. The verified output is
    /// cleared with it, so no dependent can open on the old hash.
    pub fn note_worker_done(&mut self, state: &mut RunState, seal: SealedCandidate) -> WorkerDone {
        let done = self.service.note_worker_done(seal.clone());
        if let Some(task) = state.tasks.get_mut(&seal.task_id) {
            let changed = state
                .verified_outputs
                .get(&seal.task_id)
                .is_none_or(|accepted| *accepted != seal.candidate_hash);
            if task.status == TaskStatus::Succeeded && changed {
                task.status = TaskStatus::AwaitingVerification;
                state.verified_outputs.remove(&seal.task_id);
            } else if task.status == TaskStatus::Succeeded {
                // Same bytes re-sealed: the acceptance still stands.
            } else {
                task.status = TaskStatus::AwaitingVerification;
            }
        }
        done
    }

    /// Build the durable report for one verifier verdict.
    ///
    /// Checks everything committable without touching run state: the task
    /// awaits verification, the verdict cites the sealed criteria and
    /// input, the evidence is untainted, and a fail names its failed
    /// criteria. Session independence is enforced by the domain commit;
    /// this constructor carries the thread and override through.
    pub fn build_report(
        &self,
        state: &RunState,
        task: TaskId,
        verdict: &VerificationV1,
        ctx: &VerifierContext,
    ) -> Result<VerificationReport, AppError> {
        let task_state = state
            .tasks
            .get(&task)
            .ok_or_else(|| AppError::NotFound(format!("task {task} is not part of this run")))?;
        if task_state.status != TaskStatus::AwaitingVerification {
            return Err(AppError::Conflict(format!(
                "task {task} is not awaiting verification (it is {:?}); \
                 stale verdict, reverify against the current seal",
                task_state.status,
            )));
        }
        let expected_criteria = criteria_hash(task_state);
        if verdict.criteria_hash != expected_criteria {
            return Err(AppError::Conflict(format!(
                "stale verdict for task {task}: criteria changed since the seal; \
                 criteria change only through an approved plan revision, reverify",
            )));
        }
        let seal = self
            .service
            .seal_for(task, &verdict.subject_hash)
            .ok_or_else(|| {
                AppError::NotFound(format!(
                    "no sealed candidate for task {task} subject {}; \
                     reverify against the current seal",
                    verdict.subject_hash,
                ))
            })?;
        if seal.criteria_hash != verdict.criteria_hash {
            return Err(AppError::Conflict(format!(
                "stale verdict for task {task}: criteria changed since the seal; reverify",
            )));
        }
        if !verdict
            .evidence_ids
            .iter()
            .any(|id| id == seal.input_hash.as_str())
        {
            return Err(AppError::Conflict(format!(
                "verdict cites a different input than the sealed one (task {task}); \
                 stale, reverify",
            )));
        }
        if ctx.verifier_wrote_tracked_source
            || verdict
                .evidence_ids
                .iter()
                .any(|id| ctx.tainted_evidence.iter().any(|tainted| tainted == id))
        {
            return Err(AppError::Conflict(format!(
                "invalid evidence for task {task}: the verifier edited tracked \
                 source while judging; the candidate was mutated under verification",
            )));
        }
        if verdict.findings.trim().is_empty() {
            return Err(AppError::Validation(format!(
                "verdict for task {task} carries no findings",
            )));
        }
        if verdict.outcome == VerificationOutcome::Fail && ctx.failed_criteria.is_empty() {
            return Err(AppError::Validation(format!(
                "fail verdict for task {task} names no failed criteria",
            )));
        }
        Ok(VerificationReport {
            subject_hash: verdict.subject_hash.clone(),
            criteria_hash: verdict.criteria_hash.clone(),
            outcome: verdict.outcome,
            evidence_ids: verdict.evidence_ids.clone(),
            failed_criteria: ctx.failed_criteria.clone(),
            environment: ctx.environment.clone(),
            check_refs: ctx.check_refs.clone(),
            verifier_thread: ctx.verifier_thread.clone(),
            verifier_wrote_tracked_source: ctx.verifier_wrote_tracked_source,
            same_session_override: ctx.same_session_override,
            findings: verdict.findings.clone(),
        })
    }

    /// Route one verdict without committing: the domain budget rule over
    /// the task the verdict binds to.
    pub fn route(
        &self,
        state: &RunState,
        verdict: &VerificationV1,
    ) -> Result<QualityDecision, AppError> {
        route_verification(state, verdict).map_err(AppError::from)
    }

    /// Plan where a dispute goes under the approved allowance.
    ///
    /// Inside the allowance the dispute opens one independent re-review of
    /// the same candidate (never of the previous report); once spent, the
    /// dispute routes to the user. The durable count in run state audits
    /// the policy; the allowance itself is the approved budget for
    /// re-reviews.
    pub fn plan_dispute(&self, state: &RunState, task: TaskId, reason: String) -> DisputePlan {
        let used = state.verification_disputes.get(&task).copied().unwrap_or(0);
        if used < self.dispute_allowance {
            if let Some(prior) = prior_attempt(state, task) {
                return DisputePlan::IndependentReviewer {
                    task,
                    prior_attempt: prior,
                    reason,
                };
            }
        }
        DisputePlan::RouteToUser { task, reason }
    }

    /// Describe the leader escalation for a task whose repair budget is
    /// exhausted. The escalation is a scheduled, budgeted work item: it
    /// requires the scheduler and the budget gate, bypasses neither, and
    /// calls no model to describe itself.
    pub fn leader_escalation(
        &self,
        state: &RunState,
        task: TaskId,
    ) -> Result<LeaderEscalation, AppError> {
        if let Some(existing) = self.escalations.get(&task) {
            return Ok(existing.clone());
        }
        let task_state = state
            .tasks
            .get(&task)
            .ok_or_else(|| AppError::NotFound(format!("task {task} is not part of this run")))?;
        if task_state.repair_rounds_used < task_state.max_repair_rounds {
            return Err(AppError::Conflict(format!(
                "task {task} still has repair rounds left; escalation is premature",
            )));
        }
        Ok(LeaderEscalation {
            task,
            failed_rounds: task_state.repair_rounds_used,
            candidate_hash: state
                .verified_outputs
                .get(&task)
                .cloned()
                .unwrap_or_else(|| ContentHash::from_hex("unverified")),
            findings: format!(
                "task {task} failed {} quality rounds; leader analysis required",
                task_state.repair_rounds_used,
            ),
            requires_scheduler: true,
            requires_budget_gate: true,
            bypasses_gate: false,
            calls_model_directly: false,
        })
    }
}

/// The latest attempt known for a task: the anchor an independent
/// re-review chains from.
fn prior_attempt(state: &RunState, task: TaskId) -> Option<AttemptId> {
    state
        .attempts
        .values()
        .filter(|attempt| attempt.task_id == task)
        .max_by_key(|attempt| attempt.created_at)
        .map(|attempt| attempt.id)
}
