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
    decide_quality, Actor, AttemptId, Command, CommandEnvelope, ContentHash, DomainError, Event,
    QualityDecision, RunState, TaskId, TaskStatus, VerificationOutcome, VerificationReport,
};
use serde::{Deserialize, Serialize};

use crate::contracts::AppError;
use crate::handover::VerificationV1;
use crate::verification::{criteria_hash, SealedCandidate, VerificationService, WorkerDone};
use crate::Runtime;

/// Route one verifier verdict to its quality decision.
///
/// The verdict binds to the single open task whose sealed subject and
/// criteria it cites: criteria alone never identify the task, since two
/// tasks may share one verify spec. A verdict naming an unsealed subject
/// is stale; one matching several sealed subjects is ambiguous (reverify
/// with an explicit task binding). Blocked tasks stay routable so
/// re-verification after recovery resumes on the preserved round budget.
/// Routing itself is the domain budget rule: two repairs, then the leader.
pub fn route_verification(
    state: &RunState,
    seals: &VerificationService,
    verdict: &VerificationV1,
) -> Result<QualityDecision, DomainError> {
    let mut candidates = Vec::new();
    for task in state.tasks.values() {
        if matches!(
            task.status,
            TaskStatus::AwaitingVerification | TaskStatus::Blocked
        ) && criteria_hash(task) == verdict.criteria_hash
            && seals.seal_for(task.id, &verdict.subject_hash).is_some()
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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
        Self::build_report_from_seal(state, task, &seal, verdict, ctx)
    }

    /// The same report against an explicitly supplied seal. The daemon has
    /// no durable in-memory seal table across restarts, so the verdict
    /// endpoint reconstructs the seal from the persisted
    /// `WorkerResultRecorded` events and checks it here — the identical
    /// binding the in-memory path enforces, never a weaker one.
    pub fn build_report_from_seal(
        state: &RunState,
        task: TaskId,
        seal: &SealedCandidate,
        verdict: &VerificationV1,
        ctx: &VerifierContext,
    ) -> Result<VerificationReport, AppError> {
        let task_state = state
            .tasks
            .get(&task)
            .ok_or_else(|| AppError::NotFound(format!("task {task} is not part of this run")))?;
        if !matches!(
            task_state.status,
            TaskStatus::AwaitingVerification | TaskStatus::Blocked
        ) {
            return Err(AppError::Conflict(format!(
                "task {task} is not open for verification (it is {:?}); \
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
        route_verification(state, &self.service, verdict).map_err(AppError::from)
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

/// The verdict endpoint body: the task and producing attempt the verdict
/// judges, the independent verdict itself, and the verifier-side context
/// the verdict envelope does not carry. The task binding is explicit —
/// criteria alone never identify the task — and the seal binding is
/// re-derived from the persisted `WorkerResultRecorded` events, never
/// from agent word alone.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VerdictSubmitBody {
    pub task: TaskId,
    pub attempt: AttemptId,
    pub verdict: VerificationV1,
    pub context: VerifierContext,
}

/// Strict parse for the verdict endpoint and the CLI import: schema
/// version, unknown fields and empty text are refused before any state.
pub fn parse_verdict_submit(text: &str) -> Result<VerdictSubmitBody, AppError> {
    let submitted: VerdictSubmitBody = serde_json::from_str(text)
        .map_err(|error| AppError::Validation(format!("invalid verdict submission: {error}")))?;
    if submitted.verdict.version != crate::handover::VERIFICATION_VERSION {
        return Err(AppError::Validation(format!(
            "unknown {} version {}; Central supports version {}",
            crate::handover::VERIFICATION_SCHEMA,
            submitted.verdict.version,
            crate::handover::VERIFICATION_VERSION,
        )));
    }
    if submitted.verdict.findings.trim().is_empty() {
        return Err(AppError::Validation(
            "verdict findings must not be empty".to_string(),
        ));
    }
    Ok(submitted)
}

impl Runtime {
    /// Record one independent verifier verdict through the durable seal.
    ///
    /// The seal is reconstructed from the persisted `WorkerResultRecorded`
    /// events — the latest recording for this task — so a verdict survives
    /// daemon restarts exactly like the seal it judges. The report is
    /// built with the same binding the in-memory path enforces (exact
    /// subject, exact criteria, sealed input cited, untainted evidence,
    /// fail names its criteria); the domain commit then enforces actor
    /// kind, attempt membership, task openness, plan binding and verifier
    /// independence. Returns the receipt and the task status after commit.
    pub async fn submit_verdict(
        &self,
        actor: Actor,
        task: TaskId,
        attempt: AttemptId,
        verdict: VerificationV1,
        ctx: VerifierContext,
    ) -> Result<(crate::contracts::Receipt, TaskStatus), AppError> {
        let Some(state) = self.store().load(self.run_id()).await? else {
            return Err(AppError::NotFound(format!(
                "run {} not found",
                self.run_id()
            )));
        };
        let attempt_task = state
            .attempts
            .get(&attempt)
            .map(|record| record.task_id)
            .ok_or_else(|| {
                AppError::NotFound(format!(
                    "attempt {attempt} is unknown to run {}",
                    self.run_id()
                ))
            })?;
        if attempt_task != task {
            return Err(AppError::Validation(format!(
                "attempt {attempt} does not belong to task {task}"
            )));
        }
        let task_state = state.tasks.get(&task).ok_or_else(|| {
            AppError::NotFound(format!("task {task} is not part of run {}", self.run_id()))
        })?;
        let recorded = self
            .store()
            .events(self.run_id())
            .await?
            .into_iter()
            .filter_map(|event| match event {
                Event::WorkerResultRecorded {
                    task: event_task,
                    input_hash,
                    candidate_hash,
                    criteria_hash,
                    ..
                } if event_task == task => Some((input_hash, candidate_hash, criteria_hash)),
                _ => None,
            })
            .next_back()
            .ok_or_else(|| {
                AppError::NotFound(format!(
                    "task {task} has no sealed candidate; import the worker result first"
                ))
            })?;
        if recorded.1 != verdict.subject_hash {
            return Err(AppError::Conflict(format!(
                "stale verdict for task {task}: subject {} is not the sealed candidate; \
                 reverify against the current seal",
                verdict.subject_hash,
            )));
        }
        let seal = SealedCandidate {
            task_id: task,
            input_hash: recorded.0,
            candidate_hash: recorded.1,
            criteria_hash: recorded.2,
            max_repair_rounds: task_state.max_repair_rounds,
        };
        let report = QualityLoop::build_report_from_seal(&state, task, &seal, &verdict, &ctx)?;
        let receipt = self
            .execute(
                actor,
                CommandEnvelope {
                    id: self.ids().next_command_id(),
                    expected_revision: None,
                    command: Command::SubmitVerification {
                        task,
                        attempt,
                        report,
                    },
                },
            )
            .await?;
        let status = self
            .store()
            .load(self.run_id())
            .await?
            .and_then(|state| state.tasks.get(&task).map(|task| task.status))
            .ok_or_else(|| {
                AppError::Storage(format!(
                    "run {} vanished after verdict commit",
                    self.run_id()
                ))
            })?;
        Ok((receipt, status))
    }
}

#[cfg(test)]
mod verdict_submit_tests {
    use super::*;

    fn body(findings: &str) -> String {
        serde_json::json!({
            "task": "00000000-0000-0000-0000-000000000001",
            "attempt": "00000000-0000-0000-0000-000000000099",
            "verdict": {"version": 1, "subjectHash": "aa", "criteriaHash": "bb", "outcome": "pass", "evidenceIds": ["input-seal"], "findings": findings},
            "context": {"verifierThread": "v-1", "environment": "test", "checkRefs": [], "failedCriteria": [], "verifierWroteTrackedSource": false, "taintedEvidence": [], "sameSessionOverride": false},
        })
        .to_string()
    }

    #[test]
    fn verdict_submit_parses_and_rejects_shape_violations() {
        assert!(parse_verdict_submit(&body("judged")).is_ok());
        assert!(parse_verdict_submit(&body("  ")).is_err());
        assert!(parse_verdict_submit("{\"task\":1}").is_err());
    }
}
