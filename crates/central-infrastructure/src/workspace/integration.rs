//! Integration check orchestration (P3 Task 5, infrastructure side).
//!
//! Runs the application [`IntegrationPlan`] over a prepared candidate
//! worktree through the approved [`CheckRunner`]: baseline checks, then the
//! affected checks, then the final acceptance — in that order, all bound to
//! the exact candidate by working directory. The plan decides the order,
//! the runner enforces approval, and the judge in
//! [`central_application::integration`] decides pass or fail. Evidence is
//! never a verdict by itself.

use central_application::contracts::AppError;
use central_application::integration::{summarize_verdict, CheckOutcome, IntegrationPlan};
use central_application::ports::checks::{CheckEvidence, CheckRequest, CheckRunner};
use central_application::ports::workspace::VerificationSummary;
use central_domain::PlanHash;

/// Check phases in execution order, matching `IntegrationPlan::required`.
const BASE_PHASE: &str = "baseline";
const AFFECTED_PHASE: &str = "affected";
const ACCEPTANCE_PHASE: &str = "acceptance";

/// Run every required check of `plan` in order with `cwd` pinned to the
/// candidate worktree. Unapproved program+argv refuse without spawning (the
/// runner's error surfaces as `Forbidden`); a transport failure aborts the
/// sequence with nothing judged. Returns the per-check evidence plus the
/// promotion summary.
pub async fn run_integration_checks(
    runner: &dyn CheckRunner,
    approved_plan: &PlanHash,
    worktree: &str,
    plan: &IntegrationPlan,
) -> Result<(Vec<CheckEvidence>, VerificationSummary), AppError> {
    let required = plan.required();
    let mut evidence = Vec::with_capacity(required.len());
    let mut outcomes = Vec::with_capacity(required.len());
    let mut labels = Vec::with_capacity(required.len());
    for (index, check) in required.iter().enumerate() {
        let phase = if index < plan.baseline.len() {
            BASE_PHASE
        } else if index < plan.baseline.len() + plan.affected.len() {
            AFFECTED_PHASE
        } else {
            ACCEPTANCE_PHASE
        };
        let got = runner
            .run(CheckRequest {
                approved_plan_hash: approved_plan.clone(),
                program: check.program.clone(),
                args: check.args.clone(),
                cwd: worktree.to_string(),
                timeout_ms: 120_000,
                allowed_write_roots: vec![worktree.to_string()],
            })
            .await?;
        outcomes.push(CheckOutcome {
            exit_code: got.exit_code,
            timed_out: got.timed_out,
        });
        labels.push(format!("{phase}:{}:{}", got.program, got.exit_code));
        evidence.push(got);
    }
    let summary = summarize_verdict(plan, &outcomes, labels);
    Ok((evidence, summary))
}
