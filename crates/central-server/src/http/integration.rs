//! Owner-only Git integration: prepare a candidate branch, run the
//! approved checks, and promote the good ref by compare-and-swap.
//!
//! One call does the whole mechanical round through the real
//! [`GitWorkspaceManager`]: seal the attempt's live worktree, prepare the
//! temporary integration branch, run baseline/affected/acceptance checks
//! with the approved program+argv allowlist, and promote only on pass.
//! Every step is validated before it mutates: candidates must seal, checks
//! must all pass, and the expected parent must still hold — otherwise the
//! good ref never moves and the temporary branch is cleaned up.
//!
//! This is the operator-driven half of P3/T5: the caller (owner) supplies
//! the exact candidate, base, ref and checks, and the daemon executes
//! them with the same guards the in-process suites assert. Scheduler
//! auto-dispatch of integration tasks stays a follow-up; nothing here
//! invents approvals, commits or verdicts.

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use central_application::ports::workspace::WorkspaceManager;
use central_domain::{AttemptId, PlanHash, RunId};
use serde::{Deserialize, Serialize};

use super::auth::{app_error_response, authenticate};
use super::AppState;
use crate::api::errors::{error_response, ApiError};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PlannedCheckBody {
    program: String,
    #[serde(default)]
    args: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ChecksBody {
    #[serde(default)]
    baseline: Vec<PlannedCheckBody>,
    #[serde(default)]
    affected: Vec<PlannedCheckBody>,
    #[serde(default)]
    acceptance: Vec<PlannedCheckBody>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IntegrationBody {
    /// Attempt whose live worktree holds the candidate. Sealed here, so
    /// the exact bytes the worker left behind are what gets integrated.
    pub attempt: String,
    /// Base commit the temporary branch starts from (must be an ancestor
    /// the manager can resolve in the candidate's repo).
    pub base: String,
    /// Good ref to move on pass (e.g. `refs/heads/central-good`).
    pub ref_name: String,
    /// Expected current value of the good ref: compare-and-swap guard.
    pub expected_parent: String,
    /// Approved plan hash: the checks below are allowlisted for exactly
    /// this plan by the check runner.
    pub approved_plan: String,
    pub checks: ChecksBody,
}

#[derive(Debug, Serialize)]
struct CheckReport {
    program: String,
    args: Vec<String>,
    exit_code: i32,
    timed_out: bool,
    stdout_hash: String,
    stderr_hash: String,
}

fn hex_hash(value: &str) -> Result<PlanHash, ApiError> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.len() != 64 || !trimmed.chars().all(|c| c.is_ascii_hexdigit())
    {
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            "approved_plan must be 64 hex chars",
        ));
    }
    Ok(PlanHash::from_hex(trimmed))
}

pub async fn post_integration_run(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(raw_id): Path<String>,
    Json(body): Json<IntegrationBody>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let auth = authenticate(&state, &headers).await?;
    // Integration mutates the good ref: control scope or higher. Readers
    // and agent bridges never reach this path.
    auth.require_control()?;
    let run_id = raw_id.parse::<RunId>().map_err(|_| {
        error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            format!("invalid run id: {raw_id}"),
        )
    })?;
    // Refuse to integrate for a run that does not exist: the manager
    // keys everything by run, and an orphan integration would have no
    // plan binding to answer to.
    if state
        .store
        .load(run_id)
        .await
        .map_err(app_error_response)?
        .is_none()
    {
        return Err(error_response(
            StatusCode::NOT_FOUND,
            "not_found",
            format!("run {run_id} not found"),
        ));
    }
    let attempt = body.attempt.parse::<AttemptId>().map_err(|_| {
        error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            format!("invalid attempt id: {}", body.attempt),
        )
    })?;
    if body.ref_name.trim().is_empty() || body.base.trim().is_empty() {
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            "base and ref_name must not be empty",
        ));
    }
    let approved_plan = hex_hash(&body.approved_plan)?;

    // One mechanical round through the shared driver: seal the attempt's
    // live worktree, prepare the temporary branch, run the approved
    // checks and CAS-promote the good ref on pass. Every validation the
    // handler performs (run exists, non-empty ref, at least one check)
    // happens here; everything else fails closed inside the manager.
    if body.checks.baseline.is_empty()
        && body.checks.affected.is_empty()
        && body.checks.acceptance.is_empty()
    {
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            "at least one approved check is required; refusing an unchecked promotion",
        ));
    }
    let to_approved = |checks: Vec<PlannedCheckBody>| {
        checks
            .into_iter()
            .map(|c| central_application::ports::workspace::ApprovedCheck {
                program: c.program,
                args: c.args,
            })
            .collect::<Vec<_>>()
    };
    let report = state
        .workspace
        .auto_integrate(
            central_application::ports::workspace::AutoIntegrationRequest {
                run: run_id,
                candidates: vec![state
                    .workspace
                    .seal_live(attempt)
                    .await
                    .map_err(app_error_response)?],
                base: body.base.clone(),
                ref_name: body.ref_name.clone(),
                expected_parent: Some(body.expected_parent.clone()),
                approved_plan,
                baseline: to_approved(body.checks.baseline),
                affected: to_approved(body.checks.affected),
                acceptance: to_approved(body.checks.acceptance),
            },
        )
        .await
        .map_err(app_error_response)?;

    let checks: Vec<CheckReport> = report
        .evidence
        .iter()
        .map(|got| CheckReport {
            program: got.program.clone(),
            args: got.args.clone(),
            exit_code: got.exit_code,
            timed_out: got.timed_out,
            stdout_hash: got.stdout_hash.to_string(),
            stderr_hash: got.stderr_hash.to_string(),
        })
        .collect();
    Ok(Json(serde_json::json!({
        "run_id": run_id.to_string(),
        "candidate": report.candidate,
        "worktree": report.worktree,
        "checks": checks,
        "summary": {
            "passed": report.summary.passed,
            "baseline_ok": report.summary.baseline_ok,
            "affected_ok": report.summary.affected_ok,
            "acceptance_ok": report.summary.acceptance_ok,
        },
        "receipt": {
            "previous_head": report.receipt.previous_head,
            "new_head": report.receipt.new_head,
        },
    })))
}
