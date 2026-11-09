//! Bounded verification rounds and leader escalation (P3 Task 2).
//!
//! A worker finishing never marks a task `Succeeded`. A verifier verdict
//! bound to the exact sealed input, candidate and criteria routes to one
//! [`QualityDecision`]: two quality-repair rounds after the first
//! verification, then escalation to the leader. `Blocked` (the environment
//! failed, not the candidate) never consumes a quality round.
//!
//! The escalation is a budgeted, scheduled work item: the domain records
//! the routing and never calls a model, never opens inference, and never
//! bypasses the scheduler or budget gates — the application layer carries
//! the escalation through the same gates as any other work. The leader can
//! never force a pass; a dispute opens one independent re-review only
//! inside the approved allowance, otherwise it routes to the user. There
//! are no verifier-of-verifier chains: a dispute re-verifies the same
//! candidate, never the previous verifier's report.
//!
//! Pure: no IO, no mutation, deterministic for a given [`RunState`].

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{ContentHash, DomainError, Task, VerificationOutcome, VerifySpec};

/// Routing answer for one verifier verdict on a task awaiting verification.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum QualityDecision {
    /// The sealed candidate passed: the task may succeed.
    Accepted,
    /// The candidate failed with repair rounds left; `round` counts from 1.
    Repair { round: u8 },
    /// The environment failed, not the candidate: no round consumed.
    Blocked,
    /// Repair rounds are exhausted: a leader analysis work item must be
    /// scheduled through the normal gates. Never a pass, never a bypass.
    AskLeader,
    /// The verdict is disputed: one independent re-review of the same
    /// candidate when the allowance covers it, else the user decides.
    Disputed,
}

/// Durable verdict record carried by `Command::SubmitVerification`.
///
/// It binds the exact subject (candidate hash), the exact criteria hash
/// and the environment/check references the verdict was produced under.
/// Criteria change only through an approved plan revision: a report naming
/// any other criteria hash is stale and must be re-verified. History is
/// append-only — a new report never rewrites a committed verdict.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VerificationReport {
    /// Exact hash of the sealed candidate the verifier judged.
    pub subject_hash: ContentHash,
    /// Exact criteria hash the verifier judged against.
    pub criteria_hash: ContentHash,
    /// Pass, fail, or blocked (environment).
    pub outcome: VerificationOutcome,
    /// Evidence backing the verdict; must cite the sealed input.
    pub evidence_ids: Vec<String>,
    /// Criteria the candidate failed. A fail verdict must name at least
    /// one: a silent fail is not actionable and is refused.
    pub failed_criteria: Vec<String>,
    /// Environment the checks ran in (pinned reference, not prose).
    pub environment: String,
    /// Approved checks the verdict rests on.
    pub check_refs: Vec<String>,
    /// Harness thread the verifier ran in. Compared against the worker
    /// attempt's session: the default verifier is an independent session,
    /// and a same-session verdict is refused without a per-task user
    /// override.
    pub verifier_thread: String,
    /// The verifier wrote to tracked source while judging. Such evidence
    /// is invalid: the candidate was mutated under verification.
    pub verifier_wrote_tracked_source: bool,
    /// One-shot per-task user override admitting a same-session verifier.
    /// Only a user-carried submission may set this; a verifier cannot
    /// self-approve it.
    pub same_session_override: bool,
    /// Human-readable findings accompanying the verdict.
    pub findings: String,
}

/// Canonical hash of a task's verify spec: the exact criteria a verifier
/// must judge against. A user-approved criteria change (plan revision)
/// produces a different hash, which stale verdicts fail against.
pub fn criteria_hash_for(verify: &VerifySpec) -> ContentHash {
    let value = serde_json::to_value(verify).expect("verify spec serializes");
    let canonical = serde_json::to_string(&value).expect("verify spec serializes");
    let digest = Sha256::digest(canonical.as_bytes());
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        hex.push_str(&format!("{byte:02x}"));
    }
    ContentHash::from_hex(hex)
}

/// Route one outcome for a task by its repair budget. Pass accepts;
/// blocked never consumes a round; fail retries while rounds remain and
/// escalates to the leader once they are exhausted.
pub fn decide_quality(task: &Task, outcome: VerificationOutcome) -> QualityDecision {
    match outcome {
        VerificationOutcome::Pass => QualityDecision::Accepted,
        VerificationOutcome::Blocked => QualityDecision::Blocked,
        VerificationOutcome::Fail if task.repair_rounds_used < task.max_repair_rounds => {
            QualityDecision::Repair {
                round: task.repair_rounds_used + 1,
            }
        }
        VerificationOutcome::Fail => QualityDecision::AskLeader,
    }
}

/// Validate a report against the task's current criteria before routing.
/// Stale criteria, silent fails, empty evidence, empty findings and
/// verifier-mutated sources are all refused without touching the task.
pub fn check_report(task: &Task, report: &VerificationReport) -> Result<(), DomainError> {
    if report.criteria_hash != criteria_hash_for(&task.verify) {
        return Err(DomainError::InvalidTransition(format!(
            "stale verdict for task {}: criteria changed since the seal; \
             criteria change only through an approved plan revision, reverify",
            task.id,
        )));
    }
    if report.verifier_wrote_tracked_source {
        return Err(DomainError::InvalidTransition(format!(
            "invalid evidence for task {}: the verifier edited tracked source \
             while judging; the candidate was mutated under verification",
            task.id,
        )));
    }
    if report.evidence_ids.iter().all(|id| id.trim().is_empty()) {
        return Err(DomainError::InvalidCommand(format!(
            "verdict for task {} cites no evidence",
            task.id,
        )));
    }
    if report.findings.trim().is_empty() {
        return Err(DomainError::InvalidCommand(format!(
            "verdict for task {} carries no findings",
            task.id,
        )));
    }
    if report.outcome == VerificationOutcome::Fail && report.failed_criteria.is_empty() {
        return Err(DomainError::InvalidCommand(format!(
            "fail verdict for task {} names no failed criteria",
            task.id,
        )));
    }
    Ok(())
}
