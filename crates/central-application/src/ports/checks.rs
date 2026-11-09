use async_trait::async_trait;
use central_domain::{ContentHash, PlanHash};

use crate::contracts::AppError;

/// One approved check invocation. Only the exact `program`+`args` pair the
/// runner was constructed with (from the approved plan) may run, and only
/// for the plan the runner was bound to: `approved_plan_hash` must equal
/// the runner's construction-plan hash. Anything else is refused without
/// spawning. `cwd` and every `allowed_write_roots` entry must stay inside
/// the runner's work root.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckRequest {
    pub approved_plan_hash: PlanHash,
    pub program: String,
    pub args: Vec<String>,
    pub cwd: String,
    pub timeout_ms: u64,
    pub allowed_write_roots: Vec<String>,
}

/// Captured evidence of one check run. Evidence is never a verdict: an exit
/// code of zero does not mark a task succeeded, only an independent verifier
/// acceptance does.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckEvidence {
    pub program: String,
    pub args: Vec<String>,
    pub exit_code: i32,
    pub timed_out: bool,
    pub stdout_hash: ContentHash,
    pub stderr_hash: ContentHash,
    pub stdout_tail: String,
    pub stderr_tail: String,
}

/// Runs approved checks and captures evidence. The runner never evaluates
/// model prose and never spawns agents: judging a candidate belongs to
/// [`crate::verification::VerificationService`], so verifiers cannot recurse.
#[async_trait]
pub trait CheckRunner: Send + Sync {
    async fn run(&self, request: CheckRequest) -> Result<CheckEvidence, AppError>;
}
