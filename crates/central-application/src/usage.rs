//! Shared-budget use cases: limits, usage reconcile and gap resolution (P3 Task 3).
//!
//! Every role spends from the same per-run ledger through the same
//! [`central_domain::admission`] gate; these helpers are the audited
//! User-only write path for that ledger. Reads (status, admission) go
//! straight through the domain functions so the CLI, the scheduler and
//! the dispatcher always agree.

use central_domain::{
    admission, Actor, Admission, Command, CommandEnvelope, RunState, Timestamp, UsageLimits,
    UsageObservation, WorkRequest, WorkRole,
};

use crate::contracts::{AppError, Receipt};
use crate::Runtime;

/// Admission for one role against a run snapshot. Uniform across all
/// roles: leaders, verifiers, retries and fallbacks share the ledger.
pub fn admission_for(state: &RunState, role: WorkRole, now: Timestamp) -> Admission {
    admission(state, &WorkRequest { role }, now)
}

async fn execute(runtime: &Runtime, actor: Actor, command: Command) -> Result<Receipt, AppError> {
    let envelope = CommandEnvelope {
        id: runtime.ids().next_command_id(),
        expected_revision: None,
        command,
    };
    runtime.execute(actor, envelope).await
}

/// Set the run's shared limits. User only (enforced by the domain
/// decision); the actor is recorded as the limits' author.
pub async fn set_limits(
    runtime: &Runtime,
    actor: Actor,
    limits: UsageLimits,
) -> Result<Receipt, AppError> {
    execute(runtime, actor, Command::SetLimits { limits }).await
}

/// Fold one usage report into the shared ledger. User only.
pub async fn reconcile_usage(
    runtime: &Runtime,
    actor: Actor,
    observation: UsageObservation,
) -> Result<Receipt, AppError> {
    execute(runtime, actor, Command::ReconcileUsage { observation }).await
}

/// Resolve one missing-final entry by explicit user decision, with an
/// audit note. Releases the hold without converting anything into
/// observed usage. User only.
pub async fn resolve_gap(
    runtime: &Runtime,
    actor: Actor,
    missing_key: String,
    note: String,
) -> Result<Receipt, AppError> {
    execute(
        runtime,
        actor,
        Command::ResolveUsageGap { missing_key, note },
    )
    .await
}
