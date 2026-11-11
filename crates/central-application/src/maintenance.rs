//! Owner-only maintenance use cases (P4): state export and garbage
//! collection. Both share one implementation with the CLI and the HTTP
//! layer — there is no second write path.
//!
//! Export writes a redacted report (runs, revisions, receipts, usage,
//! recovery); it is a report, never a database backup, and it never
//! contains credentials. GC is explicit and dry-run-first: blobs still
//! referenced by committed metadata, runs with nonterminal work, and
//! anything the store reports as active-or-unknown is never deleted.

use std::collections::BTreeSet;

use central_domain::{RunControl, RunId};

use crate::contracts::AppError;
use crate::ports::Store;

/// What an export contains. Credential records are listed by id, name,
/// scope and revocation only — hashes and secrets never leave the store.
#[derive(Clone, Debug)]
pub struct ExportReport {
    pub runs: Vec<RunId>,
    pub client_ids: Vec<String>,
    pub profiles: Vec<String>,
    pub note: String,
}

/// Build the export report. No credentials, no secrets, no hashes.
pub async fn export_state(store: &dyn Store) -> Result<ExportReport, AppError> {
    let runs = store.list_run_ids().await?;
    let clients = store.list_clients().await.unwrap_or_default();
    let profiles = store.list_profiles().await.unwrap_or_default();
    Ok(ExportReport {
        runs,
        client_ids: clients.iter().map(|c| c.client_id.clone()).collect(),
        profiles: profiles.iter().map(|p| p.profile_id.clone()).collect(),
        note: "report only: not a database backup; copy the DB file plus \
               its WAL and the artifact store together while the daemon is \
               stopped and the owner lock is free"
            .to_string(),
    })
}

/// One GC candidate the dry run would delete.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcCandidate {
    pub kind: String,
    pub id: String,
    pub reason: String,
}

/// Dry-run garbage collection: names what a real GC would delete
/// without deleting anything. Never names a run whose control is not
/// `Stopped`, a run holding unknown usage/outcomes, or anything the
/// caller flags as referenced.
pub async fn gc_dry_run(
    store: &dyn Store,
    referenced: &BTreeSet<String>,
) -> Result<Vec<GcCandidate>, AppError> {
    let mut candidates = Vec::new();
    for run_id in store.list_run_ids().await? {
        let id_text = run_id.to_string();
        if referenced.contains(&id_text) {
            continue;
        }
        let Some(state) = store.load(run_id).await? else {
            continue;
        };
        if state.control != RunControl::Stopped {
            continue;
        }
        if state.usage.unknown || !state.usage.missing_final.is_empty() {
            continue;
        }
        let unknown_attempt = state.attempts.values().any(|attempt| {
            matches!(
                attempt.status,
                central_domain::AttemptStatus::UnknownOutcome
                    | central_domain::AttemptStatus::Reconciling
                    | central_domain::AttemptStatus::Dispatching
            )
        });
        if unknown_attempt {
            continue;
        }
        candidates.push(GcCandidate {
            kind: "stopped-run".to_string(),
            id: id_text,
            reason: "stopped with no unknown usage and no ambiguous attempts".to_string(),
        });
    }
    Ok(candidates)
}
