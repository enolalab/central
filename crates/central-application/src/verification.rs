//! One-task independent verification (P2 Task 5).
//!
//! A worker finishing never marks a task `Succeeded`: [`VerificationService`]
//! parks the sealed candidate in `AwaitingVerification`, and only an
//! independent verifier verdict bound to the exact sealed input, candidate
//! and criteria flips the task to `Succeeded`. A verdict that names another
//! subject, other criteria, or other input is stale and must be re-verified.
//!
//! The service is pure and synchronous: it owns no subprocesses, spawns no
//! agents and performs no IO, so a verifier can never recursively verify.
//! It never edits the candidate or the criteria; it only compares hashes.

use std::collections::HashMap;

use central_domain::{ContentHash, RunState, Task, TaskId, TaskStatus, VerificationOutcome};
use serde_json;

use crate::artifacts::check_run_relative_path;
use crate::contracts::AppError;
use crate::handover::{accept_worker_result, VerificationV1, WorkerResultV1};

/// Canonical hash of a task's verify spec: the exact criteria the verifier
/// must judge against. A user-approved criteria change produces a different
/// hash, which stale verdicts fail against.
pub fn criteria_hash(task: &Task) -> ContentHash {
    let value = serde_json::to_value(&task.verify).expect("verify spec serializes");
    let canonical = serde_json::to_string(&value).expect("verify spec serializes");
    crate::artifacts::content_hash(canonical.as_bytes())
}

/// A worker candidate sealed for independent verification: the exact input,
/// candidate and criteria hashes, plus the task's repair budget. Created by
/// [`seal_candidate`] after the report passes membership and containment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SealedCandidate {
    pub task_id: TaskId,
    pub input_hash: ContentHash,
    pub candidate_hash: ContentHash,
    pub criteria_hash: ContentHash,
    pub max_repair_rounds: u8,
}

/// What recording a worker report does to its task: parks it in
/// `AwaitingVerification`, never `Succeeded`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerDone {
    pub task_id: TaskId,
    pub task_status: TaskStatus,
}

/// The receipt for one judged verdict: what was judged and where the task
/// stands now. Durable run-state commits for this receipt arrive with the
/// quality loop (P3); this receipt is the in-memory acceptance record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerificationReceipt {
    pub task_id: TaskId,
    pub candidate_hash: ContentHash,
    pub outcome: VerificationOutcome,
    pub task_status: TaskStatus,
    pub repair_rounds_used: u8,
}

/// Bind a parsed worker report to its task: the attempt must belong to this
/// run and the candidate path must stay inside it (same gate the leader
/// bridge enforces), then seal the exact input/candidate/criteria hashes the
/// verifier must reproduce. Pure: no IO, no mutation of candidate or criteria.
pub fn seal_candidate(
    state: &RunState,
    result: &WorkerResultV1,
) -> Result<SealedCandidate, AppError> {
    accept_worker_result(state, result)?;
    check_run_relative_path(&result.candidate_path)?;
    let attempt = state
        .attempts
        .get(&result.attempt_id)
        .expect("accept_worker_result already proved the attempt belongs to this run");
    let task = state.tasks.get(&attempt.task_id).ok_or_else(|| {
        AppError::Validation(format!(
            "attempt {} names unknown task {}",
            result.attempt_id, attempt.task_id,
        ))
    })?;
    Ok(SealedCandidate {
        task_id: task.id,
        input_hash: result.input_hash.clone(),
        candidate_hash: result.candidate_hash.clone(),
        criteria_hash: criteria_hash(task),
        max_repair_rounds: task.max_repair_rounds,
    })
}

/// Independent verdicts judged only against sealed subjects.
#[derive(Clone, Debug, Default)]
pub struct VerificationService {
    sealed: HashMap<ContentHash, SealedCandidate>,
    status: HashMap<TaskId, (TaskStatus, u8)>,
}

impl VerificationService {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a sealed worker candidate. Worker completion parks the task in
    /// `AwaitingVerification` — never `Succeeded`, no matter how the worker
    /// or its checks exited.
    pub fn note_worker_done(&mut self, seal: SealedCandidate) -> WorkerDone {
        let done = WorkerDone {
            task_id: seal.task_id,
            task_status: TaskStatus::AwaitingVerification,
        };
        self.sealed
            .insert(seal.candidate_hash.clone(), seal.clone());
        self.status
            .entry(seal.task_id)
            .or_insert((TaskStatus::AwaitingVerification, 0));
        done
    }

    /// Judge one verifier verdict against the sealed subject.
    ///
    /// The verifier runs in its own session with its own scratch: it must
    /// cite the exact sealed input (as an evidence id), the exact sealed
    /// candidate and the exact sealed criteria. Anything else is stale and
    /// must be re-verified — the service never edits the candidate or the
    /// criteria to make a verdict fit.
    ///
    /// `Pass` marks the task `Succeeded`. `Fail` parks the task for a quality
    /// retry while repair rounds remain and escalates to `Failed` once they
    /// are exhausted. `Blocked` records `Blocked` without consuming a round.
    pub fn accept(&mut self, verdict: &VerificationV1) -> Result<VerificationReceipt, AppError> {
        let seal = self
            .sealed
            .get(&verdict.subject_hash)
            .cloned()
            .ok_or_else(|| {
                AppError::NotFound(format!(
                    "no sealed candidate for subject {}; reverify against the current seal",
                    verdict.subject_hash,
                ))
            })?;
        if seal.criteria_hash != verdict.criteria_hash {
            return Err(AppError::Conflict(format!(
                "criteria changed since the seal (task {}); stale verdict, reverify",
                seal.task_id,
            )));
        }
        if !verdict
            .evidence_ids
            .iter()
            .any(|id| id == seal.input_hash.as_str())
        {
            return Err(AppError::Conflict(format!(
                "verdict cites a different input than the sealed one (task {}); stale, reverify",
                seal.task_id,
            )));
        }
        let entry = self
            .status
            .entry(seal.task_id)
            .or_insert((TaskStatus::AwaitingVerification, 0));
        let (task_status, repair_rounds_used) = match verdict.outcome {
            VerificationOutcome::Pass => (TaskStatus::Succeeded, entry.1),
            VerificationOutcome::Fail if entry.1 < seal.max_repair_rounds => {
                entry.1 += 1;
                (TaskStatus::AwaitingVerification, entry.1)
            }
            VerificationOutcome::Fail => (TaskStatus::Failed, entry.1),
            VerificationOutcome::Blocked => (TaskStatus::Blocked, entry.1),
        };
        entry.0 = task_status;
        Ok(VerificationReceipt {
            task_id: seal.task_id,
            candidate_hash: seal.candidate_hash.clone(),
            outcome: verdict.outcome,
            task_status,
            repair_rounds_used,
        })
    }

    /// The task status this service currently records, if the task was sealed.
    pub fn task_status(&self, task: TaskId) -> Option<TaskStatus> {
        self.status.get(&task).map(|(status, _)| *status)
    }
}
