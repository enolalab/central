use std::fmt;

use crate::{PlanHash, TaskId};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DomainError {
    Forbidden,
    StaleRevision,
    PlanHashMismatch,
    UnknownPlan(PlanHash),
    UnsupportedPlanVersion(u32),
    EmptyPlan,
    DuplicateTaskId(TaskId),
    MissingDependency {
        task: TaskId,
        dependency: TaskId,
    },
    DependencyCycle,
    MissingVerification(TaskId),
    InvalidPlan(String),
    InvalidCommand(String),
    InvalidTransition(String),
    RunNotCreated,
    RunAlreadyCreated,
    /// The reply targets a request that is no longer live: wrong epoch,
    /// wrong preview, already answered, resolved, or stale. Never accept.
    StaleRequest,
    /// No inbox entry exists for this key. A plan hash is never an inbox
    /// key: plan approval goes through `ApprovePlan`.
    UnknownRequest(String),
    /// The same request id was reused inside one epoch with different
    /// content. Not a replay; refuse instead of overwriting.
    RequestConflict(String),
    /// The scheduler asked to queue work that is not ready to start:
    /// a closed gate, an unsatisfied dependency, a live attempt already
    /// covering the task, a held resource, or a full concurrency bound.
    /// Deferrable: a later tick may succeed. Loud misconfiguration (an
    /// empty input or resource key) never maps here.
    QueueConflict(String),
    /// The named task is not part of this run.
    UnknownTask(TaskId),
}

impl fmt::Display for DomainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Forbidden => write!(f, "actor is not allowed to perform this command"),
            Self::StaleRevision => write!(f, "expected revision does not match current revision"),
            Self::PlanHashMismatch => {
                write!(f, "target plan hash does not match the pending plan")
            }
            Self::UnknownPlan(hash) => write!(f, "unknown plan: {hash}"),
            Self::UnsupportedPlanVersion(v) => write!(f, "unsupported plan version: {v}"),
            Self::EmptyPlan => write!(f, "plan must contain at least one task"),
            Self::DuplicateTaskId(id) => write!(f, "duplicate task id: {id}"),
            Self::MissingDependency { task, dependency } => {
                write!(f, "task {task} depends on unknown task {dependency}")
            }
            Self::DependencyCycle => write!(f, "plan task dependencies contain a cycle"),
            Self::MissingVerification(id) => {
                write!(f, "task {id} has no verification method and criteria")
            }
            Self::InvalidPlan(msg) => write!(f, "invalid plan: {msg}"),
            Self::InvalidCommand(msg) => write!(f, "invalid command: {msg}"),
            Self::InvalidTransition(msg) => write!(f, "invalid transition: {msg}"),
            Self::RunNotCreated => write!(f, "run has not been created"),
            Self::RunAlreadyCreated => write!(f, "run has already been created"),
            Self::StaleRequest => write!(
                f,
                "stale request: the reply is not for the live request (wrong epoch, \
                 preview, or the entry is already answered, resolved or stale)"
            ),
            Self::UnknownRequest(key) => write!(
                f,
                "unknown inbox request: {key} (plan hashes are not inbox keys)"
            ),
            Self::RequestConflict(key) => write!(
                f,
                "request id reused in one epoch with different content: {key}"
            ),
            Self::QueueConflict(reason) => write!(f, "task is not queueable now: {reason}"),
            Self::UnknownTask(id) => write!(f, "unknown task: {id}"),
        }
    }
}

impl std::error::Error for DomainError {}
