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
    MissingDependency { task: TaskId, dependency: TaskId },
    DependencyCycle,
    MissingVerification(TaskId),
    InvalidPlan(String),
    InvalidCommand(String),
    InvalidTransition(String),
    RunNotCreated,
    RunAlreadyCreated,
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
        }
    }
}

impl std::error::Error for DomainError {}
