use async_trait::async_trait;

use crate::contracts::{AppError, ProcessIdentity, ProcessSpec, ProcessState};

/// Supervises OS processes owned by Central. The Windows implementation
/// arrives with the daemon task; no OS handle appears in this contract.
#[async_trait]
pub trait ProcessSupervisor: Send + Sync {
    async fn spawn(&self, spec: &ProcessSpec) -> Result<ProcessIdentity, AppError>;
    async fn inspect(&self, identity: &ProcessIdentity) -> Result<ProcessState, AppError>;
    async fn terminate(&self, identity: &ProcessIdentity) -> Result<(), AppError>;
}
