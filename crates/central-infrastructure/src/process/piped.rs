//! Piped spawn for RPC-backed agent processes.
//!
//! [`ProcessSupervisor`] only hands out identities: stdio stays null there
//! so buffered output can never block a child nobody reads. A gateway that
//! speaks JSON-RPC needs owned stdin/stdout pipes to the same supervised
//! child. [`PipedSpawner`] provides exactly that: the child is supervised
//! identically (kernel creation time, job assignment, identity-checked
//! kill through the owned handle) and the caller additionally owns the
//! pipes. The default body refuses honestly so supervisors that cannot
//! pipe (test stubs) fail closed instead of pretending.

use async_trait::async_trait;

use central_application::contracts::{AppError, ProcessIdentity, ProcessSpec};

/// Owned stdio pipes to a supervised child: write side plus read side.
pub struct PipedStdio {
    pub stdin: Box<dyn tokio::io::AsyncWrite + Send + Unpin>,
    pub stdout: Box<dyn tokio::io::AsyncRead + Send + Unpin>,
}

/// Spawn with owned pipes. Implementors must supervise the child exactly
/// like [`ProcessSupervisor::spawn`](central_application::ports::ProcessSupervisor::spawn)
/// (same identity, containment and kill path); only stdio differs.
#[async_trait]
pub trait PipedSpawner: Send + Sync {
    async fn spawn_piped(
        &self,
        spec: &ProcessSpec,
    ) -> Result<(ProcessIdentity, PipedStdio), AppError> {
        let _ = spec;
        Err(AppError::Capability(
            "piped spawn is not supported by this supervisor; RPC attach refuses instead of pretending".to_string(),
        ))
    }
}
