use async_trait::async_trait;
use central_domain::{Observation, ResolvedProfile};

use crate::contracts::{
    AppError, CancelObservation, Capabilities, DispatchError, ExecutionRef, ExternalAnswer,
    OpenSession, ReconcileEvidence, SendTurn, SessionRef, TurnRef,
};

/// Port to an external agent harness. `observe` drains a bounded
/// notification buffer; it does not poll inference. Buffer overflow or a
/// lost connection must surface as reconcile-required evidence upstream,
/// never silently drop an important event.
#[async_trait]
pub trait AgentGateway: Send + Sync {
    async fn capabilities(&self, profile: &ResolvedProfile) -> Result<Capabilities, AppError>;
    async fn open(&self, request: OpenSession) -> Result<SessionRef, AppError>;
    async fn send(&self, request: SendTurn) -> Result<TurnRef, DispatchError>;
    async fn observe(&self, session: &SessionRef) -> Result<Vec<Observation>, AppError>;
    async fn answer(&self, answer: ExternalAnswer) -> Result<(), DispatchError>;
    async fn cancel(&self, target: &ExecutionRef) -> Result<CancelObservation, AppError>;
    async fn reconcile(&self, target: &ExecutionRef) -> Result<ReconcileEvidence, AppError>;
    /// Normalized harness requests (approvals/questions) bound to this
    /// session that Central has not persisted yet. Draining removes them
    /// from the gateway: the caller must persist them before any UI notice.
    /// The default is empty for gateways without a request table.
    async fn drain_requests(
        &self,
        _session: &SessionRef,
    ) -> Result<Vec<crate::contracts::ExternalRequest>, AppError> {
        Ok(Vec::new())
    }
}
