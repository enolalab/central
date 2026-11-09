// Deterministic fake agent for restart/dispatch tests. This file is the
// single fixture source: the daemon only includes it behind the
// `test-support` feature, and tests read its counters through the
// test-only `/v1/test/fake-sends` endpoint. A release build never
// includes this code, so it can never serve fake output.
//
// Included with `include!` by `central-server` under `test-support`;
// it expects `async_trait`, `central_application` and `central_domain`
// to be in scope of the including crate.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use central_application::contracts::{
    AppError, CancelObservation, Capabilities, DispatchError, ExecutionRef, ExternalAnswer,
    OpenSession, ReconcileEvidence, SendTurn, SessionRef, TurnRef,
};
use central_domain::{AttemptStatus, Observation, ResolvedProfile};

/// Fake harness with observable send/cancel counters. In ambiguous mode
/// every external call fails as if the transport was lost after the
/// request went out: the outcome is unknown, never a success.
pub struct FakeAgent {
    pub sends: AtomicUsize,
    pub cancels: AtomicUsize,
    pub ambiguous: AtomicBool,
}

impl FakeAgent {
    pub fn new() -> Self {
        Self {
            sends: AtomicUsize::new(0),
            cancels: AtomicUsize::new(0),
            ambiguous: AtomicBool::new(false),
        }
    }

    pub fn send_count(&self) -> usize {
        self.sends.load(Ordering::SeqCst)
    }

    pub fn cancel_count(&self) -> usize {
        self.cancels.load(Ordering::SeqCst)
    }

    pub fn set_ambiguous(&self, ambiguous: bool) {
        self.ambiguous.store(ambiguous, Ordering::SeqCst);
    }

    fn is_ambiguous(&self) -> bool {
        self.ambiguous.load(Ordering::SeqCst)
    }
}

impl Default for FakeAgent {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl central_application::ports::AgentGateway for FakeAgent {
    async fn capabilities(
        &self,
        _profile: &ResolvedProfile,
    ) -> Result<Capabilities, AppError> {
        Ok(Capabilities {
            provider: "fake".to_string(),
            supports_resume: true,
            features: vec!["test-support".to_string()],
        })
    }

    async fn open(&self, _request: OpenSession) -> Result<SessionRef, AppError> {
        Ok(SessionRef {
            provider: "fake".to_string(),
            instance_epoch: "epoch-1".to_string(),
            thread_id: "thread-1".to_string(),
        })
    }

    async fn send(&self, request: SendTurn) -> Result<TurnRef, DispatchError> {
        self.sends.fetch_add(1, Ordering::SeqCst);
        if self.is_ambiguous() {
            return Err(DispatchError::OutcomeUnknown(
                "fake transport lost after send".to_string(),
            ));
        }
        Ok(TurnRef {
            session: request.session,
            turn_id: "turn-1".to_string(),
        })
    }

    async fn observe(&self, _session: &SessionRef) -> Result<Vec<Observation>, AppError> {
        Ok(Vec::new())
    }

    async fn answer(&self, _answer: ExternalAnswer) -> Result<(), DispatchError> {
        if self.is_ambiguous() {
            return Err(DispatchError::OutcomeUnknown(
                "fake transport lost after answer".to_string(),
            ));
        }
        Ok(())
    }

    async fn cancel(&self, _target: &ExecutionRef) -> Result<CancelObservation, AppError> {
        self.cancels.fetch_add(1, Ordering::SeqCst);
        if self.is_ambiguous() {
            return Err(AppError::Transport(
                "fake transport lost after cancel request".to_string(),
            ));
        }
        Ok(CancelObservation {
            cancelled: true,
            detail: "fake cancel observed".to_string(),
        })
    }

    async fn reconcile(&self, target: &ExecutionRef) -> Result<ReconcileEvidence, AppError> {
        Ok(ReconcileEvidence {
            attempt_id: target.attempt_id,
            status: AttemptStatus::UnknownOutcome,
            detail: "fake has no evidence".to_string(),
        })
    }
}
