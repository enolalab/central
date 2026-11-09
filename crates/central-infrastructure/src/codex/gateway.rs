//! Versioned Codex app-server gateway: the [`AgentGateway`] implementation.
//!
//! T1 scope: versioned framing, correlation, bounded buffering and the
//! no-silent-fallback policy are fully implemented. There is no live child
//! process yet — spawning and the stdio handshake wait for the P2/T2
//! supervisor, so [`CodexGateway::open`] and [`CodexGateway::send`] report
//! honestly instead of pretending. The T2 reader loop feeds frames through
//! [`CodexGateway::ingest_wire_frame`]; the T3 inbox binds attempts to events.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use central_application::contracts::{
    AppError, CancelObservation, Capabilities, DispatchError, ExecutionRef, ExternalAnswer,
    OpenSession, ReconcileEvidence, SendTurn, SessionRef, TurnRef,
};
use central_application::ports::{AgentGateway, ProcessSupervisor};
use central_domain::{AttemptStatus, Observation, ResolvedProfile};

use super::stdio::EventBuffer;
use super::version::{check_model_match, check_supported_version, parse_cli_version, VersionError};
use super::wire::{parse_frame, sanitize_text, IncomingFrame, FRAME_CAP_BYTES};

/// Static configuration for one gateway instance.
#[derive(Clone, Debug)]
pub struct CodexConfig {
    /// Program used to spawn the app-server (P2/T2 wires it; T1 never spawns).
    pub binary: String,
    /// Pinned CLI build this gateway speaks, e.g. `0.167.0-alpha.3.2`.
    pub cli_version: String,
    /// Extra per-deployment allowlist. Empty means the global allowlist.
    pub allowed_versions: Vec<String>,
    /// Informational frame cap; the wire cap is [`FRAME_CAP_BYTES`].
    pub frame_cap_bytes: usize,
    /// Bounded notification buffer capacity.
    pub notification_capacity: usize,
    /// Per-request wait budget in milliseconds.
    pub request_timeout_ms: u64,
    /// Must stay false: Central never enables provider model fallback.
    pub allow_provider_model_fallback: bool,
}

impl Default for CodexConfig {
    fn default() -> Self {
        Self {
            binary: "codex".to_string(),
            cli_version: super::version::SUPPORTED_CLI_VERSION.to_string(),
            allowed_versions: Vec::new(),
            frame_cap_bytes: FRAME_CAP_BYTES,
            notification_capacity: 256,
            request_timeout_ms: 30_000,
            allow_provider_model_fallback: false,
        }
    }
}

struct GatewayState {
    buffer: EventBuffer,
    closed: bool,
    closed_reason: Option<String>,
}

/// Codex app-server adapter. One instance per supervised process; the
/// constructor mints a fresh `instance_epoch` so RPC ids and pending answers
/// never cross a process boundary (no accept replay after restart).
pub struct CodexGateway {
    supervisor: Arc<dyn ProcessSupervisor>,
    config: CodexConfig,
    epoch: String,
    state: Mutex<GatewayState>,
}

impl CodexGateway {
    pub fn new(supervisor: Arc<dyn ProcessSupervisor>, config: CodexConfig) -> Self {
        Self {
            supervisor,
            epoch: uuid::Uuid::new_v4().to_string(),
            state: Mutex::new(GatewayState {
                buffer: EventBuffer::new(config.notification_capacity),
                closed: false,
                closed_reason: None,
            }),
            config,
        }
    }

    /// The process supervisor this gateway will spawn through (T2 wiring).
    pub fn supervisor(&self) -> &Arc<dyn ProcessSupervisor> {
        &self.supervisor
    }

    /// Fresh instance epoch minted for this gateway's process.
    pub fn epoch(&self) -> &str {
        &self.epoch
    }

    /// True once events may have been lost (overflow or close).
    pub fn reconcile_required(&self) -> bool {
        let state = self.state.lock().expect("gateway state");
        state.closed || state.buffer.overflowed()
    }

    /// Feed one raw wire line from the T2 reader loop. Infallible
    /// bookkeeping: malformed or oversize input closes the gateway and latches
    /// reconcile-required instead of producing a partial event.
    pub fn ingest_wire_frame(&self, line: &str) {
        let line = line.trim_end_matches(['\n', '\r']);
        let mut state = self.state.lock().expect("gateway state");
        if state.closed {
            return;
        }
        if line.len() > FRAME_CAP_BYTES {
            state.closed = true;
            state.closed_reason = Some(format!(
                "oversize frame ({} bytes, cap {FRAME_CAP_BYTES}); transport closed",
                line.len()
            ));
            return;
        }
        match parse_frame(line) {
            Ok(IncomingFrame::Notification { method, params }) => {
                state.buffer.push(super::stdio::BufferedEvent {
                    id: None,
                    method,
                    params,
                    is_request: false,
                });
            }
            Ok(IncomingFrame::ServerRequest { id, method, params }) => {
                state.buffer.push(super::stdio::BufferedEvent {
                    id: Some(id),
                    method,
                    params,
                    is_request: true,
                });
            }
            Ok(IncomingFrame::Response { .. }) => {
                // No outstanding request table exists before T2 wiring; an
                // unsolicited response means the streams are out of sync.
                state.closed = true;
                state.closed_reason =
                    Some("unsolicited response with no outstanding request".to_string());
            }
            Err(error) => {
                state.closed = true;
                state.closed_reason = Some(sanitize_text(&error.to_string()).into_owned());
            }
        }
    }

    /// Check the requested model against the model the server actually
    /// resolved. Mismatch is a capability error, never a silent fallback.
    pub fn verify_model_match(&self, requested: &str, observed: &str) -> Result<(), AppError> {
        check_model_match(requested, observed).map_err(into_capability)
    }

    fn ensure_compatible(&self) -> Result<(), AppError> {
        let version = parse_cli_version(&self.config.cli_version).map_err(into_capability)?;
        check_supported_version(&version).map_err(into_capability)?;
        if !self.config.allowed_versions.is_empty()
            && !self
                .config
                .allowed_versions
                .iter()
                .any(|v| v == version.as_str())
        {
            return Err(AppError::Capability(format!(
                "unsupported codex CLI version '{}'; gateway allowlist is [{}]",
                version.as_str(),
                self.config.allowed_versions.join(", "),
            )));
        }
        if self.config.allow_provider_model_fallback {
            return Err(AppError::Capability(
                "central refuses allowProviderModelFallback=true: the resolved model must equal \
                 the requested model, otherwise dispatch fails as a capability error"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

fn into_capability(error: VersionError) -> AppError {
    AppError::Capability(error.to_string())
}

#[async_trait]
impl AgentGateway for CodexGateway {
    async fn capabilities(&self, _profile: &ResolvedProfile) -> Result<Capabilities, AppError> {
        self.ensure_compatible()?;
        Ok(Capabilities {
            provider: "codex".to_string(),
            supports_resume: true,
            features: vec![
                "initialize".to_string(),
                "thread/start".to_string(),
                "thread/resume".to_string(),
                "thread/read".to_string(),
                "turn/start".to_string(),
                "item/commandExecution/requestApproval".to_string(),
                "item/tool/requestUserInput".to_string(),
                "serverRequest/resolved".to_string(),
            ],
        })
    }

    async fn open(&self, _request: OpenSession) -> Result<SessionRef, AppError> {
        self.ensure_compatible()?;
        // No live stdio transport before the P2/T2 supervisor exists.
        Err(AppError::Transport(
            "live stdio transport not attached (waits for P2/T2 supervisor); \
             cannot open session from fixtures"
                .to_string(),
        ))
    }

    async fn send(&self, _request: SendTurn) -> Result<TurnRef, DispatchError> {
        if let Err(error) = self.ensure_compatible() {
            return Err(DispatchError::Rejected(error.to_string()));
        }
        // Nothing was written, so this is DefinitelyNotSent, not
        // OutcomeUnknown. The timeout-after-write path lives in RpcClient.
        Err(DispatchError::DefinitelyNotSent(
            "live stdio transport not attached (waits for P2/T2 supervisor); \
             no turn was written"
                .to_string(),
        ))
    }

    async fn observe(&self, _session: &SessionRef) -> Result<Vec<Observation>, AppError> {
        self.ensure_compatible()?;
        let mut state = self.state.lock().expect("gateway state");
        if state.closed {
            return Err(AppError::Transport(format!(
                "reconcile-required: transport closed ({})",
                state.closed_reason.as_deref().unwrap_or("unknown")
            )));
        }
        if state.buffer.overflowed() {
            return Err(AppError::Transport(
                "reconcile-required: notification buffer overflowed; events may have been \
                 dropped, full reconcile instead of silent resumption"
                    .to_string(),
            ));
        }
        if !state.buffer.is_empty() {
            let pending = state.buffer.len();
            return Err(AppError::Transport(format!(
                "reconcile-required: {pending} unattributed notification(s); attempt binding \
                 arrives with the T3 inbox, Central never fabricates Observations"
            )));
        }
        let _ = &mut state;
        Ok(Vec::new())
    }

    async fn answer(&self, answer: ExternalAnswer) -> Result<(), DispatchError> {
        if let Err(error) = self.ensure_compatible() {
            return Err(DispatchError::Rejected(error.to_string()));
        }
        // Correlation is epoch-scoped: an answer recorded for a previous
        // process epoch is stale and must never replay as an accept.
        if answer.key.session.instance_epoch != self.epoch {
            return Err(DispatchError::Rejected(format!(
                "stale external answer: epoch '{}' does not match current '{}'",
                answer.key.session.instance_epoch, self.epoch
            )));
        }
        if answer.answer.is_empty() {
            return Err(DispatchError::Rejected("empty external answer".to_string()));
        }
        // The pending-request table that routes this answer arrives with T3.
        Ok(())
    }

    async fn cancel(&self, target: &ExecutionRef) -> Result<CancelObservation, AppError> {
        self.ensure_compatible()?;
        Ok(CancelObservation {
            cancelled: false,
            detail: format!(
                "no live session for attempt {}; cancellation executes through the \
                 P2/T2 supervisor",
                target.attempt_id
            ),
        })
    }

    async fn reconcile(&self, target: &ExecutionRef) -> Result<ReconcileEvidence, AppError> {
        self.ensure_compatible()?;
        Ok(ReconcileEvidence {
            attempt_id: target.attempt_id,
            status: AttemptStatus::Reconciling,
            detail: "gateway awaiting live transport; reconcile against thread/read once \
                     the P2/T2 supervisor attaches stdio"
                .to_string(),
        })
    }
}
