//! Versioned Codex app-server gateway: the [`AgentGateway`] implementation.
//!
//! T1 scope: versioned framing, correlation, bounded buffering and the
//! no-silent-fallback policy are fully implemented. P2/T2 wires the real
//! [`ProcessSupervisor`]: [`CodexGateway::open`] spawns the app-server as an
//! owned, job-contained process and [`CodexGateway::cancel`] terminates it
//! through the supervisor, so no session outlives Central and a recycled PID
//! is never killed. Stdio RPC frames still ride the T1 path
//! ([`CodexGateway::ingest_wire_frame`]); the T3 inbox binds attempts to
//! events. Anything not yet attached reports honestly instead of pretending.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use central_application::contracts::{
    AppError, CancelObservation, Capabilities, DispatchError, ExecutionRef, ExternalAnswer,
    ExternalRequest, OpenSession, ProcessIdentity, ProcessSpec, ReconcileEvidence, SendTurn,
    SessionRef, TurnRef,
};
use central_application::ports::{AgentGateway, ProcessSupervisor};
use central_domain::{AnswerPayload, AttemptId, AttemptStatus, Observation, ResolvedProfile};

use super::requests::{normalize_request, observation_for, PendingRequestTable};
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
    /// Arguments for spawning the app-server binary (default `app-server`).
    /// Owned by P2/T2: `open` spawns `binary` with these args through the
    /// real supervisor.
    pub app_server_args: Vec<String>,
    /// Models proven servable by a real harness or config smoke (for example
    /// an observed `initialize` result or a versioned model list). Only these
    /// are advertised as `model:<id>` features. Entries must come from
    /// observed evidence, never from the request: an unsurveyed model stays
    /// unadvertised so `resolve_profile` raises `ModelUnavailable` instead of
    /// silently falling back. Empty — the default while no smoke has proven
    /// anything — advertises no model at all.
    pub surveyed_models: Vec<String>,
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
            app_server_args: vec!["app-server".to_string()],
            surveyed_models: Vec::new(),
        }
    }
}

/// One live session: the correlation reference plus the owned process that
/// backs it. The process is supervised (job-contained, identity-checked),
/// so cancelling the session provably ends the process tree.
struct ManagedSession {
    session: SessionRef,
    identity: ProcessIdentity,
}

struct GatewayState {
    buffer: EventBuffer,
    closed: bool,
    closed_reason: Option<String>,
    sessions: HashMap<AttemptId, ManagedSession>,
    /// Gateway-side harness requests. The application drains these into the
    /// durable inbox (persist-before-notice); answers route through this
    /// table with a Once scope.
    requests: PendingRequestTable,
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
                sessions: HashMap::new(),
                requests: PendingRequestTable::new(),
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
                let event = super::stdio::BufferedEvent {
                    id: Some(id),
                    method,
                    params,
                    is_request: true,
                };
                // Inbox traffic normalizes into the gateway-side request
                // table here; a replay keeps the first copy, never a second
                // entry. The application persists it before any UI notice.
                if let Some(normalized) = normalize_request(&self.epoch, &event) {
                    state.requests.record(normalized);
                }
                state.buffer.push(event);
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
        // Never self-attest the requested model: only models surveyed from a
        // real harness/config smoke are advertised as `model:<id>` features.
        // An unsurveyed model stays unadvertised so `resolve_profile` treats
        // it as unavailable and raises `ModelUnavailable` — never a silent
        // fallback.
        let mut features = vec![
            "initialize".to_string(),
            "thread/start".to_string(),
            "thread/resume".to_string(),
            "thread/read".to_string(),
            "turn/start".to_string(),
            "item/commandExecution/requestApproval".to_string(),
            "item/tool/requestUserInput".to_string(),
            "serverRequest/resolved".to_string(),
        ];
        for model in &self.config.surveyed_models {
            features.push(format!("model:{model}"));
        }
        Ok(Capabilities {
            provider: "codex".to_string(),
            supports_resume: true,
            features,
        })
    }

    async fn open(&self, request: OpenSession) -> Result<SessionRef, AppError> {
        self.ensure_compatible()?;
        // Spawn the app-server as an owned, job-contained process through
        // the real supervisor. Stdio RPC attach rides on top of this in a
        // later task; the session is correlation-bound to the process from
        // birth so cancel/reconcile already operate on the right tree.
        let identity = self
            .supervisor
            .spawn(&ProcessSpec {
                program: self.config.binary.clone(),
                args: self.config.app_server_args.clone(),
                working_dir: None,
            })
            .await
            .map_err(|error| match error {
                AppError::Transport(message) => AppError::Transport(format!(
                    "cannot spawn app-server '{}': {message}",
                    self.config.binary
                )),
                other => other,
            })?;
        let session = SessionRef {
            provider: "codex".to_string(),
            instance_epoch: self.epoch.clone(),
            thread_id: uuid::Uuid::new_v4().to_string(),
        };
        self.state.lock().expect("gateway state").sessions.insert(
            request.attempt_id,
            ManagedSession {
                session: session.clone(),
                identity,
            },
        );
        Ok(session)
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

    async fn observe(&self, session: &SessionRef) -> Result<Vec<Observation>, AppError> {
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
        if state.buffer.is_empty() {
            return Ok(Vec::new());
        }
        // Bind buffered events to the attempt that owns this session. Events
        // for other threads stay buffered for their own session; events no
        // live session owns are reconcile-required evidence, never silent
        // drops and never fabricated Observations.
        let attempt_id = state
            .sessions
            .iter()
            .find(|(_, managed)| {
                managed.session.thread_id == session.thread_id
                    && managed.session.instance_epoch == session.instance_epoch
            })
            .map(|(attempt_id, _)| *attempt_id);
        let Some(attempt_id) = attempt_id else {
            let pending = state.buffer.len();
            return Err(AppError::Transport(format!(
                "reconcile-required: {pending} unattributed notification(s) for an unknown \
                 session; Central never fabricates Observations"
            )));
        };
        let epoch = self.epoch.clone();
        let mut matched = Vec::new();
        let mut rest = Vec::new();
        for event in state.buffer.drain() {
            let thread = event
                .params
                .get("threadId")
                .or_else(|| event.params.get("thread_id"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            if thread == session.thread_id {
                matched.push(observation_for(&epoch, attempt_id, &event));
            } else {
                rest.push(event);
            }
        }
        if matched.is_empty() {
            for event in rest {
                state.buffer.push(event);
                if state.buffer.overflowed() {
                    break;
                }
            }
            if state.buffer.overflowed() {
                return Err(AppError::Transport(
                    "reconcile-required: notification buffer overflowed; events may have been \
                     dropped, full reconcile instead of silent resumption"
                        .to_string(),
                ));
            }
            let pending = state.buffer.len();
            return Err(AppError::Transport(format!(
                "reconcile-required: {pending} unattributed notification(s) for an unknown \
                 session; Central never fabricates Observations"
            )));
        }
        for event in rest {
            state.buffer.push(event);
            if state.buffer.overflowed() {
                break;
            }
        }
        if state.buffer.overflowed() {
            return Err(AppError::Transport(
                "reconcile-required: notification buffer overflowed; events may have been \
                 dropped, full reconcile instead of silent resumption"
                    .to_string(),
            ));
        }
        Ok(matched)
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
        match &answer.payload {
            AnswerPayload::Plain(text) if text.is_empty() => {
                return Err(DispatchError::Rejected("empty external answer".to_string()));
            }
            AnswerPayload::Plain(_) => {}
            AnswerPayload::Secret(_) => {
                // The gateway never sees credentials: the application resolves
                // the memory-only reference before calling.
                return Err(DispatchError::Rejected(
                    "secret answers must be resolved to plain text by the caller".to_string(),
                ));
            }
        }
        // Once scope: the table refuses unknown, not-yet-persisted and
        // already-answered keys instead of resending.
        self.state
            .lock()
            .expect("gateway state")
            .requests
            .consume_answer(&answer.key)
            .map_err(|error: AppError| DispatchError::Rejected(error.to_string()))
    }

    async fn drain_requests(&self, session: &SessionRef) -> Result<Vec<ExternalRequest>, AppError> {
        self.ensure_compatible()?;
        let mut state = self.state.lock().expect("gateway state");
        let attempt_id = state
            .sessions
            .iter()
            .find(|(_, managed)| {
                managed.session.thread_id == session.thread_id
                    && managed.session.instance_epoch == session.instance_epoch
            })
            .map(|(attempt_id, _)| *attempt_id);
        Ok(state
            .requests
            .drain_for_thread(&session.thread_id, attempt_id))
    }

    async fn cancel(&self, target: &ExecutionRef) -> Result<CancelObservation, AppError> {
        self.ensure_compatible()?;
        let identity = {
            let mut state = self.state.lock().expect("gateway state");
            match state.sessions.remove(&target.attempt_id) {
                Some(managed) => {
                    // A cancel naming a stale epoch must never end the live
                    // process: put the session back and refuse loudly.
                    if target.session.instance_epoch != managed.session.instance_epoch {
                        let epoch = target.session.instance_epoch.clone();
                        state.sessions.insert(target.attempt_id, managed);
                        return Err(AppError::Conflict(format!(
                            "stale cancel: epoch '{epoch}' does not match the live session"
                        )));
                    }
                    managed.identity
                }
                None => {
                    return Ok(CancelObservation {
                        cancelled: false,
                        detail: format!(
                            "no live session for attempt {}; nothing to cancel",
                            target.attempt_id
                        ),
                    });
                }
            }
        };
        // Identity-checked: a recycled PID is never killed (the supervisor
        // refuses stale identities instead of guessing).
        self.supervisor.terminate(&identity).await?;
        Ok(CancelObservation {
            cancelled: true,
            detail: format!(
                "terminated owned app-server process {} for attempt {}",
                identity.pid, target.attempt_id
            ),
        })
    }

    async fn reconcile(&self, target: &ExecutionRef) -> Result<ReconcileEvidence, AppError> {
        self.ensure_compatible()?;
        let identity = {
            let state = self.state.lock().expect("gateway state");
            match state.sessions.get(&target.attempt_id) {
                Some(managed) => managed.identity.clone(),
                None => {
                    return Ok(ReconcileEvidence {
                        attempt_id: target.attempt_id,
                        status: AttemptStatus::UnknownOutcome,
                        detail: "no live session for attempt; outcome unknown, re-read \
                                 thread state before reuse"
                            .to_string(),
                    });
                }
            }
        };
        let state = self.supervisor.inspect(&identity).await?;
        Ok(ReconcileEvidence {
            attempt_id: target.attempt_id,
            status: AttemptStatus::Reconciling,
            detail: format!(
                "owned app-server process {} is {state:?}; reconcile against thread/read; \
                 stdio RPC attach arrives after P2/T2",
                identity.pid
            ),
        })
    }
}
