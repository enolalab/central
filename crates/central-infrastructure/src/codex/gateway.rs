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
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use central_application::contracts::{
    AppError, CancelObservation, Capabilities, DispatchError, ExecutionRef, ExternalAnswer,
    ExternalRequest, OpenSession, ProcessIdentity, ProcessSpec, ReconcileEvidence, SendTurn,
    SessionRef, TurnRef,
};
use central_application::ports::{AgentGateway, ProcessSupervisor};
use central_domain::{
    canonical_key, AnswerPayload, AttemptId, AttemptStatus, InboxKind, Observation, ResolvedProfile,
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::process::PipedSpawner;

use super::requests::{normalize_request, observation_for, PendingRequestTable};
use super::stdio::{BufferedEvent, EventBuffer, IncomingEvent, RpcClient};
use super::version::{check_model_match, check_supported_version, parse_cli_version, VersionError};
use super::wire::{parse_frame, sanitize_text, IncomingFrame, RpcId, FRAME_CAP_BYTES};

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

/// Owned stdio halves of one supervised app-server child, joined into a
/// single bidirectional stream for [`RpcClient`].
struct ChildStdio {
    stdin: Box<dyn AsyncWrite + Send + Unpin>,
    stdout: Box<dyn AsyncRead + Send + Unpin>,
}

impl AsyncRead for ChildStdio {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stdout).poll_read(cx, buf)
    }
}

impl AsyncWrite for ChildStdio {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.stdin).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stdin).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stdin).poll_shutdown(cx)
    }
}

/// Live RPC attachment of one session: the client plus handshake/thread
/// state. Taken out of the session mutex for every blocking call so a
/// long turn wait never holds a lock across an await.
struct AttachedTransport {
    client: RpcClient<ChildStdio>,
    initialized: bool,
    server_thread: Option<String>,
}

/// Wire route back to one server request: the JSON-RPC id to answer plus
/// the original params (question ids live there). Memory-only, never
/// persisted; dropped with the session.
struct WireRoute {
    id: RpcId,
    kind: InboxKind,
    params: serde_json::Value,
}

/// One live session: the correlation reference plus the owned process that
/// backs it. The process is supervised (job-contained, identity-checked),
/// so cancelling the session provably ends the process tree. When the
/// gateway was built with a [`PipedSpawner`], the session additionally
/// owns the stdio pipes the RPC attach rides on.
struct ManagedSession {
    session: SessionRef,
    identity: ProcessIdentity,
    profile: ResolvedProfile,
    workspace: String,
    resume_from: Option<SessionRef>,
    io: Mutex<Option<AttachedTransport>>,
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
    /// Wire routes for drained requests, keyed by canonical request key.
    wire_routes: HashMap<String, WireRoute>,
}

/// Codex app-server adapter. One instance per supervised process; the
/// constructor mints a fresh `instance_epoch` so RPC ids and pending answers
/// never cross a process boundary (no accept replay after restart).
pub struct CodexGateway {
    supervisor: Arc<dyn ProcessSupervisor>,
    piped: Option<Arc<dyn PipedSpawner>>,
    config: CodexConfig,
    epoch: String,
    state: Mutex<GatewayState>,
}

impl CodexGateway {
    pub fn new(supervisor: Arc<dyn ProcessSupervisor>, config: CodexConfig) -> Self {
        Self {
            supervisor,
            piped: None,
            epoch: uuid::Uuid::new_v4().to_string(),
            state: Mutex::new(GatewayState {
                buffer: EventBuffer::new(config.notification_capacity),
                closed: false,
                closed_reason: None,
                sessions: HashMap::new(),
                requests: PendingRequestTable::new(),
                wire_routes: HashMap::new(),
            }),
            config,
        }
    }

    /// Attach owned stdio pipes for RPC: without this, `open` supervises
    /// the process but carries no transport and `send` refuses loudly.
    pub fn with_piped(mut self, piped: Arc<dyn PipedSpawner>) -> Self {
        self.piped = Some(piped);
        self
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

    /// Take the RPC attachment out of a session for one blocking call. The
    /// mutex is only held for the take: no lock is ever held across an
    /// await. A missing attachment means the session was opened without a
    /// piped spawner (supervision only) or another call holds it — either
    /// way sending refuses loudly instead of writing nowhere.
    fn take_transport(
        &self,
        session: &SessionRef,
        attempt: AttemptId,
    ) -> Result<TakenSession, DispatchError> {
        let state = self.state.lock().expect("gateway state");
        let managed = state.sessions.get(&attempt).ok_or_else(|| {
            DispatchError::Rejected(format!("no live session for attempt {attempt}"))
        })?;
        if managed.session.thread_id != session.thread_id
            || managed.session.instance_epoch != session.instance_epoch
        {
            return Err(DispatchError::Rejected(
                "session reference does not match the live session".to_string(),
            ));
        }
        let attached = managed
            .io
            .lock()
            .expect("session io")
            .take()
            .ok_or_else(|| {
                DispatchError::Rejected(
                    "no attached stdio transport for this session (supervision only, \
                     or another call holds it); refusing instead of writing nowhere"
                        .to_string(),
                )
            })?;
        Ok(TakenSession {
            attempt_id: attempt,
            local: managed.session.clone(),
            profile: managed.profile.clone(),
            workspace: managed.workspace.clone(),
            resume_from: managed.resume_from.clone(),
            attached,
        })
    }

    /// Give the attachment back unless cancel removed the session meanwhile.
    fn restore_transport(&self, taken: TakenSession) {
        let state = self.state.lock().expect("gateway state");
        let live = state
            .sessions
            .get(&taken.attempt_id)
            .is_some_and(|managed| {
                managed.session.thread_id == taken.local.thread_id
                    && managed.session.instance_epoch == taken.local.instance_epoch
            });
        if live {
            if let Some(managed) = state.sessions.get(&taken.attempt_id) {
                *managed.io.lock().expect("session io") = Some(taken.attached);
            }
        }
        // Otherwise the session is gone: the transport drops here with the
        // dead process instead of being resurrected behind cancel.
    }

    /// Move newly arrived frames into the bounded buffer plus the request
    /// table. Stops at the first quiet window; a closed transport latches
    /// reconcile-required instead of losing events silently.
    async fn pump(&self, client: &mut RpcClient<ChildStdio>) {
        loop {
            match client.poll_event(Duration::from_millis(50)).await {
                Ok(IncomingEvent::Request { id, method, params }) => {
                    let event = BufferedEvent {
                        id: Some(id.clone()),
                        method: method.clone(),
                        params: params.clone(),
                        is_request: true,
                    };
                    let mut state = self.state.lock().expect("gateway state");
                    if let Some(normalized) = normalize_request(&self.epoch, &event) {
                        let key = &normalized.key;
                        let canonical = canonical_key(
                            &key.session.provider,
                            &key.session.instance_epoch,
                            &key.session.thread_id,
                            &key.turn_id,
                            &key.item_id,
                            &key.request_id,
                        );
                        state.wire_routes.insert(
                            canonical,
                            WireRoute {
                                id,
                                kind: normalized.kind,
                                params,
                            },
                        );
                        state.requests.record(normalized);
                    }
                    state.buffer.push(event);
                }
                Ok(IncomingEvent::Notification { method, params }) => {
                    let mut state = self.state.lock().expect("gateway state");
                    state.buffer.push(BufferedEvent {
                        id: None,
                        method,
                        params,
                        is_request: false,
                    });
                }
                Err(super::wire::RpcError::Timeout(_)) => break,
                Err(error) => {
                    let mut state = self.state.lock().expect("gateway state");
                    state.closed = true;
                    state.closed_reason = Some(sanitize_text(&error.to_string()).into_owned());
                    break;
                }
            }
        }
    }

    /// Drive the wire for one session, then hand the attachment back. Used
    /// by `observe` (and after every `send`) so arrivals never sit in the
    /// OS pipe while Central reports nothing. Best-effort: a missing or
    /// busy transport simply skips the pump.
    async fn pump_for(&self, session: &SessionRef, attempt: Option<AttemptId>) {
        let taken = (|| {
            let state = self.state.lock().expect("gateway state");
            let (attempt_id, managed) =
                match attempt.and_then(|id| state.sessions.get(&id).map(|managed| (id, managed))) {
                    Some(found) => found,
                    None => state
                        .sessions
                        .iter()
                        .find(|(_, managed)| {
                            managed.session.thread_id == session.thread_id
                                && managed.session.instance_epoch == session.instance_epoch
                        })
                        .map(|(id, managed)| (*id, managed))?,
                };
            if managed.session.thread_id != session.thread_id
                || managed.session.instance_epoch != session.instance_epoch
            {
                return None;
            }
            let attached = managed.io.lock().expect("session io").take()?;
            Some(TakenSession {
                attempt_id,
                local: managed.session.clone(),
                profile: managed.profile.clone(),
                workspace: managed.workspace.clone(),
                resume_from: managed.resume_from.clone(),
                attached,
            })
        })();
        if let Some(mut taken) = taken {
            self.pump(&mut taken.attached.client).await;
            self.restore_transport(taken);
        }
    }

    /// `initialize` handshake once per attachment. A timeout after the
    /// write is OutcomeUnknown (the server may yet answer); anything else
    /// refuses without starting any thread or turn.
    async fn ensure_initialized(
        &self,
        attached: &mut AttachedTransport,
        timeout: Duration,
    ) -> Result<(), DispatchError> {
        if attached.initialized {
            return Ok(());
        }
        let result = attached
            .client
            .request(
                "initialize",
                serde_json::json!({
                    "clientInfo": {
                        "name": "central",
                        "title": "Central",
                        "version": env!("CARGO_PKG_VERSION"),
                    },
                    "capabilities": { "experimentalApi": true },
                }),
                timeout,
            )
            .await
            .map_err(Self::into_dispatch)?;
        if !result.is_object() {
            return Err(DispatchError::Rejected(
                "initialize did not return a result object; refusing an unvalidated server"
                    .to_string(),
            ));
        }
        attached
            .client
            .notify("initialized", serde_json::json!({}))
            .await
            .map_err(Self::into_dispatch)?;
        attached.initialized = true;
        Ok(())
    }

    /// Bind the server thread once per attachment: resume the handover
    /// thread when one was supplied, else start a fresh one on the
    /// attempt's frozen profile. The observed model and server CLI build
    /// must match what was requested and allowlisted — mismatch is a
    /// capability error, never a silent fallback.
    async fn ensure_thread(
        &self,
        taken: &mut TakenSession,
        timeout: Duration,
    ) -> Result<String, DispatchError> {
        if let Some(thread) = taken.attached.server_thread.clone() {
            return Ok(thread);
        }
        let client = &mut taken.attached.client;
        let thread_id = if let Some(resume) = &taken.resume_from {
            let result = client
                .request(
                    "thread/resume",
                    serde_json::json!({ "threadId": resume.thread_id }),
                    timeout,
                )
                .await
                .map_err(Self::into_dispatch)?;
            let id = result["thread"]["id"].as_str().ok_or_else(|| {
                DispatchError::Rejected("thread/resume did not return a thread id".to_string())
            })?;
            if let Some(observed) = result.get("model").and_then(|m| m.as_str()) {
                self.verify_model_match(&taken.profile.model, observed)
                    .map_err(|error| DispatchError::Rejected(error.to_string()))?;
            }
            id.to_string()
        } else {
            let result = client
                .request(
                    "thread/start",
                    serde_json::json!({
                        "model": taken.profile.model,
                        "cwd": taken.workspace,
                        "sandbox": "workspace-write",
                        "approvalPolicy": "on-request",
                        "ephemeral": false,
                        "allowProviderModelFallback": false,
                        "config": { "model_reasoning_effort": taken.profile.effort },
                    }),
                    timeout,
                )
                .await
                .map_err(Self::into_dispatch)?;
            let observed = result["model"].as_str().ok_or_else(|| {
                DispatchError::Rejected(
                    "thread/start did not report the resolved model; refusing to run blind"
                        .to_string(),
                )
            })?;
            self.verify_model_match(&taken.profile.model, observed)
                .map_err(|error| DispatchError::Rejected(error.to_string()))?;
            if let Some(cli) = result["thread"]["cliVersion"].as_str() {
                if cli != self.config.cli_version {
                    return Err(DispatchError::Rejected(format!(
                        "server CLI build '{cli}' does not match the allowlisted '{}'; \
                         refusing an unvalidated protocol",
                        self.config.cli_version,
                    )));
                }
            }
            result["thread"]["id"]
                .as_str()
                .ok_or_else(|| {
                    DispatchError::Rejected("thread/start did not return a thread id".to_string())
                })?
                .to_string()
        };
        taken.attached.server_thread = Some(thread_id.clone());
        Ok(thread_id)
    }

    /// One turn on the bound server thread, then pump arrivals into the
    /// buffer. A timeout after the write is OutcomeUnknown: the turn may
    /// still run server-side, so it is never resent.
    async fn send_via(
        &self,
        taken: &mut TakenSession,
        request: &SendTurn,
        timeout: Duration,
    ) -> Result<TurnRef, DispatchError> {
        self.ensure_initialized(&mut taken.attached, timeout)
            .await?;
        let server_thread = self.ensure_thread(taken, timeout).await?;
        let result = taken
            .attached
            .client
            .request(
                "turn/start",
                serde_json::json!({
                    "threadId": server_thread,
                    "effort": taken.profile.effort,
                    "input": [{ "type": "text", "text": request.input, "text_elements": [] }],
                }),
                timeout,
            )
            .await
            .map_err(Self::into_dispatch)?;
        let turn_id = result["turn"]["id"].as_str().ok_or_else(|| {
            DispatchError::Rejected("turn/start did not return a turn id".to_string())
        })?;
        self.pump(&mut taken.attached.client).await;
        Ok(TurnRef {
            session: request.session.clone(),
            turn_id: turn_id.to_string(),
        })
    }

    fn into_dispatch(error: super::wire::RpcError) -> DispatchError {
        use super::wire::RpcError as Wire;
        match error {
            Wire::OutcomeUnknown(message) | Wire::Closed(message) | Wire::Io(message) => {
                DispatchError::OutcomeUnknown(message)
            }
            Wire::Rejected(message) | Wire::Protocol(message) | Wire::Timeout(message) => {
                DispatchError::Rejected(message)
            }
            Wire::Oversize { len } => DispatchError::Rejected(format!(
                "frame of {len} bytes exceeds the {FRAME_CAP_BYTES}-byte cap; \
                 transport closed, reconcile before retrying"
            )),
        }
    }
}

/// Session plus frozen dispatch inputs carried across a blocking gateway
/// call. The transport inside is owned by the caller for the duration —
/// never shared, never held across a lock.
struct TakenSession {
    attempt_id: AttemptId,
    local: SessionRef,
    profile: ResolvedProfile,
    workspace: String,
    resume_from: Option<SessionRef>,
    attached: AttachedTransport,
}

/// Human-readable usage note for a `thread/tokenUsage/updated` payload.
/// Evidence only: the ledger never reads this string.
fn usage_note(params: &serde_json::Value) -> String {
    fn num(params: &serde_json::Value, path: &[&str]) -> String {
        let mut current = params;
        for key in path {
            match current.get(*key) {
                Some(next) => current = next,
                None => return "unknown".to_string(),
            }
        }
        current
            .as_u64()
            .map(|n| n.to_string())
            .unwrap_or_else(|| "unknown".to_string())
    }
    format!(
        "usage total={} in={} cached_in={} out={} reasoning={} context={} (observed on the wire, not ledgered)",
        num(params, &["tokenUsage", "total", "totalTokens"]),
        num(params, &["tokenUsage", "total", "inputTokens"]),
        num(params, &["tokenUsage", "total", "cachedInputTokens"]),
        num(params, &["tokenUsage", "total", "outputTokens"]),
        num(params, &["tokenUsage", "total", "reasoningOutputTokens"]),
        num(params, &["tokenUsage", "modelContextWindow"]),
    )
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
        let spec = ProcessSpec {
            program: self.config.binary.clone(),
            args: self.config.app_server_args.clone(),
            working_dir: None,
        };
        // Prefer owned pipes: the session is correlation-bound to the
        // process from birth either way, but only the piped attach can
        // later speak RPC. Without a piped spawner (unit tests, legacy
        // callers) supervision alone still works and `send` refuses loudly.
        let (identity, io) = if let Some(piped) = &self.piped {
            let (identity, stdio) =
                piped
                    .spawn_piped(&spec)
                    .await
                    .map_err(|error| match error {
                        AppError::Transport(message) => AppError::Transport(format!(
                            "cannot spawn piped app-server '{}': {message}",
                            self.config.binary
                        )),
                        other => other,
                    })?;
            let transport = AttachedTransport {
                client: RpcClient::new(ChildStdio {
                    stdin: stdio.stdin,
                    stdout: stdio.stdout,
                }),
                initialized: false,
                server_thread: None,
            };
            (identity, Some(transport))
        } else {
            let identity = self
                .supervisor
                .spawn(&spec)
                .await
                .map_err(|error| match error {
                    AppError::Transport(message) => AppError::Transport(format!(
                        "cannot spawn app-server '{}': {message}",
                        self.config.binary
                    )),
                    other => other,
                })?;
            (identity, None)
        };
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
                profile: request.profile,
                workspace: request.workspace,
                resume_from: request.resume_from,
                io: Mutex::new(io),
            },
        );
        Ok(session)
    }

    async fn send(&self, request: SendTurn) -> Result<TurnRef, DispatchError> {
        if let Err(error) = self.ensure_compatible() {
            return Err(DispatchError::Rejected(error.to_string()));
        }
        let mut taken = self.take_transport(&request.session, request.attempt_id)?;
        let timeout = Duration::from_millis(self.config.request_timeout_ms.max(1));
        let outcome = self.send_via(&mut taken, &request, timeout).await;
        // The transport goes back unless cancel won the race and removed
        // the session underneath us — then it is dropped with the dead
        // process instead of being resurrected.
        self.restore_transport(taken);
        outcome
    }

    async fn observe(&self, session: &SessionRef) -> Result<Vec<Observation>, AppError> {
        self.ensure_compatible()?;
        // Drive the wire first so arrivals never sit in the OS pipe while
        // Central reports nothing. Best-effort: no transport, no pump.
        self.pump_for(session, None).await;
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
        // drops and never fabricated Observations. Wire params carry the
        // SERVER thread id once attached, so match that when bound —
        // falling back to the local correlation id for sessions that
        // never attached (tests, legacy).
        let found = state
            .sessions
            .iter()
            .find(|(_, managed)| {
                managed.session.thread_id == session.thread_id
                    && managed.session.instance_epoch == session.instance_epoch
            })
            .map(|(attempt_id, managed)| {
                let want = managed
                    .io
                    .lock()
                    .expect("session io")
                    .as_ref()
                    .and_then(|attached| attached.server_thread.clone())
                    .unwrap_or_else(|| session.thread_id.clone());
                (*attempt_id, want)
            });
        let Some((attempt_id, want)) = found else {
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
            if thread == want {
                let mut observation = observation_for(&epoch, attempt_id, &event);
                // Usage arrives on its own notification, never inside
                // turn/completed: keep the counts as a human-readable
                // evidence note. This is not ledgering — the shared
                // budget only moves through the audited user resolve
                // path, and unknown stays unknown until then.
                if event.method == "thread/tokenUsage/updated" {
                    observation.detail = usage_note(&event.params);
                }
                matched.push(observation);
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
        // already-answered keys instead of resending. The table is consumed
        // first: a wire failure afterwards surfaces as OutcomeUnknown and
        // the table blocks any re-answer, forcing the next server request
        // cycle instead of a blind resend.
        let route = {
            let mut state = self.state.lock().expect("gateway state");
            state
                .requests
                .consume_answer(&answer.key)
                .map_err(|error: AppError| DispatchError::Rejected(error.to_string()))?;
            let canonical = canonical_key(
                &answer.key.session.provider,
                &answer.key.session.instance_epoch,
                &answer.key.session.thread_id,
                &answer.key.turn_id,
                &answer.key.item_id,
                &answer.key.request_id,
            );
            state.wire_routes.remove(&canonical)
        };
        let Some(route) = route else {
            // No live wire route (legacy/test traffic, or the connection
            // dropped after the request arrived): the Once scope above is
            // the whole guarantee, nothing to write.
            return Ok(());
        };
        // Shape the wire answer strictly from the domain verdict: only the
        // exact approved/declined forms map to a decision, and questions
        // need their server-side ids. Anything else refuses instead of
        // guessing — a guessed accept/cancel would be a fabricated approval.
        let result = match route.kind {
            InboxKind::Approval => match &answer.payload {
                AnswerPayload::Plain(text) if text == "approved" => {
                    serde_json::json!({ "decision": "accept" })
                }
                AnswerPayload::Plain(text) if text.starts_with("declined:") => {
                    serde_json::json!({ "decision": "cancel" })
                }
                _ => {
                    return Err(DispatchError::Rejected(
                        "approval payload must be the domain approved/declined verdict; \
                         refusing to guess a wire decision"
                            .to_string(),
                    ));
                }
            },
            InboxKind::Question => match &answer.payload {
                AnswerPayload::Plain(text) => {
                    let questions = route.params.get("questions").and_then(|q| q.as_array());
                    let mut answers = serde_json::Map::new();
                    for question in questions.into_iter().flatten() {
                        if let Some(id) = question.get("id").and_then(|v| v.as_str()) {
                            answers
                                .insert(id.to_string(), serde_json::json!({ "answers": [text] }));
                        }
                    }
                    if answers.is_empty() {
                        return Err(DispatchError::Rejected(
                            "server question carries no question ids; cannot shape the answer"
                                .to_string(),
                        ));
                    }
                    serde_json::json!({ "answers": answers })
                }
                AnswerPayload::Secret(_) => {
                    return Err(DispatchError::Rejected(
                        "secret answers must be resolved to plain text by the caller".to_string(),
                    ));
                }
            },
        };
        // Write through the owning session's transport: the key carries the
        // server thread id, so find the session bound to it.
        let mut taken = {
            let state = self.state.lock().expect("gateway state");
            let found = state
                .sessions
                .iter()
                .find(|(_, managed)| {
                    managed
                        .io
                        .lock()
                        .expect("session io")
                        .as_ref()
                        .and_then(|attached| attached.server_thread.clone())
                        .is_some_and(|server| server == answer.key.session.thread_id)
                })
                .map(|(attempt_id, managed)| (*attempt_id, managed.session.clone()));
            match found {
                Some((attempt_id, local)) => {
                    let attached = state
                        .sessions
                        .get(&attempt_id)
                        .and_then(|managed| managed.io.lock().expect("session io").take())
                        .ok_or_else(|| {
                            DispatchError::Rejected(
                                "the owning session's transport is busy or gone; \
                                 the Once scope already consumed this answer, so a fresh \
                                 server request is required instead of a resend"
                                    .to_string(),
                            )
                        })?;
                    let managed = state.sessions.get(&attempt_id).expect("session live");
                    TakenSession {
                        attempt_id,
                        local,
                        profile: managed.profile.clone(),
                        workspace: managed.workspace.clone(),
                        resume_from: managed.resume_from.clone(),
                        attached,
                    }
                }
                None => {
                    return Err(DispatchError::Rejected(
                        "no live session owns this request's thread; the Once scope \
                         already consumed the answer"
                            .to_string(),
                    ));
                }
            }
        };
        let outcome = taken.attached.client.respond(&route.id, result).await;
        self.restore_transport(taken);
        outcome.map_err(|error| match error {
            super::wire::RpcError::OutcomeUnknown(_)
            | super::wire::RpcError::Closed(_)
            | super::wire::RpcError::Io(_) => {
                // The write may or may not have landed — but the table
                // already consumed this answer, so it can never be resent.
                // Surface OutcomeUnknown so the dispatcher reconciles and
                // waits for the harness to re-ask.
                DispatchError::OutcomeUnknown(
                    "answer write failed after the Once scope consumed it; \
                     reconcile and wait for a fresh server request, never resend"
                        .to_string(),
                )
            }
            other => DispatchError::Rejected(other.to_string()),
        })
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
            .map(|(attempt_id, managed)| {
                // Normalized keys carry the server thread id once attached.
                let want = managed
                    .io
                    .lock()
                    .expect("session io")
                    .as_ref()
                    .and_then(|attached| attached.server_thread.clone())
                    .unwrap_or_else(|| session.thread_id.clone());
                (*attempt_id, want)
            });
        let (attempt_id, want) = match attempt_id {
            Some((id, want)) => (Some(id), want),
            // No live session: legacy behavior drains by the presented
            // thread with no owning attempt.
            None => (None, session.thread_id.clone()),
        };
        Ok(state.requests.drain_for_thread(&want, attempt_id))
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
        let (identity, transport_note) = {
            let state = self.state.lock().expect("gateway state");
            match state.sessions.get(&target.attempt_id) {
                Some(managed) => {
                    let note = managed
                        .io
                        .lock()
                        .expect("session io")
                        .as_ref()
                        .map(|attached| {
                            format!(
                                "rpc attached (initialized={}, thread={})",
                                attached.initialized,
                                attached.server_thread.as_deref().unwrap_or("unbound")
                            )
                        })
                        .unwrap_or_else(|| "supervision only, no RPC transport".to_string());
                    (managed.identity.clone(), note)
                }
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
        // Outcome stays honest: without a completed-turn record Central
        // cannot claim the turn finished. The note tells the reconciler
        // what channel exists for a follow-up thread/read.
        Ok(ReconcileEvidence {
            attempt_id: target.attempt_id,
            status: AttemptStatus::UnknownOutcome,
            detail: format!(
                "owned app-server process {} is {state:?} ({transport_note}); outcome \
                 unknown, re-read thread state before reuse",
                identity.pid
            ),
        })
    }
}
