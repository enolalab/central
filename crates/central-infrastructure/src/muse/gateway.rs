//! Versioned Muse MSP session-host gateway: the [`AgentGateway`] implementation.
//!
//! The transport speaks JSON-RPC 2.0 as JSONL over stdio with an 8 MiB frame
//! cap, correlates out-of-order responses by id, and drains a bounded
//! notification buffer: overflow or a lost connection surfaces
//! reconcile-required evidence instead of dropping events silently.
//!
//! MSP differs from the Codex app-server in one load-bearing way: server
//! approvals are two-step. A server `approval/request` or `userInput/request`
//! is answered first with an empty `RequestReceipt` (presentation only,
//! SS5.3.3); the decision travels as a separate `approval/decide` /
//! `userInput/answer` command. The Once scope is consumed before any wire
//! write, so a failed write surfaces as OutcomeUnknown and waits for the
//! server to re-ask — never a resend.
//!
//! Schema provenance: offline export (`muse schema generate-json-schema`)
//! from muse 1.4.20; see `contracts/muse/1.4.20/`. The serve handshake is
//! NOT live-exercised yet: shapes the server does not confirm are refused
//! loudly instead of guessed.

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

use crate::codex::stdio::{BufferedEvent, EventBuffer, IncomingEvent, RpcClient};
use crate::codex::wire::{parse_frame, sanitize_text, IncomingFrame, RpcId, FRAME_CAP_BYTES};
use crate::process::PipedSpawner;

use super::requests::{normalize_request, observation_for};
use super::version::{check_model_match, check_supported_version, parse_cli_version, VersionError};

/// Static configuration for one gateway instance.
#[derive(Clone, Debug)]
pub struct MuseConfig {
    /// Program used to spawn the session host (`muse serve`).
    pub binary: String,
    /// Pinned CLI build this gateway speaks, e.g. `1.4.20`.
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
    /// Arguments for spawning the session host (default `serve`).
    pub serve_args: Vec<String>,
    /// Models proven servable by a real harness or config smoke. Only these
    /// are advertised as `model:<id>` features. Empty — the default while no
    /// smoke has proven anything — advertises no model at all.
    pub surveyed_models: Vec<String>,
}

impl Default for MuseConfig {
    fn default() -> Self {
        Self {
            binary: "muse".to_string(),
            cli_version: super::version::SUPPORTED_CLI_VERSION.to_string(),
            allowed_versions: Vec::new(),
            frame_cap_bytes: FRAME_CAP_BYTES,
            notification_capacity: 256,
            request_timeout_ms: 30_000,
            allow_provider_model_fallback: false,
            serve_args: vec!["serve".to_string()],
            surveyed_models: Vec::new(),
        }
    }
}

/// Owned stdio halves of one supervised session-host child, joined into a
/// single bidirectional stream for [`RpcClient`].
struct MuseStdio {
    stdin: Box<dyn AsyncWrite + Send + Unpin>,
    stdout: Box<dyn AsyncRead + Send + Unpin>,
}

impl AsyncRead for MuseStdio {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stdout).poll_read(cx, buf)
    }
}

impl AsyncWrite for MuseStdio {
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

/// Live RPC attachment of one session: the client plus handshake/session
/// state. Taken out of the session mutex for every blocking call so a
/// long turn wait never holds a lock across an await.
struct AttachedTransport {
    client: RpcClient<MuseStdio>,
    initialized: bool,
    server_session: Option<String>,
    last_turn: Option<String>,
    /// False only when the server explicitly reported an unavailable
    /// sandbox at initialize. A missing field is unknown, never a refusal.
    sandbox_ok: bool,
}

/// Wire route back to one server request: the JSON-RPC id to receipt plus
/// the original params (approval/question ids live there). Memory-only,
/// never persisted; dropped with the session.
struct WireRoute {
    id: RpcId,
    kind: InboxKind,
    params: serde_json::Value,
}

/// One live session: the correlation reference plus the owned process that
/// backs it. The process is supervised (identity-checked), so cancelling
/// the session provably ends the process tree.
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
    requests: crate::codex::PendingRequestTable,
    /// Wire routes for drained requests, keyed by canonical request key.
    wire_routes: HashMap<String, WireRoute>,
}

/// Muse MSP session-host adapter. One instance per supervised process; the
/// constructor mints a fresh `instance_epoch` so RPC ids and pending answers
/// never cross a process boundary (no accept replay after restart).
pub struct MuseGateway {
    supervisor: Arc<dyn ProcessSupervisor>,
    piped: Option<Arc<dyn PipedSpawner>>,
    config: MuseConfig,
    epoch: String,
    state: Mutex<GatewayState>,
}

impl MuseGateway {
    pub fn new(supervisor: Arc<dyn ProcessSupervisor>, config: MuseConfig) -> Self {
        Self {
            supervisor,
            piped: None,
            epoch: uuid::Uuid::new_v4().to_string(),
            state: Mutex::new(GatewayState {
                buffer: EventBuffer::new(config.notification_capacity),
                closed: false,
                closed_reason: None,
                sessions: HashMap::new(),
                requests: crate::codex::PendingRequestTable::new(),
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

    /// The process supervisor this gateway will spawn through.
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

    /// Feed one raw wire line into the buffer. Infallible bookkeeping:
    /// malformed or oversize input closes the gateway and latches
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
                state.buffer.push(BufferedEvent {
                    id: None,
                    method,
                    params,
                    is_request: false,
                });
            }
            Ok(IncomingFrame::ServerRequest { id, method, params }) => {
                let event = BufferedEvent {
                    id: Some(id),
                    method,
                    params,
                    is_request: true,
                };
                if let Some(normalized) = normalize_request(&self.epoch, &event) {
                    state.requests.record(normalized);
                }
                state.buffer.push(event);
            }
            Ok(IncomingFrame::Response { .. }) => {
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
                "unsupported muse CLI version '{}'; gateway allowlist is [{}]",
                version.as_str(),
                self.config.allowed_versions.join(", "),
            )));
        }
        if self.config.allow_provider_model_fallback {
            return Err(AppError::Capability(
                "central refuses provider model fallback: the resolved model must equal \
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
            sandbox_refused: false,
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
    }

    /// Move newly arrived frames into the bounded buffer plus the request
    /// table. Stops at the first quiet window; a closed transport latches
    /// reconcile-required instead of losing events silently.
    async fn pump(&self, client: &mut RpcClient<MuseStdio>) {
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
                Err(crate::codex::RpcError::Timeout(_)) => break,
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
                sandbox_refused: false,
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
    /// refuses without starting any session or turn. An explicitly
    /// unavailable sandbox latches `sandbox_ok = false`; a missing field is
    /// unknown and never a refusal.
    async fn ensure_initialized(
        &self,
        taken: &mut TakenSession,
        timeout: Duration,
    ) -> Result<(), DispatchError> {
        if taken.attached.initialized {
            return Ok(());
        }
        let result = taken
            .attached
            .client
            .request(
                "initialize",
                serde_json::json!({
                    "clientInfo": {
                        "name": "central",
                        "title": "Central",
                        "version": env!("CARGO_PKG_VERSION"),
                    },
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
        if let Some(user_agent) = result.get("userAgent").and_then(|v| v.as_str()) {
            if !user_agent.contains(&self.config.cli_version) {
                return Err(DispatchError::Rejected(format!(
                    "server user agent does not contain the allowlisted '{}'; \
                     refusing an unvalidated protocol",
                    self.config.cli_version,
                )));
            }
        }
        if sandbox_explicitly_unavailable(&result) {
            taken.attached.sandbox_ok = false;
        }
        // The session host serves nothing until the client acknowledges
        // the handshake: without this notify every session/start fails
        // with `notInitialized` (verified live on 1.4.23).
        taken
            .attached
            .client
            .notify("initialized", serde_json::json!({}))
            .await
            .map_err(Self::into_dispatch)?;
        taken.attached.initialized = true;
        Ok(())
    }

    /// Bind the server session once per attachment: resume the handover
    /// session when one was supplied, else start a fresh one on the
    /// attempt's frozen profile. The observed model must match what was
    /// requested when the server reports one — mismatch is a capability
    /// error, never a silent fallback.
    async fn ensure_session(
        &self,
        taken: &mut TakenSession,
        timeout: Duration,
    ) -> Result<String, DispatchError> {
        if let Some(session) = taken.attached.server_session.clone() {
            return Ok(session);
        }
        let client = &mut taken.attached.client;
        let server_session = if let Some(resume) = &taken.resume_from {
            if resume.provider != "muse" {
                return Err(DispatchError::Rejected(
                    "handover names a non-muse session; refusing cross-harness resume".to_string(),
                ));
            }
            let result = client
                .request(
                    "session/resume",
                    serde_json::json!({ "sessionId": resume.thread_id }),
                    timeout,
                )
                .await
                .map_err(Self::into_dispatch)?;
            session_id_from(&result).ok_or_else(|| {
                DispatchError::Rejected("session/resume did not return a session id".to_string())
            })?
        } else {
            let result = client
                .request(
                    "session/start",
                    serde_json::json!({
                        "commandId": uuid::Uuid::now_v7().to_string(),
                        "modelId": taken.profile.model,
                        "approvalMode": "onRequest",
                        // Compose the workspace at start: a turn whose
                        // roots were never composed is rejected with
                        // `workspace_not_composed` (verified live on 1.4.23).
                        "workspaceRoot": taken.workspace,
                    }),
                    timeout,
                )
                .await
                .map_err(Self::into_dispatch)?;
            if let Some(observed) = result
                .pointer("/session/model")
                .or_else(|| result.pointer("/session/modelId"))
                .and_then(serde_json::Value::as_str)
            {
                self.verify_model_match(&taken.profile.model, observed)
                    .map_err(|error| DispatchError::Rejected(error.to_string()))?;
            }
            session_id_from(&result).ok_or_else(|| {
                DispatchError::Rejected(
                    "session/start did not return a session id; refusing to run blind".to_string(),
                )
            })?
        };
        taken.attached.server_session = Some(server_session.clone());
        Ok(server_session)
    }

    /// One turn on the bound server session, then pump arrivals into the
    /// buffer. A timeout after the write is OutcomeUnknown: the turn may
    /// still run server-side, so it is never resent.
    async fn send_via(
        &self,
        taken: &mut TakenSession,
        request: &SendTurn,
        timeout: Duration,
    ) -> Result<TurnRef, DispatchError> {
        self.ensure_initialized(taken, timeout).await?;
        if !taken.attached.sandbox_ok {
            taken.sandbox_refused = true;
            return Err(DispatchError::Rejected(
                "session host reported its sandbox unavailable; check sandbox configuration and retry"
                    .to_string(),
            ));
        }
        let server_session = self.ensure_session(taken, timeout).await?;
        let turn_command = uuid::Uuid::now_v7().to_string();
        let display = truncate_display(&request.input);
        let result = taken
            .attached
            .client
            .request(
                "turn/start",
                serde_json::json!({
                    "sessionId": server_session,
                    "commandId": turn_command,
                    "input": [{ "type": "text", "text": request.input }],
                    "ifBusy": "queue",
                    "reasoningEffort": taken.profile.effort,
                    "displayText": display,
                    "workspaceRoots": [taken.workspace],
                }),
                timeout,
            )
            .await
            .map_err(Self::into_dispatch)?;
        let turn_id = result
            .get("turnId")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                DispatchError::Rejected("turn/start did not return a turn id".to_string())
            })?;
        taken.attached.last_turn = Some(turn_id.to_string());
        self.pump(&mut taken.attached.client).await;
        Ok(TurnRef {
            session: request.session.clone(),
            turn_id: turn_id.to_string(),
        })
    }

    fn into_dispatch(error: crate::codex::RpcError) -> DispatchError {
        use crate::codex::RpcError as Wire;
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

/// Session id from a `session/start` / `session/resume` result. The id
/// lives on the session object; accept the documented spellings and refuse
/// when none is present instead of correlating blind.
fn session_id_from(result: &serde_json::Value) -> Option<String> {
    for pointer in ["/session/id", "/session/sessionId", "/sessionId"] {
        if let Some(id) = result.pointer(pointer).and_then(serde_json::Value::as_str) {
            if !id.is_empty() {
                return Some(id.to_string());
            }
        }
    }
    None
}

/// True only when the server explicitly reports its sandbox unavailable.
/// A missing `sandboxAvailability` is unknown, never a refusal.
fn sandbox_explicitly_unavailable(result: &serde_json::Value) -> bool {
    let availability = &result["sandboxAvailability"];
    if availability
        .get("available")
        .and_then(serde_json::Value::as_bool)
        == Some(false)
    {
        return true;
    }
    if let Some(text) = availability.as_str() {
        return matches!(text, "unavailable" | "disabled" | "unsupported");
    }
    false
}

/// Presentation form of the prompt for transcripts. Durable, never
/// model-visible; bounded so a huge prompt does not bloat the view.
fn truncate_display(input: &str) -> String {
    const MAX: usize = 2000;
    if input.chars().count() <= MAX {
        return input.to_string();
    }
    let mut out: String = input.chars().take(MAX).collect();
    out.push('…');
    out
}

/// MSP-local process notifications that never bind to a session.
/// `initialized` is connection-scoped. Anything else without a session
/// stays reconcile-required so a truly unattributed event still 503s.
fn is_benign_process_notification(method: &str) -> bool {
    matches!(method, "initialized")
}

/// Human-readable usage note for a `session/tokenUsage` or
/// `session/contextUsage` payload. Evidence only: the ledger never reads
/// this string.
fn usage_note(method: &str, params: &serde_json::Value) -> String {
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
    if method == "session/contextUsage" {
        return format!(
            "context window={} used={} pressure={} (observed on the wire, not ledgered)",
            num(params, &["windowTokens"]),
            num(params, &["usedTokens"]),
            params
                .get("pressure")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown"),
        );
    }
    // Live shape (1.4.23): `usage` carries the raw counters, `cumulative`
    // and `totalTokens` sit beside it — not nested under `usage`.
    let cumulative = num(params, &["cumulative", "totalTokens"]);
    let cumulative = if cumulative == "unknown" {
        num(params, &["totalTokens"])
    } else {
        cumulative
    };
    format!(
        "usage in={} out={} cached={} cumulative_total={} (observed on the wire, not ledgered)",
        num(params, &["usage", "inputTokens"]),
        num(params, &["usage", "outputTokens"]),
        num(params, &["usage", "cachedTokens"]),
        cumulative,
    )
}

/// Human-readable terminal note for a `turn/completed` payload.
fn turn_note(params: &serde_json::Value) -> String {
    let terminal = params
        .get("terminal")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("completed");
    match params.get("error") {
        Some(error) if !error.is_null() => format!("turn {terminal}: {error}"),
        _ => format!("turn {terminal}"),
    }
}

fn into_capability(error: VersionError) -> AppError {
    AppError::Capability(error.to_string())
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
    sandbox_refused: bool,
    attached: AttachedTransport,
}

/// Extract the MSP session a buffered event belongs to: `sessionId` first,
/// then the Codex spellings. Empty means unattributed.
fn event_session(params: &serde_json::Value) -> &str {
    params
        .get("sessionId")
        .or_else(|| params.get("threadId"))
        .or_else(|| params.get("thread_id"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
}

fn event_turn(params: &serde_json::Value) -> &str {
    params
        .get("turnId")
        .or_else(|| params.get("turn_id"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
}

#[async_trait]
impl AgentGateway for MuseGateway {
    async fn capabilities(&self, _profile: &ResolvedProfile) -> Result<Capabilities, AppError> {
        self.ensure_compatible()?;
        // Never self-attest the requested model: only models surveyed from a
        // real harness/config smoke are advertised as `model:<id>` features.
        // An unsurveyed model stays unadvertised so `resolve_profile` treats
        // it as unavailable and raises `ModelUnavailable` — never a silent
        // fallback.
        let mut features = vec![
            "initialize".to_string(),
            "session/start".to_string(),
            "session/resume".to_string(),
            "turn/start".to_string(),
            "turn/cancel".to_string(),
            "approval/request".to_string(),
            "userInput/request".to_string(),
            "approval/decide".to_string(),
            "userInput/answer".to_string(),
            "approval/listPending".to_string(),
        ];
        for model in &self.config.surveyed_models {
            features.push(format!("model:{model}"));
        }
        Ok(Capabilities {
            provider: "muse".to_string(),
            supports_resume: true,
            features,
        })
    }

    async fn open(&self, request: OpenSession) -> Result<SessionRef, AppError> {
        self.ensure_compatible()?;
        let spec = ProcessSpec {
            program: self.config.binary.clone(),
            args: self.config.serve_args.clone(),
            working_dir: None,
            env: Vec::new(),
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
                            "cannot spawn piped muse serve '{}': {message}",
                            self.config.binary
                        )),
                        other => other,
                    })?;
            let transport = AttachedTransport {
                client: RpcClient::with_notification_capacity(
                    MuseStdio {
                        stdin: stdio.stdin,
                        stdout: stdio.stdout,
                    },
                    self.config.notification_capacity,
                ),
                initialized: false,
                server_session: None,
                last_turn: None,
                sandbox_ok: true,
            };
            (identity, Some(transport))
        } else {
            let identity = self
                .supervisor
                .spawn(&spec)
                .await
                .map_err(|error| match error {
                    AppError::Transport(message) => AppError::Transport(format!(
                        "cannot spawn muse serve '{}': {message}",
                        self.config.binary
                    )),
                    other => other,
                })?;
            (identity, None)
        };
        let session = SessionRef {
            provider: "muse".to_string(),
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
        let sandbox_refused = taken.sandbox_refused;
        // The transport goes back unless cancel won the race and removed
        // the session underneath us — then it is dropped with the dead
        // process instead of being resurrected.
        self.restore_transport(taken);
        if sandbox_refused {
            if let Err(DispatchError::Rejected(message)) = &outcome {
                // This message is a fixed string from the sandbox check;
                // never an app-server payload or credential.
                eprintln!("central: attempt {}: {message}", request.attempt_id);
            }
            let target = ExecutionRef {
                attempt_id: request.attempt_id,
                session: request.session,
                turn: None,
            };
            if !matches!(
                self.cancel(&target).await,
                Ok(CancelObservation {
                    cancelled: true,
                    ..
                })
            ) {
                return Err(DispatchError::OutcomeUnknown(
                    "sandbox reported unavailable before turn/start, but termination of the owned muse serve process was not confirmed; reconcile process ownership".to_string(),
                ));
            }
        }
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
        // Bind buffered events to the attempt that owns this session. The
        // wire params carry the SERVER session id once bound — falling back
        // to the local correlation id for sessions that never attached
        // (tests, legacy) and to the last turn id for turn-scoped events
        // that carry no session.
        let found = state
            .sessions
            .iter()
            .find(|(_, managed)| {
                managed.session.thread_id == session.thread_id
                    && managed.session.instance_epoch == session.instance_epoch
            })
            .map(|(attempt_id, managed)| {
                let attached = managed.io.lock().expect("session io");
                let (server, last_turn) = attached
                    .as_ref()
                    .map(|a| (a.server_session.clone(), a.last_turn.clone()))
                    .unwrap_or((None, None));
                let want = server.unwrap_or_else(|| session.thread_id.clone());
                (*attempt_id, want, last_turn)
            });
        let Some((attempt_id, want, last_turn)) = found else {
            // Drop documented connection-scoped noise before counting: an
            // `initialized` handshake must not 503 a poll for a gone
            // session. Anything else unattributed stays reconcile-required.
            let mut rest = Vec::new();
            for event in state.buffer.drain() {
                if event_session(&event.params).is_empty()
                    && !event.is_request
                    && is_benign_process_notification(&event.method)
                {
                    continue;
                }
                rest.push(event);
            }
            if rest.is_empty() {
                return Ok(Vec::new());
            }
            return Err(unattributed(&mut state, rest));
        };
        let epoch = self.epoch.clone();
        let mut matched = Vec::new();
        let mut rest = Vec::new();
        for event in state.buffer.drain() {
            let owner = event_session(&event.params);
            let ours = if owner == want {
                true
            } else if owner.is_empty() {
                // Turn-scoped events (e.g. a `turn/completed` without a
                // session member) bind through the last turn we started.
                last_turn
                    .as_deref()
                    .is_some_and(|last| event_turn(&event.params) == last)
            } else {
                false
            };
            if ours {
                let mut observation = observation_for(&epoch, attempt_id, &event);
                // Usage arrives on its own notifications, never inside
                // turn/completed: keep the counts as a human-readable
                // evidence note. This is not ledgering — the shared
                // budget only moves through the audited user resolve
                // path, and unknown stays unknown until then.
                if event.method == "session/tokenUsage" || event.method == "session/contextUsage" {
                    observation.detail = usage_note(&event.method, &event.params);
                } else if event.method == "turn/completed" {
                    observation.detail = turn_note(&event.params);
                }
                matched.push(observation);
            } else if owner.is_empty()
                && !event.is_request
                && is_benign_process_notification(&event.method)
            {
                // Documented connection-scoped notifications never bind to
                // one attempt, so drop them here instead of failing the
                // whole poll with 503. Any other unattributed event goes to
                // `rest` and stays reconcile-required.
                continue;
            } else {
                rest.push(event);
            }
        }
        if matched.is_empty() {
            if rest.is_empty() {
                return Ok(Vec::new());
            }
            return Err(unattributed(&mut state, rest));
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
        // process epoch is stale and must never replay as a decision.
        if answer.key.session.instance_epoch != self.epoch {
            return Err(DispatchError::Rejected(format!(
                "stale external answer: epoch '{}' does not match current '{}'",
                answer.key.session.instance_epoch, self.epoch
            )));
        }
        if answer.key.session.provider != "muse" {
            return Err(DispatchError::Rejected(
                "external answer names a non-muse session".to_string(),
            ));
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
        // guessing — a guessed decision would be a fabricated approval.
        let AnswerPayload::Plain(text) = &answer.payload else {
            return Err(DispatchError::Rejected(
                "secret answers must be resolved to plain text by the caller".to_string(),
            ));
        };
        let command_id = uuid::Uuid::now_v7().to_string();
        let (command, receipt_first) = match route.kind {
            InboxKind::Approval => {
                // The requirement guard travels verbatim: live servers send
                // an OBJECT ({approvalId, sourceIndex}), not a string.
                // Absent means the decide would race — refuse instead.
                let requirement = route
                    .params
                    .get("currentRequirementId")
                    .or_else(|| route.params.get("requirementId"))
                    .cloned()
                    .filter(|value| !value.is_null());
                let Some(requirement) = requirement else {
                    return Err(DispatchError::Rejected(
                        "server approval carries no currentRequirementId; refusing the race-prone decide"
                            .to_string(),
                    ));
                };
                let item = item_id_from(&answer.key);
                let (choice, feedback) = decide_choice(&route.params, text, &item)?;
                let mut params = serde_json::json!({
                    "sessionId": answer.key.session.thread_id,
                    "commandId": command_id,
                    "approvalId": item,
                    "choiceId": choice,
                    "requirementId": requirement,
                });
                if let Some(feedback) = feedback {
                    params["feedback"] = serde_json::Value::String(feedback);
                }
                (
                    serde_json::json!({ "method": "approval/decide", "params": params }),
                    true,
                )
            }
            InboxKind::Question => {
                let answers = question_answers(&route.params, text)?;
                let params = serde_json::json!({
                    "sessionId": answer.key.session.thread_id,
                    "commandId": command_id,
                    "userInputId": item_id_from(&answer.key),
                    "answers": answers,
                });
                (
                    serde_json::json!({ "method": "userInput/answer", "params": params }),
                    true,
                )
            }
        };
        // Write through the owning session's transport: the key carries the
        // server session id, so find the session bound to it.
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
                        .and_then(|attached| attached.server_session.clone())
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
                        sandbox_refused: false,
                        attached,
                    }
                }
                None => {
                    return Err(DispatchError::Rejected(
                        "no live session owns this request's session; the Once scope \
                         already consumed the answer"
                            .to_string(),
                    ));
                }
            }
        };
        // MSP two-step: the Receipt acknowledges presentation (SS5.3.3) and
        // the decision travels as its own command. Either write failing
        // after the Once scope consumed the answer is OutcomeUnknown: wait
        // for the server to re-ask, never resend.
        let method = command["method"].as_str().unwrap_or("").to_string();
        let params = command["params"].clone();
        let outcome = async {
            taken
                .attached
                .client
                .respond(&route.id, serde_json::json!({}))
                .await
                .map_err(Self::into_dispatch)?;
            if receipt_first {
                taken
                    .attached
                    .client
                    .request(&method, params, Duration::from_millis(30_000))
                    .await
                    .map_err(Self::into_dispatch)?;
            }
            Ok::<(), DispatchError>(())
        }
        .await;
        self.restore_transport(taken);
        outcome.map_err(|error| match error {
            DispatchError::OutcomeUnknown(_)
            | DispatchError::Rejected(_)
            | DispatchError::DefinitelyNotSent(_) => {
                // Writes after the Once scope consumed the answer can never
                // be resent: surface OutcomeUnknown so the dispatcher
                // reconciles and waits for the harness to re-ask. A plain
                // rejection here (e.g. the server refusing the shape) is
                // also terminal for this answer — the table already consumed
                // it, so report it as unknown-outcome rather than a
                // retryable refusal.
                DispatchError::OutcomeUnknown(match error {
                    DispatchError::Rejected(message)
                    | DispatchError::OutcomeUnknown(message)
                    | DispatchError::DefinitelyNotSent(message) => message,
                })
            }
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
                // Normalized keys carry the server session id once attached.
                let want = managed
                    .io
                    .lock()
                    .expect("session io")
                    .as_ref()
                    .and_then(|attached| attached.server_session.clone())
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
        let managed = {
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
                    managed
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
        if let Err(error) = self.supervisor.terminate(&managed.identity).await {
            // A failed termination is not evidence that the owned process
            // exited. Keep its identity available for reconciliation.
            self.state
                .lock()
                .expect("gateway state")
                .sessions
                .entry(target.attempt_id)
                .or_insert(managed);
            return Err(error);
        }
        Ok(CancelObservation {
            cancelled: true,
            detail: format!(
                "terminated owned muse serve process {} for attempt {}",
                managed.identity.pid, target.attempt_id
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
                                "rpc attached (initialized={}, session={})",
                                attached.initialized,
                                attached.server_session.as_deref().unwrap_or("unbound")
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
                                 session state before reuse"
                            .to_string(),
                    });
                }
            }
        };
        let state = self.supervisor.inspect(&identity).await?;
        // Outcome stays honest: without a completed-turn record Central
        // cannot claim the turn finished. The note tells the reconciler
        // what channel exists for follow-up reads.
        Ok(ReconcileEvidence {
            attempt_id: target.attempt_id,
            status: AttemptStatus::UnknownOutcome,
            detail: format!(
                "owned muse serve process {} is {state:?} ({transport_note}); outcome \
                 unknown, re-read session state before reuse",
                identity.pid
            ),
        })
    }
}

/// Fail-closed evidence for buffered events no live session owns: push them
/// back and report reconcile-required instead of fabricating Observations.
/// Method names are fixed protocol strings, safe to keep in the body.
fn unattributed(state: &mut GatewayState, rest: Vec<BufferedEvent>) -> AppError {
    let mut seen = std::collections::BTreeSet::new();
    for event in &rest {
        seen.insert(event.method.clone());
    }
    let mut method_list: Vec<String> = seen.into_iter().collect();
    method_list.sort();
    method_list.truncate(8);
    let pending = rest.len();
    for event in rest {
        state.buffer.push(event);
        if state.buffer.overflowed() {
            break;
        }
    }
    if state.buffer.overflowed() {
        return AppError::Transport(
            "reconcile-required: notification buffer overflowed; events may have been \
             dropped, full reconcile instead of silent resumption"
                .to_string(),
        );
    }
    AppError::Transport(format!(
        "reconcile-required: {pending} unattributed notification(s) [{}] for an unknown \
         session; Central never fabricates Observations",
        method_list.join(", ")
    ))
}

/// The item (approval or prompt) an answer addresses: the wire id from the
/// request params, falling back to the normalized item.
fn item_id_from(key: &central_application::contracts::ExternalRequestKey) -> String {
    if key.item_id.is_empty() {
        key.request_id.clone()
    } else {
        key.item_id.clone()
    }
}

/// Pick the wire choice for an approval verdict. `approved` selects the
/// one-shot accept choice, `declined:<feedback>` the abort choice with the
/// feedback attached. Anything else refuses instead of guessing — and an
/// ambiguous choice list refuses rather than picking a winner blind.
///
/// Choice matching reads the server's `decision` member first (live:
/// `approved` / `approvedPolicyAmendment` / `abort`), falling back to
/// label text. `always`-style amendments (`approvedPolicyAmendment`,
/// persistent scopes) are never selected: standing permissions stay an
/// explicit owner act, never an agent side effect.
fn decide_choice(
    params: &serde_json::Value,
    text: &str,
    item: &str,
) -> Result<(String, Option<String>), DispatchError> {
    let choices = available_choices(params);
    let reject = |why: &str| {
        DispatchError::Rejected(format!(
            "cannot shape approval decision for '{item}': {why}; \
             refusing to guess a wire decision"
        ))
    };
    if choices.is_empty() {
        return Err(reject("the server listed no available choices"));
    }
    let single = || {
        if choices.len() == 1 {
            Some(choices[0].id.clone())
        } else {
            None
        }
    };
    if text == "approved" {
        // One-shot accept only: never a persistent-scope amendment.
        let accept = choices
            .iter()
            .find(|choice| choice.decision == "approved" && choice.scope_one_shot())
            .map(|choice| choice.id.clone())
            .or_else(|| {
                choices
                    .iter()
                    .find(|choice| {
                        let hay = format!("{} {}", choice.id, choice.label).to_lowercase();
                        (hay.contains("accept") || hay.contains("approv") || hay.contains("allow"))
                            && !hay.contains("always")
                    })
                    .map(|choice| choice.id.clone())
            });
        match accept.or_else(single) {
            Some(choice) => Ok((choice, None)),
            None => Err(reject("no unambiguous one-shot accept choice")),
        }
    } else if let Some(feedback) = text.strip_prefix("declined:") {
        let abort = choices
            .iter()
            .find(|choice| choice.decision == "abort")
            .map(|choice| choice.id.clone())
            .or_else(|| {
                choices
                    .iter()
                    .find(|choice| {
                        let hay = format!("{} {}", choice.id, choice.label).to_lowercase();
                        hay.contains("cancel")
                            || hay.contains("declin")
                            || hay.contains("deny")
                            || hay.contains("reject")
                            || hay.contains("abort")
                    })
                    .map(|choice| choice.id.clone())
            });
        match abort.or_else(single) {
            Some(choice) => {
                let feedback = feedback.trim();
                Ok((
                    choice,
                    if feedback.is_empty() {
                        None
                    } else {
                        Some(feedback.to_string())
                    },
                ))
            }
            None => Err(reject("no unambiguous abort choice")),
        }
    } else {
        Err(reject(
            "approval payload must be the domain approved/declined verdict",
        ))
    }
}

/// One server-provided decision choice. Live shape (1.4.23):
/// `{choiceId, decision, label, scope}`, e.g. `allow_once` /
/// `approved`, `allow_local_prefix` / `approvedPolicyAmendment`,
/// `abort` / `abort`.
#[derive(Clone, Debug, PartialEq, Eq)]
struct AvailableChoice {
    id: String,
    label: String,
    decision: String,
    scope: String,
}

impl AvailableChoice {
    /// One-shot decisions only: persistent scopes (`localPersistent`,
    /// `globalPersistent`) are never auto-selected.
    fn scope_one_shot(&self) -> bool {
        self.scope == "once" || self.scope.is_empty()
    }
}

/// `(id, label, decision, scope)` rows from the server's
/// `availableChoices`: objects with `choiceId`/`id` plus `label`,
/// `decision` and `scope`, or bare strings (unknown decision/scope).
fn available_choices(params: &serde_json::Value) -> Vec<AvailableChoice> {
    let Some(list) = params.get("availableChoices").and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for choice in list {
        if let Some(id) = choice.as_str() {
            out.push(AvailableChoice {
                id: id.to_string(),
                label: id.to_string(),
                decision: String::new(),
                scope: String::new(),
            });
        } else if let Some(object) = choice.as_object() {
            let id = object
                .get("id")
                .or_else(|| object.get("choiceId"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            if id.is_empty() {
                continue;
            }
            let str_field = |names: &[&str]| {
                names
                    .iter()
                    .filter_map(|name| object.get(*name))
                    .filter_map(serde_json::Value::as_str)
                    .next()
                    .unwrap_or("")
                    .to_string()
            };
            out.push(AvailableChoice {
                id: id.to_string(),
                label: str_field(&["label", "title"]),
                decision: str_field(&["decision"]),
                scope: str_field(&["scope"]),
            });
        }
    }
    out
}

/// Shape one `userInput/answer` entry per asked question. Plain text that
/// matches an option label selects it; otherwise (or when the question
/// lists no options) the text travels as free text.
fn question_answers(
    params: &serde_json::Value,
    text: &str,
) -> Result<Vec<serde_json::Value>, DispatchError> {
    let Some(questions) = params.get("questions").and_then(|v| v.as_array()) else {
        return Err(DispatchError::Rejected(
            "server question carries no question list; cannot shape the answer".to_string(),
        ));
    };
    let mut answers = Vec::new();
    for question in questions {
        let Some(id) = question.get("id").and_then(|v| v.as_str()) else {
            continue;
        };
        let options: Vec<String> = question
            .get("options")
            .and_then(|v| v.as_array())
            .map(|list| {
                list.iter()
                    .filter_map(|o| {
                        o.as_str()
                            .map(str::to_string)
                            .or_else(|| o.get("label").and_then(|l| l.as_str()).map(str::to_string))
                    })
                    .collect()
            })
            .unwrap_or_default();
        if options.iter().any(|label| label == text) {
            answers.push(serde_json::json!({ "questionId": id, "selectedLabel": text }));
        } else if options.is_empty() {
            answers.push(serde_json::json!({ "questionId": id, "freeText": text }));
        } else {
            // The user answered outside the listed options: free text keeps
            // the verbatim reply instead of forcing a wrong label. The
            // server validates (-32057 on mismatch) and the error surfaces.
            answers.push(serde_json::json!({ "questionId": id, "freeText": text }));
        }
    }
    if answers.is_empty() {
        return Err(DispatchError::Rejected(
            "server question carries no question ids; cannot shape the answer".to_string(),
        ));
    }
    Ok(answers)
}

#[cfg(test)]
mod choice_tests {
    use super::{available_choices, decide_choice};

    /// Live-captured 1.4.23 shell approval: the receipt+decide flow the lab
    /// exercised end to end (RequestReceipt, then decide with the object
    /// requirement guard).
    fn live_shell_approval() -> serde_json::Value {
        serde_json::json!({
            "approvalId": "01a1e8c6-fecc-7412-8cd3-3b981ffa0542",
            "availableChoices": [
                {"choiceId": "allow_once", "decision": "approved", "label": "Allow once", "scope": "once"},
                {"choiceId": "allow_local_prefix", "decision": "approvedPolicyAmendment", "label": "Always allow in this workspace: echo ...", "scope": "localPersistent"},
                {"acceptsFeedback": true, "choiceId": "abort", "decision": "abort", "label": "Reject", "scope": "once"}
            ],
            "currentRequirementId": {"approvalId": "01a1e8c6-fecc-7412-8cd3-3b981ffa0542", "sourceIndex": 0},
            "itemId": "01a1e8c6-fecc-7412-8cd3-3b981ffa0542",
            "sessionId": "ses-live"
        })
    }

    #[test]
    fn approved_selects_the_one_shot_accept_never_the_amendment() {
        let (choice, feedback) = decide_choice(&live_shell_approval(), "approved", "item").unwrap();
        assert_eq!(choice, "allow_once");
        assert_eq!(feedback, None);
    }

    #[test]
    fn declined_selects_abort_with_feedback() {
        let (choice, feedback) =
            decide_choice(&live_shell_approval(), "declined:lab decline", "item").unwrap();
        assert_eq!(choice, "abort");
        assert_eq!(feedback, Some("lab decline".to_string()));
    }

    #[test]
    fn non_verdict_payloads_refuse() {
        assert!(decide_choice(&live_shell_approval(), "maybe", "item").is_err());
        assert!(available_choices(&serde_json::json!({})).is_empty());
    }
}
