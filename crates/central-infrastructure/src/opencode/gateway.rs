//! Versioned opencode REST gateway: the [`AgentGateway`] implementation.
//!
//! Each session owns one `opencode serve` child on loopback (`--port 0`
//! picks a free port, `--stdio` prints the machine-readable `{"url"}` and
//! requires the parent to hold stdin open). Auth is HTTP Basic
//! (`opencode` + a per-session secret passed only through the child
//! environment, never logged or persisted). Central polls the message and
//! permission lists per `observe()` — the SSE stream is intentionally
//! unused — and diffs against seen ids, so a repeated poll never
//! fabricates a second observation.
//!
//! Schema provenance: the live 2.2.7 OpenAPI plus wire probes; see
//! `contracts/opencode/2.2.7/`. Turn flow, auth and the permission LIST are
//! live-verified; the permission reply/reject + interrupt cycle was
//! live-exercised once in the 2026-11-17 lab (recorded in
//! `docs/opencode-gateway-spec.md`, no machine-checkable artifact in repo):
//! shapes the server does not confirm are refused loudly instead of guessed.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use central_application::contracts::{
    AppError, CancelObservation, Capabilities, DispatchError, ExecutionRef, ExternalAnswer,
    ExternalRequest, OpenSession, ProcessIdentity, ProcessSpec, ReconcileEvidence, SendTurn,
    SessionRef, TurnRef,
};
use central_application::ports::{AgentGateway, ProcessSupervisor};
use central_domain::{
    canonical_key, AnswerPayload, AttemptId, AttemptStatus, Observation, ResolvedProfile,
};
use tokio::io::AsyncBufReadExt;

use crate::process::{PipedSpawner, PipedStdio};

use super::client::{ClientError, OpencodeClient};
use super::requests::{normalize_request, observation_for as permission_observation};
use super::version::{
    check_model_ref_match, check_supported_version, parse_cli_version, split_model_ref,
    VersionError,
};

/// Static configuration for one gateway instance.
#[derive(Clone, Debug)]
pub struct OpencodeConfig {
    /// Program used to spawn the per-session server (`opencode serve`).
    pub binary: String,
    /// Pinned CLI build this gateway speaks, e.g. `2.2.7`.
    pub cli_version: String,
    /// Extra per-deployment allowlist. Empty means the global allowlist.
    pub allowed_versions: Vec<String>,
    /// Per-request wait budget in milliseconds.
    pub request_timeout_ms: u64,
    /// Must stay false: Central never enables provider model fallback.
    pub allow_provider_model_fallback: bool,
    /// Extra args appended after the fixed loopback serve flags. Owner
    /// controlled; must never change the bind away from loopback.
    pub extra_args: Vec<String>,
    /// Models proven servable by a real harness or config smoke. Only these
    /// are advertised as `model:<id>` features. Empty — the default while no
    /// smoke has proven anything — advertises no model at all.
    pub surveyed_models: Vec<String>,
}

impl Default for OpencodeConfig {
    fn default() -> Self {
        Self {
            binary: "opencode".to_string(),
            cli_version: super::version::SUPPORTED_CLI_VERSION.to_string(),
            allowed_versions: Vec::new(),
            request_timeout_ms: 30_000,
            allow_provider_model_fallback: false,
            extra_args: Vec::new(),
            surveyed_models: Vec::new(),
        }
    }
}

/// Fixed serve flags. Loopback-only, free port, machine-readable URL on
/// stdout. The parent must hold stdin open or the server exits.
fn serve_args(extra: &[String]) -> Vec<String> {
    let mut args = vec![
        "serve".to_string(),
        "--hostname".to_string(),
        "127.0.0.1".to_string(),
        "--port".to_string(),
        "0".to_string(),
        "--stdio".to_string(),
    ];
    args.extend(extra.iter().cloned());
    args
}

/// Live HTTP attachment of one session: the client plus poll cursors.
/// Taken out of the session mutex for every blocking call so a long poll
/// never holds a lock across an await.
struct AttachedTransport {
    client: OpencodeClient,
    server_session: String,
    /// stdin write-half of the owned server, held open: closing it kills
    /// the server. Dropped (then the process terminated) only on cancel.
    stdin_hold: Option<Box<dyn tokio::io::AsyncWrite + Send + Unpin>>,
    /// Assistant message ids already reported as completed turns.
    seen_completed: HashSet<String>,
    /// Permission ids already reported as observations.
    seen_permissions: HashSet<String>,
}

/// Wire route back to one permission request: the session plus the
/// permission id the REST reply targets. Memory-only, never persisted;
// dropped with the session.
struct WireRoute {
    session_id: String,
    request_id: String,
}

/// One live session: the correlation reference plus the owned server
/// process that backs it. The process is supervised (identity-checked),
/// so cancelling the session provably ends the process tree.
struct ManagedSession {
    session: SessionRef,
    identity: ProcessIdentity,
    profile: ResolvedProfile,
    workspace: String,
    resume_from: Option<SessionRef>,
    io: Mutex<Option<AttachedTransport>>,
}

struct GatewayState {
    closed: bool,
    closed_reason: Option<String>,
    sessions: HashMap<AttemptId, ManagedSession>,
    /// Gateway-side permission requests. The application drains these into
    /// the durable inbox (persist-before-notice); answers route through
    /// this table with a Once scope.
    requests: crate::codex::PendingRequestTable,
    /// Wire routes for drained requests, keyed by canonical request key.
    wire_routes: HashMap<String, WireRoute>,
}

/// opencode REST adapter. One instance per supervised process; the
/// constructor mints a fresh `instance_epoch` so session ids and pending
/// answers never cross a process boundary (no accept replay after
/// restart).
pub struct OpencodeGateway {
    supervisor: Arc<dyn ProcessSupervisor>,
    piped: Option<Arc<dyn PipedSpawner>>,
    config: OpencodeConfig,
    epoch: String,
    state: Mutex<GatewayState>,
}

impl OpencodeGateway {
    pub fn new(supervisor: Arc<dyn ProcessSupervisor>, config: OpencodeConfig) -> Self {
        Self {
            supervisor,
            piped: None,
            epoch: uuid::Uuid::new_v4().to_string(),
            state: Mutex::new(GatewayState {
                closed: false,
                closed_reason: None,
                sessions: HashMap::new(),
                requests: crate::codex::PendingRequestTable::new(),
                wire_routes: HashMap::new(),
            }),
            config,
        }
    }

    /// Attach owned stdio pipes: required to hold the `--stdio` server's
    /// stdin open. Without this, `open` supervises the process but `send`
    /// refuses loudly.
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

    /// True once the transport failed and events may have been lost.
    pub fn reconcile_required(&self) -> bool {
        let state = self.state.lock().expect("gateway state");
        state.closed
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
                "unsupported opencode CLI version '{}'; gateway allowlist is [{}]",
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

    /// Take the HTTP attachment out of a session for one blocking call.
    /// The mutex is only held for the take: no lock is ever held across
    /// an await.
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
                    "no attached HTTP transport for this session (supervision only, \
                     or another call holds it); refusing instead of writing nowhere"
                        .to_string(),
                )
            })?;
        Ok(TakenSession {
            attempt_id: attempt,
            local: managed.session.clone(),
            profile: managed.profile.clone(),
            _workspace: managed.workspace.clone(),
            _resume_from: managed.resume_from.clone(),
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

    fn timeout(&self) -> Duration {
        Duration::from_millis(self.config.request_timeout_ms.max(1))
    }

    /// Spawn one owned server, read its machine-readable URL, and bind a
    /// server session. The `--stdio` server exits when stdin closes, so the
    /// write-half is held in the attachment until cancel.
    async fn attach(
        &self,
        taken_profile: &ResolvedProfile,
        workspace: &str,
        resume_from: &Option<SessionRef>,
        attempt_id: AttemptId,
    ) -> Result<(ProcessIdentity, AttachedTransport), AppError> {
        let piped = self.piped.as_ref().ok_or_else(|| {
            AppError::Transport(
                "no piped spawner for the opencode server; refusing instead of writing nowhere"
                    .to_string(),
            )
        })?;
        // Per-session secret, passed only through the child environment:
        // never logged, never persisted.
        let password = format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let spec = ProcessSpec {
            program: self.config.binary.clone(),
            args: serve_args(&self.config.extra_args),
            working_dir: Some(workspace.to_string()),
            env: vec![("OPENCODE_PASSWORD".to_string(), password.clone())],
        };
        let (identity, stdio) = piped
            .spawn_piped(&spec)
            .await
            .map_err(|error| match error {
                AppError::Transport(message) => AppError::Transport(format!(
                    "cannot spawn piped opencode serve '{}': {message}",
                    self.config.binary
                )),
                other => other,
            })?;
        let server = read_server_url(stdio, &self.timeout()).await?;
        let client = OpencodeClient::new(&server.url, &password, self.timeout())
            .map_err(|error| AppError::Transport(format!("cannot build HTTP client: {error}")))?;
        // Health + version pin: the spawned binary must be the allowlisted
        // one, otherwise refuse before creating anything.
        let info = client.info().await.map_err(|error| {
            AppError::Transport(format!("opencode server info failed: {error}"))
        })?;
        if let Some(version) = info.get("version").and_then(|v| v.as_str()) {
            if version != self.config.cli_version {
                return Err(AppError::Capability(format!(
                    "server version '{version}' does not match the allowlisted '{}'; \
                     refusing an unvalidated protocol",
                    self.config.cli_version,
                )));
            }
        }
        let server_session = match resume_from {
            Some(resume) if resume.provider == "opencode" => resume.thread_id.clone(),
            Some(_) => {
                return Err(AppError::Capability(
                    "handover names a non-opencode session; refusing cross-harness resume"
                        .to_string(),
                ));
            }
            None => {
                let (provider_id, model_id) = split_model_ref(&taken_profile.model);
                let (session, echoed) = client
                    .create_session(&format!("central-{attempt_id}"), provider_id, model_id)
                    .await
                    .map_err(|error| match error {
                        ClientError::Rejected(message) | ClientError::Transport(message) => {
                            AppError::Transport(format!(
                                "opencode session create failed: {message}"
                            ))
                        }
                        ClientError::OutcomeUnknown(message) => AppError::Transport(format!(
                            "opencode session create is outcome-unknown: {message}; \
                             reconcile before reuse"
                        )),
                    })?;
                // Fail fast on a misrouted session: the server echoes its
                // selection, and anything but the requested Model.Ref is a
                // capability error before any prompt is sent.
                match echoed {
                    Some((provider, id)) => {
                        check_model_ref_match(&taken_profile.model, Some(&provider), &id)
                            .map_err(|error| AppError::Capability(error.to_string()))?
                    }
                    None => {
                        return Err(AppError::Capability(
                            "session create did not echo its model selection; \
                             refusing to run blind"
                                .to_string(),
                        ));
                    }
                }
                session
            }
        };
        Ok((
            identity,
            AttachedTransport {
                client,
                server_session,
                stdin_hold: server.stdin_hold,
                seen_completed: HashSet::new(),
                seen_permissions: HashSet::new(),
            },
        ))
    }
}

type PipedStdioStdin = Box<dyn tokio::io::AsyncWrite + Send + Unpin>;

/// The URL line plus the held stdin from a fresh `--stdio` server.
struct ServerAttach {
    url: String,
    stdin_hold: Option<PipedStdioStdin>,
}

/// Read the machine-readable `{"url": ...}` first line, then detach stdout
/// into a drain task so a chatty server log can never park the child on a
/// full pipe. Times out loudly instead of correlating blind.
async fn read_server_url(stdio: PipedStdio, timeout: &Duration) -> Result<ServerAttach, AppError> {
    let PipedStdio { stdin, stdout } = stdio;
    let mut lines = tokio::io::BufReader::new(stdout).lines();
    let first = tokio::time::timeout(*timeout, lines.next_line())
        .await
        .map_err(|_| {
            AppError::Transport(
                "timed out waiting for the opencode server URL; refusing to correlate blind"
                    .to_string(),
            )
        })?
        .map_err(|error| AppError::Transport(format!("cannot read opencode server URL: {error}")))?
        .ok_or_else(|| {
            AppError::Transport("opencode server closed stdout before printing its URL".to_string())
        })?;
    let url = serde_json::from_str::<serde_json::Value>(&first)
        .ok()
        .and_then(|value| {
            value
                .get("url")
                .and_then(|u| u.as_str())
                .map(str::to_string)
        })
        .ok_or_else(|| {
            AppError::Transport(format!(
                "opencode server first line is not a URL object: {first}"
            ))
        })?;
    // Drain the rest of the log in the background; the write-half stays
    // with the attachment so the server never sees EOF.
    tokio::spawn(async move {
        let mut lines = lines;
        while let Ok(Some(_)) = lines.next_line().await {}
    });
    Ok(ServerAttach {
        url,
        stdin_hold: Some(stdin),
    })
}

fn into_capability(error: VersionError) -> AppError {
    AppError::Capability(error.to_string())
}

fn into_dispatch(error: ClientError) -> DispatchError {
    match error {
        ClientError::Rejected(message) => DispatchError::Rejected(message),
        ClientError::OutcomeUnknown(message) => DispatchError::OutcomeUnknown(message),
        // A 5xx may have executed the turn: never claim it did not.
        ClientError::Transport(message) => DispatchError::OutcomeUnknown(message),
    }
}

/// Session plus frozen dispatch inputs carried across a blocking gateway
/// call. The transport inside is owned by the caller for the duration —
/// never shared, never held across a lock.
struct TakenSession {
    attempt_id: AttemptId,
    local: SessionRef,
    profile: ResolvedProfile,
    _workspace: String,
    _resume_from: Option<SessionRef>,
    attached: AttachedTransport,
}

/// Human-readable usage note for a completed assistant message. Evidence
/// only: the ledger never reads this string.
fn usage_note(message: &serde_json::Value) -> String {
    fn num(message: &serde_json::Value, path: &[&str]) -> String {
        let mut current = message;
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
        "usage in={} out={} reasoning={} cached_read={} cached_write={} (observed on the wire, not ledgered)",
        num(message, &["tokens", "input"]),
        num(message, &["tokens", "output"]),
        num(message, &["tokens", "reasoning"]),
        num(message, &["tokens", "cache", "read"]),
        num(message, &["tokens", "cache", "write"]),
    )
}

/// Completed assistant message: `time.completed` set. Streaming messages
/// (null) are never terminal.
fn completed_id(message: &serde_json::Value) -> Option<&str> {
    if message.get("type").and_then(|v| v.as_str()) != Some("assistant") {
        return None;
    }
    let completed = message
        .pointer("/time/completed")
        .and_then(|v| v.as_u64())?;
    if completed == 0 {
        return None;
    }
    message.get("id").and_then(|v| v.as_str())
}

fn message_model(message: &serde_json::Value) -> Option<(Option<String>, String)> {
    let model = message.get("model")?;
    Some((
        model
            .get("providerID")
            .and_then(|v| v.as_str())
            .map(str::to_string),
        model.get("id")?.as_str()?.to_string(),
    ))
}

#[async_trait]
impl AgentGateway for OpencodeGateway {
    async fn capabilities(&self, _profile: &ResolvedProfile) -> Result<Capabilities, AppError> {
        self.ensure_compatible()?;
        // Never self-attest the requested model: only models surveyed from a
        // real harness/config smoke are advertised as `model:<id>` features.
        // An unsurveyed model stays unadvertised so `resolve_profile` treats
        // it as unavailable and raises `ModelUnavailable` — never a silent
        // fallback.
        let mut features = vec![
            "session.create".to_string(),
            "session.prompt".to_string(),
            "session.message.list".to_string(),
            "session.permission.list".to_string(),
            "session.permission.reply".to_string(),
            "session.interrupt".to_string(),
        ];
        for model in &self.config.surveyed_models {
            features.push(format!("model:{model}"));
        }
        Ok(Capabilities {
            provider: "opencode".to_string(),
            supports_resume: true,
            features,
        })
    }

    async fn open(&self, request: OpenSession) -> Result<SessionRef, AppError> {
        self.ensure_compatible()?;
        // Each session owns one server: no cross-session traffic is
        // possible by construction, and cancelling the session provably
        // ends its process tree.
        let attempt = request.attempt_id;
        let profile = request.profile.clone();
        let workspace = request.workspace.clone();
        let resume_from = request.resume_from.clone();
        let (identity, attached) = self
            .attach(&profile, &workspace, &resume_from, attempt)
            .await?;
        let session = SessionRef {
            provider: "opencode".to_string(),
            instance_epoch: self.epoch.clone(),
            thread_id: attached.server_session.clone(),
        };
        self.state.lock().expect("gateway state").sessions.insert(
            attempt,
            ManagedSession {
                session: session.clone(),
                identity,
                profile,
                workspace,
                resume_from,
                io: Mutex::new(Some(attached)),
            },
        );
        Ok(session)
    }

    async fn send(&self, request: SendTurn) -> Result<TurnRef, DispatchError> {
        if let Err(error) = self.ensure_compatible() {
            return Err(DispatchError::Rejected(error.to_string()));
        }
        let taken = self.take_transport(&request.session, request.attempt_id)?;
        let outcome = taken
            .attached
            .client
            .prompt(&taken.attached.server_session, &request.input)
            .await
            .map(|message_id| TurnRef {
                session: request.session.clone(),
                turn_id: message_id,
            })
            .map_err(into_dispatch);
        self.restore_transport(taken);
        outcome
    }

    async fn observe(&self, session: &SessionRef) -> Result<Vec<Observation>, AppError> {
        self.ensure_compatible()?;
        {
            let state = self.state.lock().expect("gateway state");
            if state.closed {
                return Err(AppError::Transport(format!(
                    "reconcile-required: transport closed ({})",
                    state.closed_reason.as_deref().unwrap_or("unknown")
                )));
            }
        }
        let attempt = session_attempt(session, &self.state)
            .map_err(|error| AppError::Transport(error.to_string()))?;
        let mut taken = self
            .take_transport(session, attempt)
            .map_err(|error| match error {
                DispatchError::Rejected(message) => AppError::Transport(message),
                other => AppError::Transport(other.to_string()),
            })?;
        let server = taken.attached.server_session.clone();
        let epoch = self.epoch.clone();
        let attempt_id = taken.attempt_id;
        let profile_model = taken.profile.model.clone();
        let mut observations = Vec::new();
        // One poll round: messages first, then open permission requests.
        // A failed poll latches nothing by itself — the error below is the
        // reconcile-required evidence — but the transport is handed back so
        // the next poll retries.
        let polled: Result<(), AppError> =
            async {
                let messages = taken
                    .attached
                    .client
                    .messages(&server)
                    .await
                    .map_err(|error| match error {
                        ClientError::Rejected(message) => AppError::Capability(message),
                        other => AppError::Transport(format!(
                            "reconcile-required: messages poll failed: {other}"
                        )),
                    })?;
                for message in &messages {
                    let Some(id) = completed_id(message) else {
                        continue;
                    };
                    if !taken.attached.seen_completed.insert(id.to_string()) {
                        continue;
                    }
                    // The server reports the model that actually ran: mismatch
                    // is a capability error, never a silent fallback.
                    if let Some((provider, id)) = message_model(message) {
                        check_model_ref_match(&profile_model, provider.as_deref(), &id)
                            .map_err(|error| AppError::Capability(error.to_string()))?;
                    }
                    // A failed turn still carries `time.completed`: surface the
                    // server error in the evidence so the operator sees why
                    // (e.g. provider quota) instead of an empty success.
                    let server_error = message
                        .get("finish")
                        .and_then(|v| v.as_str())
                        .filter(|finish| *finish == "error")
                        .and_then(|_| message.pointer("/error/message").and_then(|v| v.as_str()))
                        .map(|text| {
                            let clean = crate::codex::wire::sanitize_text(text);
                            let short: String = clean.chars().take(500).collect();
                            format!(" error: {short}")
                        })
                        .unwrap_or_default();
                    observations.push(Observation {
                        attempt_id,
                        provider_epoch: epoch.clone(),
                        dedup_key: format!("session.message:{server}:{id}"),
                        outcome: None,
                        detail: format!(
                            "turn completed{} ({}){server_error}",
                            message
                                .get("finish")
                                .and_then(|v| v.as_str())
                                .map(|finish| format!(": {finish}"))
                                .unwrap_or_default(),
                            usage_note(message),
                        ),
                    });
                }
                let permissions = taken.attached.client.permissions(&server).await.map_err(
                    |error| match error {
                        ClientError::Rejected(message) => AppError::Capability(message),
                        other => AppError::Transport(format!(
                            "reconcile-required: permissions poll failed: {other}"
                        )),
                    },
                )?;
                for permission in &permissions {
                    let Some(normalized) = normalize_request(&epoch, &server, permission) else {
                        continue;
                    };
                    let item = normalized.key.item_id.clone();
                    {
                        let mut state = self.state.lock().expect("gateway state");
                        let key = &normalized.key;
                        let canonical = canonical_key(
                            &key.session.provider,
                            &key.session.instance_epoch,
                            &key.session.thread_id,
                            &key.turn_id,
                            &key.item_id,
                            &key.request_id,
                        );
                        state.wire_routes.entry(canonical).or_insert(WireRoute {
                            session_id: server.clone(),
                            request_id: item.clone(),
                        });
                        state.requests.record(normalized);
                    }
                    if taken.attached.seen_permissions.insert(item.clone()) {
                        observations
                            .push(permission_observation(&epoch, attempt_id, &server, &item));
                    }
                }
                Ok(())
            }
            .await;
        self.restore_transport(taken);
        polled?;
        Ok(observations)
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
        if answer.key.session.provider != "opencode" {
            return Err(DispatchError::Rejected(
                "external answer names a non-opencode session".to_string(),
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
        // Shape the wire answer strictly from the domain verdict: approved
        // allows the request once, declined rejects it with the feedback.
        // Central never sends `always` — standing permissions stay an
        // explicit owner act, never an agent side effect.
        let AnswerPayload::Plain(text) = &answer.payload else {
            return Err(DispatchError::Rejected(
                "secret answers must be resolved to plain text by the caller".to_string(),
            ));
        };
        let (decision, message) = if text == "approved" {
            ("once", None)
        } else if let Some(feedback) = text.strip_prefix("declined:") {
            let feedback = feedback.trim();
            (
                "reject",
                if feedback.is_empty() {
                    None
                } else {
                    Some(feedback)
                },
            )
        } else {
            return Err(DispatchError::Rejected(
                "approval payload must be the domain approved/declined verdict; \
                 refusing to guess a wire decision"
                    .to_string(),
            ));
        };
        // Write through the owning session's transport: the key carries the
        // server session id, so find the session bound to it.
        let taken = {
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
                        .is_some_and(|attached| attached.server_session == route.session_id)
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
                        _workspace: managed.workspace.clone(),
                        _resume_from: managed.resume_from.clone(),
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
        let outcome = taken
            .attached
            .client
            .reply_permission(&route.session_id, &route.request_id, decision, message)
            .await;
        self.restore_transport(taken);
        outcome.map_err(|error| match error {
            // The write may or may not have landed — but the table already
            // consumed this answer, so it can never be resent. Surface
            // OutcomeUnknown so the dispatcher reconciles and waits for the
            // harness to re-ask.
            ClientError::OutcomeUnknown(_)
            | ClientError::Transport(_)
            | ClientError::Rejected(_) => DispatchError::OutcomeUnknown(match error {
                ClientError::Rejected(message)
                | ClientError::OutcomeUnknown(message)
                | ClientError::Transport(message) => message,
            }),
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
                // Normalized keys carry the server session id (== thread).
                (*attempt_id, managed.session.thread_id.clone())
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
        // Best-effort in-turn stop first; the identity-checked terminate
        // below is the real guarantee either way.
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
        // Close the held stdin (the server exits on EOF), then terminate
        // identity-checked: a recycled PID is never killed.
        let attached = { managed.io.lock().expect("session io").take() };
        if let Some(attached) = attached {
            drop(attached.stdin_hold);
            let _ = attached.client.interrupt(&attached.server_session).await;
        }
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
                "terminated owned opencode serve process {} for attempt {}",
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
                            format!("http attached (server session {})", attached.server_session)
                        })
                        .unwrap_or_else(|| "supervision only, no HTTP transport".to_string());
                    (managed.identity.clone(), note)
                }
                None => {
                    return Ok(ReconcileEvidence {
                        attempt_id: target.attempt_id,
                        status: AttemptStatus::UnknownOutcome,
                        detail: "no live session for attempt; outcome unknown, re-read \
                                 server state before reuse"
                            .to_string(),
                    });
                }
            }
        };
        let state = self.supervisor.inspect(&identity).await?;
        // Outcome stays honest: without a completed-message record Central
        // cannot claim the turn finished. The note tells the reconciler
        // what channel exists for follow-up reads.
        Ok(ReconcileEvidence {
            attempt_id: target.attempt_id,
            status: AttemptStatus::UnknownOutcome,
            detail: format!(
                "owned opencode serve process {} is {state:?} ({transport_note}); outcome \
                 unknown, re-read message state before reuse",
                identity.pid
            ),
        })
    }
}

/// Attempt that owns a session reference. opencode thread ids ARE server
/// session ids, so the lookup is exact.
fn session_attempt(
    session: &SessionRef,
    state: &Mutex<GatewayState>,
) -> Result<AttemptId, DispatchError> {
    let state = state.lock().expect("gateway state");
    state
        .sessions
        .iter()
        .find(|(_, managed)| {
            managed.session.thread_id == session.thread_id
                && managed.session.instance_epoch == session.instance_epoch
        })
        .map(|(attempt_id, _)| *attempt_id)
        .ok_or_else(|| {
            DispatchError::Rejected("no live session owns this session reference".to_string())
        })
}
