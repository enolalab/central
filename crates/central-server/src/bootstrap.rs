//! Daemon composition root: ownership, credentials, storage,
//! recovery, then HTTP. The order is the safety property — the owner
//! file lock is acquired before the database is opened, and recovery
//! runs (with zero external sends) before the listener accepts work.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use central_application::contracts::{
    AppError, CancelObservation, Capabilities, DispatchError, ExecutionRef, ExternalAnswer,
    OpenSession, ReconcileEvidence, SendTurn, SessionRef, TurnRef,
};
use central_application::ports::{AgentGateway, Clock, IdSource};
use central_domain::{
    Actor, ActorId, CommandId, IntentId, Observation, ResolvedProfile, RunId, Timestamp,
};
use central_infrastructure::muse::{MuseConfig, MuseGateway};
use central_infrastructure::opencode::{OpencodeConfig, OpencodeGateway};
use central_infrastructure::process::{DaemonError, OwnerLock};
use central_infrastructure::storage::DuckDbStore;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::http::{self, AppState};

pub struct DaemonConfig {
    pub state_dir: PathBuf,
    pub listen: String,
    pub fake_agent: bool,
}

/// Owner-configured harnesses in the state directory, written by the owner
/// and never fetched from the network:
/// - Codex: `codex.json` (`{ "binary": "codex", "cli_version":
///   "0.167.0-alpha.3.2", "surveyed_models": ["gpt-6-luna"],
///   "windows_sandbox": "elevated" }`).
/// - Muse: `muse.json` (`{ "binary": "muse", "cli_version": "1.4.23",
///   "surveyed_models": [...], "serve_args": ["serve"] }`).
/// - opencode: `opencode.json` (`{ "binary": "opencode", "cli_version":
///   "2.2.7", "surveyed_models": [...] }`).
///
/// Every field is validated before the gateway is built: the binary must
/// resolve on `PATH`, the CLI version must pass the allowlist, and
/// provider model fallback stays off. Anything missing or invalid drops
/// that provider; with no provider configured the daemon keeps the
/// `UnavailableGateway` — dispatch then defers with a capability error
/// instead of serving fake output.
type BackendList = Vec<(String, Arc<dyn AgentGateway>)>;

fn configured_backends(state_dir: &Path) -> (BackendList, Vec<String>) {
    let mut backends = Vec::new();
    let mut notes = Vec::new();
    match load_codex_gateway(state_dir) {
        Some(gateway) => {
            backends.push(("codex".to_string(), gateway));
            notes.push("codex gateway configured".to_string());
        }
        None => notes.push(codex_config_note(state_dir)),
    }
    match load_muse_gateway(state_dir) {
        Some(gateway) => {
            backends.push(("muse".to_string(), gateway));
            notes.push("muse gateway configured".to_string());
        }
        None => notes.push(muse_config_note(state_dir)),
    }
    match load_opencode_gateway(state_dir) {
        Some(gateway) => {
            backends.push(("opencode".to_string(), gateway));
            notes.push("opencode gateway configured".to_string());
        }
        None => notes.push(opencode_config_note(state_dir)),
    }
    (backends, notes)
}

fn codex_config_note(state_dir: &Path) -> String {
    let path = state_dir.join("codex.json");
    if std::fs::read_to_string(&path).is_err() {
        return "no codex.json: codex provider not configured".to_string();
    }
    "codex.json invalid (bad JSON, binary not on PATH, version not allowlisted, or bad windows_sandbox): ignoring codex config".to_string()
}

fn muse_config_note(state_dir: &Path) -> String {
    let path = state_dir.join("muse.json");
    if std::fs::read_to_string(&path).is_err() {
        return "no muse.json: muse provider not configured".to_string();
    }
    "muse.json invalid (bad JSON, binary not on PATH, version not allowlisted, or bad serve_args): ignoring muse config".to_string()
}

fn opencode_config_note(state_dir: &Path) -> String {
    let path = state_dir.join("opencode.json");
    if std::fs::read_to_string(&path).is_err() {
        return "no opencode.json: opencode provider not configured".to_string();
    }
    "opencode.json invalid (bad JSON, binary not on PATH, or version not allowlisted): ignoring opencode config".to_string()
}

fn load_opencode_gateway(state_dir: &Path) -> Option<Arc<dyn AgentGateway>> {
    let text = std::fs::read_to_string(state_dir.join("opencode.json")).ok()?;
    let value: serde_json::Value = serde_json::from_str(&text).ok()?;
    let binary = value["binary"].as_str().unwrap_or("opencode");
    find_on_path(binary)?;
    let cli_version = value["cli_version"]
        .as_str()
        .unwrap_or(central_infrastructure::opencode::SUPPORTED_CLI_VERSION);
    let version_ok = central_infrastructure::opencode::parse_cli_version(cli_version)
        .map(|parsed| central_infrastructure::opencode::check_supported_version(&parsed).is_ok())
        .unwrap_or(false);
    if !version_ok {
        return None;
    }
    let extra_args: Vec<String> = value["extra_args"]
        .as_array()
        .map(|args| {
            args.iter()
                .filter_map(|arg| arg.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    let config = OpencodeConfig {
        binary: binary.to_string(),
        cli_version: cli_version.to_string(),
        surveyed_models: surveyed_models(&value),
        extra_args,
        ..OpencodeConfig::default()
    };
    let supervisor = Arc::new(central_infrastructure::process::OwnedProcessSupervisor::new());
    let gateway = OpencodeGateway::new(
        supervisor.clone() as Arc<dyn central_application::ports::ProcessSupervisor>,
        config,
    )
    .with_piped(supervisor as Arc<dyn central_infrastructure::process::PipedSpawner>);
    Some(Arc::new(gateway))
}

fn surveyed_models(value: &serde_json::Value) -> Vec<String> {
    value["surveyed_models"]
        .as_array()
        .map(|models| {
            models
                .iter()
                .filter_map(|model| model.as_str().map(|model| model.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

pub(crate) fn read_model_catalog(state_dir: &Path) -> BTreeMap<String, BTreeSet<String>> {
    std::fs::read_to_string(state_dir.join("model-catalog.json"))
        .ok()
        .and_then(|text| serde_json::from_str::<BTreeMap<String, BTreeSet<String>>>(&text).ok())
        .unwrap_or_default()
}

pub(crate) fn enables_catalog_models(config: &serde_json::Value) -> bool {
    config["enable_catalog_models"].as_bool().unwrap_or(true)
}

fn load_codex_gateway(state_dir: &Path) -> Option<Arc<dyn AgentGateway>> {
    let text = std::fs::read_to_string(state_dir.join("codex.json")).ok()?;
    let value: serde_json::Value = serde_json::from_str(&text).ok()?;
    let binary = value["binary"].as_str().unwrap_or("codex");
    find_on_path(binary)?;
    let cli_version = value["cli_version"]
        .as_str()
        .unwrap_or(central_infrastructure::codex::SUPPORTED_CLI_VERSION);
    let version_ok = central_infrastructure::codex::parse_cli_version(cli_version)
        .map(|parsed| central_infrastructure::codex::check_supported_version(&parsed).is_ok())
        .unwrap_or(false);
    if !version_ok {
        return None;
    }
    let app_server_args = codex_server_args(value.get("windows_sandbox")).ok()?;
    let config = central_infrastructure::codex::CodexConfig {
        binary: binary.to_string(),
        cli_version: cli_version.to_string(),
        surveyed_models: surveyed_models(&value),
        app_server_args,
        ..central_infrastructure::codex::CodexConfig::default()
    };
    // One owned supervisor plays both roles: process ownership/kill and
    // the piped stdio the RPC attach rides on. Same process, same
    // identity-checked kill path either way.
    let supervisor = Arc::new(central_infrastructure::process::OwnedProcessSupervisor::new());
    let gateway = central_infrastructure::codex::CodexGateway::new(
        supervisor.clone() as Arc<dyn central_application::ports::ProcessSupervisor>,
        config,
    )
    .with_piped(supervisor as Arc<dyn central_infrastructure::process::PipedSpawner>);
    Some(Arc::new(gateway))
}

fn load_muse_gateway(state_dir: &Path) -> Option<Arc<dyn AgentGateway>> {
    let text = std::fs::read_to_string(state_dir.join("muse.json")).ok()?;
    let value: serde_json::Value = serde_json::from_str(&text).ok()?;
    let binary = value["binary"].as_str().unwrap_or("muse");
    find_on_path(binary)?;
    let cli_version = value["cli_version"]
        .as_str()
        .unwrap_or(central_infrastructure::muse::SUPPORTED_CLI_VERSION);
    let version_ok = central_infrastructure::muse::parse_cli_version(cli_version)
        .map(|parsed| central_infrastructure::muse::check_supported_version(&parsed).is_ok())
        .unwrap_or(false);
    if !version_ok {
        return None;
    }
    let serve_args = match value.get("serve_args") {
        None => vec!["serve".to_string()],
        Some(array) => {
            let args: Vec<String> = array
                .as_array()?
                .iter()
                .map(|arg| arg.as_str().map(str::to_string))
                .collect::<Option<Vec<_>>>()?;
            if args.is_empty() {
                return None;
            }
            args
        }
    };
    let config = MuseConfig {
        binary: binary.to_string(),
        cli_version: cli_version.to_string(),
        surveyed_models: surveyed_models(&value),
        serve_args,
        ..MuseConfig::default()
    };
    let supervisor = Arc::new(central_infrastructure::process::OwnedProcessSupervisor::new());
    let gateway = MuseGateway::new(
        supervisor.clone() as Arc<dyn central_application::ports::ProcessSupervisor>,
        config,
    )
    .with_piped(supervisor as Arc<dyn central_infrastructure::process::PipedSpawner>);
    Some(Arc::new(gateway))
}

/// Fan-out over the configured harness backends. Every method routes on
/// the provider the caller names (`profile.provider` for capabilities/open,
/// the session/key provider otherwise) BEFORE any profile resolution, so a
/// `muse` attempt never touches the Codex process and vice versa. An
/// unconfigured or unknown provider fails closed as a capability error —
/// never fake output, never a cross-harness send.
pub struct RouterGateway {
    backends: HashMap<String, Arc<dyn AgentGateway>>,
}

impl RouterGateway {
    pub fn new() -> Self {
        Self {
            backends: HashMap::new(),
        }
    }

    pub fn insert(&mut self, provider: &str, gateway: Arc<dyn AgentGateway>) {
        self.backends.insert(provider.to_string(), gateway);
    }

    /// Configured providers, sorted. Published on `GET /v1/meta`.
    pub fn providers(&self) -> Vec<String> {
        let mut providers: Vec<String> = self.backends.keys().cloned().collect();
        providers.sort();
        providers
    }

    fn route(&self, provider: &str) -> Result<Arc<dyn AgentGateway>, AppError> {
        self.backends.get(provider).cloned().ok_or_else(|| {
            let mut configured = self.providers();
            if configured.is_empty() {
                configured.push("(none)".to_string());
            }
            AppError::Capability(format!(
                "provider '{provider}' is not configured in this daemon (configured: [{}]); \
                 add the harness config to the state directory and restart",
                configured.join(", "),
            ))
        })
    }
}

impl Default for RouterGateway {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl AgentGateway for RouterGateway {
    async fn capabilities(
        &self,
        profile: &central_domain::ResolvedProfile,
    ) -> Result<central_application::contracts::Capabilities, AppError> {
        self.route(&profile.provider)?.capabilities(profile).await
    }

    async fn open(
        &self,
        request: central_application::contracts::OpenSession,
    ) -> Result<central_application::contracts::SessionRef, AppError> {
        self.route(&request.profile.provider)?.open(request).await
    }

    async fn send(
        &self,
        request: central_application::contracts::SendTurn,
    ) -> Result<
        central_application::contracts::TurnRef,
        central_application::contracts::DispatchError,
    > {
        self.route(&request.session.provider)
            .map_err(|error| DispatchError::Rejected(error.to_string()))?
            .send(request)
            .await
    }

    async fn observe(
        &self,
        session: &central_application::contracts::SessionRef,
    ) -> Result<Vec<central_domain::Observation>, AppError> {
        self.route(&session.provider)?.observe(session).await
    }

    async fn answer(
        &self,
        answer: central_application::contracts::ExternalAnswer,
    ) -> Result<(), central_application::contracts::DispatchError> {
        self.route(&answer.key.session.provider)
            .map_err(|error| DispatchError::Rejected(error.to_string()))?
            .answer(answer)
            .await
    }

    async fn cancel(
        &self,
        target: &central_application::contracts::ExecutionRef,
    ) -> Result<central_application::contracts::CancelObservation, AppError> {
        self.route(&target.session.provider)?.cancel(target).await
    }

    async fn reconcile(
        &self,
        target: &central_application::contracts::ExecutionRef,
    ) -> Result<central_application::contracts::ReconcileEvidence, AppError> {
        self.route(&target.session.provider)?
            .reconcile(target)
            .await
    }

    async fn drain_requests(
        &self,
        session: &central_application::contracts::SessionRef,
    ) -> Result<Vec<central_application::contracts::ExternalRequest>, AppError> {
        self.route(&session.provider)?.drain_requests(session).await
    }
}

/// Build the daemon's agent from the owner-configured backends. One
/// backend serves directly (today's single-harness behavior); several
/// fan out through the [`RouterGateway`]; none keeps the
/// [`UnavailableGateway`]. Returns the agent plus the sorted provider
/// list published on `GET /v1/meta`.
fn build_agent(state_dir: &Path) -> (Arc<dyn AgentGateway>, Vec<String>) {
    let (backends, notes) = configured_backends(state_dir);
    for note in &notes {
        eprintln!("central-server: gateway: {note}");
    }
    let mut providers: Vec<String> = backends.iter().map(|(p, _)| p.clone()).collect();
    providers.sort();
    let agent: Arc<dyn AgentGateway> = if backends.is_empty() {
        Arc::new(UnavailableGateway)
    } else if backends.len() == 1 {
        backends
            .into_iter()
            .next()
            .map(|(_, gateway)| gateway)
            .expect("one backend")
    } else {
        let mut router = RouterGateway::new();
        for (provider, gateway) in backends {
            router.insert(&provider, gateway);
        }
        Arc::new(router)
    };
    (agent, providers)
}

/// An optional, explicit per-daemon override; never substitute a weaker
/// Windows sandbox automatically or accept arbitrary CLI/config arguments.
fn codex_server_args(value: Option<&serde_json::Value>) -> Result<Vec<String>, ()> {
    match value {
        None => Ok(vec!["app-server".into()]),
        Some(serde_json::Value::String(mode)) if mode == "elevated" || mode == "unelevated" => {
            Ok(vec![
                "-c".into(),
                format!("windows.sandbox=\"{mode}\""),
                "app-server".into(),
            ])
        }
        _ => Err(()),
    }
}

#[cfg(test)]
mod codex_sandbox_config_tests {
    use super::codex_server_args;
    use serde_json::json;

    #[test]
    fn sandbox_selection_is_explicit_and_allowlisted() {
        assert_eq!(codex_server_args(None).unwrap(), ["app-server"]);
        assert_eq!(
            codex_server_args(Some(&json!("elevated"))).unwrap(),
            ["-c", "windows.sandbox=\"elevated\"", "app-server"]
        );
        assert_eq!(
            codex_server_args(Some(&json!("unelevated"))).unwrap(),
            ["-c", "windows.sandbox=\"unelevated\"", "app-server"]
        );
        for invalid in [
            json!("danger-full-access"),
            json!("elevated\" -c x=1"),
            json!(null),
            json!(true),
        ] {
            assert!(codex_server_args(Some(&invalid)).is_err());
        }
    }
}

#[cfg(test)]
mod router_tests {
    use super::RouterGateway;
    use central_application::contracts::SessionRef;
    use central_application::ports::AgentGateway;
    use central_domain::{ContentHash, ProfileId, ResolvedProfile};

    fn profile_for(provider: &str) -> ResolvedProfile {
        ResolvedProfile {
            id: ProfileId::from_u128(7),
            provider: provider.to_string(),
            model: "model-under-test".to_string(),
            effort: "low".to_string(),
            skills: vec![],
            subagents: vec![],
            permissions: vec![],
            fallbacks: vec![],
            hash: ContentHash::from_hex("e".repeat(64)),
        }
    }

    fn session_for(provider: &str, epoch: &str) -> SessionRef {
        SessionRef {
            provider: provider.to_string(),
            instance_epoch: epoch.to_string(),
            thread_id: "thread-under-test".to_string(),
        }
    }

    #[test]
    fn providers_are_sorted_and_empty_router_fails_closed() {
        let empty = RouterGateway::new();
        assert!(empty.providers().is_empty());
        let error = match empty.route("muse") {
            Ok(_) => panic!("empty router must fail closed"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("(none)"));

        let mut router = RouterGateway::new();
        router.insert("muse", std::sync::Arc::new(super::UnavailableGateway));
        router.insert("codex", std::sync::Arc::new(super::UnavailableGateway));
        assert_eq!(router.providers(), vec!["codex", "muse"]);
        let error = match router.route("opencode") {
            Ok(_) => panic!("unknown provider must fail closed"),
            Err(error) => error,
        };
        let message = error.to_string();
        assert!(message.contains("opencode"), "{message}");
        assert!(
            message.contains("codex") && message.contains("muse"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn capabilities_route_by_profile_provider() {
        let mut router = RouterGateway::new();
        router.insert("muse", std::sync::Arc::new(super::UnavailableGateway));
        // A muse profile reaches the muse backend (here the closed
        // UnavailableGateway), never a cross-harness send.
        let error = router
            .capabilities(&profile_for("muse"))
            .await
            .expect_err("unavailable backend reports capability");
        assert!(matches!(
            error,
            central_application::contracts::AppError::Capability(_)
        ));
        // An unconfigured provider fails before touching any backend.
        let error = router
            .capabilities(&profile_for("opencode"))
            .await
            .expect_err("opencode is not configured");
        assert!(error.to_string().contains("opencode"));
    }

    #[tokio::test]
    async fn observe_routes_by_session_provider() {
        let mut router = RouterGateway::new();
        router.insert("codex", std::sync::Arc::new(super::UnavailableGateway));
        let error = router
            .observe(&session_for("muse", "epoch-1"))
            .await
            .expect_err("muse is not configured");
        let message = error.to_string();
        assert!(message.contains("muse"), "{message}");
        assert!(message.contains("codex"), "{message}");
    }
}

pub(crate) fn find_on_path(binary: &str) -> Option<PathBuf> {
    if binary.contains('/') || binary.contains('\\') {
        let path = PathBuf::from(binary);
        return path.is_file().then_some(path);
    }
    let path_var = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path_var) {
        #[cfg(windows)]
        for name in [format!("{binary}.exe"), binary.to_string()] {
            let candidate = dir.join(&name);
            if candidate.is_file() {
                return Some(candidate);
            }
        }
        #[cfg(not(windows))]
        {
            let candidate = dir.join(binary);
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

#[derive(Debug)]
pub enum BootstrapError {
    AlreadyRunning,
    Storage(AppError),
    Io(std::io::Error),
    Bind(std::io::Error),
    InvalidConfig(String),
}

impl std::fmt::Display for BootstrapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyRunning => write!(f, "a daemon already owns this state directory"),
            Self::Storage(error) => write!(f, "storage error: {error}"),
            Self::Io(error) => write!(f, "io error: {error}"),
            Self::Bind(error) => write!(f, "cannot bind listener: {error}"),
            Self::InvalidConfig(msg) => write!(f, "invalid configuration: {msg}"),
        }
    }
}

impl std::error::Error for BootstrapError {}

impl From<DaemonError> for BootstrapError {
    fn from(error: DaemonError) -> Self {
        match error {
            DaemonError::AlreadyRunning => Self::AlreadyRunning,
            DaemonError::Io(error) => Self::Io(error),
        }
    }
}

impl From<AppError> for BootstrapError {
    fn from(error: AppError) -> Self {
        Self::Storage(error)
    }
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        let millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
            .unwrap_or(0);
        Timestamp::from_millis(millis)
    }
}

pub struct UuidIds;

impl IdSource for UuidIds {
    fn next_run_id(&self) -> RunId {
        RunId::from_uuid(Uuid::new_v4())
    }
    fn next_intent_id(&self) -> IntentId {
        IntentId::from_uuid(Uuid::new_v4())
    }
    fn next_command_id(&self) -> CommandId {
        CommandId::from_uuid(Uuid::new_v4())
    }
}

/// Production gateway when no real harness is configured (P1 has no
/// live-model integration). It never fabricates a successful send:
/// every operation reports that the capability is unavailable, so a
/// release build can never serve fake agent output.
pub struct UnavailableGateway;

#[async_trait]
impl AgentGateway for UnavailableGateway {
    async fn capabilities(&self, _profile: &ResolvedProfile) -> Result<Capabilities, AppError> {
        Err(AppError::Capability(
            "no agent gateway is configured in this build".to_string(),
        ))
    }

    async fn open(&self, _request: OpenSession) -> Result<SessionRef, AppError> {
        Err(AppError::Capability(
            "no agent gateway is configured in this build".to_string(),
        ))
    }

    async fn send(&self, _request: SendTurn) -> Result<TurnRef, DispatchError> {
        Err(DispatchError::Rejected(
            "no agent gateway is configured in this build".to_string(),
        ))
    }

    async fn observe(&self, _session: &SessionRef) -> Result<Vec<Observation>, AppError> {
        Err(AppError::Capability(
            "no agent gateway is configured in this build".to_string(),
        ))
    }

    async fn answer(&self, _answer: ExternalAnswer) -> Result<(), DispatchError> {
        Err(DispatchError::Rejected(
            "no agent gateway is configured in this build".to_string(),
        ))
    }

    async fn cancel(&self, _target: &ExecutionRef) -> Result<CancelObservation, AppError> {
        Err(AppError::Capability(
            "no agent gateway is configured in this build".to_string(),
        ))
    }

    async fn reconcile(&self, target: &ExecutionRef) -> Result<ReconcileEvidence, AppError> {
        let _ = target;
        Err(AppError::Capability(
            "no agent gateway is configured in this build".to_string(),
        ))
    }
}

/// Parse `--listen` and accept loopback addresses only. The API is
/// authenticated by a bearer token in the state directory; binding a
/// non-loopback address would expose it off this machine, so anything
/// else (including `0.0.0.0` and `[::]`) is rejected before binding.
pub fn parse_listen_addr(listen: &str) -> Result<std::net::SocketAddr, BootstrapError> {
    let addr: std::net::SocketAddr = listen.parse().map_err(|_| {
        BootstrapError::InvalidConfig(format!(
            "--listen must be a loopback socket address such as 127.0.0.1:7878, got {listen:?}"
        ))
    })?;
    if !addr.ip().is_loopback() {
        return Err(BootstrapError::InvalidConfig(format!(
            "--listen must be a loopback address, got {listen:?}; \
             refusing to expose the local runtime API off-loopback"
        )));
    }
    Ok(addr)
}

/// Restrict `path` so only the OS user running the daemon can access
/// it: a protected owner-only DACL on Windows, mode `0700`/`0600` on
/// Unix. Applied to the state directory and to the credential/info
/// files inside it; failure fails closed.
pub(crate) fn restrict_to_owner(path: &Path, directory: bool) -> Result<(), BootstrapError> {
    #[cfg(windows)]
    {
        owner_only_windows::restrict(path, directory).map_err(BootstrapError::Io)
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = if directory { 0o700 } else { 0o600 };
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
            .map_err(BootstrapError::Io)
    }
    #[cfg(not(any(windows, unix)))]
    {
        let _ = (path, directory);
        Ok(())
    }
}

#[cfg(windows)]
mod owner_only_windows {
    use std::ffi::c_void;
    use std::path::Path;

    use windows_sys::Win32::Foundation::{CloseHandle, LocalFree, GENERIC_ALL, HANDLE};
    use windows_sys::Win32::Security::Authorization::{
        SetEntriesInAclW, SetNamedSecurityInfoW, EXPLICIT_ACCESS_W, SE_FILE_OBJECT, TRUSTEE_IS_SID,
        TRUSTEE_IS_USER, TRUSTEE_W,
    };
    use windows_sys::Win32::Security::{
        GetTokenInformation, TokenUser, DACL_SECURITY_INFORMATION,
        PROTECTED_DACL_SECURITY_INFORMATION, TOKEN_QUERY, TOKEN_USER,
    };
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

    // ACCESS_MODE::SET_ACCESS and MULTIPLE_TRUSTEE_OPERATION::NO_MULTIPLE_TRUSTEE.
    const SET_ACCESS: i32 = 2;
    // ACE inheritance: OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE.
    const INHERIT_TO_CHILDREN: u32 = 0x1 | 0x2;
    const ERROR_SUCCESS: u32 = 0;

    pub fn restrict(path: &Path, directory: bool) -> std::io::Result<()> {
        let wide: Vec<u16> = std::os::windows::ffi::OsStrExt::encode_wide(path.as_os_str())
            .chain(std::iter::once(0))
            .collect();
        unsafe {
            let mut token: HANDLE = std::ptr::null_mut();
            if OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) == 0 {
                return Err(std::io::Error::last_os_error());
            }
            let result = restrict_with_token(token, &wide, directory);
            CloseHandle(token);
            result
        }
    }

    unsafe fn restrict_with_token(
        token: HANDLE,
        wide_path: &[u16],
        directory: bool,
    ) -> std::io::Result<()> {
        // Size the TOKEN_USER buffer, then read the owner SID from it.
        let mut needed: u32 = 0;
        GetTokenInformation(token, TokenUser, std::ptr::null_mut(), 0, &mut needed);
        if needed == 0 {
            return Err(std::io::Error::last_os_error());
        }
        let mut buffer = vec![0u8; needed as usize];
        if GetTokenInformation(
            token,
            TokenUser,
            buffer.as_mut_ptr() as *mut c_void,
            needed,
            &mut needed,
        ) == 0
        {
            return Err(std::io::Error::last_os_error());
        }
        let owner_sid = (*(buffer.as_ptr() as *const TOKEN_USER)).User.Sid;

        let mut trustee: TRUSTEE_W = std::mem::zeroed();
        trustee.TrusteeForm = TRUSTEE_IS_SID;
        trustee.TrusteeType = TRUSTEE_IS_USER;
        // With TRUSTEE_IS_SID, ptstrName carries the SID pointer.
        trustee.ptstrName = owner_sid as *mut u16;

        let mut entry: EXPLICIT_ACCESS_W = std::mem::zeroed();
        entry.grfAccessPermissions = GENERIC_ALL;
        entry.grfAccessMode = SET_ACCESS;
        entry.grfInheritance = if directory { INHERIT_TO_CHILDREN } else { 0 };
        entry.Trustee = trustee;

        let mut acl: *mut windows_sys::Win32::Security::ACL = std::ptr::null_mut();
        let code = SetEntriesInAclW(1, &entry, std::ptr::null(), &mut acl);
        if code != ERROR_SUCCESS {
            return Err(std::io::Error::from_raw_os_error(code as i32));
        }
        let code = SetNamedSecurityInfoW(
            wide_path.as_ptr(),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            acl,
            std::ptr::null_mut(),
        );
        LocalFree(acl as *mut c_void);
        if code != ERROR_SUCCESS {
            return Err(std::io::Error::from_raw_os_error(code as i32));
        }
        Ok(())
    }
}

fn load_or_create_token(state_dir: &Path) -> Result<String, BootstrapError> {
    let path = state_dir.join("daemon.token");
    if let Ok(existing) = std::fs::read_to_string(&path) {
        let token = existing.trim().to_string();
        if !token.is_empty() {
            restrict_to_owner(&path, false)?;
            return Ok(token);
        }
    }
    let token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    std::fs::write(&path, &token).map_err(BootstrapError::Io)?;
    restrict_to_owner(&path, false)?;
    Ok(token)
}

fn load_or_create_actor(state_dir: &Path) -> Result<Actor, BootstrapError> {
    let path = state_dir.join("owner.json");
    if let Ok(existing) = std::fs::read_to_string(&path) {
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(&existing) {
            if let Some(raw) = value["actor_id"].as_str() {
                if let Ok(uuid) = raw.parse::<Uuid>() {
                    restrict_to_owner(&path, false)?;
                    return Ok(Actor::user(ActorId::from_uuid(uuid)));
                }
            }
        }
    }
    let actor = Actor::user(ActorId::from_uuid(Uuid::new_v4()));
    let body = serde_json::json!({ "actor_id": actor.id.to_string() }).to_string();
    std::fs::write(&path, body).map_err(BootstrapError::Io)?;
    restrict_to_owner(&path, false)?;
    Ok(actor)
}

struct ServerInfoGuard(PathBuf);

impl Drop for ServerInfoGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

pub async fn run(config: DaemonConfig) -> Result<(), BootstrapError> {
    // Loopback only, validated before any state is touched or bound.
    let listen_addr = parse_listen_addr(&config.listen)?;
    if config.fake_agent && cfg!(not(feature = "test-support")) {
        return Err(BootstrapError::InvalidConfig(
            "the fake agent is only available in test-support builds; \
             this release build cannot serve fake output"
                .to_string(),
        ));
    }

    // (1) Single-owner lock BEFORE opening the database.
    let _owner = OwnerLock::acquire(&config.state_dir)?;
    // The state directory holds the credential; only this OS user may
    // read it or anything created inside it from here on.
    restrict_to_owner(&config.state_dir, true)?;

    // (2) Owner-only credentials in the state directory. The token is
    // the only credential; the actor is derived from it, never from a
    // request body.
    let token = load_or_create_token(&config.state_dir)?;
    let actor = load_or_create_actor(&config.state_dir)?;

    // (3) Storage. Failure here fails closed: no listener, no success.
    let db_path = config.state_dir.join("central.duckdb");
    let store = Arc::new(DuckDbStore::open(&db_path).map_err(BootstrapError::Storage)?);

    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let ids: Arc<dyn IdSource> = Arc::new(UuidIds);

    #[cfg(feature = "test-support")]
    let (agent, providers, fake_counts): (
        Arc<dyn AgentGateway>,
        Vec<String>,
        Option<crate::http::FakeCounts>,
    ) = if config.fake_agent {
        let fake = Arc::new(crate::fake_agent::FakeAgent::new());
        let counter = fake.clone();
        (
            fake as Arc<dyn AgentGateway>,
            vec!["fake".to_string()],
            Some(
                Arc::new(move || (counter.send_count(), counter.cancel_count()))
                    as crate::http::FakeCounts,
            ),
        )
    } else {
        // Owner-configured harness backends (`codex.json` / `muse.json`
        // when each validates), else the capability-closed gateway that
        // defers instead of faking.
        let (gateway, providers) = build_agent(&config.state_dir);
        (gateway, providers, None)
    };
    #[cfg(not(feature = "test-support"))]
    let (agent, providers, fake_counts): (
        Arc<dyn AgentGateway>,
        Vec<String>,
        Option<crate::http::FakeCounts>,
    ) = {
        // Release builds never serve the fake agent; the real gateway
        // still needs the owner's explicit harness config.
        let (gateway, providers) = build_agent(&config.state_dir);
        (gateway, providers, None)
    };

    // (4) Startup recovery: park nonterminal runs, mark dispatching
    // work unknown, send nothing.
    let summary = crate::recovery::recover_all(
        store.clone() as Arc<dyn central_application::ports::Store>,
        clock.clone(),
        ids.clone(),
        agent.clone(),
    )
    .await
    .map_err(BootstrapError::Storage)?;
    eprintln!(
        "central-server: recovery runs_seen={} awaiting_resume={} unknown_outcome={}",
        summary.runs_seen, summary.awaiting_resume, summary.unknown_outcome
    );

    // (5) HTTP listener on the validated loopback address only.
    let listener = tokio::net::TcpListener::bind(listen_addr)
        .await
        .map_err(BootstrapError::Bind)?;
    let addr = listener.local_addr().map_err(BootstrapError::Io)?;

    let info_path = config.state_dir.join("server.json");
    let info = serde_json::json!({
        "addr": addr.to_string(),
        "pid": std::process::id(),
        "actor_id": actor.id.to_string(),
        "fake_agent": config.fake_agent && cfg!(feature = "test-support"),
    });
    std::fs::write(&info_path, info.to_string()).map_err(BootstrapError::Io)?;
    restrict_to_owner(&info_path, false)?;
    let _info_guard = ServerInfoGuard(info_path);

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let scan_notify = Arc::new(tokio::sync::Notify::new());
    // This boot's instance id: snapshots and cursors bind to it, and a
    // restart rotates it so old cursors fail fast with resync_required.
    let instance_epoch = Uuid::new_v4().to_string();
    let origins = crate::http::origin::OriginConfig::load(&config.state_dir).allowed;
    let artifacts = Arc::new(
        central_infrastructure::artifacts::FilesystemArtifactStore::open(
            config.state_dir.join("artifacts"),
        )
        .map_err(BootstrapError::Storage)?,
    );
    // Git workspaces live directly under the state dir so the manager's
    // sync-queue is the same directory the sync intake writes to: the
    // daemon never has two sources of truth for user-synced commits.
    let workspace = Arc::new(
        central_infrastructure::workspace::GitWorkspaceManager::open(&config.state_dir)
            .map_err(BootstrapError::Storage)?,
    );
    let state = AppState {
        state_dir: config.state_dir.clone(),
        store,
        actor,
        token,
        clock,
        ids,
        agent,
        providers,
        sync_intake: Arc::new(central_infrastructure::workspace::DaemonSyncIntake::new(
            config.state_dir.clone(),
        )),
        artifact_root: config.state_dir.join("artifacts"),
        artifacts,
        instance_epoch,
        origins,
        // Fresh per process: secrets never survive a restart by construction.
        secrets: Arc::new(central_application::inbox::SecretVault::new()),
        fake_agent_enabled: config.fake_agent && cfg!(feature = "test-support"),
        fake_counts,
        shutdown: Arc::new(std::sync::Mutex::new(Some(shutdown_tx))),
        scan_notify: Arc::clone(&scan_notify),
        workspace,
    };
    // Daemon scan loop (P3/T6 carries T1's scheduler into production):
    // periodic + on-event ticks via should_tick/tick. Parked runs stay
    // parked — only an explicit Resume reopens their gate — and dispatch
    // rechecks every gate, so the loop can never send before resume.
    // `CENTRAL_SCAN_MS=0` disables it (deterministic tests).
    let mut scan_config = crate::scan::ScanConfig::from_env(state.fake_agent_enabled);
    // Live turns need a real, owned working directory: every dispatched
    // attempt gets its own scratch leaf under the state dir (explicit,
    // per-attempt, never the repo). This is scratch isolation, not Git
    // sealing — worktrees stay on the workspace integration path.
    scan_config.scheduler.attempt_scratch_root = Some(config.state_dir.join("attempt-scratch"));
    if scan_config.enabled() {
        crate::scan::spawn_scan_loop(
            Arc::clone(&state.store) as Arc<dyn central_application::ports::Store>,
            Arc::clone(&state.clock) as Arc<dyn central_application::ports::Clock>,
            Arc::clone(&state.ids) as Arc<dyn central_application::ports::IdSource>,
            Arc::clone(&state.agent) as Arc<dyn central_application::ports::AgentGateway>,
            state.actor.clone(),
            scan_config,
            scan_notify,
            Arc::clone(&state.secrets),
            Some(Arc::clone(&state.workspace)
                as Arc<
                    dyn central_application::ports::workspace::WorkspaceManager,
                >),
        );
    }
    eprintln!("central-server: listening on {addr}");

    let router = http::router(state);
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            let _ = shutdown_rx.await;
        })
        .await
        .map_err(BootstrapError::Io)?;
    Ok(())
}
