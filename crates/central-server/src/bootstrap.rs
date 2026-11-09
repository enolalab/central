//! Daemon composition root: ownership, credentials, storage,
//! recovery, then HTTP. The order is the safety property — the owner
//! file lock is acquired before the database is opened, and recovery
//! runs (with zero external sends) before the listener accepts work.

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
fn restrict_to_owner(path: &Path, directory: bool) -> Result<(), BootstrapError> {
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
    let (agent, fake_counts): (Arc<dyn AgentGateway>, Option<crate::http::FakeCounts>) =
        if config.fake_agent {
            let fake = Arc::new(crate::fake_agent::FakeAgent::new());
            let counter = fake.clone();
            (
                fake as Arc<dyn AgentGateway>,
                Some(
                    Arc::new(move || (counter.send_count(), counter.cancel_count()))
                        as crate::http::FakeCounts,
                ),
            )
        } else {
            (Arc::new(UnavailableGateway), None)
        };
    #[cfg(not(feature = "test-support"))]
    let (agent, fake_counts): (Arc<dyn AgentGateway>, Option<crate::http::FakeCounts>) =
        (Arc::new(UnavailableGateway), None);

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
    let state = AppState {
        store,
        actor,
        token,
        clock,
        ids,
        agent,
        // Fresh per process: secrets never survive a restart by construction.
        secrets: Arc::new(central_application::inbox::SecretVault::new()),
        fake_agent_enabled: config.fake_agent && cfg!(feature = "test-support"),
        fake_counts,
        shutdown: Arc::new(std::sync::Mutex::new(Some(shutdown_tx))),
    };
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
