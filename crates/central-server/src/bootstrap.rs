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

fn load_or_create_token(state_dir: &Path) -> Result<String, BootstrapError> {
    let path = state_dir.join("daemon.token");
    if let Ok(existing) = std::fs::read_to_string(&path) {
        let token = existing.trim().to_string();
        if !token.is_empty() {
            return Ok(token);
        }
    }
    let token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    std::fs::write(&path, &token).map_err(BootstrapError::Io)?;
    Ok(token)
}

fn load_or_create_actor(state_dir: &Path) -> Result<Actor, BootstrapError> {
    let path = state_dir.join("owner.json");
    if let Ok(existing) = std::fs::read_to_string(&path) {
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(&existing) {
            if let Some(raw) = value["actor_id"].as_str() {
                if let Ok(uuid) = raw.parse::<Uuid>() {
                    return Ok(Actor::user(ActorId::from_uuid(uuid)));
                }
            }
        }
    }
    let actor = Actor::user(ActorId::from_uuid(Uuid::new_v4()));
    let body = serde_json::json!({ "actor_id": actor.id.to_string() }).to_string();
    std::fs::write(&path, body).map_err(BootstrapError::Io)?;
    Ok(actor)
}

struct ServerInfoGuard(PathBuf);

impl Drop for ServerInfoGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

pub async fn run(config: DaemonConfig) -> Result<(), BootstrapError> {
    if config.fake_agent && cfg!(not(feature = "test-support")) {
        return Err(BootstrapError::InvalidConfig(
            "the fake agent is only available in test-support builds; \
             this release build cannot serve fake output"
                .to_string(),
        ));
    }

    // (1) Single-owner lock BEFORE opening the database.
    let _owner = OwnerLock::acquire(&config.state_dir)?;

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

    // (5) HTTP listener (loopback per the CLI-provided address).
    let listener = tokio::net::TcpListener::bind(&config.listen)
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
    let _info_guard = ServerInfoGuard(info_path);

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let state = AppState {
        store,
        actor,
        token,
        clock,
        ids,
        agent,
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
