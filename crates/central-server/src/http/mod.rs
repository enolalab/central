pub mod auth;
pub mod commands;
pub mod runs;

use std::sync::Arc;

use axum::extract::DefaultBodyLimit;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::Router;
use central_application::ports::{AgentGateway, Clock, IdSource};
use central_domain::Actor;
use central_infrastructure::storage::DuckDbStore;
use tokio::sync::oneshot;

/// Maximum accepted command body: 1 MiB.
pub const MAX_COMMAND_BODY_BYTES: usize = 1024 * 1024;

/// Test-support fake-agent counters: `(sends, cancels)`.
pub type FakeCounts = Arc<dyn Fn() -> (usize, usize) + Send + Sync>;

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<DuckDbStore>,
    pub actor: Actor,
    pub token: String,
    pub clock: Arc<dyn Clock>,
    pub ids: Arc<dyn IdSource>,
    pub agent: Arc<dyn AgentGateway>,
    pub fake_agent_enabled: bool,
    pub fake_counts: Option<FakeCounts>,
    pub shutdown: Arc<std::sync::Mutex<Option<oneshot::Sender<()>>>>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route(
            "/v1/commands",
            post(commands::post_command).layer(DefaultBodyLimit::max(MAX_COMMAND_BODY_BYTES)),
        )
        .route("/v1/runs", get(runs::list_runs))
        .route("/v1/runs/{id}", get(runs::get_run))
        .route("/v1/runs/{id}/dispatch", post(commands::dispatch))
        .route("/v1/shutdown", post(commands::shutdown))
        .route("/v1/test/fake-sends", get(fake_sends))
        .fallback(ui_not_built)
        .with_state(state)
}

async fn healthz(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({
        "status": "ok",
        "fake_agent": state.fake_agent_enabled,
    }))
}

/// Test-support only: observable fake-agent counters. Without a fake
/// agent (every release build) this is a plain 404.
async fn fake_sends(
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: axum::http::HeaderMap,
) -> Result<axum::Json<serde_json::Value>, auth::ApiError> {
    auth::authenticate(&state, &headers)?;
    match &state.fake_counts {
        Some(count) => {
            let (sends, cancels) = count();
            Ok(axum::Json(serde_json::json!({
                "sends": sends,
                "cancels": cancels,
            })))
        }
        None => Err(auth::error_response(
            StatusCode::NOT_FOUND,
            "fake agent is not enabled in this build",
        )),
    }
}

/// The web UI arrives in P4. Until then every non-API path is a clear
/// 503 — it must never crash or confuse the daemon.
async fn ui_not_built() -> (StatusCode, &'static str) {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        "central web UI is not built yet; use the central CLI or the /v1 API",
    )
}
