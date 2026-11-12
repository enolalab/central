pub mod attempts;
pub mod auth;
pub mod commands;
pub mod inbox;
pub mod integration;
pub mod meta;
pub mod origin;
pub mod runs;

pub mod artifacts;
pub mod chat;
pub mod clients;
pub mod events;
pub mod logs;
pub mod maintenance;
pub mod plans;
pub mod profiles;
pub mod recovery;
pub mod results;
pub mod sync;
pub mod tasks;
pub mod usage;

use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::DefaultBodyLimit;
use axum::http::StatusCode;
use axum::middleware;
use axum::routing::{get, post};
use axum::Router;
use central_application::ports::{AgentGateway, Clock, IdSource, Store, SyncIntake};
use central_domain::Actor;
use tokio::sync::oneshot;

use crate::api::errors::{error_response, ApiError};

/// Maximum accepted command body: 1 MiB.
pub const MAX_COMMAND_BODY_BYTES: usize = 1024 * 1024;

/// Test-support fake-agent counters: `(sends, cancels)`.
pub type FakeCounts = Arc<dyn Fn() -> (usize, usize) + Send + Sync>;

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<dyn Store>,
    pub actor: Actor,
    pub token: String,
    pub clock: Arc<dyn Clock>,
    pub ids: Arc<dyn IdSource>,
    pub agent: Arc<dyn AgentGateway>,
    /// Daemon-side sync intake (the CLI never writes queue files).
    pub sync_intake: Arc<dyn SyncIntake>,
    /// Content-addressed artifact blobs under the state directory.
    pub artifact_root: PathBuf,
    /// Byte store backing artifact content reads (hash-verified).
    pub artifacts: Arc<dyn central_application::ports::ArtifactStore>,
    /// This boot's instance id. Snapshots and cursors bind to it: a
    /// restart rotates it, and old-epoch cursors get `resync_required`
    /// instead of a silent resume.
    pub instance_epoch: String,
    /// Owner-configured browser origins (`origins.json`); empty denies
    /// every browser origin without affecting bearer clients.
    pub origins: Vec<String>,
    /// Memory-only secret vault shared by the answer endpoint and dispatch.
    /// Never persisted, never logged: a restart drops every reference.
    pub secrets: Arc<central_application::inbox::SecretVault>,
    pub fake_agent_enabled: bool,
    pub fake_counts: Option<FakeCounts>,
    pub shutdown: Arc<std::sync::Mutex<Option<oneshot::Sender<()>>>>,
    /// Wakes the daemon scan loop after an API write (periodic ticks still
    /// apply; see `crate::scan`). Never blocks request handling.
    pub scan_notify: Arc<tokio::sync::Notify>,
    /// Git workspace manager owned by the daemon: per-attempt worktrees,
    /// seals and integration live here. Shared with the scan loop so
    /// dispatches for synced repos get real worktrees.
    pub workspace: Arc<central_infrastructure::workspace::GitWorkspaceManager>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/meta", get(meta::get_meta))
        .route("/v1/openapi.json", get(meta::get_openapi))
        .route(
            "/v1/commands",
            post(commands::post_command).layer(DefaultBodyLimit::max(MAX_COMMAND_BODY_BYTES)),
        )
        .route("/v1/runs", get(runs::list_runs))
        .route("/v1/runs/{id}", get(runs::get_run))
        .route("/v1/runs/{id}/dispatch", post(commands::dispatch))
        .route("/v1/runs/{id}/tasks", get(tasks::list_tasks))
        .route("/v1/runs/{id}/tasks/{task}", get(tasks::get_task))
        .route("/v1/runs/{id}/attempts/{attempt}", get(tasks::get_attempt))
        .route("/v1/runs/{id}/plans", get(plans::list_plans))
        .route("/v1/runs/{id}/plans/{hash}", get(plans::get_plan))
        .route("/v1/runs/{id}/plans/diff", get(plans::diff_plans))
        .route("/v1/runs/{id}/events", get(events::list_events))
        .route("/v1/runs/{id}/events/stream", get(events::stream_events))
        .route(
            "/v1/runs/{id}/chat",
            get(chat::list_chat).post(chat::post_chat),
        )
        .route("/v1/runs/{id}/inbox", get(inbox::get_inbox))
        .route("/v1/runs/{id}/inbox/approve", post(inbox::post_approve))
        .route("/v1/runs/{id}/inbox/decline", post(inbox::post_decline))
        .route("/v1/runs/{id}/inbox/answer", post(inbox::post_answer))
        .route("/v1/runs/{id}/inbox/reconcile", post(inbox::post_reconcile))
        .route(
            "/v1/runs/{id}/attempts/{attempt_id}/reconcile",
            post(attempts::post_reconcile_attempt),
        )
        .route(
            "/v1/runs/{id}/attempts/{attempt}/results",
            post(results::post_results),
        )
        .route("/v1/runs/{id}/handover", post(results::post_handover))
        .route("/v1/runs/{id}/usage", get(usage::get_usage))
        .route("/v1/runs/{id}/usage/resolve", post(usage::post_resolve))
        .route("/v1/runs/{id}/recovery", get(usage::get_recovery))
        .route("/v1/recovery", get(recovery::get_recovery_overview))
        .route("/v1/runs/{id}/artifacts", get(artifacts::list_artifacts))
        .route(
            "/v1/runs/{id}/artifacts/{artifact}",
            get(artifacts::get_artifact),
        )
        .route(
            "/v1/runs/{id}/artifacts/{artifact}/content",
            get(artifacts::get_artifact_content),
        )
        .route("/v1/runs/{id}/logs", get(logs::get_logs))
        .route("/v1/runs/{id}/sync", post(sync::post_sync))
        .route(
            "/v1/runs/{id}/integration/run",
            post(integration::post_integration_run),
        )
        .route(
            "/v1/profiles",
            get(profiles::list_profiles).post(profiles::create_profile),
        )
        .route("/v1/profiles/{id}", get(profiles::get_profile))
        .route(
            "/v1/clients",
            get(clients::list_clients).post(clients::create_client),
        )
        .route("/v1/clients/{id}/revoke", post(clients::revoke_client))
        .route("/v1/maintenance/export", post(maintenance::post_export))
        .route("/v1/maintenance/gc", post(maintenance::post_gc))
        .route("/v1/shutdown", post(commands::shutdown))
        .route("/v1/test/fake-sends", get(fake_sends))
        .fallback(outside_api)
        .layer(middleware::from_fn_with_state(
            state.clone(),
            origin::enforce_state,
        ))
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
) -> Result<axum::Json<serde_json::Value>, ApiError> {
    auth::authenticate(&state, &headers).await?;
    match &state.fake_counts {
        Some(count) => {
            let (sends, cancels) = count();
            Ok(axum::Json(serde_json::json!({
                "sends": sends,
                "cancels": cancels,
            })))
        }
        None => Err(error_response(
            StatusCode::NOT_FOUND,
            "not_found",
            "fake agent is not enabled in this build",
        )),
    }
}

/// Outside the public API there is nothing: no UI, no static assets.
/// Every non-API path is a stable 404, never a 503 and never a crash.
async fn outside_api() -> ApiError {
    error_response(
        StatusCode::NOT_FOUND,
        "not_found",
        "no such API path; Central serves no UI — see /v1/openapi.json",
    )
}
