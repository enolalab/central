//! Usage and recovery: `GET /v1/runs/{id}/usage`,
//! `POST /v1/runs/{id}/usage/resolve` and `GET /v1/runs/{id}/recovery`.
//!
//! The ledger is always shown with its observed/estimated/unknown
//! split: estimates and missing finals are never presented as
//! observed. Gap resolution is an explicit audited user decision — it
//! releases the hold without converting anything missing into
//! observed. Recovery lists passive findings plus the required user
//! actions; it never dispatches, answers or resumes.

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use central_domain::RunId;
use serde::Deserialize;

use super::auth::{app_error_response, authenticate};
use super::AppState;
use crate::api::errors::{error_response, ApiError};
use crate::api::read_models::project_usage;

async fn load_run(state: &AppState, run_id: RunId) -> Result<central_domain::RunState, ApiError> {
    state
        .store
        .load(run_id)
        .await
        .map_err(app_error_response)?
        .ok_or_else(|| {
            error_response(
                StatusCode::NOT_FOUND,
                "not_found",
                format!("run {run_id} not found"),
            )
        })
}

fn parse_run(raw: &str) -> Result<RunId, ApiError> {
    raw.parse::<RunId>().map_err(|_| {
        error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            format!("invalid run id: {raw}"),
        )
    })
}

pub async fn get_usage(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(raw_id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authenticate(&state, &headers).await?;
    let run_state = load_run(&state, parse_run(&raw_id)?).await?;
    // Admission reason: why new work may or may not dispatch right now.
    let held: Vec<String> = run_state
        .holds
        .iter()
        .map(|hold| format!("{hold:?}"))
        .collect();
    let admission = if run_state.usage.unknown || !run_state.usage.missing_final.is_empty() {
        serde_json::json!({
            "dispatch_open": false,
            "reason": "usage unknown: new work in this run holds until the gap is evidenced or the user resolves it",
        })
    } else if !held.is_empty() {
        serde_json::json!({ "dispatch_open": false, "reason": format!("held: {}", held.join(", ")) })
    } else {
        serde_json::json!({ "dispatch_open": true, "reason": "gate open" })
    };
    Ok(Json(serde_json::json!({
        "run_id": run_state.id.to_string(),
        "revision": run_state.revision.0,
        "ledger": project_usage(&run_state.usage),
        "admission": admission,
    })))
}

#[derive(Deserialize)]
pub struct ResolveGapBody {
    pub missing_key: String,
    pub note: String,
}

/// One audited usage observation to fold into the shared ledger. Every
/// figure is caller-supplied: the daemon never auto-fills these from
/// gateway traffic. The caller cites what it observed (for example the
/// tokenUsage evidence a reconcile response surfaced) and takes
/// responsibility for the numbers — the ledger records the reporting
/// actor alongside them.
#[derive(Deserialize)]
#[serde(default)]
pub struct FoldUsageBody {
    pub provider: String,
    pub epoch: String,
    pub turn: String,
    #[serde(default)]
    pub sequence: u64,
    #[serde(default)]
    pub tokens: u64,
    #[serde(default)]
    pub metric: String,
    #[serde(default)]
    pub source: String,
    #[serde(default)]
    pub finality: String,
    pub cumulative_tokens: Option<u64>,
    pub cumulative_cost_minor: Option<u64>,
    pub cost_minor: Option<u64>,
}

impl Default for FoldUsageBody {
    fn default() -> Self {
        Self {
            provider: String::new(),
            epoch: String::new(),
            turn: String::new(),
            sequence: 0,
            tokens: 0,
            metric: "tokens".to_string(),
            source: "live".to_string(),
            finality: "observed".to_string(),
            cumulative_tokens: None,
            cumulative_cost_minor: None,
            cost_minor: None,
        }
    }
}

fn parse_finality(raw: &str) -> Result<central_domain::UsageFinality, ApiError> {
    use central_domain::UsageFinality;
    match raw.trim().to_ascii_lowercase().as_str() {
        "" | "observed" => Ok(UsageFinality::Observed),
        "estimated" => Ok(UsageFinality::Estimated),
        "in-flight" | "inflight" => Ok(UsageFinality::InFlight),
        "missing-final" | "missing_final" => Ok(UsageFinality::MissingFinal),
        other => Err(error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            format!(
                "unknown finality: {other} (want observed, estimated, in-flight or missing-final)"
            ),
        )),
    }
}

pub async fn post_reconcile_usage(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(raw_id): Path<String>,
    Json(body): Json<FoldUsageBody>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let auth = authenticate(&state, &headers).await?;
    auth.require_control()?;
    let run_id = parse_run(&raw_id)?;
    if body.provider.trim().is_empty()
        || body.epoch.trim().is_empty()
        || body.turn.trim().is_empty()
    {
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            "provider, epoch and turn are required: the ledger keys every report by provider turn sequence",
        ));
    }
    if body.metric.trim().is_empty() || body.source.trim().is_empty() {
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            "metric and source are required: the ledger must know what was counted and where it was reported from",
        ));
    }
    let envelope = central_domain::CommandEnvelope {
        id: state.ids.next_command_id(),
        expected_revision: None,
        command: central_domain::Command::ReconcileUsage {
            observation: central_domain::UsageObservation {
                provider: body.provider.trim().to_string(),
                epoch: body.epoch.trim().to_string(),
                turn: body.turn.trim().to_string(),
                sequence: body.sequence,
                tokens: body.tokens,
                cost_minor: body.cost_minor,
                cumulative_tokens: body.cumulative_tokens,
                cumulative_cost_minor: body.cumulative_cost_minor,
                finality: parse_finality(&body.finality)?,
                metric: body.metric.trim().to_string(),
                source: body.source.trim().to_string(),
            },
        },
    };
    let runtime = central_application::Runtime::new(
        std::sync::Arc::clone(&state.store),
        std::sync::Arc::clone(&state.clock),
        std::sync::Arc::clone(&state.ids),
        std::sync::Arc::clone(&state.agent),
        run_id,
    )
    .with_secrets(std::sync::Arc::clone(&state.secrets));
    let receipt = runtime
        .execute(auth.actor, envelope)
        .await
        .map_err(app_error_response)?;
    state.scan_notify.notify_one();
    let receipt = serde_json::to_value(&receipt).map_err(|error| {
        error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            format!("cannot encode receipt: {error}"),
        )
    })?;
    Ok(Json(receipt))
}

pub async fn post_resolve(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(raw_id): Path<String>,
    Json(body): Json<ResolveGapBody>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let auth = authenticate(&state, &headers).await?;
    auth.require_control()?;
    let run_id = parse_run(&raw_id)?;
    if body.missing_key.trim().is_empty() {
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            "missing_key must not be empty",
        ));
    }
    if body.note.trim().is_empty() {
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            "an audited resolution needs a note",
        ));
    }
    let envelope = central_domain::CommandEnvelope {
        id: state.ids.next_command_id(),
        expected_revision: None,
        command: central_domain::Command::ResolveUsageGap {
            missing_key: body.missing_key.trim().to_string(),
            note: body.note.trim().to_string(),
        },
    };
    let runtime = central_application::Runtime::new(
        std::sync::Arc::clone(&state.store),
        std::sync::Arc::clone(&state.clock),
        std::sync::Arc::clone(&state.ids),
        std::sync::Arc::clone(&state.agent),
        run_id,
    )
    .with_secrets(std::sync::Arc::clone(&state.secrets));
    let receipt = runtime
        .execute(auth.actor, envelope)
        .await
        .map_err(app_error_response)?;
    state.scan_notify.notify_one();
    let receipt = serde_json::to_value(&receipt).map_err(|error| {
        error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            format!("cannot encode receipt: {error}"),
        )
    })?;
    Ok(Json(receipt))
}

pub async fn get_recovery(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(raw_id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authenticate(&state, &headers).await?;
    let run_state = load_run(&state, parse_run(&raw_id)?).await?;
    let report = central_application::recovery::inspect_run(&run_state);
    let report = serde_json::to_value(&report).map_err(|error| {
        error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            format!("cannot encode recovery report: {error}"),
        )
    })?;
    Ok(Json(serde_json::json!({
        "run_id": run_state.id.to_string(),
        "instance_epoch": state.instance_epoch,
        "control": format!("{:?}", run_state.control),
        "actions": report["required_actions"],
        "findings": report["findings"],
    })))
}
