//! Agent profiles: `GET/POST /v1/profiles` and
//! `GET /v1/profiles/{id}`.
//!
//! Profiles are user-supplied configuration: provider, model, effort
//! and harness config. Each record carries a live capability
//! assessment — `supported`, `unsupported` or `unknown` — resolved
//! against the gateway's advertised capabilities, never fabricated:
//! an unreachable gateway reports `unknown`, and an unadvertised
//! model reports `unsupported`, never a silent substitution. Attempts
//! keep the frozen copy they dispatched with; editing a profile never
//! rewrites history.

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use central_application::ports::store::UserProfileRecord;
use serde::Deserialize;

use super::auth::{app_error_response, authenticate};
use super::AppState;
use crate::api::errors::{error_response, ApiError};
use crate::api::read_models::project_profile;

#[derive(Deserialize)]
pub struct CreateProfileBody {
    pub profile_id: Option<String>,
    pub name: String,
    pub provider: String,
    pub model: String,
    pub effort: Option<String>,
    pub config: Option<serde_json::Value>,
}

async fn capability_for(state: &AppState, record: &UserProfileRecord) -> serde_json::Value {
    let profile = central_domain::ResolvedProfile {
        id: record
            .profile_id
            .parse()
            .unwrap_or_else(|_| central_domain::ProfileId::new()),
        provider: record.provider.clone(),
        model: record.model.clone(),
        effort: record.effort.clone(),
        skills: Vec::new(),
        subagents: Vec::new(),
        permissions: Vec::new(),
        fallbacks: Vec::new(),
        hash: central_domain::ContentHash::from_hex(format!("profile:{}", record.profile_id)),
    };
    match state.agent.capabilities(&profile).await {
        Err(error) => serde_json::json!({
            "status": "unknown",
            "detail": format!("gateway did not report capabilities: {error}"),
        }),
        Ok(capabilities) => {
            if capabilities.provider != profile.provider {
                return serde_json::json!({
                    "status": "unsupported",
                    "detail": format!(
                        "gateway serves '{}', profile wants '{}'",
                        capabilities.provider, profile.provider
                    ),
                });
            }
            let advertised = format!("model:{}", profile.model);
            if capabilities
                .features
                .iter()
                .any(|feature| feature == &advertised)
            {
                serde_json::json!({
                    "status": "supported",
                    "detail": format!("model '{}' advertised by '{}'", profile.model, capabilities.provider),
                })
            } else {
                serde_json::json!({
                    "status": "unsupported",
                    "detail": format!(
                        "model '{}' is not advertised by '{}'; Central never substitutes silently",
                        profile.model, capabilities.provider
                    ),
                })
            }
        }
    }
}

pub async fn list_profiles(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    authenticate(&state, &headers).await?;
    let records = state
        .store
        .list_profiles()
        .await
        .map_err(app_error_response)?;
    let mut items = Vec::new();
    for record in &records {
        items.push(project_profile(
            record,
            capability_for(&state, record).await,
        ));
    }
    Ok(Json(
        serde_json::json!({ "items": items, "next_cursor": None::<String> }),
    ))
}

pub async fn get_profile(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(raw_id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authenticate(&state, &headers).await?;
    let record = state
        .store
        .get_profile(&raw_id)
        .await
        .map_err(app_error_response)?
        .ok_or_else(|| {
            error_response(
                StatusCode::NOT_FOUND,
                "not_found",
                format!("profile {raw_id} not found"),
            )
        })?;
    Ok(Json(project_profile(
        &record,
        capability_for(&state, &record).await,
    )))
}

pub async fn create_profile(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<CreateProfileBody>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let auth = authenticate(&state, &headers).await?;
    auth.require_control()?;
    if body.name.trim().is_empty() {
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            "profile name must not be empty",
        ));
    }
    if body.provider.trim().is_empty() || body.model.trim().is_empty() {
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            "profile provider and model must not be empty",
        ));
    }
    let profile_id = match body.profile_id {
        Some(id) if !id.trim().is_empty() => id.trim().to_string(),
        _ => uuid::Uuid::new_v4().to_string(),
    };
    let record = UserProfileRecord {
        profile_id: profile_id.clone(),
        name: body.name.trim().to_string(),
        provider: body.provider.trim().to_string(),
        model: body.model.trim().to_string(),
        effort: body.effort.unwrap_or_else(|| "low".to_string()),
        config_json: body.config.unwrap_or(serde_json::Value::Null).to_string(),
        updated_at: state.clock.now(),
    };
    state
        .store
        .put_profile(&record)
        .await
        .map_err(app_error_response)?;
    Ok(Json(project_profile(
        &record,
        capability_for(&state, &record).await,
    )))
}

fn runtime_for(state: &AppState, run_id: central_domain::RunId) -> central_application::Runtime {
    central_application::Runtime::new(
        std::sync::Arc::clone(&state.store),
        std::sync::Arc::clone(&state.clock),
        std::sync::Arc::clone(&state.ids),
        std::sync::Arc::clone(&state.agent),
        run_id,
    )
}

/// Bind (or clear, with `null`) the run-wide profile for one agent role:
/// `POST /v1/runs/{id}/profile`. Worker attempts without a pinned task
/// profile dispatch through the worker default; leader reviews dispatch
/// only through the leader profile. Setting a profile never touches live
/// work: only later dispatches resolve through it. Owner or control only.
pub async fn post_run_profile(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(raw_id): Path<String>,
    body: String,
) -> Result<Json<serde_json::Value>, ApiError> {
    let auth = authenticate(&state, &headers).await?;
    auth.require_control()?;
    let run_id = raw_id.parse::<central_domain::RunId>().map_err(|_| {
        error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            format!("invalid run id: {raw_id}"),
        )
    })?;
    let submitted: RunProfileBody = serde_json::from_str(&body).map_err(|error| {
        error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            format!("invalid run profile binding: {error}"),
        )
    })?;
    let role = match submitted.role.as_str() {
        "worker" => central_domain::ProfileRole::Worker,
        "leader" => central_domain::ProfileRole::Leader,
        other => {
            return Err(error_response(
                StatusCode::BAD_REQUEST,
                "validation_failed",
                format!("unknown role {other:?}; want \"worker\" or \"leader\""),
            ));
        }
    };
    let profile = match submitted.profile.as_deref() {
        None | Some("") => None,
        Some(raw) => Some(
            raw.trim()
                .parse::<central_domain::ProfileId>()
                .map_err(|_| {
                    error_response(
                        StatusCode::BAD_REQUEST,
                        "validation_failed",
                        format!("invalid profile id: {raw}"),
                    )
                })?,
        ),
    };
    let receipt = runtime_for(&state, run_id)
        .set_run_profile(auth.actor, role, profile)
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
    Ok(Json(serde_json::json!({
        "run_id": run_id.to_string(),
        "receipt": receipt,
        "profile": {"role": submitted.role, "profile": submitted.profile},
    })))
}

/// The run-profile binding body: which role selects what, or `null` to
/// clear back to defer-when-unpinned.
#[derive(Deserialize)]
struct RunProfileBody {
    role: String,
    profile: Option<String>,
}
