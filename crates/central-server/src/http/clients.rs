//! Client credentials: `GET/POST /v1/clients` and
//! `POST /v1/clients/{id}/revoke`. Owner-only: control tokens never
//! mint other tokens, and no endpoint ever returns a plaintext secret
//! except the one-time issuance answer. Listings carry metadata only —
//! no secret, no hash. Revocation is monotonic and takes effect on new
//! requests immediately; open streams re-check every tick and close.

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use central_application::client_access::{issue_bridge, issue_client};
use central_application::ports::store::ClientScope;
use serde::Deserialize;

use super::auth::authenticate;
use super::AppState;
use crate::api::errors::{error_response, ApiError};

#[derive(Deserialize)]
pub struct CreateClientBody {
    pub name: String,
    /// `read`, `control`, or `bridge` (bridge also needs `role` and
    /// `attempt`).
    pub scope: String,
    pub role: Option<String>,
    pub attempt: Option<String>,
    /// Optional expiry as UTC ms. Past expiries are refused.
    pub expires_at: Option<i64>,
}

fn scope_view(scope: &ClientScope) -> serde_json::Value {
    match scope {
        ClientScope::Read => serde_json::json!("read"),
        ClientScope::Control => serde_json::json!("control"),
        ClientScope::Bridge { role, attempt } => {
            serde_json::json!({"bridge": {"role": role, "attempt": attempt.to_string()}})
        }
    }
}

fn record_view(record: &central_application::ports::store::ClientRecord) -> serde_json::Value {
    serde_json::json!({
        "client_id": record.client_id,
        "name": record.name,
        "scope": scope_view(&record.scope),
        "expires_at": record.expires_at.as_ref().map(|at| at.0),
        "revoked": record.revoked,
    })
}

pub async fn list_clients(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    let auth = authenticate(&state, &headers).await?;
    auth.require_owner()?;
    let records = state
        .store
        .list_clients()
        .await
        .map_err(super::auth::app_error_response)?;
    let items: Vec<serde_json::Value> = records.iter().map(record_view).collect();
    Ok(Json(
        serde_json::json!({ "items": items, "next_cursor": None::<String> }),
    ))
}

pub async fn create_client(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<CreateClientBody>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let auth = authenticate(&state, &headers).await?;
    auth.require_owner()?;
    let expires_at = match body.expires_at {
        Some(millis) => {
            if millis <= state.clock.now().0 {
                return Err(error_response(
                    StatusCode::BAD_REQUEST,
                    "validation_failed",
                    "expires_at is in the past",
                ));
            }
            Some(central_domain::Timestamp(millis))
        }
        None => None,
    };
    let issued = match body.scope.as_str() {
        "read" => {
            issue_client(
                &*state.store,
                &*state.ids,
                &*state.clock,
                &body.name,
                ClientScope::Read,
                expires_at,
            )
            .await
        }
        "control" => {
            issue_client(
                &*state.store,
                &*state.ids,
                &*state.clock,
                &body.name,
                ClientScope::Control,
                expires_at,
            )
            .await
        }
        "bridge" => {
            let role = body.role.unwrap_or_default();
            let attempt = body.attempt.unwrap_or_default();
            let attempt = attempt.parse::<central_domain::AttemptId>().map_err(|_| {
                error_response(
                    StatusCode::BAD_REQUEST,
                    "validation_failed",
                    "bridge scope needs a valid attempt id",
                )
            })?;
            issue_bridge(
                &*state.store,
                &*state.ids,
                &body.name,
                &role,
                attempt,
                expires_at,
            )
            .await
        }
        other => {
            return Err(error_response(
                StatusCode::BAD_REQUEST,
                "validation_failed",
                format!("unknown scope {other:?}; want read, control or bridge"),
            ));
        }
    }
    .map_err(super::auth::app_error_response)?;
    // The one and only time the secret is shown. Nothing else in this
    // response (or any listing, log or error) carries it or its hash.
    Ok(Json(serde_json::json!({
        "client_id": issued.record.client_id,
        "name": issued.record.name,
        "scope": scope_view(&issued.record.scope),
        "secret": issued.secret,
        "expires_at": issued.record.expires_at.as_ref().map(|at| at.0),
    })))
}

pub async fn revoke_client(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(raw_id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let auth = authenticate(&state, &headers).await?;
    auth.require_owner()?;
    if raw_id.trim().is_empty() {
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            "client id must not be empty",
        ));
    }
    let revoked = state
        .store
        .revoke_client(&raw_id)
        .await
        .map_err(super::auth::app_error_response)?;
    if !revoked {
        return Err(error_response(
            StatusCode::NOT_FOUND,
            "not_found",
            format!("client {raw_id} not found"),
        ));
    }
    Ok(Json(
        serde_json::json!({ "client_id": raw_id, "revoked": true }),
    ))
}
