//! Discovery: `GET /v1/meta` and `GET /v1/openapi.json`.
//!
//! Meta reports the API version, this boot's instance epoch, and the
//! real capabilities (fake agent flag, gateway provider, stored
//! profiles) — never a capability the harness was not verified to
//! support, and never a secret. The OpenAPI document is the shipped
//! contract mirror; tests assert its paths match the router.

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::Json;

use super::auth::authenticate;
use super::AppState;
use crate::api::errors::{error_response, ApiError};

/// Machine-readable OpenAPI 3.1.2 mirror of the shipped contract.
/// Kept as JSON next to `contracts/openapi-v1.yaml`; the contract
/// tests assert every served route is described here.
const OPENAPI_JSON: &str = include_str!("../../../../contracts/openapi-v1.json");

pub async fn get_meta(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    authenticate(&state, &headers).await?;
    let profiles = state.store.list_profiles().await.unwrap_or_default();
    Ok(Json(serde_json::json!({
        "version": "1.0.0",
        "api": "v1",
        "instance_epoch": state.instance_epoch,
        "capabilities": {
            "fake_agent": state.fake_agent_enabled,
            "profiles": profiles.len(),
            "streaming": true,
            "pagination": {"default_limit": 100, "max_limit": 500},
        },
    })))
}

pub async fn get_openapi(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    authenticate(&state, &headers).await?;
    serde_json::from_str(OPENAPI_JSON)
        .map(Json)
        .map_err(|error| {
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                format!("shipped OpenAPI contract is corrupt: {error}"),
            )
        })
}
