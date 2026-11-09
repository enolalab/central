//! Loopback bearer-token auth. The actor is derived from the token
//! presented by the client — never from a request body, which may
//! claim any actor it likes and is ignored for identity.

use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use central_domain::Actor;

use super::AppState;

pub type ApiError = (StatusCode, Json<serde_json::Value>);

pub fn error_response(status: StatusCode, message: impl Into<String>) -> ApiError {
    (status, Json(serde_json::json!({ "error": message.into() })))
}

/// Resolve the authenticated actor from the `Authorization` header.
/// Missing or wrong tokens are `401`; there is no body fallback.
pub fn authenticate(state: &AppState, headers: &HeaderMap) -> Result<Actor, ApiError> {
    let presented = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    match presented {
        Some(token) if token == state.token => Ok(state.actor.clone()),
        _ => Err(error_response(
            StatusCode::UNAUTHORIZED,
            "missing or invalid bearer token",
        )),
    }
}

pub fn app_error_response(error: central_application::contracts::AppError) -> ApiError {
    use central_application::contracts::AppError;
    let status = match &error {
        AppError::Validation(_) => StatusCode::BAD_REQUEST,
        AppError::Forbidden(_) => StatusCode::FORBIDDEN,
        AppError::Conflict(_) => StatusCode::CONFLICT,
        AppError::NotFound(_) => StatusCode::NOT_FOUND,
        AppError::Storage(_) => StatusCode::INTERNAL_SERVER_ERROR,
        AppError::Capability(_) | AppError::Transport(_) => StatusCode::BAD_GATEWAY,
    };
    error_response(status, error.to_string())
}
