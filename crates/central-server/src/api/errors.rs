//! Stable v1 error envelope (`ApiErrorV1`):
//! `{ "error": { "code", "message", "details" } }`.
//!
//! Clients match on `code`, never on the human `message`. Codes are
//! stable: `validation_failed`, `auth_required`, `forbidden`,
//! `not_found`, `stale_revision`, `stale_target`, `command_conflict`,
//! `conflict`, `resync_required`, `capability_unavailable`,
//! `transport_error`, `internal_error`. Error bodies never carry
//! secrets: messages are fixed strings plus ids, never tokens.

use axum::http::StatusCode;
use axum::Json;

pub type ApiError = (StatusCode, Json<serde_json::Value>);

pub fn error_response(status: StatusCode, code: &str, message: impl Into<String>) -> ApiError {
    error_response_details(status, code, message, serde_json::json!({}))
}

pub fn error_response_details(
    status: StatusCode,
    code: &str,
    message: impl Into<String>,
    details: serde_json::Value,
) -> ApiError {
    (
        status,
        Json(serde_json::json!({
            "error": {
                "code": code,
                "message": message.into(),
                "details": details,
            }
        })),
    )
}

/// Map an application error onto a status + stable code. The code comes
/// from [`AppError::code`](central_application::contracts::AppError::code);
/// the message stays human-readable and secret-free.
pub fn app_error_response(error: central_application::contracts::AppError) -> ApiError {
    use central_application::contracts::AppError;
    let status = match &error {
        AppError::Validation(_) => StatusCode::BAD_REQUEST,
        AppError::Forbidden(_) => StatusCode::FORBIDDEN,
        AppError::Conflict(_) => StatusCode::CONFLICT,
        AppError::NotFound(_) => StatusCode::NOT_FOUND,
        AppError::Storage(_) => StatusCode::INTERNAL_SERVER_ERROR,
        AppError::Capability(_) | AppError::Transport(_) => StatusCode::SERVICE_UNAVAILABLE,
    };
    let code = error.code().to_string();
    error_response(status, &code, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use central_application::contracts::AppError;

    #[test]
    fn conflict_codes_are_stable() {
        let cases = [
            (
                AppError::Conflict("expected revision 3 but run is at revision 4".to_string()),
                StatusCode::CONFLICT,
                "stale_revision",
            ),
            (
                AppError::Conflict("target plan hash does not match the pending plan".to_string()),
                StatusCode::CONFLICT,
                "stale_target",
            ),
            (
                AppError::Conflict("command id already used with a different payload".to_string()),
                StatusCode::CONFLICT,
                "command_conflict",
            ),
            (
                AppError::Validation("x".to_string()),
                StatusCode::BAD_REQUEST,
                "validation_failed",
            ),
            (
                AppError::Capability("no gateway".to_string()),
                StatusCode::SERVICE_UNAVAILABLE,
                "capability_unavailable",
            ),
        ];
        for (error, status, code) in cases {
            let (got_status, Json(body)) = app_error_response(error);
            assert_eq!(got_status, status);
            assert_eq!(body["error"]["code"], code, "body: {body}");
            assert!(body["error"]["message"].as_str().is_some());
            assert!(body["error"]["details"].is_object());
        }
    }
}
