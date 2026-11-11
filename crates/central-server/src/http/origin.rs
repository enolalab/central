//! Browser-origin enforcement (P4).
//!
//! The `Origin` allowlist is owner configuration (`origins.json` in the
//! state directory), never authentication: every request still needs
//! its bearer token, including SSE. Requests without an `Origin`
//! header (CLI, scripts, independent clients) are unaffected. A
//! present but unlisted origin is 403 `forbidden` on every method,
//! including reads and streams. Preflight (`OPTIONS`) answers 204 with
//! the echoed origin for allowlisted callers — never `*`, never
//! credentials for strangers. Tokens in the query string are ignored
//! everywhere by construction (auth only reads the header).

use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, Method, Request, StatusCode};
use axum::middleware::Next;
use axum::response::Response;

#[derive(Clone, Debug, Default)]
pub struct OriginConfig {
    pub allowed: Vec<String>,
}

impl OriginConfig {
    /// Load the owner allowlist. A missing or unreadable file means an
    /// empty allowlist (browsers denied, bearer clients unaffected) —
    /// never an open allowlist.
    pub fn load(state_dir: &std::path::Path) -> Self {
        let path = state_dir.join("origins.json");
        let text = std::fs::read_to_string(&path).unwrap_or_default();
        let value: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
        let allowed = value["origins"]
            .as_array()
            .map(|origins| {
                origins
                    .iter()
                    .filter_map(|origin| origin.as_str().map(|origin| origin.trim().to_string()))
                    .filter(|origin| !origin.is_empty())
                    .collect()
            })
            .unwrap_or_default();
        Self { allowed }
    }

    pub fn allows(&self, origin: &str) -> bool {
        self.allowed.iter().any(|listed| listed == origin)
    }
}

fn origin_of(headers: &HeaderMap) -> Option<String> {
    headers
        .get(axum::http::header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .map(|origin| origin.trim().to_string())
}

fn forbidden_body() -> Response {
    let body = serde_json::json!({
        "error": {
            "code": "forbidden",
            "message": "origin is not allowlisted by the daemon owner",
            "details": {},
        }
    });
    Response::builder()
        .status(StatusCode::FORBIDDEN)
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .expect("forbidden response builds")
}

/// Enforce the allowlist and stamp CORS headers on allowlisted
/// browser responses. Runs before auth: an unlisted origin learns
/// nothing, not even whether the token was valid.
pub async fn enforce_state(
    axum::extract::State(state): axum::extract::State<super::AppState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let config = OriginConfig {
        allowed: state.origins.clone(),
    };
    enforce_inner(&config, request, next).await
}

async fn enforce_inner(config: &OriginConfig, request: Request<Body>, next: Next) -> Response {
    let Some(origin) = origin_of(request.headers()) else {
        return next.run(request).await;
    };
    if !config.allows(&origin) {
        return forbidden_body();
    }
    if request.method() == Method::OPTIONS {
        return Response::builder()
            .status(StatusCode::NO_CONTENT)
            .header(axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN, &origin)
            .header(axum::http::header::VARY, "Origin")
            .header(
                axum::http::header::ACCESS_CONTROL_ALLOW_METHODS,
                "GET, POST, OPTIONS",
            )
            .header(
                axum::http::header::ACCESS_CONTROL_ALLOW_HEADERS,
                "Authorization, Content-Type, Last-Event-ID",
            )
            .header(axum::http::header::ACCESS_CONTROL_MAX_AGE, "600")
            .body(Body::empty())
            .expect("preflight response builds");
    }
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    if let Ok(value) = HeaderValue::from_str(&origin) {
        headers.insert(axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN, value);
    }
    if let Ok(value) = HeaderValue::from_str("Origin") {
        headers.append(axum::http::header::VARY, value);
    }
    response
}
