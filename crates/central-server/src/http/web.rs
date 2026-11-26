//! The bundled browser client. Its assets are public and contain no run data.
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE};
use axum::response::IntoResponse;

const POLICY: &str =
    "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'";

pub async fn index() -> impl IntoResponse {
    (
        [
            (CONTENT_TYPE.as_str(), "text/html; charset=utf-8"),
            (CACHE_CONTROL.as_str(), "no-store"),
            ("content-security-policy", POLICY),
            ("referrer-policy", "no-referrer"),
            ("x-content-type-options", "nosniff"),
        ],
        include_str!("../../web/index.html"),
    )
}

fn asset(kind: &'static str, content: &'static str) -> impl IntoResponse {
    (
        [
            (CONTENT_TYPE.as_str(), kind),
            ("content-security-policy", POLICY),
            ("x-content-type-options", "nosniff"),
        ],
        content,
    )
}

pub async fn stylesheet() -> impl IntoResponse {
    asset(
        "text/css; charset=utf-8",
        include_str!("../../web/design.css"),
    )
}

pub async fn workspace_stylesheet() -> impl IntoResponse {
    asset(
        "text/css; charset=utf-8",
        include_str!("../../web/workspace.css"),
    )
}

pub async fn javascript() -> impl IntoResponse {
    asset(
        "text/javascript; charset=utf-8",
        include_str!("../../web/workspace.js"),
    )
}

pub async fn marked_javascript() -> impl IntoResponse {
    asset(
        "text/javascript; charset=utf-8",
        include_str!("../../web/vendor/marked.min.js"),
    )
}

pub async fn purify_javascript() -> impl IntoResponse {
    asset(
        "text/javascript; charset=utf-8",
        include_str!("../../web/vendor/purify.min.js"),
    )
}

pub async fn office() -> impl IntoResponse {
    asset("image/svg+xml", include_str!("../../web/office.svg"))
}
