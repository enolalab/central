//! P4 release-security tests: no UI routes, no fake runtime without
//! the test flag, owner-only shutdown, and token/Origin rules at the
//! release boundary. Wire-level against the real daemon binary.

#[path = "common/mod.rs"]
mod common;

use common::*;

#[test]
fn release_has_no_ui_or_fake_runtime() {
    let dir = state_dir("sec-release");
    let server = start_server_in(&dir, false);
    let token = server.token();
    for path in ["/", "/ui", "/ui/index.html", "/static/app.js"] {
        let (status, body) = http("GET", &server.addr, path, Some(&token), None, &[]);
        assert_eq!(status, 404, "no UI at {path}: {body}");
    }
    // Without --fake-agent the fake counters are a plain 404: a
    // release build can never serve fake agent output.
    let (status, body) = http(
        "GET",
        &server.addr,
        "/v1/test/fake-sends",
        Some(&token),
        None,
        &[],
    );
    assert_eq!(status, 404, "no fake runtime: {body}");
    // With the flag the counters exist (test builds only).
    let dir2 = state_dir("sec-fake");
    let fake = start_server_in(&dir2, true);
    let (status, _) = http(
        "GET",
        &fake.addr,
        "/v1/test/fake-sends",
        Some(&fake.token()),
        None,
        &[],
    );
    assert_eq!(status, 200, "flagged build serves counters");
}

#[test]
fn shutdown_is_owner_only() {
    let dir = state_dir("sec-shutdown");
    let server = start_server(&dir);
    let issued = issue_client(&server, "operator", "control");
    let control = client_token(&issued);
    let (status, _) = http(
        "POST",
        &server.addr,
        "/v1/shutdown",
        Some(&control),
        Some("{}"),
        &[],
    );
    assert_eq!(status, 403, "control cannot shut down");
    let (status, _) = http("POST", &server.addr, "/v1/shutdown", None, Some("{}"), &[]);
    assert_eq!(status, 401, "anonymous cannot shut down");
}

#[test]
fn healthz_stays_minimal() {
    let dir = state_dir("sec-health");
    let server = start_server(&dir);
    let (status, body) = http("GET", &server.addr, "/healthz", None, None, &[]);
    assert_eq!(status, 200, "health: {body}");
    let lowered = body.to_lowercase();
    assert!(!lowered.contains("token"), "health leaks token: {body}");
    assert!(!lowered.contains("secret"), "health leaks secret: {body}");
}
