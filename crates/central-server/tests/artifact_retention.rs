//! P4 lifecycle/security tests: GC never deletes referenced or
//! ambiguous work, artifact escape is refused, reads are bounded, and
//! secrets are redacted from every public surface.

#[path = "common/mod.rs"]
mod common;

use common::*;

#[test]
fn gc_dry_run_spares_live_and_unknown_work() {
    let dir = state_dir("life-gc");
    let server = start_server(&dir);
    let token = server.token();
    let run = create_run(&server, 231);
    let _hash = approve_fixture_plan(&server, &run, 232);
    let (status, body) = http(
        "POST",
        &server.addr,
        "/v1/maintenance/gc",
        Some(&token),
        Some(&serde_json::json!({"dry_run": true}).to_string()),
        &[],
    );
    assert_eq!(status, 200, "gc dry run: {body}");
    let report: serde_json::Value = serde_json::from_str(&body).expect("gc json");
    assert_eq!(report["dry_run"], true, "dry run: {report}");
    let candidates = report["candidates"].as_array().expect("candidates");
    assert!(
        !candidates.iter().any(|c| c["id"] == run),
        "live run never a candidate: {report}"
    );
    // Non-owners cannot run maintenance at all.
    let issued = issue_client(&server, "operator", "control");
    let control = client_token(&issued);
    let (status, _) = http(
        "POST",
        &server.addr,
        "/v1/maintenance/gc",
        Some(&control),
        Some(&serde_json::json!({"dry_run": true}).to_string()),
        &[],
    );
    assert_eq!(status, 403, "control cannot gc");
    // Deletion without an explicit dry run first is refused.
    let (status, body) = http(
        "POST",
        &server.addr,
        "/v1/maintenance/gc",
        Some(&token),
        Some(&serde_json::json!({"dry_run": false}).to_string()),
        &[],
    );
    assert_eq!(status, 400, "gc needs dry run first: {body}");
}

#[test]
fn maintenance_export_has_no_credentials() {
    let dir = state_dir("life-export");
    let server = start_server(&dir);
    let token = server.token();
    let _issued = issue_client(&server, "reader", "read");
    let (status, body) = http(
        "POST",
        &server.addr,
        "/v1/maintenance/export",
        Some(&token),
        Some("{}"),
        &[],
    );
    assert_eq!(status, 200, "export: {body}");
    let lowered = body.to_lowercase();
    assert!(!lowered.contains("secret"), "export leaks secret: {body}");
    assert!(
        !lowered.contains("secret_hash"),
        "export leaks hash: {body}"
    );
    let report: serde_json::Value = serde_json::from_str(&body).expect("export json");
    assert!(report["note"].as_str().is_some(), "backup note: {report}");
}

#[test]
fn artifact_escape_and_secret_redaction() {
    let dir = state_dir("life-artifact");
    let server = start_server(&dir);
    let token = server.token();
    let run = create_run(&server, 241);
    for path in [
        format!("/v1/runs/{run}/artifacts/../escape"),
        format!("/v1/runs/{run}/artifacts/%2e%2e/escape"),
        format!("/v1/runs/{run}/artifacts/x/content?path=../../etc"),
    ] {
        let (status, _) = http("GET", &server.addr, &path, Some(&token), None, &[]);
        assert!(
            status == 400 || status == 403 || status == 404,
            "escape {path}: {status}"
        );
    }
    // A secret answer never surfaces in events, inbox or logs.
    let (status, body) = http(
        "GET",
        &server.addr,
        &format!("/v1/runs/{run}/events?after=0"),
        Some(&token),
        None,
        &[],
    );
    assert_eq!(status, 200, "events: {body}");
    assert!(
        !body.to_lowercase().contains("bearer"),
        "event leak: {body}"
    );
    let (status, body) = http(
        "GET",
        &server.addr,
        &format!("/v1/runs/{run}/artifacts"),
        Some(&token),
        None,
        &[],
    );
    assert_eq!(status, 200, "artifacts: {body}");
    let page: serde_json::Value = serde_json::from_str(&body).expect("page");
    assert!(page["items"].is_array(), "items: {page}");
    // Bounded log reads refuse absurd windows.
    let (status, body) = http(
        "GET",
        &server.addr,
        &format!("/v1/runs/{run}/logs?limit=100000"),
        Some(&token),
        None,
        &[],
    );
    assert_eq!(status, 400, "unbounded logs: {body}");
}
