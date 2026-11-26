//! Release-security tests: bundled web workspace, no fake runtime without
//! the test flag, owner-only shutdown, and token/Origin rules at the
//! release boundary. Wire-level against the real daemon binary.

#[path = "common/mod.rs"]
mod common;

use common::*;

#[test]
fn provider_settings_are_owner_only_and_preserve_existing_configuration() {
    let dir = state_dir("sec-provider-settings");
    let path = dir.join("muse.json");
    let original = serde_json::json!({
        "binary": env!("CARGO_BIN_EXE_central-server"),
        "cli_version": "1.4.25",
        "surveyed_models": ["original"],
        "serve_args": ["serve"],
    });
    std::fs::write(&path, original.to_string()).expect("write fixture config");
    std::fs::write(
        dir.join("model-catalog.json"),
        r#"{"muse":["original","catalog-only"],"codex":["listed-but-not-configured"]}"#,
    )
    .expect("write catalog fixture");
    let server = start_server_in(&dir, false);
    let owner = server.token();
    let controller = client_token(&issue_client(&server, "controller", "control"));
    let endpoint = "/app/provider-settings";
    let (status, _) = http("GET", &server.addr, "/app/models", None, None, &[]);
    assert_eq!(status, 401);
    let live_models = || {
        let (status, body) = http(
            "GET",
            &server.addr,
            "/app/models",
            Some(&controller),
            None,
            &[],
        );
        assert_eq!(status, 200, "{body}");
        serde_json::from_str::<serde_json::Value>(&body).unwrap()
    };
    let active = live_models();
    assert_eq!(
        active["items"],
        serde_json::json!([
            { "provider": "muse", "models": ["catalog-only", "original"], "discovered": ["catalog-only", "original"] },
            { "provider": "codex", "models": [], "discovered": ["listed-but-not-configured"] },
        ])
    );
    let (status, body) = http("GET", &server.addr, endpoint, Some(&owner), None, &[]);
    assert_eq!(status, 200, "{body}");
    let list: serde_json::Value = serde_json::from_str(&body).expect("settings JSON");
    let muse = list["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["provider"] == "muse")
        .unwrap();
    let mut update = serde_json::json!({
        "provider": "muse",
        "cli_version": "1.4.25",
        "models": ["model-a", "model-b", "model-a"],
        "enable_catalog_models": false,
        "revision": muse["revision"],
    });
    let post = |token: &str, payload: &serde_json::Value| {
        http(
            "POST",
            &server.addr,
            endpoint,
            Some(token),
            Some(&payload.to_string()),
            &[],
        )
    };
    assert_eq!(post(&controller, &update).0, 403);
    update["cli_version"] = serde_json::json!("999.0.0");
    assert_eq!(post(&owner, &update).0, 400);
    update["cli_version"] = serde_json::json!("1.4.25");
    let (status, response) = post(&owner, &update);
    assert_eq!(status, 200, "{response}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&response).unwrap()["restart_required"],
        true
    );
    let current: serde_json::Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(current["binary"], original["binary"]);
    assert_eq!(current["serve_args"], original["serve_args"]);
    assert_eq!(current["enable_catalog_models"], false);
    assert_eq!(
        current["surveyed_models"],
        serde_json::json!(["model-a", "model-b"])
    );
    assert_eq!(post(&owner, &update).0, 409);
    assert_eq!(live_models(), active, "staged settings require a restart");
    drop(server);
    let restarted = start_server_in(&dir, false);
    let (status, body) = http(
        "GET",
        &restarted.addr,
        "/app/models",
        Some(&restarted.token()),
        None,
        &[],
    );
    assert_eq!(status, 200, "{body}");
    let catalog: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        catalog["items"][0]["models"],
        serde_json::json!(["model-a", "model-b"])
    );
    assert_eq!(
        catalog["items"][0]["discovered"],
        serde_json::json!(["catalog-only", "original"])
    );
}

#[test]
fn release_serves_workspace_but_keeps_api_authenticated() {
    let dir = state_dir("sec-release");
    let server = start_server_in(&dir, false);
    let token = server.token();
    for path in ["/ui", "/ui/index.html", "/static/app.js"] {
        let (status, body) = http("GET", &server.addr, path, Some(&token), None, &[]);
        assert_eq!(status, 404, "unregistered path {path}: {body}");
    }
    let (status, body) = http("GET", &server.addr, "/app", None, None, &[]);
    assert_eq!(status, 200, "workspace entry point: {body}");
    assert!(body.contains("/app/workspace.js"));
    let origin = format!("http://{}", server.addr);
    let headers = [("Origin", origin.as_str())];
    let (status, _) = http("GET", &server.addr, "/v1/runs", None, None, &headers);
    assert_eq!(
        status, 401,
        "the workspace origin does not grant API access"
    );
    let (status, _) = http(
        "GET",
        &server.addr,
        "/v1/runs",
        Some(&token),
        None,
        &headers,
    );
    assert_eq!(status, 200, "the workspace can use its bearer credential");
    // Preserve the legacy API root response for existing clients.
    let (status, body) = http("GET", &server.addr, "/", Some(&token), None, &[]);
    assert_eq!(status, 503, "root {body}");
    let value: serde_json::Value = serde_json::from_str(&body).expect("503 json");
    assert_eq!(
        value["error"]["code"], "capability_unavailable",
        "code: {value}"
    );
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
fn vendor_markdown_assets_are_served_with_csp_and_referenced_by_index() {
    let dir = state_dir("sec-vendor-markdown");
    let server = start_server_in(&dir, false);
    let (status, _, index) = http_full("GET", &server.addr, "/app", None, None, &[]);
    assert_eq!(status, 200, "workspace entry point: {index}");
    let marked_tag = "<script src=\"/app/vendor/marked.js\" defer></script>";
    let purify_tag = "<script src=\"/app/vendor/purify.js\" defer></script>";
    let workspace_tag = "<script src=\"/app/workspace.js\" defer></script>";
    let marked_pos = index
        .find(marked_tag)
        .expect("index references marked vendor route");
    let purify_pos = index
        .find(purify_tag)
        .expect("index references purify vendor route");
    let workspace_pos = index
        .find(workspace_tag)
        .expect("index references workspace script");
    assert!(
        marked_pos < workspace_pos,
        "marked must load before workspace.js"
    );
    assert!(
        purify_pos < workspace_pos,
        "purify must load before workspace.js"
    );
    for (path, marker) in [
        ("/app/vendor/marked.js", "marked"),
        ("/app/vendor/purify.js", "DOMPurify"),
    ] {
        let (status, head, body) = http_full("GET", &server.addr, path, None, None, &[]);
        assert_eq!(status, 200, "vendor asset {path}: {body}");
        let head = head.to_ascii_lowercase();
        assert!(
            head.contains("content-type: text/javascript"),
            "vendor asset {path} content-type: {head}"
        );
        assert!(
            head.contains("content-security-policy:"),
            "vendor asset {path} CSP header: {head}"
        );
        assert!(
            head.contains("script-src 'self'"),
            "vendor asset {path} CSP keeps script-src 'self': {head}"
        );
        assert!(
            head.contains("x-content-type-options: nosniff"),
            "vendor asset {path} nosniff: {head}"
        );
        assert!(body.len() > 1000, "vendor asset {path} body too small");
        assert!(
            body.contains(marker),
            "vendor asset {path} should contain {marker}"
        );
    }
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
