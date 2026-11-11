//! P4 auth tests: owner issuance, read/control/bridge separation,
//! revocation (new requests and open streams), Origin allowlist and the
//! no-token-in-query rule. Secrets never appear in errors or listings.

#[path = "common/mod.rs"]
mod common;

use common::*;

#[test]
fn owner_issues_and_lists_clients_without_secrets() {
    let dir = state_dir("auth-issue");
    let server = start_server(&dir);
    let token = server.token();
    let issued = issue_client(&server, "reader", "read");
    assert!(
        issued["secret"].as_str().is_some(),
        "one-time secret: {issued}"
    );
    assert!(
        issued.get("secret_hash").is_none(),
        "no hash in answer: {issued}"
    );
    // The listing carries metadata only: no secret, no hash.
    let (status, body) = http("GET", &server.addr, "/v1/clients", Some(&token), None, &[]);
    assert_eq!(status, 200, "list: {body}");
    let text = body.to_lowercase();
    assert!(!text.contains("secret"), "listing leaks secret: {body}");
    let value: serde_json::Value = serde_json::from_str(&body).expect("list json");
    let items = value["items"].as_array().expect("items");
    assert_eq!(items.len(), 1, "one client: {value}");
    assert_eq!(items[0]["scope"], "read", "scope: {value}");
    assert_eq!(items[0]["name"], "reader", "name: {value}");
    // Non-owners cannot manage clients.
    let reader = client_token(&issued);
    let (status, body) = http(
        "POST",
        &server.addr,
        "/v1/clients",
        Some(&reader),
        Some(&serde_json::json!({"name": "x", "scope": "read"}).to_string()),
        &[],
    );
    assert_eq!(status, 403, "control mint by reader: {body}");
}

#[test]
fn read_client_cannot_mutate() {
    let dir = state_dir("auth-read");
    let server = start_server(&dir);
    let run = create_run(&server, 71);
    let issued = issue_client(&server, "reader", "read");
    let reader = client_token(&issued);
    let (status, body) = http(
        "GET",
        &server.addr,
        &format!("/v1/runs/{run}"),
        Some(&reader),
        None,
        &[],
    );
    assert_eq!(status, 200, "read own run: {body}");
    let (status, value) = post_command_as(
        &server.addr,
        &reader,
        Some(&run),
        72,
        serde_json::json!("Pause"),
    );
    assert_eq!(status, 403, "read must not write: {value}");
    assert_eq!(value["error"]["code"], "forbidden", "code: {value}");
}

#[test]
fn control_client_cannot_manage_credentials_or_daemon() {
    let dir = state_dir("auth-control");
    let server = start_server(&dir);
    let issued = issue_client(&server, "operator", "control");
    let control = client_token(&issued);
    let (status, _) = http(
        "POST",
        &server.addr,
        "/v1/clients",
        Some(&control),
        Some(&serde_json::json!({"name": "y", "scope": "read"}).to_string()),
        &[],
    );
    assert_eq!(status, 403, "control must not mint clients");
    let (status, _) = http(
        "POST",
        &server.addr,
        "/v1/shutdown",
        Some(&control),
        Some("{}"),
        &[],
    );
    assert_eq!(status, 403, "control must not shut down the daemon");
    // Owner still can.
    let (status, _) = http(
        "GET",
        &server.addr,
        "/v1/runs",
        Some(&server.token()),
        None,
        &[],
    );
    assert_eq!(status, 200, "owner reads on");
}

#[test]
fn agent_cannot_approve_plan() {
    let dir = state_dir("auth-bridge");
    let server = start_server(&dir);
    let run = create_run(&server, 81);
    let hash = approve_fixture_plan(&server, &run, 82);
    // A bridge token bound to a worker attempt may import that
    // attempt's result but may never approve plans or inbox entries.
    let (status, body) = http(
        "POST",
        &server.addr,
        "/v1/clients",
        Some(&server.token()),
        Some(
            &serde_json::json!({"name": "worker-1", "scope": "bridge", "role": "worker", "attempt": "00000000-0000-0000-0000-000000000099"})
                .to_string(),
        ),
        &[],
    );
    assert_eq!(status, 200, "issue bridge: {body}");
    let issued: serde_json::Value = serde_json::from_str(&body).expect("bridge json");
    let bridge = client_token(&issued);
    let snapshot = get_snapshot(&server, &run);
    let revision = snapshot["state"]["revision"].as_u64().expect("rev");
    let path = format!("/v1/commands?run={run}");
    let body = envelope_rev(
        84,
        revision,
        serde_json::json!({"ApprovePlan": {"plan_hash": hash}}),
    );
    let (status, text) = http("POST", &server.addr, &path, Some(&bridge), Some(&body), &[]);
    assert_eq!(status, 403, "bridge must not approve: {text}");
    let (status, text) = http(
        "POST",
        &server.addr,
        &format!("/v1/runs/{run}/inbox/approve"),
        Some(&bridge),
        Some(&serde_json::json!({"key": "codex:e:t:u:i:r", "preview_hash": "ab"}).to_string()),
        &[],
    );
    assert!(status == 403 || status == 400, "bridge inbox: {text}");
}

#[test]
fn revoked_client_cannot_reconnect() {
    let dir = state_dir("auth-revoke");
    let server = start_server(&dir);
    let run = create_run(&server, 91);
    let issued = issue_client(&server, "temp", "control");
    let id = issued["client_id"].as_str().expect("id").to_string();
    let token = client_token(&issued);
    let (status, _) = http(
        "GET",
        &server.addr,
        &format!("/v1/runs/{run}"),
        Some(&token),
        None,
        &[],
    );
    assert_eq!(status, 200, "live token reads");
    let (status, body) = http(
        "POST",
        &server.addr,
        &format!("/v1/clients/{id}/revoke"),
        Some(&server.token()),
        Some("{}"),
        &[],
    );
    assert_eq!(status, 200, "revoke: {body}");
    // New requests fail closed.
    let (status, body) = http(
        "GET",
        &server.addr,
        &format!("/v1/runs/{run}"),
        Some(&token),
        None,
        &[],
    );
    assert!(status == 401 || status == 403, "revoked: {body}");
    let value: serde_json::Value = serde_json::from_str(&body).unwrap_or(serde_json::Value::Null);
    assert!(value["error"]["code"].as_str().is_some(), "code: {body}");
    // And the secret never appears in error bodies.
    let (status, body) = http(
        "GET",
        &server.addr,
        &format!("/v1/runs/{run}"),
        Some("bogus.secret"),
        None,
        &[],
    );
    assert!(status == 401 || status == 403, "bogus: {body}");
    assert!(!body.contains(&token), "error leaks presented secret");
}

#[test]
fn revoke_closes_open_streams() {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::time::Duration;

    let dir = state_dir("auth-stream-revoke");
    let server = start_server(&dir);
    let run = create_run(&server, 95);
    let issued = issue_client(&server, "watcher", "read");
    let id = issued["client_id"].as_str().expect("id").to_string();
    let token = client_token(&issued);

    let mut stream = TcpStream::connect(&server.addr).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(8)))
        .expect("timeout");
    let request = format!(
        "GET /v1/runs/{run}/events/stream?after=0 HTTP/1.1\r\nhost: {}\r\nconnection: close\r\nauthorization: Bearer {token}\r\naccept: text/event-stream\r\n\r\n",
        server.addr
    );
    stream.write_all(request.as_bytes()).expect("write");
    // The replayed frames prove the stream is open.
    let mut first = vec![0u8; 4096];
    let n = stream.read(&mut first).expect("first read");
    assert!(n > 0, "stream opens");
    // Revoke while the stream is open: the pump re-checks every tick
    // and closes instead of serving a dead credential.
    let (status, _) = http(
        "POST",
        &server.addr,
        &format!("/v1/clients/{id}/revoke"),
        Some(&server.token()),
        Some("{}"),
        &[],
    );
    assert_eq!(status, 200, "revoke lands");
    // The pump re-checks every second and closes on revoke: EOF must
    // arrive well before the 8 s read timeout (the 15 s keepalive
    // cannot fire first, so an early return proves the close).
    let start = std::time::Instant::now();
    let mut rest = Vec::new();
    let _ = stream.read_to_end(&mut rest);
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(7),
        "stream stayed open after revoke ({elapsed:?})"
    );
}

#[test]
fn untrusted_origin_rejected() {
    let dir = state_dir("auth-origin");
    std::fs::write(
        dir.join("origins.json"),
        r#"{"origins": ["https://app.example"]}"#,
    )
    .expect("origins file");
    let server = start_server(&dir);
    let token = server.token();
    let run = create_run(&server, 101);
    // Browser request from an untrusted origin: 403 on read and stream.
    let (status, _) = http(
        "GET",
        &server.addr,
        &format!("/v1/runs/{run}"),
        Some(&token),
        None,
        &[("Origin", "https://evil.example")],
    );
    assert_eq!(status, 403, "untrusted origin reads");
    let (status, _) = sse_first_bytes(
        &server.addr,
        &format!("/v1/runs/{run}/events/stream"),
        &token,
        &[("Origin", "https://evil.example")],
    );
    assert_eq!(status, 403, "untrusted origin streams");
    // Allowlisted origin passes; non-browser requests (no Origin) pass.
    let (status, _) = http(
        "GET",
        &server.addr,
        &format!("/v1/runs/{run}"),
        Some(&token),
        None,
        &[("Origin", "https://app.example")],
    );
    assert_eq!(status, 200, "allowlisted origin reads");
    let (status, _) = http(
        "GET",
        &server.addr,
        &format!("/v1/runs/{run}"),
        Some(&token),
        None,
        &[],
    );
    assert_eq!(status, 200, "bearer without origin reads");
    // Preflight echoes only the allowlisted origin, never *.
    let (status, head, _) = http_full(
        "OPTIONS",
        &server.addr,
        &format!("/v1/runs/{run}"),
        None,
        None,
        &[
            ("Origin", "https://app.example"),
            ("Access-Control-Request-Method", "GET"),
        ],
    );
    assert_eq!(status, 204, "preflight: {head}");
    assert!(head.contains("https://app.example"), "echo origin: {head}");
    assert!(
        !head.contains("Access-Control-Allow-Origin: *"),
        "no wildcard: {head}"
    );
}

#[test]
fn no_token_in_query_or_missing_auth() {
    let dir = state_dir("auth-query");
    let server = start_server(&dir);
    let token = server.token();
    let run = create_run(&server, 111);
    // A token smuggled as a query parameter is ignored: still 401.
    let (status, body) = http(
        "GET",
        &server.addr,
        &format!("/v1/runs/{run}?token={token}"),
        None,
        None,
        &[],
    );
    assert_eq!(status, 401, "query token ignored: {body}");
    let value: serde_json::Value = serde_json::from_str(&body).expect("401 json");
    assert_eq!(value["error"]["code"], "auth_required", "code: {value}");
    // No auth at all: 401 with a stable code, on reads and streams.
    let (status, _) = http(
        "GET",
        &server.addr,
        &format!("/v1/runs/{run}"),
        None,
        None,
        &[],
    );
    assert_eq!(status, 401, "anonymous read");
    let (status, _) = sse_first_bytes(
        &server.addr,
        &format!("/v1/runs/{run}/events/stream"),
        "bad",
        &[],
    );
    assert!(status == 401 || status == 403, "anonymous stream: {status}");
}
