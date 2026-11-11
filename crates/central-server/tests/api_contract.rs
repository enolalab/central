//! P4 public contract tests: meta/openapi discovery, receipt replay,
//! stale-target conflicts, snapshot/cursor atomicity, restart
//! resnapshot, pagination bounds and the 404 boundary. Wire-level: the
//! real daemon binary over loopback HTTP, no crate imports.

#[path = "common/mod.rs"]
mod common;

use common::*;

// ---------- discovery ----------

#[test]
fn independent_client_contract() {
    let dir = state_dir("contract-discovery");
    let server = start_server(&dir);
    let token = server.token();

    let (status, body) = http("GET", &server.addr, "/v1/meta", Some(&token), None, &[]);
    assert_eq!(status, 200, "meta: {body}");
    let meta: serde_json::Value = serde_json::from_str(&body).expect("meta json");
    assert!(meta["version"].as_str().is_some(), "version: {meta}");
    assert!(meta["instance_epoch"].as_str().is_some(), "epoch: {meta}");
    assert!(meta["capabilities"].is_object(), "capabilities: {meta}");
    assert!(
        meta.get("secret").is_none() && meta.get("token").is_none(),
        "no secret: {meta}"
    );

    let (status, body) = http(
        "GET",
        &server.addr,
        "/v1/openapi.json",
        Some(&token),
        None,
        &[],
    );
    assert_eq!(status, 200, "openapi: {body}");
    let spec: serde_json::Value = serde_json::from_str(&body).expect("openapi json");
    assert_eq!(spec["openapi"], "3.1.2", "openapi version: {spec}");
    for path in [
        "/v1/meta",
        "/v1/commands",
        "/v1/runs",
        "/v1/runs/{id}",
        "/v1/runs/{id}/events",
        "/v1/runs/{id}/events/stream",
        "/v1/clients",
    ] {
        assert!(spec["paths"][path].is_object(), "missing path {path}");
    }
    let text = serde_json::to_string(&spec).expect("spec text");
    // The contract documents secret handling in prose, but carries no
    // credential material: no query token, no example secret, no token
    // path parameter anywhere in the served surface.
    assert!(!text.contains("?token="), "spec smuggles query tokens");
    assert!(!text.contains("{token}"), "spec has token path params");
    assert!(!text.contains("central_sk"), "spec has example secrets");
    assert!(
        !text.contains("Authorization: Bearer <"),
        "spec has token examples"
    );
}

#[test]
fn unknown_paths_are_404_json() {
    let dir = state_dir("contract-404");
    let server = start_server(&dir);
    let token = server.token();
    for path in [
        "/",
        "/ui",
        "/v1/nope",
        "/v1/runs/00000000-0000-0000-0000-000000000000/nope",
    ] {
        let (status, body) = http("GET", &server.addr, path, Some(&token), None, &[]);
        assert_eq!(status, 404, "path {path}: {body}");
        let value: serde_json::Value = serde_json::from_str(&body).expect("404 json");
        assert_eq!(value["error"]["code"], "not_found", "code: {value}");
    }
}

#[test]
fn pagination_bounds_are_400() {
    let dir = state_dir("contract-page");
    let server = start_server(&dir);
    let run = create_run(&server, 11);
    for path in [
        format!("/v1/runs/{run}/events?limit=0"),
        format!("/v1/runs/{run}/events?limit=501"),
        format!("/v1/runs/{run}/events?after=nope"),
        "/v1/runs?limit=999".to_string(),
    ] {
        let (status, body) = http("GET", &server.addr, &path, Some(&server.token()), None, &[]);
        assert_eq!(status, 400, "path {path}: {body}");
        let value: serde_json::Value = serde_json::from_str(&body).expect("400 json");
        assert!(value["error"]["code"].as_str().is_some(), "code: {value}");
    }
    let (status, body) = http(
        "GET",
        &server.addr,
        &format!("/v1/runs/{run}/events"),
        Some(&server.token()),
        None,
        &[],
    );
    assert_eq!(status, 200, "events: {body}");
    let page: serde_json::Value = serde_json::from_str(&body).expect("page json");
    assert!(page["items"].is_array(), "items: {page}");
    assert!(page.get("next_cursor").is_some(), "cursor: {page}");
}

// ---------- receipts and conflicts ----------

#[test]
fn retry_same_command_returns_same_receipt() {
    let dir = state_dir("contract-retry");
    let server = start_server(&dir);
    let run = create_run(&server, 21);
    let command = serde_json::json!("Pause");
    let (first_status, first) = post_command(&server, Some(&run), 22, command.clone());
    assert_eq!(first_status, 200, "first: {first}");
    // A retry is byte-identical (same id, same payload): the stored
    // receipt replays before decide, verdict-identical.
    let path = format!("/v1/commands?run={run}");
    let body = envelope(22, command);
    let (status, text) = http(
        "POST",
        &server.addr,
        &path,
        Some(&server.token()),
        Some(&body),
        &[],
    );
    assert_eq!(status, 200, "replay: {text}");
    let second: serde_json::Value = serde_json::from_str(&text).expect("receipt json");
    assert_eq!(first, second, "receipt replay must be identical");
}

#[test]
fn retry_changed_payload_conflicts() {
    let dir = state_dir("contract-conflict");
    let server = start_server(&dir);
    let run = create_run(&server, 31);
    let (status, first) = post_command(&server, Some(&run), 32, serde_json::json!("Pause"));
    assert_eq!(status, 200, "first: {first}");
    // Same command id, different payload: 409, never a resend.
    let sends_before = server.fake_sends();
    let path = format!("/v1/commands?run={run}");
    let body = envelope(32, serde_json::json!("Stop"));
    let (status, text) = http(
        "POST",
        &server.addr,
        &path,
        Some(&server.token()),
        Some(&body),
        &[],
    );
    assert_eq!(status, 409, "conflict: {text}");
    let value: serde_json::Value = serde_json::from_str(&text).expect("409 json");
    assert_eq!(value["error"]["code"], "command_conflict", "code: {value}");
    assert_eq!(
        server.fake_sends(),
        sends_before,
        "no agent send on conflict"
    );
}

#[test]
fn two_clients_stale_approval_conflicts() {
    let dir = state_dir("contract-stale");
    let server = start_server(&dir);
    let run = create_run(&server, 41);
    let hash = approve_fixture_plan(&server, &run, 42);
    let snapshot = get_snapshot(&server, &run);
    let active = snapshot["state"]["active_plan"].as_str().expect("active");
    assert_eq!(active, hash, "plan active: {snapshot}");
    // A second client holding the pre-approval revision retries the same
    // approval: 409 and the active plan does not move.
    let stale_revision = snapshot["state"]["revision"].as_u64().expect("rev") - 1;
    let path = format!("/v1/commands?run={run}");
    let body = envelope_rev(
        44,
        stale_revision,
        serde_json::json!({"ApprovePlan": {"plan_hash": hash}}),
    );
    let (status, text) = http(
        "POST",
        &server.addr,
        &path,
        Some(&server.token()),
        Some(&body),
        &[],
    );
    assert_eq!(status, 409, "stale approval: {text}");
    let value: serde_json::Value = serde_json::from_str(&text).expect("409 json");
    let code = value["error"]["code"].as_str().expect("code");
    assert!(
        code == "stale_revision" || code == "stale_target",
        "stale code: {value}"
    );
    let after = get_snapshot(&server, &run);
    assert_eq!(
        after["state"]["active_plan"].as_str().expect("active"),
        hash
    );
}

// ---------- snapshot / cursor ----------

#[test]
fn snapshot_cursor_has_no_commit_gap() {
    let dir = state_dir("contract-gap");
    let server = start_server(&dir);
    let run = create_run(&server, 51);
    let before = get_snapshot(&server, &run);
    let cursor = before["last_seq"].as_u64().expect("last_seq");
    let (status, receipt) = post_command(&server, Some(&run), 52, serde_json::json!("Pause"));
    assert_eq!(status, 200, "pause: {receipt}");
    let (status, body) = http(
        "GET",
        &server.addr,
        &format!("/v1/runs/{run}/events?after={cursor}"),
        Some(&server.token()),
        None,
        &[],
    );
    assert_eq!(status, 200, "events: {body}");
    let page: serde_json::Value = serde_json::from_str(&body).expect("page");
    let items = page["items"].as_array().expect("items");
    assert!(
        !items.is_empty(),
        "commit must be visible at the cursor: {page}"
    );
    let mut seqs: Vec<u64> = items
        .iter()
        .map(|item| item["seq"].as_u64().expect("seq"))
        .collect();
    assert!(seqs.iter().all(|seq| *seq > cursor), "after cursor: {page}");
    seqs.sort_unstable();
    for pair in seqs.windows(2) {
        assert!(pair[1] > pair[0], "ordered replay: {page}");
    }
    let after = get_snapshot(&server, &run);
    let last = after["last_seq"].as_u64().expect("last_seq");
    assert_eq!(
        last,
        *seqs.last().expect("last"),
        "cursor covers snapshot: {after}"
    );
}

#[test]
fn restart_requires_resnapshot_not_resume() {
    let dir = state_dir("contract-restart");
    let mut server = start_server(&dir);
    let run = create_run(&server, 61);
    let before = get_snapshot(&server, &run);
    let epoch_before = before["instance_epoch"]
        .as_str()
        .expect("epoch")
        .to_string();
    let cursor = before["last_seq"].as_u64().expect("last_seq");
    let sends_before = server.fake_sends();
    server.crash();
    drop(server);
    let server = start_server_in(&dir, true);
    let token = server.token();
    let (status, body) = http("GET", &server.addr, "/v1/meta", Some(&token), None, &[]);
    assert_eq!(status, 200, "meta: {body}");
    let meta: serde_json::Value = serde_json::from_str(&body).expect("meta");
    let epoch_after = meta["instance_epoch"].as_str().expect("epoch");
    assert_ne!(
        epoch_before, epoch_after,
        "restart rotates the instance epoch"
    );
    // The old cursor belongs to the old instance: resume-from-cursor is
    // refused with resync_required, never silently resumed.
    let (status, body) = http(
        "GET",
        &server.addr,
        &format!("/v1/runs/{run}/events?after={cursor}&known_epoch={epoch_before}"),
        Some(&token),
        None,
        &[],
    );
    assert_eq!(status, 409, "old epoch: {body}");
    let value: serde_json::Value = serde_json::from_str(&body).expect("409 json");
    assert_eq!(value["error"]["code"], "resync_required", "code: {value}");
    // The run parks for explicit resume and nothing was sent at boot.
    let snapshot = get_snapshot_as(&server.addr, &token, &run);
    assert_eq!(
        snapshot["state"]["control"], "AwaitingResume",
        "parked: {snapshot}"
    );
    assert_eq!(
        server.fake_sends(),
        sends_before,
        "zero inference across restart"
    );
    // Usage holds survive the restart (missing-usage fixture covered in
    // workflow tests); the cursor itself survives too.
    assert!(
        snapshot["last_seq"].as_u64().expect("seq") >= cursor,
        "cursor: {snapshot}"
    );
}
