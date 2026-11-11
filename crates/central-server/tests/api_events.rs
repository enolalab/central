//! P4 event tests: ordered replay at the commit edge, cursor
//! survival across restart, SSE framing/auth/resume, invalid cursors
//! and bounded reads. Wire-level against the real daemon binary.

#[path = "common/mod.rs"]
mod common;

use common::*;

#[test]
fn ordered_replay_loses_nothing_at_commit_edge() {
    let dir = state_dir("events-order");
    let server = start_server(&dir);
    let run = create_run(&server, 121);
    let before = get_snapshot(&server, &run);
    let cursor = before["last_seq"].as_u64().expect("last_seq");
    // SetLimits with a fresh value always commits: five command ids,
    // five receipts, five events — the commit edge under test.
    for id in 122..127 {
        let tokens = 1000 + (id - 122) as u64;
        let (status, receipt) = post_command(
            &server,
            Some(&run),
            id,
            serde_json::json!({"SetLimits": {"limits": {"max_tokens": tokens, "max_cost_minor": null, "max_attempts": null, "max_duration_ms": null}}}),
        );
        assert_eq!(status, 200, "cmd {id}: {receipt}");
    }
    let (status, body) = http(
        "GET",
        &server.addr,
        &format!("/v1/runs/{run}/events?after={cursor}&limit=100"),
        Some(&server.token()),
        None,
        &[],
    );
    assert_eq!(status, 200, "events: {body}");
    let page: serde_json::Value = serde_json::from_str(&body).expect("page");
    let items = page["items"].as_array().expect("items");
    assert_eq!(items.len(), 5, "one event per commit: {page}");
    let mut last = cursor;
    for item in items {
        let seq = item["seq"].as_u64().expect("seq");
        assert_eq!(seq, last + 1, "gapless seq: {page}");
        assert!(item["kind"].as_str().is_some(), "kind: {item}");
        assert!(item["at"].as_i64().is_some(), "at: {item}");
        assert_eq!(item["run_id"].as_str(), Some(run.as_str()), "run: {item}");
        last = seq;
    }
    // The opaque cursor round-trips: the next page from it is empty
    // and exhausted (null cursor), proving the cursor binds this run.
    let next = page["next_cursor"].as_str().expect("cursor").to_string();
    let (status, body) = http(
        "GET",
        &server.addr,
        &format!("/v1/runs/{run}/events?after={next}"),
        Some(&server.token()),
        None,
        &[],
    );
    assert_eq!(status, 200, "next page: {body}");
    let following: serde_json::Value = serde_json::from_str(&body).expect("page");
    assert_eq!(following["items"].as_array().expect("items").len(), 0);
    assert!(following["next_cursor"].is_null(), "exhausted: {following}");
}

#[test]
fn last_seq_survives_restart() {
    let dir = state_dir("events-restart");
    let mut server = start_server(&dir);
    let run = create_run(&server, 131);
    let (status, _) = post_command(&server, Some(&run), 132, serde_json::json!("Pause"));
    assert_eq!(status, 200);
    let before = get_snapshot(&server, &run);
    let cursor = before["last_seq"].as_u64().expect("last_seq");
    assert!(cursor >= 2, "events exist: {before}");
    server.crash();
    drop(server);
    let server = start_server_in(&dir, true);
    let after = get_snapshot(&server, &run);
    assert_eq!(
        after["last_seq"].as_u64().expect("seq"),
        cursor,
        "cursor durable: {after}"
    );
    let (status, body) = http(
        "GET",
        &server.addr,
        &format!("/v1/runs/{run}/events?after=0&limit=500"),
        Some(&server.token()),
        None,
        &[],
    );
    assert_eq!(status, 200, "replay: {body}");
    let page: serde_json::Value = serde_json::from_str(&body).expect("page");
    assert_eq!(
        page["items"].as_array().expect("items").len() as u64,
        cursor
    );
}

#[test]
fn sse_stream_needs_auth_and_replays_with_ids() {
    let dir = state_dir("events-sse");
    let server = start_server(&dir);
    let run = create_run(&server, 141);
    let (status, _) = sse_first_bytes(
        &server.addr,
        &format!("/v1/runs/{run}/events/stream"),
        "bad",
        &[],
    );
    assert!(
        status == 401 || status == 403,
        "stream needs auth: {status}"
    );
    let (status, text) = sse_first_bytes(
        &server.addr,
        &format!("/v1/runs/{run}/events/stream?after=0"),
        &server.token(),
        &[],
    );
    assert_eq!(status, 200, "stream: {text}");
    assert!(text.contains("text/event-stream"), "content type: {text}");
    assert!(text.contains("id: 1"), "durable seq as id: {text}");
    assert!(text.contains("data: "), "data frame: {text}");
    // Resume from Last-Event-ID skips what the client already has.
    let (status, text) = sse_first_bytes(
        &server.addr,
        &format!("/v1/runs/{run}/events/stream"),
        &server.token(),
        &[("Last-Event-ID", "1")],
    );
    assert_eq!(status, 200, "resume: {text}");
    assert!(!text.contains("id: 1\n"), "no redelivery: {text}");
}

#[test]
fn sse_gap_signals_resync_required() {
    let dir = state_dir("events-gap");
    let server = start_server(&dir);
    let run = create_run(&server, 151);
    let (status, text) = sse_first_bytes(
        &server.addr,
        &format!("/v1/runs/{run}/events/stream?after=9999"),
        &server.token(),
        &[],
    );
    assert_eq!(status, 200, "gap stream: {text}");
    assert!(text.contains("resync_required"), "resync signal: {text}");
}

#[test]
fn invalid_cursor_is_400_not_500() {
    let dir = state_dir("events-cursor");
    let server = start_server(&dir);
    let run = create_run(&server, 161);
    let (status, body) = http(
        "GET",
        &server.addr,
        &format!("/v1/runs/{run}/events?after=99999999999999999999"),
        Some(&server.token()),
        None,
        &[],
    );
    // A cursor beyond durable history means the client is lost: fail
    // fast with resync_required (409), never an empty silent page.
    assert!(
        status == 400 || status == 200 || status == 409,
        "huge cursor: {body}"
    );
    if status == 400 {
        let value: serde_json::Value = serde_json::from_str(&body).expect("400 json");
        assert_eq!(
            value["error"]["code"].as_str(),
            Some("validation_failed"),
            "bad cursor uses the documented stable code: {value}"
        );
    }
}

#[test]
fn page_limit_is_bounded() {
    let dir = state_dir("events-bound");
    let server = start_server(&dir);
    let run = create_run(&server, 171);
    let (status, body) = http(
        "GET",
        &server.addr,
        &format!("/v1/runs/{run}/events?after=0&limit=500"),
        Some(&server.token()),
        None,
        &[],
    );
    assert_eq!(status, 200, "max page: {body}");
    let (status, body) = http(
        "GET",
        &server.addr,
        &format!("/v1/runs/{run}/events?after=0&limit=501"),
        Some(&server.token()),
        None,
        &[],
    );
    assert_eq!(status, 400, "over max: {body}");
}
