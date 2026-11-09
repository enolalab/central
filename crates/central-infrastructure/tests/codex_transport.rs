//! P2 Task 1: versioned Codex app-server stdio transport (fixture-gated).
//!
//! Live stdio `initialize` smoke waits for the P2/T2 supervisor; no concrete
//! process adapter is built here. These tests run against fixture transcripts
//! and in-memory duplex pipes only.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use central_application::contracts::{
    AppError, ProcessIdentity, ProcessSpec, ProcessState, SessionRef,
};
use central_application::ports::{AgentGateway, ProcessSupervisor};
use central_domain::{ContentHash, ProfileId, ResolvedProfile};
use central_infrastructure::codex::{
    check_model_match, check_supported_version, parse_cli_version, CodexConfig, CodexGateway,
    IncomingEvent, RpcClient, RpcError, SUPPORTED_CLI_VERSION,
};
use serde_json::{json, Value};
use tokio::io::duplex;

struct StubSupervisor;

#[async_trait]
impl ProcessSupervisor for StubSupervisor {
    async fn spawn(&self, _spec: &ProcessSpec) -> Result<ProcessIdentity, AppError> {
        unreachable!("T1 builds no process adapter; live smoke waits for T2");
    }

    async fn inspect(&self, _identity: &ProcessIdentity) -> Result<ProcessState, AppError> {
        unreachable!("T1 builds no process adapter; live smoke waits for T2");
    }

    async fn terminate(&self, _identity: &ProcessIdentity) -> Result<(), AppError> {
        unreachable!("T1 builds no process adapter; live smoke waits for T2");
    }
}

fn supervisor() -> Arc<dyn ProcessSupervisor> {
    Arc::new(StubSupervisor)
}

fn profile() -> ResolvedProfile {
    ResolvedProfile {
        id: ProfileId::from_u128(1),
        provider: "codex".to_string(),
        model: "gpt-6-luna".to_string(),
        effort: "low".to_string(),
        skills: vec![],
        subagents: vec![],
        permissions: vec![],
        fallbacks: vec![],
        hash: ContentHash::from_hex("f".repeat(64)),
    }
}

fn session_for(gateway: &CodexGateway) -> SessionRef {
    SessionRef {
        provider: "codex".to_string(),
        instance_epoch: gateway.epoch().to_string(),
        thread_id: "fixture-thread".to_string(),
    }
}

/// Trace-style fixture line: {"dir": "sent"|"received", "message": {...}}.
/// Fixtures live in the workspace `tests/fixtures/codex/` directory.
fn fixture_messages(name: &str) -> Vec<(String, Value)> {
    let path = format!(
        "{}/../../tests/fixtures/codex/{}",
        env!("CARGO_MANIFEST_DIR"),
        name
    );
    let text = std::fs::read_to_string(&path).expect("read fixture");
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let entry: Value = serde_json::from_str(line).expect("fixture line is JSON");
            (
                entry["dir"].as_str().expect("dir").to_string(),
                entry["message"].clone(),
            )
        })
        .collect()
}

/// Splits every write into small chunks so the peer must reassemble frames.
struct ChunkedWriter<S> {
    inner: S,
    chunk: usize,
}

impl<S> ChunkedWriter<S> {
    fn new(inner: S, chunk: usize) -> Self {
        Self { inner, chunk }
    }
}

impl<S: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for ChunkedWriter<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<S: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for ChunkedWriter<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let end = buf.len().min(self.chunk);
        Pin::new(&mut self.inner).poll_write(cx, &buf[..end])
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// Chunked reads plus responses arriving out of order must still correlate
/// by JSON-RPC id; interleaved server requests and notifications surface via
/// `poll_event` instead of blocking the reader.
#[tokio::test]
async fn chunked_frames_and_out_of_order_responses() {
    let entries = fixture_messages("out-of-order.jsonl");
    let mut responses: Vec<Value> = Vec::new();
    let mut server_request: Option<Value> = None;
    let mut notification: Option<Value> = None;
    for (dir, message) in &entries {
        if dir == "received" && message.get("id").is_some() && message.get("method").is_none() {
            responses.push(message.clone());
        } else if dir == "received"
            && message.get("id").is_some()
            && message.get("method").is_some()
            && server_request.is_none()
        {
            server_request = Some(message.clone());
        } else if dir == "received" && message.get("method").is_some() && notification.is_none() {
            notification = Some(message.clone());
        }
    }
    assert_eq!(responses.len(), 3);
    let peer_results = responses.clone();
    let server_request = server_request.expect("fixture holds a server request");
    let notification = notification.expect("fixture holds a notification");
    let expected_request_id = server_request["id"].clone();
    let expected_request_method = server_request["method"].as_str().unwrap().to_string();
    let expected_notification_method = notification["method"].as_str().unwrap().to_string();

    let (client_io, peer_io) = duplex(64 * 1024);
    // The peer writes in 11-byte chunks: the client must reassemble frames.
    let mut peer_reader = RpcClient::new(ChunkedWriter::new(peer_io, 11));

    let peer = tokio::spawn(async move {
        // Read the three client requests, then answer out of order.
        let mut seen: Vec<(Value, String, Value)> = Vec::new();
        for _ in 0..3 {
            match peer_reader
                .poll_event(Duration::from_secs(5))
                .await
                .expect("peer reads request")
            {
                IncomingEvent::Request { id, method, params } => {
                    seen.push((serde_json::to_value(&id).unwrap(), method, params));
                }
                other => panic!("peer expected client requests, got {other:?}"),
            }
        }
        // Deliberate out-of-order: third, first, second; server request and
        // notification interleaved between them.
        let order = [2usize, 0, 1];
        let frames = vec![
            result_frame(&seen[order[0]].0, &peer_results[2]["result"]),
            canonical(&server_request),
            result_frame(&seen[order[1]].0, &peer_results[0]["result"]),
            canonical(&notification),
            result_frame(&seen[order[2]].0, &peer_results[1]["result"]),
        ];
        for frame in frames {
            peer_reader.send_raw(&frame).await.expect("peer writes");
        }
        seen
    });

    let mut client = RpcClient::new(client_io);
    let id_a = client
        .send_request("thread/start", json!({}))
        .await
        .expect("send A");
    let id_b = client
        .send_request("turn/start", json!({}))
        .await
        .expect("send B");
    let id_c = client
        .send_request("thread/read", json!({}))
        .await
        .expect("send C");

    // Responses arrive 3rd, 1st, 2nd on the wire; correlation is by id.
    let response_for_c = client
        .recv_response(id_c, Duration::from_secs(5))
        .await
        .expect("response C");
    let response_for_a = client
        .recv_response(id_a, Duration::from_secs(5))
        .await
        .expect("response A");
    let response_for_b = client
        .recv_response(id_b, Duration::from_secs(5))
        .await
        .expect("response B");

    assert_eq!(response_for_c, responses[2]["result"]);
    assert_eq!(response_for_a, responses[0]["result"]);
    assert_eq!(response_for_b, responses[1]["result"]);

    // The interleaved server request and notification were queued, not lost.
    let mut saw_request = false;
    let mut saw_notification = false;
    for _ in 0..2 {
        match client
            .poll_event(Duration::from_secs(5))
            .await
            .expect("queued event")
        {
            IncomingEvent::Request { id, method, .. } => {
                assert_eq!(method, expected_request_method);
                assert_eq!(serde_json::to_value(&id).unwrap(), expected_request_id);
                saw_request = true;
            }
            IncomingEvent::Notification { method, .. } => {
                assert_eq!(method, expected_notification_method);
                saw_notification = true;
            }
        }
    }
    assert!(saw_request && saw_notification);

    let peer_seen = peer.await.expect("peer done");
    assert_eq!(peer_seen.len(), 3);
}

fn result_frame(id: &Value, result: &Value) -> String {
    format!("{{\"id\":{id},\"result\":{result}}}\n")
}

fn canonical(value: &Value) -> String {
    format!("{value}\n")
}

/// A CLI version outside the allowlist is an actionable capability error,
/// never silent acceptance.
#[tokio::test]
async fn unknown_version_rejected() {
    let parsed = parse_cli_version(SUPPORTED_CLI_VERSION).expect("pinned CLI version parses");
    check_supported_version(&parsed).expect("pinned CLI version is allowlisted");

    let config = CodexConfig {
        cli_version: "9.9.9-nope".to_string(),
        ..CodexConfig::default()
    };
    let gateway = CodexGateway::new(supervisor(), config);
    let err = gateway
        .capabilities(&profile())
        .await
        .expect_err("unsupported CLI version is rejected");
    match err {
        AppError::Capability(message) => {
            assert!(message.contains("9.9.9-nope"), "names observed: {message}");
            assert!(
                message.contains(SUPPORTED_CLI_VERSION),
                "names allowlist: {message}"
            );
        }
        other => panic!("expected capability error, got {other:?}"),
    }

    let unsupported = parse_cli_version("9.9.9-nope").expect("still parses");
    assert!(check_supported_version(&unsupported).is_err());

    // Requested/observed model mismatch is a capability error, not a silent
    // fallback: Central never enables provider model fallback.
    assert!(check_model_match("gpt-6-luna", "gpt-6-luna").is_ok());
    assert!(check_model_match("gpt-6-luna", "gpt-6-other").is_err());
}

/// A bounded notification buffer that overflows surfaces reconcile-required
/// evidence instead of dropping events.
#[tokio::test]
async fn bounded_buffer_requires_reconciliation() {
    let config = CodexConfig {
        notification_capacity: 2,
        ..CodexConfig::default()
    };
    let gateway = CodexGateway::new(supervisor(), config);
    let session = session_for(&gateway);

    // Fresh gateway with no traffic observes nothing.
    let empty = gateway
        .observe(&session)
        .await
        .expect("empty observe works");
    assert!(empty.is_empty());

    // Buffered events the gateway cannot attribute yet also require
    // reconciliation: Central never fabricates Observations and never drops
    // events silently. (Attempt binding arrives with the T3 inbox; the frozen
    // domain Observation struct has no ReconcileRequired variant, so the
    // reconcile-required evidence surfaces as a Transport error from the
    // frozen `observe` signature.)
    gateway.ingest_wire_frame(&notification_frame("thread/status/changed", 1));
    gateway.ingest_wire_frame(&notification_frame("thread/status/changed", 2));
    let err = gateway
        .observe(&session)
        .await
        .expect_err("unattributed events require reconciliation");
    match err {
        AppError::Transport(message) => {
            assert!(
                message.contains("reconcile-required"),
                "reconcile evidence: {message}"
            );
        }
        other => panic!("expected transport error, got {other:?}"),
    }

    // Overflow: reconcile is required, events are never silently dropped.
    gateway.ingest_wire_frame(&notification_frame("thread/status/changed", 3));
    gateway.ingest_wire_frame(&notification_frame("thread/status/changed", 4));
    gateway.ingest_wire_frame(&notification_frame("thread/status/changed", 5));
    let err = gateway
        .observe(&session)
        .await
        .expect_err("overflow requires reconciliation");
    match err {
        AppError::Transport(message) => {
            assert!(
                message.contains("reconcile-required"),
                "reconcile evidence: {message}"
            );
        }
        other => panic!("expected transport error, got {other:?}"),
    }
    assert!(gateway.reconcile_required());
}

fn notification_frame(method: &str, seq: u64) -> String {
    format!("{{\"method\":\"{method}\",\"params\":{{\"seq\":{seq}}}}}\n")
}

/// A request that times out after the write is OutcomeUnknown and is never
/// resent: the peer sees exactly one frame.
#[tokio::test]
async fn request_timeout_is_outcome_unknown_never_resends() {
    let (client_io, peer_io) = duplex(64 * 1024);
    let mut peer = RpcClient::new(peer_io);
    let peer_task = tokio::spawn(async move {
        // The client must write exactly one frame and never resend.
        let mut count = 0;
        for _ in 0..3 {
            match tokio::time::timeout(
                Duration::from_millis(500),
                peer.poll_event(Duration::from_millis(500)),
            )
            .await
            {
                Ok(Ok(IncomingEvent::Request { .. })) => count += 1,
                _ => break,
            }
        }
        count
    });

    let mut client = RpcClient::new(client_io);
    let err = client
        .request("turn/start", json!({}), Duration::from_millis(50))
        .await
        .expect_err("silent peer times out");
    assert!(
        matches!(err, RpcError::OutcomeUnknown(_)),
        "timeout is OutcomeUnknown, got {err:?}"
    );

    let seen = peer_task.await.expect("peer done");
    assert_eq!(seen, 1, "exactly one write, never resent");
}

/// Oversize frames close the transport instead of parsing partial events.
#[tokio::test]
async fn oversize_frame_closes_transport() {
    let (client_io, peer_io) = duplex(64 * 1024);
    let mut peer = RpcClient::new(peer_io);
    let pad = "p".repeat(8 * 1024 * 1024 + 16);
    let big = format!("{{\"method\":\"pad\",\"params\":{{\"pad\":\"{pad}\"}}}}\n");
    tokio::spawn(async move {
        peer.send_raw(&big).await.expect("peer writes oversize");
    });

    let mut client = RpcClient::new(client_io);
    let err = client
        .poll_event(Duration::from_secs(10))
        .await
        .expect_err("oversize rejected");
    assert!(
        matches!(err, RpcError::Oversize { .. }),
        "oversize error, got {err:?}"
    );
    assert!(client.is_closed());
    assert!(client.reconcile_required());
}
