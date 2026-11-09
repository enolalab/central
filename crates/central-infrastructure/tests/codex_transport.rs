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
use central_infrastructure::codex::wire::{parse_frame, IncomingFrame};
use central_infrastructure::codex::{
    check_model_match, check_supported_version, parse_cli_version, CodexConfig, CodexGateway,
    IncomingEvent, RpcClient, RpcError, RpcId, SUPPORTED_CLI_VERSION,
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

/// initialize.jsonl replays through parse_frame: the initialize answer is a
/// Response echoing the CLI version, the sent `initialized` frame is a
/// Notification, and the status broadcast is a Notification.
#[test]
fn initialize_fixture_classifies_handshake_frames() {
    let entries = fixture_messages("initialize.jsonl");
    assert_eq!(entries.len(), 4, "handshake transcript has 4 frames");

    // Sent `initialize` request (id + method) classifies as a server-bound
    // request shape.
    let (dir, message) = &entries[0];
    assert_eq!(dir, "sent");
    match parse_frame(&canonical(message)).expect("initialize parses") {
        IncomingFrame::ServerRequest { id, method, .. } => {
            assert_eq!(id, RpcId::Num(100));
            assert_eq!(method, "initialize");
        }
        other => panic!("initialize must be a request, got {other:?}"),
    }

    // Received initialize answer: a Response whose user agent echoes the
    // allowlisted CLI version.
    let (dir, message) = &entries[1];
    assert_eq!(dir, "received");
    match parse_frame(&canonical(message)).expect("initialize answer parses") {
        IncomingFrame::Response { id, result } => {
            assert_eq!(id, RpcId::Num(100));
            let result = result.expect("initialize succeeds");
            let agent = result["userAgent"].as_str().expect("user agent echo");
            assert!(
                agent.contains(SUPPORTED_CLI_VERSION),
                "CLI version echo: {agent}"
            );
        }
        other => panic!("initialize answer must be a response, got {other:?}"),
    }

    // Sent `initialized` (method only) is a one-way notification.
    let (dir, message) = &entries[2];
    assert_eq!(dir, "sent");
    match parse_frame(&canonical(message)).expect("initialized parses") {
        IncomingFrame::Notification { method, .. } => {
            assert_eq!(method, "initialized");
        }
        other => panic!("initialized must be a notification, got {other:?}"),
    }

    // Received status broadcast is a one-way notification.
    let (dir, message) = &entries[3];
    assert_eq!(dir, "received");
    match parse_frame(&canonical(message)).expect("status broadcast parses") {
        IncomingFrame::Notification { method, .. } => {
            assert_eq!(method, "remoteControl/status/changed");
        }
        other => panic!("status broadcast must be a notification, got {other:?}"),
    }
}

/// chunked.jsonl delivered in small fragments still reassembles: responses
/// correlate by id and the interleaved request/notification frames surface
/// in wire order with byte-identical payloads.
#[tokio::test]
async fn chunked_fixture_survives_fragmented_delivery() {
    let entries = fixture_messages("chunked.jsonl");
    let received: Vec<Value> = entries
        .iter()
        .filter(|(dir, _)| *dir == "received")
        .map(|(_, message)| message.clone())
        .collect();
    assert_eq!(received.len(), 6, "chunked transcript has 6 inbound frames");

    let (client_io, peer_io) = duplex(64 * 1024);
    // 7-byte writes: the client must reassemble every frame.
    let mut peer = RpcClient::new(ChunkedWriter::new(peer_io, 7));
    let peer_frames = received.clone();
    let peer_task = tokio::spawn(async move {
        for message in &peer_frames {
            peer.send_raw(&canonical(message))
                .await
                .expect("peer writes");
        }
    });

    let mut client = RpcClient::new(client_io);
    let thread = client
        .recv_response(101, Duration::from_secs(5))
        .await
        .expect("thread/start answer");
    assert_eq!(
        thread["thread"]["id"],
        received[0]["result"]["thread"]["id"]
    );
    assert_eq!(
        thread["thread"]["cliVersion"],
        json!(SUPPORTED_CLI_VERSION),
        "thread/start echoes the CLI version"
    );
    let turn = client
        .recv_response(102, Duration::from_secs(5))
        .await
        .expect("turn/start answer");
    assert_eq!(turn, received[2]["result"]);

    let expected = [
        "thread/started",
        "thread/status/changed",
        "item/commandExecution/requestApproval",
        "serverRequest/resolved",
    ];
    for method in expected {
        match client
            .poll_event(Duration::from_secs(5))
            .await
            .expect("queued event")
        {
            IncomingEvent::Notification {
                method: got,
                params,
            } => {
                assert_eq!(got, method);
                let want = received
                    .iter()
                    .find(|m| m.get("method").and_then(Value::as_str) == Some(method))
                    .expect("fixture holds event");
                assert_eq!(&params, &want["params"], "payload intact for {method}");
            }
            IncomingEvent::Request {
                id,
                method: got,
                params,
            } => {
                assert_eq!(got, method);
                assert_eq!(id, RpcId::Num(0));
                let want = received
                    .iter()
                    .find(|m| m.get("method").and_then(Value::as_str) == Some(method))
                    .expect("fixture holds event");
                assert_eq!(&params, &want["params"], "payload intact for {method}");
            }
        }
    }

    peer_task.await.expect("peer done");
}

/// A prose server error carrying a secret reaches the caller redacted: the
/// Rejected message keeps the code but not the credential.
#[tokio::test]
async fn rejected_server_error_is_redacted_end_to_end() {
    let token = "sk-live-e2eFAIL1234567890abcdef1234567890";
    let (client_io, peer_io) = duplex(64 * 1024);
    let mut peer = RpcClient::new(peer_io);
    let peer_task = tokio::spawn(async move {
        match peer
            .poll_event(Duration::from_secs(5))
            .await
            .expect("peer reads request")
        {
            IncomingEvent::Request { id, .. } => {
                let line = format!(
                    "{{\"id\":{id},\"error\":{{\"code\":-32001,\"message\":\"invalid token {token}\"}}}}\n"
                );
                peer.send_raw(&line).await.expect("peer answers");
            }
            other => panic!("peer expected a client request, got {other:?}"),
        }
    });

    let mut client = RpcClient::new(client_io);
    let err = client
        .request("thread/start", json!({}), Duration::from_secs(5))
        .await
        .expect_err("server rejects the request");
    match err {
        RpcError::Rejected(message) => {
            assert!(!message.contains(token), "secret leaked: {message}");
            assert!(message.contains("[redacted"), "secret redacted: {message}");
            assert!(message.contains("-32001"), "code preserved: {message}");
        }
        other => panic!("expected rejection, got {other:?}"),
    }
    peer_task.await.expect("peer done");
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

/// T3 carry-over: buffered events bound to a live session drain into
/// Observations instead of erroring; only overflow/closed (and sessions no
/// live attempt owns) stay reconcile-required Transport errors. The request
/// table behind the drain routes exactly one answer per request.
#[tokio::test]
async fn buffered_events_drain_to_bound_observations() {
    use central_application::contracts::{ExternalAnswer, OpenSession};
    use central_domain::{AnswerPayload, AnswerScope, AttemptId, Timestamp};

    struct OpeningSupervisor;

    #[async_trait]
    impl ProcessSupervisor for OpeningSupervisor {
        async fn spawn(&self, _spec: &ProcessSpec) -> Result<ProcessIdentity, AppError> {
            Ok(ProcessIdentity {
                pid: 4242,
                created_at: Timestamp(1_700_000_000_000),
                ownership_id: "test-job".to_string(),
            })
        }

        async fn inspect(&self, _identity: &ProcessIdentity) -> Result<ProcessState, AppError> {
            Ok(ProcessState::Running)
        }

        async fn terminate(&self, _identity: &ProcessIdentity) -> Result<(), AppError> {
            Ok(())
        }
    }

    let gateway = CodexGateway::new(Arc::new(OpeningSupervisor), CodexConfig::default());
    let session = gateway
        .open(OpenSession {
            attempt_id: AttemptId::from_u128(42),
            profile: profile(),
            workspace: "C:\\fixture\\workspace".to_string(),
            resume_from: None,
        })
        .await
        .expect("open registers the attempt binding");

    let approval = format!(
        "{{\"id\":0,\"method\":\"item/commandExecution/requestApproval\",\"params\":{{\"kind\":\"command\",\"threadId\":\"{}\",\"turnId\":\"turn-1\",\"itemId\":\"exec-1\",\"command\":\"pwsh -NoProfile -Command demo\",\"availableDecisions\":[\"accept\",\"cancel\"]}}}}\n",
        session.thread_id,
    );
    gateway.ingest_wire_frame(&approval);

    // Bound events drain to attempt-scoped Observations: no Transport error
    // merely because the buffer is non-empty.
    let observations = gateway.observe(&session).await.expect("bound events drain");
    assert_eq!(observations.len(), 1);
    assert_eq!(observations[0].attempt_id, AttemptId::from_u128(42));
    assert_eq!(observations[0].provider_epoch, gateway.epoch());
    assert!(
        observations[0].dedup_key.contains("exec-1"),
        "dedup names the item: {}",
        observations[0].dedup_key
    );

    // The same wire request feeds the durable inbox exactly once, bound to
    // the owning attempt; a replay is not a second entry.
    let drained = gateway
        .drain_requests(&session)
        .await
        .expect("drain persists once");
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].attempt_id, Some(AttemptId::from_u128(42)));
    assert_eq!(
        drained[0].key.session.instance_epoch,
        gateway.epoch().to_string()
    );
    assert!(drained[0].preview.contains("demo"));
    gateway.ingest_wire_frame(&approval);
    let replay = gateway
        .drain_requests(&session)
        .await
        .expect("replay drains nothing new");
    assert!(replay.is_empty());

    // Exactly one answer routes; the second use and a stale epoch refuse.
    let key = drained[0].key.clone();
    gateway
        .answer(ExternalAnswer {
            key: key.clone(),
            payload: AnswerPayload::Plain("approved".to_string()),
            scope: AnswerScope::Once,
        })
        .await
        .expect("first answer routes");
    let again = gateway
        .answer(ExternalAnswer {
            key: key.clone(),
            payload: AnswerPayload::Plain("approved".to_string()),
            scope: AnswerScope::Once,
        })
        .await
        .expect_err("Once scope refuses a second reply");
    assert!(
        matches!(
            again,
            central_application::contracts::DispatchError::Rejected(_)
        ),
        "second reply is rejected, got {again:?}"
    );
    let stale = gateway
        .answer(ExternalAnswer {
            key: central_application::contracts::ExternalRequestKey {
                session: SessionRef {
                    provider: "codex".to_string(),
                    instance_epoch: "epoch-0".to_string(),
                    thread_id: session.thread_id.clone(),
                },
                turn_id: drained[0].key.turn_id.clone(),
                item_id: drained[0].key.item_id.clone(),
                request_id: drained[0].key.request_id.clone(),
            },
            payload: AnswerPayload::Plain("approved".to_string()),
            scope: AnswerScope::Once,
        })
        .await
        .expect_err("stale epoch never replays");
    assert!(
        format!("{stale:?}").contains("stale"),
        "stale epoch is rejected, got {stale:?}"
    );
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
