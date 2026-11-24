//! opencode REST gateway: contracts, normalization and loopback wire parity.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use central_application::contracts::{
    AppError, ExecutionRef, ProcessIdentity, ProcessSpec, ProcessState,
};
use central_application::ports::{AgentGateway, ProcessSupervisor};
use central_domain::{
    AnswerPayload, AnswerScope, AttemptId, CommandId, ContentHash, InboxKind, ProfileId,
    ResolvedProfile,
};
use central_infrastructure::opencode::{
    method_kind, normalize_request, OpencodeConfig, OpencodeGateway, SUPPORTED_CLI_VERSION,
};
use central_infrastructure::process::{PipedSpawner, PipedStdio};
use serde_json::{json, Value};
use tokio::io::AsyncWriteExt;

fn profile() -> ResolvedProfile {
    ResolvedProfile {
        id: ProfileId::from_u128(21),
        provider: "opencode".to_string(),
        model: "probe-model".to_string(),
        effort: "low".to_string(),
        skills: vec![],
        subagents: vec![],
        permissions: vec![],
        fallbacks: vec![],
        hash: ContentHash::from_hex("b".repeat(64)),
    }
}

fn gateway_with(models: &[&str]) -> OpencodeGateway {
    let config = OpencodeConfig {
        surveyed_models: models.iter().map(|model| model.to_string()).collect(),
        ..OpencodeConfig::default()
    };
    OpencodeGateway::new(Arc::new(StubSupervisor), config)
}

struct StubSupervisor;

#[async_trait]
impl ProcessSupervisor for StubSupervisor {
    async fn spawn(&self, _spec: &ProcessSpec) -> Result<ProcessIdentity, AppError> {
        unreachable!("opencode tests attach through the piped spawner");
    }

    async fn inspect(&self, _identity: &ProcessIdentity) -> Result<ProcessState, AppError> {
        Ok(ProcessState::Running)
    }

    async fn terminate(&self, _identity: &ProcessIdentity) -> Result<(), AppError> {
        Ok(())
    }
}

#[test]
fn recorded_opencode_contracts_are_valid_json_and_pin_the_build() {
    let manifest: Value = serde_json::from_str(include_str!(
        "../../../contracts/opencode/2.2.7/manifest.json"
    ))
    .expect("opencode manifest must be valid JSON");
    assert_eq!(
        manifest["cliVersion"].as_str().unwrap(),
        SUPPORTED_CLI_VERSION
    );
    assert!(manifest["cliVersionAllowlist"]
        .as_array()
        .unwrap()
        .iter()
        .any(|entry| entry.as_str() == Some(SUPPORTED_CLI_VERSION)));
    assert_eq!(manifest["allowProviderModelFallback"], Value::Bool(false));

    let subset: Value = serde_json::from_str(include_str!(
        "../../../contracts/opencode/2.2.7/protocol-subset.json"
    ))
    .expect("opencode protocol subset must be valid JSON");
    for (method, path) in [
        ("POST /api/session", "session.create"),
        ("POST /api/session/{sessionID}/prompt", "session.prompt"),
        (
            "POST /api/session/{sessionID}/permission/{requestID}/reply",
            "permission.reply",
        ),
    ] {
        assert!(
            subset["restMethods"][method].is_object(),
            "subset must document {path} at {method}"
        );
    }
}

#[test]
fn unknown_methods_are_never_inbox_traffic() {
    assert_eq!(method_kind("session.permission"), Some(InboxKind::Approval));
    assert_eq!(method_kind("session.message"), None);
}

#[test]
fn permission_normalizes_to_an_opencode_scoped_key() {
    let epoch = "epoch-9";
    let normalized = normalize_request(
        epoch,
        "ses_live",
        &json!({
            "id": "perm_1",
            "sessionID": "ses_live",
            "action": "edit",
            "resources": ["/tmp/x"],
            "message": "Allow edit?",
        }),
    )
    .expect("permission requests are inbox traffic");
    assert_eq!(normalized.kind, InboxKind::Approval);
    assert_eq!(normalized.key.session.provider, "opencode");
    assert_eq!(normalized.key.session.instance_epoch, epoch);
    assert_eq!(normalized.key.session.thread_id, "ses_live");
    assert_eq!(normalized.key.item_id, "perm_1");
    assert!(normalized.preview.contains("edit"));
    assert!(normalized.preview.contains("/tmp/x"));
}

#[tokio::test]
async fn capabilities_advertise_only_surveyed_models() {
    let gateway = gateway_with(&["probe-model"]);
    let capabilities = gateway
        .capabilities(&profile())
        .await
        .expect("pinned build is compatible");
    assert_eq!(capabilities.provider, "opencode");
    assert!(capabilities.supports_resume);
    assert!(capabilities
        .features
        .contains(&"session.permission.reply".to_string()));
    assert!(capabilities
        .features
        .contains(&"model:probe-model".to_string()));

    let unsurveyed = gateway_with(&[]);
    let capabilities = unsurveyed
        .capabilities(&profile())
        .await
        .expect("pinned build is compatible");
    assert!(!capabilities
        .features
        .iter()
        .any(|feature| feature.starts_with("model:")));
}

/// Full wire loop against a stub loopback server: open (URL + version pin +
/// session create), send (prompt), observe (completed turn + usage +
/// permission request), answer (Once scope, `once` on the wire), and cancel
/// teardown. The stub speaks the live-verified 2.2.7 surface: Basic auth,
/// `{data}` envelopes, completed assistant messages with tokens.
#[tokio::test]
async fn loopback_wire_speaks_verified_protocol() {
    use tokio::io::{AsyncBufReadExt, BufReader};

    use central_application::contracts::{ExternalAnswer, OpenSession, SendTurn};

    struct StubPiped {
        url: String,
        password_slot: Arc<Mutex<Option<String>>>,
    }

    #[async_trait]
    impl PipedSpawner for StubPiped {
        async fn spawn_piped(
            &self,
            spec: &ProcessSpec,
        ) -> Result<(ProcessIdentity, PipedStdio), AppError> {
            let password = spec
                .env
                .iter()
                .find(|(key, _)| key == "OPENCODE_PASSWORD")
                .map(|(_, value)| value.clone())
                .expect("server secret travels only in the child environment");
            *self.password_slot.lock().expect("password slot") = Some(password);
            let (gw_write, _srv_read) = tokio::io::duplex(4096);
            drop(_srv_read);
            // Preload the URL line: the gateway reads it as child stdout,
            // then drains the rest; the stdin half stays open in the
            // gateway so the server never sees EOF.
            let (mut seed_write, seed_read) = tokio::io::duplex(4096);
            seed_write
                .write_all(format!("{{\"url\": \"{}\"}}\n", self.url).as_bytes())
                .await
                .expect("seed url");
            drop(seed_write);
            Ok((
                ProcessIdentity {
                    pid: 4343,
                    created_at: central_domain::Timestamp::from_millis(1_700_000_000_000),
                    ownership_id: "opencode-loopback".to_string(),
                },
                PipedStdio {
                    stdin: Box::new(gw_write),
                    stdout: Box::new(seed_read),
                },
            ))
        }
    }

    fn reply(body: &str) -> Vec<u8> {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    // Minimal stub server speaking the verified surface.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback bind");
    let port = listener.local_addr().expect("port").port();
    let password_slot: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let seen_replies: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let seen_session_bodies: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let message_calls = Arc::new(AtomicUsize::new(0));

    let server_passwords = Arc::clone(&password_slot);
    let server_replies = Arc::clone(&seen_replies);
    let session_bodies = Arc::clone(&seen_session_bodies);
    let server_calls = Arc::clone(&message_calls);
    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            let passwords = Arc::clone(&server_passwords);
            let replies = Arc::clone(&server_replies);
            let session_bodies = Arc::clone(&session_bodies);
            let calls = Arc::clone(&server_calls);
            tokio::spawn(async move {
                let (mut reader, mut writer) = socket.into_split();
                let mut reader = BufReader::new(&mut reader);
                let mut line = String::new();
                // Request line first: it routes the stub.
                if reader.read_line(&mut line).await.is_err() {
                    return;
                }
                let request_line = line.trim_end().to_string();
                let mut content_length = 0usize;
                let mut auth = String::new();
                loop {
                    line.clear();
                    if reader.read_line(&mut line).await.is_err() {
                        return;
                    }
                    let header = line.trim_end().to_string();
                    if header.is_empty() {
                        break;
                    }
                    let lower = header.to_ascii_lowercase();
                    if let Some(value) = lower.strip_prefix("content-length:") {
                        content_length = value.trim().parse().unwrap_or(0);
                    }
                    if let Some(value) = lower.strip_prefix("authorization:") {
                        auth = value.trim().to_string();
                    }
                }
                let mut body = vec![0u8; content_length];
                if content_length > 0 {
                    use tokio::io::AsyncReadExt;
                    if reader.read_exact(&mut body).await.is_err() {
                        return;
                    }
                }
                // The spawner already asserted the per-session secret travels
                // in the child environment; here every request must carry
                // some Basic credential.
                let has_secret = passwords.lock().expect("passwords").is_some();
                if !has_secret || !auth.starts_with("basic ") || auth.len() <= 6 {
                    let denied = b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 21\r\nConnection: close\r\n\r\n{\"message\":\"no auth\"}";
                    let _ = writer.write_all(denied).await;
                    return;
                }
                let parts: Vec<&str> = request_line.split_whitespace().collect();
                let path = parts.get(1).copied().unwrap_or("/");
                let response = if path == "/api/info" {
                    reply(r#"{"version":"2.2.7"}"#)
                } else if path == "/api/session" {
                    let value = serde_json::from_slice::<Value>(&body).unwrap_or(Value::Null);
                    {
                        session_bodies.lock().expect("sessions").push(value);
                    }
                    // The server echoes its model selection for fail-fast
                    // verification at open.
                    reply(
                        r#"{"data":{"id":"ses_stub","model":{"id":"probe-model","providerID":"stub"}}}"#,
                    )
                } else if path == "/api/session/ses_stub/prompt" {
                    reply(r#"{"data":{"id":"msg_p1","sessionID":"ses_stub","type":"user"}}"#)
                } else if path == "/api/session/ses_stub/message" {
                    let call = calls.fetch_add(1, Ordering::SeqCst);
                    if call == 0 {
                        reply(r#"{"data":[{"id":"msg_p1","type":"user"}]}"#)
                    } else {
                        reply(
                            r#"{"data":[{"id":"msg_p1","type":"user"},{"id":"msg_a1","type":"assistant","time":{"created":1,"completed":2},"model":{"id":"probe-model","providerID":"opencode"},"finish":"stop","tokens":{"input":10,"output":3,"reasoning":0,"cache":{"read":0,"write":0}}}]}"#,
                        )
                    }
                } else if path == "/api/session/ses_stub/permission" {
                    reply(
                        r#"{"data":[{"id":"perm_1","sessionID":"ses_stub","action":"edit","resources":["/tmp/x"],"message":"Allow edit?"}]}"#,
                    )
                } else if path == "/api/session/ses_stub/permission/perm_1/reply" {
                    let value = serde_json::from_slice::<Value>(&body).unwrap_or(Value::Null);
                    {
                        replies.lock().expect("replies").push(value);
                    }
                    reply(r#"{"data":{}}"#)
                } else {
                    reply(r#"{"data":[]}"#)
                };
                let _ = writer.write_all(&response).await;
            });
        }
    });

    let spawner = Arc::new(StubPiped {
        url: format!("http://127.0.0.1:{port}"),
        password_slot: Arc::clone(&password_slot),
    });
    let gateway = OpencodeGateway::new(Arc::new(StubSupervisor), OpencodeConfig::default())
        .with_piped(spawner);
    let _ = gateway.supervisor();

    let session = gateway
        .open(OpenSession {
            attempt_id: AttemptId::from_u128(44),
            profile: profile(),
            workspace: "/tmp".to_string(),
            resume_from: None,
        })
        .await
        .expect("open binds a server session");
    assert_eq!(session.provider, "opencode");
    assert_eq!(session.thread_id, "ses_stub");

    let turn = gateway
        .send(SendTurn {
            session: session.clone(),
            attempt_id: AttemptId::from_u128(44),
            input: "Say hello.".to_string(),
            output_schema: None,
            command_id: CommandId::from_u128(45),
        })
        .await
        .expect("send prompts the server session");
    assert_eq!(turn.turn_id, "msg_p1");
    // The session create names the profile model as a Model.Ref: a bare
    // profile id travels as `{"id"}` and the server echo is verified at
    // open, before any prompt is sent.
    {
        let bodies = seen_session_bodies.lock().expect("sessions");
        assert_eq!(bodies.len(), 1, "one session create, got {bodies:?}");
        assert_eq!(
            bodies[0]["model"]["id"],
            Value::String("probe-model".to_string()),
            "session create carries the profile model"
        );
    }

    // First poll: streaming (no completed message yet) plus one permission.
    let first = gateway.observe(&session).await.expect("observe polls");
    assert_eq!(first.len(), 1, "one permission observation, got {first:?}");
    assert!(first[0].dedup_key.contains("perm_1"));

    // Second poll: the completed turn lands with usage, permission replays
    // are suppressed by the seen-set.
    let second = gateway.observe(&session).await.expect("observe polls");
    assert_eq!(
        second.len(),
        1,
        "one completed-turn observation, got {second:?}"
    );
    assert!(second[0].dedup_key.contains("msg_a1"));
    assert!(second[0].detail.contains("turn completed"));
    assert!(second[0].detail.contains("in=10"));

    // Drain moves the Seen request into the durable inbox shape.
    let drained = gateway
        .drain_requests(&session)
        .await
        .expect("drain requests");
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].kind, InboxKind::Approval);
    assert_eq!(drained[0].key.item_id, "perm_1");

    gateway
        .answer(ExternalAnswer {
            key: drained[0].key.clone(),
            payload: AnswerPayload::Plain("approved".to_string()),
            scope: AnswerScope::Once,
        })
        .await
        .expect("answer replies once");
    {
        let replies = seen_replies.lock().expect("replies");
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0]["decision"], Value::String("once".to_string()));
    }

    let cancelled = gateway
        .cancel(&ExecutionRef {
            attempt_id: AttemptId::from_u128(44),
            session: session.clone(),
            turn: None,
        })
        .await
        .expect("cancel terminates");
    assert!(cancelled.cancelled);
    assert!(!gateway.reconcile_required());
}
