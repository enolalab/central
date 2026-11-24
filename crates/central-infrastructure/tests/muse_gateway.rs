//! Muse MSP gateway: contracts, normalization and capabilities parity.

use std::sync::Arc;

use central_application::contracts::AppError;
use central_application::ports::AgentGateway;
use central_domain::{ContentHash, InboxKind, ProfileId, ResolvedProfile};
use central_infrastructure::muse::{
    method_kind, normalize_request, MuseConfig, MuseGateway, SUPPORTED_CLI_VERSION,
};
use central_infrastructure::process::OwnedProcessSupervisor;
use serde_json::{json, Value};

fn profile() -> ResolvedProfile {
    ResolvedProfile {
        id: ProfileId::from_u128(11),
        provider: "muse".to_string(),
        model: "model-under-test".to_string(),
        effort: "low".to_string(),
        skills: vec![],
        subagents: vec![],
        permissions: vec![],
        fallbacks: vec![],
        hash: ContentHash::from_hex("a".repeat(64)),
    }
}

fn gateway_with(models: &[&str]) -> MuseGateway {
    let config = MuseConfig {
        surveyed_models: models.iter().map(|model| model.to_string()).collect(),
        ..MuseConfig::default()
    };
    MuseGateway::new(Arc::new(OwnedProcessSupervisor::new()), config)
}

fn server_request(method: &str, params: Value) -> central_infrastructure::codex::BufferedEvent {
    central_infrastructure::codex::BufferedEvent {
        id: Some(central_infrastructure::codex::RpcId::Num(3)),
        method: method.to_string(),
        params,
        is_request: true,
    }
}

#[test]
fn recorded_muse_contracts_are_valid_json_and_pin_the_build() {
    let manifest: Value =
        serde_json::from_str(include_str!("../../../contracts/muse/1.4.23/manifest.json"))
            .expect("muse manifest must be valid JSON");
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
        "../../../contracts/muse/1.4.23/protocol-subset.json"
    ))
    .expect("muse protocol subset must be valid JSON");
    for method in [
        "session/start",
        "turn/start",
        "approval/decide",
        "userInput/answer",
    ] {
        assert!(
            subset["clientMethods"][method].is_object(),
            "subset must document {method}"
        );
    }
    for method in ["approval/request", "userInput/request"] {
        assert!(
            subset["serverRequests"][method].is_object(),
            "subset must document {method}"
        );
    }

    let errors: Value =
        serde_json::from_str(include_str!("../../../contracts/muse/1.4.23/errors.json"))
            .expect("muse error table must be valid JSON");
    assert_eq!(errors["provenance"]["errorCount"], Value::Number(39.into()));
}

#[test]
fn muse_1_4_25_capture_keeps_the_verified_surface() {
    // Allowlist extension for 1.4.25 (echo handshake 2026-11-23, after the
    // binary auto-updated past 1.4.24 on the lab machine): identical
    // surface, all 39 error codes, grown allowlist.
    let manifest: Value =
        serde_json::from_str(include_str!("../../../contracts/muse/1.4.25/manifest.json"))
            .expect("muse 1.4.25 manifest must be valid JSON");
    assert_eq!(manifest["cliVersion"].as_str().unwrap(), "1.4.25");
    let allowlist = manifest["cliVersionAllowlist"]
        .as_array()
        .expect("1.4.25 manifest carries an allowlist");
    for pinned in ["1.4.20", "1.4.23", "1.4.24", "1.4.25"] {
        assert!(
            allowlist.iter().any(|entry| entry.as_str() == Some(pinned)),
            "allowlist keeps {pinned}"
        );
    }
    let subset: Value = serde_json::from_str(include_str!(
        "../../../contracts/muse/1.4.25/protocol-subset.json"
    ))
    .expect("muse 1.4.25 protocol subset must be valid JSON");
    for method in [
        "session/start",
        "turn/start",
        "approval/decide",
        "userInput/answer",
    ] {
        assert!(
            subset["clientMethods"][method].is_object(),
            "subset must document {method}"
        );
    }
    let errors: Value =
        serde_json::from_str(include_str!("../../../contracts/muse/1.4.25/errors.json"))
            .expect("muse 1.4.25 error table must be valid JSON");
    assert_eq!(errors["provenance"]["errorCount"], Value::Number(39.into()));
    assert_eq!(
        errors["errors"].as_array().unwrap().len(),
        39,
        "1.4.25 keeps all 39 error codes"
    );
}

#[test]
fn muse_1_4_24_capture_keeps_the_verified_surface() {
    // Allowlist extension for 1.4.24 (echo handshake 2026-11-21): the
    // curated subset documents the same methods/requests, the error table
    // keeps all 39 codes, and the manifest advertises the grown allowlist.
    let manifest: Value =
        serde_json::from_str(include_str!("../../../contracts/muse/1.4.24/manifest.json"))
            .expect("muse 1.4.24 manifest must be valid JSON");
    assert_eq!(manifest["cliVersion"].as_str().unwrap(), "1.4.24");
    let allowlist = manifest["cliVersionAllowlist"]
        .as_array()
        .expect("1.4.24 manifest carries an allowlist");
    for pinned in ["1.4.20", "1.4.23", "1.4.24"] {
        assert!(
            allowlist.iter().any(|entry| entry.as_str() == Some(pinned)),
            "allowlist keeps {pinned}"
        );
    }

    let subset: Value = serde_json::from_str(include_str!(
        "../../../contracts/muse/1.4.24/protocol-subset.json"
    ))
    .expect("muse 1.4.24 protocol subset must be valid JSON");
    for method in [
        "session/start",
        "turn/start",
        "approval/decide",
        "userInput/answer",
    ] {
        assert!(
            subset["clientMethods"][method].is_object(),
            "subset must document {method}"
        );
    }
    for method in ["approval/request", "userInput/request"] {
        assert!(
            subset["serverRequests"][method].is_object(),
            "subset must document {method}"
        );
    }

    let errors: Value =
        serde_json::from_str(include_str!("../../../contracts/muse/1.4.24/errors.json"))
            .expect("muse 1.4.24 error table must be valid JSON");
    assert_eq!(errors["provenance"]["errorCount"], Value::Number(39.into()));
    assert_eq!(
        errors["errors"].as_array().unwrap().len(),
        39,
        "1.4.24 keeps all 39 error codes"
    );
}

#[test]
fn unknown_server_methods_are_never_inbox_traffic() {
    assert_eq!(method_kind("approval/request"), Some(InboxKind::Approval));
    assert_eq!(method_kind("userInput/request"), Some(InboxKind::Question));
    assert_eq!(method_kind("turn/completed"), None);
    assert_eq!(method_kind("session/tokenUsage"), None);
}

#[test]
fn approval_and_question_normalize_to_muse_scoped_keys() {
    let epoch = "epoch-1";
    let approval = normalize_request(
        epoch,
        &server_request(
            "approval/request",
            json!({
                "sessionId": "server-session",
                "turnId": "turn-1",
                "approvalId": "approval-1",
                "currentRequirementId": "req-1",
                "command": "rm -rf /",
            }),
        ),
    )
    .expect("approval/request is inbox traffic");
    assert_eq!(approval.kind, InboxKind::Approval);
    assert_eq!(approval.key.session.provider, "muse");
    assert_eq!(approval.key.session.instance_epoch, epoch);
    assert_eq!(approval.key.session.thread_id, "server-session");
    assert_eq!(approval.key.turn_id, "turn-1");
    assert_eq!(approval.key.item_id, "approval-1");

    let question = normalize_request(
        epoch,
        &server_request(
            "userInput/request",
            json!({
                "sessionId": "server-session",
                "userInputId": "prompt-1",
                "questions": [{
                    "id": "q1",
                    "header": "Deploy?",
                    "question": "Deploy now?",
                    "isSecret": false,
                    "options": [{"label": "yes"}, {"label": "no"}],
                }],
            }),
        ),
    )
    .expect("userInput/request is inbox traffic");
    assert_eq!(question.kind, InboxKind::Question);
    assert_eq!(question.key.item_id, "prompt-1");
    assert!(question.preview.contains("Deploy now?"));
    // The `isSecret` key exists (even false), so the request takes the
    // hardened secret-bearing path — same conservative semantics as Codex.
    // The question text stays visible; only the stored hash covers redacted text.
    assert!(question.is_secret);

    assert!(normalize_request(epoch, &server_request("turn/completed", json!({}))).is_none());
}

#[test]
fn oversize_frame_closes_the_transport_instead_of_parsing_partially() {
    use central_application::ports::ProcessSupervisor;

    struct StubSupervisor;
    #[async_trait::async_trait]
    impl ProcessSupervisor for StubSupervisor {
        async fn spawn(
            &self,
            _spec: &central_application::contracts::ProcessSpec,
        ) -> Result<
            central_application::contracts::ProcessIdentity,
            central_application::contracts::AppError,
        > {
            unreachable!("ingest test never spawns");
        }

        async fn inspect(
            &self,
            _identity: &central_application::contracts::ProcessIdentity,
        ) -> Result<
            central_application::contracts::ProcessState,
            central_application::contracts::AppError,
        > {
            Ok(central_application::contracts::ProcessState::Running)
        }

        async fn terminate(
            &self,
            _identity: &central_application::contracts::ProcessIdentity,
        ) -> Result<(), central_application::contracts::AppError> {
            Ok(())
        }
    }

    let gateway = MuseGateway::new(std::sync::Arc::new(StubSupervisor), MuseConfig::default());
    assert!(!gateway.reconcile_required());
    // 8 MiB + 1 byte of payload (no newline counted by the cap check on the
    // trimmed line): must latch closed, never parse.
    gateway.ingest_wire_frame(&"x".repeat(8 * 1024 * 1024 + 1));
    assert!(gateway.reconcile_required());
}

#[tokio::test]
async fn capabilities_advertise_only_surveyed_models() {
    let gateway = gateway_with(&["model-under-test"]);
    let capabilities = gateway
        .capabilities(&profile())
        .await
        .expect("pinned build is compatible");
    assert_eq!(capabilities.provider, "muse");
    assert!(capabilities.supports_resume);
    assert!(capabilities.features.contains(&"session/start".to_string()));
    assert!(capabilities
        .features
        .contains(&"approval/request".to_string()));
    assert!(capabilities
        .features
        .contains(&"model:model-under-test".to_string()));

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

/// Foreign notifications must never sink our own observations: the live
/// 1.4.25 host emits session/global notifications (branch changes, usage)
/// that do not belong to the polled session, and a poll carrying only
/// those must return no observations instead of failing the whole
/// reconcile. A poll that also carries our turn/completed still reports
/// the completion.
#[tokio::test]
async fn foreign_notifications_never_sink_our_turn() {
    use central_application::contracts::{OpenSession, SendTurn};
    use central_application::ports::ProcessSupervisor;
    use central_domain::{AttemptId, CommandId, Timestamp};
    use central_infrastructure::process::{PipedSpawner, PipedStdio};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    struct StubPiped;

    #[async_trait::async_trait]
    impl PipedSpawner for StubPiped {
        async fn spawn_piped(
            &self,
            _spec: &central_application::contracts::ProcessSpec,
        ) -> Result<(central_application::contracts::ProcessIdentity, PipedStdio), AppError>
        {
            let (gw_write, srv_read) = tokio::io::duplex(65536);
            let (mut srv_write, gw_read) = tokio::io::duplex(65536);
            let turn_count = std::sync::Arc::new(std::sync::Mutex::new(0usize));
            tokio::spawn({
                let turn_count = std::sync::Arc::clone(&turn_count);
                async move {
                    let mut reader = BufReader::new(srv_read);
                    let mut line = String::new();
                    loop {
                        line.clear();
                        if reader.read_line(&mut line).await.is_err() {
                            return;
                        }
                        let Ok(msg) = serde_json::from_str::<Value>(&line) else {
                            continue;
                        };
                        let Some(id) = msg.get("id").cloned() else {
                            continue;
                        };
                        let method = msg.get("method").and_then(|m| m.as_str()).unwrap_or("");
                        // Count turns: the first turn completes, later turns
                        // only draw foreign chatter (another session's branch
                        // change plus a session-less usage notice). Neither
                        // belongs to our attempt.
                        if method == "turn/start" {
                            let n = {
                                let mut turns = turn_count.lock().expect("turns");
                                *turns += 1;
                                *turns
                            };
                            let reply = json!({"jsonrpc":"2.0","id":id,"result":{"turnId":format!("turn-{n}")}});
                            let _ = srv_write.write_all(format!("{reply}\n").as_bytes()).await;
                            if n == 1 {
                                let done = json!({"jsonrpc":"2.0","method":"turn/completed","params":{"sessionId":"ses_live","turnId":"turn-1"}});
                                let _ = srv_write.write_all(format!("{done}\n").as_bytes()).await;
                            } else {
                                for note in [
                                    json!({"jsonrpc":"2.0","method":"session/branchChanged","params":{"sessionId":"ses_other"}}),
                                    json!({"jsonrpc":"2.0","method":"usage/changed","params":{}}),
                                ] {
                                    let _ =
                                        srv_write.write_all(format!("{note}\n").as_bytes()).await;
                                }
                            }
                            continue;
                        }
                        let reply = match method {
                            "initialize" => {
                                json!({"jsonrpc":"2.0","id":id,"result":{"userAgent":"muse-build/1.4.25 (serve; stub)"}})
                            }
                            "session/start" => {
                                json!({"jsonrpc":"2.0","id":id,"result":{"session":{"id":"ses_live"}}})
                            }
                            _ => json!({"jsonrpc":"2.0","id":id,"result":{}}),
                        };
                        let _ = srv_write.write_all(format!("{reply}\n").as_bytes()).await;
                    }
                }
            });
            Ok((
                central_application::contracts::ProcessIdentity {
                    pid: 4243,
                    created_at: Timestamp::from_millis(1_700_000_000_000),
                    ownership_id: "muse-foreign-notif".to_string(),
                },
                PipedStdio {
                    stdin: Box::new(gw_write),
                    stdout: Box::new(gw_read),
                },
            ))
        }
    }

    struct StubSupervisor;

    #[async_trait::async_trait]
    impl ProcessSupervisor for StubSupervisor {
        async fn spawn(
            &self,
            _spec: &central_application::contracts::ProcessSpec,
        ) -> Result<central_application::contracts::ProcessIdentity, AppError> {
            unreachable!("probe attaches through the piped spawner");
        }
        async fn inspect(
            &self,
            _identity: &central_application::contracts::ProcessIdentity,
        ) -> Result<central_application::contracts::ProcessState, AppError> {
            Ok(central_application::contracts::ProcessState::Running)
        }
        async fn terminate(
            &self,
            _identity: &central_application::contracts::ProcessIdentity,
        ) -> Result<(), AppError> {
            Ok(())
        }
    }

    let config = MuseConfig {
        // The stub serves the allowlisted 1.4.25 handshake.
        cli_version: "1.4.25".to_string(),
        ..MuseConfig::default()
    };
    let gateway = MuseGateway::new(std::sync::Arc::new(StubSupervisor), config)
        .with_piped(std::sync::Arc::new(StubPiped));
    let session = gateway
        .open(OpenSession {
            attempt_id: AttemptId::from_u128(64),
            profile: profile(),
            workspace: "/tmp".to_string(),
            resume_from: None,
        })
        .await
        .expect("open binds a server session");
    let turn = gateway
        .send(SendTurn {
            session: session.clone(),
            attempt_id: AttemptId::from_u128(64),
            input: "Say hello.".to_string(),
            output_schema: None,
            command_id: CommandId::from_u128(65),
        })
        .await
        .expect("send starts a turn");
    assert_eq!(turn.turn_id, "turn-1");
    let completed = gateway.observe(&session).await.expect("observe polls");
    assert!(
        completed
            .iter()
            .any(|observation| observation.dedup_key.starts_with("turn/completed")),
        "our turn/completed survives foreign chatter: {completed:?}"
    );

    // A later turn draws only foreign chatter (another session's branch
    // change plus a session-less usage notice). The poll reports nothing
    // instead of failing the whole reconcile with
    // reconcile-required.
    let turn2 = gateway
        .send(SendTurn {
            session: session.clone(),
            attempt_id: AttemptId::from_u128(64),
            input: "Say hello again.".to_string(),
            output_schema: None,
            command_id: CommandId::from_u128(66),
        })
        .await
        .expect("second turn starts");
    assert_eq!(turn2.turn_id, "turn-2");
    let quiet = gateway.observe(&session).await;
    assert!(
        quiet
            .map(|observations| observations.is_empty())
            .unwrap_or(false),
        "foreign-only poll must be Ok(empty), never reconcile-required"
    );
}
