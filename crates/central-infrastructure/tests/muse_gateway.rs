//! Muse MSP gateway: contracts, normalization and capabilities parity.

use std::sync::Arc;

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
