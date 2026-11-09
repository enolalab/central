//! Structured leader/result channel over the app-server (P2 Task 4).
//!
//! Central offers the harness a proposal/result channel with a schema, and
//! never scrapes chat markdown to mutate state. Two wire shapes exist:
//!
//! * **Dynamic tools** (`central_propose_plan`, `central_report_result`),
//!   used when the harness advertises them in [`Capabilities::features`].
//! * **`outputSchema` final DTO**, used when the harness supports a
//!   structured final payload but no Central tools.
//!
//! [`channel_for`] selects the channel from advertised capabilities. When
//! neither is advertised the error is actionable: it names the missing
//! capability and the validated file-import route, and it never drops the
//! requirement. [`ensure_worker_execution_allowed`] is the smoke gate: worker
//! execution stays disabled until the structured channel is proven, so no
//! worker turn runs on scraped chat.

use central_application::contracts::{AppError, Capabilities};

/// Dynamic tool the harness may advertise for structured plan proposals.
pub const PROPOSE_TOOL: &str = "central_propose_plan";
/// Dynamic tool the harness may advertise for structured worker reports.
pub const REPORT_TOOL: &str = "central_report_result";
/// Capability feature name for a structured final payload (`outputSchema`).
pub const OUTPUT_SCHEMA_FEATURE: &str = "outputSchema";

/// Validated file-import fallback, always available. The CLI validates the
/// file against the schema, prints the content hash, and commits it through
/// the normal command path — never scraped chat.
pub const PLAN_IMPORT_ROUTE: &str = "central plan import --run <RUN> --file <PLAN.json>";
pub const RESULT_IMPORT_ROUTE: &str =
    "central result import --run <RUN> --attempt <ATTEMPT> --file <WORKER-RESULT.json>";
pub const HANDOVER_IMPORT_ROUTE: &str =
    "central handover import --run <RUN> --file <HANDOVER.json>";

/// Which structured wire shape to use for this harness.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StructuredChannel {
    DynamicTools,
    OutputSchema,
}

/// Select the structured channel from harness-advertised capabilities.
/// Dynamic tools win when both are present. Neither present is a
/// [`AppError::Capability`] naming the missing capability and the validated
/// import route — the requirement is never silently dropped.
pub fn channel_for(capabilities: &Capabilities) -> Result<StructuredChannel, AppError> {
    let has = |feature: &str| capabilities.features.iter().any(|f| f == feature);
    if has(PROPOSE_TOOL) && has(REPORT_TOOL) {
        return Ok(StructuredChannel::DynamicTools);
    }
    if has(OUTPUT_SCHEMA_FEATURE) {
        return Ok(StructuredChannel::OutputSchema);
    }
    Err(AppError::Capability(format!(
        "provider '{}' advertises no structured leader/result channel (needs '{PROPOSE_TOOL}' + '{REPORT_TOOL}' or '{OUTPUT_SCHEMA_FEATURE}'); \
         worker execution stays disabled until the channel smoke passes; \
         validated fallback: {PLAN_IMPORT_ROUTE} (schema central/plan-v1)",
        capabilities.provider,
    )))
}

/// Smoke gate before worker execution enablement: the structured channel
/// must be proven for this harness. An unsupported capability fails here
/// with the actionable error from [`channel_for`], never with scraped chat.
pub fn ensure_worker_execution_allowed(
    capabilities: &Capabilities,
) -> Result<StructuredChannel, AppError> {
    channel_for(capabilities)
}

/// The `outputSchema` document id to request for a leader turn: the final
/// payload must parse as `central/leader-reply-v1`.
pub fn leader_output_schema() -> &'static str {
    central_application::leader::LEADER_REPLY_SCHEMA
}

/// The `outputSchema` document id to request for a worker turn: the final
/// payload must parse as `central/worker-result-v1`.
pub fn worker_output_schema() -> &'static str {
    central_application::handover::WORKER_RESULT_SCHEMA
}

#[cfg(test)]
mod tests {
    use super::*;

    fn capabilities(features: &[&str]) -> Capabilities {
        Capabilities {
            provider: "codex".to_string(),
            supports_resume: true,
            features: features.iter().map(|f| f.to_string()).collect(),
        }
    }

    #[test]
    fn dynamic_tools_win_when_advertised() {
        let channel = channel_for(&capabilities(&[PROPOSE_TOOL, REPORT_TOOL])).expect("channel");
        assert_eq!(channel, StructuredChannel::DynamicTools);
    }

    #[test]
    fn output_schema_is_the_fallback_channel() {
        let channel = channel_for(&capabilities(&[OUTPUT_SCHEMA_FEATURE])).expect("channel");
        assert_eq!(channel, StructuredChannel::OutputSchema);
    }

    #[test]
    fn unsupported_channel_is_actionable_not_silent() {
        let error = channel_for(&capabilities(&["turn/start"])).expect_err("must fail closed");
        let message = error.to_string();
        assert!(message.contains(PROPOSE_TOOL), "names the missing tool");
        assert!(
            message.contains("central plan import"),
            "names the import route"
        );
        assert!(
            message.contains("stays disabled"),
            "never drops the requirement"
        );
    }

    #[test]
    fn smoke_gate_blocks_worker_execution_without_channel() {
        assert!(ensure_worker_execution_allowed(&capabilities(&[])).is_err());
        assert!(ensure_worker_execution_allowed(&capabilities(&[OUTPUT_SCHEMA_FEATURE])).is_ok());
    }
}
