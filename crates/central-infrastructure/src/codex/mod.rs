//! Versioned Codex app-server stdio transport (P2 Task 1).
//!
//! [`CodexGateway`] implements [`AgentGateway`](central_application::ports::AgentGateway).
//! The transport speaks JSON-RPC 2.0 as JSONL over stdio with an 8 MiB frame
//! cap, correlates out-of-order responses by id, and drains a bounded
//! notification buffer: overflow or a lost connection surfaces
//! reconcile-required evidence instead of dropping events silently.
//!
//! Schema provenance: spike-captured (`spikes/codex-app-server`), CLI
//! `0.167.0-alpha.3.2`; see `contracts/codex/0.167.0-alpha.3.2/`.

pub mod bridge;
pub mod gateway;
pub mod prompts;
pub mod requests;
pub mod stdio;
pub mod version;
pub mod wire;

pub use gateway::{CodexConfig, CodexGateway};
pub use requests::{
    method_kind, normalize_request, observation_for, NormalizedRequest, PendingRequestTable,
    SECRET_PREVIEW_PLACEHOLDER,
};
pub use stdio::{BufferedEvent, EventBuffer, IncomingEvent, RpcClient};
pub use version::{
    check_model_match, check_supported_version, parse_cli_version, CliVersion, VersionError,
    CLI_VERSION_ALLOWLIST, SUPPORTED_CLI_VERSION,
};
pub use wire::{RpcError, RpcErrorObject, RpcId, FRAME_CAP_BYTES};
