//! Versioned Muse MSP session-host stdio transport.
//!
//! [`MuseGateway`] implements [`AgentGateway`](central_application::ports::AgentGateway).
//! The transport speaks JSON-RPC 2.0 as JSONL over stdio with an 8 MiB frame
//! cap, correlates out-of-order responses by id, and drains a bounded
//! notification buffer: overflow or a lost connection surfaces
//! reconcile-required evidence instead of dropping events silently.
//!
//! Schema provenance: offline export (`muse schema generate-json-schema`)
//! from muse 1.4.20; see `contracts/muse/1.4.20/`. The serve handshake is
//! NOT live-exercised yet — shapes the server does not confirm are refused
//! loudly instead of guessed.

pub mod gateway;
pub mod requests;
pub mod version;

pub use gateway::{MuseConfig, MuseGateway};
pub use requests::{method_kind, normalize_request, observation_for, session_field};
pub use version::{
    check_model_match, check_supported_version, parse_cli_version, CliVersion, VersionError,
    CLI_VERSION_ALLOWLIST, SUPPORTED_CLI_VERSION,
};
