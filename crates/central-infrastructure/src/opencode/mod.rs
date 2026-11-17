//! Per-session opencode REST gateway over loopback HTTP.
//!
//! [`OpencodeGateway`] implements [`AgentGateway`](central_application::ports::AgentGateway).
//! Each session owns one `opencode serve` child (`--port 0` picks a free
//! port, `--stdio` prints the machine-readable `{"url"}`); auth is HTTP
//! Basic with a per-session secret passed only through the child
//! environment. Central polls message + permission lists per `observe()`;
//! the SSE stream is intentionally unused.
//!
//! Schema provenance: the live 2.2.7 OpenAPI plus wire probes; see
//! `contracts/opencode/2.2.7/`. The permission reply cycle is
//! schema-captured but NOT live-denied (the lab never asked).

pub mod client;
pub mod gateway;
pub mod requests;
pub mod version;

pub use client::{ClientError, OpencodeClient};
pub use gateway::{OpencodeConfig, OpencodeGateway};
pub use requests::{method_kind, normalize_request, observation_for};
pub use version::{
    check_model_match, check_supported_version, parse_cli_version, CliVersion, VersionError,
    CLI_VERSION_ALLOWLIST, SUPPORTED_CLI_VERSION,
};
