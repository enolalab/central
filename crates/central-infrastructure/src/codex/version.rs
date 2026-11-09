//! Pinned Codex CLI version allowlist.
//!
//! The app-server protocol is experimental; Central pins the exact CLI build
//! it was validated against and refuses anything outside the allowlist with
//! an actionable error (see [`check_supported_version`]).

/// CLI build the T1 fixtures and protocol subset were captured from.
pub const SUPPORTED_CLI_VERSION: &str = "0.167.0-alpha.3.2";

/// CLI builds Central speaks. Extended only with a new capture + subset.
pub const CLI_VERSION_ALLOWLIST: &[&str] = &[SUPPORTED_CLI_VERSION];

/// A parsed `major.minor.patch[-pre][+build]` version. Suffixes compare
/// lexically; only exact allowlist membership is accepted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CliVersion {
    raw: String,
    core: (u64, u64, u64),
    suffix: Option<String>,
}

impl CliVersion {
    /// The original version string, e.g. `0.167.0-alpha.3.2`.
    pub fn as_str(&self) -> &str {
        &self.raw
    }
}

/// Error for unparseable or non-allowlisted CLI versions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionError(pub String);

impl std::fmt::Display for VersionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for VersionError {}

/// Parse a CLI version string. Never accepts an empty string.
pub fn parse_cli_version(raw: &str) -> Result<CliVersion, VersionError> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(VersionError("codex CLI version is empty".to_string()));
    }
    let (core_part, suffix) = match raw.split_once('-') {
        Some((core, suffix)) => (core, Some(suffix.to_string())),
        None => (raw.split_once('+').map(|(c, _)| c).unwrap_or(raw), None),
    };
    let plus_suffix = if suffix.is_none() {
        raw.split_once('+').map(|(_, b)| b.to_string())
    } else {
        None
    };
    let mut parts = core_part.split('.');
    let major = parse_part(parts.next(), raw)?;
    let minor = parse_part(parts.next(), raw)?;
    let patch = parse_part(parts.next(), raw)?;
    if parts.next().is_some() {
        return Err(VersionError(format!(
            "unsupported codex CLI version: {raw}"
        )));
    }
    Ok(CliVersion {
        raw: raw.to_string(),
        core: (major, minor, patch),
        suffix: suffix.or(plus_suffix),
    })
}

fn parse_part(part: Option<&str>, raw: &str) -> Result<u64, VersionError> {
    part.unwrap_or_default()
        .parse::<u64>()
        .map_err(|_| VersionError(format!("unsupported codex CLI version: {raw}")))
}

/// Reject any CLI build outside the allowlist with an actionable error that
/// names the observed version, the allowlist, and the remediation.
pub fn check_supported_version(version: &CliVersion) -> Result<(), VersionError> {
    if CLI_VERSION_ALLOWLIST.contains(&version.as_str()) {
        return Ok(());
    }
    Err(VersionError(format!(
        "unsupported codex CLI version '{}' (parsed {}.{}.{}-{}); Central T1 supports [{}]. \
         Pin or install a supported codex CLI build, then re-run; refusing to speak an \
         unvalidated protocol version",
        version.as_str(),
        version.core.0,
        version.core.1,
        version.core.2,
        version.suffix.as_deref().unwrap_or(""),
        CLI_VERSION_ALLOWLIST.join(", "),
    )))
}

/// Requested/observed model mismatch is a capability error, never a silent
/// fallback: Central always sends `allowProviderModelFallback=false`.
pub fn check_model_match(requested: &str, observed: &str) -> Result<(), VersionError> {
    if requested == observed {
        return Ok(());
    }
    Err(VersionError(format!(
        "model capability mismatch: requested '{requested}' but the server resolved '{observed}'. \
         Central sets allowProviderModelFallback=false and never falls back silently; \
         resolve the profile to an available model and retry"
    )))
}
