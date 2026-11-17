//! Pinned Muse Code CLI version allowlist.
//!
//! The MSP session-host protocol is versioned; Central pins the exact CLI
//! build it was validated against (offline schema export of `1.4.20`) and
//! refuses anything outside the allowlist with an actionable error (see
//! [`check_supported_version`]). The serve handshake itself is not
//! live-exercised yet — see `contracts/muse/1.4.20/manifest.json`.

/// CLI build the MSP protocol subset was live-verified against.
pub const SUPPORTED_CLI_VERSION: &str = "1.4.23";

/// CLI builds Central speaks. Extended only with a new capture + subset.
/// `1.4.20` shares the identical stable surface (75 methods / 47
/// notifications / 2 requests / 39 errors); `1.4.23` additionally passed
/// the live echo-provider handshake on 2026-11-17. See
/// `contracts/muse/1.4.23/protocol-subset.json`.
pub const CLI_VERSION_ALLOWLIST: &[&str] = &["1.4.20", SUPPORTED_CLI_VERSION];

/// A parsed `major.minor.patch[-pre][+build]` version. Suffixes compare
/// lexically; only exact allowlist membership is accepted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CliVersion {
    raw: String,
    core: (u64, u64, u64),
    suffix: Option<String>,
}

impl CliVersion {
    /// The original version string, e.g. `1.4.20`.
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
        return Err(VersionError("muse CLI version is empty".to_string()));
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
        return Err(VersionError(format!("unsupported muse CLI version: {raw}")));
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
        .map_err(|_| VersionError(format!("unsupported muse CLI version: {raw}")))
}

/// Reject any CLI build outside the allowlist with an actionable error that
/// names the observed version, the allowlist, and the remediation.
pub fn check_supported_version(version: &CliVersion) -> Result<(), VersionError> {
    if CLI_VERSION_ALLOWLIST.contains(&version.as_str()) {
        return Ok(());
    }
    Err(VersionError(format!(
        "unsupported muse CLI version '{}' (parsed {}.{}.{}-{}); Central MSP support pins [{}]. \
         Pin or install a supported muse CLI build, then re-run; refusing to speak an \
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
/// fallback: Central always sends the profile model verbatim.
pub fn check_model_match(requested: &str, observed: &str) -> Result<(), VersionError> {
    if requested == observed {
        return Ok(());
    }
    Err(VersionError(format!(
        "model capability mismatch: requested '{requested}' but the server resolved '{observed}'. \
         Central never falls back silently; \
         resolve the profile to an available model and retry"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allowlist_accepts_the_pinned_build() {
        let version = parse_cli_version(SUPPORTED_CLI_VERSION).unwrap();
        assert!(check_supported_version(&version).is_ok());
        // The 1.4.20 schema capture stays speakable: identical stable surface.
        let older = parse_cli_version("1.4.20").unwrap();
        assert!(check_supported_version(&older).is_ok());
    }

    #[test]
    fn allowlist_rejects_anything_else_with_an_actionable_error() {
        for raw in ["1.4.21", "0.167.1", "1.4.20-rc1", "", "muse"] {
            let parsed = parse_cli_version(raw);
            let failed = match parsed {
                Err(_) => true,
                Ok(version) => check_supported_version(&version).is_err(),
            };
            assert!(failed, "version {raw:?} must not pass the allowlist");
        }
        let error = check_supported_version(&parse_cli_version("1.4.21").unwrap()).unwrap_err();
        assert!(error.to_string().contains("1.4.21"));
        assert!(error.to_string().contains(SUPPORTED_CLI_VERSION));
    }

    #[test]
    fn model_mismatch_is_an_error_never_a_fallback() {
        assert!(check_model_match("m", "m").is_ok());
        assert!(check_model_match("a", "b").is_err());
    }
}
