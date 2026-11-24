//! Versioned opencode CLI version allowlist.
//!
//! The REST surface is versioned; Central pins the exact CLI build it was
//! probed against and refuses anything outside the allowlist with an
//! actionable error (see [`check_supported_version`]).

/// CLI build the REST protocol subset was captured from.
pub const SUPPORTED_CLI_VERSION: &str = "2.2.7";

/// CLI builds Central speaks. Extended only with a new capture + subset.
/// See `contracts/opencode/2.2.7/protocol-subset.json`.
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
    /// The original version string, e.g. `2.2.7`.
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
        return Err(VersionError("opencode CLI version is empty".to_string()));
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
            "unsupported opencode CLI version: {raw}"
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
        .map_err(|_| VersionError(format!("unsupported opencode CLI version: {raw}")))
}

/// Reject any CLI build outside the allowlist with an actionable error that
/// names the observed version, the allowlist, and the remediation.
pub fn check_supported_version(version: &CliVersion) -> Result<(), VersionError> {
    if CLI_VERSION_ALLOWLIST.contains(&version.as_str()) {
        return Ok(());
    }
    Err(VersionError(format!(
        "unsupported opencode CLI version '{}' (parsed {}.{}.{}-{}); Central REST support pins [{}]. \
         Pin or install a supported opencode CLI build, then re-run; refusing to speak an \
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

/// Split a profile model reference into its opencode-side provider and id.
/// `"ollama/qwen3:4b"` splits at the first `/`; a bare id carries no
/// provider and only ever matches on the id itself.
pub fn split_model_ref(requested: &str) -> (Option<&str>, &str) {
    match requested.split_once('/') {
        Some((provider, id)) => (Some(provider), id),
        None => (None, requested),
    }
}

/// Requested/observed model match on the resolved `Model.Ref` pair. The
/// server echoes its selection per session and per message as
/// `{providerID, id}`; a `provider/id` profile must match both halves, a
/// bare profile id must match the observed id exactly. Anything else is a
/// capability error, never a silent fallback.
pub fn check_model_ref_match(
    requested: &str,
    observed_provider: Option<&str>,
    observed_id: &str,
) -> Result<(), VersionError> {
    let (provider, id) = split_model_ref(requested);
    let matched = match provider {
        Some(provider) => observed_provider == Some(provider) && observed_id == id,
        None => observed_id == id,
    };
    if matched {
        return Ok(());
    }
    let observed = match observed_provider {
        Some(provider) => format!("{provider}/{observed_id}"),
        None => observed_id.to_string(),
    };
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
    }

    #[test]
    fn allowlist_rejects_anything_else_with_an_actionable_error() {
        for raw in ["2.2.8", "1.4.23", "2.2.7-rc1", "", "opencode"] {
            let parsed = parse_cli_version(raw);
            let failed = match parsed {
                Err(_) => true,
                Ok(version) => check_supported_version(&version).is_err(),
            };
            assert!(failed, "version {raw:?} must not pass the allowlist");
        }
        let error = check_supported_version(&parse_cli_version("2.2.8").unwrap()).unwrap_err();
        assert!(error.to_string().contains("2.2.8"));
        assert!(error.to_string().contains(SUPPORTED_CLI_VERSION));
    }

    #[test]
    fn model_mismatch_is_an_error_never_a_fallback() {
        assert!(check_model_match("m", "m").is_ok());
        assert!(check_model_match("a", "b").is_err());
    }

    #[test]
    fn model_ref_match_covers_pairs_and_bare_ids() {
        // Full ref: both halves must match.
        assert!(check_model_ref_match("ollama/qwen3:4b", Some("ollama"), "qwen3:4b").is_ok());
        assert!(check_model_ref_match("ollama/qwen3:4b", Some("ollama"), "other").is_err());
        assert!(check_model_ref_match("ollama/qwen3:4b", Some("opencode"), "qwen3:4b").is_err());
        // Backend aliasing (free router resolving to another backend) fails loud.
        assert!(check_model_ref_match(
            "opencode/deepseek-v4.1-flash-free",
            Some("opencode"),
            "gemini-4-neon"
        )
        .is_err());
        // Bare id: exact id match only.
        assert!(check_model_ref_match("probe-model", None, "probe-model").is_ok());
        assert!(check_model_ref_match("probe-model", Some("stub"), "probe-model").is_ok());
        assert!(check_model_ref_match("probe-model", Some("stub"), "other").is_err());
    }
}
