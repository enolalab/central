//! Profile isolation and policy (P2 Task 2).
//!
//! [`resolve_profile`] merges the personal -> workflow -> task -> attempt
//! layers (pure function of its inputs, so concurrent A/B resolutions can
//! never mix) and checks the merged profile against harness-advertised
//! [`Capabilities`]. The harness advertises available models as
//! `model:<id>` feature entries; a requested model that is not advertised is
//! a [`ProfileError::ModelUnavailable`], never a silent substitution from
//! `fallbacks`. The resolved profile is frozen at attempt start in a
//! [`FrozenAttemptProfile`]: later layer edits resolve differently but never
//! rewrite the frozen copy.
//!
//! Skill hashes that cannot be observed are published as an explicit `None`
//! by [`observe_skills`], never fabricated. Capabilities whose support is
//! `Unknown` map to [`Enforcement::InstructionOnly`] and need explicit user
//! acceptance ([`may_dispatch`]) before dispatch.

use serde::{Deserialize, Serialize};

use central_domain::{AttemptId, ContentHash, ProfileError, ProfileLayers, ResolvedProfile};

use crate::contracts::{AppError, Capabilities};

impl From<ProfileError> for AppError {
    fn from(error: ProfileError) -> Self {
        match error {
            ProfileError::ModelUnavailable(message) => Self::Capability(message),
            ProfileError::CapabilityUnsupported(message) => Self::Capability(message),
            ProfileError::InvalidProfile(message) => Self::Validation(message),
        }
    }
}

/// Resolve the layered profile against harness capabilities. Fails loudly
/// when the provider mismatches or the model is not advertised.
pub fn resolve_profile(
    layers: &ProfileLayers,
    capabilities: &Capabilities,
) -> Result<ResolvedProfile, ProfileError> {
    let profile = central_domain::apply_layers(layers);
    if profile.model.trim().is_empty() {
        return Err(ProfileError::InvalidProfile(
            "resolved model is empty; layers must name a model".to_string(),
        ));
    }
    if profile.provider != capabilities.provider {
        return Err(ProfileError::CapabilityUnsupported(format!(
            "profile provider '{}' does not match harness provider '{}'",
            profile.provider, capabilities.provider
        )));
    }
    let advertised = format!("model:{}", profile.model);
    if !capabilities
        .features
        .iter()
        .any(|feature| feature == &advertised)
    {
        let available: Vec<&str> = capabilities
            .features
            .iter()
            .filter_map(|feature| feature.strip_prefix("model:"))
            .collect();
        return Err(ProfileError::ModelUnavailable(format!(
            "model '{}' is not advertised by provider '{}' (available: [{}]); \
             Central never falls back silently — resolve the profile to an available model",
            profile.model,
            capabilities.provider,
            available.join(", "),
        )));
    }
    Ok(profile)
}

/// A profile frozen when its attempt starts. Later layer edits are invisible
/// here by construction: the struct owns its copy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrozenAttemptProfile {
    pub attempt_id: AttemptId,
    pub profile: ResolvedProfile,
}

impl FrozenAttemptProfile {
    pub fn freeze(attempt_id: AttemptId, profile: ResolvedProfile) -> Self {
        Self {
            attempt_id,
            profile,
        }
    }
}

/// One skill with its observed content hash. `observed_hash` is `None` when
/// the hash cannot be observed; the unknown is published, not fabricated.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillObservation {
    pub name: String,
    pub observed_hash: Option<ContentHash>,
}

/// Source of observable skill content hashes. Implementations return `None`
/// for skills they cannot observe.
pub trait SkillHasher: Send + Sync {
    fn hash_skill(&self, name: &str) -> Option<ContentHash>;
}

/// Observe every skill on the profile, publishing unobservable hashes as an
/// explicit `None`.
pub fn observe_skills(
    profile: &ResolvedProfile,
    hasher: &dyn SkillHasher,
) -> Vec<SkillObservation> {
    profile
        .skills
        .iter()
        .map(|name| SkillObservation {
            name: name.clone(),
            observed_hash: hasher.hash_skill(name),
        })
        .collect()
}

/// Map harness-reported support to Central enforcement. `Unknown` is never
/// silently enforced.
pub fn enforcement_for(support: central_domain::CapabilitySupport) -> central_domain::Enforcement {
    use central_domain::{CapabilitySupport, Enforcement};
    match support {
        CapabilitySupport::Supported => Enforcement::Enforced,
        CapabilitySupport::Unknown => Enforcement::InstructionOnly,
        CapabilitySupport::Unsupported => Enforcement::Unsupported,
    }
}

/// A published capability with its enforcement and user acceptance. Only
/// `InstructionOnly` with explicit acceptance may dispatch besides `Enforced`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityReport {
    pub name: String,
    pub support: central_domain::CapabilitySupport,
    pub enforcement: central_domain::Enforcement,
    pub user_accepted: bool,
}

impl CapabilityReport {
    pub fn publish(name: &str, support: central_domain::CapabilitySupport) -> Self {
        Self {
            name: name.to_string(),
            support,
            enforcement: enforcement_for(support),
            user_accepted: false,
        }
    }

    /// Record explicit user acceptance of instruction-only enforcement.
    /// Accepting `Enforced` or `Unsupported` changes nothing.
    pub fn accept_instruction_only(&mut self) {
        if self.enforcement == central_domain::Enforcement::InstructionOnly {
            self.user_accepted = true;
        }
    }
}

/// Whether dispatch may proceed under this report. Defaults to closed:
/// `InstructionOnly` needs acceptance, `Unsupported` never dispatches.
pub fn may_dispatch(report: &CapabilityReport) -> bool {
    match report.enforcement {
        central_domain::Enforcement::Enforced => true,
        central_domain::Enforcement::InstructionOnly => report.user_accepted,
        central_domain::Enforcement::Unsupported => false,
    }
}
