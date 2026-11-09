use std::fmt;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{ContentHash, ProfileId};

/// Resolved agent profile. Policy validation lives in P2/T2 (application
/// `profiles` module); this is the single shared data type, not a second
/// application-level copy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedProfile {
    pub id: ProfileId,
    pub provider: String,
    pub model: String,
    pub effort: String,
    pub skills: Vec<String>,
    pub subagents: Vec<String>,
    pub permissions: Vec<String>,
    pub fallbacks: Vec<ProfileId>,
    pub hash: ContentHash,
}

/// Sparse per-layer override. `None` inherits the lower layer; `Some`
/// replaces it wholesale (lists included: layers never merge lists).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfilePatch {
    pub provider: Option<String>,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub skills: Option<Vec<String>>,
    pub subagents: Option<Vec<String>>,
    pub permissions: Option<Vec<String>>,
    pub fallbacks: Option<Vec<ProfileId>>,
}

/// Ordered profile layers, lowest precedence first. Resolution merges
/// personal -> workflow -> task -> attempt; the merged profile is frozen
/// (hash recomputed) when an attempt starts and never re-read afterwards,
/// so a later task edit cannot rewrite a running attempt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileLayers {
    pub personal: ResolvedProfile,
    pub workflow: ProfilePatch,
    pub task: ProfilePatch,
    pub attempt: ProfilePatch,
}

/// Whether the harness actually provides a capability. `Unknown` means the
/// harness neither confirms nor denies it: Central must publish that
/// honestly instead of assuming support.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CapabilitySupport {
    Supported,
    Unsupported,
    Unknown,
}

/// How Central enforces a capability. `Unknown` support is never silently
/// treated as enforced: it is published as `InstructionOnly` and requires
/// explicit user acceptance before dispatch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Enforcement {
    Enforced,
    InstructionOnly,
    Unsupported,
}

/// Policy failures for profile resolution. An unavailable model is an error,
/// never a silent substitution from `fallbacks`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProfileError {
    ModelUnavailable(String),
    CapabilityUnsupported(String),
    InvalidProfile(String),
}

impl fmt::Display for ProfileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ModelUnavailable(message) => write!(f, "model unavailable: {message}"),
            Self::CapabilityUnsupported(message) => {
                write!(f, "capability unsupported: {message}")
            }
            Self::InvalidProfile(message) => write!(f, "invalid profile: {message}"),
        }
    }
}

impl std::error::Error for ProfileError {}

/// Merge personal -> workflow -> task -> attempt (later layers win when set)
/// and recompute the content hash so the frozen attempt profile is
/// self-describing.
pub fn apply_layers(layers: &ProfileLayers) -> ResolvedProfile {
    let base = &layers.personal;
    let provider = layers
        .attempt
        .provider
        .clone()
        .or_else(|| layers.task.provider.clone())
        .or_else(|| layers.workflow.provider.clone())
        .unwrap_or_else(|| base.provider.clone());
    let model = layers
        .attempt
        .model
        .clone()
        .or_else(|| layers.task.model.clone())
        .or_else(|| layers.workflow.model.clone())
        .unwrap_or_else(|| base.model.clone());
    let effort = layers
        .attempt
        .effort
        .clone()
        .or_else(|| layers.task.effort.clone())
        .or_else(|| layers.workflow.effort.clone())
        .unwrap_or_else(|| base.effort.clone());
    let skills = layers
        .attempt
        .skills
        .clone()
        .or_else(|| layers.task.skills.clone())
        .or_else(|| layers.workflow.skills.clone())
        .unwrap_or_else(|| base.skills.clone());
    let subagents = layers
        .attempt
        .subagents
        .clone()
        .or_else(|| layers.task.subagents.clone())
        .or_else(|| layers.workflow.subagents.clone())
        .unwrap_or_else(|| base.subagents.clone());
    let permissions = layers
        .attempt
        .permissions
        .clone()
        .or_else(|| layers.task.permissions.clone())
        .or_else(|| layers.workflow.permissions.clone())
        .unwrap_or_else(|| base.permissions.clone());
    let fallbacks = layers
        .attempt
        .fallbacks
        .clone()
        .or_else(|| layers.task.fallbacks.clone())
        .or_else(|| layers.workflow.fallbacks.clone())
        .unwrap_or_else(|| base.fallbacks.clone());
    let hash = compute_profile_hash(
        &base.id,
        &provider,
        &model,
        &effort,
        &skills,
        &subagents,
        &permissions,
        &fallbacks,
    );
    ResolvedProfile {
        id: base.id,
        provider,
        model,
        effort,
        skills,
        subagents,
        permissions,
        fallbacks,
        hash,
    }
}

/// Canonical content hash for a resolved profile: SHA-256 over the canonical
/// JSON of every field except the hash itself.
#[allow(clippy::too_many_arguments)]
pub fn compute_profile_hash(
    id: &ProfileId,
    provider: &str,
    model: &str,
    effort: &str,
    skills: &[String],
    subagents: &[String],
    permissions: &[String],
    fallbacks: &[ProfileId],
) -> ContentHash {
    let payload = serde_json::json!({
        "id": id.to_string(),
        "provider": provider,
        "model": model,
        "effort": effort,
        "skills": skills,
        "subagents": subagents,
        "permissions": permissions,
        "fallbacks": fallbacks.iter().map(ToString::to_string).collect::<Vec<_>>(),
    });
    let canonical = serde_json::to_string(&payload).expect("profile hash payload serializes");
    let digest = Sha256::digest(canonical.as_bytes());
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        hex.push_str(&format!("{byte:02x}"));
    }
    ContentHash::from_hex(hex)
}
