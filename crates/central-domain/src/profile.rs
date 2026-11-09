use serde::{Deserialize, Serialize};

use crate::{ContentHash, ProfileId};

/// Resolved agent profile. Policy validation lives in a later task; this is
/// the single shared data type, not a second application-level copy.
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
