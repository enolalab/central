//! Profile fallback on quota/auth refusal (P3 Task 3).
//!
//! A quota or auth refusal never retries: it walks the user-granted
//! fallback chain, confirms the old attempt stopped, and opens one new
//! attempt on the first candidate that meets harness capability without
//! raising permissions — then records the decision durably as the user
//! notification. No separate budget exists for fallbacks: the new
//! attempt passes the same shared admission gate, so a held or
//! exhausted budget opens zero fallback attempts.

use std::collections::BTreeMap;
use std::sync::Mutex;

use central_domain::{Failure, ProfileId, ResolvedProfile, TaskId};

use crate::contracts::Capabilities;

/// Chain gate: whether this failure may consult the user-granted
/// fallback chain at all. Only quota/auth failures with a non-empty
/// chain and a matching harness provider may; every other failure
/// parks without touching the chain.
pub fn choose_fallback(
    current: &ResolvedProfile,
    failure: &Failure,
    capabilities: &Capabilities,
) -> Option<ProfileId> {
    match failure {
        Failure::QuotaExceeded | Failure::Unauthorized => {
            if current.fallbacks.is_empty() {
                return None;
            }
            if current.provider != capabilities.provider {
                return None;
            }
            current.fallbacks.first().copied()
        }
        _ => None,
    }
}

/// Whether one candidate profile may serve the fallback: the harness
/// must still advertise its model, and its permissions must be a subset
/// of the current profile's — a fallback never raises permissions and
/// never loosens the accepted enforcement.
pub fn candidate_allowed(
    candidate: &ResolvedProfile,
    current: &ResolvedProfile,
    capabilities: &Capabilities,
) -> bool {
    if candidate.provider != capabilities.provider {
        return false;
    }
    let advertised = format!("model:{}", candidate.model);
    if !capabilities
        .features
        .iter()
        .any(|feature| feature == &advertised)
    {
        return false;
    }
    candidate
        .permissions
        .iter()
        .all(|permission| current.permissions.contains(permission))
}

/// Select the fallback profile to continue on: the first chain entry
/// that resolves in the registry and passes capability and permission
/// checks. Returns the owned profile so dispatch freezes exactly this
/// resolution for the new attempt.
pub fn select_fallback(
    current: &ResolvedProfile,
    registry: &BTreeMap<ProfileId, ResolvedProfile>,
    capabilities: &Capabilities,
) -> Option<ResolvedProfile> {
    current
        .fallbacks
        .iter()
        .filter_map(|id| registry.get(id))
        .find(|candidate| candidate_allowed(candidate, current, capabilities))
        .cloned()
}

/// Per-task frozen fallback profiles chosen by quota/auth handling. The
/// scheduler consults these before the base profile resolver, so the
/// new attempt dispatches on the fallback without rewriting the
/// approved task definition.
#[derive(Default)]
pub struct FallbackOverrides {
    inner: Mutex<BTreeMap<TaskId, ResolvedProfile>>,
    registry: Mutex<BTreeMap<ProfileId, ResolvedProfile>>,
}

impl FallbackOverrides {
    pub fn new(registry: BTreeMap<ProfileId, ResolvedProfile>) -> Self {
        Self {
            inner: Mutex::new(BTreeMap::new()),
            registry: Mutex::new(registry),
        }
    }

    pub fn get(&self, task: TaskId) -> Option<ResolvedProfile> {
        self.inner.lock().expect("lock").get(&task).cloned()
    }

    pub fn set(&self, task: TaskId, profile: ResolvedProfile) {
        self.inner.lock().expect("lock").insert(task, profile);
    }

    pub fn lookup(&self, id: ProfileId) -> Option<ResolvedProfile> {
        self.registry.lock().expect("lock").get(&id).cloned()
    }

    pub fn registry_snapshot(&self) -> BTreeMap<ProfileId, ResolvedProfile> {
        self.registry.lock().expect("lock").clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use central_domain::ContentHash;

    fn profile(id: u128, model: &str, permissions: &[&str], fallbacks: &[u128]) -> ResolvedProfile {
        ResolvedProfile {
            id: ProfileId::from_u128(id),
            provider: "fake".to_string(),
            model: model.to_string(),
            effort: "low".to_string(),
            skills: Vec::new(),
            subagents: Vec::new(),
            permissions: permissions.iter().map(|p| p.to_string()).collect(),
            fallbacks: fallbacks.iter().map(|f| ProfileId::from_u128(*f)).collect(),
            hash: ContentHash::from_hex("profile"),
        }
    }

    fn capabilities() -> Capabilities {
        Capabilities {
            provider: "fake".to_string(),
            supports_resume: true,
            features: vec![
                "model:fake-model".to_string(),
                "model:fake-small".to_string(),
            ],
        }
    }

    #[test]
    fn chain_gate_only_opens_for_quota_or_auth() {
        let current = profile(1, "fake-model", &["read"], &[2]);
        assert_eq!(
            choose_fallback(&current, &Failure::QuotaExceeded, &capabilities()),
            Some(ProfileId::from_u128(2))
        );
        assert_eq!(
            choose_fallback(&current, &Failure::Unauthorized, &capabilities()),
            Some(ProfileId::from_u128(2))
        );
        assert_eq!(
            choose_fallback(&current, &Failure::OutcomeUnknown, &capabilities()),
            None
        );
        assert_eq!(
            choose_fallback(&current, &Failure::DefinitelyNotSent, &capabilities()),
            None
        );
        assert_eq!(
            choose_fallback(&current, &Failure::Refused, &capabilities()),
            None
        );
        let chainless = profile(1, "fake-model", &["read"], &[]);
        assert_eq!(
            choose_fallback(&chainless, &Failure::QuotaExceeded, &capabilities()),
            None
        );
    }

    #[test]
    fn selection_never_raises_permissions_or_capability() {
        let current = profile(1, "fake-model", &["read", "write"], &[9, 3, 2]);
        let registry = BTreeMap::from([
            (
                ProfileId::from_u128(9),
                profile(9, "fake-small", &["read", "admin"], &[]),
            ),
            (
                ProfileId::from_u128(3),
                profile(3, "unadvertised-model", &["read"], &[]),
            ),
            (
                ProfileId::from_u128(2),
                profile(2, "fake-small", &["read"], &[]),
            ),
        ]);
        let chosen = select_fallback(&current, &registry, &capabilities()).expect("selects");
        assert_eq!(chosen.id, ProfileId::from_u128(2));
    }
}
