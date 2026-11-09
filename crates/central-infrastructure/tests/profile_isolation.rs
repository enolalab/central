//! P2 Task 2: profile isolation (TDD gate).
//!
//! Profiles resolve personal -> workflow -> task -> attempt and freeze when
//! the attempt starts. A later task edit must never rewrite a frozen attempt,
//! an unavailable model must fail instead of falling back silently, and
//! concurrent A/B resolutions must never mix model/config. Unobservable skill
//! hashes are published as an explicit `None`, never fabricated.

use central_application::contracts::Capabilities;
use central_application::profiles::{
    enforcement_for, may_dispatch, observe_skills, resolve_profile, CapabilityReport,
    FrozenAttemptProfile, SkillHasher,
};
use central_domain::profile::{
    CapabilitySupport, Enforcement, ProfileError, ProfileLayers, ProfilePatch,
};
use central_domain::{AttemptId, ContentHash, ProfileId, ResolvedProfile};

fn personal_profile() -> ResolvedProfile {
    ResolvedProfile {
        id: ProfileId::from_u128(0xA1),
        provider: "codex".to_string(),
        model: "gpt-6-luna".to_string(),
        effort: "low".to_string(),
        skills: vec!["review".to_string()],
        subagents: vec![],
        permissions: vec!["read".to_string()],
        fallbacks: vec![],
        hash: ContentHash::from_hex("0".repeat(64)),
    }
}

fn layers_with_task_model(model: &str) -> ProfileLayers {
    ProfileLayers {
        personal: personal_profile(),
        workflow: ProfilePatch::default(),
        task: ProfilePatch {
            model: Some(model.to_string()),
            ..Default::default()
        },
        attempt: ProfilePatch::default(),
    }
}

fn capabilities_for(models: &[&str]) -> Capabilities {
    let mut features = vec![
        "initialize".to_string(),
        "thread/start".to_string(),
        "thread/resume".to_string(),
        "thread/read".to_string(),
    ];
    for model in models {
        features.push(format!("model:{model}"));
    }
    Capabilities {
        provider: "codex".to_string(),
        supports_resume: true,
        features,
    }
}

#[test]
fn task_override_freezes_at_attempt() {
    let layers = layers_with_task_model("gpt-6-vega");
    let caps = capabilities_for(&["gpt-6-luna", "gpt-6-vega"]);
    let resolved = resolve_profile(&layers, &caps).expect("advertised model resolves");
    assert_eq!(resolved.model, "gpt-6-vega");
    assert_eq!(resolved.effort, "low");

    // Freeze when the attempt starts.
    let attempt = FrozenAttemptProfile::freeze(AttemptId::from_u128(7), resolved);
    let original = attempt.profile.hash.clone();

    // A later task-layer edit resolves differently but must not rewrite
    // the frozen attempt profile.
    let later = layers_with_task_model("gpt-6-nova");
    let caps2 = capabilities_for(&["gpt-6-luna", "gpt-6-vega", "gpt-6-nova"]);
    let re_resolved = resolve_profile(&later, &caps2).expect("nova resolves once advertised");
    assert_ne!(re_resolved.hash, original);
    assert_eq!(attempt.profile.hash, original);
}

#[test]
fn model_unavailable_no_silent_fallback() {
    let mut layers = layers_with_task_model("gpt-9-ghost");
    // Even with fallbacks recorded, resolution must fail loudly, never
    // silently substitute one of them.
    layers.personal.fallbacks = vec![ProfileId::from_u128(0xF1)];
    let caps = capabilities_for(&["gpt-6-luna"]);
    let unsupported_model = resolve_profile(&layers, &caps);
    assert!(unsupported_model.is_err());
    match unsupported_model {
        Err(ProfileError::ModelUnavailable(_)) => {}
        other => panic!("expected ModelUnavailable, got {other:?}"),
    }
}

#[test]
fn concurrent_ab_profiles_never_mix() {
    std::thread::scope(|scope| {
        let worker_a = scope.spawn(|| {
            let layers = ProfileLayers {
                personal: personal_profile(),
                workflow: ProfilePatch {
                    effort: Some("high".to_string()),
                    ..Default::default()
                },
                task: ProfilePatch {
                    model: Some("profile-a-model".to_string()),
                    ..Default::default()
                },
                attempt: ProfilePatch::default(),
            };
            resolve_profile(&layers, &capabilities_for(&["profile-a-model"]))
                .expect("profile A resolves")
        });
        let worker_b = scope.spawn(|| {
            let layers = ProfileLayers {
                personal: personal_profile(),
                workflow: ProfilePatch::default(),
                task: ProfilePatch {
                    model: Some("profile-b-model".to_string()),
                    skills: Some(vec!["deep-research".to_string()]),
                    ..Default::default()
                },
                attempt: ProfilePatch::default(),
            };
            resolve_profile(&layers, &capabilities_for(&["profile-b-model"]))
                .expect("profile B resolves")
        });
        let resolved_a = worker_a.join().expect("worker A joins");
        let resolved_b = worker_b.join().expect("worker B joins");
        assert_eq!(resolved_a.model, "profile-a-model");
        assert_eq!(resolved_b.model, "profile-b-model");
        assert_eq!(resolved_a.effort, "high");
        assert_eq!(resolved_b.effort, "low");
        assert_eq!(resolved_b.skills, vec!["deep-research".to_string()]);
        assert_ne!(resolved_a.hash, resolved_b.hash);
    });
}

struct UnobservableHasher;

impl SkillHasher for UnobservableHasher {
    fn hash_skill(&self, _name: &str) -> Option<ContentHash> {
        None
    }
}

#[test]
fn unobservable_skill_hash_visible() {
    let profile = personal_profile();
    assert_eq!(profile.skills, vec!["review".to_string()]);
    let observations = observe_skills(&profile, &UnobservableHasher);
    assert_eq!(observations.len(), 1);
    let skill = &observations[0];
    assert_eq!(skill.name, "review");
    // Unobservable is published as an explicit unknown, never a fabricated hash.
    assert_eq!(skill.observed_hash, None);
}

#[test]
fn unenforceable_capability_needs_acceptance() {
    assert_eq!(
        enforcement_for(CapabilitySupport::Supported),
        Enforcement::Enforced
    );
    assert_eq!(
        enforcement_for(CapabilitySupport::Unknown),
        Enforcement::InstructionOnly
    );
    assert_eq!(
        enforcement_for(CapabilitySupport::Unsupported),
        Enforcement::Unsupported
    );

    let mut report = CapabilityReport::publish("nested-job", CapabilitySupport::Unknown);
    assert_eq!(report.enforcement, Enforcement::InstructionOnly);
    // Unknown support never dispatches silently: explicit user acceptance first.
    assert!(!may_dispatch(&report));
    report.accept_instruction_only();
    assert!(may_dispatch(&report));

    let enforced = CapabilityReport::publish("kill-on-close", CapabilitySupport::Supported);
    assert!(may_dispatch(&enforced));
    let denied = CapabilityReport::publish("remote-worker", CapabilitySupport::Unsupported);
    assert!(!may_dispatch(&denied));
    // Accepting what is unsupported (or already enforced) changes nothing.
    let mut denied = denied;
    denied.accept_instruction_only();
    assert!(!may_dispatch(&denied));
}
