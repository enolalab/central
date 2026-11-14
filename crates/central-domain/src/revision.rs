//! Plan revision impact, stale outputs and reuse assessment (P3 Task 4).
//!
//! A revision proposal is immutable: [`assess_impact`] diffs the active
//! plan against the proposed one — explicitly changed nodes plus the
//! transitive data descendants that read their outputs — and the
//! leader-supplied semantic nodes are merged with [`ImpactSet::expanded_with`],
//! which can only grow the machine impact, never shrink it. An
//! order-only change (a `depends_on` reorder with every data input
//! untouched) seeds no data descendants, so unrelated data evidence
//! stays valid.
//!
//! Results produced by attempts bound to a superseded revision are
//! quarantined, never relabeled: [`StaleCandidate`] keeps the original
//! plan binding forever, and [`assess_reuse`] routes it to one
//! [`ReuseDecision`]. Unchanged evidence is reused only when the input,
//! candidate, criteria and environment bindings all still match.
//!
//! Pure: no IO, no mutation, deterministic for given inputs.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{
    criteria_hash_for, plan_hash, validate_plan, ActorId, AttemptId, ContentHash, DomainError,
    PlanDocumentV1, PlanHash, PlanTaskV1, TaskId, Timestamp,
};

/// What one revision touches: the tasks whose definition changed, the
/// tasks that transitively read their data outputs, and the
/// leader-supplied semantic nodes (which only ever grow this set).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImpactSet {
    /// Tasks added, removed, or whose definition differs.
    pub changed: BTreeSet<TaskId>,
    /// Tasks reachable from data-relevant changes through
    /// `data_depends_on` edges. Order-only changes seed nothing here.
    pub data_descendants: BTreeSet<TaskId>,
    /// Leader-supplied semantic nodes. Merged, never subtracted.
    pub semantic: BTreeSet<TaskId>,
}

impl ImpactSet {
    /// Every task the revision touches, in stable order.
    pub fn impacted(&self) -> BTreeSet<TaskId> {
        self.changed
            .iter()
            .chain(&self.data_descendants)
            .chain(&self.semantic)
            .copied()
            .collect()
    }

    /// Merge leader-supplied semantic nodes. The merge is additive:
    /// machine-known entries are preserved, so the result always
    /// covers at least the previous impact.
    pub fn expanded_with(&self, semantic: BTreeSet<TaskId>) -> ImpactSet {
        let mut merged = self.clone();
        merged.semantic.extend(semantic);
        merged
    }
}

/// Diff the active plan against a proposed revision.
///
/// Every task present on exactly one side, or whose canonical task
/// JSON differs, lands in `changed`. A change that only reorders
/// `depends_on` is order-only and seeds no data descendants; any other
/// difference (objective, inputs, data edges, outputs, criteria,
/// evidence, permissions, profile, repair budget, approvals, handover,
/// kind) is data-relevant and its transitive `data_depends_on`
/// followers land in `data_descendants`. Both documents are validated
/// first: an unapprovable revision has no meaningful impact.
pub fn assess_impact(old: &PlanDocumentV1, new: &PlanDocumentV1) -> Result<ImpactSet, DomainError> {
    validate_plan(old).map_err(|error| {
        DomainError::InvalidPlan(format!("cannot diff from an invalid active plan: {error}"))
    })?;
    validate_plan(new).map_err(|error| {
        DomainError::InvalidPlan(format!("cannot diff to an invalid revised plan: {error}"))
    })?;

    let old_tasks: BTreeMap<TaskId, &PlanTaskV1> =
        old.tasks.iter().map(|task| (task.id, task)).collect();
    let new_tasks: BTreeMap<TaskId, &PlanTaskV1> =
        new.tasks.iter().map(|task| (task.id, task)).collect();

    let mut changed = BTreeSet::new();
    let mut data_seeds = BTreeSet::new();
    for id in old_tasks.keys().chain(new_tasks.keys()) {
        match (old_tasks.get(id), new_tasks.get(id)) {
            (Some(before), Some(after)) => {
                if task_json(before) != task_json(after) {
                    changed.insert(*id);
                    if !is_order_only(before, after) {
                        data_seeds.insert(*id);
                    }
                }
            }
            // Added or removed tasks always count as changed, and an
            // added task seeds data impact for its followers.
            (None, Some(_)) => {
                changed.insert(*id);
                data_seeds.insert(*id);
            }
            (Some(_), None) => {
                changed.insert(*id);
            }
            (None, None) => {}
        }
    }

    let data_descendants = data_closure(new, &data_seeds);
    Ok(ImpactSet {
        changed,
        data_descendants,
        semantic: BTreeSet::new(),
    })
}

/// Canonical JSON of one plan task for change comparison.
fn task_json(task: &PlanTaskV1) -> serde_json::Value {
    serde_json::to_value(task).expect("plan task serialization is infallible")
}

/// Whether two definitions of one task differ only in `depends_on`.
/// Such a change never moves data evidence: no input, output,
/// criteria or data edge changed.
fn is_order_only(before: &PlanTaskV1, after: &PlanTaskV1) -> bool {
    if before.depends_on == after.depends_on {
        return false;
    }
    let mut left = task_json(before);
    let mut right = task_json(after);
    // `rename_all = "camelCase"` serializes the field as `dependsOn`.
    if let Some(object) = left.as_object_mut() {
        object.insert("dependsOn".to_string(), serde_json::json!([]));
    }
    if let Some(object) = right.as_object_mut() {
        object.insert("dependsOn".to_string(), serde_json::json!([]));
    }
    left == right
}

/// Transitive closure of `data_depends_on` followers of the seeds over
/// the revised plan. The seeds themselves stay in `changed`; this set
/// names the downstream tasks whose data inputs moved.
fn data_closure(new: &PlanDocumentV1, seeds: &BTreeSet<TaskId>) -> BTreeSet<TaskId> {
    let mut followers: BTreeMap<TaskId, Vec<TaskId>> = BTreeMap::new();
    for task in &new.tasks {
        for predecessor in &task.data_depends_on {
            followers.entry(*predecessor).or_default().push(task.id);
        }
    }
    let mut visited = BTreeSet::new();
    let mut frontier: Vec<TaskId> = seeds.iter().copied().collect();
    while let Some(node) = frontier.pop() {
        if let Some(next) = followers.get(&node) {
            for follower in next {
                if visited.insert(*follower) {
                    frontier.push(*follower);
                }
            }
        }
    }
    // A seed that is also downstream of another seed is both changed
    // and a data descendant; the closure keeps it.
    visited
}

/// What a quarantined stale result may become. `NeedsAssessment` is
/// the initial state every quarantined result starts in; [`assess_reuse`]
/// concludes with one of the other four.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReuseDecision {
    /// Recorded with original provenance, still awaiting assessment.
    NeedsAssessment,
    /// Bindings moved: schedule verification of the old candidate
    /// under the current bindings before any reuse.
    VerifyAgain,
    /// The candidate failed and repair rounds remain: chain a repair
    /// attempt from the assessed predecessor.
    Repair,
    /// The candidate has no future under the active revision.
    Discard,
    /// Every binding still matches: the old evidence stands without
    /// re-verification.
    ReuseUnchangedEvidence,
}

/// The exact bindings a stale candidate was sealed under, and the
/// current bindings it is assessed against.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReuseBindings {
    pub input_hash: ContentHash,
    pub candidate_hash: ContentHash,
    pub criteria_hash: ContentHash,
    pub environment: String,
}

/// A result produced by an attempt bound to a superseded revision,
/// quarantined with its original provenance. `bound_plan` is never
/// rewritten: a late result can never become new-plan evidence by
/// relabeling.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StaleCandidate {
    pub attempt_id: AttemptId,
    pub task_id: TaskId,
    /// The revision the producing attempt was bound to. Immutable.
    pub bound_plan: PlanHash,
    pub input_hash: ContentHash,
    pub candidate_hash: ContentHash,
    pub criteria_hash: ContentHash,
    pub environment: String,
    /// Starts as [`ReuseDecision::NeedsAssessment`]; [`Command::AssessReuse`]
    /// concludes it.
    pub reuse: ReuseDecision,
    /// The current bindings the concluding assessment was made against.
    #[serde(default)]
    pub verified_against: Option<ReuseBindings>,
}

impl StaleCandidate {
    /// Whether every binding still matches: the only state in which
    /// [`ReuseDecision::ReuseUnchangedEvidence`] is honest.
    pub fn matches(&self, current: &ReuseBindings) -> bool {
        self.input_hash == current.input_hash
            && self.candidate_hash == current.candidate_hash
            && self.criteria_hash == current.criteria_hash
            && self.environment == current.environment
    }
}

/// Route one quarantined candidate to its reuse decision.
///
/// A removed task has no future (`Discard` even when the bytes match:
/// there is nothing to reuse them for). Otherwise every binding must
/// still match for `ReuseUnchangedEvidence`; a previously failed
/// candidate with bindings moved chains a `Repair`, and anything else
/// schedules verification first (`VerifyAgain`), never a silent accept
/// and never a whole re-implementation up front.
pub fn assess_reuse(
    candidate: &StaleCandidate,
    current: &ReuseBindings,
    task_retained: bool,
    failed_before: bool,
) -> ReuseDecision {
    if !task_retained {
        return ReuseDecision::Discard;
    }
    if candidate.matches(current) {
        return ReuseDecision::ReuseUnchangedEvidence;
    }
    if failed_before {
        return ReuseDecision::Repair;
    }
    ReuseDecision::VerifyAgain
}

/// What a verification-criteria change means for requirements,
/// verdicts and follow-up work.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CriteriaImpact {
    /// Tasks whose verify spec differs between the plans, in stable order.
    pub changed_tasks: Vec<TaskId>,
    /// `changed_tasks` plus their transitive data descendants: every
    /// task that must re-verify under the new criteria.
    pub reverify_tasks: Vec<TaskId>,
    /// Which requirements the change touches.
    pub requirement_impact: String,
    /// What happens to verdicts sealed under the old criteria.
    pub verdict_impact: String,
}

/// Describe the criteria delta between the active plan and a revision.
/// Sealed verdicts name an exact criteria hash, so any task whose
/// spec changed — and every data task downstream of it — must
/// re-verify; the strings name the requirements and the verdict
/// impact the proposer's explanation must cover.
pub fn describe_criteria_impact(
    old: &PlanDocumentV1,
    new: &PlanDocumentV1,
) -> Result<CriteriaImpact, DomainError> {
    validate_plan(old).map_err(|error| {
        DomainError::InvalidPlan(format!("cannot diff from an invalid active plan: {error}"))
    })?;
    validate_plan(new).map_err(|error| {
        DomainError::InvalidPlan(format!("cannot diff to an invalid revised plan: {error}"))
    })?;

    let old_specs: BTreeMap<TaskId, &PlanTaskV1> =
        old.tasks.iter().map(|task| (task.id, task)).collect();
    let mut changed_tasks = Vec::new();
    let mut details = Vec::new();
    for task in &new.tasks {
        if let Some(before) = old_specs.get(&task.id) {
            let before_hash = before
                .verify
                .as_ref()
                .map(criteria_hash_for)
                .expect("validated plans carry verify specs");
            let after_hash = task
                .verify
                .as_ref()
                .map(criteria_hash_for)
                .expect("validated plans carry verify specs");
            if before_hash != after_hash {
                changed_tasks.push(task.id);
                details.push(format!(
                    "task {id}: criteria {before} -> {after}",
                    id = task.id,
                    before = short_hash(before_hash.as_str()),
                    after = short_hash(after_hash.as_str()),
                ));
            }
        }
    }

    let seeds: BTreeSet<TaskId> = changed_tasks.iter().copied().collect();
    let mut reverify: BTreeSet<TaskId> = seeds.clone();
    reverify.extend(data_closure(new, &seeds));
    let reverify_tasks: Vec<TaskId> = reverify.into_iter().collect();

    let requirement_impact = if details.is_empty() {
        "no verification criteria changed; requirements are untouched".to_string()
    } else {
        format!(
            "verification criteria changed for {} task(s): {}",
            details.len(),
            details.join("; ")
        )
    };
    let verdict_impact = if changed_tasks.is_empty() {
        "sealed verdicts still bind their criteria; nothing goes stale".to_string()
    } else {
        format!(
            "verdicts sealed under the previous criteria no longer bind {} task(s); {} task(s) must re-verify before their evidence is reused",
            changed_tasks.len(),
            reverify_tasks.len(),
        )
    };
    Ok(CriteriaImpact {
        changed_tasks,
        reverify_tasks,
        requirement_impact,
        verdict_impact,
    })
}

/// Whether a revision changes any verification criteria.
pub fn criteria_changed(old: &PlanDocumentV1, new: &PlanDocumentV1) -> Result<bool, DomainError> {
    Ok(!describe_criteria_impact(old, new)?.changed_tasks.is_empty())
}

fn short_hash(hash: &str) -> &str {
    hash.get(..12).unwrap_or(hash)
}

/// One immutable revision proposal: the full replacement document,
/// the reason it exists, the criteria explanation (mandatory when the
/// criteria move), the computed impact and the leader's semantic nodes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingRevision {
    /// The active plan hash this revision was diffed against.
    pub base_plan: PlanHash,
    pub plan_hash: PlanHash,
    pub document: PlanDocumentV1,
    pub reason: String,
    pub criteria_explanation: Option<String>,
    pub impact: ImpactSet,
    pub semantic_affected: BTreeSet<TaskId>,
    pub proposed_by: ActorId,
    pub proposed_at: Timestamp,
}

/// Canonical hash of the active plan for cheap identity checks.
pub fn active_plan_hash(plan: &PlanDocumentV1) -> PlanHash {
    plan_hash(plan)
}

pub(crate) fn content_hash_of(bytes: &[u8]) -> ContentHash {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(bytes);
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        hex.push_str(&format!("{byte:02x}"));
    }
    ContentHash::from_hex(hex)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(id: u128, order: Vec<u128>, data: Vec<u128>) -> PlanTaskV1 {
        use crate::{ProfileId, TaskKind, VerifySpec};
        PlanTaskV1 {
            id: TaskId::from_u128(id),
            kind: TaskKind::Implementation,
            objective: format!("implement task {id}"),
            inputs: Vec::new(),
            scope: Vec::new(),
            out_of_scope: Vec::new(),
            depends_on: order.into_iter().map(TaskId::from_u128).collect(),
            data_depends_on: data.into_iter().map(TaskId::from_u128).collect(),
            outputs: vec![format!("output-{id}")],
            verify: Some(VerifySpec {
                method: "run the approved check".to_string(),
                criteria: vec!["the approved check exits zero".to_string()],
            }),
            evidence: Vec::new(),
            profile_id: None::<ProfileId>,
            permissions: Vec::new(),
            max_repair_rounds: 1,
            requires_approval: Vec::new(),
            handover: String::new(),
            integration: None,
        }
    }

    fn plan(tasks: Vec<PlanTaskV1>) -> PlanDocumentV1 {
        PlanDocumentV1 {
            version: 1,
            goal: "ship".to_string(),
            tasks,
        }
    }

    #[test]
    fn impact_names_changed_nodes_and_data_descendants() {
        let a = TaskId::from_u128(1);
        let b = TaskId::from_u128(2);
        let c = TaskId::from_u128(3);
        let old = plan(vec![
            task(1, vec![], vec![]),
            task(2, vec![], vec![]),
            task(3, vec![1], vec![1]),
        ]);
        let mut changed_a = task(1, vec![], vec![]);
        changed_a.objective = "implement task 1 with the new contract".to_string();
        let new = plan(vec![
            changed_a,
            task(2, vec![], vec![]),
            task(3, vec![1], vec![1]),
        ]);

        let impact = assess_impact(&old, &new).expect("impact");
        assert!(impact.changed.contains(&a));
        assert!(!impact.changed.contains(&b));
        assert!(impact.data_descendants.contains(&c));
        assert!(!impact.impacted().contains(&b));
    }

    #[test]
    fn order_only_changes_seed_no_data_descendants() {
        let old = plan(vec![
            task(1, vec![], vec![]),
            task(2, vec![], vec![]),
            task(3, vec![1], vec![1]),
        ]);
        // Only the order edge of c moves; every data input is untouched.
        let new = plan(vec![
            task(1, vec![], vec![]),
            task(2, vec![], vec![]),
            task(3, vec![1, 2], vec![1]),
        ]);

        let impact = assess_impact(&old, &new).expect("impact");
        assert!(impact.changed.contains(&TaskId::from_u128(3)));
        assert!(impact.data_descendants.is_empty());
    }

    #[test]
    fn semantic_merge_only_expands() {
        let b = TaskId::from_u128(2);
        let impact = ImpactSet {
            changed: [TaskId::from_u128(1)].into_iter().collect(),
            data_descendants: [TaskId::from_u128(3)].into_iter().collect(),
            semantic: BTreeSet::new(),
        };
        let expanded = impact.expanded_with([b].into_iter().collect());
        assert!(expanded.impacted().is_superset(&impact.impacted()));
        assert!(expanded.impacted().contains(&b));
        let same = impact.expanded_with(BTreeSet::new());
        assert_eq!(same.impacted(), impact.impacted());
    }

    #[test]
    fn reuse_matrix_routes_every_decision() {
        let candidate = StaleCandidate {
            attempt_id: AttemptId::from_u128(9),
            task_id: TaskId::from_u128(1),
            bound_plan: PlanHash::from_hex("aa"),
            input_hash: ContentHash::from_hex("in"),
            candidate_hash: ContentHash::from_hex("cand"),
            criteria_hash: ContentHash::from_hex("crit"),
            environment: "env-1".to_string(),
            reuse: ReuseDecision::NeedsAssessment,
            verified_against: None,
        };
        let same = ReuseBindings {
            input_hash: ContentHash::from_hex("in"),
            candidate_hash: ContentHash::from_hex("cand"),
            criteria_hash: ContentHash::from_hex("crit"),
            environment: "env-1".to_string(),
        };
        let moved = ReuseBindings {
            input_hash: ContentHash::from_hex("in2"),
            ..same.clone()
        };
        assert_eq!(
            assess_reuse(&candidate, &same, true, false),
            ReuseDecision::ReuseUnchangedEvidence
        );
        assert_eq!(
            assess_reuse(&candidate, &moved, true, false),
            ReuseDecision::VerifyAgain
        );
        assert_eq!(
            assess_reuse(&candidate, &moved, true, true),
            ReuseDecision::Repair
        );
        assert_eq!(
            assess_reuse(&candidate, &moved, false, false),
            ReuseDecision::Discard
        );
        assert!(candidate.matches(&same));
        assert!(!candidate.matches(&moved));
    }

    #[test]
    fn criteria_impact_lists_reverify_tasks() {
        let old = plan(vec![task(1, vec![], vec![]), task(3, vec![1], vec![1])]);
        let mut changed_a = task(1, vec![], vec![]);
        changed_a
            .verify
            .as_mut()
            .expect("verify")
            .criteria
            .push("no warnings".to_string());
        let new = plan(vec![changed_a, task(3, vec![1], vec![1])]);

        let impact = describe_criteria_impact(&old, &new).expect("impact");
        assert_eq!(impact.changed_tasks, vec![TaskId::from_u128(1)]);
        assert!(impact.reverify_tasks.contains(&TaskId::from_u128(1)));
        assert!(impact.reverify_tasks.contains(&TaskId::from_u128(3)));
        assert!(!impact.requirement_impact.is_empty());
        assert!(!impact.verdict_impact.is_empty());
        assert!(criteria_changed(&old, &new).expect("changed"));
        assert!(!criteria_changed(&old, &old).expect("unchanged"));
    }
}
