//! Incremental integration policy (P3 Task 5, application side).
//!
//! Pure policy with no Git, no subprocesses and no IO: serialization of
//! approved integration nodes per run, the baseline/affected/acceptance
//! check plan from the approved plan, the compare-and-swap promotion gate,
//! intent-before-mutation ordering, and receipt recovery from actual ref
//! state. The [`WorkspaceManager`] port performs the mechanics; this module
//! decides what is allowed.
//!
//! [`WorkspaceManager`]: crate::ports::workspace::WorkspaceManager

use std::collections::HashMap;

use central_domain::RunId;
use serde::{Deserialize, Serialize};

use crate::contracts::AppError;
use crate::ports::workspace::{PromotionIntent, PromotionReceipt, VerificationSummary};

/// One approved check invocation, in plan order. The infrastructure runner
/// executes exactly this program+argv against the approved plan hash.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlannedCheck {
    pub program: String,
    pub args: Vec<String>,
}

/// The check plan for one integration, from the approved plan: baseline
/// checks first, then the checks affected by the merged candidates, then
/// the final acceptance of the whole run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntegrationPlan {
    pub baseline: Vec<PlannedCheck>,
    pub affected: Vec<PlannedCheck>,
    pub acceptance: Vec<PlannedCheck>,
}

impl IntegrationPlan {
    /// Required checks in execution order: baseline, affected, acceptance.
    pub fn required(&self) -> Vec<&PlannedCheck> {
        self.baseline
            .iter()
            .chain(self.affected.iter())
            .chain(self.acceptance.iter())
            .collect()
    }
}

/// The observed outcome of one planned check, in `required` order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckOutcome {
    pub exit_code: i32,
    pub timed_out: bool,
}

/// What the ordered outcomes mean for the integration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IntegrationVerdict {
    /// Every required check exited zero without timing out.
    Pass,
    /// At least one required check failed or timed out; the good ref stays.
    Fail(String),
    /// Fewer outcomes than required checks; more evidence is needed.
    Incomplete,
}

/// Judge ordered outcomes against the plan. Evidence is never a verdict by
/// itself: only this binding of plan, order and outcomes promotes.
pub fn judge_integration(plan: &IntegrationPlan, outcomes: &[CheckOutcome]) -> IntegrationVerdict {
    let required = plan.required();
    if outcomes.len() < required.len() {
        return IntegrationVerdict::Incomplete;
    }
    for (index, outcome) in outcomes.iter().enumerate().take(required.len()) {
        if outcome.timed_out || outcome.exit_code != 0 {
            return IntegrationVerdict::Fail(format!(
                "check {} of {} failed (exit {}, timed out {})",
                index + 1,
                required.len(),
                outcome.exit_code,
                outcome.timed_out
            ));
        }
    }
    IntegrationVerdict::Pass
}

/// Fold a judged verdict into the promotion summary the CAS gate requires.
pub fn summarize_verdict(
    plan: &IntegrationPlan,
    outcomes: &[CheckOutcome],
    evidence: Vec<String>,
) -> VerificationSummary {
    let boundary = |from: usize, len: usize| {
        outcomes
            .iter()
            .skip(from)
            .take(len)
            .all(|o| !o.timed_out && o.exit_code == 0)
    };
    let baseline_ok = boundary(0, plan.baseline.len());
    let affected_ok = boundary(plan.baseline.len(), plan.affected.len());
    let acceptance_ok = boundary(
        plan.baseline.len() + plan.affected.len(),
        plan.acceptance.len(),
    );
    let passed = matches!(judge_integration(plan, outcomes), IntegrationVerdict::Pass);
    VerificationSummary {
        passed: passed && baseline_ok && affected_ok && acceptance_ok,
        baseline_ok,
        affected_ok,
        acceptance_ok,
        evidence,
    }
}

/// One node queued for integration on a run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueuedIntegration {
    pub node: String,
    pub candidates: Vec<String>,
}

/// Approved integration nodes, serialized per run: at most one in-flight
/// integration per run. A second submission while one is active conflicts
/// instead of interleaving; old workers are never rebased onto new nodes.
#[derive(Clone, Debug, Default)]
pub struct IntegrationQueue {
    in_flight: HashMap<RunId, QueuedIntegration>,
}

impl IntegrationQueue {
    pub fn new() -> Self {
        Self::default()
    }

    /// Claim the run's integration slot. A live claim blocks any other node
    /// on the same run; other runs are unaffected.
    pub fn submit(&mut self, run: RunId, node: QueuedIntegration) -> Result<(), AppError> {
        if self.in_flight.contains_key(&run) {
            return Err(AppError::Conflict(format!(
                "run {run} already has an in-flight integration; nodes serialize per run"
            )));
        }
        self.in_flight.insert(run, node);
        Ok(())
    }

    /// Release the run's slot after promotion or abandonment.
    pub fn complete(&mut self, run: RunId) {
        self.in_flight.remove(&run);
    }

    /// The run's live node, if any.
    pub fn in_flight(&self, run: RunId) -> Option<&QueuedIntegration> {
        self.in_flight.get(&run)
    }

    /// True when the run has no live integration.
    pub fn is_idle(&self, run: RunId) -> bool {
        !self.in_flight.contains_key(&run)
    }
}

/// Where an over-scope integration conflict goes: the leader proposes a way
/// forward. Central never creates a conflict-resolution task outside the
/// approved plan to settle a merge itself.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaderProposal {
    pub run: RunId,
    pub reason: String,
    pub requires_user_scope: bool,
}

/// Route a merge conflict (or any over-scope integration outcome) to the
/// leader proposal path. Returns the proposal; creating no task.
pub fn plan_conflict_action(run: RunId, reason: String) -> LeaderProposal {
    LeaderProposal {
        run,
        reason,
        requires_user_scope: true,
    }
}

/// The CAS promotion gate: failed checks never move the good ref, and a
/// parent that moved since preparation conflicts instead of force-moving.
/// A conflict means a new candidate and new checks are required.
pub fn gate_promotion(
    expected_parent: &str,
    actual_parent: &str,
    verification: &VerificationSummary,
) -> Result<(), AppError> {
    if !verification.passed {
        return Err(AppError::Forbidden(
            "integration checks did not all pass; the good ref stays".to_string(),
        ));
    }
    if expected_parent != actual_parent {
        return Err(AppError::Conflict(format!(
            "good ref moved since preparation (expected {expected_parent}, is {actual_parent}); \
             a new candidate and new checks are required"
        )));
    }
    Ok(())
}

/// Recover the durable promotion outcome from the persisted intent and the
/// actual ref head: when the ref already points at the candidate, the
/// promotion committed even if the receipt was lost; otherwise it did not.
/// Recovery never mutates the ref.
pub fn recover_receipt(intent: &PromotionIntent, actual_head: &str) -> PromotionReceipt {
    let committed = actual_head == intent.candidate;
    PromotionReceipt {
        ref_name: intent.ref_name.clone(),
        previous_head: intent.expected_parent.clone(),
        new_head: if committed {
            intent.candidate.clone()
        } else {
            actual_head.to_string()
        },
        candidate: intent.candidate.clone(),
        intent_id: intent.intent_id.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use central_domain::ContentHash;

    fn check(program: &str) -> PlannedCheck {
        PlannedCheck {
            program: program.to_string(),
            args: Vec::new(),
        }
    }

    fn ok() -> CheckOutcome {
        CheckOutcome {
            exit_code: 0,
            timed_out: false,
        }
    }

    fn plan() -> IntegrationPlan {
        IntegrationPlan {
            baseline: vec![check("base")],
            affected: vec![check("aff")],
            acceptance: vec![check("acc")],
        }
    }

    #[test]
    fn checks_run_in_plan_order_and_all_must_pass() {
        let plan = plan();
        let names: Vec<&str> = plan
            .required()
            .iter()
            .map(|c| c.program.as_str())
            .collect();
        assert_eq!(names, vec!["base", "aff", "acc"]);
        assert_eq!(
            judge_integration(&plan, &[ok(), ok(), ok()]),
            IntegrationVerdict::Pass
        );
        assert!(matches!(
            judge_integration(
                &plan,
                &[
                    ok(),
                    CheckOutcome {
                        exit_code: 1,
                        timed_out: false
                    },
                    ok()
                ]
            ),
            IntegrationVerdict::Fail(_)
        ));
        assert_eq!(
            judge_integration(&plan, &[ok()]),
            IntegrationVerdict::Incomplete
        );
    }

    #[test]
    fn queue_serializes_nodes_per_run() {
        let mut queue = IntegrationQueue::new();
        let run = RunId::from_u128(1);
        let other = RunId::from_u128(2);
        queue
            .submit(
                run,
                QueuedIntegration {
                    node: "n1".to_string(),
                    candidates: vec!["c1".to_string()],
                },
            )
            .expect("first node claims the slot");
        assert!(!queue.is_idle(run));
        assert!(queue.is_idle(other));
        assert!(matches!(
            queue.submit(
                run,
                QueuedIntegration {
                    node: "n2".to_string(),
                    candidates: Vec::new(),
                }
            ),
            Err(AppError::Conflict(_))
        ));
        queue.complete(run);
        assert!(queue.is_idle(run));
    }

    #[test]
    fn promotion_gate_holds_failed_checks_and_stale_parents() {
        let pass = VerificationSummary {
            passed: true,
            baseline_ok: true,
            affected_ok: true,
            acceptance_ok: true,
            evidence: Vec::new(),
        };
        assert!(gate_promotion("a", "a", &pass).is_ok());
        let fail = VerificationSummary {
            passed: false,
            ..pass.clone()
        };
        assert!(matches!(
            gate_promotion("a", "a", &fail),
            Err(AppError::Forbidden(_))
        ));
        assert!(matches!(
            gate_promotion("a", "b", &pass),
            Err(AppError::Conflict(_))
        ));
    }

    #[test]
    fn recovery_reports_the_actual_ref_without_mutating() {
        let intent = PromotionIntent {
            intent_id: "intent-1".to_string(),
            run: RunId::from_u128(7),
            repo_root: "repo".to_string(),
            ref_name: "refs/heads/central-good".to_string(),
            base: "base".to_string(),
            candidate: "cand".to_string(),
            expected_parent: "base".to_string(),
        };
        let committed = recover_receipt(&intent, "cand");
        assert_eq!(committed.new_head, "cand");
        let missing = recover_receipt(&intent, "base");
        assert_eq!(missing.new_head, "base");
        assert_eq!(missing.candidate, "cand");
        let _ = ContentHash::from_hex("ab");
    }
}
