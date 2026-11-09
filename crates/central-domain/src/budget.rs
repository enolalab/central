//! Shared budget ledger: admission, usage accounting and holds (P3 Task 3).
//!
//! One ledger per run covers every role — planning, leader handling,
//! implementation, verification and integration — through the same
//! [`admission`] gate. There is no per-role or per-retry quota: retry
//! and fallback attempts pass the same gate as first attempts.
//!
//! Observed, estimated and unknown usage are kept apart:
//!
//! * `Observed` finals add to the observed totals (deduplicated, and a
//!   late lower cumulative never reduces what is already known).
//! * `Estimated` finals add to the estimated bucket only; resolving a
//!   gap never converts estimated (or missing) usage into observed.
//! * `InFlight` entries record live cost against a soft threshold
//!   without inventing a final total.
//! * `MissingFinal` (and an unsupported metric without a cap) flags the
//!   ledger unknown, which holds new work in the affected run while that
//!   run carries token/money limits. Only matching evidence or an
//!   explicit audited user resolution releases it.
//!
//! Cached reads (`source == "cache"`) are never added to the ledger.
//!
//! Pure: no IO, deterministic for a given snapshot and timestamp.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{ActorId, AttemptId, HoldReason, RunState, Timestamp};

/// Source marker whose reports never add to the ledger: a cached read is
/// not new consumption.
pub const CACHE_SOURCE: &str = "cache";

/// Natively capped usage metrics. Any other metric needs an entry in
/// [`UsageLimits::extra_caps`] or every report of it holds the run.
pub const METRIC_TOKENS: &str = "tokens";
pub const METRIC_COST: &str = "cost";

/// How complete one usage report is.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum UsageFinality {
    /// A confirmed final: counts toward observed totals.
    Observed,
    /// A vendor estimate: counts toward the estimated bucket only, never
    /// toward observed.
    Estimated,
    /// Live cost with no final yet: recorded, never totalled.
    InFlight,
    /// The final never arrived (for example a crash after send): flags
    /// the ledger unknown until evidence or an audited user resolution.
    MissingFinal,
}

/// One usage report bound to its provider session and turn.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageObservation {
    pub provider: String,
    pub epoch: String,
    pub turn: String,
    pub sequence: u64,
    /// Delta tokens when no cumulative figure is carried.
    pub tokens: u64,
    pub cost_minor: Option<u64>,
    /// Running total reported by the provider, if any. Only the increase
    /// over the per-stream high-water mark counts, so replays and late
    /// lower figures never move the known total.
    pub cumulative_tokens: Option<u64>,
    pub cumulative_cost_minor: Option<u64>,
    pub finality: UsageFinality,
    /// Which metric this reports (`tokens`/`cost` natively, anything else
    /// needs a cap in [`UsageLimits::extra_caps`]).
    pub metric: String,
    /// Where the report came from (`cache` sources are ignored).
    pub source: String,
}

impl UsageObservation {
    /// Deduplication key: one provider turn sequence counts exactly once.
    pub fn dedup_key(&self) -> String {
        format!(
            "{}/{}/{}/{}",
            self.provider, self.epoch, self.turn, self.sequence
        )
    }

    /// Cumulative high-water scope: one running total per provider epoch.
    pub fn stream_key(&self) -> String {
        format!("{}/{}/{}", self.provider, self.epoch, self.metric)
    }

    /// Live-entry scope: one live record per provider turn.
    pub fn live_key(&self) -> String {
        format!(
            "{}/{}/{}/{}",
            self.provider, self.epoch, self.turn, self.metric
        )
    }
}

/// One pending usage item: a live attempt cost with no final, or a final
/// that never arrived.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissingUsage {
    /// Dedup key (missing finals) or live key (in-flight entries).
    pub key: String,
    pub provider: String,
    pub epoch: String,
    pub metric: String,
    pub source: String,
    pub noted_at: Timestamp,
}

/// Audit trail for a user-resolved usage gap. The missing figure stays
/// missing: resolution releases the hold, it never fabricates observed
/// usage.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageAuditEntry {
    pub missing_key: String,
    pub note: String,
    pub by: ActorId,
    pub at: Timestamp,
    /// Always `false` today: a resolution never converts estimated or
    /// missing usage into observed usage. The field exists so a future
    /// writer cannot silently flip the invariant.
    pub resolved_observed: bool,
}

/// Count/token/time limits plus the user-audited provenance of the last
/// change. An optional monetary estimate is user-configured only: Central
/// never derives money from tokens.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageLimits {
    pub max_tokens: Option<u64>,
    pub max_cost_minor: Option<u64>,
    pub max_attempts: Option<u64>,
    pub max_duration_ms: Option<i64>,
    /// Metrics beyond `tokens`/`cost` that carry an explicit cap.
    #[serde(default)]
    pub extra_caps: BTreeSet<String>,
    /// Who last set the limits (filled from the command actor).
    #[serde(default)]
    pub updated_by: Option<ActorId>,
}

impl UsageLimits {
    /// Reject loud misconfiguration; absence of limits is valid.
    pub fn validate(&self) -> Result<(), crate::DomainError> {
        for metric in &self.extra_caps {
            if metric.trim().is_empty() {
                return Err(crate::DomainError::InvalidCommand(
                    "a capped metric needs a non-empty name".to_string(),
                ));
            }
        }
        Ok(())
    }

    /// Whether this run carries any token/money limit (the user-locked
    /// condition under which unknown usage holds new work).
    pub fn has_cost_limits(&self) -> bool {
        self.max_tokens.is_some() || self.max_cost_minor.is_some()
    }
}

/// Whether a metric is capped by these limits.
pub fn capped_metric(metric: &str, limits: &UsageLimits) -> bool {
    metric == METRIC_TOKENS || metric == METRIC_COST || limits.extra_caps.contains(metric)
}

/// Which role the gated work belongs to. All roles share one ledger;
/// the role is audit context, never a separate quota.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkRole {
    Leader,
    Worker,
    Verifier,
    Integrator,
    Repair,
    Retry,
    Fallback,
}

/// The work an admission check gates.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkRequest {
    pub role: WorkRole,
}

/// The shared-ledger admission verdict.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Admission {
    Allowed,
    Held(HoldReason),
}

/// Whether unknown usage must hold this run: unknown only holds a run
/// that carries token/money limits (the user-locked policy). Runs
/// without such limits keep dispatching on their known totals.
pub fn usage_unknown_hold(state: &RunState) -> bool {
    state.usage.unknown && state.usage.limits.has_cost_limits()
}

/// Whether the run hit any configured limit. Cost figures the provider
/// never reported cannot trip a money cap on their own: nothing is
/// invented to fill them in.
pub fn budget_exceeded(state: &RunState, now: Timestamp) -> bool {
    if let Some(max) = state.usage.limits.max_tokens {
        if state.usage.total_tokens >= max {
            return true;
        }
    }
    if let (Some(max), Some(cost)) = (
        state.usage.limits.max_cost_minor,
        state.usage.total_cost_minor,
    ) {
        if cost >= max {
            return true;
        }
    }
    if let Some(max) = state.usage.limits.max_attempts {
        if state.attempts.len() as u64 >= max {
            return true;
        }
    }
    if let (Some(start), Some(max_ms)) = (
        state.usage.window_started_at,
        state.usage.limits.max_duration_ms,
    ) {
        if now.0.saturating_sub(start.0) >= max_ms {
            return true;
        }
    }
    false
}

/// The shared gate every new attempt passes — leaders, verifiers,
/// retries and fallbacks included. Running attempts are never touched
/// here; callers keep them running and only block new work.
pub fn admission(state: &RunState, request: &WorkRequest, now: Timestamp) -> Admission {
    let _ = request.role;
    if usage_unknown_hold(state) {
        return Admission::Held(HoldReason::UsageUnknown);
    }
    if budget_exceeded(state, now) {
        return Admission::Held(HoldReason::BudgetExceeded);
    }
    Admission::Allowed
}

/// Recompute the budget holds from the ledger: insert the hold whose
/// condition holds, clear it when the condition lifts. Other holds
/// (`PlanImpact`, `ResourceConflict`) are never touched.
pub fn sync_budget_holds(state: &mut RunState, now: Timestamp) {
    if usage_unknown_hold(state) {
        state.holds.insert(HoldReason::UsageUnknown);
    } else {
        state.holds.remove(&HoldReason::UsageUnknown);
    }
    if budget_exceeded(state, now) {
        state.holds.insert(HoldReason::BudgetExceeded);
    } else {
        state.holds.remove(&HoldReason::BudgetExceeded);
    }
}

/// Fold one usage report into the ledger. Returns whether the ledger
/// changed. Never reduces a known total, never clears a pending item
/// without matching evidence, never adds cached reads.
pub fn apply_usage(
    ledger: &mut crate::UsageLedger,
    observation: &UsageObservation,
    now: Timestamp,
) -> Result<bool, crate::DomainError> {
    if observation.provider.trim().is_empty()
        || observation.epoch.trim().is_empty()
        || observation.turn.trim().is_empty()
        || observation.metric.trim().is_empty()
    {
        return Err(crate::DomainError::InvalidCommand(
            "a usage observation needs provider, epoch, turn and metric".to_string(),
        ));
    }
    if observation.source == CACHE_SOURCE {
        return Ok(false);
    }
    if !capped_metric(&observation.metric, &ledger.limits) {
        // Unsupported metric without a cap: hold, never fill 0/estimate.
        let key = observation.dedup_key();
        if ledger.seen_events.contains(&key) {
            return Ok(false);
        }
        ledger.seen_events.insert(key.clone());
        ledger.missing_final.insert(
            key.clone(),
            MissingUsage {
                key: key.clone(),
                provider: observation.provider.clone(),
                epoch: observation.epoch.clone(),
                metric: observation.metric.clone(),
                source: observation.source.clone(),
                noted_at: now,
            },
        );
        ledger.unknown = true;
        return Ok(true);
    }

    match observation.finality {
        UsageFinality::Observed | UsageFinality::Estimated => {
            let key = observation.dedup_key();
            // Matching evidence releases the pending item it proves.
            if ledger.missing_final.remove(&key).is_none() {
                ledger.in_flight.remove(&observation.live_key());
            }
            if ledger.seen_events.contains(&key) {
                ledger.unknown = !ledger.missing_final.is_empty();
                return Ok(false);
            }
            let observed = observation.finality == UsageFinality::Observed;
            let stream = observation.stream_key();
            let high = ledger
                .cumulative_high_water
                .get(&stream)
                .copied()
                .unwrap_or(0);
            let delta = match observation.cumulative_tokens {
                Some(cumulative) => cumulative.saturating_sub(high),
                None => observation.tokens,
            };
            if observation.cumulative_tokens.is_some() {
                let high = ledger
                    .cumulative_high_water
                    .entry(stream.clone())
                    .or_insert(0);
                *high = (*high).max(observation.cumulative_tokens.unwrap_or(0));
            } else {
                *ledger.cumulative_high_water.entry(stream).or_insert(0) += observation.tokens;
            }
            ledger.seen_events.insert(key);
            ledger.total_tokens += delta;
            if observed {
                ledger.observed_tokens += delta;
            } else {
                ledger.estimated_tokens += delta;
            }
            let cost_high = ledger
                .cumulative_cost_high_water
                .get(&observation.stream_key())
                .copied()
                .unwrap_or(0);
            let cost_delta = match (observation.cost_minor, observation.cumulative_cost_minor) {
                (_, Some(cumulative)) => cumulative.saturating_sub(cost_high),
                (Some(cost), None) => cost,
                (None, None) => 0,
            };
            if observation.cumulative_cost_minor.is_some() {
                let high = ledger
                    .cumulative_cost_high_water
                    .entry(observation.stream_key())
                    .or_insert(0);
                *high = (*high).max(observation.cumulative_cost_minor.unwrap_or(0));
            } else if let Some(cost) = observation.cost_minor {
                *ledger
                    .cumulative_cost_high_water
                    .entry(observation.stream_key())
                    .or_insert(0) += cost;
            }
            if cost_delta > 0 || observation.cost_minor.is_some() {
                let total = ledger.total_cost_minor.get_or_insert(0);
                *total += cost_delta;
                if observed {
                    ledger.observed_cost_minor += cost_delta;
                }
            }
            ledger.unknown = !ledger.missing_final.is_empty();
            Ok(true)
        }
        UsageFinality::InFlight => {
            // Live cost with no final: recorded against the soft
            // threshold, never totalled, never deduced into a final.
            ledger.in_flight.insert(
                observation.live_key(),
                MissingUsage {
                    key: observation.live_key(),
                    provider: observation.provider.clone(),
                    epoch: observation.epoch.clone(),
                    metric: observation.metric.clone(),
                    source: observation.source.clone(),
                    noted_at: now,
                },
            );
            Ok(true)
        }
        UsageFinality::MissingFinal => {
            let key = observation.dedup_key();
            if ledger.seen_events.contains(&key) {
                return Ok(false);
            }
            ledger.seen_events.insert(key.clone());
            ledger.missing_final.insert(
                key.clone(),
                MissingUsage {
                    key,
                    provider: observation.provider.clone(),
                    epoch: observation.epoch.clone(),
                    metric: observation.metric.clone(),
                    source: observation.source.clone(),
                    noted_at: now,
                },
            );
            ledger.unknown = true;
            Ok(true)
        }
    }
}

/// Retry bookkeeping: how many safe retries one attempt already used.
pub fn retry_used(state: &RunState, attempt: AttemptId) -> u32 {
    state.retry_used.get(&attempt).copied().unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ledger() -> crate::UsageLedger {
        crate::UsageLedger::default()
    }

    fn observed(cumulative: u64) -> UsageObservation {
        UsageObservation {
            provider: "fake".to_string(),
            epoch: "e1".to_string(),
            turn: "t1".to_string(),
            sequence: 1,
            tokens: 0,
            cost_minor: None,
            cumulative_tokens: Some(cumulative),
            cumulative_cost_minor: None,
            finality: UsageFinality::Observed,
            metric: METRIC_TOKENS.to_string(),
            source: "reported".to_string(),
        }
    }

    #[test]
    fn cumulative_replays_never_move_the_total() {
        let mut ledger = ledger();
        let now = Timestamp(1);
        assert!(apply_usage(&mut ledger, &observed(120), now).expect("applies"));
        assert!(!apply_usage(&mut ledger, &observed(120), now).expect("dedups"));
        let mut older = observed(100);
        older.turn = "t0".to_string();
        older.sequence = 0;
        assert!(apply_usage(&mut ledger, &older, now).expect("applies"));
        assert_eq!(ledger.observed_tokens, 120);
        assert_eq!(ledger.total_tokens, 120);
    }

    #[test]
    fn cache_source_never_adds() {
        let mut ledger = ledger();
        let mut cached = observed(500);
        cached.source = CACHE_SOURCE.to_string();
        assert!(!apply_usage(&mut ledger, &cached, Timestamp(1)).expect("ignored"));
        assert_eq!(ledger.total_tokens, 0);
        assert!(ledger.seen_events.is_empty());
    }

    #[test]
    fn unsupported_metric_holds_without_filling() {
        let mut ledger = ledger();
        let mut exotic = observed(10);
        exotic.metric = "video-seconds".to_string();
        assert!(apply_usage(&mut ledger, &exotic, Timestamp(1)).expect("holds"));
        assert!(ledger.unknown);
        assert_eq!(ledger.total_tokens, 0);
    }

    #[test]
    fn unknown_holds_only_runs_with_cost_limits() {
        let mut state = RunState::new(crate::RunId::from_u128(1));
        state.usage.unknown = true;
        let req = WorkRequest {
            role: WorkRole::Verifier,
        };
        assert_eq!(
            admission(&state, &req, Timestamp(1)),
            Admission::Allowed,
            "no limits: unknown usage must not hold"
        );
        state.usage.limits.max_tokens = Some(100);
        assert_eq!(
            admission(&state, &req, Timestamp(1)),
            Admission::Held(HoldReason::UsageUnknown)
        );
    }

    #[test]
    fn duration_limit_expires_new_work() {
        let mut state = RunState::new(crate::RunId::from_u128(1));
        state.usage.window_started_at = Some(Timestamp(1_000));
        state.usage.limits.max_duration_ms = Some(60_000);
        let req = WorkRequest {
            role: WorkRole::Leader,
        };
        assert_eq!(
            admission(&state, &req, Timestamp(30_000)),
            Admission::Allowed
        );
        assert_eq!(
            admission(&state, &req, Timestamp(61_000)),
            Admission::Held(HoldReason::BudgetExceeded)
        );
    }
}
