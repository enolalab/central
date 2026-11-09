//! Classified retry policy: safe retries, never blind ones (P3 Task 3).
//!
//! Only a send the harness proves never happened (`DefinitelyNotSent`)
//! may retry, at most [`MAX_RETRIES_DEFAULT`] times with backoff delays
//! of [`DEFAULT_DELAYS_MS`]. An ambiguous send (`OutcomeUnknown`) never
//! retries: the attempt parks until reconciliation proves what happened.
//! Quota/auth refusals never retry either — they belong to the fallback
//! chain (application `fallback` module), not to a retry storm.
//!
//! Pure: delay arithmetic only, no clocks, no sleeps. Callers advance a
//! virtual clock and compare against the computed `not_before`.

/// How many safe retries one attempt may use before it parks failed
/// (recoverable through an explicit reopen, never silently dropped).
pub const MAX_RETRIES_DEFAULT: u32 = 3;

/// Backoff between safe retries: 2s, 10s, 30s. The default implements
/// the plan's configurable default; deployments override it through
/// [`RetryPolicy`].
pub const DEFAULT_DELAYS_MS: [i64; 3] = [2_000, 10_000, 30_000];

/// Classified send failure. Transport losses before the send are
/// retryable; ambiguity, quota/auth and hard refusals each park
/// differently and never share a retry budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Failure {
    /// The harness proves nothing was sent: may retry with backoff.
    DefinitelyNotSent,
    /// The send may have landed: never retry, reconcile only.
    OutcomeUnknown,
    /// Quota exhausted: fallback chain or park, never retry.
    QuotaExceeded,
    /// Auth or permission refused: fallback chain or park, never retry.
    Unauthorized,
    /// Capability refused: park, never retry.
    CapabilityDenied,
    /// Any other hard refusal: park recoverably, never retry.
    Refused,
}

/// What a harness refusal message most likely means. The match is a
/// documented heuristic over adapter text — adapter error shapes, not
/// Central, own the definitive taxonomy — so dispatch treats `Other`
/// conservatively (park, never retry, never fall back silently).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RejectionKind {
    Quota,
    Auth,
    Other,
}

/// Classify a harness refusal message. Quota keywords win over auth
/// keywords when a message carries both.
pub fn classify_rejection(message: &str) -> RejectionKind {
    let text = message.to_lowercase();
    let quota = [
        "quota",
        "rate limit",
        "rate_limit",
        "too many requests",
        "429",
        "budget",
    ]
    .iter()
    .any(|needle| text.contains(needle));
    if quota {
        return RejectionKind::Quota;
    }
    let auth = [
        "auth",
        "unauthor",
        "permission",
        "forbidden",
        "api key",
        "apikey",
        "credential",
        "401",
        "403",
    ]
    .iter()
    .any(|needle| text.contains(needle));
    if auth {
        return RejectionKind::Auth;
    }
    RejectionKind::Other
}

/// Map a refusal classification onto the send-failure taxonomy.
pub fn failure_for_rejection(kind: RejectionKind) -> Failure {
    match kind {
        RejectionKind::Quota => Failure::QuotaExceeded,
        RejectionKind::Auth => Failure::Unauthorized,
        RejectionKind::Other => Failure::Refused,
    }
}

/// Bounded safe-retry policy. Deployments tune the bound and the
/// backoff through the plan/scheduler config; the domain default is
/// 3 retries at 2s/10s/30s.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub delays_ms: Vec<i64>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: MAX_RETRIES_DEFAULT,
            delays_ms: DEFAULT_DELAYS_MS.to_vec(),
        }
    }
}

impl RetryPolicy {
    /// Reject loud misconfiguration: an empty or non-positive delay
    /// would otherwise retry immediately or never.
    pub fn validate(&self) -> Result<(), crate::DomainError> {
        if self.delays_ms.is_empty() {
            return Err(crate::DomainError::InvalidCommand(
                "a retry policy needs at least one delay".to_string(),
            ));
        }
        if self.delays_ms.iter().any(|delay| *delay <= 0) {
            return Err(crate::DomainError::InvalidCommand(
                "retry delays must be positive milliseconds".to_string(),
            ));
        }
        Ok(())
    }
}

/// Backoff before the next safe retry, or `None` when the retry budget
/// is exhausted. `retries_used` counts completed safe retries for the
/// attempt; the first retry waits `delays[0]`.
pub fn next_retry_delay(policy: &RetryPolicy, retries_used: u32) -> Option<i64> {
    if retries_used >= policy.max_retries {
        return None;
    }
    let delay = policy
        .delays_ms
        .get(retries_used as usize)
        .copied()
        .unwrap_or_else(|| policy.delays_ms.last().copied().unwrap_or(30_000));
    Some(delay)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_policy_retries_three_times_with_backoff() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.max_retries, 3);
        assert_eq!(next_retry_delay(&policy, 0), Some(2_000));
        assert_eq!(next_retry_delay(&policy, 1), Some(10_000));
        assert_eq!(next_retry_delay(&policy, 2), Some(30_000));
        assert_eq!(next_retry_delay(&policy, 3), None);
        assert_eq!(next_retry_delay(&policy, 99), None);
    }

    #[test]
    fn short_policy_repeats_its_last_delay() {
        let policy = RetryPolicy {
            max_retries: 3,
            delays_ms: vec![500],
        };
        assert_eq!(next_retry_delay(&policy, 0), Some(500));
        assert_eq!(next_retry_delay(&policy, 2), Some(500));
        assert_eq!(next_retry_delay(&policy, 3), None);
    }

    #[test]
    fn rejection_keywords_classify_quota_and_auth() {
        assert_eq!(
            classify_rejection("quota exceeded for model x"),
            RejectionKind::Quota
        );
        assert_eq!(
            classify_rejection("HTTP 429 too many requests"),
            RejectionKind::Quota
        );
        assert_eq!(
            classify_rejection("auth refused: permission denied"),
            RejectionKind::Auth
        );
        assert_eq!(
            classify_rejection("harness refused the turn"),
            RejectionKind::Other
        );
        assert_eq!(
            failure_for_rejection(RejectionKind::Quota),
            Failure::QuotaExceeded
        );
        assert_eq!(
            failure_for_rejection(RejectionKind::Other),
            Failure::Refused
        );
    }

    #[test]
    fn empty_or_negative_delays_are_rejected() {
        let empty = RetryPolicy {
            max_retries: 3,
            delays_ms: Vec::new(),
        };
        assert!(empty.validate().is_err());
        let negative = RetryPolicy {
            max_retries: 3,
            delays_ms: vec![1_000, -5],
        };
        assert!(negative.validate().is_err());
    }
}
