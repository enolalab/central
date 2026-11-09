//! Classified retry: bounded safe retries on a virtual clock (P3 Task 3).
//!
//! Only a send the harness proves never happened retries, at most the
//! policy bound (default 3) with backoff (default 2s/10s/30s) measured
//! against the run's virtual clock — never a real sleep. `OutcomeUnknown`
//! never retries; quota/auth refusals belong to the fallback chain and
//! never consume retry budget. Exhaustion parks the attempt as failed,
//! recoverable through an explicit reopen.

use central_domain::{
    classify_rejection, failure_for_rejection, next_retry_delay, AttemptId, Failure, RetryPolicy,
    RunState, Timestamp,
};

use crate::contracts::{AppError, DispatchError};

/// What a dispatch outcome means for retry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetryAction {
    /// Safe to retry: the harness proves nothing was sent.
    RetryNotSent,
    /// Ambiguous: reconcile only, never resend.
    NeverRetryUnknown,
    /// Quota/auth: the fallback chain owns it, not the retry budget.
    Fallback,
    /// Any other hard refusal: park recoverably, never retry.
    Park,
}

/// Classify a send outcome into its retry action.
pub fn classify_send(error: &DispatchError) -> RetryAction {
    match error {
        DispatchError::DefinitelyNotSent(_) => RetryAction::RetryNotSent,
        DispatchError::OutcomeUnknown(_) => RetryAction::NeverRetryUnknown,
        DispatchError::Rejected(message) => {
            match failure_for_rejection(classify_rejection(message)) {
                Failure::QuotaExceeded | Failure::Unauthorized => RetryAction::Fallback,
                _ => RetryAction::Park,
            }
        }
    }
}

/// Classify an open-session failure. Transport errors (that are not
/// quota/auth in disguise) are safe to defer; capability and request
/// errors are definitive; quota/auth goes to the fallback chain.
pub fn classify_open(error: &AppError) -> RetryAction {
    match error {
        AppError::Transport(message) => match failure_for_rejection(classify_rejection(message)) {
            Failure::QuotaExceeded | Failure::Unauthorized => RetryAction::Fallback,
            _ => RetryAction::RetryNotSent,
        },
        AppError::Capability(_) | AppError::Forbidden(_) => RetryAction::Park,
        _ => RetryAction::Park,
    }
}

/// How many safe retries one attempt already used (durable, survives
/// restarts so a storm can never hide behind a reboot).
pub fn retries_used(state: &RunState, attempt: AttemptId) -> u32 {
    state.retry_used.get(&attempt).copied().unwrap_or(0)
}

/// Whether the attempt's deferred retry is due at `now`.
pub fn retry_due(state: &RunState, attempt: AttemptId, now: Timestamp) -> bool {
    match state.retry_not_before.get(&attempt) {
        Some(not_before) => *not_before <= now,
        // No deferral recorded: a queued attempt is due.
        None => true,
    }
}

/// When the next safe retry may send, or `None` when the retry budget
/// for this attempt is exhausted.
pub fn retry_not_before(policy: &RetryPolicy, used: u32, now: Timestamp) -> Option<Timestamp> {
    next_retry_delay(policy, used).map(|delay| Timestamp(now.0.saturating_add(delay)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_outcomes_classify_without_retry_storms() {
        assert_eq!(
            classify_send(&DispatchError::DefinitelyNotSent("dropped".to_string())),
            RetryAction::RetryNotSent
        );
        assert_eq!(
            classify_send(&DispatchError::OutcomeUnknown("lost".to_string())),
            RetryAction::NeverRetryUnknown
        );
        assert_eq!(
            classify_send(&DispatchError::Rejected("quota exceeded".to_string())),
            RetryAction::Fallback
        );
        assert_eq!(
            classify_send(&DispatchError::Rejected("auth refused".to_string())),
            RetryAction::Fallback
        );
        assert_eq!(
            classify_send(&DispatchError::Rejected("harness refused".to_string())),
            RetryAction::Park
        );
    }

    #[test]
    fn open_transport_defers_but_open_quota_falls_back() {
        assert_eq!(
            classify_open(&AppError::Transport("connection reset".to_string())),
            RetryAction::RetryNotSent
        );
        assert_eq!(
            classify_open(&AppError::Transport("429 quota exceeded".to_string())),
            RetryAction::Fallback
        );
        assert_eq!(
            classify_open(&AppError::Capability("no model".to_string())),
            RetryAction::Park
        );
    }

    #[test]
    fn retry_deadlines_follow_the_policy() {
        let policy = RetryPolicy::default();
        assert_eq!(
            retry_not_before(&policy, 0, Timestamp(1_000)),
            Some(Timestamp(3_000))
        );
        assert_eq!(
            retry_not_before(&policy, 2, Timestamp(1_000)),
            Some(Timestamp(31_000))
        );
        assert_eq!(retry_not_before(&policy, 3, Timestamp(1_000)), None);
    }
}
