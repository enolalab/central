use central_domain::{AttemptId, CommandId, IntentId, RunId};

/// ID source. Tests substitute a deterministic fake.
pub trait IdSource: Send + Sync {
    fn next_run_id(&self) -> RunId;
    fn next_intent_id(&self) -> IntentId;
    fn next_command_id(&self) -> CommandId;
    /// A fresh attempt id for the scheduler queue. The default mints a
    /// random id; deterministic tests override it with a counter.
    fn next_attempt_id(&self) -> AttemptId {
        AttemptId::new()
    }
}
