use central_domain::{CommandId, IntentId, RunId};

/// ID source. Tests substitute a deterministic fake.
pub trait IdSource: Send + Sync {
    fn next_run_id(&self) -> RunId;
    fn next_intent_id(&self) -> IntentId;
    fn next_command_id(&self) -> CommandId;
}
