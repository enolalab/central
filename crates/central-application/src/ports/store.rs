use async_trait::async_trait;
use central_domain::{
    ActorId, AttemptId, CommandId, Event, IntentId, Revision, RunId, RunState, Timestamp,
};

use crate::contracts::{AppError, CommitBatch, OutboxIntent, OutboxStatus, Receipt};

/// Scope of a client credential (P4). `Read` never writes; `Control`
/// drives workflow as the user but never manages credentials or the
/// daemon; `Bridge` is an agent-scoped token bound to one role and one
/// attempt, used only for result/handover import — never approvals.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ClientScope {
    Read,
    Control,
    Bridge { role: String, attempt: AttemptId },
}

impl ClientScope {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Control => "control",
            Self::Bridge { .. } => "bridge",
        }
    }
}

/// A stored client credential. Only the SHA-256 hash of the secret is
/// persisted — the plaintext secret is returned once at issuance and
/// never stored, logged or listed.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ClientRecord {
    pub client_id: String,
    pub name: String,
    pub scope: ClientScope,
    pub secret_hash: String,
    pub expires_at: Option<Timestamp>,
    pub revoked: bool,
}

/// One durable leader-conversation message (P4 chat). Messages are
/// discussion only: storing one never approves a plan, answers inbox,
/// or dispatches work.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChatMessage {
    pub seq: u64,
    pub run_id: RunId,
    pub author: String,
    pub body: String,
    pub at: Timestamp,
}

/// One user-supplied agent profile (P4). Attempts keep the frozen copy
/// they dispatched with; editing a profile never rewrites history.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UserProfileRecord {
    pub profile_id: String,
    pub name: String,
    pub provider: String,
    pub model: String,
    pub effort: String,
    pub config_json: String,
    pub updated_at: Timestamp,
}

/// A run snapshot read together with its durable event cursor in one
/// read transaction: `last_seq` is the highest event seq covered by
/// `state`, so a client starting a stream at `last_seq` misses nothing.
#[derive(Clone, Debug, PartialEq)]
pub struct SnapshotWithCursor {
    pub state: RunState,
    pub last_seq: u64,
}

/// One event with its durable per-run sequence number.
#[derive(Clone, Debug, PartialEq)]
pub struct SequencedEvent {
    pub seq: u64,
    pub event: Event,
}

/// Durable run store. Implementations must commit a `CommitBatch`
/// atomically: snapshot, events, outbox intents and receipt land together
/// or not at all, and the ack is returned only after the commit.
#[async_trait]
pub trait Store: Send + Sync {
    async fn load(&self, id: RunId) -> Result<Option<RunState>, AppError>;
    async fn receipt(&self, actor: ActorId, id: CommandId) -> Result<Option<Receipt>, AppError>;
    async fn commit(&self, batch: CommitBatch) -> Result<Receipt, AppError>;
    async fn pending(&self, id: RunId) -> Result<Vec<OutboxIntent>, AppError>;

    /// All run ids known to the store. Used by startup recovery; the
    /// default reports the operation as unsupported rather than
    /// silently recovering nothing.
    async fn list_run_ids(&self) -> Result<Vec<RunId>, AppError> {
        Err(AppError::Storage(
            "list_run_ids is not supported by this store".to_string(),
        ))
    }

    /// Every outbox intent for a run, in any status. Recovery needs the
    /// non-pending rows (for example `Dispatching`) that `pending`
    /// deliberately hides.
    async fn outbox(&self, id: RunId) -> Result<Vec<OutboxIntent>, AppError> {
        let _ = id;
        Err(AppError::Storage(
            "outbox is not supported by this store".to_string(),
        ))
    }

    /// Guarded outbox transition: the intent moves from `expected` to
    /// `next` only if it is still in `expected`. Returns `false` when
    /// the guard fails (already handled elsewhere), never overwrites.
    /// Dispatch code must use this — evidence moves an intent to
    /// `Completed`, ambiguity to `UnknownOutcome` — and never an
    /// unguarded status write.
    async fn transition_outbox(
        &self,
        id: IntentId,
        expected: OutboxStatus,
        next: OutboxStatus,
    ) -> Result<bool, AppError> {
        let _ = (id, expected, next);
        Err(AppError::Storage(
            "transition_outbox is not supported by this store".to_string(),
        ))
    }

    /// Every persisted event for a run, in sequence order. The default
    /// reports the operation as unsupported rather than silently
    /// returning no history.
    async fn events(&self, id: RunId) -> Result<Vec<Event>, AppError> {
        let _ = id;
        Err(AppError::Storage(
            "events is not supported by this store".to_string(),
        ))
    }

    /// Replace a run snapshot under a revision compare-and-swap, with
    /// no receipt or events. Reserved for startup recovery, which is a
    /// system transition, not a user command; the revision is kept so
    /// recovery never forges command history.
    async fn replace_state(
        &self,
        state: &RunState,
        expected_revision: Revision,
    ) -> Result<(), AppError> {
        let _ = (state, expected_revision);
        Err(AppError::Storage(
            "replace_state is not supported by this store".to_string(),
        ))
    }

    /// Snapshot plus durable cursor in one read transaction (P4). The
    /// default keeps `events(RunId)` consumers working during migration.
    async fn snapshot_with_cursor(
        &self,
        id: RunId,
    ) -> Result<Option<SnapshotWithCursor>, AppError> {
        let _ = id;
        Err(AppError::Storage(
            "snapshot_with_cursor is not supported by this store".to_string(),
        ))
    }

    /// Events strictly after `after`, in seq order, at most `limit`
    /// rows (P4 cursor paging). The default keeps `events(RunId)`
    /// consumers working during migration.
    async fn events_after(
        &self,
        id: RunId,
        after: u64,
        limit: u32,
    ) -> Result<Vec<SequencedEvent>, AppError> {
        let _ = (id, after, limit);
        Err(AppError::Storage(
            "events_after is not supported by this store".to_string(),
        ))
    }

    /// Persist a new client credential record (hash only, P4).
    async fn create_client(&self, record: &ClientRecord) -> Result<(), AppError> {
        let _ = record;
        Err(AppError::Storage(
            "client credentials are not supported by this store".to_string(),
        ))
    }

    /// Every client record, including revoked ones (secret hashes only).
    async fn list_clients(&self) -> Result<Vec<ClientRecord>, AppError> {
        Err(AppError::Storage(
            "client credentials are not supported by this store".to_string(),
        ))
    }

    /// One client record by id, if any.
    async fn get_client(&self, client_id: &str) -> Result<Option<ClientRecord>, AppError> {
        let _ = client_id;
        Err(AppError::Storage(
            "client credentials are not supported by this store".to_string(),
        ))
    }

    /// Revoke a client. Returns false when the id is unknown. Revocation
    /// is monotonic: a revoked record can never be un-revoked.
    async fn revoke_client(&self, client_id: &str) -> Result<bool, AppError> {
        let _ = client_id;
        Err(AppError::Storage(
            "client credentials are not supported by this store".to_string(),
        ))
    }

    /// Append one chat message; the store assigns the per-run seq.
    async fn append_chat(
        &self,
        run_id: RunId,
        author: &str,
        body: &str,
        at: Timestamp,
    ) -> Result<ChatMessage, AppError> {
        let _ = (run_id, author, body, at);
        Err(AppError::Storage(
            "conversation is not supported by this store".to_string(),
        ))
    }

    /// Chat messages in seq order, strictly after `after`, at most
    /// `limit` rows.
    async fn chat_after(
        &self,
        run_id: RunId,
        after: u64,
        limit: u32,
    ) -> Result<Vec<ChatMessage>, AppError> {
        let _ = (run_id, after, limit);
        Err(AppError::Storage(
            "conversation is not supported by this store".to_string(),
        ))
    }

    /// Create or replace a user-supplied profile (P4). Attempts keep
    /// their frozen copy; this never rewrites history.
    async fn put_profile(&self, record: &UserProfileRecord) -> Result<(), AppError> {
        let _ = record;
        Err(AppError::Storage(
            "profiles are not supported by this store".to_string(),
        ))
    }

    /// One profile by id, if any.
    async fn get_profile(&self, profile_id: &str) -> Result<Option<UserProfileRecord>, AppError> {
        let _ = profile_id;
        Err(AppError::Storage(
            "profiles are not supported by this store".to_string(),
        ))
    }

    /// Every stored profile.
    async fn list_profiles(&self) -> Result<Vec<UserProfileRecord>, AppError> {
        Err(AppError::Storage(
            "profiles are not supported by this store".to_string(),
        ))
    }
}
