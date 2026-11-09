//! Durable approval/question inbox: the application side of P2 Task 3.
//!
//! The domain owns the inbox state machine; this module owns what the domain
//! must not persist:
//!
//! * [`SecretVault`]: memory-only answers. [`Runtime::answer_secret`] stashes
//!   the credential under a random [`VolatileAnswerRef`](central_domain::VolatileAnswerRef)
//!   and commits a command holding the reference only. Dispatch resolves the
//!   reference and consumes it on success. A restart drops the vault, so the
//!   durable intent can never replay a secret and the user must re-enter it.
//! * [`Runtime::reconcile_inbox`]: epoch rotation, request recording and
//!   passive resolution in one honest pass. Entries raised under another
//!   epoch go stale until a new request is verified; drained requests are
//!   persisted before any UI notice; observations resolve answered entries.
//!
//! The leader answers worker questions through budgeted tasks, never through
//! this module: there is no auto-answer path here, and dispatch only fires
//! while the run is `Running`.

use std::collections::HashMap;
use std::sync::Mutex;

use central_domain::{
    Actor, AnswerPayload, Command, CommandEnvelope, ContentHash, InboxKey, InboxRequest,
    InboxStatus, RunId,
};
use serde::{Deserialize, Serialize};

use crate::contracts::{AppError, ExternalRequest, ExternalRequestKey, Receipt, SessionRef};
use crate::Runtime;

/// Memory-only answers. The vault never touches the store, the outbox, the
/// audit trail or any log: entries are random ids mapped to credentials, and
/// [`SecretVault::consume`] removes the credential the moment it is sent.
#[derive(Debug, Default)]
pub struct SecretVault {
    inner: Mutex<HashMap<String, String>>,
}

impl SecretVault {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// Stash a secret under a fresh random reference. The reference is
    /// random per call and derived from nothing.
    pub fn stash(&self, secret: String) -> central_domain::VolatileAnswerRef {
        let reference = central_domain::VolatileAnswerRef::generate();
        self.inner
            .lock()
            .expect("secret vault")
            .insert(reference.as_str().to_string(), secret);
        reference
    }

    /// Look up a live reference without consuming it.
    pub fn lookup(&self, reference: &central_domain::VolatileAnswerRef) -> Option<String> {
        self.inner
            .lock()
            .expect("secret vault")
            .get(reference.as_str())
            .cloned()
    }

    /// Forget one reference, silently when already gone.
    pub fn forget(&self, reference: &central_domain::VolatileAnswerRef) {
        self.inner
            .lock()
            .expect("secret vault")
            .remove(reference.as_str());
    }

    /// Consume a reference after its answer was sent: exactly-once delivery
    /// of the credential, from memory to the harness, never to disk.
    pub fn consume(&self, reference: &central_domain::VolatileAnswerRef) -> Option<String> {
        self.inner
            .lock()
            .expect("secret vault")
            .remove(reference.as_str())
    }

    pub fn len(&self) -> usize {
        self.inner.lock().expect("secret vault").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// What one [`Runtime::reconcile_inbox`] pass did. Counts are best-effort
/// (each command commits independently); the returned run state is the
/// source of truth.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InboxReconcileSummary {
    /// Live entries of a previous epoch marked stale.
    pub marked_stale: usize,
    /// Drained requests persisted as new pending entries.
    pub recorded: usize,
    /// Drained requests that were already persisted (harness replay).
    pub duplicates: usize,
    /// Same-key/different-text reuses: protocol violations, never benign
    /// replays. Counted and surfaced separately (including in the JSON
    /// reconcile response) so an attack-shaped reuse is never mistaken for a
    /// harmless duplicate in the summary.
    pub conflicts: usize,
    /// Observations persisted from the notification buffer.
    pub observations: usize,
    /// Answered entries confirmed by those observations.
    pub resolved: usize,
}

impl From<&ExternalRequestKey> for InboxKey {
    fn from(key: &ExternalRequestKey) -> Self {
        Self {
            provider: key.session.provider.clone(),
            epoch: key.session.instance_epoch.clone(),
            thread_id: key.session.thread_id.clone(),
            turn_id: key.turn_id.clone(),
            item_id: key.item_id.clone(),
            request_id: key.request_id.clone(),
        }
    }
}

impl From<&ExternalRequest> for InboxRequest {
    fn from(request: &ExternalRequest) -> Self {
        Self {
            key: InboxKey::from(&request.key),
            kind: request.kind,
            preview: request.preview.clone(),
            preview_hash: request.preview_hash.clone(),
            attempt_id: request.attempt_id,
            is_secret: request.is_secret,
        }
    }
}

impl Runtime {
    async fn next_envelope(&self, command: Command) -> Result<CommandEnvelope, AppError> {
        let id = self.ids().next_command_id();
        Ok(CommandEnvelope {
            id,
            expected_revision: None,
            command,
        })
    }

    /// Answer a pending question with a secret. The credential is stashed in
    /// the memory-only vault under a fresh random reference; the committed
    /// command, receipt, events and outbox intent carry the reference only.
    /// When the decision rejects the reply (stale, wrong preview, resolved),
    /// the stash is dropped so nothing lingers.
    pub async fn answer_secret(
        &self,
        actor: Actor,
        key: InboxKey,
        preview_hash: ContentHash,
        secret: String,
    ) -> Result<Receipt, AppError> {
        let reference = self.secrets().stash(secret);
        let envelope = self
            .next_envelope(Command::AnswerQuestion {
                key,
                preview_hash,
                payload: AnswerPayload::Secret(reference.clone()),
            })
            .await?;
        match self.execute(actor, envelope).await {
            Ok(receipt) => Ok(receipt),
            Err(error) => {
                self.secrets().forget(&reference);
                Err(error)
            }
        }
    }

    /// One honest reconcile pass for the inbox of this run:
    ///
    /// 1. Entries raised under another instance epoch go stale — an old
    ///    accept never applies to a new lifetime, even for identical text.
    /// 2. Gateway-queued requests are persisted as pending entries before
    ///    any UI notice (benign replays count as duplicates, never as new
    ///    entries; same-key/different-text reuses count as conflicts).
    /// 3. Buffered observations are persisted; answered entries of the same
    ///    attempt resolve passively.
    ///
    /// No inference opens here: recording and resolving never answer.
    pub async fn reconcile_inbox(
        &self,
        actor: &Actor,
        session: &SessionRef,
    ) -> Result<InboxReconcileSummary, AppError> {
        let mut summary = InboxReconcileSummary::default();
        let run_id: RunId = self.run_id();
        let Some(state) = self.store().load(run_id).await? else {
            return Err(AppError::NotFound(format!("run {run_id} not found")));
        };
        let resolved_before = state
            .inbox
            .values()
            .filter(|entry| entry.status == InboxStatus::Resolved)
            .count();

        // (1) Epoch rotation: stale everything live from another epoch.
        let mut next = state.clone();
        for entry in next.inbox.values_mut() {
            if entry.epoch != session.instance_epoch
                && matches!(entry.status, InboxStatus::Pending | InboxStatus::Answered)
            {
                entry.status = InboxStatus::Stale;
                summary.marked_stale += 1;
            }
        }
        if summary.marked_stale > 0 {
            // System transition like recovery: no receipt, no forged
            // history, revision kept so nothing is rewritten.
            self.store().replace_state(&next, state.revision).await?;
        }

        // (2) Persist drained requests before any UI notice.
        let drained = self.agent().drain_requests(session).await?;
        let live = self.store().load(run_id).await?;
        for request in &drained {
            let key = InboxKey::from(&request.key);
            let canonical = key.canonical();
            let duplicate = live.as_ref().is_some_and(|state| {
                state
                    .inbox
                    .get(&canonical)
                    .is_some_and(|entry| entry.preview_hash == request.preview_hash)
            });
            if duplicate {
                summary.duplicates += 1;
                continue;
            }
            let envelope = self
                .next_envelope(Command::RecordExternalRequest {
                    request: InboxRequest::from(request),
                })
                .await?;
            // A same-key/different-text reuse is a protocol violation, not
            // a crash and not a benign replay: count the attempt as a
            // conflict and keep reconciling the rest.
            match self.execute(actor.clone(), envelope).await {
                Ok(_) => summary.recorded += 1,
                Err(AppError::Conflict(_)) => summary.conflicts += 1,
                Err(error) => return Err(error),
            }
        }

        // (3) Persist observations; answered entries resolve passively.
        for observation in self.agent().observe(session).await? {
            let envelope = self
                .next_envelope(Command::RecordObservation {
                    observation: observation.clone(),
                })
                .await?;
            self.execute(actor.clone(), envelope).await?;
            summary.observations += 1;
        }

        let after =
            self.store().load(run_id).await?.ok_or_else(|| {
                AppError::Storage(format!("run {run_id} vanished during reconcile"))
            })?;
        summary.resolved = after
            .inbox
            .values()
            .filter(|entry| entry.status == InboxStatus::Resolved)
            .count()
            .saturating_sub(resolved_before);
        Ok(summary)
    }
}
