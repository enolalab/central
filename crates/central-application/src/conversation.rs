//! Durable leader conversation (P4 chat).
//!
//! `post_message` stores one user/leader message through the budget and
//! pause gates and returns it. It never approves a plan, never answers
//! inbox, never dispatches work and never spends budget: discussion is
//! not a mutation. Sending while the run is stopped or held closes the
//! gate with a conflict; reading always works.

use central_domain::{RunControl, RunId};

use crate::contracts::AppError;
use crate::ports::store::ChatMessage;
use crate::ports::{Clock, Store};

/// Maximum stored chat body: 64 KiB. Larger bodies are refused, never
/// truncated — truncation would silently rewrite the user's words.
pub const MAX_CHAT_BODY_CHARS: usize = 65_536;

/// Page cap for chat reads.
pub const MAX_CHAT_PAGE: u32 = 500;

/// Post one message. Control/owner actor only (enforced by the HTTP
/// layer's scope check); the gate here is the run state.
pub async fn post_message(
    store: &dyn Store,
    clock: &dyn Clock,
    run_id: RunId,
    author: &str,
    body: &str,
) -> Result<ChatMessage, AppError> {
    let body = body.trim();
    if body.is_empty() {
        return Err(AppError::Validation(
            "chat message must not be empty".to_string(),
        ));
    }
    if body.chars().count() > MAX_CHAT_BODY_CHARS {
        return Err(AppError::Validation(format!(
            "chat message exceeds {MAX_CHAT_BODY_CHARS} characters"
        )));
    }
    let author = author.trim();
    if author.is_empty() {
        return Err(AppError::Validation(
            "chat author must not be empty".to_string(),
        ));
    }
    let state = store
        .load(run_id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("run {run_id} not found")))?;
    if state.control == RunControl::Stopped {
        return Err(AppError::Conflict(
            "run is stopped; chat is closed with it".to_string(),
        ));
    }
    if !state.holds.is_empty() {
        return Err(AppError::Conflict(
            "run is held (budget, usage or plan impact); resolve the hold before chatting"
                .to_string(),
        ));
    }
    store.append_chat(run_id, author, body, clock.now()).await
}

/// Read messages in seq order, strictly after `after`, at most `limit`.
pub async fn read_messages(
    store: &dyn Store,
    run_id: RunId,
    after: u64,
    limit: u32,
) -> Result<Vec<ChatMessage>, AppError> {
    if limit == 0 || limit > MAX_CHAT_PAGE {
        return Err(AppError::Validation(format!(
            "chat limit must be 1..={MAX_CHAT_PAGE}"
        )));
    }
    let state = store
        .load(run_id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("run {run_id} not found")))?;
    let _ = state;
    store.chat_after(run_id, after, limit).await
}
