//! Raw JSON codec between domain/application values and stored text.
//! SQL never crosses this boundary into the application layer, and a
//! corrupt or future-versioned snapshot is a storage error, never an
//! empty run.

use central_application::contracts::{AppError, OutboxIntent, Receipt};
use central_domain::run::RUN_STATE_VERSION;
use central_domain::{Event, RunState};

fn encode<T: serde::Serialize>(value: &T, what: &str) -> Result<String, AppError> {
    serde_json::to_string(value)
        .map_err(|e| AppError::Storage(format!("cannot encode {what}: {e}")))
}

fn decode<T: serde::de::DeserializeOwned>(json: &str, what: &str) -> Result<T, AppError> {
    serde_json::from_str(json).map_err(|e| AppError::Storage(format!("corrupt stored {what}: {e}")))
}

pub fn encode_state(state: &RunState) -> Result<String, AppError> {
    encode(state, "run snapshot")
}

pub fn decode_state(json: &str) -> Result<RunState, AppError> {
    let state: RunState = decode(json, "run snapshot")?;
    if state.version != RUN_STATE_VERSION {
        return Err(AppError::Storage(format!(
            "unsupported run snapshot version {} (supported {RUN_STATE_VERSION})",
            state.version
        )));
    }
    Ok(state)
}

pub fn encode_event(event: &Event) -> Result<String, AppError> {
    encode(event, "event")
}

pub fn decode_event(json: &str) -> Result<Event, AppError> {
    decode(json, "event")
}

pub fn encode_intent(intent: &OutboxIntent) -> Result<String, AppError> {
    encode(intent, "outbox intent")
}

pub fn decode_intent(json: &str) -> Result<OutboxIntent, AppError> {
    decode(json, "outbox intent")
}

pub fn encode_receipt(receipt: &Receipt) -> Result<String, AppError> {
    encode(receipt, "command receipt")
}

pub fn decode_receipt(json: &str) -> Result<Receipt, AppError> {
    decode(json, "command receipt")
}

pub fn encode_client(
    record: &central_application::ports::store::ClientRecord,
) -> Result<String, AppError> {
    encode(record, "client credential")
}

pub fn decode_client(
    json: &str,
) -> Result<central_application::ports::store::ClientRecord, AppError> {
    decode(json, "client credential")
}

pub fn encode_chat(
    message: &central_application::ports::store::ChatMessage,
) -> Result<String, AppError> {
    encode(message, "chat message")
}

pub fn decode_chat(json: &str) -> Result<central_application::ports::store::ChatMessage, AppError> {
    decode(json, "chat message")
}

pub fn encode_profile(
    record: &central_application::ports::store::UserProfileRecord,
) -> Result<String, AppError> {
    encode(record, "user profile")
}

pub fn decode_profile(
    json: &str,
) -> Result<central_application::ports::store::UserProfileRecord, AppError> {
    decode(json, "user profile")
}
