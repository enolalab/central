//! Client credential issuance and verification (P4).
//!
//! The owner (daemon bearer token) issues scoped credentials for
//! independent clients. Only the SHA-256 hash of a secret is persisted;
//! the plaintext is returned once at issuance and never stored, logged,
//! listed or exported. Verification compares hashes in constant time.
//! Credentials never travel in URLs, logs, events or OpenAPI examples.

use sha2::{Digest, Sha256};

use central_domain::Timestamp;

use crate::contracts::AppError;
use crate::ports::store::{ClientRecord, ClientScope};
use crate::ports::{Clock, IdSource, Store};

/// Hash one secret for storage or comparison. Hex-encoded SHA-256.
pub fn hash_secret(secret: &str) -> String {
    let digest = Sha256::digest(secret.as_bytes());
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        hex.push_str(&format!("{byte:02x}"));
    }
    hex
}

/// Constant-time equality for secret hashes: no early exit on the
/// first differing byte.
pub fn hashes_equal(left: &str, right: &str) -> bool {
    let left = left.as_bytes();
    let right = right.as_bytes();
    if left.len() != right.len() {
        return false;
    }
    let mut diff = 0u8;
    for (a, b) in left.iter().zip(right.iter()) {
        diff |= a ^ b;
    }
    diff == 0
}

/// The one-time issuance answer: the record plus the plaintext secret.
/// Callers must show the secret once and then forget it; it is never
/// persisted anywhere.
#[derive(Clone, Debug)]
pub struct IssuedCredential {
    pub record: ClientRecord,
    pub secret: String,
}

/// Issue a `read` or `control` credential. Bridge tokens are issued by
/// [`issue_bridge`] with their role/attempt binding; this entry point
/// refuses bridge scope so a generic client can never mint agent rights.
pub async fn issue_client(
    store: &dyn Store,
    ids: &dyn IdSource,
    clock: &dyn Clock,
    name: &str,
    scope: ClientScope,
    expires_at: Option<Timestamp>,
) -> Result<IssuedCredential, AppError> {
    if matches!(scope, ClientScope::Bridge { .. }) {
        return Err(AppError::Validation(
            "bridge credentials need a role and attempt binding; use the bridge issuance path"
                .to_string(),
        ));
    }
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(AppError::Validation(
            "client name must not be empty".to_string(),
        ));
    }
    if !matches!(scope, ClientScope::Read | ClientScope::Control) {
        return Err(AppError::Validation("unknown client scope".to_string()));
    }
    let client_id = ids.next_command_id().to_string();
    let secret = format!(
        "{}{}",
        ids.next_command_id().to_string().replace('-', ""),
        ids.next_command_id().to_string().replace('-', "")
    );
    let record = ClientRecord {
        client_id,
        name: trimmed.to_string(),
        scope,
        secret_hash: hash_secret(&secret),
        expires_at,
        revoked: false,
    };
    let _ = clock;
    store.create_client(&record).await?;
    Ok(IssuedCredential { record, secret })
}

/// Issue an agent-bridge credential bound to one role and one attempt.
/// Only the owner calls this (the daemon mints bridge tokens when it
/// dispatches agent work); the token can only import results/handover
/// for its attempt, never approve plans or answer for another attempt.
pub async fn issue_bridge(
    store: &dyn Store,
    ids: &dyn IdSource,
    name: &str,
    role: &str,
    attempt: central_domain::AttemptId,
    expires_at: Option<Timestamp>,
) -> Result<IssuedCredential, AppError> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(AppError::Validation(
            "client name must not be empty".to_string(),
        ));
    }
    if role.trim().is_empty() {
        return Err(AppError::Validation(
            "bridge role must not be empty".to_string(),
        ));
    }
    let client_id = ids.next_command_id().to_string();
    let secret = format!(
        "{}{}",
        ids.next_command_id().to_string().replace('-', ""),
        ids.next_command_id().to_string().replace('-', "")
    );
    let record = ClientRecord {
        client_id,
        name: trimmed.to_string(),
        scope: ClientScope::Bridge {
            role: role.to_string(),
            attempt,
        },
        secret_hash: hash_secret(&secret),
        expires_at,
        revoked: false,
    };
    store.create_client(&record).await?;
    Ok(IssuedCredential { record, secret })
}

/// Verify a presented `client_id` + secret against the store. Returns
/// the live record on success. Revoked, expired or unknown credentials
/// all fail closed with the same error shape (no oracle for which).
pub async fn verify_client(
    store: &dyn Store,
    clock: &dyn Clock,
    client_id: &str,
    secret: &str,
) -> Result<ClientRecord, AppError> {
    let denied = || AppError::Validation("invalid client credential".to_string());
    let record = store.get_client(client_id).await?.ok_or_else(denied)?;
    if record.revoked {
        return Err(denied());
    }
    if let Some(expires) = record.expires_at {
        if clock.now().0 >= expires.0 {
            return Err(denied());
        }
    }
    if !hashes_equal(&record.secret_hash, &hash_secret(secret)) {
        return Err(denied());
    }
    Ok(record)
}

/// Split a presented client token of the form `<client_id>.<secret>`.
/// Anything else is not a client token (the owner bearer token is
/// checked before this ever runs).
pub fn split_client_token(presented: &str) -> Option<(&str, &str)> {
    let (id, secret) = presented.split_once('.')?;
    if id.is_empty() || secret.is_empty() {
        return None;
    }
    Some((id, secret))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_compare_is_constant_time_shape() {
        let hash = hash_secret("s3cret");
        assert!(hashes_equal(&hash, &hash_secret("s3cret")));
        assert!(!hashes_equal(&hash, &hash_secret("other")));
        assert!(!hashes_equal(&hash, "short"));
    }

    #[test]
    fn split_token_needs_both_parts() {
        assert!(split_client_token("id.secret").is_some());
        assert!(split_client_token("nosplit").is_none());
        assert!(split_client_token(".secret").is_none());
        assert!(split_client_token("id.").is_none());
    }
}
