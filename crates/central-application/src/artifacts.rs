//! Pure artifact helpers (P2 Task 5): path containment and content hashing.
//!
//! The filesystem store and the verification seal share these so a candidate
//! that escapes the run root is refused the same way everywhere.

use central_domain::ContentHash;
use sha2::{Digest, Sha256};

use crate::contracts::AppError;

/// SHA-256 of bytes, hex-encoded.
pub fn content_hash(bytes: &[u8]) -> ContentHash {
    let digest = Sha256::digest(bytes);
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        hex.push_str(&format!("{byte:02x}"));
    }
    ContentHash::from_hex(hex)
}

/// Fail unless `bytes` hash to `expected`.
pub fn verify_content_hash(bytes: &[u8], expected: &ContentHash) -> Result<(), AppError> {
    let actual = content_hash(bytes);
    if actual == *expected {
        Ok(())
    } else {
        Err(AppError::Validation(format!(
            "content hash mismatch: expected {expected}, staged {actual}"
        )))
    }
}

/// A candidate path is only accepted when it stays inside the run: relative,
/// non-empty, without parent escapes or drive/root prefixes. Mirrors the
/// containment rule the leader bridge enforces for worker reports.
pub fn check_run_relative_path(path: &str) -> Result<(), AppError> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err(AppError::Validation(
            "artifact path must not be empty".to_string(),
        ));
    }
    let normalized = trimmed.replace('\\', "/");
    if normalized.starts_with('/')
        || normalized.split('/').any(|segment| segment == "..")
        || normalized.contains(':')
    {
        return Err(AppError::Validation(format!(
            "artifact path escapes the run root: {trimmed}"
        )));
    }
    Ok(())
}
