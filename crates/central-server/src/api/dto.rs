//! Pagination, opaque cursors and page envelopes (`PageV1`).
//!
//! Cursors are opaque hex strings binding collection + run + position:
//! a cursor minted for one collection/run is rejected (`400`) anywhere
//! else. Seq positions (`events`, `chat`) also accept a bare decimal
//! for convenience; anything else is `400`, never a silent restart.

use serde::Serialize;

use super::errors::{error_response, ApiError};

pub const DEFAULT_LIMIT: u32 = 100;
pub const MAX_LIMIT: u32 = 500;

/// One page: `items` plus the cursor for the next page, or null when
/// exhausted. Serialized inline by each handler (`items`/`next_cursor`).
#[derive(Serialize)]
pub struct Page<T: Serialize> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

/// Mint an opaque cursor for (`collection`, `run`, `pos`).
pub fn encode_cursor(collection: &str, run: &str, pos: u64) -> String {
    let raw = format!("v1|{collection}|{run}|{pos}");
    hex_encode(raw.as_bytes())
}

/// Decode and validate a cursor against the expected collection/run.
/// Returns the position. A bare decimal is accepted as a position for
/// the expected collection/run (client convenience, same binding).
pub fn decode_cursor(raw: &str, collection: &str, run: &str) -> Result<u64, ApiError> {
    if let Ok(pos) = raw.parse::<u64>() {
        return Ok(pos);
    }
    let bytes = hex_decode(raw).ok_or_else(|| {
        error_response(
            axum::http::StatusCode::BAD_REQUEST,
            "validation_failed",
            format!("cursor for {collection} is not valid; refetch the collection head"),
        )
    })?;
    let text = String::from_utf8(bytes).map_err(|_| {
        error_response(
            axum::http::StatusCode::BAD_REQUEST,
            "validation_failed",
            format!("cursor for {collection} is not valid; refetch the collection head"),
        )
    })?;
    let mut parts = text.split('|');
    match (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) {
        (Some("v1"), Some(seen_collection), Some(seen_run), Some(pos), None)
            if seen_collection == collection && seen_run == run =>
        {
            pos.parse::<u64>().map_err(|_| {
                error_response(
                    axum::http::StatusCode::BAD_REQUEST,
                    "validation_failed",
                    format!("cursor for {collection} is not valid; refetch the collection head"),
                )
            })
        }
        _ => Err(error_response(
            axum::http::StatusCode::BAD_REQUEST,
            "validation_failed",
            format!("cursor belongs to another collection or run; refetch the {collection} head"),
        )),
    }
}

/// Parse `?limit=`: default 100, max 500, anything else is 400.
pub fn parse_limit(raw: Option<&str>) -> Result<u32, ApiError> {
    match raw {
        None => Ok(DEFAULT_LIMIT),
        Some(text) => {
            let limit = text.parse::<u32>().map_err(|_| {
                error_response(
                    axum::http::StatusCode::BAD_REQUEST,
                    "validation_failed",
                    format!("limit must be a number 1..={MAX_LIMIT}, got {text:?}"),
                )
            })?;
            if limit == 0 || limit > MAX_LIMIT {
                return Err(error_response(
                    axum::http::StatusCode::BAD_REQUEST,
                    "validation_failed",
                    format!("limit must be 1..={MAX_LIMIT}, got {limit}"),
                ));
            }
            Ok(limit)
        }
    }
}

/// Parse `?after=`: a bare seq or an opaque cursor bound to
/// (`collection`, `run`). Missing means 0 (the head).
pub fn parse_after(raw: Option<&str>, collection: &str, run: &str) -> Result<u64, ApiError> {
    match raw {
        None => Ok(0),
        Some(text) => decode_cursor(text, collection, run),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn hex_decode(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) || text.is_empty() || text.len() > 512 {
        return None;
    }
    let mut out = Vec::with_capacity(text.len() / 2);
    let bytes = text.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let pair = std::str::from_utf8(&bytes[index..index + 2]).ok()?;
        out.push(u8::from_str_radix(pair, 16).ok()?);
        index += 2;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_round_trips_and_binds() {
        let cursor = encode_cursor("events", "run-1", 42);
        assert_eq!(
            decode_cursor(&cursor, "events", "run-1").expect("decode"),
            42
        );
        assert_eq!(decode_cursor("42", "events", "run-1").expect("bare"), 42);
        assert!(decode_cursor(&cursor, "chat", "run-1").is_err());
        assert!(decode_cursor(&cursor, "events", "run-2").is_err());
        assert!(decode_cursor("nope", "events", "run-1").is_err());
    }

    #[test]
    fn limit_bounds_hold() {
        assert_eq!(parse_limit(None).expect("default"), 100);
        assert_eq!(parse_limit(Some("500")).expect("max"), 500);
        assert!(parse_limit(Some("0")).is_err());
        assert!(parse_limit(Some("501")).is_err());
        assert!(parse_limit(Some("nope")).is_err());
    }
}
