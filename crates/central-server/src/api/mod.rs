//! Public v1 wire contract: DTO shapes, opaque cursors, pagination
//! bounds and secret redaction. HTTP handlers map domain/application
//! values through here; the full `RunState` is never serialized to a
//! client (see `read_models` for the curated projection).

pub mod dto;
pub mod errors;
pub mod read_models;

pub use dto::{
    decode_cursor, encode_cursor, parse_after, parse_limit, Page, DEFAULT_LIMIT, MAX_LIMIT,
};
pub use errors::{app_error_response, error_response, ApiError};
