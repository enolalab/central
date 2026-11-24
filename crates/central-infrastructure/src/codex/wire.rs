//! JSON-RPC 2.0 wire framing for the Codex app-server stdio transport.
//!
//! One frame per line (UTF-8, LF terminated). Responses carry `id` plus
//! `result`/`error`; server requests carry `id` plus `method`; notifications
//! carry `method` only. The reader never parses a partial line, and frames
//! over [`FRAME_CAP_BYTES`] close the transport instead of becoming events.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Maximum decoded frame size: 8 MiB. Oversize input closes the transport
/// and requires reconciliation; it is never parsed into a partial event.
pub const FRAME_CAP_BYTES: usize = 8 * 1024 * 1024;

/// JSON-RPC id. The captures use numbers on both directions.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RpcId {
    Num(u64),
    Str(String),
}

impl std::fmt::Display for RpcId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Num(n) => write!(f, "{n}"),
            Self::Str(s) => write!(f, "{s}"),
        }
    }
}

/// A classified inbound frame: a response to one of our requests, a server
/// request that needs an answer, or a one-way notification.
#[derive(Clone, Debug, PartialEq)]
pub enum IncomingFrame {
    Response {
        id: RpcId,
        result: Result<Value, RpcErrorObject>,
    },
    ServerRequest {
        id: RpcId,
        method: String,
        params: Value,
    },
    Notification {
        method: String,
        params: Value,
    },
}

/// A JSON-RPC error object from the server.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RpcErrorObject {
    pub code: i64,
    pub message: String,
}

/// Failure modes of the transport layer.
#[derive(Clone, Debug, PartialEq)]
pub enum RpcError {
    /// The write happened but no receipt arrived in time. The effect is
    /// unknown; the caller must reconcile instead of resending.
    OutcomeUnknown(String),
    /// The peer answered with a JSON-RPC error object (message sanitized).
    Rejected(String),
    /// A frame exceeded [`FRAME_CAP_BYTES`]. The transport is closed.
    Oversize { len: usize },
    /// Wait deadline with no send outstanding (event poll). Distinct from
    /// [`RpcError::OutcomeUnknown`], which is only used after a write.
    Timeout(String),
    /// Malformed frame or protocol violation (message sanitized).
    Protocol(String),
    /// The transport is closed (after oversize or IO failure).
    Closed(String),
    /// Underlying IO failure.
    Io(String),
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OutcomeUnknown(msg) => write!(f, "rpc outcome unknown: {msg}"),
            Self::Rejected(msg) => write!(f, "rpc rejected: {msg}"),
            Self::Oversize { len } => {
                write!(
                    f,
                    "rpc frame too large ({len} bytes, cap {FRAME_CAP_BYTES}); transport closed"
                )
            }
            Self::Timeout(msg) => write!(f, "rpc timeout: {msg}"),
            Self::Protocol(msg) => write!(f, "rpc protocol error: {msg}"),
            Self::Closed(msg) => write!(f, "rpc transport closed: {msg}"),
            Self::Io(msg) => write!(f, "rpc io error: {msg}"),
        }
    }
}

impl std::error::Error for RpcError {}

/// Encode a client request frame (without trailing newline).
pub fn encode_request(id: u64, method: &str, params: &Value) -> String {
    let params = canonical_params(params);
    format!(
        r#"{{"jsonrpc":"2.0","id":{id},"method":{method},"params":{params}}}"#,
        method = quote(method)
    )
}

/// Encode a response to a server request (without trailing newline).
pub fn encode_response(id: &RpcId, result: &Value) -> String {
    let id = match id {
        RpcId::Num(n) => n.to_string(),
        RpcId::Str(s) => quote(s),
    };
    format!(r#"{{"jsonrpc":"2.0","id":{id},"result":{result}}}"#)
}

/// Encode a client notification frame (without trailing newline).
pub fn encode_notification(method: &str, params: &Value) -> String {
    let params = canonical_params(params);
    format!(
        r#"{{"jsonrpc":"2.0","method":{method},"params":{params}}}"#,
        method = quote(method)
    )
}

fn quote(value: &str) -> String {
    serde_json::to_string(value).expect("method serializes")
}

fn canonical_params(params: &Value) -> String {
    if params.is_null() {
        "{}".to_string()
    } else {
        params.to_string()
    }
}

/// Classify one complete decoded line. Never called on oversize input: the
/// frame reader enforces the cap before this runs.
pub fn parse_frame(line: &str) -> Result<IncomingFrame, RpcError> {
    let value: Value = serde_json::from_str(line).map_err(|e| {
        RpcError::Protocol(format!("invalid JSON-RPC frame: {e}").replace('\n', " "))
    })?;
    let obj = value.as_object().ok_or_else(|| {
        RpcError::Protocol("invalid JSON-RPC frame: top level is not an object".to_string())
    })?;
    let id = obj
        .get("id")
        .map(|id| {
            serde_json::from_value::<RpcId>(id.clone())
                .map_err(|_| RpcError::Protocol("invalid JSON-RPC frame: bad id shape".to_string()))
        })
        .transpose()?;
    let method = obj
        .get("method")
        .and_then(Value::as_str)
        .map(str::to_string);
    match (id, method) {
        (Some(id), None) => {
            if let Some(error) = obj.get("error") {
                let message = error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown server error");
                let code = error.get("code").and_then(Value::as_i64).unwrap_or(0);
                return Ok(IncomingFrame::Response {
                    id,
                    result: Err(RpcErrorObject {
                        code,
                        message: sanitize_text(message).into_owned(),
                    }),
                });
            }
            let result = obj.get("result").cloned().unwrap_or(Value::Null);
            Ok(IncomingFrame::Response {
                id,
                result: Ok(result),
            })
        }
        (Some(id), Some(method)) => Ok(IncomingFrame::ServerRequest {
            id,
            method,
            params: obj.get("params").cloned().unwrap_or(Value::Null),
        }),
        (None, Some(method)) => Ok(IncomingFrame::Notification {
            method,
            params: obj.get("params").cloned().unwrap_or(Value::Null),
        }),
        (None, None) => Err(RpcError::Protocol(
            "invalid JSON-RPC frame: neither id nor method".to_string(),
        )),
    }
}

/// Build `thread/start` params with provider fallback forced off. The
/// `allow` argument exists so a `true` can only arrive explicitly; it is
/// ignored and `false` is always sent.
pub fn thread_start_params(base: Value, allow: bool) -> Value {
    let _ = allow;
    let mut params = base.as_object().cloned().unwrap_or_default();
    params.insert("allowProviderModelFallback".to_string(), Value::Bool(false));
    Value::Object(params)
}

/// Redact secret-bearing keys for logs and error messages. Anything that
/// plausibly carries a credential is replaced; unknown keys pass through.
pub fn sanitize_value(value: &Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| {
                    if is_secret_key(k) {
                        (k.clone(), Value::String("[redacted]".to_string()))
                    } else {
                        (k.clone(), sanitize_value(v))
                    }
                })
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(sanitize_value).collect()),
        Value::String(s) => Value::String(sanitize_text(s).into_owned()),
        other => other.clone(),
    }
}

pub(crate) fn is_secret_key(key: &str) -> bool {
    let lower = key.to_lowercase();
    lower.contains("token")
        || lower.contains("secret")
        || lower.contains("authorization")
        || lower.contains("api_key")
        || lower.contains("apikey")
        || lower.contains("password")
        || lower.contains("credential")
        || lower.contains("cookie")
        || lower.contains("set-cookie")
        || lower.contains("bearer")
}

/// True when any object key in the value looks secret-bearing, at any depth.
/// Used by inbox normalization to flag requests whose stored preview needs
/// the hardened path even when the harness did not declare a secret.
pub(crate) fn is_secret_key_in(value: &Value) -> bool {
    match value {
        Value::Object(map) => map
            .iter()
            .any(|(key, nested)| is_secret_key(key) || is_secret_key_in(nested)),
        Value::Array(items) => items.iter().any(is_secret_key_in),
        _ => false,
    }
}

/// Secret-bearing key names matched inside free text (case-insensitive).
/// Longest first so `set-cookie` wins over `cookie` at the same position.
const SECRET_HINTS: &[&str] = &[
    "authorization",
    "set-cookie",
    "credential",
    "password",
    "secret",
    "bearer",
    "token",
    "api_key",
    "api-key",
    "apikey",
    "cookie",
];

/// Redact secret material inside free text (tokens, not paths).
///
/// Beyond whole-string hex blobs, this scrubs `sk-...` style tokens,
/// `key=value` / `key: value` shapes for secret-bearing keys
/// (case-insensitive), `Bearer <credential>` pairs, and long opaque runs.
/// Ordinary prose passes through untouched.
pub fn sanitize_text(text: &str) -> std::borrow::Cow<'_, str> {
    if text.len() > 64 && text.chars().all(|c| c.is_ascii_hexdigit()) {
        return std::borrow::Cow::Owned("[redacted-hex]".to_string());
    }
    match redact_free_text(text) {
        Some(redacted) => std::borrow::Cow::Owned(redacted),
        None => std::borrow::Cow::Borrowed(text),
    }
}

/// Scan for secret patterns, returning the redacted string when at least one
/// match is found. All match boundaries sit on ASCII bytes, so slicing the
/// UTF-8 input at them is always safe.
fn redact_free_text(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    let mut out: Option<String> = None;
    let mut plain_start = 0;
    let mut i = 0;
    while i < bytes.len() {
        let mut hit = None;
        if i == 0 || !bytes[i - 1].is_ascii_alphanumeric() {
            hit = sk_token_len(&bytes[i..]);
            if hit.is_none() {
                hit = secret_assignment_len(&bytes[i..]);
            }
        }
        if hit.is_none() && (i == 0 || !is_token_byte(bytes[i - 1])) {
            hit = opaque_run_len(&bytes[i..]);
        }
        match hit {
            Some(len) => {
                let out = out.get_or_insert_with(String::new);
                out.push_str(&text[plain_start..i]);
                out.push_str("[redacted-secret]");
                i += len;
                plain_start = i;
            }
            None => i += 1,
        }
    }
    out.map(|mut redacted| {
        redacted.push_str(&text[plain_start..]);
        redacted
    })
}

/// Length of an `sk-...` style token at the start of `rest`, if any.
fn sk_token_len(rest: &[u8]) -> Option<usize> {
    if rest.len() > 3
        && rest[0].eq_ignore_ascii_case(&b's')
        && rest[1].eq_ignore_ascii_case(&b'k')
        && rest[2] == b'-'
    {
        let tail = rest[3..]
            .iter()
            .take_while(|b| b.is_ascii_alphanumeric() || **b == b'-' || **b == b'_')
            .count();
        if tail >= 8 {
            return Some(3 + tail);
        }
    }
    None
}

/// Length of a `key=value` / `key: value` / `Bearer <credential>` match at the
/// start of `rest` (the caller guarantees a word boundary before it).
fn secret_assignment_len(rest: &[u8]) -> Option<usize> {
    for hint in SECRET_HINTS {
        let key = hint.as_bytes();
        if rest.len() < key.len() || !rest[..key.len()].eq_ignore_ascii_case(key) {
            continue;
        }
        // `tokens=` (plural) is not `token=`; require a boundary after the key.
        if rest.len() > key.len()
            && (rest[key.len()].is_ascii_alphanumeric() || rest[key.len()] == b'_')
        {
            continue;
        }
        let mut j = key.len() + skip_set(&rest[key.len()..], b" \t\"'`");
        let paired = j < rest.len() && (rest[j] == b'=' || rest[j] == b':');
        if paired {
            j += 1 + skip_set(&rest[j + 1..], b" \t\"'`");
        } else if *hint != "bearer" {
            continue;
        }
        let mut end = j + secret_value_len(&rest[j..]);
        if end == j {
            continue;
        }
        while end > j && matches!(rest[end - 1], b'.' | b'!' | b'?') {
            end -= 1;
        }
        if end == j {
            continue;
        }
        if rest[j..end].eq_ignore_ascii_case(b"bearer")
            || rest[j..end].eq_ignore_ascii_case(b"basic")
        {
            let k = end + skip_set(&rest[end..], b" \t\"'`");
            let more = secret_value_len(&rest[k..]);
            if more > 0 {
                end = k + more;
            }
        }
        if !paired {
            // Space-separated `Bearer <credential>` only counts when the
            // credential itself looks token-shaped.
            if end - j < 8 {
                continue;
            }
        }
        return Some(end);
    }
    None
}

/// Length of a long opaque run at the start of `rest` (the caller guarantees
/// a run start). UUID-shaped runs are left alone: they identify, not conceal.
fn opaque_run_len(rest: &[u8]) -> Option<usize> {
    let len = rest.iter().take_while(|b| is_token_byte(**b)).count();
    if len >= 32 && !is_uuid_shaped(&rest[..len]) {
        Some(len)
    } else {
        None
    }
}

fn is_token_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'+' | b'.' | b'=')
}

fn is_uuid_shaped(run: &[u8]) -> bool {
    const DASHES: [usize; 4] = [8, 13, 18, 23];
    run.len() == 36
        && DASHES.iter().all(|&p| run[p] == b'-')
        && run
            .iter()
            .enumerate()
            .all(|(i, b)| DASHES.contains(&i) || b.is_ascii_hexdigit())
}

fn skip_set(rest: &[u8], set: &[u8]) -> usize {
    rest.iter().take_while(|b| set.contains(b)).count()
}

fn secret_value_len(rest: &[u8]) -> usize {
    rest.iter()
        .take_while(|b| is_secret_value_byte(**b))
        .count()
}

fn is_secret_value_byte(b: u8) -> bool {
    !b.is_ascii_whitespace() && !matches!(b, b'"' | b'\'' | b'`' | b',' | b';' | b'}' | b']' | b')')
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn classifies_all_three_frame_kinds() {
        assert!(matches!(
            parse_frame(r#"{"id":101,"result":{"thread":{}}}"#),
            Ok(IncomingFrame::Response { .. })
        ));
        assert!(matches!(
            parse_frame(r#"{"id":0,"method":"item/commandExecution/requestApproval","params":{}}"#),
            Ok(IncomingFrame::ServerRequest { .. })
        ));
        assert!(matches!(
            parse_frame(r#"{"method":"thread/started","params":{}}"#),
            Ok(IncomingFrame::Notification { .. })
        ));
        assert!(parse_frame(r#"{"foo":1}"#).is_err());
    }

    #[test]
    fn fallback_is_always_off() {
        let params = thread_start_params(json!({"model": "gpt-6-luna"}), true);
        assert_eq!(params["allowProviderModelFallback"], json!(false));
    }

    #[test]
    fn secrets_are_redacted_for_errors() {
        let value = json!({
            "model": "gpt-6-luna",
            "wsToken": "deadbeef",
            "nested": { "authorization": "Bearer abc", "cwd": "C:\\fixture" }
        });
        let clean = sanitize_value(&value);
        assert_eq!(clean["model"], json!("gpt-6-luna"));
        assert_eq!(clean["wsToken"], json!("[redacted]"));
        assert_eq!(clean["nested"]["authorization"], json!("[redacted]"));
        assert_eq!(clean["nested"]["cwd"], json!("C:\\fixture"));
        assert_eq!(sanitize_text(&"e".repeat(65)), "[redacted-hex]");
    }

    #[test]
    fn free_text_server_errors_do_not_leak_secrets() {
        let token = "sk-live-abcDEF1234567890abcdef1234567890";
        let frame = format!(
            "{{\"id\":7,\"error\":{{\"code\":-32000,\"message\":\"invalid token {token}; retry with a fresh credential\"}}}}"
        );
        match parse_frame(&frame) {
            Ok(IncomingFrame::Response {
                result: Err(error), ..
            }) => {
                assert!(
                    !error.message.contains(token),
                    "prose token leaked: {}",
                    error.message
                );
                assert!(
                    error.message.contains("[redacted"),
                    "prose token redacted: {}",
                    error.message
                );
            }
            other => panic!("expected error response, got {other:?}"),
        }

        let kv = sanitize_text(r#"auth failed: api_key="AKIAIOSFODNN7EXAMPLE" for host"#);
        assert!(
            !kv.contains("AKIAIOSFODNN7EXAMPLE"),
            "key=value leaked: {kv}"
        );
        assert!(kv.contains("[redacted"), "key=value redacted: {kv}");

        let bearer = sanitize_text("authorization: Bearer abcdef1234567890XYZ");
        assert!(
            !bearer.contains("abcdef1234567890XYZ"),
            "bearer credential leaked: {bearer}"
        );

        // Ordinary prose is untouched: no `=`/`:` after the keyword, and no
        // token-shaped material.
        let prose = sanitize_text("see the cookie policy before retrying");
        assert_eq!(prose, "see the cookie policy before retrying");
    }

    #[test]
    fn outbound_frames_carry_the_jsonrpc_member() {
        // The muse session host answers -32600 to turn/start frames
        // missing the JSON-RPC version member (verified live 2026-11-23);
        // every client frame carries it, while inbound parsing stays
        // lenient for older captures.
        let request = encode_request(3, "turn/start", &json!({"a": 1}));
        let frame: Value = serde_json::from_str(&request).expect("request encodes");
        assert_eq!(frame["jsonrpc"], json!("2.0"));
        assert_eq!(frame["id"], json!(3));
        let notify = encode_notification("initialized", &json!({}));
        let frame: Value = serde_json::from_str(&notify).expect("notify encodes");
        assert_eq!(frame["jsonrpc"], json!("2.0"));
        assert!(frame.get("id").is_none());
        let response = encode_response(&RpcId::Num(7), &json!({}));
        let frame: Value = serde_json::from_str(&response).expect("response encodes");
        assert_eq!(frame["jsonrpc"], json!("2.0"));
        assert_eq!(frame["id"], json!(7));
    }
}
