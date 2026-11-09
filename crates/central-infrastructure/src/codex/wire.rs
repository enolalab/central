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
        r#"{{"id":{id},"method":{method},"params":{params}}}"#,
        method = quote(method)
    )
}

/// Encode a response to a server request (without trailing newline).
pub fn encode_response(id: &RpcId, result: &Value) -> String {
    let id = match id {
        RpcId::Num(n) => n.to_string(),
        RpcId::Str(s) => quote(s),
    };
    format!(r#"{{"id":{id},"result":{result}}}"#)
}

/// Encode a client notification frame (without trailing newline).
pub fn encode_notification(method: &str, params: &Value) -> String {
    let params = canonical_params(params);
    format!(
        r#"{{"method":{method},"params":{params}}}"#,
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

fn is_secret_key(key: &str) -> bool {
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

/// Redact bearer-looking material inside free text (tokens, not paths).
pub fn sanitize_text(text: &str) -> std::borrow::Cow<'_, str> {
    if text.len() > 64 && text.chars().all(|c| c.is_ascii_hexdigit()) {
        return std::borrow::Cow::Owned("[redacted-hex]".to_string());
    }
    std::borrow::Cow::Borrowed(text)
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
}
