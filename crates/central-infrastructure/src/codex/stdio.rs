//! Line-framed stdio transport for the Codex app-server.
//!
//! [`RpcClient`] speaks JSON-RPC 2.0 over any bidirectional async stream
//! (a child-process stdio pair once the P2/T2 supervisor wires it; an
//! in-memory duplex pipe in tests). Reads never block on approvals: a
//! response wait loop queues interleaved server requests and notifications
//! instead of dropping them, and answers go out through [`RpcClient::respond`]
//! independently of any outstanding request.

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::wire::{
    encode_notification, encode_request, encode_response, parse_frame, IncomingFrame, RpcError,
    RpcErrorObject, RpcId, FRAME_CAP_BYTES,
};

/// A classified inbound event for the consumer: an incoming request (from
/// either side of the wire) or a one-way notification. Responses are never
/// surfaced here; they belong to the matching [`RpcClient::recv_response`].
#[derive(Clone, Debug)]
pub enum IncomingEvent {
    Request {
        id: RpcId,
        method: String,
        params: Value,
    },
    Notification {
        method: String,
        params: Value,
    },
}

/// One buffered inbound event.
#[derive(Clone, Debug)]
pub struct BufferedEvent {
    pub id: Option<RpcId>,
    pub method: String,
    pub params: Value,
    pub is_request: bool,
}

/// Bounded FIFO for inbound events. When full, the incoming event is dropped
/// and the buffer latches `overflowed`: the documented recovery is a full
/// reconcile, never silent resumption with a gappy buffer.
#[derive(Clone, Debug)]
pub struct EventBuffer {
    capacity: usize,
    items: VecDeque<BufferedEvent>,
    overflowed: bool,
}

impl EventBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            items: VecDeque::new(),
            overflowed: false,
        }
    }

    /// Push an event. Returns false (and latches overflow) when full.
    pub fn push(&mut self, event: BufferedEvent) -> bool {
        if self.items.len() >= self.capacity {
            self.overflowed = true;
            return false;
        }
        self.items.push_back(event);
        true
    }

    pub fn pop(&mut self) -> Option<BufferedEvent> {
        self.items.pop_front()
    }

    pub fn drain(&mut self) -> Vec<BufferedEvent> {
        self.items.drain(..).collect()
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn overflowed(&self) -> bool {
        self.overflowed
    }
}

fn buffered_request(id: RpcId, method: String, params: Value) -> BufferedEvent {
    BufferedEvent {
        id: Some(id),
        method,
        params,
        is_request: true,
    }
}

fn buffered_notification(method: String, params: Value) -> BufferedEvent {
    BufferedEvent {
        id: None,
        method,
        params,
        is_request: false,
    }
}

/// JSON-RPC client over a bidirectional stream.
///
/// Correlates responses by id, so chunked reads and out-of-order responses
/// are safe. A request that times out after its write is reported as
/// [`RpcError::OutcomeUnknown`] and is never resent by this client.
pub struct RpcClient<S> {
    io: S,
    read_buf: Vec<u8>,
    next_id: u64,
    stash: HashMap<RpcId, Result<Value, RpcErrorObject>>,
    queued: EventBuffer,
    closed: bool,
    reconcile: bool,
}

impl<S: AsyncRead + AsyncWrite + Unpin> RpcClient<S> {
    pub fn new(io: S) -> Self {
        Self::with_notification_capacity(io, 256)
    }

    pub fn with_notification_capacity(io: S, capacity: usize) -> Self {
        Self {
            io,
            read_buf: Vec::new(),
            next_id: 1,
            stash: HashMap::new(),
            queued: EventBuffer::new(capacity),
            closed: false,
            reconcile: false,
        }
    }

    /// Closed after oversize or IO failure. A closed client refuses writes.
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// True once events may have been lost (overflow or close): the caller
    /// must reconcile instead of resuming silently.
    pub fn reconcile_required(&self) -> bool {
        self.reconcile || self.queued.overflowed()
    }

    fn fail_closed(&mut self, reason: &str) {
        self.closed = true;
        self.reconcile = true;
        let _ = reason;
    }

    /// Read one LF-terminated line. Returns `None` on clean EOF with no
    /// partial line. Enforces [`FRAME_CAP_BYTES`]: oversize input closes the
    /// transport and is never parsed.
    async fn read_line(&mut self) -> Result<Option<String>, RpcError> {
        if self.closed {
            return Err(RpcError::Closed("transport closed".to_string()));
        }
        let mut chunk = [0u8; 65536];
        loop {
            if let Some(pos) = self.read_buf.iter().position(|&b| b == b'\n') {
                let line: Vec<u8> = self.read_buf.drain(..pos + 1).collect();
                let mut text = String::from_utf8(line).map_err(|_| {
                    RpcError::Protocol("invalid JSON-RPC frame: not UTF-8".to_string())
                })?;
                if text.ends_with('\n') {
                    text.pop();
                }
                if text.ends_with('\r') {
                    text.pop();
                }
                return Ok(Some(text));
            }
            if self.read_buf.len() > FRAME_CAP_BYTES {
                let len = self.read_buf.len();
                self.read_buf.clear();
                self.fail_closed("oversize frame");
                return Err(RpcError::Oversize { len });
            }
            let n = self.io.read(&mut chunk).await.map_err(|e| {
                let msg = format!("read failed: {e}");
                self.fail_closed(&msg);
                RpcError::Io(msg)
            })?;
            if n == 0 {
                if self.read_buf.is_empty() {
                    return Ok(None);
                }
                self.read_buf.clear();
                self.fail_closed("truncated frame");
                return Err(RpcError::Protocol(
                    "truncated frame: EOF before newline".to_string(),
                ));
            }
            if self.read_buf.len() + n > FRAME_CAP_BYTES {
                let len = self.read_buf.len() + n;
                self.read_buf.clear();
                self.fail_closed("oversize frame");
                return Err(RpcError::Oversize { len });
            }
            self.read_buf.extend_from_slice(&chunk[..n]);
        }
    }

    /// Read and classify the next frame.
    async fn read_frame(&mut self) -> Result<Option<IncomingFrame>, RpcError> {
        match self.read_line().await? {
            None => Ok(None),
            Some(line) => parse_frame(&line).map(Some),
        }
    }

    async fn write_line(&mut self, line: &str) -> Result<(), RpcError> {
        if self.closed {
            return Err(RpcError::Closed("transport closed".to_string()));
        }
        self.io.write_all(line.as_bytes()).await.map_err(|e| {
            let msg = format!("write failed: {e}");
            self.fail_closed(&msg);
            RpcError::Io(msg)
        })?;
        self.io.write_all(b"\n").await.map_err(|e| {
            let msg = format!("write failed: {e}");
            self.fail_closed(&msg);
            RpcError::Io(msg)
        })?;
        self.io.flush().await.map_err(|e| {
            let msg = format!("flush failed: {e}");
            self.fail_closed(&msg);
            RpcError::Io(msg)
        })?;
        Ok(())
    }

    /// Write a complete raw frame (test and peer-driving helper).
    pub async fn send_raw(&mut self, line: &str) -> Result<(), RpcError> {
        let trimmed = line.trim_end_matches(['\n', '\r']);
        self.write_line(trimmed).await
    }

    /// Send a request and return its id. The response arrives via
    /// [`RpcClient::recv_response`].
    pub async fn send_request(&mut self, method: &str, params: Value) -> Result<u64, RpcError> {
        let id = self.next_id;
        self.next_id += 1;
        self.write_line(&encode_request(id, method, &params))
            .await?;
        Ok(id)
    }

    /// Wait for the response to `id`. Interleaved server requests and
    /// notifications are queued, not lost. A timeout after the write is
    /// [`RpcError::OutcomeUnknown`]; this client never resends.
    pub async fn recv_response(
        &mut self,
        id: u64,
        timeout_wait: Duration,
    ) -> Result<Value, RpcError> {
        let wanted = RpcId::Num(id);
        if let Some(stashed) = self.stash.remove(&wanted) {
            return stashed.map_err(|e| RpcError::Rejected(sanitized_error(&e)));
        }
        loop {
            let frame = tokio::time::timeout(timeout_wait, self.read_frame())
                .await
                .map_err(|_| {
                    RpcError::OutcomeUnknown(format!(
                        "no receipt for request {id} within {}ms; effect unknown, never resend",
                        timeout_wait.as_millis()
                    ))
                })??;
            match frame {
                None => {
                    self.fail_closed("peer closed");
                    return Err(RpcError::Closed(
                        "peer closed before responding".to_string(),
                    ));
                }
                Some(IncomingFrame::Response { id: got, result }) => {
                    if got == wanted {
                        return result.map_err(|e| RpcError::Rejected(sanitized_error(&e)));
                    }
                    self.stash.insert(got, result);
                }
                Some(IncomingFrame::ServerRequest { id, method, params }) => {
                    self.push_queued(buffered_request(id, method, params));
                }
                Some(IncomingFrame::Notification { method, params }) => {
                    self.push_queued(buffered_notification(method, params));
                }
            }
        }
    }

    /// Send a request and wait for its response.
    pub async fn request(
        &mut self,
        method: &str,
        params: Value,
        timeout_wait: Duration,
    ) -> Result<Value, RpcError> {
        let id = self.send_request(method, params).await?;
        self.recv_response(id, timeout_wait).await
    }

    /// Answer a server request. Independent of any outstanding request wait:
    /// the reader is never blocked by an approval.
    pub async fn respond(&mut self, id: &RpcId, result: Value) -> Result<(), RpcError> {
        self.write_line(&encode_response(id, &result)).await
    }

    /// Send a client notification.
    pub async fn notify(&mut self, method: &str, params: Value) -> Result<(), RpcError> {
        self.write_line(&encode_notification(method, &params)).await
    }

    /// Next inbound event: a request from either side, or a notification.
    /// Queued events drain first in arrival order, then the wire is read.
    pub async fn poll_event(&mut self, timeout_wait: Duration) -> Result<IncomingEvent, RpcError> {
        if let Some(event) = self.queued.pop() {
            return Ok(event.into());
        }
        loop {
            let frame = tokio::time::timeout(timeout_wait, self.read_frame())
                .await
                .map_err(|_| {
                    RpcError::Timeout(format!("no event within {}ms", timeout_wait.as_millis()))
                })??;
            match frame {
                None => {
                    self.fail_closed("peer closed");
                    return Err(RpcError::Closed("peer closed".to_string()));
                }
                Some(IncomingFrame::Response { id, result }) => {
                    self.stash.insert(id, result);
                    if let Some(event) = self.queued.pop() {
                        return Ok(event.into());
                    }
                }
                Some(IncomingFrame::ServerRequest { id, method, params }) => {
                    return Ok(IncomingEvent::Request { id, method, params });
                }
                Some(IncomingFrame::Notification { method, params }) => {
                    return Ok(IncomingEvent::Notification { method, params });
                }
            }
        }
    }

    fn push_queued(&mut self, event: BufferedEvent) {
        if !self.queued.push(event) {
            self.reconcile = true;
        }
    }
}

fn sanitized_error(error: &RpcErrorObject) -> String {
    format!(
        "code {}: {}",
        error.code,
        super::wire::sanitize_text(&error.message)
    )
}

impl From<BufferedEvent> for IncomingEvent {
    fn from(event: BufferedEvent) -> Self {
        if event.is_request {
            Self::Request {
                id: event.id.expect("request events carry an id"),
                method: event.method,
                params: event.params,
            }
        } else {
            Self::Notification {
                method: event.method,
                params: event.params,
            }
        }
    }
}
