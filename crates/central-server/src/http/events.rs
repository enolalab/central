//! Events: `GET /v1/runs/{id}/events` (cursor pages) and
//! `GET /v1/runs/{id}/events/stream` (authenticated SSE).
//!
//! Pages and the stream share one projection (`project_event`) over
//! the durable seq log. `after` is a bare seq or an opaque cursor
//! bound to this run; `known_epoch` (or the stream's epoch view)
//! mismatching the daemon instance is 409 / a `resync_required`
//! frame — the client refetches the snapshot, never resumes blindly.
//! SSE carries the durable seq as the event id, supports
//! `Last-Event-ID`, sends a keepalive every 15 s, and applies
//! backpressure through a bounded channel (a slow subscriber stalls
//! the pump, never grows an unbounded buffer). Revoked credentials
//! close open streams at the next tick. Auth is header-only: a token
//! in the query string is ignored.

use std::collections::HashMap;
use std::convert::Infallible;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::Json;
use central_domain::RunId;
use futures_core::Stream;
use tokio::sync::mpsc;

use super::auth::{app_error_response, authenticate};
use super::AppState;
use crate::api::dto::{encode_cursor, parse_after, parse_limit};
use crate::api::errors::{error_response, ApiError};
use crate::api::read_models::project_event;

pub async fn list_events(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(raw_id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authenticate(&state, &headers).await?;
    let run_id = raw_id.parse::<RunId>().map_err(|_| {
        error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            format!("invalid run id: {raw_id}"),
        )
    })?;
    let run_text = run_id.to_string();
    if let Some(known) = query.get("known_epoch") {
        if known != &state.instance_epoch {
            return Err(error_response(
                StatusCode::CONFLICT,
                "resync_required",
                "daemon instance changed since this cursor; refetch the snapshot, then stream from its cursor",
            ));
        }
    }
    let after = parse_after(
        query.get("after").map(|text| text.as_str()),
        "events",
        &run_text,
    )?;
    let limit = parse_limit(query.get("limit").map(|text| text.as_str()))?;
    let snapshot = state
        .store
        .snapshot_with_cursor(run_id)
        .await
        .map_err(app_error_response)?
        .ok_or_else(|| {
            error_response(
                StatusCode::NOT_FOUND,
                "not_found",
                format!("run {run_id} not found"),
            )
        })?;
    if after > snapshot.last_seq {
        return Err(error_response(
            StatusCode::CONFLICT,
            "resync_required",
            format!(
                "cursor {after} is beyond the durable history (last {}); refetch the snapshot",
                snapshot.last_seq
            ),
        ));
    }
    let rows = state
        .store
        .events_after(run_id, after, limit)
        .await
        .map_err(app_error_response)?;
    let items: Vec<serde_json::Value> = rows
        .iter()
        .map(|row| project_event(row.seq, &row.event))
        .collect();
    let next_cursor = items
        .last()
        .map(|last| encode_cursor("events", &run_text, last["seq"].as_u64().unwrap_or(after)));
    Ok(Json(serde_json::json!({
        "items": items,
        "next_cursor": next_cursor,
    })))
}

/// One SSE frame per durable event, then live polling. The stream ends
/// when the client leaves, when the credential is revoked, or right
/// after a `resync_required` frame (terminal signal, not a retryable
/// offset).
pub async fn stream_events(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(raw_id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<axum::response::Response, ApiError> {
    let auth = authenticate(&state, &headers).await?;
    let run_id = raw_id.parse::<RunId>().map_err(|_| {
        error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            format!("invalid run id: {raw_id}"),
        )
    })?;
    let run_text = run_id.to_string();
    let after = match query.get("after") {
        Some(text) => parse_after(Some(text.as_str()), "events", &run_text)?,
        None => match headers
            .get("last-event-id")
            .and_then(|value| value.to_str().ok())
        {
            Some(text) if !text.trim().is_empty() => {
                parse_after(Some(text.trim()), "events", &run_text)?
            }
            _ => 0,
        },
    };
    if let Some(known) = query.get("known_epoch") {
        if known != &state.instance_epoch {
            return Err(error_response(
                StatusCode::CONFLICT,
                "resync_required",
                "daemon instance changed since this cursor; refetch the snapshot, then stream from its cursor",
            ));
        }
    }
    // The run must exist before any frame flows.
    let snapshot = state
        .store
        .snapshot_with_cursor(run_id)
        .await
        .map_err(app_error_response)?
        .ok_or_else(|| {
            error_response(
                StatusCode::NOT_FOUND,
                "not_found",
                format!("run {run_id} not found"),
            )
        })?;
    let (sender, receiver) = mpsc::channel::<Result<SseEvent, Infallible>>(32);
    let pump = StreamPump {
        state: state.clone(),
        run_id,
        after,
        last_seq: snapshot.last_seq,
        client_id: auth.client_id.clone(),
        sender,
    };
    tokio::spawn(pump.run());
    Ok(axum::response::IntoResponse::into_response(
        Sse::new(FrameStream { receiver }).keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("stream-alive"),
        ),
    ))
}

struct StreamPump {
    state: AppState,
    run_id: RunId,
    after: u64,
    last_seq: u64,
    client_id: Option<String>,
    sender: mpsc::Sender<Result<SseEvent, Infallible>>,
}

impl StreamPump {
    fn resync_frame(last_seq: u64) -> SseEvent {
        SseEvent::default()
            .event("resync_required")
            .id(last_seq.to_string())
            .data("{\"reason\":\"cursor beyond durable history or instance changed; refetch the snapshot\"}")
    }

    async fn run(self) {
        // A cursor beyond history is a lost client: one terminal frame.
        if self.after > self.last_seq {
            let _ = self
                .sender
                .send(Ok(Self::resync_frame(self.last_seq)))
                .await;
            return;
        }
        let mut cursor = self.after;
        loop {
            let rows = match self
                .state
                .store
                .events_after(self.run_id, cursor, 100)
                .await
            {
                Ok(rows) => rows,
                Err(_) => return,
            };
            for row in rows {
                let view = project_event(row.seq, &row.event);
                let data = serde_json::to_string(&view).unwrap_or_else(|_| "{}".to_string());
                let frame = SseEvent::default().id(row.seq.to_string()).data(data);
                // Bounded channel: a slow subscriber stalls the pump
                // here instead of buffering without bound.
                if self.sender.send(Ok(frame)).await.is_err() {
                    return;
                }
                cursor = row.seq;
            }
            // Revocation closes open streams: re-check the record every
            // tick (owner tokens have no record and stay open).
            if let Some(client_id) = &self.client_id {
                match self.state.store.get_client(client_id).await {
                    Ok(Some(record)) if !record.revoked => {}
                    _ => return,
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

struct FrameStream {
    receiver: mpsc::Receiver<Result<SseEvent, Infallible>>,
}

impl Stream for FrameStream {
    type Item = Result<SseEvent, Infallible>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}
