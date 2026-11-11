//! Leader conversation: `GET /v1/runs/{id}/chat` and
//! `POST /v1/runs/{id}/chat`.
//!
//! Discussion only: posting stores one message through the budget and
//! pause gates and never approves a plan, answers inbox, dispatches
//! work or spends budget. Reading is open to every authenticated
//! caller; posting needs workflow rights.

use std::collections::HashMap;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use central_domain::RunId;
use serde::Deserialize;

use super::auth::{app_error_response, authenticate};
use super::AppState;
use crate::api::dto::{encode_cursor, parse_after, parse_limit};
use crate::api::errors::{error_response, ApiError};
use crate::api::read_models::project_chat_message;

#[derive(Deserialize)]
pub struct PostChatBody {
    pub body: String,
}

pub async fn list_chat(
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
    let after = parse_after(
        query.get("after").map(|text| text.as_str()),
        "chat",
        &run_text,
    )?;
    let limit = parse_limit(query.get("limit").map(|text| text.as_str()))?;
    let messages = state
        .store
        .chat_after(run_id, after, limit)
        .await
        .map_err(app_error_response)?;
    let items: Vec<serde_json::Value> = messages.iter().map(project_chat_message).collect();
    let next_cursor = items
        .last()
        .map(|last| encode_cursor("chat", &run_text, last["seq"].as_u64().unwrap_or(after)));
    Ok(Json(
        serde_json::json!({ "items": items, "next_cursor": next_cursor }),
    ))
}

pub async fn post_chat(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(raw_id): Path<String>,
    Json(body): Json<PostChatBody>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let auth = authenticate(&state, &headers).await?;
    auth.require_control()?;
    let run_id = raw_id.parse::<RunId>().map_err(|_| {
        error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            format!("invalid run id: {raw_id}"),
        )
    })?;
    let author = auth.client_id.as_deref().unwrap_or("owner");
    let message = central_application::conversation::post_message(
        &*state.store,
        &*state.clock,
        run_id,
        author,
        &body.body,
    )
    .await
    .map_err(app_error_response)?;
    Ok(Json(project_chat_message(&message)))
}
