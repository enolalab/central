//! Leader conversation: `GET /v1/runs/{id}/chat` and
//! `POST /v1/runs/{id}/chat`.
//!
//! Discussion only: posting stores one message through the budget and
//! pause gates and never approves a plan, answers inbox, dispatches
//! work or spends budget. Reading is open to every authenticated
//! caller; posting needs workflow rights.

use std::collections::HashMap;
use std::path::Path as FsPath;

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

/// Submit a user message and queue a durable leader turn for the web
/// conversation. The CLI's notes-only chat endpoint keeps its contract.
pub async fn post_leader_chat(
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
            "invalid run id",
        )
    })?;
    let snapshot = state
        .store
        .load(run_id)
        .await
        .map_err(app_error_response)?
        .ok_or_else(|| error_response(StatusCode::NOT_FOUND, "not_found", "run not found"))?;
    let profile = snapshot.leader_profile.ok_or_else(|| {
        error_response(
            StatusCode::CONFLICT,
            "conflict",
            "choose a leader model first",
        )
    })?;
    let record = state
        .store
        .get_profile(&profile.to_string())
        .await
        .map_err(app_error_response)?
        .ok_or_else(|| {
            error_response(
                StatusCode::CONFLICT,
                "conflict",
                "leader profile is missing",
            )
        })?;
    if super::profiles::capability_for(&state, &record).await["status"] != "supported" {
        return Err(error_response(
            StatusCode::CONFLICT,
            "conflict",
            "leader model is not available",
        ));
    }
    let message = body.body.trim().to_owned();
    // Local project metadata is collected for the owner. Control clients
    // still get the run's stored path in the prompt.
    let (local_checkout, entries) = if auth.is_owner() {
        let repository = snapshot.repository.clone();
        tokio::task::spawn_blocking(move || workspace_entries(FsPath::new(&repository)))
            .await
            .unwrap_or_default()
    } else {
        (false, Vec::new())
    };
    let workspace_read_only = local_checkout && record.provider == "codex";
    let inspect_reason = if workspace_read_only {
        None
    } else if !local_checkout {
        Some("workspace không phải checkout local")
    } else {
        Some("provider không phải Codex")
    };
    let input = central_application::discussion::prompt(
        &snapshot,
        &message,
        &entries,
        workspace_read_only,
        inspect_reason,
    )
    .map_err(app_error_response)?;
    let attempt = state.ids.next_attempt_id();
    let runtime = central_application::Runtime::new(
        state.store.clone(),
        state.clock.clone(),
        state.ids.clone(),
        state.agent.clone(),
        run_id,
    );
    let receipt = runtime
        .execute(
            auth.actor,
            central_domain::CommandEnvelope {
                id: state.ids.next_command_id(),
                expected_revision: Some(snapshot.revision),
                command: central_domain::Command::QueueLeaderDiscussion {
                    attempt,
                    workspace_read_only,
                    message,
                    input,
                },
            },
        )
        .await
        .map_err(app_error_response)?;
    state.scan_notify.notify_one();
    Ok(Json(
        serde_json::json!({"receipt": receipt, "attempt_id": attempt}),
    ))
}

fn workspace_entries(root: &FsPath) -> (bool, Vec<String>) {
    // Only inspect the root of a selected Git checkout. A worktree can
    // have a regular .git file instead of a directory.
    if !root.is_absolute()
        || !std::fs::symlink_metadata(root.join(".git"))
            .is_ok_and(|entry| entry.is_file() || entry.is_dir())
    {
        return (false, Vec::new());
    }
    let Ok(directory) = std::fs::read_dir(root) else {
        return (false, Vec::new());
    };
    let mut entries = Vec::new();
    for entry in directory.take(128).flatten() {
        let Ok(mut name) = entry.file_name().into_string() else {
            continue;
        };
        if name.starts_with('.') || name.chars().any(char::is_control) {
            continue;
        }
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        if kind.is_dir() {
            name.push('/');
        } else if !kind.is_file() {
            continue;
        }
        entries.push(name);
        if entries.len() == 64 {
            break;
        }
    }
    entries.sort_unstable();
    (true, entries)
}
