//! Loopback bearer-token auth with scoped client credentials (P4).
//!
//! The actor is derived from the presented token — never from a request
//! body. Three credential kinds exist: the owner bearer token (the
//! daemon's own user), client tokens (`<client_id>.<secret>` with
//! `read`/`control` scope), and agent-bridge tokens (one role, one
//! attempt, results import only). Query-string tokens are ignored,
//! `Origin` is enforced by middleware (see `origin`), and every failure
//! carries a stable `ApiErrorV1` code with no secret content.

use axum::http::{HeaderMap, StatusCode};
use central_application::ports::store::ClientScope;
use central_domain::{Actor, ActorKind};

use super::AppState;
use crate::api::errors::{error_response, ApiError};

/// What one authenticated request may do.
#[derive(Clone, Debug)]
pub enum AuthScope {
    /// The daemon owner: everything, including client management,
    /// maintenance and shutdown.
    Owner,
    /// A `read` client: GET only.
    Read,
    /// A `control` client: workflow control as the user, but no client
    /// management, maintenance or shutdown.
    Control,
    /// An agent bridge: result/handover import for its bound attempt
    /// plus reads. Never approvals, never other attempts.
    Bridge {
        role: String,
        attempt: central_domain::AttemptId,
    },
}

#[derive(Clone, Debug)]
pub struct Auth {
    pub actor: Actor,
    pub scope: AuthScope,
    pub client_id: Option<String>,
}

impl Auth {
    pub fn is_owner(&self) -> bool {
        matches!(self.scope, AuthScope::Owner)
    }

    /// Workflow writes (commands, chat, sync, usage, results, profiles).
    pub fn can_control(&self) -> bool {
        matches!(self.scope, AuthScope::Owner | AuthScope::Control)
    }

    /// Client management, maintenance, shutdown: owner only.
    pub fn require_owner(&self) -> Result<(), ApiError> {
        if self.is_owner() {
            Ok(())
        } else {
            Err(error_response(
                StatusCode::FORBIDDEN,
                "forbidden",
                "client management, maintenance and shutdown are owner-only",
            ))
        }
    }

    /// Workflow writes: owner or control. Bridge tokens never pass
    /// here — they import through their own endpoints.
    pub fn require_control(&self) -> Result<(), ApiError> {
        if self.can_control() {
            Ok(())
        } else {
            Err(error_response(
                StatusCode::FORBIDDEN,
                "forbidden",
                "this credential cannot write: read and agent-bridge tokens are read/import-only",
            ))
        }
    }
}

fn bearer_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(|token| token.trim().to_string())
}

/// Resolve the authenticated caller. Missing or wrong tokens are 401
/// `auth_required`; revoked, expired or out-of-scope credentials fail
/// closed at the endpoint's scope check, never here.
pub async fn authenticate(state: &AppState, headers: &HeaderMap) -> Result<Auth, ApiError> {
    let presented = bearer_token(headers).ok_or_else(|| {
        error_response(
            StatusCode::UNAUTHORIZED,
            "auth_required",
            "missing or invalid bearer token",
        )
    })?;
    if presented == state.token {
        return Ok(Auth {
            actor: state.actor.clone(),
            scope: AuthScope::Owner,
            client_id: None,
        });
    }
    let Some((client_id, secret)) =
        central_application::client_access::split_client_token(&presented)
    else {
        return Err(error_response(
            StatusCode::UNAUTHORIZED,
            "auth_required",
            "missing or invalid bearer token",
        ));
    };
    let record = central_application::client_access::verify_client(
        &*state.store,
        &*state.clock,
        client_id,
        secret,
    )
    .await
    .map_err(|_| {
        error_response(
            StatusCode::UNAUTHORIZED,
            "auth_required",
            "missing or invalid bearer token",
        )
    })?;
    let actor_id = client_id
        .parse::<uuid::Uuid>()
        .map(central_domain::ActorId::from_uuid)
        .unwrap_or_else(|_| state.actor.id);
    match record.scope {
        ClientScope::Read => Ok(Auth {
            actor: Actor {
                id: actor_id,
                kind: ActorKind::User,
            },
            scope: AuthScope::Read,
            client_id: Some(record.client_id),
        }),
        ClientScope::Control => Ok(Auth {
            actor: Actor {
                id: actor_id,
                kind: ActorKind::User,
            },
            scope: AuthScope::Control,
            client_id: Some(record.client_id),
        }),
        ClientScope::Bridge { role, attempt } => Ok(Auth {
            actor: Actor {
                id: actor_id,
                kind: ActorKind::Worker,
            },
            scope: AuthScope::Bridge { role, attempt },
            client_id: Some(record.client_id),
        }),
    }
}

pub use crate::api::errors::app_error_response;
