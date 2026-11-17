//! Minimal async HTTP client for one owned opencode server.
//!
//! Every request carries HTTP Basic auth (`opencode` + the per-session
//! server secret). Error classification is the whole point: a timeout on
//! a prompt write is [`ClientError::OutcomeUnknown`] (the turn may still
//! run — never resend), a refused connection is `Rejected` (definitely
//! unsent), and anything else is `Transport` (reconcile-required).

use std::time::Duration;

/// Errors from one opencode server, classified for the gateway.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientError {
    /// Definitely unsent (connection refused, DNS, 4xx validation).
    Rejected(String),
    /// May have landed (timeout on a prompt write). Never resend.
    OutcomeUnknown(String),
    /// The transport itself is suspect (5xx, broken body). Reconcile.
    Transport(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rejected(message) => write!(f, "rejected: {message}"),
            Self::OutcomeUnknown(message) => write!(f, "outcome unknown: {message}"),
            Self::Transport(message) => write!(f, "transport: {message}"),
        }
    }
}

impl std::error::Error for ClientError {}

/// One owned server: base URL plus its secret. Cheap to clone.
#[derive(Clone, Debug)]
pub struct OpencodeClient {
    http: reqwest::Client,
    base: String,
    password: String,
}

impl OpencodeClient {
    pub fn new(base_url: &str, password: &str, timeout: Duration) -> Result<Self, ClientError> {
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|error| {
                ClientError::Transport(format!("cannot build HTTP client: {error}"))
            })?;
        Ok(Self {
            http,
            base: base_url.trim_end_matches('/').to_string(),
            password: password.to_string(),
        })
    }

    fn get(&self, path: &str) -> reqwest::RequestBuilder {
        self.http
            .get(format!("{}{path}", self.base))
            .basic_auth("opencode", Some(&self.password))
    }

    fn post(&self, path: &str) -> reqwest::RequestBuilder {
        self.http
            .post(format!("{}{path}", self.base))
            .basic_auth("opencode", Some(&self.password))
    }

    async fn data(
        response: Result<reqwest::Response, reqwest::Error>,
        what: &str,
    ) -> Result<serde_json::Value, ClientError> {
        let response = response.map_err(|error| {
            if error.is_timeout() {
                ClientError::OutcomeUnknown(format!(
                    "{what} timed out; it may still run server-side"
                ))
            } else if error.is_connect() {
                ClientError::Rejected(format!("{what}: cannot reach the owned server ({error})"))
            } else {
                ClientError::Transport(format!("{what} failed: {error}"))
            }
        })?;
        let status = response.status();
        let body: serde_json::Value = response.json().await.map_err(|error| {
            ClientError::Transport(format!("{what}: unreadable body (HTTP {status}): {error}"))
        })?;
        if !status.is_success() {
            let detail = body
                .get("message")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown server error");
            if status.is_client_error() {
                return Err(ClientError::Rejected(format!(
                    "{what} refused (HTTP {status}): {detail}"
                )));
            }
            return Err(ClientError::Transport(format!(
                "{what} failed (HTTP {status}): {detail}"
            )));
        }
        Ok(body.get("data").cloned().unwrap_or(body))
    }

    /// Health + version pin. Called once at open.
    pub async fn info(&self) -> Result<serde_json::Value, ClientError> {
        Self::data(self.get("/api/info").send().await, "server info").await
    }

    /// Create a server session. Returns the server session id.
    pub async fn create_session(&self, title: &str) -> Result<String, ClientError> {
        let body = Self::data(
            self.post("/api/session")
                .json(&serde_json::json!({ "title": title }))
                .send()
                .await,
            "session create",
        )
        .await?;
        body.get("id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| {
                ClientError::Transport("session create did not return a session id".to_string())
            })
    }

    /// Send one prompt. Returns the user message id. A timeout here is
    /// OutcomeUnknown — never resend.
    pub async fn prompt(&self, session: &str, text: &str) -> Result<String, ClientError> {
        let body = Self::data(
            self.post(&format!("/api/session/{session}/prompt"))
                .json(&serde_json::json!({ "text": text }))
                .send()
                .await,
            "session prompt",
        )
        .await?;
        body.get("id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| {
                ClientError::Transport("session prompt did not return a message id".to_string())
            })
    }

    /// Full message list, oldest first.
    pub async fn messages(&self, session: &str) -> Result<Vec<serde_json::Value>, ClientError> {
        let body = Self::data(
            self.get(&format!("/api/session/{session}/message"))
                .send()
                .await,
            "session messages",
        )
        .await?;
        body.as_array()
            .cloned()
            .ok_or_else(|| ClientError::Transport("session messages is not a list".to_string()))
    }

    /// Open permission requests for one session.
    pub async fn permissions(&self, session: &str) -> Result<Vec<serde_json::Value>, ClientError> {
        let body = Self::data(
            self.get(&format!("/api/session/{session}/permission"))
                .send()
                .await,
            "session permissions",
        )
        .await?;
        body.as_array()
            .cloned()
            .ok_or_else(|| ClientError::Transport("session permissions is not a list".to_string()))
    }

    /// Answer one permission request. `decision` is `once` or `reject`
    /// (Central never sends `always`).
    pub async fn reply_permission(
        &self,
        session: &str,
        request_id: &str,
        decision: &str,
        message: Option<&str>,
    ) -> Result<(), ClientError> {
        let mut payload = serde_json::json!({ "decision": decision });
        if let Some(message) = message {
            payload["message"] = serde_json::Value::String(message.to_string());
        }
        Self::data(
            self.post(&format!(
                "/api/session/{session}/permission/{request_id}/reply"
            ))
            .json(&payload)
            .send()
            .await,
            "permission reply",
        )
        .await?;
        Ok(())
    }

    /// Best-effort in-turn stop. Cancel still terminates the owned server.
    pub async fn interrupt(&self, session: &str) -> Result<(), ClientError> {
        Self::data(
            self.post(&format!("/api/session/{session}/interrupt"))
                .send()
                .await,
            "session interrupt",
        )
        .await?;
        Ok(())
    }
}
