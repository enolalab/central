//! Minimal loopback HTTP client for the daemon API. The CLI never
//! opens the database: it discovers the daemon through `server.json`
//! and authenticates with the owner token in `daemon.token`, both in
//! the state directory.

use std::path::{Path, PathBuf};

pub struct DaemonClient {
    addr: String,
    token: String,
    http: reqwest::blocking::Client,
}

pub struct ServerInfo {
    pub addr: String,
    pub pid: u32,
    pub fake_agent: bool,
}

pub fn read_server_info(state_dir: &Path) -> Option<ServerInfo> {
    let text = std::fs::read_to_string(state_dir.join("server.json")).ok()?;
    let value: serde_json::Value = serde_json::from_str(&text).ok()?;
    Some(ServerInfo {
        addr: value["addr"].as_str()?.to_string(),
        pid: value["pid"].as_u64()? as u32,
        fake_agent: value["fake_agent"].as_bool().unwrap_or(false),
    })
}

pub fn health_ok(addr: &str) -> bool {
    let Ok(http) = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
    else {
        return false;
    };
    http.get(format!("http://{addr}/healthz"))
        .send()
        .map(|response| response.status().is_success())
        .unwrap_or(false)
}

impl DaemonClient {
    pub fn discover(state_dir: &Path) -> Result<Self, String> {
        let info = read_server_info(state_dir).ok_or_else(|| {
            format!(
                "no daemon server.json in {}; run `central daemon start` first",
                state_dir.display()
            )
        })?;
        let token = std::fs::read_to_string(state_dir.join("daemon.token"))
            .map_err(|error| format!("cannot read daemon token: {error}"))?
            .trim()
            .to_string();
        let http = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .build()
            .map_err(|error| format!("cannot build http client: {error}"))?;
        Ok(Self {
            addr: info.addr,
            token,
            http,
        })
    }

    pub fn get(&self, path: &str) -> Result<serde_json::Value, String> {
        let response = self
            .http
            .get(format!("http://{}{path}", self.addr))
            .bearer_auth(&self.token)
            .send()
            .map_err(|error| format!("request failed: {error}"))?;
        Self::decode(response)
    }

    pub fn post_json(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let response = self
            .http
            .post(format!("http://{}{path}", self.addr))
            .bearer_auth(&self.token)
            .json(body)
            .send()
            .map_err(|error| format!("request failed: {error}"))?;
        Self::decode(response)
    }

    fn decode(response: reqwest::blocking::Response) -> Result<serde_json::Value, String> {
        let status = response.status();
        let text = response
            .text()
            .map_err(|error| format!("cannot read response: {error}"))?;
        if !status.is_success() {
            return Err(format!("daemon returned {status}: {text}"));
        }
        serde_json::from_str(&text).map_err(|error| format!("invalid daemon JSON: {error}"))
    }
}

pub fn resolve_state_dir(explicit: Option<PathBuf>) -> PathBuf {
    explicit
        .or_else(|| std::env::var_os("CENTRAL_STATE_DIR").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from(".central"))
}
