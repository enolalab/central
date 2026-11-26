//! Owner-only browser settings for the three supported harnesses.
//! The same files remain the source of truth at boot; running sessions
//! keep their gateway and are never replaced by editing these settings.

use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::sync::Mutex;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use central_infrastructure::{codex, muse, opencode};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::{auth::authenticate, AppState};
use crate::api::errors::{error_response, ApiError};
use crate::bootstrap::{
    enables_catalog_models, find_on_path, read_model_catalog, restrict_to_owner,
};

static CONFIG_WRITE: Mutex<()> = Mutex::new(());

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum Provider {
    Codex,
    Muse,
    Opencode,
}

impl Provider {
    const ALL: [Self; 3] = [Self::Codex, Self::Muse, Self::Opencode];

    fn name(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Muse => "muse",
            Self::Opencode => "opencode",
        }
    }

    fn versions(self) -> &'static [&'static str] {
        match self {
            Self::Codex => codex::version::CLI_VERSION_ALLOWLIST,
            Self::Muse => muse::version::CLI_VERSION_ALLOWLIST,
            Self::Opencode => opencode::version::CLI_VERSION_ALLOWLIST,
        }
    }

    fn default_version(self) -> &'static str {
        match self {
            Self::Codex => codex::SUPPORTED_CLI_VERSION,
            Self::Muse => muse::SUPPORTED_CLI_VERSION,
            Self::Opencode => opencode::SUPPORTED_CLI_VERSION,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SaveProvider {
    provider: Provider,
    cli_version: String,
    models: Vec<String>,
    enable_catalog_models: Option<bool>,
    revision: Option<String>,
}

fn validation(message: impl Into<String>) -> ApiError {
    error_response(StatusCode::BAD_REQUEST, "validation_failed", message)
}

fn storage_error() -> ApiError {
    error_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal_error",
        "could not read or write provider configuration",
    )
}

fn read_config(path: &Path) -> Result<Option<String>, ApiError> {
    match fs::read_to_string(path) {
        Ok(text) => Ok(Some(text)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(storage_error()),
    }
}

fn revision(text: &Option<String>) -> Option<String> {
    text.as_ref()
        .map(|text| central_domain::preview_hash_for(text).to_string())
}

fn binary_name(provider: Provider, config: &Value) -> &str {
    config["binary"].as_str().unwrap_or(provider.name())
}

pub async fn models(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<([(&'static str, &'static str); 1], Json<Value>), ApiError> {
    authenticate(&state, &headers).await?;
    // The current snapshot can include models added after this boot.
    // Only live gateway capabilities tell the picker which ones can run.
    let mut catalogs = read_model_catalog(&state.state_dir);
    let codex_efforts = fs::read_to_string(state.state_dir.join("codex-model-efforts.json"))
        .ok()
        .and_then(|text| serde_json::from_str::<Map<String, Value>>(&text).ok())
        .unwrap_or_default();
    let mut items = Vec::new();
    for provider in &state.providers {
        // Capabilities is a passive gateway read keyed by provider. It
        // reads the booted gateway, not staged settings on disk.
        let probe = central_domain::ResolvedProfile {
            id: central_domain::ProfileId::new(),
            provider: provider.clone(),
            model: String::new(),
            effort: "low".into(),
            skills: Vec::new(),
            subagents: Vec::new(),
            permissions: Vec::new(),
            fallbacks: Vec::new(),
            hash: central_domain::preview_hash_for(provider),
        };
        let mut models = BTreeSet::new();
        if let Ok(caps) = state.agent.capabilities(&probe).await {
            models.extend(
                caps.features
                    .iter()
                    .filter_map(|feature| feature.strip_prefix("model:").map(str::to_owned)),
            );
        }
        let discovered = catalogs.remove(provider).unwrap_or_default();
        let mut item = serde_json::json!({ "provider": provider, "models": models });
        if !discovered.is_empty() {
            item["discovered"] = serde_json::json!(discovered);
        }
        if provider == "codex" && !codex_efforts.is_empty() {
            item["reasoning_efforts"] = serde_json::json!(codex_efforts);
        }
        items.push(item);
    }
    for provider in Provider::ALL {
        if let Some(discovered) = catalogs.remove(provider.name()) {
            items.push(serde_json::json!({
                "provider": provider, "models": [], "discovered": discovered,
            }));
        }
    }
    Ok((
        [("cache-control", "no-store")],
        Json(serde_json::json!({ "items": items })),
    ))
}

pub async fn list(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<([(&'static str, &'static str); 1], Json<Value>), ApiError> {
    authenticate(&state, &headers).await?.require_owner()?;
    let mut items = Vec::new();
    for provider in Provider::ALL {
        let path = state.state_dir.join(format!("{}.json", provider.name()));
        let text = read_config(&path)?;
        let config = match &text {
            Some(text) => serde_json::from_str::<Value>(text).ok(),
            None => Some(Value::Null),
        };
        let valid = config
            .as_ref()
            .is_some_and(|value| text.is_none() || value.is_object());
        let config = config.unwrap_or(Value::Null);
        items.push(serde_json::json!({
            "provider": provider,
            "active": state.providers.iter().any(|name| name == provider.name()),
            "configured": text.is_some(),
            "valid_json": valid,
            "installed": find_on_path(binary_name(provider, &config)).is_some(),
            "cli_version": config["cli_version"].as_str().unwrap_or(provider.default_version()),
            "supported_versions": provider.versions(),
            "enable_catalog_models": enables_catalog_models(&config),
            "models": config["surveyed_models"].as_array().map(|models| {
                models.iter().filter_map(Value::as_str).collect::<Vec<_>>()
            }).unwrap_or_default(),
            "revision": revision(&text),
        }));
    }
    Ok((
        [("cache-control", "no-store")],
        Json(serde_json::json!({ "items": items })),
    ))
}

pub async fn save(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<SaveProvider>,
) -> Result<Json<Value>, ApiError> {
    authenticate(&state, &headers).await?.require_owner()?;
    if state.fake_agent_enabled {
        return Err(validation(
            "provider settings are unavailable in fake-agent mode",
        ));
    }
    if !body
        .provider
        .versions()
        .contains(&body.cli_version.as_str())
    {
        return Err(validation("CLI version is outside Central's allowlist"));
    }
    if body.models.len() > 100 {
        return Err(validation("at most 100 models can be configured"));
    }
    let mut models = Vec::new();
    for model in &body.models {
        let model = model.trim();
        if model.is_empty() || model.len() > 200 || model.chars().any(char::is_control) {
            return Err(validation("invalid model identifier"));
        }
        if !models.contains(&model) {
            models.push(model);
        }
    }
    let _guard = CONFIG_WRITE.lock().map_err(|_| storage_error())?;
    let path = state
        .state_dir
        .join(format!("{}.json", body.provider.name()));
    let text = read_config(&path)?;
    if revision(&text) != body.revision {
        return Err(error_response(
            StatusCode::CONFLICT,
            "stale_target",
            "provider settings changed; reopen the settings and try again",
        ));
    }
    let mut config = match text {
        Some(text) => serde_json::from_str::<Map<String, Value>>(&text).map_err(|_| {
            validation("the existing provider file is not a JSON object; repair it first")
        })?,
        None => Map::new(),
    };
    let binary = config
        .get("binary")
        .and_then(Value::as_str)
        .unwrap_or(body.provider.name());
    if find_on_path(binary).is_none() {
        return Err(validation(
            "the provider CLI was not found on the daemon's PATH",
        ));
    }
    config.insert("cli_version".into(), Value::String(body.cli_version));
    config.insert("surveyed_models".into(), serde_json::json!(models));
    if let Some(enabled) = body.enable_catalog_models {
        config.insert("enable_catalog_models".into(), Value::Bool(enabled));
    }
    let data = serde_json::to_vec_pretty(&config).map_err(|_| storage_error())?;
    write_config(&path, &data)?;
    Ok(Json(
        serde_json::json!({ "status": "saved", "restart_required": true }),
    ))
}

fn write_config(path: &Path, data: &[u8]) -> Result<(), ApiError> {
    let temporary = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|_| storage_error())?;
        restrict_to_owner(&temporary, false).map_err(|_| storage_error())?;
        file.write_all(data).map_err(|_| storage_error())?;
        file.sync_all().map_err(|_| storage_error())?;
        drop(file);
        fs::rename(&temporary, path).map_err(|_| storage_error())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}
