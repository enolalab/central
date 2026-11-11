//! Recovery snapshot: `GET /v1/runs/{id}/recovery` lives in
//! [`super::usage`]; this module serves the daemon-level recovery
//! summary shape used by the CLI (`central recovery`): every known
//! run's control, holds and required actions, plus this boot's
//! instance epoch so clients can detect the restart boundary.

use axum::extract::State;
use axum::http::HeaderMap;
use axum::Json;

use super::auth::{app_error_response, authenticate};
use super::AppState;
use crate::api::errors::ApiError;

pub async fn get_recovery_overview(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    authenticate(&state, &headers).await?;
    let mut runs = Vec::new();
    for run_id in state
        .store
        .list_run_ids()
        .await
        .map_err(app_error_response)?
    {
        let Some(run_state) = state.store.load(run_id).await.map_err(app_error_response)? else {
            continue;
        };
        let report = central_application::recovery::inspect_run(&run_state);
        runs.push(serde_json::json!({
            "run_id": run_id.to_string(),
            "control": format!("{:?}", run_state.control),
            "holds": run_state.holds.iter().map(|hold| format!("{hold:?}")).collect::<Vec<_>>(),
            "required_actions": report.required_actions,
        }));
    }
    Ok(Json(serde_json::json!({
        "instance_epoch": state.instance_epoch,
        "runs": runs,
    })))
}
