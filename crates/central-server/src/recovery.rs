//! Startup recovery wiring. Runs before the HTTP listener opens and
//! performs zero external sends: parked runs stay parked until an
//! explicit `Resume` command arrives through the API.

use std::sync::Arc;

use central_application::contracts::{AppError, RecoverySummary};
use central_application::ports::{AgentGateway, Clock, IdSource, Store};
use central_application::Runtime;
use central_domain::RunId;

pub async fn recover_all(
    store: Arc<dyn Store>,
    clock: Arc<dyn Clock>,
    ids: Arc<dyn IdSource>,
    agent: Arc<dyn AgentGateway>,
) -> Result<RecoverySummary, AppError> {
    // The probe runtime's own run id is irrelevant: `recover` scans
    // every run in the store.
    let probe = Runtime::new(store, clock, ids, agent, RunId::new());
    probe.recover().await
}
