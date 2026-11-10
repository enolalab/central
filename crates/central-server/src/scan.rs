//! Daemon scan loop: periodic + on-event scheduler ticks (P3 Task 6,
//! ledger carry-over: T1 delivered pure `should_tick`/`tick`, this wires it
//! into the daemon so the scheduler actually runs in production).
//!
//! The loop rescans every `Running` run on a cadence ([`ScanConfig`] from
//! `CENTRAL_SCAN_MS`, default 5s) and wakes early when an API write lands
//! (via [`scan_notify`]). Each tick is [`Scheduler::tick`]: queue ready
//! work, then dispatch. Parked runs (`AwaitingResume`/`Paused`/stopped)
//! are skipped here — they resume only through an explicit `Resume`
//! command — and dispatch itself rechecks every gate, so a tick can never
//! send for a parked run even if the skip missed.
//!
//! [`scan_notify`]: crate::http::AppState::scan_notify

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use central_application::ports::{AgentGateway, Clock, IdSource, Store};
use central_application::scheduler::{Scheduler, SchedulerConfig};
use central_domain::{Actor, ContentHash, ResolvedProfile, RunControl, RunId, Task, Timestamp};
use tokio::sync::Notify;

/// Poll cadence for the daemon scan loop, in milliseconds.
pub const DEFAULT_SCAN_MS: u64 = 5_000;

/// Profile the scan loop dispatches with under `--fake-agent` only: the
/// deterministic low model the fake harness serves. Production has no
/// profile registry yet (P2/T2 layers wire into the resolver below), so
/// without the fake agent the resolver yields `None` and dispatch defers
/// while queueing still proceeds — never an invented servable profile.
pub fn dev_profile() -> ResolvedProfile {
    ResolvedProfile {
        id: central_domain::ProfileId::from_u128(1),
        provider: "fake".to_string(),
        model: "fake-model".to_string(),
        effort: "low".to_string(),
        skills: Vec::new(),
        subagents: Vec::new(),
        permissions: Vec::new(),
        fallbacks: Vec::new(),
        hash: ContentHash::from_hex("dev-profile"),
    }
}

#[derive(Clone, Debug)]
pub struct ScanConfig {
    /// Rescan interval. Zero disables the loop (deterministic tests).
    pub interval_ms: u64,
    pub scheduler: SchedulerConfig,
    pub fake_agent: bool,
}

impl ScanConfig {
    /// From the environment: `CENTRAL_SCAN_MS` (default 5000, `0` disables).
    /// A malformed value fails closed to the default, never to spinning.
    pub fn from_env(fake_agent: bool) -> Self {
        let interval_ms = std::env::var("CENTRAL_SCAN_MS")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(DEFAULT_SCAN_MS);
        Self {
            interval_ms,
            scheduler: SchedulerConfig::default(),
            fake_agent,
        }
    }

    pub fn disabled(fake_agent: bool) -> Self {
        Self {
            interval_ms: 0,
            scheduler: SchedulerConfig::default(),
            fake_agent,
        }
    }

    pub fn enabled(&self) -> bool {
        self.interval_ms > 0
    }
}

fn resolver(fake_agent: bool) -> Arc<central_application::scheduler::ProfileResolver> {
    Arc::new(move |task: &Task| {
        if task.profile_id.is_some() {
            // No profile registry is wired yet: a pinned task waits for
            // one rather than dispatching on a substituted profile.
            return None;
        }
        if fake_agent {
            Some(dev_profile())
        } else {
            None
        }
    })
}

fn scheduler_for(
    store: &Arc<dyn Store>,
    clock: &Arc<dyn Clock>,
    ids: &Arc<dyn IdSource>,
    agent: &Arc<dyn AgentGateway>,
    actor: &Actor,
    config: &ScanConfig,
) -> Result<Scheduler, central_application::contracts::AppError> {
    Scheduler::new(
        Arc::clone(store),
        Arc::clone(clock),
        Arc::clone(ids),
        Arc::clone(agent),
        config.scheduler.clone(),
        resolver(config.fake_agent),
        actor.clone(),
    )
}

/// One scan pass over every known run. Returns `(ticked, sent, unknown)`.
/// A run that is not `Running` is skipped (it resumes only explicitly);
/// a stale cadence entry is skipped via `should_tick`. Failures are
/// per-run: one bad run never stops the scan of the rest.
pub async fn tick_all(
    store: &Arc<dyn Store>,
    clock: &Arc<dyn Clock>,
    ids: &Arc<dyn IdSource>,
    agent: &Arc<dyn AgentGateway>,
    actor: &Actor,
    config: &ScanConfig,
    last_tick: &mut HashMap<RunId, Timestamp>,
) -> (usize, usize, usize) {
    let scheduler = match scheduler_for(store, clock, ids, agent, actor, config) {
        Ok(scheduler) => scheduler,
        Err(error) => {
            eprintln!("central-server: scan scheduler unavailable: {error}");
            return (0, 0, 0);
        }
    };
    let now = clock.now();
    let run_ids = match store.list_run_ids().await {
        Ok(ids) => ids,
        Err(error) => {
            eprintln!("central-server: scan cannot list runs: {error}");
            return (0, 0, 0);
        }
    };
    let mut ticked = 0;
    let mut sent = 0;
    let mut unknown = 0;
    for run_id in run_ids {
        let state = match store.load(run_id).await {
            Ok(Some(state)) => state,
            Ok(None) => continue,
            Err(error) => {
                eprintln!("central-server: scan cannot load run {run_id}: {error}");
                continue;
            }
        };
        if state.control != RunControl::Running {
            continue;
        }
        let last = last_tick.get(&run_id).copied().unwrap_or(Timestamp(0));
        if !scheduler.should_tick(last, now) {
            continue;
        }
        match scheduler.tick(run_id).await {
            Ok(report) => {
                ticked += 1;
                sent += report.sent;
                unknown += report.unknown;
                last_tick.insert(run_id, now);
            }
            Err(error) => {
                eprintln!("central-server: scan tick for run {run_id} failed: {error}");
            }
        }
    }
    (ticked, sent, unknown)
}

/// Spawn the background scan loop. The loop wakes on the interval or on
/// [`Notify`] (API writes), scans, then sleeps again. Scan errors are
/// logged to stderr; the loop never takes the daemon down.
pub fn spawn_scan_loop(
    store: Arc<dyn Store>,
    clock: Arc<dyn Clock>,
    ids: Arc<dyn IdSource>,
    agent: Arc<dyn AgentGateway>,
    actor: Actor,
    config: ScanConfig,
    notify: Arc<Notify>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut last_tick: HashMap<RunId, Timestamp> = HashMap::new();
        let interval = Duration::from_millis(config.interval_ms.max(1));
        loop {
            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                _ = notify.notified() => {}
            }
            tick_all(
                &store,
                &clock,
                &ids,
                &agent,
                &actor,
                &config,
                &mut last_tick,
            )
            .await;
        }
    })
}
