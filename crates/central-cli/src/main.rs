mod client;
mod commands;

use std::path::PathBuf;
use std::process::ExitCode;

use central_domain::{AttemptId, PlanHash, RunId};
use clap::{Parser, Subcommand};

/// Central local runtime CLI. Every run operation talks to the single
/// owning daemon over its loopback API; the CLI never opens the
/// database itself.
#[derive(Parser)]
#[command(name = "central", version, about)]
pub struct Cli {
    /// Daemon state directory (default: $CENTRAL_STATE_DIR or .central).
    #[arg(long, global = true)]
    pub state_dir: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Manage the owning daemon process.
    Daemon {
        #[command(subcommand)]
        command: DaemonCommands,
    },
    /// Create a run (the daemon allocates its id).
    Init {
        #[arg(long)]
        goal: String,
        #[arg(long)]
        repository: String,
    },
    /// Dispatch pending outbox intents for a run once.
    Start {
        #[arg(long)]
        run: RunId,
    },
    /// Show one run (or list all runs without --run).
    Status {
        #[arg(long)]
        run: Option<RunId>,
    },
    /// Plan operations.
    Plan {
        #[command(subcommand)]
        command: PlanCommands,
    },
    /// Worker-result operations (validated import route when the live
    /// structured channel is unavailable; committing lands in P2/T5).
    Result {
        #[command(subcommand)]
        command: ResultCommands,
    },
    /// Handover operations (validated import route for new sessions).
    Handover {
        #[command(subcommand)]
        command: HandoverCommands,
    },
    /// Show pending approval/question entries for a run.
    Inbox {
        #[arg(long)]
        run: RunId,
    },
    /// Approve a pending action approval by inbox key. A plan hash is not
    /// a key: plans approve through `central plan approve` only.
    Approve {
        #[arg(long)]
        run: RunId,
        #[arg(long)]
        key: String,
        #[arg(long)]
        preview_hash: String,
    },
    /// Decline a pending action approval by inbox key.
    Decline {
        #[arg(long)]
        run: RunId,
        #[arg(long)]
        key: String,
        #[arg(long)]
        preview_hash: String,
        #[arg(long)]
        reason: Option<String>,
    },
    /// Answer a pending question by inbox key. Exactly one of --message
    /// (plain) or --secret (memory-only; never persisted) is required.
    Answer {
        #[arg(long)]
        run: RunId,
        #[arg(long)]
        key: String,
        #[arg(long)]
        preview_hash: String,
        #[arg(long)]
        message: Option<String>,
        #[arg(long)]
        secret: Option<String>,
    },
    /// Pause a running run (in-flight attempts finish).
    Pause {
        #[arg(long)]
        run: RunId,
    },
    /// Stop a run and request cancellation of active attempts.
    Stop {
        #[arg(long)]
        run: RunId,
    },
    /// Resume a paused or restart-parked run. This is the only way
    /// dispatch reopens after a daemon restart.
    Resume {
        #[arg(long)]
        run: RunId,
    },
}

#[derive(Subcommand)]
pub enum DaemonCommands {
    /// Start the daemon detached/hidden if it is not already running.
    Start {
        #[arg(long, default_value = "127.0.0.1:0")]
        listen: String,
        /// Test-support builds only: serve the fake agent.
        #[arg(long)]
        fake_agent: bool,
    },
    /// Report whether the daemon is running.
    Status,
    /// Ask the daemon to shut down gracefully.
    Shutdown,
}

#[derive(Subcommand)]
pub enum PlanCommands {
    /// Propose a plan document from a JSON file.
    Import {
        #[arg(long)]
        run: RunId,
        #[arg(long)]
        file: PathBuf,
    },
    /// Approve the pending plan (or the given hash).
    Approve {
        #[arg(long)]
        run: RunId,
        #[arg(long)]
        hash: Option<PlanHashArg>,
    },
}

#[derive(Subcommand)]
pub enum ResultCommands {
    /// Validate a worker-result JSON file against its schema: version,
    /// attempt scope (flag and run state), contained paths, content hash.
    /// Prints the hash; committing the candidate lands in P2/T5 verification.
    Import {
        #[arg(long)]
        run: RunId,
        #[arg(long)]
        attempt: AttemptId,
        #[arg(long)]
        file: PathBuf,
    },
}

#[derive(Subcommand)]
pub enum HandoverCommands {
    /// Validate a handover JSON file against its schema: version, contained
    /// artifact paths, verified/unverified separation, content hash.
    Import {
        #[arg(long)]
        run: RunId,
        #[arg(long)]
        file: PathBuf,
    },
}

/// Wrapper so clap can parse a plan hash from its hex string.
#[derive(Clone, Debug)]
pub struct PlanHashArg(pub PlanHash);

impl std::str::FromStr for PlanHashArg {
    type Err = std::convert::Infallible;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(Self(PlanHash::from_hex(value)))
    }
}

impl std::ops::Deref for PlanHashArg {
    type Target = PlanHash;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    ExitCode::from(commands::execute(cli) as u8)
}
