mod client;
mod commands;

use std::path::PathBuf;
use std::process::ExitCode;

use central_domain::{PlanHash, RunId};
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
