mod client;
mod commands;

use std::path::PathBuf;
use std::process::ExitCode;

use central_domain::{AttemptId, PlanHash, RunId};
use clap::{Parser, Subcommand};

/// Central local runtime CLI. Every run operation talks to the single
/// owning daemon over its loopback API; the CLI never opens the
/// database itself and never writes queue files directly.
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
    /// Show daemon/API discovery info and check the contract version.
    Meta,
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
    /// Worker-result operations: validate locally, then import through
    /// the daemon so the candidate is sealed and parked for
    /// verification. Importing never marks a task passed.
    Result {
        #[command(subcommand)]
        command: ResultCommands,
    },
    /// Handover operations: validate locally, then import through the
    /// daemon with evidence inspection.
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
    /// (plain) or --secret-stdin (read from stdin, never argv/history)
    /// is required. Secrets are memory-only and never persisted.
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
        secret_stdin: bool,
    },
    /// Event operations: ordered replay with durable cursors.
    Events {
        #[arg(long)]
        run: RunId,
        #[arg(long)]
        after: Option<String>,
        #[arg(long)]
        limit: Option<u32>,
        /// Keep polling from the cursor (1 s interval) until interrupted.
        #[arg(long)]
        follow: bool,
    },
    /// Bounded evidence reads for a run, optionally one attempt.
    Logs {
        #[arg(long)]
        run: RunId,
        #[arg(long)]
        attempt: Option<AttemptId>,
        #[arg(long)]
        limit: Option<u32>,
        /// Keep polling until interrupted.
        #[arg(long)]
        follow: bool,
    },
    /// Artifact metadata and hash-verified content windows.
    Artifacts {
        #[arg(long)]
        run: RunId,
        #[arg(long)]
        id: Option<String>,
        /// Show content (bounded window; --offset/--limit apply).
        #[arg(long)]
        content: bool,
        #[arg(long)]
        offset: Option<usize>,
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Leader conversation: post a message or read history. Posting is
    /// discussion only and never approves, answers or dispatches.
    Chat {
        #[arg(long)]
        run: RunId,
        #[arg(long)]
        message: Option<String>,
        #[arg(long)]
        after: Option<String>,
        #[arg(long)]
        limit: Option<u32>,
    },
    /// Agent profiles: user-supplied configuration with capability
    /// assessment. Attempts keep their frozen copy.
    Profiles {
        #[command(subcommand)]
        command: ProfileCommands,
    },
    /// Client credentials: issue, list and revoke scoped tokens.
    /// Owner only. Issuance prints the secret once; it is never stored.
    Clients {
        #[command(subcommand)]
        command: ClientCommands,
    },
    /// Show the shared budget ledger of a run (limits, totals, unknown).
    Budget {
        #[arg(long)]
        run: RunId,
    },
    /// Resolve one missing-usage gap by explicit audited decision.
    /// Releases the hold without fabricating observed usage.
    UsageResolve {
        #[arg(long)]
        run: RunId,
        #[arg(long)]
        missing_key: String,
        #[arg(long)]
        note: String,
    },
    /// Recovery report: passive findings plus required user actions.
    /// Never dispatches, answers or resumes.
    Recovery {
        #[arg(long)]
        run: Option<RunId>,
    },
    /// Queue the user's commit through the daemon without touching the
    /// user worktree: the commit is verified read-only daemon-side.
    /// Never checks out, stashes or pulls.
    Sync {
        #[arg(long)]
        run: RunId,
        #[arg(long)]
        commit: String,
        #[arg(long)]
        repository: Option<PathBuf>,
    },
    /// Set the run's shared count/token/time limits (plus the optional
    /// user-configured monetary estimate cap). User only.
    SetLimits {
        #[arg(long)]
        run: RunId,
        /// Maximum total tokens before new attempts hold.
        #[arg(long)]
        max_tokens: Option<u64>,
        /// Maximum total cost in minor units before new attempts hold.
        #[arg(long)]
        max_cost_minor: Option<u64>,
        /// Maximum live (non-terminal) attempts before new attempts hold.
        #[arg(long)]
        max_attempts: Option<u64>,
        /// Maximum window duration in ms before new attempts hold.
        #[arg(long)]
        max_duration_ms: Option<i64>,
    },
    /// Passively reconcile one attempt: store the harness evidence
    /// durably without starting inference. An unknown outcome stays
    /// unknown until a new attempt continues with a handover.
    Reconcile {
        #[arg(long)]
        run: RunId,
        #[arg(long)]
        attempt: AttemptId,
    },
    /// Owner-only maintenance: redacted state report (never a backup,
    /// never credentials) or explicit dry-run garbage collection.
    Maintenance {
        #[command(subcommand)]
        command: MaintenanceCommands,
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
    /// Show one exact plan revision (or the pending/active pointer).
    Show {
        #[arg(long)]
        run: RunId,
        #[arg(long)]
        hash: Option<String>,
    },
    /// Diff two exact plan revisions.
    Diff {
        #[arg(long)]
        run: RunId,
        #[arg(long)]
        from: String,
        #[arg(long)]
        to: String,
    },
}

#[derive(Subcommand)]
pub enum ResultCommands {
    /// Import a worker-result JSON file through the daemon: validated,
    /// sealed and parked for verification. Never marks a task passed.
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
    /// Import a handover JSON file through the daemon with evidence
    /// inspection. Unbacked claims stay unverified.
    Import {
        #[arg(long)]
        run: RunId,
        #[arg(long)]
        file: PathBuf,
    },
}

#[derive(Subcommand)]
pub enum ProfileCommands {
    /// List stored profiles with capability assessment.
    List,
    /// Show one profile.
    Get {
        #[arg(long)]
        id: String,
    },
    /// Create or replace a profile from flags (config JSON optional).
    Create {
        #[arg(long)]
        id: Option<String>,
        #[arg(long)]
        name: String,
        #[arg(long)]
        provider: String,
        #[arg(long)]
        model: String,
        #[arg(long)]
        effort: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum ClientCommands {
    /// List clients (metadata only; owner only).
    List,
    /// Issue a scoped credential (owner only). Prints the secret once.
    Create {
        #[arg(long)]
        name: String,
        #[arg(long)]
        scope: String,
        /// Bridge role (bridge scope only).
        #[arg(long)]
        role: Option<String>,
        /// Bound attempt (bridge scope only).
        #[arg(long)]
        attempt: Option<AttemptId>,
    },
    /// Revoke a credential (owner only). Open streams close.
    Revoke {
        #[arg(long)]
        id: String,
    },
}

#[derive(Subcommand)]
pub enum MaintenanceCommands {
    /// Redacted state report (never credentials).
    Export,
    /// Explicit dry-run garbage collection. Active, unknown or
    /// referenced work is never a candidate.
    Gc {
        /// Required: deletion never runs without an explicit dry run.
        #[arg(long)]
        dry_run: bool,
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
