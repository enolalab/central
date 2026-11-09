//! `central-server --state-dir PATH --listen 127.0.0.1:0`
//!
//! Exit codes: 0 clean shutdown, 3 another daemon already owns the
//! state directory, 2 any other failure (storage failures included —
//! a daemon that cannot open its database never reports success).

use std::path::PathBuf;
use std::process::ExitCode;

use central_infrastructure::process::ALREADY_RUNNING_EXIT_CODE;
use central_server::{parse_listen_addr, run, BootstrapError, DaemonConfig};

fn parse_args() -> Result<DaemonConfig, String> {
    let mut state_dir: Option<PathBuf> = None;
    let mut listen = "127.0.0.1:7878".to_string();
    let mut fake_agent = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--state-dir" => {
                let value = args.next().ok_or("--state-dir requires a path")?;
                state_dir = Some(PathBuf::from(value));
            }
            "--listen" => {
                listen = args.next().ok_or("--listen requires an address")?;
            }
            "--fake-agent" => fake_agent = true,
            "--help" | "-h" => {
                println!("usage: central-server --state-dir PATH [--listen ADDR] [--fake-agent]");
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    // Reject a non-loopback bind here as well as in bootstrap, so a
    // bad address fails before any state directory is touched.
    parse_listen_addr(&listen).map_err(|error| error.to_string())?;
    Ok(DaemonConfig {
        state_dir: state_dir.ok_or("--state-dir is required")?,
        listen,
        fake_agent,
    })
}

fn main() -> ExitCode {
    let config = match parse_args() {
        Ok(config) => config,
        Err(message) => {
            eprintln!("central-server: {message}");
            return ExitCode::from(2);
        }
    };
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("central-server: cannot build runtime: {error}");
            return ExitCode::from(2);
        }
    };
    match runtime.block_on(run(config)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(BootstrapError::AlreadyRunning) => {
            eprintln!("central-server: another daemon already owns this state directory");
            ExitCode::from(ALREADY_RUNNING_EXIT_CODE as u8)
        }
        Err(error) => {
            eprintln!("central-server: {error}");
            ExitCode::from(2)
        }
    }
}
