pub mod daemon;

pub use daemon::{spawn_detached, DaemonError, OwnerLock, ALREADY_RUNNING_EXIT_CODE};
