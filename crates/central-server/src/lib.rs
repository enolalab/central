pub mod bootstrap;
pub mod http;
pub mod recovery;

#[cfg(feature = "test-support")]
pub mod fake_agent {
    include!("../../../tests/fixtures/fake_agent.rs");
}

pub use bootstrap::{run, BootstrapError, DaemonConfig};
