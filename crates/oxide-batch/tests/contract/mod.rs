//! Component contracts plus the shared adapter-certification source.

// Only the service test binaries run every certification case; other binaries
// reuse the module for repository or component contracts alone.
#[allow(dead_code)]
#[path = "../../../oxide-batch-test/src/certification/mod.rs"]
mod certification;
mod components;

#[allow(unused_imports)]
pub use certification::{
    ContractClock, ServiceBackend, run_repository_contract, run_service_contract,
};
pub use components::run_component_contract;
