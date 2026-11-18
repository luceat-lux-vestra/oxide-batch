//! Component contracts plus the shared adapter-certification source.

// Only the service test binaries run every certification case; other binaries
// reuse the module for repository or component contracts alone.
#[allow(dead_code)]
#[path = "../../../oxide-batch-test/src/certification/mod.rs"]
mod certification;
mod components;

#[allow(unused_imports)]
pub use certification::{
    CheckpointContextBackend, ContractClock, ServiceBackend,
    run_checkpoint_context_restart_contract, run_core_repository_contract, run_explorer_contract,
    run_full_repository_contract as run_repository_contract,
    run_full_service_contract as run_service_contract, run_runnable_core_repository_contract,
};
pub use components::run_component_contract;
