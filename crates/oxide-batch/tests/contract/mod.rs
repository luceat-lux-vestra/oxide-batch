//! Component contracts plus the shared adapter-certification source.

#[path = "../../../oxide-batch-test/src/certification/mod.rs"]
mod certification;
mod components;

pub use certification::{
    ContractClock, ServiceBackend, run_repository_contract, run_service_contract,
};
pub use components::run_component_contract;
