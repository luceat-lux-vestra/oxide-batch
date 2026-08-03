//! Executable evidence for the RFC-0005 static-versus-erased item hot path.
//!
//! [`contract`] holds the proposed public component contract: one trait per
//! role, implemented with plain `async fn`, plus `Boxed*` handles that are
//! themselves instances of those traits. Erasure is a type, not a second
//! trait, and the dyn-compatible machinery behind it is sealed.
//!
//! [`driver`] holds one chunk loop. The monomorphized pipeline and the
//! dynamically dispatched one are the same function with different type
//! arguments, so what the measurements compare is dispatch and nothing else.
//!
//! [`erased`] keeps adapters onto the accepted ADR-0002 boxed traits, retained
//! as migration evidence for retiring them.
//!
//! This crate is private and disposable. The retained decision evidence is the
//! report under `docs/architecture/spikes`.

pub mod allocation;
pub mod composite;
pub mod contract;
pub mod driver;
pub mod erased;
pub mod executor;
pub mod scenario;
pub mod sizes;
pub mod workload;
