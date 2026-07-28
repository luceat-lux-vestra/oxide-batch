//! The public facade for `OxideBatch`.
//!
//! `OxideBatch` is currently in its foundation phase. Runtime APIs will be
//! introduced only after their execution semantics and compatibility contracts
//! are documented.

#![forbid(unsafe_code)]

/// The version of the `OxideBatch` facade crate.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
