//! Executable evidence for the M0 architecture decisions.
//!
//! This crate is intentionally private and disposable. The tests and reports
//! under `docs/architecture/spikes` are the retained decision evidence.

#![forbid(unsafe_code)]

pub mod context;
pub mod execution;
pub mod postgres;
