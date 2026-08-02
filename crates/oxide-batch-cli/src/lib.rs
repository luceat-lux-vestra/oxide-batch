//! The minimal guarded operator command line for `OxideBatch`.
//!
//! This crate is a thin client over the portable operator, explorer, and
//! retention services. It owns no correctness rule of its own: every guard,
//! every compare-and-swap, every idempotency record, and every audit row
//! belongs to the services it calls. Removing this crate removes no correctness
//! capability.
//!
//! The crate is both a library and a binary. The shipped `oxide-batch` binary
//! serves every command that a repository alone can answer. `launch` and
//! `execution restart` additionally need the job's canonical
//! [`DefinitionIdentity`](oxide_batch::DefinitionIdentity), which only the
//! owning application can construct, so a host application embeds this crate
//! and supplies a [`DefinitionCatalog`].
//!
//! # Boundaries
//!
//! The CLI adds no hosted API, no identity system, no scheduler, and no user
//! interface. It never writes metadata directly, and repository state rather
//! than CLI output or process state is authoritative.

#![forbid(unsafe_code)]

mod args;
#[cfg(feature = "postgres")]
mod backend;
mod catalog;
mod command;
mod config;
mod exit;
mod failure;
mod host;
mod output;
mod project;
mod run;

pub use args::{ArgumentError, Arguments, DirectiveArg, OutputForm, RecordArg};
pub use catalog::{CatalogError, DefinitionCatalog};
pub use command::{ActionClass, Command};
pub use config::{
    CONFIG_VERSION, ConfigError, ConfigIssue, Configuration, EffectiveValue, Resolved, Secret,
    Source, TlsSetting, default_config_path, environment_variable, known_keys,
};
pub use exit::{ExitCategory, Outcome};
pub use host::{Host, ProcessHost};
pub use output::{
    Diagnostic, MAX_OUTPUT_BYTES, OUTPUT_SCHEMA_VERSION, OutputFailure, PageInfo, Response, Writer,
};
pub use run::{
    NoSchema, Plan, RecoveryProposalPort, SchemaReport, SchemaState, Services, dispatch, local,
    prepare,
};

#[cfg(feature = "postgres")]
pub use backend::{BackendFailure, PostgresServices, connect, connection_config};
