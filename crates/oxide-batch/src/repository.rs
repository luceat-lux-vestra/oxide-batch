//! Metadata adapters for the extracted repository ports.
//!
//! The ports themselves live in `oxide-batch-repository`. This module holds the
//! adapters that implement them: the reference in-memory implementation and the
//! `PostgreSQL` implementation behind the `postgres` feature.

mod memory;
#[cfg(feature = "postgres")]
pub(crate) mod postgres;

pub use memory::{InMemoryExplorer, InMemoryJobRepository};
#[cfg(feature = "postgres")]
pub use postgres::{
    CaCertificate, PostgresChunkStateError, PostgresChunkStateProvider,
    PostgresChunkTransactionManager, PostgresConfig, PostgresConfigError, PostgresDurableStepState,
    PostgresExplorer, PostgresFaultState, PostgresJobRepository, PostgresMigrator, TlsMode,
};
