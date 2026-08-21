//! Application-facing test kit for `OxideBatch`.
//!
//! This crate is the [Gate G](https://github.com/luceat-lux-vestra/oxide-batch/blob/main/docs/project/m6-design-gate-evidence.md#gate-g--oxide-batch-test-boundary)
//! public test-kit boundary: a dedicated package consumed by application test
//! code, not a module re-exported from the `oxide-batch` facade. It has its
//! own dependency/resource boundary independent of the production runtime,
//! and it consumes only `oxide-batch`'s public contracts.
//!
//! `oxide-batch` never depends on this crate, and its facade never
//! re-exports it.
//!
//! ## What this crate provides
//!
//! - [`ManualClock`] and [`DeterministicIds`]: deterministic implementations
//!   of the framework's own [`Clock`](oxide_batch::Clock) and
//!   [`IdGenerator`](oxide_batch::IdGenerator) ports.
//! - [`ComponentFixture`]: typed call-scope factories for exercising a bare
//!   [`ItemReader`](oxide_batch::ItemReader),
//!   [`ItemProcessor`](oxide_batch::ItemProcessor),
//!   [`ItemWriter`](oxide_batch::ItemWriter), or
//!   [`ItemStream`](oxide_batch::ItemStream) implementation without
//!   constructing a full job.
//! - [`TestStep`]: a single-step harness that drives a
//!   [`ChunkStep`](oxide_batch::ChunkStep) through its real, production
//!   `execute` path.
//! - [`EmbeddedRepository`] and [`TestJob`]: a full-job harness that launches
//!   a [`ChunkJob`](oxide_batch::ChunkJob) through the real
//!   [`JobLauncher`](oxide_batch::JobLauncher), backed by an isolated
//!   in-process repository.
//! - [`inject`]: reusable failure, panic, and cooperative-stop injection at
//!   named lifecycle points, distinguishable from a genuine framework defect
//!   by a bounded, test-owned [`inject::InjectionId`].
//! - [`restart`]: a restart harness that runs a second execution attempt
//!   against the same job instance and proves it resumes from only the last
//!   *committed* checkpoint and component state.
//! - `postgres` feature: [`postgres::PostgresFixture`], an isolated,
//!   self-cleaning `PostgreSQL` repository fixture, required for the durable
//!   restart harness because only a durable
//!   [`ChunkTransactionManager`](oxide_batch::ChunkTransactionManager) proves
//!   real inherited progress.
//!
//! None of the above leaks a `SQLx`, Tokio runtime-handle, or other
//! database-driver concrete type in its public API.

mod clock;
mod ids;
mod scope;
mod step;
mod transactions;

pub use clock::{ManualClock, ManualClockError};
pub use ids::{DeterministicIds, IdSequenceError};
pub use scope::ComponentFixture;
pub use step::TestStep;
pub use transactions::{NoCompletion, StandaloneTransaction, StandaloneTransactions};
