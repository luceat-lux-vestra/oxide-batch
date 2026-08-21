//! An isolated `PostgreSQL` repository fixture (`TEST-REPO-001`, postgres).
//!
//! Restart and transactional behavior ultimately need real durable storage
//! (only [`PostgresChunkTransactionManager`] reports genuine inherited
//! progress across attempts), so [`crate::restart`] requires this fixture.
//!
//! The public surface never exposes `SQLx`, a raw connection, or any
//! database-driver type: [`PostgresFixture`] hands out only
//! `oxide-batch`'s own [`PostgresJobRepository`] port and framework-owned
//! configuration.
//!
//! Isolation is by job name: every fixture-driven test picks its own
//! [`JobName`] and never touches another job's durable rows. Cleanup goes
//! through the real, adapter-neutral [`RetentionService`] purge path used in
//! production -- never a hand-written `DELETE` against an internal table
//! name -- which enforces a real minimum retention age
//! ([`MIN_PURGE_AGE`]) that a test cannot bypass. Because every durable
//! timestamp in this fixture's repository is stamped from its own injected
//! [`ManualClock`] rather than wall-clock time, a test satisfies that age
//! deterministically by advancing the clock, with no real wait.

use std::fmt;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::SystemTime;

use oxide_batch::{
    ActorRef, BatchStatus, Clock, JobName, MIN_PURGE_AGE, OperationId, PostgresChunkStateProvider,
    PostgresChunkTransactionManager, PostgresConfig, PostgresConfigError, PostgresJobRepository,
    PostgresMigrator, PurgeBatchBound, PurgePlanRequest, ReasonCode, RepositoryError,
    RequestFieldError, RetentionError, RetentionReport, RetentionService, TerminalStatusSet,
    TlsMode,
};

use crate::{DeterministicIds, ManualClock};

/// A failure building or operating a [`PostgresFixture`].
#[derive(Debug)]
#[non_exhaustive]
pub enum PostgresFixtureError {
    /// The connection configuration was rejected.
    Config(PostgresConfigError),
    /// The repository could not be reached, or its schema is unsupported.
    Repository(RepositoryError),
    /// The retention/purge request was rejected or failed.
    Retention(RetentionError),
    /// A bounded actor reference, reason code, or operation ID was rejected.
    Request(RequestFieldError),
}

impl fmt::Display for PostgresFixtureError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(error) => write!(formatter, "postgres fixture configuration: {error}"),
            Self::Repository(error) => write!(formatter, "postgres fixture repository: {error}"),
            Self::Retention(error) => write!(formatter, "postgres fixture retention: {error}"),
            Self::Request(error) => write!(formatter, "postgres fixture request field: {error}"),
        }
    }
}

impl std::error::Error for PostgresFixtureError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Config(error) => Some(error),
            Self::Repository(error) => Some(error),
            Self::Retention(error) => Some(error),
            Self::Request(error) => Some(error),
        }
    }
}

/// A `PostgreSQL`-backed repository fixture with its own deterministic
/// clock and ID source.
pub struct PostgresFixture {
    repository: PostgresJobRepository,
    clock: ManualClock,
    ids: DeterministicIds,
}

impl PostgresFixture {
    /// Applies the framework's metadata migrations to the database named by
    /// `connection_string`.
    ///
    /// Call this once before [`PostgresFixture::connect`] against a fresh
    /// database; it is idempotent against an already-migrated one.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresFixtureError`] when the configuration is rejected
    /// or the migration cannot complete.
    pub async fn migrate(connection_string: impl Into<String>) -> Result<(), PostgresFixtureError> {
        let config = plaintext_config(connection_string)?;
        PostgresMigrator::migrate(&config)
            .await
            .map_err(PostgresFixtureError::Repository)
    }

    /// Connects a fixture with a deterministic clock started at the Unix
    /// epoch and a deterministic ID sequence starting at `1`.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresFixtureError`] when the configuration is rejected,
    /// the database is unreachable, or its schema is unsupported.
    pub async fn connect(
        connection_string: impl Into<String>,
    ) -> Result<Self, PostgresFixtureError> {
        Self::connect_with_clock(connection_string, ManualClock::new(SystemTime::UNIX_EPOCH)).await
    }

    /// Connects a fixture over an explicit deterministic clock.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresFixtureError`] when the configuration is rejected,
    /// the database is unreachable, or its schema is unsupported.
    pub async fn connect_with_clock(
        connection_string: impl Into<String>,
        clock: ManualClock,
    ) -> Result<Self, PostgresFixtureError> {
        let config = plaintext_config(connection_string)?;
        let clock_handle: Arc<dyn Clock> = Arc::new(clock.clone());
        let repository = PostgresJobRepository::connect(config, clock_handle)
            .await
            .map_err(PostgresFixtureError::Repository)?;
        Ok(Self {
            repository,
            clock,
            ids: DeterministicIds::new(NonZeroU64::MIN),
        })
    }

    /// Borrows the durable repository.
    #[must_use]
    pub const fn repository(&self) -> &PostgresJobRepository {
        &self.repository
    }

    /// Borrows the fixture's deterministic clock.
    ///
    /// Every durable timestamp this fixture's repository writes is stamped
    /// from this clock, not wall-clock time: advancing it moves what the
    /// repository considers "now" for every subsequent write and for
    /// [`PostgresFixture::purge_job`].
    #[must_use]
    pub const fn clock(&self) -> &ManualClock {
        &self.clock
    }

    /// Borrows the fixture's deterministic ID source.
    #[must_use]
    pub const fn ids(&self) -> &DeterministicIds {
        &self.ids
    }

    /// Builds a same-resource chunk transaction manager over this fixture's
    /// repository.
    ///
    /// `state_provider` supplies the checkpoint/context this manager commits
    /// alongside business work and counters -- the adapter-owned hook the
    /// production [`PostgresChunkTransactionManager`] contract already
    /// requires, unchanged by this test kit.
    #[must_use]
    pub fn transaction_manager(
        &self,
        state_provider: Arc<dyn PostgresChunkStateProvider>,
    ) -> PostgresChunkTransactionManager {
        PostgresChunkTransactionManager::new(self.repository.clone(), state_provider)
    }

    /// Purges every terminal execution of `job_name` through the real,
    /// adapter-neutral production retention path.
    ///
    /// The production minimum retention age ([`MIN_PURGE_AGE`]) still
    /// applies and cannot be bypassed: advance [`PostgresFixture::clock`]
    /// past it (relative to when the job's executions were durably created)
    /// before calling this, so cleanup is deterministic and needs no real
    /// wall-clock wait.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresFixtureError`] when planning or applying the purge
    /// is rejected, e.g. because no execution is old enough yet.
    pub async fn purge_job(
        &self,
        job_name: JobName,
        operation_id: OperationId,
    ) -> Result<RetentionReport, PostgresFixtureError> {
        let clock_handle: Arc<dyn Clock> = Arc::new(self.clock.clone());
        let service = RetentionService::new(self.repository.clone(), clock_handle);
        let statuses = TerminalStatusSet::new([
            BatchStatus::Completed,
            BatchStatus::Failed,
            BatchStatus::Stopped,
            BatchStatus::Abandoned,
        ])
        .map_err(PostgresFixtureError::Retention)?;
        let batch = PurgeBatchBound::new(1000).map_err(PostgresFixtureError::Retention)?;
        let request = PurgePlanRequest::new(job_name, statuses, MIN_PURGE_AGE, batch)
            .map_err(PostgresFixtureError::Retention)?;
        let plan = service
            .plan_purge(&request)
            .await
            .map_err(PostgresFixtureError::Retention)?;
        let actor = ActorRef::new("oxide-batch-test").map_err(PostgresFixtureError::Request)?;
        let reason =
            ReasonCode::new("TEST_FIXTURE_CLEANUP").map_err(PostgresFixtureError::Request)?;
        service
            .apply_purge(operation_id, actor, reason, &plan)
            .await
            .map_err(PostgresFixtureError::Retention)
    }
}

fn plaintext_config(
    connection_string: impl Into<String>,
) -> Result<PostgresConfig, PostgresFixtureError> {
    PostgresConfig::new(connection_string)
        .map(|config| config.with_tls_mode(TlsMode::Plaintext))
        .map_err(PostgresFixtureError::Config)
}
