//! Repository, clock, identifier, and unit-of-work contracts.

use std::error::Error;
use std::fmt;
use std::future::Future;
use std::num::NonZeroU64;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use crate::{
    BatchStatus, DomainError, ExecutionVersion, ExitStatus, FailureId, IdentifierKind,
    JobExecution, JobExecutionId, JobInstance, JobInstanceId, JobInstanceKey, LifecycleError,
    LifecycleTransition, StepExecution, StepExecutionId, StepName,
};

mod memory;
#[cfg(feature = "postgres")]
mod postgres;

pub use memory::InMemoryJobRepository;
#[cfg(feature = "postgres")]
pub use postgres::{
    CaCertificate, PostgresChunkStateError, PostgresChunkStateProvider,
    PostgresChunkTransactionManager, PostgresConfig, PostgresConfigError, PostgresDurableStepState,
    PostgresJobRepository, PostgresMigrator, TlsMode,
};

/// An owned, dynamically dispatched future used by public asynchronous ports.
///
/// The alias is runtime-neutral: callers may poll it with Tokio or another
/// compatible executor, and repository implementations need not expose their
/// executor or database-driver types.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Supplies instants to repository and runtime operations.
///
/// Implementations must be thread-safe. Test clocks should return controlled
/// instants rather than consulting wall-clock time.
pub trait Clock: Send + Sync {
    /// Returns the current instant.
    fn now(&self) -> SystemTime;
}

/// An explicitly injected wall-clock implementation.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

/// Supplies facade-owned opaque identifiers.
///
/// One generator may use a shared sequence for all identifier kinds or
/// independent sequences. Implementations must never return zero.
pub trait IdGenerator: Send + Sync {
    /// Returns the next job-instance identifier.
    ///
    /// # Errors
    ///
    /// Returns [`IdGenerationError`] when the source is exhausted or produces
    /// an invalid value.
    fn next_job_instance_id(&self) -> Result<JobInstanceId, IdGenerationError>;

    /// Returns the next job-execution identifier.
    ///
    /// # Errors
    ///
    /// Returns [`IdGenerationError`] when the source is exhausted or produces
    /// an invalid value.
    fn next_job_execution_id(&self) -> Result<JobExecutionId, IdGenerationError>;

    /// Returns the next step-execution identifier.
    ///
    /// # Errors
    ///
    /// Returns [`IdGenerationError`] when the source is exhausted or produces
    /// an invalid value.
    fn next_step_execution_id(&self) -> Result<StepExecutionId, IdGenerationError>;

    /// Returns the next opaque failure-correlation identifier.
    ///
    /// # Errors
    ///
    /// Returns [`IdGenerationError`] when the source is exhausted or produces
    /// an invalid value.
    fn next_failure_id(&self) -> Result<FailureId, IdGenerationError>;
}

/// A thread-safe nonzero identifier sequence suitable for local execution.
///
/// The sequence is deterministic for a given call order. A single sequence is
/// shared by all identifier kinds so generated values cannot collide when
/// records are inspected together.
#[derive(Debug)]
pub struct SequentialIdGenerator {
    next: AtomicU64,
}

impl SequentialIdGenerator {
    /// Constructs a sequence whose first returned value is `first`.
    #[must_use]
    pub const fn new(first: NonZeroU64) -> Self {
        Self {
            next: AtomicU64::new(first.get()),
        }
    }

    fn next_raw(&self, kind: IdentifierKind) -> Result<u64, IdGenerationError> {
        self.next
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                if current == 0 {
                    None
                } else {
                    Some(current.checked_add(1).unwrap_or(0))
                }
            })
            .map_err(|_| IdGenerationError::Exhausted { kind })
    }
}

impl IdGenerator for SequentialIdGenerator {
    fn next_job_instance_id(&self) -> Result<JobInstanceId, IdGenerationError> {
        JobInstanceId::new(self.next_raw(IdentifierKind::JobInstance)?)
            .map_err(IdGenerationError::Invalid)
    }

    fn next_job_execution_id(&self) -> Result<JobExecutionId, IdGenerationError> {
        JobExecutionId::new(self.next_raw(IdentifierKind::JobExecution)?)
            .map_err(IdGenerationError::Invalid)
    }

    fn next_step_execution_id(&self) -> Result<StepExecutionId, IdGenerationError> {
        StepExecutionId::new(self.next_raw(IdentifierKind::StepExecution)?)
            .map_err(IdGenerationError::Invalid)
    }

    fn next_failure_id(&self) -> Result<FailureId, IdGenerationError> {
        FailureId::new(self.next_raw(IdentifierKind::Failure)?).map_err(IdGenerationError::Invalid)
    }
}

/// Failure from an injected identifier source.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum IdGenerationError {
    /// The source cannot issue another identifier of this kind.
    Exhausted {
        /// The identifier category that was requested.
        kind: IdentifierKind,
    },
    /// The source produced a value that violated a domain invariant.
    Invalid(DomainError),
}

impl fmt::Display for IdGenerationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exhausted { kind } => write!(formatter, "{kind} identifier source is exhausted"),
            Self::Invalid(error) => write!(formatter, "generated identifier was invalid: {error}"),
        }
    }
}

impl Error for IdGenerationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Invalid(error) => Some(error),
            Self::Exhausted { .. } => None,
        }
    }
}

/// The result of selecting the canonical instance for an identifying key.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum JobInstanceSelection {
    /// This unit of work created the logical instance.
    Created(JobInstance),
    /// The logical instance already existed.
    Existing(JobInstance),
}

impl JobInstanceSelection {
    /// Borrows the selected instance regardless of whether it was created.
    #[must_use]
    pub const fn instance(&self) -> &JobInstance {
        match self {
            Self::Created(instance) | Self::Existing(instance) => instance,
        }
    }

    /// Returns whether the instance was created by this operation.
    #[must_use]
    pub const fn was_created(&self) -> bool {
        matches!(self, Self::Created(_))
    }
}

/// Starts isolated repository units of work.
///
/// A unit of work does not become visible until it is committed. Dropping one
/// without committing has rollback semantics.
pub trait JobRepository: Send + Sync {
    /// Begins a repository-owned unit of work.
    ///
    /// The returned object may borrow this repository and cannot outlive it.
    fn begin<'a>(
        &'a self,
    ) -> BoxFuture<'a, Result<Box<dyn RepositoryUnitOfWork + 'a>, RepositoryError>>;
}

/// Transaction-scoped metadata operations required by the executable kernel.
///
/// Methods borrow the unit of work for the returned future, allowing a future
/// `PostgreSQL` adapter to keep its concrete transaction private. A successful
/// operation is still provisional until [`commit`](Self::commit) succeeds.
pub trait RepositoryUnitOfWork: Send {
    /// Selects or creates the unique logical instance for `key`.
    fn select_or_create_job_instance<'a>(
        &'a mut self,
        key: &'a JobInstanceKey,
    ) -> BoxFuture<'a, Result<JobInstanceSelection, RepositoryError>>;

    /// Creates a new launch or restart attempt for an existing instance.
    ///
    /// A first attempt is allowed when no prior execution exists. A later
    /// attempt is allowed only after `STOPPED` or `FAILED`. Completed,
    /// abandoned, active, and unknown instances are rejected.
    fn create_job_execution(
        &mut self,
        job_instance_id: JobInstanceId,
    ) -> BoxFuture<'_, Result<JobExecution, RepositoryError>>;

    /// Creates a step attempt linked to an existing job execution.
    fn create_step_execution<'a>(
        &'a mut self,
        job_execution_id: JobExecutionId,
        step_name: &'a StepName,
    ) -> BoxFuture<'a, Result<StepExecution, RepositoryError>>;

    /// Applies a compare-and-swap lifecycle transition to a job execution.
    fn transition_job_execution(
        &mut self,
        id: JobExecutionId,
        expected_version: ExecutionVersion,
        transition: LifecycleTransition,
    ) -> BoxFuture<'_, Result<JobExecution, RepositoryError>>;

    /// Enriches a job execution's exit status with compare-and-swap semantics.
    fn enrich_job_exit_status<'a>(
        &'a mut self,
        id: JobExecutionId,
        expected_version: ExecutionVersion,
        exit_status: &'a ExitStatus,
    ) -> BoxFuture<'a, Result<JobExecution, RepositoryError>>;

    /// Applies a compare-and-swap lifecycle transition to a step execution.
    fn transition_step_execution(
        &mut self,
        id: StepExecutionId,
        expected_version: ExecutionVersion,
        transition: LifecycleTransition,
    ) -> BoxFuture<'_, Result<StepExecution, RepositoryError>>;

    /// Enriches a step execution's exit status with compare-and-swap semantics.
    fn enrich_step_exit_status<'a>(
        &'a mut self,
        id: StepExecutionId,
        expected_version: ExecutionVersion,
        exit_status: &'a ExitStatus,
    ) -> BoxFuture<'a, Result<StepExecution, RepositoryError>>;

    /// Finds a job instance by its canonical identifying key.
    fn find_job_instance<'a>(
        &'a mut self,
        key: &'a JobInstanceKey,
    ) -> BoxFuture<'a, Result<Option<JobInstance>, RepositoryError>>;

    /// Loads one job instance snapshot by its opaque identifier.
    fn get_job_instance(
        &mut self,
        id: JobInstanceId,
    ) -> BoxFuture<'_, Result<Option<JobInstance>, RepositoryError>>;

    /// Loads one job execution snapshot for inspection.
    fn get_job_execution(
        &mut self,
        id: JobExecutionId,
    ) -> BoxFuture<'_, Result<Option<JobExecution>, RepositoryError>>;

    /// Loads job execution snapshots in creation order.
    fn job_executions(
        &mut self,
        job_instance_id: JobInstanceId,
    ) -> BoxFuture<'_, Result<Vec<JobExecution>, RepositoryError>>;

    /// Loads one step execution snapshot for inspection.
    fn get_step_execution(
        &mut self,
        id: StepExecutionId,
    ) -> BoxFuture<'_, Result<Option<StepExecution>, RepositoryError>>;

    /// Loads step execution snapshots in creation order.
    fn step_executions(
        &mut self,
        job_execution_id: JobExecutionId,
    ) -> BoxFuture<'_, Result<Vec<StepExecution>, RepositoryError>>;

    /// Atomically publishes all changes made by this unit of work.
    fn commit<'a>(self: Box<Self>) -> BoxFuture<'a, Result<(), RepositoryError>>
    where
        Self: 'a;

    /// Explicitly rolls back this unit of work.
    ///
    /// Dropping a unit of work has the same metadata effect.
    fn rollback<'a>(self: Box<Self>) -> BoxFuture<'a, Result<(), RepositoryError>>
    where
        Self: 'a;
}

/// A stable repository failure independent of a database or async runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RepositoryError {
    /// The durable metadata schema has not been initialized.
    SchemaUninitialized,
    /// The durable metadata schema must be migrated before use.
    MigrationRequired {
        /// The version found in the database.
        current: u32,
        /// The version understood by this runtime.
        supported: u32,
    },
    /// The durable metadata schema is newer than this runtime.
    NewerSchema {
        /// The version found in the database.
        current: u32,
        /// The version understood by this runtime.
        supported: u32,
    },
    /// A facade identifier cannot be represented by the durable adapter.
    IdentifierOutOfRange {
        /// The identifier category.
        kind: IdentifierKind,
        /// The rejected facade value.
        value: u64,
    },
    /// A referenced job instance does not exist.
    JobInstanceNotFound {
        /// The missing identifier.
        id: JobInstanceId,
    },
    /// A referenced job execution does not exist.
    JobExecutionNotFound {
        /// The missing identifier.
        id: JobExecutionId,
    },
    /// A referenced step execution does not exist.
    StepExecutionNotFound {
        /// The missing identifier.
        id: StepExecutionId,
    },
    /// An injected source reused an existing identifier.
    DuplicateIdentifier {
        /// The duplicated identifier category.
        kind: IdentifierKind,
        /// The duplicated numeric value.
        value: u64,
    },
    /// A completed logical instance cannot be launched again.
    CompletedInstance {
        /// The terminal logical instance.
        id: JobInstanceId,
    },
    /// An abandoned logical instance cannot be launched again.
    AbandonedInstance {
        /// The terminal logical instance.
        id: JobInstanceId,
    },
    /// A prior attempt is active or requires explicit recovery.
    ExecutionAlreadyActive {
        /// The logical instance selected for launch.
        instance_id: JobInstanceId,
        /// The attempt preventing another launch.
        execution_id: JobExecutionId,
        /// Its current framework status.
        status: BatchStatus,
    },
    /// A domain value could not be constructed.
    Domain(DomainError),
    /// An injected identifier source failed.
    Identifier(IdGenerationError),
    /// A lifecycle or optimistic-version rule rejected an update.
    Lifecycle(LifecycleError),
    /// Another committed unit of work invalidated this snapshot.
    ConcurrentModification,
    /// A commit failed after `PostgreSQL` may have made it durable.
    ///
    /// Callers must inspect durable metadata through a new healthy unit of
    /// work before deciding whether to retry.
    CommitOutcomeUnknown,
    /// The repository is unavailable because of an infrastructure failure.
    Unavailable,
}

impl fmt::Display for RepositoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SchemaUninitialized => {
                formatter.write_str("PostgreSQL metadata schema is not initialized")
            }
            Self::MigrationRequired { current, supported } => write!(
                formatter,
                "PostgreSQL metadata schema version {current} requires migration to {supported}"
            ),
            Self::NewerSchema { current, supported } => write!(
                formatter,
                "PostgreSQL metadata schema version {current} is newer than supported version {supported}"
            ),
            Self::IdentifierOutOfRange { kind, value } => {
                write!(
                    formatter,
                    "{kind} identifier {value} exceeds PostgreSQL bigint"
                )
            }
            Self::JobInstanceNotFound { id } => {
                write!(formatter, "job instance {id} was not found")
            }
            Self::JobExecutionNotFound { id } => {
                write!(formatter, "job execution {id} was not found")
            }
            Self::StepExecutionNotFound { id } => {
                write!(formatter, "step execution {id} was not found")
            }
            Self::DuplicateIdentifier { kind, value } => {
                write!(formatter, "{kind} identifier {value} already exists")
            }
            Self::CompletedInstance { id } => {
                write!(formatter, "job instance {id} is already completed")
            }
            Self::AbandonedInstance { id } => {
                write!(formatter, "job instance {id} is abandoned")
            }
            Self::ExecutionAlreadyActive {
                instance_id,
                execution_id,
                status,
            } => write!(
                formatter,
                "job instance {instance_id} already has execution {execution_id} in {status}"
            ),
            Self::Domain(error) => write!(formatter, "invalid repository domain value: {error}"),
            Self::Identifier(error) => write!(formatter, "identifier generation failed: {error}"),
            Self::Lifecycle(error) => error.fmt(formatter),
            Self::ConcurrentModification => {
                formatter.write_str("repository unit of work is based on a stale snapshot")
            }
            Self::CommitOutcomeUnknown => formatter.write_str(
                "PostgreSQL commit outcome is unknown; inspect durable metadata before recovery",
            ),
            Self::Unavailable => formatter.write_str("repository is unavailable"),
        }
    }
}

impl Error for RepositoryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Domain(error) => Some(error),
            Self::Identifier(error) => Some(error),
            Self::Lifecycle(error) => Some(error),
            _ => None,
        }
    }
}

impl From<DomainError> for RepositoryError {
    fn from(error: DomainError) -> Self {
        Self::Domain(error)
    }
}

impl From<IdGenerationError> for RepositoryError {
    fn from(error: IdGenerationError) -> Self {
        Self::Identifier(error)
    }
}

impl From<LifecycleError> for RepositoryError {
    fn from(error: LifecycleError) -> Self {
        Self::Lifecycle(error)
    }
}
