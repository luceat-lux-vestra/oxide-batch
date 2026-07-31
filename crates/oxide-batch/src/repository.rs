//! Repository, clock, identifier, and unit-of-work contracts.

use std::error::Error;
use std::fmt;
use std::future::Future;
use std::num::NonZeroU64;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use crate::{
    BatchStatus, DefinitionIdentity, DefinitionRevision, DefinitionUpgrade, DomainError,
    ExecutionMetadata, ExecutionTimestamps, ExecutionVersion, ExitStatus, FailureCategory,
    FailureId, FailureSummary, FlowDecision, FlowDecisionRequest, FlowStepState,
    FlowTransitionKind, IdentifierKind, JobExecution, JobExecutionId, JobInstance, JobInstanceId,
    JobInstanceKey, JobName, LifecycleError, LifecycleTransition, NodeId, StartLimit,
    StepExecution, StepExecutionId, StepName,
};

const MAX_RECOVERY_REASON_BYTES: usize = 64;
const MAX_OPERATOR_REFERENCE_BYTES: usize = 128;

mod memory;
#[cfg(feature = "postgres")]
mod postgres;

pub use memory::InMemoryJobRepository;
#[cfg(feature = "postgres")]
pub use postgres::{
    CaCertificate, PostgresChunkStateError, PostgresChunkStateProvider,
    PostgresChunkTransactionManager, PostgresConfig, PostgresConfigError, PostgresDurableStepState,
    PostgresFaultState, PostgresJobRepository, PostgresMigrator, TlsMode,
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

/// Explicit operator disposition for an orphaned or ambiguous execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RecoveryDisposition {
    /// Make the observed attempt restart-eligible.
    MarkFailed,
    /// Make the logical instance permanently non-restartable.
    Abandon,
}

impl RecoveryDisposition {
    /// Returns the durable status produced by this disposition.
    #[must_use]
    pub const fn resulting_status(self) -> BatchStatus {
        match self {
            Self::MarkFailed => BatchStatus::Failed,
            Self::Abandon => BatchStatus::Abandoned,
        }
    }
}

/// Bounded, value-redacted request for one audited recovery decision.
#[derive(Clone, Eq, PartialEq)]
pub struct RecoveryRequest {
    expected_version: ExecutionVersion,
    disposition: RecoveryDisposition,
    reason_code: String,
    operator_reference: String,
    evidence_digest: [u8; 32],
    failure: Option<FailureSummary>,
}

impl RecoveryRequest {
    /// Validates a request that makes an observed execution restart-eligible.
    ///
    /// Authentication and authorization remain deployment responsibilities;
    /// `operator_reference` is an opaque audit correlation, not a credential.
    ///
    /// # Errors
    ///
    /// Rejects empty, oversized, whitespace-padded, or control-containing
    /// reason and operator values.
    pub fn mark_failed(
        expected_version: ExecutionVersion,
        reason_code: impl Into<String>,
        operator_reference: impl Into<String>,
        evidence_digest: [u8; 32],
        failure_category: FailureCategory,
        failure_id: FailureId,
    ) -> Result<Self, RecoveryRequestError> {
        Self::new(
            expected_version,
            RecoveryDisposition::MarkFailed,
            reason_code,
            operator_reference,
            evidence_digest,
            Some(FailureSummary::new(failure_category, failure_id)),
        )
    }

    /// Validates a request that permanently abandons the logical instance.
    ///
    /// # Errors
    ///
    /// Rejects empty, oversized, whitespace-padded, or control-containing
    /// reason and operator values.
    pub fn abandon(
        expected_version: ExecutionVersion,
        reason_code: impl Into<String>,
        operator_reference: impl Into<String>,
        evidence_digest: [u8; 32],
    ) -> Result<Self, RecoveryRequestError> {
        Self::new(
            expected_version,
            RecoveryDisposition::Abandon,
            reason_code,
            operator_reference,
            evidence_digest,
            None,
        )
    }

    fn new(
        expected_version: ExecutionVersion,
        disposition: RecoveryDisposition,
        reason_code: impl Into<String>,
        operator_reference: impl Into<String>,
        evidence_digest: [u8; 32],
        failure: Option<FailureSummary>,
    ) -> Result<Self, RecoveryRequestError> {
        let reason_code = reason_code.into();
        validate_recovery_text(
            &reason_code,
            RecoveryField::ReasonCode,
            MAX_RECOVERY_REASON_BYTES,
        )?;
        let operator_reference = operator_reference.into();
        validate_recovery_text(
            &operator_reference,
            RecoveryField::OperatorReference,
            MAX_OPERATOR_REFERENCE_BYTES,
        )?;
        Ok(Self {
            expected_version,
            disposition,
            reason_code,
            operator_reference,
            evidence_digest,
            failure,
        })
    }

    /// Returns the observed optimistic version.
    #[must_use]
    pub const fn expected_version(&self) -> ExecutionVersion {
        self.expected_version
    }

    /// Returns the requested disposition.
    #[must_use]
    pub const fn disposition(&self) -> RecoveryDisposition {
        self.disposition
    }

    /// Borrows the bounded reason code.
    #[must_use]
    pub fn reason_code(&self) -> &str {
        &self.reason_code
    }

    /// Borrows the opaque operator correlation.
    #[must_use]
    pub fn operator_reference(&self) -> &str {
        &self.operator_reference
    }

    /// Returns the digest of externally retained inspection evidence.
    #[must_use]
    pub const fn evidence_digest(&self) -> &[u8; 32] {
        &self.evidence_digest
    }

    /// Returns the typed failure applied by a `FAILED` disposition.
    #[must_use]
    pub const fn failure(&self) -> Option<FailureSummary> {
        self.failure
    }
}

impl fmt::Debug for RecoveryRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecoveryRequest")
            .field("expected_version", &self.expected_version)
            .field("disposition", &self.disposition)
            .field("reason_code", &self.reason_code)
            .field("operator_reference", &self.operator_reference)
            .field("evidence_digest", &"<redacted>")
            .field("failure", &self.failure)
            .finish()
    }
}

/// One append-only recovery audit record.
#[derive(Clone, Eq, PartialEq)]
pub struct RecoveryDecision {
    job_execution_id: JobExecutionId,
    execution_version: ExecutionVersion,
    prior_status: BatchStatus,
    resulting_status: BatchStatus,
    reason_code: String,
    operator_reference: String,
    evidence_digest: [u8; 32],
    decided_at: SystemTime,
}

impl RecoveryDecision {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        job_execution_id: JobExecutionId,
        execution_version: ExecutionVersion,
        prior_status: BatchStatus,
        resulting_status: BatchStatus,
        reason_code: String,
        operator_reference: String,
        evidence_digest: [u8; 32],
        decided_at: SystemTime,
    ) -> Self {
        Self {
            job_execution_id,
            execution_version,
            prior_status,
            resulting_status,
            reason_code,
            operator_reference,
            evidence_digest,
            decided_at,
        }
    }

    /// Returns the execution whose observed version was resolved.
    #[must_use]
    pub const fn job_execution_id(&self) -> JobExecutionId {
        self.job_execution_id
    }

    /// Returns the observed version before the decision.
    #[must_use]
    pub const fn execution_version(&self) -> ExecutionVersion {
        self.execution_version
    }

    /// Returns the status observed under lock.
    #[must_use]
    pub const fn prior_status(&self) -> BatchStatus {
        self.prior_status
    }

    /// Returns the durable status produced by the decision.
    #[must_use]
    pub const fn resulting_status(&self) -> BatchStatus {
        self.resulting_status
    }

    /// Borrows the bounded reason code.
    #[must_use]
    pub fn reason_code(&self) -> &str {
        &self.reason_code
    }

    /// Borrows the opaque operator correlation.
    #[must_use]
    pub fn operator_reference(&self) -> &str {
        &self.operator_reference
    }

    /// Returns the digest of externally retained evidence.
    #[must_use]
    pub const fn evidence_digest(&self) -> &[u8; 32] {
        &self.evidence_digest
    }

    /// Returns the injected facade-clock decision time.
    #[must_use]
    pub const fn decided_at(&self) -> SystemTime {
        self.decided_at
    }
}

impl fmt::Debug for RecoveryDecision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecoveryDecision")
            .field("job_execution_id", &self.job_execution_id)
            .field("execution_version", &self.execution_version)
            .field("prior_status", &self.prior_status)
            .field("resulting_status", &self.resulting_status)
            .field("reason_code", &self.reason_code)
            .field("operator_reference", &self.operator_reference)
            .field("evidence_digest", &"<redacted>")
            .field("decided_at", &self.decided_at)
            .finish()
    }
}

/// Result of atomically appending an audit decision and changing execution state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryResult {
    execution: JobExecution,
    decision: RecoveryDecision,
}

impl RecoveryResult {
    pub(crate) const fn new(execution: JobExecution, decision: RecoveryDecision) -> Self {
        Self {
            execution,
            decision,
        }
    }

    /// Borrows the recovered execution snapshot.
    #[must_use]
    pub const fn execution(&self) -> &JobExecution {
        &self.execution
    }

    /// Borrows the append-only audit decision.
    #[must_use]
    pub const fn decision(&self) -> &RecoveryDecision {
        &self.decision
    }
}

/// Recovery request field category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RecoveryField {
    /// Stable machine-readable reason code.
    ReasonCode,
    /// Opaque authenticated-operator correlation.
    OperatorReference,
}

/// Invalid bounded recovery request.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RecoveryRequestError {
    /// A field was empty.
    Empty {
        /// Invalid field.
        field: RecoveryField,
    },
    /// A field exceeded its UTF-8 byte bound.
    TooLong {
        /// Invalid field.
        field: RecoveryField,
        /// Maximum accepted UTF-8 bytes.
        max_bytes: usize,
    },
    /// A field had surrounding whitespace.
    SurroundingWhitespace {
        /// Invalid field.
        field: RecoveryField,
    },
    /// A field contained a control character.
    ControlCharacter {
        /// Invalid field.
        field: RecoveryField,
    },
}

impl fmt::Display for RecoveryRequestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty { field } => write!(formatter, "{field:?} must not be empty"),
            Self::TooLong { field, max_bytes } => {
                write!(formatter, "{field:?} exceeds {max_bytes} bytes")
            }
            Self::SurroundingWhitespace { field } => {
                write!(formatter, "{field:?} has surrounding whitespace")
            }
            Self::ControlCharacter { field } => {
                write!(formatter, "{field:?} contains a control character")
            }
        }
    }
}

impl Error for RecoveryRequestError {}

fn validate_recovery_text(
    value: &str,
    field: RecoveryField,
    max_bytes: usize,
) -> Result<(), RecoveryRequestError> {
    if value.is_empty() {
        return Err(RecoveryRequestError::Empty { field });
    }
    if value.len() > max_bytes {
        return Err(RecoveryRequestError::TooLong { field, max_bytes });
    }
    if value.trim() != value {
        return Err(RecoveryRequestError::SurroundingWhitespace { field });
    }
    if value.chars().any(char::is_control) {
        return Err(RecoveryRequestError::ControlCharacter { field });
    }
    Ok(())
}

pub(crate) fn recovered_execution(
    prior: &JobExecution,
    request: &RecoveryRequest,
    decided_at: SystemTime,
) -> Result<JobExecution, RepositoryError> {
    if prior.version() != request.expected_version() {
        return Err(RepositoryError::Lifecycle(LifecycleError::StaleVersion {
            expected: request.expected_version(),
            actual: prior.version(),
        }));
    }
    let prior_status = prior.metadata().status();
    if !matches!(
        prior_status,
        BatchStatus::Starting | BatchStatus::Started | BatchStatus::Stopping | BatchStatus::Unknown
    ) {
        return Err(RepositoryError::RecoveryNotAllowed {
            id: prior.id(),
            status: prior_status,
        });
    }
    let current_time = prior.metadata().timestamps();
    let timestamps = ExecutionTimestamps::new(
        current_time.created_at(),
        current_time.started_at(),
        Some(decided_at),
    )?;
    let resulting_status = request.disposition().resulting_status();
    let metadata = ExecutionMetadata::new(
        resulting_status,
        prior.metadata().exit_status().clone(),
        timestamps,
        prior.metadata().counts(),
        request.failure(),
    )?;
    Ok(JobExecution::from_snapshot(
        prior.id(),
        prior.job_instance_id(),
        metadata,
        prior.version().next()?,
    ))
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
    /// Registers one explicit directed definition compatibility edge.
    fn register_definition_upgrade<'a>(
        &'a mut self,
        job_name: &'a JobName,
        upgrade: &'a DefinitionUpgrade,
    ) -> BoxFuture<'a, Result<(), RepositoryError>>;

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

    /// Creates an attempt bound to an exact restart-relevant definition.
    ///
    /// Durable adapters compare the supplied identity with the definition that
    /// produced the latest checkpoint before creating a restart attempt.
    fn create_job_execution_with_definition<'a>(
        &'a mut self,
        job_instance_id: JobInstanceId,
        definition: &'a DefinitionIdentity,
    ) -> BoxFuture<'a, Result<JobExecution, RepositoryError>>;

    /// Creates a step attempt linked to an existing job execution.
    fn create_step_execution<'a>(
        &'a mut self,
        job_execution_id: JobExecutionId,
        step_name: &'a StepName,
    ) -> BoxFuture<'a, Result<StepExecution, RepositoryError>>;

    /// Atomically checks an instance-wide start limit and creates one logical
    /// step attempt.
    ///
    /// Entering `STARTING` consumes one start. The logical ID is independent
    /// of the display/durable step name and is the restart authority for a
    /// format-2 plan.
    fn create_flow_step_execution<'a>(
        &'a mut self,
        _job_execution_id: JobExecutionId,
        _step_name: &'a StepName,
        _node_id: &'a NodeId,
        _start_limit: StartLimit,
    ) -> BoxFuture<'a, Result<StepExecution, RepositoryError>> {
        Box::pin(async { Err(RepositoryError::FlowStateCorrupt) })
    }

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

    /// Loads the latest durable attempt for one instance/logical-step pair.
    fn latest_flow_step<'a>(
        &'a mut self,
        _job_instance_id: JobInstanceId,
        _node_id: &'a NodeId,
    ) -> BoxFuture<'a, Result<Option<FlowStepState>, RepositoryError>> {
        Box::pin(async { Err(RepositoryError::FlowStateCorrupt) })
    }

    /// Appends one already plan-validated transition before its target starts.
    fn append_flow_decision<'a>(
        &'a mut self,
        _request: &'a FlowDecisionRequest,
    ) -> BoxFuture<'a, Result<FlowDecision, RepositoryError>> {
        Box::pin(async { Err(RepositoryError::FlowStateCorrupt) })
    }

    /// Finds a prior decision whose exact durable input may be reused.
    fn find_reusable_flow_decision<'a>(
        &'a mut self,
        _job_instance_id: JobInstanceId,
        _node_id: &'a NodeId,
        _plan_fingerprint: &'a [u8; 32],
        _input_digest: &'a [u8; 32],
        _kind: FlowTransitionKind,
    ) -> BoxFuture<'a, Result<Option<FlowDecision>, RepositoryError>> {
        Box::pin(async { Err(RepositoryError::FlowStateCorrupt) })
    }

    /// Loads one execution's flow decisions in sequence order.
    fn flow_decisions(
        &mut self,
        _job_execution_id: JobExecutionId,
    ) -> BoxFuture<'_, Result<Vec<FlowDecision>, RepositoryError>> {
        Box::pin(async { Err(RepositoryError::FlowStateCorrupt) })
    }

    /// Atomically resolves one orphaned or ambiguous execution and appends its audit record.
    fn recover_job_execution<'a>(
        &'a mut self,
        id: JobExecutionId,
        request: &'a RecoveryRequest,
    ) -> BoxFuture<'a, Result<RecoveryResult, RepositoryError>>;

    /// Loads the append-only recovery decision for one execution, when present.
    fn recovery_decision(
        &mut self,
        id: JobExecutionId,
    ) -> BoxFuture<'_, Result<Option<RecoveryDecision>, RepositoryError>>;

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
    /// One job name and revision were bound to a different manifest.
    DefinitionDrift {
        /// Definition whose application revision drifted.
        job_name: JobName,
        /// Reused application-owned revision.
        revision: DefinitionRevision,
    },
    /// A manifest was registered or launched under a different job name.
    DefinitionJobMismatch {
        /// Job name selected by the instance or registration call.
        expected: JobName,
        /// Job name encoded in the definition manifest.
        actual: JobName,
    },
    /// The proposed definition cannot interpret the latest checkpoint.
    IncompatibleDefinition {
        /// Logical instance whose last definition is incompatible.
        instance_id: JobInstanceId,
    },
    /// The runtime cannot interpret the supplied or persisted manifest format.
    UnsupportedManifestVersion {
        /// Unsupported format version.
        format: u16,
    },
    /// A registered directed edge did not map a required durable step.
    InvalidDefinitionUpgrade {
        /// New execution whose mapped state could not be resolved.
        execution_id: JobExecutionId,
    },
    /// A directed edge was already registered with different immutable content.
    DefinitionUpgradeConflict {
        /// Job whose edge conflicted.
        job_name: JobName,
    },
    /// A restartable definition required durable step state that was absent.
    RestartStateNotFound {
        /// New restart execution.
        execution_id: JobExecutionId,
        /// Target step whose source state was absent.
        step_name: StepName,
    },
    /// Durable fault state could not be interpreted, so no work may begin.
    ///
    /// Corruption, an unsupported fault-state version, a checksum mismatch, or
    /// state that belongs to a superseded checkpoint fails closed.
    FaultStateCorrupt,
    /// The instance-wide start limit for a logical step is exhausted.
    StartLimitExceeded {
        /// Logical instance whose historical starts were counted.
        instance_id: JobInstanceId,
        /// Stable logical step identifier.
        node_id: NodeId,
        /// Configured finite limit.
        limit: StartLimit,
    },
    /// Durable flow history is missing, contradictory, or corrupt.
    FlowStateCorrupt,
    /// Recovery was requested for a state that needs no recovery decision.
    RecoveryNotAllowed {
        /// Rejected execution.
        id: JobExecutionId,
        /// Durable status observed under lock.
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
    #[allow(clippy::too_many_lines)]
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
            Self::DefinitionDrift { job_name, revision } => write!(
                formatter,
                "job {job_name} definition revision {} has drifted",
                revision.as_str()
            ),
            Self::DefinitionJobMismatch { expected, actual } => write!(
                formatter,
                "definition for job {actual} cannot be used for job {expected}"
            ),
            Self::IncompatibleDefinition { instance_id } => write!(
                formatter,
                "job instance {instance_id} has no direct compatible definition"
            ),
            Self::UnsupportedManifestVersion { format } => {
                write!(
                    formatter,
                    "definition manifest format {format} is unsupported"
                )
            }
            Self::InvalidDefinitionUpgrade { execution_id } => write!(
                formatter,
                "definition upgrade for execution {execution_id} is incomplete"
            ),
            Self::DefinitionUpgradeConflict { job_name } => {
                write!(formatter, "job {job_name} definition upgrade conflicts")
            }
            Self::RestartStateNotFound {
                execution_id,
                step_name,
            } => write!(
                formatter,
                "restart execution {execution_id} has no durable source for step {step_name}"
            ),
            Self::FaultStateCorrupt => {
                formatter.write_str("durable fault state is unusable and no work may begin")
            }
            Self::StartLimitExceeded {
                instance_id,
                node_id,
                limit,
            } => write!(
                formatter,
                "job instance {instance_id} exhausted start limit {} for node {}",
                limit.get(),
                node_id.as_str()
            ),
            Self::FlowStateCorrupt => {
                formatter.write_str("durable flow history is unusable and no work may begin")
            }
            Self::RecoveryNotAllowed { id, status } => {
                write!(
                    formatter,
                    "job execution {id} in {status} cannot be recovered"
                )
            }
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
