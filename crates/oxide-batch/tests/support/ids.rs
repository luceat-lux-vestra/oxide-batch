use std::error::Error;
use std::fmt;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex, PoisonError};

use oxide_batch::{
    DomainError, FailureId, IdGenerationError, IdGenerator, IdentifierKind, JobExecutionId,
    JobInstanceId, StepExecutionId,
};

/// A cloneable, monotonically increasing nonzero ID source.
#[derive(Clone, Debug)]
pub struct DeterministicIds {
    next: Arc<Mutex<Option<u64>>>,
}

impl DeterministicIds {
    /// Starts a sequence at the supplied nonzero value.
    #[must_use]
    pub fn new(first: NonZeroU64) -> Self {
        Self {
            next: Arc::new(Mutex::new(Some(first.get()))),
        }
    }

    /// Returns the next raw ID.
    ///
    /// # Errors
    ///
    /// Returns [`IdSequenceError::Exhausted`] after `u64::MAX` is issued.
    pub fn next_raw(&self) -> Result<u64, IdSequenceError> {
        let mut next = self.next.lock().unwrap_or_else(PoisonError::into_inner);
        let current = next.ok_or(IdSequenceError::Exhausted)?;
        *next = current.checked_add(1);
        Ok(current)
    }

    /// Returns the next job-instance ID.
    ///
    /// # Errors
    ///
    /// Returns an error if the sequence is exhausted or conversion fails.
    pub fn next_job_instance(&self) -> Result<JobInstanceId, IdSequenceError> {
        JobInstanceId::new(self.next_raw()?).map_err(IdSequenceError::Domain)
    }

    /// Returns the next job-execution ID.
    ///
    /// # Errors
    ///
    /// Returns an error if the sequence is exhausted or conversion fails.
    pub fn next_job_execution(&self) -> Result<JobExecutionId, IdSequenceError> {
        JobExecutionId::new(self.next_raw()?).map_err(IdSequenceError::Domain)
    }

    /// Returns the next step-execution ID.
    ///
    /// # Errors
    ///
    /// Returns an error if the sequence is exhausted or conversion fails.
    pub fn next_step_execution(&self) -> Result<StepExecutionId, IdSequenceError> {
        StepExecutionId::new(self.next_raw()?).map_err(IdSequenceError::Domain)
    }

    /// Returns the next opaque failure ID.
    ///
    /// # Errors
    ///
    /// Returns an error if the sequence is exhausted or conversion fails.
    pub fn next_failure(&self) -> Result<FailureId, IdSequenceError> {
        FailureId::new(self.next_raw()?).map_err(IdSequenceError::Domain)
    }
}

impl IdGenerator for DeterministicIds {
    fn next_job_instance_id(&self) -> Result<JobInstanceId, IdGenerationError> {
        self.next_job_instance()
            .map_err(|error| map_generation_error(error, IdentifierKind::JobInstance))
    }

    fn next_job_execution_id(&self) -> Result<JobExecutionId, IdGenerationError> {
        self.next_job_execution()
            .map_err(|error| map_generation_error(error, IdentifierKind::JobExecution))
    }

    fn next_step_execution_id(&self) -> Result<StepExecutionId, IdGenerationError> {
        self.next_step_execution()
            .map_err(|error| map_generation_error(error, IdentifierKind::StepExecution))
    }
}

fn map_generation_error(error: IdSequenceError, kind: IdentifierKind) -> IdGenerationError {
    match error {
        IdSequenceError::Exhausted => IdGenerationError::Exhausted { kind },
        IdSequenceError::Domain(error) => IdGenerationError::Invalid(error),
    }
}

/// Failure from a deterministic ID sequence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IdSequenceError {
    /// Every representable nonzero ID has been issued.
    Exhausted,
    /// A generated value violated a domain ID invariant.
    Domain(DomainError),
}

impl fmt::Display for IdSequenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exhausted => formatter.write_str("deterministic ID sequence is exhausted"),
            Self::Domain(error) => write!(formatter, "generated ID was invalid: {error}"),
        }
    }
}

impl Error for IdSequenceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Exhausted => None,
            Self::Domain(error) => Some(error),
        }
    }
}
