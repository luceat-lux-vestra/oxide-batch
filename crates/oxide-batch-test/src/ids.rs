//! A deterministic [`IdGenerator`] for application tests.

use std::error::Error;
use std::fmt;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex, PoisonError};

use oxide_batch::{
    DomainError, FailureId, IdGenerationError, IdGenerator, IdentifierKind, JobExecutionId,
    JobInstanceId, StepExecutionId,
};

/// A cloneable, monotonically increasing nonzero ID source.
///
/// `DeterministicIds` implements the framework's own [`IdGenerator`] port. A
/// single shared sequence issues every identifier kind in call order, so
/// values generated in one test run are reproducible and never collide with
/// each other when inspected together. Exhaustion after `u64::MAX` fails
/// explicitly; it never falls back to a random UUID.
///
/// ```
/// use oxide_batch::IdGenerator;
/// use oxide_batch_test::DeterministicIds;
/// use std::num::NonZeroU64;
///
/// let ids = DeterministicIds::new(NonZeroU64::new(1).ok_or("nonzero")?);
/// let first = ids.next_job_execution_id()?;
/// let second = ids.next_job_execution_id()?;
/// assert_ne!(first, second);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
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

    /// Returns the next raw identifier.
    ///
    /// # Errors
    ///
    /// Returns [`IdSequenceError::Exhausted`] once every representable
    /// nonzero value has been issued.
    pub fn next_raw(&self) -> Result<u64, IdSequenceError> {
        let mut next = self.next.lock().unwrap_or_else(PoisonError::into_inner);
        let current = next.ok_or(IdSequenceError::Exhausted)?;
        *next = current.checked_add(1);
        Ok(current)
    }

    /// Returns the next job-instance identifier.
    ///
    /// # Errors
    ///
    /// Returns an error if the sequence is exhausted or the value is invalid.
    pub fn next_job_instance(&self) -> Result<JobInstanceId, IdSequenceError> {
        JobInstanceId::new(self.next_raw()?).map_err(IdSequenceError::Domain)
    }

    /// Returns the next job-execution identifier.
    ///
    /// # Errors
    ///
    /// Returns an error if the sequence is exhausted or the value is invalid.
    pub fn next_job_execution(&self) -> Result<JobExecutionId, IdSequenceError> {
        JobExecutionId::new(self.next_raw()?).map_err(IdSequenceError::Domain)
    }

    /// Returns the next step-execution identifier.
    ///
    /// # Errors
    ///
    /// Returns an error if the sequence is exhausted or the value is invalid.
    pub fn next_step_execution(&self) -> Result<StepExecutionId, IdSequenceError> {
        StepExecutionId::new(self.next_raw()?).map_err(IdSequenceError::Domain)
    }

    /// Returns the next opaque failure identifier.
    ///
    /// # Errors
    ///
    /// Returns an error if the sequence is exhausted or the value is invalid.
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

    fn next_failure_id(&self) -> Result<FailureId, IdGenerationError> {
        self.next_failure()
            .map_err(|error| map_generation_error(error, IdentifierKind::Failure))
    }
}

fn map_generation_error(error: IdSequenceError, kind: IdentifierKind) -> IdGenerationError {
    match error {
        IdSequenceError::Exhausted => IdGenerationError::Exhausted { kind },
        IdSequenceError::Domain(error) => IdGenerationError::Invalid(error),
    }
}

/// Failure from a [`DeterministicIds`] sequence.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum IdSequenceError {
    /// Every representable nonzero identifier has already been issued.
    Exhausted,
    /// A generated value violated a domain identifier invariant.
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
