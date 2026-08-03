use std::error::Error;
use std::fmt;
use std::time::SystemTime;

use super::{BatchStatus, DomainError, FailureSummary};

/// A database-agnostic optimistic-lock version for an execution record.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ExecutionVersion(u64);

impl ExecutionVersion {
    /// The version assigned to a newly created execution attempt.
    pub const INITIAL: Self = Self(0);

    /// Reconstructs a version stored by a repository.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the repository-independent numeric value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the next optimistic version.
    ///
    /// # Errors
    ///
    /// Returns [`LifecycleError::VersionExhausted`] at the representable limit.
    pub fn next(self) -> Result<Self, LifecycleError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(LifecycleError::VersionExhausted { version: self })
    }
}

impl fmt::Display for ExecutionVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// A requested framework lifecycle transition and its deterministic timestamp.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LifecycleTransition {
    target: BatchStatus,
    transitioned_at: SystemTime,
    failure: Option<FailureSummary>,
    terminal_rollback: bool,
}

impl LifecycleTransition {
    /// Requests a transition that does not introduce a failure.
    #[must_use]
    pub const fn new(target: BatchStatus, transitioned_at: SystemTime) -> Self {
        Self {
            target,
            transitioned_at,
            failure: None,
            terminal_rollback: false,
        }
    }

    /// Requests a transition to `FAILED` with a redacted failure summary.
    #[must_use]
    pub const fn failed(transitioned_at: SystemTime, failure: FailureSummary) -> Self {
        Self {
            target: BatchStatus::Failed,
            transitioned_at,
            failure: Some(failure),
            terminal_rollback: false,
        }
    }

    /// Returns the requested framework status.
    #[must_use]
    pub const fn target(self) -> BatchStatus {
        self.target
    }

    /// Returns the deterministic transition instant supplied by the caller.
    #[must_use]
    pub const fn transitioned_at(self) -> SystemTime {
        self.transitioned_at
    }

    /// Marks the transition as one whose terminal work was rolled back.
    #[must_use]
    pub const fn with_terminal_rollback(mut self) -> Self {
        self.terminal_rollback = true;
        self
    }

    pub(crate) const fn terminal_rollback(self) -> bool {
        self.terminal_rollback
    }

    pub(crate) const fn failure(self) -> Option<FailureSummary> {
        self.failure
    }
}

/// A typed lifecycle-policy or optimistic-concurrency failure.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum LifecycleError {
    /// The caller observed an older or otherwise different execution version.
    StaleVersion {
        /// The version supplied by the caller.
        expected: ExecutionVersion,
        /// The current version of the execution record.
        actual: ExecutionVersion,
    },
    /// The requested in-place status transition is not legal.
    IllegalTransition {
        /// The current framework status.
        from: BatchStatus,
        /// The requested framework status.
        to: BatchStatus,
    },
    /// Restart is valid only by creating another execution attempt.
    RestartRequiresNewAttempt {
        /// The finished status from which a restart was requested.
        from: BatchStatus,
    },
    /// The current execution outcome cannot be restarted.
    NotRestartable {
        /// The status that prevents restart.
        status: BatchStatus,
    },
    /// A restart reused a prior job- or step-attempt identifier.
    AttemptIdentifierReused,
    /// A transition to `FAILED` did not include a redacted failure summary.
    FailedTransitionMissingFailure,
    /// A supplied transition instant violated timestamp ordering.
    InvalidTransitionTime {
        /// The facade-owned validation failure.
        source: DomainError,
    },
    /// The optimistic version cannot be incremented.
    VersionExhausted {
        /// The maximum current version.
        version: ExecutionVersion,
    },
    /// A durable execution counter cannot be incremented.
    CountExhausted,
}

impl fmt::Display for LifecycleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StaleVersion { expected, actual } => {
                write!(
                    formatter,
                    "stale execution version: expected {expected}, actual {actual}"
                )
            }
            Self::IllegalTransition { from, to } => {
                write!(
                    formatter,
                    "illegal lifecycle transition from {from} to {to}"
                )
            }
            Self::RestartRequiresNewAttempt { from } => write!(
                formatter,
                "restart from {from} requires a new execution attempt"
            ),
            Self::NotRestartable { status } => {
                write!(formatter, "an execution in {status} is not restartable")
            }
            Self::AttemptIdentifierReused => {
                formatter.write_str("a restart requires a distinct execution identifier")
            }
            Self::FailedTransitionMissingFailure => {
                formatter.write_str("a transition to FAILED requires a failure summary")
            }
            Self::InvalidTransitionTime { .. } => {
                formatter.write_str("the lifecycle transition timestamp is out of order")
            }
            Self::VersionExhausted { version } => {
                write!(
                    formatter,
                    "execution version {version} cannot be incremented"
                )
            }
            Self::CountExhausted => formatter.write_str("an execution counter is exhausted"),
        }
    }
}

impl Error for LifecycleError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidTransitionTime { source } => Some(source),
            _ => None,
        }
    }
}

pub(crate) fn validate_expected_version(
    expected: ExecutionVersion,
    actual: ExecutionVersion,
) -> Result<(), LifecycleError> {
    if expected != actual {
        return Err(LifecycleError::StaleVersion { expected, actual });
    }
    Ok(())
}

pub(crate) const fn is_legal_in_place_transition(from: BatchStatus, to: BatchStatus) -> bool {
    matches!(
        (from, to),
        (
            BatchStatus::Starting,
            BatchStatus::Started
                | BatchStatus::Stopping
                | BatchStatus::Failed
                | BatchStatus::Unknown
        ) | (
            BatchStatus::Started,
            BatchStatus::Stopping
                | BatchStatus::Stopped
                | BatchStatus::Failed
                | BatchStatus::Completed
                | BatchStatus::Unknown
        ) | (
            BatchStatus::Stopping,
            BatchStatus::Stopped | BatchStatus::Failed | BatchStatus::Unknown
        ) | (
            BatchStatus::Stopped | BatchStatus::Failed | BatchStatus::Unknown,
            BatchStatus::Abandoned
        ) | (BatchStatus::Unknown, BatchStatus::Failed)
    )
}

pub(crate) fn validate_transition(
    from: BatchStatus,
    transition: LifecycleTransition,
) -> Result<(), LifecycleError> {
    let to = transition.target();
    if matches!(from, BatchStatus::Stopped | BatchStatus::Failed)
        && matches!(to, BatchStatus::Starting)
    {
        return Err(LifecycleError::RestartRequiresNewAttempt { from });
    }
    if !is_legal_in_place_transition(from, to) {
        return Err(LifecycleError::IllegalTransition { from, to });
    }
    if matches!(to, BatchStatus::Failed) && transition.failure().is_none() {
        return Err(LifecycleError::FailedTransitionMissingFailure);
    }
    Ok(())
}

pub(crate) fn validate_restart(status: BatchStatus) -> Result<(), LifecycleError> {
    if !matches!(status, BatchStatus::Stopped | BatchStatus::Failed) {
        return Err(LifecycleError::NotRestartable { status });
    }
    Ok(())
}
