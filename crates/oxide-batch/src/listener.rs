//! Job and step listener contracts with redacted failure diagnostics.

use std::error::Error;
use std::fmt;

use crate::{
    BoxFuture, ExecutionCorrelation, FailureSummary, JobParameters, StopToken,
    TaskletExecutionOutcome,
};

/// Borrowed execution data supplied to job and step listeners.
#[derive(Clone, Copy, Debug)]
pub struct ListenerContext<'a> {
    correlation: &'a ExecutionCorrelation,
    parameters: &'a JobParameters,
    stop: &'a StopToken,
}

impl<'a> ListenerContext<'a> {
    pub(crate) const fn new(
        correlation: &'a ExecutionCorrelation,
        parameters: &'a JobParameters,
        stop: &'a StopToken,
    ) -> Self {
        Self {
            correlation,
            parameters,
            stop,
        }
    }

    /// Borrows the complete bounded execution correlation.
    #[must_use]
    pub const fn correlation(&self) -> &'a ExecutionCorrelation {
        self.correlation
    }

    /// Borrows launch parameters for authorized application use.
    ///
    /// The framework never copies these values into listener diagnostics.
    #[must_use]
    pub const fn parameters(&self) -> &'a JobParameters {
        self.parameters
    }

    /// Borrows the cooperative stop token.
    #[must_use]
    pub const fn stop_token(&self) -> &'a StopToken {
        self.stop
    }
}

/// A dynamically dispatched job lifecycle listener.
pub trait JobExecutionListener: Send + Sync {
    /// Runs before the job becomes `STARTED`.
    fn before_job<'a>(
        &'a self,
        context: ListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>>;

    /// Runs after the nested step has a provisional outcome and before the job
    /// receives its final status.
    fn after_job<'a>(
        &'a self,
        context: ListenerContext<'a>,
        outcome: TaskletExecutionOutcome,
    ) -> BoxFuture<'a, Result<(), ListenerError>>;
}

/// A dynamically dispatched step lifecycle listener.
pub trait StepExecutionListener: Send + Sync {
    /// Runs before the step becomes `STARTED`.
    fn before_step<'a>(
        &'a self,
        context: ListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>>;

    /// Runs after tasklet work has a provisional outcome and before the step
    /// receives its final status.
    fn after_step<'a>(
        &'a self,
        context: ListenerContext<'a>,
        outcome: TaskletExecutionOutcome,
    ) -> BoxFuture<'a, Result<(), ListenerError>>;
}

/// A value-redacted listener failure.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ListenerError;

impl ListenerError {
    /// Constructs a classified listener failure.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Classifies an arbitrary user error without retaining its payload.
    #[must_use]
    pub fn from_error(error: impl Error + Send + Sync + 'static) -> Self {
        drop(error);
        Self
    }
}

impl fmt::Display for ListenerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("the execution listener failed")
    }
}

impl Error for ListenerError {}

/// The listener callback boundary where a failure occurred.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum ListenerPhase {
    /// A job before-listener.
    BeforeJob,
    /// A step before-listener.
    BeforeStep,
    /// A step after-listener.
    AfterStep,
    /// A job after-listener.
    AfterJob,
}

/// Stable classification of a listener boundary failure.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum ListenerFailureKind {
    /// The listener returned [`ListenerError`].
    Error,
    /// The listener panicked before or while its future was polled.
    Panic,
}

/// One value-redacted listener failure retained by a launch report.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ListenerFailure {
    phase: ListenerPhase,
    registration_index: usize,
    kind: ListenerFailureKind,
    summary: FailureSummary,
}

impl ListenerFailure {
    pub(crate) const fn new(
        phase: ListenerPhase,
        registration_index: usize,
        kind: ListenerFailureKind,
        summary: FailureSummary,
    ) -> Self {
        Self {
            phase,
            registration_index,
            kind,
            summary,
        }
    }

    /// Returns the callback phase.
    #[must_use]
    pub const fn phase(self) -> ListenerPhase {
        self.phase
    }

    /// Returns the zero-based listener registration index.
    #[must_use]
    pub const fn registration_index(self) -> usize {
        self.registration_index
    }

    /// Returns whether the boundary returned an error or panicked.
    #[must_use]
    pub const fn kind(self) -> ListenerFailureKind {
        self.kind
    }

    /// Returns the redacted failure category and opaque ID.
    #[must_use]
    pub const fn summary(self) -> FailureSummary {
        self.summary
    }
}
