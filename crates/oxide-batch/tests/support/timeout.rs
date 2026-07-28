use std::error::Error;
use std::fmt;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::Duration;

const MAX_TEST_TIMEOUT: Duration = Duration::from_secs(30);

/// An explicit upper bound for a test operation that genuinely needs waiting.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BoundedTimeout(Duration);

impl BoundedTimeout {
    /// Validates a nonzero timeout no longer than 30 seconds.
    ///
    /// # Errors
    ///
    /// Returns [`BoundedTimeoutError`] for zero or an excessive duration.
    pub fn new(duration: Duration) -> Result<Self, BoundedTimeoutError> {
        if duration.is_zero() {
            return Err(BoundedTimeoutError::Zero);
        }
        if duration > MAX_TEST_TIMEOUT {
            return Err(BoundedTimeoutError::ExceedsMaximum {
                maximum: MAX_TEST_TIMEOUT,
            });
        }
        Ok(Self(duration))
    }

    /// Returns the validated duration.
    #[must_use]
    pub const fn duration(self) -> Duration {
        self.0
    }

    /// Receives a scheduled event with this explicit upper bound.
    ///
    /// This helper is reserved for tests whose subject is cross-thread
    /// scheduling; ordinary tests should use deterministic event capture.
    pub fn receive<T>(self, receiver: &Receiver<T>) -> Result<T, RecvTimeoutError> {
        receiver.recv_timeout(self.0)
    }
}

/// Invalid bounded-timeout configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BoundedTimeoutError {
    /// A zero timeout cannot observe a scheduled operation.
    Zero,
    /// The requested duration exceeded the test-suite ceiling.
    ExceedsMaximum {
        /// The largest timeout accepted by this helper.
        maximum: Duration,
    },
}

impl fmt::Display for BoundedTimeoutError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Zero => formatter.write_str("bounded test timeout must be nonzero"),
            Self::ExceedsMaximum { maximum } => {
                write!(formatter, "bounded test timeout exceeds {maximum:?}")
            }
        }
    }
}

impl Error for BoundedTimeoutError {}
