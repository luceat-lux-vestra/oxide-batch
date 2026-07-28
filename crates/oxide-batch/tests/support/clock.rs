use std::error::Error;
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime};

use oxide_batch::Clock;

/// A cloneable clock advanced explicitly by a test.
#[derive(Clone, Debug)]
pub struct ManualClock {
    current: Arc<Mutex<SystemTime>>,
}

impl ManualClock {
    /// Creates a clock at an explicit instant.
    #[must_use]
    pub fn new(initial: SystemTime) -> Self {
        Self {
            current: Arc::new(Mutex::new(initial)),
        }
    }

    /// Returns the current test instant.
    #[must_use]
    pub fn now(&self) -> SystemTime {
        *self.current.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Moves the clock to an explicit instant.
    pub fn set(&self, instant: SystemTime) {
        *self.current.lock().unwrap_or_else(PoisonError::into_inner) = instant;
    }

    /// Advances the clock without sleeping.
    ///
    /// # Errors
    ///
    /// Returns [`ManualClockError::Overflow`] if the instant cannot represent
    /// the requested advance.
    pub fn advance(&self, duration: Duration) -> Result<SystemTime, ManualClockError> {
        let mut current = self.current.lock().unwrap_or_else(PoisonError::into_inner);
        let advanced = current
            .checked_add(duration)
            .ok_or(ManualClockError::Overflow)?;
        *current = advanced;
        Ok(advanced)
    }
}

impl Clock for ManualClock {
    fn now(&self) -> SystemTime {
        Self::now(self)
    }
}

/// Failure to advance a [`ManualClock`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManualClockError {
    /// The requested instant is outside [`SystemTime`]'s range.
    Overflow,
}

impl fmt::Display for ManualClockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("manual clock advance overflowed")
    }
}

impl Error for ManualClockError {}
