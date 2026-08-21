//! A deterministic [`Clock`] for application tests.

use std::error::Error;
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime};

use oxide_batch::Clock;

/// A cloneable clock that only advances when a test tells it to.
///
/// `ManualClock` implements the framework's own [`Clock`] port, so it plugs
/// into [`crate::TestJob`], [`crate::TestStep`], and any production API that
/// accepts `&dyn Clock` without a second, test-only clock abstraction. Reads
/// are deterministic for a given call order and never consult wall-clock
/// time.
///
/// ```
/// use oxide_batch::Clock;
/// use oxide_batch_test::ManualClock;
/// use std::time::{Duration, SystemTime};
///
/// let epoch = SystemTime::UNIX_EPOCH;
/// let clock = ManualClock::new(epoch);
/// assert_eq!(clock.now(), epoch);
///
/// let advanced = clock.advance(Duration::from_secs(60))?;
/// assert_eq!(advanced, epoch + Duration::from_secs(60));
/// assert_eq!(Clock::now(&clock), advanced);
/// # Ok::<(), oxide_batch_test::ManualClockError>(())
/// ```
#[derive(Clone, Debug)]
pub struct ManualClock {
    current: Arc<Mutex<SystemTime>>,
}

impl ManualClock {
    /// Creates a clock at an explicit initial instant.
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

    /// Advances the clock by `duration` without sleeping.
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
#[non_exhaustive]
pub enum ManualClockError {
    /// The requested instant is outside [`SystemTime`]'s representable range.
    Overflow,
}

impl fmt::Display for ManualClockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("manual clock advance overflowed")
    }
}

impl Error for ManualClockError {}
