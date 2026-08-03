//! The cancellable backoff waiting contract.

use std::time::Duration;

use crate::{BoxFuture, StopToken};

/// The result of one cancellable backoff wait.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum BackoffOutcome {
    /// The complete delay elapsed.
    Elapsed,
    /// Cooperative stop cancelled the wait.
    Stopped,
}

/// An injected monotonic, cancellable delay source.
///
/// Implementations must not detach a task or timer, must observe the supplied
/// [`StopToken`] while waiting, and must not consult wall-clock time.
pub trait BackoffSleeper: Send + Sync {
    /// Waits for `delay` unless cooperative stop is observed first.
    fn sleep<'a>(&'a self, delay: Duration, stop: &'a StopToken) -> BoxFuture<'a, BackoffOutcome>;
}
