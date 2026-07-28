use std::collections::VecDeque;
use std::time::Duration;

/// A finite backoff plan that never sleeps.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ControlledBackoff {
    initial: Vec<Duration>,
    remaining: VecDeque<Duration>,
    requested: usize,
}

impl ControlledBackoff {
    /// Creates a plan from the exact delays a test wants to observe.
    #[must_use]
    pub fn new(delays: impl IntoIterator<Item = Duration>) -> Self {
        let initial = delays.into_iter().collect::<Vec<_>>();
        Self {
            remaining: initial.iter().copied().collect(),
            initial,
            requested: 0,
        }
    }

    /// Returns the next planned delay without waiting.
    pub fn next_delay(&mut self) -> Option<Duration> {
        let delay = self.remaining.pop_front()?;
        self.requested += 1;
        Some(delay)
    }

    /// Returns how many delays have been requested.
    #[must_use]
    pub const fn requested(&self) -> usize {
        self.requested
    }

    /// Restores the original plan.
    pub fn reset(&mut self) {
        self.remaining = self.initial.iter().copied().collect();
        self.requested = 0;
    }
}
