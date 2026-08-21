//! An isolated, in-process repository fixture (`TEST-REPO-001`, embedded).

use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::SystemTime;

use oxide_batch::{Clock, IdGenerator, InMemoryJobRepository};

use crate::{DeterministicIds, ManualClock};

/// A fresh, isolated in-process [`JobRepository`](oxide_batch::JobRepository)
/// fixture with its own deterministic clock and ID source.
///
/// Every [`EmbeddedRepository::new`] call constructs a repository with
/// private, unshared state: it never observes another fixture's job
/// instances, executions, or checkpoints. There is nothing durable to clean
/// up beyond dropping the value, which is the cheapest valid repository path
/// for application unit and single-step tests.
///
/// ```
/// use oxide_batch::Clock;
/// use oxide_batch_test::EmbeddedRepository;
/// use std::time::{Duration, SystemTime};
///
/// let first = EmbeddedRepository::new();
/// let second = EmbeddedRepository::new();
/// first.clock().advance(Duration::from_secs(1))?;
/// // Each fixture owns an independent clock: advancing one never moves the other.
/// assert_eq!(Clock::now(second.clock()), SystemTime::UNIX_EPOCH);
/// # Ok::<(), oxide_batch_test::ManualClockError>(())
/// ```
pub struct EmbeddedRepository {
    repository: InMemoryJobRepository,
    clock: ManualClock,
    ids: DeterministicIds,
}

impl EmbeddedRepository {
    /// Builds a fixture with a deterministic clock started at the Unix epoch
    /// and a deterministic ID sequence starting at `1`.
    #[must_use]
    pub fn new() -> Self {
        let clock = ManualClock::new(SystemTime::UNIX_EPOCH);
        let ids = DeterministicIds::new(NonZeroU64::MIN);
        Self::with_clock_and_ids(clock, ids)
    }

    /// Builds a fixture over an explicit deterministic clock and ID source.
    #[must_use]
    pub fn with_clock_and_ids(clock: ManualClock, ids: DeterministicIds) -> Self {
        let repository = InMemoryJobRepository::new(
            Arc::new(clock.clone()) as Arc<dyn Clock>,
            Arc::new(ids.clone()) as Arc<dyn IdGenerator>,
        );
        Self {
            repository,
            clock,
            ids,
        }
    }

    /// Borrows the isolated in-process repository.
    #[must_use]
    pub const fn repository(&self) -> &InMemoryJobRepository {
        &self.repository
    }

    /// Borrows the fixture's deterministic clock.
    #[must_use]
    pub const fn clock(&self) -> &ManualClock {
        &self.clock
    }

    /// Borrows the fixture's deterministic ID source.
    #[must_use]
    pub const fn ids(&self) -> &DeterministicIds {
        &self.ids
    }
}

impl Default for EmbeddedRepository {
    fn default() -> Self {
        Self::new()
    }
}
