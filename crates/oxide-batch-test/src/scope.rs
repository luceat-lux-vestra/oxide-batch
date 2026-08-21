//! Typed fixture/context factories for scoped component tests (`TEST-SCOPE-001`).

use std::num::NonZeroU64;
use std::time::SystemTime;

use oxide_batch::{
    ComponentStateEnvelope, ProcessContext, ReadContext, StopSource, StopToken, StreamCloseContext,
    StreamOpenContext, StreamRuntimeOutcome, StreamUpdateContext, WriteContext,
};

use crate::{DeterministicIds, ManualClock};

/// A typed, production-compatible call scope for one component under test.
///
/// `ComponentFixture` owns a [`StopSource`]/[`StopToken`] pair, a
/// [`ManualClock`], and [`DeterministicIds`], and hands out the exact public
/// call-context types production `ItemReader`, `ItemProcessor`, `ItemWriter`,
/// and `ItemStream` implementations receive at runtime. It never constructs a
/// private/internal type: every method here delegates to a production public
/// constructor.
///
/// ```
/// use oxide_batch::{ItemReader, ReadOutcome};
/// use oxide_batch_test::ComponentFixture;
///
/// struct Counter(u64);
///
/// impl ItemReader<u64> for Counter {
///     async fn read(
///         &mut self,
///         _context: oxide_batch::ReadContext<'_>,
///     ) -> Result<ReadOutcome<u64>, oxide_batch::ReaderError> {
///         self.0 += 1;
///         Ok(ReadOutcome::Item(self.0))
///     }
/// }
///
/// # futures_executor::block_on(async {
/// let fixture = ComponentFixture::new();
/// let mut counter = Counter(0);
/// let outcome = counter.read(fixture.read_context()).await?;
/// assert_eq!(outcome, ReadOutcome::Item(1));
/// # Ok::<(), oxide_batch::ReaderError>(())
/// # })?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct ComponentFixture {
    stop_source: StopSource,
    stop_token: StopToken,
    clock: ManualClock,
    ids: DeterministicIds,
}

impl ComponentFixture {
    /// Builds a fixture with an unrequested stop, deterministic clock started
    /// at the Unix epoch, and a deterministic ID sequence starting at `1`.
    #[must_use]
    pub fn new() -> Self {
        let (stop_source, stop_token) = StopSource::new();
        Self {
            stop_source,
            stop_token,
            clock: ManualClock::new(SystemTime::UNIX_EPOCH),
            ids: DeterministicIds::new(NonZeroU64::MIN),
        }
    }

    /// Borrows the fixture's cooperative stop token.
    #[must_use]
    pub const fn stop_token(&self) -> &StopToken {
        &self.stop_token
    }

    /// Requests cooperative stop for every context this fixture issues from
    /// this point on, and for every call already holding a borrowed token.
    pub fn request_stop(&self) {
        self.stop_source.request_stop();
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

    /// Constructs a reader call scope over this fixture's stop token.
    #[must_use]
    pub const fn read_context(&self) -> ReadContext<'_> {
        ReadContext::new(&self.stop_token)
    }

    /// Constructs a processor call scope over this fixture's stop token.
    #[must_use]
    pub const fn process_context(&self) -> ProcessContext<'_> {
        ProcessContext::new(&self.stop_token)
    }

    /// Constructs a non-enlisted writer call scope over this fixture's stop
    /// token.
    #[must_use]
    pub const fn write_context(&self) -> WriteContext<'_> {
        WriteContext::non_transactional(&self.stop_token)
    }

    /// Constructs a stream-open call scope, optionally restoring an inherited
    /// committed envelope.
    #[must_use]
    pub const fn stream_open_context<'a>(
        &'a self,
        inherited: Option<&'a ComponentStateEnvelope>,
    ) -> StreamOpenContext<'a> {
        StreamOpenContext::new(inherited, &self.stop_token)
    }

    /// Constructs a stream-update call scope over this fixture's stop token.
    #[must_use]
    pub const fn stream_update_context(&self) -> StreamUpdateContext<'_> {
        StreamUpdateContext::new(&self.stop_token)
    }

    /// Constructs a stream-close call scope with an explicit terminal
    /// outcome.
    #[must_use]
    pub const fn stream_close_context(
        &self,
        outcome: StreamRuntimeOutcome,
    ) -> StreamCloseContext<'_> {
        StreamCloseContext::new(&self.stop_token, outcome)
    }
}

impl Default for ComponentFixture {
    fn default() -> Self {
        Self::new()
    }
}
