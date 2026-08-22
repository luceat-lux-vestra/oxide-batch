//! Aggregate reader decorator (#146).
//!
//! [`AggregatingReader`] combines up to a bounded number of delegate items
//! into one logical output item. The bound reuses [`crate::ChunkSize`] --
//! the framework's existing nonzero, validated item-count type -- rather than
//! introducing a second bounded-count type for the same purpose.

use crate::{ChunkSize, ItemReader, ReadContext, ReadOutcome, ReaderError};

/// A reader decorator that aggregates up to a bounded number of delegate
/// items into one logical output item.
///
/// # Contract
///
/// - **Input/output**: `I -> O` via the supplied aggregation function, one
///   `O` per completed (or final partial) group of `I`.
/// - **State/checkpoint**: the in-flight buffer is in-memory only, exactly
///   like [`crate::item_components::PeekReader`]'s lookahead: it is never
///   reported to the framework as read progress until a full or final
///   partial group is returned from `read`, so it advances no checkpoint by
///   itself. A read failure preserves the buffer across the call (see
///   "Malformed input" below); only cooperative stop clears it, because a
///   stopped step attempt is not retried. A restart (a fresh process,
///   reconstructing this reader from scratch) resumes the delegate from its
///   own last committed position (via a paired [`crate::ItemStream`], if
///   any) and re-aggregates from there.
/// - **Restartability**: exactly the delegate's.
/// - **Ordering**: preserves delegate order within each aggregated group.
/// - **Thread safety**: used exclusively (`&mut self`) like every reader.
/// - **Bounded resource**: the buffer never exceeds `bound` items -- the
///   defining property of this component, proved by
///   [`crate::item_components`]'s aggregate tests, never left as the
///   default. There is no unbounded-accumulation mode.
/// - **Cancellation**: a delegate [`ReadOutcome::Stopped`] returns `Stopped`
///   immediately; any partially buffered group is discarded, not emitted.
/// - **Close**: nothing to close.
/// - **Malformed input**: propagates the delegate's [`ReaderError`]
///   unchanged without emitting a truncated aggregate. The in-flight buffer
///   is *not* cleared: the framework's fault-retry contract re-invokes this
///   same reader instance, from the same in-memory position, rather than
///   rewinding or reconstructing it ("replays the chunk from inputs it
///   already read, so a stateful reader never rewinds" -- see the facade's
///   fault-runtime documentation), so discarding already-accumulated
///   delegate items on a retryable failure would lose real, already-read
///   input. A retried call resumes accumulating from exactly where the
///   failed call left off; see
///   `aggregate_retry_after_failure_resumes_the_preserved_buffer` in the
///   evidence file below.
/// - **End of input**: a nonempty buffer at end of input is emitted once as
///   a final, possibly smaller-than-`bound` aggregate; the next call returns
///   [`ReadOutcome::EndOfInput`] and continues to do so.
/// - **Support tier**: first-party.
/// - **Evidence**: `crates/oxide-batch-test/tests/item_components_decorators.rs`.
pub struct AggregatingReader<I, R, F> {
    inner: R,
    bound: ChunkSize,
    aggregate: F,
    buffer: Vec<I>,
    exhausted: bool,
}

impl<I, R, F> AggregatingReader<I, R, F> {
    /// Wraps `inner`, combining up to `bound` items per call to `aggregate`.
    #[must_use]
    pub const fn new(inner: R, bound: ChunkSize, aggregate: F) -> Self {
        Self {
            inner,
            bound,
            aggregate,
            buffer: Vec::new(),
            exhausted: false,
        }
    }
}

impl<I, O, R, F> ItemReader<O> for AggregatingReader<I, R, F>
where
    I: Send,
    O: 'static,
    R: ItemReader<I>,
    F: FnMut(Vec<I>) -> O + Send,
{
    async fn read(&mut self, context: ReadContext<'_>) -> Result<ReadOutcome<O>, ReaderError> {
        if self.exhausted && self.buffer.is_empty() {
            return Ok(ReadOutcome::EndOfInput);
        }
        loop {
            match self.inner.read(context).await? {
                ReadOutcome::Item(item) => {
                    self.buffer.push(item);
                    if self.buffer.len() >= self.bound.get() as usize {
                        let group = std::mem::take(&mut self.buffer);
                        return Ok(ReadOutcome::Item((self.aggregate)(group)));
                    }
                }
                ReadOutcome::EndOfInput => {
                    self.exhausted = true;
                    return Ok(if self.buffer.is_empty() {
                        ReadOutcome::EndOfInput
                    } else {
                        let group = std::mem::take(&mut self.buffer);
                        ReadOutcome::Item((self.aggregate)(group))
                    });
                }
                ReadOutcome::Stopped => {
                    self.buffer.clear();
                    return Ok(ReadOutcome::Stopped);
                }
            }
        }
    }
}
