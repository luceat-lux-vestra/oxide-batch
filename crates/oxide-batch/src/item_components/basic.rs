//! Basic iterator/list-backed item components (#146).
//!
//! These are minimal, ephemeral, in-memory components: they hold no durable
//! state of their own and therefore declare no restart capability beyond what
//! a paired [`crate::ItemStream`] would add (none is provided here). They
//! exist to make the rest of the composition catalog practically usable and
//! testable without every example standing up a resource-backed adapter.

use crate::{
    ProcessContext, ProcessOutcome, ProcessorError, ReadContext, ReadOutcome, ReaderError,
    WriteContext, WriteOutcome, WriterError,
};

/// A basic [`crate::ItemReader`] over any `Send` iterator.
///
/// # Contract
///
/// - **Input/output**: produces `It::Item`; no format/version (in-memory).
/// - **State/checkpoint**: none; not restartable. A restart reconstructs this
///   reader from its initial iterator and therefore re-reads from the start,
///   exactly like any other component with no paired [`crate::ItemStream`].
/// - **Ordering**: preserves the iterator's own order.
/// - **Thread safety**: `Send`; used exclusively (`&mut self`) like every
///   `ItemReader`, so no additional synchronization is meaningful.
/// - **Reentrancy**: not reentrant (owns mutable iteration state).
/// - **Transaction/delivery**: not applicable (a reader never enlists).
/// - **Bounded resource**: bounded by the wrapped iterator; this type adds no
///   buffering.
/// - **Cancellation**: cooperative stop is observed by the driving
///   [`crate::ChunkStep`] between calls, matching every other reader; this
///   type does not check the stop token itself because it never blocks.
/// - **Close**: nothing to close.
/// - **Sensitive diagnostics**: none; carries no sensitive-data declaration
///   of its own.
/// - **Malformed input**: not applicable; the iterator cannot fail.
/// - **Support tier**: first-party, reference-only (not for production
///   durable pipelines without a paired stream).
/// - **Evidence**: `crates/oxide-batch-test/tests/item_components_basic.rs`.
pub struct IterReader<It>(It);

impl<It> IterReader<It> {
    /// Wraps any `IntoIterator` as a basic in-memory reader.
    pub fn new<Src: IntoIterator<IntoIter = It>>(source: Src) -> Self {
        Self(source.into_iter())
    }
}

impl<I, It> crate::ItemReader<I> for IterReader<It>
where
    I: 'static,
    It: Iterator<Item = I> + Send,
{
    async fn read(&mut self, _context: ReadContext<'_>) -> Result<ReadOutcome<I>, ReaderError> {
        Ok(self
            .0
            .next()
            .map_or(ReadOutcome::EndOfInput, ReadOutcome::Item))
    }
}

/// A basic pass-through [`crate::ItemProcessor`] that returns its input
/// unchanged.
///
/// # Contract
///
/// - **Input/output**: `I -> I`, unchanged.
/// - **State/checkpoint**: stateless.
/// - **Ordering**: preserves order; never filters or stops.
/// - **Thread safety**: `Send + Sync`; safe under concurrent shared calls
///   (no interior state).
/// - **Reentrancy**: fully reentrant.
/// - **Transaction/delivery**: not applicable.
/// - **Bounded resource**: none; clones one item per call.
/// - **Cancellation**: honors the call-scoped stop token.
/// - **Close**: nothing to close.
/// - **Sensitive diagnostics**: none.
/// - **Malformed input**: not applicable; cannot fail.
/// - **Support tier**: first-party.
/// - **Evidence**: `crates/oxide-batch-test/tests/item_components_basic.rs`.
pub struct IdentityProcessor;

impl<I> crate::ItemProcessor<I, I> for IdentityProcessor
where
    I: Clone + Send + Sync,
{
    async fn process(
        &self,
        item: &I,
        context: ProcessContext<'_>,
    ) -> Result<ProcessOutcome<I>, ProcessorError> {
        if context.stop_token().is_stop_requested() {
            return Ok(ProcessOutcome::Stopped);
        }
        Ok(ProcessOutcome::Item(item.clone()))
    }
}

/// A basic [`crate::ItemWriter`] that accepts and discards every item.
///
/// Useful as a delegate placeholder in composite/classifier pipelines, or for
/// steps whose business effect is entirely inside the processor/reader.
///
/// # Contract
///
/// - **Input/output**: accepts any `O`; produces nothing.
/// - **State/checkpoint**: stateless.
/// - **Ordering**: not order-sensitive (discards).
/// - **Thread safety**: `Send + Sync`.
/// - **Reentrancy**: fully reentrant.
/// - **Transaction/delivery**: never enlists; ignores any supplied
///   transaction. Safe to compose under [`WriteContext::enlisted`] because it
///   performs no business effect to roll back.
/// - **Bounded resource**: none.
/// - **Cancellation**: honors the call-scoped stop token.
/// - **Close**: nothing to close.
/// - **Sensitive diagnostics**: none; never inspects item contents.
/// - **Malformed input**: not applicable; cannot fail.
/// - **Support tier**: first-party.
/// - **Evidence**: `crates/oxide-batch-test/tests/item_components_basic.rs`.
pub struct NoopWriter;

impl<O> crate::ItemWriter<O> for NoopWriter
where
    O: Send + Sync,
{
    async fn write(
        &self,
        _items: &[O],
        context: WriteContext<'_>,
    ) -> Result<WriteOutcome, WriterError> {
        if context.stop_token().is_stop_requested() {
            return Ok(WriteOutcome::Stopped);
        }
        Ok(WriteOutcome::Written)
    }
}
