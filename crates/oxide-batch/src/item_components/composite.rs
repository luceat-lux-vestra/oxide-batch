//! Composite and delegating reader/processor/writer components (#146).
//!
//! Every type here is a monomorphized decorator: no `Boxed*` erasure is
//! introduced by this module, so a pipeline built entirely from these types
//! keeps the ADR-0008 zero-per-item-allocation property. Heterogeneous
//! delegates remain possible by naming `BoxedReader`/`BoxedProcessor`/
//! `BoxedWriter` as the delegate type parameter, at the same erasure boundary
//! ADR-0008 already accepts.
//!
//! # Composition semantics (Gate E)
//!
//! A wrapper's advertised capability is the meet (intersection) of its
//! delegates', never their union:
//!
//! - **ordering** — [`CompositeReader`] and [`FanOutWriter`] preserve each
//!   delegate's relative item order; if any delegate is order-sensitive, so
//!   is the composite.
//! - **restartability** — none of these types add durable state; the
//!   composite is restartable only if every delegate it holds is (typically
//!   via a separately registered [`crate::ItemStream`], never hidden by this
//!   wrapper — see the module-level docs on state below).
//! - **checkpoint/state** — a delegate that implements [`crate::ItemStream`]
//!   is registered with the owning [`crate::ChunkStep`] independently
//!   (`with_item_stream`), under its own namespace; these composite types
//!   never swallow or proxy that registration, so no delegate's durable state
//!   is hidden and no two delegates' namespaces can collide through this
//!   wrapper.
//! - **thread safety** — [`ChainProcessor`] and [`FanOutWriter`] are
//!   `Send + Sync` exactly when every delegate is; no synchronization is
//!   added here (contrast [`crate::item_components::sync`]).
//! - **error classification** — a delegate's [`ReaderError`]/
//!   [`ProcessorError`]/[`WriterError`] is returned unchanged; these wrappers
//!   never reclassify it.
//! - **close ordering** — not applicable to these types directly (they own no
//!   `ItemStream` themselves); see the module docs above.
//! - **stop propagation** — a [`ReadOutcome::Stopped`], [`ProcessOutcome::Stopped`],
//!   or [`WriteOutcome::Stopped`] observed from any delegate stops the
//!   composite immediately and is returned as-is; later delegates in the same
//!   call are never invoked.

use crate::{
    BusinessTransaction, ItemProcessor, ItemReader, ItemWriter, ProcessContext, ProcessOutcome,
    ProcessorError, ReadContext, ReadOutcome, ReaderError, WriteContext, WriteOutcome, WriterError,
};

/// Reads from an ordered sequence of homogeneous delegate readers, advancing
/// to the next delegate only once the current one reports
/// [`ReadOutcome::EndOfInput`].
///
/// A [`ReadOutcome::Stopped`] or [`ReaderError`] from any delegate is
/// returned immediately; later delegates are not consulted for that call.
/// The composite's position (which delegate is "current") is never advanced
/// on a failing call, so a retry -- the framework's fault-retry contract
/// re-invokes the same reader instance without rewinding it, exactly as
/// [`crate::item_components::aggregate`]'s module docs describe -- resumes
/// at the same delegate rather than skipping to the next one; see
/// `composite_reader_retry_after_failure_resumes_the_same_delegate` in the
/// evidence file below.
///
/// # Contract
///
/// - **Input/output**: `I`, same as every delegate `R`.
/// - **State/checkpoint**: the current-delegate index is in-memory only, not
///   restartable by itself; a restartable composite pairs each delegate with
///   its own [`crate::ItemStream`] and registers them independently (see the
///   module docs).
/// - **Ordering**: concatenates delegates in declared order; order-sensitive
///   if any delegate is.
/// - **Thread safety**: used exclusively (`&mut self`) like every reader.
/// - **Bounded resource**: none beyond the delegates' own.
/// - **Cancellation**: propagated from whichever delegate observes it.
/// - **Close**: nothing to close.
/// - **Malformed input**: propagates the failing delegate's [`ReaderError`]
///   unchanged.
/// - **Support tier**: first-party.
/// - **Evidence**: `crates/oxide-batch-test/tests/item_components_composite.rs`.
pub struct CompositeReader<R> {
    delegates: Vec<R>,
    current: usize,
}

impl<R> CompositeReader<R> {
    /// Builds a composite over delegates read in the given order.
    #[must_use]
    pub fn new(delegates: Vec<R>) -> Self {
        Self {
            delegates,
            current: 0,
        }
    }
}

impl<I, R> ItemReader<I> for CompositeReader<R>
where
    I: 'static,
    R: ItemReader<I>,
{
    async fn read(&mut self, context: ReadContext<'_>) -> Result<ReadOutcome<I>, ReaderError> {
        while let Some(delegate) = self.delegates.get_mut(self.current) {
            match delegate.read(context).await? {
                ReadOutcome::Item(item) => return Ok(ReadOutcome::Item(item)),
                ReadOutcome::Stopped => return Ok(ReadOutcome::Stopped),
                ReadOutcome::EndOfInput => self.current += 1,
            }
        }
        Ok(ReadOutcome::EndOfInput)
    }
}

/// Chains two [`crate::ItemProcessor`]s: `P1`'s output feeds `P2`'s input.
///
/// A [`ProcessOutcome::Filtered`] or [`ProcessOutcome::Stopped`] from `P1`
/// short-circuits without invoking `P2`. Nest another `ChainProcessor` as
/// `P1` or `P2` to compose more than two stages.
///
/// # Contract
///
/// - **Input/output**: `I -> O` via the intermediate type `M`.
/// - **State/checkpoint**: stateless (assuming both delegates are).
/// - **Ordering**: preserves order; filters/stops exactly when either stage
///   does.
/// - **Thread safety**: `Send + Sync` whenever `P1` and `P2` are.
/// - **Reentrancy**: fully reentrant when both delegates are.
/// - **Transaction/delivery**: not applicable.
/// - **Bounded resource**: none beyond the delegates' own.
/// - **Cancellation**: the call-scoped stop token is shared by both stages;
///   `P1`'s own `Stopped` outcome takes priority over invoking `P2`.
/// - **Close**: nothing to close.
/// - **Error classification**: whichever delegate fails, its
///   [`ProcessorError`] is returned unchanged.
/// - **Support tier**: first-party.
/// - **Evidence**: `crates/oxide-batch-test/tests/item_components_composite.rs`.
pub struct ChainProcessor<P1, P2, M> {
    first: P1,
    second: P2,
    _marker: std::marker::PhantomData<fn(M) -> M>,
}

impl<P1, P2, M> ChainProcessor<P1, P2, M> {
    /// Chains `first` into `second`.
    ///
    /// `M` (the intermediate item type `first` produces and `second`
    /// consumes) is usually inferred from context; annotate it explicitly
    /// (`ChainProcessor::<_, _, Mid>::new(..)`) if inference cannot pick it
    /// up from how the result is used.
    #[must_use]
    pub const fn new(first: P1, second: P2) -> Self {
        Self {
            first,
            second,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<I, M, O, P1, P2> ItemProcessor<I, O> for ChainProcessor<P1, P2, M>
where
    I: Sync,
    M: Send + Sync + 'static,
    O: 'static,
    P1: ItemProcessor<I, M>,
    P2: ItemProcessor<M, O>,
{
    async fn process(
        &self,
        item: &I,
        context: ProcessContext<'_>,
    ) -> Result<ProcessOutcome<O>, ProcessorError> {
        match self.first.process(item, context).await? {
            ProcessOutcome::Item(mid) => self.second.process(&mid, context).await,
            ProcessOutcome::Filtered => Ok(ProcessOutcome::Filtered),
            ProcessOutcome::Stopped => Ok(ProcessOutcome::Stopped),
        }
    }
}

/// Fans a chunk out to an ordered sequence of homogeneous delegate writers.
///
/// When the call is enlisted in a business transaction, the single
/// `&mut dyn BusinessTransaction` is reborrowed sequentially, once per
/// delegate: no two delegates ever hold it simultaneously, no delegate's
/// reference escapes its own call, and this wrapper never opens a second
/// transaction or connection (the enlisted [`WriteContext`] transaction
/// model, unchanged).
///
/// A [`WriteOutcome::Stopped`] or [`WriterError`] from any delegate stops the
/// fan-out immediately; later delegates in that call are not invoked.
///
/// # Contract
///
/// - **Input/output**: `[O]`, same as every delegate `W`.
/// - **State/checkpoint**: stateless.
/// - **Ordering**: each delegate receives the full batch in its original
///   order; order-sensitive if any delegate is.
/// - **Thread safety**: `Send + Sync` whenever `W` is; requires `O: Sync`
///   because the borrowed batch is held across each delegate's `await`
///   (ADR-0008's generic-composite bound).
/// - **Reentrancy**: fully reentrant when every delegate is.
/// - **Transaction/delivery**: never claims a stronger mode than every
///   delegate supports; see the reborrow guarantee above.
/// - **Bounded resource**: none beyond the delegates' own.
/// - **Cancellation**: checked once before the first delegate, and again via
///   each delegate's own outcome.
/// - **Close**: nothing to close.
/// - **Error classification**: whichever delegate fails, its [`WriterError`]
///   is returned unchanged.
/// - **Support tier**: first-party.
/// - **Evidence**: `crates/oxide-batch-test/tests/item_components_composite.rs`.
pub struct FanOutWriter<W> {
    delegates: Vec<W>,
}

impl<W> FanOutWriter<W> {
    /// Builds a fan-out writer over delegates invoked in the given order.
    #[must_use]
    pub fn new(delegates: Vec<W>) -> Self {
        Self { delegates }
    }
}

impl<O, W> ItemWriter<O> for FanOutWriter<W>
where
    O: Sync,
    W: ItemWriter<O>,
{
    async fn write<'a>(
        &'a self,
        items: &'a [O],
        mut context: WriteContext<'a>,
    ) -> Result<WriteOutcome, WriterError> {
        let stop = context.stop_token();
        if stop.is_stop_requested() {
            return Ok(WriteOutcome::Stopped);
        }
        let enlisted = context.is_enlisted();
        for delegate in &self.delegates {
            let delegate_context = if enlisted {
                let transaction: &mut dyn BusinessTransaction =
                    context.transaction().ok_or_else(WriterError::new)?;
                WriteContext::enlisted(stop, transaction)
            } else {
                WriteContext::non_transactional(stop)
            };
            match delegate.write(items, delegate_context).await? {
                WriteOutcome::Written => {}
                WriteOutcome::Stopped => return Ok(WriteOutcome::Stopped),
            }
        }
        Ok(WriteOutcome::Written)
    }
}
