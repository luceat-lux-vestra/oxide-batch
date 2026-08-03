//! Delegating components: the shapes M6 actually has to ship.
//!
//! M6's catalogue is mostly decorators and composites — classifiers,
//! delegates, validators, filters, peek, multi-resource, thread-safety
//! wrappers. A contract that is pleasant for leaf components but awkward for
//! these would fail in practice, so the ergonomics review has to write them
//! rather than assume them.
//!
//! Four findings are recorded here in code:
//!
//! 1. A delegating component ties its lifetimes explicitly. It is still
//!    `async fn`, with no future type and no `Box::pin`, but the receiver, the
//!    item, and the call scope share one `'a` because the inner call unifies
//!    them.
//! 2. A composite is generic over the components it wraps, so wrapping costs
//!    no dispatch. A decorated pipeline stays monomorphized.
//! 3. `WriteContext` is not `Copy`, because it carries
//!    `&mut dyn BusinessTransaction`. A writer that fans out to several
//!    delegates must therefore reborrow the enlisted transaction for each
//!    call, which also means it hands them the transaction one at a time. That
//!    is the correct semantics — two writers cannot hold the same transaction
//!    concurrently — but it is a constraint composite authors must know.
//! 4. A *generic* composite states bounds a leaf never has to. The minimum,
//!    established by removing each one until the compiler objected:
//!
//!    | Composite | Needs |
//!    | --- | --- |
//!    | reader decorator | `I: 'static` |
//!    | processor decorator | `I: Sync`, `O: 'static` |
//!    | writer composite | `I: Sync` |
//!
//!    The rule behind the table: a type that appears in the *returned* value
//!    needs `'static`, because the opaque future must outlive the call
//!    lifetime for every choice of it; a type passed *by reference* needs
//!    `Sync`, because `&T: Send` requires it. Leaf components with concrete
//!    item types satisfy both silently. The diagnostics say exactly this, so
//!    the cost is one line in a `where` clause, not a debugging session.

use oxide_batch::{
    ProcessContext, ProcessOutcome, ProcessorError, ReadContext, ReadOutcome, ReaderError,
    WriteContext, WriteOutcome, WriterError,
};

use crate::contract::{ItemProcessor, ItemReader, ItemWriter};

/// A reader decorator that counts the items its delegate produces.
///
/// The `peek`/observer shape.
#[derive(Clone, Copy, Debug)]
pub struct CountingReader<R> {
    inner: R,
    observed: u64,
}

impl<R> CountingReader<R> {
    /// Wraps a reader.
    pub const fn new(inner: R) -> Self {
        Self { inner, observed: 0 }
    }

    /// Returns how many items passed through.
    pub const fn observed(&self) -> u64 {
        self.observed
    }
}

impl<I: 'static, R: ItemReader<I>> ItemReader<I> for CountingReader<R> {
    async fn read<'a>(
        &'a mut self,
        context: ReadContext<'a>,
    ) -> Result<ReadOutcome<I>, ReaderError> {
        let outcome = self.inner.read(context).await?;
        if matches!(outcome, ReadOutcome::Item(_)) {
            self.observed += 1;
        }
        Ok(outcome)
    }
}

/// A processor decorator that filters before delegating.
///
/// The `filter`/`validator` shape.
#[derive(Clone, Copy, Debug)]
pub struct FilteringProcessor<P, F> {
    inner: P,
    accepts: F,
}

impl<P, F> FilteringProcessor<P, F> {
    /// Wraps a processor with an acceptance predicate.
    pub const fn new(inner: P, accepts: F) -> Self {
        Self { inner, accepts }
    }
}

impl<I, O, P, F> ItemProcessor<I, O> for FilteringProcessor<P, F>
where
    I: Sync,
    O: 'static,
    P: ItemProcessor<I, O>,
    F: Fn(&I) -> bool + Send + Sync,
{
    async fn process<'a>(
        &'a self,
        item: &'a I,
        context: ProcessContext<'a>,
    ) -> Result<ProcessOutcome<O>, ProcessorError> {
        if !(self.accepts)(item) {
            return Ok(ProcessOutcome::Filtered);
        }
        self.inner.process(item, context).await
    }
}

/// A writer composite that writes each batch to two delegates in order.
///
/// The `multi-resource` shape. When the chunk is enlisted, the transaction is
/// reborrowed for each delegate in turn: both writes land in the one chunk
/// transaction, and neither delegate can hold it while the other runs.
#[derive(Clone, Copy, Debug)]
pub struct FanOutWriter<A, B> {
    primary: A,
    secondary: B,
}

impl<A, B> FanOutWriter<A, B> {
    /// Composes two writers.
    pub const fn new(primary: A, secondary: B) -> Self {
        Self { primary, secondary }
    }
}

impl<I, A, B> ItemWriter<I> for FanOutWriter<A, B>
where
    I: Sync,
    A: ItemWriter<I>,
    B: ItemWriter<I>,
{
    async fn write<'a>(
        &'a self,
        items: &'a [I],
        mut context: WriteContext<'a>,
    ) -> Result<WriteOutcome, WriterError> {
        let stop = context.stop_token();

        let first = match context.transaction() {
            Some(transaction) => {
                self.primary
                    .write(items, WriteContext::enlisted(stop, transaction))
                    .await?
            }
            None => {
                self.primary
                    .write(items, WriteContext::non_transactional(stop))
                    .await?
            }
        };
        if first != WriteOutcome::Written {
            return Ok(first);
        }

        match context.transaction() {
            Some(transaction) => {
                self.secondary
                    .write(items, WriteContext::enlisted(stop, transaction))
                    .await
            }
            None => {
                self.secondary
                    .write(items, WriteContext::non_transactional(stop))
                    .await
            }
        }
    }
}
