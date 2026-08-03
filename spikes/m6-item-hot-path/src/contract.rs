//! The proposed public component contract, in the shape a superseding ADR
//! would publish it.
//!
//! There is exactly one trait per role. Implementors write a plain `async fn`
//! and never name a lifetime or a future type:
//!
//! ```ignore
//! impl ItemReader<Invoice> for CsvReader {
//!     async fn read(&mut self, cx: ReadContext<'_>) -> Result<ReadOutcome<Invoice>, ReaderError> {
//!         ..
//!     }
//! }
//! ```
//!
//! The declaration carries the explicit call lifetime and the `Send` bound
//! that the chunk driver needs. Tying the receiver and the call scope to one
//! `'a` mirrors the accepted ADR-0002 signature exactly, and it is what lets
//! the erased handle below satisfy the same trait without an item-type
//! `'static` bound.
//!
//! Erasure is a *type*, not a second trait. [`BoxedReader`] and its siblings
//! are concrete handles that are themselves `ItemReader`/`ItemProcessor`/
//! `ItemWriter`, so a registry, a plan, or a facade stores one named type and
//! the driver stays single-source. The dyn-compatible trait that makes this
//! work lives in a private module: it cannot be named, implemented, or
//! depended on from outside this crate, so its shape stays changeable.
//!
//! This is the `Box<dyn Iterator>: Iterator` arrangement, applied to
//! components.

use std::future::Future;

use oxide_batch::{
    ProcessContext, ProcessOutcome, ProcessorError, ReadContext, ReadOutcome, ReaderError,
    WriteContext, WriteOutcome, WriterError,
};

/// A stateful asynchronous item source.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not an OxideBatch item reader for `{I}`",
    label = "this component cannot read `{I}`",
    note = "implement `ItemReader<{I}>` with `async fn read(&mut self, context: ReadContext<'_>)`",
    note = "the returned future must be `Send`: do not hold a non-`Send` value across an await"
)]
pub trait ItemReader<I>: Send {
    /// Reads at most one item while borrowing the reader and call scope.
    fn read<'a>(
        &'a mut self,
        context: ReadContext<'a>,
    ) -> impl Future<Output = Result<ReadOutcome<I>, ReaderError>> + Send + 'a;
}

/// A shared asynchronous item transformer.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not an OxideBatch item processor from `{I}` to `{O}`",
    label = "this component cannot process `{I}` into `{O}`",
    note = "implement `ItemProcessor<{I}, {O}>` with `async fn process(&self, item: &{I}, context: ProcessContext<'_>)`",
    note = "a processor is shared across the chunk, so it takes `&self` and must be `Sync`"
)]
pub trait ItemProcessor<I, O>: Send + Sync {
    /// Processes one borrowed item.
    fn process<'a>(
        &'a self,
        item: &'a I,
        context: ProcessContext<'a>,
    ) -> impl Future<Output = Result<ProcessOutcome<O>, ProcessorError>> + Send + 'a;
}

/// A shared asynchronous batch writer.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not an OxideBatch item writer for `{I}`",
    label = "this component cannot write `{I}`",
    note = "implement `ItemWriter<{I}>` with `async fn write(&self, items: &[{I}], context: WriteContext<'_>)`",
    note = "a writer that delegates to another component must tie its lifetimes: `async fn write<'a>(&'a self, items: &'a [{I}], context: WriteContext<'a>)`"
)]
pub trait ItemWriter<I>: Send + Sync {
    /// Writes one borrowed, nonempty batch.
    ///
    /// A durable step supplies an enlisted transaction in `context`, borrowed
    /// for exactly this call.
    fn write<'a>(
        &'a self,
        items: &'a [I],
        context: WriteContext<'a>,
    ) -> impl Future<Output = Result<WriteOutcome, WriterError>> + Send + 'a;
}

/// The dyn-compatible mirror of the public contract.
///
/// Nothing here is exported. Its only implementors are the blanket impls
/// below, so no external crate can observe or depend on this shape, and the
/// single `Box::pin` per call is the only boxing in the system.
mod sealed {
    use oxide_batch::{
        BoxFuture, ProcessContext, ProcessOutcome, ProcessorError, ReadContext, ReadOutcome,
        ReaderError, WriteContext, WriteOutcome, WriterError,
    };

    use super::{ItemProcessor, ItemReader, ItemWriter};

    pub trait ReaderObject<I>: Send {
        fn read_boxed<'a>(
            &'a mut self,
            context: ReadContext<'a>,
        ) -> BoxFuture<'a, Result<ReadOutcome<I>, ReaderError>>;
    }

    impl<I, R: ItemReader<I>> ReaderObject<I> for R {
        fn read_boxed<'a>(
            &'a mut self,
            context: ReadContext<'a>,
        ) -> BoxFuture<'a, Result<ReadOutcome<I>, ReaderError>> {
            Box::pin(self.read(context))
        }
    }

    pub trait ProcessorObject<I, O>: Send + Sync {
        fn process_boxed<'a>(
            &'a self,
            item: &'a I,
            context: ProcessContext<'a>,
        ) -> BoxFuture<'a, Result<ProcessOutcome<O>, ProcessorError>>;
    }

    impl<I, O, P: ItemProcessor<I, O>> ProcessorObject<I, O> for P {
        fn process_boxed<'a>(
            &'a self,
            item: &'a I,
            context: ProcessContext<'a>,
        ) -> BoxFuture<'a, Result<ProcessOutcome<O>, ProcessorError>> {
            Box::pin(self.process(item, context))
        }
    }

    pub trait WriterObject<I>: Send + Sync {
        fn write_boxed<'a>(
            &'a self,
            items: &'a [I],
            context: WriteContext<'a>,
        ) -> BoxFuture<'a, Result<WriteOutcome, WriterError>>;
    }

    impl<I, W: ItemWriter<I>> WriterObject<I> for W {
        fn write_boxed<'a>(
            &'a self,
            items: &'a [I],
            context: WriteContext<'a>,
        ) -> BoxFuture<'a, Result<WriteOutcome, WriterError>> {
            Box::pin(self.write(items, context))
        }
    }
}

/// A reader of any concrete type, behind one dynamic dispatch.
///
/// Constructing one is the explicit, greppable point where the pipeline stops
/// being monomorphized and starts paying a boxed future per call.
pub struct BoxedReader<I>(Box<dyn sealed::ReaderObject<I>>);

impl<I> BoxedReader<I> {
    /// Erases a concrete reader.
    pub fn new<R: ItemReader<I> + 'static>(reader: R) -> Self {
        Self(Box::new(reader))
    }
}

impl<I> ItemReader<I> for BoxedReader<I> {
    fn read<'a>(
        &'a mut self,
        context: ReadContext<'a>,
    ) -> impl Future<Output = Result<ReadOutcome<I>, ReaderError>> + Send + 'a {
        self.0.read_boxed(context)
    }
}

/// A processor of any concrete type, behind one dynamic dispatch.
pub struct BoxedProcessor<I, O>(Box<dyn sealed::ProcessorObject<I, O>>);

impl<I, O> BoxedProcessor<I, O> {
    /// Erases a concrete processor.
    pub fn new<P: ItemProcessor<I, O> + 'static>(processor: P) -> Self {
        Self(Box::new(processor))
    }
}

impl<I, O> ItemProcessor<I, O> for BoxedProcessor<I, O> {
    fn process<'a>(
        &'a self,
        item: &'a I,
        context: ProcessContext<'a>,
    ) -> impl Future<Output = Result<ProcessOutcome<O>, ProcessorError>> + Send + 'a {
        self.0.process_boxed(item, context)
    }
}

/// A writer of any concrete type, behind one dynamic dispatch.
pub struct BoxedWriter<I>(Box<dyn sealed::WriterObject<I>>);

impl<I> BoxedWriter<I> {
    /// Erases a concrete writer.
    pub fn new<W: ItemWriter<I> + 'static>(writer: W) -> Self {
        Self(Box::new(writer))
    }
}

impl<I> ItemWriter<I> for BoxedWriter<I> {
    fn write<'a>(
        &'a self,
        items: &'a [I],
        context: WriteContext<'a>,
    ) -> impl Future<Output = Result<WriteOutcome, WriterError>> + Send + 'a {
        self.0.write_boxed(items, context)
    }
}
