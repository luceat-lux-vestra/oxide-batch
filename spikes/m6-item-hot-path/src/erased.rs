//! Migration evidence: the proposed contract can still produce the accepted
//! ADR-0002 handles.
//!
//! The superseding ADR retires `oxide_batch::ItemReader` and friends as the
//! public contract. Retiring them is only safe if a component written against
//! the new contract can still be handed to code that expects the old boxed
//! trait objects, which is what these adapters demonstrate.
//!
//! Note what is absent compared with the first draft of this spike: no
//! `I: 'static`. The explicit call lifetime on the contract carries through,
//! so erasure no longer pins the item type.

use oxide_batch::{
    BoxFuture, ItemProcessor as BoxedItemProcessor, ItemReader as BoxedItemReader,
    ItemWriter as BoxedItemWriter, ProcessContext, ProcessOutcome, ProcessorError, ReadContext,
    ReadOutcome, ReaderError, WriteContext, WriteOutcome, WriterError,
};

use crate::contract::{ItemProcessor, ItemReader, ItemWriter};

/// Presents a contract reader as an ADR-0002 boxed reader.
#[derive(Clone, Copy, Debug)]
pub struct ErasedReader<R>(R);

impl<R> ErasedReader<R> {
    /// Wraps a reader.
    pub const fn new(reader: R) -> Self {
        Self(reader)
    }

    /// Borrows the wrapped reader.
    pub const fn inner(&self) -> &R {
        &self.0
    }
}

impl<I, R: ItemReader<I>> BoxedItemReader<I> for ErasedReader<R> {
    fn read<'a>(
        &'a mut self,
        context: ReadContext<'a>,
    ) -> BoxFuture<'a, Result<ReadOutcome<I>, ReaderError>> {
        Box::pin(self.0.read(context))
    }
}

/// Presents a contract processor as an ADR-0002 boxed processor.
#[derive(Clone, Copy, Debug)]
pub struct ErasedProcessor<P>(P);

impl<P> ErasedProcessor<P> {
    /// Wraps a processor.
    pub const fn new(processor: P) -> Self {
        Self(processor)
    }

    /// Borrows the wrapped processor.
    pub const fn inner(&self) -> &P {
        &self.0
    }
}

impl<I, O, P: ItemProcessor<I, O>> BoxedItemProcessor<I, O> for ErasedProcessor<P> {
    fn process<'a>(
        &'a self,
        item: &'a I,
        context: ProcessContext<'a>,
    ) -> BoxFuture<'a, Result<ProcessOutcome<O>, ProcessorError>> {
        Box::pin(self.0.process(item, context))
    }
}

/// Presents a contract writer as an ADR-0002 boxed writer.
#[derive(Clone, Copy, Debug)]
pub struct ErasedWriter<W>(W);

impl<W> ErasedWriter<W> {
    /// Wraps a writer.
    pub const fn new(writer: W) -> Self {
        Self(writer)
    }

    /// Borrows the wrapped writer.
    pub const fn inner(&self) -> &W {
        &self.0
    }
}

impl<I, W: ItemWriter<I>> BoxedItemWriter<I> for ErasedWriter<W> {
    fn write<'a>(
        &'a self,
        items: &'a [I],
        context: WriteContext<'a>,
    ) -> BoxFuture<'a, Result<WriteOutcome, WriterError>> {
        Box::pin(self.0.write(items, context))
    }
}
