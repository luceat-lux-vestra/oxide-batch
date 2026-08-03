//! Distinct pipelines for the monomorphization cost measurement.
//!
//! RFC-0005 leaves code-size and compile-time budgets open. Answering that
//! needs more than one pipeline: a single instantiation cannot show how the
//! typed path scales as a real application adds component types.
//!
//! The const parameter `N` makes each pipeline a genuinely distinct set of
//! types, so `N` pipelines force `N` monomorphizations of the driver and of
//! every component call. The boxed path instantiates the driver once per item
//! type as well, but its component calls collapse onto one dynamically
//! dispatched code path per role.

use std::sync::atomic::{AtomicU64, Ordering};

use oxide_batch::{
    ProcessContext, ProcessOutcome, ProcessorError, ReadContext, ReadOutcome, ReaderError,
    StopSource, WriteContext, WriteOutcome, WriterError,
};

use crate::contract::{
    BoxedProcessor, BoxedReader, BoxedWriter, ItemProcessor, ItemReader, ItemWriter,
};
use crate::driver::{RunReport, TraceKey, run};
use crate::executor::block_on;

/// An item type that is distinct for every `N`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Tagged<const N: usize> {
    /// The item's position in the source.
    pub id: u64,
    /// An opaque business value.
    pub payload: u64,
}

impl<const N: usize> TraceKey for Tagged<N> {
    fn trace_key(&self) -> u64 {
        self.id
    }
}

/// A distinct reader for every `N`.
#[derive(Clone, Copy, Debug)]
pub struct TaggedReader<const N: usize> {
    next: u64,
    end: u64,
}

impl<const N: usize> TaggedReader<N> {
    /// Produces `count` items.
    #[must_use]
    pub const fn new(count: u64) -> Self {
        Self {
            next: 0,
            end: count,
        }
    }
}

impl<const N: usize> ItemReader<Tagged<N>> for TaggedReader<N> {
    async fn read(
        &mut self,
        _context: ReadContext<'_>,
    ) -> Result<ReadOutcome<Tagged<N>>, ReaderError> {
        if self.next >= self.end {
            return Ok(ReadOutcome::EndOfInput);
        }
        let index = self.next;
        self.next += 1;
        Ok(ReadOutcome::Item(Tagged {
            id: index,
            payload: index
                .wrapping_mul(2_654_435_761)
                .wrapping_add(u64::try_from(N).unwrap_or(0)),
        }))
    }
}

/// A distinct processor for every `N`.
#[derive(Clone, Copy, Debug, Default)]
pub struct TaggedProcessor<const N: usize>;

impl<const N: usize> ItemProcessor<Tagged<N>, Tagged<N>> for TaggedProcessor<N> {
    async fn process(
        &self,
        item: &Tagged<N>,
        _context: ProcessContext<'_>,
    ) -> Result<ProcessOutcome<Tagged<N>>, ProcessorError> {
        Ok(ProcessOutcome::Item(Tagged {
            id: item.id,
            payload: item
                .payload
                .rotate_left(u32::try_from(N % 64).unwrap_or(0))
                .wrapping_mul(3),
        }))
    }
}

/// A distinct writer for every `N`.
#[derive(Debug, Default)]
pub struct TaggedWriter<const N: usize> {
    checksum: AtomicU64,
}

impl<const N: usize> TaggedWriter<N> {
    /// Creates an empty sink.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            checksum: AtomicU64::new(0),
        }
    }

    /// Returns the fold of every accepted output.
    pub fn checksum(&self) -> u64 {
        self.checksum.load(Ordering::Relaxed)
    }
}

impl<const N: usize> ItemWriter<Tagged<N>> for TaggedWriter<N> {
    async fn write(
        &self,
        items: &[Tagged<N>],
        _context: WriteContext<'_>,
    ) -> Result<WriteOutcome, WriterError> {
        let mut fold = 0_u64;
        for output in items {
            fold = fold.rotate_left(7).wrapping_add(output.payload ^ output.id);
        }
        self.checksum.fetch_add(fold, Ordering::Relaxed);
        Ok(WriteOutcome::Written)
    }
}

/// Runs pipeline `N` over concrete components and returns its checksum.
#[must_use]
pub fn run_typed_pipeline<const N: usize>(items: u64, chunk_size: usize) -> u64 {
    let (_source, stop) = StopSource::new();
    let mut reader = TaggedReader::<N>::new(items);
    let processor = TaggedProcessor::<N>;
    let writer = TaggedWriter::<N>::new();
    let mut buffer = Vec::with_capacity(chunk_size);
    let mut report = RunReport::untraced();

    block_on(run(
        &mut reader,
        &processor,
        &writer,
        &stop,
        chunk_size,
        &mut buffer,
        &mut report,
    ));

    writer.checksum()
}

/// Runs pipeline `N` through `Boxed*` handles and returns its checksum.
#[must_use]
pub fn run_boxed_pipeline<const N: usize>(items: u64, chunk_size: usize) -> u64 {
    let (_source, stop) = StopSource::new();
    let mut reader = BoxedReader::new(TaggedReader::<N>::new(items));
    let processor = BoxedProcessor::new(TaggedProcessor::<N>);
    let writer = BoxedWriter::new(TaggedWriter::<N>::new());
    let mut buffer = Vec::with_capacity(chunk_size);
    let mut report = RunReport::untraced();

    block_on(run(
        &mut reader,
        &processor,
        &writer,
        &stop,
        chunk_size,
        &mut buffer,
        &mut report,
    ));

    report.items_written
}
