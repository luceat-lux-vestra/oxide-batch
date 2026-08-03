//! One representative item workload, written once against the contract.
//!
//! Every component here is a plain `async fn` implementation. No lifetime, no
//! future type, and no boxing appears in user-authored code — that is the
//! ergonomics claim the contract has to earn, so the workload is written the
//! way an application would write it.
//!
//! The components are allocation-free in their steady state: the reader
//! generates items arithmetically, the item and output types are `Copy`, and
//! the writer folds a batch into an atomic checksum. Anything the measurement
//! then observes is dispatch, not workload.
//!
//! Faults are positional rather than time-based so that both type arguments
//! see the identical sequence on every run.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use oxide_batch::{
    ProcessContext, ProcessOutcome, ProcessorError, ReadContext, ReadOutcome, ReaderError,
    WriteContext, WriteOutcome, WriterError,
};

use crate::contract::{ItemProcessor, ItemReader, ItemWriter};
use crate::driver::TraceKey;

/// The input item.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Record {
    /// The item's position in the source.
    pub id: u64,
    /// An opaque business value.
    pub payload: u64,
}

impl TraceKey for Record {
    fn trace_key(&self) -> u64 {
        self.id
    }
}

/// The output item.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Output {
    /// The originating item's position.
    pub id: u64,
    /// The transformed business value.
    pub payload: u64,
}

impl TraceKey for Output {
    fn trace_key(&self) -> u64 {
        self.id
    }
}

/// What a component does at a chosen position.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Fault {
    /// Report the contract's stop outcome at this position.
    Stop(u64),
    /// Report the contract's error at this position.
    Fail(u64),
    /// Panic at this position.
    Panic(u64),
}

impl Fault {
    const fn position(self) -> u64 {
        match self {
            Self::Stop(at) | Self::Fail(at) | Self::Panic(at) => at,
        }
    }

    fn fires_at(self, index: u64) -> bool {
        self.position() == index
    }
}

/// An allocation-free arithmetic item source.
#[derive(Clone, Copy, Debug)]
pub struct RangeReader {
    next: u64,
    end: u64,
    fault: Option<Fault>,
}

impl RangeReader {
    /// Produces `count` items with ascending identifiers.
    #[must_use]
    pub const fn new(count: u64) -> Self {
        Self {
            next: 0,
            end: count,
            fault: None,
        }
    }

    /// Installs a positional fault.
    #[must_use]
    pub const fn with_fault(mut self, fault: Fault) -> Self {
        self.fault = Some(fault);
        self
    }
}

impl ItemReader<Record> for RangeReader {
    async fn read(
        &mut self,
        _context: ReadContext<'_>,
    ) -> Result<ReadOutcome<Record>, ReaderError> {
        let index = self.next;
        match self.fault.filter(|fault| fault.fires_at(index)) {
            Some(Fault::Stop(_)) => return Ok(ReadOutcome::Stopped),
            Some(Fault::Fail(_)) => return Err(ReaderError::new()),
            #[allow(clippy::panic)]
            Some(Fault::Panic(at)) => panic!("reader panic at item {at}"),
            None => {}
        }

        if index >= self.end {
            return Ok(ReadOutcome::EndOfInput);
        }
        self.next += 1;
        Ok(ReadOutcome::Item(Record {
            id: index,
            payload: index.wrapping_mul(2_654_435_761),
        }))
    }
}

/// A pure transformer that optionally filters and optionally faults.
#[derive(Clone, Copy, Debug)]
pub struct ScalingProcessor {
    factor: u64,
    filter_every: Option<u64>,
    fault: Option<Fault>,
}

impl ScalingProcessor {
    /// Multiplies each payload by `factor`.
    #[must_use]
    pub const fn new(factor: u64) -> Self {
        Self {
            factor,
            filter_every: None,
            fault: None,
        }
    }

    /// Filters every item whose identifier is a multiple of `period`.
    #[must_use]
    pub const fn filtering_every(mut self, period: u64) -> Self {
        self.filter_every = Some(period);
        self
    }

    /// Installs a positional fault.
    #[must_use]
    pub const fn with_fault(mut self, fault: Fault) -> Self {
        self.fault = Some(fault);
        self
    }
}

impl ItemProcessor<Record, Output> for ScalingProcessor {
    async fn process(
        &self,
        item: &Record,
        _context: ProcessContext<'_>,
    ) -> Result<ProcessOutcome<Output>, ProcessorError> {
        match self.fault.filter(|fault| fault.fires_at(item.id)) {
            Some(Fault::Stop(_)) => return Ok(ProcessOutcome::Stopped),
            Some(Fault::Fail(_)) => return Err(ProcessorError::new()),
            #[allow(clippy::panic)]
            Some(Fault::Panic(at)) => panic!("processor panic at item {at}"),
            None => {}
        }

        let filtered = self
            .filter_every
            .is_some_and(|period| period != 0 && item.id.is_multiple_of(period));
        if filtered {
            return Ok(ProcessOutcome::Filtered);
        }

        Ok(ProcessOutcome::Item(Output {
            id: item.id,
            payload: item.payload.wrapping_mul(self.factor),
        }))
    }
}

/// An allocation-free sink that folds each batch into an atomic checksum.
#[derive(Debug, Default)]
pub struct ChecksumWriter {
    checksum: AtomicU64,
    batches: AtomicU64,
    items: AtomicU64,
    fault: Option<Fault>,
}

impl ChecksumWriter {
    /// Creates an empty sink.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            checksum: AtomicU64::new(0),
            batches: AtomicU64::new(0),
            items: AtomicU64::new(0),
            fault: None,
        }
    }

    /// Installs a fault keyed on the one-based batch ordinal.
    #[must_use]
    pub const fn with_fault(mut self, fault: Fault) -> Self {
        self.fault = Some(fault);
        self
    }

    /// Returns the fold of every accepted output.
    pub fn checksum(&self) -> u64 {
        self.checksum.load(Ordering::Relaxed)
    }

    /// Returns the number of accepted batches.
    pub fn batches(&self) -> u64 {
        self.batches.load(Ordering::Relaxed)
    }

    /// Returns the number of accepted outputs.
    pub fn items(&self) -> u64 {
        self.items.load(Ordering::Relaxed)
    }
}

impl ItemWriter<Output> for ChecksumWriter {
    async fn write(
        &self,
        items: &[Output],
        _context: WriteContext<'_>,
    ) -> Result<WriteOutcome, WriterError> {
        let ordinal = self.batches.load(Ordering::Relaxed) + 1;
        match self.fault.filter(|fault| fault.fires_at(ordinal)) {
            Some(Fault::Stop(_)) => return Ok(WriteOutcome::Stopped),
            Some(Fault::Fail(_)) => return Err(WriterError::new()),
            #[allow(clippy::panic)]
            Some(Fault::Panic(at)) => panic!("writer panic at batch {at}"),
            None => {}
        }

        let mut fold = 0_u64;
        let mut accepted = 0_u64;
        for output in items {
            fold = fold.rotate_left(7).wrapping_add(output.payload ^ output.id);
            accepted += 1;
        }

        self.checksum.fetch_add(fold, Ordering::Relaxed);
        self.batches.fetch_add(1, Ordering::Relaxed);
        self.items.fetch_add(accepted, Ordering::Relaxed);
        Ok(WriteOutcome::Written)
    }
}

/// A writer handle that shares one [`ChecksumWriter`] with the caller.
///
/// Boxing a writer moves it behind the handle, so a measurement that needs the
/// writer's durable fold after an erased run keeps an `Arc` and erases this
/// instead. The extra indirection is present on both paths only when both use
/// it, so it never biases a comparison.
#[derive(Clone, Debug)]
pub struct SharedChecksumWriter(pub Arc<ChecksumWriter>);

// Ergonomics finding worth recording: a *leaf* component can elide every
// lifetime, but a component that delegates to another component has to tie
// them, because the inner call unifies receiver, item, and context at one
// lifetime. M6's composites, classifiers, delegates, and wrappers are all
// delegating components, so this is the shape their authors will write.
impl ItemWriter<Output> for SharedChecksumWriter {
    async fn write<'a>(
        &'a self,
        items: &'a [Output],
        context: WriteContext<'a>,
    ) -> Result<WriteOutcome, WriterError> {
        self.0.write(items, context).await
    }
}
