//! One scenario description, executed with each set of type arguments.
//!
//! `execute_typed` and `execute_boxed` differ in exactly one respect: whether
//! the components are handed to [`driver::run`](crate::driver::run) concretely
//! or wrapped in `Boxed*` handles first. They call the same function.

use std::sync::Arc;

use oxide_batch::{StopSource, StopToken};

use crate::contract::{BoxedProcessor, BoxedReader, BoxedWriter};
use crate::driver::{RunReport, run};
use crate::workload::{
    ChecksumWriter, Fault, Output, RangeReader, ScalingProcessor, SharedChecksumWriter,
};

/// The multiplier every scenario's processor applies.
pub const FACTOR: u64 = 3;

/// A reproducible pipeline configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Scenario {
    /// The number of items the reader produces.
    pub items: u64,
    /// The chunk size the driver fills.
    pub chunk_size: usize,
    /// Filter every item whose identifier is a multiple of this period.
    pub filter_every: Option<u64>,
    /// A positional reader fault.
    pub reader_fault: Option<Fault>,
    /// A positional processor fault.
    pub processor_fault: Option<Fault>,
    /// A batch-ordinal writer fault.
    pub writer_fault: Option<Fault>,
    /// Whether stop is already requested when the run begins.
    pub stopped_before_start: bool,
}

impl Scenario {
    /// Describes a clean run of `items` items in chunks of `chunk_size`.
    #[must_use]
    pub const fn new(items: u64, chunk_size: usize) -> Self {
        Self {
            items,
            chunk_size,
            filter_every: None,
            reader_fault: None,
            processor_fault: None,
            writer_fault: None,
            stopped_before_start: false,
        }
    }

    /// Filters every item whose identifier is a multiple of `period`.
    #[must_use]
    pub const fn filtering_every(mut self, period: u64) -> Self {
        self.filter_every = Some(period);
        self
    }

    /// Installs a reader fault.
    #[must_use]
    pub const fn with_reader_fault(mut self, fault: Fault) -> Self {
        self.reader_fault = Some(fault);
        self
    }

    /// Installs a processor fault.
    #[must_use]
    pub const fn with_processor_fault(mut self, fault: Fault) -> Self {
        self.processor_fault = Some(fault);
        self
    }

    /// Installs a writer fault.
    #[must_use]
    pub const fn with_writer_fault(mut self, fault: Fault) -> Self {
        self.writer_fault = Some(fault);
        self
    }

    /// Requests stop before the first read.
    #[must_use]
    pub const fn stopped_before_start(mut self) -> Self {
        self.stopped_before_start = true;
        self
    }

    fn reader(self) -> RangeReader {
        let reader = RangeReader::new(self.items);
        match self.reader_fault {
            Some(fault) => reader.with_fault(fault),
            None => reader,
        }
    }

    fn processor(self) -> ScalingProcessor {
        let processor = ScalingProcessor::new(FACTOR);
        let processor = match self.filter_every {
            Some(period) => processor.filtering_every(period),
            None => processor,
        };
        match self.processor_fault {
            Some(fault) => processor.with_fault(fault),
            None => processor,
        }
    }

    fn writer(self) -> ChecksumWriter {
        let writer = ChecksumWriter::new();
        match self.writer_fault {
            Some(fault) => writer.with_fault(fault),
            None => writer,
        }
    }

    fn stop(self) -> (StopSource, StopToken) {
        let (source, token) = StopSource::new();
        if self.stopped_before_start {
            source.request_stop();
        }
        (source, token)
    }

    fn buffer(self) -> Vec<Output> {
        Vec::with_capacity(self.chunk_size)
    }

    fn report(self) -> RunReport {
        RunReport::with_capacity(usize::try_from(self.items).unwrap_or(usize::MAX))
    }
}

/// Everything a run leaves behind that either set of type arguments could
/// differ on.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Observed {
    /// The ordered trace and counters.
    pub report: RunReport,
    /// The writer's fold over every accepted output.
    pub checksum: u64,
    /// Batches the writer accepted.
    pub batches: u64,
    /// Outputs the writer accepted.
    pub written: u64,
}

/// Runs the scenario over concrete components.
pub async fn execute_typed(scenario: Scenario) -> Observed {
    let (_source, stop) = scenario.stop();
    let mut reader = scenario.reader();
    let processor = scenario.processor();
    let writer = scenario.writer();
    let mut buffer = scenario.buffer();
    let mut report = scenario.report();

    run(
        &mut reader,
        &processor,
        &writer,
        &stop,
        scenario.chunk_size,
        &mut buffer,
        &mut report,
    )
    .await;

    Observed {
        report,
        checksum: writer.checksum(),
        batches: writer.batches(),
        written: writer.items(),
    }
}

/// Runs the scenario over the same components behind `Boxed*` handles.
///
/// Identical to [`execute_typed`] except for the three `Boxed*` constructions.
/// Both call [`run`].
pub async fn execute_boxed(scenario: Scenario) -> Observed {
    let (_source, stop) = scenario.stop();
    let mut reader = BoxedReader::new(scenario.reader());
    let processor = BoxedProcessor::new(scenario.processor());
    let mut buffer = scenario.buffer();
    let mut report = scenario.report();

    // Boxing moves the writer behind the handle, so the scenario keeps an
    // `Arc` in order to read its counters back afterwards.
    let writer = Arc::new(scenario.writer());
    let boxed_writer = BoxedWriter::new(SharedChecksumWriter(Arc::clone(&writer)));

    run(
        &mut reader,
        &processor,
        &boxed_writer,
        &stop,
        scenario.chunk_size,
        &mut buffer,
        &mut report,
    )
    .await;

    Observed {
        report,
        checksum: writer.checksum(),
        batches: writer.batches(),
        written: writer.items(),
    }
}
