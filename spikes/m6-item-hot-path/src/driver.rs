//! The chunk loop. There is one of it.
//!
//! Under the single-contract design the typed and erased pipelines are the
//! same function with different type arguments, so trace equivalence is not a
//! property to test — it is the same code. The equivalence tests remain as
//! regression cover for the `Boxed*` handles, not as the argument.

use oxide_batch::{
    ProcessContext, ProcessOutcome, ReadContext, ReadOutcome, StopToken, WriteContext, WriteOutcome,
};

use crate::contract::{ItemProcessor, ItemReader, ItemWriter};

/// A stable identity used to compare traces across runs.
pub trait TraceKey {
    /// Returns the item's comparison key.
    fn trace_key(&self) -> u64;
}

/// One observable step of the chunk loop.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TraceEvent {
    /// The reader produced the identified item.
    ItemRead(u64),
    /// The reader reported normal exhaustion.
    ReadEndOfInput,
    /// The reader observed cooperative stop.
    ReadStopped,
    /// The reader failed.
    ReadFailed,
    /// The processor produced the identified output.
    ItemProcessed(u64),
    /// The processor filtered the identified input.
    ItemFiltered(u64),
    /// The processor observed cooperative stop.
    ProcessStopped,
    /// The processor failed.
    ProcessFailed,
    /// The writer accepted a batch of the given size.
    BatchWritten(usize),
    /// The writer observed cooperative stop.
    WriteStopped,
    /// The writer failed.
    WriteFailed,
    /// A chunk completed.
    ChunkCommitted {
        /// The one-based chunk ordinal.
        index: u64,
        /// The number of outputs the chunk wrote.
        written: u64,
    },
    /// Stop was observed at the chunk loop boundary, before a read.
    StepStopped,
}

/// How a run ended.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RunOutcome {
    /// The reader reached end of input.
    #[default]
    Completed,
    /// Cooperative stop ended the run.
    Stopped,
    /// The reader failed.
    ReaderFailed,
    /// The processor failed.
    ProcessorFailed,
    /// The writer failed.
    WriterFailed,
}

/// The full observable result of one run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunReport {
    /// The ordered trace. Empty when the report is untraced.
    pub events: Vec<TraceEvent>,
    /// Whether events are retained.
    recording: bool,
    /// Items the reader produced.
    pub items_read: u64,
    /// Items the processor filtered.
    pub items_filtered: u64,
    /// Outputs the writer accepted.
    pub items_written: u64,
    /// Chunks that completed.
    pub chunks_committed: u64,
    /// How the run ended.
    pub outcome: RunOutcome,
}

impl Default for RunReport {
    fn default() -> Self {
        Self {
            events: Vec::new(),
            recording: true,
            items_read: 0,
            items_filtered: 0,
            items_written: 0,
            chunks_committed: 0,
            outcome: RunOutcome::Completed,
        }
    }
}

impl RunReport {
    /// Preallocates trace storage so that recording never allocates inside a
    /// measurement window.
    ///
    /// A run emits at most one read, one process, and a small constant number
    /// of chunk events per item.
    #[must_use]
    pub fn with_capacity(items: usize) -> Self {
        Self {
            events: Vec::with_capacity(items.saturating_mul(3).saturating_add(64)),
            ..Self::default()
        }
    }

    /// Creates a report that keeps counters but retains no trace.
    ///
    /// The throughput measurement uses this so that trace bookkeeping does not
    /// dilute the dispatch difference it is trying to observe. Both type
    /// arguments are always measured with the same setting.
    #[must_use]
    pub fn untraced() -> Self {
        Self {
            recording: false,
            ..Self::default()
        }
    }

    /// Appends one event when the report is recording.
    pub fn record(&mut self, event: TraceEvent) {
        if self.recording {
            self.events.push(event);
        }
    }
}

/// Drives one chunk-oriented step to completion.
///
/// Monomorphized over whatever components it is given. Passing concrete
/// components produces a pipeline with no per-item heap traffic; passing
/// [`BoxedReader`](crate::contract::BoxedReader) and its siblings produces the
/// dynamically dispatched pipeline. Both are this function.
pub async fn run<I, O, R, P, W>(
    reader: &mut R,
    processor: &P,
    writer: &W,
    stop: &StopToken,
    chunk_size: usize,
    buffer: &mut Vec<O>,
    report: &mut RunReport,
) where
    I: TraceKey,
    O: TraceKey,
    R: ItemReader<I>,
    P: ItemProcessor<I, O>,
    W: ItemWriter<O>,
{
    let mut chunk_index: u64 = 0;

    'step: loop {
        buffer.clear();
        let mut exhausted = false;

        while buffer.len() < chunk_size {
            if stop.is_stop_requested() {
                report.record(TraceEvent::StepStopped);
                report.outcome = RunOutcome::Stopped;
                break 'step;
            }

            match reader.read(ReadContext::new(stop)).await {
                Ok(ReadOutcome::Item(item)) => {
                    report.record(TraceEvent::ItemRead(item.trace_key()));
                    report.items_read += 1;

                    match processor.process(&item, ProcessContext::new(stop)).await {
                        Ok(ProcessOutcome::Item(output)) => {
                            report.record(TraceEvent::ItemProcessed(output.trace_key()));
                            buffer.push(output);
                        }
                        Ok(ProcessOutcome::Filtered) => {
                            report.record(TraceEvent::ItemFiltered(item.trace_key()));
                            report.items_filtered += 1;
                        }
                        Ok(_) => {
                            report.record(TraceEvent::ProcessStopped);
                            report.outcome = RunOutcome::Stopped;
                            break 'step;
                        }
                        Err(_) => {
                            report.record(TraceEvent::ProcessFailed);
                            report.outcome = RunOutcome::ProcessorFailed;
                            break 'step;
                        }
                    }
                }
                Ok(ReadOutcome::EndOfInput) => {
                    report.record(TraceEvent::ReadEndOfInput);
                    exhausted = true;
                    break;
                }
                Ok(_) => {
                    report.record(TraceEvent::ReadStopped);
                    report.outcome = RunOutcome::Stopped;
                    break 'step;
                }
                Err(_) => {
                    report.record(TraceEvent::ReadFailed);
                    report.outcome = RunOutcome::ReaderFailed;
                    break 'step;
                }
            }
        }

        if !buffer.is_empty() {
            let batch = buffer.len();
            match writer
                .write(buffer.as_slice(), WriteContext::non_transactional(stop))
                .await
            {
                Ok(WriteOutcome::Written) => {
                    report.record(TraceEvent::BatchWritten(batch));
                    let written = u64::try_from(batch).unwrap_or(u64::MAX);
                    report.items_written += written;
                    chunk_index += 1;
                    report.chunks_committed += 1;
                    report.record(TraceEvent::ChunkCommitted {
                        index: chunk_index,
                        written,
                    });
                }
                Ok(_) => {
                    report.record(TraceEvent::WriteStopped);
                    report.outcome = RunOutcome::Stopped;
                    break 'step;
                }
                Err(_) => {
                    report.record(TraceEvent::WriteFailed);
                    report.outcome = RunOutcome::WriterFailed;
                    break 'step;
                }
            }
        }

        if exhausted {
            break 'step;
        }
    }
}
