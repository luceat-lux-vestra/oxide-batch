//! Named lifecycle/ordering evidence for the M6 `#144` `ItemStream` contract.
//!
//! Scenarios prove the open/update/close ordering fixed by the issue: open
//! before any item work, update after the writer accepts the chunk and
//! before the durable commit, close after the step attempt's terminal
//! outcome is known; multiple streams open in registration order and close
//! in reverse successful-open order; a failed open closes only the streams
//! already opened; a close failure never skips another stream's close, never
//! erases an earlier primary failure, and never erases already-committed
//! chunks. Envelope/codec/checksum/migration/bounds/disclosure evidence for
//! the same contract lives in `item_stream_state.rs`.

#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::type_complexity,
    // Trace events are dotted log labels ("a.open"), not file paths; this
    // lint's file-extension framing does not apply.
    clippy::case_sensitive_file_extension_comparisons
)]

#[allow(dead_code)]
#[path = "support/clock.rs"]
mod clock;
#[allow(dead_code)]
#[path = "support/ids.rs"]
mod ids;

use std::collections::VecDeque;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};
use std::time::UNIX_EPOCH;

use clock::ManualClock;
use ids::DeterministicIds;
use oxide_batch::{
    BoxFuture, BusinessTransaction, Checkpoint, ChunkCommitReceipt, ChunkComponentRevisions,
    ChunkCount, ChunkCounts, ChunkDeliveryMode, ChunkExecutionOutcome, ChunkFailure,
    ChunkFaultProgress, ChunkJob, ChunkRestartContract, ChunkSize, ChunkStep, ChunkTransaction,
    ChunkTransactionContext, ChunkTransactionError, ChunkTransactionManager, CodecId, CodecVersion,
    ComponentRevision, ComponentStateEnvelope, ComponentStreamIdentity, DefaultComponentCodec,
    DefinitionRevision, ExecutionContext, InMemoryJobRepository, ItemProcessor, ItemReader,
    ItemStream, ItemWriter, JobLauncher, JobName, JobParameters, ProcessContext, ProcessOutcome,
    ProcessorError, ReadContext, ReadOutcome, ReaderError, RestartabilityDeclaration,
    StateCodecError, StateLimits, StateSchemaId, StateSchemaVersion, StepName, StopSource,
    StreamCloseContext, StreamCloseError, StreamCloseOutcome, StreamOpenContext, StreamOpenError,
    StreamOpenOutcome, StreamStateContract, StreamUpdateContext, StreamUpdateError,
    VersionedStateCodec, WriteContext, WriteOutcome, WriterError,
};

type Trace = Arc<Mutex<Vec<String>>>;

fn record(trace: &Trace, event: impl Into<String>) {
    trace
        .lock()
        .expect("trace lock poisoned")
        .push(event.into());
}

fn trace_of(trace: &Trace) -> Vec<String> {
    trace.lock().expect("trace lock poisoned").clone()
}

/// A minimal codec so fixtures can produce a valid [`ComponentStateEnvelope`]
/// without depending on `item_stream_state.rs`'s fixtures.
struct UnitSchema {
    schema: StateSchemaId,
}

impl VersionedStateCodec<()> for UnitSchema {
    fn schema_id(&self) -> &StateSchemaId {
        &self.schema
    }
    fn current_version(&self) -> StateSchemaVersion {
        StateSchemaVersion::new(1).expect("nonzero")
    }
    fn encode(&self, (): &()) -> Result<Vec<u8>, StateCodecError> {
        serde_json::to_vec(&serde_json::json!({})).map_err(|_| StateCodecError::InvalidPayload)
    }
    fn decode(&self, _payload: &[u8]) -> Result<(), StateCodecError> {
        Ok(())
    }
}

fn envelope_with_schema(namespace: &str, schema: &str) -> ComponentStateEnvelope {
    let codec = DefaultComponentCodec::new(
        UnitSchema {
            schema: StateSchemaId::new(schema).expect("valid schema id"),
        },
        CodecId::new("test.stream-codec").expect("valid codec id"),
        CodecVersion::new(1).expect("nonzero"),
        RestartabilityDeclaration::Restartable,
    );
    ComponentStateEnvelope::encode(
        ComponentStreamIdentity::new(namespace).expect("valid namespace"),
        &(),
        &codec,
        StateLimits::default(),
    )
    .expect("minimal envelope encodes")
}

fn minimal_envelope(namespace: &str) -> ComponentStateEnvelope {
    envelope_with_schema(namespace, "test.stream")
}

/// The [`StreamStateContract`] matching [`minimal_envelope`]'s codec, for
/// fixtures that register a stream through [`ChunkStep::with_item_stream`].
fn minimal_contract() -> StreamStateContract {
    StreamStateContract::new(DefaultComponentCodec::new(
        UnitSchema {
            schema: StateSchemaId::new("test.stream").expect("valid schema id"),
        },
        CodecId::new("test.stream-codec").expect("valid codec id"),
        CodecVersion::new(1).expect("nonzero"),
        RestartabilityDeclaration::Restartable,
    ))
}

/// A stream that records every lifecycle call and can be configured to fail
/// its open or close, or to return a candidate envelope under a namespace
/// other than its own registered identity.
struct RecordingStream {
    name: &'static str,
    trace: Trace,
    fail_open: bool,
    fail_close: bool,
    update_namespace: Option<&'static str>,
}

impl RecordingStream {
    fn new(name: &'static str, trace: &Trace) -> Self {
        Self {
            name,
            trace: Arc::clone(trace),
            fail_open: false,
            fail_close: false,
            update_namespace: None,
        }
    }

    fn failing_open(mut self) -> Self {
        self.fail_open = true;
        self
    }

    fn failing_close(mut self) -> Self {
        self.fail_close = true;
        self
    }

    /// Makes `update` return a candidate envelope under `namespace` instead
    /// of this stream's own registered identity, so a namespace-mismatch
    /// scenario is reproducible in a fixture without a codec bug.
    fn returning_namespace(mut self, namespace: &'static str) -> Self {
        self.update_namespace = Some(namespace);
        self
    }
}

impl ItemStream for RecordingStream {
    async fn open(
        &self,
        _context: StreamOpenContext<'_>,
    ) -> Result<StreamOpenOutcome, StreamOpenError> {
        record(&self.trace, format!("{}.open", self.name));
        if self.fail_open {
            Err(StreamOpenError::new())
        } else {
            Ok(StreamOpenOutcome::Initial)
        }
    }

    async fn update(
        &self,
        _context: StreamUpdateContext<'_>,
    ) -> Result<ComponentStateEnvelope, StreamUpdateError> {
        record(&self.trace, format!("{}.update", self.name));
        Ok(minimal_envelope(self.update_namespace.unwrap_or(self.name)))
    }

    async fn close(
        &self,
        _context: StreamCloseContext<'_>,
    ) -> Result<StreamCloseOutcome, StreamCloseError> {
        record(&self.trace, format!("{}.close", self.name));
        if self.fail_close {
            Err(StreamCloseError::new())
        } else {
            Ok(StreamCloseOutcome::Closed)
        }
    }
}

#[derive(Clone, Copy)]
enum Boundary {
    Normal,
    Error,
}

struct Reader {
    items: VecDeque<i32>,
    boundary: Boundary,
    trace: Trace,
}

impl Reader {
    fn new(items: impl IntoIterator<Item = i32>, trace: &Trace) -> Self {
        Self {
            items: items.into_iter().collect(),
            boundary: Boundary::Normal,
            trace: Arc::clone(trace),
        }
    }
}

impl ItemReader<i32> for Reader {
    async fn read(&mut self, _context: ReadContext<'_>) -> Result<ReadOutcome<i32>, ReaderError> {
        record(&self.trace, "reader.read");
        match self.boundary {
            Boundary::Error => Err(ReaderError::new()),
            Boundary::Normal => Ok(self
                .items
                .pop_front()
                .map_or(ReadOutcome::EndOfInput, ReadOutcome::Item)),
        }
    }
}

struct Processor {
    trace: Trace,
}

impl ItemProcessor<i32, i32> for Processor {
    async fn process(
        &self,
        item: &i32,
        _context: ProcessContext<'_>,
    ) -> Result<ProcessOutcome<i32>, ProcessorError> {
        record(&self.trace, "processor.process");
        Ok(ProcessOutcome::Item(*item))
    }
}

struct Writer {
    boundary: Boundary,
    trace: Trace,
}

impl ItemWriter<i32> for Writer {
    async fn write(
        &self,
        _items: &[i32],
        _context: WriteContext<'_>,
    ) -> Result<WriteOutcome, WriterError> {
        record(&self.trace, "writer.write");
        match self.boundary {
            Boundary::Error => Err(WriterError::new()),
            Boundary::Normal => Ok(WriteOutcome::Written),
        }
    }
}

fn receipt() -> ChunkCommitReceipt {
    let checkpoint = Checkpoint::from_json(
        br#"{"format":"oxide-batch.checkpoint","format_version":1,"schema":"test.position","schema_version":1,"payload":{"position":0}}"#,
        StateLimits::default(),
    )
    .expect("checkpoint fixture must be valid");
    let context = ExecutionContext::from_json(
        br#"{"format":"oxide-batch.execution-context","format_version":1,"schema":"test.context","schema_version":1,"payload":{}}"#,
        StateLimits::default(),
    )
    .expect("context fixture must be valid");
    ChunkCommitReceipt::new(checkpoint, context)
}

struct Transactions {
    receipt: ChunkCommitReceipt,
    trace: Trace,
}

impl ChunkTransactionManager for Transactions {
    fn begin(
        &self,
    ) -> BoxFuture<'_, Result<Box<dyn ChunkTransaction + '_>, ChunkTransactionError>> {
        let transaction = TestTransaction {
            receipt: self.receipt.clone(),
            trace: Arc::clone(&self.trace),
        };
        Box::pin(async move { Ok(Box::new(transaction) as Box<dyn ChunkTransaction>) })
    }
}

struct InheritedTransactions {
    receipt: ChunkCommitReceipt,
    trace: Trace,
    inherited: Vec<ComponentStateEnvelope>,
}

impl ChunkTransactionManager for InheritedTransactions {
    fn begin(
        &self,
    ) -> BoxFuture<'_, Result<Box<dyn ChunkTransaction + '_>, ChunkTransactionError>> {
        let transaction = TestTransaction {
            receipt: self.receipt.clone(),
            trace: Arc::clone(&self.trace),
        };
        Box::pin(async move { Ok(Box::new(transaction) as Box<dyn ChunkTransaction + '_>) })
    }

    fn inherited_component_state(
        &self,
        _context: ChunkTransactionContext,
    ) -> BoxFuture<'_, Result<Vec<ComponentStateEnvelope>, ChunkTransactionError>> {
        let inherited = self.inherited.clone();
        Box::pin(async move { Ok(inherited) })
    }
}

struct TestTransaction {
    receipt: ChunkCommitReceipt,
    trace: Trace,
}

impl ChunkTransaction for TestTransaction {
    fn business_transaction(&mut self) -> Option<&mut dyn BusinessTransaction> {
        None
    }

    fn commit(
        &mut self,
        counts: ChunkCounts,
        fault: ChunkFaultProgress,
    ) -> BoxFuture<'_, Result<ChunkCommitReceipt, ChunkTransactionError>> {
        self.commit_with_component_state(counts, fault, &[])
    }

    fn commit_with_component_state(
        &mut self,
        _counts: ChunkCounts,
        _fault: ChunkFaultProgress,
        _component_state: &[ComponentStateEnvelope],
    ) -> BoxFuture<'_, Result<ChunkCommitReceipt, ChunkTransactionError>> {
        record(&self.trace, "transaction.commit");
        let receipt = self.receipt.clone();
        Box::pin(async move { Ok(receipt) })
    }

    fn rollback(&mut self) -> BoxFuture<'_, Result<(), ChunkTransactionError>> {
        record(&self.trace, "transaction.rollback");
        Box::pin(async { Ok(()) })
    }
}

/// A transaction whose commit always fails, so the step ends in a known
/// rollback -- used for the close-failure/primary-failure scenario.
struct FailingWriteTransactions {
    trace: Trace,
}

impl ChunkTransactionManager for FailingWriteTransactions {
    fn begin(
        &self,
    ) -> BoxFuture<'_, Result<Box<dyn ChunkTransaction + '_>, ChunkTransactionError>> {
        let transaction = FailingWriteTransaction {
            trace: Arc::clone(&self.trace),
        };
        Box::pin(async move { Ok(Box::new(transaction) as Box<dyn ChunkTransaction>) })
    }
}

struct FailingWriteTransaction {
    trace: Trace,
}

impl ChunkTransaction for FailingWriteTransaction {
    fn business_transaction(&mut self) -> Option<&mut dyn BusinessTransaction> {
        None
    }

    fn commit(
        &mut self,
        counts: ChunkCounts,
        fault: ChunkFaultProgress,
    ) -> BoxFuture<'_, Result<ChunkCommitReceipt, ChunkTransactionError>> {
        self.commit_with_component_state(counts, fault, &[])
    }

    fn commit_with_component_state(
        &mut self,
        _counts: ChunkCounts,
        _fault: ChunkFaultProgress,
        _component_state: &[ComponentStateEnvelope],
    ) -> BoxFuture<'_, Result<ChunkCommitReceipt, ChunkTransactionError>> {
        unreachable!("the writer fails before a commit is ever attempted")
    }

    fn rollback(&mut self) -> BoxFuture<'_, Result<(), ChunkTransactionError>> {
        record(&self.trace, "transaction.rollback");
        Box::pin(async { Ok(()) })
    }
}

fn step_name() -> StepName {
    StepName::new("item_stream_step").expect("valid step name")
}

fn stream_components() -> ChunkComponentRevisions {
    ChunkComponentRevisions::new(
        ComponentRevision::new("reader-v1").expect("valid revision"),
        ComponentRevision::new("processor-v1").expect("valid revision"),
        ComponentRevision::new("writer-v1").expect("valid revision"),
        ComponentRevision::new("checkpoint-v1").expect("valid revision"),
        ChunkRestartContract::new(
            StateSchemaId::new("test.position").expect("valid schema id"),
            StateSchemaVersion::new(1).expect("nonzero schema version"),
            StateSchemaId::new("test.context").expect("valid schema id"),
            StateSchemaVersion::new(1).expect("nonzero schema version"),
            ChunkDeliveryMode::AtLeastOnce,
        ),
    )
    .with_stream_revision(
        ComponentStreamIdentity::new("stream_a").expect("valid namespace"),
        ComponentRevision::new("stream-a-v1").expect("valid revision"),
    )
    .with_stream_revision(
        ComponentStreamIdentity::new("stream_b").expect("valid namespace"),
        ComponentRevision::new("stream-b-v1").expect("valid revision"),
    )
}

#[tokio::test]
async fn item_stream_opens_before_item_work() {
    let trace = Trace::default();
    let step = ChunkStep::new(
        step_name(),
        ChunkSize::new(10).expect("valid chunk size"),
        Reader::new([1], &trace),
        Processor {
            trace: Arc::clone(&trace),
        },
        Writer {
            boundary: Boundary::Normal,
            trace: Arc::clone(&trace),
        },
        Arc::new(Transactions {
            receipt: receipt(),
            trace: Arc::clone(&trace),
        }),
        Arc::new(NoopCompletion),
    )
    .with_item_stream(
        ComponentStreamIdentity::new("stream").expect("valid namespace"),
        RecordingStream::new("stream", &trace),
        minimal_contract(),
    );
    let mut step = step;
    let (_source, stop_token) = StopSource::new();

    let _report = step.execute(&correlation(), &stop_token).await;

    let events = trace_of(&trace);
    let open_index = events
        .iter()
        .position(|event| event == "stream.open")
        .expect("stream must open");
    let read_index = events
        .iter()
        .position(|event| event == "reader.read")
        .expect("reader must read");
    assert!(open_index < read_index, "trace: {events:?}");
}

#[tokio::test]
async fn item_stream_update_prepares_state_before_accepting_commit() {
    let trace = Trace::default();
    let mut step = ChunkStep::new(
        step_name(),
        ChunkSize::new(10).expect("valid chunk size"),
        Reader::new([1], &trace),
        Processor {
            trace: Arc::clone(&trace),
        },
        Writer {
            boundary: Boundary::Normal,
            trace: Arc::clone(&trace),
        },
        Arc::new(Transactions {
            receipt: receipt(),
            trace: Arc::clone(&trace),
        }),
        Arc::new(NoopCompletion),
    )
    .with_item_stream(
        ComponentStreamIdentity::new("stream").expect("valid namespace"),
        RecordingStream::new("stream", &trace),
        minimal_contract(),
    );
    let (_source, stop_token) = StopSource::new();

    let report = step.execute(&correlation(), &stop_token).await;
    assert_eq!(report.outcome(), ChunkExecutionOutcome::Completed);

    let events = trace_of(&trace);
    let write_index = events
        .iter()
        .position(|event| event == "writer.write")
        .expect("writer must write");
    let update_index = events
        .iter()
        .position(|event| event == "stream.update")
        .expect("stream must update");
    let commit_index = events
        .iter()
        .position(|event| event == "transaction.commit")
        .expect("transaction must commit");
    assert!(
        write_index < update_index && update_index < commit_index,
        "trace: {events:?}"
    );
}

#[tokio::test]
async fn item_stream_close_runs_after_runtime_completion() {
    let trace = Trace::default();
    let mut step = ChunkStep::new(
        step_name(),
        ChunkSize::new(10).expect("valid chunk size"),
        Reader::new([1], &trace),
        Processor {
            trace: Arc::clone(&trace),
        },
        Writer {
            boundary: Boundary::Normal,
            trace: Arc::clone(&trace),
        },
        Arc::new(Transactions {
            receipt: receipt(),
            trace: Arc::clone(&trace),
        }),
        Arc::new(NoopCompletion),
    )
    .with_item_stream(
        ComponentStreamIdentity::new("stream").expect("valid namespace"),
        RecordingStream::new("stream", &trace),
        minimal_contract(),
    );
    let (_source, stop_token) = StopSource::new();

    let report = step.execute(&correlation(), &stop_token).await;
    assert_eq!(report.outcome(), ChunkExecutionOutcome::Completed);

    let events = trace_of(&trace);
    assert_eq!(events.last().map(String::as_str), Some("stream.close"));
}

#[tokio::test]
async fn multiple_streams_open_in_registration_order() {
    let trace = Trace::default();
    let mut step = ChunkStep::new(
        step_name(),
        ChunkSize::new(10).expect("valid chunk size"),
        Reader::new([1], &trace),
        Processor {
            trace: Arc::clone(&trace),
        },
        Writer {
            boundary: Boundary::Normal,
            trace: Arc::clone(&trace),
        },
        Arc::new(Transactions {
            receipt: receipt(),
            trace: Arc::clone(&trace),
        }),
        Arc::new(NoopCompletion),
    )
    .with_item_stream(
        ComponentStreamIdentity::new("stream_a").expect("valid namespace"),
        RecordingStream::new("a", &trace),
        minimal_contract(),
    )
    .with_item_stream(
        ComponentStreamIdentity::new("stream_b").expect("valid namespace"),
        RecordingStream::new("b", &trace),
        minimal_contract(),
    );
    let (_source, stop_token) = StopSource::new();

    let _report = step.execute(&correlation(), &stop_token).await;

    let events = trace_of(&trace);
    let opens: Vec<&String> = events
        .iter()
        .filter(|event| event.ends_with(".open"))
        .collect();
    assert_eq!(opens, vec!["a.open", "b.open"], "trace: {events:?}");
}

#[tokio::test]
async fn multiple_streams_close_in_reverse_successful_open_order() {
    let trace = Trace::default();
    let mut step = ChunkStep::new(
        step_name(),
        ChunkSize::new(10).expect("valid chunk size"),
        Reader::new([1], &trace),
        Processor {
            trace: Arc::clone(&trace),
        },
        Writer {
            boundary: Boundary::Normal,
            trace: Arc::clone(&trace),
        },
        Arc::new(Transactions {
            receipt: receipt(),
            trace: Arc::clone(&trace),
        }),
        Arc::new(NoopCompletion),
    )
    .with_item_stream(
        ComponentStreamIdentity::new("stream_a").expect("valid namespace"),
        RecordingStream::new("a", &trace),
        minimal_contract(),
    )
    .with_item_stream(
        ComponentStreamIdentity::new("stream_b").expect("valid namespace"),
        RecordingStream::new("b", &trace),
        minimal_contract(),
    )
    .with_item_stream(
        ComponentStreamIdentity::new("stream_c").expect("valid namespace"),
        RecordingStream::new("c", &trace),
        minimal_contract(),
    );
    let (_source, stop_token) = StopSource::new();

    let _report = step.execute(&correlation(), &stop_token).await;

    let events = trace_of(&trace);
    let closes: Vec<&String> = events
        .iter()
        .filter(|event| event.ends_with(".close"))
        .collect();
    assert_eq!(
        closes,
        vec!["c.close", "b.close", "a.close"],
        "trace: {events:?}"
    );
}

#[tokio::test]
async fn open_failure_closes_only_previously_opened_streams() {
    let trace = Trace::default();
    let mut step = ChunkStep::new(
        step_name(),
        ChunkSize::new(10).expect("valid chunk size"),
        Reader::new([1], &trace),
        Processor {
            trace: Arc::clone(&trace),
        },
        Writer {
            boundary: Boundary::Normal,
            trace: Arc::clone(&trace),
        },
        Arc::new(Transactions {
            receipt: receipt(),
            trace: Arc::clone(&trace),
        }),
        Arc::new(NoopCompletion),
    )
    .with_item_stream(
        ComponentStreamIdentity::new("stream_a").expect("valid namespace"),
        RecordingStream::new("a", &trace),
        minimal_contract(),
    )
    .with_item_stream(
        ComponentStreamIdentity::new("stream_b").expect("valid namespace"),
        RecordingStream::new("b", &trace).failing_open(),
        minimal_contract(),
    );
    let (_source, stop_token) = StopSource::new();

    let report = step.execute(&correlation(), &stop_token).await;

    assert_eq!(
        report.outcome(),
        ChunkExecutionOutcome::Failed(ChunkFailure::StreamOpen)
    );
    let events = trace_of(&trace);
    assert_eq!(
        events,
        vec!["a.open", "b.open", "a.close"],
        "trace: {events:?}"
    );
    assert!(
        !events.iter().any(|event| event == "reader.read"),
        "no component invocation may start when required stream restoration fails: {events:?}"
    );
}

#[tokio::test]
async fn open_failure_preserves_cleanup_close_failure() {
    let trace = Trace::default();
    let mut step = ChunkStep::new(
        step_name(),
        ChunkSize::new(10).expect("valid chunk size"),
        Reader::new([1], &trace),
        Processor {
            trace: Arc::clone(&trace),
        },
        Writer {
            boundary: Boundary::Normal,
            trace: Arc::clone(&trace),
        },
        Arc::new(Transactions {
            receipt: receipt(),
            trace: Arc::clone(&trace),
        }),
        Arc::new(NoopCompletion),
    )
    .with_item_stream(
        ComponentStreamIdentity::new("stream_a").expect("valid namespace"),
        RecordingStream::new("a", &trace).failing_close(),
        minimal_contract(),
    )
    .with_item_stream(
        ComponentStreamIdentity::new("stream_b").expect("valid namespace"),
        RecordingStream::new("b", &trace).failing_open(),
        minimal_contract(),
    );
    let (_source, stop_token) = StopSource::new();

    let report = step.execute(&correlation(), &stop_token).await;

    assert_eq!(
        report.outcome(),
        ChunkExecutionOutcome::Failed(ChunkFailure::StreamOpen)
    );
    assert!(report.stream_close_failed());
    assert_eq!(trace_of(&trace), vec!["a.open", "b.open", "a.close"]);
}

#[tokio::test]
async fn open_failure_cleanup_closes_all_previously_opened_streams() {
    let trace = Trace::default();
    let mut step = ChunkStep::new(
        step_name(),
        ChunkSize::new(10).expect("valid chunk size"),
        Reader::new([1], &trace),
        Processor {
            trace: Arc::clone(&trace),
        },
        Writer {
            boundary: Boundary::Normal,
            trace: Arc::clone(&trace),
        },
        Arc::new(Transactions {
            receipt: receipt(),
            trace: Arc::clone(&trace),
        }),
        Arc::new(NoopCompletion),
    )
    .with_item_stream(
        ComponentStreamIdentity::new("stream_a").expect("valid namespace"),
        RecordingStream::new("a", &trace),
        minimal_contract(),
    )
    .with_item_stream(
        ComponentStreamIdentity::new("stream_b").expect("valid namespace"),
        RecordingStream::new("b", &trace).failing_close(),
        minimal_contract(),
    )
    .with_item_stream(
        ComponentStreamIdentity::new("stream_c").expect("valid namespace"),
        RecordingStream::new("c", &trace).failing_open(),
        minimal_contract(),
    );
    let (_source, stop_token) = StopSource::new();

    let report = step.execute(&correlation(), &stop_token).await;

    assert_eq!(
        report.outcome(),
        ChunkExecutionOutcome::Failed(ChunkFailure::StreamOpen)
    );
    assert!(report.stream_close_failed());
    assert_eq!(
        trace_of(&trace),
        vec!["a.open", "b.open", "c.open", "b.close", "a.close"]
    );
}

#[tokio::test]
async fn validation_failure_preserves_cleanup_close_failure() {
    let trace = Trace::default();
    let step = ChunkStep::new(
        step_name(),
        ChunkSize::new(10).expect("valid chunk size"),
        Reader::new([1], &trace),
        Processor {
            trace: Arc::clone(&trace),
        },
        Writer {
            boundary: Boundary::Normal,
            trace: Arc::clone(&trace),
        },
        Arc::new(InheritedTransactions {
            receipt: receipt(),
            trace: Arc::clone(&trace),
            inherited: vec![envelope_with_schema("stream_b", "test.other")],
        }),
        Arc::new(NoopCompletion),
    )
    .with_item_stream(
        ComponentStreamIdentity::new("stream_a").expect("valid namespace"),
        RecordingStream::new("a", &trace).failing_close(),
        minimal_contract(),
    )
    .with_item_stream(
        ComponentStreamIdentity::new("stream_b").expect("valid namespace"),
        RecordingStream::new("b", &trace),
        minimal_contract(),
    );
    let mut job = ChunkJob::new(
        JobName::new("validation_cleanup_test").expect("valid job name"),
        step,
        DefinitionRevision::new("test-v1").expect("valid definition revision"),
        &stream_components(),
    )
    .expect("stream registration is valid");
    let clock = ManualClock::new(UNIX_EPOCH);
    let ids = DeterministicIds::new(NonZeroU64::MIN);
    let repository = InMemoryJobRepository::new(Arc::new(clock.clone()), Arc::new(ids.clone()));
    let launcher = JobLauncher::new(&repository, &clock, &ids);
    let (_source, stop_token) = StopSource::new();

    let launch = launcher
        .launch_chunk(&mut job, &JobParameters::new(), &stop_token)
        .await
        .expect("launch completes");
    let report = launch.chunk().expect("chunk work started");

    assert_eq!(
        report.outcome(),
        ChunkExecutionOutcome::Failed(ChunkFailure::StreamOpen)
    );
    assert!(report.stream_close_failed());
    assert_eq!(trace_of(&trace), vec!["a.open", "a.close"]);
}

#[tokio::test]
async fn close_failure_does_not_skip_remaining_closes() {
    let trace = Trace::default();
    let mut step = ChunkStep::new(
        step_name(),
        ChunkSize::new(10).expect("valid chunk size"),
        Reader::new([1], &trace),
        Processor {
            trace: Arc::clone(&trace),
        },
        Writer {
            boundary: Boundary::Normal,
            trace: Arc::clone(&trace),
        },
        Arc::new(Transactions {
            receipt: receipt(),
            trace: Arc::clone(&trace),
        }),
        Arc::new(NoopCompletion),
    )
    .with_item_stream(
        ComponentStreamIdentity::new("stream_a").expect("valid namespace"),
        RecordingStream::new("a", &trace),
        minimal_contract(),
    )
    .with_item_stream(
        ComponentStreamIdentity::new("stream_b").expect("valid namespace"),
        RecordingStream::new("b", &trace).failing_close(),
        minimal_contract(),
    )
    .with_item_stream(
        ComponentStreamIdentity::new("stream_c").expect("valid namespace"),
        RecordingStream::new("c", &trace),
        minimal_contract(),
    );
    let (_source, stop_token) = StopSource::new();

    let report = step.execute(&correlation(), &stop_token).await;

    assert!(report.stream_close_failed());
    let events = trace_of(&trace);
    let closes: Vec<&String> = events
        .iter()
        .filter(|event| event.ends_with(".close"))
        .collect();
    assert_eq!(
        closes,
        vec!["c.close", "b.close", "a.close"],
        "trace: {events:?}"
    );
}

#[tokio::test]
async fn close_failure_does_not_erase_primary_failure() {
    let trace = Trace::default();
    let mut step = ChunkStep::new(
        step_name(),
        ChunkSize::new(10).expect("valid chunk size"),
        Reader::new([1], &trace),
        Processor {
            trace: Arc::clone(&trace),
        },
        Writer {
            boundary: Boundary::Error,
            trace: Arc::clone(&trace),
        },
        Arc::new(FailingWriteTransactions {
            trace: Arc::clone(&trace),
        }),
        Arc::new(NoopCompletion),
    )
    .with_item_stream(
        ComponentStreamIdentity::new("stream").expect("valid namespace"),
        RecordingStream::new("stream", &trace).failing_close(),
        minimal_contract(),
    );
    let (_source, stop_token) = StopSource::new();

    let report = step.execute(&correlation(), &stop_token).await;

    assert_eq!(
        report.outcome(),
        ChunkExecutionOutcome::Failed(ChunkFailure::Writer),
        "a close failure must not erase the earlier primary failure"
    );
    assert!(report.stream_close_failed());
}

#[tokio::test]
async fn close_failure_does_not_erase_committed_chunks() {
    let trace = Trace::default();
    let mut step = ChunkStep::new(
        step_name(),
        ChunkSize::new(1).expect("valid chunk size"),
        Reader::new([1, 2], &trace),
        Processor {
            trace: Arc::clone(&trace),
        },
        Writer {
            boundary: Boundary::Normal,
            trace: Arc::clone(&trace),
        },
        Arc::new(Transactions {
            receipt: receipt(),
            trace: Arc::clone(&trace),
        }),
        Arc::new(NoopCompletion),
    )
    .with_item_stream(
        ComponentStreamIdentity::new("stream").expect("valid namespace"),
        RecordingStream::new("stream", &trace).failing_close(),
        minimal_contract(),
    );
    let (_source, stop_token) = StopSource::new();

    let report = step.execute(&correlation(), &stop_token).await;

    assert_eq!(report.committed_chunks(), ChunkCount::new(2));
    assert!(report.stream_close_failed());
    assert_eq!(
        report.outcome(),
        ChunkExecutionOutcome::Failed(ChunkFailure::StreamClose)
    );
}

/// Corrective-review evidence (PR #161, fix 4): a stream that returns a
/// candidate envelope under a namespace other than its own registered
/// identity is rejected before the candidate ever reaches the durable
/// commit.
#[tokio::test]
async fn stream_update_namespace_mismatch_is_rejected() {
    let trace = Trace::default();
    let mut step = ChunkStep::new(
        step_name(),
        ChunkSize::new(10).expect("valid chunk size"),
        Reader::new([1], &trace),
        Processor {
            trace: Arc::clone(&trace),
        },
        Writer {
            boundary: Boundary::Normal,
            trace: Arc::clone(&trace),
        },
        Arc::new(Transactions {
            receipt: receipt(),
            trace: Arc::clone(&trace),
        }),
        Arc::new(NoopCompletion),
    )
    .with_item_stream(
        ComponentStreamIdentity::new("stream_a").expect("valid namespace"),
        RecordingStream::new("a", &trace).returning_namespace("stream_b"),
        minimal_contract(),
    );
    let (_source, stop_token) = StopSource::new();

    let report = step.execute(&correlation(), &stop_token).await;

    assert_eq!(
        report.outcome(),
        ChunkExecutionOutcome::Failed(ChunkFailure::StreamUpdate)
    );
}

#[tokio::test]
async fn stream_update_namespace_mismatch_does_not_commit_checkpoint() {
    let trace = Trace::default();
    let mut step = ChunkStep::new(
        step_name(),
        ChunkSize::new(10).expect("valid chunk size"),
        Reader::new([1], &trace),
        Processor {
            trace: Arc::clone(&trace),
        },
        Writer {
            boundary: Boundary::Normal,
            trace: Arc::clone(&trace),
        },
        Arc::new(Transactions {
            receipt: receipt(),
            trace: Arc::clone(&trace),
        }),
        Arc::new(NoopCompletion),
    )
    .with_item_stream(
        ComponentStreamIdentity::new("stream_a").expect("valid namespace"),
        RecordingStream::new("a", &trace).returning_namespace("stream_b"),
        minimal_contract(),
    );
    let (_source, stop_token) = StopSource::new();

    let _report = step.execute(&correlation(), &stop_token).await;

    let events = trace_of(&trace);
    assert!(
        !events.iter().any(|event| event == "transaction.commit"),
        "a namespace-mismatched candidate must never reach the durable commit: {events:?}"
    );
    assert!(
        events.iter().any(|event| event == "transaction.rollback"),
        "a rejected update must roll back the open chunk: {events:?}"
    );
}

#[tokio::test]
async fn stream_update_namespace_mismatch_does_not_replace_other_stream_state() {
    let trace = Trace::default();
    let mut step = ChunkStep::new(
        step_name(),
        ChunkSize::new(10).expect("valid chunk size"),
        Reader::new([1], &trace),
        Processor {
            trace: Arc::clone(&trace),
        },
        Writer {
            boundary: Boundary::Normal,
            trace: Arc::clone(&trace),
        },
        Arc::new(Transactions {
            receipt: receipt(),
            trace: Arc::clone(&trace),
        }),
        Arc::new(NoopCompletion),
    )
    .with_item_stream(
        ComponentStreamIdentity::new("stream_b").expect("valid namespace"),
        RecordingStream::new("b", &trace),
        minimal_contract(),
    )
    .with_item_stream(
        ComponentStreamIdentity::new("stream_a").expect("valid namespace"),
        RecordingStream::new("a", &trace).returning_namespace("stream_b"),
        minimal_contract(),
    );
    let (_source, stop_token) = StopSource::new();

    let report = step.execute(&correlation(), &stop_token).await;

    assert_eq!(
        report.outcome(),
        ChunkExecutionOutcome::Failed(ChunkFailure::StreamUpdate)
    );
    let events = trace_of(&trace);
    assert!(
        events.iter().any(|event| event == "b.update"),
        "b's own update must still run before a's mismatch is detected: {events:?}"
    );
    assert!(
        !events.iter().any(|event| event == "transaction.commit"),
        "a's namespace-mismatched candidate must discard b's legitimate candidate too, \
         never partially commit or let a overwrite b: {events:?}"
    );
}

struct NoopCompletion;

impl oxide_batch::ChunkCompletion for NoopCompletion {
    fn after_commit<'a>(
        &'a self,
        _context: oxide_batch::ChunkCompletionContext<'a>,
    ) -> BoxFuture<'a, Result<oxide_batch::ChunkCompletionOutcome, oxide_batch::ChunkCompletionError>>
    {
        Box::pin(async { Ok(oxide_batch::ChunkCompletionOutcome::Acknowledged) })
    }
}

fn correlation() -> oxide_batch::ExecutionCorrelation {
    use std::num::NonZeroU64;

    use oxide_batch::{ExecutionAttempt, JobExecutionId, JobInstanceId, JobName, StepExecutionId};

    let attempt =
        |value: u64| ExecutionAttempt::new(NonZeroU64::new(value).expect("nonzero attempt"));
    oxide_batch::ExecutionCorrelation::new(
        JobName::new("item_stream_job").expect("valid job name"),
        JobInstanceId::new(1).expect("nonzero instance id"),
        JobExecutionId::new(1).expect("nonzero execution id"),
        attempt(1),
        step_name(),
        StepExecutionId::new(1).expect("nonzero execution id"),
        attempt(1),
    )
}
