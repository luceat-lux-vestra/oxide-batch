//! Deterministic chunk-step orchestration and lifecycle integration.

#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::similar_names,
    clippy::type_complexity
)]

#[allow(dead_code)]
#[path = "support/clock.rs"]
mod clock;
#[allow(dead_code)]
#[path = "support/ids.rs"]
mod ids;

use std::collections::VecDeque;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};

use clock::ManualClock;
use ids::DeterministicIds;
use oxide_batch::{
    BatchStatus, BoxFuture, Checkpoint, ChunkAttemptOutcome, ChunkCommitReceipt, ChunkCompletion,
    ChunkCompletionContext, ChunkCompletionError, ChunkCompletionOutcome, ChunkComponentRevisions,
    ChunkCount, ChunkCounts, ChunkDeliveryMode, ChunkExecutionOutcome, ChunkFailure,
    ChunkFaultProgress, ChunkJob, ChunkListener, ChunkListenerContext, ChunkListenerError,
    ChunkRestartContract, ChunkSize, ChunkStep, ChunkTransaction, ChunkTransactionContext,
    ChunkTransactionError, ChunkTransactionManager, CodecId, CodecVersion, CodecVersionUpgrade,
    ComponentRevision, ComponentStateEnvelope, ComponentStreamIdentity, ContentIdentity,
    DefaultComponentCodec, DefinitionError, DefinitionRevision, ExecutionAttempt, ExecutionContext,
    ExecutionCorrelation, ExternalStateReference, FlowExecutionOutcome, FlowGraph, FlowJob,
    FlowLauncher, FlowNode, FlowTarget, InFlightPolicy, InMemoryJobRepository, ItemProcessor,
    ItemReader, ItemStream, ItemWriter, JobExecutionId, JobInstanceId, JobLauncher, JobName,
    JobParameters, LifecycleEvent, LifecycleEventKind, LifecycleEventSink, ListenerContext,
    ListenerError, NodeId, ProcessContext, ProcessOutcome, ProcessorError, ReadContext,
    ReadOutcome, ReaderError, RestartabilityDeclaration, StateCodecError, StateLimits,
    StateSchemaId, StateSchemaUpgrade, StateSchemaVersion, StepComponents, StepExecutionId,
    StepExecutionListener, StepName, StepNode, StopSource, StreamCloseContext, StreamCloseError,
    StreamCloseOutcome, StreamOpenContext, StreamOpenError, StreamOpenOutcome, StreamStateContract,
    StreamUpdateContext, StreamUpdateError, TaskletExecutionOutcome, TerminalKind,
    VersionedStateCodec, WriteContext, WriteOutcome, WriterError,
};

fn correlation() -> ExecutionCorrelation {
    let attempt = |value: u64| {
        ExecutionAttempt::new(NonZeroU64::new(value).expect("static attempt is nonzero"))
    };
    ExecutionCorrelation::new(
        JobName::new("standalone_chunk").expect("static job name is valid"),
        JobInstanceId::new(1).expect("static instance id is nonzero"),
        JobExecutionId::new(1).expect("static execution id is nonzero"),
        attempt(1),
        StepName::new("standalone_step").expect("static step name is valid"),
        StepExecutionId::new(1).expect("static execution id is nonzero"),
        attempt(1),
    )
}

fn chunk_revisions() -> ChunkComponentRevisions {
    chunk_revisions_with_policy(InFlightPolicy::FinishChunk)
}

fn chunk_revisions_with_policy(policy: InFlightPolicy) -> ChunkComponentRevisions {
    ChunkComponentRevisions::new(
        ComponentRevision::new("reader-v1").expect("static reader revision is valid"),
        ComponentRevision::new("processor-v1").expect("static processor revision is valid"),
        ComponentRevision::new("writer-v1").expect("static writer revision is valid"),
        ComponentRevision::new("checkpoint-v1").expect("static checkpoint revision is valid"),
        ChunkRestartContract::new(
            StateSchemaId::new("test.chunk.checkpoint").expect("static schema is valid"),
            StateSchemaVersion::new(1).expect("static schema version is valid"),
            StateSchemaId::new("test.chunk.context").expect("static schema is valid"),
            StateSchemaVersion::new(1).expect("static schema version is valid"),
            ChunkDeliveryMode::AtLeastOnce,
        )
        .with_in_flight_policy(policy),
    )
}

#[derive(Clone, Copy)]
enum Boundary {
    Normal,
    Error,
    Panic,
    Stop,
}

struct Reader {
    items: VecDeque<i32>,
    boundary: Boundary,
}

impl Reader {
    fn new(items: impl IntoIterator<Item = i32>) -> Self {
        Self {
            items: items.into_iter().collect(),
            boundary: Boundary::Normal,
        }
    }

    fn with_boundary(mut self, boundary: Boundary) -> Self {
        self.boundary = boundary;
        self
    }
}

impl ItemReader<i32> for Reader {
    async fn read(&mut self, _context: ReadContext<'_>) -> Result<ReadOutcome<i32>, ReaderError> {
        match self.boundary {
            Boundary::Panic => panic!("reader secret"),
            Boundary::Error => Err(ReaderError::new()),
            Boundary::Stop => Ok(ReadOutcome::Stopped),
            Boundary::Normal => Ok(self
                .items
                .pop_front()
                .map_or(ReadOutcome::EndOfInput, ReadOutcome::Item)),
        }
    }
}

struct ShutdownRequestingReader {
    items: VecDeque<i32>,
    source: Option<StopSource>,
}

impl ItemReader<i32> for ShutdownRequestingReader {
    async fn read(&mut self, _context: ReadContext<'_>) -> Result<ReadOutcome<i32>, ReaderError> {
        if let Some(source) = self.source.take() {
            source.request_stop();
        }
        Ok(self
            .items
            .pop_front()
            .map_or(ReadOutcome::EndOfInput, ReadOutcome::Item))
    }
}

struct Processor {
    boundary: Boundary,
    filter: Option<i32>,
}

impl Processor {
    const fn normal() -> Self {
        Self {
            boundary: Boundary::Normal,
            filter: None,
        }
    }
}

impl ItemProcessor<i32, i32> for Processor {
    async fn process(
        &self,
        item: &i32,
        _context: ProcessContext<'_>,
    ) -> Result<ProcessOutcome<i32>, ProcessorError> {
        match self.boundary {
            Boundary::Panic => panic!("processor secret"),
            Boundary::Error => Err(ProcessorError::new()),
            Boundary::Stop => Ok(ProcessOutcome::Stopped),
            Boundary::Normal if self.filter == Some(*item) => Ok(ProcessOutcome::Filtered),
            Boundary::Normal => Ok(ProcessOutcome::Item(item * 10)),
        }
    }
}

struct Writer {
    boundary: Boundary,
    batches: Arc<Mutex<Vec<Vec<i32>>>>,
}

impl Writer {
    fn new(boundary: Boundary) -> (Self, Arc<Mutex<Vec<Vec<i32>>>>) {
        let batches = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                boundary,
                batches: Arc::clone(&batches),
            },
            batches,
        )
    }
}

impl ItemWriter<i32> for Writer {
    async fn write(
        &self,
        items: &[i32],
        _context: WriteContext<'_>,
    ) -> Result<WriteOutcome, WriterError> {
        match self.boundary {
            Boundary::Panic => panic!("writer secret"),
            Boundary::Error => Err(WriterError::new()),
            Boundary::Stop => Ok(WriteOutcome::Stopped),
            Boundary::Normal => {
                self.batches
                    .lock()
                    .expect("writer batches lock poisoned")
                    .push(items.to_vec());
                Ok(WriteOutcome::Written)
            }
        }
    }
}

struct Completion {
    boundary: Boundary,
    calls: Arc<Mutex<Vec<ChunkCounts>>>,
}

impl Completion {
    fn new(boundary: Boundary) -> (Self, Arc<Mutex<Vec<ChunkCounts>>>) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                boundary,
                calls: Arc::clone(&calls),
            },
            calls,
        )
    }
}

impl ChunkCompletion for Completion {
    fn after_commit<'a>(
        &'a self,
        context: ChunkCompletionContext<'a>,
    ) -> BoxFuture<'a, Result<ChunkCompletionOutcome, ChunkCompletionError>> {
        self.calls
            .lock()
            .expect("completion calls lock poisoned")
            .push(context.counts());
        match self.boundary {
            Boundary::Panic => panic!("completion secret"),
            Boundary::Error => Box::pin(async { Err(ChunkCompletionError::new()) }),
            Boundary::Stop => Box::pin(async { Ok(ChunkCompletionOutcome::StoppedAfterCommit) }),
            Boundary::Normal => Box::pin(async { Ok(ChunkCompletionOutcome::Acknowledged) }),
        }
    }
}

#[derive(Default)]
struct TransactionEvidence {
    commits: Mutex<Vec<ChunkCounts>>,
    rollbacks: Mutex<u64>,
}

struct Transactions {
    receipt: ChunkCommitReceipt,
    evidence: Arc<TransactionEvidence>,
    commit_error: Option<ChunkTransactionError>,
}

impl ChunkTransactionManager for Transactions {
    fn begin(
        &self,
    ) -> BoxFuture<'_, Result<Box<dyn ChunkTransaction + '_>, ChunkTransactionError>> {
        let transaction = TestTransaction {
            receipt: self.receipt.clone(),
            evidence: Arc::clone(&self.evidence),
            commit_error: self.commit_error,
        };
        Box::pin(async move { Ok(Box::new(transaction) as Box<dyn ChunkTransaction>) })
    }
}

struct TestTransaction {
    receipt: ChunkCommitReceipt,
    evidence: Arc<TransactionEvidence>,
    commit_error: Option<ChunkTransactionError>,
}

impl ChunkTransaction for TestTransaction {
    fn business_transaction(&mut self) -> Option<&mut dyn oxide_batch::BusinessTransaction> {
        None
    }

    fn commit(
        &mut self,
        counts: ChunkCounts,
        _fault: oxide_batch::ChunkFaultProgress,
    ) -> BoxFuture<'_, Result<ChunkCommitReceipt, ChunkTransactionError>> {
        if let Some(error) = self.commit_error {
            return Box::pin(async move { Err(error) });
        }
        self.evidence
            .commits
            .lock()
            .expect("commit evidence lock poisoned")
            .push(counts);
        let receipt = self.receipt.clone();
        Box::pin(async move { Ok(receipt) })
    }

    fn rollback(&mut self) -> BoxFuture<'_, Result<(), ChunkTransactionError>> {
        let mut rollbacks = self
            .evidence
            .rollbacks
            .lock()
            .expect("rollback evidence lock poisoned");
        *rollbacks += 1;
        Box::pin(async { Ok(()) })
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

fn step(
    reader: Reader,
    processor: Processor,
    writer_boundary: Boundary,
    completion_boundary: Boundary,
    commit_error: Option<ChunkTransactionError>,
) -> (
    ChunkStep<i32, i32, Reader, Processor, Writer>,
    Arc<Mutex<Vec<Vec<i32>>>>,
    Arc<Mutex<Vec<ChunkCounts>>>,
    Arc<TransactionEvidence>,
) {
    let (writer, batches) = Writer::new(writer_boundary);
    let (completion, calls) = Completion::new(completion_boundary);
    let evidence = Arc::new(TransactionEvidence::default());
    let transactions = Transactions {
        receipt: receipt(),
        evidence: Arc::clone(&evidence),
        commit_error,
    };
    let step = ChunkStep::new(
        StepName::new("import").expect("valid step name"),
        ChunkSize::new(2).expect("valid chunk size"),
        reader,
        processor,
        writer,
        Arc::new(transactions),
        Arc::new(completion),
    );
    (step, batches, calls, evidence)
}

#[tokio::test]
async fn partial_final_chunk_commits_checked_counts() {
    let (mut step, batches, completions, evidence) = step(
        Reader::new([1, 2, 3]),
        Processor {
            boundary: Boundary::Normal,
            filter: Some(2),
        },
        Boundary::Normal,
        Boundary::Normal,
        None,
    );
    let (_source, stop) = StopSource::new();

    let report = step.execute(&correlation(), &stop).await;

    assert_eq!(report.outcome(), ChunkExecutionOutcome::Completed);
    assert_eq!(report.committed_chunks(), ChunkCount::new(2));
    assert_eq!(
        report.committed_counts(),
        ChunkCounts::new(
            ChunkCount::new(3),
            ChunkCount::new(2),
            ChunkCount::new(2),
            ChunkCount::new(1),
        )
        .expect("aggregate counts must be valid")
    );
    assert_eq!(
        *batches.lock().expect("writer batches lock poisoned"),
        vec![vec![10], vec![30]]
    );
    assert_eq!(
        completions
            .lock()
            .expect("completion calls lock poisoned")
            .len(),
        2
    );
    assert_eq!(
        evidence
            .commits
            .lock()
            .expect("commit evidence lock poisoned")
            .len(),
        2
    );
}

#[tokio::test]
async fn empty_input_completes_without_committed_or_rolled_back_counts() {
    let (mut step, batches, completions, _evidence) = step(
        Reader::new([]),
        Processor::normal(),
        Boundary::Normal,
        Boundary::Normal,
        None,
    );
    let (_source, stop) = StopSource::new();

    let report = step.execute(&correlation(), &stop).await;

    assert_eq!(report.outcome(), ChunkExecutionOutcome::Completed);
    assert_eq!(report.committed_counts(), ChunkCounts::default());
    assert_eq!(report.committed_chunks(), ChunkCount::ZERO);
    assert_eq!(report.rolled_back_chunks(), ChunkCount::ZERO);
    assert!(
        batches
            .lock()
            .expect("writer batches lock poisoned")
            .is_empty()
    );
    assert!(
        completions
            .lock()
            .expect("completion calls lock poisoned")
            .is_empty()
    );
}

async fn assert_component_error_rolls_back(
    reader_boundary: Boundary,
    processor_boundary: Boundary,
    writer_boundary: Boundary,
    expected: ChunkFailure,
) {
    let (mut step, _batches, _completions, evidence) = step(
        Reader::new([1]).with_boundary(reader_boundary),
        Processor {
            boundary: processor_boundary,
            filter: None,
        },
        writer_boundary,
        Boundary::Normal,
        None,
    );
    let (_source, stop) = StopSource::new();

    let report = step.execute(&correlation(), &stop).await;

    assert_eq!(report.outcome(), ChunkExecutionOutcome::Failed(expected));
    assert_eq!(report.committed_counts(), ChunkCounts::default());
    assert_eq!(report.rolled_back_chunks(), ChunkCount::new(1));
    assert!(
        evidence
            .commits
            .lock()
            .expect("commit evidence lock poisoned")
            .is_empty()
    );
}

#[tokio::test]
async fn reader_failure_preserves_checkpoint() {
    assert_component_error_rolls_back(
        Boundary::Error,
        Boundary::Normal,
        Boundary::Normal,
        ChunkFailure::Reader,
    )
    .await;
}

#[tokio::test]
async fn processor_failure_rolls_back_chunk() {
    assert_component_error_rolls_back(
        Boundary::Normal,
        Boundary::Error,
        Boundary::Normal,
        ChunkFailure::Processor,
    )
    .await;
}

#[tokio::test]
async fn writer_failure_rolls_back_open_chunk() {
    assert_component_error_rolls_back(
        Boundary::Normal,
        Boundary::Normal,
        Boundary::Error,
        ChunkFailure::Writer,
    )
    .await;
}

#[tokio::test]
async fn component_panics_are_typed_and_payload_redacted() {
    for (reader_boundary, processor_boundary, writer_boundary, expected) in [
        (
            Boundary::Panic,
            Boundary::Normal,
            Boundary::Normal,
            ChunkFailure::ReaderPanic,
        ),
        (
            Boundary::Normal,
            Boundary::Panic,
            Boundary::Normal,
            ChunkFailure::ProcessorPanic,
        ),
        (
            Boundary::Normal,
            Boundary::Normal,
            Boundary::Panic,
            ChunkFailure::WriterPanic,
        ),
    ] {
        let (mut step, _batches, _completions, _evidence) = step(
            Reader::new([1]).with_boundary(reader_boundary),
            Processor {
                boundary: processor_boundary,
                filter: None,
            },
            writer_boundary,
            Boundary::Normal,
            None,
        );
        let (_source, stop) = StopSource::new();

        let report = step.execute(&correlation(), &stop).await;

        assert_eq!(report.outcome(), ChunkExecutionOutcome::Failed(expected));
        assert!(!format!("{report:?}").contains("secret"));
        assert_eq!(report.committed_counts(), ChunkCounts::default());
    }
}

#[tokio::test]
async fn stop_during_chunk_uses_commit_boundary() {
    let (mut step, _batches, _completions, evidence) = step(
        Reader::new([1]).with_boundary(Boundary::Stop),
        Processor::normal(),
        Boundary::Normal,
        Boundary::Normal,
        None,
    );
    let (_source, stop) = StopSource::new();

    let report = step.execute(&correlation(), &stop).await;

    assert_eq!(report.outcome(), ChunkExecutionOutcome::Stopped);
    assert_eq!(report.committed_counts(), ChunkCounts::default());
    assert_eq!(report.rolled_back_chunks(), ChunkCount::new(1));
    assert!(
        evidence
            .commits
            .lock()
            .expect("commit evidence lock poisoned")
            .is_empty()
    );
}

#[tokio::test]
async fn late_completion_failure_cannot_undo_committed_chunk() {
    let (mut step, batches, _completions, evidence) = step(
        Reader::new([1]),
        Processor::normal(),
        Boundary::Normal,
        Boundary::Error,
        None,
    );
    let (_source, stop) = StopSource::new();

    let report = step.execute(&correlation(), &stop).await;

    assert_eq!(
        report.outcome(),
        ChunkExecutionOutcome::Failed(ChunkFailure::Completion)
    );
    assert_eq!(report.committed_chunks(), ChunkCount::new(1));
    assert_eq!(report.committed_counts().written(), ChunkCount::new(1));
    assert_eq!(
        *batches.lock().expect("writer batches lock poisoned"),
        vec![vec![10]]
    );
    assert_eq!(
        evidence
            .commits
            .lock()
            .expect("commit evidence lock poisoned")
            .len(),
        1
    );
}

#[tokio::test]
async fn stop_acknowledged_after_commit_retains_committed_chunk() {
    let (mut step, _batches, _completions, _evidence) = step(
        Reader::new([1]),
        Processor::normal(),
        Boundary::Normal,
        Boundary::Stop,
        None,
    );
    let (_source, stop) = StopSource::new();

    let report = step.execute(&correlation(), &stop).await;

    assert_eq!(report.outcome(), ChunkExecutionOutcome::Stopped);
    assert_eq!(report.committed_chunks(), ChunkCount::new(1));
    assert_eq!(report.committed_counts().written(), ChunkCount::new(1));
    assert_eq!(report.rolled_back_chunks(), ChunkCount::ZERO);
}

#[tokio::test]
async fn declared_in_flight_policy_commits_or_rolls_back_the_open_chunk() {
    for (policy, committed, rolled_back) in [
        (InFlightPolicy::FinishChunk, 1, 0),
        (InFlightPolicy::RollbackChunk, 0, 1),
    ] {
        let (source, stop) = StopSource::new();
        let (writer, _batches) = Writer::new(Boundary::Normal);
        let (completion, _calls) = Completion::new(Boundary::Normal);
        let evidence = Arc::new(TransactionEvidence::default());
        let transactions = Transactions {
            receipt: receipt(),
            evidence: Arc::clone(&evidence),
            commit_error: None,
        };
        let step = ChunkStep::new(
            StepName::new("import").expect("valid step name"),
            ChunkSize::new(2).expect("valid chunk size"),
            ShutdownRequestingReader {
                items: [1].into_iter().collect(),
                source: Some(source),
            },
            Processor::normal(),
            writer,
            Arc::new(transactions),
            Arc::new(completion),
        );
        let mut job = ChunkJob::new(
            JobName::new(format!("shutdown_{policy:?}")).expect("valid job name"),
            step,
            DefinitionRevision::new("test-v1").expect("valid revision"),
            &chunk_revisions_with_policy(policy),
        )
        .expect("valid chunk definition");
        let clock = ManualClock::new(UNIX_EPOCH + Duration::from_secs(500));
        let ids = DeterministicIds::new(NonZeroU64::MIN);
        let repository = InMemoryJobRepository::new(Arc::new(clock.clone()), Arc::new(ids.clone()));
        let launcher = JobLauncher::new(&repository, &clock, &ids);

        let report = launcher
            .launch_chunk(&mut job, &JobParameters::new(), &stop)
            .await
            .expect("shutdown produces a durable report");
        let chunk = report.chunk().expect("chunk work started");

        assert_eq!(chunk.outcome(), ChunkExecutionOutcome::Stopped);
        assert_eq!(chunk.committed_chunks().get(), committed);
        assert_eq!(chunk.rolled_back_chunks().get(), rolled_back);
        assert_eq!(
            evidence.commits.lock().expect("commit evidence lock").len(),
            usize::try_from(committed).expect("small static count fits usize")
        );
    }
}

struct OrderedListener {
    name: &'static str,
    events: Arc<Mutex<Vec<String>>>,
    after_error: bool,
}

impl ChunkListener for OrderedListener {
    fn before_chunk<'a>(
        &'a self,
        _context: ChunkListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ChunkListenerError>> {
        self.events
            .lock()
            .expect("listener events lock poisoned")
            .push(format!("before:{}", self.name));
        Box::pin(async { Ok(()) })
    }

    fn after_chunk<'a>(
        &'a self,
        _context: ChunkListenerContext<'a>,
        outcome: ChunkAttemptOutcome,
    ) -> BoxFuture<'a, Result<(), ChunkListenerError>> {
        self.events
            .lock()
            .expect("listener events lock poisoned")
            .push(format!("after:{}:{outcome:?}", self.name));
        if self.after_error {
            Box::pin(async { Err(ChunkListenerError::new()) })
        } else {
            Box::pin(async { Ok(()) })
        }
    }
}

struct PanickingListener {
    before: bool,
}

impl ChunkListener for PanickingListener {
    fn before_chunk<'a>(
        &'a self,
        _context: ChunkListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ChunkListenerError>> {
        assert!(!self.before, "before-listener secret");
        Box::pin(async { Ok(()) })
    }

    fn after_chunk<'a>(
        &'a self,
        _context: ChunkListenerContext<'a>,
        _outcome: ChunkAttemptOutcome,
    ) -> BoxFuture<'a, Result<(), ChunkListenerError>> {
        if self.before {
            Box::pin(async { Ok(()) })
        } else {
            panic!("after-listener secret");
        }
    }
}

#[tokio::test]
async fn listener_failure_preserves_committed_work() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let (step, _batches, _completions, _evidence) = step(
        Reader::new([1]),
        Processor::normal(),
        Boundary::Normal,
        Boundary::Normal,
        None,
    );
    let mut step = step
        .with_chunk_listener(Arc::new(OrderedListener {
            name: "outer",
            events: Arc::clone(&events),
            after_error: false,
        }))
        .with_chunk_listener(Arc::new(OrderedListener {
            name: "inner",
            events: Arc::clone(&events),
            after_error: true,
        }));
    let (_source, stop) = StopSource::new();

    let report = step.execute(&correlation(), &stop).await;

    assert_eq!(
        *events.lock().expect("listener events lock poisoned"),
        vec![
            "before:outer",
            "before:inner",
            "after:inner:Committed",
            "after:outer:Committed",
        ]
    );
    assert_eq!(
        report.outcome(),
        ChunkExecutionOutcome::Failed(ChunkFailure::Listener)
    );
    assert_eq!(report.committed_chunks(), ChunkCount::new(1));
    assert_eq!(report.listener_failures().len(), 1);
}

#[tokio::test]
async fn chunk_listener_panics_are_typed_at_both_boundaries() {
    for before in [true, false] {
        let (step, _batches, _completions, _evidence) = step(
            Reader::new([1]),
            Processor::normal(),
            Boundary::Normal,
            Boundary::Normal,
            None,
        );
        let mut step = step.with_chunk_listener(Arc::new(PanickingListener { before }));
        let (_source, stop) = StopSource::new();

        let report = step.execute(&correlation(), &stop).await;

        assert_eq!(
            report.outcome(),
            ChunkExecutionOutcome::Failed(ChunkFailure::ListenerPanic)
        );
        assert!(!format!("{report:?}").contains("secret"));
        assert_eq!(
            report.committed_chunks(),
            if before {
                ChunkCount::ZERO
            } else {
                ChunkCount::new(1)
            }
        );
    }
}

#[tokio::test]
async fn unknown_commit_is_not_rolled_back_or_guessed() {
    let (mut step, _batches, _completions, evidence) = step(
        Reader::new([1]),
        Processor::normal(),
        Boundary::Normal,
        Boundary::Normal,
        Some(ChunkTransactionError::CommitOutcomeUnknown),
    );
    let (_source, stop) = StopSource::new();

    let report = step.execute(&correlation(), &stop).await;

    assert_eq!(report.outcome(), ChunkExecutionOutcome::Unknown);
    assert_eq!(report.committed_counts(), ChunkCounts::default());
    assert_eq!(report.rolled_back_chunks(), ChunkCount::ZERO);
    assert_eq!(
        *evidence
            .rollbacks
            .lock()
            .expect("rollback evidence lock poisoned"),
        0
    );
}

#[tokio::test]
async fn job_launcher_persists_chunk_step_lifecycle() {
    let (step, _batches, _completions, _evidence) = step(
        Reader::new([1, 2, 3]),
        Processor::normal(),
        Boundary::Normal,
        Boundary::Normal,
        None,
    );
    let mut job = ChunkJob::new(
        JobName::new("daily_import").expect("valid job name"),
        step,
        DefinitionRevision::new("test-v1").expect("static definition revision is valid"),
        &chunk_revisions(),
    )
    .expect("static chunk definition is valid");
    let clock = ManualClock::new(UNIX_EPOCH + Duration::from_secs(100));
    let ids = DeterministicIds::new(NonZeroU64::MIN);
    let repository = InMemoryJobRepository::new(Arc::new(clock.clone()), Arc::new(ids.clone()));
    let events = EventRecorder::default();
    let launcher = JobLauncher::new(&repository, &clock, &ids).with_event_sink(&events);
    let (_source, stop) = StopSource::new();

    let report = launcher
        .launch_chunk(&mut job, &JobParameters::new(), &stop)
        .await
        .expect("chunk launch must complete");

    assert_eq!(
        report.launch().outcome(),
        TaskletExecutionOutcome::Completed
    );
    assert_eq!(
        report.launch().job_execution().metadata().status(),
        BatchStatus::Completed
    );
    assert_eq!(
        report.launch().step_execution().metadata().status(),
        BatchStatus::Completed
    );
    let chunk = report.chunk().expect("chunk body must have run");
    assert_eq!(chunk.outcome(), ChunkExecutionOutcome::Completed);
    assert_eq!(chunk.committed_counts().read(), ChunkCount::new(3));
    assert_eq!(chunk.committed_chunks(), ChunkCount::new(2));
    let chunk_events: Vec<_> = events
        .0
        .lock()
        .expect("event recorder lock poisoned")
        .iter()
        .copied()
        .filter(|(kind, _)| {
            matches!(
                kind,
                LifecycleEventKind::ChunkStarted
                    | LifecycleEventKind::ChunkCommitted
                    | LifecycleEventKind::ChunkRolledBack
                    | LifecycleEventKind::ChunkUnknown
            )
        })
        .collect();
    assert_eq!(
        chunk_events,
        vec![
            (LifecycleEventKind::ChunkStarted, Some(1)),
            (LifecycleEventKind::ChunkCommitted, Some(1)),
            (LifecycleEventKind::ChunkStarted, Some(2)),
            (LifecycleEventKind::ChunkCommitted, Some(2)),
        ]
    );
}

#[tokio::test]
async fn terminal_known_rollback_commits_with_failed_step_lifecycle() {
    let (step, _batches, _completions, _evidence) = step(
        Reader::new([1]),
        Processor::normal(),
        Boundary::Error,
        Boundary::Normal,
        None,
    );
    let mut job = ChunkJob::new(
        JobName::new("terminal_rollback").expect("valid job name"),
        step,
        DefinitionRevision::new("test-v1").expect("static definition revision is valid"),
        &chunk_revisions(),
    )
    .expect("static chunk definition is valid");
    let clock = ManualClock::new(UNIX_EPOCH + Duration::from_secs(150));
    let ids = DeterministicIds::new(NonZeroU64::MIN);
    let repository = InMemoryJobRepository::new(Arc::new(clock.clone()), Arc::new(ids.clone()));
    let launcher = JobLauncher::new(&repository, &clock, &ids);
    let (_source, stop) = StopSource::new();

    let report = launcher
        .launch_chunk(&mut job, &JobParameters::new(), &stop)
        .await
        .expect("known rollback must persist a failed lifecycle");

    assert!(matches!(
        report.chunk().expect("chunk body must run").outcome(),
        ChunkExecutionOutcome::Failed(_)
    ));
    assert_eq!(
        report
            .launch()
            .step_execution()
            .metadata()
            .counts()
            .rolled_back(),
        1
    );
}

#[tokio::test]
async fn flow_launcher_executes_a_bound_chunk_step() {
    let (step, _batches, _completions, evidence) = step(
        Reader::new([1, 2, 3]),
        Processor::normal(),
        Boundary::Normal,
        Boundary::Normal,
        None,
    );
    let revisions = chunk_revisions();
    let node = NodeId::new("import").expect("static node ID is valid");
    let name = JobName::new("flow_chunk").expect("static job name is valid");
    let plan = FlowGraph::new(node.clone())
        .with_node(FlowNode::step(StepNode::new(
            node.clone(),
            StepName::new("import").expect("static step name is valid"),
            StepComponents::Chunk {
                size: ChunkSize::new(2).expect("static chunk size is nonzero"),
                revisions: Box::new(revisions.clone()),
            },
        )))
        .with_sequence(node.clone(), FlowTarget::Terminal(TerminalKind::Complete))
        .expect("static sequence is valid")
        .compile(
            &name,
            DefinitionRevision::new("flow-v1").expect("static revision is valid"),
        )
        .expect("static flow compiles");
    let job = FlowJob::new(name, plan)
        .expect("format-2 flow is valid")
        .with_chunk_step(node, step, &revisions)
        .expect("chunk declaration matches the plan");
    let clock = ManualClock::new(UNIX_EPOCH + Duration::from_secs(200));
    let ids = DeterministicIds::new(NonZeroU64::MIN);
    let repository = InMemoryJobRepository::new(Arc::new(clock.clone()), Arc::new(ids.clone()));
    let (_source, stop) = StopSource::new();

    let report = FlowLauncher::new(&repository, &clock, &ids)
        .launch(&job, &JobParameters::new(), &stop)
        .await
        .expect("flow chunk launch must complete");

    assert_eq!(report.outcome(), &FlowExecutionOutcome::Completed);
    assert_eq!(report.step_executions().len(), 1);
    assert_eq!(
        report.step_executions()[0].metadata().status(),
        BatchStatus::Completed
    );
    assert_eq!(
        evidence
            .commits
            .lock()
            .expect("commit evidence lock poisoned")
            .len(),
        2
    );
}

#[tokio::test]
async fn flow_launcher_persists_a_bound_chunk_terminal_rollback() {
    let (step, _batches, _completions, _evidence) = step(
        Reader::new([1]),
        Processor::normal(),
        Boundary::Error,
        Boundary::Normal,
        None,
    );
    let revisions = chunk_revisions();
    let node = NodeId::new("import").expect("static node ID is valid");
    let name = JobName::new("flow_chunk_rollback").expect("static job name is valid");
    let plan = FlowGraph::new(node.clone())
        .with_node(FlowNode::step(StepNode::new(
            node.clone(),
            StepName::new("import").expect("static step name is valid"),
            StepComponents::Chunk {
                size: ChunkSize::new(2).expect("static chunk size is nonzero"),
                revisions: Box::new(revisions.clone()),
            },
        )))
        .with_sequence(node.clone(), FlowTarget::Terminal(TerminalKind::Complete))
        .expect("static sequence is valid")
        .compile(
            &name,
            DefinitionRevision::new("flow-v1").expect("static revision is valid"),
        )
        .expect("static flow compiles");
    let job = FlowJob::new(name, plan)
        .expect("format-2 flow is valid")
        .with_chunk_step(node, step, &revisions)
        .expect("chunk declaration matches the plan");
    let clock = ManualClock::new(UNIX_EPOCH + Duration::from_secs(250));
    let ids = DeterministicIds::new(NonZeroU64::MIN);
    let repository = InMemoryJobRepository::new(Arc::new(clock.clone()), Arc::new(ids.clone()));
    let (_source, stop) = StopSource::new();

    let report = FlowLauncher::new(&repository, &clock, &ids)
        .launch(&job, &JobParameters::new(), &stop)
        .await
        .expect("flow chunk failure must produce a durable report");

    assert!(matches!(report.outcome(), FlowExecutionOutcome::Failed(_)));
    assert_eq!(report.step_executions().len(), 1);
    assert_eq!(
        report.step_executions()[0]
            .metadata()
            .counts()
            .rolled_back(),
        1
    );
}

#[derive(Default)]
struct EventRecorder(Mutex<Vec<(LifecycleEventKind, Option<u64>)>>);

impl LifecycleEventSink for EventRecorder {
    fn emit(&self, event: &LifecycleEvent) {
        self.0
            .lock()
            .expect("event recorder lock poisoned")
            .push((event.kind(), event.chunk_sequence().map(ChunkCount::get)));
    }
}

struct AfterStepError;

impl StepExecutionListener for AfterStepError {
    fn before_step<'a>(
        &'a self,
        _context: ListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        Box::pin(async { Ok(()) })
    }

    fn after_step<'a>(
        &'a self,
        _context: ListenerContext<'a>,
        _outcome: TaskletExecutionOutcome,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        Box::pin(async { Err(ListenerError::new()) })
    }
}

#[tokio::test]
async fn unknown_chunk_commit_persists_unknown_lifecycle() {
    let (step, _batches, _completions, _evidence) = step(
        Reader::new([1]),
        Processor::normal(),
        Boundary::Normal,
        Boundary::Normal,
        Some(ChunkTransactionError::CommitOutcomeUnknown),
    );
    let step = step.with_listener(Arc::new(AfterStepError));
    let mut job = ChunkJob::new(
        JobName::new("ambiguous_import").expect("valid job name"),
        step,
        DefinitionRevision::new("test-v1").expect("static definition revision is valid"),
        &chunk_revisions(),
    )
    .expect("static chunk definition is valid");
    let clock = ManualClock::new(UNIX_EPOCH + Duration::from_secs(200));
    let ids = DeterministicIds::new(NonZeroU64::MIN);
    let repository = InMemoryJobRepository::new(Arc::new(clock.clone()), Arc::new(ids.clone()));
    let events = EventRecorder::default();
    let launcher = JobLauncher::new(&repository, &clock, &ids).with_event_sink(&events);
    let (_source, stop) = StopSource::new();

    let report = launcher
        .launch_chunk(&mut job, &JobParameters::new(), &stop)
        .await
        .expect("unknown commit must still produce a launch report");

    assert_eq!(report.launch().outcome(), TaskletExecutionOutcome::Unknown);
    assert_eq!(
        report.launch().job_execution().metadata().status(),
        BatchStatus::Unknown
    );
    assert_eq!(
        report.launch().step_execution().metadata().status(),
        BatchStatus::Unknown
    );
    assert_eq!(
        report.chunk().expect("chunk body must have run").outcome(),
        ChunkExecutionOutcome::Unknown
    );
    assert_eq!(report.launch().listener_failures().len(), 1);
    let events = events.0.lock().expect("event recorder lock poisoned");
    assert!(events.contains(&(LifecycleEventKind::ChunkUnknown, Some(1))));
    assert!(events.contains(&(LifecycleEventKind::StepUnknown, None)));
    assert!(events.contains(&(LifecycleEventKind::JobUnknown, None)));
}

#[tokio::test]
async fn launch_without_chunk_body_does_not_reuse_a_prior_chunk_report() {
    let (step, _batches, _completions, _evidence) = step(
        Reader::new([1]).with_boundary(Boundary::Stop),
        Processor::normal(),
        Boundary::Normal,
        Boundary::Normal,
        None,
    );
    let mut job = ChunkJob::new(
        JobName::new("restartable_import").expect("valid job name"),
        step,
        DefinitionRevision::new("test-v1").expect("static definition revision is valid"),
        &chunk_revisions(),
    )
    .expect("static chunk definition is valid");
    let clock = ManualClock::new(UNIX_EPOCH + Duration::from_secs(301));
    let ids = DeterministicIds::new(NonZeroU64::MIN);
    let repository = InMemoryJobRepository::new(Arc::new(clock.clone()), Arc::new(ids.clone()));
    let launcher = JobLauncher::new(&repository, &clock, &ids);
    let (_first_source, first_stop) = StopSource::new();

    let first = launcher
        .launch_chunk(&mut job, &JobParameters::new(), &first_stop)
        .await
        .expect("first launch must stop in chunk work");
    assert!(first.chunk().is_some());

    let (second_source, second_stop) = StopSource::new();
    second_source.request_stop();
    let second = launcher
        .launch_chunk(&mut job, &JobParameters::new(), &second_stop)
        .await
        .expect("second launch must stop before chunk work");

    assert_eq!(
        second.launch().outcome(),
        TaskletExecutionOutcome::Stopped(oxide_batch::StopTiming::BeforeStart)
    );
    assert!(second.chunk().is_none());
}

/// Corrective-review evidence for the PR #161 fixes: runtime/manifest stream
/// identity bijection (fix 3), and pre-`open` schema/codec/restartability
/// contract enforcement (fix 5).
mod stream_contract {
    use super::{
        Arc, AtomicUsize, Boundary, BoxFuture, ChunkComponentRevisions, ChunkCounts,
        ChunkExecutionOutcome, ChunkFailure, ChunkFaultProgress, ChunkJob, ChunkSize, ChunkStep,
        ChunkTransaction, ChunkTransactionContext, ChunkTransactionError, ChunkTransactionManager,
        CodecId, CodecVersion, CodecVersionUpgrade, Completion, ComponentRevision,
        ComponentStateEnvelope, ComponentStreamIdentity, ContentIdentity, DefaultComponentCodec,
        DefinitionError, DefinitionRevision, DeterministicIds, ExternalStateReference, FlowGraph,
        FlowJob, FlowNode, FlowTarget, InMemoryJobRepository, ItemStream, JobLauncher, JobName,
        JobParameters, ManualClock, Mutex, NodeId, NonZeroU64, Ordering, Processor, Reader,
        RestartabilityDeclaration, StateCodecError, StateLimits, StateSchemaId, StateSchemaUpgrade,
        StateSchemaVersion, StepComponents, StepName, StepNode, StopSource, StreamCloseContext,
        StreamCloseError, StreamCloseOutcome, StreamOpenContext, StreamOpenError,
        StreamOpenOutcome, StreamStateContract, StreamUpdateContext, StreamUpdateError,
        TerminalKind, UNIX_EPOCH, VersionedStateCodec, Writer, chunk_revisions, receipt,
    };
    use std::time::Duration;

    const NAMESPACE: &str = "counter";

    fn identity() -> ComponentStreamIdentity {
        ComponentStreamIdentity::new(NAMESPACE).expect("valid namespace")
    }

    struct CounterSchema {
        schema: StateSchemaId,
        version: u32,
        upgrades: Vec<StateSchemaUpgrade>,
    }

    impl CounterSchema {
        fn new(version: u32) -> Self {
            Self {
                schema: StateSchemaId::new("test.stream.counter").expect("valid schema id"),
                version,
                upgrades: Vec::new(),
            }
        }

        fn with_schema(mut self, schema: &str) -> Self {
            self.schema = StateSchemaId::new(schema).expect("valid schema id");
            self
        }

        fn with_upgrades(mut self, upgrades: Vec<StateSchemaUpgrade>) -> Self {
            self.upgrades = upgrades;
            self
        }
    }

    impl VersionedStateCodec<u64> for CounterSchema {
        fn schema_id(&self) -> &StateSchemaId {
            &self.schema
        }

        fn current_version(&self) -> StateSchemaVersion {
            StateSchemaVersion::new(self.version).expect("nonzero")
        }

        fn upgrades(&self) -> &[StateSchemaUpgrade] {
            &self.upgrades
        }

        fn encode(&self, value: &u64) -> Result<Vec<u8>, StateCodecError> {
            serde_json::to_vec(&serde_json::json!({ "rows": value }))
                .map_err(|_| StateCodecError::InvalidPayload)
        }

        fn decode(&self, payload: &[u8]) -> Result<u64, StateCodecError> {
            let value: serde_json::Value =
                serde_json::from_slice(payload).map_err(|_| StateCodecError::InvalidPayload)?;
            value
                .get("rows")
                .and_then(serde_json::Value::as_u64)
                .ok_or(StateCodecError::InvalidPayload)
        }
    }

    fn migrate_add_100(payload: &[u8]) -> Result<Vec<u8>, StateCodecError> {
        let mut value: serde_json::Value =
            serde_json::from_slice(payload).map_err(|_| StateCodecError::InvalidPayload)?;
        let rows = value
            .get("rows")
            .and_then(serde_json::Value::as_u64)
            .ok_or(StateCodecError::InvalidPayload)?;
        value["rows"] = serde_json::json!(rows + 100);
        serde_json::to_vec(&value).map_err(|_| StateCodecError::InvalidPayload)
    }

    fn migrate_add_1000(payload: &[u8]) -> Result<Vec<u8>, StateCodecError> {
        let mut value: serde_json::Value =
            serde_json::from_slice(payload).map_err(|_| StateCodecError::InvalidPayload)?;
        let rows = value
            .get("rows")
            .and_then(serde_json::Value::as_u64)
            .ok_or(StateCodecError::InvalidPayload)?;
        value["rows"] = serde_json::json!(rows + 1000);
        serde_json::to_vec(&value).map_err(|_| StateCodecError::InvalidPayload)
    }

    fn codec(schema_version: u32, codec_version: u32) -> DefaultComponentCodec<CounterSchema> {
        DefaultComponentCodec::new(
            CounterSchema::new(schema_version),
            CodecId::new("test.stream.counter-codec").expect("valid codec id"),
            CodecVersion::new(codec_version).expect("nonzero"),
            RestartabilityDeclaration::Restartable,
        )
    }

    fn contract(codec: DefaultComponentCodec<CounterSchema>) -> StreamStateContract {
        StreamStateContract::new(codec)
    }

    fn encode_with(
        codec: &DefaultComponentCodec<CounterSchema>,
        rows: u64,
    ) -> ComponentStateEnvelope {
        ComponentStateEnvelope::encode(identity(), &rows, codec, StateLimits::default())
            .expect("envelope encodes")
    }

    fn external_with_versions(schema_version: u32, codec_version: u32) -> ComponentStateEnvelope {
        ComponentStateEnvelope::external(
            identity(),
            StateSchemaId::new("test.stream.counter").expect("valid schema id"),
            StateSchemaVersion::new(schema_version).expect("nonzero schema version"),
            CodecId::new("test.stream.counter-codec").expect("valid codec id"),
            CodecVersion::new(codec_version).expect("nonzero codec version"),
            ExternalStateReference::new(ContentIdentity::of(b"external-state"), 14),
        )
    }

    /// Records every `open` call and, when an inherited envelope is present,
    /// the value it decoded -- so a test can prove `open` was never entered
    /// (rejection) or observed a migrated value (successful migration).
    struct CountingStream {
        open_calls: Arc<AtomicUsize>,
        observed: Arc<Mutex<Option<u64>>>,
        // The schema/codec version this application decodes with -- the same
        // "current" versions its `StreamStateContract` was built from. The
        // runtime hands `open` an envelope already migrated to those current
        // versions, so decode must expect them, not the originally-recorded
        // (pre-migration) versions.
        schema_version: u32,
        codec_version: u32,
    }

    impl CountingStream {
        fn new() -> (Self, Arc<AtomicUsize>, Arc<Mutex<Option<u64>>>) {
            Self::with_current_versions(1, 1)
        }

        fn with_current_versions(
            schema_version: u32,
            codec_version: u32,
        ) -> (Self, Arc<AtomicUsize>, Arc<Mutex<Option<u64>>>) {
            let open_calls = Arc::new(AtomicUsize::new(0));
            let observed = Arc::new(Mutex::new(None));
            (
                Self {
                    open_calls: Arc::clone(&open_calls),
                    observed: Arc::clone(&observed),
                    schema_version,
                    codec_version,
                },
                open_calls,
                observed,
            )
        }
    }

    impl ItemStream for CountingStream {
        async fn open(
            &self,
            context: StreamOpenContext<'_>,
        ) -> Result<StreamOpenOutcome, StreamOpenError> {
            self.open_calls.fetch_add(1, Ordering::SeqCst);
            let Some(envelope) = context.inherited_state() else {
                return Ok(StreamOpenOutcome::Initial);
            };
            let rows: u64 = envelope
                .decode(&codec(self.schema_version, self.codec_version))
                .map_err(|_| StreamOpenError::new())?;
            *self.observed.lock().expect("lock poisoned") = Some(rows);
            Ok(StreamOpenOutcome::Restored)
        }

        async fn update(
            &self,
            _context: StreamUpdateContext<'_>,
        ) -> Result<ComponentStateEnvelope, StreamUpdateError> {
            Ok(encode_with(&codec(1, 1), 1))
        }

        async fn close(
            &self,
            _context: StreamCloseContext<'_>,
        ) -> Result<StreamCloseOutcome, StreamCloseError> {
            Ok(StreamCloseOutcome::Closed)
        }
    }

    /// Observes only the envelope shape so current external-state acceptance
    /// is tested without pretending this fixture can resolve the blob.
    struct ExternalObservingStream {
        open_calls: Arc<AtomicUsize>,
        observed_external: Arc<Mutex<Option<bool>>>,
    }

    impl ExternalObservingStream {
        fn new() -> (Self, Arc<AtomicUsize>, Arc<Mutex<Option<bool>>>) {
            let open_calls = Arc::new(AtomicUsize::new(0));
            let observed_external = Arc::new(Mutex::new(None));
            (
                Self {
                    open_calls: Arc::clone(&open_calls),
                    observed_external: Arc::clone(&observed_external),
                },
                open_calls,
                observed_external,
            )
        }
    }

    impl ItemStream for ExternalObservingStream {
        async fn open(
            &self,
            context: StreamOpenContext<'_>,
        ) -> Result<StreamOpenOutcome, StreamOpenError> {
            self.open_calls.fetch_add(1, Ordering::SeqCst);
            *self.observed_external.lock().expect("lock poisoned") = context
                .inherited_state()
                .map(oxide_batch::ComponentStateEnvelope::is_external);
            Ok(StreamOpenOutcome::Restored)
        }

        async fn update(
            &self,
            _context: StreamUpdateContext<'_>,
        ) -> Result<ComponentStateEnvelope, StreamUpdateError> {
            Ok(external_with_versions(2, 2))
        }

        async fn close(
            &self,
            _context: StreamCloseContext<'_>,
        ) -> Result<StreamCloseOutcome, StreamCloseError> {
            Ok(StreamCloseOutcome::Closed)
        }
    }

    struct StateTransactions {
        receipt: oxide_batch::ChunkCommitReceipt,
        inherited: Vec<ComponentStateEnvelope>,
    }

    impl ChunkTransactionManager for StateTransactions {
        fn begin(
            &self,
        ) -> BoxFuture<'_, Result<Box<dyn ChunkTransaction + '_>, ChunkTransactionError>> {
            let transaction = StateTestTransaction {
                receipt: self.receipt.clone(),
            };
            Box::pin(async move { Ok(Box::new(transaction) as Box<dyn ChunkTransaction>) })
        }

        fn inherited_component_state(
            &self,
            _context: ChunkTransactionContext,
        ) -> BoxFuture<'_, Result<Vec<ComponentStateEnvelope>, ChunkTransactionError>> {
            let inherited = self.inherited.clone();
            Box::pin(async move { Ok(inherited) })
        }
    }

    struct StateTestTransaction {
        receipt: oxide_batch::ChunkCommitReceipt,
    }

    impl ChunkTransaction for StateTestTransaction {
        fn business_transaction(&mut self) -> Option<&mut dyn oxide_batch::BusinessTransaction> {
            None
        }

        fn commit(
            &mut self,
            _counts: ChunkCounts,
            _fault: ChunkFaultProgress,
        ) -> BoxFuture<'_, Result<oxide_batch::ChunkCommitReceipt, ChunkTransactionError>> {
            let receipt = self.receipt.clone();
            Box::pin(async move { Ok(receipt) })
        }

        fn commit_with_component_state<'a>(
            &'a mut self,
            counts: ChunkCounts,
            fault: ChunkFaultProgress,
            _component_state: &'a [ComponentStateEnvelope],
        ) -> BoxFuture<'a, Result<oxide_batch::ChunkCommitReceipt, ChunkTransactionError>> {
            self.commit(counts, fault)
        }

        fn rollback(&mut self) -> BoxFuture<'_, Result<(), ChunkTransactionError>> {
            Box::pin(async { Ok(()) })
        }
    }

    fn stream_components() -> ChunkComponentRevisions {
        chunk_revisions().with_stream_revision(
            identity(),
            ComponentRevision::new("counter-v1").expect("valid revision"),
        )
    }

    /// [`run_with_inherited_versions`] for the common case where the
    /// application's decode versions match the encoded envelope's (1, 1).
    async fn run_with_inherited(
        inherited: Vec<ComponentStateEnvelope>,
        contract: StreamStateContract,
    ) -> (
        ChunkExecutionOutcome,
        Arc<AtomicUsize>,
        Arc<Mutex<Option<u64>>>,
    ) {
        run_with_inherited_versions(inherited, contract, (1, 1)).await
    }

    /// Launches one chunk step with `inherited` as this attempt's inherited
    /// component state, registering a [`CountingStream`] under [`identity`]
    /// with `contract`. `stream_versions` is the (schema, codec) version pair
    /// the application itself decodes with -- the same current versions
    /// `contract`'s codec declares.
    async fn run_with_inherited_versions(
        inherited: Vec<ComponentStateEnvelope>,
        contract: StreamStateContract,
        stream_versions: (u32, u32),
    ) -> (
        ChunkExecutionOutcome,
        Arc<AtomicUsize>,
        Arc<Mutex<Option<u64>>>,
    ) {
        let (writer, _batches) = Writer::new(Boundary::Normal);
        let (completion, _calls) = Completion::new(Boundary::Normal);
        let transactions = StateTransactions {
            receipt: receipt(),
            inherited,
        };
        let (stream, open_calls, observed) =
            CountingStream::with_current_versions(stream_versions.0, stream_versions.1);
        let step = ChunkStep::new(
            StepName::new("import").expect("valid step name"),
            ChunkSize::new(2).expect("valid chunk size"),
            Reader::new([1]),
            Processor::normal(),
            writer,
            Arc::new(transactions),
            Arc::new(completion),
        )
        .with_item_stream(identity(), stream, contract);
        let mut job = ChunkJob::new(
            JobName::new("stream_contract_test").expect("valid job name"),
            step,
            DefinitionRevision::new("test-v1").expect("valid revision"),
            &stream_components(),
        )
        .expect("bijection-valid definition");
        let clock = ManualClock::new(UNIX_EPOCH + Duration::from_secs(700));
        let ids = DeterministicIds::new(NonZeroU64::MIN);
        let repository = InMemoryJobRepository::new(Arc::new(clock.clone()), Arc::new(ids.clone()));
        let launcher = JobLauncher::new(&repository, &clock, &ids);
        let (_stop_source, stop) = StopSource::new();
        let report = launcher
            .launch_chunk(&mut job, &JobParameters::new(), &stop)
            .await
            .expect("launch completes");
        let chunk = report.chunk().expect("chunk work started");
        (chunk.outcome(), open_calls, observed)
    }

    async fn run_with_external_inherited(
        inherited: Vec<ComponentStateEnvelope>,
        contract: StreamStateContract,
    ) -> (
        ChunkExecutionOutcome,
        Arc<AtomicUsize>,
        Arc<Mutex<Option<bool>>>,
    ) {
        let (writer, _batches) = Writer::new(Boundary::Normal);
        let (completion, _calls) = Completion::new(Boundary::Normal);
        let transactions = StateTransactions {
            receipt: receipt(),
            inherited,
        };
        let (stream, open_calls, observed_external) = ExternalObservingStream::new();
        let step = ChunkStep::new(
            StepName::new("import").expect("valid step name"),
            ChunkSize::new(2).expect("valid chunk size"),
            Reader::new([1]),
            Processor::normal(),
            writer,
            Arc::new(transactions),
            Arc::new(completion),
        )
        .with_item_stream(identity(), stream, contract);
        let mut job = ChunkJob::new(
            JobName::new("external_stream_contract_test").expect("valid job name"),
            step,
            DefinitionRevision::new("test-v1").expect("valid revision"),
            &stream_components(),
        )
        .expect("bijection-valid definition");
        let clock = ManualClock::new(UNIX_EPOCH + Duration::from_secs(700));
        let ids = DeterministicIds::new(NonZeroU64::MIN);
        let repository = InMemoryJobRepository::new(Arc::new(clock.clone()), Arc::new(ids.clone()));
        let launcher = JobLauncher::new(&repository, &clock, &ids);
        let (_stop_source, stop) = StopSource::new();
        let report = launcher
            .launch_chunk(&mut job, &JobParameters::new(), &stop)
            .await
            .expect("launch completes");
        let chunk = report.chunk().expect("chunk work started");
        (chunk.outcome(), open_calls, observed_external)
    }

    #[tokio::test]
    async fn open_rejects_unknown_schema_before_user_stream_is_called() {
        let wrong_schema = DefaultComponentCodec::new(
            CounterSchema::new(1).with_schema("test.stream.other"),
            CodecId::new("test.stream.counter-codec").expect("valid codec id"),
            CodecVersion::new(1).expect("nonzero"),
            RestartabilityDeclaration::Restartable,
        );
        let inherited = encode_with(&wrong_schema, 5);

        let (outcome, open_calls, observed) =
            run_with_inherited(vec![inherited], contract(codec(1, 1))).await;

        assert_eq!(
            outcome,
            ChunkExecutionOutcome::Failed(ChunkFailure::StreamOpen)
        );
        assert_eq!(
            open_calls.load(Ordering::SeqCst),
            0,
            "a rejected contract must never enter the application's open()"
        );
        assert_eq!(*observed.lock().expect("lock poisoned"), None);
    }

    #[tokio::test]
    async fn open_rejects_newer_schema_before_user_stream_is_called() {
        let inherited = encode_with(&codec(2, 1), 5);

        let (outcome, open_calls, _observed) =
            run_with_inherited(vec![inherited], contract(codec(1, 1))).await;

        assert_eq!(
            outcome,
            ChunkExecutionOutcome::Failed(ChunkFailure::StreamOpen)
        );
        assert_eq!(open_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn open_rejects_unknown_codec_before_user_stream_is_called() {
        let wrong_codec = DefaultComponentCodec::new(
            CounterSchema::new(1),
            CodecId::new("test.stream.other-codec").expect("valid codec id"),
            CodecVersion::new(1).expect("nonzero"),
            RestartabilityDeclaration::Restartable,
        );
        let inherited = encode_with(&wrong_codec, 5);

        let (outcome, open_calls, _observed) =
            run_with_inherited(vec![inherited], contract(codec(1, 1))).await;

        assert_eq!(
            outcome,
            ChunkExecutionOutcome::Failed(ChunkFailure::StreamOpen)
        );
        assert_eq!(open_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn open_rejects_older_external_schema_before_user_stream_is_called() {
        let inherited = external_with_versions(1, 2);

        let (outcome, open_calls, _observed) =
            run_with_external_inherited(vec![inherited], contract(codec(2, 2))).await;

        assert_eq!(
            outcome,
            ChunkExecutionOutcome::Failed(ChunkFailure::StreamOpen)
        );
        assert_eq!(open_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn open_rejects_older_external_codec_before_user_stream_is_called() {
        let inherited = external_with_versions(2, 1);

        let (outcome, open_calls, _observed) =
            run_with_external_inherited(vec![inherited], contract(codec(2, 2))).await;

        assert_eq!(
            outcome,
            ChunkExecutionOutcome::Failed(ChunkFailure::StreamOpen)
        );
        assert_eq!(open_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn open_accepts_current_external_state() {
        let (outcome, open_calls, observed_external) =
            run_with_external_inherited(vec![external_with_versions(2, 2)], contract(codec(2, 2)))
                .await;

        assert_eq!(outcome, ChunkExecutionOutcome::Completed);
        assert_eq!(open_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            *observed_external.lock().expect("lock poisoned"),
            Some(true)
        );
    }

    #[tokio::test]
    async fn open_applies_declared_schema_migration_before_user_stream_is_called() {
        let inherited = encode_with(&codec(1, 1), 5);
        let upgrade = StateSchemaUpgrade::new(
            StateSchemaVersion::new(1).expect("nonzero"),
            StateSchemaVersion::new(2).expect("nonzero"),
            migrate_add_100,
        )
        .expect("valid upgrade");
        let current = DefaultComponentCodec::new(
            CounterSchema::new(2).with_upgrades(vec![upgrade]),
            CodecId::new("test.stream.counter-codec").expect("valid codec id"),
            CodecVersion::new(1).expect("nonzero"),
            RestartabilityDeclaration::Restartable,
        );

        let (outcome, open_calls, observed) =
            run_with_inherited_versions(vec![inherited], contract(current), (2, 1)).await;

        assert_eq!(outcome, ChunkExecutionOutcome::Completed);
        assert_eq!(open_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            *observed.lock().expect("lock poisoned"),
            Some(105),
            "open must observe the migrated value, not the recorded one"
        );
    }

    #[tokio::test]
    async fn open_applies_declared_codec_migration_before_user_stream_is_called() {
        let inherited = encode_with(&codec(1, 1), 5);
        let upgrade = CodecVersionUpgrade::new(
            CodecVersion::new(1).expect("nonzero"),
            CodecVersion::new(2).expect("nonzero"),
            migrate_add_1000,
        )
        .expect("valid upgrade");
        let current = DefaultComponentCodec::new(
            CounterSchema::new(1),
            CodecId::new("test.stream.counter-codec").expect("valid codec id"),
            CodecVersion::new(2).expect("nonzero"),
            RestartabilityDeclaration::Restartable,
        )
        .with_codec_upgrades(vec![upgrade]);

        let (outcome, open_calls, observed) =
            run_with_inherited_versions(vec![inherited], contract(current), (1, 2)).await;

        assert_eq!(outcome, ChunkExecutionOutcome::Completed);
        assert_eq!(open_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            *observed.lock().expect("lock poisoned"),
            Some(1005),
            "open must observe the codec-migrated value, not the recorded one"
        );
    }

    fn stream_step() -> ChunkStep<i32, i32, Reader, Processor, Writer> {
        let (writer, _batches) = Writer::new(Boundary::Normal);
        let (completion, _calls) = Completion::new(Boundary::Normal);
        let (stream, _open_calls, _observed) = CountingStream::new();
        ChunkStep::new(
            StepName::new("import").expect("valid step name"),
            ChunkSize::new(2).expect("valid chunk size"),
            Reader::new([1]),
            Processor::normal(),
            writer,
            Arc::new(StateTransactions {
                receipt: receipt(),
                inherited: Vec::new(),
            }),
            Arc::new(completion),
        )
        .with_item_stream(identity(), stream, contract(codec(1, 1)))
    }

    #[test]
    fn runtime_stream_missing_from_manifest_is_rejected() {
        let step = stream_step();
        let result = ChunkJob::new(
            JobName::new("bijection_missing_manifest").expect("valid job name"),
            step,
            DefinitionRevision::new("test-v1").expect("valid revision"),
            &chunk_revisions(),
        );
        assert!(matches!(
            result,
            Err(DefinitionError::RuntimeStreamNotDeclared { .. })
        ));
    }

    #[test]
    fn manifest_stream_missing_from_runtime_is_rejected() {
        let (writer, _batches) = Writer::new(Boundary::Normal);
        let (completion, _calls) = Completion::new(Boundary::Normal);
        let step = ChunkStep::new(
            StepName::new("import").expect("valid step name"),
            ChunkSize::new(2).expect("valid chunk size"),
            Reader::new([1]),
            Processor::normal(),
            writer,
            Arc::new(StateTransactions {
                receipt: receipt(),
                inherited: Vec::new(),
            }),
            Arc::new(completion),
        );
        let result = ChunkJob::new(
            JobName::new("bijection_missing_runtime").expect("valid job name"),
            step,
            DefinitionRevision::new("test-v1").expect("valid revision"),
            &stream_components(),
        );
        assert!(matches!(
            result,
            Err(DefinitionError::DeclaredStreamMissingRuntime { .. })
        ));
    }

    #[test]
    fn duplicate_runtime_stream_namespace_is_rejected() {
        let (writer, _batches) = Writer::new(Boundary::Normal);
        let (completion, _calls) = Completion::new(Boundary::Normal);
        let (stream_a, _, _) = CountingStream::new();
        let (stream_b, _, _) = CountingStream::new();
        let step = ChunkStep::new(
            StepName::new("import").expect("valid step name"),
            ChunkSize::new(2).expect("valid chunk size"),
            Reader::new([1]),
            Processor::normal(),
            writer,
            Arc::new(StateTransactions {
                receipt: receipt(),
                inherited: Vec::new(),
            }),
            Arc::new(completion),
        )
        .with_item_stream(identity(), stream_a, contract(codec(1, 1)))
        .with_item_stream(identity(), stream_b, contract(codec(1, 1)));
        let result = ChunkJob::new(
            JobName::new("bijection_duplicate").expect("valid job name"),
            step,
            DefinitionRevision::new("test-v1").expect("valid revision"),
            &stream_components(),
        );
        assert!(matches!(
            result,
            Err(DefinitionError::DuplicateRuntimeStream { .. })
        ));
    }

    #[test]
    fn matching_runtime_and_manifest_streams_are_accepted() {
        let step = stream_step();
        let result = ChunkJob::new(
            JobName::new("bijection_matching").expect("valid job name"),
            step,
            DefinitionRevision::new("test-v1").expect("valid revision"),
            &stream_components(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn restartable_stream_allows_restartable_plan() {
        let step = stream_step();
        let result = ChunkJob::new(
            JobName::new("restartable_ok").expect("valid job name"),
            step,
            DefinitionRevision::new("test-v1").expect("valid revision"),
            &stream_components(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn stateful_nonrestartable_stream_prevents_restartable_plan() {
        let (writer, _batches) = Writer::new(Boundary::Normal);
        let (completion, _calls) = Completion::new(Boundary::Normal);
        let (stream, _, _) = CountingStream::new();
        let not_restartable = DefaultComponentCodec::new(
            CounterSchema::new(1),
            CodecId::new("test.stream.counter-codec").expect("valid codec id"),
            CodecVersion::new(1).expect("nonzero"),
            RestartabilityDeclaration::NotRestartable,
        );
        let step = ChunkStep::new(
            StepName::new("import").expect("valid step name"),
            ChunkSize::new(2).expect("valid chunk size"),
            Reader::new([1]),
            Processor::normal(),
            writer,
            Arc::new(StateTransactions {
                receipt: receipt(),
                inherited: Vec::new(),
            }),
            Arc::new(completion),
        )
        .with_item_stream(identity(), stream, contract(not_restartable));
        let result = ChunkJob::new(
            JobName::new("nonrestartable").expect("valid job name"),
            step,
            DefinitionRevision::new("test-v1").expect("valid revision"),
            &stream_components(),
        );
        assert!(matches!(
            result,
            Err(DefinitionError::NonRestartableStream { .. })
        ));
    }

    #[test]
    fn flow_job_rejects_a_runtime_stream_not_declared_in_the_bound_revisions() {
        let step = stream_step();
        let revisions = chunk_revisions();
        let node = NodeId::new("import").expect("static node ID is valid");
        let name = JobName::new("flow_stream_bijection").expect("static job name is valid");
        let plan = FlowGraph::new(node.clone())
            .with_node(FlowNode::step(StepNode::new(
                node.clone(),
                StepName::new("import").expect("static step name is valid"),
                StepComponents::Chunk {
                    size: ChunkSize::new(2).expect("static chunk size is nonzero"),
                    revisions: Box::new(revisions.clone()),
                },
            )))
            .with_sequence(node.clone(), FlowTarget::Terminal(TerminalKind::Complete))
            .expect("static sequence is valid")
            .compile(
                &name,
                DefinitionRevision::new("flow-v1").expect("static revision is valid"),
            )
            .expect("static flow compiles");
        let result = FlowJob::new(name, plan)
            .expect("format-2 flow is valid")
            .with_chunk_step(node, step, &revisions);
        assert!(result.is_err());
    }
}

/// Real `ChunkStep::execute` evidence for `AdaptiveCompletionPolicy`'s #179
/// blockers: the same registered instance drives both completion decisions
/// and `ItemStream` persistence, a rollback never lets a speculative target
/// leak as authoritative, the same process picks up correctly on its next
/// chunk, and a panicking `CompletionPolicy` is contained the same way every
/// other user-supplied component call already is.
mod adaptive_completion_policy_integration {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, UNIX_EPOCH};

    use oxide_batch::{
        AdaptiveBounds, AdaptiveCompletionPolicy, BoxFuture, BusinessTransaction,
        ChunkAttemptOutcome, ChunkCommitReceipt, ChunkCount, ChunkCounts, ChunkExecutionOutcome,
        ChunkFailure, ChunkFaultProgress, ChunkJob, ChunkSize, ChunkStep, ChunkTimeThreshold,
        ChunkTransaction, ChunkTransactionError, ChunkTransactionManager, CompletionPolicy,
        ComponentStateEnvelope, ComponentStreamIdentity, CompositeCompletionPolicy, CompositeMode,
        DefinitionRevision, ItemCountCompletionPolicy, JobName, StepName, StopSource,
    };

    use super::{
        Boundary, Completion, ManualClock, Processor, Reader, Writer, chunk_revisions, correlation,
        receipt,
    };

    fn identity() -> ComponentStreamIdentity {
        ComponentStreamIdentity::new("adaptive.integration").expect("valid identity")
    }

    fn adaptive_policy(clock: Arc<dyn oxide_batch::Clock>) -> Arc<AdaptiveCompletionPolicy> {
        let bounds = AdaptiveBounds::new(
            ChunkSize::new(1).expect("valid ChunkSize"),
            ChunkSize::new(100).expect("valid ChunkSize"),
        )
        .expect("valid bounds");
        let target_duration =
            ChunkTimeThreshold::new(Duration::from_secs(1)).expect("valid threshold");
        Arc::new(AdaptiveCompletionPolicy::new(
            identity(),
            bounds,
            target_duration,
            clock,
        ))
    }

    #[derive(Default)]
    struct ToggleEvidence {
        commits: Mutex<Vec<Vec<ComponentStateEnvelope>>>,
        rollbacks: Mutex<u64>,
    }

    /// A transaction manager whose `begin`-numbered attempt equal to
    /// `fail_at` fails to commit (`ChunkTransactionError::NotCommitted`,
    /// triggering a real rollback); every other attempt commits normally.
    /// `fail_at: 0` never fails, since attempts are numbered from `1`.
    struct ToggleTransactions {
        receipt: ChunkCommitReceipt,
        attempt: Arc<AtomicUsize>,
        fail_at: usize,
        evidence: Arc<ToggleEvidence>,
    }

    impl ChunkTransactionManager for ToggleTransactions {
        fn begin(
            &self,
        ) -> BoxFuture<'_, Result<Box<dyn ChunkTransaction + '_>, ChunkTransactionError>> {
            let index = self.attempt.fetch_add(1, Ordering::SeqCst) + 1;
            let transaction = ToggleTransaction {
                receipt: self.receipt.clone(),
                should_fail: index == self.fail_at,
                evidence: Arc::clone(&self.evidence),
            };
            Box::pin(async move { Ok(Box::new(transaction) as Box<dyn ChunkTransaction>) })
        }
    }

    struct ToggleTransaction {
        receipt: ChunkCommitReceipt,
        should_fail: bool,
        evidence: Arc<ToggleEvidence>,
    }

    impl ChunkTransaction for ToggleTransaction {
        fn business_transaction(&mut self) -> Option<&mut dyn BusinessTransaction> {
            None
        }

        fn commit(
            &mut self,
            _counts: ChunkCounts,
            _fault: ChunkFaultProgress,
        ) -> BoxFuture<'_, Result<ChunkCommitReceipt, ChunkTransactionError>> {
            let receipt = self.receipt.clone();
            Box::pin(async move { Ok(receipt) })
        }

        fn commit_with_component_state<'a>(
            &'a mut self,
            _counts: ChunkCounts,
            _fault: ChunkFaultProgress,
            component_state: &'a [ComponentStateEnvelope],
        ) -> BoxFuture<'a, Result<ChunkCommitReceipt, ChunkTransactionError>> {
            if self.should_fail {
                return Box::pin(async { Err(ChunkTransactionError::NotCommitted) });
            }
            self.evidence
                .commits
                .lock()
                .expect("commit evidence lock poisoned")
                .push(component_state.to_vec());
            let receipt = self.receipt.clone();
            Box::pin(async move { Ok(receipt) })
        }

        fn rollback(&mut self) -> BoxFuture<'_, Result<(), ChunkTransactionError>> {
            *self
                .evidence
                .rollbacks
                .lock()
                .expect("rollback evidence lock poisoned") += 1;
            Box::pin(async { Ok(()) })
        }
    }

    #[tokio::test]
    async fn adaptive_policy_drives_completion_and_persists_state_through_same_instance() {
        let clock: Arc<dyn oxide_batch::Clock> = Arc::new(ManualClock::new(UNIX_EPOCH));
        let policy = adaptive_policy(clock);
        let (writer, batches) = Writer::new(Boundary::Normal);
        let (completion, _calls) = Completion::new(Boundary::Normal);
        let transactions = ToggleTransactions {
            receipt: receipt(),
            attempt: Arc::new(AtomicUsize::new(0)),
            fail_at: 0,
            evidence: Arc::new(ToggleEvidence::default()),
        };
        let mut step = ChunkStep::new(
            StepName::new("adaptive_growth").expect("valid step name"),
            ChunkSize::new(3).expect("valid chunk size"),
            Reader::new(1..=6),
            Processor::normal(),
            writer,
            Arc::new(transactions),
            Arc::new(completion),
        )
        .with_adaptive_completion_policy(Arc::clone(&policy));
        let (_source, stop) = StopSource::new();

        let report = step.execute(&correlation(), &stop).await;

        assert_eq!(report.outcome(), ChunkExecutionOutcome::Completed);
        assert_eq!(
            *batches.lock().expect("writer batches lock poisoned"),
            vec![vec![10], vec![20, 30], vec![40, 50, 60]],
            "each chunk's size must grow exactly as the policy's own committed \
             target -- the same instance driving both completion and persistence"
        );
        assert_eq!(
            policy.current_target().get(),
            4,
            "the confirmed target must reflect every real committed chunk"
        );
    }

    #[tokio::test]
    async fn rollback_during_real_execution_preserves_state_and_next_execute_recovers() {
        let clock: Arc<dyn oxide_batch::Clock> = Arc::new(ManualClock::new(UNIX_EPOCH));
        let policy = adaptive_policy(clock);
        let (writer, batches) = Writer::new(Boundary::Normal);
        let (completion, _calls) = Completion::new(Boundary::Normal);
        let attempt = Arc::new(AtomicUsize::new(0));
        let transactions = ToggleTransactions {
            receipt: receipt(),
            attempt: Arc::clone(&attempt),
            // The third physical attempt (3 items read, candidate 4) fails to
            // commit.
            fail_at: 3,
            evidence: Arc::new(ToggleEvidence::default()),
        };
        let mut step = ChunkStep::new(
            StepName::new("adaptive_rollback").expect("valid step name"),
            ChunkSize::new(3).expect("valid chunk size"),
            Reader::new(1..=8),
            Processor::normal(),
            writer,
            Arc::new(transactions),
            Arc::new(completion),
        )
        .with_adaptive_completion_policy(Arc::clone(&policy));
        let (_source, stop) = StopSource::new();

        let first = step.execute(&correlation(), &stop).await;

        assert_eq!(
            first.outcome(),
            ChunkExecutionOutcome::Failed(ChunkFailure::TransactionCommit)
        );
        assert_eq!(
            *batches.lock().expect("writer batches lock poisoned"),
            vec![vec![10], vec![20, 30], vec![40, 50, 60]],
            "the third attempt's write still happens inside its (later rolled \
             back) transaction"
        );
        assert_eq!(
            policy.current_target().get(),
            3,
            "a rollback must leave confirmed exactly where the last real \
             commit left it, never the discarded candidate the failed \
             attempt computed"
        );

        // Same process, same policy instance, a fresh `execute` call --
        // exactly how a supervisor retries a failed step attempt in place.
        let second = step.execute(&correlation(), &stop).await;

        assert_eq!(second.outcome(), ChunkExecutionOutcome::Completed);
        assert_eq!(
            *batches.lock().expect("writer batches lock poisoned"),
            vec![vec![10], vec![20, 30], vec![40, 50, 60], vec![70, 80]],
            "the next chunk must read the remaining items once execution resumes"
        );
        assert_eq!(
            policy.current_target().get(),
            4,
            "growth must resume from the unchanged post-rollback baseline, not \
             from the corrupted candidate a pre-fix policy would have kept"
        );
    }

    #[derive(Clone, Copy)]
    enum PanicPoint {
        BeginChunk,
        IsComplete,
        EndChunk,
    }

    struct PanickingPolicy {
        panic_in: PanicPoint,
    }

    impl CompletionPolicy for PanickingPolicy {
        fn begin_chunk(&self) {
            if matches!(self.panic_in, PanicPoint::BeginChunk) {
                panic!("completion policy secret");
            }
        }

        fn is_complete(&self, _items_read: ChunkCount) -> bool {
            if matches!(self.panic_in, PanicPoint::IsComplete) {
                panic!("completion policy secret");
            }
            false
        }

        fn end_chunk(&self, _outcome: ChunkAttemptOutcome) {
            if matches!(self.panic_in, PanicPoint::EndChunk) {
                panic!("completion policy secret");
            }
        }
    }

    #[tokio::test]
    async fn completion_policy_panic_is_typed_and_contained() {
        for panic_in in [
            PanicPoint::BeginChunk,
            PanicPoint::IsComplete,
            PanicPoint::EndChunk,
        ] {
            let (writer, _batches) = Writer::new(Boundary::Normal);
            let (completion, _calls) = Completion::new(Boundary::Normal);
            let transactions = ToggleTransactions {
                receipt: receipt(),
                attempt: Arc::new(AtomicUsize::new(0)),
                fail_at: 0,
                evidence: Arc::new(ToggleEvidence::default()),
            };
            let mut step = ChunkStep::new(
                StepName::new("completion_panic").expect("valid step name"),
                ChunkSize::new(3).expect("valid chunk size"),
                Reader::new([1, 2, 3]),
                Processor::normal(),
                writer,
                Arc::new(transactions),
                Arc::new(completion),
            )
            .with_completion_policy(Arc::new(PanickingPolicy { panic_in }));
            let (_source, stop) = StopSource::new();

            let report = step.execute(&correlation(), &stop).await;

            assert_eq!(
                report.outcome(),
                ChunkExecutionOutcome::Failed(ChunkFailure::CompletionPolicyPanic)
            );
            assert!(!format!("{report:?}").contains("secret"));
        }
    }

    #[tokio::test]
    async fn composite_child_panic_is_contained_through_composite_dispatch() {
        let (writer, _batches) = Writer::new(Boundary::Normal);
        let (completion, _calls) = Completion::new(Boundary::Normal);
        let transactions = ToggleTransactions {
            receipt: receipt(),
            attempt: Arc::new(AtomicUsize::new(0)),
            fail_at: 0,
            evidence: Arc::new(ToggleEvidence::default()),
        };
        let panicking: Arc<dyn CompletionPolicy> = Arc::new(PanickingPolicy {
            panic_in: PanicPoint::IsComplete,
        });
        let normal: Arc<dyn CompletionPolicy> = Arc::new(ItemCountCompletionPolicy::new(
            ChunkSize::new(5).expect("valid ChunkSize"),
        ));
        // `panicking` must be evaluated: `CompositeMode::All`'s `Iterator::all`
        // short-circuits on the first `false`, so it is listed first to
        // guarantee it is reached regardless of member evaluation order
        // semantics.
        let composite = CompositeCompletionPolicy::new(CompositeMode::All, vec![panicking, normal])
            .expect("valid composite");
        let mut step = ChunkStep::new(
            StepName::new("composite_panic").expect("valid step name"),
            ChunkSize::new(3).expect("valid chunk size"),
            Reader::new([1, 2, 3]),
            Processor::normal(),
            writer,
            Arc::new(transactions),
            Arc::new(completion),
        )
        .with_completion_policy(Arc::new(composite));
        let (_source, stop) = StopSource::new();

        let report = step.execute(&correlation(), &stop).await;

        assert_eq!(
            report.outcome(),
            ChunkExecutionOutcome::Failed(ChunkFailure::CompletionPolicyPanic)
        );
        assert!(!format!("{report:?}").contains("secret"));
    }

    fn definition_digest_for(policy: Option<Arc<dyn CompletionPolicy>>) -> [u8; 32] {
        let (writer, _batches) = Writer::new(Boundary::Normal);
        let (completion, _calls) = Completion::new(Boundary::Normal);
        let transactions = ToggleTransactions {
            receipt: receipt(),
            attempt: Arc::new(AtomicUsize::new(0)),
            fail_at: 0,
            evidence: Arc::new(ToggleEvidence::default()),
        };
        let mut step = ChunkStep::new(
            StepName::new("fingerprint_probe").expect("valid step name"),
            ChunkSize::new(2).expect("valid chunk size"),
            Reader::new([1]),
            Processor::normal(),
            writer,
            Arc::new(transactions),
            Arc::new(completion),
        );
        if let Some(policy) = policy {
            step = step.with_completion_policy(policy);
        }
        let job = ChunkJob::new(
            JobName::new("fingerprint_probe_job").expect("valid job name"),
            step,
            DefinitionRevision::new("v1").expect("valid revision"),
            &chunk_revisions(),
        )
        .expect("valid definition");
        *job.definition_identity().manifest_digest()
    }

    #[test]
    fn completion_policy_configuration_changes_the_definition_fingerprint() {
        let no_policy = definition_digest_for(None);

        let count_5_a: Arc<dyn CompletionPolicy> = Arc::new(ItemCountCompletionPolicy::new(
            ChunkSize::new(5).expect("valid ChunkSize"),
        ));
        let count_5_b: Arc<dyn CompletionPolicy> = Arc::new(ItemCountCompletionPolicy::new(
            ChunkSize::new(5).expect("valid ChunkSize"),
        ));
        let count_6: Arc<dyn CompletionPolicy> = Arc::new(ItemCountCompletionPolicy::new(
            ChunkSize::new(6).expect("valid ChunkSize"),
        ));

        let digest_a = definition_digest_for(Some(count_5_a));
        let digest_b = definition_digest_for(Some(count_5_b));
        let digest_c = definition_digest_for(Some(count_6));

        assert_ne!(
            no_policy, digest_a,
            "installing a completion policy must change the definition fingerprint"
        );
        assert_eq!(
            digest_a, digest_b,
            "identical completion-policy configuration must fingerprint identically"
        );
        assert_ne!(
            digest_a, digest_c,
            "a different completion-policy configuration must change the fingerprint"
        );
    }

    #[test]
    fn nested_composite_structure_changes_the_definition_fingerprint() {
        let leaf: Arc<dyn CompletionPolicy> = Arc::new(ItemCountCompletionPolicy::new(
            ChunkSize::new(2).expect("valid ChunkSize"),
        ));
        let sibling: Arc<dyn CompletionPolicy> = Arc::new(ItemCountCompletionPolicy::new(
            ChunkSize::new(3).expect("valid ChunkSize"),
        ));

        let inner_any = Arc::new(
            CompositeCompletionPolicy::new(CompositeMode::Any, vec![Arc::clone(&leaf)])
                .expect("valid composite"),
        ) as Arc<dyn CompletionPolicy>;
        let inner_all = Arc::new(
            CompositeCompletionPolicy::new(CompositeMode::All, vec![leaf])
                .expect("valid composite"),
        ) as Arc<dyn CompletionPolicy>;

        let outer_a: Arc<dyn CompletionPolicy> = Arc::new(
            CompositeCompletionPolicy::new(
                CompositeMode::All,
                vec![inner_any, Arc::clone(&sibling)],
            )
            .expect("valid composite"),
        );
        let outer_b: Arc<dyn CompletionPolicy> = Arc::new(
            CompositeCompletionPolicy::new(CompositeMode::All, vec![inner_all, sibling])
                .expect("valid composite"),
        );

        let digest_a = definition_digest_for(Some(outer_a));
        let digest_b = definition_digest_for(Some(outer_b));

        assert_ne!(
            digest_a, digest_b,
            "a mode change nested inside a composite member must change the outer definition's fingerprint"
        );
    }

    #[test]
    fn adaptive_bounds_change_the_definition_fingerprint() {
        let clock_a: Arc<dyn oxide_batch::Clock> = Arc::new(ManualClock::new(UNIX_EPOCH));
        let clock_b: Arc<dyn oxide_batch::Clock> = Arc::new(ManualClock::new(UNIX_EPOCH));
        let target_duration =
            ChunkTimeThreshold::new(Duration::from_secs(1)).expect("valid threshold");
        let bounds_a = AdaptiveBounds::new(
            ChunkSize::new(1).expect("valid"),
            ChunkSize::new(10).expect("valid"),
        )
        .expect("valid bounds");
        let bounds_b = AdaptiveBounds::new(
            ChunkSize::new(1).expect("valid"),
            ChunkSize::new(20).expect("valid"),
        )
        .expect("valid bounds");
        let policy_a: Arc<dyn CompletionPolicy> = Arc::new(AdaptiveCompletionPolicy::new(
            identity(),
            bounds_a,
            target_duration,
            clock_a,
        ));
        let policy_b: Arc<dyn CompletionPolicy> = Arc::new(AdaptiveCompletionPolicy::new(
            identity(),
            bounds_b,
            target_duration,
            clock_b,
        ));

        let digest_a = definition_digest_for(Some(policy_a));
        let digest_b = definition_digest_for(Some(policy_b));

        assert_ne!(
            digest_a, digest_b,
            "an adaptive policy's bounds are restart-relevant configuration"
        );
    }
}
