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
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};

use clock::ManualClock;
use ids::DeterministicIds;
use oxide_batch::{
    BatchStatus, BoxFuture, Checkpoint, ChunkAttemptOutcome, ChunkCommitReceipt, ChunkCompletion,
    ChunkCompletionContext, ChunkCompletionError, ChunkCompletionOutcome, ChunkCount, ChunkCounts,
    ChunkExecutionOutcome, ChunkFailure, ChunkJob, ChunkListener, ChunkListenerContext,
    ChunkListenerError, ChunkSize, ChunkStep, ChunkTransaction, ChunkTransactionError,
    ChunkTransactionManager, ExecutionContext, InMemoryJobRepository, ItemProcessor, ItemReader,
    ItemWriter, JobLauncher, JobName, JobParameters, LifecycleEvent, LifecycleEventKind,
    LifecycleEventSink, ListenerContext, ListenerError, ProcessContext, ProcessOutcome,
    ProcessorError, ReadContext, ReadOutcome, ReaderError, StateLimits, StepExecutionListener,
    StepName, StopSource, TaskletExecutionOutcome, WriteContext, WriteOutcome, WriterError,
};

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
    fn read<'a>(
        &'a mut self,
        _context: ReadContext<'a>,
    ) -> BoxFuture<'a, Result<ReadOutcome<i32>, ReaderError>> {
        match self.boundary {
            Boundary::Panic => panic!("reader secret"),
            Boundary::Error => Box::pin(async { Err(ReaderError::new()) }),
            Boundary::Stop => Box::pin(async { Ok(ReadOutcome::Stopped) }),
            Boundary::Normal => {
                let item = self.items.pop_front();
                Box::pin(async move { Ok(item.map_or(ReadOutcome::EndOfInput, ReadOutcome::Item)) })
            }
        }
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
    fn process<'a>(
        &'a self,
        item: &'a i32,
        _context: ProcessContext<'a>,
    ) -> BoxFuture<'a, Result<ProcessOutcome<i32>, ProcessorError>> {
        match self.boundary {
            Boundary::Panic => panic!("processor secret"),
            Boundary::Error => Box::pin(async { Err(ProcessorError::new()) }),
            Boundary::Stop => Box::pin(async { Ok(ProcessOutcome::Stopped) }),
            Boundary::Normal if self.filter == Some(*item) => {
                Box::pin(async { Ok(ProcessOutcome::Filtered) })
            }
            Boundary::Normal => {
                let output = item * 10;
                Box::pin(async move { Ok(ProcessOutcome::Item(output)) })
            }
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
    fn write<'a>(
        &'a self,
        items: &'a [i32],
        _context: WriteContext<'a>,
    ) -> BoxFuture<'a, Result<WriteOutcome, WriterError>> {
        match self.boundary {
            Boundary::Panic => panic!("writer secret"),
            Boundary::Error => Box::pin(async { Err(WriterError::new()) }),
            Boundary::Stop => Box::pin(async { Ok(WriteOutcome::Stopped) }),
            Boundary::Normal => {
                self.batches
                    .lock()
                    .expect("writer batches lock poisoned")
                    .push(items.to_vec());
                Box::pin(async { Ok(WriteOutcome::Written) })
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
    ChunkStep<i32, i32>,
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
        Box::new(reader),
        Arc::new(processor),
        Arc::new(writer),
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

    let report = step.execute(&stop).await;

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

    let report = step.execute(&stop).await;

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

#[tokio::test]
async fn component_errors_roll_back_without_publishing_counts() {
    for (reader_boundary, processor_boundary, writer_boundary, expected) in [
        (
            Boundary::Error,
            Boundary::Normal,
            Boundary::Normal,
            ChunkFailure::Reader,
        ),
        (
            Boundary::Normal,
            Boundary::Error,
            Boundary::Normal,
            ChunkFailure::Processor,
        ),
        (
            Boundary::Normal,
            Boundary::Normal,
            Boundary::Error,
            ChunkFailure::Writer,
        ),
    ] {
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

        let report = step.execute(&stop).await;

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

        let report = step.execute(&stop).await;

        assert_eq!(report.outcome(), ChunkExecutionOutcome::Failed(expected));
        assert!(!format!("{report:?}").contains("secret"));
        assert_eq!(report.committed_counts(), ChunkCounts::default());
    }
}

#[tokio::test]
async fn stop_during_open_chunk_rolls_back_partial_progress() {
    let (mut step, _batches, _completions, evidence) = step(
        Reader::new([1]).with_boundary(Boundary::Stop),
        Processor::normal(),
        Boundary::Normal,
        Boundary::Normal,
        None,
    );
    let (_source, stop) = StopSource::new();

    let report = step.execute(&stop).await;

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

    let report = step.execute(&stop).await;

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

    let report = step.execute(&stop).await;

    assert_eq!(report.outcome(), ChunkExecutionOutcome::Stopped);
    assert_eq!(report.committed_chunks(), ChunkCount::new(1));
    assert_eq!(report.committed_counts().written(), ChunkCount::new(1));
    assert_eq!(report.rolled_back_chunks(), ChunkCount::ZERO);
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
async fn chunk_listeners_nest_and_after_failure_retains_commit() {
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

    let report = step.execute(&stop).await;

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

        let report = step.execute(&stop).await;

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

    let report = step.execute(&stop).await;

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
    let mut job = ChunkJob::new(JobName::new("daily_import").expect("valid job name"), step);
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
    );
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
    );
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
