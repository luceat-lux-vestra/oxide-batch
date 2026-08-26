//! Deterministic retry, backoff, skip, rollback, and item-listener execution.
//!
//! These scenarios cover the M3 chunk-integration slice of `FT-RETRY-001`,
//! `FT-BACKOFF-001`, `FT-SKIP-001`, `FT-ROLLBACK-001`, and
//! `LISTENER-ITEM-001`. Crash, restart, and durable-reservation evidence is
//! owned by the `PostgreSQL` workstream.

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
#[allow(dead_code)]
#[path = "support/secrets.rs"]
mod secrets;

use std::collections::VecDeque;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};

use clock::ManualClock;
use ids::DeterministicIds;

use oxide_batch::{
    BackoffOutcome, BackoffPolicy, BackoffSleeper, BoxFuture, BusinessTransaction,
    BusinessTransactionError, BusinessWriteResult, Checkpoint, ChunkCommitReceipt, ChunkCompletion,
    ChunkCompletionContext, ChunkCompletionError, ChunkCompletionOutcome, ChunkComponentRevisions,
    ChunkCounts, ChunkDeliveryMode, ChunkExecutionOutcome, ChunkExecutionReport, ChunkFailure,
    ChunkFaultProgress, ChunkJob, ChunkListener, ChunkRestartContract, ChunkSize, ChunkStep,
    ChunkTransaction, ChunkTransactionContext, ChunkTransactionError, ChunkTransactionManager,
    ClassifierRevision, ComponentRevision, DefinitionRevision, ExecutionAttempt, ExecutionContext,
    ExecutionCorrelation, FailureCategory, FaultAction, FaultClassifier, FaultDescriptor,
    FaultPhase, FaultPolicy, FaultPolicyError, FaultProgress, FaultRule, FaultRuntime,
    FaultStateStore, InMemoryFaultState, InMemoryJobRepository, InheritedStepProgress,
    ItemListenerContext, ItemListenerSet, ItemProcessor, ItemReader, ItemWriter, JobExecutionId,
    JobInstanceId, JobLauncher, JobName, JobParameters, LifecycleEvent, LifecycleEventKind,
    LifecycleEventSink, ListenerError, ProcessContext, ProcessListener, ProcessOutcome,
    ProcessorError, ReadContext, ReadListener, ReadOutcome, ReaderError, RetryCounts, RetryLimit,
    RetryOrdinal, RetryOutcome, RetryReservation, RetryStateLimit, RollbackDisposition, SkipCounts,
    SkipLimit, SkipListener, StateLimits, StateSchemaId, StateSchemaVersion, StepExecutionId,
    StepName, StopSource, StopToken, WriteContext, WriteListener, WriteOutcome, WriterError,
};
use secrets::assert_sentinel_absent;

// ---------------------------------------------------------------- fixtures --

fn correlation() -> ExecutionCorrelation {
    let attempt =
        |value: u64| ExecutionAttempt::new(NonZeroU64::new(value).expect("attempt is nonzero"));
    ExecutionCorrelation::new(
        JobName::new("fault_job").expect("static job name is valid"),
        JobInstanceId::new(1).expect("static instance id is nonzero"),
        JobExecutionId::new(1).expect("static execution id is nonzero"),
        attempt(1),
        StepName::new("fault_step").expect("static step name is valid"),
        StepExecutionId::new(1).expect("static execution id is nonzero"),
        attempt(1),
    )
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

/// An ordered, shared log of framework and component boundaries.
#[derive(Clone, Default)]
struct Trace(Arc<Mutex<Vec<String>>>);

impl Trace {
    fn record(&self, entry: impl Into<String>) {
        self.0
            .lock()
            .expect("trace lock poisoned")
            .push(entry.into());
    }

    fn entries(&self) -> Vec<String> {
        self.0.lock().expect("trace lock poisoned").clone()
    }

    fn count(&self, entry: &str) -> usize {
        self.entries()
            .iter()
            .filter(|value| *value == entry)
            .count()
    }

    fn position(&self, entry: &str) -> Option<usize> {
        self.entries().iter().position(|value| value == entry)
    }
}

/// A reader that yields a fixed script and can fail a bounded number of times.
struct Reader {
    items: VecDeque<i32>,
    failures: Mutex<u32>,
    error: Option<ReaderError>,
    trace: Trace,
}

impl Reader {
    fn new(items: impl IntoIterator<Item = i32>, trace: Trace) -> Self {
        Self {
            items: items.into_iter().collect(),
            failures: Mutex::new(0),
            error: None,
            trace,
        }
    }

    fn failing(mut self, times: u32, error: ReaderError) -> Self {
        *self.failures.get_mut().expect("failure lock poisoned") = times;
        self.error = Some(error);
        self
    }
}

impl ItemReader<i32> for Reader {
    async fn read(&mut self, _context: ReadContext<'_>) -> Result<ReadOutcome<i32>, ReaderError> {
        self.trace.record("reader");
        let remaining = self.failures.get_mut().expect("failure lock poisoned");
        if *remaining > 0
            && let Some(error) = self.error
        {
            *remaining -= 1;
            return Err(error);
        }
        Ok(self
            .items
            .pop_front()
            .map_or(ReadOutcome::EndOfInput, ReadOutcome::Item))
    }
}

/// A processor that can fail for one nominated input value.
struct Processor {
    failing_item: Option<i32>,
    error: ProcessorError,
    remaining: Mutex<u32>,
    filter: Option<i32>,
    trace: Trace,
}

impl Processor {
    fn new(trace: Trace) -> Self {
        Self {
            failing_item: None,
            error: ProcessorError::new(),
            remaining: Mutex::new(0),
            filter: None,
            trace,
        }
    }

    fn failing(mut self, item: i32, times: u32, error: ProcessorError) -> Self {
        self.failing_item = Some(item);
        self.error = error;
        *self.remaining.get_mut().expect("failure lock poisoned") = times;
        self
    }
}

impl ItemProcessor<i32, i32> for Processor {
    async fn process(
        &self,
        item: &i32,
        _context: ProcessContext<'_>,
    ) -> Result<ProcessOutcome<i32>, ProcessorError> {
        self.trace.record(format!("processor:{item}"));
        if self.failing_item == Some(*item) {
            let mut remaining = self.remaining.lock().expect("failure lock poisoned");
            if *remaining > 0 {
                *remaining -= 1;
                return Err(self.error);
            }
        }
        if self.filter == Some(*item) {
            return Ok(ProcessOutcome::Filtered);
        }
        Ok(ProcessOutcome::Item(item * 10))
    }
}

/// A writer that records accepted batches and can fail a bounded number of
/// times.
struct Writer {
    remaining: Mutex<u32>,
    error: WriterError,
    batches: Arc<Mutex<Vec<Vec<i32>>>>,
    trace: Trace,
}

impl Writer {
    fn new(trace: Trace) -> (Self, Arc<Mutex<Vec<Vec<i32>>>>) {
        let batches = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                remaining: Mutex::new(0),
                error: WriterError::new(),
                batches: Arc::clone(&batches),
                trace,
            },
            batches,
        )
    }

    fn failing(mut self, times: u32, error: WriterError) -> Self {
        *self.remaining.get_mut().expect("failure lock poisoned") = times;
        self.error = error;
        self
    }
}

impl ItemWriter<i32> for Writer {
    async fn write(
        &self,
        items: &[i32],
        _context: WriteContext<'_>,
    ) -> Result<WriteOutcome, WriterError> {
        self.trace.record("writer");
        let mut remaining = self.remaining.lock().expect("failure lock poisoned");
        if *remaining > 0 {
            *remaining -= 1;
            return Err(self.error);
        }
        self.batches
            .lock()
            .expect("batch lock poisoned")
            .push(items.to_vec());
        Ok(WriteOutcome::Written)
    }
}

struct Completion;

impl ChunkCompletion for Completion {
    fn after_commit<'a>(
        &'a self,
        _context: ChunkCompletionContext<'a>,
    ) -> BoxFuture<'a, Result<ChunkCompletionOutcome, ChunkCompletionError>> {
        Box::pin(async { Ok(ChunkCompletionOutcome::Acknowledged) })
    }
}

struct Transactions {
    trace: Trace,
    enlisted: bool,
    commit_error: Option<ChunkTransactionError>,
    accepted: Arc<Mutex<Vec<ChunkFaultProgress>>>,
    inherited: Option<InheritedStepProgress>,
}

impl Transactions {
    fn new(trace: Trace) -> Self {
        Self {
            trace,
            enlisted: false,
            commit_error: None,
            accepted: Arc::new(Mutex::new(Vec::new())),
            inherited: Some(InheritedStepProgress::NONE),
        }
    }

    /// Declares the durable progress a restarted attempt inherits.
    fn inheriting(mut self, inherited: InheritedStepProgress) -> Self {
        self.inherited = Some(inherited);
        self
    }

    /// Makes the durable progress unreadable, as corrupt state would.
    fn unreadable_progress(mut self) -> Self {
        self.inherited = None;
        self
    }

    /// Returns the fault progress every committed chunk made authoritative.
    fn accepted(&self) -> Arc<Mutex<Vec<ChunkFaultProgress>>> {
        Arc::clone(&self.accepted)
    }

    fn enlisted(mut self) -> Self {
        self.enlisted = true;
        self
    }

    fn failing_commit(mut self, error: ChunkTransactionError) -> Self {
        self.commit_error = Some(error);
        self
    }
}

impl ChunkTransactionManager for Transactions {
    fn begin(
        &self,
    ) -> BoxFuture<'_, Result<Box<dyn ChunkTransaction + '_>, ChunkTransactionError>> {
        let transaction = TestTransaction {
            trace: self.trace.clone(),
            business: if self.enlisted {
                Some(NoopBusiness)
            } else {
                None
            },
            commit_error: self.commit_error,
            accepted: Arc::clone(&self.accepted),
        };
        Box::pin(async move { Ok(Box::new(transaction) as Box<dyn ChunkTransaction>) })
    }

    fn inherited_progress(
        &self,
        _context: ChunkTransactionContext,
    ) -> BoxFuture<'_, Result<InheritedStepProgress, ChunkTransactionError>> {
        let inherited = self.inherited;
        Box::pin(async move { inherited.ok_or(ChunkTransactionError::NotCommitted) })
    }
}

struct NoopBusiness;

impl BusinessTransaction for NoopBusiness {
    fn execute<'a>(
        &'a mut self,
        _statement: oxide_batch::BusinessStatement<'a>,
    ) -> BoxFuture<'a, Result<BusinessWriteResult, BusinessTransactionError>> {
        Box::pin(async { Ok(BusinessWriteResult::new(1)) })
    }
}

struct TestTransaction {
    trace: Trace,
    business: Option<NoopBusiness>,
    commit_error: Option<ChunkTransactionError>,
    accepted: Arc<Mutex<Vec<ChunkFaultProgress>>>,
}

impl ChunkTransaction for TestTransaction {
    fn business_transaction(&mut self) -> Option<&mut dyn BusinessTransaction> {
        self.business
            .as_mut()
            .map(|business| business as &mut dyn BusinessTransaction)
    }

    fn commit(
        &mut self,
        _counts: ChunkCounts,
        fault: ChunkFaultProgress,
    ) -> BoxFuture<'_, Result<ChunkCommitReceipt, ChunkTransactionError>> {
        if let Some(error) = self.commit_error {
            self.trace.record("commit_failed");
            return Box::pin(async move { Err(error) });
        }
        self.accepted
            .lock()
            .expect("accepted fault progress lock poisoned")
            .push(fault);
        self.trace.record("commit");
        Box::pin(async { Ok(receipt()) })
    }

    fn rollback(&mut self) -> BoxFuture<'_, Result<(), ChunkTransactionError>> {
        self.trace.record("rollback");
        Box::pin(async { Ok(()) })
    }
}

/// A deterministic sleeper that records requested delays and never waits.
struct RecordingSleeper {
    delays: Mutex<Vec<Duration>>,
    stop_on_wait: Option<StopSource>,
}

impl RecordingSleeper {
    const fn new() -> Self {
        Self {
            delays: Mutex::new(Vec::new()),
            stop_on_wait: None,
        }
    }

    /// Requests cooperative stop while the wait is in progress.
    fn stopping(mut self, source: StopSource) -> Self {
        self.stop_on_wait = Some(source);
        self
    }

    fn recorded(&self) -> Vec<Duration> {
        self.delays.lock().expect("delay lock poisoned").clone()
    }
}

impl BackoffSleeper for RecordingSleeper {
    fn sleep<'a>(&'a self, delay: Duration, stop: &'a StopToken) -> BoxFuture<'a, BackoffOutcome> {
        self.delays.lock().expect("delay lock poisoned").push(delay);
        if let Some(source) = self.stop_on_wait.as_ref() {
            source.request_stop();
        }
        let stopped = stop.is_stop_requested();
        Box::pin(async move {
            if stopped {
                BackoffOutcome::Stopped
            } else {
                BackoffOutcome::Elapsed
            }
        })
    }
}

// ----------------------------------------------------------------- builders --

fn policy(
    rules: impl IntoIterator<Item = FaultRule>,
    retry_limit: u32,
    skip_limit: u64,
    backoff: BackoffPolicy,
) -> FaultPolicy {
    FaultPolicy::new(
        FaultClassifier::new(
            ClassifierRevision::new("fault_test_v1").expect("static revision is valid"),
            rules,
        )
        .expect("static classifier is valid"),
        RetryLimit::new(retry_limit).expect("static retry limit is valid"),
        RetryStateLimit::new(16).expect("static retry state limit is valid"),
        SkipLimit::new(skip_limit),
        backoff,
    )
    .expect("static policy is valid")
}

fn runtime(
    policy: FaultPolicy,
    sleeper: Arc<RecordingSleeper>,
    delivery_mode: ChunkDeliveryMode,
) -> FaultRuntime {
    let state = Arc::new(InMemoryFaultState::new(policy.retry_state_limit()));
    FaultRuntime::new(policy, sleeper, state, delivery_mode).expect("static runtime is valid")
}

fn rule(phase: FaultPhase, category: FailureCategory, action: FaultAction) -> FaultRule {
    FaultRule::new(phase, category, action).expect("static rule is valid")
}

fn chunk_size(value: u32) -> ChunkSize {
    ChunkSize::new(value).expect("static chunk size is nonzero")
}

fn step_name() -> StepName {
    StepName::new("fault_step").expect("static step name is valid")
}

fn block_on<F: Future>(future: F) -> F::Output {
    futures_executor::block_on(future)
}

// ------------------------------------------------------------- retry --------

#[test]
fn retryable_failure_succeeds_within_limit() {
    let trace = Trace::default();
    let (writer, batches) = Writer::new(trace.clone());
    let writer = writer.failing(1, WriterError::with_category(FailureCategory::Timeout));
    let sleeper = Arc::new(RecordingSleeper::new());
    let mut step = ChunkStep::new(
        step_name(),
        chunk_size(2),
        Reader::new([1, 2], trace.clone()),
        Processor::new(trace.clone()),
        writer,
        Arc::new(Transactions::new(trace.clone())),
        Arc::new(Completion),
    )
    .with_fault_runtime(runtime(
        policy(
            [rule(
                FaultPhase::Write,
                FailureCategory::Timeout,
                FaultAction::retry(),
            )],
            2,
            0,
            BackoffPolicy::none(),
        ),
        Arc::clone(&sleeper),
        ChunkDeliveryMode::AtLeastOnce,
    ));

    let report = block_on(step.execute(&correlation(), &StopSource::new().1));

    assert_eq!(report.outcome(), ChunkExecutionOutcome::Completed);
    assert_eq!(report.retry_counts().write(), 1);
    assert_eq!(report.rollback_count(), 1);
    assert_eq!(trace.count("writer"), 2);
    assert_eq!(
        batches.lock().expect("batch lock poisoned").as_slice(),
        [vec![10, 20]]
    );
    assert_eq!(report.committed_counts().written().get(), 2);
}

#[test]
fn retry_exhaustion_uses_initial_plus_reserved_retries() {
    let trace = Trace::default();
    let (writer, _batches) = Writer::new(trace.clone());
    let writer = writer.failing(9, WriterError::with_category(FailureCategory::Timeout));
    let sleeper = Arc::new(RecordingSleeper::new());
    let mut step = ChunkStep::new(
        step_name(),
        chunk_size(1),
        Reader::new([1], trace.clone()),
        Processor::new(trace.clone()),
        writer,
        Arc::new(Transactions::new(trace.clone())),
        Arc::new(Completion),
    )
    .with_fault_runtime(runtime(
        policy(
            [rule(
                FaultPhase::Write,
                FailureCategory::Timeout,
                FaultAction::retry(),
            )],
            2,
            0,
            BackoffPolicy::none(),
        ),
        Arc::clone(&sleeper),
        ChunkDeliveryMode::AtLeastOnce,
    ));

    let report = block_on(step.execute(&correlation(), &StopSource::new().1));

    assert_eq!(
        report.outcome(),
        ChunkExecutionOutcome::Failed(ChunkFailure::Writer)
    );
    // One initial call plus exactly two durably reserved retries.
    assert_eq!(trace.count("writer"), 3);
    assert_eq!(report.retry_counts().write(), 2);
    assert_eq!(report.committed_chunks().get(), 0);
}

#[test]
fn retry_rolls_back_before_reinvoke() {
    let trace = Trace::default();
    let (writer, _batches) = Writer::new(trace.clone());
    let writer = writer.failing(1, WriterError::with_category(FailureCategory::Timeout));
    let sleeper = Arc::new(RecordingSleeper::new());
    let mut step = ChunkStep::new(
        step_name(),
        chunk_size(1),
        Reader::new([1], trace.clone()),
        Processor::new(trace.clone()),
        writer,
        Arc::new(Transactions::new(trace.clone())),
        Arc::new(Completion),
    )
    .with_fault_runtime(runtime(
        policy(
            [rule(
                FaultPhase::Write,
                FailureCategory::Timeout,
                FaultAction::retry(),
            )],
            1,
            0,
            BackoffPolicy::none(),
        ),
        Arc::clone(&sleeper),
        ChunkDeliveryMode::AtLeastOnce,
    ));

    let report = block_on(step.execute(&correlation(), &StopSource::new().1));

    assert_eq!(report.outcome(), ChunkExecutionOutcome::Completed);
    let entries = trace.entries();
    let first_write = entries
        .iter()
        .position(|entry| entry == "writer")
        .expect("the writer ran");
    let rollback = entries
        .iter()
        .position(|entry| entry == "rollback")
        .expect("the attempt rolled back");
    let second_write = entries
        .iter()
        .enumerate()
        .filter(|(_, entry)| *entry == "writer")
        .nth(1)
        .map(|(index, _)| index)
        .expect("the writer was re-invoked");
    assert!(first_write < rollback, "rollback follows the failed call");
    assert!(rollback < second_write, "re-invocation follows rollback");
    assert!(
        entries
            .iter()
            .position(|entry| entry == "commit")
            .expect("commit ran")
            > second_write
    );
}

#[test]
fn backoff_uses_injected_monotonic_time() {
    let trace = Trace::default();
    let (writer, _batches) = Writer::new(trace.clone());
    let writer = writer.failing(2, WriterError::with_category(FailureCategory::Timeout));
    let sleeper = Arc::new(RecordingSleeper::new());
    let mut step = ChunkStep::new(
        step_name(),
        chunk_size(1),
        Reader::new([1], trace.clone()),
        Processor::new(trace.clone()),
        writer,
        Arc::new(Transactions::new(trace.clone())),
        Arc::new(Completion),
    )
    .with_fault_runtime(runtime(
        policy(
            [rule(
                FaultPhase::Write,
                FailureCategory::Timeout,
                FaultAction::retry(),
            )],
            3,
            0,
            BackoffPolicy::exponential(Duration::from_millis(50), 2, Duration::from_secs(5))
                .expect("static backoff is valid"),
        ),
        Arc::clone(&sleeper),
        ChunkDeliveryMode::AtLeastOnce,
    ));

    let report = block_on(step.execute(&correlation(), &StopSource::new().1));

    assert_eq!(report.outcome(), ChunkExecutionOutcome::Completed);
    assert_eq!(
        sleeper.recorded(),
        [Duration::from_millis(50), Duration::from_millis(100)]
    );
}

#[test]
fn stop_during_backoff_consumes_reservation_without_reinvoke() {
    let trace = Trace::default();
    let (source, stop) = StopSource::new();
    let (writer, _batches) = Writer::new(trace.clone());
    let writer = writer.failing(9, WriterError::with_category(FailureCategory::Timeout));
    let sleeper = Arc::new(RecordingSleeper::new().stopping(source));
    let mut step = ChunkStep::new(
        step_name(),
        chunk_size(1),
        Reader::new([1], trace.clone()),
        Processor::new(trace.clone()),
        writer,
        Arc::new(Transactions::new(trace.clone())),
        Arc::new(Completion),
    )
    .with_fault_runtime(runtime(
        policy(
            [rule(
                FaultPhase::Write,
                FailureCategory::Timeout,
                FaultAction::retry(),
            )],
            3,
            0,
            BackoffPolicy::fixed(Duration::from_millis(20)).expect("static backoff is valid"),
        ),
        Arc::clone(&sleeper),
        ChunkDeliveryMode::AtLeastOnce,
    ));

    let report = block_on(step.execute(&correlation(), &stop));

    assert_eq!(report.outcome(), ChunkExecutionOutcome::Stopped);
    // The reservation is consumed even though the component never ran again.
    assert_eq!(report.retry_counts().write(), 1);
    assert_eq!(trace.count("writer"), 1);
    assert_eq!(sleeper.recorded(), [Duration::from_millis(20)]);
    assert_eq!(report.committed_chunks().get(), 0);
}

// ------------------------------------------------------------- skip ---------

#[test]
fn skip_limit_is_shared_across_phases() {
    let trace = Trace::default();
    let (writer, batches) = Writer::new(trace.clone());
    let writer = writer.failing(
        1,
        WriterError::with_category(FailureCategory::UserComponent).with_rolled_back_output(0),
    );
    let sleeper = Arc::new(RecordingSleeper::new());
    let mut step = ChunkStep::new(
        step_name(),
        chunk_size(3),
        Reader::new([1, 2, 3], trace.clone()),
        Processor::new(trace.clone()).failing(
            2,
            9,
            ProcessorError::with_category(FailureCategory::UserComponent),
        ),
        writer,
        Arc::new(Transactions::new(trace.clone())),
        Arc::new(Completion),
    )
    .with_fault_runtime(runtime(
        policy(
            [
                rule(
                    FaultPhase::Process,
                    FailureCategory::UserComponent,
                    FaultAction::skip(RollbackDisposition::Rollback),
                ),
                rule(
                    FaultPhase::Write,
                    FailureCategory::UserComponent,
                    FaultAction::skip(RollbackDisposition::Rollback),
                ),
            ],
            0,
            2,
            BackoffPolicy::none(),
        ),
        Arc::clone(&sleeper),
        ChunkDeliveryMode::AtLeastOnce,
    ));

    let report = block_on(step.execute(&correlation(), &StopSource::new().1));

    assert_eq!(report.outcome(), ChunkExecutionOutcome::Completed);
    assert_eq!(report.skip_counts().process(), 1);
    assert_eq!(report.skip_counts().write(), 1);
    assert_eq!(
        report
            .skip_counts()
            .checked_total()
            .expect("skip totals do not overflow"),
        2
    );
    // Item 2 was skipped in process; item 1's output was skipped in write.
    assert_eq!(
        batches.lock().expect("batch lock poisoned").as_slice(),
        [vec![30]]
    );
}

#[test]
fn next_skip_after_limit_fails() {
    let trace = Trace::default();
    let (writer, _batches) = Writer::new(trace.clone());
    let sleeper = Arc::new(RecordingSleeper::new());
    let mut step = ChunkStep::new(
        step_name(),
        chunk_size(3),
        Reader::new([1, 2, 3], trace.clone()),
        Processor::new(trace.clone()).failing(
            1,
            9,
            ProcessorError::with_category(FailureCategory::UserComponent),
        ),
        writer,
        Arc::new(Transactions::new(trace.clone())),
        Arc::new(Completion),
    )
    .with_fault_runtime(runtime(
        policy(
            [rule(
                FaultPhase::Process,
                FailureCategory::UserComponent,
                FaultAction::skip(RollbackDisposition::Rollback),
            )],
            0,
            0,
            BackoffPolicy::none(),
        ),
        Arc::clone(&sleeper),
        ChunkDeliveryMode::AtLeastOnce,
    ));

    let report = block_on(step.execute(&correlation(), &StopSource::new().1));

    assert_eq!(
        report.outcome(),
        ChunkExecutionOutcome::Failed(ChunkFailure::Processor)
    );
    assert_eq!(report.skip_counts().process(), 0);
    assert_eq!(report.committed_chunks().get(), 0);
}

#[test]
fn skip_count_commits_with_chunk() {
    let trace = Trace::default();
    let (writer, _batches) = Writer::new(trace.clone());
    let sleeper = Arc::new(RecordingSleeper::new());
    let mut step = ChunkStep::new(
        step_name(),
        chunk_size(2),
        Reader::new([1, 2], trace.clone()),
        Processor::new(trace.clone()).failing(
            1,
            9,
            ProcessorError::with_category(FailureCategory::UserComponent),
        ),
        writer,
        Arc::new(
            Transactions::new(trace.clone()).failing_commit(ChunkTransactionError::NotCommitted),
        ),
        Arc::new(Completion),
    )
    .with_fault_runtime(runtime(
        policy(
            [rule(
                FaultPhase::Process,
                FailureCategory::UserComponent,
                FaultAction::skip(RollbackDisposition::Rollback),
            )],
            0,
            4,
            BackoffPolicy::none(),
        ),
        Arc::clone(&sleeper),
        ChunkDeliveryMode::AtLeastOnce,
    ));

    let report = block_on(step.execute(&correlation(), &StopSource::new().1));

    assert_eq!(
        report.outcome(),
        ChunkExecutionOutcome::Failed(ChunkFailure::TransactionCommit)
    );
    // The skip was accepted but never committed, so it is not authoritative.
    assert_eq!(report.skip_counts().process(), 0);
}

#[test]
fn write_skip_requires_located_known_rollback() {
    for (located, expected) in [
        (false, ChunkExecutionOutcome::Failed(ChunkFailure::Writer)),
        (true, ChunkExecutionOutcome::Completed),
    ] {
        let trace = Trace::default();
        let (writer, _batches) = Writer::new(trace.clone());
        let error = WriterError::with_category(FailureCategory::UserComponent);
        let writer = writer.failing(
            1,
            if located {
                error.with_rolled_back_output(0)
            } else {
                error
            },
        );
        let sleeper = Arc::new(RecordingSleeper::new());
        let mut step = ChunkStep::new(
            step_name(),
            chunk_size(1),
            Reader::new([1], trace.clone()),
            Processor::new(trace.clone()),
            writer,
            Arc::new(Transactions::new(trace.clone())),
            Arc::new(Completion),
        )
        .with_fault_runtime(runtime(
            policy(
                [rule(
                    FaultPhase::Write,
                    FailureCategory::UserComponent,
                    FaultAction::skip(RollbackDisposition::Rollback),
                )],
                0,
                4,
                BackoffPolicy::none(),
            ),
            Arc::clone(&sleeper),
            ChunkDeliveryMode::AtLeastOnce,
        ));

        let report = block_on(step.execute(&correlation(), &StopSource::new().1));

        assert_eq!(report.outcome(), expected, "located = {located}");
        assert_eq!(report.skip_counts().write(), u64::from(located));
    }
}

#[test]
fn read_skip_requires_forward_checkpoint_proof() {
    for (advanced, expected) in [
        (false, ChunkExecutionOutcome::Failed(ChunkFailure::Reader)),
        (true, ChunkExecutionOutcome::Completed),
    ] {
        let trace = Trace::default();
        let (writer, _batches) = Writer::new(trace.clone());
        let sleeper = Arc::new(RecordingSleeper::new());
        let mut step = ChunkStep::new(
            step_name(),
            chunk_size(2),
            Reader::new([1], trace.clone()).failing(
                1,
                ReaderError::with_category(FailureCategory::UserComponent)
                    .with_checkpoint_advanced(advanced),
            ),
            Processor::new(trace.clone()),
            writer,
            Arc::new(Transactions::new(trace.clone())),
            Arc::new(Completion),
        )
        .with_fault_runtime(runtime(
            policy(
                [rule(
                    FaultPhase::Read,
                    FailureCategory::UserComponent,
                    FaultAction::skip(RollbackDisposition::Rollback),
                )],
                0,
                4,
                BackoffPolicy::none(),
            ),
            Arc::clone(&sleeper),
            ChunkDeliveryMode::AtLeastOnce,
        ));

        let report = block_on(step.execute(&correlation(), &StopSource::new().1));

        assert_eq!(report.outcome(), expected, "advanced = {advanced}");
        assert_eq!(report.skip_counts().read(), u64::from(advanced));
    }
}

// ------------------------------------------------------- rollback/no-rollback

#[test]
fn commit_safe_skip_requires_capability() {
    let commit_safe = || {
        policy(
            [rule(
                FaultPhase::Process,
                FailureCategory::UserComponent,
                FaultAction::skip(RollbackDisposition::CommitSafeSkip),
            )],
            0,
            4,
            BackoffPolicy::none(),
        )
    };

    // The declared delivery mode cannot commit a skip atomically.
    let rejected = FaultRuntime::new(
        commit_safe(),
        Arc::new(RecordingSleeper::new()),
        Arc::new(InMemoryFaultState::new(
            RetryStateLimit::new(4).expect("static limit is valid"),
        )),
        ChunkDeliveryMode::AtLeastOnce,
    );
    assert_eq!(
        rejected.err(),
        Some(FaultPolicyError::CommitSafeSkipUnsupported)
    );

    // The mode is declared, but the resource does not enlist a transaction.
    let trace = Trace::default();
    let (writer, _batches) = Writer::new(trace.clone());
    let mut step = ChunkStep::new(
        step_name(),
        chunk_size(1),
        Reader::new([1], trace.clone()),
        Processor::new(trace.clone()),
        writer,
        Arc::new(Transactions::new(trace.clone())),
        Arc::new(Completion),
    )
    .with_fault_runtime(runtime(
        commit_safe(),
        Arc::new(RecordingSleeper::new()),
        ChunkDeliveryMode::AtomicSameResource,
    ));

    let report = block_on(step.execute(&correlation(), &StopSource::new().1));

    assert_eq!(
        report.outcome(),
        ChunkExecutionOutcome::Failed(ChunkFailure::UnsupportedCapability)
    );
    // The capability mismatch failed before any user work.
    assert!(trace.entries().iter().all(|entry| entry != "reader"));
}

#[test]
fn commit_safe_skip_counts_a_skip_without_rolling_back() {
    let trace = Trace::default();
    let (writer, batches) = Writer::new(trace.clone());
    let sleeper = Arc::new(RecordingSleeper::new());
    let mut step = ChunkStep::new(
        step_name(),
        chunk_size(2),
        Reader::new([1, 2], trace.clone()),
        Processor::new(trace.clone()).failing(
            1,
            9,
            ProcessorError::with_category(FailureCategory::UserComponent),
        ),
        writer,
        Arc::new(Transactions::new(trace.clone()).enlisted()),
        Arc::new(Completion),
    )
    .with_fault_runtime(runtime(
        policy(
            [rule(
                FaultPhase::Process,
                FailureCategory::UserComponent,
                FaultAction::skip(RollbackDisposition::CommitSafeSkip),
            )],
            0,
            4,
            BackoffPolicy::none(),
        ),
        Arc::clone(&sleeper),
        ChunkDeliveryMode::AtomicSameResource,
    ));

    let report = block_on(step.execute(&correlation(), &StopSource::new().1));

    assert_eq!(report.outcome(), ChunkExecutionOutcome::Completed);
    assert_eq!(report.skip_counts().process(), 1);
    assert_eq!(report.no_rollback_count(), 1);
    assert_eq!(report.rolled_back_chunks().get(), 0);
    assert_eq!(
        batches.lock().expect("batch lock poisoned").as_slice(),
        [vec![20]]
    );
}

#[test]
fn unknown_commit_is_never_retried() {
    let trace = Trace::default();
    let (writer, _batches) = Writer::new(trace.clone());
    let sleeper = Arc::new(RecordingSleeper::new());
    let mut step = ChunkStep::new(
        step_name(),
        chunk_size(1),
        Reader::new([1], trace.clone()),
        Processor::new(trace.clone()),
        writer,
        Arc::new(
            Transactions::new(trace.clone())
                .failing_commit(ChunkTransactionError::CommitOutcomeUnknown),
        ),
        Arc::new(Completion),
    )
    .with_fault_runtime(runtime(
        policy(
            [rule(
                FaultPhase::Write,
                FailureCategory::Timeout,
                FaultAction::retry(),
            )],
            2,
            4,
            BackoffPolicy::none(),
        ),
        Arc::clone(&sleeper),
        ChunkDeliveryMode::AtLeastOnce,
    ));

    let report = block_on(step.execute(&correlation(), &StopSource::new().1));

    assert_eq!(report.outcome(), ChunkExecutionOutcome::Unknown);
    assert_eq!(report.retry_counts().write(), 0);
    assert!(sleeper.recorded().is_empty());
    assert_eq!(
        trace.count("rollback"),
        0,
        "an unknown commit is not rolled back"
    );
}

#[test]
fn unknown_commit_category_is_never_retried_or_skipped() {
    let trace = Trace::default();
    let (writer, _batches) = Writer::new(trace.clone());
    let writer = writer.failing(
        9,
        WriterError::with_category(FailureCategory::UnknownCommit),
    );
    let sleeper = Arc::new(RecordingSleeper::new());
    let mut step = ChunkStep::new(
        step_name(),
        chunk_size(1),
        Reader::new([1], trace.clone()),
        Processor::new(trace.clone()),
        writer,
        Arc::new(Transactions::new(trace.clone())),
        Arc::new(Completion),
    )
    .with_fault_runtime(runtime(
        policy(
            [rule(
                FaultPhase::Write,
                FailureCategory::Timeout,
                FaultAction::retry(),
            )],
            2,
            4,
            BackoffPolicy::none(),
        ),
        Arc::clone(&sleeper),
        ChunkDeliveryMode::AtLeastOnce,
    ));

    let report = block_on(step.execute(&correlation(), &StopSource::new().1));

    assert_eq!(report.outcome(), ChunkExecutionOutcome::Unknown);
    assert_eq!(trace.count("writer"), 1);
    assert_eq!(report.retry_counts().write(), 0);
}

// ----------------------------------------------------------- item listeners --

#[derive(Clone)]
struct TracingListener {
    label: &'static str,
    trace: Trace,
    fail_at: Option<&'static str>,
}

impl TracingListener {
    const fn new(label: &'static str, trace: Trace) -> Self {
        Self {
            label,
            trace,
            fail_at: None,
        }
    }

    const fn failing(mut self, boundary: &'static str) -> Self {
        self.fail_at = Some(boundary);
        self
    }

    fn enter(&self, boundary: &'static str) -> BoxFuture<'_, Result<(), ListenerError>> {
        self.trace.record(format!("{}:{boundary}", self.label));
        let failed = self.fail_at == Some(boundary);
        Box::pin(async move {
            if failed {
                Err(ListenerError::new())
            } else {
                Ok(())
            }
        })
    }
}

impl ReadListener<i32> for TracingListener {
    fn before_read<'a>(
        &'a self,
        _context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        self.enter("before_read")
    }

    fn after_read<'a>(
        &'a self,
        _item: &'a i32,
        _context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        self.enter("after_read")
    }

    fn on_read_error<'a>(
        &'a self,
        _fault: FaultDescriptor,
        _context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        self.enter("read_error")
    }
}

impl ProcessListener<i32, i32> for TracingListener {
    fn before_process<'a>(
        &'a self,
        _input: &'a i32,
        _context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        self.enter("before_process")
    }

    fn after_process<'a>(
        &'a self,
        _input: &'a i32,
        _output: Option<&'a i32>,
        _context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        self.enter("after_process")
    }
}

impl WriteListener<i32> for TracingListener {
    fn before_write<'a>(
        &'a self,
        _outputs: &'a [i32],
        _context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        self.enter("before_write")
    }

    fn after_write<'a>(
        &'a self,
        _outputs: &'a [i32],
        _context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        self.enter("after_write")
    }

    fn on_write_error<'a>(
        &'a self,
        _outputs: &'a [i32],
        _fault: FaultDescriptor,
        _context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        self.enter("write_error")
    }
}

impl SkipListener<i32, i32> for TracingListener {
    fn on_skip_in_process<'a>(
        &'a self,
        _input: &'a i32,
        _fault: FaultDescriptor,
        _context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        self.enter("skip_process")
    }
}

struct RetryTracer {
    trace: Trace,
}

impl oxide_batch::RetryListener for RetryTracer {
    fn before_retry<'a>(
        &'a self,
        _fault: FaultDescriptor,
        _context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        self.trace.record("before_retry");
        Box::pin(async { Ok(()) })
    }

    fn after_retry<'a>(
        &'a self,
        _fault: FaultDescriptor,
        outcome: RetryOutcome,
        _context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        self.trace.record(format!("after_retry:{outcome:?}"));
        Box::pin(async { Ok(()) })
    }

    fn on_retry_exhausted<'a>(
        &'a self,
        _fault: FaultDescriptor,
        _context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        self.trace.record("retry_exhausted");
        Box::pin(async { Ok(()) })
    }
}

#[test]
fn item_listeners_nest_and_reverse_after_order() {
    let trace = Trace::default();
    let (writer, _batches) = Writer::new(trace.clone());
    let listeners = ItemListenerSet::<i32, i32>::new()
        .with_read_listener(Arc::new(TracingListener::new("first", trace.clone())))
        .expect("registration is bounded")
        .with_read_listener(Arc::new(TracingListener::new("second", trace.clone())))
        .expect("registration is bounded")
        .with_process_listener(Arc::new(TracingListener::new("first", trace.clone())))
        .expect("registration is bounded")
        .with_process_listener(Arc::new(TracingListener::new("second", trace.clone())))
        .expect("registration is bounded")
        .with_write_listener(Arc::new(TracingListener::new("first", trace.clone())))
        .expect("registration is bounded")
        .with_write_listener(Arc::new(TracingListener::new("second", trace.clone())))
        .expect("registration is bounded");

    let mut step = ChunkStep::new(
        step_name(),
        chunk_size(1),
        Reader::new([1], trace.clone()),
        Processor::new(trace.clone()),
        writer,
        Arc::new(Transactions::new(trace.clone())),
        Arc::new(Completion),
    )
    .with_item_listeners(listeners);

    let report = block_on(step.execute(&correlation(), &StopSource::new().1));

    assert_eq!(report.outcome(), ChunkExecutionOutcome::Completed);
    let entries = trace.entries();
    let prefix: Vec<&str> = entries.iter().map(String::as_str).take(16).collect();
    assert_eq!(
        prefix,
        [
            "first:before_read",
            "second:before_read",
            "reader",
            "second:after_read",
            "first:after_read",
            "first:before_process",
            "second:before_process",
            "processor:1",
            "second:after_process",
            "first:after_process",
            "first:before_write",
            "second:before_write",
            "writer",
            "second:after_write",
            "first:after_write",
            "commit",
        ]
    );
}

#[test]
fn item_error_precedes_policy_decision() {
    let trace = Trace::default();
    let (writer, _batches) = Writer::new(trace.clone());
    let writer = writer.failing(1, WriterError::with_category(FailureCategory::Timeout));
    let listeners = ItemListenerSet::<i32, i32>::new()
        .with_write_listener(Arc::new(TracingListener::new("audit", trace.clone())))
        .expect("registration is bounded")
        .with_retry_listener(Arc::new(RetryTracer {
            trace: trace.clone(),
        }))
        .expect("registration is bounded");

    let mut step = ChunkStep::new(
        step_name(),
        chunk_size(1),
        Reader::new([1], trace.clone()),
        Processor::new(trace.clone()),
        writer,
        Arc::new(Transactions::new(trace.clone())),
        Arc::new(Completion),
    )
    .with_item_listeners(listeners)
    .with_fault_runtime(runtime(
        policy(
            [rule(
                FaultPhase::Write,
                FailureCategory::Timeout,
                FaultAction::retry(),
            )],
            1,
            0,
            BackoffPolicy::none(),
        ),
        Arc::new(RecordingSleeper::new()),
        ChunkDeliveryMode::AtLeastOnce,
    ));

    let report = block_on(step.execute(&correlation(), &StopSource::new().1));

    assert_eq!(report.outcome(), ChunkExecutionOutcome::Completed);
    let error = trace
        .position("audit:write_error")
        .expect("the error callback ran");
    let rollback = trace.position("rollback").expect("the attempt rolled back");
    let before_retry = trace.position("before_retry").expect("the retry scope ran");
    assert!(error < rollback, "the item error precedes the decision");
    assert!(rollback < before_retry, "reservation follows rollback");
    assert_eq!(trace.count("after_retry:Recovered"), 1);
    assert_eq!(trace.count("retry_exhausted"), 0);
}

#[test]
fn retry_exhaustion_runs_its_callback_once() {
    let trace = Trace::default();
    let (writer, _batches) = Writer::new(trace.clone());
    let writer = writer.failing(9, WriterError::with_category(FailureCategory::Timeout));
    let listeners = ItemListenerSet::<i32, i32>::new()
        .with_retry_listener(Arc::new(RetryTracer {
            trace: trace.clone(),
        }))
        .expect("registration is bounded");

    let mut step = ChunkStep::new(
        step_name(),
        chunk_size(1),
        Reader::new([1], trace.clone()),
        Processor::new(trace.clone()),
        writer,
        Arc::new(Transactions::new(trace.clone())),
        Arc::new(Completion),
    )
    .with_item_listeners(listeners)
    .with_fault_runtime(runtime(
        policy(
            [rule(
                FaultPhase::Write,
                FailureCategory::Timeout,
                FaultAction::retry(),
            )],
            1,
            0,
            BackoffPolicy::none(),
        ),
        Arc::new(RecordingSleeper::new()),
        ChunkDeliveryMode::AtLeastOnce,
    ));

    let report = block_on(step.execute(&correlation(), &StopSource::new().1));

    assert_eq!(
        report.outcome(),
        ChunkExecutionOutcome::Failed(ChunkFailure::Writer)
    );
    assert_eq!(trace.count("before_retry"), 1);
    assert_eq!(trace.count("after_retry:Failed"), 1);
    assert_eq!(trace.count("retry_exhausted"), 1);
}

#[test]
fn skip_listener_effect_commits_once_with_skip() {
    let trace = Trace::default();
    let (writer, _batches) = Writer::new(trace.clone());
    let listeners = ItemListenerSet::<i32, i32>::new()
        .with_skip_listener(Arc::new(TracingListener::new("audit", trace.clone())))
        .expect("registration is bounded");

    let mut step = ChunkStep::new(
        step_name(),
        chunk_size(2),
        Reader::new([1, 2], trace.clone()),
        Processor::new(trace.clone()).failing(
            1,
            9,
            ProcessorError::with_category(FailureCategory::UserComponent),
        ),
        writer,
        Arc::new(Transactions::new(trace.clone()).enlisted()),
        Arc::new(Completion),
    )
    .with_item_listeners(listeners)
    .with_fault_runtime(runtime(
        policy(
            [rule(
                FaultPhase::Process,
                FailureCategory::UserComponent,
                FaultAction::skip(RollbackDisposition::CommitSafeSkip),
            )],
            0,
            4,
            BackoffPolicy::none(),
        ),
        Arc::new(RecordingSleeper::new()),
        ChunkDeliveryMode::AtomicSameResource,
    ));

    let report = block_on(step.execute(&correlation(), &StopSource::new().1));

    assert_eq!(report.outcome(), ChunkExecutionOutcome::Completed);
    assert_eq!(report.skip_counts().process(), 1);
    assert_eq!(trace.count("audit:skip_process"), 1);
    let skip = trace
        .position("audit:skip_process")
        .expect("the skip callback ran");
    let commit = trace.position("commit").expect("the chunk committed");
    assert!(skip < commit, "the skip callback precedes its commit");
}

#[test]
fn listener_failure_rolls_back_and_redacts() {
    let trace = Trace::default();
    let (writer, batches) = Writer::new(trace.clone());
    let listeners = ItemListenerSet::<i32, i32>::new()
        .with_write_listener(Arc::new(
            TracingListener::new("audit", trace.clone()).failing("after_write"),
        ))
        .expect("registration is bounded");

    let mut step = ChunkStep::new(
        step_name(),
        chunk_size(1),
        Reader::new([1], trace.clone()),
        Processor::new(trace.clone()),
        writer,
        Arc::new(Transactions::new(trace.clone())),
        Arc::new(Completion),
    )
    .with_item_listeners(listeners);

    let report = block_on(step.execute(&correlation(), &StopSource::new().1));

    assert_eq!(
        report.outcome(),
        ChunkExecutionOutcome::Failed(ChunkFailure::ItemListener)
    );
    assert_eq!(
        trace.count("commit"),
        0,
        "an uncommitted chunk cannot commit"
    );
    assert_eq!(trace.count("rollback"), 1);
    assert_eq!(report.committed_chunks().get(), 0);
    // The non-transactional writer already ran; only the commit is prevented.
    assert_eq!(batches.lock().expect("batch lock poisoned").len(), 1);
    let failures = report.item_listener_failures();
    assert_eq!(failures.len(), 1);
    assert_eq!(
        failures[0].phase(),
        oxide_batch::ItemListenerPhase::AfterWrite
    );
    assert_redacted(&report);
}

fn assert_redacted(report: &ChunkExecutionReport) {
    let rendered = format!("{report:?}");
    assert_sentinel_absent([("chunk execution report", rendered.as_str())]);
    assert!(
        !rendered.contains("audit"),
        "the report exposes no component or listener payload"
    );
}

// ------------------------------------------ cross-family order fixtures -----
//
// `item_listeners_nest_and_reverse_after_order` and the tests above each
// exercise one listener family (or one family plus retry/skip) against the
// chunk lifecycle in isolation. The two tests below assert one combined,
// deterministic order across the chunk listener, every item-listener family,
// and the fault runtime's retry/skip callbacks together, per
// `LISTENER-ITEM-001`'s "complete taxonomy" requirement.

/// A [`ChunkListener`] that records into the shared [`Trace`].
struct ChunkTracer {
    trace: Trace,
}

impl ChunkListener for ChunkTracer {
    fn before_chunk<'a>(
        &'a self,
        _context: oxide_batch::ChunkListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), oxide_batch::ChunkListenerError>> {
        self.trace.record("chunk:before");
        Box::pin(async { Ok(()) })
    }

    fn after_chunk<'a>(
        &'a self,
        _context: oxide_batch::ChunkListenerContext<'a>,
        outcome: oxide_batch::ChunkAttemptOutcome,
    ) -> BoxFuture<'a, Result<(), oxide_batch::ChunkListenerError>> {
        self.trace.record(format!("chunk:after:{outcome:?}"));
        Box::pin(async { Ok(()) })
    }
}

/// A processor that fails one nominated item once with a retryable category,
/// and fails a second nominated item every time with a skippable category --
/// so one chunk can exercise both the retry and skip scopes.
struct DualFaultProcessor {
    retry_once_for: i32,
    skip_for: i32,
    retried: Mutex<bool>,
    trace: Trace,
}

impl ItemProcessor<i32, i32> for DualFaultProcessor {
    async fn process(
        &self,
        item: &i32,
        _context: ProcessContext<'_>,
    ) -> Result<ProcessOutcome<i32>, ProcessorError> {
        self.trace.record(format!("processor:{item}"));
        if *item == self.retry_once_for {
            let mut retried = self.retried.lock().expect("retry flag lock poisoned");
            if !*retried {
                *retried = true;
                return Err(ProcessorError::with_category(FailureCategory::Timeout));
            }
        }
        if *item == self.skip_for {
            return Err(ProcessorError::with_category(
                FailureCategory::UserComponent,
            ));
        }
        Ok(ProcessOutcome::Item(item * 10))
    }
}

#[test]
fn chunk_read_process_write_and_retry_listeners_interleave_in_one_committed_attempt() {
    let trace = Trace::default();
    let (writer, batches) = Writer::new(trace.clone());
    let listeners = ItemListenerSet::<i32, i32>::new()
        .with_read_listener(Arc::new(TracingListener::new("audit", trace.clone())))
        .expect("registration is bounded")
        .with_process_listener(Arc::new(TracingListener::new("audit", trace.clone())))
        .expect("registration is bounded")
        .with_write_listener(Arc::new(TracingListener::new("audit", trace.clone())))
        .expect("registration is bounded")
        .with_retry_listener(Arc::new(RetryTracer {
            trace: trace.clone(),
        }))
        .expect("registration is bounded");

    let mut step = ChunkStep::new(
        step_name(),
        chunk_size(1),
        Reader::new([1], trace.clone()),
        DualFaultProcessor {
            retry_once_for: 1,
            skip_for: -1,
            retried: Mutex::new(false),
            trace: trace.clone(),
        },
        writer,
        Arc::new(Transactions::new(trace.clone())),
        Arc::new(Completion),
    )
    .with_item_listeners(listeners)
    .with_chunk_listener(Arc::new(ChunkTracer {
        trace: trace.clone(),
    }))
    .with_fault_runtime(runtime(
        policy(
            [rule(
                FaultPhase::Process,
                FailureCategory::Timeout,
                FaultAction::retry(),
            )],
            1,
            0,
            BackoffPolicy::none(),
        ),
        Arc::new(RecordingSleeper::new()),
        ChunkDeliveryMode::AtLeastOnce,
    ));

    let report = block_on(step.execute(&correlation(), &StopSource::new().1));

    assert_eq!(report.outcome(), ChunkExecutionOutcome::Completed);
    assert_eq!(
        batches.lock().expect("batch lock poisoned").as_slice(),
        [vec![10]]
    );
    let entries = trace.entries();
    assert_eq!(
        entries,
        [
            // Attempt 1: process fails, so the chunk rolls back and a retry
            // is reserved. `TracingListener` does not override
            // `on_process_error` (its default is a silent no-op), so no
            // "audit:process_error" entry is expected here.
            "chunk:before",
            "audit:before_read",
            "reader",
            "audit:after_read",
            "audit:before_process",
            "processor:1",
            "rollback",
            "before_retry",
            "chunk:after:RolledBack",
            // Attempt 2 (the reserved retry): the already-buffered item is
            // not re-read -- only the failed phase (process) re-runs -- and
            // this attempt commits.
            "chunk:before",
            "audit:before_process",
            "processor:1",
            "audit:after_process",
            "after_retry:Recovered",
            "audit:before_write",
            "writer",
            "audit:after_write",
            "commit",
            "chunk:after:Committed",
            // Attempt 3: the step's final, empty end-of-input probe -- read
            // observes end-of-input with nothing buffered, so the runtime
            // discards that empty attempt's transaction before reporting the
            // step `Completed`.
            "chunk:before",
            "audit:before_read",
            "reader",
            "rollback",
            "chunk:after:RolledBack",
        ],
        "the chunk, item, and retry listener families interleave in one \
         deterministic order across the failed and recovered attempt"
    );
}

#[test]
fn chunk_and_item_listeners_observe_a_skip_before_its_commit() {
    let trace = Trace::default();
    let (writer, batches) = Writer::new(trace.clone());
    let listeners = ItemListenerSet::<i32, i32>::new()
        .with_read_listener(Arc::new(TracingListener::new("audit", trace.clone())))
        .expect("registration is bounded")
        .with_write_listener(Arc::new(TracingListener::new("audit", trace.clone())))
        .expect("registration is bounded")
        .with_skip_listener(Arc::new(TracingListener::new("audit", trace.clone())))
        .expect("registration is bounded");

    let mut step = ChunkStep::new(
        step_name(),
        chunk_size(2),
        Reader::new([1, 2], trace.clone()),
        DualFaultProcessor {
            retry_once_for: -1,
            skip_for: 2,
            retried: Mutex::new(false),
            trace: trace.clone(),
        },
        writer,
        Arc::new(Transactions::new(trace.clone()).enlisted()),
        Arc::new(Completion),
    )
    .with_item_listeners(listeners)
    .with_chunk_listener(Arc::new(ChunkTracer {
        trace: trace.clone(),
    }))
    .with_fault_runtime(runtime(
        policy(
            [rule(
                FaultPhase::Process,
                FailureCategory::UserComponent,
                FaultAction::skip(RollbackDisposition::CommitSafeSkip),
            )],
            0,
            4,
            BackoffPolicy::none(),
        ),
        Arc::new(RecordingSleeper::new()),
        ChunkDeliveryMode::AtomicSameResource,
    ));

    let report = block_on(step.execute(&correlation(), &StopSource::new().1));

    assert_eq!(report.outcome(), ChunkExecutionOutcome::Completed);
    assert_eq!(report.skip_counts().process(), 1);
    assert_eq!(
        batches.lock().expect("batch lock poisoned").as_slice(),
        [vec![10]]
    );

    let before_chunk = trace.position("chunk:before").expect("chunk listener ran");
    let skip = trace
        .position("audit:skip_process")
        .expect("the skip callback ran");
    let write = trace.position("audit:before_write").expect("write ran");
    let commit = trace.position("commit").expect("the chunk committed");
    let after_chunk = trace
        .position("chunk:after:Committed")
        .expect("chunk listener observed the commit");

    assert!(before_chunk < write, "chunk listener brackets item work");
    assert!(skip < commit, "the skip callback precedes its commit");
    assert!(commit < after_chunk, "chunk listener observes after commit");
}

// ------------------------------------------------------- reservation state --

#[test]
fn stale_retry_reservation_loses_cas() {
    let state = InMemoryFaultState::new(RetryStateLimit::new(2).expect("static limit is valid"));
    let key = reservation_key();
    let first = RetryReservation::new(
        key,
        FaultPhase::Write,
        FailureCategory::Timeout,
        RetryOrdinal::new(1).expect("static ordinal is valid"),
    );

    assert!(block_on(state.reserve(first)).is_ok());
    // The same ordinal cannot be spent twice.
    assert_eq!(
        block_on(state.reserve(first)).err(),
        Some(oxide_batch::FaultStateError::StaleReservation)
    );
    // Skipping an ordinal also loses.
    let skipped = RetryReservation::new(
        key,
        FaultPhase::Write,
        FailureCategory::Timeout,
        RetryOrdinal::new(3).expect("static ordinal is valid"),
    );
    assert_eq!(
        block_on(state.reserve(skipped)).err(),
        Some(oxide_batch::FaultStateError::StaleReservation)
    );
    assert_eq!(
        block_on(state.reserved_ordinal(key)).expect("state is available"),
        Some(RetryOrdinal::new(1).expect("static ordinal is valid"))
    );
}

#[test]
fn retry_state_capacity_fails_before_reserving_another_key() {
    let state = InMemoryFaultState::new(RetryStateLimit::new(1).expect("static limit is valid"));
    let first = RetryReservation::new(
        reservation_key(),
        FaultPhase::Write,
        FailureCategory::Timeout,
        RetryOrdinal::new(1).expect("static ordinal is valid"),
    );
    assert!(block_on(state.reserve(first)).is_ok());
    assert_eq!(block_on(state.unresolved()).expect("state is available"), 1);

    let second = RetryReservation::new(
        other_reservation_key(),
        FaultPhase::Process,
        FailureCategory::Timeout,
        RetryOrdinal::new(1).expect("static ordinal is valid"),
    );
    assert_eq!(
        block_on(state.reserve(second)).err(),
        Some(oxide_batch::FaultStateError::CapacityExhausted { max: 1 })
    );

    // A resolved key is cleared in the commit that advances the checkpoint.
    assert!(block_on(state.resolve(first.key())).is_ok());
    assert!(block_on(state.clear_resolved()).is_ok());
    assert_eq!(block_on(state.unresolved()).expect("state is available"), 0);
    assert!(block_on(state.reserve(second)).is_ok());
}

fn reservation_key() -> oxide_batch::RetryKey {
    oxide_batch::RetryKey::from_bytes([1; 32])
}

fn other_reservation_key() -> oxide_batch::RetryKey {
    oxide_batch::RetryKey::from_bytes([2; 32])
}

// ------------------------------------------------------------------ events --

#[derive(Default)]
struct EventRecorder(Mutex<Vec<(LifecycleEventKind, Vec<(String, String)>)>>);

impl LifecycleEventSink for EventRecorder {
    fn emit(&self, event: &LifecycleEvent) {
        self.0.lock().expect("event lock poisoned").push((
            event.kind(),
            event
                .span_fields()
                .iter()
                .map(|field| (field.key().to_owned(), field.value().to_owned()))
                .collect(),
        ));
    }
}

fn chunk_revisions(delivery_mode: ChunkDeliveryMode) -> ChunkComponentRevisions {
    let revision =
        |value: &str| ComponentRevision::new(value).expect("static component revision is valid");
    ChunkComponentRevisions::new(
        revision("reader-v1"),
        revision("processor-v1"),
        revision("writer-v1"),
        revision("checkpoint-v1"),
        ChunkRestartContract::new(
            StateSchemaId::new("test.position").expect("static schema is valid"),
            StateSchemaVersion::new(1).expect("static schema version is valid"),
            StateSchemaId::new("test.context").expect("static schema is valid"),
            StateSchemaVersion::new(1).expect("static schema version is valid"),
            delivery_mode,
        ),
    )
}

/// Builds a one-step chunk job whose process phase skips item 2.
fn skipping_job(
    transactions: Arc<Transactions>,
    skip_limit: u64,
) -> ChunkJob<i32, i32, Reader, Processor, Writer> {
    let trace = Trace::default();
    let (writer, _batches) = Writer::new(trace.clone());
    let step = ChunkStep::new(
        step_name(),
        chunk_size(3),
        Reader::new([1, 2, 3], trace.clone()),
        Processor::new(trace.clone()).failing(
            2,
            9,
            ProcessorError::with_category(FailureCategory::UserComponent),
        ),
        writer,
        transactions,
        Arc::new(Completion),
    )
    .with_fault_runtime(runtime(
        policy(
            [rule(
                FaultPhase::Process,
                FailureCategory::UserComponent,
                FaultAction::skip(RollbackDisposition::Rollback),
            )],
            0,
            skip_limit,
            BackoffPolicy::none(),
        ),
        Arc::new(RecordingSleeper::new()),
        ChunkDeliveryMode::AtLeastOnce,
    ));
    ChunkJob::new(
        JobName::new("skip_job").expect("static job name is valid"),
        step,
        DefinitionRevision::new("test-v1").expect("static definition revision is valid"),
        &chunk_revisions(ChunkDeliveryMode::AtLeastOnce),
    )
    .expect("static chunk definition is valid")
}

async fn launch(job: &mut ChunkJob<i32, i32, Reader, Processor, Writer>) -> ChunkExecutionReport {
    let clock = ManualClock::new(UNIX_EPOCH + Duration::from_secs(100));
    let generator = DeterministicIds::new(NonZeroU64::MIN);
    let repository =
        InMemoryJobRepository::new(Arc::new(clock.clone()), Arc::new(generator.clone()));
    let launcher = JobLauncher::new(&repository, &clock, &generator);
    let (_source, stop) = StopSource::new();
    launcher
        .launch_chunk(job, &JobParameters::new(), &stop)
        .await
        .expect("chunk launch must complete")
        .chunk()
        .expect("the chunk body ran")
        .clone()
}

#[tokio::test]
async fn accepted_skips_are_committed_as_one_delta() {
    let transactions = Arc::new(Transactions::new(Trace::default()));
    let accepted = transactions.accepted();
    let mut job = skipping_job(Arc::clone(&transactions), 4);

    let report = launch(&mut job).await;

    assert_eq!(report.outcome(), ChunkExecutionOutcome::Completed);
    assert_eq!(report.skip_counts().process(), 1);
    let committed = accepted.lock().expect("accepted lock poisoned").clone();
    // Exactly one commit carried the skip, and it carried it once.
    let skipping: Vec<_> = committed
        .iter()
        .filter(|progress| *progress != &ChunkFaultProgress::NONE)
        .collect();
    assert_eq!(skipping.len(), 1);
    assert_eq!(skipping[0].skips().process(), 1);
    assert_eq!(skipping[0].skips().read(), 0);
    assert_eq!(skipping[0].skips().write(), 0);
    assert_eq!(skipping[0].no_rollbacks(), 0);
}

#[tokio::test]
async fn inherited_skip_totals_exhaust_the_shared_limit() {
    let inherited = InheritedStepProgress::new(
        7,
        [4; 32],
        FaultProgress::new(RetryCounts::ZERO, SkipCounts::new(1, 0, 0), 3, 0),
    );
    let transactions = Arc::new(Transactions::new(Trace::default()).inheriting(inherited));
    let mut job = skipping_job(Arc::clone(&transactions), 1);

    let report = launch(&mut job).await;

    // The inherited read skip already spent the shared limit of one, so the
    // next skippable failure fails the step instead of skipping again.
    assert_eq!(
        report.outcome(),
        ChunkExecutionOutcome::Failed(ChunkFailure::Processor)
    );
    assert_eq!(report.skip_counts().read(), 1);
    assert_eq!(report.skip_counts().process(), 0);
    assert_eq!(report.no_rollback_count(), 0);
}

#[tokio::test]
async fn unreadable_durable_progress_fails_before_component_work() {
    let trace = Trace::default();
    let transactions = Arc::new(Transactions::new(trace.clone()).unreadable_progress());
    let mut job = skipping_job(Arc::clone(&transactions), 4);

    let report = launch(&mut job).await;

    assert_eq!(
        report.outcome(),
        ChunkExecutionOutcome::Failed(ChunkFailure::FaultState)
    );
    assert_eq!(report.committed_chunks().get(), 0);
    assert!(
        transactions
            .accepted()
            .lock()
            .expect("lock poisoned")
            .is_empty(),
        "no chunk may commit after unusable durable state"
    );
}

#[tokio::test]
async fn fault_events_are_non_authoritative_and_bounded() {
    let trace = Trace::default();
    let (writer, _batches) = Writer::new(trace.clone());
    let writer = writer.failing(1, WriterError::with_category(FailureCategory::Timeout));
    let step = ChunkStep::new(
        step_name(),
        chunk_size(1),
        Reader::new([1], trace.clone()),
        Processor::new(trace.clone()),
        writer,
        Arc::new(Transactions::new(trace.clone())),
        Arc::new(Completion),
    )
    .with_fault_runtime(runtime(
        policy(
            [rule(
                FaultPhase::Write,
                FailureCategory::Timeout,
                FaultAction::retry(),
            )],
            1,
            0,
            BackoffPolicy::fixed(Duration::from_millis(25)).expect("static backoff is valid"),
        ),
        Arc::new(RecordingSleeper::new()),
        ChunkDeliveryMode::AtLeastOnce,
    ));
    let mut job = ChunkJob::new(
        JobName::new("fault_job").expect("static job name is valid"),
        step,
        DefinitionRevision::new("test-v1").expect("static definition revision is valid"),
        &chunk_revisions(ChunkDeliveryMode::AtLeastOnce),
    )
    .expect("static chunk definition is valid");

    let clock = ManualClock::new(UNIX_EPOCH + Duration::from_secs(100));
    let generator = DeterministicIds::new(NonZeroU64::MIN);
    let repository =
        InMemoryJobRepository::new(Arc::new(clock.clone()), Arc::new(generator.clone()));
    let events = EventRecorder::default();
    let launcher = JobLauncher::new(&repository, &clock, &generator).with_event_sink(&events);
    let (_source, stop) = StopSource::new();

    let report = launcher
        .launch_chunk(&mut job, &JobParameters::new(), &stop)
        .await
        .expect("chunk launch must complete");

    assert_eq!(
        report.chunk().expect("the chunk body ran").outcome(),
        ChunkExecutionOutcome::Completed
    );

    let recorded = events.0.lock().expect("event lock poisoned").clone();
    let fault_events: Vec<LifecycleEventKind> = recorded
        .iter()
        .map(|(kind, _)| *kind)
        .filter(|kind| {
            matches!(
                kind,
                LifecycleEventKind::RetryReserved
                    | LifecycleEventKind::RetryBackoffStarted
                    | LifecycleEventKind::RetryBackoffCancelled
                    | LifecycleEventKind::RetryExhausted
                    | LifecycleEventKind::ItemSkipped
                    | LifecycleEventKind::FaultRollbackCommitted
                    | LifecycleEventKind::FaultNoRollbackCommitted
            )
        })
        .collect();
    assert_eq!(
        fault_events,
        [
            LifecycleEventKind::RetryReserved,
            LifecycleEventKind::FaultRollbackCommitted,
            LifecycleEventKind::RetryBackoffStarted,
        ]
    );

    let reserved = recorded
        .iter()
        .find(|(kind, _)| *kind == LifecycleEventKind::RetryReserved)
        .expect("the reservation was observed");
    let keys: Vec<&str> = reserved.1.iter().map(|(key, _)| key.as_str()).collect();
    assert!(keys.contains(&"fault.phase"));
    assert!(keys.contains(&"retry.ordinal"));
    assert!(keys.contains(&"failure.category"));
    // Digests, item values, and error text are never event fields.
    assert!(
        !keys
            .iter()
            .any(|key| key.contains("item") || key.contains("digest"))
    );

    let rendered = recorded
        .iter()
        .flat_map(|(_, fields)| fields.iter().map(|(key, value)| format!("{key}={value}")))
        .collect::<Vec<_>>()
        .join(" ");
    assert_sentinel_absent([("lifecycle events", rendered.as_str())]);
    assert!(!rendered.contains("item reader failed"));
}
