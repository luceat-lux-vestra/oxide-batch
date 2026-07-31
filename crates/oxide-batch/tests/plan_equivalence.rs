//! One-step wrapper and compiled-plan execution equivalence.
//!
//! Each scenario records a normalized trace of every repository command, every
//! lifecycle event, and the final durable rows of one launch, then compares it
//! with a golden file captured from the pre-lowering wrapper implementation.
//! Routing execution through the compiled plan must not change any line.

#![allow(clippy::expect_used, clippy::panic, clippy::similar_names)]

#[allow(dead_code)]
#[path = "support/clock.rs"]
mod clock;
#[allow(dead_code)]
#[path = "support/ids.rs"]
mod ids;
#[allow(dead_code)]
#[path = "support/trace.rs"]
mod trace;

use std::collections::VecDeque;
use std::error::Error;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, UNIX_EPOCH};

use clock::ManualClock;
use ids::DeterministicIds;
use oxide_batch::{
    BoxFuture, Checkpoint, ChunkCommitReceipt, ChunkCompletion, ChunkCompletionContext,
    ChunkCompletionError, ChunkCompletionOutcome, ChunkComponentRevisions, ChunkCounts,
    ChunkDeliveryMode, ChunkFaultProgress, ChunkJob, ChunkRestartContract, ChunkSize, ChunkStep,
    ChunkTransaction, ChunkTransactionError, ChunkTransactionManager, ComponentRevision,
    DefinitionRevision, ExecutionContext, InMemoryJobRepository, ItemProcessor, ItemReader,
    ItemWriter, JobLauncher, JobName, JobParameter, JobParameters, JobRepository, ListenerContext,
    ListenerError, ParameterName, ParameterRole, ParameterValue, ProcessContext, ProcessOutcome,
    ProcessorError, ReadContext, ReadOutcome, ReaderError, StateLimits, StateSchemaId,
    StateSchemaVersion, StepExecutionListener, StepName, StopSource, StopToken, Tasklet,
    TaskletContext, TaskletError, TaskletExecutionOutcome, TaskletJob, TaskletOutcome, TaskletStep,
    WriteContext, WriteOutcome, WriterError,
};
use trace::{ExecutionTrace, RecordingRepository, TracingEventSink};

struct Harness {
    trace: ExecutionTrace,
    clock: ManualClock,
    ids: DeterministicIds,
    repository: RecordingRepository<InMemoryJobRepository>,
    sink: TracingEventSink,
}

impl Harness {
    fn new() -> Self {
        let trace = ExecutionTrace::new();
        let clock = ManualClock::new(UNIX_EPOCH + Duration::from_secs(100));
        let ids = DeterministicIds::new(NonZeroU64::MIN);
        let repository = RecordingRepository::new(
            InMemoryJobRepository::new(Arc::new(clock.clone()), Arc::new(ids.clone())),
            trace.clone(),
        );
        let sink = TracingEventSink::new(trace.clone());
        Self {
            trace,
            clock,
            ids,
            repository,
            sink,
        }
    }

    fn launcher(&self) -> JobLauncher<'_> {
        JobLauncher::new(&self.repository, &self.clock, &self.ids).with_event_sink(&self.sink)
    }

    async fn finish(&self, name: &str) -> Result<(), Box<dyn Error>> {
        let instance_id = {
            let mut unit = self.repository.inner().begin().await?;
            let instance = unit.find_job_instance(&instance_key()?).await?;
            unit.rollback().await?;
            instance
                .ok_or("the scenario must create a job instance")?
                .id()
        };
        self.trace
            .record_durable_state(self.repository.inner(), instance_id)
            .await?;
        self.trace.assert_matches_golden(name)?;
        Ok(())
    }
}

fn parameters() -> Result<JobParameters, Box<dyn Error>> {
    Ok(JobParameters::try_from_iter([(
        ParameterName::new("business_date")?,
        JobParameter::new(
            ParameterValue::string("2026-07-31")?,
            ParameterRole::Identifying,
        ),
    )])?)
}

fn instance_key() -> Result<oxide_batch::JobInstanceKey, Box<dyn Error>> {
    Ok(oxide_batch::JobInstanceKey::new(
        JobName::new("daily_import")?,
        &parameters()?,
    ))
}

fn tasklet_job(tasklet: Arc<dyn Tasklet>) -> Result<TaskletJob, Box<dyn Error>> {
    Ok(TaskletJob::new(
        JobName::new("daily_import")?,
        TaskletStep::new(StepName::new("import")?, tasklet),
        DefinitionRevision::new("equivalence-v1")?,
        &ComponentRevision::new("tasklet-v1")?,
    )?)
}

struct ScriptedTasklet {
    outcomes: Vec<ScriptedOutcome>,
    calls: AtomicUsize,
}

#[derive(Clone, Copy)]
enum ScriptedOutcome {
    Completed,
    Error,
    Panic,
    Unknown,
    ObserveStop,
}

impl ScriptedTasklet {
    fn new(outcomes: impl IntoIterator<Item = ScriptedOutcome>) -> Arc<Self> {
        Arc::new(Self {
            outcomes: outcomes.into_iter().collect(),
            calls: AtomicUsize::new(0),
        })
    }
}

impl Tasklet for ScriptedTasklet {
    fn execute<'a>(
        &'a self,
        context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        let index = self.calls.fetch_add(1, Ordering::SeqCst);
        let outcome = self
            .outcomes
            .get(index)
            .copied()
            .unwrap_or(ScriptedOutcome::Completed);
        if matches!(outcome, ScriptedOutcome::Panic) {
            panic!("tasklet secret");
        }
        Box::pin(async move {
            match outcome {
                ScriptedOutcome::Completed | ScriptedOutcome::Panic => {
                    Ok(TaskletOutcome::Completed)
                }
                ScriptedOutcome::Error => Err(TaskletError::new()),
                ScriptedOutcome::Unknown => Ok(TaskletOutcome::CommitOutcomeUnknown),
                ScriptedOutcome::ObserveStop => {
                    let _ = context.stop_token();
                    Ok(TaskletOutcome::Stopped)
                }
            }
        })
    }
}

struct FailingStepListener;

impl StepExecutionListener for FailingStepListener {
    fn before_step<'a>(
        &'a self,
        _context: ListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        Box::pin(async { Err(ListenerError::new()) })
    }

    fn after_step<'a>(
        &'a self,
        _context: ListenerContext<'a>,
        _outcome: TaskletExecutionOutcome,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn one_step_tasklet_completion_matches_the_wrapper_trace() -> Result<(), Box<dyn Error>> {
    let harness = Harness::new();
    let job = tasklet_job(ScriptedTasklet::new([ScriptedOutcome::Completed]))?;
    let (_source, stop) = StopSource::new();
    harness
        .launcher()
        .launch(&job, &parameters()?, &stop)
        .await?;
    harness.finish("wrapper-tasklet-completed").await
}

#[tokio::test]
async fn one_step_tasklet_failure_matches_the_wrapper_trace() -> Result<(), Box<dyn Error>> {
    let harness = Harness::new();
    let job = tasklet_job(ScriptedTasklet::new([ScriptedOutcome::Error]))?;
    let (_source, stop) = StopSource::new();
    harness
        .launcher()
        .launch(&job, &parameters()?, &stop)
        .await?;
    harness.finish("wrapper-tasklet-failed").await
}

#[tokio::test]
async fn one_step_tasklet_panic_matches_the_wrapper_trace() -> Result<(), Box<dyn Error>> {
    let harness = Harness::new();
    let job = tasklet_job(ScriptedTasklet::new([ScriptedOutcome::Panic]))?;
    let (_source, stop) = StopSource::new();
    harness
        .launcher()
        .launch(&job, &parameters()?, &stop)
        .await?;
    harness.finish("wrapper-tasklet-panicked").await
}

#[tokio::test]
async fn one_step_tasklet_unknown_commit_matches_the_wrapper_trace() -> Result<(), Box<dyn Error>> {
    let harness = Harness::new();
    let job = tasklet_job(ScriptedTasklet::new([ScriptedOutcome::Unknown]))?;
    let (_source, stop) = StopSource::new();
    harness
        .launcher()
        .launch(&job, &parameters()?, &stop)
        .await?;
    harness.finish("wrapper-tasklet-unknown").await
}

#[tokio::test]
async fn one_step_tasklet_stop_before_start_matches_the_wrapper_trace() -> Result<(), Box<dyn Error>>
{
    let harness = Harness::new();
    let job = tasklet_job(ScriptedTasklet::new([ScriptedOutcome::Completed]))?;
    let (source, stop) = StopSource::new();
    source.request_stop();
    harness
        .launcher()
        .launch(&job, &parameters()?, &stop)
        .await?;
    harness.finish("wrapper-tasklet-stopped-before-start").await
}

#[tokio::test]
async fn one_step_tasklet_stop_during_execution_matches_the_wrapper_trace()
-> Result<(), Box<dyn Error>> {
    let harness = Harness::new();
    let job = tasklet_job(ScriptedTasklet::new([ScriptedOutcome::ObserveStop]))?;
    let (_source, stop) = StopSource::new();
    harness
        .launcher()
        .launch(&job, &parameters()?, &stop)
        .await?;
    harness
        .finish("wrapper-tasklet-stopped-during-execution")
        .await
}

#[tokio::test]
async fn one_step_before_step_listener_failure_matches_the_wrapper_trace()
-> Result<(), Box<dyn Error>> {
    let harness = Harness::new();
    let step = TaskletStep::new(
        StepName::new("import")?,
        ScriptedTasklet::new([ScriptedOutcome::Completed]),
    )
    .with_listener(Arc::new(FailingStepListener));
    let job = TaskletJob::new(
        JobName::new("daily_import")?,
        step,
        DefinitionRevision::new("equivalence-v1")?,
        &ComponentRevision::new("tasklet-v1")?,
    )?;
    let (_source, stop) = StopSource::new();
    harness
        .launcher()
        .launch(&job, &parameters()?, &stop)
        .await?;
    harness.finish("wrapper-tasklet-listener-failure").await
}

#[tokio::test]
async fn one_step_restart_after_failure_matches_the_wrapper_trace() -> Result<(), Box<dyn Error>> {
    let harness = Harness::new();
    let job = tasklet_job(ScriptedTasklet::new([
        ScriptedOutcome::Error,
        ScriptedOutcome::Completed,
    ]))?;
    let (_source, stop) = StopSource::new();
    harness
        .launcher()
        .launch(&job, &parameters()?, &stop)
        .await?;
    harness
        .launcher()
        .launch(&job, &parameters()?, &stop)
        .await?;
    harness.finish("wrapper-tasklet-restart").await
}

struct ScriptedReader {
    items: VecDeque<i32>,
    fail: bool,
}

impl ItemReader<i32> for ScriptedReader {
    fn read<'a>(
        &'a mut self,
        _context: ReadContext<'a>,
    ) -> BoxFuture<'a, Result<ReadOutcome<i32>, ReaderError>> {
        if self.fail {
            return Box::pin(async { Err(ReaderError::new()) });
        }
        let item = self.items.pop_front();
        Box::pin(async move { Ok(item.map_or(ReadOutcome::EndOfInput, ReadOutcome::Item)) })
    }
}

struct DoublingProcessor;

impl ItemProcessor<i32, i32> for DoublingProcessor {
    fn process<'a>(
        &'a self,
        item: &'a i32,
        _context: ProcessContext<'a>,
    ) -> BoxFuture<'a, Result<ProcessOutcome<i32>, ProcessorError>> {
        let output = item * 2;
        Box::pin(async move { Ok(ProcessOutcome::Item(output)) })
    }
}

struct DiscardingWriter;

impl ItemWriter<i32> for DiscardingWriter {
    fn write<'a>(
        &'a self,
        _items: &'a [i32],
        _context: WriteContext<'a>,
    ) -> BoxFuture<'a, Result<WriteOutcome, WriterError>> {
        Box::pin(async { Ok(WriteOutcome::Written) })
    }
}

struct AcknowledgingCompletion;

impl ChunkCompletion for AcknowledgingCompletion {
    fn after_commit<'a>(
        &'a self,
        _context: ChunkCompletionContext<'a>,
    ) -> BoxFuture<'a, Result<ChunkCompletionOutcome, ChunkCompletionError>> {
        Box::pin(async { Ok(ChunkCompletionOutcome::Acknowledged) })
    }
}

struct ScriptedTransactions {
    commit_error: Option<ChunkTransactionError>,
}

impl ChunkTransactionManager for ScriptedTransactions {
    fn begin(
        &self,
    ) -> BoxFuture<'_, Result<Box<dyn ChunkTransaction + '_>, ChunkTransactionError>> {
        let transaction = ScriptedTransaction {
            commit_error: self.commit_error,
        };
        Box::pin(async move { Ok(Box::new(transaction) as Box<dyn ChunkTransaction>) })
    }
}

struct ScriptedTransaction {
    commit_error: Option<ChunkTransactionError>,
}

impl ChunkTransaction for ScriptedTransaction {
    fn business_transaction(&mut self) -> Option<&mut dyn oxide_batch::BusinessTransaction> {
        None
    }

    fn commit(
        &mut self,
        _counts: ChunkCounts,
        _fault: ChunkFaultProgress,
    ) -> BoxFuture<'_, Result<ChunkCommitReceipt, ChunkTransactionError>> {
        if let Some(error) = self.commit_error {
            return Box::pin(async move { Err(error) });
        }
        let receipt = chunk_receipt();
        Box::pin(async move { receipt })
    }

    fn rollback(&mut self) -> BoxFuture<'_, Result<(), ChunkTransactionError>> {
        Box::pin(async { Ok(()) })
    }
}

fn chunk_receipt() -> Result<ChunkCommitReceipt, ChunkTransactionError> {
    let checkpoint = Checkpoint::from_json(
        br#"{"format":"oxide-batch.checkpoint","format_version":1,"schema":"test.position","schema_version":1,"payload":{"position":0}}"#,
        StateLimits::default(),
    )
    .map_err(|_| ChunkTransactionError::NotCommitted)?;
    let context = ExecutionContext::from_json(
        br#"{"format":"oxide-batch.execution-context","format_version":1,"schema":"test.context","schema_version":1,"payload":{}}"#,
        StateLimits::default(),
    )
    .map_err(|_| ChunkTransactionError::NotCommitted)?;
    Ok(ChunkCommitReceipt::new(checkpoint, context))
}

fn chunk_revisions() -> Result<ChunkComponentRevisions, Box<dyn Error>> {
    Ok(ChunkComponentRevisions::new(
        ComponentRevision::new("reader-v1")?,
        ComponentRevision::new("processor-v1")?,
        ComponentRevision::new("writer-v1")?,
        ComponentRevision::new("checkpoint-v1")?,
        ChunkRestartContract::new(
            StateSchemaId::new("test.position")?,
            StateSchemaVersion::new(1)?,
            StateSchemaId::new("test.context")?,
            StateSchemaVersion::new(1)?,
            ChunkDeliveryMode::AtLeastOnce,
        ),
    ))
}

fn chunk_job(
    fail_read: bool,
    commit_error: Option<ChunkTransactionError>,
) -> Result<ChunkJob<i32, i32>, Box<dyn Error>> {
    let step = ChunkStep::new(
        StepName::new("import")?,
        ChunkSize::new(2)?,
        Box::new(ScriptedReader {
            items: [1, 2, 3].into_iter().collect(),
            fail: fail_read,
        }),
        Arc::new(DoublingProcessor),
        Arc::new(DiscardingWriter),
        Arc::new(ScriptedTransactions { commit_error }),
        Arc::new(AcknowledgingCompletion),
    );
    Ok(ChunkJob::new(
        JobName::new("daily_import")?,
        step,
        DefinitionRevision::new("equivalence-v1")?,
        &chunk_revisions()?,
    )?)
}

async fn launch_chunk_scenario(
    harness: &Harness,
    job: &mut ChunkJob<i32, i32>,
    stop: &StopToken,
) -> Result<(), Box<dyn Error>> {
    harness
        .launcher()
        .launch_chunk(job, &parameters()?, stop)
        .await?;
    Ok(())
}

#[tokio::test]
async fn one_step_chunk_completion_matches_the_wrapper_trace() -> Result<(), Box<dyn Error>> {
    let harness = Harness::new();
    let mut job = chunk_job(false, None)?;
    let (_source, stop) = StopSource::new();
    launch_chunk_scenario(&harness, &mut job, &stop).await?;
    harness.finish("wrapper-chunk-completed").await
}

#[tokio::test]
async fn one_step_chunk_failure_matches_the_wrapper_trace() -> Result<(), Box<dyn Error>> {
    let harness = Harness::new();
    let mut job = chunk_job(true, None)?;
    let (_source, stop) = StopSource::new();
    launch_chunk_scenario(&harness, &mut job, &stop).await?;
    harness.finish("wrapper-chunk-failed").await
}

#[tokio::test]
async fn one_step_chunk_unknown_commit_matches_the_wrapper_trace() -> Result<(), Box<dyn Error>> {
    let harness = Harness::new();
    let mut job = chunk_job(false, Some(ChunkTransactionError::CommitOutcomeUnknown))?;
    let (_source, stop) = StopSource::new();
    launch_chunk_scenario(&harness, &mut job, &stop).await?;
    harness.finish("wrapper-chunk-unknown").await
}
