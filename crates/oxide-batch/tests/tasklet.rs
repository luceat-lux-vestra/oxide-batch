//! End-to-end executable-kernel tasklet behavior.

#![allow(clippy::expect_used, clippy::panic)]

#[allow(dead_code)]
#[path = "support/clock.rs"]
mod clock;
#[allow(dead_code)]
#[path = "support/ids.rs"]
mod ids;

use std::error::Error;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, UNIX_EPOCH};

use clock::ManualClock;
use futures_util::future::join_all;
use ids::DeterministicIds;
use oxide_batch::{
    ActorRef, BatchStatus, BlockingTasklet, BlockingTaskletAdapter, BlockingTaskletContext,
    BoxFuture, ComponentRevision, DefinitionRevision, FailureCategory, InMemoryJobRepository,
    JobInstanceKey, JobLauncher, JobName, JobParameter, JobParameters, JobRepository, LaunchError,
    OwnerToken, ParameterName, ParameterRole, ParameterValue, RepositoryError, ShutdownCoordinator,
    StopPollInterval, StopSource, StopTiming, StopToken, Tasklet, TaskletContext, TaskletError,
    TaskletExecutionOutcome, TaskletFailure, TaskletJob, TaskletOutcome, TaskletStep,
};

struct Fixture {
    repository: InMemoryJobRepository,
    clock: ManualClock,
    ids: DeterministicIds,
}

impl Fixture {
    fn new() -> Result<Self, Box<dyn Error>> {
        let clock = ManualClock::new(UNIX_EPOCH + Duration::from_secs(100));
        let first_id = NonZeroU64::new(1).ok_or("the fixture ID must be nonzero")?;
        let ids = DeterministicIds::new(first_id);
        let repository = InMemoryJobRepository::new(Arc::new(clock.clone()), Arc::new(ids.clone()));
        Ok(Self {
            repository,
            clock,
            ids,
        })
    }

    fn launcher(&self) -> JobLauncher<'_> {
        JobLauncher::new(&self.repository, &self.clock, &self.ids)
    }
}

fn parameters() -> Result<JobParameters, oxide_batch::DomainError> {
    JobParameters::try_from_iter([(
        ParameterName::new("business_date")?,
        JobParameter::new(
            ParameterValue::string("2026-07-29")?,
            ParameterRole::Identifying,
        ),
    )])
}

fn job(tasklet: Arc<dyn Tasklet>) -> Result<TaskletJob, oxide_batch::DomainError> {
    Ok(TaskletJob::new(
        JobName::new("daily_import")?,
        TaskletStep::new(oxide_batch::StepName::new("import")?, tasklet),
        DefinitionRevision::new("test-v1").expect("static definition revision is valid"),
        &ComponentRevision::new("tasklet-v1").expect("static component revision is valid"),
    )
    .expect("static tasklet definition is valid"))
}

async fn assert_report_is_persisted(
    repository: &dyn JobRepository,
    report: &oxide_batch::LaunchReport,
) -> Result<(), Box<dyn Error>> {
    let mut inspection = repository.begin().await?;
    assert_eq!(
        inspection
            .get_job_execution(report.job_execution().id())
            .await?,
        Some(report.job_execution().clone())
    );
    assert_eq!(
        inspection
            .get_step_execution(report.step_execution().id())
            .await?,
        Some(report.step_execution().clone())
    );
    inspection.rollback().await?;
    Ok(())
}

struct BorrowingTasklet {
    called: Arc<AtomicUsize>,
}

impl Tasklet for BorrowingTasklet {
    fn execute<'a>(
        &'a self,
        context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            let parameter_name =
                ParameterName::new("business_date").map_err(|_| TaskletError::new())?;
            if context.parameters().get(&parameter_name).is_none()
                || context.job_execution_id().get() == 0
                || context.step_execution_id().get() == 0
            {
                return Err(TaskletError::new());
            }
            self.called.fetch_add(1, Ordering::SeqCst);
            Ok(TaskletOutcome::Completed)
        })
    }
}

#[tokio::test]
async fn successful_launch_borrows_context_and_persists_final_graph() -> Result<(), Box<dyn Error>>
{
    let fixture = Fixture::new()?;
    let called = Arc::new(AtomicUsize::new(0));
    let definition = job(Arc::new(BorrowingTasklet {
        called: Arc::clone(&called),
    }))?;
    let parameters = parameters()?;
    let (_source, stop) = StopSource::new();

    let report = fixture
        .launcher()
        .launch(&definition, &parameters, &stop)
        .await?;

    assert_eq!(called.load(Ordering::SeqCst), 1);
    assert_eq!(report.outcome(), TaskletExecutionOutcome::Completed);
    assert_eq!(
        report.job_execution().metadata().status(),
        BatchStatus::Completed
    );
    assert_eq!(
        report.step_execution().metadata().status(),
        BatchStatus::Completed
    );
    assert_eq!(
        report.job_execution().metadata().exit_status().to_string(),
        "COMPLETED"
    );
    assert_eq!(report.job_execution().version().get(), 3);
    assert_eq!(report.step_execution().version().get(), 3);
    assert_report_is_persisted(&fixture.repository, &report).await
}

struct StartedObservingTasklet {
    repository: InMemoryJobRepository,
    observed: Arc<AtomicBool>,
}

impl Tasklet for StartedObservingTasklet {
    fn execute<'a>(
        &'a self,
        context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            let mut inspection = self
                .repository
                .begin()
                .await
                .map_err(|_| TaskletError::new())?;
            let job = inspection
                .get_job_execution(context.job_execution_id())
                .await
                .map_err(|_| TaskletError::new())?
                .ok_or_else(TaskletError::new)?;
            let step = inspection
                .get_step_execution(context.step_execution_id())
                .await
                .map_err(|_| TaskletError::new())?
                .ok_or_else(TaskletError::new)?;
            inspection
                .rollback()
                .await
                .map_err(|_| TaskletError::new())?;
            if job.metadata().status() != BatchStatus::Started
                || step.metadata().status() != BatchStatus::Started
            {
                return Err(TaskletError::new());
            }
            self.observed.store(true, Ordering::Release);
            Ok(TaskletOutcome::Completed)
        })
    }
}

#[tokio::test]
async fn started_state_is_committed_before_user_work() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new()?;
    let observed = Arc::new(AtomicBool::new(false));
    let definition = job(Arc::new(StartedObservingTasklet {
        repository: fixture.repository.clone(),
        observed: Arc::clone(&observed),
    }))?;
    let parameters = parameters()?;
    let (_source, stop) = StopSource::new();

    let report = fixture
        .launcher()
        .launch(&definition, &parameters, &stop)
        .await?;

    assert!(observed.load(Ordering::Acquire));
    assert_eq!(report.outcome(), TaskletExecutionOutcome::Completed);
    Ok(())
}

struct FailingTasklet;

impl Tasklet for FailingTasklet {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async { Err(TaskletError::new()) })
    }
}

#[tokio::test]
async fn typed_tasklet_failure_is_redacted_and_persisted() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new()?;
    let definition = job(Arc::new(FailingTasklet))?;
    let parameters = parameters()?;
    let (_source, stop) = StopSource::new();

    let report = fixture
        .launcher()
        .launch(&definition, &parameters, &stop)
        .await?;

    assert_eq!(
        report.outcome(),
        TaskletExecutionOutcome::Failed(TaskletFailure::Error)
    );
    for metadata in [
        report.job_execution().metadata(),
        report.step_execution().metadata(),
    ] {
        assert_eq!(metadata.status(), BatchStatus::Failed);
        assert_eq!(metadata.exit_status().to_string(), "FAILED");
        assert_eq!(
            metadata
                .failure()
                .map(oxide_batch::FailureSummary::category),
            Some(FailureCategory::UserComponent)
        );
    }
    assert_report_is_persisted(&fixture.repository, &report).await
}

struct PanickingTasklet;

impl Tasklet for PanickingTasklet {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        panic!("sentinel panic payload must not become framework data");
    }
}

#[tokio::test]
async fn tasklet_panic_is_classified_and_runtime_remains_usable() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new()?;
    let parameters = parameters()?;
    let (_source, stop) = StopSource::new();

    let panic_report = fixture
        .launcher()
        .launch(&job(Arc::new(PanickingTasklet))?, &parameters, &stop)
        .await?;

    assert_eq!(
        panic_report.outcome(),
        TaskletExecutionOutcome::Failed(TaskletFailure::Panic)
    );
    let restart = fixture
        .launcher()
        .launch(
            &job(Arc::new(BorrowingTasklet {
                called: Arc::new(AtomicUsize::new(0)),
            }))?,
            &parameters,
            &stop,
        )
        .await?;
    assert_eq!(restart.outcome(), TaskletExecutionOutcome::Completed);
    assert_ne!(
        restart.job_execution().id(),
        panic_report.job_execution().id()
    );
    Ok(())
}

struct CountingTasklet {
    called: Arc<AtomicUsize>,
}

impl Tasklet for CountingTasklet {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            self.called.fetch_add(1, Ordering::SeqCst);
            Ok(TaskletOutcome::Completed)
        })
    }
}

#[tokio::test]
async fn completed_instance_is_rejected_before_user_work_runs() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new()?;
    let called = Arc::new(AtomicUsize::new(0));
    let definition = job(Arc::new(CountingTasklet {
        called: Arc::clone(&called),
    }))?;
    let parameters = parameters()?;
    let (_source, stop) = StopSource::new();

    let first = fixture
        .launcher()
        .launch(&definition, &parameters, &stop)
        .await?;
    let duplicate = fixture
        .launcher()
        .launch(&definition, &parameters, &stop)
        .await;

    assert_eq!(called.load(Ordering::SeqCst), 1);
    assert_eq!(
        duplicate,
        Err(LaunchError::Repository(
            RepositoryError::CompletedInstance {
                id: first.instance().id(),
            }
        ))
    );
    Ok(())
}

struct CancellableTasklet {
    entered: Arc<AtomicBool>,
}

impl Tasklet for CancellableTasklet {
    fn execute<'a>(
        &'a self,
        context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            self.entered.store(true, Ordering::Release);
            context.stop_token().cancelled().await;
            Ok(TaskletOutcome::Stopped)
        })
    }
}

#[tokio::test]
async fn cooperative_stop_during_async_work_is_persisted() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new()?;
    let entered = Arc::new(AtomicBool::new(false));
    let definition = job(Arc::new(CancellableTasklet {
        entered: Arc::clone(&entered),
    }))?;
    let parameters = parameters()?;
    let (source, stop) = StopSource::new();
    let launcher = fixture.launcher();

    let launch = launcher.launch(&definition, &parameters, &stop);
    let request = async {
        while !entered.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        source.request_stop();
    };
    let (report, ()) = tokio::join!(launch, request);
    let report = report?;

    assert_eq!(
        report.outcome(),
        TaskletExecutionOutcome::Stopped(StopTiming::DuringExecution)
    );
    assert_eq!(
        report.job_execution().metadata().status(),
        BatchStatus::Stopped
    );
    assert_eq!(
        report.step_execution().metadata().status(),
        BatchStatus::Stopped
    );
    assert_report_is_persisted(&fixture.repository, &report).await
}

#[tokio::test]
async fn application_shutdown_signal_stops_work_and_rejects_new_intake()
-> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new()?;
    let entered = Arc::new(AtomicBool::new(false));
    let definition = job(Arc::new(CancellableTasklet {
        entered: Arc::clone(&entered),
    }))?;
    let parameters = parameters()?;
    let (_source, stop) = StopSource::new();
    let coordinator = ShutdownCoordinator::default();
    let signal = coordinator.signal();
    let launcher = fixture.launcher().with_shutdown_signal(&signal);

    let launch = launcher.launch(&definition, &parameters, &stop);
    let request = async {
        while !entered.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            signal.request_shutdown(),
            oxide_batch::ShutdownRequest::Initiated
        );
    };
    let (report, ()) = tokio::join!(launch, request);
    let report = report?;
    assert_eq!(
        report.outcome(),
        TaskletExecutionOutcome::Stopped(StopTiming::DuringExecution)
    );

    let (_next_source, next_stop) = StopSource::new();
    assert_eq!(
        launcher.launch(&definition, &parameters, &next_stop).await,
        Err(LaunchError::ShuttingDown)
    );
    Ok(())
}

#[tokio::test]
async fn durable_operator_stop_is_polled_by_the_owning_launcher() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new()?;
    let entered = Arc::new(AtomicBool::new(false));
    let definition = job(Arc::new(CancellableTasklet {
        entered: Arc::clone(&entered),
    }))?;
    let parameters = parameters()?;
    let (_source, stop) = StopSource::new();
    let owner = OwnerToken::from_bytes([3; 16]);
    let launcher = fixture
        .launcher()
        .with_execution_control(owner, StopPollInterval::new(Duration::from_millis(100))?);

    let launch = launcher.launch(&definition, &parameters, &stop);
    let request = async {
        while !entered.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        let key = JobInstanceKey::new(JobName::new("daily_import")?, &parameters);
        let mut unit = fixture.repository.begin().await?;
        let instance = unit
            .find_job_instance(&key)
            .await?
            .ok_or(RepositoryError::Unavailable)?;
        let execution = unit
            .job_executions(instance.id())
            .await?
            .pop()
            .ok_or(RepositoryError::Unavailable)?;
        unit.request_execution_stop(
            execution.id(),
            execution.version(),
            &ActorRef::new("operator:test")?,
            fixture.clock.now(),
        )
        .await?;
        unit.commit().await?;
        Ok::<(), Box<dyn Error>>(())
    };
    let (report, requested) = tokio::join!(launch, request);
    requested?;
    let report = report?;

    assert_eq!(
        report.outcome(),
        TaskletExecutionOutcome::Stopped(StopTiming::DuringExecution)
    );
    assert_eq!(
        report.job_execution().metadata().status(),
        BatchStatus::Stopped
    );
    Ok(())
}

#[tokio::test]
async fn stop_before_start_prevents_user_work() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new()?;
    let called = Arc::new(AtomicUsize::new(0));
    let definition = job(Arc::new(CountingTasklet {
        called: Arc::clone(&called),
    }))?;
    let parameters = parameters()?;
    let (source, stop) = StopSource::new();
    source.request_stop();

    let report = fixture
        .launcher()
        .launch(&definition, &parameters, &stop)
        .await?;

    assert_eq!(called.load(Ordering::SeqCst), 0);
    assert_eq!(
        report.outcome(),
        TaskletExecutionOutcome::Stopped(StopTiming::BeforeStart)
    );
    assert_eq!(
        report.job_execution().metadata().status(),
        BatchStatus::Stopped
    );
    Ok(())
}

struct SleepingBlockingTasklet {
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    completed: Arc<AtomicUsize>,
    delay: Duration,
}

impl BlockingTasklet for SleepingBlockingTasklet {
    fn execute(&self, _context: BlockingTaskletContext) -> Result<TaskletOutcome, TaskletError> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        std::thread::sleep(self.delay);
        self.active.fetch_sub(1, Ordering::SeqCst);
        self.completed.fetch_add(1, Ordering::SeqCst);
        Ok(TaskletOutcome::Completed)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocking_adapter_awaits_running_work_and_reports_late_stop() -> Result<(), Box<dyn Error>>
{
    let fixture = Fixture::new()?;
    let active = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let adapter = BlockingTaskletAdapter::new(
        SleepingBlockingTasklet {
            active: Arc::clone(&active),
            peak: Arc::new(AtomicUsize::new(0)),
            completed: Arc::clone(&completed),
            delay: Duration::from_millis(40),
        },
        NonZeroUsize::new(1).ok_or("the blocking bound must be nonzero")?,
    );
    let definition = job(Arc::new(adapter))?;
    let parameters = parameters()?;
    let (source, stop) = StopSource::new();
    let launcher = fixture.launcher();

    let launch = launcher.launch(&definition, &parameters, &stop);
    let request = async {
        while active.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
        source.request_stop();
    };
    let (report, ()) = tokio::join!(launch, request);
    let report = report?;

    assert_eq!(completed.load(Ordering::SeqCst), 1);
    assert_eq!(
        report.outcome(),
        TaskletExecutionOutcome::Stopped(StopTiming::AfterBlockingWork)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blocking_adapter_enforces_its_concurrency_bound() -> Result<(), Box<dyn Error>> {
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let adapter: Arc<dyn Tasklet> = Arc::new(BlockingTaskletAdapter::new(
        SleepingBlockingTasklet {
            active,
            peak: Arc::clone(&peak),
            completed: Arc::clone(&completed),
            delay: Duration::from_millis(30),
        },
        NonZeroUsize::new(2).ok_or("the blocking bound must be nonzero")?,
    ));
    let fixtures = (0..5)
        .map(|_| Fixture::new())
        .collect::<Result<Vec<_>, _>>()?;
    let jobs = (0..5)
        .map(|_| job(Arc::clone(&adapter)))
        .collect::<Result<Vec<_>, _>>()?;
    let parameter_sets = (0..5)
        .map(|_| parameters())
        .collect::<Result<Vec<_>, _>>()?;
    let tokens = (0..5)
        .map(|_| StopSource::new().1)
        .collect::<Vec<StopToken>>();
    let launchers = fixtures.iter().map(Fixture::launcher).collect::<Vec<_>>();

    let reports = join_all(
        launchers
            .iter()
            .zip(&jobs)
            .zip(&parameter_sets)
            .zip(&tokens)
            .map(|(((launcher, definition), parameters), stop)| {
                launcher.launch(definition, parameters, stop)
            }),
    )
    .await;

    assert!(reports.iter().all(Result::is_ok));
    assert_eq!(completed.load(Ordering::SeqCst), 5);
    assert_eq!(peak.load(Ordering::SeqCst), 2);
    Ok(())
}

struct PanickingBlockingTasklet;

impl BlockingTasklet for PanickingBlockingTasklet {
    fn execute(&self, _context: BlockingTaskletContext) -> Result<TaskletOutcome, TaskletError> {
        panic!("blocking panic payload must remain redacted");
    }
}

#[tokio::test]
async fn blocking_panic_is_classified_at_the_tasklet_boundary() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new()?;
    let adapter = BlockingTaskletAdapter::new(
        PanickingBlockingTasklet,
        NonZeroUsize::new(1).ok_or("the blocking bound must be nonzero")?,
    );
    let definition = job(Arc::new(adapter))?;
    let parameters = parameters()?;
    let (_source, stop) = StopSource::new();

    let report = fixture
        .launcher()
        .launch(&definition, &parameters, &stop)
        .await?;

    assert_eq!(
        report.outcome(),
        TaskletExecutionOutcome::Failed(TaskletFailure::Panic)
    );
    Ok(())
}
