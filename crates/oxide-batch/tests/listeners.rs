//! Listener ordering, lifecycle events, and diagnostic redaction.

#![allow(clippy::expect_used, clippy::panic)]

#[allow(dead_code)]
#[path = "support/clock.rs"]
mod clock;
#[allow(dead_code)]
#[path = "support/ids.rs"]
mod ids;
#[allow(dead_code)]
#[path = "support/secrets.rs"]
mod secrets;

use std::error::Error;
use std::fmt;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, UNIX_EPOCH};

use clock::ManualClock;
use ids::DeterministicIds;
use oxide_batch::{
    BatchStatus, BoxFuture, ComponentRevision, DefinitionRevision, InMemoryJobRepository,
    JobExecutionListener, JobLauncher, JobName, JobParameter, JobParameters, LifecycleEvent,
    LifecycleEventKind, LifecycleEventSink, ListenerContext, ListenerError, ListenerFailureKind,
    ListenerPhase, ParameterName, ParameterRole, ParameterValue, StopSource, Tasklet,
    TaskletContext, TaskletError, TaskletExecutionOutcome, TaskletFailure, TaskletJob,
    TaskletOutcome, TaskletStep,
};
use secrets::{SENTINEL_SECRET, assert_sentinel_absent};

struct Fixture {
    repository: InMemoryJobRepository,
    clock: ManualClock,
    ids: DeterministicIds,
}

impl Fixture {
    fn new() -> Result<Self, Box<dyn Error>> {
        let clock = ManualClock::new(UNIX_EPOCH + Duration::from_secs(100));
        let first_id = NonZeroU64::new(1).ok_or("fixture IDs must be nonzero")?;
        let ids = DeterministicIds::new(first_id);
        let repository = InMemoryJobRepository::new(Arc::new(clock.clone()), Arc::new(ids.clone()));
        Ok(Self {
            repository,
            clock,
            ids,
        })
    }

    fn launcher<'a>(&'a self, sink: &'a dyn LifecycleEventSink) -> JobLauncher<'a> {
        JobLauncher::new(&self.repository, &self.clock, &self.ids).with_event_sink(sink)
    }
}

fn parameters(value: &str) -> Result<JobParameters, oxide_batch::DomainError> {
    JobParameters::try_from_iter([(
        ParameterName::new("business_date")?,
        JobParameter::new(ParameterValue::string(value)?, ParameterRole::Identifying),
    )])
}

fn job(
    tasklet: Arc<dyn Tasklet>,
    step_listeners: impl IntoIterator<Item = Arc<dyn oxide_batch::StepExecutionListener>>,
    job_listeners: impl IntoIterator<Item = Arc<dyn JobExecutionListener>>,
) -> Result<TaskletJob, oxide_batch::DomainError> {
    let mut step = TaskletStep::new(oxide_batch::StepName::new("import")?, tasklet);
    for listener in step_listeners {
        step = step.with_listener(listener);
    }
    let mut job = TaskletJob::new(
        JobName::new("daily_import")?,
        step,
        DefinitionRevision::new("test-v1").expect("static definition revision is valid"),
        &ComponentRevision::new("tasklet-v1").expect("static component revision is valid"),
    )
    .expect("static tasklet definition is valid");
    for listener in job_listeners {
        job = job.with_listener(listener);
    }
    Ok(job)
}

#[derive(Clone, Default)]
struct CapturingSink {
    events: Arc<Mutex<Vec<LifecycleEvent>>>,
}

impl CapturingSink {
    fn snapshot(&self) -> Vec<LifecycleEvent> {
        self.events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl LifecycleEventSink for CapturingSink {
    fn emit(&self, event: &LifecycleEvent) {
        self.events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(event.clone());
    }
}

#[derive(Clone, Copy)]
enum Behavior {
    Pass,
    Error,
    Panic,
}

fn listener_result(behavior: Behavior) -> Result<(), ListenerError> {
    match behavior {
        Behavior::Pass => Ok(()),
        Behavior::Error => Err(ListenerError::new()),
        Behavior::Panic => panic!("listener panic payload must remain redacted"),
    }
}

struct RecordingJobListener {
    name: &'static str,
    calls: Arc<Mutex<Vec<String>>>,
    before: Behavior,
    after: Behavior,
}

impl JobExecutionListener for RecordingJobListener {
    fn before_job<'a>(
        &'a self,
        context: ListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        assert!(context.correlation().job_attempt().get() > 0);
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(format!("job.before.{}", self.name));
        let result = listener_result(self.before);
        Box::pin(async move { result })
    }

    fn after_job<'a>(
        &'a self,
        _context: ListenerContext<'a>,
        outcome: TaskletExecutionOutcome,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(format!("job.after.{}.{}", self.name, outcome_name(outcome)));
        let result = listener_result(self.after);
        Box::pin(async move { result })
    }
}

struct RecordingStepListener {
    name: &'static str,
    calls: Arc<Mutex<Vec<String>>>,
    before: Behavior,
    after: Behavior,
}

impl oxide_batch::StepExecutionListener for RecordingStepListener {
    fn before_step<'a>(
        &'a self,
        context: ListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        assert!(context.correlation().step_attempt().get() > 0);
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(format!("step.before.{}", self.name));
        let result = listener_result(self.before);
        Box::pin(async move { result })
    }

    fn after_step<'a>(
        &'a self,
        _context: ListenerContext<'a>,
        outcome: TaskletExecutionOutcome,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(format!(
                "step.after.{}.{}",
                self.name,
                outcome_name(outcome)
            ));
        let result = listener_result(self.after);
        Box::pin(async move { result })
    }
}

const fn outcome_name(outcome: TaskletExecutionOutcome) -> &'static str {
    match outcome {
        TaskletExecutionOutcome::Completed => "completed",
        TaskletExecutionOutcome::Failed(_) => "failed",
        TaskletExecutionOutcome::Stopped(_) => "stopped",
        _ => "other",
    }
}

struct RecordingTasklet {
    calls: Arc<Mutex<Vec<String>>>,
    count: Arc<AtomicUsize>,
    fail_with_secret: bool,
}

impl Tasklet for RecordingTasklet {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            self.calls
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(String::from("tasklet"));
            self.count.fetch_add(1, Ordering::SeqCst);
            if self.fail_with_secret {
                Err(TaskletError::from_error(SecretError(String::from(
                    SENTINEL_SECRET,
                ))))
            } else {
                Ok(TaskletOutcome::Completed)
            }
        })
    }
}

#[derive(Debug)]
struct SecretError(String);

impl fmt::Display for SecretError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for SecretError {}

fn job_listener(
    name: &'static str,
    calls: &Arc<Mutex<Vec<String>>>,
    before: Behavior,
    after: Behavior,
) -> Arc<dyn JobExecutionListener> {
    Arc::new(RecordingJobListener {
        name,
        calls: Arc::clone(calls),
        before,
        after,
    })
}

fn step_listener(
    name: &'static str,
    calls: &Arc<Mutex<Vec<String>>>,
    before: Behavior,
    after: Behavior,
) -> Arc<dyn oxide_batch::StepExecutionListener> {
    Arc::new(RecordingStepListener {
        name,
        calls: Arc::clone(calls),
        before,
        after,
    })
}

fn tasklet(
    calls: &Arc<Mutex<Vec<String>>>,
    count: &Arc<AtomicUsize>,
    fail_with_secret: bool,
) -> Arc<dyn Tasklet> {
    Arc::new(RecordingTasklet {
        calls: Arc::clone(calls),
        count: Arc::clone(count),
        fail_with_secret,
    })
}

#[tokio::test]
async fn listeners_nest_and_reverse_after_order() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new()?;
    let sink = CapturingSink::default();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let count = Arc::new(AtomicUsize::new(0));
    let definition = job(
        tasklet(&calls, &count, false),
        [
            step_listener("a", &calls, Behavior::Pass, Behavior::Pass),
            step_listener("b", &calls, Behavior::Pass, Behavior::Pass),
        ],
        [
            job_listener("a", &calls, Behavior::Pass, Behavior::Pass),
            job_listener("b", &calls, Behavior::Pass, Behavior::Pass),
        ],
    )?;
    let (_source, stop) = StopSource::new();

    let report = fixture
        .launcher(&sink)
        .launch(&definition, &parameters("2026-07-29")?, &stop)
        .await?;

    assert_eq!(report.outcome(), TaskletExecutionOutcome::Completed);
    assert_eq!(
        *calls.lock().unwrap_or_else(PoisonError::into_inner),
        [
            "job.before.a",
            "job.before.b",
            "step.before.a",
            "step.before.b",
            "tasklet",
            "step.after.b.completed",
            "step.after.a.completed",
            "job.after.b.completed",
            "job.after.a.completed",
        ]
    );
    Ok(())
}

#[tokio::test]
async fn before_listener_failure_prevents_associated_user_body() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new()?;
    let sink = CapturingSink::default();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let count = Arc::new(AtomicUsize::new(0));
    let definition = job(
        tasklet(&calls, &count, false),
        [
            step_listener("a", &calls, Behavior::Pass, Behavior::Pass),
            step_listener("b", &calls, Behavior::Error, Behavior::Pass),
            step_listener("c", &calls, Behavior::Pass, Behavior::Pass),
        ],
        [],
    )?;
    let (_source, stop) = StopSource::new();

    let report = fixture
        .launcher(&sink)
        .launch(&definition, &parameters("2026-07-29")?, &stop)
        .await?;

    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert_eq!(
        *calls.lock().unwrap_or_else(PoisonError::into_inner),
        ["step.before.a", "step.before.b"]
    );
    assert_eq!(
        report.outcome(),
        TaskletExecutionOutcome::Failed(TaskletFailure::ListenerError)
    );
    assert_eq!(
        report.listener_failures()[0].phase(),
        ListenerPhase::BeforeStep
    );
    assert_eq!(
        report.step_execution().metadata().status(),
        BatchStatus::Failed
    );
    assert_eq!(
        report.job_execution().metadata().status(),
        BatchStatus::Failed
    );
    Ok(())
}

#[tokio::test]
async fn after_listener_failure_retains_original_outcome_and_work() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new()?;
    let sink = CapturingSink::default();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let count = Arc::new(AtomicUsize::new(0));
    let definition = job(
        tasklet(&calls, &count, false),
        [
            step_listener("a", &calls, Behavior::Pass, Behavior::Error),
            step_listener("b", &calls, Behavior::Pass, Behavior::Pass),
        ],
        [],
    )?;
    let (_source, stop) = StopSource::new();

    let report = fixture
        .launcher(&sink)
        .launch(&definition, &parameters("2026-07-29")?, &stop)
        .await?;

    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(
        *calls.lock().unwrap_or_else(PoisonError::into_inner),
        [
            "step.before.a",
            "step.before.b",
            "tasklet",
            "step.after.b.completed",
            "step.after.a.completed",
        ]
    );
    assert_eq!(
        report.outcome(),
        TaskletExecutionOutcome::Failed(TaskletFailure::ListenerError)
    );
    assert_eq!(
        report.original_outcome(),
        Some(TaskletExecutionOutcome::Completed)
    );
    assert_eq!(
        report.listener_failures()[0].phase(),
        ListenerPhase::AfterStep
    );
    Ok(())
}

#[tokio::test]
async fn listener_panic_uses_the_same_redacted_boundary() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new()?;
    let sink = CapturingSink::default();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let count = Arc::new(AtomicUsize::new(0));
    let definition = job(
        tasklet(&calls, &count, false),
        [],
        [
            job_listener("panic", &calls, Behavior::Panic, Behavior::Pass),
            job_listener("unreached", &calls, Behavior::Pass, Behavior::Pass),
        ],
    )?;
    let (_source, stop) = StopSource::new();

    let report = fixture
        .launcher(&sink)
        .launch(&definition, &parameters("2026-07-29")?, &stop)
        .await?;

    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert_eq!(
        report.outcome(),
        TaskletExecutionOutcome::Failed(TaskletFailure::ListenerPanic)
    );
    assert_eq!(
        report.listener_failures()[0].kind(),
        ListenerFailureKind::Panic
    );
    assert_eq!(
        *calls.lock().unwrap_or_else(PoisonError::into_inner),
        ["job.before.panic"]
    );
    Ok(())
}

// OBS-001
#[tokio::test]
async fn telemetry_correlates_execution() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new()?;
    let sink = CapturingSink::default();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let count = Arc::new(AtomicUsize::new(0));
    let failing = job(tasklet(&calls, &count, true), [], [])?;
    let succeeding = job(tasklet(&calls, &count, false), [], [])?;
    let launch_parameters = parameters("2026-07-29")?;
    let (_source, stop) = StopSource::new();

    let first = fixture
        .launcher(&sink)
        .launch(&failing, &launch_parameters, &stop)
        .await?;
    let second = fixture
        .launcher(&sink)
        .launch(&succeeding, &launch_parameters, &stop)
        .await?;

    assert_eq!(
        first.outcome(),
        TaskletExecutionOutcome::Failed(TaskletFailure::Error)
    );
    assert_eq!(second.outcome(), TaskletExecutionOutcome::Completed);
    let events = sink.snapshot();
    let expected = [
        LifecycleEventKind::LaunchAccepted,
        LifecycleEventKind::JobStarting,
        LifecycleEventKind::StepStarting,
        LifecycleEventKind::JobStarted,
        LifecycleEventKind::StepStarted,
        LifecycleEventKind::StepFailed,
        LifecycleEventKind::JobFailed,
        LifecycleEventKind::LaunchAccepted,
        LifecycleEventKind::JobStarting,
        LifecycleEventKind::StepStarting,
        LifecycleEventKind::JobStarted,
        LifecycleEventKind::StepStarted,
        LifecycleEventKind::StepCompleted,
        LifecycleEventKind::JobCompleted,
    ];
    assert_eq!(
        events.iter().map(LifecycleEvent::kind).collect::<Vec<_>>(),
        expected
    );
    for event in &events[..7] {
        assert_eq!(event.correlation().job_attempt().get(), 1);
        assert_eq!(event.correlation().step_attempt().get(), 1);
        assert_eq!(
            event.correlation().job_execution_id(),
            first.job_execution().id()
        );
        assert_eq!(
            event.correlation().step_execution_id(),
            first.step_execution().id()
        );
    }
    for event in &events[7..] {
        assert_eq!(event.correlation().job_attempt().get(), 2);
        assert_eq!(event.correlation().step_attempt().get(), 2);
        assert_eq!(
            event.correlation().job_execution_id(),
            second.job_execution().id()
        );
        assert_eq!(
            event.correlation().step_execution_id(),
            second.step_execution().id()
        );
    }
    Ok(())
}

// OBS-INSPECT-001
#[tokio::test]
async fn inspection_redacts_record_contents() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new()?;
    let sink = CapturingSink::default();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let count = Arc::new(AtomicUsize::new(0));
    let definition = job(
        tasklet(&calls, &count, true),
        [step_listener(
            "redacted",
            &calls,
            Behavior::Pass,
            Behavior::Error,
        )],
        [],
    )?;
    let (_source, stop) = StopSource::new();

    let report = fixture
        .launcher(&sink)
        .launch(&definition, &parameters(SENTINEL_SECRET)?, &stop)
        .await?;
    let events = sink.snapshot();
    let formatted_events = events
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    let debug_events = format!("{events:?}");
    let span_fields = events
        .iter()
        .flat_map(LifecycleEvent::span_fields)
        .map(|field| field.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    let metric_labels = events
        .iter()
        .flat_map(LifecycleEvent::metric_labels)
        .map(|label| label.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    let report_debug = format!("{report:?}");
    let tasklet_error = TaskletError::from_error(SecretError(String::from(SENTINEL_SECRET)));
    let listener_error = ListenerError::from_error(SecretError(String::from(SENTINEL_SECRET)));
    let error_chain =
        format!("{tasklet_error:?} {tasklet_error} {listener_error:?} {listener_error}");

    assert_sentinel_absent([
        ("events", formatted_events.as_str()),
        ("event_debug", debug_events.as_str()),
        ("span_fields", span_fields.as_str()),
        ("metric_labels", metric_labels.as_str()),
        ("execution_inspection", report_debug.as_str()),
        ("error_chain", error_chain.as_str()),
    ]);
    for event in &events {
        for label in event.metric_labels() {
            assert!(matches!(label.key(), "event" | "component" | "status"));
        }
    }
    assert_eq!(
        report.original_outcome(),
        Some(TaskletExecutionOutcome::Failed(TaskletFailure::Error))
    );
    assert!(report.original_failure().is_some());
    Ok(())
}

struct PanickingSink;

impl LifecycleEventSink for PanickingSink {
    fn emit(&self, _event: &LifecycleEvent) {
        panic!("diagnostic sink failure must not affect execution");
    }
}

#[tokio::test]
async fn event_sink_panic_cannot_fail_execution() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new()?;
    let calls = Arc::new(Mutex::new(Vec::new()));
    let count = Arc::new(AtomicUsize::new(0));
    let definition = job(tasklet(&calls, &count, false), [], [])?;
    let (_source, stop) = StopSource::new();

    let report = fixture
        .launcher(&PanickingSink)
        .launch(&definition, &parameters("2026-07-29")?, &stop)
        .await?;

    assert_eq!(report.outcome(), TaskletExecutionOutcome::Completed);
    assert_eq!(count.load(Ordering::SeqCst), 1);
    Ok(())
}
