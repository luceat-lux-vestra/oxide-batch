//! Runtime evidence for registered custom leaves.

#![allow(clippy::expect_used, clippy::panic)]

use std::error::Error;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use oxide_batch::{
    BackoffOutcome, BackoffPolicy, BackoffSleeper, BatchStatus, BoxFuture, ChunkDeliveryMode,
    ClassifierRevision, Clock, ComponentRevision, CustomLeafContext, CustomLeafHandler,
    CustomLeafKind, CustomLeafNode, CustomLeafRegistration, CustomLeafResult, DefinitionRevision,
    ExecutionContext, FailureCategory, FaultAction, FaultClassifier, FaultPhase, FaultPolicy,
    FaultRule, FaultRuntime, FlowExecutionOutcome, FlowFailure, FlowGraph, FlowJob, FlowLauncher,
    FlowNode, FlowTarget, InMemoryFaultState, InMemoryJobRepository, JobName, JobParameters,
    JobRepository, ListenerContext, ListenerError, RetryLimit, RetryStateLimit,
    SequentialIdGenerator, SkipLimit, StateLimits, StateSchemaId, StateSchemaVersion,
    StepExecutionListener, StepName, StopSource, StopToken, TaskletError, TaskletExecutionOutcome,
    TaskletFailure, TaskletOutcome, TerminalKind,
};

#[derive(Debug)]
struct FixedClock(SystemTime);

impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        self.0
    }
}

fn infrastructure() -> (
    Arc<FixedClock>,
    Arc<SequentialIdGenerator>,
    InMemoryJobRepository,
) {
    let clock = Arc::new(FixedClock(SystemTime::UNIX_EPOCH + Duration::from_secs(30)));
    let ids = Arc::new(SequentialIdGenerator::new(NonZeroU64::MIN));
    let repository = InMemoryJobRepository::new(clock.clone(), ids.clone());
    (clock, ids, repository)
}

fn context(schema: &str, version: u32, value: u64) -> Result<ExecutionContext, Box<dyn Error>> {
    let bytes = format!(
        r#"{{"format":"oxide-batch.execution-context","format_version":1,"schema":"{schema}","schema_version":{version},"payload":{{"value":{value}}}}}"#
    );
    Ok(ExecutionContext::from_json(
        bytes.as_bytes(),
        StateLimits::default(),
    )?)
}

fn context_value(state: &ExecutionContext) -> Option<u64> {
    serde_json::from_slice::<serde_json::Value>(&state.payload_json().ok()?)
        .ok()?
        .get("value")?
        .as_u64()
}

fn node(
    job_name: &str,
    listener_revision: Option<ComponentRevision>,
    fault: Option<FaultPolicy>,
) -> Result<
    (
        JobName,
        oxide_batch::NodeId,
        oxide_batch::CompiledExecutionPlan,
    ),
    Box<dyn Error>,
> {
    let name = JobName::new(job_name)?;
    let id = oxide_batch::NodeId::new("custom")?;
    let mut custom = CustomLeafNode::new(
        id.clone(),
        StepName::new("custom-step")?,
        CustomLeafKind::new("example.handler")?,
        ComponentRevision::new("handler-v1")?,
        StateSchemaId::new("example.state")?,
        StateSchemaVersion::new(1)?,
    );
    if let Some(revision) = listener_revision {
        custom = custom.with_listener_revision(revision);
    }
    if let Some(policy) = fault {
        custom = custom.with_fault_policy(policy);
    }
    let plan = FlowGraph::new(id.clone())
        .with_node(FlowNode::custom_leaf(custom))
        .with_sequence(id.clone(), FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(&name, DefinitionRevision::new("v1")?)?;
    Ok((name, id, plan))
}

struct StatefulHandler {
    seen: Arc<Mutex<Vec<Option<u64>>>>,
    next: Arc<AtomicUsize>,
}

impl CustomLeafHandler for StatefulHandler {
    fn execute<'a>(
        &'a self,
        context: CustomLeafContext<'a>,
    ) -> BoxFuture<'a, Result<CustomLeafResult, TaskletError>> {
        Box::pin(async move {
            let previous = context.previous_state().and_then(context_value);
            self.seen
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(previous);
            let next = self.next.fetch_add(1, Ordering::SeqCst) as u64 + 1;
            Ok(CustomLeafResult::new(TaskletOutcome::Completed)
                .with_state(crate_context(next).map_err(TaskletError::from_error)?))
        })
    }
}

fn crate_context(value: u64) -> Result<ExecutionContext, std::io::Error> {
    context("example.state", 1, value).map_err(|error| std::io::Error::other(error.to_string()))
}

struct FailFirstAfter {
    calls: AtomicUsize,
}

impl StepExecutionListener for FailFirstAfter {
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
        Box::pin(async move {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(ListenerError::new())
            } else {
                Ok(())
            }
        })
    }
}

#[tokio::test]
async fn committed_custom_state_is_reused_on_restart() -> Result<(), Box<dyn Error>> {
    let listener_revision = ComponentRevision::new("listener-v1")?;
    let (name, id, plan) = node("custom-leaf-restart", Some(listener_revision.clone()), None)?;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let handler = Arc::new(StatefulHandler {
        seen: seen.clone(),
        next: Arc::new(AtomicUsize::new(0)),
    });
    let registration = CustomLeafRegistration::new(
        CustomLeafKind::new("example.handler")?,
        ComponentRevision::new("handler-v1")?,
        StateSchemaId::new("example.state")?,
        StateSchemaVersion::new(1)?,
        handler,
    )
    .with_listener(
        listener_revision,
        Arc::new(FailFirstAfter {
            calls: AtomicUsize::new(0),
        }),
    );
    let job = FlowJob::new(name, plan)?.with_custom_leaf_registration(id.clone(), registration)?;
    let (clock, ids, repository) = infrastructure();
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());
    let (_, stop) = StopSource::new();

    let first = launcher.launch(&job, &JobParameters::new(), &stop).await?;
    assert!(matches!(
        first.outcome(),
        FlowExecutionOutcome::Failed(FlowFailure::Listener(_))
    ));

    let second = launcher.launch(&job, &JobParameters::new(), &stop).await?;
    assert_eq!(second.outcome(), &FlowExecutionOutcome::Completed);

    assert_eq!(
        *seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        vec![None, Some(1)]
    );

    let mut unit = repository.begin().await?;
    let durable = unit
        .latest_flow_step(second.instance().id(), &id)
        .await?
        .expect("custom leaf state");
    assert_eq!(
        durable.context().and_then(context_value),
        Some(2),
        "the second committed custom state must remain authoritative"
    );
    unit.rollback().await?;
    Ok(())
}

struct CooperativeStopHandler {
    entered: Arc<AtomicUsize>,
    observed_stop: Arc<AtomicUsize>,
}

impl CustomLeafHandler for CooperativeStopHandler {
    fn execute<'a>(
        &'a self,
        context: CustomLeafContext<'a>,
    ) -> BoxFuture<'a, Result<CustomLeafResult, TaskletError>> {
        let stop = context.stop_token().clone();
        let entered = self.entered.clone();
        let observed_stop = self.observed_stop.clone();
        Box::pin(async move {
            entered.store(1, Ordering::SeqCst);
            while !stop.is_stop_requested() {
                tokio::task::yield_now().await;
            }
            observed_stop.store(1, Ordering::SeqCst);
            Ok(CustomLeafResult::new(TaskletOutcome::Stopped))
        })
    }
}

#[tokio::test]
async fn custom_leaf_observes_framework_owned_cancellation_and_is_joined()
-> Result<(), Box<dyn Error>> {
    let (name, id, plan) = node("custom-leaf-cancellation", None, None)?;
    let entered = Arc::new(AtomicUsize::new(0));
    let observed_stop = Arc::new(AtomicUsize::new(0));
    let handler = Arc::new(CooperativeStopHandler {
        entered: entered.clone(),
        observed_stop: observed_stop.clone(),
    });
    let registration = CustomLeafRegistration::new(
        CustomLeafKind::new("example.handler")?,
        ComponentRevision::new("handler-v1")?,
        StateSchemaId::new("example.state")?,
        StateSchemaVersion::new(1)?,
        handler,
    );
    let job = FlowJob::new(name, plan)?.with_custom_leaf_registration(id, registration)?;
    let (clock, ids, repository) = infrastructure();
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());
    let (source, stop) = StopSource::new();

    let launch = launcher.launch(&job, &JobParameters::new(), &stop);
    let request_stop = async {
        while entered.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        source.request_stop();
    };
    let (report, ()) = tokio::join!(launch, request_stop);
    let report = report?;

    assert_eq!(observed_stop.load(Ordering::SeqCst), 1);
    assert_eq!(report.outcome(), &FlowExecutionOutcome::Stopped);
    assert_eq!(report.step_executions().len(), 1);
    assert_eq!(
        report.step_executions()[0].metadata().status(),
        BatchStatus::Stopped
    );
    Ok(())
}

#[derive(Clone, Copy)]
enum Mode {
    Error,
    Panic,
    Stop,
    Unknown,
    WrongState,
}

struct OutcomeHandler(Mode);

impl CustomLeafHandler for OutcomeHandler {
    fn execute<'a>(
        &'a self,
        _context: CustomLeafContext<'a>,
    ) -> BoxFuture<'a, Result<CustomLeafResult, TaskletError>> {
        Box::pin(async move {
            match self.0 {
                Mode::Error => Err(TaskletError::new()),
                Mode::Panic => panic!("contained custom-leaf panic"),
                Mode::Stop => Ok(CustomLeafResult::new(TaskletOutcome::Stopped)),
                Mode::Unknown => Ok(CustomLeafResult::new(TaskletOutcome::CommitOutcomeUnknown)),
                Mode::WrongState => Ok(CustomLeafResult::new(TaskletOutcome::Completed)
                    .with_state(
                        context("example.state", 2, 1).expect("valid wrong-state fixture"),
                    )),
            }
        })
    }
}

async fn launch_mode(mode: Mode, name: &str) -> Result<FlowExecutionOutcome, Box<dyn Error>> {
    let (name, id, plan) = node(name, None, None)?;
    let registration = CustomLeafRegistration::new(
        CustomLeafKind::new("example.handler")?,
        ComponentRevision::new("handler-v1")?,
        StateSchemaId::new("example.state")?,
        StateSchemaVersion::new(1)?,
        Arc::new(OutcomeHandler(mode)),
    );
    let job = FlowJob::new(name, plan)?.with_custom_leaf_registration(id, registration)?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();
    Ok(FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?
        .outcome()
        .clone())
}

#[tokio::test]
async fn custom_leaf_error_panic_stop_unknown_and_state_mismatch_fail_closed()
-> Result<(), Box<dyn Error>> {
    assert!(matches!(
        launch_mode(Mode::Error, "custom-leaf-error").await?,
        FlowExecutionOutcome::Failed(FlowFailure::Tasklet(TaskletFailure::Error))
    ));
    assert!(matches!(
        launch_mode(Mode::Panic, "custom-leaf-panic").await?,
        FlowExecutionOutcome::Failed(FlowFailure::Tasklet(TaskletFailure::Panic))
    ));
    assert_eq!(
        launch_mode(Mode::Stop, "custom-leaf-stop").await?,
        FlowExecutionOutcome::Stopped
    );
    assert_eq!(
        launch_mode(Mode::Unknown, "custom-leaf-unknown").await?,
        FlowExecutionOutcome::Unknown
    );
    assert!(matches!(
        launch_mode(Mode::WrongState, "custom-leaf-wrong-state").await?,
        FlowExecutionOutcome::Failed(FlowFailure::CustomLeafState { .. })
    ));
    Ok(())
}

struct FailOnceHandler(AtomicUsize);

impl CustomLeafHandler for FailOnceHandler {
    fn execute<'a>(
        &'a self,
        _context: CustomLeafContext<'a>,
    ) -> BoxFuture<'a, Result<CustomLeafResult, TaskletError>> {
        Box::pin(async move {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(TaskletError::new())
            } else {
                Ok(CustomLeafResult::new(TaskletOutcome::Completed))
            }
        })
    }
}

struct ImmediateSleeper;

impl BackoffSleeper for ImmediateSleeper {
    fn sleep<'a>(&'a self, _delay: Duration, stop: &'a StopToken) -> BoxFuture<'a, BackoffOutcome> {
        Box::pin(async move {
            if stop.is_stop_requested() {
                BackoffOutcome::Stopped
            } else {
                BackoffOutcome::Elapsed
            }
        })
    }
}

fn retry_policy() -> Result<FaultPolicy, Box<dyn Error>> {
    Ok(FaultPolicy::new(
        FaultClassifier::new(
            ClassifierRevision::new("custom-leaf-v1")?,
            [FaultRule::new(
                FaultPhase::Process,
                FailureCategory::UserComponent,
                FaultAction::retry(),
            )?],
        )?,
        RetryLimit::new(1)?,
        RetryStateLimit::new(8)?,
        SkipLimit::NONE,
        BackoffPolicy::fixed(Duration::from_millis(1))?,
    )?)
}

#[tokio::test]
async fn custom_leaf_uses_the_framework_fault_port_for_retry() -> Result<(), Box<dyn Error>> {
    let policy = retry_policy()?;
    let (name, id, plan) = node("custom-leaf-fault", None, Some(policy.clone()))?;
    let handler = Arc::new(FailOnceHandler(AtomicUsize::new(0)));
    let runtime = FaultRuntime::new(
        policy.clone(),
        Arc::new(ImmediateSleeper),
        Arc::new(InMemoryFaultState::new(policy.retry_state_limit())),
        ChunkDeliveryMode::AtLeastOnce,
    )?;
    let registration = CustomLeafRegistration::new(
        CustomLeafKind::new("example.handler")?,
        ComponentRevision::new("handler-v1")?,
        StateSchemaId::new("example.state")?,
        StateSchemaVersion::new(1)?,
        handler.clone(),
    )
    .with_fault_runtime(runtime);
    let job = FlowJob::new(name, plan)?.with_custom_leaf_registration(id, registration)?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();

    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;
    assert_eq!(report.outcome(), &FlowExecutionOutcome::Completed);
    assert_eq!(handler.0.load(Ordering::SeqCst), 2);
    assert_eq!(
        report.step_executions()[0].metadata().status(),
        BatchStatus::Completed
    );
    Ok(())
}
