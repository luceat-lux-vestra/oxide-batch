//! M7 #277 integration evidence for attempt-local live scoped components.

#![allow(clippy::expect_used, clippy::panic)]

use std::error::Error;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use oxide_batch::{
    BatchStatus, BoxFuture, Clock, ComponentRevision, DefinitionRevision, FlowExecutionOutcome,
    FlowFailure, FlowGraph, FlowJob, FlowJobError, FlowLauncher, FlowNode, FlowRuntimeError,
    FlowTarget, InMemoryJobRepository, JobName, JobParameter, JobParameters, LateBoundInput,
    LateBoundSource, MissingParameterPolicy, NodeId, ParameterCoercion, ParameterName,
    ParameterRole, ParameterValue, ParameterValueKind, ScopeBuildFailureKind, ScopeFactoryKind,
    ScopeKind, ScopeResolverKind, ScopedCleanupError, ScopedComponentDefinition,
    ScopedComponentFactory, ScopedComponentHandle, ScopedComponentId, ScopedComponentRegistration,
    ScopedFactoryContext, ScopedFactoryError, SequentialIdGenerator, StepComponents, StepName,
    StepNode, StopSource, Tasklet, TaskletContext, TaskletError, TaskletFailure, TaskletOutcome,
    TaskletStep, TerminalKind,
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
    let clock = Arc::new(FixedClock(SystemTime::UNIX_EPOCH + Duration::from_secs(40)));
    let ids = Arc::new(SequentialIdGenerator::new(NonZeroU64::MIN));
    let repository = InMemoryJobRepository::new(clock.clone(), ids.clone());
    (clock, ids, repository)
}

fn id(value: &str) -> Result<ScopedComponentId, Box<dyn Error>> {
    Ok(ScopedComponentId::new(value)?)
}

fn definition(
    scope: ScopeKind,
    component: &str,
) -> Result<ScopedComponentDefinition, Box<dyn Error>> {
    let tenant = ParameterName::new("tenant")?;
    Ok(ScopedComponentDefinition::new(
        scope,
        id(component)?,
        ScopeFactoryKind::new("integration-factory")?,
        ComponentRevision::new("factory-v1")?,
        ScopeResolverKind::new("structured-selector")?,
        ComponentRevision::new("resolver-v1")?,
        vec![LateBoundInput::new(
            tenant.clone(),
            LateBoundSource::JobParameter(tenant),
            ParameterValueKind::String,
            ParameterCoercion::Exact,
            MissingParameterPolicy::Fail,
        )],
    )?)
}

fn parameters() -> Result<JobParameters, Box<dyn Error>> {
    let mut parameters = JobParameters::new();
    parameters.insert(
        ParameterName::new("tenant")?,
        JobParameter::new(ParameterValue::string("acme")?, ParameterRole::Identifying),
    )?;
    Ok(parameters)
}

struct RecordingFactory {
    label: &'static str,
    creates: Arc<AtomicUsize>,
    cleanups: Arc<AtomicUsize>,
    events: Arc<Mutex<Vec<String>>>,
    reject_first: bool,
    cleanup_fails: bool,
}

impl ScopedComponentFactory for RecordingFactory {
    fn create<'a>(
        &'a self,
        context: ScopedFactoryContext<'a>,
    ) -> BoxFuture<'a, Result<ScopedComponentHandle, ScopedFactoryError>> {
        Box::pin(async move {
            let tenant = ParameterName::new("tenant").map_err(|_| ScopedFactoryError)?;
            let value = context
                .inputs()
                .get(&tenant)
                .and_then(ParameterValue::as_str)
                .ok_or(ScopedFactoryError)?;
            if value != "acme" {
                return Err(ScopedFactoryError);
            }
            let ordinal = self.creates.fetch_add(1, Ordering::SeqCst) + 1;
            if self.reject_first && ordinal == 1 {
                return Err(ScopedFactoryError);
            }
            let value = format!("{}-{ordinal}", self.label);
            self.events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(format!("create:{value}"));
            Ok(ScopedComponentHandle::new(value))
        })
    }

    fn cleanup(
        &self,
        component: ScopedComponentHandle,
    ) -> BoxFuture<'_, Result<(), ScopedCleanupError>> {
        Box::pin(async move {
            let value = component
                .downcast_ref::<String>()
                .ok_or(ScopedCleanupError)?
                .clone();
            self.cleanups.fetch_add(1, Ordering::SeqCst);
            self.events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(format!("cleanup:{value}"));
            if self.cleanup_fails {
                Err(ScopedCleanupError)
            } else {
                Ok(())
            }
        })
    }
}

fn registration(
    scope: ScopeKind,
    component: &str,
    label: &'static str,
    creates: Arc<AtomicUsize>,
    cleanups: Arc<AtomicUsize>,
    events: Arc<Mutex<Vec<String>>>,
) -> Result<ScopedComponentRegistration, Box<dyn Error>> {
    registration_with_modes(
        scope, component, label, creates, cleanups, events, false, false,
    )
}

fn registration_with_reject_first(
    scope: ScopeKind,
    component: &str,
    label: &'static str,
    creates: Arc<AtomicUsize>,
    cleanups: Arc<AtomicUsize>,
    events: Arc<Mutex<Vec<String>>>,
    reject_first: bool,
) -> Result<ScopedComponentRegistration, Box<dyn Error>> {
    registration_with_modes(
        scope,
        component,
        label,
        creates,
        cleanups,
        events,
        reject_first,
        false,
    )
}

fn registration_with_modes(
    scope: ScopeKind,
    component: &str,
    label: &'static str,
    creates: Arc<AtomicUsize>,
    cleanups: Arc<AtomicUsize>,
    events: Arc<Mutex<Vec<String>>>,
    reject_first: bool,
    cleanup_fails: bool,
) -> Result<ScopedComponentRegistration, Box<dyn Error>> {
    Ok(ScopedComponentRegistration::new(
        scope,
        id(component)?,
        ScopeFactoryKind::new("integration-factory")?,
        ComponentRevision::new("factory-v1")?,
        Vec::new(),
        Arc::new(RecordingFactory {
            label,
            creates,
            cleanups,
            events,
            reject_first,
            cleanup_fails,
        }),
    )?)
}

#[derive(Clone, Copy)]
enum TaskletMode {
    Complete,
    FailFirst,
    Fail,
    Panic,
    Stop,
    WaitForStop,
}

struct ScopedTasklet {
    mode: TaskletMode,
    calls: Arc<AtomicUsize>,
    observations: Arc<Mutex<Vec<(String, String)>>>,
    events: Arc<Mutex<Vec<String>>>,
}

impl Tasklet for ScopedTasklet {
    fn execute<'a>(
        &'a self,
        context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            let job_first = context
                .scoped_component_as::<String>(
                    ScopeKind::Job,
                    &ScopedComponentId::new("job-client").expect("job component id"),
                )
                .ok_or_else(TaskletError::new)?;
            let job_second = context
                .scoped_component_as::<String>(
                    ScopeKind::Job,
                    &ScopedComponentId::new("job-client").expect("job component id"),
                )
                .ok_or_else(TaskletError::new)?;
            let step_first = context
                .scoped_component_as::<String>(
                    ScopeKind::Step,
                    &ScopedComponentId::new("step-client").expect("step component id"),
                )
                .ok_or_else(TaskletError::new)?;
            let step_second = context
                .scoped_component_as::<String>(
                    ScopeKind::Step,
                    &ScopedComponentId::new("step-client").expect("step component id"),
                )
                .ok_or_else(TaskletError::new)?;

            if !std::ptr::eq(job_first, job_second) || !std::ptr::eq(step_first, step_second) {
                return Err(TaskletError::new());
            }
            if context
                .scoped_component(
                    ScopeKind::Job,
                    &ScopedComponentId::new("job-client").expect("job component id"),
                )
                .and_then(ScopedComponentHandle::downcast_ref::<String>)
                != Some(job_first)
            {
                return Err(TaskletError::new());
            }

            self.observations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push((job_first.clone(), step_first.clone()));
            self.events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(format!("tasklet:{job_first}:{step_first}"));

            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            match self.mode {
                TaskletMode::Complete => Ok(TaskletOutcome::Completed),
                TaskletMode::FailFirst => {
                    if call == 0 {
                        Err(TaskletError::new())
                    } else {
                        Ok(TaskletOutcome::Completed)
                    }
                }
                TaskletMode::Fail => Err(TaskletError::new()),
                TaskletMode::Panic => panic!("scoped tasklet panic"),
                TaskletMode::Stop => Ok(TaskletOutcome::Stopped),
                TaskletMode::WaitForStop => {
                    context.stop_token().cancelled().await;
                    Ok(TaskletOutcome::Stopped)
                }
            }
        })
    }
}

struct Harness {
    job: FlowJob,
    job_creates: Arc<AtomicUsize>,
    job_cleanups: Arc<AtomicUsize>,
    step_creates: Arc<AtomicUsize>,
    step_cleanups: Arc<AtomicUsize>,
    observations: Arc<Mutex<Vec<(String, String)>>>,
    events: Arc<Mutex<Vec<String>>>,
    tasklet_calls: Arc<AtomicUsize>,
}

fn harness(name: &str, mode: TaskletMode) -> Result<Harness, Box<dyn Error>> {
    let node = NodeId::new("work")?;
    let job_name = JobName::new(name)?;
    let plan = FlowGraph::new(node.clone())
        .with_node(FlowNode::step(StepNode::new(
            node.clone(),
            StepName::new("work")?,
            StepComponents::Tasklet(ComponentRevision::new("tasklet-v1")?),
        )))
        .with_sequence(node.clone(), FlowTarget::Terminal(TerminalKind::Complete))?
        .with_scoped_component(definition(ScopeKind::Job, "job-client")?)
        .with_scoped_component(definition(ScopeKind::Step, "step-client")?)
        .compile(&job_name, DefinitionRevision::new("v1")?)?;

    let job_creates = Arc::new(AtomicUsize::new(0));
    let job_cleanups = Arc::new(AtomicUsize::new(0));
    let step_creates = Arc::new(AtomicUsize::new(0));
    let step_cleanups = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let observations = Arc::new(Mutex::new(Vec::new()));
    let events = Arc::new(Mutex::new(Vec::new()));

    let job = FlowJob::new(job_name, plan)?
        .with_tasklet_step(
            node,
            TaskletStep::new(
                StepName::new("work")?,
                Arc::new(ScopedTasklet {
                    mode,
                    calls: Arc::clone(&calls),
                    observations: Arc::clone(&observations),
                    events: Arc::clone(&events),
                }),
            ),
        )?
        .with_scoped_component_registration(registration(
            ScopeKind::Job,
            "job-client",
            "job",
            Arc::clone(&job_creates),
            Arc::clone(&job_cleanups),
            Arc::clone(&events),
        )?)?
        .with_scoped_component_registration(registration(
            ScopeKind::Step,
            "step-client",
            "step",
            Arc::clone(&step_creates),
            Arc::clone(&step_cleanups),
            Arc::clone(&events),
        )?)?;

    Ok(Harness {
        job,
        job_creates,
        job_cleanups,
        step_creates,
        step_cleanups,
        observations,
        events,
        tasklet_calls: calls,
    })
}

#[derive(Debug)]
struct ValidationFactory;

impl ScopedComponentFactory for ValidationFactory {
    fn create<'a>(
        &'a self,
        _context: ScopedFactoryContext<'a>,
    ) -> BoxFuture<'a, Result<ScopedComponentHandle, ScopedFactoryError>> {
        Box::pin(async { Ok(ScopedComponentHandle::new(())) })
    }

    fn cleanup(
        &self,
        _component: ScopedComponentHandle,
    ) -> BoxFuture<'_, Result<(), ScopedCleanupError>> {
        Box::pin(async { Ok(()) })
    }
}

fn validation_job() -> Result<FlowJob, Box<dyn Error>> {
    let node = NodeId::new("work")?;
    let job_name = JobName::new("live-scope-validation")?;
    let plan = FlowGraph::new(node.clone())
        .with_node(FlowNode::step(StepNode::new(
            node.clone(),
            StepName::new("work")?,
            StepComponents::Tasklet(ComponentRevision::new("tasklet-v1")?),
        )))
        .with_sequence(node.clone(), FlowTarget::Terminal(TerminalKind::Complete))?
        .with_scoped_component(definition(ScopeKind::Step, "step-client")?)
        .compile(&job_name, DefinitionRevision::new("v1")?)?;

    Ok(FlowJob::new(job_name, plan)?.with_tasklet_step(
        node,
        TaskletStep::new(
            StepName::new("work")?,
            Arc::new(ScopedTasklet {
                mode: TaskletMode::Complete,
                calls: Arc::new(AtomicUsize::new(0)),
                observations: Arc::new(Mutex::new(Vec::new())),
                events: Arc::new(Mutex::new(Vec::new())),
            }),
        ),
    )?)
}

#[test]
fn flow_job_rejects_missing_mismatched_and_invalid_scope_bindings() -> Result<(), Box<dyn Error>> {
    let missing = validation_job()?;
    assert!(matches!(
        missing.validate(),
        Err(FlowJobError::MissingScopedComponentBinding {
            scope: ScopeKind::Step,
            ..
        })
    ));

    let mismatched = ScopedComponentRegistration::new(
        ScopeKind::Step,
        id("step-client")?,
        ScopeFactoryKind::new("integration-factory")?,
        ComponentRevision::new("factory-v2")?,
        Vec::new(),
        Arc::new(ValidationFactory),
    )?;
    assert!(matches!(
        validation_job()?.with_scoped_component_registration(mismatched),
        Err(FlowJobError::ScopedComponentRegistrationMismatch {
            scope: ScopeKind::Step,
            ..
        })
    ));

    let invalid_graph = ScopedComponentRegistration::new(
        ScopeKind::Step,
        id("step-client")?,
        ScopeFactoryKind::new("integration-factory")?,
        ComponentRevision::new("factory-v1")?,
        vec![id("missing-dependency")?],
        Arc::new(ValidationFactory),
    )?;
    let invalid = validation_job()?.with_scoped_component_registration(invalid_graph)?;
    assert!(matches!(
        invalid.validate(),
        Err(FlowJobError::InvalidScopedComponentGraph {
            scope: ScopeKind::Step,
            ..
        })
    ));

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn restart_recreates_attempt_local_scopes_and_preserves_in_scope_memoization()
-> Result<(), Box<dyn Error>> {
    let harness = harness("live-scope-restart", TaskletMode::FailFirst)?;
    let (clock, ids, repository) = infrastructure();
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());
    let (_, stop) = StopSource::new();
    let parameters = parameters()?;

    let first = launcher.launch(&harness.job, &parameters, &stop).await?;
    assert!(matches!(first.outcome(), FlowExecutionOutcome::Failed(_)));
    let second = launcher.launch(&harness.job, &parameters, &stop).await?;
    assert_eq!(second.outcome(), &FlowExecutionOutcome::Completed);

    assert_eq!(harness.job_creates.load(Ordering::SeqCst), 2);
    assert_eq!(harness.step_creates.load(Ordering::SeqCst), 2);
    assert_eq!(harness.job_cleanups.load(Ordering::SeqCst), 2);
    assert_eq!(harness.step_cleanups.load(Ordering::SeqCst), 2);
    assert_eq!(
        *harness
            .observations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        vec![
            (String::from("job-1"), String::from("step-1")),
            (String::from("job-2"), String::from("step-2")),
        ],
    );
    assert_eq!(
        *harness
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        vec![
            "create:job-1",
            "create:step-1",
            "tasklet:job-1:step-1",
            "cleanup:step-1",
            "cleanup:job-1",
            "create:job-2",
            "create:step-2",
            "tasklet:job-2:step-2",
            "cleanup:step-2",
            "cleanup:job-2",
        ],
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn failure_panic_and_stopped_outcomes_cleanup_both_scopes_exactly_once()
-> Result<(), Box<dyn Error>> {
    for (name, mode, expected) in [
        ("live-scope-failure", TaskletMode::Fail, BatchStatus::Failed),
        ("live-scope-panic", TaskletMode::Panic, BatchStatus::Failed),
        ("live-scope-stop", TaskletMode::Stop, BatchStatus::Stopped),
    ] {
        let harness = harness(name, mode)?;
        let (clock, ids, repository) = infrastructure();
        let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());
        let (_, stop) = StopSource::new();
        let report = launcher.launch(&harness.job, &parameters()?, &stop).await?;

        assert_eq!(report.job_execution().metadata().status(), expected);
        assert_eq!(harness.job_creates.load(Ordering::SeqCst), 1);
        assert_eq!(harness.step_creates.load(Ordering::SeqCst), 1);
        assert_eq!(harness.job_cleanups.load(Ordering::SeqCst), 1);
        assert_eq!(harness.step_cleanups.load(Ordering::SeqCst), 1);
        let events = harness
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(events.len(), 5);
        assert_eq!(events[0], "create:job-1");
        assert_eq!(events[1], "create:step-1");
        assert_eq!(events[3], "cleanup:step-1");
        assert_eq!(events[4], "cleanup:job-1");
    }
    Ok(())
}

fn rejecting_harness(name: &str, reject_scope: ScopeKind) -> Result<Harness, Box<dyn Error>> {
    let node = NodeId::new("work")?;
    let job_name = JobName::new(name)?;
    let plan = FlowGraph::new(node.clone())
        .with_node(FlowNode::step(StepNode::new(
            node.clone(),
            StepName::new("work")?,
            StepComponents::Tasklet(ComponentRevision::new("tasklet-v1")?),
        )))
        .with_sequence(node.clone(), FlowTarget::Terminal(TerminalKind::Complete))?
        .with_scoped_component(definition(ScopeKind::Job, "job-client")?)
        .with_scoped_component(definition(ScopeKind::Step, "step-client")?)
        .compile(&job_name, DefinitionRevision::new("v1")?)?;

    let job_creates = Arc::new(AtomicUsize::new(0));
    let job_cleanups = Arc::new(AtomicUsize::new(0));
    let step_creates = Arc::new(AtomicUsize::new(0));
    let step_cleanups = Arc::new(AtomicUsize::new(0));
    let observations = Arc::new(Mutex::new(Vec::new()));
    let events = Arc::new(Mutex::new(Vec::new()));
    let tasklet_calls = Arc::new(AtomicUsize::new(0));

    let job = FlowJob::new(job_name, plan)?
        .with_tasklet_step(
            node,
            TaskletStep::new(
                StepName::new("work")?,
                Arc::new(ScopedTasklet {
                    mode: TaskletMode::Complete,
                    calls: Arc::clone(&tasklet_calls),
                    observations: Arc::clone(&observations),
                    events: Arc::clone(&events),
                }),
            ),
        )?
        .with_scoped_component_registration(registration_with_reject_first(
            ScopeKind::Job,
            "job-client",
            "job",
            Arc::clone(&job_creates),
            Arc::clone(&job_cleanups),
            Arc::clone(&events),
            reject_scope == ScopeKind::Job,
        )?)?
        .with_scoped_component_registration(registration_with_reject_first(
            ScopeKind::Step,
            "step-client",
            "step",
            Arc::clone(&step_creates),
            Arc::clone(&step_cleanups),
            Arc::clone(&events),
            reject_scope == ScopeKind::Step,
        )?)?;

    Ok(Harness {
        job,
        job_creates,
        job_cleanups,
        step_creates,
        step_cleanups,
        observations,
        events,
        tasklet_calls,
    })
}

fn cleanup_failure_harness(
    name: &str,
    mode: TaskletMode,
    job_cleanup_fails: bool,
    step_cleanup_fails: bool,
) -> Result<Harness, Box<dyn Error>> {
    let node = NodeId::new("work")?;
    let job_name = JobName::new(name)?;
    let plan = FlowGraph::new(node.clone())
        .with_node(FlowNode::step(StepNode::new(
            node.clone(),
            StepName::new("work")?,
            StepComponents::Tasklet(ComponentRevision::new("tasklet-v1")?),
        )))
        .with_sequence(node.clone(), FlowTarget::Terminal(TerminalKind::Complete))?
        .with_scoped_component(definition(ScopeKind::Job, "job-client")?)
        .with_scoped_component(definition(ScopeKind::Step, "step-client")?)
        .compile(&job_name, DefinitionRevision::new("v1")?)?;

    let job_creates = Arc::new(AtomicUsize::new(0));
    let job_cleanups = Arc::new(AtomicUsize::new(0));
    let step_creates = Arc::new(AtomicUsize::new(0));
    let step_cleanups = Arc::new(AtomicUsize::new(0));
    let observations = Arc::new(Mutex::new(Vec::new()));
    let events = Arc::new(Mutex::new(Vec::new()));
    let tasklet_calls = Arc::new(AtomicUsize::new(0));

    let job = FlowJob::new(job_name, plan)?
        .with_tasklet_step(
            node,
            TaskletStep::new(
                StepName::new("work")?,
                Arc::new(ScopedTasklet {
                    mode,
                    calls: Arc::clone(&tasklet_calls),
                    observations: Arc::clone(&observations),
                    events: Arc::clone(&events),
                }),
            ),
        )?
        .with_scoped_component_registration(registration_with_modes(
            ScopeKind::Job,
            "job-client",
            "job",
            Arc::clone(&job_creates),
            Arc::clone(&job_cleanups),
            Arc::clone(&events),
            false,
            job_cleanup_fails,
        )?)?
        .with_scoped_component_registration(registration_with_modes(
            ScopeKind::Step,
            "step-client",
            "step",
            Arc::clone(&step_creates),
            Arc::clone(&step_cleanups),
            Arc::clone(&events),
            false,
            step_cleanup_fails,
        )?)?;

    Ok(Harness {
        job,
        job_creates,
        job_cleanups,
        step_creates,
        step_cleanups,
        observations,
        events,
        tasklet_calls,
    })
}

#[tokio::test(flavor = "current_thread")]
async fn cleanup_failure_is_primary_only_after_successful_work() -> Result<(), Box<dyn Error>> {
    for (name, job_cleanup_fails, step_cleanup_fails, expected_scope) in [
        (
            "live-scope-step-cleanup-failure",
            false,
            true,
            ScopeKind::Step,
        ),
        (
            "live-scope-job-cleanup-failure",
            true,
            false,
            ScopeKind::Job,
        ),
    ] {
        let harness = cleanup_failure_harness(
            name,
            TaskletMode::Complete,
            job_cleanup_fails,
            step_cleanup_fails,
        )?;
        let (clock, ids, repository) = infrastructure();
        let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());
        let (_, stop) = StopSource::new();

        let report = launcher.launch(&harness.job, &parameters()?, &stop).await?;
        assert!(matches!(
            report.outcome(),
            FlowExecutionOutcome::Failed(FlowFailure::ScopedCleanup { scope, failures: 1 })
                if *scope == expected_scope
        ));
        assert_eq!(harness.job_cleanups.load(Ordering::SeqCst), 1);
        assert_eq!(harness.step_cleanups.load(Ordering::SeqCst), 1);
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn cleanup_failures_do_not_replace_existing_tasklet_failure() -> Result<(), Box<dyn Error>> {
    let harness = cleanup_failure_harness(
        "live-scope-secondary-cleanup-failure",
        TaskletMode::Fail,
        true,
        true,
    )?;
    let (clock, ids, repository) = infrastructure();
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());
    let (_, stop) = StopSource::new();

    let report = launcher.launch(&harness.job, &parameters()?, &stop).await?;
    assert!(matches!(
        report.outcome(),
        FlowExecutionOutcome::Failed(FlowFailure::Tasklet(TaskletFailure::Error))
    ));
    assert_eq!(harness.job_cleanups.load(Ordering::SeqCst), 1);
    assert_eq!(harness.step_cleanups.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn framework_stop_token_cleans_both_live_scopes() -> Result<(), Box<dyn Error>> {
    let harness = harness("live-scope-cancellation", TaskletMode::WaitForStop)?;
    let (clock, ids, repository) = infrastructure();
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());
    let (source, stop) = StopSource::new();
    let parameters = parameters()?;

    let launch = launcher.launch(&harness.job, &parameters, &stop);
    let request_stop = async {
        while harness.tasklet_calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        source.request_stop();
    };
    let (report, ()) = tokio::join!(launch, request_stop);
    let report = report?;

    assert_eq!(report.outcome(), &FlowExecutionOutcome::Stopped);
    assert_eq!(
        report.job_execution().metadata().status(),
        BatchStatus::Stopped
    );
    assert_eq!(harness.job_cleanups.load(Ordering::SeqCst), 1);
    assert_eq!(harness.step_cleanups.load(Ordering::SeqCst), 1);
    let events = harness
        .events
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(events[0], "create:job-1");
    assert_eq!(events[1], "create:step-1");
    assert_eq!(events[3], "cleanup:step-1");
    assert_eq!(events[4], "cleanup:job-1");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn scope_construction_failure_terminalizes_the_attempt_and_allows_restart()
-> Result<(), Box<dyn Error>> {
    for (name, rejected_scope) in [
        ("live-scope-job-build-restart", ScopeKind::Job),
        ("live-scope-step-build-restart", ScopeKind::Step),
    ] {
        let harness = rejecting_harness(name, rejected_scope)?;
        let (clock, ids, repository) = infrastructure();
        let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());
        let (_, stop) = StopSource::new();
        let parameters = parameters()?;

        let first = launcher
            .launch(&harness.job, &parameters, &stop)
            .await
            .expect_err("first factory construction must fail");
        assert!(matches!(
            first,
            FlowRuntimeError::ScopeConstruction {
                scope,
                failure: ScopeBuildFailureKind::FactoryRejected,
                ..
            } if scope == rejected_scope
        ));

        let second = launcher.launch(&harness.job, &parameters, &stop).await?;
        assert_eq!(second.outcome(), &FlowExecutionOutcome::Completed);
        assert_eq!(
            second.job_execution().metadata().status(),
            BatchStatus::Completed
        );

        assert_eq!(harness.job_creates.load(Ordering::SeqCst), 2);
        assert_eq!(
            harness.job_cleanups.load(Ordering::SeqCst),
            if rejected_scope == ScopeKind::Job {
                1
            } else {
                2
            }
        );
        assert_eq!(
            harness.step_creates.load(Ordering::SeqCst),
            if rejected_scope == ScopeKind::Job {
                1
            } else {
                2
            }
        );
        assert_eq!(harness.step_cleanups.load(Ordering::SeqCst), 1);
    }
    Ok(())
}
