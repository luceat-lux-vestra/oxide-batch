//! In-memory end-to-end evidence for M7 #265 nested-job runtime semantics.

#![allow(clippy::expect_used, clippy::panic)]

use std::error::Error;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use oxide_batch::{
    BatchStatus, BoxFuture, Clock, ComponentRevision, DefinitionRevision, FlowExecutionOutcome,
    FlowFailure, FlowGraph, FlowJob, FlowLauncher, FlowNode, FlowTarget, InMemoryJobRepository,
    JobName, JobParameter, JobParameters, JobRepository, MissingParameterPolicy,
    NestedJobMappingFailure, NestedJobNode, NestedJobParameterMapping, NestedJobParameterSource,
    NodeId, ParameterCoercion, ParameterName, ParameterRole, ParameterValue, ParameterValueKind,
    SequentialIdGenerator, StepComponents, StepName, StepNode, StopSource, Tasklet, TaskletContext,
    TaskletError, TaskletJob, TaskletOutcome, TaskletStep, TerminalKind,
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
    let clock = Arc::new(FixedClock(SystemTime::UNIX_EPOCH + Duration::from_secs(10)));
    let ids = Arc::new(SequentialIdGenerator::new(NonZeroU64::MIN));
    let repository = InMemoryJobRepository::new(clock.clone(), ids.clone());
    (clock, ids, repository)
}

fn parameter(value: ParameterValue, role: ParameterRole) -> JobParameter {
    JobParameter::new(value, role)
}

fn parent_parameters(run: &str, payload: Option<&str>) -> Result<JobParameters, Box<dyn Error>> {
    let mut parameters = JobParameters::new();
    parameters.insert(
        ParameterName::new("run")?,
        parameter(ParameterValue::string(run)?, ParameterRole::Identifying),
    )?;
    if let Some(payload) = payload {
        parameters.insert(
            ParameterName::new("payload")?,
            parameter(
                ParameterValue::string(payload)?,
                ParameterRole::NonIdentifying,
            ),
        )?;
    }
    Ok(parameters)
}

struct CaptureChild {
    calls: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<String>>>,
    parameter: ParameterName,
}

impl Tasklet for CaptureChild {
    fn execute<'a>(
        &'a self,
        context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let value = context
                .parameters()
                .get(&self.parameter)
                .and_then(|parameter| parameter.value().as_str())
                .ok_or_else(TaskletError::new)?;
            self.seen
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(value.to_owned());
            Ok(TaskletOutcome::Completed)
        })
    }
}

struct FailOnce {
    calls: Arc<AtomicUsize>,
}

impl Tasklet for FailOnce {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(TaskletError::new())
            } else {
                Ok(TaskletOutcome::Completed)
            }
        })
    }
}

fn child_job(
    calls: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<String>>>,
) -> Result<Arc<TaskletJob>, Box<dyn Error>> {
    let step = TaskletStep::new(
        StepName::new("child-step")?,
        Arc::new(CaptureChild {
            calls,
            seen,
            parameter: ParameterName::new("child-payload")?,
        }),
    );
    Ok(Arc::new(TaskletJob::new(
        JobName::new("child-job")?,
        step,
        DefinitionRevision::new("child-v1")?,
        &ComponentRevision::new("child-tasklet-v1")?,
    )?))
}

fn payload_mapping(
    missing: MissingParameterPolicy,
) -> Result<NestedJobParameterMapping, Box<dyn Error>> {
    Ok(NestedJobParameterMapping::new(
        ParameterName::new("child-payload")?,
        ParameterRole::Identifying,
        NestedJobParameterSource::ParentParameter(ParameterName::new("payload")?),
        ParameterValueKind::String,
        ParameterCoercion::Exact,
        missing,
    ))
}

fn parent_job(
    child: Arc<TaskletJob>,
    after_calls: Arc<AtomicUsize>,
    missing: MissingParameterPolicy,
) -> Result<(FlowJob, NodeId), Box<dyn Error>> {
    let nested = NodeId::new("child-job-node")?;
    let after = NodeId::new("after-child")?;
    let nested_node = NestedJobNode::new(
        nested.clone(),
        child.definition_identity().clone(),
        ComponentRevision::new("mapping-v1")?,
        vec![payload_mapping(missing)?],
    )?;
    let plan = FlowGraph::new(nested.clone())
        .with_node(FlowNode::nested_job(nested_node))
        .with_node(FlowNode::step(StepNode::new(
            after.clone(),
            StepName::new("after-child")?,
            StepComponents::Tasklet(ComponentRevision::new("after-v1")?),
        )))
        .with_sequence(nested.clone(), FlowTarget::Node(after.clone()))?
        .with_sequence(after.clone(), FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(
            &JobName::new("parent-job")?,
            DefinitionRevision::new("parent-v1")?,
        )?;
    let job = FlowJob::new(JobName::new("parent-job")?, plan)?
        .with_nested_tasklet_job(nested.clone(), child)?
        .with_tasklet_step(
            after,
            TaskletStep::new(
                StepName::new("after-child")?,
                Arc::new(FailOnce { calls: after_calls }),
            ),
        )?;
    Ok((job, nested))
}

#[tokio::test(flavor = "current_thread")]
async fn restart_reuses_completed_child_and_does_not_remap_new_parent_value()
-> Result<(), Box<dyn Error>> {
    let child_calls = Arc::new(AtomicUsize::new(0));
    let after_calls = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let child = child_job(child_calls.clone(), seen.clone())?;
    let (parent, nested) = parent_job(child, after_calls.clone(), MissingParameterPolicy::Fail)?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());

    let first = parent_parameters("same-instance", Some("first"))?;
    let first_report = launcher.launch(&parent, &first, &stop).await?;
    assert!(matches!(
        first_report.outcome(),
        FlowExecutionOutcome::Failed(_)
    ));
    assert_eq!(child_calls.load(Ordering::SeqCst), 1);
    assert_eq!(after_calls.load(Ordering::SeqCst), 1);

    let first_link = {
        let mut unit = repository.begin().await?;
        let link = unit
            .nested_job_link(first_report.job_execution().id(), &nested)
            .await?
            .expect("first link committed before child work");
        unit.rollback().await?;
        link
    };
    assert_eq!(
        first_link
            .terminal()
            .map(oxide_batch::NestedJobTerminalObservation::status),
        Some(BatchStatus::Completed)
    );

    let second = parent_parameters("same-instance", Some("second"))?;
    let second_report = launcher.launch(&parent, &second, &stop).await?;
    assert_eq!(second_report.outcome(), &FlowExecutionOutcome::Completed);
    assert_eq!(child_calls.load(Ordering::SeqCst), 1);
    assert_eq!(after_calls.load(Ordering::SeqCst), 2);

    let (second_link, persisted) = {
        let mut unit = repository.begin().await?;
        let link = unit
            .nested_job_link(second_report.job_execution().id(), &nested)
            .await?
            .expect("restart link exists");
        let parameters = unit
            .nested_job_parameters(second_report.job_execution().id(), &nested)
            .await?;
        unit.rollback().await?;
        (link, parameters)
    };
    assert_eq!(
        first_link.child_job_instance_id(),
        second_link.child_job_instance_id()
    );
    assert_eq!(
        first_link.child_job_execution_id(),
        second_link.child_job_execution_id()
    );
    assert_eq!(
        persisted
            .get(&ParameterName::new("child-payload")?)
            .and_then(|parameter| parameter.value().as_str()),
        Some("first")
    );
    assert_eq!(
        seen.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_slice(),
        ["first"]
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn missing_required_mapping_fails_before_link_or_child_work() -> Result<(), Box<dyn Error>> {
    let child_calls = Arc::new(AtomicUsize::new(0));
    let after_calls = Arc::new(AtomicUsize::new(0));
    let child = child_job(child_calls.clone(), Arc::new(Mutex::new(Vec::new())))?;
    let (parent, nested) = parent_job(child, after_calls, MissingParameterPolicy::Fail)?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());
    let parameters = parent_parameters("missing", None)?;

    let report = launcher.launch(&parent, &parameters, &stop).await?;
    assert!(matches!(
        report.outcome(),
        FlowExecutionOutcome::Failed(FlowFailure::NestedJobMapping {
            failure: NestedJobMappingFailure::MissingSource,
            ..
        })
    ));
    assert_eq!(child_calls.load(Ordering::SeqCst), 0);

    let mut unit = repository.begin().await?;
    assert!(
        unit.nested_job_link(report.job_execution().id(), &nested)
            .await?
            .is_none()
    );
    unit.rollback().await?;
    Ok(())
}

struct TypedChild {
    calls: Arc<AtomicUsize>,
}

impl Tasklet for TypedChild {
    fn execute<'a>(
        &'a self,
        context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            let number = context
                .parameters()
                .get(&ParameterName::new("number").map_err(TaskletError::from_error)?)
                .and_then(|parameter| parameter.value().as_u64());
            let defaulted = context
                .parameters()
                .get(&ParameterName::new("defaulted").map_err(TaskletError::from_error)?)
                .and_then(|parameter| parameter.value().as_bool());
            if number != Some(42) || defaulted != Some(false) {
                return Err(TaskletError::new());
            }
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(TaskletOutcome::Completed)
        })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn explicit_coercion_and_type_default_are_applied_before_link_commit()
-> Result<(), Box<dyn Error>> {
    let child_calls = Arc::new(AtomicUsize::new(0));
    let child_step = TaskletStep::new(
        StepName::new("typed-child-step")?,
        Arc::new(TypedChild {
            calls: child_calls.clone(),
        }),
    );
    let child = Arc::new(TaskletJob::new(
        JobName::new("typed-child-job")?,
        child_step,
        DefinitionRevision::new("v1")?,
        &ComponentRevision::new("typed-child-v1")?,
    )?);
    let nested = NodeId::new("typed-child-node")?;
    let nested_node = NestedJobNode::new(
        nested.clone(),
        child.definition_identity().clone(),
        ComponentRevision::new("typed-mapping-v1")?,
        vec![
            NestedJobParameterMapping::new(
                ParameterName::new("number")?,
                ParameterRole::Identifying,
                NestedJobParameterSource::ParentParameter(ParameterName::new("payload")?),
                ParameterValueKind::U64,
                ParameterCoercion::StringToU64,
                MissingParameterPolicy::Fail,
            ),
            NestedJobParameterMapping::new(
                ParameterName::new("defaulted")?,
                ParameterRole::NonIdentifying,
                NestedJobParameterSource::ParentParameter(ParameterName::new("optional")?),
                ParameterValueKind::Bool,
                ParameterCoercion::Exact,
                MissingParameterPolicy::TypeDefault,
            ),
        ],
    )?;
    let plan = FlowGraph::new(nested.clone())
        .with_node(FlowNode::nested_job(nested_node))
        .with_sequence(nested.clone(), FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(
            &JobName::new("typed-parent")?,
            DefinitionRevision::new("v1")?,
        )?;
    let parent = FlowJob::new(JobName::new("typed-parent")?, plan)?
        .with_nested_tasklet_job(nested.clone(), child)?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());
    let parameters = parent_parameters("typed", Some("42"))?;

    let report = launcher.launch(&parent, &parameters, &stop).await?;
    assert_eq!(report.outcome(), &FlowExecutionOutcome::Completed);
    assert_eq!(child_calls.load(Ordering::SeqCst), 1);

    let mut unit = repository.begin().await?;
    let persisted = unit
        .nested_job_parameters(report.job_execution().id(), &nested)
        .await?;
    unit.rollback().await?;
    assert_eq!(
        persisted
            .get(&ParameterName::new("number")?)
            .and_then(|parameter| parameter.value().as_u64()),
        Some(42)
    );
    assert_eq!(
        persisted
            .get(&ParameterName::new("defaulted")?)
            .and_then(|parameter| parameter.value().as_bool()),
        Some(false)
    );
    Ok(())
}

#[derive(Clone, Copy)]
enum ChildTerminalMode {
    Failed,
    Stopped,
    Unknown,
}

struct TerminalChild {
    calls: Arc<AtomicUsize>,
    mode: ChildTerminalMode,
}

impl Tasklet for TerminalChild {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self.mode {
                ChildTerminalMode::Failed => Err(TaskletError::new()),
                ChildTerminalMode::Stopped => Ok(TaskletOutcome::Stopped),
                ChildTerminalMode::Unknown => Ok(TaskletOutcome::CommitOutcomeUnknown),
            }
        })
    }
}

fn terminal_child_job(
    name: &str,
    calls: Arc<AtomicUsize>,
    mode: ChildTerminalMode,
) -> Result<Arc<TaskletJob>, Box<dyn Error>> {
    Ok(Arc::new(TaskletJob::new(
        JobName::new(name)?,
        TaskletStep::new(
            StepName::new(format!("{name}-step"))?,
            Arc::new(TerminalChild { calls, mode }),
        ),
        DefinitionRevision::new("v1")?,
        &ComponentRevision::new(format!("{name}-tasklet-v1"))?,
    )?))
}

fn single_nested_parent(
    parent_name: &str,
    child: Arc<TaskletJob>,
) -> Result<(FlowJob, NodeId), Box<dyn Error>> {
    let nested = NodeId::new("nested-child")?;
    let node = NestedJobNode::new(
        nested.clone(),
        child.definition_identity().clone(),
        ComponentRevision::new("mapping-v1")?,
        vec![payload_mapping(MissingParameterPolicy::Fail)?],
    )?;
    let plan = FlowGraph::new(nested.clone())
        .with_node(FlowNode::nested_job(node))
        .with_sequence(nested.clone(), FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(&JobName::new(parent_name)?, DefinitionRevision::new("v1")?)?;
    Ok((
        FlowJob::new(JobName::new(parent_name)?, plan)?
            .with_nested_tasklet_job(nested.clone(), child)?,
        nested,
    ))
}

#[tokio::test(flavor = "current_thread")]
async fn failed_child_fails_parent_node_and_commits_terminal_observation()
-> Result<(), Box<dyn Error>> {
    let calls = Arc::new(AtomicUsize::new(0));
    let child = terminal_child_job("failed-child", calls.clone(), ChildTerminalMode::Failed)?;
    let (parent, nested) = single_nested_parent("failed-parent", child)?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());
    let parameters = parent_parameters("failed", Some("payload"))?;

    let report = launcher.launch(&parent, &parameters, &stop).await?;
    assert!(matches!(
        report.outcome(),
        FlowExecutionOutcome::Failed(FlowFailure::NestedJobChildFailed { .. })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let mut unit = repository.begin().await?;
    let link = unit
        .nested_job_link(report.job_execution().id(), &nested)
        .await?
        .expect("failed child remains durably linked");
    unit.rollback().await?;
    assert_eq!(
        link.terminal()
            .map(oxide_batch::NestedJobTerminalObservation::status),
        Some(BatchStatus::Failed)
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn stopped_child_stops_parent_and_commits_terminal_observation() -> Result<(), Box<dyn Error>>
{
    let calls = Arc::new(AtomicUsize::new(0));
    let child = terminal_child_job("stopped-child", calls.clone(), ChildTerminalMode::Stopped)?;
    let (parent, nested) = single_nested_parent("stopped-parent", child)?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());
    let parameters = parent_parameters("stopped", Some("payload"))?;

    let report = launcher.launch(&parent, &parameters, &stop).await?;
    assert_eq!(report.outcome(), &FlowExecutionOutcome::Stopped);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let mut unit = repository.begin().await?;
    let link = unit
        .nested_job_link(report.job_execution().id(), &nested)
        .await?
        .expect("stopped child remains durably linked");
    unit.rollback().await?;
    assert_eq!(
        link.terminal()
            .map(oxide_batch::NestedJobTerminalObservation::status),
        Some(BatchStatus::Stopped)
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn unknown_child_leaves_parent_unknown_without_fabricated_terminal()
-> Result<(), Box<dyn Error>> {
    let calls = Arc::new(AtomicUsize::new(0));
    let child = terminal_child_job("unknown-child", calls.clone(), ChildTerminalMode::Unknown)?;
    let (parent, nested) = single_nested_parent("unknown-parent", child)?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());
    let parameters = parent_parameters("unknown", Some("payload"))?;

    let report = launcher.launch(&parent, &parameters, &stop).await?;
    assert_eq!(report.outcome(), &FlowExecutionOutcome::Unknown);
    assert_eq!(
        report.job_execution().metadata().status(),
        BatchStatus::Unknown
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let mut unit = repository.begin().await?;
    let link = unit
        .nested_job_link(report.job_execution().id(), &nested)
        .await?
        .expect("unknown child linkage is retained for recovery");
    let child_execution = unit
        .get_job_execution(link.child_job_execution_id())
        .await?
        .expect("linked child execution exists");
    unit.rollback().await?;
    assert!(link.terminal().is_none());
    assert_eq!(child_execution.metadata().status(), BatchStatus::Unknown);
    Ok(())
}

struct StopOwnedChild {
    entered: Arc<AtomicBool>,
    exited: Arc<AtomicBool>,
}

impl Tasklet for StopOwnedChild {
    fn execute<'a>(
        &'a self,
        context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            self.entered.store(true, Ordering::Release);
            context.stop_token().cancelled().await;
            self.exited.store(true, Ordering::Release);
            Ok(TaskletOutcome::Stopped)
        })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn parent_stop_is_owned_joined_and_propagated_to_child() -> Result<(), Box<dyn Error>> {
    let entered = Arc::new(AtomicBool::new(false));
    let exited = Arc::new(AtomicBool::new(false));
    let child = Arc::new(TaskletJob::new(
        JobName::new("cancel-child")?,
        TaskletStep::new(
            StepName::new("cancel-child-step")?,
            Arc::new(StopOwnedChild {
                entered: entered.clone(),
                exited: exited.clone(),
            }),
        ),
        DefinitionRevision::new("v1")?,
        &ComponentRevision::new("cancel-child-v1")?,
    )?);
    let (parent, nested) = single_nested_parent("cancel-parent", child)?;
    let (clock, ids, repository) = infrastructure();
    let (source, stop) = StopSource::new();
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());
    let parameters = parent_parameters("cancel", Some("payload"))?;

    let launch = launcher.launch(&parent, &parameters, &stop);
    let request = async {
        while !entered.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        source.request_stop();
    };
    let (report, ()) = tokio::join!(launch, request);
    let report = report?;

    assert_eq!(report.outcome(), &FlowExecutionOutcome::Stopped);
    assert!(entered.load(Ordering::Acquire));
    assert!(exited.load(Ordering::Acquire));
    let mut unit = repository.begin().await?;
    let link = unit
        .nested_job_link(report.job_execution().id(), &nested)
        .await?
        .expect("cancelled child remains durably linked");
    unit.rollback().await?;
    assert_eq!(
        link.terminal()
            .map(oxide_batch::NestedJobTerminalObservation::status),
        Some(BatchStatus::Stopped)
    );
    Ok(())
}
