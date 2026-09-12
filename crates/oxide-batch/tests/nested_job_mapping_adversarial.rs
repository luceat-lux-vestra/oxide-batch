//! Adversarial nested-job mapping failures required by M7 #265.
//!
//! These cases prove that type/coercion failures happen before durable linkage
//! or child user work and that the value which caused the failure never appears
//! in the typed diagnostic rendered by the parent flow.

#![allow(clippy::expect_used)]

use std::error::Error;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

use oxide_batch::{
    BoxFuture, Clock, ComponentRevision, DefinitionRevision, FlowExecutionOutcome, FlowFailure,
    FlowGraph, FlowJob, FlowLauncher, FlowNode, FlowTarget, InMemoryJobRepository, JobName,
    JobParameter, JobParameters, JobRepository, MissingParameterPolicy, NestedJobMappingFailure,
    NestedJobNode, NestedJobParameterMapping, NestedJobParameterSource, NodeId, ParameterCoercion,
    ParameterName, ParameterRole, ParameterValue, ParameterValueKind, SequentialIdGenerator,
    StopSource, Tasklet, TaskletContext, TaskletError, TaskletJob, TaskletOutcome, TaskletStep,
    TerminalKind,
};

#[derive(Debug)]
struct FixedClock(SystemTime);

impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        self.0
    }
}

struct NeverRun {
    calls: Arc<AtomicUsize>,
}

impl Tasklet for NeverRun {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(TaskletOutcome::Completed)
        })
    }
}

fn child_job(calls: Arc<AtomicUsize>) -> Result<Arc<TaskletJob>, Box<dyn Error>> {
    Ok(Arc::new(TaskletJob::new(
        JobName::new("mapping-adversarial-child")?,
        TaskletStep::new(
            oxide_batch::StepName::new("child-step")?,
            Arc::new(NeverRun { calls }),
        ),
        DefinitionRevision::new("child-v1")?,
        &ComponentRevision::new("child-tasklet-v1")?,
    )?))
}

fn parent_job(
    child: Arc<TaskletJob>,
    expected: ParameterValueKind,
    coercion: ParameterCoercion,
) -> Result<(FlowJob, NodeId), Box<dyn Error>> {
    let node_id = NodeId::new("nested-child")?;
    let mapping = NestedJobParameterMapping::new(
        ParameterName::new("child-value")?,
        ParameterRole::Identifying,
        NestedJobParameterSource::ParentParameter(ParameterName::new("payload")?),
        expected,
        coercion,
        MissingParameterPolicy::Fail,
    );
    let node = NestedJobNode::new(
        node_id.clone(),
        child.definition_identity().clone(),
        ComponentRevision::new("mapping-v1")?,
        vec![mapping],
    )?;
    let plan = FlowGraph::new(node_id.clone())
        .with_node(FlowNode::nested_job(node))
        .with_sequence(
            node_id.clone(),
            FlowTarget::Terminal(TerminalKind::Complete),
        )?
        .compile(
            &JobName::new("mapping-adversarial-parent")?,
            DefinitionRevision::new("parent-v1")?,
        )?;
    Ok((
        FlowJob::new(JobName::new("mapping-adversarial-parent")?, plan)?
            .with_nested_tasklet_job(node_id.clone(), child)?,
        node_id,
    ))
}

fn parameters(payload: ParameterValue) -> Result<JobParameters, Box<dyn Error>> {
    let mut parameters = JobParameters::new();
    parameters.insert(
        ParameterName::new("run")?,
        JobParameter::new(
            ParameterValue::string("one")?,
            ParameterRole::Identifying,
        ),
    )?;
    parameters.insert(
        ParameterName::new("payload")?,
        JobParameter::new(payload, ParameterRole::NonIdentifying),
    )?;
    Ok(parameters)
}

async fn run_failure(
    expected: ParameterValueKind,
    coercion: ParameterCoercion,
    payload: ParameterValue,
) -> Result<(FlowExecutionOutcome, usize, bool), Box<dyn Error>> {
    let calls = Arc::new(AtomicUsize::new(0));
    let child = child_job(calls.clone())?;
    let (parent, node_id) = parent_job(child, expected, coercion)?;
    let clock = Arc::new(FixedClock(
        SystemTime::UNIX_EPOCH + Duration::from_secs(10),
    ));
    let ids = Arc::new(SequentialIdGenerator::new(NonZeroU64::MIN));
    let repository = InMemoryJobRepository::new(clock.clone(), ids.clone());
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());
    let (_, stop) = StopSource::new();
    let report = launcher.launch(&parent, &parameters(payload)?, &stop).await?;

    let has_link = {
        let mut unit = repository.begin().await?;
        let link = unit
            .nested_job_link(report.job_execution().id(), &node_id)
            .await?;
        unit.rollback().await?;
        link.is_some()
    };
    Ok((
        report.outcome().clone(),
        calls.load(Ordering::SeqCst),
        has_link,
    ))
}

#[tokio::test(flavor = "current_thread")]
async fn exact_type_mismatch_fails_before_link_or_child_work() -> Result<(), Box<dyn Error>> {
    let (outcome, calls, has_link) = run_failure(
        ParameterValueKind::String,
        ParameterCoercion::Exact,
        ParameterValue::from(42_u64),
    )
    .await?;

    assert!(matches!(
        outcome,
        FlowExecutionOutcome::Failed(FlowFailure::NestedJobMapping {
            failure: NestedJobMappingFailure::SourceTypeMismatch,
            ..
        })
    ));
    assert_eq!(calls, 0);
    assert!(!has_link);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn coercion_failure_is_value_redacted_and_precedes_link_commit() -> Result<(), Box<dyn Error>> {
    const SENTINEL: &str = "nested-secret-not-a-number-7f3c";
    let (outcome, calls, has_link) = run_failure(
        ParameterValueKind::U64,
        ParameterCoercion::StringToU64,
        ParameterValue::string(SENTINEL)?,
    )
    .await?;

    assert!(matches!(
        outcome,
        FlowExecutionOutcome::Failed(FlowFailure::NestedJobMapping {
            failure: NestedJobMappingFailure::CoercionFailed,
            ..
        })
    ));
    assert_eq!(calls, 0);
    assert!(!has_link);
    let rendered = format!("{outcome:?}");
    assert!(!rendered.contains(SENTINEL));
    assert!(rendered.contains("CoercionFailed"));
    Ok(())
}
