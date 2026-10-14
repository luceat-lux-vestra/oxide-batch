//! Adversarial evidence for custom-leaf state publication across non-success outcomes.

use std::error::Error;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

use oxide_batch::{
    BatchStatus, BoxFuture, Clock, ComponentRevision, CustomLeafContext, CustomLeafHandler,
    CustomLeafKind, CustomLeafNode, CustomLeafRegistration, CustomLeafResult,
    DefinitionNodeUpgrade, DefinitionRevision, DefinitionStateKind, DefinitionStateTransform,
    DefinitionStateTransformError, DefinitionStateTransformId, DefinitionStateTransformer,
    DefinitionUpgrade, DefinitionUpgradeExecution, DefinitionUpgradeKey, DefinitionUpgradeRevision,
    ExecutionContext, FlowExecutionOutcome, FlowGraph, FlowJob, FlowLauncher, FlowNode, FlowTarget,
    InMemoryJobRepository, JobName, JobParameters, JobRepository, SequentialIdGenerator,
    StateLimits, StateSchemaId, StateSchemaVersion, StepName, StopSource, TaskletError,
    TaskletOutcome, TerminalKind,
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

fn state(value: u64) -> Result<ExecutionContext, Box<dyn Error>> {
    let bytes = format!(
        r#"{{"format":"oxide-batch.execution-context","format_version":1,"schema":"example.state","schema_version":1,"payload":{{"value":{value}}}}}"#
    );
    Ok(ExecutionContext::from_json(
        bytes.as_bytes(),
        StateLimits::default(),
    )?)
}

fn state_value(state: &ExecutionContext) -> Option<u64> {
    serde_json::from_slice::<serde_json::Value>(&state.payload_json().ok()?)
        .ok()?
        .get("value")?
        .as_u64()
}

#[derive(Clone, Copy)]
enum Mode {
    Stopped,
    Unknown,
}

struct CheckpointHandler {
    mode: Mode,
    value: u64,
}

impl CustomLeafHandler for CheckpointHandler {
    fn execute<'a>(
        &'a self,
        _context: CustomLeafContext<'a>,
    ) -> BoxFuture<'a, Result<CustomLeafResult, TaskletError>> {
        Box::pin(async move {
            let outcome = match self.mode {
                Mode::Stopped => TaskletOutcome::Stopped,
                Mode::Unknown => TaskletOutcome::CommitOutcomeUnknown,
            };
            let checkpoint = state(self.value).map_err(|error| {
                TaskletError::from_error(std::io::Error::other(error.to_string()))
            })?;
            Ok(CustomLeafResult::new(outcome).with_state(checkpoint))
        })
    }
}

fn job(
    job_name: &str,
    handler: Arc<dyn CustomLeafHandler>,
) -> Result<(FlowJob, oxide_batch::NodeId), Box<dyn Error>> {
    let name = JobName::new(job_name)?;
    let id = oxide_batch::NodeId::new("custom")?;
    let leaf = CustomLeafNode::new(
        id.clone(),
        StepName::new("custom-step")?,
        CustomLeafKind::new("example.handler")?,
        ComponentRevision::new("handler-v1")?,
        StateSchemaId::new("example.state")?,
        StateSchemaVersion::new(1)?,
    );
    let plan = FlowGraph::new(id.clone())
        .with_node(FlowNode::custom_leaf(leaf))
        .with_sequence(id.clone(), FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(&name, DefinitionRevision::new("v1")?)?;
    let registration = CustomLeafRegistration::new(
        CustomLeafKind::new("example.handler")?,
        ComponentRevision::new("handler-v1")?,
        StateSchemaId::new("example.state")?,
        StateSchemaVersion::new(1)?,
        handler,
    );
    Ok((
        FlowJob::new(name, plan)?.with_custom_leaf_registration(id.clone(), registration)?,
        id,
    ))
}

async fn assert_checkpoint(
    mode: Mode,
    job_name: &str,
    expected_outcome: FlowExecutionOutcome,
    expected_status: BatchStatus,
) -> Result<(), Box<dyn Error>> {
    let (job, id) = job(job_name, Arc::new(CheckpointHandler { mode, value: 7 }))?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();
    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;

    assert_eq!(report.outcome(), &expected_outcome);

    let mut unit = repository.begin().await?;
    let durable = unit
        .latest_flow_step(report.instance().id(), &id)
        .await?
        .ok_or("custom-leaf checkpoint was not durable")?;
    assert_eq!(durable.execution().metadata().status(), expected_status);
    assert_eq!(
        durable.context().and_then(state_value),
        Some(7),
        "a valid checkpoint returned with a non-success outcome must remain restart-visible"
    );
    unit.rollback().await?;
    Ok(())
}


fn state_with_version(value: u64, version: u32) -> Result<ExecutionContext, Box<dyn Error>> {
    let bytes = format!(
        r#"{{"format":"oxide-batch.execution-context","format_version":1,"schema":"example.state","schema_version":{version},"payload":{{"value":{value}}}}}"#
    );
    Ok(ExecutionContext::from_json(
        bytes.as_bytes(),
        StateLimits::default(),
    )?)
}

fn increment_state_payload(payload: &[u8]) -> Result<Vec<u8>, DefinitionStateTransformError> {
    let mut value: serde_json::Value = serde_json::from_slice(payload)
        .map_err(|_| DefinitionStateTransformError::InvalidSource)?;
    let current = value
        .get("value")
        .and_then(serde_json::Value::as_u64)
        .ok_or(DefinitionStateTransformError::InvalidSource)?;
    value["value"] = serde_json::json!(current.saturating_add(1));
    serde_json::to_vec(&value).map_err(|_| DefinitionStateTransformError::TransformFailed)
}

struct VersionedStateHandler {
    mode: Mode,
    value: u64,
    version: u32,
}

impl CustomLeafHandler for VersionedStateHandler {
    fn execute<'a>(
        &'a self,
        _context: CustomLeafContext<'a>,
    ) -> BoxFuture<'a, Result<CustomLeafResult, TaskletError>> {
        Box::pin(async move {
            let outcome = match self.mode {
                Mode::Stopped => TaskletOutcome::Stopped,
                Mode::Unknown => TaskletOutcome::CommitOutcomeUnknown,
            };
            let state =
                state_with_version(self.value, self.version).map_err(|_| TaskletError::new())?;
            Ok(CustomLeafResult::new(outcome).with_state(state))
        })
    }
}

struct ObservePreviousState {
    observed: Arc<AtomicUsize>,
}

impl CustomLeafHandler for ObservePreviousState {
    fn execute<'a>(
        &'a self,
        context: CustomLeafContext<'a>,
    ) -> BoxFuture<'a, Result<CustomLeafResult, TaskletError>> {
        Box::pin(async move {
            let previous = context.previous_state().ok_or_else(TaskletError::new)?;
            if previous.schema_id().as_str() != "example.state"
                || previous.schema_version().get() != 2
            {
                return Err(TaskletError::new());
            }
            let value = state_value(previous).ok_or_else(TaskletError::new)?;
            self.observed.store(
                usize::try_from(value).map_err(|_| TaskletError::new())?,
                Ordering::SeqCst,
            );
            Ok(CustomLeafResult::new(TaskletOutcome::Completed))
        })
    }
}

fn versioned_job(
    name: &JobName,
    revision: &str,
    handler_revision: &str,
    schema_version: u32,
    handler: Arc<dyn CustomLeafHandler>,
) -> Result<(FlowJob, oxide_batch::NodeId), Box<dyn Error>> {
    let id = oxide_batch::NodeId::new("custom")?;
    let schema = StateSchemaId::new("example.state")?;
    let version = StateSchemaVersion::new(schema_version)?;
    let leaf = CustomLeafNode::new(
        id.clone(),
        StepName::new("custom-step")?,
        CustomLeafKind::new("example.handler")?,
        ComponentRevision::new(handler_revision)?,
        schema.clone(),
        version,
    );
    let plan = FlowGraph::new(id.clone())
        .with_node(FlowNode::custom_leaf(leaf))
        .with_sequence(id.clone(), FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(name, DefinitionRevision::new(revision)?)?;
    let registration = CustomLeafRegistration::new(
        CustomLeafKind::new("example.handler")?,
        ComponentRevision::new(handler_revision)?,
        schema,
        version,
        handler,
    );
    Ok((
        FlowJob::new(name.clone(), plan)?
            .with_custom_leaf_registration(id.clone(), registration)?,
        id,
    ))
}

#[tokio::test(flavor = "current_thread")]
async fn compatible_flow_transforms_custom_leaf_state_before_user_work()
-> Result<(), Box<dyn Error>> {
    let name = JobName::new("custom-leaf-compatible-upgrade")?;
    let (source, node) = versioned_job(
        &name,
        "v1",
        "handler-v1",
        1,
        Arc::new(VersionedStateHandler {
            mode: Mode::Stopped,
            value: 7,
            version: 1,
        }),
    )?;
    let observed = Arc::new(AtomicUsize::new(0));
    let (target, target_node) = versioned_job(
        &name,
        "v2",
        "handler-v2",
        2,
        Arc::new(ObservePreviousState {
            observed: Arc::clone(&observed),
        }),
    )?;

    let (clock, ids, repository) = infrastructure();
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());
    let (_, first_stop) = StopSource::new();
    let first = launcher
        .launch(&source, &JobParameters::new(), &first_stop)
        .await?;
    assert_eq!(first.outcome(), &FlowExecutionOutcome::Stopped);

    let transform_revision = DefinitionUpgradeRevision::new("state-v1-to-v2")?;
    let edge = DefinitionUpgrade::transformed(
        DefinitionUpgradeKey::new("custom-v1-to-v2")?,
        DefinitionUpgradeRevision::new("edge-v1")?,
        source.compiled_plan().definition_identity().clone(),
        target.compiled_plan().definition_identity().clone(),
        [DefinitionNodeUpgrade::new(node.clone(), target_node)],
        [DefinitionStateTransform::new(
            node.clone(),
            node,
            DefinitionStateKind::ExecutionContext,
            DefinitionStateTransformId::new("custom-state")?,
            StateSchemaId::new("example.state")?,
            StateSchemaVersion::new(1)?,
            StateSchemaId::new("example.state")?,
            StateSchemaVersion::new(2)?,
            transform_revision.clone(),
        )],
    )?;
    let selected = DefinitionUpgradeExecution::new(
        edge.clone(),
        [DefinitionStateTransformer::new(
            transform_revision,
            increment_state_payload,
        )],
    )?;
    let mut register = repository.begin().await?;
    register.register_definition_upgrade(&name, &edge).await?;
    register.commit().await?;

    let (_, second_stop) = StopSource::new();
    let second = launcher
        .launch_compatible(&target, &selected, &JobParameters::new(), &second_stop)
        .await?;
    assert_eq!(second.outcome(), &FlowExecutionOutcome::Completed);
    assert_eq!(observed.load(Ordering::SeqCst), 8);
    Ok(())
}

#[tokio::test]
async fn stopped_result_can_publish_restart_checkpoint() -> Result<(), Box<dyn Error>> {
    assert_checkpoint(
        Mode::Stopped,
        "custom-leaf-stopped-checkpoint",
        FlowExecutionOutcome::Stopped,
        BatchStatus::Stopped,
    )
    .await
}

#[tokio::test]
async fn unknown_result_can_publish_recovery_checkpoint() -> Result<(), Box<dyn Error>> {
    assert_checkpoint(
        Mode::Unknown,
        "custom-leaf-unknown-checkpoint",
        FlowExecutionOutcome::Unknown,
        BatchStatus::Unknown,
    )
    .await
}
