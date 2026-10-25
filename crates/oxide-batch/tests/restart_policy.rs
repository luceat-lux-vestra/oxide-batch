//! M7/#606: explicit job/step non-restartable definition policy and restart guards.

use std::error::Error;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use oxide_batch::{
    BoxFuture, ComponentRevision, DefinitionManifest, DefinitionRevision, ExitPattern,
    FlowExecutionOutcome, FlowFailure, FlowGraph, FlowJob, FlowLauncher, FlowNode,
    FlowRuntimeError, FlowTarget, FlowTransition,
    InMemoryJobRepository, JobName, JobParameters, JobRepository, ManifestError, NestedFlow,
    NodeId, RepositoryError, RestartPolicy, SequentialIdGenerator, StartControls, StartLimit,
    StepComponents, StepName, StepNode, StopSource, SystemClock, Tasklet, TaskletContext,
    TaskletError, TaskletOutcome, TaskletStep, TerminalKind,
};

struct CountingTasklet(Arc<AtomicUsize>);

impl Tasklet for CountingTasklet {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(TaskletOutcome::Completed)
        })
    }
}

fn step_node(
    id: &NodeId,
    policy: RestartPolicy,
    allow_start_if_complete: bool,
) -> Result<StepNode, Box<dyn Error>> {
    step_node_with_limit(
        id,
        policy,
        allow_start_if_complete,
        StartLimit::UNRESTRICTED,
    )
}

fn step_node_with_limit(
    id: &NodeId,
    policy: RestartPolicy,
    allow_start_if_complete: bool,
    start_limit: StartLimit,
) -> Result<StepNode, Box<dyn Error>> {
    Ok(StepNode::new(
        id.clone(),
        StepName::new(id.as_str())?,
        StepComponents::Tasklet(ComponentRevision::new("tasklet-v1")?),
    )
    .with_start_controls(
        StartControls::new(start_limit, allow_start_if_complete).with_restart_policy(policy),
    ))
}

fn failing_plan(
    name: &JobName,
    job_policy: RestartPolicy,
    step_policy: RestartPolicy,
    allow_start_if_complete: bool,
) -> Result<oxide_batch::CompiledExecutionPlan, Box<dyn Error>> {
    let only = NodeId::new("only")?;
    Ok(FlowGraph::new(only.clone())
        .with_job_restart_policy(job_policy)
        .with_node(FlowNode::step(step_node(
            &only,
            step_policy,
            allow_start_if_complete,
        )?))
        .with_transition(FlowTransition::new(
            only,
            ExitPattern::new("*")?,
            FlowTarget::Terminal(TerminalKind::Fail),
        ))
        .compile(name, DefinitionRevision::new("v1")?)?)
}

fn legacy_failing_plan(
    name: &JobName,
) -> Result<oxide_batch::CompiledExecutionPlan, Box<dyn Error>> {
    let only = NodeId::new("only")?;
    Ok(FlowGraph::new(only.clone())
        .with_node(FlowNode::step(StepNode::new(
            only.clone(),
            StepName::new("only")?,
            StepComponents::Tasklet(ComponentRevision::new("tasklet-v1")?),
        )))
        .with_transition(FlowTransition::new(
            only,
            ExitPattern::new("*")?,
            FlowTarget::Terminal(TerminalKind::Fail),
        ))
        .compile(name, DefinitionRevision::new("v1")?)?)
}

fn flow_job(
    name: &JobName,
    job_policy: RestartPolicy,
    step_policy: RestartPolicy,
    allow_start_if_complete: bool,
    calls: Arc<AtomicUsize>,
) -> Result<FlowJob, Box<dyn Error>> {
    Ok(FlowJob::new(
        name.clone(),
        failing_plan(name, job_policy, step_policy, allow_start_if_complete)?,
    )?
    .with_tasklet_step(
        NodeId::new("only")?,
        TaskletStep::new(StepName::new("only")?, Arc::new(CountingTasklet(calls))),
    )?)
}

#[test]
fn restart_policy_is_format4_only_and_default_preserves_legacy_identity()
-> Result<(), Box<dyn Error>> {
    let name = JobName::new("restart-policy-fingerprint")?;
    let legacy = legacy_failing_plan(&name)?;
    let explicit_default = failing_plan(
        &name,
        RestartPolicy::Restartable,
        RestartPolicy::Restartable,
        false,
    )?;
    assert_eq!(legacy.manifest_format(), 2);
    assert_eq!(explicit_default.manifest_format(), 2);
    assert_eq!(legacy.fingerprint(), explicit_default.fingerprint());

    let job_nonrestartable = failing_plan(
        &name,
        RestartPolicy::NotRestartable,
        RestartPolicy::Restartable,
        false,
    )?;
    assert_eq!(job_nonrestartable.manifest_format(), 4);
    assert_eq!(
        job_nonrestartable.job_restart_policy(),
        RestartPolicy::NotRestartable
    );
    assert_ne!(legacy.fingerprint(), job_nonrestartable.fingerprint());

    let step_nonrestartable = failing_plan(
        &name,
        RestartPolicy::Restartable,
        RestartPolicy::NotRestartable,
        false,
    )?;
    assert_eq!(step_nonrestartable.manifest_format(), 4);
    assert_ne!(legacy.fingerprint(), step_nonrestartable.fingerprint());

    let job_manifest: serde_json::Value = serde_json::from_slice(
        job_nonrestartable
            .definition_identity()
            .canonical_manifest(),
    )?;
    assert_eq!(
        job_manifest
            .get("restart_policy")
            .and_then(serde_json::Value::as_str),
        Some("not_restartable")
    );
    let step_manifest: serde_json::Value = serde_json::from_slice(
        step_nonrestartable
            .definition_identity()
            .canonical_manifest(),
    )?;
    assert_eq!(
        step_manifest
            .get("nodes")
            .and_then(serde_json::Value::as_array)
            .and_then(|nodes| nodes.first())
            .and_then(|node| node.get("start"))
            .and_then(|start| start.get("restart_policy"))
            .and_then(serde_json::Value::as_str),
        Some("not_restartable")
    );
    Ok(())
}

#[test]
fn restart_policy_manifest_validation_fails_closed() -> Result<(), Box<dyn Error>> {
    let name = JobName::new("restart-policy-manifest")?;
    let legacy = legacy_failing_plan(&name)?;
    let mut legacy_document: serde_json::Value =
        serde_json::from_slice(legacy.definition_identity().canonical_manifest())?;
    legacy_document
        .as_object_mut()
        .ok_or("legacy manifest is not an object")?
        .insert(
            "restart_policy".to_owned(),
            serde_json::Value::String("not_restartable".to_owned()),
        );
    assert!(matches!(
        DefinitionManifest::read(&serde_json::to_vec(&legacy_document)?),
        Err(ManifestError::MalformedGraph)
    ));

    let format4 = failing_plan(
        &name,
        RestartPolicy::NotRestartable,
        RestartPolicy::Restartable,
        false,
    )?;
    let mut format4_document: serde_json::Value =
        serde_json::from_slice(format4.definition_identity().canonical_manifest())?;
    format4_document
        .as_object_mut()
        .ok_or("format-4 manifest is not an object")?
        .insert(
            "restart_policy".to_owned(),
            serde_json::Value::String("restartable".to_owned()),
        );
    assert!(matches!(
        DefinitionManifest::read(&serde_json::to_vec(&format4_document)?),
        Err(ManifestError::MalformedGraph)
    ));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn m7_nonrestartable_job_rejects_restart_before_user_work() -> Result<(), Box<dyn Error>> {
    let name = JobName::new("m7-nonrestartable-job")?;
    let calls = Arc::new(AtomicUsize::new(0));
    let job = flow_job(
        &name,
        RestartPolicy::NotRestartable,
        RestartPolicy::Restartable,
        false,
        Arc::clone(&calls),
    )?;
    let clock = Arc::new(SystemClock);
    let ids = Arc::new(SequentialIdGenerator::new(NonZeroU64::MIN));
    let repository = InMemoryJobRepository::new(clock.clone(), ids.clone());
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());
    let parameters = JobParameters::new();
    let (_, stop) = StopSource::new();

    let first = launcher.launch(&job, &parameters, &stop).await?;
    let instance_id = first.instance().id();
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    assert!(matches!(
        launcher.launch(&job, &parameters, &stop).await,
        Err(FlowRuntimeError::NonRestartableJob { instance_id: rejected })
            if rejected == instance_id
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let mut inspection = repository.begin().await?;
    assert_eq!(inspection.job_executions(instance_id).await?.len(), 1);
    inspection.rollback().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn completed_instance_guard_remains_authoritative_for_nonrestartable_job()
-> Result<(), Box<dyn Error>> {
    let name = JobName::new("m7-nonrestartable-completed-job")?;
    let only = NodeId::new("only")?;
    let plan = FlowGraph::new(only.clone())
        .with_job_restart_policy(RestartPolicy::NotRestartable)
        .with_node(FlowNode::step(step_node(
            &only,
            RestartPolicy::Restartable,
            false,
        )?))
        .with_sequence(only.clone(), FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(&name, DefinitionRevision::new("v1")?)?;
    let calls = Arc::new(AtomicUsize::new(0));
    let job = FlowJob::new(name.clone(), plan)?.with_tasklet_step(
        only,
        TaskletStep::new(
            StepName::new("only")?,
            Arc::new(CountingTasklet(Arc::clone(&calls))),
        ),
    )?;
    let clock = Arc::new(SystemClock);
    let ids = Arc::new(SequentialIdGenerator::new(NonZeroU64::MIN));
    let repository = InMemoryJobRepository::new(clock.clone(), ids.clone());
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());
    let parameters = JobParameters::new();
    let (_, stop) = StopSource::new();

    let first = launcher.launch(&job, &parameters, &stop).await?;
    let instance_id = first.instance().id();
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    assert!(matches!(
        launcher.launch(&job, &parameters, &stop).await,
        Err(FlowRuntimeError::Repository(
            RepositoryError::CompletedInstance { id }
        )) if id == instance_id
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn nonrestartable_step_rejects_required_reentry_before_new_attempt_or_user_work()
-> Result<(), Box<dyn Error>> {
    let name = JobName::new("m7-nonrestartable-step")?;
    let calls = Arc::new(AtomicUsize::new(0));
    let job = flow_job(
        &name,
        RestartPolicy::Restartable,
        RestartPolicy::NotRestartable,
        true,
        Arc::clone(&calls),
    )?;
    let clock = Arc::new(SystemClock);
    let ids = Arc::new(SequentialIdGenerator::new(NonZeroU64::MIN));
    let repository = InMemoryJobRepository::new(clock.clone(), ids.clone());
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());
    let parameters = JobParameters::new();
    let (_, stop) = StopSource::new();

    let first = launcher.launch(&job, &parameters, &stop).await?;
    let instance_id = first.instance().id();
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let only = NodeId::new("only")?;
    assert!(matches!(
        launcher.launch(&job, &parameters, &stop).await,
        Err(FlowRuntimeError::NonRestartableStep {
            instance_id: rejected,
            node_id
        }) if rejected == instance_id && node_id == only
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let mut inspection = repository.begin().await?;
    assert_eq!(inspection.job_executions(instance_id).await?.len(), 1);
    inspection.rollback().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn completed_nonrestartable_step_is_still_reused_when_reentry_is_not_requested()
-> Result<(), Box<dyn Error>> {
    let name = JobName::new("m7-nonrestartable-step-reuse")?;
    let calls = Arc::new(AtomicUsize::new(0));
    let job = flow_job(
        &name,
        RestartPolicy::Restartable,
        RestartPolicy::NotRestartable,
        false,
        Arc::clone(&calls),
    )?;
    let clock = Arc::new(SystemClock);
    let ids = Arc::new(SequentialIdGenerator::new(NonZeroU64::MIN));
    let repository = InMemoryJobRepository::new(clock.clone(), ids.clone());
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());
    let parameters = JobParameters::new();
    let (_, stop) = StopSource::new();

    let first = launcher.launch(&job, &parameters, &stop).await?;
    let second = launcher.launch(&job, &parameters, &stop).await?;
    assert_eq!(first.instance().id(), second.instance().id());
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let mut inspection = repository.begin().await?;
    assert_eq!(
        inspection
            .job_executions(first.instance().id())
            .await?
            .len(),
        2
    );
    inspection.rollback().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn nested_flow_does_not_reset_nonrestartable_step_history() -> Result<(), Box<dyn Error>> {
    let name = JobName::new("m7-nested-nonrestartable-step")?;
    let owner = NodeId::new("nested")?;
    let inner = NodeId::new("inner")?;
    let nested = FlowGraph::new(inner.clone())
        .with_node(FlowNode::step(step_node_with_limit(
            &inner,
            RestartPolicy::NotRestartable,
            true,
            StartLimit::new(1)?,
        )?))
        .with_transition(FlowTransition::new(
            inner.clone(),
            ExitPattern::new("*")?,
            FlowTarget::Terminal(TerminalKind::Fail),
        ));
    let plan = FlowGraph::new(owner.clone())
        .with_nested_flow(NestedFlow::new(owner.clone(), nested))
        .with_transition(FlowTransition::new(
            owner,
            ExitPattern::new("*")?,
            FlowTarget::Terminal(TerminalKind::Fail),
        ))
        .compile(&name, DefinitionRevision::new("v1")?)?;
    let calls = Arc::new(AtomicUsize::new(0));
    let job = FlowJob::new(name.clone(), plan)?.with_tasklet_step(
        inner.clone(),
        TaskletStep::new(
            StepName::new("inner")?,
            Arc::new(CountingTasklet(Arc::clone(&calls))),
        ),
    )?;

    let clock = Arc::new(SystemClock);
    let ids = Arc::new(SequentialIdGenerator::new(NonZeroU64::MIN));
    let repository = InMemoryJobRepository::new(clock.clone(), ids.clone());
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());
    let parameters = JobParameters::new();
    let (_, stop) = StopSource::new();

    let first = launcher.launch(&job, &parameters, &stop).await?;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(matches!(
        launcher.launch(&job, &parameters, &stop).await,
        Err(FlowRuntimeError::NonRestartableStep {
            instance_id,
            node_id
        }) if instance_id == first.instance().id() && node_id == inner
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn nested_flow_completed_step_reuse_does_not_consume_start() -> Result<(), Box<dyn Error>> {
    let name = JobName::new("m7-nested-start-control-reuse")?;
    let owner = NodeId::new("nested")?;
    let inner = NodeId::new("inner")?;
    let limit = StartLimit::new(1)?;
    let nested = FlowGraph::new(inner.clone())
        .with_node(FlowNode::step(step_node_with_limit(
            &inner,
            RestartPolicy::Restartable,
            false,
            limit,
        )?))
        .with_transition(FlowTransition::new(
            inner.clone(),
            ExitPattern::new("*")?,
            FlowTarget::Terminal(TerminalKind::Fail),
        ));
    let plan = FlowGraph::new(owner.clone())
        .with_nested_flow(NestedFlow::new(owner.clone(), nested))
        .with_transition(FlowTransition::new(
            owner,
            ExitPattern::new("*")?,
            FlowTarget::Terminal(TerminalKind::Fail),
        ))
        .compile(&name, DefinitionRevision::new("v1")?)?;
    let calls = Arc::new(AtomicUsize::new(0));
    let job = FlowJob::new(name, plan)?.with_tasklet_step(
        inner,
        TaskletStep::new(
            StepName::new("inner")?,
            Arc::new(CountingTasklet(Arc::clone(&calls))),
        ),
    )?;

    let clock = Arc::new(SystemClock);
    let ids = Arc::new(SequentialIdGenerator::new(NonZeroU64::MIN));
    let repository = InMemoryJobRepository::new(clock.clone(), ids.clone());
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());
    let (_, stop) = StopSource::new();

    let first = launcher
        .launch(&job, &JobParameters::new(), &stop)
        .await?;
    let second = launcher
        .launch(&job, &JobParameters::new(), &stop)
        .await?;
    assert!(matches!(first.outcome(), FlowExecutionOutcome::Failed(_)));
    assert!(matches!(second.outcome(), FlowExecutionOutcome::Failed(_)));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn nested_flow_required_reentry_uses_same_start_counter() -> Result<(), Box<dyn Error>> {
    let name = JobName::new("m7-nested-start-control-reentry")?;
    let owner = NodeId::new("nested")?;
    let inner = NodeId::new("inner")?;
    let limit = StartLimit::new(1)?;
    let nested = FlowGraph::new(inner.clone())
        .with_node(FlowNode::step(step_node_with_limit(
            &inner,
            RestartPolicy::Restartable,
            true,
            limit,
        )?))
        .with_transition(FlowTransition::new(
            inner.clone(),
            ExitPattern::new("*")?,
            FlowTarget::Terminal(TerminalKind::Fail),
        ));
    let plan = FlowGraph::new(owner.clone())
        .with_nested_flow(NestedFlow::new(owner.clone(), nested))
        .with_transition(FlowTransition::new(
            owner,
            ExitPattern::new("*")?,
            FlowTarget::Terminal(TerminalKind::Fail),
        ))
        .compile(&name, DefinitionRevision::new("v1")?)?;
    let calls = Arc::new(AtomicUsize::new(0));
    let job = FlowJob::new(name, plan)?.with_tasklet_step(
        inner.clone(),
        TaskletStep::new(
            StepName::new("inner")?,
            Arc::new(CountingTasklet(Arc::clone(&calls))),
        ),
    )?;

    let clock = Arc::new(SystemClock);
    let ids = Arc::new(SequentialIdGenerator::new(NonZeroU64::MIN));
    let repository = InMemoryJobRepository::new(clock.clone(), ids.clone());
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());
    let (_, stop) = StopSource::new();

    let first = launcher
        .launch(&job, &JobParameters::new(), &stop)
        .await?;
    assert!(matches!(first.outcome(), FlowExecutionOutcome::Failed(_)));
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let second = launcher
        .launch(&job, &JobParameters::new(), &stop)
        .await?;
    assert_eq!(
        second.outcome(),
        &FlowExecutionOutcome::Failed(FlowFailure::StartLimitExceeded {
            node: inner,
            limit,
        })
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    Ok(())
}
