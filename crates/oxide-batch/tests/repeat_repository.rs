//! M7 #300 durable repeat repository contract evidence.

#![allow(clippy::expect_used, clippy::panic)]

use std::error::Error;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::SystemTime;

use oxide_batch::{
    ComponentRevision, DefinitionRevision, DefinitionUpgrade, DefinitionUpgradeKey,
    ExecutionContext, FailureCategory, FailureId, FailureSummary, FlowGraph, FlowNode, FlowTarget,
    InMemoryJobRepository, JobInstanceKey, JobName, JobParameters, JobRepository,
    LifecycleTransition, NodeId, RepeatCommitRequest, RepeatDecision, RepeatDefinition, RepeatId,
    RepeatLineage, RepeatOrdinal, RepeatPolicyConfiguration, RepeatPolicyDefinition,
    RepeatPolicyKind, RepeatStateSchema, RepositoryError, SequentialIdGenerator, StartLimit,
    StateLimits, StateSchemaId, StateSchemaVersion, StepComponents, StepDefinitionUpgrade,
    StepName, StepNode, SystemClock, TerminalKind,
};

fn plan() -> Result<oxide_batch::CompiledExecutionPlan, Box<dyn Error>> {
    let node = NodeId::new("repeat-step")?;
    let repeat = RepeatDefinition::new(
        RepeatId::new("window")?,
        RepeatPolicyDefinition::new(
            RepeatPolicyKind::new("bounded-count")?,
            ComponentRevision::new("policy-v1")?,
            RepeatPolicyConfiguration::new("limit-3")?,
        ),
        Vec::new(),
        RepeatStateSchema::new(
            StateSchemaId::new("repeat.state")?,
            StateSchemaVersion::new(1)?,
        ),
    )?;
    Ok(FlowGraph::new(node.clone())
        .with_node(FlowNode::step(
            StepNode::new(
                node.clone(),
                StepName::new("repeat-step")?,
                StepComponents::Tasklet(ComponentRevision::new("tasklet-v1")?),
            )
            .with_repeat_definition(repeat),
        ))
        .with_sequence(node, FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(
            &JobName::new("repeat-repository")?,
            DefinitionRevision::new("v1")?,
        )?)
}

fn upgraded_plan() -> Result<oxide_batch::CompiledExecutionPlan, Box<dyn Error>> {
    let node = NodeId::new("repeat-step")?;
    let repeat = RepeatDefinition::new(
        RepeatId::new("window")?,
        RepeatPolicyDefinition::new(
            RepeatPolicyKind::new("bounded-count")?,
            ComponentRevision::new("policy-v2")?,
            RepeatPolicyConfiguration::new("limit-3")?,
        ),
        Vec::new(),
        RepeatStateSchema::new(
            StateSchemaId::new("repeat.state")?,
            StateSchemaVersion::new(1)?,
        ),
    )?;
    Ok(FlowGraph::new(node.clone())
        .with_node(FlowNode::step(
            StepNode::new(
                node.clone(),
                StepName::new("repeat-step")?,
                StepComponents::Tasklet(ComponentRevision::new("tasklet-v1")?),
            )
            .with_repeat_definition(repeat),
        ))
        .with_sequence(node, FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(
            &JobName::new("repeat-repository")?,
            DefinitionRevision::new("v2")?,
        )?)
}

fn state(value: &str) -> Result<ExecutionContext, Box<dyn Error>> {
    let bytes = serde_json::to_vec(&serde_json::json!({
        "format": "oxide-batch.execution-context",
        "format_version": 1,
        "schema": "repeat.state",
        "schema_version": 1,
        "payload": { "cursor": value }
    }))?;
    Ok(ExecutionContext::from_json(&bytes, StateLimits::default())?)
}

fn repository() -> InMemoryJobRepository {
    InMemoryJobRepository::new(
        Arc::new(SystemClock),
        Arc::new(SequentialIdGenerator::new(NonZeroU64::MIN)),
    )
}

async fn create_owner(
    repository: &InMemoryJobRepository,
    plan: &oxide_batch::CompiledExecutionPlan,
) -> Result<(oxide_batch::JobInstanceId, oxide_batch::StepExecutionId), Box<dyn Error>> {
    let key = JobInstanceKey::new(JobName::new("repeat-repository")?, &JobParameters::new());
    let mut unit = repository.begin().await?;
    let instance = unit
        .select_or_create_job_instance(&key)
        .await?
        .instance()
        .clone();
    let job = unit
        .create_job_execution_with_definition(instance.id(), plan.definition_identity())
        .await?;
    let step = unit
        .create_flow_step_execution(
            job.id(),
            &StepName::new("repeat-step")?,
            &NodeId::new("repeat-step")?,
            StartLimit::UNRESTRICTED,
        )
        .await?;
    unit.commit().await?;
    Ok((instance.id(), step.id()))
}

fn request(
    plan: &oxide_batch::CompiledExecutionPlan,
    instance: oxide_batch::JobInstanceId,
    step: oxide_batch::StepExecutionId,
    ordinal: u32,
    state: ExecutionContext,
    decision: RepeatDecision,
) -> Result<RepeatCommitRequest, Box<dyn Error>> {
    Ok(RepeatCommitRequest::new(
        instance,
        NodeId::new("repeat-step")?,
        step,
        RepeatId::new("window")?,
        RepeatOrdinal::new(ordinal),
        state,
        decision,
        *plan.fingerprint(),
    ))
}

#[tokio::test]
async fn repeat_state_advances_only_on_commit_and_restarts_from_durable_authority()
-> Result<(), Box<dyn Error>> {
    let repository = repository();
    let plan = plan()?;
    let (instance, first_step) = create_owner(&repository, &plan).await?;

    let first = request(
        &plan,
        instance,
        first_step,
        0,
        state("sensitive-first")?,
        RepeatDecision::Continue,
    )?;
    let mut unit = repository.begin().await?;
    let committed = unit.commit_repeat_iteration(&first).await?;
    assert_eq!(committed.ordinal(), RepeatOrdinal::INITIAL);
    assert_eq!(committed.decision(), RepeatDecision::Continue);
    assert!(
        !format!("{committed:?}").contains("sensitive-first"),
        "repeat state diagnostics must redact execution-context payloads",
    );
    unit.commit().await?;

    // Exact replay is idempotent, while a conflicting replay at the same
    // ordinal is rejected rather than rewriting durable meaning.
    let mut replay = repository.begin().await?;
    assert_eq!(replay.commit_repeat_iteration(&first).await?, committed);
    let conflicting = request(
        &plan,
        instance,
        first_step,
        0,
        state("different")?,
        RepeatDecision::Continue,
    )?;
    assert_eq!(
        replay.commit_repeat_iteration(&conflicting).await,
        Err(RepositoryError::RepeatStateCorrupt)
    );
    replay.rollback().await?;

    // A new step attempt sees the last committed decision. Preparing ordinal
    // one and then rolling back must leave ordinal zero authoritative.
    let mut setup = repository.begin().await?;
    let job_execution_id = setup
        .get_step_execution(first_step)
        .await?
        .expect("first step exists")
        .job_execution_id();
    let second_step = setup
        .create_flow_step_execution(
            job_execution_id,
            &StepName::new("repeat-step")?,
            &NodeId::new("repeat-step")?,
            StartLimit::UNRESTRICTED,
        )
        .await?;
    setup.commit().await?;

    let mut inspect = repository.begin().await?;
    let inherited = inspect
        .latest_repeat_execution(
            instance,
            &NodeId::new("repeat-step")?,
            &RepeatId::new("window")?,
        )
        .await?
        .expect("restart sees prior committed repeat state");
    assert_eq!(inherited.ordinal(), RepeatOrdinal::INITIAL);
    assert_eq!(inherited.decision(), RepeatDecision::Continue);
    inspect.rollback().await?;

    let second = request(
        &plan,
        instance,
        second_step.id(),
        1,
        state("second")?,
        RepeatDecision::Complete,
    )?;
    let mut rolled_back = repository.begin().await?;
    assert_eq!(
        rolled_back
            .commit_repeat_iteration(&second)
            .await?
            .ordinal(),
        RepeatOrdinal::new(1)
    );
    rolled_back.rollback().await?;

    let mut after_rollback = repository.begin().await?;
    assert!(
        after_rollback
            .repeat_execution(second_step.id(), &RepeatId::new("window")?)
            .await?
            .is_none(),
        "rollback must not consume an ordinal",
    );
    assert_eq!(
        after_rollback
            .latest_repeat_execution(
                instance,
                &NodeId::new("repeat-step")?,
                &RepeatId::new("window")?,
            )
            .await?
            .expect("prior record remains")
            .ordinal(),
        RepeatOrdinal::INITIAL,
    );
    after_rollback.rollback().await?;

    let mut accept = repository.begin().await?;
    accept.commit_repeat_iteration(&second).await?;
    accept.commit().await?;

    let third = request(
        &plan,
        instance,
        second_step.id(),
        2,
        state("third")?,
        RepeatDecision::Continue,
    )?;
    let mut terminal = repository.begin().await?;
    assert_eq!(
        terminal.commit_repeat_iteration(&third).await,
        Err(RepositoryError::RepeatAlreadyComplete)
    );
    terminal.rollback().await?;
    Ok(())
}

#[tokio::test]
async fn ambiguous_repeat_commit_is_recovered_only_by_fresh_durable_inspection()
-> Result<(), Box<dyn Error>> {
    let repository = repository();
    let plan = plan()?;
    let (instance, step) = create_owner(&repository, &plan).await?;
    let first = request(
        &plan,
        instance,
        step,
        0,
        state("ambiguous-sentinel")?,
        RepeatDecision::Continue,
    )?;

    repository.inject_next_repeat_commit_unknown();
    let mut unit = repository.begin().await?;
    unit.commit_repeat_iteration(&first).await?;
    assert_eq!(
        unit.commit().await,
        Err(RepositoryError::CommitOutcomeUnknown)
    );

    let mut recovery = repository.begin().await?;
    let durable = recovery
        .repeat_execution(step, &RepeatId::new("window")?)
        .await?
        .expect("published state survives the lost commit response");
    assert_eq!(durable.ordinal(), RepeatOrdinal::INITIAL);
    assert_eq!(durable.decision(), RepeatDecision::Continue);
    // The same command is safe to replay after inspection and must not advance.
    assert_eq!(recovery.commit_repeat_iteration(&first).await?, durable);
    recovery.commit().await?;
    Ok(())
}

#[tokio::test]
async fn repeat_ordinal_rejects_skips_before_any_state_is_written() -> Result<(), Box<dyn Error>> {
    let repository = repository();
    let plan = plan()?;
    let (instance, step) = create_owner(&repository, &plan).await?;
    let skipped = request(
        &plan,
        instance,
        step,
        1,
        state("skip")?,
        RepeatDecision::Continue,
    )?;
    let mut unit = repository.begin().await?;
    assert_eq!(
        unit.commit_repeat_iteration(&skipped).await,
        Err(RepositoryError::RepeatOrdinalConflict {
            expected: 0,
            actual: 1,
        })
    );
    assert!(
        unit.repeat_execution(step, &RepeatId::new("window")?)
            .await?
            .is_none()
    );
    unit.rollback().await?;
    Ok(())
}

#[tokio::test]
async fn repeat_lineage_rejects_prior_state_from_an_upgraded_definition()
-> Result<(), Box<dyn Error>> {
    let repository = repository();
    let v1 = plan()?;
    let v2 = upgraded_plan()?;
    let (instance, first_step) = create_owner(&repository, &v1).await?;

    let first = request(
        &v1,
        instance,
        first_step,
        0,
        state("v1-state")?,
        RepeatDecision::Continue,
    )?;
    let mut commit = repository.begin().await?;
    commit.commit_repeat_iteration(&first).await?;
    commit.commit().await?;

    let mut inspect = repository.begin().await?;
    let first_step_execution = inspect
        .get_step_execution(first_step)
        .await?
        .expect("first step exists");
    let first_job = inspect
        .get_job_execution(first_step_execution.job_execution_id())
        .await?
        .expect("first job exists");
    inspect.rollback().await?;

    let mut fail = repository.begin().await?;
    fail.transition_job_execution(
        first_job.id(),
        first_job.version(),
        LifecycleTransition::failed(
            SystemTime::now(),
            FailureSummary::new(FailureCategory::UserComponent, FailureId::new(700)?),
        ),
    )
    .await?;
    fail.commit().await?;

    let upgrade = DefinitionUpgrade::new(
        DefinitionUpgradeKey::new("repeat-v1-to-v2")?,
        v1.definition_identity().clone(),
        v2.definition_identity().clone(),
        [StepDefinitionUpgrade::new(
            StepName::new("repeat-step")?,
            StepName::new("repeat-step")?,
        )],
    )?;
    let mut register = repository.begin().await?;
    register
        .register_definition_upgrade(&JobName::new("repeat-repository")?, &upgrade)
        .await?;
    register.commit().await?;

    let mut create = repository.begin().await?;
    let second_job = create
        .create_job_execution_with_definition(instance, v2.definition_identity())
        .await?;
    let second_step = create
        .create_flow_step_execution(
            second_job.id(),
            &StepName::new("repeat-step")?,
            &NodeId::new("repeat-step")?,
            StartLimit::UNRESTRICTED,
        )
        .await?;
    create.commit().await?;

    let next = request(
        &v2,
        instance,
        second_step.id(),
        1,
        state("v2-state")?,
        RepeatDecision::Continue,
    )?;
    let mut rejected = repository.begin().await?;
    assert_eq!(
        rejected.commit_repeat_iteration(&next).await,
        Err(RepositoryError::RepeatStateCorrupt),
        "a directed definition upgrade does not authorize carrying repeat lineage state across a different plan fingerprint",
    );
    rejected.rollback().await?;
    Ok(())
}

#[tokio::test]
async fn nested_repeat_state_is_scoped_to_exact_parent_lineage() -> Result<(), Box<dyn Error>> {
    let repository = repository();
    let node = NodeId::new("repeat-step")?;
    let inner = RepeatDefinition::new(
        RepeatId::new("inner")?,
        RepeatPolicyDefinition::new(
            RepeatPolicyKind::new("bounded-count")?,
            ComponentRevision::new("inner-policy-v1")?,
            RepeatPolicyConfiguration::new("limit-1")?,
        ),
        Vec::new(),
        RepeatStateSchema::new(
            StateSchemaId::new("repeat.state")?,
            StateSchemaVersion::new(1)?,
        ),
    )?;
    let outer = RepeatDefinition::new(
        RepeatId::new("outer")?,
        RepeatPolicyDefinition::new(
            RepeatPolicyKind::new("bounded-count")?,
            ComponentRevision::new("outer-policy-v1")?,
            RepeatPolicyConfiguration::new("limit-2")?,
        ),
        Vec::new(),
        RepeatStateSchema::new(
            StateSchemaId::new("repeat.state")?,
            StateSchemaVersion::new(1)?,
        ),
    )?
    .with_nested(inner)?;
    let nested_plan = FlowGraph::new(node.clone())
        .with_node(FlowNode::step(
            StepNode::new(
                node.clone(),
                StepName::new("repeat-step")?,
                StepComponents::Tasklet(ComponentRevision::new("tasklet-v1")?),
            )
            .with_repeat_definition(outer),
        ))
        .with_sequence(node.clone(), FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(
            &JobName::new("repeat-repository")?,
            DefinitionRevision::new("nested-v1")?,
        )?;
    let (instance, step) = create_owner(&repository, &nested_plan).await?;

    let lineage0 = RepeatLineage::root()
        .child(RepeatId::new("outer")?, RepeatOrdinal::new(0))
        .ok_or_else(|| std::io::Error::other("first child lineage exceeds bound"))?;
    let first = RepeatCommitRequest::new(
        instance,
        node.clone(),
        step,
        RepeatId::new("inner")?,
        RepeatOrdinal::INITIAL,
        state("child-0")?,
        RepeatDecision::Complete,
        *nested_plan.fingerprint(),
    )
    .with_lineage(lineage0.clone());
    let mut unit = repository.begin().await?;
    let committed0 = unit.commit_repeat_iteration(&first).await?;
    assert_eq!(committed0.lineage(), &lineage0);
    unit.commit().await?;

    let lineage1 = RepeatLineage::root()
        .child(RepeatId::new("outer")?, RepeatOrdinal::new(1))
        .ok_or_else(|| std::io::Error::other("second child lineage exceeds bound"))?;
    let second = RepeatCommitRequest::new(
        instance,
        node.clone(),
        step,
        RepeatId::new("inner")?,
        RepeatOrdinal::INITIAL,
        state("child-1")?,
        RepeatDecision::Complete,
        *nested_plan.fingerprint(),
    )
    .with_lineage(lineage1.clone());
    let mut replace = repository.begin().await?;
    let committed1 = replace.commit_repeat_iteration(&second).await?;
    assert_eq!(committed1.ordinal(), RepeatOrdinal::INITIAL);
    assert_eq!(committed1.lineage(), &lineage1);
    replace.commit().await?;

    let mut terminal = repository.begin().await?;
    let invalid_next = RepeatCommitRequest::new(
        instance,
        node,
        step,
        RepeatId::new("inner")?,
        RepeatOrdinal::new(1),
        state("must-not-advance")?,
        RepeatDecision::Continue,
        *nested_plan.fingerprint(),
    )
    .with_lineage(lineage1);
    assert_eq!(
        terminal.commit_repeat_iteration(&invalid_next).await,
        Err(RepositoryError::RepeatAlreadyComplete)
    );
    terminal.rollback().await?;
    Ok(())
}

#[tokio::test]
async fn repeat_lineage_and_definition_owner_mismatch_fail_closed() -> Result<(), Box<dyn Error>> {
    let repository = repository();
    let plan = plan()?;
    let (instance, step) = create_owner(&repository, &plan).await?;

    let wrong_parent = RepeatLineage::root()
        .child(RepeatId::new("not-a-parent")?, RepeatOrdinal::INITIAL)
        .ok_or_else(|| std::io::Error::other("test lineage exceeds bound"))?;
    let wrong_lineage = request(
        &plan,
        instance,
        step,
        0,
        state("wrong-lineage")?,
        RepeatDecision::Continue,
    )?
    .with_lineage(wrong_parent);
    let mut unit = repository.begin().await?;
    assert_eq!(
        unit.commit_repeat_iteration(&wrong_lineage).await,
        Err(RepositoryError::RepeatStateCorrupt)
    );
    unit.rollback().await?;

    let wrong_definition = request(
        &plan,
        instance,
        step,
        0,
        state("wrong-owner")?,
        RepeatDecision::Continue,
    )?
    .with_definition_node_id(NodeId::new("worker-definition")?);
    let mut owner = repository.begin().await?;
    assert_eq!(
        owner.commit_repeat_iteration(&wrong_definition).await,
        Err(RepositoryError::RepeatStateCorrupt),
        "a dynamic execution owner cannot claim a different definition without a durable partition assignment",
    );
    owner.rollback().await?;
    Ok(())
}

#[test]
fn repeat_ordinal_overflow_fails_closed() {
    assert_eq!(RepeatOrdinal::new(u32::MAX).checked_next(), None);
}
