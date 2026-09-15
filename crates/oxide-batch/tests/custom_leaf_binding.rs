//! Binding and preflight evidence for registered custom leaves.

use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use oxide_batch::{
    BackoffPolicy, BoxFuture, ClassifierRevision, ComponentRevision, CustomLeafContext,
    CustomLeafHandler, CustomLeafKind, CustomLeafNode, CustomLeafRegistration, CustomLeafResult,
    DefinitionRevision, FailureCategory, FaultAction, FaultClassifier, FaultPhase, FaultPolicy,
    FaultRule, FlowGraph, FlowJob, FlowJobError, FlowNode, FlowTarget, JobName, NodeId, RetryLimit,
    RetryStateLimit, SkipLimit, StateSchemaId, StateSchemaVersion, StepName, TaskletError,
    TaskletOutcome, TerminalKind,
};

struct CompleteHandler;

impl CustomLeafHandler for CompleteHandler {
    fn execute<'a>(
        &'a self,
        _context: CustomLeafContext<'a>,
    ) -> BoxFuture<'a, Result<CustomLeafResult, TaskletError>> {
        Box::pin(async { Ok(CustomLeafResult::new(TaskletOutcome::Completed)) })
    }
}

fn plan() -> Result<(JobName, NodeId, oxide_batch::CompiledExecutionPlan), Box<dyn Error>> {
    plan_with_node(CustomLeafNode::new(
        NodeId::new("custom")?,
        StepName::new("custom-step")?,
        CustomLeafKind::new("example.handler")?,
        ComponentRevision::new("handler-v1")?,
        StateSchemaId::new("example.state")?,
        StateSchemaVersion::new(1)?,
    ))
}

fn plan_with_node(
    node: CustomLeafNode,
) -> Result<(JobName, NodeId, oxide_batch::CompiledExecutionPlan), Box<dyn Error>> {
    let name = JobName::new("custom-leaf-binding")?;
    let id = node.id().clone();
    let compiled = FlowGraph::new(id.clone())
        .with_node(FlowNode::custom_leaf(node))
        .with_sequence(id.clone(), FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(&name, DefinitionRevision::new("v1")?)?;
    Ok((name, id, compiled))
}

fn registration(revision: &str) -> Result<CustomLeafRegistration, Box<dyn Error>> {
    registration_with("example.handler", revision, "example.state", 1)
}

fn registration_with(
    kind: &str,
    revision: &str,
    schema: &str,
    schema_version: u32,
) -> Result<CustomLeafRegistration, Box<dyn Error>> {
    Ok(CustomLeafRegistration::new(
        CustomLeafKind::new(kind)?,
        ComponentRevision::new(revision)?,
        StateSchemaId::new(schema)?,
        StateSchemaVersion::new(schema_version)?,
        Arc::new(CompleteHandler),
    ))
}

fn retry_policy() -> Result<FaultPolicy, Box<dyn Error>> {
    Ok(FaultPolicy::new(
        FaultClassifier::new(
            ClassifierRevision::new("classifier-v1")?,
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

fn assert_registration_mismatch(
    name: JobName,
    id: NodeId,
    compiled: oxide_batch::CompiledExecutionPlan,
    registration: CustomLeafRegistration,
) -> Result<(), Box<dyn Error>> {
    let result =
        FlowJob::new(name, compiled)?.with_custom_leaf_registration(id.clone(), registration);
    assert!(matches!(
        result,
        Err(FlowJobError::CustomLeafRegistrationMismatch { node }) if node == id
    ));
    Ok(())
}

#[test]
fn exact_custom_leaf_registration_validates() -> Result<(), Box<dyn Error>> {
    let (name, id, compiled) = plan()?;
    FlowJob::new(name, compiled)?
        .with_custom_leaf_registration(id, registration("handler-v1")?)?
        .validate()?;
    Ok(())
}

#[test]
fn duplicate_custom_leaf_registration_fails_closed() -> Result<(), Box<dyn Error>> {
    let (name, id, compiled) = plan()?;
    let job = FlowJob::new(name, compiled)?
        .with_custom_leaf_registration(id.clone(), registration("handler-v1")?)?;
    let result = job.with_custom_leaf_registration(id.clone(), registration("handler-v1")?);
    assert!(matches!(
        result,
        Err(FlowJobError::DuplicateBinding { node }) if node == id
    ));
    Ok(())
}

#[test]
fn custom_leaf_revision_mismatch_fails_closed() -> Result<(), Box<dyn Error>> {
    let (name, id, compiled) = plan()?;
    assert_registration_mismatch(name, id, compiled, registration("handler-v2")?)
}

#[test]
fn custom_leaf_kind_mismatch_fails_closed() -> Result<(), Box<dyn Error>> {
    let (name, id, compiled) = plan()?;
    assert_registration_mismatch(
        name,
        id,
        compiled,
        registration_with("other.handler", "handler-v1", "example.state", 1)?,
    )
}

#[test]
fn custom_leaf_state_schema_id_mismatch_fails_closed() -> Result<(), Box<dyn Error>> {
    let (name, id, compiled) = plan()?;
    assert_registration_mismatch(
        name,
        id,
        compiled,
        registration_with("example.handler", "handler-v1", "other.state", 1)?,
    )
}

#[test]
fn custom_leaf_state_schema_version_mismatch_fails_closed() -> Result<(), Box<dyn Error>> {
    let (name, id, compiled) = plan()?;
    assert_registration_mismatch(
        name,
        id,
        compiled,
        registration_with("example.handler", "handler-v1", "example.state", 2)?,
    )
}

#[test]
fn custom_leaf_listener_presence_mismatch_fails_closed() -> Result<(), Box<dyn Error>> {
    let node = CustomLeafNode::new(
        NodeId::new("custom")?,
        StepName::new("custom-step")?,
        CustomLeafKind::new("example.handler")?,
        ComponentRevision::new("handler-v1")?,
        StateSchemaId::new("example.state")?,
        StateSchemaVersion::new(1)?,
    )
    .with_listener_revision(ComponentRevision::new("listener-v1")?);
    let (name, id, compiled) = plan_with_node(node)?;
    assert_registration_mismatch(name, id, compiled, registration("handler-v1")?)
}

#[test]
fn custom_leaf_fault_runtime_presence_mismatch_fails_closed() -> Result<(), Box<dyn Error>> {
    let node = CustomLeafNode::new(
        NodeId::new("custom")?,
        StepName::new("custom-step")?,
        CustomLeafKind::new("example.handler")?,
        ComponentRevision::new("handler-v1")?,
        StateSchemaId::new("example.state")?,
        StateSchemaVersion::new(1)?,
    )
    .with_fault_policy(retry_policy()?);
    let (name, id, compiled) = plan_with_node(node)?;
    assert_registration_mismatch(name, id, compiled, registration("handler-v1")?)
}

#[test]
fn missing_custom_leaf_registration_fails_validation() -> Result<(), Box<dyn Error>> {
    let (name, id, compiled) = plan()?;
    assert_eq!(
        FlowJob::new(name, compiled)?.validate(),
        Err(FlowJobError::MissingBinding { node: id })
    );
    Ok(())
}

#[test]
fn framework_empty_context_schema_is_reserved_for_custom_leaves() -> Result<(), Box<dyn Error>> {
    let name = JobName::new("custom-leaf-reserved-schema")?;
    let id = NodeId::new("custom")?;
    let reserved = StateSchemaId::new("oxide_batch.empty.v1")?;
    let node = CustomLeafNode::new(
        id.clone(),
        StepName::new("custom-step")?,
        CustomLeafKind::new("example.handler")?,
        ComponentRevision::new("handler-v1")?,
        reserved.clone(),
        StateSchemaVersion::new(1)?,
    );
    let compiled = FlowGraph::new(id.clone())
        .with_node(FlowNode::custom_leaf(node))
        .with_sequence(id.clone(), FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(&name, DefinitionRevision::new("v1")?)?;
    let registration = CustomLeafRegistration::new(
        CustomLeafKind::new("example.handler")?,
        ComponentRevision::new("handler-v1")?,
        reserved,
        StateSchemaVersion::new(1)?,
        Arc::new(CompleteHandler),
    );
    let result =
        FlowJob::new(name, compiled)?.with_custom_leaf_registration(id.clone(), registration);
    assert!(matches!(
        result,
        Err(FlowJobError::CustomLeafRegistrationMismatch { node }) if node == id
    ));
    Ok(())
}
