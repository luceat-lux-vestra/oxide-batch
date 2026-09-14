//! Binding and preflight evidence for registered custom leaves.

use std::error::Error;
use std::sync::Arc;

use oxide_batch::{
    BoxFuture, ComponentRevision, CustomLeafContext, CustomLeafHandler, CustomLeafKind,
    CustomLeafNode, CustomLeafRegistration, CustomLeafResult, DefinitionRevision, FlowGraph,
    FlowJob, FlowJobError, FlowNode, FlowTarget, JobName, NodeId, StateSchemaId,
    StateSchemaVersion, StepName, TaskletError, TaskletOutcome, TerminalKind,
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
    let name = JobName::new("custom-leaf-binding")?;
    let id = NodeId::new("custom")?;
    let node = CustomLeafNode::new(
        id.clone(),
        StepName::new("custom-step")?,
        CustomLeafKind::new("example.handler")?,
        ComponentRevision::new("handler-v1")?,
        StateSchemaId::new("example.state")?,
        StateSchemaVersion::new(1)?,
    );
    let compiled = FlowGraph::new(id.clone())
        .with_node(FlowNode::custom_leaf(node))
        .with_sequence(id.clone(), FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(&name, DefinitionRevision::new("v1")?)?;
    Ok((name, id, compiled))
}

fn registration(revision: &str) -> Result<CustomLeafRegistration, Box<dyn Error>> {
    Ok(CustomLeafRegistration::new(
        CustomLeafKind::new("example.handler")?,
        ComponentRevision::new(revision)?,
        StateSchemaId::new("example.state")?,
        StateSchemaVersion::new(1)?,
        Arc::new(CompleteHandler),
    ))
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
fn custom_leaf_revision_mismatch_fails_closed() -> Result<(), Box<dyn Error>> {
    let (name, id, compiled) = plan()?;
    let result = FlowJob::new(name, compiled)?
        .with_custom_leaf_registration(id.clone(), registration("handler-v2")?);
    assert!(matches!(
        result,
        Err(FlowJobError::CustomLeafRegistrationMismatch { node }) if node == id
    ));
    Ok(())
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
