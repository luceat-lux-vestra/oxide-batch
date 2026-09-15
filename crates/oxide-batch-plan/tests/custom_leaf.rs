//! Custom-leaf definition identity and manifest tests.

use std::error::Error;
use std::time::Duration;

use oxide_batch_core::{
    BackoffPolicy, ClassifierRevision, ComponentRevision, DefinitionRevision, FailureCategory,
    FaultAction, FaultClassifier, FaultPhase, FaultPolicy, FaultRule, FlowTarget, JobName,
    MANIFEST_FORMAT_ADVANCED_FLOW, NodeId, RetryLimit, RetryStateLimit, SkipLimit, StartControls,
    StartLimit, StateSchemaId, StateSchemaVersion, StepName, TerminalKind,
};
use oxide_batch_plan::{CustomLeafKind, CustomLeafNode, FlowGraph, FlowNode};

fn compiled(
    kind: &str,
    revision: &str,
    schema: &str,
    schema_version: u32,
) -> Result<oxide_batch_plan::CompiledExecutionPlan, Box<dyn Error>> {
    let id = NodeId::new("custom")?;
    let leaf = CustomLeafNode::new(
        id.clone(),
        StepName::new("custom")?,
        CustomLeafKind::new(kind)?,
        ComponentRevision::new(revision)?,
        StateSchemaId::new(schema)?,
        StateSchemaVersion::new(schema_version)?,
    );
    compiled_leaf(leaf)
}

fn leaf() -> Result<CustomLeafNode, Box<dyn Error>> {
    Ok(CustomLeafNode::new(
        NodeId::new("custom")?,
        StepName::new("custom")?,
        CustomLeafKind::new("example")?,
        ComponentRevision::new("handler-v1")?,
        StateSchemaId::new("state")?,
        StateSchemaVersion::new(1)?,
    ))
}

fn compiled_leaf(
    leaf: CustomLeafNode,
) -> Result<oxide_batch_plan::CompiledExecutionPlan, Box<dyn Error>> {
    let id = leaf.id().clone();
    Ok(FlowGraph::new(id.clone())
        .with_node(FlowNode::custom_leaf(leaf))
        .with_sequence(id, FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(&JobName::new("custom_job")?, DefinitionRevision::new("r1")?)?)
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

#[test]
fn custom_leaf_forces_format_four() -> Result<(), Box<dyn Error>> {
    assert_eq!(
        compiled("example", "handler-v1", "state", 1)?.manifest_format(),
        MANIFEST_FORMAT_ADVANCED_FLOW
    );
    Ok(())
}

#[test]
fn handler_revision_changes_definition_fingerprint() -> Result<(), Box<dyn Error>> {
    assert_ne!(
        compiled("example", "handler-v1", "state", 1)?.fingerprint(),
        compiled("example", "handler-v2", "state", 1)?.fingerprint(),
    );
    Ok(())
}

#[test]
fn state_schema_identity_changes_definition_fingerprint() -> Result<(), Box<dyn Error>> {
    assert_ne!(
        compiled("example", "handler-v1", "state", 1)?.fingerprint(),
        compiled("example", "handler-v1", "state", 2)?.fingerprint(),
    );
    assert_ne!(
        compiled("example", "handler-v1", "state-a", 1)?.fingerprint(),
        compiled("example", "handler-v1", "state-b", 1)?.fingerprint(),
    );
    Ok(())
}

#[test]
fn custom_kind_changes_definition_fingerprint() -> Result<(), Box<dyn Error>> {
    assert_ne!(
        compiled("example-a", "handler-v1", "state", 1)?.fingerprint(),
        compiled("example-b", "handler-v1", "state", 1)?.fingerprint(),
    );
    Ok(())
}

#[test]
fn start_controls_change_definition_fingerprint() -> Result<(), Box<dyn Error>> {
    let baseline = compiled_leaf(leaf()?)?;
    let changed =
        compiled_leaf(leaf()?.with_start_controls(StartControls::new(StartLimit::new(2)?, true)))?;
    assert_ne!(baseline.fingerprint(), changed.fingerprint());
    Ok(())
}

#[test]
fn fault_policy_changes_definition_fingerprint() -> Result<(), Box<dyn Error>> {
    let baseline = compiled_leaf(leaf()?)?;
    let changed = compiled_leaf(leaf()?.with_fault_policy(retry_policy()?))?;
    assert_ne!(baseline.fingerprint(), changed.fingerprint());
    Ok(())
}

#[test]
fn listener_order_changes_definition_fingerprint() -> Result<(), Box<dyn Error>> {
    let first = ComponentRevision::new("listener-a")?;
    let second = ComponentRevision::new("listener-b")?;
    let forward = compiled_leaf(
        leaf()?
            .with_listener_revision(first.clone())
            .with_listener_revision(second.clone()),
    )?;
    let reversed = compiled_leaf(
        leaf()?
            .with_listener_revision(second)
            .with_listener_revision(first),
    )?;
    assert_ne!(forward.fingerprint(), reversed.fingerprint());
    Ok(())
}
