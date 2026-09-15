//! Custom-leaf definition identity and manifest tests.

use std::error::Error;

use oxide_batch_core::{
    ComponentRevision, DefinitionRevision, FlowTarget, JobName, MANIFEST_FORMAT_ADVANCED_FLOW,
    NodeId, StateSchemaId, StateSchemaVersion, StepName, TerminalKind,
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
    Ok(FlowGraph::new(id.clone())
        .with_node(FlowNode::custom_leaf(leaf))
        .with_sequence(id, FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(&JobName::new("custom_job")?, DefinitionRevision::new("r1")?)?)
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
