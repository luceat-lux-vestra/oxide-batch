//! Custom-leaf definition identity and manifest tests.

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
) -> oxide_batch_plan::CompiledExecutionPlan {
    let id = NodeId::new("custom").expect("node");
    let leaf = CustomLeafNode::new(
        id.clone(),
        StepName::new("custom").expect("step"),
        CustomLeafKind::new(kind).expect("kind"),
        ComponentRevision::new(revision).expect("revision"),
        StateSchemaId::new(schema).expect("schema"),
        StateSchemaVersion::new(schema_version).expect("schema version"),
    );
    FlowGraph::new(id.clone())
        .with_node(FlowNode::custom_leaf(leaf))
        .with_sequence(id, FlowTarget::Terminal(TerminalKind::Complete))
        .expect("sequence")
        .compile(
            &JobName::new("custom_job").expect("job"),
            DefinitionRevision::new("r1").expect("definition revision"),
        )
        .expect("compile")
}

#[test]
fn custom_leaf_forces_format_four() {
    assert_eq!(
        compiled("example", "handler-v1", "state", 1).manifest_format(),
        MANIFEST_FORMAT_ADVANCED_FLOW
    );
}

#[test]
fn handler_revision_changes_definition_fingerprint() {
    assert_ne!(
        compiled("example", "handler-v1", "state", 1).fingerprint(),
        compiled("example", "handler-v2", "state", 1).fingerprint(),
    );
}

#[test]
fn state_schema_identity_changes_definition_fingerprint() {
    assert_ne!(
        compiled("example", "handler-v1", "state", 1).fingerprint(),
        compiled("example", "handler-v1", "state", 2).fingerprint(),
    );
    assert_ne!(
        compiled("example", "handler-v1", "state-a", 1).fingerprint(),
        compiled("example", "handler-v1", "state-b", 1).fingerprint(),
    );
}

#[test]
fn custom_kind_changes_definition_fingerprint() {
    assert_ne!(
        compiled("example-a", "handler-v1", "state", 1).fingerprint(),
        compiled("example-b", "handler-v1", "state", 1).fingerprint(),
    );
}
