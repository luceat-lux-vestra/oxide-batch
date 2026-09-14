from pathlib import Path

ci = Path(".github/workflows/ci.yml")
text = ci.read_text()
anchor = """          cargo test -p oxide-batch --features postgres \\
            --test postgres_nested_job_crash_recovery \\
            -- --nocapture --test-threads=1
"""
addition = anchor + """          cargo test -p oxide-batch --features postgres \\
            --test postgres_custom_leaf_crash_recovery \\
            -- --nocapture --test-threads=1
"""
if text.count(anchor) != 1:
    raise SystemExit(f"expected one nested-job crash anchor, found {text.count(anchor)}")
ci.write_text(text.replace(anchor, addition, 1))

crash = Path("crates/oxide-batch/tests/postgres_custom_leaf_crash_recovery.rs")
text = crash.read_text()
old = 'state(1).map_err(|error| std::io::Error::other(error.to_string()))?'
new = 'state(1).map_err(|error| TaskletError::from_error(std::io::Error::other(error.to_string())))?'
if text.count(old) != 1:
    raise SystemExit(f"expected one state(1) conversion, found {text.count(old)}")
text = text.replace(old, new, 1)
old = 'state(2).map_err(|error| std::io::Error::other(error.to_string()))?'
new = 'state(2).map_err(|error| TaskletError::from_error(std::io::Error::other(error.to_string())))?'
if text.count(old) != 1:
    raise SystemExit(f"expected one state(2) conversion, found {text.count(old)}")
crash.write_text(text.replace(old, new, 1))

flow = Path("crates/oxide-batch/src/flow.rs")
text = flow.read_text()
signature = """fn custom_leaf_registration_matches(
    compiled: &crate::CustomLeafNode,
    registration: &crate::CustomLeafRegistration,
) -> bool {
    compiled.kind() == registration.kind()
"""
replacement = """const RESERVED_CUSTOM_LEAF_STATE_SCHEMA: &str = "oxide_batch.empty.v1";

fn custom_leaf_registration_matches(
    compiled: &crate::CustomLeafNode,
    registration: &crate::CustomLeafRegistration,
) -> bool {
    compiled.state_schema_id().as_str() != RESERVED_CUSTOM_LEAF_STATE_SCHEMA
        && compiled.kind() == registration.kind()
"""
if text.count(signature) != 1:
    raise SystemExit(f"expected custom-leaf registration helper once, found {text.count(signature)}")
text = text.replace(signature, replacement, 1)
unreachable_backoff = "                                _ => return Ok(Err(failure)),\n"
if text.count(unreachable_backoff) != 1:
    raise SystemExit(f"expected one unreachable backoff arm, found {text.count(unreachable_backoff)}")
text = text.replace(unreachable_backoff, "", 1)
unreachable_outcome = """                        _ => (
                            TaskletExecutionOutcome::Failed(TaskletFailure::Error),
                            ExitStatus::failed(),
                            Some(TaskletFailure::Error),
                        ),
"""
if text.count(unreachable_outcome) != 1:
    raise SystemExit(f"expected one unreachable tasklet arm, found {text.count(unreachable_outcome)}")
flow.write_text(text.replace(unreachable_outcome, "", 1))

binding = Path("crates/oxide-batch/tests/custom_leaf_binding.rs")
text = binding.read_text()
test = r'''

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
    let result = FlowJob::new(name, compiled)?
        .with_custom_leaf_registration(id.clone(), registration);
    assert!(matches!(
        result,
        Err(FlowJobError::CustomLeafRegistrationMismatch { node }) if node == id
    ));
    Ok(())
}
'''
marker = "fn framework_empty_context_schema_is_reserved_for_custom_leaves()"
if marker not in text:
    binding.write_text(text + test)

plan = Path("crates/oxide-batch-plan/tests/custom_leaf.rs")
plan.write_text(r'''//! Custom-leaf definition identity and manifest tests.

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
        .compile(
            &JobName::new("custom_job")?,
            DefinitionRevision::new("r1")?,
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
''')
