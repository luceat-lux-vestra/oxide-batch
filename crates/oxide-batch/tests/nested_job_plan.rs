//! M7 #265 nested-job format-4 plan and fingerprint evidence.

#![allow(clippy::expect_used, clippy::panic)]

use std::error::Error;

use oxide_batch::{
    ComponentRevision, DefinitionManifest, DefinitionRevision, FlowGraph, FlowNode, FlowTarget,
    FrameworkParameterSource, JobName, MissingParameterPolicy, NestedJobNode,
    NestedJobParameterMapping, NestedJobParameterSource, NodeId, ParameterCoercion, ParameterName,
    ParameterRole, ParameterValueKind, PlanError, SelectorPath, StepComponents, StepName, StepNode,
    TerminalKind, MAX_NESTED_JOB_PARAMETERS, MAX_SELECTOR_PATH_BYTES,
};

fn token(value: &str) -> Result<ComponentRevision, Box<dyn Error>> {
    Ok(ComponentRevision::new(value)?)
}

fn child_plan() -> Result<oxide_batch::CompiledExecutionPlan, Box<dyn Error>> {
    let id = NodeId::new("child-step")?;
    let graph = FlowGraph::new(id.clone())
        .with_node(FlowNode::step(StepNode::new(
            id.clone(),
            StepName::new("child-step")?,
            StepComponents::Tasklet(token("child-tasklet-v1")?),
        )))
        .with_sequence(id, FlowTarget::Terminal(TerminalKind::Complete))?;
    Ok(graph.compile(
        &JobName::new("child-job")?,
        DefinitionRevision::new("child-v1")?,
    )?)
}

fn mapping(target: &str) -> Result<NestedJobParameterMapping, Box<dyn Error>> {
    Ok(NestedJobParameterMapping::new(
        ParameterName::new(target)?,
        ParameterRole::Identifying,
        NestedJobParameterSource::ParentParameter(ParameterName::new("tenant")?),
        ParameterValueKind::String,
        ParameterCoercion::Exact,
        MissingParameterPolicy::Fail,
    ))
}

fn parent_plan(
    mappings: Vec<NestedJobParameterMapping>,
    mapping_revision: &str,
) -> Result<oxide_batch::CompiledExecutionPlan, Box<dyn Error>> {
    let child = child_plan()?;
    let id = NodeId::new("child-job-node")?;
    let node = NestedJobNode::new(
        id.clone(),
        child.definition_identity().clone(),
        token(mapping_revision)?,
        mappings,
    )?;
    let graph = FlowGraph::new(id.clone())
        .with_node(FlowNode::nested_job(node))
        .with_sequence(id, FlowTarget::Terminal(TerminalKind::Complete))?;
    Ok(graph.compile(
        &JobName::new("parent-job")?,
        DefinitionRevision::new("parent-v1")?,
    )?)
}

#[test]
fn nested_job_forces_format_4_and_reader_accepts_it() -> Result<(), Box<dyn Error>> {
    let plan = parent_plan(vec![mapping("tenant")?], "mapping-v1")?;
    assert_eq!(plan.manifest_format(), 4);
    let manifest = DefinitionManifest::read(plan.definition_identity().canonical_manifest())?;
    assert_eq!(manifest.format(), 4);
    assert_eq!(manifest.node_count(), Some(1));
    let text = std::str::from_utf8(plan.definition_identity().canonical_manifest())?;
    assert!(text.contains("\"kind\":\"nested_job\""));
    assert!(text.contains("\"mapping_revision\":\"mapping-v1\""));
    assert!(text.contains("\"job\":\"child-job\""));
    assert!(!text.contains("secret-value"));
    Ok(())
}

#[test]
fn mapping_declaration_order_does_not_change_fingerprint() -> Result<(), Box<dyn Error>> {
    let first = parent_plan(vec![mapping("zeta")?, mapping("alpha")?], "mapping-v1")?;
    let second = parent_plan(vec![mapping("alpha")?, mapping("zeta")?], "mapping-v1")?;
    assert_eq!(first.fingerprint(), second.fingerprint());
    assert_eq!(
        first.definition_identity().canonical_manifest(),
        second.definition_identity().canonical_manifest()
    );
    Ok(())
}

#[test]
fn mapping_revision_changes_restart_identity() -> Result<(), Box<dyn Error>> {
    let first = parent_plan(vec![mapping("tenant")?], "mapping-v1")?;
    let second = parent_plan(vec![mapping("tenant")?], "mapping-v2")?;
    assert_ne!(first.fingerprint(), second.fingerprint());
    Ok(())
}

#[test]
fn duplicate_child_parameter_is_rejected() -> Result<(), Box<dyn Error>> {
    let child = child_plan()?;
    let node = NestedJobNode::new(
        NodeId::new("child-job-node")?,
        child.definition_identity().clone(),
        token("mapping-v1")?,
        vec![mapping("tenant")?, mapping("tenant")?],
    );
    assert!(matches!(
        node,
        Err(PlanError::DuplicateNestedJobParameter { .. })
    ));
    Ok(())
}

#[test]
fn mapping_count_and_selector_path_are_bounded() -> Result<(), Box<dyn Error>> {
    let child = child_plan()?;
    let mappings = (0..=MAX_NESTED_JOB_PARAMETERS)
        .map(|index| mapping(&format!("p{index}")))
        .collect::<Result<Vec<_>, _>>()?;
    let node = NestedJobNode::new(
        NodeId::new("child-job-node")?,
        child.definition_identity().clone(),
        token("mapping-v1")?,
        mappings,
    );
    assert!(matches!(
        node,
        Err(PlanError::TooManyNestedJobParameters { .. })
    ));

    let oversized = "x".repeat(MAX_SELECTOR_PATH_BYTES + 1);
    assert!(matches!(
        SelectorPath::new(vec![oversized]),
        Err(PlanError::InvalidSelectorPath { .. })
    ));
    Ok(())
}

#[test]
fn selector_model_is_closed_and_typed() -> Result<(), Box<dyn Error>> {
    let path = SelectorPath::new(vec!["customer".to_owned(), "id".to_owned()])?;
    assert_eq!(path.segments(), ["customer", "id"]);
    assert_eq!(
        FrameworkParameterSource::ParentJobExecutionId.value_kind(),
        ParameterValueKind::U64
    );
    assert_eq!(
        FrameworkParameterSource::ParentDefinitionFingerprint.value_kind(),
        ParameterValueKind::String
    );
    Ok(())
}
