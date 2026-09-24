//! M7 #299 repeat definition identity, bounds, and manifest evidence.

use std::error::Error;

use oxide_batch::{
    ComponentRevision, DefinitionManifest, DefinitionRevision, FlowGraph, FlowNode, FlowTarget,
    JobName, MAX_REPEAT_INTERCEPTORS, MAX_REPEAT_NESTING_DEPTH, ManifestError, NestedFlow, NodeId,
    PlanError, RepeatDefinition, RepeatDefinitionError, RepeatId, RepeatInterceptorDefinition,
    RepeatInterceptorId, RepeatInterceptorKind, RepeatPolicyConfiguration, RepeatPolicyDefinition,
    RepeatPolicyKind, RepeatStateSchema, StateSchemaId, StateSchemaVersion, StepComponents,
    StepName, StepNode, TerminalKind,
};

fn interceptor(id: &str) -> Result<RepeatInterceptorDefinition, Box<dyn Error>> {
    Ok(RepeatInterceptorDefinition::new(
        RepeatInterceptorId::new(id)?,
        RepeatInterceptorKind::new("audit")?,
        ComponentRevision::new("v1")?,
    ))
}

fn repeat(
    id: &str,
    configuration: &str,
    interceptor_ids: &[&str],
) -> Result<RepeatDefinition, Box<dyn Error>> {
    Ok(RepeatDefinition::new(
        RepeatId::new(id)?,
        RepeatPolicyDefinition::new(
            RepeatPolicyKind::new("bounded-count")?,
            ComponentRevision::new("policy-v1")?,
            RepeatPolicyConfiguration::new(configuration)?,
        ),
        interceptor_ids
            .iter()
            .map(|id| interceptor(id))
            .collect::<Result<Vec<_>, _>>()?,
        RepeatStateSchema::new(
            StateSchemaId::new("repeat.state")?,
            StateSchemaVersion::new(1)?,
        ),
    )?)
}

fn repeat_identity(
    id: &str,
    policy_kind: &str,
    policy_revision: &str,
    configuration: &str,
    interceptors: &[(&str, &str, &str)],
    state_schema: &str,
    state_version: u32,
) -> Result<RepeatDefinition, Box<dyn Error>> {
    Ok(RepeatDefinition::new(
        RepeatId::new(id)?,
        RepeatPolicyDefinition::new(
            RepeatPolicyKind::new(policy_kind)?,
            ComponentRevision::new(policy_revision)?,
            RepeatPolicyConfiguration::new(configuration)?,
        ),
        interceptors
            .iter()
            .map(|(id, kind, revision)| {
                Ok(RepeatInterceptorDefinition::new(
                    RepeatInterceptorId::new(*id)?,
                    RepeatInterceptorKind::new(*kind)?,
                    ComponentRevision::new(*revision)?,
                ))
            })
            .collect::<Result<Vec<_>, Box<dyn Error>>>()?,
        RepeatStateSchema::new(
            StateSchemaId::new(state_schema)?,
            StateSchemaVersion::new(state_version)?,
        ),
    )?)
}

fn step(id: &str, repeat: Option<RepeatDefinition>) -> Result<StepNode, Box<dyn Error>> {
    let mut step = StepNode::new(
        NodeId::new(id)?,
        StepName::new(id)?,
        StepComponents::Tasklet(ComponentRevision::new("tasklet-v1")?),
    );
    if let Some(repeat) = repeat {
        step = step.with_repeat_definition(repeat);
    }
    Ok(step)
}

fn two_step_plan(
    reverse_declaration_order: bool,
    repeat: RepeatDefinition,
) -> Result<oxide_batch::CompiledExecutionPlan, Box<dyn Error>> {
    let alpha = NodeId::new("alpha")?;
    let zeta = NodeId::new("zeta")?;
    let alpha_step = step("alpha", Some(repeat))?;
    let zeta_step = step("zeta", None)?;

    let graph = if reverse_declaration_order {
        FlowGraph::new(alpha.clone())
            .with_node(FlowNode::step(zeta_step))
            .with_node(FlowNode::step(alpha_step))
    } else {
        FlowGraph::new(alpha.clone())
            .with_node(FlowNode::step(alpha_step))
            .with_node(FlowNode::step(zeta_step))
    }
    .with_sequence(alpha, FlowTarget::Node(zeta.clone()))?
    .with_sequence(zeta, FlowTarget::Terminal(TerminalKind::Complete))?;

    Ok(graph.compile(
        &JobName::new("repeat-definition")?,
        DefinitionRevision::new("v1")?,
    )?)
}

#[test]
fn repeat_definition_forces_format4_and_reader_accepts_it() -> Result<(), Box<dyn Error>> {
    let plan = two_step_plan(false, repeat("outer", "limit-10", &["audit"])?)?;

    assert_eq!(plan.manifest_format(), 4);
    let manifest = DefinitionManifest::read_verified(
        plan.definition_identity().canonical_manifest(),
        plan.fingerprint(),
    )?;
    assert_eq!(manifest.format(), 4);
    Ok(())
}

#[test]
fn every_restart_relevant_repeat_field_changes_fingerprint() -> Result<(), Box<dyn Error>> {
    let baseline_repeat = repeat_identity(
        "outer",
        "bounded-count",
        "policy-v1",
        "limit-10",
        &[("a", "audit", "v1"), ("b", "audit", "v1")],
        "repeat.state",
        1,
    )?;
    let baseline = two_step_plan(false, baseline_repeat.clone())?;

    let nested = baseline_repeat.clone().with_nested(repeat_identity(
        "inner",
        "bounded-count",
        "policy-v1",
        "limit-3",
        &[],
        "repeat.inner-state",
        1,
    )?)?;
    let variants = [
        (
            "repeat id",
            repeat_identity(
                "outer-v2",
                "bounded-count",
                "policy-v1",
                "limit-10",
                &[("a", "audit", "v1"), ("b", "audit", "v1")],
                "repeat.state",
                1,
            )?,
        ),
        (
            "policy kind",
            repeat_identity(
                "outer",
                "bounded-window",
                "policy-v1",
                "limit-10",
                &[("a", "audit", "v1"), ("b", "audit", "v1")],
                "repeat.state",
                1,
            )?,
        ),
        (
            "policy revision",
            repeat_identity(
                "outer",
                "bounded-count",
                "policy-v2",
                "limit-10",
                &[("a", "audit", "v1"), ("b", "audit", "v1")],
                "repeat.state",
                1,
            )?,
        ),
        (
            "policy configuration",
            repeat_identity(
                "outer",
                "bounded-count",
                "policy-v1",
                "limit-11",
                &[("a", "audit", "v1"), ("b", "audit", "v1")],
                "repeat.state",
                1,
            )?,
        ),
        (
            "interceptor id",
            repeat_identity(
                "outer",
                "bounded-count",
                "policy-v1",
                "limit-10",
                &[("a-v2", "audit", "v1"), ("b", "audit", "v1")],
                "repeat.state",
                1,
            )?,
        ),
        (
            "interceptor kind",
            repeat_identity(
                "outer",
                "bounded-count",
                "policy-v1",
                "limit-10",
                &[("a", "metrics", "v1"), ("b", "audit", "v1")],
                "repeat.state",
                1,
            )?,
        ),
        (
            "interceptor revision",
            repeat_identity(
                "outer",
                "bounded-count",
                "policy-v1",
                "limit-10",
                &[("a", "audit", "v2"), ("b", "audit", "v1")],
                "repeat.state",
                1,
            )?,
        ),
        (
            "interceptor order",
            repeat_identity(
                "outer",
                "bounded-count",
                "policy-v1",
                "limit-10",
                &[("b", "audit", "v1"), ("a", "audit", "v1")],
                "repeat.state",
                1,
            )?,
        ),
        (
            "state schema",
            repeat_identity(
                "outer",
                "bounded-count",
                "policy-v1",
                "limit-10",
                &[("a", "audit", "v1"), ("b", "audit", "v1")],
                "repeat.state-v2",
                1,
            )?,
        ),
        (
            "state schema version",
            repeat_identity(
                "outer",
                "bounded-count",
                "policy-v1",
                "limit-10",
                &[("a", "audit", "v1"), ("b", "audit", "v1")],
                "repeat.state",
                2,
            )?,
        ),
        ("nested repeat identity", nested),
    ];

    for (field, variant) in variants {
        let changed = two_step_plan(false, variant)?;
        assert_ne!(
            baseline.fingerprint(),
            changed.fingerprint(),
            "{field} must participate in restart identity"
        );
    }
    Ok(())
}

#[test]
fn node_declaration_order_remains_nonsemantic_with_repeat_metadata() -> Result<(), Box<dyn Error>> {
    let forward = two_step_plan(false, repeat("outer", "limit-10", &["a", "b"])?)?;
    let reverse = two_step_plan(true, repeat("outer", "limit-10", &["a", "b"])?)?;

    assert_eq!(
        forward.definition_identity().canonical_manifest(),
        reverse.definition_identity().canonical_manifest()
    );
    assert_eq!(forward.fingerprint(), reverse.fingerprint());
    Ok(())
}

#[test]
fn repeat_bounds_are_exact() -> Result<(), Box<dyn Error>> {
    let accepted_ids = (0..MAX_REPEAT_INTERCEPTORS)
        .map(|index| format!("i{index}"))
        .collect::<Vec<_>>();
    let accepted_refs = accepted_ids.iter().map(String::as_str).collect::<Vec<_>>();
    assert!(repeat("accepted", "limit-10", &accepted_refs).is_ok());

    let rejected_ids = (0..=MAX_REPEAT_INTERCEPTORS)
        .map(|index| format!("i{index}"))
        .collect::<Vec<_>>();
    let rejected_refs = rejected_ids.iter().map(String::as_str).collect::<Vec<_>>();
    assert!(matches!(
        repeat("rejected", "limit-10", &rejected_refs),
        Err(error) if error.downcast_ref::<RepeatDefinitionError>().is_some_and(
            |error| matches!(error, RepeatDefinitionError::TooManyInterceptors { .. })
        )
    ));
    assert!(matches!(
        repeat("duplicate-interceptor", "limit-10", &["same", "same"]),
        Err(error) if error.downcast_ref::<RepeatDefinitionError>().is_some_and(
            |error| matches!(error, RepeatDefinitionError::DuplicateInterceptorId { .. })
        )
    ));

    let mut current = repeat("r8", "limit-10", &[])?;
    for depth in (1..8).rev() {
        current = repeat(&format!("r{depth}"), "limit-10", &[])?.with_nested(current)?;
    }
    assert_eq!(current.nesting_depth(), MAX_REPEAT_NESTING_DEPTH);
    assert!(matches!(
        repeat("r0", "limit-10", &[])?.with_nested(current),
        Err(RepeatDefinitionError::NestingTooDeep { .. })
    ));
    Ok(())
}

#[test]
fn duplicate_repeat_id_across_steps_fails_closed() -> Result<(), Box<dyn Error>> {
    let alpha = NodeId::new("alpha")?;
    let zeta = NodeId::new("zeta")?;
    let graph = FlowGraph::new(alpha.clone())
        .with_node(FlowNode::step(step(
            "alpha",
            Some(repeat("same", "limit-10", &[])?),
        )?))
        .with_node(FlowNode::step(step(
            "zeta",
            Some(repeat("same", "limit-10", &[])?),
        )?))
        .with_sequence(alpha, FlowTarget::Node(zeta.clone()))?
        .with_sequence(zeta, FlowTarget::Terminal(TerminalKind::Complete))?;

    let result = graph.compile(
        &JobName::new("repeat-duplicate")?,
        DefinitionRevision::new("v1")?,
    );
    assert!(matches!(result, Err(PlanError::DuplicateRepeatId { .. })));
    Ok(())
}

#[test]
fn duplicate_repeat_id_across_nested_flow_fails_closed() -> Result<(), Box<dyn Error>> {
    let root = NodeId::new("root")?;
    let owner = NodeId::new("embedded")?;
    let child = NodeId::new("child")?;
    let child_graph = FlowGraph::new(child.clone())
        .with_node(FlowNode::step(step(
            "child",
            Some(repeat("same", "limit-10", &[])?),
        )?))
        .with_sequence(child, FlowTarget::Terminal(TerminalKind::Complete))?;

    let graph = FlowGraph::new(root.clone())
        .with_node(FlowNode::step(step(
            "root",
            Some(repeat("same", "limit-10", &[])?),
        )?))
        .with_nested_flow(NestedFlow::new(owner.clone(), child_graph))
        .with_sequence(root, FlowTarget::Node(owner.clone()))?
        .with_sequence(owner, FlowTarget::Terminal(TerminalKind::Complete))?;

    let result = graph.compile(
        &JobName::new("repeat-nested-duplicate")?,
        DefinitionRevision::new("v1")?,
    );
    assert!(matches!(result, Err(PlanError::DuplicateRepeatId { .. })));
    Ok(())
}

#[test]
fn repeat_manifest_rejects_malformed_unknown_and_newer_forms() -> Result<(), Box<dyn Error>> {
    let plan = two_step_plan(false, repeat("outer", "limit-10", &["audit"])?)?;
    let canonical = plan.definition_identity().canonical_manifest();

    let mut malformed: serde_json::Value = serde_json::from_slice(canonical)?;
    malformed["nodes"][0]["repeat"]["interceptors"] = serde_json::json!({});
    let malformed = serde_json::to_vec(&malformed)?;
    assert!(matches!(
        DefinitionManifest::read(&malformed),
        Err(ManifestError::MalformedGraph)
    ));

    let mut unknown: serde_json::Value = serde_json::from_slice(canonical)?;
    unknown["nodes"][0]["repeat"]["future_member"] = serde_json::json!(true);
    let unknown = serde_json::to_vec(&unknown)?;
    assert!(matches!(
        DefinitionManifest::read(&unknown),
        Err(ManifestError::MalformedGraph)
    ));

    let mut wrong_owner: serde_json::Value = serde_json::from_slice(canonical)?;
    wrong_owner["nodes"][0]["kind"] = serde_json::json!("decision");
    let wrong_owner = serde_json::to_vec(&wrong_owner)?;
    assert!(matches!(
        DefinitionManifest::read(&wrong_owner),
        Err(ManifestError::MalformedGraph)
    ));

    let mut downgraded: serde_json::Value = serde_json::from_slice(canonical)?;
    downgraded["format"] = serde_json::json!(2);
    let downgraded = serde_json::to_vec(&downgraded)?;
    assert!(matches!(
        DefinitionManifest::read(&downgraded),
        Err(ManifestError::MalformedGraph)
    ));

    let mut newer: serde_json::Value = serde_json::from_slice(canonical)?;
    newer["format"] = serde_json::json!(5);
    let newer = serde_json::to_vec(&newer)?;
    assert!(matches!(
        DefinitionManifest::read(&newer),
        Err(ManifestError::UnsupportedFormat {
            format: 5,
            supported: 4
        })
    ));
    Ok(())
}

#[test]
fn old_format2_manifest_remains_readable_without_repeat_metadata() -> Result<(), Box<dyn Error>> {
    let only = NodeId::new("only")?;
    let graph = FlowGraph::new(only.clone())
        .with_node(FlowNode::step(step("only", None)?))
        .with_sequence(only, FlowTarget::Terminal(TerminalKind::Complete))?;
    let plan = graph.compile(
        &JobName::new("repeat-old-format")?,
        DefinitionRevision::new("v1")?,
    )?;

    assert_eq!(plan.manifest_format(), 2);
    assert_eq!(
        DefinitionManifest::read_verified(
            plan.definition_identity().canonical_manifest(),
            plan.fingerprint(),
        )?
        .format(),
        2
    );
    Ok(())
}
