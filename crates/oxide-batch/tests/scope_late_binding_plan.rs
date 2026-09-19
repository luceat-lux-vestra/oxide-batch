//! M7 #276 scoped late-binding format-4 fingerprint evidence.

use std::error::Error;

use oxide_batch::{
    ComponentRevision, DefinitionRevision, FlowGraph, FlowNode, FlowTarget, JobName,
    LateBoundInput, LateBoundSource, MissingParameterPolicy, NodeId, ParameterCoercion,
    ParameterName, ParameterValueKind, ScopeFactoryKind, ScopeKind, ScopeResolverKind,
    ScopedComponentDefinition, ScopedComponentId, SelectorPath, StateSchemaId, StateSchemaVersion,
    StepComponents, StepName, StepNode, TerminalKind,
};

fn input(
    name: &str,
    schema_version: u32,
    coercion: ParameterCoercion,
    missing: MissingParameterPolicy,
) -> Result<LateBoundInput, Box<dyn Error>> {
    Ok(LateBoundInput::new(
        ParameterName::new(name)?,
        LateBoundSource::JobContext {
            schema: StateSchemaId::new("scope.contract")?,
            schema_version: StateSchemaVersion::new(schema_version)?,
            path: SelectorPath::new([String::from("tenant")])?,
        },
        ParameterValueKind::String,
        coercion,
        missing,
    ))
}

fn component(
    id: &str,
    resolver_revision: &str,
    inputs: Vec<LateBoundInput>,
) -> Result<ScopedComponentDefinition, Box<dyn Error>> {
    Ok(ScopedComponentDefinition::new(
        ScopeKind::Job,
        ScopedComponentId::new(id)?,
        ScopeFactoryKind::new("client-factory")?,
        ComponentRevision::new("factory-v1")?,
        ScopeResolverKind::new("structured-selector")?,
        ComponentRevision::new(resolver_revision)?,
        inputs,
    )?)
}

fn plan(
    first: ScopedComponentDefinition,
    second: Option<ScopedComponentDefinition>,
) -> Result<oxide_batch::CompiledExecutionPlan, Box<dyn Error>> {
    let node = NodeId::new("work")?;
    let mut graph = FlowGraph::new(node.clone())
        .with_node(FlowNode::step(StepNode::new(
            node.clone(),
            StepName::new("work")?,
            StepComponents::Tasklet(ComponentRevision::new("tasklet-v1")?),
        )))
        .with_sequence(node, FlowTarget::Terminal(TerminalKind::Complete))?
        .with_scoped_component(first);
    if let Some(second) = second {
        graph = graph.with_scoped_component(second);
    }
    Ok(graph.compile(
        &JobName::new("scope-fingerprint")?,
        DefinitionRevision::new("v1")?,
    )?)
}

#[test]
fn scoped_component_declaration_order_does_not_change_manifest_or_fingerprint()
-> Result<(), Box<dyn Error>> {
    let alpha = component(
        "alpha",
        "resolver-v1",
        vec![
            input(
                "zeta",
                1,
                ParameterCoercion::Exact,
                MissingParameterPolicy::Fail,
            )?,
            input(
                "alpha",
                1,
                ParameterCoercion::Exact,
                MissingParameterPolicy::Fail,
            )?,
        ],
    )?;
    let zeta = component(
        "zeta",
        "resolver-v1",
        vec![input(
            "tenant",
            1,
            ParameterCoercion::Exact,
            MissingParameterPolicy::Fail,
        )?],
    )?;

    let first = plan(alpha.clone(), Some(zeta.clone()))?;
    let second = plan(zeta, Some(alpha))?;

    assert_eq!(first.manifest_format(), 4);
    assert_eq!(
        first.definition_identity().canonical_manifest(),
        second.definition_identity().canonical_manifest()
    );
    assert_eq!(first.fingerprint(), second.fingerprint());
    Ok(())
}

#[test]
fn resolver_and_source_contract_changes_restart_identity() -> Result<(), Box<dyn Error>> {
    let baseline = plan(
        component(
            "client",
            "resolver-v1",
            vec![input(
                "tenant",
                1,
                ParameterCoercion::Exact,
                MissingParameterPolicy::Fail,
            )?],
        )?,
        None,
    )?;

    for changed in [
        component(
            "client",
            "resolver-v2",
            vec![input(
                "tenant",
                1,
                ParameterCoercion::Exact,
                MissingParameterPolicy::Fail,
            )?],
        )?,
        component(
            "client",
            "resolver-v1",
            vec![input(
                "tenant",
                2,
                ParameterCoercion::Exact,
                MissingParameterPolicy::Fail,
            )?],
        )?,
        component(
            "client",
            "resolver-v1",
            vec![input(
                "tenant",
                1,
                ParameterCoercion::StringToBool,
                MissingParameterPolicy::Fail,
            )?],
        )?,
        component(
            "client",
            "resolver-v1",
            vec![input(
                "tenant",
                1,
                ParameterCoercion::Exact,
                MissingParameterPolicy::TypeDefault,
            )?],
        )?,
    ] {
        let changed = plan(changed, None)?;
        assert_ne!(baseline.fingerprint(), changed.fingerprint());
        assert_ne!(
            baseline.definition_identity().canonical_manifest(),
            changed.definition_identity().canonical_manifest()
        );
    }
    Ok(())
}

#[test]
fn scoped_manifest_reader_rejects_malformed_and_overbound_components() -> Result<(), Box<dyn Error>>
{
    let scoped = plan(
        component(
            "client",
            "resolver-v1",
            vec![input(
                "tenant",
                1,
                ParameterCoercion::Exact,
                MissingParameterPolicy::Fail,
            )?],
        )?,
        None,
    )?;
    let canonical = scoped.definition_identity().canonical_manifest();

    let mut malformed: serde_json::Value = serde_json::from_slice(canonical)?;
    malformed["scoped_components"] = serde_json::json!({});
    let malformed = serde_json::to_vec(&malformed)?;
    assert!(matches!(
        oxide_batch::DefinitionManifest::read(&malformed),
        Err(oxide_batch::ManifestError::MalformedGraph)
    ));

    let mut overbound: serde_json::Value = serde_json::from_slice(canonical)?;
    overbound["scoped_components"] = serde_json::Value::Array(
        (0..=oxide_batch::MAX_SCOPED_COMPONENTS)
            .map(|_| serde_json::json!({"scope": "job"}))
            .collect(),
    );
    let overbound = serde_json::to_vec(&overbound)?;
    assert!(matches!(
        oxide_batch::DefinitionManifest::read(&overbound),
        Err(oxide_batch::ManifestError::MalformedGraph)
    ));

    Ok(())
}
