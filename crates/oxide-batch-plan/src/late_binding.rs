use std::error::Error;
use std::fmt;

use oxide_batch_core::{
    ComponentRevision, DefinitionError, DefinitionTokenKind, NodeId, ParameterName,
    ParameterValueKind, StateSchemaId, StateSchemaVersion, definition_token, validate_token,
};
use serde_json::{Value, json};

use crate::{MissingParameterPolicy, ParameterCoercion, SelectorPath};

pub use oxide_batch_core::{
    MAX_LATE_BOUND_INPUTS, MAX_SCOPED_COMPONENTS, ScopeFrameworkSource, ScopeKind,
    ScopedComponentId,
};

definition_token!(
    ScopeFactoryKind,
    DefinitionTokenKind::Component,
    "A stable application-owned kind identifier for one scoped component factory."
);

definition_token!(
    ScopeResolverKind,
    DefinitionTokenKind::Component,
    "A stable application-owned kind identifier for one scoped late-binding resolver."
);

/// One accepted source family for a scoped late-bound value.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum LateBoundSource {
    /// An immutable launch parameter.
    JobParameter(ParameterName),
    /// A value from the committed job-execution context visible at scope creation.
    JobContext {
        /// Required context schema identity.
        schema: StateSchemaId,
        /// Required committed schema version.
        schema_version: StateSchemaVersion,
        /// Structured object path.
        path: SelectorPath,
    },
    /// A value from the latest committed context of one logical step.
    StepContext {
        /// Logical step whose committed context is selected.
        node: NodeId,
        /// Required context schema identity.
        schema: StateSchemaId,
        /// Required committed schema version.
        schema_version: StateSchemaVersion,
        /// Structured object path.
        path: SelectorPath,
    },
    /// Closed framework metadata with no ambient source access.
    Framework(ScopeFrameworkSource),
}

impl LateBoundSource {
    pub(crate) fn manifest_value(&self) -> Value {
        match self {
            Self::JobParameter(name) => json!({
                "kind": "job_parameter",
                "name": name.as_str()
            }),
            Self::JobContext {
                schema,
                schema_version,
                path,
            } => json!({
                "kind": "job_context",
                "path": path.manifest_value(),
                "schema": schema.as_str(),
                "schema_version": schema_version.get()
            }),
            Self::StepContext {
                node,
                schema,
                schema_version,
                path,
            } => json!({
                "kind": "step_context",
                "node": node.as_str(),
                "path": path.manifest_value(),
                "schema": schema.as_str(),
                "schema_version": schema_version.get()
            }),
            Self::Framework(source) => json!({
                "field": source.as_str(),
                "kind": "framework"
            }),
        }
    }
}

/// One named, deterministic late-bound input to a scoped component factory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LateBoundInput {
    name: ParameterName,
    source: LateBoundSource,
    expected: ParameterValueKind,
    coercion: ParameterCoercion,
    missing: MissingParameterPolicy,
}

impl LateBoundInput {
    /// Declares one late-bound input.
    #[must_use]
    pub const fn new(
        name: ParameterName,
        source: LateBoundSource,
        expected: ParameterValueKind,
        coercion: ParameterCoercion,
        missing: MissingParameterPolicy,
    ) -> Self {
        Self {
            name,
            source,
            expected,
            coercion,
            missing,
        }
    }

    /// Borrows the stable factory-input name.
    #[must_use]
    pub const fn name(&self) -> &ParameterName {
        &self.name
    }

    /// Borrows the structured source declaration.
    #[must_use]
    pub const fn source(&self) -> &LateBoundSource {
        &self.source
    }

    /// Returns the type required by the factory input.
    #[must_use]
    pub const fn expected_kind(&self) -> ParameterValueKind {
        self.expected
    }

    /// Returns the deterministic coercion policy.
    #[must_use]
    pub const fn coercion(&self) -> ParameterCoercion {
        self.coercion
    }

    /// Returns missing-source behavior.
    #[must_use]
    pub const fn missing_policy(&self) -> MissingParameterPolicy {
        self.missing
    }

    pub(crate) fn manifest_value(&self) -> Value {
        json!({
            "coercion": self.coercion.as_str(),
            "expected": self.expected.as_str(),
            "missing": self.missing.as_str(),
            "name": self.name.as_str(),
            "source": self.source.manifest_value()
        })
    }
}

/// Restart-relevant declaration for one job- or step-scoped component.
///
/// Actual resolved values and application resources are deliberately absent.
/// Declaration order cannot affect fingerprint material because inputs are
/// canonicalized by their stable input name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScopedComponentDefinition {
    scope: ScopeKind,
    id: ScopedComponentId,
    factory_kind: ScopeFactoryKind,
    factory_revision: ComponentRevision,
    resolver_kind: ScopeResolverKind,
    resolver_revision: ComponentRevision,
    inputs: Vec<LateBoundInput>,
}

impl ScopedComponentDefinition {
    /// Constructs a bounded, canonical scoped-component definition.
    ///
    /// # Errors
    ///
    /// Rejects more than 64 late-bound inputs or duplicate stable input names.
    pub fn new(
        scope: ScopeKind,
        id: ScopedComponentId,
        factory_kind: ScopeFactoryKind,
        factory_revision: ComponentRevision,
        resolver_kind: ScopeResolverKind,
        resolver_revision: ComponentRevision,
        mut inputs: Vec<LateBoundInput>,
    ) -> Result<Self, LateBindingDefinitionError> {
        if inputs.len() > MAX_LATE_BOUND_INPUTS {
            return Err(LateBindingDefinitionError::TooManyInputs {
                max: MAX_LATE_BOUND_INPUTS,
            });
        }
        inputs.sort_by(|left, right| left.name.cmp(&right.name));
        if inputs.windows(2).any(|pair| pair[0].name == pair[1].name) {
            return Err(LateBindingDefinitionError::DuplicateInput);
        }
        Ok(Self {
            scope,
            id,
            factory_kind,
            factory_revision,
            resolver_kind,
            resolver_revision,
            inputs,
        })
    }

    /// Returns the attempt-local scope kind.
    #[must_use]
    pub const fn scope(&self) -> ScopeKind {
        self.scope
    }

    /// Borrows the logical component identifier.
    #[must_use]
    pub const fn id(&self) -> &ScopedComponentId {
        &self.id
    }

    /// Borrows the application-owned factory kind.
    #[must_use]
    pub const fn factory_kind(&self) -> &ScopeFactoryKind {
        &self.factory_kind
    }

    /// Borrows the restart-relevant factory revision.
    #[must_use]
    pub const fn factory_revision(&self) -> &ComponentRevision {
        &self.factory_revision
    }

    /// Borrows the application-owned resolver kind.
    #[must_use]
    pub const fn resolver_kind(&self) -> &ScopeResolverKind {
        &self.resolver_kind
    }

    /// Borrows the restart-relevant resolver revision.
    #[must_use]
    pub const fn resolver_revision(&self) -> &ComponentRevision {
        &self.resolver_revision
    }

    /// Borrows late-bound inputs in canonical input-name order.
    #[must_use]
    pub fn inputs(&self) -> &[LateBoundInput] {
        &self.inputs
    }

    pub(crate) fn manifest_value(&self) -> Value {
        json!({
            "factory": {
                "kind": self.factory_kind.as_str(),
                "revision": self.factory_revision.as_str()
            },
            "id": self.id.as_str(),
            "inputs": self.inputs.iter().map(LateBoundInput::manifest_value).collect::<Vec<_>>(),
            "resolver": {
                "kind": self.resolver_kind.as_str(),
                "revision": self.resolver_revision.as_str()
            },
            "scope": self.scope.as_str()
        })
    }
}

/// Stable failures produced while validating a scoped-component definition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum LateBindingDefinitionError {
    /// One component declared more late-bound inputs than the accepted bound.
    TooManyInputs {
        /// Maximum accepted input count.
        max: usize,
    },
    /// Two declarations reused one stable input name.
    DuplicateInput,
}

impl fmt::Display for LateBindingDefinitionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyInputs { max } => {
                write!(
                    formatter,
                    "scoped component exceeds the late-bound input limit of {max}"
                )
            }
            Self::DuplicateInput => {
                formatter.write_str("scoped component contains a duplicate input name")
            }
        }
    }
}

impl Error for LateBindingDefinitionError {}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use oxide_batch_core::{
        ComponentRevision, NodeId, ParameterName, ParameterValueKind, StateSchemaId,
        StateSchemaVersion,
    };

    use super::*;

    fn input(name: &str, source: LateBoundSource) -> LateBoundInput {
        LateBoundInput::new(
            ParameterName::new(name).expect("input name"),
            source,
            ParameterValueKind::String,
            ParameterCoercion::Exact,
            MissingParameterPolicy::Fail,
        )
    }

    fn component(inputs: Vec<LateBoundInput>) -> ScopedComponentDefinition {
        ScopedComponentDefinition::new(
            ScopeKind::Step,
            ScopedComponentId::new("reader").expect("component id"),
            ScopeFactoryKind::new("postgres-reader").expect("factory kind"),
            ComponentRevision::new("factory-v1").expect("factory revision"),
            ScopeResolverKind::new("structured-selector").expect("resolver kind"),
            ComponentRevision::new("resolver-v1").expect("resolver revision"),
            inputs,
        )
        .expect("component definition")
    }

    #[test]
    fn declaration_order_does_not_change_canonical_definition_material() {
        let left = component(vec![
            input(
                "tenant",
                LateBoundSource::JobParameter(ParameterName::new("tenant").expect("parameter")),
            ),
            input(
                "region",
                LateBoundSource::Framework(ScopeFrameworkSource::DefinitionRevision),
            ),
        ]);
        let right = component(vec![
            input(
                "region",
                LateBoundSource::Framework(ScopeFrameworkSource::DefinitionRevision),
            ),
            input(
                "tenant",
                LateBoundSource::JobParameter(ParameterName::new("tenant").expect("parameter")),
            ),
        ]);

        assert_eq!(left.manifest_value(), right.manifest_value());
        assert_eq!(left.inputs()[0].name().as_str(), "region");
        assert_eq!(left.inputs()[1].name().as_str(), "tenant");
    }

    #[test]
    fn definition_material_contains_contract_identity_but_no_resolved_value() {
        let definition = component(vec![input(
            "cursor",
            LateBoundSource::StepContext {
                node: NodeId::new("load").expect("node"),
                schema: StateSchemaId::new("cursor.v1").expect("schema"),
                schema_version: StateSchemaVersion::new(1).expect("schema version"),
                path: SelectorPath::new([String::from("position")]).expect("selector"),
            },
        )]);
        let encoded = serde_json::to_string(&definition.manifest_value()).expect("json");

        assert!(encoded.contains("factory-v1"));
        assert!(encoded.contains("resolver-v1"));
        assert!(encoded.contains("cursor.v1"));
        assert!(encoded.contains("position"));
        assert!(!encoded.contains("resolved_value"));
        assert!(!encoded.contains("value_digest"));
    }

    #[test]
    fn duplicate_and_over_bound_inputs_fail_closed() {
        let duplicate = input(
            "tenant",
            LateBoundSource::JobParameter(ParameterName::new("tenant").expect("parameter")),
        );
        let result = ScopedComponentDefinition::new(
            ScopeKind::Job,
            ScopedComponentId::new("service").expect("component id"),
            ScopeFactoryKind::new("service-factory").expect("factory kind"),
            ComponentRevision::new("factory-v1").expect("factory revision"),
            ScopeResolverKind::new("structured-selector").expect("resolver kind"),
            ComponentRevision::new("resolver-v1").expect("resolver revision"),
            vec![duplicate.clone(), duplicate],
        );
        assert_eq!(result, Err(LateBindingDefinitionError::DuplicateInput));

        let inputs = (0..=MAX_LATE_BOUND_INPUTS)
            .map(|index| {
                input(
                    &format!("input-{index}"),
                    LateBoundSource::Framework(ScopeFrameworkSource::DefinitionRevision),
                )
            })
            .collect::<Vec<_>>();
        let result = ScopedComponentDefinition::new(
            ScopeKind::Job,
            ScopedComponentId::new("service").expect("component id"),
            ScopeFactoryKind::new("service-factory").expect("factory kind"),
            ComponentRevision::new("factory-v1").expect("factory revision"),
            ScopeResolverKind::new("structured-selector").expect("resolver kind"),
            ComponentRevision::new("resolver-v1").expect("resolver revision"),
            inputs,
        );
        assert_eq!(
            result,
            Err(LateBindingDefinitionError::TooManyInputs {
                max: MAX_LATE_BOUND_INPUTS
            })
        );
    }

    #[test]
    fn scoped_components_extend_format4_definition_identity() {
        use crate::{
            ExitPattern, FlowGraph, FlowNode, FlowTarget, FlowTransition, StepComponents, StepNode,
        };
        use oxide_batch_core::{
            DefinitionRevision, JobName, MANIFEST_FORMAT_ADVANCED_FLOW, StepName, TerminalKind,
        };

        let node = NodeId::new("load").expect("node");
        let graph = FlowGraph::new(node.clone())
            .with_node(FlowNode::step(StepNode::new(
                node.clone(),
                StepName::new("load").expect("step"),
                StepComponents::Tasklet(ComponentRevision::new("tasklet-v1").expect("revision")),
            )))
            .with_transition(FlowTransition::new(
                node,
                ExitPattern::new("*").expect("pattern"),
                FlowTarget::Terminal(TerminalKind::Complete),
            ))
            .with_scoped_component(component(vec![input(
                "tenant",
                LateBoundSource::JobParameter(ParameterName::new("tenant").expect("parameter")),
            )]));
        let plan = graph
            .compile(
                &JobName::new("scoped").expect("job"),
                DefinitionRevision::new("v1").expect("definition revision"),
            )
            .expect("compiled");

        assert_eq!(plan.manifest_format(), MANIFEST_FORMAT_ADVANCED_FLOW);
        assert_eq!(plan.scoped_components().len(), 1);
        let encoded = String::from_utf8(plan.definition_identity().canonical_manifest().to_vec())
            .expect("utf8 manifest");
        assert!(encoded.contains("\"scoped_components\""));
        assert!(encoded.contains("\"resolver-v1\""));
        assert!(!encoded.contains("resolved_value"));
        assert!(!encoded.contains("value_digest"));
    }

    #[test]
    fn scoped_component_ceiling_is_enforced_per_scope_not_globally() {
        use crate::{
            ExitPattern, FlowGraph, FlowNode, FlowTarget, FlowTransition, StepComponents, StepNode,
        };
        use oxide_batch_core::{DefinitionRevision, JobName, StepName, TerminalKind};

        fn empty_component(scope: ScopeKind, id: String) -> ScopedComponentDefinition {
            ScopedComponentDefinition::new(
                scope,
                ScopedComponentId::new(id).expect("component id"),
                ScopeFactoryKind::new("factory").expect("factory kind"),
                ComponentRevision::new("factory-v1").expect("factory revision"),
                ScopeResolverKind::new("selector").expect("resolver kind"),
                ComponentRevision::new("resolver-v1").expect("resolver revision"),
                Vec::new(),
            )
            .expect("component")
        }

        fn graph() -> FlowGraph {
            let node = NodeId::new("load").expect("node");
            FlowGraph::new(node.clone())
                .with_node(FlowNode::step(StepNode::new(
                    node.clone(),
                    StepName::new("load").expect("step"),
                    StepComponents::Tasklet(
                        ComponentRevision::new("tasklet-v1").expect("revision"),
                    ),
                )))
                .with_transition(FlowTransition::new(
                    node,
                    ExitPattern::new("*").expect("pattern"),
                    FlowTarget::Terminal(TerminalKind::Complete),
                ))
        }

        let mixed = (0..129).fold(graph(), |graph, index| {
            graph.with_scoped_component(empty_component(ScopeKind::Job, format!("job-{index}")))
        });
        let mixed = (0..128).fold(mixed, |graph, index| {
            graph.with_scoped_component(empty_component(ScopeKind::Step, format!("step-{index}")))
        });
        mixed
            .compile(
                &JobName::new("mixed-scope-bound").expect("job"),
                DefinitionRevision::new("v1").expect("revision"),
            )
            .expect("257 total components remain legal when each scope stays below 256");

        let over_one_scope = (0..=MAX_SCOPED_COMPONENTS).fold(graph(), |graph, index| {
            graph.with_scoped_component(empty_component(ScopeKind::Job, format!("job-{index}")))
        });
        assert!(matches!(
            over_one_scope.compile(
                &JobName::new("over-scope-bound").expect("job"),
                DefinitionRevision::new("v1").expect("revision"),
            ),
            Err(crate::PlanError::TooManyScopedComponents {
                max: MAX_SCOPED_COMPONENTS
            })
        ));
    }

    #[test]
    fn framework_sources_publish_stable_pre_coercion_types() {
        for source in [
            ScopeFrameworkSource::JobInstanceId,
            ScopeFrameworkSource::JobExecutionId,
            ScopeFrameworkSource::StepExecutionId,
            ScopeFrameworkSource::Attempt,
        ] {
            assert_eq!(source.value_kind(), ParameterValueKind::U64);
        }
        for source in [
            ScopeFrameworkSource::DefinitionRevision,
            ScopeFrameworkSource::DefinitionFingerprint,
            ScopeFrameworkSource::LogicalNodeId,
            ScopeFrameworkSource::StepName,
        ] {
            assert_eq!(source.value_kind(), ParameterValueKind::String);
        }
    }
}
