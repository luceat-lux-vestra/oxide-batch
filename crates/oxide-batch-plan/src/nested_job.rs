use std::collections::BTreeSet;

use oxide_batch_core::{
    ComponentRevision, DefinitionIdentity, JobName, NodeId, ParameterName, ParameterRole,
    ParameterValueKind, StateSchemaId, StateSchemaVersion,
};
use serde_json::{Value, json};

use super::PlanError;

/// Maximum number of typed child parameters one nested-job node may declare.
pub const MAX_NESTED_JOB_PARAMETERS: usize = 64;
/// Maximum UTF-8 bytes across one structured selector path.
pub const MAX_SELECTOR_PATH_BYTES: usize = 1_024;
/// Maximum number of path segments in one structured selector.
pub const MAX_SELECTOR_PATH_SEGMENTS: usize = 32;

/// A bounded structured path into committed execution-context state.
///
/// The path is represented as validated segments rather than an expression
/// language. It therefore cannot invoke functions, mutate state, perform I/O,
/// or consult ambient process state.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SelectorPath(Vec<String>);

impl SelectorPath {
    /// Validates one structured path.
    ///
    /// # Errors
    ///
    /// Rejects an empty path, more than 32 segments, empty/whitespace-padded
    /// segments, control characters, or a path larger than 1,024 UTF-8 bytes.
    pub fn new(segments: impl IntoIterator<Item = String>) -> Result<Self, PlanError> {
        let segments = segments.into_iter().collect::<Vec<_>>();
        if segments.is_empty() || segments.len() > MAX_SELECTOR_PATH_SEGMENTS {
            return Err(PlanError::InvalidSelectorPath {
                max_bytes: MAX_SELECTOR_PATH_BYTES,
                max_segments: MAX_SELECTOR_PATH_SEGMENTS,
            });
        }
        let bytes = segments
            .iter()
            .map(String::len)
            .sum::<usize>()
            .saturating_add(segments.len().saturating_sub(1));
        if bytes > MAX_SELECTOR_PATH_BYTES
            || segments.iter().any(|segment| {
                segment.is_empty()
                    || segment.trim() != segment
                    || segment.chars().any(char::is_control)
            })
        {
            return Err(PlanError::InvalidSelectorPath {
                max_bytes: MAX_SELECTOR_PATH_BYTES,
                max_segments: MAX_SELECTOR_PATH_SEGMENTS,
            });
        }
        Ok(Self(segments))
    }

    /// Borrows the path segments in declaration order.
    #[must_use]
    pub fn segments(&self) -> &[String] {
        &self.0
    }

    pub(crate) fn manifest_value(&self) -> Value {
        Value::Array(self.0.iter().cloned().map(Value::String).collect())
    }
}

/// Closed framework metadata that a nested-job mapping may read.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum FrameworkParameterSource {
    /// Opaque parent job-instance identifier.
    ParentJobInstanceId,
    /// Opaque parent job-execution identifier.
    ParentJobExecutionId,
    /// One-based parent execution attempt ordinal.
    ParentAttempt,
    /// Parent definition revision.
    ParentDefinitionRevision,
    /// Parent definition fingerprint encoded as lower-case hexadecimal.
    ParentDefinitionFingerprint,
    /// Logical nested-job node identifier.
    NestedJobNodeId,
}

impl FrameworkParameterSource {
    /// Returns the stable manifest code.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ParentJobInstanceId => "parent_job_instance_id",
            Self::ParentJobExecutionId => "parent_job_execution_id",
            Self::ParentAttempt => "parent_attempt",
            Self::ParentDefinitionRevision => "parent_definition_revision",
            Self::ParentDefinitionFingerprint => "parent_definition_fingerprint",
            Self::NestedJobNodeId => "nested_job_node_id",
        }
    }

    /// Returns the value kind produced before any declared coercion.
    #[must_use]
    pub const fn value_kind(self) -> ParameterValueKind {
        match self {
            Self::ParentJobInstanceId | Self::ParentJobExecutionId | Self::ParentAttempt => {
                ParameterValueKind::U64
            }
            Self::ParentDefinitionRevision
            | Self::ParentDefinitionFingerprint
            | Self::NestedJobNodeId => ParameterValueKind::String,
        }
    }
}

/// One accepted source family for a nested-job child parameter.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum NestedJobParameterSource {
    /// An immutable parent launch parameter.
    ParentParameter(ParameterName),
    /// A value from the committed parent job-execution context snapshot.
    ParentJobContext {
        /// Required context schema identity.
        schema: StateSchemaId,
        /// Required committed schema version.
        schema_version: StateSchemaVersion,
        /// Structured object path.
        path: SelectorPath,
    },
    /// A value from the latest committed context of one logical parent step.
    ParentStepContext {
        /// Logical parent step whose committed context is selected.
        node: NodeId,
        /// Required context schema identity.
        schema: StateSchemaId,
        /// Required committed schema version.
        schema_version: StateSchemaVersion,
        /// Structured object path.
        path: SelectorPath,
    },
    /// Closed framework metadata with no ambient source access.
    Framework(FrameworkParameterSource),
}

impl NestedJobParameterSource {
    pub(crate) fn manifest_value(&self) -> Value {
        match self {
            Self::ParentParameter(name) => json!({
                "kind": "parent_parameter",
                "name": name.as_str()
            }),
            Self::ParentJobContext {
                schema,
                schema_version,
                path,
            } => json!({
                "kind": "parent_job_context",
                "path": path.manifest_value(),
                "schema": schema.as_str(),
                "schema_version": schema_version.get()
            }),
            Self::ParentStepContext {
                node,
                schema,
                schema_version,
                path,
            } => json!({
                "kind": "parent_step_context",
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

/// Explicit deterministic coercion applied after source selection.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum ParameterCoercion {
    /// Source and target kinds must match exactly.
    #[default]
    Exact,
    /// Parse a UTF-8 string as a signed integer.
    StringToI64,
    /// Parse a UTF-8 string as an unsigned integer.
    StringToU64,
    /// Parse exactly `true` or `false` as a boolean.
    StringToBool,
    /// Convert a non-negative signed integer to unsigned.
    I64ToU64,
    /// Convert an unsigned integer that fits into `i64`.
    U64ToI64,
}

impl ParameterCoercion {
    /// Returns the stable manifest code.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::StringToI64 => "string_to_i64",
            Self::StringToU64 => "string_to_u64",
            Self::StringToBool => "string_to_bool",
            Self::I64ToU64 => "i64_to_u64",
            Self::U64ToI64 => "u64_to_i64",
        }
    }
}

/// Closed behavior when a mapping source is absent.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum MissingParameterPolicy {
    /// Missing data rejects the complete mapping.
    #[default]
    Fail,
    /// Use the non-sensitive zero/empty value of the declared target kind.
    ///
    /// This is deliberately not an arbitrary literal default: user values stay
    /// out of manifests and fingerprints.
    TypeDefault,
}

impl MissingParameterPolicy {
    /// Returns the stable manifest code.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fail => "fail",
            Self::TypeDefault => "type_default",
        }
    }
}

/// One deterministic typed child-parameter mapping.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NestedJobParameterMapping {
    target: ParameterName,
    role: ParameterRole,
    source: NestedJobParameterSource,
    expected: ParameterValueKind,
    coercion: ParameterCoercion,
    missing: MissingParameterPolicy,
}

impl NestedJobParameterMapping {
    /// Declares one child-parameter mapping.
    #[must_use]
    pub const fn new(
        target: ParameterName,
        role: ParameterRole,
        source: NestedJobParameterSource,
        expected: ParameterValueKind,
        coercion: ParameterCoercion,
        missing: MissingParameterPolicy,
    ) -> Self {
        Self {
            target,
            role,
            source,
            expected,
            coercion,
            missing,
        }
    }

    /// Borrows the child parameter name.
    #[must_use]
    pub const fn target(&self) -> &ParameterName {
        &self.target
    }

    /// Returns whether the mapped child parameter selects child instance identity.
    #[must_use]
    pub const fn role(&self) -> ParameterRole {
        self.role
    }

    /// Borrows the structured source declaration.
    #[must_use]
    pub const fn source(&self) -> &NestedJobParameterSource {
        &self.source
    }

    /// Returns the required child parameter type.
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
            "role": match self.role {
                ParameterRole::Identifying => "identifying",
                ParameterRole::NonIdentifying => "non_identifying",
            },
            "source": self.source.manifest_value(),
            "target": self.target.as_str()
        })
    }
}

/// A format-4 nested-job lifecycle boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NestedJobNode {
    id: NodeId,
    child_definition: DefinitionIdentity,
    mapping_revision: ComponentRevision,
    mappings: Vec<NestedJobParameterMapping>,
}

impl NestedJobNode {
    /// Declares a child definition and its complete deterministic parameter map.
    ///
    /// Mappings are canonicalized by child parameter name so builder declaration
    /// order cannot change the definition fingerprint.
    ///
    /// # Errors
    ///
    /// Rejects a legacy/unbound child identity, more than 64 mappings, or a
    /// duplicate child parameter name.
    pub fn new(
        id: NodeId,
        child_definition: DefinitionIdentity,
        mapping_revision: ComponentRevision,
        mut mappings: Vec<NestedJobParameterMapping>,
    ) -> Result<Self, PlanError> {
        if child_definition.job_name().is_none() {
            return Err(PlanError::NestedJobMissingChildIdentity { node: id });
        }
        if mappings.len() > MAX_NESTED_JOB_PARAMETERS {
            return Err(PlanError::TooManyNestedJobParameters {
                node: id,
                max: MAX_NESTED_JOB_PARAMETERS,
            });
        }
        mappings.sort_by(|left, right| left.target.cmp(&right.target));
        let mut targets = BTreeSet::new();
        for mapping in &mappings {
            if !targets.insert(mapping.target.clone()) {
                return Err(PlanError::DuplicateNestedJobParameter {
                    node: id,
                    parameter: mapping.target.clone(),
                });
            }
        }
        Ok(Self {
            id,
            child_definition,
            mapping_revision,
            mappings,
        })
    }

    /// Borrows the stable parent logical node identifier.
    #[must_use]
    pub const fn id(&self) -> &NodeId {
        &self.id
    }

    /// Borrows the exact child definition identity required by the node.
    #[must_use]
    pub const fn child_definition(&self) -> &DefinitionIdentity {
        &self.child_definition
    }

    /// Borrows the mapping implementation revision.
    #[must_use]
    pub const fn mapping_revision(&self) -> &ComponentRevision {
        &self.mapping_revision
    }

    /// Borrows mappings in canonical target-name order.
    #[must_use]
    pub fn mappings(&self) -> &[NestedJobParameterMapping] {
        &self.mappings
    }

    pub(crate) fn manifest_value(&self) -> Value {
        let child_name = self
            .child_definition
            .job_name()
            .map_or("", JobName::as_str);
        json!({
            "child": {
                "fingerprint": digest_hex(self.child_definition.manifest_digest()),
                "format": self.child_definition.manifest_format(),
                "job": child_name,
                "revision": self.child_definition.revision().as_str()
            },
            "id": self.id.as_str(),
            "kind": "nested_job",
            "mapping_revision": self.mapping_revision.as_str(),
            "parameters": self
                .mappings
                .iter()
                .map(NestedJobParameterMapping::manifest_value)
                .collect::<Vec<_>>()
        })
    }
}

fn digest_hex(digest: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}
