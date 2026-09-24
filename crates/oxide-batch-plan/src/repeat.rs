use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;

use oxide_batch_core::{
    ComponentRevision, DefinitionError, DefinitionTokenKind, MAX_REPEAT_INTERCEPTORS,
    MAX_REPEAT_NESTING_DEPTH, StateSchemaId, StateSchemaVersion, definition_token, validate_token,
};
use serde_json::{Value, json};

definition_token!(
    RepeatId,
    DefinitionTokenKind::Repeat,
    "A stable logical identifier for one repeat definition."
);
definition_token!(
    RepeatPolicyKind,
    DefinitionTokenKind::RepeatPolicy,
    "A stable application-owned kind identifier for one repeat policy."
);
definition_token!(
    RepeatPolicyConfiguration,
    DefinitionTokenKind::RepeatPolicy,
    "A bounded restart-relevant configuration fingerprint for one repeat policy."
);
definition_token!(
    RepeatInterceptorId,
    DefinitionTokenKind::RepeatInterceptor,
    "A stable logical identifier for one repeat interceptor registration."
);
definition_token!(
    RepeatInterceptorKind,
    DefinitionTokenKind::RepeatInterceptor,
    "A stable application-owned kind identifier for one repeat interceptor."
);

/// Restart-relevant identity of one repeat policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepeatPolicyDefinition {
    kind: RepeatPolicyKind,
    revision: ComponentRevision,
    configuration: RepeatPolicyConfiguration,
}

impl RepeatPolicyDefinition {
    /// Declares one repeat policy and its restart-relevant configuration.
    #[must_use]
    pub const fn new(
        kind: RepeatPolicyKind,
        revision: ComponentRevision,
        configuration: RepeatPolicyConfiguration,
    ) -> Self {
        Self {
            kind,
            revision,
            configuration,
        }
    }

    /// Borrows the application-owned policy kind.
    #[must_use]
    pub const fn kind(&self) -> &RepeatPolicyKind {
        &self.kind
    }

    /// Borrows the policy implementation revision.
    #[must_use]
    pub const fn revision(&self) -> &ComponentRevision {
        &self.revision
    }

    /// Borrows the bounded restart-relevant configuration fingerprint.
    #[must_use]
    pub const fn configuration(&self) -> &RepeatPolicyConfiguration {
        &self.configuration
    }

    fn manifest_value(&self) -> Value {
        json!({
            "configuration": self.configuration.as_str(),
            "kind": self.kind.as_str(),
            "revision": self.revision.as_str()
        })
    }
}

/// Restart-relevant identity of one ordered repeat interceptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepeatInterceptorDefinition {
    id: RepeatInterceptorId,
    kind: RepeatInterceptorKind,
    revision: ComponentRevision,
}

impl RepeatInterceptorDefinition {
    /// Declares one interceptor registration.
    #[must_use]
    pub const fn new(
        id: RepeatInterceptorId,
        kind: RepeatInterceptorKind,
        revision: ComponentRevision,
    ) -> Self {
        Self { id, kind, revision }
    }

    /// Borrows the stable registration ID.
    #[must_use]
    pub const fn id(&self) -> &RepeatInterceptorId {
        &self.id
    }

    /// Borrows the application-owned interceptor kind.
    #[must_use]
    pub const fn kind(&self) -> &RepeatInterceptorKind {
        &self.kind
    }

    /// Borrows the interceptor implementation revision.
    #[must_use]
    pub const fn revision(&self) -> &ComponentRevision {
        &self.revision
    }

    fn manifest_value(&self) -> Value {
        json!({
            "id": self.id.as_str(),
            "kind": self.kind.as_str(),
            "revision": self.revision.as_str()
        })
    }
}

/// Bounded durable state-schema identity for one repeat definition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepeatStateSchema {
    schema: StateSchemaId,
    version: StateSchemaVersion,
}

impl RepeatStateSchema {
    /// Declares the schema used by the later durable repeat-state boundary.
    #[must_use]
    pub const fn new(schema: StateSchemaId, version: StateSchemaVersion) -> Self {
        Self { schema, version }
    }

    /// Borrows the state schema ID.
    #[must_use]
    pub const fn schema(&self) -> &StateSchemaId {
        &self.schema
    }

    /// Returns the state schema version.
    #[must_use]
    pub const fn version(&self) -> StateSchemaVersion {
        self.version
    }

    fn manifest_value(&self) -> Value {
        json!({
            "schema": self.schema.as_str(),
            "version": self.version.get()
        })
    }
}

/// Canonical restart-relevant repeat declaration for one step.
///
/// Repeat declarations are metadata over one step execution. They do not add a
/// graph edge and therefore cannot create a graph cycle. Nested repeats form a
/// single wrapper chain; runtime iteration is deliberately owned by #301.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepeatDefinition {
    id: RepeatId,
    policy: RepeatPolicyDefinition,
    interceptors: Vec<RepeatInterceptorDefinition>,
    state: RepeatStateSchema,
    nested: Option<Box<RepeatDefinition>>,
    depth: usize,
}

impl RepeatDefinition {
    /// Constructs one bounded repeat declaration.
    ///
    /// # Errors
    ///
    /// Rejects more than 32 interceptors or duplicate interceptor IDs.
    pub fn new(
        id: RepeatId,
        policy: RepeatPolicyDefinition,
        interceptors: Vec<RepeatInterceptorDefinition>,
        state: RepeatStateSchema,
    ) -> Result<Self, RepeatDefinitionError> {
        if interceptors.len() > MAX_REPEAT_INTERCEPTORS {
            return Err(RepeatDefinitionError::TooManyInterceptors {
                max: MAX_REPEAT_INTERCEPTORS,
            });
        }
        let mut ids = BTreeSet::new();
        for interceptor in &interceptors {
            if !ids.insert(interceptor.id.clone()) {
                return Err(RepeatDefinitionError::DuplicateInterceptorId {
                    interceptor: interceptor.id.clone(),
                });
            }
        }
        Ok(Self {
            id,
            policy,
            interceptors,
            state,
            nested: None,
            depth: 1,
        })
    }

    /// Wraps one nested repeat declaration.
    ///
    /// # Errors
    ///
    /// Rejects nesting deeper than 8 or reuse of this repeat ID anywhere in
    /// the nested chain.
    pub fn with_nested(mut self, nested: RepeatDefinition) -> Result<Self, RepeatDefinitionError> {
        if nested.contains_id(&self.id) {
            return Err(RepeatDefinitionError::DuplicateRepeatId {
                repeat: self.id.clone(),
            });
        }
        let depth = nested.depth.saturating_add(1);
        if depth > MAX_REPEAT_NESTING_DEPTH {
            return Err(RepeatDefinitionError::NestingTooDeep {
                max: MAX_REPEAT_NESTING_DEPTH,
            });
        }
        self.nested = Some(Box::new(nested));
        self.depth = depth;
        Ok(self)
    }

    /// Borrows the stable repeat ID.
    #[must_use]
    pub const fn id(&self) -> &RepeatId {
        &self.id
    }

    /// Borrows the restart-relevant policy declaration.
    #[must_use]
    pub const fn policy(&self) -> &RepeatPolicyDefinition {
        &self.policy
    }

    /// Borrows interceptors in semantic before-order.
    #[must_use]
    pub fn interceptors(&self) -> &[RepeatInterceptorDefinition] {
        &self.interceptors
    }

    /// Borrows the durable state-schema identity.
    #[must_use]
    pub const fn state_schema(&self) -> &RepeatStateSchema {
        &self.state
    }

    /// Borrows the nested repeat wrapper, when present.
    #[must_use]
    pub fn nested(&self) -> Option<&RepeatDefinition> {
        self.nested.as_deref()
    }

    /// Returns the complete wrapper-chain depth.
    #[must_use]
    pub const fn nesting_depth(&self) -> usize {
        self.depth
    }

    pub(crate) fn register_ids(&self, ids: &mut BTreeSet<RepeatId>) -> Result<(), RepeatId> {
        if !ids.insert(self.id.clone()) {
            return Err(self.id.clone());
        }
        if let Some(nested) = &self.nested {
            nested.register_ids(ids)?;
        }
        Ok(())
    }

    pub(crate) fn manifest_value(&self) -> Value {
        let mut value = json!({
            "id": self.id.as_str(),
            "interceptors": self
                .interceptors
                .iter()
                .map(RepeatInterceptorDefinition::manifest_value)
                .collect::<Vec<_>>(),
            "policy": self.policy.manifest_value(),
            "state": self.state.manifest_value()
        });
        if let Some(nested) = &self.nested
            && let Some(object) = value.as_object_mut()
        {
            object.insert("nested".to_owned(), nested.manifest_value());
        }
        value
    }

    fn contains_id(&self, id: &RepeatId) -> bool {
        &self.id == id
            || self
                .nested
                .as_deref()
                .is_some_and(|nested| nested.contains_id(id))
    }
}

/// Stable failures while constructing bounded repeat definition metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RepeatDefinitionError {
    /// One repeat declared more interceptors than the accepted bound.
    TooManyInterceptors {
        /// Maximum accepted interceptor count.
        max: usize,
    },
    /// Two interceptor registrations reused one stable ID.
    DuplicateInterceptorId {
        /// Repeated interceptor ID.
        interceptor: RepeatInterceptorId,
    },
    /// Nested repeat wrappers exceeded the accepted depth.
    NestingTooDeep {
        /// Maximum accepted nesting depth.
        max: usize,
    },
    /// A repeat ID was reused in one nested chain.
    DuplicateRepeatId {
        /// Repeated repeat ID.
        repeat: RepeatId,
    },
}

impl fmt::Display for RepeatDefinitionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyInterceptors { max } => {
                write!(formatter, "repeat definition exceeds {max} interceptors")
            }
            Self::DuplicateInterceptorId { interceptor } => write!(
                formatter,
                "repeat interceptor {} is declared more than once",
                interceptor.as_str()
            ),
            Self::NestingTooDeep { max } => {
                write!(formatter, "repeat nesting exceeds depth {max}")
            }
            Self::DuplicateRepeatId { repeat } => write!(
                formatter,
                "repeat {} is declared more than once in one nesting chain",
                repeat.as_str()
            ),
        }
    }
}

impl Error for RepeatDefinitionError {}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::*;

    fn interceptor(id: &str) -> RepeatInterceptorDefinition {
        RepeatInterceptorDefinition::new(
            RepeatInterceptorId::new(id).expect("interceptor id"),
            RepeatInterceptorKind::new("audit").expect("interceptor kind"),
            ComponentRevision::new("v1").expect("revision"),
        )
    }

    fn leaf(id: &str, count: usize) -> Result<RepeatDefinition, RepeatDefinitionError> {
        RepeatDefinition::new(
            RepeatId::new(id).expect("repeat id"),
            RepeatPolicyDefinition::new(
                RepeatPolicyKind::new("count").expect("policy kind"),
                ComponentRevision::new("v1").expect("revision"),
                RepeatPolicyConfiguration::new("limit-10").expect("configuration"),
            ),
            (0..count)
                .map(|index| interceptor(&format!("i{index}")))
                .collect(),
            RepeatStateSchema::new(
                StateSchemaId::new("repeat.state").expect("schema"),
                StateSchemaVersion::new(1).expect("version"),
            ),
        )
    }

    #[test]
    fn interceptor_bound_is_exact() {
        assert!(leaf("accepted", MAX_REPEAT_INTERCEPTORS).is_ok());
        assert!(matches!(
            leaf("rejected", MAX_REPEAT_INTERCEPTORS + 1),
            Err(RepeatDefinitionError::TooManyInterceptors { .. })
        ));
    }

    #[test]
    fn nesting_bound_is_exact() {
        let mut current = leaf("r8", 0).expect("leaf");
        for depth in (1..8).rev() {
            current = leaf(&format!("r{depth}"), 0)
                .expect("wrapper")
                .with_nested(current)
                .expect("depth <= 8");
        }
        assert_eq!(current.nesting_depth(), MAX_REPEAT_NESTING_DEPTH);
        assert!(matches!(
            leaf("r0", 0).expect("wrapper").with_nested(current),
            Err(RepeatDefinitionError::NestingTooDeep { .. })
        ));
    }
}
