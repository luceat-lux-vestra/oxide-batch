use oxide_batch_core::{
    ComponentRevision, DefinitionError, DefinitionTokenKind, FaultPolicy, NodeId, StartControls,
    StateSchemaId, StateSchemaVersion, StepName, definition_token, validate_token,
};
use serde_json::{Value, json};

use super::{fault_manifest_value, start_controls_manifest};

definition_token!(
    CustomLeafKind,
    DefinitionTokenKind::Component,
    "A stable application-owned kind identifier for one registered custom leaf."
);

/// One bounded registered custom-leaf declaration in a format-4 flow.
///
/// The declaration contains only restart-relevant meaning. Runtime locations,
/// credentials, handler objects, repository handles, and deployment policy are
/// deliberately absent from the definition fingerprint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CustomLeafNode {
    id: NodeId,
    step_name: StepName,
    kind: CustomLeafKind,
    handler_revision: ComponentRevision,
    state_schema_id: StateSchemaId,
    state_schema_version: StateSchemaVersion,
    start: StartControls,
    fault: Option<FaultPolicy>,
    listeners: Vec<ComponentRevision>,
}

impl CustomLeafNode {
    /// Declares one custom leaf with its exact handler and durable-state identity.
    #[must_use]
    pub const fn new(
        id: NodeId,
        step_name: StepName,
        kind: CustomLeafKind,
        handler_revision: ComponentRevision,
        state_schema_id: StateSchemaId,
        state_schema_version: StateSchemaVersion,
    ) -> Self {
        Self {
            id,
            step_name,
            kind,
            handler_revision,
            state_schema_id,
            state_schema_version,
            start: StartControls::new(oxide_batch_core::StartLimit::UNRESTRICTED, false),
            fault: None,
            listeners: Vec::new(),
        }
    }

    /// Applies restart-relevant start controls.
    #[must_use]
    pub const fn with_start_controls(mut self, start: StartControls) -> Self {
        self.start = start;
        self
    }

    /// Applies the framework fault policy executed around this leaf.
    #[must_use]
    pub fn with_fault_policy(mut self, fault: FaultPolicy) -> Self {
        self.fault = Some(fault);
        self
    }

    /// Adds one framework step-listener revision in deterministic before-order.
    #[must_use]
    pub fn with_listener_revision(mut self, listener: ComponentRevision) -> Self {
        self.listeners.push(listener);
        self
    }

    /// Borrows the stable logical node identifier.
    #[must_use]
    pub const fn id(&self) -> &NodeId {
        &self.id
    }

    /// Borrows the framework-owned durable step name.
    #[must_use]
    pub const fn step_name(&self) -> &StepName {
        &self.step_name
    }

    /// Borrows the stable custom-leaf kind selected at registration.
    #[must_use]
    pub const fn kind(&self) -> &CustomLeafKind {
        &self.kind
    }

    /// Borrows the exact executable handler revision.
    #[must_use]
    pub const fn handler_revision(&self) -> &ComponentRevision {
        &self.handler_revision
    }

    /// Borrows the durable custom-state schema identifier.
    #[must_use]
    pub const fn state_schema_id(&self) -> &StateSchemaId {
        &self.state_schema_id
    }

    /// Returns the durable custom-state schema version.
    #[must_use]
    pub const fn state_schema_version(&self) -> StateSchemaVersion {
        self.state_schema_version
    }

    /// Returns the framework start controls.
    #[must_use]
    pub const fn start_controls(&self) -> StartControls {
        self.start
    }

    /// Borrows the optional framework fault policy.
    #[must_use]
    pub const fn fault_policy(&self) -> Option<&FaultPolicy> {
        self.fault.as_ref()
    }

    /// Borrows registered framework listener revisions.
    #[must_use]
    pub fn listener_revisions(&self) -> &[ComponentRevision] {
        &self.listeners
    }

    pub(super) fn manifest_value(&self) -> Value {
        json!({
            "handler_revision": self.handler_revision.as_str(),
            "id": self.id.as_str(),
            "kind": "custom_leaf",
            "leaf_kind": self.kind.as_str(),
            "listeners": self.listeners.iter().map(ComponentRevision::as_str).collect::<Vec<_>>(),
            "policy": self.fault.as_ref().map_or(Value::Null, fault_manifest_value),
            "start": start_controls_manifest(self.start),
            "state_schema": {
                "id": self.state_schema_id.as_str(),
                "version": self.state_schema_version.get()
            },
            "step_name": self.step_name.as_str()
        })
    }
}
