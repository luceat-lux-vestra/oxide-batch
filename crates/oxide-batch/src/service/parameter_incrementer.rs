//! Bounded typed M7 parameter-incrementer application boundary.
use crate::{
    DefinitionIdentity, DefinitionRevision, JobInstanceKey, JobParameters, ParameterName,
    ParameterRole, ParameterValueKind,
};
use std::error::Error;
use std::fmt;
const MAX_INCREMENTER_FIELDS: usize = 128;

/// A declared output parameter name, type and identity role.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncrementerField {
    name: ParameterName,
    kind: ParameterValueKind,
    role: ParameterRole,
}
impl IncrementerField {
    /// Declares one reviewed output field.
    #[must_use]
    pub const fn new(name: ParameterName, kind: ParameterValueKind, role: ParameterRole) -> Self {
        Self { name, kind, role }
    }
    /// Borrows the name.
    #[must_use]
    pub const fn name(&self) -> &ParameterName {
        &self.name
    }
    /// Returns the typed value kind.
    #[must_use]
    pub const fn kind(&self) -> ParameterValueKind {
        self.kind
    }
    /// Returns the identifying role.
    #[must_use]
    pub const fn role(&self) -> ParameterRole {
        self.role
    }
}

/// Redacted application and validation failure categories.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum IncrementerError {
    /// Target definition lacks a named job.
    InvalidDefinition,
    /// Output shape exceeds its hard cap.
    TooManyFields,
    /// Repeated output field name.
    DuplicateField,
    /// Output name/kind/role differs from declaration.
    InvalidOutputShape,
    /// Existing input parameter silently changed role or kind.
    InputRoleOrKindDrift,
    /// Application explicitly rejected the input.
    ApplicationRejected,
}
impl fmt::Display for IncrementerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidDefinition => f.write_str("incrementer requires a named definition"),
            Self::TooManyFields => f.write_str("incrementer output exceeds its field bound"),
            Self::DuplicateField => f.write_str("incrementer output declares a duplicate field"),
            Self::InvalidOutputShape => f.write_str("incrementer output shape is invalid"),
            Self::InputRoleOrKindDrift => {
                f.write_str("incrementer changed a previous role or kind")
            }
            Self::ApplicationRejected => f.write_str("incrementer application rejected the input"),
        }
    }
}
impl Error for IncrementerError {}

/// Reviewed kind/revision and callback, with no captured application state.
///
/// The callback receives only the target definition and explicitly supplied
/// previous typed parameters. The caller must enforce purity and ensure its
/// kind/revision are encoded in the target definition restart fingerprint.
/// No repository query, launch or implicit retry occurs here.
#[derive(Clone)]
pub struct ParameterIncrementer {
    kind: DefinitionRevision,
    revision: DefinitionRevision,
    output_shape: Vec<IncrementerField>,
    apply: fn(&DefinitionIdentity, &JobParameters) -> Result<JobParameters, IncrementerError>,
}
impl fmt::Debug for ParameterIncrementer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParameterIncrementer")
            .field("kind", &self.kind)
            .field("revision", &self.revision)
            .field("output_field_count", &self.output_shape.len())
            .finish_non_exhaustive()
    }
}
impl ParameterIncrementer {
    /// Builds an immutable bounded output contract.
    ///
    /// # Errors
    /// Rejects excess or duplicate output fields before invoking application code.
    pub fn new(
        kind: DefinitionRevision,
        revision: DefinitionRevision,
        mut output_shape: Vec<IncrementerField>,
        apply: fn(&DefinitionIdentity, &JobParameters) -> Result<JobParameters, IncrementerError>,
    ) -> Result<Self, IncrementerError> {
        if output_shape.len() > MAX_INCREMENTER_FIELDS {
            return Err(IncrementerError::TooManyFields);
        }
        output_shape.sort_by(|left, right| left.name.cmp(&right.name));
        if output_shape
            .windows(2)
            .any(|pair| pair[0].name == pair[1].name)
        {
            return Err(IncrementerError::DuplicateField);
        }
        Ok(Self {
            kind,
            revision,
            output_shape,
            apply,
        })
    }
    /// Borrows the reviewed kind.
    #[must_use]
    pub const fn kind(&self) -> &DefinitionRevision {
        &self.kind
    }
    /// Borrows the reviewed revision.
    #[must_use]
    pub const fn revision(&self) -> &DefinitionRevision {
        &self.revision
    }
    /// Computes exactly one candidate from explicit prior parameters.
    ///
    /// No retry or automatic sequence bump occurs for unchanged instance identity.
    ///
    /// # Errors
    /// Fails closed on invalid definition, callback rejection, output mismatch,
    /// or mutation of a previous parameter's role or type.
    pub fn increment(
        &self,
        target: &DefinitionIdentity,
        prior: &JobParameters,
    ) -> Result<IncrementedParameters, IncrementerError> {
        let job_name = target
            .job_name()
            .ok_or(IncrementerError::InvalidDefinition)?;
        if prior.len() > MAX_INCREMENTER_FIELDS {
            return Err(IncrementerError::TooManyFields);
        }
        let output = (self.apply)(target, prior)?;
        if output.len() != self.output_shape.len() {
            return Err(IncrementerError::InvalidOutputShape);
        }
        for field in &self.output_shape {
            let next = output
                .get(&field.name)
                .ok_or(IncrementerError::InvalidOutputShape)?;
            if next.value().kind() != field.kind || next.role() != field.role {
                return Err(IncrementerError::InvalidOutputShape);
            }
        }
        for (name, previous) in prior.iter() {
            if let Some(next) = output.get(name)
                && (previous.role() != next.role()
                    || previous.value().kind() != next.value().kind())
            {
                return Err(IncrementerError::InputRoleOrKindDrift);
            }
        }
        let before = JobInstanceKey::new(job_name.clone(), prior);
        let target_key = JobInstanceKey::new(job_name.clone(), &output);
        let identifying_changed = before != target_key;
        Ok(IncrementedParameters {
            parameters: output,
            target_key,
            identifying_changed,
        })
    }
}

/// One typed result without payload-bearing Debug output.
#[derive(Clone, Eq, PartialEq)]
pub struct IncrementedParameters {
    parameters: JobParameters,
    target_key: JobInstanceKey,
    identifying_changed: bool,
}
impl fmt::Debug for IncrementedParameters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IncrementedParameters")
            .field("parameter_count", &self.parameters.len())
            .field("identifying_changed", &self.identifying_changed)
            .finish_non_exhaustive()
    }
}
impl IncrementedParameters {
    /// Borrows the candidate parameters for the authorized launch boundary.
    #[must_use]
    pub const fn parameters(&self) -> &JobParameters {
        &self.parameters
    }
    /// Borrows the recomputed instance identity.
    #[must_use]
    pub const fn target_key(&self) -> &JobInstanceKey {
        &self.target_key
    }
    /// Whether identifying fields choose a different logical instance.
    #[must_use]
    pub const fn identifying_changed(&self) -> bool {
        self.identifying_changed
    }
}
