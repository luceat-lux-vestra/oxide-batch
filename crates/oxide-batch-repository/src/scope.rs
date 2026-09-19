//! Bounded, value-free provenance for scoped late-bound resolution.
//!
//! Provenance identifies which committed framework record supplied an input.
//! It deliberately contains no resolved parameter/context value and no digest
//! derived from such a value.

use std::error::Error;
use std::fmt;

use oxide_batch_core::{
    ExecutionVersion, JobExecutionId, NodeId, ParameterName, ScopeFrameworkSource, ScopeKind,
    ScopedComponentId, StateSchemaId, StateSchemaVersion, StepExecutionId, selector_path_is_valid,
};

/// Durable source family recorded for one late-bound input.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum ScopeResolutionSourceKind {
    /// Immutable parameters owned by one job-execution attempt.
    JobParameter,
    /// Committed job-execution context.
    JobContext,
    /// Committed step-execution context.
    StepContext,
    /// Closed framework metadata.
    Framework,
}

impl ScopeResolutionSourceKind {
    /// Returns the stable durable code.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::JobParameter => "job_parameter",
            Self::JobContext => "job_context",
            Self::StepContext => "step_context",
            Self::Framework => "framework",
        }
    }
}

/// Authoritative committed source identity for one late-bound input.
///
/// No variant can carry a resolved value. Context variants retain only schema,
/// structured path, and the execution/version that owns the committed payload.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ScopeResolutionSource {
    /// Immutable job parameter owned by an execution attempt.
    JobParameter {
        /// Attempt that owns the complete immutable parameter set.
        job_execution_id: JobExecutionId,
        /// Selected parameter name.
        parameter: ParameterName,
    },
    /// Committed job context owned by an execution version.
    JobContext {
        /// Attempt whose context is authoritative.
        job_execution_id: JobExecutionId,
        /// Optimistic version observed with the committed context.
        execution_version: ExecutionVersion,
        /// Required application schema.
        schema: StateSchemaId,
        /// Required application schema version.
        schema_version: StateSchemaVersion,
        /// Validated structured path.
        path: Vec<String>,
    },
    /// Committed step context owned by a step execution/version.
    StepContext {
        /// Logical source node declared by the plan.
        node: NodeId,
        /// Durable step attempt that owns the committed context, when one existed.
        step_execution_id: Option<StepExecutionId>,
        /// Optimistic version observed with the committed context, paired with the step ID.
        execution_version: Option<ExecutionVersion>,
        /// Required application schema.
        schema: StateSchemaId,
        /// Required application schema version.
        schema_version: StateSchemaVersion,
        /// Validated structured path.
        path: Vec<String>,
    },
    /// Closed framework metadata derived from durable execution/definition identity.
    Framework {
        /// Closed metadata field.
        source: ScopeFrameworkSource,
        /// Owning job attempt.
        job_execution_id: JobExecutionId,
        /// Owning step attempt for step-scoped metadata.
        step_execution_id: Option<StepExecutionId>,
    },
}

impl ScopeResolutionSource {
    /// Constructs a validated job-context source reference.
    ///
    /// # Errors
    ///
    /// Rejects an invalid structured selector path.
    pub fn job_context(
        job_execution_id: JobExecutionId,
        execution_version: ExecutionVersion,
        schema: StateSchemaId,
        schema_version: StateSchemaVersion,
        path: Vec<String>,
    ) -> Result<Self, ScopeResolutionProvenanceError> {
        if !selector_path_is_valid(&path) {
            return Err(ScopeResolutionProvenanceError::InvalidPath);
        }
        Ok(Self::JobContext {
            job_execution_id,
            execution_version,
            schema,
            schema_version,
            path,
        })
    }

    /// Constructs a validated step-context source reference.
    ///
    /// # Errors
    ///
    /// Rejects an invalid structured selector path.
    pub fn step_context(
        node: NodeId,
        step_execution_id: Option<StepExecutionId>,
        execution_version: Option<ExecutionVersion>,
        schema: StateSchemaId,
        schema_version: StateSchemaVersion,
        path: Vec<String>,
    ) -> Result<Self, ScopeResolutionProvenanceError> {
        if !selector_path_is_valid(&path) {
            return Err(ScopeResolutionProvenanceError::InvalidPath);
        }
        if step_execution_id.is_some() != execution_version.is_some() {
            return Err(ScopeResolutionProvenanceError::InvalidSource);
        }
        Ok(Self::StepContext {
            node,
            step_execution_id,
            execution_version,
            schema,
            schema_version,
            path,
        })
    }

    /// Returns the durable source-family code.
    #[must_use]
    pub const fn kind(&self) -> ScopeResolutionSourceKind {
        match self {
            Self::JobParameter { .. } => ScopeResolutionSourceKind::JobParameter,
            Self::JobContext { .. } => ScopeResolutionSourceKind::JobContext,
            Self::StepContext { .. } => ScopeResolutionSourceKind::StepContext,
            Self::Framework { .. } => ScopeResolutionSourceKind::Framework,
        }
    }

    fn validate_shape(&self) -> Result<(), ScopeResolutionProvenanceError> {
        match self {
            Self::JobParameter { .. } | Self::Framework { .. } => Ok(()),
            Self::JobContext { path, .. } => {
                if selector_path_is_valid(path) {
                    Ok(())
                } else {
                    Err(ScopeResolutionProvenanceError::InvalidPath)
                }
            }
            Self::StepContext {
                step_execution_id,
                execution_version,
                path,
                ..
            } => {
                if !selector_path_is_valid(path) {
                    return Err(ScopeResolutionProvenanceError::InvalidPath);
                }
                if step_execution_id.is_some() != execution_version.is_some() {
                    return Err(ScopeResolutionProvenanceError::InvalidSource);
                }
                Ok(())
            }
        }
    }
}

/// One durable, value-free input provenance record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScopeResolutionProvenance {
    scope: ScopeKind,
    component: ScopedComponentId,
    input: ParameterName,
    owner_job_execution_id: JobExecutionId,
    owner_step_execution_id: Option<StepExecutionId>,
    source: ScopeResolutionSource,
}

impl ScopeResolutionProvenance {
    /// Constructs one validated provenance entry.
    ///
    /// # Errors
    ///
    /// Rejects a job scope with a step owner or a step scope without one.
    pub fn new(
        scope: ScopeKind,
        component: ScopedComponentId,
        input: ParameterName,
        owner_job_execution_id: JobExecutionId,
        owner_step_execution_id: Option<StepExecutionId>,
        source: ScopeResolutionSource,
    ) -> Result<Self, ScopeResolutionProvenanceError> {
        let owner_shape_valid = match scope {
            ScopeKind::Job => owner_step_execution_id.is_none(),
            ScopeKind::Step => owner_step_execution_id.is_some(),
            _ => false,
        };
        if !owner_shape_valid {
            return Err(ScopeResolutionProvenanceError::InvalidOwner);
        }
        source.validate_shape()?;
        let source_owner_valid = match &source {
            ScopeResolutionSource::JobParameter { .. }
            | ScopeResolutionSource::JobContext { .. }
            | ScopeResolutionSource::StepContext { .. } => true,
            ScopeResolutionSource::Framework {
                job_execution_id,
                step_execution_id,
                ..
            } => {
                *job_execution_id == owner_job_execution_id
                    && *step_execution_id == owner_step_execution_id
            }
        };
        if !source_owner_valid {
            return Err(ScopeResolutionProvenanceError::InvalidSource);
        }
        Ok(Self {
            scope,
            component,
            input,
            owner_job_execution_id,
            owner_step_execution_id,
            source,
        })
    }

    /// Returns the execution scope kind.
    #[must_use]
    pub const fn scope(&self) -> ScopeKind {
        self.scope
    }

    /// Borrows the stable logical component identity.
    #[must_use]
    pub const fn component(&self) -> &ScopedComponentId {
        &self.component
    }

    /// Borrows the late-bound input name.
    #[must_use]
    pub const fn input(&self) -> &ParameterName {
        &self.input
    }

    /// Returns the owning job attempt.
    #[must_use]
    pub const fn owner_job_execution_id(&self) -> JobExecutionId {
        self.owner_job_execution_id
    }

    /// Returns the owning step attempt for a step scope.
    #[must_use]
    pub const fn owner_step_execution_id(&self) -> Option<StepExecutionId> {
        self.owner_step_execution_id
    }

    /// Borrows the authoritative source identity.
    #[must_use]
    pub const fn source(&self) -> &ScopeResolutionSource {
        &self.source
    }
}

/// Stable validation failure for value-free provenance metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ScopeResolutionProvenanceError {
    /// Scope kind and owner execution shape disagree.
    InvalidOwner,
    /// Structured selector path violates the shared bound.
    InvalidPath,
    /// Durable source identity/version shape is contradictory.
    InvalidSource,
}

impl fmt::Display for ScopeResolutionProvenanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidOwner => formatter.write_str("scope provenance owner is invalid"),
            Self::InvalidPath => formatter.write_str("scope provenance selector path is invalid"),
            Self::InvalidSource => {
                formatter.write_str("scope provenance source identity is invalid")
            }
        }
    }
}

impl Error for ScopeResolutionProvenanceError {}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use oxide_batch_core::{
        ExecutionVersion, JobExecutionId, NodeId, ParameterName, ScopeFrameworkSource, ScopeKind,
        ScopedComponentId, StateSchemaId, StateSchemaVersion, StepExecutionId,
    };

    use super::{ScopeResolutionProvenance, ScopeResolutionProvenanceError, ScopeResolutionSource};

    fn job_id(value: u64) -> JobExecutionId {
        JobExecutionId::new(value).expect("job id")
    }

    fn step_id(value: u64) -> StepExecutionId {
        StepExecutionId::new(value).expect("step id")
    }

    fn provenance(
        owner_job: JobExecutionId,
        owner_step: Option<StepExecutionId>,
        source: ScopeResolutionSource,
    ) -> Result<ScopeResolutionProvenance, ScopeResolutionProvenanceError> {
        ScopeResolutionProvenance::new(
            if owner_step.is_some() {
                ScopeKind::Step
            } else {
                ScopeKind::Job
            },
            ScopedComponentId::new("client").expect("component"),
            ParameterName::new("tenant").expect("input"),
            owner_job,
            owner_step,
            source,
        )
    }

    #[test]
    fn direct_source_construction_cannot_bypass_provenance_validation() {
        let owner_job = job_id(1);
        let malformed_path = ScopeResolutionSource::JobContext {
            job_execution_id: owner_job,
            execution_version: ExecutionVersion::new(1),
            schema: StateSchemaId::new("scope.job").expect("schema"),
            schema_version: StateSchemaVersion::new(1).expect("schema version"),
            path: vec![String::new()],
        };
        assert_eq!(
            provenance(owner_job, None, malformed_path),
            Err(ScopeResolutionProvenanceError::InvalidPath)
        );

        let contradictory_step = ScopeResolutionSource::StepContext {
            node: NodeId::new("source").expect("node"),
            step_execution_id: Some(step_id(2)),
            execution_version: None,
            schema: StateSchemaId::new("scope.step").expect("schema"),
            schema_version: StateSchemaVersion::new(1).expect("schema version"),
            path: vec![String::from("tenant")],
        };
        assert_eq!(
            provenance(owner_job, Some(step_id(3)), contradictory_step),
            Err(ScopeResolutionProvenanceError::InvalidSource)
        );
    }

    #[test]
    fn historical_sources_are_allowed_but_framework_metadata_stays_owner_bound() {
        let owner_job = job_id(1);
        let historical_parameter = ParameterName::new("tenant").expect("parameter");
        assert!(
            provenance(
                owner_job,
                None,
                ScopeResolutionSource::JobParameter {
                    job_execution_id: job_id(2),
                    parameter: historical_parameter,
                },
            )
            .is_ok(),
            "restart provenance may reference an earlier committed parameter set",
        );

        assert_eq!(
            provenance(
                owner_job,
                Some(step_id(3)),
                ScopeResolutionSource::Framework {
                    source: ScopeFrameworkSource::Attempt,
                    job_execution_id: owner_job,
                    step_execution_id: Some(step_id(4)),
                },
            ),
            Err(ScopeResolutionProvenanceError::InvalidSource)
        );
    }
}
