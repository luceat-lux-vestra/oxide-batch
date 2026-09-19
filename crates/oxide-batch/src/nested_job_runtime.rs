//! Deterministic typed parameter resolution for nested-job boundaries.
//!
//! Values remain inside the execution boundary. Failures expose only stable
//! categories; no mapped value is retained in diagnostics.

use std::error::Error;
use std::fmt;

use crate::structured_selector::{
    SelectorFailure, SourceValue, coerce, context_value, default_value, digest_hex,
    require_durable_context_source,
};
use crate::{
    CompiledExecutionPlan, ExecutionAttempt, FrameworkParameterSource, JobInstanceId, JobParameter,
    JobParameters, JobRepository, NestedJobNode, NestedJobParameterMapping,
    NestedJobParameterSource, ParameterValue, RepositoryError, RepositoryUnitOfWork,
};

/// Stable, value-redacted nested-job parameter-mapping failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum NestedJobMappingFailure {
    /// The selected durable source did not contain the declared value.
    MissingSource,
    /// The selected durable context source is unavailable from this repository.
    SourceUnavailable,
    /// A committed context used another schema identity or version.
    SourceSchemaMismatch,
    /// The selected value did not have the type required by the mapping.
    SourceTypeMismatch,
    /// The declared coercion could not represent the selected value.
    CoercionFailed,
    /// A bounded framework value could not be represented as a parameter.
    InvalidValue,
}

impl fmt::Display for NestedJobMappingFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::MissingSource => "nested-job mapping source is missing",
            Self::SourceUnavailable => "nested-job durable context source is unavailable",
            Self::SourceSchemaMismatch => "nested-job mapping source schema does not match",
            Self::SourceTypeMismatch => "nested-job mapping source type does not match",
            Self::CoercionFailed => "nested-job parameter coercion failed",
            Self::InvalidValue => "nested-job mapped parameter is invalid",
        })
    }
}

impl Error for NestedJobMappingFailure {}

#[derive(Debug)]
pub(crate) enum NestedJobResolutionError {
    Repository(RepositoryError),
    Mapping(NestedJobMappingFailure),
}

impl From<RepositoryError> for NestedJobResolutionError {
    fn from(error: RepositoryError) -> Self {
        Self::Repository(error)
    }
}

impl From<NestedJobMappingFailure> for NestedJobResolutionError {
    fn from(error: NestedJobMappingFailure) -> Self {
        Self::Mapping(error)
    }
}

impl From<SelectorFailure> for NestedJobMappingFailure {
    fn from(error: SelectorFailure) -> Self {
        match error {
            SelectorFailure::SourceUnavailable => Self::SourceUnavailable,
            SelectorFailure::SourceSchemaMismatch => Self::SourceSchemaMismatch,
            SelectorFailure::SourceTypeMismatch => Self::SourceTypeMismatch,
            SelectorFailure::CoercionFailed => Self::CoercionFailed,
            SelectorFailure::InvalidValue => Self::InvalidValue,
        }
    }
}

impl From<SelectorFailure> for NestedJobResolutionError {
    fn from(error: SelectorFailure) -> Self {
        Self::Mapping(error.into())
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn resolve_nested_job_parameters(
    repository: &dyn JobRepository,
    plan: &CompiledExecutionPlan,
    node: &NestedJobNode,
    parent_job_instance_id: JobInstanceId,
    parent_job_execution_id: crate::JobExecutionId,
    attempt: ExecutionAttempt,
    parent_parameters: &JobParameters,
) -> Result<JobParameters, NestedJobResolutionError> {
    let durable_context_sources = repository.descriptor().schema_version() != 0;
    let mut unit = repository.begin().await?;
    let mut resolved = JobParameters::new();
    let mut primary = None;

    for mapping in node.mappings() {
        let source = match resolve_source(
            unit.as_mut(),
            durable_context_sources,
            plan,
            node,
            mapping,
            parent_job_instance_id,
            parent_job_execution_id,
            attempt,
            parent_parameters,
        )
        .await
        {
            Ok(source) => source,
            Err(error) => {
                primary = Some(error);
                break;
            }
        };
        let value = match source {
            Some(source) => match coerce(source, mapping.coercion(), mapping.expected_kind()) {
                Ok(value) => value,
                Err(error) => {
                    primary = Some(error.into());
                    break;
                }
            },
            None => match mapping.missing_policy() {
                crate::MissingParameterPolicy::Fail => {
                    primary = Some(NestedJobMappingFailure::MissingSource.into());
                    break;
                }
                crate::MissingParameterPolicy::TypeDefault => {
                    match default_value(mapping.expected_kind()) {
                        Ok(value) => value,
                        Err(error) => {
                            primary = Some(error.into());
                            break;
                        }
                    }
                }
                _ => {
                    primary = Some(NestedJobMappingFailure::InvalidValue.into());
                    break;
                }
            },
        };
        if resolved
            .insert(
                mapping.target().clone(),
                JobParameter::new(value, mapping.role()),
            )
            .is_err()
        {
            primary = Some(NestedJobMappingFailure::InvalidValue.into());
            break;
        }
    }

    let rollback = unit.rollback().await;
    if let Err(error) = rollback {
        return Err(error.into());
    }
    match primary {
        Some(error) => Err(error),
        None => Ok(resolved),
    }
}

#[allow(clippy::too_many_arguments)]
async fn resolve_source(
    unit: &mut dyn RepositoryUnitOfWork,
    durable_context_sources: bool,
    plan: &CompiledExecutionPlan,
    node: &NestedJobNode,
    mapping: &NestedJobParameterMapping,
    parent_job_instance_id: JobInstanceId,
    parent_job_execution_id: crate::JobExecutionId,
    attempt: ExecutionAttempt,
    parent_parameters: &JobParameters,
) -> Result<Option<SourceValue>, NestedJobResolutionError> {
    match mapping.source() {
        NestedJobParameterSource::ParentParameter(name) => Ok(parent_parameters
            .get(name)
            .map(|parameter| SourceValue::Parameter(parameter.value().clone()))),
        NestedJobParameterSource::ParentJobContext {
            schema,
            schema_version,
            path,
        } => {
            require_durable_context_source(durable_context_sources)?;
            let Some(context) = unit.job_execution_context(parent_job_execution_id).await? else {
                return Ok(None);
            };
            Ok(context_value(&context, schema, *schema_version, path)?)
        }
        NestedJobParameterSource::ParentStepContext {
            node: source_node,
            schema,
            schema_version,
            path,
        } => {
            require_durable_context_source(durable_context_sources)?;
            let Some(state) = unit
                .latest_flow_step(parent_job_instance_id, source_node)
                .await?
            else {
                return Ok(None);
            };
            let Some(context) = state.context() else {
                return Ok(None);
            };
            Ok(context_value(context, schema, *schema_version, path)?)
        }
        NestedJobParameterSource::Framework(source) => {
            let value = match source {
                FrameworkParameterSource::ParentJobInstanceId => {
                    ParameterValue::from(parent_job_instance_id.get())
                }
                FrameworkParameterSource::ParentJobExecutionId => {
                    ParameterValue::from(parent_job_execution_id.get())
                }
                FrameworkParameterSource::ParentAttempt => ParameterValue::from(attempt.get()),
                FrameworkParameterSource::ParentDefinitionRevision => ParameterValue::string(
                    plan.definition_identity().revision().as_str().to_owned(),
                )
                .map_err(|_| NestedJobMappingFailure::InvalidValue)?,
                FrameworkParameterSource::ParentDefinitionFingerprint => {
                    ParameterValue::string(digest_hex(plan.fingerprint()))
                        .map_err(|_| NestedJobMappingFailure::InvalidValue)?
                }
                FrameworkParameterSource::NestedJobNodeId => {
                    ParameterValue::string(node.id().as_str().to_owned())
                        .map_err(|_| NestedJobMappingFailure::InvalidValue)?
                }
                _ => return Err(NestedJobMappingFailure::InvalidValue.into()),
            };
            Ok(Some(SourceValue::Parameter(value)))
        }
        _ => Err(NestedJobMappingFailure::InvalidValue.into()),
    }
}
