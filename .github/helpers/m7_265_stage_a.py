from pathlib import Path

nested_runtime = r'''//! Deterministic typed parameter resolution for nested-job boundaries.
//!
//! Values remain inside the execution boundary. Failures expose only stable
//! categories; no mapped value is retained in diagnostics.

use std::error::Error;
use std::fmt;

use serde_json::Value;

use crate::{
    CompiledExecutionPlan, ExecutionAttempt, FrameworkParameterSource, JobInstanceId,
    JobParameter, JobParameters, JobRepository, NestedJobNode, NestedJobParameterMapping,
    NestedJobParameterSource, ParameterCoercion, ParameterValue, ParameterValueKind,
    RepositoryError, RepositoryUnitOfWork,
};

/// Stable, value-redacted nested-job parameter-mapping failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum NestedJobMappingFailure {
    /// The selected durable source did not contain the declared value.
    MissingSource,
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

enum SourceValue {
    Parameter(ParameterValue),
    Json(Value),
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
    let mut unit = repository.begin().await?;
    let mut resolved = JobParameters::new();
    let mut primary = None;

    for mapping in node.mappings() {
        let source = match resolve_source(
            unit.as_mut(),
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
            let Some(context) = unit.job_execution_context(parent_job_execution_id).await? else {
                return Ok(None);
            };
            context_value(&context, schema, *schema_version, path)
        }
        NestedJobParameterSource::ParentStepContext {
            node: source_node,
            schema,
            schema_version,
            path,
        } => {
            let Some(state) = unit
                .latest_flow_step(parent_job_instance_id, source_node)
                .await?
            else {
                return Ok(None);
            };
            let Some(context) = state.context() else {
                return Ok(None);
            };
            context_value(context, schema, *schema_version, path)
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

fn context_value(
    context: &crate::ExecutionContext,
    schema: &crate::StateSchemaId,
    schema_version: crate::StateSchemaVersion,
    path: &crate::SelectorPath,
) -> Result<Option<SourceValue>, NestedJobResolutionError> {
    if context.schema_id() != schema || context.schema_version() != schema_version {
        return Err(NestedJobMappingFailure::SourceSchemaMismatch.into());
    }
    let bytes = context
        .payload_json()
        .map_err(|_| NestedJobMappingFailure::InvalidValue)?;
    let value: Value =
        serde_json::from_slice(&bytes).map_err(|_| NestedJobMappingFailure::InvalidValue)?;
    let mut selected = &value;
    for segment in path.segments() {
        let Some(next) = selected.as_object().and_then(|object| object.get(segment)) else {
            return Ok(None);
        };
        selected = next;
    }
    Ok(Some(SourceValue::Json(selected.clone())))
}

fn coerce(
    source: SourceValue,
    coercion: ParameterCoercion,
    expected: ParameterValueKind,
) -> Result<ParameterValue, NestedJobMappingFailure> {
    let value = match coercion {
        ParameterCoercion::Exact => exact(source, expected)?,
        ParameterCoercion::StringToI64 => {
            let value = source_string(&source)?;
            ParameterValue::from(
                value
                    .parse::<i64>()
                    .map_err(|_| NestedJobMappingFailure::CoercionFailed)?,
            )
        }
        ParameterCoercion::StringToU64 => {
            let value = source_string(&source)?;
            ParameterValue::from(
                value
                    .parse::<u64>()
                    .map_err(|_| NestedJobMappingFailure::CoercionFailed)?,
            )
        }
        ParameterCoercion::StringToBool => {
            let value = match source_string(&source)? {
                "true" => true,
                "false" => false,
                _ => return Err(NestedJobMappingFailure::CoercionFailed),
            };
            ParameterValue::from(value)
        }
        ParameterCoercion::I64ToU64 => {
            let value = source_i64(&source)?;
            ParameterValue::from(
                u64::try_from(value).map_err(|_| NestedJobMappingFailure::CoercionFailed)?,
            )
        }
        ParameterCoercion::U64ToI64 => {
            let value = source_u64(&source)?;
            ParameterValue::from(
                i64::try_from(value).map_err(|_| NestedJobMappingFailure::CoercionFailed)?,
            )
        }
        _ => return Err(NestedJobMappingFailure::CoercionFailed),
    };
    if value.kind() != expected {
        return Err(NestedJobMappingFailure::SourceTypeMismatch);
    }
    Ok(value)
}

fn exact(
    source: SourceValue,
    expected: ParameterValueKind,
) -> Result<ParameterValue, NestedJobMappingFailure> {
    match source {
        SourceValue::Parameter(value) if value.kind() == expected => Ok(value),
        SourceValue::Parameter(_) => Err(NestedJobMappingFailure::SourceTypeMismatch),
        SourceValue::Json(value) => match expected {
            ParameterValueKind::String => value
                .as_str()
                .ok_or(NestedJobMappingFailure::SourceTypeMismatch)
                .and_then(|value| {
                    ParameterValue::string(value.to_owned())
                        .map_err(|_| NestedJobMappingFailure::InvalidValue)
                }),
            ParameterValueKind::I64 => value
                .as_i64()
                .map(ParameterValue::from)
                .ok_or(NestedJobMappingFailure::SourceTypeMismatch),
            ParameterValueKind::U64 => value
                .as_u64()
                .map(ParameterValue::from)
                .ok_or(NestedJobMappingFailure::SourceTypeMismatch),
            ParameterValueKind::Bool => value
                .as_bool()
                .map(ParameterValue::from)
                .ok_or(NestedJobMappingFailure::SourceTypeMismatch),
            _ => Err(NestedJobMappingFailure::SourceTypeMismatch),
        },
    }
}

fn source_string(source: &SourceValue) -> Result<&str, NestedJobMappingFailure> {
    match source {
        SourceValue::Parameter(value) => value
            .as_str()
            .ok_or(NestedJobMappingFailure::SourceTypeMismatch),
        SourceValue::Json(value) => value
            .as_str()
            .ok_or(NestedJobMappingFailure::SourceTypeMismatch),
    }
}

fn source_i64(source: &SourceValue) -> Result<i64, NestedJobMappingFailure> {
    match source {
        SourceValue::Parameter(value) => value
            .as_i64()
            .ok_or(NestedJobMappingFailure::SourceTypeMismatch),
        SourceValue::Json(value) => value
            .as_i64()
            .ok_or(NestedJobMappingFailure::SourceTypeMismatch),
    }
}

fn source_u64(source: &SourceValue) -> Result<u64, NestedJobMappingFailure> {
    match source {
        SourceValue::Parameter(value) => value
            .as_u64()
            .ok_or(NestedJobMappingFailure::SourceTypeMismatch),
        SourceValue::Json(value) => value
            .as_u64()
            .ok_or(NestedJobMappingFailure::SourceTypeMismatch),
    }
}

fn default_value(kind: ParameterValueKind) -> Result<ParameterValue, NestedJobMappingFailure> {
    match kind {
        ParameterValueKind::String => ParameterValue::string(String::new())
            .map_err(|_| NestedJobMappingFailure::InvalidValue),
        ParameterValueKind::I64 => Ok(ParameterValue::from(0_i64)),
        ParameterValueKind::U64 => Ok(ParameterValue::from(0_u64)),
        ParameterValueKind::Bool => Ok(ParameterValue::from(false)),
        _ => Err(NestedJobMappingFailure::InvalidValue),
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
'''

Path('crates/oxide-batch/src/nested_job_runtime.rs').write_text(nested_runtime)

lib = Path('crates/oxide-batch/src/lib.rs')
s = lib.read_text()
s = s.replace('mod listener;\nmod repository;', 'mod listener;\nmod nested_job_runtime;\nmod repository;')
s = s.replace('pub use listener::{\n', 'pub use nested_job_runtime::NestedJobMappingFailure;\npub use listener::{\n')
lib.write_text(s)

repo_flow = Path('crates/oxide-batch-repository/src/flow.rs')
s = repo_flow.read_text()
s = s.replace(
    '    /// A bounded local split joined durable branch results in declared order.\n    SplitAggregate,\n',
    '    /// A bounded local split joined durable branch results in declared order.\n    SplitAggregate,\n    /// A separately durable nested child completed and exposed its committed exit.\n    NestedJobExit,\n',
)
s = s.replace(
    '            Self::SplitAggregate => "SPLIT_AGGREGATE",\n',
    '            Self::SplitAggregate => "SPLIT_AGGREGATE",\n            Self::NestedJobExit => "NESTED_JOB_EXIT",\n',
)
s = s.replace(
    '            "SPLIT_AGGREGATE" => Some(Self::SplitAggregate),\n',
    '            "SPLIT_AGGREGATE" => Some(Self::SplitAggregate),\n            "NESTED_JOB_EXIT" => Some(Self::NestedJobExit),\n',
)
repo_flow.write_text(s)

flow = Path('crates/oxide-batch/src/flow.rs')
s = flow.read_text()
s = s.replace(
    '    StopToken, TaskletContext, TaskletExecutionOutcome, TaskletFailure, TaskletOutcome,\n    TaskletStep, TerminalKind,\n',
    '    StopToken, TaskletContext, TaskletExecutionOutcome, TaskletFailure, TaskletJob, TaskletOutcome,\n    TaskletStep, TerminalKind,\n',
)
s = s.replace(
    '                    FlowTransitionKind::SplitAggregate => kind == Some("join"),\n',
    '                    FlowTransitionKind::SplitAggregate => kind == Some("join"),\n                    FlowTransitionKind::NestedJobExit => kind == Some("nested_job"),\n',
)
marker = '#[derive(Clone, Debug)]\nstruct PartitionedTaskletBinding {'
nested = '''#[derive(Clone)]
enum NestedJobBinding {
    Tasklet(Arc<TaskletJob>),
}

impl NestedJobBinding {
    fn definition_identity(&self) -> &crate::DefinitionIdentity {
        match self {
            Self::Tasklet(job) => job.definition_identity(),
        }
    }
}

impl fmt::Debug for NestedJobBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NestedJobBinding")
            .field("definition", self.definition_identity())
            .finish_non_exhaustive()
    }
}

'''
s = s.replace(marker, nested + marker)
s = s.replace(
    '    partitioned_tasklets: BTreeMap<NodeId, PartitionedTaskletBinding>,\n}',
    '    partitioned_tasklets: BTreeMap<NodeId, PartitionedTaskletBinding>,\n    nested_jobs: BTreeMap<NodeId, NestedJobBinding>,\n}',
)
s = s.replace(
    '            .field(\n                "partitioned_tasklet_count",\n                &self.partitioned_tasklets.len(),\n            )\n            .finish()',
    '            .field(\n                "partitioned_tasklet_count",\n                &self.partitioned_tasklets.len(),\n            )\n            .field("nested_job_count", &self.nested_jobs.len())\n            .finish()',
)
s = s.replace(
    '            partitioned_tasklets: BTreeMap::new(),\n        })',
    '            partitioned_tasklets: BTreeMap::new(),\n            nested_jobs: BTreeMap::new(),\n        })',
)
insert_before = '    /// Validates that every compiled node has exactly one executable binding.\n'
method = '''    /// Binds one separately durable tasklet child job to a nested-job node.
    ///
    /// # Errors
    ///
    /// Rejects a non-nested node, a child whose exact definition identity differs
    /// from the compiled declaration, or a second binding for the same node.
    pub fn with_nested_tasklet_job(
        mut self,
        node_id: NodeId,
        child: Arc<TaskletJob>,
    ) -> Result<Self, FlowJobError> {
        let Some(FlowNode::NestedJob(compiled)) = self.plan.node(&node_id) else {
            return Err(FlowJobError::WrongNodeKind { node: node_id });
        };
        if compiled.child_definition() != child.definition_identity() {
            return Err(FlowJobError::NestedJobDefinitionMismatch { node: node_id });
        }
        if self
            .nested_jobs
            .insert(node_id.clone(), NestedJobBinding::Tasklet(child))
            .is_some()
        {
            return Err(FlowJobError::DuplicateBinding { node: node_id });
        }
        Ok(self)
    }

'''
s = s.replace(insert_before, method + insert_before)
s = s.replace(
    '                FlowNode::Decision(_) => self.deciders.contains_key(id),\n                FlowNode::Split(_) | FlowNode::Join(_) => true,',
    '                FlowNode::Decision(_) => self.deciders.contains_key(id),\n                FlowNode::NestedJob(compiled) => self\n                    .nested_jobs\n                    .get(id)\n                    .is_some_and(|binding| binding.definition_identity() == compiled.child_definition()),\n                FlowNode::Split(_) | FlowNode::Join(_) => true,',
)
s = s.replace(
    '    /// The bound executable does not match the compiled step declaration.\n    ComponentMismatch {',
    '    /// A nested child executable does not match the exact compiled child definition.\n    NestedJobDefinitionMismatch {\n        /// Mismatched logical nested-job node.\n        node: NodeId,\n    },\n    /// The bound executable does not match the compiled step declaration.\n    ComponentMismatch {',
)
s = s.replace(
    '            Self::ComponentMismatch { node } => write!(',
    '            Self::NestedJobDefinitionMismatch { node } => write!(\n                formatter,\n                "nested-job node {} was bound to a different child definition",\n                node.as_str()\n            ),\n            Self::ComponentMismatch { node } => write!(',
)
s = s.replace(
    '            FlowNode::PartitionedStep(_) => Some(RepositoryCapability::StepPartitions),\n            _ => None,',
    '            FlowNode::PartitionedStep(_) => Some(RepositoryCapability::StepPartitions),\n            FlowNode::NestedJob(_) => Some(RepositoryCapability::NestedJobs),\n            _ => None,',
)
flow.write_text(s)
