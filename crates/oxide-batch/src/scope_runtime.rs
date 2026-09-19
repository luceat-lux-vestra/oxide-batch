//! Deterministic scoped late-binding resolution and durable provenance.
//!
//! This module resolves only typed values. Live component construction,
//! memoization, cleanup, and partial-unwind semantics belong to the sibling
//! runtime workstream.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use oxide_batch_repository::{ScopeResolutionProvenance, ScopeResolutionSource};

use crate::structured_selector::{
    SelectorFailure, SourceValue, coerce, context_value, default_value, digest_hex,
};
use crate::{
    CompiledExecutionPlan, ExecutionAttempt, JobExecutionId, JobInstanceId, JobRepository,
    LateBoundInput, LateBoundSource, MissingParameterPolicy, NodeId, ParameterName, ParameterValue,
    RepositoryError, RepositoryUnitOfWork, ScopeFrameworkSource, ScopeKind,
    ScopedComponentDefinition, StepExecutionId, StepName,
};

#[derive(Clone, Copy)]
pub(crate) struct ScopeStepOwner<'a> {
    pub(crate) execution_id: StepExecutionId,
    pub(crate) node_id: &'a NodeId,
    pub(crate) step_name: &'a StepName,
}

/// Stable, value-redacted failure category for scoped late-bound resolution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ScopeResolutionFailure {
    /// The selected durable source did not contain the declared value.
    MissingSource,
    /// The selected durable source is unavailable from this repository.
    SourceUnavailable,
    /// A committed context used another schema identity or version.
    SourceSchemaMismatch,
    /// The selected value did not have the type required by the input.
    SourceTypeMismatch,
    /// The declared coercion could not represent the selected value.
    CoercionFailed,
    /// A bounded source value or context shape is invalid.
    InvalidValue,
    /// Durable provenance no longer matches the declared or authoritative source.
    ProvenanceMismatch,
}

impl fmt::Display for ScopeResolutionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::MissingSource => "scope late-binding source is missing",
            Self::SourceUnavailable => "scope late-binding durable source is unavailable",
            Self::SourceSchemaMismatch => "scope late-binding source schema does not match",
            Self::SourceTypeMismatch => "scope late-binding source type does not match",
            Self::CoercionFailed => "scope late-binding coercion failed",
            Self::InvalidValue => "scope late-binding source value is invalid",
            Self::ProvenanceMismatch => "scope late-binding provenance does not match",
        })
    }
}

impl Error for ScopeResolutionFailure {}

#[derive(Debug)]
pub(crate) enum ScopeResolutionError {
    Repository(RepositoryError),
    Resolution(ScopeResolutionFailure),
}

impl From<RepositoryError> for ScopeResolutionError {
    fn from(error: RepositoryError) -> Self {
        Self::Repository(error)
    }
}

impl From<ScopeResolutionFailure> for ScopeResolutionError {
    fn from(error: ScopeResolutionFailure) -> Self {
        Self::Resolution(error)
    }
}

impl From<SelectorFailure> for ScopeResolutionError {
    fn from(error: SelectorFailure) -> Self {
        Self::Resolution(match error {
            SelectorFailure::SourceUnavailable => ScopeResolutionFailure::SourceUnavailable,
            SelectorFailure::SourceSchemaMismatch => ScopeResolutionFailure::SourceSchemaMismatch,
            SelectorFailure::SourceTypeMismatch => ScopeResolutionFailure::SourceTypeMismatch,
            SelectorFailure::CoercionFailed => ScopeResolutionFailure::CoercionFailed,
            SelectorFailure::InvalidValue => ScopeResolutionFailure::InvalidValue,
        })
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(crate) async fn resolve_scoped_component_inputs(
    repository: &dyn JobRepository,
    plan: &CompiledExecutionPlan,
    component: &ScopedComponentDefinition,
    job_instance_id: JobInstanceId,
    job_execution_id: JobExecutionId,
    attempt: ExecutionAttempt,
    step_owner: Option<ScopeStepOwner<'_>>,
) -> Result<BTreeMap<ParameterName, ParameterValue>, ScopeResolutionError> {
    let mut unit = repository.begin().await?;
    let mut primary = None;
    let mut resolved = BTreeMap::new();
    let mut fresh_provenance = Vec::new();

    if plan.scoped_component(component.scope(), component.id()) != Some(component) {
        primary = Some(ScopeResolutionFailure::ProvenanceMismatch.into());
    }

    if primary.is_none() {
        let durable_definition = unit.scope_execution_definition(job_execution_id).await?;
        if &durable_definition != plan.definition_identity() {
            primary = Some(ScopeResolutionFailure::ProvenanceMismatch.into());
        }
    }

    if primary.is_none() {
        let durable_attempt = unit.scope_execution_attempt(job_execution_id).await?;
        if durable_attempt.get() != attempt.get() {
            primary = Some(ScopeResolutionFailure::ProvenanceMismatch.into());
        }
    }

    if primary.is_none()
        && let Err(error) = validate_owner(
            unit.as_mut(),
            component.scope(),
            job_instance_id,
            job_execution_id,
            step_owner,
        )
        .await
    {
        primary = Some(error);
    }

    let existing = if primary.is_none() {
        unit.scope_resolution_provenance(
            component.scope(),
            component.id(),
            job_execution_id,
            step_owner.map(|owner| owner.execution_id),
        )
        .await?
    } else {
        Vec::new()
    };

    if primary.is_none() && !existing.is_empty() && existing.len() != component.inputs().len() {
        primary = Some(ScopeResolutionFailure::ProvenanceMismatch.into());
    }

    for input in component.inputs() {
        if primary.is_some() {
            break;
        }
        let source = if existing.is_empty() {
            match resolve_fresh_source(
                unit.as_mut(),
                plan,
                input,
                job_instance_id,
                job_execution_id,
                attempt,
                step_owner,
            )
            .await
            {
                Ok((source, provenance_source)) => {
                    let Ok(provenance) = ScopeResolutionProvenance::new(
                        component.scope(),
                        component.id().clone(),
                        input.name().clone(),
                        job_execution_id,
                        step_owner.map(|owner| owner.execution_id),
                        provenance_source,
                    ) else {
                        primary = Some(ScopeResolutionFailure::ProvenanceMismatch.into());
                        break;
                    };
                    fresh_provenance.push(provenance);
                    source
                }
                Err(error) => {
                    primary = Some(error);
                    break;
                }
            }
        } else {
            let Some(provenance) = existing.iter().find(|entry| entry.input() == input.name())
            else {
                primary = Some(ScopeResolutionFailure::ProvenanceMismatch.into());
                break;
            };
            match resolve_recorded_source(
                unit.as_mut(),
                plan,
                input,
                provenance,
                job_instance_id,
                job_execution_id,
                attempt,
                step_owner,
            )
            .await
            {
                Ok(source) => source,
                Err(error) => {
                    primary = Some(error);
                    break;
                }
            }
        };

        let value = match resolve_value(input, source) {
            Ok(value) => value,
            Err(error) => {
                primary = Some(error);
                break;
            }
        };
        if resolved.insert(input.name().clone(), value).is_some() {
            primary = Some(ScopeResolutionFailure::InvalidValue.into());
            break;
        }
    }

    if let Some(error) = primary {
        if let Err(rollback) = unit.rollback().await {
            return Err(rollback.into());
        }
        return Err(error);
    }

    if existing.is_empty() && !fresh_provenance.is_empty() {
        unit.store_scope_resolution_provenance(&fresh_provenance)
            .await?;
        unit.commit().await?;
    } else {
        unit.rollback().await?;
    }
    Ok(resolved)
}

async fn validate_owner(
    unit: &mut dyn RepositoryUnitOfWork,
    scope: ScopeKind,
    job_instance_id: JobInstanceId,
    job_execution_id: JobExecutionId,
    step_owner: Option<ScopeStepOwner<'_>>,
) -> Result<(), ScopeResolutionError> {
    let job = unit.get_job_execution(job_execution_id).await?.ok_or(
        RepositoryError::JobExecutionNotFound {
            id: job_execution_id,
        },
    )?;
    if job.job_instance_id() != job_instance_id {
        return Err(ScopeResolutionFailure::ProvenanceMismatch.into());
    }
    match (scope, step_owner) {
        (ScopeKind::Job, None) => Ok(()),
        (ScopeKind::Step, Some(owner)) => {
            let state = unit.scope_step_state(owner.execution_id).await?.ok_or(
                RepositoryError::StepExecutionNotFound {
                    id: owner.execution_id,
                },
            )?;
            if state.execution().job_execution_id() != job_execution_id
                || state.node_id() != owner.node_id
                || state.execution().step_name() != owner.step_name
            {
                return Err(ScopeResolutionFailure::ProvenanceMismatch.into());
            }
            Ok(())
        }
        _ => Err(ScopeResolutionFailure::ProvenanceMismatch.into()),
    }
}

#[allow(clippy::too_many_arguments)]
async fn resolve_fresh_source(
    unit: &mut dyn RepositoryUnitOfWork,
    plan: &CompiledExecutionPlan,
    input: &LateBoundInput,
    job_instance_id: JobInstanceId,
    job_execution_id: JobExecutionId,
    attempt: ExecutionAttempt,
    step_owner: Option<ScopeStepOwner<'_>>,
) -> Result<(Option<SourceValue>, ScopeResolutionSource), ScopeResolutionError> {
    match input.source() {
        LateBoundSource::JobParameter(name) => {
            let parameters = unit.job_execution_parameters(job_execution_id).await?;
            let value = parameters
                .get(name)
                .map(|parameter| SourceValue::Parameter(parameter.value().clone()));
            Ok((
                value,
                ScopeResolutionSource::JobParameter {
                    job_execution_id,
                    parameter: name.clone(),
                },
            ))
        }
        LateBoundSource::JobContext {
            schema,
            schema_version,
            path,
        } => {
            let execution = unit.get_job_execution(job_execution_id).await?.ok_or(
                RepositoryError::JobExecutionNotFound {
                    id: job_execution_id,
                },
            )?;
            let context = unit.scope_job_execution_context(job_execution_id).await?;
            let value = match context {
                Some(context) => {
                    Some(context_value(&context, schema, *schema_version, path)?).flatten()
                }
                None => None,
            };
            let source = ScopeResolutionSource::job_context(
                job_execution_id,
                execution.version(),
                schema.clone(),
                *schema_version,
                path.segments().to_vec(),
            )
            .map_err(|_| ScopeResolutionFailure::ProvenanceMismatch)?;
            Ok((value, source))
        }
        LateBoundSource::StepContext {
            node,
            schema,
            schema_version,
            path,
        } => {
            let state = unit.latest_flow_step(job_instance_id, node).await?;
            let (value, step_execution_id, execution_version) = match state {
                Some(state) => {
                    validate_source_step_instance(unit, &state, job_instance_id).await?;
                    let value = match state.context() {
                        Some(context) => context_value(context, schema, *schema_version, path)?,
                        None => None,
                    };
                    (
                        value,
                        Some(state.execution().id()),
                        Some(state.execution().version()),
                    )
                }
                None => (None, None, None),
            };
            let source = ScopeResolutionSource::step_context(
                node.clone(),
                step_execution_id,
                execution_version,
                schema.clone(),
                *schema_version,
                path.segments().to_vec(),
            )
            .map_err(|_| ScopeResolutionFailure::ProvenanceMismatch)?;
            Ok((value, source))
        }
        LateBoundSource::Framework(source) => Ok((
            framework_value(
                plan,
                *source,
                job_instance_id,
                job_execution_id,
                attempt,
                step_owner,
            )?
            .map(SourceValue::Parameter),
            ScopeResolutionSource::Framework {
                source: *source,
                job_execution_id,
                step_execution_id: step_owner.map(|owner| owner.execution_id),
            },
        )),
        _ => Err(ScopeResolutionFailure::InvalidValue.into()),
    }
}

#[allow(clippy::too_many_arguments)]
async fn resolve_recorded_source(
    unit: &mut dyn RepositoryUnitOfWork,
    plan: &CompiledExecutionPlan,
    input: &LateBoundInput,
    provenance: &ScopeResolutionProvenance,
    job_instance_id: JobInstanceId,
    job_execution_id: JobExecutionId,
    attempt: ExecutionAttempt,
    step_owner: Option<ScopeStepOwner<'_>>,
) -> Result<Option<SourceValue>, ScopeResolutionError> {
    if provenance.owner_job_execution_id() != job_execution_id
        || provenance.owner_step_execution_id() != step_owner.map(|owner| owner.execution_id)
    {
        return Err(ScopeResolutionFailure::ProvenanceMismatch.into());
    }

    match (input.source(), provenance.source()) {
        (
            LateBoundSource::JobParameter(expected),
            ScopeResolutionSource::JobParameter {
                job_execution_id: source_execution_id,
                parameter,
            },
        ) if expected == parameter => {
            validate_source_job_instance(unit, *source_execution_id, job_instance_id).await?;
            let parameters = unit.job_execution_parameters(*source_execution_id).await?;
            Ok(parameters
                .get(parameter)
                .map(|value| SourceValue::Parameter(value.value().clone())))
        }
        (
            LateBoundSource::JobContext {
                schema,
                schema_version,
                path,
            },
            ScopeResolutionSource::JobContext {
                job_execution_id: source_execution_id,
                execution_version,
                schema: recorded_schema,
                schema_version: recorded_schema_version,
                path: recorded_path,
            },
        ) if schema == recorded_schema
            && schema_version == recorded_schema_version
            && path.segments() == recorded_path.as_slice() =>
        {
            validate_source_job_instance(unit, *source_execution_id, job_instance_id).await?;
            let execution = unit.get_job_execution(*source_execution_id).await?.ok_or(
                RepositoryError::JobExecutionNotFound {
                    id: *source_execution_id,
                },
            )?;
            // Job execution context has no mutation port after creation. The
            // execution version may still advance for lifecycle transitions,
            // so the version recorded at scope resolution is a lower bound:
            // an older observed row is impossible, while a newer lifecycle
            // version still owns the same immutable context payload.
            if execution.version() < *execution_version {
                return Err(ScopeResolutionFailure::ProvenanceMismatch.into());
            }
            let Some(context) = unit
                .scope_job_execution_context(*source_execution_id)
                .await?
            else {
                return Ok(None);
            };
            Ok(context_value(&context, schema, *schema_version, path)?)
        }
        (
            LateBoundSource::StepContext {
                node,
                schema,
                schema_version,
                path,
            },
            ScopeResolutionSource::StepContext {
                node: recorded_node,
                step_execution_id,
                execution_version,
                schema: recorded_schema,
                schema_version: recorded_schema_version,
                path: recorded_path,
            },
        ) if node == recorded_node
            && schema == recorded_schema
            && schema_version == recorded_schema_version
            && path.segments() == recorded_path.as_slice() =>
        {
            match (step_execution_id, execution_version) {
                (Some(step_id), Some(version)) => {
                    let state = unit
                        .scope_step_state(*step_id)
                        .await?
                        .ok_or(RepositoryError::StepExecutionNotFound { id: *step_id })?;
                    validate_source_step_instance(unit, &state, job_instance_id).await?;
                    if state.node_id() != node || state.execution().version() != *version {
                        return Err(ScopeResolutionFailure::ProvenanceMismatch.into());
                    }
                    let Some(context) = state.context() else {
                        return Ok(None);
                    };
                    Ok(context_value(context, schema, *schema_version, path)?)
                }
                (None, None) => {
                    if unit
                        .latest_flow_step(job_instance_id, node)
                        .await?
                        .is_some()
                    {
                        return Err(ScopeResolutionFailure::ProvenanceMismatch.into());
                    }
                    Ok(None)
                }
                _ => Err(ScopeResolutionFailure::ProvenanceMismatch.into()),
            }
        }
        (
            LateBoundSource::Framework(expected),
            ScopeResolutionSource::Framework {
                source,
                job_execution_id: source_execution_id,
                step_execution_id,
            },
        ) if expected == source
            && *source_execution_id == job_execution_id
            && *step_execution_id == step_owner.map(|owner| owner.execution_id) =>
        {
            Ok(framework_value(
                plan,
                *source,
                job_instance_id,
                job_execution_id,
                attempt,
                step_owner,
            )?
            .map(SourceValue::Parameter))
        }
        _ => Err(ScopeResolutionFailure::ProvenanceMismatch.into()),
    }
}

async fn validate_source_job_instance(
    unit: &mut dyn RepositoryUnitOfWork,
    source_job_id: JobExecutionId,
    job_instance_id: JobInstanceId,
) -> Result<(), ScopeResolutionError> {
    let source_job = unit
        .get_job_execution(source_job_id)
        .await?
        .ok_or(RepositoryError::JobExecutionNotFound { id: source_job_id })?;
    if source_job.job_instance_id() != job_instance_id {
        return Err(ScopeResolutionFailure::ProvenanceMismatch.into());
    }
    Ok(())
}

async fn validate_source_step_instance(
    unit: &mut dyn RepositoryUnitOfWork,
    state: &oxide_batch_repository::FlowStepState,
    job_instance_id: JobInstanceId,
) -> Result<(), ScopeResolutionError> {
    validate_source_job_instance(unit, state.execution().job_execution_id(), job_instance_id).await
}

fn resolve_value(
    input: &LateBoundInput,
    source: Option<SourceValue>,
) -> Result<ParameterValue, ScopeResolutionError> {
    match source {
        Some(source) => Ok(coerce(source, input.coercion(), input.expected_kind())?),
        None => match input.missing_policy() {
            MissingParameterPolicy::Fail => Err(ScopeResolutionFailure::MissingSource.into()),
            MissingParameterPolicy::TypeDefault => Ok(default_value(input.expected_kind())?),
            _ => Err(ScopeResolutionFailure::InvalidValue.into()),
        },
    }
}

fn framework_value(
    plan: &CompiledExecutionPlan,
    source: ScopeFrameworkSource,
    job_instance_id: JobInstanceId,
    job_execution_id: JobExecutionId,
    attempt: ExecutionAttempt,
    step_owner: Option<ScopeStepOwner<'_>>,
) -> Result<Option<ParameterValue>, ScopeResolutionError> {
    let value = match source {
        ScopeFrameworkSource::JobInstanceId => ParameterValue::from(job_instance_id.get()),
        ScopeFrameworkSource::JobExecutionId => ParameterValue::from(job_execution_id.get()),
        ScopeFrameworkSource::StepExecutionId => {
            let Some(owner) = step_owner else {
                return Ok(None);
            };
            ParameterValue::from(owner.execution_id.get())
        }
        ScopeFrameworkSource::Attempt => ParameterValue::from(attempt.get()),
        ScopeFrameworkSource::DefinitionRevision => {
            ParameterValue::string(plan.definition_identity().revision().as_str().to_owned())
                .map_err(|_| ScopeResolutionFailure::InvalidValue)?
        }
        ScopeFrameworkSource::DefinitionFingerprint => {
            ParameterValue::string(digest_hex(plan.fingerprint()))
                .map_err(|_| ScopeResolutionFailure::InvalidValue)?
        }
        ScopeFrameworkSource::LogicalNodeId => {
            let Some(owner) = step_owner else {
                return Ok(None);
            };
            ParameterValue::string(owner.node_id.as_str().to_owned())
                .map_err(|_| ScopeResolutionFailure::InvalidValue)?
        }
        ScopeFrameworkSource::StepName => {
            let Some(owner) = step_owner else {
                return Ok(None);
            };
            ParameterValue::string(owner.step_name.as_str().to_owned())
                .map_err(|_| ScopeResolutionFailure::InvalidValue)?
        }
        _ => return Err(ScopeResolutionFailure::InvalidValue.into()),
    };
    Ok(Some(value))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use std::num::NonZeroU64;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, SystemTime};

    use futures_executor::block_on;

    use super::*;
    use crate::{
        BatchStatus, Clock, ComponentRevision, DefinitionRevision, DefinitionUpgrade,
        DefinitionUpgradeKey, ExecutionContext, ExecutionVersion, FlowGraph, FlowNode, FlowTarget,
        InMemoryJobRepository, JobInstanceKey, JobName, JobParameter, JobParameters,
        LifecycleTransition, ParameterCoercion, ParameterRole, ParameterValueKind, PurgeBatchBound,
        PurgePlanRequest, ScopeFactoryKind, ScopeResolverKind, SequentialIdGenerator, StartLimit,
        StateLimits, StateSchemaId, StateSchemaVersion, StepComponents, StepDefinitionUpgrade,
        StepNode, SystemClock, TerminalKind, TerminalStatusSet,
    };

    fn parameter(value: ParameterValue, role: ParameterRole) -> JobParameter {
        JobParameter::new(value, role)
    }

    fn component(
        resolver_revision: &str,
        inputs: Vec<LateBoundInput>,
    ) -> ScopedComponentDefinition {
        component_for(ScopeKind::Job, "client", resolver_revision, inputs)
    }

    fn component_for(
        scope: ScopeKind,
        id: &str,
        resolver_revision: &str,
        inputs: Vec<LateBoundInput>,
    ) -> ScopedComponentDefinition {
        ScopedComponentDefinition::new(
            scope,
            crate::ScopedComponentId::new(id).expect("component"),
            ScopeFactoryKind::new("http-client").expect("factory kind"),
            ComponentRevision::new("factory-v1").expect("factory revision"),
            ScopeResolverKind::new("structured-selector").expect("resolver kind"),
            ComponentRevision::new(resolver_revision).expect("resolver revision"),
            inputs,
        )
        .expect("scoped component")
    }

    fn plan(component: ScopedComponentDefinition) -> CompiledExecutionPlan {
        plan_with_revision(component, "r1")
    }

    fn plan_with_revision(
        component: ScopedComponentDefinition,
        revision: &str,
    ) -> CompiledExecutionPlan {
        let source = NodeId::new("source").expect("source");
        let work = NodeId::new("work").expect("work");
        FlowGraph::new(source.clone())
            .with_node(FlowNode::step(StepNode::new(
                source.clone(),
                StepName::new("source").expect("source step"),
                StepComponents::Tasklet(ComponentRevision::new("source-v1").expect("source")),
            )))
            .with_node(FlowNode::step(StepNode::new(
                work.clone(),
                StepName::new("work").expect("step"),
                StepComponents::Tasklet(ComponentRevision::new("tasklet-v1").expect("tasklet")),
            )))
            .with_scoped_component(component)
            .with_sequence(source, FlowTarget::Node(work.clone()))
            .expect("source sequence")
            .with_sequence(work, FlowTarget::Terminal(TerminalKind::Complete))
            .expect("work sequence")
            .compile(
                &JobName::new("scope_job").expect("job"),
                DefinitionRevision::new(revision).expect("revision"),
            )
            .expect("plan")
    }

    fn repository() -> InMemoryJobRepository {
        InMemoryJobRepository::new(
            Arc::new(SystemClock),
            Arc::new(SequentialIdGenerator::new(NonZeroU64::MIN)),
        )
    }

    #[derive(Clone)]
    struct AdjustableClock(Arc<Mutex<SystemTime>>);

    impl AdjustableClock {
        fn new(now: SystemTime) -> Self {
            Self(Arc::new(Mutex::new(now)))
        }

        fn advance(&self, duration: Duration) {
            let mut now = self.0.lock().expect("clock lock");
            *now = now.checked_add(duration).expect("clock advance");
        }
    }

    impl Clock for AdjustableClock {
        fn now(&self) -> SystemTime {
            *self.0.lock().expect("clock lock")
        }
    }

    async fn create_execution(
        repository: &InMemoryJobRepository,
        plan: &CompiledExecutionPlan,
        parameters: &JobParameters,
    ) -> (JobInstanceId, JobExecutionId) {
        let key = JobInstanceKey::new(JobName::new("scope_job").expect("job"), parameters);
        let mut unit = repository.begin().await.expect("begin");
        let instance = unit
            .select_or_create_job_instance(&key)
            .await
            .expect("instance")
            .instance()
            .clone();
        let execution = unit
            .create_job_execution_with_definition_and_parameters(
                instance.id(),
                plan.definition_identity(),
                parameters,
            )
            .await
            .expect("execution");
        unit.commit().await.expect("commit");
        (instance.id(), execution.id())
    }

    async fn stop_execution(repository: &InMemoryJobRepository, execution_id: JobExecutionId) {
        let mut unit = repository.begin().await.expect("begin stop");
        let execution = unit
            .get_job_execution(execution_id)
            .await
            .expect("execution read")
            .expect("execution");
        let at = execution.metadata().timestamps().created_at();
        let started = unit
            .transition_job_execution(
                execution.id(),
                execution.version(),
                LifecycleTransition::new(BatchStatus::Started, at),
            )
            .await
            .expect("start execution");
        unit.transition_job_execution(
            started.id(),
            started.version(),
            LifecycleTransition::new(BatchStatus::Stopped, at),
        )
        .await
        .expect("stop execution");
        unit.commit().await.expect("commit stop");
    }

    fn step_context(value: &str) -> ExecutionContext {
        let bytes = format!(
            r#"{{"format":"oxide-batch.execution-context","format_version":1,"schema":"scope.step","schema_version":1,"payload":{{"value":"{value}"}}}}"#
        );
        ExecutionContext::from_json(bytes.as_bytes(), StateLimits::default()).expect("context")
    }

    async fn create_owner_step(
        repository: &InMemoryJobRepository,
        execution_id: JobExecutionId,
    ) -> StepExecutionId {
        let mut unit = repository.begin().await.expect("begin owner");
        let owner = unit
            .create_flow_step_execution(
                execution_id,
                &StepName::new("work").expect("work"),
                &NodeId::new("work").expect("work"),
                StartLimit::UNRESTRICTED,
            )
            .await
            .expect("owner");
        unit.commit().await.expect("commit owner");
        owner.id()
    }

    async fn create_source_step(
        repository: &InMemoryJobRepository,
        execution_id: JobExecutionId,
        context: Option<&ExecutionContext>,
    ) -> StepExecutionId {
        let mut unit = repository.begin().await.expect("begin source");
        let source = unit
            .create_flow_step_execution(
                execution_id,
                &StepName::new("source").expect("source"),
                &NodeId::new("source").expect("source"),
                StartLimit::UNRESTRICTED,
            )
            .await
            .expect("source");
        let started = unit
            .transition_step_execution(
                source.id(),
                source.version(),
                LifecycleTransition::new(
                    BatchStatus::Started,
                    source.metadata().timestamps().created_at(),
                ),
            )
            .await
            .expect("start source");
        let source = if let Some(context) = context {
            unit.commit_step_execution_context(started.id(), started.version(), context)
                .await
                .expect("source context")
        } else {
            started
        };
        unit.commit().await.expect("commit source");
        source.id()
    }

    #[test]
    fn fresh_resolution_commits_value_free_provenance_and_reuses_it() {
        block_on(async {
            let tenant = ParameterName::new("tenant").expect("tenant");
            let optional = ParameterName::new("optional").expect("optional");
            let component = component(
                "resolver-v1",
                vec![
                    LateBoundInput::new(
                        tenant.clone(),
                        LateBoundSource::JobParameter(tenant.clone()),
                        ParameterValueKind::String,
                        ParameterCoercion::Exact,
                        MissingParameterPolicy::Fail,
                    ),
                    LateBoundInput::new(
                        ParameterName::new("attempt").expect("attempt"),
                        LateBoundSource::Framework(ScopeFrameworkSource::Attempt),
                        ParameterValueKind::U64,
                        ParameterCoercion::Exact,
                        MissingParameterPolicy::Fail,
                    ),
                    LateBoundInput::new(
                        optional.clone(),
                        LateBoundSource::JobParameter(optional),
                        ParameterValueKind::String,
                        ParameterCoercion::Exact,
                        MissingParameterPolicy::TypeDefault,
                    ),
                ],
            );
            let plan = plan(component.clone());
            let mut parameters = JobParameters::new();
            parameters
                .insert(
                    tenant.clone(),
                    parameter(
                        ParameterValue::string("acme-low-entropy").expect("value"),
                        ParameterRole::Identifying,
                    ),
                )
                .expect("parameter");
            let repository = repository();
            let (instance_id, execution_id) =
                create_execution(&repository, &plan, &parameters).await;

            let first = resolve_scoped_component_inputs(
                &repository,
                &plan,
                &component,
                instance_id,
                execution_id,
                ExecutionAttempt::new(NonZeroU64::MIN),
                None,
            )
            .await
            .expect("first resolution");
            assert_eq!(
                first.get(&tenant).and_then(ParameterValue::as_str),
                Some("acme-low-entropy")
            );
            assert_eq!(
                first
                    .get(&ParameterName::new("attempt").expect("attempt"))
                    .and_then(ParameterValue::as_u64),
                Some(1)
            );
            assert_eq!(
                first
                    .get(&ParameterName::new("optional").expect("optional"))
                    .and_then(ParameterValue::as_str),
                Some("")
            );

            let mut inspect = repository.begin().await.expect("inspect");
            let provenance = inspect
                .scope_resolution_provenance(ScopeKind::Job, component.id(), execution_id, None)
                .await
                .expect("provenance");
            inspect.rollback().await.expect("rollback");
            assert_eq!(provenance.len(), 3);
            let debug = format!("{provenance:?}");
            assert!(!debug.contains("acme-low-entropy"));
            assert!(!debug.contains("value_digest"));
            assert!(!debug.contains("resolved_value"));

            let second = resolve_scoped_component_inputs(
                &repository,
                &plan,
                &component,
                instance_id,
                execution_id,
                ExecutionAttempt::new(NonZeroU64::MIN),
                None,
            )
            .await
            .expect("re-resolution");
            assert_eq!(first, second);
        });
    }

    #[test]
    fn restart_reuses_authoritative_parameter_source_and_rebinds_framework_metadata() {
        block_on(async {
            let business_key = ParameterName::new("business_key").expect("business key");
            let payload = ParameterName::new("payload").expect("payload");
            let attempt_name = ParameterName::new("attempt").expect("attempt");
            let component = component(
                "resolver-v1",
                vec![
                    LateBoundInput::new(
                        payload.clone(),
                        LateBoundSource::JobParameter(payload.clone()),
                        ParameterValueKind::String,
                        ParameterCoercion::Exact,
                        MissingParameterPolicy::Fail,
                    ),
                    LateBoundInput::new(
                        attempt_name.clone(),
                        LateBoundSource::Framework(ScopeFrameworkSource::Attempt),
                        ParameterValueKind::U64,
                        ParameterCoercion::Exact,
                        MissingParameterPolicy::Fail,
                    ),
                ],
            );
            let plan = plan(component.clone());
            let repository = repository();

            let parameters = |payload_value: &str| {
                let mut parameters = JobParameters::new();
                parameters
                    .insert(
                        business_key.clone(),
                        parameter(
                            ParameterValue::string("same-instance").expect("key value"),
                            ParameterRole::Identifying,
                        ),
                    )
                    .expect("business key");
                parameters
                    .insert(
                        payload.clone(),
                        parameter(
                            ParameterValue::string(payload_value).expect("payload value"),
                            ParameterRole::NonIdentifying,
                        ),
                    )
                    .expect("payload");
                parameters
            };

            let first_parameters = parameters("first");
            let (instance_id, first_execution_id) =
                create_execution(&repository, &plan, &first_parameters).await;
            let first = resolve_scoped_component_inputs(
                &repository,
                &plan,
                &component,
                instance_id,
                first_execution_id,
                ExecutionAttempt::new(NonZeroU64::MIN),
                None,
            )
            .await
            .expect("first resolution");
            assert_eq!(
                first.get(&payload).and_then(ParameterValue::as_str),
                Some("first")
            );
            stop_execution(&repository, first_execution_id).await;

            let second_parameters = parameters("second");
            let (same_instance, second_execution_id) =
                create_execution(&repository, &plan, &second_parameters).await;
            assert_eq!(same_instance, instance_id);
            let second = resolve_scoped_component_inputs(
                &repository,
                &plan,
                &component,
                instance_id,
                second_execution_id,
                ExecutionAttempt::new(NonZeroU64::new(2).expect("attempt two")),
                None,
            )
            .await
            .expect("restart resolution");
            assert_eq!(
                second.get(&payload).and_then(ParameterValue::as_str),
                Some("first"),
                "restart must reuse the originally referenced immutable parameter set",
            );
            assert_eq!(
                second.get(&attempt_name).and_then(ParameterValue::as_u64),
                Some(2),
                "framework attempt metadata belongs to the new live scope",
            );

            let mut inspect = repository
                .begin()
                .await
                .expect("inspect restart provenance");
            let provenance = inspect
                .scope_resolution_provenance(
                    ScopeKind::Job,
                    component.id(),
                    second_execution_id,
                    None,
                )
                .await
                .expect("restart provenance");
            inspect.rollback().await.expect("rollback inspect");
            assert_eq!(provenance.len(), 2);
            assert!(provenance.iter().any(|entry| {
                matches!(
                    entry.source(),
                    ScopeResolutionSource::JobParameter {
                        job_execution_id,
                        parameter,
                    } if *job_execution_id == first_execution_id && parameter == &payload
                )
            }));
            assert!(provenance.iter().any(|entry| {
                matches!(
                    entry.source(),
                    ScopeResolutionSource::Framework {
                        source: ScopeFrameworkSource::Attempt,
                        job_execution_id,
                        step_execution_id: None,
                    } if *job_execution_id == second_execution_id
                )
            }));

            stop_execution(&repository, second_execution_id).await;
            let third_parameters = parameters("third");
            let (same_instance, third_execution_id) =
                create_execution(&repository, &plan, &third_parameters).await;
            assert_eq!(same_instance, instance_id);
            let third = resolve_scoped_component_inputs(
                &repository,
                &plan,
                &component,
                instance_id,
                third_execution_id,
                ExecutionAttempt::new(NonZeroU64::new(3).expect("attempt three")),
                None,
            )
            .await
            .expect("second restart resolution");
            assert_eq!(
                third.get(&payload).and_then(ParameterValue::as_str),
                Some("first"),
                "copy-forward must preserve the authoritative source through restart chains",
            );
            assert_eq!(
                third.get(&attempt_name).and_then(ParameterValue::as_u64),
                Some(3),
            );
        });
    }

    #[test]
    fn retention_preserves_execution_referenced_by_restart_provenance() {
        block_on(async {
            let tenant = ParameterName::new("tenant").expect("tenant");
            let scoped = component(
                "resolver-v1",
                vec![LateBoundInput::new(
                    tenant.clone(),
                    LateBoundSource::JobParameter(tenant.clone()),
                    ParameterValueKind::String,
                    ParameterCoercion::Exact,
                    MissingParameterPolicy::Fail,
                )],
            );
            let plan = plan(scoped.clone());
            let mut parameters = JobParameters::new();
            parameters
                .insert(
                    tenant,
                    parameter(
                        ParameterValue::string("stable").expect("tenant value"),
                        ParameterRole::Identifying,
                    ),
                )
                .expect("tenant parameter");

            let clock = AdjustableClock::new(SystemTime::UNIX_EPOCH + Duration::from_hours(2));
            let repository = InMemoryJobRepository::new(
                Arc::new(clock.clone()),
                Arc::new(SequentialIdGenerator::new(NonZeroU64::MIN)),
            );
            let (instance_id, first_execution_id) =
                create_execution(&repository, &plan, &parameters).await;
            resolve_scoped_component_inputs(
                &repository,
                &plan,
                &scoped,
                instance_id,
                first_execution_id,
                ExecutionAttempt::new(NonZeroU64::MIN),
                None,
            )
            .await
            .expect("first resolution");
            stop_execution(&repository, first_execution_id).await;

            let (_, second_execution_id) = create_execution(&repository, &plan, &parameters).await;
            stop_execution(&repository, second_execution_id).await;

            clock.advance(Duration::from_hours(2));
            let request = PurgePlanRequest::new(
                JobName::new("scope_job").expect("job"),
                TerminalStatusSet::new([BatchStatus::Stopped]).expect("statuses"),
                Duration::from_hours(1),
                PurgeBatchBound::new(10).expect("batch"),
            )
            .expect("purge request");
            let mut unit = repository.begin().await.expect("begin purge survey");
            let survey = unit.purge_survey(&request).await.expect("purge survey");
            unit.rollback().await.expect("rollback survey");

            assert!(
                survey
                    .candidates()
                    .iter()
                    .all(|candidate| candidate.job_execution_id() != first_execution_id),
                "a surviving restart provenance source must not be purgeable",
            );
            assert!(
                survey
                    .candidates()
                    .iter()
                    .any(|candidate| candidate.job_execution_id() == second_execution_id),
                "the provenance owner remains independently purgeable",
            );
        });
    }

    #[test]
    fn compatible_restart_with_existing_provenance_fails_closed() {
        block_on(async {
            let tenant = ParameterName::new("tenant").expect("tenant");
            let first_component = component(
                "resolver-v1",
                vec![LateBoundInput::new(
                    tenant.clone(),
                    LateBoundSource::JobParameter(tenant.clone()),
                    ParameterValueKind::String,
                    ParameterCoercion::Exact,
                    MissingParameterPolicy::Fail,
                )],
            );
            let first_plan = plan(first_component.clone());
            let mut parameters = JobParameters::new();
            parameters
                .insert(
                    tenant.clone(),
                    parameter(
                        ParameterValue::string("stable").expect("tenant value"),
                        ParameterRole::Identifying,
                    ),
                )
                .expect("tenant parameter");

            let repository = repository();
            let (instance_id, first_execution_id) =
                create_execution(&repository, &first_plan, &parameters).await;
            resolve_scoped_component_inputs(
                &repository,
                &first_plan,
                &first_component,
                instance_id,
                first_execution_id,
                ExecutionAttempt::new(NonZeroU64::MIN),
                None,
            )
            .await
            .expect("first resolution");
            stop_execution(&repository, first_execution_id).await;

            let second_component = component(
                "resolver-v2",
                vec![LateBoundInput::new(
                    tenant,
                    LateBoundSource::JobParameter(
                        ParameterName::new("tenant").expect("tenant source"),
                    ),
                    ParameterValueKind::String,
                    ParameterCoercion::Exact,
                    MissingParameterPolicy::Fail,
                )],
            );
            let second_plan = plan_with_revision(second_component, "r2");
            let upgrade = DefinitionUpgrade::new(
                DefinitionUpgradeKey::new("scope-v1-to-v2").expect("upgrade key"),
                first_plan.definition_identity().clone(),
                second_plan.definition_identity().clone(),
                [
                    StepDefinitionUpgrade::new(
                        StepName::new("source").expect("source step"),
                        StepName::new("source").expect("source step"),
                    ),
                    StepDefinitionUpgrade::new(
                        StepName::new("work").expect("work step"),
                        StepName::new("work").expect("work step"),
                    ),
                ],
            )
            .expect("compatible definition edge");

            let mut register = repository
                .begin()
                .await
                .expect("begin upgrade registration");
            register
                .register_definition_upgrade(&JobName::new("scope_job").expect("job"), &upgrade)
                .await
                .expect("register upgrade");
            register.commit().await.expect("commit upgrade");

            let mut restart = repository.begin().await.expect("begin compatible restart");
            assert_eq!(
                restart
                    .create_job_execution_with_definition_and_parameters(
                        instance_id,
                        second_plan.definition_identity(),
                        &parameters,
                    )
                    .await,
                Err(RepositoryError::IncompatibleDefinition { instance_id }),
                "scope provenance has no accepted transform across a changed definition",
            );
            restart.rollback().await.expect("rollback restart");
        });
    }

    #[test]
    fn forged_cross_instance_historical_source_is_rejected_before_write() {
        block_on(async {
            let key = ParameterName::new("business_key").expect("business key");
            let tenant = ParameterName::new("tenant").expect("tenant");
            let component = component(
                "resolver-v1",
                vec![LateBoundInput::new(
                    tenant.clone(),
                    LateBoundSource::JobParameter(tenant.clone()),
                    ParameterValueKind::String,
                    ParameterCoercion::Exact,
                    MissingParameterPolicy::Fail,
                )],
            );
            let plan = plan(component.clone());
            let repository = repository();

            let parameters = |key_value: &str| {
                let mut parameters = JobParameters::new();
                parameters
                    .insert(
                        key.clone(),
                        parameter(
                            ParameterValue::string(key_value).expect("key value"),
                            ParameterRole::Identifying,
                        ),
                    )
                    .expect("key");
                parameters
                    .insert(
                        tenant.clone(),
                        parameter(
                            ParameterValue::string("tenant-value").expect("tenant value"),
                            ParameterRole::NonIdentifying,
                        ),
                    )
                    .expect("tenant");
                parameters
            };

            let (source_instance, source_execution) =
                create_execution(&repository, &plan, &parameters("source")).await;
            let (owner_instance, owner_execution) =
                create_execution(&repository, &plan, &parameters("owner")).await;
            assert_ne!(source_instance, owner_instance);

            let forged = ScopeResolutionProvenance::new(
                ScopeKind::Job,
                component.id().clone(),
                tenant.clone(),
                owner_execution,
                None,
                ScopeResolutionSource::JobParameter {
                    job_execution_id: source_execution,
                    parameter: tenant,
                },
            )
            .expect("historical sources are structurally representable");

            let mut unit = repository.begin().await.expect("begin forged write");
            let error = unit
                .store_scope_resolution_provenance(std::slice::from_ref(&forged))
                .await
                .expect_err("cross-instance provenance must fail closed");
            assert_eq!(error, RepositoryError::ScopeResolutionStateCorrupt);
            unit.rollback().await.expect("rollback forged write");
        });
    }

    #[test]
    fn step_scope_restart_preserves_source_and_rebinds_step_framework_metadata() {
        block_on(async {
            let business_key = ParameterName::new("business_key").expect("business key");
            let payload = ParameterName::new("payload").expect("payload");
            let step_id_name = ParameterName::new("step_execution_id").expect("step id input");
            let component = component_for(
                ScopeKind::Step,
                "step-client",
                "resolver-v1",
                vec![
                    LateBoundInput::new(
                        payload.clone(),
                        LateBoundSource::JobParameter(payload.clone()),
                        ParameterValueKind::String,
                        ParameterCoercion::Exact,
                        MissingParameterPolicy::Fail,
                    ),
                    LateBoundInput::new(
                        step_id_name.clone(),
                        LateBoundSource::Framework(ScopeFrameworkSource::StepExecutionId),
                        ParameterValueKind::U64,
                        ParameterCoercion::Exact,
                        MissingParameterPolicy::Fail,
                    ),
                ],
            );
            let plan = plan(component.clone());
            let repository = repository();
            let parameters = |payload_value: &str| {
                let mut parameters = JobParameters::new();
                parameters
                    .insert(
                        business_key.clone(),
                        parameter(
                            ParameterValue::string("same-instance").expect("key value"),
                            ParameterRole::Identifying,
                        ),
                    )
                    .expect("business key");
                parameters
                    .insert(
                        payload.clone(),
                        parameter(
                            ParameterValue::string(payload_value).expect("payload value"),
                            ParameterRole::NonIdentifying,
                        ),
                    )
                    .expect("payload");
                parameters
            };

            let (instance_id, first_execution_id) =
                create_execution(&repository, &plan, &parameters("first")).await;
            let first_owner_id = create_owner_step(&repository, first_execution_id).await;
            let first_node = NodeId::new("work").expect("work");
            let first_step = StepName::new("work").expect("work");
            let first = resolve_scoped_component_inputs(
                &repository,
                &plan,
                &component,
                instance_id,
                first_execution_id,
                ExecutionAttempt::new(NonZeroU64::MIN),
                Some(ScopeStepOwner {
                    execution_id: first_owner_id,
                    node_id: &first_node,
                    step_name: &first_step,
                }),
            )
            .await
            .expect("first step-scope resolution");
            assert_eq!(
                first.get(&payload).and_then(ParameterValue::as_str),
                Some("first"),
            );
            assert_eq!(
                first.get(&step_id_name).and_then(ParameterValue::as_u64),
                Some(first_owner_id.get()),
            );
            stop_execution(&repository, first_execution_id).await;

            let (same_instance, second_execution_id) =
                create_execution(&repository, &plan, &parameters("second")).await;
            assert_eq!(same_instance, instance_id);
            let second_owner_id = create_owner_step(&repository, second_execution_id).await;
            let second_node = NodeId::new("work").expect("work");
            let second_step = StepName::new("work").expect("work");
            let second = resolve_scoped_component_inputs(
                &repository,
                &plan,
                &component,
                instance_id,
                second_execution_id,
                ExecutionAttempt::new(NonZeroU64::new(2).expect("attempt two")),
                Some(ScopeStepOwner {
                    execution_id: second_owner_id,
                    node_id: &second_node,
                    step_name: &second_step,
                }),
            )
            .await
            .expect("restart step-scope resolution");
            assert_eq!(
                second.get(&payload).and_then(ParameterValue::as_str),
                Some("first"),
                "step scope must keep the original authoritative parameter source",
            );
            assert_eq!(
                second.get(&step_id_name).and_then(ParameterValue::as_u64),
                Some(second_owner_id.get()),
                "step framework metadata must bind to the new live step scope",
            );

            let mut inspect = repository.begin().await.expect("inspect step provenance");
            let provenance = inspect
                .scope_resolution_provenance(
                    ScopeKind::Step,
                    component.id(),
                    second_execution_id,
                    Some(second_owner_id),
                )
                .await
                .expect("step provenance");
            inspect.rollback().await.expect("rollback inspect");
            assert_eq!(provenance.len(), 2);
            assert!(provenance.iter().any(|entry| {
                matches!(
                    entry.source(),
                    ScopeResolutionSource::JobParameter {
                        job_execution_id,
                        parameter,
                    } if *job_execution_id == first_execution_id && parameter == &payload
                )
            }));
            assert!(provenance.iter().any(|entry| {
                matches!(
                    entry.source(),
                    ScopeResolutionSource::Framework {
                        source: ScopeFrameworkSource::StepExecutionId,
                        job_execution_id,
                        step_execution_id: Some(step_execution_id),
                    } if *job_execution_id == second_execution_id
                        && *step_execution_id == second_owner_id
                )
            }));
        });
    }

    #[test]
    fn coercion_failure_rolls_back_provenance() {
        block_on(async {
            let raw = ParameterName::new("raw").expect("raw");
            let component = component(
                "resolver-v1",
                vec![LateBoundInput::new(
                    ParameterName::new("number").expect("number"),
                    LateBoundSource::JobParameter(raw.clone()),
                    ParameterValueKind::U64,
                    ParameterCoercion::StringToU64,
                    MissingParameterPolicy::Fail,
                )],
            );
            let plan = plan(component.clone());
            let mut parameters = JobParameters::new();
            parameters
                .insert(
                    raw,
                    parameter(
                        ParameterValue::string("not-a-number").expect("value"),
                        ParameterRole::Identifying,
                    ),
                )
                .expect("parameter");
            let repository = repository();
            let (instance_id, execution_id) =
                create_execution(&repository, &plan, &parameters).await;

            let error = resolve_scoped_component_inputs(
                &repository,
                &plan,
                &component,
                instance_id,
                execution_id,
                ExecutionAttempt::new(NonZeroU64::MIN),
                None,
            )
            .await
            .expect_err("coercion must fail");
            assert!(matches!(
                error,
                ScopeResolutionError::Resolution(ScopeResolutionFailure::CoercionFailed)
            ));

            let mut inspect = repository.begin().await.expect("inspect");
            let provenance = inspect
                .scope_resolution_provenance(ScopeKind::Job, component.id(), execution_id, None)
                .await
                .expect("provenance");
            inspect.rollback().await.expect("rollback");
            assert!(provenance.is_empty());
        });
    }

    #[test]
    fn missing_source_and_exact_type_mismatch_fail_closed() {
        block_on(async {
            let missing = ParameterName::new("missing").expect("missing");
            let missing_component = component(
                "resolver-v1",
                vec![LateBoundInput::new(
                    missing.clone(),
                    LateBoundSource::JobParameter(missing),
                    ParameterValueKind::String,
                    ParameterCoercion::Exact,
                    MissingParameterPolicy::Fail,
                )],
            );
            let missing_plan = plan(missing_component.clone());
            let missing_repository = repository();
            let parameters = JobParameters::new();
            let (instance_id, execution_id) =
                create_execution(&missing_repository, &missing_plan, &parameters).await;
            let error = resolve_scoped_component_inputs(
                &missing_repository,
                &missing_plan,
                &missing_component,
                instance_id,
                execution_id,
                ExecutionAttempt::new(NonZeroU64::MIN),
                None,
            )
            .await
            .expect_err("missing source must fail");
            assert!(matches!(
                error,
                ScopeResolutionError::Resolution(ScopeResolutionFailure::MissingSource)
            ));

            let actual = ParameterName::new("actual").expect("actual");
            let mismatch_component = component(
                "resolver-v1",
                vec![LateBoundInput::new(
                    actual.clone(),
                    LateBoundSource::JobParameter(actual.clone()),
                    ParameterValueKind::U64,
                    ParameterCoercion::Exact,
                    MissingParameterPolicy::Fail,
                )],
            );
            let mismatch_plan = plan(mismatch_component.clone());
            let mismatch_repository = repository();
            let mut parameters = JobParameters::new();
            parameters
                .insert(
                    actual,
                    parameter(
                        ParameterValue::string("7").expect("value"),
                        ParameterRole::Identifying,
                    ),
                )
                .expect("parameter");
            let (instance_id, execution_id) =
                create_execution(&mismatch_repository, &mismatch_plan, &parameters).await;
            let error = resolve_scoped_component_inputs(
                &mismatch_repository,
                &mismatch_plan,
                &mismatch_component,
                instance_id,
                execution_id,
                ExecutionAttempt::new(NonZeroU64::MIN),
                None,
            )
            .await
            .expect_err("exact type mismatch must fail");
            assert!(matches!(
                error,
                ScopeResolutionError::Resolution(ScopeResolutionFailure::SourceTypeMismatch)
            ));
        });
    }

    #[test]
    fn step_context_reuses_exact_source_version_and_rejects_drift() {
        block_on(async {
            let path = crate::SelectorPath::new([String::from("value")]).expect("path");
            let component = component_for(
                ScopeKind::Step,
                "reader",
                "resolver-v1",
                vec![LateBoundInput::new(
                    ParameterName::new("selected").expect("selected"),
                    LateBoundSource::StepContext {
                        node: NodeId::new("source").expect("source"),
                        schema: StateSchemaId::new("scope.step").expect("schema"),
                        schema_version: StateSchemaVersion::new(1).expect("version"),
                        path,
                    },
                    ParameterValueKind::String,
                    ParameterCoercion::Exact,
                    MissingParameterPolicy::Fail,
                )],
            );
            let plan = plan(component.clone());
            let repository = repository();
            let parameters = JobParameters::new();
            let (instance_id, execution_id) =
                create_execution(&repository, &plan, &parameters).await;
            let source_context = step_context("first");
            let source_id =
                create_source_step(&repository, execution_id, Some(&source_context)).await;
            let owner_id = create_owner_step(&repository, execution_id).await;
            let owner_node = NodeId::new("work").expect("work");
            let owner_name = StepName::new("work").expect("work");
            let owner = ScopeStepOwner {
                execution_id: owner_id,
                node_id: &owner_node,
                step_name: &owner_name,
            };

            let values = resolve_scoped_component_inputs(
                &repository,
                &plan,
                &component,
                instance_id,
                execution_id,
                ExecutionAttempt::new(NonZeroU64::MIN),
                Some(owner),
            )
            .await
            .expect("step resolution");
            assert_eq!(
                values
                    .get(&ParameterName::new("selected").expect("selected"))
                    .and_then(ParameterValue::as_str),
                Some("first")
            );

            let mut inspect = repository.begin().await.expect("inspect");
            let provenance = inspect
                .scope_resolution_provenance(
                    ScopeKind::Step,
                    component.id(),
                    execution_id,
                    Some(owner_id),
                )
                .await
                .expect("provenance");
            let source_before = inspect
                .scope_step_state(source_id)
                .await
                .expect("source")
                .expect("source state")
                .execution()
                .clone();
            inspect.rollback().await.expect("rollback");
            assert_eq!(provenance.len(), 1);
            assert!(matches!(
                provenance[0].source(),
                ScopeResolutionSource::StepContext {
                    step_execution_id: Some(id),
                    execution_version: Some(version),
                    ..
                } if *id == source_id && *version == source_before.version()
            ));

            let replacement = step_context("second");
            let mut mutate = repository.begin().await.expect("mutate");
            mutate
                .commit_step_execution_context(
                    source_before.id(),
                    source_before.version(),
                    &replacement,
                )
                .await
                .expect("mutate source");
            mutate.commit().await.expect("commit mutation");

            let error = resolve_scoped_component_inputs(
                &repository,
                &plan,
                &component,
                instance_id,
                execution_id,
                ExecutionAttempt::new(NonZeroU64::MIN),
                Some(owner),
            )
            .await
            .expect_err("changed source version must fail");
            assert!(matches!(
                error,
                ScopeResolutionError::Resolution(ScopeResolutionFailure::ProvenanceMismatch)
            ));
        });
    }

    #[test]
    fn recorded_step_source_absence_does_not_switch_to_a_later_step() {
        block_on(async {
            let path = crate::SelectorPath::new([String::from("value")]).expect("path");
            let component = component_for(
                ScopeKind::Step,
                "reader",
                "resolver-v1",
                vec![LateBoundInput::new(
                    ParameterName::new("selected").expect("selected"),
                    LateBoundSource::StepContext {
                        node: NodeId::new("source").expect("source"),
                        schema: StateSchemaId::new("scope.step").expect("schema"),
                        schema_version: StateSchemaVersion::new(1).expect("version"),
                        path,
                    },
                    ParameterValueKind::String,
                    ParameterCoercion::Exact,
                    MissingParameterPolicy::TypeDefault,
                )],
            );
            let plan = plan(component.clone());
            let repository = repository();
            let parameters = JobParameters::new();
            let (instance_id, execution_id) =
                create_execution(&repository, &plan, &parameters).await;
            let owner_id = create_owner_step(&repository, execution_id).await;
            let owner_node = NodeId::new("work").expect("work");
            let owner_name = StepName::new("work").expect("work");
            let owner = ScopeStepOwner {
                execution_id: owner_id,
                node_id: &owner_node,
                step_name: &owner_name,
            };

            let values = resolve_scoped_component_inputs(
                &repository,
                &plan,
                &component,
                instance_id,
                execution_id,
                ExecutionAttempt::new(NonZeroU64::MIN),
                Some(owner),
            )
            .await
            .expect("default resolution");
            assert_eq!(
                values
                    .get(&ParameterName::new("selected").expect("selected"))
                    .and_then(ParameterValue::as_str),
                Some("")
            );

            let mut inspect = repository.begin().await.expect("inspect");
            let provenance = inspect
                .scope_resolution_provenance(
                    ScopeKind::Step,
                    component.id(),
                    execution_id,
                    Some(owner_id),
                )
                .await
                .expect("provenance");
            inspect.rollback().await.expect("rollback");
            assert!(matches!(
                provenance[0].source(),
                ScopeResolutionSource::StepContext {
                    step_execution_id: None,
                    execution_version: None,
                    ..
                }
            ));

            let context = step_context("late");
            create_source_step(&repository, execution_id, Some(&context)).await;

            let error = resolve_scoped_component_inputs(
                &repository,
                &plan,
                &component,
                instance_id,
                execution_id,
                ExecutionAttempt::new(NonZeroU64::MIN),
                Some(owner),
            )
            .await
            .expect_err("recorded absence must not switch to a later source");
            assert!(matches!(
                error,
                ScopeResolutionError::Resolution(ScopeResolutionFailure::ProvenanceMismatch)
            ));
        });
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_job_context_source_resolves_and_records_provenance()
    -> Result<(), Box<dyn std::error::Error>> {
        let Some(runtime_url) = std::env::var("OXIDEBATCH_POSTGRES_TEST_URL").ok() else {
            return Ok(());
        };
        let Some(migrator_url) = std::env::var("OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL").ok() else {
            return Ok(());
        };

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let path = crate::SelectorPath::new([String::from("tenant")])?;
            let component = component(
                "resolver-v1",
                vec![LateBoundInput::new(
                    ParameterName::new("tenant")?,
                    LateBoundSource::JobContext {
                        schema: StateSchemaId::new("scope.job")?,
                        schema_version: StateSchemaVersion::new(1)?,
                        path,
                    },
                    ParameterValueKind::String,
                    ParameterCoercion::Exact,
                    MissingParameterPolicy::Fail,
                )],
            );
            let plan = plan(component.clone());
            let identity = plan.definition_identity();

            let migrator_config = crate::PostgresConfig::new(migrator_url.clone())?
                .with_tls_mode(crate::TlsMode::Plaintext);
            crate::PostgresMigrator::migrate(&migrator_config).await?;
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(2)
                .connect(&migrator_url)
                .await?;

            for statement in [
                "DELETE FROM oxide_batch.ob_scope_resolution_provenance WHERE owner_job_execution_id IN (\
                 SELECT execution.id FROM oxide_batch.ob_job_execution execution \
                 JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
                 WHERE instance.job_name = 'scope_job')",
                "DELETE FROM oxide_batch.ob_job_execution WHERE job_instance_id IN (\
                 SELECT id FROM oxide_batch.ob_job_instance WHERE job_name = 'scope_job')",
                "DELETE FROM oxide_batch.ob_job_instance WHERE job_name = 'scope_job'",
                "DELETE FROM oxide_batch.ob_definition_upgrade WHERE from_definition_id IN (\
                 SELECT id FROM oxide_batch.ob_job_definition WHERE job_name = 'scope_job') \
                 OR to_definition_id IN (\
                 SELECT id FROM oxide_batch.ob_job_definition WHERE job_name = 'scope_job')",
                "DELETE FROM oxide_batch.ob_job_definition WHERE job_name = 'scope_job'",
            ] {
                sqlx::query(statement).execute(&pool).await?;
            }

            let manifest: serde_json::Value =
                serde_json::from_slice(identity.canonical_manifest())?;
            let definition_id: i64 = sqlx::query_scalar(
                "INSERT INTO oxide_batch.ob_job_definition (\
                 job_name, definition_revision, manifest_format, manifest_digest, manifest, registered_at) \
                 VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP) RETURNING id",
            )
            .bind(identity.job_name().expect("compiled definition job").as_str())
            .bind(identity.revision().as_str())
            .bind(i16::try_from(identity.manifest_format())?)
            .bind(identity.manifest_digest().to_vec())
            .bind(manifest)
            .fetch_one(&pool)
            .await?;

            let instance_id: i64 = sqlx::query_scalar(
                "INSERT INTO oxide_batch.ob_job_instance (\
                 job_name, instance_key, identifying_parameters, created_at) \
                 VALUES ('scope_job', $1, '{}'::jsonb, CURRENT_TIMESTAMP) RETURNING id",
            )
            .bind(vec![0x71_u8; 32])
            .fetch_one(&pool)
            .await?;

            let execution_id: i64 = sqlx::query_scalar(
                "INSERT INTO oxide_batch.ob_job_execution (\
                 job_instance_id, definition_id, attempt, status, exit_code, parameters, \
                 context_format, context_schema, context_schema_version, context_payload, \
                 created_at, started_at, ended_at, updated_at, version) \
                 VALUES ($1, $2, 1, 'STARTED', 'UNKNOWN', '{}'::jsonb, \
                 1, 'scope.job', 1, '{\"tenant\":\"context-value\"}'::jsonb, \
                 date_trunc('milliseconds', CURRENT_TIMESTAMP), \
                 date_trunc('milliseconds', CURRENT_TIMESTAMP), NULL, \
                 date_trunc('milliseconds', CURRENT_TIMESTAMP), 2) \
                 RETURNING id",
            )
            .bind(instance_id)
            .bind(definition_id)
            .fetch_one(&pool)
            .await?;

            let job_instance_id = JobInstanceId::new(u64::try_from(instance_id)?)?;
            let job_execution_id = JobExecutionId::new(u64::try_from(execution_id)?)?;
            let repository = crate::PostgresJobRepository::connect(
                crate::PostgresConfig::new(runtime_url.clone())?
                    .with_tls_mode(crate::TlsMode::Plaintext),
                Arc::new(SystemClock),
            )
            .await?;

            let values = resolve_scoped_component_inputs(
                &repository,
                &plan,
                &component,
                job_instance_id,
                job_execution_id,
                ExecutionAttempt::new(NonZeroU64::MIN),
                None,
            )
            .await
            .map_err(|error| format!("scope resolution failed: {error:?}"))?;
            assert_eq!(
                values
                    .get(&ParameterName::new("tenant")?)
                    .and_then(ParameterValue::as_str),
                Some("context-value")
            );

            let mut inspect = repository.begin().await?;
            let provenance = inspect
                .scope_resolution_provenance(ScopeKind::Job, component.id(), job_execution_id, None)
                .await?;
            inspect.rollback().await?;
            assert_eq!(provenance.len(), 1);
            assert!(matches!(
                provenance[0].source(),
                ScopeResolutionSource::JobContext {
                    job_execution_id: source_execution,
                    execution_version,
                    schema,
                    schema_version,
                    path,
                } if *source_execution == job_execution_id
                    && execution_version.get() == 2
                    && schema.as_str() == "scope.job"
                    && schema_version.get() == 1
                    && path.as_slice() == ["tenant"]
            ));

            let mut stop = repository.begin().await?;
            let stopped = stop
                .transition_job_execution(
                    job_execution_id,
                    ExecutionVersion::new(2),
                    LifecycleTransition::new(
                        BatchStatus::Stopped,
                        std::time::SystemTime::now(),
                    ),
                )
                .await?;
            assert_eq!(stopped.version().get(), 3);
            stop.commit().await?;

            let mut restart = repository.begin().await?;
            let restarted = restart
                .create_job_execution_with_definition_and_parameters(
                    job_instance_id,
                    identity,
                    &JobParameters::new(),
                )
                .await?;
            restart.commit().await?;
            assert_ne!(restarted.id(), job_execution_id);

            let restarted_values = resolve_scoped_component_inputs(
                &repository,
                &plan,
                &component,
                job_instance_id,
                restarted.id(),
                ExecutionAttempt::new(NonZeroU64::new(2).expect("restart attempt")),
                None,
            )
            .await
            .map_err(|error| format!("restart scope resolution failed: {error:?}"))?;
            assert_eq!(
                restarted_values
                    .get(&ParameterName::new("tenant")?)
                    .and_then(ParameterValue::as_str),
                Some("context-value"),
                "restart must read the referenced prior context, not the new empty execution context",
            );

            let mut inspect = repository.begin().await?;
            let restarted_provenance = inspect
                .scope_resolution_provenance(
                    ScopeKind::Job,
                    component.id(),
                    restarted.id(),
                    None,
                )
                .await?;
            inspect.rollback().await?;
            assert_eq!(restarted_provenance.len(), 1);
            assert!(matches!(
                restarted_provenance[0].source(),
                ScopeResolutionSource::JobContext {
                    job_execution_id: source_execution,
                    execution_version,
                    ..
                } if *source_execution == job_execution_id && execution_version.get() == 2
            ));

            repository.close().await?;
            sqlx::query(
                "DELETE FROM oxide_batch.ob_scope_resolution_provenance \
                 WHERE owner_job_execution_id IN ($1, $2)",
            )
            .bind(execution_id)
            .bind(i64::try_from(restarted.id().get())?)
            .execute(&pool)
            .await?;
            sqlx::query("DELETE FROM oxide_batch.ob_job_execution WHERE id = $1")
                .bind(i64::try_from(restarted.id().get())?)
                .execute(&pool)
                .await?;
            sqlx::query("DELETE FROM oxide_batch.ob_job_execution WHERE id = $1")
                .bind(execution_id)
                .execute(&pool)
                .await?;
            sqlx::query("DELETE FROM oxide_batch.ob_job_instance WHERE id = $1")
                .bind(instance_id)
                .execute(&pool)
                .await?;
            sqlx::query("DELETE FROM oxide_batch.ob_job_definition WHERE id = $1")
                .bind(definition_id)
                .execute(&pool)
                .await?;
            pool.close().await;
            Ok::<(), Box<dyn std::error::Error>>(())
        })
    }

    #[test]
    fn caller_attempt_must_match_the_durable_execution_attempt() {
        block_on(async {
            let component = component(
                "resolver-v1",
                vec![LateBoundInput::new(
                    ParameterName::new("attempt").expect("attempt"),
                    LateBoundSource::Framework(ScopeFrameworkSource::Attempt),
                    ParameterValueKind::U64,
                    ParameterCoercion::Exact,
                    MissingParameterPolicy::Fail,
                )],
            );
            let plan = plan(component.clone());
            let repository = repository();
            let parameters = JobParameters::new();
            let (instance_id, execution_id) =
                create_execution(&repository, &plan, &parameters).await;

            let error = resolve_scoped_component_inputs(
                &repository,
                &plan,
                &component,
                instance_id,
                execution_id,
                ExecutionAttempt::new(NonZeroU64::new(2).expect("attempt")),
                None,
            )
            .await
            .expect_err("caller attempt drift must fail");
            assert!(matches!(
                error,
                ScopeResolutionError::Resolution(ScopeResolutionFailure::ProvenanceMismatch)
            ));

            let mut inspect = repository.begin().await.expect("inspect");
            let provenance = inspect
                .scope_resolution_provenance(ScopeKind::Job, component.id(), execution_id, None)
                .await
                .expect("provenance");
            inspect.rollback().await.expect("rollback");
            assert!(provenance.is_empty());
        });
    }

    #[test]
    fn resolver_contract_drift_is_rejected_before_re_resolution() {
        block_on(async {
            let tenant = ParameterName::new("tenant").expect("tenant");
            let input = LateBoundInput::new(
                tenant.clone(),
                LateBoundSource::JobParameter(tenant.clone()),
                ParameterValueKind::String,
                ParameterCoercion::Exact,
                MissingParameterPolicy::Fail,
            );
            let original = component("resolver-v1", vec![input.clone()]);
            let original_plan = plan(original.clone());
            let drifted = component("resolver-v2", vec![input]);
            let drifted_plan = plan(drifted.clone());

            let mut parameters = JobParameters::new();
            parameters
                .insert(
                    tenant,
                    parameter(
                        ParameterValue::string("stable").expect("value"),
                        ParameterRole::Identifying,
                    ),
                )
                .expect("parameter");
            let repository = repository();
            let (instance_id, execution_id) =
                create_execution(&repository, &original_plan, &parameters).await;

            resolve_scoped_component_inputs(
                &repository,
                &original_plan,
                &original,
                instance_id,
                execution_id,
                ExecutionAttempt::new(NonZeroU64::MIN),
                None,
            )
            .await
            .expect("original resolution");

            let error = resolve_scoped_component_inputs(
                &repository,
                &drifted_plan,
                &drifted,
                instance_id,
                execution_id,
                ExecutionAttempt::new(NonZeroU64::MIN),
                None,
            )
            .await
            .expect_err("drift must fail");
            assert!(matches!(
                error,
                ScopeResolutionError::Resolution(ScopeResolutionFailure::ProvenanceMismatch)
            ));
        });
    }
}
