//! Portable plain-fork orchestration within one repository-owned unit of work.
//!
//! Transformed fork is deliberately not inferred from compatible restart.

use crate::{
    DefinitionStateKind, DefinitionStateTransformer, ExecutionContext, ForkLineage, ForkRequest,
    JobExecution, JobInstanceId, JobInstanceSelection, JobRepository, RepositoryCapability,
    RepositoryError, RepositoryUnitOfWork, StateLimits,
};
use sha2::{Digest, Sha256};

pub(crate) enum ForkStage {
    Existing(ForkLineage),
    Created {
        lineage: ForkLineage,
        execution: JobExecution,
    },
}

pub(crate) enum ForkStageError {
    Repository(RepositoryError),
    TargetCollision(JobInstanceId),
}

impl From<RepositoryError> for ForkStageError {
    fn from(value: RepositoryError) -> Self {
        Self::Repository(value)
    }
}

/// Starts an explicit new instance and publishes its immutable lineage.
#[derive(Clone, Debug)]
pub struct JobFork<R> {
    repository: R,
}

impl<R: JobRepository> JobFork<R> {
    /// Binds a repository implementing atomic fork primitives.
    #[must_use]
    pub const fn new(repository: R) -> Self {
        Self { repository }
    }

    /// Looks up one exact idempotency key without mutating source or target.
    ///
    /// # Errors
    /// Rejects missing capability, corrupt lineage or repository errors.
    pub async fn lineage(
        &self,
        operation_id: &crate::OperationId,
    ) -> Result<Option<ForkLineage>, RepositoryError> {
        self.repository
            .descriptor()
            .require(RepositoryCapability::DefinitionRegistry)?;
        let mut unit = self.repository.begin().await?;
        let found = unit.fork_lineage(operation_id).await?;
        unit.rollback().await?;
        Ok(found)
    }

    /// Atomically starts one new instance; never calls compatible-restart APIs.
    ///
    /// # Errors
    /// Rejects stale/nonterminal sources, missing registrations, target key
    /// collisions, divergent replays, and incomplete transaction commits.
    pub async fn fork(&self, request: &ForkRequest) -> Result<ForkLineage, RepositoryError> {
        self.fork_inner(request, None).await
    }

    /// Forks a terminal source with one explicit, deterministic context transform.
    ///
    /// # Errors
    /// Rejects a missing descriptor, mismatched transformer revision, corrupt
    /// source state or unsupported family before new target state is committed.
    pub async fn fork_transformed(
        &self,
        request: &ForkRequest,
        transformer: &DefinitionStateTransformer,
    ) -> Result<ForkLineage, RepositoryError> {
        self.fork_inner(request, Some(transformer)).await
    }

    async fn fork_inner(
        &self,
        request: &ForkRequest,
        transformer: Option<&DefinitionStateTransformer>,
    ) -> Result<ForkLineage, RepositoryError> {
        if request.context_transform().is_some() != transformer.is_some() {
            return Err(RepositoryError::ForkTransformRejected);
        }
        self.repository
            .descriptor()
            .require(RepositoryCapability::DefinitionRegistry)?;
        self.repository
            .descriptor()
            .require(RepositoryCapability::ScopeResolution)?;
        let mut unit = self.repository.begin().await?;
        match stage_fork_in_unit(unit.as_mut(), request, transformer).await {
            Ok(ForkStage::Existing(existing)) => {
                unit.rollback().await?;
                Ok(existing)
            }
            Ok(ForkStage::Created { lineage, .. }) => match unit.commit().await {
                Ok(()) => Ok(lineage),
                Err(
                    RepositoryError::CommitOutcomeUnknown | RepositoryError::ConcurrentModification,
                ) => match self.lineage(request.operation_id()).await? {
                    Some(stored) if stored.matches_request(request) => Ok(stored),
                    Some(_) => Err(RepositoryError::ForkOperationConflict),
                    None => Err(RepositoryError::CommitOutcomeUnknown),
                },
                Err(error) => Err(error),
            },
            Err(ForkStageError::TargetCollision(id)) => {
                unit.rollback().await?;
                match self.lineage(request.operation_id()).await? {
                    Some(stored) if stored.matches_request(request) => Ok(stored),
                    Some(_) => Err(RepositoryError::ForkOperationConflict),
                    None => Err(RepositoryError::ForkInstanceCollision { id }),
                }
            }
            Err(ForkStageError::Repository(error)) => {
                let _ = unit.rollback().await;
                Err(error)
            }
        }
    }
}

pub(crate) async fn stage_fork_in_unit(
    unit: &mut dyn RepositoryUnitOfWork,
    request: &ForkRequest,
    transformer: Option<&DefinitionStateTransformer>,
) -> Result<ForkStage, ForkStageError> {
    if request.context_transform().is_some() != transformer.is_some() {
        return Err(RepositoryError::ForkTransformRejected.into());
    }
    if let Some(existing) = unit.fork_lineage(request.operation_id()).await? {
        return if existing.matches_request(request) {
            Ok(ForkStage::Existing(existing))
        } else {
            Err(RepositoryError::ForkOperationConflict.into())
        };
    }

    let source = unit
        .lock_fork_source(request.source_execution_id())
        .await?
        .ok_or(RepositoryError::ForkSourceStale)?;
    if source.version() != request.source_version() {
        return Err(RepositoryError::ForkSourceStale.into());
    }
    if !source.metadata().status().is_finished() {
        return Err(RepositoryError::ForkSourceNotTerminal.into());
    }
    let attempts = unit.job_executions(source.job_instance_id()).await?;
    if attempts.last().map(crate::JobExecution::id) != Some(source.id()) {
        return Err(RepositoryError::ForkSourceStale.into());
    }
    let source_definition = unit
        .scope_execution_definition(source.id())
        .await
        .map_err(|_| RepositoryError::ForkStateCorrupt)?;
    let source_name = source_definition
        .job_name()
        .ok_or(RepositoryError::ForkStateCorrupt)?;
    let registered_source = unit
        .definition(source_name, source_definition.revision())
        .await?
        .ok_or_else(|| RepositoryError::DefinitionNotRegistered {
            job_name: source_name.clone(),
            revision: source_definition.revision().clone(),
        })?;
    if registered_source.definition() != &source_definition {
        return Err(RepositoryError::ForkStateCorrupt.into());
    }

    let target = request.target();
    let registered_target = unit
        .definition(target.job_name(), target.revision())
        .await?
        .ok_or_else(|| RepositoryError::DefinitionNotRegistered {
            job_name: target.job_name().clone(),
            revision: target.revision().clone(),
        })?;
    if registered_target.definition() != target.definition() {
        return Err(RepositoryError::DefinitionDrift {
            job_name: target.job_name().clone(),
            revision: target.revision().clone(),
        }
        .into());
    }
    if registered_target.artifact() != target.artifact() {
        return Err(RepositoryError::DefinitionArtifactDrift {
            job_name: target.job_name().clone(),
            revision: target.revision().clone(),
        }
        .into());
    }

    let mut steps = unit.step_executions(source.id()).await?;
    if steps.len() > 1024 {
        return Err(RepositoryError::ForkStateCorrupt.into());
    }
    steps.sort_by_key(crate::StepExecution::id);
    let mut hash = Sha256::new();
    hash.update(b"oxide-batch.fork-source-committed-state.v2\0");
    hash.update(source.id().get().to_be_bytes());
    hash.update(source.version().get().to_be_bytes());
    hash.update(source.metadata().status().as_str().as_bytes());
    for step in steps {
        let committed = unit
            .scope_step_state(step.id())
            .await?
            .ok_or(RepositoryError::ForkStateCorrupt)?;
        if committed.execution() != &step || committed.execution().job_execution_id() != source.id()
        {
            return Err(RepositoryError::ForkStateCorrupt.into());
        }
        hash.update(step.id().get().to_be_bytes());
        hash.update(step.version().get().to_be_bytes());
        hash.update(step.metadata().status().as_str().as_bytes());
        hash.update(committed.node_id().as_str().as_bytes());
        if let Some(context) = committed.context() {
            let data = context
                .to_json()
                .map_err(|_| RepositoryError::ForkStateCorrupt)?;
            hash.update([1_u8]);
            hash.update(
                u64::try_from(data.len())
                    .map_err(|_| RepositoryError::ForkStateCorrupt)?
                    .to_be_bytes(),
            );
            hash.update(&data);
        } else {
            hash.update([0_u8]);
        }
    }
    let source_state_digest: [u8; 32] = hash.finalize().into();

    let transformed_context: Option<ExecutionContext> =
        if let Some(selected) = request.context_transform() {
            let descriptor = selected.descriptor();
            let live = transformer.ok_or(RepositoryError::ForkTransformRejected)?;
            if descriptor.kind() != DefinitionStateKind::ExecutionContext
                || descriptor.revision() != live.revision()
            {
                return Err(RepositoryError::ForkTransformRejected.into());
            }
            if !definition_contains_node(&source_definition, descriptor.source_node())
                || !definition_contains_node(target.definition(), descriptor.target_node())
            {
                return Err(RepositoryError::ForkTransformRejected.into());
            }
            let source_step = unit
                .scope_step_state(selected.source_step_execution_id())
                .await?
                .ok_or(RepositoryError::ForkTransformRejected)?;
            if source_step.execution().job_execution_id() != source.id()
                || source_step.execution().version() != selected.source_step_version()
                || source_step.node_id() != descriptor.source_node()
            {
                return Err(RepositoryError::ForkTransformRejected.into());
            }
            let context = source_step
                .context()
                .ok_or(RepositoryError::ForkTransformRejected)?;
            if context.schema_id() != descriptor.source_schema()
                || context.schema_version() != descriptor.source_version()
            {
                return Err(RepositoryError::ForkTransformRejected.into());
            }
            let source_bytes = context
                .payload_json()
                .map_err(|_| RepositoryError::ForkTransformRejected)?;
            let output = live
                .apply(&source_bytes)
                .map_err(|_| RepositoryError::ForkTransformRejected)?;
            Some(
                ExecutionContext::from_transformed_payload(
                    descriptor.target_schema().clone(),
                    descriptor.target_version(),
                    &output,
                    StateLimits::default(),
                )
                .map_err(|_| RepositoryError::ForkTransformRejected)?,
            )
        } else {
            None
        };

    let key = request.target_key();
    let instance = match unit.select_or_create_job_instance(&key).await? {
        JobInstanceSelection::Created(created) => created,
        JobInstanceSelection::Existing(existing) => {
            return Err(ForkStageError::TargetCollision(existing.id()));
        }
        _ => return Err(RepositoryError::ForkStateCorrupt.into()),
    };
    let execution = unit
        .create_job_execution_with_definition_and_parameters(
            instance.id(),
            target.definition(),
            request.parameters(),
        )
        .await?;
    if let Some(context) = transformed_context.as_ref() {
        unit.stage_fork_job_context(execution.id(), context).await?;
    }
    let lineage = ForkLineage::new(
        request,
        &source_definition,
        source_state_digest,
        instance.id(),
        execution.id(),
    );
    unit.append_fork_lineage(&lineage).await?;
    Ok(ForkStage::Created { lineage, execution })
}

/// Checks a declared node against the exact immutable definition manifest.
fn definition_contains_node(definition: &crate::DefinitionIdentity, node: &crate::NodeId) -> bool {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(definition.canonical_manifest())
    else {
        return false;
    };
    value.get("step").and_then(serde_json::Value::as_str) == Some(node.as_str())
        || value
            .get("nodes")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|nodes| {
                nodes.iter().any(|item| {
                    item.get("id").and_then(serde_json::Value::as_str) == Some(node.as_str())
                })
            })
}
