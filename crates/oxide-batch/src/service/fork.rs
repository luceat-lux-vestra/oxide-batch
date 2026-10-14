//! Portable plain-fork orchestration within one repository-owned unit of work.
//!
//! Transformed fork is deliberately not inferred from compatible restart.

use crate::{
    ForkLineage, ForkRequest, JobInstanceSelection, JobRepository, RepositoryCapability,
    RepositoryError,
};
use sha2::{Digest, Sha256};

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
        self.repository
            .descriptor()
            .require(RepositoryCapability::DefinitionRegistry)?;
        self.repository
            .descriptor()
            .require(RepositoryCapability::ScopeResolution)?;
        let mut unit = self.repository.begin().await?;

        if let Some(existing) = unit.fork_lineage(request.operation_id()).await? {
            unit.rollback().await?;
            return if existing.matches_request(request) {
                Ok(existing)
            } else {
                Err(RepositoryError::ForkOperationConflict)
            };
        }

        let source = unit
            .lock_fork_source(request.source_execution_id())
            .await?
            .ok_or(RepositoryError::ForkSourceStale)?;
        if source.version() != request.source_version() {
            return Err(RepositoryError::ForkSourceStale);
        }
        if !source.metadata().status().is_finished() {
            return Err(RepositoryError::ForkSourceNotTerminal);
        }
        let attempts = unit.job_executions(source.job_instance_id()).await?;
        if attempts.last().map(crate::JobExecution::id) != Some(source.id()) {
            return Err(RepositoryError::ForkSourceStale);
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
            return Err(RepositoryError::ForkStateCorrupt);
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
            });
        }
        if registered_target.artifact() != target.artifact() {
            return Err(RepositoryError::DefinitionArtifactDrift {
                job_name: target.job_name().clone(),
                revision: target.revision().clone(),
            });
        }

        let mut steps = unit.step_executions(source.id()).await?;
        if steps.len() > 1024 {
            return Err(RepositoryError::ForkStateCorrupt);
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
            if committed.execution() != &step
                || committed.execution().job_execution_id() != source.id()
            {
                return Err(RepositoryError::ForkStateCorrupt);
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
                hash.update(u64::try_from(data.len())
                    .map_err(|_| RepositoryError::ForkStateCorrupt)?
                    .to_be_bytes());
                hash.update(&data);
            } else {
                hash.update([0_u8]);
            }
        }
        let source_state_digest: [u8; 32] = hash.finalize().into();

        let key = request.target_key();
        let instance = match unit.select_or_create_job_instance(&key).await? {
            JobInstanceSelection::Created(created) => created,
            JobInstanceSelection::Existing(existing) => {
                // The unique target key can race with a concurrent request.
                // Reconcile the exact immutable operation before declaring a collision.
                unit.rollback().await?;
                return match self.lineage(request.operation_id()).await? {
                    Some(stored) if stored.matches_request(request) => Ok(stored),
                    Some(_) => Err(RepositoryError::ForkOperationConflict),
                    None => Err(RepositoryError::ForkInstanceCollision { id: existing.id() }),
                };
            }
            _ => return Err(RepositoryError::ForkStateCorrupt),
        };
        let execution = unit
            .create_job_execution_with_definition_and_parameters(
                instance.id(),
                target.definition(),
                request.parameters(),
            )
            .await?;
        let lineage = ForkLineage::new(
            request,
            &source_definition,
            source_state_digest,
            instance.id(),
            execution.id(),
        );
        unit.append_fork_lineage(&lineage).await?;
        match unit.commit().await {
            Ok(()) => Ok(lineage),
            Err(
                RepositoryError::CommitOutcomeUnknown | RepositoryError::ConcurrentModification,
            ) => {
                // A new transaction, never a blind retry of instance creation.
                match self.lineage(request.operation_id()).await? {
                    Some(stored) if stored.matches_request(request) => Ok(stored),
                    Some(_) => Err(RepositoryError::ForkOperationConflict),
                    None => Err(RepositoryError::CommitOutcomeUnknown),
                }
            }
            Err(error) => Err(error),
        }
    }
}
