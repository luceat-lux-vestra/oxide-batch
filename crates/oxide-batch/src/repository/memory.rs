use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use super::{
    BoxFuture, Clock, IdGenerator, JobInstanceSelection, JobRepository, RepositoryError,
    RepositoryUnitOfWork,
};
use crate::{
    BatchStatus, ExecutionCounts, ExecutionMetadata, ExecutionTimestamps, ExecutionVersion,
    ExitStatus, IdentifierKind, JobExecution, JobExecutionId, JobInstance, JobInstanceId,
    JobInstanceKey, LifecycleTransition, StepExecution, StepExecutionId, StepName,
};

/// Deterministic, process-local reference implementation of [`JobRepository`].
///
/// Each unit of work operates on an isolated snapshot and publishes it with a
/// repository-wide compare-and-swap commit. Concurrent commits therefore have
/// one winner; losers receive [`RepositoryError::ConcurrentModification`] and
/// can retry from a fresh snapshot. No state survives process termination.
#[derive(Clone)]
pub struct InMemoryJobRepository {
    state: Arc<Mutex<MemoryState>>,
    clock: Arc<dyn Clock>,
    ids: Arc<dyn IdGenerator>,
}

impl InMemoryJobRepository {
    /// Constructs an empty repository with explicitly injected time and IDs.
    #[must_use]
    pub fn new(clock: Arc<dyn Clock>, ids: Arc<dyn IdGenerator>) -> Self {
        Self {
            state: Arc::new(Mutex::new(MemoryState::default())),
            clock,
            ids,
        }
    }
}

impl fmt::Debug for InMemoryJobRepository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("InMemoryJobRepository");
        match self.state.lock() {
            Ok(state) => debug
                .field("revision", &state.revision)
                .field("job_instance_count", &state.instances_by_id.len())
                .field("job_execution_count", &state.job_executions.len())
                .field("step_execution_count", &state.step_executions.len()),
            Err(_) => debug.field("state", &"<poisoned>"),
        };
        debug.finish_non_exhaustive()
    }
}

impl JobRepository for InMemoryJobRepository {
    fn begin<'a>(
        &'a self,
    ) -> BoxFuture<'a, Result<Box<dyn RepositoryUnitOfWork + 'a>, RepositoryError>> {
        Box::pin(async move {
            let snapshot = self
                .state
                .lock()
                .map_err(|_| RepositoryError::Unavailable)?
                .clone();
            let base_revision = snapshot.revision;
            Ok(Box::new(InMemoryUnitOfWork {
                repository: self,
                base_revision,
                staged: snapshot,
            }) as Box<dyn RepositoryUnitOfWork + 'a>)
        })
    }
}

#[derive(Clone, Debug, Default)]
struct MemoryState {
    revision: u64,
    instances_by_key: BTreeMap<JobInstanceKey, JobInstanceId>,
    instances_by_id: BTreeMap<JobInstanceId, JobInstance>,
    job_executions: BTreeMap<JobExecutionId, JobExecution>,
    job_executions_by_instance: BTreeMap<JobInstanceId, Vec<JobExecutionId>>,
    step_executions: BTreeMap<StepExecutionId, StepExecution>,
    step_executions_by_job: BTreeMap<JobExecutionId, Vec<StepExecutionId>>,
}

struct InMemoryUnitOfWork<'repository> {
    repository: &'repository InMemoryJobRepository,
    base_revision: u64,
    staged: MemoryState,
}

impl InMemoryUnitOfWork<'_> {
    fn create_starting_metadata(&self) -> Result<ExecutionMetadata, RepositoryError> {
        let created_at = self.repository.clock.now();
        let timestamps = ExecutionTimestamps::new(created_at, None, None)?;
        ExecutionMetadata::new(
            BatchStatus::Starting,
            ExitStatus::unknown(),
            timestamps,
            ExecutionCounts::default(),
            None,
        )
        .map_err(RepositoryError::from)
    }

    fn latest_job_execution(
        &self,
        instance_id: JobInstanceId,
    ) -> Result<Option<&JobExecution>, RepositoryError> {
        let Some(execution_ids) = self.staged.job_executions_by_instance.get(&instance_id) else {
            if self.staged.instances_by_id.contains_key(&instance_id) {
                return Ok(None);
            }
            return Err(RepositoryError::JobInstanceNotFound { id: instance_id });
        };
        Ok(execution_ids
            .last()
            .and_then(|id| self.staged.job_executions.get(id)))
    }
}

impl RepositoryUnitOfWork for InMemoryUnitOfWork<'_> {
    fn select_or_create_job_instance<'a>(
        &'a mut self,
        key: &'a JobInstanceKey,
    ) -> BoxFuture<'a, Result<JobInstanceSelection, RepositoryError>> {
        Box::pin(async move {
            if let Some(id) = self.staged.instances_by_key.get(key) {
                let instance = self
                    .staged
                    .instances_by_id
                    .get(id)
                    .cloned()
                    .ok_or(RepositoryError::JobInstanceNotFound { id: *id })?;
                return Ok(JobInstanceSelection::Existing(instance));
            }

            let id = self.repository.ids.next_job_instance_id()?;
            if self.staged.instances_by_id.contains_key(&id) {
                return Err(RepositoryError::DuplicateIdentifier {
                    kind: IdentifierKind::JobInstance,
                    value: id.get(),
                });
            }
            let instance = JobInstance::new(id, key.clone());
            self.staged.instances_by_key.insert(key.clone(), id);
            self.staged.instances_by_id.insert(id, instance.clone());
            self.staged
                .job_executions_by_instance
                .insert(id, Vec::new());
            Ok(JobInstanceSelection::Created(instance))
        })
    }

    fn create_job_execution(
        &mut self,
        job_instance_id: JobInstanceId,
    ) -> BoxFuture<'_, Result<JobExecution, RepositoryError>> {
        Box::pin(async move {
            if let Some(latest) = self.latest_job_execution(job_instance_id)? {
                match latest.metadata().status() {
                    BatchStatus::Stopped | BatchStatus::Failed => {}
                    BatchStatus::Completed => {
                        return Err(RepositoryError::CompletedInstance {
                            id: job_instance_id,
                        });
                    }
                    BatchStatus::Abandoned => {
                        return Err(RepositoryError::AbandonedInstance {
                            id: job_instance_id,
                        });
                    }
                    status => {
                        return Err(RepositoryError::ExecutionAlreadyActive {
                            instance_id: job_instance_id,
                            execution_id: latest.id(),
                            status,
                        });
                    }
                }
            }

            let id = self.repository.ids.next_job_execution_id()?;
            if self.staged.job_executions.contains_key(&id) {
                return Err(RepositoryError::DuplicateIdentifier {
                    kind: IdentifierKind::JobExecution,
                    value: id.get(),
                });
            }
            let execution =
                JobExecution::new(id, job_instance_id, self.create_starting_metadata()?);
            self.staged.job_executions.insert(id, execution.clone());
            self.staged
                .job_executions_by_instance
                .get_mut(&job_instance_id)
                .ok_or(RepositoryError::JobInstanceNotFound {
                    id: job_instance_id,
                })?
                .push(id);
            Ok(execution)
        })
    }

    fn create_step_execution<'a>(
        &'a mut self,
        job_execution_id: JobExecutionId,
        step_name: &'a StepName,
    ) -> BoxFuture<'a, Result<StepExecution, RepositoryError>> {
        Box::pin(async move {
            if !self.staged.job_executions.contains_key(&job_execution_id) {
                return Err(RepositoryError::JobExecutionNotFound {
                    id: job_execution_id,
                });
            }
            let id = self.repository.ids.next_step_execution_id()?;
            if self.staged.step_executions.contains_key(&id) {
                return Err(RepositoryError::DuplicateIdentifier {
                    kind: IdentifierKind::StepExecution,
                    value: id.get(),
                });
            }
            let execution = StepExecution::new(
                id,
                job_execution_id,
                step_name.clone(),
                self.create_starting_metadata()?,
            );
            self.staged.step_executions.insert(id, execution.clone());
            self.staged
                .step_executions_by_job
                .entry(job_execution_id)
                .or_default()
                .push(id);
            Ok(execution)
        })
    }

    fn transition_job_execution(
        &mut self,
        id: JobExecutionId,
        expected_version: ExecutionVersion,
        transition: LifecycleTransition,
    ) -> BoxFuture<'_, Result<JobExecution, RepositoryError>> {
        Box::pin(async move {
            let execution = self
                .staged
                .job_executions
                .get_mut(&id)
                .ok_or(RepositoryError::JobExecutionNotFound { id })?;
            execution.transition(expected_version, transition)?;
            Ok(execution.clone())
        })
    }

    fn enrich_job_exit_status<'a>(
        &'a mut self,
        id: JobExecutionId,
        expected_version: ExecutionVersion,
        exit_status: &'a ExitStatus,
    ) -> BoxFuture<'a, Result<JobExecution, RepositoryError>> {
        Box::pin(async move {
            let execution = self
                .staged
                .job_executions
                .get_mut(&id)
                .ok_or(RepositoryError::JobExecutionNotFound { id })?;
            execution.enrich_exit_status(expected_version, exit_status.clone())?;
            Ok(execution.clone())
        })
    }

    fn transition_step_execution(
        &mut self,
        id: StepExecutionId,
        expected_version: ExecutionVersion,
        transition: LifecycleTransition,
    ) -> BoxFuture<'_, Result<StepExecution, RepositoryError>> {
        Box::pin(async move {
            let execution = self
                .staged
                .step_executions
                .get_mut(&id)
                .ok_or(RepositoryError::StepExecutionNotFound { id })?;
            execution.transition(expected_version, transition)?;
            Ok(execution.clone())
        })
    }

    fn enrich_step_exit_status<'a>(
        &'a mut self,
        id: StepExecutionId,
        expected_version: ExecutionVersion,
        exit_status: &'a ExitStatus,
    ) -> BoxFuture<'a, Result<StepExecution, RepositoryError>> {
        Box::pin(async move {
            let execution = self
                .staged
                .step_executions
                .get_mut(&id)
                .ok_or(RepositoryError::StepExecutionNotFound { id })?;
            execution.enrich_exit_status(expected_version, exit_status.clone())?;
            Ok(execution.clone())
        })
    }

    fn find_job_instance<'a>(
        &'a mut self,
        key: &'a JobInstanceKey,
    ) -> BoxFuture<'a, Result<Option<JobInstance>, RepositoryError>> {
        Box::pin(async move {
            Ok(self
                .staged
                .instances_by_key
                .get(key)
                .and_then(|id| self.staged.instances_by_id.get(id))
                .cloned())
        })
    }

    fn get_job_execution(
        &mut self,
        id: JobExecutionId,
    ) -> BoxFuture<'_, Result<Option<JobExecution>, RepositoryError>> {
        Box::pin(async move { Ok(self.staged.job_executions.get(&id).cloned()) })
    }

    fn get_job_instance(
        &mut self,
        id: JobInstanceId,
    ) -> BoxFuture<'_, Result<Option<JobInstance>, RepositoryError>> {
        Box::pin(async move { Ok(self.staged.instances_by_id.get(&id).cloned()) })
    }

    fn job_executions(
        &mut self,
        job_instance_id: JobInstanceId,
    ) -> BoxFuture<'_, Result<Vec<JobExecution>, RepositoryError>> {
        Box::pin(async move {
            if !self.staged.instances_by_id.contains_key(&job_instance_id) {
                return Err(RepositoryError::JobInstanceNotFound {
                    id: job_instance_id,
                });
            }
            Ok(self
                .staged
                .job_executions_by_instance
                .get(&job_instance_id)
                .into_iter()
                .flatten()
                .filter_map(|id| self.staged.job_executions.get(id))
                .cloned()
                .collect())
        })
    }

    fn get_step_execution(
        &mut self,
        id: StepExecutionId,
    ) -> BoxFuture<'_, Result<Option<StepExecution>, RepositoryError>> {
        Box::pin(async move { Ok(self.staged.step_executions.get(&id).cloned()) })
    }

    fn step_executions(
        &mut self,
        job_execution_id: JobExecutionId,
    ) -> BoxFuture<'_, Result<Vec<StepExecution>, RepositoryError>> {
        Box::pin(async move {
            if !self.staged.job_executions.contains_key(&job_execution_id) {
                return Err(RepositoryError::JobExecutionNotFound {
                    id: job_execution_id,
                });
            }
            Ok(self
                .staged
                .step_executions_by_job
                .get(&job_execution_id)
                .into_iter()
                .flatten()
                .filter_map(|id| self.staged.step_executions.get(id))
                .cloned()
                .collect())
        })
    }

    fn commit<'a>(self: Box<Self>) -> BoxFuture<'a, Result<(), RepositoryError>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            let mut current = self
                .repository
                .state
                .lock()
                .map_err(|_| RepositoryError::Unavailable)?;
            if current.revision != self.base_revision {
                return Err(RepositoryError::ConcurrentModification);
            }
            let mut staged = self.staged;
            staged.revision = current
                .revision
                .checked_add(1)
                .ok_or(RepositoryError::ConcurrentModification)?;
            *current = staged;
            Ok(())
        })
    }

    fn rollback<'a>(self: Box<Self>) -> BoxFuture<'a, Result<(), RepositoryError>>
    where
        Self: 'a,
    {
        Box::pin(async move { Ok(()) })
    }
}
