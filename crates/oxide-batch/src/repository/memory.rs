use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use super::{
    BoxFuture, Clock, IdGenerator, JobInstanceSelection, JobRepository, RecoveryDecision,
    RecoveryRequest, RecoveryResult, RepositoryError, RepositoryUnitOfWork, recovered_execution,
};
use crate::{
    BatchStatus, DefinitionIdentity, DefinitionRevision, DefinitionUpgrade, ExecutionCounts,
    ExecutionMetadata, ExecutionTimestamps, ExecutionVersion, ExitStatus, FlowDecision,
    FlowDecisionId, FlowDecisionRequest, FlowStepState, FlowTransitionKind, IdentifierKind,
    JobExecution, JobExecutionId, JobInstance, JobInstanceId, JobInstanceKey, JobName,
    LifecycleTransition, NodeId, StartLimit, StepExecution, StepExecutionId, StepName,
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
                definition_override: None,
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
    step_logical_ids: BTreeMap<StepExecutionId, NodeId>,
    flow_decisions: BTreeMap<FlowDecisionId, FlowDecision>,
    flow_decisions_by_job: BTreeMap<JobExecutionId, Vec<FlowDecisionId>>,
    recovery_decisions: BTreeMap<JobExecutionId, Vec<RecoveryDecision>>,
    definitions: BTreeMap<(JobName, DefinitionRevision), DefinitionIdentity>,
    execution_definitions: BTreeMap<JobExecutionId, DefinitionIdentity>,
    definition_upgrades: BTreeMap<(JobName, [u8; 32], [u8; 32]), DefinitionUpgrade>,
}

struct InMemoryUnitOfWork<'repository> {
    repository: &'repository InMemoryJobRepository,
    base_revision: u64,
    staged: MemoryState,
    definition_override: Option<DefinitionIdentity>,
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

    fn ensure_definition(
        &mut self,
        job_name: &JobName,
        definition: &DefinitionIdentity,
    ) -> Result<(), RepositoryError> {
        if let Some(actual) = definition.job_name()
            && actual != job_name
        {
            return Err(RepositoryError::DefinitionJobMismatch {
                expected: job_name.clone(),
                actual: actual.clone(),
            });
        }
        let key = (job_name.clone(), definition.revision().clone());
        if let Some(existing) = self.staged.definitions.get(&key) {
            if existing.manifest_digest() != definition.manifest_digest() {
                return Err(RepositoryError::DefinitionDrift {
                    job_name: job_name.clone(),
                    revision: definition.revision().clone(),
                });
            }
            return Ok(());
        }
        self.staged.definitions.insert(key, definition.clone());
        Ok(())
    }

    fn instance_for_execution(
        &self,
        execution_id: JobExecutionId,
    ) -> Result<JobInstanceId, RepositoryError> {
        self.staged
            .job_executions
            .get(&execution_id)
            .map(JobExecution::job_instance_id)
            .ok_or(RepositoryError::JobExecutionNotFound { id: execution_id })
    }

    fn next_flow_decision_id(&self) -> Result<FlowDecisionId, RepositoryError> {
        let next = self
            .staged
            .flow_decisions
            .keys()
            .next_back()
            .map_or(1, |id| id.get().checked_add(1).unwrap_or(0));
        FlowDecisionId::new(next).map_err(RepositoryError::from)
    }

    fn latest_flow_step_snapshot(
        &self,
        instance_id: JobInstanceId,
        node_id: &NodeId,
    ) -> Result<Option<FlowStepState>, RepositoryError> {
        let executions = self
            .staged
            .job_executions_by_instance
            .get(&instance_id)
            .ok_or(RepositoryError::JobInstanceNotFound { id: instance_id })?;
        for execution_id in executions.iter().rev() {
            let step_ids = self
                .staged
                .step_executions_by_job
                .get(execution_id)
                .into_iter()
                .flatten();
            for step_id in step_ids.rev() {
                if self.staged.step_logical_ids.get(step_id) == Some(node_id) {
                    let execution = self
                        .staged
                        .step_executions
                        .get(step_id)
                        .cloned()
                        .ok_or(RepositoryError::FlowStateCorrupt)?;
                    return Ok(Some(FlowStepState::new(node_id.clone(), execution, None)));
                }
            }
        }
        Ok(None)
    }
}

impl RepositoryUnitOfWork for InMemoryUnitOfWork<'_> {
    fn register_definition_upgrade<'a>(
        &'a mut self,
        job_name: &'a JobName,
        upgrade: &'a DefinitionUpgrade,
    ) -> BoxFuture<'a, Result<(), RepositoryError>> {
        Box::pin(async move {
            self.ensure_definition(job_name, upgrade.from())?;
            self.ensure_definition(job_name, upgrade.to())?;
            let key = (
                job_name.clone(),
                *upgrade.from().manifest_digest(),
                *upgrade.to().manifest_digest(),
            );
            if let Some(existing) = self.staged.definition_upgrades.get(&key) {
                if existing != upgrade {
                    return Err(RepositoryError::DefinitionUpgradeConflict {
                        job_name: job_name.clone(),
                    });
                }
                return Ok(());
            }
            self.staged.definition_upgrades.insert(key, upgrade.clone());
            Ok(())
        })
    }

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
            let definition = self
                .definition_override
                .take()
                .unwrap_or_else(DefinitionIdentity::legacy);
            let job_name = self
                .staged
                .instances_by_id
                .get(&job_instance_id)
                .ok_or(RepositoryError::JobInstanceNotFound {
                    id: job_instance_id,
                })?
                .key()
                .job_name()
                .clone();
            self.ensure_definition(&job_name, &definition)?;
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
                let previous_definition =
                    self.staged.execution_definitions.get(&latest.id()).ok_or(
                        RepositoryError::IncompatibleDefinition {
                            instance_id: job_instance_id,
                        },
                    )?;
                if previous_definition.manifest_digest() != definition.manifest_digest()
                    && !self.staged.definition_upgrades.contains_key(&(
                        job_name,
                        *previous_definition.manifest_digest(),
                        *definition.manifest_digest(),
                    ))
                {
                    return Err(RepositoryError::IncompatibleDefinition {
                        instance_id: job_instance_id,
                    });
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
            self.staged.execution_definitions.insert(id, definition);
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

    fn create_job_execution_with_definition<'a>(
        &'a mut self,
        job_instance_id: JobInstanceId,
        definition: &'a DefinitionIdentity,
    ) -> BoxFuture<'a, Result<JobExecution, RepositoryError>> {
        Box::pin(async move {
            self.definition_override = Some(definition.clone());
            self.create_job_execution(job_instance_id).await
        })
    }

    fn create_step_execution<'a>(
        &'a mut self,
        job_execution_id: JobExecutionId,
        step_name: &'a StepName,
    ) -> BoxFuture<'a, Result<StepExecution, RepositoryError>> {
        Box::pin(async move {
            let node_id =
                NodeId::new(step_name.as_str()).map_err(|_| RepositoryError::FlowStateCorrupt)?;
            self.create_flow_step_execution(
                job_execution_id,
                step_name,
                &node_id,
                StartLimit::UNRESTRICTED,
            )
            .await
        })
    }

    fn create_flow_step_execution<'a>(
        &'a mut self,
        job_execution_id: JobExecutionId,
        step_name: &'a StepName,
        node_id: &'a NodeId,
        start_limit: StartLimit,
    ) -> BoxFuture<'a, Result<StepExecution, RepositoryError>> {
        Box::pin(async move {
            let instance_id = self.instance_for_execution(job_execution_id)?;
            let historical_starts = self
                .staged
                .job_executions_by_instance
                .get(&instance_id)
                .into_iter()
                .flatten()
                .flat_map(|execution_id| {
                    self.staged
                        .step_executions_by_job
                        .get(execution_id)
                        .into_iter()
                        .flatten()
                })
                .filter(|step_id| self.staged.step_logical_ids.get(step_id) == Some(node_id))
                .count();
            if u64::try_from(historical_starts).unwrap_or(u64::MAX) >= u64::from(start_limit.get())
            {
                return Err(RepositoryError::StartLimitExceeded {
                    instance_id,
                    node_id: node_id.clone(),
                    limit: start_limit,
                });
            }
            let id = self.repository.ids.next_step_execution_id()?;
            if self.staged.step_executions.contains_key(&id) {
                return Err(RepositoryError::DuplicateIdentifier {
                    kind: IdentifierKind::StepExecution,
                    value: id.get(),
                });
            }
            let counts = self
                .latest_flow_step_snapshot(instance_id, node_id)?
                .map_or_else(ExecutionCounts::default, |state| {
                    state.execution().metadata().counts()
                });
            let created_at = self.repository.clock.now();
            let metadata = ExecutionMetadata::new(
                BatchStatus::Starting,
                ExitStatus::unknown(),
                ExecutionTimestamps::new(created_at, None, None)?,
                counts,
                None,
            )?;
            let execution = StepExecution::new(id, job_execution_id, step_name.clone(), metadata);
            self.staged.step_executions.insert(id, execution.clone());
            self.staged.step_logical_ids.insert(id, node_id.clone());
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

    fn latest_flow_step<'a>(
        &'a mut self,
        job_instance_id: JobInstanceId,
        node_id: &'a NodeId,
    ) -> BoxFuture<'a, Result<Option<FlowStepState>, RepositoryError>> {
        Box::pin(async move { self.latest_flow_step_snapshot(job_instance_id, node_id) })
    }

    fn append_flow_decision<'a>(
        &'a mut self,
        request: &'a FlowDecisionRequest,
    ) -> BoxFuture<'a, Result<FlowDecision, RepositoryError>> {
        Box::pin(async move {
            let instance_id = self.instance_for_execution(request.job_execution_id())?;
            let definition = self
                .staged
                .execution_definitions
                .get(&request.job_execution_id())
                .ok_or(RepositoryError::FlowStateCorrupt)?;
            if definition.manifest_digest() != request.plan_fingerprint() {
                return Err(RepositoryError::FlowStateCorrupt);
            }
            let manifest = serde_json::from_slice(definition.canonical_manifest())
                .map_err(|_| RepositoryError::FlowStateCorrupt)?;
            if !crate::flow::decision_matches_manifest(&manifest, request) {
                return Err(RepositoryError::FlowStateCorrupt);
            }
            let existing = self
                .staged
                .flow_decisions_by_job
                .get(&request.job_execution_id())
                .cloned()
                .unwrap_or_default();
            let expected_sequence = u64::try_from(existing.len())
                .ok()
                .and_then(|value| value.checked_add(1))
                .ok_or(RepositoryError::FlowStateCorrupt)?;
            if request.sequence().get() != expected_sequence
                || existing.iter().any(|id| {
                    self.staged.flow_decisions.get(id).is_some_and(|decision| {
                        decision.source_node_id() == request.source_node_id()
                    })
                })
            {
                return Err(RepositoryError::ConcurrentModification);
            }
            if let Some(step_id) = request.source_step_execution_id() {
                let step = self
                    .staged
                    .step_executions
                    .get(&step_id)
                    .ok_or(RepositoryError::FlowStateCorrupt)?;
                if self.instance_for_execution(step.job_execution_id())? != instance_id
                    || self.staged.step_logical_ids.get(&step_id) != Some(request.source_node_id())
                {
                    return Err(RepositoryError::FlowStateCorrupt);
                }
            } else if request.kind() != FlowTransitionKind::Decider {
                return Err(RepositoryError::FlowStateCorrupt);
            }
            if let Some(reused_id) = request.reused_decision_id() {
                let reused = self
                    .staged
                    .flow_decisions
                    .get(&reused_id)
                    .ok_or(RepositoryError::FlowStateCorrupt)?;
                let reused_instance = self.instance_for_execution(reused.job_execution_id())?;
                if reused_instance != instance_id
                    || reused.source_node_id() != request.source_node_id()
                    || reused.plan_fingerprint() != request.plan_fingerprint()
                    || reused.input_digest() != request.input_digest()
                    || reused.observed_outcome() != request.observed_outcome()
                    || reused.target() != request.target()
                {
                    return Err(RepositoryError::FlowStateCorrupt);
                }
            }
            let id = self.next_flow_decision_id()?;
            let decision = FlowDecision::new(
                id,
                request.job_execution_id(),
                request.sequence(),
                request.source_node_id().clone(),
                request.source_step_execution_id(),
                request.kind(),
                request.observed_outcome().clone(),
                request.target().clone(),
                *request.plan_fingerprint(),
                *request.input_digest(),
                request.reused_decision_id(),
                request.decided_at(),
            );
            self.staged.flow_decisions.insert(id, decision.clone());
            self.staged
                .flow_decisions_by_job
                .entry(request.job_execution_id())
                .or_default()
                .push(id);
            Ok(decision)
        })
    }

    fn find_reusable_flow_decision<'a>(
        &'a mut self,
        job_instance_id: JobInstanceId,
        node_id: &'a NodeId,
        plan_fingerprint: &'a [u8; 32],
        input_digest: &'a [u8; 32],
        kind: FlowTransitionKind,
    ) -> BoxFuture<'a, Result<Option<FlowDecision>, RepositoryError>> {
        Box::pin(async move {
            let executions = self
                .staged
                .job_executions_by_instance
                .get(&job_instance_id)
                .ok_or(RepositoryError::JobInstanceNotFound {
                    id: job_instance_id,
                })?;
            for execution_id in executions.iter().rev() {
                for decision_id in self
                    .staged
                    .flow_decisions_by_job
                    .get(execution_id)
                    .into_iter()
                    .flatten()
                    .rev()
                {
                    let decision = self
                        .staged
                        .flow_decisions
                        .get(decision_id)
                        .ok_or(RepositoryError::FlowStateCorrupt)?;
                    if decision.source_node_id() == node_id
                        && decision.plan_fingerprint() == plan_fingerprint
                        && decision.input_digest() == input_digest
                        && decision.kind() == kind
                    {
                        return Ok(Some(decision.clone()));
                    }
                }
            }
            Ok(None)
        })
    }

    fn flow_decisions(
        &mut self,
        job_execution_id: JobExecutionId,
    ) -> BoxFuture<'_, Result<Vec<FlowDecision>, RepositoryError>> {
        Box::pin(async move {
            if !self.staged.job_executions.contains_key(&job_execution_id) {
                return Err(RepositoryError::JobExecutionNotFound {
                    id: job_execution_id,
                });
            }
            self.staged
                .flow_decisions_by_job
                .get(&job_execution_id)
                .into_iter()
                .flatten()
                .map(|id| {
                    self.staged
                        .flow_decisions
                        .get(id)
                        .cloned()
                        .ok_or(RepositoryError::FlowStateCorrupt)
                })
                .collect()
        })
    }

    fn recover_job_execution<'a>(
        &'a mut self,
        id: JobExecutionId,
        request: &'a RecoveryRequest,
    ) -> BoxFuture<'a, Result<RecoveryResult, RepositoryError>> {
        Box::pin(async move {
            let prior = self
                .staged
                .job_executions
                .get(&id)
                .cloned()
                .ok_or(RepositoryError::JobExecutionNotFound { id })?;
            if self
                .staged
                .recovery_decisions
                .get(&id)
                .is_some_and(|decisions| {
                    decisions
                        .iter()
                        .any(|decision| decision.execution_version() == request.expected_version())
                })
            {
                return Err(RepositoryError::ConcurrentModification);
            }
            let decided_at = self.repository.clock.now();
            let recovered = recovered_execution(&prior, request, decided_at)?;
            let decision = RecoveryDecision::new(
                id,
                request.expected_version(),
                prior.metadata().status(),
                recovered.metadata().status(),
                request.reason_code().to_owned(),
                request.operator_reference().to_owned(),
                *request.evidence_digest(),
                decided_at,
            );
            self.staged.job_executions.insert(id, recovered.clone());
            self.staged
                .recovery_decisions
                .entry(id)
                .or_default()
                .push(decision.clone());
            Ok(RecoveryResult::new(recovered, decision))
        })
    }

    fn recovery_decision(
        &mut self,
        id: JobExecutionId,
    ) -> BoxFuture<'_, Result<Option<RecoveryDecision>, RepositoryError>> {
        Box::pin(async move {
            if !self.staged.job_executions.contains_key(&id) {
                return Err(RepositoryError::JobExecutionNotFound { id });
            }
            Ok(self
                .staged
                .recovery_decisions
                .get(&id)
                .and_then(|decisions| decisions.first())
                .cloned())
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
