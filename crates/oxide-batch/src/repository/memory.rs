use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use oxide_batch_repository::{
    PartitionMutationError, aggregate_partition_parent, map_partition_aggregation,
    recovered_execution,
};

use crate::{
    ActorRef, BatchStatus, CursorKey, DefinitionDescriptor, DefinitionIdentity, DefinitionRevision,
    DefinitionUpgrade, DurableStateKind, ExecutionCounts, ExecutionMetadata, ExecutionTimestamps,
    ExecutionVersion, ExitStatus, ExplorerError, ExplorerQuery, ExplorerRepository, FlowDecision,
    FlowDecisionId, FlowDecisionRequest, FlowStepState, FlowTransitionKind, IdentifierKind,
    JobExecution, JobExecutionId, JobExecutionProjection, JobInstance, JobInstanceId,
    JobInstanceKey, JobInstanceProjection, JobName, LifecycleError, LifecycleTransition,
    MAX_PARTITIONS, NodeId, OperationId, OperatorAction, OperatorRecord, OperatorRecordDraft,
    OperatorRequestId, OwnerObservation, OwnerToken, ParameterDescriptor, PartitionPlanEntry,
    PartitionResult, PurgeCandidate, PurgeCounts, PurgePlan, PurgePlanRequest, PurgeSurvey,
    QueryWindow, ReasonCode, RecoveryDecisionId, RecoveryRepository, RecoverySnapshot,
    RecoveryStepEvidence, RetentionAction, RetentionActionId, RetentionHold, RetentionRecord,
    RetentionRecordDraft, StartLimit, StateEnvelopeDescriptor, StepExecution, StepExecutionId,
    StepExecutionProjection, StepName, StepPartition, StepPartitionId, StepPartitionProjection,
};
use crate::{
    BoxFuture, Clock, IdGenerator, JobInstanceSelection, JobRepository, RecoveryDecision,
    RecoveryRequest, RecoveryResult, RepositoryCapability, RepositoryDescriptor, RepositoryError,
    RepositoryUnitOfWork,
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
    fail_next_partition_aggregate_commit: Arc<AtomicBool>,
}

impl InMemoryJobRepository {
    /// Constructs an empty repository with explicitly injected time and IDs.
    #[must_use]
    pub fn new(clock: Arc<dyn Clock>, ids: Arc<dyn IdGenerator>) -> Self {
        Self {
            state: Arc::new(Mutex::new(MemoryState::default())),
            clock,
            ids,
            fail_next_partition_aggregate_commit: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Injects one lost commit response after the next partition aggregate is published.
    ///
    /// This deterministic failure fixture is intended for conformance tests of
    /// fresh-state inspection after an ambiguous repository commit.
    pub fn inject_next_partition_aggregate_commit_unknown(&self) {
        self.fail_next_partition_aggregate_commit
            .store(true, Ordering::Release);
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
    fn connection_capacity(&self) -> u32 {
        u32::from(crate::MAX_PARTITION_WORKERS) + 1
    }

    /// The reference adapter implements every capability this milestone
    /// defines. It reports schema version `0` because it holds no durable
    /// metadata schema.
    fn descriptor(&self) -> RepositoryDescriptor {
        RepositoryDescriptor::new(
            0,
            [
                RepositoryCapability::ExecutionOwnership,
                RepositoryCapability::InstanceHolds,
                RepositoryCapability::OperatorRequests,
                RepositoryCapability::RetentionPurge,
                RepositoryCapability::StepPartitions,
                RepositoryCapability::StopRequests,
            ],
        )
    }

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
                created_partition_plans: BTreeSet::new(),
                aggregated_partition_parent: false,
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
    step_partitions: BTreeMap<StepPartitionId, StepPartition>,
    step_partitions_by_step: BTreeMap<StepExecutionId, Vec<StepPartitionId>>,
    flow_decisions: BTreeMap<FlowDecisionId, FlowDecision>,
    flow_decisions_by_job: BTreeMap<JobExecutionId, Vec<FlowDecisionId>>,
    recovery_decisions: BTreeMap<JobExecutionId, Vec<RecoveryDecision>>,
    definitions: BTreeMap<(JobName, DefinitionRevision), DefinitionIdentity>,
    execution_definitions: BTreeMap<JobExecutionId, DefinitionIdentity>,
    definition_upgrades: BTreeMap<(JobName, [u8; 32], [u8; 32]), DefinitionUpgrade>,
    job_name_order: BTreeMap<JobName, u64>,
    instance_created_at: BTreeMap<JobInstanceId, SystemTime>,
    execution_updated_at: BTreeMap<JobExecutionId, SystemTime>,
    holds: BTreeMap<JobInstanceId, RetentionHold>,
    stop_requests: BTreeMap<JobExecutionId, SystemTime>,
    owner_tokens: BTreeMap<JobExecutionId, OwnerToken>,
    operator_requests: BTreeMap<OperatorRequestId, OperatorRecord>,
    operator_request_keys: BTreeMap<(&'static str, String), OperatorRequestId>,
    retention_actions: BTreeMap<RetentionActionId, RetentionRecord>,
    retention_action_keys: BTreeMap<(&'static str, String), RetentionActionId>,
}

impl MemoryState {
    fn register_job_name(&mut self, job_name: &JobName) {
        if self.job_name_order.contains_key(job_name) {
            return;
        }
        let next = self
            .job_name_order
            .values()
            .copied()
            .max()
            .map_or(1, |value| value.saturating_add(1));
        self.job_name_order.insert(job_name.clone(), next);
    }

    fn attempt_of(&self, execution: &JobExecution) -> u32 {
        self.job_executions_by_instance
            .get(&execution.job_instance_id())
            .and_then(|executions| {
                executions
                    .iter()
                    .position(|candidate| *candidate == execution.id())
            })
            .and_then(|position| u32::try_from(position.saturating_add(1)).ok())
            .unwrap_or(1)
    }

    fn job_name_of(&self, instance_id: JobInstanceId) -> Option<JobName> {
        self.instances_by_id
            .get(&instance_id)
            .map(|instance| instance.key().job_name().clone())
    }

    fn job_execution_projection(
        &self,
        execution: &JobExecution,
    ) -> Result<JobExecutionProjection, ExplorerError> {
        let job_name =
            self.job_name_of(execution.job_instance_id())
                .ok_or(ExplorerError::Repository(
                    RepositoryError::JobInstanceNotFound {
                        id: execution.job_instance_id(),
                    },
                ))?;
        let definition = self
            .execution_definitions
            .get(&execution.id())
            .map(|definition| {
                DefinitionDescriptor::new(
                    definition.revision().clone(),
                    definition.manifest_format(),
                    *definition.manifest_digest(),
                )
            });
        Ok(JobExecutionProjection::new(
            execution.id(),
            execution.job_instance_id(),
            job_name,
            self.attempt_of(execution),
            execution.metadata().status(),
            execution.metadata().exit_status().clone(),
            execution.metadata().counts(),
            execution.version(),
            execution.metadata().timestamps(),
            self.execution_updated_at
                .get(&execution.id())
                .copied()
                .unwrap_or_else(|| updated_at(execution)),
            execution.metadata().failure(),
            definition,
            None,
            self.stop_requests.get(&execution.id()).copied(),
            false,
        ))
    }

    fn job_instance_projection(&self, instance: &JobInstance) -> JobInstanceProjection {
        let key = instance.key();
        let parameters = key
            .identifying_fields()
            .map(|(name, kind)| ParameterDescriptor::new(name.clone(), kind, true))
            .collect();
        JobInstanceProjection::new(
            instance.id(),
            key.job_name().clone(),
            key.digest(),
            parameters,
            self.instance_created_at.get(&instance.id()).copied(),
            self.holds.get(&instance.id()).cloned(),
        )
    }

    fn step_execution_projection(&self, execution: &StepExecution) -> StepExecutionProjection {
        StepExecutionProjection::new(
            execution.id(),
            execution.job_execution_id(),
            execution.step_name().clone(),
            self.step_logical_ids.get(&execution.id()).cloned(),
            execution.metadata().status(),
            execution.metadata().exit_status().clone(),
            execution.metadata().counts(),
            execution.version(),
            execution.metadata().timestamps(),
            execution.metadata().failure(),
            None,
            None,
        )
    }
}

fn updated_at(execution: &JobExecution) -> SystemTime {
    let timestamps = execution.metadata().timestamps();
    timestamps
        .ended_at()
        .or_else(|| timestamps.started_at())
        .unwrap_or_else(|| timestamps.created_at())
}

struct InMemoryUnitOfWork<'repository> {
    repository: &'repository InMemoryJobRepository,
    base_revision: u64,
    staged: MemoryState,
    definition_override: Option<DefinitionIdentity>,
    created_partition_plans: BTreeSet<StepExecutionId>,
    aggregated_partition_parent: bool,
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
        self.staged.register_job_name(job_name);
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

    fn next_recovery_decision_id(&self) -> Result<RecoveryDecisionId, RepositoryError> {
        let next = self
            .staged
            .recovery_decisions
            .values()
            .flatten()
            .map(|decision| decision.id().get())
            .max()
            .map_or(1, |id| id.checked_add(1).unwrap_or(0));
        RecoveryDecisionId::new(next).map_err(RepositoryError::from)
    }

    fn next_operator_request_id(&self) -> Result<OperatorRequestId, RepositoryError> {
        let next = self
            .staged
            .operator_requests
            .keys()
            .next_back()
            .map_or(1, |id| id.get().checked_add(1).unwrap_or(0));
        OperatorRequestId::new(next).map_err(RepositoryError::from)
    }

    fn next_retention_action_id(&self) -> Result<RetentionActionId, RepositoryError> {
        let next = self
            .staged
            .retention_actions
            .keys()
            .next_back()
            .map_or(1, |id| id.get().checked_add(1).unwrap_or(0));
        RetentionActionId::new(next).map_err(RepositoryError::from)
    }

    fn next_step_partition_id(&self) -> Result<StepPartitionId, RepositoryError> {
        let next = self
            .staged
            .step_partitions
            .keys()
            .next_back()
            .map_or(1, |id| id.get().checked_add(1).unwrap_or(0));
        StepPartitionId::new(next).map_err(RepositoryError::from)
    }

    fn remove_step_execution(&mut self, step_execution_id: StepExecutionId) {
        for partition_id in self
            .staged
            .step_partitions_by_step
            .remove(&step_execution_id)
            .unwrap_or_default()
        {
            self.staged.step_partitions.remove(&partition_id);
        }
        self.staged.step_executions.remove(&step_execution_id);
        self.staged.step_logical_ids.remove(&step_execution_id);
    }

    fn purge_eligible(&self, request: &PurgePlanRequest, now: SystemTime) -> Vec<PurgeCandidate> {
        let mut candidates = Vec::new();
        for (instance_id, instance) in &self.staged.instances_by_id {
            if instance.key().job_name() != request.job_name()
                || self.staged.holds.contains_key(instance_id)
            {
                continue;
            }
            let executions = self
                .staged
                .job_executions_by_instance
                .get(instance_id)
                .into_iter()
                .flatten()
                .filter_map(|id| self.staged.job_executions.get(id))
                .collect::<Vec<_>>();
            if executions
                .iter()
                .any(|execution| !execution.metadata().status().is_finished())
            {
                continue;
            }
            for execution in executions {
                let status = execution.metadata().status();
                if !request.statuses().contains(status) {
                    continue;
                }
                let age = now
                    .duration_since(updated_at(execution))
                    .unwrap_or(Duration::ZERO);
                if age < request.minimum_age() {
                    continue;
                }
                candidates.push(PurgeCandidate::new(
                    *instance_id,
                    execution.id(),
                    execution.version(),
                ));
            }
        }
        candidates.sort_unstable();
        candidates.truncate(usize::try_from(request.batch().get()).unwrap_or(usize::MAX));
        candidates
    }

    fn purge_counts(&self, candidates: &[PurgeCandidate]) -> PurgeCounts {
        let mut flow_decisions = 0_u64;
        let mut recovery_decisions = 0_u64;
        let mut operator_requests = 0_u64;
        let mut step_partitions = 0_u64;
        let mut step_executions = 0_u64;
        let mut instances = BTreeMap::new();
        for candidate in candidates {
            let execution_id = candidate.job_execution_id();
            flow_decisions = flow_decisions.saturating_add(count_of(
                self.staged.flow_decisions_by_job.get(&execution_id),
            ));
            recovery_decisions = recovery_decisions
                .saturating_add(count_of(self.staged.recovery_decisions.get(&execution_id)));
            step_executions = step_executions.saturating_add(count_of(
                self.staged.step_executions_by_job.get(&execution_id),
            ));
            step_partitions = step_partitions.saturating_add(
                self.staged
                    .step_executions_by_job
                    .get(&execution_id)
                    .into_iter()
                    .flatten()
                    .map(|id| count_of(self.staged.step_partitions_by_step.get(id)))
                    .fold(0_u64, u64::saturating_add),
            );
            operator_requests = operator_requests.saturating_add(
                u64::try_from(
                    self.staged
                        .operator_requests
                        .values()
                        .filter(|record| record.job_execution_id() == Some(execution_id))
                        .count(),
                )
                .unwrap_or(u64::MAX),
            );
            *instances
                .entry(candidate.job_instance_id())
                .or_insert(0_u64) += 1;
        }
        let job_instances = instances
            .iter()
            .filter(|(instance_id, purged)| {
                count_of(self.staged.job_executions_by_instance.get(instance_id)) == **purged
            })
            .count();
        PurgeCounts::new(
            flow_decisions,
            recovery_decisions,
            operator_requests,
            step_partitions,
            step_executions,
            u64::try_from(candidates.len()).unwrap_or(u64::MAX),
            u64::try_from(job_instances).unwrap_or(u64::MAX),
        )
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
            let created_at = self.repository.clock.now();
            self.staged.instances_by_key.insert(key.clone(), id);
            self.staged.instances_by_id.insert(id, instance.clone());
            self.staged.instance_created_at.insert(id, created_at);
            self.staged.register_job_name(key.job_name());
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
            self.staged
                .execution_updated_at
                .insert(id, execution.metadata().timestamps().created_at());
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
            self.staged
                .execution_updated_at
                .insert(id, transition.transitioned_at());
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
            } else if !matches!(
                request.kind(),
                FlowTransitionKind::Decider | FlowTransitionKind::SplitAggregate
            ) {
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

    fn create_step_partition_plan<'a>(
        &'a mut self,
        step_execution_id: StepExecutionId,
        entries: &'a [PartitionPlanEntry],
    ) -> BoxFuture<'a, Result<Vec<StepPartition>, RepositoryError>> {
        Box::pin(async move {
            let parent = self.staged.step_executions.get(&step_execution_id).ok_or(
                RepositoryError::StepExecutionNotFound {
                    id: step_execution_id,
                },
            )?;
            if !matches!(
                parent.metadata().status(),
                BatchStatus::Starting | BatchStatus::Started
            ) {
                return Err(RepositoryError::PartitionParentNotActive {
                    step_execution_id,
                    status: parent.metadata().status(),
                });
            }
            if entries.is_empty() {
                return Err(RepositoryError::EmptyPartitionPlan);
            }
            if entries.len() > usize::from(MAX_PARTITIONS) {
                return Err(RepositoryError::PartitionPlanTooLarge {
                    max: usize::from(MAX_PARTITIONS),
                });
            }
            if self
                .staged
                .step_partitions_by_step
                .contains_key(&step_execution_id)
            {
                return Err(RepositoryError::PartitionPlanExists { step_execution_id });
            }
            let mut keys = BTreeSet::new();
            for entry in entries {
                if !keys.insert(entry.key().clone()) {
                    return Err(RepositoryError::DuplicatePartitionKey);
                }
            }

            let mut partitions = Vec::with_capacity(entries.len());
            for (index, entry) in entries.iter().cloned().enumerate() {
                let id = self.next_step_partition_id()?;
                let ordinal = u32::try_from(index.saturating_add(1))
                    .map_err(|_| RepositoryError::PartitionStateCorrupt)?;
                let partition = StepPartition::starting(id, step_execution_id, ordinal, entry);
                self.staged.step_partitions.insert(id, partition.clone());
                partitions.push(partition);
            }
            self.staged.step_partitions_by_step.insert(
                step_execution_id,
                partitions.iter().map(StepPartition::id).collect(),
            );
            self.created_partition_plans.insert(step_execution_id);
            Ok(partitions)
        })
    }

    fn step_partition_plan(
        &mut self,
        step_execution_id: StepExecutionId,
    ) -> BoxFuture<'_, Result<Vec<StepPartition>, RepositoryError>> {
        Box::pin(async move {
            if !self.staged.step_executions.contains_key(&step_execution_id) {
                return Err(RepositoryError::StepExecutionNotFound {
                    id: step_execution_id,
                });
            }
            let mut partitions = self
                .staged
                .step_partitions_by_step
                .get(&step_execution_id)
                .into_iter()
                .flatten()
                .map(|id| {
                    self.staged
                        .step_partitions
                        .get(id)
                        .cloned()
                        .ok_or(RepositoryError::PartitionStateCorrupt)
                })
                .collect::<Result<Vec<_>, _>>()?;
            partitions.sort_by(|left, right| left.key().cmp(right.key()));
            Ok(partitions)
        })
    }

    fn restart_step_partition_plan(
        &mut self,
        source_step_execution_id: StepExecutionId,
        target_step_execution_id: StepExecutionId,
    ) -> BoxFuture<'_, Result<Vec<StepPartition>, RepositoryError>> {
        Box::pin(async move {
            if self
                .staged
                .step_partitions_by_step
                .contains_key(&target_step_execution_id)
            {
                return Err(RepositoryError::PartitionPlanExists {
                    step_execution_id: target_step_execution_id,
                });
            }
            let source_parent = self
                .staged
                .step_executions
                .get(&source_step_execution_id)
                .ok_or(RepositoryError::StepExecutionNotFound {
                    id: source_step_execution_id,
                })?;
            let source_job = self
                .staged
                .job_executions
                .get(&source_parent.job_execution_id())
                .ok_or(RepositoryError::PartitionStateCorrupt)?;
            if !matches!(
                source_job.metadata().status(),
                BatchStatus::Failed | BatchStatus::Stopped
            ) {
                return Err(RepositoryError::PartitionStateCorrupt);
            }
            let target_parent = self
                .staged
                .step_executions
                .get(&target_step_execution_id)
                .ok_or(RepositoryError::StepExecutionNotFound {
                    id: target_step_execution_id,
                })?;
            if !matches!(
                target_parent.metadata().status(),
                BatchStatus::Starting | BatchStatus::Started
            ) {
                return Err(RepositoryError::PartitionParentNotActive {
                    step_execution_id: target_step_execution_id,
                    status: target_parent.metadata().status(),
                });
            }
            let source_ids = self
                .staged
                .step_partitions_by_step
                .get(&source_step_execution_id)
                .cloned()
                .ok_or(RepositoryError::PartitionStateCorrupt)?;
            let mut copied = Vec::with_capacity(source_ids.len());
            for source_id in source_ids {
                let source = self
                    .staged
                    .step_partitions
                    .get(&source_id)
                    .cloned()
                    .ok_or(RepositoryError::PartitionStateCorrupt)?;
                let id = self.next_step_partition_id()?;
                let partition = if source.status() == BatchStatus::Completed {
                    if source.worker_step_execution_id().is_none() {
                        return Err(RepositoryError::PartitionStateCorrupt);
                    }
                    StepPartition::from_snapshot(
                        id,
                        target_step_execution_id,
                        source.worker_step_execution_id(),
                        source.key().clone(),
                        source.ordinal(),
                        source.status(),
                        source.exit_status().clone(),
                        source.counts(),
                        source.context().clone(),
                        ExecutionVersion::INITIAL,
                    )
                } else {
                    StepPartition::starting(
                        id,
                        target_step_execution_id,
                        source.ordinal(),
                        PartitionPlanEntry::new(source.key().clone(), source.context().clone())
                            .map_err(|_| RepositoryError::PartitionStateCorrupt)?,
                    )
                };
                self.staged.step_partitions.insert(id, partition.clone());
                copied.push(partition);
            }
            self.staged.step_partitions_by_step.insert(
                target_step_execution_id,
                copied.iter().map(StepPartition::id).collect(),
            );
            self.created_partition_plans
                .insert(target_step_execution_id);
            Ok(copied)
        })
    }

    fn assign_step_partition(
        &mut self,
        id: StepPartitionId,
        expected_version: ExecutionVersion,
        worker_step_execution_id: StepExecutionId,
    ) -> BoxFuture<'_, Result<StepPartition, RepositoryError>> {
        Box::pin(async move {
            let mut partition = self
                .staged
                .step_partitions
                .get(&id)
                .cloned()
                .ok_or(RepositoryError::StepPartitionNotFound { id })?;
            if self
                .created_partition_plans
                .contains(&partition.step_execution_id())
            {
                return Err(RepositoryError::PartitionPlanNotCommitted {
                    step_execution_id: partition.step_execution_id(),
                });
            }
            let parent = self
                .staged
                .step_executions
                .get(&partition.step_execution_id())
                .ok_or(RepositoryError::PartitionStateCorrupt)?;
            if parent.metadata().status() != BatchStatus::Started {
                return Err(RepositoryError::PartitionParentNotActive {
                    step_execution_id: parent.id(),
                    status: parent.metadata().status(),
                });
            }
            partition
                .assign(expected_version, worker_step_execution_id)
                .map_err(|error| map_partition_mutation(id, error))?;
            let worker = self
                .staged
                .step_executions
                .get(&worker_step_execution_id)
                .ok_or(RepositoryError::StepExecutionNotFound {
                    id: worker_step_execution_id,
                })?;
            if partition.step_execution_id() == worker_step_execution_id
                || parent.job_execution_id() != worker.job_execution_id()
            {
                return Err(RepositoryError::PartitionWorkerMismatch {
                    partition_id: id,
                    worker_step_execution_id,
                });
            }
            if self.staged.step_partitions.values().any(|candidate| {
                candidate.worker_step_execution_id() == Some(worker_step_execution_id)
            }) {
                return Err(RepositoryError::PartitionWorkerAlreadyAssigned {
                    worker_step_execution_id,
                });
            }
            self.staged.step_partitions.insert(id, partition.clone());
            Ok(partition)
        })
    }

    fn complete_step_partition(
        &mut self,
        id: StepPartitionId,
        expected_version: ExecutionVersion,
        worker_step_execution_id: StepExecutionId,
    ) -> BoxFuture<'_, Result<StepPartition, RepositoryError>> {
        Box::pin(async move {
            let mut partition = self
                .staged
                .step_partitions
                .get(&id)
                .cloned()
                .ok_or(RepositoryError::StepPartitionNotFound { id })?;
            let parent = self
                .staged
                .step_executions
                .get(&partition.step_execution_id())
                .ok_or(RepositoryError::PartitionStateCorrupt)?;
            if !matches!(
                parent.metadata().status(),
                BatchStatus::Started | BatchStatus::Stopping
            ) {
                return Err(RepositoryError::PartitionParentNotActive {
                    step_execution_id: parent.id(),
                    status: parent.metadata().status(),
                });
            }
            if partition.worker_step_execution_id() != Some(worker_step_execution_id) {
                return Err(RepositoryError::PartitionWorkerStale {
                    partition_id: id,
                    worker_step_execution_id,
                });
            }
            let worker = self
                .staged
                .step_executions
                .get(&worker_step_execution_id)
                .ok_or(RepositoryError::StepExecutionNotFound {
                    id: worker_step_execution_id,
                })?;
            if worker.job_execution_id() != parent.job_execution_id() {
                return Err(RepositoryError::PartitionWorkerMismatch {
                    partition_id: id,
                    worker_step_execution_id,
                });
            }
            let result = PartitionResult::from_worker(worker).map_err(|_| {
                RepositoryError::PartitionAggregationIncomplete {
                    step_execution_id: parent.id(),
                    status: worker.metadata().status(),
                }
            })?;
            partition
                .complete(expected_version, &result)
                .map_err(|error| map_partition_mutation(id, error))?;
            self.staged.step_partitions.insert(id, partition.clone());
            Ok(partition.clone())
        })
    }

    fn aggregate_step_partitions(
        &mut self,
        step_execution_id: StepExecutionId,
        expected_version: ExecutionVersion,
        transitioned_at: SystemTime,
    ) -> BoxFuture<'_, Result<StepExecution, RepositoryError>> {
        Box::pin(async move {
            let partitions = self.step_partition_plan(step_execution_id).await?;
            let aggregate = crate::aggregate_step_partitions(&partitions)
                .map_err(|error| map_partition_aggregation(step_execution_id, error))?;
            for partition in &partitions {
                let worker_id = partition.worker_step_execution_id().ok_or(
                    RepositoryError::PartitionAggregationIncomplete {
                        step_execution_id,
                        status: partition.status(),
                    },
                )?;
                let worker = self
                    .staged
                    .step_executions
                    .get(&worker_id)
                    .ok_or(RepositoryError::PartitionStateCorrupt)?;
                if worker.metadata().status() != partition.status()
                    || worker.metadata().exit_status() != partition.exit_status()
                    || worker.metadata().counts() != partition.counts()
                {
                    return Err(RepositoryError::PartitionStateCorrupt);
                }
            }
            let parent = self
                .staged
                .step_executions
                .get(&step_execution_id)
                .cloned()
                .ok_or(RepositoryError::StepExecutionNotFound {
                    id: step_execution_id,
                })?;
            let selected_worker = self
                .staged
                .step_executions
                .get(&aggregate.selected_worker_step_execution_id())
                .ok_or(RepositoryError::PartitionStateCorrupt)?;
            let failure = selected_worker.metadata().failure();
            if let Some(next) = expected_version.get().checked_add(1)
                && parent.version().get() == next
                && parent.metadata().status() == aggregate.status()
                && parent.metadata().exit_status() == aggregate.exit_status()
                && parent.metadata().counts() == aggregate.counts()
                && parent.metadata().failure() == failure
            {
                return Ok(parent);
            }
            if !matches!(
                parent.metadata().status(),
                BatchStatus::Started | BatchStatus::Stopping
            ) {
                return Err(RepositoryError::PartitionParentNotActive {
                    step_execution_id,
                    status: parent.metadata().status(),
                });
            }
            let aggregated = aggregate_partition_parent(
                &parent,
                expected_version,
                &aggregate,
                transitioned_at,
                failure,
            )?;
            self.staged
                .step_executions
                .insert(step_execution_id, aggregated.clone());
            self.aggregated_partition_parent = true;
            Ok(aggregated)
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
            let decision_id = self.next_recovery_decision_id()?;
            let decision = RecoveryDecision::new(
                decision_id,
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
            self.staged.execution_updated_at.insert(id, decided_at);
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

    fn find_operator_request<'a>(
        &'a mut self,
        action: OperatorAction,
        operation_id: &'a OperationId,
    ) -> BoxFuture<'a, Result<Option<OperatorRecord>, RepositoryError>> {
        Box::pin(async move {
            Ok(self
                .staged
                .operator_request_keys
                .get(&(action.as_str(), operation_id.as_str().to_owned()))
                .and_then(|id| self.staged.operator_requests.get(id))
                .cloned())
        })
    }

    fn append_operator_request<'a>(
        &'a mut self,
        draft: &'a OperatorRecordDraft,
    ) -> BoxFuture<'a, Result<OperatorRecord, RepositoryError>> {
        Box::pin(async move {
            let key = (
                draft.action().as_str(),
                draft.operation_id().as_str().to_owned(),
            );
            if self.staged.operator_request_keys.contains_key(&key) {
                return Err(RepositoryError::ConcurrentModification);
            }
            let id = self.next_operator_request_id()?;
            let record = OperatorRecord::from_parts(id, draft.clone());
            self.staged.operator_requests.insert(id, record.clone());
            self.staged.operator_request_keys.insert(key, id);
            Ok(record)
        })
    }

    fn request_execution_stop<'a>(
        &'a mut self,
        id: JobExecutionId,
        expected_version: ExecutionVersion,
        _actor: &'a ActorRef,
        requested_at: SystemTime,
    ) -> BoxFuture<'a, Result<JobExecution, RepositoryError>> {
        Box::pin(async move {
            let execution = self
                .staged
                .job_executions
                .get(&id)
                .cloned()
                .ok_or(RepositoryError::JobExecutionNotFound { id })?;
            if execution.version() != expected_version {
                return Err(RepositoryError::Lifecycle(LifecycleError::StaleVersion {
                    expected: expected_version,
                    actual: execution.version(),
                }));
            }
            self.staged.stop_requests.insert(id, requested_at);
            self.staged.execution_updated_at.insert(id, requested_at);
            Ok(execution)
        })
    }

    fn claim_execution_owner<'a>(
        &'a mut self,
        id: JobExecutionId,
        expected_version: ExecutionVersion,
        owner: &'a OwnerToken,
        claimed_at: SystemTime,
    ) -> BoxFuture<'a, Result<JobExecution, RepositoryError>> {
        Box::pin(async move {
            let execution = self
                .staged
                .job_executions
                .get(&id)
                .cloned()
                .ok_or(RepositoryError::JobExecutionNotFound { id })?;
            if execution.version() != expected_version {
                return Err(RepositoryError::Lifecycle(LifecycleError::StaleVersion {
                    expected: expected_version,
                    actual: execution.version(),
                }));
            }
            if execution.metadata().status() != BatchStatus::Starting {
                return Err(RepositoryError::ExecutionOwnershipNotAllowed {
                    id,
                    status: execution.metadata().status(),
                });
            }
            if self
                .staged
                .owner_tokens
                .get(&id)
                .is_some_and(|recorded| recorded != owner)
            {
                return Err(RepositoryError::ExecutionOwned { id });
            }
            self.staged.owner_tokens.insert(id, *owner);
            self.staged.execution_updated_at.insert(id, claimed_at);
            Ok(execution)
        })
    }

    fn observe_execution_control<'a>(
        &'a mut self,
        id: JobExecutionId,
        owner: &'a OwnerToken,
        observed_at: SystemTime,
    ) -> BoxFuture<'a, Result<crate::ExecutionControl, RepositoryError>> {
        Box::pin(async move {
            let owner_matches = self.staged.owner_tokens.get(&id) == Some(owner);
            let stop_requested = self.staged.stop_requests.contains_key(&id);
            let execution = self
                .staged
                .job_executions
                .get_mut(&id)
                .ok_or(RepositoryError::JobExecutionNotFound { id })?;
            if owner_matches
                && stop_requested
                && matches!(
                    execution.metadata().status(),
                    BatchStatus::Starting | BatchStatus::Started
                )
            {
                execution.transition(
                    execution.version(),
                    LifecycleTransition::new(BatchStatus::Stopping, observed_at),
                )?;
                self.staged.execution_updated_at.insert(id, observed_at);
            }
            Ok(crate::ExecutionControl::new(
                execution.clone(),
                owner_matches,
                stop_requested,
            ))
        })
    }

    fn job_instance_hold(
        &mut self,
        id: JobInstanceId,
    ) -> BoxFuture<'_, Result<Option<RetentionHold>, RepositoryError>> {
        Box::pin(async move {
            if !self.staged.instances_by_id.contains_key(&id) {
                return Err(RepositoryError::JobInstanceNotFound { id });
            }
            Ok(self.staged.holds.get(&id).cloned())
        })
    }

    fn place_instance_hold<'a>(
        &'a mut self,
        id: JobInstanceId,
        actor: &'a ActorRef,
        reason: &'a ReasonCode,
        placed_at: SystemTime,
    ) -> BoxFuture<'a, Result<RetentionHold, RepositoryError>> {
        Box::pin(async move {
            if !self.staged.instances_by_id.contains_key(&id) {
                return Err(RepositoryError::JobInstanceNotFound { id });
            }
            let hold = RetentionHold::new(id, actor.clone(), reason.clone(), placed_at);
            self.staged.holds.insert(id, hold.clone());
            Ok(hold)
        })
    }

    fn release_instance_hold(
        &mut self,
        id: JobInstanceId,
    ) -> BoxFuture<'_, Result<Option<RetentionHold>, RepositoryError>> {
        Box::pin(async move {
            if !self.staged.instances_by_id.contains_key(&id) {
                return Err(RepositoryError::JobInstanceNotFound { id });
            }
            Ok(self.staged.holds.remove(&id))
        })
    }

    fn find_retention_action<'a>(
        &'a mut self,
        action: RetentionAction,
        operation_id: &'a OperationId,
    ) -> BoxFuture<'a, Result<Option<RetentionRecord>, RepositoryError>> {
        Box::pin(async move {
            Ok(self
                .staged
                .retention_action_keys
                .get(&(action.as_str(), operation_id.as_str().to_owned()))
                .and_then(|id| self.staged.retention_actions.get(id))
                .cloned())
        })
    }

    fn append_retention_action<'a>(
        &'a mut self,
        draft: &'a RetentionRecordDraft,
    ) -> BoxFuture<'a, Result<RetentionRecord, RepositoryError>> {
        Box::pin(async move {
            let key = (
                draft.action().as_str(),
                draft.operation_id().as_str().to_owned(),
            );
            if self.staged.retention_action_keys.contains_key(&key) {
                return Err(RepositoryError::ConcurrentModification);
            }
            let id = self.next_retention_action_id()?;
            let record = RetentionRecord::from_parts(id, draft.clone());
            self.staged.retention_actions.insert(id, record.clone());
            self.staged.retention_action_keys.insert(key, id);
            Ok(record)
        })
    }

    fn purge_survey<'a>(
        &'a mut self,
        request: &'a PurgePlanRequest,
    ) -> BoxFuture<'a, Result<PurgeSurvey, RepositoryError>> {
        Box::pin(async move {
            let now = self.repository.clock.now();
            let candidates = self.purge_eligible(request, now);
            let counts = self.purge_counts(&candidates);
            Ok(PurgeSurvey::new(candidates, counts))
        })
    }

    fn apply_purge<'a>(
        &'a mut self,
        plan: &'a PurgePlan,
    ) -> BoxFuture<'a, Result<PurgeCounts, RepositoryError>> {
        Box::pin(async move {
            for candidate in plan.candidates() {
                let execution = self
                    .staged
                    .job_executions
                    .get(&candidate.job_execution_id())
                    .ok_or(RepositoryError::RetentionPlanStale)?;
                let status = execution.metadata().status();
                if execution.version() != candidate.version()
                    || !plan.request().statuses().contains(status)
                    || self.staged.holds.contains_key(&candidate.job_instance_id())
                {
                    return Err(RepositoryError::RetentionPlanStale);
                }
                let siblings_resolved = self
                    .staged
                    .job_executions_by_instance
                    .get(&candidate.job_instance_id())
                    .into_iter()
                    .flatten()
                    .filter_map(|id| self.staged.job_executions.get(id))
                    .all(|sibling| sibling.metadata().status().is_finished());
                if !siblings_resolved {
                    return Err(RepositoryError::RetentionPlanStale);
                }
            }
            let counts = self.purge_counts(plan.candidates());
            for candidate in plan.candidates() {
                let execution_id = candidate.job_execution_id();
                for decision_id in self
                    .staged
                    .flow_decisions_by_job
                    .remove(&execution_id)
                    .unwrap_or_default()
                {
                    self.staged.flow_decisions.remove(&decision_id);
                }
                self.staged.recovery_decisions.remove(&execution_id);
                let request_ids = self
                    .staged
                    .operator_requests
                    .iter()
                    .filter(|(_, record)| record.job_execution_id() == Some(execution_id))
                    .map(|(id, _)| *id)
                    .collect::<Vec<_>>();
                for request_id in request_ids {
                    if let Some(record) = self.staged.operator_requests.remove(&request_id) {
                        self.staged.operator_request_keys.remove(&(
                            record.action().as_str(),
                            record.operation_id().as_str().to_owned(),
                        ));
                    }
                }
                for step_id in self
                    .staged
                    .step_executions_by_job
                    .remove(&execution_id)
                    .unwrap_or_default()
                {
                    self.remove_step_execution(step_id);
                }
                self.staged.job_executions.remove(&execution_id);
                self.staged.execution_updated_at.remove(&execution_id);
                self.staged.owner_tokens.remove(&execution_id);
                self.staged.execution_definitions.remove(&execution_id);
                self.staged.stop_requests.remove(&execution_id);
                if let Some(executions) = self
                    .staged
                    .job_executions_by_instance
                    .get_mut(&candidate.job_instance_id())
                {
                    executions.retain(|id| *id != execution_id);
                }
            }
            let mut touched = plan
                .candidates()
                .iter()
                .map(PurgeCandidate::job_instance_id)
                .collect::<Vec<_>>();
            touched.dedup();
            for instance_id in touched {
                if self
                    .staged
                    .job_executions_by_instance
                    .get(&instance_id)
                    .is_none_or(|executions| !executions.is_empty())
                {
                    continue;
                }
                let Some(instance) = self.staged.instances_by_id.remove(&instance_id) else {
                    continue;
                };
                self.staged.instances_by_key.remove(instance.key());
                self.staged.job_executions_by_instance.remove(&instance_id);
                self.staged.instance_created_at.remove(&instance_id);
                self.staged.holds.remove(&instance_id);
            }
            Ok(counts)
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
            if self.aggregated_partition_parent
                && self
                    .repository
                    .fail_next_partition_aggregate_commit
                    .swap(false, Ordering::AcqRel)
            {
                return Err(RepositoryError::CommitOutcomeUnknown);
            }
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

fn count_of<T>(values: Option<&Vec<T>>) -> u64 {
    values.map_or(0, |values| u64::try_from(values.len()).unwrap_or(u64::MAX))
}

fn map_partition_mutation(id: StepPartitionId, error: PartitionMutationError) -> RepositoryError {
    match error {
        PartitionMutationError::StaleVersion { expected, actual } => {
            RepositoryError::Lifecycle(LifecycleError::StaleVersion { expected, actual })
        }
        PartitionMutationError::InvalidState { status } => {
            RepositoryError::PartitionUpdateNotAllowed { id, status }
        }
        PartitionMutationError::VersionExhausted => RepositoryError::PartitionStateCorrupt,
    }
}

/// The bounded keyset read port of [`InMemoryJobRepository`].
///
/// The reference explorer reads a consistent snapshot of process-local state.
/// It records no job/step execution context or checkpoint, so those projection
/// fields are absent rather than guessed. Partition plans retain their bounded
/// contexts and expose only redacted descriptors. Its unresolved-execution age
/// bound uses the injected facade clock, because a process-local repository has
/// no separate server time.
#[derive(Clone)]
pub struct InMemoryExplorer {
    state: Arc<Mutex<MemoryState>>,
    clock: Arc<dyn Clock>,
}

impl InMemoryExplorer {
    /// Binds one in-memory repository's state to the explorer port.
    #[must_use]
    pub fn new(repository: &InMemoryJobRepository) -> Self {
        Self {
            state: Arc::clone(&repository.state),
            clock: Arc::clone(&repository.clock),
        }
    }

    fn snapshot(&self) -> Result<MemoryState, ExplorerError> {
        self.state
            .lock()
            .map(|state| state.clone())
            .map_err(|_| ExplorerError::Repository(RepositoryError::Unavailable))
    }
}

impl fmt::Debug for InMemoryExplorer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InMemoryExplorer")
            .finish_non_exhaustive()
    }
}

const fn identity_after(window: &QueryWindow) -> Option<u64> {
    match window.after() {
        Some(CursorKey::Identity(value)) => Some(*value),
        _ => None,
    }
}

const fn ordered_after(window: &QueryWindow) -> Option<(u64, u64)> {
    match window.after() {
        Some(CursorKey::Ordered { primary, identity }) => Some((*primary, *identity)),
        _ => None,
    }
}

fn name_after(window: &QueryWindow) -> Option<&str> {
    match window.after() {
        Some(CursorKey::Name(value)) => Some(value.as_str()),
        _ => None,
    }
}

fn limit_of(window: &QueryWindow) -> usize {
    usize::from(window.limit())
}

impl ExplorerRepository for InMemoryExplorer {
    fn identity_ceiling<'a>(
        &'a self,
        query: &'a ExplorerQuery,
    ) -> BoxFuture<'a, Result<u64, ExplorerError>> {
        Box::pin(async move {
            let state = self.snapshot()?;
            let ceiling = match query {
                ExplorerQuery::JobNames => state.job_name_order.values().copied().max(),
                ExplorerQuery::Instances { .. } => {
                    state.instances_by_id.keys().next_back().map(|id| id.get())
                }
                ExplorerQuery::Executions { .. } | ExplorerQuery::UnresolvedExecutions { .. } => {
                    state.job_executions.keys().next_back().map(|id| id.get())
                }
                ExplorerQuery::StepExecutions { .. } => {
                    state.step_executions.keys().next_back().map(|id| id.get())
                }
                ExplorerQuery::RecoveryDecisions { .. } => state
                    .recovery_decisions
                    .values()
                    .flatten()
                    .map(|decision| decision.id().get())
                    .max(),
                ExplorerQuery::FlowDecisions { .. } => {
                    state.flow_decisions.keys().next_back().map(|id| id.get())
                }
                ExplorerQuery::StepPartitions { .. } => {
                    state.step_partitions.keys().next_back().map(|id| id.get())
                }
                ExplorerQuery::OperatorRequests { .. } => state
                    .operator_requests
                    .keys()
                    .next_back()
                    .map(|id| id.get()),
                // Absorbs any query added later: this adapter cannot bound a
                // traversal it does not know, so it reports the missing
                // capability instead of paging from a guessed ceiling.
                _ => return Err(ExplorerError::UnsupportedCapability),
            };
            Ok(ceiling.unwrap_or(0))
        })
    }

    fn job_names<'a>(
        &'a self,
        window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<JobName>, ExplorerError>> {
        Box::pin(async move {
            let state = self.snapshot()?;
            let after = name_after(window);
            Ok(state
                .job_name_order
                .iter()
                .filter(|(_, order)| **order <= window.ceiling())
                .map(|(name, _)| name)
                .filter(|name| after.is_none_or(|after| name.as_str() > after))
                .take(limit_of(window))
                .cloned()
                .collect())
        })
    }

    fn instances<'a>(
        &'a self,
        job_name: &'a JobName,
        window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<JobInstanceProjection>, ExplorerError>> {
        Box::pin(async move {
            let state = self.snapshot()?;
            let after = identity_after(window);
            let rows = state
                .instances_by_id
                .values()
                .rev()
                .filter(|instance| instance.key().job_name() == job_name)
                .filter(|instance| instance.id().get() <= window.ceiling())
                .filter(|instance| after.is_none_or(|after| instance.id().get() < after))
                .take(limit_of(window))
                .map(|instance| state.job_instance_projection(instance))
                .collect::<Vec<_>>();
            Ok(rows)
        })
    }

    fn executions<'a>(
        &'a self,
        job_instance_id: JobInstanceId,
        window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<JobExecutionProjection>, ExplorerError>> {
        Box::pin(async move {
            let state = self.snapshot()?;
            if !state.instances_by_id.contains_key(&job_instance_id) {
                return Err(ExplorerError::Repository(
                    RepositoryError::JobInstanceNotFound {
                        id: job_instance_id,
                    },
                ));
            }
            let after = ordered_after(window);
            state
                .job_executions_by_instance
                .get(&job_instance_id)
                .into_iter()
                .flatten()
                .rev()
                .filter_map(|id| state.job_executions.get(id))
                .filter(|execution| execution.id().get() <= window.ceiling())
                .filter(|execution| {
                    after.is_none_or(|after| {
                        (u64::from(state.attempt_of(execution)), execution.id().get()) < after
                    })
                })
                .take(limit_of(window))
                .map(|execution| state.job_execution_projection(execution))
                .collect()
        })
    }

    fn execution(
        &self,
        job_execution_id: JobExecutionId,
    ) -> BoxFuture<'_, Result<Option<JobExecutionProjection>, ExplorerError>> {
        Box::pin(async move {
            let state = self.snapshot()?;
            state
                .job_executions
                .get(&job_execution_id)
                .map(|execution| state.job_execution_projection(execution))
                .transpose()
        })
    }

    fn step_executions<'a>(
        &'a self,
        job_execution_id: JobExecutionId,
        window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<StepExecutionProjection>, ExplorerError>> {
        Box::pin(async move {
            let state = self.snapshot()?;
            if !state.job_executions.contains_key(&job_execution_id) {
                return Err(ExplorerError::Repository(
                    RepositoryError::JobExecutionNotFound {
                        id: job_execution_id,
                    },
                ));
            }
            let after = identity_after(window);
            Ok(state
                .step_executions_by_job
                .get(&job_execution_id)
                .into_iter()
                .flatten()
                .filter_map(|id| state.step_executions.get(id))
                .filter(|execution| execution.id().get() <= window.ceiling())
                .filter(|execution| after.is_none_or(|after| execution.id().get() > after))
                .take(limit_of(window))
                .map(|execution| state.step_execution_projection(execution))
                .collect())
        })
    }

    fn unresolved_executions<'a>(
        &'a self,
        minimum_age: Duration,
        window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<JobExecutionProjection>, ExplorerError>> {
        Box::pin(async move {
            let state = self.snapshot()?;
            let now = self.clock.now();
            let after = identity_after(window);
            state
                .job_executions
                .values()
                .filter(|execution| !execution.metadata().status().is_finished())
                .filter(|execution| execution.id().get() <= window.ceiling())
                .filter(|execution| after.is_none_or(|after| execution.id().get() > after))
                .filter(|execution| {
                    now.duration_since(updated_at(execution))
                        .unwrap_or(Duration::ZERO)
                        >= minimum_age
                })
                .take(limit_of(window))
                .map(|execution| state.job_execution_projection(execution))
                .collect()
        })
    }

    fn recovery_decisions<'a>(
        &'a self,
        job_execution_id: JobExecutionId,
        window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<RecoveryDecision>, ExplorerError>> {
        Box::pin(async move {
            let state = self.snapshot()?;
            let after = identity_after(window);
            Ok(state
                .recovery_decisions
                .get(&job_execution_id)
                .into_iter()
                .flatten()
                .filter(|decision| decision.id().get() <= window.ceiling())
                .filter(|decision| after.is_none_or(|after| decision.id().get() > after))
                .take(limit_of(window))
                .cloned()
                .collect())
        })
    }

    fn flow_decisions<'a>(
        &'a self,
        job_execution_id: JobExecutionId,
        window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<FlowDecision>, ExplorerError>> {
        Box::pin(async move {
            let state = self.snapshot()?;
            let after = ordered_after(window);
            Ok(state
                .flow_decisions_by_job
                .get(&job_execution_id)
                .into_iter()
                .flatten()
                .filter_map(|id| state.flow_decisions.get(id))
                .filter(|decision| decision.id().get() <= window.ceiling())
                .filter(|decision| {
                    after.is_none_or(|after| {
                        (decision.sequence().get(), decision.id().get()) > after
                    })
                })
                .take(limit_of(window))
                .cloned()
                .collect())
        })
    }

    fn step_partitions<'a>(
        &'a self,
        step_execution_id: StepExecutionId,
        window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<StepPartitionProjection>, ExplorerError>> {
        Box::pin(async move {
            let state = self.snapshot()?;
            if !state.step_executions.contains_key(&step_execution_id) {
                return Err(ExplorerError::Repository(
                    RepositoryError::StepExecutionNotFound {
                        id: step_execution_id,
                    },
                ));
            }
            let after = identity_after(window);
            state
                .step_partitions_by_step
                .get(&step_execution_id)
                .into_iter()
                .flatten()
                .filter_map(|id| state.step_partitions.get(id))
                .filter(|partition| partition.id().get() <= window.ceiling())
                .filter(|partition| after.is_none_or(|after| partition.id().get() > after))
                .take(limit_of(window))
                .map(|partition| {
                    Ok(StepPartitionProjection::new(
                        partition.id(),
                        partition.step_execution_id(),
                        partition.key().as_str().to_owned(),
                        partition.ordinal(),
                        partition.status(),
                        partition.exit_status().clone(),
                        partition.counts(),
                        partition.version(),
                        partition.worker_step_execution_id(),
                        Some(StateEnvelopeDescriptor::new(
                            DurableStateKind::ExecutionContext,
                            partition.context().format_version(),
                            partition.context().schema_id().clone(),
                            partition.context().schema_version(),
                            partition.context().encoded_len(),
                        )),
                    ))
                })
                .collect()
        })
    }

    fn operator_requests<'a>(
        &'a self,
        job_execution_id: JobExecutionId,
        window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<OperatorRecord>, ExplorerError>> {
        Box::pin(async move {
            let state = self.snapshot()?;
            let after = identity_after(window);
            Ok(state
                .operator_requests
                .values()
                .filter(|record| record.job_execution_id() == Some(job_execution_id))
                .filter(|record| record.id().get() <= window.ceiling())
                .filter(|record| after.is_none_or(|after| record.id().get() > after))
                .take(limit_of(window))
                .cloned()
                .collect())
        })
    }
}

impl RecoveryRepository for InMemoryExplorer {
    fn recovery_snapshot<'a>(
        &'a self,
        execution_id: JobExecutionId,
        current_owner: &'a OwnerToken,
    ) -> BoxFuture<'a, Result<RecoverySnapshot, RepositoryError>> {
        Box::pin(async move {
            let state = self
                .state
                .lock()
                .map_err(|_| RepositoryError::Unavailable)?
                .clone();
            let execution = state
                .job_executions
                .get(&execution_id)
                .ok_or(RepositoryError::JobExecutionNotFound { id: execution_id })?;
            let owner = match state.owner_tokens.get(&execution_id) {
                None => OwnerObservation::Absent,
                Some(recorded) if recorded == current_owner => OwnerObservation::CurrentProcess,
                Some(_) => OwnerObservation::OtherProcess,
            };
            let latest_step = state
                .step_executions_by_job
                .get(&execution_id)
                .and_then(|ids| ids.last())
                .and_then(|id| state.step_executions.get(id))
                .map(|step| RecoveryStepEvidence::new(step.id(), step.metadata().status(), None));
            let unknown_commit = execution.metadata().status() == BatchStatus::Unknown
                || execution.metadata().failure().is_some_and(|failure| {
                    failure.category() == crate::FailureCategory::UnknownCommit
                })
                || latest_step
                    .as_ref()
                    .is_some_and(|step| step.status() == BatchStatus::Unknown);
            let committed_flow_decision = state
                .flow_decisions_by_job
                .get(&execution_id)
                .is_some_and(|decisions| !decisions.is_empty());
            let ambiguous_external_effect = state
                .execution_definitions
                .get(&execution_id)
                .is_none_or(definition_has_ambiguous_effect);
            Ok(RecoverySnapshot::new(
                execution_id,
                execution.metadata().status(),
                state.attempt_of(execution),
                execution.version(),
                owner,
                state
                    .execution_updated_at
                    .get(&execution_id)
                    .copied()
                    .unwrap_or_else(|| updated_at(execution)),
                self.clock.now(),
                latest_step,
                crate::RecoveryMarkers::new()
                    .with_unknown_commit(unknown_commit)
                    .with_committed_flow_decision(committed_flow_decision)
                    .with_ambiguous_external_effect(ambiguous_external_effect),
            ))
        })
    }
}

fn definition_has_ambiguous_effect(definition: &DefinitionIdentity) -> bool {
    let Ok(document) = serde_json::from_slice::<serde_json::Value>(definition.canonical_manifest())
    else {
        return true;
    };
    document
        .get("delivery_mode")
        .and_then(serde_json::Value::as_str)
        != Some("atomic_same_resource")
}
