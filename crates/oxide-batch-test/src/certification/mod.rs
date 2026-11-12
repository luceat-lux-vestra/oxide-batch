//! Reusable repository-adapter certification contracts.
//!
//! These runners exercise public `OxideBatch` repository, explorer, operator,
//! and retention semantics. The contract surface is capability-scoped: core
//! repository semantics are certified separately from optional definition,
//! partition, explorer, operator/control, and retention capabilities.
//!
//! Passing the applicable groups is necessary evidence for an adapter, but does
//! not by itself grant an `OxideBatch` support or compatibility claim. Concrete
//! adapters remain responsible for isolated setup, cleanup, version declarations,
//! and the additional evidence required by the support matrix.

mod services;

pub use services::{
    ContractClock, ServiceBackend, run_explorer_contract, run_full_service_contract,
    run_operator_control_contract, run_retention_contract,
};

use std::error::Error;
use std::fmt;
use std::future::Future;
use std::time::{Duration, UNIX_EPOCH};

use oxide_batch::{
    BatchStatus, ComponentRevision, DefinitionIdentity, DefinitionNodeUpgrade, DefinitionRevision,
    DefinitionStateKind, DefinitionStateTransform, DefinitionStateTransformError,
    DefinitionStateTransformId, DefinitionStateTransformer, DefinitionUpgrade,
    DefinitionUpgradeExecution, DefinitionUpgradeKey, DefinitionUpgradeRevision, DomainError,
    ExecutionContext, ExecutionCounts, ExecutionVersion, ExitStatus, FailureCategory, FailureId,
    FailureSummary, JobInstanceKey, JobName, JobParameter, JobParameters, JobRepository,
    LifecycleError, LifecycleTransition, NodeId, ParameterName, ParameterRole, ParameterValue,
    PartitionKey, PartitionPlanEntry, RepositoryError, StartLimit, StateLimits, StateSchemaId,
    StateSchemaVersion, StepName,
};

/// Runs the mandatory portable repository contract.
///
/// This group contains only semantics required of every durable
/// `JobRepository`: logical-instance identity, execution graph identity,
/// lifecycle compare-and-swap, completion/relaunch policy, and rollback
/// visibility. Optional repository capabilities are certified by separate
/// runners.
///
/// # Errors
///
/// Returns [`RepositoryContractFailure`] with the stable case and backend
/// names when construction, an operation, or an observation differs.
pub async fn run_core_repository_contract<R, F, Fut>(
    backend: &'static str,
    mut factory: F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<R, RepositoryError>>,
{
    create_then_find(backend, &mut factory).await?;
    duplicate_key_preserves_original_id(backend, &mut factory).await?;
    first_launch_creates_linked_graph(backend, &mut factory).await?;
    completed_instance_rejects_another_execution(backend, &mut factory).await?;
    stale_transition_is_typed_and_atomic(backend, &mut factory).await?;
    rollback_discards_staged_metadata(backend, &mut factory).await
}

/// Runs the definition-registry/compatible-upgrade capability contract.
///
/// Adapters that do not declare this capability should not run this group.
///
/// # Errors
///
/// Returns [`RepositoryContractFailure`] when any definition-upgrade case
/// differs from the accepted public semantics.
pub async fn run_definition_upgrade_contract<R, F, Fut>(
    backend: &'static str,
    mut factory: F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<R, RepositoryError>>,
{
    compatible_upgrade_transforms_context_before_target_step(backend, &mut factory).await?;
    compatible_upgrade_rejects_conflicting_or_stale_edge(backend, &mut factory).await?;
    compatible_upgrade_is_not_transitive(backend, &mut factory).await?;
    compatible_upgrade_rejects_newer_persisted_state(backend, &mut factory).await?;
    compatible_upgrade_target_only_node_starts_fresh(backend, &mut factory).await
}

/// Runs the durable local-partition capability contract.
///
/// Adapters that do not declare durable step partitions should not run this
/// group.
///
/// # Errors
///
/// Returns [`RepositoryContractFailure`] when any partition case differs from
/// the accepted public semantics.
pub async fn run_partition_contract<R, F, Fut>(
    backend: &'static str,
    mut factory: F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<R, RepositoryError>>,
{
    partition_plan_commits_before_any_worker_starts(backend, &mut factory).await?;
    duplicate_partition_key_is_rejected(backend, &mut factory).await?;
    partition_aggregation_commits_with_parent_terminal_state(backend, &mut factory).await
}

/// Runs every repository contract group used by the in-memory and PostgreSQL
/// reference adapters.
///
/// This aggregate is an equivalence/control convenience for adapters that claim
/// all currently represented capabilities. It is not the minimum portable
/// adapter contract and must not be used to infer support for an undeclared
/// capability.
///
/// # Errors
///
/// Returns [`RepositoryContractFailure`] with the stable case and backend
/// names when any included case differs.
pub async fn run_full_repository_contract<R, F, Fut>(
    backend: &'static str,
    mut factory: F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<R, RepositoryError>>,
{
    create_then_find(backend, &mut factory).await?;
    duplicate_key_preserves_original_id(backend, &mut factory).await?;
    first_launch_creates_linked_graph(backend, &mut factory).await?;
    completed_instance_rejects_another_execution(backend, &mut factory).await?;
    stale_transition_is_typed_and_atomic(backend, &mut factory).await?;
    rollback_discards_staged_metadata(backend, &mut factory).await?;
    compatible_upgrade_transforms_context_before_target_step(backend, &mut factory).await?;
    compatible_upgrade_rejects_conflicting_or_stale_edge(backend, &mut factory).await?;
    compatible_upgrade_is_not_transitive(backend, &mut factory).await?;
    compatible_upgrade_rejects_newer_persisted_state(backend, &mut factory).await?;
    compatible_upgrade_target_only_node_starts_fresh(backend, &mut factory).await?;
    partition_plan_commits_before_any_worker_starts(backend, &mut factory).await?;
    duplicate_partition_key_is_rejected(backend, &mut factory).await?;
    partition_aggregation_commits_with_parent_terminal_state(backend, &mut factory).await
}

fn compatible_upgrade_payload(payload: &[u8]) -> Result<Vec<u8>, DefinitionStateTransformError> {
    let mut value: serde_json::Value = serde_json::from_slice(payload)
        .map_err(|_| DefinitionStateTransformError::InvalidSource)?;
    let current = value
        .get("value")
        .and_then(serde_json::Value::as_u64)
        .ok_or(DefinitionStateTransformError::InvalidSource)?;
    value["value"] = serde_json::json!(current.saturating_add(1));
    serde_json::to_vec(&value).map_err(|_| DefinitionStateTransformError::TransformFailed)
}

fn contract_upgrade_definition(
    job_name: &JobName,
    revision: &str,
    node: &NodeId,
    step_name: &StepName,
    schema_version: u32,
) -> Result<DefinitionIdentity, Box<dyn Error>> {
    let manifest = serde_json::json!({
        "entry": node.as_str(),
        "format": 4,
        "job": job_name.as_str(),
        "nodes": [{
            "handler_revision": revision,
            "id": node.as_str(),
            "kind": "custom_leaf",
            "leaf_kind": "contract.handler",
            "listeners": [],
            "policy": null,
            "start": {
                "allow_start_if_complete": false,
                "start_limit": u32::MAX,
            },
            "state_schema": {
                "id": "contract.context",
                "version": schema_version,
            },
            "step_name": step_name.as_str(),
        }],
        "transitions": [],
    });
    let bytes = serde_json::to_vec(&manifest)?;
    Ok(DefinitionIdentity::from_flow_manifest(
        job_name,
        DefinitionRevision::new(revision)?,
        &bytes,
    )?)
}

fn contract_upgrade_multi_definition(
    job_name: &JobName,
    revision: &str,
    nodes: &[(&NodeId, &StepName)],
) -> Result<DefinitionIdentity, Box<dyn Error>> {
    let entry = nodes.first().ok_or("upgrade contract requires one node")?.0;
    let nodes = nodes
        .iter()
        .map(|(node, step)| {
            serde_json::json!({
                "handler_revision": revision,
                "id": node.as_str(),
                "kind": "custom_leaf",
                "leaf_kind": "contract.handler",
                "listeners": [],
                "policy": null,
                "start": {
                    "allow_start_if_complete": false,
                    "start_limit": u32::MAX,
                },
                "state_schema": {
                    "id": "contract.context",
                    "version": 1,
                },
                "step_name": step.as_str(),
            })
        })
        .collect::<Vec<_>>();
    let manifest = serde_json::json!({
        "entry": entry.as_str(),
        "format": 4,
        "job": job_name.as_str(),
        "nodes": nodes,
        "transitions": [],
    });
    let bytes = serde_json::to_vec(&manifest)?;
    Ok(DefinitionIdentity::from_flow_manifest(
        job_name,
        DefinitionRevision::new(revision)?,
        &bytes,
    )?)
}

fn contract_upgrade_context(value: u64, version: u32) -> Result<ExecutionContext, Box<dyn Error>> {
    let bytes = serde_json::to_vec(&serde_json::json!({
        "format": "oxide-batch.execution-context",
        "format_version": 1,
        "schema": "contract.context",
        "schema_version": version,
        "payload": { "value": value },
    }))?;
    Ok(ExecutionContext::from_json(&bytes, StateLimits::default())?)
}

async fn compatible_upgrade_transforms_context_before_target_step<R, F, Fut>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<R, RepositoryError>>,
{
    const CASE: &str = "compatible_upgrade_transforms_context_before_target_step";
    let repository = factory().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let job_name = JobName::new("repository_contract_job")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let key = JobInstanceKey::new(job_name.clone(), &JobParameters::new());
    let source_node = NodeId::new("upgrade-source")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let target_node = NodeId::new("upgrade-target")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let source_step = StepName::new("upgrade-source-step")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let target_step = StepName::new("upgrade-target-step")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let source_definition =
        contract_upgrade_definition(&job_name, "upgrade-v1", &source_node, &source_step, 1)
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let target_definition =
        contract_upgrade_definition(&job_name, "upgrade-v2", &target_node, &target_step, 2)
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut setup = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let instance = setup.select_or_create_job_instance(&key).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?
        .instance()
        .clone();
    let source_job =
        setup.create_job_execution_with_definition(instance.id(), &source_definition).await
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let source_execution = setup.create_flow_step_execution(
        source_job.id(),
        &source_step,
        &source_node,
        StartLimit::UNRESTRICTED,
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let started = setup.transition_step_execution(
        source_execution.id(),
        source_execution.version(),
        LifecycleTransition::new(BatchStatus::Started, UNIX_EPOCH + Duration::from_secs(1)),
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let source_context = contract_upgrade_context(7, 1)
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    setup.commit_step_execution_context(started.id(), started.version(), &source_context).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    setup.transition_job_execution(
        source_job.id(),
        source_job.version(),
        LifecycleTransition::failed(
            UNIX_EPOCH + Duration::from_secs(2),
            FailureSummary::new(
                FailureCategory::UserComponent,
                FailureId::new(9).map_err(|error| {
                    RepositoryContractFailure::new(backend, CASE, error.to_string())
                })?,
            ),
        ),
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    setup.commit().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let transform_revision = DefinitionUpgradeRevision::new("context-v1-to-v2")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let edge = DefinitionUpgrade::transformed(
        DefinitionUpgradeKey::new("upgrade-v1-to-v2")
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?,
        DefinitionUpgradeRevision::new("edge-v1")
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?,
        source_definition,
        target_definition,
        [DefinitionNodeUpgrade::new(
            source_node.clone(),
            target_node.clone(),
        )],
        [DefinitionStateTransform::new(
            source_node,
            target_node.clone(),
            DefinitionStateKind::ExecutionContext,
            DefinitionStateTransformId::new("context").map_err(|error| {
                RepositoryContractFailure::new(backend, CASE, error.to_string())
            })?,
            StateSchemaId::new("contract.context").map_err(|error| {
                RepositoryContractFailure::new(backend, CASE, error.to_string())
            })?,
            StateSchemaVersion::new(1).map_err(|error| {
                RepositoryContractFailure::new(backend, CASE, error.to_string())
            })?,
            StateSchemaId::new("contract.context").map_err(|error| {
                RepositoryContractFailure::new(backend, CASE, error.to_string())
            })?,
            StateSchemaVersion::new(2).map_err(|error| {
                RepositoryContractFailure::new(backend, CASE, error.to_string())
            })?,
            transform_revision.clone(),
        )],
    )
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let selected = DefinitionUpgradeExecution::new(
        edge.clone(),
        [DefinitionStateTransformer::new(
            transform_revision,
            compatible_upgrade_payload,
        )],
    )
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut registration = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    registration.register_definition_upgrade(&job_name, &edge).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    registration.commit().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut restart = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let target_job = restart.create_job_execution_with_upgrade(instance.id(), &selected).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    restart.create_flow_step_execution(
        target_job.id(),
        &target_step,
        &target_node,
        StartLimit::UNRESTRICTED,
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    restart.commit().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut inspect = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let state = inspect.latest_flow_step(instance.id(), &target_node).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?
        .ok_or_else(|| RepositoryContractFailure::new(backend, CASE, "target state missing"))?;
    let context = state
        .context()
        .ok_or_else(|| RepositoryContractFailure::new(backend, CASE, "target context missing"))?;
    ensure(
        context.schema_id().as_str() == "contract.context" && context.schema_version().get() == 2,
        backend,
        CASE,
        "target context schema was not transformed before materialization",
    )?;
    let payload: serde_json::Value = serde_json::from_slice(
        &context
            .payload_json()
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?,
    )
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        payload.get("value").and_then(serde_json::Value::as_u64) == Some(8),
        backend,
        CASE,
        "target context payload did not use the selected live transformer",
    )?;
    inspect.rollback().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    Ok(())
}

fn contract_tasklet_definition(
    job_name: &JobName,
    revision: &str,
    step: &str,
) -> Result<DefinitionIdentity, Box<dyn Error>> {
    Ok(DefinitionIdentity::tasklet(
        job_name,
        &StepName::new(step)?,
        DefinitionRevision::new(revision)?,
        &ComponentRevision::new(format!("tasklet-{revision}"))?,
    )?)
}

fn metadata_only_upgrade(
    key: &str,
    revision: &str,
    from: DefinitionIdentity,
    to: DefinitionIdentity,
    source: &str,
    target: &str,
) -> Result<DefinitionUpgrade, Box<dyn Error>> {
    Ok(DefinitionUpgrade::transformed(
        DefinitionUpgradeKey::new(key)?,
        DefinitionUpgradeRevision::new(revision)?,
        from,
        to,
        [DefinitionNodeUpgrade::new(
            NodeId::new(source)?,
            NodeId::new(target)?,
        )],
        [],
    )?)
}

async fn compatible_upgrade_rejects_conflicting_or_stale_edge<R, F, Fut>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<R, RepositoryError>>,
{
    const CASE: &str = "compatible_upgrade_rejects_conflicting_or_stale_edge";
    let repository = factory().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let job_name = JobName::new("repository_contract_upgrade_conflict")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let key = JobInstanceKey::new(job_name.clone(), &JobParameters::new());
    let source = contract_tasklet_definition(&job_name, "v1", "source")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let target = contract_tasklet_definition(&job_name, "v2", "target")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut setup = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let instance = setup.select_or_create_job_instance(&key).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?
        .instance()
        .clone();
    let source_job =
        setup.create_job_execution_with_definition(instance.id(), &source).await
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    setup.commit().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut fail = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    fail.transition_job_execution(
        source_job.id(),
        source_job.version(),
        LifecycleTransition::failed(
            UNIX_EPOCH + Duration::from_secs(20),
            FailureSummary::new(
                FailureCategory::UserComponent,
                FailureId::new(20).map_err(|error| {
                    RepositoryContractFailure::new(backend, CASE, error.to_string())
                })?,
            ),
        ),
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    fail.commit().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let registered = metadata_only_upgrade(
        "registered-edge",
        "edge-v1",
        source.clone(),
        target.clone(),
        "source",
        "target",
    )
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let stale = metadata_only_upgrade("stale-edge", "edge-v2", source, target, "source", "target")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut registration = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    registration.register_definition_upgrade(&job_name, &registered).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    registration.commit().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut conflict = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        matches!(
            conflict.register_definition_upgrade(&job_name, &stale).await,
            Err(RepositoryError::DefinitionUpgradeConflict { .. })
        ),
        backend,
        CASE,
        "a second immutable edge for the same source/target pair was accepted",
    )?;
    conflict.rollback().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let selected = DefinitionUpgradeExecution::new(stale, [])
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let mut restart = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        matches!(
            restart.create_job_execution_with_upgrade(instance.id(), &selected).await,
            Err(RepositoryError::IncompatibleDefinition { .. })
        ),
        backend,
        CASE,
        "an unregistered/stale selected edge created a compatible restart",
    )?;
    restart.rollback().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut inspect = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        inspect.job_executions(instance.id()).await
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?
            .len()
            == 1,
        backend,
        CASE,
        "rejected edge selection created a partial target attempt",
    )?;
    inspect.rollback().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    Ok(())
}

async fn compatible_upgrade_is_not_transitive<R, F, Fut>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<R, RepositoryError>>,
{
    const CASE: &str = "compatible_upgrade_is_not_transitive";
    let repository = factory().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let job_name = JobName::new("repository_contract_upgrade_non_transitive")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let key = JobInstanceKey::new(job_name.clone(), &JobParameters::new());
    let v1 = contract_tasklet_definition(&job_name, "v1", "step-v1")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let v2 = contract_tasklet_definition(&job_name, "v2", "step-v2")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let v3 = contract_tasklet_definition(&job_name, "v3", "step-v3")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut setup = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let instance = setup.select_or_create_job_instance(&key).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?
        .instance()
        .clone();
    let first = setup.create_job_execution_with_definition(instance.id(), &v1).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    setup.commit().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let mut fail = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    fail.transition_job_execution(
        first.id(),
        first.version(),
        LifecycleTransition::failed(
            UNIX_EPOCH + Duration::from_secs(21),
            FailureSummary::new(
                FailureCategory::UserComponent,
                FailureId::new(21).map_err(|error| {
                    RepositoryContractFailure::new(backend, CASE, error.to_string())
                })?,
            ),
        ),
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    fail.commit().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let v1_v2 = metadata_only_upgrade("v1-v2", "edge-12", v1, v2.clone(), "step-v1", "step-v2")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let v2_v3 = metadata_only_upgrade("v2-v3", "edge-23", v2, v3.clone(), "step-v2", "step-v3")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let mut register = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    register.register_definition_upgrade(&job_name, &v1_v2).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    register.register_definition_upgrade(&job_name, &v2_v3).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    register.commit().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let selected = DefinitionUpgradeExecution::new(v2_v3, [])
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let mut compatible = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        matches!(
            compatible.create_job_execution_with_upgrade(instance.id(), &selected).await,
            Err(RepositoryError::IncompatibleDefinition { .. })
        ),
        backend,
        CASE,
        "v1->v2 plus v2->v3 was inferred as authority for v1->v3",
    )?;
    compatible.rollback().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut strict = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        matches!(
            strict.create_job_execution_with_definition(instance.id(), &v3).await,
            Err(RepositoryError::IncompatibleDefinition { .. })
        ),
        backend,
        CASE,
        "strict restart inferred transitive definition compatibility",
    )?;
    strict.rollback().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    Ok(())
}

async fn compatible_upgrade_rejects_newer_persisted_state<R, F, Fut>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<R, RepositoryError>>,
{
    const CASE: &str = "compatible_upgrade_rejects_newer_persisted_state";
    let repository = factory().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let job_name = JobName::new("repository_contract_upgrade_newer_state")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let key = JobInstanceKey::new(job_name.clone(), &JobParameters::new());
    let source_node = NodeId::new("source")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let target_node = NodeId::new("target")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let source_step = StepName::new("source-step")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let target_step = StepName::new("target-step")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let source_definition =
        contract_upgrade_definition(&job_name, "v1", &source_node, &source_step, 1)
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let target_definition =
        contract_upgrade_definition(&job_name, "v2", &target_node, &target_step, 2)
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut setup = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let instance = setup.select_or_create_job_instance(&key).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?
        .instance()
        .clone();
    let source_job =
        setup.create_job_execution_with_definition(instance.id(), &source_definition).await
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let source_execution = setup.create_flow_step_execution(
        source_job.id(),
        &source_step,
        &source_node,
        StartLimit::UNRESTRICTED,
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let started = setup.transition_step_execution(
        source_execution.id(),
        source_execution.version(),
        LifecycleTransition::new(BatchStatus::Started, UNIX_EPOCH + Duration::from_secs(22)),
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let newer = contract_upgrade_context(9, 2)
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    setup.commit_step_execution_context(started.id(), started.version(), &newer).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    setup.transition_job_execution(
        source_job.id(),
        source_job.version(),
        LifecycleTransition::failed(
            UNIX_EPOCH + Duration::from_secs(23),
            FailureSummary::new(
                FailureCategory::UserComponent,
                FailureId::new(23).map_err(|error| {
                    RepositoryContractFailure::new(backend, CASE, error.to_string())
                })?,
            ),
        ),
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    setup.commit().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let transform_revision = DefinitionUpgradeRevision::new("context-v1-to-v2")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let edge = DefinitionUpgrade::transformed(
        DefinitionUpgradeKey::new("v1-to-v2")
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?,
        DefinitionUpgradeRevision::new("edge-v1")
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?,
        source_definition,
        target_definition,
        [DefinitionNodeUpgrade::new(
            source_node.clone(),
            target_node.clone(),
        )],
        [DefinitionStateTransform::new(
            source_node,
            target_node,
            DefinitionStateKind::ExecutionContext,
            DefinitionStateTransformId::new("context").map_err(|error| {
                RepositoryContractFailure::new(backend, CASE, error.to_string())
            })?,
            StateSchemaId::new("contract.context").map_err(|error| {
                RepositoryContractFailure::new(backend, CASE, error.to_string())
            })?,
            StateSchemaVersion::new(1).map_err(|error| {
                RepositoryContractFailure::new(backend, CASE, error.to_string())
            })?,
            StateSchemaId::new("contract.context").map_err(|error| {
                RepositoryContractFailure::new(backend, CASE, error.to_string())
            })?,
            StateSchemaVersion::new(2).map_err(|error| {
                RepositoryContractFailure::new(backend, CASE, error.to_string())
            })?,
            transform_revision.clone(),
        )],
    )
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let selected = DefinitionUpgradeExecution::new(
        edge.clone(),
        [DefinitionStateTransformer::new(
            transform_revision,
            compatible_upgrade_payload,
        )],
    )
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut register = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    register.register_definition_upgrade(&job_name, &edge).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    register.commit().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut restart = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        matches!(
            restart.create_job_execution_with_upgrade(instance.id(), &selected).await,
            Err(RepositoryError::DefinitionUpgradeFailed)
        ),
        backend,
        CASE,
        "state newer than the declared source transform was accepted",
    )?;
    restart.rollback().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut inspect = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        inspect.job_executions(instance.id()).await
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?
            .len()
            == 1,
        backend,
        CASE,
        "newer-state rejection created a partial target attempt",
    )?;
    inspect.rollback().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "the shared proof keeps source mapping, target-only materialization, and both parity observations together"
)]
async fn compatible_upgrade_target_only_node_starts_fresh<R, F, Fut>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<R, RepositoryError>>,
{
    const CASE: &str = "compatible_upgrade_target_only_node_starts_fresh";
    let repository = factory().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let job_name = JobName::new("repository_contract_upgrade_target_only")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let key = JobInstanceKey::new(job_name.clone(), &JobParameters::new());

    let source = NodeId::new("source")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let legacy_extra = NodeId::new("legacy-extra")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let target = NodeId::new("target")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let renamed_extra = NodeId::new("renamed-extra")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let source_step = StepName::new("source-step")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let legacy_extra_step = StepName::new("legacy-extra-step")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let target_step = StepName::new("target-step")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let renamed_extra_step = StepName::new("renamed-extra-step")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let target_only_step = StepName::new("target-only-step")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let source_definition = contract_upgrade_multi_definition(
        &job_name,
        "v1",
        &[(&source, &source_step), (&legacy_extra, &legacy_extra_step)],
    )
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let target_definition = contract_upgrade_multi_definition(
        &job_name,
        "v2",
        &[
            (&target, &target_step),
            (&renamed_extra, &renamed_extra_step),
            (&legacy_extra, &target_only_step),
        ],
    )
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut setup = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let instance = setup.select_or_create_job_instance(&key).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?
        .instance()
        .clone();
    let source_job =
        setup.create_job_execution_with_definition(instance.id(), &source_definition).await
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let started_job = setup.transition_job_execution(
        source_job.id(),
        source_job.version(),
        LifecycleTransition::new(BatchStatus::Started, UNIX_EPOCH + Duration::from_secs(30)),
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    for (node, step, value, failure_id) in [
        (&source, &source_step, 7_u64, 30_u64),
        (&legacy_extra, &legacy_extra_step, 41_u64, 31_u64),
    ] {
        let execution = setup.create_flow_step_execution(
            source_job.id(),
            step,
            node,
            StartLimit::UNRESTRICTED,
        ).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
        let started = setup.transition_step_execution(
            execution.id(),
            execution.version(),
            LifecycleTransition::new(BatchStatus::Started, UNIX_EPOCH + Duration::from_secs(30)),
        ).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
        let context = contract_upgrade_context(value, 1)
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
        let committed = setup.commit_step_execution_context(
            started.id(),
            started.version(),
            &context,
        ).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
        setup.transition_step_execution(
            committed.id(),
            committed.version(),
            LifecycleTransition::failed(
                UNIX_EPOCH + Duration::from_secs(31),
                FailureSummary::new(
                    FailureCategory::UserComponent,
                    FailureId::new(failure_id).map_err(|error| {
                        RepositoryContractFailure::new(backend, CASE, error.to_string())
                    })?,
                ),
            ),
        ).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    }
    setup.transition_job_execution(
        started_job.id(),
        started_job.version(),
        LifecycleTransition::failed(
            UNIX_EPOCH + Duration::from_secs(31),
            FailureSummary::new(
                FailureCategory::UserComponent,
                FailureId::new(32).map_err(|error| {
                    RepositoryContractFailure::new(backend, CASE, error.to_string())
                })?,
            ),
        ),
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    setup.commit().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let edge = DefinitionUpgrade::transformed(
        DefinitionUpgradeKey::new("v1-to-v2-target-only")
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?,
        DefinitionUpgradeRevision::new("edge-v1")
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?,
        source_definition,
        target_definition,
        [
            DefinitionNodeUpgrade::new(source, target),
            DefinitionNodeUpgrade::new(legacy_extra.clone(), renamed_extra.clone()),
        ],
        [],
    )
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let selected = DefinitionUpgradeExecution::new(edge.clone(), [])
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let mut register = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    register.register_definition_upgrade(&job_name, &edge).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    register.commit().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut restart = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let target_job = restart.create_job_execution_with_upgrade(instance.id(), &selected).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    restart.create_flow_step_execution(
        target_job.id(),
        &renamed_extra_step,
        &renamed_extra,
        StartLimit::UNRESTRICTED,
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    restart.create_flow_step_execution(
        target_job.id(),
        &target_only_step,
        &legacy_extra,
        StartLimit::UNRESTRICTED,
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    restart.commit().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut inspect = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let mapped = inspect.latest_flow_step(instance.id(), &renamed_extra).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?
        .ok_or_else(|| RepositoryContractFailure::new(backend, CASE, "mapped target missing"))?;
    let mapped_context = mapped.context().ok_or_else(|| {
        RepositoryContractFailure::new(backend, CASE, "mapped source state was dropped")
    })?;
    let mapped_payload: serde_json::Value = serde_json::from_slice(
        &mapped_context
            .payload_json()
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?,
    )
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        mapped_payload
            .get("value")
            .and_then(serde_json::Value::as_u64)
            == Some(41),
        backend,
        CASE,
        "mapped durable source state was not preserved under its renamed target",
    )?;

    let fresh = inspect.latest_flow_step(instance.id(), &legacy_extra).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?
        .ok_or_else(|| RepositoryContractFailure::new(backend, CASE, "target-only node missing"))?;
    ensure(
        fresh.execution().job_execution_id() == target_job.id()
            && fresh.execution().metadata().counts() == ExecutionCounts::default()
            && fresh.context().is_none(),
        backend,
        CASE,
        "target-only node inherited historical state instead of declared initial state",
    )?;
    inspect.rollback().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "the shared contract keeps plan, assignment, CAS, completion, and restart observations in one named scenario"
)]
async fn partition_plan_commits_before_any_worker_starts<R, F, Fut>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<R, RepositoryError>>,
{
    const CASE: &str = "partition_plan_commits_before_any_worker_starts";
    let repository = factory().await
        .map_err(|error| RepositoryContractFailure::new("unavailable", CASE, error.to_string()))?;
    let key = contract_key("2026-08-02")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let parent_name = StepName::new("partitioned")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let worker_name = StepName::new("worker")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let retry_worker_name = StepName::new("retry-worker")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let restarted_worker_name = StepName::new("restarted-worker")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let mut setup = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let instance = setup.select_or_create_job_instance(&key).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?
        .instance()
        .clone();
    let job = setup.create_job_execution(instance.id()).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let parent = setup.create_step_execution(job.id(), &parent_name).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let parent = setup.transition_step_execution(
        parent.id(),
        parent.version(),
        LifecycleTransition::new(BatchStatus::Started, UNIX_EPOCH + Duration::from_secs(1)),
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let worker = setup.create_step_execution(job.id(), &worker_name).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let retry_worker = setup.create_step_execution(job.id(), &retry_worker_name).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let restarted_worker = setup.create_step_execution(job.id(), &restarted_worker_name).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    setup.commit().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let entries = [partition_entry("zeta")?, partition_entry("alpha")?];
    let mut planning = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let planned = planning.create_step_partition_plan(parent.id(), &entries).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        planned.iter().all(|partition| {
            partition.status() == BatchStatus::Starting
                && partition.worker_step_execution_id().is_none()
                && partition.version() == ExecutionVersion::INITIAL
        }),
        backend,
        CASE,
        "new partition plan did not remain wholly unassigned",
    )?;
    let early_assignment = planning.assign_step_partition(
        planned[0].id(),
        planned[0].version(),
        worker.id(),
    ).await;
    ensure(
        early_assignment
            == Err(RepositoryError::PartitionPlanNotCommitted {
                step_execution_id: parent.id(),
            }),
        backend,
        CASE,
        "worker assignment was visible before the plan transaction committed",
    )?;
    planning.commit().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut assignment = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let ordered = assignment.step_partition_plan(parent.id()).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        ordered
            .iter()
            .map(|partition| partition.key().as_str())
            .collect::<Vec<_>>()
            == vec!["alpha", "zeta"],
        backend,
        CASE,
        "partition aggregation read was not ordered by byte-exact key",
    )?;
    let assigned = assignment.assign_step_partition(
        planned[0].id(),
        ExecutionVersion::INITIAL,
        worker.id(),
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let reused_worker = assignment.assign_step_partition(
        planned[1].id(),
        ExecutionVersion::INITIAL,
        worker.id(),
    ).await;
    ensure(
        reused_worker
            == Err(RepositoryError::PartitionWorkerAlreadyAssigned {
                worker_step_execution_id: worker.id(),
            }),
        backend,
        CASE,
        "one worker attempt was assigned to multiple partitions",
    )?;
    let assigned_for_retry = assignment.assign_step_partition(
        planned[1].id(),
        ExecutionVersion::INITIAL,
        retry_worker.id(),
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    assignment.commit().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut completion = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let worker = completion.transition_step_execution(
        worker.id(),
        worker.version(),
        LifecycleTransition::new(BatchStatus::Started, UNIX_EPOCH + Duration::from_secs(2)),
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let worker = completion.enrich_step_exit_status(
        worker.id(),
        worker.version(),
        &ExitStatus::completed(),
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let worker = completion.transition_step_execution(
        worker.id(),
        worker.version(),
        LifecycleTransition::new(BatchStatus::Completed, UNIX_EPOCH + Duration::from_secs(3)),
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let stale = completion.complete_step_partition(
        assigned.id(),
        ExecutionVersion::INITIAL,
        worker.id(),
    ).await;
    ensure(
        stale
            == Err(RepositoryError::Lifecycle(LifecycleError::StaleVersion {
                expected: ExecutionVersion::INITIAL,
                actual: assigned.version(),
            })),
        backend,
        CASE,
        "stale partition writer did not lose compare-and-swap",
    )?;
    let completed = completion.complete_step_partition(
        assigned.id(),
        assigned.version(),
        worker.id(),
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let retry_worker = completion.transition_step_execution(
        retry_worker.id(),
        retry_worker.version(),
        LifecycleTransition::new(BatchStatus::Started, UNIX_EPOCH + Duration::from_secs(2)),
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let retry_worker = completion.enrich_step_exit_status(
        retry_worker.id(),
        retry_worker.version(),
        &ExitStatus::failed(),
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let retry_worker = completion.transition_step_execution(
        retry_worker.id(),
        retry_worker.version(),
        LifecycleTransition::failed(
            UNIX_EPOCH + Duration::from_secs(3),
            FailureSummary::new(
                FailureCategory::UserComponent,
                FailureId::new(800).map_err(|error| {
                    RepositoryContractFailure::new(backend, CASE, error.to_string())
                })?,
            ),
        ),
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let failed = completion.complete_step_partition(
        assigned_for_retry.id(),
        assigned_for_retry.version(),
        retry_worker.id(),
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    completion.commit().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut restart = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let rerun =
        restart.assign_step_partition(completed.id(), completed.version(), worker.id()).await;
    ensure(
        rerun
            == Err(RepositoryError::PartitionUpdateNotAllowed {
                id: completed.id(),
                status: BatchStatus::Completed,
            }),
        backend,
        CASE,
        "completed partition was assignable on restart",
    )?;
    let retried = restart.assign_step_partition(
        failed.id(),
        failed.version(),
        restarted_worker.id(),
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        retried.status() == BatchStatus::Started
            && retried.worker_step_execution_id() == Some(restarted_worker.id())
            && retried.exit_status() == &ExitStatus::unknown()
            && retried.counts() == ExecutionCounts::default(),
        backend,
        CASE,
        "failed partition retry retained the prior attempt result",
    )?;
    let replan = restart.create_step_partition_plan(parent.id(), &entries).await;
    ensure(
        replan
            == Err(RepositoryError::PartitionPlanExists {
                step_execution_id: parent.id(),
            }),
        backend,
        CASE,
        "persisted partition plan could be replaced on restart",
    )?;
    restart.commit().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))
}

async fn duplicate_partition_key_is_rejected<R, F, Fut>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<R, RepositoryError>>,
{
    const CASE: &str = "duplicate_partition_key_is_rejected";
    let repository = factory().await
        .map_err(|error| RepositoryContractFailure::new("unavailable", CASE, error.to_string()))?;
    let key = contract_key("2026-08-03")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let step_name = StepName::new("partitioned")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let mut unit = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let instance = unit.select_or_create_job_instance(&key).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?
        .instance()
        .clone();
    let job = unit.create_job_execution(instance.id()).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let parent = unit.create_step_execution(job.id(), &step_name).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let entries = [partition_entry("same")?, partition_entry("same")?];
    let duplicate = unit.create_step_partition_plan(parent.id(), &entries).await;
    ensure(
        duplicate == Err(RepositoryError::DuplicatePartitionKey),
        backend,
        CASE,
        "duplicate byte-exact key was accepted",
    )?;
    ensure(
        unit.step_partition_plan(parent.id()).await
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?
            .is_empty(),
        backend,
        CASE,
        "duplicate-key rejection retained a partial plan",
    )?;
    unit.rollback().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))
}

#[allow(
    clippy::too_many_lines,
    reason = "the shared scenario proves incomplete, rollback, and committed aggregation boundaries together"
)]
async fn partition_aggregation_commits_with_parent_terminal_state<R, F, Fut>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<R, RepositoryError>>,
{
    const CASE: &str = "partition_aggregation_commits_with_parent_terminal_state";
    let repository = factory().await
        .map_err(|error| RepositoryContractFailure::new("unavailable", CASE, error.to_string()))?;
    let key = contract_key("2026-08-04")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let parent_name = StepName::new("aggregate-parent")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let worker_name = StepName::new("aggregate-worker-a")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let other_worker_name = StepName::new("aggregate-worker-z")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let started_at = UNIX_EPOCH + Duration::from_secs(1);
    let ended_at = UNIX_EPOCH + Duration::from_secs(2);

    let mut setup = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let instance = setup.select_or_create_job_instance(&key).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?
        .instance()
        .clone();
    let job = setup.create_job_execution(instance.id()).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let parent = setup.create_step_execution(job.id(), &parent_name).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let parent = setup.transition_step_execution(
        parent.id(),
        parent.version(),
        LifecycleTransition::new(BatchStatus::Started, started_at),
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let worker = setup.create_step_execution(job.id(), &worker_name).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let other_worker = setup.create_step_execution(job.id(), &other_worker_name).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    setup.commit().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut plan = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let partitions = plan.create_step_partition_plan(
        parent.id(),
        &[partition_entry("zeta")?, partition_entry("alpha")?],
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    plan.commit().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut work = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let zeta = work.assign_step_partition(
        partitions[0].id(),
        partitions[0].version(),
        other_worker.id(),
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let alpha = work.assign_step_partition(
        partitions[1].id(),
        partitions[1].version(),
        worker.id(),
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let alpha_exit = ExitStatus::new(
        oxide_batch::ExitCode::new("ALPHA_FAILED")
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?,
    );
    let worker = work.transition_step_execution(
        worker.id(),
        worker.version(),
        LifecycleTransition::new(BatchStatus::Started, started_at),
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let worker = work.enrich_step_exit_status(worker.id(), worker.version(), &alpha_exit).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let alpha_failure = FailureSummary::new(
        FailureCategory::UserComponent,
        FailureId::new(901)
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?,
    );
    let worker = work.transition_step_execution(
        worker.id(),
        worker.version(),
        LifecycleTransition::failed(ended_at, alpha_failure),
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let zeta_exit = ExitStatus::new(
        oxide_batch::ExitCode::new("ZETA_FAILED")
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?,
    );
    let other_worker = work.transition_step_execution(
        other_worker.id(),
        other_worker.version(),
        LifecycleTransition::new(BatchStatus::Started, started_at),
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let other_worker = work.enrich_step_exit_status(
        other_worker.id(),
        other_worker.version(),
        &zeta_exit,
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let other_worker = work.transition_step_execution(
        other_worker.id(),
        other_worker.version(),
        LifecycleTransition::failed(
            ended_at,
            FailureSummary::new(
                FailureCategory::UserComponent,
                FailureId::new(902).map_err(|error| {
                    RepositoryContractFailure::new(backend, CASE, error.to_string())
                })?,
            ),
        ),
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    work.complete_step_partition(alpha.id(), alpha.version(), worker.id()).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let incomplete =
        work.aggregate_step_partitions(parent.id(), parent.version(), ended_at).await;
    ensure(
        incomplete
            == Err(RepositoryError::PartitionAggregationIncomplete {
                step_execution_id: parent.id(),
                status: BatchStatus::Started,
            }),
        backend,
        CASE,
        "active child allowed a partial parent aggregate",
    )?;
    work.complete_step_partition(zeta.id(), zeta.version(), other_worker.id()).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    work.commit().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut rolled_back = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let provisional =
        rolled_back.aggregate_step_partitions(parent.id(), parent.version(), ended_at).await
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        provisional.metadata().status() == BatchStatus::Failed
            && provisional.metadata().exit_status().code().as_str() == "ALPHA_FAILED"
            && provisional.metadata().counts() == ExecutionCounts::default(),
        backend,
        CASE,
        "provisional aggregate did not follow key order and checked counter sums",
    )?;
    rolled_back.rollback().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut commit = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let aggregated =
        commit.aggregate_step_partitions(parent.id(), parent.version(), ended_at).await
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    commit.commit().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        aggregated.metadata().status() == BatchStatus::Failed
            && aggregated.metadata().failure() == Some(alpha_failure)
            && aggregated.metadata().timestamps().ended_at() == Some(ended_at),
        backend,
        CASE,
        "parent terminal state did not commit with its aggregate",
    )?;
    let mut retry = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let duplicate =
        retry.aggregate_step_partitions(parent.id(), parent.version(), ended_at).await
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        duplicate == aggregated,
        backend,
        CASE,
        "retried aggregation did not return the already committed parent",
    )?;
    let reopened = retry.assign_step_partition(
        alpha.id(),
        ExecutionVersion::new(alpha.version().get() + 1),
        worker.id(),
    ).await;
    ensure(
        reopened
            == Err(RepositoryError::PartitionParentNotActive {
                step_execution_id: parent.id(),
                status: BatchStatus::Failed,
            }),
        backend,
        CASE,
        "terminal parent allowed a child retry assignment",
    )?;
    retry.rollback().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))
}

fn partition_entry(key: &str) -> Result<PartitionPlanEntry, RepositoryContractFailure> {
    const CASE: &str = "partition_fixture";
    let context = ExecutionContext::from_json(
        br#"{"format":"oxide-batch.execution-context","format_version":1,"schema":"contract.partition","schema_version":1,"payload":{"range":"redacted"}}"#,
        StateLimits::new(4 * 1024, 16)
            .map_err(|error| RepositoryContractFailure::new("fixture", CASE, error.to_string()))?,
    )
    .map_err(|error| RepositoryContractFailure::new("fixture", CASE, error.to_string()))?;
    PartitionPlanEntry::new(
        PartitionKey::new(key)
            .map_err(|error| RepositoryContractFailure::new("fixture", CASE, error.to_string()))?,
        context,
    )
    .map_err(|error| RepositoryContractFailure::new("fixture", CASE, error.to_string()))
}

async fn create_then_find<R, F, Fut>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<R, RepositoryError>>,
{
    const CASE: &str = "repository_create_then_find_instance";
    let repository = factory().await
        .map_err(|error| RepositoryContractFailure::new("unavailable", CASE, error.to_string()))?;
    let key = contract_key("2026-07-29")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let mut unit = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let outcome = unit.select_or_create_job_instance(&key).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let proposed_id = outcome.instance().id();
    ensure(
        outcome.was_created(),
        backend,
        CASE,
        "first selection did not create an instance",
    )?;
    unit.commit().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let mut inspection = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let found = inspection.find_job_instance(&key).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        found.as_ref().map(oxide_batch::JobInstance::id) == Some(proposed_id),
        backend,
        CASE,
        "created instance was not found by its identifying key",
    )?;
    inspection.rollback().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))
}

async fn duplicate_key_preserves_original_id<R, F, Fut>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<R, RepositoryError>>,
{
    const CASE: &str = "repository_duplicate_key_preserves_original_id";
    let repository = factory().await
        .map_err(|error| RepositoryContractFailure::new("unavailable", CASE, error.to_string()))?;
    let key = contract_key("2026-07-29")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let mut first = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let created = first.select_or_create_job_instance(&key).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let first_id = created.instance().id();
    first.commit().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut second = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let duplicate = second.select_or_create_job_instance(&key).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        !duplicate.was_created() && duplicate.instance().id() == first_id,
        backend,
        CASE,
        "duplicate creation replaced or duplicated the logical instance",
    )?;
    second.rollback().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))
}

async fn first_launch_creates_linked_graph<R, F, Fut>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<R, RepositoryError>>,
{
    const CASE: &str = "repository_first_launch_creates_linked_graph";
    let repository = factory().await
        .map_err(|error| RepositoryContractFailure::new("unavailable", CASE, error.to_string()))?;
    let key = contract_key("2026-07-29")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let step_name = StepName::new("import")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let mut unit = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let instance = unit.select_or_create_job_instance(&key).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?
        .instance()
        .clone();
    let job = unit.create_job_execution(instance.id()).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let step = unit.create_step_execution(job.id(), &step_name).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        job.job_instance_id() == instance.id() && step.job_execution_id() == job.id(),
        backend,
        CASE,
        "created execution graph was not linked",
    )?;
    unit.commit().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut inspection = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let jobs = inspection.job_executions(instance.id()).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let steps = inspection.step_executions(job.id()).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        jobs == vec![job] && steps == vec![step],
        backend,
        CASE,
        "execution graph inspection differed from creation",
    )?;
    inspection.rollback().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))
}

async fn completed_instance_rejects_another_execution<R, F, Fut>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<R, RepositoryError>>,
{
    const CASE: &str = "repository_completed_instance_rejects_launch";
    let repository = factory().await
        .map_err(|error| RepositoryContractFailure::new("unavailable", CASE, error.to_string()))?;
    let key = contract_key("2026-07-29")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let mut first = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let instance = first.select_or_create_job_instance(&key).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?
        .instance()
        .clone();
    let execution = first.create_job_execution(instance.id()).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let started = first.transition_job_execution(
        execution.id(),
        execution.version(),
        LifecycleTransition::new(BatchStatus::Started, UNIX_EPOCH + Duration::from_secs(1)),
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    first.transition_job_execution(
        started.id(),
        started.version(),
        LifecycleTransition::new(BatchStatus::Completed, UNIX_EPOCH + Duration::from_secs(2)),
    ).await
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    first.commit().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut duplicate = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let error = duplicate.create_job_execution(instance.id()).await;
    ensure(
        error == Err(RepositoryError::CompletedInstance { id: instance.id() }),
        backend,
        CASE,
        "completed instance accepted another execution",
    )?;
    duplicate.rollback().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))
}

async fn stale_transition_is_typed_and_atomic<R, F, Fut>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<R, RepositoryError>>,
{
    const CASE: &str = "repository_stale_transition_is_typed_and_atomic";
    let repository = factory().await
        .map_err(|error| RepositoryContractFailure::new("unavailable", CASE, error.to_string()))?;
    let key = contract_key("2026-07-29")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let mut unit = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let instance = unit.select_or_create_job_instance(&key).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?
        .instance()
        .clone();
    let execution = unit.create_job_execution(instance.id()).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let error = unit.transition_job_execution(
        execution.id(),
        ExecutionVersion::new(1),
        LifecycleTransition::failed(
            UNIX_EPOCH + Duration::from_secs(1),
            FailureSummary::new(
                FailureCategory::UserComponent,
                FailureId::new(99).map_err(|source| {
                    RepositoryContractFailure::new(backend, CASE, source.to_string())
                })?,
            ),
        ),
    ).await;
    ensure(
        error
            == Err(RepositoryError::Lifecycle(LifecycleError::StaleVersion {
                expected: ExecutionVersion::new(1),
                actual: ExecutionVersion::INITIAL,
            })),
        backend,
        CASE,
        "stale transition did not return the accepted optimistic conflict",
    )?;
    let unchanged = unit.get_job_execution(execution.id()).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        unchanged == Some(execution),
        backend,
        CASE,
        "rejected stale transition mutated the execution",
    )?;
    unit.rollback().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))
}

async fn rollback_discards_staged_metadata<R, F, Fut>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<R, RepositoryError>>,
{
    const CASE: &str = "repository_rollback_discards_staged_metadata";
    let repository = factory().await
        .map_err(|error| RepositoryContractFailure::new("unavailable", CASE, error.to_string()))?;
    let key = contract_key("2026-07-29")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let mut staged = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    staged.select_or_create_job_instance(&key).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    staged.rollback().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut inspection = repository.begin().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let found = inspection.find_job_instance(&key).await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        found.is_none(),
        backend,
        CASE,
        "rolled-back metadata became visible",
    )?;
    inspection.rollback().await
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))
}

fn contract_key(business_date: &str) -> Result<JobInstanceKey, DomainError> {
    let parameters = JobParameters::try_from_iter([(
        ParameterName::new("business_date")?,
        JobParameter::new(
            ParameterValue::string(business_date)?,
            ParameterRole::Identifying,
        ),
    )])?;
    Ok(JobInstanceKey::new(
        JobName::new("repository_contract_job")?,
        &parameters,
    ))
}

fn ensure(
    condition: bool,
    backend: &'static str,
    case: &'static str,
    detail: &'static str,
) -> Result<(), RepositoryContractFailure> {
    if condition {
        Ok(())
    } else {
        Err(RepositoryContractFailure::new(backend, case, detail))
    }
}

/// Safe diagnostic from a shared repository contract case.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepositoryContractFailure {
    backend: &'static str,
    case: &'static str,
    detail: String,
}

impl RepositoryContractFailure {
    fn new(backend: &'static str, case: &'static str, detail: impl Into<String>) -> Self {
        Self {
            backend,
            case,
            detail: detail.into(),
        }
    }
}

impl fmt::Display for RepositoryContractFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "backend={} contract={} detail={}",
            self.backend, self.case, self.detail
        )
    }
}

impl Error for RepositoryContractFailure {}
