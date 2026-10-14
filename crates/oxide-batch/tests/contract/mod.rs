//! Repository contract cases shared by every metadata implementation.

mod components;
// Only the service test binaries run these cases; other binaries reuse the
// module for the repository and component contracts alone.
#[allow(dead_code)]
mod services;

pub use components::run_component_contract;
#[allow(unused_imports)]
pub use services::{ContractClock, ServiceBackend, run_service_contract};

use std::error::Error;
use std::fmt;
use std::time::{Duration, UNIX_EPOCH};

use futures_executor::block_on;
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

/// Runs the reusable M1 repository instance-identity contract.
///
/// A fresh backend is constructed for each case so registration order and
/// state leakage cannot affect the result.
///
/// # Errors
///
/// Returns [`RepositoryContractFailure`] with the stable case and backend
/// names when construction, an operation, or an observation differs.
pub fn run_repository_contract<R, F>(
    backend: &'static str,
    mut factory: F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    F: FnMut() -> Result<R, RepositoryError>,
{
    create_then_find(backend, &mut factory)?;
    duplicate_key_preserves_original_id(backend, &mut factory)?;
    first_launch_creates_linked_graph(backend, &mut factory)?;
    completed_instance_rejects_another_execution(backend, &mut factory)?;
    stale_transition_is_typed_and_atomic(backend, &mut factory)?;
    rollback_discards_staged_metadata(backend, &mut factory)?;
    compatible_upgrade_transforms_context_before_target_step(backend, &mut factory)?;
    compatible_upgrade_rejects_conflicting_or_stale_edge(backend, &mut factory)?;
    compatible_upgrade_is_not_transitive(backend, &mut factory)?;
    compatible_upgrade_rejects_newer_persisted_state(backend, &mut factory)?;
    partition_plan_commits_before_any_worker_starts(backend, &mut factory)?;
    duplicate_partition_key_is_rejected(backend, &mut factory)?;
    partition_aggregation_commits_with_parent_terminal_state(backend, &mut factory)
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

fn compatible_upgrade_transforms_context_before_target_step<R, F>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    F: FnMut() -> Result<R, RepositoryError>,
{
    const CASE: &str = "compatible_upgrade_transforms_context_before_target_step";
    let repository = factory()
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

    let mut setup = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let instance = block_on(setup.select_or_create_job_instance(&key))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?
        .instance()
        .clone();
    let source_job =
        block_on(setup.create_job_execution_with_definition(instance.id(), &source_definition))
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let source_execution = block_on(setup.create_flow_step_execution(
        source_job.id(),
        &source_step,
        &source_node,
        StartLimit::UNRESTRICTED,
    ))
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let started = block_on(setup.transition_step_execution(
        source_execution.id(),
        source_execution.version(),
        LifecycleTransition::new(BatchStatus::Started, UNIX_EPOCH + Duration::from_secs(1)),
    ))
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let source_context = contract_upgrade_context(7, 1)
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(setup.commit_step_execution_context(started.id(), started.version(), &source_context))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(setup.transition_job_execution(
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
    ))
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(setup.commit())
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

    let mut registration = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(registration.register_definition_upgrade(&job_name, &edge))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(registration.commit())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut restart = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let target_job = block_on(restart.create_job_execution_with_upgrade(instance.id(), &selected))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(restart.create_flow_step_execution(
        target_job.id(),
        &target_step,
        &target_node,
        StartLimit::UNRESTRICTED,
    ))
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(restart.commit())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut inspect = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let state = block_on(inspect.latest_flow_step(instance.id(), &target_node))
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
    block_on(inspect.rollback())
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

fn compatible_upgrade_rejects_conflicting_or_stale_edge<R, F>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    F: FnMut() -> Result<R, RepositoryError>,
{
    const CASE: &str = "compatible_upgrade_rejects_conflicting_or_stale_edge";
    let repository = factory()
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let job_name = JobName::new("repository_contract_upgrade_conflict")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let key = JobInstanceKey::new(job_name.clone(), &JobParameters::new());
    let source = contract_tasklet_definition(&job_name, "v1", "source")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let target = contract_tasklet_definition(&job_name, "v2", "target")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut setup = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let instance = block_on(setup.select_or_create_job_instance(&key))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?
        .instance()
        .clone();
    let source_job =
        block_on(setup.create_job_execution_with_definition(instance.id(), &source))
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(setup.commit())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut fail = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(fail.transition_job_execution(
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
    ))
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(fail.commit())
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

    let mut registration = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(registration.register_definition_upgrade(&job_name, &registered))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(registration.commit())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut conflict = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        matches!(
            block_on(conflict.register_definition_upgrade(&job_name, &stale)),
            Err(RepositoryError::DefinitionUpgradeConflict { .. })
        ),
        backend,
        CASE,
        "a second immutable edge for the same source/target pair was accepted",
    )?;
    block_on(conflict.rollback())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let selected = DefinitionUpgradeExecution::new(stale, [])
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let mut restart = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        matches!(
            block_on(restart.create_job_execution_with_upgrade(instance.id(), &selected)),
            Err(RepositoryError::IncompatibleDefinition { .. })
        ),
        backend,
        CASE,
        "an unregistered/stale selected edge created a compatible restart",
    )?;
    block_on(restart.rollback())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut inspect = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        block_on(inspect.job_executions(instance.id()))
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?
            .len()
            == 1,
        backend,
        CASE,
        "rejected edge selection created a partial target attempt",
    )?;
    block_on(inspect.rollback())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    Ok(())
}

fn compatible_upgrade_is_not_transitive<R, F>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    F: FnMut() -> Result<R, RepositoryError>,
{
    const CASE: &str = "compatible_upgrade_is_not_transitive";
    let repository = factory()
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

    let mut setup = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let instance = block_on(setup.select_or_create_job_instance(&key))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?
        .instance()
        .clone();
    let first = block_on(setup.create_job_execution_with_definition(instance.id(), &v1))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(setup.commit())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let mut fail = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(fail.transition_job_execution(
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
    ))
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(fail.commit())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let v1_v2 = metadata_only_upgrade("v1-v2", "edge-12", v1, v2.clone(), "step-v1", "step-v2")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let v2_v3 = metadata_only_upgrade("v2-v3", "edge-23", v2, v3.clone(), "step-v2", "step-v3")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let mut register = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(register.register_definition_upgrade(&job_name, &v1_v2))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(register.register_definition_upgrade(&job_name, &v2_v3))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(register.commit())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let selected = DefinitionUpgradeExecution::new(v2_v3, [])
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let mut compatible = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        matches!(
            block_on(compatible.create_job_execution_with_upgrade(instance.id(), &selected)),
            Err(RepositoryError::IncompatibleDefinition { .. })
        ),
        backend,
        CASE,
        "v1->v2 plus v2->v3 was inferred as authority for v1->v3",
    )?;
    block_on(compatible.rollback())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut strict = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        matches!(
            block_on(strict.create_job_execution_with_definition(instance.id(), &v3)),
            Err(RepositoryError::IncompatibleDefinition { .. })
        ),
        backend,
        CASE,
        "strict restart inferred transitive definition compatibility",
    )?;
    block_on(strict.rollback())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    Ok(())
}

fn compatible_upgrade_rejects_newer_persisted_state<R, F>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    F: FnMut() -> Result<R, RepositoryError>,
{
    const CASE: &str = "compatible_upgrade_rejects_newer_persisted_state";
    let repository = factory()
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

    let mut setup = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let instance = block_on(setup.select_or_create_job_instance(&key))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?
        .instance()
        .clone();
    let source_job =
        block_on(setup.create_job_execution_with_definition(instance.id(), &source_definition))
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let source_execution = block_on(setup.create_flow_step_execution(
        source_job.id(),
        &source_step,
        &source_node,
        StartLimit::UNRESTRICTED,
    ))
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let started = block_on(setup.transition_step_execution(
        source_execution.id(),
        source_execution.version(),
        LifecycleTransition::new(BatchStatus::Started, UNIX_EPOCH + Duration::from_secs(22)),
    ))
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let newer = contract_upgrade_context(9, 2)
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(setup.commit_step_execution_context(started.id(), started.version(), &newer))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(setup.transition_job_execution(
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
    ))
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(setup.commit())
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

    let mut register = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(register.register_definition_upgrade(&job_name, &edge))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(register.commit())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut restart = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        matches!(
            block_on(restart.create_job_execution_with_upgrade(instance.id(), &selected)),
            Err(RepositoryError::DefinitionUpgradeFailed)
        ),
        backend,
        CASE,
        "state newer than the declared source transform was accepted",
    )?;
    block_on(restart.rollback())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut inspect = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        block_on(inspect.job_executions(instance.id()))
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?
            .len()
            == 1,
        backend,
        CASE,
        "newer-state rejection created a partial target attempt",
    )?;
    block_on(inspect.rollback())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "the shared contract keeps plan, assignment, CAS, completion, and restart observations in one named scenario"
)]
fn partition_plan_commits_before_any_worker_starts<R, F>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    F: FnMut() -> Result<R, RepositoryError>,
{
    const CASE: &str = "partition_plan_commits_before_any_worker_starts";
    let repository = factory()
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
    let mut setup = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let instance = block_on(setup.select_or_create_job_instance(&key))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?
        .instance()
        .clone();
    let job = block_on(setup.create_job_execution(instance.id()))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let parent = block_on(setup.create_step_execution(job.id(), &parent_name))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let parent = block_on(setup.transition_step_execution(
        parent.id(),
        parent.version(),
        LifecycleTransition::new(BatchStatus::Started, UNIX_EPOCH + Duration::from_secs(1)),
    ))
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let worker = block_on(setup.create_step_execution(job.id(), &worker_name))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let retry_worker = block_on(setup.create_step_execution(job.id(), &retry_worker_name))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let restarted_worker = block_on(setup.create_step_execution(job.id(), &restarted_worker_name))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(setup.commit())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let entries = [partition_entry("zeta")?, partition_entry("alpha")?];
    let mut planning = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let planned = block_on(planning.create_step_partition_plan(parent.id(), &entries))
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
    let early_assignment = block_on(planning.assign_step_partition(
        planned[0].id(),
        planned[0].version(),
        worker.id(),
    ));
    ensure(
        early_assignment
            == Err(RepositoryError::PartitionPlanNotCommitted {
                step_execution_id: parent.id(),
            }),
        backend,
        CASE,
        "worker assignment was visible before the plan transaction committed",
    )?;
    block_on(planning.commit())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut assignment = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let ordered = block_on(assignment.step_partition_plan(parent.id()))
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
    let assigned = block_on(assignment.assign_step_partition(
        planned[0].id(),
        ExecutionVersion::INITIAL,
        worker.id(),
    ))
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let reused_worker = block_on(assignment.assign_step_partition(
        planned[1].id(),
        ExecutionVersion::INITIAL,
        worker.id(),
    ));
    ensure(
        reused_worker
            == Err(RepositoryError::PartitionWorkerAlreadyAssigned {
                worker_step_execution_id: worker.id(),
            }),
        backend,
        CASE,
        "one worker attempt was assigned to multiple partitions",
    )?;
    let assigned_for_retry = block_on(assignment.assign_step_partition(
        planned[1].id(),
        ExecutionVersion::INITIAL,
        retry_worker.id(),
    ))
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(assignment.commit())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut completion = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let worker = block_on(completion.transition_step_execution(
        worker.id(),
        worker.version(),
        LifecycleTransition::new(BatchStatus::Started, UNIX_EPOCH + Duration::from_secs(2)),
    ))
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let worker = block_on(completion.enrich_step_exit_status(
        worker.id(),
        worker.version(),
        &ExitStatus::completed(),
    ))
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let worker = block_on(completion.transition_step_execution(
        worker.id(),
        worker.version(),
        LifecycleTransition::new(BatchStatus::Completed, UNIX_EPOCH + Duration::from_secs(3)),
    ))
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let stale = block_on(completion.complete_step_partition(
        assigned.id(),
        ExecutionVersion::INITIAL,
        worker.id(),
    ));
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
    let completed = block_on(completion.complete_step_partition(
        assigned.id(),
        assigned.version(),
        worker.id(),
    ))
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let retry_worker = block_on(completion.transition_step_execution(
        retry_worker.id(),
        retry_worker.version(),
        LifecycleTransition::new(BatchStatus::Started, UNIX_EPOCH + Duration::from_secs(2)),
    ))
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let retry_worker = block_on(completion.enrich_step_exit_status(
        retry_worker.id(),
        retry_worker.version(),
        &ExitStatus::failed(),
    ))
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let retry_worker = block_on(completion.transition_step_execution(
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
    ))
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let failed = block_on(completion.complete_step_partition(
        assigned_for_retry.id(),
        assigned_for_retry.version(),
        retry_worker.id(),
    ))
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(completion.commit())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut restart = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let rerun =
        block_on(restart.assign_step_partition(completed.id(), completed.version(), worker.id()));
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
    let retried = block_on(restart.assign_step_partition(
        failed.id(),
        failed.version(),
        restarted_worker.id(),
    ))
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
    let replan = block_on(restart.create_step_partition_plan(parent.id(), &entries));
    ensure(
        replan
            == Err(RepositoryError::PartitionPlanExists {
                step_execution_id: parent.id(),
            }),
        backend,
        CASE,
        "persisted partition plan could be replaced on restart",
    )?;
    block_on(restart.commit())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))
}

fn duplicate_partition_key_is_rejected<R, F>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    F: FnMut() -> Result<R, RepositoryError>,
{
    const CASE: &str = "duplicate_partition_key_is_rejected";
    let repository = factory()
        .map_err(|error| RepositoryContractFailure::new("unavailable", CASE, error.to_string()))?;
    let key = contract_key("2026-08-03")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let step_name = StepName::new("partitioned")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let mut unit = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let instance = block_on(unit.select_or_create_job_instance(&key))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?
        .instance()
        .clone();
    let job = block_on(unit.create_job_execution(instance.id()))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let parent = block_on(unit.create_step_execution(job.id(), &step_name))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let entries = [partition_entry("same")?, partition_entry("same")?];
    let duplicate = block_on(unit.create_step_partition_plan(parent.id(), &entries));
    ensure(
        duplicate == Err(RepositoryError::DuplicatePartitionKey),
        backend,
        CASE,
        "duplicate byte-exact key was accepted",
    )?;
    ensure(
        block_on(unit.step_partition_plan(parent.id()))
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?
            .is_empty(),
        backend,
        CASE,
        "duplicate-key rejection retained a partial plan",
    )?;
    block_on(unit.rollback())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))
}

#[allow(
    clippy::too_many_lines,
    reason = "the shared scenario proves incomplete, rollback, and committed aggregation boundaries together"
)]
fn partition_aggregation_commits_with_parent_terminal_state<R, F>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    F: FnMut() -> Result<R, RepositoryError>,
{
    const CASE: &str = "partition_aggregation_commits_with_parent_terminal_state";
    let repository = factory()
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

    let mut setup = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let instance = block_on(setup.select_or_create_job_instance(&key))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?
        .instance()
        .clone();
    let job = block_on(setup.create_job_execution(instance.id()))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let parent = block_on(setup.create_step_execution(job.id(), &parent_name))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let parent = block_on(setup.transition_step_execution(
        parent.id(),
        parent.version(),
        LifecycleTransition::new(BatchStatus::Started, started_at),
    ))
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let worker = block_on(setup.create_step_execution(job.id(), &worker_name))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let other_worker = block_on(setup.create_step_execution(job.id(), &other_worker_name))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(setup.commit())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut plan = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let partitions = block_on(plan.create_step_partition_plan(
        parent.id(),
        &[partition_entry("zeta")?, partition_entry("alpha")?],
    ))
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(plan.commit())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut work = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let zeta = block_on(work.assign_step_partition(
        partitions[0].id(),
        partitions[0].version(),
        other_worker.id(),
    ))
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let alpha = block_on(work.assign_step_partition(
        partitions[1].id(),
        partitions[1].version(),
        worker.id(),
    ))
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let alpha_exit = ExitStatus::new(
        oxide_batch::ExitCode::new("ALPHA_FAILED")
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?,
    );
    let worker = block_on(work.transition_step_execution(
        worker.id(),
        worker.version(),
        LifecycleTransition::new(BatchStatus::Started, started_at),
    ))
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let worker = block_on(work.enrich_step_exit_status(worker.id(), worker.version(), &alpha_exit))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let alpha_failure = FailureSummary::new(
        FailureCategory::UserComponent,
        FailureId::new(901)
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?,
    );
    let worker = block_on(work.transition_step_execution(
        worker.id(),
        worker.version(),
        LifecycleTransition::failed(ended_at, alpha_failure),
    ))
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let zeta_exit = ExitStatus::new(
        oxide_batch::ExitCode::new("ZETA_FAILED")
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?,
    );
    let other_worker = block_on(work.transition_step_execution(
        other_worker.id(),
        other_worker.version(),
        LifecycleTransition::new(BatchStatus::Started, started_at),
    ))
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let other_worker = block_on(work.enrich_step_exit_status(
        other_worker.id(),
        other_worker.version(),
        &zeta_exit,
    ))
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let other_worker = block_on(work.transition_step_execution(
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
    ))
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(work.complete_step_partition(alpha.id(), alpha.version(), worker.id()))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let incomplete =
        block_on(work.aggregate_step_partitions(parent.id(), parent.version(), ended_at));
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
    block_on(work.complete_step_partition(zeta.id(), zeta.version(), other_worker.id()))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(work.commit())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut rolled_back = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let provisional =
        block_on(rolled_back.aggregate_step_partitions(parent.id(), parent.version(), ended_at))
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        provisional.metadata().status() == BatchStatus::Failed
            && provisional.metadata().exit_status().code().as_str() == "ALPHA_FAILED"
            && provisional.metadata().counts() == ExecutionCounts::default(),
        backend,
        CASE,
        "provisional aggregate did not follow key order and checked counter sums",
    )?;
    block_on(rolled_back.rollback())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut commit = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let aggregated =
        block_on(commit.aggregate_step_partitions(parent.id(), parent.version(), ended_at))
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(commit.commit())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        aggregated.metadata().status() == BatchStatus::Failed
            && aggregated.metadata().failure() == Some(alpha_failure)
            && aggregated.metadata().timestamps().ended_at() == Some(ended_at),
        backend,
        CASE,
        "parent terminal state did not commit with its aggregate",
    )?;
    let mut retry = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let duplicate =
        block_on(retry.aggregate_step_partitions(parent.id(), parent.version(), ended_at))
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        duplicate == aggregated,
        backend,
        CASE,
        "retried aggregation did not return the already committed parent",
    )?;
    let reopened = block_on(retry.assign_step_partition(
        alpha.id(),
        ExecutionVersion::new(alpha.version().get() + 1),
        worker.id(),
    ));
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
    block_on(retry.rollback())
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

fn create_then_find<R, F>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    F: FnMut() -> Result<R, RepositoryError>,
{
    const CASE: &str = "repository_create_then_find_instance";
    let repository = factory()
        .map_err(|error| RepositoryContractFailure::new("unavailable", CASE, error.to_string()))?;
    let key = contract_key("2026-07-29")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let mut unit = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let outcome = block_on(unit.select_or_create_job_instance(&key))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let proposed_id = outcome.instance().id();
    ensure(
        outcome.was_created(),
        backend,
        CASE,
        "first selection did not create an instance",
    )?;
    block_on(unit.commit())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let mut inspection = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let found = block_on(inspection.find_job_instance(&key))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        found.as_ref().map(oxide_batch::JobInstance::id) == Some(proposed_id),
        backend,
        CASE,
        "created instance was not found by its identifying key",
    )?;
    block_on(inspection.rollback())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))
}

fn duplicate_key_preserves_original_id<R, F>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    F: FnMut() -> Result<R, RepositoryError>,
{
    const CASE: &str = "repository_duplicate_key_preserves_original_id";
    let repository = factory()
        .map_err(|error| RepositoryContractFailure::new("unavailable", CASE, error.to_string()))?;
    let key = contract_key("2026-07-29")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let mut first = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let created = block_on(first.select_or_create_job_instance(&key))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let first_id = created.instance().id();
    block_on(first.commit())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut second = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let duplicate = block_on(second.select_or_create_job_instance(&key))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        !duplicate.was_created() && duplicate.instance().id() == first_id,
        backend,
        CASE,
        "duplicate creation replaced or duplicated the logical instance",
    )?;
    block_on(second.rollback())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))
}

fn first_launch_creates_linked_graph<R, F>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    F: FnMut() -> Result<R, RepositoryError>,
{
    const CASE: &str = "repository_first_launch_creates_linked_graph";
    let repository = factory()
        .map_err(|error| RepositoryContractFailure::new("unavailable", CASE, error.to_string()))?;
    let key = contract_key("2026-07-29")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let step_name = StepName::new("import")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let mut unit = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let instance = block_on(unit.select_or_create_job_instance(&key))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?
        .instance()
        .clone();
    let job = block_on(unit.create_job_execution(instance.id()))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let step = block_on(unit.create_step_execution(job.id(), &step_name))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        job.job_instance_id() == instance.id() && step.job_execution_id() == job.id(),
        backend,
        CASE,
        "created execution graph was not linked",
    )?;
    block_on(unit.commit())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut inspection = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let jobs = block_on(inspection.job_executions(instance.id()))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let steps = block_on(inspection.step_executions(job.id()))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        jobs == vec![job] && steps == vec![step],
        backend,
        CASE,
        "execution graph inspection differed from creation",
    )?;
    block_on(inspection.rollback())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))
}

fn completed_instance_rejects_another_execution<R, F>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    F: FnMut() -> Result<R, RepositoryError>,
{
    const CASE: &str = "repository_completed_instance_rejects_launch";
    let repository = factory()
        .map_err(|error| RepositoryContractFailure::new("unavailable", CASE, error.to_string()))?;
    let key = contract_key("2026-07-29")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let mut first = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let instance = block_on(first.select_or_create_job_instance(&key))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?
        .instance()
        .clone();
    let execution = block_on(first.create_job_execution(instance.id()))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let started = block_on(first.transition_job_execution(
        execution.id(),
        execution.version(),
        LifecycleTransition::new(BatchStatus::Started, UNIX_EPOCH + Duration::from_secs(1)),
    ))
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(first.transition_job_execution(
        started.id(),
        started.version(),
        LifecycleTransition::new(BatchStatus::Completed, UNIX_EPOCH + Duration::from_secs(2)),
    ))
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(first.commit())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut duplicate = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let error = block_on(duplicate.create_job_execution(instance.id()));
    ensure(
        error == Err(RepositoryError::CompletedInstance { id: instance.id() }),
        backend,
        CASE,
        "completed instance accepted another execution",
    )?;
    block_on(duplicate.rollback())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))
}

fn stale_transition_is_typed_and_atomic<R, F>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    F: FnMut() -> Result<R, RepositoryError>,
{
    const CASE: &str = "repository_stale_transition_is_typed_and_atomic";
    let repository = factory()
        .map_err(|error| RepositoryContractFailure::new("unavailable", CASE, error.to_string()))?;
    let key = contract_key("2026-07-29")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let mut unit = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let instance = block_on(unit.select_or_create_job_instance(&key))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?
        .instance()
        .clone();
    let execution = block_on(unit.create_job_execution(instance.id()))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let error = block_on(unit.transition_job_execution(
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
    ));
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
    let unchanged = block_on(unit.get_job_execution(execution.id()))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        unchanged == Some(execution),
        backend,
        CASE,
        "rejected stale transition mutated the execution",
    )?;
    block_on(unit.rollback())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))
}

fn rollback_discards_staged_metadata<R, F>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    F: FnMut() -> Result<R, RepositoryError>,
{
    const CASE: &str = "repository_rollback_discards_staged_metadata";
    let repository = factory()
        .map_err(|error| RepositoryContractFailure::new("unavailable", CASE, error.to_string()))?;
    let key = contract_key("2026-07-29")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let mut staged = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(staged.select_or_create_job_instance(&key))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(staged.rollback())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut inspection = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let found = block_on(inspection.find_job_instance(&key))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        found.is_none(),
        backend,
        CASE,
        "rolled-back metadata became visible",
    )?;
    block_on(inspection.rollback())
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
