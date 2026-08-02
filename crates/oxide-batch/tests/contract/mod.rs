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
    BatchStatus, DomainError, ExecutionContext, ExecutionCounts, ExecutionVersion, ExitStatus,
    FailureCategory, FailureId, FailureSummary, JobInstanceKey, JobName, JobParameter,
    JobParameters, JobRepository, LifecycleError, LifecycleTransition, ParameterName,
    ParameterRole, ParameterValue, PartitionKey, PartitionPlanEntry, PartitionResult,
    RepositoryError, StateLimits, StepName,
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
    partition_plan_commits_before_any_worker_starts(backend, &mut factory)?;
    duplicate_partition_key_is_rejected(backend, &mut factory)?;
    partition_aggregation_commits_with_parent_terminal_state(backend, &mut factory)
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

    let result = PartitionResult::new(
        BatchStatus::Completed,
        ExitStatus::completed(),
        ExecutionCounts::new(2, 2, 2, 0, 1, 0),
    )
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let mut completion = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let stale = block_on(completion.complete_step_partition(
        assigned.id(),
        ExecutionVersion::INITIAL,
        &result,
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
    let completed =
        block_on(completion.complete_step_partition(assigned.id(), assigned.version(), &result))
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let failed_result = PartitionResult::new(
        BatchStatus::Failed,
        ExitStatus::failed(),
        ExecutionCounts::new(1, 1, 0, 0, 0, 1),
    )
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let failed = block_on(completion.complete_step_partition(
        assigned_for_retry.id(),
        assigned_for_retry.version(),
        &failed_result,
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
    let alpha_result =
        PartitionResult::new(
            BatchStatus::Failed,
            ExitStatus::new(oxide_batch::ExitCode::new("ALPHA_FAILED").map_err(|error| {
                RepositoryContractFailure::new(backend, CASE, error.to_string())
            })?),
            ExecutionCounts::new(1, 2, 3, 4, 5, 6),
        )
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let zeta_result =
        PartitionResult::new(
            BatchStatus::Failed,
            ExitStatus::new(oxide_batch::ExitCode::new("ZETA_FAILED").map_err(|error| {
                RepositoryContractFailure::new(backend, CASE, error.to_string())
            })?),
            ExecutionCounts::new(10, 20, 30, 40, 50, 60),
        )
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(work.complete_step_partition(alpha.id(), alpha.version(), &alpha_result))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let incomplete = block_on(work.aggregate_step_partitions(
        parent.id(),
        parent.version(),
        ended_at,
        Some(FailureSummary::new(
            FailureCategory::UserComponent,
            FailureId::new(900).map_err(|error| {
                RepositoryContractFailure::new(backend, CASE, error.to_string())
            })?,
        )),
    ));
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
    block_on(work.complete_step_partition(zeta.id(), zeta.version(), &zeta_result))
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(work.commit())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let failure = FailureSummary::new(
        FailureCategory::UserComponent,
        FailureId::new(901)
            .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?,
    );
    let mut rolled_back = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let provisional = block_on(rolled_back.aggregate_step_partitions(
        parent.id(),
        parent.version(),
        ended_at,
        Some(failure),
    ))
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        provisional.metadata().status() == BatchStatus::Failed
            && provisional.metadata().exit_status().code().as_str() == "ALPHA_FAILED"
            && provisional.metadata().counts() == ExecutionCounts::new(11, 22, 33, 44, 55, 66),
        backend,
        CASE,
        "provisional aggregate did not follow key order and checked counter sums",
    )?;
    block_on(rolled_back.rollback())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let mut commit = block_on(repository.begin())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let aggregated = block_on(commit.aggregate_step_partitions(
        parent.id(),
        parent.version(),
        ended_at,
        Some(failure),
    ))
    .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    block_on(commit.commit())
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        aggregated.metadata().status() == BatchStatus::Failed
            && aggregated.metadata().failure() == Some(failure)
            && aggregated.metadata().timestamps().ended_at() == Some(ended_at),
        backend,
        CASE,
        "parent terminal state did not commit with its aggregate",
    )
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
