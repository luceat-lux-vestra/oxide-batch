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
    BatchStatus, DomainError, ExecutionVersion, FailureCategory, FailureId, FailureSummary,
    JobInstanceKey, JobName, JobParameter, JobParameters, JobRepository, LifecycleError,
    LifecycleTransition, ParameterName, ParameterRole, ParameterValue, RepositoryError, StepName,
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
    rollback_discards_staged_metadata(backend, &mut factory)
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
