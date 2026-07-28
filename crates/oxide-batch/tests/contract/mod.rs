//! Repository contract cases shared by every metadata implementation.

use std::error::Error;
use std::fmt;

use oxide_batch::{
    DomainError, JobInstanceId, JobInstanceKey, JobName, JobParameter, JobParameters,
    ParameterName, ParameterRole, ParameterValue,
};

/// Result of creating a logical job instance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreateInstanceOutcome {
    /// This call created the instance.
    Created(JobInstanceId),
    /// The identifying key was already associated with this instance.
    Existing(JobInstanceId),
}

/// The narrow adapter implemented by each repository contract test.
///
/// It is deliberately test-owned: future production repository APIs can
/// evolve without publishing the harness, while in-memory and `PostgreSQL`
/// adapters still execute identical semantic cases.
pub trait RepositoryContract {
    /// Backend-specific safe-to-display failure.
    type Error: Error;

    /// Human-readable implementation name included in failures.
    fn backend_name(&self) -> &'static str;

    /// Creates or finds the instance selected by an identifying key.
    fn create_instance(
        &mut self,
        key: &JobInstanceKey,
        proposed_id: JobInstanceId,
    ) -> Result<CreateInstanceOutcome, Self::Error>;

    /// Finds the ID associated with an identifying key.
    fn find_instance(&self, key: &JobInstanceKey) -> Result<Option<JobInstanceId>, Self::Error>;
}

/// Runs the reusable M1 repository instance-identity contract.
///
/// A fresh backend is constructed for each case so registration order and
/// state leakage cannot affect the result.
///
/// # Errors
///
/// Returns [`RepositoryContractFailure`] with the stable case and backend
/// names when construction, an operation, or an observation differs.
pub fn run_repository_contract<R, F>(mut factory: F) -> Result<(), RepositoryContractFailure>
where
    R: RepositoryContract,
    F: FnMut() -> Result<R, R::Error>,
{
    create_then_find(&mut factory)?;
    duplicate_key_preserves_original_id(&mut factory)
}

fn create_then_find<R, F>(factory: &mut F) -> Result<(), RepositoryContractFailure>
where
    R: RepositoryContract,
    F: FnMut() -> Result<R, R::Error>,
{
    const CASE: &str = "repository_create_then_find_instance";
    let mut repository = factory()
        .map_err(|error| RepositoryContractFailure::new("unavailable", CASE, error.to_string()))?;
    let backend = repository.backend_name();
    let key = contract_key("2026-07-29")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let proposed_id = JobInstanceId::new(101)
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    let outcome = repository
        .create_instance(&key, proposed_id)
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        outcome == CreateInstanceOutcome::Created(proposed_id),
        backend,
        CASE,
        "first creation did not report the proposed ID",
    )?;
    let found = repository
        .find_instance(&key)
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        found == Some(proposed_id),
        backend,
        CASE,
        "created instance was not found by its identifying key",
    )
}

fn duplicate_key_preserves_original_id<R, F>(
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: RepositoryContract,
    F: FnMut() -> Result<R, R::Error>,
{
    const CASE: &str = "repository_duplicate_key_preserves_original_id";
    let mut repository = factory()
        .map_err(|error| RepositoryContractFailure::new("unavailable", CASE, error.to_string()))?;
    let backend = repository.backend_name();
    let key = contract_key("2026-07-29")
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let first_id = JobInstanceId::new(201)
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let conflicting_id = JobInstanceId::new(202)
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;

    repository
        .create_instance(&key, first_id)
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    let duplicate = repository
        .create_instance(&key, conflicting_id)
        .map_err(|error| RepositoryContractFailure::new(backend, CASE, error.to_string()))?;
    ensure(
        duplicate == CreateInstanceOutcome::Existing(first_id),
        backend,
        CASE,
        "duplicate creation replaced or duplicated the logical instance",
    )
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
