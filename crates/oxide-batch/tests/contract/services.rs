//! Operator, explorer, and retention contract cases shared by every backend.
//!
//! The cases below are the named M4 scenarios for `REPO-EXPLORE-001`,
//! `REPO-OPERATOR-001`, `REPO-RETENTION-001`, and the M4 `LIFE-ABANDON-001`
//! and `LIFE-STOP-001` slices. They observe only the public service surface,
//! so an adapter cannot pass them by exposing durable internals.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_executor::block_on;
use oxide_batch::{
    ActorRef, BatchStatus, Clock, ComponentRevision, CursorError, DefinitionIdentity,
    DefinitionRevision, ExecutionVersion, ExplorerError, ExplorerRepository, FailureCategory,
    FailureId, FailureSummary, JobExecution, JobExecutionId, JobExplorer, JobInstanceId,
    JobInstanceKey, JobName, JobOperator, JobParameter, JobParameters, JobRepository,
    LifecycleTransition, OperationId, OperatorError, OperatorOutcomeClass, OperatorRejection,
    OperatorRequest, PageRequest, PageSize, ParameterName, ParameterRole, ParameterValue,
    PurgeBatchBound, PurgeCandidate, PurgePlanRequest, ReasonCode, RepositoryError,
    RetentionOutcome, RetentionService, StepName, TerminalStatusSet,
};

use super::{RepositoryContractFailure, ensure};

/// A deterministic, explicitly advanced facade clock.
pub struct ContractClock {
    millis: AtomicU64,
}

impl ContractClock {
    /// Starts the clock at a fixed instant after the epoch.
    #[must_use]
    pub const fn new(millis: u64) -> Self {
        Self {
            millis: AtomicU64::new(millis),
        }
    }

    /// Advances the clock by an explicit duration.
    pub fn advance(&self, step: Duration) {
        let millis = u64::try_from(step.as_millis()).unwrap_or(u64::MAX);
        self.millis.fetch_add(millis, Ordering::Relaxed);
    }
}

impl Clock for ContractClock {
    fn now(&self) -> SystemTime {
        UNIX_EPOCH + Duration::from_millis(self.millis.load(Ordering::Relaxed))
    }
}

/// One backend under test and the clock that drives it.
pub struct ServiceBackend<R, S> {
    /// The metadata repository the operator and retention services bind to.
    pub repository: R,
    /// The bounded read port the explorer binds to.
    pub explorer: S,
    /// The facade clock shared by the backend and the test.
    pub clock: Arc<ContractClock>,
}

/// Runs the reusable M4 operator, explorer, and retention contract.
///
/// A fresh backend is constructed for each case, so registration order and
/// state leakage cannot affect the result.
///
/// # Errors
///
/// Returns [`RepositoryContractFailure`] with the stable case and backend
/// names when construction, an operation, or an observation differs.
pub fn run_service_contract<R, S, F>(
    backend: &'static str,
    mut factory: F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository + Clone,
    S: ExplorerRepository,
    F: FnMut(Arc<ContractClock>) -> Result<ServiceBackend<R, S>, RepositoryError>,
{
    keyset_traversal_returns_each_row_once(backend, &mut factory)?;
    rows_created_after_traversal_start_are_not_returned(backend, &mut factory)?;
    cursor_rejects_a_different_query_or_filter(backend, &mut factory)?;
    corrupt_cursor_checksum_is_rejected(backend, &mut factory)?;
    page_and_response_bounds_are_enforced(backend, &mut factory)?;
    projection_excludes_parameter_and_context_values(backend, &mut factory)?;
    replayed_operation_id_returns_the_recorded_outcome(backend, &mut factory)?;
    operation_id_reuse_with_a_different_digest_is_rejected(backend, &mut factory)?;
    operator_request_and_effect_commit_together(backend, &mut factory)?;
    rejected_action_is_audited_without_an_effect(backend, &mut factory)?;
    stale_expected_version_loses_the_compare_and_swap(backend, &mut factory)?;
    abandon_requires_a_stopped_failed_or_recovered_execution(backend, &mut factory)?;
    repeat_abandon_changes_nothing(backend, &mut factory)?;
    abandoned_execution_rejects_restart(backend, &mut factory)?;
    stop_on_a_stopping_or_terminal_execution_changes_nothing(backend, &mut factory)?;
    held_instance_is_never_purged(backend, &mut factory)?;
    running_stopping_or_unknown_execution_is_never_purged(backend, &mut factory)?;
    stale_plan_digest_rejects_apply_without_deleting(backend, &mut factory)?;
    purge_deletes_in_instance_owned_order_within_batch_bounds(backend, &mut factory)?;
    interrupted_purge_leaves_completed_batches_durable(backend, &mut factory)
}

const JOB: &str = "service_contract_job";
const OTHER_JOB: &str = "service_contract_other_job";
const SENTINEL: &str = "sentinel-parameter-value";

type CaseResult<T> = Result<T, RepositoryContractFailure>;

/// Builds a fresh case-scoped error mapper for any displayable failure.
macro_rules! at {
    ($backend:expr, $case:expr) => {
        |error| RepositoryContractFailure::new($backend, $case, error.to_string())
    };
}

struct Fixture<R, S> {
    repository: R,
    operator: JobOperator<R>,
    retention: RetentionService<R>,
    explorer: JobExplorer<S>,
    clock: Arc<ContractClock>,
}

fn fixture<R, S, F>(
    backend: &'static str,
    case: &'static str,
    factory: &mut F,
) -> CaseResult<Fixture<R, S>>
where
    R: JobRepository + Clone,
    S: ExplorerRepository,
    F: FnMut(Arc<ContractClock>) -> Result<ServiceBackend<R, S>, RepositoryError>,
{
    let clock = Arc::new(ContractClock::new(1_700_000_000_000));
    let backend_under_test = factory(Arc::clone(&clock)).map_err(at!(backend, case))?;
    let repository = backend_under_test.repository;
    Ok(Fixture {
        operator: JobOperator::new(
            repository.clone(),
            Arc::clone(&backend_under_test.clock) as _,
        ),
        retention: RetentionService::new(
            repository.clone(),
            Arc::clone(&backend_under_test.clock) as _,
        ),
        explorer: JobExplorer::new(backend_under_test.explorer),
        clock: backend_under_test.clock,
        repository,
    })
}

fn instance_key(
    job: &str,
    discriminator: &str,
) -> Result<JobInstanceKey, Box<dyn std::error::Error>> {
    let mut parameters = JobParameters::new();
    parameters.insert(
        ParameterName::new("run")?,
        JobParameter::new(
            ParameterValue::string(discriminator)?,
            ParameterRole::Identifying,
        ),
    )?;
    parameters.insert(
        ParameterName::new("secret")?,
        JobParameter::new(
            ParameterValue::string(SENTINEL)?,
            ParameterRole::NonIdentifying,
        ),
    )?;
    Ok(JobInstanceKey::new(JobName::new(job)?, &parameters))
}

fn definition(job: &str) -> Result<DefinitionIdentity, Box<dyn std::error::Error>> {
    Ok(DefinitionIdentity::tasklet(
        &JobName::new(job)?,
        &StepName::new("only")?,
        DefinitionRevision::new("v1")?,
        &ComponentRevision::new("tasklet-1")?,
    )?)
}

fn actor() -> Result<ActorRef, Box<dyn std::error::Error>> {
    Ok(ActorRef::new("operator:contract")?)
}

fn reason() -> Result<ReasonCode, Box<dyn std::error::Error>> {
    Ok(ReasonCode::new("CONTRACT_CASE")?)
}

fn launch<R, S>(
    fixture: &Fixture<R, S>,
    job: &str,
    discriminator: &str,
    operation: &str,
) -> Result<JobExecution, Box<dyn std::error::Error>>
where
    R: JobRepository + Clone,
    S: ExplorerRepository,
{
    let request = OperatorRequest::launch(
        OperationId::new(operation)?,
        actor()?,
        instance_key(job, discriminator)?,
        definition(job)?,
    );
    let outcome = block_on(fixture.operator.execute(&request))?;
    outcome
        .execution()
        .cloned()
        .ok_or_else(|| String::from("launch produced no execution").into())
}

fn transition<R, S>(
    fixture: &Fixture<R, S>,
    execution: &JobExecution,
    target: BatchStatus,
) -> Result<JobExecution, Box<dyn std::error::Error>>
where
    R: JobRepository + Clone,
    S: ExplorerRepository,
{
    let at = fixture.clock.now();
    let requested = if target == BatchStatus::Failed {
        LifecycleTransition::failed(
            at,
            FailureSummary::new(FailureCategory::UserComponent, FailureId::new(1)?),
        )
    } else {
        LifecycleTransition::new(target, at)
    };
    let mut unit = block_on(fixture.repository.begin())?;
    let moved =
        block_on(unit.transition_job_execution(execution.id(), execution.version(), requested))?;
    block_on(unit.commit())?;
    Ok(moved)
}

fn finish<R, S>(
    fixture: &Fixture<R, S>,
    execution: &JobExecution,
    target: BatchStatus,
) -> Result<JobExecution, Box<dyn std::error::Error>>
where
    R: JobRepository + Clone,
    S: ExplorerRepository,
{
    let started = transition(fixture, execution, BatchStatus::Started)?;
    transition(fixture, &started, target)
}

fn keyset_traversal_returns_each_row_once<R, S, F>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository + Clone,
    S: ExplorerRepository,
    F: FnMut(Arc<ContractClock>) -> Result<ServiceBackend<R, S>, RepositoryError>,
{
    const CASE: &str = "keyset_traversal_returns_each_row_once";
    let fixture = fixture(backend, CASE, factory)?;
    let mut expected = Vec::new();
    for index in 0..5 {
        let execution = launch(
            &fixture,
            JOB,
            &format!("traversal-{index}"),
            &format!("traverse-{index}"),
        )
        .map_err(at!(backend, CASE))?;
        expected.push(execution.job_instance_id());
    }
    let observed = traverse_instances(&fixture, JOB, 2).map_err(at!(backend, CASE))?;
    expected.sort_unstable();
    let mut sorted = observed.clone();
    sorted.sort_unstable();
    sorted.dedup();
    ensure(
        sorted == expected,
        backend,
        CASE,
        "traversal did not return every row exactly once",
    )?;
    ensure(
        observed.len() == sorted.len(),
        backend,
        CASE,
        "traversal repeated a row",
    )
}

fn traverse_instances<R, S>(
    fixture: &Fixture<R, S>,
    job: &str,
    page_size: u16,
) -> Result<Vec<JobInstanceId>, Box<dyn std::error::Error>>
where
    R: JobRepository + Clone,
    S: ExplorerRepository,
{
    let name = JobName::new(job)?;
    let size = PageSize::new(page_size)?;
    let mut request = PageRequest::first(size);
    let mut observed = Vec::new();
    loop {
        let page = block_on(fixture.explorer.list_instances(&name, &request))?;
        for row in page.rows() {
            observed.push(row.id());
        }
        match page.next_cursor() {
            Some(cursor) => request = PageRequest::resume(size, cursor.clone()),
            None => break,
        }
    }
    Ok(observed)
}

fn rows_created_after_traversal_start_are_not_returned<R, S, F>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository + Clone,
    S: ExplorerRepository,
    F: FnMut(Arc<ContractClock>) -> Result<ServiceBackend<R, S>, RepositoryError>,
{
    const CASE: &str = "rows_created_after_traversal_start_are_not_returned";
    let fixture = fixture(backend, CASE, factory)?;
    for index in 0..3 {
        launch(
            &fixture,
            JOB,
            &format!("ceiling-{index}"),
            &format!("ceiling-{index}"),
        )
        .map_err(at!(backend, CASE))?;
    }
    let name = JobName::new(JOB).map_err(at!(backend, CASE))?;
    let size = PageSize::new(2).map_err(at!(backend, CASE))?;
    let first = block_on(
        fixture
            .explorer
            .list_instances(&name, &PageRequest::first(size)),
    )
    .map_err(at!(backend, CASE))?;
    let cursor = first
        .next_cursor()
        .cloned()
        .ok_or_else(|| String::from("first page carried no cursor"))
        .map_err(at!(backend, CASE))?;
    let late = launch(&fixture, JOB, "ceiling-late", "ceiling-late").map_err(at!(backend, CASE))?;
    let second = block_on(
        fixture
            .explorer
            .list_instances(&name, &PageRequest::resume(size, cursor)),
    )
    .map_err(at!(backend, CASE))?;
    let returned = second
        .rows()
        .iter()
        .any(|row| row.id() == late.job_instance_id());
    ensure(
        !returned,
        backend,
        CASE,
        "a row created after the traversal started was returned",
    )
}

fn cursor_rejects_a_different_query_or_filter<R, S, F>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository + Clone,
    S: ExplorerRepository,
    F: FnMut(Arc<ContractClock>) -> Result<ServiceBackend<R, S>, RepositoryError>,
{
    const CASE: &str = "cursor_rejects_a_different_query_or_filter";
    let fixture = fixture(backend, CASE, factory)?;
    for index in 0..3 {
        launch(
            &fixture,
            JOB,
            &format!("mismatch-{index}"),
            &format!("mismatch-{index}"),
        )
        .map_err(at!(backend, CASE))?;
    }
    launch(&fixture, OTHER_JOB, "mismatch-other", "mismatch-other").map_err(at!(backend, CASE))?;
    let name = JobName::new(JOB).map_err(at!(backend, CASE))?;
    let other = JobName::new(OTHER_JOB).map_err(at!(backend, CASE))?;
    let size = PageSize::new(2).map_err(at!(backend, CASE))?;
    let page = block_on(
        fixture
            .explorer
            .list_instances(&name, &PageRequest::first(size)),
    )
    .map_err(at!(backend, CASE))?;
    let cursor = page
        .next_cursor()
        .cloned()
        .ok_or_else(|| String::from("first page carried no cursor"))
        .map_err(at!(backend, CASE))?;
    let filtered = block_on(
        fixture
            .explorer
            .list_instances(&other, &PageRequest::resume(size, cursor.clone())),
    );
    ensure(
        matches!(
            filtered,
            Err(ExplorerError::Cursor(CursorError::CursorQueryMismatch))
        ),
        backend,
        CASE,
        "a cursor from another filter was accepted",
    )?;
    let resized = PageSize::new(3).map_err(at!(backend, CASE))?;
    let sized = block_on(
        fixture
            .explorer
            .list_instances(&name, &PageRequest::resume(resized, cursor)),
    );
    ensure(
        matches!(
            sized,
            Err(ExplorerError::Cursor(CursorError::CursorQueryMismatch))
        ),
        backend,
        CASE,
        "a cursor from another page size was accepted",
    )
}

fn corrupt_cursor_checksum_is_rejected<R, S, F>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository + Clone,
    S: ExplorerRepository,
    F: FnMut(Arc<ContractClock>) -> Result<ServiceBackend<R, S>, RepositoryError>,
{
    const CASE: &str = "corrupt_cursor_checksum_is_rejected";
    let fixture = fixture(backend, CASE, factory)?;
    for index in 0..3 {
        launch(
            &fixture,
            JOB,
            &format!("corrupt-{index}"),
            &format!("corrupt-{index}"),
        )
        .map_err(at!(backend, CASE))?;
    }
    let name = JobName::new(JOB).map_err(at!(backend, CASE))?;
    let size = PageSize::new(2).map_err(at!(backend, CASE))?;
    let page = block_on(
        fixture
            .explorer
            .list_instances(&name, &PageRequest::first(size)),
    )
    .map_err(at!(backend, CASE))?;
    let cursor = page
        .next_cursor()
        .ok_or_else(|| String::from("first page carried no cursor"))
        .map_err(at!(backend, CASE))?;
    let mut bytes = cursor.as_bytes().to_vec();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    let corrupt = oxide_batch::Cursor::from_bytes(bytes).map_err(at!(backend, CASE))?;
    let rejected = block_on(
        fixture
            .explorer
            .list_instances(&name, &PageRequest::resume(size, corrupt)),
    );
    ensure(
        matches!(
            rejected,
            Err(ExplorerError::Cursor(CursorError::CursorInvalid))
        ),
        backend,
        CASE,
        "a cursor with a broken checksum was accepted",
    )
}

fn page_and_response_bounds_are_enforced<R, S, F>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository + Clone,
    S: ExplorerRepository,
    F: FnMut(Arc<ContractClock>) -> Result<ServiceBackend<R, S>, RepositoryError>,
{
    const CASE: &str = "page_and_response_bounds_are_enforced";
    let fixture = fixture(backend, CASE, factory)?;
    ensure(
        matches!(
            PageSize::new(0),
            Err(ExplorerError::PageSizeOutOfRange { requested: 0 })
        ),
        backend,
        CASE,
        "a zero page size was accepted",
    )?;
    ensure(
        matches!(
            PageSize::new(oxide_batch::MAX_PAGE_SIZE + 1),
            Err(ExplorerError::PageSizeOutOfRange { .. })
        ),
        backend,
        CASE,
        "an oversized page size was accepted",
    )?;
    for index in 0..3 {
        launch(
            &fixture,
            JOB,
            &format!("bounds-{index}"),
            &format!("bounds-{index}"),
        )
        .map_err(at!(backend, CASE))?;
    }
    let name = JobName::new(JOB).map_err(at!(backend, CASE))?;
    let size = PageSize::new(2).map_err(at!(backend, CASE))?;
    let page = block_on(
        fixture
            .explorer
            .list_instances(&name, &PageRequest::first(size)),
    )
    .map_err(at!(backend, CASE))?;
    ensure(
        page.rows().len() <= 2,
        backend,
        CASE,
        "a page exceeded its requested row bound",
    )
}

fn projection_excludes_parameter_and_context_values<R, S, F>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository + Clone,
    S: ExplorerRepository,
    F: FnMut(Arc<ContractClock>) -> Result<ServiceBackend<R, S>, RepositoryError>,
{
    const CASE: &str = "projection_excludes_parameter_and_context_values";
    let fixture = fixture(backend, CASE, factory)?;
    let execution = launch(&fixture, JOB, SENTINEL, "redaction").map_err(at!(backend, CASE))?;
    let name = JobName::new(JOB).map_err(at!(backend, CASE))?;
    let size = PageSize::new(10).map_err(at!(backend, CASE))?;
    let instances = block_on(
        fixture
            .explorer
            .list_instances(&name, &PageRequest::first(size)),
    )
    .map_err(at!(backend, CASE))?;
    let rendered = format!("{:?}", instances.rows());
    ensure(
        !rendered.contains(SENTINEL),
        backend,
        CASE,
        "an instance projection exposed a parameter value",
    )?;
    let projection = block_on(fixture.explorer.get_execution(execution.id()))
        .map_err(at!(backend, CASE))?
        .ok_or_else(|| String::from("execution projection was absent"))
        .map_err(at!(backend, CASE))?;
    ensure(
        !format!("{projection:?}").contains(SENTINEL),
        backend,
        CASE,
        "an execution projection exposed a parameter value",
    )?;
    let names = instances
        .rows()
        .iter()
        .flat_map(|row| row.parameters().iter())
        .map(|parameter| parameter.name().as_str().to_owned())
        .collect::<Vec<_>>();
    ensure(
        names.iter().all(|value| value != SENTINEL),
        backend,
        CASE,
        "a parameter descriptor carried a value",
    )
}

fn replayed_operation_id_returns_the_recorded_outcome<R, S, F>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository + Clone,
    S: ExplorerRepository,
    F: FnMut(Arc<ContractClock>) -> Result<ServiceBackend<R, S>, RepositoryError>,
{
    const CASE: &str = "replayed_operation_id_returns_the_recorded_outcome";
    let fixture = fixture(backend, CASE, factory)?;
    let request = OperatorRequest::launch(
        OperationId::new("replay-1").map_err(at!(backend, CASE))?,
        actor().map_err(at!(backend, CASE))?,
        instance_key(JOB, "replay").map_err(at!(backend, CASE))?,
        definition(JOB).map_err(at!(backend, CASE))?,
    );
    let first = block_on(fixture.operator.execute(&request)).map_err(at!(backend, CASE))?;
    let second = block_on(fixture.operator.execute(&request)).map_err(at!(backend, CASE))?;
    ensure(
        first.class() == OperatorOutcomeClass::Applied,
        backend,
        CASE,
        "the first launch was not applied",
    )?;
    ensure(
        second.class() == OperatorOutcomeClass::Replayed,
        backend,
        CASE,
        "the replayed operation identifier repeated the effect",
    )?;
    ensure(
        second.record().id() == first.record().id(),
        backend,
        CASE,
        "the replay returned a different audit record",
    )?;
    ensure(
        !second.changed_state(),
        backend,
        CASE,
        "the replay reported a durable change",
    )?;
    let instance = first
        .record()
        .job_instance_id()
        .ok_or_else(|| String::from("the launch recorded no instance"))
        .map_err(at!(backend, CASE))?;
    let size = PageSize::new(10).map_err(at!(backend, CASE))?;
    let executions = block_on(
        fixture
            .explorer
            .list_executions(instance, &PageRequest::first(size)),
    )
    .map_err(at!(backend, CASE))?;
    ensure(
        executions.rows().len() == 1,
        backend,
        CASE,
        "the replay created a second execution",
    )
}

fn operation_id_reuse_with_a_different_digest_is_rejected<R, S, F>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository + Clone,
    S: ExplorerRepository,
    F: FnMut(Arc<ContractClock>) -> Result<ServiceBackend<R, S>, RepositoryError>,
{
    const CASE: &str = "operation_id_reuse_with_a_different_digest_is_rejected";
    let fixture = fixture(backend, CASE, factory)?;
    launch(&fixture, JOB, "conflict-a", "conflict").map_err(at!(backend, CASE))?;
    let other = OperatorRequest::launch(
        OperationId::new("conflict").map_err(at!(backend, CASE))?,
        actor().map_err(at!(backend, CASE))?,
        instance_key(JOB, "conflict-b").map_err(at!(backend, CASE))?,
        definition(JOB).map_err(at!(backend, CASE))?,
    );
    let rejected = block_on(fixture.operator.execute(&other));
    ensure(
        matches!(rejected, Err(OperatorError::OperationIdConflict { .. })),
        backend,
        CASE,
        "a reused operation identifier with a different request was accepted",
    )
}

fn operator_request_and_effect_commit_together<R, S, F>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository + Clone,
    S: ExplorerRepository,
    F: FnMut(Arc<ContractClock>) -> Result<ServiceBackend<R, S>, RepositoryError>,
{
    const CASE: &str = "operator_request_and_effect_commit_together";
    let fixture = fixture(backend, CASE, factory)?;
    let execution = launch(&fixture, JOB, "audited", "audited").map_err(at!(backend, CASE))?;
    let size = PageSize::new(10).map_err(at!(backend, CASE))?;
    let audit = block_on(
        fixture
            .explorer
            .list_operator_requests(execution.id(), &PageRequest::first(size)),
    )
    .map_err(at!(backend, CASE))?;
    ensure(
        audit.rows().len() == 1,
        backend,
        CASE,
        "the applied effect did not commit exactly one audit row",
    )?;
    let record = audit
        .rows()
        .first()
        .ok_or_else(|| String::from("audit row was absent"))
        .map_err(at!(backend, CASE))?;
    ensure(
        record.outcome() == OperatorOutcomeClass::Applied
            && record.job_execution_id() == Some(execution.id())
            && record.result_status() == Some(BatchStatus::Starting),
        backend,
        CASE,
        "the audit row did not describe its effect",
    )
}

fn rejected_action_is_audited_without_an_effect<R, S, F>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository + Clone,
    S: ExplorerRepository,
    F: FnMut(Arc<ContractClock>) -> Result<ServiceBackend<R, S>, RepositoryError>,
{
    const CASE: &str = "rejected_action_is_audited_without_an_effect";
    let fixture = fixture(backend, CASE, factory)?;
    let execution =
        launch(&fixture, JOB, "rejected", "rejected-launch").map_err(at!(backend, CASE))?;
    let request = OperatorRequest::abandon(
        OperationId::new("rejected-abandon").map_err(at!(backend, CASE))?,
        actor().map_err(at!(backend, CASE))?,
        reason().map_err(at!(backend, CASE))?,
        execution.id(),
        execution.version(),
    );
    let outcome = block_on(fixture.operator.execute(&request)).map_err(at!(backend, CASE))?;
    ensure(
        outcome.class() == OperatorOutcomeClass::Rejected
            && matches!(
                outcome.rejection(),
                Some(OperatorRejection::InvalidState {
                    status: BatchStatus::Starting
                })
            ),
        backend,
        CASE,
        "abandoning a starting execution was not rejected",
    )?;
    let observed = block_on(fixture.explorer.get_execution(execution.id()))
        .map_err(at!(backend, CASE))?
        .ok_or_else(|| String::from("execution projection was absent"))
        .map_err(at!(backend, CASE))?;
    ensure(
        observed.status() == BatchStatus::Starting && observed.version() == execution.version(),
        backend,
        CASE,
        "a rejected action changed durable state",
    )?;
    let size = PageSize::new(10).map_err(at!(backend, CASE))?;
    let audit = block_on(
        fixture
            .explorer
            .list_operator_requests(execution.id(), &PageRequest::first(size)),
    )
    .map_err(at!(backend, CASE))?;
    ensure(
        audit
            .rows()
            .iter()
            .any(|record| record.outcome() == OperatorOutcomeClass::Rejected),
        backend,
        CASE,
        "a rejected action was not audited",
    )
}

fn stale_expected_version_loses_the_compare_and_swap<R, S, F>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository + Clone,
    S: ExplorerRepository,
    F: FnMut(Arc<ContractClock>) -> Result<ServiceBackend<R, S>, RepositoryError>,
{
    const CASE: &str = "stale_expected_version_loses_the_compare_and_swap";
    let fixture = fixture(backend, CASE, factory)?;
    let execution = launch(&fixture, JOB, "stale", "stale-launch").map_err(at!(backend, CASE))?;
    let failed = finish(&fixture, &execution, BatchStatus::Failed).map_err(at!(backend, CASE))?;
    let request = OperatorRequest::abandon(
        OperationId::new("stale-abandon").map_err(at!(backend, CASE))?,
        actor().map_err(at!(backend, CASE))?,
        reason().map_err(at!(backend, CASE))?,
        failed.id(),
        ExecutionVersion::new(failed.version().get().saturating_sub(1)),
    );
    let outcome = block_on(fixture.operator.execute(&request)).map_err(at!(backend, CASE))?;
    ensure(
        matches!(
            outcome.rejection(),
            Some(OperatorRejection::OptimisticConflict { .. })
        ),
        backend,
        CASE,
        "a stale expected version won its compare-and-swap",
    )?;
    let observed = block_on(fixture.explorer.get_execution(failed.id()))
        .map_err(at!(backend, CASE))?
        .ok_or_else(|| String::from("execution projection was absent"))
        .map_err(at!(backend, CASE))?;
    ensure(
        observed.status() == BatchStatus::Failed,
        backend,
        CASE,
        "a losing compare-and-swap changed the execution",
    )
}

fn abandon_requires_a_stopped_failed_or_recovered_execution<R, S, F>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository + Clone,
    S: ExplorerRepository,
    F: FnMut(Arc<ContractClock>) -> Result<ServiceBackend<R, S>, RepositoryError>,
{
    const CASE: &str = "abandon_requires_a_stopped_failed_or_recovered_execution";
    let fixture = fixture(backend, CASE, factory)?;
    let running =
        launch(&fixture, JOB, "abandon-running", "abandon-running").map_err(at!(backend, CASE))?;
    let started =
        transition(&fixture, &running, BatchStatus::Started).map_err(at!(backend, CASE))?;
    let rejected = abandon(&fixture, &started, "abandon-active").map_err(at!(backend, CASE))?;
    ensure(
        rejected.class() == OperatorOutcomeClass::Rejected,
        backend,
        CASE,
        "an active execution was abandoned",
    )?;
    let stoppable =
        launch(&fixture, JOB, "abandon-stopped", "abandon-stopped").map_err(at!(backend, CASE))?;
    let stopped = finish(&fixture, &stoppable, BatchStatus::Stopped).map_err(at!(backend, CASE))?;
    let accepted = abandon(&fixture, &stopped, "abandon-ok").map_err(at!(backend, CASE))?;
    ensure(
        accepted.class() == OperatorOutcomeClass::Applied
            && accepted.record().prior_status() == Some(BatchStatus::Stopped)
            && accepted.record().result_status() == Some(BatchStatus::Abandoned)
            && accepted.record().actor().as_str() == actor().map_err(at!(backend, CASE))?.as_str()
            && accepted.record().reason().map(ReasonCode::as_str) == Some("CONTRACT_CASE"),
        backend,
        CASE,
        "abandon did not record actor, reason, and prior state",
    )
}

fn abandon<R, S>(
    fixture: &Fixture<R, S>,
    execution: &JobExecution,
    operation: &str,
) -> Result<oxide_batch::OperatorOutcome, Box<dyn std::error::Error>>
where
    R: JobRepository + Clone,
    S: ExplorerRepository,
{
    let request = OperatorRequest::abandon(
        OperationId::new(operation)?,
        actor()?,
        reason()?,
        execution.id(),
        execution.version(),
    );
    Ok(block_on(fixture.operator.execute(&request))?)
}

fn repeat_abandon_changes_nothing<R, S, F>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository + Clone,
    S: ExplorerRepository,
    F: FnMut(Arc<ContractClock>) -> Result<ServiceBackend<R, S>, RepositoryError>,
{
    const CASE: &str = "repeat_abandon_changes_nothing";
    let fixture = fixture(backend, CASE, factory)?;
    let execution = launch(&fixture, JOB, "repeat", "repeat-launch").map_err(at!(backend, CASE))?;
    let failed = finish(&fixture, &execution, BatchStatus::Failed).map_err(at!(backend, CASE))?;
    let first = abandon(&fixture, &failed, "repeat-abandon-1").map_err(at!(backend, CASE))?;
    let abandoned = first
        .execution()
        .cloned()
        .ok_or_else(|| String::from("abandon returned no execution"))
        .map_err(at!(backend, CASE))?;
    let second = abandon(&fixture, &abandoned, "repeat-abandon-2").map_err(at!(backend, CASE))?;
    ensure(
        second.class() == OperatorOutcomeClass::Applied && !second.changed_state(),
        backend,
        CASE,
        "a repeated abandon changed durable state",
    )?;
    let observed = block_on(fixture.explorer.get_execution(abandoned.id()))
        .map_err(at!(backend, CASE))?
        .ok_or_else(|| String::from("execution projection was absent"))
        .map_err(at!(backend, CASE))?;
    ensure(
        observed.version() == abandoned.version(),
        backend,
        CASE,
        "a repeated abandon advanced the optimistic version",
    )
}

fn abandoned_execution_rejects_restart<R, S, F>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository + Clone,
    S: ExplorerRepository,
    F: FnMut(Arc<ContractClock>) -> Result<ServiceBackend<R, S>, RepositoryError>,
{
    const CASE: &str = "abandoned_execution_rejects_restart";
    let fixture = fixture(backend, CASE, factory)?;
    let execution =
        launch(&fixture, JOB, "no-restart", "no-restart-launch").map_err(at!(backend, CASE))?;
    let failed = finish(&fixture, &execution, BatchStatus::Failed).map_err(at!(backend, CASE))?;
    abandon(&fixture, &failed, "no-restart-abandon").map_err(at!(backend, CASE))?;
    let restart = OperatorRequest::restart(
        OperationId::new("no-restart-restart").map_err(at!(backend, CASE))?,
        actor().map_err(at!(backend, CASE))?,
        failed.job_instance_id(),
        definition(JOB).map_err(at!(backend, CASE))?,
    );
    let outcome = block_on(fixture.operator.execute(&restart)).map_err(at!(backend, CASE))?;
    ensure(
        outcome.class() == OperatorOutcomeClass::Rejected,
        backend,
        CASE,
        "an abandoned execution accepted a restart",
    )
}

fn stop_on_a_stopping_or_terminal_execution_changes_nothing<R, S, F>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository + Clone,
    S: ExplorerRepository,
    F: FnMut(Arc<ContractClock>) -> Result<ServiceBackend<R, S>, RepositoryError>,
{
    const CASE: &str = "stop_on_a_stopping_or_terminal_execution_changes_nothing";
    let fixture = fixture(backend, CASE, factory)?;
    let execution = launch(&fixture, JOB, "stop", "stop-launch").map_err(at!(backend, CASE))?;
    let started =
        transition(&fixture, &execution, BatchStatus::Started).map_err(at!(backend, CASE))?;
    let stopping =
        transition(&fixture, &started, BatchStatus::Stopping).map_err(at!(backend, CASE))?;
    let request = OperatorRequest::stop(
        OperationId::new("stop-repeat").map_err(at!(backend, CASE))?,
        actor().map_err(at!(backend, CASE))?,
        stopping.id(),
        stopping.version(),
    );
    let outcome = block_on(fixture.operator.execute(&request)).map_err(at!(backend, CASE))?;
    ensure(
        outcome.class() == OperatorOutcomeClass::Applied && !outcome.changed_state(),
        backend,
        CASE,
        "a stop request on a stopping execution changed durable state",
    )?;
    let observed = block_on(fixture.explorer.get_execution(stopping.id()))
        .map_err(at!(backend, CASE))?
        .ok_or_else(|| String::from("execution projection was absent"))
        .map_err(at!(backend, CASE))?;
    ensure(
        observed.status() == BatchStatus::Stopping
            && observed.version() == stopping.version()
            && observed.stop_requested_at().is_none(),
        backend,
        CASE,
        "a repeated stop recorded a durable request",
    )
}

fn purge_request(job: &str) -> Result<PurgePlanRequest, Box<dyn std::error::Error>> {
    Ok(PurgePlanRequest::new(
        JobName::new(job)?,
        TerminalStatusSet::all(),
        Duration::from_hours(1),
        PurgeBatchBound::new(10)?,
    )?)
}

fn held_instance_is_never_purged<R, S, F>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository + Clone,
    S: ExplorerRepository,
    F: FnMut(Arc<ContractClock>) -> Result<ServiceBackend<R, S>, RepositoryError>,
{
    const CASE: &str = "held_instance_is_never_purged";
    let fixture = fixture(backend, CASE, factory)?;
    let execution = launch(&fixture, JOB, "held", "held-launch").map_err(at!(backend, CASE))?;
    finish(&fixture, &execution, BatchStatus::Completed).map_err(at!(backend, CASE))?;
    let report = block_on(fixture.retention.place_hold(
        OperationId::new("held-hold").map_err(at!(backend, CASE))?,
        actor().map_err(at!(backend, CASE))?,
        reason().map_err(at!(backend, CASE))?,
        execution.job_instance_id(),
    ))
    .map_err(at!(backend, CASE))?;
    ensure(
        report.outcome() == RetentionOutcome::Applied && report.hold().is_some(),
        backend,
        CASE,
        "the hold was not applied",
    )?;
    fixture.clock.advance(Duration::from_hours(2));
    let plan = block_on(
        fixture
            .retention
            .plan_purge(&purge_request(JOB).map_err(at!(backend, CASE))?),
    )
    .map_err(at!(backend, CASE))?;
    ensure(
        plan.candidates()
            .iter()
            .all(|candidate| candidate.job_instance_id() != execution.job_instance_id()),
        backend,
        CASE,
        "a held instance was planned for purge",
    )
}

fn running_stopping_or_unknown_execution_is_never_purged<R, S, F>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository + Clone,
    S: ExplorerRepository,
    F: FnMut(Arc<ContractClock>) -> Result<ServiceBackend<R, S>, RepositoryError>,
{
    const CASE: &str = "running_stopping_or_unknown_execution_is_never_purged";
    let fixture = fixture(backend, CASE, factory)?;
    let execution = launch(&fixture, JOB, "active", "active-launch").map_err(at!(backend, CASE))?;
    transition(&fixture, &execution, BatchStatus::Started).map_err(at!(backend, CASE))?;
    fixture.clock.advance(Duration::from_hours(2));
    let plan = block_on(
        fixture
            .retention
            .plan_purge(&purge_request(JOB).map_err(at!(backend, CASE))?),
    )
    .map_err(at!(backend, CASE))?;
    ensure(
        plan.is_empty(),
        backend,
        CASE,
        "an unresolved instance was planned for purge",
    )
}

fn stale_plan_digest_rejects_apply_without_deleting<R, S, F>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository + Clone,
    S: ExplorerRepository,
    F: FnMut(Arc<ContractClock>) -> Result<ServiceBackend<R, S>, RepositoryError>,
{
    const CASE: &str = "stale_plan_digest_rejects_apply_without_deleting";
    let fixture = fixture(backend, CASE, factory)?;
    let execution =
        launch(&fixture, JOB, "stale-plan", "stale-plan-launch").map_err(at!(backend, CASE))?;
    let failed = finish(&fixture, &execution, BatchStatus::Failed).map_err(at!(backend, CASE))?;
    fixture.clock.advance(Duration::from_hours(2));
    let plan = block_on(
        fixture
            .retention
            .plan_purge(&purge_request(JOB).map_err(at!(backend, CASE))?),
    )
    .map_err(at!(backend, CASE))?;
    ensure(
        !plan.is_empty(),
        backend,
        CASE,
        "the plan observed no candidate",
    )?;
    abandon(&fixture, &failed, "stale-plan-abandon").map_err(at!(backend, CASE))?;
    let applied = block_on(fixture.retention.apply_purge(
        OperationId::new("stale-plan-apply").map_err(at!(backend, CASE))?,
        actor().map_err(at!(backend, CASE))?,
        reason().map_err(at!(backend, CASE))?,
        &plan,
    ));
    ensure(
        matches!(
            applied,
            Err(oxide_batch::RetentionError::RetentionPlanStale)
        ),
        backend,
        CASE,
        "a stale plan was applied",
    )?;
    let observed =
        block_on(fixture.explorer.get_execution(failed.id())).map_err(at!(backend, CASE))?;
    ensure(
        observed.is_some(),
        backend,
        CASE,
        "a stale plan deleted an execution",
    )
}

fn purge_deletes_in_instance_owned_order_within_batch_bounds<R, S, F>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository + Clone,
    S: ExplorerRepository,
    F: FnMut(Arc<ContractClock>) -> Result<ServiceBackend<R, S>, RepositoryError>,
{
    const CASE: &str = "purge_deletes_in_instance_owned_order_within_batch_bounds";
    let fixture = fixture(backend, CASE, factory)?;
    let mut executions = Vec::new();
    for index in 0..2 {
        let execution = launch(
            &fixture,
            JOB,
            &format!("purge-{index}"),
            &format!("purge-{index}"),
        )
        .map_err(at!(backend, CASE))?;
        finish(&fixture, &execution, BatchStatus::Completed).map_err(at!(backend, CASE))?;
        executions.push(execution);
    }
    fixture.clock.advance(Duration::from_hours(2));
    let plan = block_on(
        fixture
            .retention
            .plan_purge(&purge_request(JOB).map_err(at!(backend, CASE))?),
    )
    .map_err(at!(backend, CASE))?;
    ensure(
        plan.candidates().len() == 2 && plan.counts().job_executions() == 2,
        backend,
        CASE,
        "the plan did not observe both candidates",
    )?;
    let report = block_on(fixture.retention.apply_purge(
        OperationId::new("purge-apply").map_err(at!(backend, CASE))?,
        actor().map_err(at!(backend, CASE))?,
        reason().map_err(at!(backend, CASE))?,
        &plan,
    ))
    .map_err(at!(backend, CASE))?;
    ensure(
        report.outcome() == RetentionOutcome::Applied
            && report.counts().job_executions() == 2
            && report.counts().job_instances() == 2
            && report.record().plan_digest() == Some(plan.digest()),
        backend,
        CASE,
        "the applied batch did not delete its planned rows",
    )?;
    for execution in &executions {
        let observed =
            block_on(fixture.explorer.get_execution(execution.id())).map_err(at!(backend, CASE))?;
        ensure(
            observed.is_none(),
            backend,
            CASE,
            "a purged execution remained readable",
        )?;
    }
    Ok(())
}

fn interrupted_purge_leaves_completed_batches_durable<R, S, F>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository + Clone,
    S: ExplorerRepository,
    F: FnMut(Arc<ContractClock>) -> Result<ServiceBackend<R, S>, RepositoryError>,
{
    const CASE: &str = "interrupted_purge_leaves_completed_batches_durable";
    let fixture = fixture(backend, CASE, factory)?;
    for index in 0..2 {
        let execution = launch(
            &fixture,
            JOB,
            &format!("batch-{index}"),
            &format!("batch-{index}"),
        )
        .map_err(at!(backend, CASE))?;
        finish(&fixture, &execution, BatchStatus::Completed).map_err(at!(backend, CASE))?;
    }
    fixture.clock.advance(Duration::from_hours(2));
    let bounded = PurgePlanRequest::new(
        JobName::new(JOB).map_err(at!(backend, CASE))?,
        TerminalStatusSet::all(),
        Duration::from_hours(1),
        PurgeBatchBound::new(1).map_err(at!(backend, CASE))?,
    )
    .map_err(at!(backend, CASE))?;
    let first = block_on(fixture.retention.plan_purge(&bounded)).map_err(at!(backend, CASE))?;
    ensure(
        first.candidates().len() == 1,
        backend,
        CASE,
        "the batch bound did not limit the plan",
    )?;
    let purged = first
        .candidates()
        .first()
        .map(PurgeCandidate::job_execution_id)
        .ok_or_else(|| String::from("the plan observed no candidate"))
        .map_err(at!(backend, CASE))?;
    block_on(fixture.retention.apply_purge(
        OperationId::new("batch-apply-1").map_err(at!(backend, CASE))?,
        actor().map_err(at!(backend, CASE))?,
        reason().map_err(at!(backend, CASE))?,
        &first,
    ))
    .map_err(at!(backend, CASE))?;
    let second = block_on(fixture.retention.plan_purge(&bounded)).map_err(at!(backend, CASE))?;
    let remaining = second
        .candidates()
        .first()
        .map(PurgeCandidate::job_execution_id)
        .ok_or_else(|| String::from("the second plan observed no candidate"))
        .map_err(at!(backend, CASE))?;
    ensure(
        remaining != purged,
        backend,
        CASE,
        "a completed batch was replanned",
    )?;
    let gone: Option<JobExecutionId> = block_on(fixture.explorer.get_execution(purged))
        .map_err(at!(backend, CASE))?
        .map(|projection| projection.id());
    ensure(
        gone.is_none(),
        backend,
        CASE,
        "an interrupted run rolled a completed batch back",
    )
}
