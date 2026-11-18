//! Portable checkpoint/context restart certification.
//!
//! This profile is separate from runnable-core and optional capability groups.
//! It proves that a durable adapter treats one committed chunk receipt as the
//! restart authority for checkpoint identity, execution context, and counters.

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use oxide_batch::{
    BatchStatus, Checkpoint, ChunkCount, ChunkCounts, ChunkFaultProgress, ChunkTransactionContext,
    ChunkTransactionError, ChunkTransactionManager, Clock, ExecutionContext, ExitCode, ExitStatus,
    FailureCategory, FailureId, FailureSummary, JobInstanceId, JobInstanceKey, JobName,
    JobParameters, JobRepository, LifecycleTransition, NodeId, RepositoryError, StateLimits,
    StepExecutionId, StepName,
};

use super::{ContractClock, RepositoryContractFailure, contract_failure, ensure};

const CONTRACT_CLOCK_MILLIS: u64 = 30_000;
const CONTRACT_JOB_NAME: &str = "repository_checkpoint_context_contract_job";
const CONTRACT_STEP_NAME: &str = "stateful";
const STATE_SENTINEL: &str = "checkpoint-context-contract-sensitive";

/// One repository and chunk transaction manager under checkpoint/context certification.
///
/// The runner owns the canonical checkpoint/context fixture and supplies it to
/// the backend factory. The concrete adapter decides how its transaction manager
/// obtains those values, but must persist and recover them through the public
/// repository and chunk transaction ports.
pub struct CheckpointContextBackend<R, M> {
    /// The metadata repository under test.
    pub repository: R,
    /// The adapter-owned durable chunk transaction manager under test.
    pub transactions: M,
}

/// Runs the portable durable checkpoint/context restart contract.
///
/// This profile is additive to runnable-core. It requires no optional repository
/// capability, no `ItemStream`/component-state support, and no same-resource
/// business transaction. A backend may return `None` from
/// `ChunkTransaction::business_transaction` and still satisfy this contract.
///
/// The factory receives the deterministic repository clock plus the exact
/// checkpoint and execution-context envelopes that the test transaction manager
/// must publish when commit succeeds.
///
/// # Errors
///
/// Returns [`RepositoryContractFailure`] when committed state is not authoritative
/// across restart, rollback changes durable state, or a stale optimistic commit
/// is not rejected as known-not-committed with redacted diagnostics.
pub async fn run_checkpoint_context_restart_contract<R, M, F, Fut>(
    backend: &'static str,
    mut factory: F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    M: ChunkTransactionManager,
    F: FnMut(Arc<ContractClock>, Checkpoint, ExecutionContext) -> Fut,
    Fut: Future<Output = Result<CheckpointContextBackend<R, M>, RepositoryError>>,
{
    committed_state_is_restart_authority(backend, &mut factory).await?;
    explicit_rollback_preserves_durable_state(backend, &mut factory).await?;
    stale_chunk_commit_is_known_not_committed(backend, &mut factory).await
}

#[derive(Clone)]
struct StartedStep {
    instance_id: JobInstanceId,
    node_id: NodeId,
    scope: ChunkTransactionContext,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ObservedState {
    step_execution_id: StepExecutionId,
    read_count: u64,
    context: Option<ExecutionContext>,
}

fn stage_failure(
    backend: &'static str,
    case: &'static str,
    stage: &'static str,
    error: impl fmt::Display,
) -> RepositoryContractFailure {
    contract_failure(backend, case, format!("{stage}: {error}"))
}

fn contract_state(
    backend: &'static str,
    case: &'static str,
) -> Result<(Checkpoint, ExecutionContext), RepositoryContractFailure> {
    let checkpoint_json = serde_json::to_vec(&serde_json::json!({
        "format": "oxide-batch.checkpoint",
        "format_version": 1,
        "schema": "contract.checkpoint",
        "schema_version": 1,
        "payload": {
            "position": 2,
            "sentinel": STATE_SENTINEL,
        },
    }))
    .map_err(|error| stage_failure(backend, case, "checkpoint-json", error))?;
    let checkpoint = Checkpoint::from_json(&checkpoint_json, StateLimits::default())
        .map_err(|error| stage_failure(backend, case, "checkpoint-envelope", error))?;

    let context_json = serde_json::to_vec(&serde_json::json!({
        "format": "oxide-batch.execution-context",
        "format_version": 1,
        "schema": "contract.context",
        "schema_version": 1,
        "payload": {
            "source": "portable-certification",
            "sentinel": STATE_SENTINEL,
        },
    }))
    .map_err(|error| stage_failure(backend, case, "context-json", error))?;
    let context = ExecutionContext::from_json(&context_json, StateLimits::default())
        .map_err(|error| stage_failure(backend, case, "context-envelope", error))?;

    Ok((checkpoint, context))
}

fn contract_counts(
    backend: &'static str,
    case: &'static str,
) -> Result<ChunkCounts, RepositoryContractFailure> {
    ChunkCounts::new(
        ChunkCount::new(2),
        ChunkCount::new(2),
        ChunkCount::new(2),
        ChunkCount::ZERO,
    )
    .map_err(|error| stage_failure(backend, case, "chunk-counts", error))
}

async fn backend_for_case<R, M, F, Fut>(
    backend: &'static str,
    case: &'static str,
    factory: &mut F,
) -> Result<
    (
        CheckpointContextBackend<R, M>,
        Arc<ContractClock>,
        Checkpoint,
        ExecutionContext,
    ),
    RepositoryContractFailure,
>
where
    R: JobRepository,
    M: ChunkTransactionManager,
    F: FnMut(Arc<ContractClock>, Checkpoint, ExecutionContext) -> Fut,
    Fut: Future<Output = Result<CheckpointContextBackend<R, M>, RepositoryError>>,
{
    let clock = Arc::new(ContractClock::new(CONTRACT_CLOCK_MILLIS));
    let (checkpoint, context) = contract_state(backend, case)?;
    let fixture = factory(Arc::clone(&clock), checkpoint.clone(), context.clone())
        .await
        .map_err(|error| stage_failure(backend, case, "factory", error))?;
    Ok((fixture, clock, checkpoint, context))
}

async fn create_started_step<R>(
    backend: &'static str,
    case: &'static str,
    repository: &R,
    clock: &ContractClock,
) -> Result<StartedStep, RepositoryContractFailure>
where
    R: JobRepository,
{
    let job_name = JobName::new(CONTRACT_JOB_NAME)
        .map_err(|error| stage_failure(backend, case, "job-name", error))?;
    let step_name = StepName::new(CONTRACT_STEP_NAME)
        .map_err(|error| stage_failure(backend, case, "step-name", error))?;
    let node_id = NodeId::new(CONTRACT_STEP_NAME)
        .map_err(|error| stage_failure(backend, case, "node-id", error))?;
    let parameters = JobParameters::new();
    let key = JobInstanceKey::new(job_name, &parameters);

    let mut create = repository
        .begin()
        .await
        .map_err(|error| stage_failure(backend, case, "create-begin", error))?;
    let instance = create
        .select_or_create_job_instance(&key)
        .await
        .map_err(|error| stage_failure(backend, case, "select-instance", error))?
        .instance()
        .clone();
    let execution = create
        .create_job_execution(instance.id())
        .await
        .map_err(|error| stage_failure(backend, case, "create-job-execution", error))?;
    let step = create
        .create_step_execution(execution.id(), &step_name)
        .await
        .map_err(|error| stage_failure(backend, case, "create-step-execution", error))?;
    create
        .commit()
        .await
        .map_err(|error| stage_failure(backend, case, "create-commit", error))?;

    clock.advance(Duration::from_secs(1));
    let started_at = clock.now();
    let mut start = repository
        .begin()
        .await
        .map_err(|error| stage_failure(backend, case, "start-begin", error))?;
    start
        .transition_job_execution(
            execution.id(),
            execution.version(),
            LifecycleTransition::new(BatchStatus::Started, started_at),
        )
        .await
        .map_err(|error| stage_failure(backend, case, "start-job", error))?;
    start
        .transition_step_execution(
            step.id(),
            step.version(),
            LifecycleTransition::new(BatchStatus::Started, started_at),
        )
        .await
        .map_err(|error| stage_failure(backend, case, "start-step", error))?;
    start
        .commit()
        .await
        .map_err(|error| stage_failure(backend, case, "start-commit", error))?;

    Ok(StartedStep {
        instance_id: instance.id(),
        node_id,
        scope: ChunkTransactionContext::new(execution.id(), step.id()),
    })
}

async fn mark_attempt_failed<R>(
    backend: &'static str,
    case: &'static str,
    repository: &R,
    clock: &ContractClock,
    scope: ChunkTransactionContext,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
{
    clock.advance(Duration::from_secs(1));
    let failed_at = clock.now();
    let mut unit = repository
        .begin()
        .await
        .map_err(|error| stage_failure(backend, case, "fail-begin", error))?;
    let job = unit
        .get_job_execution(scope.job_execution_id())
        .await
        .map_err(|error| stage_failure(backend, case, "load-job-before-fail", error))?
        .ok_or_else(|| contract_failure(backend, case, "job execution missing before fail"))?;
    let step = unit
        .get_step_execution(scope.step_execution_id())
        .await
        .map_err(|error| stage_failure(backend, case, "load-step-before-fail", error))?
        .ok_or_else(|| contract_failure(backend, case, "step execution missing before fail"))?;

    let step_failure = FailureSummary::new(
        FailureCategory::UserComponent,
        FailureId::new(1)
            .map_err(|error| stage_failure(backend, case, "step-failure-id", error))?,
    );
    let job_failure = FailureSummary::new(
        FailureCategory::UserComponent,
        FailureId::new(2).map_err(|error| stage_failure(backend, case, "job-failure-id", error))?,
    );
    unit.transition_step_execution(
        step.id(),
        step.version(),
        LifecycleTransition::failed(failed_at, step_failure),
    )
    .await
    .map_err(|error| stage_failure(backend, case, "fail-step", error))?;
    unit.transition_job_execution(
        job.id(),
        job.version(),
        LifecycleTransition::failed(failed_at, job_failure),
    )
    .await
    .map_err(|error| stage_failure(backend, case, "fail-job", error))?;
    unit.commit()
        .await
        .map_err(|error| stage_failure(backend, case, "fail-commit", error))
}

async fn create_restart_step<R>(
    backend: &'static str,
    case: &'static str,
    repository: &R,
    clock: &ContractClock,
    instance_id: JobInstanceId,
) -> Result<ChunkTransactionContext, RepositoryContractFailure>
where
    R: JobRepository,
{
    let step_name = StepName::new(CONTRACT_STEP_NAME)
        .map_err(|error| stage_failure(backend, case, "restart-step-name", error))?;
    let mut create = repository
        .begin()
        .await
        .map_err(|error| stage_failure(backend, case, "restart-create-begin", error))?;
    let execution = create
        .create_job_execution(instance_id)
        .await
        .map_err(|error| stage_failure(backend, case, "restart-create-job", error))?;
    let step = create
        .create_step_execution(execution.id(), &step_name)
        .await
        .map_err(|error| stage_failure(backend, case, "restart-create-step", error))?;
    create
        .commit()
        .await
        .map_err(|error| stage_failure(backend, case, "restart-create-commit", error))?;

    clock.advance(Duration::from_secs(1));
    let started_at = clock.now();
    let mut start = repository
        .begin()
        .await
        .map_err(|error| stage_failure(backend, case, "restart-start-begin", error))?;
    start
        .transition_job_execution(
            execution.id(),
            execution.version(),
            LifecycleTransition::new(BatchStatus::Started, started_at),
        )
        .await
        .map_err(|error| stage_failure(backend, case, "restart-start-job", error))?;
    start
        .transition_step_execution(
            step.id(),
            step.version(),
            LifecycleTransition::new(BatchStatus::Started, started_at),
        )
        .await
        .map_err(|error| stage_failure(backend, case, "restart-start-step", error))?;
    start
        .commit()
        .await
        .map_err(|error| stage_failure(backend, case, "restart-start-commit", error))?;

    Ok(ChunkTransactionContext::new(execution.id(), step.id()))
}

async fn observe_state<R>(
    backend: &'static str,
    case: &'static str,
    repository: &R,
    instance_id: JobInstanceId,
    node_id: &NodeId,
) -> Result<ObservedState, RepositoryContractFailure>
where
    R: JobRepository,
{
    let mut unit = repository
        .begin()
        .await
        .map_err(|error| stage_failure(backend, case, "observe-begin", error))?;
    let state = unit
        .latest_flow_step(instance_id, node_id)
        .await
        .map_err(|error| stage_failure(backend, case, "observe-latest-step", error))?
        .ok_or_else(|| contract_failure(backend, case, "latest flow step missing"))?;
    let observed = ObservedState {
        step_execution_id: state.execution().id(),
        read_count: state.execution().metadata().counts().read(),
        context: state.context().cloned(),
    };
    unit.rollback()
        .await
        .map_err(|error| stage_failure(backend, case, "observe-rollback", error))?;
    Ok(observed)
}

#[allow(
    clippy::too_many_lines,
    reason = "commit and restart inheritance form one contract scenario"
)]
async fn committed_state_is_restart_authority<R, M, F, Fut>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    M: ChunkTransactionManager,
    F: FnMut(Arc<ContractClock>, Checkpoint, ExecutionContext) -> Fut,
    Fut: Future<Output = Result<CheckpointContextBackend<R, M>, RepositoryError>>,
{
    const CASE: &str = "checkpoint_context_committed_state_is_restart_authority";
    let (fixture, clock, expected_checkpoint, expected_context) =
        backend_for_case(backend, CASE, factory).await?;
    let started = create_started_step(backend, CASE, &fixture.repository, clock.as_ref()).await?;
    let counts = contract_counts(backend, CASE)?;

    let mut transaction = fixture
        .transactions
        .begin_for(started.scope)
        .await
        .map_err(|error| stage_failure(backend, CASE, "chunk-begin", error))?;
    let receipt = transaction
        .commit(counts, ChunkFaultProgress::NONE)
        .await
        .map_err(|error| stage_failure(backend, CASE, "chunk-commit", error))?;
    ensure(
        receipt.checkpoint() == &expected_checkpoint
            && receipt.execution_context() == &expected_context,
        backend,
        CASE,
        "commit receipt did not preserve the supplied checkpoint/context envelopes",
    )?;

    let inherited = fixture
        .transactions
        .inherited_progress(started.scope)
        .await
        .map_err(|error| stage_failure(backend, CASE, "first-inherited-progress", error))?;
    let observed = observe_state(
        backend,
        CASE,
        &fixture.repository,
        started.instance_id,
        &started.node_id,
    )
    .await?;
    ensure(
        inherited.read_ordinal() == 2
            && inherited.checkpoint_digest() == expected_checkpoint.generation_digest()
            && observed.read_count == 2
            && observed.context.as_ref() == Some(&expected_context),
        backend,
        CASE,
        "committed checkpoint/context/counters were not observable as durable authority",
    )?;

    mark_attempt_failed(
        backend,
        CASE,
        &fixture.repository,
        clock.as_ref(),
        started.scope,
    )
    .await?;
    let restarted = create_restart_step(
        backend,
        CASE,
        &fixture.repository,
        clock.as_ref(),
        started.instance_id,
    )
    .await?;
    let restarted_progress = fixture
        .transactions
        .inherited_progress(restarted)
        .await
        .map_err(|error| stage_failure(backend, CASE, "restart-inherited-progress", error))?;
    let restarted_state = observe_state(
        backend,
        CASE,
        &fixture.repository,
        started.instance_id,
        &started.node_id,
    )
    .await?;
    ensure(
        restarted.step_execution_id() != started.scope.step_execution_id()
            && restarted_progress.read_ordinal() == 2
            && restarted_progress.checkpoint_digest() == expected_checkpoint.generation_digest()
            && restarted_state.step_execution_id == restarted.step_execution_id()
            && restarted_state.read_count == 2
            && restarted_state.context.as_ref() == Some(&expected_context),
        backend,
        CASE,
        "restart did not inherit only the last committed checkpoint/context/counters",
    )
}

async fn explicit_rollback_preserves_durable_state<R, M, F, Fut>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    M: ChunkTransactionManager,
    F: FnMut(Arc<ContractClock>, Checkpoint, ExecutionContext) -> Fut,
    Fut: Future<Output = Result<CheckpointContextBackend<R, M>, RepositoryError>>,
{
    const CASE: &str = "checkpoint_context_rollback_preserves_durable_state";
    let (fixture, clock, _, _) = backend_for_case(backend, CASE, factory).await?;
    let started = create_started_step(backend, CASE, &fixture.repository, clock.as_ref()).await?;
    let before_progress = fixture
        .transactions
        .inherited_progress(started.scope)
        .await
        .map_err(|error| stage_failure(backend, CASE, "before-progress", error))?;
    let before_state = observe_state(
        backend,
        CASE,
        &fixture.repository,
        started.instance_id,
        &started.node_id,
    )
    .await?;

    let mut transaction = fixture
        .transactions
        .begin_for(started.scope)
        .await
        .map_err(|error| stage_failure(backend, CASE, "chunk-begin", error))?;
    transaction
        .rollback()
        .await
        .map_err(|error| stage_failure(backend, CASE, "chunk-rollback", error))?;

    let after_progress = fixture
        .transactions
        .inherited_progress(started.scope)
        .await
        .map_err(|error| stage_failure(backend, CASE, "after-progress", error))?;
    let after_state = observe_state(
        backend,
        CASE,
        &fixture.repository,
        started.instance_id,
        &started.node_id,
    )
    .await?;
    ensure(
        after_progress == before_progress && after_state == before_state,
        backend,
        CASE,
        "explicit rollback changed checkpoint/context/counters",
    )
}

#[allow(
    clippy::too_many_lines,
    reason = "the stale-CAS race and post-failure authority observations must remain one scenario"
)]
async fn stale_chunk_commit_is_known_not_committed<R, M, F, Fut>(
    backend: &'static str,
    factory: &mut F,
) -> Result<(), RepositoryContractFailure>
where
    R: JobRepository,
    M: ChunkTransactionManager,
    F: FnMut(Arc<ContractClock>, Checkpoint, ExecutionContext) -> Fut,
    Fut: Future<Output = Result<CheckpointContextBackend<R, M>, RepositoryError>>,
{
    const CASE: &str = "checkpoint_context_stale_chunk_commit_is_known_not_committed";
    let (fixture, clock, _, _) = backend_for_case(backend, CASE, factory).await?;
    let started = create_started_step(backend, CASE, &fixture.repository, clock.as_ref()).await?;
    let before_progress = fixture
        .transactions
        .inherited_progress(started.scope)
        .await
        .map_err(|error| stage_failure(backend, CASE, "before-progress", error))?;
    let before_state = observe_state(
        backend,
        CASE,
        &fixture.repository,
        started.instance_id,
        &started.node_id,
    )
    .await?;

    let mut transaction = fixture
        .transactions
        .begin_for(started.scope)
        .await
        .map_err(|error| stage_failure(backend, CASE, "chunk-begin", error))?;

    clock.advance(Duration::from_secs(1));
    let mut concurrent = fixture
        .repository
        .begin()
        .await
        .map_err(|error| stage_failure(backend, CASE, "concurrent-begin", error))?;
    let step = concurrent
        .get_step_execution(started.scope.step_execution_id())
        .await
        .map_err(|error| stage_failure(backend, CASE, "concurrent-load-step", error))?
        .ok_or_else(|| contract_failure(backend, CASE, "step missing before stale mutation"))?;
    let exit_code = ExitCode::new("CONTRACT_STALE")
        .map_err(|error| stage_failure(backend, CASE, "stale-exit-code", error))?;
    let exit_status = ExitStatus::new(exit_code);
    concurrent
        .enrich_step_exit_status(step.id(), step.version(), &exit_status)
        .await
        .map_err(|error| stage_failure(backend, CASE, "concurrent-step-cas", error))?;
    concurrent
        .commit()
        .await
        .map_err(|error| stage_failure(backend, CASE, "concurrent-commit", error))?;

    let Err(error) = transaction
        .commit(contract_counts(backend, CASE)?, ChunkFaultProgress::NONE)
        .await
    else {
        return Err(contract_failure(
            backend,
            CASE,
            "stale chunk commit unexpectedly succeeded",
        ));
    };
    ensure(
        error == ChunkTransactionError::NotCommitted,
        backend,
        CASE,
        "stale chunk commit was not classified as known-not-committed",
    )?;
    let diagnostic = format!("{error:?} {error}");
    ensure(
        !diagnostic.contains(STATE_SENTINEL),
        backend,
        CASE,
        "chunk transaction error exposed checkpoint/context payload",
    )?;

    let after_progress = fixture
        .transactions
        .inherited_progress(started.scope)
        .await
        .map_err(|error| stage_failure(backend, CASE, "after-progress", error))?;
    let after_state = observe_state(
        backend,
        CASE,
        &fixture.repository,
        started.instance_id,
        &started.node_id,
    )
    .await?;
    ensure(
        after_progress == before_progress
            && after_state.read_count == before_state.read_count
            && after_state.context == before_state.context,
        backend,
        CASE,
        "stale chunk commit changed checkpoint/context/counters",
    )
}
