//! In-memory repository behavior and public port contracts.

#[allow(dead_code)]
#[path = "support/clock.rs"]
mod clock;
#[allow(dead_code)]
#[path = "support/ids.rs"]
mod ids;

use std::error::Error;
use std::num::NonZeroU64;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clock::ManualClock;
use futures_executor::block_on;
use ids::{DeterministicIds, IdSequenceError};
use oxide_batch::{
    BatchStatus, ExecutionVersion, ExitCode, ExitStatus, FailureCategory, FailureId,
    FailureSummary, IdGenerationError, IdGenerator, IdentifierKind, InMemoryJobRepository,
    JobInstanceKey, JobName, JobParameter, JobParameters, JobRepository, LifecycleError,
    LifecycleTransition, ParameterName, ParameterRole, ParameterValue, RepositoryError,
    SequentialIdGenerator, StepName,
};

fn time(second: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(second)
}

fn repository(
    initial_time: SystemTime,
) -> Result<(InMemoryJobRepository, ManualClock), IdSequenceError> {
    let clock = ManualClock::new(initial_time);
    let first_id = NonZeroU64::new(1).ok_or(IdSequenceError::Exhausted)?;
    Ok((
        InMemoryJobRepository::new(
            Arc::new(clock.clone()),
            Arc::new(DeterministicIds::new(first_id)),
        ),
        clock,
    ))
}

fn instance_key() -> Result<JobInstanceKey, oxide_batch::DomainError> {
    let parameters = JobParameters::try_from_iter([(
        ParameterName::new("business_date")?,
        JobParameter::new(
            ParameterValue::string("2026-07-29")?,
            ParameterRole::Identifying,
        ),
    )])?;
    Ok(JobInstanceKey::new(
        JobName::new("repository_import")?,
        &parameters,
    ))
}

// VS-LAUNCH-001
#[test]
fn first_launch_creates_execution_graph() -> Result<(), Box<dyn Error>> {
    let (repository, _) = repository(time(100))?;
    let key = instance_key()?;
    let mut unit = block_on(repository.begin())?;

    let selection = block_on(unit.select_or_create_job_instance(&key))?;
    assert!(selection.was_created());
    let instance = selection.instance().clone();
    let job = block_on(unit.create_job_execution(instance.id()))?;
    let step = block_on(unit.create_step_execution(job.id(), &StepName::new("import")?))?;
    block_on(unit.commit())?;

    assert_eq!(instance.id().get(), 1);
    assert_eq!(job.id().get(), 2);
    assert_eq!(step.id().get(), 3);
    assert_eq!(job.job_instance_id(), instance.id());
    assert_eq!(step.job_execution_id(), job.id());
    assert_eq!(job.metadata().status(), BatchStatus::Starting);
    assert_eq!(step.metadata().status(), BatchStatus::Starting);
    assert_eq!(job.metadata().timestamps().created_at(), time(100));

    let mut inspection = block_on(repository.begin())?;
    assert_eq!(
        block_on(inspection.find_job_instance(&key))?,
        Some(instance.clone())
    );
    assert_eq!(
        block_on(inspection.get_job_instance(instance.id()))?,
        Some(instance.clone())
    );
    assert_eq!(
        block_on(inspection.get_job_execution(job.id()))?,
        Some(job.clone())
    );
    assert_eq!(
        block_on(inspection.get_step_execution(step.id()))?,
        Some(step.clone())
    );
    assert_eq!(
        block_on(inspection.job_executions(instance.id()))?,
        vec![job]
    );
    assert_eq!(
        block_on(inspection.step_executions(step.job_execution_id()))?,
        vec![step]
    );
    block_on(inspection.rollback())?;
    Ok(())
}

#[test]
fn rollback_leaves_no_visible_metadata() -> Result<(), Box<dyn Error>> {
    let (repository, _) = repository(time(100))?;
    let key = instance_key()?;
    let mut unit = block_on(repository.begin())?;
    block_on(unit.select_or_create_job_instance(&key))?;
    block_on(unit.rollback())?;

    let mut inspection = block_on(repository.begin())?;
    assert_eq!(block_on(inspection.find_job_instance(&key))?, None);
    block_on(inspection.rollback())?;
    Ok(())
}

#[test]
fn job_and_step_exit_status_updates_preserve_lifecycle_status() -> Result<(), Box<dyn Error>> {
    let (repository, _) = repository(time(100))?;
    let key = instance_key()?;
    let mut unit = block_on(repository.begin())?;
    let instance = block_on(unit.select_or_create_job_instance(&key))?
        .instance()
        .clone();
    let job = block_on(unit.create_job_execution(instance.id()))?;
    let step = block_on(unit.create_step_execution(job.id(), &StepName::new("import")?))?;
    let started_job = block_on(unit.transition_job_execution(
        job.id(),
        job.version(),
        LifecycleTransition::new(BatchStatus::Started, time(101)),
    ))?;
    let started_step = block_on(unit.transition_step_execution(
        step.id(),
        step.version(),
        LifecycleTransition::new(BatchStatus::Started, time(101)),
    ))?;
    let warning = ExitStatus::new(ExitCode::new("COMPLETED_WITH_WARNINGS")?);
    let enriched_job =
        block_on(unit.enrich_job_exit_status(started_job.id(), started_job.version(), &warning))?;
    let enriched_step = block_on(unit.enrich_step_exit_status(
        started_step.id(),
        started_step.version(),
        &warning,
    ))?;
    block_on(unit.commit())?;

    assert_eq!(enriched_job.metadata().status(), BatchStatus::Started);
    assert_eq!(enriched_step.metadata().status(), BatchStatus::Started);
    assert_eq!(enriched_job.metadata().exit_status(), &warning);
    assert_eq!(enriched_step.metadata().exit_status(), &warning);
    assert_eq!(enriched_job.version(), ExecutionVersion::new(2));
    assert_eq!(enriched_step.version(), ExecutionVersion::new(2));
    Ok(())
}

#[test]
fn repository_rejects_stale_and_illegal_transitions_atomically() -> Result<(), Box<dyn Error>> {
    let (repository, clock) = repository(time(100))?;
    let key = instance_key()?;
    let mut unit = block_on(repository.begin())?;
    let instance = block_on(unit.select_or_create_job_instance(&key))?
        .instance()
        .clone();
    let execution = block_on(unit.create_job_execution(instance.id()))?;

    let stale = block_on(unit.transition_job_execution(
        execution.id(),
        ExecutionVersion::new(1),
        LifecycleTransition::new(BatchStatus::Started, time(101)),
    ));
    assert_eq!(
        stale,
        Err(RepositoryError::Lifecycle(LifecycleError::StaleVersion {
            expected: ExecutionVersion::new(1),
            actual: ExecutionVersion::INITIAL,
        }))
    );

    let illegal = block_on(unit.transition_job_execution(
        execution.id(),
        ExecutionVersion::INITIAL,
        LifecycleTransition::new(BatchStatus::Completed, time(102)),
    ));
    assert_eq!(
        illegal,
        Err(RepositoryError::Lifecycle(
            LifecycleError::IllegalTransition {
                from: BatchStatus::Starting,
                to: BatchStatus::Completed,
            }
        ))
    );

    clock.set(time(101));
    let started = block_on(unit.transition_job_execution(
        execution.id(),
        ExecutionVersion::INITIAL,
        LifecycleTransition::new(BatchStatus::Started, clock.now()),
    ))?;
    assert_eq!(started.version(), ExecutionVersion::new(1));
    block_on(unit.commit())?;

    let mut inspection = block_on(repository.begin())?;
    assert_eq!(
        block_on(inspection.get_job_execution(execution.id()))?,
        Some(started)
    );
    block_on(inspection.rollback())?;
    Ok(())
}

// JOB-COMPLETE-001
#[test]
fn completed_instance_rejects_launch() -> Result<(), Box<dyn Error>> {
    let (repository, _) = repository(time(100))?;
    let key = instance_key()?;
    let mut first = block_on(repository.begin())?;
    let instance = block_on(first.select_or_create_job_instance(&key))?
        .instance()
        .clone();
    let execution = block_on(first.create_job_execution(instance.id()))?;
    let started = block_on(first.transition_job_execution(
        execution.id(),
        execution.version(),
        LifecycleTransition::new(BatchStatus::Started, time(101)),
    ))?;
    block_on(first.transition_job_execution(
        started.id(),
        started.version(),
        LifecycleTransition::new(BatchStatus::Completed, time(102)),
    ))?;
    block_on(first.commit())?;

    let mut duplicate = block_on(repository.begin())?;
    let selected = block_on(duplicate.select_or_create_job_instance(&key))?;
    assert!(!selected.was_created());
    assert_eq!(
        block_on(duplicate.create_job_execution(instance.id())),
        Err(RepositoryError::CompletedInstance { id: instance.id() })
    );
    block_on(duplicate.rollback())?;
    Ok(())
}

// JOB-EXEC-001
#[test]
fn failed_instance_restart_creates_distinct_execution() -> Result<(), Box<dyn Error>> {
    let (repository, clock) = repository(time(100))?;
    let key = instance_key()?;
    let mut first = block_on(repository.begin())?;
    let instance = block_on(first.select_or_create_job_instance(&key))?
        .instance()
        .clone();
    let initial = block_on(first.create_job_execution(instance.id()))?;
    let failed = block_on(first.transition_job_execution(
        initial.id(),
        initial.version(),
        LifecycleTransition::failed(
            time(101),
            FailureSummary::new(FailureCategory::UserComponent, FailureId::new(99)?),
        ),
    ))?;
    block_on(first.commit())?;

    clock.set(time(200));
    let mut restart = block_on(repository.begin())?;
    let restarted = block_on(restart.create_job_execution(instance.id()))?;
    block_on(restart.commit())?;

    assert_ne!(restarted.id(), failed.id());
    assert_eq!(restarted.job_instance_id(), instance.id());
    assert_eq!(restarted.metadata().status(), BatchStatus::Starting);
    assert_eq!(restarted.version(), ExecutionVersion::INITIAL);
    assert_eq!(restarted.metadata().timestamps().created_at(), time(200));

    let mut inspection = block_on(repository.begin())?;
    assert_eq!(
        block_on(inspection.job_executions(instance.id()))?,
        vec![failed, restarted]
    );
    block_on(inspection.rollback())?;
    Ok(())
}

// JOB-CONCURRENCY-001
#[test]
fn concurrent_launch_creates_single_instance() -> Result<(), Box<dyn Error>> {
    const CONTENDERS: usize = 12;
    let (repository, _) = repository(time(100))?;
    let key = instance_key()?;
    let barrier = Arc::new(Barrier::new(CONTENDERS));
    let mut handles = Vec::with_capacity(CONTENDERS);

    for _ in 0..CONTENDERS {
        let repository = repository.clone();
        let key = key.clone();
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || -> Result<(), RepositoryError> {
            let mut unit = block_on(repository.begin())?;
            let instance = block_on(unit.select_or_create_job_instance(&key))?
                .instance()
                .clone();
            block_on(unit.create_job_execution(instance.id()))?;
            barrier.wait();
            block_on(unit.commit())
        }));
    }

    let results = handles
        .into_iter()
        .map(|handle| {
            handle
                .join()
                .map_err(|_| "concurrent launch thread panicked")
        })
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(RepositoryError::ConcurrentModification)))
            .count(),
        CONTENDERS - 1
    );

    let mut inspection = block_on(repository.begin())?;
    let instance =
        block_on(inspection.find_job_instance(&key))?.ok_or("committed instance was not found")?;
    assert_eq!(block_on(inspection.job_executions(instance.id()))?.len(), 1);
    block_on(inspection.rollback())?;
    Ok(())
}

#[test]
fn overlapping_units_of_work_have_one_commit_winner() -> Result<(), Box<dyn Error>> {
    let (repository, _) = repository(time(100))?;
    let first_key = instance_key()?;
    let second_key = JobInstanceKey::new(JobName::new("other_job")?, &JobParameters::new());
    let mut first = block_on(repository.begin())?;
    let mut second = block_on(repository.begin())?;

    block_on(first.select_or_create_job_instance(&first_key))?;
    block_on(second.select_or_create_job_instance(&second_key))?;
    block_on(first.commit())?;
    assert_eq!(
        block_on(second.commit()),
        Err(RepositoryError::ConcurrentModification)
    );

    let mut inspection = block_on(repository.begin())?;
    assert!(block_on(inspection.find_job_instance(&first_key))?.is_some());
    assert_eq!(block_on(inspection.find_job_instance(&second_key))?, None);
    block_on(inspection.rollback())?;
    Ok(())
}

#[test]
fn sequential_identifier_source_reports_exhaustion_without_returning_zero()
-> Result<(), Box<dyn Error>> {
    let first = NonZeroU64::new(u64::MAX).ok_or("maximum u64 must be nonzero")?;
    let ids = SequentialIdGenerator::new(first);
    assert_eq!(ids.next_job_instance_id()?.get(), u64::MAX);
    assert_eq!(
        ids.next_job_execution_id(),
        Err(IdGenerationError::Exhausted {
            kind: IdentifierKind::JobExecution,
        })
    );
    Ok(())
}
