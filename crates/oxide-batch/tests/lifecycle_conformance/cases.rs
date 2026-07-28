use std::error::Error;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use oxide_batch::{
    BatchStatus, ExecutionCounts, ExecutionMetadata, ExecutionTimestamps, ExecutionVersion,
    ExitCode, ExitStatus, FailureCategory, FailureId, FailureSummary, JobExecution, JobExecutionId,
    JobInstanceId, LifecycleError, LifecycleTransition, StepExecution, StepExecutionId, StepName,
};

fn time(second: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(second)
}

fn metadata(status: BatchStatus) -> Result<ExecutionMetadata, oxide_batch::DomainError> {
    let started_at = (!matches!(status, BatchStatus::Starting)).then(|| time(101));
    let ended_at = status.is_finished().then(|| time(102));
    let failure = if matches!(status, BatchStatus::Failed) {
        Some(FailureSummary::new(
            FailureCategory::UserComponent,
            FailureId::new(91)?,
        ))
    } else {
        None
    };
    ExecutionMetadata::new(
        status,
        ExitStatus::unknown(),
        ExecutionTimestamps::new(time(100), started_at, ended_at)?,
        ExecutionCounts::default(),
        failure,
    )
}

// STEP-STATUS-001
#[test]
fn exit_status_does_not_forge_batch_status() -> Result<(), Box<dyn Error>> {
    let mut step = StepExecution::new(
        StepExecutionId::new(21)?,
        JobExecutionId::new(20)?,
        StepName::new("import")?,
        metadata(BatchStatus::Started)?,
    );
    let status_before_enrichment = step.metadata().status();

    let version = step.enrich_exit_status(
        step.version(),
        ExitStatus::new(ExitCode::new("COMPLETED_WITH_WARNINGS")?),
    )?;

    assert_eq!(version, ExecutionVersion::new(1));
    assert_eq!(step.metadata().status(), status_before_enrichment);
    assert_eq!(
        step.metadata().exit_status().code().as_str(),
        "COMPLETED_WITH_WARNINGS"
    );
    Ok(())
}

// JOB-EXEC-001
#[test]
fn restart_creates_new_execution() -> Result<(), Box<dyn Error>> {
    let prior = JobExecution::new(
        JobExecutionId::new(31)?,
        JobInstanceId::new(30)?,
        metadata(BatchStatus::Failed)?,
    );
    let prior_snapshot = prior.clone();

    let restarted =
        prior.new_restart_attempt(prior.version(), JobExecutionId::new(32)?, time(200))?;

    assert_eq!(prior, prior_snapshot);
    assert_eq!(restarted.id(), JobExecutionId::new(32)?);
    assert_eq!(restarted.job_instance_id(), prior.job_instance_id());
    assert_eq!(restarted.metadata().status(), BatchStatus::Starting);
    assert_eq!(restarted.metadata().exit_status(), &ExitStatus::unknown());
    assert_eq!(restarted.metadata().counts(), ExecutionCounts::default());
    assert_eq!(restarted.metadata().timestamps().created_at(), time(200));
    assert_eq!(restarted.metadata().timestamps().started_at(), None);
    assert_eq!(restarted.metadata().timestamps().ended_at(), None);
    assert_eq!(restarted.version(), ExecutionVersion::INITIAL);
    Ok(())
}

// JOB-COMPLETE-001
#[test]
fn completed_instance_rejects_launch() -> Result<(), Box<dyn Error>> {
    let completed = JobExecution::new(
        JobExecutionId::new(41)?,
        JobInstanceId::new(40)?,
        metadata(BatchStatus::Completed)?,
    );

    assert_eq!(
        completed.new_restart_attempt(completed.version(), JobExecutionId::new(42)?, time(200)),
        Err(LifecycleError::NotRestartable {
            status: BatchStatus::Completed
        })
    );
    Ok(())
}

#[test]
fn stale_optimistic_version_is_typed_and_leaves_snapshot_unchanged() -> Result<(), Box<dyn Error>> {
    let mut execution = JobExecution::new(
        JobExecutionId::new(51)?,
        JobInstanceId::new(50)?,
        metadata(BatchStatus::Starting)?,
    );
    execution.transition(
        ExecutionVersion::INITIAL,
        LifecycleTransition::new(BatchStatus::Started, time(101)),
    )?;
    let winning_snapshot = execution.clone();

    assert_eq!(
        execution.transition(
            ExecutionVersion::INITIAL,
            LifecycleTransition::new(BatchStatus::Completed, time(102))
        ),
        Err(LifecycleError::StaleVersion {
            expected: ExecutionVersion::INITIAL,
            actual: ExecutionVersion::new(1)
        })
    );
    assert_eq!(execution, winning_snapshot);
    Ok(())
}

#[test]
fn direct_restart_transition_requires_a_new_attempt() -> Result<(), Box<dyn Error>> {
    let mut stopped = JobExecution::new(
        JobExecutionId::new(61)?,
        JobInstanceId::new(60)?,
        metadata(BatchStatus::Stopped)?,
    );
    let snapshot = stopped.clone();

    assert_eq!(
        stopped.transition(
            stopped.version(),
            LifecycleTransition::new(BatchStatus::Starting, time(200))
        ),
        Err(LifecycleError::RestartRequiresNewAttempt {
            from: BatchStatus::Stopped
        })
    );
    assert_eq!(stopped, snapshot);
    Ok(())
}

#[test]
fn failed_transition_requires_a_redacted_failure() -> Result<(), Box<dyn Error>> {
    let mut execution = JobExecution::new(
        JobExecutionId::new(71)?,
        JobInstanceId::new(70)?,
        metadata(BatchStatus::Started)?,
    );
    let snapshot = execution.clone();

    assert_eq!(
        execution.transition(
            execution.version(),
            LifecycleTransition::new(BatchStatus::Failed, time(102))
        ),
        Err(LifecycleError::FailedTransitionMissingFailure)
    );
    assert_eq!(execution, snapshot);
    Ok(())
}

#[test]
fn transition_time_and_version_overflow_fail_atomically() -> Result<(), Box<dyn Error>> {
    let starting_metadata = metadata(BatchStatus::Starting)?;
    let mut invalid_time = JobExecution::new(
        JobExecutionId::new(81)?,
        JobInstanceId::new(80)?,
        starting_metadata.clone(),
    );
    let invalid_time_snapshot = invalid_time.clone();
    assert!(matches!(
        invalid_time.transition(
            invalid_time.version(),
            LifecycleTransition::new(BatchStatus::Started, time(99))
        ),
        Err(LifecycleError::InvalidTransitionTime { .. })
    ));
    assert_eq!(invalid_time, invalid_time_snapshot);

    let mut exhausted = JobExecution::from_snapshot(
        JobExecutionId::new(82)?,
        JobInstanceId::new(80)?,
        starting_metadata,
        ExecutionVersion::new(u64::MAX),
    );
    let exhausted_snapshot = exhausted.clone();
    assert_eq!(
        exhausted.transition(
            exhausted.version(),
            LifecycleTransition::new(BatchStatus::Started, time(101))
        ),
        Err(LifecycleError::VersionExhausted {
            version: ExecutionVersion::new(u64::MAX)
        })
    );
    assert_eq!(exhausted, exhausted_snapshot);

    let stopped = JobExecution::new(
        JobExecutionId::new(83)?,
        JobInstanceId::new(80)?,
        metadata(BatchStatus::Stopped)?,
    );
    assert!(matches!(
        stopped.new_restart_attempt(stopped.version(), JobExecutionId::new(84)?, time(101)),
        Err(LifecycleError::InvalidTransitionTime { .. })
    ));
    Ok(())
}
