use std::error::Error;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use oxide_batch::{
    BatchStatus, ExecutionCounts, ExecutionMetadata, ExecutionTimestamps, ExitStatus,
    FailureCategory, FailureId, FailureSummary, JobExecution, JobExecutionId, JobInstanceId,
    LifecycleError, LifecycleTransition, StepExecution, StepExecutionId, StepName,
};

const STATUSES: [BatchStatus; 8] = [
    BatchStatus::Starting,
    BatchStatus::Started,
    BatchStatus::Stopping,
    BatchStatus::Stopped,
    BatchStatus::Failed,
    BatchStatus::Completed,
    BatchStatus::Abandoned,
    BatchStatus::Unknown,
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExpectedTransition {
    Legal,
    RequiresNewAttempt,
    Illegal,
}

fn time(second: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(second)
}

fn failure() -> Result<FailureSummary, oxide_batch::DomainError> {
    Ok(FailureSummary::new(
        FailureCategory::UserComponent,
        FailureId::new(41)?,
    ))
}

fn metadata(status: BatchStatus) -> Result<ExecutionMetadata, oxide_batch::DomainError> {
    let started_at = (!matches!(status, BatchStatus::Starting)).then(|| time(101));
    let ended_at = status.is_finished().then(|| time(102));
    let failure = matches!(status, BatchStatus::Failed)
        .then(failure)
        .transpose()?;
    ExecutionMetadata::new(
        status,
        ExitStatus::unknown(),
        ExecutionTimestamps::new(time(100), started_at, ended_at)?,
        ExecutionCounts::default(),
        failure,
    )
}

fn execution(status: BatchStatus) -> Result<JobExecution, oxide_batch::DomainError> {
    Ok(JobExecution::new(
        JobExecutionId::new(11)?,
        JobInstanceId::new(7)?,
        metadata(status)?,
    ))
}

fn transition(
    target: BatchStatus,
    at: SystemTime,
) -> Result<LifecycleTransition, oxide_batch::DomainError> {
    if matches!(target, BatchStatus::Failed) {
        Ok(LifecycleTransition::failed(
            at,
            FailureSummary::new(FailureCategory::UserComponent, FailureId::new(42)?),
        ))
    } else {
        Ok(LifecycleTransition::new(target, at))
    }
}

const fn expected_transition(from: BatchStatus, to: BatchStatus) -> ExpectedTransition {
    if matches!(from, BatchStatus::Stopped | BatchStatus::Failed)
        && matches!(to, BatchStatus::Starting)
    {
        return ExpectedTransition::RequiresNewAttempt;
    }
    if matches!(
        (from, to),
        (
            BatchStatus::Starting,
            BatchStatus::Started
                | BatchStatus::Stopping
                | BatchStatus::Failed
                | BatchStatus::Unknown
        ) | (
            BatchStatus::Started,
            BatchStatus::Stopping
                | BatchStatus::Stopped
                | BatchStatus::Failed
                | BatchStatus::Completed
                | BatchStatus::Unknown
        ) | (
            BatchStatus::Stopping,
            BatchStatus::Stopped | BatchStatus::Failed | BatchStatus::Unknown
        ) | (
            BatchStatus::Stopped | BatchStatus::Failed | BatchStatus::Unknown,
            BatchStatus::Abandoned
        ) | (BatchStatus::Unknown, BatchStatus::Failed)
    ) {
        ExpectedTransition::Legal
    } else {
        ExpectedTransition::Illegal
    }
}

#[test]
fn all_status_pairs_follow_the_accepted_transition_policy() -> Result<(), Box<dyn Error>> {
    for from in STATUSES {
        for to in STATUSES {
            let mut execution = execution(from)?;
            let before = execution.clone();
            let result = execution.transition(execution.version(), transition(to, time(103))?);

            match expected_transition(from, to) {
                ExpectedTransition::Legal => {
                    assert_eq!(result?, oxide_batch::ExecutionVersion::new(1));
                    assert_eq!(execution.metadata().status(), to);
                    assert_eq!(
                        execution.metadata().exit_status(),
                        before.metadata().exit_status()
                    );
                }
                ExpectedTransition::RequiresNewAttempt => {
                    assert_eq!(
                        result,
                        Err(LifecycleError::RestartRequiresNewAttempt { from })
                    );
                    assert_eq!(execution, before);
                }
                ExpectedTransition::Illegal => {
                    assert_eq!(result, Err(LifecycleError::IllegalTransition { from, to }));
                    assert_eq!(execution, before);
                }
            }
        }
    }
    Ok(())
}

#[test]
fn representative_transition_sequences_preserve_terminal_invariants() -> Result<(), Box<dyn Error>>
{
    let paths: &[&[BatchStatus]] = &[
        &[
            BatchStatus::Started,
            BatchStatus::Stopping,
            BatchStatus::Stopped,
        ],
        &[BatchStatus::Started, BatchStatus::Completed],
        &[BatchStatus::Failed, BatchStatus::Abandoned],
        &[
            BatchStatus::Unknown,
            BatchStatus::Failed,
            BatchStatus::Abandoned,
        ],
        &[
            BatchStatus::Stopping,
            BatchStatus::Unknown,
            BatchStatus::Abandoned,
        ],
    ];

    for path in paths {
        let mut execution = execution(BatchStatus::Starting)?;
        for (index, target) in path.iter().copied().enumerate() {
            let at = time(101 + u64::try_from(index)?);
            let previous_version = execution.version();
            let updated_version =
                execution.transition(previous_version, transition(target, at)?)?;
            assert_eq!(updated_version.get(), previous_version.get() + 1);
            assert_eq!(execution.metadata().status(), target);
        }

        if execution.metadata().status().is_terminal() {
            let terminal_snapshot = execution.clone();
            for target in STATUSES {
                assert!(matches!(
                    execution.transition(execution.version(), transition(target, time(110))?),
                    Err(LifecycleError::IllegalTransition { .. })
                ));
                assert_eq!(execution, terminal_snapshot);
            }
        }
    }
    Ok(())
}

#[test]
fn stopped_and_failed_restart_as_fresh_step_and_job_attempts() -> Result<(), Box<dyn Error>> {
    for restartable_status in [BatchStatus::Stopped, BatchStatus::Failed] {
        let execution = execution(restartable_status)?;
        let restarted = execution.new_restart_attempt(
            execution.version(),
            JobExecutionId::new(12)?,
            time(200),
        )?;

        assert_eq!(execution.metadata().status(), restartable_status);
        assert_eq!(restarted.metadata().status(), BatchStatus::Starting);
        assert_eq!(restarted.version(), oxide_batch::ExecutionVersion::INITIAL);
        assert_eq!(restarted.job_instance_id(), execution.job_instance_id());
        assert_ne!(restarted.id(), execution.id());

        let step = StepExecution::new(
            StepExecutionId::new(21)?,
            execution.id(),
            StepName::new("import")?,
            metadata(restartable_status)?,
        );
        let restarted_step = step.new_restart_attempt(
            step.version(),
            StepExecutionId::new(22)?,
            restarted.id(),
            time(200),
        )?;
        assert_eq!(step.metadata().status(), restartable_status);
        assert_eq!(restarted_step.metadata().status(), BatchStatus::Starting);
        assert_eq!(restarted_step.job_execution_id(), restarted.id());
        assert_eq!(restarted_step.step_name(), step.step_name());
        assert_ne!(restarted_step.id(), step.id());
    }
    Ok(())
}

#[test]
fn step_restart_rejects_reuse_of_either_attempt_identifier() -> Result<(), Box<dyn Error>> {
    let step = StepExecution::new(
        StepExecutionId::new(31)?,
        JobExecutionId::new(30)?,
        StepName::new("import")?,
        metadata(BatchStatus::Stopped)?,
    );

    for (step_execution_id, job_execution_id) in [
        (step.id(), JobExecutionId::new(32)?),
        (StepExecutionId::new(33)?, step.job_execution_id()),
    ] {
        assert_eq!(
            step.new_restart_attempt(
                step.version(),
                step_execution_id,
                job_execution_id,
                time(200)
            ),
            Err(LifecycleError::AttemptIdentifierReused)
        );
    }
    Ok(())
}
