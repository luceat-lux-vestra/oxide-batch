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
    BatchStatus, ComponentRevision, DefinitionIdentity, DefinitionRevision, DefinitionUpgrade,
    DefinitionUpgradeKey, ExecutionVersion, ExitCode, ExitStatus, FailureCategory, FailureId,
    FailureSummary, IdGenerationError, IdGenerator, IdentifierKind, InMemoryJobRepository,
    JobInstanceKey, JobName, JobParameter, JobParameters, JobRepository, LifecycleError,
    LifecycleTransition, NestedJobLinkRequest, NodeId, OwnerToken, ParameterName, ParameterRole,
    ParameterValue, RecoveryRequest, RepositoryError, SequentialIdGenerator, StepDefinitionUpgrade,
    StepName,
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

#[test]
fn owner_comparison_and_stop_observation_are_atomic() -> Result<(), Box<dyn Error>> {
    let (repository, _) = repository(time(100))?;
    let mut create = block_on(repository.begin())?;
    let instance = block_on(create.select_or_create_job_instance(&instance_key()?))?
        .instance()
        .clone();
    let execution = block_on(create.create_job_execution(instance.id()))?;
    let owner = OwnerToken::from_bytes([1; 16]);
    let claimed = block_on(create.claim_execution_owner(
        execution.id(),
        execution.version(),
        &owner,
        time(101),
    ))?;
    block_on(create.commit())?;

    let mut competing = block_on(repository.begin())?;
    assert_eq!(
        block_on(competing.claim_execution_owner(
            execution.id(),
            claimed.version(),
            &OwnerToken::from_bytes([2; 16]),
            time(102),
        )),
        Err(RepositoryError::ExecutionOwned { id: execution.id() })
    );
    block_on(competing.rollback())?;

    let mut request = block_on(repository.begin())?;
    block_on(request.request_execution_stop(
        execution.id(),
        claimed.version(),
        &oxide_batch::ActorRef::new("operator:test")?,
        time(103),
    ))?;
    block_on(request.commit())?;

    let mut observe = block_on(repository.begin())?;
    let control = block_on(observe.observe_execution_control(execution.id(), &owner, time(104)))?;
    block_on(observe.commit())?;
    assert!(control.owner_matches());
    assert!(control.stop_requested());
    assert_eq!(
        control.execution().metadata().status(),
        BatchStatus::Stopping
    );
    Ok(())
}

#[test]
fn explicit_recovery_is_audited_before_restart() -> Result<(), Box<dyn Error>> {
    let (repository, clock) = repository(time(100))?;
    let key = instance_key()?;
    let mut create = block_on(repository.begin())?;
    let instance = block_on(create.select_or_create_job_instance(&key))?
        .instance()
        .clone();
    let execution = block_on(create.create_job_execution(instance.id()))?;
    block_on(create.commit())?;

    let mut unknown = block_on(repository.begin())?;
    let ambiguous = block_on(unknown.transition_job_execution(
        execution.id(),
        execution.version(),
        LifecycleTransition::new(BatchStatus::Unknown, time(101)),
    ))?;
    block_on(unknown.commit())?;

    let mut blocked = block_on(repository.begin())?;
    assert_eq!(
        block_on(blocked.create_job_execution(instance.id())),
        Err(RepositoryError::ExecutionAlreadyActive {
            instance_id: instance.id(),
            execution_id: execution.id(),
            status: BatchStatus::Unknown,
        })
    );
    block_on(blocked.rollback())?;

    clock.set(time(102));
    let request = RecoveryRequest::mark_failed(
        ambiguous.version(),
        "COMMIT_INSPECTED_NOT_DURABLE",
        "operator-correlation-42",
        [0xA5; 32],
        FailureCategory::PermanentInfrastructure,
        FailureId::new(900)?,
    )?;
    let mut recovery = block_on(repository.begin())?;
    let result = block_on(recovery.recover_job_execution(execution.id(), &request))?;
    assert_eq!(result.execution().metadata().status(), BatchStatus::Failed);
    assert_eq!(result.decision().prior_status(), BatchStatus::Unknown);
    assert_eq!(result.decision().resulting_status(), BatchStatus::Failed);
    assert_eq!(result.decision().decided_at(), time(102));
    block_on(recovery.commit())?;

    let mut restart = block_on(repository.begin())?;
    let restarted = block_on(restart.create_job_execution(instance.id()))?;
    assert_ne!(restarted.id(), execution.id());
    block_on(restart.commit())?;

    let mut inspection = block_on(repository.begin())?;
    let decision = block_on(inspection.recovery_decision(execution.id()))?;
    assert_eq!(decision, Some(result.decision().clone()));
    block_on(inspection.rollback())?;
    Ok(())
}

#[test]
fn recovery_is_versioned_bounded_and_value_redacted() -> Result<(), Box<dyn Error>> {
    let request = RecoveryRequest::abandon(
        ExecutionVersion::INITIAL,
        "ORPHAN_CONFIRMED",
        "operator-secret-correlation",
        [0x5A; 32],
    )?;
    let diagnostic = format!("{request:?}");
    assert!(diagnostic.contains("ORPHAN_CONFIRMED"));
    assert!(diagnostic.contains("operator-secret-correlation"));
    assert!(!diagnostic.contains("5a5a5a"));
    assert!(
        RecoveryRequest::mark_failed(
            ExecutionVersion::INITIAL,
            " padded ",
            "operator",
            [0; 32],
            FailureCategory::Invariant,
            FailureId::new(2)?,
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn audited_abandon_makes_an_orphaned_instance_terminal() -> Result<(), Box<dyn Error>> {
    let (repository, clock) = repository(time(100))?;
    let key = instance_key()?;
    let mut create = block_on(repository.begin())?;
    let instance = block_on(create.select_or_create_job_instance(&key))?
        .instance()
        .clone();
    let execution = block_on(create.create_job_execution(instance.id()))?;
    block_on(create.commit())?;
    let mut start = block_on(repository.begin())?;
    let started = block_on(start.transition_job_execution(
        execution.id(),
        execution.version(),
        LifecycleTransition::new(BatchStatus::Started, time(101)),
    ))?;
    block_on(start.commit())?;

    clock.set(time(102));
    let request = RecoveryRequest::abandon(
        started.version(),
        "ORPHAN_CONFIRMED",
        "operator-correlation-99",
        [0x99; 32],
    )?;
    let mut recover = block_on(repository.begin())?;
    let abandoned = block_on(recover.recover_job_execution(execution.id(), &request))?;
    block_on(recover.commit())?;
    assert_eq!(
        abandoned.execution().metadata().status(),
        BatchStatus::Abandoned
    );
    let diagnostic = format!("{:?}", abandoned.decision());
    assert!(diagnostic.contains("ORPHAN_CONFIRMED"));
    assert!(!diagnostic.contains("153, 153"));

    let mut restart = block_on(repository.begin())?;
    assert_eq!(
        block_on(restart.create_job_execution(instance.id())),
        Err(RepositoryError::AbandonedInstance { id: instance.id() })
    );
    block_on(restart.rollback())?;
    Ok(())
}

#[test]
fn restart_definition_drift_and_direct_compatibility_are_typed() -> Result<(), Box<dyn Error>> {
    let (repository, clock) = repository(time(100))?;
    let key = instance_key()?;
    let step = StepName::new("import")?;
    let renamed_step = StepName::new("import-v2")?;
    let v1 = DefinitionIdentity::tasklet(
        key.job_name(),
        &step,
        DefinitionRevision::new("v1")?,
        &ComponentRevision::new("tasklet-v1")?,
    )?;
    let drift = DefinitionIdentity::tasklet(
        key.job_name(),
        &step,
        DefinitionRevision::new("v1")?,
        &ComponentRevision::new("tasklet-v1-drifted")?,
    )?;
    let v2 = DefinitionIdentity::tasklet(
        key.job_name(),
        &renamed_step,
        DefinitionRevision::new("v2")?,
        &ComponentRevision::new("tasklet-v2-compatible")?,
    )?;
    let mut create = block_on(repository.begin())?;
    let instance = block_on(create.select_or_create_job_instance(&key))?
        .instance()
        .clone();
    let first = block_on(create.create_job_execution_with_definition(instance.id(), &v1))?;
    block_on(create.commit())?;
    clock.set(time(101));
    let mut fail = block_on(repository.begin())?;
    block_on(fail.transition_job_execution(
        first.id(),
        first.version(),
        LifecycleTransition::failed(
            time(101),
            FailureSummary::new(FailureCategory::UserComponent, FailureId::new(700)?),
        ),
    ))?;
    block_on(fail.commit())?;

    let mut rejected = block_on(repository.begin())?;
    assert!(matches!(
        block_on(rejected.create_job_execution_with_definition(instance.id(), &drift)),
        Err(RepositoryError::DefinitionDrift { .. })
    ));
    block_on(rejected.rollback())?;
    let mut rejected = block_on(repository.begin())?;
    assert_eq!(
        block_on(rejected.create_job_execution_with_definition(instance.id(), &v2)),
        Err(RepositoryError::IncompatibleDefinition {
            instance_id: instance.id(),
        })
    );
    block_on(rejected.rollback())?;

    let upgrade = DefinitionUpgrade::new(
        DefinitionUpgradeKey::new("v1-to-v2")?,
        v1,
        v2.clone(),
        [StepDefinitionUpgrade::new(step, renamed_step)],
    )?;
    let mut register = block_on(repository.begin())?;
    block_on(register.register_definition_upgrade(key.job_name(), &upgrade))?;
    block_on(register.commit())?;
    let mut restart = block_on(repository.begin())?;
    let second = block_on(restart.create_job_execution_with_definition(instance.id(), &v2))?;
    block_on(restart.commit())?;
    assert_ne!(first.id(), second.id());
    Ok(())
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

fn nested_job_parameters_fixture() -> Result<JobParameters, oxide_batch::DomainError> {
    JobParameters::try_from_iter([
        (
            ParameterName::new("account")?,
            JobParameter::new(
                ParameterValue::string("acct-42")?,
                ParameterRole::Identifying,
            ),
        ),
        (
            ParameterName::new("trace")?,
            JobParameter::new(
                ParameterValue::string("mapped-once")?,
                ParameterRole::NonIdentifying,
            ),
        ),
    ])
}

fn nested_child_definition() -> Result<DefinitionIdentity, Box<dyn Error>> {
    Ok(DefinitionIdentity::tasklet(
        &JobName::new("nested_child")?,
        &StepName::new("child_step")?,
        DefinitionRevision::new("nested-child-v1")?,
        &ComponentRevision::new("nested-child-tasklet-v1")?,
    )?)
}

fn create_parent_attempt(
    repository: &InMemoryJobRepository,
    key: &JobInstanceKey,
) -> Result<(oxide_batch::JobInstance, oxide_batch::JobExecution), Box<dyn Error>> {
    let mut unit = block_on(repository.begin())?;
    let instance = block_on(unit.select_or_create_job_instance(key))?
        .instance()
        .clone();
    let execution = block_on(unit.create_job_execution(instance.id()))?;
    block_on(unit.commit())?;
    Ok((instance, execution))
}

fn stop_execution(
    repository: &InMemoryJobRepository,
    execution: &oxide_batch::JobExecution,
    started_at: SystemTime,
    stopped_at: SystemTime,
) -> Result<oxide_batch::JobExecution, Box<dyn Error>> {
    let mut unit = block_on(repository.begin())?;
    let started = block_on(unit.transition_job_execution(
        execution.id(),
        execution.version(),
        LifecycleTransition::new(BatchStatus::Started, started_at),
    ))?;
    let stopped = block_on(unit.transition_job_execution(
        started.id(),
        started.version(),
        LifecycleTransition::new(BatchStatus::Stopped, stopped_at),
    ))?;
    block_on(unit.commit())?;
    Ok(stopped)
}

#[test]
fn nested_job_completed_child_is_reused_with_exact_parameters() -> Result<(), Box<dyn Error>> {
    let (repository, clock) = repository(time(100))?;
    let parent_key = JobInstanceKey::new(
        JobName::new("nested_parent_completed")?,
        &JobParameters::new(),
    );
    let (parent_instance, parent_execution) = create_parent_attempt(&repository, &parent_key)?;
    let node = NodeId::new("child")?;
    let child_definition = nested_child_definition()?;
    let parameters = nested_job_parameters_fixture()?;

    let mut create_link = block_on(repository.begin())?;
    let first_link = block_on(
        create_link.create_nested_job_link(&NestedJobLinkRequest::new(
            parent_instance.id(),
            parent_execution.id(),
            node.clone(),
            child_definition.clone(),
            parameters.clone(),
            time(101),
        )),
    )?;
    block_on(create_link.commit())?;

    let mut complete_child = block_on(repository.begin())?;
    let child = block_on(complete_child.get_job_execution(first_link.child_job_execution_id()))?
        .ok_or("linked child execution missing")?;
    let started = block_on(complete_child.transition_job_execution(
        child.id(),
        child.version(),
        LifecycleTransition::new(BatchStatus::Started, time(102)),
    ))?;
    block_on(complete_child.transition_job_execution(
        started.id(),
        started.version(),
        LifecycleTransition::new(BatchStatus::Completed, time(103)),
    ))?;
    let observed = block_on(complete_child.observe_nested_job_terminal(
        parent_execution.id(),
        &node,
        time(104),
    ))?;
    assert_eq!(
        observed
            .terminal()
            .map(oxide_batch::NestedJobTerminalObservation::status),
        Some(BatchStatus::Completed)
    );
    block_on(complete_child.commit())?;

    stop_execution(&repository, &parent_execution, time(105), time(106))?;
    clock.set(time(107));
    let (_, restarted_parent) = create_parent_attempt(&repository, &parent_key)?;

    let mut continuation = block_on(repository.begin())?;
    let reused = block_on(continuation.continue_nested_job_link(
        restarted_parent.id(),
        &node,
        parent_execution.id(),
        time(108),
    ))?;
    assert_eq!(
        reused.child_job_execution_id(),
        first_link.child_job_execution_id()
    );
    assert_eq!(
        reused.child_job_instance_id(),
        first_link.child_job_instance_id()
    );
    assert_eq!(
        reused
            .terminal()
            .map(oxide_batch::NestedJobTerminalObservation::status),
        Some(BatchStatus::Completed)
    );
    let restored = block_on(continuation.nested_job_parameters(restarted_parent.id(), &node))?;
    assert_eq!(restored, parameters);
    block_on(continuation.commit())?;
    Ok(())
}

#[test]
fn nested_job_stopped_child_restarts_same_instance_with_exact_parameters()
-> Result<(), Box<dyn Error>> {
    let (repository, clock) = repository(time(200))?;
    let parent_key = JobInstanceKey::new(
        JobName::new("nested_parent_restart")?,
        &JobParameters::new(),
    );
    let (parent_instance, parent_execution) = create_parent_attempt(&repository, &parent_key)?;
    let node = NodeId::new("child")?;
    let parameters = nested_job_parameters_fixture()?;

    let mut create_link = block_on(repository.begin())?;
    let first_link = block_on(
        create_link.create_nested_job_link(&NestedJobLinkRequest::new(
            parent_instance.id(),
            parent_execution.id(),
            node.clone(),
            nested_child_definition()?,
            parameters.clone(),
            time(201),
        )),
    )?;
    block_on(create_link.commit())?;

    let mut child_read = block_on(repository.begin())?;
    let child = block_on(child_read.get_job_execution(first_link.child_job_execution_id()))?
        .ok_or("linked child execution missing")?;
    block_on(child_read.rollback())?;
    stop_execution(&repository, &child, time(202), time(203))?;
    stop_execution(&repository, &parent_execution, time(204), time(205))?;

    clock.set(time(206));
    let (_, restarted_parent) = create_parent_attempt(&repository, &parent_key)?;
    let mut continuation = block_on(repository.begin())?;
    let restarted = block_on(continuation.continue_nested_job_link(
        restarted_parent.id(),
        &node,
        parent_execution.id(),
        time(207),
    ))?;
    assert_eq!(
        restarted.child_job_instance_id(),
        first_link.child_job_instance_id()
    );
    assert_ne!(
        restarted.child_job_execution_id(),
        first_link.child_job_execution_id()
    );
    assert!(restarted.terminal().is_none());
    let restored = block_on(continuation.nested_job_parameters(restarted_parent.id(), &node))?;
    assert_eq!(restored, parameters);
    let attempts = block_on(continuation.job_executions(first_link.child_job_instance_id()))?;
    assert_eq!(attempts.len(), 2);
    block_on(continuation.commit())?;
    Ok(())
}

#[test]
fn nested_job_active_and_unknown_children_fail_closed_without_duplicate_attempt()
-> Result<(), Box<dyn Error>> {
    let (repository, clock) = repository(time(300))?;
    let parent_key = JobInstanceKey::new(
        JobName::new("nested_parent_unresolved")?,
        &JobParameters::new(),
    );
    let (parent_instance, parent_execution) = create_parent_attempt(&repository, &parent_key)?;
    let node = NodeId::new("child")?;

    let mut create_link = block_on(repository.begin())?;
    let first_link = block_on(
        create_link.create_nested_job_link(&NestedJobLinkRequest::new(
            parent_instance.id(),
            parent_execution.id(),
            node.clone(),
            nested_child_definition()?,
            nested_job_parameters_fixture()?,
            time(301),
        )),
    )?;
    block_on(create_link.commit())?;
    stop_execution(&repository, &parent_execution, time(302), time(303))?;
    clock.set(time(304));
    let (_, restarted_parent) = create_parent_attempt(&repository, &parent_key)?;

    let mut active = block_on(repository.begin())?;
    assert_eq!(
        block_on(active.continue_nested_job_link(
            restarted_parent.id(),
            &node,
            parent_execution.id(),
            time(305),
        )),
        Err(RepositoryError::NestedJobChildUnresolved {
            child_execution_id: first_link.child_job_execution_id(),
            status: BatchStatus::Starting,
        })
    );
    let attempts = block_on(active.job_executions(first_link.child_job_instance_id()))?;
    assert_eq!(attempts.len(), 1);
    block_on(active.rollback())?;

    let mut mark_unknown = block_on(repository.begin())?;
    let child = block_on(mark_unknown.get_job_execution(first_link.child_job_execution_id()))?
        .ok_or("linked child execution missing")?;
    block_on(mark_unknown.transition_job_execution(
        child.id(),
        child.version(),
        LifecycleTransition::new(BatchStatus::Unknown, time(306)),
    ))?;
    block_on(mark_unknown.commit())?;

    let mut ambiguous = block_on(repository.begin())?;
    assert_eq!(
        block_on(ambiguous.continue_nested_job_link(
            restarted_parent.id(),
            &node,
            parent_execution.id(),
            time(307),
        )),
        Err(RepositoryError::NestedJobChildUnresolved {
            child_execution_id: first_link.child_job_execution_id(),
            status: BatchStatus::Unknown,
        })
    );
    let attempts = block_on(ambiguous.job_executions(first_link.child_job_instance_id()))?;
    assert_eq!(attempts.len(), 1);
    assert_eq!(
        block_on(ambiguous.observe_nested_job_terminal(parent_execution.id(), &node, time(308),)),
        Err(RepositoryError::NestedJobChildUnresolved {
            child_execution_id: first_link.child_job_execution_id(),
            status: BatchStatus::Unknown,
        })
    );
    block_on(ambiguous.rollback())?;
    Ok(())
}

#[test]
fn nested_job_existing_link_rejects_parameter_drift_without_duplicate_child()
-> Result<(), Box<dyn Error>> {
    let (repository, _) = repository(time(400))?;
    let parent_key =
        JobInstanceKey::new(JobName::new("nested_parent_drift")?, &JobParameters::new());
    let (parent_instance, parent_execution) = create_parent_attempt(&repository, &parent_key)?;
    let node = NodeId::new("child")?;
    let definition = nested_child_definition()?;
    let parameters = nested_job_parameters_fixture()?;

    let mut first = block_on(repository.begin())?;
    let link = block_on(first.create_nested_job_link(&NestedJobLinkRequest::new(
        parent_instance.id(),
        parent_execution.id(),
        node.clone(),
        definition.clone(),
        parameters,
        time(401),
    )))?;
    block_on(first.commit())?;

    let drifted = JobParameters::try_from_iter([
        (
            ParameterName::new("account")?,
            JobParameter::new(
                ParameterValue::string("acct-42")?,
                ParameterRole::Identifying,
            ),
        ),
        (
            ParameterName::new("trace")?,
            JobParameter::new(
                ParameterValue::string("remapped-differently")?,
                ParameterRole::NonIdentifying,
            ),
        ),
    ])?;
    let mut duplicate = block_on(repository.begin())?;
    assert_eq!(
        block_on(duplicate.create_nested_job_link(&NestedJobLinkRequest::new(
            parent_instance.id(),
            parent_execution.id(),
            node,
            definition,
            drifted,
            time(402),
        ))),
        Err(RepositoryError::NestedJobStateCorrupt)
    );
    let attempts = block_on(duplicate.job_executions(link.child_job_instance_id()))?;
    assert_eq!(attempts.len(), 1);
    block_on(duplicate.rollback())?;
    Ok(())
}
