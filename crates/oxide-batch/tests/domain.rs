//! Public domain-value, identity, execution-record, and redaction contracts.

use std::time::{Duration, UNIX_EPOCH};

use oxide_batch::{
    BatchStatus, DomainError, ExecutionCounts, ExecutionMetadata, ExecutionTimestamps, ExitCode,
    ExitStatus, FailureCategory, FailureId, FailureSummary, IdentifierKind, JobExecution,
    JobExecutionId, JobInstance, JobInstanceId, JobInstanceKey, JobName, JobParameter,
    JobParameters, NameKind, ParameterName, ParameterRole, ParameterValue, ParameterValueKind,
    StepExecution, StepExecutionId, StepName,
};

fn identifying_string(value: &str) -> Result<JobParameter, DomainError> {
    Ok(JobParameter::new(
        ParameterValue::string(value)?,
        ParameterRole::Identifying,
    ))
}

fn parameter_set_in_order(order: &[usize]) -> Result<JobParameters, DomainError> {
    let entries = [
        ("business_date", identifying_string("2026-07-29")?),
        (
            "sequence",
            JobParameter::new(ParameterValue::from(42_u64), ParameterRole::Identifying),
        ),
        (
            "dry_run",
            JobParameter::new(ParameterValue::from(false), ParameterRole::NonIdentifying),
        ),
    ];

    let mut parameters = JobParameters::new();
    for index in order {
        let (name, parameter) = &entries[*index];
        parameters.insert(ParameterName::new(*name)?, parameter.clone())?;
    }
    Ok(parameters)
}

#[test]
fn validated_names_and_ids_reject_invalid_construction() -> Result<(), DomainError> {
    assert_eq!(
        JobName::new(""),
        Err(DomainError::EmptyName {
            kind: NameKind::Job
        })
    );
    assert_eq!(
        StepName::new(" import"),
        Err(DomainError::NameHasSurroundingWhitespace {
            kind: NameKind::Step
        })
    );
    assert_eq!(
        ParameterName::new("input\npath"),
        Err(DomainError::NameContainsControl {
            kind: NameKind::Parameter,
            character_index: 5
        })
    );
    assert_eq!(
        JobExecutionId::new(0),
        Err(DomainError::ZeroIdentifier {
            kind: IdentifierKind::JobExecution
        })
    );
    assert_eq!(
        ParameterValue::string("s".repeat(65_537)),
        Err(DomainError::ParameterStringTooLong { max_bytes: 65_536 })
    );

    let instance_id = JobInstanceId::new(7)?;
    let execution_id = JobExecutionId::new(7)?;
    let step_execution_id = StepExecutionId::new(7)?;
    assert_eq!(instance_id.get(), execution_id.get());
    assert_eq!(execution_id.get(), step_execution_id.get());

    Ok(())
}

#[test]
fn job_instance_same_identifying_parameters() -> Result<(), DomainError> {
    let expected = JobInstanceKey::new(
        JobName::new("daily_import")?,
        &parameter_set_in_order(&[0, 1, 2])?,
    );

    for order in [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ] {
        let actual = JobInstanceKey::new(
            JobName::new("daily_import")?,
            &parameter_set_in_order(&order)?,
        );
        assert_eq!(actual, expected);
    }

    let fields = expected
        .identifying_fields()
        .map(|(name, kind)| (name.as_str(), kind))
        .collect::<Vec<_>>();
    assert_eq!(
        fields,
        vec![
            ("business_date", ParameterValueKind::String),
            ("sequence", ParameterValueKind::U64),
        ]
    );

    Ok(())
}

#[test]
fn instance_identity_uses_typed_values_not_redacted_display() -> Result<(), DomainError> {
    let parameter_name = ParameterName::new("sequence")?;
    let string_value = ParameterValue::string("42")?;
    let integer_value = ParameterValue::from(42_i64);
    assert_eq!(string_value.to_string(), integer_value.to_string());

    let string_parameters = JobParameters::try_from_iter([(
        parameter_name.clone(),
        JobParameter::new(string_value, ParameterRole::Identifying),
    )])?;
    let integer_parameters = JobParameters::try_from_iter([(
        parameter_name,
        JobParameter::new(integer_value, ParameterRole::Identifying),
    )])?;

    let string_key = JobInstanceKey::new(JobName::new("daily_import")?, &string_parameters);
    let integer_key = JobInstanceKey::new(JobName::new("daily_import")?, &integer_parameters);
    assert_ne!(string_key, integer_key);

    Ok(())
}

#[test]
fn non_identifying_parameters_do_not_change_instance_identity() -> Result<(), DomainError> {
    let mut first = parameter_set_in_order(&[0, 1])?;
    first.insert(
        ParameterName::new("request_id")?,
        JobParameter::new(
            ParameterValue::string("request-one")?,
            ParameterRole::NonIdentifying,
        ),
    )?;

    let mut second = parameter_set_in_order(&[1, 0])?;
    second.insert(
        ParameterName::new("request_id")?,
        JobParameter::new(
            ParameterValue::string("request-two")?,
            ParameterRole::NonIdentifying,
        ),
    )?;

    assert_eq!(
        JobInstanceKey::new(JobName::new("daily_import")?, &first),
        JobInstanceKey::new(JobName::new("daily_import")?, &second)
    );

    Ok(())
}

#[test]
fn duplicate_parameters_are_rejected_without_replacement() -> Result<(), DomainError> {
    let name = ParameterName::new("business_date")?;
    let mut parameters = JobParameters::new();
    parameters.insert(name.clone(), identifying_string("2026-07-29")?)?;

    assert_eq!(
        parameters.insert(name.clone(), identifying_string("2026-07-30")?),
        Err(DomainError::DuplicateParameter)
    );
    assert_eq!(
        parameters
            .get(&name)
            .and_then(|parameter| parameter.value().as_str()),
        Some("2026-07-29")
    );

    Ok(())
}

#[test]
fn parameter_debug_output_redacts_names_and_values() -> Result<(), DomainError> {
    let sentinel = "sentinel-secret-42";
    let name = ParameterName::new("sensitive_parameter_name")?;
    let parameter = identifying_string(sentinel)?;
    let mut parameters = JobParameters::new();
    parameters.insert(name.clone(), parameter.clone())?;
    let key = JobInstanceKey::new(JobName::new("daily_import")?, &parameters);
    let instance = JobInstance::new(JobInstanceId::new(1)?, key.clone());

    let diagnostics = [
        format!("{name:?}"),
        format!("{:?}", parameter.value()),
        format!("{parameter:?}"),
        format!("{parameters:?}"),
        format!("{key:?}"),
        format!("{instance:?}"),
        parameter.value().to_string(),
    ]
    .join("\n");

    assert!(!diagnostics.contains(sentinel));
    assert!(!diagnostics.contains("sensitive_parameter_name"));
    assert!(diagnostics.contains("<redacted>"));

    Ok(())
}

#[test]
fn execution_records_expose_structured_redacted_metadata() -> Result<(), DomainError> {
    let created_at = UNIX_EPOCH + Duration::from_secs(100);
    let started_at = UNIX_EPOCH + Duration::from_secs(101);
    let ended_at = UNIX_EPOCH + Duration::from_secs(105);
    let timestamps = ExecutionTimestamps::new(created_at, Some(started_at), Some(ended_at))?;
    let counts = ExecutionCounts::new(10, 9, 8, 1, 2, 1);
    let failure = FailureSummary::new(FailureCategory::UserComponent, FailureId::new(9001)?);
    let metadata = ExecutionMetadata::new(
        BatchStatus::Failed,
        ExitStatus::new(ExitCode::new("FAILED")?),
        timestamps,
        counts,
        Some(failure),
    )?;
    let job_execution = JobExecution::new(
        JobExecutionId::new(11)?,
        JobInstanceId::new(10)?,
        metadata.clone(),
    );
    let step_execution = StepExecution::new(
        StepExecutionId::new(12)?,
        job_execution.id(),
        StepName::new("import")?,
        metadata,
    );

    assert_eq!(job_execution.metadata().status(), BatchStatus::Failed);
    assert_eq!(
        job_execution.metadata().exit_status().code().as_str(),
        "FAILED"
    );
    assert_eq!(
        job_execution.metadata().timestamps().created_at(),
        created_at
    );
    assert_eq!(job_execution.metadata().counts().read(), 10);
    assert_eq!(job_execution.metadata().counts().processed(), 9);
    assert_eq!(job_execution.metadata().counts().written(), 8);
    assert_eq!(job_execution.metadata().counts().filtered(), 1);
    assert_eq!(job_execution.metadata().counts().committed(), 2);
    assert_eq!(job_execution.metadata().counts().rolled_back(), 1);
    assert_eq!(
        job_execution
            .metadata()
            .failure()
            .map(FailureSummary::category),
        Some(FailureCategory::UserComponent)
    );
    assert_eq!(step_execution.job_execution_id(), job_execution.id());
    assert_eq!(step_execution.step_name().as_str(), "import");

    Ok(())
}

#[test]
fn execution_metadata_rejects_inconsistent_states() -> Result<(), DomainError> {
    let created_at = UNIX_EPOCH + Duration::from_secs(100);
    let ended_at = UNIX_EPOCH + Duration::from_secs(101);
    let active_timestamps = ExecutionTimestamps::new(created_at, None, Some(ended_at))?;
    assert_eq!(
        ExecutionMetadata::new(
            BatchStatus::Started,
            ExitStatus::unknown(),
            active_timestamps,
            ExecutionCounts::default(),
            None,
        ),
        Err(DomainError::ActiveExecutionHasEndTime)
    );

    let unfinished_timestamps = ExecutionTimestamps::new(created_at, None, None)?;
    assert_eq!(
        ExecutionMetadata::new(
            BatchStatus::Completed,
            ExitStatus::new(ExitCode::new("COMPLETED")?),
            unfinished_timestamps,
            ExecutionCounts::default(),
            None,
        ),
        Err(DomainError::FinishedExecutionMissingEndTime)
    );
    assert_eq!(
        ExecutionMetadata::new(
            BatchStatus::Failed,
            ExitStatus::new(ExitCode::new("FAILED")?),
            ExecutionTimestamps::new(created_at, None, Some(ended_at))?,
            ExecutionCounts::default(),
            None,
        ),
        Err(DomainError::FailedExecutionMissingFailure)
    );
    assert_eq!(
        ExecutionTimestamps::new(ended_at, Some(created_at), None),
        Err(DomainError::InvalidTimestampOrder)
    );

    Ok(())
}
