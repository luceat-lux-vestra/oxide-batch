//! #594: pure M7 parameter incrementing never supplies hidden instance identity.
use std::error::Error;

use oxide_batch::{
    ComponentRevision, DefinitionIdentity, DefinitionRevision, IncrementedParameters,
    IncrementerError, IncrementerField, JobInstanceKey, JobName, JobParameter, JobParameters,
    ParameterIncrementer, ParameterName, ParameterRole, ParameterValue, ParameterValueKind,
    StepName,
};

fn definition() -> Result<DefinitionIdentity, Box<dyn Error>> {
    let job_name = JobName::new("incremented-job")?;
    Ok(DefinitionIdentity::tasklet(
        &job_name,
        &StepName::new("only")?,
        DefinitionRevision::new("v1")?,
        &ComponentRevision::new("component-v1")?,
    )?)
}

fn next_run(
    _target: &DefinitionIdentity,
    previous: &JobParameters,
) -> Result<JobParameters, IncrementerError> {
    let name = ParameterName::new("run.id").map_err(|_| IncrementerError::ApplicationRejected)?;
    let current = previous
        .get(&name)
        .map(|parameter| {
            parameter
                .value()
                .as_u64()
                .ok_or(IncrementerError::ApplicationRejected)
        })
        .transpose()?
        .unwrap_or(0);
    let value = current
        .checked_add(1)
        .ok_or(IncrementerError::ApplicationRejected)?;
    let mut result = JobParameters::try_from_iter(
        previous
            .iter()
            .filter(|(name, _)| name.as_str() != "run.id")
            .map(|(name, parameter)| (name.clone(), parameter.clone())),
    )
    .map_err(|_| IncrementerError::ApplicationRejected)?;
    result
        .insert(
            name,
            JobParameter::new(value.into(), ParameterRole::Identifying),
        )
        .map_err(|_| IncrementerError::ApplicationRejected)?;
    Ok(result)
}

fn unchanged(
    _target: &DefinitionIdentity,
    previous: &JobParameters,
) -> Result<JobParameters, IncrementerError> {
    Ok(previous.clone())
}

fn role_drift(
    _target: &DefinitionIdentity,
    _previous: &JobParameters,
) -> Result<JobParameters, IncrementerError> {
    let mut result = JobParameters::new();
    let name = ParameterName::new("run.id").map_err(|_| IncrementerError::ApplicationRejected)?;
    result
        .insert(
            name,
            JobParameter::new(ParameterValue::from(4_u64), ParameterRole::NonIdentifying),
        )
        .map_err(|_| IncrementerError::ApplicationRejected)?;
    Ok(result)
}

fn incrementer(
    apply: fn(&DefinitionIdentity, &JobParameters) -> Result<JobParameters, IncrementerError>,
    role: ParameterRole,
) -> Result<ParameterIncrementer, Box<dyn Error>> {
    Ok(ParameterIncrementer::new(
        DefinitionRevision::new("run-id")?,
        DefinitionRevision::new("run-id-v1")?,
        vec![IncrementerField::new(
            ParameterName::new("run.id")?,
            ParameterValueKind::U64,
            role,
        )],
        apply,
    )?)
}

fn prior(value: u64) -> Result<JobParameters, Box<dyn Error>> {
    Ok(JobParameters::try_from_iter([(
        ParameterName::new("run.id")?,
        JobParameter::new(value.into(), ParameterRole::Identifying),
    )])?)
}

#[test]
fn deterministic_identifying_increment_recomputes_key_without_mutating_prior()
-> Result<(), Box<dyn Error>> {
    let target = definition()?;
    let before = prior(6)?;
    let selected = incrementer(next_run, ParameterRole::Identifying)?;
    let first = selected.increment(&target, &before)?;
    let second = selected.increment(&target, &before)?;
    assert_eq!(first, second);
    assert!(first.identifying_changed());
    assert_eq!(
        before
            .get(&ParameterName::new("run.id")?)
            .and_then(|x| x.value().as_u64()),
        Some(6)
    );
    assert_eq!(
        first
            .parameters()
            .get(&ParameterName::new("run.id")?)
            .and_then(|x| x.value().as_u64()),
        Some(7)
    );
    assert_eq!(
        first.target_key(),
        &JobInstanceKey::new(JobName::new("incremented-job")?, first.parameters())
    );
    assert_ne!(
        first.target_key(),
        &JobInstanceKey::new(JobName::new("incremented-job")?, &before)
    );
    assert!(!format!("{selected:?}").contains("run.id"));
    assert!(!format!("{first:?}").contains("run.id"));
    Ok(())
}

#[test]
fn unchanged_identity_does_not_spin_or_invent_an_increment() -> Result<(), Box<dyn Error>> {
    let before = prior(4)?;
    let selected = incrementer(unchanged, ParameterRole::Identifying)?;
    let proposal: IncrementedParameters = selected.increment(&definition()?, &before)?;
    assert!(!proposal.identifying_changed());
    assert_eq!(proposal.parameters(), &before);
    Ok(())
}

#[test]
fn fails_closed_on_undeclared_or_changed_identifying_role() -> Result<(), Box<dyn Error>> {
    let selected = incrementer(role_drift, ParameterRole::NonIdentifying)?;
    let prior = prior(3)?;
    assert_eq!(
        selected.increment(&definition()?, &prior),
        Err(IncrementerError::InputRoleOrKindDrift)
    );
    let incorrect = incrementer(next_run, ParameterRole::NonIdentifying)?;
    assert_eq!(
        incorrect.increment(&definition()?, &prior),
        Err(IncrementerError::InvalidOutputShape)
    );
    Ok(())
}

#[test]
fn rejects_excess_and_duplicate_fields_before_application_code() -> Result<(), Box<dyn Error>> {
    let name = ParameterName::new("run.id")?;
    let field = IncrementerField::new(name, ParameterValueKind::U64, ParameterRole::Identifying);
    assert_eq!(
        ParameterIncrementer::new(
            DefinitionRevision::new("run-id")?,
            DefinitionRevision::new("v1")?,
            vec![field.clone(), field.clone()],
            next_run,
        )
        .err(),
        Some(IncrementerError::DuplicateField)
    );
    assert_eq!(
        ParameterIncrementer::new(
            DefinitionRevision::new("run-id")?,
            DefinitionRevision::new("v1")?,
            vec![field; 129],
            next_run,
        )
        .err(),
        Some(IncrementerError::TooManyFields)
    );
    assert!(!format!("{}", IncrementerError::ApplicationRejected).contains("run.id"));
    Ok(())
}

#[test]
fn detects_overflow_without_mutation_or_payload_diagnostics() -> Result<(), Box<dyn Error>> {
    let selected = incrementer(next_run, ParameterRole::Identifying)?;
    let before = prior(u64::MAX)?;
    assert_eq!(
        selected.increment(&definition()?, &before),
        Err(IncrementerError::ApplicationRejected),
    );
    assert_eq!(
        before
            .get(&ParameterName::new("run.id")?)
            .and_then(|x| x.value().as_u64()),
        Some(u64::MAX)
    );
    Ok(())
}

#[test]
fn excessive_prior_parameter_set_is_rejected_before_callback() -> Result<(), Box<dyn Error>> {
    let mut many = JobParameters::new();
    for i in 0..129_u16 {
        many.insert(
            ParameterName::new(format!("field-{i}"))?,
            JobParameter::new(
                ParameterValue::from(u64::from(i)),
                ParameterRole::NonIdentifying,
            ),
        )?;
    }
    let selected = incrementer(next_run, ParameterRole::Identifying)?;
    assert_eq!(
        selected.increment(&definition()?, &many),
        Err(IncrementerError::TooManyFields)
    );
    Ok(())
}
