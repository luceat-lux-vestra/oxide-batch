//! #594: pure M7 parameter incrementing never supplies hidden instance identity.
use std::error::Error;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use oxide_batch::{
    BoxFuture, ComponentRevision, DefinitionIdentity, DefinitionManifest, DefinitionRevision,
    FlowGraph, FlowJob, FlowLauncher, FlowNode, FlowRuntimeError, FlowTarget,
    InMemoryJobRepository, IncrementedParameters, IncrementerError, IncrementerField,
    JobInstanceKey, JobName, JobParameter, JobParameters, JobRepository, ManifestError, NodeId,
    ParameterIncrementer, ParameterIncrementerIdentity, ParameterName, ParameterRole,
    ParameterValue, ParameterValueKind, SequentialIdGenerator, StepComponents, StepName, StepNode,
    StopSource, SystemClock, Tasklet, TaskletContext, TaskletError, TaskletOutcome, TaskletStep,
    TerminalKind,
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
    let selected = incrementer(
        |_target, previous| Ok(previous.clone()),
        ParameterRole::Identifying,
    )?;
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

static MATCH_CALLBACKS: AtomicUsize = AtomicUsize::new(0);
static REJECTED_CALLBACKS: AtomicUsize = AtomicUsize::new(0);

fn counted_next_run(
    target: &DefinitionIdentity,
    previous: &JobParameters,
) -> Result<JobParameters, IncrementerError> {
    MATCH_CALLBACKS.fetch_add(1, Ordering::SeqCst);
    next_run(target, previous)
}

fn should_not_run(
    target: &DefinitionIdentity,
    previous: &JobParameters,
) -> Result<JobParameters, IncrementerError> {
    REJECTED_CALLBACKS.fetch_add(1, Ordering::SeqCst);
    next_run(target, previous)
}

struct CompleteTasklet;

impl Tasklet for CompleteTasklet {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async { Ok(TaskletOutcome::Completed) })
    }
}

fn incrementer_identity(revision: &str) -> Result<ParameterIncrementerIdentity, Box<dyn Error>> {
    Ok(ParameterIncrementerIdentity::new(
        DefinitionRevision::new("run-id")?,
        DefinitionRevision::new(revision)?,
    ))
}

fn flow_plan(
    name: &JobName,
    identity: Option<ParameterIncrementerIdentity>,
) -> Result<oxide_batch::CompiledExecutionPlan, Box<dyn Error>> {
    let only = NodeId::new("only")?;
    let mut graph = FlowGraph::new(only.clone()).with_node(FlowNode::step(StepNode::new(
        only.clone(),
        StepName::new("only")?,
        StepComponents::Tasklet(ComponentRevision::new("only-v1")?),
    )));
    if let Some(identity) = identity {
        graph = graph.with_parameter_incrementer_identity(identity);
    }
    Ok(graph
        .with_sequence(only, FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(name, DefinitionRevision::new("flow-v1")?)?)
}

fn flow_job(
    name: &JobName,
    identity: Option<ParameterIncrementerIdentity>,
) -> Result<FlowJob, Box<dyn Error>> {
    Ok(
        FlowJob::new(name.clone(), flow_plan(name, identity)?)?.with_tasklet_step(
            NodeId::new("only")?,
            TaskletStep::new(StepName::new("only")?, Arc::new(CompleteTasklet)),
        )?,
    )
}

#[test]
fn incrementer_identity_is_restart_relevant_only_when_explicitly_declared()
-> Result<(), Box<dyn Error>> {
    let name = JobName::new("incrementer-fingerprint")?;
    let legacy = flow_plan(&name, None)?;
    assert_eq!(legacy.manifest_format(), 2);
    assert!(legacy.parameter_incrementer_identity().is_none());

    let v1_identity = incrementer_identity("run-id-v1")?;
    let v1 = flow_plan(&name, Some(v1_identity.clone()))?;
    assert_eq!(v1.manifest_format(), 4);
    assert_eq!(v1.parameter_incrementer_identity(), Some(&v1_identity));
    assert_ne!(legacy.fingerprint(), v1.fingerprint());

    let v2_identity = incrementer_identity("run-id-v2")?;
    let v2 = flow_plan(&name, Some(v2_identity.clone()))?;
    assert_eq!(v2.parameter_incrementer_identity(), Some(&v2_identity));
    assert_ne!(v1.fingerprint(), v2.fingerprint());

    let encoded = String::from_utf8(v1.definition_identity().canonical_manifest().to_vec())?;
    assert!(
        encoded.contains(r#""parameter_incrementer":{"kind":"run-id","revision":"run-id-v1"}"#)
    );
    Ok(())
}

#[test]
fn malformed_format4_incrementer_identity_fails_closed() -> Result<(), Box<dyn Error>> {
    let name = JobName::new("incrementer-manifest-reader")?;
    let plan = flow_plan(&name, Some(incrementer_identity("run-id-v1")?))?;
    let mut document: serde_json::Value =
        serde_json::from_slice(plan.definition_identity().canonical_manifest())?;
    document
        .get_mut("parameter_incrementer")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or("missing incrementer identity")?
        .insert("unexpected".to_owned(), serde_json::json!(true));
    let malformed = serde_json::to_vec(&document)?;
    assert!(matches!(
        DefinitionManifest::read(&malformed),
        Err(ManifestError::MalformedGraph)
    ));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn matching_incrementer_identity_launches_exactly_one_derived_instance()
-> Result<(), Box<dyn Error>> {
    MATCH_CALLBACKS.store(0, Ordering::SeqCst);
    let name = JobName::new("incrementer-launch")?;
    let identity = incrementer_identity("run-id-v1")?;
    let job = flow_job(&name, Some(identity.clone()))?;
    let incrementer = incrementer(counted_next_run, ParameterRole::Identifying)?;
    assert_eq!(incrementer.identity(), &identity);

    let clock = Arc::new(SystemClock);
    let ids = Arc::new(SequentialIdGenerator::new(NonZeroU64::MIN));
    let repository = InMemoryJobRepository::new(clock.clone(), ids.clone());
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());
    let prior_parameters = prior(6)?;
    let expected = prior(7)?;
    let (_, stop) = StopSource::new();

    let report = launcher
        .increment_and_launch(&job, &incrementer, &prior_parameters, &stop)
        .await?;

    assert_eq!(MATCH_CALLBACKS.load(Ordering::SeqCst), 1);
    assert_eq!(
        report.instance().key(),
        &JobInstanceKey::new(name, &expected)
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn mismatched_or_undeclared_incrementer_fails_before_callback_and_lifecycle_write()
-> Result<(), Box<dyn Error>> {
    REJECTED_CALLBACKS.store(0, Ordering::SeqCst);
    let declared_name = JobName::new("incrementer-mismatch")?;
    let declared_job = flow_job(&declared_name, Some(incrementer_identity("run-id-v1")?))?;
    let mismatched = ParameterIncrementer::new(
        DefinitionRevision::new("run-id")?,
        DefinitionRevision::new("run-id-v2")?,
        vec![IncrementerField::new(
            ParameterName::new("run.id")?,
            ParameterValueKind::U64,
            ParameterRole::Identifying,
        )],
        should_not_run,
    )?;

    let clock = Arc::new(SystemClock);
    let ids = Arc::new(SequentialIdGenerator::new(NonZeroU64::MIN));
    let repository = InMemoryJobRepository::new(clock.clone(), ids.clone());
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());
    let prior_parameters = prior(3)?;
    let expected = prior(4)?;
    let (_, stop) = StopSource::new();

    assert_eq!(
        launcher
            .increment_and_launch(&declared_job, &mismatched, &prior_parameters, &stop)
            .await,
        Err(FlowRuntimeError::Incrementer(
            IncrementerError::IdentityMismatch
        ))
    );
    assert_eq!(REJECTED_CALLBACKS.load(Ordering::SeqCst), 0);

    let mut inspection = repository.begin().await?;
    assert!(
        inspection
            .find_job_instance(&JobInstanceKey::new(declared_name.clone(), &prior_parameters))
            .await?
            .is_none()
    );
    assert!(
        inspection
            .find_job_instance(&JobInstanceKey::new(declared_name, &expected))
            .await?
            .is_none()
    );
    inspection.rollback().await?;

    let undeclared_name = JobName::new("incrementer-undeclared")?;
    let undeclared_job = flow_job(&undeclared_name, None)?;
    assert_eq!(
        launcher
            .increment_and_launch(&undeclared_job, &mismatched, &prior_parameters, &stop)
            .await,
        Err(FlowRuntimeError::Incrementer(
            IncrementerError::IdentityMismatch
        ))
    );
    assert_eq!(REJECTED_CALLBACKS.load(Ordering::SeqCst), 0);

    let mut inspection = repository.begin().await?;
    assert!(
        inspection
            .find_job_instance(&JobInstanceKey::new(undeclared_name, &prior_parameters))
            .await?
            .is_none()
    );
    inspection.rollback().await?;
    Ok(())
}
