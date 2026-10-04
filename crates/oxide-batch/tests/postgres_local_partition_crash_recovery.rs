//! Process-kill and restart evidence for bounded `PostgreSQL` local partitions.

#![cfg(feature = "postgres")]
#![allow(clippy::panic)]

use std::error::Error;
use std::num::NonZeroU64;
use std::process::{Command, ExitStatus};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use oxide_batch::{
    BatchStatus, BoxFuture, Clock, ComponentRevision, DefinitionRevision, ExecutionContext,
    FailureCategory, FailureId, FlowExecutionOutcome, FlowGraph, FlowJob, FlowLauncher, FlowNode,
    FlowTarget, JobInstanceKey, JobName, JobParameters, JobRepository, NodeId, PartitionBudget,
    PartitionCount, PartitionFactoryError, PartitionKey, PartitionPlanEntry, PartitionPlanFactory,
    PartitionTaskletFactory, PostgresConfig, PostgresJobRepository, PostgresMigrator,
    RecoveryRequest, RepeatCallbackError, RepeatCommitRequest, RepeatContext, RepeatDecision,
    RepeatDefinition, RepeatId, RepeatOrdinal, RepeatPolicy, RepeatPolicyConfiguration,
    RepeatPolicyDefinition, RepeatPolicyKind, RepeatPolicyOutcome, RepeatPolicyRegistration,
    RepeatRuntimeRegistration, RepeatStateSchema, SequentialIdGenerator, StateLimits,
    StateSchemaId, StateSchemaVersion, StepComponents, StepName, StepNode, StopSource, Tasklet,
    TaskletContext, TaskletError, TaskletOutcome, TaskletStep, TerminalKind, TlsMode,
};
use sqlx::postgres::PgPoolOptions;

const JOB: &str = "postgres_m4_local_partition_crash";
const CRASH_MODE_ENV: &str = "OXIDEBATCH_M4_PARTITION_CRASH_MODE";
const CRASH_EXIT_CODE: i32 = 91;
const REPEAT_ID: &str = "partition-window";
const REPEAT_POLICY_KIND: &str = "partition-crash-count";
const REPEAT_POLICY_REVISION: &str = "partition-crash-policy-v1";
const REPEAT_STATE_SCHEMA: &str = "m7.partition.repeat.state";

#[derive(Clone, Copy)]
struct FixedClock(SystemTime);

impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        self.0
    }
}

struct PartitionRepeatPolicy;

impl RepeatPolicy for PartitionRepeatPolicy {
    fn decide<'a>(
        &'a self,
        context: RepeatContext<'a>,
    ) -> BoxFuture<'a, Result<RepeatPolicyOutcome, RepeatCallbackError>> {
        Box::pin(async move {
            let state = ExecutionContext::from_json(
                format!(
                    r#"{{"format":"oxide-batch.execution-context","format_version":1,"schema":"{REPEAT_STATE_SCHEMA}","schema_version":1,"payload":{{"ordinal":{}}}}}"#,
                    context.ordinal().get()
                )
                .as_bytes(),
                StateLimits::default(),
            )
            .map_err(RepeatCallbackError::from_error)?;
            if context.ordinal() == RepeatOrdinal::INITIAL {
                Ok(RepeatPolicyOutcome::continue_with(state))
            } else {
                Ok(RepeatPolicyOutcome::complete_with(state))
            }
        })
    }
}

fn partition_repeat_definition() -> Result<RepeatDefinition, Box<dyn Error>> {
    Ok(RepeatDefinition::new(
        RepeatId::new(REPEAT_ID)?,
        RepeatPolicyDefinition::new(
            RepeatPolicyKind::new(REPEAT_POLICY_KIND)?,
            ComponentRevision::new(REPEAT_POLICY_REVISION)?,
            RepeatPolicyConfiguration::new("limit-2")?,
        ),
        Vec::new(),
        RepeatStateSchema::new(
            StateSchemaId::new(REPEAT_STATE_SCHEMA)?,
            StateSchemaVersion::new(1)?,
        ),
    )?)
}

fn partition_repeat_registration() -> Result<RepeatRuntimeRegistration, Box<dyn Error>> {
    Ok(RepeatRuntimeRegistration::new(
        RepeatId::new(REPEAT_ID)?,
        RepeatPolicyRegistration::new(
            RepeatPolicyKind::new(REPEAT_POLICY_KIND)?,
            ComponentRevision::new(REPEAT_POLICY_REVISION)?,
            RepeatPolicyConfiguration::new("limit-2")?,
            Arc::new(PartitionRepeatPolicy),
        ),
        Vec::new(),
    ))
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

struct ExitProcessTasklet;

impl Tasklet for ExitProcessTasklet {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async { std::process::exit(CRASH_EXIT_CODE) })
    }
}

struct PanicIfInvokedTasklet;

impl Tasklet for PanicIfInvokedTasklet {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async { panic!("a completed partition was re-executed") })
    }
}

fn runtime_url() -> Option<String> {
    std::env::var("OXIDEBATCH_POSTGRES_TEST_URL").ok()
}

fn migrator_url() -> Option<String> {
    std::env::var("OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL").ok()
}

fn config(url: String) -> Result<PostgresConfig, Box<dyn Error>> {
    Ok(PostgresConfig::new(url)?.with_tls_mode(TlsMode::Plaintext))
}

fn entry(key: &str) -> Result<PartitionPlanEntry, Box<dyn Error>> {
    let context = ExecutionContext::from_json(
        format!(
            "{{\"format\":\"oxide-batch.execution-context\",\"format_version\":1,\"schema\":\"m4.crash\",\"schema_version\":1,\"payload\":{{\"key\":\"{key}\"}}}}"
        )
        .as_bytes(),
        StateLimits::new(4 * 1024, 16)?,
    )?;
    Ok(PartitionPlanEntry::new(PartitionKey::new(key)?, context)?)
}

fn job(crash: bool) -> Result<FlowJob, Box<dyn Error>> {
    let name = JobName::new(JOB)?;
    let manager = NodeId::new("partitioned")?;
    let worker_name = StepName::new("worker")?;
    let worker_node = NodeId::new("worker")?;
    let worker = StepNode::new(
        worker_node.clone(),
        worker_name.clone(),
        StepComponents::Tasklet(ComponentRevision::new("worker-v1")?),
    )
    .with_repeat_definition(partition_repeat_definition()?);
    let plan = FlowGraph::new(manager.clone())
        .with_node(FlowNode::partitioned_step(
            oxide_batch::PartitionedStepNode::new(
                manager.clone(),
                StepName::new("partitioned")?,
                worker,
                ComponentRevision::new("partitioner-v1")?,
                ComponentRevision::new("canonical-v1")?,
                PartitionCount::new(2)?,
                PartitionBudget::new(1, 2)?,
            ),
        ))
        .with_sequence(
            manager.clone(),
            FlowTarget::Terminal(TerminalKind::Complete),
        )?
        .compile(&name, DefinitionRevision::new("v1")?)?;
    let entries = vec![entry("alpha")?, entry("beta")?];
    let partitioner = PartitionPlanFactory::new(move |request| {
        if request.partition_count().get() != 2 {
            return Err(PartitionFactoryError::Rejected);
        }
        Ok(entries.clone())
    });
    let factory_name = worker_name.clone();
    let factory = PartitionTaskletFactory::new(worker_name, move |input| {
        let tasklet: Arc<dyn Tasklet> = match (crash, input.key().as_str()) {
            (true, "alpha") | (false, "beta") => Arc::new(CompleteTasklet),
            (true, "beta") => Arc::new(ExitProcessTasklet),
            _ => Arc::new(PanicIfInvokedTasklet),
        };
        TaskletStep::new(factory_name.clone(), tasklet)
    });
    Ok(FlowJob::new(name, plan)?
        .with_partitioned_tasklet(manager, partitioner, factory)?
        .with_repeat_registration(worker_node, partition_repeat_registration()?)?)
}

async fn remove_job(url: &str) -> Result<(), sqlx::Error> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    for statement in [
        "DELETE FROM oxide_batch.ob_step_partition WHERE step_execution_id IN (\
         SELECT step.id FROM oxide_batch.ob_step_execution step \
         JOIN oxide_batch.ob_job_execution execution ON execution.id = step.job_execution_id \
         JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = $1)",
        "DELETE FROM oxide_batch.ob_flow_decision WHERE job_execution_id IN (\
         SELECT execution.id FROM oxide_batch.ob_job_execution execution \
         JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = $1)",
        "DELETE FROM oxide_batch.ob_recovery_decision WHERE job_execution_id IN (\
         SELECT execution.id FROM oxide_batch.ob_job_execution execution \
         JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = $1)",
        "DELETE FROM oxide_batch.ob_step_execution WHERE job_execution_id IN (\
         SELECT execution.id FROM oxide_batch.ob_job_execution execution \
         JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = $1)",
        "DELETE FROM oxide_batch.ob_job_execution WHERE job_instance_id IN (\
         SELECT id FROM oxide_batch.ob_job_instance WHERE job_name = $1)",
        "DELETE FROM oxide_batch.ob_job_instance WHERE job_name = $1",
        "DELETE FROM oxide_batch.ob_job_definition WHERE job_name = $1",
    ] {
        sqlx::query(statement).bind(JOB).execute(&pool).await?;
    }
    pool.close().await;
    Ok(())
}

async fn run_crash_worker(url: String) -> Result<(), Box<dyn Error>> {
    let clock = FixedClock(SystemTime::UNIX_EPOCH + Duration::from_secs(8_000));
    let repository = PostgresJobRepository::connect(config(url)?, Arc::new(clock)).await?;
    let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
    let (_, stop) = StopSource::new();
    let _ = FlowLauncher::new(&repository, &clock, &ids)
        .launch(&job(true)?, &JobParameters::new(), &stop)
        .await?;
    Err("partition crash worker crossed the process-exit boundary".into())
}

fn spawn_crash_worker() -> Result<ExitStatus, Box<dyn Error>> {
    Ok(Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("local_partition_crash_worker_process")
        .arg("--nocapture")
        .env(CRASH_MODE_ENV, "1")
        .status()?)
}

async fn inspect_recover_restart(url: String, migrator_url: &str) -> Result<(), Box<dyn Error>> {
    let clock = FixedClock(SystemTime::UNIX_EPOCH + Duration::from_secs(8_001));
    let repository = PostgresJobRepository::connect(config(url)?, Arc::new(clock)).await?;
    let key = JobInstanceKey::new(JobName::new(JOB)?, &JobParameters::new());
    let mut inspect = repository.begin().await?;
    let instance = inspect
        .find_job_instance(&key)
        .await?
        .ok_or("crash worker did not create an instance")?;
    let original = inspect
        .job_executions(instance.id())
        .await?
        .into_iter()
        .next()
        .ok_or("crash worker did not create an execution")?;
    let original_steps = inspect.step_executions(original.id()).await?;
    let parent = original_steps
        .iter()
        .find(|step| step.step_name().as_str() == "partitioned")
        .ok_or("partition manager is missing")?;
    let original_plan = inspect.step_partition_plan(parent.id()).await?;
    inspect.rollback().await?;
    assert_eq!(original.metadata().status(), BatchStatus::Started);
    assert_eq!(original_plan[0].key().as_str(), "alpha");
    assert_eq!(original_plan[0].status(), BatchStatus::Completed);
    assert_eq!(original_plan[1].key().as_str(), "beta");
    assert_eq!(original_plan[1].status(), BatchStatus::Started);
    let alpha_worker = original_plan[0]
        .worker_step_execution_id()
        .ok_or("completed alpha worker is missing")?;
    let beta_worker = original_plan[1]
        .worker_step_execution_id()
        .ok_or("crashed beta worker is missing")?;

    let mut repeat_inspect = repository.begin().await?;
    let alpha_repeat = repeat_inspect
        .repeat_execution(alpha_worker, &RepeatId::new(REPEAT_ID)?)
        .await?
        .ok_or("completed alpha partition has no repeat state")?;
    let beta_repeat_before = repeat_inspect
        .repeat_execution(beta_worker, &RepeatId::new(REPEAT_ID)?)
        .await?;
    repeat_inspect.rollback().await?;
    assert_eq!(alpha_repeat.definition_node_id().as_str(), "worker");
    assert_ne!(alpha_repeat.node_id().as_str(), "worker");
    assert!(alpha_repeat.lineage().is_root());
    assert_eq!(alpha_repeat.ordinal(), RepeatOrdinal::new(1));
    assert_eq!(alpha_repeat.decision(), RepeatDecision::Complete);
    assert!(
        beta_repeat_before.is_none(),
        "crashed beta partition must not inherit or fabricate alpha repeat state"
    );

    // Exact repeat replay must remain bound to the durable partition assignment.
    // Removing only that assignment turns an otherwise byte-identical replay into
    // corruption instead of letting the dynamic worker claim the static definition.
    let alpha_replay = RepeatCommitRequest::new(
        instance.id(),
        alpha_repeat.node_id().clone(),
        alpha_worker,
        RepeatId::new(REPEAT_ID)?,
        alpha_repeat.ordinal(),
        alpha_repeat.state().clone(),
        alpha_repeat.decision(),
        *alpha_repeat.plan_fingerprint(),
    )
    .with_definition_node_id(alpha_repeat.definition_node_id().clone())
    .with_lineage(alpha_repeat.lineage().clone());
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(migrator_url)
        .await?;
    let detached = sqlx::query(
        "UPDATE oxide_batch.ob_step_partition          SET worker_step_execution_id = NULL          WHERE id = $1 AND worker_step_execution_id = $2",
    )
    .bind(i64::try_from(original_plan[0].id().get())?)
    .bind(i64::try_from(alpha_worker.get())?)
    .execute(&admin)
    .await?
    .rows_affected();
    assert_eq!(detached, 1, "alpha worker assignment was not detached");

    let mut spoof = repository.begin().await?;
    assert_eq!(
        spoof.commit_repeat_iteration(&alpha_replay).await,
        Err(oxide_batch::RepositoryError::RepeatStateCorrupt),
        "an exact repeat replay without its durable partition assignment must fail closed",
    );
    spoof.rollback().await?;

    let restored = sqlx::query(
        "UPDATE oxide_batch.ob_step_partition          SET worker_step_execution_id = $1          WHERE id = $2 AND worker_step_execution_id IS NULL",
    )
    .bind(i64::try_from(alpha_worker.get())?)
    .bind(i64::try_from(original_plan[0].id().get())?)
    .execute(&admin)
    .await?
    .rows_affected();
    assert_eq!(restored, 1, "alpha worker assignment was not restored");
    admin.close().await;

    let request = RecoveryRequest::mark_failed(
        original.version(),
        "PARTITION_PROCESS_EXIT_INSPECTED",
        "m4-local-partition-crash-harness",
        [80; 32],
        FailureCategory::PermanentInfrastructure,
        FailureId::new(8_001)?,
    )?;
    let mut recover = repository.begin().await?;
    recover
        .recover_job_execution(original.id(), &request)
        .await?;
    recover.commit().await?;

    let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
    let (_, stop) = StopSource::new();
    let report = FlowLauncher::new(&repository, &clock, &ids)
        .launch(&job(false)?, &JobParameters::new(), &stop)
        .await?;
    assert_eq!(report.outcome(), &FlowExecutionOutcome::Completed);
    let restarted_parent = report
        .step_executions()
        .iter()
        .find(|step| step.step_name().as_str() == "partitioned")
        .ok_or("restarted partition manager is missing")?;
    let mut verify = repository.begin().await?;
    let restarted_plan = verify.step_partition_plan(restarted_parent.id()).await?;
    let restarted_alpha_worker = restarted_plan[0]
        .worker_step_execution_id()
        .ok_or("restarted alpha worker is missing")?;
    let restarted_beta_worker = restarted_plan[1]
        .worker_step_execution_id()
        .ok_or("restarted beta worker is missing")?;
    let alpha_after = verify
        .repeat_execution(restarted_alpha_worker, &RepeatId::new(REPEAT_ID)?)
        .await?
        .ok_or("reused alpha worker lost repeat state")?;
    let beta_after = verify
        .repeat_execution(restarted_beta_worker, &RepeatId::new(REPEAT_ID)?)
        .await?
        .ok_or("restarted beta worker has no repeat state")?;
    verify.rollback().await?;
    assert!(
        restarted_plan
            .iter()
            .all(|partition| partition.status() == BatchStatus::Completed)
    );
    assert_eq!(
        restarted_plan[0].worker_step_execution_id(),
        Some(alpha_worker)
    );
    assert_ne!(
        restarted_plan[1].worker_step_execution_id(),
        original_plan[1].worker_step_execution_id()
    );
    assert_eq!(restarted_alpha_worker, alpha_worker);
    assert_eq!(alpha_after, alpha_repeat);
    assert_ne!(restarted_beta_worker, beta_worker);
    assert_eq!(beta_after.definition_node_id().as_str(), "worker");
    assert_ne!(beta_after.node_id(), alpha_after.node_id());
    assert!(beta_after.lineage().is_root());
    assert_eq!(beta_after.ordinal(), RepeatOrdinal::new(1));
    assert_eq!(beta_after.decision(), RepeatDecision::Complete);
    repository.close().await?;
    Ok(())
}

#[test]
fn local_partition_crash_worker_process() -> Result<(), Box<dyn Error>> {
    if std::env::var(CRASH_MODE_ENV).is_err() {
        return Ok(());
    }
    let url = runtime_url().ok_or("partition crash worker database URL is missing")?;
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run_crash_worker(url))
}

#[test]
fn committed_partition_is_reused_after_process_kill_and_recovery() -> Result<(), Box<dyn Error>> {
    let Some(runtime_url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let Some(migrator_url) = migrator_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        PostgresMigrator::migrate(&config(migrator_url.clone())?).await?;
        remove_job(&migrator_url).await?;
        Ok::<(), Box<dyn Error>>(())
    })?;
    assert_eq!(spawn_crash_worker()?.code(), Some(CRASH_EXIT_CODE));
    runtime.block_on(inspect_recover_restart(runtime_url.clone(), &migrator_url))?;
    runtime.block_on(remove_job(&migrator_url))?;
    Ok(())
}
