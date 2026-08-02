//! Process-kill and restart evidence for bounded `PostgreSQL` parallel splits.

#![cfg(feature = "postgres")]
#![allow(clippy::panic)]

use std::error::Error;
use std::num::NonZeroU64;
use std::process::{Command, ExitStatus};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use oxide_batch::{
    BatchStatus, BoxFuture, Clock, ComponentRevision, DefinitionRevision, FailureCategory,
    FailureId, FlowExecutionOutcome, FlowGraph, FlowJob, FlowLauncher, FlowNode, FlowTarget,
    FlowTransitionKind, JobInstanceKey, JobName, JobParameters, JobRepository, JoinNode, NodeId,
    PostgresConfig, PostgresJobRepository, PostgresMigrator, RecoveryRequest,
    SequentialIdGenerator, SplitBranch, SplitBudget, SplitNode, StepComponents, StepName, StepNode,
    StopSource, Tasklet, TaskletContext, TaskletError, TaskletOutcome, TaskletStep,
    TaskletStepFactory, TerminalKind, TlsMode,
};
use sqlx::postgres::PgPoolOptions;

const JOB: &str = "postgres_m4_local_split_crash";
const CRASH_MODE_ENV: &str = "OXIDEBATCH_M4_SPLIT_CRASH_MODE";
const CRASH_EXIT_CODE: i32 = 92;

#[derive(Clone, Copy)]
struct FixedClock(SystemTime);

impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        self.0
    }
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
        Box::pin(async { panic!("a completed branch was re-executed") })
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

fn step_node(id: &str) -> Result<StepNode, Box<dyn Error>> {
    Ok(StepNode::new(
        NodeId::new(id)?,
        StepName::new(id)?,
        StepComponents::Tasklet(ComponentRevision::new(format!("{id}-v1"))?),
    ))
}

fn branch_factory(
    name: &str,
    tasklet: Arc<dyn Tasklet>,
) -> Result<TaskletStepFactory, Box<dyn Error>> {
    let step_name = StepName::new(name)?;
    let factory_name = step_name.clone();
    Ok(TaskletStepFactory::new(step_name, move || {
        TaskletStep::new(factory_name.clone(), Arc::clone(&tasklet))
    }))
}

/// Builds the crash or restart binding for a two-branch split.
///
/// `MaxParallelBranches` is `1`, so the declared branch order is also the
/// execution order: `first` commits before `second` reaches the crash point.
fn job(crash: bool) -> Result<FlowJob, Box<dyn Error>> {
    let name = JobName::new(JOB)?;
    let prepare = NodeId::new("prepare")?;
    let split = NodeId::new("parallel")?;
    let join = NodeId::new("joined")?;
    let plan = FlowGraph::new(prepare.clone())
        .with_node(FlowNode::step(step_node("prepare")?))
        .with_node(FlowNode::split(SplitNode::new(
            split.clone(),
            vec![
                SplitBranch::new(vec![step_node("first")?]),
                SplitBranch::new(vec![step_node("second")?]),
            ],
            join.clone(),
            SplitBudget::new(1, 2)?,
        )))
        .with_node(FlowNode::join(JoinNode::new(join.clone())))
        .with_sequence(prepare.clone(), FlowTarget::Node(split))?
        .with_sequence(join, FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(&name, DefinitionRevision::new("v1")?)?;

    let first: Arc<dyn Tasklet> = if crash {
        Arc::new(CompleteTasklet)
    } else {
        Arc::new(PanicIfInvokedTasklet)
    };
    let second: Arc<dyn Tasklet> = if crash {
        Arc::new(ExitProcessTasklet)
    } else {
        Arc::new(CompleteTasklet)
    };
    let prepare_tasklet: Arc<dyn Tasklet> = if crash {
        Arc::new(CompleteTasklet)
    } else {
        Arc::new(PanicIfInvokedTasklet)
    };
    Ok(FlowJob::new(name, plan)?
        .with_tasklet_step(
            prepare,
            TaskletStep::new(StepName::new("prepare")?, prepare_tasklet),
        )?
        .with_split_tasklet_factory(NodeId::new("first")?, branch_factory("first", first)?)?
        .with_split_tasklet_factory(NodeId::new("second")?, branch_factory("second", second)?)?)
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
    let clock = FixedClock(SystemTime::UNIX_EPOCH + Duration::from_secs(9_100));
    let repository = PostgresJobRepository::connect(config(url)?, Arc::new(clock)).await?;
    let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
    let (_, stop) = StopSource::new();
    let _ = FlowLauncher::new(&repository, &clock, &ids)
        .launch(&job(true)?, &JobParameters::new(), &stop)
        .await?;
    Err("split crash worker crossed the process-exit boundary".into())
}

fn spawn_crash_worker() -> Result<ExitStatus, Box<dyn Error>> {
    Ok(Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("local_split_crash_worker_process")
        .arg("--nocapture")
        .env(CRASH_MODE_ENV, "1")
        .status()?)
}

async fn inspect_recover_restart(url: String) -> Result<(), Box<dyn Error>> {
    let clock = FixedClock(SystemTime::UNIX_EPOCH + Duration::from_secs(9_101));
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
    let original_decisions = inspect.flow_decisions(original.id()).await?;
    inspect.rollback().await?;

    // The process exited after the first branch committed and before the join
    // aggregate could be selected.
    assert_eq!(original.metadata().status(), BatchStatus::Started);
    let durable_status = |name: &str| {
        original_steps
            .iter()
            .find(|step| step.step_name().as_str() == name)
            .map(|step| step.metadata().status())
    };
    assert_eq!(durable_status("prepare"), Some(BatchStatus::Completed));
    assert_eq!(durable_status("first"), Some(BatchStatus::Completed));
    assert_eq!(durable_status("second"), Some(BatchStatus::Started));
    assert!(
        !original_decisions
            .iter()
            .any(|decision| decision.kind() == FlowTransitionKind::SplitAggregate)
    );
    let completed_branch = original_steps
        .iter()
        .find(|step| step.step_name().as_str() == "first")
        .ok_or("completed branch is missing")?
        .id();

    let request = RecoveryRequest::mark_failed(
        original.version(),
        "SPLIT_PROCESS_EXIT_INSPECTED",
        "m4-local-split-crash-harness",
        [92; 32],
        FailureCategory::PermanentInfrastructure,
        FailureId::new(9_101)?,
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

    // `prepare` and `first` are durable and are not re-executed; their restart
    // bindings would panic if they were. Only `second` runs a new attempt.
    assert_eq!(report.outcome(), &FlowExecutionOutcome::Completed);
    let restarted_names = report
        .step_executions()
        .iter()
        .map(|step| step.step_name().as_str().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(restarted_names, vec!["second".to_owned()]);
    assert!(
        report
            .decisions()
            .iter()
            .any(|decision| decision.kind() == FlowTransitionKind::SplitAggregate)
    );

    let mut verify = repository.begin().await?;
    let reused = verify
        .get_step_execution(completed_branch)
        .await?
        .ok_or("the completed branch attempt disappeared")?;
    let restarted_second = verify
        .step_executions(report.job_execution().id())
        .await?
        .into_iter()
        .find(|step| step.step_name().as_str() == "second")
        .ok_or("the restarted branch attempt is missing")?;
    verify.rollback().await?;
    assert_eq!(reused.metadata().status(), BatchStatus::Completed);
    assert_eq!(reused.job_execution_id(), original.id());
    assert_eq!(restarted_second.metadata().status(), BatchStatus::Completed);
    assert_ne!(restarted_second.job_execution_id(), original.id());
    repository.close().await?;
    Ok(())
}

#[test]
fn local_split_crash_worker_process() -> Result<(), Box<dyn Error>> {
    if std::env::var(CRASH_MODE_ENV).is_err() {
        return Ok(());
    }
    let url = runtime_url().ok_or("split crash worker database URL is missing")?;
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run_crash_worker(url))
}

#[test]
fn committed_branch_is_reused_after_process_kill_and_recovery() -> Result<(), Box<dyn Error>> {
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
    runtime.block_on(inspect_recover_restart(runtime_url.clone()))?;
    runtime.block_on(remove_job(&migrator_url))?;
    Ok(())
}
