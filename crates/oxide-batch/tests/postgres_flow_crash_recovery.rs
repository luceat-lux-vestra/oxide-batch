//! Process-kill evidence for the M3 durable flow-decision boundary.

#![cfg(feature = "postgres")]

use std::error::Error;
use std::num::NonZeroU64;
use std::process::{Command, ExitStatus};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

use oxide_batch::{
    BatchStatus, BoxFuture, Clock, ComponentRevision, DefinitionRevision, FailureCategory,
    FailureId, FlowEvent, FlowEventKind, FlowEventSink, FlowExecutionOutcome, FlowGraph, FlowJob,
    FlowLauncher, FlowNode, FlowTarget, FlowTransitionKind, JobInstanceKey, JobName, JobParameters,
    JobRepository, NodeId, PostgresConfig, PostgresMigrator, RecoveryRequest,
    SequentialIdGenerator, StepComponents, StepName, StepNode, StopSource, Tasklet, TaskletContext,
    TaskletError, TaskletOutcome, TaskletStep, TerminalKind, TlsMode,
};
use sqlx::postgres::PgPoolOptions;

const CRASH_MODE_ENV: &str = "OXIDEBATCH_M3_FLOW_CRASH_MODE";
const CRASH_EXIT_CODE: i32 = 87;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CrashPoint {
    BeforeDecisionCommit,
    AfterDecisionCommit,
}

impl CrashPoint {
    const fn job_name(self) -> &'static str {
        match self {
            Self::BeforeDecisionCommit => "m3_flow_crash_before_decision",
            Self::AfterDecisionCommit => "m3_flow_crash_after_decision",
        }
    }

    const fn environment_value(self) -> &'static str {
        match self {
            Self::BeforeDecisionCommit => "before-decision-commit",
            Self::AfterDecisionCommit => "after-decision-commit",
        }
    }

    fn parse(value: &str) -> Result<Self, Box<dyn Error>> {
        match value {
            "before-decision-commit" => Ok(Self::BeforeDecisionCommit),
            "after-decision-commit" => Ok(Self::AfterDecisionCommit),
            _ => Err("unknown M3 flow crash mode".into()),
        }
    }
}

#[derive(Clone, Copy)]
struct FixedClock(SystemTime);

impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        self.0
    }
}

struct CountingTasklet(Arc<AtomicUsize>);

impl Tasklet for CountingTasklet {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(TaskletOutcome::Completed)
        })
    }
}

struct CrashEventSink(CrashPoint);

impl FlowEventSink for CrashEventSink {
    fn emit(&self, event: &FlowEvent) {
        if event.source_node_id().as_str() != "source" {
            return;
        }
        let should_exit = matches!(
            (self.0, event.kind()),
            (
                CrashPoint::BeforeDecisionCommit,
                FlowEventKind::StepResultCommitted
            ) | (
                CrashPoint::AfterDecisionCommit,
                FlowEventKind::DecisionCommitted
            )
        );
        if should_exit {
            std::process::exit(CRASH_EXIT_CODE);
        }
    }
}

fn runtime_url() -> Option<String> {
    std::env::var("OXIDEBATCH_POSTGRES_TEST_URL").ok()
}

fn migrator_url() -> Option<String> {
    std::env::var("OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL").ok()
}

fn plaintext_config(url: String) -> Result<PostgresConfig, Box<dyn Error>> {
    Ok(PostgresConfig::new(url)?.with_tls_mode(TlsMode::Plaintext))
}

fn flow_job(
    job_name: &str,
    source_calls: Arc<AtomicUsize>,
    target_calls: Arc<AtomicUsize>,
) -> Result<FlowJob, Box<dyn Error>> {
    let source = NodeId::new("source")?;
    let target = NodeId::new("target")?;
    let node = |id: &NodeId| -> Result<FlowNode, Box<dyn Error>> {
        Ok(FlowNode::step(StepNode::new(
            id.clone(),
            StepName::new(id.as_str())?,
            StepComponents::Tasklet(ComponentRevision::new(format!("{}-v1", id.as_str()))?),
        )))
    };
    let name = JobName::new(job_name)?;
    let plan = FlowGraph::new(source.clone())
        .with_node(node(&source)?)
        .with_node(node(&target)?)
        .with_sequence(source.clone(), FlowTarget::Node(target.clone()))?
        .with_sequence(target.clone(), FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(&name, DefinitionRevision::new("m3-flow-crash-v1")?)?;
    Ok(FlowJob::new(name, plan)?
        .with_tasklet_step(
            source,
            TaskletStep::new(
                StepName::new("source")?,
                Arc::new(CountingTasklet(source_calls)),
            ),
        )?
        .with_tasklet_step(
            target,
            TaskletStep::new(
                StepName::new("target")?,
                Arc::new(CountingTasklet(target_calls)),
            ),
        )?)
}

async fn remove_job(url: &str, job_name: &str) -> Result<(), sqlx::Error> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    for statement in [
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
        "DELETE FROM oxide_batch.ob_definition_upgrade WHERE from_definition_id IN (\
         SELECT id FROM oxide_batch.ob_job_definition WHERE job_name = $1)",
        "DELETE FROM oxide_batch.ob_job_definition WHERE job_name = $1",
    ] {
        sqlx::query(statement).bind(job_name).execute(&pool).await?;
    }
    pool.close().await;
    Ok(())
}

async fn run_crash_worker(point: CrashPoint, url: String) -> Result<(), Box<dyn Error>> {
    let clock = FixedClock(SystemTime::UNIX_EPOCH + Duration::from_secs(4_000));
    let repository =
        oxide_batch::PostgresJobRepository::connect(plaintext_config(url)?, Arc::new(clock))
            .await?;
    let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
    let job = flow_job(
        point.job_name(),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
    )?;
    let (_, stop) = StopSource::new();
    let sink = CrashEventSink(point);
    let _ = FlowLauncher::new(&repository, &clock, &ids)
        .with_event_sink(&sink)
        .launch(&job, &JobParameters::new(), &stop)
        .await?;
    Err("flow crash worker crossed the selected process-exit boundary".into())
}

fn spawn_crash_worker(point: CrashPoint) -> Result<ExitStatus, Box<dyn Error>> {
    Ok(Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("flow_crash_worker_process")
        .arg("--nocapture")
        .env(CRASH_MODE_ENV, point.environment_value())
        .status()?)
}

#[allow(
    clippy::too_many_lines,
    reason = "crash inspection, audited recovery, and deterministic restart form one evidence chain"
)]
async fn inspect_recover_and_restart(point: CrashPoint, url: String) -> Result<(), Box<dyn Error>> {
    let clock = FixedClock(SystemTime::UNIX_EPOCH + Duration::from_secs(4_001));
    let repository =
        oxide_batch::PostgresJobRepository::connect(plaintext_config(url)?, Arc::new(clock))
            .await?;
    let key = JobInstanceKey::new(JobName::new(point.job_name())?, &JobParameters::new());
    let mut inspect = repository.begin().await?;
    let instance = inspect
        .find_job_instance(&key)
        .await?
        .ok_or("flow crash worker did not create an instance")?;
    let original = inspect
        .job_executions(instance.id())
        .await?
        .into_iter()
        .next()
        .ok_or("flow crash worker did not create an execution")?;
    let steps = inspect.step_executions(original.id()).await?;
    let prior_decisions = inspect.flow_decisions(original.id()).await?;
    inspect.rollback().await?;

    assert_eq!(original.metadata().status(), BatchStatus::Started);
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].metadata().status(), BatchStatus::Completed);
    assert_eq!(
        prior_decisions.len(),
        usize::from(point == CrashPoint::AfterDecisionCommit)
    );

    let request = RecoveryRequest::mark_failed(
        original.version(),
        "FLOW_PROCESS_EXIT_INSPECTED",
        "m3-flow-crash-harness",
        [43; 32],
        FailureCategory::PermanentInfrastructure,
        FailureId::new(4_001)?,
    )?;
    let mut recover = repository.begin().await?;
    let recovered = recover
        .recover_job_execution(original.id(), &request)
        .await?;
    recover.commit().await?;
    assert_eq!(recovered.decision().resulting_status(), BatchStatus::Failed);

    let source_calls = Arc::new(AtomicUsize::new(0));
    let target_calls = Arc::new(AtomicUsize::new(0));
    let job = flow_job(point.job_name(), source_calls.clone(), target_calls.clone())?;
    let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
    let (_, stop) = StopSource::new();
    let report = FlowLauncher::new(&repository, &clock, &ids)
        .launch(&job, &JobParameters::new(), &stop)
        .await?;

    assert_eq!(report.outcome(), &FlowExecutionOutcome::Completed);
    assert_eq!(source_calls.load(Ordering::SeqCst), 0);
    assert_eq!(target_calls.load(Ordering::SeqCst), 1);
    assert_eq!(report.decisions().len(), 2);
    assert_eq!(
        report.decisions()[0].kind(),
        FlowTransitionKind::CompletedStepReuse
    );
    assert_eq!(
        report.decisions()[0].reused_decision_id().is_some(),
        point == CrashPoint::AfterDecisionCommit
    );
    repository.close().await?;
    Ok(())
}

fn run_parent_scenario(point: CrashPoint) -> Result<(), Box<dyn Error>> {
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
        PostgresMigrator::migrate(&plaintext_config(migrator_url.clone())?).await?;
        remove_job(&migrator_url, point.job_name()).await?;
        Ok::<(), Box<dyn Error>>(())
    })?;

    assert_eq!(spawn_crash_worker(point)?.code(), Some(CRASH_EXIT_CODE));
    runtime.block_on(inspect_recover_and_restart(point, runtime_url.clone()))?;
    runtime.block_on(remove_job(&migrator_url, point.job_name()))?;
    Ok(())
}

#[test]
fn flow_crash_worker_process() -> Result<(), Box<dyn Error>> {
    let Ok(value) = std::env::var(CRASH_MODE_ENV) else {
        return Ok(());
    };
    let point = CrashPoint::parse(&value)?;
    let url = runtime_url().ok_or("flow crash worker database URL is missing")?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run_crash_worker(point, url))
}

#[test]
fn crash_after_step_result_before_decision_reuses_durable_step() -> Result<(), Box<dyn Error>> {
    run_parent_scenario(CrashPoint::BeforeDecisionCommit)
}

#[test]
fn crash_after_decision_commit_reuses_durable_decision() -> Result<(), Box<dyn Error>> {
    run_parent_scenario(CrashPoint::AfterDecisionCommit)
}
