//! `PostgreSQL` 15/18 SIGKILL and restart evidence for M7 format-4 advanced flow.
//!
//! Each scenario parks a separate worker process only after the named durable
//! flow event has been committed, then the parent sends SIGKILL. A fresh
//! process inspects `PostgreSQL`, records an audited recovery decision, and
//! restarts the same definition with panic-on-reuse bindings. Any duplicated
//! completed work or re-evaluated committed decider therefore fails closed.

#![cfg(all(feature = "postgres", unix))]
#![allow(clippy::panic)]

use std::error::Error;
use std::num::NonZeroU64;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use oxide_batch::{
    BatchStatus, BoxFuture, Clock, ComponentRevision, DeciderError, DeciderRevision, DecisionInput,
    DecisionInputVersion, DecisionNode, DefinitionRevision, ExitCode, ExitPattern, ExitStatus,
    FailureCategory, FailureId, FlowEvent, FlowEventKind, FlowEventSink, FlowExecutionOutcome,
    FlowGraph, FlowJob, FlowLauncher, FlowNode, FlowTarget, FlowTransition, FlowTransitionKind,
    JobExecutionDecider, JobInstanceKey, JobName, JobParameters, JobRepository, JoinNode, NodeId,
    PostgresConfig, PostgresJobRepository, PostgresMigrator, RecoveryRequest,
    SequentialIdGenerator, SplitBranch, SplitBudget, SplitNode, StepComponents, StepName, StepNode,
    StopSource, Tasklet, TaskletContext, TaskletError, TaskletOutcome, TaskletStep, TerminalKind,
    TlsMode,
};
use sqlx::postgres::PgPoolOptions;

const WORKER_ENV: &str = "OXIDEBATCH_M7_ADVANCED_FLOW_WORKER";
const HANDSHAKE_ENV: &str = "OXIDEBATCH_M7_ADVANCED_FLOW_HANDSHAKE";
const WAIT_BOUND: Duration = Duration::from_secs(15);
const SIGKILL: i32 = 9;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CrashPoint {
    BranchTransition,
    BranchCompletion,
    JoinDecision,
}

impl CrashPoint {
    const ALL: [Self; 3] = [
        Self::BranchTransition,
        Self::BranchCompletion,
        Self::JoinDecision,
    ];

    const fn job_name(self) -> &'static str {
        match self {
            Self::BranchTransition => "postgres_m7_branch_transition_kill",
            Self::BranchCompletion => "postgres_m7_branch_completion_kill",
            Self::JoinDecision => "postgres_m7_join_decision_kill",
        }
    }

    const fn environment_value(self) -> &'static str {
        match self {
            Self::BranchTransition => "branch-transition",
            Self::BranchCompletion => "branch-completion",
            Self::JoinDecision => "join-decision",
        }
    }

    fn parse(value: &str) -> Result<Self, Box<dyn Error>> {
        match value {
            "branch-transition" => Ok(Self::BranchTransition),
            "branch-completion" => Ok(Self::BranchCompletion),
            "join-decision" => Ok(Self::JoinDecision),
            _ => Err("unknown M7 advanced-flow crash point".into()),
        }
    }

    fn matches_event(self, event: &FlowEvent) -> bool {
        match self {
            Self::BranchTransition => {
                event.kind() == FlowEventKind::DecisionCommitted
                    && event.source_node_id().as_str() == "branch-route"
            }
            Self::BranchCompletion => {
                event.kind() == FlowEventKind::StepResultCommitted
                    && event.source_node_id().as_str() == "branch-target"
            }
            Self::JoinDecision => {
                event.kind() == FlowEventKind::DecisionCommitted
                    && event.source_node_id().as_str() == "joined"
            }
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

struct CompleteTasklet;

impl Tasklet for CompleteTasklet {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async { Ok(TaskletOutcome::Completed) })
    }
}

struct PanicIfInvokedTasklet(&'static str);

impl Tasklet for PanicIfInvokedTasklet {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move { panic!("durably completed step was re-executed: {}", self.0) })
    }
}

struct RunDecider;

impl JobExecutionDecider for RunDecider {
    fn decide<'a>(
        &'a self,
        _input: DecisionInput<'a>,
    ) -> BoxFuture<'a, Result<ExitStatus, DeciderError>> {
        Box::pin(async {
            Ok(ExitStatus::new(
                ExitCode::new("RUN").map_err(|_| DeciderError::new())?,
            ))
        })
    }
}

struct PanicIfInvokedDecider;

impl JobExecutionDecider for PanicIfInvokedDecider {
    fn decide<'a>(
        &'a self,
        _input: DecisionInput<'a>,
    ) -> BoxFuture<'a, Result<ExitStatus, DeciderError>> {
        Box::pin(async { panic!("durably committed branch decider was re-evaluated") })
    }
}

struct CrashSink {
    point: CrashPoint,
    handshake: PathBuf,
}

impl FlowEventSink for CrashSink {
    fn emit(&self, event: &FlowEvent) {
        if !self.point.matches_event(event) {
            return;
        }
        if let Err(error) = std::fs::write(self.handshake.join("reached"), []) {
            panic!("worker must announce the durable crash boundary: {error}");
        }
        loop {
            std::thread::sleep(Duration::from_mins(1));
        }
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

fn step(id: &str) -> Result<StepNode, Box<dyn Error>> {
    Ok(StepNode::new(
        NodeId::new(id)?,
        StepName::new(id)?,
        StepComponents::Tasklet(ComponentRevision::new(format!("{id}-v1"))?),
    ))
}

fn step_node(id: &str) -> Result<FlowNode, Box<dyn Error>> {
    Ok(FlowNode::step(step(id)?))
}

fn branch_with_decider() -> Result<FlowGraph, Box<dyn Error>> {
    let source = NodeId::new("branch-source")?;
    let route = NodeId::new("branch-route")?;
    let target = NodeId::new("branch-target")?;
    Ok(FlowGraph::new(source.clone())
        .with_node(step_node("branch-source")?)
        .with_node(FlowNode::decision(DecisionNode::new(
            route.clone(),
            DeciderRevision::new("branch-route-v1")?,
            DecisionInputVersion::new(1)?,
        )))
        .with_node(step_node("branch-target")?)
        .with_sequence(source, FlowTarget::Node(route.clone()))?
        .with_transition(FlowTransition::new(
            route,
            ExitPattern::new("RUN")?,
            FlowTarget::Node(target.clone()),
        ))
        .with_sequence(target, FlowTarget::Terminal(TerminalKind::Complete))?)
}

fn sibling_branch() -> Result<FlowGraph, Box<dyn Error>> {
    let sibling = NodeId::new("sibling")?;
    Ok(FlowGraph::new(sibling.clone())
        .with_node(step_node("sibling")?)
        .with_sequence(sibling, FlowTarget::Terminal(TerminalKind::Complete))?)
}

fn plan(job_name: &JobName) -> Result<oxide_batch::CompiledExecutionPlan, Box<dyn Error>> {
    let prepare = NodeId::new("prepare")?;
    let split = NodeId::new("parallel")?;
    let join = NodeId::new("joined")?;
    let after = NodeId::new("after")?;
    Ok(FlowGraph::new(prepare.clone())
        .with_node(step_node("prepare")?)
        .with_node(FlowNode::split(SplitNode::new(
            split.clone(),
            vec![
                SplitBranch::flow(branch_with_decider()?),
                SplitBranch::flow(sibling_branch()?),
            ],
            join.clone(),
            SplitBudget::new(1, 2)?,
        )))
        .with_node(FlowNode::join(JoinNode::new(join.clone())))
        .with_node(step_node("after")?)
        .with_sequence(prepare, FlowTarget::Node(split))?
        .with_sequence(join, FlowTarget::Node(after.clone()))?
        .with_sequence(after, FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(
            job_name,
            DefinitionRevision::new("m7-advanced-flow-crash-v1")?,
        )?)
}

fn tasklet(name: &'static str, panic_if_invoked: bool) -> Result<TaskletStep, Box<dyn Error>> {
    let implementation: Arc<dyn Tasklet> = if panic_if_invoked {
        Arc::new(PanicIfInvokedTasklet(name))
    } else {
        Arc::new(CompleteTasklet)
    };
    Ok(TaskletStep::new(StepName::new(name)?, implementation))
}

fn restart_should_reuse(point: CrashPoint, step: &str) -> bool {
    match point {
        CrashPoint::BranchTransition => matches!(step, "prepare" | "branch-source"),
        CrashPoint::BranchCompletion => {
            matches!(step, "prepare" | "branch-source" | "branch-target")
        }
        CrashPoint::JoinDecision => {
            matches!(
                step,
                "prepare" | "branch-source" | "branch-target" | "sibling"
            )
        }
    }
}

fn job(point: CrashPoint, restart: bool) -> Result<FlowJob, Box<dyn Error>> {
    let name = JobName::new(point.job_name())?;
    let mut job = FlowJob::new(name.clone(), plan(&name)?)?;
    for step_name in [
        "prepare",
        "branch-source",
        "branch-target",
        "sibling",
        "after",
    ] {
        job = job.with_tasklet_step(
            NodeId::new(step_name)?,
            tasklet(step_name, restart && restart_should_reuse(point, step_name))?,
        )?;
    }
    let decider: Arc<dyn JobExecutionDecider> = if restart {
        Arc::new(PanicIfInvokedDecider)
    } else {
        Arc::new(RunDecider)
    };
    Ok(job.with_decider(NodeId::new("branch-route")?, decider)?)
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

fn handshake_directory(point: CrashPoint) -> Result<PathBuf, Box<dyn Error>> {
    let path = std::env::temp_dir().join(format!(
        "oxide-batch-m7-advanced-flow-{}-{}",
        point.environment_value(),
        std::process::id()
    ));
    if path.exists() {
        std::fs::remove_dir_all(&path)?;
    }
    std::fs::create_dir_all(&path)?;
    Ok(path)
}

fn wait_for_file(path: &Path) -> Result<(), Box<dyn Error>> {
    let started = Instant::now();
    while !path.exists() {
        if started.elapsed() >= WAIT_BOUND {
            return Err("advanced-flow worker did not reach the durable crash boundary".into());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    Ok(())
}

fn spawn_worker(point: CrashPoint, handshake: &Path) -> Result<Child, Box<dyn Error>> {
    Ok(Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("advanced_flow_crash_worker_process")
        .arg("--nocapture")
        .env(WORKER_ENV, point.environment_value())
        .env(HANDSHAKE_ENV, handshake)
        .spawn()?)
}

async fn run_worker(
    point: CrashPoint,
    handshake: PathBuf,
    url: String,
) -> Result<(), Box<dyn Error>> {
    let clock = FixedClock(SystemTime::UNIX_EPOCH + Duration::from_mins(200));
    let repository = PostgresJobRepository::connect(config(url)?, Arc::new(clock)).await?;
    let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
    let (_, stop) = StopSource::new();
    let sink = CrashSink { point, handshake };
    let _ = FlowLauncher::new(&repository, &clock, &ids)
        .with_event_sink(&sink)
        .launch(&job(point, false)?, &JobParameters::new(), &stop)
        .await?;
    Err("advanced-flow worker crossed the selected SIGKILL boundary".into())
}

fn durable_step_status(steps: &[oxide_batch::StepExecution], name: &str) -> Option<BatchStatus> {
    steps
        .iter()
        .find(|step| step.step_name().as_str() == name)
        .map(|step| step.metadata().status())
}

#[allow(
    clippy::too_many_lines,
    reason = "durable inspection, audited recovery, restart reuse, and sequence assertions form one evidence chain"
)]
async fn inspect_recover_restart(point: CrashPoint, url: String) -> Result<(), Box<dyn Error>> {
    let clock = FixedClock(SystemTime::UNIX_EPOCH + Duration::from_secs(12_001));
    let repository = PostgresJobRepository::connect(config(url)?, Arc::new(clock)).await?;
    let key = JobInstanceKey::new(JobName::new(point.job_name())?, &JobParameters::new());
    let mut inspect = repository.begin().await?;
    let instance = inspect
        .find_job_instance(&key)
        .await?
        .ok_or("advanced-flow worker did not create an instance")?;
    let original = inspect
        .job_executions(instance.id())
        .await?
        .into_iter()
        .next()
        .ok_or("advanced-flow worker did not create an execution")?;
    let original_steps = inspect.step_executions(original.id()).await?;
    let original_decisions = inspect.flow_decisions(original.id()).await?;
    inspect.rollback().await?;

    assert_eq!(original.metadata().status(), BatchStatus::Started);
    assert_eq!(
        durable_step_status(&original_steps, "prepare"),
        Some(BatchStatus::Completed)
    );
    assert_eq!(
        durable_step_status(&original_steps, "branch-source"),
        Some(BatchStatus::Completed)
    );
    match point {
        CrashPoint::BranchTransition => {
            assert_eq!(durable_step_status(&original_steps, "branch-target"), None);
            assert_eq!(durable_step_status(&original_steps, "sibling"), None);
        }
        CrashPoint::BranchCompletion => {
            assert_eq!(
                durable_step_status(&original_steps, "branch-target"),
                Some(BatchStatus::Completed)
            );
            assert_eq!(durable_step_status(&original_steps, "sibling"), None);
        }
        CrashPoint::JoinDecision => {
            assert_eq!(
                durable_step_status(&original_steps, "branch-target"),
                Some(BatchStatus::Completed)
            );
            assert_eq!(
                durable_step_status(&original_steps, "sibling"),
                Some(BatchStatus::Completed)
            );
            assert!(original_decisions.iter().any(|decision| {
                decision.kind() == FlowTransitionKind::SplitAggregate
                    && decision.source_node_id().as_str() == "joined"
            }));
        }
    }
    let sequences = original_decisions
        .iter()
        .map(|decision| decision.sequence().get())
        .collect::<Vec<_>>();
    assert!(sequences.windows(2).all(|pair| pair[0] < pair[1]));

    let request = RecoveryRequest::mark_failed(
        original.version(),
        "M7_ADVANCED_FLOW_PROCESS_KILL_INSPECTED",
        "m7-advanced-flow-sigkill-harness",
        [97; 32],
        FailureCategory::PermanentInfrastructure,
        FailureId::new(12_001)?,
    )?;
    let mut recover = repository.begin().await?;
    recover
        .recover_job_execution(original.id(), &request)
        .await?;
    recover.commit().await?;

    let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
    let (_, stop) = StopSource::new();
    let report = FlowLauncher::new(&repository, &clock, &ids)
        .launch(&job(point, true)?, &JobParameters::new(), &stop)
        .await?;

    assert_eq!(report.outcome(), &FlowExecutionOutcome::Completed);
    let restarted_names = report
        .step_executions()
        .iter()
        .map(|step| step.step_name().as_str().to_owned())
        .collect::<Vec<_>>();
    for completed in ["prepare", "branch-source", "branch-target", "sibling"] {
        if restart_should_reuse(point, completed) {
            assert!(
                !restarted_names.iter().any(|name| name == completed),
                "durably completed step was executed again: {completed}"
            );
        }
    }
    assert!(report.decisions().iter().any(|decision| {
        decision.source_node_id().as_str() == "branch-route"
            && decision.kind() == FlowTransitionKind::Decider
            && decision.reused_decision_id().is_some()
    }));
    if point == CrashPoint::JoinDecision {
        assert!(report.decisions().iter().any(|decision| {
            decision.kind() == FlowTransitionKind::SplitAggregate
                && decision.source_node_id().as_str() == "joined"
                && decision.reused_decision_id().is_some()
        }));
    }
    assert!(restarted_names.iter().any(|name| name == "after"));
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
        PostgresMigrator::migrate(&config(migrator_url.clone())?).await?;
        remove_job(&migrator_url, point.job_name()).await?;
        Ok::<(), Box<dyn Error>>(())
    })?;

    let handshake = handshake_directory(point)?;
    let mut child = spawn_worker(point, &handshake)?;
    let reached = wait_for_file(&handshake.join("reached"));
    if reached.is_err() {
        let _ = child.kill();
        let _ = child.wait();
        reached?;
    }
    child.kill()?;
    let status = child.wait()?;
    assert_eq!(status.signal(), Some(SIGKILL));

    runtime.block_on(inspect_recover_restart(point, runtime_url.clone()))?;
    runtime.block_on(remove_job(&migrator_url, point.job_name()))?;
    std::fs::remove_dir_all(handshake)?;
    Ok(())
}

#[test]
fn advanced_flow_crash_worker_process() -> Result<(), Box<dyn Error>> {
    let Ok(value) = std::env::var(WORKER_ENV) else {
        return Ok(());
    };
    let point = CrashPoint::parse(&value)?;
    let handshake = PathBuf::from(std::env::var(HANDSHAKE_ENV)?);
    let url = runtime_url().ok_or("advanced-flow worker database URL is missing")?;
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run_worker(point, handshake, url))
}

#[test]
fn sigkill_restart_reuses_branch_join_and_transition_state() -> Result<(), Box<dyn Error>> {
    for point in CrashPoint::ALL {
        run_parent_scenario(point)?;
    }
    Ok(())
}
