//! `PostgreSQL` evidence for durable M3 flow decisions and restart traversal.

#![cfg(feature = "postgres")]

use std::error::Error;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

use oxide_batch::{
    BatchStatus, BoxFuture, Clock, ComponentRevision, DeciderError, DeciderRevision, DecisionInput,
    DecisionInputVersion, DecisionNode, DefinitionRevision, ExitCode, ExitPattern, ExitStatus,
    FlowExecutionOutcome, FlowFailure, FlowGraph, FlowJob, FlowLauncher, FlowNode, FlowTarget,
    FlowTransition, FlowTransitionKind, JobExecutionDecider, JobName, JobParameters, JobRepository,
    NodeId, PostgresConfig, PostgresJobRepository, SequentialIdGenerator, StartControls,
    StartLimit, StepComponents, StepName, StepNode, StopSource, Tasklet, TaskletContext,
    TaskletError, TaskletOutcome, TaskletStep, TerminalKind, TlsMode,
};
use sqlx::postgres::PgPoolOptions;

#[derive(Clone, Copy)]
struct FixedClock(SystemTime);

impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        self.0
    }
}

struct StatefulTasklet {
    calls: Arc<AtomicUsize>,
    kind: TaskletKind,
}

enum TaskletKind {
    Complete,
    Custom(&'static str),
    FailOnce,
}

impl Tasklet for StatefulTasklet {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            match self.kind {
                TaskletKind::Custom(code) => Ok(TaskletOutcome::CompletedWith(ExitStatus::new(
                    ExitCode::new(code).map_err(TaskletError::from_error)?,
                ))),
                TaskletKind::FailOnce if call == 0 => Err(TaskletError::new()),
                TaskletKind::Complete | TaskletKind::FailOnce => Ok(TaskletOutcome::Completed),
            }
        })
    }
}

struct CountingDecider(Arc<AtomicUsize>);

impl JobExecutionDecider for CountingDecider {
    fn decide<'a>(
        &'a self,
        input: DecisionInput<'a>,
    ) -> BoxFuture<'a, Result<ExitStatus, DeciderError>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            assert_eq!(
                input
                    .preceding_step()
                    .map(oxide_batch::DecisionStepInput::status),
                Some(BatchStatus::Completed)
            );
            Ok(ExitStatus::new(
                ExitCode::new("RUN").map_err(|_| DeciderError::new())?,
            ))
        })
    }
}

fn runtime_url() -> Option<String> {
    std::env::var("OXIDEBATCH_POSTGRES_TEST_URL").ok()
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

fn node(id: &str) -> Result<FlowNode, Box<dyn Error>> {
    node_with_controls(id, StartControls::default())
}

fn node_with_controls(id: &str, controls: StartControls) -> Result<FlowNode, Box<dyn Error>> {
    Ok(FlowNode::step(
        StepNode::new(
            NodeId::new(id)?,
            StepName::new(id)?,
            StepComponents::Tasklet(ComponentRevision::new(format!("{id}-v1"))?),
        )
        .with_start_controls(controls),
    ))
}

fn step(
    name: &str,
    calls: Arc<AtomicUsize>,
    kind: TaskletKind,
) -> Result<TaskletStep, Box<dyn Error>> {
    Ok(TaskletStep::new(
        StepName::new(name)?,
        Arc::new(StatefulTasklet { calls, kind }),
    ))
}

#[test]
fn decisions_and_completed_step_reuse_survive_postgres_restart() -> Result<(), Box<dyn Error>> {
    const JOB: &str = "postgres_m3_flow_restart";
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        remove_job(&url, JOB).await?;
        let clock = Arc::new(FixedClock(
            SystemTime::UNIX_EPOCH + Duration::from_secs(2_000),
        ));
        let repository = PostgresJobRepository::connect(
            PostgresConfig::new(url.clone())?.with_tls_mode(TlsMode::Plaintext),
            clock.clone(),
        )
        .await?;
        let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
        let load = NodeId::new("load")?;
        let decide = NodeId::new("decide")?;
        let publish = NodeId::new("publish")?;
        let plan = FlowGraph::new(load.clone())
            .with_node(node("load")?)
            .with_node(FlowNode::decision(DecisionNode::new(
                decide.clone(),
                DeciderRevision::new("route-v1")?,
                DecisionInputVersion::new(1)?,
            )))
            .with_node(node("publish")?)
            .with_sequence(load.clone(), FlowTarget::Node(decide.clone()))?
            .with_transition(FlowTransition::new(
                decide.clone(),
                ExitPattern::new("RUN")?,
                FlowTarget::Node(publish.clone()),
            ))
            .with_sequence(
                publish.clone(),
                FlowTarget::Terminal(TerminalKind::Complete),
            )?
            .compile(&JobName::new(JOB)?, DefinitionRevision::new("v1")?)?;
        let load_calls = Arc::new(AtomicUsize::new(0));
        let publish_calls = Arc::new(AtomicUsize::new(0));
        let decider_calls = Arc::new(AtomicUsize::new(0));
        let job = FlowJob::new(JobName::new(JOB)?, plan)?
            .with_tasklet_step(
                load.clone(),
                step("load", load_calls.clone(), TaskletKind::Complete)?,
            )?
            .with_decider(decide, Arc::new(CountingDecider(decider_calls.clone())))?
            .with_tasklet_step(
                publish,
                step("publish", publish_calls.clone(), TaskletKind::FailOnce)?,
            )?;
        let (_, stop) = StopSource::new();
        let launcher = FlowLauncher::new(&repository, clock.as_ref(), &ids);

        let first = launcher.launch(&job, &JobParameters::new(), &stop).await?;
        assert!(matches!(first.outcome(), FlowExecutionOutcome::Failed(_)));
        let second = launcher.launch(&job, &JobParameters::new(), &stop).await?;

        assert_eq!(second.outcome(), &FlowExecutionOutcome::Completed);
        assert_eq!(load_calls.load(Ordering::SeqCst), 1);
        assert_eq!(decider_calls.load(Ordering::SeqCst), 1);
        assert_eq!(publish_calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            second.decisions()[0].kind(),
            FlowTransitionKind::CompletedStepReuse
        );
        assert!(second.decisions()[0].reused_decision_id().is_some());
        assert!(second.decisions()[1].reused_decision_id().is_some());

        let mut inspect = repository.begin().await?;
        let durable = inspect.flow_decisions(second.job_execution().id()).await?;
        inspect.rollback().await?;
        assert_eq!(durable, second.decisions());
        repository.close().await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn postgres_flow_persists_custom_exit_mapping() -> Result<(), Box<dyn Error>> {
    const JOB: &str = "postgres_m3_flow_custom_exit";
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        remove_job(&url, JOB).await?;
        let clock = Arc::new(FixedClock(SystemTime::UNIX_EPOCH + Duration::from_mins(35)));
        let repository = PostgresJobRepository::connect(
            PostgresConfig::new(url.clone())?.with_tls_mode(TlsMode::Plaintext),
            clock.clone(),
        )
        .await?;
        let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
        let choose = NodeId::new("choose")?;
        let selected = NodeId::new("selected")?;
        let plan = FlowGraph::new(choose.clone())
            .with_node(node("choose")?)
            .with_node(node("selected")?)
            .with_transition(FlowTransition::new(
                choose.clone(),
                ExitPattern::new("PRIORITY")?,
                FlowTarget::Node(selected.clone()),
            ))
            .with_transition(FlowTransition::new(
                choose.clone(),
                ExitPattern::new("*")?,
                FlowTarget::Terminal(TerminalKind::Fail),
            ))
            .with_sequence(
                selected.clone(),
                FlowTarget::Terminal(TerminalKind::Complete),
            )?
            .compile(&JobName::new(JOB)?, DefinitionRevision::new("v1")?)?;
        let job = FlowJob::new(JobName::new(JOB)?, plan)?
            .with_tasklet_step(
                choose,
                step(
                    "choose",
                    Arc::new(AtomicUsize::new(0)),
                    TaskletKind::Custom("PRIORITY"),
                )?,
            )?
            .with_tasklet_step(
                selected,
                step(
                    "selected",
                    Arc::new(AtomicUsize::new(0)),
                    TaskletKind::Complete,
                )?,
            )?;
        let (_, stop) = StopSource::new();
        let report = FlowLauncher::new(&repository, clock.as_ref(), &ids)
            .launch(&job, &JobParameters::new(), &stop)
            .await?;
        assert_eq!(report.outcome(), &FlowExecutionOutcome::Completed);
        assert_eq!(
            report.decisions()[0].observed_outcome().as_str(),
            "PRIORITY"
        );
        repository.close().await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn postgres_flow_persists_stop_terminal() -> Result<(), Box<dyn Error>> {
    const JOB: &str = "postgres_m3_flow_stop";
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        remove_job(&url, JOB).await?;
        let clock = Arc::new(FixedClock(SystemTime::UNIX_EPOCH + Duration::from_mins(40)));
        let repository = PostgresJobRepository::connect(
            PostgresConfig::new(url.clone())?.with_tls_mode(TlsMode::Plaintext),
            clock.clone(),
        )
        .await?;
        let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
        let only = NodeId::new("only")?;
        let plan = FlowGraph::new(only.clone())
            .with_node(node("only")?)
            .with_transition(FlowTransition::new(
                only.clone(),
                ExitPattern::new("*")?,
                FlowTarget::Terminal(TerminalKind::Stop),
            ))
            .compile(&JobName::new(JOB)?, DefinitionRevision::new("v1")?)?;
        let job = FlowJob::new(JobName::new(JOB)?, plan)?.with_tasklet_step(
            only,
            step("only", Arc::new(AtomicUsize::new(0)), TaskletKind::Complete)?,
        )?;
        let (_, stop) = StopSource::new();

        let report = FlowLauncher::new(&repository, clock.as_ref(), &ids)
            .launch(&job, &JobParameters::new(), &stop)
            .await?;

        assert_eq!(report.outcome(), &FlowExecutionOutcome::Stopped);
        assert_eq!(
            report.job_execution().metadata().status(),
            BatchStatus::Stopped
        );
        assert_eq!(
            report.decisions()[0].target(),
            &FlowTarget::Terminal(TerminalKind::Stop)
        );
        repository.close().await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn postgres_start_controls_survive_restart() -> Result<(), Box<dyn Error>> {
    const JOB: &str = "postgres_m3_flow_start_controls";
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        remove_job(&url, JOB).await?;
        let clock = Arc::new(FixedClock(SystemTime::UNIX_EPOCH + Duration::from_mins(45)));
        let repository = PostgresJobRepository::connect(
            PostgresConfig::new(url.clone())?.with_tls_mode(TlsMode::Plaintext),
            clock.clone(),
        )
        .await?;
        let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
        let prepare = NodeId::new("prepare")?;
        let publish = NodeId::new("publish")?;
        let limit = StartLimit::new(2)?;
        let plan = FlowGraph::new(prepare.clone())
            .with_node(node_with_controls(
                "prepare",
                StartControls::new(limit, true),
            )?)
            .with_node(node("publish")?)
            .with_sequence(prepare.clone(), FlowTarget::Node(publish.clone()))?
            .with_sequence(
                publish.clone(),
                FlowTarget::Terminal(TerminalKind::Complete),
            )?
            .compile(&JobName::new(JOB)?, DefinitionRevision::new("v1")?)?;
        let prepare_calls = Arc::new(AtomicUsize::new(0));
        let publish_calls = Arc::new(AtomicUsize::new(0));
        let job = FlowJob::new(JobName::new(JOB)?, plan)?
            .with_tasklet_step(
                prepare.clone(),
                step("prepare", prepare_calls.clone(), TaskletKind::Complete)?,
            )?
            .with_tasklet_step(
                publish,
                step("publish", publish_calls.clone(), TaskletKind::FailOnce)?,
            )?;
        let (_, stop) = StopSource::new();
        let launcher = FlowLauncher::new(&repository, clock.as_ref(), &ids);

        assert!(matches!(
            launcher
                .launch(&job, &JobParameters::new(), &stop)
                .await?
                .outcome(),
            FlowExecutionOutcome::Failed(_)
        ));
        assert_eq!(
            launcher
                .launch(&job, &JobParameters::new(), &stop)
                .await?
                .outcome(),
            &FlowExecutionOutcome::Completed
        );
        assert_eq!(prepare_calls.load(Ordering::SeqCst), 2);
        assert_eq!(publish_calls.load(Ordering::SeqCst), 2);

        repository.close().await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn postgres_failed_start_consumes_limit() -> Result<(), Box<dyn Error>> {
    const JOB: &str = "postgres_m3_flow_start_limit";
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        remove_job(&url, JOB).await?;
        let clock = Arc::new(FixedClock(SystemTime::UNIX_EPOCH + Duration::from_mins(50)));
        let repository = PostgresJobRepository::connect(
            PostgresConfig::new(url.clone())?.with_tls_mode(TlsMode::Plaintext),
            clock.clone(),
        )
        .await?;
        let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
        let only = NodeId::new("only")?;
        let limit = StartLimit::new(1)?;
        let plan = FlowGraph::new(only.clone())
            .with_node(node_with_controls(
                "only",
                StartControls::new(limit, false),
            )?)
            .with_sequence(only.clone(), FlowTarget::Terminal(TerminalKind::Complete))?
            .compile(&JobName::new(JOB)?, DefinitionRevision::new("v1")?)?;
        let calls = Arc::new(AtomicUsize::new(0));
        let job = FlowJob::new(JobName::new(JOB)?, plan)?.with_tasklet_step(
            only.clone(),
            step("only", calls.clone(), TaskletKind::FailOnce)?,
        )?;
        let (_, stop) = StopSource::new();
        let launcher = FlowLauncher::new(&repository, clock.as_ref(), &ids);

        assert!(matches!(
            launcher
                .launch(&job, &JobParameters::new(), &stop)
                .await?
                .outcome(),
            FlowExecutionOutcome::Failed(_)
        ));
        let exhausted = launcher.launch(&job, &JobParameters::new(), &stop).await?;
        assert_eq!(
            exhausted.outcome(),
            &FlowExecutionOutcome::Failed(FlowFailure::StartLimitExceeded { node: only, limit })
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        repository.close().await?;
        Ok::<(), Box<dyn Error>>(())
    })
}
