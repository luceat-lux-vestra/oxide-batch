//! PostgreSQL 15/18 real-process SIGKILL and restart evidence for registered custom leaves.
//!
//! The three boundaries are deliberately distinct:
//! - before the handler returns its candidate state (no custom state may become durable),
//! - after custom state CAS commits but before the terminal step transition (the state must survive),
//! - after the terminal step transition commits (restart must reuse the completed leaf and never invoke it again).
//!
//! The parent always kills a separate worker process from the outside, inspects durable PostgreSQL state,
//! records an audited recovery decision, and restarts through the public flow launcher.

#![cfg(all(feature = "postgres", unix))]
#![allow(clippy::expect_used, clippy::panic)]

use std::error::Error;
use std::num::NonZeroU64;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use oxide_batch::{
    BatchStatus, BoxFuture, Clock, ComponentRevision, CustomLeafContext, CustomLeafHandler,
    CustomLeafKind, CustomLeafNode, CustomLeafRegistration, CustomLeafResult, DefinitionRevision,
    ExecutionContext, FailureCategory, FailureId, FlowEvent, FlowEventKind, FlowEventSink,
    FlowExecutionOutcome, FlowGraph, FlowJob, FlowLauncher, FlowNode, FlowTarget, JobInstanceKey,
    JobName, JobParameters, JobRepository, ListenerContext, ListenerError, PostgresConfig,
    PostgresJobRepository, PostgresMigrator, RecoveryRequest, SequentialIdGenerator, StateLimits,
    StateSchemaId, StateSchemaVersion, StepExecutionListener, StepName, StopSource, TaskletError,
    TaskletExecutionOutcome, TaskletOutcome, TerminalKind, TlsMode,
};
use sqlx::postgres::PgPoolOptions;

const WORKER_ENV: &str = "OXIDEBATCH_M7_CUSTOM_LEAF_WORKER";
const HANDSHAKE_ENV: &str = "OXIDEBATCH_M7_CUSTOM_LEAF_HANDSHAKE";
const WAIT_BOUND: Duration = Duration::from_secs(15);
const SIGKILL: i32 = 9;
const NODE: &str = "custom";
const STEP: &str = "custom-step";
const KIND: &str = "m7.custom-leaf.handler";
const HANDLER_REVISION: &str = "handler-v1";
const LISTENER_REVISION: &str = "listener-v1";
const STATE_SCHEMA: &str = "m7.custom-leaf.state";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CrashPoint {
    BeforeStateCommit,
    AfterStateCommitBeforeTransition,
    AfterTerminalTransition,
}

impl CrashPoint {
    const ALL: [Self; 3] = [
        Self::BeforeStateCommit,
        Self::AfterStateCommitBeforeTransition,
        Self::AfterTerminalTransition,
    ];

    const fn environment_value(self) -> &'static str {
        match self {
            Self::BeforeStateCommit => "before-state-commit",
            Self::AfterStateCommitBeforeTransition => "after-state-before-transition",
            Self::AfterTerminalTransition => "after-terminal-transition",
        }
    }

    const fn job_name(self) -> &'static str {
        match self {
            Self::BeforeStateCommit => "postgres_m7_custom_leaf_before_state_kill",
            Self::AfterStateCommitBeforeTransition => "postgres_m7_custom_leaf_after_state_kill",
            Self::AfterTerminalTransition => "postgres_m7_custom_leaf_after_terminal_kill",
        }
    }

    fn parse(value: &str) -> Result<Self, Box<dyn Error>> {
        match value {
            "before-state-commit" => Ok(Self::BeforeStateCommit),
            "after-state-before-transition" => Ok(Self::AfterStateCommitBeforeTransition),
            "after-terminal-transition" => Ok(Self::AfterTerminalTransition),
            _ => Err("unknown custom-leaf crash point".into()),
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

fn runtime_url() -> Option<String> {
    std::env::var("OXIDEBATCH_POSTGRES_TEST_URL").ok()
}

fn migrator_url() -> Option<String> {
    std::env::var("OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL").ok()
}

fn config(url: String) -> Result<PostgresConfig, Box<dyn Error>> {
    Ok(PostgresConfig::new(url)?.with_tls_mode(TlsMode::Plaintext))
}

fn state(value: u64) -> Result<ExecutionContext, Box<dyn Error>> {
    let document = format!(
        r#"{{"format":"oxide-batch.execution-context","format_version":1,"schema":"{STATE_SCHEMA}","schema_version":1,"payload":{{"value":{value}}}}}"#
    );
    Ok(ExecutionContext::from_json(
        document.as_bytes(),
        StateLimits::default(),
    )?)
}

fn state_value(context: &ExecutionContext) -> Option<u64> {
    serde_json::from_slice::<serde_json::Value>(&context.payload_json().ok()?)
        .ok()?
        .get("value")?
        .as_u64()
}

fn plan(point: CrashPoint) -> Result<oxide_batch::CompiledExecutionPlan, Box<dyn Error>> {
    let name = JobName::new(point.job_name())?;
    let id = oxide_batch::NodeId::new(NODE)?;
    let mut leaf = CustomLeafNode::new(
        id.clone(),
        StepName::new(STEP)?,
        CustomLeafKind::new(KIND)?,
        ComponentRevision::new(HANDLER_REVISION)?,
        StateSchemaId::new(STATE_SCHEMA)?,
        StateSchemaVersion::new(1)?,
    );
    if point == CrashPoint::AfterStateCommitBeforeTransition {
        leaf = leaf.with_listener_revision(ComponentRevision::new(LISTENER_REVISION)?);
    }
    Ok(FlowGraph::new(id.clone())
        .with_node(FlowNode::custom_leaf(leaf))
        .with_sequence(id, FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(&name, DefinitionRevision::new("m7-custom-leaf-crash-v1")?)?)
}

fn registration(
    handler: Arc<dyn CustomLeafHandler>,
    listener: Option<Arc<dyn StepExecutionListener>>,
) -> Result<CustomLeafRegistration, Box<dyn Error>> {
    let mut registration = CustomLeafRegistration::new(
        CustomLeafKind::new(KIND)?,
        ComponentRevision::new(HANDLER_REVISION)?,
        StateSchemaId::new(STATE_SCHEMA)?,
        StateSchemaVersion::new(1)?,
        handler,
    );
    if let Some(listener) = listener {
        registration = registration.with_listener(
            ComponentRevision::new(LISTENER_REVISION)?,
            listener,
        );
    }
    Ok(registration)
}

fn job(
    point: CrashPoint,
    handler: Arc<dyn CustomLeafHandler>,
    listener: Option<Arc<dyn StepExecutionListener>>,
) -> Result<FlowJob, Box<dyn Error>> {
    let name = JobName::new(point.job_name())?;
    Ok(FlowJob::new(name, plan(point)?)?.with_custom_leaf_registration(
        oxide_batch::NodeId::new(NODE)?,
        registration(handler, listener)?,
    )?)
}

struct ParkBeforeState {
    reached: PathBuf,
}

impl CustomLeafHandler for ParkBeforeState {
    fn execute<'a>(
        &'a self,
        _context: CustomLeafContext<'a>,
    ) -> BoxFuture<'a, Result<CustomLeafResult, TaskletError>> {
        Box::pin(async move {
            std::fs::write(&self.reached, []).map_err(TaskletError::from_error)?;
            loop {
                std::thread::sleep(Duration::from_secs(60));
            }
        })
    }
}

struct PublishState;

impl CustomLeafHandler for PublishState {
    fn execute<'a>(
        &'a self,
        _context: CustomLeafContext<'a>,
    ) -> BoxFuture<'a, Result<CustomLeafResult, TaskletError>> {
        Box::pin(async {
            let next = state(1).map_err(|error| std::io::Error::other(error.to_string()))?;
            Ok(CustomLeafResult::new(TaskletOutcome::Completed).with_state(next))
        })
    }
}

struct ExpectedRestartHandler {
    expected: Option<u64>,
    calls: Arc<AtomicUsize>,
}

impl CustomLeafHandler for ExpectedRestartHandler {
    fn execute<'a>(
        &'a self,
        context: CustomLeafContext<'a>,
    ) -> BoxFuture<'a, Result<CustomLeafResult, TaskletError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let observed = context.previous_state().and_then(state_value);
            assert_eq!(
                observed, self.expected,
                "restart must receive exactly the last committed custom state"
            );
            let next = state(2).map_err(|error| std::io::Error::other(error.to_string()))?;
            Ok(CustomLeafResult::new(TaskletOutcome::Completed).with_state(next))
        })
    }
}

struct PanicIfInvoked;

impl CustomLeafHandler for PanicIfInvoked {
    fn execute<'a>(
        &'a self,
        _context: CustomLeafContext<'a>,
    ) -> BoxFuture<'a, Result<CustomLeafResult, TaskletError>> {
        Box::pin(async { panic!("durably completed custom leaf was re-executed") })
    }
}

struct ParkAfterState {
    reached: PathBuf,
}

impl StepExecutionListener for ParkAfterState {
    fn before_step<'a>(
        &'a self,
        _context: ListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        Box::pin(async { Ok(()) })
    }

    fn after_step<'a>(
        &'a self,
        _context: ListenerContext<'a>,
        _outcome: TaskletExecutionOutcome,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        Box::pin(async move {
            std::fs::write(&self.reached, []).map_err(|_| ListenerError::new())?;
            loop {
                std::thread::sleep(Duration::from_secs(60));
            }
        })
    }
}

struct NoopListener;

impl StepExecutionListener for NoopListener {
    fn before_step<'a>(
        &'a self,
        _context: ListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        Box::pin(async { Ok(()) })
    }

    fn after_step<'a>(
        &'a self,
        _context: ListenerContext<'a>,
        _outcome: TaskletExecutionOutcome,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        Box::pin(async { Ok(()) })
    }
}

struct ParkAfterTerminal {
    reached: PathBuf,
}

impl FlowEventSink for ParkAfterTerminal {
    fn emit(&self, event: &FlowEvent) {
        if event.kind() != FlowEventKind::StepResultCommitted || event.source_node_id().as_str() != NODE
        {
            return;
        }
        std::fs::write(&self.reached, []).expect("announce terminal transition boundary");
        loop {
            std::thread::sleep(Duration::from_secs(60));
        }
    }
}

fn handshake_directory(point: CrashPoint) -> Result<PathBuf, Box<dyn Error>> {
    let path = std::env::temp_dir().join(format!(
        "oxide-batch-m7-custom-leaf-{}-{}",
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
            return Err("custom-leaf worker did not reach the selected crash boundary".into());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    Ok(())
}

fn spawn_worker(point: CrashPoint, handshake: &Path) -> Result<Child, Box<dyn Error>> {
    Ok(Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("custom_leaf_crash_worker_process")
        .arg("--nocapture")
        .env(WORKER_ENV, point.environment_value())
        .env(HANDSHAKE_ENV, handshake)
        .spawn()?)
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

async fn run_worker(
    point: CrashPoint,
    handshake: PathBuf,
    url: String,
) -> Result<(), Box<dyn Error>> {
    let clock = FixedClock(SystemTime::UNIX_EPOCH + Duration::from_secs(20_000));
    let repository = PostgresJobRepository::connect(config(url)?, Arc::new(clock)).await?;
    let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
    let (_, stop) = StopSource::new();

    match point {
        CrashPoint::BeforeStateCommit => {
            let handler = Arc::new(ParkBeforeState {
                reached: handshake.join("reached"),
            });
            let _ = FlowLauncher::new(&repository, &clock, &ids)
                .launch(&job(point, handler, None)?, &JobParameters::new(), &stop)
                .await?;
        }
        CrashPoint::AfterStateCommitBeforeTransition => {
            let listener = Arc::new(ParkAfterState {
                reached: handshake.join("reached"),
            });
            let _ = FlowLauncher::new(&repository, &clock, &ids)
                .launch(
                    &job(point, Arc::new(PublishState), Some(listener))?,
                    &JobParameters::new(),
                    &stop,
                )
                .await?;
        }
        CrashPoint::AfterTerminalTransition => {
            let sink = ParkAfterTerminal {
                reached: handshake.join("reached"),
            };
            let _ = FlowLauncher::new(&repository, &clock, &ids)
                .with_event_sink(&sink)
                .launch(
                    &job(point, Arc::new(PublishState), None)?,
                    &JobParameters::new(),
                    &stop,
                )
                .await?;
        }
    }
    Err("custom-leaf worker crossed the selected SIGKILL boundary".into())
}

async fn inspect_recover_restart(point: CrashPoint, url: String) -> Result<(), Box<dyn Error>> {
    let clock = FixedClock(SystemTime::UNIX_EPOCH + Duration::from_secs(21_000));
    let repository = PostgresJobRepository::connect(config(url)?, Arc::new(clock)).await?;
    let key = JobInstanceKey::new(JobName::new(point.job_name())?, &JobParameters::new());
    let node_id = oxide_batch::NodeId::new(NODE)?;

    let mut inspect = repository.begin().await?;
    let instance = inspect
        .find_job_instance(&key)
        .await?
        .ok_or("custom-leaf worker did not create an instance")?;
    let original = inspect
        .job_executions(instance.id())
        .await?
        .into_iter()
        .next()
        .ok_or("custom-leaf worker did not create an execution")?;
    let durable = inspect
        .latest_flow_step(instance.id(), &node_id)
        .await?
        .ok_or("custom-leaf worker did not create a step")?;
    inspect.rollback().await?;

    assert_eq!(original.metadata().status(), BatchStatus::Started);
    match point {
        CrashPoint::BeforeStateCommit => {
            assert_eq!(durable.execution().metadata().status(), BatchStatus::Started);
            assert!(durable.context().is_none());
        }
        CrashPoint::AfterStateCommitBeforeTransition => {
            assert_eq!(durable.execution().metadata().status(), BatchStatus::Started);
            assert_eq!(durable.context().and_then(state_value), Some(1));
        }
        CrashPoint::AfterTerminalTransition => {
            assert_eq!(durable.execution().metadata().status(), BatchStatus::Completed);
            assert_eq!(durable.context().and_then(state_value), Some(1));
        }
    }

    let request = RecoveryRequest::mark_failed(
        original.version(),
        "M7_CUSTOM_LEAF_PROCESS_KILL_INSPECTED",
        "m7-custom-leaf-sigkill-harness",
        [101; 32],
        FailureCategory::PermanentInfrastructure,
        FailureId::new(21_000)?,
    )?;
    let mut recover = repository.begin().await?;
    recover.recover_job_execution(original.id(), &request).await?;
    recover.commit().await?;

    let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
    let (_, stop) = StopSource::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let (handler, listener): (
        Arc<dyn CustomLeafHandler>,
        Option<Arc<dyn StepExecutionListener>>,
    ) = match point {
        CrashPoint::BeforeStateCommit => (
            Arc::new(ExpectedRestartHandler {
                expected: None,
                calls: calls.clone(),
            }),
            None,
        ),
        CrashPoint::AfterStateCommitBeforeTransition => (
            Arc::new(ExpectedRestartHandler {
                expected: Some(1),
                calls: calls.clone(),
            }),
            Some(Arc::new(NoopListener)),
        ),
        CrashPoint::AfterTerminalTransition => (Arc::new(PanicIfInvoked), None),
    };
    let report = FlowLauncher::new(&repository, &clock, &ids)
        .launch(
            &job(point, handler, listener)?,
            &JobParameters::new(),
            &stop,
        )
        .await?;
    assert_eq!(report.outcome(), &FlowExecutionOutcome::Completed);

    let mut verify = repository.begin().await?;
    let final_state = verify
        .latest_flow_step(instance.id(), &node_id)
        .await?
        .ok_or("restart left no custom-leaf state")?;
    verify.rollback().await?;

    match point {
        CrashPoint::BeforeStateCommit => {
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(final_state.context().and_then(state_value), Some(2));
        }
        CrashPoint::AfterStateCommitBeforeTransition => {
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(final_state.context().and_then(state_value), Some(2));
        }
        CrashPoint::AfterTerminalTransition => {
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert_eq!(final_state.context().and_then(state_value), Some(1));
            assert_eq!(final_state.execution().metadata().status(), BatchStatus::Completed);
        }
    }
    repository.close().await?;
    Ok(())
}

fn run_parent(point: CrashPoint) -> Result<(), Box<dyn Error>> {
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
    assert_eq!(status.code(), None);

    runtime.block_on(inspect_recover_restart(point, runtime_url.clone()))?;
    runtime.block_on(remove_job(&migrator_url, point.job_name()))?;
    std::fs::remove_dir_all(handshake)?;
    Ok(())
}

#[test]
fn custom_leaf_crash_worker_process() -> Result<(), Box<dyn Error>> {
    let Ok(value) = std::env::var(WORKER_ENV) else {
        return Ok(());
    };
    let point = CrashPoint::parse(&value)?;
    let handshake = PathBuf::from(std::env::var(HANDSHAKE_ENV)?);
    let url = runtime_url().ok_or("custom-leaf worker database URL is missing")?;
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run_worker(point, handshake, url))
}

#[test]
fn sigkill_restart_proves_custom_state_and_terminal_boundaries() -> Result<(), Box<dyn Error>> {
    for point in CrashPoint::ALL {
        run_parent(point)?;
    }
    Ok(())
}
