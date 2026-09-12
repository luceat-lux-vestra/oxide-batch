//! `PostgreSQL` 15/18 SIGKILL evidence for M7 #265 nested-job linkage.
//!
//! The harness uses only ordinary `PostgreSQL` locks and a child-tasklet
//! handshake. No production fault hook is required. Four boundaries are
//! exercised in fresh worker processes:
//!
//! 1. before the parent↔child link commits;
//! 2. after the link commits but before child business work;
//! 3. after child completion commits but before parent terminal observation;
//! 4. after terminal observation commits but before the outer flow decision.

#![cfg(all(feature = "postgres", unix))]
#![allow(clippy::panic)]

use std::error::Error;
use std::fs::OpenOptions;
use std::io::Write;
use std::num::NonZeroU64;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use oxide_batch::{
    BatchStatus, BoxFuture, Clock, ComponentRevision, DefinitionRevision, FailureCategory,
    FailureId, FlowExecutionOutcome, FlowGraph, FlowJob, FlowLauncher, FlowNode, FlowTarget,
    JobExecution, JobExecutionId, JobInstanceKey, JobName, JobParameter, JobParameters,
    JobRepository, MissingParameterPolicy, NestedJobNode, NestedJobParameterMapping,
    NestedJobParameterSource, NodeId, ParameterCoercion, ParameterName, ParameterRole,
    ParameterValue, ParameterValueKind, PostgresConfig, PostgresJobRepository, PostgresMigrator,
    RecoveryRequest, SequentialIdGenerator, StepName, StopSource, Tasklet, TaskletContext,
    TaskletError, TaskletJob, TaskletOutcome, TaskletStep, TerminalKind, TlsMode,
};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

const WORKER_ENV: &str = "OXIDEBATCH_M7_NESTED_JOB_WORKER";
const HANDSHAKE_ENV: &str = "OXIDEBATCH_M7_NESTED_JOB_HANDSHAKE";
const WAIT_BOUND: Duration = Duration::from_secs(20);
const SIGKILL: i32 = 9;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CrashPoint {
    BeforeLinkCommit,
    AfterLinkCommit,
    BeforeTerminalObservation,
    AfterTerminalObservation,
}

impl CrashPoint {
    const ALL: [Self; 4] = [
        Self::BeforeLinkCommit,
        Self::AfterLinkCommit,
        Self::BeforeTerminalObservation,
        Self::AfterTerminalObservation,
    ];

    const fn slug(self) -> &'static str {
        match self {
            Self::BeforeLinkCommit => "before-link-commit",
            Self::AfterLinkCommit => "after-link-commit",
            Self::BeforeTerminalObservation => "before-terminal-observation",
            Self::AfterTerminalObservation => "after-terminal-observation",
        }
    }

    const fn parent_job(self) -> &'static str {
        match self {
            Self::BeforeLinkCommit => "postgres_m7_nested_before_link_parent",
            Self::AfterLinkCommit => "postgres_m7_nested_after_link_parent",
            Self::BeforeTerminalObservation => "postgres_m7_nested_before_observe_parent",
            Self::AfterTerminalObservation => "postgres_m7_nested_after_observe_parent",
        }
    }

    const fn child_job(self) -> &'static str {
        match self {
            Self::BeforeLinkCommit => "postgres_m7_nested_before_link_child",
            Self::AfterLinkCommit => "postgres_m7_nested_after_link_child",
            Self::BeforeTerminalObservation => "postgres_m7_nested_before_observe_child",
            Self::AfterTerminalObservation => "postgres_m7_nested_after_observe_child",
        }
    }

    fn parse(value: &str) -> Result<Self, Box<dyn Error>> {
        match value {
            "before-link-commit" => Ok(Self::BeforeLinkCommit),
            "after-link-commit" => Ok(Self::AfterLinkCommit),
            "before-terminal-observation" => Ok(Self::BeforeTerminalObservation),
            "after-terminal-observation" => Ok(Self::AfterTerminalObservation),
            _ => Err("unknown nested-job crash point".into()),
        }
    }

    const fn expects_committed_first_link(self) -> bool {
        !matches!(self, Self::BeforeLinkCommit)
    }

    const fn expects_completed_original_child(self) -> bool {
        matches!(
            self,
            Self::BeforeTerminalObservation | Self::AfterTerminalObservation
        )
    }
}

#[derive(Clone, Copy)]
struct FixedClock(SystemTime);

impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        self.0
    }
}

#[derive(Clone, Copy)]
enum ChildMode {
    Worker(CrashPoint),
    Restart(CrashPoint),
}

struct BoundaryChild {
    mode: ChildMode,
    handshake: PathBuf,
}

impl BoundaryChild {
    fn require_original(context: TaskletContext<'_>) -> Result<(), TaskletError> {
        let name = ParameterName::new("child_key").map_err(TaskletError::from_error)?;
        let observed = context
            .parameters()
            .get(&name)
            .and_then(|parameter| parameter.value().as_str());
        if observed == Some("original") {
            Ok(())
        } else {
            Err(TaskletError::new())
        }
    }

    fn write_marker(path: &Path) -> Result<(), TaskletError> {
        std::fs::write(path, []).map_err(|_| TaskletError::new())
    }

    fn append_business_effect(path: &Path) -> Result<(), TaskletError> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|_| TaskletError::new())?;
        writeln!(file, "effect").map_err(|_| TaskletError::new())
    }
}

impl Tasklet for BoundaryChild {
    fn execute<'a>(
        &'a self,
        context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            Self::require_original(context)?;
            match self.mode {
                ChildMode::Worker(CrashPoint::BeforeLinkCommit) => {
                    panic!("child user work started before nested link committed")
                }
                ChildMode::Worker(CrashPoint::AfterLinkCommit) => {
                    Self::write_marker(&self.handshake.join("child-entered"))?;
                    loop {
                        tokio::time::sleep(Duration::from_mins(1)).await;
                    }
                }
                ChildMode::Worker(
                    CrashPoint::BeforeTerminalObservation | CrashPoint::AfterTerminalObservation,
                ) => {
                    Self::write_marker(&self.handshake.join("child-entered"))?;
                    wait_for_file(&self.handshake.join("release-child"))
                        .await
                        .map_err(|_| TaskletError::new())?;
                    Self::append_business_effect(&self.handshake.join("business-effects"))?;
                    Ok(TaskletOutcome::Completed)
                }
                ChildMode::Restart(CrashPoint::BeforeLinkCommit | CrashPoint::AfterLinkCommit) => {
                    Self::append_business_effect(&self.handshake.join("business-effects"))?;
                    Ok(TaskletOutcome::Completed)
                }
                ChildMode::Restart(
                    CrashPoint::BeforeTerminalObservation | CrashPoint::AfterTerminalObservation,
                ) => panic!("completed nested child user work was executed again"),
            }
        })
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

fn parent_parameters(point: CrashPoint, child_key: &str) -> Result<JobParameters, Box<dyn Error>> {
    Ok(JobParameters::try_from_iter([
        (
            ParameterName::new("run")?,
            JobParameter::new(
                ParameterValue::string(point.slug())?,
                ParameterRole::Identifying,
            ),
        ),
        (
            ParameterName::new("child_key")?,
            JobParameter::new(
                ParameterValue::string(child_key)?,
                ParameterRole::NonIdentifying,
            ),
        ),
    ])?)
}

fn child_job(
    point: CrashPoint,
    mode: ChildMode,
    handshake: PathBuf,
) -> Result<Arc<TaskletJob>, Box<dyn Error>> {
    Ok(Arc::new(TaskletJob::new(
        JobName::new(point.child_job())?,
        TaskletStep::new(
            StepName::new("nested-child-step")?,
            Arc::new(BoundaryChild { mode, handshake }),
        ),
        DefinitionRevision::new("m7-nested-child-v1")?,
        &ComponentRevision::new("m7-nested-child-tasklet-v1")?,
    )?))
}

fn parent_job(
    point: CrashPoint,
    mode: ChildMode,
    handshake: PathBuf,
) -> Result<(FlowJob, NodeId), Box<dyn Error>> {
    let child = child_job(point, mode, handshake)?;
    let nested = NodeId::new("nested-child")?;
    let nested_node = NestedJobNode::new(
        nested.clone(),
        child.definition_identity().clone(),
        ComponentRevision::new("m7-nested-mapping-v1")?,
        vec![NestedJobParameterMapping::new(
            ParameterName::new("child_key")?,
            ParameterRole::Identifying,
            NestedJobParameterSource::ParentParameter(ParameterName::new("child_key")?),
            ParameterValueKind::String,
            ParameterCoercion::Exact,
            MissingParameterPolicy::Fail,
        )],
    )?;
    let plan = FlowGraph::new(nested.clone())
        .with_node(FlowNode::nested_job(nested_node))
        .with_sequence(nested.clone(), FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(
            &JobName::new(point.parent_job())?,
            DefinitionRevision::new("m7-nested-parent-v1")?,
        )?;
    Ok((
        FlowJob::new(JobName::new(point.parent_job())?, plan)?
            .with_nested_tasklet_job(nested.clone(), child)?,
        nested,
    ))
}

fn handshake_directory(point: CrashPoint) -> Result<PathBuf, Box<dyn Error>> {
    let path = std::env::temp_dir().join(format!(
        "oxide-batch-m7-nested-{}-{}",
        point.slug(),
        std::process::id()
    ));
    if path.exists() {
        std::fs::remove_dir_all(&path)?;
    }
    std::fs::create_dir_all(&path)?;
    Ok(path)
}

async fn wait_for_file(path: &Path) -> Result<(), Box<dyn Error>> {
    let started = Instant::now();
    while !path.exists() {
        if started.elapsed() >= WAIT_BOUND {
            return Err(format!("timed out waiting for {}", path.display()).into());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok(())
}

async fn wait_for_lock_wait(pool: &PgPool, relation_text: &str) -> Result<(), Box<dyn Error>> {
    let started = Instant::now();
    loop {
        let waiting: bool = sqlx::query_scalar(
            "SELECT EXISTS (\
             SELECT 1 FROM pg_stat_activity \
             WHERE datname = current_database() \
               AND pid <> pg_backend_pid() \
               AND wait_event_type = 'Lock' \
               AND position($1 in query) > 0)",
        )
        .bind(relation_text)
        .fetch_one(pool)
        .await?;
        if waiting {
            return Ok(());
        }
        if started.elapsed() >= WAIT_BOUND {
            return Err(format!("timed out waiting for PostgreSQL lock on {relation_text}").into());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for_child_status(
    pool: &PgPool,
    child_execution_id: i64,
    expected: &str,
) -> Result<(), Box<dyn Error>> {
    let started = Instant::now();
    loop {
        let status: Option<String> =
            sqlx::query_scalar("SELECT status FROM oxide_batch.ob_job_execution WHERE id = $1")
                .bind(child_execution_id)
                .fetch_optional(pool)
                .await?;
        if status.as_deref() == Some(expected) {
            return Ok(());
        }
        if started.elapsed() >= WAIT_BOUND {
            return Err(format!("timed out waiting for child status {expected}").into());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for_terminal_observation(
    pool: &PgPool,
    parent_job: &str,
) -> Result<(), Box<dyn Error>> {
    let started = Instant::now();
    loop {
        let terminal: Option<String> = sqlx::query_scalar(
            "SELECT link.terminal_status \
             FROM oxide_batch.ob_nested_job_link link \
             JOIN oxide_batch.ob_job_execution execution \
               ON execution.id = link.parent_job_execution_id \
             JOIN oxide_batch.ob_job_instance instance \
               ON instance.id = execution.job_instance_id \
             WHERE instance.job_name = $1 \
             ORDER BY link.id DESC LIMIT 1",
        )
        .bind(parent_job)
        .fetch_optional(pool)
        .await?
        .flatten();
        if terminal.as_deref() == Some("COMPLETED") {
            return Ok(());
        }
        if started.elapsed() >= WAIT_BOUND {
            return Err("timed out waiting for nested terminal observation".into());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn link_row(
    pool: &PgPool,
    parent_job: &str,
) -> Result<Option<(i64, i64, i64, Option<String>)>, Box<dyn Error>> {
    let row = sqlx::query(
        "SELECT link.id, link.parent_job_execution_id, link.child_job_instance_id, \
                link.child_job_execution_id, link.terminal_status \
         FROM oxide_batch.ob_nested_job_link link \
         JOIN oxide_batch.ob_job_execution execution \
           ON execution.id = link.parent_job_execution_id \
         JOIN oxide_batch.ob_job_instance instance \
           ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = $1 \
         ORDER BY link.id DESC LIMIT 1",
    )
    .bind(parent_job)
    .fetch_optional(pool)
    .await?;
    row.map(|row| {
        Ok((
            row.try_get::<i64, _>("id")?,
            row.try_get::<i64, _>("child_job_instance_id")?,
            row.try_get::<i64, _>("child_job_execution_id")?,
            row.try_get::<Option<String>, _>("terminal_status")?,
        ))
    })
    .transpose()
}

async fn lock_link_row<'a>(
    pool: &'a PgPool,
    parent_job: &str,
) -> Result<sqlx::Transaction<'a, sqlx::Postgres>, Box<dyn Error>> {
    let mut transaction = pool.begin().await?;
    let _: i64 = sqlx::query_scalar(
        "SELECT link.id \
         FROM oxide_batch.ob_nested_job_link link \
         JOIN oxide_batch.ob_job_execution execution \
           ON execution.id = link.parent_job_execution_id \
         JOIN oxide_batch.ob_job_instance instance \
           ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = $1 \
         ORDER BY link.id DESC LIMIT 1 \
         FOR UPDATE OF link",
    )
    .bind(parent_job)
    .fetch_one(&mut *transaction)
    .await?;
    Ok(transaction)
}

async fn remove_jobs(pool: &PgPool, parent: &str, child: &str) -> Result<(), Box<dyn Error>> {
    let statements = [
        "DELETE FROM oxide_batch.ob_nested_job_link WHERE parent_job_instance_id IN (\
             SELECT id FROM oxide_batch.ob_job_instance WHERE job_name IN ($1, $2)) \
          OR child_job_instance_id IN (\
             SELECT id FROM oxide_batch.ob_job_instance WHERE job_name IN ($1, $2))",
        "DELETE FROM oxide_batch.ob_flow_decision WHERE job_execution_id IN (\
             SELECT execution.id FROM oxide_batch.ob_job_execution execution \
             JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
             WHERE instance.job_name IN ($1, $2))",
        "DELETE FROM oxide_batch.ob_recovery_decision WHERE job_execution_id IN (\
             SELECT execution.id FROM oxide_batch.ob_job_execution execution \
             JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
             WHERE instance.job_name IN ($1, $2))",
        "DELETE FROM oxide_batch.ob_step_execution WHERE job_execution_id IN (\
             SELECT execution.id FROM oxide_batch.ob_job_execution execution \
             JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
             WHERE instance.job_name IN ($1, $2))",
        "DELETE FROM oxide_batch.ob_job_execution WHERE job_instance_id IN (\
             SELECT id FROM oxide_batch.ob_job_instance WHERE job_name IN ($1, $2))",
        "DELETE FROM oxide_batch.ob_job_instance WHERE job_name IN ($1, $2)",
        "DELETE FROM oxide_batch.ob_definition_upgrade WHERE from_definition_id IN (\
             SELECT id FROM oxide_batch.ob_job_definition WHERE job_name IN ($1, $2)) \
          OR to_definition_id IN (\
             SELECT id FROM oxide_batch.ob_job_definition WHERE job_name IN ($1, $2))",
        "DELETE FROM oxide_batch.ob_job_definition WHERE job_name IN ($1, $2)",
    ];
    for statement in statements {
        sqlx::query(statement)
            .bind(parent)
            .bind(child)
            .execute(pool)
            .await?;
    }
    Ok(())
}

fn spawn_worker(point: CrashPoint, handshake: &Path) -> Result<Child, Box<dyn Error>> {
    Ok(Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("nested_job_crash_worker_process")
        .arg("--nocapture")
        .env(WORKER_ENV, point.slug())
        .env(HANDSHAKE_ENV, handshake)
        .spawn()?)
}

fn kill_worker(child: &mut Child) -> Result<(), Box<dyn Error>> {
    child.kill()?;
    let status = child.wait()?;
    assert_eq!(status.signal(), Some(SIGKILL));
    Ok(())
}
fn business_effect_count(handshake: &Path) -> Result<usize, Box<dyn Error>> {
    let path = handshake.join("business-effects");
    if !path.exists() {
        return Ok(0);
    }
    Ok(std::fs::read_to_string(path)?.lines().count())
}

async fn recover_execution(
    repository: &PostgresJobRepository,
    id: JobExecutionId,
    reason: &str,
    failure_id: u64,
) -> Result<JobExecution, Box<dyn Error>> {
    let mut unit = repository.begin().await?;
    let execution = unit
        .get_job_execution(id)
        .await?
        .ok_or("execution missing during recovery")?;
    let request = RecoveryRequest::mark_failed(
        execution.version(),
        reason,
        "m7-nested-job-sigkill-harness",
        [91; 32],
        FailureCategory::PermanentInfrastructure,
        FailureId::new(failure_id)?,
    )?;
    let result = unit.recover_job_execution(id, &request).await?;
    unit.commit().await?;
    Ok(result.execution().clone())
}

async fn original_parent_execution(
    repository: &PostgresJobRepository,
    point: CrashPoint,
) -> Result<JobExecution, Box<dyn Error>> {
    let parameters = parent_parameters(point, "original")?;
    let key = JobInstanceKey::new(JobName::new(point.parent_job())?, &parameters);
    let mut unit = repository.begin().await?;
    let instance = unit
        .find_job_instance(&key)
        .await?
        .ok_or("worker did not create parent instance")?;
    let execution = unit
        .job_executions(instance.id())
        .await?
        .into_iter()
        .next()
        .ok_or("worker did not create parent execution")?;
    unit.rollback().await?;
    Ok(execution)
}

async fn run_worker(
    point: CrashPoint,
    handshake: PathBuf,
    url: String,
) -> Result<(), Box<dyn Error>> {
    let clock = FixedClock(SystemTime::UNIX_EPOCH + Duration::from_secs(40_000));
    let repository = PostgresJobRepository::connect(config(url)?, Arc::new(clock)).await?;
    let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
    let (_, stop) = StopSource::new();
    let (job, _) = parent_job(point, ChildMode::Worker(point), handshake)?;
    let _ = FlowLauncher::new(&repository, &clock, &ids)
        .launch(&job, &parent_parameters(point, "original")?, &stop)
        .await?;
    Err("nested-job worker crossed selected SIGKILL boundary".into())
}

#[allow(
    clippy::too_many_lines,
    reason = "one evidence chain owns lock positioning, SIGKILL, durable inspection, recovery, and restart assertions"
)]
async fn run_parent_scenario(
    point: CrashPoint,
    runtime_url: String,
    migrator_url: String,
) -> Result<(), Box<dyn Error>> {
    PostgresMigrator::migrate(&config(migrator_url.clone())?).await?;
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&migrator_url)
        .await?;
    remove_jobs(&pool, point.parent_job(), point.child_job()).await?;
    let handshake = handshake_directory(point)?;

    let mut pre_link_lock = if point == CrashPoint::BeforeLinkCommit {
        let mut transaction = pool.begin().await?;
        sqlx::query("LOCK TABLE oxide_batch.ob_nested_job_link IN SHARE MODE")
            .execute(&mut *transaction)
            .await?;
        Some(transaction)
    } else {
        None
    };

    let mut child = spawn_worker(point, &handshake)?;
    let boundary_result: Result<(), Box<dyn Error>> = async {
        match point {
            CrashPoint::BeforeLinkCommit => {
                wait_for_lock_wait(&pool, "ob_nested_job_link").await?;
                kill_worker(&mut child)?;
                if let Some(transaction) = pre_link_lock.take() {
                    transaction.rollback().await?;
                }
                assert!(link_row(&pool, point.parent_job()).await?.is_none());
                assert_eq!(business_effect_count(&handshake)?, 0);
            }
            CrashPoint::AfterLinkCommit => {
                wait_for_file(&handshake.join("child-entered")).await?;
                let link = link_row(&pool, point.parent_job())
                    .await?
                    .ok_or("link must commit before child tasklet entry")?;
                assert!(link.3.is_none());
                wait_for_child_status(&pool, link.2, "STARTED").await?;
                assert_eq!(business_effect_count(&handshake)?, 0);
                kill_worker(&mut child)?;
            }
            CrashPoint::BeforeTerminalObservation => {
                wait_for_file(&handshake.join("child-entered")).await?;
                let link = link_row(&pool, point.parent_job())
                    .await?
                    .ok_or("nested link missing before terminal observation")?;
                let link_lock = lock_link_row(&pool, point.parent_job()).await?;
                std::fs::write(handshake.join("release-child"), [])?;
                wait_for_child_status(&pool, link.2, "COMPLETED").await?;
                wait_for_lock_wait(&pool, "ob_nested_job_link").await?;
                assert_eq!(business_effect_count(&handshake)?, 1);
                kill_worker(&mut child)?;
                link_lock.rollback().await?;
                let durable = link_row(&pool, point.parent_job())
                    .await?
                    .ok_or("nested link disappeared after worker kill")?;
                assert!(durable.3.is_none());
            }
            CrashPoint::AfterTerminalObservation => {
                wait_for_file(&handshake.join("child-entered")).await?;
                let mut decision_lock = pool.begin().await?;
                sqlx::query("LOCK TABLE oxide_batch.ob_flow_decision IN SHARE MODE")
                    .execute(&mut *decision_lock)
                    .await?;
                std::fs::write(handshake.join("release-child"), [])?;
                wait_for_terminal_observation(&pool, point.parent_job()).await?;
                wait_for_lock_wait(&pool, "ob_flow_decision").await?;
                assert_eq!(business_effect_count(&handshake)?, 1);
                kill_worker(&mut child)?;
                decision_lock.rollback().await?;
                let durable = link_row(&pool, point.parent_job())
                    .await?
                    .ok_or("nested link disappeared after terminal observation")?;
                assert_eq!(durable.3.as_deref(), Some("COMPLETED"));
            }
        }
        Ok(())
    }
    .await;
    if boundary_result.is_err() {
        let _ = child.kill();
        let _ = child.wait();
        if let Some(transaction) = pre_link_lock.take() {
            let _ = transaction.rollback().await;
        }
        boundary_result?;
    }

    let clock = FixedClock(SystemTime::UNIX_EPOCH + Duration::from_secs(40_100));
    let repository = PostgresJobRepository::connect(config(runtime_url)?, Arc::new(clock)).await?;
    let original_parent = original_parent_execution(&repository, point).await?;
    assert_eq!(original_parent.metadata().status(), BatchStatus::Started);

    let (_, nested) = parent_job(point, ChildMode::Restart(point), handshake.clone())?;
    let first_link = {
        let mut unit = repository.begin().await?;
        let link = unit.nested_job_link(original_parent.id(), &nested).await?;
        unit.rollback().await?;
        link
    };
    assert_eq!(first_link.is_some(), point.expects_committed_first_link());

    if point == CrashPoint::AfterLinkCommit {
        let link = first_link.as_ref().ok_or("after-link link missing")?;
        let child_execution = {
            let mut unit = repository.begin().await?;
            let execution = unit
                .get_job_execution(link.child_job_execution_id())
                .await?
                .ok_or("linked child execution missing")?;
            unit.rollback().await?;
            execution
        };
        assert_eq!(child_execution.metadata().status(), BatchStatus::Started);
        recover_execution(
            &repository,
            child_execution.id(),
            "M7_NESTED_CHILD_PROCESS_KILL_INSPECTED",
            40_001,
        )
        .await?;
    } else if point.expects_completed_original_child() {
        let link = first_link.as_ref().ok_or("completed child link missing")?;
        let mut unit = repository.begin().await?;
        let child_execution = unit
            .get_job_execution(link.child_job_execution_id())
            .await?
            .ok_or("completed linked child execution missing")?;
        unit.rollback().await?;
        assert_eq!(child_execution.metadata().status(), BatchStatus::Completed);
    }

    recover_execution(
        &repository,
        original_parent.id(),
        "M7_NESTED_PARENT_PROCESS_KILL_INSPECTED",
        40_002,
    )
    .await?;

    let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
    let (_, stop) = StopSource::new();
    let (restart_job, nested) = parent_job(point, ChildMode::Restart(point), handshake.clone())?;
    let restart_child_key = if point == CrashPoint::BeforeLinkCommit {
        "original"
    } else {
        "drifted"
    };
    let report = FlowLauncher::new(&repository, &clock, &ids)
        .launch(
            &restart_job,
            &parent_parameters(point, restart_child_key)?,
            &stop,
        )
        .await?;
    assert_eq!(report.outcome(), &FlowExecutionOutcome::Completed);
    assert_eq!(business_effect_count(&handshake)?, 1);

    let mut inspect = repository.begin().await?;
    let restart_link = inspect
        .nested_job_link(report.job_execution().id(), &nested)
        .await?
        .ok_or("restart nested link missing")?;
    let persisted = inspect
        .nested_job_parameters(report.job_execution().id(), &nested)
        .await?;
    let restart_decisions = inspect.flow_decisions(report.job_execution().id()).await?;
    let original_decisions = inspect.flow_decisions(original_parent.id()).await?;
    inspect.rollback().await?;

    assert_eq!(
        persisted
            .get(&ParameterName::new("child_key")?)
            .and_then(|parameter| parameter.value().as_str()),
        Some("original")
    );
    assert_eq!(restart_decisions.len(), 1);
    assert!(original_decisions.is_empty());

    if let Some(first_link) = first_link {
        assert_eq!(
            restart_link.child_job_instance_id(),
            first_link.child_job_instance_id()
        );
        if point == CrashPoint::AfterLinkCommit {
            assert_ne!(
                restart_link.child_job_execution_id(),
                first_link.child_job_execution_id()
            );
        } else {
            assert_eq!(
                restart_link.child_job_execution_id(),
                first_link.child_job_execution_id()
            );
        }
    }

    repository.close().await?;
    remove_jobs(&pool, point.parent_job(), point.child_job()).await?;
    pool.close().await;
    std::fs::remove_dir_all(handshake)?;
    Ok(())
}

#[test]
fn nested_job_crash_worker_process() -> Result<(), Box<dyn Error>> {
    let Ok(value) = std::env::var(WORKER_ENV) else {
        return Ok(());
    };
    let point = CrashPoint::parse(&value)?;
    let handshake = PathBuf::from(std::env::var(HANDSHAKE_ENV)?);
    let url = runtime_url().ok_or("nested-job worker database URL is missing")?;
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run_worker(point, handshake, url))
}

#[test]
fn sigkill_restart_preserves_nested_link_and_completed_child_work() -> Result<(), Box<dyn Error>> {
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
    for point in CrashPoint::ALL {
        runtime.block_on(run_parent_scenario(
            point,
            runtime_url.clone(),
            migrator_url.clone(),
        ))?;
    }
    Ok(())
}