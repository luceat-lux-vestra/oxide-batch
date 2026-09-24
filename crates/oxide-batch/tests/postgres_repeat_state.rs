//! `PostgreSQL` SIGKILL and corruption evidence for M7 #300 durable repeat state.

#![cfg(all(feature = "postgres", unix))]
#![allow(clippy::panic)]

use std::error::Error;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::{Duration, Instant};

use oxide_batch::{
    ComponentRevision, DefinitionRevision, ExecutionContext, FlowGraph, FlowNode, FlowTarget,
    JobInstanceKey, JobName, JobParameters, JobRepository, NodeId, PostgresConfig,
    PostgresJobRepository, PostgresMigrator, RepeatCommitRequest, RepeatDecision, RepeatDefinition,
    RepeatId, RepeatOrdinal, RepeatPolicyConfiguration, RepeatPolicyDefinition, RepeatPolicyKind,
    RepeatStateSchema, RepositoryError, StartLimit, StateLimits, StateSchemaId, StateSchemaVersion,
    StepComponents, StepName, StepNode, SystemClock, TerminalKind, TlsMode,
};
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPoolOptions;
use sqlx::types::Json;

const JOB: &str = "postgres_m7_repeat_sigkill";
const WORKER_ENV: &str = "OXIDEBATCH_M7_REPEAT_WORKER";
const INSTANCE_ENV: &str = "OXIDEBATCH_M7_REPEAT_INSTANCE";
const STEP_ENV: &str = "OXIDEBATCH_M7_REPEAT_STEP";
const HANDSHAKE_ENV: &str = "OXIDEBATCH_M7_REPEAT_HANDSHAKE";
const WAIT_BOUND: Duration = Duration::from_secs(20);
const SIGKILL: i32 = 9;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CrashPoint {
    BeforeCommit,
    AfterCommit,
}

impl CrashPoint {
    const fn slug(self) -> &'static str {
        match self {
            Self::BeforeCommit => "before-commit",
            Self::AfterCommit => "after-commit",
        }
    }

    fn parse(value: &str) -> Result<Self, Box<dyn Error>> {
        match value {
            "before-commit" => Ok(Self::BeforeCommit),
            "after-commit" => Ok(Self::AfterCommit),
            _ => Err("unknown repeat crash point".into()),
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

fn plan() -> Result<oxide_batch::CompiledExecutionPlan, Box<dyn Error>> {
    let node = NodeId::new("repeat-step")?;
    let repeat = RepeatDefinition::new(
        RepeatId::new("window")?,
        RepeatPolicyDefinition::new(
            RepeatPolicyKind::new("bounded-count")?,
            ComponentRevision::new("policy-v1")?,
            RepeatPolicyConfiguration::new("limit-3")?,
        ),
        Vec::new(),
        RepeatStateSchema::new(
            StateSchemaId::new("repeat.state")?,
            StateSchemaVersion::new(1)?,
        ),
    )?;
    Ok(FlowGraph::new(node.clone())
        .with_node(FlowNode::step(
            StepNode::new(
                node.clone(),
                StepName::new("repeat-step")?,
                StepComponents::Tasklet(ComponentRevision::new("tasklet-v1")?),
            )
            .with_repeat_definition(repeat),
        ))
        .with_sequence(node, FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(&JobName::new(JOB)?, DefinitionRevision::new("v1")?)?)
}

fn state(schema_version: u32) -> Result<ExecutionContext, Box<dyn Error>> {
    let bytes = serde_json::to_vec(&serde_json::json!({
        "format": "oxide-batch.execution-context",
        "format_version": 1,
        "schema": "repeat.state",
        "schema_version": schema_version,
        "payload": { "cursor": "worker-state" }
    }))?;
    Ok(ExecutionContext::from_json(&bytes, StateLimits::default())?)
}

fn request(
    plan: &oxide_batch::CompiledExecutionPlan,
    instance: oxide_batch::JobInstanceId,
    step: oxide_batch::StepExecutionId,
) -> Result<RepeatCommitRequest, Box<dyn Error>> {
    Ok(RepeatCommitRequest::new(
        instance,
        NodeId::new("repeat-step")?,
        step,
        RepeatId::new("window")?,
        RepeatOrdinal::INITIAL,
        state(1)?,
        RepeatDecision::Continue,
        *plan.fingerprint(),
    ))
}

async fn remove_fixture(url: &str) -> Result<(), sqlx::Error> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    for statement in [
        "DELETE FROM oxide_batch.ob_job_execution WHERE job_instance_id IN (\
         SELECT id FROM oxide_batch.ob_job_instance WHERE job_name = $1)",
        "DELETE FROM oxide_batch.ob_job_instance WHERE job_name = $1",
        "DELETE FROM oxide_batch.ob_definition_upgrade WHERE from_definition_id IN (\
         SELECT id FROM oxide_batch.ob_job_definition WHERE job_name = $1) \
         OR to_definition_id IN (\
         SELECT id FROM oxide_batch.ob_job_definition WHERE job_name = $1)",
        "DELETE FROM oxide_batch.ob_job_definition WHERE job_name = $1",
    ] {
        sqlx::query(statement).bind(JOB).execute(&pool).await?;
    }
    pool.close().await;
    Ok(())
}

async fn create_owner(
    repository: &PostgresJobRepository,
    plan: &oxide_batch::CompiledExecutionPlan,
) -> Result<(oxide_batch::JobInstanceId, oxide_batch::StepExecutionId), Box<dyn Error>> {
    let key = JobInstanceKey::new(JobName::new(JOB)?, &JobParameters::new());
    let mut unit = repository.begin().await?;
    let instance = unit
        .select_or_create_job_instance(&key)
        .await?
        .instance()
        .clone();
    let job = unit
        .create_job_execution_with_definition(instance.id(), plan.definition_identity())
        .await?;
    let step = unit
        .create_flow_step_execution(
            job.id(),
            &StepName::new("repeat-step")?,
            &NodeId::new("repeat-step")?,
            StartLimit::UNRESTRICTED,
        )
        .await?;
    unit.commit().await?;
    Ok((instance.id(), step.id()))
}

fn handshake_path(point: CrashPoint) -> PathBuf {
    std::env::temp_dir().join(format!(
        "oxide-batch-m7-repeat-{}-{}",
        std::process::id(),
        point.slug()
    ))
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

fn spawn_worker(
    point: CrashPoint,
    instance: oxide_batch::JobInstanceId,
    step: oxide_batch::StepExecutionId,
    handshake: &Path,
) -> Result<Child, Box<dyn Error>> {
    Ok(Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("repeat_crash_worker_process")
        .arg("--nocapture")
        .env(WORKER_ENV, point.slug())
        .env(INSTANCE_ENV, instance.get().to_string())
        .env(STEP_ENV, step.get().to_string())
        .env(HANDSHAKE_ENV, handshake)
        .spawn()?)
}

fn kill_worker(child: &mut Child) -> Result<(), Box<dyn Error>> {
    child.kill()?;
    let status = child.wait()?;
    assert_eq!(status.signal(), Some(SIGKILL));
    Ok(())
}

async fn repeat_count(url: &str, step: oxide_batch::StepExecutionId) -> Result<i64, sqlx::Error> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    let count = sqlx::query_scalar(
        "SELECT count(*) FROM oxide_batch.ob_repeat_execution WHERE step_execution_id = $1",
    )
    .bind(i64::try_from(step.get()).unwrap_or(i64::MAX))
    .fetch_one(&pool)
    .await?;
    pool.close().await;
    Ok(count)
}

async fn run_worker(
    point: CrashPoint,
    instance: oxide_batch::JobInstanceId,
    step: oxide_batch::StepExecutionId,
    handshake: PathBuf,
    url: String,
) -> Result<(), Box<dyn Error>> {
    let repository = PostgresJobRepository::connect(config(url)?, Arc::new(SystemClock)).await?;
    let plan = plan()?;
    let request = request(&plan, instance, step)?;
    let mut unit = repository.begin().await?;
    unit.commit_repeat_iteration(&request).await?;

    if point == CrashPoint::BeforeCommit {
        std::fs::write(&handshake, b"staged")?;
        loop {
            tokio::time::sleep(Duration::from_mins(1)).await;
        }
    }

    unit.commit().await?;
    std::fs::write(&handshake, b"committed")?;
    loop {
        tokio::time::sleep(Duration::from_mins(1)).await;
    }
}

#[test]
fn repeat_crash_worker_process() -> Result<(), Box<dyn Error>> {
    let Some(point) = std::env::var(WORKER_ENV).ok() else {
        return Ok(());
    };
    let url = runtime_url().ok_or("repeat worker has no runtime URL")?;
    let instance = oxide_batch::JobInstanceId::new(std::env::var(INSTANCE_ENV)?.parse()?)?;
    let step = oxide_batch::StepExecutionId::new(std::env::var(STEP_ENV)?.parse()?)?;
    let handshake = PathBuf::from(std::env::var(HANDSHAKE_ENV)?);
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(run_worker(
            CrashPoint::parse(&point)?,
            instance,
            step,
            handshake,
            url,
        ))
}

#[test]
fn repeat_commit_boundary_survives_sigkill_and_corruption_fails_closed()
-> Result<(), Box<dyn Error>> {
    let Some(runtime) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let Some(migrator) = migrator_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL is not set");
        return Ok(());
    };
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(async {
        PostgresMigrator::migrate(&config(migrator.clone())?).await?;
        remove_fixture(&migrator).await?;

        let repository =
            PostgresJobRepository::connect(config(runtime.clone())?, Arc::new(SystemClock)).await?;
        let plan = plan()?;
        let (instance, step) = create_owner(&repository, &plan).await?;

        let before_path = handshake_path(CrashPoint::BeforeCommit);
        let _ = std::fs::remove_file(&before_path);
        let mut before = spawn_worker(CrashPoint::BeforeCommit, instance, step, &before_path)?;
        wait_for_file(&before_path).await?;
        kill_worker(&mut before)?;
        assert_eq!(
            repeat_count(&migrator, step).await?,
            0,
            "SIGKILL before repository commit must replay ordinal zero",
        );

        let after_path = handshake_path(CrashPoint::AfterCommit);
        let _ = std::fs::remove_file(&after_path);
        let mut after = spawn_worker(CrashPoint::AfterCommit, instance, step, &after_path)?;
        wait_for_file(&after_path).await?;
        kill_worker(&mut after)?;
        assert_eq!(
            repeat_count(&migrator, step).await?,
            1,
            "accepted iterations retain one current row, not an iteration log",
        );

        let mut fresh = repository.begin().await?;
        let durable = fresh
            .repeat_execution(step, &RepeatId::new("window")?)
            .await?
            .ok_or_else(|| std::io::Error::other("post-commit SIGKILL left no durable repeat authority"))?;
        assert_eq!(durable.ordinal(), RepeatOrdinal::INITIAL);
        assert_eq!(durable.decision(), RepeatDecision::Continue);
        let same = request(&plan, instance, step)?;
        assert_eq!(fresh.commit_repeat_iteration(&same).await?, durable);
        fresh.commit().await?;

        // Re-sign a newer application-state schema so checksum validation
        // succeeds. The read must still fail because schema version 2 is not
        // the repeat definition's declared version 1.
        let newer = state(2)?;
        let payload = serde_json::from_slice::<serde_json::Value>(&newer.payload_json()?)?;
        let checksum: [u8; 32] = Sha256::digest(newer.to_json()?).into();
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&migrator)
            .await?;
        sqlx::query(
            "UPDATE oxide_batch.ob_repeat_execution SET state_schema_version = $1, \
             state_payload = $2, state_checksum = $3 WHERE step_execution_id = $4 AND repeat_id = $5",
        )
        .bind(i32::try_from(newer.schema_version().get())?)
        .bind(Json(payload))
        .bind(&checksum[..])
        .bind(i64::try_from(step.get())?)
        .bind("window")
        .execute(&admin)
        .await?;
        admin.close().await;

        let mut corrupted = repository.begin().await?;
        assert_eq!(
            corrupted
                .repeat_execution(step, &RepeatId::new("window")?)
                .await,
            Err(RepositoryError::RepeatStateCorrupt),
        );
        corrupted.rollback().await?;

        repository.close().await?;
        remove_fixture(&migrator).await?;
        let _ = std::fs::remove_file(before_path);
        let _ = std::fs::remove_file(after_path);
        Ok::<(), Box<dyn Error>>(())
    })
}
