//! Process-kill evidence for M7 compatible-upgrade transformation.
//!
//! The three phases are reached without production-only hooks:
//!
//! - pre-transform blocks on the job-instance row lock taken before upgrade
//!   validation/seed construction;
//! - transform parks inside the application-owned pure transformer;
//! - commit blocks in a deferred constraint trigger while COMMIT is in flight.
//!
//! Every child is killed with SIGKILL. The parent then reads durable authority
//! back and proves that pre-transform/transform deaths create no target attempt,
//! while an in-flight commit produces exactly one complete target attempt rather
//! than a partial or duplicate restart.

#![cfg(all(feature = "postgres", unix))]
#![allow(clippy::expect_used, clippy::panic)]

mod crash_restore;

use std::error::Error;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use oxide_batch::{
    BatchStatus, DefinitionIdentity, DefinitionNodeUpgrade, DefinitionRevision,
    DefinitionStateKind, DefinitionStateTransform, DefinitionStateTransformError,
    DefinitionStateTransformId, DefinitionStateTransformer, DefinitionUpgrade,
    DefinitionUpgradeExecution, DefinitionUpgradeKey, DefinitionUpgradeRevision, ExecutionContext,
    FailureCategory, FailureId, FailureSummary, JobInstanceId, JobInstanceKey, JobName,
    JobParameters, JobRepository, LifecycleTransition, NodeId, PostgresJobRepository,
    PostgresMigrator, RepositoryError, StartLimit, StateLimits, StateSchemaId, StateSchemaVersion,
    StepName,
};
use sqlx::{Connection as _, PgConnection};

use crash_restore::{
    FixedClock, HANDSHAKE_BOUND, announce, config, handshake_directory, migrator_url,
    park_until_killed, runtime_url, wait_for_blocked_statement, wait_for_file,
};

const PHASE_ENV: &str = "OXIDEBATCH_M7_UPGRADE_KILL_PHASE";
const HANDSHAKE_ENV: &str = "OXIDEBATCH_M7_UPGRADE_KILL_HANDSHAKE";
const JOB_ENV: &str = "OXIDEBATCH_M7_UPGRADE_KILL_JOB";
const ADVISORY_KEY: i64 = 55_100_151;
const SIGKILL: i32 = 9;
const CONTEXT_SCHEMA: &str = "m7.compatible-upgrade.context";
const SOURCE_NODE: &str = "source";
const TARGET_NODE: &str = "target";
const SOURCE_STEP: &str = "source-step";
const TARGET_STEP: &str = "target-step";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    PreTransform,
    Transform,
    CommitInFlight,
}

impl Phase {
    const ALL: [Self; 3] = [Self::PreTransform, Self::Transform, Self::CommitInFlight];

    const fn id(self) -> &'static str {
        match self {
            Self::PreTransform => "pre-transform",
            Self::Transform => "transform",
            Self::CommitInFlight => "commit-in-flight",
        }
    }

    const fn job_name(self) -> &'static str {
        match self {
            Self::PreTransform => "m7_upgrade_kill_pre_transform",
            Self::Transform => "m7_upgrade_kill_transform",
            Self::CommitInFlight => "m7_upgrade_kill_commit",
        }
    }

    fn parse(value: &str) -> Result<Self, Box<dyn Error>> {
        Self::ALL
            .into_iter()
            .find(|phase| phase.id() == value)
            .ok_or_else(|| format!("unknown upgrade kill phase {value}").into())
    }
}

struct UpgradeFixture {
    instance_id: JobInstanceId,
}

#[test]
fn compatible_upgrade_process_kill_boundaries_are_atomic() -> Result<(), Box<dyn Error>> {
    let Some(runtime_url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let Some(migrator_url) = migrator_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL is not set");
        return Ok(());
    };

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            PostgresMigrator::migrate(&config(migrator_url.clone())?).await?;
            for phase in Phase::ALL {
                run_phase(phase, &runtime_url, &migrator_url).await?;
            }
            Ok::<(), Box<dyn Error>>(())
        })
}

async fn run_phase(
    phase: Phase,
    runtime_url: &str,
    migrator_url: &str,
) -> Result<(), Box<dyn Error>> {
    drop_commit_gate(migrator_url).await?;
    cleanup_job(migrator_url, phase.job_name()).await?;
    let fixture = prepare_source(runtime_url, phase.job_name()).await?;

    let mut block = PhaseBlock::arrange(phase, migrator_url, fixture.instance_id).await?;
    let handshake = handshake_directory(&format!("m7-upgrade-{}", phase.id()))?;
    let mut child = spawn_worker(phase, phase.job_name(), &handshake)?;
    let observed = drive_kill(
        phase,
        runtime_url,
        migrator_url,
        fixture.instance_id,
        &handshake,
        &mut child,
        &mut block,
    )
    .await;
    if observed.is_err() {
        let _ = child.kill();
        let _ = child.wait();
    }

    // A failed assertion must not leave a child or deferred trigger that
    // poisons the next test invocation.
    let _ = block.release().await;
    let _ = drop_commit_gate(migrator_url).await;
    let result = observed;
    let _ = cleanup_job(migrator_url, phase.job_name()).await;
    result
}

async fn drive_kill(
    phase: Phase,
    runtime_url: &str,
    migrator_url: &str,
    instance_id: JobInstanceId,
    handshake: &Path,
    child: &mut Child,
    block: &mut PhaseBlock,
) -> Result<(), Box<dyn Error>> {
    wait_for_file(&handshake.join("ready"), HANDSHAKE_BOUND).await?;
    announce(&handshake.join("go"));

    match phase {
        Phase::PreTransform => {
            wait_for_blocked_statement(
                migrator_url,
                "SELECT job_name, identifying_parameters",
                HANDSHAKE_BOUND,
            )
            .await?;
        }
        Phase::Transform => {
            wait_for_file(&handshake.join("reached"), HANDSHAKE_BOUND).await?;
        }
        Phase::CommitInFlight => {
            wait_for_blocked_statement(migrator_url, "COMMIT", HANDSHAKE_BOUND).await?;
        }
    }

    child.kill()?;
    let status = child.wait()?;
    assert_eq!(status.signal(), Some(SIGKILL));
    assert_eq!(status.code(), None);

    block.release().await?;
    let expected_attempts = if phase == Phase::CommitInFlight { 2 } else { 1 };
    wait_for_attempt_count(runtime_url, phase.job_name(), expected_attempts).await?;
    assert_phase_state(runtime_url, phase, instance_id).await
}

async fn prepare_source(runtime_url: &str, job: &str) -> Result<UpgradeFixture, Box<dyn Error>> {
    let repository = PostgresJobRepository::connect(
        config(runtime_url.to_owned())?,
        Arc::new(FixedClock(UNIX_EPOCH + Duration::from_secs(551))),
    )
    .await?;
    let job_name = JobName::new(job)?;
    let source = definition(&job_name, "v1", SOURCE_NODE, SOURCE_STEP, 1)?;
    let key = JobInstanceKey::new(job_name.clone(), &JobParameters::new());
    let source_node = NodeId::new(SOURCE_NODE)?;
    let source_step_name = StepName::new(SOURCE_STEP)?;

    let mut create = repository.begin().await?;
    let instance = create
        .select_or_create_job_instance(&key)
        .await?
        .instance()
        .clone();
    let source_job = create
        .create_job_execution_with_definition(instance.id(), &source)
        .await?;
    let source_step = create
        .create_flow_step_execution(
            source_job.id(),
            &source_step_name,
            &source_node,
            StartLimit::UNRESTRICTED,
        )
        .await?;
    create.commit().await?;

    let mut finish = repository.begin().await?;
    let started_job = finish
        .transition_job_execution(
            source_job.id(),
            source_job.version(),
            LifecycleTransition::new(BatchStatus::Started, at(552)),
        )
        .await?;
    let started_step = finish
        .transition_step_execution(
            source_step.id(),
            source_step.version(),
            LifecycleTransition::new(BatchStatus::Started, at(552)),
        )
        .await?;
    let with_context = finish
        .commit_step_execution_context(started_step.id(), started_step.version(), &context(1, 7)?)
        .await?;
    finish
        .transition_step_execution(
            with_context.id(),
            with_context.version(),
            LifecycleTransition::failed(
                at(553),
                FailureSummary::new(FailureCategory::UserComponent, FailureId::new(5511)?),
            ),
        )
        .await?;
    finish
        .transition_job_execution(
            started_job.id(),
            started_job.version(),
            LifecycleTransition::failed(
                at(553),
                FailureSummary::new(FailureCategory::UserComponent, FailureId::new(5512)?),
            ),
        )
        .await?;
    finish.commit().await?;

    let (edge, _) = selected_upgrade(&job_name, normal_transform)?;
    let mut register = repository.begin().await?;
    register
        .register_definition_upgrade(&job_name, &edge)
        .await?;
    register.commit().await?;
    repository.close().await?;

    Ok(UpgradeFixture {
        instance_id: instance.id(),
    })
}

fn selected_upgrade(
    job_name: &JobName,
    transformer: fn(&[u8]) -> Result<Vec<u8>, DefinitionStateTransformError>,
) -> Result<(DefinitionUpgrade, DefinitionUpgradeExecution), Box<dyn Error>> {
    let source_node = NodeId::new(SOURCE_NODE)?;
    let target_node = NodeId::new(TARGET_NODE)?;
    let source = definition(job_name, "v1", SOURCE_NODE, SOURCE_STEP, 1)?;
    let target = definition(job_name, "v2", TARGET_NODE, TARGET_STEP, 2)?;
    let transform_revision = DefinitionUpgradeRevision::new("context-v1-to-v2")?;
    let edge = DefinitionUpgrade::transformed(
        DefinitionUpgradeKey::new("explicit-v1-to-v2")?,
        DefinitionUpgradeRevision::new("edge-v1")?,
        source,
        target,
        [DefinitionNodeUpgrade::new(
            source_node.clone(),
            target_node.clone(),
        )],
        [DefinitionStateTransform::new(
            source_node,
            target_node,
            DefinitionStateKind::ExecutionContext,
            DefinitionStateTransformId::new("context")?,
            StateSchemaId::new(CONTEXT_SCHEMA)?,
            StateSchemaVersion::new(1)?,
            StateSchemaId::new(CONTEXT_SCHEMA)?,
            StateSchemaVersion::new(2)?,
            transform_revision.clone(),
        )],
    )?;
    let selected = DefinitionUpgradeExecution::new(
        edge.clone(),
        [DefinitionStateTransformer::new(
            transform_revision,
            transformer,
        )],
    )?;
    Ok((edge, selected))
}

fn definition(
    job_name: &JobName,
    revision: &str,
    node: &str,
    step: &str,
    schema_version: u32,
) -> Result<DefinitionIdentity, Box<dyn Error>> {
    let manifest = serde_json::json!({
        "entry": node,
        "format": 4,
        "job": job_name.as_str(),
        "nodes": [{
            "handler_revision": revision,
            "id": node,
            "kind": "custom_leaf",
            "leaf_kind": "m7.compatible-upgrade-kill",
            "listeners": [],
            "policy": null,
            "start": {
                "allow_start_if_complete": false,
                "start_limit": u32::MAX,
            },
            "state_schema": {
                "id": CONTEXT_SCHEMA,
                "version": schema_version,
            },
            "step_name": step,
        }],
        "transitions": [],
    });
    let bytes = serde_json::to_vec(&manifest)?;
    Ok(DefinitionIdentity::from_flow_manifest(
        job_name,
        DefinitionRevision::new(revision)?,
        &bytes,
    )?)
}

fn context(version: u32, value: u64) -> Result<ExecutionContext, Box<dyn Error>> {
    let bytes = serde_json::to_vec(&serde_json::json!({
        "format": "oxide-batch.execution-context",
        "format_version": 1,
        "schema": CONTEXT_SCHEMA,
        "schema_version": version,
        "payload": { "value": value },
    }))?;
    Ok(ExecutionContext::from_json(&bytes, StateLimits::default())?)
}

fn normal_transform(payload: &[u8]) -> Result<Vec<u8>, DefinitionStateTransformError> {
    let mut value: serde_json::Value = serde_json::from_slice(payload)
        .map_err(|_| DefinitionStateTransformError::InvalidSource)?;
    let current = value
        .get("value")
        .and_then(serde_json::Value::as_u64)
        .ok_or(DefinitionStateTransformError::InvalidSource)?;
    value["value"] = serde_json::json!(current + 1);
    serde_json::to_vec(&value).map_err(|_| DefinitionStateTransformError::TransformFailed)
}

fn worker_transform(payload: &[u8]) -> Result<Vec<u8>, DefinitionStateTransformError> {
    if std::env::var(PHASE_ENV).as_deref() == Ok(Phase::Transform.id())
        && let Ok(path) = std::env::var(HANDSHAKE_ENV)
    {
        announce(&PathBuf::from(path).join("reached"));
        park_until_killed();
    }
    normal_transform(payload)
}

#[test]
fn compatible_upgrade_kill_worker_process() -> Result<(), Box<dyn Error>> {
    let Ok(raw_phase) = std::env::var(PHASE_ENV) else {
        return Ok(());
    };
    let phase = Phase::parse(&raw_phase)?;
    let job = std::env::var(JOB_ENV)?;
    let handshake = PathBuf::from(std::env::var(HANDSHAKE_ENV)?);
    let runtime_url = runtime_url().ok_or("upgrade worker has no runtime URL")?;

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let repository =
                PostgresJobRepository::connect(config(runtime_url)?, Arc::new(FixedClock(at(554))))
                    .await?;
            let job_name = JobName::new(job)?;
            let key = JobInstanceKey::new(job_name.clone(), &JobParameters::new());
            let mut read = repository.begin().await?;
            let instance = read
                .find_job_instance(&key)
                .await?
                .ok_or("upgrade worker cannot find source instance")?;
            read.rollback().await?;

            let (_, selected) = selected_upgrade(&job_name, worker_transform)?;
            announce(&handshake.join("ready"));
            wait_for_file(&handshake.join("go"), HANDSHAKE_BOUND).await?;

            let mut unit = repository.begin().await?;
            let target_job = unit
                .create_job_execution_with_upgrade(instance.id(), &selected)
                .await?;
            unit.create_flow_step_execution(
                target_job.id(),
                &StepName::new(TARGET_STEP)?,
                &NodeId::new(TARGET_NODE)?,
                StartLimit::UNRESTRICTED,
            )
            .await?;
            unit.commit().await?;

            // Reaching here means the parent failed to pin the requested phase.
            announce(&handshake.join("unexpected-return"));
            park_until_killed();
        })
}

fn spawn_worker(phase: Phase, job: &str, handshake: &Path) -> Result<Child, Box<dyn Error>> {
    Ok(Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("compatible_upgrade_kill_worker_process")
        .arg("--nocapture")
        .env(PHASE_ENV, phase.id())
        .env(JOB_ENV, job)
        .env(HANDSHAKE_ENV, handshake)
        .spawn()?)
}

enum PhaseBlock {
    None,
    InstanceRow(PgConnection),
    CommitGate(PgConnection),
}

impl PhaseBlock {
    async fn arrange(
        phase: Phase,
        migrator_url: &str,
        instance_id: JobInstanceId,
    ) -> Result<Self, Box<dyn Error>> {
        match phase {
            Phase::PreTransform => {
                let mut connection = PgConnection::connect(migrator_url).await?;
                sqlx::query("BEGIN").execute(&mut connection).await?;
                sqlx::query("SELECT id FROM oxide_batch.ob_job_instance WHERE id = $1 FOR UPDATE")
                    .bind(i64::try_from(instance_id.get())?)
                    .fetch_one(&mut connection)
                    .await?;
                Ok(Self::InstanceRow(connection))
            }
            Phase::Transform => Ok(Self::None),
            Phase::CommitInFlight => {
                let mut connection = PgConnection::connect(migrator_url).await?;
                sqlx::query(
                    "CREATE OR REPLACE FUNCTION oxide_batch.m7_upgrade_commit_gate() \
                     RETURNS trigger LANGUAGE plpgsql AS $gate$ BEGIN \
                     PERFORM pg_advisory_xact_lock(55100151); RETURN NULL; END; $gate$",
                )
                .execute(&mut connection)
                .await?;
                sqlx::query(
                    "CREATE CONSTRAINT TRIGGER m7_upgrade_commit_gate \
                     AFTER INSERT ON oxide_batch.ob_job_execution \
                     DEFERRABLE INITIALLY DEFERRED FOR EACH ROW \
                     WHEN (NEW.attempt > 1) \
                     EXECUTE FUNCTION oxide_batch.m7_upgrade_commit_gate()",
                )
                .execute(&mut connection)
                .await?;
                sqlx::query("SELECT pg_advisory_lock($1)")
                    .bind(ADVISORY_KEY)
                    .execute(&mut connection)
                    .await?;
                Ok(Self::CommitGate(connection))
            }
        }
    }

    async fn release(&mut self) -> Result<(), Box<dyn Error>> {
        match self {
            Self::None => Ok(()),
            Self::InstanceRow(connection) => {
                sqlx::query("ROLLBACK").execute(&mut *connection).await?;
                Ok(())
            }
            Self::CommitGate(connection) => {
                sqlx::query("SELECT pg_advisory_unlock($1)")
                    .bind(ADVISORY_KEY)
                    .execute(&mut *connection)
                    .await?;
                Ok(())
            }
        }
    }
}

async fn assert_phase_state(
    runtime_url: &str,
    phase: Phase,
    instance_id: JobInstanceId,
) -> Result<(), Box<dyn Error>> {
    let repository = PostgresJobRepository::connect(
        config(runtime_url.to_owned())?,
        Arc::new(FixedClock(at(555))),
    )
    .await?;
    let mut inspect = repository.begin().await?;
    let executions = inspect.job_executions(instance_id).await?;
    let expected = if phase == Phase::CommitInFlight { 2 } else { 1 };
    assert_eq!(executions.len(), expected);

    let source = inspect
        .latest_flow_step(instance_id, &NodeId::new(SOURCE_NODE)?)
        .await?
        .ok_or("source step disappeared after upgrade kill")?;
    let source_context = source.context().ok_or("source context disappeared")?;
    assert_eq!(source_context.schema_version().get(), 1);
    assert_eq!(context_value(source_context)?, 7);

    if phase != Phase::CommitInFlight {
        assert!(
            inspect
                .latest_flow_step(instance_id, &NodeId::new(TARGET_NODE)?)
                .await?
                .is_none(),
            "pre-commit kill created a partial target step",
        );
        assert_eq!(executions[0].metadata().status(), BatchStatus::Failed);
        inspect.rollback().await?;
        repository.close().await?;
        return Ok(());
    }

    assert_eq!(executions[0].metadata().status(), BatchStatus::Failed);
    assert_eq!(executions[1].metadata().status(), BatchStatus::Starting);
    let target = inspect
        .latest_flow_step(instance_id, &NodeId::new(TARGET_NODE)?)
        .await?
        .ok_or("commit-in-flight left no target step")?;
    let target_context = target
        .context()
        .ok_or("target context was not materialized")?;
    assert_eq!(target_context.schema_version().get(), 2);
    assert_eq!(context_value(target_context)?, 8);
    let target_definition = inspect
        .scope_execution_definition(executions[1].id())
        .await?;
    assert_eq!(target_definition.revision().as_str(), "v2");
    inspect.rollback().await?;

    // Replaying after an acknowledged-by-server / unknown-to-client commit
    // must observe the exact active target instead of blindly inserting a
    // duplicate third attempt.
    let job_name = JobName::new(phase.job_name())?;
    let (_, selected) = selected_upgrade(&job_name, normal_transform)?;
    let mut replay = repository.begin().await?;
    assert!(matches!(
        replay
            .create_job_execution_with_upgrade(instance_id, &selected)
            .await,
        Err(RepositoryError::ExecutionAlreadyActive {
            execution_id,
            status: BatchStatus::Starting,
            ..
        }) if execution_id == executions[1].id()
    ));
    replay.rollback().await?;

    let mut count = repository.begin().await?;
    assert_eq!(count.job_executions(instance_id).await?.len(), 2);
    count.rollback().await?;
    repository.close().await?;
    Ok(())
}

fn context_value(context: &ExecutionContext) -> Result<u64, Box<dyn Error>> {
    let payload: serde_json::Value = serde_json::from_slice(&context.payload_json()?)?;
    payload
        .get("value")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| "context has no numeric value".into())
}

async fn wait_for_attempt_count(
    runtime_url: &str,
    job_name: &str,
    expected: i64,
) -> Result<(), Box<dyn Error>> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(runtime_url)
        .await?;
    let started = std::time::Instant::now();
    while started.elapsed() < HANDSHAKE_BOUND {
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM oxide_batch.ob_job_execution execution \
             JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
             WHERE instance.job_name = $1",
        )
        .bind(job_name)
        .fetch_one(&pool)
        .await?;
        if count == expected {
            pool.close().await;
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    pool.close().await;
    Err(format!("job {job_name} did not reach {expected} attempts").into())
}

async fn cleanup_job(url: &str, job_name: &str) -> Result<(), Box<dyn Error>> {
    let mut connection = PgConnection::connect(url).await?;
    for statement in [
        "DELETE FROM oxide_batch.ob_scope_resolution_provenance WHERE owner_job_execution_id IN (\
         SELECT execution.id FROM oxide_batch.ob_job_execution execution \
         JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = $1)",
        "DELETE FROM oxide_batch.ob_repeat_execution WHERE step_execution_id IN (\
         SELECT step.id FROM oxide_batch.ob_step_execution step \
         JOIN oxide_batch.ob_job_execution execution ON execution.id = step.job_execution_id \
         JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = $1)",
        "DELETE FROM oxide_batch.ob_component_state WHERE step_execution_id IN (\
         SELECT step.id FROM oxide_batch.ob_step_execution step \
         JOIN oxide_batch.ob_job_execution execution ON execution.id = step.job_execution_id \
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
         SELECT id FROM oxide_batch.ob_job_definition WHERE job_name = $1) \
         OR to_definition_id IN (\
         SELECT id FROM oxide_batch.ob_job_definition WHERE job_name = $1)",
        "DELETE FROM oxide_batch.ob_job_definition WHERE job_name = $1",
    ] {
        sqlx::query(statement)
            .bind(job_name)
            .execute(&mut connection)
            .await?;
    }
    connection.close().await?;
    Ok(())
}

async fn drop_commit_gate(url: &str) -> Result<(), Box<dyn Error>> {
    let mut connection = PgConnection::connect(url).await?;
    sqlx::query("DROP TRIGGER IF EXISTS m7_upgrade_commit_gate ON oxide_batch.ob_job_execution")
        .execute(&mut connection)
        .await?;
    sqlx::query("DROP FUNCTION IF EXISTS oxide_batch.m7_upgrade_commit_gate()")
        .execute(&mut connection)
        .await?;
    connection.close().await?;
    Ok(())
}

fn at(seconds: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(seconds)
}
