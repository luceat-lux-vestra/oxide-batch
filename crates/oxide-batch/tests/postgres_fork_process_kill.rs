//! M7/#552 fork process-kill evidence: lock, transformer, and commit boundaries.
//!
//! Each child is SIGKILLed at an observed phase. The parent then opens a new
//! connection and checks source preservation and the absence of partial forks.
#![cfg(all(feature = "postgres", unix))]
#![allow(clippy::expect_used, clippy::panic)]

mod crash_restore;

use std::error::Error;
use std::os::unix::process::ExitStatusExt;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use oxide_batch::{
    BatchStatus, ComponentRevision, DefinitionArtifactId, DefinitionIdentity,
    DefinitionRegistration, DefinitionRegistry, DefinitionRevision, DefinitionStateKind,
    DefinitionStateTransform, DefinitionStateTransformError, DefinitionStateTransformId,
    DefinitionStateTransformer, DefinitionUpgradeRevision, ExecutionContext, ForkContextTransform,
    ForkRequest, JobExecution, JobFork, JobInstanceId, JobInstanceKey, JobName, JobParameters,
    JobRepository, LifecycleTransition, NodeId, OperationId, PostgresJobRepository,
    PostgresMigrator, StateLimits, StateSchemaId, StateSchemaVersion, StepName,
};
use sqlx::{Connection as _, PgConnection};

use crash_restore::{
    FixedClock, announce, config, handshake_directory, migrator_url, park_until_killed,
    runtime_url, wait_for_blocked_statement, wait_for_file,
};

const PHASE_ENV: &str = "OXIDEBATCH_M7_FORK_KILL_PHASE";
const JOB_ENV: &str = "OXIDEBATCH_M7_FORK_KILL_JOB";
const HANDSHAKE_ENV: &str = "OXIDEBATCH_M7_FORK_KILL_HANDSHAKE";
const ADVISORY_KEY: i64 = 55_200_151;
const SIGKILL: i32 = 9;
const BOUND: Duration = Duration::from_secs(35);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    SourceLock,
    Transform,
    Commit,
}

impl Phase {
    const ALL: [Self; 3] = [Self::SourceLock, Self::Transform, Self::Commit];

    const fn id(self) -> &'static str {
        match self {
            Self::SourceLock => "source-lock",
            Self::Transform => "transform",
            Self::Commit => "commit-in-flight",
        }
    }

    fn parse(value: &str) -> Result<Self, Box<dyn Error>> {
        Self::ALL
            .into_iter()
            .find(|phase| phase.id() == value)
            .ok_or_else(|| format!("unrecognized fork kill phase {value}").into())
    }
}

struct SourceFixture {
    job: String,
    source: JobExecution,
}

fn registration(name: &str) -> Result<DefinitionRegistration, Box<dyn Error>> {
    let job = JobName::new(name)?;
    Ok(DefinitionRegistration::new(
        DefinitionIdentity::tasklet(
            &job,
            &StepName::new("only")?,
            DefinitionRevision::new("v1")?,
            &ComponentRevision::new("component-v1")?,
        )?,
        DefinitionArtifactId::new("fork-kill-artifact-v1")?,
    )?)
}

fn transform(payload: &[u8]) -> Result<Vec<u8>, DefinitionStateTransformError> {
    if std::env::var(PHASE_ENV).as_deref() == Ok(Phase::Transform.id())
        && let Ok(handshake) = std::env::var(HANDSHAKE_ENV)
    {
        announce(&PathBuf::from(handshake).join("reached"));
        park_until_killed();
    }
    let mut value: serde_json::Value = serde_json::from_slice(payload)
        .map_err(|_| DefinitionStateTransformError::InvalidSource)?;
    let before = value
        .get("value")
        .and_then(serde_json::Value::as_u64)
        .ok_or(DefinitionStateTransformError::InvalidSource)?;
    value["value"] = serde_json::json!(before + 1);
    serde_json::to_vec(&value).map_err(|_| DefinitionStateTransformError::TransformFailed)
}

async fn prepare_source(url: &str, job: &str) -> Result<SourceFixture, Box<dyn Error>> {
    let repository = PostgresJobRepository::connect(
        config(url.to_owned())?,
        Arc::new(FixedClock(UNIX_EPOCH + Duration::from_secs(90))),
    )
    .await?;
    let source_name = format!("{job}_src");
    let target_name = format!("{job}_dst");
    let source_registration = registration(&source_name)?;
    let target_registration = registration(&target_name)?;
    let registry = DefinitionRegistry::new(repository.clone());
    registry.register(&source_registration).await?;
    registry.register(&target_registration).await?;

    let params = JobParameters::new();
    let mut unit = repository.begin().await?;
    let instance = unit
        .select_or_create_job_instance(&JobInstanceKey::new(
            source_registration.job_name().clone(),
            &params,
        ))
        .await?
        .instance()
        .clone();
    let job_starting = unit
        .create_job_execution_with_definition_and_parameters(
            instance.id(),
            source_registration.definition(),
            &params,
        )
        .await?;
    let job_started = unit
        .transition_job_execution(
            job_starting.id(),
            job_starting.version(),
            LifecycleTransition::new(BatchStatus::Started, UNIX_EPOCH + Duration::from_secs(100)),
        )
        .await?;
    let step = unit
        .create_step_execution(job_started.id(), &StepName::new("only")?)
        .await?;
    let step_started = unit
        .transition_step_execution(
            step.id(),
            step.version(),
            LifecycleTransition::new(BatchStatus::Started, UNIX_EPOCH + Duration::from_secs(101)),
        )
        .await?;
    let context = ExecutionContext::from_transformed_payload(
        StateSchemaId::new("fork-kill-v1")?,
        StateSchemaVersion::new(1)?,
        br#"{"value":7}"#,
        StateLimits::default(),
    )?;
    let step_committed = unit
        .commit_step_execution_context(step_started.id(), step_started.version(), &context)
        .await?;
    unit.transition_step_execution(
        step_committed.id(),
        step_committed.version(),
        LifecycleTransition::new(
            BatchStatus::Completed,
            UNIX_EPOCH + Duration::from_secs(102),
        ),
    )
    .await?;
    let terminal = unit
        .transition_job_execution(
            job_started.id(),
            job_started.version(),
            LifecycleTransition::new(BatchStatus::Stopped, UNIX_EPOCH + Duration::from_secs(103)),
        )
        .await?;
    unit.commit().await?;
    repository.close().await?;
    Ok(SourceFixture {
        job: job.to_owned(),
        source: terminal,
    })
}

async fn request_for(
    repository: &PostgresJobRepository,
    job: &str,
) -> Result<ForkRequest, Box<dyn Error>> {
    let mut read = repository.begin().await?;
    let source_name = format!("{job}_src");
    let target_name = format!("{job}_dst");
    let source = read
        .find_job_instance(&JobInstanceKey::new(
            JobName::new(&source_name)?,
            &JobParameters::new(),
        ))
        .await?
        .ok_or("missing prepared source")?;
    let source_execution = read
        .job_executions(source.id())
        .await?
        .pop()
        .ok_or("missing prepared source execution")?;
    let step = read
        .step_executions(source_execution.id())
        .await?
        .pop()
        .ok_or("missing prepared source step")?;
    read.rollback().await?;

    let descriptor = DefinitionStateTransform::new(
        NodeId::new("only")?,
        NodeId::new("only")?,
        DefinitionStateKind::ExecutionContext,
        DefinitionStateTransformId::new("fork-kill-transform")?,
        StateSchemaId::new("fork-kill-v1")?,
        StateSchemaVersion::new(1)?,
        StateSchemaId::new("fork-kill-v2")?,
        StateSchemaVersion::new(2)?,
        DefinitionUpgradeRevision::new("fork-kill-transform-v1")?,
    );
    Ok(ForkRequest::new(
        OperationId::new(format!("{job}-op"))?,
        source_execution.id(),
        source_execution.version(),
        registration(&target_name)?,
        JobParameters::new(),
    )
    .with_context_transform(ForkContextTransform::new(
        step.id(),
        step.version(),
        descriptor,
    )))
}

#[test]
fn fork_process_kill_preserves_transactional_source_and_target() -> Result<(), Box<dyn Error>> {
    let Some(runtime) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL not configured");
        return Ok(());
    };
    let Some(admin) = migrator_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL not configured");
        return Ok(());
    };
    let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            PostgresMigrator::migrate(&config(admin.clone())?).await?;
            for phase in Phase::ALL {
                let job = format!("fork_kill_{}_{}", phase.id().replace('-', "_"), stamp);
                run_phase(phase, &job, &runtime, &admin).await?;
            }
            Ok::<(), Box<dyn Error>>(())
        })
}

async fn run_phase(
    phase: Phase,
    job: &str,
    runtime: &str,
    admin: &str,
) -> Result<(), Box<dyn Error>> {
    drop_commit_gate(admin).await?;
    let fixture = prepare_source(runtime, job).await?;
    let mut blocker = Blocker::arrange(phase, admin, fixture.source.job_instance_id()).await?;
    let handshake = handshake_directory(&format!("fork-{}-{job}", phase.id()))?;
    let mut child = Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("fork_kill_worker")
        .arg("--nocapture")
        .env(PHASE_ENV, phase.id())
        .env(JOB_ENV, &fixture.job)
        .env(HANDSHAKE_ENV, &handshake)
        .spawn()?;
    let observed = async {
        wait_for_file(&handshake.join("ready"), BOUND).await?;
        announce(&handshake.join("go"));
        match phase {
            Phase::SourceLock => {
                wait_for_blocked_statement(
                    admin,
                    "SELECT id FROM oxide_batch.ob_job_instance WHERE id = $1 FOR UPDATE",
                    BOUND,
                )
                .await?;
            }
            Phase::Transform => {
                wait_for_file(&handshake.join("reached"), BOUND).await?;
            }
            Phase::Commit => {
                wait_for_blocked_statement(admin, "COMMIT", BOUND).await?;
            }
        }
        child.kill()?;
        let status = child.wait()?;
        assert_eq!(status.signal(), Some(SIGKILL));
        assert_eq!(status.code(), None);
        blocker.release().await?;
        verify_no_partial_fork(runtime, &fixture).await
    }
    .await;
    if observed.is_err() {
        let _ = child.kill();
        let _ = child.wait();
    }
    let _ = blocker.release().await;
    let _ = drop_commit_gate(admin).await;
    observed
}

async fn verify_no_partial_fork(
    runtime: &str,
    fixture: &SourceFixture,
) -> Result<(), Box<dyn Error>> {
    let repository = PostgresJobRepository::connect(
        config(runtime.to_owned())?,
        Arc::new(FixedClock(UNIX_EPOCH + Duration::from_secs(90))),
    )
    .await?;
    let request = request_for(&repository, &fixture.job).await?;
    let service = JobFork::new(repository.clone());
    assert!(service.lineage(request.operation_id()).await?.is_none());
    let mut read = repository.begin().await?;
    assert!(
        read.find_job_instance(&request.target_key())
            .await?
            .is_none()
    );
    assert_eq!(
        read.get_job_execution(fixture.source.id()).await?,
        Some(fixture.source.clone())
    );
    read.rollback().await?;
    repository.close().await?;
    Ok(())
}

#[test]
fn fork_kill_worker() -> Result<(), Box<dyn Error>> {
    let Ok(raw_phase) = std::env::var(PHASE_ENV) else {
        return Ok(());
    };
    Phase::parse(&raw_phase)?;
    let job = std::env::var(JOB_ENV)?;
    let handshake = PathBuf::from(std::env::var(HANDSHAKE_ENV)?);
    let url = runtime_url().ok_or("fork worker missing runtime URL")?;
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let repository = PostgresJobRepository::connect(
                config(url)?,
                Arc::new(FixedClock(UNIX_EPOCH + Duration::from_secs(90))),
            )
            .await?;
            let request = request_for(&repository, &job).await?;
            let transformer = DefinitionStateTransformer::new(
                DefinitionUpgradeRevision::new("fork-kill-transform-v1")?,
                transform,
            );
            announce(&handshake.join("ready"));
            wait_for_file(&handshake.join("go"), BOUND).await?;
            JobFork::new(repository.clone())
                .fork_transformed(&request, &transformer)
                .await?;
            announce(&handshake.join("unexpected-return"));
            park_until_killed();
        })
}

enum Blocker {
    None,
    Instance(PgConnection),
    CommitGate(PgConnection),
}

impl Blocker {
    async fn arrange(
        phase: Phase,
        admin: &str,
        source_instance: JobInstanceId,
    ) -> Result<Self, Box<dyn Error>> {
        match phase {
            Phase::SourceLock => {
                let mut connection = PgConnection::connect(admin).await?;
                sqlx::query("BEGIN").execute(&mut connection).await?;
                sqlx::query("SELECT id FROM oxide_batch.ob_job_instance WHERE id = $1 FOR UPDATE")
                    .bind(i64::try_from(source_instance.get())?)
                    .fetch_one(&mut connection)
                    .await?;
                Ok(Self::Instance(connection))
            }
            Phase::Transform => Ok(Self::None),
            Phase::Commit => {
                let mut connection = PgConnection::connect(admin).await?;
                sqlx::query(
                    "CREATE OR REPLACE FUNCTION oxide_batch.m7_fork_commit_gate() \
                     RETURNS trigger LANGUAGE plpgsql AS $gate$ BEGIN \
                     PERFORM pg_advisory_xact_lock(55200151); RETURN NULL; END; $gate$",
                )
                .execute(&mut connection)
                .await?;
                sqlx::query(
                    "CREATE CONSTRAINT TRIGGER m7_fork_commit_gate \
                     AFTER INSERT ON oxide_batch.ob_job_fork_lineage \
                     DEFERRABLE INITIALLY DEFERRED FOR EACH ROW \
                     EXECUTE FUNCTION oxide_batch.m7_fork_commit_gate()",
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
            Self::Instance(connection) => {
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

async fn drop_commit_gate(admin: &str) -> Result<(), Box<dyn Error>> {
    let mut connection = PgConnection::connect(admin).await?;
    sqlx::query("DROP TRIGGER IF EXISTS m7_fork_commit_gate ON oxide_batch.ob_job_fork_lineage")
        .execute(&mut connection)
        .await?;
    sqlx::query("DROP FUNCTION IF EXISTS oxide_batch.m7_fork_commit_gate()")
        .execute(&mut connection)
        .await?;
    Ok(())
}
