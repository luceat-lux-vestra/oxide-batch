//! Runnable-core repository certification source for the first-party `MongoDB` adapter.

#![cfg(feature = "mongodb")]

#[allow(dead_code, unused_imports)]
#[path = "contract/mod.rs"]
mod contract;

use std::error::Error;
use std::process::Command;
use std::sync::Arc;

use contract::{ServiceBackend, run_explorer_contract, run_runnable_core_repository_contract};
use mongodb::Client;
use mongodb::bson::{Document, doc};
use oxide_batch::{
    JobInstanceKey, JobName, JobParameters, JobRepository, MongoConfig, MongoExplorer,
    MongoJobRepository, MongoMigrator, RepositoryError, SystemClock,
};

#[derive(Clone)]
struct TestSettings {
    uri: String,
    database: String,
}

impl TestSettings {
    fn from_environment() -> Option<Self> {
        Some(Self {
            uri: std::env::var("OXIDEBATCH_MONGODB_TEST_URI").ok()?,
            database: std::env::var("OXIDEBATCH_MONGODB_TEST_DATABASE").ok()?,
        })
    }

    fn config(&self) -> Result<MongoConfig, Box<dyn Error>> {
        Ok(MongoConfig::new(self.uri.clone(), self.database.clone())?)
    }
}

async fn clear_runnable_core_fixture(settings: &TestSettings) -> Result<(), Box<dyn Error>> {
    let client = Client::with_uri_str(&settings.uri).await?;
    let database = client.database(&settings.database);
    for collection in [
        "ob_flow_decision",
        "ob_step_execution",
        "ob_job_execution",
        "ob_job_instance",
        "ob_core_job_definition",
    ] {
        database
            .collection::<mongodb::bson::Document>(collection)
            .delete_many(doc! {})
            .await?;
    }
    drop(database);
    client.shutdown().await;
    Ok(())
}

const CONCURRENT_LAUNCH_JOB: &str = "repository_concurrent_launch";
const TRANSIENT_MUTATION_JOB: &str = "repository_transient_mutation_fault";
const TRANSIENT_COMMIT_JOB: &str = "repository_transient_commit_fault";
const UNKNOWN_COMMIT_JOB: &str = "repository_unknown_commit_fault";
const FAULT_PROBE_CHILD_ENV: &str = "OXIDEBATCH_MONGODB_FAULT_PROBE_CHILD";
const TRANSIENT_MUTATION_TEST: &str = "mongodb_transient_mutation_error_is_known_not_committed";
const TRANSIENT_COMMIT_TEST: &str = "mongodb_transient_commit_error_is_known_not_committed";
const UNKNOWN_COMMIT_TEST: &str = "mongodb_unknown_commit_result_is_never_guessed_rolled_back";

fn mongodb_failpoints_enabled() -> bool {
    std::env::var("OXIDEBATCH_MONGODB_FAILPOINTS").as_deref() == Ok("1")
}

fn mongodb_fault_probe_child() -> bool {
    std::env::var(FAULT_PROBE_CHILD_ENV).as_deref() == Ok("1")
}

fn fault_stage<T, E>(stage: &str, result: Result<T, E>) -> Result<T, Box<dyn Error>>
where
    E: std::fmt::Display,
{
    result.map_err(|error| {
        std::io::Error::other(format!("MongoDB fault stage {stage} failed: {error}")).into()
    })
}

async fn configure_fail_command(
    settings: &TestSettings,
    times: i32,
    data: Document,
) -> Result<(), Box<dyn Error>> {
    let client = Client::with_uri_str(&settings.uri).await?;
    let admin = client.database("admin");
    admin
        .run_command(doc! {
            "configureFailPoint": "failCommand",
            "mode": { "times": times },
            "data": data,
        })
        .await?;
    drop(admin);
    client.shutdown().await;
    Ok(())
}

async fn disable_fail_command(settings: &TestSettings) -> Result<(), Box<dyn Error>> {
    let client = Client::with_uri_str(&settings.uri).await?;
    let admin = client.database("admin");
    admin
        .run_command(doc! {
            "configureFailPoint": "failCommand",
            "mode": "off",
        })
        .await?;
    drop(admin);
    client.shutdown().await;
    Ok(())
}

async fn terminate_fault_sessions(settings: &TestSettings) -> Result<(), Box<dyn Error>> {
    let client = Client::with_uri_str(&settings.uri).await?;
    let admin = client.database("admin");
    admin.run_command(doc! { "killAllSessions": [] }).await?;
    drop(admin);
    client.shutdown().await;
    Ok(())
}

async fn clear_fault_fixture(
    settings: &TestSettings,
    job_name: &str,
) -> Result<(), Box<dyn Error>> {
    let client = Client::with_uri_str(&settings.uri).await?;
    let database = client.database(&settings.database);
    let instances = database.collection::<Document>("ob_job_instance");
    if let Some(instance) = instances.find_one(doc! { "job_name": job_name }).await? {
        let instance_id = instance.get_i64("id")?;
        database
            .collection::<Document>("ob_job_execution")
            .delete_many(doc! { "job_instance_id": instance_id })
            .await?;
    }
    instances.delete_many(doc! { "job_name": job_name }).await?;
    drop(instances);
    drop(database);
    client.shutdown().await;
    Ok(())
}

async fn reset_fault_boundary(
    settings: &TestSettings,
    job_name: &str,
) -> Result<(), Box<dyn Error>> {
    let mut errors = Vec::new();

    if let Err(error) = disable_fail_command(settings).await {
        errors.push(format!("disable failpoint: {error}"));
    }
    if let Err(error) = terminate_fault_sessions(settings).await {
        errors.push(format!("kill server sessions: {error}"));
    }
    if let Err(error) = clear_fault_fixture(settings, job_name).await {
        errors.push(format!("clear fixture: {error}"));
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(std::io::Error::other(errors.join("; ")).into())
    }
}

async fn fault_fixture_counts(
    settings: &TestSettings,
    job_name: &str,
) -> Result<(u64, u64), Box<dyn Error>> {
    let client = Client::with_uri_str(&settings.uri).await?;
    let database = client.database(&settings.database);
    let instances = database.collection::<Document>("ob_job_instance");
    let instance_count = instances
        .count_documents(doc! { "job_name": job_name })
        .await?;
    let execution_count =
        if let Some(instance) = instances.find_one(doc! { "job_name": job_name }).await? {
            database
                .collection::<Document>("ob_job_execution")
                .count_documents(doc! { "job_instance_id": instance.get_i64("id")? })
                .await?
        } else {
            0
        };
    drop(instances);
    drop(database);
    client.shutdown().await;
    Ok((instance_count, execution_count))
}

fn run_isolated_fault_probe(
    settings: &TestSettings,
    test_name: &str,
    job_name: &str,
) -> Result<(), Box<dyn Error>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    fault_stage(
        "isolated-fault/pre-boundary",
        runtime.block_on(reset_fault_boundary(settings, job_name)),
    )?;

    let output = Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg(test_name)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(FAULT_PROBE_CHILD_ENV, "1")
        .output()?;

    let cleanup_error = runtime
        .block_on(reset_fault_boundary(settings, job_name))
        .err()
        .map(|error| error.to_string());

    if output.status.success() && cleanup_error.is_none() {
        return Ok(());
    }

    let status = output
        .status
        .code()
        .map_or_else(|| "signal".to_owned(), |code| code.to_string());
    Err(std::io::Error::other(format!(
        "MongoDB isolated fault probe {test_name} failed: status={status}; cleanup={}; \
         stdout={}; stderr={}",
        cleanup_error.as_deref().unwrap_or("ok"),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    ))
    .into())
}

async fn clear_concurrent_launch_fixture(settings: &TestSettings) -> Result<(), Box<dyn Error>> {
    let client = Client::with_uri_str(&settings.uri).await?;
    let database = client.database(&settings.database);
    let instances = database.collection::<Document>("ob_job_instance");

    if let Some(instance) = instances
        .find_one(doc! { "job_name": CONCURRENT_LAUNCH_JOB })
        .await?
    {
        let instance_id = instance.get_i64("id")?;
        database
            .collection::<Document>("ob_job_execution")
            .delete_many(doc! { "job_instance_id": instance_id })
            .await?;
    }
    instances
        .delete_many(doc! { "job_name": CONCURRENT_LAUNCH_JOB })
        .await?;
    drop(instances);
    drop(database);
    client.shutdown().await;
    Ok(())
}

const LIFECYCLE_PROBE_TEST: &str = "mongodb_client_lifecycle_probe";
const LIFECYCLE_PROBE_CYCLES: usize = 16;

type RepositoryRegistry = Arc<tokio::sync::Mutex<Vec<MongoJobRepository>>>;

async fn close_repository_registry(registry: &RepositoryRegistry) {
    let repositories = {
        let mut guard = registry.lock().await;
        std::mem::take(&mut *guard)
    };
    for repository in repositories {
        repository.close().await;
    }
}

#[test]
fn migration_is_idempotent_for_mongodb() -> Result<(), Box<dyn Error>> {
    let Some(settings) = TestSettings::from_environment() else {
        eprintln!("skipped: MongoDB test environment is not configured");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let config = settings.config()?;
        MongoMigrator::migrate(&config).await?;
        MongoMigrator::migrate(&config).await?;
        assert_eq!(
            MongoMigrator::installed_schema_version(&config).await?,
            Some(MongoMigrator::supported_schema_version())
        );
        Ok::<(), Box<dyn Error>>(())
    })?;
    Ok(())
}

#[test]
fn undefined_older_mongodb_schema_is_rejected_without_mutation() -> Result<(), Box<dyn Error>> {
    let Some(settings) = TestSettings::from_environment() else {
        eprintln!("skipped: MongoDB test environment is not configured");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let config = settings.config()?;
        MongoMigrator::migrate(&config).await?;

        let client = Client::with_uri_str(&settings.uri).await?;
        let database = client.database(&settings.database);
        let schema = database.collection::<Document>("ob_schema");
        schema
            .update_one(
                doc! { "_id": "schema" },
                doc! { "$set": { "version": 1_i64 } },
            )
            .await?;
        database
            .collection::<Document>("ob_flow_decision")
            .drop()
            .await?;

        let connect_error =
            match MongoJobRepository::connect(config.clone(), Arc::new(SystemClock)).await {
                Ok(repository) => {
                    repository.close().await;
                    None
                }
                Err(error) => Some(error),
            };
        let migration_result = MongoMigrator::migrate(&config).await;
        let version_after = schema
            .find_one(doc! { "_id": "schema" })
            .await?
            .ok_or_else(|| std::io::Error::other("MongoDB schema marker disappeared"))?
            .get_i64("version")?;
        let collections_after = database.list_collection_names().await?;
        let decisions_recreated = collections_after
            .iter()
            .any(|name| name == "ob_flow_decision");

        schema
            .update_one(
                doc! { "_id": "schema" },
                doc! { "$set": { "version": 2_i64 } },
            )
            .await?;
        drop(schema);
        drop(database);
        client.shutdown().await;
        MongoMigrator::migrate(&config).await?;

        assert_eq!(
            connect_error,
            Some(RepositoryError::MigrationRequired {
                current: 1,
                supported: MongoMigrator::supported_schema_version(),
            })
        );
        assert_eq!(
            migration_result,
            Err(RepositoryError::MigrationRequired {
                current: 1,
                supported: MongoMigrator::supported_schema_version(),
            })
        );
        assert_eq!(version_after, 1);
        assert!(
            !decisions_recreated,
            "lower unsupported schema was mutated before rejection"
        );
        Ok::<(), Box<dyn Error>>(())
    })?;
    Ok(())
}

#[test]
fn named_mongodb_lane_reports_expected_version() -> Result<(), Box<dyn Error>> {
    let Some(settings) = TestSettings::from_environment() else {
        eprintln!("skipped: MongoDB test environment is not configured");
        return Ok(());
    };
    let Ok(expected) = std::env::var("OXIDEBATCH_MONGODB_EXPECTED_VERSION") else {
        eprintln!("skipped: OXIDEBATCH_MONGODB_EXPECTED_VERSION is not configured");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let client = Client::with_uri_str(&settings.uri).await?;
        let database = client.database("admin");
        let row = database.run_command(doc! { "buildInfo": 1_i32 }).await?;
        let actual = row.get_str("version")?.to_owned();
        drop(database);
        client.shutdown().await;
        assert_eq!(actual, expected);
        Ok::<(), Box<dyn Error>>(())
    })?;
    Ok(())
}

#[test]
fn runnable_core_repository_contract_passes_on_mongodb() -> Result<(), Box<dyn Error>> {
    let Some(settings) = TestSettings::from_environment() else {
        eprintln!("skipped: MongoDB test environment is not configured");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        MongoMigrator::migrate(&settings.config()?).await?;
        let repositories = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let contract_settings = settings.clone();
        let contract_repositories = Arc::clone(&repositories);
        let contract_result = run_runnable_core_repository_contract("mongodb", move |clock| {
            let settings = contract_settings.clone();
            let repositories = Arc::clone(&contract_repositories);
            async move {
                clear_runnable_core_fixture(&settings)
                    .await
                    .map_err(|_| RepositoryError::Unavailable)?;
                let repository = MongoJobRepository::connect(
                    settings
                        .config()
                        .map_err(|_| RepositoryError::Unavailable)?,
                    clock,
                )
                .await?;
                repositories.lock().await.push(repository.clone());
                Ok(repository)
            }
        })
        .await;
        close_repository_registry(&repositories).await;
        contract_result?;
        clear_runnable_core_fixture(&settings).await?;
        Ok::<(), Box<dyn Error>>(())
    })?;
    Ok(())
}

#[test]
fn explorer_contract_passes_on_mongodb() -> Result<(), Box<dyn Error>> {
    let Some(settings) = TestSettings::from_environment() else {
        eprintln!("skipped: MongoDB test environment is not configured");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        MongoMigrator::migrate(&settings.config()?).await?;
        let repositories = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let contract_settings = settings.clone();
        let contract_repositories = Arc::clone(&repositories);
        let contract_result = run_explorer_contract("mongodb", move |clock| {
            let settings = contract_settings.clone();
            let repositories = Arc::clone(&contract_repositories);
            async move {
                clear_runnable_core_fixture(&settings)
                    .await
                    .map_err(|_| RepositoryError::Unavailable)?;
                let repository = MongoJobRepository::connect(
                    settings
                        .config()
                        .map_err(|_| RepositoryError::Unavailable)?,
                    clock.clone(),
                )
                .await?;
                repositories.lock().await.push(repository.clone());
                Ok(ServiceBackend {
                    explorer: MongoExplorer::new(repository.clone()),
                    repository,
                    clock,
                })
            }
        })
        .await;
        close_repository_registry(&repositories).await;
        contract_result?;
        clear_runnable_core_fixture(&settings).await?;
        Ok::<(), Box<dyn Error>>(())
    })?;
    Ok(())
}

#[test]
fn concurrent_launch_creates_one_mongodb_instance_and_attempt() -> Result<(), Box<dyn Error>> {
    const CONTENDERS: usize = 8;

    let Some(settings) = TestSettings::from_environment() else {
        eprintln!("skipped: MongoDB test environment is not configured");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let config = settings.config()?;
        MongoMigrator::migrate(&config).await?;
        clear_concurrent_launch_fixture(&settings).await?;
        let repository = MongoJobRepository::connect(config, Arc::new(SystemClock)).await?;
        let barrier = Arc::new(tokio::sync::Barrier::new(CONTENDERS));
        let key = JobInstanceKey::new(JobName::new(CONCURRENT_LAUNCH_JOB)?, &JobParameters::new());

        let mut handles = Vec::with_capacity(CONTENDERS);
        for _ in 0..CONTENDERS {
            let repository = repository.clone();
            let barrier = Arc::clone(&barrier);
            let key = key.clone();
            handles.push(tokio::spawn(async move {
                let mut unit = repository.begin().await?;
                barrier.wait().await;
                let instance = unit
                    .select_or_create_job_instance(&key)
                    .await?
                    .instance()
                    .clone();
                unit.create_job_execution(instance.id()).await?;
                unit.commit().await
            }));
        }

        let mut committed = 0;
        let mut rejected = 0;
        for handle in handles {
            match handle.await? {
                Ok(()) => committed += 1,
                Err(
                    RepositoryError::ExecutionAlreadyActive { .. }
                    | RepositoryError::ConcurrentModification
                    | RepositoryError::TransactionAborted,
                ) => rejected += 1,
                Err(error) => return Err(error.into()),
            }
        }
        assert_eq!(committed, 1);
        assert_eq!(rejected, CONTENDERS - 1);

        let mut inspection = repository.begin().await?;
        let instance = inspection
            .find_job_instance(&key)
            .await?
            .ok_or_else(|| std::io::Error::other("canonical MongoDB instance was not committed"))?;
        let latest = inspection
            .latest_job_execution(instance.id())
            .await?
            .ok_or_else(|| {
                std::io::Error::other("canonical MongoDB execution was not committed")
            })?;
        assert_eq!(
            inspection.job_execution_attempt(latest.id()).await?.get(),
            1
        );
        inspection.rollback().await?;

        let client = Client::with_uri_str(&settings.uri).await?;
        let database = client.database(&settings.database);
        let instance_count = database
            .collection::<Document>("ob_job_instance")
            .count_documents(doc! { "job_name": CONCURRENT_LAUNCH_JOB })
            .await?;
        let execution_count = database
            .collection::<Document>("ob_job_execution")
            .count_documents(doc! {
                "job_instance_id": i64::try_from(instance.id().get())?,
            })
            .await?;
        drop(database);
        client.shutdown().await;

        assert_eq!(instance_count, 1);
        assert_eq!(execution_count, 1);

        repository.close().await;
        clear_concurrent_launch_fixture(&settings).await?;
        Ok::<(), Box<dyn Error>>(())
    })?;
    Ok(())
}

#[test]
fn mongodb_transient_mutation_error_is_known_not_committed() -> Result<(), Box<dyn Error>> {
    let Some(settings) = TestSettings::from_environment() else {
        eprintln!("skipped: MongoDB test environment is not configured");
        return Ok(());
    };
    if !mongodb_failpoints_enabled() {
        eprintln!("skipped: MongoDB failpoint evidence is not enabled");
        return Ok(());
    }
    if !mongodb_fault_probe_child() {
        return run_isolated_fault_probe(
            &settings,
            TRANSIENT_MUTATION_TEST,
            TRANSIENT_MUTATION_JOB,
        );
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let config = fault_stage("transient-mutation/config", settings.config())?;
        fault_stage(
            "transient-mutation/migrate",
            MongoMigrator::migrate(&config).await,
        )?;
        fault_stage(
            "transient-mutation/pre-cleanup",
            clear_fault_fixture(&settings, TRANSIENT_MUTATION_JOB).await,
        )?;
        let repository = fault_stage(
            "transient-mutation/connect",
            MongoJobRepository::connect(config, Arc::new(SystemClock)).await,
        )?;
        let job_name = fault_stage(
            "transient-mutation/job-name",
            JobName::new(TRANSIENT_MUTATION_JOB),
        )?;
        let key = JobInstanceKey::new(job_name, &JobParameters::new());

        let mut unit = fault_stage("transient-mutation/begin", repository.begin().await)?;
        fault_stage(
            "transient-mutation/configure-failpoint",
            configure_fail_command(
                &settings,
                1,
                doc! {
                    "failCommands": ["findAndModify"],
                    "errorCode": 112_i32,
                },
            )
            .await,
        )?;
        let result = unit.select_or_create_job_instance(&key).await;
        fault_stage(
            "transient-mutation/disable-failpoint",
            disable_fail_command(&settings).await,
        )?;
        fault_stage("transient-mutation/rollback", unit.rollback().await)?;

        match result {
            Err(RepositoryError::TransactionAborted) => {}
            other => {
                return Err(std::io::Error::other(format!(
                    "MongoDB fault stage transient-mutation/classification failed: \
                     expected TransactionAborted, observed {other:?}"
                ))
                .into());
            }
        }

        let counts = fault_stage(
            "transient-mutation/readback",
            fault_fixture_counts(&settings, TRANSIENT_MUTATION_JOB).await,
        )?;
        if counts != (0, 0) {
            return Err(std::io::Error::other(format!(
                "MongoDB fault stage transient-mutation/readback failed: \
                 expected (0, 0), observed {counts:?}"
            ))
            .into());
        }

        repository.close().await;
        fault_stage(
            "transient-mutation/post-cleanup",
            clear_fault_fixture(&settings, TRANSIENT_MUTATION_JOB).await,
        )?;
        Ok::<(), Box<dyn Error>>(())
    })?;
    Ok(())
}

#[test]
fn mongodb_transient_commit_error_is_known_not_committed() -> Result<(), Box<dyn Error>> {
    let Some(settings) = TestSettings::from_environment() else {
        eprintln!("skipped: MongoDB test environment is not configured");
        return Ok(());
    };
    if !mongodb_failpoints_enabled() {
        eprintln!("skipped: MongoDB failpoint evidence is not enabled");
        return Ok(());
    }
    if !mongodb_fault_probe_child() {
        return run_isolated_fault_probe(&settings, TRANSIENT_COMMIT_TEST, TRANSIENT_COMMIT_JOB);
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let config = fault_stage("transient-commit/config", settings.config())?;
        fault_stage(
            "transient-commit/migrate",
            MongoMigrator::migrate(&config).await,
        )?;
        fault_stage(
            "transient-commit/pre-cleanup",
            clear_fault_fixture(&settings, TRANSIENT_COMMIT_JOB).await,
        )?;
        let repository = fault_stage(
            "transient-commit/connect",
            MongoJobRepository::connect(config, Arc::new(SystemClock)).await,
        )?;
        let job_name = fault_stage(
            "transient-commit/job-name",
            JobName::new(TRANSIENT_COMMIT_JOB),
        )?;
        let key = JobInstanceKey::new(job_name, &JobParameters::new());

        let mut unit = fault_stage("transient-commit/begin", repository.begin().await)?;
        let instance = fault_stage(
            "transient-commit/select-or-create",
            unit.select_or_create_job_instance(&key).await,
        )?
        .instance()
        .clone();
        fault_stage(
            "transient-commit/create-execution",
            unit.create_job_execution(instance.id()).await,
        )?;
        fault_stage(
            "transient-commit/configure-failpoint",
            configure_fail_command(
                &settings,
                1,
                doc! {
                    "failCommands": ["commitTransaction"],
                    "errorCode": 251_i32,
                },
            )
            .await,
        )?;
        let result = unit.commit().await;
        fault_stage(
            "transient-commit/disable-failpoint",
            disable_fail_command(&settings).await,
        )?;

        match result {
            Err(RepositoryError::TransactionAborted) => {}
            other => {
                return Err(std::io::Error::other(format!(
                    "MongoDB fault stage transient-commit/classification failed: \
                     expected TransactionAborted, observed {other:?}"
                ))
                .into());
            }
        }

        let counts = fault_stage(
            "transient-commit/readback",
            fault_fixture_counts(&settings, TRANSIENT_COMMIT_JOB).await,
        )?;
        if counts != (0, 0) {
            return Err(std::io::Error::other(format!(
                "MongoDB fault stage transient-commit/readback failed: \
                 expected (0, 0), observed {counts:?}"
            ))
            .into());
        }

        repository.close().await;
        fault_stage(
            "transient-commit/post-cleanup",
            clear_fault_fixture(&settings, TRANSIENT_COMMIT_JOB).await,
        )?;
        Ok::<(), Box<dyn Error>>(())
    })?;
    Ok(())
}

#[test]
fn mongodb_unknown_commit_result_is_never_guessed_rolled_back() -> Result<(), Box<dyn Error>> {
    let Some(settings) = TestSettings::from_environment() else {
        eprintln!("skipped: MongoDB test environment is not configured");
        return Ok(());
    };
    if !mongodb_failpoints_enabled() {
        eprintln!("skipped: MongoDB failpoint evidence is not enabled");
        return Ok(());
    }
    if !mongodb_fault_probe_child() {
        return run_isolated_fault_probe(&settings, UNKNOWN_COMMIT_TEST, UNKNOWN_COMMIT_JOB);
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let config = fault_stage("unknown-commit/config", settings.config())?;
        fault_stage(
            "unknown-commit/migrate",
            MongoMigrator::migrate(&config).await,
        )?;
        fault_stage(
            "unknown-commit/pre-cleanup",
            clear_fault_fixture(&settings, UNKNOWN_COMMIT_JOB).await,
        )?;
        let repository = fault_stage(
            "unknown-commit/connect",
            MongoJobRepository::connect(config, Arc::new(SystemClock)).await,
        )?;
        let job_name = fault_stage("unknown-commit/job-name", JobName::new(UNKNOWN_COMMIT_JOB))?;
        let key = JobInstanceKey::new(job_name, &JobParameters::new());

        let mut unit = fault_stage("unknown-commit/begin", repository.begin().await)?;
        let instance = fault_stage(
            "unknown-commit/select-or-create",
            unit.select_or_create_job_instance(&key).await,
        )?
        .instance()
        .clone();
        fault_stage(
            "unknown-commit/create-execution",
            unit.create_job_execution(instance.id()).await,
        )?;
        fault_stage(
            "unknown-commit/configure-failpoint",
            configure_fail_command(
                &settings,
                1,
                doc! {
                    "failCommands": ["commitTransaction"],
                    "writeConcernError": {
                        "code": 64_i32,
                        "errmsg": "injected write concern timeout",
                        "errInfo": { "wtimeout": true },
                    },
                },
            )
            .await,
        )?;
        let result = unit.commit().await;
        fault_stage(
            "unknown-commit/disable-failpoint",
            disable_fail_command(&settings).await,
        )?;

        match result {
            Err(RepositoryError::CommitOutcomeUnknown) => {}
            other => {
                return Err(std::io::Error::other(format!(
                    "MongoDB fault stage unknown-commit/classification failed: \
                     expected CommitOutcomeUnknown, observed {other:?}"
                ))
                .into());
            }
        }

        let counts = fault_stage(
            "unknown-commit/readback",
            fault_fixture_counts(&settings, UNKNOWN_COMMIT_JOB).await,
        )?;
        if counts != (1, 1) {
            return Err(std::io::Error::other(format!(
                "MongoDB fault stage unknown-commit/readback failed: \
                 expected (1, 1), observed {counts:?}"
            ))
            .into());
        }

        repository.close().await;
        fault_stage(
            "unknown-commit/post-cleanup",
            clear_fault_fixture(&settings, UNKNOWN_COMMIT_JOB).await,
        )?;
        Ok::<(), Box<dyn Error>>(())
    })?;
    Ok(())
}

#[test]
fn repeated_mongodb_client_lifecycle_has_clean_teardown() -> Result<(), Box<dyn Error>> {
    let Some(_settings) = TestSettings::from_environment() else {
        eprintln!("skipped: MongoDB test environment is not configured");
        return Ok(());
    };

    let output = Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg(LIFECYCLE_PROBE_TEST)
        .arg("--ignored")
        .arg("--nocapture")
        .arg("--test-threads=1")
        .output()?;

    if !output.status.success() {
        return Err(std::io::Error::other(format!(
            "MongoDB lifecycle probe failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        ))
        .into());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    if stdout.contains("panicked at")
        || stderr.contains("panicked at")
        || stdout.contains("connection_requester.rs")
        || stderr.contains("connection_requester.rs")
        || stdout.contains("SendError")
        || stderr.contains("SendError")
    {
        return Err(std::io::Error::other(format!(
            "MongoDB lifecycle probe emitted a background-driver panic: stdout={stdout} stderr={stderr}"
        ))
        .into());
    }
    Ok(())
}

#[test]
#[ignore = "invoked by repeated lifecycle regression harness"]
fn mongodb_client_lifecycle_probe() -> Result<(), Box<dyn Error>> {
    let Some(settings) = TestSettings::from_environment() else {
        eprintln!("skipped: MongoDB test environment is not configured");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let config = settings.config()?;
        for _ in 0..LIFECYCLE_PROBE_CYCLES {
            MongoMigrator::migrate(&config).await?;
            assert_eq!(
                MongoMigrator::installed_schema_version(&config).await?,
                Some(MongoMigrator::supported_schema_version())
            );

            let repository =
                MongoJobRepository::connect(config.clone(), Arc::new(SystemClock)).await?;
            let unit = repository.begin().await?;
            unit.rollback().await?;
            repository.close().await;
        }
        Ok::<(), Box<dyn Error>>(())
    })?;
    Ok(())
}

#[test]
fn mongodb_descriptor_declares_zero_optional_capabilities() -> Result<(), Box<dyn Error>> {
    let Some(settings) = TestSettings::from_environment() else {
        eprintln!("skipped: MongoDB test environment is not configured");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let config = settings.config()?;
        MongoMigrator::migrate(&config).await?;
        let repository = MongoJobRepository::connect(config, Arc::new(SystemClock)).await?;
        assert_eq!(repository.descriptor().capabilities().len(), 0);
        repository.close().await;
        Ok::<(), Box<dyn Error>>(())
    })?;
    Ok(())
}

#[test]
fn newer_mongodb_schema_is_rejected_without_guessing_compatibility() -> Result<(), Box<dyn Error>> {
    let Some(settings) = TestSettings::from_environment() else {
        eprintln!("skipped: MongoDB test environment is not configured");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let config = settings.config()?;
        MongoMigrator::migrate(&config).await?;
        let client = Client::with_uri_str(&settings.uri).await?;
        let schema = client
            .database(&settings.database)
            .collection::<mongodb::bson::Document>("ob_schema");
        schema
            .update_one(
                doc! { "_id": "schema" },
                doc! { "$set": { "version": 3_i64 } },
            )
            .await?;
        assert_eq!(
            MongoMigrator::migrate(&config).await,
            Err(RepositoryError::NewerSchema {
                current: 3,
                supported: MongoMigrator::supported_schema_version(),
            })
        );
        schema
            .update_one(
                doc! { "_id": "schema" },
                doc! { "$set": { "version": 2_i64 } },
            )
            .await?;
        drop(schema);
        client.shutdown().await;
        Ok::<(), Box<dyn Error>>(())
    })?;
    Ok(())
}
