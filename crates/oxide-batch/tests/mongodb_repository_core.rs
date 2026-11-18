//! Runnable-core repository certification source for the first-party `MongoDB` adapter.

#![cfg(feature = "mongodb")]

#[allow(dead_code, unused_imports)]
#[path = "contract/mod.rs"]
mod contract;

use std::error::Error;
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
    Ok(())
}

const CONCURRENT_LAUNCH_JOB: &str = "repository_concurrent_launch";

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
    Ok(())
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
        let row = client
            .database("admin")
            .run_command(doc! { "buildInfo": 1_i32 })
            .await?;
        let actual = row.get_str("version")?;
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
        run_runnable_core_repository_contract("mongodb", |clock| {
            let settings = settings.clone();
            async move {
                clear_runnable_core_fixture(&settings)
                    .await
                    .map_err(|_| RepositoryError::Unavailable)?;
                MongoJobRepository::connect(
                    settings
                        .config()
                        .map_err(|_| RepositoryError::Unavailable)?,
                    clock,
                )
                .await
            }
        })
        .await?;
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
        run_explorer_contract("mongodb", |clock| {
            let settings = settings.clone();
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
                Ok(ServiceBackend {
                    explorer: MongoExplorer::new(repository.clone()),
                    repository,
                    clock,
                })
            }
        })
        .await?;
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
        assert_eq!(
            database
                .collection::<Document>("ob_job_instance")
                .count_documents(doc! { "job_name": CONCURRENT_LAUNCH_JOB })
                .await?,
            1
        );
        assert_eq!(
            database
                .collection::<Document>("ob_job_execution")
                .count_documents(doc! {
                    "job_instance_id": i64::try_from(instance.id().get())?,
                })
                .await?,
            1
        );

        clear_concurrent_launch_fixture(&settings).await?;
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
        Ok::<(), Box<dyn Error>>(())
    })?;
    Ok(())
}
