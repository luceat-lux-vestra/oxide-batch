//! Runnable-core repository certification source for the first-party `MongoDB` adapter.

#![cfg(feature = "mongodb")]

#[allow(dead_code, unused_imports)]
#[path = "contract/mod.rs"]
mod contract;

use std::error::Error;
use std::sync::Arc;

use contract::run_runnable_core_repository_contract;
use mongodb::Client;
use mongodb::bson::doc;
use oxide_batch::{
    JobRepository, MongoConfig, MongoJobRepository, MongoMigrator, RepositoryError, SystemClock,
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
