//! Runnable-core repository certification for the `SQLx` `MySQL`-protocol adapter family.

#![cfg(feature = "mysql")]

#[allow(dead_code, unused_imports)]
#[path = "contract/mod.rs"]
mod contract;

use contract::run_runnable_core_repository_contract;
use oxide_batch::{MysqlConfig, MysqlJobRepository, MysqlMigrator, RepositoryError};
use sqlx::mysql::MySqlPoolOptions;
use std::error::Error;

fn runtime_url() -> Option<String> {
    std::env::var("OXIDEBATCH_MYSQL_TEST_URL").ok()
}

fn backend_name() -> &'static str {
    match std::env::var("OXIDEBATCH_MYSQL_TEST_PRODUCT").as_deref() {
        Ok("mariadb") => "mariadb",
        _ => "mysql",
    }
}

async fn remove_contract_rows(url: &str) -> Result<(), sqlx::Error> {
    let pool = MySqlPoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await?;
    sqlx::query(
        "UPDATE ob_flow_decision decision \
         JOIN ob_job_execution execution ON execution.id = decision.job_execution_id \
         JOIN ob_job_instance instance ON instance.id = execution.job_instance_id \
         SET decision.reused_decision_id = NULL \
         WHERE instance.job_name = 'repository_contract_job'",
    )
    .execute(&pool)
    .await?;
    sqlx::query(
        "DELETE decision FROM ob_flow_decision decision \
         JOIN ob_job_execution execution ON execution.id = decision.job_execution_id \
         JOIN ob_job_instance instance ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = 'repository_contract_job'",
    )
    .execute(&pool)
    .await?;
    sqlx::query(
        "DELETE step FROM ob_step_execution step \
         JOIN ob_job_execution execution ON execution.id = step.job_execution_id \
         JOIN ob_job_instance instance ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = 'repository_contract_job'",
    )
    .execute(&pool)
    .await?;
    sqlx::query(
        "DELETE execution FROM ob_job_execution execution \
         JOIN ob_job_instance instance ON instance.id = execution.job_instance_id \
         WHERE instance.job_name = 'repository_contract_job'",
    )
    .execute(&pool)
    .await?;
    sqlx::query("DELETE FROM ob_job_instance WHERE job_name = 'repository_contract_job'")
        .execute(&pool)
        .await?;
    sqlx::query("DELETE FROM ob_core_job_definition WHERE job_name = 'repository_contract_job'")
        .execute(&pool)
        .await?;
    pool.close().await;
    Ok(())
}

#[test]
fn migration_is_idempotent_for_mysql_protocol_family() -> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_MYSQL_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let config = MysqlConfig::new(url)?;
        MysqlMigrator::migrate(&config).await?;
        MysqlMigrator::migrate(&config).await?;
        assert_eq!(
            MysqlMigrator::installed_schema_version(&config).await?,
            Some(MysqlMigrator::supported_schema_version())
        );
        Ok::<(), Box<dyn Error>>(())
    })?;
    Ok(())
}

#[test]
fn runnable_core_repository_contract_passes_on_mysql_protocol_family() -> Result<(), Box<dyn Error>>
{
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_MYSQL_TEST_URL is not set");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let config = MysqlConfig::new(url.clone())?;
        MysqlMigrator::migrate(&config).await?;
        run_runnable_core_repository_contract(backend_name(), |clock| {
            let url = url.clone();
            async move {
                remove_contract_rows(&url)
                    .await
                    .map_err(|_| RepositoryError::Unavailable)?;
                MysqlJobRepository::connect(
                    MysqlConfig::new(url).map_err(|_| RepositoryError::Unavailable)?,
                    clock,
                )
                .await
            }
        })
        .await?;
        remove_contract_rows(&url).await?;
        Ok::<(), Box<dyn Error>>(())
    })?;
    Ok(())
}
