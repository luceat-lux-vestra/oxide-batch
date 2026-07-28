//! SQLx/PostgreSQL transaction-enlistment spike.

use crate::execution::{BoxFuture, BusinessTransaction, ExecutionError, TransactionalWriter};
use serde_json::json;
use sqlx::migrate::Migrator;
use sqlx::{PgPool, Postgres, Transaction};
use thiserror::Error;

/// The immutable spike migration set.
pub static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

/// The schema version understood by this spike.
pub const SUPPORTED_SCHEMA_VERSION: i32 = 1;

/// `PostgreSQL` spike failures.
#[derive(Debug, Error)]
pub enum PostgresSpikeError {
    /// `SQLx` reported a database or pool failure.
    #[error("PostgreSQL operation failed")]
    Database(#[from] sqlx::Error),
    /// The metadata schema is newer than this runtime.
    #[error("metadata schema is newer than this runtime")]
    NewerSchema,
    /// An optimistic update lost a race.
    #[error("metadata optimistic-lock conflict")]
    OptimisticConflict,
    /// A deterministic failure was injected before commit.
    #[error("failure injected before commit")]
    InjectedBeforeCommit,
    /// A user-facing writer port failed.
    #[error("transactional writer failed")]
    Writer,
}

/// Applies the immutable migration set and validates the explicit schema row.
///
/// # Errors
///
/// Returns a database failure or [`PostgresSpikeError::NewerSchema`].
pub async fn migrate_and_verify(pool: &PgPool) -> Result<(), PostgresSpikeError> {
    MIGRATOR.run(pool).await.map_err(sqlx::Error::from)?;
    let version: i32 =
        sqlx::query_scalar("SELECT version FROM ob_schema_version WHERE singleton = TRUE")
            .fetch_one(pool)
            .await?;
    verify_schema_version(version)
}

/// Rejects metadata created by a newer runtime.
///
/// # Errors
///
/// Returns [`PostgresSpikeError::NewerSchema`] when `version` exceeds the
/// version understood by this spike.
pub fn verify_schema_version(version: i32) -> Result<(), PostgresSpikeError> {
    if version > SUPPORTED_SCHEMA_VERSION {
        Err(PostgresSpikeError::NewerSchema)
    } else {
        Ok(())
    }
}

struct PgBusinessTransaction {
    transaction: Transaction<'static, Postgres>,
}

impl BusinessTransaction for PgBusinessTransaction {
    fn insert<'a>(
        &'a mut self,
        run_id: &'a str,
        item_key: &'a str,
        payload: &'a str,
    ) -> BoxFuture<'a, Result<(), ExecutionError>> {
        Box::pin(async move {
            sqlx::query(
                "INSERT INTO ob_business_item (run_id, item_key, payload) VALUES ($1, $2, $3)",
            )
            .bind(run_id)
            .bind(item_key)
            .bind(payload)
            .execute(&mut *self.transaction)
            .await
            .map_err(|_| ExecutionError::Component)?;
            Ok(())
        })
    }
}

/// A fixture writer implemented only against the core-owned transaction port.
#[derive(Debug)]
pub struct PortOnlyWriter;

impl TransactionalWriter<dyn BusinessTransaction> for PortOnlyWriter {
    fn write<'a>(
        &'a self,
        transaction: &'a mut (dyn BusinessTransaction + 'static),
        run_id: &'a str,
        items: &'a [(&'a str, &'a str)],
    ) -> BoxFuture<'a, Result<(), ExecutionError>> {
        Box::pin(async move {
            for (key, payload) in items {
                transaction.insert(run_id, key, payload).await?;
            }
            Ok(())
        })
    }
}

/// Commits business values, checkpoint, context, counter, and version together.
///
/// # Errors
///
/// Returns a classified database, writer, optimistic-conflict, or injected
/// failure. The transaction remains uncommitted on every error path.
pub async fn commit_enlisted_chunk(
    pool: &PgPool,
    writer: &dyn TransactionalWriter<dyn BusinessTransaction>,
    run_id: &str,
    items: &[(&str, &str)],
    expected_version: i64,
    inject_failure_before_commit: bool,
) -> Result<(), PostgresSpikeError> {
    let mut resource = PgBusinessTransaction {
        transaction: pool.begin().await?,
    };
    writer
        .write(&mut resource, run_id, items)
        .await
        .map_err(|_| PostgresSpikeError::Writer)?;

    let checkpoint = i64::try_from(items.len()).map_err(|_| PostgresSpikeError::Writer)?;
    let write_count = checkpoint;
    let context = json!({"cursor": checkpoint});
    let updated = sqlx::query(
        "INSERT INTO ob_step_execution \
         (step_id, checkpoint, write_count, context, version) \
         VALUES ($1, $2, $3, $4, 0) \
         ON CONFLICT (step_id) DO UPDATE SET \
           checkpoint = EXCLUDED.checkpoint, \
           write_count = EXCLUDED.write_count, \
           context = EXCLUDED.context, \
           version = ob_step_execution.version + 1 \
         WHERE ob_step_execution.version = $5",
    )
    .bind(run_id)
    .bind(checkpoint)
    .bind(write_count)
    .bind(context)
    .bind(expected_version)
    .execute(&mut *resource.transaction)
    .await?;
    if updated.rows_affected() != 1 {
        resource.transaction.rollback().await?;
        return Err(PostgresSpikeError::OptimisticConflict);
    }

    if inject_failure_before_commit {
        resource.transaction.rollback().await?;
        return Err(PostgresSpikeError::InjectedBeforeCommit);
    }

    resource.transaction.commit().await?;
    Ok(())
}
