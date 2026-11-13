use std::error::Error;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Map, Value, json};
use sqlx::migrate::Migrator;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow};
use sqlx::pool::PoolConnection;
use sqlx::{AssertSqlSafe, Connection, Sqlite, SqliteConnection, SqlitePool, Row};

use crate::{
    BatchStatus, BoxFuture, Clock, DefinitionIdentity, DefinitionUpgrade, ExecutionCounts,
    ExecutionMetadata, ExecutionTimestamps, ExecutionVersion, ExitCode, ExitStatus,
    FailureCategory, FailureId, FailureSummary, IdentifierKind, JobExecution, JobExecutionId,
    JobInstance,
    JobInstanceId, JobInstanceKey, JobInstanceSelection, JobName, JobParameter, JobParameters,
    JobRepository, LifecycleError, LifecycleTransition, ParameterName, ParameterRole,
    ParameterValue, ParameterValueKind, RecoveryDecision, RecoveryRequest, RecoveryResult,
    RepositoryCapability, RepositoryDescriptor, RepositoryError, RepositoryUnitOfWork,
    StepExecution, StepExecutionId, StepName,
};

const SUPPORTED_SCHEMA_VERSION: u32 = 1;
const MAX_POOL_SIZE: u32 = 16;
const MAX_INSTANCE_KEY_INPUT: usize = 1024 * 1024;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations-sqlite");

/// Redacted configuration for the `SQLx` `SQLite` repository adapter.
///
/// The connection string is retained only for connection establishment and is
/// never rendered by `Debug` or `Display` output.
#[derive(Clone)]
pub struct SqliteConfig {
    connection_string: String,
    pool_size: u32,
}

impl SqliteConfig {
    /// Builds the default configuration for a SQLite repository.
    ///
    /// # Errors
    ///
    /// Returns `SqliteConfigError` when the connection string is empty or invalid.
    pub fn new(connection_string: impl Into<String>) -> Result<Self, SqliteConfigError> {
        let config = Self {
            connection_string: connection_string.into(),
            pool_size: 1,
        };
        config.validate()?;
        Ok(config)
    }

    /// Sets the finite connection-pool ceiling.
    ///
    /// # Errors
    ///
    /// Rejects zero and values above 16.
    pub fn with_pool_size(mut self, pool_size: u32) -> Result<Self, SqliteConfigError> {
        self.pool_size = pool_size;
        self.validate()?;
        Ok(self)
    }

    fn validate(&self) -> Result<(), SqliteConfigError> {
        if self.connection_string.is_empty() {
            return Err(SqliteConfigError::EmptyConnectionString);
        }
        if !(1..=MAX_POOL_SIZE).contains(&self.pool_size) {
            return Err(SqliteConfigError::PoolSize);
        }
        SqliteConnectOptions::from_str(&self.connection_string)
            .map_err(|_| SqliteConfigError::InvalidConnectionString)?;
        Ok(())
    }

    fn connect_options(&self) -> Result<SqliteConnectOptions, SqliteConfigError> {
        self.validate()?;
        let options = SqliteConnectOptions::from_str(&self.connection_string)
            .map_err(|_| SqliteConfigError::InvalidConnectionString)?
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(5));
        Ok(options)
    }
}

impl fmt::Debug for SqliteConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqliteConfig")
            .field("connection_string", &"<redacted>")
            .field("pool_size", &self.pool_size)
            .finish()
    }
}

/// Value-redacted SQLite adapter configuration error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SqliteConfigError {
    /// The connection string was empty.
    EmptyConnectionString,
    /// `SQLx` rejected the connection-string shape.
    InvalidConnectionString,
    /// Pool size was outside 1 through 16.
    PoolSize,
}

impl fmt::Display for SqliteConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyConnectionString => formatter.write_str("SQLite connection string is empty"),
            Self::InvalidConnectionString => {
                formatter.write_str("SQLite connection string is invalid")
            }
            Self::PoolSize => formatter.write_str("SQLite pool size must be from 1 to 16"),
        }
    }
}

impl Error for SqliteConfigError {}

/// Applies the immutable SQLite metadata migrations for this adapter.
#[derive(Clone, Copy, Debug, Default)]
pub struct SqliteMigrator;

impl SqliteMigrator {
    /// Returns the metadata schema version understood by this adapter.
    #[must_use]
    pub const fn supported_schema_version() -> u32 {
        SUPPORTED_SCHEMA_VERSION
    }

    /// Reads the installed migration version without changing the database.
    ///
    /// # Errors
    ///
    /// Returns a redacted repository failure. An uninitialized database is
    /// reported as `None`.
    pub async fn installed_schema_version(
        config: &SqliteConfig,
    ) -> Result<Option<u32>, RepositoryError> {
        let options = config
            .connect_options()
            .map_err(|_| RepositoryError::Unavailable)?;
        let mut connection = SqliteConnection::connect_with(&options)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        match read_schema_version(&mut connection).await {
            Ok(version) => Ok(Some(version)),
            Err(RepositoryError::SchemaUninitialized) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Applies pending immutable migrations.
    ///
    /// # Errors
    ///
    /// Returns a redacted failure and refuses a schema newer than this runtime.
    pub async fn migrate(config: &SqliteConfig) -> Result<(), RepositoryError> {
        let options = config
            .connect_options()
            .map_err(|_| RepositoryError::Unavailable)?;
        let mut connection = SqliteConnection::connect_with(&options)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        match read_schema_version(&mut connection).await {
            Ok(current) if current > SUPPORTED_SCHEMA_VERSION => {
                return Err(RepositoryError::NewerSchema {
                    current,
                    supported: SUPPORTED_SCHEMA_VERSION,
                });
            }
            Ok(_) | Err(RepositoryError::SchemaUninitialized) => {}
            Err(error) => return Err(error),
        }
        MIGRATOR
            .run(&mut connection)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        verify_schema_version(read_schema_version(&mut connection).await?)
    }
}

/// Core durable `JobRepository` implementation for the `SQLx` `SQLite`.
///
/// This concrete adapter sits below the database-neutral repository SPI. Its
/// initial descriptor deliberately declares no optional capabilities.
///
/// SQLite uses a serialized-writer profile: each repository unit of work starts
/// with `BEGIN IMMEDIATE`, and the default pool has one connection. Raising the
/// pool ceiling permits concurrent readers and queued writers but does not claim
/// server-database write concurrency.
#[derive(Clone)]
pub struct SqliteJobRepository {
    pool: SqlitePool,
    clock: Arc<dyn Clock>,
    config: SqliteConfig,
}

impl SqliteJobRepository {
    /// Opens a pool and verifies the metadata schema version.
    ///
    /// # Errors
    ///
    /// Returns a redacted repository/configuration failure.
    pub async fn connect(
        config: SqliteConfig,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, RepositoryError> {
        let options = config
            .connect_options()
            .map_err(|_| RepositoryError::Unavailable)?;
        let pool = SqlitePoolOptions::new()
            .max_connections(config.pool_size)
            .connect_with(options)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        verify_schema_version(read_schema_version(&pool).await?)?;
        Ok(Self {
            pool,
            clock,
            config,
        })
    }

    /// Closes the repository connection pool.
    pub async fn close(&self) {
        self.pool.close().await;
    }

    async fn begin_connection(&self) -> Result<PoolConnection<Sqlite>, RepositoryError> {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        if sqlx::raw_sql("BEGIN IMMEDIATE")
            .execute(&mut *connection)
            .await
            .is_err()
        {
            connection.close_on_drop();
            return Err(RepositoryError::Unavailable);
        }
        Ok(connection)
    }
}

impl fmt::Debug for SqliteJobRepository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqliteJobRepository")
            .field("pool_size", &self.pool.size())
            .field("pool_idle", &self.pool.num_idle())
            .finish_non_exhaustive()
    }
}

impl JobRepository for SqliteJobRepository {
    fn connection_capacity(&self) -> u32 {
        self.config.pool_size
    }

    fn descriptor(&self) -> RepositoryDescriptor {
        RepositoryDescriptor::new(SUPPORTED_SCHEMA_VERSION, [])
    }

    fn begin<'a>(
        &'a self,
    ) -> BoxFuture<'a, Result<Box<dyn RepositoryUnitOfWork + 'a>, RepositoryError>> {
        Box::pin(async move {
            let connection = self.begin_connection().await?;
            Ok(Box::new(SqliteUnitOfWork {
                repository: self,
                connection: Some(connection),
            }) as Box<dyn RepositoryUnitOfWork + 'a>)
        })
    }
}

struct SqliteUnitOfWork<'repository> {
    repository: &'repository SqliteJobRepository,
    connection: Option<PoolConnection<Sqlite>>,
}

impl SqliteUnitOfWork<'_> {
    fn connection(&mut self) -> Result<&mut PoolConnection<Sqlite>, RepositoryError> {
        self.connection.as_mut().ok_or(RepositoryError::Unavailable)
    }

    async fn job_execution(
        &mut self,
        id: JobExecutionId,
    ) -> Result<Option<JobExecution>, RepositoryError> {
        let row = sqlx::query(AssertSqlSafe(job_execution_select(
            "WHERE execution.id = ?",
        )))
        .bind(sqlite_identifier(id.get(), IdentifierKind::JobExecution)?)
        .fetch_optional(&mut **self.connection()?)
        .await
        .map_err(|_| RepositoryError::Unavailable)?;
        row.map(|row| decode_job_execution(&row)).transpose()
    }

    async fn step_execution(
        &mut self,
        id: StepExecutionId,
    ) -> Result<Option<StepExecution>, RepositoryError> {
        let row = sqlx::query(AssertSqlSafe(step_execution_select(
            "WHERE execution.id = ?",
        )))
        .bind(sqlite_identifier(id.get(), IdentifierKind::StepExecution)?)
        .fetch_optional(&mut **self.connection()?)
        .await
        .map_err(|_| RepositoryError::Unavailable)?;
        row.map(|row| decode_step_execution(&row)).transpose()
    }

    async fn classify_job_cas(
        &mut self,
        id: JobExecutionId,
        expected: ExecutionVersion,
    ) -> RepositoryError {
        let connection = match self.connection() {
            Ok(connection) => connection,
            Err(error) => return error,
        };
        let row = sqlx::query(AssertSqlSafe(job_execution_select(
            "WHERE execution.id = ?",
        )))
        .bind(match sqlite_identifier(id.get(), IdentifierKind::JobExecution) {
            Ok(value) => value,
            Err(error) => return error,
        })
        .fetch_optional(&mut **connection)
        .await;
        match row {
            Ok(Some(row)) => match decode_job_execution(&row) {
                Ok(actual) => RepositoryError::Lifecycle(LifecycleError::StaleVersion {
                    expected,
                    actual: actual.version(),
                }),
                Err(error) => error,
            },
            Ok(None) => RepositoryError::JobExecutionNotFound { id },
            Err(_) => RepositoryError::Unavailable,
        }
    }

    async fn classify_step_cas(
        &mut self,
        id: StepExecutionId,
        expected: ExecutionVersion,
    ) -> RepositoryError {
        let connection = match self.connection() {
            Ok(connection) => connection,
            Err(error) => return error,
        };
        let row = sqlx::query(AssertSqlSafe(step_execution_select(
            "WHERE execution.id = ?",
        )))
        .bind(match sqlite_identifier(id.get(), IdentifierKind::StepExecution) {
            Ok(value) => value,
            Err(error) => return error,
        })
        .fetch_optional(&mut **connection)
        .await;
        match row {
            Ok(Some(row)) => match decode_step_execution(&row) {
                Ok(actual) => RepositoryError::Lifecycle(LifecycleError::StaleVersion {
                    expected,
                    actual: actual.version(),
                }),
                Err(error) => error,
            },
            Ok(None) => RepositoryError::StepExecutionNotFound { id },
            Err(_) => RepositoryError::Unavailable,
        }
    }
}

impl RepositoryUnitOfWork for SqliteUnitOfWork<'_> {
    fn register_definition_upgrade<'a>(
        &'a mut self,
        _job_name: &'a JobName,
        _upgrade: &'a DefinitionUpgrade,
    ) -> BoxFuture<'a, Result<(), RepositoryError>> {
        Box::pin(async {
            Err(RepositoryError::UnsupportedCapability {
                capability: RepositoryCapability::DefinitionRegistry,
            })
        })
    }

    fn select_or_create_job_instance<'a>(
        &'a mut self,
        key: &'a JobInstanceKey,
    ) -> BoxFuture<'a, Result<JobInstanceSelection, RepositoryError>> {
        Box::pin(async move {
            let digest = key.digest();
            let parameters = serde_json::to_string(&encode_identifying_parameters(key)?)
                .map_err(|_| RepositoryError::Unavailable)?;
            let created_ms = system_time_millis(self.repository.clock.now())?;
            let inserted = sqlx::query(
                "INSERT INTO ob_job_instance \
                 (job_name, instance_key, identifying_parameters, created_ms) \
                 VALUES (?, ?, ?, ?)",
            )
            .bind(key.job_name().as_str())
            .bind(&digest[..])
            .bind(parameters)
            .bind(created_ms)
            .execute(&mut **self.connection()?)
            .await;

            match inserted {
                Ok(result) => {
                    let id = JobInstanceId::new(rowid_u64(
                        result.last_insert_rowid(),
                        IdentifierKind::JobInstance,
                    )?)?;
                    Ok(JobInstanceSelection::Created(JobInstance::new(
                        id,
                        key.clone(),
                    )))
                }
                Err(error) if is_duplicate_key(&error) => {
                    let row = sqlx::query(
                        "SELECT id, job_name, identifying_parameters \
                         FROM ob_job_instance WHERE job_name = ? AND instance_key = ?",
                    )
                    .bind(key.job_name().as_str())
                    .bind(&digest[..])
                    .fetch_one(&mut **self.connection()?)
                    .await
                    .map_err(|_| RepositoryError::Unavailable)?;
                    Ok(JobInstanceSelection::Existing(decode_job_instance(&row)?))
                }
                Err(_) => Err(RepositoryError::Unavailable),
            }
        })
    }

    fn create_job_execution(
        &mut self,
        job_instance_id: JobInstanceId,
    ) -> BoxFuture<'_, Result<JobExecution, RepositoryError>> {
        Box::pin(async move {
            let instance: Option<i64> =
                sqlx::query_scalar("SELECT id FROM ob_job_instance WHERE id = ?")
                    .bind(sqlite_identifier(job_instance_id.get(), IdentifierKind::JobInstance)?)
                    .fetch_optional(&mut **self.connection()?)
                    .await
                    .map_err(|_| RepositoryError::Unavailable)?;
            if instance.is_none() {
                return Err(RepositoryError::JobInstanceNotFound {
                    id: job_instance_id,
                });
            }

            let row = sqlx::query(AssertSqlSafe(job_execution_select(
                "WHERE execution.job_instance_id = ? ORDER BY execution.attempt DESC LIMIT 1",
            )))
            .bind(sqlite_identifier(job_instance_id.get(), IdentifierKind::JobInstance)?)
            .fetch_optional(&mut **self.connection()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;

            let mut attempt = 1_u64;
            if let Some(row) = row {
                let latest = decode_job_execution(&row)?;
                attempt = read_u64(&row, "attempt")?
                    .checked_add(1)
                    .ok_or(RepositoryError::Unavailable)?;
                match latest.metadata().status() {
                    BatchStatus::Stopped | BatchStatus::Failed => {}
                    BatchStatus::Completed => {
                        return Err(RepositoryError::CompletedInstance {
                            id: job_instance_id,
                        });
                    }
                    BatchStatus::Abandoned => {
                        return Err(RepositoryError::AbandonedInstance {
                            id: job_instance_id,
                        });
                    }
                    status => {
                        return Err(RepositoryError::ExecutionAlreadyActive {
                            instance_id: job_instance_id,
                            execution_id: latest.id(),
                            status,
                        });
                    }
                }
            }

            let created_at = self.repository.clock.now();
            let created_ms = system_time_millis(created_at)?;
            let result = sqlx::query(
                "INSERT INTO ob_job_execution \
                 (job_instance_id, attempt, status, exit_code, created_ms, updated_ms, version) \
                 VALUES (?, ?, 'STARTING', 'UNKNOWN', ?, ?, 0)",
            )
            .bind(sqlite_identifier(job_instance_id.get(), IdentifierKind::JobInstance)?)
            .bind(sqlite_i64(attempt)?)
            .bind(created_ms)
            .bind(created_ms)
            .execute(&mut **self.connection()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            let id = JobExecutionId::new(rowid_u64(
                result.last_insert_rowid(),
                IdentifierKind::JobExecution,
            )?)?;
            Ok(JobExecution::new(
                id,
                job_instance_id,
                starting_metadata(created_at)?,
            ))
        })
    }

    fn create_job_execution_with_definition<'a>(
        &'a mut self,
        _job_instance_id: JobInstanceId,
        _definition: &'a DefinitionIdentity,
    ) -> BoxFuture<'a, Result<JobExecution, RepositoryError>> {
        Box::pin(async {
            Err(RepositoryError::UnsupportedCapability {
                capability: RepositoryCapability::DefinitionRegistry,
            })
        })
    }

    fn create_step_execution<'a>(
        &'a mut self,
        job_execution_id: JobExecutionId,
        step_name: &'a StepName,
    ) -> BoxFuture<'a, Result<StepExecution, RepositoryError>> {
        Box::pin(async move {
            let job: Option<i64> =
                sqlx::query_scalar("SELECT id FROM ob_job_execution WHERE id = ?")
                    .bind(sqlite_identifier(job_execution_id.get(), IdentifierKind::JobExecution)?)
                    .fetch_optional(&mut **self.connection()?)
                    .await
                    .map_err(|_| RepositoryError::Unavailable)?;
            if job.is_none() {
                return Err(RepositoryError::JobExecutionNotFound {
                    id: job_execution_id,
                });
            }
            let created_at = self.repository.clock.now();
            let created_ms = system_time_millis(created_at)?;
            let result = sqlx::query(
                "INSERT INTO ob_step_execution \
                 (job_execution_id, step_name, status, exit_code, created_ms, updated_ms, version) \
                 VALUES (?, ?, 'STARTING', 'UNKNOWN', ?, ?, 0)",
            )
            .bind(sqlite_identifier(job_execution_id.get(), IdentifierKind::JobExecution)?)
            .bind(step_name.as_str())
            .bind(created_ms)
            .bind(created_ms)
            .execute(&mut **self.connection()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            let id = StepExecutionId::new(rowid_u64(
                result.last_insert_rowid(),
                IdentifierKind::StepExecution,
            )?)?;
            Ok(StepExecution::new(
                id,
                job_execution_id,
                step_name.clone(),
                starting_metadata(created_at)?,
            ))
        })
    }

    fn transition_job_execution(
        &mut self,
        id: JobExecutionId,
        expected_version: ExecutionVersion,
        transition: LifecycleTransition,
    ) -> BoxFuture<'_, Result<JobExecution, RepositoryError>> {
        Box::pin(async move {
            let mut execution = self
                .job_execution(id)
                .await?
                .ok_or(RepositoryError::JobExecutionNotFound { id })?;
            execution.transition(expected_version, transition)?;
            let affected = update_job_execution(
                &mut **self.connection()?,
                &execution,
                transition.transitioned_at(),
                expected_version,
            )
            .await?;
            if affected != 1 {
                return Err(self.classify_job_cas(id, expected_version).await);
            }
            Ok(execution)
        })
    }

    fn enrich_job_exit_status<'a>(
        &'a mut self,
        id: JobExecutionId,
        expected_version: ExecutionVersion,
        exit_status: &'a ExitStatus,
    ) -> BoxFuture<'a, Result<JobExecution, RepositoryError>> {
        Box::pin(async move {
            let mut execution = self
                .job_execution(id)
                .await?
                .ok_or(RepositoryError::JobExecutionNotFound { id })?;
            execution.enrich_exit_status(expected_version, exit_status.clone())?;
            let updated_at = self.repository.clock.now();
            let affected = update_job_execution(
                &mut **self.connection()?,
                &execution,
                updated_at,
                expected_version,
            )
            .await?;
            if affected != 1 {
                return Err(self.classify_job_cas(id, expected_version).await);
            }
            Ok(execution)
        })
    }

    fn transition_step_execution(
        &mut self,
        id: StepExecutionId,
        expected_version: ExecutionVersion,
        transition: LifecycleTransition,
    ) -> BoxFuture<'_, Result<StepExecution, RepositoryError>> {
        Box::pin(async move {
            let mut execution = self
                .step_execution(id)
                .await?
                .ok_or(RepositoryError::StepExecutionNotFound { id })?;
            execution.transition(expected_version, transition)?;
            let affected = update_step_execution(
                &mut **self.connection()?,
                &execution,
                transition.transitioned_at(),
                expected_version,
            )
            .await?;
            if affected != 1 {
                return Err(self.classify_step_cas(id, expected_version).await);
            }
            Ok(execution)
        })
    }

    fn enrich_step_exit_status<'a>(
        &'a mut self,
        id: StepExecutionId,
        expected_version: ExecutionVersion,
        exit_status: &'a ExitStatus,
    ) -> BoxFuture<'a, Result<StepExecution, RepositoryError>> {
        Box::pin(async move {
            let mut execution = self
                .step_execution(id)
                .await?
                .ok_or(RepositoryError::StepExecutionNotFound { id })?;
            execution.enrich_exit_status(expected_version, exit_status.clone())?;
            let updated_at = self.repository.clock.now();
            let affected = update_step_execution(
                &mut **self.connection()?,
                &execution,
                updated_at,
                expected_version,
            )
            .await?;
            if affected != 1 {
                return Err(self.classify_step_cas(id, expected_version).await);
            }
            Ok(execution)
        })
    }

    fn find_job_instance<'a>(
        &'a mut self,
        key: &'a JobInstanceKey,
    ) -> BoxFuture<'a, Result<Option<JobInstance>, RepositoryError>> {
        Box::pin(async move {
            let digest = key.digest();
            let row = sqlx::query(
                "SELECT id, job_name, identifying_parameters \
                 FROM ob_job_instance WHERE job_name = ? AND instance_key = ?",
            )
            .bind(key.job_name().as_str())
            .bind(&digest[..])
            .fetch_optional(&mut **self.connection()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            row.map(|row| decode_job_instance(&row)).transpose()
        })
    }

    fn get_job_instance(
        &mut self,
        id: JobInstanceId,
    ) -> BoxFuture<'_, Result<Option<JobInstance>, RepositoryError>> {
        Box::pin(async move {
            let row = sqlx::query(
                "SELECT id, job_name, identifying_parameters FROM ob_job_instance WHERE id = ?",
            )
            .bind(sqlite_identifier(id.get(), IdentifierKind::JobInstance)?)
            .fetch_optional(&mut **self.connection()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            row.map(|row| decode_job_instance(&row)).transpose()
        })
    }

    fn get_job_execution(
        &mut self,
        id: JobExecutionId,
    ) -> BoxFuture<'_, Result<Option<JobExecution>, RepositoryError>> {
        Box::pin(async move { self.job_execution(id).await })
    }

    fn job_executions(
        &mut self,
        job_instance_id: JobInstanceId,
    ) -> BoxFuture<'_, Result<Vec<JobExecution>, RepositoryError>> {
        Box::pin(async move {
            let instance: Option<i64> =
                sqlx::query_scalar("SELECT id FROM ob_job_instance WHERE id = ?")
                    .bind(sqlite_identifier(job_instance_id.get(), IdentifierKind::JobInstance)?)
                    .fetch_optional(&mut **self.connection()?)
                    .await
                    .map_err(|_| RepositoryError::Unavailable)?;
            if instance.is_none() {
                return Err(RepositoryError::JobInstanceNotFound {
                    id: job_instance_id,
                });
            }
            let rows = sqlx::query(AssertSqlSafe(job_execution_select(
                "WHERE execution.job_instance_id = ? ORDER BY execution.attempt",
            )))
            .bind(sqlite_identifier(job_instance_id.get(), IdentifierKind::JobInstance)?)
            .fetch_all(&mut **self.connection()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            rows.iter().map(decode_job_execution).collect()
        })
    }

    fn get_step_execution(
        &mut self,
        id: StepExecutionId,
    ) -> BoxFuture<'_, Result<Option<StepExecution>, RepositoryError>> {
        Box::pin(async move { self.step_execution(id).await })
    }

    fn step_executions(
        &mut self,
        job_execution_id: JobExecutionId,
    ) -> BoxFuture<'_, Result<Vec<StepExecution>, RepositoryError>> {
        Box::pin(async move {
            let job: Option<i64> =
                sqlx::query_scalar("SELECT id FROM ob_job_execution WHERE id = ?")
                    .bind(sqlite_identifier(job_execution_id.get(), IdentifierKind::JobExecution)?)
                    .fetch_optional(&mut **self.connection()?)
                    .await
                    .map_err(|_| RepositoryError::Unavailable)?;
            if job.is_none() {
                return Err(RepositoryError::JobExecutionNotFound {
                    id: job_execution_id,
                });
            }
            let rows = sqlx::query(AssertSqlSafe(step_execution_select(
                "WHERE execution.job_execution_id = ? ORDER BY execution.id",
            )))
            .bind(sqlite_identifier(job_execution_id.get(), IdentifierKind::JobExecution)?)
            .fetch_all(&mut **self.connection()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            rows.iter().map(decode_step_execution).collect()
        })
    }

    fn recover_job_execution<'a>(
        &'a mut self,
        _id: JobExecutionId,
        _request: &'a RecoveryRequest,
    ) -> BoxFuture<'a, Result<RecoveryResult, RepositoryError>> {
        Box::pin(async {
            Err(RepositoryError::UnsupportedCapability {
                capability: RepositoryCapability::ExecutionOwnership,
            })
        })
    }

    fn recovery_decision(
        &mut self,
        _id: JobExecutionId,
    ) -> BoxFuture<'_, Result<Option<RecoveryDecision>, RepositoryError>> {
        Box::pin(async {
            Err(RepositoryError::UnsupportedCapability {
                capability: RepositoryCapability::ExecutionOwnership,
            })
        })
    }

    fn commit<'a>(mut self: Box<Self>) -> BoxFuture<'a, Result<(), RepositoryError>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            let mut connection = self.connection.take().ok_or(RepositoryError::Unavailable)?;
            if sqlx::raw_sql("COMMIT")
                .execute(&mut *connection)
                .await
                .is_err()
            {
                connection.close_on_drop();
                return Err(RepositoryError::CommitOutcomeUnknown);
            }
            Ok(())
        })
    }

    fn rollback<'a>(mut self: Box<Self>) -> BoxFuture<'a, Result<(), RepositoryError>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            let mut connection = self.connection.take().ok_or(RepositoryError::Unavailable)?;
            if sqlx::raw_sql("ROLLBACK")
                .execute(&mut *connection)
                .await
                .is_err()
            {
                connection.close_on_drop();
                return Err(RepositoryError::Unavailable);
            }
            Ok(())
        })
    }
}

impl Drop for SqliteUnitOfWork<'_> {
    fn drop(&mut self) {
        if let Some(connection) = &mut self.connection {
            connection.close_on_drop();
        }
    }
}

fn job_execution_select(suffix: &str) -> String {
    format!(
        "SELECT execution.id, execution.job_instance_id, execution.attempt, \
         execution.status, execution.exit_code, \
         CAST(0 AS INTEGER) AS read_count, CAST(0 AS INTEGER) AS processed_count, \
         CAST(0 AS INTEGER) AS write_count, CAST(0 AS INTEGER) AS filter_count, \
         CAST(0 AS INTEGER) AS commit_count, CAST(0 AS INTEGER) AS rollback_count, \
         execution.failure_category, execution.failure_id, execution.created_ms, \
         execution.started_ms, execution.ended_ms, execution.version \
         FROM ob_job_execution execution {suffix}"
    )
}

fn step_execution_select(suffix: &str) -> String {
    format!(
        "SELECT execution.id, execution.job_execution_id, execution.step_name, \
         execution.status, execution.exit_code, execution.read_count, \
         execution.processed_count, execution.write_count, execution.filter_count, \
         execution.commit_count, execution.rollback_count, execution.failure_category, \
         execution.failure_id, execution.created_ms, execution.started_ms, \
         execution.ended_ms, execution.version \
         FROM ob_step_execution execution {suffix}"
    )
}

async fn update_job_execution(
    connection: &mut SqliteConnection,
    execution: &JobExecution,
    updated_at: SystemTime,
    expected: ExecutionVersion,
) -> Result<u64, RepositoryError> {
    update_execution(
        connection,
        "ob_job_execution",
        execution.id().get(),
        execution.metadata(),
        execution.version(),
        updated_at,
        expected,
    )
    .await
}

async fn update_step_execution(
    connection: &mut SqliteConnection,
    execution: &StepExecution,
    updated_at: SystemTime,
    expected: ExecutionVersion,
) -> Result<u64, RepositoryError> {
    let metadata = execution.metadata();
    let timestamps = metadata.timestamps();
    let failure = metadata.failure();
    let counts = metadata.counts();
    let result = sqlx::query(
        "UPDATE ob_step_execution SET status = ?, exit_code = ?, failure_category = ?, \
         failure_id = ?, started_ms = ?, ended_ms = ?, read_count = ?, processed_count = ?, \
         write_count = ?, filter_count = ?, commit_count = ?, rollback_count = ?, \
         updated_ms = ?, version = ? WHERE id = ? AND version = ?",
    )
    .bind(metadata.status().to_string())
    .bind(metadata.exit_status().code().as_str())
    .bind(failure.map(|value| value.category().durable_code()))
    .bind(failure.map(|value| sqlite_i64(value.failure_id().get())).transpose()?)
    .bind(
        timestamps
            .started_at()
            .map(system_time_millis)
            .transpose()?,
    )
    .bind(timestamps.ended_at().map(system_time_millis).transpose()?)
    .bind(sqlite_i64(counts.read())?)
    .bind(sqlite_i64(counts.processed())?)
    .bind(sqlite_i64(counts.written())?)
    .bind(sqlite_i64(counts.filtered())?)
    .bind(sqlite_i64(counts.committed())?)
    .bind(sqlite_i64(counts.rolled_back())?)
    .bind(system_time_millis(updated_at)?)
    .bind(sqlite_i64(execution.version().get())?)
    .bind(sqlite_identifier(execution.id().get(), IdentifierKind::StepExecution)?)
    .bind(sqlite_i64(expected.get())?)
    .execute(connection)
    .await
    .map_err(|_| RepositoryError::Unavailable)?;
    Ok(result.rows_affected())
}

async fn update_execution(
    connection: &mut SqliteConnection,
    table: &'static str,
    id: u64,
    metadata: &ExecutionMetadata,
    new_version: ExecutionVersion,
    updated_at: SystemTime,
    expected: ExecutionVersion,
) -> Result<u64, RepositoryError> {
    let query = format!(
        "UPDATE {table} SET status = ?, exit_code = ?, failure_category = ?, failure_id = ?, \
         started_ms = ?, ended_ms = ?, updated_ms = ?, version = ? WHERE id = ? AND version = ?"
    );
    let timestamps = metadata.timestamps();
    let failure = metadata.failure();
    let result = sqlx::query(AssertSqlSafe(query))
        .bind(metadata.status().to_string())
        .bind(metadata.exit_status().code().as_str())
        .bind(failure.map(|value| value.category().durable_code()))
        .bind(failure.map(|value| sqlite_i64(value.failure_id().get())).transpose()?)
        .bind(
            timestamps
                .started_at()
                .map(system_time_millis)
                .transpose()?,
        )
        .bind(timestamps.ended_at().map(system_time_millis).transpose()?)
        .bind(system_time_millis(updated_at)?)
        .bind(sqlite_i64(new_version.get())?)
        .bind(sqlite_i64(id)?)
        .bind(sqlite_i64(expected.get())?)
        .execute(connection)
        .await
        .map_err(|_| RepositoryError::Unavailable)?;
    Ok(result.rows_affected())
}

fn decode_job_instance(row: &SqliteRow) -> Result<JobInstance, RepositoryError> {
    let id = JobInstanceId::new(read_u64(row, "id")?)?;
    let job_name = JobName::new(
        row.try_get::<String, _>("job_name")
            .map_err(|_| RepositoryError::Unavailable)?,
    )?;
    let encoded = row
        .try_get::<String, _>("identifying_parameters")
        .map_err(|_| RepositoryError::Unavailable)?;
    let value: Value = serde_json::from_str(&encoded).map_err(|_| RepositoryError::Unavailable)?;
    let parameters = decode_identifying_parameters(&value)?;
    Ok(JobInstance::new(
        id,
        JobInstanceKey::new(job_name, &parameters),
    ))
}

fn decode_job_execution(row: &SqliteRow) -> Result<JobExecution, RepositoryError> {
    let id = JobExecutionId::new(read_u64(row, "id")?)?;
    let instance_id = JobInstanceId::new(read_u64(row, "job_instance_id")?)?;
    let metadata = decode_execution_metadata(row)?;
    Ok(JobExecution::from_snapshot(
        id,
        instance_id,
        metadata,
        ExecutionVersion::new(read_u64(row, "version")?),
    ))
}

fn decode_step_execution(row: &SqliteRow) -> Result<StepExecution, RepositoryError> {
    let id = StepExecutionId::new(read_u64(row, "id")?)?;
    let job_id = JobExecutionId::new(read_u64(row, "job_execution_id")?)?;
    let name = StepName::new(
        row.try_get::<String, _>("step_name")
            .map_err(|_| RepositoryError::Unavailable)?,
    )?;
    let metadata = decode_execution_metadata(row)?;
    Ok(StepExecution::from_snapshot(
        id,
        job_id,
        name,
        metadata,
        ExecutionVersion::new(read_u64(row, "version")?),
    ))
}

fn decode_execution_metadata(row: &SqliteRow) -> Result<ExecutionMetadata, RepositoryError> {
    let status = decode_status(
        &row.try_get::<String, _>("status")
            .map_err(|_| RepositoryError::Unavailable)?,
    )?;
    let exit_status = ExitStatus::new(ExitCode::new(
        row.try_get::<String, _>("exit_code")
            .map_err(|_| RepositoryError::Unavailable)?,
    )?);
    let created = millis_system_time(read_i64(row, "created_ms")?)?;
    let started = read_optional_i64(row, "started_ms")?
        .map(millis_system_time)
        .transpose()?;
    let ended = read_optional_i64(row, "ended_ms")?
        .map(millis_system_time)
        .transpose()?;
    let category = row
        .try_get::<Option<String>, _>("failure_category")
        .map_err(|_| RepositoryError::Unavailable)?;
    let failure_id = read_optional_u64(row, "failure_id")?;
    let failure = match (category, failure_id) {
        (None, None) => None,
        (Some(category), Some(id)) => Some(FailureSummary::new(
            FailureCategory::from_durable_code(&category).ok_or(RepositoryError::Unavailable)?,
            FailureId::new(id)?,
        )),
        _ => return Err(RepositoryError::Unavailable),
    };
    ExecutionMetadata::new(
        status,
        exit_status,
        ExecutionTimestamps::new(created, started, ended)?,
        ExecutionCounts::new(
            read_u64(row, "read_count")?,
            read_u64(row, "processed_count")?,
            read_u64(row, "write_count")?,
            read_u64(row, "filter_count")?,
            read_u64(row, "commit_count")?,
            read_u64(row, "rollback_count")?,
        ),
        failure,
    )
    .map_err(Into::into)
}

fn encode_identifying_parameters(key: &JobInstanceKey) -> Result<Value, RepositoryError> {
    let mut object = Map::new();
    for (name, kind) in key.identifying_fields() {
        let parameter = key
            .identifying_value(name)
            .ok_or(RepositoryError::Unavailable)?;
        let (kind_name, value) = match kind {
            ParameterValueKind::String => (
                "string",
                Value::String(
                    parameter
                        .as_str()
                        .ok_or(RepositoryError::Unavailable)?
                        .to_owned(),
                ),
            ),
            ParameterValueKind::I64 => (
                "i64",
                Value::Number(
                    parameter
                        .as_i64()
                        .ok_or(RepositoryError::Unavailable)?
                        .into(),
                ),
            ),
            ParameterValueKind::U64 => (
                "u64",
                Value::Number(
                    parameter
                        .as_u64()
                        .ok_or(RepositoryError::Unavailable)?
                        .into(),
                ),
            ),
            ParameterValueKind::Bool => (
                "bool",
                Value::Bool(parameter.as_bool().ok_or(RepositoryError::Unavailable)?),
            ),
            _ => return Err(RepositoryError::Unavailable),
        };
        object.insert(
            name.as_str().to_owned(),
            json!({"type": kind_name, "identifying": true, "value": value}),
        );
    }
    let value = Value::Object(object);
    if serde_json::to_vec(&value)
        .map_err(|_| RepositoryError::Unavailable)?
        .len()
        > MAX_INSTANCE_KEY_INPUT
    {
        return Err(RepositoryError::Unavailable);
    }
    Ok(value)
}

fn decode_identifying_parameters(value: &Value) -> Result<JobParameters, RepositoryError> {
    let object = value.as_object().ok_or(RepositoryError::Unavailable)?;
    let mut parameters = JobParameters::new();
    for (raw_name, envelope) in object {
        let envelope = envelope.as_object().ok_or(RepositoryError::Unavailable)?;
        if envelope.get("identifying") != Some(&Value::Bool(true)) {
            return Err(RepositoryError::Unavailable);
        }
        let kind = envelope
            .get("type")
            .and_then(Value::as_str)
            .ok_or(RepositoryError::Unavailable)?;
        let raw_value = envelope.get("value").ok_or(RepositoryError::Unavailable)?;
        let value = match kind {
            "string" => {
                ParameterValue::string(raw_value.as_str().ok_or(RepositoryError::Unavailable)?)?
            }
            "i64" => ParameterValue::from(raw_value.as_i64().ok_or(RepositoryError::Unavailable)?),
            "u64" => ParameterValue::from(raw_value.as_u64().ok_or(RepositoryError::Unavailable)?),
            "bool" => {
                ParameterValue::from(raw_value.as_bool().ok_or(RepositoryError::Unavailable)?)
            }
            _ => return Err(RepositoryError::Unavailable),
        };
        parameters.insert(
            ParameterName::new(raw_name.clone())?,
            JobParameter::new(value, ParameterRole::Identifying),
        )?;
    }
    Ok(parameters)
}

async fn read_schema_version<'executor, E>(executor: E) -> Result<u32, RepositoryError>
where
    E: sqlx::Executor<'executor, Database = Sqlite>,
{
    let version = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT MAX(version) FROM _sqlx_migrations WHERE success = TRUE",
    )
    .fetch_one(executor)
    .await
    .map_err(|error| {
        if is_missing_table(&error) {
            RepositoryError::SchemaUninitialized
        } else {
            RepositoryError::Unavailable
        }
    })?
    .ok_or(RepositoryError::SchemaUninitialized)?;
    u32::try_from(version).map_err(|_| RepositoryError::Unavailable)
}

fn verify_schema_version(current: u32) -> Result<(), RepositoryError> {
    match current.cmp(&SUPPORTED_SCHEMA_VERSION) {
        std::cmp::Ordering::Less => Err(RepositoryError::MigrationRequired {
            current,
            supported: SUPPORTED_SCHEMA_VERSION,
        }),
        std::cmp::Ordering::Equal => Ok(()),
        std::cmp::Ordering::Greater => Err(RepositoryError::NewerSchema {
            current,
            supported: SUPPORTED_SCHEMA_VERSION,
        }),
    }
}

fn is_missing_table(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::code)
        .as_deref()
        .is_some_and(|code| code == "1")
}

fn is_duplicate_key(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::code)
        .as_deref()
        .is_some_and(|code| code == "19" || code == "1555" || code == "2067")
}

fn starting_metadata(created_at: SystemTime) -> Result<ExecutionMetadata, RepositoryError> {
    Ok(ExecutionMetadata::new(
        BatchStatus::Starting,
        ExitStatus::unknown(),
        ExecutionTimestamps::new(created_at, None, None)?,
        ExecutionCounts::default(),
        None,
    )?)
}

fn system_time_millis(value: SystemTime) -> Result<i64, RepositoryError> {
    let duration = value
        .duration_since(UNIX_EPOCH)
        .map_err(|_| RepositoryError::Unavailable)?;
    i64::try_from(duration.as_millis()).map_err(|_| RepositoryError::Unavailable)
}

fn millis_system_time(value: i64) -> Result<SystemTime, RepositoryError> {
    let value = u64::try_from(value).map_err(|_| RepositoryError::Unavailable)?;
    Ok(UNIX_EPOCH + std::time::Duration::from_millis(value))
}

fn read_i64(row: &SqliteRow, name: &str) -> Result<i64, RepositoryError> {
    row.try_get(name).map_err(|_| RepositoryError::Unavailable)
}

fn read_optional_i64(row: &SqliteRow, name: &str) -> Result<Option<i64>, RepositoryError> {
    row.try_get(name).map_err(|_| RepositoryError::Unavailable)
}

fn read_u64(row: &SqliteRow, name: &str) -> Result<u64, RepositoryError> {
    let value = row
        .try_get::<i64, _>(name)
        .map_err(|_| RepositoryError::Unavailable)?;
    u64::try_from(value).map_err(|_| RepositoryError::Unavailable)
}

fn read_optional_u64(row: &SqliteRow, name: &str) -> Result<Option<u64>, RepositoryError> {
    row.try_get::<Option<i64>, _>(name)
        .map_err(|_| RepositoryError::Unavailable)?
        .map(|value| u64::try_from(value).map_err(|_| RepositoryError::Unavailable))
        .transpose()
}

fn sqlite_i64(value: u64) -> Result<i64, RepositoryError> {
    i64::try_from(value).map_err(|_| RepositoryError::Unavailable)
}

fn sqlite_identifier(value: u64, kind: IdentifierKind) -> Result<i64, RepositoryError> {
    i64::try_from(value).map_err(|_| RepositoryError::IdentifierOutOfRange { kind, value })
}

fn rowid_u64(value: i64, kind: IdentifierKind) -> Result<u64, RepositoryError> {
    u64::try_from(value).map_err(|_| RepositoryError::IdentifierOutOfRange {
        kind,
        value: value.unsigned_abs(),
    })
}

fn decode_status(value: &str) -> Result<BatchStatus, RepositoryError> {
    match value {
        "STARTING" => Ok(BatchStatus::Starting),
        "STARTED" => Ok(BatchStatus::Started),
        "STOPPING" => Ok(BatchStatus::Stopping),
        "STOPPED" => Ok(BatchStatus::Stopped),
        "FAILED" => Ok(BatchStatus::Failed),
        "COMPLETED" => Ok(BatchStatus::Completed),
        "ABANDONED" => Ok(BatchStatus::Abandoned),
        "UNKNOWN" => Ok(BatchStatus::Unknown),
        _ => Err(RepositoryError::Unavailable),
    }
}
