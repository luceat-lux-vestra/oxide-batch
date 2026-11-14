use std::error::Error;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Map, Value, json};
use sqlx::migrate::Migrator;
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions, MySqlRow};
use sqlx::pool::PoolConnection;
use sqlx::{AssertSqlSafe, Connection, MySql, MySqlConnection, MySqlPool, Row};

use crate::{
    BatchStatus, BoxFuture, Clock, DefinitionIdentity, DefinitionUpgrade, ExecutionCounts,
    ExecutionMetadata, ExecutionTimestamps, ExecutionVersion, ExitCode, ExitStatus,
    FailureCategory, FailureId, FailureSummary, FlowDecision, FlowDecisionId, FlowDecisionRequest,
    FlowDecisionSequence, FlowStepState, FlowTarget, FlowTransitionKind, JobExecution,
    JobExecutionId, JobInstance, JobInstanceId, JobInstanceKey, JobInstanceSelection, JobName,
    JobParameter, JobParameters, JobRepository, LifecycleError, LifecycleTransition, NodeId,
    ParameterName, ParameterRole, ParameterValue, ParameterValueKind, RecoveryDecision,
    RecoveryRequest, RecoveryResult, RepositoryCapability, RepositoryDescriptor, RepositoryError,
    RepositoryUnitOfWork, StartLimit, StepExecution, StepExecutionId, StepName, TerminalKind,
};

const SUPPORTED_SCHEMA_VERSION: u32 = 2;
const MAX_POOL_SIZE: u32 = 1024;
const MAX_INSTANCE_KEY_INPUT: usize = 1024 * 1024;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations-mysql");

/// Redacted configuration for the `SQLx` `MySQL`-protocol repository adapter.
///
/// The connection string is retained only for connection establishment and is
/// never rendered by `Debug` or `Display` output.
#[derive(Clone)]
pub struct MysqlConfig {
    connection_string: String,
    pool_size: u32,
}

impl MysqlConfig {
    /// Builds the default configuration for a MySQL-protocol repository.
    ///
    /// # Errors
    ///
    /// Returns `MysqlConfigError` when the connection string is empty or invalid.
    pub fn new(connection_string: impl Into<String>) -> Result<Self, MysqlConfigError> {
        let config = Self {
            connection_string: connection_string.into(),
            pool_size: 10,
        };
        config.validate()?;
        Ok(config)
    }

    /// Sets the finite connection-pool ceiling.
    ///
    /// # Errors
    ///
    /// Rejects zero and values above 1024.
    pub fn with_pool_size(mut self, pool_size: u32) -> Result<Self, MysqlConfigError> {
        self.pool_size = pool_size;
        self.validate()?;
        Ok(self)
    }

    fn validate(&self) -> Result<(), MysqlConfigError> {
        if self.connection_string.is_empty() {
            return Err(MysqlConfigError::EmptyConnectionString);
        }
        if !(1..=MAX_POOL_SIZE).contains(&self.pool_size) {
            return Err(MysqlConfigError::PoolSize);
        }
        MySqlConnectOptions::from_str(&self.connection_string)
            .map_err(|_| MysqlConfigError::InvalidConnectionString)?;
        Ok(())
    }

    fn connect_options(&self) -> Result<MySqlConnectOptions, MysqlConfigError> {
        self.validate()?;
        MySqlConnectOptions::from_str(&self.connection_string)
            .map_err(|_| MysqlConfigError::InvalidConnectionString)
    }
}

impl fmt::Debug for MysqlConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MysqlConfig")
            .field("connection_string", &"<redacted>")
            .field("pool_size", &self.pool_size)
            .finish()
    }
}

/// Value-redacted MySQL-protocol adapter configuration error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MysqlConfigError {
    /// The connection string was empty.
    EmptyConnectionString,
    /// `SQLx` rejected the connection-string shape.
    InvalidConnectionString,
    /// Pool size was outside 1 through 1024.
    PoolSize,
}

impl fmt::Display for MysqlConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyConnectionString => formatter.write_str("MySQL connection string is empty"),
            Self::InvalidConnectionString => {
                formatter.write_str("MySQL connection string is invalid")
            }
            Self::PoolSize => formatter.write_str("MySQL pool size must be from 1 to 1024"),
        }
    }
}

impl Error for MysqlConfigError {}

/// Applies the immutable MySQL/MariaDB metadata migrations for this adapter.
#[derive(Clone, Copy, Debug, Default)]
pub struct MysqlMigrator;

impl MysqlMigrator {
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
        config: &MysqlConfig,
    ) -> Result<Option<u32>, RepositoryError> {
        let options = config
            .connect_options()
            .map_err(|_| RepositoryError::Unavailable)?;
        let mut connection = MySqlConnection::connect_with(&options)
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
    pub async fn migrate(config: &MysqlConfig) -> Result<(), RepositoryError> {
        let options = config
            .connect_options()
            .map_err(|_| RepositoryError::Unavailable)?;
        let mut connection = MySqlConnection::connect_with(&options)
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

/// Core durable `JobRepository` implementation for the `SQLx` `MySQL` protocol.
///
/// This concrete adapter sits below the database-neutral repository SPI. Its
/// initial descriptor deliberately declares no optional capabilities.
#[derive(Clone)]
pub struct MysqlJobRepository {
    pool: MySqlPool,
    clock: Arc<dyn Clock>,
    config: MysqlConfig,
}

impl MysqlJobRepository {
    /// Opens a pool and verifies the metadata schema version.
    ///
    /// # Errors
    ///
    /// Returns a redacted repository/configuration failure.
    pub async fn connect(
        config: MysqlConfig,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, RepositoryError> {
        let options = config
            .connect_options()
            .map_err(|_| RepositoryError::Unavailable)?;
        let pool = MySqlPoolOptions::new()
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

    async fn begin_connection(&self) -> Result<PoolConnection<MySql>, RepositoryError> {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        if sqlx::raw_sql("START TRANSACTION")
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

impl fmt::Debug for MysqlJobRepository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MysqlJobRepository")
            .field("pool_size", &self.pool.size())
            .field("pool_idle", &self.pool.num_idle())
            .finish_non_exhaustive()
    }
}

impl JobRepository for MysqlJobRepository {
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
            Ok(Box::new(MysqlUnitOfWork {
                repository: self,
                connection: Some(connection),
            }) as Box<dyn RepositoryUnitOfWork + 'a>)
        })
    }
}

struct MysqlUnitOfWork<'repository> {
    repository: &'repository MysqlJobRepository,
    connection: Option<PoolConnection<MySql>>,
}

impl MysqlUnitOfWork<'_> {
    fn connection(&mut self) -> Result<&mut PoolConnection<MySql>, RepositoryError> {
        self.connection.as_mut().ok_or(RepositoryError::Unavailable)
    }

    async fn ensure_core_definition(
        &mut self,
        job_name: &JobName,
        definition: &DefinitionIdentity,
    ) -> Result<u64, RepositoryError> {
        let manifest = std::str::from_utf8(definition.canonical_manifest())
            .map_err(|_| RepositoryError::Unavailable)?;
        let created_ms = system_time_millis(self.repository.clock.now())?;
        let inserted = sqlx::query(
            "INSERT INTO ob_core_job_definition              (job_name, definition_revision, manifest_format, manifest_digest, manifest, created_ms)              VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(job_name.as_str())
        .bind(definition.revision().as_str())
        .bind(i32::from(definition.manifest_format()))
        .bind(&definition.manifest_digest()[..])
        .bind(manifest)
        .bind(created_ms)
        .execute(&mut **self.connection()?)
        .await;

        match inserted {
            Ok(result) => Ok(result.last_insert_id()),
            Err(error) if is_duplicate_key(&error) => {
                let row = sqlx::query(
                    "SELECT id, manifest_digest FROM ob_core_job_definition                      WHERE job_name = ? AND definition_revision = ?",
                )
                .bind(job_name.as_str())
                .bind(definition.revision().as_str())
                .fetch_one(&mut **self.connection()?)
                .await
                .map_err(|_| RepositoryError::Unavailable)?;
                let digest = row
                    .try_get::<Vec<u8>, _>("manifest_digest")
                    .map_err(|_| RepositoryError::Unavailable)?;
                if digest.as_slice() != definition.manifest_digest() {
                    return Err(RepositoryError::DefinitionDrift {
                        job_name: job_name.clone(),
                        revision: definition.revision().clone(),
                    });
                }
                row.try_get::<u64, _>("id")
                    .map_err(|_| RepositoryError::Unavailable)
            }
            Err(_) => Err(RepositoryError::Unavailable),
        }
    }

    async fn job_execution(
        &mut self,
        id: JobExecutionId,
    ) -> Result<Option<JobExecution>, RepositoryError> {
        let row = sqlx::query(AssertSqlSafe(job_execution_select(
            "WHERE execution.id = ?",
        )))
        .bind(id.get())
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
        .bind(id.get())
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
            "WHERE execution.id = ? FOR UPDATE",
        )))
        .bind(id.get())
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
            "WHERE execution.id = ? FOR UPDATE",
        )))
        .bind(id.get())
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

impl RepositoryUnitOfWork for MysqlUnitOfWork<'_> {
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
                    let id = JobInstanceId::new(result.last_insert_id())?;
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
            let instance: Option<u64> =
                sqlx::query_scalar("SELECT id FROM ob_job_instance WHERE id = ? FOR UPDATE")
                    .bind(job_instance_id.get())
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
            .bind(job_instance_id.get())
            .fetch_optional(&mut **self.connection()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;

            let mut attempt = 1_u64;
            if let Some(row) = row {
                let latest = decode_job_execution(&row)?;
                attempt = row
                    .try_get::<u64, _>("attempt")
                    .map_err(|_| RepositoryError::Unavailable)?
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
            .bind(job_instance_id.get())
            .bind(attempt)
            .bind(created_ms)
            .bind(created_ms)
            .execute(&mut **self.connection()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            let id = JobExecutionId::new(result.last_insert_id())?;
            Ok(JobExecution::new(
                id,
                job_instance_id,
                starting_metadata(created_at)?,
            ))
        })
    }

    fn create_job_execution_with_definition<'a>(
        &'a mut self,
        job_instance_id: JobInstanceId,
        definition: &'a DefinitionIdentity,
    ) -> BoxFuture<'a, Result<JobExecution, RepositoryError>> {
        Box::pin(async move {
            let instance =
                sqlx::query("SELECT id, job_name FROM ob_job_instance WHERE id = ? FOR UPDATE")
                    .bind(job_instance_id.get())
                    .fetch_optional(&mut **self.connection()?)
                    .await
                    .map_err(|_| RepositoryError::Unavailable)?
                    .ok_or(RepositoryError::JobInstanceNotFound {
                        id: job_instance_id,
                    })?;
            let job_name = JobName::new(
                instance
                    .try_get::<String, _>("job_name")
                    .map_err(|_| RepositoryError::Unavailable)?,
            )?;
            if let Some(actual) = definition.job_name()
                && actual != &job_name
            {
                return Err(RepositoryError::DefinitionJobMismatch {
                    expected: job_name,
                    actual: actual.clone(),
                });
            }
            let definition_id = self.ensure_core_definition(&job_name, definition).await?;

            let row = sqlx::query(AssertSqlSafe(job_execution_select(
                "WHERE execution.job_instance_id = ? ORDER BY execution.attempt DESC LIMIT 1",
            )))
            .bind(job_instance_id.get())
            .fetch_optional(&mut **self.connection()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;

            let mut attempt = 1_u64;
            if let Some(row) = row {
                let latest = decode_job_execution(&row)?;
                attempt = row
                    .try_get::<u64, _>("attempt")
                    .map_err(|_| RepositoryError::Unavailable)?
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

                let previous_digest = sqlx::query_scalar::<_, Option<Vec<u8>>>(
                    "SELECT definition.manifest_digest                      FROM ob_job_execution execution                      LEFT JOIN ob_core_job_definition definition                        ON definition.id = execution.definition_id                      WHERE execution.id = ?",
                )
                .bind(latest.id().get())
                .fetch_one(&mut **self.connection()?)
                .await
                .map_err(|_| RepositoryError::Unavailable)?
                .ok_or(RepositoryError::IncompatibleDefinition {
                    instance_id: job_instance_id,
                })?;
                if previous_digest.as_slice() != definition.manifest_digest() {
                    return Err(RepositoryError::IncompatibleDefinition {
                        instance_id: job_instance_id,
                    });
                }
            }

            let created_at = self.repository.clock.now();
            let created_ms = system_time_millis(created_at)?;
            let result = sqlx::query(
                "INSERT INTO ob_job_execution                  (job_instance_id, definition_id, attempt, status, exit_code,                   created_ms, updated_ms, version)                  VALUES (?, ?, ?, 'STARTING', 'UNKNOWN', ?, ?, 0)",
            )
            .bind(job_instance_id.get())
            .bind(definition_id)
            .bind(attempt)
            .bind(created_ms)
            .bind(created_ms)
            .execute(&mut **self.connection()?)
            .await
            .map_err(|error| {
                if is_duplicate_key(&error) {
                    RepositoryError::ConcurrentModification
                } else {
                    RepositoryError::Unavailable
                }
            })?;
            let id = JobExecutionId::new(result.last_insert_id())?;
            Ok(JobExecution::new(
                id,
                job_instance_id,
                starting_metadata(created_at)?,
            ))
        })
    }

    fn create_step_execution<'a>(
        &'a mut self,
        job_execution_id: JobExecutionId,
        step_name: &'a StepName,
    ) -> BoxFuture<'a, Result<StepExecution, RepositoryError>> {
        Box::pin(async move {
            let node_id =
                NodeId::new(step_name.as_str()).map_err(|_| RepositoryError::FlowStateCorrupt)?;
            self.create_flow_step_execution(
                job_execution_id,
                step_name,
                &node_id,
                StartLimit::UNRESTRICTED,
            )
            .await
        })
    }

    fn create_flow_step_execution<'a>(
        &'a mut self,
        job_execution_id: JobExecutionId,
        step_name: &'a StepName,
        node_id: &'a NodeId,
        start_limit: StartLimit,
    ) -> BoxFuture<'a, Result<StepExecution, RepositoryError>> {
        Box::pin(async move {
            let row = sqlx::query("SELECT job_instance_id FROM ob_job_execution WHERE id = ?")
                .bind(job_execution_id.get())
                .fetch_optional(&mut **self.connection()?)
                .await
                .map_err(|_| RepositoryError::Unavailable)?
                .ok_or(RepositoryError::JobExecutionNotFound {
                    id: job_execution_id,
                })?;
            let instance_id = row
                .try_get::<u64, _>("job_instance_id")
                .map_err(|_| RepositoryError::Unavailable)?;

            sqlx::query_scalar::<_, u64>("SELECT id FROM ob_job_instance WHERE id = ? FOR UPDATE")
                .bind(instance_id)
                .fetch_one(&mut **self.connection()?)
                .await
                .map_err(|_| RepositoryError::Unavailable)?;

            let starts = sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM ob_step_execution step                  JOIN ob_job_execution job ON job.id = step.job_execution_id                  WHERE job.job_instance_id = ? AND step.step_logical_id = ?",
            )
            .bind(instance_id)
            .bind(node_id.as_str())
            .fetch_one(&mut **self.connection()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            let starts = u64::try_from(starts).map_err(|_| RepositoryError::FlowStateCorrupt)?;
            if starts >= u64::from(start_limit.get()) {
                return Err(RepositoryError::StartLimitExceeded {
                    instance_id: JobInstanceId::new(instance_id)?,
                    node_id: node_id.clone(),
                    limit: start_limit,
                });
            }

            let created_at = self.repository.clock.now();
            let created_ms = system_time_millis(created_at)?;
            let result = sqlx::query(
                "INSERT INTO ob_step_execution                  (job_execution_id, step_name, step_logical_id, status, exit_code,                   created_ms, updated_ms, version)                  VALUES (?, ?, ?, 'STARTING', 'UNKNOWN', ?, ?, 0)",
            )
            .bind(job_execution_id.get())
            .bind(step_name.as_str())
            .bind(node_id.as_str())
            .bind(created_ms)
            .bind(created_ms)
            .execute(&mut **self.connection()?)
            .await
            .map_err(|error| {
                if is_duplicate_key(&error) {
                    RepositoryError::ConcurrentModification
                } else {
                    RepositoryError::Unavailable
                }
            })?;
            let id = StepExecutionId::new(result.last_insert_id())?;
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
            .bind(id.get())
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
            let instance: Option<u64> =
                sqlx::query_scalar("SELECT id FROM ob_job_instance WHERE id = ?")
                    .bind(job_instance_id.get())
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
            .bind(job_instance_id.get())
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
            let job: Option<u64> =
                sqlx::query_scalar("SELECT id FROM ob_job_execution WHERE id = ?")
                    .bind(job_execution_id.get())
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
            .bind(job_execution_id.get())
            .fetch_all(&mut **self.connection()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            rows.iter().map(decode_step_execution).collect()
        })
    }

    fn latest_flow_step<'a>(
        &'a mut self,
        job_instance_id: JobInstanceId,
        node_id: &'a NodeId,
    ) -> BoxFuture<'a, Result<Option<FlowStepState>, RepositoryError>> {
        Box::pin(async move {
            let exists =
                sqlx::query_scalar::<_, u64>("SELECT id FROM ob_job_instance WHERE id = ?")
                    .bind(job_instance_id.get())
                    .fetch_optional(&mut **self.connection()?)
                    .await
                    .map_err(|_| RepositoryError::Unavailable)?;
            if exists.is_none() {
                return Err(RepositoryError::JobInstanceNotFound {
                    id: job_instance_id,
                });
            }
            let row = sqlx::query(AssertSqlSafe(step_execution_select(
                "JOIN ob_job_execution job ON job.id = execution.job_execution_id                  WHERE job.job_instance_id = ? AND execution.step_logical_id = ?                  ORDER BY job.attempt DESC, execution.id DESC LIMIT 1",
            )))
            .bind(job_instance_id.get())
            .bind(node_id.as_str())
            .fetch_optional(&mut **self.connection()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            row.map(|row| {
                Ok(FlowStepState::new(
                    node_id.clone(),
                    decode_step_execution(&row)?,
                    None,
                ))
            })
            .transpose()
        })
    }

    fn append_flow_decision<'a>(
        &'a mut self,
        request: &'a FlowDecisionRequest,
    ) -> BoxFuture<'a, Result<FlowDecision, RepositoryError>> {
        Box::pin(async move {
            let row = sqlx::query(
                "SELECT definition.manifest_digest, definition.manifest                  FROM ob_job_execution execution                  JOIN ob_core_job_definition definition ON definition.id = execution.definition_id                  WHERE execution.id = ?",
            )
            .bind(request.job_execution_id().get())
            .fetch_optional(&mut **self.connection()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?
            .ok_or(RepositoryError::JobExecutionNotFound {
                id: request.job_execution_id(),
            })?;
            let fingerprint = row
                .try_get::<Option<Vec<u8>>, _>("manifest_digest")
                .map_err(|_| RepositoryError::FlowStateCorrupt)?
                .ok_or(RepositoryError::FlowStateCorrupt)?;
            if fingerprint.as_slice() != request.plan_fingerprint() {
                return Err(RepositoryError::FlowStateCorrupt);
            }
            let manifest_text = row
                .try_get::<Option<String>, _>("manifest")
                .map_err(|_| RepositoryError::FlowStateCorrupt)?
                .ok_or(RepositoryError::FlowStateCorrupt)?;
            let manifest: Value = serde_json::from_str(&manifest_text)
                .map_err(|_| RepositoryError::FlowStateCorrupt)?;
            if !crate::flow::decision_matches_manifest(&manifest, request) {
                return Err(RepositoryError::FlowStateCorrupt);
            }

            let existing = sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM ob_flow_decision WHERE job_execution_id = ?",
            )
            .bind(request.job_execution_id().get())
            .fetch_one(&mut **self.connection()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            let existing =
                u64::try_from(existing).map_err(|_| RepositoryError::FlowStateCorrupt)?;
            let advanced = manifest
                .get("format")
                .and_then(Value::as_u64)
                .is_some_and(|format| {
                    format == u64::from(oxide_batch_core::MANIFEST_FORMAT_ADVANCED_FLOW)
                });
            if !advanced
                && request.sequence().get()
                    != existing
                        .checked_add(1)
                        .ok_or(RepositoryError::FlowStateCorrupt)?
            {
                return Err(RepositoryError::ConcurrentModification);
            }

            if let Some(step_id) = request.source_step_execution_id() {
                let valid = sqlx::query_scalar::<_, i64>(
                    "SELECT COUNT(*) FROM ob_step_execution source                      JOIN ob_job_execution source_job ON source_job.id = source.job_execution_id                      JOIN ob_job_execution target_job ON target_job.id = ?                      WHERE source.id = ?                        AND source_job.job_instance_id = target_job.job_instance_id                        AND source.step_logical_id = ?",
                )
                .bind(request.job_execution_id().get())
                .bind(step_id.get())
                .bind(request.source_node_id().as_str())
                .fetch_one(&mut **self.connection()?)
                .await
                .map_err(|_| RepositoryError::Unavailable)?;
                if valid != 1 {
                    return Err(RepositoryError::FlowStateCorrupt);
                }
            } else if !matches!(
                request.kind(),
                FlowTransitionKind::Decider
                    | FlowTransitionKind::SplitAggregate
                    | FlowTransitionKind::NestedJobExit
            ) {
                return Err(RepositoryError::FlowStateCorrupt);
            }

            if let Some(reused_id) = request.reused_decision_id() {
                let valid = sqlx::query_scalar::<_, i64>(
                    "SELECT COUNT(*) FROM ob_flow_decision prior                      JOIN ob_job_execution prior_job ON prior_job.id = prior.job_execution_id                      JOIN ob_job_execution target_job ON target_job.id = ?                      WHERE prior.id = ?                        AND prior_job.job_instance_id = target_job.job_instance_id                        AND prior.source_node_id = ?                        AND prior.plan_fingerprint = ?                        AND prior.input_digest = ?                        AND prior.observed_outcome = ?                        AND prior.target_node_id <=> ?                        AND prior.terminal_kind <=> ?",
                )
                .bind(request.job_execution_id().get())
                .bind(reused_id.get())
                .bind(request.source_node_id().as_str())
                .bind(&request.plan_fingerprint()[..])
                .bind(&request.input_digest()[..])
                .bind(request.observed_outcome().as_str())
                .bind(flow_target_node(request.target()))
                .bind(flow_terminal_code(request.target())?)
                .fetch_one(&mut **self.connection()?)
                .await
                .map_err(|_| RepositoryError::Unavailable)?;
                if valid != 1 {
                    return Err(RepositoryError::FlowStateCorrupt);
                }
            }

            let result = sqlx::query(
                "INSERT INTO ob_flow_decision                  (job_execution_id, source_step_execution_id, reused_decision_id, sequence,                   source_node_id, observed_outcome, target_node_id, transition_kind,                   terminal_kind, plan_fingerprint, input_digest, decided_ms)                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(request.job_execution_id().get())
            .bind(request.source_step_execution_id().map(StepExecutionId::get))
            .bind(request.reused_decision_id().map(FlowDecisionId::get))
            .bind(request.sequence().get())
            .bind(request.source_node_id().as_str())
            .bind(request.observed_outcome().as_str())
            .bind(flow_target_node(request.target()))
            .bind(request.kind().durable_code())
            .bind(flow_terminal_code(request.target())?)
            .bind(&request.plan_fingerprint()[..])
            .bind(&request.input_digest()[..])
            .bind(system_time_millis(request.decided_at())?)
            .execute(&mut **self.connection()?)
            .await
            .map_err(|error| {
                if is_duplicate_key(&error) {
                    RepositoryError::ConcurrentModification
                } else {
                    RepositoryError::Unavailable
                }
            })?;
            let id = FlowDecisionId::new(result.last_insert_id())?;
            Ok(request.materialize(id))
        })
    }

    fn find_reusable_flow_decision<'a>(
        &'a mut self,
        job_instance_id: JobInstanceId,
        node_id: &'a NodeId,
        plan_fingerprint: &'a [u8; 32],
        input_digest: &'a [u8; 32],
        kind: FlowTransitionKind,
    ) -> BoxFuture<'a, Result<Option<FlowDecision>, RepositoryError>> {
        Box::pin(async move {
            let exists =
                sqlx::query_scalar::<_, u64>("SELECT id FROM ob_job_instance WHERE id = ?")
                    .bind(job_instance_id.get())
                    .fetch_optional(&mut **self.connection()?)
                    .await
                    .map_err(|_| RepositoryError::Unavailable)?;
            if exists.is_none() {
                return Err(RepositoryError::JobInstanceNotFound {
                    id: job_instance_id,
                });
            }
            let row = sqlx::query(AssertSqlSafe(flow_decision_select(
                "JOIN ob_job_execution flow_job ON flow_job.id = decision.job_execution_id                  WHERE flow_job.job_instance_id = ? AND decision.source_node_id = ?                    AND decision.plan_fingerprint = ? AND decision.input_digest = ?                    AND decision.transition_kind = ?                  ORDER BY flow_job.attempt DESC, decision.sequence DESC LIMIT 1",
            )))
            .bind(job_instance_id.get())
            .bind(node_id.as_str())
            .bind(&plan_fingerprint[..])
            .bind(&input_digest[..])
            .bind(kind.durable_code())
            .fetch_optional(&mut **self.connection()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            row.map(|row| decode_flow_decision(&row)).transpose()
        })
    }

    fn flow_decisions(
        &mut self,
        job_execution_id: JobExecutionId,
    ) -> BoxFuture<'_, Result<Vec<FlowDecision>, RepositoryError>> {
        Box::pin(async move {
            let exists =
                sqlx::query_scalar::<_, u64>("SELECT id FROM ob_job_execution WHERE id = ?")
                    .bind(job_execution_id.get())
                    .fetch_optional(&mut **self.connection()?)
                    .await
                    .map_err(|_| RepositoryError::Unavailable)?;
            if exists.is_none() {
                return Err(RepositoryError::JobExecutionNotFound {
                    id: job_execution_id,
                });
            }
            let rows = sqlx::query(AssertSqlSafe(flow_decision_select(
                "WHERE decision.job_execution_id = ? ORDER BY decision.sequence",
            )))
            .bind(job_execution_id.get())
            .fetch_all(&mut **self.connection()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            rows.iter().map(decode_flow_decision).collect()
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

impl Drop for MysqlUnitOfWork<'_> {
    fn drop(&mut self) {
        if let Some(connection) = &mut self.connection {
            connection.close_on_drop();
        }
    }
}

fn flow_decision_select(suffix: &str) -> String {
    format!(
        "SELECT decision.id, decision.job_execution_id,          decision.source_step_execution_id, decision.reused_decision_id,          decision.sequence, decision.source_node_id, decision.observed_outcome,          decision.target_node_id, decision.transition_kind, decision.terminal_kind,          decision.plan_fingerprint, decision.input_digest, decision.decided_ms          FROM ob_flow_decision decision {suffix}"
    )
}

fn flow_target_node(target: &FlowTarget) -> Option<&str> {
    match target {
        FlowTarget::Node(node) => Some(node.as_str()),
        FlowTarget::Terminal(_) => None,
    }
}

fn flow_terminal_code(target: &FlowTarget) -> Result<Option<&'static str>, RepositoryError> {
    Ok(match target {
        FlowTarget::Node(_) => None,
        FlowTarget::Terminal(TerminalKind::Complete) => Some("COMPLETE"),
        FlowTarget::Terminal(TerminalKind::Fail) => Some("FAIL"),
        FlowTarget::Terminal(TerminalKind::Stop) => Some("STOP"),
        FlowTarget::Terminal(_) => return Err(RepositoryError::Unavailable),
    })
}

fn decode_flow_decision(row: &MySqlRow) -> Result<FlowDecision, RepositoryError> {
    let id = FlowDecisionId::new(read_u64(row, "id")?)?;
    let job_execution_id = JobExecutionId::new(read_u64(row, "job_execution_id")?)?;
    let sequence = FlowDecisionSequence::new(read_u64(row, "sequence")?)
        .map_err(|_| RepositoryError::FlowStateCorrupt)?;
    let source_node_id = NodeId::new(
        row.try_get::<String, _>("source_node_id")
            .map_err(|_| RepositoryError::FlowStateCorrupt)?,
    )
    .map_err(|_| RepositoryError::FlowStateCorrupt)?;
    let source_step_execution_id = row
        .try_get::<Option<u64>, _>("source_step_execution_id")
        .map_err(|_| RepositoryError::FlowStateCorrupt)?
        .map(StepExecutionId::new)
        .transpose()?;
    let reused_decision_id = row
        .try_get::<Option<u64>, _>("reused_decision_id")
        .map_err(|_| RepositoryError::FlowStateCorrupt)?
        .map(FlowDecisionId::new)
        .transpose()?;
    let kind = FlowTransitionKind::from_durable_code(
        &row.try_get::<String, _>("transition_kind")
            .map_err(|_| RepositoryError::FlowStateCorrupt)?,
    )
    .ok_or(RepositoryError::FlowStateCorrupt)?;
    let observed_outcome = ExitCode::new(
        row.try_get::<String, _>("observed_outcome")
            .map_err(|_| RepositoryError::FlowStateCorrupt)?,
    )?;
    let target_node = row
        .try_get::<Option<String>, _>("target_node_id")
        .map_err(|_| RepositoryError::FlowStateCorrupt)?;
    let terminal = row
        .try_get::<Option<String>, _>("terminal_kind")
        .map_err(|_| RepositoryError::FlowStateCorrupt)?;
    let target = match (target_node, terminal.as_deref()) {
        (Some(node), None) => {
            FlowTarget::Node(NodeId::new(node).map_err(|_| RepositoryError::FlowStateCorrupt)?)
        }
        (None, Some("COMPLETE")) => FlowTarget::Terminal(TerminalKind::Complete),
        (None, Some("FAIL")) => FlowTarget::Terminal(TerminalKind::Fail),
        (None, Some("STOP")) => FlowTarget::Terminal(TerminalKind::Stop),
        _ => return Err(RepositoryError::FlowStateCorrupt),
    };
    let fingerprint: [u8; 32] = row
        .try_get::<Vec<u8>, _>("plan_fingerprint")
        .map_err(|_| RepositoryError::FlowStateCorrupt)?
        .try_into()
        .map_err(|_| RepositoryError::FlowStateCorrupt)?;
    let input_digest: [u8; 32] = row
        .try_get::<Vec<u8>, _>("input_digest")
        .map_err(|_| RepositoryError::FlowStateCorrupt)?
        .try_into()
        .map_err(|_| RepositoryError::FlowStateCorrupt)?;
    Ok(FlowDecision::new(
        id,
        job_execution_id,
        sequence,
        source_node_id,
        source_step_execution_id,
        kind,
        observed_outcome,
        target,
        fingerprint,
        input_digest,
        reused_decision_id,
        millis_system_time(read_i64(row, "decided_ms")?)?,
    ))
}

fn job_execution_select(suffix: &str) -> String {
    format!(
        "SELECT execution.id, execution.job_instance_id, execution.attempt, \
         execution.status, execution.exit_code, \
         CAST(0 AS UNSIGNED) AS read_count, CAST(0 AS UNSIGNED) AS processed_count, \
         CAST(0 AS UNSIGNED) AS write_count, CAST(0 AS UNSIGNED) AS filter_count, \
         CAST(0 AS UNSIGNED) AS commit_count, CAST(0 AS UNSIGNED) AS rollback_count, \
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
    connection: &mut MySqlConnection,
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
    connection: &mut MySqlConnection,
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
    .bind(failure.map(|value| value.failure_id().get()))
    .bind(
        timestamps
            .started_at()
            .map(system_time_millis)
            .transpose()?,
    )
    .bind(timestamps.ended_at().map(system_time_millis).transpose()?)
    .bind(counts.read())
    .bind(counts.processed())
    .bind(counts.written())
    .bind(counts.filtered())
    .bind(counts.committed())
    .bind(counts.rolled_back())
    .bind(system_time_millis(updated_at)?)
    .bind(execution.version().get())
    .bind(execution.id().get())
    .bind(expected.get())
    .execute(connection)
    .await
    .map_err(|_| RepositoryError::Unavailable)?;
    Ok(result.rows_affected())
}

async fn update_execution(
    connection: &mut MySqlConnection,
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
        .bind(failure.map(|value| value.failure_id().get()))
        .bind(
            timestamps
                .started_at()
                .map(system_time_millis)
                .transpose()?,
        )
        .bind(timestamps.ended_at().map(system_time_millis).transpose()?)
        .bind(system_time_millis(updated_at)?)
        .bind(new_version.get())
        .bind(id)
        .bind(expected.get())
        .execute(connection)
        .await
        .map_err(|_| RepositoryError::Unavailable)?;
    Ok(result.rows_affected())
}

fn decode_job_instance(row: &MySqlRow) -> Result<JobInstance, RepositoryError> {
    let id = JobInstanceId::new(
        row.try_get::<u64, _>("id")
            .map_err(|_| RepositoryError::Unavailable)?,
    )?;
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

fn decode_job_execution(row: &MySqlRow) -> Result<JobExecution, RepositoryError> {
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

fn decode_step_execution(row: &MySqlRow) -> Result<StepExecution, RepositoryError> {
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

fn decode_execution_metadata(row: &MySqlRow) -> Result<ExecutionMetadata, RepositoryError> {
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
    let failure_id = row
        .try_get::<Option<u64>, _>("failure_id")
        .map_err(|_| RepositoryError::Unavailable)?;
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
    E: sqlx::Executor<'executor, Database = MySql>,
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
        .is_some_and(|code| code == "42S02" || code == "1146")
}

fn is_duplicate_key(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::code)
        .as_deref()
        .is_some_and(|code| code == "23000" || code == "1062")
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

fn read_i64(row: &MySqlRow, name: &str) -> Result<i64, RepositoryError> {
    row.try_get(name).map_err(|_| RepositoryError::Unavailable)
}

fn read_optional_i64(row: &MySqlRow, name: &str) -> Result<Option<i64>, RepositoryError> {
    row.try_get(name).map_err(|_| RepositoryError::Unavailable)
}

fn read_u64(row: &MySqlRow, name: &str) -> Result<u64, RepositoryError> {
    row.try_get(name).map_err(|_| RepositoryError::Unavailable)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_version_guard_requires_runnable_core_schema() {
        assert!(matches!(
            verify_schema_version(1),
            Err(RepositoryError::MigrationRequired {
                current: 1,
                supported: 2
            })
        ));
        assert!(verify_schema_version(2).is_ok());
        assert!(matches!(
            verify_schema_version(3),
            Err(RepositoryError::NewerSchema {
                current: 3,
                supported: 2
            })
        ));
    }
}
