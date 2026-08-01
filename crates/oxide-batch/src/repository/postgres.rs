use std::error::Error;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use sqlx::migrate::Migrator;
use sqlx::pool::PoolConnection;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgRow, PgSslMode};
use sqlx::types::Json;
use sqlx::{AssertSqlSafe, Connection, PgConnection, PgPool, Postgres, Row};

use super::{
    BoxFuture, Clock, JobInstanceSelection, JobRepository, RecoveryDecision, RecoveryRequest,
    RecoveryResult, RepositoryError, RepositoryUnitOfWork, recovered_execution,
};
use crate::{
    BatchStatus, BusinessStatement, BusinessTransaction, BusinessTransactionError,
    BusinessValueKind, BusinessWriteResult, Checkpoint, ChunkCommitReceipt, ChunkCounts,
    ChunkFaultProgress, ChunkTransaction, ChunkTransactionContext, ChunkTransactionError,
    ChunkTransactionManager, ClassifierRevision, DefinitionIdentity, DefinitionUpgrade,
    ExecutionContext, ExecutionCounts, ExecutionMetadata, ExecutionTimestamps, ExecutionVersion,
    ExitCode, ExitStatus, FailureCategory, FailureId, FailureSummary, FaultPhase, FaultPolicy,
    FaultProgress, FaultStateEntry, FaultStateEnvelope, FaultStateError, FaultStateFormatError,
    FaultStateStore, FlowDecision, FlowDecisionId, FlowDecisionRequest, FlowDecisionSequence,
    FlowStepState, FlowTarget, FlowTransitionKind, IdentifierKind, InheritedStepProgress,
    JobExecution, JobExecutionId, JobInstance, JobInstanceId, JobInstanceKey, JobName,
    JobParameter, JobParameters, LifecycleError, LifecycleTransition, NodeId, ParameterName,
    ParameterRole, ParameterValue, ParameterValueKind, RetryCounts, RetryKey, RetryLimit,
    RetryOrdinal, RetryReservation, RetryStateLimit, SkipCounts, StartLimit, StateLimits,
    StepExecution, StepExecutionId, StepName, TerminalKind,
};

const SUPPORTED_SCHEMA_VERSION: u32 = 2;
const MAX_POOL_SIZE: u32 = 1024;
const MAX_SHORT_TIMEOUT: Duration = Duration::from_mins(5);
const MAX_STATEMENT_TIMEOUT: Duration = Duration::from_hours(24);
const MAX_CONNECTION_LIFETIME: Duration = Duration::from_hours(7 * 24);
const MAX_INSTANCE_KEY_INPUT: usize = 1024 * 1024;
const MAX_CA_CERTIFICATE_BYTES: usize = 1024 * 1024;
const DEFAULT_CONTEXT_SCHEMA: &str = "oxide_batch.empty.v1";

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

/// Transport security for a `PostgreSQL` repository connection.
#[derive(Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum TlsMode {
    /// Validate the server certificate and hostname.
    VerifyFull {
        /// An optional bounded PEM CA bundle. System roots are used when absent.
        ca_certificate: Option<CaCertificate>,
    },
    /// Use an unencrypted connection for an explicitly isolated environment.
    Plaintext,
}

/// A bounded, value-redacted PEM certificate-authority bundle.
#[derive(Clone, Eq, PartialEq)]
pub struct CaCertificate(Vec<u8>);

impl CaCertificate {
    /// Validates an in-memory PEM CA bundle.
    ///
    /// # Errors
    ///
    /// Rejects an empty bundle or one larger than 1 MiB. Certificate parsing
    /// and trust validation occur when the TLS connection is established.
    pub fn new(pem: impl Into<Vec<u8>>) -> Result<Self, PostgresConfigError> {
        let pem = pem.into();
        if pem.is_empty() {
            return Err(PostgresConfigError::EmptyCaCertificate);
        }
        if pem.len() > MAX_CA_CERTIFICATE_BYTES {
            return Err(PostgresConfigError::CaCertificateTooLarge {
                max_bytes: MAX_CA_CERTIFICATE_BYTES,
            });
        }
        Ok(Self(pem))
    }

    fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for CaCertificate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CaCertificate")
            .field("byte_length", &self.0.len())
            .field("contents", &"<redacted>")
            .finish()
    }
}

impl Default for TlsMode {
    fn default() -> Self {
        Self::VerifyFull {
            ca_certificate: None,
        }
    }
}

impl fmt::Debug for TlsMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::VerifyFull { ca_certificate } => formatter
                .debug_struct("VerifyFull")
                .field(
                    "ca_certificate",
                    &ca_certificate.as_ref().map(|_| "<redacted>"),
                )
                .finish(),
            Self::Plaintext => formatter.write_str("Plaintext"),
        }
    }
}

/// Facade-owned `PostgreSQL` pool, TLS, and timeout configuration.
///
/// `Debug` deliberately omits the connection string and certificate path.
#[derive(Clone)]
pub struct PostgresConfig {
    connection_string: String,
    tls_mode: TlsMode,
    pool_size: u32,
    acquire_timeout: Duration,
    connect_timeout: Duration,
    statement_timeout: Duration,
    lock_timeout: Duration,
    idle_transaction_timeout: Duration,
    connection_idle_timeout: Duration,
    connection_max_lifetime: Duration,
    pool_close_timeout: Duration,
}

impl PostgresConfig {
    /// Builds production-default configuration for a `PostgreSQL` connection.
    ///
    /// The connection string is treated as a secret and is never included in
    /// facade diagnostics.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresConfigError`] when the connection string is empty.
    pub fn new(connection_string: impl Into<String>) -> Result<Self, PostgresConfigError> {
        let config = Self {
            connection_string: connection_string.into(),
            tls_mode: TlsMode::default(),
            pool_size: 10,
            acquire_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            statement_timeout: Duration::from_secs(30),
            lock_timeout: Duration::from_secs(5),
            idle_transaction_timeout: Duration::from_mins(1),
            connection_idle_timeout: Duration::from_mins(10),
            connection_max_lifetime: Duration::from_mins(30),
            pool_close_timeout: Duration::from_secs(30),
        };
        config.validate()?;
        Ok(config)
    }

    /// Selects certificate validation or explicit plaintext transport.
    #[must_use]
    pub fn with_tls_mode(mut self, tls_mode: TlsMode) -> Self {
        self.tls_mode = tls_mode;
        self
    }

    /// Sets the maximum number of connections.
    ///
    /// # Errors
    ///
    /// Rejects zero and values above 1024.
    pub fn with_pool_size(mut self, value: u32) -> Result<Self, PostgresConfigError> {
        self.pool_size = value;
        self.validate()?;
        Ok(self)
    }

    /// Sets connection acquisition timeout.
    ///
    /// # Errors
    ///
    /// Rejects values outside 1 ms through 5 minutes or above close timeout.
    pub fn with_acquire_timeout(mut self, value: Duration) -> Result<Self, PostgresConfigError> {
        self.acquire_timeout = value;
        self.validate()?;
        Ok(self)
    }

    /// Sets TCP, TLS, authentication establishment timeout.
    ///
    /// # Errors
    ///
    /// Rejects values outside 1 ms through 5 minutes.
    pub fn with_connect_timeout(mut self, value: Duration) -> Result<Self, PostgresConfigError> {
        self.connect_timeout = value;
        self.validate()?;
        Ok(self)
    }

    /// Sets the ordinary server-side statement timeout.
    ///
    /// # Errors
    ///
    /// Rejects values outside 1 ms through 24 hours or below lock timeout.
    pub fn with_statement_timeout(mut self, value: Duration) -> Result<Self, PostgresConfigError> {
        self.statement_timeout = value;
        self.validate()?;
        Ok(self)
    }

    /// Sets the server-side lock timeout.
    ///
    /// # Errors
    ///
    /// Rejects values outside 1 ms through 5 minutes or above statement
    /// timeout.
    pub fn with_lock_timeout(mut self, value: Duration) -> Result<Self, PostgresConfigError> {
        self.lock_timeout = value;
        self.validate()?;
        Ok(self)
    }

    /// Sets protection against idle open transactions.
    ///
    /// # Errors
    ///
    /// Rejects values outside 1 second through 24 hours.
    pub fn with_idle_transaction_timeout(
        mut self,
        value: Duration,
    ) -> Result<Self, PostgresConfigError> {
        self.idle_transaction_timeout = value;
        self.validate()?;
        Ok(self)
    }

    /// Sets idle pooled-connection retirement.
    ///
    /// # Errors
    ///
    /// Rejects values outside 1 second through 24 hours.
    pub fn with_connection_idle_timeout(
        mut self,
        value: Duration,
    ) -> Result<Self, PostgresConfigError> {
        self.connection_idle_timeout = value;
        self.validate()?;
        Ok(self)
    }

    /// Sets the maximum physical connection lifetime.
    ///
    /// # Errors
    ///
    /// Rejects values outside 1 minute through 7 days.
    pub fn with_connection_max_lifetime(
        mut self,
        value: Duration,
    ) -> Result<Self, PostgresConfigError> {
        self.connection_max_lifetime = value;
        self.validate()?;
        Ok(self)
    }

    /// Sets cooperative pool shutdown timeout.
    ///
    /// # Errors
    ///
    /// Rejects values outside 1 ms through 5 minutes or below acquire timeout.
    pub fn with_pool_close_timeout(mut self, value: Duration) -> Result<Self, PostgresConfigError> {
        self.pool_close_timeout = value;
        self.validate()?;
        Ok(self)
    }

    fn validate(&self) -> Result<(), PostgresConfigError> {
        if self.connection_string.is_empty() {
            return Err(PostgresConfigError::EmptyConnectionString);
        }
        validate_connection_query(&self.connection_string)?;
        if !(1..=MAX_POOL_SIZE).contains(&self.pool_size) {
            return Err(PostgresConfigError::PoolSize);
        }
        validate_duration(
            self.acquire_timeout,
            Duration::from_millis(1),
            MAX_SHORT_TIMEOUT,
            "acquire",
        )?;
        validate_duration(
            self.connect_timeout,
            Duration::from_millis(1),
            MAX_SHORT_TIMEOUT,
            "connect",
        )?;
        validate_duration(
            self.statement_timeout,
            Duration::from_millis(1),
            MAX_STATEMENT_TIMEOUT,
            "statement",
        )?;
        validate_duration(
            self.lock_timeout,
            Duration::from_millis(1),
            MAX_SHORT_TIMEOUT,
            "lock",
        )?;
        validate_duration(
            self.idle_transaction_timeout,
            Duration::from_secs(1),
            MAX_STATEMENT_TIMEOUT,
            "idle transaction",
        )?;
        validate_duration(
            self.connection_idle_timeout,
            Duration::from_secs(1),
            MAX_STATEMENT_TIMEOUT,
            "connection idle",
        )?;
        validate_duration(
            self.connection_max_lifetime,
            Duration::from_mins(1),
            MAX_CONNECTION_LIFETIME,
            "connection lifetime",
        )?;
        validate_duration(
            self.pool_close_timeout,
            Duration::from_millis(1),
            MAX_SHORT_TIMEOUT,
            "pool close",
        )?;
        if self.lock_timeout > self.statement_timeout {
            return Err(PostgresConfigError::LockExceedsStatement);
        }
        if self.acquire_timeout > self.pool_close_timeout {
            return Err(PostgresConfigError::AcquireExceedsClose);
        }
        Ok(())
    }

    fn connect_options(&self) -> Result<PgConnectOptions, PostgresConfigError> {
        let mut options = PgConnectOptions::from_str(&self.connection_string)
            .map_err(|_| PostgresConfigError::InvalidConnectionString)?;
        options = options.application_name("oxide-batch");
        options = match &self.tls_mode {
            TlsMode::VerifyFull { ca_certificate } => {
                let options = options.ssl_mode(PgSslMode::VerifyFull);
                if let Some(certificate) = ca_certificate {
                    options.ssl_root_cert_from_pem(certificate.as_bytes().to_vec())
                } else {
                    options
                }
            }
            TlsMode::Plaintext => options.ssl_mode(PgSslMode::Disable),
        };
        Ok(options)
    }
}

impl fmt::Debug for PostgresConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresConfig")
            .field("connection", &"<redacted>")
            .field(
                "tls_mode",
                &match self.tls_mode {
                    TlsMode::VerifyFull { .. } => "verify-full",
                    TlsMode::Plaintext => "plaintext",
                },
            )
            .field("pool_size", &self.pool_size)
            .field("acquire_timeout", &self.acquire_timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("statement_timeout", &self.statement_timeout)
            .field("lock_timeout", &self.lock_timeout)
            .field("idle_transaction_timeout", &self.idle_transaction_timeout)
            .field("connection_idle_timeout", &self.connection_idle_timeout)
            .field("connection_max_lifetime", &self.connection_max_lifetime)
            .field("pool_close_timeout", &self.pool_close_timeout)
            .finish_non_exhaustive()
    }
}

/// Invalid facade-owned `PostgreSQL` configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PostgresConfigError {
    /// The redacted connection string was empty.
    EmptyConnectionString,
    /// The redacted connection string was not a `PostgreSQL` connection string.
    InvalidConnectionString,
    /// Pool size was outside 1 through 1024.
    PoolSize,
    /// TLS material or mode was embedded in the connection string.
    TlsOptionInConnectionString,
    /// An explicitly supplied CA bundle was empty.
    EmptyCaCertificate,
    /// An explicitly supplied CA bundle exceeded its hard limit.
    CaCertificateTooLarge {
        /// Maximum accepted PEM bytes.
        max_bytes: usize,
    },
    /// A timeout was outside its documented finite bounds.
    Timeout {
        /// The safe timeout class.
        class: &'static str,
    },
    /// Lock timeout exceeded ordinary statement timeout.
    LockExceedsStatement,
    /// Acquire timeout exceeded cooperative pool-close timeout.
    AcquireExceedsClose,
}

impl fmt::Display for PostgresConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyConnectionString => {
                formatter.write_str("PostgreSQL connection string is empty")
            }
            Self::InvalidConnectionString => {
                formatter.write_str("PostgreSQL connection string is invalid")
            }
            Self::PoolSize => formatter.write_str("PostgreSQL pool size must be from 1 to 1024"),
            Self::TlsOptionInConnectionString => formatter
                .write_str("PostgreSQL TLS options must use facade-owned TLS configuration"),
            Self::EmptyCaCertificate => {
                formatter.write_str("PostgreSQL CA certificate bundle is empty")
            }
            Self::CaCertificateTooLarge { max_bytes } => write!(
                formatter,
                "PostgreSQL CA certificate bundle exceeds {max_bytes} bytes"
            ),
            Self::Timeout { class } => {
                write!(
                    formatter,
                    "PostgreSQL {class} timeout is outside its bounds"
                )
            }
            Self::LockExceedsStatement => {
                formatter.write_str("PostgreSQL lock timeout exceeds statement timeout")
            }
            Self::AcquireExceedsClose => {
                formatter.write_str("PostgreSQL acquire timeout exceeds pool close timeout")
            }
        }
    }
}

impl Error for PostgresConfigError {}

fn validate_duration(
    value: Duration,
    minimum: Duration,
    maximum: Duration,
    class: &'static str,
) -> Result<(), PostgresConfigError> {
    if value < minimum || value > maximum {
        return Err(PostgresConfigError::Timeout { class });
    }
    Ok(())
}

fn validate_connection_query(connection_string: &str) -> Result<(), PostgresConfigError> {
    let Some((_, query)) = connection_string.split_once('?') else {
        return Ok(());
    };
    for pair in query.split('&') {
        let key = pair
            .split_once('=')
            .map_or(pair, |(key, _)| key)
            .to_ascii_lowercase();
        if matches!(
            key.as_str(),
            "sslmode"
                | "ssl-mode"
                | "sslrootcert"
                | "ssl-root-cert"
                | "ssl-ca"
                | "sslcert"
                | "ssl-cert"
                | "sslkey"
                | "ssl-key"
        ) {
            return Err(PostgresConfigError::TlsOptionInConnectionString);
        }
        let recognized = matches!(
            key.as_str(),
            "statement-cache-capacity"
                | "host"
                | "hostaddr"
                | "port"
                | "dbname"
                | "user"
                | "password"
                | "application_name"
                | "options"
        ) || (key.starts_with("options[") && key.ends_with(']'));
        if !recognized {
            return Err(PostgresConfigError::InvalidConnectionString);
        }
    }
    Ok(())
}

/// Applies the immutable `OxideBatch` `PostgreSQL` migration set.
#[derive(Clone, Copy, Debug, Default)]
pub struct PostgresMigrator;

impl PostgresMigrator {
    /// Returns the schema version installed by this crate.
    #[must_use]
    pub const fn supported_schema_version() -> u32 {
        SUPPORTED_SCHEMA_VERSION
    }

    /// Applies pending migrations under a database-scoped advisory lock.
    ///
    /// This operation is intended for a dedicated migrator identity. Runtime
    /// repository startup only verifies the schema and never migrates it.
    ///
    /// # Errors
    ///
    /// Returns a redacted configuration or repository failure. A newer schema
    /// is never changed.
    pub async fn migrate(config: &PostgresConfig) -> Result<(), RepositoryError> {
        config
            .validate()
            .map_err(|_| RepositoryError::Unavailable)?;
        let options = config
            .connect_options()
            .map_err(|_| RepositoryError::Unavailable)?;
        let mut connection =
            tokio::time::timeout(config.connect_timeout, PgConnection::connect_with(&options))
                .await
                .map_err(|_| RepositoryError::Unavailable)?
                .map_err(|_| RepositoryError::Unavailable)?;
        let lock_timeout = duration_millis(config.lock_timeout)?;
        sqlx::query("SELECT set_config('lock_timeout', $1, false)")
            .bind(lock_timeout.to_string())
            .execute(&mut connection)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        tokio::time::timeout(
            config.lock_timeout,
            sqlx::query(
                "SELECT pg_advisory_lock(hashtextextended('oxide_batch.schema.migrations', 0))",
            )
            .execute(&mut connection),
        )
        .await
        .map_err(|_| RepositoryError::Unavailable)?
        .map_err(|_| RepositoryError::Unavailable)?;

        let result = async {
            match read_schema_version(&mut connection).await {
                Ok(current) => verify_schema_version(current)?,
                Err(RepositoryError::SchemaUninitialized) => {
                    let schema_exists: bool = sqlx::query_scalar(
                        "SELECT EXISTS(\
                         SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'oxide_batch')",
                    )
                    .fetch_one(&mut connection)
                    .await
                    .map_err(|_| RepositoryError::Unavailable)?;
                    if !schema_exists {
                        sqlx::query("CREATE SCHEMA oxide_batch")
                            .execute(&mut connection)
                            .await
                            .map_err(|_| RepositoryError::Unavailable)?;
                    }
                }
                Err(error) => return Err(error),
            }
            sqlx::query("SET search_path TO oxide_batch, pg_catalog")
                .execute(&mut connection)
                .await
                .map_err(|_| RepositoryError::Unavailable)?;
            MIGRATOR
                .run(&mut connection)
                .await
                .map_err(|_| RepositoryError::Unavailable)?;
            let installed = read_schema_version(&mut connection).await?;
            verify_schema_version(installed)
        }
        .await;

        let _unlock = sqlx::query(
            "SELECT pg_advisory_unlock(hashtextextended('oxide_batch.schema.migrations', 0))",
        )
        .execute(&mut connection)
        .await;
        result
    }
}

/// Durable `PostgreSQL` implementation of [`JobRepository`].
///
/// `PostgreSQL` identity columns issue instance and execution identifiers, so
/// allocations remain collision-free across process restarts.
#[derive(Clone)]
pub struct PostgresJobRepository {
    pool: PgPool,
    clock: Arc<dyn Clock>,
    config: PostgresConfig,
}

impl PostgresJobRepository {
    /// Opens and verifies a repository-owned `PostgreSQL` pool.
    ///
    /// # Errors
    ///
    /// Returns a redacted unavailable or schema-version failure. Call
    /// [`PostgresMigrator::migrate`] separately with a migrator identity.
    pub async fn connect(
        config: PostgresConfig,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, RepositoryError> {
        config
            .validate()
            .map_err(|_| RepositoryError::Unavailable)?;
        let options = config
            .connect_options()
            .map_err(|_| RepositoryError::Unavailable)?;
        let pool = tokio::time::timeout(
            config.connect_timeout,
            PgPoolOptions::new()
                .max_connections(config.pool_size)
                .acquire_timeout(config.acquire_timeout)
                .idle_timeout(Some(config.connection_idle_timeout))
                .max_lifetime(Some(config.connection_max_lifetime))
                .connect_with(options),
        )
        .await
        .map_err(|_| RepositoryError::Unavailable)?
        .map_err(|_| RepositoryError::Unavailable)?;
        let current = read_schema_version(&pool).await?;
        verify_schema_version(current)?;
        Ok(Self {
            pool,
            clock,
            config,
        })
    }

    /// Closes the repository pool within the configured deadline.
    ///
    /// # Errors
    ///
    /// Returns unavailable when active borrowers do not release the pool
    /// before the deadline.
    pub async fn close(&self) -> Result<(), RepositoryError> {
        tokio::time::timeout(self.config.pool_close_timeout, self.pool.close())
            .await
            .map_err(|_| RepositoryError::Unavailable)
    }

    async fn begin_connection(&self) -> Result<PoolConnection<Postgres>, RepositoryError> {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        if sqlx::query("BEGIN")
            .execute(&mut *connection)
            .await
            .is_err()
        {
            connection.close_on_drop();
            return Err(RepositoryError::Unavailable);
        }
        if let Err(error) = configure_transaction(&mut connection, &self.config).await {
            connection.close_on_drop();
            return Err(error);
        }
        Ok(connection)
    }
}

impl fmt::Debug for PostgresJobRepository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresJobRepository")
            .field("pool_size", &self.pool.size())
            .field("pool_idle", &self.pool.num_idle())
            .finish_non_exhaustive()
    }
}

impl JobRepository for PostgresJobRepository {
    fn begin<'a>(
        &'a self,
    ) -> BoxFuture<'a, Result<Box<dyn RepositoryUnitOfWork + 'a>, RepositoryError>> {
        Box::pin(async move {
            let connection = self.begin_connection().await?;
            Ok(Box::new(PostgresUnitOfWork {
                repository: self,
                connection: Some(connection),
                definition_override: None,
            }) as Box<dyn RepositoryUnitOfWork + 'a>)
        })
    }
}

/// Durable progress loaded for one PostgreSQL-backed step execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PostgresDurableStepState {
    step_execution: StepExecution,
    checkpoint: Checkpoint,
    execution_context: ExecutionContext,
    fault_progress: FaultProgress,
    fault_state: FaultStateEnvelope,
}

impl PostgresDurableStepState {
    /// Borrows the step snapshot, including committed counters and version.
    #[must_use]
    pub const fn step_execution(&self) -> &StepExecution {
        &self.step_execution
    }

    /// Borrows the last committed checkpoint.
    #[must_use]
    pub const fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }

    /// Borrows the last committed execution context.
    #[must_use]
    pub const fn execution_context(&self) -> &ExecutionContext {
        &self.execution_context
    }

    /// Returns the committed fault-tolerance totals of this attempt.
    #[must_use]
    pub const fn fault_progress(&self) -> FaultProgress {
        self.fault_progress
    }

    /// Borrows the validated unresolved retry state of this attempt.
    #[must_use]
    pub const fn fault_state(&self) -> &FaultStateEnvelope {
        &self.fault_state
    }
}

/// Durable `PostgreSQL` retry-reservation state for one step execution.
///
/// A reservation is one short metadata-only transaction that runs after a known
/// rollback and before backoff. It reads the retained state under a row lock,
/// requires the supplied ordinal to follow the persisted one, and advances the
/// phase retry count, the acknowledged rollback count, and the retained
/// envelope with an optimistic version check. A stale or concurrent writer
/// loses instead of spending the same ordinal twice.
///
/// The retained state is cleared by the enlisted chunk commit, because the
/// commit that advances the checkpoint supersedes the whole generation.
pub struct PostgresFaultState {
    repository: PostgresJobRepository,
    revision: ClassifierRevision,
    retry_limit: RetryLimit,
    state_limit: RetryStateLimit,
    bound: Mutex<Option<ChunkTransactionContext>>,
}

impl PostgresFaultState {
    /// Constructs durable state for the step that installs `policy`.
    ///
    /// The runtime binds the step execution before the first chunk attempt.
    #[must_use]
    pub fn new(repository: PostgresJobRepository, policy: &FaultPolicy) -> Self {
        Self {
            repository,
            revision: policy.classifier().revision().clone(),
            retry_limit: policy.retry_limit(),
            state_limit: policy.retry_state_limit(),
            bound: Mutex::new(None),
        }
    }

    fn context(&self) -> Result<ChunkTransactionContext, FaultStateError> {
        (*self
            .bound
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner))
        .ok_or(FaultStateError::Unbound)
    }

    async fn load<'executor, E>(
        &self,
        executor: E,
        lock: bool,
    ) -> Result<PostgresFaultRow, FaultStateError>
    where
        E: sqlx::Executor<'executor, Database = Postgres>,
    {
        let context = self.context()?;
        let query = format!(
            "SELECT execution.version, execution.status, \
             execution.checkpoint_format, execution.checkpoint_schema, \
             execution.checkpoint_schema_version, execution.checkpoint_payload, \
             execution.fault_state_format, execution.fault_state_schema, \
             execution.fault_state_schema_version, execution.fault_state_payload, \
             execution.fault_state_checksum \
             FROM oxide_batch.ob_step_execution execution \
             WHERE execution.id = $1 AND execution.job_execution_id = $2{}",
            if lock { " FOR UPDATE" } else { "" }
        );
        let row = sqlx::query(AssertSqlSafe(query))
            .bind(
                database_id(
                    context.step_execution_id().get(),
                    IdentifierKind::StepExecution,
                )
                .map_err(|_| FaultStateError::Unavailable)?,
            )
            .bind(
                database_id(
                    context.job_execution_id().get(),
                    IdentifierKind::JobExecution,
                )
                .map_err(|_| FaultStateError::Unavailable)?,
            )
            .fetch_optional(executor)
            .await
            .map_err(|_| FaultStateError::Unavailable)?
            .ok_or(FaultStateError::Unavailable)?;
        let checkpoint: Checkpoint = decode_durable_state(
            &row,
            "checkpoint_format",
            "checkpoint_schema",
            "checkpoint_schema_version",
            "checkpoint_payload",
            "oxide-batch.checkpoint",
            Checkpoint::from_json,
        )
        .map_err(|_| FaultStateError::Unavailable)?;
        let checkpoint_digest = checkpoint.generation_digest();
        let envelope = decode_fault_state(&row).map_err(FaultStateError::Corrupt)?;
        envelope.validate_for(self.retry_limit, self.state_limit, &checkpoint_digest)?;
        Ok(PostgresFaultRow {
            version: ExecutionVersion::new(
                read_u64(&row, "version").map_err(|_| FaultStateError::Unavailable)?,
            ),
            started: row
                .try_get::<String, _>("status")
                .map_err(|_| FaultStateError::Unavailable)?
                == BatchStatus::Started.to_string(),
            checkpoint_digest,
            envelope,
        })
    }
}

struct PostgresFaultRow {
    version: ExecutionVersion,
    started: bool,
    checkpoint_digest: [u8; 32],
    envelope: FaultStateEnvelope,
}

impl fmt::Debug for PostgresFaultState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresFaultState")
            .field("retry_limit", &self.retry_limit)
            .field("retry_state_limit", &self.state_limit)
            .finish_non_exhaustive()
    }
}

impl FaultStateStore for PostgresFaultState {
    fn bind(&self, context: ChunkTransactionContext) -> BoxFuture<'_, Result<(), FaultStateError>> {
        Box::pin(async move {
            *self
                .bound
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(context);
            self.load(&self.repository.pool, false).await.map(|_| ())
        })
    }

    fn reserved_ordinal(
        &self,
        key: RetryKey,
    ) -> BoxFuture<'_, Result<Option<RetryOrdinal>, FaultStateError>> {
        Box::pin(async move {
            let row = self.load(&self.repository.pool, false).await?;
            Ok(row.envelope.reserved_ordinal(key))
        })
    }

    fn reserve(&self, reservation: RetryReservation) -> BoxFuture<'_, Result<(), FaultStateError>> {
        Box::pin(async move {
            let context = self.context()?;
            let mut connection = self
                .repository
                .begin_connection()
                .await
                .map_err(|_| FaultStateError::Unavailable)?;
            let result = self
                .reserve_locked(&mut connection, context, reservation)
                .await;
            match result {
                Ok(()) => commit_postgres_connection(connection)
                    .await
                    .map_err(|()| FaultStateError::Unavailable),
                Err(error) => {
                    rollback_chunk_connection(&mut connection).await;
                    Err(error)
                }
            }
        })
    }

    fn resolve(&self, _key: RetryKey) -> BoxFuture<'_, Result<(), FaultStateError>> {
        Box::pin(std::future::ready(Ok(())))
    }

    fn clear_resolved(&self) -> BoxFuture<'_, Result<(), FaultStateError>> {
        Box::pin(std::future::ready(Ok(())))
    }

    fn unresolved(&self) -> BoxFuture<'_, Result<u32, FaultStateError>> {
        Box::pin(async move {
            let row = self.load(&self.repository.pool, false).await?;
            u32::try_from(row.envelope.len()).map_err(|_| FaultStateError::Unavailable)
        })
    }
}

impl PostgresFaultState {
    async fn reserve_locked(
        &self,
        connection: &mut PoolConnection<Postgres>,
        context: ChunkTransactionContext,
        reservation: RetryReservation,
    ) -> Result<(), FaultStateError> {
        let row = self.load(&mut **connection, true).await?;
        if !row.started {
            return Err(FaultStateError::StaleReservation);
        }
        let entry = FaultStateEntry::new(
            reservation.key(),
            reservation.phase(),
            reservation.category(),
            reservation.ordinal(),
            self.revision.clone(),
        );
        let next = row
            .envelope
            .reserved(entry, row.checkpoint_digest, self.state_limit)?;
        let payload: Value = serde_json::from_slice(&next.to_canonical_json()?)
            .map_err(|_| FaultStateError::Unavailable)?;
        let checksum = next.checksum()?;
        let retry_column = match reservation.phase() {
            FaultPhase::Read => "read_retry_count",
            FaultPhase::Process => "process_retry_count",
            FaultPhase::Write => "write_retry_count",
            _ => return Err(FaultStateError::StaleReservation),
        };
        let next_version = row
            .version
            .next()
            .map_err(|_| FaultStateError::Unavailable)?;
        let updated = sqlx::query(AssertSqlSafe(format!(
            "UPDATE oxide_batch.ob_step_execution SET \
             {retry_column} = {retry_column} + 1, rollback_count = rollback_count + 1, \
             fault_state_format = $1, fault_state_schema = $2, \
             fault_state_schema_version = $3, fault_state_payload = $4, \
             fault_state_checksum = $5, \
             updated_at = to_timestamp($6::double precision / 1000.0), version = $7 \
             WHERE id = $8 AND job_execution_id = $9 AND version = $10 AND status = 'STARTED'"
        )))
        .bind(
            i16::try_from(FaultStateEnvelope::FORMAT_VERSION)
                .map_err(|_| FaultStateError::Unavailable)?,
        )
        .bind(FaultStateEnvelope::FORMAT)
        .bind(
            i32::try_from(FaultStateEnvelope::SCHEMA_VERSION)
                .map_err(|_| FaultStateError::Unavailable)?,
        )
        .bind(Json(payload))
        .bind(checksum.as_slice())
        .bind(
            system_time_millis(self.repository.clock.now())
                .map_err(|_| FaultStateError::Unavailable)?,
        )
        .bind(database_version(next_version).map_err(|_| FaultStateError::Unavailable)?)
        .bind(
            database_id(
                context.step_execution_id().get(),
                IdentifierKind::StepExecution,
            )
            .map_err(|_| FaultStateError::Unavailable)?,
        )
        .bind(
            database_id(
                context.job_execution_id().get(),
                IdentifierKind::JobExecution,
            )
            .map_err(|_| FaultStateError::Unavailable)?,
        )
        .bind(database_version(row.version).map_err(|_| FaultStateError::Unavailable)?)
        .execute(&mut **connection)
        .await
        .map_err(|_| FaultStateError::Unavailable)?;
        if updated.rows_affected() == 1 {
            Ok(())
        } else {
            Err(FaultStateError::StaleReservation)
        }
    }
}

/// `PostgreSQL` same-resource chunk transaction manager.
///
/// Each launched chunk receives a single adapter-owned connection. Enlisted
/// business statements and provider-produced checkpoint/context are committed
/// with step counters and the optimistic version.
#[derive(Clone)]
pub struct PostgresChunkTransactionManager {
    repository: PostgresJobRepository,
    state_provider: Arc<dyn PostgresChunkStateProvider>,
}

impl PostgresChunkTransactionManager {
    /// Constructs a same-resource transaction manager.
    #[must_use]
    pub const fn new(
        repository: PostgresJobRepository,
        state_provider: Arc<dyn PostgresChunkStateProvider>,
    ) -> Self {
        Self {
            repository,
            state_provider,
        }
    }

    /// Reads the authoritative committed state through a healthy connection.
    ///
    /// # Errors
    ///
    /// Returns a redacted repository failure when the execution is missing or
    /// its durable state cannot be validated.
    pub async fn load_committed_state(
        &self,
        context: ChunkTransactionContext,
    ) -> Result<PostgresDurableStepState, RepositoryError> {
        let row = sqlx::query(AssertSqlSafe(durable_step_select(
            "WHERE execution.id = $1 AND execution.job_execution_id = $2",
        )))
        .bind(database_id(
            context.step_execution_id().get(),
            IdentifierKind::StepExecution,
        )?)
        .bind(database_id(
            context.job_execution_id().get(),
            IdentifierKind::JobExecution,
        )?)
        .fetch_optional(&self.repository.pool)
        .await
        .map_err(|_| RepositoryError::Unavailable)?
        .ok_or(RepositoryError::StepExecutionNotFound {
            id: context.step_execution_id(),
        })?;
        decode_durable_step_state(&row)
    }
}

/// Produces checkpoint and context state at a `PostgreSQL` chunk commit boundary.
///
/// The provider receives the last committed durable counters and the checked
/// counts for the open chunk. It may also observe application-owned reader
/// state through synchronized values captured by the implementation.
pub trait PostgresChunkStateProvider: Send + Sync {
    /// Produces the durable state to commit with `chunk_counts`.
    ///
    /// # Errors
    ///
    /// Returns a value-redacted preparation failure. Panics are caught by the
    /// adapter and the open transaction is left eligible only for rollback.
    fn state_for_commit(
        &self,
        committed_counts: ExecutionCounts,
        chunk_counts: ChunkCounts,
    ) -> Result<ChunkCommitReceipt, PostgresChunkStateError>;
}

impl<F> PostgresChunkStateProvider for F
where
    F: Fn(ExecutionCounts, ChunkCounts) -> Result<ChunkCommitReceipt, PostgresChunkStateError>
        + Send
        + Sync,
{
    fn state_for_commit(
        &self,
        committed_counts: ExecutionCounts,
        chunk_counts: ChunkCounts,
    ) -> Result<ChunkCommitReceipt, PostgresChunkStateError> {
        self(committed_counts, chunk_counts)
    }
}

/// Value-redacted failure while preparing `PostgreSQL` chunk state.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PostgresChunkStateError;

impl PostgresChunkStateError {
    /// Constructs a redacted state-preparation failure.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl fmt::Display for PostgresChunkStateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PostgreSQL chunk state preparation failed")
    }
}

impl Error for PostgresChunkStateError {}

impl fmt::Debug for PostgresChunkTransactionManager {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresChunkTransactionManager")
            .field("repository", &self.repository)
            .field("durable_state", &"<redacted>")
            .finish_non_exhaustive()
    }
}

impl ChunkTransactionManager for PostgresChunkTransactionManager {
    fn begin(
        &self,
    ) -> BoxFuture<'_, Result<Box<dyn ChunkTransaction + '_>, ChunkTransactionError>> {
        Box::pin(async { Err(ChunkTransactionError::NotCommitted) })
    }

    fn begin_for(
        &self,
        context: ChunkTransactionContext,
    ) -> BoxFuture<'_, Result<Box<dyn ChunkTransaction + '_>, ChunkTransactionError>> {
        Box::pin(async move {
            let step_id = database_id(
                context.step_execution_id().get(),
                IdentifierKind::StepExecution,
            )
            .map_err(|_| ChunkTransactionError::NotCommitted)?;
            let job_id = database_id(
                context.job_execution_id().get(),
                IdentifierKind::JobExecution,
            )
            .map_err(|_| ChunkTransactionError::NotCommitted)?;
            let mut connection = self
                .repository
                .begin_connection()
                .await
                .map_err(|_| ChunkTransactionError::NotCommitted)?;
            let row = sqlx::query(AssertSqlSafe(durable_step_select(
                "WHERE execution.id = $1 AND execution.job_execution_id = $2",
            )))
            .bind(step_id)
            .bind(job_id)
            .fetch_optional(&mut *connection)
            .await;
            let Ok(row) = row else {
                rollback_chunk_connection(&mut connection).await;
                return Err(ChunkTransactionError::NotCommitted);
            };
            let Some(row) = row else {
                rollback_chunk_connection(&mut connection).await;
                return Err(ChunkTransactionError::NotCommitted);
            };
            let Ok(durable) = decode_durable_step_state(&row) else {
                rollback_chunk_connection(&mut connection).await;
                return Err(ChunkTransactionError::NotCommitted);
            };
            if durable.step_execution.metadata().status() != BatchStatus::Started {
                rollback_chunk_connection(&mut connection).await;
                return Err(ChunkTransactionError::NotCommitted);
            }
            Ok(Box::new(PostgresChunkTransaction {
                connection: Some(connection),
                context,
                expected_version: durable.step_execution.version(),
                committed_counts: durable.step_execution.metadata().counts(),
                clock: Arc::clone(&self.repository.clock),
                state_provider: Arc::clone(&self.state_provider),
            }) as Box<dyn ChunkTransaction>)
        })
    }

    fn inherited_progress(
        &self,
        context: ChunkTransactionContext,
    ) -> BoxFuture<'_, Result<InheritedStepProgress, ChunkTransactionError>> {
        Box::pin(async move {
            let durable = self
                .load_committed_state(context)
                .await
                .map_err(|_| ChunkTransactionError::NotCommitted)?;
            let digest = durable.checkpoint.generation_digest();
            if !durable.fault_state.is_empty() && durable.fault_state.checkpoint_digest() != &digest
            {
                return Err(ChunkTransactionError::NotCommitted);
            }
            Ok(InheritedStepProgress::new(
                durable.step_execution.metadata().counts().read(),
                digest,
                durable.fault_progress,
            ))
        })
    }
}

struct PostgresUnitOfWork<'repository> {
    repository: &'repository PostgresJobRepository,
    connection: Option<PoolConnection<Postgres>>,
    definition_override: Option<DefinitionIdentity>,
}

impl PostgresUnitOfWork<'_> {
    fn transaction(&mut self) -> Result<&mut PoolConnection<Postgres>, RepositoryError> {
        self.connection.as_mut().ok_or(RepositoryError::Unavailable)
    }

    async fn job_execution(
        &mut self,
        id: JobExecutionId,
    ) -> Result<Option<JobExecution>, RepositoryError> {
        let id = database_id(id.get(), IdentifierKind::JobExecution)?;
        let row = sqlx::query(AssertSqlSafe(job_execution_select(
            "WHERE execution.id = $1",
        )))
        .bind(id)
        .fetch_optional(&mut **self.transaction()?)
        .await
        .map_err(|_| RepositoryError::Unavailable)?;
        row.map(|row| decode_job_execution(&row)).transpose()
    }

    async fn step_execution(
        &mut self,
        id: StepExecutionId,
    ) -> Result<Option<StepExecution>, RepositoryError> {
        let id = database_id(id.get(), IdentifierKind::StepExecution)?;
        let row = sqlx::query(AssertSqlSafe(step_execution_select(
            "WHERE execution.id = $1",
        )))
        .bind(id)
        .fetch_optional(&mut **self.transaction()?)
        .await
        .map_err(|_| RepositoryError::Unavailable)?;
        row.map(|row| decode_step_execution(&row)).transpose()
    }

    async fn classify_job_cas(
        &mut self,
        id: JobExecutionId,
        expected: ExecutionVersion,
    ) -> RepositoryError {
        match self.job_execution(id).await {
            Ok(Some(actual)) => RepositoryError::Lifecycle(LifecycleError::StaleVersion {
                expected,
                actual: actual.version(),
            }),
            Ok(None) => RepositoryError::JobExecutionNotFound { id },
            Err(error) => error,
        }
    }

    async fn classify_step_cas(
        &mut self,
        id: StepExecutionId,
        expected: ExecutionVersion,
    ) -> RepositoryError {
        match self.step_execution(id).await {
            Ok(Some(actual)) => RepositoryError::Lifecycle(LifecycleError::StaleVersion {
                expected,
                actual: actual.version(),
            }),
            Ok(None) => RepositoryError::StepExecutionNotFound { id },
            Err(error) => error,
        }
    }
}

impl RepositoryUnitOfWork for PostgresUnitOfWork<'_> {
    fn register_definition_upgrade<'a>(
        &'a mut self,
        job_name: &'a JobName,
        upgrade: &'a DefinitionUpgrade,
    ) -> BoxFuture<'a, Result<(), RepositoryError>> {
        Box::pin(async move {
            let registered_at = self.repository.clock.now();
            let from_id = ensure_definition(
                &mut **self.transaction()?,
                job_name.as_str(),
                upgrade.from(),
                registered_at,
            )
            .await?;
            let to_id = ensure_definition(
                &mut **self.transaction()?,
                job_name.as_str(),
                upgrade.to(),
                registered_at,
            )
            .await?;
            let mapping = upgrade
                .step_mapping()
                .iter()
                .map(|(source, target)| {
                    (
                        source.as_str().to_owned(),
                        Value::String(target.as_str().to_owned()),
                    )
                })
                .collect::<Map<String, Value>>();
            let registered_ms = system_time_millis(registered_at)?;
            sqlx::query(
                "INSERT INTO oxide_batch.ob_definition_upgrade \
                 (from_definition_id, to_definition_id, upgrade_key, step_mapping, registered_at) \
                 VALUES ($1, $2, $3, $4, to_timestamp($5::double precision / 1000.0)) \
                 ON CONFLICT (from_definition_id, to_definition_id) DO NOTHING",
            )
            .bind(from_id)
            .bind(to_id)
            .bind(upgrade.key().as_str())
            .bind(Json(Value::Object(mapping.clone())))
            .bind(registered_ms)
            .execute(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            let matches: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM oxide_batch.ob_definition_upgrade \
                 WHERE from_definition_id = $1 AND to_definition_id = $2 \
                 AND upgrade_key = $3 AND step_mapping = $4)",
            )
            .bind(from_id)
            .bind(to_id)
            .bind(upgrade.key().as_str())
            .bind(Json(Value::Object(mapping)))
            .fetch_one(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            if !matches {
                return Err(RepositoryError::DefinitionUpgradeConflict {
                    job_name: job_name.clone(),
                });
            }
            Ok(())
        })
    }

    fn select_or_create_job_instance<'a>(
        &'a mut self,
        key: &'a JobInstanceKey,
    ) -> BoxFuture<'a, Result<JobInstanceSelection, RepositoryError>> {
        Box::pin(async move {
            let encoded = encode_identifying_parameters(key)?;
            let instance_key = canonical_instance_digest(key)?;
            let created_ms = system_time_millis(self.repository.clock.now())?;
            let inserted = sqlx::query(
                "INSERT INTO oxide_batch.ob_job_instance \
                 (job_name, instance_key, identifying_parameters, created_at) \
                 VALUES ($1, $2, $3, to_timestamp($4::double precision / 1000.0)) \
                 ON CONFLICT (job_name, instance_key) DO NOTHING \
                 RETURNING id, job_name, identifying_parameters",
            )
            .bind(key.job_name().as_str())
            .bind(&instance_key[..])
            .bind(Json(encoded))
            .bind(created_ms)
            .fetch_optional(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;

            if let Some(row) = inserted {
                return Ok(JobInstanceSelection::Created(decode_job_instance(&row)?));
            }

            let row = sqlx::query(
                "SELECT id, job_name, identifying_parameters \
                 FROM oxide_batch.ob_job_instance \
                 WHERE job_name = $1 AND instance_key = $2",
            )
            .bind(key.job_name().as_str())
            .bind(&instance_key[..])
            .fetch_one(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            Ok(JobInstanceSelection::Existing(decode_job_instance(&row)?))
        })
    }

    #[allow(clippy::too_many_lines)]
    fn create_job_execution(
        &mut self,
        job_instance_id: JobInstanceId,
    ) -> BoxFuture<'_, Result<JobExecution, RepositoryError>> {
        Box::pin(async move {
            let definition = self
                .definition_override
                .take()
                .unwrap_or_else(DefinitionIdentity::legacy);
            let instance_database_id =
                database_id(job_instance_id.get(), IdentifierKind::JobInstance)?;
            let instance = sqlx::query(
                "SELECT job_name, identifying_parameters \
                 FROM oxide_batch.ob_job_instance WHERE id = $1 FOR UPDATE",
            )
            .bind(instance_database_id)
            .fetch_optional(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?
            .ok_or(RepositoryError::JobInstanceNotFound {
                id: job_instance_id,
            })?;

            let latest = sqlx::query(AssertSqlSafe(job_execution_select(
                "WHERE execution.job_instance_id = $1 ORDER BY execution.attempt DESC LIMIT 1",
            )))
            .bind(instance_database_id)
            .fetch_optional(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?
            .map(|row| decode_job_execution(&row))
            .transpose()?;

            if let Some(execution) = &latest {
                match execution.metadata().status() {
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
                            execution_id: execution.id(),
                            status,
                        });
                    }
                }
            }

            let job_name: String = instance
                .try_get("job_name")
                .map_err(|_| RepositoryError::Unavailable)?;
            let parameters: Json<Value> = instance
                .try_get("identifying_parameters")
                .map_err(|_| RepositoryError::Unavailable)?;
            let registered_at = self.repository.clock.now();
            let definition_id = ensure_definition(
                &mut **self.transaction()?,
                &job_name,
                &definition,
                registered_at,
            )
            .await?;
            let mut upgrade_from = None;
            if let Some(previous) = &latest {
                let previous_definition_id: i64 = sqlx::query_scalar(
                    "SELECT definition_id FROM oxide_batch.ob_job_execution WHERE id = $1",
                )
                .bind(database_id(
                    previous.id().get(),
                    IdentifierKind::JobExecution,
                )?)
                .fetch_one(&mut **self.transaction()?)
                .await
                .map_err(|_| RepositoryError::Unavailable)?;
                if previous_definition_id != definition_id {
                    let compatible: bool = sqlx::query_scalar(
                        "SELECT EXISTS(SELECT 1 FROM oxide_batch.ob_definition_upgrade \
                         WHERE from_definition_id = $1 AND to_definition_id = $2)",
                    )
                    .bind(previous_definition_id)
                    .bind(definition_id)
                    .fetch_one(&mut **self.transaction()?)
                    .await
                    .map_err(|_| RepositoryError::Unavailable)?;
                    if !compatible {
                        return Err(RepositoryError::IncompatibleDefinition {
                            instance_id: job_instance_id,
                        });
                    }
                    upgrade_from = Some(previous_definition_id);
                }
            }
            let attempt: i32 = sqlx::query_scalar(
                "SELECT COALESCE(MAX(attempt), 0) + 1 \
                 FROM oxide_batch.ob_job_execution WHERE job_instance_id = $1",
            )
            .bind(instance_database_id)
            .fetch_one(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            let restart_of = latest
                .as_ref()
                .map(|execution| database_id(execution.id().get(), IdentifierKind::JobExecution))
                .transpose()?;
            let created_at = self.repository.clock.now();
            let created_ms = system_time_millis(created_at)?;
            let context = Json(json!({}));
            let id: i64 = sqlx::query_scalar(
                "INSERT INTO oxide_batch.ob_job_execution \
                 (job_instance_id, definition_id, upgrade_from_definition_id, \
                  restart_of_execution_id, attempt, \
                  status, exit_code, parameters, context_format, context_schema, \
                  context_schema_version, context_payload, created_at, updated_at, version) \
                 VALUES ($1, $2, $3, $4, $5, 'STARTING', 'UNKNOWN', $6, 1, $7, 1, $8, \
                  to_timestamp($9::double precision / 1000.0), \
                  to_timestamp($9::double precision / 1000.0), 0) \
                 RETURNING id",
            )
            .bind(instance_database_id)
            .bind(definition_id)
            .bind(upgrade_from)
            .bind(restart_of)
            .bind(attempt)
            .bind(parameters)
            .bind(DEFAULT_CONTEXT_SCHEMA)
            .bind(context)
            .bind(created_ms)
            .fetch_one(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            let id_value =
                JobExecutionId::new(u64::try_from(id).map_err(|_| RepositoryError::Unavailable)?)?;
            Ok(JobExecution::new(
                id_value,
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
            self.definition_override = Some(definition.clone());
            self.create_job_execution(job_instance_id).await
        })
    }

    #[allow(
        clippy::too_many_lines,
        reason = "exact and mapped restart-state insertion remain visible in one transaction"
    )]
    fn create_step_execution<'a>(
        &'a mut self,
        job_execution_id: JobExecutionId,
        step_name: &'a StepName,
    ) -> BoxFuture<'a, Result<StepExecution, RepositoryError>> {
        Box::pin(async move {
            let job_id = database_id(job_execution_id.get(), IdentifierKind::JobExecution)?;
            let execution_row = sqlx::query(
                "SELECT restart_of_execution_id, upgrade_from_definition_id \
                 FROM oxide_batch.ob_job_execution WHERE id = $1",
            )
            .bind(job_id)
            .fetch_optional(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?
            .ok_or(RepositoryError::JobExecutionNotFound {
                id: job_execution_id,
            })?;
            let restart_source = execution_row
                .try_get::<Option<i64>, _>("restart_of_execution_id")
                .map_err(|_| RepositoryError::Unavailable)?;
            let upgraded = execution_row
                .try_get::<Option<i64>, _>("upgrade_from_definition_id")
                .map_err(|_| RepositoryError::Unavailable)?
                .is_some();
            let created_at = self.repository.clock.now();
            let created_ms = system_time_millis(created_at)?;
            let restarted_id = if let Some(source_job_id) = restart_source {
                let source_step_name = if upgraded {
                    sqlx::query_scalar(
                        "SELECT mapping.key \
                         FROM oxide_batch.ob_job_execution execution \
                         JOIN oxide_batch.ob_definition_upgrade upgrade \
                           ON upgrade.from_definition_id = execution.upgrade_from_definition_id \
                          AND upgrade.to_definition_id = execution.definition_id \
                         CROSS JOIN LATERAL jsonb_each_text(upgrade.step_mapping) mapping \
                         WHERE execution.id = $1 AND mapping.value = $2",
                    )
                    .bind(job_id)
                    .bind(step_name.as_str())
                    .fetch_optional(&mut **self.transaction()?)
                    .await
                    .map_err(|_| RepositoryError::Unavailable)?
                    .ok_or(RepositoryError::InvalidDefinitionUpgrade {
                        execution_id: job_execution_id,
                    })?
                } else {
                    step_name.as_str().to_owned()
                };
                sqlx::query_scalar(
                    "INSERT INTO oxide_batch.ob_step_execution \
                     (job_execution_id, step_name, step_logical_id, status, exit_code, \
                      read_count, processed_count, write_count, filter_count, commit_count, \
                      rollback_count, checkpoint_format, checkpoint_schema, \
                      checkpoint_schema_version, checkpoint_payload, context_format, \
                      context_schema, context_schema_version, context_payload, \
                      read_retry_count, process_retry_count, write_retry_count, \
                      read_skip_count, process_skip_count, write_skip_count, \
                      no_rollback_count, fault_state_format, fault_state_schema, \
                      fault_state_schema_version, fault_state_payload, fault_state_checksum, \
                      created_at, updated_at, version) \
                     SELECT $1, $2, $2, 'STARTING', 'UNKNOWN', source.read_count, \
                      source.processed_count, source.write_count, source.filter_count, \
                      source.commit_count, source.rollback_count, source.checkpoint_format, \
                      source.checkpoint_schema, source.checkpoint_schema_version, \
                      source.checkpoint_payload, source.context_format, source.context_schema, \
                      source.context_schema_version, source.context_payload, \
                      source.read_retry_count, source.process_retry_count, \
                      source.write_retry_count, source.read_skip_count, \
                      source.process_skip_count, source.write_skip_count, \
                      source.no_rollback_count, source.fault_state_format, \
                      source.fault_state_schema, source.fault_state_schema_version, \
                      source.fault_state_payload, source.fault_state_checksum, \
                      to_timestamp($3::double precision / 1000.0), \
                      to_timestamp($3::double precision / 1000.0), 0 \
                     FROM oxide_batch.ob_step_execution source \
                     WHERE source.job_execution_id = $4 AND source.step_name = $5 \
                     ORDER BY source.id DESC LIMIT 1 \
                     RETURNING id",
                )
                .bind(job_id)
                .bind(step_name.as_str())
                .bind(created_ms)
                .bind(source_job_id)
                .bind(source_step_name)
                .fetch_optional(&mut **self.transaction()?)
                .await
                .map_err(|_| RepositoryError::Unavailable)?
            } else {
                None
            };
            let id: i64 = match (restart_source, restarted_id) {
                (Some(_), Some(id)) => id,
                (Some(_), None) => {
                    return Err(RepositoryError::RestartStateNotFound {
                        execution_id: job_execution_id,
                        step_name: step_name.clone(),
                    });
                }
                (None, _) => sqlx::query_scalar(
                    "INSERT INTO oxide_batch.ob_step_execution \
                     (job_execution_id, step_name, step_logical_id, status, exit_code, \
                      checkpoint_format, checkpoint_schema, checkpoint_schema_version, \
                      checkpoint_payload, context_format, context_schema, \
                      context_schema_version, context_payload, created_at, updated_at, version) \
                     VALUES ($1, $2, $2, 'STARTING', 'UNKNOWN', 1, $3, 1, $4, 1, $3, 1, $4, \
                      to_timestamp($5::double precision / 1000.0), \
                      to_timestamp($5::double precision / 1000.0), 0) \
                     RETURNING id",
                )
                .bind(job_id)
                .bind(step_name.as_str())
                .bind(DEFAULT_CONTEXT_SCHEMA)
                .bind(Json(json!({})))
                .bind(created_ms)
                .fetch_one(&mut **self.transaction()?)
                .await
                .map_err(|_| RepositoryError::Unavailable)?,
            };
            let id_value =
                StepExecutionId::new(u64::try_from(id).map_err(|_| RepositoryError::Unavailable)?)?;
            let execution = self
                .step_execution(id_value)
                .await?
                .ok_or(RepositoryError::StepExecutionNotFound { id: id_value })?;
            Ok(execution)
        })
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the instance lock, start-limit check, and state-copy insert form one visible atomic rule"
    )]
    fn create_flow_step_execution<'a>(
        &'a mut self,
        job_execution_id: JobExecutionId,
        step_name: &'a StepName,
        node_id: &'a NodeId,
        start_limit: StartLimit,
    ) -> BoxFuture<'a, Result<StepExecution, RepositoryError>> {
        Box::pin(async move {
            let job_id = database_id(job_execution_id.get(), IdentifierKind::JobExecution)?;
            let instance_id: i64 = sqlx::query_scalar(
                "SELECT job_instance_id FROM oxide_batch.ob_job_execution WHERE id = $1",
            )
            .bind(job_id)
            .fetch_optional(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?
            .ok_or(RepositoryError::JobExecutionNotFound {
                id: job_execution_id,
            })?;
            sqlx::query("SELECT id FROM oxide_batch.ob_job_instance WHERE id = $1 FOR UPDATE")
                .bind(instance_id)
                .fetch_one(&mut **self.transaction()?)
                .await
                .map_err(|_| RepositoryError::Unavailable)?;
            let starts: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM oxide_batch.ob_step_execution step \
                 JOIN oxide_batch.ob_job_execution job ON job.id = step.job_execution_id \
                 WHERE job.job_instance_id = $1 AND step.step_logical_id = $2",
            )
            .bind(instance_id)
            .bind(node_id.as_str())
            .fetch_one(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            if u64::try_from(starts).map_err(|_| RepositoryError::FlowStateCorrupt)?
                >= u64::from(start_limit.get())
            {
                return Err(RepositoryError::StartLimitExceeded {
                    instance_id: JobInstanceId::new(
                        u64::try_from(instance_id)
                            .map_err(|_| RepositoryError::FlowStateCorrupt)?,
                    )?,
                    node_id: node_id.clone(),
                    limit: start_limit,
                });
            }

            let created_ms = system_time_millis(self.repository.clock.now())?;
            let source_id: Option<i64> = sqlx::query_scalar(
                "SELECT step.id FROM oxide_batch.ob_step_execution step \
                 JOIN oxide_batch.ob_job_execution job ON job.id = step.job_execution_id \
                 WHERE job.job_instance_id = $1 AND step.step_logical_id = $2 \
                 ORDER BY job.attempt DESC, step.id DESC LIMIT 1",
            )
            .bind(instance_id)
            .bind(node_id.as_str())
            .fetch_optional(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;

            let id: i64 = if let Some(source_id) = source_id {
                sqlx::query_scalar(
                    "INSERT INTO oxide_batch.ob_step_execution \
                     (job_execution_id, step_name, step_logical_id, status, exit_code, \
                      read_count, processed_count, write_count, filter_count, commit_count, \
                      rollback_count, checkpoint_format, checkpoint_schema, \
                      checkpoint_schema_version, checkpoint_payload, context_format, \
                      context_schema, context_schema_version, context_payload, \
                      read_retry_count, process_retry_count, write_retry_count, \
                      read_skip_count, process_skip_count, write_skip_count, \
                      no_rollback_count, fault_state_format, fault_state_schema, \
                      fault_state_schema_version, fault_state_payload, fault_state_checksum, \
                      created_at, updated_at, version) \
                     SELECT $1, $2, $3, 'STARTING', 'UNKNOWN', source.read_count, \
                      source.processed_count, source.write_count, source.filter_count, \
                      source.commit_count, source.rollback_count, source.checkpoint_format, \
                      source.checkpoint_schema, source.checkpoint_schema_version, \
                      source.checkpoint_payload, source.context_format, source.context_schema, \
                      source.context_schema_version, source.context_payload, \
                      source.read_retry_count, source.process_retry_count, \
                      source.write_retry_count, source.read_skip_count, \
                      source.process_skip_count, source.write_skip_count, \
                      source.no_rollback_count, source.fault_state_format, \
                      source.fault_state_schema, source.fault_state_schema_version, \
                      source.fault_state_payload, source.fault_state_checksum, \
                      to_timestamp($4::double precision / 1000.0), \
                      to_timestamp($4::double precision / 1000.0), 0 \
                     FROM oxide_batch.ob_step_execution source WHERE source.id = $5 \
                     RETURNING id",
                )
                .bind(job_id)
                .bind(step_name.as_str())
                .bind(node_id.as_str())
                .bind(created_ms)
                .bind(source_id)
                .fetch_one(&mut **self.transaction()?)
                .await
                .map_err(|_| RepositoryError::ConcurrentModification)?
            } else {
                sqlx::query_scalar(
                    "INSERT INTO oxide_batch.ob_step_execution \
                     (job_execution_id, step_name, step_logical_id, status, exit_code, \
                      checkpoint_format, checkpoint_schema, checkpoint_schema_version, \
                      checkpoint_payload, context_format, context_schema, \
                      context_schema_version, context_payload, created_at, updated_at, version) \
                     VALUES ($1, $2, $3, 'STARTING', 'UNKNOWN', 1, $4, 1, $5, 1, $4, 1, $5, \
                      to_timestamp($6::double precision / 1000.0), \
                      to_timestamp($6::double precision / 1000.0), 0) RETURNING id",
                )
                .bind(job_id)
                .bind(step_name.as_str())
                .bind(node_id.as_str())
                .bind(DEFAULT_CONTEXT_SCHEMA)
                .bind(Json(json!({})))
                .bind(created_ms)
                .fetch_one(&mut **self.transaction()?)
                .await
                .map_err(|_| RepositoryError::ConcurrentModification)?
            };
            let id = StepExecutionId::new(
                u64::try_from(id).map_err(|_| RepositoryError::FlowStateCorrupt)?,
            )?;
            self.step_execution(id)
                .await?
                .ok_or(RepositoryError::StepExecutionNotFound { id })
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
                &mut **self.transaction()?,
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
                &mut **self.transaction()?,
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
                &mut **self.transaction()?,
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
                &mut **self.transaction()?,
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
            let digest = canonical_instance_digest(key)?;
            let row = sqlx::query(
                "SELECT id, job_name, identifying_parameters \
                 FROM oxide_batch.ob_job_instance \
                 WHERE job_name = $1 AND instance_key = $2",
            )
            .bind(key.job_name().as_str())
            .bind(&digest[..])
            .fetch_optional(&mut **self.transaction()?)
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
            let id = database_id(id.get(), IdentifierKind::JobInstance)?;
            let row = sqlx::query(
                "SELECT id, job_name, identifying_parameters \
                 FROM oxide_batch.ob_job_instance WHERE id = $1",
            )
            .bind(id)
            .fetch_optional(&mut **self.transaction()?)
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
            let instance_id = database_id(job_instance_id.get(), IdentifierKind::JobInstance)?;
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM oxide_batch.ob_job_instance WHERE id = $1)",
            )
            .bind(instance_id)
            .fetch_one(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            if !exists {
                return Err(RepositoryError::JobInstanceNotFound {
                    id: job_instance_id,
                });
            }
            let rows = sqlx::query(AssertSqlSafe(job_execution_select(
                "WHERE execution.job_instance_id = $1 ORDER BY execution.attempt",
            )))
            .bind(instance_id)
            .fetch_all(&mut **self.transaction()?)
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
            let job_id = database_id(job_execution_id.get(), IdentifierKind::JobExecution)?;
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM oxide_batch.ob_job_execution WHERE id = $1)",
            )
            .bind(job_id)
            .fetch_one(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            if !exists {
                return Err(RepositoryError::JobExecutionNotFound {
                    id: job_execution_id,
                });
            }
            let rows = sqlx::query(AssertSqlSafe(step_execution_select(
                "WHERE execution.job_execution_id = $1 ORDER BY execution.id",
            )))
            .bind(job_id)
            .fetch_all(&mut **self.transaction()?)
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
            let instance_id = database_id(job_instance_id.get(), IdentifierKind::JobInstance)?;
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM oxide_batch.ob_job_instance WHERE id = $1)",
            )
            .bind(instance_id)
            .fetch_one(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            if !exists {
                return Err(RepositoryError::JobInstanceNotFound {
                    id: job_instance_id,
                });
            }
            let row = sqlx::query(AssertSqlSafe(durable_step_select(
                "JOIN oxide_batch.ob_job_execution flow_job \
                 ON flow_job.id = execution.job_execution_id \
                 WHERE flow_job.job_instance_id = $1 AND execution.step_logical_id = $2 \
                 ORDER BY flow_job.attempt DESC, execution.id DESC LIMIT 1",
            )))
            .bind(instance_id)
            .bind(node_id.as_str())
            .fetch_optional(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            row.map(|row| {
                let durable = decode_durable_step_state(&row)?;
                Ok(FlowStepState::new(
                    node_id.clone(),
                    durable.step_execution,
                    Some(durable.execution_context),
                ))
            })
            .transpose()
        })
    }

    #[allow(clippy::too_many_lines)]
    fn append_flow_decision<'a>(
        &'a mut self,
        request: &'a FlowDecisionRequest,
    ) -> BoxFuture<'a, Result<FlowDecision, RepositoryError>> {
        Box::pin(async move {
            let job_id = database_id(
                request.job_execution_id().get(),
                IdentifierKind::JobExecution,
            )?;
            let (fingerprint, Json(manifest)): (Vec<u8>, Json<Value>) = sqlx::query_as(
                "SELECT definition.manifest_digest, definition.manifest \
                 FROM oxide_batch.ob_job_execution execution \
                 JOIN oxide_batch.ob_job_definition definition \
                   ON definition.id = execution.definition_id WHERE execution.id = $1",
            )
            .bind(job_id)
            .fetch_optional(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?
            .ok_or(RepositoryError::JobExecutionNotFound {
                id: request.job_execution_id(),
            })?;
            if fingerprint.as_slice() != request.plan_fingerprint() {
                return Err(RepositoryError::FlowStateCorrupt);
            }
            if !crate::flow::decision_matches_manifest(&manifest, request) {
                return Err(RepositoryError::FlowStateCorrupt);
            }
            if let Some(step_id) = request.source_step_execution_id() {
                let valid: bool = sqlx::query_scalar(
                    "SELECT EXISTS( \
                       SELECT 1 FROM oxide_batch.ob_step_execution source \
                       JOIN oxide_batch.ob_job_execution source_job \
                         ON source_job.id = source.job_execution_id \
                       JOIN oxide_batch.ob_job_execution target_job ON target_job.id = $1 \
                       WHERE source.id = $2 \
                         AND source_job.job_instance_id = target_job.job_instance_id \
                         AND source.step_logical_id = $3)",
                )
                .bind(job_id)
                .bind(database_id(step_id.get(), IdentifierKind::StepExecution)?)
                .bind(request.source_node_id().as_str())
                .fetch_one(&mut **self.transaction()?)
                .await
                .map_err(|_| RepositoryError::Unavailable)?;
                if !valid {
                    return Err(RepositoryError::FlowStateCorrupt);
                }
            } else if request.kind() != FlowTransitionKind::Decider {
                return Err(RepositoryError::FlowStateCorrupt);
            }
            if let Some(reused_id) = request.reused_decision_id() {
                let valid: bool = sqlx::query_scalar(
                    "SELECT EXISTS( \
                       SELECT 1 FROM oxide_batch.ob_flow_decision prior \
                       JOIN oxide_batch.ob_job_execution prior_job \
                         ON prior_job.id = prior.job_execution_id \
                       JOIN oxide_batch.ob_job_execution target_job ON target_job.id = $1 \
                       WHERE prior.id = $2 \
                         AND prior_job.job_instance_id = target_job.job_instance_id \
                         AND prior.source_node_id = $3 \
                         AND prior.plan_fingerprint = $4 \
                         AND prior.input_digest = $5 \
                         AND prior.observed_outcome = $6 \
                         AND prior.target_node_id IS NOT DISTINCT FROM $7 \
                         AND prior.terminal_kind IS NOT DISTINCT FROM $8)",
                )
                .bind(job_id)
                .bind(database_id(reused_id.get(), IdentifierKind::FlowDecision)?)
                .bind(request.source_node_id().as_str())
                .bind(request.plan_fingerprint().as_slice())
                .bind(request.input_digest().as_slice())
                .bind(request.observed_outcome().as_str())
                .bind(flow_target_node(request.target()))
                .bind(flow_terminal_code(request.target()))
                .fetch_one(&mut **self.transaction()?)
                .await
                .map_err(|_| RepositoryError::Unavailable)?;
                if !valid {
                    return Err(RepositoryError::FlowStateCorrupt);
                }
            }
            let decided_ms = system_time_millis(request.decided_at())?;
            let id: i64 = sqlx::query_scalar(
                "INSERT INTO oxide_batch.ob_flow_decision \
                 (job_execution_id, source_step_execution_id, reused_decision_id, sequence, \
                  source_node_id, observed_outcome, target_node_id, transition_kind, \
                  terminal_kind, plan_fingerprint, input_digest, decided_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, \
                  to_timestamp($12::double precision / 1000.0)) RETURNING id",
            )
            .bind(job_id)
            .bind(
                request
                    .source_step_execution_id()
                    .map(|id| database_id(id.get(), IdentifierKind::StepExecution))
                    .transpose()?,
            )
            .bind(
                request
                    .reused_decision_id()
                    .map(|id| database_id(id.get(), IdentifierKind::FlowDecision))
                    .transpose()?,
            )
            .bind(database_id(
                request.sequence().get(),
                IdentifierKind::FlowDecision,
            )?)
            .bind(request.source_node_id().as_str())
            .bind(request.observed_outcome().as_str())
            .bind(flow_target_node(request.target()))
            .bind(request.kind().durable_code())
            .bind(flow_terminal_code(request.target()))
            .bind(request.plan_fingerprint().as_slice())
            .bind(request.input_digest().as_slice())
            .bind(decided_ms)
            .fetch_one(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::ConcurrentModification)?;
            Ok(FlowDecision::new(
                FlowDecisionId::new(
                    u64::try_from(id).map_err(|_| RepositoryError::FlowStateCorrupt)?,
                )?,
                request.job_execution_id(),
                request.sequence(),
                request.source_node_id().clone(),
                request.source_step_execution_id(),
                request.kind(),
                request.observed_outcome().clone(),
                request.target().clone(),
                *request.plan_fingerprint(),
                *request.input_digest(),
                request.reused_decision_id(),
                request.decided_at(),
            ))
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
            let instance_id = database_id(job_instance_id.get(), IdentifierKind::JobInstance)?;
            let row = sqlx::query(AssertSqlSafe(flow_decision_select(
                "JOIN oxide_batch.ob_job_execution flow_job \
                 ON flow_job.id = decision.job_execution_id \
                 WHERE flow_job.job_instance_id = $1 AND decision.source_node_id = $2 \
                   AND decision.plan_fingerprint = $3 AND decision.input_digest = $4 \
                   AND decision.transition_kind = $5 \
                 ORDER BY flow_job.attempt DESC, decision.sequence DESC LIMIT 1",
            )))
            .bind(instance_id)
            .bind(node_id.as_str())
            .bind(plan_fingerprint.as_slice())
            .bind(input_digest.as_slice())
            .bind(kind.durable_code())
            .fetch_optional(&mut **self.transaction()?)
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
            let job_id = database_id(job_execution_id.get(), IdentifierKind::JobExecution)?;
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM oxide_batch.ob_job_execution WHERE id = $1)",
            )
            .bind(job_id)
            .fetch_one(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            if !exists {
                return Err(RepositoryError::JobExecutionNotFound {
                    id: job_execution_id,
                });
            }
            let rows = sqlx::query(AssertSqlSafe(flow_decision_select(
                "WHERE decision.job_execution_id = $1 ORDER BY decision.sequence",
            )))
            .bind(job_id)
            .fetch_all(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            rows.iter().map(decode_flow_decision).collect()
        })
    }

    fn recover_job_execution<'a>(
        &'a mut self,
        id: JobExecutionId,
        request: &'a RecoveryRequest,
    ) -> BoxFuture<'a, Result<RecoveryResult, RepositoryError>> {
        Box::pin(async move {
            let database_execution_id = database_id(id.get(), IdentifierKind::JobExecution)?;
            let row = sqlx::query(AssertSqlSafe(job_execution_select(
                "WHERE execution.id = $1 FOR UPDATE",
            )))
            .bind(database_execution_id)
            .fetch_optional(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?
            .ok_or(RepositoryError::JobExecutionNotFound { id })?;
            let prior = decode_job_execution(&row)?;
            let decided_at = self.repository.clock.now();
            let recovered = recovered_execution(&prior, request, decided_at)?;
            let decided_ms = system_time_millis(decided_at)?;
            let prior_version = database_version(request.expected_version())?;
            let resulting_status = recovered.metadata().status().to_string();
            sqlx::query("SAVEPOINT ob_recovery_decision")
                .execute(&mut **self.transaction()?)
                .await
                .map_err(|_| RepositoryError::Unavailable)?;
            let affected = update_job_execution(
                &mut **self.transaction()?,
                &recovered,
                decided_at,
                request.expected_version(),
            )
            .await?;
            if affected != 1 {
                rollback_recovery_savepoint(&mut **self.transaction()?).await;
                return Err(self.classify_job_cas(id, request.expected_version()).await);
            }
            let insert = sqlx::query(
                "INSERT INTO oxide_batch.ob_recovery_decision \
                 (job_execution_id, execution_version, prior_status, resulting_status, \
                  reason_code, operator_reference, evidence_digest, decided_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, \
                  to_timestamp($8::double precision / 1000.0))",
            )
            .bind(database_execution_id)
            .bind(prior_version)
            .bind(prior.metadata().status().to_string())
            .bind(&resulting_status)
            .bind(request.reason_code())
            .bind(request.operator_reference())
            .bind(&request.evidence_digest()[..])
            .bind(decided_ms)
            .execute(&mut **self.transaction()?)
            .await;
            if insert.is_err() {
                rollback_recovery_savepoint(&mut **self.transaction()?).await;
                return Err(RepositoryError::ConcurrentModification);
            }
            sqlx::query("RELEASE SAVEPOINT ob_recovery_decision")
                .execute(&mut **self.transaction()?)
                .await
                .map_err(|_| RepositoryError::Unavailable)?;
            let decision = RecoveryDecision::new(
                id,
                request.expected_version(),
                prior.metadata().status(),
                recovered.metadata().status(),
                request.reason_code().to_owned(),
                request.operator_reference().to_owned(),
                *request.evidence_digest(),
                decided_at,
            );
            Ok(RecoveryResult::new(recovered, decision))
        })
    }

    fn recovery_decision(
        &mut self,
        id: JobExecutionId,
    ) -> BoxFuture<'_, Result<Option<RecoveryDecision>, RepositoryError>> {
        Box::pin(async move {
            let database_execution_id = database_id(id.get(), IdentifierKind::JobExecution)?;
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM oxide_batch.ob_job_execution WHERE id = $1)",
            )
            .bind(database_execution_id)
            .fetch_one(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            if !exists {
                return Err(RepositoryError::JobExecutionNotFound { id });
            }
            let row = sqlx::query(
                "SELECT execution_version, prior_status, resulting_status, reason_code, \
                 operator_reference, evidence_digest, \
                 (extract(epoch FROM decided_at) * 1000)::bigint AS decided_ms \
                 FROM oxide_batch.ob_recovery_decision \
                 WHERE job_execution_id = $1 ORDER BY decided_at DESC, id DESC LIMIT 1",
            )
            .bind(database_execution_id)
            .fetch_optional(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            row.as_ref()
                .map(|row| decode_recovery_decision(id, row))
                .transpose()
        })
    }

    fn commit<'a>(mut self: Box<Self>) -> BoxFuture<'a, Result<(), RepositoryError>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            let connection = self.connection.take().ok_or(RepositoryError::Unavailable)?;
            commit_postgres_connection(connection)
                .await
                .map_err(|()| RepositoryError::CommitOutcomeUnknown)
        })
    }

    fn rollback<'a>(mut self: Box<Self>) -> BoxFuture<'a, Result<(), RepositoryError>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            let mut connection = self.connection.take().ok_or(RepositoryError::Unavailable)?;
            if sqlx::query("ROLLBACK")
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

impl Drop for PostgresUnitOfWork<'_> {
    fn drop(&mut self) {
        if let Some(connection) = &mut self.connection {
            connection.close_on_drop();
        }
    }
}

struct PostgresChunkTransaction {
    connection: Option<PoolConnection<Postgres>>,
    context: ChunkTransactionContext,
    expected_version: ExecutionVersion,
    committed_counts: ExecutionCounts,
    clock: Arc<dyn Clock>,
    state_provider: Arc<dyn PostgresChunkStateProvider>,
}

impl PostgresChunkTransaction {
    fn connection(&mut self) -> Result<&mut PoolConnection<Postgres>, ChunkTransactionError> {
        self.connection
            .as_mut()
            .ok_or(ChunkTransactionError::NotCommitted)
    }

    fn discard_connection(&mut self) {
        if let Some(connection) = &mut self.connection {
            connection.close_on_drop();
        }
    }
}

impl BusinessTransaction for PostgresChunkTransaction {
    fn execute<'a>(
        &'a mut self,
        statement: BusinessStatement<'a>,
    ) -> BoxFuture<'a, Result<BusinessWriteResult, BusinessTransactionError>> {
        Box::pin(async move {
            let statement_text = String::from(statement.text());
            let mut query = sqlx::query(AssertSqlSafe(statement_text));
            for value in statement.values() {
                query = match value.kind() {
                    BusinessValueKind::Text => {
                        query.bind(value.as_text().ok_or(BusinessTransactionError::Rejected)?)
                    }
                    BusinessValueKind::Bytes => {
                        query.bind(value.as_bytes().ok_or(BusinessTransactionError::Rejected)?)
                    }
                    BusinessValueKind::I64 => {
                        query.bind(value.as_i64().ok_or(BusinessTransactionError::Rejected)?)
                    }
                    BusinessValueKind::Bool => {
                        query.bind(value.as_bool().ok_or(BusinessTransactionError::Rejected)?)
                    }
                    BusinessValueKind::Null => query.bind(Option::<String>::None),
                };
            }
            match query
                .execute(
                    &mut **self
                        .connection()
                        .map_err(|_| BusinessTransactionError::Infrastructure)?,
                )
                .await
            {
                Ok(result) => Ok(BusinessWriteResult::new(result.rows_affected())),
                Err(error) => {
                    let classified = classify_business_error(&error);
                    if classified != BusinessTransactionError::Rejected {
                        self.discard_connection();
                    }
                    Err(classified)
                }
            }
        })
    }
}

impl ChunkTransaction for PostgresChunkTransaction {
    fn business_transaction(&mut self) -> Option<&mut dyn BusinessTransaction> {
        Some(self)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the atomic business/progress bind and commit boundary remains visible"
    )]
    fn commit(
        &mut self,
        counts: ChunkCounts,
        fault: ChunkFaultProgress,
    ) -> BoxFuture<'_, Result<ChunkCommitReceipt, ChunkTransactionError>> {
        Box::pin(async move {
            let next_counts = add_chunk_counts(self.committed_counts, counts)?;
            let empty_state = FaultStateEnvelope::empty();
            let empty_payload = durable_fault_payload(&empty_state)?;
            let empty_checksum = empty_state
                .checksum()
                .map_err(|_| ChunkTransactionError::NotCommitted)?;
            let receipt = catch_unwind(AssertUnwindSafe(|| {
                self.state_provider
                    .state_for_commit(self.committed_counts, counts)
            }))
            .ok()
            .and_then(Result::ok)
            .ok_or(ChunkTransactionError::NotCommitted)?;
            let checkpoint_payload = durable_payload(receipt.checkpoint())?;
            let context_payload = durable_payload(receipt.execution_context())?;
            let next_version = self
                .expected_version
                .get()
                .checked_add(1)
                .map(ExecutionVersion::new)
                .ok_or(ChunkTransactionError::NotCommitted)?;
            let result = sqlx::query(
                "UPDATE oxide_batch.ob_step_execution SET \
                 read_count = $1, processed_count = $2, write_count = $3, \
                 filter_count = $4, commit_count = $5, rollback_count = $6, \
                 checkpoint_format = $7, checkpoint_schema = $8, \
                 checkpoint_schema_version = $9, checkpoint_payload = $10, \
                 context_format = $11, context_schema = $12, \
                 context_schema_version = $13, context_payload = $14, \
                 read_skip_count = read_skip_count + $20, \
                 process_skip_count = process_skip_count + $21, \
                 write_skip_count = write_skip_count + $22, \
                 no_rollback_count = no_rollback_count + $23, \
                 fault_state_format = $24, fault_state_schema = $25, \
                 fault_state_schema_version = $26, fault_state_payload = $27, \
                 fault_state_checksum = $28, \
                 updated_at = to_timestamp($15::double precision / 1000.0), version = $16 \
                 WHERE id = $17 AND job_execution_id = $18 \
                 AND version = $19 AND status = 'STARTED'",
            )
            .bind(chunk_database_count(next_counts.read())?)
            .bind(chunk_database_count(next_counts.processed())?)
            .bind(chunk_database_count(next_counts.written())?)
            .bind(chunk_database_count(next_counts.filtered())?)
            .bind(chunk_database_count(next_counts.committed())?)
            .bind(chunk_database_count(next_counts.rolled_back())?)
            .bind(
                i16::try_from(receipt.checkpoint().format_version())
                    .map_err(|_| ChunkTransactionError::NotCommitted)?,
            )
            .bind(receipt.checkpoint().schema_id().as_str())
            .bind(
                i32::try_from(receipt.checkpoint().schema_version().get())
                    .map_err(|_| ChunkTransactionError::NotCommitted)?,
            )
            .bind(Json(checkpoint_payload))
            .bind(
                i16::try_from(receipt.execution_context().format_version())
                    .map_err(|_| ChunkTransactionError::NotCommitted)?,
            )
            .bind(receipt.execution_context().schema_id().as_str())
            .bind(
                i32::try_from(receipt.execution_context().schema_version().get())
                    .map_err(|_| ChunkTransactionError::NotCommitted)?,
            )
            .bind(Json(context_payload))
            .bind(
                system_time_millis(self.clock.now())
                    .map_err(|_| ChunkTransactionError::NotCommitted)?,
            )
            .bind(database_version(next_version).map_err(|_| ChunkTransactionError::NotCommitted)?)
            .bind(
                database_id(
                    self.context.step_execution_id().get(),
                    IdentifierKind::StepExecution,
                )
                .map_err(|_| ChunkTransactionError::NotCommitted)?,
            )
            .bind(
                database_id(
                    self.context.job_execution_id().get(),
                    IdentifierKind::JobExecution,
                )
                .map_err(|_| ChunkTransactionError::NotCommitted)?,
            )
            .bind(
                database_version(self.expected_version)
                    .map_err(|_| ChunkTransactionError::NotCommitted)?,
            )
            .bind(chunk_database_count(fault.skips().read())?)
            .bind(chunk_database_count(fault.skips().process())?)
            .bind(chunk_database_count(fault.skips().write())?)
            .bind(chunk_database_count(fault.no_rollbacks())?)
            .bind(
                i16::try_from(FaultStateEnvelope::FORMAT_VERSION)
                    .map_err(|_| ChunkTransactionError::NotCommitted)?,
            )
            .bind(FaultStateEnvelope::FORMAT)
            .bind(
                i32::try_from(FaultStateEnvelope::SCHEMA_VERSION)
                    .map_err(|_| ChunkTransactionError::NotCommitted)?,
            )
            .bind(Json(empty_payload))
            .bind(empty_checksum.as_slice())
            .execute(&mut **self.connection()?)
            .await;

            let Ok(result) = result else {
                rollback_chunk_transaction(&mut self.connection).await;
                return Err(ChunkTransactionError::NotCommitted);
            };
            let affected = result.rows_affected();
            if affected != 1 {
                rollback_chunk_transaction(&mut self.connection).await;
                return Err(ChunkTransactionError::NotCommitted);
            }

            let connection = self
                .connection
                .take()
                .ok_or(ChunkTransactionError::NotCommitted)?;
            commit_postgres_connection(connection)
                .await
                .map_err(|()| ChunkTransactionError::CommitOutcomeUnknown)?;
            self.expected_version = next_version;
            self.committed_counts = next_counts;
            Ok(receipt)
        })
    }

    fn rollback(&mut self) -> BoxFuture<'_, Result<(), ChunkTransactionError>> {
        Box::pin(async move {
            let Some(mut connection) = self.connection.take() else {
                return Ok(());
            };
            if sqlx::query("ROLLBACK")
                .execute(&mut *connection)
                .await
                .is_err()
            {
                connection.close_on_drop();
            }
            Ok(())
        })
    }
}

impl Drop for PostgresChunkTransaction {
    fn drop(&mut self) {
        if let Some(connection) = &mut self.connection {
            connection.close_on_drop();
        }
    }
}

fn classify_business_error(error: &sqlx::Error) -> BusinessTransactionError {
    let Some(database) = error.as_database_error() else {
        return BusinessTransactionError::Infrastructure;
    };
    let Some(code) = database.code() else {
        return BusinessTransactionError::Infrastructure;
    };
    if code == "57014" {
        return BusinessTransactionError::Cancelled;
    }
    if code.starts_with("22") || code.starts_with("23") || code.starts_with("42") {
        return BusinessTransactionError::Rejected;
    }
    BusinessTransactionError::Infrastructure
}

fn add_chunk_counts(
    current: ExecutionCounts,
    chunk: ChunkCounts,
) -> Result<ExecutionCounts, ChunkTransactionError> {
    Ok(ExecutionCounts::new(
        current
            .read()
            .checked_add(chunk.read().get())
            .ok_or(ChunkTransactionError::NotCommitted)?,
        current
            .processed()
            .checked_add(chunk.processed().get())
            .ok_or(ChunkTransactionError::NotCommitted)?,
        current
            .written()
            .checked_add(chunk.written().get())
            .ok_or(ChunkTransactionError::NotCommitted)?,
        current
            .filtered()
            .checked_add(chunk.filtered().get())
            .ok_or(ChunkTransactionError::NotCommitted)?,
        current
            .committed()
            .checked_add(1)
            .ok_or(ChunkTransactionError::NotCommitted)?,
        current.rolled_back(),
    ))
}

fn chunk_database_count(value: u64) -> Result<i64, ChunkTransactionError> {
    i64::try_from(value).map_err(|_| ChunkTransactionError::NotCommitted)
}

fn durable_fault_payload(state: &FaultStateEnvelope) -> Result<Value, ChunkTransactionError> {
    let bytes = state
        .to_canonical_json()
        .map_err(|_| ChunkTransactionError::NotCommitted)?;
    serde_json::from_slice(&bytes).map_err(|_| ChunkTransactionError::NotCommitted)
}

fn durable_payload(state: &impl DurablePayload) -> Result<Value, ChunkTransactionError> {
    let bytes = state
        .payload_json()
        .map_err(|_| ChunkTransactionError::NotCommitted)?;
    serde_json::from_slice(&bytes).map_err(|_| ChunkTransactionError::NotCommitted)
}

trait DurablePayload {
    fn payload_json(&self) -> Result<Vec<u8>, crate::StateError>;
}

impl DurablePayload for Checkpoint {
    fn payload_json(&self) -> Result<Vec<u8>, crate::StateError> {
        Checkpoint::payload_json(self)
    }
}

impl DurablePayload for ExecutionContext {
    fn payload_json(&self) -> Result<Vec<u8>, crate::StateError> {
        ExecutionContext::payload_json(self)
    }
}

async fn rollback_chunk_connection(connection: &mut PoolConnection<Postgres>) {
    if sqlx::query("ROLLBACK")
        .execute(&mut **connection)
        .await
        .is_err()
    {
        connection.close_on_drop();
    }
}

async fn rollback_recovery_savepoint(connection: &mut PgConnection) {
    let _ = sqlx::query("ROLLBACK TO SAVEPOINT ob_recovery_decision")
        .execute(connection)
        .await;
}

async fn rollback_chunk_transaction(connection: &mut Option<PoolConnection<Postgres>>) {
    let Some(mut connection) = connection.take() else {
        return;
    };
    rollback_chunk_connection(&mut connection).await;
}

async fn commit_postgres_connection(mut connection: PoolConnection<Postgres>) -> Result<(), ()> {
    if sqlx::query("COMMIT")
        .execute(&mut *connection)
        .await
        .is_err()
    {
        connection.close_on_drop();
        return Err(());
    }
    Ok(())
}

async fn configure_transaction(
    connection: &mut PoolConnection<Postgres>,
    config: &PostgresConfig,
) -> Result<(), RepositoryError> {
    for (name, value) in [
        (
            "statement_timeout",
            duration_millis(config.statement_timeout)?,
        ),
        ("lock_timeout", duration_millis(config.lock_timeout)?),
        (
            "idle_in_transaction_session_timeout",
            duration_millis(config.idle_transaction_timeout)?,
        ),
    ] {
        sqlx::query("SELECT set_config($1, $2, true)")
            .bind(name)
            .bind(value.to_string())
            .execute(&mut **connection)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
    }
    Ok(())
}

async fn read_schema_version<'executor, E>(executor: E) -> Result<u32, RepositoryError>
where
    E: sqlx::Executor<'executor, Database = Postgres>,
{
    let version = sqlx::query_scalar::<_, i32>(
        "SELECT version FROM oxide_batch.ob_schema_version WHERE singleton = TRUE",
    )
    .fetch_optional(executor)
    .await
    .map_err(|error| {
        if error
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::code)
            .as_deref()
            .is_some_and(|code| code == "42P01" || code == "3F000")
        {
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

fn duration_millis(duration: Duration) -> Result<i64, RepositoryError> {
    i64::try_from(duration.as_millis()).map_err(|_| RepositoryError::Unavailable)
}

fn database_id(value: u64, kind: IdentifierKind) -> Result<i64, RepositoryError> {
    i64::try_from(value).map_err(|_| RepositoryError::IdentifierOutOfRange { kind, value })
}

fn system_time_millis(value: SystemTime) -> Result<i64, RepositoryError> {
    let duration = value
        .duration_since(UNIX_EPOCH)
        .map_err(|_| RepositoryError::Unavailable)?;
    i64::try_from(duration.as_millis()).map_err(|_| RepositoryError::Unavailable)
}

fn millis_system_time(value: i64) -> Result<SystemTime, RepositoryError> {
    let value = u64::try_from(value).map_err(|_| RepositoryError::Unavailable)?;
    Ok(UNIX_EPOCH + Duration::from_millis(value))
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

fn canonical_instance_digest(key: &JobInstanceKey) -> Result<[u8; 32], RepositoryError> {
    let mut encoded = Vec::new();
    encoded.push(1);
    push_length_prefixed(&mut encoded, key.job_name().as_str().as_bytes())?;
    for (name, kind) in key.identifying_fields() {
        push_length_prefixed(&mut encoded, name.as_str().as_bytes())?;
        let value = key
            .identifying_value(name)
            .ok_or(RepositoryError::Unavailable)?;
        encoded.push(parameter_tag(kind));
        match kind {
            ParameterValueKind::String => push_length_prefixed(
                &mut encoded,
                value
                    .as_str()
                    .ok_or(RepositoryError::Unavailable)?
                    .as_bytes(),
            )?,
            ParameterValueKind::I64 => encoded.extend_from_slice(
                &value
                    .as_i64()
                    .ok_or(RepositoryError::Unavailable)?
                    .to_be_bytes(),
            ),
            ParameterValueKind::U64 => encoded.extend_from_slice(
                &value
                    .as_u64()
                    .ok_or(RepositoryError::Unavailable)?
                    .to_be_bytes(),
            ),
            ParameterValueKind::Bool => encoded.push(u8::from(
                value.as_bool().ok_or(RepositoryError::Unavailable)?,
            )),
        }
        if encoded.len() > MAX_INSTANCE_KEY_INPUT {
            return Err(RepositoryError::Unavailable);
        }
    }
    Ok(Sha256::digest(encoded).into())
}

fn push_length_prefixed(target: &mut Vec<u8>, value: &[u8]) -> Result<(), RepositoryError> {
    let length = u32::try_from(value.len()).map_err(|_| RepositoryError::Unavailable)?;
    target.extend_from_slice(&length.to_be_bytes());
    target.extend_from_slice(value);
    if target.len() > MAX_INSTANCE_KEY_INPUT {
        return Err(RepositoryError::Unavailable);
    }
    Ok(())
}

const fn parameter_tag(kind: ParameterValueKind) -> u8 {
    match kind {
        ParameterValueKind::String => 1,
        ParameterValueKind::I64 => 2,
        ParameterValueKind::U64 => 3,
        ParameterValueKind::Bool => 4,
    }
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
        };
        object.insert(
            name.as_str().to_owned(),
            json!({"type": kind_name, "identifying": true, "value": value}),
        );
    }
    let value = Value::Object(object);
    let size = serde_json::to_vec(&value)
        .map_err(|_| RepositoryError::Unavailable)?
        .len();
    if size > MAX_INSTANCE_KEY_INPUT {
        return Err(RepositoryError::Unavailable);
    }
    Ok(value)
}

fn decode_job_instance(row: &PgRow) -> Result<JobInstance, RepositoryError> {
    let id = row
        .try_get::<i64, _>("id")
        .map_err(|_| RepositoryError::Unavailable)?;
    let job_name = row
        .try_get::<String, _>("job_name")
        .map_err(|_| RepositoryError::Unavailable)?;
    let parameters = row
        .try_get::<Json<Value>, _>("identifying_parameters")
        .map_err(|_| RepositoryError::Unavailable)?;
    let parameters = decode_identifying_parameters(&parameters.0)?;
    let id = JobInstanceId::new(u64::try_from(id).map_err(|_| RepositoryError::Unavailable)?)?;
    let key = JobInstanceKey::new(JobName::new(job_name)?, &parameters);
    Ok(JobInstance::new(id, key))
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

async fn ensure_definition(
    transaction: &mut PgConnection,
    job_name: &str,
    definition: &DefinitionIdentity,
    registered_at: SystemTime,
) -> Result<i64, RepositoryError> {
    let expected_job_name = JobName::new(job_name.to_owned())?;
    if let Some(actual) = definition.job_name()
        && actual != &expected_job_name
    {
        return Err(RepositoryError::DefinitionJobMismatch {
            expected: expected_job_name,
            actual: actual.clone(),
        });
    }
    crate::definition::check_manifest_format(definition.manifest_format()).map_err(|_| {
        RepositoryError::UnsupportedManifestVersion {
            format: definition.manifest_format(),
        }
    })?;
    let manifest: Value = serde_json::from_slice(definition.canonical_manifest())
        .map_err(|_| RepositoryError::Unavailable)?;
    let registered_ms = system_time_millis(registered_at)?;
    sqlx::query(
        "INSERT INTO oxide_batch.ob_job_definition \
         (job_name, definition_revision, manifest_format, manifest_digest, manifest, registered_at) \
         VALUES ($1, $2, $3, $4, $5, to_timestamp($6::double precision / 1000.0)) \
         ON CONFLICT DO NOTHING",
    )
    .bind(job_name)
    .bind(definition.revision().as_str())
    .bind(i16::try_from(definition.manifest_format()).map_err(|_| {
        RepositoryError::UnsupportedManifestVersion {
            format: definition.manifest_format(),
        }
    })?)
    .bind(&definition.manifest_digest()[..])
    .bind(Json(manifest))
    .bind(registered_ms)
    .execute(&mut *transaction)
    .await
    .map_err(|_| RepositoryError::Unavailable)?;
    let id = sqlx::query_scalar(
        "SELECT id FROM oxide_batch.ob_job_definition \
         WHERE job_name = $1 AND manifest_digest = $2",
    )
    .bind(job_name)
    .bind(&definition.manifest_digest()[..])
    .fetch_optional(&mut *transaction)
    .await
    .map_err(|_| RepositoryError::Unavailable)?;
    match id {
        Some(id) => Ok(id),
        None => Err(RepositoryError::DefinitionDrift {
            job_name: expected_job_name,
            revision: definition.revision().clone(),
        }),
    }
}

fn job_execution_select(suffix: &str) -> String {
    format!(
        "SELECT execution.id, execution.job_instance_id, execution.status, \
         execution.exit_code, 0::bigint AS read_count, 0::bigint AS processed_count, \
         0::bigint AS write_count, 0::bigint AS filter_count, \
         0::bigint AS commit_count, 0::bigint AS rollback_count, \
         execution.failure_category, execution.failure_id, \
         (extract(epoch FROM execution.created_at) * 1000)::bigint AS created_ms, \
         (extract(epoch FROM execution.started_at) * 1000)::bigint AS started_ms, \
         (extract(epoch FROM execution.ended_at) * 1000)::bigint AS ended_ms, \
         execution.version FROM oxide_batch.ob_job_execution execution {suffix}"
    )
}

fn step_execution_select(suffix: &str) -> String {
    format!(
        "SELECT execution.id, execution.job_execution_id, execution.step_name, \
         execution.status, execution.exit_code, execution.read_count, \
         execution.processed_count, execution.write_count, execution.filter_count, \
         execution.commit_count, execution.rollback_count, execution.failure_category, \
         execution.failure_id, \
         (extract(epoch FROM execution.created_at) * 1000)::bigint AS created_ms, \
         (extract(epoch FROM execution.started_at) * 1000)::bigint AS started_ms, \
         (extract(epoch FROM execution.ended_at) * 1000)::bigint AS ended_ms, \
         execution.version FROM oxide_batch.ob_step_execution execution {suffix}"
    )
}

fn durable_step_select(suffix: &str) -> String {
    format!(
        "SELECT execution.id, execution.job_execution_id, execution.step_name, \
         execution.status, execution.exit_code, execution.read_count, \
         execution.processed_count, execution.write_count, execution.filter_count, \
         execution.commit_count, execution.rollback_count, execution.failure_category, \
         execution.failure_id, execution.checkpoint_format, execution.checkpoint_schema, \
         execution.checkpoint_schema_version, execution.checkpoint_payload, \
         execution.context_format, execution.context_schema, \
         execution.context_schema_version, execution.context_payload, \
         execution.read_retry_count, execution.process_retry_count, \
         execution.write_retry_count, execution.read_skip_count, \
         execution.process_skip_count, execution.write_skip_count, \
         execution.no_rollback_count, execution.fault_state_format, \
         execution.fault_state_schema, execution.fault_state_schema_version, \
         execution.fault_state_payload, execution.fault_state_checksum, \
         (extract(epoch FROM execution.created_at) * 1000)::bigint AS created_ms, \
         (extract(epoch FROM execution.started_at) * 1000)::bigint AS started_ms, \
         (extract(epoch FROM execution.ended_at) * 1000)::bigint AS ended_ms, \
         execution.version FROM oxide_batch.ob_step_execution execution {suffix}"
    )
}

fn flow_decision_select(suffix: &str) -> String {
    format!(
        "SELECT decision.id, decision.job_execution_id, \
         decision.source_step_execution_id, decision.reused_decision_id, \
         decision.sequence, decision.source_node_id, decision.observed_outcome, \
         decision.target_node_id, decision.transition_kind, decision.terminal_kind, \
         decision.plan_fingerprint, decision.input_digest, \
         (extract(epoch FROM decision.decided_at) * 1000)::bigint AS decided_ms \
         FROM oxide_batch.ob_flow_decision decision {suffix}"
    )
}

fn flow_target_node(target: &FlowTarget) -> Option<&str> {
    match target {
        FlowTarget::Node(node) => Some(node.as_str()),
        FlowTarget::Terminal(_) => None,
    }
}

fn flow_terminal_code(target: &FlowTarget) -> Option<&'static str> {
    match target {
        FlowTarget::Node(_) => None,
        FlowTarget::Terminal(TerminalKind::Complete) => Some("COMPLETE"),
        FlowTarget::Terminal(TerminalKind::Fail) => Some("FAIL"),
        FlowTarget::Terminal(TerminalKind::Stop) => Some("STOP"),
    }
}

fn decode_flow_decision(row: &PgRow) -> Result<FlowDecision, RepositoryError> {
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
        .try_get::<Option<i64>, _>("source_step_execution_id")
        .map_err(|_| RepositoryError::FlowStateCorrupt)?
        .map(|value| {
            StepExecutionId::new(
                u64::try_from(value).map_err(|_| RepositoryError::FlowStateCorrupt)?,
            )
            .map_err(RepositoryError::from)
        })
        .transpose()?;
    let reused_decision_id = row
        .try_get::<Option<i64>, _>("reused_decision_id")
        .map_err(|_| RepositoryError::FlowStateCorrupt)?
        .map(|value| {
            FlowDecisionId::new(
                u64::try_from(value).map_err(|_| RepositoryError::FlowStateCorrupt)?,
            )
            .map_err(RepositoryError::from)
        })
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

fn decode_durable_step_state(row: &PgRow) -> Result<PostgresDurableStepState, RepositoryError> {
    let checkpoint = decode_durable_state(
        row,
        "checkpoint_format",
        "checkpoint_schema",
        "checkpoint_schema_version",
        "checkpoint_payload",
        "oxide-batch.checkpoint",
        Checkpoint::from_json,
    )?;
    let execution_context = decode_durable_state(
        row,
        "context_format",
        "context_schema",
        "context_schema_version",
        "context_payload",
        "oxide-batch.execution-context",
        ExecutionContext::from_json,
    )?;
    Ok(PostgresDurableStepState {
        step_execution: decode_step_execution(row)?,
        checkpoint,
        execution_context,
        fault_progress: decode_fault_progress(row)?,
        fault_state: decode_fault_state(row).map_err(|_| RepositoryError::FaultStateCorrupt)?,
    })
}

fn decode_fault_progress(row: &PgRow) -> Result<FaultProgress, RepositoryError> {
    Ok(FaultProgress::new(
        RetryCounts::new(
            read_u64(row, "read_retry_count")?,
            read_u64(row, "process_retry_count")?,
            read_u64(row, "write_retry_count")?,
        ),
        SkipCounts::new(
            read_u64(row, "read_skip_count")?,
            read_u64(row, "process_skip_count")?,
            read_u64(row, "write_skip_count")?,
        ),
        read_u64(row, "rollback_count")?,
        read_u64(row, "no_rollback_count")?,
    ))
}

fn decode_fault_state(row: &PgRow) -> Result<FaultStateEnvelope, FaultStateFormatError> {
    let format_version = u16::try_from(
        row.try_get::<i16, _>("fault_state_format")
            .map_err(|_| FaultStateFormatError::Malformed)?,
    )
    .map_err(|_| FaultStateFormatError::UnsupportedFormat)?;
    let schema: String = row
        .try_get("fault_state_schema")
        .map_err(|_| FaultStateFormatError::Malformed)?;
    let schema_version = u32::try_from(
        row.try_get::<i32, _>("fault_state_schema_version")
            .map_err(|_| FaultStateFormatError::Malformed)?,
    )
    .map_err(|_| FaultStateFormatError::UnsupportedSchemaVersion)?;
    let Json(payload): Json<Value> = row
        .try_get("fault_state_payload")
        .map_err(|_| FaultStateFormatError::Malformed)?;
    let checksum: Vec<u8> = row
        .try_get("fault_state_checksum")
        .map_err(|_| FaultStateFormatError::Malformed)?;
    let checksum: [u8; 32] = checksum
        .try_into()
        .map_err(|_| FaultStateFormatError::ChecksumMismatch)?;
    let bytes = canonical_fault_bytes(&payload)?;
    FaultStateEnvelope::from_canonical_json(
        format_version,
        &schema,
        schema_version,
        &bytes,
        &checksum,
    )
}

/// Rebuilds the exact canonical bytes the durable checksum covers.
///
/// `jsonb` does not preserve the stored byte form, so the adapter re-emits the
/// document through the framework's canonical member order before validating.
fn canonical_fault_bytes(payload: &Value) -> Result<Vec<u8>, FaultStateFormatError> {
    let object = payload
        .as_object()
        .ok_or(FaultStateFormatError::Malformed)?;
    let mut canonical = serde_json::Map::new();
    for member in ["checkpoint", "entries"] {
        canonical.insert(
            String::from(member),
            object
                .get(member)
                .cloned()
                .ok_or(FaultStateFormatError::Malformed)?,
        );
    }
    serde_json::to_vec(&Value::Object(canonical)).map_err(|_| FaultStateFormatError::Malformed)
}

#[allow(clippy::too_many_arguments)]
fn decode_durable_state<T>(
    row: &PgRow,
    format_column: &str,
    schema_column: &str,
    schema_version_column: &str,
    payload_column: &str,
    format: &str,
    decode: impl FnOnce(&[u8], StateLimits) -> Result<T, crate::StateError>,
) -> Result<T, RepositoryError> {
    let format_version = u16::try_from(
        row.try_get::<i16, _>(format_column)
            .map_err(|_| RepositoryError::Unavailable)?,
    )
    .map_err(|_| RepositoryError::Unavailable)?;
    let schema: String = row
        .try_get(schema_column)
        .map_err(|_| RepositoryError::Unavailable)?;
    let schema_version = u32::try_from(
        row.try_get::<i32, _>(schema_version_column)
            .map_err(|_| RepositoryError::Unavailable)?,
    )
    .map_err(|_| RepositoryError::Unavailable)?;
    let Json(payload): Json<Value> = row
        .try_get(payload_column)
        .map_err(|_| RepositoryError::Unavailable)?;
    let envelope = json!({
        "format": format,
        "format_version": format_version,
        "schema": schema,
        "schema_version": schema_version,
        "payload": payload,
    });
    let bytes = serde_json::to_vec(&envelope).map_err(|_| RepositoryError::Unavailable)?;
    let limits = StateLimits::new(1024 * 1024, 64).map_err(|_| RepositoryError::Unavailable)?;
    decode(&bytes, limits).map_err(|_| RepositoryError::Unavailable)
}

fn decode_job_execution(row: &PgRow) -> Result<JobExecution, RepositoryError> {
    let id = JobExecutionId::new(read_u64(row, "id")?)?;
    let instance_id = JobInstanceId::new(read_u64(row, "job_instance_id")?)?;
    let metadata = decode_execution_metadata(row)?;
    let version = ExecutionVersion::new(read_u64(row, "version")?);
    Ok(JobExecution::from_snapshot(
        id,
        instance_id,
        metadata,
        version,
    ))
}

fn decode_step_execution(row: &PgRow) -> Result<StepExecution, RepositoryError> {
    let id = StepExecutionId::new(read_u64(row, "id")?)?;
    let job_id = JobExecutionId::new(read_u64(row, "job_execution_id")?)?;
    let name = StepName::new(
        row.try_get::<String, _>("step_name")
            .map_err(|_| RepositoryError::Unavailable)?,
    )?;
    let metadata = decode_execution_metadata(row)?;
    let version = ExecutionVersion::new(read_u64(row, "version")?);
    Ok(StepExecution::from_snapshot(
        id, job_id, name, metadata, version,
    ))
}

fn decode_recovery_decision(
    id: JobExecutionId,
    row: &PgRow,
) -> Result<RecoveryDecision, RepositoryError> {
    let digest = row
        .try_get::<Vec<u8>, _>("evidence_digest")
        .map_err(|_| RepositoryError::Unavailable)?;
    let evidence_digest: [u8; 32] = digest
        .try_into()
        .map_err(|_| RepositoryError::Unavailable)?;
    Ok(RecoveryDecision::new(
        id,
        ExecutionVersion::new(read_u64(row, "execution_version")?),
        decode_status(
            &row.try_get::<String, _>("prior_status")
                .map_err(|_| RepositoryError::Unavailable)?,
        )?,
        decode_status(
            &row.try_get::<String, _>("resulting_status")
                .map_err(|_| RepositoryError::Unavailable)?,
        )?,
        row.try_get("reason_code")
            .map_err(|_| RepositoryError::Unavailable)?,
        row.try_get("operator_reference")
            .map_err(|_| RepositoryError::Unavailable)?,
        evidence_digest,
        millis_system_time(read_i64(row, "decided_ms")?)?,
    ))
}

fn decode_execution_metadata(row: &PgRow) -> Result<ExecutionMetadata, RepositoryError> {
    let status = decode_status(
        &row.try_get::<String, _>("status")
            .map_err(|_| RepositoryError::Unavailable)?,
    )?;
    let exit_status = ExitStatus::new(ExitCode::new(
        row.try_get::<String, _>("exit_code")
            .map_err(|_| RepositoryError::Unavailable)?,
    )?);
    let timestamps = ExecutionTimestamps::new(
        millis_system_time(read_i64(row, "created_ms")?)?,
        read_optional_i64(row, "started_ms")?
            .map(millis_system_time)
            .transpose()?,
        read_optional_i64(row, "ended_ms")?
            .map(millis_system_time)
            .transpose()?,
    )?;
    let counts = ExecutionCounts::new(
        read_u64(row, "read_count")?,
        read_u64(row, "processed_count")?,
        read_u64(row, "write_count")?,
        read_u64(row, "filter_count")?,
        read_u64(row, "commit_count")?,
        read_u64(row, "rollback_count")?,
    );
    let category = row
        .try_get::<Option<String>, _>("failure_category")
        .map_err(|_| RepositoryError::Unavailable)?;
    let failure_id = read_optional_i64(row, "failure_id")?;
    let failure = match (category, failure_id) {
        (None, None) => None,
        (Some(category), Some(id)) => Some(FailureSummary::new(
            decode_failure_category(&category)?,
            FailureId::new(u64::try_from(id).map_err(|_| RepositoryError::Unavailable)?)?,
        )),
        _ => return Err(RepositoryError::Unavailable),
    };
    ExecutionMetadata::new(status, exit_status, timestamps, counts, failure)
        .map_err(RepositoryError::from)
}

async fn update_job_execution(
    transaction: &mut PgConnection,
    execution: &JobExecution,
    updated_at: SystemTime,
    expected: ExecutionVersion,
) -> Result<u64, RepositoryError> {
    update_execution(
        transaction,
        "oxide_batch.ob_job_execution",
        execution.id().get(),
        IdentifierKind::JobExecution,
        execution.metadata(),
        execution.version(),
        updated_at,
        expected,
    )
    .await
}

async fn update_step_execution(
    transaction: &mut PgConnection,
    execution: &StepExecution,
    updated_at: SystemTime,
    expected: ExecutionVersion,
) -> Result<u64, RepositoryError> {
    let affected = update_execution(
        transaction,
        "oxide_batch.ob_step_execution",
        execution.id().get(),
        IdentifierKind::StepExecution,
        execution.metadata(),
        execution.version(),
        updated_at,
        expected,
    )
    .await?;
    if affected == 1 {
        let rollback_update = sqlx::query(
            "UPDATE oxide_batch.ob_step_execution SET rollback_count = $1 \
             WHERE id = $2 AND version = $3",
        )
        .bind(
            i64::try_from(execution.metadata().counts().rolled_back())
                .map_err(|_| RepositoryError::Unavailable)?,
        )
        .bind(database_id(
            execution.id().get(),
            IdentifierKind::StepExecution,
        )?)
        .bind(database_version(execution.version())?)
        .execute(transaction)
        .await
        .map_err(|_| RepositoryError::Unavailable)?;
        if rollback_update.rows_affected() != 1 {
            return Err(RepositoryError::Unavailable);
        }
    }
    Ok(affected)
}

#[allow(clippy::too_many_arguments)]
async fn update_execution(
    transaction: &mut PgConnection,
    table: &'static str,
    id: u64,
    kind: IdentifierKind,
    metadata: &ExecutionMetadata,
    new_version: ExecutionVersion,
    updated_at: SystemTime,
    expected: ExecutionVersion,
) -> Result<u64, RepositoryError> {
    let query = format!(
        "UPDATE {table} SET status = $1, exit_code = $2, failure_category = $3, \
         failure_id = $4, started_at = CASE WHEN $5::bigint IS NULL THEN NULL \
         ELSE to_timestamp($5::double precision / 1000.0) END, \
         ended_at = CASE WHEN $6::bigint IS NULL THEN NULL \
         ELSE to_timestamp($6::double precision / 1000.0) END, \
         updated_at = to_timestamp($7::double precision / 1000.0), version = $8 \
         WHERE id = $9 AND version = $10"
    );
    let timestamps = metadata.timestamps();
    let failure = metadata.failure();
    let failure_category = failure.map(|value| encode_failure_category(value.category()));
    let failure_id = failure
        .map(|value| database_id(value.failure_id().get(), IdentifierKind::Failure))
        .transpose()?;
    let result = sqlx::query(AssertSqlSafe(query))
        .bind(metadata.status().to_string())
        .bind(metadata.exit_status().code().as_str())
        .bind(failure_category)
        .bind(failure_id)
        .bind(
            timestamps
                .started_at()
                .map(system_time_millis)
                .transpose()?,
        )
        .bind(timestamps.ended_at().map(system_time_millis).transpose()?)
        .bind(system_time_millis(updated_at)?)
        .bind(database_version(new_version)?)
        .bind(database_id(id, kind)?)
        .bind(database_version(expected)?)
        .execute(transaction)
        .await
        .map_err(|_| RepositoryError::Unavailable)?;
    Ok(result.rows_affected())
}

fn database_version(version: ExecutionVersion) -> Result<i64, RepositoryError> {
    i64::try_from(version.get()).map_err(|_| RepositoryError::Unavailable)
}

fn read_i64(row: &PgRow, name: &str) -> Result<i64, RepositoryError> {
    row.try_get(name).map_err(|_| RepositoryError::Unavailable)
}

fn read_optional_i64(row: &PgRow, name: &str) -> Result<Option<i64>, RepositoryError> {
    row.try_get(name).map_err(|_| RepositoryError::Unavailable)
}

fn read_u64(row: &PgRow, name: &str) -> Result<u64, RepositoryError> {
    u64::try_from(read_i64(row, name)?).map_err(|_| RepositoryError::Unavailable)
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

/// Maps a facade category to its durable name.
///
/// The four M3 categories are accepted by the schema-2 constraint added in
/// migration `0002`. A schema-1 database rejects them, so no runtime can
/// silently store a value an older reader cannot interpret.
const fn encode_failure_category(value: FailureCategory) -> &'static str {
    value.durable_code()
}

fn decode_failure_category(value: &str) -> Result<FailureCategory, RepositoryError> {
    FailureCategory::from_durable_code(value).ok_or(RepositoryError::Unavailable)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_instance_key_matches_version_one_golden_vector() -> Result<(), Box<dyn Error>> {
        let parameters = JobParameters::try_from_iter([
            (
                ParameterName::new("region")?,
                JobParameter::new(ParameterValue::string("서울")?, ParameterRole::Identifying),
            ),
            (
                ParameterName::new("limit")?,
                JobParameter::new(
                    ParameterValue::from(1_u64 << 63),
                    ParameterRole::Identifying,
                ),
            ),
            (
                ParameterName::new("count")?,
                JobParameter::new(ParameterValue::from(-2_i64), ParameterRole::Identifying),
            ),
            (
                ParameterName::new("active")?,
                JobParameter::new(ParameterValue::from(true), ParameterRole::Identifying),
            ),
        ])?;
        let key = JobInstanceKey::new(JobName::new("golden_job")?, &parameters);
        assert_eq!(
            canonical_instance_digest(&key)?,
            [
                0x71, 0xf1, 0x2d, 0xb9, 0xe3, 0x88, 0x7d, 0xe2, 0xcf, 0x92, 0xe9, 0x3b, 0xb6, 0x3f,
                0xd4, 0xe9, 0xe7, 0xc5, 0x36, 0xdf, 0x8f, 0xa2, 0x02, 0x21, 0x24, 0x45, 0xd1, 0x8b,
                0xf2, 0xe4, 0x36, 0x04,
            ]
        );
        Ok(())
    }
}
