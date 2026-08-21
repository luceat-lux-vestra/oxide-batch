use std::collections::BTreeSet;
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
use sqlx::postgres::{PgArguments, PgConnectOptions, PgPoolOptions, PgRow, PgSslMode};
use sqlx::types::Json;
use sqlx::{AssertSqlSafe, Connection, PgConnection, PgPool, Postgres, Row};

use oxide_batch_repository::{
    PartitionMutationError, aggregate_partition_parent, map_partition_aggregation,
    recovered_execution,
};

use crate::{
    ActorRef, BatchStatus, BoxFuture, BusinessStatement, BusinessTransaction,
    BusinessTransactionError, BusinessValueKind, BusinessWriteResult, Checkpoint,
    ChunkCommitReceipt, ChunkCounts, ChunkFaultProgress, ChunkTransaction, ChunkTransactionContext,
    ChunkTransactionError, ChunkTransactionManager, ClassifierRevision, Clock,
    ComponentStateEnvelope, ComponentStatePayload, ComponentStreamIdentity, ContentIdentity,
    CursorError, CursorKey, DefinitionDescriptor, DefinitionIdentity, DefinitionRevision,
    DefinitionUpgrade, DurableStateKind, ExecutionContext, ExecutionCounts, ExecutionMetadata,
    ExecutionTimestamps, ExecutionVersion, ExitCode, ExitStatus, ExplorerError, ExplorerQuery,
    ExplorerRepository, ExternalStateReference, FailureCategory, FailureId, FailureSummary,
    FaultPhase, FaultPolicy, FaultProgress, FaultStateEntry, FaultStateEnvelope, FaultStateError,
    FaultStateFormatError, FaultStateStore, FlowDecision, FlowDecisionId, FlowDecisionRequest,
    FlowDecisionSequence, FlowStepState, FlowTarget, FlowTransitionKind, IdentifierKind,
    InheritedStepProgress, JobExecution, JobExecutionId, JobExecutionProjection, JobInstance,
    JobInstanceId, JobInstanceKey, JobInstanceProjection, JobInstanceSelection, JobName,
    JobParameter, JobParameters, JobRepository, LifecycleError, LifecycleTransition,
    MAX_PARTITION_CONTEXT_BYTES, MAX_PARTITIONS, NodeId, OperationId, OperatorAction,
    OperatorOutcomeClass, OperatorRecord, OperatorRecordDraft, OperatorRejection,
    OperatorRequestId, ParameterDescriptor, ParameterName, ParameterRole, ParameterValue,
    ParameterValueKind, PartitionKey, PartitionPlanEntry, PartitionResult, PurgeBatchBound,
    PurgeCandidate, PurgeCounts, PurgePlan, PurgePlanRequest, PurgeSurvey, QueryWindow, ReasonCode,
    RecoveryDecision, RecoveryDecisionId, RecoveryRequest, RecoveryResult, RepositoryCapability,
    RepositoryDescriptor, RepositoryError, RepositoryUnitOfWork, RequestDigest, RetentionAction,
    RetentionActionId, RetentionHold, RetentionOutcome, RetentionRecord, RetentionRecordDraft,
    RetryCounts, RetryKey, RetryLimit, RetryOrdinal, RetryReservation, RetryStateLimit, SkipCounts,
    StartLimit, StateEnvelopeDescriptor, StateLimits, StateSchemaId, StateSchemaVersion,
    StepExecution, StepExecutionId, StepExecutionProjection, StepName, StepPartition,
    StepPartitionId, StepPartitionProjection, TerminalKind,
};

const SUPPORTED_SCHEMA_VERSION: u32 = 4;
const MAX_INSTANCE_KEY_INPUT: usize = 1024 * 1024;
const MAX_POOL_SIZE: u32 = 1024;
const MAX_SHORT_TIMEOUT: Duration = Duration::from_mins(5);
const MAX_STATEMENT_TIMEOUT: Duration = Duration::from_hours(24);
const MAX_CONNECTION_LIFETIME: Duration = Duration::from_hours(7 * 24);
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

    /// Reads the installed schema version without changing it.
    ///
    /// This is the read-only counterpart of [`PostgresMigrator::migrate`]. It
    /// takes no advisory lock, applies no migration, and creates no schema, so
    /// an unprivileged operator identity can report migration state. An
    /// uninitialized schema is reported as `None` rather than as a failure,
    /// because "not yet migrated" is an answer rather than an outage.
    ///
    /// # Errors
    ///
    /// Returns a redacted configuration or repository failure. The connection
    /// string, driver message, and SQL text are never included.
    pub async fn installed_schema_version(
        config: &PostgresConfig,
    ) -> Result<Option<u32>, RepositoryError> {
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
        match read_schema_version(&mut connection).await {
            Ok(version) => Ok(Some(version)),
            Err(RepositoryError::SchemaUninitialized) => Ok(None),
            Err(error) => Err(error),
        }
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
                Ok(current) if current > SUPPORTED_SCHEMA_VERSION => {
                    return Err(RepositoryError::NewerSchema {
                        current,
                        supported: SUPPORTED_SCHEMA_VERSION,
                    });
                }
                Ok(_) => {}
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
    fn connection_capacity(&self) -> u32 {
        self.config.pool_size
    }

    /// Schema 3 provides every capability this milestone defines. The pool
    /// size above is a throughput setting and is deliberately absent here:
    /// capabilities describe what the deployment can do, not how fast.
    fn descriptor(&self) -> RepositoryDescriptor {
        RepositoryDescriptor::new(
            SUPPORTED_SCHEMA_VERSION,
            [
                RepositoryCapability::ExecutionOwnership,
                RepositoryCapability::InstanceHolds,
                RepositoryCapability::OperatorRequests,
                RepositoryCapability::RetentionPurge,
                RepositoryCapability::StepPartitions,
                RepositoryCapability::StopRequests,
            ],
        )
    }

    fn begin<'a>(
        &'a self,
    ) -> BoxFuture<'a, Result<Box<dyn RepositoryUnitOfWork + 'a>, RepositoryError>> {
        Box::pin(async move {
            let connection = self.begin_connection().await?;
            Ok(Box::new(PostgresUnitOfWork {
                repository: self,
                connection: Some(connection),
                definition_override: None,
                created_partition_plans: BTreeSet::new(),
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

    fn inherited_component_state(
        &self,
        context: ChunkTransactionContext,
    ) -> BoxFuture<'_, Result<Vec<ComponentStateEnvelope>, ChunkTransactionError>> {
        Box::pin(async move {
            let step_id = database_id(
                context.step_execution_id().get(),
                IdentifierKind::StepExecution,
            )
            .map_err(|_| ChunkTransactionError::NotCommitted)?;
            let rows = sqlx::query(
                "SELECT namespace, schema_id, schema_version, codec_id, codec_version, \
                 checksum_algorithm, checksum_algorithm_version, checksum, payload_kind, \
                 payload, external_content_id, external_encoded_len \
                 FROM oxide_batch.ob_component_state WHERE step_execution_id = $1",
            )
            .bind(step_id)
            .fetch_all(&self.repository.pool)
            .await
            .map_err(|_| ChunkTransactionError::NotCommitted)?;
            rows.iter()
                .map(decode_component_state_row)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|()| ChunkTransactionError::NotCommitted)
        })
    }
}

fn decode_component_state_row(row: &PgRow) -> Result<ComponentStateEnvelope, ()> {
    let namespace = read_text(row, "namespace").map_err(|_| ())?;
    let namespace = ComponentStreamIdentity::new(namespace).map_err(|_| ())?;
    let schema_id = read_text(row, "schema_id").map_err(|_| ())?;
    let schema_version =
        u32::try_from(row.try_get::<i32, _>("schema_version").map_err(|_| ())?).map_err(|_| ())?;
    let codec_id = read_text(row, "codec_id").map_err(|_| ())?;
    let codec_version =
        u32::try_from(row.try_get::<i32, _>("codec_version").map_err(|_| ())?).map_err(|_| ())?;
    let checksum_algorithm = u16::try_from(
        row.try_get::<i16, _>("checksum_algorithm")
            .map_err(|_| ())?,
    )
    .map_err(|_| ())?;
    let checksum_algorithm_version = u16::try_from(
        row.try_get::<i16, _>("checksum_algorithm_version")
            .map_err(|_| ())?,
    )
    .map_err(|_| ())?;
    let checksum = read_digest(row, "checksum").map_err(|_| ())?;
    let payload_kind: String = row.try_get("payload_kind").map_err(|_| ())?;
    let payload = match payload_kind.as_str() {
        "INLINE" => {
            // `payload` is `bytea`: read the exact codec-produced bytes the
            // checksum was computed over, never a `jsonb` round-trip that
            // could reserialize with different whitespace/key order.
            let bytes: Vec<u8> = row.try_get("payload").map_err(|_| ())?;
            ComponentStatePayload::Inline(bytes)
        }
        "EXTERNAL" => {
            let content_id = read_digest(row, "external_content_id").map_err(|_| ())?;
            let encoded_len: i64 = row.try_get("external_encoded_len").map_err(|_| ())?;
            let encoded_len = u64::try_from(encoded_len).map_err(|_| ())?;
            ComponentStatePayload::External(ExternalStateReference::new(
                ContentIdentity::from_bytes(content_id),
                encoded_len,
            ))
        }
        _ => return Err(()),
    };
    ComponentStateEnvelope::from_durable(
        namespace,
        schema_id.as_str(),
        schema_version,
        codec_id.as_str(),
        codec_version,
        checksum_algorithm,
        checksum_algorithm_version,
        checksum,
        payload,
        component_state_hard_limits(),
    )
    .map_err(|_| ())
}

/// The bounds `ob_component_state.payload`'s own `CHECK` constraints already
/// enforce (1 MiB, depth 64) -- read-side reconstruction validates against
/// exactly this ceiling rather than a smaller configured default, since the
/// specific `StateLimits` a namespace's codec was encoded under is not known
/// at this layer.
fn component_state_hard_limits() -> StateLimits {
    StateLimits::new(1_048_576, 64).unwrap_or_default()
}

struct PostgresUnitOfWork<'repository> {
    repository: &'repository PostgresJobRepository,
    connection: Option<PoolConnection<Postgres>>,
    definition_override: Option<DefinitionIdentity>,
    created_partition_plans: BTreeSet<StepExecutionId>,
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

    async fn purge_delete(
        &mut self,
        statement: &'static str,
        ids: &[i64],
    ) -> Result<u64, RepositoryError> {
        if ids.is_empty() {
            return Ok(0);
        }
        Ok(sqlx::query(statement)
            .bind(ids)
            .execute(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?
            .rows_affected())
    }

    async fn purge_count(
        &mut self,
        statement: &'static str,
        ids: &[i64],
    ) -> Result<u64, RepositoryError> {
        if ids.is_empty() {
            return Ok(0);
        }
        let row = sqlx::query(statement)
            .bind(ids)
            .fetch_one(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        read_u64(&row, "matched")
    }

    async fn purge_counts(
        &mut self,
        candidates: &[PurgeCandidate],
    ) -> Result<PurgeCounts, RepositoryError> {
        let executions = candidate_execution_ids(candidates)?;
        let instances = candidate_instance_ids(candidates)?;
        let flow_decisions = self
            .purge_count(
                "SELECT count(*) AS matched FROM oxide_batch.ob_flow_decision \
                 WHERE job_execution_id = ANY($1)",
                &executions,
            )
            .await?;
        let recovery_decisions = self
            .purge_count(
                "SELECT count(*) AS matched FROM oxide_batch.ob_recovery_decision \
                 WHERE job_execution_id = ANY($1)",
                &executions,
            )
            .await?;
        let operator_requests = self
            .purge_count(
                "SELECT count(*) AS matched FROM oxide_batch.ob_operator_request \
                 WHERE job_execution_id = ANY($1)",
                &executions,
            )
            .await?;
        let step_partitions = self
            .purge_count(
                "SELECT count(*) AS matched FROM oxide_batch.ob_step_partition \
                 WHERE step_execution_id IN ( \
                   SELECT id FROM oxide_batch.ob_step_execution \
                   WHERE job_execution_id = ANY($1))",
                &executions,
            )
            .await?;
        let step_executions = self
            .purge_count(
                "SELECT count(*) AS matched FROM oxide_batch.ob_step_execution \
                 WHERE job_execution_id = ANY($1)",
                &executions,
            )
            .await?;
        let job_instances = if instances.is_empty() {
            0
        } else {
            let row = sqlx::query(
                "SELECT count(*) AS matched FROM oxide_batch.ob_job_instance instance \
                 WHERE instance.id = ANY($2) AND NOT EXISTS ( \
                   SELECT 1 FROM oxide_batch.ob_job_execution execution \
                   WHERE execution.job_instance_id = instance.id \
                     AND execution.id <> ALL($1))",
            )
            .bind(&executions)
            .bind(&instances)
            .fetch_one(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            read_u64(&row, "matched")?
        };
        Ok(PurgeCounts::new(
            flow_decisions,
            recovery_decisions,
            operator_requests,
            step_partitions,
            step_executions,
            u64::try_from(candidates.len()).unwrap_or(u64::MAX),
            job_instances,
        ))
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
            let instance_key = key.digest();
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
            let (restarted_id, source_step_execution_id) = if let Some(source_job_id) =
                restart_source
            {
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
                let source_id: Option<i64> = sqlx::query_scalar(
                    "SELECT source.id FROM oxide_batch.ob_step_execution source \
                     WHERE source.job_execution_id = $1 AND source.step_name = $2 \
                     ORDER BY source.id DESC LIMIT 1",
                )
                .bind(source_job_id)
                .bind(&source_step_name)
                .fetch_optional(&mut **self.transaction()?)
                .await
                .map_err(|_| RepositoryError::Unavailable)?;
                let restarted_id = match source_id {
                    Some(source_id) => sqlx::query_scalar(
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
                         FROM oxide_batch.ob_step_execution source WHERE source.id = $4 \
                         RETURNING id",
                    )
                    .bind(job_id)
                    .bind(step_name.as_str())
                    .bind(created_ms)
                    .bind(source_id)
                    .fetch_optional(&mut **self.transaction()?)
                    .await
                    .map_err(|_| RepositoryError::Unavailable)?,
                    None => None,
                };
                (restarted_id, source_id)
            } else {
                (None, None)
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
            if let Some(source_step_execution_id) = source_step_execution_id {
                copy_forward_component_state(
                    &mut **self.transaction()?,
                    source_step_execution_id,
                    id,
                )
                .await?;
            }
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
            if let Some(source_id) = source_id {
                copy_forward_component_state(&mut **self.transaction()?, source_id, id).await?;
            }
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
            let digest = key.digest();
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
            } else if !matches!(
                request.kind(),
                FlowTransitionKind::Decider | FlowTransitionKind::SplitAggregate
            ) {
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
                .bind(flow_terminal_code(request.target())?)
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
            .bind(flow_terminal_code(request.target())?)
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

    fn create_step_partition_plan<'a>(
        &'a mut self,
        step_execution_id: StepExecutionId,
        entries: &'a [PartitionPlanEntry],
    ) -> BoxFuture<'a, Result<Vec<StepPartition>, RepositoryError>> {
        Box::pin(async move {
            if entries.is_empty() {
                return Err(RepositoryError::EmptyPartitionPlan);
            }
            if entries.len() > usize::from(MAX_PARTITIONS) {
                return Err(RepositoryError::PartitionPlanTooLarge {
                    max: usize::from(MAX_PARTITIONS),
                });
            }
            let mut keys = BTreeSet::new();
            for entry in entries {
                if !keys.insert(entry.key()) {
                    return Err(RepositoryError::DuplicatePartitionKey);
                }
            }

            let parent_id = database_id(step_execution_id.get(), IdentifierKind::StepExecution)?;
            let parent_status = sqlx::query_scalar::<_, String>(
                "SELECT status FROM oxide_batch.ob_step_execution \
                 WHERE id = $1 FOR UPDATE",
            )
            .bind(parent_id)
            .fetch_optional(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            let parent_status = parent_status
                .ok_or(RepositoryError::StepExecutionNotFound {
                    id: step_execution_id,
                })
                .and_then(|status| decode_status(&status))?;
            if !matches!(parent_status, BatchStatus::Starting | BatchStatus::Started) {
                return Err(RepositoryError::PartitionParentNotActive {
                    step_execution_id,
                    status: parent_status,
                });
            }
            let plan_exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM oxide_batch.ob_step_partition \
                 WHERE step_execution_id = $1)",
            )
            .bind(parent_id)
            .fetch_one(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            if plan_exists {
                return Err(RepositoryError::PartitionPlanExists { step_execution_id });
            }

            let mut partitions = Vec::with_capacity(entries.len());
            for (index, entry) in entries.iter().enumerate() {
                let ordinal = u32::try_from(index.saturating_add(1))
                    .map_err(|_| RepositoryError::PartitionStateCorrupt)?;
                let payload = entry
                    .context()
                    .payload_json()
                    .map_err(|_| RepositoryError::PartitionStateCorrupt)?;
                let payload = serde_json::from_slice::<Value>(&payload)
                    .map_err(|_| RepositoryError::PartitionStateCorrupt)?;
                let envelope = entry
                    .context()
                    .to_json()
                    .map_err(|_| RepositoryError::PartitionStateCorrupt)?;
                let checksum: [u8; 32] = Sha256::digest(&envelope).into();
                let database_ordinal =
                    i32::try_from(ordinal).map_err(|_| RepositoryError::PartitionStateCorrupt)?;
                let format = i16::try_from(entry.context().format_version())
                    .map_err(|_| RepositoryError::PartitionStateCorrupt)?;
                let schema_version = i32::try_from(entry.context().schema_version().get())
                    .map_err(|_| RepositoryError::PartitionStateCorrupt)?;
                let database_partition_id: i64 = sqlx::query_scalar(
                    "INSERT INTO oxide_batch.ob_step_partition \
                     (step_execution_id, partition_key, partition_ordinal, status, \
                      context_format, context_schema, context_schema_version, \
                      context_payload, context_checksum, version) \
                     VALUES ($1, $2, $3, 'STARTING', $4, $5, $6, $7, $8, 0) \
                     RETURNING id",
                )
                .bind(parent_id)
                .bind(entry.key().as_str())
                .bind(database_ordinal)
                .bind(format)
                .bind(entry.context().schema_id().as_str())
                .bind(schema_version)
                .bind(Json(payload))
                .bind(&checksum[..])
                .fetch_one(&mut **self.transaction()?)
                .await
                .map_err(|_| RepositoryError::Unavailable)?;
                let id = StepPartitionId::new(
                    u64::try_from(database_partition_id)
                        .map_err(|_| RepositoryError::PartitionStateCorrupt)?,
                )?;
                partitions.push(StepPartition::starting(
                    id,
                    step_execution_id,
                    ordinal,
                    entry.clone(),
                ));
            }
            self.created_partition_plans.insert(step_execution_id);
            Ok(partitions)
        })
    }

    fn step_partition_plan(
        &mut self,
        step_execution_id: StepExecutionId,
    ) -> BoxFuture<'_, Result<Vec<StepPartition>, RepositoryError>> {
        Box::pin(async move {
            let parent_id = database_id(step_execution_id.get(), IdentifierKind::StepExecution)?;
            let parent_exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM oxide_batch.ob_step_execution WHERE id = $1)",
            )
            .bind(parent_id)
            .fetch_one(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            if !parent_exists {
                return Err(RepositoryError::StepExecutionNotFound {
                    id: step_execution_id,
                });
            }
            let rows = sqlx::query(AssertSqlSafe(partition_select(
                "WHERE partition.step_execution_id = $1 ORDER BY partition.partition_key",
            )))
            .bind(parent_id)
            .fetch_all(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            rows.iter().map(decode_step_partition).collect()
        })
    }

    #[allow(
        clippy::too_many_lines,
        reason = "source validation and complete bounded carry-forward form one transaction rule"
    )]
    fn restart_step_partition_plan(
        &mut self,
        source_step_execution_id: StepExecutionId,
        target_step_execution_id: StepExecutionId,
    ) -> BoxFuture<'_, Result<Vec<StepPartition>, RepositoryError>> {
        Box::pin(async move {
            let source_id = database_id(
                source_step_execution_id.get(),
                IdentifierKind::StepExecution,
            )?;
            let target_id = database_id(
                target_step_execution_id.get(),
                IdentifierKind::StepExecution,
            )?;
            let rows = sqlx::query(
                "SELECT source.step_logical_id = target.step_logical_id AS same_logical_id, \
                        source_job.status AS source_job_status, target.status AS target_status \
                 FROM oxide_batch.ob_step_execution source \
                 JOIN oxide_batch.ob_job_execution source_job \
                   ON source_job.id = source.job_execution_id \
                 CROSS JOIN oxide_batch.ob_step_execution target \
                 WHERE source.id = $1 AND target.id = $2 \
                 FOR UPDATE OF source_job, source, target",
            )
            .bind(source_id)
            .bind(target_id)
            .fetch_optional(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?
            .ok_or(RepositoryError::PartitionStateCorrupt)?;
            let same_logical_id = rows
                .try_get::<bool, _>("same_logical_id")
                .map_err(|_| RepositoryError::PartitionStateCorrupt)?;
            let source_job_status = decode_status(
                rows.try_get::<String, _>("source_job_status")
                    .map_err(|_| RepositoryError::PartitionStateCorrupt)?
                    .as_str(),
            )?;
            let target_status = decode_status(
                rows.try_get::<String, _>("target_status")
                    .map_err(|_| RepositoryError::PartitionStateCorrupt)?
                    .as_str(),
            )?;
            if !same_logical_id
                || !matches!(
                    source_job_status,
                    BatchStatus::Failed | BatchStatus::Stopped
                )
            {
                return Err(RepositoryError::PartitionStateCorrupt);
            }
            if !matches!(target_status, BatchStatus::Starting | BatchStatus::Started) {
                return Err(RepositoryError::PartitionParentNotActive {
                    step_execution_id: target_step_execution_id,
                    status: target_status,
                });
            }
            let target_exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM oxide_batch.ob_step_partition \
                 WHERE step_execution_id = $1)",
            )
            .bind(target_id)
            .fetch_one(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            if target_exists {
                return Err(RepositoryError::PartitionPlanExists {
                    step_execution_id: target_step_execution_id,
                });
            }
            let source_rows = sqlx::query(AssertSqlSafe(partition_select(
                "WHERE partition.step_execution_id = $1 \
                 ORDER BY partition.partition_ordinal FOR UPDATE",
            )))
            .bind(source_id)
            .fetch_all(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            if source_rows.is_empty() {
                return Err(RepositoryError::PartitionStateCorrupt);
            }
            let sources = source_rows
                .iter()
                .map(decode_step_partition)
                .collect::<Result<Vec<_>, _>>()?;
            let mut copied = Vec::with_capacity(sources.len());
            for source in sources {
                let context_payload = source
                    .context()
                    .payload_json()
                    .map_err(|_| RepositoryError::PartitionStateCorrupt)?;
                let payload = serde_json::from_slice::<Value>(&context_payload)
                    .map_err(|_| RepositoryError::PartitionStateCorrupt)?;
                let envelope = source
                    .context()
                    .to_json()
                    .map_err(|_| RepositoryError::PartitionStateCorrupt)?;
                let checksum: [u8; 32] = Sha256::digest(&envelope).into();
                let completed = source.status() == BatchStatus::Completed;
                if completed && source.worker_step_execution_id().is_none() {
                    return Err(RepositoryError::PartitionStateCorrupt);
                }
                let worker_id = source
                    .worker_step_execution_id()
                    .map(|id| database_id(id.get(), IdentifierKind::StepExecution))
                    .transpose()?;
                let counts = if completed {
                    source.counts()
                } else {
                    crate::ExecutionCounts::default()
                };
                let status = if completed {
                    BatchStatus::Completed
                } else {
                    BatchStatus::Starting
                };
                let exit = if completed {
                    Some(source.exit_status().code().as_str())
                } else {
                    None
                };
                let database_partition_id: i64 = sqlx::query_scalar(
                    "INSERT INTO oxide_batch.ob_step_partition \
                     (step_execution_id, worker_step_execution_id, partition_key, \
                      partition_ordinal, status, exit_code, read_count, processed_count, \
                      write_count, filter_count, commit_count, rollback_count, \
                      context_format, context_schema, context_schema_version, \
                      context_payload, context_checksum, version) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, \
                             $13, $14, $15, $16, $17, 0) RETURNING id",
                )
                .bind(target_id)
                .bind(if completed { worker_id } else { None })
                .bind(source.key().as_str())
                .bind(
                    i32::try_from(source.ordinal())
                        .map_err(|_| RepositoryError::PartitionStateCorrupt)?,
                )
                .bind(status.as_str())
                .bind(exit)
                .bind(partition_count(counts.read())?)
                .bind(partition_count(counts.processed())?)
                .bind(partition_count(counts.written())?)
                .bind(partition_count(counts.filtered())?)
                .bind(partition_count(counts.committed())?)
                .bind(partition_count(counts.rolled_back())?)
                .bind(
                    i16::try_from(source.context().format_version())
                        .map_err(|_| RepositoryError::PartitionStateCorrupt)?,
                )
                .bind(source.context().schema_id().as_str())
                .bind(
                    i32::try_from(source.context().schema_version().get())
                        .map_err(|_| RepositoryError::PartitionStateCorrupt)?,
                )
                .bind(Json(payload))
                .bind(&checksum[..])
                .fetch_one(&mut **self.transaction()?)
                .await
                .map_err(|_| RepositoryError::Unavailable)?;
                copied.push(StepPartition::from_snapshot(
                    StepPartitionId::new(
                        u64::try_from(database_partition_id)
                            .map_err(|_| RepositoryError::PartitionStateCorrupt)?,
                    )?,
                    target_step_execution_id,
                    if completed {
                        source.worker_step_execution_id()
                    } else {
                        None
                    },
                    source.key().clone(),
                    source.ordinal(),
                    status,
                    if completed {
                        source.exit_status().clone()
                    } else {
                        crate::ExitStatus::unknown()
                    },
                    counts,
                    source.context().clone(),
                    ExecutionVersion::INITIAL,
                ));
            }
            self.created_partition_plans
                .insert(target_step_execution_id);
            Ok(copied)
        })
    }

    #[allow(
        clippy::too_many_lines,
        reason = "parent, partition, and worker validation form one lock-ordered assignment rule"
    )]
    fn assign_step_partition(
        &mut self,
        id: StepPartitionId,
        expected_version: ExecutionVersion,
        worker_step_execution_id: StepExecutionId,
    ) -> BoxFuture<'_, Result<StepPartition, RepositoryError>> {
        Box::pin(async move {
            let database_partition_id = database_id(id.get(), IdentifierKind::StepPartition)?;
            let parent_id: i64 = sqlx::query_scalar(
                "SELECT step_execution_id FROM oxide_batch.ob_step_partition WHERE id = $1",
            )
            .bind(database_partition_id)
            .fetch_optional(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?
            .ok_or(RepositoryError::StepPartitionNotFound { id })?;
            let parent_status: String = sqlx::query_scalar(
                "SELECT status FROM oxide_batch.ob_step_execution WHERE id = $1 FOR UPDATE",
            )
            .bind(parent_id)
            .fetch_one(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            let parent_status = decode_status(&parent_status)?;
            if parent_status != BatchStatus::Started {
                return Err(RepositoryError::PartitionParentNotActive {
                    step_execution_id: StepExecutionId::new(
                        u64::try_from(parent_id)
                            .map_err(|_| RepositoryError::PartitionStateCorrupt)?,
                    )?,
                    status: parent_status,
                });
            }
            let row = sqlx::query(AssertSqlSafe(partition_select(
                "WHERE partition.id = $1 FOR UPDATE",
            )))
            .bind(database_partition_id)
            .fetch_optional(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?
            .ok_or(RepositoryError::StepPartitionNotFound { id })?;
            let mut partition = decode_step_partition(&row)?;
            if self
                .created_partition_plans
                .contains(&partition.step_execution_id())
            {
                return Err(RepositoryError::PartitionPlanNotCommitted {
                    step_execution_id: partition.step_execution_id(),
                });
            }
            partition
                .assign(expected_version, worker_step_execution_id)
                .map_err(|error| map_partition_mutation(id, error))?;
            let worker_id = database_id(
                worker_step_execution_id.get(),
                IdentifierKind::StepExecution,
            )?;
            let same_job: Option<bool> = sqlx::query_scalar(
                "SELECT parent.job_execution_id = worker.job_execution_id \
                 FROM oxide_batch.ob_step_execution parent \
                 CROSS JOIN oxide_batch.ob_step_execution worker \
                 WHERE parent.id = $1 AND worker.id = $2 FOR UPDATE OF worker",
            )
            .bind(parent_id)
            .bind(worker_id)
            .fetch_optional(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            match same_job {
                None => {
                    return Err(RepositoryError::StepExecutionNotFound {
                        id: worker_step_execution_id,
                    });
                }
                Some(same_job)
                    if !same_job || partition.step_execution_id() == worker_step_execution_id =>
                {
                    return Err(RepositoryError::PartitionWorkerMismatch {
                        partition_id: id,
                        worker_step_execution_id,
                    });
                }
                Some(true) => {}
                Some(false) => return Err(RepositoryError::PartitionStateCorrupt),
            }
            let worker_assigned: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM oxide_batch.ob_step_partition \
                 WHERE worker_step_execution_id = $1)",
            )
            .bind(worker_id)
            .fetch_one(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            if worker_assigned {
                return Err(RepositoryError::PartitionWorkerAlreadyAssigned {
                    worker_step_execution_id,
                });
            }
            let affected = sqlx::query(
                "UPDATE oxide_batch.ob_step_partition \
                 SET worker_step_execution_id = $1, status = 'STARTED', exit_code = NULL, \
                     read_count = 0, processed_count = 0, write_count = 0, filter_count = 0, \
                     commit_count = 0, rollback_count = 0, version = version + 1 \
                 WHERE id = $2 AND version = $3",
            )
            .bind(worker_id)
            .bind(database_partition_id)
            .bind(database_version(expected_version)?)
            .execute(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?
            .rows_affected();
            if affected != 1 {
                return Err(RepositoryError::ConcurrentModification);
            }
            Ok(partition)
        })
    }

    fn complete_step_partition(
        &mut self,
        id: StepPartitionId,
        expected_version: ExecutionVersion,
        worker_step_execution_id: StepExecutionId,
    ) -> BoxFuture<'_, Result<StepPartition, RepositoryError>> {
        Box::pin(async move {
            let database_partition_id = database_id(id.get(), IdentifierKind::StepPartition)?;
            let parent_id: i64 = sqlx::query_scalar(
                "SELECT step_execution_id FROM oxide_batch.ob_step_partition WHERE id = $1",
            )
            .bind(database_partition_id)
            .fetch_optional(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?
            .ok_or(RepositoryError::StepPartitionNotFound { id })?;
            let parent_row = sqlx::query(AssertSqlSafe(step_execution_select(
                "WHERE execution.id = $1 FOR UPDATE",
            )))
            .bind(parent_id)
            .fetch_one(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            let parent = decode_step_execution(&parent_row)?;
            if !matches!(
                parent.metadata().status(),
                BatchStatus::Started | BatchStatus::Stopping
            ) {
                return Err(RepositoryError::PartitionParentNotActive {
                    step_execution_id: parent.id(),
                    status: parent.metadata().status(),
                });
            }
            let row = sqlx::query(AssertSqlSafe(partition_select(
                "WHERE partition.id = $1 FOR UPDATE",
            )))
            .bind(database_partition_id)
            .fetch_optional(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?
            .ok_or(RepositoryError::StepPartitionNotFound { id })?;
            let mut partition = decode_step_partition(&row)?;
            if partition.worker_step_execution_id() != Some(worker_step_execution_id) {
                return Err(RepositoryError::PartitionWorkerStale {
                    partition_id: id,
                    worker_step_execution_id,
                });
            }
            let worker_id = database_id(
                worker_step_execution_id.get(),
                IdentifierKind::StepExecution,
            )?;
            let worker_row = sqlx::query(AssertSqlSafe(step_execution_select(
                "WHERE execution.id = $1 FOR UPDATE",
            )))
            .bind(worker_id)
            .fetch_optional(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?
            .ok_or(RepositoryError::StepExecutionNotFound {
                id: worker_step_execution_id,
            })?;
            let worker = decode_step_execution(&worker_row)?;
            if worker.job_execution_id() != parent.job_execution_id() {
                return Err(RepositoryError::PartitionWorkerMismatch {
                    partition_id: id,
                    worker_step_execution_id,
                });
            }
            let result = PartitionResult::from_worker(&worker).map_err(|_| {
                RepositoryError::PartitionAggregationIncomplete {
                    step_execution_id: parent.id(),
                    status: worker.metadata().status(),
                }
            })?;
            partition
                .complete(expected_version, &result)
                .map_err(|error| map_partition_mutation(id, error))?;
            let counts = result.counts();
            let affected = sqlx::query(
                "UPDATE oxide_batch.ob_step_partition \
                 SET status = $1, exit_code = $2, read_count = $3, processed_count = $4, \
                     write_count = $5, filter_count = $6, commit_count = $7, \
                     rollback_count = $8, version = version + 1 \
                 WHERE id = $9 AND version = $10",
            )
            .bind(result.status().as_str())
            .bind(result.exit_status().code().as_str())
            .bind(partition_count(counts.read())?)
            .bind(partition_count(counts.processed())?)
            .bind(partition_count(counts.written())?)
            .bind(partition_count(counts.filtered())?)
            .bind(partition_count(counts.committed())?)
            .bind(partition_count(counts.rolled_back())?)
            .bind(database_partition_id)
            .bind(database_version(expected_version)?)
            .execute(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?
            .rows_affected();
            if affected != 1 {
                return Err(RepositoryError::ConcurrentModification);
            }
            Ok(partition)
        })
    }

    fn aggregate_step_partitions(
        &mut self,
        step_execution_id: StepExecutionId,
        expected_version: ExecutionVersion,
        transitioned_at: SystemTime,
    ) -> BoxFuture<'_, Result<StepExecution, RepositoryError>> {
        Box::pin(async move {
            let parent_id = database_id(step_execution_id.get(), IdentifierKind::StepExecution)?;
            let parent_row = sqlx::query(AssertSqlSafe(step_execution_select(
                "WHERE execution.id = $1 FOR UPDATE",
            )))
            .bind(parent_id)
            .fetch_optional(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?
            .ok_or(RepositoryError::StepExecutionNotFound {
                id: step_execution_id,
            })?;
            let parent = decode_step_execution(&parent_row)?;
            let rows = sqlx::query(AssertSqlSafe(partition_select(
                "WHERE partition.step_execution_id = $1 \
                 ORDER BY partition.partition_key FOR UPDATE",
            )))
            .bind(parent_id)
            .fetch_all(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            let partitions = rows
                .iter()
                .map(decode_step_partition)
                .collect::<Result<Vec<_>, _>>()?;
            let aggregate = crate::aggregate_step_partitions(&partitions)
                .map_err(|error| map_partition_aggregation(step_execution_id, error))?;
            for partition in &partitions {
                let worker_id = partition.worker_step_execution_id().ok_or(
                    RepositoryError::PartitionAggregationIncomplete {
                        step_execution_id,
                        status: partition.status(),
                    },
                )?;
                let worker_row = sqlx::query(AssertSqlSafe(step_execution_select(
                    "WHERE execution.id = $1 FOR UPDATE",
                )))
                .bind(database_id(worker_id.get(), IdentifierKind::StepExecution)?)
                .fetch_optional(&mut **self.transaction()?)
                .await
                .map_err(|_| RepositoryError::Unavailable)?
                .ok_or(RepositoryError::PartitionStateCorrupt)?;
                let worker = decode_step_execution(&worker_row)?;
                if worker.metadata().status() != partition.status()
                    || worker.metadata().exit_status() != partition.exit_status()
                    || worker.metadata().counts() != partition.counts()
                {
                    return Err(RepositoryError::PartitionStateCorrupt);
                }
            }
            let selected_worker = self
                .step_execution(aggregate.selected_worker_step_execution_id())
                .await?
                .ok_or(RepositoryError::PartitionStateCorrupt)?;
            let failure = selected_worker.metadata().failure();
            if let Some(next) = expected_version.get().checked_add(1)
                && parent.version().get() == next
                && parent.metadata().status() == aggregate.status()
                && parent.metadata().exit_status() == aggregate.exit_status()
                && parent.metadata().counts() == aggregate.counts()
                && parent.metadata().failure() == failure
            {
                return Ok(parent);
            }
            if !matches!(
                parent.metadata().status(),
                BatchStatus::Started | BatchStatus::Stopping
            ) {
                return Err(RepositoryError::PartitionParentNotActive {
                    step_execution_id,
                    status: parent.metadata().status(),
                });
            }
            let aggregated = aggregate_partition_parent(
                &parent,
                expected_version,
                &aggregate,
                transitioned_at,
                failure,
            )?;
            let affected = update_step_execution(
                &mut **self.transaction()?,
                &aggregated,
                transitioned_at,
                expected_version,
            )
            .await?;
            if affected != 1 {
                return Err(self
                    .classify_step_cas(step_execution_id, expected_version)
                    .await);
            }
            Ok(aggregated)
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
                  to_timestamp($8::double precision / 1000.0)) \
                 RETURNING id",
            )
            .bind(database_execution_id)
            .bind(prior_version)
            .bind(prior.metadata().status().to_string())
            .bind(&resulting_status)
            .bind(request.reason_code())
            .bind(request.operator_reference())
            .bind(&request.evidence_digest()[..])
            .bind(decided_ms)
            .fetch_one(&mut **self.transaction()?)
            .await;
            if insert.is_err() {
                rollback_recovery_savepoint(&mut **self.transaction()?).await;
                return Err(RepositoryError::ConcurrentModification);
            }
            sqlx::query("RELEASE SAVEPOINT ob_recovery_decision")
                .execute(&mut **self.transaction()?)
                .await
                .map_err(|_| RepositoryError::Unavailable)?;
            let inserted = insert.map_err(|_| RepositoryError::Unavailable)?;
            let decision = RecoveryDecision::new(
                RecoveryDecisionId::new(read_u64(&inserted, "id")?)?,
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
                "SELECT id, execution_version, prior_status, resulting_status, reason_code, \
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

    fn find_operator_request<'a>(
        &'a mut self,
        action: OperatorAction,
        operation_id: &'a OperationId,
    ) -> BoxFuture<'a, Result<Option<OperatorRecord>, RepositoryError>> {
        Box::pin(async move {
            let row = sqlx::query(AssertSqlSafe(operator_request_select(
                "WHERE request.action = $1 AND request.operation_id = $2",
            )))
            .bind(action.as_str())
            .bind(operation_id.as_str())
            .fetch_optional(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            row.as_ref().map(decode_operator_record).transpose()
        })
    }

    fn append_operator_request<'a>(
        &'a mut self,
        draft: &'a OperatorRecordDraft,
    ) -> BoxFuture<'a, Result<OperatorRecord, RepositoryError>> {
        Box::pin(async move {
            let instance_id = draft
                .job_instance_id()
                .map(|id| database_id(id.get(), IdentifierKind::JobInstance))
                .transpose()?;
            let execution_id = draft
                .job_execution_id()
                .map(|id| database_id(id.get(), IdentifierKind::JobExecution))
                .transpose()?;
            let requested_ms = system_time_millis(draft.requested_at())?;
            let row = sqlx::query(
                "INSERT INTO oxide_batch.ob_operator_request \
                 (job_instance_id, job_execution_id, action, authorization_class, \
                  operation_id, actor_ref, reason_code, request_digest, observed_version, \
                  prior_status, result_status, outcome_class, rejection_code, requested_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, \
                  to_timestamp($14::double precision / 1000.0)) \
                 RETURNING id",
            )
            .bind(instance_id)
            .bind(execution_id)
            .bind(draft.action().as_str())
            .bind(draft.action().authorization_class().as_str())
            .bind(draft.operation_id().as_str())
            .bind(draft.actor().as_str())
            .bind(draft.reason().map(ReasonCode::as_str))
            .bind(&draft.digest().as_bytes()[..])
            .bind(draft.observed_version().map(database_version).transpose()?)
            .bind(draft.prior_status().map(BatchStatus::as_str))
            .bind(draft.result_status().map(BatchStatus::as_str))
            .bind(draft.outcome().as_str())
            .bind(draft.rejection().map(OperatorRejection::as_str))
            .bind(requested_ms)
            .fetch_one(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::ConcurrentModification)?;
            let id = OperatorRequestId::new(read_u64(&row, "id")?)?;
            Ok(OperatorRecord::from_parts(id, draft.clone()))
        })
    }

    fn request_execution_stop<'a>(
        &'a mut self,
        id: JobExecutionId,
        expected_version: ExecutionVersion,
        actor: &'a ActorRef,
        requested_at: SystemTime,
    ) -> BoxFuture<'a, Result<JobExecution, RepositoryError>> {
        Box::pin(async move {
            let database_execution_id = database_id(id.get(), IdentifierKind::JobExecution)?;
            let requested_ms = system_time_millis(requested_at)?;
            let affected = sqlx::query(
                "UPDATE oxide_batch.ob_job_execution \
                 SET stop_requested_at = to_timestamp($1::double precision / 1000.0), \
                     stop_requested_by = $2, \
                     updated_at = greatest(updated_at, \
                        to_timestamp($1::double precision / 1000.0)) \
                 WHERE id = $3 AND version = $4",
            )
            .bind(requested_ms)
            .bind(actor.as_str())
            .bind(database_execution_id)
            .bind(database_version(expected_version)?)
            .execute(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?
            .rows_affected();
            if affected != 1 {
                return Err(self.classify_job_cas(id, expected_version).await);
            }
            self.job_execution(id)
                .await?
                .ok_or(RepositoryError::JobExecutionNotFound { id })
        })
    }

    fn claim_execution_owner<'a>(
        &'a mut self,
        id: JobExecutionId,
        expected_version: ExecutionVersion,
        owner: &'a crate::OwnerToken,
        claimed_at: SystemTime,
    ) -> BoxFuture<'a, Result<JobExecution, RepositoryError>> {
        Box::pin(async move {
            let database_execution_id = database_id(id.get(), IdentifierKind::JobExecution)?;
            let claimed_ms = system_time_millis(claimed_at)?;
            let affected = sqlx::query(
                "UPDATE oxide_batch.ob_job_execution \
                 SET owner_token = $1, updated_at = greatest(updated_at, \
                       to_timestamp($2::double precision / 1000.0)) \
                 WHERE id = $3 AND version = $4 AND status = 'STARTING' \
                   AND (owner_token IS NULL OR owner_token = $1)",
            )
            .bind(&owner.as_bytes()[..])
            .bind(claimed_ms)
            .bind(database_execution_id)
            .bind(database_version(expected_version)?)
            .execute(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?
            .rows_affected();
            if affected != 1 {
                let row = sqlx::query(
                    "SELECT version, owner_token, status \
                     FROM oxide_batch.ob_job_execution WHERE id = $1",
                )
                .bind(database_execution_id)
                .fetch_optional(&mut **self.transaction()?)
                .await
                .map_err(|_| RepositoryError::Unavailable)?
                .ok_or(RepositoryError::JobExecutionNotFound { id })?;
                let actual = ExecutionVersion::new(read_u64(&row, "version")?);
                if actual != expected_version {
                    return Err(RepositoryError::Lifecycle(LifecycleError::StaleVersion {
                        expected: expected_version,
                        actual,
                    }));
                }
                let status = decode_status(&read_text(&row, "status")?)?;
                if status != BatchStatus::Starting {
                    return Err(RepositoryError::ExecutionOwnershipNotAllowed { id, status });
                }
                return Err(RepositoryError::ExecutionOwned { id });
            }
            self.job_execution(id)
                .await?
                .ok_or(RepositoryError::JobExecutionNotFound { id })
        })
    }

    fn observe_execution_control<'a>(
        &'a mut self,
        id: JobExecutionId,
        owner: &'a crate::OwnerToken,
        observed_at: SystemTime,
    ) -> BoxFuture<'a, Result<crate::ExecutionControl, RepositoryError>> {
        Box::pin(async move {
            let database_execution_id = database_id(id.get(), IdentifierKind::JobExecution)?;
            let row = sqlx::query(
                "SELECT owner_token = $2 AS owner_matches, \
                        stop_requested_at IS NOT NULL AS stop_requested \
                 FROM oxide_batch.ob_job_execution WHERE id = $1 FOR UPDATE",
            )
            .bind(database_execution_id)
            .bind(&owner.as_bytes()[..])
            .fetch_optional(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?
            .ok_or(RepositoryError::JobExecutionNotFound { id })?;
            let owner_matches = row
                .try_get::<Option<bool>, _>("owner_matches")
                .map_err(|_| RepositoryError::Unavailable)?
                .unwrap_or(false);
            let stop_requested = row
                .try_get::<bool, _>("stop_requested")
                .map_err(|_| RepositoryError::Unavailable)?;
            let mut execution = self
                .job_execution(id)
                .await?
                .ok_or(RepositoryError::JobExecutionNotFound { id })?;
            if owner_matches
                && stop_requested
                && matches!(
                    execution.metadata().status(),
                    BatchStatus::Starting | BatchStatus::Started
                )
            {
                execution = self
                    .transition_job_execution(
                        id,
                        execution.version(),
                        LifecycleTransition::new(BatchStatus::Stopping, observed_at),
                    )
                    .await?;
            }
            Ok(crate::ExecutionControl::new(
                execution,
                owner_matches,
                stop_requested,
            ))
        })
    }

    fn job_instance_hold(
        &mut self,
        id: JobInstanceId,
    ) -> BoxFuture<'_, Result<Option<RetentionHold>, RepositoryError>> {
        Box::pin(async move {
            let database_instance_id = database_id(id.get(), IdentifierKind::JobInstance)?;
            let row = sqlx::query(
                "SELECT hold_actor, hold_reason, \
                 (extract(epoch FROM hold_placed_at) * 1000)::bigint AS placed_ms \
                 FROM oxide_batch.ob_job_instance WHERE id = $1",
            )
            .bind(database_instance_id)
            .fetch_optional(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?
            .ok_or(RepositoryError::JobInstanceNotFound { id })?;
            decode_retention_hold(id, &row)
        })
    }

    fn place_instance_hold<'a>(
        &'a mut self,
        id: JobInstanceId,
        actor: &'a ActorRef,
        reason: &'a ReasonCode,
        placed_at: SystemTime,
    ) -> BoxFuture<'a, Result<RetentionHold, RepositoryError>> {
        Box::pin(async move {
            let database_instance_id = database_id(id.get(), IdentifierKind::JobInstance)?;
            let placed_ms = system_time_millis(placed_at)?;
            let affected = sqlx::query(
                "UPDATE oxide_batch.ob_job_instance \
                 SET hold_actor = $1, hold_reason = $2, \
                     hold_placed_at = to_timestamp($3::double precision / 1000.0) \
                 WHERE id = $4",
            )
            .bind(actor.as_str())
            .bind(reason.as_str())
            .bind(placed_ms)
            .bind(database_instance_id)
            .execute(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?
            .rows_affected();
            if affected != 1 {
                return Err(RepositoryError::JobInstanceNotFound { id });
            }
            Ok(RetentionHold::new(
                id,
                actor.clone(),
                reason.clone(),
                placed_at,
            ))
        })
    }

    fn release_instance_hold(
        &mut self,
        id: JobInstanceId,
    ) -> BoxFuture<'_, Result<Option<RetentionHold>, RepositoryError>> {
        Box::pin(async move {
            let existing = self.job_instance_hold(id).await?;
            let database_instance_id = database_id(id.get(), IdentifierKind::JobInstance)?;
            sqlx::query(
                "UPDATE oxide_batch.ob_job_instance \
                 SET hold_actor = NULL, hold_reason = NULL, hold_placed_at = NULL \
                 WHERE id = $1",
            )
            .bind(database_instance_id)
            .execute(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            Ok(existing)
        })
    }

    fn find_retention_action<'a>(
        &'a mut self,
        action: RetentionAction,
        operation_id: &'a OperationId,
    ) -> BoxFuture<'a, Result<Option<RetentionRecord>, RepositoryError>> {
        Box::pin(async move {
            let row = sqlx::query(AssertSqlSafe(retention_action_select(
                "WHERE retention.action = $1 AND retention.operation_id = $2",
            )))
            .bind(action.as_str())
            .bind(operation_id.as_str())
            .fetch_optional(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            row.as_ref().map(decode_retention_record).transpose()
        })
    }

    fn append_retention_action<'a>(
        &'a mut self,
        draft: &'a RetentionRecordDraft,
    ) -> BoxFuture<'a, Result<RetentionRecord, RepositoryError>> {
        Box::pin(async move {
            let instance_id = draft
                .job_instance_id()
                .map(|id| database_id(id.get(), IdentifierKind::JobInstance))
                .transpose()?;
            let applied_ms = system_time_millis(draft.applied_at())?;
            let counts = draft.counts();
            let row = sqlx::query(
                "INSERT INTO oxide_batch.ob_retention_action \
                 (job_instance_id, action, operation_id, actor_ref, reason_code, \
                  plan_digest, batch_bound, deleted_flow_decisions, \
                  deleted_recovery_decisions, deleted_operator_requests, \
                  deleted_step_partitions, deleted_step_executions, \
                  deleted_job_executions, deleted_job_instances, outcome_class, \
                  applied_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, \
                  $15, to_timestamp($16::double precision / 1000.0)) \
                 RETURNING id",
            )
            .bind(instance_id)
            .bind(draft.action().as_str())
            .bind(draft.operation_id().as_str())
            .bind(draft.actor().as_str())
            .bind(draft.reason().as_str())
            .bind(draft.plan_digest().map(|digest| digest.to_vec()))
            .bind(
                draft
                    .batch_bound()
                    .map(|bound| i32::try_from(bound.get()))
                    .transpose()
                    .map_err(|_| RepositoryError::Unavailable)?,
            )
            .bind(retention_count(counts.flow_decisions())?)
            .bind(retention_count(counts.recovery_decisions())?)
            .bind(retention_count(counts.operator_requests())?)
            .bind(retention_count(counts.step_partitions())?)
            .bind(retention_count(counts.step_executions())?)
            .bind(retention_count(counts.job_executions())?)
            .bind(retention_count(counts.job_instances())?)
            .bind(draft.outcome().as_str())
            .bind(applied_ms)
            .fetch_one(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::ConcurrentModification)?;
            let id = RetentionActionId::new(read_u64(&row, "id")?)?;
            Ok(RetentionRecord::from_parts(id, draft.clone()))
        })
    }

    fn purge_survey<'a>(
        &'a mut self,
        request: &'a PurgePlanRequest,
    ) -> BoxFuture<'a, Result<PurgeSurvey, RepositoryError>> {
        Box::pin(async move {
            let statuses = request
                .statuses()
                .iter()
                .map(|status| status.as_str().to_owned())
                .collect::<Vec<_>>();
            let now = self.repository.clock.now();
            let threshold = system_time_millis(
                now.checked_sub(request.minimum_age())
                    .ok_or(RepositoryError::Unavailable)?,
            )?;
            let limit = i64::from(request.batch().get());
            let rows = sqlx::query(
                "SELECT execution.job_instance_id, execution.id, execution.version \
                 FROM oxide_batch.ob_job_execution execution \
                 JOIN oxide_batch.ob_job_instance instance \
                   ON instance.id = execution.job_instance_id \
                 WHERE instance.job_name = $1 \
                   AND instance.hold_actor IS NULL \
                   AND execution.status = ANY($2) \
                   AND execution.updated_at < to_timestamp($3::double precision / 1000.0) \
                   AND NOT EXISTS ( \
                     SELECT 1 FROM oxide_batch.ob_job_execution sibling \
                     WHERE sibling.job_instance_id = execution.job_instance_id \
                       AND sibling.status IN ('STARTING', 'STARTED', 'STOPPING', 'UNKNOWN')) \
                 ORDER BY execution.job_instance_id, execution.id \
                 LIMIT $4",
            )
            .bind(request.job_name().as_str())
            .bind(&statuses)
            .bind(threshold)
            .bind(limit)
            .fetch_all(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            let mut candidates = Vec::with_capacity(rows.len());
            for row in &rows {
                candidates.push(PurgeCandidate::new(
                    JobInstanceId::new(read_u64(row, "job_instance_id")?)?,
                    JobExecutionId::new(read_u64(row, "id")?)?,
                    ExecutionVersion::new(read_u64(row, "version")?),
                ));
            }
            let counts = self.purge_counts(&candidates).await?;
            Ok(PurgeSurvey::new(candidates, counts))
        })
    }

    #[allow(clippy::too_many_lines)]
    fn apply_purge<'a>(
        &'a mut self,
        plan: &'a PurgePlan,
    ) -> BoxFuture<'a, Result<PurgeCounts, RepositoryError>> {
        Box::pin(async move {
            let executions = candidate_execution_ids(plan.candidates())?;
            let instances = candidate_instance_ids(plan.candidates())?;
            let versions = plan
                .candidates()
                .iter()
                .map(|candidate| database_version(candidate.version()))
                .collect::<Result<Vec<_>, _>>()?;
            let statuses = plan
                .request()
                .statuses()
                .iter()
                .map(|status| status.as_str().to_owned())
                .collect::<Vec<_>>();
            let confirmed = sqlx::query(
                "SELECT count(*) AS matched \
                 FROM unnest($1::bigint[], $2::bigint[]) AS candidate(id, version) \
                 JOIN oxide_batch.ob_job_execution execution \
                   ON execution.id = candidate.id AND execution.version = candidate.version \
                 JOIN oxide_batch.ob_job_instance instance \
                   ON instance.id = execution.job_instance_id \
                 WHERE instance.hold_actor IS NULL \
                   AND execution.status = ANY($3) \
                   AND NOT EXISTS ( \
                     SELECT 1 FROM oxide_batch.ob_job_execution sibling \
                     WHERE sibling.job_instance_id = execution.job_instance_id \
                       AND sibling.status IN ('STARTING', 'STARTED', 'STOPPING', 'UNKNOWN'))",
            )
            .bind(&executions)
            .bind(&versions)
            .bind(&statuses)
            .fetch_one(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            if read_u64(&confirmed, "matched")?
                != u64::try_from(executions.len()).unwrap_or(u64::MAX)
            {
                return Err(RepositoryError::RetentionPlanStale);
            }
            // A surviving decision may cite a purged decision as its reused
            // provenance. The citation is cleared before the target row is
            // removed, because the evidence it names no longer exists.
            sqlx::query(
                "UPDATE oxide_batch.ob_flow_decision SET reused_decision_id = NULL \
                 WHERE reused_decision_id IN ( \
                   SELECT id FROM oxide_batch.ob_flow_decision \
                   WHERE job_execution_id = ANY($1)) \
                   AND job_execution_id <> ALL($1)",
            )
            .bind(&executions)
            .execute(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            let flow_decisions = self
                .purge_delete(
                    "DELETE FROM oxide_batch.ob_flow_decision WHERE job_execution_id = ANY($1)",
                    &executions,
                )
                .await?;
            let recovery_decisions = self
                .purge_delete(
                    "DELETE FROM oxide_batch.ob_recovery_decision WHERE job_execution_id = ANY($1)",
                    &executions,
                )
                .await?;
            let operator_requests = self
                .purge_delete(
                    "DELETE FROM oxide_batch.ob_operator_request WHERE job_execution_id = ANY($1)",
                    &executions,
                )
                .await?;
            let step_partitions = self
                .purge_delete(
                    "DELETE FROM oxide_batch.ob_step_partition WHERE step_execution_id IN ( \
                     SELECT id FROM oxide_batch.ob_step_execution \
                     WHERE job_execution_id = ANY($1))",
                    &executions,
                )
                .await?;
            let step_executions = self
                .purge_delete(
                    "DELETE FROM oxide_batch.ob_step_execution WHERE job_execution_id = ANY($1)",
                    &executions,
                )
                .await?;
            let job_executions = self
                .purge_delete(
                    "DELETE FROM oxide_batch.ob_job_execution WHERE id = ANY($1)",
                    &executions,
                )
                .await?;
            let orphaned_requests = self
                .purge_delete(
                    "DELETE FROM oxide_batch.ob_operator_request \
                     WHERE job_execution_id IS NULL AND job_instance_id = ANY($1) \
                       AND NOT EXISTS ( \
                         SELECT 1 FROM oxide_batch.ob_job_execution execution \
                         WHERE execution.job_instance_id \
                           = oxide_batch.ob_operator_request.job_instance_id)",
                    &instances,
                )
                .await?;
            // Retention audit outlives the instance it protected, so its
            // reference is cleared rather than cascading the row away.
            sqlx::query(
                "UPDATE oxide_batch.ob_retention_action SET job_instance_id = NULL \
                 WHERE job_instance_id = ANY($1) AND NOT EXISTS ( \
                   SELECT 1 FROM oxide_batch.ob_job_execution execution \
                   WHERE execution.job_instance_id \
                     = oxide_batch.ob_retention_action.job_instance_id)",
            )
            .bind(&instances)
            .execute(&mut **self.transaction()?)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
            let job_instances = self
                .purge_delete(
                    "DELETE FROM oxide_batch.ob_job_instance WHERE id = ANY($1) \
                     AND NOT EXISTS ( \
                       SELECT 1 FROM oxide_batch.ob_job_execution execution \
                       WHERE execution.job_instance_id = oxide_batch.ob_job_instance.id)",
                    &instances,
                )
                .await?;
            Ok(PurgeCounts::new(
                flow_decisions,
                recovery_decisions,
                operator_requests.saturating_add(orphaned_requests),
                step_partitions,
                step_executions,
                job_executions,
                job_instances,
            ))
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

/// Upserts one namespaced `ItemStream` candidate envelope in the same
/// transaction as the enclosing chunk commit.
///
/// `ob_component_state.version` is an audit counter, not a concurrency guard:
/// the enclosing `ob_step_execution` optimistic-version check already
/// serializes every commit for this step execution, so a second per-namespace
/// guard here would be redundant with, not additional to, that protection.
type ComponentStatePayloadColumns = (&'static str, Option<Vec<u8>>, Option<Vec<u8>>, Option<i64>);

const COMPONENT_STATE_UPSERT: &str = "\
    INSERT INTO oxide_batch.ob_component_state (\
        step_execution_id, namespace, schema_id, schema_version, codec_id, codec_version, \
        checksum_algorithm, checksum_algorithm_version, checksum, payload_kind, payload, \
        external_content_id, external_encoded_len, version, updated_at\
    ) VALUES (\
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, 0, \
        to_timestamp($14::double precision / 1000.0)\
    ) \
    ON CONFLICT (step_execution_id, namespace) DO UPDATE SET \
        schema_id = EXCLUDED.schema_id, \
        schema_version = EXCLUDED.schema_version, \
        codec_id = EXCLUDED.codec_id, \
        codec_version = EXCLUDED.codec_version, \
        checksum_algorithm = EXCLUDED.checksum_algorithm, \
        checksum_algorithm_version = EXCLUDED.checksum_algorithm_version, \
        checksum = EXCLUDED.checksum, \
        payload_kind = EXCLUDED.payload_kind, \
        payload = EXCLUDED.payload, \
        external_content_id = EXCLUDED.external_content_id, \
        external_encoded_len = EXCLUDED.external_encoded_len, \
        version = oxide_batch.ob_component_state.version + 1, \
        updated_at = EXCLUDED.updated_at";

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

    fn commit(
        &mut self,
        counts: ChunkCounts,
        fault: ChunkFaultProgress,
    ) -> BoxFuture<'_, Result<ChunkCommitReceipt, ChunkTransactionError>> {
        self.commit_with_component_state(counts, fault, &[])
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the atomic business/progress bind and commit boundary remains visible"
    )]
    fn commit_with_component_state<'a>(
        &'a mut self,
        counts: ChunkCounts,
        fault: ChunkFaultProgress,
        component_state: &'a [ComponentStateEnvelope],
    ) -> BoxFuture<'a, Result<ChunkCommitReceipt, ChunkTransactionError>> {
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
            let step_id = database_id(
                self.context.step_execution_id().get(),
                IdentifierKind::StepExecution,
            )
            .map_err(|_| ChunkTransactionError::NotCommitted)?;
            let job_id = database_id(
                self.context.job_execution_id().get(),
                IdentifierKind::JobExecution,
            )
            .map_err(|_| ChunkTransactionError::NotCommitted)?;
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
            .bind(step_id)
            .bind(job_id)
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

            for envelope in component_state {
                let schema_version = i32::try_from(envelope.schema_version().get())
                    .map_err(|_| ChunkTransactionError::NotCommitted)?;
                let codec_version = i32::try_from(envelope.codec_version().get())
                    .map_err(|_| ChunkTransactionError::NotCommitted)?;
                let checksum_algorithm = i16::try_from(envelope.checksum_algorithm())
                    .map_err(|_| ChunkTransactionError::NotCommitted)?;
                let checksum_algorithm_version =
                    i16::try_from(envelope.checksum_algorithm_version())
                        .map_err(|_| ChunkTransactionError::NotCommitted)?;
                let checksum = envelope.checksum().to_vec();
                let payload = envelope
                    .payload()
                    .map_err(|_| ChunkTransactionError::NotCommitted)?;
                // `payload` binds to `bytea`: the exact codec-produced bytes
                // are stored and later read back verbatim, so the checksum
                // recomputed in `ComponentStateEnvelope::from_durable` always
                // matches the checksum computed at encode time -- no `jsonb`
                // round-trip can reserialize the payload in between.
                let (payload_kind, payload_bytes, external_content_id, external_encoded_len): ComponentStatePayloadColumns =
                    match payload {
                    ComponentStatePayload::Inline(bytes) => ("INLINE", Some(bytes), None, None),
                    ComponentStatePayload::External(reference) => {
                        let encoded_len = i64::try_from(reference.encoded_len())
                            .map_err(|_| ChunkTransactionError::NotCommitted)?;
                        (
                            "EXTERNAL",
                            None,
                            Some(reference.content_id().as_bytes().to_vec()),
                            Some(encoded_len),
                        )
                    }
                };
                let updated_at_millis = system_time_millis(self.clock.now())
                    .map_err(|_| ChunkTransactionError::NotCommitted)?;
                let result = sqlx::query(COMPONENT_STATE_UPSERT)
                    .bind(step_id)
                    .bind(envelope.namespace().as_str())
                    .bind(envelope.schema_id().as_str())
                    .bind(schema_version)
                    .bind(envelope.codec_id().as_str())
                    .bind(codec_version)
                    .bind(checksum_algorithm)
                    .bind(checksum_algorithm_version)
                    .bind(checksum)
                    .bind(payload_kind)
                    .bind(payload_bytes)
                    .bind(external_content_id)
                    .bind(external_encoded_len)
                    .bind(updated_at_millis)
                    .execute(&mut **self.connection()?)
                    .await;
                let Ok(result) = result else {
                    rollback_chunk_transaction(&mut self.connection).await;
                    return Err(ChunkTransactionError::NotCommitted);
                };
                if result.rows_affected() != 1 {
                    rollback_chunk_transaction(&mut self.connection).await;
                    return Err(ChunkTransactionError::NotCommitted);
                }
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
            // `ParameterValueKind` is `#[non_exhaustive]`. A kind this adapter
            // cannot spell is rejected rather than written under a guessed
            // durable tag, because the encoding selects the instance key.
            _ => return Err(RepositoryError::Unavailable),
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

/// Copies every committed `ItemStream` component-state row from a
/// predecessor step execution forward to a newly created restart attempt.
///
/// `ob_component_state` is a separate table keyed by `(step_execution_id,
/// namespace)`, unlike the checkpoint/context/fault-state columns that live
/// inline on `ob_step_execution` and are already copied forward by the
/// `INSERT ... SELECT ... FROM ob_step_execution source` restart statements
/// in [`PostgresUnitOfWork::create_step_execution`] and
/// [`PostgresUnitOfWork::create_flow_step_execution`]. Without this, a
/// genuinely new `step_execution_id` would find no committed component
/// state at all, even though its predecessor committed some: a real restart
/// must inherit the last committed envelope per namespace exactly as it
/// inherits the checkpoint. Runs in the same transaction as the
/// step-execution row it accompanies, so the new step execution and its
/// inherited component state become visible atomically.
async fn copy_forward_component_state(
    transaction: &mut PgConnection,
    source_step_execution_id: i64,
    target_step_execution_id: i64,
) -> Result<(), RepositoryError> {
    sqlx::query(
        "INSERT INTO oxide_batch.ob_component_state ( \
             step_execution_id, namespace, schema_id, schema_version, codec_id, \
             codec_version, checksum_algorithm, checksum_algorithm_version, checksum, \
             payload_kind, payload, external_content_id, external_encoded_len, \
             version, updated_at) \
         SELECT $1, namespace, schema_id, schema_version, codec_id, codec_version, \
             checksum_algorithm, checksum_algorithm_version, checksum, payload_kind, \
             payload, external_content_id, external_encoded_len, 0, updated_at \
         FROM oxide_batch.ob_component_state \
         WHERE step_execution_id = $2",
    )
    .bind(target_step_execution_id)
    .bind(source_step_execution_id)
    .execute(&mut *transaction)
    .await
    .map_err(|_| RepositoryError::Unavailable)?;
    Ok(())
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
    oxide_batch_core::check_manifest_format(definition.manifest_format()).map_err(|_| {
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

fn partition_select(suffix: &str) -> String {
    format!(
        "SELECT partition.id, partition.step_execution_id, \
         partition.worker_step_execution_id, partition.partition_key, \
         partition.partition_ordinal, partition.status, partition.exit_code, \
         partition.read_count, partition.processed_count, partition.write_count, \
         partition.filter_count, partition.commit_count, partition.rollback_count, \
         partition.context_format, partition.context_schema, \
         partition.context_schema_version, partition.context_payload, \
         partition.context_checksum, partition.version \
         FROM oxide_batch.ob_step_partition partition {suffix}"
    )
}

fn decode_step_partition(row: &PgRow) -> Result<StepPartition, RepositoryError> {
    let id = StepPartitionId::new(read_u64(row, "id")?)?;
    let step_execution_id = StepExecutionId::new(read_u64(row, "step_execution_id")?)?;
    let worker_step_execution_id = read_optional_u64(row, "worker_step_execution_id")?
        .map(StepExecutionId::new)
        .transpose()?;
    let key = PartitionKey::new(read_text(row, "partition_key")?)
        .map_err(|_| RepositoryError::PartitionStateCorrupt)?;
    let ordinal = row
        .try_get::<i32, _>("partition_ordinal")
        .map_err(|_| RepositoryError::PartitionStateCorrupt)
        .and_then(|value| {
            u32::try_from(value).map_err(|_| RepositoryError::PartitionStateCorrupt)
        })?;
    if ordinal == 0 || ordinal > u32::from(MAX_PARTITIONS) {
        return Err(RepositoryError::PartitionStateCorrupt);
    }
    let status = decode_status(&read_text(row, "status")?)?;
    let exit_status = ExitStatus::new(
        ExitCode::new(
            read_optional_text(row, "exit_code")?.unwrap_or_else(|| String::from("UNKNOWN")),
        )
        .map_err(|_| RepositoryError::PartitionStateCorrupt)?,
    );
    let counts = ExecutionCounts::new(
        read_u64(row, "read_count")?,
        read_u64(row, "processed_count")?,
        read_u64(row, "write_count")?,
        read_u64(row, "filter_count")?,
        read_u64(row, "commit_count")?,
        read_u64(row, "rollback_count")?,
    );
    let context = decode_partition_context(row)?;
    let version = ExecutionVersion::new(read_u64(row, "version")?);
    if matches!(status, BatchStatus::Starting) && worker_step_execution_id.is_some()
        || !matches!(status, BatchStatus::Starting) && worker_step_execution_id.is_none()
    {
        return Err(RepositoryError::PartitionStateCorrupt);
    }
    Ok(StepPartition::from_snapshot(
        id,
        step_execution_id,
        worker_step_execution_id,
        key,
        ordinal,
        status,
        exit_status,
        counts,
        context,
        version,
    ))
}

fn decode_partition_context(row: &PgRow) -> Result<ExecutionContext, RepositoryError> {
    let format_version = u16::try_from(
        row.try_get::<i16, _>("context_format")
            .map_err(|_| RepositoryError::PartitionStateCorrupt)?,
    )
    .map_err(|_| RepositoryError::PartitionStateCorrupt)?;
    let schema = read_text(row, "context_schema")?;
    let schema_version = u32::try_from(
        row.try_get::<i32, _>("context_schema_version")
            .map_err(|_| RepositoryError::PartitionStateCorrupt)?,
    )
    .map_err(|_| RepositoryError::PartitionStateCorrupt)?;
    let Json(payload): Json<Value> = row
        .try_get("context_payload")
        .map_err(|_| RepositoryError::PartitionStateCorrupt)?;
    let envelope = json!({
        "format": "oxide-batch.execution-context",
        "format_version": format_version,
        "schema": schema,
        "schema_version": schema_version,
        "payload": payload,
    });
    let bytes =
        serde_json::to_vec(&envelope).map_err(|_| RepositoryError::PartitionStateCorrupt)?;
    let limits = StateLimits::new(MAX_PARTITION_CONTEXT_BYTES, 16)
        .map_err(|_| RepositoryError::PartitionStateCorrupt)?;
    let context = ExecutionContext::from_json(&bytes, limits)
        .map_err(|_| RepositoryError::PartitionStateCorrupt)?;
    let stored_checksum: [u8; 32] = row
        .try_get::<Vec<u8>, _>("context_checksum")
        .map_err(|_| RepositoryError::PartitionStateCorrupt)?
        .try_into()
        .map_err(|_| RepositoryError::PartitionStateCorrupt)?;
    let actual_checksum: [u8; 32] = Sha256::digest(
        context
            .to_json()
            .map_err(|_| RepositoryError::PartitionStateCorrupt)?,
    )
    .into();
    if stored_checksum != actual_checksum {
        return Err(RepositoryError::PartitionStateCorrupt);
    }
    Ok(context)
}

fn partition_count(value: u64) -> Result<i64, RepositoryError> {
    i64::try_from(value).map_err(|_| RepositoryError::PartitionStateCorrupt)
}

fn execution_count(value: u64) -> Result<i64, RepositoryError> {
    i64::try_from(value).map_err(|_| RepositoryError::Unavailable)
}

fn map_partition_mutation(id: StepPartitionId, error: PartitionMutationError) -> RepositoryError {
    match error {
        PartitionMutationError::StaleVersion { expected, actual } => {
            RepositoryError::Lifecycle(LifecycleError::StaleVersion { expected, actual })
        }
        PartitionMutationError::InvalidState { status } => {
            RepositoryError::PartitionUpdateNotAllowed { id, status }
        }
        PartitionMutationError::VersionExhausted => RepositoryError::PartitionStateCorrupt,
    }
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
        // `TerminalKind` is `#[non_exhaustive]`. A terminal this adapter cannot
        // spell is rejected rather than written as neither node nor terminal.
        FlowTarget::Terminal(_) => return Err(RepositoryError::Unavailable),
    })
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
        RecoveryDecisionId::new(read_u64(row, "id")?)?,
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
    let metadata = execution.metadata();
    let timestamps = metadata.timestamps();
    let failure = metadata.failure();
    let failure_category = failure.map(|value| encode_failure_category(value.category()));
    let failure_id = failure
        .map(|value| database_id(value.failure_id().get(), IdentifierKind::Failure))
        .transpose()?;
    let counts = metadata.counts();
    let result = sqlx::query(
        "UPDATE oxide_batch.ob_step_execution \
         SET status = $1, exit_code = $2, failure_category = $3, failure_id = $4, \
             started_at = CASE WHEN $5::bigint IS NULL THEN NULL \
                 ELSE to_timestamp($5::double precision / 1000.0) END, \
             ended_at = CASE WHEN $6::bigint IS NULL THEN NULL \
                 ELSE to_timestamp($6::double precision / 1000.0) END, \
             read_count = $7, processed_count = $8, write_count = $9, \
             filter_count = $10, commit_count = $11, rollback_count = $12, \
             updated_at = to_timestamp($13::double precision / 1000.0), version = $14 \
         WHERE id = $15 AND version = $16",
    )
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
    .bind(execution_count(counts.read())?)
    .bind(execution_count(counts.processed())?)
    .bind(execution_count(counts.written())?)
    .bind(execution_count(counts.filtered())?)
    .bind(execution_count(counts.committed())?)
    .bind(execution_count(counts.rolled_back())?)
    .bind(system_time_millis(updated_at)?)
    .bind(database_version(execution.version())?)
    .bind(database_id(
        execution.id().get(),
        IdentifierKind::StepExecution,
    )?)
    .bind(database_version(expected)?)
    .execute(transaction)
    .await
    .map_err(|_| RepositoryError::Unavailable)?;
    Ok(result.rows_affected())
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

fn read_optional_u64(row: &PgRow, name: &str) -> Result<Option<u64>, RepositoryError> {
    read_optional_i64(row, name)?
        .map(|value| u64::try_from(value).map_err(|_| RepositoryError::Unavailable))
        .transpose()
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
            key.digest(),
            [
                0x71, 0xf1, 0x2d, 0xb9, 0xe3, 0x88, 0x7d, 0xe2, 0xcf, 0x92, 0xe9, 0x3b, 0xb6, 0x3f,
                0xd4, 0xe9, 0xe7, 0xc5, 0x36, 0xdf, 0x8f, 0xa2, 0x02, 0x21, 0x24, 0x45, 0xd1, 0x8b,
                0xf2, 0xe4, 0x36, 0x04,
            ]
        );
        Ok(())
    }
}

fn candidate_execution_ids(candidates: &[PurgeCandidate]) -> Result<Vec<i64>, RepositoryError> {
    candidates
        .iter()
        .map(|candidate| {
            database_id(
                candidate.job_execution_id().get(),
                IdentifierKind::JobExecution,
            )
        })
        .collect()
}

fn candidate_instance_ids(candidates: &[PurgeCandidate]) -> Result<Vec<i64>, RepositoryError> {
    let mut ids = candidates
        .iter()
        .map(|candidate| {
            database_id(
                candidate.job_instance_id().get(),
                IdentifierKind::JobInstance,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    ids.sort_unstable();
    ids.dedup();
    Ok(ids)
}

fn retention_count(value: u64) -> Result<i64, RepositoryError> {
    i64::try_from(value).map_err(|_| RepositoryError::Unavailable)
}

fn operator_request_select(suffix: &str) -> String {
    format!(
        "SELECT request.id, request.job_instance_id, request.job_execution_id, \
         request.action, request.operation_id, request.actor_ref, request.reason_code, \
         request.request_digest, request.observed_version, request.prior_status, \
         request.result_status, request.outcome_class, request.rejection_code, \
         (extract(epoch FROM request.requested_at) * 1000)::bigint AS requested_ms \
         FROM oxide_batch.ob_operator_request request {suffix}"
    )
}

fn retention_action_select(suffix: &str) -> String {
    format!(
        "SELECT retention.id, retention.job_instance_id, retention.action, \
         retention.operation_id, retention.actor_ref, retention.reason_code, \
         retention.plan_digest, retention.batch_bound, \
         retention.deleted_flow_decisions, retention.deleted_recovery_decisions, \
         retention.deleted_operator_requests, retention.deleted_step_partitions, \
         retention.deleted_step_executions, retention.deleted_job_executions, \
         retention.deleted_job_instances, retention.outcome_class, \
         (extract(epoch FROM retention.applied_at) * 1000)::bigint AS applied_ms \
         FROM oxide_batch.ob_retention_action retention {suffix}"
    )
}

fn read_text(row: &PgRow, name: &str) -> Result<String, RepositoryError> {
    row.try_get::<String, _>(name)
        .map_err(|_| RepositoryError::Unavailable)
}

fn read_optional_text(row: &PgRow, name: &str) -> Result<Option<String>, RepositoryError> {
    row.try_get::<Option<String>, _>(name)
        .map_err(|_| RepositoryError::Unavailable)
}

fn read_digest(row: &PgRow, name: &str) -> Result<[u8; 32], RepositoryError> {
    row.try_get::<Vec<u8>, _>(name)
        .map_err(|_| RepositoryError::Unavailable)?
        .try_into()
        .map_err(|_| RepositoryError::Unavailable)
}

fn decode_operator_action(value: &str) -> Result<OperatorAction, RepositoryError> {
    Ok(match value {
        "LAUNCH" => OperatorAction::Launch,
        "RESTART" => OperatorAction::Restart,
        "STOP" => OperatorAction::Stop,
        "ABANDON" => OperatorAction::Abandon,
        "RECOVER" => OperatorAction::Recover,
        _ => return Err(RepositoryError::Unavailable),
    })
}

fn decode_retention_action(value: &str) -> Result<RetentionAction, RepositoryError> {
    Ok(match value {
        "HOLD" => RetentionAction::Hold,
        "RELEASE_HOLD" => RetentionAction::ReleaseHold,
        "APPLY_PURGE" => RetentionAction::ApplyPurge,
        _ => return Err(RepositoryError::Unavailable),
    })
}

fn decode_operator_outcome(value: &str) -> Result<OperatorOutcomeClass, RepositoryError> {
    Ok(match value {
        "APPLIED" => OperatorOutcomeClass::Applied,
        "REJECTED" => OperatorOutcomeClass::Rejected,
        _ => return Err(RepositoryError::Unavailable),
    })
}

fn decode_retention_outcome(value: &str) -> Result<RetentionOutcome, RepositoryError> {
    Ok(match value {
        "APPLIED" => RetentionOutcome::Applied,
        "REJECTED" => RetentionOutcome::Rejected,
        _ => return Err(RepositoryError::Unavailable),
    })
}

fn decode_operator_rejection(
    value: &str,
    row: &PgRow,
) -> Result<OperatorRejection, RepositoryError> {
    Ok(match value {
        "OPTIMISTIC_CONFLICT" => OperatorRejection::OptimisticConflict {
            current: ExecutionVersion::new(
                read_optional_u64(row, "observed_version")?.unwrap_or(0),
            ),
        },
        "INVALID_STATE" => OperatorRejection::InvalidState {
            status: read_optional_text(row, "prior_status")?
                .as_deref()
                .map(decode_status)
                .transpose()?
                .unwrap_or(BatchStatus::Unknown),
        },
        "INSTANCE_COMPLETED" => OperatorRejection::InstanceCompleted,
        "INSTANCE_ABANDONED" => OperatorRejection::InstanceAbandoned,
        "EXECUTION_ALREADY_ACTIVE" => OperatorRejection::ExecutionAlreadyActive {
            execution_id: JobExecutionId::new(read_u64(row, "job_execution_id")?)?,
            status: read_optional_text(row, "prior_status")?
                .as_deref()
                .map(decode_status)
                .transpose()?
                .unwrap_or(BatchStatus::Unknown),
        },
        "INCOMPATIBLE_DEFINITION" => OperatorRejection::IncompatibleDefinition,
        "RESTART_WITHOUT_PRIOR_ATTEMPT" => OperatorRejection::RestartWithoutPriorAttempt,
        "START_LIMIT_EXCEEDED" => OperatorRejection::StartLimitExceeded,
        "UNRESOLVED_RECOVERY_REQUIRED" => OperatorRejection::UnresolvedRecoveryRequired,
        "EXECUTION_NOT_FOUND" => OperatorRejection::ExecutionNotFound,
        "INSTANCE_NOT_FOUND" => OperatorRejection::InstanceNotFound,
        "UNSUPPORTED_ACTION" => OperatorRejection::UnsupportedAction,
        _ => return Err(RepositoryError::Unavailable),
    })
}

fn decode_operator_record(row: &PgRow) -> Result<OperatorRecord, RepositoryError> {
    let id = OperatorRequestId::new(read_u64(row, "id")?)?;
    let action = decode_operator_action(&read_text(row, "action")?)?;
    let outcome = decode_operator_outcome(&read_text(row, "outcome_class")?)?;
    let rejection = read_optional_text(row, "rejection_code")?
        .map(|code| decode_operator_rejection(&code, row))
        .transpose()?;
    let draft = OperatorRecordDraft::from_durable(
        action,
        OperationId::new(read_text(row, "operation_id")?)
            .map_err(|_| RepositoryError::Unavailable)?,
        ActorRef::new(read_text(row, "actor_ref")?).map_err(|_| RepositoryError::Unavailable)?,
        read_optional_text(row, "reason_code")?
            .map(ReasonCode::new)
            .transpose()
            .map_err(|_| RepositoryError::Unavailable)?,
        RequestDigest::from_bytes(read_digest(row, "request_digest")?),
        read_optional_u64(row, "job_instance_id")?
            .map(JobInstanceId::new)
            .transpose()?,
        read_optional_u64(row, "job_execution_id")?
            .map(JobExecutionId::new)
            .transpose()?,
        read_optional_u64(row, "observed_version")?.map(ExecutionVersion::new),
        read_optional_text(row, "prior_status")?
            .as_deref()
            .map(decode_status)
            .transpose()?,
        read_optional_text(row, "result_status")?
            .as_deref()
            .map(decode_status)
            .transpose()?,
        outcome,
        rejection,
        millis_system_time(read_i64(row, "requested_ms")?)?,
    );
    Ok(OperatorRecord::from_parts(id, draft))
}

fn decode_retention_record(row: &PgRow) -> Result<RetentionRecord, RepositoryError> {
    let id = RetentionActionId::new(read_u64(row, "id")?)?;
    let counts = PurgeCounts::new(
        read_u64(row, "deleted_flow_decisions")?,
        read_u64(row, "deleted_recovery_decisions")?,
        read_u64(row, "deleted_operator_requests")?,
        read_u64(row, "deleted_step_partitions")?,
        read_u64(row, "deleted_step_executions")?,
        read_u64(row, "deleted_job_executions")?,
        read_u64(row, "deleted_job_instances")?,
    );
    let batch_bound = row
        .try_get::<Option<i32>, _>("batch_bound")
        .map_err(|_| RepositoryError::Unavailable)?
        .map(|bound| {
            u32::try_from(bound)
                .map_err(|_| RepositoryError::Unavailable)
                .and_then(|bound| {
                    PurgeBatchBound::new(bound).map_err(|_| RepositoryError::Unavailable)
                })
        })
        .transpose()?;
    let plan_digest = row
        .try_get::<Option<Vec<u8>>, _>("plan_digest")
        .map_err(|_| RepositoryError::Unavailable)?
        .map(|digest| <[u8; 32]>::try_from(digest).map_err(|_| RepositoryError::Unavailable))
        .transpose()?;
    let draft = RetentionRecordDraft::from_durable(
        decode_retention_action(&read_text(row, "action")?)?,
        OperationId::new(read_text(row, "operation_id")?)
            .map_err(|_| RepositoryError::Unavailable)?,
        ActorRef::new(read_text(row, "actor_ref")?).map_err(|_| RepositoryError::Unavailable)?,
        ReasonCode::new(read_text(row, "reason_code")?)
            .map_err(|_| RepositoryError::Unavailable)?,
        read_optional_u64(row, "job_instance_id")?
            .map(JobInstanceId::new)
            .transpose()?,
        plan_digest,
        counts,
        batch_bound,
        decode_retention_outcome(&read_text(row, "outcome_class")?)?,
        millis_system_time(read_i64(row, "applied_ms")?)?,
    );
    Ok(RetentionRecord::from_parts(id, draft))
}

fn decode_retention_hold(
    id: JobInstanceId,
    row: &PgRow,
) -> Result<Option<RetentionHold>, RepositoryError> {
    let Some(actor) = read_optional_text(row, "hold_actor")? else {
        return Ok(None);
    };
    let reason = read_optional_text(row, "hold_reason")?.ok_or(RepositoryError::Unavailable)?;
    let placed_ms = read_optional_i64(row, "placed_ms")?.ok_or(RepositoryError::Unavailable)?;
    Ok(Some(RetentionHold::new(
        id,
        ActorRef::new(actor).map_err(|_| RepositoryError::Unavailable)?,
        ReasonCode::new(reason).map_err(|_| RepositoryError::Unavailable)?,
        millis_system_time(placed_ms)?,
    )))
}

/// The bounded keyset read port of [`PostgresJobRepository`].
///
/// Each page is one statement executed under the configured statement timeout
/// and the adapter's ordinary read committed isolation. No page takes a lock
/// or participates in a chunk transaction, and cross-page snapshot isolation
/// is not provided.
///
/// The unresolved-execution age bound compares the durable `updated_at` column
/// against repository server time. It never consults the inspecting process's
/// wall clock and never updates a row.
#[derive(Clone)]
pub struct PostgresExplorer {
    repository: PostgresJobRepository,
}

impl PostgresExplorer {
    /// Binds one durable repository to the bounded read port.
    #[must_use]
    pub const fn new(repository: PostgresJobRepository) -> Self {
        Self { repository }
    }

    async fn fetch_all(
        &self,
        query: sqlx::query::Query<'_, Postgres, PgArguments>,
    ) -> Result<Vec<PgRow>, ExplorerError> {
        let mut connection = self
            .repository
            .begin_connection()
            .await
            .map_err(ExplorerError::Repository)?;
        let result = query.fetch_all(&mut *connection).await;
        let _ = sqlx::query("ROLLBACK").execute(&mut *connection).await;
        result.map_err(|error| classify_explorer_error(&error))
    }

    async fn fetch_optional(
        &self,
        query: sqlx::query::Query<'_, Postgres, PgArguments>,
    ) -> Result<Option<PgRow>, ExplorerError> {
        let mut connection = self
            .repository
            .begin_connection()
            .await
            .map_err(ExplorerError::Repository)?;
        let result = query.fetch_optional(&mut *connection).await;
        let _ = sqlx::query("ROLLBACK").execute(&mut *connection).await;
        result.map_err(|error| classify_explorer_error(&error))
    }
}

impl fmt::Debug for PostgresExplorer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresExplorer")
            .finish_non_exhaustive()
    }
}

fn classify_explorer_error(error: &sqlx::Error) -> ExplorerError {
    if let sqlx::Error::Database(database) = error
        && database.code().as_deref() == Some("57014")
    {
        return ExplorerError::Timeout;
    }
    ExplorerError::Repository(RepositoryError::Unavailable)
}

const fn ceiling_source(query: &ExplorerQuery) -> Option<&'static str> {
    Some(match query {
        ExplorerQuery::JobNames => "SELECT max(id) AS ceiling FROM oxide_batch.ob_job_definition",
        ExplorerQuery::Instances { .. } => {
            "SELECT max(id) AS ceiling FROM oxide_batch.ob_job_instance WHERE job_name = $1"
        }
        ExplorerQuery::Executions { .. } | ExplorerQuery::UnresolvedExecutions { .. } => {
            "SELECT max(id) AS ceiling FROM oxide_batch.ob_job_execution"
        }
        ExplorerQuery::StepExecutions { .. } => {
            "SELECT max(id) AS ceiling FROM oxide_batch.ob_step_execution"
        }
        ExplorerQuery::RecoveryDecisions { .. } => {
            "SELECT max(id) AS ceiling FROM oxide_batch.ob_recovery_decision"
        }
        ExplorerQuery::FlowDecisions { .. } => {
            "SELECT max(id) AS ceiling FROM oxide_batch.ob_flow_decision"
        }
        ExplorerQuery::StepPartitions { .. } => {
            "SELECT max(id) AS ceiling FROM oxide_batch.ob_step_partition"
        }
        ExplorerQuery::OperatorRequests { .. } => {
            "SELECT max(id) AS ceiling FROM oxide_batch.ob_operator_request"
        }
        // Absorbs any query added later: this adapter has no statement that
        // bounds a traversal it does not know, so it reports the missing
        // capability instead of paging from a guessed ceiling.
        _ => return None,
    })
}

fn window_limit(window: &QueryWindow) -> i64 {
    i64::from(window.limit())
}

fn window_ceiling(window: &QueryWindow) -> Result<i64, ExplorerError> {
    i64::try_from(window.ceiling())
        .map_err(|_| ExplorerError::Repository(RepositoryError::Unavailable))
}

fn window_identity(window: &QueryWindow) -> Result<Option<i64>, ExplorerError> {
    match window.after() {
        Some(CursorKey::Identity(value)) => i64::try_from(*value)
            .map(Some)
            .map_err(|_| ExplorerError::Cursor(CursorError::CursorInvalid)),
        Some(_) => Err(ExplorerError::Cursor(CursorError::CursorInvalid)),
        None => Ok(None),
    }
}

fn window_ordered(window: &QueryWindow) -> Result<Option<(i64, i64)>, ExplorerError> {
    match window.after() {
        Some(CursorKey::Ordered { primary, identity }) => {
            let primary = i64::try_from(*primary)
                .map_err(|_| ExplorerError::Cursor(CursorError::CursorInvalid))?;
            let identity = i64::try_from(*identity)
                .map_err(|_| ExplorerError::Cursor(CursorError::CursorInvalid))?;
            Ok(Some((primary, identity)))
        }
        Some(_) => Err(ExplorerError::Cursor(CursorError::CursorInvalid)),
        None => Ok(None),
    }
}

fn window_name(window: &QueryWindow) -> Result<Option<String>, ExplorerError> {
    match window.after() {
        Some(CursorKey::Name(value)) => Ok(Some(value.clone())),
        Some(_) => Err(ExplorerError::Cursor(CursorError::CursorInvalid)),
        None => Ok(None),
    }
}

fn explorer_id(value: u64, kind: IdentifierKind) -> Result<i64, ExplorerError> {
    database_id(value, kind).map_err(ExplorerError::Repository)
}

fn job_execution_projection_select(suffix: &str) -> String {
    format!(
        "SELECT execution.id, execution.job_instance_id, instance.job_name, \
         execution.attempt, execution.status, execution.exit_code, \
         execution.failure_category, execution.failure_id, \
         (extract(epoch FROM execution.created_at) * 1000)::bigint AS created_ms, \
         (extract(epoch FROM execution.started_at) * 1000)::bigint AS started_ms, \
         (extract(epoch FROM execution.ended_at) * 1000)::bigint AS ended_ms, \
         (extract(epoch FROM execution.updated_at) * 1000)::bigint AS updated_ms, \
         (extract(epoch FROM execution.stop_requested_at) * 1000)::bigint AS stop_ms, \
         (execution.owner_token IS NOT NULL) AS owner_recorded, execution.version, \
         definition.definition_revision, definition.manifest_format, \
         definition.manifest_digest, execution.context_format, \
         execution.context_schema, execution.context_schema_version, \
         pg_column_size(execution.context_payload)::bigint AS context_bytes \
         FROM oxide_batch.ob_job_execution execution \
         JOIN oxide_batch.ob_job_instance instance \
           ON instance.id = execution.job_instance_id \
         JOIN oxide_batch.ob_job_definition definition \
           ON definition.id = execution.definition_id {suffix}"
    )
}

fn step_execution_projection_select(suffix: &str) -> String {
    format!(
        "SELECT execution.id, execution.job_execution_id, execution.step_name, \
         execution.step_logical_id, execution.status, execution.exit_code, \
         execution.read_count, execution.processed_count, execution.write_count, \
         execution.filter_count, execution.commit_count, execution.rollback_count, \
         execution.failure_category, execution.failure_id, execution.version, \
         (extract(epoch FROM execution.created_at) * 1000)::bigint AS created_ms, \
         (extract(epoch FROM execution.started_at) * 1000)::bigint AS started_ms, \
         (extract(epoch FROM execution.ended_at) * 1000)::bigint AS ended_ms, \
         execution.checkpoint_format, execution.checkpoint_schema, \
         execution.checkpoint_schema_version, \
         pg_column_size(execution.checkpoint_payload)::bigint AS checkpoint_bytes, \
         execution.context_format, execution.context_schema, \
         execution.context_schema_version, \
         pg_column_size(execution.context_payload)::bigint AS context_bytes \
         FROM oxide_batch.ob_step_execution execution {suffix}"
    )
}

fn step_partition_projection_select(suffix: &str) -> String {
    format!(
        "SELECT partition.id, partition.step_execution_id, \
         partition.worker_step_execution_id, partition.partition_key, \
         partition.partition_ordinal, partition.status, partition.exit_code, \
         partition.read_count, partition.processed_count, partition.write_count, \
         partition.filter_count, partition.commit_count, partition.rollback_count, \
         partition.version, partition.context_format, partition.context_schema, \
         partition.context_schema_version, \
         pg_column_size(partition.context_payload)::bigint AS context_bytes \
         FROM oxide_batch.ob_step_partition partition {suffix}"
    )
}

fn decode_state_descriptor(
    row: &PgRow,
    kind: DurableStateKind,
    format: &str,
    schema: &str,
    schema_version: &str,
    bytes: &str,
) -> Result<StateEnvelopeDescriptor, ExplorerError> {
    let unavailable = || ExplorerError::Repository(RepositoryError::Unavailable);
    let format_version = row
        .try_get::<i16, _>(format)
        .map_err(|_| unavailable())
        .and_then(|value| u16::try_from(value).map_err(|_| unavailable()))?;
    let schema_id = StateSchemaId::new(read_text(row, schema).map_err(ExplorerError::Repository)?)
        .map_err(|_| unavailable())?;
    let schema_version = row
        .try_get::<i32, _>(schema_version)
        .map_err(|_| unavailable())
        .and_then(|value| u32::try_from(value).map_err(|_| unavailable()))
        .and_then(|value| StateSchemaVersion::new(value).map_err(|_| unavailable()))?;
    let encoded_len = read_optional_i64(row, bytes)
        .map_err(ExplorerError::Repository)?
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(0);
    Ok(StateEnvelopeDescriptor::new(
        kind,
        format_version,
        schema_id,
        schema_version,
        encoded_len,
    ))
}

fn decode_projection_counts(row: &PgRow) -> Result<ExecutionCounts, ExplorerError> {
    let read = |name: &str| read_u64(row, name).map_err(ExplorerError::Repository);
    Ok(ExecutionCounts::new(
        read("read_count")?,
        read("processed_count")?,
        read("write_count")?,
        read("filter_count")?,
        read("commit_count")?,
        read("rollback_count")?,
    ))
}

fn decode_projection_failure(row: &PgRow) -> Result<Option<FailureSummary>, ExplorerError> {
    let unavailable = || ExplorerError::Repository(RepositoryError::Unavailable);
    let category =
        read_optional_text(row, "failure_category").map_err(ExplorerError::Repository)?;
    let failure_id = read_optional_u64(row, "failure_id").map_err(ExplorerError::Repository)?;
    match (category, failure_id) {
        (Some(category), Some(failure_id)) => Ok(Some(FailureSummary::new(
            decode_failure_category(&category).map_err(|_| unavailable())?,
            FailureId::new(failure_id).map_err(|_| unavailable())?,
        ))),
        (None, None) => Ok(None),
        _ => Err(unavailable()),
    }
}

fn decode_projection_timestamps(row: &PgRow) -> Result<ExecutionTimestamps, ExplorerError> {
    let unavailable = || ExplorerError::Repository(RepositoryError::Unavailable);
    let created =
        millis_system_time(read_i64(row, "created_ms").map_err(ExplorerError::Repository)?)
            .map_err(|_| unavailable())?;
    let started = read_optional_i64(row, "started_ms")
        .map_err(ExplorerError::Repository)?
        .map(millis_system_time)
        .transpose()
        .map_err(|_| unavailable())?;
    let ended = read_optional_i64(row, "ended_ms")
        .map_err(ExplorerError::Repository)?
        .map(millis_system_time)
        .transpose()
        .map_err(|_| unavailable())?;
    ExecutionTimestamps::new(created, started, ended).map_err(|_| unavailable())
}

fn decode_job_execution_projection(row: &PgRow) -> Result<JobExecutionProjection, ExplorerError> {
    let unavailable = || ExplorerError::Repository(RepositoryError::Unavailable);
    let id = JobExecutionId::new(read_u64(row, "id").map_err(ExplorerError::Repository)?)
        .map_err(|_| unavailable())?;
    let instance_id =
        JobInstanceId::new(read_u64(row, "job_instance_id").map_err(ExplorerError::Repository)?)
            .map_err(|_| unavailable())?;
    let job_name = JobName::new(read_text(row, "job_name").map_err(ExplorerError::Repository)?)
        .map_err(|_| unavailable())?;
    let attempt = row
        .try_get::<i32, _>("attempt")
        .map_err(|_| unavailable())
        .and_then(|value| u32::try_from(value).map_err(|_| unavailable()))?;
    let status = decode_status(&read_text(row, "status").map_err(ExplorerError::Repository)?)
        .map_err(|_| unavailable())?;
    let exit_status = ExitStatus::new(
        ExitCode::new(read_text(row, "exit_code").map_err(ExplorerError::Repository)?)
            .map_err(|_| unavailable())?,
    );
    let definition = DefinitionDescriptor::new(
        DefinitionRevision::new(
            read_text(row, "definition_revision").map_err(ExplorerError::Repository)?,
        )
        .map_err(|_| unavailable())?,
        row.try_get::<i16, _>("manifest_format")
            .map_err(|_| unavailable())
            .and_then(|value| u16::try_from(value).map_err(|_| unavailable()))?,
        read_digest(row, "manifest_digest").map_err(ExplorerError::Repository)?,
    );
    let context = decode_state_descriptor(
        row,
        DurableStateKind::ExecutionContext,
        "context_format",
        "context_schema",
        "context_schema_version",
        "context_bytes",
    )?;
    let stop_requested_at = read_optional_i64(row, "stop_ms")
        .map_err(ExplorerError::Repository)?
        .map(millis_system_time)
        .transpose()
        .map_err(|_| unavailable())?;
    let owner_recorded = row
        .try_get::<bool, _>("owner_recorded")
        .map_err(|_| unavailable())?;
    Ok(JobExecutionProjection::new(
        id,
        instance_id,
        job_name,
        attempt,
        status,
        exit_status,
        ExecutionCounts::default(),
        ExecutionVersion::new(read_u64(row, "version").map_err(ExplorerError::Repository)?),
        decode_projection_timestamps(row)?,
        millis_system_time(read_i64(row, "updated_ms").map_err(ExplorerError::Repository)?)
            .map_err(|_| unavailable())?,
        decode_projection_failure(row)?,
        Some(definition),
        Some(context),
        stop_requested_at,
        owner_recorded,
    ))
}

fn decode_job_instance_projection(row: &PgRow) -> Result<JobInstanceProjection, ExplorerError> {
    let unavailable = || ExplorerError::Repository(RepositoryError::Unavailable);
    let id = JobInstanceId::new(read_u64(row, "id").map_err(ExplorerError::Repository)?)
        .map_err(|_| unavailable())?;
    let job_name = JobName::new(read_text(row, "job_name").map_err(ExplorerError::Repository)?)
        .map_err(|_| unavailable())?;
    let parameters = row
        .try_get::<Json<Value>, _>("identifying_parameters")
        .map_err(|_| unavailable())?;
    let parameters = decode_identifying_parameters(&parameters.0).map_err(|_| unavailable())?;
    let descriptors = parameters
        .iter()
        .map(|(name, parameter)| {
            ParameterDescriptor::new(
                name.clone(),
                parameter.value().kind(),
                parameter.is_identifying(),
            )
        })
        .collect();
    let created_at = read_optional_i64(row, "created_ms")
        .map_err(ExplorerError::Repository)?
        .map(millis_system_time)
        .transpose()
        .map_err(|_| unavailable())?;
    let hold = decode_retention_hold(id, row).map_err(ExplorerError::Repository)?;
    Ok(JobInstanceProjection::new(
        id,
        job_name,
        read_digest(row, "instance_key").map_err(ExplorerError::Repository)?,
        descriptors,
        created_at,
        hold,
    ))
}

fn decode_step_execution_projection(row: &PgRow) -> Result<StepExecutionProjection, ExplorerError> {
    let unavailable = || ExplorerError::Repository(RepositoryError::Unavailable);
    let id = StepExecutionId::new(read_u64(row, "id").map_err(ExplorerError::Repository)?)
        .map_err(|_| unavailable())?;
    let job_execution_id =
        JobExecutionId::new(read_u64(row, "job_execution_id").map_err(ExplorerError::Repository)?)
            .map_err(|_| unavailable())?;
    let step_name = StepName::new(read_text(row, "step_name").map_err(ExplorerError::Repository)?)
        .map_err(|_| unavailable())?;
    let node_id = read_optional_text(row, "step_logical_id")
        .map_err(ExplorerError::Repository)?
        .map(NodeId::new)
        .transpose()
        .map_err(|_| unavailable())?;
    let status = decode_status(&read_text(row, "status").map_err(ExplorerError::Repository)?)
        .map_err(|_| unavailable())?;
    let exit_status = ExitStatus::new(
        ExitCode::new(read_text(row, "exit_code").map_err(ExplorerError::Repository)?)
            .map_err(|_| unavailable())?,
    );
    Ok(StepExecutionProjection::new(
        id,
        job_execution_id,
        step_name,
        node_id,
        status,
        exit_status,
        decode_projection_counts(row)?,
        ExecutionVersion::new(read_u64(row, "version").map_err(ExplorerError::Repository)?),
        decode_projection_timestamps(row)?,
        decode_projection_failure(row)?,
        Some(decode_state_descriptor(
            row,
            DurableStateKind::Checkpoint,
            "checkpoint_format",
            "checkpoint_schema",
            "checkpoint_schema_version",
            "checkpoint_bytes",
        )?),
        Some(decode_state_descriptor(
            row,
            DurableStateKind::ExecutionContext,
            "context_format",
            "context_schema",
            "context_schema_version",
            "context_bytes",
        )?),
    ))
}

fn decode_step_partition_projection(row: &PgRow) -> Result<StepPartitionProjection, ExplorerError> {
    let unavailable = || ExplorerError::Repository(RepositoryError::Unavailable);
    let id = StepPartitionId::new(read_u64(row, "id").map_err(ExplorerError::Repository)?)
        .map_err(|_| unavailable())?;
    let step_execution_id = StepExecutionId::new(
        read_u64(row, "step_execution_id").map_err(ExplorerError::Repository)?,
    )
    .map_err(|_| unavailable())?;
    let worker = read_optional_u64(row, "worker_step_execution_id")
        .map_err(ExplorerError::Repository)?
        .map(StepExecutionId::new)
        .transpose()
        .map_err(|_| unavailable())?;
    let status = decode_status(&read_text(row, "status").map_err(ExplorerError::Repository)?)
        .map_err(|_| unavailable())?;
    let exit_status = ExitStatus::new(
        ExitCode::new(
            read_optional_text(row, "exit_code")
                .map_err(ExplorerError::Repository)?
                .unwrap_or_else(|| String::from("UNKNOWN")),
        )
        .map_err(|_| unavailable())?,
    );
    let ordinal = row
        .try_get::<i32, _>("partition_ordinal")
        .map_err(|_| unavailable())
        .and_then(|value| u32::try_from(value).map_err(|_| unavailable()))?;
    Ok(StepPartitionProjection::new(
        id,
        step_execution_id,
        read_text(row, "partition_key").map_err(ExplorerError::Repository)?,
        ordinal,
        status,
        exit_status,
        decode_projection_counts(row)?,
        ExecutionVersion::new(read_u64(row, "version").map_err(ExplorerError::Repository)?),
        worker,
        Some(decode_state_descriptor(
            row,
            DurableStateKind::ExecutionContext,
            "context_format",
            "context_schema",
            "context_schema_version",
            "context_bytes",
        )?),
    ))
}

impl ExplorerRepository for PostgresExplorer {
    fn identity_ceiling<'a>(
        &'a self,
        query: &'a ExplorerQuery,
    ) -> BoxFuture<'a, Result<u64, ExplorerError>> {
        Box::pin(async move {
            let Some(source) = ceiling_source(query) else {
                return Err(ExplorerError::UnsupportedCapability);
            };
            let statement = sqlx::query(source);
            let statement = match query {
                ExplorerQuery::Instances { job_name } => statement.bind(job_name.as_str()),
                _ => statement,
            };
            let row = self.fetch_optional(statement).await?;
            let ceiling = row
                .as_ref()
                .map(|row| read_optional_i64(row, "ceiling"))
                .transpose()
                .map_err(ExplorerError::Repository)?
                .flatten()
                .unwrap_or(0);
            u64::try_from(ceiling)
                .map_err(|_| ExplorerError::Repository(RepositoryError::Unavailable))
        })
    }

    fn job_names<'a>(
        &'a self,
        window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<JobName>, ExplorerError>> {
        Box::pin(async move {
            let rows = self
                .fetch_all(
                    sqlx::query(
                        "SELECT DISTINCT job_name FROM oxide_batch.ob_job_definition \
                         WHERE id <= $1 AND ($2::text IS NULL OR job_name > $2) \
                         ORDER BY job_name LIMIT $3",
                    )
                    .bind(window_ceiling(window)?)
                    .bind(window_name(window)?)
                    .bind(window_limit(window)),
                )
                .await?;
            rows.iter()
                .map(|row| {
                    JobName::new(read_text(row, "job_name").map_err(ExplorerError::Repository)?)
                        .map_err(|_| ExplorerError::Repository(RepositoryError::Unavailable))
                })
                .collect()
        })
    }

    fn instances<'a>(
        &'a self,
        job_name: &'a JobName,
        window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<JobInstanceProjection>, ExplorerError>> {
        Box::pin(async move {
            let rows = self
                .fetch_all(
                    sqlx::query(
                        "SELECT id, job_name, instance_key, identifying_parameters, \
                         (extract(epoch FROM created_at) * 1000)::bigint AS created_ms, \
                         hold_actor, hold_reason, \
                         (extract(epoch FROM hold_placed_at) * 1000)::bigint AS placed_ms \
                         FROM oxide_batch.ob_job_instance \
                         WHERE job_name = $1 AND id <= $2 \
                           AND ($3::bigint IS NULL OR id < $3) \
                         ORDER BY id DESC LIMIT $4",
                    )
                    .bind(job_name.as_str())
                    .bind(window_ceiling(window)?)
                    .bind(window_identity(window)?)
                    .bind(window_limit(window)),
                )
                .await?;
            rows.iter().map(decode_job_instance_projection).collect()
        })
    }

    fn executions<'a>(
        &'a self,
        job_instance_id: JobInstanceId,
        window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<JobExecutionProjection>, ExplorerError>> {
        Box::pin(async move {
            let after = window_ordered(window)?;
            let rows = self
                .fetch_all(
                    sqlx::query(AssertSqlSafe(job_execution_projection_select(
                        "WHERE execution.job_instance_id = $1 AND execution.id <= $2 \
                         AND ($3::bigint IS NULL \
                              OR (execution.attempt, execution.id) < ($3, $4)) \
                         ORDER BY execution.attempt DESC, execution.id DESC LIMIT $5",
                    )))
                    .bind(explorer_id(
                        job_instance_id.get(),
                        IdentifierKind::JobInstance,
                    )?)
                    .bind(window_ceiling(window)?)
                    .bind(after.map(|(primary, _)| primary))
                    .bind(after.map_or(0, |(_, identity)| identity))
                    .bind(window_limit(window)),
                )
                .await?;
            rows.iter().map(decode_job_execution_projection).collect()
        })
    }

    fn execution(
        &self,
        job_execution_id: JobExecutionId,
    ) -> BoxFuture<'_, Result<Option<JobExecutionProjection>, ExplorerError>> {
        Box::pin(async move {
            let row = self
                .fetch_optional(
                    sqlx::query(AssertSqlSafe(job_execution_projection_select(
                        "WHERE execution.id = $1",
                    )))
                    .bind(explorer_id(
                        job_execution_id.get(),
                        IdentifierKind::JobExecution,
                    )?),
                )
                .await?;
            row.as_ref()
                .map(decode_job_execution_projection)
                .transpose()
        })
    }

    fn step_executions<'a>(
        &'a self,
        job_execution_id: JobExecutionId,
        window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<StepExecutionProjection>, ExplorerError>> {
        Box::pin(async move {
            let rows = self
                .fetch_all(
                    sqlx::query(AssertSqlSafe(step_execution_projection_select(
                        "WHERE execution.job_execution_id = $1 AND execution.id <= $2 \
                         AND ($3::bigint IS NULL OR execution.id > $3) \
                         ORDER BY execution.id LIMIT $4",
                    )))
                    .bind(explorer_id(
                        job_execution_id.get(),
                        IdentifierKind::JobExecution,
                    )?)
                    .bind(window_ceiling(window)?)
                    .bind(window_identity(window)?)
                    .bind(window_limit(window)),
                )
                .await?;
            rows.iter().map(decode_step_execution_projection).collect()
        })
    }

    fn unresolved_executions<'a>(
        &'a self,
        minimum_age: Duration,
        window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<JobExecutionProjection>, ExplorerError>> {
        Box::pin(async move {
            let seconds = i64::try_from(minimum_age.as_secs())
                .map_err(|_| ExplorerError::Repository(RepositoryError::Unavailable))?;
            let rows = self
                .fetch_all(
                    sqlx::query(AssertSqlSafe(job_execution_projection_select(
                        "WHERE execution.status IN \
                           ('STARTING', 'STARTED', 'STOPPING', 'UNKNOWN') \
                         AND execution.updated_at \
                             < CURRENT_TIMESTAMP - make_interval(secs => $1::double precision) \
                         AND execution.id <= $2 AND ($3::bigint IS NULL OR execution.id > $3) \
                         ORDER BY execution.id LIMIT $4",
                    )))
                    .bind(seconds)
                    .bind(window_ceiling(window)?)
                    .bind(window_identity(window)?)
                    .bind(window_limit(window)),
                )
                .await?;
            rows.iter().map(decode_job_execution_projection).collect()
        })
    }

    fn recovery_decisions<'a>(
        &'a self,
        job_execution_id: JobExecutionId,
        window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<RecoveryDecision>, ExplorerError>> {
        Box::pin(async move {
            let rows = self
                .fetch_all(
                    sqlx::query(
                        "SELECT id, execution_version, prior_status, resulting_status, \
                         reason_code, operator_reference, evidence_digest, \
                         (extract(epoch FROM decided_at) * 1000)::bigint AS decided_ms \
                         FROM oxide_batch.ob_recovery_decision \
                         WHERE job_execution_id = $1 AND id <= $2 \
                           AND ($3::bigint IS NULL OR id > $3) \
                         ORDER BY id LIMIT $4",
                    )
                    .bind(explorer_id(
                        job_execution_id.get(),
                        IdentifierKind::JobExecution,
                    )?)
                    .bind(window_ceiling(window)?)
                    .bind(window_identity(window)?)
                    .bind(window_limit(window)),
                )
                .await?;
            rows.iter()
                .map(|row| {
                    decode_recovery_decision(job_execution_id, row)
                        .map_err(ExplorerError::Repository)
                })
                .collect()
        })
    }

    fn flow_decisions<'a>(
        &'a self,
        job_execution_id: JobExecutionId,
        window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<FlowDecision>, ExplorerError>> {
        Box::pin(async move {
            let after = window_ordered(window)?;
            let rows = self
                .fetch_all(
                    sqlx::query(AssertSqlSafe(flow_decision_select(
                        "WHERE decision.job_execution_id = $1 AND decision.id <= $2 \
                         AND ($3::bigint IS NULL \
                              OR (decision.sequence, decision.id) > ($3, $4)) \
                         ORDER BY decision.sequence, decision.id LIMIT $5",
                    )))
                    .bind(explorer_id(
                        job_execution_id.get(),
                        IdentifierKind::JobExecution,
                    )?)
                    .bind(window_ceiling(window)?)
                    .bind(after.map(|(primary, _)| primary))
                    .bind(after.map_or(0, |(_, identity)| identity))
                    .bind(window_limit(window)),
                )
                .await?;
            rows.iter()
                .map(|row| decode_flow_decision(row).map_err(ExplorerError::Repository))
                .collect()
        })
    }

    fn step_partitions<'a>(
        &'a self,
        step_execution_id: StepExecutionId,
        window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<StepPartitionProjection>, ExplorerError>> {
        Box::pin(async move {
            let rows = self
                .fetch_all(
                    sqlx::query(AssertSqlSafe(step_partition_projection_select(
                        "WHERE partition.step_execution_id = $1 AND partition.id <= $2 \
                         AND ($3::bigint IS NULL OR partition.id > $3) \
                         ORDER BY partition.id LIMIT $4",
                    )))
                    .bind(explorer_id(
                        step_execution_id.get(),
                        IdentifierKind::StepExecution,
                    )?)
                    .bind(window_ceiling(window)?)
                    .bind(window_identity(window)?)
                    .bind(window_limit(window)),
                )
                .await?;
            rows.iter().map(decode_step_partition_projection).collect()
        })
    }

    fn operator_requests<'a>(
        &'a self,
        job_execution_id: JobExecutionId,
        window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<OperatorRecord>, ExplorerError>> {
        Box::pin(async move {
            let rows = self
                .fetch_all(
                    sqlx::query(AssertSqlSafe(operator_request_select(
                        "WHERE request.job_execution_id = $1 AND request.id <= $2 \
                         AND ($3::bigint IS NULL OR request.id > $3) \
                         ORDER BY request.id LIMIT $4",
                    )))
                    .bind(explorer_id(
                        job_execution_id.get(),
                        IdentifierKind::JobExecution,
                    )?)
                    .bind(window_ceiling(window)?)
                    .bind(window_identity(window)?)
                    .bind(window_limit(window)),
                )
                .await?;
            rows.iter()
                .map(|row| decode_operator_record(row).map_err(ExplorerError::Repository))
                .collect()
        })
    }
}

impl crate::RecoveryRepository for PostgresExplorer {
    #[allow(
        clippy::too_many_lines,
        reason = "one bounded snapshot query keeps its redacted decode mapping adjacent"
    )]
    fn recovery_snapshot<'a>(
        &'a self,
        execution_id: JobExecutionId,
        current_owner: &'a crate::OwnerToken,
    ) -> BoxFuture<'a, Result<crate::RecoverySnapshot, RepositoryError>> {
        Box::pin(async move {
            let row = self
                .fetch_optional(
                    sqlx::query(
                        "SELECT execution.status, execution.attempt, execution.version, \
                         (extract(epoch FROM execution.updated_at) * 1000)::bigint AS updated_ms, \
                         (extract(epoch FROM clock_timestamp()) * 1000)::bigint AS server_ms, \
                         CASE WHEN execution.owner_token IS NULL THEN 'ABSENT' \
                              WHEN execution.owner_token = $2 THEN 'CURRENT' ELSE 'OTHER' END \
                              AS owner_observation, \
                         latest_step.id AS step_id, latest_step.status AS step_status, \
                         latest_step.checkpoint_format, latest_step.checkpoint_schema, \
                         latest_step.checkpoint_schema_version, \
                         pg_column_size(latest_step.checkpoint_payload)::bigint AS checkpoint_bytes, \
                         (execution.status = 'UNKNOWN' \
                           OR COALESCE(execution.failure_category = 'UNKNOWN_COMMIT', false) \
                           OR COALESCE(latest_step.status = 'UNKNOWN', false)) \
                           AS unknown_commit, \
                         EXISTS (SELECT 1 FROM oxide_batch.ob_step_partition partition \
                           JOIN oxide_batch.ob_step_execution parent \
                             ON parent.id = partition.step_execution_id \
                           WHERE parent.job_execution_id = execution.id \
                             AND partition.status = 'COMPLETED') AS completed_partition, \
                         EXISTS (SELECT 1 FROM oxide_batch.ob_flow_decision decision \
                           WHERE decision.job_execution_id = execution.id) \
                           AS committed_flow_decision, \
                         COALESCE(definition.manifest ->> 'delivery_mode', 'ambiguous') \
                           <> 'atomic_same_resource' AS ambiguous_external_effect \
                         FROM oxide_batch.ob_job_execution execution \
                         JOIN oxide_batch.ob_job_definition definition \
                           ON definition.id = execution.definition_id \
                         LEFT JOIN LATERAL ( \
                           SELECT step.id, step.status, step.checkpoint_format, \
                                  step.checkpoint_schema, step.checkpoint_schema_version, \
                                  step.checkpoint_payload \
                           FROM oxide_batch.ob_step_execution step \
                           WHERE step.job_execution_id = execution.id \
                           ORDER BY step.id DESC LIMIT 1 \
                         ) latest_step ON true \
                         WHERE execution.id = $1",
                    )
                    .bind(database_id(
                        execution_id.get(),
                        IdentifierKind::JobExecution,
                    )?)
                    .bind(&current_owner.as_bytes()[..]),
                )
                .await
                .map_err(|error| match error {
                    ExplorerError::Repository(error) => error,
                    _ => RepositoryError::Unavailable,
                })?
                .ok_or(RepositoryError::JobExecutionNotFound { id: execution_id })?;
            let owner = match read_text(&row, "owner_observation")?.as_str() {
                "ABSENT" => crate::OwnerObservation::Absent,
                "CURRENT" => crate::OwnerObservation::CurrentProcess,
                "OTHER" => crate::OwnerObservation::OtherProcess,
                _ => return Err(RepositoryError::Unavailable),
            };
            let latest_step = match read_optional_u64(&row, "step_id")? {
                Some(id) => {
                    let descriptor = decode_state_descriptor(
                        &row,
                        DurableStateKind::Checkpoint,
                        "checkpoint_format",
                        "checkpoint_schema",
                        "checkpoint_schema_version",
                        "checkpoint_bytes",
                    )
                    .map_err(|_| RepositoryError::Unavailable)?;
                    Some(crate::RecoveryStepEvidence::new(
                        StepExecutionId::new(id)?,
                        decode_status(&read_text(&row, "step_status")?)?,
                        Some(descriptor),
                    ))
                }
                None => None,
            };
            Ok(crate::RecoverySnapshot::new(
                execution_id,
                decode_status(&read_text(&row, "status")?)?,
                // `attempt` is an `integer` column, which decodes as `i32`. It
                // is read here the way the execution projection reads it,
                // because reading it as `i64` failed every snapshot with a
                // redacted `Unavailable` and no PostgreSQL test called this.
                u32::try_from(
                    row.try_get::<i32, _>("attempt")
                        .map_err(|_| RepositoryError::Unavailable)?,
                )
                .map_err(|_| RepositoryError::Unavailable)?,
                ExecutionVersion::new(read_u64(&row, "version")?),
                owner,
                millis_system_time(read_i64(&row, "updated_ms")?)?,
                millis_system_time(read_i64(&row, "server_ms")?)?,
                latest_step,
                crate::RecoveryMarkers::new()
                    .with_unknown_commit(
                        row.try_get::<bool, _>("unknown_commit")
                            .map_err(|_| RepositoryError::Unavailable)?,
                    )
                    .with_completed_partition(
                        row.try_get::<bool, _>("completed_partition")
                            .map_err(|_| RepositoryError::Unavailable)?,
                    )
                    .with_committed_flow_decision(
                        row.try_get::<bool, _>("committed_flow_decision")
                            .map_err(|_| RepositoryError::Unavailable)?,
                    )
                    .with_ambiguous_external_effect(
                        row.try_get::<bool, _>("ambiguous_external_effect")
                            .map_err(|_| RepositoryError::Unavailable)?,
                    ),
            ))
        })
    }
}
