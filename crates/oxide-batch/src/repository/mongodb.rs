use std::error::Error;
use std::fmt;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mongodb::bson::{Binary, Bson, Document, doc, spec::BinarySubtype};
use mongodb::error::{
    Error as MongoError, ErrorKind, TRANSIENT_TRANSACTION_ERROR, UNKNOWN_TRANSACTION_COMMIT_RESULT,
    WriteFailure,
};
use mongodb::options::{
    Acknowledgment, ClientOptions, IndexOptions, ReadConcern, ReturnDocument, WriteConcern,
};
use mongodb::{Client, ClientSession, Collection, Database, IndexModel};
use serde_json::{Map, Value, json};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{
    BatchStatus, BoxFuture, Clock, CursorError, CursorKey, DefinitionDescriptor,
    DefinitionIdentity, DefinitionRevision, DefinitionUpgrade, ExecutionCounts, ExecutionMetadata,
    ExecutionTimestamps, ExecutionVersion, ExitCode, ExitStatus, ExplorerError, ExplorerQuery,
    ExplorerRepository, FailureCategory, FailureId, FailureSummary, FlowDecision, FlowDecisionId,
    FlowDecisionRequest, FlowDecisionSequence, FlowStepState, FlowTarget, FlowTransitionKind,
    JobExecution, JobExecutionId, JobExecutionProjection, JobInstance, JobInstanceId,
    JobInstanceKey, JobInstanceProjection, JobInstanceSelection, JobName, JobParameter,
    JobParameters, JobRepository, LifecycleError, LifecycleTransition, NodeId, ParameterDescriptor,
    ParameterName, ParameterRole, ParameterValue, ParameterValueKind, QueryWindow,
    RecoveryDecision, RecoveryRequest, RecoveryResult, RepositoryCapability, RepositoryDescriptor,
    RepositoryError, RepositoryUnitOfWork, StartLimit, StepExecution, StepExecutionId, StepName,
    TerminalKind,
};

const SUPPORTED_SCHEMA_VERSION: u32 = 2;
const DEFAULT_POOL_SIZE: u32 = 10;
const MAX_POOL_SIZE: u32 = 1024;
const MAX_INSTANCE_KEY_INPUT: usize = 1024 * 1024;
const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_SERVER_SELECTION_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_MAX_IDLE_TIME: Duration = Duration::from_mins(30);
const DEFAULT_MAX_COMMIT_TIME: Duration = Duration::from_secs(30);
const MIN_TIMEOUT: Duration = Duration::from_millis(1);
const MAX_TIMEOUT: Duration = Duration::from_mins(10);
const MAX_IDLE: Duration = Duration::from_hours(24);

const SCHEMA: &str = "ob_schema";
const COUNTERS: &str = "ob_counter";
const DEFINITIONS: &str = "ob_core_job_definition";
const INSTANCES: &str = "ob_job_instance";
const EXECUTIONS: &str = "ob_job_execution";
const STEPS: &str = "ob_step_execution";
const DECISIONS: &str = "ob_flow_decision";

/// Redacted first-party `MongoDB` repository configuration.
///
/// The connection string is retained only for driver construction and is never
/// rendered by Debug or Display output.
#[derive(Clone)]
pub struct MongoConfig {
    uri: String,
    database: String,
    pool_size: u32,
    acquire_timeout: Duration,
    connect_timeout: Duration,
    server_selection_timeout: Duration,
    max_idle_time: Duration,
    max_commit_time: Duration,
}

impl MongoConfig {
    /// Constructs a `MongoDB` repository configuration.
    ///
    /// The URI owns authentication, replica-set discovery, and TLS inputs. The
    /// adapter always overrides retryable reads/writes for repository work so
    /// retry authority remains with `OxideBatch`.
    ///
    /// # Errors
    ///
    /// Returns [`MongoConfigError`] when the URI or database input violates the
    /// repository configuration bounds.
    pub fn new(
        uri: impl Into<String>,
        database: impl Into<String>,
    ) -> Result<Self, MongoConfigError> {
        let config = Self {
            uri: uri.into(),
            database: database.into(),
            pool_size: DEFAULT_POOL_SIZE,
            acquire_timeout: DEFAULT_ACQUIRE_TIMEOUT,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            server_selection_timeout: DEFAULT_SERVER_SELECTION_TIMEOUT,
            max_idle_time: DEFAULT_MAX_IDLE_TIME,
            max_commit_time: DEFAULT_MAX_COMMIT_TIME,
        };
        config.validate()?;
        Ok(config)
    }

    /// Sets both the driver pool ceiling and the repository `UoW` concurrency bound.
    ///
    /// # Errors
    ///
    /// Returns [`MongoConfigError`] when `pool_size` is outside the supported range.
    pub fn with_pool_size(mut self, pool_size: u32) -> Result<Self, MongoConfigError> {
        self.pool_size = pool_size;
        self.validate()?;
        Ok(self)
    }

    /// Sets the maximum wait for an `OxideBatch` repository `UoW` permit.
    ///
    /// # Errors
    ///
    /// Returns [`MongoConfigError`] when `value` is outside the supported timeout range.
    pub fn with_acquire_timeout(mut self, value: Duration) -> Result<Self, MongoConfigError> {
        self.acquire_timeout = value;
        self.validate()?;
        Ok(self)
    }

    /// Sets the `MongoDB` driver socket connection timeout.
    ///
    /// # Errors
    ///
    /// Returns [`MongoConfigError`] when `value` is outside the supported timeout range.
    pub fn with_connect_timeout(mut self, value: Duration) -> Result<Self, MongoConfigError> {
        self.connect_timeout = value;
        self.validate()?;
        Ok(self)
    }

    /// Sets the `MongoDB` driver server-selection timeout.
    ///
    /// # Errors
    ///
    /// Returns [`MongoConfigError`] when `value` is outside the supported timeout range.
    pub fn with_server_selection_timeout(
        mut self,
        value: Duration,
    ) -> Result<Self, MongoConfigError> {
        self.server_selection_timeout = value;
        self.validate()?;
        Ok(self)
    }

    /// Sets the maximum idle lifetime for pooled `MongoDB` connections.
    ///
    /// # Errors
    ///
    /// Returns [`MongoConfigError`] when `value` is outside the supported idle range.
    pub fn with_max_idle_time(mut self, value: Duration) -> Result<Self, MongoConfigError> {
        self.max_idle_time = value;
        self.validate()?;
        Ok(self)
    }

    /// Sets the server-side maximum time allowed for transaction commit.
    ///
    /// # Errors
    ///
    /// Returns [`MongoConfigError`] when `value` is outside the supported timeout range.
    pub fn with_max_commit_time(mut self, value: Duration) -> Result<Self, MongoConfigError> {
        self.max_commit_time = value;
        self.validate()?;
        Ok(self)
    }

    fn validate(&self) -> Result<(), MongoConfigError> {
        if self.uri.is_empty() {
            return Err(MongoConfigError::EmptyUri);
        }
        if self.database.is_empty() {
            return Err(MongoConfigError::EmptyDatabase);
        }
        if !(1..=MAX_POOL_SIZE).contains(&self.pool_size) {
            return Err(MongoConfigError::PoolSize);
        }
        for timeout in [
            self.acquire_timeout,
            self.connect_timeout,
            self.server_selection_timeout,
            self.max_commit_time,
        ] {
            if timeout < MIN_TIMEOUT || timeout > MAX_TIMEOUT {
                return Err(MongoConfigError::Timeout);
            }
        }
        if self.max_idle_time < Duration::from_secs(1) || self.max_idle_time > MAX_IDLE {
            return Err(MongoConfigError::IdleTime);
        }
        Ok(())
    }

    async fn client_options(&self) -> Result<ClientOptions, RepositoryError> {
        self.validate().map_err(|_| RepositoryError::Unavailable)?;
        let mut options = ClientOptions::parse(&self.uri)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        options.app_name = Some("OxideBatch".to_owned());
        options.max_pool_size = Some(self.pool_size);
        options.connect_timeout = Some(self.connect_timeout);
        options.server_selection_timeout = Some(self.server_selection_timeout);
        options.max_idle_time = Some(self.max_idle_time);
        options.retry_reads = Some(false);
        options.retry_writes = Some(false);
        Ok(options)
    }
}

impl fmt::Debug for MongoConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MongoConfig")
            .field("uri", &"<redacted>")
            .field("database", &"<redacted>")
            .field("pool_size", &self.pool_size)
            .field("acquire_timeout", &self.acquire_timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("server_selection_timeout", &self.server_selection_timeout)
            .field("max_idle_time", &self.max_idle_time)
            .field("max_commit_time", &self.max_commit_time)
            .finish()
    }
}

/// Validation error for [`MongoConfig`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MongoConfigError {
    /// The `MongoDB` connection URI is empty.
    EmptyUri,
    /// The `MongoDB` database name is empty.
    EmptyDatabase,
    /// The configured pool size is outside the supported bound.
    PoolSize,
    /// A configured operation timeout is outside the supported bound.
    Timeout,
    /// The configured pooled-connection idle lifetime is outside the supported bound.
    IdleTime,
}

impl fmt::Display for MongoConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyUri => "MongoDB connection URI is empty",
            Self::EmptyDatabase => "MongoDB database is empty",
            Self::PoolSize => "MongoDB pool size must be from 1 to 1024",
            Self::Timeout => "MongoDB timeout must be from 1 ms through 10 minutes",
            Self::IdleTime => "MongoDB max idle time must be from 1 second through 24 hours",
        })
    }
}

impl Error for MongoConfigError {}

async fn client_and_database(config: &MongoConfig) -> Result<(Client, Database), RepositoryError> {
    let client = Client::with_options(config.client_options().await?)
        .map_err(|_| RepositoryError::Unavailable)?;
    let database = client.database(&config.database);
    database
        .run_command(doc! { "ping": 1_i32 })
        .await
        .map_err(|_| RepositoryError::Unavailable)?;
    Ok((client, database))
}

/// Owns `MongoDB` collection/index bootstrap and schema compatibility checks.
#[derive(Clone, Copy, Debug, Default)]
pub struct MongoMigrator;

impl MongoMigrator {
    /// Returns the newest `MongoDB` repository schema understood by this adapter.
    #[must_use]
    pub const fn supported_schema_version() -> u32 {
        SUPPORTED_SCHEMA_VERSION
    }

    /// Reads the installed `MongoDB` repository schema version, if initialized.
    ///
    /// # Errors
    ///
    /// Returns [`RepositoryError`] when the repository cannot be reached or its
    /// stored schema marker cannot be decoded.
    pub async fn installed_schema_version(
        config: &MongoConfig,
    ) -> Result<Option<u32>, RepositoryError> {
        let (_, database) = client_and_database(config).await?;
        let row = database
            .collection::<Document>(SCHEMA)
            .find_one(doc! { "_id": "schema" })
            .await
            .map_err(|error| {
                if namespace_missing(&error) {
                    RepositoryError::SchemaUninitialized
                } else {
                    RepositoryError::Unavailable
                }
            });
        match row {
            Ok(Some(row)) => Ok(Some(read_u32(&row, "version")?)),
            Ok(None) | Err(RepositoryError::SchemaUninitialized) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Creates or advances the adapter-owned collections, indexes, and schema marker.
    ///
    /// # Errors
    ///
    /// Returns [`RepositoryError`] when bootstrap fails or the installed schema is
    /// newer than this adapter supports.
    pub async fn migrate(config: &MongoConfig) -> Result<(), RepositoryError> {
        let (_, database) = client_and_database(config).await?;
        if let Some(current) = Self::installed_schema_version(config).await?
            && current > SUPPORTED_SCHEMA_VERSION
        {
            return Err(RepositoryError::NewerSchema {
                current,
                supported: SUPPORTED_SCHEMA_VERSION,
            });
        }

        let existing = database
            .list_collection_names()
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        for name in [
            SCHEMA,
            COUNTERS,
            DEFINITIONS,
            INSTANCES,
            EXECUTIONS,
            STEPS,
            DECISIONS,
        ] {
            if !existing.iter().any(|value| value == name) {
                database
                    .create_collection(name)
                    .await
                    .map_err(|_| RepositoryError::Unavailable)?;
            }
        }

        create_unique_index(
            &database.collection::<Document>(DEFINITIONS),
            "uq_definition_identity",
            doc! { "job_name": 1_i32, "definition_revision": 1_i32 },
        )
        .await?;
        create_unique_index(
            &database.collection::<Document>(DEFINITIONS),
            "uq_definition_id",
            doc! { "id": 1_i32 },
        )
        .await?;
        create_unique_index(
            &database.collection::<Document>(INSTANCES),
            "uq_instance_identity",
            doc! { "job_name": 1_i32, "instance_key": 1_i32 },
        )
        .await?;
        create_unique_index(
            &database.collection::<Document>(INSTANCES),
            "uq_instance_id",
            doc! { "id": 1_i32 },
        )
        .await?;
        create_unique_index(
            &database.collection::<Document>(EXECUTIONS),
            "uq_execution_attempt",
            doc! { "job_instance_id": 1_i32, "attempt": 1_i32 },
        )
        .await?;
        create_unique_index(
            &database.collection::<Document>(EXECUTIONS),
            "uq_execution_id",
            doc! { "id": 1_i32 },
        )
        .await?;
        create_unique_index(
            &database.collection::<Document>(STEPS),
            "uq_step_logical_attempt",
            doc! { "job_execution_id": 1_i32, "step_logical_id": 1_i32 },
        )
        .await?;
        create_unique_index(
            &database.collection::<Document>(STEPS),
            "uq_step_id",
            doc! { "id": 1_i32 },
        )
        .await?;
        create_unique_index(
            &database.collection::<Document>(DECISIONS),
            "uq_decision_sequence",
            doc! { "job_execution_id": 1_i32, "sequence": 1_i32 },
        )
        .await?;
        create_unique_index(
            &database.collection::<Document>(DECISIONS),
            "uq_decision_id",
            doc! { "id": 1_i32 },
        )
        .await?;

        database
            .collection::<Document>(SCHEMA)
            .update_one(
                doc! { "_id": "schema" },
                doc! { "$set": { "version": i64::from(SUPPORTED_SCHEMA_VERSION) } },
            )
            .upsert(true)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        Ok(())
    }
}

async fn create_unique_index(
    collection: &Collection<Document>,
    name: &str,
    keys: Document,
) -> Result<(), RepositoryError> {
    collection
        .create_index(
            IndexModel::builder()
                .keys(keys)
                .options(
                    IndexOptions::builder()
                        .name(name.to_owned())
                        .unique(true)
                        .build(),
                )
                .build(),
        )
        .await
        .map_err(|_| RepositoryError::Unavailable)?;
    Ok(())
}

/// First-party `MongoDB` implementation of the mandatory `OxideBatch` repository profile.
#[derive(Clone)]
pub struct MongoJobRepository {
    client: Client,
    database: Database,
    clock: Arc<dyn Clock>,
    config: MongoConfig,
    permits: Arc<Semaphore>,
}

impl MongoJobRepository {
    /// Connects to an initialized `MongoDB` repository using the supplied clock.
    ///
    /// # Errors
    ///
    /// Returns [`RepositoryError`] when the repository is unavailable, uninitialized,
    /// or requires a schema migration.
    pub async fn connect(
        config: MongoConfig,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, RepositoryError> {
        let (client, database) = client_and_database(&config).await?;
        verify_schema_version(
            MongoMigrator::installed_schema_version(&config)
                .await?
                .ok_or(RepositoryError::SchemaUninitialized)?,
        )?;
        Ok(Self {
            client,
            database,
            clock,
            permits: Arc::new(Semaphore::new(config.pool_size as usize)),
            config,
        })
    }

    fn collection(&self, name: &str) -> Collection<Document> {
        self.database.collection(name)
    }
}

impl fmt::Debug for MongoJobRepository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MongoJobRepository")
            .field("pool_size", &self.config.pool_size)
            .field("available_uow_permits", &self.permits.available_permits())
            .finish_non_exhaustive()
    }
}

/// Read-only bounded Explorer adapter for MongoDB metadata.
///
/// This port is additive and deliberately does not advertise a
/// `RepositoryCapability`. Unsupported queries retain the
/// `ExplorerRepository` fail-closed defaults.
#[derive(Clone)]
pub struct MongoExplorer {
    repository: MongoJobRepository,
}

impl MongoExplorer {
    /// Binds one MongoDB repository to the bounded read port.
    #[must_use]
    pub const fn new(repository: MongoJobRepository) -> Self {
        Self { repository }
    }
}

impl fmt::Debug for MongoExplorer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MongoExplorer")
            .finish_non_exhaustive()
    }
}

impl ExplorerRepository for MongoExplorer {
    fn identity_ceiling<'a>(
        &'a self,
        query: &'a ExplorerQuery,
    ) -> BoxFuture<'a, Result<u64, ExplorerError>> {
        Box::pin(async move {
            let ExplorerQuery::Instances { job_name } = query else {
                return Err(ExplorerError::UnsupportedCapability);
            };
            let row = self
                .repository
                .collection(INSTANCES)
                .find_one(doc! { "job_name": job_name.as_str() })
                .sort(doc! { "id": -1_i32 })
                .projection(doc! { "_id": 0_i32, "id": 1_i32 })
                .await
                .map_err(|_| explorer_unavailable())?;
            row.as_ref()
                .map(|row| read_u64(row, "id").map_err(ExplorerError::Repository))
                .transpose()
                .map(Option::unwrap_or_default)
        })
    }

    fn instances<'a>(
        &'a self,
        job_name: &'a JobName,
        window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<JobInstanceProjection>, ExplorerError>> {
        Box::pin(async move {
            let ceiling = i64::try_from(window.ceiling()).map_err(|_| explorer_unavailable())?;
            let mut id_window = doc! { "$lte": ceiling };
            if let Some(after) = mongo_window_identity(window)? {
                id_window.insert("$lt", after);
            }
            let mut cursor = self
                .repository
                .collection(INSTANCES)
                .find(doc! {
                    "job_name": job_name.as_str(),
                    "id": id_window,
                })
                .sort(doc! { "id": -1_i32 })
                .limit(i64::from(window.limit()))
                .await
                .map_err(|_| explorer_unavailable())?;
            let mut rows = Vec::with_capacity(usize::from(window.limit()));
            while cursor.advance().await.map_err(|_| explorer_unavailable())? {
                let row = cursor
                    .deserialize_current()
                    .map_err(|_| explorer_unavailable())?;
                rows.push(decode_mongo_instance_projection(&row)?);
            }
            Ok(rows)
        })
    }

    fn execution(
        &self,
        job_execution_id: JobExecutionId,
    ) -> BoxFuture<'_, Result<Option<JobExecutionProjection>, ExplorerError>> {
        Box::pin(async move {
            let mut cursor = self
                .repository
                .collection(EXECUTIONS)
                .aggregate(vec![
                    doc! {
                        "$match": {
                            "id": u64_to_i64(job_execution_id.get())
                                .map_err(ExplorerError::Repository)?
                        }
                    },
                    doc! { "$limit": 1_i64 },
                    doc! {
                        "$lookup": {
                            "from": INSTANCES,
                            "localField": "job_instance_id",
                            "foreignField": "id",
                            "as": "explorer_instance",
                        }
                    },
                    doc! {
                        "$lookup": {
                            "from": DEFINITIONS,
                            "localField": "definition_id",
                            "foreignField": "id",
                            "as": "explorer_definition",
                        }
                    },
                ])
                .await
                .map_err(|_| explorer_unavailable())?;
            if !cursor.advance().await.map_err(|_| explorer_unavailable())? {
                return Ok(None);
            }
            let row = cursor
                .deserialize_current()
                .map_err(|_| explorer_unavailable())?;
            decode_mongo_execution_projection(&row).map(Some)
        })
    }
}

impl JobRepository for MongoJobRepository {
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
            let permit = tokio::time::timeout(
                self.config.acquire_timeout,
                Arc::clone(&self.permits).acquire_owned(),
            )
            .await
            .map_err(|_| RepositoryError::Unavailable)?
            .map_err(|_| RepositoryError::Unavailable)?;
            let mut session = self
                .client
                .start_session()
                .await
                .map_err(|_| RepositoryError::Unavailable)?;
            session
                .start_transaction()
                .read_concern(ReadConcern::snapshot())
                .write_concern(
                    WriteConcern::builder()
                        .w(Acknowledgment::Majority)
                        .journal(true)
                        .build(),
                )
                .max_commit_time(self.config.max_commit_time)
                .await
                .map_err(|_| RepositoryError::Unavailable)?;
            Ok(Box::new(MongoUnitOfWork {
                repository: self,
                session: Some(session),
                _permit: permit,
            }) as Box<dyn RepositoryUnitOfWork + 'a>)
        })
    }
}

struct MongoUnitOfWork<'repository> {
    repository: &'repository MongoJobRepository,
    session: Option<ClientSession>,
    _permit: OwnedSemaphorePermit,
}

impl MongoUnitOfWork<'_> {
    fn session(&mut self) -> Result<&mut ClientSession, RepositoryError> {
        self.session.as_mut().ok_or(RepositoryError::Unavailable)
    }

    async fn next_id(&mut self, kind: &'static str) -> Result<u64, RepositoryError> {
        let collection = self.repository.collection(COUNTERS);
        let row = collection
            .find_one_and_update(doc! { "_id": kind }, doc! { "$inc": { "value": 1_i64 } })
            .upsert(true)
            .return_document(ReturnDocument::After)
            .session(self.session()?)
            .await
            .map_err(map_operation_error)?
            .ok_or(RepositoryError::Unavailable)?;
        i64_to_u64(read_i64(&row, "value")?)
    }

    async fn touch_instance(&mut self, id: JobInstanceId) -> Result<(), RepositoryError> {
        let collection = self.repository.collection(INSTANCES);
        let result = collection
            .update_one(
                doc! { "id": u64_to_i64(id.get())? },
                doc! { "$inc": { "lock_version": 1_i64 } },
            )
            .session(self.session()?)
            .await
            .map_err(map_operation_error)?;
        if result.matched_count == 1 {
            Ok(())
        } else {
            Err(RepositoryError::JobInstanceNotFound { id })
        }
    }

    async fn ensure_core_definition(
        &mut self,
        job_name: &JobName,
        definition: &DefinitionIdentity,
    ) -> Result<u64, RepositoryError> {
        let id = self.next_id("definition").await?;
        let manifest = std::str::from_utf8(definition.canonical_manifest())
            .map_err(|_| RepositoryError::Unavailable)?;
        let created_ms = system_time_millis(self.repository.clock.now())?;
        let collection = self.repository.collection(DEFINITIONS);
        collection
            .update_one(
                doc! {
                    "job_name": job_name.as_str(),
                    "definition_revision": definition.revision().as_str(),
                },
                doc! {
                    "$setOnInsert": {
                        "id": u64_to_i64(id)?,
                        "job_name": job_name.as_str(),
                        "definition_revision": definition.revision().as_str(),
                        "manifest_format": i32::from(definition.manifest_format()),
                        "manifest_digest": binary(definition.manifest_digest()),
                        "manifest": manifest,
                        "created_ms": created_ms,
                    }
                },
            )
            .upsert(true)
            .session(self.session()?)
            .await
            .map_err(map_operation_error)?;

        let row = collection
            .find_one(doc! {
                "job_name": job_name.as_str(),
                "definition_revision": definition.revision().as_str(),
            })
            .session(self.session()?)
            .await
            .map_err(map_operation_error)?
            .ok_or(RepositoryError::Unavailable)?;
        if read_bytes(&row, "manifest_digest")?.as_slice() != definition.manifest_digest() {
            return Err(RepositoryError::DefinitionDrift {
                job_name: job_name.clone(),
                revision: definition.revision().clone(),
            });
        }
        read_u64(&row, "id")
    }

    async fn raw_job_execution(
        &mut self,
        id: JobExecutionId,
    ) -> Result<Option<Document>, RepositoryError> {
        let collection = self.repository.collection(EXECUTIONS);
        collection
            .find_one(doc! { "id": u64_to_i64(id.get())? })
            .session(self.session()?)
            .await
            .map_err(map_operation_error)
    }

    async fn raw_step_execution(
        &mut self,
        id: StepExecutionId,
    ) -> Result<Option<Document>, RepositoryError> {
        let collection = self.repository.collection(STEPS);
        collection
            .find_one(doc! { "id": u64_to_i64(id.get())? })
            .session(self.session()?)
            .await
            .map_err(map_operation_error)
    }

    async fn classify_job_cas(
        &mut self,
        id: JobExecutionId,
        expected: ExecutionVersion,
    ) -> RepositoryError {
        match self.raw_job_execution(id).await {
            Ok(Some(row)) => match decode_job_execution(&row) {
                Ok(actual) => RepositoryError::Lifecycle(LifecycleError::StaleVersion {
                    expected,
                    actual: actual.version(),
                }),
                Err(error) => error,
            },
            Ok(None) => RepositoryError::JobExecutionNotFound { id },
            Err(error) => error,
        }
    }

    async fn classify_step_cas(
        &mut self,
        id: StepExecutionId,
        expected: ExecutionVersion,
    ) -> RepositoryError {
        match self.raw_step_execution(id).await {
            Ok(Some(row)) => match decode_step_execution(&row) {
                Ok(actual) => RepositoryError::Lifecycle(LifecycleError::StaleVersion {
                    expected,
                    actual: actual.version(),
                }),
                Err(error) => error,
            },
            Ok(None) => RepositoryError::StepExecutionNotFound { id },
            Err(error) => error,
        }
    }
}

impl RepositoryUnitOfWork for MongoUnitOfWork<'_> {
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
            let id = self.next_id("job_instance").await?;
            let encoded = serde_json::to_string(&encode_identifying_parameters(key)?)
                .map_err(|_| RepositoryError::Unavailable)?;
            let collection = self.repository.collection(INSTANCES);
            let result = collection
                .update_one(
                    doc! {
                        "job_name": key.job_name().as_str(),
                        "instance_key": binary(&key.digest()),
                    },
                    doc! {
                        "$setOnInsert": {
                            "id": u64_to_i64(id)?,
                            "job_name": key.job_name().as_str(),
                            "instance_key": binary(&key.digest()),
                            "identifying_parameters": encoded,
                            "lock_version": 0_i64,
                            "created_ms": system_time_millis(self.repository.clock.now())?,
                        }
                    },
                )
                .upsert(true)
                .session(self.session()?)
                .await
                .map_err(map_operation_error)?;
            let row = collection
                .find_one(doc! {
                    "job_name": key.job_name().as_str(),
                    "instance_key": binary(&key.digest()),
                })
                .session(self.session()?)
                .await
                .map_err(map_operation_error)?
                .ok_or(RepositoryError::Unavailable)?;
            let instance = decode_job_instance(&row)?;
            if result.upserted_id.is_some() {
                Ok(JobInstanceSelection::Created(instance))
            } else {
                Ok(JobInstanceSelection::Existing(instance))
            }
        })
    }

    fn create_job_execution(
        &mut self,
        job_instance_id: JobInstanceId,
    ) -> BoxFuture<'_, Result<JobExecution, RepositoryError>> {
        Box::pin(async move {
            self.touch_instance(job_instance_id).await?;
            let collection = self.repository.collection(EXECUTIONS);
            let latest = collection
                .find_one(doc! { "job_instance_id": u64_to_i64(job_instance_id.get())? })
                .sort(doc! { "attempt": -1_i32 })
                .session(self.session()?)
                .await
                .map_err(map_operation_error)?;

            let attempt = next_attempt(job_instance_id, latest.as_ref())?;
            let id = self.next_id("job_execution").await?;
            let created_at = self.repository.clock.now();
            collection
                .insert_one(job_execution_document(
                    id,
                    job_instance_id,
                    None,
                    attempt,
                    created_at,
                )?)
                .session(self.session()?)
                .await
                .map_err(map_operation_error)?;
            Ok(JobExecution::new(
                JobExecutionId::new(id)?,
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
            self.touch_instance(job_instance_id).await?;
            let instances = self.repository.collection(INSTANCES);
            let instance = instances
                .find_one(doc! { "id": u64_to_i64(job_instance_id.get())? })
                .session(self.session()?)
                .await
                .map_err(map_operation_error)?
                .ok_or(RepositoryError::JobInstanceNotFound {
                    id: job_instance_id,
                })?;
            let job_name = JobName::new(read_string(&instance, "job_name")?)?;
            if let Some(actual) = definition.job_name()
                && actual != &job_name
            {
                return Err(RepositoryError::DefinitionJobMismatch {
                    expected: job_name,
                    actual: actual.clone(),
                });
            }
            let definition_id = self.ensure_core_definition(&job_name, definition).await?;

            let executions = self.repository.collection(EXECUTIONS);
            let latest = executions
                .find_one(doc! { "job_instance_id": u64_to_i64(job_instance_id.get())? })
                .sort(doc! { "attempt": -1_i32 })
                .session(self.session()?)
                .await
                .map_err(map_operation_error)?;
            let attempt = next_attempt(job_instance_id, latest.as_ref())?;
            if let Some(latest) = &latest {
                let prior_definition_id = read_optional_u64(latest, "definition_id")?.ok_or(
                    RepositoryError::IncompatibleDefinition {
                        instance_id: job_instance_id,
                    },
                )?;
                let definitions = self.repository.collection(DEFINITIONS);
                let prior = definitions
                    .find_one(doc! { "id": u64_to_i64(prior_definition_id)? })
                    .session(self.session()?)
                    .await
                    .map_err(map_operation_error)?
                    .ok_or(RepositoryError::IncompatibleDefinition {
                        instance_id: job_instance_id,
                    })?;
                if read_bytes(&prior, "manifest_digest")?.as_slice() != definition.manifest_digest()
                {
                    return Err(RepositoryError::IncompatibleDefinition {
                        instance_id: job_instance_id,
                    });
                }
            }

            let id = self.next_id("job_execution").await?;
            let created_at = self.repository.clock.now();
            executions
                .insert_one(job_execution_document(
                    id,
                    job_instance_id,
                    Some(definition_id),
                    attempt,
                    created_at,
                )?)
                .session(self.session()?)
                .await
                .map_err(map_operation_error)?;
            Ok(JobExecution::new(
                JobExecutionId::new(id)?,
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
            let executions = self.repository.collection(EXECUTIONS);
            let job = executions
                .find_one(doc! { "id": u64_to_i64(job_execution_id.get())? })
                .session(self.session()?)
                .await
                .map_err(map_operation_error)?
                .ok_or(RepositoryError::JobExecutionNotFound {
                    id: job_execution_id,
                })?;
            let instance_id = JobInstanceId::new(read_u64(&job, "job_instance_id")?)?;
            self.touch_instance(instance_id).await?;

            let mut cursor = executions
                .aggregate(vec![
                    doc! { "$match": { "job_instance_id": u64_to_i64(instance_id.get())? } },
                    doc! {
                        "$lookup": {
                            "from": STEPS,
                            "let": { "execution_id": "$id" },
                            "pipeline": [
                                {
                                    "$match": {
                                        "$expr": {
                                            "$and": [
                                                { "$eq": ["$job_execution_id", "$$execution_id"] },
                                                { "$eq": ["$step_logical_id", node_id.as_str()] },
                                            ]
                                        }
                                    }
                                },
                                { "$limit": 1_i64 },
                            ],
                            "as": "matching_steps",
                        }
                    },
                    doc! { "$match": { "matching_steps.0": { "$exists": true } } },
                    doc! { "$count": "count" },
                ])
                .session(self.session()?)
                .await
                .map_err(map_operation_error)?;
            let starts = cursor
                .next(self.session()?)
                .await
                .transpose()
                .map_err(map_operation_error)?
                .as_ref()
                .map(|row| read_numeric_u64(row, "count"))
                .transpose()?
                .unwrap_or(0);
            if starts >= u64::from(start_limit.get()) {
                return Err(RepositoryError::StartLimitExceeded {
                    instance_id,
                    node_id: node_id.clone(),
                    limit: start_limit,
                });
            }

            let id = self.next_id("step_execution").await?;
            let created_at = self.repository.clock.now();
            let steps = self.repository.collection(STEPS);
            steps
                .insert_one(step_execution_document(
                    id,
                    job_execution_id,
                    step_name,
                    node_id,
                    created_at,
                )?)
                .session(self.session()?)
                .await
                .map_err(map_operation_error)?;
            Ok(StepExecution::new(
                StepExecutionId::new(id)?,
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
            let row = self
                .raw_job_execution(id)
                .await?
                .ok_or(RepositoryError::JobExecutionNotFound { id })?;
            let mut execution = decode_job_execution(&row)?;
            execution.transition(expected_version, transition)?;
            let collection = self.repository.collection(EXECUTIONS);
            let result = collection
                .update_one(
                    doc! {
                        "id": u64_to_i64(id.get())?,
                        "version": u64_to_i64(expected_version.get())?,
                    },
                    doc! {
                        "$set": execution_metadata_update(
                            execution.metadata(),
                            execution.version(),
                            transition.transitioned_at(),
                            false,
                        )?
                    },
                )
                .session(self.session()?)
                .await
                .map_err(map_operation_error)?;
            if result.matched_count != 1 {
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
            let row = self
                .raw_job_execution(id)
                .await?
                .ok_or(RepositoryError::JobExecutionNotFound { id })?;
            let mut execution = decode_job_execution(&row)?;
            execution.enrich_exit_status(expected_version, exit_status.clone())?;
            let collection = self.repository.collection(EXECUTIONS);
            let result = collection
                .update_one(
                    doc! {
                        "id": u64_to_i64(id.get())?,
                        "version": u64_to_i64(expected_version.get())?,
                    },
                    doc! {
                        "$set": execution_metadata_update(
                            execution.metadata(),
                            execution.version(),
                            self.repository.clock.now(),
                            false,
                        )?
                    },
                )
                .session(self.session()?)
                .await
                .map_err(map_operation_error)?;
            if result.matched_count != 1 {
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
            let row = self
                .raw_step_execution(id)
                .await?
                .ok_or(RepositoryError::StepExecutionNotFound { id })?;
            let mut execution = decode_step_execution(&row)?;
            execution.transition(expected_version, transition)?;
            let collection = self.repository.collection(STEPS);
            let result = collection
                .update_one(
                    doc! {
                        "id": u64_to_i64(id.get())?,
                        "version": u64_to_i64(expected_version.get())?,
                    },
                    doc! {
                        "$set": execution_metadata_update(
                            execution.metadata(),
                            execution.version(),
                            transition.transitioned_at(),
                            true,
                        )?
                    },
                )
                .session(self.session()?)
                .await
                .map_err(map_operation_error)?;
            if result.matched_count != 1 {
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
            let row = self
                .raw_step_execution(id)
                .await?
                .ok_or(RepositoryError::StepExecutionNotFound { id })?;
            let mut execution = decode_step_execution(&row)?;
            execution.enrich_exit_status(expected_version, exit_status.clone())?;
            let collection = self.repository.collection(STEPS);
            let result = collection
                .update_one(
                    doc! {
                        "id": u64_to_i64(id.get())?,
                        "version": u64_to_i64(expected_version.get())?,
                    },
                    doc! {
                        "$set": execution_metadata_update(
                            execution.metadata(),
                            execution.version(),
                            self.repository.clock.now(),
                            true,
                        )?
                    },
                )
                .session(self.session()?)
                .await
                .map_err(map_operation_error)?;
            if result.matched_count != 1 {
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
            let collection = self.repository.collection(INSTANCES);
            collection
                .find_one(doc! {
                    "job_name": key.job_name().as_str(),
                    "instance_key": binary(&key.digest()),
                })
                .session(self.session()?)
                .await
                .map_err(map_operation_error)?
                .map(|row| decode_job_instance(&row))
                .transpose()
        })
    }

    fn get_job_instance(
        &mut self,
        id: JobInstanceId,
    ) -> BoxFuture<'_, Result<Option<JobInstance>, RepositoryError>> {
        Box::pin(async move {
            let collection = self.repository.collection(INSTANCES);
            collection
                .find_one(doc! { "id": u64_to_i64(id.get())? })
                .session(self.session()?)
                .await
                .map_err(map_operation_error)?
                .map(|row| decode_job_instance(&row))
                .transpose()
        })
    }

    fn get_job_execution(
        &mut self,
        id: JobExecutionId,
    ) -> BoxFuture<'_, Result<Option<JobExecution>, RepositoryError>> {
        Box::pin(async move {
            self.raw_job_execution(id)
                .await?
                .map(|row| decode_job_execution(&row))
                .transpose()
        })
    }

    fn latest_job_execution(
        &mut self,
        job_instance_id: JobInstanceId,
    ) -> BoxFuture<'_, Result<Option<JobExecution>, RepositoryError>> {
        Box::pin(async move {
            let instances = self.repository.collection(INSTANCES);
            if instances
                .find_one(doc! { "id": u64_to_i64(job_instance_id.get())? })
                .session(self.session()?)
                .await
                .map_err(map_operation_error)?
                .is_none()
            {
                return Err(RepositoryError::JobInstanceNotFound {
                    id: job_instance_id,
                });
            }
            let collection = self.repository.collection(EXECUTIONS);
            collection
                .find_one(doc! { "job_instance_id": u64_to_i64(job_instance_id.get())? })
                .sort(doc! { "attempt": -1_i32 })
                .session(self.session()?)
                .await
                .map_err(map_operation_error)?
                .map(|row| decode_job_execution(&row))
                .transpose()
        })
    }

    fn job_execution_attempt(
        &mut self,
        job_execution_id: JobExecutionId,
    ) -> BoxFuture<'_, Result<NonZeroU64, RepositoryError>> {
        Box::pin(async move {
            let row = self.raw_job_execution(job_execution_id).await?.ok_or(
                RepositoryError::JobExecutionNotFound {
                    id: job_execution_id,
                },
            )?;
            NonZeroU64::new(read_u64(&row, "attempt")?).ok_or(RepositoryError::Unavailable)
        })
    }
    fn get_step_execution(
        &mut self,
        id: StepExecutionId,
    ) -> BoxFuture<'_, Result<Option<StepExecution>, RepositoryError>> {
        Box::pin(async move {
            self.raw_step_execution(id)
                .await?
                .map(|row| decode_step_execution(&row))
                .transpose()
        })
    }

    fn flow_step_execution<'a>(
        &'a mut self,
        job_execution_id: JobExecutionId,
        node_id: &'a NodeId,
    ) -> BoxFuture<'a, Result<Option<StepExecution>, RepositoryError>> {
        Box::pin(async move {
            if self.raw_job_execution(job_execution_id).await?.is_none() {
                return Err(RepositoryError::JobExecutionNotFound {
                    id: job_execution_id,
                });
            }
            let collection = self.repository.collection(STEPS);
            collection
                .find_one(doc! {
                    "job_execution_id": u64_to_i64(job_execution_id.get())?,
                    "step_logical_id": node_id.as_str(),
                })
                .session(self.session()?)
                .await
                .map_err(map_operation_error)?
                .map(|row| decode_step_execution(&row))
                .transpose()
        })
    }
    fn latest_flow_step<'a>(
        &'a mut self,
        job_instance_id: JobInstanceId,
        node_id: &'a NodeId,
    ) -> BoxFuture<'a, Result<Option<FlowStepState>, RepositoryError>> {
        Box::pin(async move {
            let instances = self.repository.collection(INSTANCES);
            if instances
                .find_one(doc! { "id": u64_to_i64(job_instance_id.get())? })
                .session(self.session()?)
                .await
                .map_err(map_operation_error)?
                .is_none()
            {
                return Err(RepositoryError::JobInstanceNotFound {
                    id: job_instance_id,
                });
            }

            let executions = self.repository.collection(EXECUTIONS);
            let mut cursor = executions
                .aggregate(vec![
                    doc! { "$match": { "job_instance_id": u64_to_i64(job_instance_id.get())? } },
                    doc! { "$sort": { "attempt": -1_i32 } },
                    doc! {
                        "$lookup": {
                            "from": STEPS,
                            "let": { "execution_id": "$id" },
                            "pipeline": [
                                {
                                    "$match": {
                                        "$expr": {
                                            "$and": [
                                                { "$eq": ["$job_execution_id", "$$execution_id"] },
                                                { "$eq": ["$step_logical_id", node_id.as_str()] },
                                            ]
                                        }
                                    }
                                },
                                { "$limit": 1_i64 },
                            ],
                            "as": "matching_steps",
                        }
                    },
                    doc! { "$match": { "matching_steps.0": { "$exists": true } } },
                    doc! { "$limit": 1_i64 },
                    doc! {
                        "$project": {
                            "_id": 0_i32,
                            "step": { "$arrayElemAt": ["$matching_steps", 0_i32] },
                        }
                    },
                ])
                .session(self.session()?)
                .await
                .map_err(map_operation_error)?;
            let Some(row) = cursor
                .next(self.session()?)
                .await
                .transpose()
                .map_err(map_operation_error)?
            else {
                return Ok(None);
            };
            let step = row
                .get_document("step")
                .map_err(|_| RepositoryError::Unavailable)?;
            Ok(Some(FlowStepState::new(
                node_id.clone(),
                decode_step_execution(step)?,
                None,
            )))
        })
    }
    fn append_flow_decision<'a>(
        &'a mut self,
        request: &'a FlowDecisionRequest,
    ) -> BoxFuture<'a, Result<FlowDecision, RepositoryError>> {
        Box::pin(async move {
            let executions = self.repository.collection(EXECUTIONS);
            let execution = executions
                .find_one(doc! { "id": u64_to_i64(request.job_execution_id().get())? })
                .session(self.session()?)
                .await
                .map_err(map_operation_error)?
                .ok_or(RepositoryError::JobExecutionNotFound {
                    id: request.job_execution_id(),
                })?;
            let definition_id = read_optional_u64(&execution, "definition_id")?
                .ok_or(RepositoryError::FlowStateCorrupt)?;
            let definitions = self.repository.collection(DEFINITIONS);
            let definition = definitions
                .find_one(doc! { "id": u64_to_i64(definition_id)? })
                .session(self.session()?)
                .await
                .map_err(map_operation_error)?
                .ok_or(RepositoryError::FlowStateCorrupt)?;
            if read_bytes(&definition, "manifest_digest")?.as_slice() != request.plan_fingerprint()
            {
                return Err(RepositoryError::FlowStateCorrupt);
            }
            let manifest: Value = serde_json::from_str(&read_string(&definition, "manifest")?)
                .map_err(|_| RepositoryError::FlowStateCorrupt)?;
            if !crate::flow::decision_matches_manifest(&manifest, request) {
                return Err(RepositoryError::FlowStateCorrupt);
            }

            let decisions = self.repository.collection(DECISIONS);
            let existing = decisions
                .count_documents(doc! {
                    "job_execution_id": u64_to_i64(request.job_execution_id().get())?
                })
                .session(self.session()?)
                .await
                .map_err(map_operation_error)?;
            let advanced = manifest
                .get("format")
                .and_then(Value::as_u64)
                .is_some_and(|format| {
                    format == u64::from(oxide_batch_core::MANIFEST_FORMAT_ADVANCED_FLOW)
                });
            if !advanced && request.sequence().get() != existing.saturating_add(1) {
                return Err(RepositoryError::ConcurrentModification);
            }

            if let Some(step_id) = request.source_step_execution_id() {
                let steps = self.repository.collection(STEPS);
                let source = steps
                    .find_one(doc! {
                        "id": u64_to_i64(step_id.get())?,
                        "step_logical_id": request.source_node_id().as_str(),
                    })
                    .session(self.session()?)
                    .await
                    .map_err(map_operation_error)?
                    .ok_or(RepositoryError::FlowStateCorrupt)?;
                let source_job = executions
                    .find_one(doc! { "id": read_i64(&source, "job_execution_id")? })
                    .session(self.session()?)
                    .await
                    .map_err(map_operation_error)?
                    .ok_or(RepositoryError::FlowStateCorrupt)?;
                if read_i64(&source_job, "job_instance_id")?
                    != read_i64(&execution, "job_instance_id")?
                {
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

            if let Some(reused) = request.reused_decision_id() {
                let prior = decisions
                    .find_one(doc! { "id": u64_to_i64(reused.get())? })
                    .session(self.session()?)
                    .await
                    .map_err(map_operation_error)?
                    .ok_or(RepositoryError::FlowStateCorrupt)?;
                let prior_job = executions
                    .find_one(doc! { "id": read_i64(&prior, "job_execution_id")? })
                    .session(self.session()?)
                    .await
                    .map_err(map_operation_error)?
                    .ok_or(RepositoryError::FlowStateCorrupt)?;
                if read_i64(&prior_job, "job_instance_id")?
                    != read_i64(&execution, "job_instance_id")?
                    || read_string(&prior, "source_node_id")? != request.source_node_id().as_str()
                    || read_bytes(&prior, "plan_fingerprint")?.as_slice()
                        != request.plan_fingerprint()
                    || read_bytes(&prior, "input_digest")?.as_slice() != request.input_digest()
                    || read_string(&prior, "observed_outcome")?
                        != request.observed_outcome().as_str()
                    || decode_flow_target(&prior)? != *request.target()
                {
                    return Err(RepositoryError::FlowStateCorrupt);
                }
            }

            let id = self.next_id("flow_decision").await?;
            decisions
                .insert_one(flow_decision_document(id, request)?)
                .session(self.session()?)
                .await
                .map_err(map_operation_error)?;
            Ok(request.materialize(FlowDecisionId::new(id)?))
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
            let instances = self.repository.collection(INSTANCES);
            if instances
                .find_one(doc! { "id": u64_to_i64(job_instance_id.get())? })
                .session(self.session()?)
                .await
                .map_err(map_operation_error)?
                .is_none()
            {
                return Err(RepositoryError::JobInstanceNotFound {
                    id: job_instance_id,
                });
            }

            let executions = self.repository.collection(EXECUTIONS);
            let mut cursor = executions
                .aggregate(vec![
                    doc! { "$match": { "job_instance_id": u64_to_i64(job_instance_id.get())? } },
                    doc! { "$sort": { "attempt": -1_i32 } },
                    doc! {
                        "$lookup": {
                            "from": DECISIONS,
                            "let": { "execution_id": "$id" },
                            "pipeline": [
                                {
                                    "$match": {
                                        "$expr": {
                                            "$eq": ["$job_execution_id", "$$execution_id"]
                                        },
                                        "source_node_id": node_id.as_str(),
                                        "plan_fingerprint": binary(plan_fingerprint),
                                        "input_digest": binary(input_digest),
                                        "transition_kind": kind.durable_code(),
                                    }
                                },
                                { "$sort": { "sequence": -1_i32 } },
                                { "$limit": 1_i64 },
                            ],
                            "as": "matching_decisions",
                        }
                    },
                    doc! { "$match": { "matching_decisions.0": { "$exists": true } } },
                    doc! { "$limit": 1_i64 },
                    doc! {
                        "$project": {
                            "_id": 0_i32,
                            "decision": { "$arrayElemAt": ["$matching_decisions", 0_i32] },
                        }
                    },
                ])
                .session(self.session()?)
                .await
                .map_err(map_operation_error)?;
            let row = cursor
                .next(self.session()?)
                .await
                .transpose()
                .map_err(map_operation_error)?;
            row.map(|row| {
                let decision = row
                    .get_document("decision")
                    .map_err(|_| RepositoryError::Unavailable)?;
                decode_flow_decision(decision)
            })
            .transpose()
        })
    }
    fn get_flow_decision(
        &mut self,
        id: FlowDecisionId,
    ) -> BoxFuture<'_, Result<Option<FlowDecision>, RepositoryError>> {
        Box::pin(async move {
            let collection = self.repository.collection(DECISIONS);
            collection
                .find_one(doc! { "id": u64_to_i64(id.get())? })
                .session(self.session()?)
                .await
                .map_err(map_operation_error)?
                .map(|row| decode_flow_decision(&row))
                .transpose()
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
            let mut session = self.session.take().ok_or(RepositoryError::Unavailable)?;
            session
                .commit_transaction()
                .await
                .map_err(|error| map_commit_error(&error))
        })
    }
    fn rollback<'a>(mut self: Box<Self>) -> BoxFuture<'a, Result<(), RepositoryError>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            let mut session = self.session.take().ok_or(RepositoryError::Unavailable)?;
            session
                .abort_transaction()
                .await
                .map_err(|_| RepositoryError::Unavailable)
        })
    }
}

fn next_attempt(
    instance_id: JobInstanceId,
    latest: Option<&Document>,
) -> Result<u64, RepositoryError> {
    let Some(latest) = latest else {
        return Ok(1);
    };
    let execution = decode_job_execution(latest)?;
    match execution.metadata().status() {
        BatchStatus::Stopped | BatchStatus::Failed => {}
        BatchStatus::Completed => {
            return Err(RepositoryError::CompletedInstance { id: instance_id });
        }
        BatchStatus::Abandoned => {
            return Err(RepositoryError::AbandonedInstance { id: instance_id });
        }
        status => {
            return Err(RepositoryError::ExecutionAlreadyActive {
                instance_id,
                execution_id: execution.id(),
                status,
            });
        }
    }
    read_u64(latest, "attempt")?
        .checked_add(1)
        .ok_or(RepositoryError::Unavailable)
}

fn job_execution_document(
    id: u64,
    instance_id: JobInstanceId,
    definition_id: Option<u64>,
    attempt: u64,
    created_at: SystemTime,
) -> Result<Document, RepositoryError> {
    let created_ms = system_time_millis(created_at)?;
    let mut row = doc! {
        "id": u64_to_i64(id)?,
        "job_instance_id": u64_to_i64(instance_id.get())?,
        "attempt": u64_to_i64(attempt)?,
        "status": "STARTING",
        "exit_code": "UNKNOWN",
        "created_ms": created_ms,
        "updated_ms": created_ms,
        "version": 0_i64,
        "read_count": 0_i64,
        "processed_count": 0_i64,
        "write_count": 0_i64,
        "filter_count": 0_i64,
        "commit_count": 0_i64,
        "rollback_count": 0_i64,
    };
    if let Some(value) = definition_id {
        row.insert("definition_id", u64_to_i64(value)?);
    }
    Ok(row)
}

fn step_execution_document(
    id: u64,
    job_execution_id: JobExecutionId,
    step_name: &StepName,
    node_id: &NodeId,
    created_at: SystemTime,
) -> Result<Document, RepositoryError> {
    let created_ms = system_time_millis(created_at)?;
    Ok(doc! {
        "id": u64_to_i64(id)?,
        "job_execution_id": u64_to_i64(job_execution_id.get())?,
        "step_name": step_name.as_str(),
        "step_logical_id": node_id.as_str(),
        "status": "STARTING",
        "exit_code": "UNKNOWN",
        "created_ms": created_ms,
        "updated_ms": created_ms,
        "version": 0_i64,
        "read_count": 0_i64,
        "processed_count": 0_i64,
        "write_count": 0_i64,
        "filter_count": 0_i64,
        "commit_count": 0_i64,
        "rollback_count": 0_i64,
    })
}

fn execution_metadata_update(
    metadata: &ExecutionMetadata,
    version: ExecutionVersion,
    updated_at: SystemTime,
    include_counts: bool,
) -> Result<Document, RepositoryError> {
    let mut update = doc! {
        "status": metadata.status().to_string(),
        "exit_code": metadata.exit_status().code().as_str(),
        "updated_ms": system_time_millis(updated_at)?,
        "version": u64_to_i64(version.get())?,
    };
    let timestamps = metadata.timestamps();
    update.insert(
        "started_ms",
        timestamps
            .started_at()
            .map(system_time_millis)
            .transpose()?
            .map_or(Bson::Null, Bson::Int64),
    );
    update.insert(
        "ended_ms",
        timestamps
            .ended_at()
            .map(system_time_millis)
            .transpose()?
            .map_or(Bson::Null, Bson::Int64),
    );
    update.insert(
        "failure_category",
        metadata.failure().map_or(Bson::Null, |failure| {
            Bson::String(failure.category().durable_code().to_owned())
        }),
    );
    update.insert(
        "failure_id",
        metadata
            .failure()
            .map(|failure| u64_to_i64(failure.failure_id().get()).map(Bson::Int64))
            .transpose()?
            .unwrap_or(Bson::Null),
    );
    if include_counts {
        let counts = metadata.counts();
        update.insert("read_count", u64_to_i64(counts.read())?);
        update.insert("processed_count", u64_to_i64(counts.processed())?);
        update.insert("write_count", u64_to_i64(counts.written())?);
        update.insert("filter_count", u64_to_i64(counts.filtered())?);
        update.insert("commit_count", u64_to_i64(counts.committed())?);
        update.insert("rollback_count", u64_to_i64(counts.rolled_back())?);
    }
    Ok(update)
}

fn flow_decision_document(
    id: u64,
    request: &FlowDecisionRequest,
) -> Result<Document, RepositoryError> {
    let mut row = doc! {
        "id": u64_to_i64(id)?,
        "job_execution_id": u64_to_i64(request.job_execution_id().get())?,
        "sequence": u64_to_i64(request.sequence().get())?,
        "source_node_id": request.source_node_id().as_str(),
        "observed_outcome": request.observed_outcome().as_str(),
        "transition_kind": request.kind().durable_code(),
        "plan_fingerprint": binary(request.plan_fingerprint()),
        "input_digest": binary(request.input_digest()),
        "decided_ms": system_time_millis(request.decided_at())?,
    };
    row.insert(
        "source_step_execution_id",
        request
            .source_step_execution_id()
            .map(|value| u64_to_i64(value.get()).map(Bson::Int64))
            .transpose()?
            .unwrap_or(Bson::Null),
    );
    row.insert(
        "reused_decision_id",
        request
            .reused_decision_id()
            .map(|value| u64_to_i64(value.get()).map(Bson::Int64))
            .transpose()?
            .unwrap_or(Bson::Null),
    );
    match request.target() {
        FlowTarget::Node(node) => {
            row.insert("target_node_id", node.as_str());
            row.insert("terminal_kind", Bson::Null);
        }
        FlowTarget::Terminal(TerminalKind::Complete) => {
            row.insert("target_node_id", Bson::Null);
            row.insert("terminal_kind", "COMPLETE");
        }
        FlowTarget::Terminal(TerminalKind::Fail) => {
            row.insert("target_node_id", Bson::Null);
            row.insert("terminal_kind", "FAIL");
        }
        FlowTarget::Terminal(TerminalKind::Stop) => {
            row.insert("target_node_id", Bson::Null);
            row.insert("terminal_kind", "STOP");
        }
        FlowTarget::Terminal(_) => return Err(RepositoryError::FlowStateCorrupt),
    }
    Ok(row)
}

fn decode_job_instance(row: &Document) -> Result<JobInstance, RepositoryError> {
    let id = JobInstanceId::new(read_u64(row, "id")?)?;
    let name = JobName::new(read_string(row, "job_name")?)?;
    let encoded = read_string(row, "identifying_parameters")?;
    let value: Value = serde_json::from_str(&encoded).map_err(|_| RepositoryError::Unavailable)?;
    let parameters = decode_identifying_parameters(&value)?;
    Ok(JobInstance::new(id, JobInstanceKey::new(name, &parameters)))
}

fn decode_job_execution(row: &Document) -> Result<JobExecution, RepositoryError> {
    Ok(JobExecution::from_snapshot(
        JobExecutionId::new(read_u64(row, "id")?)?,
        JobInstanceId::new(read_u64(row, "job_instance_id")?)?,
        decode_execution_metadata(row)?,
        ExecutionVersion::new(read_u64(row, "version")?),
    ))
}

fn decode_step_execution(row: &Document) -> Result<StepExecution, RepositoryError> {
    Ok(StepExecution::from_snapshot(
        StepExecutionId::new(read_u64(row, "id")?)?,
        JobExecutionId::new(read_u64(row, "job_execution_id")?)?,
        StepName::new(read_string(row, "step_name")?)?,
        decode_execution_metadata(row)?,
        ExecutionVersion::new(read_u64(row, "version")?),
    ))
}

fn decode_execution_metadata(row: &Document) -> Result<ExecutionMetadata, RepositoryError> {
    let category = read_optional_string(row, "failure_category")?;
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
        decode_status(&read_string(row, "status")?)?,
        ExitStatus::new(ExitCode::new(read_string(row, "exit_code")?)?),
        ExecutionTimestamps::new(
            millis_system_time(read_i64(row, "created_ms")?)?,
            read_optional_i64(row, "started_ms")?
                .map(millis_system_time)
                .transpose()?,
            read_optional_i64(row, "ended_ms")?
                .map(millis_system_time)
                .transpose()?,
        )?,
        ExecutionCounts::new(
            read_u64_default(row, "read_count")?,
            read_u64_default(row, "processed_count")?,
            read_u64_default(row, "write_count")?,
            read_u64_default(row, "filter_count")?,
            read_u64_default(row, "commit_count")?,
            read_u64_default(row, "rollback_count")?,
        ),
        failure,
    )
    .map_err(Into::into)
}

fn decode_flow_decision(row: &Document) -> Result<FlowDecision, RepositoryError> {
    let fingerprint: [u8; 32] = read_bytes(row, "plan_fingerprint")?
        .try_into()
        .map_err(|_| RepositoryError::FlowStateCorrupt)?;
    let input_digest: [u8; 32] = read_bytes(row, "input_digest")?
        .try_into()
        .map_err(|_| RepositoryError::FlowStateCorrupt)?;
    Ok(FlowDecision::new(
        FlowDecisionId::new(read_u64(row, "id")?)?,
        JobExecutionId::new(read_u64(row, "job_execution_id")?)?,
        FlowDecisionSequence::new(read_u64(row, "sequence")?)
            .map_err(|_| RepositoryError::FlowStateCorrupt)?,
        NodeId::new(read_string(row, "source_node_id")?)
            .map_err(|_| RepositoryError::FlowStateCorrupt)?,
        read_optional_u64(row, "source_step_execution_id")?
            .map(StepExecutionId::new)
            .transpose()?,
        FlowTransitionKind::from_durable_code(&read_string(row, "transition_kind")?)
            .ok_or(RepositoryError::FlowStateCorrupt)?,
        ExitCode::new(read_string(row, "observed_outcome")?)?,
        decode_flow_target(row)?,
        fingerprint,
        input_digest,
        read_optional_u64(row, "reused_decision_id")?
            .map(FlowDecisionId::new)
            .transpose()?,
        millis_system_time(read_i64(row, "decided_ms")?)?,
    ))
}

fn decode_flow_target(row: &Document) -> Result<FlowTarget, RepositoryError> {
    match (
        read_optional_string(row, "target_node_id")?,
        read_optional_string(row, "terminal_kind")?,
    ) {
        (Some(node), None) => Ok(FlowTarget::Node(
            NodeId::new(node).map_err(|_| RepositoryError::FlowStateCorrupt)?,
        )),
        (None, Some(value)) if value == "COMPLETE" => {
            Ok(FlowTarget::Terminal(TerminalKind::Complete))
        }
        (None, Some(value)) if value == "FAIL" => Ok(FlowTarget::Terminal(TerminalKind::Fail)),
        (None, Some(value)) if value == "STOP" => Ok(FlowTarget::Terminal(TerminalKind::Stop)),
        _ => Err(RepositoryError::FlowStateCorrupt),
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
            _ => return Err(RepositoryError::Unavailable),
        };
        object.insert(
            name.as_str().to_owned(),
            json!({ "type": kind_name, "identifying": true, "value": value }),
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
        let raw = envelope.get("value").ok_or(RepositoryError::Unavailable)?;
        let value = match kind {
            "string" => ParameterValue::string(raw.as_str().ok_or(RepositoryError::Unavailable)?)?,
            "i64" => ParameterValue::from(raw.as_i64().ok_or(RepositoryError::Unavailable)?),
            "u64" => ParameterValue::from(raw.as_u64().ok_or(RepositoryError::Unavailable)?),
            "bool" => ParameterValue::from(raw.as_bool().ok_or(RepositoryError::Unavailable)?),
            _ => return Err(RepositoryError::Unavailable),
        };
        parameters.insert(
            ParameterName::new(raw_name.clone())?,
            JobParameter::new(value, ParameterRole::Identifying),
        )?;
    }
    Ok(parameters)
}

fn binary(value: &[u8]) -> Bson {
    Bson::Binary(Binary {
        subtype: BinarySubtype::Generic,
        bytes: value.to_vec(),
    })
}

fn read_i64(row: &Document, name: &str) -> Result<i64, RepositoryError> {
    row.get_i64(name).map_err(|_| RepositoryError::Unavailable)
}

fn read_u64(row: &Document, name: &str) -> Result<u64, RepositoryError> {
    i64_to_u64(read_i64(row, name)?)
}

fn read_numeric_u64(row: &Document, name: &str) -> Result<u64, RepositoryError> {
    match row.get(name) {
        Some(Bson::Int32(value)) => u64::try_from(*value).map_err(|_| RepositoryError::Unavailable),
        Some(Bson::Int64(value)) => i64_to_u64(*value),
        _ => Err(RepositoryError::Unavailable),
    }
}

fn read_u64_default(row: &Document, name: &str) -> Result<u64, RepositoryError> {
    match row.get(name) {
        None => Ok(0),
        Some(Bson::Int64(value)) => i64_to_u64(*value),
        _ => Err(RepositoryError::Unavailable),
    }
}

fn read_u32(row: &Document, name: &str) -> Result<u32, RepositoryError> {
    u32::try_from(read_i64(row, name)?).map_err(|_| RepositoryError::Unavailable)
}

fn read_optional_i64(row: &Document, name: &str) -> Result<Option<i64>, RepositoryError> {
    match row.get(name) {
        None | Some(Bson::Null) => Ok(None),
        Some(Bson::Int64(value)) => Ok(Some(*value)),
        _ => Err(RepositoryError::Unavailable),
    }
}

fn read_optional_u64(row: &Document, name: &str) -> Result<Option<u64>, RepositoryError> {
    read_optional_i64(row, name)?.map(i64_to_u64).transpose()
}

fn read_string(row: &Document, name: &str) -> Result<String, RepositoryError> {
    row.get_str(name)
        .map(str::to_owned)
        .map_err(|_| RepositoryError::Unavailable)
}

fn read_optional_string(row: &Document, name: &str) -> Result<Option<String>, RepositoryError> {
    match row.get(name) {
        None | Some(Bson::Null) => Ok(None),
        Some(Bson::String(value)) => Ok(Some(value.clone())),
        _ => Err(RepositoryError::Unavailable),
    }
}

fn read_bytes(row: &Document, name: &str) -> Result<Vec<u8>, RepositoryError> {
    match row.get(name) {
        Some(Bson::Binary(value)) if value.subtype == BinarySubtype::Generic => {
            Ok(value.bytes.clone())
        }
        _ => Err(RepositoryError::Unavailable),
    }
}

fn u64_to_i64(value: u64) -> Result<i64, RepositoryError> {
    i64::try_from(value).map_err(|_| RepositoryError::Unavailable)
}

fn i64_to_u64(value: i64) -> Result<u64, RepositoryError> {
    u64::try_from(value).map_err(|_| RepositoryError::Unavailable)
}

fn system_time_millis(value: SystemTime) -> Result<i64, RepositoryError> {
    let duration = value
        .duration_since(UNIX_EPOCH)
        .map_err(|_| RepositoryError::Unavailable)?;
    i64::try_from(duration.as_millis()).map_err(|_| RepositoryError::Unavailable)
}

fn millis_system_time(value: i64) -> Result<SystemTime, RepositoryError> {
    Ok(UNIX_EPOCH + Duration::from_millis(i64_to_u64(value)?))
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

fn map_operation_error(error: MongoError) -> RepositoryError {
    let transient = error.contains_label(TRANSIENT_TRANSACTION_ERROR);
    let kind = error.kind;
    if transient {
        return RepositoryError::TransactionAborted;
    }
    if is_duplicate_key(kind.as_ref()) {
        RepositoryError::ConcurrentModification
    } else {
        RepositoryError::Unavailable
    }
}

fn map_commit_error(error: &MongoError) -> RepositoryError {
    if error.contains_label(UNKNOWN_TRANSACTION_COMMIT_RESULT) {
        RepositoryError::CommitOutcomeUnknown
    } else {
        RepositoryError::TransactionAborted
    }
}

fn is_duplicate_key(kind: &ErrorKind) -> bool {
    match kind {
        ErrorKind::Write(WriteFailure::WriteError(write)) => write.code == 11000,
        ErrorKind::Command(command) => command.code == 11000,
        _ => false,
    }
}

fn explorer_unavailable() -> ExplorerError {
    ExplorerError::Repository(RepositoryError::Unavailable)
}

fn mongo_window_identity(window: &QueryWindow) -> Result<Option<i64>, ExplorerError> {
    match window.after() {
        Some(CursorKey::Identity(value)) => i64::try_from(*value)
            .map(Some)
            .map_err(|_| ExplorerError::Cursor(CursorError::CursorInvalid)),
        Some(_) => Err(ExplorerError::Cursor(CursorError::CursorInvalid)),
        None => Ok(None),
    }
}

fn decode_mongo_instance_projection(
    row: &Document,
) -> Result<JobInstanceProjection, ExplorerError> {
    let id = JobInstanceId::new(read_u64(row, "id").map_err(ExplorerError::Repository)?)
        .map_err(|_| explorer_unavailable())?;
    let job_name = JobName::new(read_string(row, "job_name").map_err(ExplorerError::Repository)?)
        .map_err(|_| explorer_unavailable())?;
    let digest: [u8; 32] = read_bytes(row, "instance_key")
        .map_err(ExplorerError::Repository)?
        .try_into()
        .map_err(|_| explorer_unavailable())?;
    let encoded = read_string(row, "identifying_parameters").map_err(ExplorerError::Repository)?;
    let value: Value = serde_json::from_str(&encoded).map_err(|_| explorer_unavailable())?;
    let parameters = decode_identifying_parameters(&value)
        .map_err(ExplorerError::Repository)?
        .iter()
        .map(|(name, parameter)| {
            ParameterDescriptor::new(
                name.clone(),
                parameter.value().kind(),
                parameter.is_identifying(),
            )
        })
        .collect();
    let created_at =
        millis_system_time(read_i64(row, "created_ms").map_err(ExplorerError::Repository)?)
            .map_err(ExplorerError::Repository)?;
    Ok(JobInstanceProjection::new(
        id,
        job_name,
        digest,
        parameters,
        Some(created_at),
        None,
    ))
}

fn decode_mongo_execution_projection(
    row: &Document,
) -> Result<JobExecutionProjection, ExplorerError> {
    let execution = decode_job_execution(row).map_err(ExplorerError::Repository)?;
    let metadata = execution.metadata();
    let instance = joined_document(row, "explorer_instance")?.ok_or_else(explorer_unavailable)?;
    let job_name =
        JobName::new(read_string(instance, "job_name").map_err(ExplorerError::Repository)?)
            .map_err(|_| explorer_unavailable())?;
    let attempt = u32::try_from(read_u64(row, "attempt").map_err(ExplorerError::Repository)?)
        .map_err(|_| explorer_unavailable())?;
    let definition = joined_document(row, "explorer_definition")?
        .map(decode_mongo_definition_descriptor)
        .transpose()?;
    let updated_at =
        millis_system_time(read_i64(row, "updated_ms").map_err(ExplorerError::Repository)?)
            .map_err(ExplorerError::Repository)?;

    Ok(JobExecutionProjection::new(
        execution.id(),
        execution.job_instance_id(),
        job_name,
        attempt,
        metadata.status(),
        metadata.exit_status().clone(),
        metadata.counts(),
        execution.version(),
        metadata.timestamps(),
        updated_at,
        metadata.failure(),
        definition,
        None,
        None,
        false,
    ))
}

fn joined_document<'a>(
    row: &'a Document,
    field: &str,
) -> Result<Option<&'a Document>, ExplorerError> {
    match row.get(field) {
        Some(Bson::Array(values)) => match values.first() {
            Some(Bson::Document(document)) => Ok(Some(document)),
            None => Ok(None),
            Some(_) => Err(explorer_unavailable()),
        },
        None => Ok(None),
        Some(_) => Err(explorer_unavailable()),
    }
}

fn decode_mongo_definition_descriptor(
    row: &Document,
) -> Result<DefinitionDescriptor, ExplorerError> {
    let revision = DefinitionRevision::new(
        read_string(row, "definition_revision").map_err(ExplorerError::Repository)?,
    )
    .map_err(|_| explorer_unavailable())?;
    let manifest_format =
        u16::try_from(read_numeric_u64(row, "manifest_format").map_err(ExplorerError::Repository)?)
            .map_err(|_| explorer_unavailable())?;
    let manifest_digest: [u8; 32] = read_bytes(row, "manifest_digest")
        .map_err(ExplorerError::Repository)?
        .try_into()
        .map_err(|_| explorer_unavailable())?;
    Ok(DefinitionDescriptor::new(
        revision,
        manifest_format,
        manifest_digest,
    ))
}

fn namespace_missing(error: &MongoError) -> bool {
    matches!(error.kind.as_ref(), ErrorKind::Command(command) if command.code == 26)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configuration_debug_redacts_uri_and_database() -> Result<(), MongoConfigError> {
        let config = MongoConfig::new(
            "mongodb://alice:secret@example.invalid/?replicaSet=rs0",
            "sensitive_db",
        )?;
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("alice"));
        assert!(!rendered.contains("secret"));
        assert!(!rendered.contains("sensitive_db"));
        assert!(rendered.contains("<redacted>"));
        Ok(())
    }

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
