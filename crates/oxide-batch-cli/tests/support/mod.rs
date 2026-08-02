//! Deterministic fixtures for the operator CLI scenarios.
//!
//! The host is fully injected, so broken output, refused confirmation, file
//! permissions, and per-value precedence are ordinary assertions rather than
//! process-level fixtures. Nothing here reads the real environment, the real
//! filesystem, or the real clock.

#![allow(dead_code, clippy::expect_used, clippy::panic)]

use std::collections::BTreeMap;
use std::io;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use oxide_batch::{
    BoxFuture, Clock, ComponentRevision, DefinitionIdentity, DefinitionRevision, ExplorerError,
    ExplorerQuery, ExplorerRepository, FlowDecision, IdGenerationError, IdentifierKind,
    InMemoryExplorer, InMemoryJobRepository, JobExecutionId, JobExecutionProjection, JobExplorer,
    JobInstanceId, JobInstanceProjection, JobName, JobOperator, JobRepository, OperatorRecord,
    QueryWindow, RecoveryDecision, RepositoryError, RepositoryUnitOfWork, RetentionService,
    SequentialIdGenerator, StepExecutionId, StepExecutionProjection, StepName,
    StepPartitionProjection,
};
use oxide_batch_cli::{DefinitionCatalog, ExitCategory, Host, NoSchema, Services};

/// A clock that never advances on its own.
#[derive(Debug)]
pub struct FixedClock {
    at: SystemTime,
}

impl FixedClock {
    #[must_use]
    pub fn new() -> Self {
        Self {
            at: UNIX_EPOCH + Duration::from_hours(500_000),
        }
    }
}

impl Default for FixedClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        self.at
    }
}

/// A deterministic in-memory process boundary.
#[derive(Debug, Default)]
pub struct TestHost {
    env: BTreeMap<String, String>,
    files: BTreeMap<PathBuf, Vec<u8>>,
    modes: BTreeMap<PathBuf, u32>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    stdin_interactive: bool,
    stdout_terminal: bool,
    confirmation: Option<String>,
    stdout_capacity: Option<usize>,
    operation_ids: u64,
    /// Number of times standard output was written.
    pub writes: usize,
}

impl TestHost {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_env(mut self, key: &str, value: &str) -> Self {
        self.env.insert(key.to_owned(), value.to_owned());
        self
    }

    #[must_use]
    pub fn with_file(mut self, path: &str, contents: &str) -> Self {
        self.files
            .insert(PathBuf::from(path), contents.as_bytes().to_vec());
        self.modes.insert(PathBuf::from(path), 0o600);
        self
    }

    #[must_use]
    pub fn with_mode(mut self, path: &str, mode: u32) -> Self {
        self.modes.insert(PathBuf::from(path), mode);
        self
    }

    /// Marks standard input interactive and queues one confirmation response.
    #[must_use]
    pub fn interactive(mut self, response: &str) -> Self {
        self.stdin_interactive = true;
        self.confirmation = Some(response.to_owned());
        self
    }

    /// Marks standard input interactive with no response available.
    #[must_use]
    pub fn interactive_silent(mut self) -> Self {
        self.stdin_interactive = true;
        self
    }

    /// Fails every standard-output write beyond `bytes`.
    #[must_use]
    pub fn with_stdout_capacity(mut self, bytes: usize) -> Self {
        self.stdout_capacity = Some(bytes);
        self
    }

    #[must_use]
    pub fn stdout_text(&self) -> String {
        String::from_utf8_lossy(&self.stdout).into_owned()
    }

    #[must_use]
    pub fn stderr_text(&self) -> String {
        String::from_utf8_lossy(&self.stderr).into_owned()
    }

    /// Parses standard output as the versioned JSON envelope.
    #[must_use]
    pub fn envelope(&self) -> serde_json::Value {
        serde_json::from_str(&self.stdout_text()).expect("standard output is one JSON object")
    }
}

impl Host for TestHost {
    fn env(&self, key: &str) -> Option<String> {
        self.env.get(key).cloned().filter(|value| !value.is_empty())
    }

    fn read_file(&self, path: &Path) -> io::Result<Vec<u8>> {
        self.files
            .get(path)
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no such test file"))
    }

    fn file_mode(&self, path: &Path) -> io::Result<Option<u32>> {
        if !self.files.contains_key(path) {
            return Err(io::Error::new(io::ErrorKind::NotFound, "no such test file"));
        }
        Ok(self.modes.get(path).copied())
    }

    fn write_stdout(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.writes += 1;
        if let Some(capacity) = self.stdout_capacity
            && self.stdout.len() + bytes.len() > capacity
        {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed pipe"));
        }
        self.stdout.extend_from_slice(bytes);
        Ok(())
    }

    fn flush_stdout(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn write_stderr(&mut self, bytes: &[u8]) {
        self.stderr.extend_from_slice(bytes);
    }

    fn is_stdin_interactive(&self) -> bool {
        self.stdin_interactive
    }

    fn is_stdout_terminal(&self) -> bool {
        self.stdout_terminal
    }

    fn read_confirmation(&mut self) -> io::Result<Option<String>> {
        Ok(self.confirmation.take())
    }

    fn new_operation_id(&mut self) -> String {
        self.operation_ids += 1;
        format!("generated-{}", self.operation_ids)
    }
}

/// The in-memory services one scenario runs against.
pub type TestServices = Services<InMemoryJobRepository, InMemoryExplorer>;

/// Builds in-memory services over a deterministic clock and identifier source.
#[must_use]
pub fn services() -> (TestServices, InMemoryJobRepository) {
    let clock: Arc<dyn Clock> = Arc::new(FixedClock::new());
    let first = NonZeroU64::new(1).expect("one is nonzero");
    let repository = InMemoryJobRepository::new(
        Arc::clone(&clock),
        Arc::new(SequentialIdGenerator::new(first)),
    );
    let explorer = JobExplorer::new(InMemoryExplorer::new(&repository));
    let operator = JobOperator::new(repository.clone(), Arc::clone(&clock));
    let retention = RetentionService::new(repository.clone(), clock);
    (
        Services::new(operator, retention, explorer, Box::new(NoSchema)),
        repository,
    )
}

/// The identity of one registered test job.
#[must_use]
pub fn test_identity(job: &str) -> DefinitionIdentity {
    let job_name = JobName::new(job).expect("the job name is valid");
    let step = StepName::new("only").expect("the step name is valid");
    let revision = DefinitionRevision::new("r1").expect("the revision is valid");
    let component = ComponentRevision::new("c1").expect("the component revision is valid");
    DefinitionIdentity::tasklet(&job_name, &step, revision, &component)
        .expect("the manifest encodes")
}

/// A catalog registering one test job.
#[must_use]
pub fn test_catalog(job: &str) -> DefinitionCatalog {
    DefinitionCatalog::new()
        .with(test_identity(job))
        .expect("the registration succeeds")
}

/// The durable identifiers a seeded fixture created.
#[derive(Clone, Copy, Debug)]
pub struct Seeded {
    /// The launched logical instance.
    pub instance_id: u64,
    /// The launched execution attempt.
    pub execution_id: u64,
    /// The optimistic version observed right after the launch.
    pub version: u64,
}

/// Builds services that already hold one launched execution.
///
/// The launch goes through the operator service rather than the repository, so
/// the fixture exercises the same guards a CLI invocation would.
#[must_use]
pub fn seeded_services(job: &str) -> (TestServices, Seeded) {
    let (services, _repository) = services();
    let mut host = TestHost::new();
    let catalog = test_catalog(job);
    let category = run_with_catalog(
        &mut host,
        &services,
        &catalog,
        &format!("launch --job {job} --actor fixture --operation-id seed-launch --output json"),
    );
    assert_eq!(
        category,
        ExitCategory::Success,
        "the fixture launch failed: {}",
        host.stdout_text()
    );
    let envelope = host.envelope();
    let execution = &envelope["data"]["execution"];
    let record = &envelope["data"]["record"];
    Seeded {
        instance_id: record["instance_id"]
            .as_u64()
            .expect("the launch recorded an instance"),
        execution_id: execution["execution_id"]
            .as_u64()
            .expect("the launch created an execution"),
        version: execution["version"]
            .as_u64()
            .expect("the launch recorded a version"),
    }
    .pair(services)
}

impl Seeded {
    fn pair(self, services: TestServices) -> (TestServices, Self) {
        (services, self)
    }
}

/// An explorer port that answers every query the same way.
///
/// `Stalled` never completes, which lets a scenario observe the client
/// deadline; the error modes let a scenario observe a category the in-memory
/// adapter cannot produce on its own.
#[derive(Clone, Copy, Debug)]
pub enum FaultyExplorer {
    /// Every query returns [`ExplorerError::Repository`] with `Unavailable`.
    Unavailable,
    /// Every query is pending forever.
    Stalled,
}

impl FaultyExplorer {
    fn answer<'a, T: 'a>(self) -> BoxFuture<'a, Result<T, ExplorerError>> {
        match self {
            Self::Unavailable => {
                Box::pin(async { Err(ExplorerError::Repository(RepositoryError::Unavailable)) })
            }
            Self::Stalled => Box::pin(std::future::pending()),
        }
    }
}

impl ExplorerRepository for FaultyExplorer {
    fn identity_ceiling<'a>(
        &'a self,
        _query: &'a ExplorerQuery,
    ) -> BoxFuture<'a, Result<u64, ExplorerError>> {
        self.answer()
    }

    fn job_names<'a>(
        &'a self,
        _window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<JobName>, ExplorerError>> {
        self.answer()
    }

    fn instances<'a>(
        &'a self,
        _job_name: &'a JobName,
        _window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<JobInstanceProjection>, ExplorerError>> {
        self.answer()
    }

    fn executions<'a>(
        &'a self,
        _job_instance_id: JobInstanceId,
        _window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<JobExecutionProjection>, ExplorerError>> {
        self.answer()
    }

    fn execution(
        &self,
        _job_execution_id: JobExecutionId,
    ) -> BoxFuture<'_, Result<Option<JobExecutionProjection>, ExplorerError>> {
        self.answer()
    }

    fn step_executions<'a>(
        &'a self,
        _job_execution_id: JobExecutionId,
        _window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<StepExecutionProjection>, ExplorerError>> {
        self.answer()
    }

    fn unresolved_executions<'a>(
        &'a self,
        _minimum_age: Duration,
        _window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<JobExecutionProjection>, ExplorerError>> {
        self.answer()
    }

    fn recovery_decisions<'a>(
        &'a self,
        _job_execution_id: JobExecutionId,
        _window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<RecoveryDecision>, ExplorerError>> {
        self.answer()
    }

    fn flow_decisions<'a>(
        &'a self,
        _job_execution_id: JobExecutionId,
        _window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<FlowDecision>, ExplorerError>> {
        self.answer()
    }

    fn step_partitions<'a>(
        &'a self,
        _step_execution_id: StepExecutionId,
        _window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<StepPartitionProjection>, ExplorerError>> {
        self.answer()
    }

    fn operator_requests<'a>(
        &'a self,
        _job_execution_id: JobExecutionId,
        _window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<OperatorRecord>, ExplorerError>> {
        self.answer()
    }
}

/// A repository whose unit of work can never be opened.
///
/// A failure to begin is the earliest possible failure, so it reaches the
/// operator and retention services before any effect is attempted.
#[derive(Clone, Copy, Debug)]
pub struct FaultyRepository(pub FaultyBegin);

/// How a [`FaultyRepository`] refuses to begin a unit of work.
#[derive(Clone, Copy, Debug)]
pub enum FaultyBegin {
    /// The repository is unavailable.
    Unavailable,
    /// The commit outcome is undetermined.
    OutcomeUnknown,
    /// An injected identifier source failed.
    Identifier,
}

impl JobRepository for FaultyRepository {
    fn begin<'a>(
        &'a self,
    ) -> BoxFuture<'a, Result<Box<dyn RepositoryUnitOfWork + 'a>, RepositoryError>> {
        let error = match self.0 {
            FaultyBegin::Unavailable => RepositoryError::Unavailable,
            FaultyBegin::OutcomeUnknown => RepositoryError::CommitOutcomeUnknown,
            FaultyBegin::Identifier => RepositoryError::Identifier(IdGenerationError::Exhausted {
                kind: IdentifierKind::JobExecution,
            }),
        };
        Box::pin(async move { Err(error) })
    }
}

/// Builds services whose explorer always fails the same way.
#[must_use]
pub fn faulty_explorer_services(
    mode: FaultyExplorer,
) -> Services<InMemoryJobRepository, FaultyExplorer> {
    let clock: Arc<dyn Clock> = Arc::new(FixedClock::new());
    let first = NonZeroU64::new(1).expect("one is nonzero");
    let repository = InMemoryJobRepository::new(
        Arc::clone(&clock),
        Arc::new(SequentialIdGenerator::new(first)),
    );
    Services::new(
        JobOperator::new(repository.clone(), Arc::clone(&clock)),
        RetentionService::new(repository, Arc::clone(&clock)),
        JobExplorer::new(mode),
        Box::new(NoSchema),
    )
}

/// Builds services whose repository always refuses to begin.
#[must_use]
pub fn faulty_repository_services(
    mode: FaultyBegin,
) -> Services<FaultyRepository, InMemoryExplorer> {
    let clock: Arc<dyn Clock> = Arc::new(FixedClock::new());
    let first = NonZeroU64::new(1).expect("one is nonzero");
    let backing = InMemoryJobRepository::new(
        Arc::clone(&clock),
        Arc::new(SequentialIdGenerator::new(first)),
    );
    let explorer = JobExplorer::new(InMemoryExplorer::new(&backing));
    let repository = FaultyRepository(mode);
    Services::new(
        JobOperator::new(repository, Arc::clone(&clock)),
        RetentionService::new(repository, clock),
        explorer,
        Box::new(NoSchema),
    )
}

/// Runs one invocation against arbitrary services with no client deadline.
pub fn run_against<R, S>(host: &mut TestHost, services: &Services<R, S>, line: &str) -> ExitCategory
where
    R: JobRepository,
    S: ExplorerRepository,
{
    let arguments = words(line);
    let mut plan = match oxide_batch_cli::prepare(host, &arguments) {
        Ok(plan) => plan,
        Err(category) => return category,
    };
    if let Some(category) = oxide_batch_cli::local(host, &plan) {
        return category;
    }
    let catalog = DefinitionCatalog::new();
    futures_executor::block_on(oxide_batch_cli::dispatch(
        host,
        &mut plan,
        services,
        &catalog,
        std::future::pending::<()>(),
    ))
}

/// Runs one invocation whose client deadline has already elapsed.
pub fn run_expired<R, S>(host: &mut TestHost, services: &Services<R, S>, line: &str) -> ExitCategory
where
    R: JobRepository,
    S: ExplorerRepository,
{
    let arguments = words(line);
    let mut plan = match oxide_batch_cli::prepare(host, &arguments) {
        Ok(plan) => plan,
        Err(category) => return category,
    };
    let catalog = DefinitionCatalog::new();
    futures_executor::block_on(oxide_batch_cli::dispatch(
        host,
        &mut plan,
        services,
        &catalog,
        std::future::ready(()),
    ))
}

/// Splits a command line into process arguments.
#[must_use]
pub fn words(line: &str) -> Vec<String> {
    line.split_whitespace().map(str::to_owned).collect()
}

/// Runs one invocation to completion with no client deadline.
///
/// The deadline future never completes, so a scenario observes only the
/// command's own behavior.
pub fn run(host: &mut TestHost, services: &TestServices, line: &str) -> ExitCategory {
    run_with_catalog(host, services, &DefinitionCatalog::new(), line)
}

/// Runs one invocation against an explicit definition catalog.
pub fn run_with_catalog(
    host: &mut TestHost,
    services: &TestServices,
    catalog: &DefinitionCatalog,
    line: &str,
) -> ExitCategory {
    let arguments = words(line);
    let mut plan = match oxide_batch_cli::prepare(host, &arguments) {
        Ok(plan) => plan,
        Err(category) => return category,
    };
    if let Some(category) = oxide_batch_cli::local(host, &plan) {
        return category;
    }
    futures_executor::block_on(oxide_batch_cli::dispatch(
        host,
        &mut plan,
        services,
        catalog,
        std::future::pending::<()>(),
    ))
}
