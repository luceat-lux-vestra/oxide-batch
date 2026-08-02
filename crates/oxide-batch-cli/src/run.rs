//! Invocation orchestration.
//!
//! One invocation runs in three stages. [`prepare`] parses the closed grammar
//! and resolves configuration without opening a connection, so a configuration
//! error always exits before any repository is contacted. [`local`] answers the
//! commands a repository is not required for. [`dispatch`] calls the portable
//! services and writes the bounded result.
//!
//! Output is written only after a mutating command's durable effect is
//! committed, and a write failure never causes a second mutating call.

use std::future::Future;
use std::pin::pin;
use std::time::Duration;

use futures_util::future::{Either, select};
use serde_json::{Value, json};

use oxide_batch::{
    ActorRef, BatchStatus, BoxFuture, Cursor, DefinitionIdentity, ExecutionVersion,
    ExplorerRepository, FailureCategory, FailureId, FailureSummary, JobExecutionId, JobExplorer,
    JobInstanceId, JobInstanceKey, JobName, JobOperator, JobParameter, JobParameters,
    JobRepository, OperationId, OperatorOutcome, OperatorOutcomeClass, OperatorRequest, Page,
    PageRequest, PageSize, ParameterName, ParameterRole, ParameterValue, PurgeBatchBound,
    PurgePlanRequest, ReasonCode, RecoveryDirective, RecoveryError, RecoveryProposal,
    RecoveryProposer, RecoveryRepository, RepositoryError, RetentionService, StepExecutionId,
    TerminalStatusSet,
};

use crate::args::{Arguments, DirectiveArg, RecordArg};
use crate::catalog::DefinitionCatalog;
use crate::command::Command;
use crate::config::{Configuration, resolve};
use crate::exit::ExitCategory;
use crate::failure;
use crate::host::Host;
use crate::output::{Diagnostic, PageInfo, Response, Writer};
use crate::project;

/// The durable schema state one repository reports.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchemaState {
    /// The schema version installed in the repository, when one is readable.
    pub installed: Option<u32>,
    /// The schema version this build supports.
    pub supported: u32,
}

impl SchemaState {
    /// Returns whether the installed schema requires migration.
    #[must_use]
    pub const fn migration_required(&self) -> bool {
        match self.installed {
            Some(installed) => installed < self.supported,
            None => true,
        }
    }

    /// Returns whether the installed schema is newer than this build supports.
    #[must_use]
    pub const fn newer_than_supported(&self) -> bool {
        match self.installed {
            Some(installed) => installed > self.supported,
            None => false,
        }
    }
}

/// Reports the durable schema version without changing it.
///
/// `schema status` never migrates. A migration is a separate, privileged
/// action of a dedicated migrator identity.
pub trait SchemaReport: Send + Sync {
    /// Reads the installed and supported schema versions.
    fn schema_state(&self) -> BoxFuture<'_, Result<SchemaState, RepositoryError>>;
}

/// Produces the current evidence-bound recovery proposal for the CLI.
pub trait RecoveryProposalPort: Send + Sync {
    /// Gathers one proposal without changing durable state.
    fn propose(
        &self,
        execution_id: JobExecutionId,
    ) -> BoxFuture<'_, Result<RecoveryProposal, RecoveryError>>;
}

impl<R: RecoveryRepository> RecoveryProposalPort for RecoveryProposer<R> {
    fn propose(
        &self,
        execution_id: JobExecutionId,
    ) -> BoxFuture<'_, Result<RecoveryProposal, RecoveryError>> {
        Box::pin(async move { self.propose(execution_id).await })
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct NoRecoveryProposals;

impl RecoveryProposalPort for NoRecoveryProposals {
    fn propose(
        &self,
        _execution_id: JobExecutionId,
    ) -> BoxFuture<'_, Result<RecoveryProposal, RecoveryError>> {
        Box::pin(async {
            Err(RecoveryError::Repository(
                RepositoryError::UnsupportedCapability {
                    capability: oxide_batch::RepositoryCapability::OperatorRequests,
                },
            ))
        })
    }
}

/// A repository that reports no durable schema, such as the in-memory adapter.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoSchema;

impl SchemaReport for NoSchema {
    fn schema_state(&self) -> BoxFuture<'_, Result<SchemaState, RepositoryError>> {
        Box::pin(async {
            Err(RepositoryError::UnsupportedCapability {
                capability: oxide_batch::RepositoryCapability::OperatorRequests,
            })
        })
    }
}

/// The portable services one invocation calls.
///
/// The CLI owns no correctness rule of its own; every guard belongs to these
/// services.
pub struct Services<R, S> {
    operator: JobOperator<R>,
    retention: RetentionService<R>,
    explorer: JobExplorer<S>,
    recovery: Box<dyn RecoveryProposalPort>,
    schema: Box<dyn SchemaReport>,
}

impl<R: JobRepository, S: ExplorerRepository> Services<R, S> {
    /// Binds already-constructed services.
    #[must_use]
    pub fn new(
        operator: JobOperator<R>,
        retention: RetentionService<R>,
        explorer: JobExplorer<S>,
        schema: Box<dyn SchemaReport>,
    ) -> Self {
        Self {
            operator,
            retention,
            explorer,
            recovery: Box::new(NoRecoveryProposals),
            schema,
        }
    }

    /// Attaches the evidence source required by `execution recover`.
    #[must_use]
    pub fn with_recovery_proposals(mut self, recovery: Box<dyn RecoveryProposalPort>) -> Self {
        self.recovery = recovery;
        self
    }
}

/// One parsed and validated invocation awaiting dispatch.
#[derive(Debug)]
pub struct Plan {
    command: Command,
    arguments: Arguments,
    config: Configuration,
    writer: Writer,
    operation_id: Option<String>,
    /// Launch parameters, resolved while the host is still available and
    /// before any repository connection is opened.
    parameters: JobParameters,
}

impl Plan {
    /// Returns the command this invocation selected.
    #[must_use]
    pub const fn command(&self) -> Command {
        self.command
    }

    /// Borrows the effective configuration.
    #[must_use]
    pub const fn config(&self) -> &Configuration {
        &self.config
    }

    /// Returns the client deadline this invocation requested.
    #[must_use]
    pub const fn timeout(&self) -> Duration {
        self.config.client_timeout()
    }
}

/// Parses the grammar and resolves configuration.
///
/// No repository connection is opened, so a usage or configuration error is
/// always reported before any connection attempt.
///
/// # Errors
///
/// Returns the exit category of a rejected invocation. The rejection has
/// already been written as a redacted diagnostic.
pub fn prepare<H: Host>(host: &mut H, argv: &[String]) -> Result<Plan, ExitCategory> {
    let (command, arguments) = match crate::args::parse(argv) {
        Ok(parsed) => parsed,
        Err(error) => {
            let category = error.category();
            report(
                host,
                category,
                &Diagnostic::new(error.code(), error.to_string()),
            );
            return Err(category);
        }
    };
    let config = match resolve(host, &arguments) {
        Ok(config) => config,
        Err(error) => {
            for issue in error.issues() {
                report(
                    host,
                    ExitCategory::ConfigurationInvalid,
                    &Diagnostic::new("INVALID_CONFIGURATION_VALUE", issue.to_string()),
                );
            }
            return Err(ExitCategory::ConfigurationInvalid);
        }
    };
    let color = !arguments.no_color && host.is_stdout_terminal();
    let writer = Writer::new(config.output(), color);
    let parameters = match launch_parameters(host, &arguments) {
        Ok(parameters) => parameters,
        Err(detail) => {
            report(
                host,
                ExitCategory::Usage,
                &Diagnostic::new("PARAMETERS_INVALID", detail),
            );
            return Err(ExitCategory::Usage);
        }
    };
    let mut plan = Plan {
        command,
        arguments,
        config,
        writer,
        operation_id: None,
        parameters,
    };
    // The confirmation and non-interactive safeguards run before any
    // connection is opened, so a refused destructive command contacts no
    // repository at all.
    authorize(host, &mut plan)?;
    Ok(plan)
}

/// Answers the commands that require no repository connection.
///
/// Returns `None` when the command needs a repository.
#[must_use]
pub fn local<H: Host>(host: &mut H, plan: &Plan) -> Option<ExitCategory> {
    if !matches!(plan.command, Command::ConfigShow) {
        return None;
    }
    let rows: Vec<Value> = plan
        .config
        .effective()
        .into_iter()
        .map(|value| {
            json!({
                "key": value.key(),
                "value": value.value(),
                "source": value.source().as_str(),
                "redacted": value.is_redacted(),
            })
        })
        .collect();
    let response = Response::success(Command::ConfigShow, json!(rows));
    Some(emit(host, plan, &response))
}

/// Applies the confirmation and non-interactive safeguards.
///
/// Returns the resolved operation identifier, or the exit category of a
/// refused invocation. Nothing is mutated when this returns an error.
fn authorize<H: Host>(host: &mut H, plan: &mut Plan) -> Result<(), ExitCategory> {
    if !plan.command.is_mutating() {
        return Ok(());
    }
    let interactive = host.is_stdin_interactive();
    let operation_id = if let Some(value) = plan.arguments.operation_id.clone() {
        value
    } else {
        if !interactive {
            // An automated caller must name the identifier it will replay
            // after an ambiguous outcome, so the CLI never invents one it
            // cannot report back on a broken pipe.
            report(
                host,
                ExitCategory::Usage,
                &Diagnostic::new(
                    "OPERATION_ID_REQUIRED",
                    "a mutating command requires --operation-id when standard input is not a terminal",
                ),
            );
            return Err(ExitCategory::Usage);
        }
        let generated = host.new_operation_id();
        host.write_stderr(format!("operation-id: {generated}\n").as_bytes());
        generated
    };
    plan.operation_id = Some(operation_id.clone());

    if !plan.command.class().requires_confirmation() {
        return Ok(());
    }
    if plan.arguments.yes {
        return Ok(());
    }
    if !interactive {
        report(
            host,
            ExitCategory::ConfirmationRequired,
            &Diagnostic::new(
                "CONFIRMATION_REQUIRED",
                "a destructive command requires --yes when standard input is not a terminal",
            ),
        );
        return Err(ExitCategory::ConfirmationRequired);
    }
    let summary = target_summary(plan);
    host.write_stderr(
        format!(
            "{} [{}] {summary}\noperation-id: {operation_id}\nconfirm (yes/no): ",
            plan.command,
            plan.command.class(),
        )
        .as_bytes(),
    );
    let response = host.read_confirmation().unwrap_or(None);
    let confirmed = response.is_some_and(|value| value.trim().eq_ignore_ascii_case("yes"));
    if confirmed {
        Ok(())
    } else {
        report(
            host,
            ExitCategory::ConfirmationRequired,
            &Diagnostic::new("CONFIRMATION_DECLINED", "the confirmation was not given"),
        );
        Err(ExitCategory::ConfirmationRequired)
    }
}

/// Renders the exact target of a destructive command for confirmation.
fn target_summary(plan: &Plan) -> String {
    let mut parts = Vec::new();
    if let Some(job) = &plan.arguments.job {
        parts.push(format!("job={job}"));
    }
    if let Some(instance) = plan.arguments.instance {
        parts.push(format!("instance={instance}"));
    }
    if let Some(execution) = plan.arguments.execution {
        parts.push(format!("execution={execution}"));
    }
    if let Some(version) = plan.arguments.expected_version {
        parts.push(format!("expected-version={version}"));
    }
    if let Some(digest) = &plan.arguments.plan_digest {
        parts.push(format!("plan-digest={digest}"));
    }
    if plan.arguments.dry_run {
        parts.push("dry-run".to_owned());
    }
    parts.join(" ")
}

/// Runs one command against the portable services.
///
/// The supplied `deadline` future completes when the client deadline elapses.
/// Passing a future that never completes disables the deadline; the process
/// entry point supplies a timer.
pub async fn dispatch<H, R, S, D>(
    host: &mut H,
    plan: &mut Plan,
    services: &Services<R, S>,
    catalog: &DefinitionCatalog,
    deadline: D,
) -> ExitCategory
where
    H: Host,
    R: JobRepository,
    S: ExplorerRepository,
    D: Future<Output = ()>,
{
    let work = pin!(run_command(plan, services, catalog));
    let deadline = pin!(deadline);
    let response = match select(work, deadline).await {
        Either::Left((response, _)) => response,
        Either::Right(((), _)) => Response::failed(
            plan.command,
            ExitCategory::DeadlineExceeded,
            json!({ "operation_id": plan.operation_id }),
        )
        .with_diagnostic(Diagnostic::new(
            "DEADLINE_EXCEEDED",
            "the client deadline elapsed; the durable outcome is undetermined",
        )),
    };
    emit(host, plan, &response)
}

/// Writes one response and returns the exit category actually reported.
fn emit<H: Host>(host: &mut H, plan: &Plan, response: &Response) -> ExitCategory {
    match plan.writer.emit(host, response) {
        Ok(()) => response.category(),
        Err(_) => {
            // The durable effect, if any, is already committed. The operation
            // identifier lets the caller re-read or replay, so no mutating call
            // is repeated to recover a display failure.
            ExitCategory::OutputFailure
        }
    }
}

/// Writes a redacted diagnostic for a rejection that produced no response.
///
/// The code is omitted when it repeats the category, so a reader never sees the
/// same word twice.
fn report<H: Host>(host: &mut H, category: ExitCategory, diagnostic: &Diagnostic) {
    let line = if diagnostic.code == category.as_str() {
        format!("{category}: {}\n", diagnostic.detail)
    } else {
        format!("{category}: {}: {}\n", diagnostic.code, diagnostic.detail)
    };
    host.write_stderr(line.as_bytes());
}

/// Builds the bounded page request of a paginated command.
fn page_request(plan: &Plan) -> Result<PageRequest, Response> {
    let size = PageSize::new(plan.config.page_size()).map_err(|error| {
        Response::failed(plan.command, failure::explorer(&error), Value::Null)
            .with_diagnostic(failure::explorer_diagnostic(&error))
    })?;
    match &plan.arguments.cursor {
        None => Ok(PageRequest::first(size)),
        Some(token) => {
            let cursor = Cursor::from_hex(token).map_err(|_| {
                Response::failed(plan.command, ExitCategory::GuardRejected, Value::Null)
                    .with_diagnostic(Diagnostic::new(
                        "CURSOR_REJECTED",
                        "the continuation token is not a valid cursor",
                    ))
            })?;
            Ok(PageRequest::resume(size, cursor))
        }
    }
}

/// Renders one page and its pagination fields.
fn paged<T>(plan: &Plan, page: &Page<T>, rows: Vec<Value>) -> Response {
    let info = PageInfo {
        page_size: plan.config.page_size(),
        returned: rows.len(),
        next_cursor: page.next_cursor().map(Cursor::to_string),
    };
    Response::success(plan.command, Value::Array(rows)).with_page(info)
}

#[allow(clippy::too_many_lines)]
async fn run_command<R, S>(
    plan: &Plan,
    services: &Services<R, S>,
    catalog: &DefinitionCatalog,
) -> Response
where
    R: JobRepository,
    S: ExplorerRepository,
{
    match plan.command {
        Command::ConfigShow => Response::success(plan.command, Value::Null),
        Command::JobList => match page_request(plan) {
            Err(response) => response,
            Ok(request) => match services.explorer.list_job_names(&request).await {
                Ok(page) => {
                    let rows = page
                        .rows()
                        .iter()
                        .map(|name| json!({ "job_name": name.as_str() }))
                        .collect();
                    paged(plan, &page, rows)
                }
                Err(error) => explorer_failure(plan, &error),
            },
        },
        Command::JobShow => job_show(plan, services).await,
        Command::InstanceList => match (job_name(plan), page_request(plan)) {
            (Err(response), _) | (Ok(_), Err(response)) => response,
            (Ok(name), Ok(request)) => match services.explorer.list_instances(&name, &request).await
            {
                Ok(page) => {
                    let rows = page.rows().iter().map(project::instance).collect();
                    paged(plan, &page, rows)
                }
                Err(error) => explorer_failure(plan, &error),
            },
        },
        Command::InstanceShow => instance_show(plan, services).await,
        Command::ExecutionList => execution_list(plan, services).await,
        Command::ExecutionShow => match execution_id(plan) {
            Err(response) => response,
            Ok(id) => match services.explorer.get_execution(id).await {
                Ok(Some(execution)) => {
                    let mut projection = project::execution(&execution);
                    let proposal = services
                        .recovery
                        .propose(id)
                        .await
                        .ok()
                        .map_or(Value::Null, |value| project::recovery_proposal(&value));
                    projection["recovery_proposal"] = proposal;
                    Response::success(plan.command, projection)
                }
                Ok(None) => not_found(plan, "execution"),
                Err(error) => explorer_failure(plan, &error),
            },
        },
        Command::ExecutionSteps => match (execution_id(plan), page_request(plan)) {
            (Err(response), _) | (Ok(_), Err(response)) => response,
            (Ok(id), Ok(request)) => {
                match services.explorer.list_step_executions(id, &request).await {
                    Ok(page) => {
                        let rows = page.rows().iter().map(project::step).collect();
                        paged(plan, &page, rows)
                    }
                    Err(error) => explorer_failure(plan, &error),
                }
            }
        },
        Command::ExecutionPartitions => match (step_id(plan), page_request(plan)) {
            (Err(response), _) | (Ok(_), Err(response)) => response,
            (Ok(id), Ok(request)) => {
                match services.explorer.list_step_partitions(id, &request).await {
                    Ok(page) => {
                        let rows = page.rows().iter().map(project::partition).collect();
                        paged(plan, &page, rows)
                    }
                    Err(error) => explorer_failure(plan, &error),
                }
            }
        },
        Command::ExecutionHistory => execution_history(plan, services).await,
        Command::ExecutionStop
        | Command::ExecutionRestart
        | Command::ExecutionAbandon
        | Command::ExecutionRecover
        | Command::Launch => operator_command(plan, services, catalog).await,
        Command::RetentionPlan => retention_plan(plan, services).await,
        Command::RetentionApply => retention_apply(plan, services).await,
        Command::RetentionHold | Command::RetentionRelease => retention_hold(plan, services).await,
        Command::SchemaStatus => schema_status(plan, services).await,
        Command::DiagnosticsBundle => Response::failed(
            plan.command,
            ExitCategory::GuardRejected,
            Value::Null,
        )
        .with_diagnostic(Diagnostic::new(
            "BUNDLE_UNAVAILABLE",
            "the diagnostic bundle requires the telemetry catalog, which this build does not implement",
        )),
    }
}

fn explorer_failure(plan: &Plan, error: &oxide_batch::ExplorerError) -> Response {
    Response::failed(plan.command, failure::explorer(error), Value::Null)
        .with_diagnostic(failure::explorer_diagnostic(error))
}

fn not_found(plan: &Plan, target: &str) -> Response {
    Response::failed(plan.command, ExitCategory::TargetNotFound, Value::Null).with_diagnostic(
        Diagnostic::new("TARGET_NOT_FOUND", format!("the {target} does not exist")),
    )
}

fn usage(plan: &Plan, code: &str, detail: &str) -> Response {
    Response::failed(plan.command, ExitCategory::Usage, Value::Null)
        .with_diagnostic(Diagnostic::new(code, detail))
}

fn job_name(plan: &Plan) -> Result<JobName, Response> {
    let raw = plan
        .arguments
        .job
        .as_deref()
        .ok_or_else(|| usage(plan, "MISSING_JOB", "the command requires --job"))?;
    JobName::new(raw).map_err(|_| usage(plan, "INVALID_JOB", "the job name is not valid"))
}

fn instance_id(plan: &Plan) -> Result<JobInstanceId, Response> {
    let raw = plan
        .arguments
        .instance
        .ok_or_else(|| usage(plan, "MISSING_INSTANCE", "the command requires --instance"))?;
    JobInstanceId::new(raw).map_err(|_| {
        usage(
            plan,
            "INVALID_INSTANCE",
            "the instance identifier is not valid",
        )
    })
}

fn execution_id(plan: &Plan) -> Result<JobExecutionId, Response> {
    let raw = plan.arguments.execution.ok_or_else(|| {
        usage(
            plan,
            "MISSING_EXECUTION",
            "the command requires --execution",
        )
    })?;
    JobExecutionId::new(raw).map_err(|_| {
        usage(
            plan,
            "INVALID_EXECUTION",
            "the execution identifier is not valid",
        )
    })
}

fn step_id(plan: &Plan) -> Result<StepExecutionId, Response> {
    let raw = plan
        .arguments
        .step
        .ok_or_else(|| usage(plan, "MISSING_STEP", "the command requires --step"))?;
    StepExecutionId::new(raw)
        .map_err(|_| usage(plan, "INVALID_STEP", "the step identifier is not valid"))
}

async fn job_show<R, S>(plan: &Plan, services: &Services<R, S>) -> Response
where
    R: JobRepository,
    S: ExplorerRepository,
{
    let name = match job_name(plan) {
        Ok(name) => name,
        Err(response) => return response,
    };
    let request = match page_request(plan) {
        Ok(request) => request,
        Err(response) => return response,
    };
    // A job's definition identity is observable through the newest instance's
    // newest attempt, because the repository records the identity it guarded.
    match services.explorer.list_instances(&name, &request).await {
        Err(error) => explorer_failure(plan, &error),
        Ok(page) => {
            let Some(instance) = page.rows().first() else {
                return not_found(plan, "job");
            };
            match services
                .explorer
                .list_executions(instance.id(), &request)
                .await
            {
                Err(error) => explorer_failure(plan, &error),
                Ok(executions) => {
                    let definition = executions.rows().first().map_or(Value::Null, |execution| {
                        project::execution(execution)["definition"].clone()
                    });
                    Response::success(
                        plan.command,
                        json!({
                            "job_name": name.as_str(),
                            "newest_instance_id": instance.id().get(),
                            "definition": definition,
                        }),
                    )
                }
            }
        }
    }
}

async fn instance_show<R, S>(plan: &Plan, services: &Services<R, S>) -> Response
where
    R: JobRepository,
    S: ExplorerRepository,
{
    let id = match instance_id(plan) {
        Ok(id) => id,
        Err(response) => return response,
    };
    let request = match page_request(plan) {
        Ok(request) => request,
        Err(response) => return response,
    };
    match services.explorer.list_executions(id, &request).await {
        Err(error) => explorer_failure(plan, &error),
        Ok(page) => match page.rows().first() {
            None => not_found(plan, "instance"),
            Some(execution) => {
                let name = execution.job_name().clone();
                match services.explorer.list_instances(&name, &request).await {
                    Err(error) => explorer_failure(plan, &error),
                    Ok(instances) => instances
                        .rows()
                        .iter()
                        .find(|candidate| candidate.id() == id)
                        .map_or_else(
                            || not_found(plan, "instance"),
                            |instance| Response::success(plan.command, project::instance(instance)),
                        ),
                }
            }
        },
    }
}

async fn execution_list<R, S>(plan: &Plan, services: &Services<R, S>) -> Response
where
    R: JobRepository,
    S: ExplorerRepository,
{
    let request = match page_request(plan) {
        Ok(request) => request,
        Err(response) => return response,
    };
    if let Some(age) = &plan.arguments.unresolved_age {
        let Some(minimum) = crate::config::parse_public_duration(age) else {
            return usage(
                plan,
                "INVALID_AGE",
                "the age bound must be an integer with a unit",
            );
        };
        return match services
            .explorer
            .list_unresolved_executions(minimum, &request)
            .await
        {
            Ok(page) => {
                let rows = page.rows().iter().map(project::execution).collect();
                paged(plan, &page, rows)
            }
            Err(error) => explorer_failure(plan, &error),
        };
    }
    let id = match instance_id(plan) {
        Ok(id) => id,
        Err(response) => return response,
    };
    match services.explorer.list_executions(id, &request).await {
        Ok(page) => {
            let rows = page.rows().iter().map(project::execution).collect();
            paged(plan, &page, rows)
        }
        Err(error) => explorer_failure(plan, &error),
    }
}

async fn execution_history<R, S>(plan: &Plan, services: &Services<R, S>) -> Response
where
    R: JobRepository,
    S: ExplorerRepository,
{
    let id = match execution_id(plan) {
        Ok(id) => id,
        Err(response) => return response,
    };
    let request = match page_request(plan) {
        Ok(request) => request,
        Err(response) => return response,
    };
    match plan.arguments.record.unwrap_or_default() {
        RecordArg::Operator => match services.explorer.list_operator_requests(id, &request).await {
            Ok(page) => {
                let rows = page.rows().iter().map(project::operator_record).collect();
                paged(plan, &page, rows)
            }
            Err(error) => explorer_failure(plan, &error),
        },
        RecordArg::Recovery => match services
            .explorer
            .list_recovery_decisions(id, &request)
            .await
        {
            Ok(page) => {
                let rows = page.rows().iter().map(project::recovery_decision).collect();
                paged(plan, &page, rows)
            }
            Err(error) => explorer_failure(plan, &error),
        },
        RecordArg::Flow => match services.explorer.list_flow_decisions(id, &request).await {
            Ok(page) => {
                let rows = page.rows().iter().map(project::flow_decision).collect();
                paged(plan, &page, rows)
            }
            Err(error) => explorer_failure(plan, &error),
        },
    }
}

fn operation_id(plan: &Plan) -> Result<OperationId, Response> {
    let raw = plan.operation_id.as_deref().ok_or_else(|| {
        usage(
            plan,
            "MISSING_OPERATION_ID",
            "the command requires --operation-id",
        )
    })?;
    OperationId::new(raw).map_err(|error| usage(plan, "INVALID_OPERATION_ID", &error.to_string()))
}

fn actor(plan: &Plan) -> Result<ActorRef, Response> {
    let raw = plan
        .arguments
        .actor
        .as_deref()
        .ok_or_else(|| usage(plan, "MISSING_ACTOR", "the command requires --actor"))?;
    ActorRef::new(raw).map_err(|error| usage(plan, "INVALID_ACTOR", &error.to_string()))
}

fn reason(plan: &Plan) -> Result<ReasonCode, Response> {
    let raw = plan
        .arguments
        .reason
        .as_deref()
        .ok_or_else(|| usage(plan, "MISSING_REASON", "the command requires --reason"))?;
    ReasonCode::new(raw).map_err(|error| usage(plan, "INVALID_REASON", &error.to_string()))
}

fn expected_version(plan: &Plan) -> Result<ExecutionVersion, Response> {
    plan.arguments
        .expected_version
        .map(ExecutionVersion::new)
        .ok_or_else(|| {
            usage(
                plan,
                "MISSING_EXPECTED_VERSION",
                "the command requires --expected-version",
            )
        })
}

fn definition_for(
    plan: &Plan,
    catalog: &DefinitionCatalog,
    name: &JobName,
) -> Result<DefinitionIdentity, Response> {
    catalog.get(name).cloned().ok_or_else(|| {
        Response::failed(plan.command, ExitCategory::GuardRejected, Value::Null).with_diagnostic(
            Diagnostic::new(
                "JOB_NOT_REGISTERED",
                "the job is not registered in this binary's definition catalog",
            ),
        )
    })
}

/// Builds the typed parameter set of a launch.
///
/// A parameter value is launch input rather than output, so it is read here and
/// never rendered back. The file form carries the type and identity role that
/// the `name=value` form cannot express.
fn launch_parameters<H: Host>(
    host: &H,
    arguments: &Arguments,
) -> Result<JobParameters, &'static str> {
    let mut parameters = JobParameters::new();
    if let Some(path) = &arguments.parameters_file {
        let bytes = host
            .read_file(path)
            .map_err(|_| "the parameter file is unreadable")?;
        let document: Value =
            serde_json::from_slice(&bytes).map_err(|_| "the parameter file is not valid JSON")?;
        let Value::Object(entries) = document else {
            return Err("the parameter file must be a JSON object");
        };
        for (name, entry) in entries {
            let parameter = typed_parameter(&entry)?;
            let name = ParameterName::new(&name).map_err(|_| "a parameter name is not valid")?;
            parameters
                .insert(name, parameter)
                .map_err(|_| "a parameter name is duplicated")?;
        }
    }
    for (name, value) in &arguments.parameters {
        let name =
            ParameterName::new(name.as_str()).map_err(|_| "a parameter name is not valid")?;
        let value =
            ParameterValue::string(value.clone()).map_err(|_| "a parameter value is too long")?;
        parameters
            .insert(name, JobParameter::new(value, ParameterRole::Identifying))
            .map_err(|_| "a parameter name is duplicated")?;
    }
    Ok(parameters)
}

/// Reads one typed parameter entry of a parameter file.
fn typed_parameter(entry: &Value) -> Result<JobParameter, &'static str> {
    const INVALID: &str = "a parameter entry is not a valid typed value";
    let object = entry.as_object().ok_or(INVALID)?;
    let kind = object.get("type").and_then(Value::as_str).ok_or(INVALID)?;
    let raw = object.get("value").ok_or(INVALID)?;
    let value = match kind {
        "string" => ParameterValue::string(raw.as_str().ok_or(INVALID)?).map_err(|_| INVALID)?,
        "i64" => ParameterValue::from(raw.as_i64().ok_or(INVALID)?),
        "u64" => ParameterValue::from(raw.as_u64().ok_or(INVALID)?),
        "bool" => ParameterValue::from(raw.as_bool().ok_or(INVALID)?),
        _ => return Err(INVALID),
    };
    let role = match object.get("role").and_then(Value::as_str) {
        None | Some("identifying") => ParameterRole::Identifying,
        Some("non_identifying") => ParameterRole::NonIdentifying,
        Some(_) => return Err(INVALID),
    };
    Ok(JobParameter::new(value, role))
}

async fn operator_command<R, S>(
    plan: &Plan,
    services: &Services<R, S>,
    catalog: &DefinitionCatalog,
) -> Response
where
    R: JobRepository,
    S: ExplorerRepository,
{
    let recovery = if matches!(plan.command, Command::ExecutionRecover) {
        match current_recovery_proposal(plan, services).await {
            Ok(proposal) => Some(proposal),
            Err(response) => return response,
        }
    } else {
        None
    };
    let request = match build_operator_request(plan, catalog, recovery) {
        Ok(request) => request,
        Err(response) => return response,
    };
    if plan.arguments.dry_run {
        return Response::success(
            plan.command,
            json!({
                "dry_run": true,
                "action": request.action().as_str(),
                "operation_id": plan.operation_id,
                "request_digest": request.digest().to_hex(),
                "applied": false,
            }),
        )
        .with_diagnostic(Diagnostic::new(
            "DRY_RUN",
            "the request was validated and no durable change was made",
        ));
    }
    match services.operator.execute(&request).await {
        Ok(outcome) => operator_response(plan, &outcome),
        Err(error) => Response::failed(plan.command, failure::operator(&error), Value::Null)
            .with_diagnostic(failure::operator_diagnostic(&error)),
    }
}

fn operator_response(plan: &Plan, outcome: &OperatorOutcome) -> Response {
    let data = json!({
        "outcome": outcome.class().as_str(),
        "changed": outcome.changed_state(),
        "operation_id": plan.operation_id,
        "record": project::operator_record(outcome.record()),
        "execution": outcome
            .execution()
            .map_or(Value::Null, |execution| json!({
                "execution_id": execution.id().get(),
                "status": execution.metadata().status().as_str(),
                "version": execution.version().get(),
            })),
    });
    match outcome.rejection() {
        None => {
            let response = Response::success(plan.command, data);
            if matches!(outcome.class(), OperatorOutcomeClass::Replayed) {
                response.with_diagnostic(Diagnostic::new(
                    "REPLAYED",
                    "the recorded outcome of this operation identifier was returned",
                ))
            } else {
                response
            }
        }
        Some(rejection) => Response::failed(plan.command, failure::rejection(rejection), data)
            .with_diagnostic(Diagnostic::new(
                "GUARD_REJECTED",
                format!("the action was rejected as {rejection}"),
            )),
    }
}

fn build_operator_request(
    plan: &Plan,
    catalog: &DefinitionCatalog,
    recovery: Option<RecoveryProposal>,
) -> Result<OperatorRequest, Response> {
    let operation = operation_id(plan)?;
    let who = actor(plan)?;
    match plan.command {
        Command::Launch => {
            let name = job_name(plan)?;
            let definition = definition_for(plan, catalog, &name)?;
            let key = JobInstanceKey::new(name, &plan.parameters);
            Ok(OperatorRequest::launch(operation, who, key, definition))
        }
        Command::ExecutionRestart => {
            let id = instance_id(plan)?;
            let name = match &plan.arguments.job {
                Some(_) => job_name(plan)?,
                None => {
                    return Err(usage(
                        plan,
                        "MISSING_JOB",
                        "a restart requires --job to select the registered definition",
                    ));
                }
            };
            let definition = definition_for(plan, catalog, &name)?;
            Ok(OperatorRequest::restart(operation, who, id, definition))
        }
        Command::ExecutionStop => {
            let id = execution_id(plan)?;
            let version = expected_version(plan)?;
            Ok(OperatorRequest::stop(operation, who, id, version))
        }
        Command::ExecutionAbandon => {
            let id = execution_id(plan)?;
            let version = expected_version(plan)?;
            let why = reason(plan)?;
            Ok(OperatorRequest::abandon(operation, who, why, id, version))
        }
        Command::ExecutionRecover => {
            let why = reason(plan)?;
            let directive = recovery_directive(plan)?;
            let proposal = recovery.ok_or_else(|| {
                usage(
                    plan,
                    "RECOVERY_EVIDENCE_UNAVAILABLE",
                    "the command has no current recovery proposal",
                )
            })?;
            Ok(OperatorRequest::recover(
                operation, who, why, directive, &proposal,
            ))
        }
        _ => Err(usage(
            plan,
            "UNSUPPORTED",
            "the command is not an operator action",
        )),
    }
}

async fn current_recovery_proposal<R, S>(
    plan: &Plan,
    services: &Services<R, S>,
) -> Result<RecoveryProposal, Response>
where
    R: JobRepository,
    S: ExplorerRepository,
{
    let id = execution_id(plan)?;
    let expected = expected_version(plan)?;
    let supplied = evidence_digest(plan)?;
    let proposal = services.recovery.propose(id).await.map_err(|error| {
        let (category, code, message) = match &error {
            RecoveryError::Repository(RepositoryError::JobExecutionNotFound { .. }) => (
                ExitCategory::TargetNotFound,
                "EXECUTION_NOT_FOUND",
                "the execution does not exist",
            ),
            RecoveryError::Repository(repository) => (
                failure::repository(repository),
                "RECOVERY_EVIDENCE_UNAVAILABLE",
                "the repository could not produce recovery evidence",
            ),
            RecoveryError::ClockEvidenceUnusable => (
                ExitCategory::GuardRejected,
                "CLOCK_EVIDENCE_UNUSABLE",
                "repository and local clocks cannot provide usable recovery evidence",
            ),
            RecoveryError::OwnedByCurrentProcess => (
                ExitCategory::GuardRejected,
                "EXECUTION_OWNED",
                "the execution is owned by the inspecting process",
            ),
            RecoveryError::NotStale { .. } => (
                ExitCategory::GuardRejected,
                "EXECUTION_NOT_STALE",
                "the execution has not crossed the configured stale threshold",
            ),
            RecoveryError::NotRecoverable { .. } => (
                ExitCategory::GuardRejected,
                "RECOVERY_NOT_ALLOWED",
                "the execution is not a recovery candidate",
            ),
            RecoveryError::InvalidStaleThreshold | RecoveryError::InvalidMaxClockSkew => (
                ExitCategory::ConfigurationInvalid,
                "RECOVERY_CONFIG_INVALID",
                "the recovery evidence configuration is invalid",
            ),
            _ => (
                ExitCategory::Internal,
                "RECOVERY_INTERNAL",
                "recovery evidence failed with an unrecognized category",
            ),
        };
        Response::failed(plan.command, category, Value::Null)
            .with_diagnostic(Diagnostic::new(code, message))
    })?;
    if proposal.observed_version() != expected {
        return Err(Response::failed(
            plan.command,
            ExitCategory::OptimisticConflict,
            json!({"current_version": proposal.observed_version().get()}),
        )
        .with_diagnostic(Diagnostic::new(
            "RECOVERY_EVIDENCE_STALE",
            "the supplied version does not match the recovery evidence",
        )));
    }
    if proposal.digest() != &supplied {
        return Err(
            Response::failed(plan.command, ExitCategory::GuardRejected, Value::Null)
                .with_diagnostic(Diagnostic::new(
                    "RECOVERY_EVIDENCE_STALE",
                    "the supplied evidence digest does not match the current proposal",
                )),
        );
    }
    Ok(proposal)
}

fn recovery_directive(plan: &Plan) -> Result<RecoveryDirective, Response> {
    match plan.arguments.directive {
        Some(DirectiveArg::Abandon) => Ok(RecoveryDirective::Abandon),
        Some(DirectiveArg::MarkFailed) => {
            let category = plan.arguments.failure_category.as_deref().ok_or_else(|| {
                usage(
                    plan,
                    "MISSING_FAILURE_CATEGORY",
                    "a mark-failed directive requires --failure-category",
                )
            })?;
            let category = parse_failure_category(category).ok_or_else(|| {
                usage(
                    plan,
                    "INVALID_FAILURE_CATEGORY",
                    "the failure category is not a framework category",
                )
            })?;
            let id = plan.arguments.failure_id.ok_or_else(|| {
                usage(
                    plan,
                    "MISSING_FAILURE_ID",
                    "a mark-failed directive requires --failure-id",
                )
            })?;
            let id = FailureId::new(id).map_err(|_| {
                usage(
                    plan,
                    "INVALID_FAILURE_ID",
                    "the failure identifier is not valid",
                )
            })?;
            Ok(RecoveryDirective::MarkFailed(FailureSummary::new(
                category, id,
            )))
        }
        None => Err(usage(
            plan,
            "MISSING_DIRECTIVE",
            "the command requires --directive",
        )),
    }
}

/// Resolves one framework failure category by its stable name.
fn parse_failure_category(value: &str) -> Option<FailureCategory> {
    [
        FailureCategory::InvalidDefinition,
        FailureCategory::DuplicateExecution,
        FailureCategory::IllegalTransition,
        FailureCategory::TransientInfrastructure,
        FailureCategory::PermanentInfrastructure,
        FailureCategory::UserComponent,
        FailureCategory::Cancelled,
        FailureCategory::Serialization,
        FailureCategory::Invariant,
        FailureCategory::OptimisticConflict,
        FailureCategory::Timeout,
        FailureCategory::UnsupportedCapability,
        FailureCategory::UnknownCommit,
        FailureCategory::ShutdownIncomplete,
        FailureCategory::StaleRecovered,
    ]
    .into_iter()
    .find(|candidate| candidate.as_str() == value)
}

fn evidence_digest(plan: &Plan) -> Result<[u8; 32], Response> {
    let raw = plan.arguments.evidence_digest.as_deref().ok_or_else(|| {
        usage(
            plan,
            "MISSING_EVIDENCE",
            "the command requires --evidence-digest",
        )
    })?;
    decode_digest(raw).ok_or_else(|| {
        usage(
            plan,
            "INVALID_EVIDENCE",
            "the evidence digest must be 64 hexadecimal characters",
        )
    })
}

/// Decodes a 32-byte digest written as lowercase or uppercase hexadecimal.
fn decode_digest(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64 {
        return None;
    }
    let mut bytes = [0_u8; 32];
    for (index, slot) in bytes.iter_mut().enumerate() {
        let start = index * 2;
        let pair = value.get(start..start + 2)?;
        *slot = u8::from_str_radix(pair, 16).ok()?;
    }
    Some(bytes)
}

async fn retention_plan<R, S>(plan: &Plan, services: &Services<R, S>) -> Response
where
    R: JobRepository,
    S: ExplorerRepository,
{
    let request = match build_purge_request(plan) {
        Ok(request) => request,
        Err(response) => return response,
    };
    match services.retention.plan_purge(&request).await {
        Ok(purge) => Response::success(plan.command, project::purge_plan(&purge)),
        Err(error) => Response::failed(plan.command, failure::retention(&error), Value::Null)
            .with_diagnostic(failure::retention_diagnostic(&error)),
    }
}

async fn retention_apply<R, S>(plan: &Plan, services: &Services<R, S>) -> Response
where
    R: JobRepository,
    S: ExplorerRepository,
{
    let request = match build_purge_request(plan) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let purge = match services.retention.plan_purge(&request).await {
        Ok(purge) => purge,
        Err(error) => {
            return Response::failed(plan.command, failure::retention(&error), Value::Null)
                .with_diagnostic(failure::retention_diagnostic(&error));
        }
    };
    // A destructive purge cannot be issued from arguments alone: the digest of
    // the plan the operator reviewed must still describe the same candidates.
    let expected = match &plan.arguments.plan_digest {
        Some(digest) => digest.clone(),
        None => {
            return usage(
                plan,
                "MISSING_PLAN_DIGEST",
                "the command requires --plan-digest from a prior retention plan",
            );
        }
    };
    if !expected.eq_ignore_ascii_case(&purge.digest_hex()) {
        return Response::failed(
            plan.command,
            ExitCategory::OptimisticConflict,
            json!({ "observed_plan_digest": purge.digest_hex() }),
        )
        .with_diagnostic(Diagnostic::new(
            "PLAN_STALE",
            "the observed plan digest does not match the supplied digest; nothing was deleted",
        ));
    }
    if plan.arguments.dry_run {
        return Response::success(
            plan.command,
            json!({
                "dry_run": true,
                "plan": project::purge_plan(&purge),
                "applied": false,
            }),
        )
        .with_diagnostic(Diagnostic::new(
            "DRY_RUN",
            "the plan was validated and no durable change was made",
        ));
    }
    let operation = match operation_id(plan) {
        Ok(value) => value,
        Err(response) => return response,
    };
    let who = match actor(plan) {
        Ok(value) => value,
        Err(response) => return response,
    };
    let why = match reason(plan) {
        Ok(value) => value,
        Err(response) => return response,
    };
    match services
        .retention
        .apply_purge(operation, who, why, &purge)
        .await
    {
        Ok(report) => Response::success(
            plan.command,
            json!({
                "outcome": report.outcome().as_str(),
                "operation_id": plan.operation_id,
                "counts": project::purge_counts(report.counts()),
                "record": project::retention_record(report.record()),
            }),
        ),
        Err(error) => Response::failed(plan.command, failure::retention(&error), Value::Null)
            .with_diagnostic(failure::retention_diagnostic(&error)),
    }
}

fn build_purge_request(plan: &Plan) -> Result<PurgePlanRequest, Response> {
    let name = job_name(plan)?;
    let age = plan
        .arguments
        .older_than
        .as_deref()
        .and_then(crate::config::parse_public_duration)
        .ok_or_else(|| {
            usage(
                plan,
                "MISSING_AGE",
                "the command requires --older-than with an integer and a unit",
            )
        })?;
    let batch = match &plan.arguments.batch {
        None => PurgeBatchBound::default(),
        Some(raw) => {
            let parsed: u32 = raw
                .parse()
                .map_err(|_| usage(plan, "INVALID_BATCH", "the batch bound is not an integer"))?;
            PurgeBatchBound::new(parsed).map_err(|error| {
                Response::failed(plan.command, failure::retention(&error), Value::Null)
                    .with_diagnostic(failure::retention_diagnostic(&error))
            })?
        }
    };
    let statuses = if plan.arguments.status.is_empty() {
        TerminalStatusSet::all()
    } else {
        let mut parsed = Vec::with_capacity(plan.arguments.status.len());
        for raw in &plan.arguments.status {
            let status = parse_status(raw)
                .ok_or_else(|| usage(plan, "INVALID_STATUS", "the status is not a batch status"))?;
            parsed.push(status);
        }
        TerminalStatusSet::new(parsed).map_err(|error| {
            Response::failed(plan.command, failure::retention(&error), Value::Null)
                .with_diagnostic(failure::retention_diagnostic(&error))
        })?
    };
    PurgePlanRequest::new(name, statuses, age, batch).map_err(|error| {
        Response::failed(plan.command, failure::retention(&error), Value::Null)
            .with_diagnostic(failure::retention_diagnostic(&error))
    })
}

/// Resolves one batch status by its stable name.
fn parse_status(value: &str) -> Option<BatchStatus> {
    [
        BatchStatus::Starting,
        BatchStatus::Started,
        BatchStatus::Stopping,
        BatchStatus::Stopped,
        BatchStatus::Failed,
        BatchStatus::Completed,
        BatchStatus::Abandoned,
        BatchStatus::Unknown,
    ]
    .into_iter()
    .find(|candidate| candidate.as_str() == value)
}

async fn retention_hold<R, S>(plan: &Plan, services: &Services<R, S>) -> Response
where
    R: JobRepository,
    S: ExplorerRepository,
{
    let id = match instance_id(plan) {
        Ok(id) => id,
        Err(response) => return response,
    };
    let operation = match operation_id(plan) {
        Ok(value) => value,
        Err(response) => return response,
    };
    let who = match actor(plan) {
        Ok(value) => value,
        Err(response) => return response,
    };
    let why = match reason(plan) {
        Ok(value) => value,
        Err(response) => return response,
    };
    let result = if matches!(plan.command, Command::RetentionHold) {
        services.retention.place_hold(operation, who, why, id).await
    } else {
        services
            .retention
            .release_hold(operation, who, why, id)
            .await
    };
    match result {
        Ok(report) => Response::success(
            plan.command,
            json!({
                "outcome": report.outcome().as_str(),
                "operation_id": plan.operation_id,
                "hold": report.hold().map_or(Value::Null, project::hold),
                "record": project::retention_record(report.record()),
            }),
        ),
        Err(error) => Response::failed(plan.command, failure::retention(&error), Value::Null)
            .with_diagnostic(failure::retention_diagnostic(&error)),
    }
}

async fn schema_status<R, S>(plan: &Plan, services: &Services<R, S>) -> Response
where
    R: JobRepository,
    S: ExplorerRepository,
{
    match services.schema.schema_state().await {
        Ok(state) => Response::success(
            plan.command,
            json!({
                "installed": state.installed,
                "supported": state.supported,
                "migration_required": state.migration_required(),
                "newer_than_supported": state.newer_than_supported(),
            }),
        ),
        Err(error) => Response::failed(plan.command, failure::repository(&error), Value::Null)
            .with_diagnostic(failure::repository_diagnostic(&error)),
    }
}
