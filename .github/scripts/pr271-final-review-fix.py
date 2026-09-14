from pathlib import Path

flow = Path("crates/oxide-batch/src/flow.rs")
text = flow.read_text()
old = """        let mut durable = started;
        if let Some(state) = candidate_state.as_ref() {
            durable = self
                .commit_custom_leaf_state(
                    correlation.job_instance_id(),
                    compiled.id(),
                    &durable,
                    state,
                )
                .await?;
        }
"""
new = """        let mut durable = started;
        if !matches!(outcome, TaskletExecutionOutcome::Unknown) {
            if let Some(state) = candidate_state.as_ref() {
                durable = self
                    .commit_custom_leaf_state(
                        correlation.job_instance_id(),
                        compiled.id(),
                        &durable,
                        state,
                    )
                    .await?;
            }
        }
"""
if old not in text and new not in text:
    raise SystemExit("custom-leaf state commit anchor not found")
if old in text:
    text = text.replace(old, new, 1)
flow.write_text(text)

runtime = Path("crates/oxide-batch/tests/custom_leaf_runtime.rs")
text = runtime.read_text()
old_import = """    SequentialIdGenerator, SkipLimit, StateLimits, StateSchemaId, StateSchemaVersion,
    StepExecutionListener, StepName, StopSource, StopToken, TaskletError, TaskletExecutionOutcome,
"""
new_import = """    SequentialIdGenerator, SkipLimit, StartControls, StartLimit, StateLimits, StateSchemaId,
    StateSchemaVersion, StepExecutionListener, StepName, StopSource, StopToken, TaskletError,
    TaskletExecutionOutcome,
"""
if old_import in text:
    text = text.replace(old_import, new_import, 1)
elif new_import not in text:
    raise SystemExit("custom-leaf runtime import anchor not found")

marker = "fn m7_custom_leaf_cannot_own_second_executor_repository_lifecycle_or_detached_work"
if marker not in text:
    text += r'''

struct UnknownWithCandidateState;

impl CustomLeafHandler for UnknownWithCandidateState {
    fn execute<'a>(
        &'a self,
        _context: CustomLeafContext<'a>,
    ) -> BoxFuture<'a, Result<CustomLeafResult, TaskletError>> {
        Box::pin(async {
            let state = crate_context(77).map_err(TaskletError::from_error)?;
            Ok(CustomLeafResult::new(TaskletOutcome::CommitOutcomeUnknown).with_state(state))
        })
    }
}

#[tokio::test]
async fn unknown_custom_leaf_does_not_commit_candidate_state() -> Result<(), Box<dyn Error>> {
    let (name, id, plan) = node("custom-leaf-unknown-state", None, None)?;
    let registration = CustomLeafRegistration::new(
        CustomLeafKind::new("example.handler")?,
        ComponentRevision::new("handler-v1")?,
        StateSchemaId::new("example.state")?,
        StateSchemaVersion::new(1)?,
        Arc::new(UnknownWithCandidateState),
    );
    let job = FlowJob::new(name, plan)?.with_custom_leaf_registration(id.clone(), registration)?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();

    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;
    assert_eq!(report.outcome(), &FlowExecutionOutcome::Unknown);

    let mut unit = repository.begin().await?;
    let durable = unit
        .latest_flow_step(report.instance().id(), &id)
        .await?
        .ok_or("unknown custom leaf left no durable step")?;
    assert_eq!(durable.execution().metadata().status(), BatchStatus::Unknown);
    assert!(
        durable.context().is_none(),
        "UNKNOWN must retain the last certain state rather than commit a candidate"
    );
    unit.rollback().await?;
    Ok(())
}

fn oversized_context() -> Result<ExecutionContext, std::io::Error> {
    let payload = "x".repeat(70 * 1024);
    let document = format!(
        r#"{{"format":"oxide-batch.execution-context","format_version":1,"schema":"example.state","schema_version":1,"payload":{{"blob":"{payload}"}}}}"#
    );
    let limits = StateLimits::new(128 * 1024, 16)
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    ExecutionContext::from_json(document.as_bytes(), limits)
        .map_err(|error| std::io::Error::other(error.to_string()))
}

struct OversizedStateHandler;

impl CustomLeafHandler for OversizedStateHandler {
    fn execute<'a>(
        &'a self,
        _context: CustomLeafContext<'a>,
    ) -> BoxFuture<'a, Result<CustomLeafResult, TaskletError>> {
        Box::pin(async {
            let state = oversized_context().map_err(TaskletError::from_error)?;
            Ok(CustomLeafResult::new(TaskletOutcome::Completed).with_state(state))
        })
    }
}

#[tokio::test]
async fn custom_leaf_rejects_state_above_the_default_durable_bound() -> Result<(), Box<dyn Error>> {
    let (name, id, plan) = node("custom-leaf-state-bound", None, None)?;
    let registration = CustomLeafRegistration::new(
        CustomLeafKind::new("example.handler")?,
        ComponentRevision::new("handler-v1")?,
        StateSchemaId::new("example.state")?,
        StateSchemaVersion::new(1)?,
        Arc::new(OversizedStateHandler),
    );
    let job = FlowJob::new(name, plan)?.with_custom_leaf_registration(id.clone(), registration)?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();

    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;
    assert!(matches!(
        report.outcome(),
        FlowExecutionOutcome::Failed(FlowFailure::CustomLeafState { .. })
    ));
    let mut unit = repository.begin().await?;
    let durable = unit
        .latest_flow_step(report.instance().id(), &id)
        .await?
        .ok_or("bounded custom leaf left no durable step")?;
    assert!(durable.context().is_none());
    unit.rollback().await?;
    Ok(())
}

struct AlwaysFailCounter(Arc<AtomicUsize>);

impl CustomLeafHandler for AlwaysFailCounter {
    fn execute<'a>(
        &'a self,
        _context: CustomLeafContext<'a>,
    ) -> BoxFuture<'a, Result<CustomLeafResult, TaskletError>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(TaskletError::new())
        })
    }
}

#[tokio::test]
async fn custom_leaf_composes_with_framework_start_limit() -> Result<(), Box<dyn Error>> {
    let name = JobName::new("custom-leaf-start-limit")?;
    let id = oxide_batch::NodeId::new("custom")?;
    let custom = CustomLeafNode::new(
        id.clone(),
        StepName::new("custom-step")?,
        CustomLeafKind::new("example.handler")?,
        ComponentRevision::new("handler-v1")?,
        StateSchemaId::new("example.state")?,
        StateSchemaVersion::new(1)?,
    )
    .with_start_controls(StartControls::new(StartLimit::new(1)?, false));
    let plan = FlowGraph::new(id.clone())
        .with_node(FlowNode::custom_leaf(custom))
        .with_sequence(id.clone(), FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(&name, DefinitionRevision::new("v1")?)?;
    let calls = Arc::new(AtomicUsize::new(0));
    let registration = CustomLeafRegistration::new(
        CustomLeafKind::new("example.handler")?,
        ComponentRevision::new("handler-v1")?,
        StateSchemaId::new("example.state")?,
        StateSchemaVersion::new(1)?,
        Arc::new(AlwaysFailCounter(calls.clone())),
    );
    let job = FlowJob::new(name, plan)?.with_custom_leaf_registration(id, registration)?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();

    let first = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;
    assert!(matches!(
        first.outcome(),
        FlowExecutionOutcome::Failed(FlowFailure::Tasklet(TaskletFailure::Error))
    ));
    let second = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;
    assert!(matches!(
        second.outcome(),
        FlowExecutionOutcome::Failed(FlowFailure::StartLimitExceeded { .. })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    Ok(())
}

const REDACTION_SECRET: &str = "sensitive-custom-leaf-state-value";

fn secret_context() -> Result<ExecutionContext, std::io::Error> {
    let document = format!(
        r#"{{"format":"oxide-batch.execution-context","format_version":1,"schema":"example.state","schema_version":1,"payload":{{"secret":"{REDACTION_SECRET}"}}}}"#
    );
    ExecutionContext::from_json(document.as_bytes(), StateLimits::default())
        .map_err(|error| std::io::Error::other(error.to_string()))
}

struct RedactionHandler {
    diagnostics: Arc<Mutex<Vec<String>>>,
}

impl CustomLeafHandler for RedactionHandler {
    fn execute<'a>(
        &'a self,
        context: CustomLeafContext<'a>,
    ) -> BoxFuture<'a, Result<CustomLeafResult, TaskletError>> {
        Box::pin(async move {
            self.diagnostics
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(format!("{context:?}"));
            if context.previous_state().is_none() {
                let state = secret_context().map_err(TaskletError::from_error)?;
                Ok(CustomLeafResult::new(TaskletOutcome::Completed).with_state(state))
            } else {
                Ok(CustomLeafResult::new(TaskletOutcome::Completed))
            }
        })
    }
}

#[tokio::test]
async fn custom_leaf_diagnostics_redact_previous_state_payload() -> Result<(), Box<dyn Error>> {
    let listener_revision = ComponentRevision::new("listener-redaction-v1")?;
    let (name, id, plan) = node(
        "custom-leaf-redaction",
        Some(listener_revision.clone()),
        None,
    )?;
    let diagnostics = Arc::new(Mutex::new(Vec::new()));
    let registration = CustomLeafRegistration::new(
        CustomLeafKind::new("example.handler")?,
        ComponentRevision::new("handler-v1")?,
        StateSchemaId::new("example.state")?,
        StateSchemaVersion::new(1)?,
        Arc::new(RedactionHandler {
            diagnostics: diagnostics.clone(),
        }),
    )
    .with_listener(
        listener_revision,
        Arc::new(FailFirstAfter {
            calls: AtomicUsize::new(0),
        }),
    );
    let job = FlowJob::new(name, plan)?.with_custom_leaf_registration(id, registration)?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();

    let first = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;
    assert!(matches!(
        first.outcome(),
        FlowExecutionOutcome::Failed(FlowFailure::Listener(_))
    ));
    let second = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;
    assert_eq!(second.outcome(), &FlowExecutionOutcome::Completed);

    let rendered = diagnostics
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .join("\n");
    assert!(!rendered.contains(REDACTION_SECRET));
    assert!(rendered.contains("<redacted>"));
    let result = CustomLeafResult::new(TaskletOutcome::Completed)
        .with_state(secret_context().map_err(|error| -> Box<dyn Error> { Box::new(error) })?);
    assert!(!format!("{result:?}").contains(REDACTION_SECRET));
    Ok(())
}

struct DropSignal(Arc<AtomicUsize>);

impl Drop for DropSignal {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct PendingHandler {
    entered: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
}

impl CustomLeafHandler for PendingHandler {
    fn execute<'a>(
        &'a self,
        _context: CustomLeafContext<'a>,
    ) -> BoxFuture<'a, Result<CustomLeafResult, TaskletError>> {
        let entered = self.entered.clone();
        let dropped = self.dropped.clone();
        Box::pin(async move {
            let _guard = DropSignal(dropped);
            entered.store(1, Ordering::SeqCst);
            std::future::pending::<()>().await;
            Ok(CustomLeafResult::new(TaskletOutcome::Completed))
        })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn m7_custom_leaf_cannot_own_second_executor_repository_lifecycle_or_detached_work()
-> Result<(), Box<dyn Error>> {
    let (name, id, plan) = node("custom-leaf-owned-runtime", None, None)?;
    let entered = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let registration = CustomLeafRegistration::new(
        CustomLeafKind::new("example.handler")?,
        ComponentRevision::new("handler-v1")?,
        StateSchemaId::new("example.state")?,
        StateSchemaVersion::new(1)?,
        Arc::new(PendingHandler {
            entered: entered.clone(),
            dropped: dropped.clone(),
        }),
    );
    let job = FlowJob::new(name, plan)?.with_custom_leaf_registration(id, registration)?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();
    let mut launch = Box::pin(
        FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
            .launch(&job, &JobParameters::new(), &stop),
    );

    loop {
        tokio::select! {
            result = launch.as_mut() => {
                let _ = result;
                return Err("pending custom leaf completed before cancellation of its owning launch".into());
            }
            () = tokio::task::yield_now() => {
                if entered.load(Ordering::SeqCst) == 1 {
                    break;
                }
            }
        }
    }
    drop(launch);
    assert_eq!(
        dropped.load(Ordering::SeqCst),
        1,
        "dropping the owning launch must drop the in-line handler future; framework work may not detach"
    );
    Ok(())
}
'''
runtime.write_text(text)

crash = Path("crates/oxide-batch/tests/postgres_custom_leaf_crash_recovery.rs")
text = crash.read_text()
old_import = """    FailureCategory, FailureId, FlowEvent, FlowEventKind, FlowEventSink,
    FlowExecutionOutcome, FlowGraph, FlowJob, FlowLauncher, FlowNode, FlowTarget, JobInstanceKey,
    JobName, JobParameters, JobRepository, ListenerContext, ListenerError, PostgresConfig,
    PostgresJobRepository, PostgresMigrator, RecoveryRequest, SequentialIdGenerator, StateLimits,
"""
new_import = """    FailureCategory, FailureId, FlowEvent, FlowEventKind, FlowEventSink,
    FlowExecutionOutcome, FlowFailure, FlowGraph, FlowJob, FlowLauncher, FlowNode, FlowRuntimeError,
    FlowTarget, JobInstanceKey, JobName, JobParameters, JobRepository, ListenerContext,
    ListenerError, PostgresConfig, PostgresJobRepository, PostgresMigrator, RecoveryRequest,
    RepositoryError, SequentialIdGenerator, StateLimits,
"""
if old_import in text:
    text = text.replace(old_import, new_import, 1)
elif new_import not in text:
    raise SystemExit("postgres custom-leaf import anchor not found")

listener_marker = "struct FailAfterState;"
if listener_marker not in text:
    anchor = """struct NoopListener;
"""
    addition = r'''struct FailAfterState;

impl StepExecutionListener for FailAfterState {
    fn before_step<'a>(
        &'a self,
        _context: ListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        Box::pin(async { Ok(()) })
    }

    fn after_step<'a>(
        &'a self,
        _context: ListenerContext<'a>,
        _outcome: TaskletExecutionOutcome,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        Box::pin(async { Err(ListenerError::new()) })
    }
}

struct NoopListener;
'''
    if anchor not in text:
        raise SystemExit("NoopListener anchor not found")
    text = text.replace(anchor, addition, 1)

pg_marker = "fn newer_persisted_custom_leaf_state_fails_closed_before_handler"
if pg_marker not in text:
    text += r'''

#[test]
fn newer_persisted_custom_leaf_state_fails_closed_before_handler() -> Result<(), Box<dyn Error>> {
    let Some(runtime_url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let Some(migrator_url) = migrator_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL is not set");
        return Ok(());
    };
    let point = CrashPoint::AfterStateCommitBeforeTransition;
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            PostgresMigrator::migrate(&config(migrator_url.clone())?).await?;
            remove_job(&migrator_url, point.job_name()).await?;
            let clock = FixedClock(SystemTime::UNIX_EPOCH + Duration::from_secs(30_000));
            let repository =
                PostgresJobRepository::connect(config(runtime_url.clone())?, Arc::new(clock)).await?;
            let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
            let (_, stop) = StopSource::new();

            let first = FlowLauncher::new(&repository, &clock, &ids)
                .launch(
                    &job(point, Arc::new(PublishState), Some(Arc::new(FailAfterState)))?,
                    &JobParameters::new(),
                    &stop,
                )
                .await?;
            assert!(matches!(
                first.outcome(),
                FlowExecutionOutcome::Failed(FlowFailure::Listener(_))
            ));

            let pool = PgPoolOptions::new()
                .max_connections(1)
                .connect(&runtime_url)
                .await?;
            let affected = sqlx::query(
                "UPDATE oxide_batch.ob_step_execution step SET context_schema_version = 2 \
                 WHERE step.id = (SELECT candidate.id FROM oxide_batch.ob_step_execution candidate \
                 JOIN oxide_batch.ob_job_execution execution ON execution.id = candidate.job_execution_id \
                 JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
                 WHERE instance.job_name = $1 ORDER BY candidate.id DESC LIMIT 1)",
            )
            .bind(point.job_name())
            .execute(&pool)
            .await?;
            assert_eq!(affected.rows_affected(), 1);
            pool.close().await;

            let second = FlowLauncher::new(&repository, &clock, &ids)
                .launch(
                    &job(point, Arc::new(PanicIfInvoked), Some(Arc::new(NoopListener)))?,
                    &JobParameters::new(),
                    &stop,
                )
                .await?;
            assert!(matches!(
                second.outcome(),
                FlowExecutionOutcome::Failed(FlowFailure::CustomLeafState { .. })
            ));
            repository.close().await?;
            remove_job(&migrator_url, point.job_name()).await?;
            Ok::<(), Box<dyn Error>>(())
        })
}

#[test]
fn corrupt_persisted_custom_leaf_state_fails_closed_before_handler() -> Result<(), Box<dyn Error>> {
    let Some(runtime_url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let Some(migrator_url) = migrator_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL is not set");
        return Ok(());
    };
    let point = CrashPoint::AfterStateCommitBeforeTransition;
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            PostgresMigrator::migrate(&config(migrator_url.clone())?).await?;
            remove_job(&migrator_url, point.job_name()).await?;
            let clock = FixedClock(SystemTime::UNIX_EPOCH + Duration::from_secs(31_000));
            let repository =
                PostgresJobRepository::connect(config(runtime_url.clone())?, Arc::new(clock)).await?;
            let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
            let (_, stop) = StopSource::new();

            let first = FlowLauncher::new(&repository, &clock, &ids)
                .launch(
                    &job(point, Arc::new(PublishState), Some(Arc::new(FailAfterState)))?,
                    &JobParameters::new(),
                    &stop,
                )
                .await?;
            assert!(matches!(
                first.outcome(),
                FlowExecutionOutcome::Failed(FlowFailure::Listener(_))
            ));

            let pool = PgPoolOptions::new()
                .max_connections(1)
                .connect(&runtime_url)
                .await?;
            let affected = sqlx::query(
                "UPDATE oxide_batch.ob_step_execution step SET context_format = 99 \
                 WHERE step.id = (SELECT candidate.id FROM oxide_batch.ob_step_execution candidate \
                 JOIN oxide_batch.ob_job_execution execution ON execution.id = candidate.job_execution_id \
                 JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
                 WHERE instance.job_name = $1 ORDER BY candidate.id DESC LIMIT 1)",
            )
            .bind(point.job_name())
            .execute(&pool)
            .await?;
            assert_eq!(affected.rows_affected(), 1);
            pool.close().await;

            let result = FlowLauncher::new(&repository, &clock, &ids)
                .launch(
                    &job(point, Arc::new(PanicIfInvoked), Some(Arc::new(NoopListener)))?,
                    &JobParameters::new(),
                    &stop,
                )
                .await;
            assert!(matches!(
                result,
                Err(FlowRuntimeError::Repository(RepositoryError::FlowStateCorrupt))
            ));
            repository.close().await?;
            remove_job(&migrator_url, point.job_name()).await?;
            Ok::<(), Box<dyn Error>>(())
        })
}
'''
crash.write_text(text)
