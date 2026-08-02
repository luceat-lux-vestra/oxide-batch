//! M4 versioned telemetry, cardinality, exporter, and shutdown evidence.

#![allow(clippy::expect_used, clippy::panic)]

use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use oxide_batch::{
    ActorRef, BatchStatus, BoxFuture, Clock, ComponentRevision, DefinitionIdentity,
    DefinitionRevision, DropReportWindow, EnqueueResult, EventTiming, ExportError,
    ExportQueueBound, InMemoryExplorer, InMemoryJobRepository, JobInstanceKey, JobName,
    JobOperator, JobParameters, JobRepository, MetricCardinalityGuard, MetricDimensions,
    MetricFamily, OperationId, OperatorOutcomeClass, OperatorRequest, PageRequest, PageSize,
    SequentialIdGenerator, ShutdownCoordinator, ShutdownDeadline, ShutdownHookError,
    ShutdownHookStatus, StepName, TELEMETRY_EVENT_CATALOG, TELEMETRY_SCHEMA_VERSION,
    TELEMETRY_SPAN_CATALOG, TaskJoinDeadline, TelemetryEventKind, TelemetryEventSink,
    TelemetryExportSink, TelemetryExporter, TelemetryFlushDeadline, TelemetryFlushStatus,
    TelemetryQueue, TelemetryRecord, TelemetrySpanKind, TelemetrySpanStatus,
};

#[derive(Debug)]
struct FixedClock;

impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_hours(1)
    }
}

#[derive(Debug, Default)]
struct Capture(Mutex<Vec<TelemetryRecord>>);

impl TelemetryEventSink for Capture {
    fn emit(&self, event: &TelemetryRecord) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(event.clone());
    }
}

#[test]
fn m4_events_match_the_published_catalog_and_schema_version() {
    let mut names = TELEMETRY_EVENT_CATALOG
        .iter()
        .map(|kind| kind.as_str())
        .collect::<Vec<_>>();
    let original_len = names.len();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), original_len, "catalog names must be unique");
    assert_eq!(TELEMETRY_SCHEMA_VERSION, 1);
    assert!(
        TELEMETRY_EVENT_CATALOG
            .iter()
            .all(|kind| !kind.as_str().is_empty())
    );
    assert_eq!(
        TelemetryEventKind::OperatorRequestCompleted.timing(),
        EventTiming::AfterCommit
    );
    assert_eq!(
        TelemetryEventKind::ExplorerPageServed.timing(),
        EventTiming::AfterRead
    );
}

#[test]
fn m4_spans_match_the_published_hierarchy_and_safe_fields() {
    let mut names = TELEMETRY_SPAN_CATALOG
        .iter()
        .map(|kind| kind.as_str())
        .collect::<Vec<_>>();
    let original_len = names.len();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), original_len, "span names must be unique");
    assert_eq!(TelemetrySpanKind::JobExecution.parent(), None);
    assert_eq!(
        TelemetrySpanKind::StepExecution.parent(),
        Some(TelemetrySpanKind::JobExecution)
    );
    assert_eq!(
        TelemetrySpanKind::RepositoryCommit.parent(),
        Some(TelemetrySpanKind::ChunkAttempt)
    );
    assert_eq!(
        TelemetrySpanKind::Backoff.parent(),
        Some(TelemetrySpanKind::Retry)
    );
    assert_eq!(TelemetrySpanStatus::Unknown.as_str(), "unknown");
    assert_eq!(
        TelemetrySpanStatus::from(BatchStatus::Completed),
        TelemetrySpanStatus::Ok
    );
    assert_eq!(
        TelemetrySpanStatus::from(BatchStatus::Stopped),
        TelemetrySpanStatus::Cancelled
    );
    assert_eq!(
        TelemetrySpanStatus::from(BatchStatus::Unknown),
        TelemetrySpanStatus::Unknown
    );

    let prohibited = [
        "parameter",
        "context",
        "checkpoint",
        "credential",
        "endpoint",
        "sql",
        "error.text",
        "item.id",
        "retry.key",
    ];
    assert!(TELEMETRY_SPAN_CATALOG.iter().all(|kind| {
        kind.safe_field_keys()
            .iter()
            .all(|key| !prohibited.contains(key))
    }));
}

#[tokio::test]
async fn operator_and_recovery_events_follow_their_durable_commit() {
    let clock: Arc<dyn Clock> = Arc::new(FixedClock);
    let first = NonZeroU64::new(1).expect("one is nonzero");
    let repository = InMemoryJobRepository::new(
        Arc::clone(&clock),
        Arc::new(SequentialIdGenerator::new(first)),
    );
    let capture = Arc::new(Capture::default());
    let sink: Arc<dyn TelemetryEventSink> = capture.clone();
    let operator = JobOperator::new(repository.clone(), clock).with_event_sink(sink);
    let job_name = JobName::new("telemetry-job").expect("valid name");
    let step_name = StepName::new("only").expect("valid name");
    let definition = DefinitionIdentity::tasklet(
        &job_name,
        &step_name,
        DefinitionRevision::new("r1").expect("valid revision"),
        &ComponentRevision::new("c1").expect("valid revision"),
    )
    .expect("manifest encodes");
    let key = JobInstanceKey::new(job_name, &JobParameters::new());
    let request = OperatorRequest::launch(
        OperationId::new("telemetry-launch").expect("valid operation"),
        ActorRef::new("test:operator").expect("valid actor"),
        key,
        definition,
    );

    let outcome = operator.execute(&request).await.expect("launch commits");
    let execution_id = outcome.execution().expect("launch returns execution").id();
    assert_eq!(outcome.class(), OperatorOutcomeClass::Applied);

    let explorer = oxide_batch::JobExplorer::new(InMemoryExplorer::new(&repository));
    let page = explorer
        .list_operator_requests(execution_id, &PageRequest::first(PageSize::default()))
        .await
        .expect("audit is durable before return");
    assert_eq!(page.rows().len(), 1);
    let events = capture
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(
        events.iter().map(TelemetryRecord::kind).collect::<Vec<_>>(),
        vec![
            TelemetryEventKind::OperatorRequestAccepted,
            TelemetryEventKind::OperatorRequestCompleted,
        ]
    );
    assert!(
        events
            .iter()
            .all(|event| event.schema_version() == TELEMETRY_SCHEMA_VERSION)
    );
}

#[test]
fn metric_labels_stay_within_the_family_cardinality_budget() {
    let mut guard = MetricCardinalityGuard::default();
    let statuses = [
        BatchStatus::Starting,
        BatchStatus::Started,
        BatchStatus::Stopping,
        BatchStatus::Stopped,
        BatchStatus::Failed,
        BatchStatus::Completed,
        BatchStatus::Abandoned,
        BatchStatus::Unknown,
    ];
    let mut overflowed = 0;
    for kind in TELEMETRY_EVENT_CATALOG {
        for status in statuses {
            let observation = guard.observe(
                MetricFamily::ExecutionEvents,
                &MetricDimensions::default()
                    .with_event(*kind)
                    .with_status(status),
            );
            overflowed += usize::from(observation.overflowed());
        }
    }
    assert!(overflowed > 0);
    assert_eq!(
        u64::try_from(overflowed).expect("the test count fits"),
        guard.dropped_cardinality(MetricFamily::ExecutionEvents)
    );
    assert!(
        guard.series_count(MetricFamily::ExecutionEvents) <= oxide_batch::METRIC_CARDINALITY_BUDGET
    );
}

#[test]
fn unallowlisted_names_map_to_other() {
    let allowed_job = JobName::new("allowed").expect("valid name");
    let allowed_step = StepName::new("allowed-step").expect("valid name");
    let mut guard = MetricCardinalityGuard::new([allowed_job], [allowed_step])
        .expect("the allowlists are bounded");
    let observation = guard.observe(
        MetricFamily::ExecutionEvents,
        &MetricDimensions::default()
            .with_event(TelemetryEventKind::JobStarted)
            .with_job_name(JobName::new("outside").expect("valid name"))
            .with_step_name(StepName::new("outside-step").expect("valid name")),
    );
    let names = observation
        .labels()
        .iter()
        .filter(|label| matches!(label.key(), "job" | "step"))
        .map(oxide_batch::MetricLabel::value)
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["__other__", "__other__"]);
}

#[test]
fn full_exporter_queue_drops_newest_and_counts() {
    let queue = TelemetryQueue::new(
        ExportQueueBound::new(64).expect("minimum bound is valid"),
        DropReportWindow::default(),
    );
    for _ in 0..64 {
        assert_eq!(
            queue.enqueue(
                TelemetryRecord::catalog(TelemetryEventKind::JobStarted),
                Duration::ZERO,
            ),
            EnqueueResult::Accepted
        );
    }
    assert_eq!(
        queue.enqueue(
            TelemetryRecord::catalog(TelemetryEventKind::MigrationFailed),
            Duration::ZERO,
        ),
        EnqueueResult::Dropped { report_due: true }
    );
    assert_eq!(queue.len(), 64);
    assert_eq!(queue.dropped(), 1);
}

struct FailingExporter;

impl TelemetryExportSink for FailingExporter {
    fn export<'a>(
        &'a self,
        _record: &'a TelemetryRecord,
    ) -> BoxFuture<'a, Result<(), ExportError>> {
        Box::pin(async { panic!("adapter panic is isolated") })
    }
}

#[derive(Clone)]
struct QueueSink(TelemetryQueue);

impl TelemetryEventSink for QueueSink {
    fn emit(&self, event: &TelemetryRecord) {
        let _ = self.0.enqueue(event.clone(), Duration::ZERO);
    }
}

#[tokio::test]
async fn exporter_failure_cannot_change_execution_state() {
    let queue = TelemetryQueue::new(ExportQueueBound::default(), DropReportWindow::default());
    let clock: Arc<dyn Clock> = Arc::new(FixedClock);
    let repository = InMemoryJobRepository::new(
        Arc::clone(&clock),
        Arc::new(SequentialIdGenerator::new(
            NonZeroU64::new(1).expect("one is nonzero"),
        )),
    );
    let sink: Arc<dyn TelemetryEventSink> = Arc::new(QueueSink(queue.clone()));
    let operator = JobOperator::new(repository.clone(), clock).with_event_sink(sink);
    let job_name = JobName::new("export-failure").expect("valid name");
    let step_name = StepName::new("only").expect("valid name");
    let definition = DefinitionIdentity::tasklet(
        &job_name,
        &step_name,
        DefinitionRevision::new("r1").expect("valid revision"),
        &ComponentRevision::new("c1").expect("valid revision"),
    )
    .expect("manifest encodes");
    let request = OperatorRequest::launch(
        OperationId::new("export-failure-launch").expect("valid operation"),
        ActorRef::new("test:operator").expect("valid actor"),
        JobInstanceKey::new(job_name, &JobParameters::new()),
        definition,
    );
    let outcome = operator.execute(&request).await.expect("launch commits");
    let execution_id = outcome.execution().expect("execution exists").id();
    let exporter = TelemetryExporter::new(queue, FailingExporter);
    let report = exporter.flush().await;
    assert_eq!(report.exported(), 0);
    assert_eq!(report.failed(), 2);
    assert_eq!(report.dropped(), 0);
    let mut unit = repository
        .begin()
        .await
        .expect("repository remains available");
    let execution = unit
        .get_job_execution(execution_id)
        .await
        .expect("read succeeds")
        .expect("execution remains durable");
    unit.rollback().await.expect("read-only rollback succeeds");
    assert_eq!(execution.metadata().status(), BatchStatus::Starting);
}

#[tokio::test]
async fn telemetry_flush_deadline_is_separate_from_shutdown() {
    let shutdown = ShutdownDeadline::new(Duration::from_secs(1)).expect("valid deadline");
    let join = TaskJoinDeadline::new(Duration::from_secs(1), shutdown).expect("valid join");
    let telemetry =
        TelemetryFlushDeadline::new(Duration::from_millis(100)).expect("valid telemetry deadline");
    let mut coordinator =
        ShutdownCoordinator::new(shutdown, join, telemetry).expect("valid coordinator");
    let report = coordinator
        .shutdown(
            || async { Ok::<(), ShutdownHookError>(()) },
            std::future::pending::<Result<u64, ShutdownHookError>>,
            || async { Ok::<(), ShutdownHookError>(()) },
        )
        .await;
    assert_eq!(report.persistence(), ShutdownHookStatus::Completed);
    assert_eq!(report.telemetry(), TelemetryFlushStatus::DeadlineExceeded);
    assert_eq!(report.repository_close(), ShutdownHookStatus::Completed);
}

#[test]
fn typed_metric_dimensions_do_not_accept_opaque_identifiers() {
    let dimensions = MetricDimensions::default()
        .with_action(oxide_batch::OperatorAction::Stop)
        .with_authorization(oxide_batch::AuthorizationClass::Lifecycle)
        .with_outcome(OperatorOutcomeClass::Applied)
        .with_status(BatchStatus::Started);
    let mut guard = MetricCardinalityGuard::default();
    let observation = guard.observe(MetricFamily::RecoveryOutcomes, &dimensions);
    assert!(
        observation
            .labels()
            .iter()
            .all(|label| !matches!(label.key(), "operation_id" | "actor" | "execution_id"))
    );
}
