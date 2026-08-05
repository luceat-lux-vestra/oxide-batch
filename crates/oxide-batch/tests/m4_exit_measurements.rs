//! Bounded M4 operations and local-scale measurements.
//!
//! These measurements close the M4 section of the
//! [performance plan](../../../docs/engineering/performance-plan.md). Each one
//! asserts the resource, ordering, and equivalence properties that make its
//! numbers meaningful, then records raw machine-readable evidence.
//!
//! Every assertion here is structural rather than timing-based: durations are
//! recorded, never compared against a threshold, so the suite stays
//! deterministic on a loaded host. Where a measurement needs a workload with
//! real await time, it uses an explicit bounded sleep as the *workload* rather
//! than as a synchronization device.

// Reported ratios convert bounded counters into floating point for the report.
#![allow(clippy::cast_precision_loss)]
// Each measurement is one linear scenario whose ordering is part of the
// evidence, so its scale points stay in a single readable sequence.
#![allow(clippy::too_many_lines)]

#[path = "measurement/mod.rs"]
mod measurement;

use std::collections::BTreeMap;
use std::error::Error;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use measurement::{Latencies, MeasurementError, Report, WORKER_THREADS, resident_kib};
use oxide_batch::{
    ActorRef, BatchStatus, BoxFuture, Clock, ComponentRevision, DefinitionRevision, DrainResult,
    DropReportWindow, EnqueueResult, ExecutionContext, ExecutionCounts, ExitStatus,
    ExportQueueBound, FlowEvent, FlowEventSink, FlowExecutionOutcome, FlowGraph, FlowJob,
    FlowLauncher, FlowNode, FlowRuntimeError, FlowTarget, IdGenerator, InMemoryExplorer,
    InMemoryJobRepository, JobExplorer, JobInstanceKey, JobLauncher, JobName, JobParameter,
    JobParameters, JobRepository, LifecycleEvent, LifecycleEventSink, LifecycleTransition,
    MAX_CURSOR_BYTES, MetricCardinalityGuard, MetricDimensions, MetricFamily, NodeId, OperationId,
    PageRequest, PageSize, ParameterName, ParameterRole, ParameterValue, PartitionBudget,
    PartitionCount, PartitionFactoryError, PartitionKey, PartitionPlanEntry, PartitionPlanFactory,
    PartitionTaskletFactory, PartitionedStepNode, PurgeBatchBound, PurgePlanRequest, ReasonCode,
    RepositoryDescriptor, RepositoryError, RepositoryUnitOfWork, RetentionService,
    SequentialIdGenerator, ShutdownCoordinator, ShutdownDeadline, ShutdownRequest,
    ShutdownTaskPhase, StateLimits, StepComponents, StepName, StepNode, StopSource,
    TaskJoinDeadline, Tasklet, TaskletContext, TaskletError, TaskletJob, TaskletOutcome,
    TaskletStep, TelemetryEventKind, TelemetryFlushDeadline, TelemetryQueue, TelemetryRecord,
    TerminalKind, TerminalStatusSet,
};

/// A clock frozen for durable timestamps so observations stay comparable.
#[derive(Debug)]
struct FixedClock(SystemTime);

impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        self.0
    }
}

/// A clock the retention measurement advances explicitly.
#[derive(Debug)]
struct AdvancingClock {
    origin: SystemTime,
    offset: AtomicU64,
}

impl AdvancingClock {
    fn new(origin: SystemTime) -> Self {
        Self {
            origin,
            offset: AtomicU64::new(0),
        }
    }

    fn advance(&self, step: Duration) {
        self.offset.fetch_add(
            step.as_millis().try_into().unwrap_or(u64::MAX),
            Ordering::SeqCst,
        );
    }
}

impl Clock for AdvancingClock {
    fn now(&self) -> SystemTime {
        self.origin + Duration::from_millis(self.offset.load(Ordering::SeqCst))
    }
}

/// A bounded exporter path attached to a flow attempt.
///
/// The sink maps every committed flow observation onto the telemetry schema,
/// offers it to a finite drop-newest queue, and applies the metric cardinality
/// budget, so the measured overhead covers the whole export path rather than a
/// bare callback.
struct RecordingSink {
    queue: TelemetryQueue,
    metrics: Mutex<MetricCardinalityGuard>,
    started: Instant,
    offered: AtomicUsize,
    accepted: AtomicUsize,
    rejected: AtomicUsize,
    peak_depth: AtomicUsize,
}

impl RecordingSink {
    fn new(bound: ExportQueueBound, guard: MetricCardinalityGuard) -> Self {
        Self {
            queue: TelemetryQueue::new(bound, DropReportWindow::default()),
            metrics: Mutex::new(guard),
            started: Instant::now(),
            offered: AtomicUsize::new(0),
            accepted: AtomicUsize::new(0),
            rejected: AtomicUsize::new(0),
            peak_depth: AtomicUsize::new(0),
        }
    }

    fn offered(&self) -> usize {
        self.offered.load(Ordering::SeqCst)
    }

    fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }

    fn rejected(&self) -> u64 {
        self.rejected.load(Ordering::SeqCst) as u64
    }

    fn dropped(&self) -> u64 {
        self.queue.dropped()
    }

    fn queue_len(&self) -> usize {
        self.queue.len()
    }

    fn peak_depth(&self) -> usize {
        self.peak_depth.load(Ordering::SeqCst)
    }

    fn series(&self) -> usize {
        self.metrics
            .lock()
            .map_or(0, |guard| guard.series_count(MetricFamily::ExecutionEvents))
    }

    /// Applies the metric budget and offers one record to the bounded queue.
    fn record(&self, kind: TelemetryEventKind, dimensions: &MetricDimensions) {
        self.offered.fetch_add(1, Ordering::SeqCst);
        if let Ok(mut metrics) = self.metrics.lock() {
            let _ = metrics.observe(MetricFamily::ExecutionEvents, dimensions);
        }
        match self
            .queue
            .enqueue(TelemetryRecord::catalog(kind), self.started.elapsed())
        {
            EnqueueResult::Accepted => {
                self.accepted.fetch_add(1, Ordering::SeqCst);
            }
            EnqueueResult::Dropped { .. } => {
                self.rejected.fetch_add(1, Ordering::SeqCst);
            }
            // The enqueue result is non-exhaustive; a future outcome is
            // neither queued nor a counted drop until it is reviewed here.
            _ => {}
        }
        self.peak_depth
            .fetch_max(self.queue.len(), Ordering::SeqCst);
    }
}

impl FlowEventSink for RecordingSink {
    fn emit(&self, event: &FlowEvent) {
        let kind = event.kind().telemetry_kind();
        self.record(
            kind,
            &MetricDimensions::default()
                .with_event(kind)
                .with_job_name(event.job_name().clone()),
        );
    }
}

impl LifecycleEventSink for RecordingSink {
    fn emit(&self, event: &LifecycleEvent) {
        let kind = event.kind().telemetry_kind();
        self.record(kind, &MetricDimensions::default().with_event(kind));
    }
}

/// Counts the units of work a run opened without altering their behavior.
struct CountingRepository<'a> {
    inner: &'a InMemoryJobRepository,
    begins: Arc<AtomicUsize>,
    capacity: Option<u32>,
}

impl<'a> CountingRepository<'a> {
    fn new(inner: &'a InMemoryJobRepository) -> Self {
        Self {
            inner,
            begins: Arc::new(AtomicUsize::new(0)),
            capacity: None,
        }
    }

    /// Presents an explicit pool ceiling instead of the adapter's own.
    fn with_capacity(mut self, capacity: u32) -> Self {
        self.capacity = Some(capacity);
        self
    }

    fn begins(&self) -> usize {
        self.begins.load(Ordering::SeqCst)
    }
}

impl JobRepository for CountingRepository<'_> {
    fn connection_capacity(&self) -> u32 {
        self.capacity
            .unwrap_or_else(|| self.inner.connection_capacity())
    }

    /// Delegates the capability declaration: this double narrows the
    /// connection budget, not what the deployment can do.
    fn descriptor(&self) -> RepositoryDescriptor {
        self.inner.descriptor()
    }

    fn begin<'a>(
        &'a self,
    ) -> BoxFuture<'a, Result<Box<dyn RepositoryUnitOfWork + 'a>, RepositoryError>> {
        self.begins.fetch_add(1, Ordering::SeqCst);
        self.inner.begin()
    }
}

/// Shared occupancy and duration observations for one partitioned run.
#[derive(Debug, Default)]
struct Occupancy {
    active: AtomicUsize,
    peak: AtomicUsize,
    finished_at: Mutex<Option<Instant>>,
    durations: Mutex<Vec<Duration>>,
}

impl Occupancy {
    fn enter(&self) {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
    }

    fn leave(&self, elapsed: Duration) {
        self.active.fetch_sub(1, Ordering::SeqCst);
        if let Ok(mut durations) = self.durations.lock() {
            durations.push(elapsed);
        }
        if let Ok(mut finished) = self.finished_at.lock() {
            *finished = Some(Instant::now());
        }
    }

    fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }

    fn active(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }

    fn last_finished(&self) -> Option<Instant> {
        self.finished_at.lock().ok().and_then(|guard| *guard)
    }

    fn skew(&self) -> (Duration, Duration) {
        let Ok(durations) = self.durations.lock() else {
            return (Duration::ZERO, Duration::ZERO);
        };
        (
            durations.iter().copied().min().unwrap_or_default(),
            durations.iter().copied().max().unwrap_or_default(),
        )
    }
}

/// A worker whose await time models bounded external work.
struct AwaitingWorker {
    occupancy: Arc<Occupancy>,
    work: Duration,
}

impl Tasklet for AwaitingWorker {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            let started = Instant::now();
            self.occupancy.enter();
            tokio::time::sleep(self.work).await;
            self.occupancy.leave(started.elapsed());
            Ok(TaskletOutcome::Completed)
        })
    }
}

/// The fixed no-op body of the P-001 lifecycle workload.
struct NoOpTasklet;

impl Tasklet for NoOpTasklet {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async { Ok(TaskletOutcome::Completed) })
    }
}

/// Per-partition invocation counts shared across the attempts of one cycle.
type Invocations = Arc<Mutex<BTreeMap<String, usize>>>;

/// A worker that fails its designated partition once before completing.
struct FlakyWorker {
    occupancy: Arc<Occupancy>,
    invocations: Invocations,
    failures: Arc<AtomicUsize>,
    key: String,
    fails_first: bool,
}

impl Tasklet for FlakyWorker {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            let started = Instant::now();
            self.occupancy.enter();
            if let Ok(mut invocations) = self.invocations.lock() {
                *invocations.entry(self.key.clone()).or_default() += 1;
            }
            tokio::task::yield_now().await;
            self.occupancy.leave(started.elapsed());
            if self.fails_first && self.failures.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(TaskletError::new());
            }
            Ok(TaskletOutcome::Completed)
        })
    }
}

/// A worker that reports a cooperative stop instead of completing.
struct CancellableWorker {
    entered: Arc<tokio::sync::Notify>,
    observed_stop_at: Arc<Mutex<Option<Instant>>>,
    occupancy: Arc<Occupancy>,
}

impl Tasklet for CancellableWorker {
    fn execute<'a>(
        &'a self,
        context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            let started = Instant::now();
            self.occupancy.enter();
            self.entered.notify_one();
            context.stop_token().cancelled().await;
            if let Ok(mut observed) = self.observed_stop_at.lock() {
                observed.get_or_insert_with(Instant::now);
            }
            self.occupancy.leave(started.elapsed());
            Ok(TaskletOutcome::Stopped)
        })
    }
}

/// The durable observation a scale point must reproduce exactly.
#[derive(Debug, Eq, PartialEq)]
struct NormalizedPartition {
    key: String,
    status: BatchStatus,
    exit_status: ExitStatus,
    counts: ExecutionCounts,
}

/// The complete durable observation compared across scale points.
#[derive(Debug, Eq, PartialEq)]
struct NormalizedObservation {
    job_status: BatchStatus,
    job_exit_status: ExitStatus,
    parent_status: BatchStatus,
    parent_counts: ExecutionCounts,
    partitions: Vec<NormalizedPartition>,
}

fn infrastructure() -> (
    Arc<FixedClock>,
    Arc<SequentialIdGenerator>,
    InMemoryJobRepository,
) {
    let clock = Arc::new(FixedClock(
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000),
    ));
    let ids = Arc::new(SequentialIdGenerator::new(NonZeroU64::MIN));
    let repository = InMemoryJobRepository::new(clock.clone(), ids.clone());
    (clock, ids, repository)
}

fn partition_entry(key: &str) -> Result<PartitionPlanEntry, Box<dyn Error>> {
    let context = ExecutionContext::from_json(
        format!(
            "{{\"format\":\"oxide-batch.execution-context\",\"format_version\":1,\"schema\":\"local.partition\",\"schema_version\":1,\"payload\":{{\"key\":\"{key}\"}}}}"
        )
        .as_bytes(),
        StateLimits::new(4 * 1024, 16)?,
    )?;
    Ok(PartitionPlanEntry::new(PartitionKey::new(key)?, context)?)
}

fn partition_keys(count: u16) -> Vec<String> {
    (0..count)
        .map(|index| format!("partition-{index:04}"))
        .collect()
}

fn partition_plan_factory(keys: &[String]) -> Result<PartitionPlanFactory, Box<dyn Error>> {
    let entries = keys
        .iter()
        .map(|key| partition_entry(key))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(PartitionPlanFactory::new(move |request| {
        if usize::from(request.partition_count().get()) != entries.len() {
            return Err(PartitionFactoryError::Rejected);
        }
        Ok(entries.clone())
    }))
}

fn partitioned_plan(
    name: &JobName,
    partitions: u16,
    workers: u8,
) -> Result<oxide_batch::CompiledExecutionPlan, Box<dyn Error>> {
    let manager = NodeId::new("partitioned")?;
    let worker = StepNode::new(
        NodeId::new("worker")?,
        StepName::new("worker")?,
        StepComponents::Tasklet(ComponentRevision::new("worker-v1")?),
    );
    Ok(FlowGraph::new(manager.clone())
        .with_node(FlowNode::partitioned_step(PartitionedStepNode::new(
            manager.clone(),
            StepName::new("partitioned")?,
            worker,
            ComponentRevision::new("partitioner-v1")?,
            ComponentRevision::new("canonical-v1")?,
            PartitionCount::new(partitions)?,
            PartitionBudget::new(workers, pool_budget(workers))?,
        )))
        .with_sequence(manager, FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(name, DefinitionRevision::new("v1")?)?)
}

/// The pool a partitioned step derives from its worker budget.
const fn pool_budget(workers: u8) -> u32 {
    workers as u32 + 1
}

fn awaiting_factory(
    occupancy: Arc<Occupancy>,
    work: Duration,
) -> Result<PartitionTaskletFactory, Box<dyn Error>> {
    let step_name = StepName::new("worker")?;
    let factory_name = step_name.clone();
    Ok(PartitionTaskletFactory::new(step_name, move |_input| {
        TaskletStep::new(
            factory_name.clone(),
            Arc::new(AwaitingWorker {
                occupancy: Arc::clone(&occupancy),
                work,
            }),
        )
    }))
}

/// Builds a factory whose designated partition fails on its first attempt.
fn flaky_factory(
    occupancy: Arc<Occupancy>,
    invocations: Invocations,
    failing_key: &str,
) -> Result<PartitionTaskletFactory, Box<dyn Error>> {
    let step_name = StepName::new("worker")?;
    let factory_name = step_name.clone();
    let failing_key = failing_key.to_owned();
    let failures = Arc::new(AtomicUsize::new(0));
    Ok(PartitionTaskletFactory::new(step_name, move |input| {
        TaskletStep::new(
            factory_name.clone(),
            Arc::new(FlakyWorker {
                occupancy: Arc::clone(&occupancy),
                invocations: Arc::clone(&invocations),
                failures: Arc::clone(&failures),
                key: input.key().as_str().to_owned(),
                fails_first: input.key().as_str() == failing_key,
            }),
        )
    }))
}

/// Reads the current per-partition invocation counts.
fn invocation_snapshot(invocations: &Invocations) -> BTreeMap<String, usize> {
    invocations
        .lock()
        .map(|counts| counts.clone())
        .unwrap_or_default()
}

/// Reads the durable observation a partitioned attempt left behind.
async fn observe(
    repository: &InMemoryJobRepository,
    report: &oxide_batch::FlowLaunchReport,
) -> Result<NormalizedObservation, Box<dyn Error>> {
    let parent = report
        .step_executions()
        .last()
        .ok_or_else(|| MeasurementError::new("the attempt recorded no parent step"))?;
    let mut unit = repository.begin().await?;
    let partitions = unit.step_partition_plan(parent.id()).await?;
    unit.rollback().await?;
    Ok(NormalizedObservation {
        job_status: report.job_execution().metadata().status(),
        job_exit_status: report.job_execution().metadata().exit_status().clone(),
        parent_status: parent.metadata().status(),
        parent_counts: parent.metadata().counts(),
        partitions: partitions
            .iter()
            .map(|partition| NormalizedPartition {
                key: partition.key().as_str().to_owned(),
                status: partition.status(),
                exit_status: partition.exit_status().clone(),
                counts: partition.counts(),
            })
            .collect(),
    })
}

/// P-010: local partition scaling, skew, aggregation, and pool ceilings.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn p010_local_partition_scaling() -> Result<(), Box<dyn Error>> {
    const PARTITIONS: u16 = 64;
    const WORK: Duration = Duration::from_millis(4);
    let worker_points: [u8; 3] = [1, 10, 64];

    let mut report = Report::new(
        "P-010",
        "1/10/100 local partitions",
        "Bounded local partition scaling at the largest configured worker count",
    );
    let keys = partition_keys(PARTITIONS);
    let mut baseline_throughput = None;
    let mut baseline_observation = None;
    let mut ceilings_hold = true;
    let mut equivalence_holds = true;

    for workers in worker_points {
        let name = JobName::new(format!("m4-p010-workers-{workers}"))?;
        let occupancy = Arc::new(Occupancy::default());
        let job = FlowJob::new(name.clone(), partitioned_plan(&name, PARTITIONS, workers)?)?
            .with_partitioned_tasklet(
                NodeId::new("partitioned")?,
                partition_plan_factory(&keys)?,
                awaiting_factory(Arc::clone(&occupancy), WORK)?,
            )?;
        let (clock, ids, repository) = infrastructure();
        let counting = CountingRepository::new(&repository);
        let (_source, stop) = StopSource::new();

        let started = Instant::now();
        let launched = FlowLauncher::new(&counting, clock.as_ref(), ids.as_ref())
            .launch(&job, &JobParameters::new(), &stop)
            .await?;
        let elapsed = started.elapsed();

        let observation = observe(&repository, &launched).await?;
        let (min_worker, max_worker) = occupancy.skew();
        let aggregation = occupancy.last_finished().map(|last| {
            started
                .elapsed()
                .saturating_sub(last.duration_since(started))
        });
        let throughput = f64::from(PARTITIONS) / elapsed.as_secs_f64();
        let efficiency = baseline_throughput
            .map(|baseline: f64| throughput / (baseline * f64::from(u32::from(workers))));

        ceilings_hold &= occupancy.peak() <= usize::from(workers) && occupancy.active() == 0;
        equivalence_holds &= baseline_observation
            .as_ref()
            .is_none_or(|first| first == &observation);
        assert!(
            occupancy.peak() <= usize::from(workers),
            "peak occupancy {} exceeded the configured budget {workers}",
            occupancy.peak()
        );
        assert_eq!(occupancy.active(), 0, "a worker outlived its parent");
        assert_eq!(launched.outcome(), &FlowExecutionOutcome::Completed);

        report.point(serde_json::json!({
            "workers": workers,
            "partitions": PARTITIONS,
            "worker_await_millis": WORK.as_millis(),
            "wall_micros": elapsed.as_micros(),
            "partitions_per_second": throughput,
            "scaling_efficiency": efficiency,
            "peak_active_workers": occupancy.peak(),
            "active_workers_after_join": occupancy.active(),
            "worker_duration_min_micros": min_worker.as_micros(),
            "worker_duration_max_micros": max_worker.as_micros(),
            "worker_skew_micros": max_worker.saturating_sub(min_worker).as_micros(),
            "aggregation_micros": aggregation.map(|value| value.as_micros()),
            "repository_units": counting.begins(),
            "repository_units_per_partition":
                counting.begins() as f64 / f64::from(PARTITIONS),
            "configured_pool": pool_budget(workers),
            "resident_kib": resident_kib(),
        }));

        if baseline_throughput.is_none() {
            baseline_throughput = Some(throughput);
        }
        if baseline_observation.is_none() {
            baseline_observation = Some(observation);
        }
    }

    // The derived pool is the connection ceiling, so a pool one connection
    // short of the budget must fail closed before any worker starts.
    let name = JobName::new("m4-p010-pool-ceiling")?;
    let occupancy = Arc::new(Occupancy::default());
    let job = FlowJob::new(name.clone(), partitioned_plan(&name, 4, 4)?)?
        .with_partitioned_tasklet(
            NodeId::new("partitioned")?,
            partition_plan_factory(&partition_keys(4))?,
            awaiting_factory(Arc::clone(&occupancy), Duration::ZERO)?,
        )?;
    let (clock, ids, repository) = infrastructure();
    let starved = CountingRepository::new(&repository).with_capacity(pool_budget(4) - 1);
    let (_source, stop) = StopSource::new();
    let rejected = FlowLauncher::new(&starved, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await;
    let rejected_before_start = matches!(
        rejected,
        Err(FlowRuntimeError::InsufficientPoolCapacity { .. })
    ) && occupancy.peak() == 0;
    assert!(
        rejected_before_start,
        "an insufficient pool did not fail closed before the first worker"
    );

    report
        .correctness(
            "peak occupancy never exceeded the configured worker budget",
            ceilings_hold,
        )
        .correctness(
            "every scale point produced the same durable partition observation",
            equivalence_holds,
        )
        .correctness(
            "a pool below the derived budget failed closed before any worker started",
            rejected_before_start,
        )
        .note(
            "Workers await a bounded timer rather than burning CPU, so the reported \
             scaling efficiency measures the launcher's concurrency ceiling and \
             aggregation cost rather than host parallelism.",
        )
        .note(format!(
            "Every run used one Tokio runtime with {WORKER_THREADS} worker threads."
        ));
    report.write()?;
    Ok(())
}

/// P-012: bounded explorer pagination over a growing execution history.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn p012_explorer_pagination_bounds() -> Result<(), Box<dyn Error>> {
    let history_points: [usize; 3] = [1_000, 5_000, 20_000];
    let page_size = PageSize::new(500)?;

    let mut report = Report::new(
        "P-012",
        "execution-history pagination",
        "Keyset explorer pagination stays bounded as history grows",
    );
    let mut bounds_hold = true;
    let mut traversal_holds = true;

    for history in history_points {
        let (clock, ids, repository) = infrastructure();
        let name = JobName::new("m4-p012")?;
        seed_instances(&repository, &name, history).await?;
        let explorer = JobExplorer::new(InMemoryExplorer::new(&repository));

        let mut latencies = Latencies::new();
        let mut observed = Vec::with_capacity(history);
        let mut widest_page = 0_usize;
        let mut widest_cursor = 0_usize;
        let mut request = PageRequest::first(page_size);
        loop {
            let started = Instant::now();
            let page = explorer.list_instances(&name, &request).await?;
            latencies.record(started.elapsed());
            widest_page = widest_page.max(page.rows().len());
            for row in page.rows() {
                observed.push(row.id());
            }
            match page.next_cursor() {
                Some(cursor) => {
                    widest_cursor = widest_cursor.max(cursor.as_bytes().len());
                    request = PageRequest::resume(page_size, cursor.clone());
                }
                None => break,
            }
        }

        let mut unique = observed.clone();
        unique.sort_unstable();
        unique.dedup();
        let page_bound_holds = widest_page <= usize::from(page_size.get());
        let cursor_bound_holds = widest_cursor <= MAX_CURSOR_BYTES;
        let each_row_once = unique.len() == observed.len() && observed.len() == history;
        bounds_hold &= page_bound_holds && cursor_bound_holds;
        traversal_holds &= each_row_once;
        assert!(page_bound_holds, "a page returned more rows than its bound");
        assert!(
            cursor_bound_holds,
            "a cursor exceeded {MAX_CURSOR_BYTES} bytes"
        );
        assert!(
            each_row_once,
            "the traversal did not return each row exactly once"
        );

        report.point(serde_json::json!({
            "history_rows": history,
            "page_size": page_size.get(),
            "pages": latencies.len(),
            "rows_returned": observed.len(),
            "widest_page_rows": widest_page,
            "widest_cursor_bytes": widest_cursor,
            "cursor_bound_bytes": MAX_CURSOR_BYTES,
            "page_latency": latencies.summary(),
            "resident_kib": resident_kib(),
        }));
        drop(clock);
        drop(ids);
    }

    report
        .correctness(
            "no page exceeded its requested size and cursor bound",
            bounds_hold,
        )
        .correctness(
            "every traversal returned each row exactly once",
            traversal_holds,
        )
        .note(
            "The in-memory adapter is a deterministic fixture that clones its whole \
             state per query, so its per-page latency grows with history and is not a \
             capacity budget. Only the result-set, cursor, and traversal bounds are \
             adapter-independent here; per-page cost, rows examined, and index \
             selection belong to the PostgreSQL campaign.",
        );
    report.write()?;
    Ok(())
}

/// Retention: bounded purge batches and their effect on interleaved launches.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retention_bounded_batch_throughput() -> Result<(), Box<dyn Error>> {
    const ELIGIBLE: usize = 200;
    const BATCH: u32 = 50;

    let mut report = Report::new(
        "RETENTION",
        "bounded metadata retention",
        "Plan and apply throughput per bounded purge batch and its effect on launches",
    );

    let clock = Arc::new(AdvancingClock::new(
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000),
    ));
    let ids = Arc::new(SequentialIdGenerator::new(NonZeroU64::MIN));
    let repository = InMemoryJobRepository::new(clock.clone(), ids.clone());
    let name = JobName::new("m4-retention")?;
    seed_terminal_instances(&repository, clock.as_ref(), &name, ELIGIBLE).await?;
    clock.advance(Duration::from_hours(2));

    let retention = RetentionService::new(repository.clone(), Arc::clone(&clock) as _);
    let request = PurgePlanRequest::new(
        name.clone(),
        TerminalStatusSet::all(),
        Duration::from_hours(1),
        PurgeBatchBound::new(BATCH)?,
    )?;

    // A quiet launch establishes the baseline the interleaved launches compare
    // against, so the reported impact isolates the purge campaign.
    let baseline = time_launch(&repository, clock.as_ref(), ids.as_ref(), "baseline").await?;

    let mut plan_latencies = Latencies::new();
    let mut apply_latencies = Latencies::new();
    let mut launch_latencies = Latencies::new();
    let mut purged = 0_u64;
    let mut rounds = 0_usize;
    let mut batches_bounded = true;
    let mut launches_succeeded = true;

    loop {
        let started = Instant::now();
        let plan = retention.plan_purge(&request).await?;
        plan_latencies.record(started.elapsed());
        if plan.is_empty() {
            break;
        }
        batches_bounded &= plan.candidates().len() <= BATCH as usize;
        let started = Instant::now();
        let applied = retention
            .apply_purge(
                OperationId::new(format!("m4-retention-{rounds}"))?,
                ActorRef::new("operator:m4-measurement")?,
                ReasonCode::new("M4_EXIT_MEASUREMENT")?,
                &plan,
            )
            .await?;
        apply_latencies.record(started.elapsed());
        purged += applied.counts().job_executions();
        rounds += 1;

        let interleaved = time_launch(
            &repository,
            clock.as_ref(),
            ids.as_ref(),
            &format!("interleaved-{rounds}"),
        )
        .await;
        match interleaved {
            Ok(elapsed) => launch_latencies.record(elapsed),
            Err(_) => launches_succeeded = false,
        }
    }

    let all_purged = purged == ELIGIBLE as u64;
    assert!(batches_bounded, "a purge plan exceeded its batch bound");
    assert!(
        all_purged,
        "the campaign purged {purged} of {ELIGIBLE} eligible executions"
    );
    assert!(
        launches_succeeded,
        "an interleaved launch failed during the purge campaign"
    );

    report
        .point(serde_json::json!({
            "eligible_executions": ELIGIBLE,
            "batch_bound": BATCH,
            "rounds": rounds,
            "purged_executions": purged,
            "plan_latency": plan_latencies.summary(),
            "apply_latency": apply_latencies.summary(),
            "baseline_launch_micros": baseline.as_micros(),
            "interleaved_launch_latency": launch_latencies.summary(),
            "resident_kib": resident_kib(),
        }))
        .correctness(
            "every plan stayed within its configured batch bound",
            batches_bounded,
        )
        .correctness(
            "the campaign purged exactly the eligible executions",
            all_purged,
        )
        .correctness(
            "every launch interleaved with the campaign still committed",
            launches_succeeded,
        )
        .note(
            "The in-memory adapter serializes writers behind one revision check, so \
             launches are interleaved between purge batches rather than run truly \
             concurrently. Lock-wait behavior under real concurrency belongs to the \
             PostgreSQL campaign.",
        );
    report.write()?;
    Ok(())
}

/// P-014: stop and shutdown latency measured per phase.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn p014_cancellation_and_shutdown_latency() -> Result<(), Box<dyn Error>> {
    const WORKERS: u8 = 8;

    let mut report = Report::new(
        "P-014",
        "stop/cancel/drain under load",
        "Cancellation and shutdown latency separated by phase, with unjoined counts",
    );

    // Phase one: a cooperative stop reaches running workers and then a durable
    // terminal status, with no worker outliving its parent.
    let name = JobName::new("m4-p014-stop")?;
    let keys = partition_keys(u16::from(WORKERS));
    let entered = Arc::new(tokio::sync::Notify::new());
    let observed_stop_at = Arc::new(Mutex::new(None));
    let occupancy = Arc::new(Occupancy::default());
    let step_name = StepName::new("worker")?;
    let factory_name = step_name.clone();
    let factory = PartitionTaskletFactory::new(step_name, {
        let entered = Arc::clone(&entered);
        let observed_stop_at = Arc::clone(&observed_stop_at);
        let occupancy = Arc::clone(&occupancy);
        move |_input| {
            TaskletStep::new(
                factory_name.clone(),
                Arc::new(CancellableWorker {
                    entered: Arc::clone(&entered),
                    observed_stop_at: Arc::clone(&observed_stop_at),
                    occupancy: Arc::clone(&occupancy),
                }),
            )
        }
    });
    let job = FlowJob::new(
        name.clone(),
        partitioned_plan(&name, u16::from(WORKERS), WORKERS)?,
    )?
    .with_partitioned_tasklet(
        NodeId::new("partitioned")?,
        partition_plan_factory(&keys)?,
        factory,
    )?;
    let (clock, ids, repository) = infrastructure();
    let (source, stop) = StopSource::new();
    let runner = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());
    let parameters = JobParameters::new();
    let requested_at = Arc::new(Mutex::new(None));
    let launch = runner.launch(&job, &parameters, &stop);
    let request = {
        let entered = Arc::clone(&entered);
        let requested_at = Arc::clone(&requested_at);
        async move {
            entered.notified().await;
            if let Ok(mut slot) = requested_at.lock() {
                *slot = Some(Instant::now());
            }
            source.request_stop();
        }
    };
    let (stopped, ()) = tokio::join!(launch, request);
    let stopped = stopped?;
    let returned_at = Instant::now();

    let requested = requested_at
        .lock()
        .ok()
        .and_then(|slot| *slot)
        .ok_or_else(|| MeasurementError::new("the stop request was never timed"))?;
    let observed = observed_stop_at
        .lock()
        .ok()
        .and_then(|slot| *slot)
        .ok_or_else(|| MeasurementError::new("no worker observed the stop request"))?;
    let to_intake_stop = observed.saturating_duration_since(requested);
    let to_durable_terminal = returned_at.saturating_duration_since(requested);
    let stopped_durably = stopped.job_execution().metadata().status() == BatchStatus::Stopped;
    let joined_every_worker = occupancy.active() == 0;
    assert!(
        stopped_durably,
        "the stopped attempt did not persist a stopped status"
    );
    assert!(joined_every_worker, "a worker outlived the stopped parent");
    assert!(
        to_intake_stop <= to_durable_terminal,
        "the durable terminal preceded cancellation propagation"
    );

    report.point(serde_json::json!({
        "phase": "runtime_stop",
        "workers": WORKERS,
        "request_to_worker_cancellation_micros": to_intake_stop.as_micros(),
        "request_to_durable_terminal_micros": to_durable_terminal.as_micros(),
        "durable_status": format!("{:?}", stopped.job_execution().metadata().status()),
        "active_workers_after_join": occupancy.active(),
    }));

    // Phase two: the application-owned coordinator joins every owned child and
    // reports the phases that remained when a second request ends waiting.
    let mut coordinator = ShutdownCoordinator::new(
        ShutdownDeadline::new(Duration::from_secs(5))?,
        TaskJoinDeadline::new(
            Duration::from_secs(5),
            ShutdownDeadline::new(Duration::from_secs(5))?,
        )?,
        TelemetryFlushDeadline::new(Duration::from_millis(500))?,
    )?;
    // The children wait on the crate's level-triggered cooperative token, so a
    // release cannot be missed by a child that has not started waiting yet.
    let (release, released) = StopSource::new();
    for phase in [
        ShutdownTaskPhase::Tasklet,
        ShutdownTaskPhase::ChunkReadProcess,
        ShutdownTaskPhase::Transaction,
    ] {
        for _ in 0..4 {
            let released = released.clone();
            coordinator.spawn(phase, async move {
                released.cancelled().await;
            })?;
        }
    }
    let signal = coordinator.signal();
    let requested_at = Instant::now();
    let release_all = async move {
        // Every child returns as soon as it observes the release, so the
        // measured drain is the coordinator's join cost, not a sleep.
        tokio::task::yield_now().await;
        release.request_stop();
    };
    let drain = async {
        coordinator
            .shutdown(|| async { Ok(()) }, || async { Ok(0) }, || async { Ok(()) })
            .await
    };
    let (clean, ()) = tokio::join!(drain, release_all);
    let clean_drain_micros = requested_at.elapsed().as_micros();
    let intake_closed = signal.ensure_accepting().is_err();
    let joined_every_child = matches!(clean.drain(), DrainResult::Complete { panicked_tasks: 0 });
    assert!(
        joined_every_child,
        "the coordinator did not join every owned child"
    );
    assert!(intake_closed, "intake stayed open after shutdown");

    // A second request ends waiting immediately and reports what remained.
    let mut escalating = ShutdownCoordinator::new(
        ShutdownDeadline::new(Duration::from_secs(5))?,
        TaskJoinDeadline::new(
            Duration::from_secs(5),
            ShutdownDeadline::new(Duration::from_secs(5))?,
        )?,
        TelemetryFlushDeadline::new(Duration::from_millis(500))?,
    )?;
    let (held, holds) = StopSource::new();
    for _ in 0..3 {
        let holds = holds.clone();
        escalating.spawn(ShutdownTaskPhase::Transaction, async move {
            holds.cancelled().await;
        })?;
    }
    let signal = escalating.signal();
    // The application records the first request itself, so entering
    // coordination cannot turn it into an escalation. The concurrent second
    // request is therefore the one that ends waiting.
    let first_request = signal.request_shutdown();
    let escalate = async move {
        tokio::task::yield_now().await;
        signal.request_shutdown()
    };
    let escalated_at = Instant::now();
    let (incomplete, second) = tokio::join!(
        escalating.shutdown(|| async { Ok(()) }, || async { Ok(0) }, || async { Ok(()) }),
        escalate
    );
    let escalation_micros = escalated_at.elapsed().as_micros();
    held.request_stop();
    let reported_unjoined = match incomplete.drain() {
        DrainResult::Incomplete {
            unjoined_tasks,
            phases,
            escalated,
            ..
        } => {
            let phase_total: usize = phases.iter().map(|phase| phase.count()).sum();
            (*unjoined_tasks == 3 && phase_total == 3 && *escalated).then_some(*unjoined_tasks)
        }
        // A complete drain, or a future variant, reports nothing unjoined.
        _ => None,
    };
    assert_eq!(first_request, ShutdownRequest::Initiated);
    assert_eq!(second, ShutdownRequest::Escalated);
    assert_eq!(
        reported_unjoined,
        Some(3),
        "escalation did not report the tasks that remained"
    );

    report
        .point(serde_json::json!({
            "phase": "coordinator_drain",
            "owned_tasks": 12,
            "request_to_drain_complete_micros": clean_drain_micros,
            "unjoined_tasks": 0,
            "panicked_tasks": 0,
            "intake_closed": intake_closed,
        }))
        .point(serde_json::json!({
            "phase": "coordinator_escalation",
            "owned_tasks": 3,
            "request_to_escalated_report_micros": escalation_micros,
            "unjoined_tasks": reported_unjoined,
            "escalated": true,
        }))
        .correctness(
            "cancellation reached a worker before the durable terminal status",
            to_intake_stop <= to_durable_terminal,
        )
        .correctness(
            "the stopped attempt persisted a stopped status",
            stopped_durably,
        )
        .correctness("no worker outlived its stopped parent", joined_every_worker)
        .correctness(
            "the clean drain joined every owned child",
            joined_every_child,
        )
        .correctness("shutdown closed intake to new work", intake_closed)
        .correctness(
            "escalation reported every task that remained owned",
            reported_unjoined == Some(3),
        )
        .note(
            "The escalation point measures a deliberately unfinished drain: a second \
             request ends waiting, so the coordinator reports the remaining phases \
             instead of waiting out the one-second minimum join deadline.",
        );
    report.write()?;
    Ok(())
}

/// Telemetry overhead with export enabled and disabled.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn telemetry_export_overhead() -> Result<(), Box<dyn Error>> {
    const ITERATIONS: usize = 64;
    const PARTITIONS: u16 = 16;
    const WORKERS: u8 = 4;

    let mut report = Report::new(
        "TELEMETRY-OVERHEAD",
        "P-001 and P-010 with export enabled and disabled",
        "Export cost, queue depth, and dropped records beside identical durable results",
    );

    let mut quiet = Latencies::new();
    let mut instrumented = Latencies::new();
    let mut quiet_observation = None;
    let mut instrumented_observation = None;
    let sink = RecordingSink::new(
        ExportQueueBound::new(64)?,
        MetricCardinalityGuard::new(Vec::new(), Vec::new())?,
    );

    for iteration in 0..ITERATIONS {
        let observation = timed_partition_run(
            &format!("m4-telemetry-quiet-{iteration}"),
            PARTITIONS,
            WORKERS,
            None,
            &mut quiet,
        )
        .await?;
        quiet_observation.get_or_insert(observation);

        let observation = timed_partition_run(
            &format!("m4-telemetry-export-{iteration}"),
            PARTITIONS,
            WORKERS,
            Some(&sink),
            &mut instrumented,
        )
        .await?;
        instrumented_observation.get_or_insert(observation);
    }

    let queue_bounded = sink.peak_depth() <= 64 && sink.queue_len() <= 64;
    let drops_counted = sink.dropped() == sink.rejected();
    let identical = quiet_observation == instrumented_observation;
    assert!(
        queue_bounded,
        "the exporter queue exceeded its configured bound"
    );
    assert!(
        drops_counted,
        "the queue's dropped counter disagreed with its rejections"
    );
    assert!(
        identical,
        "telemetry changed the durable observation it is supposed to only describe"
    );

    let quiet_total = quiet.summary()["total_micros"].as_u64().unwrap_or_default();
    let instrumented_total = instrumented.summary()["total_micros"]
        .as_u64()
        .unwrap_or_default();
    report
        .point(serde_json::json!({
            "workload": "P-010 partitioned attempt",
            "iterations": ITERATIONS,
            "partitions": PARTITIONS,
            "workers": WORKERS,
            "export_disabled_latency": quiet.summary(),
            "export_enabled_latency": instrumented.summary(),
            "overhead_ratio": instrumented_total as f64 / quiet_total.max(1) as f64,
            "records_offered": sink.offered(),
            "records_queued": sink.accepted(),
            "records_dropped": sink.dropped(),
            "queue_bound_records": 64,
            "peak_queue_depth": sink.peak_depth(),
            "metric_series": sink.series(),
            "resident_kib": resident_kib(),
        }))
        .correctness(
            "the exporter queue never exceeded its configured bound",
            queue_bounded,
        )
        .correctness(
            "every rejected record was counted as dropped",
            drops_counted,
        )
        .correctness(
            "export enabled and disabled produced identical durable observations",
            identical,
        )
        .note(
            "The queue is deliberately configured at its 64-record minimum and never \
             drained, so this point measures drop-newest behavior under sustained \
             overflow rather than a steady-state exporter.",
        );

    // P-001: the same comparison over the fixed no-op tasklet lifecycle, where
    // framework overhead is the whole workload.
    let lifecycle_sink = RecordingSink::new(
        ExportQueueBound::new(1_024)?,
        MetricCardinalityGuard::new(Vec::new(), Vec::new())?,
    );
    let mut quiet_lifecycle = Latencies::new();
    let mut exported_lifecycle = Latencies::new();
    let mut lifecycle_statuses = Vec::new();
    for iteration in 0..ITERATIONS {
        lifecycle_statuses.push(
            timed_tasklet_run(
                &format!("m4-telemetry-noop-quiet-{iteration}"),
                None,
                &mut quiet_lifecycle,
            )
            .await?,
        );
        lifecycle_statuses.push(
            timed_tasklet_run(
                &format!("m4-telemetry-noop-export-{iteration}"),
                Some(&lifecycle_sink),
                &mut exported_lifecycle,
            )
            .await?,
        );
    }
    let every_attempt_completed = lifecycle_statuses
        .iter()
        .all(|status| *status == BatchStatus::Completed);
    let lifecycle_queue_bounded = lifecycle_sink.peak_depth() <= 1_024;
    assert!(
        every_attempt_completed,
        "a no-op lifecycle attempt did not complete"
    );
    assert!(
        lifecycle_queue_bounded,
        "the lifecycle exporter queue exceeded its configured bound"
    );

    let quiet_lifecycle_total = quiet_lifecycle.summary()["total_micros"]
        .as_u64()
        .unwrap_or_default();
    let exported_lifecycle_total = exported_lifecycle.summary()["total_micros"]
        .as_u64()
        .unwrap_or_default();
    report
        .point(serde_json::json!({
            "workload": "P-001 no-op tasklet lifecycle",
            "iterations": ITERATIONS,
            "export_disabled_latency": quiet_lifecycle.summary(),
            "export_enabled_latency": exported_lifecycle.summary(),
            "overhead_ratio":
                exported_lifecycle_total as f64 / quiet_lifecycle_total.max(1) as f64,
            "records_offered": lifecycle_sink.offered(),
            "records_queued": lifecycle_sink.accepted(),
            "records_dropped": lifecycle_sink.dropped(),
            "queue_bound_records": 1_024,
            "peak_queue_depth": lifecycle_sink.peak_depth(),
            "resident_kib": resident_kib(),
        }))
        .correctness(
            "every instrumented and quiet no-op lifecycle attempt completed",
            every_attempt_completed,
        )
        .correctness(
            "the lifecycle exporter queue never exceeded its configured bound",
            lifecycle_queue_bounded,
        )
        .note(
            "The reported overhead ratios are single-sample comparisons on a shared \
             host. A ratio near one means the export path was not resolvable above \
             run-to-run variance, not that it is free.",
        );
    report.write()?;
    Ok(())
}

/// Runs one no-op tasklet lifecycle and returns its durable status.
async fn timed_tasklet_run(
    name: &str,
    sink: Option<&RecordingSink>,
    latencies: &mut Latencies,
) -> Result<BatchStatus, Box<dyn Error>> {
    let name = JobName::new(name)?;
    let job = TaskletJob::new(
        name,
        TaskletStep::new(StepName::new("only")?, Arc::new(NoOpTasklet)),
        DefinitionRevision::new("v1")?,
        &ComponentRevision::new("noop-v1")?,
    )?;
    let (clock, ids, repository) = infrastructure();
    let (_source, stop) = StopSource::new();
    let runner = JobLauncher::new(&repository, clock.as_ref(), ids.as_ref());
    let runner = match sink {
        Some(sink) => runner.with_event_sink(sink),
        None => runner,
    };
    let started = Instant::now();
    let launched = runner.launch(&job, &JobParameters::new(), &stop).await?;
    latencies.record(started.elapsed());
    Ok(launched.job_execution().metadata().status())
}

/// P-015: repeated launch, drain, restart, and recovery cycles.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn p015_shutdown_restart_soak() -> Result<(), Box<dyn Error>> {
    const CYCLES: usize = 24;
    const PARTITIONS: u16 = 8;
    const WORKERS: u8 = 4;

    let mut report = Report::new(
        "P-015",
        "long-running soak",
        "Repeated launch, drain, restart, and recovery cycles with resource growth",
    );

    let mut first_units = None;
    let mut first_resident = None;
    let mut last_resident = None;
    let mut observation = None;
    let mut units_stable = true;
    let mut drains_complete = true;
    let mut observations_stable = true;
    let mut reuse_holds = true;
    let mut reused_partitions = 0;
    let mut cycles = Latencies::new();

    for cycle in 0..CYCLES {
        let started = Instant::now();
        let name = JobName::new(format!("m4-p015-{cycle}"))?;
        let occupancy = Arc::new(Occupancy::default());
        let invocations: Invocations = Arc::new(Mutex::new(BTreeMap::new()));
        let job = FlowJob::new(name.clone(), partitioned_plan(&name, PARTITIONS, WORKERS)?)?
            .with_partitioned_tasklet(
                NodeId::new("partitioned")?,
                partition_plan_factory(&partition_keys(PARTITIONS))?,
                flaky_factory(
                    Arc::clone(&occupancy),
                    Arc::clone(&invocations),
                    // The last partition starts only after earlier ones have
                    // committed, so the restart always has reuse to prove.
                    &partition_keys(PARTITIONS)[usize::from(PARTITIONS) - 1],
                )?,
            )?;
        let (clock, ids, repository) = infrastructure();
        let counting = CountingRepository::new(&repository);
        let (_source, stop) = StopSource::new();

        // The first attempt fails one partition, so the restart in this cycle
        // is a real recovery rather than a rejected relaunch of finished work.
        let failed = FlowLauncher::new(&counting, clock.as_ref(), ids.as_ref())
            .launch(&job, &JobParameters::new(), &stop)
            .await?;
        let after_failure = observe(&repository, &failed).await?;
        let before_restart = invocation_snapshot(&invocations);
        let launched = FlowLauncher::new(&counting, clock.as_ref(), ids.as_ref())
            .launch(&job, &JobParameters::new(), &stop)
            .await?;
        let after_restart = invocation_snapshot(&invocations);
        observations_stable &= matches!(failed.outcome(), FlowExecutionOutcome::Failed(_));
        observations_stable &= launched.outcome() == &FlowExecutionOutcome::Completed;

        // A partition that already committed must not run again, whatever the
        // failed attempt's sibling cancellation left pending.
        let completed_first: Vec<_> = after_failure
            .partitions
            .iter()
            .filter(|partition| partition.status == BatchStatus::Completed)
            .map(|partition| partition.key.clone())
            .collect();
        reuse_holds &= !completed_first.is_empty();
        for key in &completed_first {
            reuse_holds &= before_restart.get(key) == after_restart.get(key);
        }
        reused_partitions = completed_first.len();

        let mut coordinator = ShutdownCoordinator::new(
            ShutdownDeadline::new(Duration::from_secs(5))?,
            TaskJoinDeadline::new(
                Duration::from_secs(5),
                ShutdownDeadline::new(Duration::from_secs(5))?,
            )?,
            TelemetryFlushDeadline::new(Duration::from_millis(500))?,
        )?;
        for _ in 0..4 {
            coordinator.spawn(ShutdownTaskPhase::Tasklet, async {})?;
        }
        let drained = coordinator
            .shutdown(|| async { Ok(()) }, || async { Ok(0) }, || async { Ok(()) })
            .await;

        let current = observe(&repository, &launched).await?;
        drains_complete &= matches!(drained.drain(), DrainResult::Complete { panicked_tasks: 0 });
        observations_stable &= observation.as_ref().is_none_or(|first| first == &current);
        observation.get_or_insert(current);
        let units = counting.begins();
        units_stable &= first_units.is_none_or(|first| first == units);
        first_units.get_or_insert(units);
        let resident = resident_kib();
        first_resident = first_resident.or(resident);
        last_resident = resident;
        assert_eq!(occupancy.active(), 0, "cycle {cycle} left a worker owned");
        cycles.record(started.elapsed());
    }

    assert!(
        drains_complete,
        "a soak cycle failed to join every owned task"
    );
    assert!(
        units_stable,
        "the repository work per cycle was not constant"
    );
    assert!(
        observations_stable,
        "a soak cycle changed its durable observation"
    );
    assert!(
        reuse_holds,
        "a restart re-ran a partition that had already committed"
    );

    report
        .point(serde_json::json!({
            "cycles": CYCLES,
            "partitions_per_cycle": PARTITIONS,
            "workers": WORKERS,
            "repository_units_per_cycle": first_units,
            "partitions_reused_on_restart": reused_partitions,
            "cycle_latency": cycles.summary(),
            "resident_kib_first_cycle": first_resident,
            "resident_kib_last_cycle": last_resident,
            "resident_kib_growth": last_resident
                .zip(first_resident)
                .map(|(last, first)| i64::try_from(last).unwrap_or(i64::MAX)
                    - i64::try_from(first).unwrap_or(i64::MAX)),
        }))
        .correctness("every cycle joined every owned task", drains_complete)
        .correctness(
            "every cycle performed the same repository work",
            units_stable,
        )
        .correctness(
            "every cycle failed, restarted, and reached the same durable observation",
            observations_stable,
        )
        .correctness(
            "every restart re-ran only the partition that had failed",
            reuse_holds,
        )
        .note(
            "Each cycle builds its own repository, so resident growth across cycles \
             measures process-level retention rather than a growing history. A restart \
             inside every cycle proves completed partitions are reused rather than rerun.",
        );
    report.write()?;
    Ok(())
}

/// Runs one partitioned attempt, optionally instrumented, and times it.
async fn timed_partition_run(
    name: &str,
    partitions: u16,
    workers: u8,
    sink: Option<&RecordingSink>,
    latencies: &mut Latencies,
) -> Result<NormalizedObservation, Box<dyn Error>> {
    let name = JobName::new(name)?;
    let occupancy = Arc::new(Occupancy::default());
    let job = FlowJob::new(name.clone(), partitioned_plan(&name, partitions, workers)?)?
        .with_partitioned_tasklet(
            NodeId::new("partitioned")?,
            partition_plan_factory(&partition_keys(partitions))?,
            awaiting_factory(Arc::clone(&occupancy), Duration::from_micros(100))?,
        )?;
    let (clock, ids, repository) = infrastructure();
    let (_source, stop) = StopSource::new();
    let runner = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());
    let runner = match sink {
        Some(sink) => runner.with_event_sink(sink),
        None => runner,
    };
    let started = Instant::now();
    let launched = runner.launch(&job, &JobParameters::new(), &stop).await?;
    latencies.record(started.elapsed());
    let mut observation = observe(&repository, &launched).await?;
    // Instance identifiers are per-repository and therefore already equal, but
    // the job name differs between the quiet and instrumented runs, so compare
    // only the durable lifecycle result.
    observation.job_exit_status = launched.job_execution().metadata().exit_status().clone();
    Ok(observation)
}

/// Times one bounded tasklet launch used as a retention-impact probe.
async fn time_launch(
    repository: &InMemoryJobRepository,
    clock: &dyn Clock,
    ids: &dyn IdGenerator,
    discriminator: &str,
) -> Result<Duration, Box<dyn Error>> {
    let name = JobName::new("m4-retention-probe")?;
    let plan = partitioned_plan(&name, 1, 1)?;
    let occupancy = Arc::new(Occupancy::default());
    let job = FlowJob::new(name.clone(), plan)?.with_partitioned_tasklet(
        NodeId::new("partitioned")?,
        partition_plan_factory(&partition_keys(1))?,
        awaiting_factory(occupancy, Duration::ZERO)?,
    )?;
    let mut parameters = JobParameters::new();
    parameters.insert(
        ParameterName::new("run")?,
        JobParameter::new(
            ParameterValue::string(discriminator)?,
            ParameterRole::Identifying,
        ),
    )?;
    let (_source, stop) = StopSource::new();
    let started = Instant::now();
    FlowLauncher::new(repository, clock, ids)
        .launch(&job, &parameters, &stop)
        .await?;
    Ok(started.elapsed())
}

/// Creates `count` completed instances inside a bounded number of transactions.
async fn seed_terminal_instances(
    repository: &InMemoryJobRepository,
    clock: &dyn Clock,
    name: &JobName,
    count: usize,
) -> Result<(), Box<dyn Error>> {
    const BATCH: usize = 100;
    let mut created = 0;
    while created < count {
        let batch = BATCH.min(count - created);
        let mut unit = repository.begin().await?;
        for index in created..created + batch {
            let mut parameters = JobParameters::new();
            parameters.insert(
                ParameterName::new("run")?,
                JobParameter::new(
                    ParameterValue::string(format!("purge-{index:06}"))?,
                    ParameterRole::Identifying,
                ),
            )?;
            let key = JobInstanceKey::new(name.clone(), &parameters);
            let instance = unit.select_or_create_job_instance(&key).await?;
            let execution = unit.create_job_execution(instance.instance().id()).await?;
            let started = unit
                .transition_job_execution(
                    execution.id(),
                    execution.version(),
                    LifecycleTransition::new(BatchStatus::Started, clock.now()),
                )
                .await?;
            unit.transition_job_execution(
                started.id(),
                started.version(),
                LifecycleTransition::new(BatchStatus::Completed, clock.now()),
            )
            .await?;
        }
        unit.commit().await?;
        created += batch;
    }
    Ok(())
}

/// Creates `count` terminal instances inside a bounded number of transactions.
async fn seed_instances(
    repository: &InMemoryJobRepository,
    name: &JobName,
    count: usize,
) -> Result<(), Box<dyn Error>> {
    const BATCH: usize = 500;
    let mut created = 0;
    while created < count {
        let batch = BATCH.min(count - created);
        let mut unit = repository.begin().await?;
        for index in created..created + batch {
            let mut parameters = JobParameters::new();
            parameters.insert(
                ParameterName::new("run")?,
                JobParameter::new(
                    ParameterValue::string(format!("seed-{index:06}"))?,
                    ParameterRole::Identifying,
                ),
            )?;
            let key = JobInstanceKey::new(name.clone(), &parameters);
            let instance = unit.select_or_create_job_instance(&key).await?;
            unit.create_job_execution(instance.instance().id()).await?;
        }
        unit.commit().await?;
        created += batch;
    }
    Ok(())
}
