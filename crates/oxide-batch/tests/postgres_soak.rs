//! P-015 over repeated `PostgreSQL` launch, failure, restart, and drain cycles.
//!
//! This is the scenario the M5 design gate names for the soak campaign. Its
//! claim is a negative one, and a negative claim is only worth what its
//! denominator and its rules are worth, so both are read from
//! `tests/fixtures/soak/campaign-scope.json` rather than declared here.
//!
//! The claim is also not one claim. For the exact-count resources — owned
//! tasks, pooled connections, checkouts, and process handles — it is
//! non-accumulation: they are integers, and every measured cycle boundary held
//! them at the post-warmup baseline. For resident memory it is weaker, and is
//! stated as what the rule actually decides: growth converged under the
//! declared warmup-relative rate-decay rule. That is not a proof that no leak
//! exists, and the scenario name — which the design gate fixes and this
//! campaign may not rename — is broader than what the resident-memory half of
//! it establishes.
//! The cycle counts, the workload shape, the correctness obligations, and the
//! growth rules all come from that document, and `cargo xtask soak` requires
//! this report to have decided every one of them.
//!
//! ## What makes it an M5 result rather than a rerun of the M4 one
//!
//! The M4 measurement `p015_shutdown_restart_soak` already runs this shape of
//! cycle and fixed its semantics: every owned task joined, the same repository
//! work each cycle, the same durable observation each cycle, no re-run of a
//! committed partition. Those are kept here, unchanged. What it cannot supply
//! is production-preview evidence, because it builds a fresh in-memory
//! repository every cycle. That resets the two observations this campaign is
//! mostly about — pooled connections and process handles — at every cycle
//! boundary, and it takes its resident reading over a process holding no pool
//! at all. So the M4 report is the baseline and stays where it is; this one
//! opens one `PostgreSQL` pool before the first cycle and closes it after the
//! last, and every boundary sample is taken against that one pool.
//!
//! ## Why the fault waits instead of firing
//!
//! Every cycle has to leave the same durable record, or the comparison that
//! makes the resource numbers meaningful cannot be made. That is harder than it
//! sounds, because a sibling stop in the partitioned runtime is cooperative and
//! is only consulted *before* a worker's tasklet is invoked. A fault that fired
//! on a timer would stop a scheduling-dependent number of not-yet-started
//! siblings, so one cycle might commit fourteen partitions and the next
//! fifteen, and every durable comparison in the campaign would fail for a
//! reason that has nothing to do with the framework.
//!
//! The injected worker therefore waits until every sibling has returned before
//! it fails. The last partition key is the one that fails, and the budget is
//! smaller than the partition count, so that worker is the last to start and
//! every sibling is already in flight when it begins waiting — it cannot
//! deadlock against the budget it is holding a slot in. The result is exact:
//! the first attempt of every cycle commits `partitions - 1` and fails one, and
//! the restart re-runs exactly the one.
//!
//! ## The campaign must not be its own leak
//!
//! This report measures the resident memory of the process it runs in, which
//! makes its own bookkeeping part of the measurement. Collecting each cycle's
//! evidence into a vector and rendering it at the end — the obvious shape for a
//! report — retains a few kilobytes per cycle and draws a straight line through
//! the measured window that has nothing to do with the framework. It did, in
//! the first implementation of this report, at around thirteen kilobytes a
//! cycle.
//!
//! So the per-cycle evidence is written out through [`Journal`] as it is
//! produced and read back after the last sample. What stays resident is a
//! handful of integers per declared metric, in vectors reserved to their final
//! length before the measured window opens. The alternative — widening the
//! memory rule until the report's own growth fit underneath it — would have
//! weakened the rule for real accumulation as well.
//!
//! ## What the four resource observations are, and are not
//!
//! Tasks are read from the Tokio runtime, not from the framework, because a
//! count the framework kept would miss exactly the task that escaped it. The
//! framework's own `ShutdownCoordinator` accounting is read too, as each
//! cycle's drain result, but it answers the narrower question of whether the
//! tasks it owns were joined.
//!
//! Connections are read from the adapter's pool, and the database's own
//! `pg_stat_activity` count is recorded beside them without being confused for
//! them: a pool that has returned a connection and a server that has closed a
//! backend are different events at different times.
//!
//! Handles are read from the process and held to a level: no boundary sample
//! above the post-warmup baseline.
//!
//! Resident memory is held to *convergence* rather than to a level, and the
//! difference is the most important thing in this file to get right. The rule
//! reads one growth rate across the whole warmup window and one across the
//! whole measured window, and requires the second to be at most a quarter of
//! the first. Each rate is the window's last reading minus its first, over the
//! number of cycle intervals between them — intervals rather than samples,
//! because the two windows are different lengths and a sample-count denominator
//! would scale each of them by its own length. It compares a rate against a
//! rate, so it carries no unit: it is scale-free in the size of the process and
//! — the part that took a failed CI run to learn — in the page size of the
//! host. Accumulation and settling differ in their derivative, not their level.
//! A leak adds the same amount every cycle, so its rate is the same in both
//! windows and the ratio is exactly one whatever the per-cycle amount; an
//! allocator reaching a steady state against an unchanging transient pattern
//! has a rate that decays toward zero.
//!
//! `cargo xtask soak` decides that rule again from these samples with its own
//! arithmetic, which is only worth something if the two can disagree, so
//! neither imports the other. Both are held to the vectors in
//! `tests/fixtures/soak/rate-vectors.json`, because writing them separately is
//! what stops one from copying the other's bug and is not what stops them from
//! sharing a misreading of what the statistic is.
//!
//! What resident memory cannot do is carry the accumulation claim on its own. A
//! 1032-cycle run on the development host is flat for its last 800 samples, and
//! the same workload on glibc rises about a kilobyte a cycle with a decaying
//! rate. Both are this framework, so flatness is a property of the allocator.
//! The claim rests on the exact counters instead — tasks, pooled connections,
//! checkouts, and handles are integers required to be flat, not trends required
//! to decay — and resident memory is required only to converge.
//!
//! This report decides every rule and retains the series it decided from, so a
//! reader can check the decision rather than trust it.
//!
//! Durable history is recorded at every sample and is deliberately *not* under
//! a growth rule. The database is supposed to grow: every cycle commits an
//! instance, two executions, and a partition plan. Recording it beside the
//! process series is what stops a flat process series from being explained away
//! as a workload that stopped working.

#![cfg(feature = "postgres")]

#[path = "soak/mod.rs"]
mod soak;

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use oxide_batch::{
    BatchStatus, BoxFuture, ComponentRevision, DefinitionRevision, DrainResult, ExecutionContext,
    ExecutionCounts, ExitStatus, FlowGraph, FlowJob, FlowLaunchReport, FlowLauncher, FlowNode,
    FlowTarget, JobInstanceId, JobName, JobParameter, JobParameters, JobRepository, NodeId,
    ParameterName, ParameterRole, ParameterValue, PartitionBudget, PartitionCount, PartitionKey,
    PartitionPlanEntry, PartitionPlanFactory, PartitionTaskletFactory, PartitionedStepNode,
    PostgresConfig, PostgresJobRepository, PostgresMigrator, RepositoryDescriptor, RepositoryError,
    RepositoryUnitOfWork, SequentialIdGenerator, ShutdownCoordinator, ShutdownDeadline,
    ShutdownHookError, ShutdownReport, ShutdownTaskPhase, StateLimits, StepComponents, StepName,
    StepNode, StopSource, TaskJoinDeadline, Tasklet, TaskletContext, TaskletError, TaskletOutcome,
    TaskletStep, TelemetryFlushDeadline, TerminalKind,
};
use serde_json::{Map, Value, json};
use tokio::sync::Notify;

use soak::journal::Journal;
use soak::scope::{Rule, Scope};
use soak::{
    APPLICATION_NAME, Failure, FixedClock, History, Observer, Occupancy, PeakConnections,
    PoolGauge, alive_tasks, config, execution_manifest, major_version, migrator_url, open_handles,
    remove_job, resident_kib, retain_observation, runtime_url,
};

/// The report identifier the runner reconciles this observation under.
const REPORT: &str = "soak";

/// Tokio worker threads the campaign pins itself to.
///
/// Pinned rather than taken from the host, so a task count from one run is
/// comparable with the same run on another machine, and recorded in the report
/// for the same reason.
const WORKER_THREADS: usize = 4;

/// How long the injected worker waits for its siblings to return.
///
/// Bounded so that a run which cannot reach the fault fails on the durable
/// record it actually produced rather than hanging until the CI job's own
/// timeout, which would retain no report at all.
const FAULT_WAIT: Duration = Duration::from_mins(2);

/// How often the connection sampler reads the pool while a cycle runs.
const POOL_SAMPLE_INTERVAL: Duration = Duration::from_millis(5);

/// How long the server is given to finish tearing down the pool's backends.
///
/// Bounded rather than absent: the count is required to reach zero, and a run
/// where it does not must fail on the count it saw rather than wait forever.
const BACKEND_TEARDOWN: Duration = Duration::from_secs(30);

/// Deadlines every drain runs under.
const DRAIN_DEADLINE: Duration = Duration::from_secs(30);

/// Deadline the telemetry flush of every drain runs under.
const FLUSH_DEADLINE: Duration = Duration::from_millis(500);

#[test]
fn soak_reports_no_task_connection_handle_or_memory_growth() -> Result<(), Box<dyn Error>> {
    let Some(runtime) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let Some(migrator) = migrator_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL is not set");
        return Ok(());
    };

    let executor = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(WORKER_THREADS)
        .enable_all()
        .build()?;
    executor.block_on(report(runtime, migrator))
}

/// Runs the declared window and retains one observation.
#[allow(
    clippy::too_many_lines,
    reason = "the campaign is one ordered run — setup, warmup, measurement, final drain, \
              post-drain observation — and the order is part of what the evidence says"
)]
async fn report(runtime: String, migrator: String) -> Result<(), Box<dyn Error>> {
    let scope = Scope::read()?;
    let job_name = scope.workload.job_name.clone();

    PostgresMigrator::migrate(&config(migrator.clone(), 1)?).await?;
    remove_job(&migrator, &job_name).await?;

    let observer = Observer::connect(&runtime).await?;
    let server = observer.server_version().await?;

    // One pool for the whole campaign. A cycle that opened its own would reset
    // the two observations this report exists to take.
    let clock = FixedClock::default();
    let configuration = config(runtime.clone(), scope.workload.pool_size)?;
    let repository = PostgresJobRepository::connect(configuration.clone(), Arc::new(clock)).await?;
    let ids = SequentialIdGenerator::new(NonZeroU64::MIN);

    // Everything long-lived is in place before the first cycle, so the baseline
    // the growth rules are decided against contains all of it. The sampler is
    // part of that: it is started here and stopped after the last sample.
    let peak = Arc::new(PeakConnections::new());
    let running = Arc::new(AtomicBool::new(true));
    let readings = tokio::spawn(sample_pool(
        repository.clone(),
        Arc::clone(&peak),
        Arc::clone(&running),
    ));

    let mut cycle_journal = Journal::open("cycles")?;
    let mut sample_journal = Journal::open("samples")?;
    let mut series = Series::new(&scope);
    let mut correctness = Correctness::new();
    let mut totals = Totals::default();
    let mut history = observer.history(&job_name).await?;
    let started_at = Instant::now();

    let total = scope.window.warmup_cycles + scope.window.measured_cycles;
    for index in 0..total {
        let measured = index >= scope.window.warmup_cycles;
        let mut cycle = run_cycle(&repository, &clock, &ids, &scope, index).await?;

        tokio::time::sleep(Duration::from_millis(scope.window.settle_millis)).await;
        let after = observer.history(&job_name).await?;
        cycle.history_growth = after.since(history);
        history = after;

        let sample = take_sample(
            &repository,
            &observer,
            &peak,
            measured,
            started_at,
            &cycle,
            after,
        )
        .await?;

        totals.observe(&cycle);
        if measured {
            series.measured(&sample);
            correctness.observe(&scope, &cycle);
        } else {
            series.baseline(&sample);
        }

        // The evidence leaves the process here. Holding it would make the
        // report part of the measurement; see this file's own documentation.
        cycle_journal.append(&cycle.evidence())?;
        sample_journal.append(&sample)?;
    }

    // The sampler's measuring lifetime ends here, with the last boundary
    // sample, and it is joined before anything closes the pool. The ordering is
    // load-bearing rather than tidy: a sampler still running through
    // `close` reads a closed pool, and a gauge that cannot be read is counted
    // as an observation failure. Left that way, the campaign could not tell a
    // reading it lost during the workload — which invalidates every peak
    // occupancy in the report — from a reading it never should have taken.
    running.store(false, Ordering::SeqCst);
    let pool_readings = readings
        .await
        .map_err(|error| Failure::boxed(format!("the connection sampler did not join: {error}")))?;

    // Taken while the pool is still open, which is what makes it evidence. This
    // is the authoritative "nothing was still checked out" reading, and it is
    // required to be present: after the pool closes there is no occupancy left
    // to read, and an absent reading must not be able to pass as a zero.
    let pre_close = PoolGauge::read(&repository);

    // The final drain is the one that closes the pool. Every earlier drain left
    // it open, because closing it would have reset the observation.
    let closed = Arc::new(AtomicUsize::new(0));
    let mut coordinator = coordinator()?;
    let closing = repository.clone();
    let counter = Arc::clone(&closed);
    let final_drain = coordinator
        .shutdown(
            || async { Ok(()) },
            || async { Ok(0) },
            || async move {
                let result = closing.close().await;
                counter.fetch_add(1, Ordering::SeqCst);
                result.map_err(|_| ShutdownHookError)
            },
        )
        .await;

    tokio::time::sleep(Duration::from_millis(scope.window.settle_millis)).await;
    let (backends, settled_after) = observer.await_backends(0, BACKEND_TEARDOWN).await?;
    let post_drain = json!({
        "pre_close_pool": pre_close.map(|gauge| json!({
            "connections": gauge.connections,
            "idle": gauge.idle,
            "in_use": gauge.in_use(),
        })),
        "drain": describe_drain(final_drain.drain()),
        "repository_closed": closed.load(Ordering::SeqCst) == 1,
        // Read after the pool is gone, so the pool's own occupancy is
        // deliberately absent here rather than reported as zero. What a closed
        // pool leaves observable is the database's view of it and the process
        // state, and those are what this records.
        "post_close": {
            "alive_tasks": alive_tasks(),
            "open_handles": open_handles(),
            "resident_kib": resident_kib(),
            "database_backends": backends,
            "backends_settled_after_millis": settled_after,
        },
    });
    let elapsed = started_at.elapsed();

    // Every allocation from here on is outside the measured window.
    let samples = sample_journal.take()?;
    let cycles = cycle_journal.take()?;

    let mut violations = Vec::new();
    violations.extend(check_final_drain(
        &final_drain,
        closed.load(Ordering::SeqCst),
        pre_close,
    ));
    violations.extend(check_window(&scope, &totals, samples.len()));
    if peak.failures() != 0 {
        violations.push(format!(
            "the pool occupancy could not be read on {} of {pool_readings} sampler readings, so \
             the connection observation is incomplete",
            peak.failures(),
        ));
    }
    if pool_readings == 0 {
        violations.push(
            "the connection sampler took no reading, so every peak occupancy in this report is a \
             zero that means nothing was measured rather than nothing was held"
                .to_owned(),
        );
    }

    let correctness = correctness.finish(&scope);
    violations.extend(correctness.violations.clone());
    let growth = decide_growth(&scope, &series);
    violations.extend(growth.violations.clone());

    let document = json!({
        "report": REPORT,
        "scenario": "soak_reports_no_task_connection_handle_or_memory_growth",
        "workload": "P-015",
        "server_version": server,
        "postgres_major_version": major_version(&server),
        "environment": environment(&configuration, &scope),
        "execution_manifest": execution_manifest()?,
        "campaign": {
            "job_name": job_name,
            "warmup_cycles": scope.window.warmup_cycles,
            "measured_cycles": scope.window.measured_cycles,
            "completed_cycles": totals.cycles,
            "partitions_per_cycle": scope.workload.partitions_per_cycle,
            "worker_budget": scope.workload.worker_budget,
            "worker_work_millis": scope.workload.worker_work_millis,
            "launches_per_cycle": scope.workload.launches_per_cycle,
            "owned_tasks_per_drain": scope.workload.owned_tasks_per_drain,
            "failure_injection_point": "the last partition of the first attempt of every cycle",
            "faults_injected": totals.faults,
            "restarts": totals.restarts,
            "recoveries": totals.recoveries,
            "drains_completed": totals.drains,
            "partitions_executed": totals.partition_invocations,
            "settle_millis": scope.window.settle_millis,
            "sampling_interval_cycles": 1,
            "pool_readings": pool_readings,
            "elapsed_millis": elapsed.as_millis(),
        },
        "samples": samples,
        "cycles": cycles,
        "correctness": correctness.evidence(),
        "growth": growth.evidence(),
        "final_drain": post_drain,
        "violations": violations,
        "passed": violations.is_empty(),
    });
    retain_observation(REPORT, &document)?;

    observer.close().await;
    remove_job(&migrator, &job_name).await?;

    assert!(
        violations.is_empty(),
        "the soak report observed {violations:#?}",
    );
    Ok(())
}

/// Records the environment every number in this report depends on.
fn environment(configuration: &PostgresConfig, scope: &Scope) -> Value {
    json!({
        "source_commit": command("git", &["rev-parse", "HEAD"]),
        "source_tree_clean": command("git", &["status", "--porcelain"])
            .map(|status| status.is_empty()),
        "rustc": command("rustc", &["--version"]),
        "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
        "os": std::env::consts::OS,
        "kernel": command("uname", &["-sr"]),
        "arch": std::env::consts::ARCH,
        "available_parallelism": std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .ok(),
        "tokio_worker_threads": WORKER_THREADS,
        "application_name": APPLICATION_NAME,
        "pool": {
            "size": scope.workload.pool_size,
            "derivation": "concurrent_children + 1",
            // The retirement schedules are recorded because a connection the
            // pool closes on its own schedule is not a leak, and a reader
            // comparing two runs needs to know which schedule was in force.
            "configuration": format!("{configuration:?}"),
        },
        "handle_source": if cfg!(target_os = "linux") {
            "/proc/self/fd"
        } else {
            "/dev/fd"
        },
    })
}

/// Runs one environment-describing command, tolerating an absent tool.
fn command(program: &str, args: &[&str]) -> Option<String> {
    let output = std::process::Command::new(program)
        .args(args)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// Reads the pool's occupancy until the campaign stops it.
///
/// Returns how many readings it took, which the report records and requires to
/// be non-zero: a sampler that silently died would otherwise leave a peak of
/// zero looking like a pool that was never filled.
async fn sample_pool(
    repository: PostgresJobRepository,
    peak: Arc<PeakConnections>,
    running: Arc<AtomicBool>,
) -> u64 {
    let mut readings = 0;
    while running.load(Ordering::SeqCst) {
        peak.record(PoolGauge::read(&repository));
        readings += 1;
        tokio::time::sleep(POOL_SAMPLE_INTERVAL).await;
    }
    readings
}

/// Opens one drain's shutdown coordinator.
fn coordinator() -> Result<ShutdownCoordinator, Box<dyn Error>> {
    Ok(ShutdownCoordinator::new(
        ShutdownDeadline::new(DRAIN_DEADLINE)?,
        TaskJoinDeadline::new(DRAIN_DEADLINE, ShutdownDeadline::new(DRAIN_DEADLINE)?)?,
        TelemetryFlushDeadline::new(FLUSH_DEADLINE)?,
    )?)
}

/// What the whole run did, kept as counters rather than as retained cycles.
#[derive(Debug, Default)]
struct Totals {
    cycles: usize,
    faults: usize,
    restarts: usize,
    recoveries: usize,
    drains: usize,
    measured: usize,
    partition_invocations: usize,
}

impl Totals {
    /// Folds one cycle into the run's counters.
    fn observe(&mut self, cycle: &Cycle) {
        self.cycles += 1;
        self.faults += usize::from(cycle.failed_status == BatchStatus::Failed);
        self.restarts += 1;
        self.recoveries += usize::from(cycle.recovered);
        self.drains += usize::from(cycle.drain_complete);
        self.measured += usize::from(cycle.measured);
        self.partition_invocations += cycle.invocations.values().sum::<usize>();
    }
}

/// One cycle: a failed attempt, a restart, a drain, and what they left behind.
///
/// One of these is alive at a time. It is folded into the counters, the series,
/// and the correctness accumulator, rendered into the journal, and dropped.
#[allow(
    clippy::struct_excessive_bools,
    reason = "each flag is one independent thing the cycle observed, and folding them into a \
              state machine would say that the cycle's phase, its recovery, its drain, and its \
              fault wait are alternatives to each other rather than facts about the same run"
)]
struct Cycle {
    index: usize,
    measured: bool,
    elapsed: Duration,
    /// The durable record the terminal attempt left, normalized for comparison.
    record: DurableRecord,
    /// Partitions the failed attempt committed.
    committed_first: BTreeSet<String>,
    /// Partitions the restart invoked again.
    re_run: BTreeSet<String>,
    /// The partition the fault was injected into.
    injected: String,
    /// The status the failed attempt's durable job execution reached.
    failed_status: BatchStatus,
    /// How the failed attempt was classified.
    failed_outcome: String,
    /// Whether the restart created a new execution on the same instance.
    recovered: bool,
    /// Invocation count per partition key across both attempts.
    invocations: BTreeMap<String, usize>,
    /// Repository transactions the two launches began.
    transactions: usize,
    /// Greatest and residual worker occupancy.
    worker_peak: u64,
    worker_residue: u64,
    /// The cycle's drain.
    drain: DrainResult,
    drain_complete: bool,
    unjoined: u64,
    panicked: u64,
    /// How much the durable history grew across the cycle.
    history_growth: History,
    /// Whether the injected worker's wait for its siblings expired.
    fault_wait_expired: bool,
}

impl Cycle {
    /// Renders the cycle for the journal.
    fn evidence(&self) -> Value {
        json!({
            "cycle": self.index,
            "phase": self.phase(),
            "elapsed_millis": self.elapsed.as_millis(),
            "failed_attempt": {
                "outcome": self.failed_outcome,
                "durable_status": format!("{:?}", self.failed_status),
                "injected_partition": self.injected,
                "partitions_committed": self.committed_first.iter().collect::<Vec<_>>(),
                "fault_wait_expired": self.fault_wait_expired,
            },
            "restart": {
                "new_execution_on_same_instance": self.recovered,
                "partitions_re_run": self.re_run.iter().collect::<Vec<_>>(),
            },
            "terminal": self.record.evidence(),
            "invocations": self.invocations,
            "repository_transactions": self.transactions,
            "worker_peak_occupancy": self.worker_peak,
            "worker_residue": self.worker_residue,
            "drain": describe_drain(&self.drain),
            "durable_history_growth": {
                "instances": self.history_growth.instances,
                "executions": self.history_growth.executions,
                "step_executions": self.history_growth.step_executions,
                "partitions": self.history_growth.partitions,
            },
        })
    }

    /// Returns which window the cycle belongs to.
    const fn phase(&self) -> &'static str {
        if self.measured { "measured" } else { "warmup" }
    }
}

/// The durable record of one terminal attempt, as the comparison reads it.
///
/// Identifiers and the cycle's own parameter are deliberately absent: every
/// cycle runs a different instance, so a comparison that included them would
/// fail on every cycle and say nothing.
#[derive(Clone, Debug, Eq, PartialEq)]
struct DurableRecord {
    outcome: String,
    job_status: BatchStatus,
    job_exit_status: ExitStatus,
    parent_status: BatchStatus,
    parent_exit_status: ExitStatus,
    parent_counts: ExecutionCounts,
    step_executions: usize,
    partitions: BTreeMap<String, DurablePartition>,
}

impl DurableRecord {
    /// Renders the record for the journal.
    ///
    /// Every partition is written out by key with its own status, exit status
    /// and counters. A count and a set of distinct statuses would be smaller
    /// and would lose partition identity — and an independent recomputation of
    /// the per-partition obligations cannot be done from a set, which is the
    /// whole reason the journal exists.
    fn evidence(&self) -> Value {
        json!({
            "outcome": self.outcome,
            "job_status": format!("{:?}", self.job_status),
            "job_exit_status": format!("{:?}", self.job_exit_status),
            "parent_status": format!("{:?}", self.parent_status),
            "parent_exit_status": format!("{:?}", self.parent_exit_status),
            "parent_counts": counts(&self.parent_counts),
            "step_executions": self.step_executions,
            "partitions": self
                .partitions
                .iter()
                .map(|(key, partition)| {
                    (
                        key.clone(),
                        json!({
                            "status": format!("{:?}", partition.status),
                            "exit_status": format!("{:?}", partition.exit_status),
                            "counts": counts(&partition.counts),
                        }),
                    )
                })
                .collect::<serde_json::Map<_, _>>(),
        })
    }
}

/// One durable partition of a terminal attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
struct DurablePartition {
    status: BatchStatus,
    exit_status: ExitStatus,
    counts: ExecutionCounts,
}

/// Renders one execution counter set.
fn counts(counts: &ExecutionCounts) -> Value {
    json!({
        "read": counts.read(),
        "processed": counts.processed(),
        "written": counts.written(),
        "filtered": counts.filtered(),
        "committed": counts.committed(),
        "rolled_back": counts.rolled_back(),
    })
}

/// Renders one drain result.
fn describe_drain(drain: &DrainResult) -> Value {
    match drain {
        DrainResult::Complete { panicked_tasks } => json!({
            "result": "complete",
            "unjoined_tasks": 0,
            "panicked_tasks": panicked_tasks,
        }),
        DrainResult::Incomplete {
            unjoined_tasks,
            panicked_tasks,
            escalated,
            phases,
        } => json!({
            "result": "incomplete",
            "unjoined_tasks": unjoined_tasks,
            "panicked_tasks": panicked_tasks,
            "escalated": escalated,
            "phases": phases
                .iter()
                .map(|phase| json!({
                    "phase": format!("{:?}", phase.phase()),
                    "count": phase.count(),
                }))
                .collect::<Vec<_>>(),
        }),
        // The result is non-exhaustive, and a drain outcome this report cannot
        // name is not one it may call complete.
        other => json!({ "result": "unrecognized", "rendered": format!("{other:?}") }),
    }
}

/// Runs one whole cycle: launch, fault, restart, recovery, drain.
async fn run_cycle(
    repository: &PostgresJobRepository,
    clock: &FixedClock,
    ids: &SequentialIdGenerator,
    scope: &Scope,
    index: usize,
) -> Result<Cycle, Box<dyn Error>> {
    let started = Instant::now();
    let keys = partition_keys(scope.workload.partitions_per_cycle);
    let injected = keys
        .last()
        .cloned()
        .ok_or_else(|| Failure::boxed("the declared workload offers no partition"))?;

    let occupancy = Arc::new(Occupancy::new());
    let invocations: Arc<Mutex<BTreeMap<String, usize>>> = Arc::new(Mutex::new(BTreeMap::new()));
    let siblings = Arc::new(Siblings::default());
    let armed = Arc::new(AtomicBool::new(true));

    let job = build_job(
        scope,
        &keys,
        &injected,
        &occupancy,
        &invocations,
        &siblings,
        &armed,
    )?;
    let parameters = cycle_parameters(index)?;
    let counting = CountingRepository::new(repository);
    let (_source, stop) = StopSource::new();

    // The failed attempt.
    let failed = FlowLauncher::new(&counting, clock, ids)
        .launch(&job, &parameters, &stop)
        .await?;
    let failed_record = read_record(repository, &failed).await?;
    let committed_first = failed_record
        .partitions
        .iter()
        .filter(|(_, partition)| partition.status == BatchStatus::Completed)
        .map(|(key, _)| key.clone())
        .collect::<BTreeSet<_>>();
    let before_restart = snapshot(&invocations);

    // The restart along the accepted recovery path.
    let restarted = FlowLauncher::new(&counting, clock, ids)
        .launch(&job, &parameters, &stop)
        .await?;
    let record = read_record(repository, &restarted).await?;
    let recovered = restarted.instance().id() == failed.instance().id()
        && restarted.job_execution().id() != failed.job_execution().id();

    let worker_peak = occupancy.peak();
    let worker_residue = occupancy.active();
    let after_restart = snapshot(&invocations);
    let re_run = after_restart
        .iter()
        .filter(|(key, count)| before_restart.get(*key).copied().unwrap_or_default() < **count)
        .map(|(key, _)| key.clone())
        .collect::<BTreeSet<_>>();

    let drain = drain_cycle(repository, scope, restarted.instance().id()).await?;
    let (drain_complete, unjoined, panicked) = read_drain(&drain);

    Ok(Cycle {
        index,
        measured: index >= scope.window.warmup_cycles,
        elapsed: started.elapsed(),
        record,
        committed_first,
        re_run,
        injected,
        failed_status: failed.job_execution().metadata().status(),
        failed_outcome: format!("{:?}", failed.outcome()),
        recovered,
        invocations: after_restart,
        transactions: counting.begins(),
        worker_peak,
        worker_residue,
        drain,
        drain_complete,
        unjoined,
        panicked,
        history_growth: History::default(),
        fault_wait_expired: siblings.expired(),
    })
}

/// Drains one cycle's owned tasks and returns what the coordinator reported.
///
/// The tasks read through the same pool the cycle ran on, so a drain that left
/// a checkout behind shows up in the boundary sample as well as in the drain
/// result. The repository is deliberately not closed here: every cycle but the
/// last leaves the pool open, because closing it would reset the observation
/// the campaign is taking.
async fn drain_cycle(
    repository: &PostgresJobRepository,
    scope: &Scope,
    instance: JobInstanceId,
) -> Result<DrainResult, Box<dyn Error>> {
    let mut coordinator = coordinator()?;
    for slot in 0..scope.workload.owned_tasks_per_drain {
        let reader = repository.clone();
        let phase = if slot % 2 == 0 {
            ShutdownTaskPhase::Tasklet
        } else {
            ShutdownTaskPhase::Transaction
        };
        coordinator.spawn(phase, async move {
            if let Ok(mut unit) = reader.begin().await {
                let _ = unit.job_executions(instance).await;
                let _ = unit.rollback().await;
            }
        })?;
    }
    let drained = coordinator
        .shutdown(|| async { Ok(()) }, || async { Ok(0) }, || async { Ok(()) })
        .await;
    Ok(drained.drain().clone())
}

/// Reads one drain result as completeness, unjoined count, and panic count.
fn read_drain(drain: &DrainResult) -> (bool, u64, u64) {
    match drain {
        DrainResult::Complete { panicked_tasks } => {
            (*panicked_tasks == 0, 0, counted(*panicked_tasks))
        }
        DrainResult::Incomplete {
            unjoined_tasks,
            panicked_tasks,
            ..
        } => (false, counted(*unjoined_tasks), counted(*panicked_tasks)),
        // The result is non-exhaustive, and one this report cannot name is not
        // one it may call complete.
        _ => (false, 0, 0),
    }
}

/// Widens one count the coordinator reports.
fn counted(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// Takes one boundary sample of every observation the campaign declares.
async fn take_sample(
    repository: &PostgresJobRepository,
    observer: &Observer,
    peak: &PeakConnections,
    measured: bool,
    started_at: Instant,
    cycle: &Cycle,
    history: History,
) -> Result<Value, Box<dyn Error>> {
    let mut metrics = Map::new();
    metrics.insert("alive_tasks".into(), json!(alive_tasks()));
    metrics.insert("unjoined_tasks".into(), json!(cycle.unjoined));
    metrics.insert("panicked_tasks".into(), json!(cycle.panicked));

    // An unreadable gauge leaves the metric out rather than defaulting it. The
    // growth rules require every measured sample to carry the metric they are
    // decided from, so a connection observation that stopped being taken fails
    // the campaign instead of reading as a pool that held nothing.
    if let Some(gauge) = PoolGauge::read(repository) {
        metrics.insert("pool_connections".into(), json!(gauge.connections));
        metrics.insert("pool_idle_connections".into(), json!(gauge.idle));
        metrics.insert("pool_connections_in_use".into(), json!(gauge.in_use()));
    }
    metrics.insert("peak_connections_in_use".into(), json!(peak.take()));
    metrics.insert(
        "database_backends".into(),
        json!(observer.backends().await?),
    );

    if let Some(handles) = open_handles() {
        metrics.insert("open_handles".into(), json!(handles));
    }
    if let Some(resident) = resident_kib() {
        metrics.insert("resident_kib".into(), json!(resident));
    }

    metrics.insert("durable_job_instances".into(), json!(history.instances));
    metrics.insert("durable_job_executions".into(), json!(history.executions));
    metrics.insert(
        "durable_step_executions".into(),
        json!(history.step_executions),
    );
    metrics.insert("transactions_begun".into(), json!(cycle.transactions));

    Ok(json!({
        "cycle": cycle.index,
        "phase": if measured { "measured" } else { "warmup" },
        "elapsed_millis": started_at.elapsed().as_millis(),
        "cycle_millis": cycle.elapsed.as_millis(),
        "metrics": Value::Object(metrics),
    }))
}

/// The measured series of every metric a declared rule is decided from.
///
/// Only the metrics the rules name are kept, and each vector is reserved to its
/// final length before the measured window opens, so the resident cost of the
/// campaign's own bookkeeping is fixed rather than growing with the run.
struct Series {
    baseline: BTreeMap<String, i64>,
    /// The whole warmup series per metric, for rules decided against its rate.
    warmup: BTreeMap<String, Vec<i64>>,
    measured: BTreeMap<String, Vec<i64>>,
    /// Metrics a measured sample failed to carry, by metric.
    missing: BTreeMap<String, usize>,
    samples: usize,
}

impl Series {
    /// Opens the series a declared rule set needs.
    fn new(scope: &Scope) -> Self {
        let measured = scope
            .rules
            .iter()
            .map(|rule| {
                (
                    rule.metric.clone(),
                    Vec::with_capacity(scope.window.measured_cycles),
                )
            })
            .collect();
        Self {
            baseline: BTreeMap::new(),
            warmup: scope
                .rules
                .iter()
                .map(|rule| {
                    (
                        rule.metric.clone(),
                        Vec::with_capacity(scope.window.warmup_cycles),
                    )
                })
                .collect(),
            measured,
            missing: BTreeMap::new(),
            samples: 0,
        }
    }

    /// Records one warmup sample as the running baseline.
    ///
    /// The last warmup sample wins, which is what the campaign declares: the
    /// baseline is the first reading taken with the pool open, the arenas
    /// sized, and the runtime started.
    fn baseline(&mut self, sample: &Value) {
        for name in self.measured.keys().cloned().collect::<Vec<_>>() {
            if let Some(value) = metric(sample, &name) {
                self.baseline.insert(name.clone(), value);
                self.warmup.entry(name).or_default().push(value);
            }
        }
    }

    /// Records one measured sample.
    fn measured(&mut self, sample: &Value) {
        self.samples += 1;
        for (name, series) in &mut self.measured {
            match metric(sample, name) {
                Some(value) => series.push(value),
                None => *self.missing.entry(name.clone()).or_default() += 1,
            }
        }
    }
}

/// Reads one metric out of one sample.
fn metric(sample: &Value, name: &str) -> Option<i64> {
    sample
        .pointer(&format!("/metrics/{name}"))
        .and_then(Value::as_i64)
}

/// Requires the run to have covered the declared window.
fn check_window(scope: &Scope, totals: &Totals, samples: usize) -> Vec<String> {
    let mut violations = Vec::new();
    let warmup = totals.cycles.saturating_sub(totals.measured);

    if warmup != scope.window.warmup_cycles {
        violations.push(format!(
            "the campaign declares {} warmup cycles and {warmup} ran",
            scope.window.warmup_cycles,
        ));
    }
    if totals.measured != scope.window.measured_cycles {
        violations.push(format!(
            "the campaign declares {} measured cycles and {} ran",
            scope.window.measured_cycles, totals.measured,
        ));
    }
    if totals.measured < scope.window.minimum_measured_samples {
        violations.push(format!(
            "the campaign requires at least {} measured samples and took {}; a growth rule \
             decided from an empty or near-empty series is not evidence",
            scope.window.minimum_measured_samples, totals.measured,
        ));
    }
    if samples != totals.cycles {
        violations.push(format!(
            "{} cycles ran and {samples} samples were retained",
            totals.cycles,
        ));
    }
    violations
}

/// Requires the final drain to have joined everything and closed the pool.
fn check_final_drain(
    report: &ShutdownReport,
    closes: usize,
    pre_close: Option<PoolGauge>,
) -> Vec<String> {
    let mut violations = Vec::new();
    if !matches!(report.drain(), DrainResult::Complete { panicked_tasks: 0 }) {
        violations.push(format!(
            "the final drain did not join every owned task: {:?}",
            report.drain(),
        ));
    }
    if closes != 1 {
        violations.push(format!(
            "the final drain must close the repository exactly once and closed it {closes} times",
        ));
    }
    // An unreadable gauge is not a zero. The campaign's claim is that nothing
    // was still checked out when the pool closed, and a reading that was never
    // taken says nothing about that either way.
    match pre_close {
        Some(gauge) if gauge.in_use() == 0 => {}
        Some(gauge) => violations.push(format!(
            "{} connection(s) were still checked out when the pool closed",
            gauge.in_use(),
        )),
        None => violations.push(
            "the pool occupancy could not be read before the pool closed, so the campaign has no \
             reading that says nothing was still checked out"
                .to_owned(),
        ),
    }
    violations
}

/// Every per-cycle correctness obligation the scope declares, decided.
///
/// The obligations are accumulated one cycle at a time and against the first
/// measured cycle, so nothing but the baseline itself stays resident.
struct Correctness {
    baseline: Option<Baseline>,
    decided: BTreeMap<String, Vec<usize>>,
    violations: Vec<String>,
}

/// The first measured cycle, kept for every later cycle to be compared against.
struct Baseline {
    record: DurableRecord,
    transactions: usize,
    history_growth: History,
}

impl Correctness {
    /// Opens the accumulator.
    fn new() -> Self {
        Self {
            baseline: None,
            decided: BTreeMap::new(),
            violations: Vec::new(),
        }
    }

    /// Decides every obligation for one measured cycle.
    #[allow(
        clippy::too_many_lines,
        reason = "each declared obligation is one named decision, and naming them in one place \
                  is what lets the declared set be reconciled against the decided set"
    )]
    fn observe(&mut self, scope: &Scope, cycle: &Cycle) {
        let baseline = self.baseline.get_or_insert_with(|| Baseline {
            record: cycle.record.clone(),
            transactions: cycle.transactions,
            history_growth: cycle.history_growth,
        });
        let partitions = usize::from(scope.workload.partitions_per_cycle);
        let record = &cycle.record;
        let base = &baseline.record;
        let index = cycle.index;

        let mut check = |id: &str, holds: bool| {
            let offenders = self.decided.entry(id.to_owned()).or_default();
            if !holds {
                offenders.push(index);
            }
        };

        check(
            "final-job-status",
            record.job_status == base.job_status
                && record.job_exit_status == base.job_exit_status
                && record.outcome == base.outcome
                && record.job_status == BatchStatus::Completed,
        );
        check(
            "final-step-status",
            record.parent_status == base.parent_status
                && record.parent_exit_status == base.parent_exit_status
                && record.parent_status == BatchStatus::Completed,
        );
        // The individual counters are compared as well as the aggregate,
        // because an aggregate that matched while one counter drifted would be
        // the interesting failure and equality alone would not see it.
        check(
            "execution-counts",
            record.parent_counts == base.parent_counts
                && record.parent_counts.read() == base.parent_counts.read()
                && record.parent_counts.processed() == base.parent_counts.processed()
                && record.parent_counts.written() == base.parent_counts.written()
                && record.parent_counts.filtered() == base.parent_counts.filtered()
                && record.parent_counts.committed() == base.parent_counts.committed()
                && record.parent_counts.rolled_back() == base.parent_counts.rolled_back()
                && record.step_executions == base.step_executions,
        );
        check("partition-count", record.partitions.len() == partitions);
        check(
            "partition-key-set",
            record.partitions.keys().eq(base.partitions.keys()),
        );
        check(
            "partition-terminal-state",
            record.partitions.iter().all(|(key, partition)| {
                partition.status == BatchStatus::Completed
                    && base.partitions.get(key).is_some_and(|other| {
                        other.exit_status == partition.exit_status
                            && other.counts == partition.counts
                    })
            }),
        );

        // The failed attempt committed every partition but the injected one,
        // and the restart re-ran exactly the one it had not committed.
        check(
            "restart-position",
            cycle.committed_first.len() == partitions - 1
                && !cycle.committed_first.contains(&cycle.injected)
                && cycle.re_run.len() == 1
                && cycle.re_run.contains(&cycle.injected),
        );
        check(
            "committed-work-reused",
            !cycle.committed_first.is_empty()
                && cycle
                    .committed_first
                    .iter()
                    .all(|key| !cycle.re_run.contains(key)),
        );
        check(
            "no-duplicate-durable-work",
            record.partitions.len() == cycle.invocations.len()
                && cycle
                    .invocations
                    .iter()
                    .all(|(key, count)| *count == usize::from(*key == cycle.injected) + 1),
        );
        // The history is required to grow by the same amount every cycle
        // rather than by a formula written here, because the number of durable
        // rows a restart writes is the framework's business and a formula in
        // the campaign would be a second implementation of it. What the
        // campaign fixes is that it is the same every time and that the two
        // counts the lifecycle does determine are exact.
        check(
            "no-missing-durable-work",
            cycle.history_growth == baseline.history_growth
                && cycle.history_growth.instances == 1
                && cycle.history_growth.executions == 2
                && cycle.invocations.len() == partitions,
        );
        check(
            "failure-not-forged",
            cycle.failed_status == BatchStatus::Failed
                && cycle.failed_outcome.starts_with("Failed")
                && !cycle.fault_wait_expired,
        );
        check("recovery-semantics", cycle.recovered);
        check(
            "no-worker-outlives-its-parent",
            cycle.worker_residue == 0
                && cycle.worker_peak <= u64::from(scope.workload.worker_budget),
        );
        check("drain-complete", cycle.drain_complete);
        check(
            "constant-repository-work",
            cycle.transactions == baseline.transactions,
        );
    }

    /// Reconciles the decided obligations against the declared ones.
    fn finish(mut self, scope: &Scope) -> Self {
        if self.baseline.is_none() {
            self.violations.push(
                "the campaign completed no measured cycle, so there is no durable baseline to \
                 compare against"
                    .to_owned(),
            );
        }
        for (id, offenders) in &self.decided {
            if offenders.is_empty() {
                continue;
            }
            self.violations.push(format!(
                "{id} does not hold in cycle(s) {offenders:?}; a soak whose durable record \
                 changes is a failure whatever its resource trajectory was",
            ));
        }
        for declared in &scope.correctness {
            if !self.decided.contains_key(declared) {
                self.violations.push(format!(
                    "the campaign declares the {declared} obligation and this report decided \
                     nothing for it",
                ));
            }
        }
        for id in self.decided.keys() {
            if !scope.correctness.contains(id) {
                self.violations.push(format!(
                    "this report decided {id}, which the campaign scope does not declare",
                ));
            }
        }
        self
    }

    /// Renders the decided obligations for the retained record.
    fn evidence(&self) -> Value {
        json!({
            "baseline": "the first measured cycle",
            "checks": self
                .decided
                .iter()
                .map(|(id, offenders)| json!({
                    "id": id,
                    "holds": offenders.is_empty(),
                    "failing_cycles": offenders,
                }))
                .collect::<Vec<_>>(),
            "passed": self.violations.is_empty(),
            "violations": self.violations,
        })
    }
}

/// Every declared growth rule, decided from the measured series.
struct Growth {
    verdicts: Vec<Value>,
    violations: Vec<String>,
}

impl Growth {
    /// Renders the decided rules for the retained record.
    fn evidence(&self) -> Value {
        json!({
            "applies_to": "the measured window only",
            "rules": self.verdicts,
            "passed": self.violations.is_empty(),
            "violations": self.violations,
        })
    }
}

/// Decides every growth rule the scope declares.
fn decide_growth(scope: &Scope, series: &Series) -> Growth {
    let mut verdicts = Vec::new();
    let mut violations = Vec::new();

    for rule in &scope.rules {
        let measured = series.measured.get(&rule.metric);
        let missing = series
            .missing
            .get(&rule.metric)
            .copied()
            .unwrap_or_default();
        let Some(measured) = measured.filter(|values| !values.is_empty() && missing == 0) else {
            violations.push(format!(
                "the {} rule is decided from {}, which {missing} of {} measured samples did not \
                 carry, so the rule was not decided",
                rule.id, rule.metric, series.samples,
            ));
            verdicts.push(json!({
                "id": rule.id,
                "metric": rule.metric,
                "rule": rule.decides,
                "decided": false,
                "passed": false,
                "missing_samples": missing,
            }));
            continue;
        };

        let baseline = series.baseline.get(&rule.metric).copied();
        let decision = decide(
            rule,
            measured,
            baseline,
            i64::from(scope.workload.pool_size),
            series.warmup.get(&rule.metric).map(Vec::as_slice),
        );
        if !decision.passed {
            violations.push(decision.explanation.clone());
        }
        verdicts.push(json!({
            "id": rule.id,
            "metric": rule.metric,
            "rule": rule.decides,
            "decided": true,
            "passed": decision.passed,
            "baseline": baseline,
            "series": measured,
            "structure": structure(measured),
            "explanation": decision.explanation,
        }));
    }

    Growth {
        verdicts,
        violations,
    }
}

/// One rule's decision and the sentence that explains it.
struct Decision {
    passed: bool,
    explanation: String,
}

/// Applies one declared rule to one measured series.
fn decide(
    rule: &Rule,
    series: &[i64],
    baseline: Option<i64>,
    capacity: i64,
    warmup: Option<&[i64]>,
) -> Decision {
    match rule.decides.as_str() {
        "no-measured-sample-above-baseline" => {
            let Some(baseline) = baseline else {
                return Decision {
                    passed: false,
                    explanation: format!(
                        "{} is decided against the post-warmup baseline and no warmup sample \
                         carried {}",
                        rule.id, rule.metric,
                    ),
                };
            };
            let worst = series.iter().copied().max().unwrap_or(baseline);
            Decision {
                passed: worst <= baseline,
                explanation: format!(
                    "{} settled at {baseline} after warmup and the measured window reached {worst}",
                    rule.metric,
                ),
            }
        }
        "every-measured-sample-equals-zero" => {
            let offenders = series.iter().filter(|value| **value != 0).count();
            Decision {
                passed: offenders == 0,
                explanation: format!(
                    "{} was non-zero at {offenders} of {} measured boundaries",
                    rule.metric,
                    series.len(),
                ),
            }
        }
        "no-measured-sample-above-configured-capacity" => {
            let worst = series.iter().copied().max().unwrap_or_default();
            Decision {
                passed: worst <= capacity,
                explanation: format!(
                    "{} reached {worst} against a configured capacity of {capacity}",
                    rule.metric,
                ),
            }
        }
        "warmup-relative-rate-decay" => {
            let Some(decay) = rule.decay_percent else {
                return Decision {
                    passed: false,
                    explanation: format!(
                        "the {} rule is decided on a decay and the campaign declares none",
                        rule.id,
                    ),
                };
            };
            let Some(warmup) = warmup else {
                return Decision {
                    passed: false,
                    explanation: format!(
                        "the {} rule is decided against the warmup rate and no warmup series \
                         carried {}",
                        rule.id, rule.metric,
                    ),
                };
            };
            let (Some(early), Some(late)) = (rate(warmup), rate(series)) else {
                return Decision {
                    passed: false,
                    explanation: format!(
                        "the {} rule is decided from a rate per cycle, and a window of {} warmup \
                         and {} measured samples spans too few cycle intervals to have one",
                        rule.id,
                        warmup.len(),
                        series.len(),
                    ),
                };
            };
            // A warmup that did not grow leaves nothing to decay from, and
            // nothing is allowed above zero: a process flat through warmup and
            // rising through measurement is the failure this rule exists for.
            let passed = if early <= 0 {
                late <= 0
            } else {
                late.saturating_mul(100) <= early.saturating_mul(decay)
            };
            Decision {
                passed,
                explanation: format!(
                    "{} grew at {early} millionths of a KiB per cycle across warmup and {late} \
                     across the measured window, against a rule that the measured rate must be at \
                     most {decay}% of the warmup rate",
                    rule.metric,
                ),
            }
        }
        other => Decision {
            passed: false,
            explanation: format!(
                "the {} rule asks for {other}, which this report does not know how to decide",
                rule.id,
            ),
        },
    }
}

/// Summarizes one measured series so a reader can check the decision.
fn structure(series: &[i64]) -> Value {
    let first = series.first().copied().unwrap_or_default();
    let last = series.last().copied().unwrap_or_default();
    let mut highs = 0;
    let mut running = i64::MIN;
    for value in series {
        if *value > running {
            highs += 1;
            running = *value;
        }
    }
    let split = series.len() / 2;
    json!({
        "samples": series.len(),
        "first": first,
        "last": last,
        "min": series.iter().copied().min(),
        "max": series.iter().copied().max(),
        "delta": last - first,
        "new_highs": highs,
        "upward_steps": upward_steps(series),
        "strictly_increasing": highs == series.len(),
        "first_half_max": series[..split].iter().copied().max(),
        "second_half_min": series[split..].iter().copied().min(),
        "slope_per_cycle_micro": slope(series),
    })
}

/// Counts how often a series rises to a level it had not held before.
///
/// The first sample is not a step: every series starts somewhere. What this
/// counts is the number of times the running maximum moved afterwards, which is
/// the quantity that separates accumulation from settling — accumulation moves
/// it on nearly every sample whatever the per-sample amount, and settling moves
/// it a handful of times and then stops.
fn upward_steps(series: &[i64]) -> i64 {
    let mut steps = 0;
    let mut running = match series.first() {
        Some(first) => *first,
        None => return 0,
    };
    for value in &series[1..] {
        if *value > running {
            steps += 1;
            running = *value;
        }
    }
    steps
}

/// Returns the mean growth rate of a series, in millionths per cycle.
///
/// Endpoint-to-endpoint over the whole window, deliberately rather than a
/// least-squares fit. The resident series is page-quantised and bursty, and a
/// regression line inside a window is tilted by where a burst happens to fall;
/// the same work produced slopes differing by seven times across CI runs when
/// the rate was read from part of the window. A whole-window rate is unmoved by
/// a burst's position because a burst shifts both endpoints of the window it
/// lands in.
///
/// The denominator is the number of cycle intervals the window spans, which is
/// one fewer than the number of samples: `n` endpoints have `n - 1` gaps
/// between them. Dividing by the sample count instead understates every rate by
/// `(n - 1) / n`, and because warmup and measurement have different lengths the
/// understatement does not cancel in their ratio — a constant leak would come
/// out at `0.97` rather than the `1.00` the campaign's scope declares it must.
///
/// A window shorter than two samples spans no interval and has no rate. That is
/// returned as `None` rather than as zero, because zero is a real rate that
/// means "flat" and would be read as a passing warmup or a passing measurement.
fn rate(series: &[i64]) -> Option<i64> {
    let first = *series.first()?;
    let last = *series.last()?;
    let intervals = i64::try_from(series.len().checked_sub(1)?).unwrap_or(i64::MAX);
    if intervals < 1 {
        return None;
    }
    Some((last - first).saturating_mul(1_000_000) / intervals)
}

/// Returns the least-squares slope of a series, in millionths per cycle.
///
/// Recorded rather than asserted on. The campaign decides its rules from the
/// samples themselves, but a reader comparing two runs wants one number.
fn slope(series: &[i64]) -> i64 {
    let count = i64::try_from(series.len()).unwrap_or(i64::MAX);
    if count < 2 {
        return 0;
    }
    let mut positions = 0_i64;
    let mut values = 0_i64;
    let mut products = 0_i64;
    let mut squares = 0_i64;
    for (index, value) in series.iter().enumerate() {
        let position = i64::try_from(index).unwrap_or(i64::MAX);
        positions += position;
        values += value;
        products += position * value;
        squares += position * position;
    }
    let denominator = count * squares - positions * positions;
    if denominator == 0 {
        return 0;
    }
    (count * products - positions * values).saturating_mul(1_000_000) / denominator
}

/// Reads the durable record one launch left behind.
async fn read_record(
    repository: &PostgresJobRepository,
    report: &FlowLaunchReport,
) -> Result<DurableRecord, Box<dyn Error>> {
    let parent = report
        .step_executions()
        .last()
        .ok_or_else(|| Failure::boxed("the attempt recorded no parent step"))?;
    let mut unit = repository.begin().await?;
    let partitions = unit.step_partition_plan(parent.id()).await?;
    unit.rollback().await?;

    Ok(DurableRecord {
        outcome: format!("{:?}", report.outcome()),
        job_status: report.job_execution().metadata().status(),
        job_exit_status: report.job_execution().metadata().exit_status().clone(),
        parent_status: parent.metadata().status(),
        parent_exit_status: parent.metadata().exit_status().clone(),
        parent_counts: parent.metadata().counts(),
        step_executions: report.step_executions().len(),
        partitions: partitions
            .iter()
            .map(|partition| {
                (
                    partition.key().as_str().to_owned(),
                    DurablePartition {
                        status: partition.status(),
                        exit_status: partition.exit_status().clone(),
                        counts: partition.counts(),
                    },
                )
            })
            .collect(),
    })
}

/// Reads the current per-partition invocation counts.
fn snapshot(invocations: &Arc<Mutex<BTreeMap<String, usize>>>) -> BTreeMap<String, usize> {
    invocations
        .lock()
        .map(|counts| counts.clone())
        .unwrap_or_default()
}

/// Builds the identifying parameter that makes each cycle its own instance.
fn cycle_parameters(index: usize) -> Result<JobParameters, Box<dyn Error>> {
    let mut parameters = JobParameters::new();
    parameters.insert(
        ParameterName::new("cycle")?,
        JobParameter::new(
            ParameterValue::string(format!("{index:06}"))?,
            ParameterRole::Identifying,
        ),
    )?;
    Ok(parameters)
}

/// Returns the partition keys one cycle offers.
fn partition_keys(count: u16) -> Vec<String> {
    (0..count)
        .map(|index| format!("partition-{index:04}"))
        .collect()
}

/// Builds the partitioned job one cycle runs.
#[allow(
    clippy::too_many_arguments,
    reason = "one job wires one cycle's instrumentation"
)]
fn build_job(
    scope: &Scope,
    keys: &[String],
    injected: &str,
    occupancy: &Arc<Occupancy>,
    invocations: &Arc<Mutex<BTreeMap<String, usize>>>,
    siblings: &Arc<Siblings>,
    armed: &Arc<AtomicBool>,
) -> Result<FlowJob, Box<dyn Error>> {
    let name = JobName::new(&scope.workload.job_name)?;
    let manager = NodeId::new("partitioned")?;
    let worker_name = StepName::new("worker")?;

    let plan = FlowGraph::new(manager.clone())
        .with_node(FlowNode::partitioned_step(PartitionedStepNode::new(
            manager.clone(),
            StepName::new("partitioned")?,
            StepNode::new(
                NodeId::new("worker")?,
                worker_name.clone(),
                StepComponents::Tasklet(ComponentRevision::new("worker-v1")?),
            ),
            ComponentRevision::new("partitioner-v1")?,
            ComponentRevision::new("canonical-v1")?,
            PartitionCount::new(scope.workload.partitions_per_cycle)?,
            PartitionBudget::new(scope.workload.worker_budget, scope.workload.pool_size)?,
        )))
        .with_sequence(
            manager.clone(),
            FlowTarget::Terminal(TerminalKind::Complete),
        )?
        .compile(&name, DefinitionRevision::new("v1")?)?;

    let entries = keys
        .iter()
        .map(|key| entry(key))
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    let partitioner = PartitionPlanFactory::new(move |_request| Ok(entries.clone()));

    let factory_name = worker_name.clone();
    let injected = injected.to_owned();
    let expected_siblings = keys.len().saturating_sub(1);
    let work = Duration::from_millis(scope.workload.worker_work_millis);
    let occupancy = Arc::clone(occupancy);
    let invocations = Arc::clone(invocations);
    let siblings = Arc::clone(siblings);
    let armed = Arc::clone(armed);
    let factory = PartitionTaskletFactory::new(worker_name, move |input| {
        let key = input.key().as_str().to_owned();
        TaskletStep::new(
            factory_name.clone(),
            Arc::new(SoakWorker {
                occupancy: Arc::clone(&occupancy),
                invocations: Arc::clone(&invocations),
                siblings: Arc::clone(&siblings),
                armed: Arc::clone(&armed),
                injects: key == injected,
                expected_siblings,
                work,
                key,
            }),
        )
    });

    Ok(FlowJob::new(name, plan)?.with_partitioned_tasklet(manager, partitioner, factory)?)
}

/// Builds one partition plan entry.
fn entry(key: &str) -> Result<PartitionPlanEntry, Box<dyn Error>> {
    let context = ExecutionContext::from_json(
        format!(
            "{{\"format\":\"oxide-batch.execution-context\",\"format_version\":1,\
             \"schema\":\"m5.soak\",\"schema_version\":1,\
             \"payload\":{{\"key\":\"{key}\"}}}}"
        )
        .as_bytes(),
        StateLimits::new(4 * 1024, 16)?,
    )?;
    Ok(PartitionPlanEntry::new(PartitionKey::new(key)?, context)?)
}

/// How many sibling workers have returned, and a way to wait for the rest.
///
/// This is what makes the injected failure deterministic instead of timed. See
/// this file's own documentation for why a timed fault would give every cycle a
/// different durable record.
#[derive(Debug, Default)]
struct Siblings {
    returned: AtomicUsize,
    expired: AtomicBool,
    notify: Notify,
}

impl Siblings {
    /// Records one sibling returning successfully.
    fn returned(&self) {
        self.returned.fetch_add(1, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Waits until `target` siblings have returned, or the deadline expires.
    async fn wait_for(&self, target: usize) {
        let waited = tokio::time::timeout(FAULT_WAIT, async {
            loop {
                // The waiter is registered before the count is read, so a
                // sibling that returns between the two still wakes it.
                let notified = self.notify.notified();
                if self.returned.load(Ordering::SeqCst) >= target {
                    return;
                }
                notified.await;
            }
        })
        .await;
        if waited.is_err() {
            self.expired.store(true, Ordering::SeqCst);
        }
    }

    /// Returns whether any wait expired.
    fn expired(&self) -> bool {
        self.expired.load(Ordering::SeqCst)
    }
}

/// One cycle's partition worker.
struct SoakWorker {
    occupancy: Arc<Occupancy>,
    invocations: Arc<Mutex<BTreeMap<String, usize>>>,
    siblings: Arc<Siblings>,
    armed: Arc<AtomicBool>,
    injects: bool,
    expected_siblings: usize,
    work: Duration,
    key: String,
}

impl Tasklet for SoakWorker {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            self.occupancy.enter();
            if let Ok(mut invocations) = self.invocations.lock() {
                *invocations.entry(self.key.clone()).or_default() += 1;
            }

            // The injected worker fires once per cycle, on the first attempt.
            // Disarming before the wait means the restart's re-run of the same
            // key completes rather than waiting for siblings that will not run.
            let fails = self.injects && self.armed.swap(false, Ordering::SeqCst);
            if fails {
                self.siblings.wait_for(self.expected_siblings).await;
                self.occupancy.leave();
                return Err(TaskletError::new());
            }

            // A bounded await as the work, so several workers are inside the
            // worker set at once and the occupancy the cycle reaches is a real
            // observation rather than an artifact of instantaneous bodies.
            tokio::time::sleep(self.work).await;
            self.occupancy.leave();
            if !self.injects {
                self.siblings.returned();
            }
            Ok(TaskletOutcome::Completed)
        })
    }
}

/// A repository decorator that counts the transactions the framework begins.
///
/// It counts calls and delegates everything else. The number it produces is the
/// campaign's "same work every cycle" check: a cycle that quietly did less
/// durable work would flatten every resource series in the report, and this is
/// what makes that visible rather than reassuring.
struct CountingRepository<'a> {
    inner: &'a PostgresJobRepository,
    begins: AtomicUsize,
}

impl<'a> CountingRepository<'a> {
    /// Wraps one repository.
    const fn new(inner: &'a PostgresJobRepository) -> Self {
        Self {
            inner,
            begins: AtomicUsize::new(0),
        }
    }

    /// Returns how many transactions have been begun through this decorator.
    fn begins(&self) -> usize {
        self.begins.load(Ordering::SeqCst)
    }
}

impl JobRepository for CountingRepository<'_> {
    fn connection_capacity(&self) -> u32 {
        self.inner.connection_capacity()
    }

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

/// The declared rate vectors, applied to this report's own arithmetic.
///
/// The campaign's memory verdict is recomputed by `cargo xtask soak` from the
/// same samples, and that recomputation is only worth something if the two
/// implementations can disagree. They are therefore written separately and
/// share no helper. What they do share is
/// `tests/fixtures/soak/rate-vectors.json`: a defect in one implementation
/// fails on that side alone, and a defect in the shared understanding of what
/// the statistic is fails on both, which is the case a private fixture on each
/// side would have missed.
#[cfg(test)]
mod rate_vectors {
    #![allow(clippy::expect_used, clippy::panic)]

    use std::fs;

    use serde_json::Value;

    use super::soak::scope::Rule;
    use super::soak::workspace_root;
    use super::{decide, rate};

    /// Reads the declared vector document.
    fn document() -> Value {
        let path = workspace_root()
            .join("tests")
            .join("fixtures")
            .join("soak")
            .join("rate-vectors.json");
        let text = fs::read_to_string(&path).expect("the declared rate vectors are committed");
        serde_json::from_str(&text).expect("the declared rate vectors parse")
    }

    /// Reads one series field as a vector of readings.
    fn series(vector: &Value, name: &str) -> Vec<i64> {
        vector[name]
            .as_array()
            .expect("the vector declares the series")
            .iter()
            .map(|value| value.as_i64().expect("a reading is an integer"))
            .collect()
    }

    /// The rule under test, at the decay the vectors declare.
    fn rule(decay: i64) -> Rule {
        Rule {
            id: "resident-memory-converges".to_owned(),
            metric: "resident_kib".to_owned(),
            decides: "warmup-relative-rate-decay".to_owned(),
            decay_percent: Some(decay),
        }
    }

    #[test]
    fn declared_rates_match_this_implementation() {
        let document = document();
        for vector in document["vectors"].as_array().expect("vectors") {
            let id = vector["id"].as_str().expect("the vector is named");
            for name in ["warmup", "measured"] {
                let declared = vector[format!("{name}_rate_micro")].as_i64();
                let computed = rate(&series(vector, name));
                assert_eq!(
                    computed, declared,
                    "{id}: the {name} window's declared rate is {declared:?} and this \
                     implementation computes {computed:?}",
                );
            }
        }
    }

    #[test]
    fn declared_verdicts_match_this_implementation() {
        let document = document();
        let decay = document["decay_percent"].as_i64().expect("decay percent");
        for vector in document["vectors"].as_array().expect("vectors") {
            let id = vector["id"].as_str().expect("the vector is named");
            let expected = vector["passes"].as_bool().expect("the verdict is declared");
            let warmup = series(vector, "warmup");
            let decision = decide(
                &rule(decay),
                &series(vector, "measured"),
                None,
                0,
                Some(&warmup),
            );
            assert_eq!(
                decision.passed, expected,
                "{id}: the declared verdict is {expected} and this implementation decided \
                 {} — {}",
                decision.passed, decision.explanation,
            );
        }
    }

    #[test]
    fn a_constant_leak_rates_the_same_in_windows_of_any_length() {
        let document = document();
        let decay = document["decay_percent"].as_i64().expect("decay percent");
        let cases = document["constant_growth_property"]["cases"]
            .as_array()
            .expect("cases");
        for case in cases {
            let slope = case["slope"].as_i64().expect("slope");
            let build = |samples: u64, origin: i64| -> Vec<i64> {
                (0..samples)
                    .map(|index| origin + slope * i64::try_from(index).expect("index fits"))
                    .collect()
            };
            let warmup = build(case["warmup_samples"].as_u64().expect("warmup"), 1_000);
            let measured = build(case["measured_samples"].as_u64().expect("measured"), 7_777);

            // The property the rule is stated in terms of: a leak adds the same
            // amount every cycle, so its rate does not depend on how long the
            // window watching it happens to be. Only an interval denominator
            // gives that; dividing by the sample count makes the rate a
            // function of the window length and the ratio drift away from one.
            let (early, late) = (rate(&warmup), rate(&measured));
            assert_eq!(
                early,
                Some(slope * 1_000_000),
                "a constant rise of {slope} over {} samples",
                warmup.len(),
            );
            assert_eq!(
                early, late,
                "the same constant rise in windows of different length"
            );

            let decision = decide(&rule(decay), &measured, None, 0, Some(&warmup));
            assert!(
                !decision.passed,
                "a constant leak must fail at {decay}% — {}",
                decision.explanation,
            );
        }
    }
}
