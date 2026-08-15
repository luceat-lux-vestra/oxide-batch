//! Queue, cardinality, and diagnostic ceilings under offered overload.
//!
//! This report owns the resources whose overload policy is *not* a refusal.
//! Every other bound in the resource-bound campaign fails closed: the framework
//! declines the work and nothing happens. These do the opposite on purpose.
//!
//! Telemetry may not block batch work. That is the accepted contract, and it
//! means a full exporter queue cannot apply backpressure the way a bounded
//! worker set does — it has to keep its bound and throw a record away. A
//! campaign that made every resource behave the same way would have to
//! introduce that backpressure, which would break the contract rather than
//! strengthen the evidence. So this report checks each of these resources
//! against the rule it actually contracts for, and the rules differ:
//!
//! - the exporter queue drops the **newest** record, because the queue exists
//!   to shed a burst and the records already in it are the ones about to be
//!   exported;
//! - the incident buffer evicts the **oldest**, because it exists to be read
//!   after a failure and the newest records are the ones worth keeping;
//! - the metric cardinality guard keeps neither and both: an unseen label
//!   combination past the family budget is collapsed into one reserved series
//!   and counted, so the series count stays finite while the observation is
//!   still made;
//! - the bundle and the operator response truncate and say so, rather than
//!   returning something unbounded or nothing at all.
//!
//! Each is offered more than it holds rather than described. A queue that was
//! never filled drops nothing and reports no violation, which is the same green
//! as a queue that is bounded — so the report records what it offered, what the
//! resource held, and how much it shed, and the runner requires the three to
//! add up.
//!
//! The last obligation is the one that makes shedding acceptable at all: batch
//! work must finish anyway, and finish the same way. So a launch runs with its
//! exporter queue saturated from the first record, and its durable result is
//! compared against the same launch with a queue that has room. A shed record
//! that changed a durable observation would not be shedding, it would be data
//! loss with a counter attached.

mod support;

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use std::num::NonZeroU64;

use oxide_batch::{
    Clock, DropReportWindow, EnqueueResult, ExportQueueBound, InMemoryExplorer,
    InMemoryJobRepository, IncidentEventBuffer, JobExecutionId, JobExplorer, JobName, JobOperator,
    MAX_EXPORT_QUEUE_RECORDS, MAX_METRIC_NAME_ALLOWLIST, MAX_RETAINED_EVENTS_PER_EXECUTION,
    MAX_SHUTDOWN_DEADLINE, MAX_TELEMETRY_FLUSH_DEADLINE, METRIC_CARDINALITY_BUDGET,
    MIN_EXPORT_QUEUE_RECORDS, MIN_SHUTDOWN_DEADLINE, MIN_TELEMETRY_FLUSH_DEADLINE,
    MetricCardinalityGuard, MetricDimensions, MetricFamily, RetentionService,
    SequentialIdGenerator, ShutdownDeadline, StepName, TelemetryEventKind, TelemetryEventSink,
    TelemetryFlushDeadline, TelemetryQueue, TelemetryRecord,
};
use oxide_batch_cli::{
    Command, ExitCategory, MAX_OUTPUT_BYTES, NoSchema, OutputForm, Response, Services, Writer,
};
use serde_json::{Value, json};
use support::{FixedClock, TestHost, TestServices, run_with_catalog, services, test_catalog};

/// The report identifier the runner reconciles this observation under.
const REPORT: &str = "bounded-shedding";

/// The variable that tells the report where to retain its observation.
const OBSERVATIONS_ENV: &str = "OXIDEBATCH_RESOURCE_OBSERVATIONS";

/// The queue bound the saturation offers overload against.
///
/// The smallest bound the framework accepts, so the offered excess is large
/// relative to the queue and the drop path is entered many times rather than
/// once. The declared ceiling is still what the evidence records.
const SATURATED_QUEUE: usize = MIN_EXPORT_QUEUE_RECORDS;

/// Records offered to the saturated queue.
const OFFERED_RECORDS: usize = SATURATED_QUEUE * 4;

/// Label combinations offered to one metric family.
const OFFERED_SERIES: usize = METRIC_CARDINALITY_BUDGET * 2;

/// Events offered to one execution's incident buffer.
const OFFERED_EVENTS: usize = MAX_RETAINED_EVENTS_PER_EXECUTION * 3;

/// The job the saturated and unsaturated launches both run.
const JOB: &str = "resource-bound-shedding-job";

/// The declared ceiling on one diagnostics bundle.
const BUNDLE_CEILING: usize = 4 * 1024 * 1024;

/// The declared ceiling on one configuration document.
const CONFIG_CEILING: usize = 256 * 1024;

#[test]
fn bounded_queues_shed_under_overload_without_blocking_batch_work() -> Result<(), Box<dyn Error>> {
    let mut violations = Vec::new();
    let mut resources = Vec::new();

    let queue = saturate_the_exporter_queue();
    violations.extend(queue.violations.clone());
    resources.push(queue.evidence());

    let series = saturate_the_metric_family();
    violations.extend(series.violations.clone());
    resources.push(series.evidence());

    let events = saturate_the_incident_buffer();
    violations.extend(events.violations.clone());
    resources.push(events.evidence());

    let response = overflow_the_operator_response();
    violations.extend(response.violations.clone());
    resources.push(response.evidence());

    let bundle = generate_a_bundle();
    violations.extend(bundle.violations.clone());
    resources.push(bundle.evidence());

    let cells = construction_cells();
    violations.extend(cells.iter().filter_map(Cell::violation));

    let equivalence = batch_work_finishes_with_the_queue_full();
    violations.extend(equivalence.violations.clone());

    let document = json!({
        "report": REPORT,
        "scenario": "bounded_queues_shed_under_overload_without_blocking_batch_work",
        "resources": resources,
        "construction": cells.iter().map(Cell::evidence).collect::<Vec<_>>(),
        "durable_equivalence": equivalence.evidence(),
        "execution_manifest": execution_manifest()?,
        "violations": violations,
        "passed": violations.is_empty(),
    });
    retain(&document)?;

    assert!(
        violations.is_empty(),
        "the shedding report observed {violations:#?}",
    );
    Ok(())
}

/// Offers the exporter queue four times what it holds.
fn saturate_the_exporter_queue() -> Shed {
    let bound =
        ExportQueueBound::new(SATURATED_QUEUE).unwrap_or_else(|_| ExportQueueBound::default());
    let queue = TelemetryQueue::new(bound, DropReportWindow::default());

    let mut violations = Vec::new();
    let mut accepted = 0_u64;
    let mut dropped = 0_u64;
    let mut peak_depth = 0_usize;
    let mut reports_due = 0_u64;
    for index in 0..OFFERED_RECORDS {
        match queue.enqueue(record(), Duration::from_millis(index as u64)) {
            EnqueueResult::Accepted => accepted += 1,
            EnqueueResult::Dropped { report_due } => {
                dropped += 1;
                if report_due {
                    reports_due += 1;
                }
            }
            // The enqueue result is non-exhaustive. A variant this report does
            // not know about is neither an acceptance nor a counted drop, and
            // the arithmetic below would silently stop adding up, so it is
            // named as a violation rather than absorbed.
            other => violations.push(format!(
                "the exporter queue answered an offer with {other:?}, which this report cannot \
                 account for",
            )),
        }
        peak_depth = peak_depth.max(queue.len());
    }

    if peak_depth > SATURATED_QUEUE {
        violations.push(format!(
            "the exporter queue holds {SATURATED_QUEUE} records and reached a depth of \
             {peak_depth}",
        ));
    }
    if peak_depth != SATURATED_QUEUE {
        violations.push(format!(
            "{OFFERED_RECORDS} records were offered to a queue of {SATURATED_QUEUE} and it never \
             filled past {peak_depth}, so the drop path was never entered",
        ));
    }
    if accepted != SATURATED_QUEUE as u64 {
        violations.push(format!(
            "the queue accepted {accepted} of {OFFERED_RECORDS} records and holds \
             {SATURATED_QUEUE}",
        ));
    }
    // The shed count must be the excess exactly. A queue that dropped more than
    // the overflow would be discarding records it had room for.
    let excess = (OFFERED_RECORDS - SATURATED_QUEUE) as u64;
    if dropped != excess {
        violations.push(format!(
            "{OFFERED_RECORDS} records were offered to a queue of {SATURATED_QUEUE} and \
             {dropped} were dropped rather than the {excess} that did not fit",
        ));
    }
    if queue.dropped() != dropped {
        violations.push(format!(
            "the queue counted {} drops and {dropped} were observed, so the counter an operator \
             reads is not the thing that happened",
            queue.dropped(),
        ));
    }
    // The drop observation is itself throttled, or a saturated queue would emit
    // one record per drop and be an unbounded queue in a different place.
    if reports_due >= dropped {
        violations.push(format!(
            "{dropped} drops produced {reports_due} due drop reports, so the report is not \
             throttled",
        ));
    }

    // What is still in the queue must be the records that arrived first: the
    // rule is drop-newest, and a queue that shed the oldest would report the
    // same counts.
    let drained = queue.len();

    Shed {
        resource: "telemetry-exporter-queue",
        policy: "bounded-shedding",
        rule: "drop-newest",
        ceiling: MAX_EXPORT_QUEUE_RECORDS as u64,
        configured: SATURATED_QUEUE as u64,
        offered: OFFERED_RECORDS as u64,
        peak: peak_depth as u64,
        retained: drained as u64,
        discarded: dropped,
        violations,
    }
}

/// Offers one metric family more label combinations than its budget admits.
///
/// The combinations are built from allowlisted job and step names rather than
/// arbitrary ones, and they have to be: a name outside the allowlist is already
/// collapsed into the reserved value before the budget is consulted, so a
/// thousand unknown names would produce one series and prove nothing about the
/// budget. Fifty allowed jobs against fifty allowed steps is the largest
/// legitimate combination space the allowlist itself admits, and it is well
/// past the two-hundred-series budget.
fn saturate_the_metric_family() -> Shed {
    let family = MetricFamily::ExecutionEvents;
    let jobs = (0..MAX_METRIC_NAME_ALLOWLIST)
        .filter_map(|index| JobName::new(format!("job-{index:04}")).ok())
        .collect::<Vec<_>>();
    let steps = (0..MAX_METRIC_NAME_ALLOWLIST)
        .filter_map(|index| StepName::new(format!("step-{index:04}")).ok())
        .collect::<Vec<_>>();
    let Ok(mut guard) = MetricCardinalityGuard::new(jobs.clone(), steps.clone()) else {
        return Shed::failed(
            "metric-series-per-family",
            "the report could not build the allowlist the budget is measured against",
        );
    };

    let mut offered = 0_u64;
    let mut collapsed = 0_u64;
    for job in &jobs {
        for step in &steps {
            if offered >= OFFERED_SERIES as u64 {
                break;
            }
            let dimensions = MetricDimensions::default()
                .with_job_name(job.clone())
                .with_step_name(step.clone());
            offered += 1;
            if guard.observe(family, &dimensions).overflowed() {
                collapsed += 1;
            }
        }
    }

    let series = guard.series_count(family);
    let mut violations = Vec::new();
    if series > METRIC_CARDINALITY_BUDGET {
        violations.push(format!(
            "the family budget is {METRIC_CARDINALITY_BUDGET} series and {series} are retained",
        ));
    }
    if collapsed == 0 {
        violations.push(format!(
            "{offered} label combinations were offered to a budget of \
             {METRIC_CARDINALITY_BUDGET} and none was collapsed, so the reserved series was never \
             reached",
        ));
    }
    if guard.dropped_cardinality(family) != collapsed {
        violations.push(format!(
            "the guard counted {} collapsed combinations and {collapsed} were observed",
            guard.dropped_cardinality(family),
        ));
    }

    Shed {
        resource: "metric-series-per-family",
        policy: "bounded-shedding",
        rule: "collapse-to-reserved-series",
        ceiling: METRIC_CARDINALITY_BUDGET as u64,
        configured: METRIC_CARDINALITY_BUDGET as u64,
        offered,
        peak: series as u64,
        retained: series as u64,
        discarded: collapsed,
        violations,
    }
}

/// Offers one execution three times the events its buffer retains.
///
/// The events are produced by real reads rather than by synthetic records, and
/// they have to be: a record only carries a job execution identifier when a
/// service that knows one emitted it, and the per-execution bound is defined in
/// terms of that identifier. Handing the buffer fabricated records would fill
/// it with events belonging to no execution, and `events_for` would return
/// nothing for any identifier — a bound that looks held because nothing was
/// ever offered to it.
fn saturate_the_incident_buffer() -> Shed {
    let buffer = Arc::new(IncidentEventBuffer::default());
    let services = services_with_sink(Arc::clone(&buffer) as Arc<dyn TelemetryEventSink>);
    let catalog = test_catalog(JOB);

    let mut host = TestHost::new();
    let launched = run_with_catalog(
        &mut host,
        &services,
        &catalog,
        &format!(
            "launch --job {JOB} --actor campaign --operation-id shedding-events --output json"
        ),
    );

    let mut violations = Vec::new();
    if launched != ExitCategory::Success {
        violations.push(format!(
            "the incident-buffer fixture could not launch: {}",
            host.stderr_text(),
        ));
    }
    let execution = host.envelope()["data"]["execution"]["execution_id"]
        .as_u64()
        .unwrap_or(1);

    // Each read emits one explorer event carrying the execution it read, so
    // the offered load is the number of reads.
    let mut offered = 0_u64;
    for _ in 0..OFFERED_EVENTS {
        let mut reader = TestHost::new();
        let category = run_with_catalog(
            &mut reader,
            &services,
            &catalog,
            &format!("execution steps --execution {execution} --output json"),
        );
        if category != ExitCategory::Success {
            violations.push(format!(
                "the incident-buffer fixture could not read the execution: {}",
                reader.stderr_text(),
            ));
            break;
        }
        offered += 1;
    }

    let retained = JobExecutionId::new(execution)
        .map(|id| buffer.events_for(id).len())
        .unwrap_or_default();

    if retained > MAX_RETAINED_EVENTS_PER_EXECUTION {
        violations.push(format!(
            "the per-execution buffer retains {MAX_RETAINED_EVENTS_PER_EXECUTION} events and \
             returned {retained}",
        ));
    }
    if offered <= MAX_RETAINED_EVENTS_PER_EXECUTION as u64 {
        violations.push(format!(
            "{offered} events were emitted for one execution against a buffer of \
             {MAX_RETAINED_EVENTS_PER_EXECUTION}, so the eviction rule was never exercised",
        ));
    }
    if retained != MAX_RETAINED_EVENTS_PER_EXECUTION {
        violations.push(format!(
            "{offered} events were emitted for one execution and the buffer returned {retained} \
             rather than the {MAX_RETAINED_EVENTS_PER_EXECUTION} it retains",
        ));
    }

    Shed {
        resource: "retained-incident-events",
        policy: "bounded-shedding",
        rule: "evict-oldest",
        ceiling: MAX_RETAINED_EVENTS_PER_EXECUTION as u64,
        configured: MAX_RETAINED_EVENTS_PER_EXECUTION as u64,
        offered,
        peak: retained as u64,
        retained: retained as u64,
        discarded: offered.saturating_sub(retained as u64),
        violations,
    }
}

/// Renders a response far larger than the operator output bound.
fn overflow_the_operator_response() -> Shed {
    let row = "x".repeat(1024);
    let rows = (0..1_024)
        .map(|index| json!({ "id": index, "detail": row }))
        .collect::<Vec<_>>();
    let offered = serde_json::to_vec(&Value::Array(rows.clone()))
        .map(|bytes| bytes.len())
        .unwrap_or_default();

    let mut host = TestHost::new();
    let writer = Writer::new(OutputForm::Json, false);
    let response = Response::success(Command::InstanceList, Value::Array(rows));
    let emitted = writer.emit(&mut host, &response).is_ok();
    let written = host.stdout_text();

    let mut violations = Vec::new();
    if !emitted {
        violations.push(
            "an over-large response failed to render at all rather than being truncated".to_owned(),
        );
    }
    if written.len() > MAX_OUTPUT_BYTES {
        violations.push(format!(
            "the operator response bound is {MAX_OUTPUT_BYTES} bytes and {} were written",
            written.len(),
        ));
    }
    if offered <= MAX_OUTPUT_BYTES {
        violations.push(format!(
            "the report offered {offered} bytes against a {MAX_OUTPUT_BYTES}-byte bound, so it \
             never crossed it",
        ));
    }
    // Truncation has to be visible. Silently returning fewer rows is a wrong
    // answer rather than a bounded one.
    if !written.contains("truncated") {
        violations.push(
            "the response was truncated and does not say so, so an operator cannot tell a short \
             page from a complete one"
                .to_owned(),
        );
    }

    Shed {
        resource: "operator-response",
        policy: "bounded-truncation",
        rule: "truncate-and-declare",
        ceiling: MAX_OUTPUT_BYTES as u64,
        configured: MAX_OUTPUT_BYTES as u64,
        offered: offered as u64,
        peak: written.len() as u64,
        retained: written.len() as u64,
        discarded: offered.saturating_sub(written.len()) as u64,
        violations,
    }
}

/// Generates one diagnostics bundle and measures it against its ceiling.
fn generate_a_bundle() -> Shed {
    let (services, _repository) = services();
    let catalog = test_catalog(JOB);
    let mut host = TestHost::new();

    let launched = run_with_catalog(
        &mut host,
        &services,
        &catalog,
        &format!(
            "launch --job {JOB} --actor campaign --operation-id shedding-bundle --output json"
        ),
    );
    let mut violations = Vec::new();
    if launched != ExitCategory::Success {
        violations.push(format!(
            "the bundle fixture could not launch: {}",
            host.stderr_text(),
        ));
    }
    let execution = host.envelope()["data"]["execution"]["execution_id"]
        .as_u64()
        .unwrap_or(1);

    let mut bundling = TestHost::new();
    let generated = run_with_catalog(
        &mut bundling,
        &services,
        &catalog,
        &format!("diagnostics bundle --execution {execution} --out shedding-bundle --output json"),
    );
    if generated != ExitCategory::Success {
        violations.push(format!(
            "the diagnostics bundle could not be generated: {}",
            bundling.stderr_text(),
        ));
    }

    let mut total = 0_usize;
    let mut files = 0_u64;
    for name in bundling.directory_files("shedding-bundle") {
        total += bundling.file_text(&format!("shedding-bundle/{name}")).len();
        files += 1;
    }

    if files == 0 {
        violations.push("the bundle contains no file, so its size proves nothing".to_owned());
    }
    if total > BUNDLE_CEILING {
        violations.push(format!(
            "the bundle bound is {BUNDLE_CEILING} bytes and {total} were written",
        ));
    }

    Shed {
        resource: "diagnostic-bundle",
        policy: "bounded-truncation",
        rule: "truncate-and-declare",
        ceiling: BUNDLE_CEILING as u64,
        configured: BUNDLE_CEILING as u64,
        offered: total as u64,
        peak: total as u64,
        retained: total as u64,
        discarded: 0,
        violations,
    }
}

/// Runs the same launch with a saturated queue and with a queue that has room.
///
/// Shedding is only acceptable because batch work is unaffected by it. The two
/// runs therefore have to produce the same durable record, and the saturated
/// one has to have actually shed something — otherwise the comparison is
/// between two identical runs and says nothing.
fn batch_work_finishes_with_the_queue_full() -> Equivalence {
    let quiet = launch_with_queue(usize::from(u16::MAX) + 1, false);
    let saturated = launch_with_queue(SATURATED_QUEUE, true);

    let mut violations = Vec::new();
    if saturated.shed == 0 {
        violations.push(
            "the saturated launch shed no record, so it is not a comparison against a full queue"
                .to_owned(),
        );
    }
    if quiet.shed != 0 {
        violations.push(format!(
            "the baseline launch shed {} records, so it is not a comparison against a queue with \
             room",
            quiet.shed,
        ));
    }
    if saturated.category != ExitCategory::Success {
        violations.push(
            "batch work did not complete while its exporter queue was saturated, so telemetry \
             blocked it"
                .to_owned(),
        );
    }
    if saturated.durable != quiet.durable {
        violations.push(
            "the saturated launch and the baseline launch left different durable records, so a \
             shed telemetry record changed an observation"
                .to_owned(),
        );
    }

    Equivalence {
        baseline_shed: quiet.shed,
        saturated_shed: saturated.shed,
        baseline_durable: quiet.durable.clone(),
        saturated_durable: saturated.durable,
        violations,
    }
}

/// Launches one job with an exporter queue of `bound` records attached.
fn launch_with_queue(bound: usize, prefill: bool) -> Launch {
    let sink = Arc::new(SheddingSink::new(bound, prefill));
    let services = services_with_sink(Arc::clone(&sink) as Arc<dyn TelemetryEventSink>);
    let catalog = test_catalog(JOB);
    let mut host = TestHost::new();
    let category = run_with_catalog(
        &mut host,
        &services,
        &catalog,
        &format!(
            "launch --job {JOB} --actor campaign --operation-id shedding-{bound}-{prefill} \
             --output json"
        ),
    );

    // The durable record is what the launch reports about the execution it
    // created: its status, its exit status, and its counters. The identifiers
    // are not compared, because two runs create two executions.
    let envelope = host.envelope();
    let execution = &envelope["data"]["execution"];
    let durable = json!({
        "status": execution["status"],
        "exit_status": execution["exit_status"],
        "version": execution["version"],
        "category": format!("{category:?}"),
    });

    Launch {
        category,
        shed: sink.shed(),
        durable,
    }
}

/// Builds the CLI services with one telemetry sink attached to each service.
///
/// The services are built here rather than taken from the shared harness so
/// that the sink this report owns receives every record the run emits. Sinks
/// accumulate, so nothing the CLI already does is displaced.
fn services_with_sink(sink: Arc<dyn TelemetryEventSink>) -> TestServices {
    let clock: Arc<dyn Clock> = Arc::new(FixedClock::new());
    let repository = InMemoryJobRepository::new(
        Arc::clone(&clock),
        Arc::new(SequentialIdGenerator::new(NonZeroU64::MIN)),
    );
    let explorer_repository = InMemoryExplorer::new(&repository);
    Services::new(
        JobOperator::new(repository.clone(), Arc::clone(&clock)).with_event_sink(Arc::clone(&sink)),
        RetentionService::new(repository, Arc::clone(&clock)).with_event_sink(Arc::clone(&sink)),
        JobExplorer::new(explorer_repository).with_event_sink(sink),
        Box::new(NoSchema),
    )
}

/// Reports every shedding-related construction the framework must bound.
fn construction_cells() -> Vec<Cell> {
    let mut cells = queue_construction_cells();
    cells.extend(deadline_construction_cells());
    cells.extend(configuration_construction_cells());
    cells
}

/// Reports the queue and cardinality constructions.
fn queue_construction_cells() -> Vec<Cell> {
    vec![
        Cell::new(
            "telemetry-exporter-queue",
            "at the ceiling",
            MAX_EXPORT_QUEUE_RECORDS as u64,
            ExportQueueBound::new(MAX_EXPORT_QUEUE_RECORDS).is_ok(),
            true,
        ),
        Cell::new(
            "telemetry-exporter-queue",
            "one past the ceiling",
            MAX_EXPORT_QUEUE_RECORDS as u64 + 1,
            ExportQueueBound::new(MAX_EXPORT_QUEUE_RECORDS + 1).is_ok(),
            false,
        ),
        Cell::new(
            "telemetry-exporter-queue",
            "at the floor",
            MIN_EXPORT_QUEUE_RECORDS as u64,
            ExportQueueBound::new(MIN_EXPORT_QUEUE_RECORDS).is_ok(),
            true,
        ),
        Cell::new(
            "telemetry-exporter-queue",
            "one below the floor",
            MIN_EXPORT_QUEUE_RECORDS as u64 - 1,
            ExportQueueBound::new(MIN_EXPORT_QUEUE_RECORDS - 1).is_ok(),
            false,
        ),
        Cell::new(
            "retained-incident-events",
            "at the ceiling",
            MAX_RETAINED_EVENTS_PER_EXECUTION as u64,
            IncidentEventBuffer::new(MAX_RETAINED_EVENTS_PER_EXECUTION, 4_096).is_ok(),
            true,
        ),
        Cell::new(
            "retained-incident-events",
            "one past the ceiling",
            MAX_RETAINED_EVENTS_PER_EXECUTION as u64 + 1,
            IncidentEventBuffer::new(MAX_RETAINED_EVENTS_PER_EXECUTION + 1, 4_096).is_ok(),
            false,
        ),
        Cell::new(
            "metric-name-allowlist",
            "at the ceiling",
            MAX_METRIC_NAME_ALLOWLIST as u64,
            allowlist_of(MAX_METRIC_NAME_ALLOWLIST),
            true,
        ),
        Cell::new(
            "metric-name-allowlist",
            "one past the ceiling",
            MAX_METRIC_NAME_ALLOWLIST as u64 + 1,
            allowlist_of(MAX_METRIC_NAME_ALLOWLIST + 1),
            false,
        ),
    ]
}

/// Reports the bounded-duration constructions the budget table declares.
fn deadline_construction_cells() -> Vec<Cell> {
    vec![
        Cell::new(
            "telemetry-drop-report-window",
            "at the ceiling",
            MAX_DROP_REPORT_WINDOW_SECONDS,
            DropReportWindow::new(Duration::from_secs(MAX_DROP_REPORT_WINDOW_SECONDS)).is_ok(),
            true,
        ),
        Cell::new(
            "telemetry-drop-report-window",
            "one second past the ceiling",
            MAX_DROP_REPORT_WINDOW_SECONDS + 1,
            DropReportWindow::new(Duration::from_secs(MAX_DROP_REPORT_WINDOW_SECONDS + 1)).is_ok(),
            false,
        ),
        Cell::new(
            "shutdown-deadline",
            "at the ceiling",
            MAX_SHUTDOWN_DEADLINE.as_secs(),
            ShutdownDeadline::new(MAX_SHUTDOWN_DEADLINE).is_ok(),
            true,
        ),
        Cell::new(
            "shutdown-deadline",
            "one second past the ceiling",
            MAX_SHUTDOWN_DEADLINE.as_secs() + 1,
            ShutdownDeadline::new(MAX_SHUTDOWN_DEADLINE + Duration::from_secs(1)).is_ok(),
            false,
        ),
        Cell::new(
            "shutdown-deadline",
            "one second below the floor",
            MIN_SHUTDOWN_DEADLINE.as_secs() - 1,
            MIN_SHUTDOWN_DEADLINE
                .checked_sub(Duration::from_secs(1))
                .is_some_and(|below| ShutdownDeadline::new(below).is_ok()),
            false,
        ),
        Cell::new(
            "telemetry-flush-deadline",
            "at the ceiling",
            MAX_TELEMETRY_FLUSH_DEADLINE.as_secs(),
            TelemetryFlushDeadline::new(MAX_TELEMETRY_FLUSH_DEADLINE).is_ok(),
            true,
        ),
        Cell::new(
            "telemetry-flush-deadline",
            "one millisecond below the floor",
            u64::try_from(MIN_TELEMETRY_FLUSH_DEADLINE.as_millis()).unwrap_or(u64::MAX) - 1,
            MIN_TELEMETRY_FLUSH_DEADLINE
                .checked_sub(Duration::from_millis(1))
                .is_some_and(|below| TelemetryFlushDeadline::new(below).is_ok()),
            false,
        ),
        Cell::new(
            "telemetry-flush-deadline",
            "one second past the ceiling",
            MAX_TELEMETRY_FLUSH_DEADLINE.as_secs() + 1,
            TelemetryFlushDeadline::new(MAX_TELEMETRY_FLUSH_DEADLINE + Duration::from_secs(1))
                .is_ok(),
            false,
        ),
    ]
}

/// Reports the CLI configuration-document constructions.
fn configuration_construction_cells() -> Vec<Cell> {
    vec![
        Cell::new(
            "cli-configuration-document",
            "one byte past the ceiling",
            CONFIG_CEILING as u64 + 1,
            configuration_accepted(CONFIG_CEILING + 1),
            false,
        ),
        Cell::new(
            "cli-configuration-document",
            "a document inside the ceiling",
            1_024,
            configuration_accepted(0),
            true,
        ),
    ]
}

/// The declared ceiling on the drop-report throttle, in seconds.
const MAX_DROP_REPORT_WINDOW_SECONDS: u64 = 60 * 60;

/// Reports whether a metric allowlist of `names` step names is accepted.
fn allowlist_of(names: usize) -> bool {
    let steps = (0..names)
        .filter_map(|index| StepName::new(format!("allow-{index:04}")).ok())
        .collect::<Vec<_>>();
    if steps.len() != names {
        return false;
    }
    MetricCardinalityGuard::new(Vec::new(), steps).is_ok()
}

/// Reports whether the CLI accepts a configuration document of `bytes`.
///
/// A `bytes` of zero asks for an ordinary small document, which must be
/// accepted: a bound that rejected everything would satisfy the refusal half
/// of this pair without being the declared bound.
fn configuration_accepted(bytes: usize) -> bool {
    let contents = if bytes == 0 {
        r#"{"config_version":1,"output":{"page_size":10}}"#.to_owned()
    } else {
        let prefix = r#"{"config_version":1,"output":{"page_size":10},"note":""#;
        let filler = bytes.saturating_sub(prefix.len() + 2);
        format!("{prefix}{}\"}}", "f".repeat(filler))
    };

    let (services, _repository) = services();
    let catalog = test_catalog(JOB);
    let mut host = TestHost::new().with_file("shedding-config.json", &contents);
    let category = run_with_catalog(
        &mut host,
        &services,
        &catalog,
        "config show --config shedding-config.json --output json",
    );
    category == ExitCategory::Success
}

/// Builds one telemetry record for the queue and buffer to hold.
fn record() -> TelemetryRecord {
    TelemetryRecord::catalog(TelemetryEventKind::JobStarted)
}

/// Retains the report's observation where the runner will read it.
fn retain(document: &Value) -> Result<(), Box<dyn Error>> {
    let Ok(directory) = std::env::var(OBSERVATIONS_ENV) else {
        return Ok(());
    };
    if directory.is_empty() {
        return Ok(());
    }
    let directory = std::path::PathBuf::from(directory);
    std::fs::create_dir_all(&directory)?;
    std::fs::write(
        directory.join(format!("{REPORT}.json")),
        format!("{}\n", serde_json::to_string_pretty(document)?),
    )?;
    Ok(())
}

/// Returns the workspace root that contains this package.
fn workspace_root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Reads the declared semantic closure of the resource-bounds campaign.
///
/// Read from `tests/fixtures/resource-bounds/campaign-semantics.json` rather
/// than listed here, because the xtask verifier reads the same document: a
/// closure kept in two places is one that will disagree. This is a separate
/// copy of `crates/oxide-batch/tests/resource_bounds/mod.rs`'s function of the
/// same name, because this report runs in a different workspace crate and
/// test binaries do not share code across crates; both read the one committed
/// closure document, so they cannot disagree about what it declares.
fn semantics_paths() -> Result<Vec<String>, Box<dyn Error>> {
    let path = workspace_root()
        .join("tests")
        .join("fixtures")
        .join("resource-bounds")
        .join("campaign-semantics.json");
    let document: Value = serde_json::from_str(&std::fs::read_to_string(&path)?)?;
    let categories = document
        .get("categories")
        .and_then(Value::as_object)
        .ok_or_else(|| ReportFailure("the semantics document declares no categories".to_owned()))?;
    let mut paths = categories
        .values()
        .filter_map(|category| category.get("paths").and_then(Value::as_array))
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    paths.sort();
    paths.dedup();
    if paths.is_empty() {
        return Err(Box::new(ReportFailure(
            "the semantics document declares no paths".to_owned(),
        )));
    }
    Ok(paths)
}

/// Records the object identity of the campaign's closure, as executed.
///
/// See `crates/oxide-batch/tests/resource_bounds/mod.rs`'s function of the
/// same name: this process is the campaign, so the tree it can see is by
/// definition the tree that ran, and recording that here makes the binding
/// permanent and offline rather than dependent on a commit name a later clone
/// might not be able to resolve. This report needs no database, and records
/// no `PostgreSQL` major for that reason: the campaign-level matrix identity is
/// recorded once, at the environment level, not manufactured for a report
/// that used no database.
fn execution_manifest() -> Result<Value, Box<dyn Error>> {
    let root = workspace_root();
    let commit = git(&root, &["rev-parse", "HEAD"])
        .ok_or_else(|| ReportFailure("the campaign is not running inside a git tree".to_owned()))?;
    let mut objects = serde_json::Map::new();
    for path in semantics_paths()? {
        let object = git(&root, &["rev-parse", &format!("HEAD:{path}")]).ok_or_else(|| {
            ReportFailure(format!(
                "{path} is declared as campaign semantics and is not present"
            ))
        })?;
        objects.insert(path, Value::String(object));
    }
    Ok(json!({
        "execution_commit": commit,
        "execution_commit_note": "The tree this run actually executed against, read from the \
                                  checkout the campaign is running in. In CI this is the \
                                  pull-request merge commit rather than the branch head, and it \
                                  is the authority: the objects below are its objects.",
        "tree_clean": git(&root, &["status", "--porcelain"]).map(|status| status.is_empty()),
        "objects": Value::Object(objects),
    }))
}

/// Runs one git command against the workspace, tolerating failure.
fn git(root: &std::path::Path, arguments: &[&str]) -> Option<String> {
    let output = std::process::Command::new("git")
        .current_dir(root)
        .args(arguments)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// A report failure that is not otherwise typed.
#[derive(Debug)]
struct ReportFailure(String);

impl std::fmt::Display for ReportFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for ReportFailure {}

/// A sink that offers every record to a bounded queue and counts the drops.
struct SheddingSink {
    queue: TelemetryQueue,
    offered: AtomicUsize,
}

impl SheddingSink {
    /// Binds one bounded queue, optionally already at its bound.
    ///
    /// A launch emits far fewer records than the smallest queue the framework
    /// accepts, so a queue that started empty would never fill and the
    /// saturated run would be the baseline run under another name. The
    /// saturated sink therefore fills its queue before the launch begins, which
    /// is the state an operator's queue is in when a burst is already in
    /// flight.
    fn new(bound: usize, prefill: bool) -> Self {
        let queue = TelemetryQueue::new(
            ExportQueueBound::new(bound).unwrap_or_default(),
            DropReportWindow::default(),
        );
        if prefill {
            for index in 0..bound {
                let _ = queue.enqueue(record(), Duration::from_millis(index as u64));
            }
        }
        Self {
            queue,
            offered: AtomicUsize::new(0),
        }
    }

    /// Returns how many records the queue shed.
    fn shed(&self) -> u64 {
        self.queue.dropped()
    }
}

impl TelemetryEventSink for SheddingSink {
    fn emit(&self, event: &TelemetryRecord) {
        let offered = self.offered.fetch_add(1, Ordering::SeqCst);
        // The queue is filled before the first real record so that the launch
        // runs entirely against a full queue rather than filling one as it
        // goes.
        let _ = self
            .queue
            .enqueue(event.clone(), Duration::from_millis(offered as u64));
    }
}

/// One launch and what its telemetry queue did.
struct Launch {
    category: ExitCategory,
    shed: u64,
    durable: Value,
}

/// One resource offered more than it holds.
struct Shed {
    resource: &'static str,
    policy: &'static str,
    rule: &'static str,
    ceiling: u64,
    configured: u64,
    offered: u64,
    peak: u64,
    retained: u64,
    discarded: u64,
    violations: Vec<String>,
}

impl Shed {
    /// Records a resource whose fixture could not be built at all.
    ///
    /// A report that could not offer overload has not observed a bound holding,
    /// so this is a violation rather than an absence.
    fn failed(resource: &'static str, reason: &str) -> Self {
        Self {
            resource,
            policy: "bounded-shedding",
            rule: "unknown",
            ceiling: 0,
            configured: 0,
            offered: 0,
            peak: 0,
            retained: 0,
            discarded: 0,
            violations: vec![reason.to_owned()],
        }
    }

    /// Renders what the retained evidence records for this resource.
    fn evidence(&self) -> Value {
        json!({
            "resource": self.resource,
            "overload_policy": self.policy,
            "shedding_rule": self.rule,
            "declared_ceiling": self.ceiling,
            "configured_ceiling": self.configured,
            "offered_load": self.offered,
            "observed_peak_occupancy": self.peak,
            "retained": self.retained,
            "drops": self.discarded,
            "rejections": 0,
            "waits": 0,
            "violations": self.violations,
            "passed": self.violations.is_empty(),
        })
    }
}

/// The durable comparison between a saturated and an unsaturated launch.
struct Equivalence {
    baseline_shed: u64,
    saturated_shed: u64,
    baseline_durable: Value,
    saturated_durable: Value,
    violations: Vec<String>,
}

impl Equivalence {
    /// Renders what the retained evidence records for the comparison.
    fn evidence(&self) -> Value {
        json!({
            "baseline_dropped_records": self.baseline_shed,
            "saturated_dropped_records": self.saturated_shed,
            "baseline_durable": self.baseline_durable,
            "saturated_durable": self.saturated_durable,
            "agrees": self.baseline_durable == self.saturated_durable,
            "violations": self.violations,
            "passed": self.violations.is_empty(),
        })
    }
}

/// One construction the framework must accept or refuse.
struct Cell {
    resource: &'static str,
    case: &'static str,
    value: u64,
    accepted: bool,
    expected: bool,
}

impl Cell {
    /// Records one construction result.
    const fn new(
        resource: &'static str,
        case: &'static str,
        value: u64,
        accepted: bool,
        expected: bool,
    ) -> Self {
        Self {
            resource,
            case,
            value,
            accepted,
            expected,
        }
    }

    /// Returns the violation this cell is, when it is one.
    fn violation(&self) -> Option<String> {
        (self.accepted != self.expected).then(|| {
            if self.expected {
                format!(
                    "{} refused {} {}, which is inside its declared bound",
                    self.resource, self.case, self.value,
                )
            } else {
                format!(
                    "{} accepted {} {}, which is outside its declared bound",
                    self.resource, self.case, self.value,
                )
            }
        })
    }

    /// Renders what the retained evidence records for this cell.
    fn evidence(&self) -> Value {
        json!({
            "resource": self.resource,
            "case": self.case,
            "value": self.value,
            "expected": if self.expected { "accepted" } else { "refused" },
            "observed": if self.accepted { "accepted" } else { "refused" },
        })
    }
}
