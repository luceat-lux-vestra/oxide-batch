//! P-014 over `PostgreSQL` stop, cancel, and drain.
//!
//! These are the scenarios the M5 cancellation campaign is made of. What P-014
//! owes is two latencies and a set of counts, and the two halves are held to
//! very different standards here, deliberately:
//!
//! - **the latencies are reported, never judged.** No accepted document states
//!   a cancellation budget, so nothing in this file compares a duration against
//!   a limit. The committed scope says so in as many words, and
//!   `cargo xtask cancellation` enforces it from the other side by checking
//!   that each duration was measured and is structurally possible rather than
//!   that it was small.
//! - **the counts are asserted.** The accepted contract does fix them: a drain
//!   that reports fewer unjoined tasks than it still owns is a defect at any
//!   speed, and it is checkable exactly because the report knows how many tasks
//!   it held.
//!
//! ## Why the latencies are read from the database
//!
//! Both start when the operator's `request_execution_stop` transaction commits
//! and end when a durable status is first readable — `STOPPING` for intake
//! stop, `STOPPED` for the terminal. Every one of those readings is taken by
//! [`Watcher`], on its own connection, from outside the runtime.
//!
//! The alternative was to have the framework report its own timings, through a
//! hook or a telemetry event. That is cheaper and it is what the M4 in-memory
//! measurement effectively does, and it is wrong here for two reasons. It asks
//! the component under test to time itself, and it would not measure the thing
//! the campaign is about: on the operator path the interesting latency is the
//! one an operator experiences, which starts at a committed request and ends at
//! a durably visible status. A hook inside the runtime would report the
//! interval between two points the runtime already knew about and would skip
//! the commit at each end.
//!
//! It costs a sampling floor — [`Watcher`] polls, so a transition is attributed
//! to at most one poll interval later than it happened — and that floor is
//! recorded beside every duration it bounds rather than left for a reader to
//! discover. It is two orders of magnitude below the framework's own stop poll
//! interval, which is the dominant term on this path.
//!
//! ## Why the drain report runs each deadline twice
//!
//! Once with tasks that finish before the deadline and once with tasks held
//! past it. Either alone is worthless as evidence: a coordinator hard-coded to
//! report nothing unjoined passes every completing drain, and one hard-coded to
//! report the held count passes every expiring one. Together, across three
//! deadlines whose held-task counts differ, they cannot both be satisfied by a
//! constant.
//!
//! ## What these scenarios do not establish
//!
//! Forced loss of a worker is a crash and recovery result and belongs to that
//! campaign — the accepted plan says so explicitly, and a process kill is not
//! measured here. Three of the six accepted `ShutdownTaskPhase` variants are
//! never occupied by an unjoined task in this campaign and are reported as
//! unexamined rather than counted as proved. Broker and remote worker phases,
//! which the plan also names, do not exist in M5 at all.

#![cfg(feature = "postgres")]

#[path = "cancellation/mod.rs"]
mod cancellation;

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use oxide_batch::{
    ActorRef, BatchStatus, BlockingTasklet, BlockingTaskletAdapter, BlockingTaskletContext,
    BoxFuture, ComponentRevision, DefinitionRevision, DrainResult, ExecutionContext, ExitStatus,
    FlowGraph, FlowJob, FlowLaunchReport, FlowLauncher, FlowNode, FlowTarget, JobExecution,
    JobInstanceKey, JobName, JobParameter, JobParameters, JobRepository, NodeId, OwnerToken,
    ParameterName, ParameterRole, ParameterValue, PartitionBudget, PartitionCount, PartitionKey,
    PartitionPlanEntry, PartitionPlanFactory, PartitionTaskletFactory, PartitionedStepNode,
    PostgresJobRepository, PostgresMigrator, SequentialIdGenerator, ShutdownCoordinator,
    ShutdownDeadline, ShutdownRequest, ShutdownSignal, ShutdownTaskPhase, StateLimits,
    StepComponents, StepName, StepNode, StopPollInterval, StopSource, StopToken, TaskJoinDeadline,
    Tasklet, TaskletContext, TaskletError, TaskletOutcome, TaskletStep, TelemetryFlushDeadline,
    TerminalKind,
};
use serde_json::{Value, json};

use cancellation::scope::{Deadline, Scope};
use cancellation::{
    Failure, FixedClock, Occupancy, Watcher, await_running_execution, config, execution_manifest,
    major_version, measurement_environment, migrator_url, remove_job, retain_observation,
    runtime_url,
};

/// Tokio worker threads every report pins itself to.
///
/// Pinned rather than taken from the host so that a latency measured on one
/// runner is comparable with the same run on another, and recorded in the
/// report for the same reason.
const WORKER_THREADS: usize = 4;

/// How long any bounded wait for a durable observation is given.
///
/// Generous, because it is not a measurement: nothing is asserted against how
/// long a wait took, only against what it eventually saw. It exists so that a
/// report whose transition never arrives fails on the observation it actually
/// took, while there is still time to retain it, rather than hanging until CI
/// kills the job and retains nothing at all.
const OBSERVATION_LIMIT: Duration = Duration::from_mins(2);

/// The telemetry flush deadline every drain in this campaign runs under.
///
/// Separate from the correctness deadlines by the accepted contract's own
/// design, and held constant across every deadline point so that varying the
/// correctness deadline varies one thing at a time.
const FLUSH_DEADLINE: Duration = Duration::from_millis(500);

/// The owner token every report claims its executions under.
const OWNER: [u8; 16] = [0x0e; 16];

// ---------------------------------------------------------------------------
// Report 1: the durable operator path
// ---------------------------------------------------------------------------

#[test]
fn operator_stop_reaches_a_durable_terminal_status() -> Result<(), Box<dyn Error>> {
    run("operator-stop", operator_stop)
}

/// Cancels a running attempt through the accepted operator path and measures
/// what it cost to stop intake and to reach a durable terminal status.
#[allow(
    clippy::too_many_lines,
    reason = "the report is one ordered run and the order is part of what the evidence says"
)]
async fn operator_stop(runtime: String, migrator: String) -> Result<Value, Box<dyn Error>> {
    let scope = Scope::read()?;
    let job_name = format!("{}_operator", scope.workload.job_name);
    let harness = Harness::open(&runtime, &migrator, &scope, &job_name).await?;

    let occupancy = Arc::new(Occupancy::new());
    let keys = partition_keys(scope.workload.partitions);
    let job = build_job(
        &scope,
        &job_name,
        &keys,
        WorkerKind::Cancellable,
        &occupancy,
        &Arc::new(Mutex::new(BTreeMap::new())),
        &harness.repository,
    )?;
    let parameters = run_parameters("operator")?;
    let key = JobInstanceKey::new(JobName::new(&job_name)?, &parameters);

    let (_source, stop) = StopSource::new();
    let owner = OwnerToken::from_bytes(OWNER);
    let interval = StopPollInterval::new(scope.workload.stop_poll_interval)?;

    let launch = async {
        FlowLauncher::new(&harness.repository, &harness.clock, &harness.ids)
            .with_execution_control(owner, interval)
            .launch(&job, &parameters, &stop)
            .await
    };

    let cancel = async {
        // The execution has to exist before an operator can name it, and it has
        // to have committed something before a cancellation has anything to
        // preserve. Both are waited for rather than slept past.
        let execution = await_running_execution(&harness.repository, &key, OBSERVATION_LIMIT)
            .await?
            .ok_or_else(|| Failure::boxed("the launch created no execution to cancel"))?;
        let committed_before = harness
            .watcher
            .await_completed_partitions(execution.id(), 1, OBSERVATION_LIMIT)
            .await?
            .ok_or_else(|| {
                Failure::boxed(
                    "no partition committed, so the cancellation had nothing to preserve",
                )
            })?;

        // The clock starts when the request is durable, not when it was made.
        let requested = request_stop(&harness.repository, &execution).await?;
        let requested_at = Instant::now();

        let intake = harness
            .watcher
            .await_status(execution.id(), &["STOPPING"], OBSERVATION_LIMIT)
            .await?;
        let terminal = harness
            .watcher
            .await_status(execution.id(), &["STOPPED"], OBSERVATION_LIMIT)
            .await?;

        Ok::<_, Box<dyn Error>>(Cancellation {
            execution,
            committed_before,
            requested,
            requested_at,
            intake_stop_at: intake.as_ref().map(|(at, _)| *at),
            terminal_at: terminal.as_ref().map(|(at, _)| *at),
            terminal_status: terminal.map(|(_, status)| status),
        })
    };

    let (launched, cancelled) = tokio::join!(launch, cancel);
    let launched = launched?;
    let cancelled = cancelled?;

    let to_intake_stop = cancelled
        .intake_stop_at
        .map(|at| at - cancelled.requested_at);
    let to_durable_terminal = cancelled.terminal_at.map(|at| at - cancelled.requested_at);

    // What the durable record says after the cancellation, read back rather
    // than taken from the launch report.
    let record = read_record(&harness.repository, &launched).await?;
    let committed_after = harness
        .watcher
        .completed_partitions(cancelled.execution.id())
        .await?;

    let mut violations = Vec::new();
    let status = launched.job_execution().metadata().status();
    let exit_status = launched.job_execution().metadata().exit_status().clone();

    if status != BatchStatus::Stopped {
        violations.push(format!(
            "the cancelled attempt persisted {status} rather than STOPPED"
        ));
    }
    if exit_status != ExitStatus::stopped() {
        violations.push(format!(
            "the cancelled attempt persisted the exit status {exit_status} rather than STOPPED"
        ));
    }
    if occupancy.active() != 0 {
        violations.push(format!(
            "{} worker(s) outlived the cancelled attempt",
            occupancy.active()
        ));
    }
    if committed_after < cancelled.committed_before {
        violations.push(format!(
            "{} partition(s) were committed before the cancellation and {committed_after} after, \
             so cancellation rolled back work that had already reached durable storage",
            cancelled.committed_before
        ));
    }
    if record
        .partitions
        .values()
        .any(|partition| partition.status == BatchStatus::Started)
    {
        violations.push(
            "a partition interrupted by the cancellation is still recorded as running".to_owned(),
        );
    }
    match (to_intake_stop, to_durable_terminal) {
        (Some(intake), Some(terminal)) if intake > terminal => violations.push(format!(
            "the durable terminal was reached in {} µs and intake stopped in {} µs, so the \
             terminal preceded intake stopping",
            terminal.as_micros(),
            intake.as_micros()
        )),
        (None, _) => violations.push(
            "intake stopping was never observed, so request-to-intake-stop was not measured"
                .to_owned(),
        ),
        (_, None) => violations.push(
            "the durable terminal was never observed, so request-to-durable-terminal was not \
             measured"
                .to_owned(),
        ),
        _ => {}
    }

    let observation = json!({
        "report": "operator-stop",
        "passed": violations.is_empty(),
        "violations": violations,
        "postgres_major_version": harness.major.clone(),
        "server_version": harness.server.clone(),
        "measurement_environment": measurement_environment(WORKER_THREADS),
        "execution_manifest": harness.manifest.clone(),
        "workload": {
            "job_name": job_name,
            "partitions": scope.workload.partitions,
            "worker_budget": scope.workload.worker_budget,
            "pool_size": scope.workload.pool_size,
            "worker_work_millis": scope.workload.worker_work.as_millis(),
            "stop_poll_interval_millis": scope.workload.stop_poll_interval.as_millis(),
            "accepted_stop_poll_default_millis": StopPollInterval::DEFAULT.get().as_millis(),
            "stop_poll_note": "The configured interval is the dominant term in \
                               request-to-intake-stop on this path. Both it and the accepted \
                               default are recorded so the measurement can be read against \
                               either.",
        },
        "cancellation_request": {
            "path": "request_execution_stop under compare-and-swap, committed before the clock \
                     starts",
            "actor": requested_actor(),
            "expected_version": cancelled.requested,
            "committed_partitions_before": cancelled.committed_before,
        },
        "latency": {
            "status": "observational",
            "status_note": "No accepted document states a cancellation budget. These are \
                            measurements and nothing in this campaign compares them against a \
                            limit.",
            "request_to_intake_stop_micros": to_intake_stop.map(|value| value.as_micros()),
            "request_to_intake_stop_means": "from the committed operator request to the durable \
                                             STOPPING transition being first readable",
            "request_to_durable_terminal_micros": to_durable_terminal.map(|value| value.as_micros()),
            "request_to_durable_terminal_means": "from the committed operator request to the \
                                                  durable STOPPED status being first readable",
            "ordering_holds": matches!(
                (to_intake_stop, to_durable_terminal),
                (Some(intake), Some(terminal)) if intake <= terminal
            ),
        },
        "durable_terminal": {
            "batch_status": status.as_str(),
            "exit_status": exit_status.to_string(),
            "watched_status": cancelled.terminal_status,
            "outcome": format!("{:?}", launched.outcome()),
        },
        "checkpoint": {
            "committed_partitions_before_cancellation": cancelled.committed_before,
            "committed_partitions_after_cancellation": committed_after,
            "preserved": committed_after >= cancelled.committed_before,
            "partition_statuses": record.partition_statuses(),
        },
        "workers": {
            "peak": occupancy.peak(),
            "admitted": occupancy.admitted(),
            "active_after_return": occupancy.active(),
        },
    });

    harness.close(&migrator, &job_name).await?;
    Ok(observation)
}

// ---------------------------------------------------------------------------
// Report 2: the phases, measured separately
// ---------------------------------------------------------------------------

#[test]
fn cancellation_latency_is_measured_separately_per_phase() -> Result<(), Box<dyn Error>> {
    run("phase-separation", phase_separation)
}

/// Measures request-to-durable-terminal separately for the async, blocking, and
/// transaction phases, and the process intake path beside them.
///
/// The accepted plan requires the phases separated rather than averaged,
/// because one number is dominated by whichever phase is slowest and hides the
/// others. Each phase is a full launch and cancellation of its own.
#[allow(
    clippy::too_many_lines,
    reason = "each phase is a full launch and cancellation in order, and the order is part of what the evidence says"
)]
async fn phase_separation(runtime: String, migrator: String) -> Result<Value, Box<dyn Error>> {
    let scope = Scope::read()?;
    let mut phases = Vec::new();
    let mut violations = Vec::new();
    let mut major = String::new();
    let mut server = String::new();
    let mut manifest = Value::Null;

    for (phase, kind) in [
        ("async", WorkerKind::Cancellable),
        ("blocking", WorkerKind::Blocking),
        ("transaction", WorkerKind::Transactional),
    ] {
        let job_name = format!("{}_{phase}", scope.workload.job_name);
        let harness = Harness::open(&runtime, &migrator, &scope, &job_name).await?;
        major = harness.major.clone();
        server = harness.server.clone();
        manifest = harness.manifest.clone();

        let occupancy = Arc::new(Occupancy::new());
        let keys = partition_keys(scope.workload.partitions);
        let job = build_job(
            &scope,
            &job_name,
            &keys,
            kind,
            &occupancy,
            &Arc::new(Mutex::new(BTreeMap::new())),
            &harness.repository,
        )?;
        let parameters = run_parameters(phase)?;
        let key = JobInstanceKey::new(JobName::new(&job_name)?, &parameters);
        let (_source, stop) = StopSource::new();
        let owner = OwnerToken::from_bytes(OWNER);
        let interval = StopPollInterval::new(scope.workload.stop_poll_interval)?;

        let launch = async {
            FlowLauncher::new(&harness.repository, &harness.clock, &harness.ids)
                .with_execution_control(owner, interval)
                .launch(&job, &parameters, &stop)
                .await
        };
        let cancel = async {
            let execution = await_running_execution(&harness.repository, &key, OBSERVATION_LIMIT)
                .await?
                .ok_or_else(|| Failure::boxed("the launch created no execution to cancel"))?;
            harness
                .watcher
                .await_completed_partitions(execution.id(), 1, OBSERVATION_LIMIT)
                .await?;
            request_stop(&harness.repository, &execution).await?;
            let requested_at = Instant::now();
            let intake = harness
                .watcher
                .await_status(execution.id(), &["STOPPING"], OBSERVATION_LIMIT)
                .await?;
            let terminal = harness
                .watcher
                .await_status(execution.id(), &["STOPPED"], OBSERVATION_LIMIT)
                .await?;
            Ok::<_, Box<dyn Error>>((
                requested_at,
                intake.map(|(at, _)| at),
                terminal.map(|(at, _)| at),
            ))
        };

        let (launched, cancelled) = tokio::join!(launch, cancel);
        let launched = launched?;
        let (requested_at, intake_at, terminal_at) = cancelled?;

        let status = launched.job_execution().metadata().status();
        let to_intake = intake_at.map(|at| at - requested_at);
        let to_terminal = terminal_at.map(|at| at - requested_at);

        if status != BatchStatus::Stopped {
            violations.push(format!(
                "the {phase}-phase cancellation persisted {status} rather than STOPPED"
            ));
        }
        if occupancy.active() != 0 {
            violations.push(format!(
                "{} worker(s) outlived the {phase}-phase cancellation",
                occupancy.active()
            ));
        }
        if to_terminal.is_none() {
            violations.push(format!(
                "the {phase} phase never reached a durable terminal, so its latency was not \
                 measured"
            ));
        }

        phases.push(json!({
            "phase": phase,
            "delivered_mechanism": kind.describe(),
            "request_to_intake_stop_micros": to_intake.map(|value| value.as_micros()),
            "request_to_durable_terminal_micros": to_terminal.map(|value| value.as_micros()),
            "batch_status": status.as_str(),
            "exit_status": launched.job_execution().metadata().exit_status().to_string(),
            "outcome": format!("{:?}", launched.outcome()),
            "workers_active_after_return": occupancy.active(),
            "stop_timings": kind.expected_timing(),
        }));

        harness.close(&migrator, &job_name).await?;
    }

    // The other intake path, measured beside the durable one rather than
    // averaged with it. This one is an atomic state transition rather than a
    // committed transaction, so it is expected to be orders of magnitude
    // shorter; reporting one figure for both would hide whichever is slower.
    let signal_coordinator = ShutdownCoordinator::default();
    let signal: ShutdownSignal = signal_coordinator.signal();
    let accepted_before = signal.ensure_accepting().is_ok();
    let process_requested_at = Instant::now();
    let first = signal.request_shutdown();
    let mut process_intake_stop = None;
    while process_intake_stop.is_none() {
        if signal.ensure_accepting().is_err() {
            process_intake_stop = Some(process_requested_at.elapsed());
        }
    }

    if !accepted_before {
        violations.push("process intake was already closed before the request".to_owned());
    }
    if first != ShutdownRequest::Initiated {
        violations.push(format!(
            "the first process shutdown request reported {first:?} rather than Initiated"
        ));
    }

    Ok(json!({
        "report": "phase-separation",
        "passed": violations.is_empty(),
        "violations": violations,
        "postgres_major_version": major,
        "server_version": server,
        "measurement_environment": measurement_environment(WORKER_THREADS),
        "execution_manifest": manifest,
        "latency": {
            "status": "observational",
            "status_note": "No accepted document states a cancellation budget. These are \
                            measurements and nothing in this campaign compares them against a \
                            limit.",
        },
        "phases": phases,
        "phase_mapping_note": "The plan's async, blocking, and transaction phases are mapped onto \
                               the delivered StopTiming and adapter mechanisms rather than onto a \
                               vocabulary invented for this campaign. See the phases section of \
                               the committed scope.",
        "process_intake": {
            "path": "ShutdownSignal::request_shutdown then ensure_accepting",
            "request_to_intake_stop_micros": process_intake_stop.map(|value| value.as_micros()),
            "first_request": format!("{first:?}"),
            "note": "An atomic state transition rather than a committed transaction, and measured \
                     by spinning on the accepted intake predicate rather than by polling a \
                     database. It shares no mechanism with the durable operator path and is \
                     reported separately for that reason.",
        },
        "unexamined": {
            "broker_phase": "M5 adds no broker, so the phase the accepted plan names does not \
                             exist to measure.",
            "remote_worker_phase": "M5 adds no remote or distributed semantics, so the phase the \
                                    accepted plan names does not exist to measure.",
        },
    }))
}

// ---------------------------------------------------------------------------
// Report 3: unjoined counts at every declared deadline
// ---------------------------------------------------------------------------

#[test]
fn drain_reports_unjoined_tasks_at_every_declared_deadline() -> Result<(), Box<dyn Error>> {
    run("deadline-unjoined", deadline_unjoined)
}

/// Runs every declared deadline twice and records what the drain reported.
///
/// Once completing and once expiring, because either alone is satisfied by a
/// coordinator that reports a constant. The held-task counts come from the
/// committed scope rather than from here, so the number this asserts against
/// and the number the runner reconciles are one number.
#[allow(
    clippy::too_many_lines,
    reason = "every declared deadline is run both ways in order, and the sequence is what the evidence is"
)]
async fn deadline_unjoined(runtime: String, migrator: String) -> Result<Value, Box<dyn Error>> {
    let scope = Scope::read()?;
    let job_name = format!("{}_drain", scope.workload.job_name);
    let harness = Harness::open(&runtime, &migrator, &scope, &job_name).await?;

    // The held tasks each perform a real repository read, so a drain has both a
    // task to join and a connection to get back. The lookup resolves nothing —
    // this job name has no instance — which is the point: it is a round trip to
    // the database, not a fixture the drain depends on.
    let lookup_key = JobInstanceKey::new(JobName::new(&job_name)?, &run_parameters("drain")?);

    let mut violations = Vec::new();
    let mut points = Vec::new();

    for deadline in &scope.deadlines {
        // A drain whose tasks finish. Nothing may be reported unjoined.
        let completing = drain_completing(&harness, &scope, deadline, &lookup_key).await?;
        if !matches!(
            completing.result,
            DrainResult::Complete { panicked_tasks: 0 }
        ) {
            violations.push(format!(
                "the completing drain at the {} deadline did not join every owned task: {:?}",
                deadline.id, completing.result
            ));
        }

        // A drain whose tasks are held past it. Everything held must be
        // reported, and attributed to the phase that holds it.
        let expiring = drain_expiring(&harness, &scope, deadline, &lookup_key).await?;
        match &expiring.result {
            DrainResult::Incomplete {
                unjoined_tasks,
                phases,
                escalated,
                ..
            } => {
                let attributed: usize = phases.iter().map(|phase| phase.count()).sum();
                if *unjoined_tasks != deadline.held_tasks {
                    violations.push(format!(
                        "the {} deadline held {} task(s) and the drain reported {unjoined_tasks} \
                         unjoined",
                        deadline.id, deadline.held_tasks
                    ));
                }
                if attributed != *unjoined_tasks {
                    violations.push(format!(
                        "the {} deadline reported {unjoined_tasks} unjoined and attributed \
                         {attributed} to phases",
                        deadline.id
                    ));
                }
                if *escalated {
                    violations.push(format!(
                        "the {} deadline reported escalation, but waiting ended by expiry",
                        deadline.id
                    ));
                }
            }
            other => violations.push(format!(
                "the {} deadline held {} task(s) past it and the drain reported {other:?}",
                deadline.id, deadline.held_tasks
            )),
        }

        points.push(json!({
            "deadline": deadline.id,
            "deadline_millis": deadline.duration.as_millis(),
            "accepted_constant": deadline.accepted_constant,
            "held_tasks": deadline.held_tasks,
            "completing": {
                "drain_complete": matches!(completing.result, DrainResult::Complete { .. }),
                "unjoined_tasks": unjoined_of(&completing.result),
                "panicked_tasks": panicked_of(&completing.result),
                "request_to_drain_complete_micros": completing.elapsed.as_micros(),
                "note": "The tasks finish well before the deadline, so this measures the \
                         coordinator's join cost rather than the deadline.",
            },
            "expiring": {
                "drain_complete": matches!(expiring.result, DrainResult::Complete { .. }),
                "unjoined_tasks": unjoined_of(&expiring.result),
                "panicked_tasks": panicked_of(&expiring.result),
                "escalated": escalated_of(&expiring.result),
                "phases": phases_of(&expiring.result),
                "waited_micros": expiring.elapsed.as_micros(),
                "note": "The tasks are held past the deadline, so the wait is the deadline and \
                         the reported count is what the coordinator still owned when it expired.",
            },
        }));
    }

    // Escalation ends waiting the other way, and owes the same count.
    let escalation = drain_escalating(&harness, &scope, &lookup_key).await?;
    match &escalation.result {
        DrainResult::Incomplete {
            unjoined_tasks,
            phases,
            escalated,
            ..
        } => {
            let attributed: usize = phases.iter().map(|phase| phase.count()).sum();
            if *unjoined_tasks != scope.escalation.held_tasks {
                violations.push(format!(
                    "escalation held {} task(s) and the drain reported {unjoined_tasks} unjoined",
                    scope.escalation.held_tasks
                ));
            }
            if attributed != *unjoined_tasks {
                violations.push(format!(
                    "escalation reported {unjoined_tasks} unjoined and attributed {attributed} to \
                     phases"
                ));
            }
            if !*escalated {
                violations.push(
                    "waiting was ended by a second request and the drain did not report escalation"
                        .to_owned(),
                );
            }
        }
        other => violations.push(format!(
            "escalation held {} task(s) and the drain reported {other:?}",
            scope.escalation.held_tasks
        )),
    }

    // Escalation must end waiting before the deadline it was configured with,
    // which is a structural check rather than a latency budget: the point is
    // that the second request rather than the clock ended the wait.
    let escalation_deadline = scope
        .deadlines
        .last()
        .map_or(Duration::from_secs(1), |deadline| deadline.duration);
    if escalation.elapsed >= escalation_deadline {
        violations.push(format!(
            "escalation took {} ms and its deadline was {} ms, so the deadline ended the wait \
             rather than the second request",
            escalation.elapsed.as_millis(),
            escalation_deadline.as_millis()
        ));
    }

    let observation = json!({
        "report": "deadline-unjoined",
        "passed": violations.is_empty(),
        "violations": violations,
        "postgres_major_version": harness.major.clone(),
        "server_version": harness.server.clone(),
        "measurement_environment": measurement_environment(WORKER_THREADS),
        "execution_manifest": harness.manifest.clone(),
        "deadlines": points,
        "escalation": {
            "held_tasks": scope.escalation.held_tasks,
            "unjoined_tasks": unjoined_of(&escalation.result),
            "escalated": escalated_of(&escalation.result),
            "phases": phases_of(&escalation.result),
            "request_to_escalated_report_micros": escalation.elapsed.as_micros(),
            "configured_deadline_millis": escalation_deadline.as_millis(),
            "note": "Waiting ended by a second request rather than by expiry. The count owed is \
                     the same either way, which is why escalation sits in this report.",
        },
        "observed_phases": scope.observed_phases.clone(),
        "unexamined_phases": unexamined_phases(&scope),
        "unexamined_note": "Accepted ShutdownTaskPhase variants this campaign never leaves a task \
                            unjoined in. Recorded as unexamined rather than counted as proved: \
                            spawning a placeholder task into each to fill the table would be \
                            reporting coverage the campaign does not have.",
        "owned_task_work": "Each held task performs a real repository read, so a drain has both a \
                            task to join and a connection to get back.",
    });

    harness.close(&migrator, &job_name).await?;
    Ok(observation)
}

// ---------------------------------------------------------------------------
// Report 4: restart after cancellation
// ---------------------------------------------------------------------------

#[test]
fn restart_after_cancellation_resumes_without_rerunning_committed_work()
-> Result<(), Box<dyn Error>> {
    run("restart-after-cancellation", restart_after_cancellation)
}

/// Cancels an attempt and then restarts it along the accepted recovery path.
///
/// A cancellation that leaves an unrestartable execution is not a successful
/// cancellation, so this closes the loop against the accepted M4 recovery
/// contract rather than assuming it still holds under a stop.
#[allow(
    clippy::too_many_lines,
    reason = "the report is one ordered run - cancel, read the durable record, restart, compare - and the order is the evidence"
)]
async fn restart_after_cancellation(
    runtime: String,
    migrator: String,
) -> Result<Value, Box<dyn Error>> {
    let scope = Scope::read()?;
    let job_name = format!("{}_restart", scope.workload.job_name);
    let harness = Harness::open(&runtime, &migrator, &scope, &job_name).await?;

    let occupancy = Arc::new(Occupancy::new());
    let invocations: Arc<Mutex<BTreeMap<String, usize>>> = Arc::new(Mutex::new(BTreeMap::new()));
    let keys = partition_keys(scope.workload.partitions);
    let job = build_job(
        &scope,
        &job_name,
        &keys,
        WorkerKind::Cancellable,
        &occupancy,
        &invocations,
        &harness.repository,
    )?;
    let parameters = run_parameters("restart")?;
    let key = JobInstanceKey::new(JobName::new(&job_name)?, &parameters);
    let owner = OwnerToken::from_bytes(OWNER);
    let interval = StopPollInterval::new(scope.workload.stop_poll_interval)?;

    // The cancelled attempt.
    let (_source, stop) = StopSource::new();
    let launch = async {
        FlowLauncher::new(&harness.repository, &harness.clock, &harness.ids)
            .with_execution_control(owner, interval)
            .launch(&job, &parameters, &stop)
            .await
    };
    let cancel = async {
        let execution = await_running_execution(&harness.repository, &key, OBSERVATION_LIMIT)
            .await?
            .ok_or_else(|| Failure::boxed("the launch created no execution to cancel"))?;
        harness
            .watcher
            .await_completed_partitions(execution.id(), 1, OBSERVATION_LIMIT)
            .await?;
        request_stop(&harness.repository, &execution).await?;
        harness
            .watcher
            .await_status(execution.id(), &["STOPPED"], OBSERVATION_LIMIT)
            .await?;
        Ok::<_, Box<dyn Error>>(execution)
    };
    let (cancelled_launch, cancelled_execution) = tokio::join!(launch, cancel);
    let cancelled_launch = cancelled_launch?;
    let cancelled_execution = cancelled_execution?;

    let committed_by_cancelled = read_record(&harness.repository, &cancelled_launch)
        .await?
        .completed_keys();
    let before_restart = snapshot(&invocations);

    // The restart along the accepted recovery path: the same job and the same
    // identifying parameters, with no stop request outstanding.
    let (_restart_source, restart_stop) = StopSource::new();
    let restarted = FlowLauncher::new(&harness.repository, &harness.clock, &harness.ids)
        .launch(&job, &parameters, &restart_stop)
        .await?;
    let after_restart = snapshot(&invocations);

    let re_run = after_restart
        .iter()
        .filter(|(key, count)| before_restart.get(*key).copied().unwrap_or_default() < **count)
        .map(|(key, _)| key.clone())
        .collect::<BTreeSet<_>>();
    let rerun_committed = re_run
        .intersection(&committed_by_cancelled)
        .cloned()
        .collect::<Vec<_>>();

    let same_instance = restarted.instance().id() == cancelled_launch.instance().id();
    let new_execution = restarted.job_execution().id() != cancelled_execution.id();
    let restart_status = restarted.job_execution().metadata().status();

    let mut violations = Vec::new();
    if !same_instance {
        violations.push(
            "the restart created a new job instance rather than a new attempt of the same one"
                .to_owned(),
        );
    }
    if !new_execution {
        violations.push("the restart reused the cancelled job execution".to_owned());
    }
    if !rerun_committed.is_empty() {
        violations.push(format!(
            "the restart re-ran {} partition(s) the cancelled attempt had already committed: {}",
            rerun_committed.len(),
            rerun_committed.join(", ")
        ));
    }
    if restart_status != BatchStatus::Completed {
        violations.push(format!(
            "the restart persisted {restart_status} rather than COMPLETED"
        ));
    }
    if occupancy.active() != 0 {
        violations.push(format!(
            "{} worker(s) outlived the restart",
            occupancy.active()
        ));
    }

    let observation = json!({
        "report": "restart-after-cancellation",
        "passed": violations.is_empty(),
        "violations": violations,
        "postgres_major_version": harness.major.clone(),
        "server_version": harness.server.clone(),
        "measurement_environment": measurement_environment(WORKER_THREADS),
        "execution_manifest": harness.manifest.clone(),
        "cancelled_attempt": {
            "batch_status": cancelled_launch.job_execution().metadata().status().as_str(),
            "exit_status": cancelled_launch.job_execution().metadata().exit_status().to_string(),
            "committed_partitions": committed_by_cancelled.len(),
        },
        "restart": {
            "same_instance": same_instance,
            "new_execution": new_execution,
            "batch_status": restart_status.as_str(),
            "exit_status": restarted.job_execution().metadata().exit_status().to_string(),
            "partitions_re_run": re_run.len(),
            "committed_partitions_re_run": rerun_committed,
            "recovery_path": "a second launch of the same job and identifying parameters after \
                              the cancelled attempt, with no stop request outstanding",
        },
        "workers": {
            "peak": occupancy.peak(),
            "admitted": occupancy.admitted(),
            "active_after_return": occupancy.active(),
        },
    });

    harness.close(&migrator, &job_name).await?;
    Ok(observation)
}

// ---------------------------------------------------------------------------
// Shared mechanics
// ---------------------------------------------------------------------------

/// Runs one report on a pinned runtime and retains its observation.
///
/// The fixture check happens here, once, and it skips rather than fails: an
/// ordinary `cargo test` on a machine with no database must not be red. That is
/// precisely why passing tests are not the campaign — `cargo xtask cancellation`
/// resolves the fixture first and fails before any target runs when it is
/// missing, so a skip can never be counted as evidence.
fn run<F, R>(name: &str, report: F) -> Result<(), Box<dyn Error>>
where
    F: FnOnce(String, String) -> R,
    R: std::future::Future<Output = Result<Value, Box<dyn Error>>>,
{
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
    let observation = executor.block_on(report(runtime, migrator))?;
    retain_observation(name, &observation)?;

    let violations = observation
        .get("violations")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    if !violations.is_empty() {
        for violation in violations {
            eprintln!("violation: {violation}");
        }
        return Err(Failure::boxed(format!(
            "{name} observed {} violation(s)",
            violations.len()
        )));
    }
    Ok(())
}

/// Everything a report needs open against the database.
struct Harness {
    repository: PostgresJobRepository,
    watcher: Watcher,
    clock: FixedClock,
    ids: SequentialIdGenerator,
    major: String,
    server: String,
    manifest: Value,
}

impl Harness {
    /// Migrates, clears the job name, and opens the repository and watcher.
    async fn open(
        runtime: &str,
        migrator: &str,
        scope: &Scope,
        job_name: &str,
    ) -> Result<Self, Box<dyn Error>> {
        PostgresMigrator::migrate(&config(migrator.to_owned(), 1)?).await?;
        remove_job(migrator, job_name).await?;

        let watcher = Watcher::connect(runtime).await?;
        let server = watcher.server_version().await?;
        let clock = FixedClock::default();
        let repository = PostgresJobRepository::connect(
            config(runtime.to_owned(), scope.workload.pool_size)?,
            Arc::new(clock),
        )
        .await?;

        Ok(Self {
            repository,
            watcher,
            clock,
            ids: SequentialIdGenerator::new(NonZeroU64::MIN),
            major: major_version(&server),
            server,
            manifest: execution_manifest()?,
        })
    }

    /// Closes everything and clears the job name behind the report.
    ///
    /// The repository close is propagated rather than discarded. A pool that
    /// cannot be closed after a cancellation is a finding of exactly the kind
    /// this campaign is looking for — work that outlived the attempt that owned
    /// it — so it fails the report instead of being swallowed by cleanup.
    async fn close(&self, migrator: &str, job_name: &str) -> Result<(), Box<dyn Error>> {
        self.watcher.close().await;
        self.repository.close().await?;
        remove_job(migrator, job_name).await?;
        Ok(())
    }
}

/// What a cancellation observed on the durable operator path.
struct Cancellation {
    execution: JobExecution,
    committed_before: i64,
    requested: u64,
    requested_at: Instant,
    intake_stop_at: Option<Instant>,
    terminal_at: Option<Instant>,
    terminal_status: Option<String>,
}

/// Makes the accepted durable stop request and returns the version it used.
///
/// The request is compare-and-swap guarded, so the version it expects is part
/// of what the report records: a request that lost the check would not be a
/// cancellation at all.
async fn request_stop(
    repository: &PostgresJobRepository,
    execution: &JobExecution,
) -> Result<u64, Box<dyn Error>> {
    let mut unit = repository.begin().await?;
    let current = unit
        .get_job_execution(execution.id())
        .await?
        .ok_or_else(|| Failure::boxed("the execution to cancel disappeared"))?;
    let version = current.version();
    unit.request_execution_stop(
        execution.id(),
        version,
        &ActorRef::new(requested_actor())?,
        FixedClock::default().0,
    )
    .await?;
    unit.commit().await?;
    Ok(version.get())
}

/// The actor every operator request in this campaign is made under.
const fn requested_actor() -> &'static str {
    "operator:m5-cancellation-campaign"
}

/// One drain and how long the campaign waited for it.
struct Drain {
    result: DrainResult,
    elapsed: Duration,
}

/// Builds a coordinator at one declared deadline.
fn coordinator_at(deadline: Duration) -> Result<ShutdownCoordinator, Box<dyn Error>> {
    let shutdown = ShutdownDeadline::new(deadline)?;
    Ok(ShutdownCoordinator::new(
        shutdown,
        TaskJoinDeadline::new(deadline, shutdown)?,
        TelemetryFlushDeadline::new(FLUSH_DEADLINE)?,
    )?)
}

/// Drains tasks that finish well before the deadline.
async fn drain_completing(
    harness: &Harness,
    scope: &Scope,
    deadline: &Deadline,
    key: &JobInstanceKey,
) -> Result<Drain, Box<dyn Error>> {
    let mut coordinator = coordinator_at(deadline.duration)?;
    for slot in 0..deadline.held_tasks {
        let reader = harness.repository.clone();
        let lookup = key.clone();
        coordinator.spawn(phase_for(scope, slot), async move {
            // A real repository read, so the drain has both a task to join and
            // a connection to get back.
            if let Ok(mut unit) = reader.begin().await {
                let _ = unit.find_job_instance(&lookup).await;
                let _ = unit.rollback().await;
            }
        })?;
    }
    let started = Instant::now();
    let report = coordinator
        .shutdown(|| async { Ok(()) }, || async { Ok(0) }, || async { Ok(()) })
        .await;
    Ok(Drain {
        result: report.drain().clone(),
        elapsed: started.elapsed(),
    })
}

/// Drains tasks held past the deadline, then releases them.
async fn drain_expiring(
    harness: &Harness,
    scope: &Scope,
    deadline: &Deadline,
    key: &JobInstanceKey,
) -> Result<Drain, Box<dyn Error>> {
    let mut coordinator = coordinator_at(deadline.duration)?;
    let (release, released) = StopSource::new();
    for slot in 0..deadline.held_tasks {
        let released = released.clone();
        let reader = harness.repository.clone();
        let lookup = key.clone();
        coordinator.spawn(phase_for(scope, slot), async move {
            if let Ok(mut unit) = reader.begin().await {
                let _ = unit.find_job_instance(&lookup).await;
                let _ = unit.rollback().await;
            }
            // Held on the crate's own level-triggered cooperative token, so a
            // release cannot be missed by a task that has not started waiting.
            released.cancelled().await;
        })?;
    }
    let started = Instant::now();
    let report = coordinator
        .shutdown(|| async { Ok(()) }, || async { Ok(0) }, || async { Ok(()) })
        .await;
    let elapsed = started.elapsed();
    // Released after the drain has reported, so the tasks are genuinely still
    // owned at the moment the count is taken, and are not leaked afterwards.
    release.request_stop();
    Ok(Drain {
        result: report.drain().clone(),
        elapsed,
    })
}

/// Drains tasks that a second request stops waiting for.
async fn drain_escalating(
    harness: &Harness,
    scope: &Scope,
    key: &JobInstanceKey,
) -> Result<Drain, Box<dyn Error>> {
    // Configured at the longest declared deadline so that the only thing that
    // can end this wait quickly is the second request.
    let deadline = scope
        .deadlines
        .last()
        .map_or(Duration::from_secs(1), |deadline| deadline.duration);
    let mut coordinator = coordinator_at(deadline)?;
    let (release, released) = StopSource::new();
    for slot in 0..scope.escalation.held_tasks {
        let released = released.clone();
        let reader = harness.repository.clone();
        let lookup = key.clone();
        coordinator.spawn(phase_for(scope, slot), async move {
            if let Ok(mut unit) = reader.begin().await {
                let _ = unit.find_job_instance(&lookup).await;
                let _ = unit.rollback().await;
            }
            released.cancelled().await;
        })?;
    }

    // The application records the first request itself, so entering
    // coordination cannot turn it into an escalation and the concurrent second
    // request is the one that ends waiting.
    let signal = coordinator.signal();
    let first = signal.request_shutdown();
    let escalate = async {
        tokio::task::yield_now().await;
        signal.request_shutdown()
    };
    let started = Instant::now();
    let (report, second) = tokio::join!(
        coordinator.shutdown(|| async { Ok(()) }, || async { Ok(0) }, || async { Ok(()) }),
        escalate
    );
    let elapsed = started.elapsed();
    release.request_stop();

    if first != ShutdownRequest::Initiated || second != ShutdownRequest::Escalated {
        return Err(Failure::boxed(format!(
            "the escalation sequence reported {first:?} then {second:?} rather than Initiated \
             then Escalated"
        )));
    }
    Ok(Drain {
        result: report.drain().clone(),
        elapsed,
    })
}

/// Spreads held tasks across the phases the campaign declares it observes.
fn phase_for(scope: &Scope, slot: usize) -> ShutdownTaskPhase {
    let names = &scope.observed_phases;
    if names.is_empty() {
        return ShutdownTaskPhase::Tasklet;
    }
    match names[slot % names.len()].as_str() {
        "ChunkReadProcess" => ShutdownTaskPhase::ChunkReadProcess,
        "ChunkWrite" => ShutdownTaskPhase::ChunkWrite,
        "Transaction" => ShutdownTaskPhase::Transaction,
        "RetryBackoff" => ShutdownTaskPhase::RetryBackoff,
        "FlowDecision" => ShutdownTaskPhase::FlowDecision,
        _ => ShutdownTaskPhase::Tasklet,
    }
}

/// The accepted phases this campaign never leaves a task unjoined in.
fn unexamined_phases(scope: &Scope) -> Vec<String> {
    [
        "Tasklet",
        "ChunkReadProcess",
        "ChunkWrite",
        "Transaction",
        "RetryBackoff",
        "FlowDecision",
    ]
    .into_iter()
    .filter(|phase| !scope.observed_phases.iter().any(|name| name == phase))
    .map(str::to_owned)
    .collect()
}

/// Returns the unjoined total a drain reported.
const fn unjoined_of(result: &DrainResult) -> usize {
    // A complete drain and any future variant both report nothing unjoined,
    // which is the honest answer: this campaign asserts on counts it can see,
    // and a variant it does not know about has not told it about one.
    match result {
        DrainResult::Incomplete { unjoined_tasks, .. } => *unjoined_tasks,
        _ => 0,
    }
}

/// Returns the panic count a drain reported.
const fn panicked_of(result: &DrainResult) -> usize {
    match result {
        DrainResult::Complete { panicked_tasks }
        | DrainResult::Incomplete { panicked_tasks, .. } => *panicked_tasks,
        _ => 0,
    }
}

/// Returns whether a drain reported that escalation ended its wait.
const fn escalated_of(result: &DrainResult) -> bool {
    match result {
        DrainResult::Incomplete { escalated, .. } => *escalated,
        _ => false,
    }
}

/// Renders the per-phase unjoined counts a drain reported.
fn phases_of(result: &DrainResult) -> Value {
    match result {
        DrainResult::Incomplete { phases, .. } => Value::Array(
            phases
                .iter()
                .map(|phase| {
                    json!({
                        "phase": format!("{:?}", phase.phase()),
                        "count": phase.count(),
                    })
                })
                .collect(),
        ),
        _ => Value::Array(Vec::new()),
    }
}

/// The worker body a report's partitioned step is built from.
#[derive(Clone, Copy)]
enum WorkerKind {
    /// An asynchronous worker that observes the stop while it is running.
    Cancellable,
    /// A synchronous worker isolated by the accepted blocking adapter.
    Blocking,
    /// An asynchronous worker holding an open repository transaction.
    Transactional,
}

impl WorkerKind {
    /// Describes the delivered mechanism this phase is measured through.
    const fn describe(self) -> &'static str {
        match self {
            Self::Cancellable => {
                "an asynchronous tasklet awaiting the cooperative stop token while it runs"
            }
            Self::Blocking => {
                "BlockingTaskletAdapter, whose synchronous body runs to completion and reports the \
                 stop afterwards"
            }
            Self::Transactional => {
                "an asynchronous tasklet holding an open repository transaction when the stop \
                 arrives"
            }
        }
    }

    /// The `StopTiming` the accepted contract produces for this mechanism.
    const fn expected_timing(self) -> &'static str {
        match self {
            Self::Cancellable | Self::Transactional => {
                "DuringExecution for a worker already running, BeforeStart for one not yet reached"
            }
            Self::Blocking => "AfterBlockingWork for a worker already inside its synchronous body",
        }
    }
}

/// Builds the partitioned job a report cancels.
fn build_job(
    scope: &Scope,
    job_name: &str,
    keys: &[String],
    kind: WorkerKind,
    occupancy: &Arc<Occupancy>,
    invocations: &Arc<Mutex<BTreeMap<String, usize>>>,
    repository: &PostgresJobRepository,
) -> Result<FlowJob, Box<dyn Error>> {
    let name = JobName::new(job_name)?;
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
            PartitionCount::new(scope.workload.partitions)?,
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
    let work = scope.workload.worker_work;
    let occupancy = Arc::clone(occupancy);
    let invocations = Arc::clone(invocations);
    let repository = repository.clone();
    let lookup = JobInstanceKey::new(JobName::new(job_name)?, &run_parameters("worker")?);
    let factory = PartitionTaskletFactory::new(worker_name, move |input| {
        let key = input.key().as_str().to_owned();
        let occupancy = Arc::clone(&occupancy);
        let invocations = Arc::clone(&invocations);
        match kind {
            WorkerKind::Blocking => TaskletStep::new(
                factory_name.clone(),
                Arc::new(BlockingTaskletAdapter::new(
                    BlockingWorker {
                        occupancy,
                        invocations,
                        work,
                        key,
                    },
                    NonZeroUsize::MIN,
                )),
            ),
            WorkerKind::Transactional => TaskletStep::new(
                factory_name.clone(),
                Arc::new(TransactionalWorker {
                    occupancy,
                    invocations,
                    repository: repository.clone(),
                    lookup: lookup.clone(),
                    work,
                    key,
                }),
            ),
            WorkerKind::Cancellable => TaskletStep::new(
                factory_name.clone(),
                Arc::new(CancellableWorker {
                    occupancy,
                    invocations,
                    work,
                    key,
                }),
            ),
        }
    });

    Ok(FlowJob::new(name, plan)?.with_partitioned_tasklet(manager, partitioner, factory)?)
}

/// An asynchronous worker that observes the cooperative stop while running.
struct CancellableWorker {
    occupancy: Arc<Occupancy>,
    invocations: Arc<Mutex<BTreeMap<String, usize>>>,
    work: Duration,
    key: String,
}

impl Tasklet for CancellableWorker {
    fn execute<'a>(
        &'a self,
        context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            self.occupancy.enter();
            if let Ok(mut invocations) = self.invocations.lock() {
                *invocations.entry(self.key.clone()).or_default() += 1;
            }
            let stop: &StopToken = context.stop_token();

            // A bounded await as the work, raced against the accepted
            // cooperative token. Racing rather than polling is what makes the
            // stop observable *during* execution, which is the async phase the
            // accepted plan asks to see measured separately.
            let outcome = tokio::select! {
                () = tokio::time::sleep(self.work) => TaskletOutcome::Completed,
                () = stop.cancelled() => TaskletOutcome::Stopped,
            };
            self.occupancy.leave();
            Ok(outcome)
        })
    }
}

/// An asynchronous worker holding an open repository transaction.
///
/// This is the accepted plan's transaction phase, and it has to actually hold a
/// transaction to be that. An earlier version of this campaign mapped the
/// transaction phase onto the same body as the async one, which produced two
/// measurements that agreed to within three milliseconds because they were the
/// same measurement taken twice — a phase separation that separated nothing.
struct TransactionalWorker {
    occupancy: Arc<Occupancy>,
    invocations: Arc<Mutex<BTreeMap<String, usize>>>,
    repository: PostgresJobRepository,
    lookup: JobInstanceKey,
    work: Duration,
    key: String,
}

impl Tasklet for TransactionalWorker {
    fn execute<'a>(
        &'a self,
        context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            self.occupancy.enter();
            if let Ok(mut invocations) = self.invocations.lock() {
                *invocations.entry(self.key.clone()).or_default() += 1;
            }
            let stop: &StopToken = context.stop_token();

            // The stop is raced while a repository transaction is open, so what
            // this phase measures is a cancellation that has a transaction to
            // resolve before it can reach a durable terminal.
            let outcome = match self.repository.begin().await {
                Ok(mut unit) => {
                    let _ = unit.find_job_instance(&self.lookup).await;
                    let observed = tokio::select! {
                        () = tokio::time::sleep(self.work) => TaskletOutcome::Completed,
                        () = stop.cancelled() => TaskletOutcome::Stopped,
                    };
                    // Rolled back rather than dropped: an open transaction that
                    // is dropped at cancellation is precisely the ambiguity the
                    // accepted contract refuses to manufacture.
                    let _ = unit.rollback().await;
                    observed
                }
                Err(_) => TaskletOutcome::Stopped,
            };
            self.occupancy.leave();
            Ok(outcome)
        })
    }
}

/// A synchronous worker isolated by the accepted blocking adapter.
struct BlockingWorker {
    occupancy: Arc<Occupancy>,
    invocations: Arc<Mutex<BTreeMap<String, usize>>>,
    work: Duration,
    key: String,
}

impl BlockingTasklet for BlockingWorker {
    fn execute(&self, _context: BlockingTaskletContext) -> Result<TaskletOutcome, TaskletError> {
        self.occupancy.enter();
        if let Ok(mut invocations) = self.invocations.lock() {
            *invocations.entry(self.key.clone()).or_default() += 1;
        }
        // Once this starts it runs to completion even when stop is requested;
        // the adapter reports the request afterwards. That late-stop
        // limitation is the accepted contract and is exactly what the blocking
        // phase measurement is about.
        std::thread::sleep(self.work);
        self.occupancy.leave();
        Ok(TaskletOutcome::Completed)
    }
}

/// One partition's durable state after a report.
struct DurablePartition {
    status: BatchStatus,
}

/// What the durable record said after a cancellation.
struct DurableRecord {
    partitions: BTreeMap<String, DurablePartition>,
}

impl DurableRecord {
    /// Renders every partition's durable status.
    fn partition_statuses(&self) -> Value {
        Value::Object(
            self.partitions
                .iter()
                .map(|(key, partition)| {
                    (
                        key.clone(),
                        Value::String(partition.status.as_str().to_owned()),
                    )
                })
                .collect(),
        )
    }

    /// Returns the keys of every partition durably recorded as complete.
    fn completed_keys(&self) -> BTreeSet<String> {
        self.partitions
            .iter()
            .filter(|(_, partition)| partition.status == BatchStatus::Completed)
            .map(|(key, _)| key.clone())
            .collect()
    }
}

/// Reads one attempt's durable partition record back from the repository.
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
        partitions: partitions
            .iter()
            .map(|partition| {
                (
                    partition.key().as_str().to_owned(),
                    DurablePartition {
                        status: partition.status(),
                    },
                )
            })
            .collect(),
    })
}

/// Takes a copy of the per-key invocation counts.
fn snapshot(invocations: &Arc<Mutex<BTreeMap<String, usize>>>) -> BTreeMap<String, usize> {
    invocations
        .lock()
        .map(|counts| counts.clone())
        .unwrap_or_default()
}

/// The partition keys the declared workload offers.
fn partition_keys(count: u16) -> Vec<String> {
    (0..count)
        .map(|index| format!("partition-{index:04}"))
        .collect()
}

/// Builds one partition plan entry.
fn entry(key: &str) -> Result<PartitionPlanEntry, Box<dyn Error>> {
    let context = ExecutionContext::from_json(
        format!(
            "{{\"format\":\"oxide-batch.execution-context\",\"format_version\":1,\
             \"schema\":\"m5.cancellation\",\"schema_version\":1,\
             \"payload\":{{\"key\":\"{key}\"}}}}"
        )
        .as_bytes(),
        StateLimits::new(4 * 1024, 16)?,
    )?;
    Ok(PartitionPlanEntry::new(PartitionKey::new(key)?, context)?)
}

/// Builds the identifying parameters one report launches under.
fn run_parameters(run: &str) -> Result<JobParameters, Box<dyn Error>> {
    let mut parameters = JobParameters::new();
    parameters.insert(
        ParameterName::new("run")?,
        JobParameter::new(
            ParameterValue::string(run.to_owned())?,
            ParameterRole::Identifying,
        ),
    )?;
    Ok(parameters)
}
