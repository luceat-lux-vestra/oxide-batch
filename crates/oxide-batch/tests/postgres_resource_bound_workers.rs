//! Worker and connection ceilings under `PostgreSQL` saturation.
//!
//! This is the scenario the M5 design gate names for the resource-bound
//! campaign, and it covers the one class of framework resource whose bound
//! cannot be checked by calling a constructor. A partition key that is one byte
//! too long is refused by a function, and a test can prove that in a line. A
//! worker set is a live quantity: the ceiling holds or does not hold only while
//! work is running, and nothing about the configuration says which happened.
//!
//! So the report offers more work than the ceiling admits and observes what the
//! framework actually held.
//!
//! Two things have to be true at once for that observation to be evidence, and
//! they pull in opposite directions. The peak must not exceed the ceiling —
//! that is the bound. And the peak must *reach* the ceiling — otherwise the run
//! never filled the worker set, and a framework that admitted three workers at
//! a time would produce the same green result. The report therefore holds each
//! wave of workers at a barrier sized to the ceiling: a run that admits fewer
//! than the ceiling never completes a wave, times out, and fails on the peak it
//! actually saw rather than hanging; a run that admits more trips the gauge.
//!
//! The connection requirement is the fail-closed half. A bounded local step
//! derives `concurrent_children + 1` connections, and a pool one connection
//! short must be refused *before* a child exists rather than deadlocking on
//! acquisition halfway through. That is checked against a real pool sized one
//! short rather than a repository double that reports a smaller number, because
//! the interesting failure — the launcher accepting a budget the pool cannot
//! serve — is a property of the real adapter's capacity. The refusal is then
//! confirmed as an absence in the database: no instance, no execution, no
//! definition. A rejection that had already written something would be a
//! partial launch wearing an error's clothes.
//!
//! Throughput is not asserted anywhere here, and no timing is compared against
//! a threshold. What the concurrent run has to match is the *durable* record of
//! the same work run one child at a time: the performance plan holds that a
//! concurrency result which changes a durable observation is invalid regardless
//! of its throughput, so the baseline runs the same 128 partitions with a
//! budget of one and the two records are compared field by field.

#![cfg(feature = "postgres")]

#[path = "resource_bounds/mod.rs"]
mod resource_bounds;

use std::collections::BTreeMap;
use std::error::Error;
use std::num::NonZeroU64;
use std::sync::Arc;

use oxide_batch::{
    BatchStatus, BoxFuture, ComponentRevision, DefinitionRevision, ExecutionContext,
    ExecutionCounts, ExitStatus, FlowExecutionOutcome, FlowGraph, FlowJob, FlowLauncher, FlowNode,
    FlowRuntimeError, FlowTarget, JobName, JobParameters, JobRepository, JoinNode,
    MAX_BRANCH_STEPS, MAX_PARTITION_WORKERS, MAX_PARTITIONS, MAX_SPLIT_BRANCHES, NodeId,
    PartitionBudget, PartitionCount, PartitionFactoryError, PartitionKey, PartitionPlanEntry,
    PartitionPlanFactory, PartitionTaskletFactory, PartitionedStepNode, PostgresConfig,
    PostgresJobRepository, PostgresMigrator, SequentialIdGenerator, SplitBranch, SplitBudget,
    SplitNode, StateLimits, StepComponents, StepName, StepNode, StopSource, Tasklet,
    TaskletContext, TaskletError, TaskletOutcome, TaskletStep, TaskletStepFactory, TerminalKind,
};
use serde_json::{Value, json};
use tokio::sync::Barrier;

use resource_bounds::{
    Failure, FixedClock, Occupancy, config, config_with_pool, execution_manifest, join_wave,
    major_version, migrator_url, remove_job, retain_observation, runtime_url, server_version,
};

/// The report identifier the runner reconciles this observation under.
const REPORT: &str = "worker-assignment";

/// The partitioned job whose worker set is saturated.
const STRESSED_JOB: &str = "m5_resource_bound_partitions_stressed";

/// The same work with a budget of one child, for the durable comparison.
const BASELINE_JOB: &str = "m5_resource_bound_partitions_baseline";

/// The split whose branch set is saturated.
const SPLIT_JOB: &str = "m5_resource_bound_split";

/// The job used to prove a pool one connection short is refused.
const SHORT_POOL_JOB: &str = "m5_resource_bound_short_pool";

/// Partitions offered against the worker ceiling.
///
/// Twice the ceiling, so the worker set has to fill, drain, and fill again. One
/// wave would leave the report unable to distinguish a bound that holds from a
/// run that simply had nothing more to admit.
const OFFERED_PARTITIONS: u16 = 128;

/// Branches offered against the branch ceiling.
const OFFERED_BRANCHES: usize = MAX_SPLIT_BRANCHES;

#[test]
fn declared_ceilings_hold_under_stress_with_backpressure() -> Result<(), Box<dyn Error>> {
    let Some(runtime) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let Some(migrator) = migrator_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL is not set");
        return Ok(());
    };

    let executor = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    executor.block_on(report(runtime, migrator))
}

/// Runs every worker-assignment obligation and retains one observation.
async fn report(runtime: String, migrator: String) -> Result<(), Box<dyn Error>> {
    PostgresMigrator::migrate(&config(migrator.clone())?).await?;
    clear(&migrator).await?;

    let server = server_version(&runtime).await?;
    let mut violations = Vec::new();

    let construction = construction_cells();
    violations.extend(construction.iter().filter_map(Cell::violation));

    let connections = short_pool_is_refused_before_any_child(&runtime).await?;
    violations.extend(connections.violations.clone());

    let (workers, stressed) = saturate_partition_workers(&runtime).await?;
    violations.extend(workers.violations.clone());

    let branches = saturate_split_branches(&runtime).await?;
    violations.extend(branches.violations.clone());

    let baseline = run_partitions(&runtime, BASELINE_JOB, 1, None).await?;
    let equivalence = compare(&baseline.observation, &stressed);
    let must_not_observe = must_not_observe_conditions(&migrator, &connections, &stressed).await?;
    let equivalence = equivalence.with_must_not_observe(must_not_observe);
    violations.extend(equivalence.violations.clone());

    let document = json!({
        "report": REPORT,
        "scenario": "declared_ceilings_hold_under_stress_with_backpressure",
        "server_version": server,
        "postgres_major_version": major_version(&server),
        "resources": [
            workers.evidence(),
            branches.evidence(),
            connections.evidence(),
        ],
        "construction": construction
            .iter()
            .map(Cell::evidence)
            .collect::<Vec<_>>(),
        "durable_equivalence": equivalence.evidence(),
        "execution_manifest": execution_manifest()?,
        "violations": violations,
        "passed": violations.is_empty(),
    });
    retain_observation(REPORT, &document)?;

    clear(&migrator).await?;

    assert!(
        violations.is_empty(),
        "the worker-assignment report observed {violations:#?}",
    );
    Ok(())
}

/// Removes every durable trace this report leaves behind.
///
/// The split runs under one job name per budget, so the names are derived the
/// same way they are built rather than listed twice.
async fn clear(url: &str) -> Result<(), Box<dyn Error>> {
    let ceiling = u8::try_from(MAX_SPLIT_BRANCHES).unwrap_or(u8::MAX);
    for job in [STRESSED_JOB, BASELINE_JOB, SHORT_POOL_JOB] {
        remove_job(url, job).await?;
    }
    for budget in [ceiling / 2, ceiling] {
        remove_job(url, &format!("{SPLIT_JOB}_{budget}")).await?;
    }
    Ok(())
}

/// Fills the worker set to its ceiling and reads what was held.
async fn saturate_partition_workers(url: &str) -> Result<(Stressed, Observation), Box<dyn Error>> {
    let ceiling = MAX_PARTITION_WORKERS;
    let barrier = Arc::new(Barrier::new(usize::from(ceiling)));
    let run = run_partitions(url, STRESSED_JOB, ceiling, Some(barrier)).await?;

    let peak = run.occupancy.peak();
    let mut violations = Vec::new();
    if peak > usize::from(ceiling) {
        violations.push(format!(
            "the partition worker budget is {ceiling} and {peak} workers were active at once",
        ));
    }
    if peak < usize::from(ceiling) {
        violations.push(format!(
            "the partition worker budget is {ceiling}, {OFFERED_PARTITIONS} partitions were \
             offered against it, and the worker set never held more than {peak}; a ceiling this \
             run never reached is not evidence that it holds",
        ));
    }
    if run.occupancy.active() != 0 {
        violations.push(format!(
            "{} partition workers were still holding the worker set after the step finished",
            run.occupancy.active(),
        ));
    }
    if run.occupancy.admitted() != usize::from(OFFERED_PARTITIONS) {
        violations.push(format!(
            "{OFFERED_PARTITIONS} partitions were offered and {} workers ran, so the ceiling was \
             held by dropping work rather than by bounding concurrency",
            run.occupancy.admitted(),
        ));
    }

    Ok((
        Stressed {
            resource: "concurrent-partition-workers",
            policy: "bounded-concurrency",
            declared: u64::from(ceiling),
            ceiling: u64::from(ceiling),
            offered: u64::from(OFFERED_PARTITIONS),
            peak: peak as u64,
            admitted: run.occupancy.admitted() as u64,
            connections: run.connections,
            detail: json!({
                "partitions_recorded": run.observation.partitions.len(),
                "waves": u64::from(OFFERED_PARTITIONS) / u64::from(ceiling),
            }),
            violations,
        },
        run.observation,
    ))
}

/// Fills the branch set to its budget and reads what was held.
///
/// This resource is the one place where the declared ceiling cannot be
/// saturated in the ordinary sense, and it is worth being explicit about why: a
/// split may declare at most `MAX_SPLIT_BRANCHES` branches and its budget may
/// admit at most the same number, so at the declared ceiling there is nothing
/// left over to hold back. Running only there would prove the branches all ran
/// and nothing about the budget bounding anything.
///
/// So the report runs twice. The budgeted run admits half the branches, which
/// is a real backlog: eight are offered, four run at a time, and the peak has
/// to be four. The ceiling run then admits all eight, which is what shows the
/// declared ceiling itself is reachable. The evidence records the budgeted run
/// as the observation, because that is the one with something to hold back, and
/// carries the ceiling run beside it.
async fn saturate_split_branches(url: &str) -> Result<Stressed, Box<dyn Error>> {
    let ceiling = u8::try_from(MAX_SPLIT_BRANCHES)
        .map_err(|_| Failure::boxed("the split branch ceiling does not fit a branch budget"))?;
    let budget = ceiling / 2;
    let occupancy = Arc::new(Occupancy::new());
    // The wave is the budget rather than the branch count: only `budget`
    // branches can be in flight, so a barrier sized to the branch count would
    // never complete.
    let barrier = Arc::new(Barrier::new(usize::from(budget)));
    let connections = u32::from(ceiling) + 1;

    let job = saturated_split_job(budget, connections, &occupancy, &barrier)?;

    let clock = FixedClock::default();
    let repository = PostgresJobRepository::connect(
        config_with_pool(url.to_owned(), connections)?,
        Arc::new(clock),
    )
    .await?;
    let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
    let (_, stop) = StopSource::new();
    let outcome = FlowLauncher::new(&repository, &clock, &ids)
        .launch(&job, &JobParameters::new(), &stop)
        .await?;
    let capacity = repository.connection_capacity();
    repository.close().await?;

    let peak = occupancy.peak();
    let mut violations = Vec::new();
    if outcome.outcome() != &FlowExecutionOutcome::Completed {
        violations.push(format!(
            "the budgeted split ended as {:?} rather than completing",
            outcome.outcome(),
        ));
    }
    if peak > usize::from(budget) {
        violations.push(format!(
            "the branch budget is {budget} and {peak} branches were active at once",
        ));
    }
    if peak < usize::from(budget) {
        violations.push(format!(
            "the branch budget is {budget}, {OFFERED_BRANCHES} branches were offered against it, \
             and no more than {peak} ran at once",
        ));
    }
    if occupancy.active() != 0 {
        violations.push(format!(
            "{} split branches were still active after the join",
            occupancy.active(),
        ));
    }

    let at_ceiling = split_at_ceiling(url, ceiling, connections).await?;
    if at_ceiling != usize::from(ceiling) {
        violations.push(format!(
            "the declared branch ceiling is {ceiling} and a split budgeted at it held no more \
             than {at_ceiling} branches at once",
        ));
    }

    Ok(Stressed {
        resource: "concurrent-split-branches",
        policy: "bounded-concurrency",
        declared: u64::from(ceiling),
        ceiling: u64::from(budget),
        offered: OFFERED_BRANCHES as u64,
        peak: peak as u64,
        admitted: occupancy.admitted() as u64,
        connections: capacity,
        detail: json!({
            "declared_ceiling": ceiling,
            "budgeted_run": { "budget": budget, "offered": OFFERED_BRANCHES, "peak": peak },
            "ceiling_run": { "budget": ceiling, "offered": OFFERED_BRANCHES, "peak": at_ceiling },
            "note": "The declared ceiling equals the largest branch count a split may declare, so \
                     a run budgeted at it has nothing left over to hold back. The budgeted run is \
                     what proves the budget bounds concurrency; the ceiling run is what proves \
                     the declared ceiling is reachable.",
            "join_outcome": format!("{:?}", outcome.outcome()),
        }),
        violations,
    })
}

/// Runs the same split budgeted at the declared ceiling and returns its peak.
async fn split_at_ceiling(
    url: &str,
    ceiling: u8,
    connections: u32,
) -> Result<usize, Box<dyn Error>> {
    let occupancy = Arc::new(Occupancy::new());
    let barrier = Arc::new(Barrier::new(usize::from(ceiling)));
    let job = saturated_split_job(ceiling, connections, &occupancy, &barrier)?;

    let clock = FixedClock::default();
    let repository = PostgresJobRepository::connect(
        config_with_pool(url.to_owned(), connections)?,
        Arc::new(clock),
    )
    .await?;
    let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
    let (_, stop) = StopSource::new();
    FlowLauncher::new(&repository, &clock, &ids)
        .launch(&job, &JobParameters::new(), &stop)
        .await?;
    repository.close().await?;

    Ok(occupancy.peak())
}

/// Builds the split whose branch set the report saturates.
fn saturated_split_job(
    budget: u8,
    connections: u32,
    occupancy: &Arc<Occupancy>,
    barrier: &Arc<Barrier>,
) -> Result<FlowJob, Box<dyn Error>> {
    // The two runs are two jobs, because the second would otherwise be a
    // restart of a completed instance rather than a launch.
    let name = JobName::new(format!("{SPLIT_JOB}_{budget}"))?;
    let prepare = NodeId::new("prepare")?;
    let split = NodeId::new("parallel")?;
    let join = NodeId::new("joined")?;
    let branches = (0..OFFERED_BRANCHES)
        .map(|index| {
            Ok(SplitBranch::new(vec![StepNode::new(
                NodeId::new(format!("branch-{index}"))?,
                StepName::new(format!("branch-{index}"))?,
                StepComponents::Tasklet(ComponentRevision::new("branch-v1")?),
            )]))
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    // A split may not be a graph's entry, so the plan starts at an ordinary
    // step. It is not part of what is being measured, and it holds nothing.
    let plan = FlowGraph::new(prepare.clone())
        .with_node(FlowNode::step(StepNode::new(
            prepare.clone(),
            StepName::new("prepare")?,
            StepComponents::Tasklet(ComponentRevision::new("prepare-v1")?),
        )))
        .with_node(FlowNode::split(SplitNode::new(
            split.clone(),
            branches,
            join.clone(),
            SplitBudget::new(budget, connections)?,
        )))
        .with_node(FlowNode::join(JoinNode::new(join.clone())))
        .with_sequence(prepare.clone(), FlowTarget::Node(split))?
        .with_sequence(join, FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(&name, DefinitionRevision::new("v1")?)?;

    let mut job = FlowJob::new(name, plan)?.with_tasklet_step(
        prepare,
        TaskletStep::new(
            StepName::new("prepare")?,
            Arc::new(WaveTasklet {
                occupancy: Arc::new(Occupancy::new()),
                barrier: None,
            }),
        ),
    )?;
    for index in 0..OFFERED_BRANCHES {
        let step_name = StepName::new(format!("branch-{index}"))?;
        let factory_name = step_name.clone();
        let occupancy = Arc::clone(occupancy);
        let barrier = Arc::clone(barrier);
        job = job.with_split_tasklet_factory(
            NodeId::new(format!("branch-{index}"))?,
            TaskletStepFactory::new(step_name, move || {
                TaskletStep::new(
                    factory_name.clone(),
                    Arc::new(WaveTasklet {
                        occupancy: Arc::clone(&occupancy),
                        barrier: Some(Arc::clone(&barrier)),
                    }),
                )
            }),
        )?;
    }

    Ok(job)
}

/// Runs the partitioned step once and reads its durable record.
async fn run_partitions(
    url: &str,
    job_name: &str,
    workers: u8,
    barrier: Option<Arc<Barrier>>,
) -> Result<Run, Box<dyn Error>> {
    let occupancy = Arc::new(Occupancy::new());
    let connections = u32::from(workers) + 1;
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
            PartitionCount::new(OFFERED_PARTITIONS)?,
            PartitionBudget::new(workers, connections)?,
        )))
        .with_sequence(
            manager.clone(),
            FlowTarget::Terminal(TerminalKind::Complete),
        )?
        .compile(&name, DefinitionRevision::new("v1")?)?;

    let entries = (0..OFFERED_PARTITIONS)
        .map(|index| entry(&format!("partition-{index:04}")))
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    let partitioner = PartitionPlanFactory::new(move |request| {
        if request.partition_count().get() != OFFERED_PARTITIONS {
            return Err(PartitionFactoryError::Rejected);
        }
        Ok(entries.clone())
    });
    let factory_name = worker_name.clone();
    let workers_occupancy = Arc::clone(&occupancy);
    let factory = PartitionTaskletFactory::new(worker_name, move |_input| {
        TaskletStep::new(
            factory_name.clone(),
            Arc::new(WaveTasklet {
                occupancy: Arc::clone(&workers_occupancy),
                barrier: barrier.clone(),
            }),
        )
    });
    let job = FlowJob::new(name, plan)?.with_partitioned_tasklet(manager, partitioner, factory)?;

    let clock = FixedClock::default();
    let repository = PostgresJobRepository::connect(
        config_with_pool(url.to_owned(), connections)?,
        Arc::new(clock),
    )
    .await?;
    let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
    let (_, stop) = StopSource::new();
    let launched = FlowLauncher::new(&repository, &clock, &ids)
        .launch(&job, &JobParameters::new(), &stop)
        .await?;

    let parent = launched
        .step_executions()
        .last()
        .ok_or_else(|| Failure::boxed("the partitioned step produced no parent execution"))?;
    let mut unit = repository.begin().await?;
    let partitions = unit.step_partition_plan(parent.id()).await?;
    unit.rollback().await?;

    let observation = Observation {
        outcome: format!("{:?}", launched.outcome()),
        job_status: launched.job_execution().metadata().status(),
        job_exit_status: launched.job_execution().metadata().exit_status().clone(),
        parent_status: parent.metadata().status(),
        parent_exit_status: parent.metadata().exit_status().clone(),
        parent_counts: parent.metadata().counts(),
        step_executions: launched.step_executions().len(),
        raw_partition_rows: partitions.len(),
        partitions: partitions
            .iter()
            .map(|partition| {
                (
                    partition.key().as_str().to_owned(),
                    DurablePartition {
                        status: partition.status(),
                        exit_status: partition.exit_status().clone(),
                        counts: partition.counts(),
                        context_bytes: partition.context().encoded_len(),
                        context: partition.context().clone(),
                    },
                )
            })
            .collect(),
    };

    let capacity = repository.connection_capacity();
    repository.close().await?;

    Ok(Run {
        occupancy,
        observation,
        connections: capacity,
    })
}

/// Refuses a pool one connection short and confirms nothing was written.
async fn short_pool_is_refused_before_any_child(url: &str) -> Result<Stressed, Box<dyn Error>> {
    // A budget of eight children declares nine connections, which is what the
    // launcher revalidates the running pool against.
    let workers: u8 = 8;
    let required = u32::from(workers) + 1;
    let job = short_pool_job(workers, required)?;

    let clock = FixedClock::default();
    let short = PostgresJobRepository::connect(
        config_with_pool(url.to_owned(), required - 1)?,
        Arc::new(clock),
    )
    .await?;
    let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
    let (_, stop) = StopSource::new();
    let refusal = FlowLauncher::new(&short, &clock, &ids)
        .launch(&job, &JobParameters::new(), &stop)
        .await;
    let configured = short.connection_capacity();
    short.close().await?;

    let mut violations = Vec::new();
    let refused_as = match &refusal {
        Err(FlowRuntimeError::InsufficientPoolCapacity {
            required: reported,
            configured: seen,
        }) => {
            if *reported != required || *seen != required - 1 {
                violations.push(format!(
                    "the pool was {} short of the {required} the step derives and the refusal \
                     reported required {reported} against configured {seen}",
                    1,
                ));
            }
            format!("InsufficientPoolCapacity({reported}/{seen})")
        }
        Err(other) => {
            violations.push(format!(
                "a pool one connection short must be refused as InsufficientPoolCapacity and was \
                 refused as {other:?}",
            ));
            format!("{other:?}")
        }
        Ok(_) => {
            violations.push(
                "a pool one connection short of the derived requirement launched the step"
                    .to_owned(),
            );
            "launched".to_owned()
        }
    };

    // The refusal must be an absence in the database, not only a value. A
    // launch that had already created an instance or an execution would be a
    // partial launch behind an error.
    let residue = launch_residue(url, SHORT_POOL_JOB).await?;
    for (table, rows) in &residue {
        if *rows != 0 {
            violations.push(format!(
                "the refused launch left {rows} row(s) in {table}, so it started work before \
                 failing closed",
            ));
        }
    }

    // The same step against a pool of exactly the derived requirement must run,
    // so the refusal above is the bound and not a broken fixture.
    let sufficient = PostgresJobRepository::connect(
        config_with_pool(url.to_owned(), required)?,
        Arc::new(clock),
    )
    .await?;
    let accepted = FlowLauncher::new(&sufficient, &clock, &ids)
        .launch(&job, &JobParameters::new(), &stop)
        .await;
    sufficient.close().await?;
    let accepted_outcome = match &accepted {
        Ok(report) => format!("{:?}", report.outcome()),
        Err(error) => {
            violations.push(format!(
                "a pool of exactly the derived {required} connections must run the step and it \
                 failed with {error:?}",
            ));
            format!("{error:?}")
        }
    };

    Ok(Stressed {
        resource: "repository-connection-capacity",
        policy: "fail-closed",
        declared: u64::from(required),
        ceiling: u64::from(required),
        offered: u64::from(required - 1),
        peak: 0,
        admitted: 0,
        connections: configured,
        detail: Value::Null,
        violations,
    }
    .with_detail(json!({
        "derivation": "concurrent_children + 1",
        "concurrent_children": workers,
        "required_connections": required,
        "short_pool": required - 1,
        "refused_as": refused_as,
        "residue_after_refusal": residue,
        "sufficient_pool": required,
        "sufficient_pool_outcome": accepted_outcome,
    })))
}

/// Builds the partitioned step whose pool the report leaves one connection short.
///
/// The worker tasklet completes if it is ever reached, which it must not be:
/// the report's whole claim is that no child starts, so a worker that panicked
/// on entry would prove the same thing by a different route and would make a
/// refusal that arrived too late look like an infrastructure failure instead of
/// the bound not holding.
fn short_pool_job(workers: u8, required: u32) -> Result<FlowJob, Box<dyn Error>> {
    let name = JobName::new(SHORT_POOL_JOB)?;
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
            PartitionCount::new(2)?,
            PartitionBudget::new(workers, required)?,
        )))
        .with_sequence(
            manager.clone(),
            FlowTarget::Terminal(TerminalKind::Complete),
        )?
        .compile(&name, DefinitionRevision::new("v1")?)?;

    let entries = vec![entry("alpha")?, entry("beta")?];
    let partitioner = PartitionPlanFactory::new(move |_| Ok(entries.clone()));
    let factory_name = worker_name.clone();
    let factory = PartitionTaskletFactory::new(worker_name, move |_input| {
        TaskletStep::new(factory_name.clone(), Arc::new(RefusedTasklet))
    });
    Ok(FlowJob::new(name, plan)?.with_partitioned_tasklet(manager, partitioner, factory)?)
}

/// Counts what one job name left behind in the tables a launch writes first.
async fn launch_residue(
    url: &str,
    job_name: &str,
) -> Result<BTreeMap<String, i64>, Box<dyn Error>> {
    let mut residue = BTreeMap::new();
    for (table, statement) in [
        (
            "ob_job_instance",
            "SELECT count(*) FROM oxide_batch.ob_job_instance WHERE job_name = $1",
        ),
        (
            "ob_job_execution",
            "SELECT count(*) FROM oxide_batch.ob_job_execution execution \
             JOIN oxide_batch.ob_job_instance instance ON instance.id = execution.job_instance_id \
             WHERE instance.job_name = $1",
        ),
        (
            "ob_job_definition",
            "SELECT count(*) FROM oxide_batch.ob_job_definition WHERE job_name = $1",
        ),
    ] {
        residue.insert(
            table.to_owned(),
            resource_bounds::count(url, statement, job_name).await?,
        );
    }
    Ok(residue)
}

/// Reports every construction the framework must refuse and must accept.
///
/// These are the fail-closed half of the worker-assignment class, and they are
/// checked at the boundary and one past it. The boundary matters as much as the
/// refusal: a ceiling enforced one short is a different bound from the declared
/// one, and only the accepted case can tell the two apart.
fn construction_cells() -> Vec<Cell> {
    let mut cells = partition_construction_cells();
    cells.extend(split_construction_cells());
    cells.extend(pool_construction_cells());
    cells
}

/// Reports the partition-count and worker-budget constructions.
fn partition_construction_cells() -> Vec<Cell> {
    let mut cells = Vec::new();

    let ceiling = MAX_PARTITIONS;
    cells.push(Cell::new(
        "partitions-per-step",
        "at the ceiling",
        ceiling.into(),
        PartitionCount::new(ceiling).is_ok(),
        true,
    ));
    cells.push(Cell::new(
        "partitions-per-step",
        "one past the ceiling",
        u64::from(ceiling) + 1,
        PartitionCount::new(ceiling.saturating_add(1)).is_ok(),
        false,
    ));

    cells.push(Cell::new(
        "concurrent-partition-workers",
        "at the ceiling",
        MAX_PARTITION_WORKERS.into(),
        PartitionBudget::new(MAX_PARTITION_WORKERS, u32::from(MAX_PARTITION_WORKERS) + 1).is_ok(),
        true,
    ));
    cells.push(Cell::new(
        "concurrent-partition-workers",
        "one past the ceiling",
        u64::from(MAX_PARTITION_WORKERS) + 1,
        PartitionBudget::new(
            MAX_PARTITION_WORKERS.saturating_add(1),
            u32::from(MAX_PARTITION_WORKERS) + 2,
        )
        .is_ok(),
        false,
    ));
    cells.push(Cell::new(
        "concurrent-partition-workers",
        "zero workers",
        0,
        PartitionBudget::new(0, 2).is_ok(),
        false,
    ));
    cells.push(Cell::new(
        "repository-connection-capacity",
        "a declared pool one short of the derivation",
        u64::from(MAX_PARTITION_WORKERS),
        PartitionBudget::new(MAX_PARTITION_WORKERS, u32::from(MAX_PARTITION_WORKERS)).is_ok(),
        false,
    ));

    cells
}

/// Reports the split-shape and branch-budget constructions.
fn split_construction_cells() -> Vec<Cell> {
    let mut cells = Vec::new();
    let branches = u8::try_from(MAX_SPLIT_BRANCHES).unwrap_or(u8::MAX);
    cells.push(Cell::new(
        "concurrent-split-branches",
        "at the ceiling",
        branches.into(),
        SplitBudget::new(branches, u32::from(branches) + 1).is_ok(),
        true,
    ));
    cells.push(Cell::new(
        "concurrent-split-branches",
        "one past the ceiling",
        u64::from(branches) + 1,
        SplitBudget::new(branches.saturating_add(1), u32::from(branches) + 2).is_ok(),
        false,
    ));

    cells.push(Cell::new(
        "split-branches-per-node",
        "at the ceiling",
        MAX_SPLIT_BRANCHES as u64,
        compiles_split(MAX_SPLIT_BRANCHES, 1),
        true,
    ));
    cells.push(Cell::new(
        "split-branches-per-node",
        "one past the ceiling",
        MAX_SPLIT_BRANCHES as u64 + 1,
        compiles_split(MAX_SPLIT_BRANCHES + 1, 1),
        false,
    ));
    cells.push(Cell::new(
        "split-branches-per-node",
        "one branch, below the two a split means",
        1,
        compiles_split(1, 1),
        false,
    ));

    cells.push(Cell::new(
        "steps-per-split-branch",
        "at the ceiling",
        MAX_BRANCH_STEPS as u64,
        compiles_split(2, MAX_BRANCH_STEPS),
        true,
    ));
    cells.push(Cell::new(
        "steps-per-split-branch",
        "one past the ceiling",
        MAX_BRANCH_STEPS as u64 + 1,
        compiles_split(2, MAX_BRANCH_STEPS + 1),
        false,
    ));

    cells
}

/// Reports the adapter pool-size constructions.
fn pool_construction_cells() -> Vec<Cell> {
    vec![
        Cell::new(
            "repository-pool-size",
            "at the ceiling",
            1024,
            pool_size_accepted(1024),
            true,
        ),
        Cell::new(
            "repository-pool-size",
            "one past the ceiling",
            1025,
            pool_size_accepted(1025),
            false,
        ),
    ]
}

/// Reports whether a split of this shape compiles.
fn compiles_split(branches: usize, steps_per_branch: usize) -> bool {
    fn build(branches: usize, steps_per_branch: usize) -> Result<(), Box<dyn Error>> {
        let name = JobName::new("m5-resource-bound-shape")?;
        let prepare = NodeId::new("prepare")?;
        let split = NodeId::new("parallel")?;
        let join = NodeId::new("joined")?;
        let mut nodes = Vec::new();
        for branch in 0..branches {
            let mut steps = Vec::new();
            for step in 0..steps_per_branch {
                steps.push(StepNode::new(
                    NodeId::new(format!("b{branch}s{step}"))?,
                    StepName::new(format!("b{branch}s{step}"))?,
                    StepComponents::Tasklet(ComponentRevision::new("shape-v1")?),
                ));
            }
            nodes.push(SplitBranch::new(steps));
        }
        FlowGraph::new(prepare.clone())
            .with_node(FlowNode::step(StepNode::new(
                prepare.clone(),
                StepName::new("prepare")?,
                StepComponents::Tasklet(ComponentRevision::new("shape-v1")?),
            )))
            .with_node(FlowNode::split(SplitNode::new(
                split.clone(),
                nodes,
                join.clone(),
                SplitBudget::default(),
            )))
            .with_node(FlowNode::join(JoinNode::new(join.clone())))
            .with_sequence(prepare, FlowTarget::Node(split))?
            .with_sequence(join, FlowTarget::Terminal(TerminalKind::Complete))?
            .compile(&name, DefinitionRevision::new("v1")?)?;
        Ok(())
    }

    build(branches, steps_per_branch).is_ok()
}

/// Reports whether the adapter accepts one pool size.
fn pool_size_accepted(size: u32) -> bool {
    PostgresConfig::new("postgres://user:password@127.0.0.1:5432/db")
        .and_then(|config| config.with_pool_size(size))
        .is_ok()
}

/// Compares the durable record of the stressed run against the baseline.
fn compare(baseline: &Observation, stressed: &Observation) -> Equivalence {
    let mut violations = Vec::new();
    let mut compared = Vec::new();

    let mut agree = |field: &str, holds: bool| {
        compared.push(json!({ "field": field, "agrees": holds }));
        if !holds {
            violations.push(format!(
                "the stressed run and the sequential baseline disagree on {field}, so the \
                 concurrency changed a durable observation",
            ));
        }
    };

    agree("outcome", baseline.outcome == stressed.outcome);
    agree(
        "job-execution-status",
        baseline.job_status == stressed.job_status,
    );
    agree(
        "job-exit-status",
        baseline.job_exit_status == stressed.job_exit_status,
    );
    agree(
        "step-execution-status",
        baseline.parent_status == stressed.parent_status,
    );
    agree(
        "step-exit-status",
        baseline.parent_exit_status == stressed.parent_exit_status,
    );
    agree(
        "aggregate-execution-counts",
        baseline.parent_counts == stressed.parent_counts,
    );
    // The scope names the individual counters as well as the aggregate,
    // because an aggregate that matched while a retry counter drifted would be
    // the interesting failure and this comparison would not see it.
    agree(
        "read-write-commit-rollback-counters",
        baseline.parent_counts.read() == stressed.parent_counts.read()
            && baseline.parent_counts.processed() == stressed.parent_counts.processed()
            && baseline.parent_counts.written() == stressed.parent_counts.written()
            && baseline.parent_counts.filtered() == stressed.parent_counts.filtered()
            && baseline.parent_counts.committed() == stressed.parent_counts.committed()
            && baseline.parent_counts.rolled_back() == stressed.parent_counts.rolled_back(),
    );
    agree(
        "partition-execution-count",
        baseline.partitions.len() == stressed.partitions.len(),
    );
    agree(
        "step-execution-count",
        baseline.step_executions == stressed.step_executions,
    );
    agree(
        "partition-key-set",
        baseline.partitions.keys().eq(stressed.partitions.keys()),
    );
    agree(
        "partition-status-per-key",
        baseline.partitions.iter().all(|(key, partition)| {
            stressed
                .partitions
                .get(key)
                .is_some_and(|other| other.status == partition.status)
        }),
    );
    agree(
        "partition-counts-per-key",
        baseline.partitions.iter().all(|(key, partition)| {
            stressed
                .partitions
                .get(key)
                .is_some_and(|other| other.counts == partition.counts)
        }),
    );
    agree(
        "partition-context-per-key",
        baseline.partitions.iter().all(|(key, partition)| {
            stressed
                .partitions
                .get(key)
                .is_some_and(|other| other.context == partition.context)
        }),
    );

    violations.extend(shape_violations(stressed));

    Equivalence {
        baseline_workers: 1,
        stressed_workers: u64::from(MAX_PARTITION_WORKERS),
        partitions: stressed.partitions.len() as u64,
        compared,
        must_not_observe: Vec::new(),
        violations,
    }
}

/// Reports what the stressed run's own record must look like regardless.
///
/// The field comparison would hold between two runs that both did nothing, so
/// the shape of the durable record is required as well as its agreement with
/// the baseline.
fn shape_violations(stressed: &Observation) -> Vec<String> {
    let mut violations = Vec::new();

    if stressed.partitions.len() != usize::from(OFFERED_PARTITIONS) {
        violations.push(format!(
            "the stressed run recorded {} durable partitions and {OFFERED_PARTITIONS} were \
             offered, so a partition is missing or duplicated",
            stressed.partitions.len(),
        ));
    }
    if stressed
        .partitions
        .values()
        .any(|partition| partition.status != BatchStatus::Completed)
    {
        violations.push(
            "the stressed run left a partition in a non-terminal status, so a child did not \
             finish"
                .to_owned(),
        );
    }

    violations
}

/// Decides each durable regression the scope's comparison must not observe.
///
/// The scope names six: two are already computed from the stressed run's own
/// record (`duplicate-partition-execution`, `missing-partition`,
/// `unfinished-child`, `forged-execution-status`), one is a fresh database
/// round trip confirming the report's own cleanup left nothing behind for
/// either job (`leaked-durable-execution`), and one is read back from the
/// connection-capacity report's own residue check
/// (`partial-launch-after-rejection`). Each is reported as an explicit
/// `{condition, observed}` pair rather than folded into a free-form violation
/// list, so the runner can reconcile it by name rather than by pattern.
async fn must_not_observe_conditions(
    migrator_url: &str,
    connections: &Stressed,
    stressed: &Observation,
) -> Result<Vec<Value>, Box<dyn Error>> {
    let stressed_residue = launch_residue(migrator_url, STRESSED_JOB).await?;
    let baseline_residue = launch_residue(migrator_url, BASELINE_JOB).await?;
    let leaked =
        |residue: &BTreeMap<String, i64>| residue.get("ob_job_execution").copied() != Some(1);
    let partial_launch_after_rejection = connections
        .detail
        .pointer("/residue_after_refusal")
        .and_then(Value::as_object)
        .is_some_and(|residue| residue.values().any(|rows| rows.as_i64() != Some(0)));

    Ok(vec![
        json!({
            "condition": "duplicate-partition-execution",
            "observed": stressed.raw_partition_rows > stressed.partitions.len(),
        }),
        json!({
            "condition": "missing-partition",
            "observed": stressed.partitions.len() < usize::from(OFFERED_PARTITIONS),
        }),
        json!({
            "condition": "unfinished-child",
            "observed": stressed
                .partitions
                .values()
                .any(|partition| partition.status != BatchStatus::Completed),
        }),
        json!({
            "condition": "leaked-durable-execution",
            "observed": leaked(&stressed_residue) || leaked(&baseline_residue),
        }),
        json!({
            "condition": "forged-execution-status",
            "observed": stressed.job_status == BatchStatus::Completed
                && stressed
                    .partitions
                    .values()
                    .any(|partition| partition.status != BatchStatus::Completed),
        }),
        json!({
            "condition": "partial-launch-after-rejection",
            "observed": partial_launch_after_rejection,
        }),
    ])
}

/// Builds one bounded partition context and key.
fn entry(key: &str) -> Result<PartitionPlanEntry, Box<dyn Error>> {
    let context = ExecutionContext::from_json(
        format!(
            "{{\"format\":\"oxide-batch.execution-context\",\"format_version\":1,\
             \"schema\":\"m5.resource-bounds\",\"schema_version\":1,\
             \"payload\":{{\"key\":\"{key}\"}}}}"
        )
        .as_bytes(),
        StateLimits::new(4 * 1024, 16)?,
    )?;
    Ok(PartitionPlanEntry::new(PartitionKey::new(key)?, context)?)
}

/// A worker that holds the resource until its whole wave has arrived.
struct WaveTasklet {
    occupancy: Arc<Occupancy>,
    barrier: Option<Arc<Barrier>>,
}

impl Tasklet for WaveTasklet {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            self.occupancy.enter();
            if let Some(barrier) = &self.barrier {
                join_wave(barrier).await;
            }
            self.occupancy.leave();
            Ok(TaskletOutcome::Completed)
        })
    }
}

/// A worker whose step must never be reached because the launch failed closed.
struct RefusedTasklet;

impl Tasklet for RefusedTasklet {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async { Ok(TaskletOutcome::Completed) })
    }
}

/// One run of the partitioned step and everything it produced.
struct Run {
    occupancy: Arc<Occupancy>,
    observation: Observation,
    connections: u32,
}

/// The durable record of one run, as the comparison reads it.
struct Observation {
    outcome: String,
    job_status: BatchStatus,
    job_exit_status: ExitStatus,
    parent_status: BatchStatus,
    parent_exit_status: ExitStatus,
    parent_counts: ExecutionCounts,
    step_executions: usize,
    /// The number of durable partition rows the database actually returned,
    /// before they are keyed by partition key. A duplicate execution of one
    /// partition would collapse into `partitions` silently; this is what
    /// tells the two apart.
    raw_partition_rows: usize,
    partitions: BTreeMap<String, DurablePartition>,
}

/// One durable partition of a run.
#[derive(Eq, PartialEq)]
struct DurablePartition {
    status: BatchStatus,
    exit_status: ExitStatus,
    counts: ExecutionCounts,
    context_bytes: usize,
    context: ExecutionContext,
}

/// One saturated or refused resource and what the run observed of it.
struct Stressed {
    resource: &'static str,
    policy: &'static str,
    /// The bound the denominator declares for this resource.
    declared: u64,
    /// The bound this run was configured with, which is the one the peak is
    /// measured against. It is below the declared ceiling only where running at
    /// the declared ceiling leaves nothing to hold back.
    ceiling: u64,
    offered: u64,
    peak: u64,
    admitted: u64,
    connections: u32,
    detail: Value,
    violations: Vec<String>,
}

impl Stressed {
    /// Attaches the policy-specific detail this resource's evidence carries.
    fn with_detail(self, detail: Value) -> Self {
        Self { detail, ..self }
    }

    /// Renders what the retained evidence records for this resource.
    fn evidence(&self) -> Value {
        json!({
            "resource": self.resource,
            "overload_policy": self.policy,
            "declared_ceiling": self.declared,
            "configured_ceiling": self.ceiling,
            "offered_load": self.offered,
            "observed_peak_occupancy": self.peak,
            "admitted_total": self.admitted,
            "connection_capacity": self.connections,
            "rejections": u64::from(self.policy == "fail-closed"),
            "waits": self.admitted.saturating_sub(self.ceiling),
            "drops": 0,
            "detail": self.detail,
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

/// The durable comparison between the baseline and the stressed run.
struct Equivalence {
    baseline_workers: u64,
    stressed_workers: u64,
    partitions: u64,
    compared: Vec<Value>,
    /// The regressions the scope names, and whether this comparison actually
    /// observed each one. Attached after construction, since deciding some of
    /// them — whether the report's own cleanup left anything behind — needs a
    /// database round trip the comparison itself does not make.
    must_not_observe: Vec<Value>,
    violations: Vec<String>,
}

impl Equivalence {
    /// Attaches the `must_not_observe` regressions this comparison decided.
    fn with_must_not_observe(self, must_not_observe: Vec<Value>) -> Self {
        Self {
            must_not_observe,
            ..self
        }
    }

    /// Renders what the retained evidence records for the comparison.
    fn evidence(&self) -> Value {
        json!({
            "baseline_workers": self.baseline_workers,
            "stressed_workers": self.stressed_workers,
            "partitions": self.partitions,
            "fields_compared": self.compared,
            "must_not_observe": self.must_not_observe,
            "violations": self.violations,
            "passed": self.violations.is_empty(),
        })
    }
}
