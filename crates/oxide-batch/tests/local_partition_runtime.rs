//! In-memory execution evidence for bounded durable local partitions.

#![allow(clippy::panic)]

use std::error::Error;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

use oxide_batch::{
    BatchStatus, BoxFuture, Clock, ComponentRevision, DefinitionRevision, ExecutionContext,
    ExecutionCounts, ExitStatus, FlowExecutionOutcome, FlowGraph, FlowJob, FlowLauncher, FlowNode,
    FlowRuntimeError, FlowTarget, InMemoryJobRepository, JobName, JobParameters, JobRepository,
    NodeId, PartitionBudget, PartitionCount, PartitionFactoryError, PartitionKey,
    PartitionPlanEntry, PartitionPlanFactory, PartitionTaskletFactory, RepositoryDescriptor,
    RepositoryError, RepositoryUnitOfWork, SequentialIdGenerator, StateLimits, StepComponents,
    StepName, StepNode, StopSource, Tasklet, TaskletContext, TaskletError, TaskletOutcome,
    TaskletStep, TerminalKind,
};
use tokio::sync::{Barrier, Notify};

#[derive(Debug)]
struct FixedClock(SystemTime);

impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        self.0
    }
}

#[derive(Clone, Copy)]
enum WorkerOutcome {
    Complete,
    Fail,
    FailOnce,
    Panic,
    Unknown,
}

struct WorkerTasklet {
    calls: Arc<AtomicUsize>,
    outcome: WorkerOutcome,
}

impl Tasklet for WorkerTasklet {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            match self.outcome {
                WorkerOutcome::Fail => Err(TaskletError::new()),
                WorkerOutcome::FailOnce if call == 0 => Err(TaskletError::new()),
                WorkerOutcome::Complete | WorkerOutcome::FailOnce => Ok(TaskletOutcome::Completed),
                WorkerOutcome::Panic => panic!("partition panic fixture"),
                WorkerOutcome::Unknown => Ok(TaskletOutcome::CommitOutcomeUnknown),
            }
        })
    }
}

struct BoundedTasklet {
    active: Arc<AtomicUsize>,
    maximum: Arc<AtomicUsize>,
    barrier: Arc<Barrier>,
}

impl Tasklet for BoundedTasklet {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.maximum.fetch_max(active, Ordering::SeqCst);
            self.barrier.wait().await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(TaskletOutcome::Completed)
        })
    }
}

struct OrderedFailureTasklet {
    key: String,
    first: String,
    notify: Arc<Notify>,
}

#[derive(Debug, Eq, PartialEq)]
struct NormalizedPartition {
    key: String,
    status: BatchStatus,
    exit_status: ExitStatus,
    counts: ExecutionCounts,
    context: ExecutionContext,
}

#[derive(Debug, Eq, PartialEq)]
struct NormalizedObservation {
    job_status: BatchStatus,
    job_exit_status: ExitStatus,
    parent_status: BatchStatus,
    parent_exit_status: ExitStatus,
    parent_counts: ExecutionCounts,
    partitions: Vec<NormalizedPartition>,
}

struct LimitedRepository<'a>(&'a InMemoryJobRepository);

impl JobRepository for LimitedRepository<'_> {
    fn connection_capacity(&self) -> u32 {
        1
    }

    /// Delegates the capability declaration: this double narrows the
    /// connection budget, not what the deployment can do.
    fn descriptor(&self) -> RepositoryDescriptor {
        self.0.descriptor()
    }

    fn begin<'a>(
        &'a self,
    ) -> BoxFuture<'a, Result<Box<dyn RepositoryUnitOfWork + 'a>, RepositoryError>> {
        self.0.begin()
    }
}

struct AwaitCancellationTasklet {
    started: Arc<Notify>,
}

impl Tasklet for AwaitCancellationTasklet {
    fn execute<'a>(
        &'a self,
        context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            self.started.notify_one();
            context.stop_token().cancelled().await;
            Ok(TaskletOutcome::Stopped)
        })
    }
}

impl Tasklet for OrderedFailureTasklet {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            if self.key == self.first {
                self.notify.notify_one();
            } else {
                self.notify.notified().await;
            }
            Err(TaskletError::new())
        })
    }
}

fn infrastructure() -> (
    Arc<FixedClock>,
    Arc<SequentialIdGenerator>,
    InMemoryJobRepository,
) {
    let clock = Arc::new(FixedClock(
        SystemTime::UNIX_EPOCH + Duration::from_secs(100),
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

fn partition_plan_factory(keys: &[&str]) -> Result<PartitionPlanFactory, Box<dyn Error>> {
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

fn plan(
    name: &JobName,
    partitions: u16,
    concurrency: u8,
) -> Result<oxide_batch::CompiledExecutionPlan, Box<dyn Error>> {
    let manager = NodeId::new("partitioned")?;
    let worker = StepNode::new(
        NodeId::new("worker")?,
        StepName::new("worker")?,
        StepComponents::Tasklet(ComponentRevision::new("worker-v1")?),
    );
    Ok(FlowGraph::new(manager.clone())
        .with_node(FlowNode::partitioned_step(
            oxide_batch::PartitionedStepNode::new(
                manager.clone(),
                StepName::new("partitioned")?,
                worker,
                ComponentRevision::new("partitioner-v1")?,
                ComponentRevision::new("canonical-v1")?,
                PartitionCount::new(partitions)?,
                PartitionBudget::new(concurrency, u32::from(concurrency) + 1)?,
            ),
        ))
        .with_sequence(manager, FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(name, DefinitionRevision::new("v1")?)?)
}

fn worker_factory(
    calls: Arc<AtomicUsize>,
    select: impl Fn(&str) -> WorkerOutcome + Send + Sync + 'static,
) -> Result<PartitionTaskletFactory, Box<dyn Error>> {
    let step_name = StepName::new("worker")?;
    let factory_name = step_name.clone();
    Ok(PartitionTaskletFactory::new(step_name, move |input| {
        TaskletStep::new(
            factory_name.clone(),
            Arc::new(WorkerTasklet {
                calls: calls.clone(),
                outcome: select(input.key().as_str()),
            }),
        )
    }))
}

async fn normalized_success(
    name: &str,
    concurrency: u8,
) -> Result<NormalizedObservation, Box<dyn Error>> {
    let name = JobName::new(name)?;
    let job = FlowJob::new(name.clone(), plan(&name, 4, concurrency)?)?.with_partitioned_tasklet(
        NodeId::new("partitioned")?,
        partition_plan_factory(&["a", "b", "c", "d"])?,
        worker_factory(Arc::new(AtomicUsize::new(0)), |_| WorkerOutcome::Complete)?,
    )?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();
    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;
    let parent = report.step_executions().last().ok_or("missing parent")?;
    let mut unit = repository.begin().await?;
    let partitions = unit.step_partition_plan(parent.id()).await?;
    unit.rollback().await?;
    Ok(NormalizedObservation {
        job_status: report.job_execution().metadata().status(),
        job_exit_status: report.job_execution().metadata().exit_status().clone(),
        parent_status: parent.metadata().status(),
        parent_exit_status: parent.metadata().exit_status().clone(),
        parent_counts: parent.metadata().counts(),
        partitions: partitions
            .iter()
            .map(|partition| NormalizedPartition {
                key: partition.key().as_str().to_owned(),
                status: partition.status(),
                exit_status: partition.exit_status().clone(),
                counts: partition.counts(),
                context: partition.context().clone(),
            })
            .collect(),
    })
}

#[tokio::test(flavor = "current_thread")]
async fn concurrency_one_matches_parallel_durable_observations() -> Result<(), Box<dyn Error>> {
    assert_eq!(
        normalized_success("partition-sequential", 1).await?,
        normalized_success("partition-parallel", 4).await?
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn completion_order_does_not_change_failed_aggregate() -> Result<(), Box<dyn Error>> {
    async fn run(first: &str, name: &str) -> Result<FlowExecutionOutcome, Box<dyn Error>> {
        let name = JobName::new(name)?;
        let notify = Arc::new(Notify::new());
        let first = first.to_owned();
        let step_name = StepName::new("worker")?;
        let factory_name = step_name.clone();
        let factory = PartitionTaskletFactory::new(step_name, move |input| {
            TaskletStep::new(
                factory_name.clone(),
                Arc::new(OrderedFailureTasklet {
                    key: input.key().as_str().to_owned(),
                    first: first.clone(),
                    notify: notify.clone(),
                }),
            )
        });
        let job = FlowJob::new(name.clone(), plan(&name, 2, 2)?)?.with_partitioned_tasklet(
            NodeId::new("partitioned")?,
            partition_plan_factory(&["alpha", "zeta"])?,
            factory,
        )?;
        let (clock, ids, repository) = infrastructure();
        let (_, stop) = StopSource::new();
        let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
            .launch(&job, &JobParameters::new(), &stop)
            .await?;
        Ok(report.outcome().clone())
    }
    assert_eq!(
        run("alpha", "partition-order-alpha").await?,
        run("zeta", "partition-order-zeta").await?
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn completed_partition_is_not_rerun_on_restart() -> Result<(), Box<dyn Error>> {
    let name = JobName::new("partition-restart")?;
    let alpha_calls = Arc::new(AtomicUsize::new(0));
    let beta_calls = Arc::new(AtomicUsize::new(0));
    let step_name = StepName::new("worker")?;
    let factory_name = step_name.clone();
    let factory = PartitionTaskletFactory::new(step_name, {
        let alpha_calls = alpha_calls.clone();
        let beta_calls = beta_calls.clone();
        move |input| {
            let (calls, outcome) = if input.key().as_str() == "alpha" {
                (alpha_calls.clone(), WorkerOutcome::Complete)
            } else {
                (beta_calls.clone(), WorkerOutcome::FailOnce)
            };
            TaskletStep::new(
                factory_name.clone(),
                Arc::new(WorkerTasklet { calls, outcome }),
            )
        }
    });
    let job = FlowJob::new(name.clone(), plan(&name, 2, 2)?)?.with_partitioned_tasklet(
        NodeId::new("partitioned")?,
        partition_plan_factory(&["alpha", "beta"])?,
        factory,
    )?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();
    let first = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;
    assert!(matches!(first.outcome(), FlowExecutionOutcome::Failed(_)));
    let second = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;
    assert_eq!(second.outcome(), &FlowExecutionOutcome::Completed);
    assert_eq!(alpha_calls.load(Ordering::SeqCst), 1);
    assert_eq!(beta_calls.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn child_panic_is_durable_failure() -> Result<(), Box<dyn Error>> {
    let name = JobName::new("partition-panic")?;
    let job = FlowJob::new(name.clone(), plan(&name, 2, 2)?)?.with_partitioned_tasklet(
        NodeId::new("partitioned")?,
        partition_plan_factory(&["alpha", "beta"])?,
        worker_factory(Arc::new(AtomicUsize::new(0)), |key| {
            if key == "alpha" {
                WorkerOutcome::Panic
            } else {
                WorkerOutcome::Complete
            }
        })?,
    )?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();
    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;
    assert!(matches!(report.outcome(), FlowExecutionOutcome::Failed(_)));
    assert_eq!(
        report.job_execution().metadata().status(),
        BatchStatus::Failed
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn cancellation_before_pending_worker_skips_its_factory() -> Result<(), Box<dyn Error>> {
    let name = JobName::new("partition-prestart-cancel")?;
    let factory_calls = Arc::new(AtomicUsize::new(0));
    let step_name = StepName::new("worker")?;
    let factory_name = step_name.clone();
    let factory = PartitionTaskletFactory::new(step_name, {
        let factory_calls = factory_calls.clone();
        move |_input| {
            factory_calls.fetch_add(1, Ordering::SeqCst);
            TaskletStep::new(
                factory_name.clone(),
                Arc::new(WorkerTasklet {
                    calls: Arc::new(AtomicUsize::new(0)),
                    outcome: WorkerOutcome::Fail,
                }),
            )
        }
    });
    let job = FlowJob::new(name.clone(), plan(&name, 2, 1)?)?.with_partitioned_tasklet(
        NodeId::new("partitioned")?,
        partition_plan_factory(&["alpha", "beta"])?,
        factory,
    )?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();
    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;
    assert!(matches!(report.outcome(), FlowExecutionOutcome::Failed(_)));
    assert_eq!(factory_calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn unknown_partition_blocks_parent_aggregation() -> Result<(), Box<dyn Error>> {
    let name = JobName::new("partition-unknown")?;
    let job = FlowJob::new(name.clone(), plan(&name, 1, 1)?)?.with_partitioned_tasklet(
        NodeId::new("partitioned")?,
        partition_plan_factory(&["alpha"])?,
        worker_factory(Arc::new(AtomicUsize::new(0)), |_| WorkerOutcome::Unknown)?,
    )?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();
    let result = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await;
    let Err(error) = result else {
        return Err("UNKNOWN produced a final report".into());
    };
    assert!(matches!(
        error,
        FlowRuntimeError::UnresolvedPartitionOutcome { .. }
    ));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn worker_concurrency_never_exceeds_manifest_bound() -> Result<(), Box<dyn Error>> {
    let name = JobName::new("partition-bound")?;
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(2));
    let step_name = StepName::new("worker")?;
    let factory_name = step_name.clone();
    let factory = PartitionTaskletFactory::new(step_name, {
        let active = active.clone();
        let maximum = maximum.clone();
        let barrier = barrier.clone();
        move |_input| {
            TaskletStep::new(
                factory_name.clone(),
                Arc::new(BoundedTasklet {
                    active: active.clone(),
                    maximum: maximum.clone(),
                    barrier: barrier.clone(),
                }),
            )
        }
    });
    let job = FlowJob::new(name.clone(), plan(&name, 4, 2)?)?.with_partitioned_tasklet(
        NodeId::new("partitioned")?,
        partition_plan_factory(&["a", "b", "c", "d"])?,
        factory,
    )?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();
    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;
    assert_eq!(report.outcome(), &FlowExecutionOutcome::Completed);
    assert_eq!(maximum.load(Ordering::SeqCst), 2);
    assert_eq!(active.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn repository_capacity_is_revalidated_before_launch() -> Result<(), Box<dyn Error>> {
    let name = JobName::new("partition-insufficient-pool")?;
    let job = FlowJob::new(name.clone(), plan(&name, 2, 2)?)?.with_partitioned_tasklet(
        NodeId::new("partitioned")?,
        partition_plan_factory(&["a", "b"])?,
        worker_factory(Arc::new(AtomicUsize::new(0)), |_| WorkerOutcome::Complete)?,
    )?;
    let (clock, ids, repository) = infrastructure();
    let limited = LimitedRepository(&repository);
    let (_, stop) = StopSource::new();
    assert!(matches!(
        FlowLauncher::new(&limited, clock.as_ref(), ids.as_ref())
            .launch(&job, &JobParameters::new(), &stop)
            .await,
        Err(FlowRuntimeError::InsufficientPoolCapacity {
            required: 3,
            configured: 1
        })
    ));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn parent_stop_cancels_and_joins_active_workers() -> Result<(), Box<dyn Error>> {
    let name = JobName::new("partition-parent-stop")?;
    let started = Arc::new(Notify::new());
    let step_name = StepName::new("worker")?;
    let factory_name = step_name.clone();
    let factory = PartitionTaskletFactory::new(step_name, {
        let started = started.clone();
        move |_input| {
            TaskletStep::new(
                factory_name.clone(),
                Arc::new(AwaitCancellationTasklet {
                    started: started.clone(),
                }),
            )
        }
    });
    let job = FlowJob::new(name.clone(), plan(&name, 2, 2)?)?.with_partitioned_tasklet(
        NodeId::new("partitioned")?,
        partition_plan_factory(&["a", "b"])?,
        factory,
    )?;
    let (clock, ids, repository) = infrastructure();
    let (stop_source, stop) = StopSource::new();
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());
    let parameters = JobParameters::new();
    let launch = launcher.launch(&job, &parameters, &stop);
    let request = async {
        started.notified().await;
        stop_source.request_stop();
    };
    let (report, ()) = tokio::join!(launch, request);
    let report = report?;
    assert_eq!(report.outcome(), &FlowExecutionOutcome::Stopped);
    assert_eq!(
        report.job_execution().metadata().status(),
        BatchStatus::Stopped
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn aggregate_commit_unknown_is_resolved_by_durable_inspection() -> Result<(), Box<dyn Error>>
{
    let name = JobName::new("partition-aggregate-unknown")?;
    let job = FlowJob::new(name.clone(), plan(&name, 2, 2)?)?.with_partitioned_tasklet(
        NodeId::new("partitioned")?,
        partition_plan_factory(&["a", "b"])?,
        worker_factory(Arc::new(AtomicUsize::new(0)), |_| WorkerOutcome::Complete)?,
    )?;
    let (clock, ids, repository) = infrastructure();
    repository.inject_next_partition_aggregate_commit_unknown();
    let (_, stop) = StopSource::new();
    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;
    assert_eq!(report.outcome(), &FlowExecutionOutcome::Completed);
    assert_eq!(
        report.job_execution().metadata().status(),
        BatchStatus::Completed
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn repeated_partition_runs_leave_no_active_worker() -> Result<(), Box<dyn Error>> {
    for index in 0..32 {
        let name = format!("partition-repeat-{index}");
        let observation = normalized_success(&name, 4).await?;
        assert_eq!(observation.job_status, BatchStatus::Completed);
        assert!(
            observation
                .partitions
                .iter()
                .all(|partition| partition.status == BatchStatus::Completed)
        );
    }
    Ok(())
}
