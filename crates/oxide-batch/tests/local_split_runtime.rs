//! In-memory execution evidence for the bounded M4 parallel-step slice.

#![allow(clippy::panic)]

use std::error::Error;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

use oxide_batch::{
    BatchStatus, BoxFuture, Clock, ComponentRevision, DefinitionRevision, ExecutionCounts,
    ExitCode, ExitStatus, FlowExecutionOutcome, FlowGraph, FlowJob, FlowLauncher, FlowNode,
    FlowRuntimeError, FlowTarget, FlowTransitionKind, InMemoryJobRepository, JobName,
    JobParameters, JobRepository, JoinNode, LocalFailurePolicy, NodeId, RepositoryDescriptor,
    RepositoryError, RepositoryUnitOfWork, SequentialIdGenerator, SplitBranch, SplitBudget,
    SplitNode, StepComponents, StepName, StepNode, StopSource, Tasklet, TaskletContext,
    TaskletError, TaskletOutcome, TaskletStep, TaskletStepFactory, TerminalKind,
};
use tokio::sync::{Barrier, Notify};

#[derive(Debug)]
struct FixedClock(SystemTime);

impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        self.0
    }
}

struct OutcomeTasklet {
    calls: Arc<AtomicUsize>,
    outcome: Outcome,
}

#[derive(Clone, Copy)]
enum Outcome {
    Complete,
    CompleteWith(&'static str),
    FailOnce,
    Panic,
    Unknown,
}

impl Tasklet for OutcomeTasklet {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            match self.outcome {
                Outcome::CompleteWith(code) => Ok(TaskletOutcome::CompletedWith(ExitStatus::new(
                    ExitCode::new(code).map_err(TaskletError::from_error)?,
                ))),
                Outcome::FailOnce if call == 0 => Err(TaskletError::new()),
                Outcome::Complete | Outcome::FailOnce => Ok(TaskletOutcome::Completed),
                Outcome::Panic => panic!("split branch panic fixture"),
                Outcome::Unknown => Ok(TaskletOutcome::CommitOutcomeUnknown),
            }
        })
    }
}

struct BarrierTasklet {
    active: Arc<AtomicUsize>,
    maximum: Arc<AtomicUsize>,
    barrier: Arc<Barrier>,
}

struct AwaitCancellationTasklet {
    started: Arc<Notify>,
    observed: Arc<AtomicUsize>,
}

impl Tasklet for AwaitCancellationTasklet {
    fn execute<'a>(
        &'a self,
        context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            self.started.notify_one();
            context.stop_token().cancelled().await;
            self.observed.fetch_add(1, Ordering::SeqCst);
            Ok(TaskletOutcome::Stopped)
        })
    }
}

/// Records peak and residual branch occupancy without blocking.
struct OccupancyTasklet {
    active: Arc<AtomicUsize>,
    maximum: Arc<AtomicUsize>,
}

impl Tasklet for OccupancyTasklet {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.maximum.fetch_max(active, Ordering::SeqCst);
            tokio::task::yield_now().await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(TaskletOutcome::Completed)
        })
    }
}

/// Fails after releasing a sibling that then samples the branch stop token.
struct SignallingFailureTasklet {
    released: Arc<Notify>,
}

impl Tasklet for SignallingFailureTasklet {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            self.released.notify_one();
            Err(TaskletError::new())
        })
    }
}

/// Records whether a sibling failure cancelled this branch before it resumed.
struct DrainObserverTasklet {
    released: Arc<Notify>,
    stop_requested: Arc<AtomicUsize>,
}

impl Tasklet for DrainObserverTasklet {
    fn execute<'a>(
        &'a self,
        context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            self.released.notified().await;
            if context.stop_token().is_stop_requested() {
                self.stop_requested.fetch_add(1, Ordering::SeqCst);
            }
            Ok(TaskletOutcome::Completed)
        })
    }
}

/// Fails only after the named branch has failed first.
struct OrderedFailureTasklet {
    name: &'static str,
    first: String,
    notify: Arc<Notify>,
}

impl Tasklet for OrderedFailureTasklet {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            if self.name == self.first {
                self.notify.notify_one();
            } else {
                self.notify.notified().await;
            }
            Err(TaskletError::new())
        })
    }
}

/// Presents a pool smaller than the split budget requires.
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

#[derive(Debug, Eq, PartialEq)]
struct NormalizedStep {
    step_name: String,
    status: BatchStatus,
    exit_status: ExitStatus,
    counts: ExecutionCounts,
}

#[derive(Debug, Eq, PartialEq)]
struct NormalizedDecision {
    sequence: u64,
    source_node_id: String,
    kind: FlowTransitionKind,
    observed_outcome: String,
    target: FlowTarget,
    input_digest: [u8; 32],
}

#[derive(Debug, Eq, PartialEq)]
struct NormalizedObservation {
    job_status: BatchStatus,
    job_exit_status: ExitStatus,
    steps: Vec<NormalizedStep>,
    decisions: Vec<NormalizedDecision>,
}

impl NormalizedObservation {
    /// Drops the plan-fingerprint-derived digests before comparison.
    ///
    /// `MaxParallelBranches` is restart-relevant and therefore participates in
    /// the plan fingerprint, so two runs that differ only in configured
    /// concurrency legitimately record different decision digests. Every other
    /// durable observation must still be identical.
    fn without_decision_digests(mut self) -> Self {
        for decision in &mut self.decisions {
            decision.input_digest = [0; 32];
        }
        self
    }
}

impl Tasklet for BarrierTasklet {
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

fn step_node(id: &str) -> Result<StepNode, Box<dyn Error>> {
    Ok(StepNode::new(
        NodeId::new(id)?,
        StepName::new(id)?,
        StepComponents::Tasklet(ComponentRevision::new(format!("{id}-v1"))?),
    ))
}

fn split_plan(
    name: &JobName,
    concurrency: u8,
) -> Result<oxide_batch::CompiledExecutionPlan, Box<dyn Error>> {
    split_plan_with(
        name,
        &["first", "second"],
        concurrency,
        LocalFailurePolicy::default(),
    )
}

fn split_plan_with(
    name: &JobName,
    branches: &[&str],
    concurrency: u8,
    failure_policy: LocalFailurePolicy,
) -> Result<oxide_batch::CompiledExecutionPlan, Box<dyn Error>> {
    let prepare = NodeId::new("prepare")?;
    let split = NodeId::new("parallel")?;
    let join = NodeId::new("joined")?;
    let branch_nodes = branches
        .iter()
        .map(|branch| Ok(SplitBranch::new(vec![step_node(branch)?])))
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    Ok(FlowGraph::new(prepare.clone())
        .with_node(FlowNode::step(step_node("prepare")?))
        .with_node(FlowNode::split(
            SplitNode::new(
                split.clone(),
                branch_nodes,
                join.clone(),
                SplitBudget::new(concurrency, u32::from(concurrency) + 1)?,
            )
            .with_failure_policy(failure_policy),
        ))
        .with_node(FlowNode::join(JoinNode::new(join.clone())))
        .with_sequence(prepare, FlowTarget::Node(split))?
        .with_sequence(join, FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(name, DefinitionRevision::new("v1")?)?)
}

fn infrastructure() -> (
    Arc<FixedClock>,
    Arc<SequentialIdGenerator>,
    InMemoryJobRepository,
) {
    let clock = Arc::new(FixedClock(SystemTime::UNIX_EPOCH + Duration::from_secs(10)));
    let ids = Arc::new(SequentialIdGenerator::new(NonZeroU64::MIN));
    let repository = InMemoryJobRepository::new(clock.clone(), ids.clone());
    (clock, ids, repository)
}

fn concrete_step(
    name: &str,
    calls: Arc<AtomicUsize>,
    outcome: Outcome,
) -> Result<TaskletStep, Box<dyn Error>> {
    Ok(TaskletStep::new(
        StepName::new(name)?,
        Arc::new(OutcomeTasklet { calls, outcome }),
    ))
}

fn factory(
    name: &str,
    calls: Arc<AtomicUsize>,
    outcome: Outcome,
) -> Result<TaskletStepFactory, Box<dyn Error>> {
    let step_name = StepName::new(name)?;
    let factory_name = step_name.clone();
    Ok(TaskletStepFactory::new(step_name, move || {
        TaskletStep::new(
            factory_name.clone(),
            Arc::new(OutcomeTasklet {
                calls: Arc::clone(&calls),
                outcome,
            }),
        )
    }))
}

/// Runs one successful four-branch split and reads its durable observation.
///
/// Every branch also tracks live occupancy, so each call asserts that the run
/// never exceeded the configured ceiling and left no branch active. Step rows
/// are ordered by logical step name rather than by insertion, so the returned
/// observation is independent of branch completion order.
async fn normalized_success(
    name: &str,
    concurrency: u8,
) -> Result<NormalizedObservation, Box<dyn Error>> {
    let branches = ["alpha", "beta", "gamma", "delta"];
    let name = JobName::new(name)?;
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let mut job = FlowJob::new(
        name.clone(),
        split_plan_with(&name, &branches, concurrency, LocalFailurePolicy::default())?,
    )?
    .with_tasklet_step(
        NodeId::new("prepare")?,
        concrete_step("prepare", Arc::new(AtomicUsize::new(0)), Outcome::Complete)?,
    )?;
    for branch in branches {
        let step_name = StepName::new(branch)?;
        let factory_name = step_name.clone();
        let active = Arc::clone(&active);
        let maximum = Arc::clone(&maximum);
        job = job.with_split_tasklet_factory(
            NodeId::new(branch)?,
            TaskletStepFactory::new(step_name, move || {
                TaskletStep::new(
                    factory_name.clone(),
                    Arc::new(OccupancyTasklet {
                        active: Arc::clone(&active),
                        maximum: Arc::clone(&maximum),
                    }),
                )
            }),
        )?;
    }
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();
    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert!(maximum.load(Ordering::SeqCst) <= usize::from(concurrency));
    let execution_id = report.job_execution().id();

    let mut unit = repository.begin().await?;
    let steps = unit.step_executions(execution_id).await?;
    let decisions = unit.flow_decisions(execution_id).await?;
    unit.rollback().await?;

    let mut steps = steps
        .iter()
        .map(|step| NormalizedStep {
            step_name: step.step_name().as_str().to_owned(),
            status: step.metadata().status(),
            exit_status: step.metadata().exit_status().clone(),
            counts: step.metadata().counts(),
        })
        .collect::<Vec<_>>();
    steps.sort_by(|left, right| left.step_name.cmp(&right.step_name));
    Ok(NormalizedObservation {
        job_status: report.job_execution().metadata().status(),
        job_exit_status: report.job_execution().metadata().exit_status().clone(),
        steps,
        decisions: decisions
            .iter()
            .map(|decision| NormalizedDecision {
                sequence: decision.sequence().get(),
                source_node_id: decision.source_node_id().as_str().to_owned(),
                kind: decision.kind(),
                observed_outcome: decision.observed_outcome().as_str().to_owned(),
                target: decision.target().clone(),
                input_digest: *decision.input_digest(),
            })
            .collect(),
    })
}

#[tokio::test(flavor = "current_thread")]
async fn parent_joins_every_branch_before_aggregating() -> Result<(), Box<dyn Error>> {
    let name = JobName::new("parallel-join")?;
    let plan = split_plan(&name, 2)?;
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(2));
    let mut job = FlowJob::new(name, plan)?.with_tasklet_step(
        NodeId::new("prepare")?,
        concrete_step("prepare", Arc::new(AtomicUsize::new(0)), Outcome::Complete)?,
    )?;
    for id in ["first", "second"] {
        let step_name = StepName::new(id)?;
        let factory_name = step_name.clone();
        let active = Arc::clone(&active);
        let maximum = Arc::clone(&maximum);
        let barrier = Arc::clone(&barrier);
        job = job.with_split_tasklet_factory(
            NodeId::new(id)?,
            TaskletStepFactory::new(step_name, move || {
                TaskletStep::new(
                    factory_name.clone(),
                    Arc::new(BarrierTasklet {
                        active: Arc::clone(&active),
                        maximum: Arc::clone(&maximum),
                        barrier: Arc::clone(&barrier),
                    }),
                )
            }),
        )?;
    }
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();

    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;

    assert_eq!(report.outcome(), &FlowExecutionOutcome::Completed);
    assert_eq!(maximum.load(Ordering::SeqCst), 2);
    assert_eq!(report.step_executions().len(), 3);
    assert_eq!(report.decisions().len(), 2);
    assert_eq!(
        report.decisions()[1].kind(),
        FlowTransitionKind::SplitAggregate
    );
    assert_eq!(report.decisions()[1].source_node_id().as_str(), "joined");
    assert_eq!(
        report.job_execution().metadata().status(),
        BatchStatus::Completed
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn branch_aggregation_is_deterministic_in_declared_order() -> Result<(), Box<dyn Error>> {
    let name = JobName::new("declared-order")?;
    let plan = split_plan(&name, 1)?;
    let job = FlowJob::new(name, plan)?
        .with_tasklet_step(
            NodeId::new("prepare")?,
            concrete_step("prepare", Arc::new(AtomicUsize::new(0)), Outcome::Complete)?,
        )?
        .with_split_tasklet_factory(
            NodeId::new("first")?,
            factory(
                "first",
                Arc::new(AtomicUsize::new(0)),
                Outcome::CompleteWith("FIRST"),
            )?,
        )?
        .with_split_tasklet_factory(
            NodeId::new("second")?,
            factory(
                "second",
                Arc::new(AtomicUsize::new(0)),
                Outcome::CompleteWith("SECOND"),
            )?,
        )?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();

    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;

    assert_eq!(report.outcome(), &FlowExecutionOutcome::Completed);
    assert_eq!(report.decisions()[1].observed_outcome().as_str(), "FIRST");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn completed_branch_is_reused_on_restart() -> Result<(), Box<dyn Error>> {
    let name = JobName::new("split-restart")?;
    let plan = split_plan(&name, 2)?;
    let first_calls = Arc::new(AtomicUsize::new(0));
    let second_calls = Arc::new(AtomicUsize::new(0));
    let job = FlowJob::new(name, plan)?
        .with_tasklet_step(
            NodeId::new("prepare")?,
            concrete_step("prepare", Arc::new(AtomicUsize::new(0)), Outcome::Complete)?,
        )?
        .with_split_tasklet_factory(
            NodeId::new("first")?,
            factory("first", Arc::clone(&first_calls), Outcome::Complete)?,
        )?
        .with_split_tasklet_factory(
            NodeId::new("second")?,
            factory("second", Arc::clone(&second_calls), Outcome::FailOnce)?,
        )?;
    let (clock, ids, repository) = infrastructure();
    let (_, first_stop) = StopSource::new();
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());

    let first = launcher
        .launch(&job, &JobParameters::new(), &first_stop)
        .await?;
    assert!(matches!(first.outcome(), FlowExecutionOutcome::Failed(_)));
    let (_, second_stop) = StopSource::new();
    let second = launcher
        .launch(&job, &JobParameters::new(), &second_stop)
        .await?;

    assert_eq!(second.outcome(), &FlowExecutionOutcome::Completed);
    assert_eq!(first_calls.load(Ordering::SeqCst), 1);
    assert_eq!(second_calls.load(Ordering::SeqCst), 2);
    assert_eq!(second.step_executions().len(), 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn unknown_branch_makes_the_parent_unknown() -> Result<(), Box<dyn Error>> {
    let name = JobName::new("split-unknown")?;
    let plan = split_plan(&name, 2)?;
    let job = FlowJob::new(name, plan)?
        .with_tasklet_step(
            NodeId::new("prepare")?,
            concrete_step("prepare", Arc::new(AtomicUsize::new(0)), Outcome::Complete)?,
        )?
        .with_split_tasklet_factory(
            NodeId::new("first")?,
            factory("first", Arc::new(AtomicUsize::new(0)), Outcome::Unknown)?,
        )?
        .with_split_tasklet_factory(
            NodeId::new("second")?,
            factory("second", Arc::new(AtomicUsize::new(0)), Outcome::Complete)?,
        )?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();

    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;

    assert_eq!(report.outcome(), &FlowExecutionOutcome::Unknown);
    assert_eq!(
        report.job_execution().metadata().status(),
        BatchStatus::Unknown
    );
    assert_eq!(report.decisions().len(), 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn cancel_siblings_still_joins_every_branch() -> Result<(), Box<dyn Error>> {
    let name = JobName::new("split-cancel-siblings")?;
    let plan = split_plan(&name, 2)?;
    let observed = Arc::new(AtomicUsize::new(0));
    let first_name = StepName::new("first")?;
    let factory_name = first_name.clone();
    let observed_by_factory = Arc::clone(&observed);
    let job = FlowJob::new(name, plan)?
        .with_tasklet_step(
            NodeId::new("prepare")?,
            concrete_step("prepare", Arc::new(AtomicUsize::new(0)), Outcome::Complete)?,
        )?
        .with_split_tasklet_factory(
            NodeId::new("first")?,
            TaskletStepFactory::new(first_name, move || {
                TaskletStep::new(
                    factory_name.clone(),
                    Arc::new(AwaitCancellationTasklet {
                        started: Arc::new(Notify::new()),
                        observed: Arc::clone(&observed_by_factory),
                    }),
                )
            }),
        )?
        .with_split_tasklet_factory(
            NodeId::new("second")?,
            factory("second", Arc::new(AtomicUsize::new(0)), Outcome::FailOnce)?,
        )?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();

    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;

    assert!(matches!(report.outcome(), FlowExecutionOutcome::Failed(_)));
    assert_eq!(observed.load(Ordering::SeqCst), 1);
    assert_eq!(report.step_executions().len(), 3);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn drain_siblings_lets_every_branch_reach_its_boundary() -> Result<(), Box<dyn Error>> {
    let name = JobName::new("split-drain-siblings")?;
    let plan = split_plan_with(
        &name,
        &["first", "second"],
        2,
        LocalFailurePolicy::DrainSiblings,
    )?;
    let released = Arc::new(Notify::new());
    let stop_requested = Arc::new(AtomicUsize::new(0));
    let first_name = StepName::new("first")?;
    let first_factory_name = first_name.clone();
    let released_by_observer = Arc::clone(&released);
    let stop_requested_by_observer = Arc::clone(&stop_requested);
    let second_name = StepName::new("second")?;
    let second_factory_name = second_name.clone();
    let released_by_failure = Arc::clone(&released);
    let job = FlowJob::new(name, plan)?
        .with_tasklet_step(
            NodeId::new("prepare")?,
            concrete_step("prepare", Arc::new(AtomicUsize::new(0)), Outcome::Complete)?,
        )?
        .with_split_tasklet_factory(
            NodeId::new("first")?,
            TaskletStepFactory::new(first_name, move || {
                TaskletStep::new(
                    first_factory_name.clone(),
                    Arc::new(DrainObserverTasklet {
                        released: Arc::clone(&released_by_observer),
                        stop_requested: Arc::clone(&stop_requested_by_observer),
                    }),
                )
            }),
        )?
        .with_split_tasklet_factory(
            NodeId::new("second")?,
            TaskletStepFactory::new(second_name, move || {
                TaskletStep::new(
                    second_factory_name.clone(),
                    Arc::new(SignallingFailureTasklet {
                        released: Arc::clone(&released_by_failure),
                    }),
                )
            }),
        )?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();

    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;

    assert!(matches!(report.outcome(), FlowExecutionOutcome::Failed(_)));
    assert_eq!(stop_requested.load(Ordering::SeqCst), 0);
    let drained = report
        .step_executions()
        .iter()
        .find(|step| step.step_name().as_str() == "first")
        .ok_or("drained branch is missing")?;
    assert_eq!(drained.metadata().status(), BatchStatus::Completed);
    assert_eq!(report.step_executions().len(), 3);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn branch_panic_is_durable_failure() -> Result<(), Box<dyn Error>> {
    let name = JobName::new("split-panic")?;
    let plan = split_plan(&name, 2)?;
    let job = FlowJob::new(name, plan)?
        .with_tasklet_step(
            NodeId::new("prepare")?,
            concrete_step("prepare", Arc::new(AtomicUsize::new(0)), Outcome::Complete)?,
        )?
        .with_split_tasklet_factory(
            NodeId::new("first")?,
            factory("first", Arc::new(AtomicUsize::new(0)), Outcome::Panic)?,
        )?
        .with_split_tasklet_factory(
            NodeId::new("second")?,
            factory("second", Arc::new(AtomicUsize::new(0)), Outcome::Complete)?,
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
    let panicked = report
        .step_executions()
        .iter()
        .find(|step| step.step_name().as_str() == "first")
        .ok_or("panicking branch is missing")?;
    assert_eq!(panicked.metadata().status(), BatchStatus::Failed);
    assert!(panicked.metadata().failure().is_some());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn branch_concurrency_never_exceeds_manifest_bound() -> Result<(), Box<dyn Error>> {
    let branches = ["alpha", "beta", "gamma", "delta"];
    let name = JobName::new("split-bound")?;
    let plan = split_plan_with(&name, &branches, 2, LocalFailurePolicy::default())?;
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(2));
    let mut job = FlowJob::new(name, plan)?.with_tasklet_step(
        NodeId::new("prepare")?,
        concrete_step("prepare", Arc::new(AtomicUsize::new(0)), Outcome::Complete)?,
    )?;
    for branch in branches {
        let step_name = StepName::new(branch)?;
        let factory_name = step_name.clone();
        let active = Arc::clone(&active);
        let maximum = Arc::clone(&maximum);
        let barrier = Arc::clone(&barrier);
        job = job.with_split_tasklet_factory(
            NodeId::new(branch)?,
            TaskletStepFactory::new(step_name, move || {
                TaskletStep::new(
                    factory_name.clone(),
                    Arc::new(BarrierTasklet {
                        active: Arc::clone(&active),
                        maximum: Arc::clone(&maximum),
                        barrier: Arc::clone(&barrier),
                    }),
                )
            }),
        )?;
    }
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
    let name = JobName::new("split-insufficient-pool")?;
    let plan = split_plan(&name, 2)?;
    let job = FlowJob::new(name, plan)?
        .with_tasklet_step(
            NodeId::new("prepare")?,
            concrete_step("prepare", Arc::new(AtomicUsize::new(0)), Outcome::Complete)?,
        )?
        .with_split_tasklet_factory(
            NodeId::new("first")?,
            factory("first", Arc::new(AtomicUsize::new(0)), Outcome::Complete)?,
        )?
        .with_split_tasklet_factory(
            NodeId::new("second")?,
            factory("second", Arc::new(AtomicUsize::new(0)), Outcome::Complete)?,
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
async fn parent_stop_cancels_and_joins_active_branches() -> Result<(), Box<dyn Error>> {
    let name = JobName::new("split-parent-stop")?;
    let plan = split_plan(&name, 2)?;
    let started = Arc::new(Notify::new());
    let observed = Arc::new(AtomicUsize::new(0));
    let mut job = FlowJob::new(name, plan)?.with_tasklet_step(
        NodeId::new("prepare")?,
        concrete_step("prepare", Arc::new(AtomicUsize::new(0)), Outcome::Complete)?,
    )?;
    for branch in ["first", "second"] {
        let step_name = StepName::new(branch)?;
        let factory_name = step_name.clone();
        let started = Arc::clone(&started);
        let observed = Arc::clone(&observed);
        job = job.with_split_tasklet_factory(
            NodeId::new(branch)?,
            TaskletStepFactory::new(step_name, move || {
                TaskletStep::new(
                    factory_name.clone(),
                    Arc::new(AwaitCancellationTasklet {
                        started: Arc::clone(&started),
                        observed: Arc::clone(&observed),
                    }),
                )
            }),
        )?;
    }
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
    assert_eq!(observed.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn concurrency_one_matches_parallel_durable_observations() -> Result<(), Box<dyn Error>> {
    // The same job name and inputs run against two isolated repositories, so
    // the only declared difference is the configured branch concurrency.
    assert_eq!(
        normalized_success("split-equivalence", 1)
            .await?
            .without_decision_digests(),
        normalized_success("split-equivalence", 4)
            .await?
            .without_decision_digests()
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn completion_order_does_not_change_failed_aggregate() -> Result<(), Box<dyn Error>> {
    async fn run(first: &str, name: &str) -> Result<NormalizedObservation, Box<dyn Error>> {
        let name = JobName::new(name)?;
        let plan = split_plan_with(
            &name,
            &["first", "second"],
            2,
            LocalFailurePolicy::DrainSiblings,
        )?;
        let notify = Arc::new(Notify::new());
        let mut job = FlowJob::new(name, plan)?.with_tasklet_step(
            NodeId::new("prepare")?,
            concrete_step("prepare", Arc::new(AtomicUsize::new(0)), Outcome::Complete)?,
        )?;
        for branch in ["first", "second"] {
            let step_name = StepName::new(branch)?;
            let factory_name = step_name.clone();
            let first = first.to_owned();
            let notify = Arc::clone(&notify);
            job = job.with_split_tasklet_factory(
                NodeId::new(branch)?,
                TaskletStepFactory::new(step_name, move || {
                    TaskletStep::new(
                        factory_name.clone(),
                        Arc::new(OrderedFailureTasklet {
                            name: branch,
                            first: first.clone(),
                            notify: Arc::clone(&notify),
                        }),
                    )
                }),
            )?;
        }
        let (clock, ids, repository) = infrastructure();
        let (_, stop) = StopSource::new();
        let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
            .launch(&job, &JobParameters::new(), &stop)
            .await?;
        let execution_id = report.job_execution().id();
        let mut unit = repository.begin().await?;
        let steps = unit.step_executions(execution_id).await?;
        unit.rollback().await?;
        let mut steps = steps
            .iter()
            .map(|step| NormalizedStep {
                step_name: step.step_name().as_str().to_owned(),
                status: step.metadata().status(),
                exit_status: step.metadata().exit_status().clone(),
                counts: step.metadata().counts(),
            })
            .collect::<Vec<_>>();
        steps.sort_by(|left, right| left.step_name.cmp(&right.step_name));
        Ok(NormalizedObservation {
            job_status: report.job_execution().metadata().status(),
            job_exit_status: report.job_execution().metadata().exit_status().clone(),
            steps,
            decisions: Vec::new(),
        })
    }
    assert_eq!(
        run("first", "split-order-first").await?,
        run("second", "split-order-second").await?
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn repeated_split_runs_leave_no_active_branch() -> Result<(), Box<dyn Error>> {
    // Identical plan and inputs on a fresh repository each time, so the durable
    // observation including every decision digest must be byte-for-byte stable.
    let baseline = normalized_success("split-repeat", 4).await?;
    for _ in 0..32 {
        assert_eq!(normalized_success("split-repeat", 4).await?, baseline);
    }
    Ok(())
}
