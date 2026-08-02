//! In-memory execution evidence for the bounded M4 parallel-step slice.

#![allow(clippy::panic)]

use std::error::Error;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

use oxide_batch::{
    BatchStatus, BoxFuture, Clock, ComponentRevision, DefinitionRevision, ExitCode, ExitStatus,
    FlowExecutionOutcome, FlowGraph, FlowJob, FlowLauncher, FlowNode, FlowTarget,
    FlowTransitionKind, InMemoryJobRepository, JobName, JobParameters, JoinNode, NodeId,
    SequentialIdGenerator, SplitBranch, SplitBudget, SplitNode, StepComponents, StepName, StepNode,
    StopSource, Tasklet, TaskletContext, TaskletError, TaskletOutcome, TaskletStep,
    TaskletStepFactory, TerminalKind,
};
use tokio::sync::Barrier;

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
    observed: Arc<AtomicUsize>,
}

impl Tasklet for AwaitCancellationTasklet {
    fn execute<'a>(
        &'a self,
        context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            context.stop_token().cancelled().await;
            self.observed.fetch_add(1, Ordering::SeqCst);
            Ok(TaskletOutcome::Stopped)
        })
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
    let prepare = NodeId::new("prepare")?;
    let split = NodeId::new("parallel")?;
    let join = NodeId::new("joined")?;
    Ok(FlowGraph::new(prepare.clone())
        .with_node(FlowNode::step(step_node("prepare")?))
        .with_node(FlowNode::split(SplitNode::new(
            split.clone(),
            vec![
                SplitBranch::new(vec![step_node("first")?]),
                SplitBranch::new(vec![step_node("second")?]),
            ],
            join.clone(),
            SplitBudget::new(concurrency, u32::from(concurrency) + 1)?,
        )))
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
