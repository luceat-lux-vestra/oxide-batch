//! In-memory conformance evidence for durable M3 flow traversal.

#![allow(clippy::panic)]

use std::error::Error;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

use oxide_batch::{
    BatchStatus, BoxFuture, Clock, ComponentRevision, DeciderError, DeciderRevision, DecisionInput,
    DecisionInputVersion, DecisionNode, DefinitionRevision, ExitCode, ExitPattern, ExitStatus,
    FlowExecutionOutcome, FlowFailure, FlowGraph, FlowJob, FlowLauncher, FlowNode, FlowTarget,
    FlowTransition, FlowTransitionKind, InMemoryJobRepository, JobExecutionDecider, JobInstanceKey,
    JobName, JobParameters, JobRepository, NodeId, RepositoryError, SequentialIdGenerator,
    StartControls, StartLimit, StepComponents, StepName, StepNode, StopSource, Tasklet,
    TaskletContext, TaskletError, TaskletOutcome, TaskletStep, TerminalKind,
};

#[derive(Debug)]
struct FixedClock(SystemTime);

impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        self.0
    }
}

struct CountingTasklet {
    calls: Arc<AtomicUsize>,
    behavior: Behavior,
}

enum Behavior {
    Complete,
    CompleteWith(&'static str),
    Fail,
    FailOnce,
}

impl Tasklet for CountingTasklet {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            match self.behavior {
                Behavior::CompleteWith(code) => Ok(TaskletOutcome::CompletedWith(ExitStatus::new(
                    ExitCode::new(code).map_err(TaskletError::from_error)?,
                ))),
                Behavior::Fail => Err(TaskletError::new()),
                Behavior::FailOnce if call == 0 => Err(TaskletError::new()),
                Behavior::Complete | Behavior::FailOnce => Ok(TaskletOutcome::Completed),
            }
        })
    }
}

struct CountingDecider {
    calls: Arc<AtomicUsize>,
    outcome: &'static str,
}

struct PanicDecider;

impl JobExecutionDecider for PanicDecider {
    fn decide<'a>(
        &'a self,
        _input: DecisionInput<'a>,
    ) -> BoxFuture<'a, Result<ExitStatus, DeciderError>> {
        std::panic::panic_any("sensitive-decider-payload")
    }
}

impl JobExecutionDecider for CountingDecider {
    fn decide<'a>(
        &'a self,
        input: DecisionInput<'a>,
    ) -> BoxFuture<'a, Result<ExitStatus, DeciderError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let _ = input.preceding_step();
            Ok(ExitStatus::new(
                ExitCode::new(self.outcome).map_err(|_| DeciderError::new())?,
            ))
        })
    }
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

fn tasklet_node(id: &str, controls: StartControls) -> Result<FlowNode, Box<dyn Error>> {
    Ok(FlowNode::step(
        StepNode::new(
            NodeId::new(id)?,
            StepName::new(id)?,
            StepComponents::Tasklet(ComponentRevision::new(format!("{id}-v1"))?),
        )
        .with_start_controls(controls),
    ))
}

fn tasklet_step(
    name: &str,
    calls: Arc<AtomicUsize>,
    behavior: Behavior,
) -> Result<TaskletStep, Box<dyn Error>> {
    Ok(TaskletStep::new(
        StepName::new(name)?,
        Arc::new(CountingTasklet { calls, behavior }),
    ))
}

#[tokio::test(flavor = "current_thread")]
async fn exit_status_selects_most_specific_transition() -> Result<(), Box<dyn Error>> {
    let load = NodeId::new("load")?;
    let priority = NodeId::new("priority")?;
    let ordinary = NodeId::new("ordinary")?;
    let plan = FlowGraph::new(load.clone())
        .with_node(tasklet_node("load", StartControls::default())?)
        .with_node(tasklet_node("priority", StartControls::default())?)
        .with_node(tasklet_node("ordinary", StartControls::default())?)
        .with_transition(FlowTransition::new(
            load.clone(),
            ExitPattern::new("PRIORITY")?,
            FlowTarget::Node(priority.clone()),
        ))
        .with_transition(FlowTransition::new(
            load.clone(),
            ExitPattern::new("*")?,
            FlowTarget::Node(ordinary.clone()),
        ))
        .with_sequence(
            priority.clone(),
            FlowTarget::Terminal(TerminalKind::Complete),
        )?
        .with_sequence(
            ordinary.clone(),
            FlowTarget::Terminal(TerminalKind::Complete),
        )?
        .compile(
            &JobName::new("conditional")?,
            DefinitionRevision::new("v1")?,
        )?;
    let load_calls = Arc::new(AtomicUsize::new(0));
    let priority_calls = Arc::new(AtomicUsize::new(0));
    let ordinary_calls = Arc::new(AtomicUsize::new(0));
    let job = FlowJob::new(JobName::new("conditional")?, plan)?
        .with_tasklet_step(
            load.clone(),
            tasklet_step(
                "load",
                load_calls.clone(),
                Behavior::CompleteWith("PRIORITY"),
            )?,
        )?
        .with_tasklet_step(
            priority,
            tasklet_step("priority", priority_calls.clone(), Behavior::Complete)?,
        )?
        .with_tasklet_step(
            ordinary,
            tasklet_step("ordinary", ordinary_calls.clone(), Behavior::Complete)?,
        )?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();

    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;

    assert_eq!(report.outcome(), &FlowExecutionOutcome::Completed);
    assert_eq!(load_calls.load(Ordering::SeqCst), 1);
    assert_eq!(priority_calls.load(Ordering::SeqCst), 1);
    assert_eq!(ordinary_calls.load(Ordering::SeqCst), 0);
    assert_eq!(report.decisions().len(), 2);
    assert_eq!(
        report.decisions()[0].observed_outcome().as_str(),
        "PRIORITY"
    );
    assert_eq!(
        report.job_execution().metadata().status(),
        BatchStatus::Completed
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn committed_decider_is_not_reinvoked() -> Result<(), Box<dyn Error>> {
    let load = NodeId::new("load")?;
    let decide = NodeId::new("decide")?;
    let publish = NodeId::new("publish")?;
    let plan = FlowGraph::new(load.clone())
        .with_node(tasklet_node("load", StartControls::default())?)
        .with_node(FlowNode::decision(DecisionNode::new(
            decide.clone(),
            DeciderRevision::new("route-v1")?,
            DecisionInputVersion::new(1)?,
        )))
        .with_node(tasklet_node("publish", StartControls::default())?)
        .with_sequence(load.clone(), FlowTarget::Node(decide.clone()))?
        .with_transition(FlowTransition::new(
            decide.clone(),
            ExitPattern::new("RUN")?,
            FlowTarget::Node(publish.clone()),
        ))
        .with_sequence(
            publish.clone(),
            FlowTarget::Terminal(TerminalKind::Complete),
        )?
        .compile(
            &JobName::new("restartable-flow")?,
            DefinitionRevision::new("v1")?,
        )?;
    let load_calls = Arc::new(AtomicUsize::new(0));
    let publish_calls = Arc::new(AtomicUsize::new(0));
    let decider_calls = Arc::new(AtomicUsize::new(0));
    let job = FlowJob::new(JobName::new("restartable-flow")?, plan)?
        .with_tasklet_step(
            load.clone(),
            tasklet_step("load", load_calls.clone(), Behavior::Complete)?,
        )?
        .with_decider(
            decide,
            Arc::new(CountingDecider {
                calls: decider_calls.clone(),
                outcome: "RUN",
            }),
        )?
        .with_tasklet_step(
            publish,
            tasklet_step("publish", publish_calls.clone(), Behavior::FailOnce)?,
        )?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());

    let first = launcher.launch(&job, &JobParameters::new(), &stop).await?;
    assert!(matches!(first.outcome(), FlowExecutionOutcome::Failed(_)));
    let second = launcher.launch(&job, &JobParameters::new(), &stop).await?;

    assert_eq!(second.outcome(), &FlowExecutionOutcome::Completed);
    assert_eq!(load_calls.load(Ordering::SeqCst), 1);
    assert_eq!(decider_calls.load(Ordering::SeqCst), 1);
    assert_eq!(publish_calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        second.decisions()[0].kind(),
        FlowTransitionKind::CompletedStepReuse
    );
    assert!(second.decisions()[0].reused_decision_id().is_some());
    assert_eq!(second.decisions()[1].kind(), FlowTransitionKind::Decider);
    assert!(second.decisions()[1].reused_decision_id().is_some());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn failed_start_consumes_limit() -> Result<(), Box<dyn Error>> {
    let only = NodeId::new("only")?;
    let limit = StartLimit::new(1)?;
    let plan = FlowGraph::new(only.clone())
        .with_node(tasklet_node("only", StartControls::new(limit, false))?)
        .with_sequence(only.clone(), FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(&JobName::new("limited")?, DefinitionRevision::new("v1")?)?;
    let calls = Arc::new(AtomicUsize::new(0));
    let job = FlowJob::new(JobName::new("limited")?, plan)?.with_tasklet_step(
        only.clone(),
        tasklet_step("only", calls.clone(), Behavior::Fail)?,
    )?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());

    let first = launcher.launch(&job, &JobParameters::new(), &stop).await?;
    assert!(matches!(first.outcome(), FlowExecutionOutcome::Failed(_)));
    let second = launcher.launch(&job, &JobParameters::new(), &stop).await?;

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        second.outcome(),
        &FlowExecutionOutcome::Failed(FlowFailure::StartLimitExceeded { node: only, limit })
    );
    assert!(second.step_executions().is_empty());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn start_limit_is_atomic_per_instance_and_logical_step() -> Result<(), Box<dyn Error>> {
    let node = NodeId::new("only")?;
    let name = JobName::new("atomic-limit")?;
    let limit = StartLimit::new(1)?;
    let plan = FlowGraph::new(node.clone())
        .with_node(tasklet_node("only", StartControls::new(limit, false))?)
        .with_sequence(node.clone(), FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(&name, DefinitionRevision::new("v1")?)?;
    let (_, _, repository) = infrastructure();
    let key = JobInstanceKey::new(name, &JobParameters::new());
    let mut setup = repository.begin().await?;
    let instance = setup
        .select_or_create_job_instance(&key)
        .await?
        .instance()
        .clone();
    let execution = setup
        .create_job_execution_with_definition(instance.id(), plan.definition_identity())
        .await?;
    setup.commit().await?;

    let mut first = repository.begin().await?;
    let mut concurrent = repository.begin().await?;
    first
        .create_flow_step_execution(execution.id(), &StepName::new("only")?, &node, limit)
        .await?;
    concurrent
        .create_flow_step_execution(execution.id(), &StepName::new("only")?, &node, limit)
        .await?;
    first.commit().await?;
    assert_eq!(
        concurrent.commit().await,
        Err(RepositoryError::ConcurrentModification)
    );

    let mut inspection = repository.begin().await?;
    assert_eq!(
        inspection
            .create_flow_step_execution(execution.id(), &StepName::new("only")?, &node, limit)
            .await,
        Err(RepositoryError::StartLimitExceeded {
            instance_id: instance.id(),
            node_id: node,
            limit,
        })
    );
    inspection.rollback().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn allow_start_if_complete_reruns_on_restart_path() -> Result<(), Box<dyn Error>> {
    let prepare = NodeId::new("prepare")?;
    let publish = NodeId::new("publish")?;
    let plan = FlowGraph::new(prepare.clone())
        .with_node(tasklet_node(
            "prepare",
            StartControls::new(StartLimit::new(2)?, true),
        )?)
        .with_node(tasklet_node("publish", StartControls::default())?)
        .with_sequence(prepare.clone(), FlowTarget::Node(publish.clone()))?
        .with_sequence(
            publish.clone(),
            FlowTarget::Terminal(TerminalKind::Complete),
        )?
        .compile(
            &JobName::new("rerun-complete")?,
            DefinitionRevision::new("v1")?,
        )?;
    let prepare_calls = Arc::new(AtomicUsize::new(0));
    let publish_calls = Arc::new(AtomicUsize::new(0));
    let job = FlowJob::new(JobName::new("rerun-complete")?, plan)?
        .with_tasklet_step(
            prepare,
            tasklet_step("prepare", prepare_calls.clone(), Behavior::Complete)?,
        )?
        .with_tasklet_step(
            publish,
            tasklet_step("publish", publish_calls.clone(), Behavior::FailOnce)?,
        )?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());

    assert!(matches!(
        launcher
            .launch(&job, &JobParameters::new(), &stop)
            .await?
            .outcome(),
        FlowExecutionOutcome::Failed(_)
    ));
    let restarted = launcher.launch(&job, &JobParameters::new(), &stop).await?;

    assert_eq!(restarted.outcome(), &FlowExecutionOutcome::Completed);
    assert_eq!(prepare_calls.load(Ordering::SeqCst), 2);
    assert_eq!(publish_calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        restarted.decisions()[0].kind(),
        FlowTransitionKind::StepExit
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn unmapped_exit_fails_job() -> Result<(), Box<dyn Error>> {
    let only = NodeId::new("only")?;
    let plan = FlowGraph::new(only.clone())
        .with_node(tasklet_node("only", StartControls::default())?)
        .with_transition(FlowTransition::new(
            only.clone(),
            ExitPattern::new("COMPLETED")?,
            FlowTarget::Terminal(TerminalKind::Complete),
        ))
        .compile(&JobName::new("unmapped")?, DefinitionRevision::new("v1")?)?;
    let calls = Arc::new(AtomicUsize::new(0));
    let job = FlowJob::new(JobName::new("unmapped")?, plan)?.with_tasklet_step(
        only.clone(),
        tasklet_step("only", calls, Behavior::CompleteWith("UNMAPPED"))?,
    )?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();

    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;

    assert!(matches!(
        report.outcome(),
        FlowExecutionOutcome::Failed(FlowFailure::UnmappedExitOutcome { node, code })
            if node == &only && code.as_str() == "UNMAPPED"
    ));
    assert_eq!(
        report.job_execution().metadata().status(),
        BatchStatus::Failed
    );
    assert!(report.decisions().is_empty());
    Ok(())
}

async fn assert_completed_step_reuse() -> Result<(), Box<dyn Error>> {
    let prepare = NodeId::new("prepare")?;
    let publish = NodeId::new("publish")?;
    let plan = FlowGraph::new(prepare.clone())
        .with_node(tasklet_node("prepare", StartControls::default())?)
        .with_node(tasklet_node("publish", StartControls::default())?)
        .with_sequence(prepare.clone(), FlowTarget::Node(publish.clone()))?
        .with_sequence(
            publish.clone(),
            FlowTarget::Terminal(TerminalKind::Complete),
        )?
        .compile(
            &JobName::new("skip-complete")?,
            DefinitionRevision::new("v1")?,
        )?;
    let prepare_calls = Arc::new(AtomicUsize::new(0));
    let publish_calls = Arc::new(AtomicUsize::new(0));
    let job = FlowJob::new(JobName::new("skip-complete")?, plan)?
        .with_tasklet_step(
            prepare,
            tasklet_step("prepare", prepare_calls.clone(), Behavior::Complete)?,
        )?
        .with_tasklet_step(
            publish,
            tasklet_step("publish", publish_calls.clone(), Behavior::FailOnce)?,
        )?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());

    assert!(matches!(
        launcher
            .launch(&job, &JobParameters::new(), &stop)
            .await?
            .outcome(),
        FlowExecutionOutcome::Failed(_)
    ));
    let restarted = launcher.launch(&job, &JobParameters::new(), &stop).await?;

    assert_eq!(restarted.outcome(), &FlowExecutionOutcome::Completed);
    assert_eq!(prepare_calls.load(Ordering::SeqCst), 1);
    assert_eq!(publish_calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        restarted.decisions()[0].kind(),
        FlowTransitionKind::CompletedStepReuse
    );
    assert!(restarted.decisions()[0].reused_decision_id().is_some());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn completed_step_is_skipped_by_default() -> Result<(), Box<dyn Error>> {
    assert_completed_step_reuse().await
}

#[tokio::test(flavor = "current_thread")]
async fn committed_transition_survives_restart() -> Result<(), Box<dyn Error>> {
    assert_completed_step_reuse().await
}

#[tokio::test(flavor = "current_thread")]
async fn decider_result_and_target_commit_together() -> Result<(), Box<dyn Error>> {
    let decide = NodeId::new("decide")?;
    let run = NodeId::new("run")?;
    let plan = FlowGraph::new(decide.clone())
        .with_node(FlowNode::decision(DecisionNode::new(
            decide.clone(),
            DeciderRevision::new("route-v1")?,
            DecisionInputVersion::new(1)?,
        )))
        .with_node(tasklet_node("run", StartControls::default())?)
        .with_transition(FlowTransition::new(
            decide.clone(),
            ExitPattern::new("RUN")?,
            FlowTarget::Node(run.clone()),
        ))
        .with_sequence(run.clone(), FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(
            &JobName::new("decision-target")?,
            DefinitionRevision::new("v1")?,
        )?;
    let decider_calls = Arc::new(AtomicUsize::new(0));
    let run_calls = Arc::new(AtomicUsize::new(0));
    let job = FlowJob::new(JobName::new("decision-target")?, plan)?
        .with_decider(
            decide,
            Arc::new(CountingDecider {
                calls: decider_calls.clone(),
                outcome: "RUN",
            }),
        )?
        .with_tasklet_step(
            run.clone(),
            tasklet_step("run", run_calls.clone(), Behavior::Complete)?,
        )?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();

    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;

    assert_eq!(report.outcome(), &FlowExecutionOutcome::Completed);
    assert_eq!(decider_calls.load(Ordering::SeqCst), 1);
    assert_eq!(run_calls.load(Ordering::SeqCst), 1);
    assert_eq!(report.decisions()[0].target(), &FlowTarget::Node(run));
    assert_eq!(report.decisions()[0].sequence().get(), 1);
    assert_eq!(report.decisions()[1].sequence().get(), 2);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn decider_input_change_records_new_path() -> Result<(), Box<dyn Error>> {
    let prepare = NodeId::new("prepare")?;
    let decide = NodeId::new("decide")?;
    let publish = NodeId::new("publish")?;
    let plan = FlowGraph::new(prepare.clone())
        .with_node(tasklet_node(
            "prepare",
            StartControls::new(StartLimit::new(2)?, true),
        )?)
        .with_node(FlowNode::decision(DecisionNode::new(
            decide.clone(),
            DeciderRevision::new("route-v1")?,
            DecisionInputVersion::new(1)?,
        )))
        .with_node(tasklet_node("publish", StartControls::default())?)
        .with_sequence(prepare.clone(), FlowTarget::Node(decide.clone()))?
        .with_transition(FlowTransition::new(
            decide.clone(),
            ExitPattern::new("RUN")?,
            FlowTarget::Node(publish.clone()),
        ))
        .with_sequence(
            publish.clone(),
            FlowTarget::Terminal(TerminalKind::Complete),
        )?
        .compile(
            &JobName::new("changed-input")?,
            DefinitionRevision::new("v1")?,
        )?;
    let prepare_calls = Arc::new(AtomicUsize::new(0));
    let publish_calls = Arc::new(AtomicUsize::new(0));
    let decider_calls = Arc::new(AtomicUsize::new(0));
    let job = FlowJob::new(JobName::new("changed-input")?, plan)?
        .with_tasklet_step(
            prepare,
            tasklet_step("prepare", prepare_calls, Behavior::Complete)?,
        )?
        .with_decider(
            decide,
            Arc::new(CountingDecider {
                calls: decider_calls.clone(),
                outcome: "RUN",
            }),
        )?
        .with_tasklet_step(
            publish,
            tasklet_step("publish", publish_calls, Behavior::FailOnce)?,
        )?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());

    let first = launcher.launch(&job, &JobParameters::new(), &stop).await?;
    let second = launcher.launch(&job, &JobParameters::new(), &stop).await?;

    assert_eq!(second.outcome(), &FlowExecutionOutcome::Completed);
    assert_eq!(decider_calls.load(Ordering::SeqCst), 2);
    assert_ne!(
        first.decisions()[1].input_digest(),
        second.decisions()[1].input_digest()
    );
    assert!(second.decisions()[1].reused_decision_id().is_none());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn decider_panic_is_redacted_failure() -> Result<(), Box<dyn Error>> {
    let decide = NodeId::new("decide")?;
    let plan = FlowGraph::new(decide.clone())
        .with_node(FlowNode::decision(DecisionNode::new(
            decide.clone(),
            DeciderRevision::new("panic-v1")?,
            DecisionInputVersion::new(1)?,
        )))
        .with_transition(FlowTransition::new(
            decide.clone(),
            ExitPattern::new("*")?,
            FlowTarget::Terminal(TerminalKind::Complete),
        ))
        .compile(
            &JobName::new("panic-decider")?,
            DefinitionRevision::new("v1")?,
        )?;
    let job = FlowJob::new(JobName::new("panic-decider")?, plan)?
        .with_decider(decide, Arc::new(PanicDecider))?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();

    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;

    assert_eq!(
        report.outcome(),
        &FlowExecutionOutcome::Failed(FlowFailure::DeciderPanic)
    );
    assert_eq!(
        report.job_execution().metadata().status(),
        BatchStatus::Failed
    );
    assert!(!format!("{report:?}").contains("sensitive-decider-payload"));
    assert!(report.decisions().is_empty());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn stop_fail_and_end_terminals_persist_lifecycle_status() -> Result<(), Box<dyn Error>> {
    for (suffix, terminal, expected) in [
        ("end", TerminalKind::Complete, BatchStatus::Completed),
        ("fail", TerminalKind::Fail, BatchStatus::Failed),
        ("stop", TerminalKind::Stop, BatchStatus::Stopped),
    ] {
        let node = NodeId::new("only")?;
        let name = JobName::new(format!("terminal-{suffix}"))?;
        let plan = FlowGraph::new(node.clone())
            .with_node(tasklet_node("only", StartControls::default())?)
            .with_transition(FlowTransition::new(
                node.clone(),
                ExitPattern::new("*")?,
                FlowTarget::Terminal(terminal),
            ))
            .compile(&name, DefinitionRevision::new("v1")?)?;
        let job = FlowJob::new(name, plan)?.with_tasklet_step(
            node,
            tasklet_step("only", Arc::new(AtomicUsize::new(0)), Behavior::Complete)?,
        )?;
        let (clock, ids, repository) = infrastructure();
        let (_, stop) = StopSource::new();
        let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
            .launch(&job, &JobParameters::new(), &stop)
            .await?;
        assert_eq!(report.job_execution().metadata().status(), expected);
    }
    Ok(())
}
