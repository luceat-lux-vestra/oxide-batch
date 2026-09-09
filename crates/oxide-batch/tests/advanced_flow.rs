//! M7 format-4 nested-flow and split-subgraph structural/runtime evidence.

#![allow(clippy::expect_used, clippy::panic)]

use std::error::Error;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

use oxide_batch::{
    BatchStatus, BoxFuture, Clock, ComponentRevision, DefinitionManifest, DefinitionRevision,
    FlowExecutionOutcome, FlowGraph, FlowJob, FlowLauncher, FlowNode, FlowTarget,
    InMemoryJobRepository, JobName, JobParameters, JoinNode, MAX_FLOW_COMPOSITION_DEPTH,
    NestedFlow, NodeId, PlanError, SequentialIdGenerator, SplitBranch, SplitBudget, SplitNode,
    StepComponents, StepName, StepNode, StopSource, Tasklet, TaskletContext, TaskletError,
    TaskletOutcome, TaskletStep, TerminalKind,
};

#[derive(Debug)]
struct FixedClock(SystemTime);

impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        self.0
    }
}

#[derive(Clone, Copy)]
enum Outcome {
    Complete,
    FailOnce,
}

struct CountingTasklet {
    calls: Arc<AtomicUsize>,
    outcome: Outcome,
}

impl Tasklet for CountingTasklet {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async move {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            match self.outcome {
                Outcome::FailOnce if call == 0 => Err(TaskletError::new()),
                Outcome::Complete | Outcome::FailOnce => Ok(TaskletOutcome::Completed),
            }
        })
    }
}

fn step(id: &str) -> Result<StepNode, Box<dyn Error>> {
    Ok(StepNode::new(
        NodeId::new(id)?,
        StepName::new(id)?,
        StepComponents::Tasklet(ComponentRevision::new(format!("{id}-v1"))?),
    ))
}

fn step_node(id: &str) -> Result<FlowNode, Box<dyn Error>> {
    Ok(FlowNode::step(step(id)?))
}

fn complete_graph(id: &str) -> Result<FlowGraph, Box<dyn Error>> {
    let id = NodeId::new(id)?;
    Ok(FlowGraph::new(id.clone())
        .with_node(step_node(id.as_str())?)
        .with_sequence(id, FlowTarget::Terminal(TerminalKind::Complete))?)
}

fn compile(graph: FlowGraph, name: &str) -> Result<oxide_batch::CompiledExecutionPlan, PlanError> {
    graph.compile(
        &JobName::new(name).expect("valid fixture job name"),
        DefinitionRevision::new("v1").map_err(PlanError::Token)?,
    )
}

fn nested_plan(name: &str) -> Result<oxide_batch::CompiledExecutionPlan, Box<dyn Error>> {
    let prepare = NodeId::new("prepare")?;
    let owner = NodeId::new("embedded")?;
    let after = NodeId::new("after")?;
    Ok(FlowGraph::new(prepare.clone())
        .with_node(step_node("prepare")?)
        .with_nested_flow(NestedFlow::new(owner.clone(), complete_graph("inner")?))
        .with_node(step_node("after")?)
        .with_sequence(prepare, FlowTarget::Node(owner.clone()))?
        .with_sequence(owner, FlowTarget::Node(after.clone()))?
        .with_sequence(after, FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(&JobName::new(name)?, DefinitionRevision::new("v1")?)?)
}

fn split_flow_plan(name: &str) -> Result<oxide_batch::CompiledExecutionPlan, Box<dyn Error>> {
    let prepare = NodeId::new("prepare")?;
    let split = NodeId::new("parallel")?;
    let join = NodeId::new("joined")?;
    Ok(FlowGraph::new(prepare.clone())
        .with_node(step_node("prepare")?)
        .with_node(FlowNode::split(SplitNode::new(
            split.clone(),
            vec![
                SplitBranch::flow(complete_graph("first")?),
                SplitBranch::flow(complete_graph("second")?),
            ],
            join.clone(),
            SplitBudget::new(2, 3)?,
        )))
        .with_node(FlowNode::join(JoinNode::new(join.clone())))
        .with_sequence(prepare, FlowTarget::Node(split))?
        .with_sequence(join, FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(&JobName::new(name)?, DefinitionRevision::new("v1")?)?)
}

fn infrastructure() -> (
    Arc<FixedClock>,
    Arc<SequentialIdGenerator>,
    InMemoryJobRepository,
) {
    let clock = Arc::new(FixedClock(SystemTime::UNIX_EPOCH + Duration::from_secs(20)));
    let ids = Arc::new(SequentialIdGenerator::new(NonZeroU64::MIN));
    let repository = InMemoryJobRepository::new(clock.clone(), ids.clone());
    (clock, ids, repository)
}

fn tasklet(
    id: &str,
    calls: Arc<AtomicUsize>,
    outcome: Outcome,
) -> Result<TaskletStep, Box<dyn Error>> {
    Ok(TaskletStep::new(
        StepName::new(id)?,
        Arc::new(CountingTasklet { calls, outcome }),
    ))
}

#[test]
fn nested_flow_compiles_to_format4_and_reader_counts_structural_state() -> Result<(), Box<dyn Error>>
{
    let plan = nested_plan("nested-format4")?;
    assert_eq!(plan.manifest_format(), 4);
    assert_eq!(plan.entry().as_str(), "prepare");
    assert_eq!(
        plan.nested_scope(&NodeId::new("embedded")?)
            .expect("nested scope")
            .entry()
            .as_str(),
        "inner"
    );
    assert_eq!(plan.node_count(), 3);
    assert_eq!(plan.transition_count(), 6);

    let manifest = DefinitionManifest::read(plan.definition_identity().canonical_manifest())?;
    assert_eq!(manifest.format(), 4);
    assert_eq!(manifest.node_count(), Some(4));
    assert_eq!(manifest.transition_count(), Some(8));
    Ok(())
}

#[test]
fn format4_manifest_is_deterministic_across_declaration_order() -> Result<(), Box<dyn Error>> {
    let first = nested_plan("nested-deterministic")?;

    let prepare = NodeId::new("prepare")?;
    let owner = NodeId::new("embedded")?;
    let after = NodeId::new("after")?;
    let second = FlowGraph::new(prepare.clone())
        .with_node(step_node("after")?)
        .with_nested_flow(NestedFlow::new(owner.clone(), complete_graph("inner")?))
        .with_node(step_node("prepare")?)
        .with_sequence(after.clone(), FlowTarget::Terminal(TerminalKind::Complete))?
        .with_sequence(owner.clone(), FlowTarget::Node(after))?
        .with_sequence(prepare, FlowTarget::Node(owner))?
        .compile(
            &JobName::new("nested-deterministic")?,
            DefinitionRevision::new("v1")?,
        )?;

    assert_eq!(
        first.definition_identity().canonical_manifest(),
        second.definition_identity().canonical_manifest()
    );
    assert_eq!(first.fingerprint(), second.fingerprint());
    Ok(())
}

#[test]
fn split_flow_scopes_are_bounded_and_globally_identified() -> Result<(), Box<dyn Error>> {
    let plan = split_flow_plan("split-flow-format4")?;
    assert_eq!(plan.manifest_format(), 4);
    assert_eq!(
        plan.split_branch_scope(&NodeId::new("parallel")?, 0)
            .expect("first branch scope")
            .entry()
            .as_str(),
        "first"
    );
    assert_eq!(
        plan.split_branch_scope(&NodeId::new("parallel")?, 1)
            .expect("second branch scope")
            .entry()
            .as_str(),
        "second"
    );
    assert!(plan.node(&NodeId::new("first")?).is_some());
    assert!(plan.node(&NodeId::new("second")?).is_some());
    Ok(())
}

#[test]
fn advanced_compiler_rejects_duplicate_escape_cycle_unreachable_and_depth()
-> Result<(), Box<dyn Error>> {
    let prepare = NodeId::new("prepare")?;
    let split = NodeId::new("parallel")?;
    let join = NodeId::new("joined")?;

    let duplicate = FlowGraph::new(prepare.clone())
        .with_node(step_node("prepare")?)
        .with_node(FlowNode::split(SplitNode::new(
            split.clone(),
            vec![
                SplitBranch::flow(complete_graph("prepare")?),
                SplitBranch::flow(complete_graph("other")?),
            ],
            join.clone(),
            SplitBudget::new(2, 3)?,
        )))
        .with_node(FlowNode::join(JoinNode::new(join.clone())))
        .with_sequence(prepare.clone(), FlowTarget::Node(split.clone()))?
        .with_sequence(join.clone(), FlowTarget::Terminal(TerminalKind::Complete))?;
    assert!(matches!(
        compile(duplicate, "duplicate"),
        Err(PlanError::DuplicateNodeId { .. })
    ));

    let branch = FlowGraph::new(NodeId::new("branch")?)
        .with_node(step_node("branch")?)
        .with_sequence(NodeId::new("branch")?, FlowTarget::Node(join.clone()))?;
    let escape = FlowGraph::new(prepare.clone())
        .with_node(step_node("prepare")?)
        .with_node(FlowNode::split(SplitNode::new(
            split.clone(),
            vec![
                SplitBranch::flow(branch),
                SplitBranch::flow(complete_graph("other")?),
            ],
            join.clone(),
            SplitBudget::new(2, 3)?,
        )))
        .with_node(FlowNode::join(JoinNode::new(join.clone())))
        .with_sequence(prepare.clone(), FlowTarget::Node(split.clone()))?
        .with_sequence(join.clone(), FlowTarget::Terminal(TerminalKind::Complete))?;
    assert!(matches!(
        compile(escape, "escape"),
        Err(PlanError::UndefinedNode { .. })
    ));

    let unreachable_branch = FlowGraph::new(NodeId::new("a")?)
        .with_node(step_node("a")?)
        .with_node(step_node("dead")?)
        .with_sequence(
            NodeId::new("a")?,
            FlowTarget::Terminal(TerminalKind::Complete),
        )?
        .with_sequence(
            NodeId::new("dead")?,
            FlowTarget::Terminal(TerminalKind::Complete),
        )?;
    let unreachable = FlowGraph::new(prepare.clone())
        .with_node(step_node("prepare")?)
        .with_node(FlowNode::split(SplitNode::new(
            split.clone(),
            vec![
                SplitBranch::flow(unreachable_branch),
                SplitBranch::flow(complete_graph("other")?),
            ],
            join.clone(),
            SplitBudget::new(2, 3)?,
        )))
        .with_node(FlowNode::join(JoinNode::new(join.clone())))
        .with_sequence(prepare.clone(), FlowTarget::Node(split.clone()))?
        .with_sequence(join.clone(), FlowTarget::Terminal(TerminalKind::Complete))?;
    assert!(matches!(
        compile(unreachable, "unreachable"),
        Err(PlanError::UnreachableNode { .. })
    ));

    let owner = NodeId::new("embedded")?;
    let after = NodeId::new("after")?;
    let cycle = FlowGraph::new(owner.clone())
        .with_nested_flow(NestedFlow::new(owner.clone(), complete_graph("inner")?))
        .with_node(step_node("after")?)
        .with_sequence(owner.clone(), FlowTarget::Node(after.clone()))?
        .with_sequence(after, FlowTarget::Node(owner))?;
    assert!(matches!(
        compile(cycle, "cycle"),
        Err(PlanError::CyclicGraph { .. })
    ));

    let mut deep = complete_graph("leaf")?;
    for index in 0..MAX_FLOW_COMPOSITION_DEPTH {
        let owner = NodeId::new(format!("owner-{index}"))?;
        deep = FlowGraph::new(owner.clone())
            .with_nested_flow(NestedFlow::new(owner.clone(), deep))
            .with_sequence(owner, FlowTarget::Terminal(TerminalKind::Complete))?;
    }
    assert!(matches!(
        compile(deep, "deep"),
        Err(PlanError::CompositionDepthExceeded { .. })
    ));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn nested_flow_executes_through_the_root_runtime_once() -> Result<(), Box<dyn Error>> {
    let name = JobName::new("nested-runtime")?;
    let prepare = Arc::new(AtomicUsize::new(0));
    let inner = Arc::new(AtomicUsize::new(0));
    let after = Arc::new(AtomicUsize::new(0));
    let job = FlowJob::new(name.clone(), nested_plan(name.as_str())?)?
        .with_tasklet_step(
            NodeId::new("prepare")?,
            tasklet("prepare", Arc::clone(&prepare), Outcome::Complete)?,
        )?
        .with_tasklet_step(
            NodeId::new("inner")?,
            tasklet("inner", Arc::clone(&inner), Outcome::Complete)?,
        )?
        .with_tasklet_step(
            NodeId::new("after")?,
            tasklet("after", Arc::clone(&after), Outcome::Complete)?,
        )?;
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();
    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;

    assert_eq!(report.outcome(), &FlowExecutionOutcome::Completed);
    assert_eq!(
        report.job_execution().metadata().status(),
        BatchStatus::Completed
    );
    assert_eq!(prepare.load(Ordering::SeqCst), 1);
    assert_eq!(inner.load(Ordering::SeqCst), 1);
    assert_eq!(after.load(Ordering::SeqCst), 1);
    assert_eq!(report.step_executions().len(), 3);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn completed_split_subgraph_branch_is_reused_on_restart() -> Result<(), Box<dyn Error>> {
    let name = JobName::new("split-subgraph-restart")?;
    let prepare_calls = Arc::new(AtomicUsize::new(0));
    let first_calls = Arc::new(AtomicUsize::new(0));
    let second_calls = Arc::new(AtomicUsize::new(0));
    let job = FlowJob::new(name.clone(), split_flow_plan(name.as_str())?)?
        .with_tasklet_step(
            NodeId::new("prepare")?,
            tasklet("prepare", Arc::clone(&prepare_calls), Outcome::Complete)?,
        )?
        .with_tasklet_step(
            NodeId::new("first")?,
            tasklet("first", Arc::clone(&first_calls), Outcome::Complete)?,
        )?
        .with_tasklet_step(
            NodeId::new("second")?,
            tasklet("second", Arc::clone(&second_calls), Outcome::FailOnce)?,
        )?;
    let (clock, ids, repository) = infrastructure();
    let launcher = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref());
    let (_, first_stop) = StopSource::new();
    let first = launcher
        .launch(&job, &JobParameters::new(), &first_stop)
        .await?;
    assert!(matches!(first.outcome(), FlowExecutionOutcome::Failed(_)));

    let (_, second_stop) = StopSource::new();
    let second = launcher
        .launch(&job, &JobParameters::new(), &second_stop)
        .await?;
    assert_eq!(second.outcome(), &FlowExecutionOutcome::Completed);
    assert_eq!(prepare_calls.load(Ordering::SeqCst), 1);
    assert_eq!(first_calls.load(Ordering::SeqCst), 1);
    assert_eq!(second_calls.load(Ordering::SeqCst), 2);
    assert_eq!(second.step_executions().len(), 1);
    Ok(())
}
