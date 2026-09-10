//! M7 format-4 nested-flow and split-subgraph structural/runtime evidence.

#![allow(clippy::expect_used, clippy::panic)]

use std::error::Error;
use std::fmt::Write as _;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use tokio::sync::Notify;

use oxide_batch::{
    BatchStatus, BoxFuture, Clock, ComponentRevision, DeciderError, DeciderRevision, DecisionInput,
    DecisionInputVersion, DecisionNode, DefinitionManifest, DefinitionRevision, ExitCode,
    ExitPattern, ExitStatus, FlowExecutionOutcome, FlowGraph, FlowJob, FlowLauncher, FlowNode,
    FlowTarget, FlowTransition, FlowTransitionKind, InMemoryExplorer, InMemoryJobRepository,
    JobExecutionDecider, JobExplorer, JobName, JobParameters, JobRepository, JoinNode,
    MAX_FLOW_COMPOSITION_DEPTH, MAX_NODES, MAX_OUTGOING_TRANSITIONS, MAX_SPLIT_BRANCHES,
    MAX_TRANSITIONS, ManifestError, NestedFlow, NodeId, PageRequest, PageSize, PlanError,
    SequentialIdGenerator, SplitBranch, SplitBudget, SplitNode, StepComponents, StepName, StepNode,
    StopSource, Tasklet, TaskletContext, TaskletError, TaskletOutcome, TaskletStep, TerminalKind,
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

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut rendered, byte| {
        let _ = write!(rendered, "{byte:02x}");
        rendered
    })
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
fn format4_has_a_deterministic_golden_vector() -> Result<(), Box<dyn Error>> {
    let plan = nested_plan("nested-format4")?;
    assert_eq!(
        hex(plan.fingerprint()),
        "5588696b6bedc009b4da64689e4e9eb46adb10454c2ea2b91025850cdc68436f"
    );
    Ok(())
}

#[test]
fn format4_reader_rejects_newer_noncanonical_oversized_and_overbound_documents()
-> Result<(), Box<dyn Error>> {
    let plan = nested_plan("format4-reader")?;
    let canonical = plan.definition_identity().canonical_manifest();

    let mut newer: serde_json::Value = serde_json::from_slice(canonical)?;
    newer["format"] = serde_json::json!(5);
    let newer = serde_json::to_vec(&newer)?;
    assert!(matches!(
        DefinitionManifest::read(&newer),
        Err(ManifestError::UnsupportedFormat {
            format: 5,
            supported: 4
        })
    ));

    let mut noncanonical = canonical.to_vec();
    noncanonical.push(b'\n');
    assert!(matches!(
        DefinitionManifest::read(&noncanonical),
        Err(ManifestError::NonCanonicalEncoding)
    ));

    let mut oversized = canonical.to_vec();
    oversized.resize(64 * 1024 + 1, b' ');
    assert!(matches!(
        DefinitionManifest::read(&oversized),
        Err(ManifestError::TooLarge { max_bytes: 65_536 })
    ));

    let too_many_nodes = serde_json::json!({
        "entry": "n0",
        "format": 4,
        "job": "reader-node-bound",
        "nodes": (0..=MAX_NODES).map(|_| serde_json::json!({"kind": "step"})).collect::<Vec<_>>(),
        "transitions": []
    });
    let too_many_nodes = serde_json::to_vec(&too_many_nodes)?;
    assert!(matches!(
        DefinitionManifest::read(&too_many_nodes),
        Err(ManifestError::GraphOutOfBounds { .. })
    ));

    let too_many_transitions = serde_json::json!({
        "entry": "n0",
        "format": 4,
        "job": "reader-transition-bound",
        "nodes": [{"kind": "step"}],
        "transitions": (0..=MAX_TRANSITIONS).map(|_| serde_json::json!({})).collect::<Vec<_>>()
    });
    let too_many_transitions = serde_json::to_vec(&too_many_transitions)?;
    assert!(matches!(
        DefinitionManifest::read(&too_many_transitions),
        Err(ManifestError::GraphOutOfBounds { .. })
    ));
    Ok(())
}

#[test]
fn format4_compiler_enforces_global_node_transition_outgoing_and_split_bounds()
-> Result<(), Box<dyn Error>> {
    let owner = NodeId::new("embedded")?;
    let mut node_bound = FlowGraph::new(owner.clone())
        .with_nested_flow(NestedFlow::new(owner.clone(), complete_graph("inner")?))
        .with_sequence(owner.clone(), FlowTarget::Terminal(TerminalKind::Complete))?;
    for index in 0..(MAX_NODES - 1) {
        node_bound = node_bound.with_node(step_node(&format!("unused-{index}"))?);
    }
    assert!(matches!(
        compile(node_bound, "node-bound"),
        Err(PlanError::TooManyNodes { max: MAX_NODES })
    ));

    let mut transition_bound = FlowGraph::new(owner.clone())
        .with_nested_flow(NestedFlow::new(owner.clone(), complete_graph("inner")?))
        .with_sequence(owner.clone(), FlowTarget::Terminal(TerminalKind::Complete))?;
    for _ in 0..(MAX_TRANSITIONS - 3) {
        transition_bound = transition_bound.with_transition(FlowTransition::new(
            owner.clone(),
            ExitPattern::new("*")?,
            FlowTarget::Terminal(TerminalKind::Complete),
        ));
    }
    assert!(matches!(
        compile(transition_bound, "transition-bound"),
        Err(PlanError::TooManyTransitions {
            max: MAX_TRANSITIONS
        })
    ));

    let mut outgoing_bound = FlowGraph::new(owner.clone())
        .with_nested_flow(NestedFlow::new(owner.clone(), complete_graph("inner")?));
    for _ in 0..=MAX_OUTGOING_TRANSITIONS {
        outgoing_bound = outgoing_bound.with_transition(FlowTransition::new(
            owner.clone(),
            ExitPattern::new("*")?,
            FlowTarget::Terminal(TerminalKind::Complete),
        ));
    }
    assert!(matches!(
        compile(outgoing_bound, "outgoing-bound"),
        Err(PlanError::TooManyOutgoingTransitions {
            max: MAX_OUTGOING_TRANSITIONS,
            ..
        })
    ));

    let prepare = NodeId::new("prepare")?;
    let split = NodeId::new("parallel")?;
    let join = NodeId::new("joined")?;
    let branches = (0..=MAX_SPLIT_BRANCHES)
        .map(|index| complete_graph(&format!("branch-{index}")).map(SplitBranch::flow))
        .collect::<Result<Vec<_>, _>>()?;
    let split_bound = FlowGraph::new(prepare.clone())
        .with_node(step_node("prepare")?)
        .with_node(FlowNode::split(SplitNode::new(
            split.clone(),
            branches,
            join.clone(),
            SplitBudget::new(1, 2)?,
        )))
        .with_node(FlowNode::join(JoinNode::new(join.clone())))
        .with_sequence(prepare, FlowTarget::Node(split))?
        .with_sequence(join, FlowTarget::Terminal(TerminalKind::Complete))?;
    assert!(matches!(
        compile(split_bound, "split-bound"),
        Err(PlanError::InvalidSplitBranchCount {
            max: MAX_SPLIT_BRANCHES,
            ..
        })
    ));
    Ok(())
}

#[test]
fn format4_rejects_foreign_join_entry_orphan_and_multi_owned_join() -> Result<(), Box<dyn Error>> {
    let prepare = NodeId::new("prepare")?;
    let split = NodeId::new("parallel")?;
    let join = NodeId::new("joined")?;
    let intruder = NodeId::new("intruder")?;
    let foreign_join = FlowGraph::new(prepare.clone())
        .with_node(step_node("prepare")?)
        .with_node(step_node("intruder")?)
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
        .with_sequence(prepare.clone(), FlowTarget::Node(split.clone()))?
        .with_sequence(intruder, FlowTarget::Node(join.clone()))?
        .with_sequence(join.clone(), FlowTarget::Terminal(TerminalKind::Complete))?;
    assert!(matches!(
        compile(foreign_join, "foreign-join"),
        Err(PlanError::JoinHasExternalEntry { .. })
    ));

    let owner = NodeId::new("embedded")?;
    let orphan = NodeId::new("orphan")?;
    let orphan_join = FlowGraph::new(owner.clone())
        .with_nested_flow(NestedFlow::new(owner.clone(), complete_graph("inner")?))
        .with_node(FlowNode::join(JoinNode::new(orphan)))
        .with_sequence(owner, FlowTarget::Terminal(TerminalKind::Complete))?;
    assert!(matches!(
        compile(orphan_join, "orphan-join"),
        Err(PlanError::OrphanJoin { .. })
    ));

    let split_two = NodeId::new("parallel-two")?;
    let multi_owned = FlowGraph::new(prepare.clone())
        .with_node(step_node("prepare")?)
        .with_node(FlowNode::split(SplitNode::new(
            split.clone(),
            vec![
                SplitBranch::flow(complete_graph("a1")?),
                SplitBranch::flow(complete_graph("a2")?),
            ],
            join.clone(),
            SplitBudget::new(2, 3)?,
        )))
        .with_node(FlowNode::split(SplitNode::new(
            split_two,
            vec![
                SplitBranch::flow(complete_graph("b1")?),
                SplitBranch::flow(complete_graph("b2")?),
            ],
            join.clone(),
            SplitBudget::new(2, 3)?,
        )))
        .with_node(FlowNode::join(JoinNode::new(join.clone())))
        .with_sequence(prepare, FlowTarget::Node(split))?
        .with_sequence(join, FlowTarget::Terminal(TerminalKind::Complete))?;
    assert!(matches!(
        compile(multi_owned, "multi-owned-join"),
        Err(PlanError::JoinHasMultipleOwners { .. })
    ));
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

struct CodeDecider {
    code: &'static str,
}

impl JobExecutionDecider for CodeDecider {
    fn decide<'a>(
        &'a self,
        _input: DecisionInput<'a>,
    ) -> BoxFuture<'a, Result<ExitStatus, DeciderError>> {
        Box::pin(async move {
            Ok(ExitStatus::new(
                ExitCode::new(self.code).map_err(|_| DeciderError::new())?,
            ))
        })
    }
}

struct OrderedCodeDecider {
    name: &'static str,
    first: &'static str,
    code: &'static str,
    release: Arc<Notify>,
    observed_order: Arc<Mutex<Vec<&'static str>>>,
}

impl JobExecutionDecider for OrderedCodeDecider {
    fn decide<'a>(
        &'a self,
        _input: DecisionInput<'a>,
    ) -> BoxFuture<'a, Result<ExitStatus, DeciderError>> {
        Box::pin(async move {
            if self.name == self.first {
                self.release.notify_one();
            } else {
                self.release.notified().await;
            }
            self.observed_order
                .lock()
                .expect("ordering fixture mutex must not be poisoned")
                .push(self.name);
            Ok(ExitStatus::new(
                ExitCode::new(self.code).map_err(|_| DeciderError::new())?,
            ))
        })
    }
}

#[derive(Debug, Eq, PartialEq)]
struct NormalizedAdvancedObservation {
    job_status: BatchStatus,
    job_exit_status: ExitStatus,
    steps: Vec<(String, BatchStatus, ExitStatus)>,
    decisions: Vec<(u64, String, FlowTransitionKind, String, FlowTarget)>,
}

fn decision_branch(id: &str, code: &str) -> Result<FlowGraph, Box<dyn Error>> {
    let route = NodeId::new(id)?;
    Ok(FlowGraph::new(route.clone())
        .with_node(FlowNode::decision(DecisionNode::new(
            route.clone(),
            DeciderRevision::new(format!("{id}-v1"))?,
            DecisionInputVersion::new(1)?,
        )))
        .with_transition(FlowTransition::new(
            route,
            ExitPattern::new(code)?,
            FlowTarget::Terminal(TerminalKind::Complete),
        )))
}

fn decision_split_plan(
    name: &JobName,
    concurrency: u8,
) -> Result<oxide_batch::CompiledExecutionPlan, Box<dyn Error>> {
    let prepare = NodeId::new("prepare")?;
    let split = NodeId::new("parallel")?;
    let join = NodeId::new("joined")?;
    Ok(FlowGraph::new(prepare.clone())
        .with_node(step_node("prepare")?)
        .with_node(FlowNode::split(SplitNode::new(
            split.clone(),
            vec![
                SplitBranch::flow(decision_branch("first-route", "FIRST")?),
                SplitBranch::flow(decision_branch("second-route", "SECOND")?),
            ],
            join.clone(),
            SplitBudget::new(concurrency, u32::from(concurrency) + 1)?,
        )))
        .with_node(FlowNode::join(JoinNode::new(join.clone())))
        .with_sequence(prepare, FlowTarget::Node(split))?
        .with_sequence(join, FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(name, DefinitionRevision::new("v1")?)?)
}

fn normalize_advanced(report: &oxide_batch::FlowLaunchReport) -> NormalizedAdvancedObservation {
    let mut steps = report
        .step_executions()
        .iter()
        .map(|step| {
            (
                step.step_name().as_str().to_owned(),
                step.metadata().status(),
                step.metadata().exit_status().clone(),
            )
        })
        .collect::<Vec<_>>();
    steps.sort_by(|left, right| left.0.cmp(&right.0));
    let mut decisions = report
        .decisions()
        .iter()
        .map(|decision| {
            (
                decision.sequence().get(),
                decision.source_node_id().as_str().to_owned(),
                decision.kind(),
                decision.observed_outcome().as_str().to_owned(),
                decision.target().clone(),
            )
        })
        .collect::<Vec<_>>();
    decisions.sort_by_key(|decision| decision.0);
    NormalizedAdvancedObservation {
        job_status: report.job_execution().metadata().status(),
        job_exit_status: report.job_execution().metadata().exit_status().clone(),
        steps,
        decisions,
    }
}

async fn run_decision_split(
    concurrency: u8,
    first: Option<&'static str>,
) -> Result<(NormalizedAdvancedObservation, Vec<&'static str>), Box<dyn Error>> {
    let name = JobName::new("advanced-split-normalized")?;
    let mut job = FlowJob::new(name.clone(), decision_split_plan(&name, concurrency)?)?
        .with_tasklet_step(
            NodeId::new("prepare")?,
            tasklet("prepare", Arc::new(AtomicUsize::new(0)), Outcome::Complete)?,
        )?;
    let order = Arc::new(Mutex::new(Vec::new()));
    if let Some(first) = first {
        let release = Arc::new(Notify::new());
        for (node, code) in [("first-route", "FIRST"), ("second-route", "SECOND")] {
            job = job.with_decider(
                NodeId::new(node)?,
                Arc::new(OrderedCodeDecider {
                    name: node,
                    first,
                    code,
                    release: Arc::clone(&release),
                    observed_order: Arc::clone(&order),
                }),
            )?;
        }
    } else {
        for (node, code) in [("first-route", "FIRST"), ("second-route", "SECOND")] {
            job = job.with_decider(NodeId::new(node)?, Arc::new(CodeDecider { code }))?;
        }
    }
    let (clock, ids, repository) = infrastructure();
    let (_, stop) = StopSource::new();
    let report = FlowLauncher::new(&repository, clock.as_ref(), ids.as_ref())
        .launch(&job, &JobParameters::new(), &stop)
        .await?;
    verify_durable_decision_order(&repository, &report).await?;
    let observed_order = order
        .lock()
        .expect("ordering fixture mutex must not be poisoned")
        .clone();
    Ok((normalize_advanced(&report), observed_order))
}

async fn verify_durable_decision_order(
    repository: &InMemoryJobRepository,
    report: &oxide_batch::FlowLaunchReport,
) -> Result<(), Box<dyn Error>> {
    let execution = report.job_execution().id();
    let expected = report
        .decisions()
        .iter()
        .map(|decision| (decision.sequence().get(), decision.id()))
        .collect::<Vec<_>>();
    let mut transaction = repository.begin().await?;
    let stored = transaction.flow_decisions(execution).await?;
    transaction.rollback().await?;
    let actual = stored
        .iter()
        .map(|decision| (decision.sequence().get(), decision.id()))
        .collect::<Vec<_>>();
    assert_eq!(
        actual, expected,
        "repository must return logical sequence order"
    );

    let explorer = JobExplorer::new(InMemoryExplorer::new(repository));
    for size in [1, 2, 3] {
        let size = PageSize::new(size)?;
        let mut request = PageRequest::first(size);
        let mut paged = Vec::new();
        let mut exhausted = false;
        // The extra empty page is allowed, but an endlessly repeated cursor is not.
        for _ in 0..=expected.len() {
            let page = explorer.list_flow_decisions(execution, &request).await?;
            paged.extend(
                page.rows()
                    .iter()
                    .map(|decision| (decision.sequence().get(), decision.id())),
            );
            let Some(cursor) = page.next_cursor() else {
                exhausted = true;
                break;
            };
            request = PageRequest::resume(size, cursor.clone());
        }
        assert!(
            exhausted,
            "bounded traversal must exhaust the captured history"
        );
        assert_eq!(
            paged, expected,
            "keyset pagination must not skip or duplicate decisions"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn split_subgraph_completion_order_does_not_change_normalized_observation()
-> Result<(), Box<dyn Error>> {
    let (first_then_second, first_order) = run_decision_split(2, Some("first-route")).await?;
    let (second_then_first, second_order) = run_decision_split(2, Some("second-route")).await?;

    assert_eq!(first_order, vec!["first-route", "second-route"]);
    assert_eq!(second_order, vec!["second-route", "first-route"]);
    assert_eq!(first_then_second, second_then_first);
    assert_eq!(first_then_second.job_status, BatchStatus::Completed);
    let aggregate = first_then_second
        .decisions
        .iter()
        .find(|decision| decision.1 == "joined")
        .expect("split aggregate decision must be committed");
    assert_eq!(aggregate.2, FlowTransitionKind::SplitAggregate);
    assert_eq!(aggregate.3, "COMPLETED");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn split_subgraph_concurrency_one_matches_parallel_normalized_observation()
-> Result<(), Box<dyn Error>> {
    let (sequential, sequential_order) = run_decision_split(1, None).await?;
    let (parallel, parallel_order) = run_decision_split(2, None).await?;

    assert!(sequential_order.is_empty());
    assert!(parallel_order.is_empty());
    assert_eq!(sequential, parallel);
    assert_eq!(sequential.job_status, BatchStatus::Completed);
    Ok(())
}
