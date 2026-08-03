//! Manifest-format-3 and bounded local-scale plan conformance.

use std::error::Error;
use std::fmt::Write;

use oxide_batch::{
    ComponentRevision, DefinitionManifest, DefinitionRevision, FlowGraph, FlowNode, FlowTarget,
    JobName, JoinNode, NodeId, PartitionBudget, PartitionCount, PartitionedStepNode, PlanError,
    SplitBranch, SplitBudget, SplitNode, StepComponents, StepName, StepNode, TerminalKind,
};

fn tasklet(id: &str) -> Result<StepNode, Box<dyn Error>> {
    Ok(StepNode::new(
        NodeId::new(id)?,
        StepName::new(id)?,
        StepComponents::Tasklet(ComponentRevision::new(format!("{id}-v1"))?),
    ))
}

fn local_scale_graph(
    declaration_order_reversed: bool,
    workers: u8,
) -> Result<FlowGraph, Box<dyn Error>> {
    let prepare = NodeId::new("prepare")?;
    let split = NodeId::new("parallel_import")?;
    let join = NodeId::new("parallel_join")?;
    let partition = NodeId::new("partition_customers")?;

    let split_node = FlowNode::split(SplitNode::new(
        split.clone(),
        vec![
            SplitBranch::new(vec![tasklet("load_accounts")?, tasklet("index_accounts")?]),
            SplitBranch::new(vec![tasklet("load_orders")?]),
        ],
        join.clone(),
        SplitBudget::new(2, 3)?,
    ));
    let partition_node = FlowNode::partitioned_step(PartitionedStepNode::new(
        partition.clone(),
        StepName::new("partition_customers")?,
        tasklet("customer_worker")?,
        ComponentRevision::new("customer-partitioner-v1")?,
        ComponentRevision::new("status-and-counts-v1")?,
        PartitionCount::new(10)?,
        PartitionBudget::new(workers, u32::from(workers).saturating_add(1))?,
    ));

    let nodes = if declaration_order_reversed {
        vec![
            partition_node,
            FlowNode::join(JoinNode::new(join.clone())),
            split_node,
            FlowNode::step(tasklet("prepare")?),
        ]
    } else {
        vec![
            FlowNode::step(tasklet("prepare")?),
            split_node,
            FlowNode::join(JoinNode::new(join.clone())),
            partition_node,
        ]
    };
    let mut graph = FlowGraph::new(prepare.clone());
    for node in nodes {
        graph = graph.with_node(node);
    }
    Ok(graph
        .with_sequence(prepare, FlowTarget::Node(split))?
        .with_sequence(join, FlowTarget::Node(partition.clone()))?
        .with_sequence(partition, FlowTarget::Terminal(TerminalKind::Complete))?)
}

#[test]
fn format3_manifest_has_a_golden_fingerprint() -> Result<(), Box<dyn Error>> {
    let plan = local_scale_graph(false, 4)?.compile(
        &JobName::new("bounded_local_scale")?,
        DefinitionRevision::new("v1")?,
    )?;

    assert_eq!(plan.manifest_format(), 3);
    assert_eq!(plan.node_count(), 4);
    assert_eq!(plan.transition_count(), 6);
    assert_eq!(
        hex(plan.fingerprint()),
        "f5ee7c2d6923411c8c068b6c2770b95575256833bddaed1be9c3893324c541a9"
    );
    let decoded = DefinitionManifest::read_verified(
        plan.definition_identity().canonical_manifest(),
        plan.fingerprint(),
    )?;
    assert_eq!(decoded.format(), 3);
    assert_eq!(decoded.node_count(), Some(4));
    assert_eq!(decoded.transition_count(), Some(6));
    Ok(())
}

#[test]
fn declaration_order_does_not_change_format3_identity() -> Result<(), Box<dyn Error>> {
    let forward = local_scale_graph(false, 4)?.compile(
        &JobName::new("bounded_local_scale")?,
        DefinitionRevision::new("v1")?,
    )?;
    let reversed = local_scale_graph(true, 4)?.compile(
        &JobName::new("bounded_local_scale")?,
        DefinitionRevision::new("v1")?,
    )?;

    assert_eq!(
        forward.definition_identity().canonical_manifest(),
        reversed.definition_identity().canonical_manifest()
    );
    assert_eq!(forward.fingerprint(), reversed.fingerprint());
    Ok(())
}

/// M4 pinned the opposite expectation: it asserted that a worker-count change
/// altered the format-3 fingerprint. ADR-0009 withdrew that expectation, because
/// a worker count reaches neither the partitioner's inputs nor the aggregation
/// order and therefore selects no durable state. The assignment identity that
/// does participate is asserted by
/// [`partition_count_changes_the_format3_fingerprint`].
#[test]
fn worker_budget_does_not_change_the_format3_fingerprint() -> Result<(), Box<dyn Error>> {
    let four = local_scale_graph(false, 4)?.compile(
        &JobName::new("bounded_local_scale")?,
        DefinitionRevision::new("v1")?,
    )?;
    let ten = local_scale_graph(false, 10)?.compile(
        &JobName::new("bounded_local_scale")?,
        DefinitionRevision::new("v1")?,
    )?;

    assert_eq!(four.fingerprint(), ten.fingerprint());
    assert_eq!(
        four.definition_identity().canonical_manifest(),
        ten.definition_identity().canonical_manifest()
    );
    Ok(())
}

#[test]
fn partition_count_changes_the_format3_fingerprint() -> Result<(), Box<dyn Error>> {
    let partitioned = |count: u16| -> Result<[u8; 32], Box<dyn Error>> {
        let partition = NodeId::new("partition_customers")?;
        let plan = FlowGraph::new(partition.clone())
            .with_node(FlowNode::partitioned_step(PartitionedStepNode::new(
                partition.clone(),
                StepName::new("partition_customers")?,
                tasklet("customer_worker")?,
                ComponentRevision::new("customer-partitioner-v1")?,
                ComponentRevision::new("status-and-counts-v1")?,
                PartitionCount::new(count)?,
                PartitionBudget::new(4, 5)?,
            )))
            .with_sequence(partition, FlowTarget::Terminal(TerminalKind::Complete))?
            .compile(
                &JobName::new("bounded_local_scale")?,
                DefinitionRevision::new("v1")?,
            )?;
        Ok(*plan.fingerprint())
    };

    assert_ne!(partitioned(10)?, partitioned(11)?);
    Ok(())
}

#[test]
fn split_outside_the_accepted_subset_is_rejected() -> Result<(), Box<dyn Error>> {
    let split = NodeId::new("split")?;
    let join = NodeId::new("join")?;
    let graph = FlowGraph::new(split.clone())
        .with_node(FlowNode::split(SplitNode::new(
            split.clone(),
            vec![
                SplitBranch::new(vec![tasklet("left")?]),
                SplitBranch::new(vec![tasklet("right")?]),
            ],
            join.clone(),
            SplitBudget::new(2, 3)?,
        )))
        .with_node(FlowNode::join(JoinNode::new(join.clone())))
        .with_sequence(join, FlowTarget::Terminal(TerminalKind::Complete))?;

    assert_eq!(
        graph.compile(
            &JobName::new("invalid_split")?,
            DefinitionRevision::new("v1")?
        ),
        Err(PlanError::SplitIsEntry { split })
    );
    Ok(())
}

#[test]
fn embedded_step_identity_cannot_alias_a_top_level_node() -> Result<(), Box<dyn Error>> {
    let entry = NodeId::new("entry")?;
    let split = NodeId::new("split")?;
    let join = NodeId::new("join")?;
    let graph = FlowGraph::new(entry.clone())
        .with_node(FlowNode::step(tasklet("entry")?))
        .with_node(FlowNode::split(SplitNode::new(
            split.clone(),
            vec![
                SplitBranch::new(vec![tasklet("entry")?]),
                SplitBranch::new(vec![tasklet("right")?]),
            ],
            join.clone(),
            SplitBudget::new(2, 3)?,
        )))
        .with_node(FlowNode::join(JoinNode::new(join.clone())))
        .with_sequence(entry.clone(), FlowTarget::Node(split))?
        .with_sequence(join, FlowTarget::Terminal(TerminalKind::Complete))?;

    assert_eq!(
        graph.compile(
            &JobName::new("duplicate_identity")?,
            DefinitionRevision::new("v1")?
        ),
        Err(PlanError::DuplicateNodeId { node: entry })
    );
    Ok(())
}

#[test]
fn empty_branch_and_external_join_entry_fail_closed() -> Result<(), Box<dyn Error>> {
    let entry = NodeId::new("entry")?;
    let split = NodeId::new("split")?;
    let join = NodeId::new("join")?;
    let empty_branch = FlowGraph::new(entry.clone())
        .with_node(FlowNode::step(tasklet("entry")?))
        .with_node(FlowNode::split(SplitNode::new(
            split.clone(),
            vec![
                SplitBranch::new(Vec::new()),
                SplitBranch::new(vec![tasklet("right")?]),
            ],
            join.clone(),
            SplitBudget::new(1, 2)?,
        )))
        .with_node(FlowNode::join(JoinNode::new(join.clone())))
        .with_sequence(entry.clone(), FlowTarget::Node(split.clone()))?
        .with_sequence(join.clone(), FlowTarget::Terminal(TerminalKind::Complete))?;
    assert_eq!(
        empty_branch.compile(
            &JobName::new("empty_branch")?,
            DefinitionRevision::new("v1")?
        ),
        Err(PlanError::InvalidBranchLength {
            split: split.clone(),
            max: 8,
        })
    );

    let external_join = FlowGraph::new(entry.clone())
        .with_node(FlowNode::step(tasklet("entry")?))
        .with_node(FlowNode::split(SplitNode::new(
            split,
            vec![
                SplitBranch::new(vec![tasklet("left")?]),
                SplitBranch::new(vec![tasklet("right")?]),
            ],
            join.clone(),
            SplitBudget::new(1, 2)?,
        )))
        .with_node(FlowNode::join(JoinNode::new(join.clone())))
        .with_sequence(entry, FlowTarget::Node(join.clone()))?
        .with_sequence(join.clone(), FlowTarget::Terminal(TerminalKind::Complete))?;
    assert_eq!(
        external_join.compile(
            &JobName::new("external_join")?,
            DefinitionRevision::new("v1")?
        ),
        Err(PlanError::JoinHasExternalEntry { join })
    );
    Ok(())
}

#[test]
fn zero_unbounded_and_contradictory_budgets_are_rejected() {
    assert_eq!(
        SplitBudget::new(0, 2),
        Err(PlanError::InvalidParallelBranchBudget { max: 8 })
    );
    assert_eq!(
        PartitionBudget::new(65, 66),
        Err(PlanError::InvalidPartitionWorkerBudget { max: 64 })
    );
    assert_eq!(
        SplitBudget::new(2, 2),
        Err(PlanError::InsufficientPoolCapacity {
            required: 3,
            configured: 2,
        })
    );
    assert_eq!(
        PartitionBudget::new(4, 4),
        Err(PlanError::InsufficientPoolCapacity {
            required: 5,
            configured: 4,
        })
    );
    assert_eq!(
        PartitionCount::new(0),
        Err(PlanError::InvalidPartitionCount { max: 1_024 })
    );
    assert_eq!(
        PartitionCount::new(1_025),
        Err(PlanError::InvalidPartitionCount { max: 1_024 })
    );
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(
        String::with_capacity(bytes.len().saturating_mul(2)),
        |mut encoded, byte| {
            let _ = write!(encoded, "{byte:02x}");
            encoded
        },
    )
}
