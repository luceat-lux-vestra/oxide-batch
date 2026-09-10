use super::{
    BTreeMap, BTreeSet, CompiledExecutionPlan, CompiledFlowScope, DefinitionError,
    DefinitionIdentity, DefinitionRevision, ExitCode, FlowGraph, FlowNode, FlowTarget,
    FlowTransition, JobName, MAX_BRANCH_STEPS, MAX_FLOW_COMPOSITION_DEPTH, MAX_NODES,
    MAX_OUTGOING_TRANSITIONS, MAX_SPLIT_BRANCHES, MAX_TRANSITIONS, NestedFlow, NodeId, PlanError,
    StepNode, TerminalKind, Value, check_unambiguous, json,
};

#[derive(Clone, Debug)]
struct NestedRecord {
    scope: CompiledFlowScope,
    exits: Vec<FlowTransition>,
}

type PreparedScope = (
    NodeId,
    BTreeMap<NodeId, FlowNode>,
    BTreeMap<NodeId, NestedFlow>,
    Vec<FlowTransition>,
);
type TransitionPartitions = (
    BTreeMap<NodeId, Vec<FlowTransition>>,
    BTreeMap<NodeId, Vec<FlowTransition>>,
);

pub(super) fn compile(
    graph: FlowGraph,
    job_name: &JobName,
    revision: DefinitionRevision,
) -> Result<CompiledExecutionPlan, PlanError> {
    let mut compiler = AdvancedCompiler::default();
    let root_scope = compiler.compile_scope(graph, 1)?;
    let decision_sequences = compiler.assign_decision_sequences()?;
    let manifest = compiler.manifest(job_name, &root_scope, &decision_sequences);
    let canonical = serde_json::to_vec(&manifest)
        .map_err(|_| PlanError::Manifest(DefinitionError::ManifestEncoding))?;
    let definition = DefinitionIdentity::from_flow_manifest(job_name, revision, &canonical)
        .map_err(PlanError::Manifest)?;

    Ok(CompiledExecutionPlan {
        definition,
        entry: root_scope.entry().clone(),
        root_scope,
        nodes: compiler.nodes,
        transitions: compiler.transitions,
        split_branch_scopes: compiler.split_branch_scopes,
        nested_scopes: compiler
            .nested_records
            .into_iter()
            .map(|(id, record)| (id, record.scope))
            .collect(),
        decision_sequences,
    })
}

#[derive(Default)]
struct AdvancedCompiler {
    nodes: BTreeMap<NodeId, FlowNode>,
    transitions: BTreeMap<NodeId, Vec<FlowTransition>>,
    split_branch_scopes: BTreeMap<(NodeId, usize), CompiledFlowScope>,
    nested_records: BTreeMap<NodeId, NestedRecord>,
    all_ids: BTreeSet<NodeId>,
    structural_branches: usize,
    declared_transitions: usize,
}

impl AdvancedCompiler {
    fn compile_scope(
        &mut self,
        graph: FlowGraph,
        depth: usize,
    ) -> Result<CompiledFlowScope, PlanError> {
        let (declared_entry, local_nodes, nested, transitions) =
            self.prepare_scope(graph, depth)?;
        let nested_scopes = self.compile_nested_scopes(nested, depth)?;
        self.materialize_local_structure(&declared_entry, &local_nodes, depth)?;
        let (mut outgoing, mut nested_exits) =
            Self::partition_transitions(transitions, &local_nodes, &nested_scopes)?;
        Self::validate_nested_exits(&nested_scopes, &mut nested_exits)?;
        self.normalize_nested_boundaries(&nested_scopes, &nested_exits, &mut outgoing)?;

        let mut members: BTreeSet<NodeId> = local_nodes.keys().cloned().collect();
        for scope in nested_scopes.values() {
            members.extend(scope.members().cloned());
        }
        let entry = nested_scopes
            .get(&declared_entry)
            .map_or_else(|| declared_entry.clone(), |scope| scope.entry().clone());

        // Parent direct nodes are executable global plan members. Advanced
        // split-branch nodes were inserted by their own recursive scope but are
        // deliberately not members of this scope.
        self.install_local_nodes(local_nodes)?;
        self.install_transitions(outgoing)?;
        self.validate_scope(&entry, &members)?;
        Ok(CompiledFlowScope::new(entry, members))
    }

    fn prepare_scope(
        &mut self,
        graph: FlowGraph,
        depth: usize,
    ) -> Result<PreparedScope, PlanError> {
        if depth > MAX_FLOW_COMPOSITION_DEPTH {
            return Err(PlanError::CompositionDepthExceeded {
                max: MAX_FLOW_COMPOSITION_DEPTH,
            });
        }
        self.declared_transitions = self
            .declared_transitions
            .checked_add(graph.transitions.len())
            .ok_or(PlanError::TooManyTransitions {
                max: MAX_TRANSITIONS,
            })?;
        if self.declared_transitions > MAX_TRANSITIONS {
            return Err(PlanError::TooManyTransitions {
                max: MAX_TRANSITIONS,
            });
        }

        let declared_entry = graph.entry.ok_or(PlanError::MissingEntryNode)?;
        let mut local_nodes = BTreeMap::new();
        for node in graph.nodes {
            if local_nodes
                .insert(node.id().clone(), node.clone())
                .is_some()
            {
                return Err(PlanError::DuplicateNodeId {
                    node: node.id().clone(),
                });
            }
        }
        let mut nested = BTreeMap::new();
        for flow in graph.nested_flows {
            let id = flow.id().clone();
            if local_nodes.contains_key(&id) || nested.insert(id.clone(), flow).is_some() {
                return Err(PlanError::DuplicateNodeId { node: id });
            }
        }
        if !local_nodes.contains_key(&declared_entry) && !nested.contains_key(&declared_entry) {
            return Err(PlanError::UndefinedNode {
                node: declared_entry,
            });
        }
        for id in local_nodes.keys().chain(nested.keys()) {
            self.reserve_id(id)?;
        }
        Ok((declared_entry, local_nodes, nested, graph.transitions))
    }

    fn compile_nested_scopes(
        &mut self,
        nested: BTreeMap<NodeId, NestedFlow>,
        depth: usize,
    ) -> Result<BTreeMap<NodeId, CompiledFlowScope>, PlanError> {
        // Compile child scopes before parent transitions are normalized. Nested
        // flow owners remain structural records; executable child nodes live in
        // this same plan and are spliced into the parent scope below.
        let mut scopes = BTreeMap::new();
        for (id, flow) in nested {
            scopes.insert(id, self.compile_scope(*flow.graph, depth + 1)?);
        }
        Ok(scopes)
    }

    fn materialize_local_structure(
        &mut self,
        declared_entry: &NodeId,
        local_nodes: &BTreeMap<NodeId, FlowNode>,
        depth: usize,
    ) -> Result<(), PlanError> {
        let mut join_owners: BTreeMap<NodeId, NodeId> = BTreeMap::new();
        let join_ids: BTreeSet<NodeId> = local_nodes
            .iter()
            .filter_map(|(id, node)| matches!(node, FlowNode::Join(_)).then_some(id.clone()))
            .collect();
        for (id, node) in local_nodes {
            match node {
                FlowNode::Split(split) => {
                    self.materialize_split(
                        declared_entry,
                        id,
                        split,
                        &join_ids,
                        &mut join_owners,
                        depth,
                    )?;
                }
                FlowNode::PartitionedStep(partitioned) => {
                    self.reserve_id(partitioned.worker().id())?;
                }
                FlowNode::Step(_) | FlowNode::Decision(_) | FlowNode::Join(_) => {}
            }
        }
        for (id, node) in local_nodes {
            if matches!(node, FlowNode::Join(_)) && !join_owners.contains_key(id) {
                return Err(PlanError::OrphanJoin { join: id.clone() });
            }
        }
        Ok(())
    }

    fn materialize_split(
        &mut self,
        declared_entry: &NodeId,
        id: &NodeId,
        split: &super::SplitNode,
        join_ids: &BTreeSet<NodeId>,
        join_owners: &mut BTreeMap<NodeId, NodeId>,
        depth: usize,
    ) -> Result<(), PlanError> {
        if id == declared_entry {
            return Err(PlanError::SplitIsEntry { split: id.clone() });
        }
        if !(2..=MAX_SPLIT_BRANCHES).contains(&split.branches().len()) {
            return Err(PlanError::InvalidSplitBranchCount {
                split: id.clone(),
                min: 2,
                max: MAX_SPLIT_BRANCHES,
            });
        }
        if usize::from(split.budget().max_parallel_branches()) > split.branches().len() {
            return Err(PlanError::ParallelBudgetExceedsBranches {
                split: id.clone(),
                branches: split.branches().len(),
            });
        }
        if !join_ids.contains(split.join()) {
            return Err(PlanError::InvalidSplitJoin {
                split: id.clone(),
                join: split.join().clone(),
            });
        }
        if let Some(first) = join_owners.insert(split.join().clone(), id.clone()) {
            return Err(PlanError::JoinHasMultipleOwners {
                join: split.join().clone(),
                first,
                second: id.clone(),
            });
        }
        for (ordinal, branch) in split.branches().iter().enumerate() {
            self.structural_branches = self.structural_branches.saturating_add(1);
            self.check_materialized_nodes()?;
            if let Some(flow) = branch.flow_graph() {
                let scope = self.compile_scope(flow.clone(), depth + 1)?;
                self.split_branch_scopes
                    .insert((id.clone(), ordinal), scope);
                continue;
            }
            if !(1..=MAX_BRANCH_STEPS).contains(&branch.steps().len()) {
                return Err(PlanError::InvalidBranchLength {
                    split: id.clone(),
                    max: MAX_BRANCH_STEPS,
                });
            }
            for step in branch.steps() {
                self.reserve_id(step.id())?;
            }
        }
        Ok(())
    }

    fn partition_transitions(
        transitions: Vec<FlowTransition>,
        local_nodes: &BTreeMap<NodeId, FlowNode>,
        nested_scopes: &BTreeMap<NodeId, CompiledFlowScope>,
    ) -> Result<TransitionPartitions, PlanError> {
        let declared_ids: BTreeSet<NodeId> = local_nodes
            .keys()
            .chain(nested_scopes.keys())
            .cloned()
            .collect();
        let mut outgoing: BTreeMap<NodeId, Vec<FlowTransition>> = BTreeMap::new();
        let mut nested_exits: BTreeMap<NodeId, Vec<FlowTransition>> = BTreeMap::new();
        for transition in transitions {
            Self::validate_transition(&transition, &declared_ids, local_nodes)?;
            let edges = if nested_scopes.contains_key(transition.source()) {
                nested_exits.entry(transition.source().clone()).or_default()
            } else {
                outgoing.entry(transition.source().clone()).or_default()
            };
            if edges.len() == MAX_OUTGOING_TRANSITIONS {
                return Err(PlanError::TooManyOutgoingTransitions {
                    node: transition.source().clone(),
                    max: MAX_OUTGOING_TRANSITIONS,
                });
            }
            edges.push(transition);
        }
        Ok((outgoing, nested_exits))
    }

    fn validate_transition(
        transition: &FlowTransition,
        declared_ids: &BTreeSet<NodeId>,
        local_nodes: &BTreeMap<NodeId, FlowNode>,
    ) -> Result<(), PlanError> {
        if !declared_ids.contains(transition.source()) {
            return Err(PlanError::UndefinedNode {
                node: transition.source().clone(),
            });
        }
        if let FlowTarget::Node(target) = transition.target()
            && !declared_ids.contains(target)
        {
            return Err(PlanError::UndefinedNode {
                node: target.clone(),
            });
        }
        if let FlowTarget::Node(target) = transition.target()
            && matches!(local_nodes.get(target), Some(FlowNode::Join(_)))
        {
            return Err(PlanError::JoinHasExternalEntry {
                join: target.clone(),
            });
        }
        if matches!(
            local_nodes.get(transition.source()),
            Some(FlowNode::Split(_))
        ) {
            return Err(PlanError::SplitHasExplicitTransition {
                split: transition.source().clone(),
            });
        }
        Ok(())
    }

    fn validate_nested_exits(
        nested_scopes: &BTreeMap<NodeId, CompiledFlowScope>,
        nested_exits: &mut BTreeMap<NodeId, Vec<FlowTransition>>,
    ) -> Result<(), PlanError> {
        for owner in nested_scopes.keys() {
            let edges = nested_exits
                .get_mut(owner)
                .filter(|edges| !edges.is_empty())
                .ok_or_else(|| PlanError::MissingTransition {
                    node: owner.clone(),
                })?;
            check_unambiguous(owner, edges)?;
            sort_transitions(edges);
        }
        Ok(())
    }

    fn normalize_nested_boundaries(
        &mut self,
        nested_scopes: &BTreeMap<NodeId, CompiledFlowScope>,
        nested_exits: &BTreeMap<NodeId, Vec<FlowTransition>>,
        outgoing: &mut BTreeMap<NodeId, Vec<FlowTransition>>,
    ) -> Result<(), PlanError> {
        // Parent transitions enter a nested flow through its compiled child
        // entry. Nested-flow exit edges are not runtime transitions: each child
        // terminal edge is rewritten to the owner-selected target, so the
        // child's ordinary durable decision remains the sole path authority.
        for edges in outgoing.values_mut() {
            for edge in edges.iter_mut() {
                edge.target = Self::resolve_nested_target(edge.target.clone(), nested_scopes);
            }
        }
        for (owner, scope) in nested_scopes {
            let exits =
                nested_exits
                    .get(owner)
                    .cloned()
                    .ok_or_else(|| PlanError::MissingTransition {
                        node: owner.clone(),
                    })?;
            let normalized = exits
                .iter()
                .cloned()
                .map(|mut edge| {
                    edge.target = Self::resolve_nested_target(edge.target, nested_scopes);
                    edge
                })
                .collect::<Vec<_>>();
            self.rewrite_nested_terminals(owner, scope, &normalized)?;
            self.nested_records.insert(
                owner.clone(),
                NestedRecord {
                    scope: scope.clone(),
                    exits: normalized,
                },
            );
        }
        Ok(())
    }

    fn install_local_nodes(
        &mut self,
        local_nodes: BTreeMap<NodeId, FlowNode>,
    ) -> Result<(), PlanError> {
        for (id, node) in local_nodes {
            if self.nodes.insert(id.clone(), node).is_some() {
                return Err(PlanError::DuplicateNodeId { node: id });
            }
        }
        Ok(())
    }

    fn install_transitions(
        &mut self,
        outgoing: BTreeMap<NodeId, Vec<FlowTransition>>,
    ) -> Result<(), PlanError> {
        for (source, mut edges) in outgoing {
            sort_transitions(&mut edges);
            if self.transitions.insert(source.clone(), edges).is_some() {
                return Err(PlanError::DuplicateNodeId { node: source });
            }
        }
        Ok(())
    }

    fn reserve_id(&mut self, id: &NodeId) -> Result<(), PlanError> {
        if !self.all_ids.insert(id.clone()) {
            return Err(PlanError::DuplicateNodeId { node: id.clone() });
        }
        self.check_materialized_nodes()
    }

    fn check_materialized_nodes(&self) -> Result<(), PlanError> {
        if self.all_ids.len().saturating_add(self.structural_branches) > MAX_NODES {
            return Err(PlanError::TooManyNodes { max: MAX_NODES });
        }
        Ok(())
    }

    fn resolve_nested_target(
        target: FlowTarget,
        nested_scopes: &BTreeMap<NodeId, CompiledFlowScope>,
    ) -> FlowTarget {
        match target {
            FlowTarget::Node(id) => nested_scopes
                .get(&id)
                .map_or(FlowTarget::Node(id), |scope| {
                    FlowTarget::Node(scope.entry().clone())
                }),
            FlowTarget::Terminal(kind) => FlowTarget::Terminal(kind),
        }
    }

    fn rewrite_nested_terminals(
        &mut self,
        owner: &NodeId,
        scope: &CompiledFlowScope,
        exits: &[FlowTransition],
    ) -> Result<(), PlanError> {
        for source in scope.members() {
            let Some(edges) = self.transitions.get_mut(source) else {
                continue;
            };
            for edge in edges.iter_mut() {
                let FlowTarget::Terminal(terminal) = edge.target() else {
                    continue;
                };
                let code = normalized_terminal_code(*terminal)?;
                let selected = exits
                    .iter()
                    .find(|candidate| candidate.pattern().matches(&code))
                    .ok_or_else(|| PlanError::UnmappedNestedFlowExit {
                        flow: owner.clone(),
                        code: code.clone(),
                    })?;
                edge.target = selected.target().clone();
            }
        }
        Ok(())
    }

    fn validate_scope(&self, entry: &NodeId, members: &BTreeSet<NodeId>) -> Result<(), PlanError> {
        if !members.contains(entry) {
            return Err(PlanError::UndefinedNode {
                node: entry.clone(),
            });
        }
        for id in members {
            let node = self
                .nodes
                .get(id)
                .ok_or_else(|| PlanError::UndefinedNode { node: id.clone() })?;
            if matches!(node, FlowNode::Split(_)) {
                continue;
            }
            let edges = self
                .transitions
                .get(id)
                .filter(|edges| !edges.is_empty())
                .ok_or_else(|| PlanError::MissingTransition { node: id.clone() })?;
            check_unambiguous(id, edges)?;
            for edge in edges {
                if let FlowTarget::Node(target) = edge.target()
                    && !members.contains(target)
                {
                    return Err(PlanError::UndefinedNode {
                        node: target.clone(),
                    });
                }
            }
        }

        let mut visited = BTreeSet::new();
        let mut on_path = BTreeSet::new();
        self.visit_scope(entry, members, &mut visited, &mut on_path)?;
        for id in members {
            if !visited.contains(id) {
                return Err(PlanError::UnreachableNode { node: id.clone() });
            }
        }
        Ok(())
    }

    fn visit_scope(
        &self,
        id: &NodeId,
        members: &BTreeSet<NodeId>,
        visited: &mut BTreeSet<NodeId>,
        on_path: &mut BTreeSet<NodeId>,
    ) -> Result<(), PlanError> {
        if on_path.contains(id) {
            return Err(PlanError::CyclicGraph { node: id.clone() });
        }
        if !visited.insert(id.clone()) {
            return Ok(());
        }
        on_path.insert(id.clone());
        if let Some(FlowNode::Split(split)) = self.nodes.get(id) {
            if !members.contains(split.join()) {
                return Err(PlanError::InvalidSplitJoin {
                    split: id.clone(),
                    join: split.join().clone(),
                });
            }
            self.visit_scope(split.join(), members, visited, on_path)?;
        }
        if let Some(edges) = self.transitions.get(id) {
            for edge in edges {
                if let FlowTarget::Node(target) = edge.target() {
                    self.visit_scope(target, members, visited, on_path)?;
                }
            }
        }
        on_path.remove(id);
        Ok(())
    }

    fn assign_decision_sequences(&self) -> Result<BTreeMap<NodeId, u64>, PlanError> {
        let mut sequences = BTreeMap::new();
        for (index, source) in self.transitions.keys().enumerate() {
            let sequence = u64::try_from(index)
                .ok()
                .and_then(|value| value.checked_add(1))
                .ok_or(PlanError::TooManyTransitions {
                    max: MAX_TRANSITIONS,
                })?;
            sequences.insert(source.clone(), sequence);
        }
        Ok(sequences)
    }

    fn manifest(
        &self,
        job_name: &JobName,
        root_scope: &CompiledFlowScope,
        decision_sequences: &BTreeMap<NodeId, u64>,
    ) -> Value {
        let mut node_values: BTreeMap<NodeId, Value> = self
            .nodes
            .iter()
            .map(|(id, node)| {
                (
                    id.clone(),
                    advanced_node_manifest(
                        node,
                        &self.split_branch_scopes,
                        decision_sequences.get(id).copied(),
                    ),
                )
            })
            .collect();
        for (owner, record) in &self.nested_records {
            node_values.insert(owner.clone(), nested_manifest(owner, record));
        }
        let transitions = self
            .transitions
            .values()
            .flat_map(|edges| edges.iter().map(FlowTransition::manifest_value))
            .collect::<Vec<_>>();
        json!({
            "entry": root_scope.entry().as_str(),
            "format": oxide_batch_core::MANIFEST_FORMAT_ADVANCED_FLOW,
            "job": job_name.as_str(),
            "nodes": node_values.into_values().collect::<Vec<_>>(),
            "transitions": transitions
        })
    }
}

fn advanced_node_manifest(
    node: &FlowNode,
    branch_scopes: &BTreeMap<(NodeId, usize), CompiledFlowScope>,
    decision_sequence: Option<u64>,
) -> Value {
    let mut value = match node {
        FlowNode::Split(split) => {
            let branches = split
                .branches()
                .iter()
                .enumerate()
                .map(|(ordinal, branch)| {
                    branch_scopes.get(&(split.id().clone(), ordinal)).map_or_else(
                        || {
                            json!({
                                "kind": "linear",
                                "ordinal": ordinal.saturating_add(1),
                                "steps": branch
                                    .steps()
                                    .iter()
                                    .map(StepNode::manifest_value)
                                    .collect::<Vec<_>>()
                            })
                        },
                        |scope| {
                            json!({
                                "entry": scope.entry().as_str(),
                                "kind": "flow",
                                "members": scope.members().map(NodeId::as_str).collect::<Vec<_>>(),
                                "ordinal": ordinal.saturating_add(1)
                            })
                        },
                    )
                })
                .collect::<Vec<_>>();
            json!({
                "branches": branches,
                "failure_policy": split.failure_policy().as_str(),
                "id": split.id().as_str(),
                "join": split.join().as_str(),
                "kind": "split"
            })
        }
        _ => node.manifest_value(),
    };
    if let Some(sequence) = decision_sequence
        && let Some(object) = value.as_object_mut()
    {
        object.insert("decision_sequence".to_owned(), json!(sequence));
    }
    value
}

fn nested_manifest(owner: &NodeId, record: &NestedRecord) -> Value {
    json!({
        "entry": record.scope.entry().as_str(),
        "exits": record.exits.iter().map(FlowTransition::manifest_value).collect::<Vec<_>>(),
        "id": owner.as_str(),
        "kind": "nested_flow",
        "members": record.scope.members().map(NodeId::as_str).collect::<Vec<_>>()
    })
}

fn normalized_terminal_code(terminal: TerminalKind) -> Result<ExitCode, PlanError> {
    let value = match terminal {
        TerminalKind::Complete => "COMPLETED",
        TerminalKind::Fail => "FAILED",
        TerminalKind::Stop => "STOPPED",
        _ => return Err(PlanError::Manifest(DefinitionError::ManifestEncoding)),
    };
    ExitCode::new(value).map_err(|_| PlanError::Manifest(DefinitionError::ManifestEncoding))
}

fn sort_transitions(edges: &mut [FlowTransition]) {
    edges.sort_by(|left, right| {
        right
            .pattern()
            .specificity()
            .cmp(&left.pattern().specificity())
            .then_with(|| left.pattern().cmp(right.pattern()))
            .then_with(|| left.target().sort_key().cmp(&right.target().sort_key()))
    });
}
