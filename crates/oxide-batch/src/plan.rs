//! Immutable flow graphs and the compiled execution plans they lower into.
//!
//! An application declares a [`FlowGraph`] of step and decision nodes joined by
//! exit-pattern transitions, then compiles it into an immutable
//! [`CompiledExecutionPlan`]. Compilation normalizes the graph, rejects every
//! structural error the accepted basic-flow contract names, and produces the
//! canonical manifest whose SHA-256 digest is the definition fingerprint.
//!
//! The M3 slice is deliberately bounded: the graph is acyclic, terminals are
//! [`TerminalKind::Complete`], [`TerminalKind::Fail`], and
//! [`TerminalKind::Stop`], and split, nested, and remote nodes are not
//! expressible. Existing one-step [`TaskletJob`](crate::TaskletJob) and
//! [`ChunkJob`](crate::ChunkJob) definitions lower into a compatibility plan
//! that retains their original format-1 manifest bytes and fingerprint.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::num::NonZeroU32;

use serde_json::{Value, json};

use crate::definition::{definition_token, validate_token};
use crate::{
    ChunkComponentRevisions, ChunkSize, ComponentRevision, DefinitionError, DefinitionIdentity,
    DefinitionRevision, DefinitionTokenKind, ExitCode, FaultPolicy, JobName, StepName,
};

/// The maximum number of nodes one M3 plan may contain.
pub const MAX_NODES: usize = 1_024;
/// The maximum number of transitions one M3 plan may contain.
pub const MAX_TRANSITIONS: usize = 4_096;
/// The maximum number of transitions leaving one node.
pub const MAX_OUTGOING_TRANSITIONS: usize = 64;
/// The maximum length of one exit pattern in UTF-8 bytes.
pub const MAX_PATTERN_BYTES: usize = 64;

definition_token!(
    NodeId,
    DefinitionTokenKind::Node,
    "A stable logical identifier for one flow-graph node.

Logical identity survives display-name changes. Runtime and database
identifiers are never node identifiers."
);
definition_token!(
    DeciderRevision,
    DefinitionTokenKind::Decider,
    "An application-owned revision token for one deterministic decider."
);

/// The version of the durable input contract one decider reads.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DecisionInputVersion(NonZeroU32);

impl DecisionInputVersion {
    /// Constructs a nonzero durable input-contract version.
    ///
    /// # Errors
    ///
    /// Returns [`PlanError::ZeroDecisionInputVersion`] for zero.
    pub fn new(value: u32) -> Result<Self, PlanError> {
        NonZeroU32::new(value)
            .map(Self)
            .ok_or(PlanError::ZeroDecisionInputVersion)
    }

    /// Returns the version.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

/// The maximum number of step executions one logical step may start.
///
/// The default is `u32::MAX`: an effectively unrestricted step that remains a
/// finite typed value rather than an absent bound.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StartLimit(NonZeroU32);

impl StartLimit {
    /// The unrestricted default.
    pub const UNRESTRICTED: Self = Self(NonZeroU32::MAX);

    /// Constructs a nonzero start limit.
    ///
    /// # Errors
    ///
    /// Returns [`PlanError::ZeroStartLimit`] for zero, because a step that can
    /// never start is a definition mistake rather than a policy.
    pub fn new(value: u32) -> Result<Self, PlanError> {
        NonZeroU32::new(value)
            .map(Self)
            .ok_or(PlanError::ZeroStartLimit)
    }

    /// Returns the limit.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

impl Default for StartLimit {
    fn default() -> Self {
        Self::UNRESTRICTED
    }
}

/// Restart-relevant start controls for one logical step.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StartControls {
    start_limit: StartLimit,
    allow_start_if_complete: bool,
}

impl StartControls {
    /// Constructs explicit start controls.
    #[must_use]
    pub const fn new(start_limit: StartLimit, allow_start_if_complete: bool) -> Self {
        Self {
            start_limit,
            allow_start_if_complete,
        }
    }

    /// Returns the maximum number of starts for one job instance.
    #[must_use]
    pub const fn start_limit(&self) -> StartLimit {
        self.start_limit
    }

    /// Returns whether a restart path reruns an already completed step.
    #[must_use]
    pub const fn allow_start_if_complete(&self) -> bool {
        self.allow_start_if_complete
    }

    fn manifest_value(self) -> Value {
        json!({
            "allow_start_if_complete": self.allow_start_if_complete,
            "start_limit": self.start_limit.get()
        })
    }
}

/// A bounded exit-outcome pattern used to select one transition.
///
/// A pattern contains literal characters plus `*` for zero or more characters
/// and `?` for exactly one character. It matches the bounded
/// [`ExitCode`], never [`BatchStatus`](crate::BatchStatus).
///
/// ```
/// use oxide_batch::{ExitCode, ExitPattern};
///
/// let failed = ExitPattern::new("FAILED")?;
/// let any = ExitPattern::new("*")?;
/// assert!(failed.matches(&ExitCode::new("FAILED")?));
/// assert!(!failed.matches(&ExitCode::new("COMPLETED")?));
/// assert!(any.matches(&ExitCode::new("COMPLETED")?));
/// assert!(failed.specificity() > any.specificity());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ExitPattern(String);

impl ExitPattern {
    /// Validates and constructs an exit pattern.
    ///
    /// # Errors
    ///
    /// Returns [`PlanError::InvalidPattern`] for an empty pattern, a pattern
    /// longer than 64 UTF-8 bytes, surrounding whitespace, or a control
    /// character.
    pub fn new(value: impl Into<String>) -> Result<Self, PlanError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_PATTERN_BYTES
            || value.trim() != value
            || value.chars().any(char::is_control)
        {
            return Err(PlanError::InvalidPattern {
                max_bytes: MAX_PATTERN_BYTES,
            });
        }
        Ok(Self(value))
    }

    /// Borrows the validated pattern.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns whether this pattern matches one exit code.
    #[must_use]
    pub fn matches(&self, code: &ExitCode) -> bool {
        let pattern: Vec<char> = self.0.chars().collect();
        let value: Vec<char> = code.as_str().chars().collect();
        matches_from(&pattern, &value)
    }

    /// Returns the computed specificity used to order transitions.
    ///
    /// A greater specificity is evaluated first.
    #[must_use]
    pub fn specificity(&self) -> PatternSpecificity {
        let wildcards = self
            .0
            .chars()
            .filter(|character| matches!(character, '*' | '?'))
            .count();
        let literals = self.0.chars().count() - wildcards;
        PatternSpecificity {
            literals,
            wildcards,
            bytes: self.0.len(),
        }
    }

    /// Returns whether some exit code exists that both patterns match.
    #[must_use]
    pub fn intersects(&self, other: &Self) -> bool {
        let left: Vec<char> = self.0.chars().collect();
        let right: Vec<char> = other.0.chars().collect();
        let mut memo = vec![None; (left.len() + 1) * (right.len() + 1)];
        intersects_from(&left, &right, 0, 0, &mut memo)
    }
}

impl fmt::Display for ExitPattern {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

fn matches_from(pattern: &[char], value: &[char]) -> bool {
    let mut pattern_index = 0_usize;
    let mut value_index = 0_usize;
    let mut star: Option<(usize, usize)> = None;
    while value_index < value.len() {
        match pattern.get(pattern_index) {
            Some('*') => {
                star = Some((pattern_index, value_index));
                pattern_index += 1;
            }
            Some('?') => {
                pattern_index += 1;
                value_index += 1;
            }
            Some(literal) if *literal == value[value_index] => {
                pattern_index += 1;
                value_index += 1;
            }
            _ => match star {
                Some((star_index, resume)) => {
                    pattern_index = star_index + 1;
                    value_index = resume + 1;
                    star = Some((star_index, resume + 1));
                }
                None => return false,
            },
        }
    }
    pattern[pattern_index..]
        .iter()
        .all(|character| *character == '*')
}

fn intersects_from(
    left: &[char],
    right: &[char],
    left_index: usize,
    right_index: usize,
    memo: &mut [Option<bool>],
) -> bool {
    let key = left_index * (right.len() + 1) + right_index;
    if let Some(cached) = memo[key] {
        return cached;
    }
    let answer = match (left.get(left_index), right.get(right_index)) {
        (None, None) => true,
        (None, Some(_)) => right[right_index..].iter().all(|value| *value == '*'),
        (Some(_), None) => left[left_index..].iter().all(|value| *value == '*'),
        (Some('*'), _) => {
            intersects_from(left, right, left_index + 1, right_index, memo)
                || intersects_from(left, right, left_index, right_index + 1, memo)
        }
        (_, Some('*')) => {
            intersects_from(left, right, left_index, right_index + 1, memo)
                || intersects_from(left, right, left_index + 1, right_index, memo)
        }
        (Some(left_character), Some(right_character)) => {
            (*left_character == '?' || *right_character == '?' || left_character == right_character)
                && intersects_from(left, right, left_index + 1, right_index + 1, memo)
        }
    };
    memo[key] = Some(answer);
    answer
}

/// The computed specificity of one exit pattern.
///
/// Ordering compares more literal characters first, then fewer wildcards, then
/// a longer UTF-8 byte length. A greater value is evaluated first.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PatternSpecificity {
    literals: usize,
    wildcards: usize,
    bytes: usize,
}

impl PatternSpecificity {
    /// Returns the number of literal characters.
    #[must_use]
    pub const fn literals(self) -> usize {
        self.literals
    }

    /// Returns the number of `*` and `?` characters.
    #[must_use]
    pub const fn wildcards(self) -> usize {
        self.wildcards
    }

    /// Returns the pattern length in UTF-8 bytes.
    #[must_use]
    pub const fn bytes(self) -> usize {
        self.bytes
    }
}

impl Ord for PatternSpecificity {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.literals
            .cmp(&other.literals)
            .then_with(|| other.wildcards.cmp(&self.wildcards))
            .then_with(|| self.bytes.cmp(&other.bytes))
    }
}

impl PartialOrd for PatternSpecificity {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// A node that ends the job without starting further work.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum TerminalKind {
    /// The job completes.
    Complete,
    /// The job fails.
    Fail,
    /// The job stops and remains restartable.
    Stop,
}

impl TerminalKind {
    /// Returns the stable manifest and telemetry name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Fail => "fail",
            Self::Stop => "stop",
        }
    }
}

/// The destination one transition selects.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum FlowTarget {
    /// Another graph node starts next.
    Node(NodeId),
    /// The job ends at a terminal.
    Terminal(TerminalKind),
}

impl FlowTarget {
    fn manifest_value(&self) -> Value {
        match self {
            Self::Node(id) => json!({ "node": id.as_str() }),
            Self::Terminal(kind) => json!({ "terminal": kind.as_str() }),
        }
    }

    fn sort_key(&self) -> (u8, &str) {
        match self {
            Self::Node(id) => (0, id.as_str()),
            Self::Terminal(kind) => (1, kind.as_str()),
        }
    }
}

/// The executable kind and restart-relevant declaration of one step node.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum StepComponents {
    /// A single-invocation tasklet body.
    Tasklet(ComponentRevision),
    /// A restartable reader, processor, and writer pipeline.
    Chunk {
        /// The committed chunk size.
        size: ChunkSize,
        /// The restart-relevant component revisions and state schemas.
        revisions: Box<ChunkComponentRevisions>,
    },
}

impl StepComponents {
    fn kind_name(&self) -> &'static str {
        match self {
            Self::Tasklet(_) => "tasklet",
            Self::Chunk { .. } => "chunk",
        }
    }

    fn manifest_value(&self) -> Value {
        match self {
            Self::Tasklet(revision) => json!({
                "component": revision.as_str(),
                "delivery_mode": "best_effort",
                "transaction_boundary": "tasklet_completion"
            }),
            Self::Chunk { size, revisions } => {
                let mut chunk = revisions.manifest_value();
                if let Some(members) = chunk.as_object_mut() {
                    members.insert("size".to_owned(), json!(size.get()));
                    members.insert(
                        "transaction_boundary".to_owned(),
                        Value::String("chunk".to_owned()),
                    );
                }
                chunk
            }
        }
    }
}

/// One executable node of a compiled plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StepNode {
    id: NodeId,
    step_name: StepName,
    components: StepComponents,
    start: StartControls,
    fault: Option<FaultPolicy>,
    listeners: Vec<ComponentRevision>,
}

impl StepNode {
    /// Declares one step node.
    #[must_use]
    pub fn new(id: NodeId, step_name: StepName, components: StepComponents) -> Self {
        Self {
            id,
            step_name,
            components,
            start: StartControls::default(),
            fault: None,
            listeners: Vec::new(),
        }
    }

    /// Declares explicit start controls.
    #[must_use]
    pub const fn with_start_controls(mut self, start: StartControls) -> Self {
        self.start = start;
        self
    }

    /// Declares the fault policy this step's fingerprint captures.
    #[must_use]
    pub fn with_fault_policy(mut self, policy: FaultPolicy) -> Self {
        self.fault = Some(policy);
        self
    }

    /// Declares one authoritative listener revision in registration order.
    #[must_use]
    pub fn with_listener_revision(mut self, revision: ComponentRevision) -> Self {
        self.listeners.push(revision);
        self
    }

    /// Borrows the stable node identifier.
    #[must_use]
    pub const fn id(&self) -> &NodeId {
        &self.id
    }

    /// Borrows the durable step name.
    #[must_use]
    pub const fn step_name(&self) -> &StepName {
        &self.step_name
    }

    /// Borrows the executable component declaration.
    #[must_use]
    pub const fn components(&self) -> &StepComponents {
        &self.components
    }

    /// Returns the restart-relevant start controls.
    #[must_use]
    pub const fn start_controls(&self) -> StartControls {
        self.start
    }

    /// Borrows the declared fault policy.
    #[must_use]
    pub const fn fault_policy(&self) -> Option<&FaultPolicy> {
        self.fault.as_ref()
    }

    /// Borrows the authoritative listener revisions in registration order.
    #[must_use]
    pub fn listener_revisions(&self) -> &[ComponentRevision] {
        &self.listeners
    }

    fn manifest_value(&self) -> Value {
        json!({
            "id": self.id.as_str(),
            "kind": "step",
            "listeners": self
                .listeners
                .iter()
                .map(|revision| Value::String(revision.as_str().to_owned()))
                .collect::<Vec<_>>(),
            "policy": self.fault.as_ref().map_or(Value::Null, fault_manifest_value),
            "start": self.start.manifest_value(),
            "step": {
                "declaration": self.components.manifest_value(),
                "kind": self.components.kind_name(),
                "name": self.step_name.as_str()
            }
        })
    }
}

/// One deterministic decision node of a compiled plan.
///
/// M3 compiles and fingerprints decision nodes; executing them is owned by the
/// durable-flow workstream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecisionNode {
    id: NodeId,
    revision: DeciderRevision,
    input_version: DecisionInputVersion,
}

impl DecisionNode {
    /// Declares one decision node.
    #[must_use]
    pub const fn new(
        id: NodeId,
        revision: DeciderRevision,
        input_version: DecisionInputVersion,
    ) -> Self {
        Self {
            id,
            revision,
            input_version,
        }
    }

    /// Borrows the stable node identifier.
    #[must_use]
    pub const fn id(&self) -> &NodeId {
        &self.id
    }

    /// Borrows the application-owned decider revision.
    #[must_use]
    pub const fn revision(&self) -> &DeciderRevision {
        &self.revision
    }

    /// Returns the durable input-contract version.
    #[must_use]
    pub const fn input_version(&self) -> DecisionInputVersion {
        self.input_version
    }

    fn manifest_value(&self) -> Value {
        json!({
            "decision": {
                "input_version": self.input_version.get(),
                "revision": self.revision.as_str()
            },
            "id": self.id.as_str(),
            "kind": "decision"
        })
    }
}

/// One node of a declared flow graph.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum FlowNode {
    /// A tasklet or chunk step.
    Step(Box<StepNode>),
    /// A deterministic decision.
    Decision(DecisionNode),
}

impl FlowNode {
    /// Declares a step node.
    #[must_use]
    pub fn step(node: StepNode) -> Self {
        Self::Step(Box::new(node))
    }

    /// Declares a decision node.
    #[must_use]
    pub const fn decision(node: DecisionNode) -> Self {
        Self::Decision(node)
    }

    /// Borrows the node's stable identifier.
    #[must_use]
    pub const fn id(&self) -> &NodeId {
        match self {
            Self::Step(node) => node.id(),
            Self::Decision(node) => node.id(),
        }
    }

    fn manifest_value(&self) -> Value {
        match self {
            Self::Step(node) => node.manifest_value(),
            Self::Decision(node) => node.manifest_value(),
        }
    }
}

/// One declared transition edge.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FlowTransition {
    source: NodeId,
    pattern: ExitPattern,
    target: FlowTarget,
}

impl FlowTransition {
    /// Declares one directed transition selected by an exit pattern.
    #[must_use]
    pub const fn new(source: NodeId, pattern: ExitPattern, target: FlowTarget) -> Self {
        Self {
            source,
            pattern,
            target,
        }
    }

    /// Borrows the source node identifier.
    #[must_use]
    pub const fn source(&self) -> &NodeId {
        &self.source
    }

    /// Borrows the selecting pattern.
    #[must_use]
    pub const fn pattern(&self) -> &ExitPattern {
        &self.pattern
    }

    /// Borrows the selected target.
    #[must_use]
    pub const fn target(&self) -> &FlowTarget {
        &self.target
    }

    fn manifest_value(&self) -> Value {
        json!({
            "pattern": self.pattern.as_str(),
            "source": self.source.as_str(),
            "target": self.target.manifest_value()
        })
    }
}

/// An immutable declaration of the M3 flow subset.
///
/// ```
/// use oxide_batch::{
///     ComponentRevision, DefinitionRevision, ExitPattern, FlowGraph, FlowNode, FlowTarget,
///     FlowTransition, JobName, NodeId, StepComponents, StepNode, StepName, TerminalKind,
/// };
///
/// let load = NodeId::new("load")?;
/// let report = NodeId::new("report")?;
/// let plan = FlowGraph::new(load.clone())
///     .with_node(FlowNode::step(StepNode::new(
///         load.clone(),
///         StepName::new("load")?,
///         StepComponents::Tasklet(ComponentRevision::new("load-v1")?),
///     )))
///     .with_node(FlowNode::step(StepNode::new(
///         report.clone(),
///         StepName::new("report")?,
///         StepComponents::Tasklet(ComponentRevision::new("report-v1")?),
///     )))
///     .with_sequence(load, FlowTarget::Node(report.clone()))?
///     .with_sequence(report, FlowTarget::Terminal(TerminalKind::Complete))?
///     .compile(&JobName::new("daily_import")?, DefinitionRevision::new("v1")?)?;
///
/// assert_eq!(plan.manifest_format(), 2);
/// assert_eq!(plan.node_count(), 2);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FlowGraph {
    entry: Option<NodeId>,
    nodes: Vec<FlowNode>,
    transitions: Vec<FlowTransition>,
}

impl FlowGraph {
    /// Starts a graph at its entry node.
    #[must_use]
    pub fn new(entry: NodeId) -> Self {
        Self {
            entry: Some(entry),
            nodes: Vec::new(),
            transitions: Vec::new(),
        }
    }

    /// Declares one node.
    #[must_use]
    pub fn with_node(mut self, node: FlowNode) -> Self {
        self.nodes.push(node);
        self
    }

    /// Declares one explicit transition.
    #[must_use]
    pub fn with_transition(mut self, transition: FlowTransition) -> Self {
        self.transitions.push(transition);
        self
    }

    /// Declares the convenience sequential edge.
    ///
    /// A sequential edge compiles to an exact `FAILED` transition leading to
    /// [`TerminalKind::Fail`] and a less specific `*` transition leading to
    /// `next`. Custom successful exit codes therefore continue, while a failed
    /// step ends the job.
    ///
    /// # Errors
    ///
    /// Returns [`PlanError::InvalidPattern`] only if the framework-owned
    /// patterns cannot be constructed.
    pub fn with_sequence(self, source: NodeId, next: FlowTarget) -> Result<Self, PlanError> {
        Ok(self
            .with_transition(FlowTransition::new(
                source.clone(),
                ExitPattern::new("FAILED")?,
                FlowTarget::Terminal(TerminalKind::Fail),
            ))
            .with_transition(FlowTransition::new(source, ExitPattern::new("*")?, next)))
    }

    /// Validates and normalizes the graph into an immutable plan.
    ///
    /// # Errors
    ///
    /// Returns [`PlanError`] for a missing entry node, a duplicate logical
    /// identifier, an undefined source or target, a node without outgoing
    /// transitions, two equally specific patterns that can match one value, an
    /// unreachable node, a cycle, an exceeded bound, or a manifest that cannot
    /// be encoded within the durable limit.
    pub fn compile(
        self,
        job_name: &JobName,
        revision: DefinitionRevision,
    ) -> Result<CompiledExecutionPlan, PlanError> {
        let entry = self.entry.ok_or(PlanError::MissingEntryNode)?;
        if self.nodes.len() > MAX_NODES {
            return Err(PlanError::TooManyNodes { max: MAX_NODES });
        }
        if self.transitions.len() > MAX_TRANSITIONS {
            return Err(PlanError::TooManyTransitions {
                max: MAX_TRANSITIONS,
            });
        }

        let mut nodes = BTreeMap::new();
        for node in self.nodes {
            if nodes.insert(node.id().clone(), node.clone()).is_some() {
                return Err(PlanError::DuplicateNodeId {
                    node: node.id().clone(),
                });
            }
        }
        if !nodes.contains_key(&entry) {
            return Err(PlanError::UndefinedNode {
                node: entry.clone(),
            });
        }

        let mut outgoing: BTreeMap<NodeId, Vec<FlowTransition>> = BTreeMap::new();
        for transition in self.transitions {
            if !nodes.contains_key(transition.source()) {
                return Err(PlanError::UndefinedNode {
                    node: transition.source().clone(),
                });
            }
            if let FlowTarget::Node(target) = transition.target()
                && !nodes.contains_key(target)
            {
                return Err(PlanError::UndefinedNode {
                    node: target.clone(),
                });
            }
            let edges = outgoing.entry(transition.source().clone()).or_default();
            if edges.len() == MAX_OUTGOING_TRANSITIONS {
                return Err(PlanError::TooManyOutgoingTransitions {
                    node: transition.source().clone(),
                    max: MAX_OUTGOING_TRANSITIONS,
                });
            }
            edges.push(transition);
        }

        for id in nodes.keys() {
            let edges = outgoing
                .get(id)
                .filter(|edges| !edges.is_empty())
                .ok_or_else(|| PlanError::MissingTransition { node: id.clone() })?;
            check_unambiguous(id, edges)?;
        }

        let mut compiled: BTreeMap<NodeId, Vec<FlowTransition>> = outgoing;
        for edges in compiled.values_mut() {
            edges.sort_by(|left, right| {
                right
                    .pattern()
                    .specificity()
                    .cmp(&left.pattern().specificity())
                    .then_with(|| left.pattern().cmp(right.pattern()))
                    .then_with(|| left.target().sort_key().cmp(&right.target().sort_key()))
            });
        }

        check_reachable_and_acyclic(&entry, &nodes, &compiled)?;

        let manifest = flow_manifest(job_name, &entry, &nodes, &compiled);
        let definition = DefinitionIdentity::from_flow_manifest(job_name, revision, &manifest)
            .map_err(PlanError::Manifest)?;
        Ok(CompiledExecutionPlan {
            definition,
            entry,
            nodes,
            transitions: compiled,
        })
    }
}

fn check_unambiguous(node: &NodeId, edges: &[FlowTransition]) -> Result<(), PlanError> {
    for (index, left) in edges.iter().enumerate() {
        for right in &edges[index + 1..] {
            if left.pattern().specificity() == right.pattern().specificity()
                && left.pattern().intersects(right.pattern())
            {
                return Err(PlanError::AmbiguousTransition {
                    node: node.clone(),
                    first: left.pattern().clone(),
                    second: right.pattern().clone(),
                });
            }
        }
    }
    Ok(())
}

fn check_reachable_and_acyclic(
    entry: &NodeId,
    nodes: &BTreeMap<NodeId, FlowNode>,
    transitions: &BTreeMap<NodeId, Vec<FlowTransition>>,
) -> Result<(), PlanError> {
    let mut visited = BTreeSet::new();
    let mut on_path = BTreeSet::new();
    visit(entry, transitions, &mut visited, &mut on_path)?;
    for id in nodes.keys() {
        if !visited.contains(id) {
            return Err(PlanError::UnreachableNode { node: id.clone() });
        }
    }
    Ok(())
}

fn visit(
    node: &NodeId,
    transitions: &BTreeMap<NodeId, Vec<FlowTransition>>,
    visited: &mut BTreeSet<NodeId>,
    on_path: &mut BTreeSet<NodeId>,
) -> Result<(), PlanError> {
    if on_path.contains(node) {
        return Err(PlanError::CyclicGraph { node: node.clone() });
    }
    if !visited.insert(node.clone()) {
        return Ok(());
    }
    on_path.insert(node.clone());
    if let Some(edges) = transitions.get(node) {
        for edge in edges {
            if let FlowTarget::Node(target) = edge.target() {
                visit(target, transitions, visited, on_path)?;
            }
        }
    }
    on_path.remove(node);
    Ok(())
}

fn fault_manifest_value(policy: &FaultPolicy) -> Value {
    let backoff = policy.backoff();
    let rules: Vec<Value> = policy
        .classifier()
        .rules()
        .iter()
        .map(|rule| {
            json!({
                "category": rule.category().as_str(),
                "phase": rule.phase().as_str(),
                "retryable": rule.action().is_retryable(),
                "skip": rule
                    .action()
                    .skip_disposition()
                    .map_or(Value::Null, |skip| Value::String(skip.as_str().to_owned()))
            })
        })
        .collect();
    json!({
        "backoff": {
            "initial_ms": u64::try_from(backoff.initial().as_millis()).unwrap_or(u64::MAX),
            "kind": backoff.kind().as_str(),
            "maximum_ms": u64::try_from(backoff.maximum().as_millis()).unwrap_or(u64::MAX),
            "multiplier": backoff.multiplier()
        },
        "classifier": {
            "revision": policy.classifier().revision().as_str(),
            "rules": rules
        },
        "retry_limit": policy.retry_limit().get(),
        "retry_state_limit": policy.retry_state_limit().get(),
        "skip_limit": policy.skip_limit().get()
    })
}

fn flow_manifest(
    job_name: &JobName,
    entry: &NodeId,
    nodes: &BTreeMap<NodeId, FlowNode>,
    transitions: &BTreeMap<NodeId, Vec<FlowTransition>>,
) -> Value {
    let node_values: Vec<Value> = nodes.values().map(FlowNode::manifest_value).collect();
    let transition_values: Vec<Value> = transitions
        .values()
        .flat_map(|edges| edges.iter().map(FlowTransition::manifest_value))
        .collect();
    json!({
        "bounds": {
            "max_nodes": MAX_NODES,
            "max_outgoing_transitions": MAX_OUTGOING_TRANSITIONS,
            "max_pattern_bytes": MAX_PATTERN_BYTES,
            "max_transitions": MAX_TRANSITIONS
        },
        "entry": entry.as_str(),
        "format": crate::definition::MANIFEST_FORMAT_FLOW,
        "job": job_name.as_str(),
        "nodes": node_values,
        "transitions": transition_values
    })
}

/// A validated, immutable execution plan.
///
/// A plan owns the exact canonical manifest and fingerprint that identify the
/// definition across restart. A plan lowered from a one-step wrapper retains
/// that wrapper's original format-1 manifest bytes instead of emitting new
/// ones, so lowering never changes a persisted identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompiledExecutionPlan {
    definition: DefinitionIdentity,
    entry: NodeId,
    nodes: BTreeMap<NodeId, FlowNode>,
    transitions: BTreeMap<NodeId, Vec<FlowTransition>>,
}

impl CompiledExecutionPlan {
    /// Lowers one validated wrapper step into an in-memory compatibility plan.
    ///
    /// The plan reuses `definition` unchanged, so its manifest bytes, format,
    /// and fingerprint stay exactly what the wrapper persisted. The synthetic
    /// graph maps the framework's own exit codes onto terminals and adds no
    /// node an application could observe as a new durable decision.
    pub(crate) fn compatibility_one_step(
        definition: DefinitionIdentity,
        step: StepNode,
    ) -> Result<Self, PlanError> {
        let entry = step.id().clone();
        let mut nodes = BTreeMap::new();
        nodes.insert(entry.clone(), FlowNode::step(step));
        let mut edges = Vec::with_capacity(3);
        for (code, terminal) in [
            ("COMPLETED", TerminalKind::Complete),
            ("FAILED", TerminalKind::Fail),
            ("STOPPED", TerminalKind::Stop),
        ] {
            edges.push(FlowTransition::new(
                entry.clone(),
                ExitPattern::new(code)?,
                FlowTarget::Terminal(terminal),
            ));
        }
        check_unambiguous(&entry, &edges)?;
        let mut transitions = BTreeMap::new();
        transitions.insert(entry.clone(), edges);
        Ok(Self {
            definition,
            entry,
            nodes,
            transitions,
        })
    }

    /// Borrows the restart-relevant definition identity.
    #[must_use]
    pub const fn definition_identity(&self) -> &DefinitionIdentity {
        &self.definition
    }

    /// Returns the canonical manifest format this plan is identified by.
    #[must_use]
    pub const fn manifest_format(&self) -> u16 {
        self.definition.manifest_format()
    }

    /// Returns the SHA-256 definition fingerprint.
    #[must_use]
    pub const fn fingerprint(&self) -> &[u8; 32] {
        self.definition.manifest_digest()
    }

    /// Borrows the entry node identifier.
    #[must_use]
    pub const fn entry(&self) -> &NodeId {
        &self.entry
    }

    /// Returns the compiled node count.
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Returns the compiled transition count.
    #[must_use]
    pub fn transition_count(&self) -> usize {
        self.transitions.values().map(Vec::len).sum()
    }

    /// Borrows one compiled node.
    #[must_use]
    pub fn node(&self, id: &NodeId) -> Option<&FlowNode> {
        self.nodes.get(id)
    }

    /// Iterates over compiled nodes in stable logical-identifier order.
    ///
    /// The returned order is canonical and independent of builder declaration
    /// order. It is useful when binding executable components to an immutable
    /// plan before launch.
    #[must_use]
    pub fn nodes(&self) -> impl ExactSizeIterator<Item = (&NodeId, &FlowNode)> {
        self.nodes.iter()
    }

    /// Borrows one node's transitions in evaluation order.
    ///
    /// The first matching transition wins, so the slice is ordered from the
    /// most specific pattern to the least specific one.
    #[must_use]
    pub fn transitions(&self, id: &NodeId) -> &[FlowTransition] {
        self.transitions.get(id).map_or(&[], Vec::as_slice)
    }

    /// Selects the target one node's exit outcome reaches.
    ///
    /// # Errors
    ///
    /// Returns [`FlowSelectionError::UnknownNode`] when `id` is not compiled
    /// into this plan and [`FlowSelectionError::UnmappedExitOutcome`] when no
    /// declared pattern matches `code`. The plan never selects an arbitrary
    /// default.
    pub fn select_target(
        &self,
        id: &NodeId,
        code: &ExitCode,
    ) -> Result<&FlowTarget, FlowSelectionError> {
        let edges = self
            .transitions
            .get(id)
            .ok_or_else(|| FlowSelectionError::UnknownNode { node: id.clone() })?;
        edges
            .iter()
            .find(|edge| edge.pattern().matches(code))
            .map(FlowTransition::target)
            .ok_or_else(|| FlowSelectionError::UnmappedExitOutcome {
                node: id.clone(),
                code: code.clone(),
            })
    }
}

/// A flow graph that cannot be compiled into an executable plan.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PlanError {
    /// No entry node was declared.
    MissingEntryNode,
    /// Two nodes declared the same logical identifier.
    DuplicateNodeId {
        /// Repeated identifier.
        node: NodeId,
    },
    /// A transition referenced a node the graph does not declare.
    UndefinedNode {
        /// Missing identifier.
        node: NodeId,
    },
    /// A node declared no outgoing transition.
    MissingTransition {
        /// Identifier of the node without a transition.
        node: NodeId,
    },
    /// Two equally specific patterns can match one exit outcome.
    AmbiguousTransition {
        /// Identifier of the node with the ambiguity.
        node: NodeId,
        /// First conflicting pattern.
        first: ExitPattern,
        /// Second conflicting pattern.
        second: ExitPattern,
    },
    /// A node cannot be reached from the entry node.
    UnreachableNode {
        /// Unreachable identifier.
        node: NodeId,
    },
    /// The graph contains a cycle.
    CyclicGraph {
        /// Identifier revisited on one path.
        node: NodeId,
    },
    /// The graph declared more nodes than M3 accepts.
    TooManyNodes {
        /// Maximum accepted node count.
        max: usize,
    },
    /// The graph declared more transitions than M3 accepts.
    TooManyTransitions {
        /// Maximum accepted transition count.
        max: usize,
    },
    /// One node declared more outgoing transitions than M3 accepts.
    TooManyOutgoingTransitions {
        /// Identifier of the node with too many transitions.
        node: NodeId,
        /// Maximum accepted outgoing transition count.
        max: usize,
    },
    /// An exit pattern violated its bounded format.
    InvalidPattern {
        /// Maximum accepted pattern length in UTF-8 bytes.
        max_bytes: usize,
    },
    /// A start limit of zero can never start its step.
    ZeroStartLimit,
    /// A decision input-contract version of zero is not a version.
    ZeroDecisionInputVersion,
    /// A logical identifier or revision token was invalid.
    Token(DefinitionError),
    /// The canonical manifest could not be encoded within its bound.
    Manifest(DefinitionError),
}

impl fmt::Display for PlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingEntryNode => formatter.write_str("flow graph has no entry node"),
            Self::DuplicateNodeId { node } => {
                write!(
                    formatter,
                    "node {} is declared more than once",
                    node.as_str()
                )
            }
            Self::UndefinedNode { node } => {
                write!(formatter, "node {} is not declared", node.as_str())
            }
            Self::MissingTransition { node } => {
                write!(
                    formatter,
                    "node {} has no outgoing transition",
                    node.as_str()
                )
            }
            Self::AmbiguousTransition {
                node,
                first,
                second,
            } => write!(
                formatter,
                "node {} patterns {first} and {second} are equally specific and overlap",
                node.as_str()
            ),
            Self::UnreachableNode { node } => {
                write!(
                    formatter,
                    "node {} is unreachable from the entry node",
                    node.as_str()
                )
            }
            Self::CyclicGraph { node } => {
                write!(formatter, "node {} closes a cycle", node.as_str())
            }
            Self::TooManyNodes { max } => write!(formatter, "flow graph exceeds {max} nodes"),
            Self::TooManyTransitions { max } => {
                write!(formatter, "flow graph exceeds {max} transitions")
            }
            Self::TooManyOutgoingTransitions { node, max } => write!(
                formatter,
                "node {} exceeds {max} outgoing transitions",
                node.as_str()
            ),
            Self::InvalidPattern { max_bytes } => write!(
                formatter,
                "exit pattern must be 1 to {max_bytes} bytes without control characters"
            ),
            Self::ZeroStartLimit => formatter.write_str("start limit must be nonzero"),
            Self::ZeroDecisionInputVersion => {
                formatter.write_str("decision input version must be nonzero")
            }
            Self::Token(error) => write!(formatter, "flow graph token is invalid: {error}"),
            Self::Manifest(error) => {
                write!(formatter, "flow manifest could not be encoded: {error}")
            }
        }
    }
}

impl Error for PlanError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Token(error) | Self::Manifest(error) => Some(error),
            _ => None,
        }
    }
}

impl From<DefinitionError> for PlanError {
    fn from(error: DefinitionError) -> Self {
        Self::Token(error)
    }
}

/// A compiled plan that cannot route one observed exit outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum FlowSelectionError {
    /// The requested node is not part of this plan.
    UnknownNode {
        /// Requested identifier.
        node: NodeId,
    },
    /// No declared pattern matches the produced exit outcome.
    UnmappedExitOutcome {
        /// Node whose outcome could not be routed.
        node: NodeId,
        /// Produced exit code.
        code: ExitCode,
    },
}

impl fmt::Display for FlowSelectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownNode { node } => {
                write!(
                    formatter,
                    "node {} is not part of the compiled plan",
                    node.as_str()
                )
            }
            Self::UnmappedExitOutcome { node, code } => write!(
                formatter,
                "node {} declares no transition for exit outcome {code}",
                node.as_str()
            ),
        }
    }
}

impl Error for FlowSelectionError {}
