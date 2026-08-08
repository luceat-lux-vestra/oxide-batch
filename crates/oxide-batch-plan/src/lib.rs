//! Internal implementation crate for `OxideBatch`.
//!
//! **This crate is implementation detail. Use
//! [`oxide-batch`](https://crates.io/crates/oxide-batch) instead.**
//!
//! It exists on crates.io only because the published `oxide-batch` facade
//! depends on it. Its API carries no stability promise: items may be added,
//! changed, or removed in any release, without a deprecation period. It has no
//! supported-configuration matrix, no compatibility ledger row, and no
//! independent release cadence.
//!
//! Everything here that `OxideBatch` supports is re-exported from `oxide-batch`
//! under a stable path.
//!
//! The crate holds immutable flow graphs and the compiled execution plans they
//! lower into. An application declares a [`FlowGraph`] of step and decision
//! nodes joined by exit-pattern transitions, then compiles it into an immutable
//! [`CompiledExecutionPlan`]. Compilation normalizes the graph, rejects every
//! structural error the accepted basic-flow contract names, and produces the
//! canonical manifest whose SHA-256 digest is the definition fingerprint.
//!
//! The M3 graph remains acyclic. M4 adds only the accepted bounded split and
//! local-partition forms; nested splits, decisions inside branches, dynamic
//! partitioning, and remote execution remain outside this crate's contract.
//! Existing one-step `TaskletJob` and `ChunkJob` definitions lower into a
//! compatibility plan that retains their original format-1 manifest bytes and
//! fingerprint.
//!
//! The crate depends on no async runtime, database driver, command-line
//! framework, telemetry SDK, broker client, or web framework, and on no
//! `OxideBatch` crate other than `oxide-batch-core`. The flow engine that
//! executes a compiled plan, the metadata ports that persist its decisions,
//! and the runtime live above this crate.
//!
//! # Items marked `#[doc(hidden)]`
//!
//! Some items exist as `#[doc(hidden)] pub` only because the facade's own code
//! was split from these types by the extraction boundary: private access that
//! one crate resolved by module privacy now crosses a crate boundary. They are
//! not part of any surface, supported or otherwise, and the facade never
//! re-exports one under its own name. The staged crate-extraction contract
//! records each one.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::num::NonZeroU32;

use serde_json::{Value, json};

use oxide_batch_core::{
    ChunkComponentRevisions, ChunkSize, ComponentRevision, DefinitionError, DefinitionIdentity,
    DefinitionRevision, DefinitionTokenKind, ExitCode, FaultPolicy, FlowTarget, InFlightPolicy,
    JobName, MAX_NODES, MAX_PARTITIONS, MAX_TRANSITIONS, NodeId, StartControls, StepName,
    TerminalKind, definition_token, validate_token,
};

/// The maximum number of transitions leaving one node.
pub const MAX_OUTGOING_TRANSITIONS: usize = 64;
/// The maximum length of one exit pattern in UTF-8 bytes.
pub const MAX_PATTERN_BYTES: usize = 64;
/// The maximum number of branches in one M4 split.
pub const MAX_SPLIT_BRANCHES: usize = 8;
/// The maximum number of linear steps in one split branch.
pub const MAX_BRANCH_STEPS: usize = 8;
/// The maximum number of concurrent local partition workers.
pub const MAX_PARTITION_WORKERS: u8 = 64;

/// The sibling behavior selected after one local child fails.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum LocalFailurePolicy {
    /// Request cooperative cancellation of siblings, then join all children.
    #[default]
    CancelSiblings,
    /// Allow siblings to reach their next boundary, then join all children.
    DrainSiblings,
}

impl LocalFailurePolicy {
    const fn as_str(self) -> &'static str {
        match self {
            Self::CancelSiblings => "cancel_siblings",
            Self::DrainSiblings => "drain_siblings",
        }
    }
}

/// The finite concurrency and connection budget for one M4 split.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SplitBudget {
    max_parallel_branches: u8,
    repository_pool_size: u32,
}

impl SplitBudget {
    /// Constructs validated branch-concurrency and connection bounds.
    ///
    /// # Errors
    ///
    /// Rejects zero/over-limit concurrency or a pool that cannot supply every
    /// active branch plus the owning parent connection.
    pub fn new(max_parallel_branches: u8, repository_pool_size: u32) -> Result<Self, PlanError> {
        if max_parallel_branches == 0 || usize::from(max_parallel_branches) > MAX_SPLIT_BRANCHES {
            return Err(PlanError::InvalidParallelBranchBudget {
                max: MAX_SPLIT_BRANCHES,
            });
        }
        let required = u32::from(max_parallel_branches).saturating_add(1);
        if repository_pool_size < required {
            return Err(PlanError::InsufficientPoolCapacity {
                required,
                configured: repository_pool_size,
            });
        }
        Ok(Self {
            max_parallel_branches,
            repository_pool_size,
        })
    }

    /// Returns the maximum concurrent split branches.
    #[must_use]
    pub const fn max_parallel_branches(self) -> u8 {
        self.max_parallel_branches
    }

    /// Returns the validated repository pool size.
    #[must_use]
    pub const fn repository_pool_size(self) -> u32 {
        self.repository_pool_size
    }
}

impl Default for SplitBudget {
    fn default() -> Self {
        Self {
            max_parallel_branches: 1,
            repository_pool_size: 2,
        }
    }
}

/// The finite worker and connection budget for one M4 partition manager.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PartitionBudget {
    max_partition_workers: u8,
    repository_pool_size: u32,
}

impl PartitionBudget {
    /// Constructs validated worker-concurrency and connection bounds.
    ///
    /// # Errors
    ///
    /// Rejects zero/over-limit concurrency or a pool that cannot supply every
    /// active worker plus the owning parent connection.
    pub fn new(max_partition_workers: u8, repository_pool_size: u32) -> Result<Self, PlanError> {
        if !(1..=MAX_PARTITION_WORKERS).contains(&max_partition_workers) {
            return Err(PlanError::InvalidPartitionWorkerBudget {
                max: MAX_PARTITION_WORKERS,
            });
        }
        let required = u32::from(max_partition_workers).saturating_add(1);
        if repository_pool_size < required {
            return Err(PlanError::InsufficientPoolCapacity {
                required,
                configured: repository_pool_size,
            });
        }
        Ok(Self {
            max_partition_workers,
            repository_pool_size,
        })
    }

    /// Returns the maximum concurrent partition workers.
    #[must_use]
    pub const fn max_partition_workers(self) -> u8 {
        self.max_partition_workers
    }

    /// Returns the validated repository pool size.
    #[must_use]
    pub const fn repository_pool_size(self) -> u32 {
        self.repository_pool_size
    }
}

impl Default for PartitionBudget {
    fn default() -> Self {
        Self {
            max_partition_workers: 4,
            repository_pool_size: 5,
        }
    }
}

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

/// A bounded exit-outcome pattern used to select one transition.
///
/// A pattern contains literal characters plus `*` for zero or more characters
/// and `?` for exactly one character. It matches the bounded
/// [`ExitCode`], never [`BatchStatus`](oxide_batch_core::BatchStatus).
///
/// The worked example lives in the `oxide-batch` crate documentation, so that
/// it keeps demonstrating the supported import path.
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
                let mut chunk = chunk_declaration_manifest(revisions);
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
            "start": start_controls_manifest(self.start),
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

/// One declared linear branch of an M4 split.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SplitBranch {
    steps: Vec<StepNode>,
}

impl SplitBranch {
    /// Declares a branch from its ordered tasklet or chunk steps.
    ///
    /// Cardinality and identifier uniqueness are checked by
    /// [`FlowGraph::compile`], so builders can assemble a complete diagnostic
    /// instead of panicking while under construction.
    #[must_use]
    pub fn new(steps: Vec<StepNode>) -> Self {
        Self { steps }
    }

    /// Borrows the branch steps in declared execution order.
    #[must_use]
    pub fn steps(&self) -> &[StepNode] {
        &self.steps
    }

    /// Borrows the branch identity, which is its first logical step ID.
    #[must_use]
    pub fn id(&self) -> Option<&NodeId> {
        self.steps.first().map(StepNode::id)
    }

    fn manifest_value(&self) -> Value {
        Value::Array(self.steps.iter().map(StepNode::manifest_value).collect())
    }
}

/// A bounded M4 split whose branches converge at exactly one join node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SplitNode {
    id: NodeId,
    branches: Vec<SplitBranch>,
    join: NodeId,
    budget: SplitBudget,
    failure_policy: LocalFailurePolicy,
}

impl SplitNode {
    /// Declares a split, its ordered branches, and its unique join.
    #[must_use]
    pub fn new(id: NodeId, branches: Vec<SplitBranch>, join: NodeId, budget: SplitBudget) -> Self {
        Self {
            id,
            branches,
            join,
            budget,
            failure_policy: LocalFailurePolicy::default(),
        }
    }

    /// Selects sibling failure behavior.
    #[must_use]
    pub const fn with_failure_policy(mut self, failure_policy: LocalFailurePolicy) -> Self {
        self.failure_policy = failure_policy;
        self
    }

    /// Borrows the stable split identifier.
    #[must_use]
    pub const fn id(&self) -> &NodeId {
        &self.id
    }

    /// Borrows branches in deterministic aggregation order.
    #[must_use]
    pub fn branches(&self) -> &[SplitBranch] {
        &self.branches
    }

    /// Borrows the split's unique join identifier.
    #[must_use]
    pub const fn join(&self) -> &NodeId {
        &self.join
    }

    /// Returns the finite local resource budget.
    #[must_use]
    pub const fn budget(&self) -> SplitBudget {
        self.budget
    }

    /// Returns sibling failure behavior.
    #[must_use]
    pub const fn failure_policy(&self) -> LocalFailurePolicy {
        self.failure_policy
    }

    /// Projects the restart-relevant split declaration.
    ///
    /// The budget is a throughput bound rather than a durable-meaning value, so
    /// [ADR-0009](https://github.com/luceat-lux-vestra/oxide-batch/blob/main/docs/architecture/decisions/0009-definition-fingerprint-input-set.md)
    /// excludes it. Branch membership and order select assignment and remain.
    fn manifest_value(&self) -> Value {
        json!({
            "branches": self.branches.iter().map(SplitBranch::manifest_value).collect::<Vec<_>>(),
            "failure_policy": self.failure_policy.as_str(),
            "id": self.id.as_str(),
            "join": self.join.as_str(),
            "kind": "split"
        })
    }
}

/// The structural join owned by one M4 split.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JoinNode {
    id: NodeId,
}

impl JoinNode {
    /// Declares a structural join.
    #[must_use]
    pub const fn new(id: NodeId) -> Self {
        Self { id }
    }

    /// Borrows the stable join identifier.
    #[must_use]
    pub const fn id(&self) -> &NodeId {
        &self.id
    }

    fn manifest_value(&self) -> Value {
        json!({
            "id": self.id.as_str(),
            "kind": "join"
        })
    }
}

/// A finite durable partition count for one M4 partitioned step.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PartitionCount(u16);

impl PartitionCount {
    /// Constructs a count in the accepted `1..=1024` range.
    ///
    /// # Errors
    ///
    /// Returns [`PlanError::InvalidPartitionCount`] outside that range.
    pub fn new(value: u16) -> Result<Self, PlanError> {
        if value == 0 || value > MAX_PARTITIONS {
            return Err(PlanError::InvalidPartitionCount {
                max: MAX_PARTITIONS,
            });
        }
        Ok(Self(value))
    }

    /// Returns the declared partition count.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0
    }
}

/// A bounded local partition manager and its ordinary worker-step definition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionedStepNode {
    id: NodeId,
    step_name: StepName,
    worker: StepNode,
    partitioner: ComponentRevision,
    aggregation: ComponentRevision,
    partitions: PartitionCount,
    budget: PartitionBudget,
    failure_policy: LocalFailurePolicy,
    start: StartControls,
}

impl PartitionedStepNode {
    /// Declares the complete restart-relevant local partition shape.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        id: NodeId,
        step_name: StepName,
        worker: StepNode,
        partitioner: ComponentRevision,
        aggregation: ComponentRevision,
        partitions: PartitionCount,
        budget: PartitionBudget,
    ) -> Self {
        Self {
            id,
            step_name,
            worker,
            partitioner,
            aggregation,
            partitions,
            budget,
            failure_policy: LocalFailurePolicy::default(),
            start: StartControls::default(),
        }
    }

    /// Selects sibling failure behavior.
    #[must_use]
    pub const fn with_failure_policy(mut self, failure_policy: LocalFailurePolicy) -> Self {
        self.failure_policy = failure_policy;
        self
    }

    /// Declares the partition manager's start controls.
    #[must_use]
    pub const fn with_start_controls(mut self, start: StartControls) -> Self {
        self.start = start;
        self
    }

    /// Borrows the stable manager node identifier.
    #[must_use]
    pub const fn id(&self) -> &NodeId {
        &self.id
    }

    /// Borrows the manager's durable step name.
    #[must_use]
    pub const fn step_name(&self) -> &StepName {
        &self.step_name
    }

    /// Borrows the ordinary tasklet or chunk worker declaration.
    #[must_use]
    pub const fn worker(&self) -> &StepNode {
        &self.worker
    }

    /// Borrows the deterministic partitioner revision.
    #[must_use]
    pub const fn partitioner(&self) -> &ComponentRevision {
        &self.partitioner
    }

    /// Borrows the deterministic aggregation revision.
    #[must_use]
    pub const fn aggregation(&self) -> &ComponentRevision {
        &self.aggregation
    }

    /// Returns the finite partition count.
    #[must_use]
    pub const fn partition_count(&self) -> PartitionCount {
        self.partitions
    }

    /// Returns the finite local resource budget.
    #[must_use]
    pub const fn budget(&self) -> PartitionBudget {
        self.budget
    }

    /// Returns sibling failure behavior.
    #[must_use]
    pub const fn failure_policy(&self) -> LocalFailurePolicy {
        self.failure_policy
    }

    /// Returns the partition manager's start controls.
    #[must_use]
    pub const fn start_controls(&self) -> StartControls {
        self.start
    }

    /// Projects the restart-relevant partition declaration.
    ///
    /// The partition count selects durable assignment and the partitioner and
    /// aggregation revisions decide how that assignment and its results are
    /// interpreted, so all three remain. The worker and connection budget is a
    /// throughput bound that [ADR-0009](https://github.com/luceat-lux-vestra/oxide-batch/blob/main/docs/architecture/decisions/0009-definition-fingerprint-input-set.md)
    /// excludes.
    fn manifest_value(&self) -> Value {
        json!({
            "aggregation": self.aggregation.as_str(),
            "failure_policy": self.failure_policy.as_str(),
            "id": self.id.as_str(),
            "kind": "partitioned_step",
            "partition_count": self.partitions.get(),
            "partitioner": self.partitioner.as_str(),
            "start": start_controls_manifest(self.start),
            "step_name": self.step_name.as_str(),
            "worker": self.worker.manifest_value()
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
    /// A bounded set of linear branches and its structural join.
    Split(Box<SplitNode>),
    /// A structural join owned by exactly one split.
    Join(JoinNode),
    /// A bounded durable local-partition manager.
    PartitionedStep(Box<PartitionedStepNode>),
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

    /// Declares a bounded split node.
    #[must_use]
    pub fn split(node: SplitNode) -> Self {
        Self::Split(Box::new(node))
    }

    /// Declares a structural join node.
    #[must_use]
    pub const fn join(node: JoinNode) -> Self {
        Self::Join(node)
    }

    /// Declares a bounded local partitioned-step node.
    #[must_use]
    pub fn partitioned_step(node: PartitionedStepNode) -> Self {
        Self::PartitionedStep(Box::new(node))
    }

    /// Borrows the node's stable identifier.
    #[must_use]
    pub const fn id(&self) -> &NodeId {
        match self {
            Self::Step(node) => node.id(),
            Self::Decision(node) => node.id(),
            Self::Split(node) => node.id(),
            Self::Join(node) => node.id(),
            Self::PartitionedStep(node) => node.id(),
        }
    }

    fn manifest_value(&self) -> Value {
        match self {
            Self::Step(node) => node.manifest_value(),
            Self::Decision(node) => node.manifest_value(),
            Self::Split(node) => node.manifest_value(),
            Self::Join(node) => node.manifest_value(),
            Self::PartitionedStep(node) => node.manifest_value(),
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
            "target": flow_target_manifest(&self.target)
        })
    }
}

/// An immutable declaration of the M3 flow subset.
///
/// The worked example lives in the `oxide-batch` crate documentation, so that
/// it keeps demonstrating the supported import path.
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
        let local_scale = check_local_scale_subset(&entry, &nodes)?;

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
            if let FlowTarget::Node(target) = transition.target()
                && matches!(nodes.get(target), Some(FlowNode::Join(_)))
            {
                return Err(PlanError::JoinHasExternalEntry {
                    join: target.clone(),
                });
            }
            if matches!(nodes.get(transition.source()), Some(FlowNode::Split(_))) {
                return Err(PlanError::SplitHasExplicitTransition {
                    split: transition.source().clone(),
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

        for (id, node) in &nodes {
            if matches!(node, FlowNode::Split(_)) {
                continue;
            }
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

        let manifest = flow_manifest(job_name, &entry, &nodes, &compiled, local_scale);
        let canonical = serde_json::to_vec(&manifest)
            .map_err(|_| PlanError::Manifest(DefinitionError::ManifestEncoding))?;
        let definition = DefinitionIdentity::from_flow_manifest(job_name, revision, &canonical)
            .map_err(PlanError::Manifest)?;
        Ok(CompiledExecutionPlan {
            definition,
            entry,
            nodes,
            transitions: compiled,
        })
    }
}

fn check_local_scale_subset(
    entry: &NodeId,
    nodes: &BTreeMap<NodeId, FlowNode>,
) -> Result<bool, PlanError> {
    let mut embedded_ids = BTreeSet::new();
    let mut join_owners: BTreeMap<NodeId, NodeId> = BTreeMap::new();
    let mut local_scale = false;
    for (id, node) in nodes {
        match node {
            FlowNode::Split(split) => {
                local_scale = true;
                if id == entry {
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
                if !matches!(nodes.get(split.join()), Some(FlowNode::Join(_))) {
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
                for branch in split.branches() {
                    if !(1..=MAX_BRANCH_STEPS).contains(&branch.steps().len()) {
                        return Err(PlanError::InvalidBranchLength {
                            split: id.clone(),
                            max: MAX_BRANCH_STEPS,
                        });
                    }
                    for step in branch.steps() {
                        if nodes.contains_key(step.id()) || !embedded_ids.insert(step.id().clone())
                        {
                            return Err(PlanError::DuplicateNodeId {
                                node: step.id().clone(),
                            });
                        }
                    }
                }
            }
            FlowNode::Join(_) => {
                local_scale = true;
            }
            FlowNode::PartitionedStep(partitioned) => {
                local_scale = true;
                let worker = partitioned.worker().id();
                if nodes.contains_key(worker) || !embedded_ids.insert(worker.clone()) {
                    return Err(PlanError::DuplicateNodeId {
                        node: worker.clone(),
                    });
                }
            }
            FlowNode::Step(_) | FlowNode::Decision(_) => {}
        }
    }
    if nodes.len().saturating_add(embedded_ids.len()) > MAX_NODES {
        return Err(PlanError::TooManyNodes { max: MAX_NODES });
    }
    for (id, node) in nodes {
        if matches!(node, FlowNode::Join(_)) && !join_owners.contains_key(id) {
            return Err(PlanError::OrphanJoin { join: id.clone() });
        }
    }
    Ok(local_scale)
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
    visit(entry, nodes, transitions, &mut visited, &mut on_path)?;
    for id in nodes.keys() {
        if !visited.contains(id) {
            return Err(PlanError::UnreachableNode { node: id.clone() });
        }
    }
    Ok(())
}

fn visit(
    node: &NodeId,
    nodes: &BTreeMap<NodeId, FlowNode>,
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
    if let Some(FlowNode::Split(split)) = nodes.get(node) {
        visit(split.join(), nodes, transitions, visited, on_path)?;
    }
    if let Some(edges) = transitions.get(node) {
        for edge in edges {
            if let FlowTarget::Node(target) = edge.target() {
                visit(target, nodes, transitions, visited, on_path)?;
            }
        }
    }
    on_path.remove(node);
    Ok(())
}

/// Projects restart-relevant start controls into their manifest member.
///
/// The projection lives here rather than on the value because the canonical
/// manifest is this crate's contract. Keeping it here is also what keeps the
/// serializer out of the core's public signatures, and out of the facade that
/// re-exports them.
fn start_controls_manifest(controls: StartControls) -> Value {
    json!({
        "allow_start_if_complete": controls.allow_start_if_complete(),
        "start_limit": controls.start_limit().get()
    })
}

/// Projects one transition target into its manifest member.
fn flow_target_manifest(target: &FlowTarget) -> Value {
    match target {
        FlowTarget::Node(id) => json!({ "node": id.as_str() }),
        FlowTarget::Terminal(kind) => json!({ "terminal": kind.as_str() }),
    }
}

/// Projects the restart-relevant chunk declaration into manifest members.
///
/// `in_flight_policy` is present only when it is the non-default rollback
/// policy, because format 1 recorded nothing for the default and the two
/// formats must agree on what a chunk declaration means.
fn chunk_declaration_manifest(revisions: &ChunkComponentRevisions) -> Value {
    let mut value = json!({
        "checkpoint": {
            "schema": revisions.checkpoint_schema().as_str(),
            "version": revisions.checkpoint_schema_version().get()
        },
        "components": {
            "checkpoint": revisions.checkpoint().as_str(),
            "processor": revisions.processor().as_str(),
            "reader": revisions.reader().as_str(),
            "writer": revisions.writer().as_str()
        },
        "context": {
            "schema": revisions.context_schema().as_str(),
            "version": revisions.context_schema_version().get()
        },
        "delivery_mode": revisions.delivery_mode().manifest_name()
    });
    if revisions.in_flight_policy() == InFlightPolicy::RollbackChunk
        && let Some(object) = value.as_object_mut()
    {
        object.insert(
            "in_flight_policy".to_owned(),
            Value::String("rollback_chunk".to_owned()),
        );
    }
    value
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

/// Projects the compiled graph into its canonical restart-relevant manifest.
///
/// The projection carries exactly the values that select or reinterpret durable
/// state. Framework capacity bounds are deliberately absent: they belong to the
/// runtime that reads a manifest, not to the definition it identifies, so
/// raising one in a later release must not change a fingerprint. `MAX_NODES`
/// and `MAX_TRANSITIONS` are enforced against the graph a manifest declares by
/// [`DefinitionManifest::read`](oxide_batch_core::DefinitionManifest::read).
fn flow_manifest(
    job_name: &JobName,
    entry: &NodeId,
    nodes: &BTreeMap<NodeId, FlowNode>,
    transitions: &BTreeMap<NodeId, Vec<FlowTransition>>,
    local_scale: bool,
) -> Value {
    let node_values: Vec<Value> = nodes.values().map(FlowNode::manifest_value).collect();
    let transition_values: Vec<Value> = transitions
        .values()
        .flat_map(|edges| edges.iter().map(FlowTransition::manifest_value))
        .collect();
    json!({
        "entry": entry.as_str(),
        "format": if local_scale {
            oxide_batch_core::MANIFEST_FORMAT_LOCAL_SCALE
        } else {
            oxide_batch_core::MANIFEST_FORMAT_FLOW
        },
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
    #[doc(hidden)]
    pub fn compatibility_one_step(
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
    /// A decision input-contract version of zero is not a version.
    ZeroDecisionInputVersion,
    /// A split declared fewer than two or more than eight branches.
    InvalidSplitBranchCount {
        /// Split whose branch count was invalid.
        split: NodeId,
        /// Minimum accepted branch count.
        min: usize,
        /// Maximum accepted branch count.
        max: usize,
    },
    /// A split branch was empty or longer than the accepted bound.
    InvalidBranchLength {
        /// Owning split.
        split: NodeId,
        /// Maximum accepted branch length.
        max: usize,
    },
    /// A split was declared as the graph entry.
    SplitIsEntry {
        /// Rejected split.
        split: NodeId,
    },
    /// A split did not reference a declared structural join.
    InvalidSplitJoin {
        /// Owning split.
        split: NodeId,
        /// Missing or wrongly typed join.
        join: NodeId,
    },
    /// More than one split tried to own the same join.
    JoinHasMultipleOwners {
        /// Multiply owned join.
        join: NodeId,
        /// First owning split.
        first: NodeId,
        /// Second owning split.
        second: NodeId,
    },
    /// A join was not owned by any split.
    OrphanJoin {
        /// Unowned join.
        join: NodeId,
    },
    /// A normal transition attempted to enter a structural join.
    JoinHasExternalEntry {
        /// Join with an external incoming edge.
        join: NodeId,
    },
    /// A split tried to bypass its implicit join edge.
    SplitHasExplicitTransition {
        /// Split with the explicit edge.
        split: NodeId,
    },
    /// The branch concurrency budget exceeded the declared branch count.
    ParallelBudgetExceedsBranches {
        /// Split with the contradictory budget.
        split: NodeId,
        /// Declared branch count.
        branches: usize,
    },
    /// A branch concurrency budget was zero or above the M4 ceiling.
    InvalidParallelBranchBudget {
        /// Maximum accepted branch concurrency.
        max: usize,
    },
    /// A partition-worker budget was zero or above the M4 ceiling.
    InvalidPartitionWorkerBudget {
        /// Maximum accepted worker concurrency.
        max: u8,
    },
    /// The declared pool cannot supply active children plus their parent.
    InsufficientPoolCapacity {
        /// Minimum required connection count.
        required: u32,
        /// Declared connection count.
        configured: u32,
    },
    /// A durable partition count was zero or above the M4 ceiling.
    InvalidPartitionCount {
        /// Maximum accepted partition count.
        max: u16,
    },
    /// A logical identifier or revision token was invalid.
    Token(DefinitionError),
    /// The canonical manifest could not be encoded within its bound.
    Manifest(DefinitionError),
}

impl fmt::Display for PlanError {
    #[allow(
        clippy::too_many_lines,
        reason = "each typed plan rejection retains one stable redacted diagnostic"
    )]
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
            Self::ZeroDecisionInputVersion => {
                formatter.write_str("decision input version must be nonzero")
            }
            Self::InvalidSplitBranchCount { split, min, max } => write!(
                formatter,
                "split {} must declare {min} to {max} branches",
                split.as_str()
            ),
            Self::InvalidBranchLength { split, max } => write!(
                formatter,
                "split {} branches must declare 1 to {max} steps",
                split.as_str()
            ),
            Self::SplitIsEntry { split } => {
                write!(
                    formatter,
                    "split {} cannot be the entry node",
                    split.as_str()
                )
            }
            Self::InvalidSplitJoin { split, join } => write!(
                formatter,
                "split {} does not own declared join {}",
                split.as_str(),
                join.as_str()
            ),
            Self::JoinHasMultipleOwners {
                join,
                first,
                second,
            } => write!(
                formatter,
                "join {} is owned by both splits {} and {}",
                join.as_str(),
                first.as_str(),
                second.as_str()
            ),
            Self::OrphanJoin { join } => {
                write!(formatter, "join {} has no owning split", join.as_str())
            }
            Self::JoinHasExternalEntry { join } => write!(
                formatter,
                "join {} can be entered only by its owning split",
                join.as_str()
            ),
            Self::SplitHasExplicitTransition { split } => write!(
                formatter,
                "split {} reaches only its declared join",
                split.as_str()
            ),
            Self::ParallelBudgetExceedsBranches { split, branches } => write!(
                formatter,
                "split {} parallel budget exceeds its {branches} branches",
                split.as_str()
            ),
            Self::InvalidParallelBranchBudget { max } => {
                write!(formatter, "parallel branch budget must be 1 to {max}")
            }
            Self::InvalidPartitionWorkerBudget { max } => {
                write!(formatter, "partition worker budget must be 1 to {max}")
            }
            Self::InsufficientPoolCapacity {
                required,
                configured,
            } => write!(
                formatter,
                "repository pool size {configured} cannot supply required capacity {required}"
            ),
            Self::InvalidPartitionCount { max } => {
                write!(formatter, "partition count must be 1 to {max}")
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
