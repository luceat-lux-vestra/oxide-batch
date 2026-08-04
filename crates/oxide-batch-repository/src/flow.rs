//! Durable flow-decision records exchanged with a metadata repository.
//!
//! A flow decision is repository-authoritative and append-only: the runtime
//! validates and proposes one transition, and the adapter allocates its
//! identity and commits it. The records below carry no engine, runtime, or plan
//! type, so a metadata adapter can persist and replay them without depending on
//! the flow engine that produced them.

use std::fmt;
use std::num::NonZeroU64;
use std::time::SystemTime;

use oxide_batch_core::{
    DomainError, ExecutionContext, ExitCode, FlowTarget, IdentifierKind, JobExecutionId, NodeId,
    StepExecution, StepExecutionId,
};

/// Opaque durable identifier of one selected transition.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FlowDecisionId(NonZeroU64);

impl FlowDecisionId {
    /// Constructs a positive flow-decision identifier.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::ZeroIdentifier`] for zero.
    pub fn new(value: u64) -> Result<Self, DomainError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(DomainError::ZeroIdentifier {
                kind: IdentifierKind::FlowDecision,
            })
    }

    /// Returns the numeric identifier.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

impl fmt::Display for FlowDecisionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.get().fmt(formatter)
    }
}

/// Positive, execution-local ordering of selected transitions.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FlowDecisionSequence(NonZeroU64);

impl FlowDecisionSequence {
    /// Constructs a positive sequence.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::ZeroIdentifier`] for zero.
    ///
    /// [`DomainError::ZeroIdentifier`]: DomainError::ZeroIdentifier
    pub fn new(value: u64) -> Result<Self, DomainError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(DomainError::ZeroIdentifier {
                kind: IdentifierKind::FlowDecisionSequence,
            })
    }

    /// Returns the numeric sequence.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Why one transition was selected.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum FlowTransitionKind {
    /// A newly executed step produced the observed outcome.
    StepExit,
    /// A deterministic decider produced the observed outcome.
    Decider,
    /// Restart reused a completed step without invoking it.
    CompletedStepReuse,
    /// A bounded local split joined durable branch results in declared order.
    SplitAggregate,
}

impl FlowTransitionKind {
    /// Returns the stable durable code of this transition kind.
    #[doc(hidden)]
    #[must_use]
    pub const fn durable_code(self) -> &'static str {
        match self {
            Self::StepExit => "STEP_EXIT",
            Self::Decider => "DECIDER",
            Self::CompletedStepReuse => "COMPLETED_STEP_REUSE",
            Self::SplitAggregate => "SPLIT_AGGREGATE",
        }
    }

    /// Reads one stable durable transition-kind code.
    #[doc(hidden)]
    #[must_use]
    pub fn from_durable_code(value: &str) -> Option<Self> {
        match value {
            "STEP_EXIT" => Some(Self::StepExit),
            "DECIDER" => Some(Self::Decider),
            "COMPLETED_STEP_REUSE" => Some(Self::CompletedStepReuse),
            "SPLIT_AGGREGATE" => Some(Self::SplitAggregate),
            _ => None,
        }
    }
}

/// One append-only, repository-authoritative selected transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FlowDecision {
    id: FlowDecisionId,
    job_execution_id: JobExecutionId,
    sequence: FlowDecisionSequence,
    source_node_id: NodeId,
    source_step_execution_id: Option<StepExecutionId>,
    kind: FlowTransitionKind,
    observed_outcome: ExitCode,
    target: FlowTarget,
    plan_fingerprint: [u8; 32],
    input_digest: [u8; 32],
    reused_decision_id: Option<FlowDecisionId>,
    decided_at: SystemTime,
}

impl FlowDecision {
    /// Reconstructs one durable flow decision an adapter allocated.
    #[allow(clippy::too_many_arguments)]
    #[doc(hidden)]
    #[must_use]
    pub const fn new(
        id: FlowDecisionId,
        job_execution_id: JobExecutionId,
        sequence: FlowDecisionSequence,
        source_node_id: NodeId,
        source_step_execution_id: Option<StepExecutionId>,
        kind: FlowTransitionKind,
        observed_outcome: ExitCode,
        target: FlowTarget,
        plan_fingerprint: [u8; 32],
        input_digest: [u8; 32],
        reused_decision_id: Option<FlowDecisionId>,
        decided_at: SystemTime,
    ) -> Self {
        Self {
            id,
            job_execution_id,
            sequence,
            source_node_id,
            source_step_execution_id,
            kind,
            observed_outcome,
            target,
            plan_fingerprint,
            input_digest,
            reused_decision_id,
            decided_at,
        }
    }

    /// Returns the repository-owned identifier.
    #[must_use]
    pub const fn id(&self) -> FlowDecisionId {
        self.id
    }

    /// Returns the execution that recorded this traversal.
    #[must_use]
    pub const fn job_execution_id(&self) -> JobExecutionId {
        self.job_execution_id
    }

    /// Returns the execution-local ordering.
    #[must_use]
    pub const fn sequence(&self) -> FlowDecisionSequence {
        self.sequence
    }

    /// Borrows the source logical node.
    #[must_use]
    pub const fn source_node_id(&self) -> &NodeId {
        &self.source_node_id
    }

    /// Returns the step attempt whose durable result was observed, if any.
    #[must_use]
    pub const fn source_step_execution_id(&self) -> Option<StepExecutionId> {
        self.source_step_execution_id
    }

    /// Returns why this transition was selected.
    #[must_use]
    pub const fn kind(&self) -> FlowTransitionKind {
        self.kind
    }

    /// Borrows the bounded observed outcome.
    #[must_use]
    pub const fn observed_outcome(&self) -> &ExitCode {
        &self.observed_outcome
    }

    /// Borrows the selected node or terminal.
    #[must_use]
    pub const fn target(&self) -> &FlowTarget {
        &self.target
    }

    /// Returns the exact plan fingerprint under which the choice was made.
    #[must_use]
    pub const fn plan_fingerprint(&self) -> &[u8; 32] {
        &self.plan_fingerprint
    }

    /// Returns the value-redacted durable-input digest.
    #[must_use]
    pub const fn input_digest(&self) -> &[u8; 32] {
        &self.input_digest
    }

    /// Returns the prior committed decision reused by restart, if any.
    #[must_use]
    pub const fn reused_decision_id(&self) -> Option<FlowDecisionId> {
        self.reused_decision_id
    }

    /// Returns the injected facade-clock timestamp.
    #[must_use]
    pub const fn decided_at(&self) -> SystemTime {
        self.decided_at
    }
}

/// A validated transition awaiting repository allocation and commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FlowDecisionRequest {
    job_execution_id: JobExecutionId,
    sequence: FlowDecisionSequence,
    source_node_id: NodeId,
    source_step_execution_id: Option<StepExecutionId>,
    kind: FlowTransitionKind,
    observed_outcome: ExitCode,
    target: FlowTarget,
    plan_fingerprint: [u8; 32],
    input_digest: [u8; 32],
    reused_decision_id: Option<FlowDecisionId>,
    decided_at: SystemTime,
}

impl FlowDecisionRequest {
    /// Builds one validated transition awaiting allocation and commit.
    #[allow(clippy::too_many_arguments)]
    #[doc(hidden)]
    #[must_use]
    pub const fn new(
        job_execution_id: JobExecutionId,
        sequence: FlowDecisionSequence,
        source_node_id: NodeId,
        source_step_execution_id: Option<StepExecutionId>,
        kind: FlowTransitionKind,
        observed_outcome: ExitCode,
        target: FlowTarget,
        plan_fingerprint: [u8; 32],
        input_digest: [u8; 32],
        reused_decision_id: Option<FlowDecisionId>,
        decided_at: SystemTime,
    ) -> Self {
        Self {
            job_execution_id,
            sequence,
            source_node_id,
            source_step_execution_id,
            kind,
            observed_outcome,
            target,
            plan_fingerprint,
            input_digest,
            reused_decision_id,
            decided_at,
        }
    }

    /// Returns the execution that will own the append.
    #[must_use]
    pub const fn job_execution_id(&self) -> JobExecutionId {
        self.job_execution_id
    }
    /// Returns the expected execution-local append sequence.
    #[must_use]
    pub const fn sequence(&self) -> FlowDecisionSequence {
        self.sequence
    }
    /// Borrows the selected transition's source node.
    #[must_use]
    pub const fn source_node_id(&self) -> &NodeId {
        &self.source_node_id
    }
    /// Returns the durable source step, when the source is a step.
    #[must_use]
    pub const fn source_step_execution_id(&self) -> Option<StepExecutionId> {
        self.source_step_execution_id
    }
    /// Returns why the runtime selected this transition.
    #[must_use]
    pub const fn kind(&self) -> FlowTransitionKind {
        self.kind
    }
    /// Borrows the bounded outcome used for selection.
    #[must_use]
    pub const fn observed_outcome(&self) -> &ExitCode {
        &self.observed_outcome
    }
    /// Borrows the selected node or terminal.
    #[must_use]
    pub const fn target(&self) -> &FlowTarget {
        &self.target
    }
    /// Returns the exact persisted plan fingerprint.
    #[must_use]
    pub const fn plan_fingerprint(&self) -> &[u8; 32] {
        &self.plan_fingerprint
    }
    /// Returns the value-redacted durable-input digest.
    #[must_use]
    pub const fn input_digest(&self) -> &[u8; 32] {
        &self.input_digest
    }
    /// Returns the exact prior decision reused by restart, when present.
    #[must_use]
    pub const fn reused_decision_id(&self) -> Option<FlowDecisionId> {
        self.reused_decision_id
    }
    /// Returns the injected facade-clock decision time.
    #[must_use]
    pub const fn decided_at(&self) -> SystemTime {
        self.decided_at
    }

    /// Materializes the immutable record after an adapter allocates its ID.
    ///
    /// Repository implementations should call this only after validating the
    /// request against the persisted plan and committing its append rules.
    #[must_use]
    pub fn materialize(&self, id: FlowDecisionId) -> FlowDecision {
        FlowDecision::new(
            id,
            self.job_execution_id,
            self.sequence,
            self.source_node_id.clone(),
            self.source_step_execution_id,
            self.kind,
            self.observed_outcome.clone(),
            self.target.clone(),
            self.plan_fingerprint,
            self.input_digest,
            self.reused_decision_id,
            self.decided_at,
        )
    }
}

/// Latest durable attempt for one logical step, used to reconstruct restart.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FlowStepState {
    node_id: NodeId,
    execution: StepExecution,
    context: Option<ExecutionContext>,
}

impl FlowStepState {
    /// Constructs adapter-supplied latest logical-step state.
    ///
    /// Repository implementations must verify that `execution` belongs to the
    /// requested job instance and `node_id` before returning this value.
    #[must_use]
    pub const fn new(
        node_id: NodeId,
        execution: StepExecution,
        context: Option<ExecutionContext>,
    ) -> Self {
        Self {
            node_id,
            execution,
            context,
        }
    }

    /// Borrows the stable step logical identifier.
    #[must_use]
    pub const fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Borrows the latest step attempt.
    #[must_use]
    pub const fn execution(&self) -> &StepExecution {
        &self.execution
    }

    /// Borrows committed step context when the adapter exposes it.
    #[must_use]
    pub const fn context(&self) -> Option<&ExecutionContext> {
        self.context.as_ref()
    }
}
