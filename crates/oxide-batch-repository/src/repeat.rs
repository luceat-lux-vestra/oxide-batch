//! Bounded durable Gate-C repeat execution state.

use oxide_batch_core::{ExecutionContext, JobInstanceId, NodeId, RepeatId, StepExecutionId};

/// Zero-based durable ordinal of one logical repeat iteration.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RepeatOrdinal(u32);

impl RepeatOrdinal {
    /// The first logical iteration.
    pub const INITIAL: Self = Self(0);
    /// Constructs an ordinal from its zero-based durable value.
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }
    /// Returns the zero-based durable ordinal value.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
    /// Returns the next ordinal, or `None` when the durable counter would overflow.
    #[must_use]
    pub const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

/// Committed Gate-C decision for one accepted repeat iteration.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum RepeatDecision {
    /// Continue with the next logical repeat iteration.
    Continue,
    /// Complete the repeat lineage after this accepted iteration.
    Complete,
}

impl RepeatDecision {
    /// Returns the stable durable decision code.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Continue => "continue",
            Self::Complete => "complete",
        }
    }
    #[doc(hidden)]
    #[must_use]
    pub const fn from_str(value: &str) -> Option<Self> {
        match value.as_bytes() {
            b"continue" => Some(Self::Continue),
            b"complete" => Some(Self::Complete),
            _ => None,
        }
    }
}

/// Current committed state of one repeat definition inside one step attempt.
///
/// Only the latest accepted iteration for the step attempt is retained.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepeatExecution {
    job_instance_id: JobInstanceId,
    node_id: NodeId,
    step_execution_id: StepExecutionId,
    repeat_id: RepeatId,
    ordinal: RepeatOrdinal,
    state: ExecutionContext,
    decision: RepeatDecision,
    plan_fingerprint: [u8; 32],
}

impl RepeatExecution {
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub const fn new(
        job_instance_id: JobInstanceId,
        node_id: NodeId,
        step_execution_id: StepExecutionId,
        repeat_id: RepeatId,
        ordinal: RepeatOrdinal,
        state: ExecutionContext,
        decision: RepeatDecision,
        plan_fingerprint: [u8; 32],
    ) -> Self {
        Self {
            job_instance_id,
            node_id,
            step_execution_id,
            repeat_id,
            ordinal,
            state,
            decision,
            plan_fingerprint,
        }
    }
    /// Returns the owning logical job instance.
    #[must_use]
    pub const fn job_instance_id(&self) -> JobInstanceId {
        self.job_instance_id
    }
    /// Returns the logical step node that owns the repeat.
    #[must_use]
    pub const fn node_id(&self) -> &NodeId {
        &self.node_id
    }
    /// Returns the concrete step execution identifier.
    #[must_use]
    pub const fn step_execution_id(&self) -> StepExecutionId {
        self.step_execution_id
    }
    /// Returns the stable repeat definition identifier.
    #[must_use]
    pub const fn repeat_id(&self) -> &RepeatId {
        &self.repeat_id
    }
    /// Returns the accepted or proposed durable iteration ordinal.
    #[must_use]
    pub const fn ordinal(&self) -> RepeatOrdinal {
        self.ordinal
    }
    /// Returns the bounded application-owned repeat state.
    #[must_use]
    pub const fn state(&self) -> &ExecutionContext {
        &self.state
    }
    /// Returns the durable continue/complete decision.
    #[must_use]
    pub const fn decision(&self) -> RepeatDecision {
        self.decision
    }
    /// Returns the plan fingerprint bound to this repeat state.
    #[must_use]
    pub const fn plan_fingerprint(&self) -> &[u8; 32] {
        &self.plan_fingerprint
    }
}

/// Proposed accepted-iteration state awaiting one repository commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepeatCommitRequest {
    job_instance_id: JobInstanceId,
    node_id: NodeId,
    step_execution_id: StepExecutionId,
    repeat_id: RepeatId,
    ordinal: RepeatOrdinal,
    state: ExecutionContext,
    decision: RepeatDecision,
    plan_fingerprint: [u8; 32],
}

impl RepeatCommitRequest {
    /// Constructs a proposed accepted iteration for one repository commit.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub const fn new(
        job_instance_id: JobInstanceId,
        node_id: NodeId,
        step_execution_id: StepExecutionId,
        repeat_id: RepeatId,
        ordinal: RepeatOrdinal,
        state: ExecutionContext,
        decision: RepeatDecision,
        plan_fingerprint: [u8; 32],
    ) -> Self {
        Self {
            job_instance_id,
            node_id,
            step_execution_id,
            repeat_id,
            ordinal,
            state,
            decision,
            plan_fingerprint,
        }
    }
    /// Returns the owning logical job instance.
    #[must_use]
    pub const fn job_instance_id(&self) -> JobInstanceId {
        self.job_instance_id
    }
    /// Returns the logical step node that owns the repeat.
    #[must_use]
    pub const fn node_id(&self) -> &NodeId {
        &self.node_id
    }
    /// Returns the concrete step execution identifier.
    #[must_use]
    pub const fn step_execution_id(&self) -> StepExecutionId {
        self.step_execution_id
    }
    /// Returns the stable repeat definition identifier.
    #[must_use]
    pub const fn repeat_id(&self) -> &RepeatId {
        &self.repeat_id
    }
    /// Returns the accepted or proposed durable iteration ordinal.
    #[must_use]
    pub const fn ordinal(&self) -> RepeatOrdinal {
        self.ordinal
    }
    /// Returns the bounded application-owned repeat state.
    #[must_use]
    pub const fn state(&self) -> &ExecutionContext {
        &self.state
    }
    /// Returns the durable continue/complete decision.
    #[must_use]
    pub const fn decision(&self) -> RepeatDecision {
        self.decision
    }
    /// Returns the plan fingerprint bound to this repeat state.
    #[must_use]
    pub const fn plan_fingerprint(&self) -> &[u8; 32] {
        &self.plan_fingerprint
    }
}
