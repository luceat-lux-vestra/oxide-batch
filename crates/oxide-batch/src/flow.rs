//! Durable execution of bounded M3 flows and the M4 tasklet-only local
//! parallel-split and partition slices.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::num::NonZeroU64;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::SystemTime;

use futures_util::{FutureExt, StreamExt};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::runtime::{invoke_after_step, invoke_before_step, invoke_tasklet};
use crate::{
    BatchStatus, BoxFuture, Clock, CompiledExecutionPlan, ExecutionAttempt, ExecutionCorrelation,
    ExecutionCounts, ExitCode, ExitStatus, FailureCategory, FailureSummary, FlowNode,
    FlowSelectionError, FlowTarget, IdGenerator, JobExecution, JobExecutionId, JobInstance,
    JobInstanceId, JobInstanceKey, JobName, JobParameters, JobRepository, LifecycleTransition,
    ListenerContext, ListenerFailure, ListenerFailureKind, ListenerPhase, NodeId, PartitionKey,
    PartitionPlanEntry, RepositoryError, StartLimit, StepExecution, StepExecutionId, StepName,
    StepPartition, StopPollInterval, StopTiming, StopToken, TaskletContext,
    TaskletExecutionOutcome, TaskletFailure, TaskletOutcome, TaskletStep, TerminalKind,
};

/// Opaque durable identifier of one selected transition.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FlowDecisionId(NonZeroU64);

impl FlowDecisionId {
    /// Constructs a positive flow-decision identifier.
    ///
    /// # Errors
    ///
    /// Returns [`crate::DomainError::ZeroIdentifier`] for zero.
    pub fn new(value: u64) -> Result<Self, crate::DomainError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(crate::DomainError::ZeroIdentifier {
                kind: crate::IdentifierKind::FlowDecision,
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
    /// Returns [`FlowRuntimeError::DecisionSequenceExhausted`] for zero.
    pub fn new(value: u64) -> Result<Self, FlowRuntimeError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(FlowRuntimeError::DecisionSequenceExhausted)
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
    #[cfg(feature = "postgres")]
    pub(crate) const fn durable_code(self) -> &'static str {
        match self {
            Self::StepExit => "STEP_EXIT",
            Self::Decider => "DECIDER",
            Self::CompletedStepReuse => "COMPLETED_STEP_REUSE",
            Self::SplitAggregate => "SPLIT_AGGREGATE",
        }
    }

    #[cfg(feature = "postgres")]
    pub(crate) fn from_durable_code(value: &str) -> Option<Self> {
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
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn new(
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
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn new(
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

pub(crate) fn decision_matches_manifest(manifest: &Value, request: &FlowDecisionRequest) -> bool {
    let Some(document) = manifest.as_object() else {
        return false;
    };
    let format = document.get("format").and_then(Value::as_u64);
    if !matches!(
        format,
        Some(value)
            if value == u64::from(oxide_batch_core::MANIFEST_FORMAT_FLOW)
                || value == u64::from(oxide_batch_core::MANIFEST_FORMAT_LOCAL_SCALE)
    ) {
        return false;
    }
    let source_is_declared = document
        .get("nodes")
        .and_then(Value::as_array)
        .is_some_and(|nodes| {
            nodes.iter().any(|node| {
                let kind = node.get("kind").and_then(Value::as_str);
                let kind_matches = match request.kind() {
                    FlowTransitionKind::Decider => kind == Some("decision"),
                    FlowTransitionKind::SplitAggregate => kind == Some("join"),
                    FlowTransitionKind::StepExit | FlowTransitionKind::CompletedStepReuse => {
                        matches!(kind, Some("step" | "partitioned_step"))
                    }
                };
                node.get("id").and_then(Value::as_str) == Some(request.source_node_id().as_str())
                    && kind_matches
            })
        });
    if !source_is_declared {
        return false;
    }
    document
        .get("transitions")
        .and_then(Value::as_array)
        .and_then(|transitions| {
            transitions.iter().find(|transition| {
                transition.get("source").and_then(Value::as_str)
                    == Some(request.source_node_id().as_str())
                    && transition
                        .get("pattern")
                        .and_then(Value::as_str)
                        .and_then(|pattern| crate::ExitPattern::new(pattern).ok())
                        .is_some_and(|pattern| pattern.matches(request.observed_outcome()))
            })
        })
        .and_then(|transition| transition.get("target"))
        .is_some_and(|target| manifest_target_matches(target, request.target()))
}

fn manifest_target_matches(value: &Value, target: &FlowTarget) -> bool {
    match target {
        FlowTarget::Node(node) => value.get("node").and_then(Value::as_str) == Some(node.as_str()),
        FlowTarget::Terminal(terminal) => {
            value.get("terminal").and_then(Value::as_str) == Some(terminal.as_str())
        }
    }
}

/// Latest durable attempt for one logical step, used to reconstruct restart.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FlowStepState {
    node_id: NodeId,
    execution: StepExecution,
    context: Option<crate::ExecutionContext>,
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
        context: Option<crate::ExecutionContext>,
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
    pub const fn context(&self) -> Option<&crate::ExecutionContext> {
        self.context.as_ref()
    }
}

/// Durable preceding-step data supplied to a decider.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecisionStepInput {
    node_id: NodeId,
    execution_id: StepExecutionId,
    status: BatchStatus,
    exit_status: ExitStatus,
    counts: ExecutionCounts,
    context: Option<crate::ExecutionContext>,
}

impl DecisionStepInput {
    fn from_state(state: &FlowStepState) -> Self {
        Self {
            node_id: state.node_id.clone(),
            execution_id: state.execution.id(),
            status: state.execution.metadata().status(),
            exit_status: state.execution.metadata().exit_status().clone(),
            counts: state.execution.metadata().counts(),
            context: state.context.clone(),
        }
    }

    /// Borrows the preceding stable logical ID.
    #[must_use]
    pub const fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Returns the durable step-attempt identifier.
    #[must_use]
    pub const fn execution_id(&self) -> StepExecutionId {
        self.execution_id
    }

    /// Returns the durable lifecycle status.
    #[must_use]
    pub const fn status(&self) -> BatchStatus {
        self.status
    }

    /// Borrows the durable exit outcome.
    #[must_use]
    pub const fn exit_status(&self) -> &ExitStatus {
        &self.exit_status
    }

    /// Returns the durable counters.
    #[must_use]
    pub const fn counts(&self) -> ExecutionCounts {
        self.counts
    }

    /// Borrows committed step context when available.
    #[must_use]
    pub const fn context(&self) -> Option<&crate::ExecutionContext> {
        self.context.as_ref()
    }
}

/// Immutable, sensitivity-aware input supplied to one decider invocation.
pub struct DecisionInput<'a> {
    job_instance_id: JobInstanceId,
    job_execution_id: JobExecutionId,
    attempt: ExecutionAttempt,
    plan_fingerprint: [u8; 32],
    node_id: &'a NodeId,
    parameters: &'a JobParameters,
    preceding_step: Option<DecisionStepInput>,
}

impl fmt::Debug for DecisionInput<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DecisionInput")
            .field("job_instance_id", &self.job_instance_id)
            .field("job_execution_id", &self.job_execution_id)
            .field("attempt", &self.attempt)
            .field("node_id", &self.node_id)
            .field("parameters", &"<redacted>")
            .field("preceding_step", &self.preceding_step)
            .finish_non_exhaustive()
    }
}

impl DecisionInput<'_> {
    /// Returns the logical job instance.
    #[must_use]
    pub const fn job_instance_id(&self) -> JobInstanceId {
        self.job_instance_id
    }

    /// Returns the current execution attempt.
    #[must_use]
    pub const fn job_execution_id(&self) -> JobExecutionId {
        self.job_execution_id
    }

    /// Returns the positive attempt number.
    #[must_use]
    pub const fn attempt(&self) -> ExecutionAttempt {
        self.attempt
    }

    /// Returns the exact plan fingerprint.
    #[must_use]
    pub const fn plan_fingerprint(&self) -> &[u8; 32] {
        &self.plan_fingerprint
    }

    /// Borrows the decision-node logical ID.
    #[must_use]
    pub const fn node_id(&self) -> &NodeId {
        self.node_id
    }

    /// Borrows typed parameters; their debug representation remains redacted.
    #[must_use]
    pub const fn parameters(&self) -> &JobParameters {
        self.parameters
    }

    /// Borrows the preceding durable step observation, when one exists.
    #[must_use]
    pub const fn preceding_step(&self) -> Option<&DecisionStepInput> {
        self.preceding_step.as_ref()
    }
}

/// A value-redacted decider failure.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DeciderError;

impl DeciderError {
    /// Constructs a redacted failure.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl fmt::Display for DeciderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("flow decider failed")
    }
}

impl Error for DeciderError {}

/// A deterministic, side-effect-free M3 flow decider.
pub trait JobExecutionDecider: Send + Sync {
    /// Selects one bounded flow-facing exit outcome from durable input.
    fn decide<'a>(
        &'a self,
        input: DecisionInput<'a>,
    ) -> BoxFuture<'a, Result<ExitStatus, DeciderError>>;
}

async fn invoke_decider(
    decider: &dyn JobExecutionDecider,
    input: DecisionInput<'_>,
) -> Result<ExitStatus, FlowFailure> {
    let future = catch_unwind(AssertUnwindSafe(|| decider.decide(input)))
        .map_err(|_| FlowFailure::DeciderPanic)?;
    match AssertUnwindSafe(future).catch_unwind().await {
        Ok(Ok(outcome)) => Ok(outcome),
        Ok(Err(_)) => Err(FlowFailure::DeciderError),
        Err(_) => Err(FlowFailure::DeciderPanic),
    }
}

/// A launch-scoped factory for one tasklet inside a bounded split branch.
///
/// A factory is invoked once per branch step and launch attempt before durable
/// work begins. This keeps component instances branch-owned instead of
/// silently sharing one tasklet across concurrent children.
#[derive(Clone)]
pub struct TaskletStepFactory {
    step_name: StepName,
    create: Arc<dyn Fn() -> TaskletStep + Send + Sync>,
}

/// Deterministic inputs supplied once when a partition plan does not yet exist.
#[derive(Clone, Copy, Debug)]
pub struct PartitionPlanRequest<'a> {
    plan_fingerprint: &'a [u8; 32],
    job_instance_id: JobInstanceId,
    node_id: &'a NodeId,
    partition_count: crate::PartitionCount,
}

impl<'a> PartitionPlanRequest<'a> {
    /// Borrows the exact restart-relevant plan fingerprint.
    #[must_use]
    pub const fn plan_fingerprint(self) -> &'a [u8; 32] {
        self.plan_fingerprint
    }

    /// Returns the stable job-instance identity.
    #[must_use]
    pub const fn job_instance_id(self) -> JobInstanceId {
        self.job_instance_id
    }

    /// Borrows the partition-manager logical ID.
    #[must_use]
    pub const fn node_id(self) -> &'a NodeId {
        self.node_id
    }

    /// Returns the exact declared partition count.
    #[must_use]
    pub const fn partition_count(self) -> crate::PartitionCount {
        self.partition_count
    }
}

/// Redacted rejection from an application partitioner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PartitionFactoryError {
    /// The partitioner could not produce a valid complete plan.
    Rejected,
}

impl fmt::Display for PartitionFactoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("partition factory rejected the requested plan")
    }
}

impl Error for PartitionFactoryError {}

/// Launch-scoped deterministic constructor for one complete partition plan.
type PartitionPlanConstructor = dyn for<'a> Fn(PartitionPlanRequest<'a>) -> Result<Vec<PartitionPlanEntry>, PartitionFactoryError>
    + Send
    + Sync;

/// Launch-scoped deterministic constructor for one complete partition plan.
#[derive(Clone)]
pub struct PartitionPlanFactory {
    create: Arc<PartitionPlanConstructor>,
}

impl PartitionPlanFactory {
    /// Wraps one deterministic, bounded plan constructor.
    #[must_use]
    pub fn new(
        create: impl for<'a> Fn(
            PartitionPlanRequest<'a>,
        ) -> Result<Vec<PartitionPlanEntry>, PartitionFactoryError>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self {
            create: Arc::new(create),
        }
    }

    fn create(
        &self,
        request: PartitionPlanRequest<'_>,
    ) -> Result<Vec<PartitionPlanEntry>, PartitionFactoryError> {
        (self.create)(request)
    }
}

impl fmt::Debug for PartitionPlanFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PartitionPlanFactory(<redacted>)")
    }
}

/// Owned, bounded input supplied to one partition worker factory.
#[derive(Clone, Eq, PartialEq)]
pub struct PartitionWorkerInput {
    key: PartitionKey,
    ordinal: u32,
    context: crate::ExecutionContext,
}

impl PartitionWorkerInput {
    fn from_partition(partition: &StepPartition) -> Self {
        Self {
            key: partition.key().clone(),
            ordinal: partition.ordinal(),
            context: partition.context().clone(),
        }
    }

    /// Borrows the byte-exact durable partition key.
    #[must_use]
    pub const fn key(&self) -> &PartitionKey {
        &self.key
    }

    /// Returns the stable one-based partition ordinal.
    #[must_use]
    pub const fn ordinal(&self) -> u32 {
        self.ordinal
    }

    /// Borrows the bounded durable partition context.
    #[must_use]
    pub const fn context(&self) -> &crate::ExecutionContext {
        &self.context
    }
}

impl fmt::Debug for PartitionWorkerInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PartitionWorkerInput")
            .field("key", &self.key)
            .field("ordinal", &self.ordinal)
            .field("context", &"<redacted>")
            .finish()
    }
}

/// Per-child constructor that creates an independently owned tasklet step.
#[derive(Clone)]
pub struct PartitionTaskletFactory {
    step_name: StepName,
    create: Arc<dyn Fn(PartitionWorkerInput) -> TaskletStep + Send + Sync>,
}

impl PartitionTaskletFactory {
    /// Declares the worker name and its per-partition constructor.
    #[must_use]
    pub fn new(
        step_name: StepName,
        create: impl Fn(PartitionWorkerInput) -> TaskletStep + Send + Sync + 'static,
    ) -> Self {
        Self {
            step_name,
            create: Arc::new(create),
        }
    }

    /// Borrows the worker step name promised by the factory.
    #[must_use]
    pub const fn step_name(&self) -> &StepName {
        &self.step_name
    }

    fn create(&self, input: PartitionWorkerInput) -> TaskletStep {
        (self.create)(input)
    }
}

impl fmt::Debug for PartitionTaskletFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PartitionTaskletFactory")
            .field("step_name", &self.step_name)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
struct PartitionedTaskletBinding {
    partitioner: PartitionPlanFactory,
    worker: PartitionTaskletFactory,
}

impl TaskletStepFactory {
    /// Declares the step name and its launch-scoped constructor.
    #[must_use]
    pub fn new(
        step_name: StepName,
        create: impl Fn() -> TaskletStep + Send + Sync + 'static,
    ) -> Self {
        Self {
            step_name,
            create: Arc::new(create),
        }
    }

    /// Borrows the step name promised by this factory.
    #[must_use]
    pub const fn step_name(&self) -> &StepName {
        &self.step_name
    }

    fn create(&self) -> TaskletStep {
        (self.create)()
    }
}

impl fmt::Debug for TaskletStepFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaskletStepFactory")
            .field("step_name", &self.step_name)
            .finish_non_exhaustive()
    }
}

/// An executable binding for one compiled format-2 or bounded split flow.
pub struct FlowJob {
    name: JobName,
    plan: CompiledExecutionPlan,
    steps: BTreeMap<NodeId, TaskletStep>,
    deciders: BTreeMap<NodeId, Arc<dyn JobExecutionDecider>>,
    split_tasklets: BTreeMap<NodeId, TaskletStepFactory>,
    partitioned_tasklets: BTreeMap<NodeId, PartitionedTaskletBinding>,
}

impl fmt::Debug for FlowJob {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FlowJob")
            .field("name", &self.name)
            .field("definition", self.plan.definition_identity())
            .field("step_count", &self.steps.len())
            .field("decider_count", &self.deciders.len())
            .field("split_tasklet_count", &self.split_tasklets.len())
            .field(
                "partitioned_tasklet_count",
                &self.partitioned_tasklets.len(),
            )
            .finish()
    }
}

impl FlowJob {
    /// Starts binding executable components to a compiled format-2 or format-3 plan.
    ///
    /// # Errors
    ///
    /// Rejects a compatibility plan or a plan identified under another job.
    pub fn new(name: JobName, plan: CompiledExecutionPlan) -> Result<Self, FlowJobError> {
        if !matches!(
            plan.manifest_format(),
            oxide_batch_core::MANIFEST_FORMAT_FLOW | oxide_batch_core::MANIFEST_FORMAT_LOCAL_SCALE
        ) {
            return Err(FlowJobError::UnsupportedManifest {
                format: plan.manifest_format(),
            });
        }
        if plan.definition_identity().job_name() != Some(&name) {
            return Err(FlowJobError::JobNameMismatch);
        }
        Ok(Self {
            name,
            plan,
            steps: BTreeMap::new(),
            deciders: BTreeMap::new(),
            split_tasklets: BTreeMap::new(),
            partitioned_tasklets: BTreeMap::new(),
        })
    }

    /// Binds one tasklet step to a compiled step node.
    ///
    /// # Errors
    ///
    /// Rejects an unknown node, a decision node, a name mismatch, or a second
    /// binding for the same logical ID.
    pub fn with_tasklet_step(
        mut self,
        node_id: NodeId,
        step: TaskletStep,
    ) -> Result<Self, FlowJobError> {
        self.bind_tasklet_step(node_id, step)?;
        Ok(self)
    }

    pub(crate) fn bind_tasklet_step(
        &mut self,
        node_id: NodeId,
        step: TaskletStep,
    ) -> Result<(), FlowJobError> {
        let Some(FlowNode::Step(compiled)) = self.plan.node(&node_id) else {
            return Err(FlowJobError::WrongNodeKind { node: node_id });
        };
        if !matches!(compiled.components(), crate::StepComponents::Tasklet(_)) {
            return Err(FlowJobError::ComponentMismatch { node: node_id });
        }
        if compiled.step_name() != step.name() {
            return Err(FlowJobError::StepNameMismatch { node: node_id });
        }
        if self.steps.insert(node_id.clone(), step).is_some() {
            return Err(FlowJobError::DuplicateBinding { node: node_id });
        }
        Ok(())
    }

    pub(crate) fn bind_chunk_tasklet(
        &mut self,
        node_id: NodeId,
        step: TaskletStep,
    ) -> Result<(), FlowJobError> {
        let Some(FlowNode::Step(compiled)) = self.plan.node(&node_id) else {
            return Err(FlowJobError::WrongNodeKind { node: node_id });
        };
        if !matches!(compiled.components(), crate::StepComponents::Chunk { .. }) {
            return Err(FlowJobError::ComponentMismatch { node: node_id });
        }
        if compiled.step_name() != step.name() {
            return Err(FlowJobError::StepNameMismatch { node: node_id });
        }
        if self.steps.insert(node_id.clone(), step).is_some() {
            return Err(FlowJobError::DuplicateBinding { node: node_id });
        }
        Ok(())
    }

    /// Binds one deterministic decider to a compiled decision node.
    ///
    /// # Errors
    ///
    /// Rejects an unknown/step node or a duplicate binding.
    pub fn with_decider(
        mut self,
        node_id: NodeId,
        decider: Arc<dyn JobExecutionDecider>,
    ) -> Result<Self, FlowJobError> {
        if !matches!(self.plan.node(&node_id), Some(FlowNode::Decision(_))) {
            return Err(FlowJobError::WrongNodeKind { node: node_id });
        }
        if self.deciders.insert(node_id.clone(), decider).is_some() {
            return Err(FlowJobError::DuplicateBinding { node: node_id });
        }
        Ok(self)
    }

    /// Binds a launch-scoped tasklet factory to one embedded split step.
    ///
    /// # Errors
    ///
    /// Rejects an unknown/non-tasklet branch step, a declared-name mismatch,
    /// or a second binding for the same logical ID.
    pub fn with_split_tasklet_factory(
        mut self,
        node_id: NodeId,
        factory: TaskletStepFactory,
    ) -> Result<Self, FlowJobError> {
        let Some(compiled) = split_step(&self.plan, &node_id) else {
            return Err(FlowJobError::WrongNodeKind { node: node_id });
        };
        if !matches!(compiled.components(), crate::StepComponents::Tasklet(_)) {
            return Err(FlowJobError::ComponentMismatch { node: node_id });
        }
        if compiled.step_name() != factory.step_name() {
            return Err(FlowJobError::StepNameMismatch { node: node_id });
        }
        if self
            .split_tasklets
            .insert(node_id.clone(), factory)
            .is_some()
        {
            return Err(FlowJobError::DuplicateBinding { node: node_id });
        }
        Ok(self)
    }

    /// Binds a deterministic plan factory and independent tasklet factory to
    /// one compiled local partition manager.
    ///
    /// # Errors
    ///
    /// Rejects an unknown/non-tasklet partition node, worker-name mismatch,
    /// or duplicate binding.
    pub fn with_partitioned_tasklet(
        mut self,
        node_id: NodeId,
        partitioner: PartitionPlanFactory,
        worker: PartitionTaskletFactory,
    ) -> Result<Self, FlowJobError> {
        let Some(FlowNode::PartitionedStep(compiled)) = self.plan.node(&node_id) else {
            return Err(FlowJobError::WrongNodeKind { node: node_id });
        };
        if !matches!(
            compiled.worker().components(),
            crate::StepComponents::Tasklet(_)
        ) {
            return Err(FlowJobError::ComponentMismatch { node: node_id });
        }
        if compiled.worker().step_name() != worker.step_name() {
            return Err(FlowJobError::StepNameMismatch { node: node_id });
        }
        if self
            .partitioned_tasklets
            .insert(
                node_id.clone(),
                PartitionedTaskletBinding {
                    partitioner,
                    worker,
                },
            )
            .is_some()
        {
            return Err(FlowJobError::DuplicateBinding { node: node_id });
        }
        Ok(self)
    }

    /// Validates that every compiled node has exactly one executable binding.
    ///
    /// # Errors
    ///
    /// Returns the first missing node in canonical identifier order.
    pub fn validate(&self) -> Result<(), FlowJobError> {
        for (id, node) in self.plan.nodes() {
            if let FlowNode::Split(split) = node {
                for step in split.branches().iter().flat_map(crate::SplitBranch::steps) {
                    if !self.split_tasklets.contains_key(step.id()) {
                        return Err(FlowJobError::MissingBinding {
                            node: step.id().clone(),
                        });
                    }
                }
                continue;
            }
            let present = match node {
                FlowNode::Step(_) => self.steps.contains_key(id),
                FlowNode::Decision(_) => self.deciders.contains_key(id),
                FlowNode::Split(_) | FlowNode::Join(_) => true,
                FlowNode::PartitionedStep(_) => self.partitioned_tasklets.contains_key(id),
            };
            if !present {
                return Err(FlowJobError::MissingBinding { node: id.clone() });
            }
        }
        Ok(())
    }

    fn materialize_split_tasklets(&self) -> Result<BTreeMap<NodeId, TaskletStep>, FlowJobError> {
        let mut tasklets = BTreeMap::new();
        for (node, factory) in &self.split_tasklets {
            let step = catch_unwind(AssertUnwindSafe(|| factory.create()))
                .map_err(|_| FlowJobError::FactoryPanic { node: node.clone() })?;
            if step.name() != factory.step_name() {
                return Err(FlowJobError::StepNameMismatch { node: node.clone() });
            }
            tasklets.insert(node.clone(), step);
        }
        Ok(tasklets)
    }

    /// Borrows the compiled plan.
    #[must_use]
    pub const fn compiled_plan(&self) -> &CompiledExecutionPlan {
        &self.plan
    }
}

/// An executable component assembly that does not match its compiled plan.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum FlowJobError {
    /// General flows require canonical manifest format 2.
    UnsupportedManifest {
        /// Observed manifest format.
        format: u16,
    },
    /// The facade job name and manifest identity differ.
    JobNameMismatch,
    /// The node is absent or has the other executable kind.
    WrongNodeKind {
        /// Logical node that could not accept the binding.
        node: NodeId,
    },
    /// The durable step name differs from the compiled declaration.
    StepNameMismatch {
        /// Logical node whose durable step name differed.
        node: NodeId,
    },
    /// A node was bound more than once.
    DuplicateBinding {
        /// Logical node bound more than once.
        node: NodeId,
    },
    /// A compiled node has no executable component.
    MissingBinding {
        /// Logical node without executable code.
        node: NodeId,
    },
    /// The bound executable does not match the compiled step declaration.
    ComponentMismatch {
        /// Mismatched logical node.
        node: NodeId,
    },
    /// A branch component factory panicked before durable launch.
    FactoryPanic {
        /// Logical branch step whose factory panicked.
        node: NodeId,
    },
}

impl fmt::Display for FlowJobError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedManifest { format } => {
                write!(
                    formatter,
                    "flow execution requires manifest format 2 or bounded format 3, found {format}"
                )
            }
            Self::JobNameMismatch => formatter.write_str("flow job name does not match its plan"),
            Self::WrongNodeKind { node } => {
                write!(
                    formatter,
                    "node {} has no matching executable kind",
                    node.as_str()
                )
            }
            Self::StepNameMismatch { node } => {
                write!(
                    formatter,
                    "node {} was bound to a different step name",
                    node.as_str()
                )
            }
            Self::DuplicateBinding { node } => {
                write!(
                    formatter,
                    "node {} has more than one executable binding",
                    node.as_str()
                )
            }
            Self::MissingBinding { node } => {
                write!(
                    formatter,
                    "node {} has no executable binding",
                    node.as_str()
                )
            }
            Self::ComponentMismatch { node } => write!(
                formatter,
                "node {} executable components do not match the compiled declaration",
                node.as_str()
            ),
            Self::FactoryPanic { node } => write!(
                formatter,
                "node {} component factory panicked",
                node.as_str()
            ),
        }
    }
}

impl Error for FlowJobError {}

fn split_step<'a>(
    plan: &'a CompiledExecutionPlan,
    node_id: &NodeId,
) -> Option<&'a crate::StepNode> {
    plan.nodes().find_map(|(_, node)| match node {
        FlowNode::Split(split) => split
            .branches()
            .iter()
            .flat_map(crate::SplitBranch::steps)
            .find(|step| step.id() == node_id),
        _ => None,
    })
}

/// Why a durable flow attempt ended.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum FlowExecutionOutcome {
    /// A `Complete` terminal was reached.
    Completed,
    /// A `Stop` terminal or cooperative stop was reached.
    Stopped,
    /// The durable outcome is ambiguous and no transition was selected.
    Unknown,
    /// A step, decider, listener, mapping, or start control failed closed.
    Failed(FlowFailure),
}

/// Stable, value-redacted flow failure classification.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum FlowFailure {
    /// A tasklet returned an error or panicked.
    Tasklet(TaskletFailure),
    /// A step listener returned an error or panicked.
    Listener(TaskletFailure),
    /// A decider returned an error.
    DeciderError,
    /// A decider panicked at the framework boundary.
    DeciderPanic,
    /// The deterministic partitioner rejected its bounded request.
    PartitionerError,
    /// The deterministic partitioner panicked at the framework boundary.
    PartitionerPanic,
    /// A per-child component factory panicked before tasklet invocation.
    PartitionFactoryPanic,
    /// A produced exit outcome had no mapping.
    UnmappedExitOutcome {
        /// Source logical node.
        node: NodeId,
        /// Unmapped bounded outcome.
        code: ExitCode,
    },
    /// The instance-wide start limit was exhausted.
    StartLimitExceeded {
        /// Logical step whose start was rejected.
        node: NodeId,
        /// Configured instance-wide maximum.
        limit: StartLimit,
    },
    /// The compiled flow deliberately selected a `Fail` terminal.
    FailTerminal,
}

/// Final durable observations from one flow attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FlowLaunchReport {
    instance: JobInstance,
    job_execution: JobExecution,
    step_executions: Vec<StepExecution>,
    decisions: Vec<FlowDecision>,
    outcome: FlowExecutionOutcome,
    listener_failures: Vec<ListenerFailure>,
}

/// Stable, post-commit observations for the bounded flow runtime.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum FlowEventKind {
    /// One step's terminal lifecycle result committed before transition selection.
    StepResultCommitted,
    /// A selected result and target committed before the target starts.
    DecisionCommitted,
    /// A completed historical step supplied the source result on restart.
    CompletedStepReused,
    /// The instance-wide logical-step start limit rejected another start.
    StartLimitExceeded,
}

impl FlowEventKind {
    /// Maps this flow observation into telemetry schema version 1.
    #[must_use]
    pub const fn telemetry_kind(self) -> crate::TelemetryEventKind {
        match self {
            Self::StepResultCommitted => crate::TelemetryEventKind::FlowStepResultCommitted,
            Self::DecisionCommitted => crate::TelemetryEventKind::FlowDecisionCommitted,
            Self::CompletedStepReused => crate::TelemetryEventKind::FlowCompletedStepReused,
            Self::StartLimitExceeded => crate::TelemetryEventKind::StepStartLimitExceeded,
        }
    }

    /// Returns the stable dotted event name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StepResultCommitted => "flow.step_result_committed",
            Self::DecisionCommitted => "flow.decision_committed",
            Self::CompletedStepReused => "flow.completed_step_reused",
            Self::StartLimitExceeded => "step.start_limit_exceeded",
        }
    }
}

impl fmt::Display for FlowEventKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A value-redacted flow observation emitted only after its named decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FlowEvent {
    kind: FlowEventKind,
    job_name: JobName,
    job_instance_id: JobInstanceId,
    job_execution_id: JobExecutionId,
    job_attempt: ExecutionAttempt,
    source_node_id: NodeId,
    source_step_execution_id: Option<StepExecutionId>,
    target: Option<FlowTarget>,
    occurred_at: SystemTime,
}

impl FlowEvent {
    /// Returns the telemetry schema version carried by this event mapping.
    #[must_use]
    pub const fn schema_version(&self) -> u16 {
        crate::TELEMETRY_SCHEMA_VERSION
    }
    #[allow(clippy::too_many_arguments)]
    const fn new(
        kind: FlowEventKind,
        job_name: JobName,
        job_instance_id: JobInstanceId,
        job_execution_id: JobExecutionId,
        job_attempt: ExecutionAttempt,
        source_node_id: NodeId,
        source_step_execution_id: Option<StepExecutionId>,
        target: Option<FlowTarget>,
        occurred_at: SystemTime,
    ) -> Self {
        Self {
            kind,
            job_name,
            job_instance_id,
            job_execution_id,
            job_attempt,
            source_node_id,
            source_step_execution_id,
            target,
            occurred_at,
        }
    }

    /// Returns the stable event category.
    #[must_use]
    pub const fn kind(&self) -> FlowEventKind {
        self.kind
    }

    /// Borrows the bounded job definition name.
    #[must_use]
    pub const fn job_name(&self) -> &JobName {
        &self.job_name
    }

    /// Returns the logical job instance.
    #[must_use]
    pub const fn job_instance_id(&self) -> JobInstanceId {
        self.job_instance_id
    }

    /// Returns the attempt that emitted this observation.
    #[must_use]
    pub const fn job_execution_id(&self) -> JobExecutionId {
        self.job_execution_id
    }

    /// Returns the instance-scoped job attempt ordinal.
    #[must_use]
    pub const fn job_attempt(&self) -> ExecutionAttempt {
        self.job_attempt
    }

    /// Borrows the bounded logical source node.
    #[must_use]
    pub const fn source_node_id(&self) -> &NodeId {
        &self.source_node_id
    }

    /// Returns the durable source step when this event follows step work.
    #[must_use]
    pub const fn source_step_execution_id(&self) -> Option<StepExecutionId> {
        self.source_step_execution_id
    }

    /// Borrows the selected target for a committed decision.
    #[must_use]
    pub const fn target(&self) -> Option<&FlowTarget> {
        self.target.as_ref()
    }

    /// Returns the injected facade-clock observation instant.
    #[must_use]
    pub const fn occurred_at(&self) -> SystemTime {
        self.occurred_at
    }
}

/// A non-authoritative observer of committed flow decisions.
pub trait FlowEventSink: Send + Sync {
    /// Observes one bounded event. Failure or panic cannot change traversal.
    fn emit(&self, event: &FlowEvent);
}

impl FlowLaunchReport {
    /// Borrows the selected logical instance.
    #[must_use]
    pub const fn instance(&self) -> &JobInstance {
        &self.instance
    }
    /// Borrows the final job-attempt snapshot.
    #[must_use]
    pub const fn job_execution(&self) -> &JobExecution {
        &self.job_execution
    }
    /// Borrows step attempts created by this execution in traversal order.
    #[must_use]
    pub fn step_executions(&self) -> &[StepExecution] {
        &self.step_executions
    }
    /// Borrows committed selected transitions in sequence order.
    #[must_use]
    pub fn decisions(&self) -> &[FlowDecision] {
        &self.decisions
    }
    /// Borrows the classified terminal outcome.
    #[must_use]
    pub const fn outcome(&self) -> &FlowExecutionOutcome {
        &self.outcome
    }
    /// Borrows redacted step-listener failures.
    #[must_use]
    pub fn listener_failures(&self) -> &[ListenerFailure] {
        &self.listener_failures
    }
}

/// A flow operation that could not produce a trustworthy final report.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum FlowRuntimeError {
    /// The executable assembly is incomplete or mismatched.
    Job(FlowJobError),
    /// A repository transaction failed.
    Repository(RepositoryError),
    /// The decision sequence exceeded `u64`.
    DecisionSequenceExhausted,
    /// A facade count could not be represented safely.
    CountExhausted,
    /// Process shutdown stopped intake before this launch was accepted.
    ShuttingDown,
    /// The connected repository cannot supply the manifest connection budget.
    InsufficientPoolCapacity {
        /// Required finite connection count.
        required: u32,
        /// Repository-reported finite capacity.
        configured: u32,
    },
    /// A durable ambiguous child blocks assignment and parent aggregation.
    UnresolvedPartitionOutcome {
        /// Parent partitioned step execution left non-terminal.
        step_execution_id: StepExecutionId,
    },
    /// The application partitioner failed before a complete plan committed.
    PartitionerRejected {
        /// Whether the failure crossed a panic boundary.
        panicked: bool,
    },
}

impl fmt::Display for FlowRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Job(error) => write!(formatter, "flow job is invalid: {error}"),
            Self::Repository(error) => {
                write!(formatter, "flow repository operation failed: {error}")
            }
            Self::DecisionSequenceExhausted => {
                formatter.write_str("flow decision sequence is exhausted")
            }
            Self::CountExhausted => formatter.write_str("flow execution count is exhausted"),
            Self::ShuttingDown => formatter.write_str("runtime intake is shutting down"),
            Self::InsufficientPoolCapacity {
                required,
                configured,
            } => write!(
                formatter,
                "flow requires {required} repository connections but only {configured} are configured"
            ),
            Self::UnresolvedPartitionOutcome { step_execution_id } => write!(
                formatter,
                "partition parent step execution {step_execution_id} is blocked by an unknown child outcome"
            ),
            Self::PartitionerRejected { panicked } => {
                if *panicked {
                    formatter.write_str("partition factory panicked")
                } else {
                    formatter.write_str("partition factory rejected the plan")
                }
            }
        }
    }
}

impl Error for FlowRuntimeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Job(error) => Some(error),
            Self::Repository(error) => Some(error),
            Self::DecisionSequenceExhausted
            | Self::CountExhausted
            | Self::ShuttingDown
            | Self::InsufficientPoolCapacity { .. }
            | Self::UnresolvedPartitionOutcome { .. }
            | Self::PartitionerRejected { .. } => None,
        }
    }
}

impl From<RepositoryError> for FlowRuntimeError {
    fn from(error: RepositoryError) -> Self {
        Self::Repository(error)
    }
}

impl From<FlowJobError> for FlowRuntimeError {
    fn from(error: FlowJobError) -> Self {
        Self::Job(error)
    }
}

/// Async-first launcher for durable sequential, conditional, and bounded split flows.
pub struct FlowLauncher<'a> {
    repository: &'a dyn JobRepository,
    clock: &'a dyn Clock,
    ids: &'a dyn IdGenerator,
    event_sink: Option<&'a dyn FlowEventSink>,
    execution_control: Option<(crate::OwnerToken, StopPollInterval)>,
    shutdown_signal: Option<&'a crate::ShutdownSignal>,
}

impl<'a> FlowLauncher<'a> {
    /// Constructs a launcher from explicit infrastructure ports.
    #[must_use]
    pub const fn new(
        repository: &'a dyn JobRepository,
        clock: &'a dyn Clock,
        ids: &'a dyn IdGenerator,
    ) -> Self {
        Self {
            repository,
            clock,
            ids,
            event_sink: None,
            execution_control: None,
            shutdown_signal: None,
        }
    }

    /// Attaches a non-authoritative flow-event sink.
    #[must_use]
    pub const fn with_event_sink(mut self, event_sink: &'a dyn FlowEventSink) -> Self {
        self.event_sink = Some(event_sink);
        self
    }

    /// Enables durable ownership evidence and operator-stop polling.
    #[must_use]
    pub const fn with_execution_control(
        mut self,
        owner: crate::OwnerToken,
        interval: StopPollInterval,
    ) -> Self {
        self.execution_control = Some((owner, interval));
        self
    }

    /// Attaches the application-owned process-shutdown intake and cancellation signal.
    #[must_use]
    pub const fn with_shutdown_signal(mut self, signal: &'a crate::ShutdownSignal) -> Self {
        self.shutdown_signal = Some(signal);
        self
    }

    /// Executes one durable sequential, conditional, or bounded split attempt.
    ///
    /// The source step result is committed before its transition, and the
    /// transition is committed before the next target starts. Expected user
    /// failures are returned in [`FlowLaunchReport`]; infrastructure failures
    /// that prevent a trustworthy final snapshot return [`FlowRuntimeError`].
    ///
    /// # Errors
    ///
    /// Returns [`FlowRuntimeError`] when the job bindings are invalid or the
    /// repository cannot durably create, update, or read the execution.
    #[allow(clippy::too_many_lines)]
    pub async fn launch(
        &self,
        job: &FlowJob,
        parameters: &JobParameters,
        stop_token: &StopToken,
    ) -> Result<FlowLaunchReport, FlowRuntimeError> {
        self.ensure_accepting()?;
        job.validate()?;
        self.validate_repository_capacity(job.compiled_plan())?;
        let split_tasklets = job.materialize_split_tasklets()?;
        let key = JobInstanceKey::new(job.name.clone(), parameters);
        let (instance, mut execution, attempt) = self
            .create_job_execution(&key, job.plan.definition_identity())
            .await?;
        execution = self.start_job(&execution).await?;
        self.poll_execution_control(execution.id(), stop_token)
            .await?;
        self.observe_process_shutdown(stop_token);

        let mut node_id = job.plan.entry().clone();
        let mut preceding: Option<FlowStepState> = None;
        let mut steps = Vec::new();
        let mut decisions = Vec::new();
        let mut listener_failures = Vec::new();

        loop {
            self.observe_process_shutdown(stop_token);
            if stop_token.is_stop_requested() {
                let final_job = self
                    .finish_job(&execution, BatchStatus::Stopped, None)
                    .await?;
                return Ok(FlowLaunchReport {
                    instance,
                    job_execution: final_job,
                    step_executions: steps,
                    decisions,
                    outcome: FlowExecutionOutcome::Stopped,
                    listener_failures,
                });
            }

            let node = job.plan.node(&node_id).ok_or_else(|| {
                FlowRuntimeError::Job(FlowJobError::MissingBinding {
                    node: node_id.clone(),
                })
            })?;
            let (
                transition_node,
                observed,
                source_step,
                kind,
                input_digest,
                reused,
                source_failure,
            ) = match node {
                FlowNode::Step(compiled) => {
                    let historical = self.latest_step(instance.id(), &node_id).await?;
                    if let Some(history) = historical.as_ref()
                        && history.execution().metadata().status() == BatchStatus::Completed
                        && !compiled.start_controls().allow_start_if_complete()
                    {
                        let digest = step_input_digest(job.plan.fingerprint(), history);
                        let reused = self
                            .reusable_decision(
                                instance.id(),
                                &node_id,
                                job.plan.fingerprint(),
                                &digest,
                                FlowTransitionKind::StepExit,
                            )
                            .await?;
                        preceding = Some(history.clone());
                        (
                            node_id.clone(),
                            history.execution().metadata().exit_status().clone(),
                            Some(history.execution().id()),
                            FlowTransitionKind::CompletedStepReuse,
                            digest,
                            reused.map(|decision| decision.id()),
                            None,
                        )
                    } else {
                        let tasklet = job.steps.get(&node_id).ok_or_else(|| {
                            FlowRuntimeError::Job(FlowJobError::MissingBinding {
                                node: node_id.clone(),
                            })
                        })?;
                        let created = match self
                            .create_step(
                                execution.id(),
                                compiled.step_name(),
                                &node_id,
                                compiled.start_controls().start_limit(),
                            )
                            .await
                        {
                            Ok(created) => created,
                            Err(FlowRuntimeError::Repository(
                                RepositoryError::StartLimitExceeded { limit, .. },
                            )) => {
                                self.emit_flow_event(&FlowEvent::new(
                                    FlowEventKind::StartLimitExceeded,
                                    job.name.clone(),
                                    instance.id(),
                                    execution.id(),
                                    attempt,
                                    node_id.clone(),
                                    None,
                                    None,
                                    self.clock.now(),
                                ));
                                let failure =
                                    self.next_failure_summary(FailureCategory::IllegalTransition)?;
                                let final_job = self
                                    .finish_job(&execution, BatchStatus::Failed, Some(failure))
                                    .await?;
                                return Ok(FlowLaunchReport {
                                    instance,
                                    job_execution: final_job,
                                    step_executions: steps,
                                    decisions,
                                    outcome: FlowExecutionOutcome::Failed(
                                        FlowFailure::StartLimitExceeded {
                                            node: node_id,
                                            limit,
                                        },
                                    ),
                                    listener_failures,
                                });
                            }
                            Err(error) => return Err(error),
                        };
                        let correlation = correlation(
                            &job.name,
                            instance.id(),
                            execution.id(),
                            attempt,
                            compiled.step_name(),
                            created.id(),
                            steps.len(),
                        )?;
                        let run = self
                            .run_step(
                                &node_id,
                                tasklet,
                                created,
                                parameters,
                                stop_token,
                                &correlation,
                            )
                            .await?;
                        listener_failures.extend(run.listener_failures);
                        steps.push(run.execution.clone());
                        if run.outcome == TaskletExecutionOutcome::Unknown {
                            let final_job = self
                                .finish_job(&execution, BatchStatus::Unknown, run.failure)
                                .await?;
                            return Ok(FlowLaunchReport {
                                instance,
                                job_execution: final_job,
                                step_executions: steps,
                                decisions,
                                outcome: FlowExecutionOutcome::Unknown,
                                listener_failures,
                            });
                        }
                        if matches!(run.outcome, TaskletExecutionOutcome::Stopped(_)) {
                            let final_job = self
                                .finish_job(&execution, BatchStatus::Stopped, None)
                                .await?;
                            return Ok(FlowLaunchReport {
                                instance,
                                job_execution: final_job,
                                step_executions: steps,
                                decisions,
                                outcome: FlowExecutionOutcome::Stopped,
                                listener_failures,
                            });
                        }
                        let state = self.latest_step(instance.id(), &node_id).await?.ok_or(
                            FlowRuntimeError::Repository(RepositoryError::FlowStateCorrupt),
                        )?;
                        let digest = step_input_digest(job.plan.fingerprint(), &state);
                        preceding = Some(state);
                        (
                            node_id.clone(),
                            run.exit_status,
                            Some(run.execution.id()),
                            FlowTransitionKind::StepExit,
                            digest,
                            None,
                            run.flow_failure,
                        )
                    }
                }
                FlowNode::Decision(compiled) => {
                    let digest = decision_input_digest(
                        job.plan.fingerprint(),
                        &node_id,
                        compiled.revision().as_str(),
                        compiled.input_version().get(),
                        instance.id(),
                        parameters,
                        preceding.as_ref(),
                    );
                    if let Some(prior) = self
                        .reusable_decision(
                            instance.id(),
                            &node_id,
                            job.plan.fingerprint(),
                            &digest,
                            FlowTransitionKind::Decider,
                        )
                        .await?
                    {
                        (
                            node_id.clone(),
                            ExitStatus::new(prior.observed_outcome().clone()),
                            None,
                            FlowTransitionKind::Decider,
                            digest,
                            Some(prior.id()),
                            None,
                        )
                    } else {
                        let decider = job.deciders.get(&node_id).ok_or_else(|| {
                            FlowRuntimeError::Job(FlowJobError::MissingBinding {
                                node: node_id.clone(),
                            })
                        })?;
                        let input = DecisionInput {
                            job_instance_id: instance.id(),
                            job_execution_id: execution.id(),
                            attempt,
                            plan_fingerprint: *job.plan.fingerprint(),
                            node_id: &node_id,
                            parameters,
                            preceding_step: preceding.as_ref().map(DecisionStepInput::from_state),
                        };
                        match invoke_decider(decider.as_ref(), input).await {
                            Ok(outcome) => (
                                node_id.clone(),
                                outcome,
                                None,
                                FlowTransitionKind::Decider,
                                digest,
                                None,
                                None,
                            ),
                            Err(flow_failure) => {
                                let failure =
                                    self.next_failure_summary(FailureCategory::UserComponent)?;
                                let final_job = self
                                    .finish_job(&execution, BatchStatus::Failed, Some(failure))
                                    .await?;
                                return Ok(FlowLaunchReport {
                                    instance,
                                    job_execution: final_job,
                                    step_executions: steps,
                                    decisions,
                                    outcome: FlowExecutionOutcome::Failed(flow_failure),
                                    listener_failures,
                                });
                            }
                        }
                    }
                }
                FlowNode::Split(split) => {
                    let run = self
                        .run_split(
                            job,
                            split,
                            &split_tasklets,
                            instance.id(),
                            execution.id(),
                            attempt,
                            parameters,
                            stop_token,
                        )
                        .await?;
                    steps.extend(run.step_executions);
                    listener_failures.extend(run.listener_failures);
                    preceding = None;
                    if run.status == BatchStatus::Unknown {
                        let final_job = self
                            .finish_job(&execution, BatchStatus::Unknown, run.failure)
                            .await?;
                        return Ok(FlowLaunchReport {
                            instance,
                            job_execution: final_job,
                            step_executions: steps,
                            decisions,
                            outcome: FlowExecutionOutcome::Unknown,
                            listener_failures,
                        });
                    }
                    if run.status == BatchStatus::Stopped {
                        let final_job = self
                            .finish_job(&execution, BatchStatus::Stopped, None)
                            .await?;
                        return Ok(FlowLaunchReport {
                            instance,
                            job_execution: final_job,
                            step_executions: steps,
                            decisions,
                            outcome: FlowExecutionOutcome::Stopped,
                            listener_failures,
                        });
                    }
                    let reused = self
                        .reusable_decision(
                            instance.id(),
                            split.join(),
                            job.plan.fingerprint(),
                            &run.input_digest,
                            FlowTransitionKind::SplitAggregate,
                        )
                        .await?;
                    (
                        split.join().clone(),
                        run.exit_status,
                        None,
                        FlowTransitionKind::SplitAggregate,
                        run.input_digest,
                        reused.map(|decision| decision.id()),
                        run.flow_failure,
                    )
                }
                FlowNode::PartitionedStep(compiled) => {
                    let historical = self.latest_step(instance.id(), &node_id).await?;
                    if let Some(history) = historical.as_ref()
                        && history.execution().metadata().status() == BatchStatus::Completed
                        && !compiled.start_controls().allow_start_if_complete()
                    {
                        let digest = step_input_digest(job.plan.fingerprint(), history);
                        let reused = self
                            .reusable_decision(
                                instance.id(),
                                &node_id,
                                job.plan.fingerprint(),
                                &digest,
                                FlowTransitionKind::StepExit,
                            )
                            .await?;
                        preceding = Some(history.clone());
                        (
                            node_id.clone(),
                            history.execution().metadata().exit_status().clone(),
                            Some(history.execution().id()),
                            FlowTransitionKind::CompletedStepReuse,
                            digest,
                            reused.map(|decision| decision.id()),
                            None,
                        )
                    } else {
                        let binding = job.partitioned_tasklets.get(&node_id).ok_or_else(|| {
                            FlowRuntimeError::Job(FlowJobError::MissingBinding {
                                node: node_id.clone(),
                            })
                        })?;
                        let run = self
                            .run_partitioned_step(
                                job,
                                compiled,
                                binding,
                                historical.as_ref(),
                                instance.id(),
                                execution.id(),
                                attempt,
                                parameters,
                                stop_token,
                            )
                            .await?;
                        listener_failures.extend(run.listener_failures);
                        steps.extend(run.worker_executions);
                        steps.push(run.parent.clone());
                        if run.status == BatchStatus::Stopped {
                            let final_job = self
                                .finish_job(&execution, BatchStatus::Stopped, None)
                                .await?;
                            return Ok(FlowLaunchReport {
                                instance,
                                job_execution: final_job,
                                step_executions: steps,
                                decisions,
                                outcome: FlowExecutionOutcome::Stopped,
                                listener_failures,
                            });
                        }
                        let state = self.latest_step(instance.id(), &node_id).await?.ok_or(
                            FlowRuntimeError::Repository(RepositoryError::FlowStateCorrupt),
                        )?;
                        let digest = step_input_digest(job.plan.fingerprint(), &state);
                        preceding = Some(state);
                        (
                            node_id.clone(),
                            run.exit_status,
                            Some(run.parent.id()),
                            FlowTransitionKind::StepExit,
                            digest,
                            None,
                            run.flow_failure,
                        )
                    }
                }
                FlowNode::Join(_) => {
                    return Err(FlowRuntimeError::Job(FlowJobError::UnsupportedManifest {
                        format: job.plan.manifest_format(),
                    }));
                }
            };

            let target = match job.plan.select_target(&transition_node, observed.code()) {
                Ok(target) => target.clone(),
                Err(FlowSelectionError::UnmappedExitOutcome { node, code }) => {
                    let failure = self.next_failure_summary(FailureCategory::InvalidDefinition)?;
                    let final_job = self
                        .finish_job(&execution, BatchStatus::Failed, Some(failure))
                        .await?;
                    return Ok(FlowLaunchReport {
                        instance,
                        job_execution: final_job,
                        step_executions: steps,
                        decisions,
                        outcome: FlowExecutionOutcome::Failed(FlowFailure::UnmappedExitOutcome {
                            node,
                            code,
                        }),
                        listener_failures,
                    });
                }
                Err(FlowSelectionError::UnknownNode { .. }) => {
                    return Err(FlowRuntimeError::Job(FlowJobError::MissingBinding {
                        node: transition_node,
                    }));
                }
            };
            let sequence = next_sequence(decisions.len())?;
            let request = FlowDecisionRequest::new(
                execution.id(),
                sequence,
                transition_node.clone(),
                source_step,
                kind,
                observed.code().clone(),
                target.clone(),
                *job.plan.fingerprint(),
                input_digest,
                reused,
                self.clock.now(),
            );
            let decision = self.append_decision(&request).await?;
            self.emit_flow_event(&FlowEvent::new(
                FlowEventKind::DecisionCommitted,
                job.name.clone(),
                instance.id(),
                execution.id(),
                attempt,
                transition_node.clone(),
                source_step,
                Some(target.clone()),
                decision.decided_at(),
            ));
            if kind == FlowTransitionKind::CompletedStepReuse {
                self.emit_flow_event(&FlowEvent::new(
                    FlowEventKind::CompletedStepReused,
                    job.name.clone(),
                    instance.id(),
                    execution.id(),
                    attempt,
                    transition_node.clone(),
                    source_step,
                    Some(target.clone()),
                    decision.decided_at(),
                ));
            }
            decisions.push(decision);

            match target {
                FlowTarget::Node(next) => node_id = next,
                FlowTarget::Terminal(terminal) => {
                    let (status, outcome) = match terminal {
                        TerminalKind::Complete => {
                            (BatchStatus::Completed, FlowExecutionOutcome::Completed)
                        }
                        TerminalKind::Stop => (BatchStatus::Stopped, FlowExecutionOutcome::Stopped),
                        // `TerminalKind::Fail`, and any terminal this build does
                        // not know: `TerminalKind` is `#[non_exhaustive]`, and an
                        // unrecognized terminal fails the job rather than
                        // completing or stopping it.
                        _ => (
                            BatchStatus::Failed,
                            FlowExecutionOutcome::Failed(
                                source_failure.unwrap_or(FlowFailure::FailTerminal),
                            ),
                        ),
                    };
                    let failure = if status == BatchStatus::Failed {
                        Some(self.next_failure_summary(FailureCategory::UserComponent)?)
                    } else {
                        None
                    };
                    let final_job = self.finish_job(&execution, status, failure).await?;
                    return Ok(FlowLaunchReport {
                        instance,
                        job_execution: final_job,
                        step_executions: steps,
                        decisions,
                        outcome,
                        listener_failures,
                    });
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_split(
        &self,
        job: &FlowJob,
        split: &crate::SplitNode,
        tasklets: &BTreeMap<NodeId, TaskletStep>,
        instance_id: JobInstanceId,
        execution_id: JobExecutionId,
        attempt: ExecutionAttempt,
        parameters: &JobParameters,
        parent_stop: &StopToken,
    ) -> Result<SplitRun, FlowRuntimeError> {
        let (split_stop_source, split_stop) = crate::StopSource::new();
        if parent_stop.is_stop_requested() {
            split_stop_source.request_stop();
        }

        let mut ordinal_base = 0_usize;
        let branches = futures_util::stream::iter(split.branches().iter().enumerate().map(
            |(index, branch)| {
                let base = ordinal_base;
                ordinal_base = ordinal_base.saturating_add(branch.steps().len());
                let split_stop = split_stop.clone();
                async move {
                    self.run_split_branch(
                        job,
                        index,
                        base,
                        branch,
                        tasklets,
                        instance_id,
                        execution_id,
                        attempt,
                        parameters,
                        &split_stop,
                    )
                    .await
                }
            },
        ))
        .buffer_unordered(usize::from(split.budget().max_parallel_branches()));
        tokio::pin!(branches);

        let mut joined = Vec::with_capacity(split.branches().len());
        let mut first_error = None;
        let mut parent_stop_observed = parent_stop.is_stop_requested();
        loop {
            tokio::select! {
                result = branches.next() => {
                    let Some(result) = result else { break; };
                    match result {
                        Ok(branch) => {
                            if split.failure_policy() == crate::LocalFailurePolicy::CancelSiblings
                                && matches!(branch.status, BatchStatus::Failed | BatchStatus::Unknown)
                            {
                                split_stop_source.request_stop();
                            }
                            joined.push(branch);
                        }
                        Err(error) => {
                            split_stop_source.request_stop();
                            if first_error.is_none() {
                                first_error = Some(error);
                            }
                        }
                    }
                }
                () = parent_stop.cancelled(), if !parent_stop_observed => {
                    parent_stop_observed = true;
                    split_stop_source.request_stop();
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        joined.sort_by_key(|branch| branch.index);
        if joined.len() != split.branches().len() {
            return Err(FlowRuntimeError::Repository(
                RepositoryError::FlowStateCorrupt,
            ));
        }

        let status = joined
            .iter()
            .map(|branch| branch.status)
            .max_by_key(|status| split_status_severity(*status))
            .ok_or(FlowRuntimeError::Repository(
                RepositoryError::FlowStateCorrupt,
            ))?;
        let selected = joined.iter().find(|branch| branch.status == status).ok_or(
            FlowRuntimeError::Repository(RepositoryError::FlowStateCorrupt),
        )?;
        let exit_status = selected.exit_status.clone();
        let failure = selected.failure;
        let flow_failure = selected.flow_failure.clone();
        let input_digest = split_input_digest(job.plan.fingerprint(), split.join(), &joined);
        let mut step_executions = Vec::new();
        let mut listener_failures = Vec::new();
        for branch in joined {
            step_executions.extend(branch.step_executions);
            listener_failures.extend(branch.listener_failures);
        }
        Ok(SplitRun {
            status,
            exit_status,
            failure,
            flow_failure,
            input_digest,
            step_executions,
            listener_failures,
        })
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn run_split_branch(
        &self,
        job: &FlowJob,
        index: usize,
        ordinal_base: usize,
        branch: &crate::SplitBranch,
        tasklets: &BTreeMap<NodeId, TaskletStep>,
        instance_id: JobInstanceId,
        execution_id: JobExecutionId,
        attempt: ExecutionAttempt,
        parameters: &JobParameters,
        stop: &StopToken,
    ) -> Result<SplitBranchRun, FlowRuntimeError> {
        let mut durable_states = Vec::with_capacity(branch.steps().len());
        let mut step_executions = Vec::new();
        let mut listener_failures = Vec::new();
        let mut status = BatchStatus::Completed;
        let mut exit_status = ExitStatus::completed();
        let mut failure = None;
        let mut flow_failure = None;

        for (offset, compiled) in branch.steps().iter().enumerate() {
            if stop.is_stop_requested() {
                status = BatchStatus::Stopped;
                exit_status = ExitStatus::stopped();
                break;
            }
            let historical = self.latest_step(instance_id, compiled.id()).await?;
            if let Some(history) = historical
                && history.execution().metadata().status() == BatchStatus::Completed
                && !compiled.start_controls().allow_start_if_complete()
            {
                exit_status = history.execution().metadata().exit_status().clone();
                durable_states.push(history);
                continue;
            }

            let tasklet = tasklets.get(compiled.id()).ok_or_else(|| {
                FlowRuntimeError::Job(FlowJobError::MissingBinding {
                    node: compiled.id().clone(),
                })
            })?;
            let created = match self
                .create_step(
                    execution_id,
                    compiled.step_name(),
                    compiled.id(),
                    compiled.start_controls().start_limit(),
                )
                .await
            {
                Ok(created) => created,
                Err(FlowRuntimeError::Repository(RepositoryError::StartLimitExceeded {
                    limit,
                    ..
                })) => {
                    self.emit_flow_event(&FlowEvent::new(
                        FlowEventKind::StartLimitExceeded,
                        job.name.clone(),
                        instance_id,
                        execution_id,
                        attempt,
                        compiled.id().clone(),
                        None,
                        None,
                        self.clock.now(),
                    ));
                    status = BatchStatus::Failed;
                    exit_status = ExitStatus::failed();
                    failure = Some(self.next_failure_summary(FailureCategory::IllegalTransition)?);
                    flow_failure = Some(FlowFailure::StartLimitExceeded {
                        node: compiled.id().clone(),
                        limit,
                    });
                    break;
                }
                Err(error) => return Err(error),
            };
            let correlation = correlation(
                &job.name,
                instance_id,
                execution_id,
                attempt,
                compiled.step_name(),
                created.id(),
                ordinal_base.saturating_add(offset),
            )?;
            let run = self
                .run_step(
                    compiled.id(),
                    tasklet,
                    created,
                    parameters,
                    stop,
                    &correlation,
                )
                .await?;
            status = status_for_tasklet(run.outcome);
            exit_status = run.exit_status;
            failure = run.failure;
            flow_failure = run.flow_failure;
            listener_failures.extend(run.listener_failures);
            step_executions.push(run.execution);
            let state = self.latest_step(instance_id, compiled.id()).await?.ok_or(
                FlowRuntimeError::Repository(RepositoryError::FlowStateCorrupt),
            )?;
            durable_states.push(state);
            if status != BatchStatus::Completed {
                break;
            }
        }

        Ok(SplitBranchRun {
            index,
            status,
            exit_status,
            failure,
            flow_failure,
            states: durable_states,
            step_executions,
            listener_failures,
        })
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn run_partitioned_step(
        &self,
        job: &FlowJob,
        compiled: &crate::PartitionedStepNode,
        binding: &PartitionedTaskletBinding,
        historical: Option<&FlowStepState>,
        instance_id: JobInstanceId,
        execution_id: JobExecutionId,
        attempt: ExecutionAttempt,
        parameters: &JobParameters,
        parent_stop: &StopToken,
    ) -> Result<PartitionRun, FlowRuntimeError> {
        let created = self
            .create_step(
                execution_id,
                compiled.step_name(),
                compiled.id(),
                compiled.start_controls().start_limit(),
            )
            .await?;
        let parent = self.start_step(&created).await?;
        let plan_result: Result<Vec<StepPartition>, FlowRuntimeError> =
            if let Some(history) = historical {
                let source = self.partition_plan(history.execution().id()).await?;
                if source.is_empty() {
                    self.create_partition_plan(
                        binding,
                        compiled,
                        instance_id,
                        parent.id(),
                        job.plan.fingerprint(),
                    )
                    .await
                } else {
                    self.restart_partition_plan(history.execution().id(), parent.id())
                        .await
                }
            } else {
                self.create_partition_plan(
                    binding,
                    compiled,
                    instance_id,
                    parent.id(),
                    job.plan.fingerprint(),
                )
                .await
            };
        let plan = match plan_result {
            Ok(plan) => plan,
            Err(FlowRuntimeError::PartitionerRejected { panicked }) => {
                let failure = self.next_failure_summary(FailureCategory::UserComponent)?;
                let parent = self
                    .finish_step(
                        &parent,
                        TaskletExecutionOutcome::Failed(TaskletFailure::Error),
                        &ExitStatus::failed(),
                        Some(failure),
                        false,
                    )
                    .await?;
                self.emit_flow_event(&FlowEvent::new(
                    FlowEventKind::StepResultCommitted,
                    job.name.clone(),
                    instance_id,
                    execution_id,
                    attempt,
                    compiled.id().clone(),
                    Some(parent.id()),
                    None,
                    self.clock.now(),
                ));
                return Ok(PartitionRun {
                    status: BatchStatus::Failed,
                    exit_status: ExitStatus::failed(),
                    parent,
                    flow_failure: Some(if panicked {
                        FlowFailure::PartitionerPanic
                    } else {
                        FlowFailure::PartitionerError
                    }),
                    worker_executions: Vec::new(),
                    listener_failures: Vec::new(),
                });
            }
            Err(error) => return Err(error),
        };

        if plan
            .iter()
            .any(|partition| partition.status() == BatchStatus::Unknown)
        {
            return Err(FlowRuntimeError::UnresolvedPartitionOutcome {
                step_execution_id: parent.id(),
            });
        }

        let pending = plan
            .into_iter()
            .filter(|partition| partition.status() != BatchStatus::Completed)
            .collect::<Vec<_>>();
        let (partition_stop_source, partition_stop) = crate::StopSource::new();
        if parent_stop.is_stop_requested() {
            partition_stop_source.request_stop();
        }
        let workers = futures_util::stream::iter(pending.into_iter().map(|partition| {
            let partition_stop = partition_stop.clone();
            async move {
                self.run_partition_worker(
                    job,
                    compiled,
                    binding,
                    partition,
                    instance_id,
                    execution_id,
                    attempt,
                    parameters,
                    &partition_stop,
                )
                .await
            }
        }))
        .buffer_unordered(usize::from(compiled.budget().max_partition_workers()));
        tokio::pin!(workers);

        let mut joined = Vec::new();
        let mut first_error = None;
        let mut parent_stop_observed = parent_stop.is_stop_requested();
        loop {
            tokio::select! {
                result = workers.next() => {
                    let Some(result) = result else { break; };
                    match result {
                        Ok(worker) => {
                            if compiled.failure_policy() == crate::LocalFailurePolicy::CancelSiblings
                                && matches!(worker.partition.status(), BatchStatus::Failed | BatchStatus::Unknown)
                            {
                                partition_stop_source.request_stop();
                            }
                            joined.push(worker);
                        }
                        Err(error) => {
                            partition_stop_source.request_stop();
                            if first_error.is_none() {
                                first_error = Some(error);
                            }
                        }
                    }
                }
                () = parent_stop.cancelled(), if !parent_stop_observed => {
                    parent_stop_observed = true;
                    partition_stop_source.request_stop();
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }

        let durable = self.partition_plan(parent.id()).await?;
        if durable
            .iter()
            .any(|partition| partition.status() == BatchStatus::Unknown)
        {
            return Err(FlowRuntimeError::UnresolvedPartitionOutcome {
                step_execution_id: parent.id(),
            });
        }
        let parent = self
            .aggregate_partition_parent(parent.id(), parent.version())
            .await?;
        self.emit_flow_event(&FlowEvent::new(
            FlowEventKind::StepResultCommitted,
            job.name.clone(),
            instance_id,
            execution_id,
            attempt,
            compiled.id().clone(),
            Some(parent.id()),
            None,
            self.clock.now(),
        ));

        joined.sort_by(|left, right| left.partition.key().cmp(right.partition.key()));
        let selected = joined
            .iter()
            .find(|worker| worker.partition.status() == parent.metadata().status());
        let flow_failure = selected.and_then(|worker| worker.flow_failure.clone());
        let mut worker_executions = Vec::with_capacity(joined.len());
        let mut listener_failures = Vec::new();
        for worker in joined {
            worker_executions.push(worker.execution);
            listener_failures.extend(worker.listener_failures);
        }
        Ok(PartitionRun {
            status: parent.metadata().status(),
            exit_status: parent.metadata().exit_status().clone(),
            parent,
            flow_failure,
            worker_executions,
            listener_failures,
        })
    }

    async fn create_partition_plan(
        &self,
        binding: &PartitionedTaskletBinding,
        compiled: &crate::PartitionedStepNode,
        instance_id: JobInstanceId,
        parent_id: StepExecutionId,
        fingerprint: &[u8; 32],
    ) -> Result<Vec<StepPartition>, FlowRuntimeError> {
        let request = PartitionPlanRequest {
            plan_fingerprint: fingerprint,
            job_instance_id: instance_id,
            node_id: compiled.id(),
            partition_count: compiled.partition_count(),
        };
        let entries = match catch_unwind(AssertUnwindSafe(|| binding.partitioner.create(request))) {
            Ok(Ok(entries)) => entries,
            Ok(Err(_)) => return Err(FlowRuntimeError::PartitionerRejected { panicked: false }),
            Err(_) => return Err(FlowRuntimeError::PartitionerRejected { panicked: true }),
        };
        if entries.len() != usize::from(compiled.partition_count().get()) {
            return Err(FlowRuntimeError::PartitionerRejected { panicked: false });
        }
        let mut unit = self.repository.begin().await?;
        let created = unit.create_step_partition_plan(parent_id, &entries).await?;
        match unit.commit().await {
            Ok(()) => Ok(created),
            Err(RepositoryError::CommitOutcomeUnknown) => {
                let durable = self.partition_plan(parent_id).await?;
                if durable == created {
                    Ok(durable)
                } else {
                    Err(RepositoryError::CommitOutcomeUnknown.into())
                }
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn restart_partition_plan(
        &self,
        source_parent_id: StepExecutionId,
        target_parent_id: StepExecutionId,
    ) -> Result<Vec<StepPartition>, FlowRuntimeError> {
        let mut unit = self.repository.begin().await?;
        let copied = unit
            .restart_step_partition_plan(source_parent_id, target_parent_id)
            .await?;
        match unit.commit().await {
            Ok(()) => Ok(copied),
            Err(RepositoryError::CommitOutcomeUnknown) => {
                let durable = self.partition_plan(target_parent_id).await?;
                if durable == copied {
                    Ok(durable)
                } else {
                    Err(RepositoryError::CommitOutcomeUnknown.into())
                }
            }
            Err(error) => Err(error.into()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_partition_worker(
        &self,
        job: &FlowJob,
        compiled: &crate::PartitionedStepNode,
        binding: &PartitionedTaskletBinding,
        partition: StepPartition,
        instance_id: JobInstanceId,
        execution_id: JobExecutionId,
        attempt: ExecutionAttempt,
        parameters: &JobParameters,
        stop: &StopToken,
    ) -> Result<PartitionWorkerRun, FlowRuntimeError> {
        let (worker, assigned) = self
            .create_and_assign_partition_worker(execution_id, compiled, &partition)
            .await?;
        let correlation = correlation(
            &job.name,
            instance_id,
            execution_id,
            attempt,
            compiled.worker().step_name(),
            worker.id(),
            usize::try_from(partition.ordinal()).unwrap_or(usize::MAX),
        )?;

        let run = if stop.is_stop_requested() {
            self.finish_uninvoked_partition_worker(
                worker,
                TaskletExecutionOutcome::Stopped(StopTiming::BeforeStart),
                ExitStatus::stopped(),
                None,
            )
            .await?
        } else {
            let input = PartitionWorkerInput::from_partition(&assigned);
            match catch_unwind(AssertUnwindSafe(|| binding.worker.create(input))) {
                Ok(tasklet_step) if tasklet_step.name() == binding.worker.step_name() => {
                    self.run_step(
                        compiled.worker().id(),
                        &tasklet_step,
                        worker,
                        parameters,
                        stop,
                        &correlation,
                    )
                    .await?
                }
                Ok(_) | Err(_) => {
                    let failure = self.next_failure_summary(FailureCategory::UserComponent)?;
                    let mut run = self
                        .finish_uninvoked_partition_worker(
                            worker,
                            TaskletExecutionOutcome::Failed(TaskletFailure::Panic),
                            ExitStatus::failed(),
                            Some(failure),
                        )
                        .await?;
                    run.flow_failure = Some(FlowFailure::PartitionFactoryPanic);
                    run
                }
            }
        };
        let completed = self
            .publish_partition_result(&assigned, run.execution.id())
            .await?;
        Ok(PartitionWorkerRun {
            partition: completed,
            execution: run.execution,
            flow_failure: run.flow_failure,
            listener_failures: run.listener_failures,
        })
    }

    async fn create_and_assign_partition_worker(
        &self,
        execution_id: JobExecutionId,
        compiled: &crate::PartitionedStepNode,
        partition: &StepPartition,
    ) -> Result<(StepExecution, StepPartition), FlowRuntimeError> {
        let (worker_name, worker_node_id) = partition_worker_identity(compiled, partition)?;
        let mut unit = self.repository.begin().await?;
        let worker = unit
            .create_flow_step_execution(
                execution_id,
                &worker_name,
                &worker_node_id,
                compiled.worker().start_controls().start_limit(),
            )
            .await?;
        let assigned = unit
            .assign_step_partition(partition.id(), partition.version(), worker.id())
            .await?;
        unit.commit().await?;
        Ok((worker, assigned))
    }

    async fn finish_uninvoked_partition_worker(
        &self,
        worker: StepExecution,
        outcome: TaskletExecutionOutcome,
        exit_status: ExitStatus,
        failure: Option<FailureSummary>,
    ) -> Result<StepRun, FlowRuntimeError> {
        let started = self.start_step(&worker).await?;
        let execution = self
            .finish_step(&started, outcome, &exit_status, failure, false)
            .await?;
        Ok(StepRun {
            execution,
            outcome,
            exit_status,
            failure,
            flow_failure: match outcome {
                TaskletExecutionOutcome::Failed(value) => Some(FlowFailure::Tasklet(value)),
                _ => None,
            },
            listener_failures: Vec::new(),
        })
    }

    async fn publish_partition_result(
        &self,
        partition: &StepPartition,
        worker_id: StepExecutionId,
    ) -> Result<StepPartition, FlowRuntimeError> {
        let mut unit = self.repository.begin().await?;
        let completed = unit
            .complete_step_partition(partition.id(), partition.version(), worker_id)
            .await?;
        match unit.commit().await {
            Ok(()) => Ok(completed),
            Err(RepositoryError::CommitOutcomeUnknown) => {
                let durable = self.partition_plan(partition.step_execution_id()).await?;
                let current = durable
                    .into_iter()
                    .find(|candidate| candidate.id() == partition.id())
                    .ok_or(RepositoryError::PartitionStateCorrupt)?;
                if current == completed {
                    Ok(current)
                } else {
                    Err(RepositoryError::CommitOutcomeUnknown.into())
                }
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn partition_plan(
        &self,
        parent_id: StepExecutionId,
    ) -> Result<Vec<StepPartition>, FlowRuntimeError> {
        let mut unit = self.repository.begin().await?;
        let plan = unit.step_partition_plan(parent_id).await?;
        unit.rollback().await?;
        Ok(plan)
    }

    async fn aggregate_partition_parent(
        &self,
        parent_id: StepExecutionId,
        expected_version: crate::ExecutionVersion,
    ) -> Result<StepExecution, FlowRuntimeError> {
        let mut unit = self.repository.begin().await?;
        let aggregated = unit
            .aggregate_step_partitions(parent_id, expected_version, self.clock.now())
            .await?;
        match unit.commit().await {
            Ok(()) => Ok(aggregated),
            Err(RepositoryError::CommitOutcomeUnknown) => {
                let mut recovery = self.repository.begin().await?;
                let inspected = recovery
                    .aggregate_step_partitions(parent_id, expected_version, self.clock.now())
                    .await?;
                recovery.commit().await?;
                Ok(inspected)
            }
            Err(error) => Err(error.into()),
        }
    }

    fn ensure_accepting(&self) -> Result<(), FlowRuntimeError> {
        self.shutdown_signal.map_or(Ok(()), |signal| {
            signal
                .ensure_accepting()
                .map_err(|_| FlowRuntimeError::ShuttingDown)
        })
    }

    fn validate_repository_capacity(
        &self,
        plan: &CompiledExecutionPlan,
    ) -> Result<(), FlowRuntimeError> {
        let configured = self.repository.connection_capacity();
        let required = plan
            .nodes()
            .filter_map(|(_, node)| match node {
                FlowNode::Split(split) => Some(split.budget().repository_pool_size()),
                FlowNode::PartitionedStep(partitioned) => {
                    Some(partitioned.budget().repository_pool_size())
                }
                _ => None,
            })
            .max()
            .unwrap_or(1);
        if configured < required {
            return Err(FlowRuntimeError::InsufficientPoolCapacity {
                required,
                configured,
            });
        }
        Ok(())
    }

    fn observe_process_shutdown(&self, stop: &StopToken) {
        if self
            .shutdown_signal
            .is_some_and(crate::ShutdownSignal::is_shutdown_requested)
        {
            stop.request_stop();
        }
    }

    async fn poll_execution_control(
        &self,
        execution_id: JobExecutionId,
        stop: &StopToken,
    ) -> Result<(), FlowRuntimeError> {
        let Some((owner, _)) = self.execution_control else {
            return Ok(());
        };
        let mut unit = self.repository.begin().await?;
        let control = unit
            .observe_execution_control(execution_id, &owner, self.clock.now())
            .await?;
        unit.commit().await?;
        if !control.owner_matches() {
            return Err(RepositoryError::ExecutionOwned { id: execution_id }.into());
        }
        if control.stop_requested() {
            stop.request_stop();
        }
        Ok(())
    }

    async fn invoke_with_execution_control(
        &self,
        execution_id: JobExecutionId,
        tasklet: &dyn crate::Tasklet,
        context: TaskletContext<'_>,
        stop: &StopToken,
    ) -> Result<Result<TaskletOutcome, TaskletFailure>, FlowRuntimeError> {
        if self.execution_control.is_none() && self.shutdown_signal.is_none() {
            return Ok(invoke_tasklet(tasklet, context).await);
        }
        let invocation = invoke_tasklet(tasklet, context);
        tokio::pin!(invocation);
        let mut shutdown_observed = false;
        loop {
            tokio::select! {
                result = &mut invocation => return Ok(result),
                () = async {
                    match self.execution_control {
                        Some((_, interval)) => tokio::time::sleep(interval.get()).await,
                        None => std::future::pending().await,
                    }
                } => {
                    self.poll_execution_control(execution_id, stop).await?;
                }
                () = async {
                    match self.shutdown_signal {
                        Some(signal) => signal.cancelled().await,
                        None => std::future::pending().await,
                    }
                }, if !shutdown_observed => {
                    shutdown_observed = true;
                    stop.request_stop();
                }
            }
        }
    }

    async fn create_job_execution(
        &self,
        key: &JobInstanceKey,
        definition: &crate::DefinitionIdentity,
    ) -> Result<(JobInstance, JobExecution, ExecutionAttempt), FlowRuntimeError> {
        let mut unit = self.repository.begin().await?;
        let instance = unit
            .select_or_create_job_instance(key)
            .await?
            .instance()
            .clone();
        let execution = unit
            .create_job_execution_with_definition(instance.id(), definition)
            .await?;
        let execution = if let Some((owner, _)) = self.execution_control {
            unit.claim_execution_owner(
                execution.id(),
                execution.version(),
                &owner,
                self.clock.now(),
            )
            .await?
        } else {
            execution
        };
        let attempt = NonZeroU64::new(
            u64::try_from(unit.job_executions(instance.id()).await?.len())
                .map_err(|_| FlowRuntimeError::CountExhausted)?,
        )
        .map(ExecutionAttempt::new)
        .ok_or(FlowRuntimeError::CountExhausted)?;
        unit.commit().await?;
        Ok((instance, execution, attempt))
    }

    async fn start_job(&self, execution: &JobExecution) -> Result<JobExecution, FlowRuntimeError> {
        let mut unit = self.repository.begin().await?;
        let started = unit
            .transition_job_execution(
                execution.id(),
                execution.version(),
                LifecycleTransition::new(BatchStatus::Started, self.clock.now()),
            )
            .await?;
        unit.commit().await?;
        Ok(started)
    }

    async fn finish_job(
        &self,
        execution: &JobExecution,
        status: BatchStatus,
        failure: Option<FailureSummary>,
    ) -> Result<JobExecution, FlowRuntimeError> {
        let exit = exit_for_status(status);
        let mut unit = self.repository.begin().await?;
        let execution = if self.execution_control.is_some() {
            unit.get_job_execution(execution.id())
                .await?
                .ok_or(RepositoryError::JobExecutionNotFound { id: execution.id() })?
        } else {
            execution.clone()
        };
        let enriched = unit
            .enrich_job_exit_status(execution.id(), execution.version(), &exit)
            .await?;
        let transition = terminal_transition(status, self.clock.now(), failure)?;
        let finished = unit
            .transition_job_execution(enriched.id(), enriched.version(), transition)
            .await?;
        unit.commit().await?;
        Ok(finished)
    }

    async fn create_step(
        &self,
        job_execution_id: JobExecutionId,
        step_name: &StepName,
        node_id: &NodeId,
        limit: StartLimit,
    ) -> Result<StepExecution, FlowRuntimeError> {
        let mut unit = self.repository.begin().await?;
        let step = unit
            .create_flow_step_execution(job_execution_id, step_name, node_id, limit)
            .await?;
        unit.commit().await?;
        Ok(step)
    }

    async fn latest_step(
        &self,
        instance_id: JobInstanceId,
        node_id: &NodeId,
    ) -> Result<Option<FlowStepState>, FlowRuntimeError> {
        let mut unit = self.repository.begin().await?;
        let state = unit.latest_flow_step(instance_id, node_id).await?;
        unit.rollback().await?;
        Ok(state)
    }

    async fn reusable_decision(
        &self,
        instance_id: JobInstanceId,
        node_id: &NodeId,
        fingerprint: &[u8; 32],
        digest: &[u8; 32],
        kind: FlowTransitionKind,
    ) -> Result<Option<FlowDecision>, FlowRuntimeError> {
        let mut unit = self.repository.begin().await?;
        let decision = unit
            .find_reusable_flow_decision(instance_id, node_id, fingerprint, digest, kind)
            .await?;
        unit.rollback().await?;
        Ok(decision)
    }

    async fn append_decision(
        &self,
        request: &FlowDecisionRequest,
    ) -> Result<FlowDecision, FlowRuntimeError> {
        let mut unit = self.repository.begin().await?;
        let decision = unit.append_flow_decision(request).await?;
        unit.commit().await?;
        Ok(decision)
    }

    #[allow(clippy::too_many_lines)]
    async fn run_step(
        &self,
        node_id: &NodeId,
        step: &TaskletStep,
        created: StepExecution,
        parameters: &JobParameters,
        stop_token: &StopToken,
        correlation: &ExecutionCorrelation,
    ) -> Result<StepRun, FlowRuntimeError> {
        let context = ListenerContext::new(correlation, parameters, stop_token);
        for (index, listener) in step.listeners().iter().enumerate() {
            if let Err(kind) = invoke_before_step(listener.as_ref(), context).await {
                let summary = self.next_failure_summary(FailureCategory::UserComponent)?;
                let failure = ListenerFailure::new(ListenerPhase::BeforeStep, index, kind, summary);
                let outcome = if kind == ListenerFailureKind::Panic {
                    TaskletExecutionOutcome::Failed(TaskletFailure::ListenerPanic)
                } else {
                    TaskletExecutionOutcome::Failed(TaskletFailure::ListenerError)
                };
                let execution = self
                    .finish_step(
                        &created,
                        outcome,
                        &ExitStatus::failed(),
                        Some(summary),
                        false,
                    )
                    .await?;
                self.emit_flow_event(&FlowEvent::new(
                    FlowEventKind::StepResultCommitted,
                    correlation.job_name().clone(),
                    correlation.job_instance_id(),
                    correlation.job_execution_id(),
                    correlation.job_attempt(),
                    node_id.clone(),
                    Some(execution.id()),
                    None,
                    self.clock.now(),
                ));
                return Ok(StepRun {
                    execution,
                    outcome,
                    exit_status: ExitStatus::failed(),
                    failure: Some(summary),
                    flow_failure: Some(FlowFailure::Listener(match outcome {
                        TaskletExecutionOutcome::Failed(value) => value,
                        _ => TaskletFailure::ListenerError,
                    })),
                    listener_failures: vec![failure],
                });
            }
        }

        let started = self.start_step(&created).await?;
        let terminal_rollback = AtomicBool::new(false);
        let tasklet_context = TaskletContext::new_for_flow(
            parameters,
            started.job_execution_id(),
            started.id(),
            stop_token,
            correlation,
            &terminal_rollback,
        );
        let invoked = self
            .invoke_with_execution_control(
                correlation.job_execution_id(),
                step.tasklet(),
                tasklet_context,
                stop_token,
            )
            .await?;
        let (mut outcome, mut exit, tasklet_failure) = match invoked {
            Ok(TaskletOutcome::Completed) if !stop_token.is_stop_requested() => (
                TaskletExecutionOutcome::Completed,
                ExitStatus::completed(),
                None,
            ),
            Ok(TaskletOutcome::CompletedWith(exit)) if !stop_token.is_stop_requested() => {
                (TaskletExecutionOutcome::Completed, exit, None)
            }
            Ok(
                TaskletOutcome::Completed
                | TaskletOutcome::CompletedWith(_)
                | TaskletOutcome::Stopped,
            ) => (
                TaskletExecutionOutcome::Stopped(StopTiming::DuringExecution),
                ExitStatus::stopped(),
                None,
            ),
            Ok(TaskletOutcome::StoppedAfterBlockingWork) => (
                TaskletExecutionOutcome::Stopped(StopTiming::AfterBlockingWork),
                ExitStatus::stopped(),
                None,
            ),
            Ok(TaskletOutcome::CommitOutcomeUnknown) => (
                TaskletExecutionOutcome::Unknown,
                ExitStatus::unknown(),
                None,
            ),
            Err(failure) => (
                TaskletExecutionOutcome::Failed(failure),
                ExitStatus::failed(),
                Some(failure),
            ),
        };
        let tasklet_summary = tasklet_failure
            .map(|_| self.next_failure_summary(FailureCategory::UserComponent))
            .transpose()?;
        let mut failures = Vec::new();
        for (index, listener) in step.listeners().iter().enumerate().rev() {
            if let Err(kind) = invoke_after_step(listener.as_ref(), context, outcome).await {
                let summary = self.next_failure_summary(FailureCategory::UserComponent)?;
                failures.push(ListenerFailure::new(
                    ListenerPhase::AfterStep,
                    index,
                    kind,
                    summary,
                ));
            }
        }
        if let Some(first) = failures.first() {
            outcome = if first.kind() == ListenerFailureKind::Panic {
                TaskletExecutionOutcome::Failed(TaskletFailure::ListenerPanic)
            } else {
                TaskletExecutionOutcome::Failed(TaskletFailure::ListenerError)
            };
            exit = ExitStatus::failed();
        }
        let failure = failures
            .first()
            .map(|failure| failure.summary())
            .or(tasklet_summary);
        let durable = self.reload_step(started.id()).await?;
        let execution = self
            .finish_step(
                &durable,
                outcome,
                &exit,
                failure,
                terminal_rollback.load(Ordering::Acquire),
            )
            .await?;
        self.emit_flow_event(&FlowEvent::new(
            FlowEventKind::StepResultCommitted,
            correlation.job_name().clone(),
            correlation.job_instance_id(),
            correlation.job_execution_id(),
            correlation.job_attempt(),
            node_id.clone(),
            Some(execution.id()),
            None,
            self.clock.now(),
        ));
        Ok(StepRun {
            execution,
            outcome,
            exit_status: exit,
            failure,
            flow_failure: match outcome {
                TaskletExecutionOutcome::Failed(value) => Some(
                    if matches!(
                        value,
                        TaskletFailure::ListenerError | TaskletFailure::ListenerPanic
                    ) {
                        FlowFailure::Listener(value)
                    } else {
                        FlowFailure::Tasklet(value)
                    },
                ),
                _ => None,
            },
            listener_failures: failures,
        })
    }

    async fn start_step(&self, step: &StepExecution) -> Result<StepExecution, FlowRuntimeError> {
        let mut unit = self.repository.begin().await?;
        let started = unit
            .transition_step_execution(
                step.id(),
                step.version(),
                LifecycleTransition::new(BatchStatus::Started, self.clock.now()),
            )
            .await?;
        unit.commit().await?;
        Ok(started)
    }

    async fn reload_step(&self, id: StepExecutionId) -> Result<StepExecution, FlowRuntimeError> {
        let mut unit = self.repository.begin().await?;
        let step = unit
            .get_step_execution(id)
            .await?
            .ok_or(RepositoryError::StepExecutionNotFound { id })?;
        unit.rollback().await?;
        Ok(step)
    }

    async fn finish_step(
        &self,
        step: &StepExecution,
        outcome: TaskletExecutionOutcome,
        exit: &ExitStatus,
        failure: Option<FailureSummary>,
        terminal_rollback: bool,
    ) -> Result<StepExecution, FlowRuntimeError> {
        let status = status_for_tasklet(outcome);
        let mut unit = self.repository.begin().await?;
        let enriched = unit
            .enrich_step_exit_status(step.id(), step.version(), exit)
            .await?;
        let mut transition = terminal_transition(status, self.clock.now(), failure)?;
        if terminal_rollback {
            transition = transition.with_terminal_rollback();
        }
        let finished = unit
            .transition_step_execution(enriched.id(), enriched.version(), transition)
            .await?;
        match unit.commit().await {
            Ok(()) => Ok(finished),
            Err(RepositoryError::CommitOutcomeUnknown) => {
                let durable = self.reload_step(finished.id()).await?;
                if durable == finished {
                    Ok(durable)
                } else {
                    Err(RepositoryError::CommitOutcomeUnknown.into())
                }
            }
            Err(error) => Err(error.into()),
        }
    }

    fn next_failure_summary(
        &self,
        category: FailureCategory,
    ) -> Result<FailureSummary, FlowRuntimeError> {
        Ok(FailureSummary::new(
            category,
            self.ids
                .next_failure_id()
                .map_err(RepositoryError::Identifier)?,
        ))
    }

    fn emit_flow_event(&self, event: &FlowEvent) {
        let Some(sink) = self.event_sink else {
            return;
        };
        let _ = catch_unwind(AssertUnwindSafe(|| sink.emit(event)));
    }
}

fn partition_worker_identity(
    compiled: &crate::PartitionedStepNode,
    partition: &StepPartition,
) -> Result<(StepName, NodeId), FlowRuntimeError> {
    let mut digest = Sha256::new();
    digest.update(b"oxide-batch.local-partition-worker.v1\0");
    digest.update(compiled.id().as_str().as_bytes());
    digest.update([0]);
    digest.update(partition.key().as_str().as_bytes());
    let token = format!(
        "__ob_partition_worker_{}",
        crate::service::hex_digest(&digest.finalize())
    );
    let step_name =
        StepName::new(token.clone()).map_err(|_| RepositoryError::PartitionStateCorrupt)?;
    let node_id = NodeId::new(token).map_err(|_| RepositoryError::PartitionStateCorrupt)?;
    Ok((step_name, node_id))
}

struct StepRun {
    execution: StepExecution,
    outcome: TaskletExecutionOutcome,
    exit_status: ExitStatus,
    failure: Option<FailureSummary>,
    flow_failure: Option<FlowFailure>,
    listener_failures: Vec<ListenerFailure>,
}

struct SplitBranchRun {
    index: usize,
    status: BatchStatus,
    exit_status: ExitStatus,
    failure: Option<FailureSummary>,
    flow_failure: Option<FlowFailure>,
    states: Vec<FlowStepState>,
    step_executions: Vec<StepExecution>,
    listener_failures: Vec<ListenerFailure>,
}

struct SplitRun {
    status: BatchStatus,
    exit_status: ExitStatus,
    failure: Option<FailureSummary>,
    flow_failure: Option<FlowFailure>,
    input_digest: [u8; 32],
    step_executions: Vec<StepExecution>,
    listener_failures: Vec<ListenerFailure>,
}

struct PartitionWorkerRun {
    partition: StepPartition,
    execution: StepExecution,
    flow_failure: Option<FlowFailure>,
    listener_failures: Vec<ListenerFailure>,
}

struct PartitionRun {
    status: BatchStatus,
    exit_status: ExitStatus,
    parent: StepExecution,
    flow_failure: Option<FlowFailure>,
    worker_executions: Vec<StepExecution>,
    listener_failures: Vec<ListenerFailure>,
}

const fn split_status_severity(status: BatchStatus) -> u8 {
    match status {
        BatchStatus::Completed => 0,
        BatchStatus::Stopped => 1,
        BatchStatus::Failed => 2,
        BatchStatus::Unknown => 3,
        // The non-terminal and abandoned statuses, and any status this build
        // does not know: `BatchStatus` is `#[non_exhaustive]`, and an
        // unrecognized status takes the highest severity so it dominates the
        // aggregate rather than being mistaken for a completed branch.
        _ => 4,
    }
}

fn split_input_digest(
    fingerprint: &[u8; 32],
    join: &NodeId,
    branches: &[SplitBranchRun],
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"oxide-batch.split-aggregate-input.v1\0");
    hash.update(fingerprint);
    hash_field(&mut hash, join.as_str().as_bytes());
    for branch in branches {
        hash.update(
            u64::try_from(branch.index)
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        hash_field(&mut hash, branch.status.to_string().as_bytes());
        hash_field(&mut hash, branch.exit_status.code().as_str().as_bytes());
        for state in &branch.states {
            hash.update(step_input_digest(fingerprint, state));
        }
    }
    hash.finalize().into()
}

fn next_sequence(length: usize) -> Result<FlowDecisionSequence, FlowRuntimeError> {
    let next = u64::try_from(length)
        .ok()
        .and_then(|value| value.checked_add(1))
        .ok_or(FlowRuntimeError::DecisionSequenceExhausted)?;
    FlowDecisionSequence::new(next)
}

fn correlation(
    job_name: &JobName,
    instance_id: JobInstanceId,
    job_execution_id: JobExecutionId,
    attempt: ExecutionAttempt,
    step_name: &StepName,
    step_execution_id: StepExecutionId,
    completed_steps: usize,
) -> Result<ExecutionCorrelation, FlowRuntimeError> {
    let step_attempt = u64::try_from(completed_steps)
        .ok()
        .and_then(|value| value.checked_add(1))
        .and_then(NonZeroU64::new)
        .map(ExecutionAttempt::new)
        .ok_or(FlowRuntimeError::CountExhausted)?;
    Ok(ExecutionCorrelation::new(
        job_name.clone(),
        instance_id,
        job_execution_id,
        attempt,
        step_name.clone(),
        step_execution_id,
        step_attempt,
    ))
}

fn status_for_tasklet(outcome: TaskletExecutionOutcome) -> BatchStatus {
    match outcome {
        TaskletExecutionOutcome::Completed => BatchStatus::Completed,
        TaskletExecutionOutcome::Stopped(_) => BatchStatus::Stopped,
        TaskletExecutionOutcome::Failed(_) => BatchStatus::Failed,
        TaskletExecutionOutcome::Unknown => BatchStatus::Unknown,
    }
}

fn exit_for_status(status: BatchStatus) -> ExitStatus {
    match status {
        BatchStatus::Completed => ExitStatus::completed(),
        BatchStatus::Stopped => ExitStatus::stopped(),
        BatchStatus::Unknown => ExitStatus::unknown(),
        _ => ExitStatus::failed(),
    }
}

fn terminal_transition(
    status: BatchStatus,
    at: SystemTime,
    failure: Option<FailureSummary>,
) -> Result<LifecycleTransition, FlowRuntimeError> {
    if status == BatchStatus::Failed {
        let failure = failure.ok_or(FlowRuntimeError::CountExhausted)?;
        Ok(LifecycleTransition::failed(at, failure))
    } else {
        Ok(LifecycleTransition::new(status, at))
    }
}

fn step_input_digest(fingerprint: &[u8; 32], state: &FlowStepState) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"oxide-batch.flow-step-input.v1\0");
    hash.update(fingerprint);
    hash_field(&mut hash, state.node_id().as_str().as_bytes());
    hash.update(state.execution().id().get().to_be_bytes());
    hash.update(state.execution().version().get().to_be_bytes());
    hash_field(
        &mut hash,
        state.execution().metadata().status().to_string().as_bytes(),
    );
    hash_field(
        &mut hash,
        state
            .execution()
            .metadata()
            .exit_status()
            .code()
            .as_str()
            .as_bytes(),
    );
    hash_counts(&mut hash, state.execution().metadata().counts());
    if let Some(context) = state.context()
        && let Ok(bytes) = context.to_json()
    {
        hash.update(Sha256::digest(bytes));
    }
    hash.finalize().into()
}

fn decision_input_digest(
    fingerprint: &[u8; 32],
    node_id: &NodeId,
    revision: &str,
    input_version: u32,
    instance_id: JobInstanceId,
    parameters: &JobParameters,
    preceding: Option<&FlowStepState>,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"oxide-batch.decider-input.v1\0");
    hash.update(fingerprint);
    hash_field(&mut hash, node_id.as_str().as_bytes());
    hash_field(&mut hash, revision.as_bytes());
    hash.update(input_version.to_be_bytes());
    hash.update(instance_id.get().to_be_bytes());
    // JobParameters intentionally owns this sensitive canonical projection;
    // only its one-way digest is retained in the flow record.
    hash.update(parameters.flow_input_digest());
    if let Some(state) = preceding {
        hash.update(step_input_digest(fingerprint, state));
    }
    hash.finalize().into()
}

fn hash_field(hash: &mut Sha256, value: &[u8]) {
    let length = u64::try_from(value.len()).unwrap_or(u64::MAX);
    hash.update(length.to_be_bytes());
    hash.update(value);
}

fn hash_counts(hash: &mut Sha256, counts: ExecutionCounts) {
    for count in [
        counts.read(),
        counts.processed(),
        counts.written(),
        counts.filtered(),
        counts.committed(),
        counts.rolled_back(),
    ] {
        hash.update(count.to_be_bytes());
    }
}
