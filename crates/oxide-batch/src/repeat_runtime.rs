//! Runtime contracts for Gate-C repeat policy and interceptor composition.

use std::error::Error;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use futures_util::FutureExt;

use crate::{
    BoxFuture, ComponentRevision, ExecutionContext, ExecutionCorrelation, JobExecutionId,
    JobParameters, MAX_REPEAT_INTERCEPTORS, MAX_REPEAT_NESTING_DEPTH, RepeatDecision,
    RepeatDefinition, RepeatId, RepeatInterceptorId, RepeatInterceptorKind, RepeatOrdinal,
    RepeatPolicyConfiguration, RepeatPolicyKind, StopToken, TaskletContext,
    TaskletExecutionOutcome, TaskletFailure,
};

/// Maximum retained secondary callback failures for one nested repeat invocation.
///
/// The structural definition bounds make this sufficient to retain every
/// possible reverse-unwind failure: 32 interceptors across nesting depth 8.
pub const MAX_REPEAT_SECONDARY_FAILURES: usize = MAX_REPEAT_INTERCEPTORS * MAX_REPEAT_NESTING_DEPTH;

/// Value-redacted error returned by a repeat policy or interceptor.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RepeatCallbackError;

impl RepeatCallbackError {
    /// Constructs a classified callback error without retaining a payload.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Classifies an application error while dropping its payload and source chain.
    #[must_use]
    pub fn from_error(error: impl Error + Send + Sync + 'static) -> Self {
        drop(error);
        Self
    }
}

impl fmt::Display for RepeatCallbackError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("repeat callback failed")
    }
}

impl Error for RepeatCallbackError {}

/// Error-vs-panic classification for one contained repeat callback failure.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum RepeatCallbackFailureKind {
    /// The callback returned a classified repeat callback error.
    Error,
    /// The callback panicked before or while its future was polled.
    Panic,
}

/// Callback boundary that produced a repeat failure.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum RepeatCallbackPhase {
    /// Ordered interceptor entry.
    Before,
    /// Post-body repeat policy decision.
    Policy,
    /// Reverse-order interceptor unwind.
    After,
}

/// One bounded, value-redacted repeat callback failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepeatCallbackFailure {
    repeat_id: RepeatId,
    interceptor_id: Option<RepeatInterceptorId>,
    phase: RepeatCallbackPhase,
    kind: RepeatCallbackFailureKind,
}

impl RepeatCallbackFailure {
    pub(crate) fn interceptor(
        repeat_id: &RepeatId,
        interceptor_id: &RepeatInterceptorId,
        phase: RepeatCallbackPhase,
        kind: RepeatCallbackFailureKind,
    ) -> Self {
        Self {
            repeat_id: repeat_id.clone(),
            interceptor_id: Some(interceptor_id.clone()),
            phase,
            kind,
        }
    }

    pub(crate) fn policy(repeat_id: &RepeatId, kind: RepeatCallbackFailureKind) -> Self {
        Self {
            repeat_id: repeat_id.clone(),
            interceptor_id: None,
            phase: RepeatCallbackPhase::Policy,
            kind,
        }
    }

    /// Borrows the logical repeat identifier.
    #[must_use]
    pub const fn repeat_id(&self) -> &RepeatId {
        &self.repeat_id
    }

    /// Borrows the interceptor identifier, when the failure came from an interceptor.
    #[must_use]
    pub const fn interceptor_id(&self) -> Option<&RepeatInterceptorId> {
        self.interceptor_id.as_ref()
    }

    /// Returns the callback phase.
    #[must_use]
    pub const fn phase(&self) -> RepeatCallbackPhase {
        self.phase
    }

    /// Returns error-vs-panic classification.
    #[must_use]
    pub const fn kind(&self) -> RepeatCallbackFailureKind {
        self.kind
    }
}

/// Primary authority for one failed logical repeat iteration.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RepeatFailureCause {
    /// The existing tasklet/chunk engine failed; repeat unwind failures are secondary.
    Body(TaskletFailure),
    /// A repeat policy/interceptor callback is primary.
    Callback(RepeatCallbackFailure),
}

/// Bounded value-redacted repeat failure aggregation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepeatFailure {
    primary: RepeatFailureCause,
    secondary: Vec<RepeatCallbackFailure>,
    secondary_truncated: bool,
}

impl RepeatFailure {
    pub(crate) fn body(primary: TaskletFailure) -> Self {
        Self {
            primary: RepeatFailureCause::Body(primary),
            secondary: Vec::new(),
            secondary_truncated: false,
        }
    }

    pub(crate) fn callback(primary: RepeatCallbackFailure) -> Self {
        Self {
            primary: RepeatFailureCause::Callback(primary),
            secondary: Vec::new(),
            secondary_truncated: false,
        }
    }

    pub(crate) fn push_secondary(&mut self, failure: RepeatCallbackFailure) {
        if self.secondary.len() < MAX_REPEAT_SECONDARY_FAILURES {
            self.secondary.push(failure);
        } else {
            self.secondary_truncated = true;
        }
    }

    /// Borrows the primary failure authority.
    #[must_use]
    pub const fn primary(&self) -> &RepeatFailureCause {
        &self.primary
    }

    /// Borrows secondary callback failures in deterministic callback order.
    #[must_use]
    pub fn secondary(&self) -> &[RepeatCallbackFailure] {
        &self.secondary
    }

    /// Returns whether defensive diagnostic truncation occurred.
    #[must_use]
    pub const fn secondary_truncated(&self) -> bool {
        self.secondary_truncated
    }
}

/// Borrowed framework-owned context for one logical repeat iteration.
#[derive(Clone, Copy)]
pub struct RepeatContext<'a> {
    tasklet: TaskletContext<'a>,
    repeat_id: &'a RepeatId,
    ordinal: RepeatOrdinal,
    previous_state: Option<&'a ExecutionContext>,
}

impl<'a> RepeatContext<'a> {
    pub(crate) const fn new(
        tasklet: TaskletContext<'a>,
        repeat_id: &'a RepeatId,
        ordinal: RepeatOrdinal,
        previous_state: Option<&'a ExecutionContext>,
    ) -> Self {
        Self {
            tasklet,
            repeat_id,
            ordinal,
            previous_state,
        }
    }

    /// Borrows the logical repeat identifier.
    #[must_use]
    pub const fn repeat_id(self) -> &'a RepeatId {
        self.repeat_id
    }

    /// Returns the durable ordinal proposed for this logical iteration.
    #[must_use]
    pub const fn ordinal(self) -> RepeatOrdinal {
        self.ordinal
    }

    /// Borrows the last committed state for this repeat lineage, when present.
    #[must_use]
    pub const fn previous_state(self) -> Option<&'a ExecutionContext> {
        self.previous_state
    }

    /// Returns the enclosing tasklet context, including scoped components.
    #[must_use]
    pub const fn tasklet_context(self) -> TaskletContext<'a> {
        self.tasklet
    }

    /// Borrows launch parameters.
    #[must_use]
    pub const fn parameters(&self) -> &'a JobParameters {
        self.tasklet.parameters()
    }

    /// Returns the enclosing job execution identifier.
    #[must_use]
    pub const fn job_execution_id(self) -> JobExecutionId {
        self.tasklet.job_execution_id()
    }

    /// Borrows cooperative stop state.
    #[must_use]
    pub const fn stop_token(&self) -> &'a StopToken {
        self.tasklet.stop_token()
    }

    /// Borrows validated execution correlation.
    #[must_use]
    pub const fn correlation(&self) -> &'a ExecutionCorrelation {
        self.tasklet.correlation()
    }
}

impl fmt::Debug for RepeatContext<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RepeatContext")
            .field("repeat_id", &self.repeat_id)
            .field("ordinal", &self.ordinal)
            .field("previous_state", &self.previous_state.map(|_| "<redacted>"))
            .field("job_execution_id", &self.tasklet.job_execution_id())
            .field("step_execution_id", &self.tasklet.step_execution_id())
            .field(
                "stop_requested",
                &self.tasklet.stop_token().is_stop_requested(),
            )
            .finish_non_exhaustive()
    }
}

/// Accepted repeat policy decision and next bounded durable state.
#[derive(Clone, Eq, PartialEq)]
pub struct RepeatPolicyOutcome {
    state: ExecutionContext,
    decision: RepeatDecision,
}

impl RepeatPolicyOutcome {
    /// Constructs one accepted policy outcome.
    #[must_use]
    pub const fn new(state: ExecutionContext, decision: RepeatDecision) -> Self {
        Self { state, decision }
    }

    /// Constructs a continue decision.
    #[must_use]
    pub const fn continue_with(state: ExecutionContext) -> Self {
        Self::new(state, RepeatDecision::Continue)
    }

    /// Constructs a complete decision.
    #[must_use]
    pub const fn complete_with(state: ExecutionContext) -> Self {
        Self::new(state, RepeatDecision::Complete)
    }

    /// Borrows the next durable state.
    #[must_use]
    pub const fn state(&self) -> &ExecutionContext {
        &self.state
    }

    /// Returns the durable continue/complete decision.
    #[must_use]
    pub const fn decision(&self) -> RepeatDecision {
        self.decision
    }

    pub(crate) fn into_parts(self) -> (ExecutionContext, RepeatDecision) {
        (self.state, self.decision)
    }
}

impl fmt::Debug for RepeatPolicyOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RepeatPolicyOutcome")
            .field("state", &"<redacted>")
            .field("decision", &self.decision)
            .finish()
    }
}

/// Application-owned policy deciding whether an accepted iteration continues.
pub trait RepeatPolicy: Send + Sync {
    /// Produces the bounded state and decision after successful inner work.
    fn decide<'a>(
        &'a self,
        context: RepeatContext<'a>,
    ) -> BoxFuture<'a, Result<RepeatPolicyOutcome, RepeatCallbackError>>;
}

/// Application-owned interceptor around one logical repeat iteration.
pub trait RepeatInterceptor: Send + Sync {
    /// Runs in declaration order before the existing tasklet/chunk engine.
    fn before<'a>(
        &'a self,
        context: RepeatContext<'a>,
    ) -> BoxFuture<'a, Result<(), RepeatCallbackError>>;

    /// Runs in reverse successful-entry order after inner work/policy handling.
    fn after<'a>(
        &'a self,
        context: RepeatContext<'a>,
        outcome: TaskletExecutionOutcome,
    ) -> BoxFuture<'a, Result<(), RepeatCallbackError>>;
}

/// Exact runtime binding for one declared repeat policy identity.
#[derive(Clone)]
pub struct RepeatPolicyRegistration {
    kind: RepeatPolicyKind,
    revision: ComponentRevision,
    configuration: RepeatPolicyConfiguration,
    policy: Arc<dyn RepeatPolicy>,
}

impl RepeatPolicyRegistration {
    /// Registers one policy implementation under its restart-relevant identity.
    #[must_use]
    pub fn new(
        kind: RepeatPolicyKind,
        revision: ComponentRevision,
        configuration: RepeatPolicyConfiguration,
        policy: Arc<dyn RepeatPolicy>,
    ) -> Self {
        Self {
            kind,
            revision,
            configuration,
            policy,
        }
    }

    /// Borrows the registered policy kind.
    #[must_use]
    pub const fn kind(&self) -> &RepeatPolicyKind {
        &self.kind
    }

    /// Borrows the registered policy revision.
    #[must_use]
    pub const fn revision(&self) -> &ComponentRevision {
        &self.revision
    }

    /// Borrows the registered restart-relevant policy configuration.
    #[must_use]
    pub const fn configuration(&self) -> &RepeatPolicyConfiguration {
        &self.configuration
    }

    pub(crate) fn policy(&self) -> &dyn RepeatPolicy {
        self.policy.as_ref()
    }
}

impl fmt::Debug for RepeatPolicyRegistration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RepeatPolicyRegistration")
            .field("kind", &self.kind)
            .field("revision", &self.revision)
            .field("configuration", &self.configuration)
            .finish_non_exhaustive()
    }
}

/// Exact runtime binding for one declared repeat interceptor identity.
#[derive(Clone)]
pub struct RepeatInterceptorRegistration {
    id: RepeatInterceptorId,
    kind: RepeatInterceptorKind,
    revision: ComponentRevision,
    interceptor: Arc<dyn RepeatInterceptor>,
}

impl RepeatInterceptorRegistration {
    /// Registers one interceptor implementation under its stable identity.
    #[must_use]
    pub fn new(
        id: RepeatInterceptorId,
        kind: RepeatInterceptorKind,
        revision: ComponentRevision,
        interceptor: Arc<dyn RepeatInterceptor>,
    ) -> Self {
        Self {
            id,
            kind,
            revision,
            interceptor,
        }
    }

    /// Borrows the stable interceptor identifier.
    #[must_use]
    pub const fn id(&self) -> &RepeatInterceptorId {
        &self.id
    }

    /// Borrows the registered interceptor kind.
    #[must_use]
    pub const fn kind(&self) -> &RepeatInterceptorKind {
        &self.kind
    }

    /// Borrows the registered interceptor revision.
    #[must_use]
    pub const fn revision(&self) -> &ComponentRevision {
        &self.revision
    }

    pub(crate) fn interceptor(&self) -> &dyn RepeatInterceptor {
        self.interceptor.as_ref()
    }
}

impl fmt::Debug for RepeatInterceptorRegistration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RepeatInterceptorRegistration")
            .field("id", &self.id)
            .field("kind", &self.kind)
            .field("revision", &self.revision)
            .finish_non_exhaustive()
    }
}

/// Process-local executable binding for one compiled repeat wrapper.
#[derive(Clone, Debug)]
pub struct RepeatRuntimeRegistration {
    id: RepeatId,
    policy: RepeatPolicyRegistration,
    interceptors: Vec<RepeatInterceptorRegistration>,
    nested: Option<Box<RepeatRuntimeRegistration>>,
}

impl RepeatRuntimeRegistration {
    /// Constructs one executable repeat wrapper.
    #[must_use]
    pub fn new(
        id: RepeatId,
        policy: RepeatPolicyRegistration,
        interceptors: Vec<RepeatInterceptorRegistration>,
    ) -> Self {
        Self {
            id,
            policy,
            interceptors,
            nested: None,
        }
    }

    /// Attaches the executable inner repeat wrapper.
    #[must_use]
    pub fn with_nested(mut self, nested: RepeatRuntimeRegistration) -> Self {
        self.nested = Some(Box::new(nested));
        self
    }

    /// Borrows the declared repeat identifier.
    #[must_use]
    pub const fn id(&self) -> &RepeatId {
        &self.id
    }

    /// Borrows the executable policy registration.
    #[must_use]
    pub const fn policy(&self) -> &RepeatPolicyRegistration {
        &self.policy
    }

    /// Borrows interceptors in declaration order.
    #[must_use]
    pub fn interceptors(&self) -> &[RepeatInterceptorRegistration] {
        &self.interceptors
    }

    /// Borrows the nested runtime wrapper, when declared.
    #[must_use]
    pub fn nested(&self) -> Option<&RepeatRuntimeRegistration> {
        self.nested.as_deref()
    }

    pub(crate) fn matches_definition(&self, definition: &RepeatDefinition) -> bool {
        self.id == *definition.id()
            && self.policy.kind() == definition.policy().kind()
            && self.policy.revision() == definition.policy().revision()
            && self.policy.configuration() == definition.policy().configuration()
            && self.interceptors.len() == definition.interceptors().len()
            && self.interceptors.iter().zip(definition.interceptors()).all(
                |(registered, declared)| {
                    registered.id() == declared.id()
                        && registered.kind() == declared.kind()
                        && registered.revision() == declared.revision()
                },
            )
            && match (self.nested(), definition.nested()) {
                (None, None) => true,
                (Some(registered), Some(declared)) => registered.matches_definition(declared),
                _ => false,
            }
    }
}

pub(crate) async fn invoke_repeat_before(
    interceptor: &dyn RepeatInterceptor,
    context: RepeatContext<'_>,
) -> Result<(), RepeatCallbackFailureKind> {
    let future = catch_unwind(AssertUnwindSafe(|| interceptor.before(context)))
        .map_err(|_| RepeatCallbackFailureKind::Panic)?;
    match AssertUnwindSafe(future).catch_unwind().await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(RepeatCallbackFailureKind::Error),
        Err(_) => Err(RepeatCallbackFailureKind::Panic),
    }
}

pub(crate) async fn invoke_repeat_after(
    interceptor: &dyn RepeatInterceptor,
    context: RepeatContext<'_>,
    outcome: TaskletExecutionOutcome,
) -> Result<(), RepeatCallbackFailureKind> {
    let future = catch_unwind(AssertUnwindSafe(|| interceptor.after(context, outcome)))
        .map_err(|_| RepeatCallbackFailureKind::Panic)?;
    match AssertUnwindSafe(future).catch_unwind().await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(RepeatCallbackFailureKind::Error),
        Err(_) => Err(RepeatCallbackFailureKind::Panic),
    }
}

pub(crate) async fn invoke_repeat_policy(
    policy: &dyn RepeatPolicy,
    context: RepeatContext<'_>,
) -> Result<RepeatPolicyOutcome, RepeatCallbackFailureKind> {
    let future = catch_unwind(AssertUnwindSafe(|| policy.decide(context)))
        .map_err(|_| RepeatCallbackFailureKind::Panic)?;
    match AssertUnwindSafe(future).catch_unwind().await {
        Ok(Ok(outcome)) => Ok(outcome),
        Ok(Err(_)) => Err(RepeatCallbackFailureKind::Error),
        Err(_) => Err(RepeatCallbackFailureKind::Panic),
    }
}
