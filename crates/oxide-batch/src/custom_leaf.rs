//! Registered custom-leaf handler contract for the single flow runtime.

use std::fmt;
use std::sync::Arc;

use crate::{
    BoxFuture, ComponentRevision, CustomLeafKind, ExecutionContext, JobExecutionId, JobParameters,
    StateSchemaId, StateSchemaVersion, StepExecutionId, StepExecutionListener, StopToken,
    TaskletContext, TaskletError, TaskletOutcome,
};

/// Application handler for one registered custom-leaf kind.
///
/// The framework owns lifecycle transitions, durable state commits, cancellation,
/// listeners, and repository access. Implementations receive only the bounded
/// call-scoped inputs needed for user work and return the next candidate state.
pub trait CustomLeafHandler: Send + Sync {
    /// Executes one custom-leaf attempt.
    fn execute<'a>(
        &'a self,
        context: CustomLeafContext<'a>,
    ) -> BoxFuture<'a, Result<CustomLeafResult, TaskletError>>;
}

/// Borrowed framework-owned inputs for one custom-leaf invocation.
#[derive(Clone, Copy)]
pub struct CustomLeafContext<'a> {
    tasklet: TaskletContext<'a>,
    previous_state: Option<&'a ExecutionContext>,
}

impl<'a> CustomLeafContext<'a> {
    pub(crate) const fn new(
        tasklet: TaskletContext<'a>,
        previous_state: Option<&'a ExecutionContext>,
    ) -> Self {
        Self {
            tasklet,
            previous_state,
        }
    }

    /// Borrows launch parameters.
    #[must_use]
    pub const fn parameters(&self) -> &'a JobParameters {
        self.tasklet.parameters()
    }

    /// Returns the enclosing job-attempt identifier.
    #[must_use]
    pub const fn job_execution_id(self) -> JobExecutionId {
        self.tasklet.job_execution_id()
    }

    /// Returns this step-attempt identifier.
    #[must_use]
    pub const fn step_execution_id(self) -> StepExecutionId {
        self.tasklet.step_execution_id()
    }

    /// Borrows the cooperative stop token.
    #[must_use]
    pub const fn stop_token(&self) -> &'a StopToken {
        self.tasklet.stop_token()
    }

    /// Borrows the previously committed custom state, when one exists.
    #[must_use]
    pub const fn previous_state(&self) -> Option<&'a ExecutionContext> {
        self.previous_state
    }
}

impl fmt::Debug for CustomLeafContext<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CustomLeafContext")
            .field("job_execution_id", &self.tasklet.job_execution_id())
            .field("step_execution_id", &self.tasklet.step_execution_id())
            .field("stop_requested", &self.tasklet.stop_token().is_stop_requested())
            .field("previous_state", &self.previous_state.map(|_| "<redacted>"))
            .finish_non_exhaustive()
    }
}

/// Framework-validated result returned by one custom-leaf handler.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CustomLeafResult {
    outcome: TaskletOutcome,
    state: Option<ExecutionContext>,
}

impl CustomLeafResult {
    /// Returns an outcome without publishing new durable custom state.
    #[must_use]
    pub const fn new(outcome: TaskletOutcome) -> Self {
        Self {
            outcome,
            state: None,
        }
    }

    /// Attaches the next candidate durable custom state.
    ///
    /// The framework validates its declared schema identity and commits it by
    /// repository CAS before the step terminal lifecycle transition.
    #[must_use]
    pub fn with_state(mut self, state: ExecutionContext) -> Self {
        self.state = Some(state);
        self
    }

    /// Borrows the ordinary tasklet-compatible outcome.
    #[must_use]
    pub const fn outcome(&self) -> &TaskletOutcome {
        &self.outcome
    }

    /// Borrows the candidate state without exposing it through diagnostics.
    #[must_use]
    pub const fn state(&self) -> Option<&ExecutionContext> {
        self.state.as_ref()
    }

    pub(crate) fn into_parts(self) -> (TaskletOutcome, Option<ExecutionContext>) {
        (self.outcome, self.state)
    }
}

/// One exact registration accepted for a compiled custom-leaf declaration.
pub struct CustomLeafRegistration {
    kind: CustomLeafKind,
    handler_revision: ComponentRevision,
    state_schema_id: StateSchemaId,
    state_schema_version: StateSchemaVersion,
    handler: Arc<dyn CustomLeafHandler>,
    listeners: Vec<(ComponentRevision, Arc<dyn StepExecutionListener>)>,
}

impl CustomLeafRegistration {
    /// Registers exactly one handler identity and state schema.
    #[must_use]
    pub fn new(
        kind: CustomLeafKind,
        handler_revision: ComponentRevision,
        state_schema_id: StateSchemaId,
        state_schema_version: StateSchemaVersion,
        handler: Arc<dyn CustomLeafHandler>,
    ) -> Self {
        Self {
            kind,
            handler_revision,
            state_schema_id,
            state_schema_version,
            handler,
            listeners: Vec::new(),
        }
    }

    /// Registers one framework step listener with its restart-relevant revision.
    #[must_use]
    pub fn with_listener(
        mut self,
        revision: ComponentRevision,
        listener: Arc<dyn StepExecutionListener>,
    ) -> Self {
        self.listeners.push((revision, listener));
        self
    }

    /// Borrows the stable registered leaf kind.
    #[must_use]
    pub const fn kind(&self) -> &CustomLeafKind {
        &self.kind
    }

    /// Borrows the exact executable handler revision.
    #[must_use]
    pub const fn handler_revision(&self) -> &ComponentRevision {
        &self.handler_revision
    }

    /// Borrows the durable state schema identifier.
    #[must_use]
    pub const fn state_schema_id(&self) -> &StateSchemaId {
        &self.state_schema_id
    }

    /// Returns the durable state schema version.
    #[must_use]
    pub const fn state_schema_version(&self) -> StateSchemaVersion {
        self.state_schema_version
    }

    pub(crate) fn handler(&self) -> &dyn CustomLeafHandler {
        self.handler.as_ref()
    }

    pub(crate) fn listeners(&self) -> &[(ComponentRevision, Arc<dyn StepExecutionListener>)] {
        &self.listeners
    }
}

impl fmt::Debug for CustomLeafRegistration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CustomLeafRegistration")
            .field("kind", &self.kind)
            .field("handler_revision", &self.handler_revision)
            .field("state_schema_id", &self.state_schema_id)
            .field("state_schema_version", &self.state_schema_version)
            .field("listener_count", &self.listeners.len())
            .finish_non_exhaustive()
    }
}
