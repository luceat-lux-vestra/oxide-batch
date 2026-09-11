//! Durable nested-job linkage exchanged with metadata repositories.
//!
//! The types in this module describe semantics only. They contain no SQL,
//! database-driver, executor, or transport type; concrete adapters decide how
//! to provide the required atomicity.

use std::fmt;
use std::time::SystemTime;

use oxide_batch_core::{
    BatchStatus, DefinitionIdentity, ExitStatus, JobExecutionId, JobInstanceId, JobParameters,
    NodeId,
};

/// The committed terminal child observation authoritative for parent routing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NestedJobTerminalObservation {
    status: BatchStatus,
    exit_status: ExitStatus,
    observed_at: SystemTime,
}

impl NestedJobTerminalObservation {
    /// Reconstructs a repository-validated terminal child observation.
    ///
    /// Adapters may construct this only from the linked child execution after
    /// validating that the child is `COMPLETED`, `FAILED`, `STOPPED`, or
    /// `UNKNOWN`.
    #[doc(hidden)]
    #[must_use]
    pub const fn new(status: BatchStatus, exit_status: ExitStatus, observed_at: SystemTime) -> Self {
        Self {
            status,
            exit_status,
            observed_at,
        }
    }

    /// Returns the committed child status.
    #[must_use]
    pub const fn status(&self) -> BatchStatus {
        self.status
    }

    /// Borrows the committed child exit status.
    #[must_use]
    pub const fn exit_status(&self) -> &ExitStatus {
        &self.exit_status
    }

    /// Returns the injected facade-clock observation time.
    #[must_use]
    pub const fn observed_at(&self) -> SystemTime {
        self.observed_at
    }
}

/// One durable parent-attempt to child-attempt linkage.
///
/// A new parent execution receives its own link. Restart derives that link from
/// the previous committed link rather than re-running parameter mapping. A
/// completed child execution may therefore be referenced by more than one
/// parent attempt, while a failed/stopped child is restarted as a new execution
/// of the same child instance.
#[derive(Clone, Eq, PartialEq)]
pub struct NestedJobLink {
    parent_job_instance_id: JobInstanceId,
    parent_job_execution_id: JobExecutionId,
    node_id: NodeId,
    child_definition: DefinitionIdentity,
    child_job_instance_id: JobInstanceId,
    child_job_execution_id: JobExecutionId,
    linked_at: SystemTime,
    terminal: Option<NestedJobTerminalObservation>,
}

impl NestedJobLink {
    /// Reconstructs one repository-validated durable link.
    #[allow(clippy::too_many_arguments)]
    #[doc(hidden)]
    #[must_use]
    pub const fn new(
        parent_job_instance_id: JobInstanceId,
        parent_job_execution_id: JobExecutionId,
        node_id: NodeId,
        child_definition: DefinitionIdentity,
        child_job_instance_id: JobInstanceId,
        child_job_execution_id: JobExecutionId,
        linked_at: SystemTime,
        terminal: Option<NestedJobTerminalObservation>,
    ) -> Self {
        Self {
            parent_job_instance_id,
            parent_job_execution_id,
            node_id,
            child_definition,
            child_job_instance_id,
            child_job_execution_id,
            linked_at,
            terminal,
        }
    }

    /// Returns the logical parent instance whose restart lineage owns the link.
    #[must_use]
    pub const fn parent_job_instance_id(&self) -> JobInstanceId {
        self.parent_job_instance_id
    }

    /// Returns the exact parent attempt that committed this link.
    #[must_use]
    pub const fn parent_job_execution_id(&self) -> JobExecutionId {
        self.parent_job_execution_id
    }

    /// Borrows the parent logical nested-job node.
    #[must_use]
    pub const fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Borrows the exact child definition required by the parent node.
    #[must_use]
    pub const fn child_definition(&self) -> &DefinitionIdentity {
        &self.child_definition
    }

    /// Returns the canonical child instance selected by mapped identifying parameters.
    #[must_use]
    pub const fn child_job_instance_id(&self) -> JobInstanceId {
        self.child_job_instance_id
    }

    /// Returns the child execution owned or reused by this parent attempt.
    #[must_use]
    pub const fn child_job_execution_id(&self) -> JobExecutionId {
        self.child_job_execution_id
    }

    /// Returns when the link became durable according to the injected facade clock.
    #[must_use]
    pub const fn linked_at(&self) -> SystemTime {
        self.linked_at
    }

    /// Borrows the committed terminal observation, when one exists.
    #[must_use]
    pub const fn terminal(&self) -> Option<&NestedJobTerminalObservation> {
        self.terminal.as_ref()
    }

    /// Returns a copy with one repository-validated terminal observation.
    #[doc(hidden)]
    #[must_use]
    pub fn with_terminal(mut self, terminal: NestedJobTerminalObservation) -> Self {
        self.terminal = Some(terminal);
        self
    }
}

impl fmt::Debug for NestedJobLink {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NestedJobLink")
            .field("parent_job_instance_id", &self.parent_job_instance_id)
            .field("parent_job_execution_id", &self.parent_job_execution_id)
            .field("node_id", &self.node_id)
            .field("child_definition", &self.child_definition)
            .field("child_job_instance_id", &self.child_job_instance_id)
            .field("child_job_execution_id", &self.child_job_execution_id)
            .field("linked_at", &self.linked_at)
            .field("terminal", &self.terminal)
            .finish()
    }
}

/// Complete first-link request after deterministic parameter resolution.
///
/// `Debug` for [`JobParameters`] exposes only counts, so mapped values cannot
/// leak through this request's diagnostics.
#[derive(Clone, Eq, PartialEq)]
pub struct NestedJobLinkRequest {
    parent_job_instance_id: JobInstanceId,
    parent_job_execution_id: JobExecutionId,
    node_id: NodeId,
    child_definition: DefinitionIdentity,
    child_parameters: JobParameters,
    linked_at: SystemTime,
}

impl NestedJobLinkRequest {
    /// Builds one request whose complete child parameter set must be committed
    /// with child instance/execution creation and the parent-child link.
    #[must_use]
    pub const fn new(
        parent_job_instance_id: JobInstanceId,
        parent_job_execution_id: JobExecutionId,
        node_id: NodeId,
        child_definition: DefinitionIdentity,
        child_parameters: JobParameters,
        linked_at: SystemTime,
    ) -> Self {
        Self {
            parent_job_instance_id,
            parent_job_execution_id,
            node_id,
            child_definition,
            child_parameters,
            linked_at,
        }
    }

    /// Returns the logical parent instance.
    #[must_use]
    pub const fn parent_job_instance_id(&self) -> JobInstanceId {
        self.parent_job_instance_id
    }

    /// Returns the exact parent attempt creating the first link.
    #[must_use]
    pub const fn parent_job_execution_id(&self) -> JobExecutionId {
        self.parent_job_execution_id
    }

    /// Borrows the parent nested-job logical node.
    #[must_use]
    pub const fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Borrows the exact child definition identity.
    #[must_use]
    pub const fn child_definition(&self) -> &DefinitionIdentity {
        &self.child_definition
    }

    /// Borrows the complete mapped typed child parameter set.
    #[must_use]
    pub const fn child_parameters(&self) -> &JobParameters {
        &self.child_parameters
    }

    /// Returns the requested durable link time.
    #[must_use]
    pub const fn linked_at(&self) -> SystemTime {
        self.linked_at
    }
}

impl fmt::Debug for NestedJobLinkRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NestedJobLinkRequest")
            .field("parent_job_instance_id", &self.parent_job_instance_id)
            .field("parent_job_execution_id", &self.parent_job_execution_id)
            .field("node_id", &self.node_id)
            .field("child_definition", &self.child_definition)
            .field("child_parameters", &self.child_parameters)
            .field("linked_at", &self.linked_at)
            .finish()
    }
}
