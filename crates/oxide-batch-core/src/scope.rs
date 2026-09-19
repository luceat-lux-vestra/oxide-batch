//! Shared scoped-component definition vocabulary.
//!
//! These types live in core because both the immutable plan and metadata
//! repository need to name the same value-free scope identities without a
//! dependency from the repository crate back to the plan crate.

use crate::{
    DefinitionError, DefinitionTokenKind, ParameterValueKind, definition_token, validate_token,
};

/// Maximum number of live scoped components one execution scope may own.
pub const MAX_SCOPED_COMPONENTS: usize = 256;
/// Maximum number of late-bound values one scoped component may declare.
pub const MAX_LATE_BOUND_INPUTS: usize = 64;
/// Maximum UTF-8 bytes across one structured selector path.
pub const MAX_SELECTOR_PATH_BYTES: usize = 1_024;
/// Maximum number of path segments in one structured selector.
pub const MAX_SELECTOR_PATH_SEGMENTS: usize = 32;

/// Returns whether `segments` satisfy the shared structured-selector path contract.
///
/// The byte bound counts segment bytes plus one separator byte between segments,
/// matching the canonical plan representation without introducing an expression language.
#[must_use]
pub fn selector_path_is_valid(segments: &[String]) -> bool {
    if segments.is_empty() || segments.len() > MAX_SELECTOR_PATH_SEGMENTS {
        return false;
    }
    let bytes = segments
        .iter()
        .map(String::len)
        .sum::<usize>()
        .saturating_add(segments.len().saturating_sub(1));
    bytes <= MAX_SELECTOR_PATH_BYTES
        && segments.iter().all(|segment| {
            !segment.is_empty()
                && segment.trim() == segment
                && !segment.chars().any(char::is_control)
        })
}

definition_token!(
    ScopedComponentId,
    DefinitionTokenKind::Component,
    "Stable logical identity of one job- or step-scoped component."
);

/// Runtime lifetime of one scoped component definition.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum ScopeKind {
    /// One live instance for one job-execution attempt.
    Job,
    /// One live instance for one step-execution attempt.
    Step,
}

impl ScopeKind {
    /// Returns the stable manifest and durable code.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Job => "job",
            Self::Step => "step",
        }
    }
}

/// Closed framework-owned metadata that a scoped resolver may read.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum ScopeFrameworkSource {
    /// Current logical job-instance identifier.
    JobInstanceId,
    /// Current job-execution attempt identifier.
    JobExecutionId,
    /// Current step-execution attempt identifier; step scope only.
    StepExecutionId,
    /// One-based current job execution attempt.
    Attempt,
    /// Current definition revision.
    DefinitionRevision,
    /// Current definition fingerprint encoded as lower-case hexadecimal.
    DefinitionFingerprint,
    /// Current logical flow-node identifier; step scope only.
    LogicalNodeId,
    /// Current durable step name; step scope only.
    StepName,
}

impl ScopeFrameworkSource {
    /// Returns the stable manifest and durable code.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::JobInstanceId => "job_instance_id",
            Self::JobExecutionId => "job_execution_id",
            Self::StepExecutionId => "step_execution_id",
            Self::Attempt => "attempt",
            Self::DefinitionRevision => "definition_revision",
            Self::DefinitionFingerprint => "definition_fingerprint",
            Self::LogicalNodeId => "logical_node_id",
            Self::StepName => "step_name",
        }
    }

    /// Returns the value kind produced before any declared coercion.
    #[must_use]
    pub const fn value_kind(self) -> ParameterValueKind {
        match self {
            Self::JobInstanceId | Self::JobExecutionId | Self::StepExecutionId | Self::Attempt => {
                ParameterValueKind::U64
            }
            Self::DefinitionRevision
            | Self::DefinitionFingerprint
            | Self::LogicalNodeId
            | Self::StepName => ParameterValueKind::String,
        }
    }
}
