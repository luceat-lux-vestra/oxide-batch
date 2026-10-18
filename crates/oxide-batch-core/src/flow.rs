//! Durable flow identities, transition targets, and start controls.
//!
//! Every value here is written to metadata, read back on restart, or
//! hashed into a definition fingerprint. The graph that arranges them and
//! the compiler that validates it live above this crate.

use std::num::NonZeroU32;

use crate::{DefinitionError, DefinitionTokenKind, definition_token, validate_token};

/// The maximum number of durable local partitions in one partitioned step.
pub const MAX_PARTITIONS: u16 = 1_024;
definition_token!(
    NodeId,
    DefinitionTokenKind::Node,
    "A stable logical identifier for one flow-graph node.

Logical identity survives display-name changes. Runtime and database
identifiers are never node identifiers."
);
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
    /// Returns [`DefinitionError::ZeroStartLimit`] for zero, because a step
    /// that can never start is a definition mistake rather than a policy.
    pub fn new(value: u32) -> Result<Self, DefinitionError> {
        NonZeroU32::new(value)
            .map(Self)
            .ok_or(DefinitionError::ZeroStartLimit)
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

/// Whether a job or logical step may be entered by a later execution attempt.
///
/// This M7 execution-definition policy is distinct from
/// `RestartabilityDeclaration`, which describes whether component state can be
/// reconstructed. The default preserves every pre-M7 definition's restart
/// behavior and manifest identity.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum RestartPolicy {
    /// Later failed/stopped execution attempts may re-enter this definition.
    #[default]
    Restartable,
    /// A later execution attempt must fail closed before re-entering this definition.
    NotRestartable,
}

impl RestartPolicy {
    /// Returns whether a later execution attempt may re-enter this definition.
    #[must_use]
    pub const fn is_restartable(self) -> bool {
        matches!(self, Self::Restartable)
    }
}

/// Restart-relevant start controls for one logical step.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StartControls {
    start_limit: StartLimit,
    allow_start_if_complete: bool,
    restart_policy: RestartPolicy,
}

impl StartControls {
    /// Constructs explicit start controls.
    #[must_use]
    pub const fn new(start_limit: StartLimit, allow_start_if_complete: bool) -> Self {
        Self {
            start_limit,
            allow_start_if_complete,
            restart_policy: RestartPolicy::Restartable,
        }
    }

    /// Applies the explicit execution-definition restart policy.
    #[must_use]
    pub const fn with_restart_policy(mut self, restart_policy: RestartPolicy) -> Self {
        self.restart_policy = restart_policy;
        self
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

    /// Returns the execution-definition restart policy for this logical step.
    #[must_use]
    pub const fn restart_policy(&self) -> RestartPolicy {
        self.restart_policy
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
    /// Returns the deterministic ordering key for canonical output.
    #[must_use]
    pub fn sort_key(&self) -> (u8, &str) {
        match self {
            Self::Node(id) => (0, id.as_str()),
            Self::Terminal(kind) => (1, kind.as_str()),
        }
    }
}
