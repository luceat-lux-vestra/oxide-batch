//! Durable flow identities, transition targets, and start controls.
//!
//! Every value here is written to metadata, read back on restart, or
//! hashed into a definition fingerprint. The graph that arranges them and
//! the compiler that validates it live above this crate.

use std::num::NonZeroU32;

use serde_json::{Value, json};

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

    /// Projects the start controls into their canonical manifest member.
    #[must_use]
    pub fn manifest_value(self) -> Value {
        json!({
            "allow_start_if_complete": self.allow_start_if_complete,
            "start_limit": self.start_limit.get()
        })
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
    /// Projects the target into its canonical manifest member.
    #[must_use]
    pub fn manifest_value(&self) -> Value {
        match self {
            Self::Node(id) => json!({ "node": id.as_str() }),
            Self::Terminal(kind) => json!({ "terminal": kind.as_str() }),
        }
    }

    /// Returns the deterministic ordering key for canonical output.
    #[must_use]
    pub fn sort_key(&self) -> (u8, &str) {
        match self {
            Self::Node(id) => (0, id.as_str()),
            Self::Terminal(kind) => (1, kind.as_str()),
        }
    }
}
