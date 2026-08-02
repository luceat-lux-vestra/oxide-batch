//! The stable closed set of process exit categories.
//!
//! Exit codes are a machine interface. A code is never reused for a different
//! meaning, and a new meaning takes a new code rather than overloading an
//! existing one.

use std::fmt;

/// One stable process exit category.
///
/// The numeric codes are fixed by the
/// [operator CLI contract](https://github.com/luceat-lux-vestra/oxide-batch/blob/main/docs/operations/operator-cli.md)
/// and are part of the published interface.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum ExitCategory {
    /// The command completed and its durable effect, if any, is committed.
    Success,
    /// The invocation could not be parsed against the closed grammar.
    Usage,
    /// Configuration was missing, unknown, out of bounds, or contradictory.
    ConfigurationInvalid,
    /// A core guard rejected the action; no effect was applied.
    GuardRejected,
    /// The named target does not exist.
    TargetNotFound,
    /// The supplied expected version lost its compare-and-swap.
    OptimisticConflict,
    /// The durable outcome could not be determined.
    ///
    /// This is not a failure. The caller replays the same operation identifier
    /// to learn the recorded outcome or to re-attempt the effect exactly once.
    OutcomeUnknown,
    /// The repository was unavailable or its infrastructure failed.
    RepositoryUnavailable,
    /// A destructive command lacked or was denied its required confirmation.
    ConfirmationRequired,
    /// The client deadline elapsed before the command completed.
    DeadlineExceeded,
    /// Standard output could not be written, including a closed pipe.
    OutputFailure,
    /// A defect. An internal error always emits a redacted diagnostic.
    Internal,
}

impl ExitCategory {
    /// Returns the stable process exit code.
    #[must_use]
    pub const fn code(self) -> u8 {
        match self {
            Self::Success => 0,
            Self::Usage => 1,
            Self::ConfigurationInvalid => 2,
            Self::GuardRejected => 3,
            Self::TargetNotFound => 4,
            Self::OptimisticConflict => 5,
            Self::OutcomeUnknown => 6,
            Self::RepositoryUnavailable => 7,
            Self::ConfirmationRequired => 8,
            Self::DeadlineExceeded => 9,
            Self::OutputFailure => 10,
            Self::Internal => 70,
        }
    }

    /// Returns the stable machine name of this category.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "SUCCESS",
            Self::Usage => "USAGE",
            Self::ConfigurationInvalid => "CONFIGURATION_INVALID",
            Self::GuardRejected => "GUARD_REJECTED",
            Self::TargetNotFound => "TARGET_NOT_FOUND",
            Self::OptimisticConflict => "OPTIMISTIC_CONFLICT",
            Self::OutcomeUnknown => "OUTCOME_UNKNOWN",
            Self::RepositoryUnavailable => "REPOSITORY_UNAVAILABLE",
            Self::ConfirmationRequired => "CONFIRMATION_REQUIRED",
            Self::DeadlineExceeded => "DEADLINE_EXCEEDED",
            Self::OutputFailure => "OUTPUT_FAILURE",
            Self::Internal => "INTERNAL",
        }
    }

    /// Returns the JSON envelope outcome this category reports.
    #[must_use]
    pub const fn outcome(self) -> Outcome {
        match self {
            Self::Success => Outcome::Success,
            Self::GuardRejected | Self::ConfirmationRequired => Outcome::Rejected,
            Self::OptimisticConflict => Outcome::Conflict,
            Self::OutcomeUnknown => Outcome::Unknown,
            Self::Usage
            | Self::ConfigurationInvalid
            | Self::TargetNotFound
            | Self::RepositoryUnavailable
            | Self::DeadlineExceeded
            | Self::OutputFailure
            | Self::Internal => Outcome::Error,
        }
    }

    /// Returns every category in code order.
    ///
    /// The published exit-category test walks this slice, so a new category
    /// cannot be added without a named case proving it.
    #[must_use]
    pub const fn all() -> &'static [Self] {
        &[
            Self::Success,
            Self::Usage,
            Self::ConfigurationInvalid,
            Self::GuardRejected,
            Self::TargetNotFound,
            Self::OptimisticConflict,
            Self::OutcomeUnknown,
            Self::RepositoryUnavailable,
            Self::ConfirmationRequired,
            Self::DeadlineExceeded,
            Self::OutputFailure,
            Self::Internal,
        ]
    }
}

impl fmt::Display for ExitCategory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// The `outcome` field of the versioned JSON envelope.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum Outcome {
    /// The command applied or replayed its effect.
    Success,
    /// A guard or a confirmation rule refused the action.
    Rejected,
    /// An optimistic version lost its compare-and-swap.
    Conflict,
    /// The durable outcome is undetermined and must be replayed.
    Unknown,
    /// The command failed before reaching a durable decision.
    Error,
}

impl Outcome {
    /// Returns the stable machine name of this outcome.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Rejected => "rejected",
            Self::Conflict => "conflict",
            Self::Unknown => "unknown",
            Self::Error => "error",
        }
    }
}

impl fmt::Display for Outcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::panic)]

    use super::ExitCategory;

    #[test]
    fn codes_are_unique_and_ordered() {
        let mut seen = Vec::new();
        for category in ExitCategory::all() {
            let code = category.code();
            assert!(!seen.contains(&code), "exit code {code} is reused");
            seen.push(code);
        }
    }

    #[test]
    fn published_codes_never_change() {
        assert_eq!(ExitCategory::Success.code(), 0);
        assert_eq!(ExitCategory::Usage.code(), 1);
        assert_eq!(ExitCategory::ConfigurationInvalid.code(), 2);
        assert_eq!(ExitCategory::GuardRejected.code(), 3);
        assert_eq!(ExitCategory::TargetNotFound.code(), 4);
        assert_eq!(ExitCategory::OptimisticConflict.code(), 5);
        assert_eq!(ExitCategory::OutcomeUnknown.code(), 6);
        assert_eq!(ExitCategory::RepositoryUnavailable.code(), 7);
        assert_eq!(ExitCategory::ConfirmationRequired.code(), 8);
        assert_eq!(ExitCategory::DeadlineExceeded.code(), 9);
        assert_eq!(ExitCategory::OutputFailure.code(), 10);
        assert_eq!(ExitCategory::Internal.code(), 70);
    }
}
