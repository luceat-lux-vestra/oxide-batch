//! The closed command grammar.
//!
//! Nouns and verbs are fixed at compile time. There is no plugin, alias, or
//! dynamic discovery, so an unknown word is always an error rather than an
//! extension point.

use std::fmt;

use oxide_batch::AuthorizationClass;

/// One command of the closed `oxide-batch <noun> <verb>` grammar.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum Command {
    /// List registered job names.
    JobList,
    /// Show one job's definition identity.
    JobShow,
    /// List instances of a job.
    InstanceList,
    /// Show one instance.
    InstanceShow,
    /// List executions of an instance, or age-bounded stale candidates.
    ExecutionList,
    /// Show one execution projection.
    ExecutionShow,
    /// List step executions.
    ExecutionSteps,
    /// List partitions of a partitioned step.
    ExecutionPartitions,
    /// List flow, recovery, and operator records.
    ExecutionHistory,
    /// Request a durable stop.
    ExecutionStop,
    /// Start a new attempt.
    ExecutionRestart,
    /// Make an execution permanently non-restartable.
    ExecutionAbandon,
    /// Propose and apply a recovery decision.
    ExecutionRecover,
    /// Launch a registered job.
    Launch,
    /// Produce a bounded purge plan and digest.
    RetentionPlan,
    /// Apply a purge plan.
    RetentionApply,
    /// Place a hold on an instance.
    RetentionHold,
    /// Release a hold.
    RetentionRelease,
    /// Print effective configuration with sources.
    ConfigShow,
    /// Report schema version and migration state.
    SchemaStatus,
    /// Write a bounded redacted incident bundle.
    DiagnosticsBundle,
}

impl Command {
    /// Resolves one closed noun and verb.
    ///
    /// `launch` is the single one-word command; every other command is a noun
    /// followed by a verb.
    #[must_use]
    pub fn resolve(words: &[&str]) -> Option<Self> {
        match words {
            ["launch"] => Some(Self::Launch),
            ["job", "list"] => Some(Self::JobList),
            ["job", "show"] => Some(Self::JobShow),
            ["instance", "list"] => Some(Self::InstanceList),
            ["instance", "show"] => Some(Self::InstanceShow),
            ["execution", "list"] => Some(Self::ExecutionList),
            ["execution", "show"] => Some(Self::ExecutionShow),
            ["execution", "steps"] => Some(Self::ExecutionSteps),
            ["execution", "partitions"] => Some(Self::ExecutionPartitions),
            ["execution", "history"] => Some(Self::ExecutionHistory),
            ["execution", "stop"] => Some(Self::ExecutionStop),
            ["execution", "restart"] => Some(Self::ExecutionRestart),
            ["execution", "abandon"] => Some(Self::ExecutionAbandon),
            ["execution", "recover"] => Some(Self::ExecutionRecover),
            ["retention", "plan"] => Some(Self::RetentionPlan),
            ["retention", "apply"] => Some(Self::RetentionApply),
            ["retention", "hold"] => Some(Self::RetentionHold),
            ["retention", "release"] => Some(Self::RetentionRelease),
            ["config", "show"] => Some(Self::ConfigShow),
            ["schema", "status"] => Some(Self::SchemaStatus),
            ["diagnostics", "bundle"] => Some(Self::DiagnosticsBundle),
            _ => None,
        }
    }

    /// Returns the canonical space-separated command name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::JobList => "job list",
            Self::JobShow => "job show",
            Self::InstanceList => "instance list",
            Self::InstanceShow => "instance show",
            Self::ExecutionList => "execution list",
            Self::ExecutionShow => "execution show",
            Self::ExecutionSteps => "execution steps",
            Self::ExecutionPartitions => "execution partitions",
            Self::ExecutionHistory => "execution history",
            Self::ExecutionStop => "execution stop",
            Self::ExecutionRestart => "execution restart",
            Self::ExecutionAbandon => "execution abandon",
            Self::ExecutionRecover => "execution recover",
            Self::Launch => "launch",
            Self::RetentionPlan => "retention plan",
            Self::RetentionApply => "retention apply",
            Self::RetentionHold => "retention hold",
            Self::RetentionRelease => "retention release",
            Self::ConfigShow => "config show",
            Self::SchemaStatus => "schema status",
            Self::DiagnosticsBundle => "diagnostics bundle",
        }
    }

    /// Returns the class a deployment authorizes before the command runs.
    #[must_use]
    pub const fn class(self) -> ActionClass {
        match self {
            Self::JobList
            | Self::JobShow
            | Self::InstanceList
            | Self::InstanceShow
            | Self::ExecutionList
            | Self::ExecutionShow
            | Self::ExecutionSteps
            | Self::ExecutionPartitions
            | Self::ExecutionHistory
            | Self::RetentionPlan
            | Self::ConfigShow
            | Self::SchemaStatus
            | Self::DiagnosticsBundle => ActionClass::Read,
            Self::ExecutionStop | Self::ExecutionRestart | Self::Launch => ActionClass::Lifecycle,
            Self::ExecutionAbandon
            | Self::ExecutionRecover
            | Self::RetentionApply
            | Self::RetentionHold
            | Self::RetentionRelease => ActionClass::Destructive,
        }
    }

    /// Returns whether the command can change durable state.
    #[must_use]
    pub const fn is_mutating(self) -> bool {
        !matches!(self.class(), ActionClass::Read)
    }

    /// Returns whether `--dry-run` is accepted.
    ///
    /// Dry run is offered only where a guard evaluation or a plan digest is
    /// worth reporting without a mutation.
    #[must_use]
    pub const fn supports_dry_run(self) -> bool {
        matches!(
            self,
            Self::Launch | Self::ExecutionRestart | Self::ExecutionRecover | Self::RetentionApply
        )
    }

    /// Returns whether the command reads a bounded page of rows.
    #[must_use]
    pub const fn is_paginated(self) -> bool {
        matches!(
            self,
            Self::JobList
                | Self::InstanceList
                | Self::ExecutionList
                | Self::ExecutionSteps
                | Self::ExecutionPartitions
                | Self::ExecutionHistory
        )
    }

    /// Returns whether the command needs an open repository connection.
    #[must_use]
    pub const fn needs_repository(self) -> bool {
        !matches!(self, Self::ConfigShow)
    }

    /// Returns every command in canonical order.
    #[must_use]
    pub const fn all() -> &'static [Self] {
        &[
            Self::JobList,
            Self::JobShow,
            Self::InstanceList,
            Self::InstanceShow,
            Self::ExecutionList,
            Self::ExecutionShow,
            Self::ExecutionSteps,
            Self::ExecutionPartitions,
            Self::ExecutionHistory,
            Self::ExecutionStop,
            Self::ExecutionRestart,
            Self::ExecutionAbandon,
            Self::ExecutionRecover,
            Self::Launch,
            Self::RetentionPlan,
            Self::RetentionApply,
            Self::RetentionHold,
            Self::RetentionRelease,
            Self::ConfigShow,
            Self::SchemaStatus,
            Self::DiagnosticsBundle,
        ]
    }
}

impl fmt::Display for Command {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// The separately authorizable class of one command.
///
/// The class mirrors [`AuthorizationClass`] so a deployment authorizes the CLI
/// with the same vocabulary it authorizes the portable services.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum ActionClass {
    /// Inspection and planning. Deletes and changes nothing.
    Read,
    /// Launch, restart, and stop.
    Lifecycle,
    /// Abandon, recover, hold, release, and purge application.
    Destructive,
}

impl ActionClass {
    /// Returns the stable machine name of this class.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Read => "READ",
            Self::Lifecycle => "LIFECYCLE",
            Self::Destructive => "DESTRUCTIVE",
        }
    }

    /// Returns whether the class requires explicit confirmation.
    #[must_use]
    pub const fn requires_confirmation(self) -> bool {
        matches!(self, Self::Destructive)
    }
}

impl From<AuthorizationClass> for ActionClass {
    fn from(value: AuthorizationClass) -> Self {
        match value {
            AuthorizationClass::Lifecycle => Self::Lifecycle,
            AuthorizationClass::Destructive => Self::Destructive,
            _ => Self::Read,
        }
    }
}

impl fmt::Display for ActionClass {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::panic)]

    use super::{ActionClass, Command};

    #[test]
    fn every_command_resolves_from_its_canonical_name() {
        for command in Command::all() {
            let words: Vec<&str> = command.as_str().split(' ').collect();
            assert_eq!(Command::resolve(&words), Some(*command));
        }
    }

    #[test]
    fn unknown_words_do_not_resolve() {
        assert_eq!(Command::resolve(&["job", "delete"]), None);
        assert_eq!(Command::resolve(&["jobs", "list"]), None);
        assert_eq!(Command::resolve(&["launch", "now"]), None);
        assert_eq!(Command::resolve(&[]), None);
    }

    #[test]
    fn destructive_commands_require_confirmation() {
        for command in Command::all() {
            assert_eq!(
                command.class().requires_confirmation(),
                matches!(command.class(), ActionClass::Destructive),
                "{command} confirmation rule disagrees with its class"
            );
        }
    }

    #[test]
    fn read_commands_never_mutate() {
        for command in Command::all() {
            if matches!(command.class(), ActionClass::Read) {
                assert!(!command.is_mutating(), "{command} is read but mutating");
            }
        }
    }
}
