//! The closed-grammar argument parser.
//!
//! Parsing is hand written rather than delegated to a parser library so that
//! the closed grammar, the rejection of every unknown word, and the stable exit
//! categories stay observable properties of this crate rather than of a
//! dependency's defaults.
//!
//! The parser records raw strings and validates only what argument syntax can
//! decide. A value that participates in configuration precedence is validated
//! by [`crate::config`] so that a bad value reports
//! [`ExitCategory::ConfigurationInvalid`] rather than a usage error.

use std::fmt;
use std::path::PathBuf;

use crate::command::Command;
use crate::exit::ExitCategory;

/// The requested output form.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum OutputForm {
    /// A stable but explicitly unversioned presentation for a person.
    #[default]
    Human,
    /// The versioned machine envelope.
    Json,
}

impl OutputForm {
    /// Parses the closed set of output forms.
    fn parse(value: &str) -> Option<Self> {
        match value {
            "human" => Some(Self::Human),
            "json" => Some(Self::Json),
            _ => None,
        }
    }

    /// Returns the stable machine name of this form.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Human => "human",
            Self::Json => "json",
        }
    }
}

/// The record family selected by `execution history`.
///
/// A single traversal is selected per invocation so that one opaque cursor
/// continues exactly one keyset traversal. Merging three record families into
/// one page would make a continuation token ambiguous.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RecordArg {
    /// Audited operator requests.
    #[default]
    Operator,
    /// Append-only recovery decisions.
    Recovery,
    /// Recorded flow transitions.
    Flow,
}

impl RecordArg {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "operator" => Some(Self::Operator),
            "recovery" => Some(Self::Recovery),
            "flow" => Some(Self::Flow),
            _ => None,
        }
    }

    /// Returns the stable machine name of this record family.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Operator => "operator",
            Self::Recovery => "recovery",
            Self::Flow => "flow",
        }
    }
}

/// The recovery disposition requested by `execution recover`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DirectiveArg {
    /// Make the observed attempt restart-eligible under a stated failure.
    MarkFailed,
    /// Make the logical instance permanently non-restartable.
    Abandon,
}

impl DirectiveArg {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "mark-failed" => Some(Self::MarkFailed),
            "abandon" => Some(Self::Abandon),
            _ => None,
        }
    }
}

/// One parsed invocation of the closed grammar.
///
/// Every field is the raw argument text or a syntactically validated value.
/// Semantic validation belongs to configuration resolution and to the command
/// itself.
#[derive(Clone, Debug, Default)]
pub struct Arguments {
    /// Path of an explicit configuration file.
    pub config: Option<PathBuf>,
    /// Requested output form.
    pub output: Option<String>,
    /// Requested page bound.
    pub page_size: Option<String>,
    /// Opaque continuation token from a prior page.
    pub cursor: Option<String>,
    /// Idempotency key for a mutating command.
    pub operation_id: Option<String>,
    /// Deployment-supplied opaque actor reference.
    pub actor: Option<String>,
    /// Bounded closed-set reason code.
    pub reason: Option<String>,
    /// Observed optimistic version for a mutation.
    pub expected_version: Option<u64>,
    /// Client deadline.
    pub timeout: Option<String>,
    /// Validate and report without mutating.
    pub dry_run: bool,
    /// Confirm a destructive command non-interactively.
    pub yes: bool,
    /// Disable styling.
    pub no_color: bool,
    /// Target job name.
    pub job: Option<String>,
    /// Target logical instance.
    pub instance: Option<u64>,
    /// Target execution attempt.
    pub execution: Option<u64>,
    /// Target step execution.
    pub step: Option<u64>,
    /// Age bound selecting stale candidates for `execution list`.
    pub unresolved_age: Option<String>,
    /// Record family selected by `execution history`.
    pub record: Option<RecordArg>,
    /// Recovery disposition.
    pub directive: Option<DirectiveArg>,
    /// Framework failure category of a `mark-failed` directive.
    pub failure_category: Option<String>,
    /// Opaque failure identifier of a `mark-failed` directive.
    pub failure_id: Option<u64>,
    /// Hexadecimal evidence digest binding a recovery decision.
    pub evidence_digest: Option<String>,
    /// Minimum age of a purge candidate.
    pub older_than: Option<String>,
    /// Bounded purge batch size.
    pub batch: Option<String>,
    /// Terminal statuses a purge may target.
    pub status: Vec<String>,
    /// Plan digest a `retention apply` must match.
    pub plan_digest: Option<String>,
    /// Identifying job parameters supplied as `name=value`.
    pub parameters: Vec<(String, String)>,
    /// Path of a typed job-parameter file.
    pub parameters_file: Option<PathBuf>,
    /// Target path of a diagnostics bundle.
    pub out: Option<PathBuf>,
}

/// A rejected invocation.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ArgumentError {
    /// No command word was supplied.
    MissingCommand,
    /// The words did not name a command of the closed grammar.
    UnknownCommand,
    /// The option is not part of the grammar.
    UnknownOption {
        /// Rejected option, without its value.
        option: String,
    },
    /// The option is not accepted by this command.
    OptionNotAccepted {
        /// Rejected option.
        option: String,
        /// Command that does not accept it.
        command: Command,
    },
    /// The option requires a value that was not supplied.
    MissingValue {
        /// Option missing its value.
        option: String,
    },
    /// A single-valued option was supplied more than once.
    RepeatedOption {
        /// Repeated option.
        option: String,
    },
    /// The value could not be parsed as the option's type.
    InvalidValue {
        /// Option whose value was rejected.
        option: String,
    },
    /// A required option was not supplied.
    MissingRequiredOption {
        /// Missing option.
        option: String,
        /// Command that requires it.
        command: Command,
    },
    /// Two options that cannot be combined were both supplied.
    ContradictoryOptions {
        /// First option.
        first: String,
        /// Second option.
        second: String,
    },
    /// The value of a configuration option was rejected.
    ///
    /// This variant reports [`ExitCategory::ConfigurationInvalid`] because the
    /// same value can be supplied by an environment variable or a file.
    InvalidConfigurationValue {
        /// Option whose value was rejected.
        option: String,
    },
}

impl ArgumentError {
    /// Returns the exit category this rejection reports.
    #[must_use]
    pub const fn category(&self) -> ExitCategory {
        match self {
            Self::InvalidConfigurationValue { .. } => ExitCategory::ConfigurationInvalid,
            _ => ExitCategory::Usage,
        }
    }

    /// Returns the stable machine code of this rejection.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::MissingCommand => "MISSING_COMMAND",
            Self::UnknownCommand => "UNKNOWN_COMMAND",
            Self::UnknownOption { .. } => "UNKNOWN_OPTION",
            Self::OptionNotAccepted { .. } => "OPTION_NOT_ACCEPTED",
            Self::MissingValue { .. } => "MISSING_VALUE",
            Self::RepeatedOption { .. } => "REPEATED_OPTION",
            Self::InvalidValue { .. } => "INVALID_VALUE",
            Self::MissingRequiredOption { .. } => "MISSING_REQUIRED_OPTION",
            Self::ContradictoryOptions { .. } => "CONTRADICTORY_OPTIONS",
            Self::InvalidConfigurationValue { .. } => "INVALID_CONFIGURATION_VALUE",
        }
    }
}

impl fmt::Display for ArgumentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingCommand => formatter.write_str("no command was supplied"),
            Self::UnknownCommand => formatter.write_str("unknown command"),
            Self::UnknownOption { option } => write!(formatter, "unknown option {option}"),
            Self::OptionNotAccepted { option, command } => {
                write!(formatter, "{command} does not accept {option}")
            }
            Self::MissingValue { option } => write!(formatter, "{option} requires a value"),
            Self::RepeatedOption { option } => {
                write!(formatter, "{option} was supplied more than once")
            }
            Self::InvalidValue { option } => write!(formatter, "{option} has an invalid value"),
            Self::MissingRequiredOption { option, command } => {
                write!(formatter, "{command} requires {option}")
            }
            Self::ContradictoryOptions { first, second } => {
                write!(formatter, "{first} cannot be combined with {second}")
            }
            Self::InvalidConfigurationValue { option } => {
                write!(formatter, "{option} has an invalid value")
            }
        }
    }
}

impl std::error::Error for ArgumentError {}

/// One accepted option of the grammar.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Opt {
    Config,
    Output,
    PageSize,
    Cursor,
    OperationId,
    Actor,
    Reason,
    ExpectedVersion,
    Timeout,
    DryRun,
    Yes,
    NoColor,
    Job,
    Instance,
    Execution,
    Step,
    UnresolvedAge,
    Record,
    Directive,
    FailureCategory,
    FailureId,
    EvidenceDigest,
    OlderThan,
    Batch,
    Status,
    PlanDigest,
    Parameter,
    ParametersFile,
    Out,
}

impl Opt {
    /// Resolves an option name from the closed set.
    fn resolve(name: &str) -> Option<Self> {
        match name {
            "--config" => Some(Self::Config),
            "--output" => Some(Self::Output),
            "--page-size" => Some(Self::PageSize),
            "--cursor" => Some(Self::Cursor),
            "--operation-id" => Some(Self::OperationId),
            "--actor" => Some(Self::Actor),
            "--reason" => Some(Self::Reason),
            "--expected-version" => Some(Self::ExpectedVersion),
            "--timeout" => Some(Self::Timeout),
            "--dry-run" => Some(Self::DryRun),
            "--yes" => Some(Self::Yes),
            "--no-color" => Some(Self::NoColor),
            "--job" => Some(Self::Job),
            "--instance" => Some(Self::Instance),
            "--execution" => Some(Self::Execution),
            "--step" => Some(Self::Step),
            "--unresolved-age" => Some(Self::UnresolvedAge),
            "--record" => Some(Self::Record),
            "--directive" => Some(Self::Directive),
            "--failure-category" => Some(Self::FailureCategory),
            "--failure-id" => Some(Self::FailureId),
            "--evidence-digest" => Some(Self::EvidenceDigest),
            "--older-than" => Some(Self::OlderThan),
            "--batch" => Some(Self::Batch),
            "--status" => Some(Self::Status),
            "--plan-digest" => Some(Self::PlanDigest),
            "--parameter" => Some(Self::Parameter),
            "--parameters-file" => Some(Self::ParametersFile),
            "--out" => Some(Self::Out),
            _ => None,
        }
    }

    /// Returns whether the option is a flag rather than a value option.
    const fn is_flag(self) -> bool {
        matches!(self, Self::DryRun | Self::Yes | Self::NoColor)
    }

    /// Returns whether the option may be repeated.
    const fn is_repeatable(self) -> bool {
        matches!(self, Self::Status | Self::Parameter)
    }

    /// Returns the canonical option text for a diagnostic.
    const fn as_str(self) -> &'static str {
        match self {
            Self::Config => "--config",
            Self::Output => "--output",
            Self::PageSize => "--page-size",
            Self::Cursor => "--cursor",
            Self::OperationId => "--operation-id",
            Self::Actor => "--actor",
            Self::Reason => "--reason",
            Self::ExpectedVersion => "--expected-version",
            Self::Timeout => "--timeout",
            Self::DryRun => "--dry-run",
            Self::Yes => "--yes",
            Self::NoColor => "--no-color",
            Self::Job => "--job",
            Self::Instance => "--instance",
            Self::Execution => "--execution",
            Self::Step => "--step",
            Self::UnresolvedAge => "--unresolved-age",
            Self::Record => "--record",
            Self::Directive => "--directive",
            Self::FailureCategory => "--failure-category",
            Self::FailureId => "--failure-id",
            Self::EvidenceDigest => "--evidence-digest",
            Self::OlderThan => "--older-than",
            Self::Batch => "--batch",
            Self::Status => "--status",
            Self::PlanDigest => "--plan-digest",
            Self::Parameter => "--parameter",
            Self::ParametersFile => "--parameters-file",
            Self::Out => "--out",
        }
    }

    /// Returns whether the option is accepted by every command.
    const fn is_global(self) -> bool {
        matches!(
            self,
            Self::Config | Self::Output | Self::Timeout | Self::NoColor | Self::PageSize
        )
    }

    /// Returns whether `command` accepts this option.
    fn accepted_by(self, command: Command) -> bool {
        if self.is_global() {
            return true;
        }
        match self {
            Self::Cursor => command.is_paginated(),
            Self::DryRun => command.supports_dry_run(),
            Self::Yes => command.class().requires_confirmation(),
            Self::OperationId | Self::Actor => command.is_mutating(),
            Self::Reason => matches!(
                command,
                Command::ExecutionAbandon
                    | Command::ExecutionRecover
                    | Command::RetentionApply
                    | Command::RetentionHold
                    | Command::RetentionRelease
            ),
            Self::ExpectedVersion => matches!(
                command,
                Command::ExecutionStop | Command::ExecutionAbandon | Command::ExecutionRecover
            ),
            Self::Job => matches!(
                command,
                Command::JobShow
                    | Command::InstanceList
                    | Command::Launch
                    | Command::ExecutionRestart
                    | Command::RetentionPlan
                    | Command::RetentionApply
            ),
            Self::Instance => matches!(
                command,
                Command::InstanceShow
                    | Command::ExecutionList
                    | Command::ExecutionRestart
                    | Command::RetentionHold
                    | Command::RetentionRelease
            ),
            Self::Execution => matches!(
                command,
                Command::ExecutionShow
                    | Command::ExecutionSteps
                    | Command::ExecutionHistory
                    | Command::ExecutionStop
                    | Command::ExecutionAbandon
                    | Command::ExecutionRecover
                    | Command::DiagnosticsBundle
            ),
            Self::Step => matches!(command, Command::ExecutionPartitions),
            Self::UnresolvedAge => matches!(command, Command::ExecutionList),
            Self::Record => matches!(command, Command::ExecutionHistory),
            Self::Directive | Self::FailureCategory | Self::FailureId | Self::EvidenceDigest => {
                matches!(command, Command::ExecutionRecover)
            }
            Self::OlderThan | Self::Batch | Self::Status => {
                matches!(command, Command::RetentionPlan | Command::RetentionApply)
            }
            Self::PlanDigest => matches!(command, Command::RetentionApply),
            Self::Parameter | Self::ParametersFile => matches!(command, Command::Launch),
            Self::Out => matches!(command, Command::DiagnosticsBundle),
            Self::Config | Self::Output | Self::Timeout | Self::NoColor | Self::PageSize => true,
        }
    }
}

/// Parses one invocation of the closed grammar.
///
/// The words are the process arguments without the program name.
///
/// # Errors
///
/// Returns [`ArgumentError`] for an unknown command or option, a missing or
/// repeated value, a value that does not parse, or a missing required option.
pub fn parse(words: &[String]) -> Result<(Command, Arguments), ArgumentError> {
    let (command, rest) = split_command(words)?;
    let arguments = parse_options(command, rest)?;
    require_target(command, &arguments)?;
    Ok((command, arguments))
}

/// Splits the leading command words from the options.
fn split_command(words: &[String]) -> Result<(Command, &[String]), ArgumentError> {
    if words.is_empty() {
        return Err(ArgumentError::MissingCommand);
    }
    let leading: Vec<&str> = words
        .iter()
        .take(2)
        .take_while(|word| !word.starts_with('-'))
        .map(String::as_str)
        .collect();
    if leading.is_empty() {
        return Err(ArgumentError::MissingCommand);
    }
    // The two-word form is tried first so that `job list` never resolves as a
    // one-word command with a stray positional argument.
    if leading.len() == 2
        && let Some(command) = Command::resolve(&leading)
    {
        return Ok((command, &words[2..]));
    }
    Command::resolve(&leading[..1])
        .map(|command| (command, &words[1..]))
        .ok_or(ArgumentError::UnknownCommand)
}

#[allow(clippy::too_many_lines)]
fn parse_options(command: Command, words: &[String]) -> Result<Arguments, ArgumentError> {
    let mut arguments = Arguments::default();
    let mut seen: Vec<Opt> = Vec::new();
    let mut index = 0;
    while index < words.len() {
        let word = words[index].as_str();
        let (name, inline) = match word.split_once('=') {
            Some((name, value)) => (name, Some(value.to_owned())),
            None => (word, None),
        };
        let option = Opt::resolve(name).ok_or_else(|| ArgumentError::UnknownOption {
            option: name.to_owned(),
        })?;
        if !option.accepted_by(command) {
            return Err(ArgumentError::OptionNotAccepted {
                option: option.as_str().to_owned(),
                command,
            });
        }
        if !option.is_repeatable() {
            if seen.contains(&option) {
                return Err(ArgumentError::RepeatedOption {
                    option: option.as_str().to_owned(),
                });
            }
            seen.push(option);
        }
        index += 1;
        if option.is_flag() {
            if inline.is_some() {
                return Err(ArgumentError::InvalidValue {
                    option: option.as_str().to_owned(),
                });
            }
            match option {
                Opt::DryRun => arguments.dry_run = true,
                Opt::Yes => arguments.yes = true,
                Opt::NoColor => arguments.no_color = true,
                _ => {}
            }
            continue;
        }
        let value = if let Some(value) = inline {
            value
        } else {
            let value = words
                .get(index)
                .ok_or_else(|| ArgumentError::MissingValue {
                    option: option.as_str().to_owned(),
                })?
                .clone();
            index += 1;
            value
        };
        assign(&mut arguments, option, value)?;
    }
    Ok(arguments)
}

fn assign(arguments: &mut Arguments, option: Opt, value: String) -> Result<(), ArgumentError> {
    let invalid = || ArgumentError::InvalidValue {
        option: option.as_str().to_owned(),
    };
    match option {
        Opt::Config => arguments.config = Some(PathBuf::from(value)),
        Opt::Output => {
            if OutputForm::parse(&value).is_none() {
                return Err(ArgumentError::InvalidConfigurationValue {
                    option: option.as_str().to_owned(),
                });
            }
            arguments.output = Some(value);
        }
        Opt::PageSize => arguments.page_size = Some(value),
        Opt::Timeout => arguments.timeout = Some(value),
        Opt::Cursor => arguments.cursor = Some(value),
        Opt::OperationId => arguments.operation_id = Some(value),
        Opt::Actor => arguments.actor = Some(value),
        Opt::Reason => arguments.reason = Some(value),
        Opt::ExpectedVersion => {
            arguments.expected_version = Some(value.parse().map_err(|_| invalid())?);
        }
        Opt::Job => arguments.job = Some(value),
        Opt::Instance => arguments.instance = Some(value.parse().map_err(|_| invalid())?),
        Opt::Execution => arguments.execution = Some(value.parse().map_err(|_| invalid())?),
        Opt::Step => arguments.step = Some(value.parse().map_err(|_| invalid())?),
        Opt::UnresolvedAge => arguments.unresolved_age = Some(value),
        Opt::Record => {
            arguments.record = Some(RecordArg::parse(&value).ok_or_else(invalid)?);
        }
        Opt::Directive => {
            arguments.directive = Some(DirectiveArg::parse(&value).ok_or_else(invalid)?);
        }
        Opt::FailureCategory => arguments.failure_category = Some(value),
        Opt::FailureId => arguments.failure_id = Some(value.parse().map_err(|_| invalid())?),
        Opt::EvidenceDigest => arguments.evidence_digest = Some(value),
        Opt::OlderThan => arguments.older_than = Some(value),
        Opt::Batch => arguments.batch = Some(value),
        Opt::Status => arguments.status.push(value),
        Opt::PlanDigest => arguments.plan_digest = Some(value),
        Opt::Parameter => {
            let (name, parameter) = value.split_once('=').ok_or_else(invalid)?;
            arguments
                .parameters
                .push((name.to_owned(), parameter.to_owned()));
        }
        Opt::ParametersFile => arguments.parameters_file = Some(PathBuf::from(value)),
        Opt::Out => arguments.out = Some(PathBuf::from(value)),
        Opt::DryRun | Opt::Yes | Opt::NoColor => {}
    }
    Ok(())
}

/// Rejects an invocation that names no target the command requires.
fn require_target(command: Command, arguments: &Arguments) -> Result<(), ArgumentError> {
    let missing = |option: &str| ArgumentError::MissingRequiredOption {
        option: option.to_owned(),
        command,
    };
    match command {
        Command::JobShow | Command::Launch | Command::InstanceList | Command::RetentionPlan => {
            if arguments.job.is_none() {
                return Err(missing("--job"));
            }
        }
        Command::InstanceShow
        | Command::ExecutionRestart
        | Command::RetentionHold
        | Command::RetentionRelease => {
            if arguments.instance.is_none() {
                return Err(missing("--instance"));
            }
        }
        Command::ExecutionList => {
            // The stale form and the per-instance form are the two accepted
            // shapes; naming both would make the traversal ambiguous.
            match (arguments.instance, arguments.unresolved_age.as_ref()) {
                (None, None) => return Err(missing("--instance")),
                (Some(_), Some(_)) => {
                    return Err(ArgumentError::ContradictoryOptions {
                        first: "--instance".to_owned(),
                        second: "--unresolved-age".to_owned(),
                    });
                }
                _ => {}
            }
        }
        Command::ExecutionShow
        | Command::ExecutionSteps
        | Command::ExecutionHistory
        | Command::ExecutionStop
        | Command::ExecutionAbandon
        | Command::ExecutionRecover
        | Command::DiagnosticsBundle => {
            if arguments.execution.is_none() {
                return Err(missing("--execution"));
            }
            if command == Command::DiagnosticsBundle && arguments.out.is_none() {
                return Err(missing("--out"));
            }
        }
        Command::ExecutionPartitions => {
            if arguments.step.is_none() {
                return Err(missing("--step"));
            }
        }
        Command::RetentionApply => {
            if arguments.job.is_none() {
                return Err(missing("--job"));
            }
            if arguments.plan_digest.is_none() {
                return Err(missing("--plan-digest"));
            }
        }
        Command::JobList | Command::ConfigShow | Command::SchemaStatus => {}
    }
    require_mutation_fields(command, arguments)
}

/// Rejects a mutating invocation that omits a field its audit record requires.
fn require_mutation_fields(command: Command, arguments: &Arguments) -> Result<(), ArgumentError> {
    let missing = |option: &str| ArgumentError::MissingRequiredOption {
        option: option.to_owned(),
        command,
    };
    if !command.is_mutating() {
        return Ok(());
    }
    if arguments.actor.is_none() {
        return Err(missing("--actor"));
    }
    let needs_reason = matches!(
        command,
        Command::ExecutionAbandon
            | Command::ExecutionRecover
            | Command::RetentionApply
            | Command::RetentionHold
            | Command::RetentionRelease
    );
    if needs_reason && arguments.reason.is_none() {
        return Err(missing("--reason"));
    }
    let needs_version = matches!(
        command,
        Command::ExecutionStop | Command::ExecutionAbandon | Command::ExecutionRecover
    );
    if needs_version && arguments.expected_version.is_none() {
        return Err(missing("--expected-version"));
    }
    if matches!(command, Command::ExecutionRecover) {
        if arguments.directive.is_none() {
            return Err(missing("--directive"));
        }
        if arguments.evidence_digest.is_none() {
            return Err(missing("--evidence-digest"));
        }
        if matches!(arguments.directive, Some(DirectiveArg::MarkFailed)) {
            if arguments.failure_category.is_none() {
                return Err(missing("--failure-category"));
            }
            if arguments.failure_id.is_none() {
                return Err(missing("--failure-id"));
            }
        } else if arguments.failure_category.is_some() || arguments.failure_id.is_some() {
            return Err(ArgumentError::ContradictoryOptions {
                first: "--directive abandon".to_owned(),
                second: "--failure-category".to_owned(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::panic)]

    use super::{ArgumentError, Command, DirectiveArg, ExitCategory, parse};

    fn words(value: &str) -> Vec<String> {
        value.split(' ').map(str::to_owned).collect()
    }

    #[test]
    fn parses_a_paginated_read() {
        let (command, arguments) = parse(&words("execution list --instance 7 --page-size 25"))
            .expect("the invocation is valid");
        assert_eq!(command, Command::ExecutionList);
        assert_eq!(arguments.instance, Some(7));
        assert_eq!(arguments.page_size.as_deref(), Some("25"));
    }

    #[test]
    fn accepts_the_inline_value_form() {
        let (_, arguments) =
            parse(&words("execution show --execution=12")).expect("the invocation is valid");
        assert_eq!(arguments.execution, Some(12));
    }

    #[test]
    fn rejects_an_unknown_option() {
        let error = parse(&words("job list --colour")).expect_err("the option is unknown");
        assert!(matches!(error, ArgumentError::UnknownOption { .. }));
        assert_eq!(error.category(), ExitCategory::Usage);
    }

    #[test]
    fn rejects_an_option_the_command_does_not_accept() {
        let error =
            parse(&words("job list --expected-version 3")).expect_err("the option is not accepted");
        assert!(matches!(error, ArgumentError::OptionNotAccepted { .. }));
    }

    #[test]
    fn rejects_a_repeated_single_valued_option() {
        let error = parse(&words("execution show --execution 1 --execution 2"))
            .expect_err("the option is repeated");
        assert!(matches!(error, ArgumentError::RepeatedOption { .. }));
    }

    #[test]
    fn rejects_a_missing_value() {
        let error = parse(&words("execution show --execution")).expect_err("the value is missing");
        assert!(matches!(error, ArgumentError::MissingValue { .. }));
    }

    #[test]
    fn rejects_an_unknown_command() {
        assert_eq!(
            parse(&words("job delete --job orders")).expect_err("the command is unknown"),
            ArgumentError::UnknownCommand
        );
    }

    #[test]
    fn rejects_a_missing_target() {
        let error = parse(&words("execution show")).expect_err("the target is required");
        assert!(matches!(error, ArgumentError::MissingRequiredOption { .. }));
    }

    #[test]
    fn rejects_the_ambiguous_execution_list_shape() {
        let error = parse(&words("execution list --instance 1 --unresolved-age 15m"))
            .expect_err("the shapes are contradictory");
        assert!(matches!(error, ArgumentError::ContradictoryOptions { .. }));
    }

    #[test]
    fn an_invalid_output_form_is_a_configuration_error() {
        let error = parse(&words("job list --output yaml")).expect_err("the form is unknown");
        assert_eq!(error.category(), ExitCategory::ConfigurationInvalid);
    }

    #[test]
    fn recover_requires_its_evidence() {
        let error = parse(&words(
            "execution recover --execution 4 --expected-version 2 --actor ops --reason STALE \
             --directive mark-failed --evidence-digest ab --failure-category Infrastructure",
        ))
        .expect_err("the failure identifier is required");
        assert!(matches!(error, ArgumentError::MissingRequiredOption { .. }));
    }

    #[test]
    fn abandon_directive_rejects_a_stated_failure() {
        let error = parse(&words(
            "execution recover --execution 4 --expected-version 2 --actor ops --reason STALE \
             --directive abandon --evidence-digest ab --failure-category Infrastructure",
        ))
        .expect_err("an abandon directive carries no failure");
        assert!(matches!(error, ArgumentError::ContradictoryOptions { .. }));
    }

    #[test]
    fn parses_a_recovery_directive() {
        let (_, arguments) = parse(&words(
            "execution recover --execution 4 --expected-version 2 --actor ops --reason STALE \
             --directive abandon --evidence-digest ab",
        ))
        .expect("the invocation is valid");
        assert_eq!(arguments.directive, Some(DirectiveArg::Abandon));
    }

    #[test]
    fn repeatable_options_accumulate() {
        let (_, arguments) = parse(&words(
            "retention plan --job orders --status COMPLETED --status FAILED --older-than 30d",
        ))
        .expect("the invocation is valid");
        assert_eq!(arguments.status, vec!["COMPLETED", "FAILED"]);
    }

    #[test]
    fn a_mutating_command_requires_an_actor() {
        let error = parse(&words("execution stop --execution 3 --expected-version 1"))
            .expect_err("the actor is required");
        assert!(matches!(error, ArgumentError::MissingRequiredOption { .. }));
    }
}
