use std::error::Error;
use std::fmt;

/// The kind of validated domain name.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum NameKind {
    /// A job definition name.
    Job,
    /// A step definition name.
    Step,
    /// A job-parameter name.
    Parameter,
    /// An exit-status code.
    ExitCode,
}

impl fmt::Display for NameKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Job => "job name",
            Self::Step => "step name",
            Self::Parameter => "parameter name",
            Self::ExitCode => "exit code",
        })
    }
}

/// The kind of opaque numeric identifier.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum IdentifierKind {
    /// A job-instance identifier.
    JobInstance,
    /// A job-execution identifier.
    JobExecution,
    /// A step-execution identifier.
    StepExecution,
    /// An opaque failure identifier.
    Failure,
}

impl fmt::Display for IdentifierKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::JobInstance => "job instance",
            Self::JobExecution => "job execution",
            Self::StepExecution => "step execution",
            Self::Failure => "failure",
        })
    }
}

/// A stable, value-redacted domain validation failure.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DomainError {
    /// A required name was empty.
    EmptyName {
        /// The name category.
        kind: NameKind,
    },
    /// A name exceeded its UTF-8 byte limit.
    NameTooLong {
        /// The name category.
        kind: NameKind,
        /// The maximum accepted UTF-8 byte length.
        max_bytes: usize,
    },
    /// A name had leading or trailing whitespace.
    NameHasSurroundingWhitespace {
        /// The name category.
        kind: NameKind,
    },
    /// A name contained a control character.
    NameContainsControl {
        /// The name category.
        kind: NameKind,
        /// The zero-based character position, without disclosing the character.
        character_index: usize,
    },
    /// A numeric identifier was zero.
    ZeroIdentifier {
        /// The identifier category.
        kind: IdentifierKind,
    },
    /// A parameter with the same name was inserted more than once.
    DuplicateParameter,
    /// A string parameter exceeded its UTF-8 byte limit.
    ParameterStringTooLong {
        /// The maximum accepted UTF-8 byte length.
        max_bytes: usize,
    },
    /// An execution timestamp preceded a timestamp that must come before it.
    InvalidTimestampOrder,
    /// A running execution included an end timestamp.
    ActiveExecutionHasEndTime,
    /// A finished execution omitted its end timestamp.
    FinishedExecutionMissingEndTime,
    /// A failed execution omitted its redacted failure summary.
    FailedExecutionMissingFailure,
}

impl fmt::Display for DomainError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyName { kind } => write!(formatter, "{kind} must not be empty"),
            Self::NameTooLong { kind, max_bytes } => {
                write!(formatter, "{kind} exceeds {max_bytes} UTF-8 bytes")
            }
            Self::NameHasSurroundingWhitespace { kind } => {
                write!(formatter, "{kind} has surrounding whitespace")
            }
            Self::NameContainsControl {
                kind,
                character_index,
            } => write!(
                formatter,
                "{kind} contains a control character at position {character_index}"
            ),
            Self::ZeroIdentifier { kind } => write!(formatter, "{kind} identifier must be nonzero"),
            Self::DuplicateParameter => formatter.write_str("job parameter names must be unique"),
            Self::ParameterStringTooLong { max_bytes } => {
                write!(
                    formatter,
                    "string parameter exceeds {max_bytes} UTF-8 bytes"
                )
            }
            Self::InvalidTimestampOrder => {
                formatter.write_str("execution timestamps are out of order")
            }
            Self::ActiveExecutionHasEndTime => {
                formatter.write_str("an active execution cannot have an end timestamp")
            }
            Self::FinishedExecutionMissingEndTime => {
                formatter.write_str("a finished execution requires an end timestamp")
            }
            Self::FailedExecutionMissingFailure => {
                formatter.write_str("a failed execution requires a failure summary")
            }
        }
    }
}

impl Error for DomainError {}
