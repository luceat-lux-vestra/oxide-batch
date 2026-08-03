use std::borrow::Borrow;
use std::fmt;
use std::num::NonZeroU64;

use super::{DomainError, IdentifierKind, NameKind};

const MAX_DOMAIN_NAME_BYTES: usize = 128;
const MAX_PARAMETER_NAME_BYTES: usize = 128;
const MAX_EXIT_CODE_BYTES: usize = 64;

fn validate_name(value: &str, kind: NameKind, max_bytes: usize) -> Result<(), DomainError> {
    if value.is_empty() {
        return Err(DomainError::EmptyName { kind });
    }
    if value.len() > max_bytes {
        return Err(DomainError::NameTooLong { kind, max_bytes });
    }
    if value.trim() != value {
        return Err(DomainError::NameHasSurroundingWhitespace { kind });
    }
    if let Some((character_index, _)) = value
        .chars()
        .enumerate()
        .find(|(_, character)| character.is_control())
    {
        return Err(DomainError::NameContainsControl {
            kind,
            character_index,
        });
    }
    Ok(())
}

macro_rules! domain_name {
    ($name:ident, $kind:expr, $max:expr, $docs:literal, $redact_debug:expr) => {
        #[doc = $docs]
        #[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(String);

        impl $name {
            /// Validates and constructs the name.
            ///
            /// # Errors
            ///
            /// Returns [`DomainError`] when the value is empty, too long, has
            /// surrounding whitespace, or contains a control character.
            pub fn new(value: impl Into<String>) -> Result<Self, DomainError> {
                let value = value.into();
                validate_name(&value, $kind, $max)?;
                Ok(Self(value))
            }

            /// Borrows the validated value.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Returns the validated value.
            #[must_use]
            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                if $redact_debug {
                    formatter.write_str(concat!(stringify!($name), "(<redacted>)"))
                } else {
                    formatter
                        .debug_tuple(stringify!($name))
                        .field(&self.0)
                        .finish()
                }
            }
        }

        impl TryFrom<String> for $name {
            type Error = DomainError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl TryFrom<&str> for $name {
            type Error = DomainError;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl Borrow<str> for $name {
            fn borrow(&self) -> &str {
                self.as_str()
            }
        }
    };
}

domain_name!(
    JobName,
    NameKind::Job,
    MAX_DOMAIN_NAME_BYTES,
    "A validated logical job-definition name.",
    false
);
domain_name!(
    StepName,
    NameKind::Step,
    MAX_DOMAIN_NAME_BYTES,
    "A validated logical step-definition name.",
    false
);
domain_name!(
    ParameterName,
    NameKind::Parameter,
    MAX_PARAMETER_NAME_BYTES,
    "A validated job-parameter name.\n\nIts `Debug` representation is redacted because parameter metadata is sensitive by default.",
    true
);
domain_name!(
    ExitCode,
    NameKind::ExitCode,
    MAX_EXIT_CODE_BYTES,
    "A validated flow- and operator-facing exit code.",
    false
);

impl ExitCode {
    pub(crate) fn framework_owned(value: &'static str) -> Self {
        Self(String::from(value))
    }
}

macro_rules! opaque_id {
    ($name:ident, $kind:expr, $docs:literal) => {
        #[doc = $docs]
        #[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(NonZeroU64);

        impl $name {
            /// Constructs a nonzero opaque identifier.
            ///
            /// # Errors
            ///
            /// Returns [`DomainError::ZeroIdentifier`] for zero.
            pub fn new(value: u64) -> Result<Self, DomainError> {
                NonZeroU64::new(value)
                    .map(Self)
                    .ok_or(DomainError::ZeroIdentifier { kind: $kind })
            }

            /// Returns the underlying nonzero numeric value.
            #[must_use]
            pub const fn get(self) -> u64 {
                self.0.get()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.get().fmt(formatter)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_tuple(stringify!($name))
                    .field(&self.get())
                    .finish()
            }
        }

        impl TryFrom<u64> for $name {
            type Error = DomainError;

            fn try_from(value: u64) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl From<$name> for u64 {
            fn from(value: $name) -> Self {
                value.get()
            }
        }
    };
}

opaque_id!(
    JobInstanceId,
    IdentifierKind::JobInstance,
    "An opaque identifier for one logical job instance."
);
opaque_id!(
    JobExecutionId,
    IdentifierKind::JobExecution,
    "An opaque identifier for one job launch or restart attempt."
);
opaque_id!(
    StepExecutionId,
    IdentifierKind::StepExecution,
    "An opaque identifier for one step attempt."
);
opaque_id!(
    RecoveryDecisionId,
    IdentifierKind::RecoveryDecision,
    "An opaque identifier for one append-only recovery decision."
);
opaque_id!(
    OperatorRequestId,
    IdentifierKind::OperatorRequest,
    "An opaque identifier for one append-only operator request record."
);
opaque_id!(
    RetentionActionId,
    IdentifierKind::RetentionAction,
    "An opaque identifier for one append-only retention audit record."
);
opaque_id!(
    StepPartitionId,
    IdentifierKind::StepPartition,
    "An opaque identifier for one durable step partition."
);
opaque_id!(
    FailureId,
    IdentifierKind::Failure,
    "An opaque identifier used to correlate a redacted failure."
);
