//! The bounded operator request envelope shared by every audited action.
//!
//! The envelope validates bounded closed-charset references, classifies the
//! authorization a deployment must grant, and computes the canonical request
//! digest that makes one operation identifier replayable. It never
//! authenticates a caller, never accepts a credential, and never treats the
//! supplied actor reference as proof of authorization.

use std::error::Error;
use std::fmt;

use sha2::{Digest, Sha256};

use oxide_batch_core::{DefinitionIdentity, ExecutionVersion};

use crate::RecoveryDirective;

/// Maximum accepted UTF-8 bytes of an opaque actor reference.
pub const MAX_ACTOR_REF_BYTES: usize = 128;
/// Maximum accepted UTF-8 bytes of a closed-set reason code.
pub const MAX_REASON_CODE_BYTES: usize = 64;
/// Maximum accepted UTF-8 bytes of a caller-supplied idempotency key.
pub const MAX_OPERATION_ID_BYTES: usize = 64;

/// A mutating action a deployment authorizes and the core guards.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum OperatorAction {
    /// Create the instance when required and one `STARTING` execution.
    Launch,
    /// Create another execution attempt from the committed checkpoint.
    Restart,
    /// Durably record a cooperative stop request.
    Stop,
    /// Make a stopped, failed, or recovered execution permanently terminal.
    Abandon,
    /// Append one evidence-bound recovery decision and apply its result.
    Recover,
}

impl OperatorAction {
    /// Returns the stable durable code for this action.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Launch => "LAUNCH",
            Self::Restart => "RESTART",
            Self::Stop => "STOP",
            Self::Abandon => "ABANDON",
            Self::Recover => "RECOVER",
        }
    }

    /// Returns the class a deployment authorizes separately.
    #[must_use]
    pub const fn authorization_class(self) -> AuthorizationClass {
        match self {
            Self::Launch | Self::Restart | Self::Stop => AuthorizationClass::Lifecycle,
            Self::Abandon | Self::Recover => AuthorizationClass::Destructive,
        }
    }
}

impl fmt::Display for OperatorAction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// The separately authorizable class of a service call.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum AuthorizationClass {
    /// Every explorer query and every retention plan.
    Read,
    /// Launch, restart, and stop.
    Lifecycle,
    /// Abandon, recover, hold, hold release, and purge application.
    Destructive,
}

impl AuthorizationClass {
    /// Returns the stable durable code for this class.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Read => "READ",
            Self::Lifecycle => "LIFECYCLE",
            Self::Destructive => "DESTRUCTIVE",
        }
    }
}

impl fmt::Display for AuthorizationClass {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

macro_rules! bounded_reference {
    (
        $(#[$meta:meta])*
        $name:ident, $field:expr, $max:expr, $allowed:expr
    ) => {
        $(#[$meta])*
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(String);

        impl $name {
            /// Validates a bounded closed-charset reference.
            ///
            /// # Errors
            ///
            /// Returns [`RequestFieldError`] when the value is empty, exceeds
            /// its byte bound, or contains a character outside the closed set.
            pub fn new(value: impl Into<String>) -> Result<Self, RequestFieldError> {
                let value = value.into();
                validate_reference(&value, $field, $max, $allowed)?;
                Ok(Self(value))
            }

            /// Borrows the validated reference.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

bounded_reference!(
    /// Deployment-supplied opaque reference to the authorized caller.
    ///
    /// The core never authenticates this value and never treats it as proof of
    /// authorization. It is an audit correlation, never a credential.
    ActorRef,
    RequestField::ActorRef,
    MAX_ACTOR_REF_BYTES,
    is_actor_character
);

bounded_reference!(
    /// Bounded closed-set machine reason code.
    ///
    /// Reason codes are uppercase machine vocabulary rather than operator
    /// prose, so audit records contain no free text.
    ReasonCode,
    RequestField::ReasonCode,
    MAX_REASON_CODE_BYTES,
    is_reason_character
);

bounded_reference!(
    /// Caller-supplied idempotency key for one mutating action.
    OperationId,
    RequestField::OperationId,
    MAX_OPERATION_ID_BYTES,
    is_operation_character
);

const fn is_actor_character(value: char) -> bool {
    value.is_ascii_alphanumeric() || matches!(value, '.' | '_' | ':' | '@' | '-')
}

const fn is_reason_character(value: char) -> bool {
    value.is_ascii_uppercase() || value.is_ascii_digit() || value == '_'
}

const fn is_operation_character(value: char) -> bool {
    value.is_ascii_alphanumeric() || matches!(value, '.' | '_' | ':' | '-')
}

fn validate_reference(
    value: &str,
    field: RequestField,
    max_bytes: usize,
    allowed: fn(char) -> bool,
) -> Result<(), RequestFieldError> {
    if value.is_empty() {
        return Err(RequestFieldError::Empty { field });
    }
    if value.len() > max_bytes {
        return Err(RequestFieldError::TooLong { field, max_bytes });
    }
    if !value.chars().all(allowed) {
        return Err(RequestFieldError::InvalidCharacter { field });
    }
    Ok(())
}

/// A bounded request-envelope field category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RequestField {
    /// Opaque authorized-caller reference.
    ActorRef,
    /// Closed-set machine reason code.
    ReasonCode,
    /// Caller-supplied idempotency key.
    OperationId,
}

impl RequestField {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ActorRef => "actor reference",
            Self::ReasonCode => "reason code",
            Self::OperationId => "operation identifier",
        }
    }
}

/// An invalid bounded request-envelope field.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RequestFieldError {
    /// The field was empty.
    Empty {
        /// Rejected field.
        field: RequestField,
    },
    /// The field exceeded its UTF-8 byte bound.
    TooLong {
        /// Rejected field.
        field: RequestField,
        /// Maximum accepted UTF-8 bytes.
        max_bytes: usize,
    },
    /// The field contained a character outside its closed set.
    InvalidCharacter {
        /// Rejected field.
        field: RequestField,
    },
}

impl fmt::Display for RequestFieldError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty { field } => write!(formatter, "{} must not be empty", field.as_str()),
            Self::TooLong { field, max_bytes } => {
                write!(formatter, "{} exceeds {max_bytes} bytes", field.as_str())
            }
            Self::InvalidCharacter { field } => write!(
                formatter,
                "{} contains an unaccepted character",
                field.as_str()
            ),
        }
    }
}

impl Error for RequestFieldError {}

/// A framework-computed SHA-256 digest of one canonical request.
///
/// The digest covers the action, target identity, expected version, and
/// bounded arguments. It never covers the actor reference, so replaying an
/// operation identifier from a different authorized caller is still a replay
/// of the same request rather than a conflict.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestDigest([u8; 32]);

impl RequestDigest {
    /// Reconstructs a digest recorded by a repository.
    #[must_use]
    pub const fn from_bytes(value: [u8; 32]) -> Self {
        Self(value)
    }

    /// Returns the raw digest bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Returns the lowercase hexadecimal encoding of the digest.
    #[must_use]
    pub fn to_hex(&self) -> String {
        hex_digest(&self.0)
    }
}

impl fmt::Debug for RequestDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("RequestDigest")
            .field(&self.to_hex())
            .finish()
    }
}

impl fmt::Display for RequestDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_hex())
    }
}

/// Renders bytes as their lowercase hexadecimal encoding.
#[doc(hidden)]
#[must_use]
pub fn hex_digest(value: &[u8]) -> String {
    let mut encoded = String::with_capacity(value.len() * 2);
    for byte in value {
        // Hexadecimal formatting of a byte cannot fail on a `String`.
        let _ = fmt::Write::write_fmt(&mut encoded, format_args!("{byte:02x}"));
    }
    encoded
}

/// Deterministic canonical encoder for digest inputs.
#[derive(Default)]
pub(crate) struct CanonicalWriter {
    bytes: Vec<u8>,
}

impl CanonicalWriter {
    pub(crate) fn new(tag: &str) -> Self {
        let mut writer = Self::default();
        writer.push_bytes(tag.as_bytes());
        writer
    }

    pub(crate) fn push_bytes(&mut self, value: &[u8]) {
        let length = u64::try_from(value.len()).unwrap_or(u64::MAX);
        self.bytes.extend_from_slice(&length.to_be_bytes());
        self.bytes.extend_from_slice(value);
    }

    pub(crate) fn push_str(&mut self, value: &str) {
        self.push_bytes(value.as_bytes());
    }

    pub(crate) fn push_u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub(crate) fn push_optional_u64(&mut self, value: Option<u64>) {
        match value {
            Some(value) => {
                self.bytes.push(1);
                self.push_u64(value);
            }
            None => self.bytes.push(0),
        }
    }

    pub(crate) fn digest(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(&self.bytes);
        hasher.finalize().into()
    }
}

/// Bounded arguments of one operator action that participate in its digest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RequestArguments {
    Definition(Box<DefinitionIdentity>),
    None,
    Recovery {
        directive: RecoveryDirective,
        evidence_digest: [u8; 32],
        unknown_commit: bool,
    },
}

pub(crate) fn request_digest(
    action: OperatorAction,
    target: &str,
    expected_version: Option<ExecutionVersion>,
    reason: Option<&ReasonCode>,
    arguments: &RequestArguments,
) -> RequestDigest {
    let mut writer = CanonicalWriter::new("oxide-batch.operator-request.v1");
    writer.push_str(action.as_str());
    writer.push_str(target);
    writer.push_optional_u64(expected_version.map(ExecutionVersion::get));
    writer.push_str(reason.map_or("", ReasonCode::as_str));
    match arguments {
        RequestArguments::None => writer.push_str("NONE"),
        RequestArguments::Definition(definition) => {
            writer.push_str("DEFINITION");
            writer.push_str(definition.revision().as_str());
            writer.push_bytes(definition.manifest_digest());
        }
        RequestArguments::Recovery {
            directive,
            evidence_digest,
            unknown_commit,
        } => {
            writer.push_str("RECOVERY");
            writer.push_str(directive.disposition().resulting_status().as_str());
            writer.push_bytes(evidence_digest);
            writer.push_u64(u64::from(*unknown_commit));
            match directive.failure() {
                Some(failure) => {
                    writer.push_str(failure.category().as_str());
                    writer.push_u64(failure.failure_id().get());
                }
                None => writer.push_str(""),
            }
        }
    }
    RequestDigest::from_bytes(writer.digest())
}
