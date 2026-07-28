use std::collections::BTreeMap;
use std::fmt;

use super::{DomainError, JobName, ParameterName};

const MAX_PARAMETER_STRING_BYTES: usize = 64 * 1024;

/// The stable type discriminator for a [`ParameterValue`].
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum ParameterValueKind {
    /// A UTF-8 string.
    String,
    /// A signed 64-bit integer.
    I64,
    /// An unsigned 64-bit integer.
    U64,
    /// A boolean.
    Bool,
}

/// A bounded typed job-parameter value.
///
/// `Debug` and `Display` intentionally redact the underlying value. Use the
/// typed accessors only at an application or persistence boundary that is
/// authorized to consume the parameter.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ParameterValue(ParameterValueInner);

#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum ParameterValueInner {
    String(String),
    I64(i64),
    U64(u64),
    Bool(bool),
}

impl ParameterValue {
    /// Validates and constructs a string parameter.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::ParameterStringTooLong`] when the UTF-8 value is
    /// larger than 64 KiB.
    pub fn string(value: impl Into<String>) -> Result<Self, DomainError> {
        let value = value.into();
        if value.len() > MAX_PARAMETER_STRING_BYTES {
            return Err(DomainError::ParameterStringTooLong {
                max_bytes: MAX_PARAMETER_STRING_BYTES,
            });
        }
        Ok(Self(ParameterValueInner::String(value)))
    }

    /// Returns the stable type discriminator.
    #[must_use]
    pub const fn kind(&self) -> ParameterValueKind {
        match self {
            Self(ParameterValueInner::String(_)) => ParameterValueKind::String,
            Self(ParameterValueInner::I64(_)) => ParameterValueKind::I64,
            Self(ParameterValueInner::U64(_)) => ParameterValueKind::U64,
            Self(ParameterValueInner::Bool(_)) => ParameterValueKind::Bool,
        }
    }

    /// Borrows the string value when this is a string parameter.
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self(ParameterValueInner::String(value)) => Some(value),
            _ => None,
        }
    }

    /// Returns the signed integer when this is an `i64` parameter.
    #[must_use]
    pub const fn as_i64(&self) -> Option<i64> {
        match self {
            Self(ParameterValueInner::I64(value)) => Some(*value),
            _ => None,
        }
    }

    /// Returns the unsigned integer when this is a `u64` parameter.
    #[must_use]
    pub const fn as_u64(&self) -> Option<u64> {
        match self {
            Self(ParameterValueInner::U64(value)) => Some(*value),
            _ => None,
        }
    }

    /// Returns the boolean when this is a boolean parameter.
    #[must_use]
    pub const fn as_bool(&self) -> Option<bool> {
        match self {
            Self(ParameterValueInner::Bool(value)) => Some(*value),
            _ => None,
        }
    }
}

impl From<i64> for ParameterValue {
    fn from(value: i64) -> Self {
        Self(ParameterValueInner::I64(value))
    }
}

impl TryFrom<String> for ParameterValue {
    type Error = DomainError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::string(value)
    }
}

impl TryFrom<&str> for ParameterValue {
    type Error = DomainError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::string(value)
    }
}

impl From<u64> for ParameterValue {
    fn from(value: u64) -> Self {
        Self(ParameterValueInner::U64(value))
    }
}

impl From<bool> for ParameterValue {
    fn from(value: bool) -> Self {
        Self(ParameterValueInner::Bool(value))
    }
}

impl fmt::Debug for ParameterValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple(match self.kind() {
                ParameterValueKind::String => "String",
                ParameterValueKind::I64 => "I64",
                ParameterValueKind::U64 => "U64",
                ParameterValueKind::Bool => "Bool",
            })
            .field(&Redacted)
            .finish()
    }
}

impl fmt::Display for ParameterValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted>")
    }
}

struct Redacted;

impl fmt::Debug for Redacted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted>")
    }
}

/// Whether a job parameter participates in job-instance identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ParameterRole {
    /// The name, type, and value participate in job-instance identity.
    Identifying,
    /// The parameter is launch metadata and does not select the job instance.
    NonIdentifying,
}

/// One typed job parameter and its identity role.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct JobParameter {
    value: ParameterValue,
    role: ParameterRole,
}

impl JobParameter {
    /// Constructs a typed job parameter.
    #[must_use]
    pub const fn new(value: ParameterValue, role: ParameterRole) -> Self {
        Self { value, role }
    }

    /// Borrows the typed value.
    #[must_use]
    pub const fn value(&self) -> &ParameterValue {
        &self.value
    }

    /// Returns the identity role.
    #[must_use]
    pub const fn role(&self) -> ParameterRole {
        self.role
    }

    /// Returns whether the parameter participates in instance identity.
    #[must_use]
    pub const fn is_identifying(&self) -> bool {
        matches!(self.role, ParameterRole::Identifying)
    }
}

impl fmt::Debug for JobParameter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JobParameter")
            .field("kind", &self.value.kind())
            .field("role", &self.role)
            .field("value", &Redacted)
            .finish()
    }
}

/// A deterministically ordered set of typed job parameters.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct JobParameters {
    values: BTreeMap<ParameterName, JobParameter>,
}

impl JobParameters {
    /// Constructs an empty parameter set.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            values: BTreeMap::new(),
        }
    }

    /// Inserts a parameter without silently replacing an existing name.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::DuplicateParameter`] when the name is already
    /// present.
    pub fn insert(
        &mut self,
        name: ParameterName,
        parameter: JobParameter,
    ) -> Result<(), DomainError> {
        if self.values.contains_key(&name) {
            return Err(DomainError::DuplicateParameter);
        }
        self.values.insert(name, parameter);
        Ok(())
    }

    /// Builds a parameter set while rejecting duplicate names.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::DuplicateParameter`] when an input name occurs
    /// more than once.
    pub fn try_from_iter(
        parameters: impl IntoIterator<Item = (ParameterName, JobParameter)>,
    ) -> Result<Self, DomainError> {
        let mut result = Self::new();
        for (name, parameter) in parameters {
            result.insert(name, parameter)?;
        }
        Ok(result)
    }

    /// Returns a parameter by its validated name.
    #[must_use]
    pub fn get(&self, name: &ParameterName) -> Option<&JobParameter> {
        self.values.get(name)
    }

    /// Iterates in canonical name order.
    #[must_use]
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&ParameterName, &JobParameter)> {
        self.values.iter()
    }

    /// Returns the number of parameters.
    #[must_use]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns whether no parameters are present.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Returns the number of parameters that participate in identity.
    #[must_use]
    pub fn identifying_len(&self) -> usize {
        self.values
            .values()
            .filter(|parameter| parameter.is_identifying())
            .count()
    }
}

impl fmt::Debug for JobParameters {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JobParameters")
            .field("parameter_count", &self.len())
            .field("identifying_count", &self.identifying_len())
            .finish_non_exhaustive()
    }
}

/// The canonical identity key for a logical job instance.
///
/// Parameter entries are ordered by validated name, retain their value type,
/// and include only parameters marked [`ParameterRole::Identifying`].
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct JobInstanceKey {
    job_name: JobName,
    identifying_parameters: BTreeMap<ParameterName, ParameterValue>,
}

impl JobInstanceKey {
    /// Constructs the canonical key for a named job and parameter set.
    #[must_use]
    pub fn new(job_name: JobName, parameters: &JobParameters) -> Self {
        let identifying_parameters = parameters
            .iter()
            .filter(|(_, parameter)| parameter.is_identifying())
            .map(|(name, parameter)| (name.clone(), parameter.value().clone()))
            .collect();

        Self {
            job_name,
            identifying_parameters,
        }
    }

    /// Borrows the logical job name.
    #[must_use]
    pub const fn job_name(&self) -> &JobName {
        &self.job_name
    }

    /// Returns the number of identifying parameters.
    #[must_use]
    pub fn identifying_parameter_count(&self) -> usize {
        self.identifying_parameters.len()
    }

    /// Returns an identifying value for authorized application or persistence
    /// use.
    #[must_use]
    pub fn identifying_value(&self, name: &ParameterName) -> Option<&ParameterValue> {
        self.identifying_parameters.get(name)
    }

    /// Iterates over identifying parameter names and value kinds in canonical
    /// order without exposing their values.
    #[must_use]
    pub fn identifying_fields(
        &self,
    ) -> impl ExactSizeIterator<Item = (&ParameterName, ParameterValueKind)> {
        self.identifying_parameters
            .iter()
            .map(|(name, value)| (name, value.kind()))
    }
}

impl fmt::Debug for JobInstanceKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JobInstanceKey")
            .field("job_name", &self.job_name)
            .field(
                "identifying_parameter_count",
                &self.identifying_parameter_count(),
            )
            .finish_non_exhaustive()
    }
}
