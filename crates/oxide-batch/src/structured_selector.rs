//! Shared deterministic structured-selection primitives.
//!
//! Nested-job parameter mapping and scoped-component late binding both use
//! these functions. Keeping selection/coercion here prevents two resolver
//! dialects from assigning different meanings to the same declaration.

use serde_json::Value;

use crate::{
    ExecutionContext, ParameterCoercion, ParameterValue, ParameterValueKind, SelectorPath,
    StateSchemaId, StateSchemaVersion,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SelectorFailure {
    SourceUnavailable,
    SourceSchemaMismatch,
    SourceTypeMismatch,
    CoercionFailed,
    InvalidValue,
}

pub(crate) enum SourceValue {
    Parameter(ParameterValue),
    Json(Value),
}

pub(crate) fn require_durable_context_source(
    durable_context_sources: bool,
) -> Result<(), SelectorFailure> {
    if durable_context_sources {
        Ok(())
    } else {
        Err(SelectorFailure::SourceUnavailable)
    }
}

pub(crate) fn context_value(
    context: &ExecutionContext,
    schema: &StateSchemaId,
    schema_version: StateSchemaVersion,
    path: &SelectorPath,
) -> Result<Option<SourceValue>, SelectorFailure> {
    if context.schema_id() != schema || context.schema_version() != schema_version {
        return Err(SelectorFailure::SourceSchemaMismatch);
    }
    let bytes = context
        .payload_json()
        .map_err(|_| SelectorFailure::InvalidValue)?;
    let value: Value = serde_json::from_slice(&bytes).map_err(|_| SelectorFailure::InvalidValue)?;
    let mut selected = &value;
    for segment in path.segments() {
        let object = selected.as_object().ok_or(SelectorFailure::InvalidValue)?;
        let Some(next) = object.get(segment) else {
            return Ok(None);
        };
        selected = next;
    }
    Ok(Some(SourceValue::Json(selected.clone())))
}

pub(crate) fn coerce(
    source: SourceValue,
    coercion: ParameterCoercion,
    expected: ParameterValueKind,
) -> Result<ParameterValue, SelectorFailure> {
    let value = match coercion {
        ParameterCoercion::Exact => exact(source, expected)?,
        ParameterCoercion::StringToI64 => {
            let value = source_string(&source)?;
            ParameterValue::from(
                value
                    .parse::<i64>()
                    .map_err(|_| SelectorFailure::CoercionFailed)?,
            )
        }
        ParameterCoercion::StringToU64 => {
            let value = source_string(&source)?;
            ParameterValue::from(
                value
                    .parse::<u64>()
                    .map_err(|_| SelectorFailure::CoercionFailed)?,
            )
        }
        ParameterCoercion::StringToBool => {
            let value = match source_string(&source)? {
                "true" => true,
                "false" => false,
                _ => return Err(SelectorFailure::CoercionFailed),
            };
            ParameterValue::from(value)
        }
        ParameterCoercion::I64ToU64 => {
            let value = source_i64(&source)?;
            ParameterValue::from(u64::try_from(value).map_err(|_| SelectorFailure::CoercionFailed)?)
        }
        ParameterCoercion::U64ToI64 => {
            let value = source_u64(&source)?;
            ParameterValue::from(i64::try_from(value).map_err(|_| SelectorFailure::CoercionFailed)?)
        }
        _ => return Err(SelectorFailure::CoercionFailed),
    };
    if value.kind() != expected {
        return Err(SelectorFailure::SourceTypeMismatch);
    }
    Ok(value)
}

fn exact(
    source: SourceValue,
    expected: ParameterValueKind,
) -> Result<ParameterValue, SelectorFailure> {
    match source {
        SourceValue::Parameter(value) if value.kind() == expected => Ok(value),
        SourceValue::Parameter(_) => Err(SelectorFailure::SourceTypeMismatch),
        SourceValue::Json(value) => match expected {
            ParameterValueKind::String => value
                .as_str()
                .ok_or(SelectorFailure::SourceTypeMismatch)
                .and_then(|value| {
                    ParameterValue::string(value.to_owned())
                        .map_err(|_| SelectorFailure::InvalidValue)
                }),
            ParameterValueKind::I64 => value
                .as_i64()
                .map(ParameterValue::from)
                .ok_or(SelectorFailure::SourceTypeMismatch),
            ParameterValueKind::U64 => value
                .as_u64()
                .map(ParameterValue::from)
                .ok_or(SelectorFailure::SourceTypeMismatch),
            ParameterValueKind::Bool => value
                .as_bool()
                .map(ParameterValue::from)
                .ok_or(SelectorFailure::SourceTypeMismatch),
            _ => Err(SelectorFailure::SourceTypeMismatch),
        },
    }
}

fn source_string(source: &SourceValue) -> Result<&str, SelectorFailure> {
    match source {
        SourceValue::Parameter(value) => value.as_str().ok_or(SelectorFailure::SourceTypeMismatch),
        SourceValue::Json(value) => value.as_str().ok_or(SelectorFailure::SourceTypeMismatch),
    }
}

fn source_i64(source: &SourceValue) -> Result<i64, SelectorFailure> {
    match source {
        SourceValue::Parameter(value) => value.as_i64().ok_or(SelectorFailure::SourceTypeMismatch),
        SourceValue::Json(value) => value.as_i64().ok_or(SelectorFailure::SourceTypeMismatch),
    }
}

fn source_u64(source: &SourceValue) -> Result<u64, SelectorFailure> {
    match source {
        SourceValue::Parameter(value) => value.as_u64().ok_or(SelectorFailure::SourceTypeMismatch),
        SourceValue::Json(value) => value.as_u64().ok_or(SelectorFailure::SourceTypeMismatch),
    }
}

pub(crate) fn default_value(kind: ParameterValueKind) -> Result<ParameterValue, SelectorFailure> {
    match kind {
        ParameterValueKind::String => {
            ParameterValue::string(String::new()).map_err(|_| SelectorFailure::InvalidValue)
        }
        ParameterValueKind::I64 => Ok(ParameterValue::from(0_i64)),
        ParameterValueKind::U64 => Ok(ParameterValue::from(0_u64)),
        ParameterValueKind::Bool => Ok(ParameterValue::from(false)),
        _ => Err(SelectorFailure::InvalidValue),
    }
}

pub(crate) fn digest_hex(digest: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use crate::{ExecutionContext, SelectorPath, StateLimits, StateSchemaId, StateSchemaVersion};

    use super::{SelectorFailure, context_value, require_durable_context_source};

    #[test]
    fn unavailable_durable_context_source_fails_closed() {
        assert_eq!(
            require_durable_context_source(false),
            Err(SelectorFailure::SourceUnavailable)
        );
        assert_eq!(require_durable_context_source(true), Ok(()));
    }

    #[test]
    fn malformed_context_shape_is_not_treated_as_a_missing_value() {
        let malformed = ExecutionContext::from_json(
            br#"{"format":"oxide-batch.execution-context","format_version":1,"schema":"scope.v1","schema_version":1,"payload":{"tenant":"scalar"}}"#,
            StateLimits::default(),
        )
        .expect("context");
        let nested =
            SelectorPath::new([String::from("tenant"), String::from("name")]).expect("path");
        assert!(matches!(
            context_value(
                &malformed,
                &StateSchemaId::new("scope.v1").expect("schema"),
                StateSchemaVersion::new(1).expect("version"),
                &nested,
            ),
            Err(SelectorFailure::InvalidValue)
        ));

        let missing = SelectorPath::new([String::from("missing")]).expect("path");
        assert!(matches!(
            context_value(
                &malformed,
                &StateSchemaId::new("scope.v1").expect("schema"),
                StateSchemaVersion::new(1).expect("version"),
                &missing,
            ),
            Ok(None)
        ));
    }

    #[test]
    fn context_schema_and_version_mismatch_fail_closed() {
        let context = ExecutionContext::from_json(
            br#"{"format":"oxide-batch.execution-context","format_version":1,"schema":"scope.v1","schema_version":1,"payload":{"tenant":"acme"}}"#,
            StateLimits::default(),
        )
        .expect("context");
        let path = SelectorPath::new([String::from("tenant")]).expect("path");

        assert!(matches!(
            context_value(
                &context,
                &StateSchemaId::new("scope.v2").expect("schema"),
                StateSchemaVersion::new(1).expect("version"),
                &path,
            ),
            Err(SelectorFailure::SourceSchemaMismatch)
        ));
        assert!(matches!(
            context_value(
                &context,
                &StateSchemaId::new("scope.v1").expect("schema"),
                StateSchemaVersion::new(2).expect("version"),
                &path,
            ),
            Err(SelectorFailure::SourceSchemaMismatch)
        ));
    }
}
