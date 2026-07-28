//! Versioned execution-context spike.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

const FORMAT: &str = "oxide-batch.execution-context";
const FORMAT_VERSION: u32 = 1;

/// Resource limits applied before application payload decoding.
#[derive(Clone, Copy, Debug)]
pub struct ContextLimits {
    /// Maximum serialized bytes.
    pub maximum_bytes: usize,
    /// Maximum JSON object/array nesting, including the root.
    pub maximum_depth: usize,
}

impl Default for ContextLimits {
    fn default() -> Self {
        Self {
            maximum_bytes: 64 * 1024,
            maximum_depth: 16,
        }
    }
}

/// Stable, data-redacted execution-context failures.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ContextError {
    /// The serialized value exceeded the configured byte limit.
    #[error("execution context exceeds its byte limit")]
    TooLarge,
    /// The JSON value exceeded the configured nesting limit.
    #[error("execution context exceeds its nesting limit")]
    TooDeep,
    /// The bytes were not a valid supported envelope.
    #[error("execution context is malformed")]
    Malformed,
    /// The framework envelope version is newer than this runtime.
    #[error("execution context format version is unsupported")]
    UnsupportedFormatVersion,
    /// The application schema identifier does not match the requested codec.
    #[error("execution context schema does not match the job")]
    SchemaMismatch,
    /// The application schema version cannot be upgraded by the codec.
    #[error("execution context schema version is unsupported")]
    UnsupportedSchemaVersion,
    /// The application payload did not satisfy its versioned schema.
    #[error("execution context payload is invalid")]
    InvalidPayload,
}

#[derive(Debug, Deserialize, Serialize)]
struct Envelope {
    format: String,
    format_version: u32,
    schema: String,
    schema_version: u32,
    payload: Value,
}

/// An application-owned schema and explicit upgrade path.
pub trait ContextSchema {
    /// The current typed form returned to application code.
    type Current: DeserializeOwned + Serialize;

    /// Returns the stable application/job schema identifier.
    fn schema_id(&self) -> &'static str;

    /// Returns the current application payload version.
    fn current_version(&self) -> u32;

    /// Decodes or upgrades one supported payload version.
    ///
    /// # Errors
    ///
    /// Returns a classified context error when the version is unsupported or
    /// the payload does not satisfy that version's schema.
    fn decode_version(&self, version: u32, payload: Value) -> Result<Self::Current, ContextError>;
}

/// Reads and upgrades a bounded execution context.
///
/// # Errors
///
/// Returns a classified context error for resource-limit, envelope, version,
/// schema, or typed-payload failures.
pub fn decode_context<S: ContextSchema>(
    bytes: &[u8],
    schema: &S,
    limits: ContextLimits,
) -> Result<S::Current, ContextError> {
    if bytes.len() > limits.maximum_bytes {
        return Err(ContextError::TooLarge);
    }

    let value: Value = serde_json::from_slice(bytes).map_err(|_| ContextError::Malformed)?;
    if json_depth(&value) > limits.maximum_depth {
        return Err(ContextError::TooDeep);
    }

    let envelope: Envelope = serde_json::from_value(value).map_err(|_| ContextError::Malformed)?;
    if envelope.format != FORMAT || envelope.format_version != FORMAT_VERSION {
        return Err(ContextError::UnsupportedFormatVersion);
    }
    if envelope.schema != schema.schema_id() {
        return Err(ContextError::SchemaMismatch);
    }
    if envelope.schema_version > schema.current_version() {
        return Err(ContextError::UnsupportedSchemaVersion);
    }

    schema.decode_version(envelope.schema_version, envelope.payload)
}

/// Serializes the current typed context into the stable envelope.
///
/// # Errors
///
/// Returns a classified context error if serialization fails or the serialized
/// envelope exceeds its byte or depth limit.
pub fn encode_context<S: ContextSchema>(
    context: &S::Current,
    schema: &S,
    limits: ContextLimits,
) -> Result<Vec<u8>, ContextError> {
    let payload = serde_json::to_value(context).map_err(|_| ContextError::InvalidPayload)?;
    let envelope = Envelope {
        format: String::from(FORMAT),
        format_version: FORMAT_VERSION,
        schema: String::from(schema.schema_id()),
        schema_version: schema.current_version(),
        payload,
    };
    let bytes = serde_json::to_vec(&envelope).map_err(|_| ContextError::InvalidPayload)?;
    if bytes.len() > limits.maximum_bytes {
        return Err(ContextError::TooLarge);
    }
    let value: Value = serde_json::from_slice(&bytes).map_err(|_| ContextError::Malformed)?;
    if json_depth(&value) > limits.maximum_depth {
        return Err(ContextError::TooDeep);
    }
    Ok(bytes)
}

fn json_depth(value: &Value) -> usize {
    match value {
        Value::Array(values) => 1 + values.iter().map(json_depth).max().unwrap_or_default(),
        Value::Object(values) => 1 + values.values().map(json_depth).max().unwrap_or_default(),
        _ => 1,
    }
}

/// The current inventory-import restart state used by the fixture spike.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct InventoryContext {
    /// The next input position.
    pub cursor: u64,
    /// An optional source identity introduced in schema version 2.
    #[serde(default)]
    pub source_checksum: Option<String>,
    /// Additive fields unknown to this reader, retained across a rewrite.
    #[serde(flatten)]
    pub extensions: Map<String, Value>,
}

#[derive(Debug, Deserialize)]
struct InventoryContextV1 {
    next_index: u64,
    #[serde(flatten)]
    extensions: Map<String, Value>,
}

/// Fixture schema demonstrating an explicit v1-to-v2 upgrade.
#[derive(Debug)]
pub struct InventoryContextSchema;

impl ContextSchema for InventoryContextSchema {
    type Current = InventoryContext;

    fn schema_id(&self) -> &'static str {
        "inventory-import"
    }

    fn current_version(&self) -> u32 {
        2
    }

    fn decode_version(&self, version: u32, payload: Value) -> Result<Self::Current, ContextError> {
        match version {
            1 => {
                let old: InventoryContextV1 =
                    serde_json::from_value(payload).map_err(|_| ContextError::InvalidPayload)?;
                Ok(InventoryContext {
                    cursor: old.next_index,
                    source_checksum: None,
                    extensions: old.extensions,
                })
            }
            2 => serde_json::from_value(payload).map_err(|_| ContextError::InvalidPayload),
            _ => Err(ContextError::UnsupportedSchemaVersion),
        }
    }
}
