//! Bounded, versioned checkpoint and execution-context values.

use std::error::Error;
use std::fmt;
use std::num::{NonZeroU32, NonZeroUsize};

use serde_json::{Map, Number, Value};
use sha2::{Digest, Sha256};

const FORMAT_VERSION: u16 = 1;
const MAX_SCHEMA_ID_BYTES: usize = 128;
const DEFAULT_MAXIMUM_BYTES: usize = 64 * 1024;
const DEFAULT_MAXIMUM_DEPTH: usize = 16;
const MAXIMUM_BYTES: usize = 1024 * 1024;
const MAXIMUM_DEPTH: usize = 64;
/// The most directed upgrades one decode may apply.
///
/// Every declared edge strictly increases the version and no version repeats,
/// so a chain cannot exceed the declared edge count. The ceiling bounds a
/// codec that declares an unreasonable number of edges.
const MAX_UPGRADE_CHAIN: usize = 64;

/// The durable state category being encoded or decoded.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum DurableStateKind {
    /// A reader position committed at a chunk boundary.
    Checkpoint,
    /// Application restart state scoped to an execution.
    ExecutionContext,
}

impl DurableStateKind {
    const fn format(self) -> &'static str {
        match self {
            Self::Checkpoint => "oxide-batch.checkpoint",
            Self::ExecutionContext => "oxide-batch.execution-context",
        }
    }
}

impl fmt::Display for DurableStateKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Checkpoint => "checkpoint",
            Self::ExecutionContext => "execution context",
        })
    }
}

/// A validated application-owned durable-state schema identifier.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StateSchemaId(String);

impl StateSchemaId {
    /// Validates a stable schema identifier.
    ///
    /// # Errors
    ///
    /// Returns a redacted [`StateError`] when the identifier is empty, exceeds
    /// 128 UTF-8 bytes, has surrounding whitespace, or contains a control
    /// character.
    pub fn new(value: impl Into<String>) -> Result<Self, StateError> {
        let value = value.into();
        if value.is_empty() {
            return Err(StateError::EmptySchemaId);
        }
        if value.len() > MAX_SCHEMA_ID_BYTES {
            return Err(StateError::SchemaIdTooLong {
                max_bytes: MAX_SCHEMA_ID_BYTES,
            });
        }
        if value.trim() != value {
            return Err(StateError::SchemaIdHasSurroundingWhitespace);
        }
        if value.chars().any(char::is_control) {
            return Err(StateError::SchemaIdContainsControl);
        }
        Ok(Self(value))
    }

    /// Borrows the validated identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for StateSchemaId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StateSchemaId(<redacted>)")
    }
}

impl fmt::Display for StateSchemaId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A nonzero application schema version.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StateSchemaVersion(NonZeroU32);

impl StateSchemaVersion {
    /// Constructs a nonzero schema version.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::ZeroSchemaVersion`] when `value` is zero.
    pub fn new(value: u32) -> Result<Self, StateError> {
        NonZeroU32::new(value)
            .map(Self)
            .ok_or(StateError::ZeroSchemaVersion)
    }

    /// Returns the numeric schema version.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

/// Resource bounds checked before application payload decoding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StateLimits {
    maximum_bytes: NonZeroUsize,
    maximum_depth: NonZeroUsize,
}

impl StateLimits {
    /// Validates explicit byte and JSON-depth limits.
    ///
    /// The hard ceilings match the accepted `PostgreSQL` metadata model. Smaller
    /// limits may be selected per definition.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::InvalidByteLimit`] or
    /// [`StateError::InvalidDepthLimit`] for zero or values above the hard
    /// ceiling.
    pub fn new(maximum_bytes: usize, maximum_depth: usize) -> Result<Self, StateError> {
        if maximum_bytes == 0 || maximum_bytes > MAXIMUM_BYTES {
            return Err(StateError::InvalidByteLimit {
                maximum: MAXIMUM_BYTES,
            });
        }
        if maximum_depth == 0 || maximum_depth > MAXIMUM_DEPTH {
            return Err(StateError::InvalidDepthLimit {
                maximum: MAXIMUM_DEPTH,
            });
        }
        let Some(maximum_bytes) = NonZeroUsize::new(maximum_bytes) else {
            return Err(StateError::InvalidByteLimit {
                maximum: MAXIMUM_BYTES,
            });
        };
        let Some(maximum_depth) = NonZeroUsize::new(maximum_depth) else {
            return Err(StateError::InvalidDepthLimit {
                maximum: MAXIMUM_DEPTH,
            });
        };
        Ok(Self {
            maximum_bytes,
            maximum_depth,
        })
    }

    /// Returns the serialized-envelope byte limit.
    #[must_use]
    pub const fn maximum_bytes(self) -> usize {
        self.maximum_bytes.get()
    }

    /// Returns the maximum JSON nesting depth, including the envelope root.
    #[must_use]
    pub const fn maximum_depth(self) -> usize {
        self.maximum_depth.get()
    }
}

impl Default for StateLimits {
    fn default() -> Self {
        Self {
            maximum_bytes: NonZeroUsize::new(DEFAULT_MAXIMUM_BYTES).unwrap_or(NonZeroUsize::MIN),
            maximum_depth: NonZeroUsize::new(DEFAULT_MAXIMUM_DEPTH).unwrap_or(NonZeroUsize::MIN),
        }
    }
}

/// One directed application-schema upgrade a codec declares.
///
/// An upgrade rewrites a JSON-object payload recorded at [`from`](Self::from)
/// into the shape [`to`](Self::to) expects. The framework, not the codec,
/// selects and applies the edges, so a codec never inspects a recorded version
/// to decide what an older payload meant.
///
/// Edges strictly increase the version and at most one edge may leave any
/// version, which is what makes a resolved chain deterministic and bounded.
#[derive(Clone, Copy)]
pub struct StateSchemaUpgrade {
    from: StateSchemaVersion,
    to: StateSchemaVersion,
    apply: fn(&[u8]) -> Result<Vec<u8>, StateCodecError>,
}

impl StateSchemaUpgrade {
    /// Declares a directed upgrade between two application schema versions.
    ///
    /// `apply` must be deterministic: the same payload bytes must always
    /// produce the same result, because a restart replays the same chain over
    /// the same durable bytes.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::NonIncreasingUpgrade`] when `to` does not exceed
    /// `from`, which would let a chain loop or move backwards.
    pub fn new(
        from: StateSchemaVersion,
        to: StateSchemaVersion,
        apply: fn(&[u8]) -> Result<Vec<u8>, StateCodecError>,
    ) -> Result<Self, StateError> {
        if to <= from {
            return Err(StateError::NonIncreasingUpgrade { from, to });
        }
        Ok(Self { from, to, apply })
    }

    /// Returns the version this upgrade reads.
    #[must_use]
    pub const fn from(&self) -> StateSchemaVersion {
        self.from
    }

    /// Returns the version this upgrade produces.
    #[must_use]
    pub const fn to(&self) -> StateSchemaVersion {
        self.to
    }
}

impl fmt::Debug for StateSchemaUpgrade {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StateSchemaUpgrade")
            .field("from", &self.from)
            .field("to", &self.to)
            .finish_non_exhaustive()
    }
}

/// Serializer-neutral application codec for one durable-state schema.
///
/// Payloads are JSON objects represented as bytes so the public contract does
/// not expose a particular serializer's types. A codec may use Serde, manual
/// JSON handling, or another implementation internally.
///
/// A codec declares its current version and the directed upgrades it can
/// apply. The framework accepts an equal or older recorded version, walks one
/// bounded deterministic chain of declared upgrades up to the current version,
/// and only then calls [`decode`](Self::decode). A codec therefore parses
/// exactly one shape, and a recorded version newer than the current one is
/// rejected rather than truncated, defaulted, or reinterpreted.
pub trait VersionedStateCodec<T>: Send + Sync {
    /// Returns the stable schema identifier.
    fn schema_id(&self) -> &StateSchemaId;

    /// Returns the version emitted by [`encode`](Self::encode).
    fn current_version(&self) -> StateSchemaVersion;

    /// Declares the directed upgrades this codec can apply.
    ///
    /// The default suits a codec whose schema has only ever had one version.
    /// A codec that has published an older version returns the edges that
    /// reach the current one; a recorded version with no path to the current
    /// version is rejected rather than guessed at.
    fn upgrades(&self) -> &[StateSchemaUpgrade] {
        &[]
    }

    /// Encodes the current typed value as one JSON object.
    ///
    /// # Errors
    ///
    /// Returns a value-redacted codec classification.
    fn encode(&self, value: &T) -> Result<Vec<u8>, StateCodecError>;

    /// Decodes one JSON-object payload already at
    /// [`current_version`](Self::current_version).
    ///
    /// # Errors
    ///
    /// Returns [`StateCodecError::InvalidPayload`] when the payload does not
    /// satisfy the current schema.
    fn decode(&self, payload: &[u8]) -> Result<T, StateCodecError>;
}

/// Stable, payload-redacted failures returned by an application codec.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum StateCodecError {
    /// The payload does not satisfy the selected schema.
    InvalidPayload,
    /// The codec has no directed upgrade path from the selected version.
    UnsupportedSchemaVersion,
}

impl fmt::Display for StateCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidPayload => "durable state payload is invalid",
            Self::UnsupportedSchemaVersion => "durable state schema version is unsupported",
        })
    }
}

impl Error for StateCodecError {}

#[derive(Clone, Eq, PartialEq)]
struct VersionedState {
    schema_id: StateSchemaId,
    schema_version: StateSchemaVersion,
    payload: Value,
    encoded_bytes: usize,
}

impl VersionedState {
    fn encode<T>(
        kind: DurableStateKind,
        value: &T,
        codec: &(impl VersionedStateCodec<T> + ?Sized),
        limits: StateLimits,
    ) -> Result<Self, StateError> {
        let payload_bytes = codec.encode(value).map_err(StateError::Codec)?;
        let payload: Value =
            serde_json::from_slice(&payload_bytes).map_err(|_| StateError::InvalidPayload)?;
        if !payload.is_object() {
            return Err(StateError::PayloadNotObject);
        }
        Self::from_parts(
            kind,
            codec.schema_id().clone(),
            codec.current_version(),
            payload,
            limits,
        )
    }

    fn from_json(
        kind: DurableStateKind,
        bytes: &[u8],
        limits: StateLimits,
    ) -> Result<Self, StateError> {
        if bytes.len() > limits.maximum_bytes() {
            return Err(StateError::TooLarge {
                kind,
                max_bytes: limits.maximum_bytes(),
            });
        }
        let value: Value =
            serde_json::from_slice(bytes).map_err(|_| StateError::Malformed { kind })?;
        if json_depth(&value) > limits.maximum_depth() {
            return Err(StateError::TooDeep {
                kind,
                max_depth: limits.maximum_depth(),
            });
        }
        let object = value.as_object().ok_or(StateError::Malformed { kind })?;
        let format = object
            .get("format")
            .and_then(Value::as_str)
            .ok_or(StateError::Malformed { kind })?;
        if format != kind.format() {
            return Err(StateError::FormatMismatch { kind });
        }
        let format_version = object
            .get("format_version")
            .and_then(Value::as_u64)
            .and_then(|version| u16::try_from(version).ok())
            .ok_or(StateError::Malformed { kind })?;
        if format_version != FORMAT_VERSION {
            return Err(StateError::UnsupportedFormatVersion {
                kind,
                version: format_version,
            });
        }
        let schema_id = object
            .get("schema")
            .and_then(Value::as_str)
            .ok_or(StateError::Malformed { kind })?;
        let schema_id = StateSchemaId::new(schema_id)?;
        let schema_version = object
            .get("schema_version")
            .and_then(Value::as_u64)
            .and_then(|version| u32::try_from(version).ok())
            .ok_or(StateError::Malformed { kind })?;
        let schema_version = StateSchemaVersion::new(schema_version)?;
        let payload = object
            .get("payload")
            .cloned()
            .ok_or(StateError::Malformed { kind })?;
        if !payload.is_object() {
            return Err(StateError::PayloadNotObject);
        }
        Ok(Self {
            schema_id,
            schema_version,
            payload,
            encoded_bytes: bytes.len(),
        })
    }

    fn from_parts(
        kind: DurableStateKind,
        schema_id: StateSchemaId,
        schema_version: StateSchemaVersion,
        payload: Value,
        limits: StateLimits,
    ) -> Result<Self, StateError> {
        let envelope = envelope(kind, &schema_id, schema_version, payload.clone());
        let bytes = serde_json::to_vec(&envelope).map_err(|_| StateError::Malformed { kind })?;
        if bytes.len() > limits.maximum_bytes() {
            return Err(StateError::TooLarge {
                kind,
                max_bytes: limits.maximum_bytes(),
            });
        }
        if json_depth(&envelope) > limits.maximum_depth() {
            return Err(StateError::TooDeep {
                kind,
                max_depth: limits.maximum_depth(),
            });
        }
        Ok(Self {
            schema_id,
            schema_version,
            payload,
            encoded_bytes: bytes.len(),
        })
    }

    fn decode<T>(
        &self,
        kind: DurableStateKind,
        codec: &(impl VersionedStateCodec<T> + ?Sized),
    ) -> Result<T, StateError> {
        if &self.schema_id != codec.schema_id() {
            return Err(StateError::SchemaMismatch { kind });
        }
        let current = codec.current_version();
        if self.schema_version > current {
            return Err(StateError::UnsupportedSchemaVersion {
                kind,
                found: self.schema_version,
                current,
            });
        }
        let payload =
            serde_json::to_vec(&self.payload).map_err(|_| StateError::Malformed { kind })?;
        let payload = self.upgrade(kind, codec, payload)?;
        codec.decode(&payload).map_err(StateError::Codec)
    }

    /// Walks the declared upgrade edges from the recorded version to `current`.
    ///
    /// Each step takes the single edge leaving the position reached so far.
    /// Two edges leaving one version would make the result depend on
    /// declaration order, so that is rejected rather than resolved.
    fn upgrade<T>(
        &self,
        kind: DurableStateKind,
        codec: &(impl VersionedStateCodec<T> + ?Sized),
        mut payload: Vec<u8>,
    ) -> Result<Vec<u8>, StateError> {
        let current = codec.current_version();
        let upgrades = codec.upgrades();
        let mut version = self.schema_version;
        let mut applied = 0_usize;
        while version < current {
            let mut edges = upgrades.iter().filter(|upgrade| upgrade.from == version);
            let edge = edges.next().ok_or(StateError::NoUpgradePath {
                kind,
                found: version,
                current,
            })?;
            if edges.next().is_some() {
                return Err(StateError::AmbiguousUpgrade {
                    kind,
                    from: version,
                });
            }
            if edge.to > current {
                return Err(StateError::UpgradeOvershootsCurrent {
                    kind,
                    to: edge.to,
                    current,
                });
            }
            applied += 1;
            if applied > MAX_UPGRADE_CHAIN {
                return Err(StateError::UpgradeChainTooLong {
                    kind,
                    max_upgrades: MAX_UPGRADE_CHAIN,
                });
            }
            payload = (edge.apply)(&payload).map_err(StateError::Codec)?;
            check_upgraded(kind, &payload)?;
            version = edge.to;
        }
        Ok(payload)
    }

    fn to_json(&self, kind: DurableStateKind) -> Result<Vec<u8>, StateError> {
        serde_json::to_vec(&envelope(
            kind,
            &self.schema_id,
            self.schema_version,
            self.payload.clone(),
        ))
        .map_err(|_| StateError::Malformed { kind })
    }

    fn payload_json(&self, kind: DurableStateKind) -> Result<Vec<u8>, StateError> {
        serde_json::to_vec(&self.payload).map_err(|_| StateError::Malformed { kind })
    }
}

fn envelope(
    kind: DurableStateKind,
    schema_id: &StateSchemaId,
    schema_version: StateSchemaVersion,
    payload: Value,
) -> Value {
    let mut object = Map::new();
    object.insert(
        String::from("format"),
        Value::String(String::from(kind.format())),
    );
    object.insert(
        String::from("format_version"),
        Value::Number(Number::from(FORMAT_VERSION)),
    );
    object.insert(
        String::from("schema"),
        Value::String(String::from(schema_id.as_str())),
    );
    object.insert(
        String::from("schema_version"),
        Value::Number(Number::from(schema_version.get())),
    );
    object.insert(String::from("payload"), payload);
    Value::Object(object)
}

/// Holds a payload produced by a declared upgrade to the envelope's own shape
/// and to the durable hard ceilings.
///
/// An upgrade is application code running between two framework checks, so a
/// transform that returns a non-object, invalid JSON, or an unbounded payload
/// fails here as a typed state error rather than reaching the codec. The
/// ceilings rather than the configured limits apply: this value is an
/// intermediate that is never persisted, and the bytes that are persisted come
/// from `encode`, which is checked against the configured limits.
fn check_upgraded(kind: DurableStateKind, payload: &[u8]) -> Result<(), StateError> {
    if payload.len() > MAXIMUM_BYTES {
        return Err(StateError::TooLarge {
            kind,
            max_bytes: MAXIMUM_BYTES,
        });
    }
    let value: Value = serde_json::from_slice(payload)
        .map_err(|_| StateError::UpgradeProducedInvalidJson { kind })?;
    if !value.is_object() {
        return Err(StateError::PayloadNotObject);
    }
    if json_depth(&value) > MAXIMUM_DEPTH {
        return Err(StateError::TooDeep {
            kind,
            max_depth: MAXIMUM_DEPTH,
        });
    }
    Ok(())
}

fn json_depth(value: &Value) -> usize {
    match value {
        Value::Array(values) => 1 + values.iter().map(json_depth).max().unwrap_or_default(),
        Value::Object(values) => 1 + values.values().map(json_depth).max().unwrap_or_default(),
        _ => 1,
    }
}

macro_rules! durable_state {
    ($name:ident, $kind:expr, $docs:literal) => {
        #[doc = $docs]
        #[derive(Clone, Eq, PartialEq)]
        pub struct $name(VersionedState);

        impl $name {
            /// Encodes a current typed value with explicit resource limits.
            ///
            /// # Errors
            ///
            /// Returns a redacted codec, schema, JSON-shape, size, or depth
            /// failure.
            pub fn encode<T>(
                value: &T,
                codec: &(impl VersionedStateCodec<T> + ?Sized),
                limits: StateLimits,
            ) -> Result<Self, StateError> {
                VersionedState::encode($kind, value, codec, limits).map(Self)
            }

            /// Validates a serialized framework envelope before retaining it.
            ///
            /// # Errors
            ///
            /// Returns a redacted format, schema, shape, size, or depth
            /// failure.
            pub fn from_json(bytes: &[u8], limits: StateLimits) -> Result<Self, StateError> {
                VersionedState::from_json($kind, bytes, limits).map(Self)
            }

            /// Decodes or upgrades the retained payload through `codec`.
            ///
            /// # Errors
            ///
            /// Returns a redacted schema-compatibility or codec failure.
            pub fn decode<T>(
                &self,
                codec: &(impl VersionedStateCodec<T> + ?Sized),
            ) -> Result<T, StateError> {
                self.0.decode($kind, codec)
            }

            /// Returns the framework envelope format version.
            #[must_use]
            pub const fn format_version(&self) -> u16 {
                FORMAT_VERSION
            }

            /// Borrows the validated application schema identifier.
            #[must_use]
            pub const fn schema_id(&self) -> &StateSchemaId {
                &self.0.schema_id
            }

            /// Returns the retained application schema version.
            #[must_use]
            pub const fn schema_version(&self) -> StateSchemaVersion {
                self.0.schema_version
            }

            /// Returns the validated serialized-envelope byte size.
            #[must_use]
            pub const fn encoded_len(&self) -> usize {
                self.0.encoded_bytes
            }

            /// Serializes the complete framework envelope.
            ///
            /// # Errors
            ///
            /// Returns a redacted format failure if the retained JSON value
            /// cannot be serialized.
            pub fn to_json(&self) -> Result<Vec<u8>, StateError> {
                self.0.to_json($kind)
            }

            /// Serializes only the application payload for an authorized
            /// persistence adapter.
            ///
            /// This value can contain sensitive restart data and must not be
            /// logged or exported as telemetry.
            ///
            /// # Errors
            ///
            /// Returns a redacted format failure if the retained JSON value
            /// cannot be serialized.
            pub fn payload_json(&self) -> Result<Vec<u8>, StateError> {
                self.0.payload_json($kind)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_struct(stringify!($name))
                    .field("format_version", &FORMAT_VERSION)
                    .field("schema_version", &self.schema_version())
                    .field("encoded_bytes", &self.encoded_len())
                    .field("payload", &"<redacted>")
                    .finish()
            }
        }
    };
}

durable_state!(
    Checkpoint,
    DurableStateKind::Checkpoint,
    "A bounded, versioned reader position committed with a chunk."
);
durable_state!(
    ExecutionContext,
    DurableStateKind::ExecutionContext,
    "Bounded, versioned application restart state committed with a chunk."
);

impl Checkpoint {
    /// Returns the framework digest identifying this checkpoint generation.
    ///
    /// Retry keys are derived from the generation, so the runtime and every
    /// durable adapter must agree on this exact derivation.
    #[must_use]
    pub fn generation_digest(&self) -> [u8; 32] {
        self.to_json()
            .map_or([0; 32], |bytes| Sha256::digest(&bytes).into())
    }
}

/// Stable, value-redacted durable-state validation failure.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum StateError {
    /// A schema identifier was empty.
    EmptySchemaId,
    /// A schema identifier exceeded its UTF-8 byte limit.
    SchemaIdTooLong {
        /// Maximum accepted UTF-8 bytes.
        max_bytes: usize,
    },
    /// A schema identifier had surrounding whitespace.
    SchemaIdHasSurroundingWhitespace,
    /// A schema identifier contained a control character.
    SchemaIdContainsControl,
    /// A schema version was zero.
    ZeroSchemaVersion,
    /// A configured byte limit was zero or above the durable hard ceiling.
    InvalidByteLimit {
        /// Largest configurable byte limit.
        maximum: usize,
    },
    /// A configured depth limit was zero or above the durable hard ceiling.
    InvalidDepthLimit {
        /// Largest configurable JSON depth.
        maximum: usize,
    },
    /// A serialized value exceeded its configured byte limit.
    TooLarge {
        /// Durable state category.
        kind: DurableStateKind,
        /// Configured maximum bytes.
        max_bytes: usize,
    },
    /// A serialized value exceeded its configured JSON depth.
    TooDeep {
        /// Durable state category.
        kind: DurableStateKind,
        /// Configured maximum depth.
        max_depth: usize,
    },
    /// The bytes were not a valid framework envelope.
    Malformed {
        /// Durable state category.
        kind: DurableStateKind,
    },
    /// The envelope belongs to the other durable-state category.
    FormatMismatch {
        /// Expected durable state category.
        kind: DurableStateKind,
    },
    /// The framework envelope format version is unsupported.
    UnsupportedFormatVersion {
        /// Durable state category.
        kind: DurableStateKind,
        /// Version observed in durable data.
        version: u16,
    },
    /// The envelope schema does not match the selected codec.
    SchemaMismatch {
        /// Durable state category.
        kind: DurableStateKind,
    },
    /// A declared upgrade did not strictly increase the schema version.
    NonIncreasingUpgrade {
        /// Version the rejected edge reads.
        from: StateSchemaVersion,
        /// Version the rejected edge claims to produce.
        to: StateSchemaVersion,
    },
    /// The codec declares no directed upgrade reaching its current version.
    NoUpgradePath {
        /// Durable state category.
        kind: DurableStateKind,
        /// Version the chain stalled at.
        found: StateSchemaVersion,
        /// Current version supported by the selected codec.
        current: StateSchemaVersion,
    },
    /// Two declared upgrades leave the same version, so the chain is not
    /// deterministic.
    AmbiguousUpgrade {
        /// Durable state category.
        kind: DurableStateKind,
        /// Version left by more than one declared upgrade.
        from: StateSchemaVersion,
    },
    /// A declared upgrade produces a version past the codec's current version.
    UpgradeOvershootsCurrent {
        /// Durable state category.
        kind: DurableStateKind,
        /// Version the rejected edge produces.
        to: StateSchemaVersion,
        /// Current version supported by the selected codec.
        current: StateSchemaVersion,
    },
    /// The resolved upgrade chain exceeded its bound.
    UpgradeChainTooLong {
        /// Durable state category.
        kind: DurableStateKind,
        /// Most upgrades one decode may apply.
        max_upgrades: usize,
    },
    /// A declared upgrade returned bytes that are not valid JSON.
    UpgradeProducedInvalidJson {
        /// Durable state category.
        kind: DurableStateKind,
    },
    /// Durable data was produced by a newer application schema.
    UnsupportedSchemaVersion {
        /// Durable state category.
        kind: DurableStateKind,
        /// Version observed in durable data.
        found: StateSchemaVersion,
        /// Current version supported by the selected codec.
        current: StateSchemaVersion,
    },
    /// The application payload was not valid JSON.
    InvalidPayload,
    /// The application payload was valid JSON but not an object.
    PayloadNotObject,
    /// The application codec rejected the payload without exposing its value.
    Codec(StateCodecError),
}

impl fmt::Display for StateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptySchemaId => formatter.write_str("state schema identifier must not be empty"),
            Self::SchemaIdTooLong { max_bytes } => {
                write!(
                    formatter,
                    "state schema identifier exceeds {max_bytes} UTF-8 bytes"
                )
            }
            Self::SchemaIdHasSurroundingWhitespace => {
                formatter.write_str("state schema identifier has surrounding whitespace")
            }
            Self::SchemaIdContainsControl => {
                formatter.write_str("state schema identifier contains a control character")
            }
            Self::ZeroSchemaVersion => formatter.write_str("state schema version must be nonzero"),
            Self::InvalidByteLimit { maximum } => {
                write!(
                    formatter,
                    "state byte limit must be between 1 and {maximum}"
                )
            }
            Self::InvalidDepthLimit { maximum } => {
                write!(
                    formatter,
                    "state depth limit must be between 1 and {maximum}"
                )
            }
            Self::TooLarge { kind, max_bytes } => {
                write!(formatter, "{kind} exceeds {max_bytes} bytes")
            }
            Self::TooDeep { kind, max_depth } => {
                write!(formatter, "{kind} exceeds JSON depth {max_depth}")
            }
            Self::Malformed { kind } => write!(formatter, "{kind} is malformed"),
            Self::FormatMismatch { kind } => {
                write!(formatter, "durable state is not a {kind}")
            }
            Self::UnsupportedFormatVersion { kind, .. } => {
                write!(formatter, "{kind} format version is unsupported")
            }
            Self::SchemaMismatch { kind } => {
                write!(formatter, "{kind} schema does not match the component")
            }
            Self::NonIncreasingUpgrade { .. } => {
                formatter.write_str("state schema upgrade must increase the version")
            }
            Self::NoUpgradePath { kind, .. } => {
                write!(formatter, "{kind} schema version has no upgrade path")
            }
            Self::AmbiguousUpgrade { kind, .. } => {
                write!(formatter, "{kind} schema upgrade is ambiguous")
            }
            Self::UpgradeOvershootsCurrent { kind, .. } => {
                write!(
                    formatter,
                    "{kind} schema upgrade passes the current version"
                )
            }
            Self::UpgradeChainTooLong { kind, max_upgrades } => {
                write!(
                    formatter,
                    "{kind} schema upgrade chain exceeds {max_upgrades} upgrades"
                )
            }
            Self::UpgradeProducedInvalidJson { kind } => {
                write!(formatter, "{kind} schema upgrade produced invalid JSON")
            }
            Self::UnsupportedSchemaVersion { kind, .. } => {
                write!(formatter, "{kind} schema version is unsupported")
            }
            Self::InvalidPayload => formatter.write_str("durable state payload is not valid JSON"),
            Self::PayloadNotObject => {
                formatter.write_str("durable state payload must be a JSON object")
            }
            Self::Codec(error) => error.fmt(formatter),
        }
    }
}

impl Error for StateError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Codec(error) => Some(error),
            _ => None,
        }
    }
}
