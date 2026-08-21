//! Bounded, versioned, checksummed component-owned durable state.
//!
//! This is the M6 `ItemStream` state contract (Gate C of the M6 design-gate
//! evidence): a namespaced envelope distinct from [`crate::Checkpoint`] and
//! [`crate::ExecutionContext`], adding a checksum-before-decode boundary and a
//! codec identity/version axis that is separate from the application schema
//! identity/version axis. It reuses [`crate::StateLimits`],
//! [`crate::StateSchemaId`], [`crate::StateSchemaVersion`],
//! [`crate::StateSchemaUpgrade`], and [`crate::VersionedStateCodec`] unchanged
//! for the application-schema axis, and the crate's one schema-upgrade-chain
//! algorithm ([`crate::state`]'s `upgrade_schema_chain`) rather than a second
//! migration implementation.

use std::error::Error;
use std::fmt;
use std::marker::PhantomData;
use std::num::NonZeroU32;
use std::sync::Arc;

use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::definition::ComponentStreamIdentity;
use crate::state::{json_depth, upgrade_schema_chain};
use crate::{
    DurableStateKind, StateCodecError, StateError, StateLimits, StateSchemaId, StateSchemaVersion,
    VersionedStateCodec,
};

const MAX_TOKEN_BYTES: usize = 128;
/// The most directed codec-version upgrades one decode may apply.
///
/// Matches the schema-axis bound in [`crate::state`] so neither axis can be
/// used to build an unbounded migration chain.
const MAX_UPGRADE_CHAIN: usize = 64;

/// A validated application-owned codec identifier.
///
/// Distinct from [`StateSchemaId`]: the schema axis identifies the
/// application-level shape of the payload, while the codec axis identifies
/// the encoding/versioning scheme a component uses to read and write it. The
/// two must not be conflated -- an unknown codec fails closed independently
/// of whether the schema is recognized.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CodecId(String);

impl CodecId {
    /// Validates a stable codec identifier.
    ///
    /// # Errors
    ///
    /// Returns [`ComponentStateError`] when the identifier is empty, exceeds
    /// 128 UTF-8 bytes, has surrounding whitespace, or contains a control
    /// character.
    pub fn new(value: impl Into<String>) -> Result<Self, ComponentStateError> {
        let value = value.into();
        if value.is_empty() {
            return Err(ComponentStateError::EmptyCodecId);
        }
        if value.len() > MAX_TOKEN_BYTES {
            return Err(ComponentStateError::CodecIdTooLong {
                max_bytes: MAX_TOKEN_BYTES,
            });
        }
        if value.trim() != value || value.chars().any(char::is_control) {
            return Err(ComponentStateError::Malformed);
        }
        Ok(Self(value))
    }

    /// Borrows the validated identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for CodecId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Codec identity is explicitly safe diagnostic metadata (Gate C), so
        // this is the one durable-state identity newtype in this crate that
        // does not redact its Debug output.
        formatter.debug_tuple("CodecId").field(&self.0).finish()
    }
}

impl fmt::Display for CodecId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A nonzero application codec version.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CodecVersion(NonZeroU32);

impl CodecVersion {
    /// Constructs a nonzero codec version.
    ///
    /// # Errors
    ///
    /// Returns [`ComponentStateError::ZeroCodecVersion`] when `value` is zero.
    pub fn new(value: u32) -> Result<Self, ComponentStateError> {
        NonZeroU32::new(value)
            .map(Self)
            .ok_or(ComponentStateError::ZeroCodecVersion)
    }

    /// Returns the numeric codec version.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

/// One directed codec-version upgrade a component declares.
///
/// Mirrors [`crate::StateSchemaUpgrade`] exactly, on the codec-version axis
/// rather than the application-schema axis: edges strictly increase the
/// version and at most one edge may leave any version.
#[derive(Clone, Copy)]
pub struct CodecVersionUpgrade {
    from: CodecVersion,
    to: CodecVersion,
    apply: fn(&[u8]) -> Result<Vec<u8>, StateCodecError>,
}

impl CodecVersionUpgrade {
    /// Declares a directed upgrade between two codec versions.
    ///
    /// # Errors
    ///
    /// Returns [`ComponentStateError::NonIncreasingCodecUpgrade`] when `to`
    /// does not exceed `from`.
    pub fn new(
        from: CodecVersion,
        to: CodecVersion,
        apply: fn(&[u8]) -> Result<Vec<u8>, StateCodecError>,
    ) -> Result<Self, ComponentStateError> {
        if to <= from {
            return Err(ComponentStateError::NonIncreasingCodecUpgrade);
        }
        Ok(Self { from, to, apply })
    }

    /// Returns the version this upgrade reads.
    #[must_use]
    pub const fn from(&self) -> CodecVersion {
        self.from
    }

    /// Returns the version this upgrade produces.
    #[must_use]
    pub const fn to(&self) -> CodecVersion {
        self.to
    }
}

impl fmt::Debug for CodecVersionUpgrade {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CodecVersionUpgrade")
            .field("from", &self.from)
            .field("to", &self.to)
            .finish_non_exhaustive()
    }
}

/// The checksum algorithm identity and version protecting a component-state
/// envelope.
///
/// Carried as its own versioned field (Gate C) so a future algorithm change is
/// a migration, not a silent reinterpretation of existing durable bytes.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ChecksumAlgorithm {
    /// SHA-256 over the canonical checksum input.
    Sha256,
}

impl ChecksumAlgorithm {
    /// The algorithm this build produces for a freshly encoded envelope.
    pub const CURRENT: Self = Self::Sha256;

    const fn identifier(self) -> u16 {
        match self {
            Self::Sha256 => 1,
        }
    }

    const fn algorithm_version(self) -> u16 {
        match self {
            Self::Sha256 => 1,
        }
    }

    const fn resolve(identifier: u16, version: u16) -> Option<Self> {
        match (identifier, version) {
            (1, 1) => Some(Self::Sha256),
            _ => None,
        }
    }

    fn digest(self, input: &[u8]) -> [u8; 32] {
        match self {
            Self::Sha256 => Sha256::digest(input).into(),
        }
    }
}

/// Declared sensitivity of one component's durable-state contract.
///
/// One source of truth: this is declared once by
/// [`ComponentStateCodec::sensitivity`] and is never duplicated as a second,
/// independently maintained envelope field. Absent an explicit non-sensitive
/// declaration, component state is [`Self::Sensitive`] (fail-safe).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum StateSensitivity {
    /// The state's payload must never appear in diagnostics.
    #[default]
    Sensitive,
    /// The component has explicitly declared its payload safe to surface in
    /// authorized diagnostic projections.
    NonSensitive,
}

/// Whether a component can honestly claim restartability.
///
/// Independent of whether a *reader* checkpoint exists: a component with
/// required durable state that cannot be reconstructed must not be marked
/// restartable merely because the reader's own checkpoint is present.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RestartabilityDeclaration {
    /// No required state, or required state is durably persisted or fully
    /// reconstructible without persistence.
    Restartable,
    /// Required state exists, persistence is disabled or unavailable, and the
    /// state is not reconstructible.
    NotRestartable,
}

/// A content-identified pointer to an external state blob.
///
/// Opaque bytes, not a payload: this is a hash, never the sensitive state
/// itself, so it is safe to display.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct ContentIdentity([u8; 32]);

impl ContentIdentity {
    /// Constructs a content identity from its raw bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the SHA-256 content identity of `bytes`.
    #[must_use]
    pub fn of(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }

    /// Borrows the raw content-identity bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for ContentIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "ContentIdentity({})", hex_encode(&self.0))
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    use fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// A bounded reference to a large component-state blob held outside the
/// durable envelope's inline metadata.
///
/// No adapter (S3, Azure, GCS, ...) ships with this contract; `#144` provides
/// only the capability/model boundary an adapter would later implement
/// against.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExternalStateReference {
    content_id: ContentIdentity,
    encoded_len: u64,
}

impl ExternalStateReference {
    /// Constructs a bounded external-state reference.
    #[must_use]
    pub const fn new(content_id: ContentIdentity, encoded_len: u64) -> Self {
        Self {
            content_id,
            encoded_len,
        }
    }

    /// Returns the content identity of the referenced blob.
    #[must_use]
    pub const fn content_id(&self) -> ContentIdentity {
        self.content_id
    }

    /// Returns the declared encoded length of the referenced blob.
    #[must_use]
    pub const fn encoded_len(&self) -> u64 {
        self.encoded_len
    }

    /// Verifies that resolved bytes match this reference's declared content
    /// identity.
    ///
    /// A caller resolving an external reference through an
    /// [`ExternalStateStore`] must call this before trusting the resolved
    /// bytes: missing content is [`ExternalStateError::ContentMissing`] from
    /// the store itself, while resolved-but-mismatched content is caught
    /// here rather than silently accepted.
    ///
    /// # Errors
    ///
    /// Returns [`ComponentStateError::ExternalReferenceContentMismatch`] when
    /// `bytes`'s content identity does not match [`Self::content_id`].
    pub fn verify(&self, bytes: &[u8]) -> Result<(), ComponentStateError> {
        if ContentIdentity::of(bytes) == self.content_id {
            Ok(())
        } else {
            Err(ComponentStateError::ExternalReferenceContentMismatch)
        }
    }
}

/// A capability an adapter may implement to resolve and store external
/// component state.
///
/// ADR-0008-shaped: a generic trait with an explicit call lifetime and an
/// opaque future return, matching the item-component contract's conventions.
/// No concrete adapter ships with `#144`.
pub trait ExternalStateStore: Send + Sync {
    /// Resolves the bytes a reference identifies.
    ///
    /// # Errors
    ///
    /// Returns [`ExternalStateError::ContentMissing`] when the referenced
    /// content cannot be found, or
    /// [`ExternalStateError::ContentMismatch`] when the resolved bytes do not
    /// match the declared content identity.
    fn resolve<'a>(
        &'a self,
        reference: &'a ExternalStateReference,
    ) -> impl Future<Output = Result<Vec<u8>, ExternalStateError>> + Send + 'a;

    /// Stores `bytes` and returns their bounded, content-identified reference.
    ///
    /// # Errors
    ///
    /// Returns [`ExternalStateError::StoreFailed`] when the adapter cannot
    /// durably retain the bytes.
    fn store<'a>(
        &'a self,
        bytes: &'a [u8],
    ) -> impl Future<Output = Result<ExternalStateReference, ExternalStateError>> + Send + 'a;
}

/// Stable, value-redacted external-state-store failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ExternalStateError {
    /// The referenced content does not exist in the store.
    ContentMissing,
    /// The resolved bytes do not match the declared content identity.
    ContentMismatch,
    /// The store could not durably retain the supplied bytes.
    StoreFailed,
}

impl fmt::Display for ExternalStateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ContentMissing => "external component state content is missing",
            Self::ContentMismatch => "external component state content identity mismatch",
            Self::StoreFailed => "external component state store failed",
        })
    }
}

impl Error for ExternalStateError {}

/// The bounded representation a component-state payload unambiguously is.
///
/// Never a silent third, unbounded form: a payload is either inline bytes
/// bounded by [`StateLimits`], or a bounded external reference identified by
/// content identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ComponentStatePayload {
    /// Bounded inline payload bytes (an encoded JSON object).
    Inline(Vec<u8>),
    /// A bounded external reference to out-of-band state.
    External(ExternalStateReference),
}

#[derive(Clone, Eq, PartialEq)]
enum StoredPayload {
    /// The exact codec-produced bytes, never reserialized through
    /// [`serde_json::Value`].
    ///
    /// Holding a parsed `Value` here (the M6 PR's original shape) let two
    /// byte-identical-in-meaning-but-not-in-bytes encodings (whitespace, key
    /// order) silently change the bytes a later checksum or decode observed.
    /// Retaining the raw bytes exactly is what makes the checksum in
    /// [`ComponentStateEnvelope`] a checksum of what is actually returned by
    /// [`ComponentStateEnvelope::payload`] and consumed by
    /// [`ComponentStateEnvelope::decode`], not of a re-encoding of it.
    Inline(Vec<u8>),
    External(ExternalStateReference),
}

/// The full durable envelope for one committed component-state namespace.
///
/// Every identity field is validated, and the checksum is verified against
/// the raw payload bytes, before any application decode or migration step
/// runs.
#[derive(Clone, Eq, PartialEq)]
pub struct ComponentStateEnvelope {
    namespace: ComponentStreamIdentity,
    schema_id: StateSchemaId,
    schema_version: StateSchemaVersion,
    codec_id: CodecId,
    codec_version: CodecVersion,
    checksum_algorithm: ChecksumAlgorithm,
    checksum: [u8; 32],
    payload: StoredPayload,
    encoded_len: usize,
}

fn canonical_checksum_input(
    namespace: &ComponentStreamIdentity,
    schema_id: &StateSchemaId,
    schema_version: StateSchemaVersion,
    codec_id: &CodecId,
    codec_version: CodecVersion,
    payload_marker: &[u8],
) -> Vec<u8> {
    let mut input = Vec::with_capacity(64 + payload_marker.len());
    input.extend_from_slice(b"oxide-batch.component-state\0");
    input.extend_from_slice(namespace.as_str().as_bytes());
    input.push(0);
    input.extend_from_slice(schema_id.as_str().as_bytes());
    input.push(0);
    input.extend_from_slice(&schema_version.get().to_be_bytes());
    input.extend_from_slice(codec_id.as_str().as_bytes());
    input.push(0);
    input.extend_from_slice(&codec_version.get().to_be_bytes());
    input.extend_from_slice(payload_marker);
    input
}

fn inline_payload_marker(bytes: &[u8]) -> Vec<u8> {
    let mut marker = Vec::with_capacity(1 + bytes.len());
    marker.push(0);
    marker.extend_from_slice(bytes);
    marker
}

fn external_payload_marker(reference: &ExternalStateReference) -> Vec<u8> {
    let mut marker = Vec::with_capacity(1 + 32 + 8);
    marker.push(1);
    marker.extend_from_slice(reference.content_id().as_bytes());
    marker.extend_from_slice(&reference.encoded_len().to_be_bytes());
    marker
}

/// Holds a payload produced by a declared codec-version upgrade to the
/// envelope's own shape and to the durable hard ceilings.
///
/// Mirrors [`crate::state`]'s `check_upgraded` on the codec-version axis: an
/// upgrade is application code running between two framework checks, so a
/// transform that returns a non-object, invalid JSON, or an unbounded payload
/// fails here rather than reaching the codec.
fn check_upgraded_component_state(payload: &[u8]) -> Result<(), ComponentStateError> {
    const HARD_MAXIMUM_BYTES: usize = 1024 * 1024;
    const HARD_MAXIMUM_DEPTH: usize = 64;
    if payload.len() > HARD_MAXIMUM_BYTES {
        return Err(ComponentStateError::TooLarge {
            max_bytes: HARD_MAXIMUM_BYTES,
        });
    }
    let value: Value =
        serde_json::from_slice(payload).map_err(|_| ComponentStateError::PayloadNotObject)?;
    if !value.is_object() {
        return Err(ComponentStateError::PayloadNotObject);
    }
    if json_depth(&value) > HARD_MAXIMUM_DEPTH {
        return Err(ComponentStateError::TooDeep {
            max_depth: HARD_MAXIMUM_DEPTH,
        });
    }
    Ok(())
}

fn upgrade_codec_chain<T>(
    recorded: CodecVersion,
    codec: &(impl ComponentStateCodec<T> + ?Sized),
    mut payload: Vec<u8>,
) -> Result<Vec<u8>, ComponentStateError> {
    let current = codec.codec_version();
    let upgrades = codec.codec_upgrades();
    let mut version = recorded;
    let mut applied = 0_usize;
    while version < current {
        let mut edges = upgrades.iter().filter(|upgrade| upgrade.from == version);
        let edge = edges
            .next()
            .ok_or(ComponentStateError::NoCodecUpgradePath {
                found: version.get(),
                current: current.get(),
            })?;
        if edges.next().is_some() {
            return Err(ComponentStateError::AmbiguousCodecUpgrade {
                from: version.get(),
            });
        }
        if edge.to > current {
            return Err(ComponentStateError::CodecUpgradeOvershootsCurrent {
                to: edge.to.get(),
                current: current.get(),
            });
        }
        applied += 1;
        if applied > MAX_UPGRADE_CHAIN {
            return Err(ComponentStateError::CodecUpgradeChainTooLong {
                max_upgrades: MAX_UPGRADE_CHAIN,
            });
        }
        payload = (edge.apply)(&payload).map_err(ComponentStateError::Codec)?;
        check_upgraded_component_state(&payload)?;
        version = edge.to;
    }
    Ok(payload)
}

fn map_schema_chain_error(error: &StateError) -> ComponentStateError {
    match error {
        StateError::SchemaMismatch { .. } => ComponentStateError::SchemaMismatch,
        StateError::UnsupportedSchemaVersion { found, current, .. } => {
            ComponentStateError::UnsupportedSchemaVersion {
                found: found.get(),
                current: current.get(),
            }
        }
        StateError::NoUpgradePath { found, current, .. } => {
            ComponentStateError::NoSchemaUpgradePath {
                found: found.get(),
                current: current.get(),
            }
        }
        StateError::AmbiguousUpgrade { from, .. } => {
            ComponentStateError::AmbiguousSchemaUpgrade { from: from.get() }
        }
        StateError::UpgradeOvershootsCurrent { to, current, .. } => {
            ComponentStateError::SchemaUpgradeOvershootsCurrent {
                to: to.get(),
                current: current.get(),
            }
        }
        StateError::UpgradeChainTooLong { max_upgrades, .. } => {
            ComponentStateError::SchemaUpgradeChainTooLong {
                max_upgrades: *max_upgrades,
            }
        }
        StateError::TooLarge { max_bytes, .. } => ComponentStateError::TooLarge {
            max_bytes: *max_bytes,
        },
        StateError::TooDeep { max_depth, .. } => ComponentStateError::TooDeep {
            max_depth: *max_depth,
        },
        StateError::Codec(inner) => ComponentStateError::Codec(*inner),
        _ => ComponentStateError::Malformed,
    }
}

impl ComponentStateEnvelope {
    /// Encodes a current typed value as a bounded inline envelope.
    ///
    /// Never falls back to an external reference: an oversized or over-deep
    /// candidate fails rather than being silently inlined or silently stored
    /// elsewhere. A caller that wants external representation constructs one
    /// explicitly with [`Self::external`].
    ///
    /// # Errors
    ///
    /// Returns a redacted codec, bounds, or shape failure.
    pub fn encode<T>(
        namespace: ComponentStreamIdentity,
        value: &T,
        codec: &(impl ComponentStateCodec<T> + ?Sized),
        limits: StateLimits,
    ) -> Result<Self, ComponentStateError> {
        let payload_bytes = codec.encode(value).map_err(ComponentStateError::Codec)?;
        if payload_bytes.len() > limits.maximum_bytes() {
            return Err(ComponentStateError::TooLarge {
                max_bytes: limits.maximum_bytes(),
            });
        }
        let schema_id = codec.schema_id().clone();
        let schema_version = codec.current_version();
        let codec_id = codec.codec_id().clone();
        let codec_version = codec.codec_version();
        let checksum_algorithm = ChecksumAlgorithm::CURRENT;
        let checksum = checksum_algorithm.digest(&canonical_checksum_input(
            &namespace,
            &schema_id,
            schema_version,
            &codec_id,
            codec_version,
            &inline_payload_marker(&payload_bytes),
        ));
        let payload_value: Value = serde_json::from_slice(&payload_bytes)
            .map_err(|_| ComponentStateError::InvalidPayload)?;
        if !payload_value.is_object() {
            return Err(ComponentStateError::PayloadNotObject);
        }
        if json_depth(&payload_value) > limits.maximum_depth() {
            return Err(ComponentStateError::TooDeep {
                max_depth: limits.maximum_depth(),
            });
        }
        let encoded_len = payload_bytes.len();
        Ok(Self {
            namespace,
            schema_id,
            schema_version,
            codec_id,
            codec_version,
            checksum_algorithm,
            checksum,
            payload: StoredPayload::Inline(payload_bytes),
            encoded_len,
        })
    }

    /// Constructs an envelope wrapping an already-stored external reference.
    ///
    /// The caller is responsible for having already stored the bytes through
    /// an [`ExternalStateStore`] and for supplying the identity/version the
    /// reference was encoded under.
    #[must_use]
    pub fn external(
        namespace: ComponentStreamIdentity,
        schema_id: StateSchemaId,
        schema_version: StateSchemaVersion,
        codec_id: CodecId,
        codec_version: CodecVersion,
        reference: ExternalStateReference,
    ) -> Self {
        let checksum_algorithm = ChecksumAlgorithm::CURRENT;
        let checksum = checksum_algorithm.digest(&canonical_checksum_input(
            &namespace,
            &schema_id,
            schema_version,
            &codec_id,
            codec_version,
            &external_payload_marker(&reference),
        ));
        Self {
            namespace,
            schema_id,
            schema_version,
            codec_id,
            codec_version,
            checksum_algorithm,
            checksum,
            payload: StoredPayload::External(reference),
            encoded_len: 0,
        }
    }

    /// Validates raw durable columns and reconstructs an undecoded envelope.
    ///
    /// The checksum is verified against the raw payload bytes before any JSON
    /// parsing, schema/codec compatibility check, or migration step runs. A
    /// mismatch never invokes decode, never invokes migration, and never
    /// substitutes empty or default state.
    ///
    /// # Errors
    ///
    /// Returns a redacted identity, checksum, bounds, or shape failure.
    #[allow(clippy::too_many_arguments)]
    pub fn from_durable(
        namespace: ComponentStreamIdentity,
        schema_id: &str,
        schema_version: u32,
        codec_id: &str,
        codec_version: u32,
        checksum_algorithm: u16,
        checksum_algorithm_version: u16,
        checksum: [u8; 32],
        payload: ComponentStatePayload,
        limits: StateLimits,
    ) -> Result<Self, ComponentStateError> {
        let schema_id =
            StateSchemaId::new(schema_id).map_err(|_| ComponentStateError::Malformed)?;
        let schema_version =
            StateSchemaVersion::new(schema_version).map_err(|_| ComponentStateError::Malformed)?;
        let codec_id = CodecId::new(codec_id)?;
        let codec_version = CodecVersion::new(codec_version)?;
        let algorithm = ChecksumAlgorithm::resolve(checksum_algorithm, checksum_algorithm_version)
            .ok_or(ComponentStateError::ChecksumAlgorithmUnsupported {
                algorithm: checksum_algorithm,
                version: checksum_algorithm_version,
            })?;

        let (marker, size_check) = match &payload {
            ComponentStatePayload::Inline(bytes) => {
                (inline_payload_marker(bytes), Some(bytes.len()))
            }
            ComponentStatePayload::External(reference) => {
                (external_payload_marker(reference), None)
            }
        };
        if let Some(len) = size_check
            && len > limits.maximum_bytes()
        {
            return Err(ComponentStateError::TooLarge {
                max_bytes: limits.maximum_bytes(),
            });
        }

        // Checksum is verified here, against the raw bytes, before any JSON
        // parse, schema/codec match, or migration step below.
        let expected = algorithm.digest(&canonical_checksum_input(
            &namespace,
            &schema_id,
            schema_version,
            &codec_id,
            codec_version,
            &marker,
        ));
        if expected != checksum {
            return Err(ComponentStateError::ChecksumMismatch);
        }

        let (stored, encoded_len) = match payload {
            ComponentStatePayload::Inline(bytes) => {
                let value: Value =
                    serde_json::from_slice(&bytes).map_err(|_| ComponentStateError::Malformed)?;
                if !value.is_object() {
                    return Err(ComponentStateError::PayloadNotObject);
                }
                if json_depth(&value) > limits.maximum_depth() {
                    return Err(ComponentStateError::TooDeep {
                        max_depth: limits.maximum_depth(),
                    });
                }
                let encoded_len = bytes.len();
                (StoredPayload::Inline(bytes), encoded_len)
            }
            ComponentStatePayload::External(reference) => (StoredPayload::External(reference), 0),
        };

        Ok(Self {
            namespace,
            schema_id,
            schema_version,
            codec_id,
            codec_version,
            checksum_algorithm: algorithm,
            checksum,
            payload: stored,
            encoded_len,
        })
    }

    /// Decodes the retained inline payload through `codec`.
    ///
    /// Applies the schema-axis migration chain (delegated to the same
    /// algorithm [`crate::Checkpoint`]/[`crate::ExecutionContext`] use) and
    /// then the codec-version migration chain, in that order, before calling
    /// the codec's own decode.
    ///
    /// # Errors
    ///
    /// Returns [`ComponentStateError::ExternalReferenceMissing`] when the
    /// payload is an unresolved external reference (resolve it through an
    /// [`ExternalStateStore`] and decode the resolved bytes with the
    /// component's own codec instead), or a redacted identity,
    /// compatibility, or codec failure.
    pub fn decode<T>(
        &self,
        codec: &(impl ComponentStateCodec<T> + ?Sized),
    ) -> Result<T, ComponentStateError> {
        if self.codec_id != *codec.codec_id() {
            return Err(ComponentStateError::UnknownCodec);
        }
        if self.schema_id != *codec.schema_id() {
            return Err(ComponentStateError::SchemaMismatch);
        }
        let current_schema = codec.current_version();
        if self.schema_version > current_schema {
            return Err(ComponentStateError::UnsupportedSchemaVersion {
                found: self.schema_version.get(),
                current: current_schema.get(),
            });
        }
        let current_codec = codec.codec_version();
        if self.codec_version > current_codec {
            return Err(ComponentStateError::UnsupportedCodecVersion {
                found: self.codec_version.get(),
                current: current_codec.get(),
            });
        }
        let Self {
            payload: StoredPayload::Inline(bytes),
            ..
        } = self
        else {
            return Err(ComponentStateError::ExternalReferenceMissing);
        };
        let payload_bytes = bytes.clone();
        let payload_bytes = upgrade_schema_chain(
            DurableStateKind::ComponentState,
            self.schema_version,
            codec,
            payload_bytes,
        )
        .map_err(|error| map_schema_chain_error(&error))?;
        let payload_bytes = upgrade_codec_chain(self.codec_version, codec, payload_bytes)?;
        codec
            .decode(&payload_bytes)
            .map_err(ComponentStateError::Codec)
    }

    /// Validates and migrates this envelope to `codec`'s current schema and
    /// codec versions, without decoding to an application type.
    ///
    /// This is the pre-`open` enforcement point (Gate C): the runtime calls
    /// this through a registered stream's [`StreamStateContract`] before
    /// [`crate::ItemStream::open`](../oxide_batch/trait.ItemStream.html#tymethod.open)
    /// ever runs, so an unknown/newer schema or codec, or a migration
    /// failure, is rejected before any application code sees the envelope.
    /// An external-reference payload has no inline bytes to migrate. Current
    /// schema and codec versions are accepted; older external versions fail
    /// closed until a resolution/migration capability exists.
    ///
    /// # Errors
    ///
    /// Returns a redacted identity, compatibility, or migration failure
    /// without invoking the codec's own decode step. Older external schema or
    /// codec versions report that external migration is unsupported because
    /// this method cannot resolve their out-of-band bytes.
    pub fn validated_for_open<T>(
        &self,
        codec: &(impl ComponentStateCodec<T> + ?Sized),
    ) -> Result<Self, ComponentStateError> {
        if self.codec_id != *codec.codec_id() {
            return Err(ComponentStateError::UnknownCodec);
        }
        if self.schema_id != *codec.schema_id() {
            return Err(ComponentStateError::SchemaMismatch);
        }
        let current_schema = codec.current_version();
        if self.schema_version > current_schema {
            return Err(ComponentStateError::UnsupportedSchemaVersion {
                found: self.schema_version.get(),
                current: current_schema.get(),
            });
        }
        let current_codec = codec.codec_version();
        if self.codec_version > current_codec {
            return Err(ComponentStateError::UnsupportedCodecVersion {
                found: self.codec_version.get(),
                current: current_codec.get(),
            });
        }
        if matches!(self.payload, StoredPayload::External(_)) {
            if self.schema_version < current_schema {
                return Err(ComponentStateError::ExternalSchemaMigrationUnsupported {
                    found: self.schema_version.get(),
                    current: current_schema.get(),
                });
            }
            if self.codec_version < current_codec {
                return Err(ComponentStateError::ExternalCodecMigrationUnsupported {
                    found: self.codec_version.get(),
                    current: current_codec.get(),
                });
            }
            return Ok(self.clone());
        }
        let Self {
            payload: StoredPayload::Inline(bytes),
            ..
        } = self
        else {
            return Ok(self.clone());
        };
        let migrated = upgrade_schema_chain(
            DurableStateKind::ComponentState,
            self.schema_version,
            codec,
            bytes.clone(),
        )
        .map_err(|error| map_schema_chain_error(&error))?;
        let migrated = upgrade_codec_chain(self.codec_version, codec, migrated)?;
        if migrated == *bytes {
            return Ok(self.clone());
        }
        let checksum_algorithm = ChecksumAlgorithm::CURRENT;
        let checksum = checksum_algorithm.digest(&canonical_checksum_input(
            &self.namespace,
            &self.schema_id,
            current_schema,
            &self.codec_id,
            current_codec,
            &inline_payload_marker(&migrated),
        ));
        let encoded_len = migrated.len();
        Ok(Self {
            namespace: self.namespace.clone(),
            schema_id: self.schema_id.clone(),
            schema_version: current_schema,
            codec_id: self.codec_id.clone(),
            codec_version: current_codec,
            checksum_algorithm,
            checksum,
            payload: StoredPayload::Inline(migrated),
            encoded_len,
        })
    }

    /// Borrows the owner-scoped namespace.
    #[must_use]
    pub const fn namespace(&self) -> &ComponentStreamIdentity {
        &self.namespace
    }

    /// Borrows the validated application schema identifier.
    #[must_use]
    pub const fn schema_id(&self) -> &StateSchemaId {
        &self.schema_id
    }

    /// Returns the retained application schema version.
    #[must_use]
    pub const fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }

    /// Borrows the validated codec identifier.
    #[must_use]
    pub const fn codec_id(&self) -> &CodecId {
        &self.codec_id
    }

    /// Returns the retained codec version.
    #[must_use]
    pub const fn codec_version(&self) -> CodecVersion {
        self.codec_version
    }

    /// Returns whether this envelope's payload is inline or external.
    #[must_use]
    pub const fn is_external(&self) -> bool {
        matches!(self.payload, StoredPayload::External(_))
    }

    /// Returns the checksum algorithm identifier protecting this envelope.
    #[must_use]
    pub const fn checksum_algorithm(&self) -> u16 {
        self.checksum_algorithm.identifier()
    }

    /// Returns the checksum algorithm version protecting this envelope.
    #[must_use]
    pub const fn checksum_algorithm_version(&self) -> u16 {
        self.checksum_algorithm.algorithm_version()
    }

    /// Returns the checksum value protecting this envelope.
    ///
    /// An authorized persistence adapter uses this alongside
    /// [`Self::checksum_algorithm`]/[`Self::checksum_algorithm_version`] to
    /// write the durable checksum column; [`Self::from_durable`] recomputes
    /// and verifies it from the same inputs on read.
    #[must_use]
    pub const fn checksum(&self) -> [u8; 32] {
        self.checksum
    }

    /// Returns the bounded payload representation an authorized persistence
    /// adapter durably stores.
    ///
    /// # Errors
    ///
    /// Returns a redacted format failure if the retained inline value cannot
    /// be serialized. This does not happen for a payload this type itself
    /// already validated.
    pub fn payload(&self) -> Result<ComponentStatePayload, ComponentStateError> {
        match &self.payload {
            StoredPayload::Inline(bytes) => Ok(ComponentStatePayload::Inline(bytes.clone())),
            StoredPayload::External(reference) => Ok(ComponentStatePayload::External(*reference)),
        }
    }

    /// Returns the validated inline payload byte size, or the declared
    /// external blob size.
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        match &self.payload {
            StoredPayload::External(reference) => {
                usize::try_from(reference.encoded_len()).unwrap_or(usize::MAX)
            }
            StoredPayload::Inline(_) => self.encoded_len,
        }
    }
}

impl fmt::Debug for ComponentStateEnvelope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ComponentStateEnvelope")
            .field("namespace", &self.namespace)
            .field("schema_id", &self.schema_id)
            .field("schema_version", &self.schema_version)
            .field("codec_id", &self.codec_id)
            .field("codec_version", &self.codec_version)
            .field("checksum_algorithm", &self.checksum_algorithm)
            .field("checksum", &"<redacted>")
            .field("is_external", &self.is_external())
            .field("encoded_len", &self.encoded_len())
            .field("payload", &"<redacted>")
            .finish()
    }
}

/// A component-state codec: the application-schema axis (via the
/// [`VersionedStateCodec`] supertrait) plus the codec identity/version axis,
/// sensitivity classification, and restartability declaration.
///
/// Any existing [`VersionedStateCodec`] implementation becomes a
/// `ComponentStateCodec` unchanged by wrapping it in [`DefaultComponentCodec`]
/// -- this trait is additive, not a replacement.
pub trait ComponentStateCodec<T>: VersionedStateCodec<T> {
    /// Returns the stable codec identifier.
    fn codec_id(&self) -> &CodecId;

    /// Returns the version emitted when encoding.
    fn codec_version(&self) -> CodecVersion;

    /// Declares the directed codec-version upgrades this codec can apply.
    fn codec_upgrades(&self) -> &[CodecVersionUpgrade] {
        &[]
    }

    /// Declares this component's state sensitivity.
    ///
    /// Defaults to [`StateSensitivity::Sensitive`] (fail-safe).
    fn sensitivity(&self) -> StateSensitivity {
        StateSensitivity::Sensitive
    }

    /// Declares whether this component can honestly claim restartability.
    fn restartability(&self) -> RestartabilityDeclaration;
}

/// Wraps an existing [`VersionedStateCodec`] as a [`ComponentStateCodec`]
/// without requiring a second codec implementation.
pub struct DefaultComponentCodec<C> {
    schema: C,
    codec_id: CodecId,
    codec_version: CodecVersion,
    codec_upgrades: Vec<CodecVersionUpgrade>,
    sensitivity: StateSensitivity,
    restartability: RestartabilityDeclaration,
}

impl<C> DefaultComponentCodec<C> {
    /// Wraps `schema` with the codec identity/version and restartability
    /// declaration this component requires.
    #[must_use]
    pub const fn new(
        schema: C,
        codec_id: CodecId,
        codec_version: CodecVersion,
        restartability: RestartabilityDeclaration,
    ) -> Self {
        Self {
            schema,
            codec_id,
            codec_version,
            codec_upgrades: Vec::new(),
            sensitivity: StateSensitivity::Sensitive,
            restartability,
        }
    }

    /// Declares the directed codec-version upgrades this codec can apply.
    #[must_use]
    pub fn with_codec_upgrades(mut self, upgrades: Vec<CodecVersionUpgrade>) -> Self {
        self.codec_upgrades = upgrades;
        self
    }

    /// Declares this component's state sensitivity.
    #[must_use]
    pub const fn with_sensitivity(mut self, sensitivity: StateSensitivity) -> Self {
        self.sensitivity = sensitivity;
        self
    }
}

impl<T, C: VersionedStateCodec<T>> VersionedStateCodec<T> for DefaultComponentCodec<C> {
    fn schema_id(&self) -> &StateSchemaId {
        self.schema.schema_id()
    }

    fn current_version(&self) -> StateSchemaVersion {
        self.schema.current_version()
    }

    fn upgrades(&self) -> &[crate::StateSchemaUpgrade] {
        self.schema.upgrades()
    }

    fn encode(&self, value: &T) -> Result<Vec<u8>, StateCodecError> {
        self.schema.encode(value)
    }

    fn decode(&self, payload: &[u8]) -> Result<T, StateCodecError> {
        self.schema.decode(payload)
    }
}

impl<T, C: VersionedStateCodec<T>> ComponentStateCodec<T> for DefaultComponentCodec<C> {
    fn codec_id(&self) -> &CodecId {
        &self.codec_id
    }

    fn codec_version(&self) -> CodecVersion {
        self.codec_version
    }

    fn codec_upgrades(&self) -> &[CodecVersionUpgrade] {
        &self.codec_upgrades
    }

    fn sensitivity(&self) -> StateSensitivity {
        self.sensitivity
    }

    fn restartability(&self) -> RestartabilityDeclaration {
        self.restartability
    }
}

mod stream_contract_sealed {
    use super::{
        CodecId, ComponentStateEnvelope, ComponentStateError, RestartabilityDeclaration,
        StateSchemaId,
    };

    /// The dyn-compatible mirror a registered stream's codec/schema contract
    /// is erased behind. Nothing here is exported: the only implementor is
    /// the blanket impl in this module, so a runtime never introspects
    /// opaque application state to enforce Gate C -- the registration
    /// supplies this contract explicitly instead.
    pub trait StreamStateContractObject: Send + Sync {
        fn schema_id(&self) -> &StateSchemaId;
        fn codec_id(&self) -> &CodecId;
        fn restartability(&self) -> RestartabilityDeclaration;
        fn validate_for_open(
            &self,
            envelope: &ComponentStateEnvelope,
        ) -> Result<ComponentStateEnvelope, ComponentStateError>;
    }
}

struct CodecContract<T, C> {
    codec: C,
    // `fn() -> T` rather than `T` so this struct is `Send + Sync` regardless
    // of `T`: the codec never actually produces a `T` here, it only proves
    // one exists on the other side of `ComponentStateCodec<T>`.
    marker: PhantomData<fn() -> T>,
}

impl<T, C: ComponentStateCodec<T> + Send + Sync> stream_contract_sealed::StreamStateContractObject
    for CodecContract<T, C>
{
    fn schema_id(&self) -> &StateSchemaId {
        self.codec.schema_id()
    }

    fn codec_id(&self) -> &CodecId {
        self.codec.codec_id()
    }

    fn restartability(&self) -> RestartabilityDeclaration {
        self.codec.restartability()
    }

    fn validate_for_open(
        &self,
        envelope: &ComponentStateEnvelope,
    ) -> Result<ComponentStateEnvelope, ComponentStateError> {
        envelope.validated_for_open(&self.codec)
    }
}

/// The codec/schema/restartability contract a registered `ItemStream`
/// carries, bound to the runtime at registration time.
///
/// Gate C requires the runtime to validate a registered stream's expected
/// schema and codec identity/version, apply declared migrations, and reject
/// unknown-newer versions -- all *before* the application's `open` runs --
/// without introspecting the stream's opaque internal state. This is the
/// small, explicit descriptor that makes that possible: it preserves the
/// existing separation between the stream's logical identity
/// ([`crate::definition::ComponentStreamIdentity`]), its runtime
/// implementation (`ItemStream`/`BoxedStream`), and its state contract (this
/// type), which the stream's own [`ComponentStateCodec`] already declares.
pub struct StreamStateContract(Arc<dyn stream_contract_sealed::StreamStateContractObject>);

impl StreamStateContract {
    /// Captures `codec`'s schema/codec identity, versions, and
    /// restartability declaration as a type-erased contract.
    pub fn new<T, C>(codec: C) -> Self
    where
        C: ComponentStateCodec<T> + Send + Sync + 'static,
        T: 'static,
    {
        Self(Arc::new(CodecContract {
            codec,
            marker: PhantomData,
        }))
    }

    /// Borrows the expected application schema identifier.
    #[must_use]
    pub fn schema_id(&self) -> &StateSchemaId {
        self.0.schema_id()
    }

    /// Borrows the expected codec identifier.
    #[must_use]
    pub fn codec_id(&self) -> &CodecId {
        self.0.codec_id()
    }

    /// Returns the declared restartability of the stream this contract
    /// belongs to.
    #[must_use]
    pub fn restartability(&self) -> RestartabilityDeclaration {
        self.0.restartability()
    }

    /// Validates and migrates `envelope` to this contract's current schema
    /// and codec versions before the runtime may call `open` with it.
    ///
    /// # Errors
    ///
    /// Returns the redacted identity, compatibility, or migration failure
    /// [`ComponentStateEnvelope::validated_for_open`] would.
    pub fn validate_for_open(
        &self,
        envelope: &ComponentStateEnvelope,
    ) -> Result<ComponentStateEnvelope, ComponentStateError> {
        self.0.validate_for_open(envelope)
    }
}

impl fmt::Debug for StreamStateContract {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StreamStateContract")
            .field("schema_id", &self.schema_id())
            .field("codec_id", &self.codec_id())
            .field("restartability", &self.restartability())
            .finish()
    }
}

/// Stable, value-redacted component-state validation failure.
///
/// No variant carries payload data: every field is a scalar identity or
/// bound, mirroring [`StateError`]'s own redaction discipline.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ComponentStateError {
    /// A codec identifier was empty.
    EmptyCodecId,
    /// A codec identifier exceeded its UTF-8 byte limit.
    CodecIdTooLong {
        /// Maximum accepted UTF-8 bytes.
        max_bytes: usize,
    },
    /// A codec version was zero.
    ZeroCodecVersion,
    /// A declared codec-version upgrade did not strictly increase the
    /// version.
    NonIncreasingCodecUpgrade,
    /// A serialized value exceeded its configured byte limit.
    TooLarge {
        /// Configured maximum bytes.
        max_bytes: usize,
    },
    /// A serialized value exceeded its configured JSON depth.
    TooDeep {
        /// Configured maximum depth.
        max_depth: usize,
    },
    /// The bytes were not a valid envelope payload.
    Malformed,
    /// The envelope schema does not match the selected codec.
    SchemaMismatch,
    /// Durable data was produced by a newer application schema.
    UnsupportedSchemaVersion {
        /// Version observed in durable data.
        found: u32,
        /// Current version supported by the selected codec.
        current: u32,
    },
    /// The codec declares no directed schema upgrade reaching its current
    /// version.
    NoSchemaUpgradePath {
        /// Version the chain stalled at.
        found: u32,
        /// Current version supported by the selected codec.
        current: u32,
    },
    /// Two declared schema upgrades leave the same version.
    AmbiguousSchemaUpgrade {
        /// Version left by more than one declared upgrade.
        from: u32,
    },
    /// A declared schema upgrade produces a version past the codec's current
    /// version.
    SchemaUpgradeOvershootsCurrent {
        /// Version the rejected edge produces.
        to: u32,
        /// Current version supported by the selected codec.
        current: u32,
    },
    /// The resolved schema-upgrade chain exceeded its bound.
    SchemaUpgradeChainTooLong {
        /// Most upgrades one decode may apply.
        max_upgrades: usize,
    },
    /// The envelope's codec identity does not match the selected codec.
    UnknownCodec,
    /// Durable data was produced by a newer codec version.
    UnsupportedCodecVersion {
        /// Version observed in durable data.
        found: u32,
        /// Current version supported by the selected codec.
        current: u32,
    },
    /// An older external payload cannot be migrated without resolving its
    /// out-of-band bytes.
    ExternalSchemaMigrationUnsupported {
        /// Version recorded in the external envelope.
        found: u32,
        /// Current version supported by the selected codec.
        current: u32,
    },
    /// An older external payload cannot be migrated without resolving its
    /// out-of-band bytes.
    ExternalCodecMigrationUnsupported {
        /// Version recorded in the external envelope.
        found: u32,
        /// Current version supported by the selected codec.
        current: u32,
    },
    /// The codec declares no directed codec-version upgrade reaching its
    /// current version.
    NoCodecUpgradePath {
        /// Version the chain stalled at.
        found: u32,
        /// Current version supported by the selected codec.
        current: u32,
    },
    /// Two declared codec-version upgrades leave the same version.
    AmbiguousCodecUpgrade {
        /// Version left by more than one declared upgrade.
        from: u32,
    },
    /// A declared codec-version upgrade produces a version past the codec's
    /// current version.
    CodecUpgradeOvershootsCurrent {
        /// Version the rejected edge produces.
        to: u32,
        /// Current version supported by the selected codec.
        current: u32,
    },
    /// The resolved codec-upgrade chain exceeded its bound.
    CodecUpgradeChainTooLong {
        /// Most upgrades one decode may apply.
        max_upgrades: usize,
    },
    /// The declared checksum algorithm identity/version is not supported.
    ChecksumAlgorithmUnsupported {
        /// Algorithm identifier observed in durable data.
        algorithm: u16,
        /// Algorithm version observed in durable data.
        version: u16,
    },
    /// The computed checksum does not match the recorded checksum.
    ///
    /// No decode and no migration were attempted.
    ChecksumMismatch,
    /// The payload is an external reference that has not been resolved.
    ExternalReferenceMissing,
    /// Resolved external content did not match its declared content
    /// identity.
    ExternalReferenceContentMismatch,
    /// The application payload was not valid JSON.
    InvalidPayload,
    /// The application payload was valid JSON but not an object.
    PayloadNotObject,
    /// The application codec rejected the payload without exposing its
    /// value.
    Codec(StateCodecError),
}

impl fmt::Display for ComponentStateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyCodecId => {
                formatter.write_str("component state codec identifier must not be empty")
            }
            Self::CodecIdTooLong { max_bytes } => {
                write!(
                    formatter,
                    "component state codec identifier exceeds {max_bytes} UTF-8 bytes"
                )
            }
            Self::ZeroCodecVersion => {
                formatter.write_str("component state codec version must be nonzero")
            }
            Self::NonIncreasingCodecUpgrade => {
                formatter.write_str("component state codec upgrade must increase the version")
            }
            Self::TooLarge { max_bytes } => {
                write!(formatter, "component state exceeds {max_bytes} bytes")
            }
            Self::TooDeep { max_depth } => {
                write!(formatter, "component state exceeds JSON depth {max_depth}")
            }
            Self::Malformed => formatter.write_str("component state is malformed"),
            Self::SchemaMismatch => {
                formatter.write_str("component state schema does not match the component")
            }
            Self::UnsupportedSchemaVersion { .. } => {
                formatter.write_str("component state schema version is unsupported")
            }
            Self::NoSchemaUpgradePath { .. } => {
                formatter.write_str("component state schema version has no upgrade path")
            }
            Self::AmbiguousSchemaUpgrade { .. } => {
                formatter.write_str("component state schema upgrade is ambiguous")
            }
            Self::SchemaUpgradeOvershootsCurrent { .. } => {
                formatter.write_str("component state schema upgrade passes the current version")
            }
            Self::SchemaUpgradeChainTooLong { max_upgrades } => {
                write!(
                    formatter,
                    "component state schema upgrade chain exceeds {max_upgrades} upgrades"
                )
            }
            Self::UnknownCodec => formatter.write_str("component state codec is unknown"),
            Self::UnsupportedCodecVersion { .. } => {
                formatter.write_str("component state codec version is unsupported")
            }
            Self::ExternalSchemaMigrationUnsupported { .. } => {
                formatter.write_str("external component state schema migration is unsupported")
            }
            Self::ExternalCodecMigrationUnsupported { .. } => {
                formatter.write_str("external component state codec migration is unsupported")
            }
            Self::NoCodecUpgradePath { .. } => {
                formatter.write_str("component state codec version has no upgrade path")
            }
            Self::AmbiguousCodecUpgrade { .. } => {
                formatter.write_str("component state codec upgrade is ambiguous")
            }
            Self::CodecUpgradeOvershootsCurrent { .. } => {
                formatter.write_str("component state codec upgrade passes the current version")
            }
            Self::CodecUpgradeChainTooLong { max_upgrades } => {
                write!(
                    formatter,
                    "component state codec upgrade chain exceeds {max_upgrades} upgrades"
                )
            }
            Self::ChecksumAlgorithmUnsupported { .. } => {
                formatter.write_str("component state checksum algorithm is unsupported")
            }
            Self::ChecksumMismatch => {
                formatter.write_str("component state checksum does not match")
            }
            Self::ExternalReferenceMissing => {
                formatter.write_str("component state external reference is missing")
            }
            Self::ExternalReferenceContentMismatch => {
                formatter.write_str("component state external content identity mismatch")
            }
            Self::InvalidPayload => {
                formatter.write_str("component state payload is not valid JSON")
            }
            Self::PayloadNotObject => {
                formatter.write_str("component state payload must be a JSON object")
            }
            Self::Codec(error) => error.fmt(formatter),
        }
    }
}

impl Error for ComponentStateError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Codec(error) => Some(error),
            _ => None,
        }
    }
}
