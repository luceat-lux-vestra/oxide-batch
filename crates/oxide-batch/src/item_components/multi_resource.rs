//! Multi-resource reader/writer components (#150, `ITEM-MULTI-001`).
//!
//! Multiple physical resources (files, objects, ...) that together form one
//! logical input or output must remain one ordered, restartable unit -- the
//! same composition rule (Gate E) the rest of
//! [`crate::item_components`] follows, applied to a resource set instead of
//! a fixed, statically-declared delegate list.
//!
//! # Why this is not [`crate::item_components::CompositeReader`] plus a
//! checkpoint
//!
//! [`crate::item_components::CompositeReader`] already has the right
//! traversal shape -- an ordered sequence of delegates, advancing to the
//! next only once the current exhausts -- but it is documented as
//! in-memory-only and explicitly not restartable by itself, because its
//! delegates are pre-constructed by the caller and each delegate's own
//! [`crate::ItemStream`] (if any) is registered independently under a
//! statically-declared [`crate::ComponentStreamIdentity`].
//!
//! A multi-resource component cannot use that shape: the resource count is
//! often not known until runtime (a directory listing, an object-store
//! prefix), so it cannot register one [`crate::ComponentStreamIdentity`] per
//! resource against [`crate::ChunkComponentRevisions`], which requires a
//! fixed, statically-declared namespace set. [`MultiResourceReader`] and
//! [`MultiResourceWriter`] instead own exactly **one** namespace for the
//! whole ordered resource set, and construct each resource's delegate
//! on demand through a [`MultiResourceReaderOpener`]/
//! [`MultiResourceWriterOpener`] -- never more than one resource open at a
//! time (bounded, one-resource-at-a-time ownership): the current resource's
//! delegate stream is always closed before the next one is opened.
//!
//! # Nested resource lifecycle
//!
//! A resource reaching its own boundary (a reader delegate exhausting, or a
//! writer delegate rolling over) is a different event from the *enclosing*
//! step attempt reaching its terminal outcome: the boundary can be, and
//! usually is, reached before the chunk transaction in flight when it is
//! discovered has committed. So the retiring delegate's stream is closed
//! right there, with [`crate::StreamRuntimeOutcome::ResourceBoundary`] --
//! never [`crate::StreamRuntimeOutcome::Committed`], which would falsely
//! claim the outer step attempt itself had reached a durable, terminal
//! commit. A close failure at a boundary is propagated as a read/write
//! error rather than silently advancing to the next resource, so the
//! checkpoint (produced by [`crate::ItemStream::update`], which runs only
//! after a chunk's work -- including any resource transition -- has fully
//! succeeded) never advances past a resource whose close failed. Every
//! resource this module opens is closed exactly once: at its own boundary
//! if it retires mid-attempt, or by this module's outer
//! [`crate::ItemStream::close`] if it is still active when the step
//! attempt's own terminal outcome is known -- never both.
//!
//! # Durable position
//!
//! The durable envelope this module's paired [`crate::ItemStream`] halves
//! produce is exactly three things:
//!
//! - [`ResourceSetRevision`]: a content fingerprint over the ordered
//!   resource identity sequence. A restart whose caller-supplied resource
//!   set no longer matches this revision fails closed
//!   ([`crate::FailureCategory::UnsupportedCapability`]) instead of silently
//!   reinterpreting a stored resource index against a different physical
//!   resource -- inserting or removing a resource ahead of the committed
//!   index is exactly the case this guards.
//! - the current resource's ordinal index.
//! - the current resource's own delegate position, embedded verbatim,
//!   including its namespace.
//!
//! That last point is what lets this module reuse the existing M6
//! component-state contract exactly, rather than inventing a second state
//! mechanism: every durable column [`crate::ComponentStateEnvelope`] carries
//! (namespace, schema id/version, codec id/version, checksum
//! algorithm/value, and the bounded payload) is a public accessor, and
//! [`crate::ComponentStateEnvelope::from_durable`] reconstructs an envelope
//! from exactly those columns. So the delegate reader/writer this module
//! opens for the current resource -- typically the same
//! `(component, stream, contract)` triple a first-party constructor like
//! [`crate::item_components::delimited_reader`] already returns -- has its
//! *own* candidate envelope captured by calling its `ItemStream::update`
//! internally, and that envelope's durable columns are embedded as plain
//! data inside this module's own envelope. Before that embedding happens,
//! the delegate's reported namespace is checked against the identity this
//! module's opener assigned it: a mismatch fails the update closed rather
//! than let an invalid nested candidate become part of this module's own
//! outer candidate, since the core runtime's own fail-closed namespace
//! check has no visibility into state nested this way. On restart, the
//! reverse happens: the embedded columns are used to reconstruct the
//! delegate's envelope via `from_durable` (re-verifying its checksum) --
//! but only after the *stored* namespace is re-checked against the same
//! expected identity, so a corrupted or hand-crafted durable record cannot
//! be silently normalized into a valid one either. Once reconstructed, the
//! delegate's own `ItemStream::open` restores its position from it, unaware
//! it is nested inside another component's state at all. No delegate state
//! is hidden -- it is carried in full, just not under a second, separately
//! registered namespace, because there is no separate registration to hide
//! it from.
//!
//! Everything before the current resource is implicitly fully committed
//! (this module never revisits a resource once it advances past it, exactly
//! like [`crate::item_components::CompositeReader`]); everything after has
//! not started. So the durable envelope only ever needs to describe the one
//! current resource, never the whole set's progress.
//!
//! # Object storage
//!
//! [`crate::item_components::object_store`] provides the M6-basics
//! provider-neutral object capability (get/put/stat/list) this module's
//! opener traits can be implemented against; full S3/Azure/GCS certification
//! remains M9 (`IO-OBJECT-001`).

use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use sha2::{Digest, Sha256};
use tokio::sync::Mutex as AsyncMutex;

use crate::{
    ComponentStateEnvelope, ComponentStatePayload, ComponentStreamIdentity, ContentIdentity,
    DefaultComponentCodec, ExternalStateReference, FailureCategory, ItemReader, ItemStream,
    ItemWriter, ReadContext, ReadOutcome, ReaderError, RestartabilityDeclaration, StateCodecError,
    StateLimits, StateSchemaId, StateSchemaVersion, StateSensitivity, StreamCloseContext,
    StreamCloseError, StreamCloseOutcome, StreamOpenContext, StreamOpenError, StreamOpenOutcome,
    StreamRuntimeOutcome, StreamStateContract, StreamUpdateContext, StreamUpdateError,
    VersionedStateCodec, WriteContext, WriteOutcome, WriterError,
};

// ---------------------------------------------------------------------
// Resource identity and resource-set revision
// ---------------------------------------------------------------------

const MAX_RESOURCE_IDENTITY_BYTES: usize = 1024;

/// A stable, caller-declared identity for one physical resource within an
/// ordered resource set.
///
/// Never derive this from filesystem iteration order, an object-store
/// listing's provider-dependent order, or any other runtime-unstable source
/// -- supply these in the exact logical order the resource set represents.
/// A path or object key is an acceptable identity (this is diagnostic
/// metadata, like [`crate::CodecId`], not covered by component-state
/// payload sensitivity rules); a caller with a stricter sensitivity
/// requirement supplies an opaque logical name instead.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ResourceIdentity(String);

impl ResourceIdentity {
    /// Validates a stable resource identity.
    ///
    /// # Errors
    ///
    /// Returns [`MultiResourceConfigError`] when `value` is empty, exceeds
    /// 1024 UTF-8 bytes, or contains a control character.
    pub fn new(value: impl Into<String>) -> Result<Self, MultiResourceConfigError> {
        let value = value.into();
        if value.is_empty() {
            return Err(MultiResourceConfigError::EmptyResourceIdentity);
        }
        if value.len() > MAX_RESOURCE_IDENTITY_BYTES {
            return Err(MultiResourceConfigError::ResourceIdentityTooLong {
                max_bytes: MAX_RESOURCE_IDENTITY_BYTES,
            });
        }
        if value.chars().any(char::is_control) {
            return Err(MultiResourceConfigError::MalformedResourceIdentity);
        }
        Ok(Self(value))
    }

    /// Borrows the validated identity.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ResourceIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ResourceIdentity")
            .field(&self.0)
            .finish()
    }
}

impl fmt::Display for ResourceIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// A content fingerprint over one ordered resource-identity sequence.
///
/// Two resource sets with the same identities in a different order, or a
/// different resource count, produce different revisions. Safe to display:
/// this is a hash of logical identities, never resource content.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct ResourceSetRevision([u8; 32]);

impl ResourceSetRevision {
    /// Computes the revision of an ordered resource-identity sequence.
    #[must_use]
    pub fn of<'a>(resources: impl IntoIterator<Item = &'a ResourceIdentity>) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(b"oxide-batch.resource-set\0");
        for resource in resources {
            hasher.update(resource.as_str().as_bytes());
            hasher.update([0u8]);
        }
        Self(hasher.finalize().into())
    }

    fn to_hex(self) -> String {
        hex_encode(&self.0)
    }

    fn from_hex(hex: &str) -> Option<Self> {
        let bytes = hex_decode(hex)?;
        let array: [u8; 32] = bytes.try_into().ok()?;
        Some(Self(array))
    }
}

impl fmt::Debug for ResourceSetRevision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "ResourceSetRevision({})", self.to_hex())
    }
}

/// An ordered, fingerprinted set of resources forming one logical
/// multi-resource input or output.
#[derive(Clone)]
pub struct ResourceSet {
    resources: Arc<[ResourceIdentity]>,
    revision: ResourceSetRevision,
}

impl ResourceSet {
    /// Builds a resource set from an explicit, caller-ordered sequence.
    #[must_use]
    pub fn new(resources: Vec<ResourceIdentity>) -> Self {
        let revision = ResourceSetRevision::of(resources.iter());
        Self {
            resources: Arc::from(resources),
            revision,
        }
    }

    /// Borrows the ordered resource identities.
    #[must_use]
    pub fn resources(&self) -> &[ResourceIdentity] {
        &self.resources
    }

    /// Returns this resource set's content revision.
    #[must_use]
    pub const fn revision(&self) -> ResourceSetRevision {
        self.revision
    }

    /// Returns the number of resources in this set.
    #[must_use]
    pub fn len(&self) -> usize {
        self.resources.len()
    }

    /// Returns whether this resource set has no resources.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.resources.is_empty()
    }
}

/// A validation failure building a [`ResourceIdentity`] or [`ResourceSet`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MultiResourceConfigError {
    /// A resource identity was empty.
    EmptyResourceIdentity,
    /// A resource identity exceeded its UTF-8 byte limit.
    ResourceIdentityTooLong {
        /// Maximum accepted UTF-8 bytes.
        max_bytes: usize,
    },
    /// A resource identity contained a control character.
    MalformedResourceIdentity,
}

impl fmt::Display for MultiResourceConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyResourceIdentity => {
                formatter.write_str("resource identity must not be empty")
            }
            Self::ResourceIdentityTooLong { max_bytes } => {
                write!(
                    formatter,
                    "resource identity exceeds {max_bytes} UTF-8 bytes"
                )
            }
            Self::MalformedResourceIdentity => {
                formatter.write_str("resource identity contains a control character")
            }
        }
    }
}

impl std::error::Error for MultiResourceConfigError {}

// ---------------------------------------------------------------------
// Hex helpers (no base64 dependency in this crate; payloads here are tiny
// position records, never item content, so the ~2x hex overhead is
// immaterial against the default 64 KiB envelope bound)
// ---------------------------------------------------------------------

fn hex_encode(bytes: &[u8]) -> String {
    use fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn hex_decode(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(text.len() / 2);
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let hi = (bytes[i] as char).to_digit(16)?;
        let lo = (bytes[i + 1] as char).to_digit(16)?;
        out.push(u8::try_from((hi << 4) | lo).ok()?);
        i += 2;
    }
    Some(out)
}

// ---------------------------------------------------------------------
// Nested delegate envelope columns
// ---------------------------------------------------------------------

/// The durable columns of one delegate [`crate::ComponentStateEnvelope`],
/// embedded verbatim inside this module's own envelope.
///
/// `namespace` is carried alongside the other columns specifically so a
/// restore can verify -- not merely assume -- that the delegate which
/// produced these columns actually reported the identity the
/// multi-resource opener assigned it (see
/// [`MultiResourceReaderStream::open`]/[`MultiResourceWriterStream::open`]'s
/// namespace check). Reconstructing the delegate envelope with the
/// caller-expected namespace regardless of what was actually stored would
/// normalize a bad namespace into a good one and bypass the core runtime's
/// fail-closed namespace-mismatch invariant.
#[derive(Clone)]
struct DelegateEnvelopeColumns {
    namespace: String,
    schema_id: String,
    schema_version: u32,
    codec_id: String,
    codec_version: u32,
    checksum_algorithm: u16,
    checksum_algorithm_version: u16,
    checksum: [u8; 32],
    is_external: bool,
    payload_inline_hex: Option<String>,
    payload_external_content_id_hex: Option<String>,
    payload_external_len: Option<u64>,
}

impl DelegateEnvelopeColumns {
    fn from_envelope(envelope: &ComponentStateEnvelope) -> Result<Self, StateCodecError> {
        let payload = envelope
            .payload()
            .map_err(|_| StateCodecError::InvalidPayload)?;
        let (payload_inline_hex, payload_external_content_id_hex, payload_external_len) =
            match payload {
                ComponentStatePayload::Inline(bytes) => (Some(hex_encode(&bytes)), None, None),
                ComponentStatePayload::External(reference) => (
                    None,
                    Some(hex_encode(reference.content_id().as_bytes())),
                    Some(reference.encoded_len()),
                ),
            };
        Ok(Self {
            namespace: envelope.namespace().as_str().to_owned(),
            schema_id: envelope.schema_id().as_str().to_owned(),
            schema_version: envelope.schema_version().get(),
            codec_id: envelope.codec_id().as_str().to_owned(),
            codec_version: envelope.codec_version().get(),
            checksum_algorithm: envelope.checksum_algorithm(),
            checksum_algorithm_version: envelope.checksum_algorithm_version(),
            checksum: envelope.checksum(),
            is_external: envelope.is_external(),
            payload_inline_hex,
            payload_external_content_id_hex,
            payload_external_len,
        })
    }

    /// Returns whether this record's stored namespace exactly matches
    /// `expected` -- the delegate identity the multi-resource opener
    /// assigned. Callers must reject a mismatch fail-closed rather than
    /// reconstruct the envelope anyway.
    fn namespace_matches(&self, expected: &ComponentStreamIdentity) -> bool {
        self.namespace == expected.as_str()
    }

    fn to_envelope(
        &self,
        namespace: ComponentStreamIdentity,
        limits: StateLimits,
    ) -> Option<ComponentStateEnvelope> {
        let payload = if self.is_external {
            let content_id_hex = self.payload_external_content_id_hex.as_deref()?;
            let bytes = hex_decode(content_id_hex)?;
            let array: [u8; 32] = bytes.try_into().ok()?;
            let content_id = ContentIdentity::from_bytes(array);
            let len = self.payload_external_len?;
            ComponentStatePayload::External(ExternalStateReference::new(content_id, len))
        } else {
            let hex = self.payload_inline_hex.as_deref()?;
            ComponentStatePayload::Inline(hex_decode(hex)?)
        };
        ComponentStateEnvelope::from_durable(
            namespace,
            &self.schema_id,
            self.schema_version,
            &self.codec_id,
            self.codec_version,
            self.checksum_algorithm,
            self.checksum_algorithm_version,
            self.checksum,
            payload,
            limits,
        )
        .ok()
    }
}

// ---------------------------------------------------------------------
// Outer multi-resource state
// ---------------------------------------------------------------------

#[derive(Clone)]
struct MultiResourceState {
    resource_set_revision: ResourceSetRevision,
    resource_index: u32,
    delegate: Option<DelegateEnvelopeColumns>,
    /// Batches committed to the current resource so far. Durable so
    /// [`RolloverPolicy::should_roll_over`] sees the true count across a
    /// restart -- an in-memory-only counter would silently reset to `0` on
    /// every restart and let a resource accumulate unboundedly many more
    /// batches than its policy allows. Unused (always `0`) on the reader
    /// path, which has no rollover decision to make.
    resource_batches_written: u64,
}

fn delegate_columns_to_json(delegate: &DelegateEnvelopeColumns) -> serde_json::Value {
    serde_json::json!({
        "namespace": delegate.namespace,
        "schema_id": delegate.schema_id,
        "schema_version": delegate.schema_version,
        "codec_id": delegate.codec_id,
        "codec_version": delegate.codec_version,
        "checksum_algorithm": delegate.checksum_algorithm,
        "checksum_algorithm_version": delegate.checksum_algorithm_version,
        "checksum": hex_encode(&delegate.checksum),
        "is_external": delegate.is_external,
        "payload_inline": delegate.payload_inline_hex,
        "payload_external_content_id": delegate.payload_external_content_id_hex,
        "payload_external_len": delegate.payload_external_len,
    })
}

fn delegate_columns_from_json(value: &serde_json::Value) -> Option<DelegateEnvelopeColumns> {
    let checksum_hex = value.get("checksum")?.as_str()?;
    let checksum_bytes = hex_decode(checksum_hex)?;
    let checksum: [u8; 32] = checksum_bytes.try_into().ok()?;
    Some(DelegateEnvelopeColumns {
        namespace: value.get("namespace")?.as_str()?.to_owned(),
        schema_id: value.get("schema_id")?.as_str()?.to_owned(),
        schema_version: u32::try_from(value.get("schema_version")?.as_u64()?).ok()?,
        codec_id: value.get("codec_id")?.as_str()?.to_owned(),
        codec_version: u32::try_from(value.get("codec_version")?.as_u64()?).ok()?,
        checksum_algorithm: u16::try_from(value.get("checksum_algorithm")?.as_u64()?).ok()?,
        checksum_algorithm_version: u16::try_from(
            value.get("checksum_algorithm_version")?.as_u64()?,
        )
        .ok()?,
        checksum,
        is_external: value.get("is_external")?.as_bool()?,
        payload_inline_hex: value
            .get("payload_inline")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned),
        payload_external_content_id_hex: value
            .get("payload_external_content_id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned),
        payload_external_len: value
            .get("payload_external_len")
            .and_then(serde_json::Value::as_u64),
    })
}

const MULTI_RESOURCE_SCHEMA: &str = "oxide-batch.multi-resource-position";
const MULTI_RESOURCE_CODEC: &str = "oxide-batch.multi-resource-position-codec";

#[derive(Clone, Copy)]
struct MultiResourcePositionSchema;

impl VersionedStateCodec<MultiResourceState> for MultiResourcePositionSchema {
    fn schema_id(&self) -> &StateSchemaId {
        static SCHEMA: std::sync::OnceLock<StateSchemaId> = std::sync::OnceLock::new();
        #[allow(
            clippy::unwrap_used,
            reason = "fixed literal schema identity cannot fail validation"
        )]
        SCHEMA.get_or_init(|| StateSchemaId::new(MULTI_RESOURCE_SCHEMA).unwrap())
    }

    fn current_version(&self) -> StateSchemaVersion {
        #[allow(
            clippy::unwrap_used,
            reason = "fixed literal schema version cannot fail validation"
        )]
        StateSchemaVersion::new(1).unwrap()
    }

    fn encode(&self, value: &MultiResourceState) -> Result<Vec<u8>, StateCodecError> {
        let delegate = value.delegate.as_ref().map(delegate_columns_to_json);
        serde_json::to_vec(&serde_json::json!({
            "resource_set_revision": value.resource_set_revision.to_hex(),
            "resource_index": value.resource_index,
            "delegate": delegate,
            "resource_batches_written": value.resource_batches_written,
        }))
        .map_err(|_| StateCodecError::InvalidPayload)
    }

    fn decode(&self, payload: &[u8]) -> Result<MultiResourceState, StateCodecError> {
        let value: serde_json::Value =
            serde_json::from_slice(payload).map_err(|_| StateCodecError::InvalidPayload)?;
        let revision_hex = value
            .get("resource_set_revision")
            .and_then(serde_json::Value::as_str)
            .ok_or(StateCodecError::InvalidPayload)?;
        let resource_set_revision =
            ResourceSetRevision::from_hex(revision_hex).ok_or(StateCodecError::InvalidPayload)?;
        let resource_index = u32::try_from(
            value
                .get("resource_index")
                .and_then(serde_json::Value::as_u64)
                .ok_or(StateCodecError::InvalidPayload)?,
        )
        .map_err(|_| StateCodecError::InvalidPayload)?;
        let delegate = match value.get("delegate") {
            None | Some(serde_json::Value::Null) => None,
            Some(delegate_value) => Some(
                delegate_columns_from_json(delegate_value)
                    .ok_or(StateCodecError::InvalidPayload)?,
            ),
        };
        let resource_batches_written = value
            .get("resource_batches_written")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        Ok(MultiResourceState {
            resource_set_revision,
            resource_index,
            delegate,
            resource_batches_written,
        })
    }
}

fn multi_resource_position_codec(
    restartability: RestartabilityDeclaration,
) -> DefaultComponentCodec<MultiResourcePositionSchema> {
    #[allow(
        clippy::unwrap_used,
        reason = "fixed literal identities cannot fail validation"
    )]
    DefaultComponentCodec::new(
        MultiResourcePositionSchema,
        crate::CodecId::new(MULTI_RESOURCE_CODEC).unwrap(),
        crate::CodecVersion::new(1).unwrap(),
        restartability,
    )
    .with_sensitivity(StateSensitivity::NonSensitive)
}

// ---------------------------------------------------------------------
// Open errors
// ---------------------------------------------------------------------

/// A redacted failure opening one resource by ordinal.
///
/// Carries only the resource's ordinal position and a stable
/// [`crate::FailureCategory`] -- never the underlying I/O error's payload,
/// path, or message, per this crate's component-error redaction
/// convention.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MultiResourceOpenError {
    resource_ordinal: u32,
    category: FailureCategory,
}

impl MultiResourceOpenError {
    /// Constructs a value-redacted [`FailureCategory::UserComponent`]
    /// open failure for the resource at `resource_ordinal`.
    #[must_use]
    pub const fn new(resource_ordinal: u32) -> Self {
        Self {
            resource_ordinal,
            category: FailureCategory::UserComponent,
        }
    }

    /// Constructs an open failure that declares its own stable category.
    #[must_use]
    pub const fn with_category(resource_ordinal: u32, category: FailureCategory) -> Self {
        Self {
            resource_ordinal,
            category,
        }
    }

    /// Returns the zero-based ordinal of the resource that failed to open.
    #[must_use]
    pub const fn resource_ordinal(self) -> u32 {
        self.resource_ordinal
    }

    /// Returns the stable category supplied by the opener.
    #[must_use]
    pub const fn category(self) -> FailureCategory {
        self.category
    }
}

impl fmt::Display for MultiResourceOpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "multi-resource open failed at resource ordinal {}",
            self.resource_ordinal
        )
    }
}

impl std::error::Error for MultiResourceOpenError {}

// ---------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------

/// Opens one physical resource on demand for a [`MultiResourceReader`],
/// producing a fresh delegate reader paired with its own
/// [`crate::ItemStream`] half and state contract -- exactly the
/// `(component, stream, contract)` triple every restartable first-party
/// reader (e.g. [`crate::item_components::delimited_reader`]) already
/// returns.
///
/// `delegate_identity` is the fixed namespace [`MultiResourceReader`]
/// reuses for every resource's delegate envelope: it is never separately
/// registered with the runtime (there is exactly one outer namespace for
/// the whole resource set), so implementations should pass it straight
/// through to whichever first-party constructor they call.
pub trait MultiResourceReaderOpener<I>: Send + Sync {
    /// The delegate reader this opener produces.
    type Reader: ItemReader<I>;
    /// The delegate reader's own [`crate::ItemStream`] half.
    type Stream: ItemStream;

    /// Opens `resource` fresh (its own delegate position starts at the
    /// beginning; [`MultiResourceReader`] drives the delegate's own
    /// `ItemStream::open` separately once this returns).
    ///
    /// # Errors
    ///
    /// Returns a redacted [`MultiResourceOpenError`] naming `resource`'s
    /// ordinal.
    fn open<'a>(
        &'a self,
        resource: &'a ResourceIdentity,
        resource_ordinal: u32,
        delegate_identity: &'a ComponentStreamIdentity,
    ) -> impl Future<
        Output = Result<(Self::Reader, Self::Stream, StreamStateContract), MultiResourceOpenError>,
    > + Send
    + 'a;
}

/// The shared half of a `(reader, stream)` pair: the delegate's own
/// [`crate::ItemStream`] handle, current resource ordinal, and state
/// contract.
///
/// Deliberately excludes the delegate *reader*: [`crate::ItemReader`] is
/// `Send`-only (never `Sync`, by design -- it is always used exclusively),
/// so a delegate reader can never live behind a lock also borrowed across
/// an `.await` by [`MultiResourceReaderStream`]'s `&self` methods without
/// making every future here non-`Send`. The delegate reader instead lives
/// directly on [`MultiResourceReader`] itself, owned exclusively, and is
/// kept in lock-step with this handle by updating both together, in the
/// same critical section, at every resource transition.
struct StreamHandle<S> {
    index: u32,
    stream: S,
    /// `true` once this handle's `stream` has already been closed at a
    /// resource boundary (see [`crate::StreamRuntimeOutcome::ResourceBoundary`]).
    /// A handle is replaced (never mutated back to `false` in place) once
    /// the transition to the next resource completes, so this flag exists
    /// only to let [`MultiResourceReaderStream::close`] recognize -- and
    /// skip -- a handle whose stream a resource-transition attempt already
    /// closed but has not yet been able to replace (the transition's own
    /// next-resource open failed and is pending retry). Without this, the
    /// outer terminal close would close the same already-closed delegate a
    /// second time.
    retired: bool,
}

struct ReaderShared<I, O: MultiResourceReaderOpener<I>> {
    opener: O,
    resources: ResourceSet,
    identity: ComponentStreamIdentity,
    handle: AsyncMutex<Option<StreamHandle<O::Stream>>>,
    /// A one-shot handoff slot: [`MultiResourceReaderStream::open`]
    /// constructs the delegate reader for the attempt's resume resource
    /// (position already restored via the delegate's own `ItemStream::open`)
    /// and stashes it here; [`MultiResourceReader::read`] takes it on its
    /// first call. A `std::sync::Mutex` is sufficient (never `tokio`'s):
    /// both sides only ever lock it for a synchronous put/take, never
    /// across an `.await`, so it does not need `O::Reader: Sync`.
    pending_reader: std::sync::Mutex<Option<O::Reader>>,
    limits: StateLimits,
    _marker: PhantomData<fn() -> I>,
}

/// Reads an ordered set of physical resources as one logical, restartable
/// input.
///
/// # Contract
///
/// - **Input/output**: `I`, same as every delegate the opener produces.
/// - **State/checkpoint**: [`ResourceSetRevision`] plus the current
///   resource's ordinal and embedded delegate position, through the paired
///   [`MultiResourceReaderStream`]. A resource-set revision mismatch on
///   restart fails closed rather than reinterpreting a stored index against
///   a different physical resource.
/// - **Ordering**: resources are traversed in [`ResourceSet`]'s declared
///   order; within a resource, delegate order is preserved.
/// - **Restartability**: the meet of this wrapper's own (always
///   restartable, since the durable envelope is self-contained) and the
///   opener's declared restartability, supplied explicitly at construction
///   ([`multi_resource_reader`]'s `restartability` parameter) because a
///   resource backend's stable-identity guarantee (e.g. an object store
///   without version tokens) cannot be introspected from here.
/// - **Thread safety**: used exclusively (`&mut self`) like every reader;
///   internally synchronized with its paired stream via an async-aware
///   lock, since the stream (`&self`) must observe the same active
///   resource's position at a commit boundary.
/// - **Bounded resource**: at most one resource's delegate open at a time;
///   the current resource's delegate stream is always closed (with
///   [`crate::StreamRuntimeOutcome::ResourceBoundary`]) before the next
///   resource is opened, never overlapping.
/// - **Close**: a resource exhausted mid-attempt is closed immediately,
///   right there, rather than held open until the outer step attempt ends;
///   the paired [`MultiResourceReaderStream::close`] closes whichever
///   resource (if any) is still active when the step attempt reaches its
///   own terminal outcome, and never re-closes a resource already closed at
///   a boundary.
/// - **Support tier**: first-party.
pub struct MultiResourceReader<I, O: MultiResourceReaderOpener<I>> {
    shared: Arc<ReaderShared<I, O>>,
    /// Owned exclusively by this half; see [`StreamHandle`]'s docs for why
    /// the delegate reader cannot live in the shared, lock-guarded state.
    ///
    /// `None` whenever [`Self::transitioning`] is `true` (the delegate that
    /// hit `EndOfInput` has already been closed and must never be polled
    /// again) or once the whole resource set is exhausted; `Some` while a
    /// delegate is open and has not yet reached its own boundary.
    reader: Option<O::Reader>,
    /// Whether `pending_reader` has been consumed yet this attempt. The
    /// handoff happens at most once, on the first `read` call; every later
    /// `None` in `reader` is a mid-transition state the loop below resolves
    /// itself, never a second reason to consult `pending_reader`.
    started: bool,
    /// `true` between closing an exhausted resource's delegate stream and
    /// completing the transition to the next resource (or to fully
    /// exhausted). Gates the loop below so a retried `read` -- after the
    /// transition's own open step failed -- resumes the transition instead
    /// of polling the already-closed delegate's `ItemReader` half again.
    transitioning: bool,
}

impl<I, O> ItemReader<I> for MultiResourceReader<I, O>
where
    I: Send + 'static,
    O: MultiResourceReaderOpener<I> + 'static,
{
    async fn read(&mut self, context: ReadContext<'_>) -> Result<ReadOutcome<I>, ReaderError> {
        if !self.started {
            self.started = true;
            let taken = self
                .shared
                .pending_reader
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            match taken {
                Some(reader) => self.reader = Some(reader),
                None => {
                    // Nothing was ever handed off: either the resource set
                    // is empty, or the restored position had already
                    // exhausted every resource before this attempt began.
                    return Ok(ReadOutcome::EndOfInput);
                }
            }
        }
        loop {
            if !self.transitioning {
                let Some(reader) = self.reader.as_mut() else {
                    // Once `started`, `reader` is `None` and not
                    // transitioning if and only if the terminal exhaustion
                    // branch below has already run: every repeated call
                    // after that must keep reporting `EndOfInput`, exactly
                    // like `CompositeReader` once its delegate list is
                    // exhausted, never error.
                    return Ok(ReadOutcome::EndOfInput);
                };
                match reader.read(context).await? {
                    ReadOutcome::Item(item) => return Ok(ReadOutcome::Item(item)),
                    ReadOutcome::Stopped => return Ok(ReadOutcome::Stopped),
                    ReadOutcome::EndOfInput => {
                        // The resource this delegate reads is exhausted.
                        // Close its stream *now*, before opening anything
                        // else: `StreamRuntimeOutcome::ResourceBoundary`
                        // (never `Committed` -- the enclosing step attempt
                        // has not reached a terminal outcome yet) tells the
                        // delegate its own local work is done without
                        // falsely claiming the outer transaction committed.
                        // A close failure here must not silently advance
                        // past this resource: it is propagated as a read
                        // error, `self.reader` stays cleared, and
                        // `transitioning` stays unset so this same close is
                        // retried on the next `read` call rather than
                        // treated as already done.
                        let mut guard = self.shared.handle.lock().await;
                        let handle = guard.as_mut().ok_or_else(|| {
                            ReaderError::with_category(FailureCategory::Invariant)
                        })?;
                        handle
                            .stream
                            .close(StreamCloseContext::new(
                                context.stop_token(),
                                StreamRuntimeOutcome::ResourceBoundary,
                            ))
                            .await
                            .map_err(|error| ReaderError::with_category(error.category()))?;
                        handle.retired = true;
                        drop(guard);
                        self.reader = None;
                        self.transitioning = true;
                    }
                }
            }

            // `transitioning`: the current resource's delegate stream is
            // already closed. Complete the move to the next resource, or to
            // fully exhausted, before this call returns or loops back to
            // poll a (new) delegate reader.
            let current_index = {
                let guard = self.shared.handle.lock().await;
                guard
                    .as_ref()
                    .ok_or_else(|| ReaderError::with_category(FailureCategory::Invariant))?
                    .index
            };
            let next_index = current_index + 1;
            if next_index as usize >= self.shared.resources.len() {
                *self.shared.handle.lock().await = None;
                self.transitioning = false;
                return Ok(ReadOutcome::EndOfInput);
            }
            let next_resource = &self.shared.resources.resources()[next_index as usize];
            // The freshly opened resource has no inherited state to
            // validate (it was never touched before this attempt), so the
            // contract returned here has nothing left to do. On failure,
            // `transitioning` stays set and `reader` stays `None`: a
            // retried `read` re-attempts this same open, never re-closes
            // the already-retired previous delegate and never silently
            // reports the whole reader exhausted.
            let (new_reader, stream, _contract) = self
                .shared
                .opener
                .open(next_resource, next_index, &self.shared.identity)
                .await
                .map_err(|error| ReaderError::with_category(error.category()))?;
            stream
                .open(StreamOpenContext::new(None, context.stop_token()))
                .await
                .map_err(|_| ReaderError::new())?;
            // Both halves of the pair are updated together, in one critical
            // section, so `StreamHandle::index` never observably points at
            // a resource `self.reader` has not yet transitioned to.
            let mut guard = self.shared.handle.lock().await;
            self.reader = Some(new_reader);
            *guard = Some(StreamHandle {
                index: next_index,
                stream,
                retired: false,
            });
            drop(guard);
            self.transitioning = false;
        }
    }
}

/// The [`crate::ItemStream`] half of a [`MultiResourceReader`].
pub struct MultiResourceReaderStream<I, O: MultiResourceReaderOpener<I>> {
    shared: Arc<ReaderShared<I, O>>,
}

impl<I, O> ItemStream for MultiResourceReaderStream<I, O>
where
    I: Send + 'static,
    O: MultiResourceReaderOpener<I> + 'static,
{
    async fn open(
        &self,
        context: StreamOpenContext<'_>,
    ) -> Result<StreamOpenOutcome, StreamOpenError> {
        let codec = multi_resource_position_codec(RestartabilityDeclaration::Restartable);
        let restored = context.inherited_state().is_some();
        let (resource_index, inherited_delegate) = match context.inherited_state() {
            Some(envelope) => {
                let state = envelope
                    .decode::<MultiResourceState>(&codec)
                    .map_err(|_| StreamOpenError::new())?;
                if state.resource_set_revision != self.shared.resources.revision() {
                    return Err(StreamOpenError::with_category(
                        FailureCategory::UnsupportedCapability,
                    ));
                }
                (state.resource_index, state.delegate)
            }
            None => (0, None),
        };

        let resources_len = u32::try_from(self.shared.resources.len()).unwrap_or(u32::MAX);
        if resource_index > resources_len {
            return Err(StreamOpenError::with_category(FailureCategory::Invariant));
        }
        if resource_index == resources_len {
            *self.shared.handle.lock().await = None;
            *self
                .shared
                .pending_reader
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
            return Ok(if restored {
                StreamOpenOutcome::Restored
            } else {
                StreamOpenOutcome::Initial
            });
        }

        let resource = &self.shared.resources.resources()[resource_index as usize];
        let (reader, stream, contract) = self
            .shared
            .opener
            .open(resource, resource_index, &self.shared.identity)
            .await
            .map_err(|_| StreamOpenError::new())?;

        match inherited_delegate {
            Some(columns) => {
                // Fail closed if the delegate that produced this durable
                // record did not actually report the namespace this opener
                // assigned it -- reconstructing the envelope under the
                // expected namespace regardless would normalize a bad
                // namespace into a good one (see
                // `DelegateEnvelopeColumns`'s docs).
                if !columns.namespace_matches(&self.shared.identity) {
                    return Err(StreamOpenError::with_category(FailureCategory::Invariant));
                }
                let inner = columns
                    .to_envelope(self.shared.identity.clone(), self.shared.limits)
                    .ok_or_else(StreamOpenError::new)?;
                let validated = contract
                    .validate_for_open(&inner)
                    .map_err(|_| StreamOpenError::new())?;
                stream
                    .open(StreamOpenContext::new(
                        Some(&validated),
                        context.stop_token(),
                    ))
                    .await
                    .map_err(|_| StreamOpenError::new())?;
            }
            None => {
                stream
                    .open(StreamOpenContext::new(None, context.stop_token()))
                    .await
                    .map_err(|_| StreamOpenError::new())?;
            }
        }

        // Handed off to `MultiResourceReader::read` via `pending_reader`
        // (see its docs): the runtime always calls `ItemStream::open`
        // before the first `ItemReader::read` in an attempt, so the reader
        // is available by the time it is needed.
        *self
            .shared
            .pending_reader
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(reader);
        *self.shared.handle.lock().await = Some(StreamHandle {
            index: resource_index,
            stream,
            retired: false,
        });

        Ok(if restored {
            StreamOpenOutcome::Restored
        } else {
            StreamOpenOutcome::Initial
        })
    }

    async fn update(
        &self,
        context: StreamUpdateContext<'_>,
    ) -> Result<ComponentStateEnvelope, StreamUpdateError> {
        let guard = self.shared.handle.lock().await;
        let (resource_index, delegate) = match guard.as_ref() {
            Some(handle) => {
                let envelope = handle
                    .stream
                    .update(StreamUpdateContext::new(context.stop_token()))
                    .await
                    .map_err(|_| StreamUpdateError::new())?;
                // Fail closed before this candidate can be embedded in the
                // outer candidate: the delegate must report exactly the
                // namespace this opener assigned it, never a substitute the
                // core runtime's own `ItemStream::update` fail-closed
                // namespace check would have rejected at the top level.
                if envelope.namespace() != &self.shared.identity {
                    return Err(StreamUpdateError::with_category(FailureCategory::Invariant));
                }
                let columns = DelegateEnvelopeColumns::from_envelope(&envelope)
                    .map_err(|_| StreamUpdateError::new())?;
                (handle.index, Some(columns))
            }
            None => (
                u32::try_from(self.shared.resources.len()).unwrap_or(u32::MAX),
                None,
            ),
        };
        drop(guard);
        let state = MultiResourceState {
            resource_set_revision: self.shared.resources.revision(),
            resource_index,
            delegate,
            resource_batches_written: 0,
        };
        let codec = multi_resource_position_codec(RestartabilityDeclaration::Restartable);
        ComponentStateEnvelope::encode(
            self.shared.identity.clone(),
            &state,
            &codec,
            self.shared.limits,
        )
        .map_err(|_| StreamUpdateError::new())
    }

    async fn close(
        &self,
        context: StreamCloseContext<'_>,
    ) -> Result<StreamCloseOutcome, StreamCloseError> {
        let guard = self.shared.handle.lock().await;
        if let Some(handle) = guard.as_ref()
            && !handle.retired
        {
            handle
                .stream
                .close(StreamCloseContext::new(
                    context.stop_token(),
                    context.outcome(),
                ))
                .await
                .map_err(|_| StreamCloseError::new())?;
        }
        Ok(StreamCloseOutcome::Closed)
    }
}

/// Builds a `(reader, stream, contract)` triple over an ordered resource
/// set, namespaced under `identity`.
///
/// Register the stream with [`crate::ChunkStep::with_item_stream`] under
/// the same `identity`, and declare `identity` in the job's
/// [`crate::ChunkComponentRevisions`] via
/// [`crate::ChunkComponentRevisions::with_stream_revision`], exactly like
/// any other restartable reader.
///
/// `restartability` should be
/// [`RestartabilityDeclaration::NotRestartable`] when `opener` cannot
/// guarantee reopening the exact same resource content after a restart
/// (e.g. an object-store backend without stable version identity for its
/// listed objects).
pub fn multi_resource_reader<I, O>(
    resources: ResourceSet,
    opener: O,
    identity: ComponentStreamIdentity,
    restartability: RestartabilityDeclaration,
) -> (
    MultiResourceReader<I, O>,
    MultiResourceReaderStream<I, O>,
    StreamStateContract,
)
where
    I: Send + 'static,
    O: MultiResourceReaderOpener<I> + 'static,
{
    let shared = Arc::new(ReaderShared {
        opener,
        resources,
        identity,
        handle: AsyncMutex::new(None),
        pending_reader: std::sync::Mutex::new(None),
        limits: StateLimits::default(),
        _marker: PhantomData,
    });
    let reader = MultiResourceReader {
        shared: Arc::clone(&shared),
        reader: None,
        started: false,
        transitioning: false,
    };
    let stream = MultiResourceReaderStream { shared };
    let contract = StreamStateContract::new(multi_resource_position_codec(restartability));
    (reader, stream, contract)
}

// ---------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------

/// Opens one physical output resource on demand for a
/// [`MultiResourceWriter`], producing a fresh delegate writer paired with
/// its own [`crate::ItemStream`] half and state contract.
pub trait MultiResourceWriterOpener<O>: Send + Sync {
    /// The delegate writer this opener produces.
    type Writer: ItemWriter<O>;
    /// The delegate writer's own [`crate::ItemStream`] half.
    type Stream: ItemStream;

    /// Opens `resource` fresh for output.
    ///
    /// # Errors
    ///
    /// Returns a redacted [`MultiResourceOpenError`] naming `resource`'s
    /// ordinal.
    fn open<'a>(
        &'a self,
        resource: &'a ResourceIdentity,
        resource_ordinal: u32,
        delegate_identity: &'a ComponentStreamIdentity,
    ) -> impl Future<
        Output = Result<(Self::Writer, Self::Stream, StreamStateContract), MultiResourceOpenError>,
    > + Send
    + 'a;
}

/// Decides, before each [`ItemWriter::write`] batch, whether the current
/// output resource should roll over to the next one first.
///
/// A batch is never split across two resources: the rollover decision is
/// made once, before the batch is written, so one `write` call's items
/// always land entirely in one physical resource.
pub trait RolloverPolicy: Send + Sync {
    /// Returns whether the current resource should roll over before the
    /// next batch is written, given the number of batches already written
    /// to it.
    fn should_roll_over(&self, resource_batches_written: u64) -> bool;
}

/// Rolls over after a fixed number of batches have been written to the
/// current resource.
#[derive(Clone, Copy, Debug)]
pub struct BatchCountRollover {
    max_batches_per_resource: u64,
}

impl BatchCountRollover {
    /// Rolls over once `max_batches_per_resource` batches have been written
    /// to the current resource.
    #[must_use]
    pub const fn new(max_batches_per_resource: u64) -> Self {
        Self {
            max_batches_per_resource,
        }
    }
}

impl RolloverPolicy for BatchCountRollover {
    fn should_roll_over(&self, resource_batches_written: u64) -> bool {
        resource_batches_written >= self.max_batches_per_resource
    }
}

/// Never rolls over: exactly one output resource is used.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoRollover;

impl RolloverPolicy for NoRollover {
    fn should_roll_over(&self, _resource_batches_written: u64) -> bool {
        false
    }
}

struct ActiveWriteResource<W, S> {
    index: u32,
    writer: W,
    stream: S,
    batches_written: u64,
    /// `true` once `stream` has already been closed at a resource boundary
    /// (see [`crate::StreamRuntimeOutcome::ResourceBoundary`]) but rollover
    /// has not yet been able to replace this entry with the next resource
    /// (the next resource's own open failed and is pending retry). Guards
    /// against closing the same delegate twice, both on a retried rollover
    /// and at the outer terminal close.
    retired: bool,
}

struct WriterShared<O, Opener: MultiResourceWriterOpener<O>, P> {
    opener: Opener,
    resources: ResourceSet,
    identity: ComponentStreamIdentity,
    rollover: P,
    active: AsyncMutex<Option<ActiveWriteResource<Opener::Writer, Opener::Stream>>>,
    limits: StateLimits,
    _marker: PhantomData<fn() -> O>,
}

/// Writes a logical output partitioned across an ordered set of physical
/// resources.
///
/// # Contract
///
/// - **Input/output**: `O`, same as every delegate the opener produces.
/// - **State/checkpoint**: [`ResourceSetRevision`] plus the current output
///   resource's ordinal and embedded delegate position, through the paired
///   [`MultiResourceWriterStream`], following the same nested-envelope
///   scheme as [`MultiResourceReader`].
/// - **Ordering**: resources are filled in [`ResourceSet`]'s declared
///   order; one `write` batch never spans two resources.
/// - **Restartability**: supplied explicitly at construction, same as
///   [`multi_resource_reader`].
/// - **Thread safety**: `Send + Sync`; an async-aware lock guards the
///   active resource and rollover decision together, so a rollover and a
///   concurrent write attempt on the same instance are serialized (this
///   crate's chunk runtime never actually calls `write` concurrently on one
///   writer instance; the lock exists so this type's own invariant --
///   rollover-then-write is one atomic transition -- holds regardless).
/// - **Transaction/delivery**: never claims a stronger mode than the
///   current delegate writer supports; a rollover happens between writer
///   calls, never inside one enlisted call.
/// - **Failure semantics**: a failure while writing to resource N does not
///   roll over to resource N+1 -- the same delegate is retried at the same
///   resource on the framework's own retry contract, exactly like an
///   undecorated writer.
/// - **Close**: rollover closes the outgoing resource's delegate stream
///   (with [`crate::StreamRuntimeOutcome::ResourceBoundary`], never
///   `Committed` -- the enclosing step attempt has not reached a terminal
///   outcome yet) before opening the next one, never leaving two resources'
///   delegates open at once; the paired [`MultiResourceWriterStream::close`]
///   closes whichever resource is still active at the step attempt's own
///   terminal outcome, and never re-closes a resource already closed by a
///   rollover.
/// - **Support tier**: first-party.
pub struct MultiResourceWriter<O, Opener: MultiResourceWriterOpener<O>, P> {
    shared: Arc<WriterShared<O, Opener, P>>,
}

impl<O, Opener, P> ItemWriter<O> for MultiResourceWriter<O, Opener, P>
where
    O: Sync + 'static,
    Opener: MultiResourceWriterOpener<O> + 'static,
    P: RolloverPolicy + 'static,
{
    async fn write<'a>(
        &'a self,
        items: &'a [O],
        mut context: WriteContext<'a>,
    ) -> Result<WriteOutcome, WriterError> {
        let stop = context.stop_token();
        if stop.is_stop_requested() {
            return Ok(WriteOutcome::Stopped);
        }
        if self.shared.resources.is_empty() {
            return Err(WriterError::with_category(FailureCategory::Invariant));
        }

        let mut guard = self.shared.active.lock().await;
        let should_roll_over = match guard.as_ref() {
            Some(active) => self
                .shared
                .rollover
                .should_roll_over(active.batches_written),
            None => return Err(WriterError::with_category(FailureCategory::Invariant)),
        };

        if should_roll_over {
            let current_index = guard.as_ref().map_or(0, |active| active.index);
            let next_index = current_index + 1;
            if (next_index as usize) < self.shared.resources.len() {
                // Close the outgoing resource's stream before opening the
                // next one: `StreamRuntimeOutcome::ResourceBoundary` (never
                // `Committed` -- the enclosing step attempt has not reached
                // a terminal outcome yet) tells the delegate its own local
                // work is done without falsely claiming the outer
                // transaction committed. Guarded by `retired` so a retried
                // rollover (the open below failed on a previous attempt)
                // never closes the same delegate twice.
                if let Some(active) = guard.as_mut()
                    && !active.retired
                {
                    active
                        .stream
                        .close(StreamCloseContext::new(
                            stop,
                            StreamRuntimeOutcome::ResourceBoundary,
                        ))
                        .await
                        .map_err(|error| WriterError::with_category(error.category()))?;
                    active.retired = true;
                }
                let next_resource = &self.shared.resources.resources()[next_index as usize];
                // Fresh resource, no inherited state: the returned contract
                // has nothing left to validate.
                let (writer, stream, _contract) = self
                    .shared
                    .opener
                    .open(next_resource, next_index, &self.shared.identity)
                    .await
                    .map_err(|_| WriterError::new())?;
                stream
                    .open(StreamOpenContext::new(None, stop))
                    .await
                    .map_err(|_| WriterError::new())?;
                *guard = Some(ActiveWriteResource {
                    index: next_index,
                    writer,
                    stream,
                    batches_written: 0,
                    retired: false,
                });
            }
            // No further resource to roll over into: keep writing to the
            // current (last) resource rather than failing -- the rollover
            // policy is a hint for splitting output, not a hard cap.
        }

        let Some(active) = guard.as_mut() else {
            return Err(WriterError::with_category(FailureCategory::Invariant));
        };
        // `WriteContext` holds `Option<&mut dyn BusinessTransaction>`, which
        // is invariant in its lifetime: the received `context` cannot be
        // passed to the delegate directly without forcing the delegate's own
        // lifetime to unify with `'a` (this call's, spanning the whole
        // outer future), which in turn would force `active`'s borrow -- and
        // therefore the mutex guard holding it -- to outlive this function.
        // Reconstructing a fresh, guard-scoped `WriteContext` (the same
        // technique `FanOutWriter` uses) avoids that.
        let enlisted = context.is_enlisted();
        let delegate_context = if enlisted {
            let transaction: &mut dyn crate::BusinessTransaction =
                context.transaction().ok_or_else(WriterError::new)?;
            WriteContext::enlisted(stop, transaction)
        } else {
            WriteContext::non_transactional(stop)
        };
        let outcome = active.writer.write(items, delegate_context).await?;
        if matches!(outcome, WriteOutcome::Written) {
            active.batches_written += 1;
        }
        Ok(outcome)
    }
}

/// The [`crate::ItemStream`] half of a [`MultiResourceWriter`].
pub struct MultiResourceWriterStream<O, Opener: MultiResourceWriterOpener<O>, P> {
    shared: Arc<WriterShared<O, Opener, P>>,
}

impl<O, Opener, P> ItemStream for MultiResourceWriterStream<O, Opener, P>
where
    O: Sync + 'static,
    Opener: MultiResourceWriterOpener<O> + 'static,
    P: RolloverPolicy + 'static,
{
    async fn open(
        &self,
        context: StreamOpenContext<'_>,
    ) -> Result<StreamOpenOutcome, StreamOpenError> {
        let codec = multi_resource_position_codec(RestartabilityDeclaration::Restartable);
        let restored = context.inherited_state().is_some();
        let (resource_index, inherited_delegate, resource_batches_written) =
            match context.inherited_state() {
                Some(envelope) => {
                    let state = envelope
                        .decode::<MultiResourceState>(&codec)
                        .map_err(|_| StreamOpenError::new())?;
                    if state.resource_set_revision != self.shared.resources.revision() {
                        return Err(StreamOpenError::with_category(
                            FailureCategory::UnsupportedCapability,
                        ));
                    }
                    (
                        state.resource_index,
                        state.delegate,
                        state.resource_batches_written,
                    )
                }
                None => (0, None, 0),
            };

        let resources_len = u32::try_from(self.shared.resources.len()).unwrap_or(u32::MAX);
        if resource_index >= resources_len {
            return Err(StreamOpenError::with_category(FailureCategory::Invariant));
        }

        let resource = &self.shared.resources.resources()[resource_index as usize];
        let (writer, stream, contract) = self
            .shared
            .opener
            .open(resource, resource_index, &self.shared.identity)
            .await
            .map_err(|_| StreamOpenError::new())?;

        match inherited_delegate {
            Some(columns) => {
                // Fail closed if the delegate that produced this durable
                // record did not actually report the namespace this opener
                // assigned it -- see `DelegateEnvelopeColumns`'s docs.
                if !columns.namespace_matches(&self.shared.identity) {
                    return Err(StreamOpenError::with_category(FailureCategory::Invariant));
                }
                let inner = columns
                    .to_envelope(self.shared.identity.clone(), self.shared.limits)
                    .ok_or_else(StreamOpenError::new)?;
                let validated = contract
                    .validate_for_open(&inner)
                    .map_err(|_| StreamOpenError::new())?;
                stream
                    .open(StreamOpenContext::new(
                        Some(&validated),
                        context.stop_token(),
                    ))
                    .await
                    .map_err(|_| StreamOpenError::new())?;
            }
            None => {
                stream
                    .open(StreamOpenContext::new(None, context.stop_token()))
                    .await
                    .map_err(|_| StreamOpenError::new())?;
            }
        }

        *self.shared.active.lock().await = Some(ActiveWriteResource {
            index: resource_index,
            writer,
            stream,
            batches_written: resource_batches_written,
            retired: false,
        });

        Ok(if restored {
            StreamOpenOutcome::Restored
        } else {
            StreamOpenOutcome::Initial
        })
    }

    async fn update(
        &self,
        context: StreamUpdateContext<'_>,
    ) -> Result<ComponentStateEnvelope, StreamUpdateError> {
        let guard = self.shared.active.lock().await;
        let Some(active) = guard.as_ref() else {
            return Err(StreamUpdateError::with_category(FailureCategory::Invariant));
        };
        let envelope = active
            .stream
            .update(StreamUpdateContext::new(context.stop_token()))
            .await
            .map_err(|_| StreamUpdateError::new())?;
        // Fail closed before this candidate can be embedded in the outer
        // candidate -- see the reader stream's `update` for the same check.
        if envelope.namespace() != &self.shared.identity {
            return Err(StreamUpdateError::with_category(FailureCategory::Invariant));
        }
        let columns = DelegateEnvelopeColumns::from_envelope(&envelope)
            .map_err(|_| StreamUpdateError::new())?;
        let state = MultiResourceState {
            resource_set_revision: self.shared.resources.revision(),
            resource_index: active.index,
            delegate: Some(columns),
            resource_batches_written: active.batches_written,
        };
        drop(guard);
        let codec = multi_resource_position_codec(RestartabilityDeclaration::Restartable);
        ComponentStateEnvelope::encode(
            self.shared.identity.clone(),
            &state,
            &codec,
            self.shared.limits,
        )
        .map_err(|_| StreamUpdateError::new())
    }

    async fn close(
        &self,
        context: StreamCloseContext<'_>,
    ) -> Result<StreamCloseOutcome, StreamCloseError> {
        let guard = self.shared.active.lock().await;
        if let Some(active) = guard.as_ref()
            && !active.retired
        {
            active
                .stream
                .close(StreamCloseContext::new(
                    context.stop_token(),
                    context.outcome(),
                ))
                .await
                .map_err(|_| StreamCloseError::new())?;
        }
        Ok(StreamCloseOutcome::Closed)
    }
}

/// The `(writer, stream, contract)` triple [`multi_resource_writer`]
/// returns.
pub type MultiResourceWriterTriple<O, Opener, P> = (
    MultiResourceWriter<O, Opener, P>,
    MultiResourceWriterStream<O, Opener, P>,
    StreamStateContract,
);

/// Builds a `(writer, stream, contract)` triple over an ordered resource
/// set, namespaced under `identity`.
///
/// `resources` must be nonempty: a multi-resource writer always has a
/// current output resource to write to (unlike the reader, which can
/// legitimately describe zero input resources).
///
/// # Errors
///
/// Returns [`WriterConfigError::EmptyResourceSet`] when `resources` is
/// empty.
pub fn multi_resource_writer<O, Opener, P>(
    resources: ResourceSet,
    opener: Opener,
    identity: ComponentStreamIdentity,
    rollover: P,
    restartability: RestartabilityDeclaration,
) -> Result<MultiResourceWriterTriple<O, Opener, P>, WriterConfigError>
where
    O: Sync + 'static,
    Opener: MultiResourceWriterOpener<O> + 'static,
    P: RolloverPolicy + 'static,
{
    if resources.is_empty() {
        return Err(WriterConfigError::EmptyResourceSet);
    }
    let shared = Arc::new(WriterShared {
        opener,
        resources,
        identity,
        rollover,
        active: AsyncMutex::new(None),
        limits: StateLimits::default(),
        _marker: PhantomData,
    });
    let writer = MultiResourceWriter {
        shared: Arc::clone(&shared),
    };
    let stream = MultiResourceWriterStream { shared };
    let contract = StreamStateContract::new(multi_resource_position_codec(restartability));
    Ok((writer, stream, contract))
}

/// A validation failure building a [`MultiResourceWriter`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum WriterConfigError {
    /// A multi-resource writer requires at least one output resource.
    EmptyResourceSet,
}

impl fmt::Display for WriterConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyResourceSet => {
                formatter.write_str("multi-resource writer requires a nonempty resource set")
            }
        }
    }
}

impl std::error::Error for WriterConfigError {}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use std::collections::{HashMap, HashSet};

    use super::*;
    use crate::StopSource;

    fn identity(name: &str) -> ComponentStreamIdentity {
        ComponentStreamIdentity::new(format!("oxide-batch.multi-resource-unit-test.{name}"))
            .expect("static identity is valid")
    }

    fn resource_set(names: &[&str]) -> ResourceSet {
        ResourceSet::new(
            names
                .iter()
                .map(|name| ResourceIdentity::new((*name).to_owned()).unwrap())
                .collect(),
        )
    }

    #[test]
    fn resource_identity_rejects_one_byte_past_its_ceiling() {
        assert!(ResourceIdentity::new("x".repeat(MAX_RESOURCE_IDENTITY_BYTES)).is_ok());
        let error = ResourceIdentity::new("x".repeat(MAX_RESOURCE_IDENTITY_BYTES + 1))
            .expect_err("one byte past the ceiling must be refused, not silently truncated");
        assert_eq!(
            error,
            MultiResourceConfigError::ResourceIdentityTooLong {
                max_bytes: MAX_RESOURCE_IDENTITY_BYTES
            }
        );
    }

    #[test]
    fn resource_identity_rejects_empty_and_control_characters() {
        assert_eq!(
            ResourceIdentity::new(""),
            Err(MultiResourceConfigError::EmptyResourceIdentity)
        );
        assert_eq!(
            ResourceIdentity::new("a\nb"),
            Err(MultiResourceConfigError::MalformedResourceIdentity)
        );
    }

    // -- a minimal in-memory `ItemReader`/`ItemStream` delegate pair, the
    // same shape `delimited_reader` returns, used only to exercise
    // `MultiResourceReader`/`MultiResourceReaderStream` without real I/O.

    #[derive(Clone, Copy, Eq, PartialEq)]
    struct VecPosition(u64);

    const VEC_READ_SCHEMA: &str = "oxide-batch.multi-resource-unit-test.vec-read-position";
    const VEC_READ_CODEC: &str = "oxide-batch.multi-resource-unit-test.vec-read-position-codec";

    #[derive(Clone, Copy)]
    struct VecReadSchema;

    impl VersionedStateCodec<VecPosition> for VecReadSchema {
        fn schema_id(&self) -> &StateSchemaId {
            static SCHEMA: std::sync::OnceLock<StateSchemaId> = std::sync::OnceLock::new();
            SCHEMA.get_or_init(|| StateSchemaId::new(VEC_READ_SCHEMA).unwrap())
        }

        fn current_version(&self) -> StateSchemaVersion {
            StateSchemaVersion::new(1).unwrap()
        }

        fn encode(&self, value: &VecPosition) -> Result<Vec<u8>, StateCodecError> {
            serde_json::to_vec(&serde_json::json!({ "ordinal": value.0 }))
                .map_err(|_| StateCodecError::InvalidPayload)
        }

        fn decode(&self, payload: &[u8]) -> Result<VecPosition, StateCodecError> {
            let value: serde_json::Value =
                serde_json::from_slice(payload).map_err(|_| StateCodecError::InvalidPayload)?;
            let ordinal = value
                .get("ordinal")
                .and_then(serde_json::Value::as_u64)
                .ok_or(StateCodecError::InvalidPayload)?;
            Ok(VecPosition(ordinal))
        }
    }

    fn vec_read_codec() -> DefaultComponentCodec<VecReadSchema> {
        DefaultComponentCodec::new(
            VecReadSchema,
            crate::CodecId::new(VEC_READ_CODEC).unwrap(),
            crate::CodecVersion::new(1).unwrap(),
            RestartabilityDeclaration::Restartable,
        )
        .with_sensitivity(StateSensitivity::NonSensitive)
    }

    struct VecItemReader {
        items: Vec<u64>,
        ordinal: Arc<AsyncMutex<u64>>,
    }

    impl ItemReader<u64> for VecItemReader {
        async fn read(
            &mut self,
            _context: ReadContext<'_>,
        ) -> Result<ReadOutcome<u64>, ReaderError> {
            let mut ordinal = self.ordinal.lock().await;
            let index = usize::try_from(*ordinal).unwrap_or(usize::MAX);
            match self.items.get(index) {
                Some(item) => {
                    let item = *item;
                    *ordinal += 1;
                    Ok(ReadOutcome::Item(item))
                }
                None => Ok(ReadOutcome::EndOfInput),
            }
        }
    }

    struct VecItemReaderStream {
        ordinal: Arc<AsyncMutex<u64>>,
        namespace: ComponentStreamIdentity,
    }

    impl ItemStream for VecItemReaderStream {
        async fn open(
            &self,
            context: StreamOpenContext<'_>,
        ) -> Result<StreamOpenOutcome, StreamOpenError> {
            let codec = vec_read_codec();
            if let Some(envelope) = context.inherited_state() {
                let restored = envelope
                    .decode::<VecPosition>(&codec)
                    .map_err(|_| StreamOpenError::new())?;
                *self.ordinal.lock().await = restored.0;
                Ok(StreamOpenOutcome::Restored)
            } else {
                *self.ordinal.lock().await = 0;
                Ok(StreamOpenOutcome::Initial)
            }
        }

        async fn update(
            &self,
            _context: StreamUpdateContext<'_>,
        ) -> Result<ComponentStateEnvelope, StreamUpdateError> {
            let codec = vec_read_codec();
            let ordinal = *self.ordinal.lock().await;
            ComponentStateEnvelope::encode(
                self.namespace.clone(),
                &VecPosition(ordinal),
                &codec,
                StateLimits::default(),
            )
            .map_err(|_| StreamUpdateError::new())
        }

        async fn close(
            &self,
            _context: StreamCloseContext<'_>,
        ) -> Result<StreamCloseOutcome, StreamCloseError> {
            Ok(StreamCloseOutcome::Closed)
        }
    }

    struct TestReaderOpener {
        data: HashMap<String, Vec<u64>>,
        fail: HashSet<String>,
    }

    impl MultiResourceReaderOpener<u64> for TestReaderOpener {
        type Reader = VecItemReader;
        type Stream = VecItemReaderStream;

        async fn open(
            &self,
            resource: &ResourceIdentity,
            resource_ordinal: u32,
            delegate_identity: &ComponentStreamIdentity,
        ) -> Result<(Self::Reader, Self::Stream, StreamStateContract), MultiResourceOpenError>
        {
            if self.fail.contains(resource.as_str()) {
                return Err(MultiResourceOpenError::new(resource_ordinal));
            }
            let items = self
                .data
                .get(resource.as_str())
                .cloned()
                .unwrap_or_default();
            let ordinal = Arc::new(AsyncMutex::new(0));
            let reader = VecItemReader {
                items,
                ordinal: Arc::clone(&ordinal),
            };
            let stream = VecItemReaderStream {
                ordinal,
                namespace: delegate_identity.clone(),
            };
            Ok((reader, stream, StreamStateContract::new(vec_read_codec())))
        }
    }

    fn stop() -> (StopSource, crate::StopToken) {
        StopSource::new()
    }

    fn read_context(stop: &crate::StopToken) -> ReadContext<'_> {
        ReadContext::new(stop)
    }

    #[test]
    fn ordered_traversal_across_resources_reads_in_declared_order() {
        let mut data = HashMap::new();
        data.insert("a".to_owned(), vec![1, 2]);
        data.insert("b".to_owned(), vec![3]);
        data.insert("c".to_owned(), vec![4, 5]);
        let opener = TestReaderOpener {
            data,
            fail: HashSet::new(),
        };
        let (mut reader, stream, _contract) = multi_resource_reader::<u64, _>(
            resource_set(&["a", "b", "c"]),
            opener,
            identity("ordered"),
            RestartabilityDeclaration::Restartable,
        );
        let (_source, token) = stop();
        futures_executor::block_on(async {
            stream
                .open(StreamOpenContext::new(None, &token))
                .await
                .unwrap();
            let mut collected = Vec::new();
            loop {
                match reader.read(read_context(&token)).await.unwrap() {
                    ReadOutcome::Item(item) => collected.push(item),
                    ReadOutcome::EndOfInput => break,
                    ReadOutcome::Stopped => panic!("unexpected stop"),
                }
            }
            assert_eq!(collected, vec![1, 2, 3, 4, 5]);
        });
    }

    #[test]
    fn empty_resource_set_reader_returns_end_of_input_immediately() {
        let opener = TestReaderOpener {
            data: HashMap::new(),
            fail: HashSet::new(),
        };
        let (mut reader, stream, _contract) = multi_resource_reader::<u64, _>(
            resource_set(&[]),
            opener,
            identity("empty"),
            RestartabilityDeclaration::Restartable,
        );
        let (_source, token) = stop();
        futures_executor::block_on(async {
            stream
                .open(StreamOpenContext::new(None, &token))
                .await
                .unwrap();
            assert_eq!(
                reader.read(read_context(&token)).await.unwrap(),
                ReadOutcome::EndOfInput
            );
        });
    }

    #[test]
    fn restart_mid_resource_resumes_at_committed_position_not_resource_start() {
        let mut data = HashMap::new();
        data.insert("a".to_owned(), vec![1, 2]);
        data.insert("b".to_owned(), vec![10, 20, 30]);
        let opener = || TestReaderOpener {
            data: data.clone(),
            fail: HashSet::new(),
        };
        let resources = resource_set(&["a", "b"]);
        let (_source, token) = stop();

        // Attempt 1: exhaust resource "a" and read one item from "b", then
        // commit (call `update`) -- simulating a chunk boundary -- before
        // "crashing" (the reader/stream pair is simply dropped).
        let committed_envelope = futures_executor::block_on(async {
            let (mut reader, stream, _contract) = multi_resource_reader::<u64, _>(
                resources.clone(),
                opener(),
                identity("restart"),
                RestartabilityDeclaration::Restartable,
            );
            stream
                .open(StreamOpenContext::new(None, &token))
                .await
                .unwrap();
            assert_eq!(
                reader.read(read_context(&token)).await.unwrap(),
                ReadOutcome::Item(1)
            );
            assert_eq!(
                reader.read(read_context(&token)).await.unwrap(),
                ReadOutcome::Item(2)
            );
            // Resource "a" exhausts and transitions to "b" on this call.
            assert_eq!(
                reader.read(read_context(&token)).await.unwrap(),
                ReadOutcome::Item(10)
            );
            stream
                .update(StreamUpdateContext::new(&token))
                .await
                .unwrap()
        });

        // Attempt 2: fresh reader/stream pair over the same resource set,
        // restored from the committed envelope.
        futures_executor::block_on(async {
            let (mut reader, stream, _contract) = multi_resource_reader::<u64, _>(
                resources,
                opener(),
                identity("restart"),
                RestartabilityDeclaration::Restartable,
            );
            stream
                .open(StreamOpenContext::new(Some(&committed_envelope), &token))
                .await
                .unwrap();
            // Must resume with "b"'s next item (20), never replaying "a" or
            // restarting "b" from its own beginning (10).
            assert_eq!(
                reader.read(read_context(&token)).await.unwrap(),
                ReadOutcome::Item(20)
            );
            assert_eq!(
                reader.read(read_context(&token)).await.unwrap(),
                ReadOutcome::Item(30)
            );
            assert_eq!(
                reader.read(read_context(&token)).await.unwrap(),
                ReadOutcome::EndOfInput
            );
        });
    }

    #[test]
    fn resource_set_revision_mismatch_on_restart_is_rejected() {
        let mut data = HashMap::new();
        data.insert("a".to_owned(), vec![1]);
        data.insert("b".to_owned(), vec![2]);
        let (_source, token) = stop();

        let committed_envelope = futures_executor::block_on(async {
            let opener = TestReaderOpener {
                data: data.clone(),
                fail: HashSet::new(),
            };
            let (mut reader, stream, _contract) = multi_resource_reader::<u64, _>(
                resource_set(&["a", "b"]),
                opener,
                identity("revision"),
                RestartabilityDeclaration::Restartable,
            );
            stream
                .open(StreamOpenContext::new(None, &token))
                .await
                .unwrap();
            reader.read(read_context(&token)).await.unwrap();
            stream
                .update(StreamUpdateContext::new(&token))
                .await
                .unwrap()
        });

        // Restart against a resource set with an inserted resource ahead of
        // the committed index -- the same physical index no longer names
        // the same resource, and this must fail closed rather than
        // silently reinterpreting it.
        futures_executor::block_on(async {
            let opener = TestReaderOpener {
                data,
                fail: HashSet::new(),
            };
            let (_reader, stream, _contract) = multi_resource_reader::<u64, _>(
                resource_set(&["z", "a", "b"]),
                opener,
                identity("revision"),
                RestartabilityDeclaration::Restartable,
            );
            let result = stream
                .open(StreamOpenContext::new(Some(&committed_envelope), &token))
                .await;
            assert!(
                result.is_err(),
                "changed resource set must be rejected, not silently resumed"
            );
        });
    }

    #[test]
    fn resource_open_failure_does_not_advance_past_the_failure_point() {
        let mut data = HashMap::new();
        data.insert("a".to_owned(), vec![1]);
        data.insert("c".to_owned(), vec![3]);
        let mut fail = HashSet::new();
        fail.insert("b".to_owned());
        let opener = TestReaderOpener { data, fail };
        let (mut reader, stream, _contract) = multi_resource_reader::<u64, _>(
            resource_set(&["a", "b", "c"]),
            opener,
            identity("open-failure"),
            RestartabilityDeclaration::Restartable,
        );
        let (_source, token) = stop();
        futures_executor::block_on(async {
            stream
                .open(StreamOpenContext::new(None, &token))
                .await
                .unwrap();
            assert_eq!(
                reader.read(read_context(&token)).await.unwrap(),
                ReadOutcome::Item(1)
            );
            // "a" exhausts, transition to "b" fails to open.
            let result = reader.read(read_context(&token)).await;
            assert!(
                result.is_err(),
                "resource b's open failure must surface, not be swallowed"
            );
            // A retried call must not have silently advanced to "c".
            let retried = reader.read(read_context(&token)).await;
            assert!(
                retried.is_err(),
                "a retry must fail at the same resource again, never skip to c"
            );
        });
    }

    // -- Finding 1/3 (#177) regression evidence: recording delegates that
    // log every `open`/`update`/`close` call (and the `StreamRuntimeOutcome`
    // `close` receives) so tests can assert the exact nested lifecycle,
    // not only its externally observable read/write effects.

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum LifecycleEvent {
        Open(String),
        Update(String),
        Close(String, RecordedOutcome),
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum RecordedOutcome {
        Committed,
        Failed,
        Stopped,
        Unknown,
        ResourceBoundary,
    }

    impl From<StreamRuntimeOutcome> for RecordedOutcome {
        fn from(outcome: StreamRuntimeOutcome) -> Self {
            match outcome {
                StreamRuntimeOutcome::Committed => Self::Committed,
                StreamRuntimeOutcome::Failed => Self::Failed,
                StreamRuntimeOutcome::Stopped => Self::Stopped,
                StreamRuntimeOutcome::Unknown => Self::Unknown,
                StreamRuntimeOutcome::ResourceBoundary => Self::ResourceBoundary,
            }
        }
    }

    type Log = Arc<std::sync::Mutex<Vec<LifecycleEvent>>>;

    fn log_event(log: &Log, event: LifecycleEvent) {
        log.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(event);
    }

    struct RecordingReaderStream {
        ordinal: Arc<AsyncMutex<u64>>,
        namespace: ComponentStreamIdentity,
        resource: String,
        log: Log,
        /// Closing this resource fails exactly once (then the flag clears
        /// itself), so a test can prove both propagation *and* that a
        /// retried close recovers cleanly.
        fail_close_once: Arc<std::sync::atomic::AtomicBool>,
    }

    impl ItemStream for RecordingReaderStream {
        async fn open(
            &self,
            context: StreamOpenContext<'_>,
        ) -> Result<StreamOpenOutcome, StreamOpenError> {
            log_event(&self.log, LifecycleEvent::Open(self.resource.clone()));
            let codec = vec_read_codec();
            if let Some(envelope) = context.inherited_state() {
                let restored = envelope
                    .decode::<VecPosition>(&codec)
                    .map_err(|_| StreamOpenError::new())?;
                *self.ordinal.lock().await = restored.0;
                Ok(StreamOpenOutcome::Restored)
            } else {
                *self.ordinal.lock().await = 0;
                Ok(StreamOpenOutcome::Initial)
            }
        }

        async fn update(
            &self,
            _context: StreamUpdateContext<'_>,
        ) -> Result<ComponentStateEnvelope, StreamUpdateError> {
            log_event(&self.log, LifecycleEvent::Update(self.resource.clone()));
            let codec = vec_read_codec();
            let ordinal = *self.ordinal.lock().await;
            ComponentStateEnvelope::encode(
                self.namespace.clone(),
                &VecPosition(ordinal),
                &codec,
                StateLimits::default(),
            )
            .map_err(|_| StreamUpdateError::new())
        }

        async fn close(
            &self,
            context: StreamCloseContext<'_>,
        ) -> Result<StreamCloseOutcome, StreamCloseError> {
            log_event(
                &self.log,
                LifecycleEvent::Close(self.resource.clone(), context.outcome().into()),
            );
            if self
                .fail_close_once
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(StreamCloseError::new());
            }
            Ok(StreamCloseOutcome::Closed)
        }
    }

    struct RecordingReaderOpener {
        data: HashMap<String, Vec<u64>>,
        log: Log,
        fail_open: HashSet<String>,
        fail_close_once: HashMap<String, Arc<std::sync::atomic::AtomicBool>>,
    }

    impl MultiResourceReaderOpener<u64> for RecordingReaderOpener {
        type Reader = VecItemReader;
        type Stream = RecordingReaderStream;

        async fn open(
            &self,
            resource: &ResourceIdentity,
            resource_ordinal: u32,
            delegate_identity: &ComponentStreamIdentity,
        ) -> Result<(Self::Reader, Self::Stream, StreamStateContract), MultiResourceOpenError>
        {
            if self.fail_open.contains(resource.as_str()) {
                return Err(MultiResourceOpenError::new(resource_ordinal));
            }
            let items = self
                .data
                .get(resource.as_str())
                .cloned()
                .unwrap_or_default();
            let ordinal = Arc::new(AsyncMutex::new(0));
            let reader = VecItemReader {
                items,
                ordinal: Arc::clone(&ordinal),
            };
            let fail_close_once = self
                .fail_close_once
                .get(resource.as_str())
                .cloned()
                .unwrap_or_else(|| Arc::new(std::sync::atomic::AtomicBool::new(false)));
            let stream = RecordingReaderStream {
                ordinal,
                namespace: delegate_identity.clone(),
                resource: resource.as_str().to_owned(),
                log: Arc::clone(&self.log),
                fail_close_once,
            };
            Ok((reader, stream, StreamStateContract::new(vec_read_codec())))
        }
    }

    #[test]
    fn reader_closes_each_delegate_exactly_once_with_resource_boundary_outcome_in_order() {
        let mut data = HashMap::new();
        data.insert("a".to_owned(), vec![1, 2]);
        data.insert("b".to_owned(), vec![3]);
        let log: Log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let opener = RecordingReaderOpener {
            data,
            log: Arc::clone(&log),
            fail_open: HashSet::new(),
            fail_close_once: HashMap::new(),
        };
        let (mut reader, stream, _contract) = multi_resource_reader::<u64, _>(
            resource_set(&["a", "b"]),
            opener,
            identity("lifecycle-order"),
            RestartabilityDeclaration::Restartable,
        );
        let (_source, token) = stop();
        futures_executor::block_on(async {
            stream
                .open(StreamOpenContext::new(None, &token))
                .await
                .unwrap();
            loop {
                match reader.read(read_context(&token)).await.unwrap() {
                    ReadOutcome::Item(_) => {}
                    ReadOutcome::EndOfInput => break,
                    ReadOutcome::Stopped => panic!("unexpected stop"),
                }
            }
        });
        let events = log.lock().unwrap().clone();
        assert_eq!(
            events,
            vec![
                LifecycleEvent::Open("a".to_owned()),
                LifecycleEvent::Close("a".to_owned(), RecordedOutcome::ResourceBoundary),
                LifecycleEvent::Open("b".to_owned()),
                LifecycleEvent::Close("b".to_owned(), RecordedOutcome::ResourceBoundary),
            ],
            "the intermediate delegate (\"a\") and the final delegate (\"b\") must \
             each open exactly once and close exactly once, in order, with \
             `ResourceBoundary` -- never `Committed`, which would falsely \
             claim the enclosing step attempt itself had committed"
        );
    }

    #[test]
    fn reader_resource_boundary_close_failure_is_propagated_without_advancing_or_double_closing() {
        let mut data = HashMap::new();
        data.insert("a".to_owned(), vec![1]);
        data.insert("b".to_owned(), vec![2]);
        let log: Log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut fail_close_once = HashMap::new();
        fail_close_once.insert(
            "a".to_owned(),
            Arc::new(std::sync::atomic::AtomicBool::new(true)),
        );
        let opener = RecordingReaderOpener {
            data,
            log: Arc::clone(&log),
            fail_open: HashSet::new(),
            fail_close_once,
        };
        let (mut reader, stream, _contract) = multi_resource_reader::<u64, _>(
            resource_set(&["a", "b"]),
            opener,
            identity("boundary-close-failure"),
            RestartabilityDeclaration::Restartable,
        );
        let (_source, token) = stop();
        futures_executor::block_on(async {
            stream
                .open(StreamOpenContext::new(None, &token))
                .await
                .unwrap();
            assert_eq!(
                reader.read(read_context(&token)).await.unwrap(),
                ReadOutcome::Item(1)
            );
            // "a" exhausts; its boundary close fails.
            let result = reader.read(read_context(&token)).await;
            assert!(result.is_err(), "a boundary close failure must surface");
            // The checkpoint must not have silently advanced past "a": the
            // outer envelope must still describe resource index 0 with "a"'s
            // own (unclosed) position, not "b".
            let envelope = stream
                .update(StreamUpdateContext::new(&token))
                .await
                .unwrap();
            let codec = multi_resource_position_codec(RestartabilityDeclaration::Restartable);
            let state = envelope.decode::<MultiResourceState>(&codec).unwrap();
            assert_eq!(
                state.resource_index, 0,
                "checkpoint advancement must not silently occur after a failed boundary close"
            );
            // A retry re-attempts the same close (which now succeeds) and
            // completes the transition to "b" -- never re-opening "a" and
            // never treating the first, failed close as if it were a second,
            // separate closed delegate.
            assert_eq!(
                reader.read(read_context(&token)).await.unwrap(),
                ReadOutcome::Item(2)
            );
        });
        let events = log.lock().unwrap().clone();
        assert_eq!(
            events,
            vec![
                LifecycleEvent::Open("a".to_owned()),
                LifecycleEvent::Close("a".to_owned(), RecordedOutcome::ResourceBoundary),
                LifecycleEvent::Update("a".to_owned()),
                LifecycleEvent::Close("a".to_owned(), RecordedOutcome::ResourceBoundary),
                LifecycleEvent::Open("b".to_owned()),
            ],
            "exactly one failed close attempt on \"a\" followed by exactly one \
             successful retry, never a second delegate opened for \"a\", never \
             \"b\" opened before \"a\" is fully retired"
        );
    }

    #[test]
    fn outer_terminal_close_does_not_double_close_a_reader_delegate_already_retired_at_a_boundary()
    {
        let mut data = HashMap::new();
        data.insert("a".to_owned(), vec![1]);
        let mut fail_open = HashSet::new();
        fail_open.insert("b".to_owned());
        let log: Log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let opener = RecordingReaderOpener {
            data,
            log: Arc::clone(&log),
            fail_open,
            fail_close_once: HashMap::new(),
        };
        let (mut reader, stream, _contract) = multi_resource_reader::<u64, _>(
            resource_set(&["a", "b"]),
            opener,
            identity("no-double-close"),
            RestartabilityDeclaration::Restartable,
        );
        let (_source, token) = stop();
        futures_executor::block_on(async {
            stream
                .open(StreamOpenContext::new(None, &token))
                .await
                .unwrap();
            assert_eq!(
                reader.read(read_context(&token)).await.unwrap(),
                ReadOutcome::Item(1)
            );
            // "a" exhausts and closes successfully; opening "b" then fails,
            // so the step attempt gives up and the outer terminal close
            // runs with the step's real (failed) outcome.
            let result = reader.read(read_context(&token)).await;
            assert!(result.is_err());
            stream
                .close(StreamCloseContext::new(
                    &token,
                    StreamRuntimeOutcome::Failed,
                ))
                .await
                .unwrap();
        });
        let events = log.lock().unwrap().clone();
        assert_eq!(
            events,
            vec![
                LifecycleEvent::Open("a".to_owned()),
                LifecycleEvent::Close("a".to_owned(), RecordedOutcome::ResourceBoundary),
            ],
            "\"a\" must be closed exactly once total -- the outer terminal \
             close must recognize it is already retired and skip it, not \
             close it a second time with the step's real outcome"
        );
    }

    #[test]
    fn reader_update_fails_closed_when_delegate_reports_the_wrong_namespace() {
        struct WrongNamespaceStream {
            wrong_namespace: ComponentStreamIdentity,
        }
        impl ItemStream for WrongNamespaceStream {
            async fn open(
                &self,
                context: StreamOpenContext<'_>,
            ) -> Result<StreamOpenOutcome, StreamOpenError> {
                Ok(if context.inherited_state().is_some() {
                    StreamOpenOutcome::Restored
                } else {
                    StreamOpenOutcome::Initial
                })
            }
            async fn update(
                &self,
                _context: StreamUpdateContext<'_>,
            ) -> Result<ComponentStateEnvelope, StreamUpdateError> {
                // A buggy/malicious delegate that ignores the identity the
                // multi-resource opener assigned it and reports a different
                // namespace of its own choosing.
                ComponentStateEnvelope::encode(
                    self.wrong_namespace.clone(),
                    &VecPosition(0),
                    &vec_read_codec(),
                    StateLimits::default(),
                )
                .map_err(|_| StreamUpdateError::new())
            }
            async fn close(
                &self,
                _context: StreamCloseContext<'_>,
            ) -> Result<StreamCloseOutcome, StreamCloseError> {
                Ok(StreamCloseOutcome::Closed)
            }
        }

        struct WrongNamespaceOpener {
            wrong_namespace: ComponentStreamIdentity,
        }
        impl MultiResourceReaderOpener<u64> for WrongNamespaceOpener {
            type Reader = VecItemReader;
            type Stream = WrongNamespaceStream;
            async fn open(
                &self,
                _resource: &ResourceIdentity,
                _resource_ordinal: u32,
                _delegate_identity: &ComponentStreamIdentity,
            ) -> Result<(Self::Reader, Self::Stream, StreamStateContract), MultiResourceOpenError>
            {
                Ok((
                    VecItemReader {
                        items: vec![1],
                        ordinal: Arc::new(AsyncMutex::new(0)),
                    },
                    WrongNamespaceStream {
                        wrong_namespace: self.wrong_namespace.clone(),
                    },
                    StreamStateContract::new(vec_read_codec()),
                ))
            }
        }

        let opener = WrongNamespaceOpener {
            wrong_namespace: identity("attacker-namespace"),
        };
        let (mut reader, stream, _contract) = multi_resource_reader::<u64, _>(
            resource_set(&["a"]),
            opener,
            identity("expected-namespace"),
            RestartabilityDeclaration::Restartable,
        );
        let (_source, token) = stop();
        futures_executor::block_on(async {
            stream
                .open(StreamOpenContext::new(None, &token))
                .await
                .unwrap();
            reader.read(read_context(&token)).await.unwrap();
            let result = stream.update(StreamUpdateContext::new(&token)).await;
            assert!(
                result.is_err(),
                "a nested delegate reporting the wrong namespace must fail \
                 the outer update closed, not be embedded into an outer \
                 candidate that could reach durable checkpoint state"
            );
        });
    }

    #[test]
    fn reader_open_fails_closed_on_a_hand_crafted_mismatched_namespace_record() {
        // Bypasses the update-time check entirely (which already prevents a
        // wrong-namespace candidate from ever being committed) to prove
        // restore *independently* rejects a durable record whose embedded
        // delegate namespace does not match the expected identity -- e.g. if
        // such a record reached storage by some other means.
        let expected_identity = identity("restore-namespace-check");
        let wrong_identity = identity("restore-namespace-check-wrong");
        let delegate_envelope = ComponentStateEnvelope::encode(
            wrong_identity,
            &VecPosition(0),
            &vec_read_codec(),
            StateLimits::default(),
        )
        .unwrap();
        let columns = DelegateEnvelopeColumns::from_envelope(&delegate_envelope).unwrap();
        let resources = resource_set(&["a"]);
        let state = MultiResourceState {
            resource_set_revision: resources.revision(),
            resource_index: 0,
            delegate: Some(columns),
            resource_batches_written: 0,
        };
        let codec = multi_resource_position_codec(RestartabilityDeclaration::Restartable);
        let outer_envelope = ComponentStateEnvelope::encode(
            expected_identity.clone(),
            &state,
            &codec,
            StateLimits::default(),
        )
        .unwrap();

        let mut data = HashMap::new();
        data.insert("a".to_owned(), vec![1]);
        let opener = TestReaderOpener {
            data,
            fail: HashSet::new(),
        };
        let (_reader, stream, _contract) = multi_resource_reader::<u64, _>(
            resources,
            opener,
            expected_identity,
            RestartabilityDeclaration::Restartable,
        );
        let (_source, token) = stop();
        let result = futures_executor::block_on(
            stream.open(StreamOpenContext::new(Some(&outer_envelope), &token)),
        );
        assert!(
            result.is_err(),
            "a stored delegate envelope under the wrong namespace must fail \
             closed on restore, never be silently reconstructed under the \
             expected identity"
        );
    }

    // -- writer side: a minimal in-memory delegate mirroring the reader
    // fixture above.

    struct VecItemWriter {
        resource: String,
        sink: Arc<std::sync::Mutex<HashMap<String, Vec<u64>>>>,
        committed_len: Arc<AsyncMutex<u64>>,
    }

    impl ItemWriter<u64> for VecItemWriter {
        async fn write<'a>(
            &'a self,
            items: &'a [u64],
            context: WriteContext<'a>,
        ) -> Result<WriteOutcome, WriterError> {
            if context.stop_token().is_stop_requested() {
                return Ok(WriteOutcome::Stopped);
            }
            let mut committed = self.committed_len.lock().await;
            self.sink
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entry(self.resource.clone())
                .or_default()
                .extend_from_slice(items);
            *committed += u64::try_from(items.len()).unwrap();
            Ok(WriteOutcome::Written)
        }
    }

    struct VecItemWriterStream {
        committed_len: Arc<AsyncMutex<u64>>,
        namespace: ComponentStreamIdentity,
    }

    impl ItemStream for VecItemWriterStream {
        async fn open(
            &self,
            context: StreamOpenContext<'_>,
        ) -> Result<StreamOpenOutcome, StreamOpenError> {
            let codec = vec_read_codec();
            if let Some(envelope) = context.inherited_state() {
                let restored = envelope
                    .decode::<VecPosition>(&codec)
                    .map_err(|_| StreamOpenError::new())?;
                *self.committed_len.lock().await = restored.0;
                Ok(StreamOpenOutcome::Restored)
            } else {
                *self.committed_len.lock().await = 0;
                Ok(StreamOpenOutcome::Initial)
            }
        }

        async fn update(
            &self,
            _context: StreamUpdateContext<'_>,
        ) -> Result<ComponentStateEnvelope, StreamUpdateError> {
            let codec = vec_read_codec();
            let committed = *self.committed_len.lock().await;
            ComponentStateEnvelope::encode(
                self.namespace.clone(),
                &VecPosition(committed),
                &codec,
                StateLimits::default(),
            )
            .map_err(|_| StreamUpdateError::new())
        }

        async fn close(
            &self,
            _context: StreamCloseContext<'_>,
        ) -> Result<StreamCloseOutcome, StreamCloseError> {
            Ok(StreamCloseOutcome::Closed)
        }
    }

    struct TestWriterOpener {
        sink: Arc<std::sync::Mutex<HashMap<String, Vec<u64>>>>,
    }

    impl MultiResourceWriterOpener<u64> for TestWriterOpener {
        type Writer = VecItemWriter;
        type Stream = VecItemWriterStream;

        async fn open(
            &self,
            resource: &ResourceIdentity,
            _resource_ordinal: u32,
            delegate_identity: &ComponentStreamIdentity,
        ) -> Result<(Self::Writer, Self::Stream, StreamStateContract), MultiResourceOpenError>
        {
            let committed_len = Arc::new(AsyncMutex::new(0));
            let writer = VecItemWriter {
                resource: resource.as_str().to_owned(),
                sink: Arc::clone(&self.sink),
                committed_len: Arc::clone(&committed_len),
            };
            let stream = VecItemWriterStream {
                committed_len,
                namespace: delegate_identity.clone(),
            };
            Ok((writer, stream, StreamStateContract::new(vec_read_codec())))
        }
    }

    fn write_context(stop: &crate::StopToken) -> WriteContext<'_> {
        WriteContext::non_transactional(stop)
    }

    #[test]
    fn rollover_writes_batches_to_successive_resources_in_order() {
        let sink = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let opener = TestWriterOpener {
            sink: Arc::clone(&sink),
        };
        let (writer, stream, _contract) = multi_resource_writer::<u64, _, _>(
            resource_set(&["a", "b", "c"]),
            opener,
            identity("rollover"),
            BatchCountRollover::new(1),
            RestartabilityDeclaration::Restartable,
        )
        .unwrap();
        let (_source, token) = stop();
        futures_executor::block_on(async {
            stream
                .open(StreamOpenContext::new(None, &token))
                .await
                .unwrap();
            writer.write(&[1, 2], write_context(&token)).await.unwrap();
            writer.write(&[3], write_context(&token)).await.unwrap();
            writer.write(&[4, 5], write_context(&token)).await.unwrap();
        });
        let sink = sink.lock().unwrap();
        assert_eq!(sink.get("a"), Some(&vec![1, 2]));
        assert_eq!(sink.get("b"), Some(&vec![3]));
        assert_eq!(sink.get("c"), Some(&vec![4, 5]));
    }

    #[test]
    fn writer_restart_mid_resource_resumes_committed_position() {
        let sink = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let (_source, token) = stop();

        let committed_envelope = futures_executor::block_on(async {
            let opener = TestWriterOpener {
                sink: Arc::clone(&sink),
            };
            let (writer, stream, _contract) = multi_resource_writer::<u64, _, _>(
                resource_set(&["a", "b"]),
                opener,
                identity("writer-restart"),
                NoRollover,
                RestartabilityDeclaration::Restartable,
            )
            .unwrap();
            stream
                .open(StreamOpenContext::new(None, &token))
                .await
                .unwrap();
            writer.write(&[1, 2], write_context(&token)).await.unwrap();
            stream
                .update(StreamUpdateContext::new(&token))
                .await
                .unwrap()
        });

        let second_attempt_envelope = futures_executor::block_on(async {
            let opener = TestWriterOpener {
                sink: Arc::clone(&sink),
            };
            let (writer, stream, _contract) = multi_resource_writer::<u64, _, _>(
                resource_set(&["a", "b"]),
                opener,
                identity("writer-restart"),
                NoRollover,
                RestartabilityDeclaration::Restartable,
            )
            .unwrap();
            stream
                .open(StreamOpenContext::new(Some(&committed_envelope), &token))
                .await
                .unwrap();
            writer.write(&[3], write_context(&token)).await.unwrap();
            stream
                .update(StreamUpdateContext::new(&token))
                .await
                .unwrap()
        });

        // The delegate's own committed-length position must have resumed
        // from the first attempt's committed value (2) and advanced by
        // exactly this attempt's one batch (1) to 3 -- never restarting
        // from 0, and never silently rolling over to resource "b".
        let codec = multi_resource_position_codec(RestartabilityDeclaration::Restartable);
        let state = second_attempt_envelope
            .decode::<MultiResourceState>(&codec)
            .unwrap();
        assert_eq!(
            state.resource_index, 0,
            "must still be resource \"a\", index 0"
        );
        let delegate = state
            .delegate
            .expect("a written resource always has delegate state");
        let inner = delegate
            .to_envelope(identity("writer-restart"), StateLimits::default())
            .expect("embedded delegate envelope must decode");
        let inner_position = inner.decode::<VecPosition>(&vec_read_codec()).unwrap();
        assert_eq!(
            inner_position.0, 3,
            "committed length must resume from 2 and advance by this attempt's batch, not restart from 0"
        );

        // The fixture's writer appends unconditionally to a process-wide
        // sink regardless of restart (it does not reconcile against the
        // restored committed length the way a real backend like
        // `DelimitedWriter` truncates to it) -- that reconciliation is a
        // delegate responsibility, already covered by the delegate's own
        // tests, not something `MultiResourceWriter` re-implements. This
        // test's job is only the position round-trip asserted above.
        let sink = sink.lock().unwrap();
        assert_eq!(sink.get("a"), Some(&vec![1, 2, 3]));
    }

    #[test]
    fn rollover_counter_survives_restart_and_still_caps_batches_per_resource() {
        // `BatchCountRollover::new(2)` must cap resource "a" at exactly two
        // committed batches, restart or not. Before this fix,
        // `batches_written` was an in-memory-only counter that reset to `0`
        // on every restart, so a crash-and-restart mid-resource silently let
        // more than `max_batches_per_resource` batches land in one resource
        // -- the durable envelope now carries the true count instead.
        let sink = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let (_source, token) = stop();

        let committed_envelope = futures_executor::block_on(async {
            let opener = TestWriterOpener {
                sink: Arc::clone(&sink),
            };
            let (writer, stream, _contract) = multi_resource_writer::<u64, _, _>(
                resource_set(&["a", "b"]),
                opener,
                identity("rollover-restart"),
                BatchCountRollover::new(2),
                RestartabilityDeclaration::Restartable,
            )
            .unwrap();
            stream
                .open(StreamOpenContext::new(None, &token))
                .await
                .unwrap();
            // First committed batch to resource "a"; batches_written -> 1.
            writer.write(&[1], write_context(&token)).await.unwrap();
            stream
                .update(StreamUpdateContext::new(&token))
                .await
                .unwrap()
        });

        // Simulate a crash-and-restart: a fresh writer instance restores
        // from the committed envelope above.
        futures_executor::block_on(async {
            let opener = TestWriterOpener {
                sink: Arc::clone(&sink),
            };
            let (writer, stream, _contract) = multi_resource_writer::<u64, _, _>(
                resource_set(&["a", "b"]),
                opener,
                identity("rollover-restart"),
                BatchCountRollover::new(2),
                RestartabilityDeclaration::Restartable,
            )
            .unwrap();
            stream
                .open(StreamOpenContext::new(Some(&committed_envelope), &token))
                .await
                .unwrap();
            // If the restored count were wrongly reset to 0, this would be
            // read as the resource's 1st and 2nd post-restart batch and
            // both would land in "a", violating the cap of 2. With the
            // count correctly restored to 1, this single write is the
            // resource's 2nd batch (1 -> 2): still no rollover yet.
            writer.write(&[2], write_context(&token)).await.unwrap();
            // This next write is attempt number 3 for resource "a" with a
            // cap of 2: it must roll over to "b" first.
            writer.write(&[3], write_context(&token)).await.unwrap();
        });

        let sink = sink.lock().unwrap();
        assert_eq!(
            sink.get("a"),
            Some(&vec![1, 2]),
            "resource \"a\" must never exceed its 2-batch cap across the restart"
        );
        assert_eq!(
            sink.get("b"),
            Some(&vec![3]),
            "the third batch must have rolled over into resource \"b\""
        );
    }

    #[test]
    fn stale_resource_set_revision_is_rejected_on_writer_restart() {
        let sink = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let (_source, token) = stop();

        let committed_envelope = futures_executor::block_on(async {
            let opener = TestWriterOpener {
                sink: Arc::clone(&sink),
            };
            let (writer, stream, _contract) = multi_resource_writer::<u64, _, _>(
                resource_set(&["a", "b"]),
                opener,
                identity("writer-revision"),
                NoRollover,
                RestartabilityDeclaration::Restartable,
            )
            .unwrap();
            stream
                .open(StreamOpenContext::new(None, &token))
                .await
                .unwrap();
            writer.write(&[1], write_context(&token)).await.unwrap();
            stream
                .update(StreamUpdateContext::new(&token))
                .await
                .unwrap()
        });

        futures_executor::block_on(async {
            let opener = TestWriterOpener {
                sink: Arc::clone(&sink),
            };
            let (_writer, stream, _contract) = multi_resource_writer::<u64, _, _>(
                resource_set(&["a", "b", "c"]),
                opener,
                identity("writer-revision"),
                NoRollover,
                RestartabilityDeclaration::Restartable,
            )
            .unwrap();
            let result = stream
                .open(StreamOpenContext::new(Some(&committed_envelope), &token))
                .await;
            assert!(
                result.is_err(),
                "changed resource set must be rejected on writer restart too"
            );
        });
    }

    // -- Finding 1/3 (#177) regression evidence: writer-side recording
    // delegate mirroring the reader's above.

    struct RecordingWriterStream {
        committed_len: Arc<AsyncMutex<u64>>,
        namespace: ComponentStreamIdentity,
        resource: String,
        log: Log,
        fail_close_once: Arc<std::sync::atomic::AtomicBool>,
    }

    impl ItemStream for RecordingWriterStream {
        async fn open(
            &self,
            context: StreamOpenContext<'_>,
        ) -> Result<StreamOpenOutcome, StreamOpenError> {
            log_event(&self.log, LifecycleEvent::Open(self.resource.clone()));
            let codec = vec_read_codec();
            if let Some(envelope) = context.inherited_state() {
                let restored = envelope
                    .decode::<VecPosition>(&codec)
                    .map_err(|_| StreamOpenError::new())?;
                *self.committed_len.lock().await = restored.0;
                Ok(StreamOpenOutcome::Restored)
            } else {
                *self.committed_len.lock().await = 0;
                Ok(StreamOpenOutcome::Initial)
            }
        }

        async fn update(
            &self,
            _context: StreamUpdateContext<'_>,
        ) -> Result<ComponentStateEnvelope, StreamUpdateError> {
            log_event(&self.log, LifecycleEvent::Update(self.resource.clone()));
            let codec = vec_read_codec();
            let committed = *self.committed_len.lock().await;
            ComponentStateEnvelope::encode(
                self.namespace.clone(),
                &VecPosition(committed),
                &codec,
                StateLimits::default(),
            )
            .map_err(|_| StreamUpdateError::new())
        }

        async fn close(
            &self,
            context: StreamCloseContext<'_>,
        ) -> Result<StreamCloseOutcome, StreamCloseError> {
            log_event(
                &self.log,
                LifecycleEvent::Close(self.resource.clone(), context.outcome().into()),
            );
            if self
                .fail_close_once
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(StreamCloseError::new());
            }
            Ok(StreamCloseOutcome::Closed)
        }
    }

    struct RecordingWriterOpener {
        sink: Arc<std::sync::Mutex<HashMap<String, Vec<u64>>>>,
        log: Log,
        fail_open: HashSet<String>,
        fail_close_once: HashMap<String, Arc<std::sync::atomic::AtomicBool>>,
    }

    impl MultiResourceWriterOpener<u64> for RecordingWriterOpener {
        type Writer = VecItemWriter;
        type Stream = RecordingWriterStream;

        async fn open(
            &self,
            resource: &ResourceIdentity,
            resource_ordinal: u32,
            delegate_identity: &ComponentStreamIdentity,
        ) -> Result<(Self::Writer, Self::Stream, StreamStateContract), MultiResourceOpenError>
        {
            if self.fail_open.contains(resource.as_str()) {
                return Err(MultiResourceOpenError::new(resource_ordinal));
            }
            let committed_len = Arc::new(AsyncMutex::new(0));
            let writer = VecItemWriter {
                resource: resource.as_str().to_owned(),
                sink: Arc::clone(&self.sink),
                committed_len: Arc::clone(&committed_len),
            };
            let fail_close_once = self
                .fail_close_once
                .get(resource.as_str())
                .cloned()
                .unwrap_or_else(|| Arc::new(std::sync::atomic::AtomicBool::new(false)));
            let stream = RecordingWriterStream {
                committed_len,
                namespace: delegate_identity.clone(),
                resource: resource.as_str().to_owned(),
                log: Arc::clone(&self.log),
                fail_close_once,
            };
            Ok((writer, stream, StreamStateContract::new(vec_read_codec())))
        }
    }

    #[test]
    fn writer_rollover_closes_outgoing_delegate_exactly_once_with_resource_boundary_outcome() {
        let sink = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let log: Log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let opener = RecordingWriterOpener {
            sink: Arc::clone(&sink),
            log: Arc::clone(&log),
            fail_open: HashSet::new(),
            fail_close_once: HashMap::new(),
        };
        let (writer, stream, _contract) = multi_resource_writer::<u64, _, _>(
            resource_set(&["a", "b"]),
            opener,
            identity("writer-lifecycle-order"),
            BatchCountRollover::new(1),
            RestartabilityDeclaration::Restartable,
        )
        .unwrap();
        let (_source, token) = stop();
        futures_executor::block_on(async {
            stream
                .open(StreamOpenContext::new(None, &token))
                .await
                .unwrap();
            writer.write(&[1], write_context(&token)).await.unwrap();
            writer.write(&[2], write_context(&token)).await.unwrap();
        });
        let events = log.lock().unwrap().clone();
        assert_eq!(
            events,
            vec![
                LifecycleEvent::Open("a".to_owned()),
                LifecycleEvent::Close("a".to_owned(), RecordedOutcome::ResourceBoundary),
                LifecycleEvent::Open("b".to_owned()),
            ],
            "rollover must close the outgoing delegate (\"a\") exactly once, \
             with `ResourceBoundary` -- never `Committed` -- before opening \
             the next resource (\"b\")"
        );
    }

    #[test]
    fn writer_resource_boundary_close_failure_is_propagated_without_rolling_over() {
        let sink = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let log: Log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut fail_close_once = HashMap::new();
        fail_close_once.insert(
            "a".to_owned(),
            Arc::new(std::sync::atomic::AtomicBool::new(true)),
        );
        let opener = RecordingWriterOpener {
            sink: Arc::clone(&sink),
            log: Arc::clone(&log),
            fail_open: HashSet::new(),
            fail_close_once,
        };
        let (writer, stream, _contract) = multi_resource_writer::<u64, _, _>(
            resource_set(&["a", "b"]),
            opener,
            identity("writer-boundary-close-failure"),
            BatchCountRollover::new(1),
            RestartabilityDeclaration::Restartable,
        )
        .unwrap();
        let (_source, token) = stop();
        futures_executor::block_on(async {
            stream
                .open(StreamOpenContext::new(None, &token))
                .await
                .unwrap();
            writer.write(&[1], write_context(&token)).await.unwrap();
            // Rollover to "b" is now due; closing "a" fails.
            let result = writer.write(&[2], write_context(&token)).await;
            assert!(result.is_err(), "a boundary close failure must surface");
            // The checkpoint must still describe "a", not "b".
            let envelope = stream
                .update(StreamUpdateContext::new(&token))
                .await
                .unwrap();
            let codec = multi_resource_position_codec(RestartabilityDeclaration::Restartable);
            let state = envelope.decode::<MultiResourceState>(&codec).unwrap();
            assert_eq!(
                state.resource_index, 0,
                "checkpoint advancement must not silently occur after a failed boundary close"
            );
            // A retry re-attempts the same close (which now succeeds) and
            // completes the rollover to "b".
            writer.write(&[2], write_context(&token)).await.unwrap();
        });
        let events = log.lock().unwrap().clone();
        assert_eq!(
            events,
            vec![
                LifecycleEvent::Open("a".to_owned()),
                LifecycleEvent::Close("a".to_owned(), RecordedOutcome::ResourceBoundary),
                LifecycleEvent::Update("a".to_owned()),
                LifecycleEvent::Close("a".to_owned(), RecordedOutcome::ResourceBoundary),
                LifecycleEvent::Open("b".to_owned()),
            ],
            "exactly one failed close attempt on \"a\" followed by exactly one \
             successful retry, never a second delegate opened for \"a\", never \
             \"b\" opened before \"a\" is fully retired"
        );
        let sink = sink.lock().unwrap();
        assert_eq!(
            sink.get("a"),
            Some(&vec![1]),
            "the item from the failed rollover attempt must never have \
             reached \"b\" -- write only succeeded once the retry rolled \
             over cleanly"
        );
        assert_eq!(sink.get("b"), Some(&vec![2]));
    }

    #[test]
    fn outer_terminal_close_does_not_double_close_a_writer_delegate_already_retired_at_a_boundary()
    {
        let sink = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let log: Log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut fail_open = HashSet::new();
        fail_open.insert("b".to_owned());
        let opener = RecordingWriterOpener {
            sink: Arc::clone(&sink),
            log: Arc::clone(&log),
            fail_open,
            fail_close_once: HashMap::new(),
        };
        let (writer, stream, _contract) = multi_resource_writer::<u64, _, _>(
            resource_set(&["a", "b"]),
            opener,
            identity("writer-no-double-close"),
            BatchCountRollover::new(1),
            RestartabilityDeclaration::Restartable,
        )
        .unwrap();
        let (_source, token) = stop();
        futures_executor::block_on(async {
            stream
                .open(StreamOpenContext::new(None, &token))
                .await
                .unwrap();
            writer.write(&[1], write_context(&token)).await.unwrap();
            // Rollover is now due: "a" closes successfully, but opening "b"
            // then fails, so the step attempt gives up and the outer
            // terminal close runs with the step's real (failed) outcome.
            let result = writer.write(&[2], write_context(&token)).await;
            assert!(result.is_err());
            stream
                .close(StreamCloseContext::new(
                    &token,
                    StreamRuntimeOutcome::Failed,
                ))
                .await
                .unwrap();
        });
        let events = log.lock().unwrap().clone();
        assert_eq!(
            events,
            vec![
                LifecycleEvent::Open("a".to_owned()),
                LifecycleEvent::Close("a".to_owned(), RecordedOutcome::ResourceBoundary),
            ],
            "\"a\" must be closed exactly once total -- the outer terminal \
             close must recognize it is already retired and skip it, not \
             close it a second time with the step's real outcome"
        );
    }

    #[test]
    fn writer_update_fails_closed_when_delegate_reports_the_wrong_namespace() {
        struct WrongNamespaceWriterStream {
            wrong_namespace: ComponentStreamIdentity,
        }
        impl ItemStream for WrongNamespaceWriterStream {
            async fn open(
                &self,
                context: StreamOpenContext<'_>,
            ) -> Result<StreamOpenOutcome, StreamOpenError> {
                Ok(if context.inherited_state().is_some() {
                    StreamOpenOutcome::Restored
                } else {
                    StreamOpenOutcome::Initial
                })
            }
            async fn update(
                &self,
                _context: StreamUpdateContext<'_>,
            ) -> Result<ComponentStateEnvelope, StreamUpdateError> {
                ComponentStateEnvelope::encode(
                    self.wrong_namespace.clone(),
                    &VecPosition(0),
                    &vec_read_codec(),
                    StateLimits::default(),
                )
                .map_err(|_| StreamUpdateError::new())
            }
            async fn close(
                &self,
                _context: StreamCloseContext<'_>,
            ) -> Result<StreamCloseOutcome, StreamCloseError> {
                Ok(StreamCloseOutcome::Closed)
            }
        }

        struct WrongNamespaceWriterOpener {
            sink: Arc<std::sync::Mutex<HashMap<String, Vec<u64>>>>,
            wrong_namespace: ComponentStreamIdentity,
        }
        impl MultiResourceWriterOpener<u64> for WrongNamespaceWriterOpener {
            type Writer = VecItemWriter;
            type Stream = WrongNamespaceWriterStream;
            async fn open(
                &self,
                resource: &ResourceIdentity,
                _resource_ordinal: u32,
                _delegate_identity: &ComponentStreamIdentity,
            ) -> Result<(Self::Writer, Self::Stream, StreamStateContract), MultiResourceOpenError>
            {
                Ok((
                    VecItemWriter {
                        resource: resource.as_str().to_owned(),
                        sink: Arc::clone(&self.sink),
                        committed_len: Arc::new(AsyncMutex::new(0)),
                    },
                    WrongNamespaceWriterStream {
                        wrong_namespace: self.wrong_namespace.clone(),
                    },
                    StreamStateContract::new(vec_read_codec()),
                ))
            }
        }

        let sink = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let opener = WrongNamespaceWriterOpener {
            sink,
            wrong_namespace: identity("attacker-namespace-writer"),
        };
        let (writer, stream, _contract) = multi_resource_writer::<u64, _, _>(
            resource_set(&["a"]),
            opener,
            identity("expected-namespace-writer"),
            NoRollover,
            RestartabilityDeclaration::Restartable,
        )
        .unwrap();
        let (_source, token) = stop();
        futures_executor::block_on(async {
            stream
                .open(StreamOpenContext::new(None, &token))
                .await
                .unwrap();
            writer.write(&[1], write_context(&token)).await.unwrap();
            let result = stream.update(StreamUpdateContext::new(&token)).await;
            assert!(
                result.is_err(),
                "a nested delegate reporting the wrong namespace must fail \
                 the outer update closed, not be embedded into an outer \
                 candidate that could reach durable checkpoint state"
            );
        });
    }

    // -- #146 residual composition audit (#150): both `PeekReader` and
    // `SynchronizedWriter` delegate 100% of restartability/ordering to
    // their inner component (peek.rs:41-49, sync.rs's own docs), so they
    // should compose over `MultiResourceReader`/`MultiResourceWriter` with
    // no special-casing needed. These two tests are that audit's evidence,
    // not a production-code gap fix.

    #[test]
    fn peek_reader_over_multi_resource_reader_crosses_resource_boundary_without_corrupting_order() {
        use crate::item_components::PeekReader;

        let mut data = HashMap::new();
        data.insert("a".to_owned(), vec![1, 2]);
        data.insert("b".to_owned(), vec![3, 4]);
        let opener = TestReaderOpener {
            data,
            fail: HashSet::new(),
        };
        let (reader, stream, _contract) = multi_resource_reader::<u64, _>(
            resource_set(&["a", "b"]),
            opener,
            identity("peek-composition"),
            RestartabilityDeclaration::Restartable,
        );
        let mut peeking = PeekReader::new(reader);
        let (_source, token) = stop();
        futures_executor::block_on(async {
            stream
                .open(StreamOpenContext::new(None, &token))
                .await
                .unwrap();
            let mut collected = Vec::new();
            loop {
                // Peek before every real read, exactly like the existing
                // `postgres_item_components_restart.rs` decorator-restart
                // evidence does for a single-resource reader -- proving
                // peeking never corrupts the *multi*-resource transition
                // in particular (crossing from "a" to "b" mid-peek).
                match peeking.peek(ReadContext::new(&token)).await.unwrap() {
                    crate::item_components::PeekOutcome::Item(_) => {}
                    crate::item_components::PeekOutcome::EndOfInput => break,
                    crate::item_components::PeekOutcome::Stopped => panic!("unexpected stop"),
                }
                match peeking.read(ReadContext::new(&token)).await.unwrap() {
                    ReadOutcome::Item(item) => collected.push(item),
                    ReadOutcome::EndOfInput => break,
                    ReadOutcome::Stopped => panic!("unexpected stop"),
                }
            }
            assert_eq!(collected, vec![1, 2, 3, 4]);
        });
    }

    #[test]
    fn synchronized_writer_over_multi_resource_writer_preserves_rollover_order() {
        use crate::item_components::SynchronizedWriter;

        let sink = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let opener = TestWriterOpener {
            sink: Arc::clone(&sink),
        };
        let (writer, stream, _contract) = multi_resource_writer::<u64, _, _>(
            resource_set(&["a", "b"]),
            opener,
            identity("sync-composition"),
            BatchCountRollover::new(1),
            RestartabilityDeclaration::Restartable,
        )
        .unwrap();
        let synchronized = SynchronizedWriter::new(writer);
        let (_source, token) = stop();
        futures_executor::block_on(async {
            stream
                .open(StreamOpenContext::new(None, &token))
                .await
                .unwrap();
            synchronized
                .write(&[1], write_context(&token))
                .await
                .unwrap();
            synchronized
                .write(&[2], write_context(&token))
                .await
                .unwrap();
        });
        let sink = sink.lock().unwrap();
        assert_eq!(sink.get("a"), Some(&vec![1]));
        assert_eq!(sink.get("b"), Some(&vec![2]));
    }
}
