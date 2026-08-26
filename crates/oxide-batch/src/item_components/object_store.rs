//! Object-store capability basics (#150, M6 slice of `IO-OBJECT-001`).
//!
//! This module provides the **provider-neutral capability boundary** an
//! object-store adapter (S3, Azure Blob, GCS, ...) would implement against:
//! bounded get/put/stat/list, with a stable, provider-agnostic version token
//! where the backend can supply one. **No cloud SDK integration ships here.**
//! Full S3/Azure/GCS certification -- credential management, multipart
//! upload, provider-specific retry/consistency semantics -- remains M9, per
//! the ledger's own M6/M9 split for `IO-OBJECT-001`.
//!
//! [`InMemoryObjectStore`] is a first-class fixture, not a toy: it is the
//! contract evidence for this module's capability semantics (deterministic
//! list pagination, version-token mismatch rejection, bounded read/write),
//! and the only [`ObjectStoreCapability`] implementation this crate ships.
//!
//! [`ObjectStoreReaderOpener`]/[`ObjectStoreWriterOpener`] bridge any
//! [`ObjectStoreCapability`] into
//! [`crate::item_components::multi_resource::MultiResourceReaderOpener`]/
//! [`crate::item_components::multi_resource::MultiResourceWriterOpener`], so
//! object-store multi-resource I/O sits on exactly the same ordered,
//! versioned, restartable model as file-backed multi-resource I/O -- the
//! whole point of building this after (and on top of) that module, per
//! #150's own scope note that object storage should not need a second
//! restart model when S3/Azure/GCS adapters land in M9.
//!
//! # Whole-object buffering (M6 basics)
//!
//! Both bridges buffer a whole object in memory: [`ObjectStoreReaderOpener`]
//! fetches and fully parses one object before serving items from it;
//! [`ObjectStoreWriterOpener`]'s accumulator holds everything written to the
//! current object so far, and reissues the full object on every
//! [`crate::ItemWriter::write`] call (object-store `PUT` semantics are
//! whole-object, not append). This is bounded by `max_object_bytes` as a
//! real, pre-materialization resource bound: [`ObjectStoreCapability::get`]
//! rejects an oversized object before a buffer proportional to its true
//! size is allocated (see its own contract), and [`ObjectItemWriter`]'s
//! `write` serializes each item into a `BoundedSink` (private) over the candidate
//! buffer, which refuses any single write that would exceed the bound
//! *before* copying a byte -- not a post-hoc length check on an
//! already-built buffer. The one residual, inherent limit -- a `serialize`
//! implementation that builds one large owned buffer internally before
//! ever writing to the sink has already paid that allocation before the
//! sink can refuse it -- is documented on [`ObjectStoreWriterOpener::new`],
//! not hidden. It is not streaming/multipart -- that stays M9.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};

use tokio::sync::Mutex as AsyncMutex;

use crate::item_components::multi_resource::{
    MultiResourceOpenError, MultiResourceReaderOpener, MultiResourceWriterOpener, ResourceIdentity,
};
use crate::{
    ComponentStreamIdentity, DefaultComponentCodec, FailureCategory, ItemReader, ItemStream,
    ItemWriter, ReadContext, ReadOutcome, ReaderError, RestartabilityDeclaration, StateCodecError,
    StateLimits, StateSchemaId, StateSchemaVersion, StateSensitivity, StreamCloseContext,
    StreamCloseError, StreamCloseOutcome, StreamOpenContext, StreamOpenError, StreamOpenOutcome,
    StreamStateContract, StreamUpdateContext, StreamUpdateError, VersionedStateCodec, WriteContext,
    WriteOutcome, WriterError,
};

// ---------------------------------------------------------------------
// Identity, version, metadata
// ---------------------------------------------------------------------

const MAX_OBJECT_KEY_BYTES: usize = 1024;

/// A stable object key (including any bucket/container prefix the caller
/// chooses to encode into it -- this module does not model bucket/container
/// as a separate axis, to stay provider-neutral).
///
/// Safe to display, like [`crate::item_components::multi_resource::ResourceIdentity`]:
/// this is diagnostic metadata, not object content.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ObjectIdentity(String);

impl ObjectIdentity {
    /// Validates a stable object key.
    ///
    /// # Errors
    ///
    /// Returns [`ObjectStoreConfigError`] when `key` is empty, exceeds 1024
    /// UTF-8 bytes, or contains a control character.
    pub fn new(key: impl Into<String>) -> Result<Self, ObjectStoreConfigError> {
        let key = key.into();
        if key.is_empty() {
            return Err(ObjectStoreConfigError::EmptyKey);
        }
        if key.len() > MAX_OBJECT_KEY_BYTES {
            return Err(ObjectStoreConfigError::KeyTooLong {
                max_bytes: MAX_OBJECT_KEY_BYTES,
            });
        }
        if key.chars().any(char::is_control) {
            return Err(ObjectStoreConfigError::MalformedKey);
        }
        Ok(Self(key))
    }

    /// Borrows the validated key.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ObjectIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ObjectIdentity")
            .field(&self.0)
            .finish()
    }
}

impl fmt::Display for ObjectIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// A validation failure building an [`ObjectIdentity`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ObjectStoreConfigError {
    /// An object key was empty.
    EmptyKey,
    /// An object key exceeded its UTF-8 byte limit.
    KeyTooLong {
        /// Maximum accepted UTF-8 bytes.
        max_bytes: usize,
    },
    /// An object key contained a control character.
    MalformedKey,
}

impl fmt::Display for ObjectStoreConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyKey => formatter.write_str("object key must not be empty"),
            Self::KeyTooLong { max_bytes } => {
                write!(formatter, "object key exceeds {max_bytes} UTF-8 bytes")
            }
            Self::MalformedKey => formatter.write_str("object key contains a control character"),
        }
    }
}

impl std::error::Error for ObjectStoreConfigError {}

/// An opaque, provider-supplied stable version token (`ETag`, version ID,
/// generation, ...).
///
/// A backend without a stable per-object version identity cannot construct
/// one honestly; [`ObjectMetadata::version_token`] is `None` in that case,
/// and a reader/writer built over such a backend must declare
/// [`RestartabilityDeclaration::NotRestartable`] rather than claim a
/// guarantee the backend cannot back up (see
/// [`crate::item_components::multi_resource::multi_resource_reader`]'s
/// `restartability` parameter).
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct ObjectVersionToken(String);

impl ObjectVersionToken {
    /// Wraps an opaque, backend-supplied version token.
    #[must_use]
    pub fn new(token: impl Into<String>) -> Self {
        Self(token.into())
    }

    /// Borrows the opaque token.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ObjectVersionToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ObjectVersionToken")
            .field(&self.0)
            .finish()
    }
}

impl fmt::Display for ObjectVersionToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Metadata for one object, returned by [`ObjectStoreCapability::stat`],
/// [`ObjectStoreCapability::get`], and [`ObjectStoreCapability::put`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectMetadata {
    size: u64,
    version_token: Option<ObjectVersionToken>,
}

impl ObjectMetadata {
    /// Constructs object metadata.
    #[must_use]
    pub const fn new(size: u64, version_token: Option<ObjectVersionToken>) -> Self {
        Self {
            size,
            version_token,
        }
    }

    /// Returns the object's size in bytes.
    #[must_use]
    pub const fn size(&self) -> u64 {
        self.size
    }

    /// Borrows the object's stable version token, when the backend supplies
    /// one.
    #[must_use]
    pub const fn version_token(&self) -> Option<&ObjectVersionToken> {
        self.version_token.as_ref()
    }
}

/// An opaque continuation token for [`ObjectStoreCapability::list`]'s
/// deterministic pagination.
#[derive(Clone, Eq, PartialEq)]
pub struct ObjectListContinuation(String);

impl fmt::Debug for ObjectListContinuation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ObjectListContinuation")
            .field(&self.0)
            .finish()
    }
}

/// One page of [`ObjectStoreCapability::list`] results, in a deterministic
/// order the backend defines and holds constant across calls with the same
/// prefix -- never a provider's unspecified/unstable listing order.
#[derive(Clone, Debug)]
pub struct ObjectListPage {
    entries: Vec<(ObjectIdentity, ObjectMetadata)>,
    continuation: Option<ObjectListContinuation>,
}

impl ObjectListPage {
    /// Borrows this page's entries, in deterministic order.
    #[must_use]
    pub fn entries(&self) -> &[(ObjectIdentity, ObjectMetadata)] {
        &self.entries
    }

    /// Returns the continuation token for the next page, if more entries
    /// remain.
    #[must_use]
    pub const fn continuation(&self) -> Option<&ObjectListContinuation> {
        self.continuation.as_ref()
    }
}

/// A redacted object-store failure: a stable category, never the
/// underlying provider error's payload or message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectStoreError {
    category: FailureCategory,
}

impl ObjectStoreError {
    /// Constructs a value-redacted [`FailureCategory::UserComponent`]
    /// failure.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            category: FailureCategory::UserComponent,
        }
    }

    /// Constructs a failure that declares its own stable category.
    #[must_use]
    pub const fn with_category(category: FailureCategory) -> Self {
        Self { category }
    }

    /// Returns the stable category.
    #[must_use]
    pub const fn category(self) -> FailureCategory {
        self.category
    }
}

impl Default for ObjectStoreError {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ObjectStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("object store operation failed")
    }
}

impl std::error::Error for ObjectStoreError {}

// ---------------------------------------------------------------------
// Capability
// ---------------------------------------------------------------------

/// The provider-neutral object-store capability boundary (M6 basics).
///
/// # Contract every implementation must honor
///
/// - **Bounded read/write**: `get`/`put` operate on the whole object, up to
///   a *caller-declared* bound (`get`'s `max_bytes`, `put`'s own size),
///   neither is unbounded streaming (multipart upload/download is M9
///   scope). Critically, the bound is a real resource bound, not
///   post-materialization validation: an implementation must reject an
///   object larger than `max_bytes` before allocating/copying a buffer
///   proportional to its true, oversized size -- a `stat`-then-compare, a
///   ranged read, or (as [`InMemoryObjectStore`] does, since it already
///   holds the object's bytes in memory regardless) checking the known
///   length before ever cloning it are all acceptable; materializing the
///   full oversized object first and rejecting it afterward is not.
/// - **Deterministic listing**: two `list` calls for the same prefix (with
///   no intervening `put`/mutation) return entries in the same order.
/// - **Version identity**: `version_token` is `None` only when the backend
///   genuinely cannot supply a stable one -- never fabricated.
/// - **Sensitive metadata**: implementations must not leak object content
///   or raw provider error payloads through [`ObjectStoreError`].
pub trait ObjectStoreCapability: Send + Sync {
    /// Fetches an object's full content and metadata, rejecting before
    /// delivery if the object exceeds `max_bytes`.
    ///
    /// # Errors
    ///
    /// Returns [`FailureCategory::UnsupportedCapability`]-categorized
    /// [`ObjectStoreError`] when `id` does not exist,
    /// [`FailureCategory::Invariant`]-categorized [`ObjectStoreError`] when
    /// the object exceeds `max_bytes`, or another redacted failure category
    /// for other faults.
    fn get<'a>(
        &'a self,
        id: &'a ObjectIdentity,
        max_bytes: usize,
    ) -> impl Future<Output = Result<(Vec<u8>, ObjectMetadata), ObjectStoreError>> + Send + 'a;

    /// Writes an object's full content, replacing any prior content at
    /// `id`, and returns the resulting metadata (with a fresh version token
    /// when the backend supplies one).
    ///
    /// Takes `bytes` by reference so a caller that must retain its own copy
    /// (e.g. an accumulator that keeps growing) never has to clone it a
    /// second time purely to satisfy this call.
    ///
    /// # Errors
    ///
    /// Returns a redacted [`ObjectStoreError`].
    fn put<'a>(
        &'a self,
        id: &'a ObjectIdentity,
        bytes: &'a [u8],
    ) -> impl Future<Output = Result<ObjectMetadata, ObjectStoreError>> + Send + 'a;

    /// Fetches an object's metadata without its content.
    ///
    /// # Errors
    ///
    /// Returns [`FailureCategory::UnsupportedCapability`]-categorized
    /// [`ObjectStoreError`] when `id` does not exist.
    fn stat<'a>(
        &'a self,
        id: &'a ObjectIdentity,
    ) -> impl Future<Output = Result<ObjectMetadata, ObjectStoreError>> + Send + 'a;

    /// Lists objects under `prefix` in deterministic order, at most
    /// `page_size` per call.
    ///
    /// # Errors
    ///
    /// Returns a redacted [`ObjectStoreError`].
    fn list<'a>(
        &'a self,
        prefix: &'a str,
        page_size: usize,
        continuation: Option<&'a ObjectListContinuation>,
    ) -> impl Future<Output = Result<ObjectListPage, ObjectStoreError>> + Send + 'a;
}

impl<C: ObjectStoreCapability + ?Sized> ObjectStoreCapability for Arc<C> {
    fn get<'a>(
        &'a self,
        id: &'a ObjectIdentity,
        max_bytes: usize,
    ) -> impl Future<Output = Result<(Vec<u8>, ObjectMetadata), ObjectStoreError>> + Send + 'a {
        C::get(self, id, max_bytes)
    }

    fn put<'a>(
        &'a self,
        id: &'a ObjectIdentity,
        bytes: &'a [u8],
    ) -> impl Future<Output = Result<ObjectMetadata, ObjectStoreError>> + Send + 'a {
        C::put(self, id, bytes)
    }

    fn stat<'a>(
        &'a self,
        id: &'a ObjectIdentity,
    ) -> impl Future<Output = Result<ObjectMetadata, ObjectStoreError>> + Send + 'a {
        C::stat(self, id)
    }

    fn list<'a>(
        &'a self,
        prefix: &'a str,
        page_size: usize,
        continuation: Option<&'a ObjectListContinuation>,
    ) -> impl Future<Output = Result<ObjectListPage, ObjectStoreError>> + Send + 'a {
        C::list(self, prefix, page_size, continuation)
    }
}

// ---------------------------------------------------------------------
// In-memory fixture
// ---------------------------------------------------------------------

#[derive(Clone)]
struct StoredObject {
    bytes: Vec<u8>,
    version: u64,
}

/// A first-class in-memory [`ObjectStoreCapability`] fixture.
///
/// Not a toy: this is the executable contract evidence for this module's
/// capability semantics (deterministic pagination, version-token mismatch
/// rejection, bounded read/write). No real cloud SDK ships in this crate --
/// S3/Azure/GCS adapters implementing the same [`ObjectStoreCapability`]
/// trait are M9 scope.
pub struct InMemoryObjectStore {
    objects: Mutex<BTreeMap<String, StoredObject>>,
    max_object_bytes: usize,
}

impl InMemoryObjectStore {
    /// Builds an empty store bounding any single object to
    /// `max_object_bytes`.
    #[must_use]
    pub fn new(max_object_bytes: usize) -> Self {
        Self {
            objects: Mutex::new(BTreeMap::new()),
            max_object_bytes,
        }
    }

    fn version_token(version: u64) -> ObjectVersionToken {
        ObjectVersionToken::new(format!("v{version}"))
    }
}

impl ObjectStoreCapability for InMemoryObjectStore {
    async fn get(
        &self,
        id: &ObjectIdentity,
        max_bytes: usize,
    ) -> Result<(Vec<u8>, ObjectMetadata), ObjectStoreError> {
        let objects = self.objects.lock().unwrap_or_else(PoisonError::into_inner);
        let stored = objects.get(id.as_str()).ok_or_else(|| {
            ObjectStoreError::with_category(FailureCategory::UnsupportedCapability)
        })?;
        // Reject before cloning: even though this fixture already holds the
        // object's bytes in memory regardless, the wrapper-visible resource
        // bound must not let a caller pay for a copy proportional to an
        // oversized object merely to have it rejected afterward.
        if stored.bytes.len() > max_bytes {
            return Err(ObjectStoreError::with_category(FailureCategory::Invariant));
        }
        let metadata = ObjectMetadata::new(
            u64::try_from(stored.bytes.len()).unwrap_or(u64::MAX),
            Some(Self::version_token(stored.version)),
        );
        Ok((stored.bytes.clone(), metadata))
    }

    async fn put(
        &self,
        id: &ObjectIdentity,
        bytes: &[u8],
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        if bytes.len() > self.max_object_bytes {
            return Err(ObjectStoreError::with_category(FailureCategory::Invariant));
        }
        let mut objects = self.objects.lock().unwrap_or_else(PoisonError::into_inner);
        let version = objects
            .get(id.as_str())
            .map_or(1, |existing| existing.version + 1);
        let size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        objects.insert(
            id.as_str().to_owned(),
            StoredObject {
                bytes: bytes.to_vec(),
                version,
            },
        );
        Ok(ObjectMetadata::new(
            size,
            Some(Self::version_token(version)),
        ))
    }

    async fn stat(&self, id: &ObjectIdentity) -> Result<ObjectMetadata, ObjectStoreError> {
        let objects = self.objects.lock().unwrap_or_else(PoisonError::into_inner);
        let stored = objects.get(id.as_str()).ok_or_else(|| {
            ObjectStoreError::with_category(FailureCategory::UnsupportedCapability)
        })?;
        Ok(ObjectMetadata::new(
            u64::try_from(stored.bytes.len()).unwrap_or(u64::MAX),
            Some(Self::version_token(stored.version)),
        ))
    }

    async fn list(
        &self,
        prefix: &str,
        page_size: usize,
        continuation: Option<&ObjectListContinuation>,
    ) -> Result<ObjectListPage, ObjectStoreError> {
        let objects = self.objects.lock().unwrap_or_else(PoisonError::into_inner);
        let start_after = continuation.map(|token| token.0.clone());
        let mut entries = Vec::new();
        let mut next_continuation = None;
        // The continuation token is the *last key this page actually
        // returned*, resumed exclusively (`key <= after` is skipped) --
        // never the first key of the next page. Storing the overflow key
        // itself here would double-skip it against the exclusive resume
        // check below and silently drop it from every later page.
        let mut last_returned_key: Option<String> = None;
        for (key, stored) in objects.range(prefix.to_owned()..) {
            if !key.starts_with(prefix) {
                break;
            }
            if let Some(after) = &start_after
                && key <= after
            {
                continue;
            }
            if entries.len() == page_size {
                next_continuation = last_returned_key.clone().map(ObjectListContinuation);
                break;
            }
            let metadata = ObjectMetadata::new(
                u64::try_from(stored.bytes.len()).unwrap_or(u64::MAX),
                Some(Self::version_token(stored.version)),
            );
            let identity = ObjectIdentity::new(key.clone())
                .map_err(|_| ObjectStoreError::with_category(FailureCategory::Invariant))?;
            entries.push((identity, metadata));
            last_returned_key = Some(key.clone());
        }
        Ok(ObjectListPage {
            entries,
            continuation: next_continuation,
        })
    }
}

// ---------------------------------------------------------------------
// Reader bridge into the multi-resource model
// ---------------------------------------------------------------------

#[derive(Clone, Eq, PartialEq)]
struct ObjectReadPosition {
    item_ordinal: u64,
    version_token: Option<String>,
}

const OBJECT_READ_SCHEMA: &str = "oxide-batch.object-store-read-position";
const OBJECT_READ_CODEC: &str = "oxide-batch.object-store-read-position-codec";

#[derive(Clone, Copy)]
struct ObjectReadPositionSchema;

impl VersionedStateCodec<ObjectReadPosition> for ObjectReadPositionSchema {
    fn schema_id(&self) -> &StateSchemaId {
        static SCHEMA: std::sync::OnceLock<StateSchemaId> = std::sync::OnceLock::new();
        #[allow(
            clippy::unwrap_used,
            reason = "fixed literal schema identity cannot fail validation"
        )]
        SCHEMA.get_or_init(|| StateSchemaId::new(OBJECT_READ_SCHEMA).unwrap())
    }

    fn current_version(&self) -> StateSchemaVersion {
        #[allow(
            clippy::unwrap_used,
            reason = "fixed literal schema version cannot fail validation"
        )]
        StateSchemaVersion::new(1).unwrap()
    }

    fn encode(&self, value: &ObjectReadPosition) -> Result<Vec<u8>, StateCodecError> {
        serde_json::to_vec(&serde_json::json!({
            "item_ordinal": value.item_ordinal,
            "version_token": value.version_token,
        }))
        .map_err(|_| StateCodecError::InvalidPayload)
    }

    fn decode(&self, payload: &[u8]) -> Result<ObjectReadPosition, StateCodecError> {
        let value: serde_json::Value =
            serde_json::from_slice(payload).map_err(|_| StateCodecError::InvalidPayload)?;
        let item_ordinal = value
            .get("item_ordinal")
            .and_then(serde_json::Value::as_u64)
            .ok_or(StateCodecError::InvalidPayload)?;
        let version_token = match value.get("version_token") {
            Some(serde_json::Value::Null) | None => None,
            Some(serde_json::Value::String(token)) => Some(token.clone()),
            Some(_) => return Err(StateCodecError::InvalidPayload),
        };
        Ok(ObjectReadPosition {
            item_ordinal,
            version_token,
        })
    }
}

#[allow(
    clippy::unwrap_used,
    reason = "fixed literal identities cannot fail validation"
)]
fn object_read_position_codec() -> DefaultComponentCodec<ObjectReadPositionSchema> {
    DefaultComponentCodec::new(
        ObjectReadPositionSchema,
        crate::CodecId::new(OBJECT_READ_CODEC).unwrap(),
        crate::CodecVersion::new(1).unwrap(),
        RestartabilityDeclaration::Restartable,
    )
    .with_sensitivity(StateSensitivity::NonSensitive)
}

/// A restartable [`crate::ItemReader`] over one whole fetched object,
/// serving pre-parsed items by ordinal.
pub struct ObjectItemReader<I> {
    items: Vec<I>,
    ordinal: Arc<Mutex<u64>>,
}

impl<I> ItemReader<I> for ObjectItemReader<I>
where
    I: Clone + Send + 'static,
{
    async fn read(&mut self, _context: ReadContext<'_>) -> Result<ReadOutcome<I>, ReaderError> {
        let mut ordinal = self.ordinal.lock().unwrap_or_else(PoisonError::into_inner);
        let index = usize::try_from(*ordinal).unwrap_or(usize::MAX);
        match self.items.get(index) {
            Some(item) => {
                let item = item.clone();
                *ordinal += 1;
                Ok(ReadOutcome::Item(item))
            }
            None => Ok(ReadOutcome::EndOfInput),
        }
    }
}

/// The [`crate::ItemStream`] half of an [`ObjectItemReader`]: rejects a
/// restart whose recorded object version no longer matches the freshly
/// fetched object (the object was replaced since the last committed
/// checkpoint), rather than silently resuming against different content.
pub struct ObjectItemReaderStream {
    ordinal: Arc<Mutex<u64>>,
    fetched_version: Option<ObjectVersionToken>,
    namespace: ComponentStreamIdentity,
}

impl ItemStream for ObjectItemReaderStream {
    async fn open(
        &self,
        context: StreamOpenContext<'_>,
    ) -> Result<StreamOpenOutcome, StreamOpenError> {
        let codec = object_read_position_codec();
        let fetched = self
            .fetched_version
            .as_ref()
            .map(|token| token.as_str().to_owned());
        if let Some(envelope) = context.inherited_state() {
            let restored = envelope
                .decode::<ObjectReadPosition>(&codec)
                .map_err(|_| StreamOpenError::new())?;
            let proven_identical = matches!(
                (&restored.version_token, &fetched),
                (Some(then), Some(now)) if then == now
            );
            if !proven_identical {
                // The object this attempt fetched is not proven identical to
                // the version the last committed checkpoint was recorded
                // against -- replaced content, an uncommitted write left a
                // different version in place, or the backend cannot supply a
                // stable version token at all (both sides `None`, which is
                // not proof of anything and must not be read as a match).
                // Fail closed rather than silently resuming an ordinal
                // against content that was never verified.
                return Err(StreamOpenError::with_category(
                    FailureCategory::UnsupportedCapability,
                ));
            }
            *self.ordinal.lock().unwrap_or_else(PoisonError::into_inner) = restored.item_ordinal;
            Ok(StreamOpenOutcome::Restored)
        } else {
            *self.ordinal.lock().unwrap_or_else(PoisonError::into_inner) = 0;
            Ok(StreamOpenOutcome::Initial)
        }
    }

    async fn update(
        &self,
        _context: StreamUpdateContext<'_>,
    ) -> Result<crate::ComponentStateEnvelope, StreamUpdateError> {
        let codec = object_read_position_codec();
        let item_ordinal = *self.ordinal.lock().unwrap_or_else(PoisonError::into_inner);
        let version_token = self
            .fetched_version
            .as_ref()
            .map(|token| token.as_str().to_owned());
        crate::ComponentStateEnvelope::encode(
            self.namespace.clone(),
            &ObjectReadPosition {
                item_ordinal,
                version_token,
            },
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

/// Bridges an [`ObjectStoreCapability`] into
/// [`MultiResourceReaderOpener`], parsing each whole fetched object with a
/// caller-supplied function.
///
/// # Version-token enforcement
///
/// Every call to [`open`](MultiResourceReaderOpener::open) fetches the
/// object fresh and records its version token (when the backend supplies
/// one) into the delegate [`ObjectItemReaderStream`]. On restart,
/// [`ObjectItemReaderStream::open`] compares that freshly fetched token
/// against the token recorded in the last *committed* checkpoint: a
/// mismatch (the object was replaced, or its version token is simply
/// absent because the backend cannot supply one) fails closed
/// ([`FailureCategory::UnsupportedCapability`]) rather than resuming an
/// item ordinal against different content.
pub struct ObjectStoreReaderOpener<C, I, F> {
    store: C,
    max_object_bytes: usize,
    parse: F,
    _marker: std::marker::PhantomData<fn() -> I>,
}

impl<C, I, F> ObjectStoreReaderOpener<C, I, F>
where
    C: ObjectStoreCapability,
    F: Fn(&[u8]) -> Result<Vec<I>, ObjectStoreError> + Send + Sync,
{
    /// Builds an opener over `store`, parsing each fetched object's bytes
    /// with `parse`; a fetched object larger than `max_object_bytes` is
    /// rejected rather than buffered.
    #[must_use]
    pub const fn new(store: C, max_object_bytes: usize, parse: F) -> Self {
        Self {
            store,
            max_object_bytes,
            parse,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<C, I, F> MultiResourceReaderOpener<I> for ObjectStoreReaderOpener<C, I, F>
where
    C: ObjectStoreCapability + Send + Sync,
    I: Clone + Send + 'static,
    F: Fn(&[u8]) -> Result<Vec<I>, ObjectStoreError> + Send + Sync,
{
    type Reader = ObjectItemReader<I>;
    type Stream = ObjectItemReaderStream;

    async fn open(
        &self,
        resource: &ResourceIdentity,
        resource_ordinal: u32,
        delegate_identity: &ComponentStreamIdentity,
    ) -> Result<(Self::Reader, Self::Stream, StreamStateContract), MultiResourceOpenError> {
        let id = ObjectIdentity::new(resource.as_str())
            .map_err(|_| MultiResourceOpenError::new(resource_ordinal))?;
        // `max_bytes` is enforced by the backend before the whole object is
        // delivered -- see `ObjectStoreCapability::get`'s contract -- so
        // there is no post-fetch length check left to perform here.
        let (bytes, metadata) =
            self.store
                .get(&id, self.max_object_bytes)
                .await
                .map_err(|error| {
                    MultiResourceOpenError::with_category(resource_ordinal, error.category())
                })?;
        let items = (self.parse)(&bytes).map_err(|error| {
            MultiResourceOpenError::with_category(resource_ordinal, error.category())
        })?;
        let ordinal = Arc::new(Mutex::new(0));
        let reader = ObjectItemReader {
            items,
            ordinal: Arc::clone(&ordinal),
        };
        let stream = ObjectItemReaderStream {
            ordinal,
            fetched_version: metadata.version_token().cloned(),
            namespace: delegate_identity.clone(),
        };
        let contract = StreamStateContract::new(object_read_position_codec());
        Ok((reader, stream, contract))
    }
}

// ---------------------------------------------------------------------
// Writer bridge into the multi-resource model
// ---------------------------------------------------------------------

#[derive(Clone)]
struct ObjectWritePosition {
    committed_item_count: u64,
    version_token: Option<String>,
}

const OBJECT_WRITE_SCHEMA: &str = "oxide-batch.object-store-write-position";
const OBJECT_WRITE_CODEC: &str = "oxide-batch.object-store-write-position-codec";

#[derive(Clone, Copy)]
struct ObjectWritePositionSchema;

impl VersionedStateCodec<ObjectWritePosition> for ObjectWritePositionSchema {
    fn schema_id(&self) -> &StateSchemaId {
        static SCHEMA: std::sync::OnceLock<StateSchemaId> = std::sync::OnceLock::new();
        #[allow(
            clippy::unwrap_used,
            reason = "fixed literal schema identity cannot fail validation"
        )]
        SCHEMA.get_or_init(|| StateSchemaId::new(OBJECT_WRITE_SCHEMA).unwrap())
    }

    fn current_version(&self) -> StateSchemaVersion {
        #[allow(
            clippy::unwrap_used,
            reason = "fixed literal schema version cannot fail validation"
        )]
        StateSchemaVersion::new(1).unwrap()
    }

    fn encode(&self, value: &ObjectWritePosition) -> Result<Vec<u8>, StateCodecError> {
        serde_json::to_vec(&serde_json::json!({
            "committed_item_count": value.committed_item_count,
            "version_token": value.version_token,
        }))
        .map_err(|_| StateCodecError::InvalidPayload)
    }

    fn decode(&self, payload: &[u8]) -> Result<ObjectWritePosition, StateCodecError> {
        let value: serde_json::Value =
            serde_json::from_slice(payload).map_err(|_| StateCodecError::InvalidPayload)?;
        let committed_item_count = value
            .get("committed_item_count")
            .and_then(serde_json::Value::as_u64)
            .ok_or(StateCodecError::InvalidPayload)?;
        let version_token = value
            .get("version_token")
            .and_then(|v| {
                if v.is_null() {
                    Some(None)
                } else {
                    v.as_str().map(|s| Some(s.to_owned()))
                }
            })
            .ok_or(StateCodecError::InvalidPayload)?;
        Ok(ObjectWritePosition {
            committed_item_count,
            version_token,
        })
    }
}

#[allow(
    clippy::unwrap_used,
    reason = "fixed literal identities cannot fail validation"
)]
fn object_write_position_codec() -> DefaultComponentCodec<ObjectWritePositionSchema> {
    DefaultComponentCodec::new(
        ObjectWritePositionSchema,
        crate::CodecId::new(OBJECT_WRITE_CODEC).unwrap(),
        crate::CodecVersion::new(1).unwrap(),
        RestartabilityDeclaration::Restartable,
    )
    .with_sensitivity(StateSensitivity::NonSensitive)
}

struct WriteAccumulator {
    bytes: Vec<u8>,
    committed_item_count: u64,
    version_token: Option<String>,
}

/// A [`std::io::Write`] sink over the accumulator's candidate buffer that
/// refuses to grow past a declared maximum.
///
/// This is what makes `max_object_bytes` a real, pre-materialization bound
/// on the wrapper's *own* accumulation rather than a post-hoc length check:
/// every `write` call is checked against the remaining budget *before* any
/// byte is copied into the candidate, so a caller's `serialize`
/// implementation that writes incrementally (as any bounded-output encoder
/// should) is stopped at the exact byte that would exceed the bound,
/// never after allocating proportional to however far past it that byte
/// would have landed.
///
/// This cannot bound what a `serialize` implementation allocates *before*
/// handing bytes to this sink (e.g. one internal `format!` call that
/// builds a single huge `String` and then calls [`std::io::Write::write_all`]
/// once) -- no fixed sink-based interface can, since `serialize` is
/// arbitrary caller code that runs before this sink ever sees a byte. That
/// residual, inherent limit is documented on [`ObjectStoreWriterOpener::new`]
/// rather than hidden.
///
/// `overflowed` is sticky and checked by [`ObjectItemWriter::write`]
/// regardless of what `serialize` itself returns: a `serialize`
/// implementation that ignores this sink's `Err` and returns `Ok(())`
/// anyway must never be able to turn a resource-limit rejection into a
/// partial item silently reaching the backend.
struct BoundedSink<'a> {
    buf: &'a mut Vec<u8>,
    max: usize,
    overflowed: bool,
}

impl std::io::Write for BoundedSink<'_> {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        let Some(new_len) = self.buf.len().checked_add(data.len()) else {
            self.overflowed = true;
            return Err(std::io::Error::other("object size overflow"));
        };
        if new_len > self.max {
            self.overflowed = true;
            return Err(std::io::Error::other(
                "object write would exceed max_object_bytes",
            ));
        }
        self.buf.extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Truncates the accumulator's candidate buffer back to its pre-write
/// length on drop, unless disarmed -- including when the drop happens
/// because the current scope is unwinding from a panic inside the
/// caller-supplied `serialize` closure (or, defensively, anywhere else in
/// [`ObjectItemWriter::write`] before the candidate is committed).
/// Without this, a panicking `serialize` call would unwind straight through
/// the growing candidate, and because the accumulator's prior content was
/// moved into that candidate (never cloned, by design -- see this module's
/// allocation docs) rather than merely borrowed, the panic would silently
/// discard every item this writer had previously, successfully committed
/// to the object, not just the one item being serialized when it panicked.
struct TruncateOnDrop<'a> {
    target: &'a mut Vec<u8>,
    original_len: usize,
    armed: bool,
}

impl Drop for TruncateOnDrop<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.target.truncate(self.original_len);
        }
    }
}

/// A restartable [`crate::ItemWriter`] that accumulates serialized items in
/// memory and re-issues the whole object on every write (object-store `PUT`
/// is whole-object, not append).
///
/// # Known limitation (documented, not a defect)
///
/// A crash between a successful `put` and the runtime's own durable commit
/// leaves the object reflecting content whose corresponding chunk never
/// committed. This is the "duplicate and unknown outcomes are expected"
/// case the integration model requires adapters to model rather than hide:
/// on restart, [`ObjectItemWriterStream::open`] refetches the object and
/// compares its version token against the *last committed* checkpoint's
/// recorded token; an uncommitted `put`'s version was never recorded there,
/// so it never matches, and the restart fails closed
/// ([`FailureCategory::UnsupportedCapability`]) rather than silently
/// accepting or silently discarding the uncommitted write. Recovering from
/// that state is an operational decision (replace or accept the object
/// manually), not something this writer resolves automatically.
pub struct ObjectItemWriter<O, C, S> {
    id: ObjectIdentity,
    store: Arc<C>,
    serialize: Arc<S>,
    accumulator: Arc<AsyncMutex<WriteAccumulator>>,
    max_object_bytes: usize,
    _marker: std::marker::PhantomData<fn(O)>,
}

impl<O, C, S> ItemWriter<O> for ObjectItemWriter<O, C, S>
where
    O: Sync,
    C: ObjectStoreCapability,
    S: Fn(&O, &mut dyn std::io::Write) -> std::io::Result<()> + Send + Sync,
{
    async fn write<'a>(
        &'a self,
        items: &'a [O],
        context: WriteContext<'a>,
    ) -> Result<WriteOutcome, WriterError> {
        if context.stop_token().is_stop_requested() {
            return Ok(WriteOutcome::Stopped);
        }
        let mut accumulator = self.accumulator.lock().await;
        let original_len = accumulator.bytes.len();
        // Extend the accumulator's buffer in place -- never a second, full
        // copy of the accumulated object for one write, unlike cloning it
        // once to build a candidate and again to hand it to `put`. Guarded
        // by `TruncateOnDrop`, armed for the whole growth phase: any early
        // return (a rejected or failed item) *or* a panic unwinding out of
        // the caller-supplied `serialize` call truncates the buffer back to
        // its pre-write length instead of leaving a partial item appended
        // or, worse, an emptied accumulator (see `TruncateOnDrop`'s docs).
        //
        // The bound is a real, pre-materialization bound on *this
        // wrapper's own* accumulation: `serialize` writes into a
        // `BoundedSink` over the buffer, and every write to that sink is
        // checked against the remaining budget before a single byte is
        // copied in -- never after allocating proportional to an oversized
        // write. See `BoundedSink`'s docs for the one residual limit this
        // cannot cover (what `serialize` allocates internally before it
        // ever calls the sink).
        let mut guard = TruncateOnDrop {
            target: &mut accumulator.bytes,
            original_len,
            armed: true,
        };
        for item in items {
            let mut sink = BoundedSink {
                buf: &mut *guard.target,
                max: self.max_object_bytes,
                overflowed: false,
            };
            let result = (self.serialize)(item, &mut sink);
            // Checked unconditionally, regardless of what `serialize`
            // itself returned: a `serialize` that ignores the sink's `Err`
            // and returns `Ok(())` anyway must not turn a resource-limit
            // rejection into a partial item reaching the backend.
            if sink.overflowed {
                return Err(WriterError::with_category(FailureCategory::Invariant));
            }
            if result.is_err() {
                // The serializer's own failure, distinct from the sink
                // refusing a write: `FailureCategory::Invariant` is not
                // policy-eligible for retry/skip, but a delegate/user
                // component failure is, and this is exactly that -- the
                // wrapper's own bound was never in question here.
                return Err(WriterError::new());
            }
        }
        match self.store.put(&self.id, guard.target.as_slice()).await {
            Ok(metadata) => {
                guard.armed = false;
                drop(guard);
                accumulator.committed_item_count += u64::try_from(items.len()).unwrap_or(u64::MAX);
                accumulator.version_token = metadata
                    .version_token()
                    .map(|token| token.as_str().to_owned());
                Ok(WriteOutcome::Written)
            }
            Err(error) => Err(WriterError::with_category(error.category())),
        }
    }
}

/// The [`crate::ItemStream`] half of an [`ObjectItemWriter`].
pub struct ObjectItemWriterStream<C> {
    id: ObjectIdentity,
    store: Arc<C>,
    accumulator: Arc<AsyncMutex<WriteAccumulator>>,
    namespace: ComponentStreamIdentity,
    max_object_bytes: usize,
}

impl<C: ObjectStoreCapability> ItemStream for ObjectItemWriterStream<C> {
    async fn open(
        &self,
        context: StreamOpenContext<'_>,
    ) -> Result<StreamOpenOutcome, StreamOpenError> {
        let codec = object_write_position_codec();
        if let Some(envelope) = context.inherited_state() {
            let restored = envelope
                .decode::<ObjectWritePosition>(&codec)
                .map_err(|_| StreamOpenError::new())?;
            let (bytes, metadata) = self
                .store
                .get(&self.id, self.max_object_bytes)
                .await
                .map_err(|error| StreamOpenError::with_category(error.category()))?;
            let current_token = metadata
                .version_token()
                .map(|token| token.as_str().to_owned());
            let proven_identical = matches!(
                (&current_token, &restored.version_token),
                (Some(now), Some(then)) if now == then
            );
            if !proven_identical {
                // The object is not proven identical to what this stream
                // last committed against -- replaced, a crash left an
                // uncommitted `put` in place, or the backend cannot supply a
                // stable version token at all (both sides `None`, which is
                // not proof of anything and must not be read as a match).
                // Fail closed rather than silently resuming against content
                // that was never verified.
                return Err(StreamOpenError::with_category(
                    FailureCategory::UnsupportedCapability,
                ));
            }
            let mut accumulator = self.accumulator.lock().await;
            accumulator.bytes = bytes;
            accumulator.committed_item_count = restored.committed_item_count;
            accumulator.version_token = current_token;
            Ok(StreamOpenOutcome::Restored)
        } else {
            let mut accumulator = self.accumulator.lock().await;
            accumulator.bytes.clear();
            accumulator.committed_item_count = 0;
            accumulator.version_token = None;
            Ok(StreamOpenOutcome::Initial)
        }
    }

    async fn update(
        &self,
        _context: StreamUpdateContext<'_>,
    ) -> Result<crate::ComponentStateEnvelope, StreamUpdateError> {
        let codec = object_write_position_codec();
        let accumulator = self.accumulator.lock().await;
        let state = ObjectWritePosition {
            committed_item_count: accumulator.committed_item_count,
            version_token: accumulator.version_token.clone(),
        };
        drop(accumulator);
        crate::ComponentStateEnvelope::encode(
            self.namespace.clone(),
            &state,
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

/// Bridges an [`ObjectStoreCapability`] into [`MultiResourceWriterOpener`].
pub struct ObjectStoreWriterOpener<C, S> {
    store: Arc<C>,
    serialize: Arc<S>,
    max_object_bytes: usize,
}

impl<C, S> ObjectStoreWriterOpener<C, S>
where
    C: ObjectStoreCapability + 'static,
{
    /// Builds an opener over `store`, serializing each item by calling
    /// `serialize` with a sink to write its encoded bytes into, before
    /// appending it to the current object's accumulator.
    ///
    /// `serialize` writes to the sink rather than returning an owned
    /// buffer specifically so `max_object_bytes` can be a real,
    /// pre-materialization bound: every write to the sink is checked
    /// against the remaining budget *before* copying a single byte into
    /// the accumulator (see the private `BoundedSink` sink type's docs for
    /// the one residual limit this cannot cover -- an item whose own `serialize`
    /// implementation allocates a large buffer internally before ever
    /// calling the sink). A well-behaved `serialize` writes incrementally
    /// (e.g. via `write!`, or by writing one already-small field at a
    /// time) rather than building one large owned buffer first.
    #[must_use]
    pub fn new(store: C, serialize: S, max_object_bytes: usize) -> Self {
        Self {
            store: Arc::new(store),
            serialize: Arc::new(serialize),
            max_object_bytes,
        }
    }
}

impl<O, C, S> MultiResourceWriterOpener<O> for ObjectStoreWriterOpener<C, S>
where
    O: Send + Sync + 'static,
    C: ObjectStoreCapability + 'static,
    S: Fn(&O, &mut dyn std::io::Write) -> std::io::Result<()> + Send + Sync + 'static,
{
    type Writer = ObjectItemWriter<O, C, S>;
    type Stream = ObjectItemWriterStream<C>;

    async fn open(
        &self,
        resource: &ResourceIdentity,
        resource_ordinal: u32,
        delegate_identity: &ComponentStreamIdentity,
    ) -> Result<(Self::Writer, Self::Stream, StreamStateContract), MultiResourceOpenError> {
        let id = ObjectIdentity::new(resource.as_str())
            .map_err(|_| MultiResourceOpenError::new(resource_ordinal))?;
        let accumulator = Arc::new(AsyncMutex::new(WriteAccumulator {
            bytes: Vec::new(),
            committed_item_count: 0,
            version_token: None,
        }));
        let writer = ObjectItemWriter {
            id: id.clone(),
            store: Arc::clone(&self.store),
            serialize: Arc::clone(&self.serialize),
            accumulator: Arc::clone(&accumulator),
            max_object_bytes: self.max_object_bytes,
            _marker: std::marker::PhantomData,
        };
        let stream = ObjectItemWriterStream {
            id,
            store: Arc::clone(&self.store),
            accumulator,
            namespace: delegate_identity.clone(),
            max_object_bytes: self.max_object_bytes,
        };
        let contract = StreamStateContract::new(object_write_position_codec());
        Ok((writer, stream, contract))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::item_components::multi_resource::{
        ResourceSet, multi_resource_reader, multi_resource_writer,
    };
    use crate::{ReadContext, ReadOutcome, StopSource};

    fn id(key: &str) -> ObjectIdentity {
        ObjectIdentity::new(key).unwrap()
    }

    #[test]
    fn object_identity_rejects_one_byte_past_its_ceiling() {
        assert!(ObjectIdentity::new("x".repeat(MAX_OBJECT_KEY_BYTES)).is_ok());
        let error = ObjectIdentity::new("x".repeat(MAX_OBJECT_KEY_BYTES + 1))
            .expect_err("one byte past the ceiling must be refused, not silently truncated");
        assert_eq!(
            error,
            ObjectStoreConfigError::KeyTooLong {
                max_bytes: MAX_OBJECT_KEY_BYTES
            }
        );
    }

    #[test]
    fn object_identity_rejects_empty_and_control_characters() {
        assert_eq!(
            ObjectIdentity::new(""),
            Err(ObjectStoreConfigError::EmptyKey)
        );
        assert_eq!(
            ObjectIdentity::new("a\nb"),
            Err(ObjectStoreConfigError::MalformedKey)
        );
    }

    #[test]
    fn list_pagination_is_deterministic_and_orders_by_key() {
        let store = InMemoryObjectStore::new(1024);
        futures_executor::block_on(async {
            for key in ["b", "a", "c", "d"] {
                store.put(&id(key), key.as_bytes()).await.unwrap();
            }
            let page1 = store.list("", 2, None).await.unwrap();
            let keys1: Vec<_> = page1
                .entries()
                .iter()
                .map(|(k, _)| k.as_str().to_owned())
                .collect();
            assert_eq!(
                keys1,
                vec!["a", "b"],
                "listing must be key-ordered, not insertion-ordered"
            );
            let continuation = page1.continuation().expect("more entries remain");
            let page2 = store.list("", 2, Some(continuation)).await.unwrap();
            let keys2: Vec<_> = page2
                .entries()
                .iter()
                .map(|(k, _)| k.as_str().to_owned())
                .collect();
            assert_eq!(keys2, vec!["c", "d"]);
            assert!(
                page2.continuation().is_none(),
                "final page must not continue"
            );
        });
    }

    #[test]
    fn missing_object_get_and_stat_fail_with_unsupported_capability() {
        let store = InMemoryObjectStore::new(1024);
        futures_executor::block_on(async {
            let error = store.get(&id("missing"), 1024).await.unwrap_err();
            assert_eq!(error.category(), FailureCategory::UnsupportedCapability);
            let error = store.stat(&id("missing")).await.unwrap_err();
            assert_eq!(error.category(), FailureCategory::UnsupportedCapability);
        });
    }

    #[test]
    fn put_bounded_by_max_object_bytes() {
        let store = InMemoryObjectStore::new(4);
        futures_executor::block_on(async {
            let result = store.put(&id("x"), &[0u8; 5]).await;
            assert!(
                result.is_err(),
                "an oversized put must be rejected, not silently truncated"
            );
            store.put(&id("x"), &[0u8; 4]).await.unwrap();
        });
    }

    #[test]
    fn get_bounded_by_caller_supplied_max_bytes_without_materializing_the_oversized_object() {
        // The backend already holds the object's bytes regardless (this is
        // an in-memory fixture), so this test proves the *wrapper-visible*
        // guarantee: a `get` whose `max_bytes` is smaller than the stored
        // object is rejected, and the caller never receives (nor is a copy
        // made of) content beyond the declared bound. See
        // `reader_opener_never_receives_more_than_max_object_bytes` below
        // for a backend-side proof that no oversized copy is ever made at
        // all, not even internally.
        let store = InMemoryObjectStore::new(1024);
        futures_executor::block_on(async {
            store.put(&id("x"), &[0u8; 100]).await.unwrap();
            let error = store.get(&id("x"), 99).await.unwrap_err();
            assert_eq!(error.category(), FailureCategory::Invariant);
            let (bytes, _metadata) = store.get(&id("x"), 100).await.unwrap();
            assert_eq!(bytes.len(), 100, "exactly-at-bound must still succeed");
        });
    }

    #[test]
    fn put_get_roundtrip_returns_incrementing_version_tokens() {
        let store = InMemoryObjectStore::new(1024);
        futures_executor::block_on(async {
            let first = store.put(&id("x"), b"one").await.unwrap();
            let second = store.put(&id("x"), b"two").await.unwrap();
            assert_ne!(
                first.version_token(),
                second.version_token(),
                "a replaced object must publish a different version token"
            );
            let (bytes, metadata) = store.get(&id("x"), 1024).await.unwrap();
            assert_eq!(bytes, b"two");
            assert_eq!(metadata.version_token(), second.version_token());
        });
    }

    fn stop() -> (StopSource, crate::StopToken) {
        StopSource::new()
    }

    #[allow(
        clippy::unnecessary_wraps,
        reason = "matches the real `parse: F` signature `ObjectStoreReaderOpener` requires"
    )]
    fn csv_parse(bytes: &[u8]) -> Result<Vec<u64>, ObjectStoreError> {
        Ok(std::str::from_utf8(bytes)
            .unwrap()
            .split(',')
            .map(|s| s.parse::<u64>().unwrap())
            .collect())
    }

    #[test]
    fn reader_restart_resumes_ordinal_when_object_version_unchanged() {
        let store = Arc::new(InMemoryObjectStore::new(1024));
        let (_source, token) = stop();
        futures_executor::block_on(async {
            store.put(&id("obj"), b"1,2,3").await.unwrap();
        });
        let resources = ResourceSet::new(vec![
            crate::item_components::multi_resource::ResourceIdentity::new("obj").unwrap(),
        ]);
        let identity =
            ComponentStreamIdentity::new("oxide-batch.object-store-unit-test.reader").unwrap();

        let committed = futures_executor::block_on(async {
            let opener = ObjectStoreReaderOpener::new(Arc::clone(&store), 1024, csv_parse);
            let (mut reader, stream, _contract) = multi_resource_reader::<u64, _>(
                resources.clone(),
                opener,
                identity.clone(),
                RestartabilityDeclaration::Restartable,
            );
            stream
                .open(StreamOpenContext::new(None, &token))
                .await
                .unwrap();
            assert_eq!(
                reader.read(ReadContext::new(&token)).await.unwrap(),
                ReadOutcome::Item(1)
            );
            stream
                .update(StreamUpdateContext::new(&token))
                .await
                .unwrap()
        });

        futures_executor::block_on(async {
            let opener = ObjectStoreReaderOpener::new(Arc::clone(&store), 1024, csv_parse);
            let (mut reader, stream, _contract) = multi_resource_reader::<u64, _>(
                resources,
                opener,
                identity,
                RestartabilityDeclaration::Restartable,
            );
            stream
                .open(StreamOpenContext::new(Some(&committed), &token))
                .await
                .unwrap();
            assert_eq!(
                reader.read(ReadContext::new(&token)).await.unwrap(),
                ReadOutcome::Item(2),
                "must resume at ordinal 1 (item `2`), not replay item `1`"
            );
        });
    }

    #[test]
    fn reader_restart_rejects_replaced_object() {
        let store = Arc::new(InMemoryObjectStore::new(1024));
        let (_source, token) = stop();
        futures_executor::block_on(async {
            store.put(&id("obj"), b"1,2,3").await.unwrap();
        });
        let resources = ResourceSet::new(vec![
            crate::item_components::multi_resource::ResourceIdentity::new("obj").unwrap(),
        ]);
        let identity =
            ComponentStreamIdentity::new("oxide-batch.object-store-unit-test.replaced").unwrap();

        let committed = futures_executor::block_on(async {
            let opener = ObjectStoreReaderOpener::new(Arc::clone(&store), 1024, csv_parse);
            let (mut reader, stream, _contract) = multi_resource_reader::<u64, _>(
                resources.clone(),
                opener,
                identity.clone(),
                RestartabilityDeclaration::Restartable,
            );
            stream
                .open(StreamOpenContext::new(None, &token))
                .await
                .unwrap();
            reader.read(ReadContext::new(&token)).await.unwrap();
            stream
                .update(StreamUpdateContext::new(&token))
                .await
                .unwrap()
        });

        // The object is replaced (new content, new version token) before
        // the restart -- this must be rejected, not silently resumed
        // against the new content at the old ordinal.
        futures_executor::block_on(async {
            store.put(&id("obj"), b"9,9,9").await.unwrap();
        });
        futures_executor::block_on(async {
            let opener = ObjectStoreReaderOpener::new(Arc::clone(&store), 1024, csv_parse);
            let (_reader, stream, _contract) = multi_resource_reader::<u64, _>(
                resources,
                opener,
                identity,
                RestartabilityDeclaration::Restartable,
            );
            let result = stream
                .open(StreamOpenContext::new(Some(&committed), &token))
                .await;
            assert!(
                result.is_err(),
                "a replaced object must fail closed on restart"
            );
        });
    }

    /// Wraps [`InMemoryObjectStore`] but reports no version identity, the
    /// same as a real backend that cannot supply one
    /// (see [`ObjectVersionToken`]'s own doc). Restart-safety tests use this
    /// to prove the stream fails closed even when the backend itself
    /// offers no proof of content identity, rather than only exercising the
    /// token-present comparison.
    struct NoVersionObjectStore {
        inner: InMemoryObjectStore,
    }

    impl NoVersionObjectStore {
        fn new(max_object_bytes: usize) -> Self {
            Self {
                inner: InMemoryObjectStore::new(max_object_bytes),
            }
        }
    }

    impl ObjectStoreCapability for NoVersionObjectStore {
        async fn get(
            &self,
            id: &ObjectIdentity,
            max_bytes: usize,
        ) -> Result<(Vec<u8>, ObjectMetadata), ObjectStoreError> {
            let (bytes, metadata) = self.inner.get(id, max_bytes).await?;
            Ok((bytes, ObjectMetadata::new(metadata.size(), None)))
        }

        async fn put(
            &self,
            id: &ObjectIdentity,
            bytes: &[u8],
        ) -> Result<ObjectMetadata, ObjectStoreError> {
            let metadata = self.inner.put(id, bytes).await?;
            Ok(ObjectMetadata::new(metadata.size(), None))
        }

        async fn stat(&self, id: &ObjectIdentity) -> Result<ObjectMetadata, ObjectStoreError> {
            let metadata = self.inner.stat(id).await?;
            Ok(ObjectMetadata::new(metadata.size(), None))
        }

        async fn list(
            &self,
            prefix: &str,
            page_size: usize,
            continuation: Option<&ObjectListContinuation>,
        ) -> Result<ObjectListPage, ObjectStoreError> {
            self.inner.list(prefix, page_size, continuation).await
        }
    }

    #[test]
    fn reader_restart_over_a_no_version_backend_fails_closed_rather_than_matching_none_to_none() {
        // Before this fix, `restored.version_token != fetched` read `None !=
        // None` as `false` (a match), so a restart over a backend with no
        // stable version identity silently resumed the ordinal with zero
        // proof the content was the same. The stream must fail closed
        // instead -- "no version identity" is not "identical content".
        let store = Arc::new(NoVersionObjectStore::new(1024));
        let (_source, token) = stop();
        futures_executor::block_on(async {
            store.put(&id("obj"), b"1,2,3").await.unwrap();
        });
        let resources = ResourceSet::new(vec![
            crate::item_components::multi_resource::ResourceIdentity::new("obj").unwrap(),
        ]);
        let identity =
            ComponentStreamIdentity::new("oxide-batch.object-store-unit-test.no-version-reader")
                .unwrap();

        let committed = futures_executor::block_on(async {
            let opener = ObjectStoreReaderOpener::new(Arc::clone(&store), 1024, csv_parse);
            let (mut reader, stream, _contract) = multi_resource_reader::<u64, _>(
                resources.clone(),
                opener,
                identity.clone(),
                RestartabilityDeclaration::Restartable,
            );
            stream
                .open(StreamOpenContext::new(None, &token))
                .await
                .unwrap();
            reader.read(ReadContext::new(&token)).await.unwrap();
            stream
                .update(StreamUpdateContext::new(&token))
                .await
                .unwrap()
        });

        futures_executor::block_on(async {
            let opener = ObjectStoreReaderOpener::new(Arc::clone(&store), 1024, csv_parse);
            let (_reader, stream, _contract) = multi_resource_reader::<u64, _>(
                resources,
                opener,
                identity,
                RestartabilityDeclaration::Restartable,
            );
            let result = stream
                .open(StreamOpenContext::new(Some(&committed), &token))
                .await;
            assert!(
                result.is_err(),
                "a restart over a backend with no version identity must fail \
                 closed, not silently treat 'no proof either time' as a match"
            );
        });
    }

    #[test]
    fn writer_restart_over_a_no_version_backend_fails_closed_rather_than_matching_none_to_none() {
        let store = Arc::new(NoVersionObjectStore::new(1024));
        let (_source, token) = stop();
        let serialize = |item: &u64, sink: &mut dyn std::io::Write| {
            sink.write_all(format!("{item},").as_bytes())
        };
        let resources = ResourceSet::new(vec![
            crate::item_components::multi_resource::ResourceIdentity::new("obj").unwrap(),
        ]);
        let identity =
            ComponentStreamIdentity::new("oxide-batch.object-store-unit-test.no-version-writer")
                .unwrap();

        let committed = futures_executor::block_on(async {
            let opener = ObjectStoreWriterOpener::new(Arc::clone(&store), serialize, 1024);
            let (writer, stream, _contract) = multi_resource_writer::<u64, _, _>(
                resources.clone(),
                opener,
                identity.clone(),
                crate::item_components::multi_resource::NoRollover,
                RestartabilityDeclaration::Restartable,
            )
            .unwrap();
            stream
                .open(StreamOpenContext::new(None, &token))
                .await
                .unwrap();
            writer
                .write(&[1], WriteContext::non_transactional(&token))
                .await
                .unwrap();
            stream
                .update(StreamUpdateContext::new(&token))
                .await
                .unwrap()
        });

        futures_executor::block_on(async {
            let opener = ObjectStoreWriterOpener::new(Arc::clone(&store), serialize, 1024);
            let (_writer, stream, _contract) = multi_resource_writer::<u64, _, _>(
                resources,
                opener,
                identity,
                crate::item_components::multi_resource::NoRollover,
                RestartabilityDeclaration::Restartable,
            )
            .unwrap();
            let result = stream
                .open(StreamOpenContext::new(Some(&committed), &token))
                .await;
            assert!(
                result.is_err(),
                "a writer restart over a backend with no version identity must \
                 fail closed, not silently resume against unverified content"
            );
        });
    }

    #[test]
    fn writer_roundtrip_through_multi_resource_writer_accumulates_and_puts() {
        let store = Arc::new(InMemoryObjectStore::new(1024));
        let (_source, token) = stop();
        let serialize = |item: &u64, sink: &mut dyn std::io::Write| {
            sink.write_all(format!("{item},").as_bytes())
        };
        let opener = ObjectStoreWriterOpener::new(Arc::clone(&store), serialize, 1024);
        let resources = ResourceSet::new(vec![
            crate::item_components::multi_resource::ResourceIdentity::new("out").unwrap(),
        ]);
        let identity =
            ComponentStreamIdentity::new("oxide-batch.object-store-unit-test.writer").unwrap();
        futures_executor::block_on(async {
            let (writer, stream, _contract) = multi_resource_writer::<u64, _, _>(
                resources,
                opener,
                identity,
                crate::item_components::multi_resource::NoRollover,
                RestartabilityDeclaration::Restartable,
            )
            .unwrap();
            stream
                .open(StreamOpenContext::new(None, &token))
                .await
                .unwrap();
            writer
                .write(&[1, 2], WriteContext::non_transactional(&token))
                .await
                .unwrap();
            writer
                .write(&[3], WriteContext::non_transactional(&token))
                .await
                .unwrap();
        });
        let (bytes, _metadata) = futures_executor::block_on(store.get(&id("out"), 1024)).unwrap();
        assert_eq!(bytes, b"1,2,3,".to_vec());
    }

    // -- Finding 2 (#177) regression evidence: real resource bounds, not
    // post-materialization validation.

    /// A backend that never actually stores content, only a *declared*
    /// length, and can only ever materialize a `Vec<u8>` for a `get` within
    /// the caller's `max_bytes`. Proves the reader bridge's bound is
    /// enforced *before* delivery -- there is no way for this fixture to
    /// hand back an over-`max_bytes` buffer at all, so a passing test
    /// structurally cannot be explained by "materialize first, reject
    /// after".
    struct DeclaredSizeObjectStore {
        declared_len: Mutex<BTreeMap<String, usize>>,
        materialize_calls: std::sync::atomic::AtomicUsize,
    }

    impl DeclaredSizeObjectStore {
        fn new() -> Self {
            Self {
                declared_len: Mutex::new(BTreeMap::new()),
                materialize_calls: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn declare(&self, key: &str, len: usize) {
            self.declared_len
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(key.to_owned(), len);
        }
    }

    impl ObjectStoreCapability for DeclaredSizeObjectStore {
        async fn get(
            &self,
            id: &ObjectIdentity,
            max_bytes: usize,
        ) -> Result<(Vec<u8>, ObjectMetadata), ObjectStoreError> {
            let len = *self
                .declared_len
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get(id.as_str())
                .ok_or_else(|| {
                    ObjectStoreError::with_category(FailureCategory::UnsupportedCapability)
                })?;
            if len > max_bytes {
                return Err(ObjectStoreError::with_category(FailureCategory::Invariant));
            }
            self.materialize_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let metadata = ObjectMetadata::new(u64::try_from(len).unwrap_or(u64::MAX), None);
            Ok((vec![0u8; len], metadata))
        }

        async fn put(
            &self,
            id: &ObjectIdentity,
            bytes: &[u8],
        ) -> Result<ObjectMetadata, ObjectStoreError> {
            self.declare(id.as_str(), bytes.len());
            Ok(ObjectMetadata::new(
                u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                None,
            ))
        }

        async fn stat(&self, id: &ObjectIdentity) -> Result<ObjectMetadata, ObjectStoreError> {
            let len = *self
                .declared_len
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get(id.as_str())
                .ok_or_else(|| {
                    ObjectStoreError::with_category(FailureCategory::UnsupportedCapability)
                })?;
            Ok(ObjectMetadata::new(
                u64::try_from(len).unwrap_or(u64::MAX),
                None,
            ))
        }

        async fn list(
            &self,
            _prefix: &str,
            _page_size: usize,
            _continuation: Option<&ObjectListContinuation>,
        ) -> Result<ObjectListPage, ObjectStoreError> {
            Ok(ObjectListPage {
                entries: Vec::new(),
                continuation: None,
            })
        }
    }

    #[test]
    fn reader_opener_rejects_an_oversized_object_without_ever_materializing_it() {
        let store = Arc::new(DeclaredSizeObjectStore::new());
        // Ten gigabytes, declared only -- never actually allocated anywhere
        // by this fixture or by the code under test.
        store.declare("huge", 10_000_000_000);
        let resource = ResourceIdentity::new("huge").unwrap();
        let identity =
            ComponentStreamIdentity::new("oxide-batch.object-store-unit-test.bounded-reader")
                .unwrap();
        let opener = ObjectStoreReaderOpener::new(Arc::clone(&store), 1024, csv_parse);
        let result = futures_executor::block_on(MultiResourceReaderOpener::<u64>::open(
            &opener, &resource, 0, &identity,
        ));
        assert!(
            result.is_err(),
            "an object declared larger than max_object_bytes must be rejected"
        );
        assert_eq!(
            store
                .materialize_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            0,
            "the backend must never construct a buffer for an over-bound \
             object at all, proving this is a real pre-materialization \
             resource bound rather than post-fetch validation"
        );
    }

    #[test]
    fn writer_rejects_growth_past_max_object_bytes_and_allows_the_exact_boundary() {
        let store = Arc::new(InMemoryObjectStore::new(1024));
        let resources = ResourceSet::new(vec![ResourceIdentity::new("out").unwrap()]);
        let identity =
            ComponentStreamIdentity::new("oxide-batch.object-store-unit-test.bounded-writer")
                .unwrap();
        // Four bytes per item; a maximum of 8 bytes allows exactly two items.
        let serialize = |item: &u8, sink: &mut dyn std::io::Write| sink.write_all(&[*item; 4]);
        let opener = ObjectStoreWriterOpener::new(Arc::clone(&store), serialize, 8);
        let (_source, token) = stop();
        futures_executor::block_on(async {
            let (writer, stream, _contract) = multi_resource_writer::<u8, _, _>(
                resources,
                opener,
                identity,
                crate::item_components::multi_resource::NoRollover,
                RestartabilityDeclaration::Restartable,
            )
            .unwrap();
            stream
                .open(StreamOpenContext::new(None, &token))
                .await
                .unwrap();
            writer
                .write(&[1], WriteContext::non_transactional(&token))
                .await
                .unwrap();
            // Exactly at the boundary (8 bytes total): must still succeed.
            writer
                .write(&[2], WriteContext::non_transactional(&token))
                .await
                .unwrap();
            // One byte past the boundary: must be rejected, and the object
            // actually stored must remain exactly what the two successful
            // writes produced -- proving the rejected write never reached
            // the backend and never corrupted the accumulator.
            let result = writer
                .write(&[3], WriteContext::non_transactional(&token))
                .await;
            assert!(result.is_err(), "growth past the bound must be rejected");
        });
        let (bytes, _metadata) = futures_executor::block_on(store.get(&id("out"), 1024)).unwrap();
        assert_eq!(
            bytes,
            vec![1, 1, 1, 1, 2, 2, 2, 2],
            "the rejected third write must not have reached the backend, \
             and the accumulator must remain exactly at its pre-rejection \
             content"
        );
    }

    #[test]
    fn writer_stops_serializing_further_items_once_one_write_call_exceeds_the_bound() {
        // Five items in one `write` call, three bytes each, a seven-byte
        // bound: items 1 and 2 fit (six bytes); item 3 alone pushes the
        // running total to nine, over the bound. Proves the wrapper checks
        // the bound incrementally, per item, rather than serializing the
        // whole batch unconditionally before ever checking it -- items 4
        // and 5 must never be handed to `serialize` at all.
        let store = Arc::new(InMemoryObjectStore::new(1024));
        let resources = ResourceSet::new(vec![ResourceIdentity::new("out").unwrap()]);
        let identity = ComponentStreamIdentity::new(
            "oxide-batch.object-store-unit-test.bounded-writer-stops-early",
        )
        .unwrap();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted_serialize = {
            let calls = Arc::clone(&calls);
            move |item: &u8, sink: &mut dyn std::io::Write| {
                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                sink.write_all(&[*item; 3])
            }
        };
        let opener = ObjectStoreWriterOpener::new(Arc::clone(&store), counted_serialize, 7);
        let (_source, token) = stop();
        futures_executor::block_on(async {
            let (writer, stream, _contract) = multi_resource_writer::<u8, _, _>(
                resources,
                opener,
                identity,
                crate::item_components::multi_resource::NoRollover,
                RestartabilityDeclaration::Restartable,
            )
            .unwrap();
            stream
                .open(StreamOpenContext::new(None, &token))
                .await
                .unwrap();
            let result = writer
                .write(&[1, 2, 3, 4, 5], WriteContext::non_transactional(&token))
                .await;
            assert!(result.is_err(), "the batch must be rejected");
        });
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "serialize must be called for items 1, 2, and the offending item \
             3, and never again after that -- items 4 and 5 must never be \
             serialized once the running total has already exceeded the bound"
        );
    }

    #[test]
    fn writer_panic_during_serialize_does_not_lose_previously_accumulated_content() {
        // `serialize` panics while writing item 2. The panic must not wipe
        // out item 1's already-accumulated content: `TruncateOnDrop` must
        // restore the accumulator to its pre-write length even though the
        // unwind happens through `mem`-taken/borrowed state, not just
        // through an explicit `Err` return.
        let store = Arc::new(InMemoryObjectStore::new(1024));
        let resources = ResourceSet::new(vec![ResourceIdentity::new("out").unwrap()]);
        let identity =
            ComponentStreamIdentity::new("oxide-batch.object-store-unit-test.panic-safety")
                .unwrap();
        let serialize = |item: &u64, sink: &mut dyn std::io::Write| {
            assert!(
                *item != 2,
                "serializer panics on item 2, deliberately, for this test"
            );
            sink.write_all(format!("{item},").as_bytes())
        };
        let opener = ObjectStoreWriterOpener::new(Arc::clone(&store), serialize, 1024);
        let (_source, token) = stop();
        futures_executor::block_on(async {
            let (writer, stream, _contract) = multi_resource_writer::<u64, _, _>(
                resources,
                opener,
                identity,
                crate::item_components::multi_resource::NoRollover,
                RestartabilityDeclaration::Restartable,
            )
            .unwrap();
            stream
                .open(StreamOpenContext::new(None, &token))
                .await
                .unwrap();
            writer
                .write(&[1], WriteContext::non_transactional(&token))
                .await
                .unwrap();

            let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                futures_executor::block_on(
                    writer.write(&[2], WriteContext::non_transactional(&token)),
                )
            }));
            assert!(panicked.is_err(), "the panic must actually propagate here");

            // Item 1's content must still be there, and a subsequent normal
            // write must succeed and append correctly -- never observe an
            // emptied or corrupted accumulator left behind by the panic.
            writer
                .write(&[3], WriteContext::non_transactional(&token))
                .await
                .unwrap();
        });
        let (bytes, _metadata) = futures_executor::block_on(store.get(&id("out"), 1024)).unwrap();
        assert_eq!(
            bytes,
            b"1,3,".to_vec(),
            "item 1 must not have been lost by the panic on item 2, and item \
             2 itself must never appear -- the panic must have unwound \
             before its bytes were ever appended"
        );
    }

    #[test]
    fn writer_serializer_failure_is_classified_as_user_component_not_invariant() {
        // A serializer failure that has nothing to do with the sink's
        // bound (e.g. the item itself is malformed) must be classified
        // as a delegate/user-component failure -- policy-eligible for
        // retry/skip -- never as `Invariant`, which is not.
        let store = Arc::new(InMemoryObjectStore::new(1024));
        let resources = ResourceSet::new(vec![ResourceIdentity::new("out").unwrap()]);
        let identity = ComponentStreamIdentity::new(
            "oxide-batch.object-store-unit-test.serializer-failure-category",
        )
        .unwrap();
        let serialize = |item: &u64, sink: &mut dyn std::io::Write| {
            if *item == 2 {
                return Err(std::io::Error::other("item 2 is deliberately malformed"));
            }
            sink.write_all(format!("{item},").as_bytes())
        };
        let opener = ObjectStoreWriterOpener::new(Arc::clone(&store), serialize, 1024);
        let (_source, token) = stop();
        let category = futures_executor::block_on(async {
            let (writer, stream, _contract) = multi_resource_writer::<u64, _, _>(
                resources,
                opener,
                identity,
                crate::item_components::multi_resource::NoRollover,
                RestartabilityDeclaration::Restartable,
            )
            .unwrap();
            stream
                .open(StreamOpenContext::new(None, &token))
                .await
                .unwrap();
            let result = writer
                .write(&[2], WriteContext::non_transactional(&token))
                .await;
            result
                .expect_err("a malformed item must fail the write")
                .category()
        });
        assert_eq!(
            category,
            FailureCategory::UserComponent,
            "a serializer's own failure must be classified as a \
             policy-eligible user-component failure, not as `Invariant` \
             (which the wrapper's own resource-bound rejection uses and \
             which is not policy-eligible for retry/skip)"
        );
    }

    #[test]
    fn writer_ignoring_the_sink_overflow_error_still_fails_closed() {
        // A `serialize` that writes past the bound, gets an `Err` back from
        // the sink, and then *ignores* it (returns `Ok(())` anyway) must
        // still have the whole write rejected -- the sink's sticky
        // `overflowed` flag is checked unconditionally, not only when
        // `serialize` itself propagates the error.
        let store = Arc::new(InMemoryObjectStore::new(1024));
        let resources = ResourceSet::new(vec![ResourceIdentity::new("out").unwrap()]);
        let identity = ComponentStreamIdentity::new(
            "oxide-batch.object-store-unit-test.ignored-overflow-fails-closed",
        )
        .unwrap();
        // Eight-byte bound; each item is four bytes, so a second item pushes
        // the sink past the bound. The closure deliberately swallows the
        // sink's `Err` and returns `Ok(())` regardless.
        let serialize = |item: &u64, sink: &mut dyn std::io::Write| {
            let _ = sink.write_all(&[u8::try_from(*item).unwrap(); 4]);
            Ok(())
        };
        let opener = ObjectStoreWriterOpener::new(Arc::clone(&store), serialize, 8);
        let (_source, token) = stop();
        futures_executor::block_on(async {
            let (writer, stream, _contract) = multi_resource_writer::<u64, _, _>(
                resources,
                opener,
                identity,
                crate::item_components::multi_resource::NoRollover,
                RestartabilityDeclaration::Restartable,
            )
            .unwrap();
            stream
                .open(StreamOpenContext::new(None, &token))
                .await
                .unwrap();
            // Two items in one batch: the second alone pushes 4 -> 8 (still
            // within bound), so extend with a third item in the same batch
            // that would push 8 -> 12, over the bound, to exercise the
            // ignored-overflow path within a single `write` call.
            let result = writer
                .write(&[1, 2, 3], WriteContext::non_transactional(&token))
                .await;
            assert!(
                result.is_err(),
                "the batch must fail even though `serialize` itself always \
                 returns Ok"
            );
        });
        // The object must never have been created at all: `put` is never
        // reached once the sink has overflowed.
        let missing = futures_executor::block_on(store.stat(&id("out")));
        assert!(
            missing.is_err(),
            "the backend must never receive a partial object built from an \
             ignored sink overflow"
        );
    }

    const LARGE_OUTPUT_TEST_MAX_OBJECT_BYTES: usize = 64;

    #[test]
    fn writer_bounds_a_large_logical_output_written_via_many_small_sink_writes() {
        // A real streaming encoder writes incrementally, many small `write`
        // calls per item, not one large owned buffer. Proves the bound
        // holds against that realistic usage: a logical output far larger
        // than `max_object_bytes`, built entirely from small (one-byte)
        // sink writes, is rejected without the candidate ever exceeding the
        // bound and without the backend ever being called.
        let store = Arc::new(InMemoryObjectStore::new(1_000_000));
        let resources = ResourceSet::new(vec![ResourceIdentity::new("out").unwrap()]);
        let identity = ComponentStreamIdentity::new(
            "oxide-batch.object-store-unit-test.large-output-many-small-writes",
        )
        .unwrap();
        // Each item writes 100 individual one-byte `write_all` calls (a
        // stand-in for a real incremental encoder) -- 100 bytes per item,
        // far more than `MAX_OBJECT_BYTES`, so even the *first* item alone
        // must be rejected partway through, one byte-sized write at a time.
        let serialize = |_item: &u64, sink: &mut dyn std::io::Write| {
            for _ in 0..100 {
                sink.write_all(&[0u8])?;
            }
            Ok(())
        };
        let opener = ObjectStoreWriterOpener::new(
            Arc::clone(&store),
            serialize,
            LARGE_OUTPUT_TEST_MAX_OBJECT_BYTES,
        );
        let (_source, token) = stop();
        futures_executor::block_on(async {
            let (writer, stream, _contract) = multi_resource_writer::<u64, _, _>(
                resources,
                opener,
                identity,
                crate::item_components::multi_resource::NoRollover,
                RestartabilityDeclaration::Restartable,
            )
            .unwrap();
            stream
                .open(StreamOpenContext::new(None, &token))
                .await
                .unwrap();
            let result = writer
                .write(&[1], WriteContext::non_transactional(&token))
                .await;
            assert!(
                result.is_err(),
                "a logical output far exceeding the bound, built from many \
                 small writes, must still be rejected"
            );
        });
        let missing = futures_executor::block_on(store.stat(&id("out")));
        assert!(
            missing.is_err(),
            "the backend must never be called for an oversized object, no \
             matter how many small writes built it up"
        );
    }

    #[test]
    fn chunk_runtime_retries_a_panicking_serializer_and_preserves_prior_committed_content() {
        // `writer_panic_during_serialize_does_not_lose_previously_accumulated_content`
        // above proves the `TruncateOnDrop` guard itself, directly, via a
        // hand-driven `catch_unwind`. This test proves the thing that
        // actually makes that guard load-bearing: the *chunk runtime's own*
        // panic-catch-and-retry path (`invoke_writer` classifying the panic
        // as `WriterError::new()`/`UserComponent`, then a `FaultDecision::Retry`
        // reinvoking the same writer instance). It drives a real
        // `ObjectItemWriter`, constructed exactly as production code would
        // via `ObjectStoreWriterOpener`/`MultiResourceWriterOpener`, through
        // a real `ChunkStep`/`FaultRuntime`, with the paired
        // `ObjectItemWriterStream` *registered* via `ChunkStep::with_item_stream`
        // (not driven by hand): the runtime opens it, calls its `update` once
        // per committing attempt, and hands the resulting envelope to
        // `ChunkTransaction::commit_with_component_state`, exactly as a real
        // adapter would receive it. The durable-checkpoint assertions below
        // decode the envelope this test's transaction captured *from that
        // call*, not a value obtained by calling `update` a second time by
        // hand after the run -- the latter would only prove the writer's
        // in-memory candidate, not what the runtime actually committed.
        use crate::{
            BackoffOutcome, BackoffPolicy, BackoffSleeper, BoxFuture, Checkpoint,
            ChunkCommitReceipt, ChunkCompletion, ChunkCompletionContext, ChunkCompletionError,
            ChunkCompletionOutcome, ChunkCounts, ChunkDeliveryMode, ChunkExecutionOutcome,
            ChunkFaultProgress, ChunkSize, ChunkStep, ChunkTransaction, ChunkTransactionError,
            ChunkTransactionManager, ClassifierRevision, ComponentStateEnvelope, ExecutionAttempt,
            ExecutionContext, ExecutionCorrelation, FaultAction, FaultClassifier, FaultPhase,
            FaultPolicy, FaultRule, FaultRuntime, InMemoryFaultState, ItemProcessor,
            JobExecutionId, JobInstanceId, JobName, ProcessContext, ProcessOutcome, ProcessorError,
            RetryLimit, RetryStateLimit, SkipLimit, StepExecutionId, StepName,
        };
        use std::collections::VecDeque;
        use std::num::NonZeroU64;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::time::Duration;

        struct FixedReader(VecDeque<u64>);
        impl ItemReader<u64> for FixedReader {
            async fn read(
                &mut self,
                _context: ReadContext<'_>,
            ) -> Result<ReadOutcome<u64>, ReaderError> {
                Ok(self
                    .0
                    .pop_front()
                    .map_or(ReadOutcome::EndOfInput, ReadOutcome::Item))
            }
        }

        struct Identity;
        impl ItemProcessor<u64, u64> for Identity {
            async fn process(
                &self,
                item: &u64,
                _context: ProcessContext<'_>,
            ) -> Result<ProcessOutcome<u64>, ProcessorError> {
                Ok(ProcessOutcome::Item(*item))
            }
        }

        struct NoWaitSleeper;
        impl BackoffSleeper for NoWaitSleeper {
            fn sleep<'a>(
                &'a self,
                _delay: Duration,
                _stop: &'a crate::StopToken,
            ) -> BoxFuture<'a, BackoffOutcome> {
                Box::pin(async { BackoffOutcome::Elapsed })
            }
        }

        // Captures the exact `component_state` slice the runtime hands to
        // `commit_with_component_state` on the most recent successful
        // commit -- overwritten (not accumulated) each time, so what
        // remains after the run is the *last durably committed* envelope,
        // matching what a real adapter would have persisted.
        struct NoopTransaction {
            captured: Arc<Mutex<Vec<ComponentStateEnvelope>>>,
        }
        impl ChunkTransaction for NoopTransaction {
            fn business_transaction(&mut self) -> Option<&mut dyn crate::BusinessTransaction> {
                None
            }

            fn commit(
                &mut self,
                counts: ChunkCounts,
                fault: ChunkFaultProgress,
            ) -> BoxFuture<'_, Result<ChunkCommitReceipt, ChunkTransactionError>> {
                self.commit_with_component_state(counts, fault, &[])
            }

            fn commit_with_component_state<'a>(
                &'a mut self,
                _counts: ChunkCounts,
                _fault: ChunkFaultProgress,
                component_state: &'a [ComponentStateEnvelope],
            ) -> BoxFuture<'a, Result<ChunkCommitReceipt, ChunkTransactionError>> {
                *self.captured.lock().unwrap_or_else(PoisonError::into_inner) =
                    component_state.to_vec();
                Box::pin(async {
                    let checkpoint = Checkpoint::from_json(
                        br#"{"format":"oxide-batch.checkpoint","format_version":1,"schema":"test.position","schema_version":1,"payload":{"position":0}}"#,
                        StateLimits::default(),
                    )
                    .expect("checkpoint fixture must be valid");
                    let context = ExecutionContext::from_json(
                        br#"{"format":"oxide-batch.execution-context","format_version":1,"schema":"test.context","schema_version":1,"payload":{}}"#,
                        StateLimits::default(),
                    )
                    .expect("context fixture must be valid");
                    Ok(ChunkCommitReceipt::new(checkpoint, context))
                })
            }

            fn rollback(&mut self) -> BoxFuture<'_, Result<(), ChunkTransactionError>> {
                Box::pin(async { Ok(()) })
            }
        }

        struct NoopTransactions {
            captured: Arc<Mutex<Vec<ComponentStateEnvelope>>>,
        }
        impl ChunkTransactionManager for NoopTransactions {
            fn begin(
                &self,
            ) -> BoxFuture<'_, Result<Box<dyn ChunkTransaction + '_>, ChunkTransactionError>>
            {
                let transaction = NoopTransaction {
                    captured: Arc::clone(&self.captured),
                };
                Box::pin(async move { Ok(Box::new(transaction) as Box<dyn ChunkTransaction>) })
            }
        }

        struct NoopCompletion;
        impl ChunkCompletion for NoopCompletion {
            fn after_commit<'a>(
                &'a self,
                _context: ChunkCompletionContext<'a>,
            ) -> BoxFuture<'a, Result<ChunkCompletionOutcome, ChunkCompletionError>> {
                Box::pin(async { Ok(ChunkCompletionOutcome::Acknowledged) })
            }
        }

        // The real object-store writer, constructed exactly as production
        // code would via `ObjectStoreWriterOpener`/`MultiResourceWriterOpener`.
        let store = Arc::new(InMemoryObjectStore::new(1024));
        let panicked_once = Arc::new(AtomicBool::new(false));
        let panicked_once_for_serialize = Arc::clone(&panicked_once);
        let serialize = move |item: &u64, sink: &mut dyn std::io::Write| {
            let first_attempt_on_item_two =
                *item == 2 && !panicked_once_for_serialize.swap(true, Ordering::SeqCst);
            assert!(
                !first_attempt_on_item_two,
                "serializer panics on item 2's first attempt, deliberately, for this test"
            );
            sink.write_all(format!("{item},").as_bytes())
        };
        let opener = ObjectStoreWriterOpener::new(Arc::clone(&store), serialize, 1024);
        let resource = ResourceIdentity::new("out").unwrap();
        let identity = ComponentStreamIdentity::new(
            "oxide-batch.object-store-unit-test.panic-retry-integration",
        )
        .unwrap();
        let (_source, token) = stop();
        let (writer, write_stream, contract) = futures_executor::block_on(
            MultiResourceWriterOpener::<u64>::open(&opener, &resource, 0, &identity),
        )
        .unwrap();

        let committed_component_state: Arc<Mutex<Vec<ComponentStateEnvelope>>> =
            Arc::new(Mutex::new(Vec::new()));

        let mut step = ChunkStep::new(
            StepName::new("object_store_panic_retry_step").unwrap(),
            ChunkSize::new(1).unwrap(),
            FixedReader(VecDeque::from([1_u64, 2_u64])),
            Identity,
            writer,
            Arc::new(NoopTransactions {
                captured: Arc::clone(&committed_component_state),
            }),
            Arc::new(NoopCompletion),
        )
        .with_item_stream(identity, write_stream, contract)
        .with_fault_runtime(
            FaultRuntime::new(
                FaultPolicy::new(
                    FaultClassifier::new(
                        ClassifierRevision::new("panic_retry_test_v1").unwrap(),
                        [FaultRule::new(
                            FaultPhase::Write,
                            FailureCategory::UserComponent,
                            FaultAction::retry(),
                        )
                        .unwrap()],
                    )
                    .unwrap(),
                    RetryLimit::new(1).unwrap(),
                    RetryStateLimit::new(4).unwrap(),
                    SkipLimit::new(0),
                    BackoffPolicy::none(),
                )
                .unwrap(),
                Arc::new(NoWaitSleeper),
                Arc::new(InMemoryFaultState::new(RetryStateLimit::new(4).unwrap())),
                ChunkDeliveryMode::AtLeastOnce,
            )
            .unwrap(),
        );

        let correlation = ExecutionCorrelation::new(
            JobName::new("object_store_panic_retry_job").unwrap(),
            JobInstanceId::new(1).unwrap(),
            JobExecutionId::new(1).unwrap(),
            ExecutionAttempt::new(NonZeroU64::new(1).unwrap()),
            StepName::new("object_store_panic_retry_step").unwrap(),
            StepExecutionId::new(1).unwrap(),
            ExecutionAttempt::new(NonZeroU64::new(1).unwrap()),
        );

        let report = futures_executor::block_on(step.execute(&correlation, &token));

        assert_eq!(report.outcome(), ChunkExecutionOutcome::Completed);
        assert_eq!(
            report.retry_counts().write(),
            1,
            "the panic on item 2's chunk must have been observed and retried \
             exactly once by the chunk runtime"
        );
        assert_eq!(report.committed_counts().written().get(), 2);

        let (bytes, metadata) = futures_executor::block_on(store.get(&id("out"), 1024)).unwrap();
        assert_eq!(
            bytes,
            b"1,2,".to_vec(),
            "item 1's prior committed content must survive the panic on \
             item 2's chunk, and item 2 must appear exactly once after the \
             retry -- never duplicated, never dropped"
        );

        // The durably committed checkpoint, exactly as the runtime handed
        // it to `commit_with_component_state` on the last successful
        // commit -- not a value obtained by calling `update` by hand after
        // the run.
        let captured = committed_component_state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        assert_eq!(
            captured.len(),
            1,
            "the one registered stream must produce exactly one committed \
             envelope per commit"
        );
        let restored: ObjectWritePosition =
            captured[0].decode(&object_write_position_codec()).unwrap();
        drop(captured);
        assert_eq!(
            restored.committed_item_count, 2,
            "the durably committed checkpoint must reflect both items, not \
             just the one written before the panic"
        );
        assert_eq!(
            restored.version_token.as_deref(),
            metadata.version_token().map(ObjectVersionToken::as_str),
            "the committed checkpoint's recorded version token must match \
             the object's actual, final version after the retried write"
        );
    }
}
