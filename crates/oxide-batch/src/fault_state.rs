//! Retry-key derivation, bounded fault-state reservation, and runtime bundle.
//!
//! The chunk runtime reserves a retry ordinal through [`FaultStateStore`]
//! *after* a known rollback and *before* backoff, so a process that stops
//! between reservation and re-invocation has still consumed the ordinal. This
//! module owns the framework side of that boundary: the opaque retry key, the
//! compare-and-swap reservation contract, and a bounded in-memory
//! implementation.
//!
//! [`InMemoryFaultState`] keeps reservations for one process only, which the
//! contract permits because a restart may invoke fewer retries than were
//! reserved, never more. [`FaultStateEnvelope`] is the durable format a
//! repository adapter persists and validates; `PostgresFaultState` implements
//! the same ordering against schema 2.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};

use serde_json::{Map, Number, Value};
use sha2::{Digest, Sha256};

use crate::{
    BackoffSleeper, BoxFuture, ChunkDeliveryMode, ChunkTransactionContext, ClassifierRevision,
    FailureCategory, FaultPhase, FaultPolicy, RetryLimit, RetryOrdinal, RetryStateLimit,
    SkipCounts, StepName,
};

/// The domain separator that keeps retry keys distinct from other digests.
const RETRY_KEY_DOMAIN: &[u8] = b"oxide-batch/retry-key/1";

/// An opaque framework digest identifying one retryable unit of work.
///
/// The key is a SHA-256 digest over the definition fingerprint, step logical
/// ID, failure phase, committed checkpoint identity, and the stable item or
/// output ordinal. It contains no item value, and it is never a telemetry
/// field: [`Debug`] redacts it and durable state sorts keys by digest.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RetryKey([u8; 32]);

impl RetryKey {
    /// Derives the key for one failed unit of work.
    #[must_use]
    pub(crate) fn derive(
        definition_digest: &[u8; 32],
        step_name: &StepName,
        phase: FaultPhase,
        checkpoint_digest: &[u8; 32],
        ordinal: u64,
    ) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(RETRY_KEY_DOMAIN);
        hasher.update(definition_digest);
        hasher.update((step_name.as_str().len() as u64).to_be_bytes());
        hasher.update(step_name.as_str().as_bytes());
        hasher.update(phase.as_str().as_bytes());
        hasher.update([0]);
        hasher.update(checkpoint_digest);
        hasher.update(ordinal.to_be_bytes());
        Self(hasher.finalize().into())
    }

    /// Restores a key an authorized durable-state adapter persisted.
    ///
    /// Only a store that round-trips [`Self::as_bytes`] may call this. The
    /// runtime always derives keys from framework inputs.
    #[must_use]
    pub const fn from_bytes(digest: [u8; 32]) -> Self {
        Self(digest)
    }

    /// Borrows the digest for an authorized durable-state adapter.
    ///
    /// The digest is restart-relevant persistence input. It must not be logged,
    /// exported as telemetry, or used as a metric label.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for RetryKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetryKey")
            .field("digest", &"<redacted>")
            .finish()
    }
}

/// One durable retry reservation for a single key.
///
/// The reservation records the phase and stable category that produced it, so
/// exhaustion preserves the last typed category without retaining error text.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct RetryReservation {
    key: RetryKey,
    phase: FaultPhase,
    category: FailureCategory,
    ordinal: RetryOrdinal,
}

impl RetryReservation {
    /// Constructs the reservation the runtime asks the store to commit.
    #[must_use]
    pub const fn new(
        key: RetryKey,
        phase: FaultPhase,
        category: FailureCategory,
        ordinal: RetryOrdinal,
    ) -> Self {
        Self {
            key,
            phase,
            category,
            ordinal,
        }
    }

    /// Returns the opaque retry key.
    #[must_use]
    pub const fn key(self) -> RetryKey {
        self.key
    }

    /// Returns the phase that produced the fault.
    #[must_use]
    pub const fn phase(self) -> FaultPhase {
        self.phase
    }

    /// Returns the stable category preserved for exhaustion.
    #[must_use]
    pub const fn category(self) -> FailureCategory {
        self.category
    }

    /// Returns the reserved retry ordinal.
    #[must_use]
    pub const fn ordinal(self) -> RetryOrdinal {
        self.ordinal
    }
}

/// A value-redacted fault-state reservation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum FaultStateError {
    /// The step already retains its maximum unresolved retry keys.
    CapacityExhausted {
        /// The configured unresolved-key capacity.
        max: u32,
    },
    /// The supplied ordinal did not follow the persisted one.
    ///
    /// A stale or concurrent writer loses rather than spending the same
    /// ordinal twice.
    StaleReservation,
    /// Durable fault state could not be interpreted and no work may begin.
    Corrupt(FaultStateFormatError),
    /// A durable store was used before the runtime bound its step execution.
    Unbound,
    /// The fault state could not be read or written.
    Unavailable,
}

impl fmt::Display for FaultStateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CapacityExhausted { max } => {
                write!(
                    formatter,
                    "step already retains {max} unresolved retry keys"
                )
            }
            Self::StaleReservation => {
                formatter.write_str("retry reservation lost to a newer persisted ordinal")
            }
            Self::Corrupt(error) => write!(formatter, "durable fault state is unusable: {error}"),
            Self::Unbound => formatter.write_str("durable fault state has no bound step execution"),
            Self::Unavailable => formatter.write_str("fault state is unavailable"),
        }
    }
}

impl Error for FaultStateError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Corrupt(error) => Some(error),
            _ => None,
        }
    }
}

impl From<FaultStateFormatError> for FaultStateError {
    fn from(error: FaultStateFormatError) -> Self {
        Self::Corrupt(error)
    }
}

/// One unresolved retry key retained in durable fault state.
///
/// The entry holds only framework-owned classification identity. It never
/// contains an item value, error text, parameter, or context value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FaultStateEntry {
    key: RetryKey,
    phase: FaultPhase,
    category: FailureCategory,
    ordinal: RetryOrdinal,
    revision: ClassifierRevision,
}

impl FaultStateEntry {
    /// Constructs one retained entry.
    #[must_use]
    pub const fn new(
        key: RetryKey,
        phase: FaultPhase,
        category: FailureCategory,
        ordinal: RetryOrdinal,
        revision: ClassifierRevision,
    ) -> Self {
        Self {
            key,
            phase,
            category,
            ordinal,
            revision,
        }
    }

    /// Returns the opaque retry key.
    #[must_use]
    pub const fn key(&self) -> RetryKey {
        self.key
    }

    /// Returns the phase that produced the retained fault.
    #[must_use]
    pub const fn phase(&self) -> FaultPhase {
        self.phase
    }

    /// Returns the stable category preserved for exhaustion.
    #[must_use]
    pub const fn category(&self) -> FailureCategory {
        self.category
    }

    /// Returns the reserved retry ordinal.
    #[must_use]
    pub const fn ordinal(&self) -> RetryOrdinal {
        self.ordinal
    }

    /// Borrows the classifier revision that produced the decision.
    #[must_use]
    pub const fn revision(&self) -> &ClassifierRevision {
        &self.revision
    }

    fn to_json(&self) -> Value {
        let mut object = Map::new();
        object.insert(
            String::from("category"),
            Value::String(String::from(self.category.durable_code())),
        );
        object.insert(String::from("key"), Value::String(hex(self.key.as_bytes())));
        object.insert(
            String::from("ordinal"),
            Value::Number(Number::from(self.ordinal.get())),
        );
        object.insert(
            String::from("phase"),
            Value::String(String::from(self.phase.as_str())),
        );
        object.insert(
            String::from("revision"),
            Value::String(String::from(self.revision.as_str())),
        );
        Value::Object(object)
    }

    fn from_json(value: &Value) -> Result<Self, FaultStateFormatError> {
        let object = value
            .as_object()
            .ok_or(FaultStateFormatError::MalformedEntry)?;
        let key = object
            .get("key")
            .and_then(Value::as_str)
            .and_then(unhex)
            .map(RetryKey::from_bytes)
            .ok_or(FaultStateFormatError::MalformedEntry)?;
        let phase = object
            .get("phase")
            .and_then(Value::as_str)
            .and_then(FaultPhase::from_durable_name)
            .ok_or(FaultStateFormatError::UnknownEnumeration)?;
        let category = object
            .get("category")
            .and_then(Value::as_str)
            .and_then(FailureCategory::from_durable_code)
            .ok_or(FaultStateFormatError::UnknownEnumeration)?;
        let ordinal = object
            .get("ordinal")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .and_then(|value| RetryOrdinal::new(value).ok())
            .ok_or(FaultStateFormatError::MalformedEntry)?;
        let revision = object
            .get("revision")
            .and_then(Value::as_str)
            .and_then(|value| ClassifierRevision::new(value).ok())
            .ok_or(FaultStateFormatError::MalformedEntry)?;
        Ok(Self::new(key, phase, category, ordinal, revision))
    }
}

/// The bounded, checksummed fault state of one durable step execution.
///
/// Format 1 is canonical JSON containing the prior committed checkpoint digest
/// and at most [`Self::MAX_ENTRIES`] digest-sorted unresolved retry
/// entries. The empty envelope carries the zero checkpoint digest because no
/// entry depends on a checkpoint generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FaultStateEnvelope {
    checkpoint_digest: [u8; 32],
    entries: Vec<FaultStateEntry>,
}

impl FaultStateEnvelope {
    /// The framework format identifier durable adapters persist.
    pub const FORMAT: &'static str = "oxide-batch.fault-state";
    /// The framework format version durable adapters persist.
    pub const FORMAT_VERSION: u16 = 1;
    /// The framework schema version durable adapters persist.
    pub const SCHEMA_VERSION: u32 = 1;
    /// The canonical byte ceiling accepted by the durable metadata model.
    pub const MAX_BYTES: usize = 64 * 1024;
    /// The hard unresolved-key ceiling of format 1.
    pub const MAX_ENTRIES: usize = 256;

    /// Returns the envelope every step execution starts from.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            checkpoint_digest: [0; 32],
            entries: Vec::new(),
        }
    }

    /// Validates a complete envelope, sorting entries by retry-key digest.
    ///
    /// # Errors
    ///
    /// Returns [`FaultStateFormatError::TooManyEntries`] above the format
    /// ceiling, [`FaultStateFormatError::DuplicateKey`] for a repeated key, and
    /// [`FaultStateFormatError::CheckpointMismatch`] when a non-empty envelope
    /// carries the zero checkpoint digest.
    pub fn new(
        checkpoint_digest: [u8; 32],
        entries: impl IntoIterator<Item = FaultStateEntry>,
    ) -> Result<Self, FaultStateFormatError> {
        let mut entries: Vec<FaultStateEntry> = entries.into_iter().collect();
        if entries.len() > Self::MAX_ENTRIES {
            return Err(FaultStateFormatError::TooManyEntries {
                max: Self::MAX_ENTRIES,
            });
        }
        entries.sort_by_key(FaultStateEntry::key);
        if entries.windows(2).any(|pair| pair[0].key == pair[1].key) {
            return Err(FaultStateFormatError::DuplicateKey);
        }
        if entries.is_empty() {
            if checkpoint_digest != [0; 32] {
                return Err(FaultStateFormatError::CheckpointMismatch);
            }
        } else if checkpoint_digest == [0; 32] {
            return Err(FaultStateFormatError::CheckpointMismatch);
        }
        Ok(Self {
            checkpoint_digest,
            entries,
        })
    }

    /// Returns the checkpoint generation the retained entries belong to.
    #[must_use]
    pub const fn checkpoint_digest(&self) -> &[u8; 32] {
        &self.checkpoint_digest
    }

    /// Borrows the digest-sorted unresolved entries.
    #[must_use]
    pub fn entries(&self) -> &[FaultStateEntry] {
        &self.entries
    }

    /// Returns whether the envelope retains no unresolved key.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the number of retained unresolved keys.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns the ordinal already reserved for `key`, when one exists.
    #[must_use]
    pub fn reserved_ordinal(&self, key: RetryKey) -> Option<RetryOrdinal> {
        self.entry(key).map(FaultStateEntry::ordinal)
    }

    /// Borrows the retained entry for `key`, when one exists.
    #[must_use]
    pub fn entry(&self, key: RetryKey) -> Option<&FaultStateEntry> {
        self.entries
            .binary_search_by(|entry| entry.key.cmp(&key))
            .ok()
            .map(|index| &self.entries[index])
    }

    /// Returns the envelope after accepting one reservation.
    ///
    /// The ordinal must directly follow the persisted one for the same key, and
    /// a new key must fit within `limit` unresolved keys.
    ///
    /// # Errors
    ///
    /// Returns [`FaultStateError::StaleReservation`] for a non-consecutive
    /// ordinal, [`FaultStateError::CapacityExhausted`] at the configured bound,
    /// and [`FaultStateError::Corrupt`] when the entry belongs to a different
    /// checkpoint generation.
    pub fn reserved(
        &self,
        entry: FaultStateEntry,
        checkpoint_digest: [u8; 32],
        limit: RetryStateLimit,
    ) -> Result<Self, FaultStateError> {
        if !self.entries.is_empty() && self.checkpoint_digest != checkpoint_digest {
            return Err(FaultStateError::Corrupt(
                FaultStateFormatError::CheckpointMismatch,
            ));
        }
        let expected = self
            .reserved_ordinal(entry.key())
            .unwrap_or(RetryOrdinal::INITIAL)
            .checked_next()
            .map_err(|_| FaultStateError::StaleReservation)?;
        if entry.ordinal() != expected {
            return Err(FaultStateError::StaleReservation);
        }
        let mut entries = self.entries.clone();
        match entries.binary_search_by(|existing| existing.key.cmp(&entry.key())) {
            Ok(index) => entries[index] = entry,
            Err(index) => {
                if entries.len() >= limit.get() as usize {
                    return Err(FaultStateError::CapacityExhausted { max: limit.get() });
                }
                entries.insert(index, entry);
            }
        }
        Ok(Self {
            checkpoint_digest,
            entries,
        })
    }

    /// Serializes the canonical bytes the durable checksum covers.
    ///
    /// # Errors
    ///
    /// Returns [`FaultStateFormatError::TooLarge`] above the durable ceiling.
    pub fn to_canonical_json(&self) -> Result<Vec<u8>, FaultStateFormatError> {
        let mut object = Map::new();
        object.insert(
            String::from("checkpoint"),
            Value::String(hex(&self.checkpoint_digest)),
        );
        object.insert(
            String::from("entries"),
            Value::Array(self.entries.iter().map(FaultStateEntry::to_json).collect()),
        );
        let bytes = serde_json::to_vec(&Value::Object(object))
            .map_err(|_| FaultStateFormatError::Malformed)?;
        if bytes.len() > Self::MAX_BYTES {
            return Err(FaultStateFormatError::TooLarge {
                max_bytes: Self::MAX_BYTES,
            });
        }
        Ok(bytes)
    }

    /// Returns the SHA-256 checksum over the canonical bytes.
    ///
    /// # Errors
    ///
    /// Returns [`FaultStateFormatError::TooLarge`] above the durable ceiling.
    pub fn checksum(&self) -> Result<[u8; 32], FaultStateFormatError> {
        Ok(Sha256::digest(self.to_canonical_json()?).into())
    }

    /// Validates canonical bytes and their durable checksum.
    ///
    /// Unknown format or schema versions, checksum mismatch, invalid
    /// enumerations, an unsorted or duplicated key, and an over-large payload
    /// are corruption. No component work may begin after one.
    ///
    /// # Errors
    ///
    /// Returns the redacted [`FaultStateFormatError`] that rejected the bytes.
    pub fn from_canonical_json(
        format_version: u16,
        schema: &str,
        schema_version: u32,
        bytes: &[u8],
        checksum: &[u8; 32],
    ) -> Result<Self, FaultStateFormatError> {
        if format_version != Self::FORMAT_VERSION || schema != Self::FORMAT {
            return Err(FaultStateFormatError::UnsupportedFormat);
        }
        if schema_version != Self::SCHEMA_VERSION {
            return Err(FaultStateFormatError::UnsupportedSchemaVersion);
        }
        if bytes.len() > Self::MAX_BYTES {
            return Err(FaultStateFormatError::TooLarge {
                max_bytes: Self::MAX_BYTES,
            });
        }
        let value: Value =
            serde_json::from_slice(bytes).map_err(|_| FaultStateFormatError::Malformed)?;
        let object = value.as_object().ok_or(FaultStateFormatError::Malformed)?;
        let checkpoint_digest = object
            .get("checkpoint")
            .and_then(Value::as_str)
            .and_then(unhex)
            .ok_or(FaultStateFormatError::Malformed)?;
        let raw = object
            .get("entries")
            .and_then(Value::as_array)
            .ok_or(FaultStateFormatError::Malformed)?;
        if raw.len() > Self::MAX_ENTRIES {
            return Err(FaultStateFormatError::TooManyEntries {
                max: Self::MAX_ENTRIES,
            });
        }
        let entries = raw
            .iter()
            .map(FaultStateEntry::from_json)
            .collect::<Result<Vec<_>, _>>()?;
        if entries.windows(2).any(|pair| pair[0].key >= pair[1].key) {
            return Err(FaultStateFormatError::UnsortedEntries);
        }
        let envelope = Self::new(checkpoint_digest, entries)?;
        if &envelope.checksum()? != checksum {
            return Err(FaultStateFormatError::ChecksumMismatch);
        }
        Ok(envelope)
    }

    /// Rejects state the current policy and checkpoint cannot own.
    ///
    /// # Errors
    ///
    /// Returns [`FaultStateFormatError::OrdinalAboveLimit`] for an ordinal the
    /// configured retry limit cannot reach, [`FaultStateFormatError::
    /// TooManyEntries`] above the configured capacity, and
    /// [`FaultStateFormatError::CheckpointMismatch`] when the retained entries
    /// belong to a superseded checkpoint.
    pub fn validate_for(
        &self,
        retry_limit: RetryLimit,
        state_limit: RetryStateLimit,
        checkpoint_digest: &[u8; 32],
    ) -> Result<(), FaultStateFormatError> {
        if self.entries.len() > state_limit.get() as usize {
            return Err(FaultStateFormatError::TooManyEntries {
                max: state_limit.get() as usize,
            });
        }
        if self
            .entries
            .iter()
            .any(|entry| entry.ordinal().get() > retry_limit.get())
        {
            return Err(FaultStateFormatError::OrdinalAboveLimit {
                max: retry_limit.get(),
            });
        }
        if !self.entries.is_empty() && &self.checkpoint_digest != checkpoint_digest {
            return Err(FaultStateFormatError::CheckpointMismatch);
        }
        Ok(())
    }
}

impl Default for FaultStateEnvelope {
    fn default() -> Self {
        Self::empty()
    }
}

/// A value-redacted durable fault-state format failure.
///
/// Every variant is corruption or an unsupported version. The runtime fails
/// closed before any component work begins.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum FaultStateFormatError {
    /// The stored format identifier or version is not format 1.
    UnsupportedFormat,
    /// The stored schema version is newer than this runtime understands.
    UnsupportedSchemaVersion,
    /// The payload was not canonical fault-state JSON.
    Malformed,
    /// One entry was not a valid fault-state entry object.
    MalformedEntry,
    /// A stored phase or category name is not a known enumeration value.
    UnknownEnumeration,
    /// The stored checksum does not cover the stored payload.
    ChecksumMismatch,
    /// The payload exceeded the durable byte ceiling.
    TooLarge {
        /// Maximum accepted canonical bytes.
        max_bytes: usize,
    },
    /// The payload retained more keys than the accepted bound.
    TooManyEntries {
        /// Maximum accepted unresolved keys.
        max: usize,
    },
    /// The payload retained the same retry key twice.
    DuplicateKey,
    /// The payload was not sorted by retry-key digest.
    UnsortedEntries,
    /// A retained ordinal is above the configured retry limit.
    OrdinalAboveLimit {
        /// Maximum re-invocations the policy allows.
        max: u32,
    },
    /// The retained keys do not belong to the committed checkpoint.
    CheckpointMismatch,
}

impl fmt::Display for FaultStateFormatError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedFormat => formatter.write_str("fault-state format is unsupported"),
            Self::UnsupportedSchemaVersion => {
                formatter.write_str("fault-state schema version is unsupported")
            }
            Self::Malformed => formatter.write_str("fault state is malformed"),
            Self::MalformedEntry => formatter.write_str("fault-state entry is malformed"),
            Self::UnknownEnumeration => {
                formatter.write_str("fault state contains an unknown enumeration value")
            }
            Self::ChecksumMismatch => formatter.write_str("fault-state checksum does not match"),
            Self::TooLarge { max_bytes } => {
                write!(formatter, "fault state exceeds {max_bytes} bytes")
            }
            Self::TooManyEntries { max } => {
                write!(formatter, "fault state retains more than {max} keys")
            }
            Self::DuplicateKey => formatter.write_str("fault state repeats one retry key"),
            Self::UnsortedEntries => formatter.write_str("fault state is not digest-sorted"),
            Self::OrdinalAboveLimit { max } => {
                write!(formatter, "fault state retains an ordinal above {max}")
            }
            Self::CheckpointMismatch => {
                formatter.write_str("fault state belongs to a superseded checkpoint")
            }
        }
    }
}

impl Error for FaultStateFormatError {}

fn hex(bytes: &[u8; 32]) -> String {
    let mut text = String::with_capacity(64);
    for byte in bytes {
        text.push(char::from_digit(u32::from(byte >> 4), 16).unwrap_or('0'));
        text.push(char::from_digit(u32::from(byte & 0x0f), 16).unwrap_or('0'));
    }
    text
}

fn unhex(text: &str) -> Option<[u8; 32]> {
    if text.len() != 64 {
        return None;
    }
    let mut bytes = [0_u8; 32];
    let raw = text.as_bytes();
    for (index, slot) in bytes.iter_mut().enumerate() {
        let high = char::from(raw[index * 2]).to_digit(16)?;
        let low = char::from(raw[index * 2 + 1]).to_digit(16)?;
        *slot = u8::try_from(high * 16 + low).ok()?;
    }
    Some(bytes)
}

/// Durable, bounded retry-reservation state for one step execution.
///
/// Implementations perform a compare-and-swap: a reservation is accepted only
/// when its ordinal directly follows the persisted ordinal for the same key.
/// The reservation must be durable before the runtime waits for backoff.
pub trait FaultStateStore: Send + Sync {
    /// Binds durable state to the step execution about to run.
    ///
    /// The runtime calls this once before the first chunk attempt when it
    /// executes through a repository. A process-local store ignores it; a
    /// durable store cannot read or write state before it is bound.
    fn bind(
        &self,
        _context: ChunkTransactionContext,
    ) -> BoxFuture<'_, Result<(), FaultStateError>> {
        Box::pin(std::future::ready(Ok(())))
    }

    /// Returns the ordinal already reserved for `key`, when one exists.
    fn reserved_ordinal(
        &self,
        key: RetryKey,
    ) -> BoxFuture<'_, Result<Option<RetryOrdinal>, FaultStateError>>;

    /// Commits one reservation, consuming its ordinal.
    fn reserve(&self, reservation: RetryReservation) -> BoxFuture<'_, Result<(), FaultStateError>>;

    /// Marks `key` resolved because its unit of work succeeded or was skipped.
    ///
    /// The key stays retained until the accepting chunk commits, because
    /// uncommitted work may still replay.
    fn resolve(&self, key: RetryKey) -> BoxFuture<'_, Result<(), FaultStateError>>;

    /// Clears every resolved key in the commit that advances the checkpoint.
    fn clear_resolved(&self) -> BoxFuture<'_, Result<(), FaultStateError>>;

    /// Returns the number of retained unresolved keys.
    fn unresolved(&self) -> BoxFuture<'_, Result<u32, FaultStateError>>;
}

#[derive(Clone, Copy, Debug)]
struct RetryEntry {
    ordinal: RetryOrdinal,
    resolved: bool,
}

/// A bounded, process-local [`FaultStateStore`].
///
/// This implementation makes the reservation ordering executable without a
/// database. It is not durable: a restart starts from an empty state, which the
/// contract permits because a restart may invoke fewer retries than were
/// reserved, never more.
#[derive(Debug)]
pub struct InMemoryFaultState {
    limit: RetryStateLimit,
    entries: Mutex<BTreeMap<RetryKey, RetryEntry>>,
}

impl InMemoryFaultState {
    /// Constructs an empty bounded state.
    #[must_use]
    pub fn new(limit: RetryStateLimit) -> Self {
        Self {
            limit,
            entries: Mutex::new(BTreeMap::new()),
        }
    }

    fn with_entries<T>(&self, body: impl FnOnce(&mut BTreeMap<RetryKey, RetryEntry>) -> T) -> T {
        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        body(&mut entries)
    }
}

impl FaultStateStore for InMemoryFaultState {
    fn reserved_ordinal(
        &self,
        key: RetryKey,
    ) -> BoxFuture<'_, Result<Option<RetryOrdinal>, FaultStateError>> {
        let result = self.with_entries(|entries| entries.get(&key).map(|entry| entry.ordinal));
        Box::pin(std::future::ready(Ok(result)))
    }

    fn reserve(&self, reservation: RetryReservation) -> BoxFuture<'_, Result<(), FaultStateError>> {
        let limit = self.limit;
        let result = self.with_entries(|entries| {
            let expected = entries
                .get(&reservation.key())
                .map_or(RetryOrdinal::INITIAL, |entry| entry.ordinal)
                .checked_next()
                .map_err(|_| FaultStateError::StaleReservation)?;
            if reservation.ordinal() != expected {
                return Err(FaultStateError::StaleReservation);
            }
            let unresolved = entries.values().filter(|entry| !entry.resolved).count();
            let is_new = !entries.contains_key(&reservation.key());
            if is_new && unresolved >= limit.get() as usize {
                return Err(FaultStateError::CapacityExhausted { max: limit.get() });
            }
            entries.insert(
                reservation.key(),
                RetryEntry {
                    ordinal: reservation.ordinal(),
                    resolved: false,
                },
            );
            Ok(())
        });
        Box::pin(std::future::ready(result))
    }

    fn resolve(&self, key: RetryKey) -> BoxFuture<'_, Result<(), FaultStateError>> {
        self.with_entries(|entries| {
            if let Some(entry) = entries.get_mut(&key) {
                entry.resolved = true;
            }
        });
        Box::pin(std::future::ready(Ok(())))
    }

    fn clear_resolved(&self) -> BoxFuture<'_, Result<(), FaultStateError>> {
        self.with_entries(|entries| entries.retain(|_, entry| !entry.resolved));
        Box::pin(std::future::ready(Ok(())))
    }

    fn unresolved(&self) -> BoxFuture<'_, Result<u32, FaultStateError>> {
        let count = self.with_entries(|entries| entries.values().filter(|e| !e.resolved).count());
        let result = u32::try_from(count).map_err(|_| FaultStateError::Unavailable);
        Box::pin(std::future::ready(result))
    }
}

/// The validated fault-tolerance capability installed on a chunk step.
///
/// The bundle owns the policy, the injected monotonic sleeper, the reservation
/// store, and the declared delivery mode. Capabilities are validated at
/// construction so a statically impossible combination cannot reach user work.
///
/// ```
/// use std::sync::Arc;
/// use std::time::Duration;
///
/// use oxide_batch::{
///     BackoffOutcome, BackoffPolicy, BackoffSleeper, BoxFuture, ChunkDeliveryMode,
///     ClassifierRevision, FailureCategory, FaultAction, FaultClassifier, FaultPhase, FaultPolicy,
///     FaultRule, FaultRuntime, InMemoryFaultState, RetryLimit, RetryStateLimit, SkipLimit,
///     StopToken,
/// };
///
/// struct ImmediateSleeper;
///
/// impl BackoffSleeper for ImmediateSleeper {
///     fn sleep<'a>(
///         &'a self,
///         _delay: Duration,
///         stop: &'a StopToken,
///     ) -> BoxFuture<'a, BackoffOutcome> {
///         let stopped = stop.is_stop_requested();
///         Box::pin(async move {
///             if stopped { BackoffOutcome::Stopped } else { BackoffOutcome::Elapsed }
///         })
///     }
/// }
///
/// let policy = FaultPolicy::new(
///     FaultClassifier::new(
///         ClassifierRevision::new("import_v1")?,
///         [FaultRule::new(
///             FaultPhase::Write,
///             FailureCategory::Timeout,
///             FaultAction::retry(),
///         )?],
///     )?,
///     RetryLimit::new(2)?,
///     RetryStateLimit::new(16)?,
///     SkipLimit::NONE,
///     BackoffPolicy::fixed(Duration::from_millis(10))?,
/// )?;
/// let state = Arc::new(InMemoryFaultState::new(policy.retry_state_limit()));
/// let runtime = FaultRuntime::new(
///     policy,
///     Arc::new(ImmediateSleeper),
///     state,
///     ChunkDeliveryMode::AtLeastOnce,
/// )?;
/// assert_eq!(runtime.delivery_mode(), ChunkDeliveryMode::AtLeastOnce);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone)]
pub struct FaultRuntime {
    policy: Arc<FaultPolicy>,
    sleeper: Arc<dyn BackoffSleeper>,
    state: Arc<dyn FaultStateStore>,
    delivery_mode: ChunkDeliveryMode,
}

impl FaultRuntime {
    /// Validates and installs the fault-tolerance capability.
    ///
    /// # Errors
    ///
    /// Returns [`crate::FaultPolicyError::CommitSafeSkipUnsupported`] when the
    /// policy accepts a commit-safe skip that the declared delivery mode cannot
    /// commit atomically.
    pub fn new(
        policy: FaultPolicy,
        sleeper: Arc<dyn BackoffSleeper>,
        state: Arc<dyn FaultStateStore>,
        delivery_mode: ChunkDeliveryMode,
    ) -> Result<Self, crate::FaultPolicyError> {
        policy.validate_capabilities(matches!(
            delivery_mode,
            ChunkDeliveryMode::AtomicSameResource
        ))?;
        Ok(Self {
            policy: Arc::new(policy),
            sleeper,
            state,
            delivery_mode,
        })
    }

    /// Borrows the validated step policy.
    #[must_use]
    pub fn policy(&self) -> &FaultPolicy {
        &self.policy
    }

    /// Borrows the injected monotonic sleeper.
    #[must_use]
    pub fn sleeper(&self) -> &dyn BackoffSleeper {
        self.sleeper.as_ref()
    }

    /// Borrows the reservation store.
    #[must_use]
    pub fn state(&self) -> &dyn FaultStateStore {
        self.state.as_ref()
    }

    /// Returns the delivery mode declared for this step.
    #[must_use]
    pub const fn delivery_mode(&self) -> ChunkDeliveryMode {
        self.delivery_mode
    }
}

impl fmt::Debug for FaultRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FaultRuntime")
            .field("retry_limit", &self.policy.retry_limit())
            .field("retry_state_limit", &self.policy.retry_state_limit())
            .field("skip_limit", &self.policy.skip_limit())
            .field("backoff", &self.policy.backoff().kind())
            .field("delivery_mode", &self.delivery_mode)
            .finish_non_exhaustive()
    }
}

/// The committed fault-tolerance totals one step attempt inherits.
///
/// A restart copies the latest committed totals to the new attempt, so a
/// bounded limit spans every attempt of one job instance rather than resetting
/// per process.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct FaultProgress {
    retries: RetryCounts,
    skips: SkipCounts,
    rollbacks: u64,
    no_rollbacks: u64,
}

impl FaultProgress {
    /// The totals a first attempt of a new job instance inherits.
    pub const NONE: Self = Self {
        retries: RetryCounts::ZERO,
        skips: SkipCounts::ZERO,
        rollbacks: 0,
        no_rollbacks: 0,
    };

    /// Constructs a committed total snapshot.
    #[must_use]
    pub const fn new(
        retries: RetryCounts,
        skips: SkipCounts,
        rollbacks: u64,
        no_rollbacks: u64,
    ) -> Self {
        Self {
            retries,
            skips,
            rollbacks,
            no_rollbacks,
        }
    }

    /// Returns inherited per-phase reserved retry counts.
    #[must_use]
    pub const fn retries(self) -> RetryCounts {
        self.retries
    }

    /// Returns inherited per-phase committed skip counts.
    #[must_use]
    pub const fn skips(self) -> SkipCounts {
        self.skips
    }

    /// Returns inherited acknowledged framework rollback decisions.
    #[must_use]
    pub const fn rollbacks(self) -> u64 {
        self.rollbacks
    }

    /// Returns inherited commits that accepted a commit-safe skip.
    #[must_use]
    pub const fn no_rollbacks(self) -> u64 {
        self.no_rollbacks
    }
}

/// Durable retry attempts, kept distinct per phase.
///
/// A count records one reserved retry ordinal, not one component call.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RetryCounts {
    read: u64,
    process: u64,
    write: u64,
}

impl RetryCounts {
    /// Counts inherited by a first attempt.
    pub const ZERO: Self = Self {
        read: 0,
        process: 0,
        write: 0,
    };

    /// Constructs per-phase retry counts.
    #[must_use]
    pub const fn new(read: u64, process: u64, write: u64) -> Self {
        Self {
            read,
            process,
            write,
        }
    }

    /// Returns reserved read retries.
    #[must_use]
    pub const fn read(self) -> u64 {
        self.read
    }

    /// Returns reserved process retries.
    #[must_use]
    pub const fn process(self) -> u64 {
        self.process
    }

    /// Returns reserved write retries.
    #[must_use]
    pub const fn write(self) -> u64 {
        self.write
    }

    /// Returns the counts after one reserved retry in `phase`.
    ///
    /// A phase that cannot reserve a retry leaves the counts unchanged.
    #[must_use]
    pub const fn increment(mut self, phase: FaultPhase) -> Self {
        let counter = match phase {
            FaultPhase::Read => &mut self.read,
            FaultPhase::Process => &mut self.process,
            FaultPhase::Write => &mut self.write,
            _ => return self,
        };
        *counter = counter.saturating_add(1);
        self
    }
}
