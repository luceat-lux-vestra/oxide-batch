//! Retry-key derivation, bounded fault-state reservation, and runtime bundle.
//!
//! The chunk runtime reserves a retry ordinal through [`FaultStateStore`]
//! *after* a known rollback and *before* backoff, so a process that stops
//! between reservation and re-invocation has still consumed the ordinal. This
//! module owns the framework side of that boundary: the opaque retry key, the
//! compare-and-swap reservation contract, and a bounded in-memory
//! implementation.
//!
//! [`InMemoryFaultState`] keeps reservations for one process only. The durable
//! `PostgreSQL` schema-2 envelope and its enlisted clear are owned by the
//! dependent repository workstream; the ordering enforced here does not change
//! when that implementation replaces this one.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};

use sha2::{Digest, Sha256};

use crate::{
    BackoffSleeper, BoxFuture, ChunkDeliveryMode, FailureCategory, FaultPhase, FaultPolicy,
    RetryOrdinal, RetryStateLimit, StepName,
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
            Self::Unavailable => formatter.write_str("fault state is unavailable"),
        }
    }
}

impl Error for FaultStateError {}

/// Durable, bounded retry-reservation state for one step execution.
///
/// Implementations perform a compare-and-swap: a reservation is accepted only
/// when its ordinal directly follows the persisted ordinal for the same key.
/// The reservation must be durable before the runtime waits for backoff.
pub trait FaultStateStore: Send + Sync {
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
