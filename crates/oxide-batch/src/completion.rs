//! Bounded chunk-completion (repeat) policies -- `REPEAT-POLICY-001`.
//!
//! A [`CompletionPolicy`] decides, *in addition to* the [`ChunkSize`] ceiling
//! every [`crate::ChunkStep`] already enforces, whether a chunk should stop
//! accepting further items before that hard ceiling is reached. Installing a
//! policy never removes the ceiling: `ChunkSize` remains the resource-safety
//! bound for every chunk regardless of which policy (if any) is installed
//! via [`crate::ChunkStep::with_completion_policy`], so buffering stays
//! bounded even if a custom policy never completes on its own.
//!
//! Four families are provided:
//!
//! - [`ItemCountCompletionPolicy`] -- deterministic item-count completion,
//!   the same bound as [`ChunkSize`] expressed as a policy.
//! - [`TimeCompletionPolicy`] -- deterministic, clock-injected time-based
//!   completion.
//! - [`CompositeCompletionPolicy`] -- bounded `Any`/`All` composition of
//!   other policies.
//! - [`AdaptiveCompletionPolicy`] -- a bounded policy whose target chunk
//!   size adapts toward an observed target duration. It is both a
//!   [`CompletionPolicy`] and an [`crate::ItemStream`] over the *same*
//!   instance state. [`CompletionPolicy::stream_registrations`] reports that
//!   `ItemStream` registration bound to the exact instance it is called on,
//!   so [`crate::ChunkStep::with_completion_policy`] (or the
//!   [`crate::ChunkStep::with_adaptive_completion_policy`] alias) wires it up
//!   automatically -- whether the policy is installed directly or nested at
//!   any depth inside a [`CompositeCompletionPolicy`] -- through the same
//!   commit boundary as any other component state, with no second
//!   persistence path and no risk of the two registrations drifting onto
//!   different instances.
//!
//! ```
//! use std::sync::Arc;
//!
//! use oxide_batch::{ChunkCount, ChunkSize, CompletionPolicy, ItemCountCompletionPolicy};
//!
//! let policy = ItemCountCompletionPolicy::new(ChunkSize::new(10)?);
//! assert!(!policy.is_complete(ChunkCount::new(9)));
//! assert!(policy.is_complete(ChunkCount::new(10)));
//! # Ok::<(), oxide_batch::ChunkError>(())
//! ```

use std::error::Error;
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::{Duration, SystemTime};

use crate::{
    BoxedStream, ChunkAttemptOutcome, ChunkCount, ChunkSize, Clock, CodecId, CodecVersion,
    ComponentStateEnvelope, ComponentStreamIdentity, DefaultComponentCodec, ItemStream,
    RestartabilityDeclaration, StateCodecError, StateLimits, StateSchemaId, StateSchemaVersion,
    StreamCloseContext, StreamCloseError, StreamCloseOutcome, StreamOpenContext, StreamOpenError,
    StreamOpenOutcome, StreamStateContract, StreamUpdateContext, StreamUpdateError,
    VersionedStateCodec,
};

/// Decides whether a chunk should stop accepting further items.
///
/// The chunk runtime calls [`begin_chunk`](Self::begin_chunk) once per chunk
/// attempt -- including a replayed attempt of the same logical chunk -- and
/// then calls [`is_complete`](Self::is_complete) once before every item read
/// in that attempt, stopping the read phase as soon as it (or the
/// configured [`ChunkSize`] ceiling) returns `true`.
///
/// Implementations must be deterministic for a given call sequence and must
/// never block on I/O: this is a synchronous, per-item decision on the
/// single-threaded chunk read path.
pub trait CompletionPolicy: Send + Sync {
    /// Resets any per-attempt state at the start of a new chunk attempt.
    ///
    /// # Lifecycle contract (`REPEAT-POLICY-001`)
    ///
    /// The chunk runtime calls [`begin_chunk`](Self::begin_chunk) exactly
    /// once for every chunk attempt whose transaction successfully began --
    /// including a replayed attempt of the same logical chunk, and
    /// regardless of what happens later in that same attempt (a read,
    /// process, or write failure; an unsupported-capability rejection; a
    /// cooperative stop) -- and calls [`end_chunk`](Self::end_chunk) exactly
    /// once in return, once that same attempt reaches a terminal outcome.
    /// An attempt whose transaction never began receives *neither* call:
    /// the pairing is exactly-once per begun attempt, never a bare
    /// `end_chunk` with no matching `begin_chunk`, and never a `begin_chunk`
    /// left without its matching `end_chunk` -- even when the attempt's own
    /// rollback subsequently fails. This pairing is structurally enforced by
    /// the chunk runtime, not left to per-call-site bookkeeping.
    ///
    /// The default implementation does nothing, which is correct for a
    /// stateless policy such as [`ItemCountCompletionPolicy`].
    fn begin_chunk(&self) {}

    /// Reports whether the chunk should stop accepting more items.
    ///
    /// `items_read` is the number of items already read into the current
    /// chunk attempt.
    ///
    /// # Forward-progress invariant
    ///
    /// Once an attempt has begun, the chunk runtime consults this once
    /// before every item read *after* the first: an attempt always reads
    /// (or observes end-of-input for) at least one item before this
    /// policy's decision can end its read phase. A policy that reports
    /// `is_complete(0) == true` therefore can never make the step
    /// repeatedly commit an all-empty chunk purely because it was never
    /// given the chance to read anything -- the runtime never repeats a
    /// zero-item commit solely on this policy's say-so. The read phase
    /// also always stops once the configured [`ChunkSize`] ceiling is
    /// reached, independent of what this method returns.
    fn is_complete(&self, items_read: ChunkCount) -> bool;

    /// Observes the terminal outcome of the chunk attempt this policy's most
    /// recent [`begin_chunk`](Self::begin_chunk) started.
    ///
    /// The chunk runtime calls this exactly once for every attempt whose
    /// `begin_chunk` actually ran -- after that attempt's transaction has
    /// committed, rolled back, stopped, or reached an unknown commit result,
    /// and always before the next attempt's `begin_chunk`. A rollback whose
    /// own call fails is reported as [`ChunkAttemptOutcome::Unknown`] rather
    /// than suppressing this callback: the transaction's fate is no longer
    /// knowable at that point, but the policy still owes a matching `end`
    /// for the `begin` it already ran. This is never called for an attempt
    /// whose transaction failed to begin in the first place, since that
    /// attempt's `begin_chunk` never ran -- see the lifecycle contract on
    /// [`begin_chunk`](Self::begin_chunk). The default implementation does
    /// nothing, which is correct for any policy whose decisions depend only
    /// on the current attempt (every policy in this module except
    /// [`AdaptiveCompletionPolicy`], which uses this callback to promote a
    /// speculative candidate into authoritative state only once its chunk is
    /// known to have committed, never before).
    fn end_chunk(&self, _outcome: ChunkAttemptOutcome) {}

    /// Returns a canonical, deterministic description of this policy's
    /// restart-relevant *configuration*, hashed into the owning chunk
    /// definition's fingerprint so a configuration change that alters
    /// completion semantics is never mistaken for the same definition across
    /// a restart.
    ///
    /// Must depend only on how this policy was configured, never on state it
    /// observes at runtime (for example [`AdaptiveCompletionPolicy`]'s
    /// currently confirmed target): two instances configured identically
    /// must return the same string regardless of what either has observed so
    /// far, and a configuration change that changes completion behavior must
    /// change this string.
    ///
    /// # Restart-safety guarantee is only as strong as this override
    ///
    /// The framework cannot itself detect a semantic change inside an
    /// arbitrary application-supplied policy: it can only hash whatever this
    /// method returns. The default falls back to this policy's concrete type
    /// name, which distinguishes different policy *kinds* but not different
    /// *configurations* of the same kind -- so a custom policy that changes
    /// a configuration value (a threshold, a bound, anything that alters
    /// which chunks it completes) without overriding this method keeps the
    /// same fingerprint across that change, and the framework will treat the
    /// two configurations as the same restart-compatible definition even
    /// though their completion behavior differs. This is a deliberately
    /// narrow guarantee (not a compatibility break enforced by the type
    /// system): every policy in this module overrides it with its actual
    /// configuration, and a custom policy is responsible for doing the same
    /// whenever a configuration change must invalidate restart metadata.
    fn fingerprint(&self) -> String {
        std::any::type_name::<Self>().to_owned()
    }

    /// Returns this policy's own required [`ItemStream`] registration(s),
    /// bound to the exact same instance this policy makes completion
    /// decisions from -- never a second, independently constructed instance
    /// that could drift out of sync with it.
    ///
    /// Every policy in this module except [`AdaptiveCompletionPolicy`]
    /// returns an empty list: it has no persisted state of its own.
    /// [`AdaptiveCompletionPolicy`] returns exactly one entry (itself, under
    /// its own [`identity`](AdaptiveCompletionPolicy::identity)).
    /// [`CompositeCompletionPolicy`] returns the concatenation of its
    /// members' registrations, recursively -- so an adaptive policy nested
    /// anywhere inside a composite tree, not only installed directly, still
    /// gets its persisted decision wired to the same instance
    /// [`crate::ChunkStep::with_completion_policy`] installs, with no
    /// separate registration call for a caller to get wrong.
    ///
    /// A custom policy with its own restart-relevant state should override
    /// this the same way it overrides [`fingerprint`](Self::fingerprint):
    /// by returning its own registration(s) here rather than requiring a
    /// caller to register a second, possibly different, instance.
    fn stream_registrations(
        &self,
    ) -> Vec<(
        ComponentStreamIdentity,
        Arc<BoxedStream>,
        StreamStateContract,
    )> {
        Vec::new()
    }

    /// Returns this policy's own composite nesting depth: `0` for a leaf
    /// policy, or `1 + ` the greatest depth among a composite's own members.
    ///
    /// Used by [`CompositeCompletionPolicy::new`] to enforce
    /// [`MAX_COMPOSITE_DEPTH`] against direct *and* indirect nesting. The
    /// default of `0` is correct for every leaf policy; only
    /// [`CompositeCompletionPolicy`] overrides it.
    #[doc(hidden)]
    fn composite_depth(&self) -> usize {
        0
    }
}

// ---------------------------------------------------------------------
// Count
// ---------------------------------------------------------------------

/// A deterministic, item-count-bounded completion policy.
///
/// Equivalent to the [`ChunkSize`] ceiling every chunk already enforces.
/// Useful as an explicit [`CompositeCompletionPolicy`] member alongside a
/// [`TimeCompletionPolicy`], where the raw ceiling alone cannot express "stop
/// at N items *or* after duration D, whichever comes first".
#[derive(Clone, Copy, Debug)]
pub struct ItemCountCompletionPolicy(ChunkSize);

impl ItemCountCompletionPolicy {
    /// Constructs a policy that completes once `size` items are read.
    #[must_use]
    pub const fn new(size: ChunkSize) -> Self {
        Self(size)
    }

    /// Returns the configured item limit.
    #[must_use]
    pub const fn size(self) -> ChunkSize {
        self.0
    }
}

impl CompletionPolicy for ItemCountCompletionPolicy {
    fn is_complete(&self, items_read: ChunkCount) -> bool {
        items_read.get() >= u64::from(self.0.get())
    }

    fn fingerprint(&self) -> String {
        format!("count/{}", self.0.get())
    }
}

// ---------------------------------------------------------------------
// Time
// ---------------------------------------------------------------------

const MIN_TIME_THRESHOLD: Duration = Duration::from_millis(1);
const MAX_TIME_THRESHOLD: Duration = Duration::from_hours(24);

/// A bounded chunk-completion time threshold in `1 ms..=24 h`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChunkTimeThreshold(Duration);

impl ChunkTimeThreshold {
    /// Validates a completion threshold.
    ///
    /// # Errors
    ///
    /// Returns [`CompletionPolicyError::InvalidTimeThreshold`] outside
    /// `1 ms..=24 h`.
    pub fn new(value: Duration) -> Result<Self, CompletionPolicyError> {
        if !(MIN_TIME_THRESHOLD..=MAX_TIME_THRESHOLD).contains(&value) {
            return Err(CompletionPolicyError::InvalidTimeThreshold);
        }
        Ok(Self(value))
    }

    /// Returns the validated duration.
    #[must_use]
    pub const fn get(self) -> Duration {
        self.0
    }
}

/// A deterministic, clock-injected time-bounded completion policy.
///
/// `clock` is read only at [`begin_chunk`](CompletionPolicy::begin_chunk)
/// and each [`is_complete`](CompletionPolicy::is_complete) call, so a test
/// can substitute a deterministic clock (for example
/// `oxide_batch_test::ManualClock`) wherever this policy is constructed --
/// no wall-clock read is spread through runtime logic. A clock reading that
/// goes backward between those two calls is treated as zero elapsed time,
/// never a negative duration and never a panic.
///
/// This policy alone does not bound the number of items buffered while
/// waiting for the threshold to elapse; the enclosing [`ChunkSize`] ceiling
/// still applies and remains the resource-safety bound.
pub struct TimeCompletionPolicy {
    clock: Arc<dyn Clock>,
    threshold: ChunkTimeThreshold,
    started: Mutex<Option<SystemTime>>,
}

impl TimeCompletionPolicy {
    /// Constructs a policy that completes once `threshold` elapses since the
    /// current chunk attempt began.
    #[must_use]
    pub fn new(clock: Arc<dyn Clock>, threshold: ChunkTimeThreshold) -> Self {
        Self {
            clock,
            threshold,
            started: Mutex::new(None),
        }
    }
}

impl CompletionPolicy for TimeCompletionPolicy {
    fn begin_chunk(&self) {
        let mut started = self.started.lock().unwrap_or_else(PoisonError::into_inner);
        *started = Some(self.clock.now());
    }

    fn is_complete(&self, _items_read: ChunkCount) -> bool {
        let started = self.started.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(started) = *started else {
            return false;
        };
        let elapsed = self
            .clock
            .now()
            .duration_since(started)
            .unwrap_or(Duration::ZERO);
        elapsed >= self.threshold.get()
    }

    fn fingerprint(&self) -> String {
        format!("time/{}", self.threshold.get().as_nanos())
    }
}

// ---------------------------------------------------------------------
// Composite
// ---------------------------------------------------------------------

/// How a [`CompositeCompletionPolicy`] combines its members' decisions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CompositeMode {
    /// Complete as soon as any member policy completes (logical OR).
    Any,
    /// Complete only once every member policy completes (logical AND).
    All,
}

/// The largest number of members one [`CompositeCompletionPolicy`] may hold.
pub const MAX_COMPOSITE_MEMBERS: usize = 32;

/// The largest nesting depth one [`CompositeCompletionPolicy`] tree may
/// reach, counting the outermost composite as depth `1`.
///
/// Bounds direct *and* indirect nesting: a composite whose member is itself a
/// composite (at any remove) contributes its own depth plus one to its
/// parent's. [`CompositeCompletionPolicy::new`] enforces this eagerly at
/// construction, before any nested structure exists to recurse over at
/// runtime.
pub const MAX_COMPOSITE_DEPTH: usize = 8;

/// A deterministic, bounded composition of completion policies.
///
/// Composition never recurses without bound: a composite's own member count
/// is validated at construction against [`MAX_COMPOSITE_MEMBERS`], and its
/// full nesting depth (direct and indirect) is validated against
/// [`MAX_COMPOSITE_DEPTH`]. Both checks run at construction, so a
/// too-deep tree is rejected before it exists rather than discovered by
/// runtime recursion.
pub struct CompositeCompletionPolicy {
    members: Vec<Arc<dyn CompletionPolicy>>,
    mode: CompositeMode,
    depth: usize,
}

impl fmt::Debug for CompositeCompletionPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompositeCompletionPolicy")
            .field("mode", &self.mode)
            .field("members", &self.members.len())
            .field("depth", &self.depth)
            .finish()
    }
}

impl CompositeCompletionPolicy {
    /// Validates and constructs a composite policy.
    ///
    /// # Errors
    ///
    /// Returns [`CompletionPolicyError::EmptyComposite`] for an empty
    /// `members`, [`CompletionPolicyError::TooManyMembers`] above
    /// [`MAX_COMPOSITE_MEMBERS`], or [`CompletionPolicyError::CompositeTooDeep`]
    /// when nesting a member (at any remove) would exceed
    /// [`MAX_COMPOSITE_DEPTH`].
    pub fn new(
        mode: CompositeMode,
        members: Vec<Arc<dyn CompletionPolicy>>,
    ) -> Result<Self, CompletionPolicyError> {
        if members.is_empty() {
            return Err(CompletionPolicyError::EmptyComposite);
        }
        if members.len() > MAX_COMPOSITE_MEMBERS {
            return Err(CompletionPolicyError::TooManyMembers {
                max: MAX_COMPOSITE_MEMBERS,
            });
        }
        let depth = 1 + members
            .iter()
            .map(|member| member.composite_depth())
            .max()
            .unwrap_or(0);
        if depth > MAX_COMPOSITE_DEPTH {
            return Err(CompletionPolicyError::CompositeTooDeep {
                max: MAX_COMPOSITE_DEPTH,
            });
        }
        Ok(Self {
            members,
            mode,
            depth,
        })
    }
}

impl CompletionPolicy for CompositeCompletionPolicy {
    fn begin_chunk(&self) {
        for member in &self.members {
            member.begin_chunk();
        }
    }

    fn is_complete(&self, items_read: ChunkCount) -> bool {
        match self.mode {
            CompositeMode::Any => self
                .members
                .iter()
                .any(|member| member.is_complete(items_read)),
            CompositeMode::All => self
                .members
                .iter()
                .all(|member| member.is_complete(items_read)),
        }
    }

    fn end_chunk(&self, outcome: ChunkAttemptOutcome) {
        for member in &self.members {
            member.end_chunk(outcome);
        }
    }

    fn fingerprint(&self) -> String {
        // Length-prefixed (rather than delimiter-joined) so the encoding is
        // injective: a delimiter appearing inside one member's own
        // fingerprint (for example a nested composite's `[...]`) can never
        // be misread as a boundary between members. Two composites whose
        // members split their fingerprints differently -- `["a", "b,c"]`
        // vs `["a,b", "c"]` -- therefore always produce different strings.
        use std::fmt::Write as _;
        let mut members = String::new();
        for member in &self.members {
            let fingerprint = member.fingerprint();
            let _ = write!(members, "{}:{fingerprint}", fingerprint.len());
        }
        format!("composite/{:?}/[{members}]", self.mode)
    }

    fn stream_registrations(
        &self,
    ) -> Vec<(
        ComponentStreamIdentity,
        Arc<BoxedStream>,
        StreamStateContract,
    )> {
        self.members
            .iter()
            .flat_map(|member| member.stream_registrations())
            .collect()
    }

    fn composite_depth(&self) -> usize {
        self.depth
    }
}

// ---------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------

/// Stable completion-policy validation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CompletionPolicyError {
    /// A time threshold was outside `1 ms..=24 h`.
    InvalidTimeThreshold,
    /// A composite policy had no members.
    EmptyComposite,
    /// A composite policy exceeded [`MAX_COMPOSITE_MEMBERS`].
    TooManyMembers {
        /// The maximum accepted member count.
        max: usize,
    },
    /// Nesting a composite policy's member (at any remove) would exceed
    /// [`MAX_COMPOSITE_DEPTH`].
    CompositeTooDeep {
        /// The maximum accepted nesting depth.
        max: usize,
    },
    /// An adaptive policy's minimum bound exceeded its maximum bound.
    InvalidAdaptiveBounds,
}

impl fmt::Display for CompletionPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTimeThreshold => {
                formatter.write_str("completion time threshold must be within 1 ms..=24 h")
            }
            Self::EmptyComposite => {
                formatter.write_str("a composite completion policy requires at least one member")
            }
            Self::TooManyMembers { max } => write!(
                formatter,
                "a composite completion policy accepts at most {max} members"
            ),
            Self::CompositeTooDeep { max } => write!(
                formatter,
                "a composite completion policy nests at most {max} levels deep"
            ),
            Self::InvalidAdaptiveBounds => formatter
                .write_str("adaptive completion policy minimum bound exceeds its maximum bound"),
        }
    }
}

impl Error for CompletionPolicyError {}

// ---------------------------------------------------------------------
// Adaptive
// ---------------------------------------------------------------------

/// Validated `min..=max` item-count bounds for an [`AdaptiveCompletionPolicy`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdaptiveBounds {
    min: ChunkSize,
    max: ChunkSize,
}

impl AdaptiveBounds {
    /// Validates `min <= max`.
    ///
    /// # Errors
    ///
    /// Returns [`CompletionPolicyError::InvalidAdaptiveBounds`] when `min`
    /// exceeds `max`.
    pub fn new(min: ChunkSize, max: ChunkSize) -> Result<Self, CompletionPolicyError> {
        if min.get() > max.get() {
            return Err(CompletionPolicyError::InvalidAdaptiveBounds);
        }
        Ok(Self { min, max })
    }

    /// Returns the minimum accepted target size.
    #[must_use]
    pub const fn min(self) -> ChunkSize {
        self.min
    }

    /// Returns the maximum accepted target size.
    #[must_use]
    pub const fn max(self) -> ChunkSize {
        self.max
    }

    fn clamp(self, target: u32) -> ChunkSize {
        let clamped = target.clamp(self.min.get(), self.max.get());
        ChunkSize::new(clamped).unwrap_or(self.min)
    }
}

#[derive(Clone, Copy)]
struct AdaptiveDecision {
    target: ChunkSize,
}

const ADAPTIVE_SCHEMA: &str = "oxide-batch.completion.adaptive-decision";
const ADAPTIVE_CODEC: &str = "oxide-batch.completion.adaptive-decision-codec";

#[derive(Clone, Copy)]
struct AdaptiveDecisionSchema;

impl VersionedStateCodec<AdaptiveDecision> for AdaptiveDecisionSchema {
    fn schema_id(&self) -> &StateSchemaId {
        static SCHEMA: std::sync::OnceLock<StateSchemaId> = std::sync::OnceLock::new();
        #[allow(
            clippy::unwrap_used,
            reason = "fixed literal schema identity cannot fail validation"
        )]
        SCHEMA.get_or_init(|| StateSchemaId::new(ADAPTIVE_SCHEMA).unwrap())
    }

    fn current_version(&self) -> StateSchemaVersion {
        #[allow(
            clippy::unwrap_used,
            reason = "fixed literal schema version cannot fail validation"
        )]
        StateSchemaVersion::new(1).unwrap()
    }

    fn encode(&self, value: &AdaptiveDecision) -> Result<Vec<u8>, StateCodecError> {
        serde_json::to_vec(&serde_json::json!({ "target_items": value.target.get() }))
            .map_err(|_| StateCodecError::InvalidPayload)
    }

    fn decode(&self, payload: &[u8]) -> Result<AdaptiveDecision, StateCodecError> {
        let value: serde_json::Value =
            serde_json::from_slice(payload).map_err(|_| StateCodecError::InvalidPayload)?;
        let target_items = u32::try_from(
            value
                .get("target_items")
                .and_then(serde_json::Value::as_u64)
                .ok_or(StateCodecError::InvalidPayload)?,
        )
        .map_err(|_| StateCodecError::InvalidPayload)?;
        let target = ChunkSize::new(target_items).map_err(|_| StateCodecError::InvalidPayload)?;
        Ok(AdaptiveDecision { target })
    }
}

fn adaptive_decision_codec() -> DefaultComponentCodec<AdaptiveDecisionSchema> {
    #[allow(
        clippy::unwrap_used,
        reason = "fixed literal identities cannot fail validation"
    )]
    DefaultComponentCodec::new(
        AdaptiveDecisionSchema,
        CodecId::new(ADAPTIVE_CODEC).unwrap(),
        CodecVersion::new(1).unwrap(),
        RestartabilityDeclaration::Restartable,
    )
}

struct AdaptiveInterior {
    /// The authoritative target: the last value this process has observed
    /// *commit* (via [`CompletionPolicy::end_chunk`]) or restore (via
    /// [`ItemStream::open`]). [`CompletionPolicy::is_complete`] and
    /// [`AdaptiveCompletionPolicy::current_target`] read only this field --
    /// never `pending` -- so an in-flight, not-yet-committed candidate can
    /// never be observed as authoritative.
    confirmed: ChunkSize,
    /// This attempt's not-yet-committed candidate, computed by
    /// [`ItemStream::update`] from `confirmed` and this attempt's freshly
    /// observed duration. Promoted into `confirmed` by
    /// [`CompletionPolicy::end_chunk`] on [`ChunkAttemptOutcome::Committed`],
    /// and discarded on every other outcome (including by the next
    /// [`CompletionPolicy::begin_chunk`], defensively, should `end_chunk`
    /// never run for this attempt).
    pending: Option<ChunkSize>,
    chunk_started: Option<SystemTime>,
}

/// A bounded, restart-safe adaptive completion policy.
///
/// The policy adjusts its target chunk size toward `target_duration`,
/// clamped to `bounds`, based on the most recently *committed* chunk's
/// observed duration. Install it via
/// [`crate::ChunkStep::with_completion_policy`] (directly, or nested inside
/// a [`CompositeCompletionPolicy`] at any depth): [`stream_registrations`]
/// reports its namespaced [`ItemStream`] registration under the same
/// identity returned by [`identity`](Self::identity), bound to this exact
/// instance, so the confirmed target survives restart through the same
/// commit-boundary mechanism as any other component state -- never a second
/// persistence path, and never two registrations that could drift onto
/// different instances.
///
/// [`stream_registrations`]: CompletionPolicy::stream_registrations
///
/// # Restart safety
///
/// A process crash before a chunk commits leaves the previously committed
/// target authoritative: [`ItemStream::open`] restores exactly that target
/// from the durable envelope, never a value a still-open, not-yet-committed
/// attempt only speculated.
///
/// # Same-process rollback safety
///
/// [`ItemStream::update`] never mutates the confirmed target: it only
/// computes a candidate from the confirmed baseline and this attempt's
/// freshly observed duration, storing it as a separate, speculative
/// `pending` value. That candidate is promoted to `confirmed` -- becoming
/// visible to [`CompletionPolicy::is_complete`] -- only when
/// [`CompletionPolicy::end_chunk`] observes
/// [`ChunkAttemptOutcome::Committed`]; every other outcome discards it. A
/// rolled-back attempt that gets replayed therefore recomputes the identical
/// candidate from the same unmodified confirmed baseline and the same kind
/// of freshly observed metrics, so replaying is idempotent even though
/// `update` runs before the commit it is conditioned on, and a rollback can
/// never leave a speculative target looking authoritative.
pub struct AdaptiveCompletionPolicy {
    bounds: AdaptiveBounds,
    target_duration: ChunkTimeThreshold,
    clock: Arc<dyn Clock>,
    limits: StateLimits,
    identity: ComponentStreamIdentity,
    state: Mutex<AdaptiveInterior>,
    /// A weak self-reference, populated at construction via
    /// [`Arc::new_cyclic`], letting [`stream_registrations`](CompletionPolicy::stream_registrations)
    /// hand out an `Arc` that shares this exact instance's state -- never a
    /// second, independently constructed instance -- without requiring a
    /// caller to already hold one.
    self_weak: Weak<AdaptiveCompletionPolicy>,
}

impl AdaptiveCompletionPolicy {
    /// Constructs an adaptive policy starting at `bounds`'s minimum target.
    ///
    /// Returns `Arc<Self>` rather than `Self`: the policy holds a weak
    /// self-reference so [`stream_registrations`](CompletionPolicy::stream_registrations)
    /// can bind its persisted state to this exact instance regardless of
    /// whether it ends up installed directly or nested inside a
    /// [`CompositeCompletionPolicy`].
    #[must_use]
    pub fn new(
        identity: ComponentStreamIdentity,
        bounds: AdaptiveBounds,
        target_duration: ChunkTimeThreshold,
        clock: Arc<dyn Clock>,
    ) -> Arc<Self> {
        Arc::new_cyclic(|self_weak| Self {
            bounds,
            target_duration,
            clock,
            limits: StateLimits::default(),
            identity,
            state: Mutex::new(AdaptiveInterior {
                confirmed: bounds.min(),
                pending: None,
                chunk_started: None,
            }),
            self_weak: self_weak.clone(),
        })
    }

    /// Returns the namespace this policy's persisted decision is registered
    /// under.
    #[must_use]
    pub const fn identity(&self) -> &ComponentStreamIdentity {
        &self.identity
    }

    /// Returns the `StreamStateContract` matching this policy's own
    /// internal codec.
    ///
    /// This policy's schema and codec identity are its own implementation
    /// detail: a caller has no way to reconstruct a matching contract
    /// itself, so [`stream_registrations`](CompletionPolicy::stream_registrations)
    /// calls this rather than accepting a contract parameter that could
    /// mismatch.
    pub(crate) fn stream_state_contract() -> StreamStateContract {
        StreamStateContract::new(adaptive_decision_codec())
    }

    /// Returns the current confirmed target: the authoritative decision as
    /// of the last chunk this process has observed commit (or restored at
    /// [`ItemStream::open`]). Never a speculative, not-yet-committed
    /// candidate.
    #[must_use]
    pub fn current_target(&self) -> ChunkSize {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .confirmed
    }
}

impl CompletionPolicy for AdaptiveCompletionPolicy {
    fn begin_chunk(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        // Defensive: a pending candidate only survives here if this attempt's
        // `end_chunk` never ran (for example a fatal error outside the
        // ordinary commit/rollback/stop/unknown outcomes). Discarding it
        // keeps `confirmed` the sole source of truth for the attempt about
        // to begin.
        state.pending = None;
        state.chunk_started = Some(self.clock.now());
    }

    fn is_complete(&self, items_read: ChunkCount) -> bool {
        let target = self.current_target();
        items_read.get() >= u64::from(target.get())
    }

    fn end_chunk(&self, outcome: ChunkAttemptOutcome) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let pending = state.pending.take();
        if let (ChunkAttemptOutcome::Committed, Some(pending)) = (outcome, pending) {
            state.confirmed = pending;
        }
    }

    fn fingerprint(&self) -> String {
        format!(
            "adaptive/{}/{}/{}",
            self.bounds.min().get(),
            self.bounds.max().get(),
            self.target_duration.get().as_nanos()
        )
    }

    fn stream_registrations(
        &self,
    ) -> Vec<(
        ComponentStreamIdentity,
        Arc<BoxedStream>,
        StreamStateContract,
    )> {
        let Some(strong) = self.self_weak.upgrade() else {
            return Vec::new();
        };
        vec![(
            self.identity.clone(),
            Arc::new(BoxedStream::new(AdaptiveCompletionStream(strong))),
            Self::stream_state_contract(),
        )]
    }
}

/// Delegates the [`ItemStream`] contract to a shared
/// [`AdaptiveCompletionPolicy`], so [`CompletionPolicy::stream_registrations`]
/// can register the same underlying instance for both roles without exposing
/// a public `ItemStream` impl over `Arc<T>` (which would let any two
/// unrelated `Arc` clones of *different* instances be registered instead,
/// reopening the exact mistake this type exists to prevent).
struct AdaptiveCompletionStream(Arc<AdaptiveCompletionPolicy>);

impl ItemStream for AdaptiveCompletionStream {
    async fn open(
        &self,
        context: StreamOpenContext<'_>,
    ) -> Result<StreamOpenOutcome, StreamOpenError> {
        self.0.open(context).await
    }

    async fn update(
        &self,
        context: StreamUpdateContext<'_>,
    ) -> Result<ComponentStateEnvelope, StreamUpdateError> {
        self.0.update(context).await
    }

    async fn close(
        &self,
        context: StreamCloseContext<'_>,
    ) -> Result<StreamCloseOutcome, StreamCloseError> {
        self.0.close(context).await
    }
}

impl ItemStream for AdaptiveCompletionPolicy {
    async fn open(
        &self,
        context: StreamOpenContext<'_>,
    ) -> Result<StreamOpenOutcome, StreamOpenError> {
        let Some(inherited) = context.inherited_state() else {
            return Ok(StreamOpenOutcome::Initial);
        };
        let codec = adaptive_decision_codec();
        let decoded: AdaptiveDecision = inherited
            .decode(&codec)
            .map_err(|_| StreamOpenError::new())?;
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.confirmed = decoded.target;
        state.pending = None;
        Ok(StreamOpenOutcome::Restored)
    }

    async fn update(
        &self,
        _context: StreamUpdateContext<'_>,
    ) -> Result<ComponentStateEnvelope, StreamUpdateError> {
        // Pure with respect to `confirmed`: reads the confirmed baseline but
        // never writes it, so a replayed attempt (rolled back and retried
        // without an intervening commit) recomputes this exact candidate
        // again from the same baseline. Only `pending` -- never observed by
        // `is_complete` -- records the result, and only `end_chunk` may ever
        // promote it into `confirmed`.
        let next_target = {
            let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let elapsed = state.chunk_started.map(|started| {
                self.clock
                    .now()
                    .duration_since(started)
                    .unwrap_or(Duration::ZERO)
            });
            adjust_target(
                state.confirmed,
                elapsed,
                self.target_duration.get(),
                self.bounds,
            )
        };
        {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.pending = Some(next_target);
        }
        let codec = adaptive_decision_codec();
        ComponentStateEnvelope::encode(
            self.identity.clone(),
            &AdaptiveDecision {
                target: next_target,
            },
            &codec,
            self.limits,
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

/// Adjusts `confirmed` one bounded step toward `target_duration`, given the
/// just-observed chunk duration (`None` when no chunk attempt has started
/// yet in this step attempt).
///
/// Moves by roughly one eighth of the current target per attempt so the
/// policy converges smoothly instead of oscillating between the bounds, and
/// only when the observed duration is more than 20% away from the target so
/// noise near the target does not perturb the decision.
fn adjust_target(
    confirmed: ChunkSize,
    observed: Option<Duration>,
    target_duration: Duration,
    bounds: AdaptiveBounds,
) -> ChunkSize {
    let Some(observed) = observed else {
        return confirmed;
    };
    let tolerance = target_duration / 5;
    let step = (confirmed.get() / 8).max(1);
    if observed < target_duration.saturating_sub(tolerance) {
        bounds.clamp(confirmed.get().saturating_add(step))
    } else if observed > target_duration.saturating_add(tolerance) {
        bounds.clamp(confirmed.get().saturating_sub(step))
    } else {
        confirmed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_policy_boundaries() {
        #[allow(clippy::unwrap_used, reason = "test literal is a valid ChunkSize")]
        let policy = ItemCountCompletionPolicy::new(ChunkSize::new(3).unwrap());
        assert!(!policy.is_complete(ChunkCount::new(0)));
        assert!(!policy.is_complete(ChunkCount::new(2)));
        assert!(policy.is_complete(ChunkCount::new(3)));
        assert!(policy.is_complete(ChunkCount::new(4)));
    }

    #[test]
    fn time_threshold_rejects_out_of_bounds() {
        assert!(ChunkTimeThreshold::new(Duration::ZERO).is_err());
        assert!(ChunkTimeThreshold::new(Duration::from_hours(25)).is_err());
        assert!(ChunkTimeThreshold::new(Duration::from_secs(1)).is_ok());
    }

    #[test]
    #[allow(clippy::unwrap_used, reason = "test asserts the exact Err variant")]
    fn composite_rejects_empty_and_oversized() {
        assert_eq!(
            CompositeCompletionPolicy::new(CompositeMode::Any, Vec::new()).unwrap_err(),
            CompletionPolicyError::EmptyComposite
        );
        #[allow(clippy::unwrap_used, reason = "test literal is a valid ChunkSize")]
        let member: Arc<dyn CompletionPolicy> =
            Arc::new(ItemCountCompletionPolicy::new(ChunkSize::new(1).unwrap()));
        let too_many = std::iter::repeat_with(|| Arc::clone(&member))
            .take(MAX_COMPOSITE_MEMBERS + 1)
            .collect();
        assert!(matches!(
            CompositeCompletionPolicy::new(CompositeMode::Any, too_many),
            Err(CompletionPolicyError::TooManyMembers { max }) if max == MAX_COMPOSITE_MEMBERS
        ));
    }

    #[test]
    #[allow(clippy::unwrap_used, reason = "test constructs known-valid policies")]
    fn composite_any_and_all_semantics() {
        #[allow(clippy::unwrap_used, reason = "test literal is a valid ChunkSize")]
        let small: Arc<dyn CompletionPolicy> =
            Arc::new(ItemCountCompletionPolicy::new(ChunkSize::new(2).unwrap()));
        #[allow(clippy::unwrap_used, reason = "test literal is a valid ChunkSize")]
        let large: Arc<dyn CompletionPolicy> =
            Arc::new(ItemCountCompletionPolicy::new(ChunkSize::new(5).unwrap()));

        let any = CompositeCompletionPolicy::new(
            CompositeMode::Any,
            vec![Arc::clone(&small), Arc::clone(&large)],
        )
        .unwrap();
        assert!(any.is_complete(ChunkCount::new(2)));
        assert!(!any.is_complete(ChunkCount::new(1)));

        let all = CompositeCompletionPolicy::new(CompositeMode::All, vec![small, large]).unwrap();
        assert!(!all.is_complete(ChunkCount::new(2)));
        assert!(all.is_complete(ChunkCount::new(5)));
    }

    #[test]
    #[allow(clippy::unwrap_used, reason = "test asserts the exact Err variant")]
    fn adaptive_bounds_reject_min_above_max() {
        #[allow(clippy::unwrap_used, reason = "test literals are valid ChunkSizes")]
        let (min, max) = (ChunkSize::new(10).unwrap(), ChunkSize::new(5).unwrap());
        assert_eq!(
            AdaptiveBounds::new(min, max).unwrap_err(),
            CompletionPolicyError::InvalidAdaptiveBounds
        );
    }

    #[test]
    fn adjust_target_moves_toward_faster_chunks_when_slow() {
        #[allow(clippy::unwrap_used, reason = "test literals are valid ChunkSizes")]
        let bounds =
            AdaptiveBounds::new(ChunkSize::new(1).unwrap(), ChunkSize::new(100).unwrap()).unwrap();
        #[allow(clippy::unwrap_used, reason = "test literal is a valid ChunkSize")]
        let confirmed = ChunkSize::new(16).unwrap();
        let slow = adjust_target(
            confirmed,
            Some(Duration::from_secs(2)),
            Duration::from_secs(1),
            bounds,
        );
        assert!(
            slow.get() < confirmed.get(),
            "slow chunk should shrink the target"
        );

        let fast = adjust_target(
            confirmed,
            Some(Duration::from_millis(100)),
            Duration::from_secs(1),
            bounds,
        );
        assert!(
            fast.get() > confirmed.get(),
            "fast chunk should grow the target"
        );

        let on_target = adjust_target(
            confirmed,
            Some(Duration::from_secs(1)),
            Duration::from_secs(1),
            bounds,
        );
        assert_eq!(on_target.get(), confirmed.get());
    }

    #[test]
    fn adjust_target_stays_within_bounds() {
        #[allow(clippy::unwrap_used, reason = "test literals are valid ChunkSizes")]
        let bounds =
            AdaptiveBounds::new(ChunkSize::new(4).unwrap(), ChunkSize::new(8).unwrap()).unwrap();
        #[allow(clippy::unwrap_used, reason = "test literal is a valid ChunkSize")]
        let confirmed = ChunkSize::new(8).unwrap();
        let still_fast = adjust_target(
            confirmed,
            Some(Duration::from_millis(1)),
            Duration::from_secs(1),
            bounds,
        );
        assert_eq!(still_fast.get(), 8, "must not exceed the configured max");

        #[allow(clippy::unwrap_used, reason = "test literal is a valid ChunkSize")]
        let confirmed_min = ChunkSize::new(4).unwrap();
        let still_slow = adjust_target(
            confirmed_min,
            Some(Duration::from_secs(1000)),
            Duration::from_secs(1),
            bounds,
        );
        assert_eq!(still_slow.get(), 4, "must not go below the configured min");
    }

    struct ManualClock(Mutex<SystemTime>);

    impl ManualClock {
        fn new(start: SystemTime) -> Self {
            Self(Mutex::new(start))
        }

        fn advance(&self, delta: Duration) {
            let mut now = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            *now += delta;
        }
    }

    impl Clock for ManualClock {
        fn now(&self) -> SystemTime {
            *self.0.lock().unwrap_or_else(PoisonError::into_inner)
        }
    }

    fn decode_target(envelope: &ComponentStateEnvelope) -> ChunkSize {
        let codec = adaptive_decision_codec();
        #[allow(
            clippy::unwrap_used,
            reason = "test decodes a value this test just encoded"
        )]
        let decoded: AdaptiveDecision = envelope.decode(&codec).unwrap();
        decoded.target
    }

    #[test]
    #[allow(clippy::unwrap_used, reason = "test builds a controlled nesting chain")]
    fn composite_depth_boundary_and_overflow() {
        #[allow(clippy::unwrap_used, reason = "test literal is a valid ChunkSize")]
        let mut current: Arc<dyn CompletionPolicy> =
            Arc::new(ItemCountCompletionPolicy::new(ChunkSize::new(1).unwrap()));
        for depth in 1..=MAX_COMPOSITE_DEPTH {
            let composite =
                CompositeCompletionPolicy::new(CompositeMode::Any, vec![current]).unwrap();
            assert_eq!(composite.composite_depth(), depth);
            current = Arc::new(composite);
        }
        // One more level of nesting exceeds MAX_COMPOSITE_DEPTH.
        assert!(matches!(
            CompositeCompletionPolicy::new(CompositeMode::Any, vec![current]),
            Err(CompletionPolicyError::CompositeTooDeep { max }) if max == MAX_COMPOSITE_DEPTH
        ));
    }

    #[test]
    #[allow(clippy::unwrap_used, reason = "test builds a controlled nesting chain")]
    fn composite_depth_bounded_via_indirect_nesting() {
        #[allow(clippy::unwrap_used, reason = "test literal is a valid ChunkSize")]
        let leaf: Arc<dyn CompletionPolicy> =
            Arc::new(ItemCountCompletionPolicy::new(ChunkSize::new(1).unwrap()));
        let mut deep = Arc::clone(&leaf);
        for _ in 0..MAX_COMPOSITE_DEPTH - 1 {
            deep =
                Arc::new(CompositeCompletionPolicy::new(CompositeMode::Any, vec![deep]).unwrap());
        }
        // `deep` has composite_depth() == MAX_COMPOSITE_DEPTH - 1.
        let at_boundary =
            CompositeCompletionPolicy::new(CompositeMode::All, vec![leaf.clone(), deep.clone()])
                .unwrap();
        assert_eq!(at_boundary.composite_depth(), MAX_COMPOSITE_DEPTH);

        let one_level_deeper =
            Arc::new(CompositeCompletionPolicy::new(CompositeMode::Any, vec![deep]).unwrap());
        assert!(matches!(
            CompositeCompletionPolicy::new(CompositeMode::All, vec![leaf, one_level_deeper]),
            Err(CompletionPolicyError::CompositeTooDeep { max }) if max == MAX_COMPOSITE_DEPTH
        ));
    }

    #[test]
    #[allow(
        clippy::unwrap_used,
        reason = "test drives a controlled adaptive policy"
    )]
    fn adaptive_update_never_mutates_confirmed_and_end_chunk_gates_promotion() {
        #[allow(clippy::unwrap_used, reason = "test literal is a valid identity")]
        let identity = ComponentStreamIdentity::new("test.adaptive").unwrap();
        #[allow(clippy::unwrap_used, reason = "test literals are valid ChunkSizes")]
        let bounds =
            AdaptiveBounds::new(ChunkSize::new(1).unwrap(), ChunkSize::new(100).unwrap()).unwrap();
        #[allow(clippy::unwrap_used, reason = "test literal is a valid threshold")]
        let target_duration = ChunkTimeThreshold::new(Duration::from_secs(1)).unwrap();
        let clock = Arc::new(ManualClock::new(SystemTime::UNIX_EPOCH));
        let policy = AdaptiveCompletionPolicy::new(
            identity,
            bounds,
            target_duration,
            Arc::clone(&clock) as Arc<dyn Clock>,
        );
        let (_source, stop) = crate::StopSource::new();
        let initial = policy.current_target();

        // Attempt 1: a fast chunk, then rolled back.
        policy.begin_chunk();
        clock.advance(Duration::from_millis(50));
        #[allow(clippy::unwrap_used, reason = "test drives a known-valid update")]
        let candidate_1 =
            futures_executor::block_on(policy.update(StreamUpdateContext::new(&stop))).unwrap();
        assert_eq!(
            policy.current_target(),
            initial,
            "update must never mutate the confirmed target before commit"
        );
        policy.end_chunk(ChunkAttemptOutcome::RolledBack);
        assert_eq!(
            policy.current_target(),
            initial,
            "a rolled-back attempt must leave confirmed exactly as it was"
        );

        // Attempt 2 (replay of the same logical chunk): identical inputs must
        // recompute the identical candidate from the same unmodified baseline.
        policy.begin_chunk();
        clock.advance(Duration::from_millis(50));
        #[allow(clippy::unwrap_used, reason = "test drives a known-valid update")]
        let candidate_2 =
            futures_executor::block_on(policy.update(StreamUpdateContext::new(&stop))).unwrap();
        assert_eq!(
            decode_target(&candidate_1),
            decode_target(&candidate_2),
            "a replayed attempt must recompute the identical candidate"
        );
        assert_ne!(
            decode_target(&candidate_2).get(),
            initial.get(),
            "the fast chunk should have produced a larger candidate than the baseline"
        );

        // Committing promotes the pending candidate, and only the pending one.
        policy.end_chunk(ChunkAttemptOutcome::Committed);
        assert_eq!(
            policy.current_target(),
            decode_target(&candidate_2),
            "a committed attempt must promote its candidate to confirmed"
        );

        // Attempt 3: a further update recomputes from the new baseline, but an
        // `Unknown` outcome must not promote it either.
        policy.begin_chunk();
        #[allow(clippy::unwrap_used, reason = "test drives a known-valid update")]
        let _candidate_3 =
            futures_executor::block_on(policy.update(StreamUpdateContext::new(&stop))).unwrap();
        let confirmed_after_commit = policy.current_target();
        policy.end_chunk(ChunkAttemptOutcome::Unknown);
        assert_eq!(
            policy.current_target(),
            confirmed_after_commit,
            "an unknown commit outcome must not promote a speculative candidate"
        );
    }

    #[test]
    #[allow(
        clippy::unwrap_used,
        reason = "test drives a controlled adaptive policy"
    )]
    fn adaptive_open_restores_confirmed_and_discards_stale_pending() {
        #[allow(clippy::unwrap_used, reason = "test literal is a valid identity")]
        let identity = ComponentStreamIdentity::new("test.adaptive").unwrap();
        #[allow(clippy::unwrap_used, reason = "test literals are valid ChunkSizes")]
        let bounds =
            AdaptiveBounds::new(ChunkSize::new(1).unwrap(), ChunkSize::new(100).unwrap()).unwrap();
        #[allow(clippy::unwrap_used, reason = "test literal is a valid threshold")]
        let target_duration = ChunkTimeThreshold::new(Duration::from_secs(1)).unwrap();
        let clock = Arc::new(ManualClock::new(SystemTime::UNIX_EPOCH));
        let policy = AdaptiveCompletionPolicy::new(
            identity.clone(),
            bounds,
            target_duration,
            Arc::clone(&clock) as Arc<dyn Clock>,
        );
        let (_source, stop) = crate::StopSource::new();

        // Produce a pending candidate that never gets confirmed.
        policy.begin_chunk();
        clock.advance(Duration::from_millis(1));
        #[allow(clippy::unwrap_used, reason = "test drives a known-valid update")]
        let _pending =
            futures_executor::block_on(policy.update(StreamUpdateContext::new(&stop))).unwrap();

        #[allow(clippy::unwrap_used, reason = "test literal is a valid ChunkSize")]
        let restored_target = ChunkSize::new(42).unwrap();
        let codec = adaptive_decision_codec();
        #[allow(clippy::unwrap_used, reason = "test builds a known-valid envelope")]
        let inherited = ComponentStateEnvelope::encode(
            identity,
            &AdaptiveDecision {
                target: restored_target,
            },
            &codec,
            StateLimits::default(),
        )
        .unwrap();
        #[allow(clippy::unwrap_used, reason = "test drives a known-valid open")]
        futures_executor::block_on(policy.open(StreamOpenContext::new(Some(&inherited), &stop)))
            .unwrap();
        assert_eq!(
            policy.current_target(),
            restored_target,
            "open must restore the durable envelope's target"
        );

        // The pending candidate from before `open` must never resurface.
        policy.end_chunk(ChunkAttemptOutcome::Committed);
        assert_eq!(
            policy.current_target(),
            restored_target,
            "a pending candidate predating `open` must never be promoted"
        );
    }

    #[test]
    #[allow(clippy::unwrap_used, reason = "test constructs known-valid policies")]
    fn fingerprint_is_deterministic_for_identical_configuration() {
        #[allow(clippy::unwrap_used, reason = "test literal is a valid ChunkSize")]
        let a = ItemCountCompletionPolicy::new(ChunkSize::new(7).unwrap());
        #[allow(clippy::unwrap_used, reason = "test literal is a valid ChunkSize")]
        let b = ItemCountCompletionPolicy::new(ChunkSize::new(7).unwrap());
        assert_eq!(a.fingerprint(), b.fingerprint());

        #[allow(clippy::unwrap_used, reason = "test literal is a valid threshold")]
        let threshold = ChunkTimeThreshold::new(Duration::from_secs(3)).unwrap();
        let clock_a: Arc<dyn Clock> = Arc::new(ManualClock::new(SystemTime::UNIX_EPOCH));
        let clock_b: Arc<dyn Clock> = Arc::new(ManualClock::new(SystemTime::UNIX_EPOCH));
        let time_a = TimeCompletionPolicy::new(clock_a, threshold);
        let time_b = TimeCompletionPolicy::new(clock_b, threshold);
        assert_eq!(
            time_a.fingerprint(),
            time_b.fingerprint(),
            "the injected clock is runtime state, not configuration"
        );

        #[allow(clippy::unwrap_used, reason = "test literals are valid ChunkSizes")]
        let bounds =
            AdaptiveBounds::new(ChunkSize::new(2).unwrap(), ChunkSize::new(9).unwrap()).unwrap();
        #[allow(clippy::unwrap_used, reason = "test literal is a valid identity")]
        let adaptive_a = AdaptiveCompletionPolicy::new(
            ComponentStreamIdentity::new("a").unwrap(),
            bounds,
            threshold,
            Arc::new(ManualClock::new(SystemTime::UNIX_EPOCH)),
        );
        #[allow(clippy::unwrap_used, reason = "test literal is a valid identity")]
        let adaptive_b = AdaptiveCompletionPolicy::new(
            ComponentStreamIdentity::new("b").unwrap(),
            bounds,
            threshold,
            Arc::new(ManualClock::new(SystemTime::UNIX_EPOCH)),
        );
        assert_eq!(
            adaptive_a.fingerprint(),
            adaptive_b.fingerprint(),
            "identity and clock are runtime wiring, not configuration"
        );
    }

    #[test]
    #[allow(clippy::unwrap_used, reason = "test constructs known-valid policies")]
    fn fingerprint_changes_with_configuration() {
        #[allow(clippy::unwrap_used, reason = "test literal is a valid ChunkSize")]
        let count_5 = ItemCountCompletionPolicy::new(ChunkSize::new(5).unwrap());
        #[allow(clippy::unwrap_used, reason = "test literal is a valid ChunkSize")]
        let count_6 = ItemCountCompletionPolicy::new(ChunkSize::new(6).unwrap());
        assert_ne!(count_5.fingerprint(), count_6.fingerprint());

        #[allow(clippy::unwrap_used, reason = "test literal is a valid ChunkSize")]
        let small: Arc<dyn CompletionPolicy> =
            Arc::new(ItemCountCompletionPolicy::new(ChunkSize::new(2).unwrap()));
        #[allow(clippy::unwrap_used, reason = "test literal is a valid ChunkSize")]
        let large: Arc<dyn CompletionPolicy> =
            Arc::new(ItemCountCompletionPolicy::new(ChunkSize::new(5).unwrap()));
        #[allow(
            clippy::unwrap_used,
            reason = "test constructs a known-valid composite"
        )]
        let any = CompositeCompletionPolicy::new(
            CompositeMode::Any,
            vec![Arc::clone(&small), Arc::clone(&large)],
        )
        .unwrap();
        #[allow(
            clippy::unwrap_used,
            reason = "test constructs a known-valid composite"
        )]
        let all = CompositeCompletionPolicy::new(CompositeMode::All, vec![small, large]).unwrap();
        assert_ne!(
            any.fingerprint(),
            all.fingerprint(),
            "Any vs All must fingerprint differently even with the same members"
        );

        #[allow(clippy::unwrap_used, reason = "test literals are valid ChunkSizes")]
        let bounds_a =
            AdaptiveBounds::new(ChunkSize::new(1).unwrap(), ChunkSize::new(10).unwrap()).unwrap();
        #[allow(clippy::unwrap_used, reason = "test literals are valid ChunkSizes")]
        let bounds_b =
            AdaptiveBounds::new(ChunkSize::new(1).unwrap(), ChunkSize::new(20).unwrap()).unwrap();
        #[allow(clippy::unwrap_used, reason = "test literal is a valid threshold")]
        let threshold = ChunkTimeThreshold::new(Duration::from_secs(1)).unwrap();
        #[allow(clippy::unwrap_used, reason = "test literal is a valid identity")]
        let adaptive_a = AdaptiveCompletionPolicy::new(
            ComponentStreamIdentity::new("same").unwrap(),
            bounds_a,
            threshold,
            Arc::new(ManualClock::new(SystemTime::UNIX_EPOCH)),
        );
        #[allow(clippy::unwrap_used, reason = "test literal is a valid identity")]
        let adaptive_b = AdaptiveCompletionPolicy::new(
            ComponentStreamIdentity::new("same").unwrap(),
            bounds_b,
            threshold,
            Arc::new(ManualClock::new(SystemTime::UNIX_EPOCH)),
        );
        assert_ne!(
            adaptive_a.fingerprint(),
            adaptive_b.fingerprint(),
            "a bounds change must change the fingerprint"
        );

        // Nested composite structure must be reflected too.
        #[allow(clippy::unwrap_used, reason = "test literal is a valid ChunkSize")]
        let nested_leaf: Arc<dyn CompletionPolicy> =
            Arc::new(ItemCountCompletionPolicy::new(ChunkSize::new(2).unwrap()));
        #[allow(
            clippy::unwrap_used,
            reason = "test constructs a known-valid composite"
        )]
        let inner_any =
            CompositeCompletionPolicy::new(CompositeMode::Any, vec![Arc::clone(&nested_leaf)])
                .unwrap();
        #[allow(
            clippy::unwrap_used,
            reason = "test constructs a known-valid composite"
        )]
        let inner_all =
            CompositeCompletionPolicy::new(CompositeMode::All, vec![nested_leaf]).unwrap();
        #[allow(clippy::unwrap_used, reason = "test literal is a valid ChunkSize")]
        let sibling: Arc<dyn CompletionPolicy> =
            Arc::new(ItemCountCompletionPolicy::new(ChunkSize::new(3).unwrap()));
        #[allow(
            clippy::unwrap_used,
            reason = "test constructs a known-valid composite"
        )]
        let outer_a = CompositeCompletionPolicy::new(
            CompositeMode::All,
            vec![Arc::new(inner_any), Arc::clone(&sibling)],
        )
        .unwrap();
        #[allow(
            clippy::unwrap_used,
            reason = "test constructs a known-valid composite"
        )]
        let outer_b =
            CompositeCompletionPolicy::new(CompositeMode::All, vec![Arc::new(inner_all), sibling])
                .unwrap();
        assert_ne!(
            outer_a.fingerprint(),
            outer_b.fingerprint(),
            "a mode change nested inside a composite must change the outer fingerprint"
        );
    }

    /// A leaf policy whose `fingerprint()` is a fixed test literal, standing
    /// in for a custom policy's own configuration string.
    struct FixedFingerprint(&'static str);

    impl CompletionPolicy for FixedFingerprint {
        fn is_complete(&self, _items_read: ChunkCount) -> bool {
            false
        }

        fn fingerprint(&self) -> String {
            self.0.to_owned()
        }
    }

    #[test]
    #[allow(clippy::unwrap_used, reason = "test constructs known-valid composites")]
    fn composite_fingerprint_encoding_is_injective_across_member_splits() {
        // A naive `join(",")` encoding cannot distinguish a composite whose
        // members are `["a", "b,c"]` from one whose members are `["a,b",
        // "c"]": both join to the same `"a,b,c"` string. The length-prefixed
        // encoding must keep them apart.
        let split_a: Vec<Arc<dyn CompletionPolicy>> = vec![
            Arc::new(FixedFingerprint("a")),
            Arc::new(FixedFingerprint("b,c")),
        ];
        let split_b: Vec<Arc<dyn CompletionPolicy>> = vec![
            Arc::new(FixedFingerprint("a,b")),
            Arc::new(FixedFingerprint("c")),
        ];
        let composite_a = CompositeCompletionPolicy::new(CompositeMode::Any, split_a).unwrap();
        let composite_b = CompositeCompletionPolicy::new(CompositeMode::Any, split_b).unwrap();
        assert_ne!(
            composite_a.fingerprint(),
            composite_b.fingerprint(),
            "different member splits must never collide onto the same \
             composite fingerprint, even when a member's own fingerprint \
             contains the naive join delimiter"
        );
    }
}
