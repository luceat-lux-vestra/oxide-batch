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
//!   size adapts toward an observed target duration. Its authoritative
//!   decision is persisted through the same [`crate::ItemStream`] contract
//!   as any other component state; there is no second persistence path.
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
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime};

use crate::{
    ChunkCount, ChunkSize, Clock, CodecId, CodecVersion, ComponentStateEnvelope,
    ComponentStreamIdentity, DefaultComponentCodec, ItemStream, RestartabilityDeclaration,
    StateCodecError, StateLimits, StateSchemaId, StateSchemaVersion, StreamCloseContext,
    StreamCloseError, StreamCloseOutcome, StreamOpenContext, StreamOpenError, StreamOpenOutcome,
    StreamUpdateContext, StreamUpdateError, VersionedStateCodec,
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
    /// The default implementation does nothing, which is correct for a
    /// stateless policy such as [`ItemCountCompletionPolicy`].
    fn begin_chunk(&self) {}

    /// Reports whether the chunk should stop accepting more items.
    ///
    /// `items_read` is the number of items already read into the current
    /// chunk attempt.
    fn is_complete(&self, items_read: ChunkCount) -> bool;
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

/// A deterministic, bounded composition of completion policies.
///
/// Composition never recurses without bound: a composite's own member count
/// is validated at construction against [`MAX_COMPOSITE_MEMBERS`]. Nesting
/// one composite inside another is bounded the same way at every level, so
/// there is no unbounded or implicit recursive structure.
pub struct CompositeCompletionPolicy {
    members: Vec<Arc<dyn CompletionPolicy>>,
    mode: CompositeMode,
}

impl fmt::Debug for CompositeCompletionPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompositeCompletionPolicy")
            .field("mode", &self.mode)
            .field("members", &self.members.len())
            .finish()
    }
}

impl CompositeCompletionPolicy {
    /// Validates and constructs a composite policy.
    ///
    /// # Errors
    ///
    /// Returns [`CompletionPolicyError::EmptyComposite`] for an empty
    /// `members`, or [`CompletionPolicyError::TooManyMembers`] above
    /// [`MAX_COMPOSITE_MEMBERS`].
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
        Ok(Self { members, mode })
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
    confirmed: ChunkSize,
    chunk_started: Option<SystemTime>,
}

/// A bounded, restart-safe adaptive completion policy.
///
/// The policy adjusts its target chunk size toward `target_duration`,
/// clamped to `bounds`, based on the most recently *committed* chunk's
/// observed duration. Register this policy as both the step's completion
/// policy (via [`crate::ChunkStep::with_completion_policy`]) and as a
/// namespaced [`ItemStream`] (via [`crate::ChunkStep::with_item_stream`])
/// under the same identity returned by [`identity`](Self::identity), so the
/// confirmed target survives restart through the same commit-boundary
/// mechanism as any other component state -- never a second persistence
/// path.
///
/// # Restart safety
///
/// A process crash before a chunk commits leaves the previously committed
/// target authoritative: [`ItemStream::open`] restores exactly that target
/// from the durable envelope, never a value a still-open, not-yet-committed
/// attempt only speculated. A rolled-back attempt that gets replayed
/// recomputes the identical candidate from the same confirmed baseline and
/// the same freshly observed metrics, so replaying is idempotent even though
/// [`ItemStream::update`] runs before the commit it is conditioned on.
pub struct AdaptiveCompletionPolicy {
    bounds: AdaptiveBounds,
    target_duration: ChunkTimeThreshold,
    clock: Arc<dyn Clock>,
    limits: StateLimits,
    identity: ComponentStreamIdentity,
    state: Mutex<AdaptiveInterior>,
}

impl AdaptiveCompletionPolicy {
    /// Constructs an adaptive policy starting at `bounds`'s minimum target.
    #[must_use]
    pub fn new(
        identity: ComponentStreamIdentity,
        bounds: AdaptiveBounds,
        target_duration: ChunkTimeThreshold,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            bounds,
            target_duration,
            clock,
            limits: StateLimits::default(),
            identity,
            state: Mutex::new(AdaptiveInterior {
                confirmed: bounds.min(),
                chunk_started: None,
            }),
        }
    }

    /// Returns the namespace this policy's persisted decision is registered
    /// under.
    #[must_use]
    pub const fn identity(&self) -> &ComponentStreamIdentity {
        &self.identity
    }

    /// Returns the current confirmed target: the authoritative decision as
    /// of the last chunk this process has observed commit (or restored at
    /// [`ItemStream::open`]).
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
        state.chunk_started = Some(self.clock.now());
    }

    fn is_complete(&self, items_read: ChunkCount) -> bool {
        let target = self.current_target();
        items_read.get() >= u64::from(target.get())
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
        Ok(StreamOpenOutcome::Restored)
    }

    async fn update(
        &self,
        _context: StreamUpdateContext<'_>,
    ) -> Result<ComponentStateEnvelope, StreamUpdateError> {
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
            state.confirmed = next_target;
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
}
