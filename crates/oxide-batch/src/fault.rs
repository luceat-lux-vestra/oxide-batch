//! Runtime-neutral retry, backoff, skip, and rollback policy contracts.
//!
//! These types implement the M3 slice of the accepted fault-tolerance
//! contract. They are pure values and traits: they neither touch a repository
//! nor start component work. Chunk integration, durable reservation, and
//! `PostgreSQL` state remain owned by later M3 workstreams.

use std::error::Error;
use std::fmt;
use std::time::Duration;

use crate::{
    BoxFuture, ChunkDeliveryMode, ClassifierRevision, FailureCategory, FailureId, FailureSummary,
    StopToken,
};

/// The largest accepted backoff delay.
const MAX_BACKOFF: Duration = Duration::from_hours(24);
/// The largest accepted retry limit and retry ordinal.
const MAX_RETRY: u32 = 65_535;
/// The smallest accepted unresolved retry-state capacity.
const MIN_RETRY_STATE: u32 = 1;
/// The largest accepted unresolved retry-state capacity.
const MAX_RETRY_STATE: u32 = 256;

/// The framework phase that produced a fault.
///
/// The phase is framework-owned input for classification. It never carries an
/// item value, error payload, or component-private state.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum FaultPhase {
    /// An item reader failed.
    Read,
    /// An item processor failed.
    Process,
    /// An item writer failed.
    Write,
    /// A chunk transaction failed to begin, commit, or roll back.
    Transaction,
    /// Checkpoint or execution-context state could not be produced or stored.
    Checkpoint,
    /// An authoritative listener callback failed or panicked.
    Listener,
    /// Cancellable backoff failed or was interrupted.
    Backoff,
}

impl FaultPhase {
    /// Returns whether a classifier rule may govern this phase.
    ///
    /// Listener failures are never retried or skipped in M3.
    #[must_use]
    pub const fn is_policy_eligible(self) -> bool {
        !matches!(self, Self::Listener)
    }

    /// Returns whether a committed skip can be counted for this phase.
    #[must_use]
    pub const fn is_skippable(self) -> bool {
        matches!(self, Self::Read | Self::Process | Self::Write)
    }

    /// Returns whether the phase can structurally accept a commit-safe skip.
    ///
    /// Only read and process faults occur before any external write effect.
    #[must_use]
    pub const fn allows_commit_safe_skip(self) -> bool {
        matches!(self, Self::Read | Self::Process)
    }

    /// Returns the stable low-cardinality telemetry name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Process => "process",
            Self::Write => "write",
            Self::Transaction => "transaction",
            Self::Checkpoint => "checkpoint",
            Self::Listener => "listener",
            Self::Backoff => "backoff",
        }
    }

    /// Returns the phase for one persisted name, rejecting unknown values.
    ///
    /// The names are durable fault-state data and are never renamed.
    pub(crate) fn from_durable_name(value: &str) -> Option<Self> {
        Some(match value {
            "read" => Self::Read,
            "process" => Self::Process,
            "write" => Self::Write,
            "transaction" => Self::Transaction,
            "checkpoint" => Self::Checkpoint,
            "listener" => Self::Listener,
            "backoff" => Self::Backoff,
            _ => return None,
        })
    }
}

impl fmt::Display for FaultPhase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// The zero-based invocation ordinal for one retry key.
///
/// [`RetryOrdinal::INITIAL`] identifies the first component call, which is not
/// a retry. Ordinal `r` identifies the `r`-th re-invocation.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RetryOrdinal(u16);

impl RetryOrdinal {
    /// The ordinal of the initial, non-retried component call.
    pub const INITIAL: Self = Self(0);

    /// Validates and constructs a retry ordinal.
    ///
    /// # Errors
    ///
    /// Returns [`FaultPolicyError::RetryOrdinalOutOfRange`] above 65,535.
    pub fn new(value: u32) -> Result<Self, FaultPolicyError> {
        u16::try_from(value)
            .map(Self)
            .map_err(|_| FaultPolicyError::RetryOrdinalOutOfRange { max: MAX_RETRY })
    }

    /// Returns the ordinal value.
    #[must_use]
    #[allow(
        clippy::cast_lossless,
        reason = "`From` is not const; the widening cast is exact"
    )]
    pub const fn get(self) -> u32 {
        self.0 as u32
    }

    /// Returns whether this is the initial, non-retried call.
    #[must_use]
    pub const fn is_initial(self) -> bool {
        self.0 == 0
    }

    /// Returns the next ordinal.
    ///
    /// # Errors
    ///
    /// Returns [`FaultPolicyError::RetryOrdinalOutOfRange`] instead of
    /// wrapping past the bounded representation.
    pub fn checked_next(self) -> Result<Self, FaultPolicyError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(FaultPolicyError::RetryOrdinalOutOfRange { max: MAX_RETRY })
    }
}

/// The maximum number of re-invocations after the initial component call.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RetryLimit(u16);

impl RetryLimit {
    /// A policy that never retries.
    pub const NONE: Self = Self(0);

    /// Validates and constructs a bounded retry limit.
    ///
    /// # Errors
    ///
    /// Returns [`FaultPolicyError::RetryLimitOutOfRange`] above 65,535.
    pub fn new(value: u32) -> Result<Self, FaultPolicyError> {
        u16::try_from(value)
            .map(Self)
            .map_err(|_| FaultPolicyError::RetryLimitOutOfRange { max: MAX_RETRY })
    }

    /// Returns the configured limit.
    #[must_use]
    #[allow(
        clippy::cast_lossless,
        reason = "`From` is not const; the widening cast is exact"
    )]
    pub const fn get(self) -> u32 {
        self.0 as u32
    }

    /// Returns whether retry is disabled.
    #[must_use]
    pub const fn is_none(self) -> bool {
        self.0 == 0
    }

    /// Returns whether `ordinal` may still be reserved.
    ///
    /// The initial call is not a retry, so it is never permitted here.
    #[must_use]
    pub const fn permits(self, ordinal: RetryOrdinal) -> bool {
        !ordinal.is_initial() && ordinal.0 <= self.0
    }
}

/// The maximum number of unresolved retry keys retained for one step.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RetryStateLimit(u16);

impl RetryStateLimit {
    /// Validates and constructs the bounded unresolved-key capacity.
    ///
    /// # Errors
    ///
    /// Returns [`FaultPolicyError::RetryStateLimitOutOfRange`] outside
    /// `1..=256`. A definition must choose the bound explicitly.
    pub fn new(value: u32) -> Result<Self, FaultPolicyError> {
        if !(MIN_RETRY_STATE..=MAX_RETRY_STATE).contains(&value) {
            return Err(FaultPolicyError::RetryStateLimitOutOfRange {
                min: MIN_RETRY_STATE,
                max: MAX_RETRY_STATE,
            });
        }
        u16::try_from(value)
            .map(Self)
            .map_err(|_| FaultPolicyError::RetryStateLimitOutOfRange {
                min: MIN_RETRY_STATE,
                max: MAX_RETRY_STATE,
            })
    }

    /// Returns the configured capacity.
    #[must_use]
    #[allow(
        clippy::cast_lossless,
        reason = "`From` is not const; the widening cast is exact"
    )]
    pub const fn get(self) -> u32 {
        self.0 as u32
    }
}

/// The maximum aggregate committed skips for one step in one job instance.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SkipLimit(u64);

impl SkipLimit {
    /// A policy that permits no committed skip.
    pub const NONE: Self = Self(0);

    /// Constructs an aggregate skip limit.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the configured limit.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Durable committed skip counts, kept distinct per phase.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SkipCounts {
    read: u64,
    process: u64,
    write: u64,
}

impl SkipCounts {
    /// Counts inherited by a first attempt.
    pub const ZERO: Self = Self {
        read: 0,
        process: 0,
        write: 0,
    };

    /// Constructs committed per-phase skip counts.
    #[must_use]
    pub const fn new(read: u64, process: u64, write: u64) -> Self {
        Self {
            read,
            process,
            write,
        }
    }

    /// Returns committed read skips.
    #[must_use]
    pub const fn read(self) -> u64 {
        self.read
    }

    /// Returns committed process skips.
    #[must_use]
    pub const fn process(self) -> u64 {
        self.process
    }

    /// Returns committed write skips.
    #[must_use]
    pub const fn write(self) -> u64 {
        self.write
    }

    /// Returns the checked aggregate used by the shared skip limit.
    ///
    /// # Errors
    ///
    /// Returns [`FaultPolicyError::SkipCountOverflow`] instead of wrapping.
    pub fn checked_total(self) -> Result<u64, FaultPolicyError> {
        self.read
            .checked_add(self.process)
            .and_then(|partial| partial.checked_add(self.write))
            .ok_or(FaultPolicyError::SkipCountOverflow)
    }

    /// Returns the totals after adding one chunk's committed skips.
    ///
    /// # Errors
    ///
    /// Returns [`FaultPolicyError::SkipCountOverflow`] instead of wrapping.
    pub fn checked_add(self, other: Self) -> Result<Self, FaultPolicyError> {
        let next = Self {
            read: self
                .read
                .checked_add(other.read)
                .ok_or(FaultPolicyError::SkipCountOverflow)?,
            process: self
                .process
                .checked_add(other.process)
                .ok_or(FaultPolicyError::SkipCountOverflow)?,
            write: self
                .write
                .checked_add(other.write)
                .ok_or(FaultPolicyError::SkipCountOverflow)?,
        };
        next.checked_total()?;
        Ok(next)
    }

    /// Returns the counts after one committed skip in `phase`.
    ///
    /// # Errors
    ///
    /// Returns [`FaultPolicyError::PhaseNotSkippable`] for a phase that cannot
    /// commit a skip, and [`FaultPolicyError::SkipCountOverflow`] instead of
    /// wrapping.
    pub fn checked_increment(self, phase: FaultPhase) -> Result<Self, FaultPolicyError> {
        let mut next = self;
        let counter = match phase {
            FaultPhase::Read => &mut next.read,
            FaultPhase::Process => &mut next.process,
            FaultPhase::Write => &mut next.write,
            other => return Err(FaultPolicyError::PhaseNotSkippable { phase: other }),
        };
        *counter = counter
            .checked_add(1)
            .ok_or(FaultPolicyError::SkipCountOverflow)?;
        next.checked_total()?;
        Ok(next)
    }
}

/// The complete framework-owned classification input for one fault.
///
/// The descriptor deliberately excludes error text, source chains, item
/// values, parameters, context values, and component-private state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FaultDescriptor {
    phase: FaultPhase,
    summary: FailureSummary,
    retry_ordinal: RetryOrdinal,
    committed_skips: SkipCounts,
    transaction_open: bool,
    delivery_mode: ChunkDeliveryMode,
}

impl FaultDescriptor {
    /// Constructs the bounded classification input.
    #[must_use]
    pub const fn new(
        phase: FaultPhase,
        summary: FailureSummary,
        retry_ordinal: RetryOrdinal,
        committed_skips: SkipCounts,
        transaction_open: bool,
        delivery_mode: ChunkDeliveryMode,
    ) -> Self {
        Self {
            phase,
            summary,
            retry_ordinal,
            committed_skips,
            transaction_open,
            delivery_mode,
        }
    }

    /// Returns the framework phase that produced the fault.
    #[must_use]
    pub const fn phase(self) -> FaultPhase {
        self.phase
    }

    /// Returns the redacted failure summary.
    #[must_use]
    pub const fn summary(self) -> FailureSummary {
        self.summary
    }

    /// Returns the stable failure category.
    #[must_use]
    pub const fn category(self) -> FailureCategory {
        self.summary.category()
    }

    /// Returns the opaque diagnostic correlation identifier.
    #[must_use]
    pub const fn failure_id(self) -> FailureId {
        self.summary.failure_id()
    }

    /// Returns the ordinal of the invocation that failed.
    #[must_use]
    pub const fn retry_ordinal(self) -> RetryOrdinal {
        self.retry_ordinal
    }

    /// Returns the durable committed skip counts inherited by this attempt.
    #[must_use]
    pub const fn committed_skips(self) -> SkipCounts {
        self.committed_skips
    }

    /// Returns whether a chunk transaction was open when the fault occurred.
    #[must_use]
    pub const fn is_transaction_open(self) -> bool {
        self.transaction_open
    }

    /// Returns the delivery mode declared by the step definition.
    #[must_use]
    pub const fn delivery_mode(self) -> ChunkDeliveryMode {
        self.delivery_mode
    }
}

/// The deterministic backoff family selected by a definition.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum BackoffKind {
    /// Retry immediately.
    None,
    /// Wait the same delay before every retry.
    Fixed,
    /// Multiply the initial delay by an integer factor, capped at a maximum.
    Exponential,
}

impl BackoffKind {
    /// Returns the stable low-cardinality telemetry name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Fixed => "fixed",
            Self::Exponential => "exponential",
        }
    }
}

impl fmt::Display for BackoffKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A deterministic, jitter-free backoff schedule.
///
/// Every delay is derived only from the fingerprinted policy and the retry
/// ordinal, so a restart reproduces the same schedule.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct BackoffPolicy {
    kind: BackoffKind,
    initial: Duration,
    multiplier: u32,
    maximum: Duration,
}

impl BackoffPolicy {
    /// Returns the immediate-retry schedule.
    #[must_use]
    pub const fn none() -> Self {
        Self {
            kind: BackoffKind::None,
            initial: Duration::ZERO,
            multiplier: 1,
            maximum: Duration::ZERO,
        }
    }

    /// Returns a constant-delay schedule.
    ///
    /// # Errors
    ///
    /// Returns [`FaultPolicyError::BackoffDelayTooLong`] above 24 hours.
    pub fn fixed(delay: Duration) -> Result<Self, FaultPolicyError> {
        check_delay(delay)?;
        Ok(Self {
            kind: BackoffKind::Fixed,
            initial: delay,
            multiplier: 1,
            maximum: delay,
        })
    }

    /// Returns an integer exponential schedule capped at `maximum`.
    ///
    /// # Errors
    ///
    /// Rejects a zero multiplier, a delay above 24 hours, and a maximum below
    /// the initial delay.
    pub fn exponential(
        initial: Duration,
        multiplier: u32,
        maximum: Duration,
    ) -> Result<Self, FaultPolicyError> {
        check_delay(initial)?;
        check_delay(maximum)?;
        if multiplier == 0 {
            return Err(FaultPolicyError::ZeroBackoffMultiplier);
        }
        if maximum < initial {
            return Err(FaultPolicyError::BackoffMaximumBelowInitial);
        }
        Ok(Self {
            kind: BackoffKind::Exponential,
            initial,
            multiplier,
            maximum,
        })
    }

    /// Returns the selected backoff family.
    #[must_use]
    pub const fn kind(self) -> BackoffKind {
        self.kind
    }

    /// Returns the first-retry delay.
    #[must_use]
    pub const fn initial(self) -> Duration {
        self.initial
    }

    /// Returns the integer growth factor.
    #[must_use]
    pub const fn multiplier(self) -> u32 {
        self.multiplier
    }

    /// Returns the schedule ceiling.
    #[must_use]
    pub const fn maximum(self) -> Duration {
        self.maximum
    }

    /// Returns the delay that precedes the retry identified by `ordinal`.
    ///
    /// The initial call never waits. Arithmetic is checked and capped at the
    /// configured maximum, so a large ordinal cannot overflow or exceed the
    /// declared bound.
    #[must_use]
    pub fn delay_for(self, ordinal: RetryOrdinal) -> Duration {
        if ordinal.is_initial() {
            return Duration::ZERO;
        }
        match self.kind {
            BackoffKind::None => Duration::ZERO,
            BackoffKind::Fixed => self.initial,
            BackoffKind::Exponential => self.exponential_delay(ordinal.get()),
        }
    }

    fn exponential_delay(self, ordinal: u32) -> Duration {
        let maximum_nanos = self.maximum.as_nanos();
        let mut nanos = self.initial.as_nanos();
        if nanos >= maximum_nanos {
            return self.maximum;
        }
        if self.multiplier > 1 {
            let factor = u128::from(self.multiplier);
            for _ in 1..ordinal {
                nanos = nanos.saturating_mul(factor);
                if nanos >= maximum_nanos {
                    return self.maximum;
                }
            }
        }
        u64::try_from(nanos).map_or(self.maximum, Duration::from_nanos)
    }
}

fn check_delay(delay: Duration) -> Result<(), FaultPolicyError> {
    if delay > MAX_BACKOFF {
        return Err(FaultPolicyError::BackoffDelayTooLong {
            max_seconds: MAX_BACKOFF.as_secs(),
        });
    }
    Ok(())
}

/// The result of one cancellable backoff wait.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum BackoffOutcome {
    /// The complete delay elapsed.
    Elapsed,
    /// Cooperative stop cancelled the wait.
    Stopped,
}

/// An injected monotonic, cancellable delay source.
///
/// Implementations must not detach a task or timer, must observe the supplied
/// [`StopToken`] while waiting, and must not consult wall-clock time.
pub trait BackoffSleeper: Send + Sync {
    /// Waits for `delay` unless cooperative stop is observed first.
    fn sleep<'a>(&'a self, delay: Duration, stop: &'a StopToken) -> BoxFuture<'a, BackoffOutcome>;
}

/// How a failed unit of work is separated from committed work.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum RollbackDisposition {
    /// Roll back the open transaction before the skip is recorded.
    Rollback,
    /// Commit the remaining successful work and the skip atomically.
    ///
    /// This narrows Spring's no-rollback behavior: the skip is still counted
    /// and still invokes skip listeners, so an item is never silently dropped.
    CommitSafeSkip,
}

impl RollbackDisposition {
    /// Returns the stable, low-cardinality manifest and telemetry name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Rollback => "rollback",
            Self::CommitSafeSkip => "commit_safe_skip",
        }
    }
}

/// The action a classifier rule declares for one phase and category.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct FaultAction {
    retryable: bool,
    skip: Option<RollbackDisposition>,
}

impl FaultAction {
    /// Fails the step after a known rollback.
    #[must_use]
    pub const fn fail() -> Self {
        Self {
            retryable: false,
            skip: None,
        }
    }

    /// Retries within the configured limit and then fails.
    #[must_use]
    pub const fn retry() -> Self {
        Self {
            retryable: true,
            skip: None,
        }
    }

    /// Skips without retrying.
    #[must_use]
    pub const fn skip(disposition: RollbackDisposition) -> Self {
        Self {
            retryable: false,
            skip: Some(disposition),
        }
    }

    /// Retries within the configured limit and skips after exhaustion.
    #[must_use]
    pub const fn retry_then_skip(disposition: RollbackDisposition) -> Self {
        Self {
            retryable: true,
            skip: Some(disposition),
        }
    }

    /// Returns whether the rule accepts a retry.
    #[must_use]
    pub const fn is_retryable(self) -> bool {
        self.retryable
    }

    /// Returns the accepted skip disposition, when the rule accepts a skip.
    #[must_use]
    pub const fn skip_disposition(self) -> Option<RollbackDisposition> {
        self.skip
    }
}

/// One ordered classifier rule for an exact phase and category.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct FaultRule {
    phase: FaultPhase,
    category: FailureCategory,
    action: FaultAction,
}

impl FaultRule {
    /// Validates and constructs one classification rule.
    ///
    /// # Errors
    ///
    /// Rejects a phase or category that fails closed in M3, and a commit-safe
    /// skip for a phase that may already have produced an external effect.
    pub fn new(
        phase: FaultPhase,
        category: FailureCategory,
        action: FaultAction,
    ) -> Result<Self, FaultPolicyError> {
        if !phase.is_policy_eligible() || !category.is_policy_eligible() {
            return Err(FaultPolicyError::NotPolicyEligible { phase, category });
        }
        if action.skip.is_some() && !phase.is_skippable() {
            return Err(FaultPolicyError::PhaseNotSkippable { phase });
        }
        if action.skip == Some(RollbackDisposition::CommitSafeSkip)
            && !phase.allows_commit_safe_skip()
        {
            return Err(FaultPolicyError::CommitSafeSkipPhase { phase });
        }
        Ok(Self {
            phase,
            category,
            action,
        })
    }

    /// Returns the governed phase.
    #[must_use]
    pub const fn phase(self) -> FaultPhase {
        self.phase
    }

    /// Returns the governed category.
    #[must_use]
    pub const fn category(self) -> FailureCategory {
        self.category
    }

    /// Returns the declared action.
    #[must_use]
    pub const fn action(self) -> FaultAction {
        self.action
    }
}

/// A bounded, order-independent classifier over phases and categories.
///
/// The revision token and the ordered rules are definition-fingerprint input.
/// Rules address exactly one phase and category, so no outcome depends on
/// registration order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FaultClassifier {
    revision: ClassifierRevision,
    rules: Box<[FaultRule]>,
}

impl FaultClassifier {
    /// Validates and constructs a classifier.
    ///
    /// Each rule addresses exactly one phase and category, so the accepted
    /// rules are bounded by that finite product.
    ///
    /// # Errors
    ///
    /// Rejects any repeated phase and category pair.
    pub fn new(
        revision: ClassifierRevision,
        rules: impl IntoIterator<Item = FaultRule>,
    ) -> Result<Self, FaultPolicyError> {
        let mut accepted: Vec<FaultRule> = Vec::new();
        for rule in rules {
            if accepted
                .iter()
                .any(|existing| existing.phase == rule.phase && existing.category == rule.category)
            {
                return Err(FaultPolicyError::DuplicateRule {
                    phase: rule.phase,
                    category: rule.category,
                });
            }
            accepted.push(rule);
        }
        Ok(Self {
            revision,
            rules: accepted.into_boxed_slice(),
        })
    }

    /// Borrows the bounded revision token.
    #[must_use]
    pub const fn revision(&self) -> &ClassifierRevision {
        &self.revision
    }

    /// Borrows the rules in registration order.
    #[must_use]
    pub fn rules(&self) -> &[FaultRule] {
        &self.rules
    }

    /// Returns the action for one phase and category.
    ///
    /// An unmatched fault has no action and therefore fails closed.
    #[must_use]
    pub fn action_for(&self, phase: FaultPhase, category: FailureCategory) -> Option<FaultAction> {
        self.rules
            .iter()
            .find(|rule| rule.phase == phase && rule.category == category)
            .map(|rule| rule.action)
    }
}

/// Framework evidence about one failed unit of work.
///
/// The runtime proves these properties before a policy may accept a skip. The
/// values describe framework bookkeeping only; they carry no item value.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FaultEvidence {
    located: bool,
    known_rollback: bool,
    forward_checkpoint_proof: bool,
}

impl FaultEvidence {
    /// Evidence for a fault whose failed unit is not located.
    pub const NONE: Self = Self {
        located: false,
        known_rollback: false,
        forward_checkpoint_proof: false,
    };

    /// Constructs complete skip evidence.
    #[must_use]
    pub const fn new(located: bool, known_rollback: bool, forward_checkpoint_proof: bool) -> Self {
        Self {
            located,
            known_rollback,
            forward_checkpoint_proof,
        }
    }

    /// Records that exactly one failed input or output ordinal is identified.
    #[must_use]
    pub const fn with_located(mut self, located: bool) -> Self {
        self.located = located;
        self
    }

    /// Records that the failed work left no visible external effect.
    #[must_use]
    pub const fn with_known_rollback(mut self, known_rollback: bool) -> Self {
        self.known_rollback = known_rollback;
        self
    }

    /// Records that the reader proved its checkpoint moved past the input.
    #[must_use]
    pub const fn with_forward_checkpoint_proof(mut self, proof: bool) -> Self {
        self.forward_checkpoint_proof = proof;
        self
    }

    /// Returns whether exactly one failed unit is identified.
    #[must_use]
    pub const fn is_located(self) -> bool {
        self.located
    }

    /// Returns whether the failed work is known to have been rolled back.
    #[must_use]
    pub const fn is_known_rollback(self) -> bool {
        self.known_rollback
    }

    /// Returns whether forward checkpoint progress is proven.
    #[must_use]
    pub const fn has_forward_checkpoint_proof(self) -> bool {
        self.forward_checkpoint_proof
    }
}

/// The authoritative policy outcome for one fault.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum FaultDecision {
    /// Roll back, reserve `ordinal` durably, wait `delay`, then re-invoke.
    Retry {
        /// The retry ordinal to reserve.
        ordinal: RetryOrdinal,
        /// The deterministic delay preceding re-invocation.
        delay: Duration,
    },
    /// Record one committed skip with the accepted disposition.
    Skip {
        /// How the failed unit is separated from committed work.
        disposition: RollbackDisposition,
    },
    /// Roll back and fail the step.
    FailAndRollback,
    /// The commit outcome is unknown and must never be guessed.
    Unknown,
    /// Cooperative stop governs the outcome.
    Stop,
}

impl FaultDecision {
    /// Returns whether the decision re-invokes the failed component.
    #[must_use]
    pub const fn is_retry(self) -> bool {
        matches!(self, Self::Retry { .. })
    }

    /// Returns the accepted skip disposition, when the decision skips.
    #[must_use]
    pub const fn skip_disposition(self) -> Option<RollbackDisposition> {
        match self {
            Self::Skip { disposition } => Some(disposition),
            _ => None,
        }
    }
}

/// The validated retry, backoff, skip, and rollback policy for one step.
///
/// ```
/// use std::time::Duration;
///
/// use oxide_batch::{
///     BackoffPolicy, ChunkDeliveryMode, ClassifierRevision, FailureCategory, FailureId,
///     FailureSummary, FaultAction, FaultClassifier, FaultDecision, FaultDescriptor,
///     FaultEvidence, FaultPhase, FaultPolicy, FaultRule, RetryLimit, RetryOrdinal,
///     RetryStateLimit, SkipCounts, SkipLimit,
/// };
///
/// let classifier = FaultClassifier::new(
///     ClassifierRevision::new("import_v1")?,
///     [FaultRule::new(
///         FaultPhase::Write,
///         FailureCategory::Timeout,
///         FaultAction::retry(),
///     )?],
/// )?;
/// let policy = FaultPolicy::new(
///     classifier,
///     RetryLimit::new(3)?,
///     RetryStateLimit::new(64)?,
///     SkipLimit::NONE,
///     BackoffPolicy::exponential(Duration::from_millis(50), 2, Duration::from_secs(5))?,
/// )?;
///
/// let fault = FaultDescriptor::new(
///     FaultPhase::Write,
///     FailureSummary::new(FailureCategory::Timeout, FailureId::new(1)?),
///     RetryOrdinal::INITIAL,
///     SkipCounts::ZERO,
///     true,
///     ChunkDeliveryMode::AtomicSameResource,
/// );
/// assert_eq!(
///     policy.decide(&fault, FaultEvidence::NONE),
///     FaultDecision::Retry {
///         ordinal: RetryOrdinal::new(1)?,
///         delay: Duration::from_millis(50),
///     }
/// );
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FaultPolicy {
    classifier: FaultClassifier,
    retry_limit: RetryLimit,
    retry_state_limit: RetryStateLimit,
    skip_limit: SkipLimit,
    backoff: BackoffPolicy,
}

impl FaultPolicy {
    /// Validates and constructs the step policy.
    ///
    /// # Errors
    ///
    /// Rejects a retry rule that no retry limit can ever satisfy, so a
    /// statically impossible combination cannot reach the runtime.
    pub fn new(
        classifier: FaultClassifier,
        retry_limit: RetryLimit,
        retry_state_limit: RetryStateLimit,
        skip_limit: SkipLimit,
        backoff: BackoffPolicy,
    ) -> Result<Self, FaultPolicyError> {
        if retry_limit.is_none()
            && let Some(rule) = classifier
                .rules()
                .iter()
                .find(|rule| rule.action().is_retryable())
        {
            return Err(FaultPolicyError::UnreachableRetryRule {
                phase: rule.phase(),
                category: rule.category(),
            });
        }
        Ok(Self {
            classifier,
            retry_limit,
            retry_state_limit,
            skip_limit,
            backoff,
        })
    }

    /// Borrows the classifier.
    #[must_use]
    pub const fn classifier(&self) -> &FaultClassifier {
        &self.classifier
    }

    /// Returns the configured retry limit.
    #[must_use]
    pub const fn retry_limit(&self) -> RetryLimit {
        self.retry_limit
    }

    /// Returns the unresolved retry-key capacity for one step.
    #[must_use]
    pub const fn retry_state_limit(&self) -> RetryStateLimit {
        self.retry_state_limit
    }

    /// Returns the aggregate skip limit.
    #[must_use]
    pub const fn skip_limit(&self) -> SkipLimit {
        self.skip_limit
    }

    /// Returns the backoff schedule.
    #[must_use]
    pub const fn backoff(&self) -> BackoffPolicy {
        self.backoff
    }

    /// Returns whether any rule accepts a commit-safe skip.
    #[must_use]
    pub fn requires_commit_safe_skip(&self) -> bool {
        self.classifier.rules().iter().any(|rule| {
            rule.action().skip_disposition() == Some(RollbackDisposition::CommitSafeSkip)
        })
    }

    /// Verifies the selected resource can honour the policy before user work.
    ///
    /// # Errors
    ///
    /// Returns [`FaultPolicyError::CommitSafeSkipUnsupported`] when a rule
    /// accepts a commit-safe skip that the transaction capability cannot
    /// commit atomically.
    pub fn validate_capabilities(
        &self,
        supports_atomic_skip: bool,
    ) -> Result<(), FaultPolicyError> {
        if self.requires_commit_safe_skip() && !supports_atomic_skip {
            return Err(FaultPolicyError::CommitSafeSkipUnsupported);
        }
        Ok(())
    }

    /// Returns the authoritative decision for one fault.
    ///
    /// The decision is a pure function of the policy, the framework-owned
    /// descriptor, and framework evidence, so it is reproducible after a
    /// restart.
    #[must_use]
    pub fn decide(&self, fault: &FaultDescriptor, evidence: FaultEvidence) -> FaultDecision {
        let category = fault.category();
        if category == FailureCategory::UnknownCommit {
            return FaultDecision::Unknown;
        }
        if category == FailureCategory::Cancelled {
            return FaultDecision::Stop;
        }
        let phase = fault.phase();
        if !phase.is_policy_eligible() || !category.is_policy_eligible() {
            return FaultDecision::FailAndRollback;
        }
        let Some(action) = self.classifier.action_for(phase, category) else {
            return FaultDecision::FailAndRollback;
        };
        if action.is_retryable()
            && let Ok(next) = fault.retry_ordinal().checked_next()
            && self.retry_limit.permits(next)
        {
            return FaultDecision::Retry {
                ordinal: next,
                delay: self.backoff.delay_for(next),
            };
        }
        match action.skip_disposition() {
            Some(disposition) => self.decide_skip(fault, disposition, evidence),
            None => FaultDecision::FailAndRollback,
        }
    }

    fn decide_skip(
        &self,
        fault: &FaultDescriptor,
        disposition: RollbackDisposition,
        evidence: FaultEvidence,
    ) -> FaultDecision {
        let phase = fault.phase();
        if !evidence.is_located() {
            return FaultDecision::FailAndRollback;
        }
        let phase_evidence = match phase {
            FaultPhase::Read => evidence.has_forward_checkpoint_proof(),
            FaultPhase::Process => true,
            FaultPhase::Write => evidence.is_known_rollback(),
            _ => false,
        };
        if !phase_evidence {
            return FaultDecision::FailAndRollback;
        }
        if disposition == RollbackDisposition::CommitSafeSkip
            && !(phase.allows_commit_safe_skip()
                && evidence.is_known_rollback()
                && evidence.has_forward_checkpoint_proof())
        {
            return FaultDecision::FailAndRollback;
        }
        match fault.committed_skips().checked_increment(phase) {
            Ok(next) => match next.checked_total() {
                Ok(total) if total <= self.skip_limit.get() => FaultDecision::Skip { disposition },
                _ => FaultDecision::FailAndRollback,
            },
            Err(_) => FaultDecision::FailAndRollback,
        }
    }
}

/// A value-redacted fault-policy validation or arithmetic failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum FaultPolicyError {
    /// A retry limit exceeded the bounded representation.
    RetryLimitOutOfRange {
        /// The largest accepted limit.
        max: u32,
    },
    /// A retry ordinal exceeded the bounded representation.
    RetryOrdinalOutOfRange {
        /// The largest accepted ordinal.
        max: u32,
    },
    /// The unresolved retry-key capacity was outside its bound.
    RetryStateLimitOutOfRange {
        /// The smallest accepted capacity.
        min: u32,
        /// The largest accepted capacity.
        max: u32,
    },
    /// A backoff delay exceeded the accepted maximum.
    BackoffDelayTooLong {
        /// The largest accepted delay in seconds.
        max_seconds: u64,
    },
    /// An exponential schedule used a zero multiplier.
    ZeroBackoffMultiplier,
    /// An exponential ceiling was below its initial delay.
    BackoffMaximumBelowInitial,
    /// A rule addressed a phase or category that fails closed in M3.
    NotPolicyEligible {
        /// The rejected phase.
        phase: FaultPhase,
        /// The rejected category.
        category: FailureCategory,
    },
    /// A rule accepted a skip for a phase that cannot commit one.
    PhaseNotSkippable {
        /// The rejected phase.
        phase: FaultPhase,
    },
    /// A rule accepted a commit-safe skip after a possible external effect.
    CommitSafeSkipPhase {
        /// The rejected phase.
        phase: FaultPhase,
    },
    /// The selected resource cannot commit a skip atomically.
    CommitSafeSkipUnsupported,
    /// Two rules addressed the same phase and category.
    DuplicateRule {
        /// The repeated phase.
        phase: FaultPhase,
        /// The repeated category.
        category: FailureCategory,
    },
    /// A retry rule could never be satisfied by the configured retry limit.
    UnreachableRetryRule {
        /// The affected phase.
        phase: FaultPhase,
        /// The affected category.
        category: FailureCategory,
    },
    /// Checked skip-count arithmetic rejected the update.
    SkipCountOverflow,
}

impl fmt::Display for FaultPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RetryLimitOutOfRange { max } => {
                write!(formatter, "retry limit exceeds {max}")
            }
            Self::RetryOrdinalOutOfRange { max } => {
                write!(formatter, "retry ordinal exceeds {max}")
            }
            Self::RetryStateLimitOutOfRange { min, max } => {
                write!(formatter, "retry state limit must be within {min}..={max}")
            }
            Self::BackoffDelayTooLong { max_seconds } => {
                write!(formatter, "backoff delay exceeds {max_seconds} seconds")
            }
            Self::ZeroBackoffMultiplier => {
                formatter.write_str("exponential backoff requires a nonzero multiplier")
            }
            Self::BackoffMaximumBelowInitial => {
                formatter.write_str("exponential backoff maximum is below its initial delay")
            }
            Self::NotPolicyEligible { phase, category } => write!(
                formatter,
                "{phase} {category:?} faults are never retried or skipped"
            ),
            Self::PhaseNotSkippable { phase } => {
                write!(formatter, "{phase} faults cannot commit a skip")
            }
            Self::CommitSafeSkipPhase { phase } => {
                write!(formatter, "{phase} faults cannot commit a skip safely")
            }
            Self::CommitSafeSkipUnsupported => {
                formatter.write_str("the selected resource cannot commit a skip atomically")
            }
            Self::DuplicateRule { phase, category } => write!(
                formatter,
                "duplicate classifier rule for {phase} {category:?}"
            ),
            Self::UnreachableRetryRule { phase, category } => write!(
                formatter,
                "{phase} {category:?} retry rule requires a nonzero retry limit"
            ),
            Self::SkipCountOverflow => formatter.write_str("skip counters overflowed"),
        }
    }
}

impl Error for FaultPolicyError {}
