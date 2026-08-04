//! Evidence-bound stale-execution recovery observations.
//!
//! A snapshot is a bounded, value-redacted observation gathered with repository
//! server time. It never mutates the execution and it cannot authorize
//! takeover. The repository compares the complete owner token without returning
//! that token to the caller. The proposer that turns these observations into a
//! proposal lives above this crate.

use std::error::Error;
use std::fmt;
use std::time::{Duration, Instant, SystemTime};

use oxide_batch_core::{BatchStatus, ExecutionVersion, JobExecutionId, StepExecutionId};

use crate::{BoxFuture, CanonicalWriter, RepositoryError, StateEnvelopeDescriptor, hex_digest};

/// Minimum accepted stale-execution threshold.
pub const MIN_STALE_THRESHOLD: Duration = Duration::from_mins(1);
/// Maximum accepted stale-execution threshold.
pub const MAX_STALE_THRESHOLD: Duration = Duration::from_hours(24);
/// Default stale-execution threshold.
pub const DEFAULT_STALE_THRESHOLD: Duration = Duration::from_mins(15);
/// Minimum accepted repository/local clock-skew bound.
pub const MIN_CLOCK_SKEW: Duration = Duration::from_millis(100);
/// Maximum accepted repository/local clock-skew bound.
pub const MAX_CLOCK_SKEW: Duration = Duration::from_mins(1);
/// Default repository/local clock-skew bound.
pub const DEFAULT_MAX_CLOCK_SKEW: Duration = Duration::from_secs(5);

/// A per-process 16-byte execution-owner token.
///
/// The token is evidence only. It is not a lease, never expires, grants no
/// authority, and must not be reused by another process.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OwnerToken([u8; 16]);

impl OwnerToken {
    /// Constructs a token from application-generated random bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the complete bytes for a repository ownership comparison.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl fmt::Debug for OwnerToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OwnerToken(<redacted>)")
    }
}

/// The durable owner-token observation relative to the inspecting process.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum OwnerObservation {
    /// No owner token was recorded.
    Absent,
    /// The complete token matches the inspecting process.
    CurrentProcess,
    /// A complete, different token was recorded.
    OtherProcess,
}

impl OwnerObservation {
    const fn code(self) -> &'static str {
        match self {
            Self::Absent => "ABSENT",
            Self::CurrentProcess => "CURRENT_PROCESS",
            Self::OtherProcess => "OTHER_PROCESS",
        }
    }
}

/// A bounded stale-execution threshold.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StaleThreshold(Duration);

impl StaleThreshold {
    /// Validates a threshold in `1 min..=24 h`.
    ///
    /// # Errors
    ///
    /// Returns [`RecoveryError::InvalidStaleThreshold`] outside the bound.
    pub fn new(value: Duration) -> Result<Self, RecoveryError> {
        if !(MIN_STALE_THRESHOLD..=MAX_STALE_THRESHOLD).contains(&value) {
            return Err(RecoveryError::InvalidStaleThreshold);
        }
        Ok(Self(value))
    }

    /// Returns the validated duration.
    #[must_use]
    pub const fn get(self) -> Duration {
        self.0
    }
}

impl Default for StaleThreshold {
    fn default() -> Self {
        Self(DEFAULT_STALE_THRESHOLD)
    }
}

/// A bounded repository/local wall-clock skew tolerance.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MaxClockSkew(Duration);

impl MaxClockSkew {
    /// Validates a skew bound in `100 ms..=60 s`.
    ///
    /// # Errors
    ///
    /// Returns [`RecoveryError::InvalidMaxClockSkew`] outside the bound.
    pub fn new(value: Duration) -> Result<Self, RecoveryError> {
        if !(MIN_CLOCK_SKEW..=MAX_CLOCK_SKEW).contains(&value) {
            return Err(RecoveryError::InvalidMaxClockSkew);
        }
        Ok(Self(value))
    }

    /// Returns the validated duration.
    #[must_use]
    pub const fn get(self) -> Duration {
        self.0
    }
}

impl Default for MaxClockSkew {
    fn default() -> Self {
        Self(DEFAULT_MAX_CLOCK_SKEW)
    }
}

/// A runtime-neutral reading of one monotonic clock.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MonotonicInstant(Duration);

impl MonotonicInstant {
    /// Constructs a deterministic reading for an injected test clock.
    #[must_use]
    pub const fn from_duration(value: Duration) -> Self {
        Self(value)
    }

    /// Returns the elapsed monotonic duration, or `None` when time went back.
    #[doc(hidden)]
    #[must_use]
    pub fn checked_elapsed_since(self, earlier: Self) -> Option<Duration> {
        self.0.checked_sub(earlier.0)
    }
}

/// Supplies monotonic readings for bounded recovery observations.
pub trait MonotonicClock: Send + Sync {
    /// Returns the current monotonic reading.
    fn now(&self) -> MonotonicInstant;
}

/// An application-owned system monotonic clock.
#[derive(Clone, Debug)]
pub struct SystemMonotonicClock {
    origin: Instant,
}

impl SystemMonotonicClock {
    /// Starts a new monotonic epoch owned by the caller.
    #[must_use]
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl Default for SystemMonotonicClock {
    fn default() -> Self {
        Self::new()
    }
}

impl MonotonicClock for SystemMonotonicClock {
    fn now(&self) -> MonotonicInstant {
        MonotonicInstant(self.origin.elapsed())
    }
}

/// Redacted evidence for the latest durable step execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryStepEvidence {
    id: StepExecutionId,
    status: BatchStatus,
    checkpoint: Option<StateEnvelopeDescriptor>,
}

/// Closed boolean recovery markers retained as one bounded bit set.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RecoveryMarkers(u8);

impl RecoveryMarkers {
    const UNKNOWN_COMMIT: u8 = 1;
    const COMPLETED_PARTITION: u8 = 1 << 1;
    const COMMITTED_FLOW_DECISION: u8 = 1 << 2;
    const AMBIGUOUS_EXTERNAL_EFFECT: u8 = 1 << 3;

    /// Constructs an empty marker set.
    #[must_use]
    pub const fn new() -> Self {
        Self(0)
    }

    /// Records whether the last durable marker is an unknown commit.
    #[must_use]
    pub const fn with_unknown_commit(mut self, value: bool) -> Self {
        if value {
            self.0 |= Self::UNKNOWN_COMMIT;
        }
        self
    }

    /// Records whether completed partition evidence exists.
    #[must_use]
    pub const fn with_completed_partition(mut self, value: bool) -> Self {
        if value {
            self.0 |= Self::COMPLETED_PARTITION;
        }
        self
    }

    /// Records whether a committed flow decision exists.
    #[must_use]
    pub const fn with_committed_flow_decision(mut self, value: bool) -> Self {
        if value {
            self.0 |= Self::COMMITTED_FLOW_DECISION;
        }
        self
    }

    /// Records whether the definition declares an ambiguous external effect.
    #[must_use]
    pub const fn with_ambiguous_external_effect(mut self, value: bool) -> Self {
        if value {
            self.0 |= Self::AMBIGUOUS_EXTERNAL_EFFECT;
        }
        self
    }

    const fn contains(self, marker: u8) -> bool {
        self.0 & marker != 0
    }
}

impl RecoveryStepEvidence {
    /// Constructs one value-redacted step observation.
    #[must_use]
    pub const fn new(
        id: StepExecutionId,
        status: BatchStatus,
        checkpoint: Option<StateEnvelopeDescriptor>,
    ) -> Self {
        Self {
            id,
            status,
            checkpoint,
        }
    }

    /// Returns the latest durable step-execution identifier.
    #[must_use]
    pub const fn id(&self) -> StepExecutionId {
        self.id
    }

    /// Returns its durable lifecycle status.
    #[must_use]
    pub const fn status(&self) -> BatchStatus {
        self.status
    }

    /// Borrows its redacted checkpoint envelope descriptor.
    #[must_use]
    pub const fn checkpoint(&self) -> Option<&StateEnvelopeDescriptor> {
        self.checkpoint.as_ref()
    }
}

/// One adapter-owned recovery snapshot gathered with repository server time.
///
/// The snapshot contains only the closed evidence fields accepted by the M4
/// contract. It has no parameter, context, checkpoint payload, item, error
/// text, credential, endpoint, or SQL value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoverySnapshot {
    execution_id: JobExecutionId,
    status: BatchStatus,
    attempt: u32,
    version: ExecutionVersion,
    owner: OwnerObservation,
    updated_at: SystemTime,
    server_time: SystemTime,
    latest_step: Option<RecoveryStepEvidence>,
    markers: RecoveryMarkers,
}

impl RecoverySnapshot {
    /// Constructs one adapter-owned, value-redacted snapshot.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        execution_id: JobExecutionId,
        status: BatchStatus,
        attempt: u32,
        version: ExecutionVersion,
        owner: OwnerObservation,
        updated_at: SystemTime,
        server_time: SystemTime,
        latest_step: Option<RecoveryStepEvidence>,
        markers: RecoveryMarkers,
    ) -> Self {
        Self {
            execution_id,
            status,
            attempt,
            version,
            owner,
            updated_at,
            server_time,
            latest_step,
            markers,
        }
    }

    /// Returns the observed lifecycle status.
    #[must_use]
    pub const fn status(&self) -> BatchStatus {
        self.status
    }

    /// Returns the owner-token observation.
    #[must_use]
    pub const fn owner(&self) -> OwnerObservation {
        self.owner
    }

    /// Returns the durable timestamp whose age establishes staleness.
    #[must_use]
    pub const fn updated_at(&self) -> SystemTime {
        self.updated_at
    }

    /// Returns the repository server time this snapshot was gathered with.
    #[must_use]
    pub const fn server_time(&self) -> SystemTime {
        self.server_time
    }
}

/// Adapter port for one bounded, server-time recovery observation.
pub trait RecoveryRepository: Send + Sync {
    /// Reads one value-redacted snapshot without changing durable state.
    fn recovery_snapshot<'a>(
        &'a self,
        execution_id: JobExecutionId,
        current_owner: &'a OwnerToken,
    ) -> BoxFuture<'a, Result<RecoverySnapshot, RepositoryError>>;
}

/// Canonical evidence retained by one recovery proposal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryEvidence {
    snapshot: RecoverySnapshot,
    inactivity: Duration,
    observed_clock_offset: Duration,
    observation_window: Duration,
}

impl RecoveryEvidence {
    /// Binds one snapshot to the clock evidence gathered around it.
    ///
    /// The three durations are the proposer's own observations: the inactivity
    /// it measured against repository server time, the offset it observed
    /// between that server time and its local wall clock, and the monotonic
    /// window the snapshot read occupied.
    #[must_use]
    pub const fn new(
        snapshot: RecoverySnapshot,
        inactivity: Duration,
        observed_clock_offset: Duration,
        observation_window: Duration,
    ) -> Self {
        Self {
            snapshot,
            inactivity,
            observed_clock_offset,
            observation_window,
        }
    }

    /// Returns the execution identity.
    #[must_use]
    pub const fn execution_id(&self) -> JobExecutionId {
        self.snapshot.execution_id
    }

    /// Returns the observed lifecycle status.
    #[must_use]
    pub const fn status(&self) -> BatchStatus {
        self.snapshot.status
    }

    /// Returns the attempt ordinal.
    #[must_use]
    pub const fn attempt(&self) -> u32 {
        self.snapshot.attempt
    }

    /// Returns the observed optimistic version.
    #[must_use]
    pub const fn version(&self) -> ExecutionVersion {
        self.snapshot.version
    }

    /// Returns the owner-token observation.
    #[must_use]
    pub const fn owner(&self) -> OwnerObservation {
        self.snapshot.owner
    }

    /// Returns the durable inactivity observed against repository server time.
    #[must_use]
    pub const fn inactivity(&self) -> Duration {
        self.inactivity
    }

    /// Returns the durable timestamp whose age established staleness.
    #[must_use]
    pub const fn updated_at(&self) -> SystemTime {
        self.snapshot.updated_at
    }

    /// Returns the repository-server time bound into this observation.
    #[must_use]
    pub const fn server_time(&self) -> SystemTime {
        self.snapshot.server_time
    }

    /// Returns the absolute repository/local wall-clock offset.
    #[must_use]
    pub const fn observed_clock_offset(&self) -> Duration {
        self.observed_clock_offset
    }

    /// Returns the monotonic window that bounded this observation.
    #[must_use]
    pub const fn observation_window(&self) -> Duration {
        self.observation_window
    }

    /// Borrows the latest durable step evidence, when any step exists.
    #[must_use]
    pub const fn latest_step(&self) -> Option<&RecoveryStepEvidence> {
        self.snapshot.latest_step.as_ref()
    }

    /// Returns whether the last durable marker is an unknown commit.
    #[must_use]
    pub const fn unknown_commit(&self) -> bool {
        self.snapshot
            .markers
            .contains(RecoveryMarkers::UNKNOWN_COMMIT)
    }

    /// Returns whether completed partition evidence exists.
    #[must_use]
    pub const fn completed_partition(&self) -> bool {
        self.snapshot
            .markers
            .contains(RecoveryMarkers::COMPLETED_PARTITION)
    }

    /// Returns whether a committed flow decision exists.
    #[must_use]
    pub const fn committed_flow_decision(&self) -> bool {
        self.snapshot
            .markers
            .contains(RecoveryMarkers::COMMITTED_FLOW_DECISION)
    }

    /// Returns whether the definition declares an ambiguous external effect.
    #[must_use]
    pub const fn ambiguous_external_effect(&self) -> bool {
        self.snapshot
            .markers
            .contains(RecoveryMarkers::AMBIGUOUS_EXTERNAL_EFFECT)
    }

    fn digest(&self) -> [u8; 32] {
        let mut writer = CanonicalWriter::new("oxide-batch.recovery-evidence.v1");
        writer.push_u64(self.execution_id().get());
        writer.push_str(self.status().as_str());
        writer.push_u64(u64::from(self.attempt()));
        writer.push_u64(self.version().get());
        writer.push_str(self.owner().code());
        // Bind the durable timestamp rather than the advancing observation
        // time and its derived durations. A stateless client can therefore
        // regenerate the same digest exactly while durable evidence is
        // unchanged; a stop request or lifecycle write changes `updated_at`.
        push_system_time(&mut writer, self.snapshot.updated_at);
        match self.latest_step() {
            Some(step) => {
                writer.push_u64(step.id().get());
                writer.push_str(step.status().as_str());
                match step.checkpoint() {
                    Some(checkpoint) => {
                        writer.push_u64(u64::from(checkpoint.format_version()));
                        writer.push_str(checkpoint.schema_id().as_str());
                        writer.push_u64(u64::from(checkpoint.schema_version().get()));
                        writer
                            .push_u64(u64::try_from(checkpoint.encoded_len()).unwrap_or(u64::MAX));
                    }
                    None => writer.push_str("NO_CHECKPOINT"),
                }
            }
            None => writer.push_str("NO_STEP"),
        }
        writer.push_u64(u64::from(self.unknown_commit()));
        writer.push_u64(u64::from(self.completed_partition()));
        writer.push_u64(u64::from(self.committed_flow_decision()));
        writer.push_u64(u64::from(self.ambiguous_external_effect()));
        writer.digest()
    }
}

fn push_duration(writer: &mut CanonicalWriter, value: Duration) {
    writer.push_u64(value.as_secs());
    writer.push_u64(u64::from(value.subsec_nanos()));
}

fn push_system_time(writer: &mut CanonicalWriter, value: SystemTime) {
    match value.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => {
            writer.push_str("AFTER_EPOCH");
            push_duration(writer, duration);
        }
        Err(error) => {
            writer.push_str("BEFORE_EPOCH");
            push_duration(writer, error.duration());
        }
    }
}

/// A validated, evidence-bound recovery proposal.
#[derive(Clone, Eq, PartialEq)]
pub struct RecoveryProposal {
    evidence: RecoveryEvidence,
    digest: [u8; 32],
}

impl RecoveryProposal {
    /// Seals one proposal over its evidence.
    ///
    /// The digest is computed here rather than supplied, so a proposal cannot
    /// carry a digest that its evidence does not produce.
    #[must_use]
    pub fn new(evidence: RecoveryEvidence) -> Self {
        let digest = evidence.digest();
        Self { evidence, digest }
    }

    /// Borrows the bounded redacted evidence.
    #[must_use]
    pub const fn evidence(&self) -> &RecoveryEvidence {
        &self.evidence
    }

    /// Returns the observed execution version bound into the digest.
    #[must_use]
    pub const fn observed_version(&self) -> ExecutionVersion {
        self.evidence.version()
    }

    /// Returns the canonical evidence digest.
    #[must_use]
    pub const fn digest(&self) -> &[u8; 32] {
        &self.digest
    }

    /// Returns the lowercase hexadecimal digest.
    #[must_use]
    pub fn digest_hex(&self) -> String {
        hex_digest(&self.digest)
    }
}

impl fmt::Debug for RecoveryProposal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecoveryProposal")
            .field("evidence", &self.evidence)
            .field("digest", &self.digest_hex())
            .finish()
    }
}

/// A typed recovery-proposal failure.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RecoveryError {
    /// The configured stale threshold was outside `1 min..=24 h`.
    InvalidStaleThreshold,
    /// The configured clock-skew bound was outside `100 ms..=60 s`.
    InvalidMaxClockSkew,
    /// Repository time, local wall time, or the monotonic observation window
    /// could not provide usable evidence.
    ClockEvidenceUnusable,
    /// The execution is still owned by the inspecting process.
    OwnedByCurrentProcess,
    /// The durable inactivity has not crossed the strict stale threshold.
    NotStale {
        /// Observed durable inactivity.
        inactivity: Duration,
        /// Configured threshold.
        threshold: StaleThreshold,
    },
    /// The status is neither ambiguous nor an active stale candidate.
    NotRecoverable {
        /// Observed durable status.
        status: BatchStatus,
    },
    /// The repository could not produce evidence.
    Repository(RepositoryError),
}

impl fmt::Display for RecoveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidStaleThreshold => {
                formatter.write_str("stale threshold must be between 1 minute and 24 hours")
            }
            Self::InvalidMaxClockSkew => {
                formatter.write_str("maximum clock skew must be between 100 ms and 60 seconds")
            }
            Self::ClockEvidenceUnusable => {
                formatter.write_str("repository and local clocks cannot provide usable evidence")
            }
            Self::OwnedByCurrentProcess => {
                formatter.write_str("the execution is owned by the inspecting process")
            }
            Self::NotStale {
                inactivity,
                threshold,
            } => write!(
                formatter,
                "durable inactivity of {inactivity:?} has not exceeded {:?}",
                threshold.get()
            ),
            Self::NotRecoverable { status } => {
                write!(
                    formatter,
                    "an execution in {status} is not a recovery candidate"
                )
            }
            Self::Repository(error) => error.fmt(formatter),
        }
    }
}

impl Error for RecoveryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Repository(error) => Some(error),
            _ => None,
        }
    }
}
