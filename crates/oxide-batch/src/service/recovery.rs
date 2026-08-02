//! Evidence-bound stale-execution recovery proposals.
//!
//! A proposal is a bounded, value-redacted observation. It never mutates the
//! execution and it cannot authorize takeover. The repository supplies its own
//! server time and compares the complete owner token without returning that
//! token to the caller.

use std::error::Error;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use super::{CanonicalWriter, StateEnvelopeDescriptor, hex_digest};
use crate::{
    BatchStatus, BoxFuture, Clock, ExecutionVersion, JobExecutionId, RepositoryError,
    StepExecutionId,
};

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

    fn checked_elapsed_since(self, earlier: Self) -> Option<Duration> {
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

/// Produces evidence-bound proposals without mutating repository state.
pub struct RecoveryProposer<R> {
    repository: R,
    wall_clock: Arc<dyn Clock>,
    monotonic_clock: Arc<dyn MonotonicClock>,
    current_owner: OwnerToken,
    stale_threshold: StaleThreshold,
    max_clock_skew: MaxClockSkew,
    server_time_floor: Mutex<Option<SystemTime>>,
}

impl<R> fmt::Debug for RecoveryProposer<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecoveryProposer")
            .field("current_owner", &self.current_owner)
            .field("stale_threshold", &self.stale_threshold)
            .field("max_clock_skew", &self.max_clock_skew)
            .finish_non_exhaustive()
    }
}

impl<R: RecoveryRepository> RecoveryProposer<R> {
    /// Binds explicit repository, wall-clock, monotonic-clock, and process
    /// ownership evidence.
    #[must_use]
    pub fn new(
        repository: R,
        wall_clock: Arc<dyn Clock>,
        monotonic_clock: Arc<dyn MonotonicClock>,
        current_owner: OwnerToken,
    ) -> Self {
        Self {
            repository,
            wall_clock,
            monotonic_clock,
            current_owner,
            stale_threshold: StaleThreshold::default(),
            max_clock_skew: MaxClockSkew::default(),
            server_time_floor: Mutex::new(None),
        }
    }

    /// Replaces the validated stale threshold.
    #[must_use]
    pub const fn with_stale_threshold(mut self, value: StaleThreshold) -> Self {
        self.stale_threshold = value;
        self
    }

    /// Replaces the validated clock-skew bound.
    #[must_use]
    pub const fn with_max_clock_skew(mut self, value: MaxClockSkew) -> Self {
        self.max_clock_skew = value;
        self
    }

    /// Gathers one bounded proposal without changing durable state.
    ///
    /// # Errors
    ///
    /// Rejects terminal/non-candidate state, current-process ownership, a
    /// candidate younger than the threshold, unusable clock evidence, or a
    /// repository failure.
    pub async fn propose(
        &self,
        execution_id: JobExecutionId,
    ) -> Result<RecoveryProposal, RecoveryError> {
        let before = self.monotonic_clock.now();
        let local_wall = self.wall_clock.now();
        let snapshot = self
            .repository
            .recovery_snapshot(execution_id, &self.current_owner)
            .await
            .map_err(RecoveryError::Repository)?;
        let after = self.monotonic_clock.now();
        {
            let mut floor = self
                .server_time_floor
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if floor.is_some_and(|previous| snapshot.server_time < previous) {
                return Err(RecoveryError::ClockEvidenceUnusable);
            }
            *floor = Some(snapshot.server_time);
        }
        let observation_window = after
            .checked_elapsed_since(before)
            .ok_or(RecoveryError::ClockEvidenceUnusable)?;
        if observation_window > self.max_clock_skew.get() {
            return Err(RecoveryError::ClockEvidenceUnusable);
        }
        let observed_clock_offset = absolute_system_difference(snapshot.server_time, local_wall);
        if observed_clock_offset > self.max_clock_skew.get() {
            return Err(RecoveryError::ClockEvidenceUnusable);
        }
        let inactivity = snapshot
            .server_time
            .duration_since(snapshot.updated_at)
            .map_err(|_| RecoveryError::ClockEvidenceUnusable)?;

        match snapshot.status {
            BatchStatus::Unknown => {}
            BatchStatus::Starting | BatchStatus::Started | BatchStatus::Stopping => {
                if snapshot.owner == OwnerObservation::CurrentProcess {
                    return Err(RecoveryError::OwnedByCurrentProcess);
                }
                if inactivity <= self.stale_threshold.get() {
                    return Err(RecoveryError::NotStale {
                        inactivity,
                        threshold: self.stale_threshold,
                    });
                }
            }
            status => return Err(RecoveryError::NotRecoverable { status }),
        }

        let evidence = RecoveryEvidence {
            snapshot,
            inactivity,
            observed_clock_offset,
            observation_window,
        };
        let digest = evidence.digest();
        Ok(RecoveryProposal { evidence, digest })
    }
}

fn absolute_system_difference(left: SystemTime, right: SystemTime) -> Duration {
    left.duration_since(right)
        .unwrap_or_else(|_| right.duration_since(left).unwrap_or(Duration::MAX))
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

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use super::*;

    #[derive(Debug)]
    struct FixedWall(SystemTime);

    impl Clock for FixedWall {
        fn now(&self) -> SystemTime {
            self.0
        }
    }

    #[derive(Debug)]
    struct SequenceMonotonic(Mutex<VecDeque<MonotonicInstant>>);

    impl SequenceMonotonic {
        fn observations(values: impl IntoIterator<Item = Duration>) -> Self {
            Self(Mutex::new(
                values
                    .into_iter()
                    .map(MonotonicInstant::from_duration)
                    .collect(),
            ))
        }
    }

    impl MonotonicClock for SequenceMonotonic {
        fn now(&self) -> MonotonicInstant {
            self.0
                .lock()
                .expect("monotonic observations lock")
                .pop_front()
                .expect("test provides every observation")
        }
    }

    #[derive(Debug)]
    struct SnapshotRepository(Mutex<VecDeque<RecoverySnapshot>>);

    impl RecoveryRepository for SnapshotRepository {
        fn recovery_snapshot<'a>(
            &'a self,
            _execution_id: JobExecutionId,
            _current_owner: &'a OwnerToken,
        ) -> BoxFuture<'a, Result<RecoverySnapshot, RepositoryError>> {
            Box::pin(async move {
                self.0
                    .lock()
                    .expect("snapshot observations lock")
                    .pop_front()
                    .ok_or(RepositoryError::Unavailable)
            })
        }
    }

    fn snapshot(server_time: SystemTime, status: BatchStatus) -> RecoverySnapshot {
        RecoverySnapshot::new(
            JobExecutionId::new(7).expect("static id"),
            status,
            2,
            ExecutionVersion::new(3),
            OwnerObservation::OtherProcess,
            server_time - Duration::from_mins(16),
            server_time,
            None,
            RecoveryMarkers::new()
                .with_unknown_commit(status == BatchStatus::Unknown)
                .with_committed_flow_decision(true),
        )
    }

    #[tokio::test]
    async fn stale_proposal_is_version_bound_and_redacted() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10_000);
        let proposer = RecoveryProposer::new(
            SnapshotRepository(Mutex::new([snapshot(now, BatchStatus::Started)].into())),
            Arc::new(FixedWall(now)),
            Arc::new(SequenceMonotonic::observations([
                Duration::ZERO,
                Duration::from_millis(2),
            ])),
            OwnerToken::from_bytes([9; 16]),
        );

        let proposal = proposer
            .propose(JobExecutionId::new(7).expect("static id"))
            .await
            .expect("old foreign-owned execution is stale");

        assert_eq!(proposal.observed_version(), ExecutionVersion::new(3));
        assert_eq!(proposal.digest_hex().len(), 64);
        assert_eq!(proposal.evidence().owner(), OwnerObservation::OtherProcess);
        assert_eq!(proposal.evidence().inactivity(), Duration::from_mins(16));
        assert!(!format!("{proposal:?}").contains(&format!("{:?}", [9; 16])));
    }

    #[tokio::test]
    async fn current_owner_and_young_activity_do_not_become_stale() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10_000);
        let mut owned = snapshot(now, BatchStatus::Started);
        owned.owner = OwnerObservation::CurrentProcess;
        let proposer = RecoveryProposer::new(
            SnapshotRepository(Mutex::new([owned].into())),
            Arc::new(FixedWall(now)),
            Arc::new(SequenceMonotonic::observations([
                Duration::ZERO,
                Duration::ZERO,
            ])),
            OwnerToken::from_bytes([9; 16]),
        );
        assert_eq!(
            proposer
                .propose(JobExecutionId::new(7).expect("static id"))
                .await,
            Err(RecoveryError::OwnedByCurrentProcess)
        );

        let mut young = snapshot(now, BatchStatus::Starting);
        young.updated_at = now - Duration::from_mins(1);
        let proposer = RecoveryProposer::new(
            SnapshotRepository(Mutex::new([young].into())),
            Arc::new(FixedWall(now)),
            Arc::new(SequenceMonotonic::observations([
                Duration::ZERO,
                Duration::ZERO,
            ])),
            OwnerToken::from_bytes([9; 16]),
        );
        assert!(matches!(
            proposer
                .propose(JobExecutionId::new(7).expect("static id"))
                .await,
            Err(RecoveryError::NotStale { .. })
        ));
    }

    #[tokio::test]
    async fn backwards_repository_time_invalidates_the_next_observation() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10_000);
        let earlier = now - Duration::from_secs(1);
        let proposer = RecoveryProposer::new(
            SnapshotRepository(Mutex::new(
                [
                    snapshot(now, BatchStatus::Unknown),
                    snapshot(earlier, BatchStatus::Unknown),
                ]
                .into(),
            )),
            Arc::new(FixedWall(now)),
            Arc::new(SequenceMonotonic::observations([
                Duration::ZERO,
                Duration::from_millis(1),
                Duration::from_millis(2),
                Duration::from_millis(3),
            ])),
            OwnerToken::from_bytes([9; 16]),
        );
        proposer
            .propose(JobExecutionId::new(7).expect("static id"))
            .await
            .expect("first observation is usable");
        assert_eq!(
            proposer
                .propose(JobExecutionId::new(7).expect("static id"))
                .await,
            Err(RecoveryError::ClockEvidenceUnusable)
        );
    }

    #[tokio::test]
    async fn advancing_observation_time_preserves_a_durable_evidence_digest() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10_000);
        let first = snapshot(now, BatchStatus::Started);
        let mut second = snapshot(now + Duration::from_secs(1), BatchStatus::Started);
        second.updated_at = first.updated_at;
        let changed = snapshot(now + Duration::from_secs(2), BatchStatus::Started);
        let proposer = RecoveryProposer::new(
            SnapshotRepository(Mutex::new([first, second, changed].into())),
            Arc::new(FixedWall(now)),
            Arc::new(SequenceMonotonic::observations([
                Duration::ZERO,
                Duration::from_millis(1),
                Duration::from_millis(2),
                Duration::from_millis(3),
                Duration::from_millis(4),
                Duration::from_millis(5),
            ])),
            OwnerToken::from_bytes([9; 16]),
        );

        let id = JobExecutionId::new(7).expect("static id");
        let earlier = proposer.propose(id).await.expect("first proposal");
        let later = proposer.propose(id).await.expect("later proposal");
        let changed = proposer.propose(id).await.expect("changed proposal");

        assert_ne!(
            earlier.evidence().server_time(),
            later.evidence().server_time()
        );
        assert_eq!(earlier.digest(), later.digest());
        assert_ne!(later.digest(), changed.digest());
    }
}
