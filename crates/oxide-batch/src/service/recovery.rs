//! Evidence-bound stale-execution recovery proposals.
//!
//! A proposal is a bounded, value-redacted observation. It never mutates the
//! execution and it cannot authorize takeover. The snapshot, evidence, proposal,
//! and the port that gathers them live in `oxide-batch-repository`.

use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use crate::{
    BatchStatus, Clock, JobExecutionId, MaxClockSkew, MonotonicClock, OwnerObservation, OwnerToken,
    RecoveryError, RecoveryEvidence, RecoveryProposal, RecoveryRepository, StaleThreshold,
    TelemetryEventKind, TelemetryEventSink, TelemetryRecord,
};

/// Produces evidence-bound proposals without mutating repository state.
pub struct RecoveryProposer<R> {
    repository: R,
    wall_clock: Arc<dyn Clock>,
    monotonic_clock: Arc<dyn MonotonicClock>,
    current_owner: OwnerToken,
    stale_threshold: StaleThreshold,
    max_clock_skew: MaxClockSkew,
    server_time_floor: Mutex<Option<SystemTime>>,
    event_sink: Option<Arc<dyn TelemetryEventSink>>,
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
            event_sink: None,
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

    /// Attaches a non-authoritative, panic-isolated telemetry sink.
    #[must_use]
    pub fn with_event_sink(mut self, sink: Arc<dyn TelemetryEventSink>) -> Self {
        self.event_sink = Some(sink);
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
            if floor.is_some_and(|previous| snapshot.server_time() < previous) {
                return Err(RecoveryError::ClockEvidenceUnusable);
            }
            *floor = Some(snapshot.server_time());
        }
        let observation_window = after
            .checked_elapsed_since(before)
            .ok_or(RecoveryError::ClockEvidenceUnusable)?;
        if observation_window > self.max_clock_skew.get() {
            return Err(RecoveryError::ClockEvidenceUnusable);
        }
        let observed_clock_offset = absolute_system_difference(snapshot.server_time(), local_wall);
        if observed_clock_offset > self.max_clock_skew.get() {
            return Err(RecoveryError::ClockEvidenceUnusable);
        }
        let inactivity = snapshot
            .server_time()
            .duration_since(snapshot.updated_at())
            .map_err(|_| RecoveryError::ClockEvidenceUnusable)?;

        match snapshot.status() {
            BatchStatus::Unknown => {}
            BatchStatus::Starting | BatchStatus::Started | BatchStatus::Stopping => {
                if snapshot.owner() == OwnerObservation::CurrentProcess {
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

        let evidence = RecoveryEvidence::new(
            snapshot,
            inactivity,
            observed_clock_offset,
            observation_window,
        );
        let proposal = RecoveryProposal::new(evidence);
        if proposal.evidence().status() != BatchStatus::Unknown {
            crate::telemetry::emit_safely(
                self.event_sink.as_ref(),
                &TelemetryRecord::recovery(TelemetryEventKind::StaleDetected, &proposal),
            );
        }
        crate::telemetry::emit_safely(
            self.event_sink.as_ref(),
            &TelemetryRecord::recovery(TelemetryEventKind::RecoveryProposed, &proposal),
        );
        Ok(proposal)
    }
}

fn absolute_system_difference(left: SystemTime, right: SystemTime) -> Duration {
    left.duration_since(right)
        .unwrap_or_else(|_| right.duration_since(left).unwrap_or(Duration::MAX))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use super::*;
    use crate::{
        BoxFuture, ExecutionVersion, MonotonicInstant, RecoveryMarkers, RecoverySnapshot,
        RepositoryError,
    };

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
        snapshot_with(
            server_time,
            status,
            OwnerObservation::OtherProcess,
            server_time - Duration::from_mins(16),
        )
    }

    // The snapshot fields are private to `oxide-batch-repository`, so a case
    // that varies one builds the whole value rather than assigning to it.
    fn snapshot_with(
        server_time: SystemTime,
        status: BatchStatus,
        owner: OwnerObservation,
        updated_at: SystemTime,
    ) -> RecoverySnapshot {
        RecoverySnapshot::new(
            JobExecutionId::new(7).expect("static id"),
            status,
            2,
            ExecutionVersion::new(3),
            owner,
            updated_at,
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
        let owned = snapshot_with(
            now,
            BatchStatus::Started,
            OwnerObservation::CurrentProcess,
            now - Duration::from_mins(16),
        );
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

        let young = snapshot_with(
            now,
            BatchStatus::Starting,
            OwnerObservation::OtherProcess,
            now - Duration::from_mins(1),
        );
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
        let second = snapshot_with(
            now + Duration::from_secs(1),
            BatchStatus::Started,
            OwnerObservation::OtherProcess,
            now - Duration::from_mins(16),
        );
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
