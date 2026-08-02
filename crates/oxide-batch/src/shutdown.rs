//! Application-owned graceful-shutdown coordination.
//!
//! The coordinator owns only tasks explicitly spawned through it. It installs
//! no process signal handler and creates no runtime. Applications translate
//! their chosen signal source into [`ShutdownSignal::request_shutdown`].

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use futures_util::FutureExt;
use tokio::sync::Notify;
use tokio::task::JoinSet;

const ACCEPTING: u8 = 0;
const STOPPING: u8 = 1;
const ESCALATED: u8 = 2;

/// The lower bound for process drain and task-join deadlines.
pub const MIN_SHUTDOWN_DEADLINE: Duration = Duration::from_secs(1);
/// The upper bound for process drain and task-join deadlines.
pub const MAX_SHUTDOWN_DEADLINE: Duration = Duration::from_hours(1);
/// The default process drain deadline.
pub const DEFAULT_SHUTDOWN_DEADLINE: Duration = Duration::from_secs(30);
/// The lower bound for telemetry flush deadlines.
pub const MIN_TELEMETRY_FLUSH_DEADLINE: Duration = Duration::from_millis(100);
/// The upper bound for telemetry flush deadlines.
pub const MAX_TELEMETRY_FLUSH_DEADLINE: Duration = Duration::from_mins(1);
/// The default telemetry flush deadline.
pub const DEFAULT_TELEMETRY_FLUSH_DEADLINE: Duration = Duration::from_secs(5);

/// The total correctness budget for intake stop through durable persistence.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ShutdownDeadline(Duration);

impl ShutdownDeadline {
    /// Validates `1 s..=1 h`.
    ///
    /// # Errors
    ///
    /// Returns [`ShutdownError::InvalidShutdownDeadline`] outside the bound.
    pub fn new(value: Duration) -> Result<Self, ShutdownError> {
        if !(MIN_SHUTDOWN_DEADLINE..=MAX_SHUTDOWN_DEADLINE).contains(&value) {
            return Err(ShutdownError::InvalidShutdownDeadline);
        }
        Ok(Self(value))
    }

    /// Returns the validated duration.
    #[must_use]
    pub const fn get(self) -> Duration {
        self.0
    }
}

impl Default for ShutdownDeadline {
    fn default() -> Self {
        Self(DEFAULT_SHUTDOWN_DEADLINE)
    }
}

/// The bounded budget for joining every owned child task.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TaskJoinDeadline(Duration);

impl TaskJoinDeadline {
    /// Validates `1 s..=1 h` and the enclosing shutdown budget.
    ///
    /// # Errors
    ///
    /// Returns a typed configuration error when the value is out of range or
    /// exceeds `shutdown`.
    pub fn new(value: Duration, shutdown: ShutdownDeadline) -> Result<Self, ShutdownError> {
        if !(MIN_SHUTDOWN_DEADLINE..=MAX_SHUTDOWN_DEADLINE).contains(&value) {
            return Err(ShutdownError::InvalidTaskJoinDeadline);
        }
        if value > shutdown.get() {
            return Err(ShutdownError::TaskJoinExceedsShutdown);
        }
        Ok(Self(value))
    }

    /// Returns the validated duration.
    #[must_use]
    pub const fn get(self) -> Duration {
        self.0
    }
}

impl Default for TaskJoinDeadline {
    fn default() -> Self {
        Self(DEFAULT_SHUTDOWN_DEADLINE)
    }
}

/// The separate, non-correctness telemetry flush budget.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TelemetryFlushDeadline(Duration);

impl TelemetryFlushDeadline {
    /// Validates `100 ms..=60 s`.
    ///
    /// # Errors
    ///
    /// Returns [`ShutdownError::InvalidTelemetryFlushDeadline`] outside the
    /// bound.
    pub fn new(value: Duration) -> Result<Self, ShutdownError> {
        if !(MIN_TELEMETRY_FLUSH_DEADLINE..=MAX_TELEMETRY_FLUSH_DEADLINE).contains(&value) {
            return Err(ShutdownError::InvalidTelemetryFlushDeadline);
        }
        Ok(Self(value))
    }

    /// Returns the validated duration.
    #[must_use]
    pub const fn get(self) -> Duration {
        self.0
    }
}

impl Default for TelemetryFlushDeadline {
    fn default() -> Self {
        Self(DEFAULT_TELEMETRY_FLUSH_DEADLINE)
    }
}

#[derive(Debug)]
struct SignalState {
    state: AtomicU8,
    notify: Notify,
}

/// An application-owned process-shutdown signal.
#[derive(Clone, Debug)]
pub struct ShutdownSignal {
    state: Arc<SignalState>,
}

impl ShutdownSignal {
    fn new() -> Self {
        Self {
            state: Arc::new(SignalState {
                state: AtomicU8::new(ACCEPTING),
                notify: Notify::new(),
            }),
        }
    }

    /// Stops intake on the first request and escalates waiting on the second.
    #[must_use]
    pub fn request_shutdown(&self) -> ShutdownRequest {
        loop {
            let current = self.state.state.load(Ordering::Acquire);
            let (next, outcome) = match current {
                ACCEPTING => (STOPPING, ShutdownRequest::Initiated),
                STOPPING => (ESCALATED, ShutdownRequest::Escalated),
                _ => return ShutdownRequest::AlreadyEscalated,
            };
            if self
                .state
                .state
                .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.state.notify.notify_waiters();
                return outcome;
            }
        }
    }

    /// Returns whether shutdown has stopped new intake.
    #[must_use]
    pub fn is_shutdown_requested(&self) -> bool {
        self.state.state.load(Ordering::Acquire) >= STOPPING
    }

    /// Returns whether a second request escalated join waiting.
    #[must_use]
    pub fn is_escalated(&self) -> bool {
        self.state.state.load(Ordering::Acquire) >= ESCALATED
    }

    /// Waits for the first request.
    pub async fn cancelled(&self) {
        self.wait_for(STOPPING).await;
    }

    async fn escalated(&self) {
        self.wait_for(ESCALATED).await;
    }

    async fn wait_for(&self, target: u8) {
        loop {
            let notified = self.state.notify.notified();
            if self.state.state.load(Ordering::Acquire) >= target {
                return;
            }
            notified.await;
        }
    }

    fn begin_shutdown(&self) {
        if self
            .state
            .state
            .compare_exchange(ACCEPTING, STOPPING, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.state.notify.notify_waiters();
        }
    }

    /// Rejects new work after the first shutdown request.
    ///
    /// # Errors
    ///
    /// Returns [`ShutdownError::ShuttingDown`] once intake has stopped.
    pub fn ensure_accepting(&self) -> Result<(), ShutdownError> {
        if self.is_shutdown_requested() {
            Err(ShutdownError::ShuttingDown)
        } else {
            Ok(())
        }
    }
}

/// Classification of one shutdown request.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum ShutdownRequest {
    /// The first request stopped intake and began cooperative cancellation.
    Initiated,
    /// The second request stopped waiting for the join deadline.
    Escalated,
    /// Waiting had already been escalated.
    AlreadyEscalated,
}

/// The bounded phase occupied by one owned child.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum ShutdownTaskPhase {
    /// Tasklet or listener work outside a chunk transaction.
    Tasklet,
    /// Reading or processing an open chunk.
    ChunkReadProcess,
    /// Writing an open chunk.
    ChunkWrite,
    /// Resolving a transaction commit or rollback.
    Transaction,
    /// Waiting in bounded retry backoff.
    RetryBackoff,
    /// Persisting or selecting a durable flow decision.
    FlowDecision,
}

/// One phase and the number of unjoined tasks observed there.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UnjoinedPhase {
    phase: ShutdownTaskPhase,
    count: usize,
}

impl UnjoinedPhase {
    /// Returns the phase.
    #[must_use]
    pub const fn phase(self) -> ShutdownTaskPhase {
        self.phase
    }

    /// Returns the number of tasks still owned in that phase.
    #[must_use]
    pub const fn count(self) -> usize {
        self.count
    }
}

/// Result of joining the structured task tree.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DrainResult {
    /// Every task joined; panics were observed rather than detached.
    Complete {
        /// Number of child panics observed at the join boundary.
        panicked_tasks: usize,
    },
    /// One or more tasks remained owned when the deadline or escalation won.
    Incomplete {
        /// Total number of unjoined children.
        unjoined_tasks: usize,
        /// Stable phase-ordered unjoined counts.
        phases: Vec<UnjoinedPhase>,
        /// Number of panics already observed before waiting ended.
        panicked_tasks: usize,
        /// Whether a second request ended waiting.
        escalated: bool,
    },
}

/// Status of one correctness or resource-close hook.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ShutdownHookStatus {
    /// The hook completed successfully.
    Completed,
    /// The hook returned a typed failure to the application boundary.
    Failed,
    /// The total correctness deadline expired before persistence completed.
    DeadlineExceeded,
}

/// Status of the non-authoritative telemetry flush.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TelemetryFlushStatus {
    /// The exporter completed and reported its dropped-event count.
    Completed {
        /// Events the exporter could not deliver during the flush.
        dropped_events: u64,
    },
    /// The exporter returned a failure.
    Failed,
    /// The separate flush deadline expired.
    DeadlineExceeded,
}

/// Complete ordered shutdown report.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShutdownReport {
    drain: DrainResult,
    persistence: ShutdownHookStatus,
    telemetry: TelemetryFlushStatus,
    repository_close: ShutdownHookStatus,
}

/// A value-redacted failure returned by an application shutdown hook.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShutdownHookError;

impl fmt::Display for ShutdownHookError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("shutdown hook failed")
    }
}

impl Error for ShutdownHookError {}

impl ShutdownReport {
    /// Borrows the structured drain result.
    #[must_use]
    pub const fn drain(&self) -> &DrainResult {
        &self.drain
    }

    /// Returns the durable-persistence hook status.
    #[must_use]
    pub const fn persistence(&self) -> ShutdownHookStatus {
        self.persistence
    }

    /// Returns the separately bounded telemetry status.
    #[must_use]
    pub const fn telemetry(&self) -> TelemetryFlushStatus {
        self.telemetry
    }

    /// Returns the repository-close hook status.
    #[must_use]
    pub const fn repository_close(&self) -> ShutdownHookStatus {
        self.repository_close
    }
}

/// Owns the Tokio adapter task set for one application runtime.
pub struct ShutdownCoordinator {
    signal: ShutdownSignal,
    shutdown_deadline: ShutdownDeadline,
    task_join_deadline: TaskJoinDeadline,
    telemetry_deadline: TelemetryFlushDeadline,
    tasks: JoinSet<(ShutdownTaskPhase, bool)>,
    phases: BTreeMap<ShutdownTaskPhase, usize>,
}

impl fmt::Debug for ShutdownCoordinator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ShutdownCoordinator")
            .field("shutdown_deadline", &self.shutdown_deadline)
            .field("task_join_deadline", &self.task_join_deadline)
            .field("telemetry_deadline", &self.telemetry_deadline)
            .field("owned_tasks", &self.tasks.len())
            .finish_non_exhaustive()
    }
}

impl ShutdownCoordinator {
    /// Constructs an empty application-owned coordinator.
    ///
    /// # Errors
    ///
    /// Returns [`ShutdownError::TaskJoinExceedsShutdown`] when the supplied
    /// join budget exceeds the total correctness budget.
    pub fn new(
        shutdown_deadline: ShutdownDeadline,
        task_join_deadline: TaskJoinDeadline,
        telemetry_deadline: TelemetryFlushDeadline,
    ) -> Result<Self, ShutdownError> {
        if task_join_deadline.get() > shutdown_deadline.get() {
            return Err(ShutdownError::TaskJoinExceedsShutdown);
        }
        Ok(Self {
            signal: ShutdownSignal::new(),
            shutdown_deadline,
            task_join_deadline,
            telemetry_deadline,
            tasks: JoinSet::new(),
            phases: BTreeMap::new(),
        })
    }

    /// Returns the application-owned handle used by API or signal adapters.
    #[must_use]
    pub fn signal(&self) -> ShutdownSignal {
        self.signal.clone()
    }

    /// Spawns one task into the coordinator's structured ownership set.
    ///
    /// Panics are caught at this boundary and counted in the shutdown report.
    /// The coordinator never detaches or force-aborts an in-flight task.
    ///
    /// # Errors
    ///
    /// Returns [`ShutdownError::ShuttingDown`] after intake stops.
    pub fn spawn<F>(&mut self, phase: ShutdownTaskPhase, future: F) -> Result<(), ShutdownError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.signal.ensure_accepting()?;
        *self.phases.entry(phase).or_default() += 1;
        self.tasks.spawn(async move {
            let panicked = AssertUnwindSafe(future).catch_unwind().await.is_err();
            (phase, panicked)
        });
        Ok(())
    }

    /// Runs the fixed shutdown sequence.
    ///
    /// The persistence closure runs after joining, telemetry uses its separate
    /// deadline, and repository close runs last. Closure errors are reported
    /// without changing the previously established drain result. The
    /// persistence closure must enforce its repository statement and commit
    /// timeouts; this coordinator never cancels an in-flight persistence
    /// future at the outer correctness deadline.
    pub async fn shutdown<P, PF, T, TF, C, CF>(
        &mut self,
        persist: P,
        flush_telemetry: T,
        close_repository: C,
    ) -> ShutdownReport
    where
        P: FnOnce() -> PF,
        PF: Future<Output = Result<(), ShutdownHookError>>,
        T: FnOnce() -> TF,
        TF: Future<Output = Result<u64, ShutdownHookError>>,
        C: FnOnce() -> CF,
        CF: Future<Output = Result<(), ShutdownHookError>>,
    {
        // Entering coordination starts the first request when necessary, but
        // never turns an already-recorded application request into escalation.
        self.signal.begin_shutdown();
        let started = tokio::time::Instant::now();
        let correctness_end = started + self.shutdown_deadline.get();
        let join_end = started + self.task_join_deadline.get();
        let mut panicked_tasks = 0;
        let mut escalated = false;

        while !self.tasks.is_empty() {
            tokio::select! {
                joined = self.tasks.join_next() => {
                    if let Some(Ok((phase, panicked))) = joined {
                        panicked_tasks += usize::from(panicked);
                        decrement_phase(&mut self.phases, phase);
                    }
                }
                () = tokio::time::sleep_until(join_end) => break,
                () = self.signal.escalated() => {
                    escalated = true;
                    break;
                }
            }
        }

        let drain = if self.tasks.is_empty() {
            DrainResult::Complete { panicked_tasks }
        } else {
            DrainResult::Incomplete {
                unjoined_tasks: self.tasks.len(),
                phases: self
                    .phases
                    .iter()
                    .map(|(phase, count)| UnjoinedPhase {
                        phase: *phase,
                        count: *count,
                    })
                    .collect(),
                panicked_tasks,
                escalated,
            }
        };

        // Persistence owns its repository statement/commit timeout. Dropping
        // this future at the process deadline could cancel an in-flight commit
        // and manufacture ambiguity, so always observe its result and report
        // a missed outer deadline afterwards.
        let persisted = persist().await;
        let persistence = if tokio::time::Instant::now() > correctness_end {
            ShutdownHookStatus::DeadlineExceeded
        } else {
            match persisted {
                Ok(()) => ShutdownHookStatus::Completed,
                Err(_) => ShutdownHookStatus::Failed,
            }
        };
        let telemetry =
            match tokio::time::timeout(self.telemetry_deadline.get(), flush_telemetry()).await {
                Ok(Ok(dropped_events)) => TelemetryFlushStatus::Completed { dropped_events },
                Ok(Err(_)) => TelemetryFlushStatus::Failed,
                Err(_) => TelemetryFlushStatus::DeadlineExceeded,
            };
        let repository_close = match close_repository().await {
            Ok(()) => ShutdownHookStatus::Completed,
            Err(_) => ShutdownHookStatus::Failed,
        };

        ShutdownReport {
            drain,
            persistence,
            telemetry,
            repository_close,
        }
    }
}

impl Default for ShutdownCoordinator {
    fn default() -> Self {
        Self {
            signal: ShutdownSignal::new(),
            shutdown_deadline: ShutdownDeadline::default(),
            task_join_deadline: TaskJoinDeadline::default(),
            telemetry_deadline: TelemetryFlushDeadline::default(),
            tasks: JoinSet::new(),
            phases: BTreeMap::new(),
        }
    }
}

fn decrement_phase(phases: &mut BTreeMap<ShutdownTaskPhase, usize>, phase: ShutdownTaskPhase) {
    if let Some(count) = phases.get_mut(&phase) {
        *count = count.saturating_sub(1);
        if *count == 0 {
            phases.remove(&phase);
        }
    }
}

/// A shutdown intake or configuration error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ShutdownError {
    /// New work was offered after shutdown stopped intake.
    ShuttingDown,
    /// The total deadline was outside `1 s..=1 h`.
    InvalidShutdownDeadline,
    /// The join deadline was outside `1 s..=1 h`.
    InvalidTaskJoinDeadline,
    /// The join deadline exceeded the total correctness deadline.
    TaskJoinExceedsShutdown,
    /// The telemetry deadline was outside `100 ms..=60 s`.
    InvalidTelemetryFlushDeadline,
}

impl fmt::Display for ShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ShuttingDown => formatter.write_str("runtime intake is shutting down"),
            Self::InvalidShutdownDeadline => {
                formatter.write_str("shutdown deadline must be between 1 second and 1 hour")
            }
            Self::InvalidTaskJoinDeadline => {
                formatter.write_str("task join deadline must be between 1 second and 1 hour")
            }
            Self::TaskJoinExceedsShutdown => {
                formatter.write_str("task join deadline cannot exceed shutdown deadline")
            }
            Self::InvalidTelemetryFlushDeadline => formatter
                .write_str("telemetry flush deadline must be between 100 ms and 60 seconds"),
        }
    }
}

impl Error for ShutdownError {}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use std::sync::{Arc, Mutex};

    use super::*;

    #[test]
    fn deadlines_enforce_accepted_bounds_and_relationship() {
        let shutdown = ShutdownDeadline::new(Duration::from_secs(2)).expect("valid deadline");
        assert_eq!(
            TaskJoinDeadline::new(Duration::from_secs(3), shutdown),
            Err(ShutdownError::TaskJoinExceedsShutdown)
        );
        assert_eq!(
            TelemetryFlushDeadline::new(Duration::from_millis(99)),
            Err(ShutdownError::InvalidTelemetryFlushDeadline)
        );
    }

    #[test]
    fn first_request_stops_intake_and_second_escalates() {
        let coordinator = ShutdownCoordinator::default();
        let signal = coordinator.signal();
        assert_eq!(signal.ensure_accepting(), Ok(()));
        assert_eq!(signal.request_shutdown(), ShutdownRequest::Initiated);
        assert_eq!(signal.ensure_accepting(), Err(ShutdownError::ShuttingDown));
        assert_eq!(signal.request_shutdown(), ShutdownRequest::Escalated);
        assert!(signal.is_escalated());
    }

    #[tokio::test]
    async fn phases_and_hooks_complete_in_fixed_order() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut coordinator = ShutdownCoordinator::default();
        let task_events = Arc::clone(&events);
        coordinator
            .spawn(ShutdownTaskPhase::Tasklet, async move {
                task_events.lock().expect("events lock").push("task");
            })
            .expect("intake is open");

        let persist_events = Arc::clone(&events);
        let telemetry_events = Arc::clone(&events);
        let close_events = Arc::clone(&events);
        let report = coordinator
            .shutdown(
                || async move {
                    persist_events.lock().expect("events lock").push("persist");
                    Ok::<_, ShutdownHookError>(())
                },
                || async move {
                    telemetry_events
                        .lock()
                        .expect("events lock")
                        .push("telemetry");
                    Ok::<_, ShutdownHookError>(0)
                },
                || async move {
                    close_events.lock().expect("events lock").push("close");
                    Ok::<_, ShutdownHookError>(())
                },
            )
            .await;

        assert_eq!(report.drain(), &DrainResult::Complete { panicked_tasks: 0 });
        assert_eq!(
            *events.lock().expect("events lock"),
            vec!["task", "persist", "telemetry", "close"]
        );
    }

    #[tokio::test]
    async fn an_existing_first_request_is_not_treated_as_escalation() {
        let mut coordinator = ShutdownCoordinator::default();
        let signal = coordinator.signal();
        assert_eq!(signal.request_shutdown(), ShutdownRequest::Initiated);

        let report = coordinator
            .shutdown(
                || async { Ok::<_, ShutdownHookError>(()) },
                || async { Ok::<_, ShutdownHookError>(0) },
                || async { Ok::<_, ShutdownHookError>(()) },
            )
            .await;

        assert!(!signal.is_escalated());
        assert_eq!(report.drain(), &DrainResult::Complete { panicked_tasks: 0 });
    }

    #[tokio::test]
    async fn escalation_reports_every_unjoined_phase_without_detaching() {
        let mut coordinator = ShutdownCoordinator::default();
        coordinator
            .spawn(ShutdownTaskPhase::Transaction, std::future::pending())
            .expect("intake is open");
        let signal = coordinator.signal();
        let escalator = signal.clone();
        tokio::spawn(async move {
            escalator.cancelled().await;
            let _ = escalator.request_shutdown();
        });

        let report = coordinator
            .shutdown(
                || async { Ok::<_, ShutdownHookError>(()) },
                || async { Ok::<_, ShutdownHookError>(0) },
                || async { Ok::<_, ShutdownHookError>(()) },
            )
            .await;

        assert_eq!(
            report.drain(),
            &DrainResult::Incomplete {
                unjoined_tasks: 1,
                phases: vec![UnjoinedPhase {
                    phase: ShutdownTaskPhase::Transaction,
                    count: 1,
                }],
                panicked_tasks: 0,
                escalated: true,
            }
        );
    }
}
