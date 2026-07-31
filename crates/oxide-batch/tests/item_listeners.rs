//! Item, retry, and skip listener ordering, nesting, panic, and aggregation.

#![allow(clippy::expect_used, clippy::panic)]

use std::error::Error;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures_executor::block_on;
use oxide_batch::{
    BackoffOutcome, BackoffSleeper, BoxFuture, ChunkCount, ChunkDeliveryMode, ExecutionAttempt,
    ExecutionCorrelation, FailureCategory, FailureId, FailureSummary, FaultDescriptor, FaultPhase,
    ItemListenerContext, ItemListenerError, ItemListenerFailure, ItemListenerPhase,
    ItemListenerSet, JobExecutionId, JobInstanceId, JobName, ListenerError, ListenerFailureKind,
    ProcessListener, ReadListener, RetryListener, RetryOrdinal, RetryOutcome, SkipCounts,
    SkipListener, StepExecutionId, StepName, StopSource, StopToken, WriteListener,
};

/// A shared, ordered trace of every callback the set invoked.
#[derive(Clone, Debug, Default)]
struct Trace(Arc<Mutex<Vec<String>>>);

impl Trace {
    fn record(&self, entry: impl Into<String>) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(entry.into());
    }

    fn entries(&self) -> Vec<String> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// How one recording listener behaves at a chosen callback.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Behavior {
    Succeed,
    Fail,
    Panic,
}

/// A recording listener that participates in every M3 family.
struct Recorder {
    label: &'static str,
    trace: Trace,
    before: Behavior,
    after: Behavior,
    on_error: Behavior,
    on_skip: Behavior,
}

impl Recorder {
    fn new(label: &'static str, trace: &Trace) -> Self {
        Self {
            label,
            trace: trace.clone(),
            before: Behavior::Succeed,
            after: Behavior::Succeed,
            on_error: Behavior::Succeed,
            on_skip: Behavior::Succeed,
        }
    }

    const fn before(mut self, behavior: Behavior) -> Self {
        self.before = behavior;
        self
    }

    const fn after(mut self, behavior: Behavior) -> Self {
        self.after = behavior;
        self
    }

    const fn on_error(mut self, behavior: Behavior) -> Self {
        self.on_error = behavior;
        self
    }

    const fn on_skip(mut self, behavior: Behavior) -> Self {
        self.on_skip = behavior;
        self
    }

    fn run<'a>(
        &'a self,
        callback: &'static str,
        behavior: Behavior,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        self.trace.record(format!("{}:{callback}", self.label));
        match behavior {
            Behavior::Succeed => Box::pin(std::future::ready(Ok(()))),
            Behavior::Fail => Box::pin(std::future::ready(Err(ListenerError::new()))),
            Behavior::Panic => panic!("listener panicked before returning a future"),
        }
    }
}

impl ReadListener<u32> for Recorder {
    fn before_read<'a>(
        &'a self,
        _context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        self.run("before_read", self.before)
    }

    fn after_read<'a>(
        &'a self,
        _item: &'a u32,
        _context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        self.run("after_read", self.after)
    }

    fn on_read_error<'a>(
        &'a self,
        _fault: FaultDescriptor,
        _context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        self.run("on_read_error", self.on_error)
    }
}

impl ProcessListener<u32, String> for Recorder {
    fn before_process<'a>(
        &'a self,
        _input: &'a u32,
        _context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        self.run("before_process", self.before)
    }

    fn after_process<'a>(
        &'a self,
        _input: &'a u32,
        _output: Option<&'a String>,
        _context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        self.run("after_process", self.after)
    }
}

impl WriteListener<String> for Recorder {
    fn before_write<'a>(
        &'a self,
        _outputs: &'a [String],
        _context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        self.run("before_write", self.before)
    }

    fn after_write<'a>(
        &'a self,
        _outputs: &'a [String],
        _context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        self.run("after_write", self.after)
    }
}

impl RetryListener for Recorder {
    fn before_retry<'a>(
        &'a self,
        _fault: FaultDescriptor,
        _context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        self.run("before_retry", self.before)
    }

    fn after_retry<'a>(
        &'a self,
        _fault: FaultDescriptor,
        _outcome: RetryOutcome,
        _context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        self.run("after_retry", self.after)
    }

    fn on_retry_exhausted<'a>(
        &'a self,
        _fault: FaultDescriptor,
        _context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        self.run("on_retry_exhausted", self.on_error)
    }
}

impl SkipListener<u32, String> for Recorder {
    fn on_skip_in_read<'a>(
        &'a self,
        _fault: FaultDescriptor,
        _context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        self.run("on_skip_in_read", self.on_skip)
    }

    fn on_skip_in_write<'a>(
        &'a self,
        _output: &'a String,
        _fault: FaultDescriptor,
        _context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        self.run("on_skip_in_write", self.on_skip)
    }
}

/// A default listener that overrides nothing.
struct Silent;

impl ReadListener<u32> for Silent {}
impl ProcessListener<u32, String> for Silent {}
impl WriteListener<String> for Silent {}
impl RetryListener for Silent {}
impl SkipListener<u32, String> for Silent {}

fn correlation() -> Result<ExecutionCorrelation, Box<dyn Error>> {
    let attempt = NonZeroU64::new(1).ok_or("attempts are nonzero")?;
    Ok(ExecutionCorrelation::new(
        JobName::new("fault_listener_job")?,
        JobInstanceId::new(1)?,
        JobExecutionId::new(2)?,
        ExecutionAttempt::new(attempt),
        StepName::new("fault_listener_step")?,
        StepExecutionId::new(3)?,
        ExecutionAttempt::new(attempt),
    ))
}

fn fault() -> Result<FaultDescriptor, Box<dyn Error>> {
    Ok(FaultDescriptor::new(
        FaultPhase::Read,
        FailureSummary::new(FailureCategory::Timeout, FailureId::new(11)?),
        RetryOrdinal::INITIAL,
        SkipCounts::ZERO,
        true,
        ChunkDeliveryMode::AtomicSameResource,
    ))
}

fn phases(
    failures: &[ItemListenerFailure],
) -> Vec<(ItemListenerPhase, usize, ListenerFailureKind)> {
    failures
        .iter()
        .map(|failure| {
            (
                failure.phase(),
                failure.registration_index(),
                failure.kind(),
            )
        })
        .collect()
}

#[test]
fn item_listeners_nest_and_reverse_after_order() -> Result<(), Box<dyn Error>> {
    let trace = Trace::default();
    let set = ItemListenerSet::<u32, String>::new()
        .with_read_listener(Arc::new(Recorder::new("first", &trace)))?
        .with_read_listener(Arc::new(Recorder::new("second", &trace)))?
        .with_read_listener(Arc::new(Recorder::new("third", &trace)))?;

    let correlation = correlation()?;
    let (_source, stop) = StopSource::new();
    let context = ItemListenerContext::new(&correlation, ChunkCount::new(0), &stop);

    let entered = block_on(set.before_read(context));
    assert!(entered.is_ok());
    assert_eq!(entered.entered(), 3);

    let item = 42_u32;
    let failures = block_on(set.after_read(entered.entered(), &item, context));
    assert!(failures.is_empty());

    assert_eq!(
        trace.entries(),
        vec![
            "first:before_read",
            "second:before_read",
            "third:before_read",
            "third:after_read",
            "second:after_read",
            "first:after_read",
        ]
    );
    Ok(())
}

#[test]
fn a_before_failure_prevents_its_component_call_and_unwinds_entered_listeners()
-> Result<(), Box<dyn Error>> {
    let trace = Trace::default();
    let set = ItemListenerSet::<u32, String>::new()
        .with_process_listener(Arc::new(Recorder::new("first", &trace)))?
        .with_process_listener(Arc::new(
            Recorder::new("second", &trace).before(Behavior::Fail),
        ))?
        .with_process_listener(Arc::new(Recorder::new("third", &trace)))?;

    let correlation = correlation()?;
    let (_source, stop) = StopSource::new();
    let context = ItemListenerContext::new(&correlation, ChunkCount::new(1), &stop);

    let input = 7_u32;
    let entered = block_on(set.before_process(&input, context));
    assert_eq!(entered.entered(), 1, "only the first listener completed");
    assert_eq!(
        entered.failure().map(|failure| (
            failure.phase(),
            failure.registration_index(),
            failure.kind()
        )),
        Some((
            ItemListenerPhase::BeforeProcess,
            1,
            ListenerFailureKind::Error
        ))
    );

    let failures = block_on(set.after_process(entered.entered(), &input, None, context));
    assert!(failures.is_empty());
    assert_eq!(
        trace.entries(),
        vec![
            "first:before_process",
            "second:before_process",
            "first:after_process",
        ],
        "the failed and unreached listeners receive no completion callback"
    );
    Ok(())
}

#[test]
fn an_after_failure_is_reported_for_an_uncommitted_chunk() -> Result<(), Box<dyn Error>> {
    let trace = Trace::default();
    let set = ItemListenerSet::<u32, String>::new()
        .with_write_listener(Arc::new(Recorder::new("first", &trace)))?
        .with_write_listener(Arc::new(
            Recorder::new("second", &trace).after(Behavior::Fail),
        ))?;

    let correlation = correlation()?;
    let (_source, stop) = StopSource::new();
    let context = ItemListenerContext::new(&correlation, ChunkCount::new(7), &stop);

    let outputs = vec!["one".to_owned(), "two".to_owned()];
    let entered = block_on(set.before_write(&outputs, context));
    assert!(entered.is_ok());

    let failures = block_on(set.after_write(entered.entered(), &outputs, context));
    assert_eq!(
        phases(&failures),
        vec![(ItemListenerPhase::AfterWrite, 1, ListenerFailureKind::Error)]
    );
    assert_eq!(
        trace.entries(),
        vec![
            "first:before_write",
            "second:before_write",
            "second:after_write",
            "first:after_write",
        ],
        "the surviving listener still completes so failures can be aggregated"
    );
    Ok(())
}

#[test]
fn a_panicking_listener_is_classified_exactly_like_an_error() -> Result<(), Box<dyn Error>> {
    let trace = Trace::default();
    let set = ItemListenerSet::<u32, String>::new()
        .with_write_listener(Arc::new(Recorder::new("first", &trace)))?
        .with_write_listener(Arc::new(
            Recorder::new("second", &trace).before(Behavior::Panic),
        ))?;

    let correlation = correlation()?;
    let (_source, stop) = StopSource::new();
    let context = ItemListenerContext::new(&correlation, ChunkCount::new(2), &stop);

    let outputs = vec!["one".to_owned()];
    let entered = block_on(set.before_write(&outputs, context));
    assert_eq!(entered.entered(), 1);
    assert_eq!(
        entered.failure().map(ItemListenerFailure::kind),
        Some(ListenerFailureKind::Panic)
    );
    Ok(())
}

#[test]
fn every_entered_reverse_callback_runs_so_failures_aggregate() -> Result<(), Box<dyn Error>> {
    let trace = Trace::default();
    let set = ItemListenerSet::<u32, String>::new()
        .with_read_listener(Arc::new(
            Recorder::new("first", &trace).on_error(Behavior::Fail),
        ))?
        .with_read_listener(Arc::new(Recorder::new("second", &trace)))?
        .with_read_listener(Arc::new(
            Recorder::new("third", &trace).on_error(Behavior::Panic),
        ))?;

    let correlation = correlation()?;
    let (_source, stop) = StopSource::new();
    let context = ItemListenerContext::new(&correlation, ChunkCount::new(3), &stop);

    let failures = block_on(set.on_read_error(3, fault()?, context));
    assert_eq!(
        phases(&failures),
        vec![
            (ItemListenerPhase::ReadError, 2, ListenerFailureKind::Panic),
            (ItemListenerPhase::ReadError, 0, ListenerFailureKind::Error),
        ]
    );
    assert_eq!(
        trace.entries(),
        vec![
            "third:on_read_error",
            "second:on_read_error",
            "first:on_read_error",
        ]
    );
    Ok(())
}

#[test]
fn retry_callbacks_run_forward_before_backoff_and_reverse_after_completion()
-> Result<(), Box<dyn Error>> {
    let trace = Trace::default();
    let set = ItemListenerSet::<u32, String>::new()
        .with_retry_listener(Arc::new(Recorder::new("first", &trace)))?
        .with_retry_listener(Arc::new(Recorder::new("second", &trace)))?;

    let correlation = correlation()?;
    let (_source, stop) = StopSource::new();
    let context = ItemListenerContext::new(&correlation, ChunkCount::new(4), &stop);
    let fault = fault()?;

    let entered = block_on(set.before_retry(fault, context));
    assert_eq!(entered.entered(), 2);
    let completion =
        block_on(set.after_retry(entered.entered(), fault, RetryOutcome::Recovered, context));
    assert!(completion.is_empty());
    let exhausted = block_on(set.on_retry_exhausted(entered.entered(), fault, context));
    assert!(exhausted.is_empty());

    assert_eq!(
        trace.entries(),
        vec![
            "first:before_retry",
            "second:before_retry",
            "second:after_retry",
            "first:after_retry",
            "second:on_retry_exhausted",
            "first:on_retry_exhausted",
        ]
    );
    Ok(())
}

#[test]
fn skip_callbacks_run_in_registration_order_and_report_every_failure() -> Result<(), Box<dyn Error>>
{
    let trace = Trace::default();
    let set = ItemListenerSet::<u32, String>::new()
        .with_skip_listener(Arc::new(Recorder::new("first", &trace)))?
        .with_skip_listener(Arc::new(
            Recorder::new("second", &trace).on_skip(Behavior::Fail),
        ))?
        .with_skip_listener(Arc::new(Recorder::new("third", &trace)))?;

    let correlation = correlation()?;
    let (_source, stop) = StopSource::new();
    let context = ItemListenerContext::new(&correlation, ChunkCount::new(5), &stop);

    let failures = block_on(set.on_skip_in_read(fault()?, context));
    assert_eq!(
        phases(&failures),
        vec![(ItemListenerPhase::Skip, 1, ListenerFailureKind::Error)]
    );
    assert_eq!(
        trace.entries(),
        vec![
            "first:on_skip_in_read",
            "second:on_skip_in_read",
            "third:on_skip_in_read",
        ],
        "an accepted skip is confirmed by every registered listener"
    );

    let output = "value".to_owned();
    let write_failures = block_on(set.on_skip_in_write(&output, fault()?, context));
    assert_eq!(write_failures.len(), 1);
    Ok(())
}

#[test]
fn default_listener_methods_observe_without_failing() -> Result<(), Box<dyn Error>> {
    let set = ItemListenerSet::<u32, String>::new()
        .with_read_listener(Arc::new(Silent))?
        .with_process_listener(Arc::new(Silent))?
        .with_write_listener(Arc::new(Silent))?
        .with_retry_listener(Arc::new(Silent))?
        .with_skip_listener(Arc::new(Silent))?;
    assert!(!set.is_empty());

    let correlation = correlation()?;
    let (_source, stop) = StopSource::new();
    let context = ItemListenerContext::new(&correlation, ChunkCount::new(6), &stop);

    assert!(block_on(set.before_read(context)).is_ok());
    let input = 1_u32;
    assert!(block_on(set.before_process(&input, context)).is_ok());
    let outputs = vec!["value".to_owned()];
    assert!(block_on(set.before_write(&outputs, context)).is_ok());
    assert!(block_on(set.after_write(1, &outputs, context)).is_empty());
    assert!(block_on(set.on_write_error(1, &outputs, fault()?, context)).is_empty());
    assert!(block_on(set.on_process_error(1, &input, fault()?, context)).is_empty());
    assert!(block_on(set.on_skip_in_process(&input, fault()?, context)).is_empty());
    Ok(())
}

#[test]
fn listener_registration_is_bounded_per_family() -> Result<(), Box<dyn Error>> {
    let mut set = ItemListenerSet::<u32, String>::new();
    for _ in 0..ItemListenerSet::<u32, String>::MAX_LISTENERS {
        set = set.with_read_listener(Arc::new(Silent))?;
    }
    assert_eq!(
        set.read_listeners(),
        ItemListenerSet::<u32, String>::MAX_LISTENERS
    );
    assert_eq!(
        set.with_read_listener(Arc::new(Silent)).err(),
        Some(ItemListenerError::TooManyListeners {
            max: ItemListenerSet::<u32, String>::MAX_LISTENERS
        })
    );
    Ok(())
}

/// A monotonic sleeper that never touches wall-clock time.
struct ControlledSleeper {
    observed: Mutex<Vec<Duration>>,
}

impl ControlledSleeper {
    fn new() -> Self {
        Self {
            observed: Mutex::new(Vec::new()),
        }
    }

    fn observed(&self) -> Vec<Duration> {
        self.observed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl BackoffSleeper for ControlledSleeper {
    fn sleep<'a>(&'a self, delay: Duration, stop: &'a StopToken) -> BoxFuture<'a, BackoffOutcome> {
        self.observed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(delay);
        let stopped = stop.is_stop_requested();
        Box::pin(async move {
            if stopped {
                BackoffOutcome::Stopped
            } else {
                BackoffOutcome::Elapsed
            }
        })
    }
}

#[test]
fn stop_during_backoff_cancels_the_wait_without_wall_clock_time() {
    let sleeper = ControlledSleeper::new();
    let (source, stop) = StopSource::new();

    assert_eq!(
        block_on(sleeper.sleep(Duration::from_millis(50), &stop)),
        BackoffOutcome::Elapsed
    );

    source.request_stop();
    assert_eq!(
        block_on(sleeper.sleep(Duration::from_millis(100), &stop)),
        BackoffOutcome::Stopped
    );
    assert_eq!(
        sleeper.observed(),
        vec![Duration::from_millis(50), Duration::from_millis(100)]
    );
}
