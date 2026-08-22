//! #147 malformed-record skip/fail evidence (E), through the real M3
//! fault-tolerance surface (`FaultRuntime`/`FaultPolicy`), for both the
//! delimited/CSV and fixed-width readers.
//!
//! `TestStep` does not yet expose a fault-runtime builder, so these drive a
//! real production [`ChunkStep`] directly -- the same pattern
//! `crates/oxide-batch/tests/chunk_fault_runtime.rs` uses -- while still
//! reusing `oxide-batch-test`'s deterministic scaffolding
//! ([`StandaloneTransactions`], [`NoCompletion`]) rather than a hand-rolled
//! transaction manager.

#![allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::expect_used,
    clippy::similar_names
)]

use std::io::Cursor;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use oxide_batch::item_components::basic::{IdentityProcessor, NoopWriter};
use oxide_batch::item_components::{
    DelimitedDialect, DelimitedRecord, FixedWidthField, FixedWidthLayout, FixedWidthRecord,
    delimited_reader, fixed_width_reader,
};
use oxide_batch::{
    BackoffOutcome, BackoffPolicy, BackoffSleeper, BoxFuture, ChunkExecutionOutcome, ChunkFailure,
    ChunkSize, ChunkStep, ClassifierRevision, ExecutionAttempt, ExecutionCorrelation,
    FailureCategory, FaultAction, FaultClassifier, FaultDescriptor, FaultPhase, FaultPolicy,
    FaultRule, FaultRuntime, InMemoryFaultState, ItemListenerContext, ItemListenerSet,
    JobExecutionId, JobInstanceId, JobName, ListenerError, ReadListener, RetryLimit,
    RetryStateLimit, RollbackDisposition, SkipLimit, StepExecutionId, StepName, StopSource,
    StopToken,
};
use oxide_batch_test::{NoCompletion, StandaloneTransactions};

/// Captures the [`FailureCategory`] the real M3 fault-tolerance runtime
/// observed at the item-listener boundary -- the actual framework-visible
/// classification, not merely the coarse [`ChunkFailure::Reader`] shape,
/// which other read failures (I/O, a different classifier entirely) would
/// also produce. Mirrors #146's own
/// `item_components_equivalence.rs::CapturingReadListener`.
struct CapturingReadListener(Arc<Mutex<Option<FailureCategory>>>);

impl<I: Send + Sync> ReadListener<I> for CapturingReadListener {
    fn on_read_error<'a>(
        &'a self,
        fault: FaultDescriptor,
        _context: ItemListenerContext<'a>,
    ) -> BoxFuture<'a, Result<(), ListenerError>> {
        let captured = Arc::clone(&self.0);
        Box::pin(async move {
            *captured.lock().unwrap_or_else(PoisonError::into_inner) = Some(fault.category());
            Ok(())
        })
    }
}

fn correlation() -> ExecutionCorrelation {
    let attempt =
        |value: u64| ExecutionAttempt::new(NonZeroU64::new(value).expect("attempt is nonzero"));
    ExecutionCorrelation::new(
        JobName::new("flat_file_fault").expect("static job name is valid"),
        JobInstanceId::new(1).expect("static instance id is nonzero"),
        JobExecutionId::new(1).expect("static execution id is nonzero"),
        attempt(1),
        StepName::new("flat_file_fault_step").expect("static step name is valid"),
        StepExecutionId::new(1).expect("static execution id is nonzero"),
        attempt(1),
    )
}

struct ImmediateSleeper;

impl BackoffSleeper for ImmediateSleeper {
    fn sleep<'a>(&'a self, _delay: Duration, stop: &'a StopToken) -> BoxFuture<'a, BackoffOutcome> {
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

/// A `FaultRuntime` that skips (without retry) every `Read`/`UserComponent`
/// fault -- exactly the classification a malformed record's `ReaderError`
/// carries.
fn skip_read_user_component_runtime() -> FaultRuntime {
    let policy = FaultPolicy::new(
        FaultClassifier::new(
            ClassifierRevision::new("flat-file-skip-v1").unwrap(),
            [FaultRule::new(
                FaultPhase::Read,
                FailureCategory::UserComponent,
                FaultAction::skip(RollbackDisposition::Rollback),
            )
            .unwrap()],
        )
        .unwrap(),
        RetryLimit::NONE,
        RetryStateLimit::new(4).unwrap(),
        SkipLimit::new(10),
        BackoffPolicy::none(),
    )
    .unwrap();
    let state = Arc::new(InMemoryFaultState::new(policy.retry_state_limit()));
    FaultRuntime::new(
        policy,
        Arc::new(ImmediateSleeper),
        state,
        oxide_batch::ChunkDeliveryMode::AtLeastOnce,
    )
    .unwrap()
}

// --------------------------------------------------------------------- CSV --

fn csv_fixture() -> Vec<u8> {
    // Two well-formed rows, one ragged (malformed) row, two more well-formed
    // rows.
    b"1,a\n2,b\nBAD\n3,c\n4,d\n".to_vec()
}

#[tokio::test]
async fn csv_fail_policy_fails_the_step_with_the_expected_classification() {
    let (reader, _s, _c) = delimited_reader::<DelimitedRecord, _>(
        Cursor::new(csv_fixture()),
        DelimitedDialect::csv(),
        oxide_batch::ComponentStreamIdentity::new("oxide-batch-test.fault-csv").unwrap(),
    );
    let category = Arc::new(Mutex::new(None));
    let listeners = ItemListenerSet::new()
        .with_read_listener(Arc::new(CapturingReadListener(Arc::clone(&category))))
        .unwrap();
    let mut step: ChunkStep<DelimitedRecord, DelimitedRecord, _, _, _> = ChunkStep::new(
        StepName::new("csv_fail").unwrap(),
        ChunkSize::new(1).unwrap(),
        reader,
        IdentityProcessor,
        NoopWriter,
        Arc::new(StandaloneTransactions),
        Arc::new(NoCompletion),
    )
    .with_item_listeners(listeners);
    let (_source, stop) = StopSource::new();
    let report = step.execute(&correlation(), &stop).await;

    assert_eq!(
        report.outcome(),
        ChunkExecutionOutcome::Failed(ChunkFailure::Reader)
    );
    assert_eq!(
        report.committed_counts().read().get(),
        2,
        "exactly the two well-formed rows before the malformed one committed"
    );
    assert_eq!(
        *category.lock().unwrap_or_else(PoisonError::into_inner),
        Some(FailureCategory::UserComponent),
        "the real M3 runtime must have classified the malformed record's failure as \
         UserComponent, not merely produced a Reader-shaped chunk failure"
    );
}

#[tokio::test]
async fn csv_skip_policy_skips_the_malformed_record_and_processes_later_valid_records() {
    let (reader, _s, _c) = delimited_reader::<DelimitedRecord, _>(
        Cursor::new(csv_fixture()),
        DelimitedDialect::csv(),
        oxide_batch::ComponentStreamIdentity::new("oxide-batch-test.fault-csv").unwrap(),
    );
    let mut step: ChunkStep<DelimitedRecord, DelimitedRecord, _, _, _> = ChunkStep::new(
        StepName::new("csv_skip").unwrap(),
        ChunkSize::new(1).unwrap(),
        reader,
        IdentityProcessor,
        NoopWriter,
        Arc::new(StandaloneTransactions),
        Arc::new(NoCompletion),
    )
    .with_fault_runtime(skip_read_user_component_runtime());
    let (_source, stop) = StopSource::new();
    let report = step.execute(&correlation(), &stop).await;

    assert_eq!(report.outcome(), ChunkExecutionOutcome::Completed);
    assert_eq!(
        report.committed_counts().read().get(),
        4,
        "all four well-formed rows were committed, including the two after the skip"
    );
    assert_eq!(report.skip_counts().read(), 1);
}

// --------------------------------------------------------------- fixed width --

fn fixed_width_layout() -> FixedWidthLayout {
    FixedWidthLayout::new(vec![FixedWidthField::new(1), FixedWidthField::new(1)])
}

fn fixed_width_fixture() -> Vec<u8> {
    b"1a\n2b\nBAD\n3c\n4d\n".to_vec()
}

#[tokio::test]
async fn fixed_width_fail_policy_fails_the_step_with_the_expected_classification() {
    let (reader, _s, _c) = fixed_width_reader::<FixedWidthRecord, _>(
        Cursor::new(fixed_width_fixture()),
        fixed_width_layout(),
        oxide_batch::ComponentStreamIdentity::new("oxide-batch-test.fault-fixed-width").unwrap(),
    );
    let category = Arc::new(Mutex::new(None));
    let listeners = ItemListenerSet::new()
        .with_read_listener(Arc::new(CapturingReadListener(Arc::clone(&category))))
        .unwrap();
    let mut step: ChunkStep<FixedWidthRecord, FixedWidthRecord, _, _, _> = ChunkStep::new(
        StepName::new("fw_fail").unwrap(),
        ChunkSize::new(1).unwrap(),
        reader,
        IdentityProcessor,
        NoopWriter,
        Arc::new(StandaloneTransactions),
        Arc::new(NoCompletion),
    )
    .with_item_listeners(listeners);
    let (_source, stop) = StopSource::new();
    let report = step.execute(&correlation(), &stop).await;

    assert_eq!(
        report.outcome(),
        ChunkExecutionOutcome::Failed(ChunkFailure::Reader)
    );
    assert_eq!(report.committed_counts().read().get(), 2);
    assert_eq!(
        *category.lock().unwrap_or_else(PoisonError::into_inner),
        Some(FailureCategory::UserComponent),
        "the real M3 runtime must have classified the malformed record's failure as \
         UserComponent, not merely produced a Reader-shaped chunk failure"
    );
}

#[tokio::test]
async fn fixed_width_skip_policy_skips_the_malformed_record_and_processes_later_valid_records() {
    let (reader, _s, _c) = fixed_width_reader::<FixedWidthRecord, _>(
        Cursor::new(fixed_width_fixture()),
        fixed_width_layout(),
        oxide_batch::ComponentStreamIdentity::new("oxide-batch-test.fault-fixed-width").unwrap(),
    );
    let mut step: ChunkStep<FixedWidthRecord, FixedWidthRecord, _, _, _> = ChunkStep::new(
        StepName::new("fw_skip").unwrap(),
        ChunkSize::new(1).unwrap(),
        reader,
        IdentityProcessor,
        NoopWriter,
        Arc::new(StandaloneTransactions),
        Arc::new(NoCompletion),
    )
    .with_fault_runtime(skip_read_user_component_runtime());
    let (_source, stop) = StopSource::new();
    let report = step.execute(&correlation(), &stop).await;

    assert_eq!(report.outcome(), ChunkExecutionOutcome::Completed);
    assert_eq!(report.committed_counts().read().get(), 4);
    assert_eq!(report.skip_counts().read(), 1);
}
