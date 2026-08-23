//! #148 malformed-input skip/fail evidence (E), through the real M3
//! fault-tolerance surface (`FaultRuntime`/`FaultPolicy`), for both the
//! JSON Lines and JSON-array readers.
//!
//! Mirrors `item_components_flat_file_fault.rs`'s pattern exactly: a real,
//! hand-assembled [`ChunkStep`] with a real `FaultRuntime`/`FaultPolicy`
//! (`TestStep` does not yet expose a fault-runtime builder), reusing the
//! kit's `StandaloneTransactions`/`NoCompletion`.
//!
//! JSON Lines malformed lines are safely skippable (the line boundary is
//! independently knowable regardless of parse success), so both a fail
//! policy and a skip policy are exercised. A JSON-array's malformed
//! structure is never safely skippable (see
//! `oxide_batch::item_components::json_array`'s module documentation) -- a
//! skip policy configured against it still fails the step, which this file
//! proves directly rather than merely asserting the (identical either way)
//! fail-policy outcome once.

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
    JsonArrayFormat, JsonLinesFormat, json_array_reader, jsonl_reader,
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
use serde_json::Value;

/// Captures the [`FailureCategory`] the real M3 fault-tolerance runtime
/// observed at the item-listener boundary. Mirrors #146/#147's own
/// `CapturingReadListener`.
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
        JobName::new("json_fault").expect("static job name is valid"),
        JobInstanceId::new(1).expect("static instance id is nonzero"),
        JobExecutionId::new(1).expect("static execution id is nonzero"),
        attempt(1),
        StepName::new("json_fault_step").expect("static step name is valid"),
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
            ClassifierRevision::new("json-fault-skip-v1").unwrap(),
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

// --------------------------------------------------------------- JSONL --

fn jsonl_fixture() -> Vec<u8> {
    // Two well-formed lines, one malformed line, two more well-formed lines.
    b"1\n2\n{not json}\n3\n4\n".to_vec()
}

#[tokio::test]
async fn jsonl_fail_policy_fails_the_step_with_the_expected_classification() {
    let (reader, _s, _c) = jsonl_reader::<Value, _>(
        Cursor::new(jsonl_fixture()),
        JsonLinesFormat::new(),
        oxide_batch::ComponentStreamIdentity::new("oxide-batch-test.fault-jsonl").unwrap(),
    );
    let category = Arc::new(Mutex::new(None));
    let listeners = ItemListenerSet::new()
        .with_read_listener(Arc::new(CapturingReadListener(Arc::clone(&category))))
        .unwrap();
    let mut step: ChunkStep<Value, Value, _, _, _> = ChunkStep::new(
        StepName::new("jsonl_fail").unwrap(),
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
        "exactly the two well-formed lines before the malformed one committed"
    );
    assert_eq!(
        *category.lock().unwrap_or_else(PoisonError::into_inner),
        Some(FailureCategory::UserComponent),
        "the real M3 runtime must have classified the malformed line's failure as \
         UserComponent, not merely produced a Reader-shaped chunk failure"
    );
}

#[tokio::test]
async fn jsonl_skip_policy_skips_the_malformed_line_and_processes_later_valid_lines() {
    let (reader, _s, _c) = jsonl_reader::<Value, _>(
        Cursor::new(jsonl_fixture()),
        JsonLinesFormat::new(),
        oxide_batch::ComponentStreamIdentity::new("oxide-batch-test.fault-jsonl").unwrap(),
    );
    let mut step: ChunkStep<Value, Value, _, _, _> = ChunkStep::new(
        StepName::new("jsonl_skip").unwrap(),
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
        "all four well-formed lines were committed, including the two after the skip"
    );
    assert_eq!(report.skip_counts().read(), 1);
}

// ------------------------------------------------------------ JSON array --

fn json_array_fixture() -> Vec<u8> {
    b"[1,2,not_json,3,4]".to_vec()
}

#[tokio::test]
async fn json_array_fail_policy_fails_the_step_with_the_expected_classification() {
    let (reader, _s, _c) = json_array_reader::<Value, _>(
        Cursor::new(json_array_fixture()),
        JsonArrayFormat::new(),
        oxide_batch::ComponentStreamIdentity::new("oxide-batch-test.fault-json-array").unwrap(),
    );
    let category = Arc::new(Mutex::new(None));
    let listeners = ItemListenerSet::new()
        .with_read_listener(Arc::new(CapturingReadListener(Arc::clone(&category))))
        .unwrap();
    let mut step: ChunkStep<Value, Value, _, _, _> = ChunkStep::new(
        StepName::new("json_array_fail").unwrap(),
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
        "exactly the two well-formed elements before the malformed one committed"
    );
    assert_eq!(
        *category.lock().unwrap_or_else(PoisonError::into_inner),
        Some(FailureCategory::UserComponent),
    );
}

/// Proves the locked distinction directly: a JSON array's malformed
/// structure destroys trustworthy element-boundary knowledge, so even a
/// *skip*-configured policy cannot honor it -- the step still fails, exactly
/// as it would with no fault runtime at all, rather than the reader
/// guessing at the next comma/bracket to keep going.
#[tokio::test]
async fn json_array_skip_policy_still_fails_the_step_because_no_boundary_is_proven() {
    let (reader, _s, _c) = json_array_reader::<Value, _>(
        Cursor::new(json_array_fixture()),
        JsonArrayFormat::new(),
        oxide_batch::ComponentStreamIdentity::new("oxide-batch-test.fault-json-array").unwrap(),
    );
    let mut step: ChunkStep<Value, Value, _, _, _> = ChunkStep::new(
        StepName::new("json_array_skip").unwrap(),
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

    assert_eq!(
        report.outcome(),
        ChunkExecutionOutcome::Failed(ChunkFailure::Reader),
        "a skip policy must not be able to resynchronize past unrecoverable array framing"
    );
    assert_eq!(
        report.committed_counts().read().get(),
        2,
        "no skip was ever recorded: exactly the elements committed before the failure"
    );
    assert_eq!(
        report.skip_counts().read(),
        0,
        "the M3 runtime must never report a skip here -- forward checkpoint proof was absent"
    );
}

#[tokio::test]
async fn sanity_json_array_well_formed_input_completes_without_any_fault() {
    let (reader, _s, _c) = json_array_reader::<Value, _>(
        Cursor::new(b"[1,2,3]".to_vec()),
        JsonArrayFormat::new(),
        oxide_batch::ComponentStreamIdentity::new("oxide-batch-test.fault-json-array-ok").unwrap(),
    );
    let mut step: ChunkStep<Value, Value, _, _, _> = ChunkStep::new(
        StepName::new("json_array_ok").unwrap(),
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
    assert_eq!(report.committed_counts().read().get(), 3);
}
