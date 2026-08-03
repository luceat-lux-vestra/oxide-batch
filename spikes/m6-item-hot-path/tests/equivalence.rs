//! Native and boxed dispatch must be observationally identical.
//!
//! Each test runs one scenario twice and compares the full trace, the
//! counters, the terminal outcome, and the writer's durable fold. A single
//! differing event fails the test, so the assertion covers ordering as well as
//! totals.

use oxide_batch_m6_spikes::driver::RunOutcome;
use oxide_batch_m6_spikes::scenario::{Scenario, execute_boxed, execute_typed};
use oxide_batch_m6_spikes::workload::Fault;

async fn assert_equivalent(scenario: Scenario, expected: RunOutcome) {
    let typed = execute_typed(scenario).await;
    let boxed = execute_boxed(scenario).await;

    assert_eq!(
        typed.report.events, boxed.report.events,
        "trace diverged for {scenario:?}"
    );
    assert_eq!(typed, boxed, "observed state diverged for {scenario:?}");
    assert_eq!(
        typed.report.outcome, expected,
        "unexpected outcome for {scenario:?}"
    );
}

#[tokio::test]
async fn whole_chunks_are_equivalent() {
    assert_equivalent(Scenario::new(64, 8), RunOutcome::Completed).await;
}

#[tokio::test]
async fn a_partial_final_chunk_is_equivalent() {
    assert_equivalent(Scenario::new(65, 8), RunOutcome::Completed).await;
}

#[tokio::test]
async fn empty_input_is_equivalent() {
    assert_equivalent(Scenario::new(0, 8), RunOutcome::Completed).await;
}

#[tokio::test]
async fn a_chunk_larger_than_the_input_is_equivalent() {
    assert_equivalent(Scenario::new(3, 4096), RunOutcome::Completed).await;
}

#[tokio::test]
async fn filtering_is_equivalent() {
    assert_equivalent(
        Scenario::new(64, 8).filtering_every(3),
        RunOutcome::Completed,
    )
    .await;
}

#[tokio::test]
async fn stop_before_the_first_read_is_equivalent() {
    assert_equivalent(
        Scenario::new(64, 8).stopped_before_start(),
        RunOutcome::Stopped,
    )
    .await;
}

#[tokio::test]
async fn a_reader_stop_outcome_is_equivalent() {
    assert_equivalent(
        Scenario::new(64, 8).with_reader_fault(Fault::Stop(21)),
        RunOutcome::Stopped,
    )
    .await;
}

#[tokio::test]
async fn a_processor_stop_outcome_is_equivalent() {
    assert_equivalent(
        Scenario::new(64, 8).with_processor_fault(Fault::Stop(21)),
        RunOutcome::Stopped,
    )
    .await;
}

#[tokio::test]
async fn a_writer_stop_outcome_is_equivalent() {
    assert_equivalent(
        Scenario::new(64, 8).with_writer_fault(Fault::Stop(3)),
        RunOutcome::Stopped,
    )
    .await;
}

#[tokio::test]
async fn a_reader_failure_is_equivalent() {
    assert_equivalent(
        Scenario::new(64, 8).with_reader_fault(Fault::Fail(21)),
        RunOutcome::ReaderFailed,
    )
    .await;
}

#[tokio::test]
async fn a_processor_failure_is_equivalent() {
    assert_equivalent(
        Scenario::new(64, 8).with_processor_fault(Fault::Fail(21)),
        RunOutcome::ProcessorFailed,
    )
    .await;
}

#[tokio::test]
async fn a_writer_failure_is_equivalent() {
    assert_equivalent(
        Scenario::new(64, 8).with_writer_fault(Fault::Fail(3)),
        RunOutcome::WriterFailed,
    )
    .await;
}

#[tokio::test]
async fn a_mid_chunk_failure_leaves_the_same_committed_prefix() {
    // The interesting case for restart identity: the failure lands after two
    // whole chunks committed and three items of the third were buffered.
    let scenario = Scenario::new(64, 8).with_processor_fault(Fault::Fail(19));
    let typed = execute_typed(scenario).await;
    let boxed = execute_boxed(scenario).await;

    assert_eq!(typed, boxed);
    assert_eq!(typed.report.chunks_committed, 2);
    assert_eq!(typed.written, 16);
    assert_eq!(typed.report.items_read, 20);
}
