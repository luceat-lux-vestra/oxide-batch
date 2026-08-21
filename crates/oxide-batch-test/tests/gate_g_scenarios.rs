//! Named Gate G evidence scenarios that need no `PostgreSQL` fixture:
//!
//! - `full_job_harness_launches_with_deterministic_clock_and_id`
//! - `single_step_and_scoped_component_harness_construct_fixture_context`
//! - `failure_panic_and_stop_injection_are_available_to_application_tests`
//!
//! `repository_fixture_cleans_up_isolated_metadata` and
//! `restart_harness_resumes_from_the_last_committed_checkpoint` live in
//! `tests/postgres_fixture.rs` and `tests/restart_harness.rs`, gated behind
//! the `postgres` feature and a live database. `package_dry_run_succeeds_for_oxide_batch_test`
//! is process-level evidence recorded by `cargo xtask package`, matching
//! this repository's convention that shelling out to `cargo` is `xtask`'s
//! job, not a `#[test]`'s.

#![allow(clippy::unwrap_used)]

use std::collections::VecDeque;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use oxide_batch::{
    ChunkComponentRevisions, ChunkExecutionOutcome, ChunkFailure, ChunkJob, ChunkSize, ChunkStep,
    DefinitionRevision, FailureCategory, ItemProcessor, ItemReader, ItemWriter, JobName,
    JobParameters, ProcessContext, ProcessOutcome, ProcessorError, ReadContext, ReadOutcome,
    ReaderError, StepName, StopSource, WriteContext, WriteOutcome, WriterError,
};
use oxide_batch_test::inject::{
    ComponentAction, InjectedReader, InjectionId, InjectionLog, Trigger,
};
use oxide_batch_test::{ComponentFixture, TestJob, TestStep, default_chunk_component_revisions};

struct VecReader(VecDeque<i64>);

impl ItemReader<i64> for VecReader {
    async fn read(&mut self, _context: ReadContext<'_>) -> Result<ReadOutcome<i64>, ReaderError> {
        Ok(self
            .0
            .pop_front()
            .map_or(ReadOutcome::EndOfInput, ReadOutcome::Item))
    }
}

struct Identity;

impl ItemProcessor<i64, i64> for Identity {
    async fn process(
        &self,
        item: &i64,
        _context: ProcessContext<'_>,
    ) -> Result<ProcessOutcome<i64>, ProcessorError> {
        Ok(ProcessOutcome::Item(*item))
    }
}

struct NoopWriter;

impl ItemWriter<i64> for NoopWriter {
    async fn write(
        &self,
        _items: &[i64],
        _context: WriteContext<'_>,
    ) -> Result<WriteOutcome, WriterError> {
        Ok(WriteOutcome::Written)
    }
}

fn revisions() -> ChunkComponentRevisions {
    default_chunk_component_revisions()
}

#[tokio::test]
async fn full_job_harness_launches_with_deterministic_clock_and_id() -> Result<(), Box<dyn Error>> {
    let step = ChunkStep::new(
        StepName::new("load")?,
        ChunkSize::new(2)?,
        VecReader((0..4).collect()),
        Identity,
        NoopWriter,
        Arc::new(oxide_batch_test::StandaloneTransactions),
        Arc::new(oxide_batch_test::NoCompletion),
    );
    let chunk_job = ChunkJob::new(
        JobName::new("gate_g_full_job_harness")?,
        step,
        DefinitionRevision::new("gate-g-v1")?,
        &revisions(),
    )?;
    let mut job = TestJob::embedded(chunk_job);

    // Deterministic clock: it never moves on its own.
    let before = job.clock().now();
    job.clock().advance(Duration::from_secs(5))?;
    assert_eq!(
        job.clock().now(),
        before + Duration::from_secs(5),
        "the harness clock only advances when the test tells it to"
    );

    let report = job.launch(&JobParameters::new()).await?;

    // Deterministic IDs: a fresh embedded fixture's shared ID sequence
    // reproducibly issues the instance ID and then the execution ID, in
    // that order, starting at 1.
    assert_eq!(report.launch().instance().id().get(), 1);
    assert_eq!(report.launch().job_execution().id().get(), 2);
    assert_eq!(
        report
            .chunk()
            .ok_or("chunk work must have started")?
            .outcome(),
        ChunkExecutionOutcome::Completed
    );
    Ok(())
}

#[tokio::test]
async fn single_step_and_scoped_component_harness_construct_fixture_context()
-> Result<(), Box<dyn Error>> {
    // TEST-SCOPE-001: a typed fixture hands out real production call
    // contexts without constructing any private/internal type.
    let fixture = ComponentFixture::new();
    let mut reader = VecReader(VecDeque::from([1]));
    assert_eq!(
        reader.read(fixture.read_context()).await?,
        ReadOutcome::Item(1)
    );
    let processor = Identity;
    assert_eq!(
        processor.process(&7, fixture.process_context()).await?,
        ProcessOutcome::Item(7)
    );
    let writer = NoopWriter;
    assert_eq!(
        writer.write(&[7], fixture.write_context()).await?,
        WriteOutcome::Written
    );

    // TEST-STEP-001: the single-step harness drives a real ChunkStep to
    // completion without a full job/repository graph.
    let mut step = TestStep::new(
        StepName::new("scope_step")?,
        ChunkSize::new(2)?,
        VecReader((0..4).collect()),
        Identity,
        NoopWriter,
    );
    let report = step.run().await;
    assert_eq!(report.outcome(), ChunkExecutionOutcome::Completed);
    assert_eq!(report.committed_counts().read().get(), 4);

    Ok(())
}

#[tokio::test]
async fn failure_panic_and_stop_injection_are_available_to_application_tests()
-> Result<(), Box<dyn Error>> {
    // Failure injection: distinguishable from a genuine defect via its
    // logged InjectionId, which no real framework failure ever produces.
    let fail_log = InjectionLog::new();
    let fail_id = InjectionId::new(1);
    let mut fail_step = TestStep::new(
        StepName::new("fail_step")?,
        ChunkSize::new(2)?,
        InjectedReader::new(
            VecReader((0..4).collect()),
            Trigger::immediately(),
            ComponentAction::Fail(FailureCategory::UserComponent),
            fail_id,
            fail_log.clone(),
        ),
        Identity,
        NoopWriter,
    );
    let fail_report = fail_step.run().await;
    assert_eq!(
        fail_report.outcome(),
        ChunkExecutionOutcome::Failed(ChunkFailure::Reader)
    );
    assert!(
        fail_log.fired(fail_id),
        "the injected failure must be logged with its own marker"
    );

    // Panic injection: the real production panic-to-typed-failure boundary
    // converts it, so this test never unwinds.
    let panic_log = InjectionLog::new();
    let panic_id = InjectionId::new(2);
    let mut panic_step = TestStep::new(
        StepName::new("panic_step")?,
        ChunkSize::new(2)?,
        InjectedReader::new(
            VecReader((0..4).collect()),
            Trigger::immediately(),
            ComponentAction::Panic,
            panic_id,
            panic_log.clone(),
        ),
        Identity,
        NoopWriter,
    );
    let panic_report = panic_step.run().await;
    assert_eq!(
        panic_report.outcome(),
        ChunkExecutionOutcome::Failed(ChunkFailure::ReaderPanic)
    );
    assert!(panic_log.fired(panic_id));

    // Cooperative-stop injection: real stop semantics, no wall-clock race.
    let stop_log = InjectionLog::new();
    let stop_id = InjectionId::new(3);
    let (stop_source, stop_token) = StopSource::new();
    let mut stop_step = TestStep::new(
        StepName::new("stop_step")?,
        ChunkSize::new(2)?,
        InjectedReader::new(
            VecReader((0..4).collect()),
            Trigger::immediately(),
            ComponentAction::Stop(stop_source),
            stop_id,
            stop_log.clone(),
        ),
        Identity,
        NoopWriter,
    );
    let stop_report = stop_step.run_with_stop(&stop_token).await;
    assert_eq!(stop_report.outcome(), ChunkExecutionOutcome::Stopped);
    assert!(stop_log.fired(stop_id));

    Ok(())
}
