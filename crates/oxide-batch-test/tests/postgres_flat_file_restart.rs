//! #147 restart evidence (C, D) through real durable committed state:
//! delimited/CSV reader restart across a genuine multiline quoted record
//! boundary (C), delimited writer uncommitted-tail truncation plus
//! fail-closed reconciliation (D), and a combined fixed-width reader/writer
//! restart round trip (both C and D for that family, sharing the same
//! byte-offset restart mechanism).
//!
//! Mirrors `postgres_item_components_restart.rs`'s pattern (#146) exactly:
//! `PostgresFixture` for durable committed state, `TestJob` +
//! `JobLauncher` for the real production restart path, and
//! `oxide_batch_test::inject` for distinguishable stop injection.
//!
//! Requires `OXIDEBATCH_POSTGRES_TEST_URL`; skips (not fails) otherwise, per
//! this repository's `PostgreSQL` evidence convention.

#![cfg(feature = "postgres")]
#![allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::expect_used,
    clippy::similar_names,
    clippy::too_many_lines
)]

use std::error::Error;
use std::sync::{Arc, Mutex};

use oxide_batch::item_components::{
    DelimitedDialect, DelimitedRecord, FixedWidthField, FixedWidthLayout, FixedWidthRecord,
    IterReader, delimited_file_reader, delimited_writer, fixed_width_file_reader,
    fixed_width_writer,
};
use oxide_batch::{
    BatchStatus, Checkpoint, ChunkCommitReceipt, ChunkCounts, ChunkDeliveryMode, ChunkJob,
    ChunkSize, ChunkStep, ChunkTransactionManager, ComponentRevision, ComponentStreamIdentity,
    DefinitionRevision, ExecutionContext, ExecutionCounts, ItemProcessor, ItemWriter, JobName,
    JobParameters, PostgresChunkStateError, PostgresChunkStateProvider, ProcessContext,
    ProcessOutcome, ProcessorError, StateLimits, WriteContext, WriteOutcome, WriterError,
};
use oxide_batch_test::inject::{
    ComponentAction, InjectedReader, InjectedTransactions, InjectionId, InjectionLog,
    PreCommitAction, Trigger,
};
use oxide_batch_test::postgres::PostgresFixture;
use oxide_batch_test::restart::ObservingTransactions;
use oxide_batch_test::{NoCompletion, TestJob, chunk_component_revisions_with_delivery_mode};

fn state_provider() -> Arc<dyn PostgresChunkStateProvider> {
    Arc::new(|committed: ExecutionCounts, chunk: ChunkCounts| {
        let position = committed
            .read()
            .checked_add(chunk.read().get())
            .ok_or_else(PostgresChunkStateError::new)?;
        let checkpoint_bytes = format!(
            r#"{{"format":"oxide-batch.checkpoint","format_version":1,"schema":"oxide-batch-test.flat-file-restart","schema_version":1,"payload":{{"position":{position}}}}}"#
        );
        let checkpoint = Checkpoint::from_json(checkpoint_bytes.as_bytes(), StateLimits::default())
            .map_err(|_| PostgresChunkStateError::new())?;
        let context = ExecutionContext::from_json(
            br#"{"format":"oxide-batch.execution-context","format_version":1,"schema":"oxide-batch-test.flat-file-restart","schema_version":1,"payload":{}}"#,
            StateLimits::default(),
        )
        .map_err(|_| PostgresChunkStateError::new())?;
        Ok(ChunkCommitReceipt::new(checkpoint, context))
    })
}

fn runtime_url() -> Option<String> {
    std::env::var("OXIDEBATCH_POSTGRES_TEST_URL")
        .ok()
        .filter(|value| !value.is_empty())
}

struct Identity;

impl ItemProcessor<DelimitedRecord, DelimitedRecord> for Identity {
    async fn process(
        &self,
        item: &DelimitedRecord,
        _context: ProcessContext<'_>,
    ) -> Result<ProcessOutcome<DelimitedRecord>, ProcessorError> {
        Ok(ProcessOutcome::Item(item.clone()))
    }
}

struct RecordingWriter(Arc<Mutex<Vec<DelimitedRecord>>>);

impl ItemWriter<DelimitedRecord> for RecordingWriter {
    async fn write(
        &self,
        items: &[DelimitedRecord],
        _context: WriteContext<'_>,
    ) -> Result<WriteOutcome, WriterError> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend_from_slice(items);
        Ok(WriteOutcome::Written)
    }
}

fn nonce() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .expect("time moves forward")
        .as_nanos()
}

fn temp_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("oxide-batch-147-pg-{name}-{}.csv", nonce()))
}

// --------------------------------------------------------------------- C --

/// Proves reader restart resumes from exactly the last *committed* record
/// boundary -- never mid a multiline quoted record, never re-reading a
/// committed record, never skipping the uncommitted one, across more than
/// one checkpoint offset -- through the real production restart path and
/// real durable committed state.
#[tokio::test]
async fn delimited_reader_restarts_after_the_last_committed_record_never_mid_multiline()
-> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    PostgresFixture::migrate(url.clone()).await?;
    let fixture = PostgresFixture::connect(url).await?;

    // R2 is a genuine multiline quoted record: after committing R1 and R2,
    // the committed byte position sits 3 physical *lines* into the file
    // (R1 is one line, R2 spans two), so a line-count-based restart
    // implementation would land after only 2 lines -- inside R2's own
    // second line -- rather than after R2's real end. A byte/record
    // position does not have this failure mode.
    let source = "1,a\n\"multi\nline\",b\n3,c\n4,d\n5,e\n";
    let path = temp_path("reader-multiline");
    std::fs::write(&path, source)?;

    let job_name = JobName::new(format!("oxide_batch_147_reader_restart_{}", nonce()))?;
    let namespace = ComponentStreamIdentity::new("oxide-batch-test.delimited-reader-restart")?;
    let revisions =
        chunk_component_revisions_with_delivery_mode(ChunkDeliveryMode::AtomicSameResource)
            .with_stream_revision(
                namespace.clone(),
                ComponentRevision::new("delimited-reader-v1")?,
            );

    // Attempt A: chunk size 2. R1+R2 commit as chunk 1. R3 is genuinely read
    // (advancing the real parser position) and buffered into chunk 2, then a
    // stop is injected on R4's read call, so chunk 2 (R3 + would-be-R4)
    // never commits: R3 is consumed but not committed.
    let (reader_a, stream_a, contract_a) = delimited_file_reader::<DelimitedRecord>(
        &path,
        DelimitedDialect::csv(),
        namespace.clone(),
    )?;
    let log = InjectionLog::new();
    let injected_reader_a = InjectedReader::new(
        reader_a,
        Trigger::after(3),
        ComponentAction::Stop(fixture_stop_source()),
        InjectionId::new(1),
        log.clone(),
    );
    let writer_a: Arc<Mutex<Vec<DelimitedRecord>>> = Arc::new(Mutex::new(Vec::new()));
    let step_a = ChunkStep::new(
        oxide_batch::StepName::new("flat_file_reader_restart")?,
        ChunkSize::new(2)?,
        injected_reader_a,
        Identity,
        RecordingWriter(Arc::clone(&writer_a)),
        Arc::new(fixture.transaction_manager(state_provider())),
        Arc::new(NoCompletion),
    )
    .with_item_stream(namespace.clone(), stream_a, contract_a);
    let chunk_job_a = ChunkJob::new(
        job_name.clone(),
        step_a,
        DefinitionRevision::new("flat-file-reader-restart-v1")?,
        &revisions,
    )?;
    let mut job_a = TestJob::new(
        chunk_job_a,
        fixture.repository().clone(),
        fixture.clock().clone(),
        fixture.ids().clone(),
    );
    let report_a = job_a.launch(&JobParameters::new()).await?;

    assert!(log.fired(InjectionId::new(1)));
    let chunk_report_a = report_a
        .chunk()
        .ok_or("attempt A must have reached the chunk step")?;
    assert_eq!(chunk_report_a.committed_counts().read().get(), 2);
    assert_eq!(
        report_a.launch().job_execution().metadata().status(),
        BatchStatus::Stopped,
    );
    assert_eq!(
        field0(&writer_a),
        vec!["1".to_owned(), "multi\nline".to_owned()],
        "only R1 and R2 committed in attempt A",
    );

    // Attempt B: a fresh reader/stream pair over the same path, launched
    // again through the real production restart path.
    let (reader_b, stream_b, contract_b) = delimited_file_reader::<DelimitedRecord>(
        &path,
        DelimitedDialect::csv(),
        namespace.clone(),
    )?;
    let writer_b: Arc<Mutex<Vec<DelimitedRecord>>> = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::new(ObservingTransactions::new(
        fixture.transaction_manager(state_provider()),
    ));
    let step_b = ChunkStep::new(
        oxide_batch::StepName::new("flat_file_reader_restart")?,
        ChunkSize::new(2)?,
        reader_b,
        Identity,
        RecordingWriter(Arc::clone(&writer_b)),
        Arc::clone(&observed) as Arc<dyn ChunkTransactionManager>,
        Arc::new(NoCompletion),
    )
    .with_item_stream(namespace.clone(), stream_b, contract_b);
    let chunk_job_b = ChunkJob::new(
        job_name,
        step_b,
        DefinitionRevision::new("flat-file-reader-restart-v1")?,
        &revisions,
    )?;
    let mut job_b = TestJob::new(
        chunk_job_b,
        fixture.repository().clone(),
        fixture.clock().clone(),
        fixture.ids().clone(),
    );
    let report_b = job_b.launch(&JobParameters::new()).await?;

    assert_eq!(
        report_b.launch().job_execution().metadata().status(),
        BatchStatus::Completed,
    );
    let chunk_report_b = report_b
        .chunk()
        .ok_or("attempt B must have reached the chunk step")?;
    assert_eq!(
        chunk_report_b.committed_counts().read().get(),
        3,
        "attempt B committed exactly the uncommitted remainder R3, R4, R5",
    );
    assert_eq!(
        field0(&writer_b),
        vec!["3".to_owned(), "4".to_owned(), "5".to_owned()],
        "attempt B resumed at R3 -- not re-reading R1/R2, not skipping R3, not \
         landing mid-R2's multiline content",
    );

    // The complete sequence across both attempts, concatenated, is exactly
    // the five source records once each: this single assertion would fail
    // for either an omission (a record missing) or a duplication (R1/R2
    // reappearing in attempt B's output).
    let mut combined = field0(&writer_a);
    combined.extend(field0(&writer_b));
    assert_eq!(
        combined,
        vec!["1", "multi\nline", "3", "4", "5"]
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>(),
    );

    let observed_progress = observed.observed_progress();
    assert_eq!(observed_progress.len(), 1);
    assert_eq!(observed_progress[0].read_ordinal(), 2);

    let _ = std::fs::remove_file(&path);
    Ok(())
}

fn field0(writer: &Arc<Mutex<Vec<DelimitedRecord>>>) -> Vec<String> {
    writer
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .map(|record| record.get(0).unwrap_or_default().to_owned())
        .collect()
}

fn fixture_stop_source() -> oxide_batch::StopSource {
    let (source, _token) = oxide_batch::StopSource::new();
    source
}

// --------------------------------------------------------------------- D --

fn csv_record(fields: &[&str]) -> DelimitedRecord {
    DelimitedRecord::new(fields.iter().map(|field| (*field).to_owned()).collect())
}

/// Proves the writer truncates a physically-present but never-committed
/// tail on restart, and that the resumed record is written exactly once --
/// detecting a writer that merely opens the file in append mode and
/// continues (which would duplicate the uncommitted record instead of
/// truncating and rewriting it).
#[tokio::test]
async fn delimited_writer_truncates_uncommitted_tail_and_resumes_exactly_once()
-> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    PostgresFixture::migrate(url.clone()).await?;
    let fixture = PostgresFixture::connect(url).await?;

    let path = temp_path("writer-truncate");
    let job_name = JobName::new(format!("oxide_batch_147_writer_restart_{}", nonce()))?;
    let namespace = ComponentStreamIdentity::new("oxide-batch-test.delimited-writer-restart")?;
    let revisions =
        chunk_component_revisions_with_delivery_mode(ChunkDeliveryMode::AtomicSameResource)
            .with_stream_revision(
                namespace.clone(),
                ComponentRevision::new("delimited-writer-v1")?,
            );

    // Attempt A: chunk size 1. Chunk 1 (record "1,a") commits normally.
    // Chunk 2's writer call physically appends record "2,b" to the file
    // *before* its commit is injected to fail, leaving those bytes on disk
    // without any corresponding committed writer-state envelope.
    let (writer_a, stream_a, contract_a) =
        delimited_writer(&path, DelimitedDialect::csv(), namespace.clone())?;
    let log = InjectionLog::new();
    let injected_transactions_a = InjectedTransactions::new(
        fixture.transaction_manager(state_provider()),
        2,
        PreCommitAction::Fail,
        InjectionId::new(1),
        log.clone(),
    );
    let step_a = ChunkStep::new(
        oxide_batch::StepName::new("flat_file_writer_restart")?,
        ChunkSize::new(1)?,
        IterReader::new(vec![
            csv_record(&["1", "a"]),
            csv_record(&["2", "b"]),
            csv_record(&["3", "c"]),
        ]),
        Identity,
        writer_a,
        Arc::new(injected_transactions_a),
        Arc::new(NoCompletion),
    )
    .with_item_stream(namespace.clone(), stream_a, contract_a);
    let chunk_job_a = ChunkJob::new(
        job_name.clone(),
        step_a,
        DefinitionRevision::new("flat-file-writer-restart-v1")?,
        &revisions,
    )?;
    let mut job_a = TestJob::new(
        chunk_job_a,
        fixture.repository().clone(),
        fixture.clock().clone(),
        fixture.ids().clone(),
    );
    let report_a = job_a.launch(&JobParameters::new()).await?;

    assert!(log.fired(InjectionId::new(1)));
    let chunk_report_a = report_a
        .chunk()
        .ok_or("attempt A must have reached the chunk step")?;
    assert_eq!(
        chunk_report_a.committed_counts().written().get(),
        1,
        "only chunk 1 (record 1) committed durably in attempt A",
    );

    let bytes_after_a = std::fs::read(&path)?;
    assert_eq!(
        bytes_after_a,
        b"1,a\n2,b\n".to_vec(),
        "record 2's bytes are physically present on disk despite never committing"
    );

    // Attempt B: a fresh writer/stream pair over the same path, resuming
    // with the two records attempt A never committed. No injection this
    // time, so every chunk commits.
    let (writer_b, stream_b, contract_b) =
        delimited_writer(&path, DelimitedDialect::csv(), namespace.clone())?;
    let step_b = ChunkStep::new(
        oxide_batch::StepName::new("flat_file_writer_restart")?,
        ChunkSize::new(1)?,
        IterReader::new(vec![csv_record(&["2", "b"]), csv_record(&["3", "c"])]),
        Identity,
        writer_b,
        Arc::new(fixture.transaction_manager(state_provider())),
        Arc::new(NoCompletion),
    )
    .with_item_stream(namespace.clone(), stream_b, contract_b);
    let chunk_job_b = ChunkJob::new(
        job_name,
        step_b,
        DefinitionRevision::new("flat-file-writer-restart-v1")?,
        &revisions,
    )?;
    let mut job_b = TestJob::new(
        chunk_job_b,
        fixture.repository().clone(),
        fixture.clock().clone(),
        fixture.ids().clone(),
    );
    let report_b = job_b.launch(&JobParameters::new()).await?;

    assert_eq!(
        report_b.launch().job_execution().metadata().status(),
        BatchStatus::Completed,
    );
    let chunk_report_b = report_b
        .chunk()
        .ok_or("attempt B must have reached the chunk step")?;
    assert_eq!(chunk_report_b.committed_counts().written().get(), 2);

    let final_bytes = std::fs::read(&path)?;
    assert_eq!(
        final_bytes,
        b"1,a\n2,b\n3,c\n".to_vec(),
        "the committed prefix is preserved, the uncommitted tail is not authoritative, and \
         record 2 appears exactly once -- not duplicated by an append-mode writer",
    );

    let _ = std::fs::remove_file(&path);
    Ok(())
}

/// Proves the writer fails closed rather than fabricating progress when the
/// output file is shorter than the last committed byte length.
#[tokio::test]
async fn delimited_writer_fails_closed_when_the_file_is_shorter_than_committed()
-> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    PostgresFixture::migrate(url.clone()).await?;
    let fixture = PostgresFixture::connect(url).await?;

    let path = temp_path("writer-fail-closed");
    let job_name = JobName::new(format!("oxide_batch_147_writer_fail_closed_{}", nonce()))?;
    let namespace = ComponentStreamIdentity::new("oxide-batch-test.delimited-writer-fail-closed")?;
    let revisions =
        chunk_component_revisions_with_delivery_mode(ChunkDeliveryMode::AtomicSameResource)
            .with_stream_revision(
                namespace.clone(),
                ComponentRevision::new("delimited-writer-v1")?,
            );

    // Attempt A commits only record 1, then stops (leaving the job instance
    // resumable, not completed) so a genuine attempt B can be launched
    // against it.
    let (writer_a, stream_a, contract_a) =
        delimited_writer(&path, DelimitedDialect::csv(), namespace.clone())?;
    let log = InjectionLog::new();
    let injected_reader_a = InjectedReader::new(
        IterReader::new(vec![
            csv_record(&["1", "a"]),
            csv_record(&["2", "b"]),
            csv_record(&["3", "c"]),
        ]),
        Trigger::after(1),
        ComponentAction::Stop(fixture_stop_source()),
        InjectionId::new(1),
        log.clone(),
    );
    let step_a = ChunkStep::new(
        oxide_batch::StepName::new("flat_file_writer_fail_closed")?,
        ChunkSize::new(1)?,
        injected_reader_a,
        Identity,
        writer_a,
        Arc::new(fixture.transaction_manager(state_provider())),
        Arc::new(NoCompletion),
    )
    .with_item_stream(namespace.clone(), stream_a, contract_a);
    let chunk_job_a = ChunkJob::new(
        job_name.clone(),
        step_a,
        DefinitionRevision::new("flat-file-writer-fail-closed-v1")?,
        &revisions,
    )?;
    let mut job_a = TestJob::new(
        chunk_job_a,
        fixture.repository().clone(),
        fixture.clock().clone(),
        fixture.ids().clone(),
    );
    let report_a = job_a.launch(&JobParameters::new()).await?;
    assert!(log.fired(InjectionId::new(1)));
    assert_eq!(
        report_a.launch().job_execution().metadata().status(),
        BatchStatus::Stopped,
    );
    assert_eq!(std::fs::read(&path)?, b"1,a\n".to_vec());

    // Externally corrupt the durable output: the committed byte length is
    // now inconsistent with reality (the file is shorter than what was
    // committed), simulating e.g. accidental truncation of the output
    // resource between attempts.
    std::fs::write(&path, b"")?;

    let (writer_b, stream_b, contract_b) =
        delimited_writer(&path, DelimitedDialect::csv(), namespace.clone())?;
    let step_b = ChunkStep::new(
        oxide_batch::StepName::new("flat_file_writer_fail_closed")?,
        ChunkSize::new(1)?,
        IterReader::new(vec![csv_record(&["3", "c"])]),
        Identity,
        writer_b,
        Arc::new(fixture.transaction_manager(state_provider())),
        Arc::new(NoCompletion),
    )
    .with_item_stream(namespace.clone(), stream_b, contract_b);
    let chunk_job_b = ChunkJob::new(
        job_name,
        step_b,
        DefinitionRevision::new("flat-file-writer-fail-closed-v1")?,
        &revisions,
    )?;
    let mut job_b = TestJob::new(
        chunk_job_b,
        fixture.repository().clone(),
        fixture.clock().clone(),
        fixture.ids().clone(),
    );
    let report_b = job_b.launch(&JobParameters::new()).await?;

    assert_eq!(
        report_b.launch().job_execution().metadata().status(),
        BatchStatus::Failed,
        "an output shorter than its committed checkpoint must fail closed, never fabricate \
         progress",
    );
    let unchanged = std::fs::read(&path)?;
    assert_eq!(
        unchanged,
        Vec::<u8>::new(),
        "a fail-closed stream open must not have written anything"
    );

    let _ = std::fs::remove_file(&path);
    Ok(())
}

// ------------------------------------------------------ fixed width C+D --

struct FixedIdentity;

impl ItemProcessor<FixedWidthRecord, FixedWidthRecord> for FixedIdentity {
    async fn process(
        &self,
        item: &FixedWidthRecord,
        _context: ProcessContext<'_>,
    ) -> Result<ProcessOutcome<FixedWidthRecord>, ProcessorError> {
        Ok(ProcessOutcome::Item(item.clone()))
    }
}

fn fw_layout() -> FixedWidthLayout {
    FixedWidthLayout::new(vec![FixedWidthField::new(1), FixedWidthField::new(1)])
}

/// A single scenario exercising both the fixed-width reader's and writer's
/// restart through one paired input/output round trip: attempt A commits
/// two records, consumes a third without committing it, then stops; attempt
/// B resumes both the reader and the writer from their last committed
/// positions and produces the complete, exactly-once output.
#[tokio::test]
async fn fixed_width_reader_and_writer_restart_from_the_last_committed_position()
-> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    PostgresFixture::migrate(url.clone()).await?;
    let fixture = PostgresFixture::connect(url).await?;

    let input_path = temp_path("fw-input");
    std::fs::write(&input_path, "1a\n2b\n3c\n4d\n5e\n")?;
    let output_path = temp_path("fw-output");

    let job_name = JobName::new(format!("oxide_batch_147_fw_restart_{}", nonce()))?;
    let reader_namespace =
        ComponentStreamIdentity::new("oxide-batch-test.fixed-width-reader-restart")?;
    let writer_namespace =
        ComponentStreamIdentity::new("oxide-batch-test.fixed-width-writer-restart")?;
    let revisions =
        chunk_component_revisions_with_delivery_mode(ChunkDeliveryMode::AtomicSameResource)
            .with_stream_revision(
                reader_namespace.clone(),
                ComponentRevision::new("fw-reader-v1")?,
            )
            .with_stream_revision(
                writer_namespace.clone(),
                ComponentRevision::new("fw-writer-v1")?,
            );

    let (reader_a, reader_stream_a, reader_contract_a) = fixed_width_file_reader::<FixedWidthRecord>(
        &input_path,
        fw_layout(),
        reader_namespace.clone(),
    )?;
    let (writer_a, writer_stream_a, writer_contract_a) =
        fixed_width_writer(&output_path, fw_layout(), writer_namespace.clone())?;
    let log = InjectionLog::new();
    let injected_reader_a = InjectedReader::new(
        reader_a,
        Trigger::after(3),
        ComponentAction::Stop(fixture_stop_source()),
        InjectionId::new(1),
        log.clone(),
    );
    let step_a = ChunkStep::new(
        oxide_batch::StepName::new("fw_restart")?,
        ChunkSize::new(2)?,
        injected_reader_a,
        FixedIdentity,
        writer_a,
        Arc::new(fixture.transaction_manager(state_provider())),
        Arc::new(NoCompletion),
    )
    .with_item_stream(reader_namespace.clone(), reader_stream_a, reader_contract_a)
    .with_item_stream(writer_namespace.clone(), writer_stream_a, writer_contract_a);
    let chunk_job_a = ChunkJob::new(
        job_name.clone(),
        step_a,
        DefinitionRevision::new("fw-restart-v1")?,
        &revisions,
    )?;
    let mut job_a = TestJob::new(
        chunk_job_a,
        fixture.repository().clone(),
        fixture.clock().clone(),
        fixture.ids().clone(),
    );
    let report_a = job_a.launch(&JobParameters::new()).await?;
    assert!(log.fired(InjectionId::new(1)));
    assert_eq!(
        report_a.launch().job_execution().metadata().status(),
        BatchStatus::Stopped,
    );
    assert_eq!(std::fs::read(&output_path)?, b"1a\n2b\n".to_vec());

    let (reader_b, reader_stream_b, reader_contract_b) = fixed_width_file_reader::<FixedWidthRecord>(
        &input_path,
        fw_layout(),
        reader_namespace.clone(),
    )?;
    let (writer_b, writer_stream_b, writer_contract_b) =
        fixed_width_writer(&output_path, fw_layout(), writer_namespace.clone())?;
    let step_b = ChunkStep::new(
        oxide_batch::StepName::new("fw_restart")?,
        ChunkSize::new(2)?,
        reader_b,
        FixedIdentity,
        writer_b,
        Arc::new(fixture.transaction_manager(state_provider())),
        Arc::new(NoCompletion),
    )
    .with_item_stream(reader_namespace.clone(), reader_stream_b, reader_contract_b)
    .with_item_stream(writer_namespace.clone(), writer_stream_b, writer_contract_b);
    let chunk_job_b = ChunkJob::new(
        job_name,
        step_b,
        DefinitionRevision::new("fw-restart-v1")?,
        &revisions,
    )?;
    let mut job_b = TestJob::new(
        chunk_job_b,
        fixture.repository().clone(),
        fixture.clock().clone(),
        fixture.ids().clone(),
    );
    let report_b = job_b.launch(&JobParameters::new()).await?;
    assert_eq!(
        report_b.launch().job_execution().metadata().status(),
        BatchStatus::Completed,
    );
    let chunk_report_b = report_b
        .chunk()
        .ok_or("attempt B must have reached the chunk step")?;
    assert_eq!(
        chunk_report_b.committed_counts().read().get(),
        3,
        "attempt B resumed at record 3, reading exactly the uncommitted remainder",
    );

    assert_eq!(
        std::fs::read(&output_path)?,
        b"1a\n2b\n3c\n4d\n5e\n".to_vec(),
        "the complete output is produced exactly once across both attempts",
    );

    let _ = std::fs::remove_file(&input_path);
    let _ = std::fs::remove_file(&output_path);
    Ok(())
}
