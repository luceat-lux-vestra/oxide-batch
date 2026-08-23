//! #148 restart evidence (C, D) through real durable `PostgreSQL` committed
//! state: JSONL reader/writer restart, and streaming JSON-array
//! reader/writer restart across a genuine multi-line, delimiter-containing
//! element boundary.
//!
//! Mirrors `postgres_flat_file_restart.rs`'s pattern exactly:
//! `PostgresFixture` for durable committed state, `TestJob` + `JobLauncher`
//! for the real production restart path, and `oxide_batch_test::inject` for
//! distinguishable stop/commit-failure injection.
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
    IterReader, JsonArrayFormat, JsonLinesFormat, json_array_file_reader, json_array_writer,
    jsonl_file_reader, jsonl_writer,
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
use serde_json::{Value, json};

fn state_provider() -> Arc<dyn PostgresChunkStateProvider> {
    Arc::new(|committed: ExecutionCounts, chunk: ChunkCounts| {
        let position = committed
            .read()
            .checked_add(chunk.read().get())
            .ok_or_else(PostgresChunkStateError::new)?;
        let checkpoint_bytes = format!(
            r#"{{"format":"oxide-batch.checkpoint","format_version":1,"schema":"oxide-batch-test.json-restart","schema_version":1,"payload":{{"position":{position}}}}}"#
        );
        let checkpoint = Checkpoint::from_json(checkpoint_bytes.as_bytes(), StateLimits::default())
            .map_err(|_| PostgresChunkStateError::new())?;
        let context = ExecutionContext::from_json(
            br#"{"format":"oxide-batch.execution-context","format_version":1,"schema":"oxide-batch-test.json-restart","schema_version":1,"payload":{}}"#,
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

impl ItemProcessor<Value, Value> for Identity {
    async fn process(
        &self,
        item: &Value,
        _context: ProcessContext<'_>,
    ) -> Result<ProcessOutcome<Value>, ProcessorError> {
        Ok(ProcessOutcome::Item(item.clone()))
    }
}

struct RecordingWriter(Arc<Mutex<Vec<Value>>>);

impl ItemWriter<Value> for RecordingWriter {
    async fn write(
        &self,
        items: &[Value],
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

fn temp_path(name: &str, extension: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("oxide-batch-148-pg-{name}-{}.{extension}", nonce()))
}

fn fixture_stop_source() -> oxide_batch::StopSource {
    let (source, _token) = oxide_batch::StopSource::new();
    source
}

fn values(writer: &Arc<Mutex<Vec<Value>>>) -> Vec<Value> {
    writer
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

// ------------------------------------------------------------------ JSONL --

/// Proves JSONL reader restart resumes from exactly the last committed line
/// boundary through the real production restart path and real durable
/// committed state.
#[tokio::test]
async fn jsonl_reader_restarts_after_the_last_committed_line() -> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    PostgresFixture::migrate(url.clone()).await?;
    let fixture = PostgresFixture::connect(url).await?;

    let source = "1\n2\n3\n4\n5\n";
    let path = temp_path("jsonl-reader-restart", "jsonl");
    std::fs::write(&path, source)?;

    let job_name = JobName::new(format!("oxide_batch_148_jsonl_reader_restart_{}", nonce()))?;
    let namespace = ComponentStreamIdentity::new("oxide-batch-test.jsonl-reader-restart")?;
    let revisions =
        chunk_component_revisions_with_delivery_mode(ChunkDeliveryMode::AtomicSameResource)
            .with_stream_revision(
                namespace.clone(),
                ComponentRevision::new("jsonl-reader-v1")?,
            );

    // Attempt A: chunk size 2. Lines 1+2 commit as chunk 1. Line 3 is
    // genuinely read (advancing the real position) and buffered into chunk
    // 2, then a stop is injected on line 4's read call, so chunk 2 (line 3 +
    // would-be-line 4) never commits: line 3 is consumed but not committed.
    let (reader_a, stream_a, contract_a) =
        jsonl_file_reader::<Value>(&path, JsonLinesFormat::new(), namespace.clone())?;
    let log = InjectionLog::new();
    let injected_reader_a = InjectedReader::new(
        reader_a,
        Trigger::after(3),
        ComponentAction::Stop(fixture_stop_source()),
        InjectionId::new(1),
        log.clone(),
    );
    let writer_a: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let step_a = ChunkStep::new(
        oxide_batch::StepName::new("jsonl_reader_restart")?,
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
        DefinitionRevision::new("jsonl-reader-restart-v1")?,
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
    assert_eq!(values(&writer_a), vec![json!(1), json!(2)]);

    // Attempt B: a fresh reader/stream pair over the same path.
    let (reader_b, stream_b, contract_b) =
        jsonl_file_reader::<Value>(&path, JsonLinesFormat::new(), namespace.clone())?;
    let writer_b: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::new(ObservingTransactions::new(
        fixture.transaction_manager(state_provider()),
    ));
    let step_b = ChunkStep::new(
        oxide_batch::StepName::new("jsonl_reader_restart")?,
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
        DefinitionRevision::new("jsonl-reader-restart-v1")?,
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
        "attempt B committed exactly the uncommitted remainder: lines 3, 4, 5",
    );
    assert_eq!(
        values(&writer_b),
        vec![json!(3), json!(4), json!(5)],
        "attempt B resumed at line 3 -- not re-reading lines 1/2, not skipping line 3",
    );

    let mut combined = values(&writer_a);
    combined.extend(values(&writer_b));
    assert_eq!(
        combined,
        vec![json!(1), json!(2), json!(3), json!(4), json!(5)],
        "committed exactly once each across both attempts: no omission, no duplication",
    );

    let observed_progress = observed.observed_progress();
    assert_eq!(observed_progress.len(), 1);
    assert_eq!(observed_progress[0].read_ordinal(), 2);

    let _ = std::fs::remove_file(&path);
    Ok(())
}

/// Proves the JSONL writer truncates a physically-present but
/// never-committed tail on restart, and that the resumed record is written
/// exactly once.
#[tokio::test]
async fn jsonl_writer_truncates_uncommitted_tail_and_resumes_exactly_once()
-> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    PostgresFixture::migrate(url.clone()).await?;
    let fixture = PostgresFixture::connect(url).await?;

    let path = temp_path("jsonl-writer-truncate", "jsonl");
    let job_name = JobName::new(format!("oxide_batch_148_jsonl_writer_restart_{}", nonce()))?;
    let namespace = ComponentStreamIdentity::new("oxide-batch-test.jsonl-writer-restart")?;
    let revisions =
        chunk_component_revisions_with_delivery_mode(ChunkDeliveryMode::AtomicSameResource)
            .with_stream_revision(
                namespace.clone(),
                ComponentRevision::new("jsonl-writer-v1")?,
            );

    let (writer_a, stream_a, contract_a) =
        jsonl_writer(&path, JsonLinesFormat::new(), namespace.clone())?;
    let log = InjectionLog::new();
    let injected_transactions_a = InjectedTransactions::new(
        fixture.transaction_manager(state_provider()),
        2,
        PreCommitAction::Fail,
        InjectionId::new(1),
        log.clone(),
    );
    let step_a = ChunkStep::new(
        oxide_batch::StepName::new("jsonl_writer_restart")?,
        ChunkSize::new(1)?,
        IterReader::new(vec![json!(1), json!(2), json!(3)]),
        Identity,
        writer_a,
        Arc::new(injected_transactions_a),
        Arc::new(NoCompletion),
    )
    .with_item_stream(namespace.clone(), stream_a, contract_a);
    let chunk_job_a = ChunkJob::new(
        job_name.clone(),
        step_a,
        DefinitionRevision::new("jsonl-writer-restart-v1")?,
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
    assert_eq!(chunk_report_a.committed_counts().written().get(), 1);

    let bytes_after_a = std::fs::read(&path)?;
    assert_eq!(
        bytes_after_a,
        b"1\n2\n".to_vec(),
        "record 2's bytes are physically present despite never committing"
    );

    let (writer_b, stream_b, contract_b) =
        jsonl_writer(&path, JsonLinesFormat::new(), namespace.clone())?;
    let step_b = ChunkStep::new(
        oxide_batch::StepName::new("jsonl_writer_restart")?,
        ChunkSize::new(1)?,
        IterReader::new(vec![json!(2), json!(3)]),
        Identity,
        writer_b,
        Arc::new(fixture.transaction_manager(state_provider())),
        Arc::new(NoCompletion),
    )
    .with_item_stream(namespace.clone(), stream_b, contract_b);
    let chunk_job_b = ChunkJob::new(
        job_name,
        step_b,
        DefinitionRevision::new("jsonl-writer-restart-v1")?,
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
    let final_bytes = std::fs::read(&path)?;
    assert_eq!(
        final_bytes,
        b"1\n2\n3\n".to_vec(),
        "the committed prefix is preserved and record 2 appears exactly once"
    );

    let _ = std::fs::remove_file(&path);
    Ok(())
}

/// Proves the JSONL writer fails closed rather than fabricating progress
/// when the output file is shorter than the last committed byte length.
#[tokio::test]
async fn jsonl_writer_fails_closed_when_the_file_is_shorter_than_committed()
-> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    PostgresFixture::migrate(url.clone()).await?;
    let fixture = PostgresFixture::connect(url).await?;

    let path = temp_path("jsonl-writer-fail-closed", "jsonl");
    let job_name = JobName::new(format!(
        "oxide_batch_148_jsonl_writer_fail_closed_{}",
        nonce()
    ))?;
    let namespace = ComponentStreamIdentity::new("oxide-batch-test.jsonl-writer-fail-closed")?;
    let revisions =
        chunk_component_revisions_with_delivery_mode(ChunkDeliveryMode::AtomicSameResource)
            .with_stream_revision(
                namespace.clone(),
                ComponentRevision::new("jsonl-writer-v1")?,
            );

    let (writer_a, stream_a, contract_a) =
        jsonl_writer(&path, JsonLinesFormat::new(), namespace.clone())?;
    let log = InjectionLog::new();
    let injected_reader_a = InjectedReader::new(
        IterReader::new(vec![json!(1), json!(2), json!(3)]),
        Trigger::after(1),
        ComponentAction::Stop(fixture_stop_source()),
        InjectionId::new(1),
        log.clone(),
    );
    let step_a = ChunkStep::new(
        oxide_batch::StepName::new("jsonl_writer_fail_closed")?,
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
        DefinitionRevision::new("jsonl-writer-fail-closed-v1")?,
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
    assert_eq!(std::fs::read(&path)?, b"1\n".to_vec());

    std::fs::write(&path, b"")?;

    let (writer_b, stream_b, contract_b) =
        jsonl_writer(&path, JsonLinesFormat::new(), namespace.clone())?;
    let step_b = ChunkStep::new(
        oxide_batch::StepName::new("jsonl_writer_fail_closed")?,
        ChunkSize::new(1)?,
        IterReader::new(vec![json!(3)]),
        Identity,
        writer_b,
        Arc::new(fixture.transaction_manager(state_provider())),
        Arc::new(NoCompletion),
    )
    .with_item_stream(namespace.clone(), stream_b, contract_b);
    let chunk_job_b = ChunkJob::new(
        job_name,
        step_b,
        DefinitionRevision::new("jsonl-writer-fail-closed-v1")?,
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
    );
    assert_eq!(std::fs::read(&path)?, Vec::<u8>::new());

    let _ = std::fs::remove_file(&path);
    Ok(())
}

// ------------------------------------------------------------ JSON array --

fn json_array_source() -> &'static str {
    "[\n  \"first\",\n  {\n    \"note\": \"a,b]c\\\"d\"\n  },\n  \"third\",\n  \"fourth\",\n  \"fifth\"\n]\n"
}

/// Proves streaming JSON-array reader restart resumes at exactly the last
/// committed *element* boundary -- never mid the multi-line, delimiter- and
/// escape-containing second element, never re-reading committed elements,
/// never skipping the uncommitted one -- through the real production
/// restart path and real durable committed state. Also proves a
/// naive line-count-based restart would land at the wrong byte offset for
/// this exact fixture, since the second element alone spans several
/// physical lines.
#[tokio::test]
async fn json_array_reader_restarts_after_the_last_committed_element_never_mid_element()
-> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    PostgresFixture::migrate(url.clone()).await?;
    let fixture = PostgresFixture::connect(url).await?;

    let source = json_array_source();
    let path = temp_path("json-array-reader-restart", "json");
    std::fs::write(&path, source)?;

    let job_name = JobName::new(format!(
        "oxide_batch_148_json_array_reader_restart_{}",
        nonce()
    ))?;
    let namespace = ComponentStreamIdentity::new("oxide-batch-test.json-array-reader-restart")?;
    let revisions =
        chunk_component_revisions_with_delivery_mode(ChunkDeliveryMode::AtomicSameResource)
            .with_stream_revision(
                namespace.clone(),
                ComponentRevision::new("json-array-reader-v1")?,
            );

    // Attempt A: chunk size 2. "first" + the multi-line object commit as
    // chunk 1. "third" is genuinely read (advancing the real position) and
    // buffered into chunk 2, then a stop is injected on "fourth"'s read
    // call, so chunk 2 never commits: "third" is consumed but not
    // committed.
    let (reader_a, stream_a, contract_a) =
        json_array_file_reader::<Value>(&path, JsonArrayFormat::new(), namespace.clone())?;
    let log = InjectionLog::new();
    let injected_reader_a = InjectedReader::new(
        reader_a,
        Trigger::after(3),
        ComponentAction::Stop(fixture_stop_source()),
        InjectionId::new(1),
        log.clone(),
    );
    let writer_a: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let step_a = ChunkStep::new(
        oxide_batch::StepName::new("json_array_reader_restart")?,
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
        DefinitionRevision::new("json-array-reader-restart-v1")?,
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
        values(&writer_a),
        vec![json!("first"), json!({"note": "a,b]c\"d"})],
        "only the first two elements committed in attempt A",
    );

    // Positive control: a naive restart that resumed after the Nth physical
    // line (N = 2 committed elements) would land far short of the real
    // checkpoint, because the second element alone spans multiple lines.
    let newline_positions: Vec<usize> = source
        .bytes()
        .enumerate()
        .filter(|&(_, byte)| byte == b'\n')
        .map(|(index, _)| index)
        .collect();
    let naive_two_line_offset = newline_positions[1] + 1;
    assert!(
        naive_two_line_offset
            < source
                .find("\"third\"")
                .expect("fixture contains \"third\""),
        "sanity: a 2-line-based resume point must land before the real second element even ends"
    );

    // Attempt B: a fresh reader/stream pair over the same path.
    let (reader_b, stream_b, contract_b) =
        json_array_file_reader::<Value>(&path, JsonArrayFormat::new(), namespace.clone())?;
    let writer_b: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::new(ObservingTransactions::new(
        fixture.transaction_manager(state_provider()),
    ));
    let step_b = ChunkStep::new(
        oxide_batch::StepName::new("json_array_reader_restart")?,
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
        DefinitionRevision::new("json-array-reader-restart-v1")?,
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
        "attempt B committed exactly the uncommitted remainder: third, fourth, fifth",
    );
    assert_eq!(
        values(&writer_b),
        vec![json!("third"), json!("fourth"), json!("fifth")],
        "attempt B resumed at \"third\" -- not re-reading the first two elements, not skipping \
         \"third\", not landing mid the multi-line second element",
    );

    let mut combined = values(&writer_a);
    combined.extend(values(&writer_b));
    assert_eq!(
        combined,
        vec![
            json!("first"),
            json!({"note": "a,b]c\"d"}),
            json!("third"),
            json!("fourth"),
            json!("fifth"),
        ],
        "committed exactly once each across both attempts",
    );

    let observed_progress = observed.observed_progress();
    assert_eq!(observed_progress.len(), 1);
    assert_eq!(observed_progress[0].read_ordinal(), 2);

    let _ = std::fs::remove_file(&path);
    Ok(())
}

/// Proves the JSON-array writer truncates a physically-present but
/// never-committed tail on restart, resumes with correct comma state (the
/// restart happens after at least one committed element), and produces
/// valid, exactly-once JSON when the array is finally closed.
#[tokio::test]
async fn json_array_writer_truncates_uncommitted_tail_and_resumes_exactly_once()
-> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    PostgresFixture::migrate(url.clone()).await?;
    let fixture = PostgresFixture::connect(url).await?;

    let path = temp_path("json-array-writer-truncate", "json");
    let job_name = JobName::new(format!(
        "oxide_batch_148_json_array_writer_restart_{}",
        nonce()
    ))?;
    let namespace = ComponentStreamIdentity::new("oxide-batch-test.json-array-writer-restart")?;
    let revisions =
        chunk_component_revisions_with_delivery_mode(ChunkDeliveryMode::AtomicSameResource)
            .with_stream_revision(
                namespace.clone(),
                ComponentRevision::new("json-array-writer-v1")?,
            );

    let (writer_a, stream_a, contract_a) = json_array_writer(&path, namespace.clone())?;
    let log = InjectionLog::new();
    let injected_transactions_a = InjectedTransactions::new(
        fixture.transaction_manager(state_provider()),
        2,
        PreCommitAction::Fail,
        InjectionId::new(1),
        log.clone(),
    );
    let step_a = ChunkStep::new(
        oxide_batch::StepName::new("json_array_writer_restart")?,
        ChunkSize::new(1)?,
        IterReader::new(vec![json!(1), json!(2), json!(3)]),
        Identity,
        writer_a,
        Arc::new(injected_transactions_a),
        Arc::new(NoCompletion),
    )
    .with_item_stream(namespace.clone(), stream_a, contract_a);
    let chunk_job_a = ChunkJob::new(
        job_name.clone(),
        step_a,
        DefinitionRevision::new("json-array-writer-restart-v1")?,
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
        "only chunk 1 (item 1) committed durably in attempt A",
    );

    let bytes_after_a = std::fs::read(&path)?;
    assert_eq!(
        bytes_after_a,
        b"[1,2".to_vec(),
        "item 2's bytes are physically present despite never committing, and the array is not \
         yet closed"
    );

    let (writer_b, stream_b, contract_b) = json_array_writer(&path, namespace.clone())?;
    let step_b = ChunkStep::new(
        oxide_batch::StepName::new("json_array_writer_restart")?,
        ChunkSize::new(1)?,
        IterReader::new(vec![json!(2), json!(3)]),
        Identity,
        writer_b,
        Arc::new(fixture.transaction_manager(state_provider())),
        Arc::new(NoCompletion),
    )
    .with_item_stream(namespace.clone(), stream_b, contract_b);
    let chunk_job_b = ChunkJob::new(
        job_name,
        step_b,
        DefinitionRevision::new("json-array-writer-restart-v1")?,
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
    let final_bytes = std::fs::read(&path)?;
    assert_eq!(
        final_bytes,
        b"[1,2,3]".to_vec(),
        "the committed prefix is preserved, comma state resumed correctly (no doubled/missing \
         comma), item 2 appears exactly once, and the array is closed exactly once",
    );
    let reparsed: Value = serde_json::from_slice(&final_bytes)?;
    assert_eq!(reparsed, json!([1, 2, 3]));

    let _ = std::fs::remove_file(&path);
    Ok(())
}

/// Proves the JSON-array writer fails closed rather than fabricating
/// progress when the output file is shorter than the last committed byte
/// length.
#[tokio::test]
async fn json_array_writer_fails_closed_when_the_file_is_shorter_than_committed()
-> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    PostgresFixture::migrate(url.clone()).await?;
    let fixture = PostgresFixture::connect(url).await?;

    let path = temp_path("json-array-writer-fail-closed", "json");
    let job_name = JobName::new(format!(
        "oxide_batch_148_json_array_writer_fail_closed_{}",
        nonce()
    ))?;
    let namespace = ComponentStreamIdentity::new("oxide-batch-test.json-array-writer-fail-closed")?;
    let revisions =
        chunk_component_revisions_with_delivery_mode(ChunkDeliveryMode::AtomicSameResource)
            .with_stream_revision(
                namespace.clone(),
                ComponentRevision::new("json-array-writer-v1")?,
            );

    let (writer_a, stream_a, contract_a) = json_array_writer(&path, namespace.clone())?;
    let log = InjectionLog::new();
    let injected_reader_a = InjectedReader::new(
        IterReader::new(vec![json!(1), json!(2), json!(3)]),
        Trigger::after(1),
        ComponentAction::Stop(fixture_stop_source()),
        InjectionId::new(1),
        log.clone(),
    );
    let step_a = ChunkStep::new(
        oxide_batch::StepName::new("json_array_writer_fail_closed")?,
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
        DefinitionRevision::new("json-array-writer-fail-closed-v1")?,
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
    assert_eq!(std::fs::read(&path)?, b"[1".to_vec());

    std::fs::write(&path, b"")?;

    let (writer_b, stream_b, contract_b) = json_array_writer(&path, namespace.clone())?;
    let step_b = ChunkStep::new(
        oxide_batch::StepName::new("json_array_writer_fail_closed")?,
        ChunkSize::new(1)?,
        IterReader::new(vec![json!(3)]),
        Identity,
        writer_b,
        Arc::new(fixture.transaction_manager(state_provider())),
        Arc::new(NoCompletion),
    )
    .with_item_stream(namespace.clone(), stream_b, contract_b);
    let chunk_job_b = ChunkJob::new(
        job_name,
        step_b,
        DefinitionRevision::new("json-array-writer-fail-closed-v1")?,
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
        "an output shorter than its committed checkpoint must fail closed",
    );
    assert_eq!(std::fs::read(&path)?, Vec::<u8>::new());

    let _ = std::fs::remove_file(&path);
    Ok(())
}
