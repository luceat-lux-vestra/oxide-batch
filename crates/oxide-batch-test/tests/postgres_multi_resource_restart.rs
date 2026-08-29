//! Real `PostgreSQL` crash/restart evidence for #150's multi-resource reader
//! (`ITEM-MULTI-001`): a crash that lands *before* the transition from one
//! physical resource to the next -- so the last durably committed envelope
//! still names the exhausted resource, not the one that would come next --
//! must resume by transitioning into the next resource itself on restart,
//! never replaying the exhausted one and never skipping the first item of
//! the next one. This exercises the real production restart path
//! (`ChunkJob`/`ChunkStep`/`PostgresChunkStateProvider`), the same way
//! `postgres_item_components_restart.rs` does for a single-resource
//! decorated reader -- this file is that evidence's multi-resource
//! counterpart.
//!
//! Requires `OXIDEBATCH_POSTGRES_TEST_URL`; skips (not fails) otherwise, per
//! this repository's `PostgreSQL` evidence convention.

#![cfg(feature = "postgres")]
#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, Mutex};

use oxide_batch::item_components::multi_resource::{
    BatchCountRollover, MultiResourceOpenError, MultiResourceReaderOpener,
    MultiResourceWriterOpener, ResourceIdentity, ResourceSet, multi_resource_reader,
    multi_resource_writer,
};
use oxide_batch::{
    BatchStatus, Checkpoint, ChunkCommitReceipt, ChunkCount, ChunkCounts, ChunkDeliveryMode,
    ChunkJob, ChunkSize, ChunkStep, ComponentRevision, ComponentStateEnvelope,
    ComponentStreamIdentity, DefaultComponentCodec, DefinitionRevision, ExecutionContext,
    ExecutionCounts, ItemProcessor, ItemReader, ItemStream, ItemWriter, JobName, JobParameters,
    PostgresChunkStateError, PostgresChunkStateProvider, ProcessContext, ProcessOutcome,
    ProcessorError, ReadContext, ReadOutcome, ReaderError, RestartabilityDeclaration,
    StateCodecError, StateLimits, StateSchemaId, StateSchemaVersion, StateSensitivity,
    StreamCloseContext, StreamCloseError, StreamCloseOutcome, StreamOpenContext, StreamOpenError,
    StreamOpenOutcome, StreamStateContract, StreamUpdateContext, StreamUpdateError,
    VersionedStateCodec, WriteContext, WriteOutcome, WriterError,
};
use oxide_batch_test::inject::{
    ComponentAction, InjectedReader, InjectedWriter, InjectionId, InjectionLog, Trigger,
};
use oxide_batch_test::postgres::PostgresFixture;
use oxide_batch_test::{NoCompletion, TestJob, chunk_component_revisions_with_delivery_mode};

fn state_provider() -> Arc<dyn PostgresChunkStateProvider> {
    Arc::new(|committed: ExecutionCounts, chunk: ChunkCounts| {
        let position = committed
            .read()
            .checked_add(chunk.read().get())
            .ok_or_else(PostgresChunkStateError::new)?;
        let checkpoint_bytes = format!(
            r#"{{"format":"oxide-batch.checkpoint","format_version":1,"schema":"oxide-batch-test.multi-resource-restart","schema_version":1,"payload":{{"position":{position}}}}}"#
        );
        let checkpoint = Checkpoint::from_json(checkpoint_bytes.as_bytes(), StateLimits::default())
            .map_err(|_| PostgresChunkStateError::new())?;
        let context = ExecutionContext::from_json(
                br#"{"format":"oxide-batch.execution-context","format_version":1,"schema":"oxide-batch-test.multi-resource-restart","schema_version":1,"payload":{}}"#,
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

impl ItemProcessor<u64, u64> for Identity {
    async fn process(
        &self,
        item: &u64,
        _context: ProcessContext<'_>,
    ) -> Result<ProcessOutcome<u64>, ProcessorError> {
        Ok(ProcessOutcome::Item(*item))
    }
}

struct RecordingWriter(Arc<Mutex<Vec<u64>>>);

impl ItemWriter<u64> for RecordingWriter {
    async fn write(
        &self,
        items: &[u64],
        _context: WriteContext<'_>,
    ) -> Result<WriteOutcome, WriterError> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend_from_slice(items);
        Ok(WriteOutcome::Written)
    }
}

// -- a minimal, real `ItemStream`-backed delegate reader (deliberately not
// `DelimitedReader`: this evidence's job is to prove the *multi-resource
// position scheme* survives real `PostgreSQL` persistence, a property of
// `MultiResourceReaderStream`'s own envelope, not of any particular
// delegate format -- delimited-file parsing already has its own PostgreSQL
// restart evidence elsewhere).

#[derive(Clone, Copy, Eq, PartialEq)]
struct VecPosition(u64);

const VEC_SCHEMA: &str = "oxide-batch-test.multi-resource-restart.vec-position";
const VEC_CODEC: &str = "oxide-batch-test.multi-resource-restart.vec-position-codec";

#[derive(Clone, Copy)]
struct VecSchema;

impl VersionedStateCodec<VecPosition> for VecSchema {
    fn schema_id(&self) -> &StateSchemaId {
        static SCHEMA: std::sync::OnceLock<StateSchemaId> = std::sync::OnceLock::new();
        SCHEMA.get_or_init(|| StateSchemaId::new(VEC_SCHEMA).unwrap())
    }

    fn current_version(&self) -> StateSchemaVersion {
        StateSchemaVersion::new(1).unwrap()
    }

    fn encode(&self, value: &VecPosition) -> Result<Vec<u8>, StateCodecError> {
        serde_json::to_vec(&serde_json::json!({ "ordinal": value.0 }))
            .map_err(|_| StateCodecError::InvalidPayload)
    }

    fn decode(&self, payload: &[u8]) -> Result<VecPosition, StateCodecError> {
        let value: serde_json::Value =
            serde_json::from_slice(payload).map_err(|_| StateCodecError::InvalidPayload)?;
        let ordinal = value
            .get("ordinal")
            .and_then(serde_json::Value::as_u64)
            .ok_or(StateCodecError::InvalidPayload)?;
        Ok(VecPosition(ordinal))
    }
}

fn vec_codec() -> DefaultComponentCodec<VecSchema> {
    DefaultComponentCodec::new(
        VecSchema,
        oxide_batch::CodecId::new(VEC_CODEC).unwrap(),
        oxide_batch::CodecVersion::new(1).unwrap(),
        RestartabilityDeclaration::Restartable,
    )
    .with_sensitivity(StateSensitivity::NonSensitive)
}

struct VecItemReader {
    items: Vec<u64>,
    ordinal: Arc<tokio::sync::Mutex<u64>>,
}

impl ItemReader<u64> for VecItemReader {
    async fn read(&mut self, _context: ReadContext<'_>) -> Result<ReadOutcome<u64>, ReaderError> {
        let mut ordinal = self.ordinal.lock().await;
        let index = usize::try_from(*ordinal).unwrap_or(usize::MAX);
        match self.items.get(index) {
            Some(item) => {
                let item = *item;
                *ordinal += 1;
                Ok(ReadOutcome::Item(item))
            }
            None => Ok(ReadOutcome::EndOfInput),
        }
    }
}

struct VecItemReaderStream {
    ordinal: Arc<tokio::sync::Mutex<u64>>,
    namespace: ComponentStreamIdentity,
}

impl ItemStream for VecItemReaderStream {
    async fn open(
        &self,
        context: StreamOpenContext<'_>,
    ) -> Result<StreamOpenOutcome, StreamOpenError> {
        let codec = vec_codec();
        if let Some(envelope) = context.inherited_state() {
            let restored = envelope
                .decode::<VecPosition>(&codec)
                .map_err(|_| StreamOpenError::new())?;
            *self.ordinal.lock().await = restored.0;
            Ok(StreamOpenOutcome::Restored)
        } else {
            *self.ordinal.lock().await = 0;
            Ok(StreamOpenOutcome::Initial)
        }
    }

    async fn update(
        &self,
        _context: StreamUpdateContext<'_>,
    ) -> Result<ComponentStateEnvelope, StreamUpdateError> {
        let codec = vec_codec();
        let ordinal = *self.ordinal.lock().await;
        ComponentStateEnvelope::encode(
            self.namespace.clone(),
            &VecPosition(ordinal),
            &codec,
            StateLimits::default(),
        )
        .map_err(|_| StreamUpdateError::new())
    }

    async fn close(
        &self,
        _context: StreamCloseContext<'_>,
    ) -> Result<StreamCloseOutcome, StreamCloseError> {
        Ok(StreamCloseOutcome::Closed)
    }
}

struct VecOpener {
    data: HashMap<String, Vec<u64>>,
}

impl MultiResourceReaderOpener<u64> for VecOpener {
    type Reader = VecItemReader;
    type Stream = VecItemReaderStream;

    async fn open(
        &self,
        resource: &ResourceIdentity,
        resource_ordinal: u32,
        delegate_identity: &ComponentStreamIdentity,
    ) -> Result<(Self::Reader, Self::Stream, StreamStateContract), MultiResourceOpenError> {
        let items = self
            .data
            .get(resource.as_str())
            .cloned()
            .unwrap_or_default();
        let ordinal = Arc::new(tokio::sync::Mutex::new(0));
        let reader = VecItemReader {
            items,
            ordinal: Arc::clone(&ordinal),
        };
        let stream = VecItemReaderStream {
            ordinal,
            namespace: delegate_identity.clone(),
        };
        let _ = resource_ordinal;
        Ok((reader, stream, StreamStateContract::new(vec_codec())))
    }
}

fn opener() -> VecOpener {
    let mut data = HashMap::new();
    data.insert("resource-a".to_owned(), vec![0, 1, 2, 3]);
    data.insert("resource-b".to_owned(), vec![4, 5, 6, 7, 8, 9]);
    VecOpener { data }
}

fn resources() -> ResourceSet {
    ResourceSet::new(vec![
        ResourceIdentity::new("resource-a").unwrap(),
        ResourceIdentity::new("resource-b").unwrap(),
    ])
}

fn fixture_stop_source() -> oxide_batch::StopSource {
    let (source, _token) = oxide_batch::StopSource::new();
    source
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "the two attempts and their committed-vs-candidate assertions are only meaningful together"
)]
async fn multi_resource_reader_restarts_across_a_resource_boundary_crash()
-> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };

    PostgresFixture::migrate(url.clone()).await?;
    let fixture = PostgresFixture::connect(url).await?;

    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)?
        .as_nanos();
    let job_name = JobName::new(format!("oxide_batch_150_multi_resource_restart_{nonce}"))?;
    let namespace = ComponentStreamIdentity::new("oxide-batch-test.multi-resource")?;
    let revisions =
        chunk_component_revisions_with_delivery_mode(ChunkDeliveryMode::AtomicSameResource)
            .with_stream_revision(
                namespace.clone(),
                ComponentRevision::new("multi-resource-v1")?,
            );

    // Attempt A: chunk size 2 over two resources (4 + 6 = 10 items). A stop
    // is injected on the 5th *real* underlying read -- exactly the read
    // that would cross from "resource-a" (4 items) into "resource-b" (6
    // items) -- so it never reaches `MultiResourceReader` at all. Chunks 1
    // and 2 (items 0..4, all of "resource-a") commit; the last durably
    // committed envelope therefore still names "resource-a" at its fully
    // exhausted position (ordinal 4), never having transitioned yet.
    let (reader_a, stream_a, contract_a) = multi_resource_reader::<u64, _>(
        resources(),
        opener(),
        namespace.clone(),
        RestartabilityDeclaration::Restartable,
    );
    let log = InjectionLog::new();
    let injected_reader_a = InjectedReader::new(
        reader_a,
        Trigger::after(4),
        ComponentAction::Stop(fixture_stop_source()),
        InjectionId::new(1),
        log.clone(),
    );
    let writer_a: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let step_a = ChunkStep::new(
        oxide_batch::StepName::new("multi-resource")?,
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
        DefinitionRevision::new("multi-resource-restart-v1")?,
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
    assert_eq!(chunk_report_a.committed_chunks(), ChunkCount::new(2));
    assert_eq!(chunk_report_a.committed_counts().read().get(), 4);
    assert_eq!(
        report_a.launch().job_execution().metadata().status(),
        BatchStatus::Stopped,
    );
    assert_eq!(
        *writer_a
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        vec![0, 1, 2, 3],
        "attempt A must have written exactly resource-a's items, never touching resource-b",
    );

    // Attempt B: a fresh multi-resource reader/stream pair over the same
    // (unchanged) resource set, launched again through the real production
    // restart path.
    let (reader_b, stream_b, contract_b) = multi_resource_reader::<u64, _>(
        resources(),
        opener(),
        namespace.clone(),
        RestartabilityDeclaration::Restartable,
    );
    let writer_b: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let step_b = ChunkStep::new(
        oxide_batch::StepName::new("multi-resource")?,
        ChunkSize::new(2)?,
        reader_b,
        Identity,
        RecordingWriter(Arc::clone(&writer_b)),
        Arc::new(fixture.transaction_manager(state_provider())),
        Arc::new(NoCompletion),
    )
    .with_item_stream(namespace, stream_b, contract_b);
    let chunk_job_b = ChunkJob::new(
        job_name,
        step_b,
        DefinitionRevision::new("multi-resource-restart-v1")?,
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
        6,
        "the restart reads exactly the uncommitted remainder (all of resource-b), never resource-a again",
    );
    assert_eq!(
        *writer_b
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        vec![4, 5, 6, 7, 8, 9],
        "the restart transitioned into resource-b and read it from its own start, \
         never replaying resource-a and never skipping resource-b's first item",
    );

    Ok(())
}

/// A crash before the very first committed chunk (no prior state at all)
/// must not require any special-casing distinct from a mid-attempt crash --
/// the `resource_set_revision`/`resource_index`/delegate envelope scheme
/// degrades to plain "no inherited state" cleanly. Also exercises a
/// same-attempt-completion path: the whole 10-item, two-resource read
/// completes in one attempt with no injected stop, proving normal
/// (non-crash) traversal across the resource boundary through the real
/// runtime too.
#[tokio::test]
async fn multi_resource_reader_completes_across_a_resource_boundary_with_no_crash()
-> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };

    PostgresFixture::migrate(url.clone()).await?;
    let fixture = PostgresFixture::connect(url).await?;

    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)?
        .as_nanos();
    let job_name = JobName::new(format!("oxide_batch_150_multi_resource_no_crash_{nonce}"))?;
    let namespace = ComponentStreamIdentity::new("oxide-batch-test.multi-resource-no-crash")?;
    let revisions =
        chunk_component_revisions_with_delivery_mode(ChunkDeliveryMode::AtomicSameResource)
            .with_stream_revision(
                namespace.clone(),
                ComponentRevision::new("multi-resource-v1")?,
            );

    let (reader, stream, contract) = multi_resource_reader::<u64, _>(
        resources(),
        opener(),
        namespace.clone(),
        RestartabilityDeclaration::Restartable,
    );
    let writer: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let step = ChunkStep::new(
        oxide_batch::StepName::new("multi-resource")?,
        ChunkSize::new(3)?,
        reader,
        Identity,
        RecordingWriter(Arc::clone(&writer)),
        Arc::new(fixture.transaction_manager(state_provider())),
        Arc::new(NoCompletion),
    )
    .with_item_stream(namespace, stream, contract);
    let chunk_job = ChunkJob::new(
        job_name,
        step,
        DefinitionRevision::new("multi-resource-restart-v1")?,
        &revisions,
    )?;
    let mut job = TestJob::new(
        chunk_job,
        fixture.repository().clone(),
        fixture.clock().clone(),
        fixture.ids().clone(),
    );
    let report = job.launch(&JobParameters::new()).await?;

    assert_eq!(
        report.launch().job_execution().metadata().status(),
        BatchStatus::Completed,
    );
    assert_eq!(
        *writer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        "a chunk boundary (size 3) that does not align with the resource boundary (4 items) \
         must still traverse both resources in full, in order",
    );

    Ok(())
}

// -- MultiResourceWriter crash/restart evidence. Confirmed absent before
// this: MultiResourceWriter's own contract doc (multi_resource.rs) declares
// real durable checkpoint state -- ResourceSetRevision plus the active
// resource's ordinal and embedded delegate position, "supplied explicitly at
// construction, same as multi_resource_reader" -- but unlike
// MultiResourceReader, nothing in this repository (crates/oxide-batch/tests
// or crates/oxide-batch-test/tests) exercised a crash at the writer's own
// resource-rollover boundary. This closes that gap, mirroring the reader
// evidence above exactly: a minimal, real ItemStream-backed delegate writer
// (deliberately not DelimitedWriter, for the same reason the reader evidence
// uses a Vec delegate -- this proves the multi-resource *position scheme*
// survives real PostgreSQL persistence, a property of
// MultiResourceWriterStream's own envelope, not of any particular delegate
// format).

struct VecItemWriter {
    resource: String,
    sink: Arc<Mutex<Vec<(String, u64)>>>,
    ordinal: Arc<tokio::sync::Mutex<u64>>,
}

impl ItemWriter<u64> for VecItemWriter {
    async fn write(
        &self,
        items: &[u64],
        _context: WriteContext<'_>,
    ) -> Result<WriteOutcome, WriterError> {
        let mut ordinal = self.ordinal.lock().await;
        let mut sink = self
            .sink
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for item in items {
            sink.push((self.resource.clone(), *item));
            *ordinal += 1;
        }
        Ok(WriteOutcome::Written)
    }
}

struct VecItemWriterStream {
    ordinal: Arc<tokio::sync::Mutex<u64>>,
    namespace: ComponentStreamIdentity,
}

impl ItemStream for VecItemWriterStream {
    async fn open(
        &self,
        context: StreamOpenContext<'_>,
    ) -> Result<StreamOpenOutcome, StreamOpenError> {
        let codec = vec_codec();
        if let Some(envelope) = context.inherited_state() {
            let restored = envelope
                .decode::<VecPosition>(&codec)
                .map_err(|_| StreamOpenError::new())?;
            *self.ordinal.lock().await = restored.0;
            Ok(StreamOpenOutcome::Restored)
        } else {
            *self.ordinal.lock().await = 0;
            Ok(StreamOpenOutcome::Initial)
        }
    }

    async fn update(
        &self,
        _context: StreamUpdateContext<'_>,
    ) -> Result<ComponentStateEnvelope, StreamUpdateError> {
        let codec = vec_codec();
        let ordinal = *self.ordinal.lock().await;
        ComponentStateEnvelope::encode(
            self.namespace.clone(),
            &VecPosition(ordinal),
            &codec,
            StateLimits::default(),
        )
        .map_err(|_| StreamUpdateError::new())
    }

    async fn close(
        &self,
        _context: StreamCloseContext<'_>,
    ) -> Result<StreamCloseOutcome, StreamCloseError> {
        Ok(StreamCloseOutcome::Closed)
    }
}

struct VecWriterOpener {
    sink: Arc<Mutex<Vec<(String, u64)>>>,
}

impl MultiResourceWriterOpener<u64> for VecWriterOpener {
    type Writer = VecItemWriter;
    type Stream = VecItemWriterStream;

    async fn open(
        &self,
        resource: &ResourceIdentity,
        _resource_ordinal: u32,
        delegate_identity: &ComponentStreamIdentity,
    ) -> Result<(Self::Writer, Self::Stream, StreamStateContract), MultiResourceOpenError> {
        let ordinal = Arc::new(tokio::sync::Mutex::new(0));
        let writer = VecItemWriter {
            resource: resource.as_str().to_owned(),
            sink: Arc::clone(&self.sink),
            ordinal: Arc::clone(&ordinal),
        };
        let stream = VecItemWriterStream {
            ordinal,
            namespace: delegate_identity.clone(),
        };
        Ok((writer, stream, StreamStateContract::new(vec_codec())))
    }
}

/// A crash that lands *before* the write batch that would roll the writer
/// over from one resource to the next -- so the last durably committed
/// envelope still names the exhausted resource -- must resume by
/// transitioning into the next resource itself on restart, never
/// re-writing the exhausted resource's items and never skipping the next
/// resource's first item. The writer analog of
/// `multi_resource_reader_restarts_across_a_resource_boundary_crash` above.
#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "the two attempts and their committed-vs-candidate assertions are only meaningful together"
)]
async fn multi_resource_writer_restarts_across_a_resource_boundary_crash()
-> Result<(), Box<dyn Error>> {
    let Some(url) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };

    PostgresFixture::migrate(url.clone()).await?;
    let fixture = PostgresFixture::connect(url).await?;

    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)?
        .as_nanos();
    let job_name = JobName::new(format!(
        "oxide_batch_150_multi_resource_writer_restart_{nonce}"
    ))?;
    let namespace = ComponentStreamIdentity::new("oxide-batch-test.multi-resource-writer")?;
    let reader_namespace =
        ComponentStreamIdentity::new("oxide-batch-test.multi-resource-writer.source")?;
    let revisions =
        chunk_component_revisions_with_delivery_mode(ChunkDeliveryMode::AtomicSameResource)
            .with_stream_revision(
                namespace.clone(),
                ComponentRevision::new("multi-resource-writer-v1")?,
            )
            .with_stream_revision(
                reader_namespace.clone(),
                ComponentRevision::new("multi-resource-writer-source-v1")?,
            );

    // Attempt A: ten items, chunk size 2 (one write batch per chunk),
    // rollover after 2 batches per resource -- so resource-a receives
    // batches 1-2 (items 0..4) and resource-b would receive batches 3-5
    // (items 4..10). A stop is injected on the 3rd write call -- exactly the
    // batch whose rollover decision would transition into resource-b -- so
    // it never reaches `MultiResourceWriter` at all. Batches 1-2 commit;
    // the last durably committed envelope therefore still names resource-a
    // at its fully written position (ordinal 4), never having transitioned.
    let sink_a: Arc<Mutex<Vec<(String, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    let (writer_a, stream_a, contract_a) = multi_resource_writer::<u64, _, _>(
        resources(),
        VecWriterOpener {
            sink: Arc::clone(&sink_a),
        },
        namespace.clone(),
        BatchCountRollover::new(2),
        RestartabilityDeclaration::Restartable,
    )?;
    let log = InjectionLog::new();
    let injected_writer_a = InjectedWriter::new(
        writer_a,
        Trigger::after(2),
        ComponentAction::Stop(fixture_stop_source()),
        InjectionId::new(1),
        log.clone(),
    );
    let (reader_a, reader_stream_a) = fixed_vec_reader(reader_namespace.clone());
    let step_a = ChunkStep::new(
        oxide_batch::StepName::new("multi-resource-writer")?,
        ChunkSize::new(2)?,
        reader_a,
        Identity,
        injected_writer_a,
        Arc::new(fixture.transaction_manager(state_provider())),
        Arc::new(NoCompletion),
    )
    .with_item_stream(namespace.clone(), stream_a, contract_a)
    .with_item_stream(
        reader_namespace.clone(),
        reader_stream_a,
        StreamStateContract::new(vec_codec()),
    );
    let chunk_job_a = ChunkJob::new(
        job_name.clone(),
        step_a,
        DefinitionRevision::new("multi-resource-writer-restart-v1")?,
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
    assert_eq!(chunk_report_a.committed_chunks(), ChunkCount::new(2));
    assert_eq!(
        report_a.launch().job_execution().metadata().status(),
        BatchStatus::Stopped,
    );
    assert_eq!(
        *sink_a
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        vec![
            ("resource-a".to_owned(), 0),
            ("resource-a".to_owned(), 1),
            ("resource-a".to_owned(), 2),
            ("resource-a".to_owned(), 3),
        ],
        "attempt A must have written exactly resource-a's items, never touching resource-b",
    );

    // Attempt B: a fresh multi-resource writer/stream pair over the same
    // resource set, launched again through the real production restart
    // path.
    let sink_b: Arc<Mutex<Vec<(String, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    let (writer_b, stream_b, contract_b) = multi_resource_writer::<u64, _, _>(
        resources(),
        VecWriterOpener {
            sink: Arc::clone(&sink_b),
        },
        namespace.clone(),
        BatchCountRollover::new(2),
        RestartabilityDeclaration::Restartable,
    )?;
    let (reader_b, reader_stream_b) = fixed_vec_reader(reader_namespace.clone());
    let step_b = ChunkStep::new(
        oxide_batch::StepName::new("multi-resource-writer")?,
        ChunkSize::new(2)?,
        reader_b,
        Identity,
        writer_b,
        Arc::new(fixture.transaction_manager(state_provider())),
        Arc::new(NoCompletion),
    )
    .with_item_stream(namespace, stream_b, contract_b)
    .with_item_stream(
        reader_namespace,
        reader_stream_b,
        StreamStateContract::new(vec_codec()),
    );
    let chunk_job_b = ChunkJob::new(
        job_name,
        step_b,
        DefinitionRevision::new("multi-resource-writer-restart-v1")?,
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
    assert_eq!(
        *sink_b
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        vec![
            ("resource-b".to_owned(), 4),
            ("resource-b".to_owned(), 5),
            ("resource-b".to_owned(), 6),
            ("resource-b".to_owned(), 7),
            ("resource-b".to_owned(), 8),
            ("resource-b".to_owned(), 9),
        ],
        "the restart transitioned into resource-b and wrote it from its own start, never \
         re-writing resource-a and never skipping resource-b's first item",
    );

    Ok(())
}

/// A checkpointed reader over the fixed `0..10` sequence, so attempt B's
/// restart resumes it through the framework's own checkpoint rather than
/// re-reading from the start (which would duplicate writes and defeat this
/// test's own assertions). The reader side of this evidence is not itself
/// what changed between attempts --
/// `multi_resource_reader_restarts_across_a_resource_boundary_crash` above
/// already proves `MultiResourceReader`'s own restart -- so this is a plain
/// single-resource checkpointed reader, registered under its own stream
/// identity distinct from the writer's.
struct FixedVecReader {
    items: Vec<u64>,
    ordinal: Arc<tokio::sync::Mutex<u64>>,
}

impl ItemReader<u64> for FixedVecReader {
    async fn read(&mut self, _context: ReadContext<'_>) -> Result<ReadOutcome<u64>, ReaderError> {
        let mut ordinal = self.ordinal.lock().await;
        let index = usize::try_from(*ordinal).unwrap_or(usize::MAX);
        match self.items.get(index) {
            Some(item) => {
                let item = *item;
                *ordinal += 1;
                Ok(ReadOutcome::Item(item))
            }
            None => Ok(ReadOutcome::EndOfInput),
        }
    }
}

struct FixedVecReaderStream {
    ordinal: Arc<tokio::sync::Mutex<u64>>,
    namespace: ComponentStreamIdentity,
}

impl ItemStream for FixedVecReaderStream {
    async fn open(
        &self,
        context: StreamOpenContext<'_>,
    ) -> Result<StreamOpenOutcome, StreamOpenError> {
        let codec = vec_codec();
        if let Some(envelope) = context.inherited_state() {
            let restored = envelope
                .decode::<VecPosition>(&codec)
                .map_err(|_| StreamOpenError::new())?;
            *self.ordinal.lock().await = restored.0;
            Ok(StreamOpenOutcome::Restored)
        } else {
            *self.ordinal.lock().await = 0;
            Ok(StreamOpenOutcome::Initial)
        }
    }

    async fn update(
        &self,
        _context: StreamUpdateContext<'_>,
    ) -> Result<ComponentStateEnvelope, StreamUpdateError> {
        let codec = vec_codec();
        let ordinal = *self.ordinal.lock().await;
        ComponentStateEnvelope::encode(
            self.namespace.clone(),
            &VecPosition(ordinal),
            &codec,
            StateLimits::default(),
        )
        .map_err(|_| StreamUpdateError::new())
    }

    async fn close(
        &self,
        _context: StreamCloseContext<'_>,
    ) -> Result<StreamCloseOutcome, StreamCloseError> {
        Ok(StreamCloseOutcome::Closed)
    }
}

fn fixed_vec_reader(namespace: ComponentStreamIdentity) -> (FixedVecReader, FixedVecReaderStream) {
    let ordinal = Arc::new(tokio::sync::Mutex::new(0));
    (
        FixedVecReader {
            items: (0..10).collect(),
            ordinal: Arc::clone(&ordinal),
        },
        FixedVecReaderStream { ordinal, namespace },
    )
}
