#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

//! Multi-`ItemStream` lifecycle composition through catalog components
//! (#146, acceptance 5.8): reverse-successful-open close order, a close
//! failure on one stream never blocking another already-opened stream's
//! close attempt, and a close failure never erasing an earlier committed
//! outcome.
//!
//! This reuses the production, already-implemented (#144) multiple-stream
//! registration on [`ChunkStep`]/[`TestStep`] rather than having a catalog
//! wrapper reimplement open/close ordering itself: #146's composite/decorator
//! types never own `ItemStream` state of their own (see their contract
//! docs), so composing durable lifecycle is a property of registering
//! several streams against one step, proved here with `oxide-batch-test`'s
//! `TestStep` and `inject::InjectedStream`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use oxide_batch::item_components::{
    ChainProcessor, CompositeReader, IdentityProcessor, IterReader,
};
use oxide_batch::{
    BoxFuture, Checkpoint, ChunkCommitReceipt, ChunkCounts, ChunkExecutionOutcome, ChunkFailure,
    ChunkFaultProgress, ChunkSize, ChunkTransaction, ChunkTransactionError,
    ChunkTransactionManager, CodecId, CodecVersion, ComponentStateEnvelope,
    ComponentStreamIdentity, DefaultComponentCodec, ExecutionContext, FailureCategory, ItemStream,
    RestartabilityDeclaration, StateCodecError, StateLimits, StateSchemaId, StateSchemaVersion,
    StepName, StreamCloseContext, StreamCloseError, StreamCloseOutcome, StreamOpenContext,
    StreamOpenError, StreamOpenOutcome, StreamStateContract, StreamUpdateContext,
    StreamUpdateError, VersionedStateCodec,
};
use oxide_batch_test::TestStep;
use oxide_batch_test::inject::{InjectedStream, InjectionId, InjectionLog, StreamAction};

fn receipt() -> ChunkCommitReceipt {
    let checkpoint = Checkpoint::from_json(
        br#"{"format":"oxide-batch.checkpoint","format_version":1,"schema":"test.position","schema_version":1,"payload":{}}"#,
        StateLimits::default(),
    )
    .unwrap();
    let context = ExecutionContext::from_json(
        br#"{"format":"oxide-batch.execution-context","format_version":1,"schema":"test.context","schema_version":1,"payload":{}}"#,
        StateLimits::default(),
    )
    .unwrap();
    ChunkCommitReceipt::new(checkpoint, context)
}

/// A non-durable transaction that accepts registered `ItemStream` candidate
/// state instead of rejecting it, unlike `StandaloneTransactions`: this test
/// is about in-process close-ordering, not durable inheritance, so accepting
/// and discarding the candidate envelopes (never persisting them) is the
/// right-sized fixture rather than standing up a `PostgreSQL` adapter.
struct AcceptingTransaction;

impl ChunkTransaction for AcceptingTransaction {
    fn business_transaction(&mut self) -> Option<&mut dyn oxide_batch::BusinessTransaction> {
        None
    }

    fn commit(
        &mut self,
        _counts: ChunkCounts,
        _fault: ChunkFaultProgress,
    ) -> BoxFuture<'_, Result<ChunkCommitReceipt, ChunkTransactionError>> {
        Box::pin(async { Ok(receipt()) })
    }

    fn commit_with_component_state<'a>(
        &'a mut self,
        _counts: ChunkCounts,
        _fault: ChunkFaultProgress,
        _component_state: &'a [ComponentStateEnvelope],
    ) -> BoxFuture<'a, Result<ChunkCommitReceipt, ChunkTransactionError>> {
        Box::pin(async move { Ok(receipt()) })
    }

    fn rollback(&mut self) -> BoxFuture<'_, Result<(), ChunkTransactionError>> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Clone, Copy, Default)]
struct AcceptingTransactions;

impl ChunkTransactionManager for AcceptingTransactions {
    fn begin(
        &self,
    ) -> BoxFuture<'_, Result<Box<dyn ChunkTransaction + '_>, ChunkTransactionError>> {
        Box::pin(async move { Ok(Box::new(AcceptingTransaction) as Box<dyn ChunkTransaction>) })
    }
}

struct DummyCodec {
    schema: StateSchemaId,
}

impl VersionedStateCodec<u8> for DummyCodec {
    fn schema_id(&self) -> &StateSchemaId {
        &self.schema
    }

    fn current_version(&self) -> StateSchemaVersion {
        StateSchemaVersion::new(1).unwrap()
    }

    fn encode(&self, value: &u8) -> Result<Vec<u8>, StateCodecError> {
        Ok(format!(r#"{{"v":{value}}}"#).into_bytes())
    }

    fn decode(&self, payload: &[u8]) -> Result<u8, StateCodecError> {
        let text = std::str::from_utf8(payload).map_err(|_| StateCodecError::InvalidPayload)?;
        let start = text.find(':').ok_or(StateCodecError::InvalidPayload)?;
        let end = text.rfind('}').ok_or(StateCodecError::InvalidPayload)?;
        text.get(start + 1..end)
            .and_then(|value| value.trim().parse().ok())
            .ok_or(StateCodecError::InvalidPayload)
    }
}

fn codec(name: &str) -> DefaultComponentCodec<DummyCodec> {
    DefaultComponentCodec::new(
        DummyCodec {
            schema: StateSchemaId::new(name).unwrap(),
        },
        CodecId::new(format!("{name}-codec")).unwrap(),
        CodecVersion::new(1).unwrap(),
        RestartabilityDeclaration::Restartable,
    )
}

/// An `ItemStream` that records whether `close` was invoked, so a test can
/// prove a sibling stream's close failure never skipped this one's attempt.
struct FlagStream {
    closed: Arc<AtomicBool>,
    namespace: ComponentStreamIdentity,
    codec: DefaultComponentCodec<DummyCodec>,
}

impl ItemStream for FlagStream {
    async fn open(
        &self,
        _context: StreamOpenContext<'_>,
    ) -> Result<StreamOpenOutcome, StreamOpenError> {
        Ok(StreamOpenOutcome::Initial)
    }

    async fn update(
        &self,
        _context: StreamUpdateContext<'_>,
    ) -> Result<ComponentStateEnvelope, StreamUpdateError> {
        ComponentStateEnvelope::encode(
            self.namespace.clone(),
            &1u8,
            &self.codec,
            StateLimits::default(),
        )
        .map_err(|_| StreamUpdateError::new())
    }

    async fn close(
        &self,
        _context: StreamCloseContext<'_>,
    ) -> Result<StreamCloseOutcome, StreamCloseError> {
        self.closed.store(true, Ordering::SeqCst);
        Ok(StreamCloseOutcome::Closed)
    }
}

#[tokio::test]
async fn a_close_failure_on_one_stream_does_not_block_another_opened_streams_close() {
    let namespace_a = ComponentStreamIdentity::new("catalog.stream-a").unwrap();
    let namespace_b = ComponentStreamIdentity::new("catalog.stream-b").unwrap();
    let closed_a = Arc::new(AtomicBool::new(false));
    let closed_b_inner = Arc::new(AtomicBool::new(false));

    let stream_a = FlagStream {
        closed: Arc::clone(&closed_a),
        namespace: namespace_a.clone(),
        codec: codec("catalog.stream-a"),
    };
    let stream_b = FlagStream {
        closed: Arc::clone(&closed_b_inner),
        namespace: namespace_b.clone(),
        codec: codec("catalog.stream-b"),
    };
    let log = InjectionLog::new();
    let injected_b = InjectedStream::new(stream_b, log.clone()).with_close(
        StreamAction::Fail(FailureCategory::Timeout),
        InjectionId::new(1),
    );

    // Reader/processor are catalog components too, proving the composed
    // pipeline (not a bare hand-written one) drives this multi-stream step.
    let reader = CompositeReader::new(vec![IterReader::new(vec![1_i64, 2, 3])]);
    let processor: ChainProcessor<_, _, i64> =
        ChainProcessor::new(IdentityProcessor, IdentityProcessor);

    let mut step = TestStep::with_transactions(
        StepName::new("multi_stream").unwrap(),
        ChunkSize::new(10).unwrap(),
        reader,
        processor,
        oxide_batch::item_components::NoopWriter,
        Arc::new(AcceptingTransactions),
        Arc::new(oxide_batch_test::NoCompletion),
    )
    .with_item_stream(
        namespace_a,
        stream_a,
        StreamStateContract::new(codec("catalog.stream-a")),
    )
    .with_item_stream(
        namespace_b,
        injected_b,
        StreamStateContract::new(codec("catalog.stream-b")),
    );

    let report = step.run().await;

    assert!(
        matches!(
            report.outcome(),
            ChunkExecutionOutcome::Failed(ChunkFailure::StreamClose)
        ),
        "a close failure surfaces as StreamClose without erasing the run"
    );
    assert_eq!(
        report.original_outcome(),
        Some(ChunkExecutionOutcome::Completed),
        "the superseded primary outcome (a clean completion) must still be visible"
    );
    assert!(report.stream_close_failed());
    assert!(
        closed_a.load(Ordering::SeqCst),
        "stream A's close must still have been attempted after stream B's close failed"
    );
    assert!(log.fired(InjectionId::new(1)));
}
