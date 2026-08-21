//! A restartable range component and the restart-harness pattern
//! (`restart_harness_resumes_from_the_last_committed_checkpoint`).
//!
//! `oxide-batch` has no reader-level resume hook: an `ItemReader` call
//! receives only a [`ReadContext`], never the durable checkpoint. A stateful
//! reader resumes the same way Gate C's `ItemStream` contract intends --
//! [`ItemStream::open`] runs before any reader call in the attempt and
//! receives the last *committed* envelope, so a reader that pairs itself
//! with an `ItemStream` restores its own position there. [`RangeReader`] and
//! [`RangeStream`] below share position state through an `Arc<AtomicU64>` to
//! demonstrate exactly this pattern, reusing the real `ItemStream` lifecycle
//! landed by #161 rather than inventing a shortcut.
//!
//! The restart itself needs no special harness call: calling
//! [`crate::TestJob::launch`] again with the same identifying `JobParameters`
//! against the same repository *is* the production restart path --
//! `JobLauncher` selects the existing instance, opens a new execution
//! attempt, and the durable [`oxide_batch::ChunkTransactionManager`]
//! supplies the last committed [`ComponentStateEnvelope`] to
//! [`ItemStream::open`] automatically. This module supplies the paired
//! reader/stream a restart test needs; it does not reimplement restart
//! selection.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use oxide_batch::{
    BoxFuture, ChunkTransaction, ChunkTransactionContext, ChunkTransactionError,
    ChunkTransactionManager, CodecId, CodecVersion, ComponentStateEnvelope,
    ComponentStreamIdentity, DefaultComponentCodec, InheritedStepProgress, ItemReader, ItemStream,
    ReadContext, ReadOutcome, ReaderError, RestartabilityDeclaration, StateCodecError, StateLimits,
    StateSchemaId, StateSchemaVersion, StreamCloseContext, StreamCloseError, StreamCloseOutcome,
    StreamOpenContext, StreamOpenError, StreamOpenOutcome, StreamStateContract,
    StreamUpdateContext, StreamUpdateError, VersionedStateCodec,
};

const SCHEMA: &str = "oxide-batch-test.range-position";
const CODEC: &str = "oxide-batch-test.range-position-codec";

#[derive(Clone)]
struct PositionCodec {
    schema_id: StateSchemaId,
}

impl VersionedStateCodec<u64> for PositionCodec {
    fn schema_id(&self) -> &StateSchemaId {
        &self.schema_id
    }

    #[allow(
        clippy::unwrap_used,
        reason = "fixed literal schema version cannot fail validation"
    )]
    fn current_version(&self) -> StateSchemaVersion {
        StateSchemaVersion::new(1).unwrap()
    }

    fn encode(&self, value: &u64) -> Result<Vec<u8>, StateCodecError> {
        Ok(format!(r#"{{"position":{value}}}"#).into_bytes())
    }

    fn decode(&self, payload: &[u8]) -> Result<u64, StateCodecError> {
        let text = std::str::from_utf8(payload).map_err(|_| StateCodecError::InvalidPayload)?;
        let start = text.find(':').ok_or(StateCodecError::InvalidPayload)?;
        let end = text.rfind('}').ok_or(StateCodecError::InvalidPayload)?;
        text.get(start + 1..end)
            .and_then(|value| value.trim().parse().ok())
            .ok_or(StateCodecError::InvalidPayload)
    }
}

#[allow(
    clippy::unwrap_used,
    reason = "fixed literal identities cannot fail validation"
)]
fn position_codec() -> DefaultComponentCodec<PositionCodec> {
    let schema = PositionCodec {
        schema_id: StateSchemaId::new(SCHEMA).unwrap(),
    };
    DefaultComponentCodec::new(
        schema,
        CodecId::new(CODEC).unwrap(),
        CodecVersion::new(1).unwrap(),
        RestartabilityDeclaration::Restartable,
    )
}

/// A minimal, position-tracking `ItemReader<u64>` over `0..len`.
///
/// It has no resume logic of its own: its paired [`RangeStream`] restores
/// (or resets) the shared position before this reader's first call in an
/// attempt, so the reader itself only ever reads forward from wherever that
/// shared position currently is.
pub struct RangeReader {
    position: Arc<AtomicU64>,
    len: u64,
}

impl ItemReader<u64> for RangeReader {
    async fn read(&mut self, _context: ReadContext<'_>) -> Result<ReadOutcome<u64>, ReaderError> {
        let current = self.position.load(Ordering::SeqCst);
        Ok(if current >= self.len {
            ReadOutcome::EndOfInput
        } else {
            self.position.store(current + 1, Ordering::SeqCst);
            ReadOutcome::Item(current)
        })
    }
}

/// The `ItemStream` half of a [`RangeReader`]: restores the shared position
/// from the last committed envelope on `open`, and reports the current
/// shared position as the candidate envelope on `update`.
pub struct RangeStream {
    position: Arc<AtomicU64>,
    codec: DefaultComponentCodec<PositionCodec>,
    namespace: ComponentStreamIdentity,
}

impl ItemStream for RangeStream {
    async fn open(
        &self,
        context: StreamOpenContext<'_>,
    ) -> Result<StreamOpenOutcome, StreamOpenError> {
        if let Some(envelope) = context.inherited_state() {
            let position = envelope
                .decode::<u64>(&self.codec)
                .map_err(|_| StreamOpenError::new())?;
            self.position.store(position, Ordering::SeqCst);
            Ok(StreamOpenOutcome::Restored)
        } else {
            self.position.store(0, Ordering::SeqCst);
            Ok(StreamOpenOutcome::Initial)
        }
    }

    async fn update(
        &self,
        _context: StreamUpdateContext<'_>,
    ) -> Result<ComponentStateEnvelope, StreamUpdateError> {
        let position = self.position.load(Ordering::SeqCst);
        ComponentStateEnvelope::encode(
            self.namespace.clone(),
            &position,
            &self.codec,
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

/// Builds a fresh `(reader, stream, contract)` triple over `0..len`,
/// namespaced under `identity`.
///
/// Register the stream with
/// [`ChunkStep::with_item_stream`](oxide_batch::ChunkStep::with_item_stream)
/// under the same `identity`, and declare `identity` in the job's
/// [`ChunkComponentRevisions`](oxide_batch::ChunkComponentRevisions) via
/// [`with_stream_revision`](oxide_batch::ChunkComponentRevisions::with_stream_revision)
/// so the runtime resolves the reader's paired stream to the durable
/// namespace a restart inherits from.
#[must_use]
pub fn range_reader(
    identity: ComponentStreamIdentity,
    len: u64,
) -> (RangeReader, RangeStream, StreamStateContract, RangePosition) {
    let position = Arc::new(AtomicU64::new(0));
    let contract = StreamStateContract::new(position_codec());
    let reader = RangeReader {
        position: Arc::clone(&position),
        len,
    };
    let stream = RangeStream {
        position: Arc::clone(&position),
        codec: position_codec(),
        namespace: identity,
    };
    (reader, stream, contract, RangePosition(position))
}

/// A cheap, shared handle onto a [`RangeReader`]/[`RangeStream`] pair's
/// in-memory position, for test observation.
///
/// This reads the reader's own uncommitted, process-local counter -- it
/// updates the moment a call to [`ItemReader::read`] returns an item,
/// regardless of whether the chunk that reads it ever durably commits. A
/// test that wants to observe a rolled-back candidate's value (as opposed to
/// what the repository durably inherited) reads this handle, not durable
/// state.
#[derive(Clone, Debug)]
pub struct RangePosition(Arc<AtomicU64>);

impl RangePosition {
    /// Returns the current in-memory position.
    #[must_use]
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// A [`ChunkTransactionManager`] decorator that records every
/// [`InheritedStepProgress`] and inherited [`ComponentStateEnvelope`] set
/// the framework actually reads through it, in call order.
///
/// A restart test wraps its manager with this to observe exactly what a new
/// attempt inherited at the moment the framework itself asked for it --
/// proving what a genuine restart resumed from, rather than re-deriving it
/// independently after the fact and risking a mismatched observation
/// window.
pub struct ObservingTransactions<M> {
    inner: M,
    progress: Mutex<Vec<InheritedStepProgress>>,
    component_state: Mutex<Vec<Vec<ComponentStateEnvelope>>>,
}

impl<M> ObservingTransactions<M> {
    /// Wraps `inner`, recording nothing yet.
    pub const fn new(inner: M) -> Self {
        Self {
            inner,
            progress: Mutex::new(Vec::new()),
            component_state: Mutex::new(Vec::new()),
        }
    }

    /// Returns every observed [`InheritedStepProgress`], in call order.
    #[must_use]
    pub fn observed_progress(&self) -> Vec<InheritedStepProgress> {
        self.progress
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Returns every observed inherited component-state set, in call order.
    #[must_use]
    pub fn observed_component_state(&self) -> Vec<Vec<ComponentStateEnvelope>> {
        self.component_state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl<M: ChunkTransactionManager> ChunkTransactionManager for ObservingTransactions<M> {
    fn begin(
        &self,
    ) -> BoxFuture<'_, Result<Box<dyn ChunkTransaction + '_>, ChunkTransactionError>> {
        self.inner.begin()
    }

    fn begin_for(
        &self,
        context: ChunkTransactionContext,
    ) -> BoxFuture<'_, Result<Box<dyn ChunkTransaction + '_>, ChunkTransactionError>> {
        self.inner.begin_for(context)
    }

    fn inherited_progress(
        &self,
        context: ChunkTransactionContext,
    ) -> BoxFuture<'_, Result<InheritedStepProgress, ChunkTransactionError>> {
        Box::pin(async move {
            let progress = self.inner.inherited_progress(context).await?;
            self.progress
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(progress);
            Ok(progress)
        })
    }

    fn inherited_component_state(
        &self,
        context: ChunkTransactionContext,
    ) -> BoxFuture<'_, Result<Vec<ComponentStateEnvelope>, ChunkTransactionError>> {
        Box::pin(async move {
            let state = self.inner.inherited_component_state(context).await?;
            self.component_state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(state.clone());
            Ok(state)
        })
    }
}
