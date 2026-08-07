//! Named codec-lifecycle evidence for the M5 context-codec direction.
//!
//! The accepted
//! [M5 codec and capability direction](../../../docs/architecture/repository-and-transaction-model.md)
//! fixes four observable rules for the durable state envelope, and this file
//! carries one scenario for each:
//!
//! 1. an older recorded version is upgraded through one bounded, deterministic
//!    chain of declared directed edges;
//! 2. a recorded version newer than the codec is rejected rather than
//!    truncated, defaulted, or reinterpreted;
//! 3. an oversized or over-deep payload is a known not-committed outcome;
//! 4. a corrupt payload never advances a checkpoint.
//!
//! Rules 3 and 4 are runtime scenarios rather than value scenarios: a bound is
//! only meaningful if breaching it stops the commit that would have made the
//! bad state authoritative. They drive the chunk runtime through a transaction
//! manager that prepares durable state at the commit boundary exactly as the
//! `PostgreSQL` adapter does, and assert both the typed outcome and that the
//! retained checkpoint generation did not move.
//!
//! A fifth scenario holds that a retained payload never reaches a diagnostic.
//! It belongs here rather than with the
//! [facade review](../../../docs/project/m5-facade-api-review-evidence.md) it
//! was added for: the payload is a sensitive value the envelope owns, so the
//! type that retains it is the one that has to withhold it.

#![allow(clippy::expect_used, clippy::panic)]

use std::collections::VecDeque;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use oxide_batch::{
    BoxFuture, Checkpoint, ChunkCommitReceipt, ChunkCompletion, ChunkCompletionContext,
    ChunkCompletionError, ChunkCompletionOutcome, ChunkCounts, ChunkExecutionOutcome, ChunkFailure,
    ChunkFaultProgress, ChunkSize, ChunkStep, ChunkTransaction, ChunkTransactionError,
    ChunkTransactionManager, DurableStateKind, ExecutionAttempt, ExecutionContext,
    ExecutionCorrelation, ItemProcessor, ItemReader, ItemWriter, JobExecutionId, JobInstanceId,
    JobName, ProcessContext, ProcessOutcome, ProcessorError, ReadContext, ReadOutcome, ReaderError,
    StateCodecError, StateError, StateLimits, StateSchemaId, StateSchemaUpgrade,
    StateSchemaVersion, StepExecutionId, StepName, StopSource, VersionedStateCodec, WriteContext,
    WriteOutcome, WriterError,
};
use serde_json::{Map, Value, json};

/// A declared upgrade edge before it is validated into a [`StateSchemaUpgrade`].
type DeclaredEdge = (u32, u32, fn(&[u8]) -> Result<Vec<u8>, StateCodecError>);

/// The reader position the scenarios below record and restore.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Position {
    cursor: u64,
    label: String,
}

/// A codec whose schema reached version 3 through two published changes.
///
/// Version 1 recorded the cursor as `next_index`; version 2 renamed it to
/// `cursor`; version 3 added a mandatory `label`. Neither change is inferable
/// from a payload, which is why the codec declares them as directed edges
/// instead of leaving `decode` to guess what an old field meant.
struct PositionCodec {
    schema: StateSchemaId,
    current: StateSchemaVersion,
    upgrades: Vec<StateSchemaUpgrade>,
}

impl PositionCodec {
    /// Builds the codec with the complete declared chain from version 1.
    fn new() -> Result<Self, StateError> {
        Self::with_upgrades(3, vec![(1, 2, rename_to_cursor), (2, 3, add_label)])
    }

    /// Builds a codec with a chosen current version and declared edge set.
    fn with_upgrades(current: u32, upgrades: Vec<DeclaredEdge>) -> Result<Self, StateError> {
        let declared = upgrades
            .into_iter()
            .map(|(from, to, apply)| {
                StateSchemaUpgrade::new(
                    StateSchemaVersion::new(from)?,
                    StateSchemaVersion::new(to)?,
                    apply,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            schema: StateSchemaId::new("test.position")?,
            current: StateSchemaVersion::new(current)?,
            upgrades: declared,
        })
    }
}

impl VersionedStateCodec<Position> for PositionCodec {
    fn schema_id(&self) -> &StateSchemaId {
        &self.schema
    }

    fn current_version(&self) -> StateSchemaVersion {
        self.current
    }

    fn upgrades(&self) -> &[StateSchemaUpgrade] {
        &self.upgrades
    }

    fn encode(&self, value: &Position) -> Result<Vec<u8>, StateCodecError> {
        serde_json::to_vec(&json!({ "cursor": value.cursor, "label": value.label }))
            .map_err(|_| StateCodecError::InvalidPayload)
    }

    fn decode(&self, payload: &[u8]) -> Result<Position, StateCodecError> {
        let value: Map<String, Value> =
            serde_json::from_slice(payload).map_err(|_| StateCodecError::InvalidPayload)?;
        let cursor = value
            .get("cursor")
            .and_then(Value::as_u64)
            .ok_or(StateCodecError::InvalidPayload)?;
        let label = value
            .get("label")
            .and_then(Value::as_str)
            .ok_or(StateCodecError::InvalidPayload)?;
        Ok(Position {
            cursor,
            label: String::from(label),
        })
    }
}

/// Counts how many times each declared edge ran, so a scenario can prove a
/// chain applied each step exactly once and skipped the steps below the
/// recorded version.
static RENAMES: AtomicUsize = AtomicUsize::new(0);
static LABELS: AtomicUsize = AtomicUsize::new(0);

fn rename_to_cursor(payload: &[u8]) -> Result<Vec<u8>, StateCodecError> {
    RENAMES.fetch_add(1, Ordering::Relaxed);
    let mut value: Map<String, Value> =
        serde_json::from_slice(payload).map_err(|_| StateCodecError::InvalidPayload)?;
    let cursor = value
        .remove("next_index")
        .ok_or(StateCodecError::InvalidPayload)?;
    value.insert(String::from("cursor"), cursor);
    serde_json::to_vec(&Value::Object(value)).map_err(|_| StateCodecError::InvalidPayload)
}

fn add_label(payload: &[u8]) -> Result<Vec<u8>, StateCodecError> {
    LABELS.fetch_add(1, Ordering::Relaxed);
    let mut value: Map<String, Value> =
        serde_json::from_slice(payload).map_err(|_| StateCodecError::InvalidPayload)?;
    value.insert(String::from("label"), Value::String(String::from("legacy")));
    serde_json::to_vec(&Value::Object(value)).map_err(|_| StateCodecError::InvalidPayload)
}

/// Serializes a durable checkpoint envelope exactly as an adapter recorded it.
fn recorded(schema: &str, schema_version: u32, payload: &Value) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "format": "oxide-batch.checkpoint",
        "format_version": 1,
        "schema": schema,
        "schema_version": schema_version,
        "payload": payload,
    }))
    .expect("static envelope serializes")
}

#[test]
fn older_recorded_schema_version_upgrades_through_one_directed_chain() {
    let codec = PositionCodec::new().expect("static codec is valid");
    let limits = StateLimits::default();

    RENAMES.store(0, Ordering::Relaxed);
    LABELS.store(0, Ordering::Relaxed);
    let from_v1 = Checkpoint::from_json(
        &recorded("test.position", 1, &json!({ "next_index": 7 })),
        limits,
    )
    .expect("recorded version 1 envelope is well formed");
    assert_eq!(from_v1.schema_version().get(), 1);
    assert_eq!(
        from_v1
            .decode(&codec)
            .expect("version 1 upgrades to current"),
        Position {
            cursor: 7,
            label: String::from("legacy"),
        },
        "the full chain must apply both declared edges in order",
    );
    assert_eq!(
        (
            RENAMES.load(Ordering::Relaxed),
            LABELS.load(Ordering::Relaxed)
        ),
        (1, 1),
        "each declared edge runs exactly once",
    );

    RENAMES.store(0, Ordering::Relaxed);
    LABELS.store(0, Ordering::Relaxed);
    let from_v2 = Checkpoint::from_json(
        &recorded("test.position", 2, &json!({ "cursor": 11 })),
        limits,
    )
    .expect("recorded version 2 envelope is well formed");
    assert_eq!(
        from_v2
            .decode(&codec)
            .expect("version 2 upgrades to current"),
        Position {
            cursor: 11,
            label: String::from("legacy"),
        },
    );
    assert_eq!(
        (
            RENAMES.load(Ordering::Relaxed),
            LABELS.load(Ordering::Relaxed)
        ),
        (0, 1),
        "a chain starts at the recorded version, not at the oldest edge",
    );

    RENAMES.store(0, Ordering::Relaxed);
    LABELS.store(0, Ordering::Relaxed);
    let current = Checkpoint::from_json(
        &recorded(
            "test.position",
            3,
            &json!({ "cursor": 13, "label": "live" }),
        ),
        limits,
    )
    .expect("recorded current envelope is well formed");
    assert_eq!(
        current.decode(&codec).expect("current version decodes"),
        Position {
            cursor: 13,
            label: String::from("live"),
        },
    );
    assert_eq!(
        (
            RENAMES.load(Ordering::Relaxed),
            LABELS.load(Ordering::Relaxed)
        ),
        (0, 0),
        "an equal recorded version applies no upgrade",
    );
}

#[test]
fn newer_recorded_schema_version_is_rejected() {
    let codec = PositionCodec::new().expect("static codec is valid");
    let limits = StateLimits::default();

    let newer = Checkpoint::from_json(
        &recorded(
            "test.position",
            4,
            &json!({ "cursor": 21, "label": "live", "shard": "a" }),
        ),
        limits,
    )
    .expect("a newer envelope is still a well-formed envelope");
    assert_eq!(
        newer.decode(&codec),
        Err(StateError::UnsupportedSchemaVersion {
            kind: DurableStateKind::Checkpoint,
            found: StateSchemaVersion::new(4).expect("static version is nonzero"),
            current: StateSchemaVersion::new(3).expect("static version is nonzero"),
        }),
        "a newer recorded version must be rejected, never truncated to the \
         fields this codec happens to understand",
    );

    // A gap in the declared chain is the same class of failure: the codec
    // cannot reach its current version, so it must not guess.
    let gapped = PositionCodec::with_upgrades(3, vec![(2, 3, add_label)])
        .expect("a partial chain is a valid declaration");
    let from_v1 = Checkpoint::from_json(
        &recorded("test.position", 1, &json!({ "next_index": 7 })),
        limits,
    )
    .expect("recorded version 1 envelope is well formed");
    assert_eq!(
        from_v1.decode(&gapped),
        Err(StateError::NoUpgradePath {
            kind: DurableStateKind::Checkpoint,
            found: StateSchemaVersion::new(1).expect("static version is nonzero"),
            current: StateSchemaVersion::new(3).expect("static version is nonzero"),
        }),
        "an unreachable recorded version fails closed",
    );
}

#[test]
fn retained_payloads_never_reach_a_diagnostic() {
    let codec = PositionCodec::new().expect("static codec is valid");
    let limits = StateLimits::default();
    let sentinel = "oxide-batch-sentinel-payload-9c41";
    let position = Position {
        cursor: 11,
        label: sentinel.to_owned(),
    };

    let checkpoint = Checkpoint::encode(&position, &codec, limits)
        .expect("the position encodes as a checkpoint");
    let context = ExecutionContext::encode(&position, &codec, limits)
        .expect("the position encodes as a context");

    // The assertion below is only meaningful if the value really is retained.
    // A codec that dropped the label would produce a clean diagnostic for the
    // wrong reason, so the durable form is checked to carry it first.
    let durable = String::from_utf8(
        checkpoint
            .to_json()
            .expect("a retained checkpoint serializes"),
    )
    .expect("the envelope is UTF-8");
    assert!(
        durable.contains(sentinel),
        "the payload must reach durable state, or this scenario proves nothing",
    );

    let diagnostics = format!("{checkpoint:?}\n{context:?}");
    assert!(
        !diagnostics.contains(sentinel),
        "a retained payload must never reach a diagnostic",
    );
    assert!(
        diagnostics.contains("<redacted>"),
        "the payload is named as withheld rather than silently omitted",
    );
    assert!(
        diagnostics.contains("schema_version") && diagnostics.contains("encoded_bytes"),
        "the diagnostic still carries the structure an operator needs",
    );
}

// ---------------------------------------------------------------------------
// Runtime scenarios
// ---------------------------------------------------------------------------

/// Durable state the scenarios below treat as the committed generation.
#[derive(Default)]
struct DurableState {
    checkpoint: Mutex<Option<Checkpoint>>,
}

/// How a scenario makes state preparation fail at the commit boundary.
#[derive(Clone, Copy)]
enum Preparation {
    /// Encode a payload past the configured byte limit.
    Oversized,
    /// Encode a payload past the configured depth limit.
    OverDeep,
    /// Read back durable bytes whose payload no longer parses.
    Corrupt,
}

/// A transaction manager that prepares durable state at the commit boundary.
///
/// This mirrors the `PostgreSQL` adapter: the state provider runs inside
/// `commit`, and a preparation failure returns
/// [`ChunkTransactionError::NotCommitted`] before any durable write, so the
/// previously committed generation stays authoritative.
struct PreparingTransactions {
    durable: Arc<DurableState>,
    preparation: Preparation,
    limits: StateLimits,
}

impl ChunkTransactionManager for PreparingTransactions {
    fn begin(
        &self,
    ) -> BoxFuture<'_, Result<Box<dyn ChunkTransaction + '_>, ChunkTransactionError>> {
        let transaction = PreparingTransaction {
            durable: Arc::clone(&self.durable),
            preparation: self.preparation,
            limits: self.limits,
        };
        Box::pin(async move { Ok(Box::new(transaction) as Box<dyn ChunkTransaction>) })
    }
}

struct PreparingTransaction {
    durable: Arc<DurableState>,
    preparation: Preparation,
    limits: StateLimits,
}

impl PreparingTransaction {
    /// Produces the durable state this chunk would commit.
    fn prepare(&self) -> Result<Checkpoint, StateError> {
        let codec = PositionCodec::new()?;
        match self.preparation {
            Preparation::Oversized => Checkpoint::encode(
                &Position {
                    cursor: 1,
                    label: "x".repeat(self.limits.maximum_bytes()),
                },
                &codec,
                self.limits,
            ),
            Preparation::OverDeep => {
                let mut payload = json!({ "cursor": 1, "label": "deep" });
                for _ in 0..=self.limits.maximum_depth() {
                    payload = json!({ "nested": payload });
                }
                Checkpoint::from_json(&recorded("test.position", 3, &payload), self.limits)
            }
            Preparation::Corrupt => {
                // Bytes an adapter read back that are no longer a valid
                // envelope payload for this schema.
                Checkpoint::from_json(
                    &recorded("test.position", 3, &json!({ "cursor": "not-a-number" })),
                    self.limits,
                )
                .and_then(|checkpoint| {
                    checkpoint.decode(&codec)?;
                    Ok(checkpoint)
                })
            }
        }
    }
}

impl ChunkTransaction for PreparingTransaction {
    fn business_transaction(&mut self) -> Option<&mut dyn oxide_batch::BusinessTransaction> {
        None
    }

    fn commit(
        &mut self,
        _counts: ChunkCounts,
        _fault: ChunkFaultProgress,
    ) -> BoxFuture<'_, Result<ChunkCommitReceipt, ChunkTransactionError>> {
        Box::pin(async move {
            // Preparation runs before any durable write, exactly as the
            // adapter's state provider does.
            let checkpoint = self
                .prepare()
                .map_err(|_| ChunkTransactionError::NotCommitted)?;
            *self
                .durable
                .checkpoint
                .lock()
                .expect("durable checkpoint lock poisoned") = Some(checkpoint.clone());
            Ok(ChunkCommitReceipt::new(
                checkpoint,
                committed_context(self.limits),
            ))
        })
    }

    fn rollback(&mut self) -> BoxFuture<'_, Result<(), ChunkTransactionError>> {
        Box::pin(async { Ok(()) })
    }
}

/// The execution context a successful commit would publish.
fn committed_context(limits: StateLimits) -> ExecutionContext {
    struct Empty(StateSchemaId, StateSchemaVersion);

    impl VersionedStateCodec<()> for Empty {
        fn schema_id(&self) -> &StateSchemaId {
            &self.0
        }

        fn current_version(&self) -> StateSchemaVersion {
            self.1
        }

        fn encode(&self, (): &()) -> Result<Vec<u8>, StateCodecError> {
            Ok(b"{}".to_vec())
        }

        fn decode(&self, _payload: &[u8]) -> Result<(), StateCodecError> {
            Ok(())
        }
    }

    let codec = Empty(
        StateSchemaId::new("test.context").expect("static schema is valid"),
        StateSchemaVersion::new(1).expect("static version is nonzero"),
    );
    ExecutionContext::encode(&(), &codec, limits).expect("empty context encodes")
}

/// The generation committed before the failing attempt runs.
fn prior_generation(limits: StateLimits) -> Checkpoint {
    let codec = PositionCodec::new().expect("static codec is valid");
    Checkpoint::encode(
        &Position {
            cursor: 1,
            label: String::from("prior"),
        },
        &codec,
        limits,
    )
    .expect("the prior generation is within limits")
}

struct Items(VecDeque<i32>);

impl ItemReader<i32> for Items {
    fn read<'a>(
        &'a mut self,
        _context: ReadContext<'a>,
    ) -> BoxFuture<'a, Result<ReadOutcome<i32>, ReaderError>> {
        let item = self.0.pop_front();
        Box::pin(async move { Ok(item.map_or(ReadOutcome::EndOfInput, ReadOutcome::Item)) })
    }
}

struct Identity;

impl ItemProcessor<i32, i32> for Identity {
    fn process<'a>(
        &'a self,
        item: &'a i32,
        _context: ProcessContext<'a>,
    ) -> BoxFuture<'a, Result<ProcessOutcome<i32>, ProcessorError>> {
        let item = *item;
        Box::pin(async move { Ok(ProcessOutcome::Item(item)) })
    }
}

struct Sink;

impl ItemWriter<i32> for Sink {
    fn write<'a>(
        &'a self,
        _items: &'a [i32],
        _context: WriteContext<'a>,
    ) -> BoxFuture<'a, Result<WriteOutcome, WriterError>> {
        Box::pin(async { Ok(WriteOutcome::Written) })
    }
}

fn correlation() -> ExecutionCorrelation {
    let attempt =
        |value: u64| ExecutionAttempt::new(NonZeroU64::new(value).expect("attempt is nonzero"));
    ExecutionCorrelation::new(
        JobName::new("codec_bounds").expect("static job name is valid"),
        JobInstanceId::new(1).expect("static instance id is nonzero"),
        JobExecutionId::new(1).expect("static execution id is nonzero"),
        attempt(1),
        StepName::new("codec_step").expect("static step name is valid"),
        StepExecutionId::new(1).expect("static execution id is nonzero"),
        attempt(1),
    )
}

struct NoCompletion;

impl ChunkCompletion for NoCompletion {
    fn after_commit<'a>(
        &'a self,
        _context: ChunkCompletionContext<'a>,
    ) -> BoxFuture<'a, Result<ChunkCompletionOutcome, ChunkCompletionError>> {
        Box::pin(async { Ok(ChunkCompletionOutcome::Acknowledged) })
    }
}

/// Runs one chunk attempt whose commit-boundary preparation fails, and returns
/// the outcome together with the checkpoint that remained authoritative.
async fn run_failing_preparation(
    preparation: Preparation,
) -> (ChunkExecutionOutcome, Option<Checkpoint>) {
    let limits = StateLimits::default();
    let durable = Arc::new(DurableState {
        checkpoint: Mutex::new(Some(prior_generation(limits))),
    });
    let mut step = ChunkStep::new(
        StepName::new("codec_step").expect("static step name is valid"),
        ChunkSize::new(2).expect("static chunk size is nonzero"),
        Box::new(Items(VecDeque::from(vec![1, 2, 3]))),
        Arc::new(Identity),
        Arc::new(Sink),
        Arc::new(PreparingTransactions {
            durable: Arc::clone(&durable),
            preparation,
            limits,
        }),
        Arc::new(NoCompletion),
    );
    let (_source, stop_token) = StopSource::new();
    let report = step.execute(&correlation(), &stop_token).await;
    let retained = durable
        .checkpoint
        .lock()
        .expect("durable checkpoint lock poisoned")
        .clone();
    (report.outcome(), retained)
}

#[tokio::test]
async fn oversized_or_over_deep_payload_is_a_known_not_committed_outcome() {
    let limits = StateLimits::default();
    let prior = prior_generation(limits);

    for preparation in [Preparation::Oversized, Preparation::OverDeep] {
        let (outcome, retained) = run_failing_preparation(preparation).await;
        assert_eq!(
            outcome,
            ChunkExecutionOutcome::Failed(ChunkFailure::TransactionCommit),
            "a bounded-state breach is a known not-committed failure, never an \
             unknown outcome and never a silent success",
        );
        assert_eq!(
            retained,
            Some(prior.clone()),
            "the previously committed generation stays authoritative",
        );
    }
}

#[tokio::test]
async fn corrupt_payload_never_advances_a_checkpoint() {
    let limits = StateLimits::default();
    let prior = prior_generation(limits);
    let (outcome, retained) = run_failing_preparation(Preparation::Corrupt).await;

    assert_eq!(
        outcome,
        ChunkExecutionOutcome::Failed(ChunkFailure::TransactionCommit),
        "a corrupt payload is a known not-committed outcome",
    );
    assert_eq!(
        retained,
        Some(prior),
        "a corrupt payload never advances the checkpoint and never degrades \
         to an empty context",
    );
}
