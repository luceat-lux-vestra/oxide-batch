//! Reusable M2 component and durable-state contract cases.

use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt;
use std::sync::{Arc, Mutex};

use futures_executor::block_on;
use oxide_batch::{
    BoxFuture, BusinessStatement, BusinessTransaction, BusinessTransactionError, BusinessValue,
    BusinessWriteResult, Checkpoint, ChunkCompletion, ChunkCompletionContext, ChunkCompletionError,
    ChunkCompletionOutcome, ChunkCount, ChunkCounts, ChunkSize, ExecutionContext, ItemProcessor,
    ItemReader, ItemWriter, ProcessContext, ProcessOutcome, ProcessorError, ReadContext,
    ReadOutcome, ReaderError, StateCodecError, StateError, StateLimits, StateSchemaId,
    StateSchemaUpgrade, StateSchemaVersion, StopSource, VersionedStateCodec, WriteContext,
    WriteOutcome, WriterError,
};
use serde_json::{Map, Value, json};

/// Runs deterministic component success, failure, stop, enlistment, size, and
/// old-version durable-state cases.
///
/// # Errors
///
/// Returns a stable case name and redacted detail when a contract observation
/// differs.
pub fn run_component_contract() -> Result<(), ComponentContractFailure> {
    reader_distinguishes_item_end_failure_and_stop()?;
    processor_distinguishes_item_filter_failure_and_stop()?;
    writer_borrows_enlisted_transaction_and_redacts_values()?;
    completion_acknowledges_only_committed_evidence()?;
    versioned_state_upgrades_old_values_and_enforces_limits()
}

struct ContractReader {
    items: VecDeque<u64>,
    fail_next: bool,
}

impl ItemReader<u64> for ContractReader {
    async fn read(&mut self, context: ReadContext<'_>) -> Result<ReadOutcome<u64>, ReaderError> {
        if context.stop_token().is_stop_requested() {
            return Ok(ReadOutcome::Stopped);
        }
        if self.fail_next {
            self.fail_next = false;
            return Err(ReaderError::new());
        }
        Ok(self
            .items
            .pop_front()
            .map_or(ReadOutcome::EndOfInput, ReadOutcome::Item))
    }
}

fn reader_distinguishes_item_end_failure_and_stop() -> Result<(), ComponentContractFailure> {
    const CASE: &str = "reader_typed_outcomes";
    let (_source, stop) = StopSource::new();
    let mut reader = ContractReader {
        items: VecDeque::from([7]),
        fail_next: false,
    };
    ensure(
        block_on(reader.read(ReadContext::new(&stop))) == Ok(ReadOutcome::Item(7)),
        CASE,
        "reader did not return its deterministic item",
    )?;
    ensure(
        block_on(reader.read(ReadContext::new(&stop))) == Ok(ReadOutcome::EndOfInput),
        CASE,
        "reader did not distinguish normal end of input",
    )?;

    reader.fail_next = true;
    ensure(
        block_on(reader.read(ReadContext::new(&stop))) == Err(ReaderError::new()),
        CASE,
        "reader did not return a typed component failure",
    )?;

    let (source, stopped) = StopSource::new();
    source.request_stop();
    ensure(
        block_on(reader.read(ReadContext::new(&stopped))) == Ok(ReadOutcome::Stopped),
        CASE,
        "reader did not distinguish cooperative stop",
    )
}

struct ContractProcessor;

impl ItemProcessor<u64, String> for ContractProcessor {
    async fn process(
        &self,
        item: &u64,
        context: ProcessContext<'_>,
    ) -> Result<ProcessOutcome<String>, ProcessorError> {
        if context.stop_token().is_stop_requested() {
            return Ok(ProcessOutcome::Stopped);
        }
        match *item {
            13 => Err(ProcessorError::new()),
            value if value % 2 == 0 => Ok(ProcessOutcome::Filtered),
            value => Ok(ProcessOutcome::Item(format!("item-{value}"))),
        }
    }
}

fn processor_distinguishes_item_filter_failure_and_stop() -> Result<(), ComponentContractFailure> {
    const CASE: &str = "processor_typed_outcomes";
    let processor = ContractProcessor;
    let (_source, stop) = StopSource::new();
    ensure(
        block_on(processor.process(&7, ProcessContext::new(&stop)))
            == Ok(ProcessOutcome::Item(String::from("item-7"))),
        CASE,
        "processor did not return deterministic output",
    )?;
    ensure(
        block_on(processor.process(&8, ProcessContext::new(&stop))) == Ok(ProcessOutcome::Filtered),
        CASE,
        "processor did not distinguish a filtered item",
    )?;
    ensure(
        block_on(processor.process(&13, ProcessContext::new(&stop))) == Err(ProcessorError::new()),
        CASE,
        "processor did not return a typed component failure",
    )?;
    let (source, stopped) = StopSource::new();
    source.request_stop();
    ensure(
        block_on(processor.process(&7, ProcessContext::new(&stopped)))
            == Ok(ProcessOutcome::Stopped),
        CASE,
        "processor did not distinguish cooperative stop",
    )
}

#[derive(Default)]
struct TransactionObservations {
    texts: Vec<String>,
    value_kinds: Vec<Vec<oxide_batch::BusinessValueKind>>,
}

struct ContractTransaction {
    observations: Arc<Mutex<TransactionObservations>>,
}

impl BusinessTransaction for ContractTransaction {
    fn execute<'a>(
        &'a mut self,
        statement: BusinessStatement<'a>,
    ) -> BoxFuture<'a, Result<BusinessWriteResult, BusinessTransactionError>> {
        Box::pin(async move {
            let mut observations = self
                .observations
                .lock()
                .map_err(|_| BusinessTransactionError::Infrastructure)?;
            observations.texts.push(String::from(statement.text()));
            observations.value_kinds.push(
                statement
                    .values()
                    .iter()
                    .map(|value| value.kind())
                    .collect(),
            );
            Ok(BusinessWriteResult::new(1))
        })
    }
}

struct ContractWriter;

impl ItemWriter<String> for ContractWriter {
    async fn write(
        &self,
        items: &[String],
        mut context: WriteContext<'_>,
    ) -> Result<WriteOutcome, WriterError> {
        if context.stop_token().is_stop_requested() {
            return Ok(WriteOutcome::Stopped);
        }
        let Some(transaction) = context.transaction() else {
            return Err(WriterError::new());
        };
        for item in items {
            let values = [BusinessValue::text(item)];
            let statement = BusinessStatement::new(
                "INSERT INTO inventory_output (payload) VALUES ($1)",
                &values,
            );
            transaction
                .execute(statement)
                .await
                .map_err(WriterError::from_error)?;
        }
        Ok(WriteOutcome::Written)
    }
}

fn writer_borrows_enlisted_transaction_and_redacts_values() -> Result<(), ComponentContractFailure>
{
    const CASE: &str = "writer_transaction_enlistment";
    let observations = Arc::new(Mutex::new(TransactionObservations::default()));
    let mut transaction = ContractTransaction {
        observations: Arc::clone(&observations),
    };
    let writer = ContractWriter;
    let (_source, stop) = StopSource::new();
    let sentinel = String::from("sentinel-business-value");
    ensure(
        block_on(writer.write(
            std::slice::from_ref(&sentinel),
            WriteContext::enlisted(&stop, &mut transaction),
        )) == Ok(WriteOutcome::Written),
        CASE,
        "enlisted writer did not acknowledge its item",
    )?;
    let observations = observations
        .lock()
        .map_err(|_| ComponentContractFailure::new(CASE, "observation lock was poisoned"))?;
    ensure(
        observations.texts == ["INSERT INTO inventory_output (payload) VALUES ($1)"],
        CASE,
        "writer did not execute through the borrowed transaction",
    )?;
    ensure(
        observations.value_kinds == [vec![oxide_batch::BusinessValueKind::Text]],
        CASE,
        "bound value kinds were not retained by the adapter",
    )?;
    let diagnostics = format!(
        "{:?}\n{:?}",
        BusinessValue::text(&sentinel),
        BusinessStatement::new("sentinel SQL", &[BusinessValue::text(&sentinel)])
    );
    ensure(
        !diagnostics.contains(&sentinel) && !diagnostics.contains("sentinel SQL"),
        CASE,
        "business statement diagnostics exposed SQL or bound values",
    )?;
    drop(observations);

    ensure(
        block_on(writer.write(
            std::slice::from_ref(&sentinel),
            WriteContext::non_transactional(&stop),
        )) == Err(WriterError::new()),
        CASE,
        "transaction-required writer accepted a non-enlisted call",
    )?;
    let (source, stopped) = StopSource::new();
    source.request_stop();
    ensure(
        block_on(writer.write(
            std::slice::from_ref(&sentinel),
            WriteContext::non_transactional(&stopped),
        )) == Ok(WriteOutcome::Stopped),
        CASE,
        "writer did not distinguish cooperative stop",
    )
}

struct InventoryCodec {
    schema: StateSchemaId,
    current: StateSchemaVersion,
    upgrades: [StateSchemaUpgrade; 1],
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct InventoryState {
    cursor: u64,
    source_checksum: Option<String>,
    extensions: BTreeMap<String, String>,
}

impl InventoryCodec {
    fn new() -> Result<Self, StateError> {
        Ok(Self {
            schema: StateSchemaId::new("inventory-import")?,
            current: StateSchemaVersion::new(2)?,
            upgrades: [StateSchemaUpgrade::new(
                StateSchemaVersion::new(1)?,
                StateSchemaVersion::new(2)?,
                rename_next_index_to_cursor,
            )?],
        })
    }
}

/// The declared version 1 to version 2 upgrade: the reader position was
/// renamed and nothing else about the payload changed.
fn rename_next_index_to_cursor(payload: &[u8]) -> Result<Vec<u8>, StateCodecError> {
    let mut value: Map<String, Value> =
        serde_json::from_slice(payload).map_err(|_| StateCodecError::InvalidPayload)?;
    let cursor = value
        .remove("next_index")
        .ok_or(StateCodecError::InvalidPayload)?;
    value.insert(String::from("cursor"), cursor);
    serde_json::to_vec(&Value::Object(value)).map_err(|_| StateCodecError::InvalidPayload)
}

impl VersionedStateCodec<InventoryState> for InventoryCodec {
    fn schema_id(&self) -> &StateSchemaId {
        &self.schema
    }

    fn current_version(&self) -> StateSchemaVersion {
        self.current
    }

    fn encode(&self, value: &InventoryState) -> Result<Vec<u8>, StateCodecError> {
        let mut payload = Map::new();
        payload.insert(String::from("cursor"), json!(value.cursor));
        if let Some(checksum) = &value.source_checksum {
            payload.insert(String::from("source_checksum"), json!(checksum));
        }
        for (name, extension) in &value.extensions {
            payload.insert(name.clone(), json!(extension));
        }
        serde_json::to_vec(&Value::Object(payload)).map_err(|_| StateCodecError::InvalidPayload)
    }

    fn upgrades(&self) -> &[StateSchemaUpgrade] {
        &self.upgrades
    }

    fn decode(&self, payload: &[u8]) -> Result<InventoryState, StateCodecError> {
        let mut value: Map<String, Value> =
            serde_json::from_slice(payload).map_err(|_| StateCodecError::InvalidPayload)?;
        let cursor = value
            .remove("cursor")
            .and_then(|field| field.as_u64())
            .ok_or(StateCodecError::InvalidPayload)?;
        let source_checksum = value
            .remove("source_checksum")
            .map(|field| {
                field
                    .as_str()
                    .map(String::from)
                    .ok_or(StateCodecError::InvalidPayload)
            })
            .transpose()?;
        let extensions = value
            .into_iter()
            .map(|(name, field)| {
                field
                    .as_str()
                    .map(|value| (name, String::from(value)))
                    .ok_or(StateCodecError::InvalidPayload)
            })
            .collect::<Result<_, _>>()?;
        Ok(InventoryState {
            cursor,
            source_checksum,
            extensions,
        })
    }
}

struct ContractCompletion;

impl ChunkCompletion for ContractCompletion {
    fn after_commit<'a>(
        &'a self,
        context: ChunkCompletionContext<'a>,
    ) -> BoxFuture<'a, Result<ChunkCompletionOutcome, ChunkCompletionError>> {
        Box::pin(async move {
            if context.checkpoint().encoded_len() == 0
                || context.execution_context().encoded_len() == 0
                || context.counts().written() != ChunkCount::new(1)
            {
                return Err(ChunkCompletionError::new());
            }
            if context.stop_token().is_stop_requested() {
                Ok(ChunkCompletionOutcome::StoppedAfterCommit)
            } else {
                Ok(ChunkCompletionOutcome::Acknowledged)
            }
        })
    }
}

fn completion_acknowledges_only_committed_evidence() -> Result<(), ComponentContractFailure> {
    const CASE: &str = "chunk_completion_acknowledgement";
    let codec = InventoryCodec::new().map_err(|error| {
        ComponentContractFailure::new(CASE, format!("codec construction failed: {error}"))
    })?;
    let state = InventoryState {
        cursor: 1,
        source_checksum: None,
        extensions: BTreeMap::new(),
    };
    let checkpoint = Checkpoint::encode(&state, &codec, StateLimits::default())
        .map_err(|error| ComponentContractFailure::new(CASE, error.to_string()))?;
    let context = ExecutionContext::encode(&state, &codec, StateLimits::default())
        .map_err(|error| ComponentContractFailure::new(CASE, error.to_string()))?;
    let counts = ChunkCounts::new(
        ChunkCount::new(1),
        ChunkCount::new(1),
        ChunkCount::new(1),
        ChunkCount::ZERO,
    )
    .map_err(|error| ComponentContractFailure::new(CASE, error.to_string()))?;
    let completion: Box<dyn ChunkCompletion> = Box::new(ContractCompletion);
    let (_source, stop) = StopSource::new();
    ensure(
        block_on(completion.after_commit(ChunkCompletionContext::new(
            &checkpoint,
            &context,
            counts,
            &stop,
        ))) == Ok(ChunkCompletionOutcome::Acknowledged),
        CASE,
        "completion callback did not acknowledge committed evidence",
    )?;
    let (source, stopped) = StopSource::new();
    source.request_stop();
    ensure(
        block_on(completion.after_commit(ChunkCompletionContext::new(
            &checkpoint,
            &context,
            counts,
            &stopped,
        ))) == Ok(ChunkCompletionOutcome::StoppedAfterCommit),
        CASE,
        "completion callback did not preserve post-commit stop timing",
    )
}

fn versioned_state_upgrades_old_values_and_enforces_limits() -> Result<(), ComponentContractFailure>
{
    const CASE: &str = "versioned_state_upgrade_and_limits";
    let codec = InventoryCodec::new()
        .map_err(|error| ComponentContractFailure::new(CASE, error.to_string()))?;
    old_state_upgrades_and_round_trips(CASE, &codec)?;
    state_limits_and_newer_versions_are_rejected(CASE, &codec)?;
    state_diagnostics_and_chunk_progress_are_safe(CASE)
}

fn old_state_upgrades_and_round_trips(
    case: &'static str,
    codec: &InventoryCodec,
) -> Result<(), ComponentContractFailure> {
    let old = br#"{
        "format":"oxide-batch.execution-context",
        "format_version":1,
        "schema":"inventory-import",
        "schema_version":1,
        "payload":{"next_index":41,"partition":"north"}
    }"#;
    let context = ExecutionContext::from_json(old, StateLimits::default())
        .map_err(|error| ComponentContractFailure::new(case, error.to_string()))?;
    let decoded = context
        .decode(codec)
        .map_err(|error| ComponentContractFailure::new(case, error.to_string()))?;
    ensure(
        decoded.cursor == 41
            && decoded.source_checksum.is_none()
            && decoded.extensions.get("partition") == Some(&String::from("north")),
        case,
        "old execution context did not follow the explicit upgrade path",
    )?;
    let rewritten = ExecutionContext::encode(&decoded, codec, StateLimits::default())
        .map_err(|error| ComponentContractFailure::new(case, error.to_string()))?;
    ensure(
        rewritten.schema_version()
            == StateSchemaVersion::new(2)
                .map_err(|error| ComponentContractFailure::new(case, error.to_string()))?,
        case,
        "upgraded context was not rewritten at the current schema version",
    )?;
    ensure(
        rewritten
            .decode(codec)
            .map_err(|error| ComponentContractFailure::new(case, error.to_string()))?
            == decoded,
        case,
        "current context did not round-trip",
    )
}

fn state_limits_and_newer_versions_are_rejected(
    case: &'static str,
    codec: &InventoryCodec,
) -> Result<(), ComponentContractFailure> {
    let old = br#"{
        "format":"oxide-batch.execution-context",
        "format_version":1,
        "schema":"inventory-import",
        "schema_version":1,
        "payload":{"next_index":41,"partition":"north"}
    }"#;
    ensure(
        ExecutionContext::from_json(
            old,
            StateLimits::new(32, 16)
                .map_err(|error| ComponentContractFailure::new(case, error.to_string()))?,
        ) == Err(StateError::TooLarge {
            kind: oxide_batch::DurableStateKind::ExecutionContext,
            max_bytes: 32,
        }),
        case,
        "oversized context was not rejected before application decoding",
    )?;
    let deep = br#"{
        "format":"oxide-batch.execution-context",
        "format_version":1,
        "schema":"inventory-import",
        "schema_version":2,
        "payload":{"cursor":1,"one":{"two":{"three":true}}}
    }"#;
    ensure(
        matches!(
            ExecutionContext::from_json(
                deep,
                StateLimits::new(1024, 4)
                    .map_err(|error| ComponentContractFailure::new(case, error.to_string()))?,
            ),
            Err(StateError::TooDeep { .. })
        ),
        case,
        "over-deep context was not rejected before application decoding",
    )?;
    let newer = br#"{
        "format":"oxide-batch.execution-context",
        "format_version":1,
        "schema":"inventory-import",
        "schema_version":3,
        "payload":{"cursor":1}
    }"#;
    let newer = ExecutionContext::from_json(newer, StateLimits::default())
        .map_err(|error| ComponentContractFailure::new(case, error.to_string()))?;
    ensure(
        matches!(
            newer.decode(codec),
            Err(StateError::UnsupportedSchemaVersion { .. })
        ),
        case,
        "newer application context was not rejected",
    )
}

fn state_diagnostics_and_chunk_progress_are_safe(
    case: &'static str,
) -> Result<(), ComponentContractFailure> {
    let sentinel = "sentinel-context-value";
    let corrupted = format!("{{\"payload\":\"{sentinel}\"");
    let error = ExecutionContext::from_json(corrupted.as_bytes(), StateLimits::default())
        .err()
        .ok_or_else(|| {
            ComponentContractFailure::new(case, "corrupted context unexpectedly decoded")
        })?;
    let diagnostics = format!("{error:?}\n{error}");
    ensure(
        !diagnostics.contains(sentinel),
        case,
        "context failure diagnostics exposed payload data",
    )?;

    let size = ChunkSize::new(2)
        .map_err(|error| ComponentContractFailure::new(case, error.to_string()))?;
    let mut progress = oxide_batch::ChunkProgress::new(size);
    progress
        .record_read()
        .and_then(|()| progress.record_processed())
        .and_then(|()| progress.record_written(ChunkCount::new(1)))
        .map_err(|error| ComponentContractFailure::new(case, error.to_string()))?;
    ensure(
        progress.counts().written() == ChunkCount::new(1),
        case,
        "checked chunk progress lost its valid counts",
    )
}

fn ensure(
    condition: bool,
    case: &'static str,
    detail: &'static str,
) -> Result<(), ComponentContractFailure> {
    if condition {
        Ok(())
    } else {
        Err(ComponentContractFailure::new(case, detail))
    }
}

/// Safe diagnostic from a shared component contract case.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComponentContractFailure {
    case: &'static str,
    detail: String,
}

impl ComponentContractFailure {
    fn new(case: &'static str, detail: impl Into<String>) -> Self {
        Self {
            case,
            detail: detail.into(),
        }
    }
}

impl fmt::Display for ComponentContractFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "component_contract={} detail={}",
            self.case, self.detail
        )
    }
}

impl Error for ComponentContractFailure {}
