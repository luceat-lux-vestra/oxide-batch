//! Bounded, keyset-paginated, redacted metadata inspection.
//!
//! The explorer owns a closed query set. Aggregation, arbitrary predicates,
//! caller-supplied ordering, and any filter over parameter, context, or
//! checkpoint content are deliberately absent. Every projection is redacted by
//! construction: a projection that cannot be produced without a prohibited
//! value fails rather than degrading.

use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use sha2::{Digest, Sha256};

use super::{CanonicalWriter, OperatorRecord, hex_digest};
use crate::{
    BatchStatus, BoxFuture, DefinitionRevision, DurableStateKind, ExecutionCounts,
    ExecutionTimestamps, ExecutionVersion, ExitStatus, FailureSummary, FlowDecision,
    JobExecutionId, JobInstanceId, JobName, NodeId, ParameterName, ParameterValueKind,
    RecoveryDecision, RepositoryError, StateSchemaId, StateSchemaVersion, StepExecutionId,
    StepName, StepPartitionId, TelemetryEventSink, TelemetryRecord,
};

/// Maximum rows one page may contain.
pub const MAX_PAGE_SIZE: u16 = 500;
/// Page size used when a caller does not choose one.
pub const DEFAULT_PAGE_SIZE: u16 = 50;
/// Maximum estimated encoded size of one page.
pub const MAX_RESPONSE_BYTES: usize = 256 * 1024;
/// Maximum size of one opaque cursor token.
pub const MAX_CURSOR_BYTES: usize = 256;
/// Smallest age bound accepted by the unresolved-execution query.
pub const MIN_UNRESOLVED_AGE: Duration = Duration::from_mins(1);

const CURSOR_FORMAT_VERSION: u8 = 1;
const MAX_CURSOR_NAME_BYTES: usize = 128;
const KEY_TAG_IDENTITY: u8 = 1;
const KEY_TAG_ORDERED: u8 = 2;
const KEY_TAG_NAME: u8 = 3;
const BINDING_BYTES: usize = 8;

/// A validated page size in `1..=500`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PageSize(u16);

impl PageSize {
    /// Validates a caller-supplied page size.
    ///
    /// # Errors
    ///
    /// Returns [`ExplorerError::PageSizeOutOfRange`] outside `1..=500`.
    pub const fn new(value: u16) -> Result<Self, ExplorerError> {
        if value == 0 || value > MAX_PAGE_SIZE {
            return Err(ExplorerError::PageSizeOutOfRange { requested: value });
        }
        Ok(Self(value))
    }

    /// Returns the validated row bound.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0
    }
}

impl Default for PageSize {
    fn default() -> Self {
        Self(DEFAULT_PAGE_SIZE)
    }
}

/// One bounded page request.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PageRequest {
    size: PageSize,
    cursor: Option<Cursor>,
}

impl PageRequest {
    /// Requests the first page of a traversal.
    #[must_use]
    pub const fn first(size: PageSize) -> Self {
        Self { size, cursor: None }
    }

    /// Requests the page that continues an existing traversal.
    #[must_use]
    pub const fn resume(size: PageSize, cursor: Cursor) -> Self {
        Self {
            size,
            cursor: Some(cursor),
        }
    }

    /// Returns the requested row bound.
    #[must_use]
    pub const fn size(&self) -> PageSize {
        self.size
    }

    /// Borrows the continuation cursor, when this is not the first page.
    #[must_use]
    pub const fn cursor(&self) -> Option<&Cursor> {
        self.cursor.as_ref()
    }
}

/// An opaque keyset continuation token.
///
/// The encoding is not a documented format and confers no authority. A token
/// presented to a different query, different filters, or a different page size
/// is rejected rather than reinterpreted.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Cursor(Vec<u8>);

impl Cursor {
    /// Reconstructs a cursor from its opaque bytes.
    ///
    /// # Errors
    ///
    /// Returns [`CursorError::CursorInvalid`] when the token is empty or
    /// exceeds [`MAX_CURSOR_BYTES`].
    pub fn from_bytes(value: impl Into<Vec<u8>>) -> Result<Self, CursorError> {
        let value = value.into();
        if value.is_empty() || value.len() > MAX_CURSOR_BYTES {
            return Err(CursorError::CursorInvalid);
        }
        Ok(Self(value))
    }

    /// Reconstructs a cursor from its lowercase hexadecimal text form.
    ///
    /// # Errors
    ///
    /// Returns [`CursorError::CursorInvalid`] when the text is not an even
    /// number of hexadecimal digits within the token bound.
    pub fn from_hex(value: &str) -> Result<Self, CursorError> {
        if !value.len().is_multiple_of(2) {
            return Err(CursorError::CursorInvalid);
        }
        let mut bytes = Vec::with_capacity(value.len() / 2);
        let raw = value.as_bytes();
        for pair in raw.chunks_exact(2) {
            let high = hex_value(pair[0]).ok_or(CursorError::CursorInvalid)?;
            let low = hex_value(pair[1]).ok_or(CursorError::CursorInvalid)?;
            bytes.push((high << 4) | low);
        }
        Self::from_bytes(bytes)
    }

    /// Returns the opaque token bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

impl fmt::Debug for Cursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Cursor")
            .field("bytes", &self.0.len())
            .finish()
    }
}

impl fmt::Display for Cursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&hex_digest(&self.0))
    }
}

/// One bounded page and its continuation token.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Page<T> {
    rows: Vec<T>,
    next: Option<Cursor>,
}

impl<T> Page<T> {
    pub(crate) const fn new(rows: Vec<T>, next: Option<Cursor>) -> Self {
        Self { rows, next }
    }

    /// Borrows the rows of this page.
    #[must_use]
    pub fn rows(&self) -> &[T] {
        &self.rows
    }

    /// Consumes the page and returns its rows.
    #[must_use]
    pub fn into_rows(self) -> Vec<T> {
        self.rows
    }

    /// Borrows the token that continues this traversal, when more may remain.
    #[must_use]
    pub const fn next_cursor(&self) -> Option<&Cursor> {
        self.next.as_ref()
    }
}

/// The closed set of paginated explorer queries.
///
/// `get_execution` is the one named query that returns a single projection and
/// therefore takes no cursor.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ExplorerQuery {
    /// Registered job names in byte order.
    JobNames,
    /// Instances of one job name, newest identity first.
    Instances {
        /// Filtered job name.
        job_name: JobName,
    },
    /// Executions of one instance, newest attempt first.
    Executions {
        /// Filtered logical instance.
        job_instance_id: JobInstanceId,
    },
    /// Step executions of one job execution.
    StepExecutions {
        /// Filtered job execution.
        job_execution_id: JobExecutionId,
    },
    /// Non-terminal executions older than a bounded age.
    UnresolvedExecutions {
        /// Minimum durable age, at least [`MIN_UNRESOLVED_AGE`].
        minimum_age: Duration,
    },
    /// Recovery decisions of one job execution.
    RecoveryDecisions {
        /// Filtered job execution.
        job_execution_id: JobExecutionId,
    },
    /// Flow decisions of one job execution in sequence order.
    FlowDecisions {
        /// Filtered job execution.
        job_execution_id: JobExecutionId,
    },
    /// Partitions of one partitioned step execution.
    StepPartitions {
        /// Filtered parent step execution.
        step_execution_id: StepExecutionId,
    },
    /// Audited operator requests for one job execution.
    OperatorRequests {
        /// Filtered job execution.
        job_execution_id: JobExecutionId,
    },
}

impl ExplorerQuery {
    const fn discriminant(&self) -> u8 {
        match self {
            Self::JobNames => 1,
            Self::Instances { .. } => 2,
            Self::Executions { .. } => 3,
            Self::StepExecutions { .. } => 4,
            Self::UnresolvedExecutions { .. } => 5,
            Self::RecoveryDecisions { .. } => 6,
            Self::FlowDecisions { .. } => 7,
            Self::StepPartitions { .. } => 8,
            Self::OperatorRequests { .. } => 9,
        }
    }

    /// Returns the stable name of the query for diagnostics and telemetry.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::JobNames => "list_job_names",
            Self::Instances { .. } => "list_instances",
            Self::Executions { .. } => "list_executions",
            Self::StepExecutions { .. } => "list_step_executions",
            Self::UnresolvedExecutions { .. } => "list_unresolved_executions",
            Self::RecoveryDecisions { .. } => "list_recovery_decisions",
            Self::FlowDecisions { .. } => "list_flow_decisions",
            Self::StepPartitions { .. } => "list_step_partitions",
            Self::OperatorRequests { .. } => "list_operator_requests",
        }
    }

    fn identity(&self, size: PageSize) -> [u8; 32] {
        let mut writer = CanonicalWriter::new("oxide-batch.explorer-query.v1");
        writer.push_str(self.name());
        writer.push_u64(u64::from(size.get()));
        match self {
            Self::JobNames => writer.push_str(""),
            Self::Instances { job_name } => writer.push_str(job_name.as_str()),
            Self::Executions { job_instance_id } => writer.push_u64(job_instance_id.get()),
            Self::StepExecutions { job_execution_id }
            | Self::RecoveryDecisions { job_execution_id }
            | Self::FlowDecisions { job_execution_id }
            | Self::OperatorRequests { job_execution_id } => {
                writer.push_u64(job_execution_id.get());
            }
            Self::UnresolvedExecutions { minimum_age } => {
                writer.push_u64(minimum_age.as_secs());
            }
            Self::StepPartitions { step_execution_id } => writer.push_u64(step_execution_id.get()),
        }
        writer.digest()
    }
}

/// The immutable ordering key of the last row returned by a page.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CursorKey {
    /// A single immutable identity column.
    Identity(u64),
    /// An immutable ordinal paired with its identity column.
    Ordered {
        /// Immutable primary ordinal, such as an attempt or sequence.
        primary: u64,
        /// Identity tiebreaker.
        identity: u64,
    },
    /// An immutable byte-ordered name column.
    Name(String),
}

impl CursorKey {
    fn encode(&self, target: &mut Vec<u8>) -> Result<(), CursorError> {
        match self {
            Self::Identity(value) => {
                target.push(KEY_TAG_IDENTITY);
                target.extend_from_slice(&value.to_be_bytes());
            }
            Self::Ordered { primary, identity } => {
                target.push(KEY_TAG_ORDERED);
                target.extend_from_slice(&primary.to_be_bytes());
                target.extend_from_slice(&identity.to_be_bytes());
            }
            Self::Name(value) => {
                if value.len() > MAX_CURSOR_NAME_BYTES {
                    return Err(CursorError::CursorInvalid);
                }
                target.push(KEY_TAG_NAME);
                let length = u8::try_from(value.len()).map_err(|_| CursorError::CursorInvalid)?;
                target.push(length);
                target.extend_from_slice(value.as_bytes());
            }
        }
        Ok(())
    }

    fn decode(bytes: &[u8]) -> Result<(Self, &[u8]), CursorError> {
        let (tag, rest) = bytes.split_first().ok_or(CursorError::CursorInvalid)?;
        match *tag {
            KEY_TAG_IDENTITY => {
                let (value, rest) = read_u64(rest)?;
                Ok((Self::Identity(value), rest))
            }
            KEY_TAG_ORDERED => {
                let (primary, rest) = read_u64(rest)?;
                let (identity, rest) = read_u64(rest)?;
                Ok((Self::Ordered { primary, identity }, rest))
            }
            KEY_TAG_NAME => {
                let (length, rest) = rest.split_first().ok_or(CursorError::CursorInvalid)?;
                let length = usize::from(*length);
                if rest.len() < length {
                    return Err(CursorError::CursorInvalid);
                }
                let (value, rest) = rest.split_at(length);
                let value = core::str::from_utf8(value).map_err(|_| CursorError::CursorInvalid)?;
                Ok((Self::Name(value.to_owned()), rest))
            }
            _ => Err(CursorError::CursorInvalid),
        }
    }
}

fn read_u64(bytes: &[u8]) -> Result<(u64, &[u8]), CursorError> {
    if bytes.len() < 8 {
        return Err(CursorError::CursorInvalid);
    }
    let (head, rest) = bytes.split_at(8);
    let mut value = [0_u8; 8];
    value.copy_from_slice(head);
    Ok((u64::from_be_bytes(value), rest))
}

/// The bounded keyset window one adapter statement must honour.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryWindow {
    after: Option<CursorKey>,
    ceiling: u64,
    limit: u16,
}

impl QueryWindow {
    pub(crate) const fn new(after: Option<CursorKey>, ceiling: u64, limit: u16) -> Self {
        Self {
            after,
            ceiling,
            limit,
        }
    }

    /// Borrows the exclusive ordering key of the previous page, when present.
    #[must_use]
    pub const fn after(&self) -> Option<&CursorKey> {
        self.after.as_ref()
    }

    /// Returns the inclusive identity ceiling captured by the traversal.
    ///
    /// A row whose identity exceeds this ceiling was created after the
    /// traversal started and is never returned by it.
    #[must_use]
    pub const fn ceiling(&self) -> u64 {
        self.ceiling
    }

    /// Returns the maximum number of rows the statement may return.
    #[must_use]
    pub const fn limit(&self) -> u16 {
        self.limit
    }
}

/// A redacted description of one job parameter.
///
/// The descriptor carries the parameter name, its type tag, and whether it
/// participates in instance identity. Values never appear.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParameterDescriptor {
    name: ParameterName,
    kind: ParameterValueKind,
    identifying: bool,
}

impl ParameterDescriptor {
    pub(crate) const fn new(
        name: ParameterName,
        kind: ParameterValueKind,
        identifying: bool,
    ) -> Self {
        Self {
            name,
            kind,
            identifying,
        }
    }

    /// Borrows the parameter name.
    #[must_use]
    pub const fn name(&self) -> &ParameterName {
        &self.name
    }

    /// Returns the parameter type tag.
    #[must_use]
    pub const fn kind(&self) -> ParameterValueKind {
        self.kind
    }

    /// Returns whether the parameter participates in instance identity.
    #[must_use]
    pub const fn is_identifying(&self) -> bool {
        self.identifying
    }
}

/// A redacted description of one durable state envelope.
///
/// Presence, format, schema, schema version, and encoded size are observable;
/// the payload is not.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateEnvelopeDescriptor {
    kind: DurableStateKind,
    format_version: u16,
    schema_id: StateSchemaId,
    schema_version: StateSchemaVersion,
    encoded_len: usize,
}

impl StateEnvelopeDescriptor {
    // Durable adapters and the in-memory partition reference retain only this
    // redacted envelope description at the explorer boundary.
    pub(crate) const fn new(
        kind: DurableStateKind,
        format_version: u16,
        schema_id: StateSchemaId,
        schema_version: StateSchemaVersion,
        encoded_len: usize,
    ) -> Self {
        Self {
            kind,
            format_version,
            schema_id,
            schema_version,
            encoded_len,
        }
    }

    /// Returns the durable state category.
    #[must_use]
    pub const fn kind(&self) -> DurableStateKind {
        self.kind
    }

    /// Returns the envelope format version.
    #[must_use]
    pub const fn format_version(&self) -> u16 {
        self.format_version
    }

    /// Borrows the application-owned schema identifier.
    #[must_use]
    pub const fn schema_id(&self) -> &StateSchemaId {
        &self.schema_id
    }

    /// Returns the application-owned schema version.
    #[must_use]
    pub const fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }

    /// Returns the encoded payload size in bytes.
    #[must_use]
    pub const fn encoded_len(&self) -> usize {
        self.encoded_len
    }
}

/// A redacted description of the definition bound to one execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DefinitionDescriptor {
    revision: DefinitionRevision,
    manifest_format: u16,
    manifest_digest: [u8; 32],
}

impl DefinitionDescriptor {
    pub(crate) const fn new(
        revision: DefinitionRevision,
        manifest_format: u16,
        manifest_digest: [u8; 32],
    ) -> Self {
        Self {
            revision,
            manifest_format,
            manifest_digest,
        }
    }

    /// Borrows the application-owned definition revision.
    #[must_use]
    pub const fn revision(&self) -> &DefinitionRevision {
        &self.revision
    }

    /// Returns the manifest format version.
    #[must_use]
    pub const fn manifest_format(&self) -> u16 {
        self.manifest_format
    }

    /// Returns the manifest digest.
    #[must_use]
    pub const fn manifest_digest(&self) -> &[u8; 32] {
        &self.manifest_digest
    }

    /// Returns the hexadecimal manifest digest.
    #[must_use]
    pub fn manifest_digest_hex(&self) -> String {
        hex_digest(&self.manifest_digest)
    }
}

/// A redacted logical job instance projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobInstanceProjection {
    id: JobInstanceId,
    job_name: JobName,
    instance_key_digest: [u8; 32],
    parameters: Vec<ParameterDescriptor>,
    created_at: Option<SystemTime>,
    hold: Option<super::RetentionHold>,
}

impl JobInstanceProjection {
    pub(crate) const fn new(
        id: JobInstanceId,
        job_name: JobName,
        instance_key_digest: [u8; 32],
        parameters: Vec<ParameterDescriptor>,
        created_at: Option<SystemTime>,
        hold: Option<super::RetentionHold>,
    ) -> Self {
        Self {
            id,
            job_name,
            instance_key_digest,
            parameters,
            created_at,
            hold,
        }
    }

    /// Returns the opaque instance identifier.
    #[must_use]
    pub const fn id(&self) -> JobInstanceId {
        self.id
    }

    /// Borrows the job name.
    #[must_use]
    pub const fn job_name(&self) -> &JobName {
        &self.job_name
    }

    /// Returns the canonical identifying-key digest.
    #[must_use]
    pub const fn instance_key_digest(&self) -> &[u8; 32] {
        &self.instance_key_digest
    }

    /// Returns the hexadecimal identifying-key digest.
    #[must_use]
    pub fn instance_key_digest_hex(&self) -> String {
        hex_digest(&self.instance_key_digest)
    }

    /// Borrows the redacted parameter descriptors.
    #[must_use]
    pub fn parameters(&self) -> &[ParameterDescriptor] {
        &self.parameters
    }

    /// Returns the durable creation instant when the adapter records one.
    #[must_use]
    pub const fn created_at(&self) -> Option<SystemTime> {
        self.created_at
    }

    /// Borrows the active retention hold, when one is placed.
    #[must_use]
    pub const fn hold(&self) -> Option<&super::RetentionHold> {
        self.hold.as_ref()
    }
}

/// A redacted job execution projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobExecutionProjection {
    id: JobExecutionId,
    job_instance_id: JobInstanceId,
    job_name: JobName,
    attempt: u32,
    status: BatchStatus,
    exit_status: ExitStatus,
    counts: ExecutionCounts,
    version: ExecutionVersion,
    timestamps: ExecutionTimestamps,
    updated_at: SystemTime,
    failure: Option<FailureSummary>,
    definition: Option<DefinitionDescriptor>,
    context: Option<StateEnvelopeDescriptor>,
    stop_requested_at: Option<SystemTime>,
    owner_recorded: bool,
}

impl JobExecutionProjection {
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn new(
        id: JobExecutionId,
        job_instance_id: JobInstanceId,
        job_name: JobName,
        attempt: u32,
        status: BatchStatus,
        exit_status: ExitStatus,
        counts: ExecutionCounts,
        version: ExecutionVersion,
        timestamps: ExecutionTimestamps,
        updated_at: SystemTime,
        failure: Option<FailureSummary>,
        definition: Option<DefinitionDescriptor>,
        context: Option<StateEnvelopeDescriptor>,
        stop_requested_at: Option<SystemTime>,
        owner_recorded: bool,
    ) -> Self {
        Self {
            id,
            job_instance_id,
            job_name,
            attempt,
            status,
            exit_status,
            counts,
            version,
            timestamps,
            updated_at,
            failure,
            definition,
            context,
            stop_requested_at,
            owner_recorded,
        }
    }

    /// Returns the opaque execution identifier.
    #[must_use]
    pub const fn id(&self) -> JobExecutionId {
        self.id
    }

    /// Returns the owning logical instance.
    #[must_use]
    pub const fn job_instance_id(&self) -> JobInstanceId {
        self.job_instance_id
    }

    /// Borrows the job name.
    #[must_use]
    pub const fn job_name(&self) -> &JobName {
        &self.job_name
    }

    /// Returns the attempt ordinal within the logical instance.
    #[must_use]
    pub const fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Returns the framework status.
    #[must_use]
    pub const fn status(&self) -> BatchStatus {
        self.status
    }

    /// Borrows the operator-facing exit status.
    #[must_use]
    pub const fn exit_status(&self) -> &ExitStatus {
        &self.exit_status
    }

    /// Returns the durable counters.
    #[must_use]
    pub const fn counts(&self) -> ExecutionCounts {
        self.counts
    }

    /// Returns the observed optimistic version.
    #[must_use]
    pub const fn version(&self) -> ExecutionVersion {
        self.version
    }

    /// Returns the lifecycle timestamps.
    #[must_use]
    pub const fn timestamps(&self) -> ExecutionTimestamps {
        self.timestamps
    }

    /// Returns the durable last-update instant.
    #[must_use]
    pub const fn updated_at(&self) -> SystemTime {
        self.updated_at
    }

    /// Returns the framework failure category and opaque failure identifier.
    #[must_use]
    pub const fn failure(&self) -> Option<FailureSummary> {
        self.failure
    }

    /// Borrows the definition descriptor when the adapter records one.
    #[must_use]
    pub const fn definition(&self) -> Option<&DefinitionDescriptor> {
        self.definition.as_ref()
    }

    /// Borrows the execution-context envelope description.
    #[must_use]
    pub const fn context(&self) -> Option<&StateEnvelopeDescriptor> {
        self.context.as_ref()
    }

    /// Returns the durable stop-request instant, when a stop was recorded.
    #[must_use]
    pub const fn stop_requested_at(&self) -> Option<SystemTime> {
        self.stop_requested_at
    }

    /// Returns whether a process recorded ownership of this execution.
    ///
    /// Ownership is evidence only. It is not a lease and never authorizes a
    /// takeover.
    #[must_use]
    pub const fn owner_recorded(&self) -> bool {
        self.owner_recorded
    }
}

/// A redacted step execution projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StepExecutionProjection {
    id: StepExecutionId,
    job_execution_id: JobExecutionId,
    step_name: StepName,
    node_id: Option<NodeId>,
    status: BatchStatus,
    exit_status: ExitStatus,
    counts: ExecutionCounts,
    version: ExecutionVersion,
    timestamps: ExecutionTimestamps,
    failure: Option<FailureSummary>,
    checkpoint: Option<StateEnvelopeDescriptor>,
    context: Option<StateEnvelopeDescriptor>,
}

impl StepExecutionProjection {
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn new(
        id: StepExecutionId,
        job_execution_id: JobExecutionId,
        step_name: StepName,
        node_id: Option<NodeId>,
        status: BatchStatus,
        exit_status: ExitStatus,
        counts: ExecutionCounts,
        version: ExecutionVersion,
        timestamps: ExecutionTimestamps,
        failure: Option<FailureSummary>,
        checkpoint: Option<StateEnvelopeDescriptor>,
        context: Option<StateEnvelopeDescriptor>,
    ) -> Self {
        Self {
            id,
            job_execution_id,
            step_name,
            node_id,
            status,
            exit_status,
            counts,
            version,
            timestamps,
            failure,
            checkpoint,
            context,
        }
    }

    /// Returns the opaque step-execution identifier.
    #[must_use]
    pub const fn id(&self) -> StepExecutionId {
        self.id
    }

    /// Returns the owning job execution.
    #[must_use]
    pub const fn job_execution_id(&self) -> JobExecutionId {
        self.job_execution_id
    }

    /// Borrows the durable step name.
    #[must_use]
    pub const fn step_name(&self) -> &StepName {
        &self.step_name
    }

    /// Borrows the stable logical node identifier, when the adapter records one.
    #[must_use]
    pub const fn node_id(&self) -> Option<&NodeId> {
        self.node_id.as_ref()
    }

    /// Returns the framework status.
    #[must_use]
    pub const fn status(&self) -> BatchStatus {
        self.status
    }

    /// Borrows the operator-facing exit status.
    #[must_use]
    pub const fn exit_status(&self) -> &ExitStatus {
        &self.exit_status
    }

    /// Returns the durable counters.
    #[must_use]
    pub const fn counts(&self) -> ExecutionCounts {
        self.counts
    }

    /// Returns the observed optimistic version.
    #[must_use]
    pub const fn version(&self) -> ExecutionVersion {
        self.version
    }

    /// Returns the lifecycle timestamps.
    #[must_use]
    pub const fn timestamps(&self) -> ExecutionTimestamps {
        self.timestamps
    }

    /// Returns the framework failure category and opaque failure identifier.
    #[must_use]
    pub const fn failure(&self) -> Option<FailureSummary> {
        self.failure
    }

    /// Borrows the checkpoint envelope description.
    #[must_use]
    pub const fn checkpoint(&self) -> Option<&StateEnvelopeDescriptor> {
        self.checkpoint.as_ref()
    }

    /// Borrows the step-context envelope description.
    #[must_use]
    pub const fn context(&self) -> Option<&StateEnvelopeDescriptor> {
        self.context.as_ref()
    }
}

/// A redacted durable partition projection.
///
/// Payloads remain hidden while plan identity, lifecycle, counters, worker
/// assignment, and context schema metadata stay inspectable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StepPartitionProjection {
    id: StepPartitionId,
    step_execution_id: StepExecutionId,
    partition_key: String,
    ordinal: u32,
    status: BatchStatus,
    exit_status: ExitStatus,
    counts: ExecutionCounts,
    version: ExecutionVersion,
    worker_step_execution_id: Option<StepExecutionId>,
    context: Option<StateEnvelopeDescriptor>,
}

impl StepPartitionProjection {
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn new(
        id: StepPartitionId,
        step_execution_id: StepExecutionId,
        partition_key: String,
        ordinal: u32,
        status: BatchStatus,
        exit_status: ExitStatus,
        counts: ExecutionCounts,
        version: ExecutionVersion,
        worker_step_execution_id: Option<StepExecutionId>,
        context: Option<StateEnvelopeDescriptor>,
    ) -> Self {
        Self {
            id,
            step_execution_id,
            partition_key,
            ordinal,
            status,
            exit_status,
            counts,
            version,
            worker_step_execution_id,
            context,
        }
    }

    /// Returns the opaque partition row identifier.
    #[must_use]
    pub const fn id(&self) -> StepPartitionId {
        self.id
    }

    /// Returns the parent partitioned step execution.
    #[must_use]
    pub const fn step_execution_id(&self) -> StepExecutionId {
        self.step_execution_id
    }

    /// Borrows the immutable partition key.
    #[must_use]
    pub fn partition_key(&self) -> &str {
        &self.partition_key
    }

    /// Returns the partition ordinal within its plan.
    #[must_use]
    pub const fn ordinal(&self) -> u32 {
        self.ordinal
    }

    /// Returns the framework status.
    #[must_use]
    pub const fn status(&self) -> BatchStatus {
        self.status
    }

    /// Borrows the operator-facing exit status.
    #[must_use]
    pub const fn exit_status(&self) -> &ExitStatus {
        &self.exit_status
    }

    /// Returns the durable counters.
    #[must_use]
    pub const fn counts(&self) -> ExecutionCounts {
        self.counts
    }

    /// Returns the observed optimistic version.
    #[must_use]
    pub const fn version(&self) -> ExecutionVersion {
        self.version
    }

    /// Returns the worker step execution that owns this partition.
    #[must_use]
    pub const fn worker_step_execution_id(&self) -> Option<StepExecutionId> {
        self.worker_step_execution_id
    }

    /// Borrows the partition-context envelope description.
    #[must_use]
    pub const fn context(&self) -> Option<&StateEnvelopeDescriptor> {
        self.context.as_ref()
    }
}

/// A bounded read port one metadata adapter implements.
///
/// Every method executes one statement under the adapter's ordinary read
/// committed isolation, returns at most [`QueryWindow::limit`] rows, and takes
/// no lock. Cross-page snapshot isolation is not provided.
pub trait ExplorerRepository: Send + Sync {
    /// Captures the exclusive identity ceiling for one traversal.
    fn identity_ceiling<'a>(
        &'a self,
        query: &'a ExplorerQuery,
    ) -> BoxFuture<'a, Result<u64, ExplorerError>>;

    /// Reads registered job names in byte order.
    fn job_names<'a>(
        &'a self,
        window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<JobName>, ExplorerError>>;

    /// Reads instances of one job name, newest identity first.
    fn instances<'a>(
        &'a self,
        job_name: &'a JobName,
        window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<JobInstanceProjection>, ExplorerError>>;

    /// Reads executions of one instance, newest attempt first.
    fn executions<'a>(
        &'a self,
        job_instance_id: JobInstanceId,
        window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<JobExecutionProjection>, ExplorerError>>;

    /// Reads one execution projection.
    fn execution(
        &self,
        job_execution_id: JobExecutionId,
    ) -> BoxFuture<'_, Result<Option<JobExecutionProjection>, ExplorerError>>;

    /// Reads step executions of one job execution.
    fn step_executions<'a>(
        &'a self,
        job_execution_id: JobExecutionId,
        window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<StepExecutionProjection>, ExplorerError>>;

    /// Reads non-terminal executions older than `minimum_age`.
    fn unresolved_executions<'a>(
        &'a self,
        minimum_age: Duration,
        window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<JobExecutionProjection>, ExplorerError>>;

    /// Reads recovery decisions of one job execution.
    fn recovery_decisions<'a>(
        &'a self,
        job_execution_id: JobExecutionId,
        window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<RecoveryDecision>, ExplorerError>>;

    /// Reads flow decisions of one job execution in sequence order.
    fn flow_decisions<'a>(
        &'a self,
        job_execution_id: JobExecutionId,
        window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<FlowDecision>, ExplorerError>>;

    /// Reads partitions of one partitioned step execution.
    fn step_partitions<'a>(
        &'a self,
        step_execution_id: StepExecutionId,
        window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<StepPartitionProjection>, ExplorerError>>;

    /// Reads audited operator requests for one job execution.
    fn operator_requests<'a>(
        &'a self,
        job_execution_id: JobExecutionId,
        window: &'a QueryWindow,
    ) -> BoxFuture<'a, Result<Vec<OperatorRecord>, ExplorerError>>;
}

/// The portable bounded inspection service.
///
/// The service owns page bounds, cursor identity, traversal ceilings, and the
/// encoded response bound. The adapter owns one statement per page.
#[derive(Clone)]
pub struct JobExplorer<S> {
    source: S,
    event_sinks: Vec<Arc<dyn TelemetryEventSink>>,
}

impl<S: fmt::Debug> fmt::Debug for JobExplorer<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JobExplorer")
            .field("source", &self.source)
            .field("event_sinks", &self.event_sinks.len())
            .finish()
    }
}

impl<S: ExplorerRepository> JobExplorer<S> {
    /// Wraps one bounded read port.
    pub const fn new(source: S) -> Self {
        Self {
            source,
            event_sinks: Vec::new(),
        }
    }

    /// Attaches a non-authoritative, panic-isolated telemetry sink.
    #[must_use]
    pub fn with_event_sink(mut self, sink: Arc<dyn TelemetryEventSink>) -> Self {
        self.event_sinks.push(sink);
        self
    }

    /// Borrows the underlying read port.
    pub const fn source(&self) -> &S {
        &self.source
    }

    /// Lists registered job names in byte order.
    ///
    /// # Errors
    ///
    /// Returns a typed cursor, bound, timeout, or repository failure.
    pub async fn list_job_names(
        &self,
        request: &PageRequest,
    ) -> Result<Page<JobName>, ExplorerError> {
        let query = ExplorerQuery::JobNames;
        let window = self.window(&query, request).await?;
        let rows = self.source.job_names(&window).await?;
        self.finish_page(None, page(&query, request, window.ceiling(), rows))
    }

    /// Lists instances of one job name, newest identity first.
    ///
    /// # Errors
    ///
    /// Returns a typed cursor, bound, timeout, or repository failure.
    pub async fn list_instances(
        &self,
        job_name: &JobName,
        request: &PageRequest,
    ) -> Result<Page<JobInstanceProjection>, ExplorerError> {
        let query = ExplorerQuery::Instances {
            job_name: job_name.clone(),
        };
        let window = self.window(&query, request).await?;
        let rows = self.source.instances(job_name, &window).await?;
        self.finish_page(None, page(&query, request, window.ceiling(), rows))
    }

    /// Lists executions of one instance, newest attempt first.
    ///
    /// # Errors
    ///
    /// Returns a typed cursor, bound, timeout, or repository failure.
    pub async fn list_executions(
        &self,
        job_instance_id: JobInstanceId,
        request: &PageRequest,
    ) -> Result<Page<JobExecutionProjection>, ExplorerError> {
        let query = ExplorerQuery::Executions { job_instance_id };
        let window = self.window(&query, request).await?;
        let rows = self.source.executions(job_instance_id, &window).await?;
        self.finish_page(None, page(&query, request, window.ceiling(), rows))
    }

    /// Reads one execution projection.
    ///
    /// # Errors
    ///
    /// Returns a typed timeout or repository failure.
    pub async fn get_execution(
        &self,
        job_execution_id: JobExecutionId,
    ) -> Result<Option<JobExecutionProjection>, ExplorerError> {
        self.source.execution(job_execution_id).await
    }

    /// Lists step executions of one job execution.
    ///
    /// # Errors
    ///
    /// Returns a typed cursor, bound, timeout, or repository failure.
    pub async fn list_step_executions(
        &self,
        job_execution_id: JobExecutionId,
        request: &PageRequest,
    ) -> Result<Page<StepExecutionProjection>, ExplorerError> {
        let query = ExplorerQuery::StepExecutions { job_execution_id };
        let window = self.window(&query, request).await?;
        let rows = self
            .source
            .step_executions(job_execution_id, &window)
            .await?;
        self.finish_page(
            Some(job_execution_id),
            page(&query, request, window.ceiling(), rows),
        )
    }

    /// Lists non-terminal executions older than an explicit age bound.
    ///
    /// # Errors
    ///
    /// Returns [`ExplorerError::AgeBoundTooSmall`] below [`MIN_UNRESOLVED_AGE`],
    /// or a typed cursor, bound, timeout, or repository failure.
    pub async fn list_unresolved_executions(
        &self,
        minimum_age: Duration,
        request: &PageRequest,
    ) -> Result<Page<JobExecutionProjection>, ExplorerError> {
        if minimum_age < MIN_UNRESOLVED_AGE {
            return Err(ExplorerError::AgeBoundTooSmall {
                minimum: MIN_UNRESOLVED_AGE,
            });
        }
        let query = ExplorerQuery::UnresolvedExecutions { minimum_age };
        let window = self.window(&query, request).await?;
        let rows = self
            .source
            .unresolved_executions(minimum_age, &window)
            .await?;
        self.finish_page(None, page(&query, request, window.ceiling(), rows))
    }

    /// Lists recovery decisions of one job execution.
    ///
    /// # Errors
    ///
    /// Returns a typed cursor, bound, timeout, or repository failure.
    pub async fn list_recovery_decisions(
        &self,
        job_execution_id: JobExecutionId,
        request: &PageRequest,
    ) -> Result<Page<RecoveryDecision>, ExplorerError> {
        let query = ExplorerQuery::RecoveryDecisions { job_execution_id };
        let window = self.window(&query, request).await?;
        let rows = self
            .source
            .recovery_decisions(job_execution_id, &window)
            .await?;
        self.finish_page(
            Some(job_execution_id),
            page(&query, request, window.ceiling(), rows),
        )
    }

    /// Lists flow decisions of one job execution in sequence order.
    ///
    /// # Errors
    ///
    /// Returns a typed cursor, bound, timeout, or repository failure.
    pub async fn list_flow_decisions(
        &self,
        job_execution_id: JobExecutionId,
        request: &PageRequest,
    ) -> Result<Page<FlowDecision>, ExplorerError> {
        let query = ExplorerQuery::FlowDecisions { job_execution_id };
        let window = self.window(&query, request).await?;
        let rows = self
            .source
            .flow_decisions(job_execution_id, &window)
            .await?;
        self.finish_page(
            Some(job_execution_id),
            page(&query, request, window.ceiling(), rows),
        )
    }

    /// Lists partitions of one partitioned step execution.
    ///
    /// # Errors
    ///
    /// Returns a typed cursor, bound, timeout, or repository failure.
    pub async fn list_step_partitions(
        &self,
        step_execution_id: StepExecutionId,
        request: &PageRequest,
    ) -> Result<Page<StepPartitionProjection>, ExplorerError> {
        let query = ExplorerQuery::StepPartitions { step_execution_id };
        let window = self.window(&query, request).await?;
        let rows = self
            .source
            .step_partitions(step_execution_id, &window)
            .await?;
        self.finish_page(None, page(&query, request, window.ceiling(), rows))
    }

    /// Lists audited operator requests for one job execution.
    ///
    /// # Errors
    ///
    /// Returns a typed cursor, bound, timeout, or repository failure.
    pub async fn list_operator_requests(
        &self,
        job_execution_id: JobExecutionId,
        request: &PageRequest,
    ) -> Result<Page<OperatorRecord>, ExplorerError> {
        let query = ExplorerQuery::OperatorRequests { job_execution_id };
        let window = self.window(&query, request).await?;
        let rows = self
            .source
            .operator_requests(job_execution_id, &window)
            .await?;
        self.finish_page(
            Some(job_execution_id),
            page(&query, request, window.ceiling(), rows),
        )
    }

    fn finish_page<T>(
        &self,
        execution_id: Option<JobExecutionId>,
        result: Result<Page<T>, ExplorerError>,
    ) -> Result<Page<T>, ExplorerError> {
        if result.is_ok() {
            let record = TelemetryRecord::explorer(execution_id);
            for sink in &self.event_sinks {
                crate::telemetry::emit_safely(Some(sink), &record);
            }
        }
        result
    }

    async fn window(
        &self,
        query: &ExplorerQuery,
        request: &PageRequest,
    ) -> Result<QueryWindow, ExplorerError> {
        let limit = request.size().get();
        match request.cursor() {
            None => {
                let ceiling = self.source.identity_ceiling(query).await?;
                Ok(QueryWindow::new(None, ceiling, limit))
            }
            Some(cursor) => {
                let (after, ceiling) = decode_cursor(cursor, query, request.size())?;
                Ok(QueryWindow::new(Some(after), ceiling, limit))
            }
        }
    }
}

/// A stable inspection failure independent of a database or async runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ExplorerError {
    /// The requested page size is outside `1..=500`.
    PageSizeOutOfRange {
        /// Rejected size.
        requested: u16,
    },
    /// The unresolved-execution query requires an explicit larger age bound.
    AgeBoundTooSmall {
        /// Smallest accepted age.
        minimum: Duration,
    },
    /// A continuation token was rejected.
    Cursor(CursorError),
    /// One row alone exceeds the encoded response bound.
    ResponseTooLarge {
        /// Maximum encoded response size in bytes.
        limit: usize,
    },
    /// The statement exceeded the configured statement timeout.
    Timeout,
    /// The adapter cannot provide bounded keyset pagination.
    UnsupportedCapability,
    /// The underlying repository failed.
    Repository(RepositoryError),
}

impl fmt::Display for ExplorerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PageSizeOutOfRange { requested } => write!(
                formatter,
                "page size {requested} is outside 1..={MAX_PAGE_SIZE}"
            ),
            Self::AgeBoundTooSmall { minimum } => write!(
                formatter,
                "the age bound must be at least {} seconds",
                minimum.as_secs()
            ),
            Self::Cursor(error) => error.fmt(formatter),
            Self::ResponseTooLarge { limit } => {
                write!(formatter, "the encoded response exceeds {limit} bytes")
            }
            Self::Timeout => {
                formatter.write_str("the bounded query exceeded its statement timeout")
            }
            Self::UnsupportedCapability => {
                formatter.write_str("the adapter does not support keyset pagination")
            }
            Self::Repository(error) => error.fmt(formatter),
        }
    }
}

impl Error for ExplorerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Cursor(error) => Some(error),
            Self::Repository(error) => Some(error),
            _ => None,
        }
    }
}

impl From<CursorError> for ExplorerError {
    fn from(value: CursorError) -> Self {
        Self::Cursor(value)
    }
}

impl From<RepositoryError> for ExplorerError {
    fn from(value: RepositoryError) -> Self {
        Self::Repository(value)
    }
}

/// A rejected continuation token.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CursorError {
    /// The token was malformed, oversized, or failed its checksum.
    CursorInvalid,
    /// The token belongs to a different query, filter, or page size.
    CursorQueryMismatch,
}

impl fmt::Display for CursorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CursorInvalid => formatter.write_str("the continuation token is not valid"),
            Self::CursorQueryMismatch => {
                formatter.write_str("the continuation token belongs to a different query")
            }
        }
    }
}

impl Error for CursorError {}

fn encode_cursor(
    query: &ExplorerQuery,
    size: PageSize,
    key: &CursorKey,
    ceiling: u64,
) -> Result<Cursor, CursorError> {
    let mut bytes = Vec::with_capacity(80);
    bytes.push(CURSOR_FORMAT_VERSION);
    bytes.push(query.discriminant());
    key.encode(&mut bytes)?;
    bytes.extend_from_slice(&ceiling.to_be_bytes());
    bytes.extend_from_slice(&query_binding(query, size));
    let checksum = cursor_checksum(&bytes);
    bytes.extend_from_slice(&checksum);
    Cursor::from_bytes(bytes)
}

fn decode_cursor(
    cursor: &Cursor,
    query: &ExplorerQuery,
    size: PageSize,
) -> Result<(CursorKey, u64), ExplorerError> {
    let bytes = cursor.as_bytes();
    if bytes.len() <= 32 {
        return Err(CursorError::CursorInvalid.into());
    }
    let (body, checksum) = bytes.split_at(bytes.len() - 32);
    if cursor_checksum(body) != checksum {
        return Err(CursorError::CursorInvalid.into());
    }
    let (version, rest) = body.split_first().ok_or(CursorError::CursorInvalid)?;
    if *version != CURSOR_FORMAT_VERSION {
        return Err(CursorError::CursorInvalid.into());
    }
    let (discriminant, rest) = rest.split_first().ok_or(CursorError::CursorInvalid)?;
    let (key, rest) = CursorKey::decode(rest)?;
    let (ceiling, rest) = read_u64(rest)?;
    if rest.len() != BINDING_BYTES {
        return Err(CursorError::CursorInvalid.into());
    }
    // The token is intact. Any difference in query, filter, or page size is a
    // mismatch rather than corruption, so a caller can tell a reused token
    // from a damaged one.
    if *discriminant != query.discriminant() || rest != query_binding(query, size) {
        return Err(CursorError::CursorQueryMismatch.into());
    }
    Ok((key, ceiling))
}

fn query_binding(query: &ExplorerQuery, size: PageSize) -> [u8; BINDING_BYTES] {
    let identity = query.identity(size);
    let mut binding = [0_u8; BINDING_BYTES];
    binding.copy_from_slice(&identity[..BINDING_BYTES]);
    binding
}

fn cursor_checksum(body: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(body);
    hasher.finalize().into()
}

/// The immutable ordering key and estimated encoded size of one explorer row.
trait ExplorerRow {
    fn cursor_key(&self) -> CursorKey;

    fn encoded_len(&self) -> usize;
}

fn page<T: ExplorerRow>(
    query: &ExplorerQuery,
    request: &PageRequest,
    ceiling: u64,
    rows: Vec<T>,
) -> Result<Page<T>, ExplorerError> {
    let limit = usize::from(request.size().get());
    let full = rows.len() >= limit;
    let mut kept = Vec::with_capacity(rows.len().min(limit));
    let mut encoded = 0_usize;
    let mut truncated = false;
    for row in rows.into_iter().take(limit) {
        let next = encoded.saturating_add(row.encoded_len());
        if next > MAX_RESPONSE_BYTES {
            if kept.is_empty() {
                return Err(ExplorerError::ResponseTooLarge {
                    limit: MAX_RESPONSE_BYTES,
                });
            }
            truncated = true;
            break;
        }
        encoded = next;
        kept.push(row);
    }
    let next = if (full || truncated) && !kept.is_empty() {
        let key = kept
            .last()
            .map(ExplorerRow::cursor_key)
            .ok_or(ExplorerError::Cursor(CursorError::CursorInvalid))?;
        Some(encode_cursor(query, request.size(), &key, ceiling)?)
    } else {
        None
    };
    Ok(Page::new(kept, next))
}

impl ExplorerRow for JobName {
    fn cursor_key(&self) -> CursorKey {
        CursorKey::Name(self.as_str().to_owned())
    }

    fn encoded_len(&self) -> usize {
        self.as_str().len().saturating_add(8)
    }
}

impl ExplorerRow for JobInstanceProjection {
    fn cursor_key(&self) -> CursorKey {
        CursorKey::Identity(self.id.get())
    }

    fn encoded_len(&self) -> usize {
        let parameters = self
            .parameters
            .iter()
            .map(|parameter| parameter.name.as_str().len().saturating_add(24))
            .fold(0_usize, usize::saturating_add);
        self.job_name
            .as_str()
            .len()
            .saturating_add(160)
            .saturating_add(parameters)
    }
}

impl ExplorerRow for JobExecutionProjection {
    fn cursor_key(&self) -> CursorKey {
        CursorKey::Ordered {
            primary: u64::from(self.attempt),
            identity: self.id.get(),
        }
    }

    fn encoded_len(&self) -> usize {
        self.job_name
            .as_str()
            .len()
            .saturating_add(self.exit_status.code().as_str().len())
            .saturating_add(256)
    }
}

impl ExplorerRow for StepExecutionProjection {
    fn cursor_key(&self) -> CursorKey {
        CursorKey::Identity(self.id.get())
    }

    fn encoded_len(&self) -> usize {
        self.step_name
            .as_str()
            .len()
            .saturating_add(self.exit_status.code().as_str().len())
            .saturating_add(256)
    }
}

impl ExplorerRow for StepPartitionProjection {
    fn cursor_key(&self) -> CursorKey {
        CursorKey::Identity(self.id.get())
    }

    fn encoded_len(&self) -> usize {
        self.partition_key.len().saturating_add(192)
    }
}

impl ExplorerRow for RecoveryDecision {
    fn cursor_key(&self) -> CursorKey {
        CursorKey::Identity(self.id().get())
    }

    fn encoded_len(&self) -> usize {
        self.reason_code()
            .len()
            .saturating_add(self.operator_reference().len())
            .saturating_add(160)
    }
}

impl ExplorerRow for FlowDecision {
    fn cursor_key(&self) -> CursorKey {
        CursorKey::Ordered {
            primary: self.sequence().get(),
            identity: self.id().get(),
        }
    }

    fn encoded_len(&self) -> usize {
        self.source_node_id()
            .as_str()
            .len()
            .saturating_add(self.observed_outcome().as_str().len())
            .saturating_add(224)
    }
}

impl ExplorerRow for OperatorRecord {
    fn cursor_key(&self) -> CursorKey {
        CursorKey::Identity(self.id().get())
    }

    fn encoded_len(&self) -> usize {
        self.operation_id()
            .as_str()
            .len()
            .saturating_add(self.actor().as_str().len())
            .saturating_add(self.reason().map_or(0, |reason| reason.as_str().len()))
            .saturating_add(192)
    }
}
