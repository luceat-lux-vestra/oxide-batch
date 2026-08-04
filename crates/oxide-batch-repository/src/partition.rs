//! Durable local-partition plan and result values.

use std::error::Error;
use std::fmt;

use oxide_batch_core::{
    BatchStatus, ExecutionContext, ExecutionCounts, ExecutionVersion, ExitStatus, MAX_PARTITIONS,
    StepExecution, StepExecutionId, StepPartitionId,
};

/// Maximum UTF-8 byte length of one durable partition key.
pub const MAX_PARTITION_KEY_BYTES: usize = 128;
/// Maximum serialized byte length of one durable partition context.
pub const MAX_PARTITION_CONTEXT_BYTES: usize = 4 * 1024;

/// A stable byte-compared key within one partitioned step execution.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PartitionKey(String);

impl PartitionKey {
    /// Validates a nonempty bounded UTF-8 partition key.
    ///
    /// Whitespace and punctuation are retained because partition identity is
    /// byte-exact and application-defined.
    ///
    /// # Errors
    ///
    /// Returns [`PartitionValueError`] for an empty or oversized key.
    pub fn new(value: impl Into<String>) -> Result<Self, PartitionValueError> {
        let value = value.into();
        if value.is_empty() {
            return Err(PartitionValueError::EmptyKey);
        }
        if value.len() > MAX_PARTITION_KEY_BYTES {
            return Err(PartitionValueError::KeyTooLong {
                max_bytes: MAX_PARTITION_KEY_BYTES,
            });
        }
        Ok(Self(value))
    }

    /// Borrows the byte-compared key.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for PartitionKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PartitionKey(<redacted>)")
    }
}

impl fmt::Display for PartitionKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// One validated entry in a partition plan before durable identity assignment.
#[derive(Clone, Eq, PartialEq)]
pub struct PartitionPlanEntry {
    key: PartitionKey,
    context: ExecutionContext,
}

impl PartitionPlanEntry {
    /// Constructs one bounded partition-plan entry.
    ///
    /// # Errors
    ///
    /// Returns [`PartitionValueError::ContextTooLarge`] when the serialized
    /// context exceeds the schema-3 `4 KiB` ceiling.
    pub fn new(key: PartitionKey, context: ExecutionContext) -> Result<Self, PartitionValueError> {
        if context.encoded_len() > MAX_PARTITION_CONTEXT_BYTES {
            return Err(PartitionValueError::ContextTooLarge {
                max_bytes: MAX_PARTITION_CONTEXT_BYTES,
            });
        }
        Ok(Self { key, context })
    }

    /// Borrows the stable partition key.
    #[must_use]
    pub const fn key(&self) -> &PartitionKey {
        &self.key
    }

    /// Borrows the redacted durable context.
    #[must_use]
    pub const fn context(&self) -> &ExecutionContext {
        &self.context
    }
}

impl fmt::Debug for PartitionPlanEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PartitionPlanEntry")
            .field("key", &self.key)
            .field("context", &self.context)
            .finish()
    }
}

/// A durable partition plan row and its latest result snapshot.
#[derive(Clone, Eq, PartialEq)]
pub struct StepPartition {
    id: StepPartitionId,
    step_execution_id: StepExecutionId,
    worker_step_execution_id: Option<StepExecutionId>,
    key: PartitionKey,
    ordinal: u32,
    status: BatchStatus,
    exit_status: ExitStatus,
    counts: ExecutionCounts,
    context: ExecutionContext,
    version: ExecutionVersion,
}

impl StepPartition {
    /// Reconstructs one durable partition row read by an adapter.
    #[allow(clippy::too_many_arguments)]
    #[doc(hidden)]
    #[must_use]
    pub const fn from_snapshot(
        id: StepPartitionId,
        step_execution_id: StepExecutionId,
        worker_step_execution_id: Option<StepExecutionId>,
        key: PartitionKey,
        ordinal: u32,
        status: BatchStatus,
        exit_status: ExitStatus,
        counts: ExecutionCounts,
        context: ExecutionContext,
        version: ExecutionVersion,
    ) -> Self {
        Self {
            id,
            step_execution_id,
            worker_step_execution_id,
            key,
            ordinal,
            status,
            exit_status,
            counts,
            context,
            version,
        }
    }

    /// Builds the initial durable row for one planned partition.
    #[doc(hidden)]
    #[must_use]
    pub fn starting(
        id: StepPartitionId,
        step_execution_id: StepExecutionId,
        ordinal: u32,
        entry: PartitionPlanEntry,
    ) -> Self {
        Self::from_snapshot(
            id,
            step_execution_id,
            None,
            entry.key,
            ordinal,
            BatchStatus::Starting,
            ExitStatus::unknown(),
            ExecutionCounts::default(),
            entry.context,
            ExecutionVersion::INITIAL,
        )
    }

    /// Returns the durable partition-row identifier.
    #[must_use]
    pub const fn id(&self) -> StepPartitionId {
        self.id
    }

    /// Returns the parent partitioned step execution.
    #[must_use]
    pub const fn step_execution_id(&self) -> StepExecutionId {
        self.step_execution_id
    }

    /// Returns the assigned worker attempt, when one has started.
    #[must_use]
    pub const fn worker_step_execution_id(&self) -> Option<StepExecutionId> {
        self.worker_step_execution_id
    }

    /// Borrows the stable partition key.
    #[must_use]
    pub const fn key(&self) -> &PartitionKey {
        &self.key
    }

    /// Returns the one-based partition-plan ordinal.
    #[must_use]
    pub const fn ordinal(&self) -> u32 {
        self.ordinal
    }

    /// Returns the current framework status.
    #[must_use]
    pub const fn status(&self) -> BatchStatus {
        self.status
    }

    /// Borrows the latest stable exit status.
    #[must_use]
    pub const fn exit_status(&self) -> &ExitStatus {
        &self.exit_status
    }

    /// Returns the latest durable counters.
    #[must_use]
    pub const fn counts(&self) -> ExecutionCounts {
        self.counts
    }

    /// Borrows the redacted partition context.
    #[must_use]
    pub const fn context(&self) -> &ExecutionContext {
        &self.context
    }

    /// Returns the optimistic-lock version.
    #[must_use]
    pub const fn version(&self) -> ExecutionVersion {
        self.version
    }

    /// Assigns one worker attempt to this partition.
    ///
    /// # Errors
    ///
    /// Returns [`PartitionMutationError`] for a stale version or a status that
    /// cannot take an assignment.
    #[doc(hidden)]
    pub fn assign(
        &mut self,
        expected_version: ExecutionVersion,
        worker_step_execution_id: StepExecutionId,
    ) -> Result<(), PartitionMutationError> {
        self.ensure_version(expected_version)?;
        let first_assignment =
            self.status == BatchStatus::Starting && self.worker_step_execution_id.is_none();
        let retry_assignment = matches!(self.status, BatchStatus::Failed | BatchStatus::Stopped)
            && self.worker_step_execution_id.is_some();
        if !first_assignment && !retry_assignment {
            return Err(PartitionMutationError::InvalidState {
                status: self.status,
            });
        }
        self.worker_step_execution_id = Some(worker_step_execution_id);
        self.status = BatchStatus::Started;
        self.exit_status = ExitStatus::unknown();
        self.counts = ExecutionCounts::default();
        self.version = self.next_version()?;
        Ok(())
    }

    /// Records one terminal worker result on this partition.
    ///
    /// # Errors
    ///
    /// Returns [`PartitionMutationError`] for a stale version or a partition
    /// that is not assigned and started.
    #[doc(hidden)]
    pub fn complete(
        &mut self,
        expected_version: ExecutionVersion,
        result: &PartitionResult,
    ) -> Result<(), PartitionMutationError> {
        self.ensure_version(expected_version)?;
        if self.status != BatchStatus::Started || self.worker_step_execution_id.is_none() {
            return Err(PartitionMutationError::InvalidState {
                status: self.status,
            });
        }
        self.status = result.status;
        self.exit_status = result.exit_status.clone();
        self.counts = result.counts;
        self.version = self.next_version()?;
        Ok(())
    }

    fn ensure_version(
        &self,
        expected_version: ExecutionVersion,
    ) -> Result<(), PartitionMutationError> {
        if expected_version != self.version {
            return Err(PartitionMutationError::StaleVersion {
                expected: expected_version,
                actual: self.version,
            });
        }
        Ok(())
    }

    fn next_version(&self) -> Result<ExecutionVersion, PartitionMutationError> {
        self.version
            .get()
            .checked_add(1)
            .map(ExecutionVersion::new)
            .ok_or(PartitionMutationError::VersionExhausted)
    }
}

impl fmt::Debug for StepPartition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StepPartition")
            .field("id", &self.id)
            .field("step_execution_id", &self.step_execution_id)
            .field("worker_step_execution_id", &self.worker_step_execution_id)
            .field("key", &self.key)
            .field("ordinal", &self.ordinal)
            .field("status", &self.status)
            .field("exit_status", &self.exit_status)
            .field("counts", &self.counts)
            .field("context", &self.context)
            .field("version", &self.version)
            .finish()
    }
}

/// A validated terminal result published by one assigned partition worker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionResult {
    status: BatchStatus,
    exit_status: ExitStatus,
    counts: ExecutionCounts,
}

/// The deterministic result of aggregating one complete durable partition plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionAggregate {
    status: BatchStatus,
    exit_status: ExitStatus,
    counts: ExecutionCounts,
    selected_worker_step_execution_id: StepExecutionId,
}

impl PartitionAggregate {
    /// Returns the most severe child status.
    #[must_use]
    pub const fn status(&self) -> BatchStatus {
        self.status
    }

    /// Borrows the first matching exit status in partition-key byte order.
    #[must_use]
    pub const fn exit_status(&self) -> &ExitStatus {
        &self.exit_status
    }

    /// Returns the checked sum of every durable child counter.
    #[must_use]
    pub const fn counts(&self) -> ExecutionCounts {
        self.counts
    }

    /// Returns the worker attempt whose result the aggregate selected.
    #[doc(hidden)]
    #[must_use]
    pub const fn selected_worker_step_execution_id(&self) -> StepExecutionId {
        self.selected_worker_step_execution_id
    }
}

/// Aggregates a complete partition plan independently of input or completion order.
///
/// Children are ordered by their byte-exact partition keys before counters and
/// exit status are selected. The status severity is fixed as
/// `UNKNOWN > FAILED > STOPPED > COMPLETED`.
///
/// # Errors
///
/// Returns [`PartitionAggregationError`] when the plan is empty, contains a
/// duplicate key, still has an active/non-runtime result, or a counter sum
/// exceeds the durable representation.
pub fn aggregate_step_partitions(
    partitions: &[StepPartition],
) -> Result<PartitionAggregate, PartitionAggregationError> {
    if partitions.is_empty() {
        return Err(PartitionAggregationError::EmptyPlan);
    }
    if partitions.len() > usize::from(MAX_PARTITIONS) {
        return Err(PartitionAggregationError::PlanTooLarge {
            max: usize::from(MAX_PARTITIONS),
        });
    }

    let mut ordered = partitions.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| left.key().cmp(right.key()));
    if ordered
        .windows(2)
        .any(|pair| pair[0].key() == pair[1].key())
    {
        return Err(PartitionAggregationError::DuplicateKey);
    }

    let mut aggregate_status = BatchStatus::Completed;
    let mut counts = ExecutionCounts::default();
    for partition in &ordered {
        let status = partition.status();
        if partition.worker_step_execution_id().is_none()
            || !matches!(
                status,
                BatchStatus::Completed
                    | BatchStatus::Failed
                    | BatchStatus::Stopped
                    | BatchStatus::Unknown
            )
        {
            return Err(PartitionAggregationError::Incomplete { status });
        }
        if partition_severity(status) > partition_severity(aggregate_status) {
            aggregate_status = status;
        }
        counts = checked_sum_counts(counts, partition.counts())?;
    }

    let selected = ordered
        .iter()
        .find(|partition| partition.status() == aggregate_status)
        .ok_or(PartitionAggregationError::Incomplete {
            status: aggregate_status,
        })?;
    let selected_worker_step_execution_id =
        selected
            .worker_step_execution_id()
            .ok_or(PartitionAggregationError::Incomplete {
                status: aggregate_status,
            })?;
    Ok(PartitionAggregate {
        status: aggregate_status,
        exit_status: selected.exit_status().clone(),
        counts,
        selected_worker_step_execution_id,
    })
}

const fn partition_severity(status: BatchStatus) -> u8 {
    match status {
        BatchStatus::Completed => 0,
        BatchStatus::Stopped => 1,
        BatchStatus::Failed => 2,
        BatchStatus::Unknown => 3,
        _ => 4,
    }
}

fn checked_sum_counts(
    left: ExecutionCounts,
    right: ExecutionCounts,
) -> Result<ExecutionCounts, PartitionAggregationError> {
    let counts = ExecutionCounts::new(
        left.read()
            .checked_add(right.read())
            .ok_or(PartitionAggregationError::CountExhausted)?,
        left.processed()
            .checked_add(right.processed())
            .ok_or(PartitionAggregationError::CountExhausted)?,
        left.written()
            .checked_add(right.written())
            .ok_or(PartitionAggregationError::CountExhausted)?,
        left.filtered()
            .checked_add(right.filtered())
            .ok_or(PartitionAggregationError::CountExhausted)?,
        left.committed()
            .checked_add(right.committed())
            .ok_or(PartitionAggregationError::CountExhausted)?,
        left.rolled_back()
            .checked_add(right.rolled_back())
            .ok_or(PartitionAggregationError::CountExhausted)?,
    );
    if [
        counts.read(),
        counts.processed(),
        counts.written(),
        counts.filtered(),
        counts.committed(),
        counts.rolled_back(),
    ]
    .into_iter()
    .any(|value| value > i64::MAX as u64)
    {
        return Err(PartitionAggregationError::CountExhausted);
    }
    Ok(counts)
}

/// A deterministic partition plan could not be aggregated safely.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PartitionAggregationError {
    /// No durable child result was supplied.
    EmptyPlan,
    /// The supplied plan exceeded the accepted M4 partition bound.
    PlanTooLarge {
        /// Maximum accepted partition count.
        max: usize,
    },
    /// More than one child used the same byte-exact key.
    DuplicateKey,
    /// At least one child did not have a durable runtime-terminal result.
    Incomplete {
        /// The unusable durable status.
        status: BatchStatus,
    },
    /// At least one aggregate counter exceeded `u64`.
    CountExhausted,
}

impl fmt::Display for PartitionAggregationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyPlan => formatter.write_str("partition aggregation requires a plan"),
            Self::PlanTooLarge { max } => {
                write!(formatter, "partition aggregation exceeds {max} children")
            }
            Self::DuplicateKey => {
                formatter.write_str("partition aggregation found a duplicate key")
            }
            Self::Incomplete { status } => write!(
                formatter,
                "partition aggregation cannot use a child in {status}"
            ),
            Self::CountExhausted => {
                formatter.write_str("partition aggregate counters are exhausted")
            }
        }
    }
}

impl Error for PartitionAggregationError {}

impl PartitionResult {
    /// Reads one terminal worker attempt as a durable partition result.
    ///
    /// # Errors
    ///
    /// Returns [`PartitionValueError::NonTerminalResult`] when the worker has
    /// not reached a terminal status.
    #[doc(hidden)]
    pub fn from_worker(worker: &StepExecution) -> Result<Self, PartitionValueError> {
        Self::new(
            worker.metadata().status(),
            worker.metadata().exit_status().clone(),
            worker.metadata().counts(),
        )
    }
    /// Validates one known or explicitly ambiguous terminal worker result.
    ///
    /// # Errors
    ///
    /// Returns [`PartitionValueError::NonTerminalResult`] for an active or
    /// abandoned status.
    pub fn new(
        status: BatchStatus,
        exit_status: ExitStatus,
        counts: ExecutionCounts,
    ) -> Result<Self, PartitionValueError> {
        if !matches!(
            status,
            BatchStatus::Completed
                | BatchStatus::Failed
                | BatchStatus::Stopped
                | BatchStatus::Unknown
        ) {
            return Err(PartitionValueError::NonTerminalResult { status });
        }
        if [
            counts.read(),
            counts.processed(),
            counts.written(),
            counts.filtered(),
            counts.committed(),
            counts.rolled_back(),
        ]
        .into_iter()
        .any(|value| value > i64::MAX as u64)
        {
            return Err(PartitionValueError::CountTooLarge);
        }
        Ok(Self {
            status,
            exit_status,
            counts,
        })
    }

    /// Returns the terminal framework status.
    #[must_use]
    pub const fn status(&self) -> BatchStatus {
        self.status
    }

    /// Borrows the terminal exit status.
    #[must_use]
    pub const fn exit_status(&self) -> &ExitStatus {
        &self.exit_status
    }

    /// Returns the terminal counters.
    #[must_use]
    pub const fn counts(&self) -> ExecutionCounts {
        self.counts
    }
}

/// Invalid public partition input.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PartitionValueError {
    /// A partition key was empty.
    EmptyKey,
    /// A partition key exceeded the durable byte bound.
    KeyTooLong {
        /// Maximum accepted UTF-8 bytes.
        max_bytes: usize,
    },
    /// A partition context exceeded the schema-3 bound.
    ContextTooLarge {
        /// Maximum accepted serialized bytes.
        max_bytes: usize,
    },
    /// A worker result used a non-terminal status.
    NonTerminalResult {
        /// Rejected status.
        status: BatchStatus,
    },
    /// A counter cannot be represented by every durable adapter.
    CountTooLarge,
}

impl fmt::Display for PartitionValueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyKey => formatter.write_str("partition key must not be empty"),
            Self::KeyTooLong { max_bytes } => {
                write!(formatter, "partition key exceeds {max_bytes} UTF-8 bytes")
            }
            Self::ContextTooLarge { max_bytes } => {
                write!(formatter, "partition context exceeds {max_bytes} bytes")
            }
            Self::NonTerminalResult { status } => {
                write!(
                    formatter,
                    "partition result status {status} is not terminal"
                )
            }
            Self::CountTooLarge => {
                formatter.write_str("partition result counter exceeds the portable durable bound")
            }
        }
    }
}

impl Error for PartitionValueError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// A rejected durable partition mutation.
#[doc(hidden)]
pub enum PartitionMutationError {
    StaleVersion {
        expected: ExecutionVersion,
        actual: ExecutionVersion,
    },
    InvalidState {
        status: BatchStatus,
    },
    VersionExhausted,
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxide_batch_core::StateLimits;

    #[test]
    fn partition_key_and_context_bounds_fail_before_persistence() -> Result<(), Box<dyn Error>> {
        assert_eq!(PartitionKey::new(""), Err(PartitionValueError::EmptyKey));
        assert_eq!(
            PartitionKey::new("x".repeat(MAX_PARTITION_KEY_BYTES + 1)),
            Err(PartitionValueError::KeyTooLong {
                max_bytes: MAX_PARTITION_KEY_BYTES,
            })
        );

        let oversized = format!(
            "{{\"format\":\"oxide-batch.execution-context\",\"format_version\":1,\
             \"schema\":\"partition.test\",\"schema_version\":1,\
             \"payload\":{{\"value\":\"{}\"}}}}",
            "x".repeat(MAX_PARTITION_CONTEXT_BYTES)
        );
        let context =
            ExecutionContext::from_json(oversized.as_bytes(), StateLimits::new(8 * 1024, 16)?)?;
        assert_eq!(
            PartitionPlanEntry::new(PartitionKey::new("partition-1")?, context),
            Err(PartitionValueError::ContextTooLarge {
                max_bytes: MAX_PARTITION_CONTEXT_BYTES,
            })
        );
        Ok(())
    }

    #[test]
    fn partition_result_accepts_only_runtime_terminal_outcomes() {
        assert_eq!(
            PartitionResult::new(
                BatchStatus::Started,
                ExitStatus::unknown(),
                ExecutionCounts::default(),
            ),
            Err(PartitionValueError::NonTerminalResult {
                status: BatchStatus::Started,
            })
        );
        assert!(
            PartitionResult::new(
                BatchStatus::Unknown,
                ExitStatus::unknown(),
                ExecutionCounts::default(),
            )
            .is_ok()
        );
        assert_eq!(
            PartitionResult::new(
                BatchStatus::Completed,
                ExitStatus::completed(),
                ExecutionCounts::new(i64::MAX as u64 + 1, 0, 0, 0, 0, 0),
            ),
            Err(PartitionValueError::CountTooLarge)
        );
    }

    #[test]
    fn aggregate_rejects_counts_above_the_postgres_bigint_bound() -> Result<(), Box<dyn Error>> {
        let context = ExecutionContext::from_json(
            br#"{"format":"oxide-batch.execution-context","format_version":1,"schema":"partition.aggregate","schema_version":1,"payload":{}}"#,
            StateLimits::new(MAX_PARTITION_CONTEXT_BYTES, 16)?,
        )?;
        let completed = |id: u64, key: &str, count: u64| -> Result<StepPartition, Box<dyn Error>> {
            let mut partition = StepPartition::starting(
                StepPartitionId::new(id)?,
                StepExecutionId::new(1)?,
                u32::try_from(id)?,
                PartitionPlanEntry::new(PartitionKey::new(key)?, context.clone())?,
            );
            partition
                .assign(ExecutionVersion::INITIAL, StepExecutionId::new(id + 10)?)
                .map_err(|_| std::io::Error::other("partition assignment failed"))?;
            partition
                .complete(
                    partition.version(),
                    &PartitionResult::new(
                        BatchStatus::Completed,
                        ExitStatus::completed(),
                        ExecutionCounts::new(count, 0, 0, 0, 0, 0),
                    )?,
                )
                .map_err(|_| std::io::Error::other("partition completion failed"))?;
            Ok(partition)
        };
        let first = completed(1, "alpha", i64::MAX as u64)?;
        let second = completed(2, "beta", 1)?;
        assert_eq!(
            aggregate_step_partitions(&[first, second]),
            Err(PartitionAggregationError::CountExhausted)
        );
        Ok(())
    }

    #[test]
    fn aggregation_is_deterministic_in_partition_key_order() -> Result<(), Box<dyn Error>> {
        let context = ExecutionContext::from_json(
            br#"{"format":"oxide-batch.execution-context","format_version":1,"schema":"partition.aggregate","schema_version":1,"payload":{}}"#,
            StateLimits::new(MAX_PARTITION_CONTEXT_BYTES, 16)?,
        )?;
        let mut alpha = StepPartition::starting(
            StepPartitionId::new(1)?,
            StepExecutionId::new(1)?,
            1,
            PartitionPlanEntry::new(PartitionKey::new("alpha")?, context.clone())?,
        );
        alpha
            .assign(ExecutionVersion::INITIAL, StepExecutionId::new(2)?)
            .map_err(|_| std::io::Error::other("alpha assignment failed"))?;
        alpha
            .complete(
                alpha.version(),
                &PartitionResult::new(
                    BatchStatus::Failed,
                    ExitStatus::new(oxide_batch_core::ExitCode::new("ALPHA_FAILED")?),
                    ExecutionCounts::new(1, 2, 3, 4, 5, 6),
                )?,
            )
            .map_err(|_| std::io::Error::other("alpha completion failed"))?;
        let mut zeta = StepPartition::starting(
            StepPartitionId::new(2)?,
            StepExecutionId::new(1)?,
            2,
            PartitionPlanEntry::new(PartitionKey::new("zeta")?, context)?,
        );
        zeta.assign(ExecutionVersion::INITIAL, StepExecutionId::new(3)?)
            .map_err(|_| std::io::Error::other("zeta assignment failed"))?;
        zeta.complete(
            zeta.version(),
            &PartitionResult::new(
                BatchStatus::Failed,
                ExitStatus::new(oxide_batch_core::ExitCode::new("ZETA_FAILED")?),
                ExecutionCounts::new(10, 20, 30, 40, 50, 60),
            )?,
        )
        .map_err(|_| std::io::Error::other("zeta completion failed"))?;

        let forward = aggregate_step_partitions(&[alpha.clone(), zeta.clone()])?;
        let reverse = aggregate_step_partitions(&[zeta.clone(), alpha.clone()])?;
        assert_eq!(forward, reverse);
        assert_eq!(forward.status(), BatchStatus::Failed);
        assert_eq!(forward.exit_status().code().as_str(), "ALPHA_FAILED");
        assert_eq!(
            forward.counts(),
            ExecutionCounts::new(11, 22, 33, 44, 55, 66)
        );

        let context = ExecutionContext::from_json(
            br#"{"format":"oxide-batch.execution-context","format_version":1,"schema":"partition.aggregate","schema_version":1,"payload":{}}"#,
            StateLimits::new(MAX_PARTITION_CONTEXT_BYTES, 16)?,
        )?;
        let mut unknown = StepPartition::starting(
            StepPartitionId::new(3)?,
            StepExecutionId::new(1)?,
            3,
            PartitionPlanEntry::new(PartitionKey::new("middle")?, context)?,
        );
        unknown
            .assign(ExecutionVersion::INITIAL, StepExecutionId::new(4)?)
            .map_err(|_| std::io::Error::other("unknown assignment failed"))?;
        unknown
            .complete(
                unknown.version(),
                &PartitionResult::new(
                    BatchStatus::Unknown,
                    ExitStatus::unknown(),
                    ExecutionCounts::default(),
                )?,
            )
            .map_err(|_| std::io::Error::other("unknown completion failed"))?;
        let ambiguous = aggregate_step_partitions(&[alpha, zeta, unknown])?;
        assert_eq!(ambiguous.status(), BatchStatus::Unknown);
        assert_eq!(ambiguous.exit_status(), &ExitStatus::unknown());
        Ok(())
    }
}
