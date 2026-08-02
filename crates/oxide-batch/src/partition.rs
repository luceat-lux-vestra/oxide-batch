//! Durable local-partition plan and result values.

use std::error::Error;
use std::fmt;

use crate::{
    BatchStatus, ExecutionContext, ExecutionCounts, ExecutionVersion, ExitStatus, StepExecutionId,
    StepPartitionId,
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
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn from_snapshot(
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

    pub(crate) fn starting(
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

    pub(crate) fn assign(
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

    pub(crate) fn complete(
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

impl PartitionResult {
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
        }
    }
}

impl Error for PartitionValueError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PartitionMutationError {
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
    use crate::StateLimits;

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
    }
}
