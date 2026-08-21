//! A standalone, adapter-neutral [`ChunkTransactionManager`] for tests that
//! do not need durable inherited progress.
//!
//! Standalone chunk execution has no durable execution graph (see
//! [`ChunkTransactionContext`](oxide_batch::ChunkTransactionContext)'s own
//! documentation), so [`StandaloneTransactions`] always commits and never
//! reports inherited progress -- exactly the shape
//! [`ChunkStep::execute`](oxide_batch::ChunkStep::execute) is documented to
//! use. It commits every attempt in-process without a business transaction
//! and without leaking a database-driver type.

use oxide_batch::{
    BoxFuture, BusinessTransaction, Checkpoint, ChunkCommitReceipt, ChunkCompletion,
    ChunkCompletionContext, ChunkCompletionError, ChunkCompletionOutcome, ChunkCounts,
    ChunkFaultProgress, ChunkTransaction, ChunkTransactionError, ChunkTransactionManager,
    ComponentStateEnvelope, ExecutionContext, StateLimits,
};

const CHECKPOINT_TEMPLATE: &[u8] =
    br#"{"format":"oxide-batch.checkpoint","format_version":1,"schema":"oxide-batch-test.standalone","schema_version":1,"payload":{}}"#;
const CONTEXT_TEMPLATE: &[u8] =
    br#"{"format":"oxide-batch.execution-context","format_version":1,"schema":"oxide-batch-test.standalone","schema_version":1,"payload":{}}"#;

fn placeholder_receipt() -> Result<ChunkCommitReceipt, ChunkTransactionError> {
    let checkpoint = Checkpoint::from_json(CHECKPOINT_TEMPLATE, StateLimits::default())
        .map_err(|_| ChunkTransactionError::NotCommitted)?;
    let execution_context = ExecutionContext::from_json(CONTEXT_TEMPLATE, StateLimits::default())
        .map_err(|_| ChunkTransactionError::NotCommitted)?;
    Ok(ChunkCommitReceipt::new(checkpoint, execution_context))
}

/// A standalone chunk transaction that always commits, with no enlisted
/// business transaction and no durable checkpoint.
pub struct StandaloneTransaction;

impl ChunkTransaction for StandaloneTransaction {
    fn business_transaction(&mut self) -> Option<&mut dyn BusinessTransaction> {
        None
    }

    fn commit(
        &mut self,
        _counts: ChunkCounts,
        _fault: ChunkFaultProgress,
    ) -> BoxFuture<'_, Result<ChunkCommitReceipt, ChunkTransactionError>> {
        Box::pin(async { placeholder_receipt() })
    }

    fn commit_with_component_state<'a>(
        &'a mut self,
        _counts: ChunkCounts,
        _fault: ChunkFaultProgress,
        component_state: &'a [ComponentStateEnvelope],
    ) -> BoxFuture<'a, Result<ChunkCommitReceipt, ChunkTransactionError>> {
        Box::pin(async move {
            if component_state.is_empty() {
                placeholder_receipt()
            } else {
                Err(ChunkTransactionError::ComponentStateUnsupported)
            }
        })
    }

    fn rollback(&mut self) -> BoxFuture<'_, Result<(), ChunkTransactionError>> {
        Box::pin(async { Ok(()) })
    }
}

/// A standalone [`ChunkTransactionManager`] with no durable execution graph.
///
/// Every [`begin`](ChunkTransactionManager::begin) call returns a fresh
/// [`StandaloneTransaction`]. [`inherited_progress`](ChunkTransactionManager::inherited_progress)
/// and [`inherited_component_state`](ChunkTransactionManager::inherited_component_state)
/// use their documented no-durable-state defaults, so this manager is
/// suitable for [`crate::TestStep`] and [`crate::ComponentFixture`] but not
/// for the [restart harness](crate::restart), which requires a durable
/// adapter that actually inherits progress across attempts.
#[derive(Clone, Copy, Debug, Default)]
pub struct StandaloneTransactions;

impl ChunkTransactionManager for StandaloneTransactions {
    fn begin(
        &self,
    ) -> BoxFuture<'_, Result<Box<dyn ChunkTransaction + '_>, ChunkTransactionError>> {
        Box::pin(async move { Ok(Box::new(StandaloneTransaction) as Box<dyn ChunkTransaction>) })
    }
}

/// A [`ChunkCompletion`] that acknowledges every commit without observing it.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoCompletion;

impl ChunkCompletion for NoCompletion {
    fn after_commit<'a>(
        &'a self,
        _context: ChunkCompletionContext<'a>,
    ) -> BoxFuture<'a, Result<ChunkCompletionOutcome, ChunkCompletionError>> {
        Box::pin(async { Ok(ChunkCompletionOutcome::Acknowledged) })
    }
}
