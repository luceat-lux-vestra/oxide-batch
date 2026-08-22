//! Synchronization/thread-safety wrapper components (#146).
//!
//! These wrappers do not implement or redesign the M10 multi-threaded local
//! execution model; today's chunk runtime drives one component call at a
//! time. They also do not relax or grant any `Send`/`Sync` bound: a delegate
//! `P: ItemProcessor<I, O>` or `W: ItemWriter<O>` already requires
//! `Send + Sync` by [`crate::ItemProcessor`]/[`crate::ItemWriter`]'s own
//! supertraits, with or without this wrapper. What these wrappers establish
//! is a
//! *behavioral*, not a type-level, guarantee ADR-0008's composition rules
//! permit a wrapper to add on top of its delegate: a real mutual-exclusion
//! boundary around the delegate's call, enforced by an async-aware lock held
//! for the delegate's entire call (including any `.await` inside it), so a
//! delegate that is `Sync` at the type level (as the trait already demands)
//! but whose internal protocol or resource is only correct under strictly
//! serialized access can be shared safely by callers that might otherwise
//! invoke it concurrently (an application-owned executor, or a future M10
//! caller). This is the one case Gate E allows a wrapper to *strengthen*
//! rather than only meet: the wrapper's own synchronization is a capability
//! it genuinely provides itself, not one falsely inferred from the delegate.

use tokio::sync::Mutex;

use crate::{
    BusinessTransaction, ItemProcessor, ItemWriter, ProcessContext, ProcessOutcome, ProcessorError,
    WriteContext, WriteOutcome, WriterError,
};

/// Serializes calls to a delegate [`crate::ItemProcessor`] behind an
/// async-aware mutex.
///
/// # Contract
///
/// - **Input/output**: `I -> O`, same as the delegate `P`.
/// - **State/checkpoint**: none added; opaque pass-through to `P`.
/// - **Ordering**: does not itself reorder; under contention, calls complete
///   in the order they acquire the lock (first-acquired, first-served for a
///   `tokio::sync::Mutex`, not a hard FIFO guarantee).
/// - **Thread safety**: `Send + Sync` exactly when `P` is -- `P` already
///   requires `Send + Sync` as an [`crate::ItemProcessor`], so this wrapper
///   grants no new type-level bound. What it genuinely adds is a *runtime*
///   guarantee no `ItemProcessor` bound alone gives a caller: at most one
///   call to `P` is ever in flight, even under concurrent shared
///   invocation, for the delegate's entire call including any internal
///   `.await`. See
///   `synchronized_processor_allows_at_most_one_delegate_call_in_flight` in
///   the evidence file below, which measures actual concurrent in-flight
///   delegate calls rather than only observing each call's individual
///   result.
/// - **Reentrancy**: not reentrant; a delegate that calls back into the same
///   wrapper instance while already holding the lock deadlocks. This mirrors
///   the lock it uses and is not a framework-level limitation.
/// - **Transaction/delivery**: not applicable.
/// - **Bounded resource**: no queueing beyond the lock's own FIFO-ish
///   waiters; this wrapper adds no buffering.
/// - **Cancellation**: honors the call-scoped stop token exactly as `P`
///   would; a stop request does not cancel a wait already queued on the
///   lock.
/// - **Close**: nothing to close.
/// - **Malformed input**: propagates the delegate's [`ProcessorError`]
///   unchanged.
/// - **Support tier**: first-party.
/// - **Evidence**: `crates/oxide-batch-test/tests/item_components_decorators.rs`.
pub struct SynchronizedProcessor<P> {
    inner: Mutex<P>,
}

impl<P> SynchronizedProcessor<P> {
    /// Wraps a delegate processor behind an async mutex.
    #[must_use]
    pub fn new(inner: P) -> Self {
        Self {
            inner: Mutex::new(inner),
        }
    }
}

impl<I, O, P> ItemProcessor<I, O> for SynchronizedProcessor<P>
where
    I: Sync,
    O: Send + 'static,
    P: ItemProcessor<I, O>,
{
    async fn process(
        &self,
        item: &I,
        context: ProcessContext<'_>,
    ) -> Result<ProcessOutcome<O>, ProcessorError> {
        let delegate = self.inner.lock().await;
        delegate.process(item, context).await
    }
}

/// Serializes calls to a delegate [`crate::ItemWriter`] behind an
/// async-aware mutex.
///
/// # Contract
///
/// Identical to [`SynchronizedProcessor`], for the writer role: exactly one
/// call to the delegate `W` is in flight at a time, for its entire call
/// including any internal `.await`, including whatever portion of the
/// enlisted transaction call that entails.
///
/// - **Evidence**: `crates/oxide-batch-test/tests/item_components_decorators.rs`.
pub struct SynchronizedWriter<W> {
    inner: Mutex<W>,
}

impl<W> SynchronizedWriter<W> {
    /// Wraps a delegate writer behind an async mutex.
    #[must_use]
    pub fn new(inner: W) -> Self {
        Self {
            inner: Mutex::new(inner),
        }
    }
}

impl<O, W> ItemWriter<O> for SynchronizedWriter<W>
where
    O: Sync,
    W: ItemWriter<O>,
{
    async fn write<'a>(
        &'a self,
        items: &'a [O],
        mut context: WriteContext<'a>,
    ) -> Result<WriteOutcome, WriterError> {
        let stop = context.stop_token();
        let enlisted = context.is_enlisted();
        let delegate = self.inner.lock().await;
        let delegate_context = if enlisted {
            let transaction: &mut dyn BusinessTransaction =
                context.transaction().ok_or_else(WriterError::new)?;
            WriteContext::enlisted(stop, transaction)
        } else {
            WriteContext::non_transactional(stop)
        };
        delegate.write(items, delegate_context).await
    }
}
