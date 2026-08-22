//! Filter processor (#146).
//!
//! Filtering uses the existing [`ProcessOutcome::Filtered`] contract, never
//! an error and never a magic output value.

use std::marker::PhantomData;

use crate::{ProcessContext, ProcessOutcome, ProcessorError};

/// A predicate deciding whether an item is kept.
///
/// Blanket-implemented for `Fn(&I) -> bool + Send + Sync` closures.
pub trait ItemFilter<I>: Send + Sync {
    /// Returns `true` to keep the item, `false` to filter it.
    fn keep(&self, item: &I) -> bool;
}

impl<I, F> ItemFilter<I> for F
where
    F: Fn(&I) -> bool + Send + Sync,
{
    fn keep(&self, item: &I) -> bool {
        self(item)
    }
}

/// A [`crate::ItemProcessor`] that keeps or filters an item by predicate,
/// using [`ProcessOutcome::Filtered`] rather than an error or a sentinel
/// value.
///
/// # Contract
///
/// - **Input/output**: `I -> I`, unchanged when kept.
/// - **State/checkpoint**: stateless.
/// - **Ordering**: preserves the order of kept items; filtered items are
///   dropped, never reordered.
/// - **Thread safety**: `Send + Sync` whenever `F: Send + Sync`.
/// - **Reentrancy**: fully reentrant when the predicate is.
/// - **Transaction/delivery**: not applicable.
/// - **Bounded resource**: clones one item per kept call.
/// - **Cancellation**: honors the call-scoped stop token, checked before the
///   predicate runs.
/// - **Close**: nothing to close.
/// - **Sensitive diagnostics**: none; this component produces no error path
///   of its own.
/// - **Malformed input**: not applicable; the predicate cannot fail. A
///   predicate that must reject invalid input rather than merely drop it
///   should use [`crate::item_components::ValidatingProcessor`] instead.
/// - **Support tier**: first-party.
/// - **Evidence**: `crates/oxide-batch-test/tests/item_components_basic.rs`.
pub struct FilterProcessor<I, F> {
    predicate: F,
    _marker: PhantomData<fn(&I)>,
}

impl<I, F> FilterProcessor<I, F>
where
    F: ItemFilter<I>,
{
    /// Wraps a keep/filter predicate as a processor.
    pub const fn new(predicate: F) -> Self {
        Self {
            predicate,
            _marker: PhantomData,
        }
    }
}

impl<I, F> crate::ItemProcessor<I, I> for FilterProcessor<I, F>
where
    I: Clone + Send + Sync,
    F: ItemFilter<I>,
{
    async fn process(
        &self,
        item: &I,
        context: ProcessContext<'_>,
    ) -> Result<ProcessOutcome<I>, ProcessorError> {
        if context.stop_token().is_stop_requested() {
            return Ok(ProcessOutcome::Stopped);
        }
        if self.predicate.keep(item) {
            Ok(ProcessOutcome::Item(item.clone()))
        } else {
            Ok(ProcessOutcome::Filtered)
        }
    }
}
