//! Classifier-selected delegate components (#146).
//!
//! A classifier picks one delegate from a bounded, configured set at runtime,
//! keyed by a value derived from the item. Every delegate is stored under one
//! generic delegate type `D`: when delegates are naturally homogeneous
//! (multiple configurations of the same component type), `D` stays
//! monomorphized and the typed hot path is preserved; when delegates are
//! genuinely heterogeneous (different concrete types), naming `D` as
//! [`crate::BoxedProcessor`]/[`crate::BoxedWriter`] erases them all to the
//! *same* type at construction time, at the existing accepted erasure
//! boundary.
//!
//! # Why this cannot overclaim a delegate's capability
//!
//! Because every entry in the delegate map shares one Rust type `D`, the
//! wrapper's static declaration (its `Send`/`Sync`/lifetime bounds) is a
//! property of `D` itself, not of whichever key a given call happens to
//! select. A heterogeneous set is only representable by erasing every
//! variant to `Boxed*` first, which imposes the same (weakest) capability
//! uniformly on all of them; there is no way to construct a
//! [`ClassifyingProcessor`] or [`ClassifyingWriter`] whose static capability
//! is stronger than its least-capable delegate, because the type system
//! never sees "the selected delegate" as a distinct type from "every
//! delegate this wrapper could select" -- both are `D`.
//!
//! Delegate errors and stops propagate unchanged: neither type reclassifies,
//! swallows, or discriminates on which delegate produced them.

use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;

use crate::{
    BusinessTransaction, ItemProcessor, ItemWriter, ProcessContext, ProcessOutcome, ProcessorError,
    WriteContext, WriteOutcome, WriterError,
};

/// Derives a delegate-selection key from a borrowed item.
///
/// Blanket-implemented for `Fn(&I) -> K + Send + Sync` closures.
pub trait Classifier<I, K>: Send + Sync {
    /// Returns the key selecting this item's delegate.
    fn classify(&self, item: &I) -> K;
}

impl<I, K, F> Classifier<I, K> for F
where
    F: Fn(&I) -> K + Send + Sync,
{
    fn classify(&self, item: &I) -> K {
        self(item)
    }
}

/// Routes each item to one of a bounded, configured set of delegate
/// processors, selected at runtime by [`Classifier`].
///
/// # Contract
///
/// - **Input/output**: `I -> O`, same as every delegate `D`.
/// - **State/checkpoint**: stateless (assuming delegates are).
/// - **Ordering**: preserves order (one item in, one outcome out, exactly
///   like any processor).
/// - **Thread safety**: `Send + Sync` whenever `D` and `C` are; identical for
///   every key, because every delegate shares type `D` (see module docs).
/// - **Reentrancy**: fully reentrant when every delegate is.
/// - **Transaction/delivery**: not applicable.
/// - **Bounded resource**: the delegate set itself is bounded at
///   construction; this type adds no per-item buffering.
/// - **Cancellation**: honors the call-scoped stop token before classifying.
/// - **Close**: nothing to close.
/// - **Malformed input/failure**: an item whose key has no registered
///   delegate is a typed [`ProcessorError`]; a selected delegate's own
///   failure is returned unchanged, never reclassified.
/// - **Support tier**: first-party.
/// - **Evidence**: `crates/oxide-batch-test/tests/item_components_classify.rs`.
pub struct ClassifyingProcessor<I, O, K, D, C> {
    delegates: HashMap<K, D>,
    classifier: C,
    _marker: PhantomData<fn(&I) -> O>,
}

impl<I, O, K, D, C> ClassifyingProcessor<I, O, K, D, C>
where
    K: Eq + Hash,
    D: ItemProcessor<I, O>,
    C: Classifier<I, K>,
{
    /// Builds a classifying processor over a bounded, keyed delegate set.
    #[must_use]
    pub fn new(delegates: HashMap<K, D>, classifier: C) -> Self {
        Self {
            delegates,
            classifier,
            _marker: PhantomData,
        }
    }
}

impl<I, O, K, D, C> ItemProcessor<I, O> for ClassifyingProcessor<I, O, K, D, C>
where
    I: Send + Sync,
    O: Send + Sync,
    K: Eq + Hash + Send + Sync,
    D: ItemProcessor<I, O>,
    C: Classifier<I, K>,
{
    async fn process(
        &self,
        item: &I,
        context: ProcessContext<'_>,
    ) -> Result<ProcessOutcome<O>, ProcessorError> {
        if context.stop_token().is_stop_requested() {
            return Ok(ProcessOutcome::Stopped);
        }
        let key = self.classifier.classify(item);
        let delegate = self.delegates.get(&key).ok_or_else(ProcessorError::new)?;
        delegate.process(item, context).await
    }
}

/// Routes each item in a written batch to one of a bounded, configured set of
/// delegate writers, selected at runtime by [`Classifier`].
///
/// Each item is written to its selected delegate as its own single-item
/// batch, in the batch's original order: this preserves exact input ordering
/// across delegates (rather than grouping same-key items into larger
/// sub-batches) and needs no `Clone` bound on `O`. When enlisted, the single
/// `&mut dyn BusinessTransaction` is reborrowed sequentially per item exactly
/// as [`crate::item_components::FanOutWriter`] does.
///
/// # Contract
///
/// - **Input/output**: `[O]`, same as every delegate `D`.
/// - **State/checkpoint**: stateless.
/// - **Ordering**: preserves the batch's original item order exactly (see
///   above); order-sensitive if any delegate is.
/// - **Thread safety**: `Send + Sync` whenever `D` and `C` are, identically
///   for every key (see module docs).
/// - **Reentrancy**: fully reentrant when every delegate is.
/// - **Transaction/delivery**: never claims a stronger mode than every
///   delegate supports; see the reborrow guarantee above. Requires `O: Sync`
///   because each borrowed item is held across its delegate's `await`.
/// - **Bounded resource**: the delegate set is bounded at construction.
/// - **Cancellation**: checked once before the first item, and again via each
///   delegate's own outcome.
/// - **Close**: nothing to close.
/// - **Malformed input/failure**: an item whose key has no registered
///   delegate is a typed [`WriterError`]; a selected delegate's own failure
///   is returned unchanged.
/// - **Support tier**: first-party.
/// - **Evidence**: `crates/oxide-batch-test/tests/item_components_classify.rs`.
pub struct ClassifyingWriter<O, K, D, C> {
    delegates: HashMap<K, D>,
    classifier: C,
    _marker: PhantomData<fn(&O)>,
}

impl<O, K, D, C> ClassifyingWriter<O, K, D, C>
where
    K: Eq + Hash,
    D: ItemWriter<O>,
    C: Classifier<O, K>,
{
    /// Builds a classifying writer over a bounded, keyed delegate set.
    #[must_use]
    pub fn new(delegates: HashMap<K, D>, classifier: C) -> Self {
        Self {
            delegates,
            classifier,
            _marker: PhantomData,
        }
    }
}

impl<O, K, D, C> ItemWriter<O> for ClassifyingWriter<O, K, D, C>
where
    O: Sync,
    K: Eq + Hash + Send + Sync,
    D: ItemWriter<O>,
    C: Classifier<O, K>,
{
    async fn write<'a>(
        &'a self,
        items: &'a [O],
        mut context: WriteContext<'a>,
    ) -> Result<WriteOutcome, WriterError> {
        let stop = context.stop_token();
        if stop.is_stop_requested() {
            return Ok(WriteOutcome::Stopped);
        }
        let enlisted = context.is_enlisted();
        for item in items {
            let key = self.classifier.classify(item);
            let delegate = self.delegates.get(&key).ok_or_else(WriterError::new)?;
            let delegate_context = if enlisted {
                let transaction: &mut dyn BusinessTransaction =
                    context.transaction().ok_or_else(WriterError::new)?;
                WriteContext::enlisted(stop, transaction)
            } else {
                WriteContext::non_transactional(stop)
            };
            match delegate
                .write(std::slice::from_ref(item), delegate_context)
                .await?
            {
                WriteOutcome::Written => {}
                WriteOutcome::Stopped => return Ok(WriteOutcome::Stopped),
            }
        }
        Ok(WriteOutcome::Written)
    }
}
