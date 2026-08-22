//! Validator processor (#146).
//!
//! Validation failure is represented through the framework's typed
//! [`ProcessorError`] failure model, never a panic and never silent
//! filtering: an invalid item is a processor failure, exactly like any other
//! typed processor error, and participates in the same fault/skip machinery.

use std::marker::PhantomData;

use crate::{ProcessContext, ProcessOutcome, ProcessorError};

/// A synchronous validation rule over a borrowed item.
///
/// Blanket-implemented for `Fn(&I) -> Result<(), ProcessorError> + Send +
/// Sync` closures, so most callers never name this trait.
pub trait ItemValidator<I>: Send + Sync {
    /// Validates one borrowed item, returning a typed failure when invalid.
    ///
    /// # Errors
    ///
    /// Returns [`ProcessorError`] when `item` is invalid.
    fn validate(&self, item: &I) -> Result<(), ProcessorError>;
}

impl<I, F> ItemValidator<I> for F
where
    F: Fn(&I) -> Result<(), ProcessorError> + Send + Sync,
{
    fn validate(&self, item: &I) -> Result<(), ProcessorError> {
        self(item)
    }
}

/// A [`crate::ItemProcessor`] that validates its input and passes it through
/// unchanged when valid.
///
/// Compose validation with a transform by chaining
/// [`crate::item_components::ChainProcessor`] after this processor rather
/// than folding both concerns into one type.
///
/// # Contract
///
/// - **Input/output**: `I -> I`; unchanged when valid.
/// - **State/checkpoint**: stateless.
/// - **Ordering**: preserves order; never filters (an invalid item fails the
///   chunk attempt rather than being silently dropped).
/// - **Thread safety**: `Send + Sync` whenever `V: Send + Sync`.
/// - **Reentrancy**: fully reentrant when the validator is.
/// - **Transaction/delivery**: not applicable.
/// - **Bounded resource**: clones one item per call.
/// - **Cancellation**: honors the call-scoped stop token, checked before
///   validation runs.
/// - **Close**: nothing to close.
/// - **Sensitive diagnostics**: [`ProcessorError`] redacts any payload the
///   validator's closure captured; do not format the item into the error.
/// - **Malformed input**: an invalid item returns [`ProcessorError`], which
///   the framework's typed fault/skip machinery classifies exactly like any
///   other processor failure.
/// - **Support tier**: first-party.
/// - **Evidence**: `crates/oxide-batch-test/tests/item_components_basic.rs`.
pub struct ValidatingProcessor<I, V> {
    validator: V,
    _marker: PhantomData<fn(&I)>,
}

impl<I, V> ValidatingProcessor<I, V>
where
    V: ItemValidator<I>,
{
    /// Wraps a validation rule as a processor.
    pub const fn new(validator: V) -> Self {
        Self {
            validator,
            _marker: PhantomData,
        }
    }
}

impl<I, V> crate::ItemProcessor<I, I> for ValidatingProcessor<I, V>
where
    I: Clone + Send + Sync,
    V: ItemValidator<I>,
{
    async fn process(
        &self,
        item: &I,
        context: ProcessContext<'_>,
    ) -> Result<ProcessOutcome<I>, ProcessorError> {
        if context.stop_token().is_stop_requested() {
            return Ok(ProcessOutcome::Stopped);
        }
        self.validator.validate(item)?;
        Ok(ProcessOutcome::Item(item.clone()))
    }
}
