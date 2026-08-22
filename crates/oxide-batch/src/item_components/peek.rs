//! Peek reader decorator (#146).
//!
//! [`PeekReader`] looks ahead at the next logical item without corrupting
//! ordering or advancing durable progress incorrectly: it buffers at most one
//! outcome from its delegate, so repeated peeking never calls the delegate
//! again, and the following [`crate::ItemReader::read`] call consumes exactly
//! that buffered outcome exactly once.

use crate::{ItemReader, ReadContext, ReadOutcome, ReaderError};

enum Peeked<I> {
    Item(I),
    EndOfInput,
    Stopped,
}

/// The outcome of [`PeekReader::peek`], borrowing the buffered item rather
/// than consuming it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PeekOutcome<'a, I> {
    /// The next logical item, not yet consumed by `read`.
    Item(&'a I),
    /// The delegate reader is exhausted.
    EndOfInput,
    /// Cooperative stop was observed while looking ahead.
    Stopped,
}

/// A reader decorator that can look ahead at the next logical item.
///
/// # Contract
///
/// - **Input/output**: `I`, same as the delegate `R`.
/// - **State/checkpoint**: the buffered lookahead is in-memory only. It adds
///   no durable state and advances no checkpoint by itself: the delegate's
///   [`crate::ItemReader::read`] is called at most once before its outcome is
///   returned to the caller through a real `read` call, exactly like an
///   undecorated reader, so the chunk runtime's own read-ordinal/checkpoint
///   bookkeeping observes exactly the same call/outcome pairing it would
///   without peeking. If the delegate is itself paired with a
///   [`crate::ItemStream`] for durable position, that pairing is unaffected:
///   a restart resumes the delegate from its own last committed position,
///   and any not-yet-`read` peeked value is simply re-derived by the first
///   `peek`/`read` call of the new attempt (see restartability below).
/// - **Restartability**: exactly the delegate's; this wrapper never persists
///   the buffered lookahead across an attempt boundary, so an attempt that
///   ends before consuming a peeked value loses nothing durable -- it was
///   never reported as read to the framework.
/// - **Ordering**: preserves the delegate's order; `peek` never reorders or
///   skips.
/// - **Thread safety**: used exclusively (`&mut self`) like every reader.
/// - **Bounded resource**: buffers at most one outcome.
/// - **Cancellation**: a [`PeekOutcome::Stopped`]/[`ReadOutcome::Stopped`]
///   observed while peeking is cached and returned by the next `read` too,
///   consistent with the delegate's own stable stop behavior.
/// - **Close**: nothing to close.
/// - **Malformed input**: propagates the delegate's [`ReaderError`]
///   unchanged; an error is never cached, so the next `peek`/`read` retries
///   the delegate.
/// - **Support tier**: first-party.
/// - **Evidence**: `crates/oxide-batch-test/tests/item_components_decorators.rs`.
pub struct PeekReader<I, R> {
    inner: R,
    buffered: Option<Peeked<I>>,
}

impl<I, R> PeekReader<I, R>
where
    R: ItemReader<I>,
{
    /// Wraps a delegate reader with one-outcome lookahead.
    #[must_use]
    pub const fn new(inner: R) -> Self {
        Self {
            inner,
            buffered: None,
        }
    }

    /// Looks ahead at the next logical outcome without consuming it.
    ///
    /// Calling this repeatedly without an intervening [`ItemReader::read`]
    /// returns the same buffered outcome and calls the delegate at most once.
    ///
    /// # Errors
    ///
    /// Returns the delegate's [`ReaderError`] unchanged; a failed lookahead
    /// is never cached, so the next call retries the delegate.
    pub async fn peek(
        &mut self,
        context: ReadContext<'_>,
    ) -> Result<PeekOutcome<'_, I>, ReaderError> {
        if self.buffered.is_none() {
            let outcome = self.inner.read(context).await?;
            self.buffered = Some(match outcome {
                ReadOutcome::Item(item) => Peeked::Item(item),
                ReadOutcome::EndOfInput => Peeked::EndOfInput,
                ReadOutcome::Stopped => Peeked::Stopped,
            });
        }
        Ok(match self.buffered.as_ref() {
            Some(Peeked::Item(item)) => PeekOutcome::Item(item),
            Some(Peeked::EndOfInput) | None => PeekOutcome::EndOfInput,
            Some(Peeked::Stopped) => PeekOutcome::Stopped,
        })
    }
}

impl<I, R> ItemReader<I> for PeekReader<I, R>
where
    I: Send + 'static,
    R: ItemReader<I>,
{
    async fn read(&mut self, context: ReadContext<'_>) -> Result<ReadOutcome<I>, ReaderError> {
        if let Some(buffered) = self.buffered.take() {
            return Ok(match buffered {
                Peeked::Item(item) => ReadOutcome::Item(item),
                Peeked::EndOfInput => ReadOutcome::EndOfInput,
                Peeked::Stopped => ReadOutcome::Stopped,
            });
        }
        self.inner.read(context).await
    }
}
