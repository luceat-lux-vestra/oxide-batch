//! The M6 `ItemStream` open/update/close contract (Gate C).
//!
//! An `ItemStream` is a component-scoped durable-state participant in the
//! chunk lifecycle: it restores its last committed [`ComponentStateEnvelope`]
//! before item work begins, prepares a candidate update at the commit
//! boundary, and closes after the step attempt's terminal outcome is known.
//! It is registered against a namespace ([`crate::ComponentStreamIdentity`])
//! at the point a chunk step is assembled -- the same way a reader,
//! processor, or writer's restart-relevant identity is declared through
//! [`crate::ChunkComponentRevisions`] rather than self-described by the
//! trait.
//!
//! The contract follows ADR-0008's shape exactly: one generic trait with an
//! explicit call lifetime and an opaque `impl Future` return, no
//! `async-trait`, no `Box::pin` in the public signature. Erasure is a
//! concrete [`BoxedStream`] handle over a private, dyn-compatible mirror,
//! mirroring [`crate::BoxedReader`]/[`crate::BoxedProcessor`]/
//! [`crate::BoxedWriter`] -- not a second public trait.

use std::error::Error;
use std::fmt;
use std::future::Future;

use crate::{ComponentStateEnvelope, FailureCategory, StopToken};

/// Borrowed call state for [`ItemStream::open`].
#[derive(Clone, Copy, Debug)]
pub struct StreamOpenContext<'a> {
    inherited: Option<&'a ComponentStateEnvelope>,
    stop: &'a StopToken,
}

impl<'a> StreamOpenContext<'a> {
    /// Constructs a stream-open call scope.
    ///
    /// `inherited` is the last committed envelope for this stream's
    /// namespace, already checksum-verified, decoded-schema/codec-matched,
    /// and migrated by the runtime. `None` means initial execution, not
    /// corruption: a corrupt or unsupported committed envelope fails before
    /// any stream is ever opened with it.
    #[must_use]
    pub const fn new(inherited: Option<&'a ComponentStateEnvelope>, stop: &'a StopToken) -> Self {
        Self { inherited, stop }
    }

    /// Borrows the last committed state for this stream's namespace, if any.
    #[must_use]
    pub const fn inherited_state(self) -> Option<&'a ComponentStateEnvelope> {
        self.inherited
    }

    /// Borrows the cooperative stop token.
    #[must_use]
    pub const fn stop_token(self) -> &'a StopToken {
        self.stop
    }
}

/// One stream-open call outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum StreamOpenOutcome {
    /// A last committed envelope was restored.
    Restored,
    /// No committed state existed: initial execution, not corruption.
    Initial,
}

/// Borrowed call state for [`ItemStream::update`].
#[derive(Clone, Copy, Debug)]
pub struct StreamUpdateContext<'a> {
    stop: &'a StopToken,
}

impl<'a> StreamUpdateContext<'a> {
    /// Constructs a stream-update call scope.
    #[must_use]
    pub const fn new(stop: &'a StopToken) -> Self {
        Self { stop }
    }

    /// Borrows the cooperative stop token.
    #[must_use]
    pub const fn stop_token(self) -> &'a StopToken {
        self.stop
    }
}

/// The coarse, non-sensitive runtime outcome an [`ItemStream`] observes at
/// close.
///
/// Deliberately excludes the full [`crate::ChunkExecutionReport`]: a stream's
/// close must react to whether the step committed, failed, stopped, or
/// reached an unknown outcome, never to internal report machinery or other
/// components' data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum StreamRuntimeOutcome {
    /// The step attempt's work committed durably.
    Committed,
    /// The step attempt ended in a known, typed failure.
    Failed,
    /// Cooperative stop ended the step attempt.
    Stopped,
    /// The step attempt's commit outcome is unknown.
    Unknown,
    /// A nested resource this stream owns reached its own logical boundary
    /// (a delegate reader exhausted, or a delegate writer rolled over)
    /// while the *enclosing* step attempt's terminal outcome is not yet
    /// known.
    ///
    /// Only ever reported by a component that nests other [`ItemStream`]s
    /// and must close a retiring delegate ahead of its own outer close --
    /// for example
    /// [`crate::item_components::multi_resource::MultiResourceReader`]/
    /// [`crate::item_components::multi_resource::MultiResourceWriter`]
    /// closing the resource they are transitioning away from. The
    /// top-level chunk runtime itself never reports this variant: it is
    /// strictly weaker than [`Self::Committed`] (the retiring resource's
    /// own prior work may already be durable, but the step attempt that
    /// observed its boundary has not yet reached a terminal outcome), so a
    /// delegate must not treat it as proof of a durable commit.
    ResourceBoundary,
}

/// Borrowed call state for [`ItemStream::close`].
#[derive(Clone, Copy, Debug)]
pub struct StreamCloseContext<'a> {
    stop: &'a StopToken,
    outcome: StreamRuntimeOutcome,
}

impl<'a> StreamCloseContext<'a> {
    /// Constructs a stream-close call scope.
    #[must_use]
    pub const fn new(stop: &'a StopToken, outcome: StreamRuntimeOutcome) -> Self {
        Self { stop, outcome }
    }

    /// Borrows the cooperative stop token.
    #[must_use]
    pub const fn stop_token(self) -> &'a StopToken {
        self.stop
    }

    /// Returns the step attempt's coarse terminal outcome.
    #[must_use]
    pub const fn outcome(self) -> StreamRuntimeOutcome {
        self.outcome
    }
}

/// One stream-close call outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum StreamCloseOutcome {
    /// The stream closed.
    Closed,
}

macro_rules! stream_error {
    ($name:ident, $message:literal) => {
        #[doc = $message]
        ///
        /// The adapter translates its own typed error into a stable
        /// [`FailureCategory`] at this boundary. The payload, display text,
        /// and source chain are dropped, so classification never inspects
        /// them.
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub struct $name {
            category: FailureCategory,
        }

        impl $name {
            /// Constructs a value-redacted [`FailureCategory::UserComponent`]
            /// failure.
            #[must_use]
            pub const fn new() -> Self {
                Self {
                    category: FailureCategory::UserComponent,
                }
            }

            /// Constructs a failure that declares its own stable category.
            #[must_use]
            pub const fn with_category(category: FailureCategory) -> Self {
                Self { category }
            }

            /// Classifies an arbitrary user error without retaining its
            /// payload or display text.
            #[must_use]
            pub fn from_error(error: impl Error + Send + Sync + 'static) -> Self {
                drop(error);
                Self::new()
            }

            /// Returns the stable category supplied by the adapter.
            #[must_use]
            pub const fn category(self) -> FailureCategory {
                self.category
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str($message)
            }
        }

        impl Error for $name {}
    };
}

stream_error!(StreamOpenError, "item stream open failed");
stream_error!(StreamUpdateError, "item stream update failed");
stream_error!(StreamCloseError, "item stream close failed");

/// One namespaced, durably checkpointed component-state participant.
///
/// # Lifecycle
///
/// - [`open`](Self::open) executes before any reader, processor, or writer
///   call in the step attempt. It receives only the last *committed*
///   envelope for this stream's namespace, or `None` for initial execution.
///   A required restoration failure here prevents every component
///   invocation in the attempt.
/// - [`update`](Self::update) runs once per committing chunk attempt, after
///   the writer has accepted the chunk and before the durable commit -- never
///   per item. Its result becomes authoritative only if the containing chunk
///   transaction commits; a rollback leaves the previously committed
///   envelope authoritative, and an update failure prevents the candidate
///   commit.
/// - [`close`](Self::close) runs once per step attempt for every stream that
///   opened successfully, regardless of how the attempt ends. A close
///   failure never suppresses another stream's close attempt and never
///   erases an earlier primary failure or already-committed chunks.
///
/// # Restartability
///
/// A stream backed by a [`crate::ComponentStateCodec`] whose
/// [`crate::RestartabilityDeclaration`] is
/// [`NotRestartable`](crate::RestartabilityDeclaration::NotRestartable)
/// makes its owning step unable to claim restartability, independent of
/// whether the step's reader checkpoint is itself restartable.
///
/// # Sensitivity
///
/// Committed and candidate envelopes redact their payload unconditionally in
/// `Debug`/`Display` (see [`ComponentStateEnvelope`]); an implementation must
/// not reintroduce the raw payload into an error, log, or trace.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not an OxideBatch `ItemStream`",
    label = "this component cannot participate in the ItemStream contract",
    note = "implement `ItemStream` with `async fn open`, `async fn update`, and `async fn close`",
    note = "the returned futures must be `Send`: do not hold a non-`Send` value across an await"
)]
pub trait ItemStream: Send + Sync {
    /// Restores last committed state, or begins initial execution.
    ///
    /// # Errors
    ///
    /// Returns [`StreamOpenError`] when required restoration fails. No
    /// reader, processor, or writer call starts for this step attempt when
    /// any registered stream's `open` fails.
    fn open<'a>(
        &'a self,
        context: StreamOpenContext<'a>,
    ) -> impl Future<Output = Result<StreamOpenOutcome, StreamOpenError>> + Send + 'a;

    /// Produces the candidate component-state envelope for the chunk about
    /// to commit.
    ///
    /// # Errors
    ///
    /// Returns [`StreamUpdateError`] when the candidate cannot be prepared.
    /// This prevents the containing chunk's candidate commit.
    fn update<'a>(
        &'a self,
        context: StreamUpdateContext<'a>,
    ) -> impl Future<Output = Result<ComponentStateEnvelope, StreamUpdateError>> + Send + 'a;

    /// Runs once for every stream that opened successfully in this step
    /// attempt.
    ///
    /// # Errors
    ///
    /// Returns [`StreamCloseError`] when close fails. A close failure never
    /// skips the close attempt on another already-opened stream, and never
    /// erases an earlier primary failure or already-committed chunks.
    fn close<'a>(
        &'a self,
        context: StreamCloseContext<'a>,
    ) -> impl Future<Output = Result<StreamCloseOutcome, StreamCloseError>> + Send + 'a;
}

/// The dyn-compatible mirror of the public `ItemStream` contract.
///
/// Nothing here is exported. Its only implementor is the blanket impl below,
/// so no external crate can observe or depend on this shape, and the single
/// `Box::pin` per call is the only boxing this erasure boundary introduces --
/// exactly the discipline ADR-0008 fixes for the item reader/processor/writer
/// contract.
mod sealed {
    use super::{
        ItemStream, StreamCloseContext, StreamCloseError, StreamCloseOutcome, StreamOpenContext,
        StreamOpenError, StreamOpenOutcome, StreamUpdateContext, StreamUpdateError,
    };
    use crate::{BoxFuture, ComponentStateEnvelope};

    pub trait StreamObject: Send + Sync {
        fn open_boxed<'a>(
            &'a self,
            context: StreamOpenContext<'a>,
        ) -> BoxFuture<'a, Result<StreamOpenOutcome, StreamOpenError>>;

        fn update_boxed<'a>(
            &'a self,
            context: StreamUpdateContext<'a>,
        ) -> BoxFuture<'a, Result<ComponentStateEnvelope, StreamUpdateError>>;

        fn close_boxed<'a>(
            &'a self,
            context: StreamCloseContext<'a>,
        ) -> BoxFuture<'a, Result<StreamCloseOutcome, StreamCloseError>>;
    }

    impl<S: ItemStream> StreamObject for S {
        fn open_boxed<'a>(
            &'a self,
            context: StreamOpenContext<'a>,
        ) -> BoxFuture<'a, Result<StreamOpenOutcome, StreamOpenError>> {
            Box::pin(self.open(context))
        }

        fn update_boxed<'a>(
            &'a self,
            context: StreamUpdateContext<'a>,
        ) -> BoxFuture<'a, Result<ComponentStateEnvelope, StreamUpdateError>> {
            Box::pin(self.update(context))
        }

        fn close_boxed<'a>(
            &'a self,
            context: StreamCloseContext<'a>,
        ) -> BoxFuture<'a, Result<StreamCloseOutcome, StreamCloseError>> {
            Box::pin(self.close(context))
        }
    }
}

/// A component-state stream of any concrete type, behind one dynamic
/// dispatch.
///
/// Constructing one is the explicit, greppable point where a registered
/// stream stops being monomorphized. Erasure does not change the stream's
/// namespace, definition identity, checkpoint selection, or restart
/// semantics: representation is never restart-relevant (ADR-0008).
pub struct BoxedStream(Box<dyn sealed::StreamObject>);

impl BoxedStream {
    /// Erases a concrete stream.
    pub fn new<S: ItemStream + 'static>(stream: S) -> Self {
        Self(Box::new(stream))
    }
}

impl ItemStream for BoxedStream {
    fn open<'a>(
        &'a self,
        context: StreamOpenContext<'a>,
    ) -> impl Future<Output = Result<StreamOpenOutcome, StreamOpenError>> + Send + 'a {
        self.0.open_boxed(context)
    }

    fn update<'a>(
        &'a self,
        context: StreamUpdateContext<'a>,
    ) -> impl Future<Output = Result<ComponentStateEnvelope, StreamUpdateError>> + Send + 'a {
        self.0.update_boxed(context)
    }

    fn close<'a>(
        &'a self,
        context: StreamCloseContext<'a>,
    ) -> impl Future<Output = Result<StreamCloseOutcome, StreamCloseError>> + Send + 'a {
        self.0.close_boxed(context)
    }
}
