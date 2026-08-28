//! An ergonomic single-step chunk pipeline builder (#152). See
//! [`ChunkPipelineBuilder`]'s own documentation for the full design rationale.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use crate::chunk_runtime::completion_policy_component_revisions;
use crate::{
    AdaptiveCompletionPolicy, BoxFuture, ChunkCompletion, ChunkCompletionContext,
    ChunkCompletionError, ChunkCompletionOutcome, ChunkComponentRevisions, ChunkJob, ChunkListener,
    ChunkRestartContract, ChunkSize, ChunkStep, ChunkTransactionManager, CompletionPolicy,
    ComponentRevision, ComponentStreamIdentity, DefinitionError, DefinitionRevision, FaultRuntime,
    ItemListenerSet, ItemProcessor, ItemReader, ItemStream, ItemWriter, JobName, StepComponents,
    StepExecutionListener, StepName, StreamStateContract,
};

/// A [`ChunkCompletion`] that acknowledges every commit without observing it.
///
/// [`ChunkCompletion::after_commit`] exists to notify an external system
/// after a durable commit without ever becoming a correctness authority, so
/// doing nothing is always a valid implementation. Most pipelines have no
/// post-commit notification need; this is the safe default
/// [`ChunkPipelineBuilder::new`] installs so a caller does not have to
/// hand-write this exact type to get a working pipeline. Install a real
/// observer with [`ChunkPipelineBuilder::with_completion`].
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopChunkCompletion;

impl ChunkCompletion for NoopChunkCompletion {
    fn after_commit<'a>(
        &'a self,
        _context: ChunkCompletionContext<'a>,
    ) -> BoxFuture<'a, Result<ChunkCompletionOutcome, ChunkCompletionError>> {
        Box::pin(async { Ok(ChunkCompletionOutcome::Acknowledged) })
    }
}

/// An ergonomic single-step chunk pipeline builder (#152).
///
/// [`ChunkPipelineBuilder`] assembles a [`ChunkStep`] and its matching
/// [`ChunkComponentRevisions`] from one builder instead of two independently
/// chained ones. It lowers to exactly the same [`ChunkStep`], [`ChunkJob`],
/// and [`crate::FlowJob`] types every other construction path uses -- ADR-0008's
/// one chunk driver, one set of transaction/checkpoint/restart/completion-policy/
/// listener/fault-tolerance semantics -- so it is a configuration-time
/// convenience, never a second execution path.
///
/// # The problem this solves
///
/// Before this builder, a stateful component's [`crate::ItemStream`]
/// namespace had to be typed twice: once into
/// [`ChunkComponentRevisions::with_stream_revision`] (the restart-relevant
/// definition side) and once into [`ChunkStep::with_item_stream`] (the
/// runtime side). Nothing at compile time proved the two independently typed
/// [`ComponentStreamIdentity`] values were the same one; a typo surfaced only
/// as a [`DefinitionError`] at [`ChunkJob::new`] or
/// [`crate::FlowJob::with_chunk_step`] time. [`ChunkPipelineBuilder::with_stream`] takes the
/// identity once and updates both sides from that single value, so the two
/// sides cannot diverge.
///
/// Similarly, installing a [`CompletionPolicy`] required a separate,
/// easy-to-forget call to the free function [`crate::completion_policy_revision`]
/// and folding its result into the revisions object by hand.
/// [`ChunkPipelineBuilder::revisions`] computes it automatically from whatever policy is
/// currently installed, using the exact same computation
/// [`ChunkJob::new`] already performs internally -- never a second,
/// independently maintained fingerprint.
///
/// Every other builder method here is a thin, non-duplicating forward onto
/// the identical [`ChunkStep`] method of the same name; none of the
/// underlying validation (duplicate/undeclared stream detection, delivery-mode
/// agreement, restart-compatibility checks) is reimplemented, so behavior is
/// identical to assembling a [`ChunkStep`] and [`ChunkComponentRevisions`] by
/// hand and passing them through the same [`ChunkJob::new`] or
/// [`crate::FlowJob::with_chunk_step`] this builder still requires.
///
/// # Typed vs. Boxed
///
/// [`ChunkPipelineBuilder<I, O, R, P, W>`](ChunkPipelineBuilder) is generic
/// over the reader, processor, and writer exactly like [`ChunkStep`] itself.
/// Use concrete component types (the default) when they are known statically:
/// the pipeline stays monomorphized, with no per-item allocation on the hot
/// path (ADR-0008). Instantiate the same builder with
/// [`crate::BoxedReader`], [`crate::BoxedProcessor`], and
/// [`crate::BoxedWriter`] when a component is chosen at runtime, resolved by
/// name, or stored heterogeneously -- erasure is a decision made once, where
/// the handle is constructed, not a second builder or a second execution
/// path. Prefer typed unless dynamic assembly is the actual requirement:
/// `Boxed*` is not the default merely because its type signature is shorter.
///
/// [`crate::BoxedReader`], [`crate::BoxedProcessor`], and
/// [`crate::BoxedWriter`] already implement the plain [`ItemReader`],
/// [`ItemProcessor`], and [`ItemWriter`] traits (ADR-0008: erasure is a
/// concrete type, not a second trait), so the identical builder accepts them
/// with no special-casing:
///
/// ```
/// use std::sync::Arc;
///
/// use oxide_batch::item_components::{IdentityProcessor, IterReader, NoopWriter};
/// use oxide_batch::{
///     BoxedProcessor, BoxedReader, BoxedWriter, ChunkComponentRevisions,
///     ChunkDeliveryMode, ChunkPipelineBuilder, ChunkRestartContract, ChunkSize,
///     ComponentRevision, StateSchemaId, StateSchemaVersion, StepName,
/// };
/// # use std::error::Error;
/// # use oxide_batch::{
/// #     BoxFuture, BusinessTransaction, ChunkCommitReceipt, ChunkCounts, ChunkFaultProgress,
/// #     ChunkTransaction, ChunkTransactionError, ChunkTransactionManager,
/// # };
/// # struct NoTransaction;
/// # impl ChunkTransaction for NoTransaction {
/// #     fn business_transaction(&mut self) -> Option<&mut dyn BusinessTransaction> {
/// #         None
/// #     }
/// #     fn commit(
/// #         &mut self,
/// #         _counts: ChunkCounts,
/// #         _fault: ChunkFaultProgress,
/// #     ) -> BoxFuture<'_, Result<ChunkCommitReceipt, ChunkTransactionError>> {
/// #         Box::pin(async { Err(ChunkTransactionError::NotCommitted) })
/// #     }
/// #     fn rollback(&mut self) -> BoxFuture<'_, Result<(), ChunkTransactionError>> {
/// #         Box::pin(async { Ok(()) })
/// #     }
/// # }
/// # struct NoTransactions;
/// # impl ChunkTransactionManager for NoTransactions {
/// #     fn begin(
/// #         &self,
/// #     ) -> BoxFuture<'_, Result<Box<dyn ChunkTransaction + '_>, ChunkTransactionError>> {
/// #         Box::pin(async { Ok(Box::new(NoTransaction) as Box<dyn ChunkTransaction>) })
/// #     }
/// # }
///
/// let restart = ChunkRestartContract::new(
///     StateSchemaId::new("example.checkpoint")?,
///     StateSchemaVersion::new(1)?,
///     StateSchemaId::new("example.context")?,
///     StateSchemaVersion::new(1)?,
///     ChunkDeliveryMode::AtLeastOnce,
/// );
///
/// // Same builder, same chunk driver -- only the component types changed.
/// let (step, revisions) = ChunkPipelineBuilder::new(
///     StepName::new("import")?,
///     ChunkSize::new(10)?,
///     BoxedReader::new(IterReader::new([1u64, 2, 3])),
///     ComponentRevision::new("reader-v1")?,
///     BoxedProcessor::new(IdentityProcessor),
///     ComponentRevision::new("processor-v1")?,
///     BoxedWriter::new(NoopWriter),
///     ComponentRevision::new("writer-v1")?,
///     ComponentRevision::new("checkpoint-v1")?,
///     restart,
///     Arc::new(NoTransactions),
/// )
/// .build()?;
/// assert_eq!(revisions.reader(), &ComponentRevision::new("reader-v1")?);
/// assert_eq!(step.name(), &StepName::new("import")?);
/// # Ok::<(), Box<dyn Error>>(())
/// ```
///
/// # Restart semantics stay explicit
///
/// This builder never derives a reader, processor, writer, checkpoint, or
/// stream revision on a caller's behalf: each is a required argument or an
/// explicit [`ChunkPipelineBuilder::with_stream`] call, exactly as restart-relevant
/// configuration must be (see
/// `docs/architecture/decisions/0004-job-definition-restart-compatibility.md`).
/// The only value this builder computes automatically is the
/// completion-policy revision, and only because it is a pure, deterministic
/// function of a policy the caller already installed -- never a value that
/// could silently omit semantic configuration. [`ChunkPipelineBuilder::build`] and
/// [`ChunkPipelineBuilder::revisions`] preserve the documented [`ChunkJob`]-vs-[`crate::FlowJob`]
/// asymmetry: a [`crate::FlowJob`] step's plan is compiled from
/// [`ChunkComponentRevisions`] before the bound [`ChunkStep`] (or the policy
/// it installs) exists, so the caller must still call [`ChunkPipelineBuilder::revisions`] (or
/// [`ChunkPipelineBuilder::flow_step_components`]) to compile the [`crate::FlowGraph`]
/// *before* calling [`ChunkPipelineBuilder::build`] to obtain the matching step -- this
/// builder makes that pattern computation-free and duplication-free, not
/// optional.
///
/// # Out of scope
///
/// This builder is configuration ergonomics over the existing chunk
/// architecture, not a new configuration framework. It does not add a
/// scope/late-binding system, an expression language, or a general
/// configuration DSL -- those remain out of scope through at least M7.
///
/// See [`ChunkPipelineBuilder::new`] for a runnable typed-pipeline example,
/// and its sibling methods for stream registration, completion-policy
/// attachment, and `Boxed*` construction.
pub struct ChunkPipelineBuilder<I, O, R, P, W> {
    step: ChunkStep<I, O, R, P, W>,
    revisions: ChunkComponentRevisions,
    /// Stream revisions declared for the *currently installed* completion
    /// policy's own runtime registrations, kept separate from `revisions`
    /// and replaced wholesale -- never merged in -- whenever
    /// [`ChunkPipelineBuilder::with_completion_policy`] (or its adaptive
    /// alias) installs a new policy, exactly mirroring how
    /// [`ChunkStep::with_completion_policy`] replaces that same policy's
    /// runtime registrations on the other side. A manually declared stream
    /// revision (via [`ChunkPipelineBuilder::with_stream`]) lives in
    /// `revisions` instead and is never touched by a policy replacement.
    completion_policy_stream_revisions: BTreeMap<ComponentStreamIdentity, ComponentRevision>,
}

impl<I, O, R, P, W> ChunkPipelineBuilder<I, O, R, P, W>
where
    R: ItemReader<I>,
    P: ItemProcessor<I, O>,
    W: ItemWriter<O>,
{
    /// Constructs a pipeline builder from its required reader, processor,
    /// writer, and restart-relevant revisions.
    ///
    /// `reader_revision`, `processor_revision`, `writer_revision`,
    /// `checkpoint_revision`, and `restart` are exactly
    /// [`ChunkComponentRevisions::new`]'s arguments: this builder never
    /// derives them, so restart-relevant configuration remains as explicit as
    /// assembling a [`ChunkStep`] and [`ChunkComponentRevisions`] by hand.
    /// The post-commit completion observer defaults to
    /// [`NoopChunkCompletion`]; override it with [`Self::with_completion`].
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use oxide_batch::item_components::{IdentityProcessor, IterReader, NoopWriter};
    /// use oxide_batch::{
    ///     BoxFuture, BusinessTransaction, ChunkComponentRevisions, ChunkCounts,
    ///     ChunkDeliveryMode, ChunkFaultProgress, ChunkPipelineBuilder, ChunkRestartContract,
    ///     ChunkSize, ChunkTransaction, ChunkTransactionError, ChunkTransactionManager,
    ///     ChunkCommitReceipt, ComponentRevision, DefinitionRevision, JobName, StateSchemaId,
    ///     StateSchemaVersion, StepName,
    /// };
    ///
    /// // A minimal in-memory transaction manager. Its bodies are never
    /// // invoked by construction alone -- ChunkJob::new only validates and
    /// // compiles the definition.
    /// struct NoTransaction;
    /// impl ChunkTransaction for NoTransaction {
    ///     fn business_transaction(&mut self) -> Option<&mut dyn BusinessTransaction> {
    ///         None
    ///     }
    ///     fn commit(
    ///         &mut self,
    ///         _counts: ChunkCounts,
    ///         _fault: ChunkFaultProgress,
    ///     ) -> BoxFuture<'_, Result<ChunkCommitReceipt, ChunkTransactionError>> {
    ///         Box::pin(async { Err(ChunkTransactionError::NotCommitted) })
    ///     }
    ///     fn rollback(&mut self) -> BoxFuture<'_, Result<(), ChunkTransactionError>> {
    ///         Box::pin(async { Ok(()) })
    ///     }
    /// }
    /// struct NoTransactions;
    /// impl ChunkTransactionManager for NoTransactions {
    ///     fn begin(
    ///         &self,
    ///     ) -> BoxFuture<'_, Result<Box<dyn ChunkTransaction + '_>, ChunkTransactionError>> {
    ///         Box::pin(async { Ok(Box::new(NoTransaction) as Box<dyn ChunkTransaction>) })
    ///     }
    /// }
    ///
    /// let restart = ChunkRestartContract::new(
    ///     StateSchemaId::new("example.checkpoint")?,
    ///     StateSchemaVersion::new(1)?,
    ///     StateSchemaId::new("example.context")?,
    ///     StateSchemaVersion::new(1)?,
    ///     ChunkDeliveryMode::AtLeastOnce,
    /// );
    ///
    /// let job = ChunkPipelineBuilder::new(
    ///     StepName::new("import")?,
    ///     ChunkSize::new(10)?,
    ///     IterReader::new([1u64, 2, 3]),
    ///     ComponentRevision::new("reader-v1")?,
    ///     IdentityProcessor,
    ///     ComponentRevision::new("processor-v1")?,
    ///     NoopWriter,
    ///     ComponentRevision::new("writer-v1")?,
    ///     ComponentRevision::new("checkpoint-v1")?,
    ///     restart,
    ///     Arc::new(NoTransactions),
    /// )
    /// .build_chunk_job(JobName::new("import_job")?, DefinitionRevision::new("v1")?)?;
    ///
    /// assert_eq!(job.step_name(), &StepName::new("import")?);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: StepName,
        size: ChunkSize,
        reader: R,
        reader_revision: ComponentRevision,
        processor: P,
        processor_revision: ComponentRevision,
        writer: W,
        writer_revision: ComponentRevision,
        checkpoint_revision: ComponentRevision,
        restart: ChunkRestartContract,
        transactions: Arc<dyn ChunkTransactionManager>,
    ) -> Self {
        let step = ChunkStep::new(
            name,
            size,
            reader,
            processor,
            writer,
            transactions,
            Arc::new(NoopChunkCompletion),
        );
        let revisions = ChunkComponentRevisions::new(
            reader_revision,
            processor_revision,
            writer_revision,
            checkpoint_revision,
            restart,
        );
        Self {
            step,
            revisions,
            completion_policy_stream_revisions: BTreeMap::new(),
        }
    }

    /// Replaces the default [`NoopChunkCompletion`] post-commit observer.
    #[must_use]
    pub fn with_completion(mut self, completion: Arc<dyn ChunkCompletion>) -> Self {
        self.step = self.step.with_completion(completion);
        self
    }

    /// Installs an early-completion policy.
    ///
    /// Forwards to [`ChunkStep::with_completion_policy`] unchanged, including
    /// its `ItemStream` auto-registration for the policy's own state (see
    /// [`CompletionPolicy::stream_registrations`]). [`Self::revisions`] and
    /// [`Self::build`] fold this policy's restart-relevant revision in
    /// automatically.
    ///
    /// If `policy` reports any [`CompletionPolicy::stream_registrations`]
    /// (for example, an installed [`AdaptiveCompletionPolicy`]), declare each
    /// one's restart-relevant revision with
    /// [`Self::with_completion_policy_stream_revision`], using the exact
    /// identity the policy itself reports (its own `identity()` accessor,
    /// where one exists). [`ChunkStep::with_completion_policy`] registers
    /// that stream on the *runtime* side automatically, but the matching
    /// *definition*-side revision is application-chosen versioning this
    /// builder cannot derive from the identity alone, exactly like a stream
    /// registered through [`Self::with_stream`] -- the difference here is
    /// only that the runtime half is already handled, so declaring the
    /// revision is the one remaining step. Omitting it surfaces as
    /// [`DefinitionError::RuntimeStreamNotDeclared`] from [`Self::build`] or
    /// [`Self::revisions`], not a silently dropped registration.
    ///
    /// # Examples
    ///
    /// The restart-relevant fingerprint is not hidden by this convenience --
    /// it is present in [`Self::revisions`]'s output and matches
    /// [`crate::completion_policy_revision`] computed by hand for the same
    /// policy:
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use oxide_batch::item_components::{IdentityProcessor, IterReader, NoopWriter};
    /// use oxide_batch::{
    ///     ChunkDeliveryMode, ChunkPipelineBuilder, ChunkRestartContract, ChunkSize,
    ///     ComponentRevision, ItemCountCompletionPolicy, StateSchemaId, StateSchemaVersion,
    ///     StepName, completion_policy_revision,
    /// };
    /// # use std::error::Error;
    /// # use oxide_batch::{
    /// #     BoxFuture, BusinessTransaction, ChunkCommitReceipt, ChunkCounts, ChunkFaultProgress,
    /// #     ChunkTransaction, ChunkTransactionError, ChunkTransactionManager,
    /// # };
    /// # struct NoTransaction;
    /// # impl ChunkTransaction for NoTransaction {
    /// #     fn business_transaction(&mut self) -> Option<&mut dyn BusinessTransaction> {
    /// #         None
    /// #     }
    /// #     fn commit(
    /// #         &mut self,
    /// #         _counts: ChunkCounts,
    /// #         _fault: ChunkFaultProgress,
    /// #     ) -> BoxFuture<'_, Result<ChunkCommitReceipt, ChunkTransactionError>> {
    /// #         Box::pin(async { Err(ChunkTransactionError::NotCommitted) })
    /// #     }
    /// #     fn rollback(&mut self) -> BoxFuture<'_, Result<(), ChunkTransactionError>> {
    /// #         Box::pin(async { Ok(()) })
    /// #     }
    /// # }
    /// # struct NoTransactions;
    /// # impl ChunkTransactionManager for NoTransactions {
    /// #     fn begin(
    /// #         &self,
    /// #     ) -> BoxFuture<'_, Result<Box<dyn ChunkTransaction + '_>, ChunkTransactionError>> {
    /// #         Box::pin(async { Ok(Box::new(NoTransaction) as Box<dyn ChunkTransaction>) })
    /// #     }
    /// # }
    ///
    /// let restart = ChunkRestartContract::new(
    ///     StateSchemaId::new("example.checkpoint")?,
    ///     StateSchemaVersion::new(1)?,
    ///     StateSchemaId::new("example.context")?,
    ///     StateSchemaVersion::new(1)?,
    ///     ChunkDeliveryMode::AtLeastOnce,
    /// );
    /// let policy = Arc::new(ItemCountCompletionPolicy::new(ChunkSize::new(5)?));
    ///
    /// let builder = ChunkPipelineBuilder::new(
    ///     StepName::new("import")?,
    ///     ChunkSize::new(10)?,
    ///     IterReader::new([1u64, 2, 3]),
    ///     ComponentRevision::new("reader-v1")?,
    ///     IdentityProcessor,
    ///     ComponentRevision::new("processor-v1")?,
    ///     NoopWriter,
    ///     ComponentRevision::new("writer-v1")?,
    ///     ComponentRevision::new("checkpoint-v1")?,
    ///     restart,
    ///     Arc::new(NoTransactions),
    /// )
    /// .with_completion_policy(Arc::clone(&policy) as _);
    ///
    /// let expected = completion_policy_revision(policy.as_ref())?;
    /// assert_eq!(
    ///     builder.revisions()?.completion_policy_revision(),
    ///     Some(&expected)
    /// );
    /// # Ok::<(), Box<dyn Error>>(())
    /// ```
    #[must_use]
    pub fn with_completion_policy(mut self, policy: Arc<dyn CompletionPolicy>) -> Self {
        self.completion_policy_stream_revisions.clear();
        self.step = self.step.with_completion_policy(policy);
        self
    }

    /// Installs one [`AdaptiveCompletionPolicy`] instance as this step's
    /// completion policy.
    ///
    /// A thin, discoverable alias for [`Self::with_completion_policy`],
    /// mirroring [`ChunkStep::with_adaptive_completion_policy`].
    #[must_use]
    pub fn with_adaptive_completion_policy(
        mut self,
        policy: Arc<AdaptiveCompletionPolicy>,
    ) -> Self {
        self.completion_policy_stream_revisions.clear();
        self.step = self.step.with_adaptive_completion_policy(policy);
        self
    }

    /// Declares the restart-relevant revision for one `ItemStream` namespace
    /// a completion policy already registered on the runtime step through
    /// [`Self::with_completion_policy`] (or
    /// [`Self::with_adaptive_completion_policy`]).
    ///
    /// Unlike [`Self::with_stream`], this only updates the definition-side
    /// revisions -- the runtime registration already happened automatically
    /// when the policy was installed (see
    /// [`CompletionPolicy::stream_registrations`]), so calling
    /// [`Self::with_stream`] for the same identity would register it a
    /// second time and fail with
    /// [`DefinitionError::DuplicateRuntimeStream`].
    ///
    /// Every declaration made here is scoped to the *currently installed*
    /// policy: calling [`Self::with_completion_policy`] (or
    /// [`Self::with_adaptive_completion_policy`]) again to replace it
    /// discards every revision declared by this method since the previous
    /// installation -- mirroring [`ChunkStep::with_completion_policy`]'s own
    /// replacement of that policy's runtime registrations -- while a stream
    /// revision declared through [`Self::with_stream`] is never affected by
    /// a policy replacement. This also composes correctly for a policy
    /// nested inside a [`crate::CompositeCompletionPolicy`] at any depth, and
    /// for any custom [`CompletionPolicy`] implementation: declare one
    /// revision per identity the installed policy reports through
    /// [`CompletionPolicy::stream_registrations`], regardless of how many or
    /// how they are nested.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use oxide_batch::item_components::{IdentityProcessor, IterReader, NoopWriter};
    /// use oxide_batch::{
    ///     AdaptiveBounds, AdaptiveCompletionPolicy, ChunkDeliveryMode, ChunkPipelineBuilder,
    ///     ChunkRestartContract, ChunkSize, ChunkTimeThreshold, ComponentRevision, JobName,
    ///     StateSchemaId, StateSchemaVersion, StepName, SystemClock,
    /// };
    /// # use std::error::Error;
    /// # use std::time::Duration;
    /// # use oxide_batch::{
    /// #     BoxFuture, BusinessTransaction, ChunkCommitReceipt, ChunkCounts, ChunkFaultProgress,
    /// #     ChunkTransaction, ChunkTransactionError, ChunkTransactionManager, DefinitionRevision,
    /// # };
    /// # struct NoTransaction;
    /// # impl ChunkTransaction for NoTransaction {
    /// #     fn business_transaction(&mut self) -> Option<&mut dyn BusinessTransaction> {
    /// #         None
    /// #     }
    /// #     fn commit(
    /// #         &mut self,
    /// #         _counts: ChunkCounts,
    /// #         _fault: ChunkFaultProgress,
    /// #     ) -> BoxFuture<'_, Result<ChunkCommitReceipt, ChunkTransactionError>> {
    /// #         Box::pin(async { Err(ChunkTransactionError::NotCommitted) })
    /// #     }
    /// #     fn rollback(&mut self) -> BoxFuture<'_, Result<(), ChunkTransactionError>> {
    /// #         Box::pin(async { Ok(()) })
    /// #     }
    /// # }
    /// # struct NoTransactions;
    /// # impl ChunkTransactionManager for NoTransactions {
    /// #     fn begin(
    /// #         &self,
    /// #     ) -> BoxFuture<'_, Result<Box<dyn ChunkTransaction + '_>, ChunkTransactionError>> {
    /// #         Box::pin(async { Ok(Box::new(NoTransaction) as Box<dyn ChunkTransaction>) })
    /// #     }
    /// # }
    ///
    /// let restart = ChunkRestartContract::new(
    ///     StateSchemaId::new("example.checkpoint")?,
    ///     StateSchemaVersion::new(1)?,
    ///     StateSchemaId::new("example.context")?,
    ///     StateSchemaVersion::new(1)?,
    ///     ChunkDeliveryMode::AtLeastOnce,
    /// );
    /// let policy = AdaptiveCompletionPolicy::new(
    ///     oxide_batch::ComponentStreamIdentity::new("import.adaptive_size")?,
    ///     AdaptiveBounds::new(ChunkSize::new(2)?, ChunkSize::new(50)?)?,
    ///     ChunkTimeThreshold::new(Duration::from_millis(200))?,
    ///     Arc::new(SystemClock),
    /// );
    ///
    /// // Without the line below, `build_chunk_job` fails with
    /// // `DefinitionError::RuntimeStreamNotDeclared`, because installing the
    /// // policy above already registered its stream on the runtime step.
    /// let job = ChunkPipelineBuilder::new(
    ///     StepName::new("import")?,
    ///     ChunkSize::new(10)?,
    ///     IterReader::new([1u64, 2, 3]),
    ///     ComponentRevision::new("reader-v1")?,
    ///     IdentityProcessor,
    ///     ComponentRevision::new("processor-v1")?,
    ///     NoopWriter,
    ///     ComponentRevision::new("writer-v1")?,
    ///     ComponentRevision::new("checkpoint-v1")?,
    ///     restart,
    ///     Arc::new(NoTransactions),
    /// )
    /// .with_adaptive_completion_policy(Arc::clone(&policy))
    /// .with_completion_policy_stream_revision(
    ///     policy.identity().clone(),
    ///     ComponentRevision::new("adaptive-v1")?,
    /// )
    /// .build_chunk_job(JobName::new("import_job")?, DefinitionRevision::new("v1")?)?;
    ///
    /// assert_eq!(job.step_name(), &StepName::new("import")?);
    /// # Ok::<(), Box<dyn Error>>(())
    /// ```
    #[must_use]
    pub fn with_completion_policy_stream_revision(
        mut self,
        identity: ComponentStreamIdentity,
        revision: ComponentRevision,
    ) -> Self {
        self.completion_policy_stream_revisions
            .insert(identity, revision);
        self
    }

    /// Registers a chunk listener in deterministic before-order.
    #[must_use]
    pub fn with_chunk_listener(mut self, listener: Arc<dyn ChunkListener>) -> Self {
        self.step = self.step.with_chunk_listener(listener);
        self
    }

    /// Installs the authoritative item, retry, and skip listener families.
    #[must_use]
    pub fn with_item_listeners(mut self, listeners: ItemListenerSet<I, O>) -> Self {
        self.step = self.step.with_item_listeners(listeners);
        self
    }

    /// Installs bounded retry, backoff, skip, and rollback behavior.
    ///
    /// The installed [`FaultRuntime`]'s delivery mode must match the one
    /// declared in the `restart` contract passed to [`Self::new`]; a mismatch
    /// surfaces from [`Self::build_chunk_job`] or
    /// [`crate::FlowJob::with_chunk_step`] exactly as it does for a
    /// hand-assembled [`ChunkStep`], since this method does not duplicate
    /// that check.
    #[must_use]
    pub fn with_fault_runtime(mut self, fault: FaultRuntime) -> Self {
        self.step = self.step.with_fault_runtime(fault);
        self
    }

    /// Registers a step listener in deterministic before-order.
    #[must_use]
    pub fn with_listener(mut self, listener: Arc<dyn StepExecutionListener>) -> Self {
        self.step = self.step.with_listener(listener);
        self
    }

    /// Registers one namespaced `ItemStream`, on both the runtime step and
    /// the restart-relevant revisions, from a single `identity`.
    ///
    /// This is the single call that replaces separately calling
    /// [`ChunkStep::with_item_stream`] and
    /// [`ChunkComponentRevisions::with_stream_revision`] with independently
    /// typed copies of the same [`ComponentStreamIdentity`]: `identity` is
    /// consumed once here and applied to both, so the two registrations
    /// cannot name different namespaces by typo.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use oxide_batch::item_components::{IdentityProcessor, IterReader, NoopWriter};
    /// use oxide_batch::{
    ///     ChunkComponentRevisions, ChunkDeliveryMode, ChunkPipelineBuilder, ChunkRestartContract,
    ///     ChunkSize, CodecId, CodecVersion, ComponentRevision, ComponentStateEnvelope,
    ///     ComponentStreamIdentity, DefaultComponentCodec, ItemStream, RestartabilityDeclaration,
    ///     StateCodecError, StateSchemaId, StateSchemaVersion, StepName, StreamCloseContext,
    ///     StreamCloseError, StreamCloseOutcome, StreamOpenContext, StreamOpenError,
    ///     StreamOpenOutcome, StreamStateContract, StreamUpdateContext, StreamUpdateError,
    ///     VersionedStateCodec,
    /// };
    /// # use std::error::Error;
    /// # use oxide_batch::{
    /// #     BoxFuture, BusinessTransaction, ChunkCommitReceipt, ChunkCounts, ChunkFaultProgress,
    /// #     ChunkTransaction, ChunkTransactionError, ChunkTransactionManager,
    /// # };
    /// # struct NoTransaction;
    /// # impl ChunkTransaction for NoTransaction {
    /// #     fn business_transaction(&mut self) -> Option<&mut dyn BusinessTransaction> {
    /// #         None
    /// #     }
    /// #     fn commit(
    /// #         &mut self,
    /// #         _counts: ChunkCounts,
    /// #         _fault: ChunkFaultProgress,
    /// #     ) -> BoxFuture<'_, Result<ChunkCommitReceipt, ChunkTransactionError>> {
    /// #         Box::pin(async { Err(ChunkTransactionError::NotCommitted) })
    /// #     }
    /// #     fn rollback(&mut self) -> BoxFuture<'_, Result<(), ChunkTransactionError>> {
    /// #         Box::pin(async { Ok(()) })
    /// #     }
    /// # }
    /// # struct NoTransactions;
    /// # impl ChunkTransactionManager for NoTransactions {
    /// #     fn begin(
    /// #         &self,
    /// #     ) -> BoxFuture<'_, Result<Box<dyn ChunkTransaction + '_>, ChunkTransactionError>> {
    /// #         Box::pin(async { Ok(Box::new(NoTransaction) as Box<dyn ChunkTransaction>) })
    /// #     }
    /// # }
    ///
    /// // A minimal codec: this stream's open/update/close are never invoked
    /// // by construction alone, so only the schema identity actually matters
    /// // here.
    /// struct UnitCodec(StateSchemaId, StateSchemaVersion);
    /// impl VersionedStateCodec<()> for UnitCodec {
    ///     fn schema_id(&self) -> &StateSchemaId {
    ///         &self.0
    ///     }
    ///     fn current_version(&self) -> StateSchemaVersion {
    ///         self.1
    ///     }
    ///     fn encode(&self, _value: &()) -> Result<Vec<u8>, StateCodecError> {
    ///         unimplemented!()
    ///     }
    ///     fn decode(&self, _payload: &[u8]) -> Result<(), StateCodecError> {
    ///         unimplemented!()
    ///     }
    /// }
    ///
    /// struct NoState;
    /// impl ItemStream for NoState {
    ///     async fn open(
    ///         &self,
    ///         _context: StreamOpenContext<'_>,
    ///     ) -> Result<StreamOpenOutcome, StreamOpenError> {
    ///         Ok(StreamOpenOutcome::Initial)
    ///     }
    ///     async fn update(
    ///         &self,
    ///         _context: StreamUpdateContext<'_>,
    ///     ) -> Result<ComponentStateEnvelope, StreamUpdateError> {
    ///         Err(StreamUpdateError::new())
    ///     }
    ///     async fn close(
    ///         &self,
    ///         _context: StreamCloseContext<'_>,
    ///     ) -> Result<StreamCloseOutcome, StreamCloseError> {
    ///         Ok(StreamCloseOutcome::Closed)
    ///     }
    /// }
    ///
    /// let restart = ChunkRestartContract::new(
    ///     StateSchemaId::new("example.checkpoint")?,
    ///     StateSchemaVersion::new(1)?,
    ///     StateSchemaId::new("example.context")?,
    ///     StateSchemaVersion::new(1)?,
    ///     ChunkDeliveryMode::AtLeastOnce,
    /// );
    /// let contract = StreamStateContract::new(DefaultComponentCodec::new(
    ///     UnitCodec(
    ///         StateSchemaId::new("example.unit")?,
    ///         StateSchemaVersion::new(1)?,
    ///     ),
    ///     CodecId::new("example.unit-codec")?,
    ///     CodecVersion::new(1)?,
    ///     RestartabilityDeclaration::Restartable,
    /// ));
    /// let identity = ComponentStreamIdentity::new("reader.row_count")?;
    ///
    /// let builder = ChunkPipelineBuilder::new(
    ///     StepName::new("import")?,
    ///     ChunkSize::new(10)?,
    ///     IterReader::new([1u64, 2, 3]),
    ///     ComponentRevision::new("reader-v1")?,
    ///     IdentityProcessor,
    ///     ComponentRevision::new("processor-v1")?,
    ///     NoopWriter,
    ///     ComponentRevision::new("writer-v1")?,
    ///     ComponentRevision::new("checkpoint-v1")?,
    ///     restart,
    ///     Arc::new(NoTransactions),
    /// )
    /// .with_stream(
    ///     identity.clone(),
    ///     NoState,
    ///     contract,
    ///     ComponentRevision::new("row-count-v1")?,
    /// );
    ///
    /// let revisions = builder.revisions()?;
    /// assert_eq!(
    ///     revisions.stream_revisions().collect::<Vec<_>>(),
    ///     vec![(&identity, &ComponentRevision::new("row-count-v1")?)]
    /// );
    /// # Ok::<(), Box<dyn Error>>(())
    /// ```
    #[must_use]
    pub fn with_stream(
        mut self,
        identity: ComponentStreamIdentity,
        stream: impl ItemStream + 'static,
        contract: StreamStateContract,
        revision: ComponentRevision,
    ) -> Self {
        self.step = self
            .step
            .with_item_stream(identity.clone(), stream, contract);
        self.revisions = self.revisions.with_stream_revision(identity, revision);
        self
    }

    /// Borrows the step name this builder will produce.
    #[must_use]
    pub fn name(&self) -> &StepName {
        self.step.name()
    }

    /// Computes the restart-relevant component revisions accumulated so far,
    /// including a completion-policy revision freshly derived from whatever
    /// policy is currently installed (if any).
    ///
    /// Call this (or [`Self::flow_step_components`]) *before* compiling the
    /// enclosing [`crate::FlowGraph`] when binding through
    /// [`crate::FlowJob::with_chunk_step`], and reuse its exact return value
    /// for both the graph compilation and the later binding call -- the same
    /// requirement [`crate::completion_policy_revision`]'s documentation
    /// describes, now computed for you instead of hand-folded.
    ///
    /// Each call to this method, [`Self::flow_step_components`], or
    /// [`Self::build`] recomputes the installed policy's live
    /// [`CompletionPolicy::fingerprint`] fresh rather than caching one
    /// computed value -- relying on the same purity contract
    /// [`crate::completion_policy_revision`]'s documentation already requires
    /// of every [`CompletionPolicy`] implementor. This mirrors
    /// [`crate::FlowJob::with_chunk_step`]'s own existing declare-then-validate
    /// pattern (one call when compiling the graph, a second, independent call
    /// when binding), not a new assumption this builder introduces.
    ///
    /// # Errors
    ///
    /// Returns [`DefinitionError::CompletionPolicyFingerprintPanic`] if an
    /// installed policy's [`CompletionPolicy::fingerprint`] panics.
    pub fn revisions(&self) -> Result<ChunkComponentRevisions, DefinitionError> {
        let mut revisions = completion_policy_component_revisions(&self.step, &self.revisions)?;
        for (identity, revision) in &self.completion_policy_stream_revisions {
            revisions = revisions.with_stream_revision(identity.clone(), revision.clone());
        }
        Ok(revisions)
    }

    /// Computes the [`StepComponents::Chunk`] value for a [`crate::FlowGraph`]
    /// node, from this builder's accumulated size and revisions.
    ///
    /// A thin convenience over [`Self::revisions`].
    ///
    /// # Errors
    ///
    /// See [`Self::revisions`].
    pub fn flow_step_components(&self) -> Result<StepComponents, DefinitionError> {
        Ok(StepComponents::Chunk {
            size: self.step.size(),
            revisions: Box::new(self.revisions()?),
        })
    }

    /// Finishes this builder into a [`ChunkStep`] and its matching
    /// [`ChunkComponentRevisions`], ready for [`ChunkJob::new`] or
    /// [`crate::FlowJob::with_chunk_step`].
    ///
    /// # Errors
    ///
    /// See [`Self::revisions`].
    #[allow(clippy::type_complexity)]
    pub fn build(
        self,
    ) -> Result<(ChunkStep<I, O, R, P, W>, ChunkComponentRevisions), DefinitionError> {
        let revisions = self.revisions()?;
        Ok((self.step, revisions))
    }

    /// Finishes this builder directly into a [`ChunkJob`].
    ///
    /// A pure forward onto [`Self::build`] and [`ChunkJob::new`]; the single
    /// convenience the [`ChunkJob`]-only path (as opposed to
    /// [`crate::FlowJob`]) can offer, since [`ChunkJob::new`] already builds
    /// its compiled plan and runtime step from one call.
    ///
    /// # Errors
    ///
    /// See [`Self::revisions`] and [`ChunkJob::new`].
    pub fn build_chunk_job(
        self,
        name: JobName,
        revision: DefinitionRevision,
    ) -> Result<ChunkJob<I, O, R, P, W>, DefinitionError>
    where
        R: Send + 'static,
        P: Send + 'static,
        W: Send + 'static,
    {
        let (step, revisions) = self.build()?;
        ChunkJob::new(name, step, revision, &revisions)
    }
}

impl<I, O, R, P, W> fmt::Debug for ChunkPipelineBuilder<I, O, R, P, W> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChunkPipelineBuilder")
            .field("step", &self.step)
            .field("revisions", &self.revisions)
            .field(
                "completion_policy_stream_revisions",
                &self.completion_policy_stream_revisions,
            )
            .finish()
    }
}
