//! Integration tests for [`ChunkPipelineBuilder`] (#152).
//!
//! These exercise the ergonomic single-step builder against the same
//! validated runtime types (`ChunkStep`, `ChunkComponentRevisions`,
//! `ChunkJob`, `FlowJob`) the rest of the suite assembles by hand, proving
//! the builder is a configuration-time convenience rather than a second
//! execution path: a builder-assembled pipeline produces byte-identical
//! restart fingerprints to the hand-assembled equivalent, typed and `Boxed*`
//! instantiations of the same builder produce identical fingerprints, and
//! validation this builder does not reimplement (duplicate/undeclared
//! stream detection, delivery-mode agreement) still fires correctly.

#![allow(clippy::expect_used, clippy::panic)]

#[path = "support/chunk_fixture.rs"]
mod chunk_fixture;

use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use chunk_fixture::{Double, NoopCompletion, NoopTransactions, Sink, Source, correlation, receipt};
use oxide_batch::item_components::{
    DelimitedDialect, DelimitedRecord, IdentityProcessor, InMemoryObjectStore, JsonArrayFormat,
    NoopWriter, ObjectIdentity, ObjectStoreCapability, ObjectStoreError, ObjectStoreReaderOpener,
    ResourceIdentity, ResourceSet, delimited_reader, json_array_reader, multi_resource_reader,
};
use oxide_batch::{
    AdaptiveBounds, AdaptiveCompletionPolicy, BackoffOutcome, BackoffPolicy, BackoffSleeper,
    BoxFuture, BoxedProcessor, BoxedReader, BoxedWriter, ChunkComponentRevisions,
    ChunkDeliveryMode, ChunkJob, ChunkPipelineBuilder, ChunkRestartContract, ChunkSize, ChunkStep,
    ChunkTimeThreshold, ClassifierRevision, CompletionPolicy, ComponentRevision,
    ComponentStreamIdentity, CompositeCompletionPolicy, CompositeMode, DefinitionError,
    DefinitionRevision, FailureCategory, FaultAction, FaultClassifier, FaultPhase, FaultPolicy,
    FaultRule, FaultRuntime, FlowGraph, FlowJob, FlowNode, FlowTarget, InMemoryFaultState,
    ItemCountCompletionPolicy, JobName, NodeId, RestartabilityDeclaration, RetryLimit,
    RetryStateLimit, SkipLimit, StateSchemaId, StateSchemaVersion, StepName, StepNode, StopSource,
    TerminalKind,
};

fn restart(delivery_mode: ChunkDeliveryMode) -> ChunkRestartContract {
    ChunkRestartContract::new(
        StateSchemaId::new("test.checkpoint").expect("valid schema id"),
        StateSchemaVersion::new(1).expect("valid schema version"),
        StateSchemaId::new("test.context").expect("valid schema id"),
        StateSchemaVersion::new(1).expect("valid schema version"),
        delivery_mode,
    )
}

/// A typed pipeline built through [`ChunkPipelineBuilder`] executes exactly
/// like one assembled by hand, with the default [`oxide_batch::NoopChunkCompletion`]
/// never blocking a real commit.
#[tokio::test]
async fn typed_pipeline_builds_and_executes() {
    let (mut step, revisions) = ChunkPipelineBuilder::new(
        StepName::new("double").expect("valid step name"),
        ChunkSize::new(10).expect("valid chunk size"),
        Source::range(3),
        ComponentRevision::new("reader-v1").expect("valid revision"),
        Double,
        ComponentRevision::new("processor-v1").expect("valid revision"),
        Sink(Arc::new(std::sync::Mutex::new(Vec::new()))),
        ComponentRevision::new("writer-v1").expect("valid revision"),
        ComponentRevision::new("checkpoint-v1").expect("valid revision"),
        restart(ChunkDeliveryMode::AtLeastOnce),
        Arc::new(NoopTransactions),
    )
    .build()
    .expect("valid pipeline builds");
    assert_eq!(
        revisions.reader(),
        &ComponentRevision::new("reader-v1").expect("valid")
    );

    let (_stop_source, stop_token) = StopSource::new();
    let report = step.execute(&correlation(), &stop_token).await;
    assert_eq!(
        report.outcome(),
        oxide_batch::ChunkExecutionOutcome::Completed
    );

    // The default `NoopChunkCompletion` also lowers into a valid `ChunkJob`.
    let job = ChunkPipelineBuilder::new(
        StepName::new("double").expect("valid step name"),
        ChunkSize::new(10).expect("valid chunk size"),
        Source::range(3),
        ComponentRevision::new("reader-v1").expect("valid revision"),
        Double,
        ComponentRevision::new("processor-v1").expect("valid revision"),
        Sink(Arc::new(std::sync::Mutex::new(Vec::new()))),
        ComponentRevision::new("writer-v1").expect("valid revision"),
        ComponentRevision::new("checkpoint-v1").expect("valid revision"),
        restart(ChunkDeliveryMode::AtLeastOnce),
        Arc::new(NoopTransactions),
    )
    .build_chunk_job(
        JobName::new("double_job").expect("valid job name"),
        DefinitionRevision::new("v1").expect("valid revision"),
    )
    .expect("valid pipeline builds");
    assert_eq!(job.step_name(), &StepName::new("double").expect("valid"));
}

/// The builder never derives restart-relevant revisions: its output is
/// byte-identical to hand-assembling [`ChunkStep`] and
/// [`ChunkComponentRevisions`] separately, proving the builder is a pure
/// configuration-time convenience over the same runtime objects.
#[test]
fn builder_output_matches_hand_assembled_definition() {
    let reader_revision = ComponentRevision::new("reader-v1").expect("valid revision");
    let processor_revision = ComponentRevision::new("processor-v1").expect("valid revision");
    let writer_revision = ComponentRevision::new("writer-v1").expect("valid revision");
    let checkpoint_revision = ComponentRevision::new("checkpoint-v1").expect("valid revision");
    let contract = restart(ChunkDeliveryMode::AtLeastOnce);

    let via_builder = ChunkPipelineBuilder::new(
        StepName::new("double").expect("valid step name"),
        ChunkSize::new(10).expect("valid chunk size"),
        Source::range(3),
        reader_revision.clone(),
        Double,
        processor_revision.clone(),
        Sink(Arc::new(std::sync::Mutex::new(Vec::new()))),
        writer_revision.clone(),
        checkpoint_revision.clone(),
        contract.clone(),
        Arc::new(NoopTransactions),
    )
    .build_chunk_job(
        JobName::new("double_job").expect("valid job name"),
        DefinitionRevision::new("v1").expect("valid revision"),
    )
    .expect("valid pipeline builds");

    let hand_assembled_step = ChunkStep::new(
        StepName::new("double").expect("valid step name"),
        ChunkSize::new(10).expect("valid chunk size"),
        Source::range(3),
        Double,
        Sink(Arc::new(std::sync::Mutex::new(Vec::new()))),
        Arc::new(NoopTransactions),
        Arc::new(NoopCompletion),
    );
    let hand_assembled_revisions = ChunkComponentRevisions::new(
        reader_revision,
        processor_revision,
        writer_revision,
        checkpoint_revision,
        contract,
    );
    let hand_assembled = ChunkJob::new(
        JobName::new("double_job").expect("valid job name"),
        hand_assembled_step,
        DefinitionRevision::new("v1").expect("valid revision"),
        &hand_assembled_revisions,
    )
    .expect("hand-assembled pipeline is equally valid");

    assert_eq!(
        via_builder.definition_identity().manifest_digest(),
        hand_assembled.definition_identity().manifest_digest(),
        "the builder must not change the restart-relevant definition fingerprint"
    );
}

/// A `Boxed*` instantiation of the identical builder, over the identical
/// revisions, produces the identical restart fingerprint as the typed
/// instantiation -- erasure is a representation decision, not a second
/// execution path (ADR-0008).
#[test]
fn typed_and_boxed_pipelines_share_one_fingerprint() {
    let reader_revision = ComponentRevision::new("reader-v1").expect("valid revision");
    let processor_revision = ComponentRevision::new("processor-v1").expect("valid revision");
    let writer_revision = ComponentRevision::new("writer-v1").expect("valid revision");
    let checkpoint_revision = ComponentRevision::new("checkpoint-v1").expect("valid revision");
    let contract = restart(ChunkDeliveryMode::AtLeastOnce);
    let name = || JobName::new("double_job").expect("valid job name");
    let step_name = || StepName::new("double").expect("valid step name");
    let revision = || DefinitionRevision::new("v1").expect("valid revision");

    let typed = ChunkPipelineBuilder::new(
        step_name(),
        ChunkSize::new(10).expect("valid chunk size"),
        Source::range(3),
        reader_revision.clone(),
        Double,
        processor_revision.clone(),
        Sink(Arc::new(std::sync::Mutex::new(Vec::new()))),
        writer_revision.clone(),
        checkpoint_revision.clone(),
        contract.clone(),
        Arc::new(NoopTransactions),
    )
    .build_chunk_job(name(), revision())
    .expect("typed pipeline builds");

    let boxed = ChunkPipelineBuilder::new(
        step_name(),
        ChunkSize::new(10).expect("valid chunk size"),
        BoxedReader::new(Source::range(3)),
        reader_revision,
        BoxedProcessor::new(Double),
        processor_revision,
        BoxedWriter::new(Sink(Arc::new(std::sync::Mutex::new(Vec::new())))),
        writer_revision,
        checkpoint_revision,
        contract,
        Arc::new(NoopTransactions),
    )
    .build_chunk_job(name(), revision())
    .expect("boxed pipeline builds");

    assert_eq!(
        typed.definition_identity().manifest_digest(),
        boxed.definition_identity().manifest_digest(),
    );
}

/// [`ChunkPipelineBuilder::with_stream`] registers a real, stateful
/// first-party component's [`ComponentStreamIdentity`] on both the runtime
/// step and the restart-relevant revisions from one call; the resulting
/// pipeline passes the same stream-registration validation a hand-assembled
/// one would.
#[test]
fn with_stream_registers_a_real_stateful_component_consistently() {
    let identity = ComponentStreamIdentity::new("orders.csv").expect("valid identity");
    let (reader, stream, contract) = delimited_reader::<DelimitedRecord, _>(
        Cursor::new(b"a,b\nc,d\n".to_vec()),
        DelimitedDialect::csv(),
        identity.clone(),
    );

    let job = ChunkPipelineBuilder::<DelimitedRecord, DelimitedRecord, _, _, _>::new(
        StepName::new("import_orders").expect("valid step name"),
        ChunkSize::new(10).expect("valid chunk size"),
        reader,
        ComponentRevision::new("reader-v1").expect("valid revision"),
        IdentityProcessor,
        ComponentRevision::new("processor-v1").expect("valid revision"),
        NoopWriter,
        ComponentRevision::new("writer-v1").expect("valid revision"),
        ComponentRevision::new("checkpoint-v1").expect("valid revision"),
        restart(ChunkDeliveryMode::AtLeastOnce),
        Arc::new(NoopTransactions),
    )
    .with_stream(
        identity.clone(),
        stream,
        contract,
        ComponentRevision::new("orders-csv-v1").expect("valid revision"),
    )
    .build_chunk_job(
        JobName::new("import_job").expect("valid job name"),
        DefinitionRevision::new("v1").expect("valid revision"),
    )
    .expect("stream registration matches on both sides");

    assert_eq!(
        job.step_name(),
        &StepName::new("import_orders").expect("valid")
    );
}

/// A completion policy installed through the builder binds into a
/// [`FlowJob`] via [`ChunkPipelineBuilder::flow_step_components`] and
/// [`ChunkPipelineBuilder::build`], mirroring the documented `ChunkJob`
/// (automatic) vs. `FlowJob` (explicit predeclaration) asymmetry -- the
/// builder makes that predeclaration duplication-free, never optional.
#[tokio::test]
async fn flow_job_binds_a_completion_policy_declared_through_the_builder() {
    let policy = Arc::new(ItemCountCompletionPolicy::new(
        ChunkSize::new(2).expect("valid chunk size"),
    ));

    let builder = ChunkPipelineBuilder::new(
        StepName::new("double").expect("valid step name"),
        ChunkSize::new(10).expect("valid chunk size"),
        Source::range(3),
        ComponentRevision::new("reader-v1").expect("valid revision"),
        Double,
        ComponentRevision::new("processor-v1").expect("valid revision"),
        Sink(Arc::new(std::sync::Mutex::new(Vec::new()))),
        ComponentRevision::new("writer-v1").expect("valid revision"),
        ComponentRevision::new("checkpoint-v1").expect("valid revision"),
        restart(ChunkDeliveryMode::AtLeastOnce),
        Arc::new(NoopTransactions),
    )
    .with_completion_policy(Arc::clone(&policy) as _);

    let node = NodeId::new("double").expect("valid node id");
    let name = JobName::new("double_flow").expect("valid job name");
    let plan = FlowGraph::new(node.clone())
        .with_node(FlowNode::step(StepNode::new(
            node.clone(),
            StepName::new("double").expect("valid step name"),
            builder.flow_step_components().expect("policy fingerprints"),
        )))
        .with_sequence(node.clone(), FlowTarget::Terminal(TerminalKind::Complete))
        .expect("valid sequence")
        .compile(
            &name,
            DefinitionRevision::new("flow-v1").expect("valid revision"),
        )
        .expect("flow compiles");

    let (step, revisions) = builder.build().expect("policy fingerprints");
    let mut job = FlowJob::new(name, plan)
        .expect("format-2 flow is valid")
        .with_chunk_step(node, step, &revisions)
        .expect("declared revision matches the live policy");

    let _ = &mut job; // binding succeeds; execution is covered by chunk_runtime's own FlowJob suite.
}

/// A [`FaultRuntime`] whose delivery mode disagrees with the restart
/// contract's is rejected -- this builder does not duplicate that check, so
/// it must still fire exactly as it does for a hand-assembled [`ChunkStep`].
#[test]
fn delivery_mode_mismatch_is_rejected() {
    struct ImmediateSleeper;
    impl BackoffSleeper for ImmediateSleeper {
        fn sleep<'a>(
            &'a self,
            _delay: Duration,
            _stop: &'a oxide_batch::StopToken,
        ) -> BoxFuture<'a, BackoffOutcome> {
            Box::pin(async { BackoffOutcome::Elapsed })
        }
    }

    let policy = FaultPolicy::new(
        FaultClassifier::new(
            ClassifierRevision::new("test_v1").expect("valid revision"),
            [FaultRule::new(
                FaultPhase::Write,
                FailureCategory::Timeout,
                FaultAction::retry(),
            )
            .expect("valid rule")],
        )
        .expect("valid classifier"),
        RetryLimit::new(2).expect("valid limit"),
        RetryStateLimit::new(16).expect("valid limit"),
        SkipLimit::NONE,
        BackoffPolicy::fixed(Duration::from_millis(1)).expect("valid backoff"),
    )
    .expect("valid policy");
    let state = Arc::new(InMemoryFaultState::new(policy.retry_state_limit()));
    let fault = FaultRuntime::new(
        policy,
        Arc::new(ImmediateSleeper),
        state,
        ChunkDeliveryMode::AtomicSameResource,
    )
    .expect("valid fault runtime");

    let result = ChunkPipelineBuilder::new(
        StepName::new("double").expect("valid step name"),
        ChunkSize::new(10).expect("valid chunk size"),
        Source::range(3),
        ComponentRevision::new("reader-v1").expect("valid revision"),
        Double,
        ComponentRevision::new("processor-v1").expect("valid revision"),
        Sink(Arc::new(std::sync::Mutex::new(Vec::new()))),
        ComponentRevision::new("writer-v1").expect("valid revision"),
        ComponentRevision::new("checkpoint-v1").expect("valid revision"),
        restart(ChunkDeliveryMode::AtLeastOnce), // disagrees with the fault runtime above
        Arc::new(NoopTransactions),
    )
    .with_fault_runtime(fault)
    .build_chunk_job(
        JobName::new("double_job").expect("valid job name"),
        DefinitionRevision::new("v1").expect("valid revision"),
    );

    assert!(matches!(result, Err(DefinitionError::DeliveryModeMismatch)));
}

/// Two [`ChunkPipelineBuilder::with_stream`] calls that reuse the same
/// [`ComponentStreamIdentity`] are rejected -- the builder does not suppress
/// or duplicate the existing duplicate-registration validation.
#[test]
fn duplicate_stream_identity_is_rejected() {
    let identity = ComponentStreamIdentity::new("orders.csv").expect("valid identity");
    let (reader, stream_a, contract_a) = delimited_reader::<DelimitedRecord, _>(
        Cursor::new(b"a,b\n".to_vec()),
        DelimitedDialect::csv(),
        identity.clone(),
    );
    let (_ignored_reader, stream_b, contract_b) = delimited_reader::<DelimitedRecord, _>(
        Cursor::new(b"c,d\n".to_vec()),
        DelimitedDialect::csv(),
        identity.clone(),
    );

    let result = ChunkPipelineBuilder::<DelimitedRecord, DelimitedRecord, _, _, _>::new(
        StepName::new("import_orders").expect("valid step name"),
        ChunkSize::new(10).expect("valid chunk size"),
        reader,
        ComponentRevision::new("reader-v1").expect("valid revision"),
        IdentityProcessor,
        ComponentRevision::new("processor-v1").expect("valid revision"),
        NoopWriter,
        ComponentRevision::new("writer-v1").expect("valid revision"),
        ComponentRevision::new("checkpoint-v1").expect("valid revision"),
        restart(ChunkDeliveryMode::AtLeastOnce),
        Arc::new(NoopTransactions),
    )
    .with_stream(
        identity.clone(),
        stream_a,
        contract_a,
        ComponentRevision::new("orders-csv-v1").expect("valid revision"),
    )
    .with_stream(
        identity,
        stream_b,
        contract_b,
        ComponentRevision::new("orders-csv-v2").expect("valid revision"),
    )
    .build_chunk_job(
        JobName::new("import_job").expect("valid job name"),
        DefinitionRevision::new("v1").expect("valid revision"),
    );

    assert!(matches!(
        result,
        Err(DefinitionError::DuplicateRuntimeStream { .. })
    ));
}

/// An installed [`AdaptiveCompletionPolicy`] auto-registers its own
/// `ItemStream` on the runtime step (via
/// [`ChunkPipelineBuilder::with_adaptive_completion_policy`]), but declares
/// no matching restart-relevant revision on its own -- that is
/// application-chosen versioning the builder cannot derive from the
/// identity alone, so omitting
/// [`ChunkPipelineBuilder::with_completion_policy_stream_revision`] must
/// still fail exactly like any other undeclared runtime stream.
#[test]
fn adaptive_completion_policy_without_a_declared_stream_revision_is_rejected() {
    let policy = AdaptiveCompletionPolicy::new(
        ComponentStreamIdentity::new("chunk_builder.adaptive_size").expect("valid identity"),
        AdaptiveBounds::new(
            ChunkSize::new(2).expect("valid"),
            ChunkSize::new(50).expect("valid"),
        )
        .expect("valid bounds"),
        ChunkTimeThreshold::new(Duration::from_millis(200)).expect("valid threshold"),
        Arc::new(oxide_batch::SystemClock),
    );

    let result = ChunkPipelineBuilder::new(
        StepName::new("double").expect("valid step name"),
        ChunkSize::new(10).expect("valid chunk size"),
        Source::range(3),
        ComponentRevision::new("reader-v1").expect("valid revision"),
        Double,
        ComponentRevision::new("processor-v1").expect("valid revision"),
        Sink(Arc::new(std::sync::Mutex::new(Vec::new()))),
        ComponentRevision::new("writer-v1").expect("valid revision"),
        ComponentRevision::new("checkpoint-v1").expect("valid revision"),
        restart(ChunkDeliveryMode::AtLeastOnce),
        Arc::new(NoopTransactions),
    )
    .with_adaptive_completion_policy(policy)
    .build_chunk_job(
        JobName::new("double_job").expect("valid job name"),
        DefinitionRevision::new("v1").expect("valid revision"),
    );

    assert!(matches!(
        result,
        Err(DefinitionError::RuntimeStreamNotDeclared { .. })
    ));
}

/// Declaring the adaptive policy's own stream revision through
/// [`ChunkPipelineBuilder::with_completion_policy_stream_revision`] resolves
/// the rejection above: the runtime registration
/// [`ChunkPipelineBuilder::with_adaptive_completion_policy`] already made
/// and the definition-side revision declared here name the same identity.
#[test]
fn adaptive_completion_policy_with_a_declared_stream_revision_builds() {
    let identity =
        ComponentStreamIdentity::new("chunk_builder.adaptive_size").expect("valid identity");
    let policy = AdaptiveCompletionPolicy::new(
        identity.clone(),
        AdaptiveBounds::new(
            ChunkSize::new(2).expect("valid"),
            ChunkSize::new(50).expect("valid"),
        )
        .expect("valid bounds"),
        ChunkTimeThreshold::new(Duration::from_millis(200)).expect("valid threshold"),
        Arc::new(oxide_batch::SystemClock),
    );

    let job = ChunkPipelineBuilder::new(
        StepName::new("double").expect("valid step name"),
        ChunkSize::new(10).expect("valid chunk size"),
        Source::range(3),
        ComponentRevision::new("reader-v1").expect("valid revision"),
        Double,
        ComponentRevision::new("processor-v1").expect("valid revision"),
        Sink(Arc::new(std::sync::Mutex::new(Vec::new()))),
        ComponentRevision::new("writer-v1").expect("valid revision"),
        ComponentRevision::new("checkpoint-v1").expect("valid revision"),
        restart(ChunkDeliveryMode::AtLeastOnce),
        Arc::new(NoopTransactions),
    )
    .with_adaptive_completion_policy(Arc::clone(&policy))
    .with_completion_policy_stream_revision(
        identity,
        ComponentRevision::new("adaptive-v1").expect("valid revision"),
    )
    .build_chunk_job(
        JobName::new("double_job").expect("valid job name"),
        DefinitionRevision::new("v1").expect("valid revision"),
    )
    .expect("declared stream revision matches the policy's own runtime registration");

    assert_eq!(job.step_name(), &StepName::new("double").expect("valid"));
}

/// Replacing an installed completion policy discards exactly the previous
/// policy's own declared stream revision -- never a manually declared one,
/// and never leaving a stale declaration for a namespace the runtime no
/// longer registers. If the first policy's declaration survived the
/// replacement, this would fail `DefinitionError::DeclaredStreamMissingRuntime`
/// (a declared revision with no matching runtime registration) instead of
/// succeeding with exactly the second policy's revision.
#[test]
fn completion_policy_replacement_discards_the_previous_policys_stream_revision() {
    let first = AdaptiveCompletionPolicy::new(
        ComponentStreamIdentity::new("chunk_builder.first_adaptive").expect("valid identity"),
        AdaptiveBounds::new(
            ChunkSize::new(2).expect("valid"),
            ChunkSize::new(50).expect("valid"),
        )
        .expect("valid bounds"),
        ChunkTimeThreshold::new(Duration::from_millis(200)).expect("valid threshold"),
        Arc::new(oxide_batch::SystemClock),
    );
    let second_identity =
        ComponentStreamIdentity::new("chunk_builder.second_adaptive").expect("valid identity");
    let second = AdaptiveCompletionPolicy::new(
        second_identity.clone(),
        AdaptiveBounds::new(
            ChunkSize::new(2).expect("valid"),
            ChunkSize::new(50).expect("valid"),
        )
        .expect("valid bounds"),
        ChunkTimeThreshold::new(Duration::from_millis(200)).expect("valid threshold"),
        Arc::new(oxide_batch::SystemClock),
    );

    let builder = ChunkPipelineBuilder::new(
        StepName::new("double").expect("valid step name"),
        ChunkSize::new(10).expect("valid chunk size"),
        Source::range(3),
        ComponentRevision::new("reader-v1").expect("valid revision"),
        Double,
        ComponentRevision::new("processor-v1").expect("valid revision"),
        Sink(Arc::new(std::sync::Mutex::new(Vec::new()))),
        ComponentRevision::new("writer-v1").expect("valid revision"),
        ComponentRevision::new("checkpoint-v1").expect("valid revision"),
        restart(ChunkDeliveryMode::AtLeastOnce),
        Arc::new(NoopTransactions),
    )
    .with_adaptive_completion_policy(Arc::clone(&first))
    .with_completion_policy_stream_revision(
        first.identity().clone(),
        ComponentRevision::new("first-adaptive-v1").expect("valid revision"),
    )
    // Replacing the policy must discard `first`'s declaration above.
    .with_adaptive_completion_policy(Arc::clone(&second))
    .with_completion_policy_stream_revision(
        second_identity.clone(),
        ComponentRevision::new("second-adaptive-v1").expect("valid revision"),
    );

    let revisions = builder
        .revisions()
        .expect("only the second policy's revision remains");
    assert_eq!(
        revisions.stream_revisions().collect::<Vec<_>>(),
        vec![(
            &second_identity,
            &ComponentRevision::new("second-adaptive-v1").expect("valid revision")
        )]
    );

    builder
        .build_chunk_job(
            JobName::new("double_job").expect("valid job name"),
            DefinitionRevision::new("v1").expect("valid revision"),
        )
        .expect("the stale first-policy declaration must not block binding");
}

/// [`AdaptiveCompletionPolicy`] nested inside a [`CompositeCompletionPolicy`]
/// still registers its stream on the runtime step (per
/// [`CompletionPolicy::stream_registrations`]'s composite-recursion
/// contract), and [`ChunkPipelineBuilder::with_completion_policy_stream_revision`]
/// composes with that transparently: the builder has no composite-specific
/// logic, only per-identity declarations.
#[test]
fn adaptive_completion_policy_nested_in_a_composite_builds() {
    let identity =
        ComponentStreamIdentity::new("chunk_builder.nested_adaptive").expect("valid identity");
    let adaptive = AdaptiveCompletionPolicy::new(
        identity.clone(),
        AdaptiveBounds::new(
            ChunkSize::new(2).expect("valid"),
            ChunkSize::new(50).expect("valid"),
        )
        .expect("valid bounds"),
        ChunkTimeThreshold::new(Duration::from_millis(200)).expect("valid threshold"),
        Arc::new(oxide_batch::SystemClock),
    );
    let composite = Arc::new(
        CompositeCompletionPolicy::new(
            CompositeMode::Any,
            vec![
                adaptive as Arc<dyn CompletionPolicy>,
                Arc::new(ItemCountCompletionPolicy::new(
                    ChunkSize::new(5).expect("valid"),
                )),
            ],
        )
        .expect("valid composite"),
    );

    let job = ChunkPipelineBuilder::new(
        StepName::new("double").expect("valid step name"),
        ChunkSize::new(10).expect("valid chunk size"),
        Source::range(3),
        ComponentRevision::new("reader-v1").expect("valid revision"),
        Double,
        ComponentRevision::new("processor-v1").expect("valid revision"),
        Sink(Arc::new(std::sync::Mutex::new(Vec::new()))),
        ComponentRevision::new("writer-v1").expect("valid revision"),
        ComponentRevision::new("checkpoint-v1").expect("valid revision"),
        restart(ChunkDeliveryMode::AtLeastOnce),
        Arc::new(NoopTransactions),
    )
    .with_completion_policy(composite)
    .with_completion_policy_stream_revision(
        identity,
        ComponentRevision::new("nested-adaptive-v1").expect("valid revision"),
    )
    .build_chunk_job(
        JobName::new("double_job").expect("valid job name"),
        DefinitionRevision::new("v1").expect("valid revision"),
    )
    .expect("the nested adaptive member's declared revision matches its runtime registration");

    assert_eq!(job.step_name(), &StepName::new("double").expect("valid"));
}

/// The `ChunkJob`-vs-`FlowJob` asymmetry holds for a *stateful* completion
/// policy too: [`ChunkPipelineBuilder::flow_step_components`] folds in both
/// the completion-policy fingerprint and the declared stream revision before
/// the [`FlowGraph`] compiles, and [`FlowJob::with_chunk_step`] accepts the
/// matching [`ChunkPipelineBuilder::build`] output.
#[tokio::test]
async fn flow_job_binds_an_adaptive_completion_policy_declared_through_the_builder() {
    let identity =
        ComponentStreamIdentity::new("chunk_builder.flow_adaptive").expect("valid identity");
    let policy = AdaptiveCompletionPolicy::new(
        identity.clone(),
        AdaptiveBounds::new(
            ChunkSize::new(2).expect("valid"),
            ChunkSize::new(50).expect("valid"),
        )
        .expect("valid bounds"),
        ChunkTimeThreshold::new(Duration::from_millis(200)).expect("valid threshold"),
        Arc::new(oxide_batch::SystemClock),
    );

    let builder = ChunkPipelineBuilder::new(
        StepName::new("double").expect("valid step name"),
        ChunkSize::new(10).expect("valid chunk size"),
        Source::range(3),
        ComponentRevision::new("reader-v1").expect("valid revision"),
        Double,
        ComponentRevision::new("processor-v1").expect("valid revision"),
        Sink(Arc::new(std::sync::Mutex::new(Vec::new()))),
        ComponentRevision::new("writer-v1").expect("valid revision"),
        ComponentRevision::new("checkpoint-v1").expect("valid revision"),
        restart(ChunkDeliveryMode::AtLeastOnce),
        Arc::new(NoopTransactions),
    )
    .with_adaptive_completion_policy(Arc::clone(&policy))
    .with_completion_policy_stream_revision(
        identity,
        ComponentRevision::new("flow-adaptive-v1").expect("valid revision"),
    );

    let node = NodeId::new("double").expect("valid node id");
    let name = JobName::new("double_flow").expect("valid job name");
    let plan = FlowGraph::new(node.clone())
        .with_node(FlowNode::step(StepNode::new(
            node.clone(),
            StepName::new("double").expect("valid step name"),
            builder
                .flow_step_components()
                .expect("policy and its stream revision both fingerprint"),
        )))
        .with_sequence(node.clone(), FlowTarget::Terminal(TerminalKind::Complete))
        .expect("valid sequence")
        .compile(
            &name,
            DefinitionRevision::new("flow-v1").expect("valid revision"),
        )
        .expect("flow compiles");

    let (step, revisions) = builder
        .build()
        .expect("policy and its stream revision both fingerprint");
    let job = FlowJob::new(name, plan)
        .expect("format-2 flow is valid")
        .with_chunk_step(node, step, &revisions)
        .expect("declared completion-policy and stream revisions match the live policy");

    let _ = &job;
}

/// A second real first-party stateful component -- `item_components::json_array_reader`,
/// covering the JSON/JSONL catalog issue #152 section 8 requires alongside
/// delimited/CSV -- registers through [`ChunkPipelineBuilder::with_stream`]
/// exactly like the delimited case, confirming the tuple-opener pattern
/// (`(component, stream, contract)`, shared verbatim by every stateful M6
/// reader/writer) is not delimited-specific.
#[test]
fn json_array_reader_registers_consistently_through_with_stream() {
    let identity = ComponentStreamIdentity::new("orders.json").expect("valid identity");
    let (reader, stream, contract) = json_array_reader::<serde_json::Value, _>(
        Cursor::new(b"[1,2,3]".to_vec()),
        JsonArrayFormat::new(),
        identity.clone(),
    );

    let job = ChunkPipelineBuilder::<serde_json::Value, serde_json::Value, _, _, _>::new(
        StepName::new("import_orders_json").expect("valid step name"),
        ChunkSize::new(10).expect("valid chunk size"),
        reader,
        ComponentRevision::new("reader-v1").expect("valid revision"),
        IdentityProcessor,
        ComponentRevision::new("processor-v1").expect("valid revision"),
        NoopWriter,
        ComponentRevision::new("writer-v1").expect("valid revision"),
        ComponentRevision::new("checkpoint-v1").expect("valid revision"),
        restart(ChunkDeliveryMode::AtLeastOnce),
        Arc::new(NoopTransactions),
    )
    .with_stream(
        identity,
        stream,
        contract,
        ComponentRevision::new("orders-json-v1").expect("valid revision"),
    )
    .build_chunk_job(
        JobName::new("import_json_job").expect("valid job name"),
        DefinitionRevision::new("v1").expect("valid revision"),
    )
    .expect("stream registration matches on both sides");

    assert_eq!(
        job.step_name(),
        &StepName::new("import_orders_json").expect("valid")
    );
}

/// A multi-resource, object-store-backed reader -- the same
/// `MultiResourceReaderOpener` composition `PostgreSQL` and file-backed
/// multi-resource components share -- assembled through the builder with a
/// real, first-party, non-test production backend
/// (`InMemoryObjectStore`/`ObjectStoreReaderOpener`, the executable contract
/// fixture the object-store catalog documents; not a hand-rolled test
/// double). Covers issue #152 section 8's multi-resource requirement
/// without needing a live `PostgreSQL` connection.
#[test]
fn multi_resource_object_store_reader_registers_consistently_through_with_stream() {
    #[allow(
        clippy::unnecessary_wraps,
        reason = "matches the real `parse: F` signature `ObjectStoreReaderOpener` requires"
    )]
    fn parse_csv(bytes: &[u8]) -> Result<Vec<u64>, ObjectStoreError> {
        Ok(std::str::from_utf8(bytes)
            .expect("valid utf8 fixture")
            .split(',')
            .map(|value| value.parse::<u64>().expect("valid integer fixture"))
            .collect())
    }

    let store = Arc::new(InMemoryObjectStore::new(1024));
    futures_executor::block_on(async {
        store
            .put(
                &ObjectIdentity::new("orders").expect("valid object identity"),
                b"1,2,3",
            )
            .await
            .expect("fixture object fits the bound");
    });
    let resources = ResourceSet::new(vec![
        ResourceIdentity::new("orders").expect("valid resource identity"),
    ]);
    let identity = ComponentStreamIdentity::new("orders.object_store").expect("valid identity");
    let opener = ObjectStoreReaderOpener::new(Arc::clone(&store), 1024, parse_csv);
    let (reader, stream, contract) = multi_resource_reader::<u64, _>(
        resources,
        opener,
        identity.clone(),
        RestartabilityDeclaration::Restartable,
    );

    let job = ChunkPipelineBuilder::<u64, u64, _, _, _>::new(
        StepName::new("import_orders_object_store").expect("valid step name"),
        ChunkSize::new(10).expect("valid chunk size"),
        reader,
        ComponentRevision::new("reader-v1").expect("valid revision"),
        IdentityProcessor,
        ComponentRevision::new("processor-v1").expect("valid revision"),
        NoopWriter,
        ComponentRevision::new("writer-v1").expect("valid revision"),
        ComponentRevision::new("checkpoint-v1").expect("valid revision"),
        restart(ChunkDeliveryMode::AtLeastOnce),
        Arc::new(NoopTransactions),
    )
    .with_stream(
        identity,
        stream,
        contract,
        ComponentRevision::new("orders-object-store-v1").expect("valid revision"),
    )
    .build_chunk_job(
        JobName::new("import_object_store_job").expect("valid job name"),
        DefinitionRevision::new("v1").expect("valid revision"),
    )
    .expect("stream registration matches on both sides");

    assert_eq!(
        job.step_name(),
        &StepName::new("import_orders_object_store").expect("valid")
    );
}

/// A real `PostgreSQL` reader (`item_components::postgres_cursor_reader`,
/// #149's representative shape) assembled through the builder at
/// configuration time only, using a syntactically valid but never-routable
/// `PostgresConfig` -- `postgres_cursor_reader` stores the config and lazily
/// connects on its first `read()`, never at construction, so this proves
/// the builder's generic bounds and surface accept a real `PostgreSQL`
/// component without a live database, exactly like the JSON and
/// multi-resource cases above. Covers issue #152 section 8's `PostgreSQL`
/// requirement.
#[cfg(feature = "postgres")]
#[test]
fn postgres_cursor_reader_registers_consistently_through_with_stream() {
    use oxide_batch::item_components::{
        KeysetColumn, PostgresCursorFormat, PostgresRow, postgres_cursor_reader,
    };
    use oxide_batch::{PostgresConfig, ReaderError, TlsMode};

    let config = PostgresConfig::new("postgresql://user:pass@127.0.0.1:1/nonexistent")
        .expect("syntactically valid configuration; never connected")
        .with_tls_mode(TlsMode::Plaintext);
    let identity = ComponentStreamIdentity::new("orders.postgres_cursor").expect("valid identity");
    let (reader, stream, contract) = postgres_cursor_reader::<u64>(
        config,
        "select id from orders order by id",
        vec![KeysetColumn::i64("id")],
        PostgresCursorFormat::new(),
        |_row: &PostgresRow<'_>| -> Result<u64, ReaderError> {
            unimplemented!("never invoked at configuration time")
        },
        identity.clone(),
    )
    .expect("valid postgres cursor configuration");

    let job = ChunkPipelineBuilder::<u64, u64, _, _, _>::new(
        StepName::new("import_orders_postgres").expect("valid step name"),
        ChunkSize::new(10).expect("valid chunk size"),
        reader,
        ComponentRevision::new("reader-v1").expect("valid revision"),
        IdentityProcessor,
        ComponentRevision::new("processor-v1").expect("valid revision"),
        NoopWriter,
        ComponentRevision::new("writer-v1").expect("valid revision"),
        ComponentRevision::new("checkpoint-v1").expect("valid revision"),
        restart(ChunkDeliveryMode::AtLeastOnce),
        Arc::new(NoopTransactions),
    )
    .with_stream(
        identity,
        stream,
        contract,
        ComponentRevision::new("orders-postgres-v1").expect("valid revision"),
    )
    .build_chunk_job(
        JobName::new("import_postgres_job").expect("valid job name"),
        DefinitionRevision::new("v1").expect("valid revision"),
    )
    .expect("stream registration matches on both sides -- configuration-time only, no connection attempted");

    assert_eq!(
        job.step_name(),
        &StepName::new("import_orders_postgres").expect("valid")
    );
}

#[allow(dead_code)]
fn touch_receipt() -> oxide_batch::ChunkCommitReceipt {
    receipt()
}
