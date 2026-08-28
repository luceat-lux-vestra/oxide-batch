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
    DelimitedDialect, DelimitedRecord, IdentityProcessor, NoopWriter, delimited_reader,
};
use oxide_batch::{
    BackoffOutcome, BackoffPolicy, BackoffSleeper, BoxFuture, BoxedProcessor, BoxedReader,
    BoxedWriter, ChunkComponentRevisions, ChunkDeliveryMode, ChunkJob, ChunkPipelineBuilder,
    ChunkRestartContract, ChunkSize, ChunkStep, ClassifierRevision, ComponentRevision,
    ComponentStreamIdentity, DefinitionError, DefinitionRevision, FailureCategory, FaultAction,
    FaultClassifier, FaultPhase, FaultPolicy, FaultRule, FaultRuntime, FlowGraph, FlowJob,
    FlowNode, FlowTarget, InMemoryFaultState, ItemCountCompletionPolicy, JobName, NodeId,
    RetryLimit, RetryStateLimit, SkipLimit, StateSchemaId, StateSchemaVersion, StepName, StepNode,
    StopSource, TerminalKind,
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

#[allow(dead_code)]
fn touch_receipt() -> oxide_batch::ChunkCommitReceipt {
    receipt()
}
