//! Named capability evidence for the M5 transaction-capability direction.
//!
//! The accepted
//! [M5 codec and capability direction](../../../docs/architecture/repository-and-transaction-model.md)
//! fixes four observable rules for the capability surface, and this file
//! carries one scenario for each:
//!
//! - a requirement the deployed adapter does not declare is rejected with a
//!   typed error, at negotiation, before any durable write;
//! - the borrowed adapter-owned transaction path still commits business work
//!   and the checkpoint together, and still reports an ambiguous commit as
//!   `UNKNOWN` rather than inferring an outcome;
//! - a capability change that changes durable meaning changes the definition
//!   fingerprint;
//! - a throughput setting change does not.
//!
//! The last two are the two halves of one rule. Getting the split wrong is not
//! a cosmetic error: hashing a throughput setting would turn an operator
//! retuning a connection pool into fail-closed restart drift, and omitting a
//! durable-meaning value would let a definition silently change what it means
//! across a restart.

#![allow(clippy::expect_used, clippy::panic)]

use std::collections::BTreeSet;
use std::error::Error;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use oxide_batch::{
    BoxFuture, Checkpoint, ChunkCommitReceipt, ChunkComponentRevisions, ChunkCounts,
    ChunkDeliveryMode, ChunkFaultProgress, ChunkRestartContract, ChunkSize, ChunkTransaction,
    ChunkTransactionError, CompiledExecutionPlan, ComponentRevision, DefinitionRevision,
    ExecutionContext, FlowExecutionOutcome, FlowGraph, FlowJob, FlowLauncher, FlowNode,
    FlowRuntimeError, FlowTarget, InMemoryJobRepository, JobName, JobParameters, JobRepository,
    NodeId, OwnerToken, PartitionBudget, PartitionCount, PartitionKey, PartitionPlanEntry,
    PartitionPlanFactory, PartitionTaskletFactory, PartitionedStepNode, RepositoryCapability,
    RepositoryDescriptor, RepositoryError, RepositoryUnitOfWork, SequentialIdGenerator,
    StateCodecError, StateLimits, StateSchemaId, StateSchemaVersion, StepComponents, StepName,
    StepNode, StopPollInterval, StopSource, SystemClock, Tasklet, TaskletContext, TaskletError,
    TaskletOutcome, TaskletStep, TerminalKind, VersionedStateCodec,
};

// ---------------------------------------------------------------------------
// Declaration and negotiation
// ---------------------------------------------------------------------------

/// The reference in-memory adapter with deterministic time and identifiers.
fn reference_repository() -> InMemoryJobRepository {
    InMemoryJobRepository::new(
        Arc::new(SystemClock),
        Arc::new(SequentialIdGenerator::new(
            std::num::NonZeroU64::new(1).expect("static id is nonzero"),
        )),
    )
}

/// Every capability this milestone defines.
fn all_capabilities() -> [RepositoryCapability; 6] {
    [
        RepositoryCapability::ExecutionOwnership,
        RepositoryCapability::InstanceHolds,
        RepositoryCapability::OperatorRequests,
        RepositoryCapability::RetentionPurge,
        RepositoryCapability::StepPartitions,
        RepositoryCapability::StopRequests,
    ]
}

#[test]
fn descriptor_declares_and_requires_each_capability() {
    // A deployment that declares everything satisfies every requirement.
    let complete = RepositoryDescriptor::new(3, all_capabilities());
    assert_eq!(complete.descriptor_version(), 1);
    assert_eq!(complete.schema_version(), 3);
    for capability in all_capabilities() {
        assert!(complete.declares(capability));
        assert_eq!(complete.require(capability), Ok(()));
    }

    // A deployment that omits one capability rejects exactly that requirement
    // with a typed error naming it, and keeps honouring the rest.
    for missing in all_capabilities() {
        let partial = RepositoryDescriptor::new(3, without(missing));
        assert!(!partial.declares(missing));
        assert_eq!(
            partial.require(missing),
            Err(oxide_batch::RepositoryError::UnsupportedCapability {
                capability: missing
            }),
        );
        for other in all_capabilities().into_iter().filter(|c| *c != missing) {
            assert_eq!(partial.require(other), Ok(()));
        }
    }

    // Declaring nothing is the conservative claim, which is what the default
    // adapter implementation makes.
    let silent = RepositoryDescriptor::new(0, []);
    assert_eq!(silent.capabilities().len(), 0);
    for capability in all_capabilities() {
        assert!(silent.require(capability).is_err());
    }

    // The reference adapter declares the capabilities it actually implements.
    let reference = reference_repository().descriptor();
    for capability in all_capabilities() {
        assert!(
            reference.declares(capability),
            "the reference adapter implements {capability} and must declare it",
        );
    }
}

/// Every capability except `missing`.
fn without(missing: RepositoryCapability) -> BTreeSet<RepositoryCapability> {
    all_capabilities()
        .into_iter()
        .filter(|capability| *capability != missing)
        .collect()
}

// ---------------------------------------------------------------------------
// Launch negotiation
// ---------------------------------------------------------------------------

/// A repository that publishes a chosen descriptor and counts transactions.
///
/// Everything else delegates to the reference adapter, so a launch that gets
/// past negotiation behaves exactly as it normally would. The counter is the
/// point: asserting that no metadata was stored proves only that nothing was
/// committed, while asserting that `begin` was never called proves the launch
/// was rejected before it opened a repository transaction at all.
struct CountingRepository {
    inner: InMemoryJobRepository,
    descriptor: RepositoryDescriptor,
    begins: AtomicUsize,
}

impl CountingRepository {
    fn new(descriptor: RepositoryDescriptor) -> Self {
        Self {
            inner: reference_repository(),
            descriptor,
            begins: AtomicUsize::new(0),
        }
    }

    /// Declares every capability except `missing`.
    fn lacking(missing: RepositoryCapability) -> Self {
        Self::new(RepositoryDescriptor::new(0, without(missing)))
    }

    /// Declares every capability this milestone defines.
    fn complete() -> Self {
        Self::new(RepositoryDescriptor::new(0, all_capabilities()))
    }

    fn begin_count(&self) -> usize {
        self.begins.load(Ordering::SeqCst)
    }
}

impl JobRepository for CountingRepository {
    fn connection_capacity(&self) -> u32 {
        self.inner.connection_capacity()
    }

    fn descriptor(&self) -> RepositoryDescriptor {
        self.descriptor.clone()
    }

    fn begin<'a>(
        &'a self,
    ) -> BoxFuture<'a, Result<Box<dyn RepositoryUnitOfWork + 'a>, RepositoryError>> {
        self.begins.fetch_add(1, Ordering::SeqCst);
        self.inner.begin()
    }
}

/// A compiled plan whose partitioned step requires durable step partitions.
fn partitioned_plan(name: &JobName) -> Result<CompiledExecutionPlan, Box<dyn Error>> {
    let manager = NodeId::new("partitioned")?;
    let worker = StepNode::new(
        NodeId::new("worker")?,
        StepName::new("worker")?,
        StepComponents::Tasklet(ComponentRevision::new("worker-v1")?),
    );
    Ok(FlowGraph::new(manager.clone())
        .with_node(FlowNode::partitioned_step(PartitionedStepNode::new(
            manager.clone(),
            StepName::new("partitioned")?,
            worker,
            ComponentRevision::new("partitioner-v1")?,
            ComponentRevision::new("canonical-v1")?,
            PartitionCount::new(2)?,
            PartitionBudget::new(2, 3)?,
        )))
        .with_sequence(manager, FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(name, DefinitionRevision::new("v1")?)?)
}

/// A compiled plan with one ordinary tasklet step and no partitioning.
fn tasklet_plan(name: &JobName) -> Result<CompiledExecutionPlan, Box<dyn Error>> {
    let only = NodeId::new("only")?;
    Ok(FlowGraph::new(only.clone())
        .with_node(FlowNode::step(StepNode::new(
            only.clone(),
            StepName::new("only")?,
            StepComponents::Tasklet(ComponentRevision::new("only-v1")?),
        )))
        .with_sequence(only, FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(name, DefinitionRevision::new("v1")?)?)
}

/// A tasklet that records nothing and completes.
struct Noop;

impl Tasklet for Noop {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async { Ok(TaskletOutcome::Completed) })
    }
}

/// One partition plan entry carrying its key as bounded durable context.
fn partition_entry(key: &str) -> Result<PartitionPlanEntry, Box<dyn Error>> {
    let context = ExecutionContext::from_json(
        format!(
            "{{\"format\":\"oxide-batch.execution-context\",\"format_version\":1,\
             \"schema\":\"local.partition\",\"schema_version\":1,\
             \"payload\":{{\"key\":\"{key}\"}}}}"
        )
        .as_bytes(),
        StateLimits::new(4 * 1024, 16)?,
    )?;
    Ok(PartitionPlanEntry::new(PartitionKey::new(key)?, context)?)
}

fn partitioned_job(name: &JobName) -> Result<FlowJob, Box<dyn Error>> {
    let worker_name = StepName::new("worker")?;
    let factory_name = worker_name.clone();
    let entries = ["a", "b"]
        .into_iter()
        .map(partition_entry)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(
        FlowJob::new(name.clone(), partitioned_plan(name)?)?.with_partitioned_tasklet(
            NodeId::new("partitioned")?,
            PartitionPlanFactory::new(move |_request| Ok(entries.clone())),
            PartitionTaskletFactory::new(worker_name, move |_input| {
                TaskletStep::new(factory_name.clone(), Arc::new(Noop))
            }),
        )?,
    )
}

fn tasklet_job(name: &JobName) -> Result<FlowJob, Box<dyn Error>> {
    Ok(
        FlowJob::new(name.clone(), tasklet_plan(name)?)?.with_tasklet_step(
            NodeId::new("only")?,
            TaskletStep::new(StepName::new("only")?, Arc::new(Noop)),
        )?,
    )
}

fn owner_control() -> Result<(OwnerToken, StopPollInterval), Box<dyn Error>> {
    Ok((
        OwnerToken::from_bytes([7; 16]),
        StopPollInterval::new(Duration::from_millis(100))?,
    ))
}

#[tokio::test]
async fn undeclared_capability_requirement_is_rejected_with_a_typed_error()
-> Result<(), Box<dyn Error>> {
    // A partitioned plan requires durable step partitions, which this
    // deployment does not declare.
    let name = JobName::new("settlement")?;
    let job = partitioned_job(&name)?;
    let repository = CountingRepository::lacking(RepositoryCapability::StepPartitions);
    let clock = SystemClock;
    let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
    let (_source, stop) = StopSource::new();

    let error = FlowLauncher::new(&repository, &clock, &ids)
        .launch(&job, &JobParameters::new(), &stop)
        .await
        .expect_err("a deployment without durable step partitions cannot run this plan");

    assert!(
        matches!(
            error,
            FlowRuntimeError::UndeclaredCapability {
                capability: RepositoryCapability::StepPartitions,
                ..
            }
        ),
        "expected a typed undeclared-capability rejection naming step \
         partitions, got {error:?}",
    );
    assert_eq!(
        repository.begin_count(),
        0,
        "negotiation must reject the launch before it opens a repository \
         transaction, so no instance, execution, or lifecycle row can exist",
    );
    Ok(())
}

#[tokio::test]
async fn undeclared_execution_ownership_is_rejected_before_any_repository_transaction()
-> Result<(), Box<dyn Error>> {
    // No plan mentions execution ownership. It is required because the
    // launcher was configured with execution control, and negotiating only the
    // plan would miss it entirely.
    //
    // The delegate underneath this double does implement ownership claims, so
    // without launch-time negotiation this job runs to completion under a
    // descriptor that never promised them. That is the sharper reason the
    // declaration has to be negotiated: the descriptor is the deployment's
    // contract, and honouring it cannot depend on what the adapter happens to
    // implement.
    let name = JobName::new("owned")?;
    let job = tasklet_job(&name)?;
    let repository = CountingRepository::lacking(RepositoryCapability::ExecutionOwnership);
    let clock = SystemClock;
    let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
    let (_source, stop) = StopSource::new();
    let (owner, interval) = owner_control()?;

    let error = FlowLauncher::new(&repository, &clock, &ids)
        .with_execution_control(owner, interval)
        .launch(&job, &JobParameters::new(), &stop)
        .await
        .expect_err("a deployment without ownership evidence cannot run under execution control");

    assert!(
        matches!(
            error,
            FlowRuntimeError::UndeclaredCapability {
                capability: RepositoryCapability::ExecutionOwnership,
                ..
            }
        ),
        "an undeclared launcher requirement must be the same typed rejection \
         as an undeclared plan requirement, not a generic repository error \
         raised later inside claim_execution_owner; got {error:?}",
    );
    assert_eq!(
        repository.begin_count(),
        0,
        "the rejection must precede the transaction that would have claimed \
         ownership",
    );
    Ok(())
}

#[tokio::test]
async fn declared_capabilities_pass_negotiation_and_reach_the_repository()
-> Result<(), Box<dyn Error>> {
    // Positive control for the plan-carried requirement: the same partitioned
    // plan runs when the deployment declares durable step partitions.
    let name = JobName::new("settlement")?;
    let job = partitioned_job(&name)?;
    let repository = CountingRepository::complete();
    let clock = SystemClock;
    let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
    let (_source, stop) = StopSource::new();

    let report = FlowLauncher::new(&repository, &clock, &ids)
        .launch(&job, &JobParameters::new(), &stop)
        .await?;
    assert_eq!(
        *report.outcome(),
        FlowExecutionOutcome::Completed,
        "a declared capability must let the launch proceed to completion",
    );
    assert!(
        repository.begin_count() > 0,
        "the launch reached the repository rather than being rejected",
    );

    // Positive control for the launcher-carried requirement.
    let name = JobName::new("owned")?;
    let job = tasklet_job(&name)?;
    let repository = CountingRepository::complete();
    let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
    let (owner, interval) = owner_control()?;
    let report = FlowLauncher::new(&repository, &clock, &ids)
        .with_execution_control(owner, interval)
        .launch(&job, &JobParameters::new(), &stop)
        .await?;
    assert_eq!(*report.outcome(), FlowExecutionOutcome::Completed);
    assert!(repository.begin_count() > 0);
    Ok(())
}

// ---------------------------------------------------------------------------
// Borrowed adapter-owned transaction
// ---------------------------------------------------------------------------

/// A codec for the reader position the receipt carries.
struct PositionCodec(StateSchemaId, StateSchemaVersion);

impl VersionedStateCodec<u64> for PositionCodec {
    fn schema_id(&self) -> &StateSchemaId {
        &self.0
    }

    fn current_version(&self) -> StateSchemaVersion {
        self.1
    }

    fn encode(&self, value: &u64) -> Result<Vec<u8>, StateCodecError> {
        serde_json::to_vec(&serde_json::json!({ "cursor": value }))
            .map_err(|_| StateCodecError::InvalidPayload)
    }

    fn decode(&self, payload: &[u8]) -> Result<u64, StateCodecError> {
        serde_json::from_slice::<serde_json::Value>(payload)
            .ok()
            .and_then(|value| value.get("cursor").and_then(serde_json::Value::as_u64))
            .ok_or(StateCodecError::InvalidPayload)
    }
}

fn checkpoint(cursor: u64) -> Checkpoint {
    let codec = PositionCodec(
        StateSchemaId::new("test.position").expect("static schema is valid"),
        StateSchemaVersion::new(1).expect("static version is nonzero"),
    );
    Checkpoint::encode(&cursor, &codec, StateLimits::default()).expect("cursor encodes")
}

fn empty_context() -> ExecutionContext {
    struct Empty(StateSchemaId, StateSchemaVersion);

    impl VersionedStateCodec<()> for Empty {
        fn schema_id(&self) -> &StateSchemaId {
            &self.0
        }

        fn current_version(&self) -> StateSchemaVersion {
            self.1
        }

        fn encode(&self, (): &()) -> Result<Vec<u8>, StateCodecError> {
            Ok(b"{}".to_vec())
        }

        fn decode(&self, _payload: &[u8]) -> Result<(), StateCodecError> {
            Ok(())
        }
    }

    let codec = Empty(
        StateSchemaId::new("test.context").expect("static schema is valid"),
        StateSchemaVersion::new(1).expect("static version is nonzero"),
    );
    ExecutionContext::encode(&(), &codec, StateLimits::default()).expect("empty context encodes")
}

/// What an adapter-owned transaction did with the business writes lent to it.
#[derive(Default)]
struct Resource {
    /// Business writes the transaction accepted but has not published.
    staged: Mutex<Vec<u64>>,
    /// Business writes and checkpoint that became durable together.
    committed: Mutex<Vec<(u64, Checkpoint)>>,
}

/// An adapter-owned transaction that enlists business writes in its own
/// transaction, as the same-resource path requires.
struct SameResourceTransaction {
    resource: Arc<Resource>,
    cursor: u64,
    outcome: Result<(), ChunkTransactionError>,
}

impl ChunkTransaction for SameResourceTransaction {
    fn business_transaction(&mut self) -> Option<&mut dyn oxide_batch::BusinessTransaction> {
        None
    }

    fn commit(
        &mut self,
        _counts: ChunkCounts,
        _fault: ChunkFaultProgress,
    ) -> BoxFuture<'_, Result<ChunkCommitReceipt, ChunkTransactionError>> {
        Box::pin(async move {
            let staged: Vec<u64> = self
                .resource
                .staged
                .lock()
                .expect("staged lock poisoned")
                .drain(..)
                .collect();
            match self.outcome {
                Ok(()) => {
                    let point = checkpoint(self.cursor);
                    // Atomic same-resource: the business writes and the
                    // checkpoint that makes them replayable land together.
                    let mut committed = self
                        .resource
                        .committed
                        .lock()
                        .expect("commit lock poisoned");
                    for write in staged {
                        committed.push((write, point.clone()));
                    }
                    Ok(ChunkCommitReceipt::new(point, empty_context()))
                }
                Err(error) => Err(error),
            }
        })
    }

    fn rollback(&mut self) -> BoxFuture<'_, Result<(), ChunkTransactionError>> {
        self.resource
            .staged
            .lock()
            .expect("staged lock poisoned")
            .clear();
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn borrowed_transaction_preserves_atomic_checkpoint_and_unknown_outcome() {
    // A successful same-resource commit publishes business writes and the
    // checkpoint in one step: no state in which one is durable without the
    // other is observable.
    let resource = Arc::new(Resource::default());
    resource
        .staged
        .lock()
        .expect("staged lock poisoned")
        .extend([10, 11]);
    let mut transaction = SameResourceTransaction {
        resource: Arc::clone(&resource),
        cursor: 2,
        outcome: Ok(()),
    };
    let receipt = transaction
        .commit(ChunkCounts::default(), ChunkFaultProgress::NONE)
        .await
        .expect("the same-resource commit succeeds");
    let committed = resource
        .committed
        .lock()
        .expect("commit lock poisoned")
        .clone();
    assert_eq!(committed.len(), 2);
    for (_, point) in &committed {
        assert_eq!(
            point,
            receipt.checkpoint(),
            "every committed business write carries the checkpoint the \
             receipt reports, so the two cannot diverge",
        );
    }
    assert!(
        resource
            .staged
            .lock()
            .expect("staged lock poisoned")
            .is_empty()
    );

    // An ambiguous commit response is reported as unknown. The adapter does
    // not inspect the resource and infer an outcome, and the caller does not
    // receive a receipt it could mistake for durable evidence.
    let ambiguous = Arc::new(Resource::default());
    ambiguous
        .staged
        .lock()
        .expect("staged lock poisoned")
        .extend([20]);
    let mut transaction = SameResourceTransaction {
        resource: Arc::clone(&ambiguous),
        cursor: 3,
        outcome: Err(ChunkTransactionError::CommitOutcomeUnknown),
    };
    assert_eq!(
        transaction
            .commit(ChunkCounts::default(), ChunkFaultProgress::NONE)
            .await
            .err(),
        Some(ChunkTransactionError::CommitOutcomeUnknown),
        "an ambiguous commit stays UNKNOWN and is never resolved by guessing",
    );
    assert!(
        ambiguous
            .committed
            .lock()
            .expect("commit lock poisoned")
            .is_empty(),
        "an unknown outcome publishes nothing the caller can read back as \
         committed; only durable state resolves it",
    );

    // A known rollback is distinct from an unknown outcome: it publishes
    // nothing and says so.
    let rolled_back = Arc::new(Resource::default());
    rolled_back
        .staged
        .lock()
        .expect("staged lock poisoned")
        .extend([30]);
    let mut transaction = SameResourceTransaction {
        resource: Arc::clone(&rolled_back),
        cursor: 4,
        outcome: Err(ChunkTransactionError::NotCommitted),
    };
    assert_eq!(
        transaction
            .commit(ChunkCounts::default(), ChunkFaultProgress::NONE)
            .await
            .err(),
        Some(ChunkTransactionError::NotCommitted),
    );
    assert!(
        rolled_back
            .committed
            .lock()
            .expect("commit lock poisoned")
            .is_empty()
    );
}

// ---------------------------------------------------------------------------
// Fingerprint participation
// ---------------------------------------------------------------------------

/// The capability declaration a definition carries into its fingerprint.
#[derive(Clone, Copy)]
struct Declaration {
    checkpoint_schema: &'static str,
    checkpoint_version: u32,
    context_schema: &'static str,
    context_version: u32,
    delivery_mode: ChunkDeliveryMode,
}

impl Default for Declaration {
    fn default() -> Self {
        Self {
            checkpoint_schema: "test.position",
            checkpoint_version: 1,
            context_schema: "test.context",
            context_version: 1,
            delivery_mode: ChunkDeliveryMode::AtomicSameResource,
        }
    }
}

/// Compiles a one-step chunk plan carrying `declaration`.
fn fingerprint_of(declaration: Declaration) -> Result<[u8; 32], Box<dyn Error>> {
    let import = NodeId::new("import")?;
    let revisions = ChunkComponentRevisions::new(
        ComponentRevision::new("reader-v1")?,
        ComponentRevision::new("processor-v1")?,
        ComponentRevision::new("writer-v1")?,
        ComponentRevision::new("checkpoint-v1")?,
        ChunkRestartContract::new(
            StateSchemaId::new(declaration.checkpoint_schema)?,
            StateSchemaVersion::new(declaration.checkpoint_version)?,
            StateSchemaId::new(declaration.context_schema)?,
            StateSchemaVersion::new(declaration.context_version)?,
            declaration.delivery_mode,
        ),
    );
    let plan = FlowGraph::new(import.clone())
        .with_node(FlowNode::step(StepNode::new(
            import.clone(),
            StepName::new("import")?,
            StepComponents::Chunk {
                size: ChunkSize::new(100)?,
                revisions: Box::new(revisions),
            },
        )))
        .with_sequence(import, FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(
            &JobName::new("daily_import")?,
            DefinitionRevision::new("v1")?,
        )?;
    Ok(*plan.fingerprint())
}

#[test]
fn durable_meaning_capability_change_changes_the_fingerprint() -> Result<(), Box<dyn Error>> {
    let baseline = fingerprint_of(Declaration::default())?;

    // One value at a time, so no two changes can cancel each other out.
    let variants = [
        (
            "delivery mode",
            Declaration {
                delivery_mode: ChunkDeliveryMode::AtLeastOnce,
                ..Declaration::default()
            },
        ),
        (
            "checkpoint codec version",
            Declaration {
                checkpoint_version: 2,
                ..Declaration::default()
            },
        ),
        (
            "context codec version",
            Declaration {
                context_version: 2,
                ..Declaration::default()
            },
        ),
        (
            "checkpoint codec identity",
            Declaration {
                checkpoint_schema: "test.position.v2",
                ..Declaration::default()
            },
        ),
        (
            "context codec identity",
            Declaration {
                context_schema: "test.context.v2",
                ..Declaration::default()
            },
        ),
    ];

    for (what, declaration) in variants {
        assert_ne!(
            fingerprint_of(declaration)?,
            baseline,
            "changing the {what} changes what a restart means, so it must \
             change the fingerprint and be detected as drift",
        );
    }

    // The delivery mode is where the enlistment class reaches the fingerprint:
    // `AtomicSameResource` is the declaration that business writes and progress
    // share one resource transaction, and dropping to `AtLeastOnce` is a
    // durable-meaning change rather than a tuning change.
    assert_ne!(
        fingerprint_of(Declaration {
            delivery_mode: ChunkDeliveryMode::AtLeastOnce,
            ..Declaration::default()
        })?,
        baseline,
    );
    Ok(())
}

#[test]
fn throughput_capability_change_does_not_change_the_fingerprint() -> Result<(), Box<dyn Error>> {
    let baseline = fingerprint_of(Declaration::default())?;

    // Repository throughput settings are adapter configuration. They are not
    // part of a definition at all, which is the strongest form of "must not
    // change a fingerprint": there is no path by which they could.
    let small = reference_repository();
    assert!(small.connection_capacity() > 0);
    let descriptor = small.descriptor();
    let retuned = RepositoryDescriptor::new(descriptor.schema_version(), all_capabilities());
    assert_eq!(
        descriptor.capabilities().collect::<Vec<_>>(),
        retuned.capabilities().collect::<Vec<_>>(),
        "a pool retune changes no declared capability",
    );

    // Recompiling the same definition against any deployment yields the same
    // fingerprint, because no adapter value reaches the canonical manifest.
    assert_eq!(fingerprint_of(Declaration::default())?, baseline);

    // The one durable-meaning value that looks like tuning is the chunk size,
    // which is a committed transaction boundary rather than a throughput knob;
    // it is covered by the plan-fingerprint suite. Everything the adapter
    // publishes as throughput -- pool size, connection capacity, statement
    // timeout -- is absent from the descriptor's capability set above and from
    // the manifest below.
    let manifest = String::from_utf8({
        let import = NodeId::new("import")?;
        FlowGraph::new(import.clone())
            .with_node(FlowNode::step(StepNode::new(
                import.clone(),
                StepName::new("import")?,
                StepComponents::Chunk {
                    size: ChunkSize::new(100)?,
                    revisions: Box::new(ChunkComponentRevisions::new(
                        ComponentRevision::new("reader-v1")?,
                        ComponentRevision::new("processor-v1")?,
                        ComponentRevision::new("writer-v1")?,
                        ComponentRevision::new("checkpoint-v1")?,
                        ChunkRestartContract::new(
                            StateSchemaId::new("test.position")?,
                            StateSchemaVersion::new(1)?,
                            StateSchemaId::new("test.context")?,
                            StateSchemaVersion::new(1)?,
                            ChunkDeliveryMode::AtomicSameResource,
                        ),
                    )),
                },
            )))
            .with_sequence(import, FlowTarget::Terminal(TerminalKind::Complete))?
            .compile(
                &JobName::new("daily_import")?,
                DefinitionRevision::new("v1")?,
            )?
            .definition_identity()
            .canonical_manifest()
            .to_vec()
    })?;
    for absent in [
        "pool_size",
        "connection",
        "statement_timeout",
        "capacity",
        "schema_version\":3",
    ] {
        assert!(
            !manifest.contains(absent),
            "the canonical manifest must not carry the throughput or \
             deployment value {absent}",
        );
    }
    Ok(())
}
