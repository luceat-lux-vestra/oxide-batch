//! M5 plan and definition-fingerprint stabilization evidence.
//!
//! These are the named scenarios the
//! [M5 design-gate record](../../../docs/project/m5-design-gate-evidence.md)
//! requires for issue #98. Together they pin the fingerprint input set fixed by
//! [ADR-0009](../../../docs/architecture/decisions/0009-definition-fingerprint-input-set.md):
//! every value that selects or reinterprets durable state changes the
//! fingerprint, every excluded value cannot, and a mismatch fails restart closed
//! before any lifecycle write.

#[allow(dead_code)]
#[path = "support/clock.rs"]
mod clock;
#[allow(dead_code)]
#[path = "support/ids.rs"]
mod ids;

use std::collections::BTreeSet;
use std::error::Error;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clock::ManualClock;
use ids::DeterministicIds;
use oxide_batch::{
    ChunkComponentRevisions, ChunkDeliveryMode, ChunkRestartContract, ChunkSize, ComponentRevision,
    DeciderRevision, DecisionInputVersion, DecisionNode, DefinitionIdentity, DefinitionManifest,
    DefinitionRevision, ExitPattern, FailureCategory, FailureId, FailureSummary, FlowGraph,
    FlowNode, FlowTarget, FlowTransition, InFlightPolicy, InMemoryJobRepository, JobInstanceKey,
    JobName, JobParameters, JobRepository, JoinNode, LifecycleTransition, ManifestError, NodeId,
    PartitionBudget, PartitionCount, PartitionedStepNode, RepositoryError, SplitBranch,
    SplitBudget, SplitNode, StartControls, StartLimit, StateSchemaId, StateSchemaVersion,
    StepComponents, StepName, StepNode, TerminalKind,
};

/// Every member name the canonical manifest may contain.
///
/// The set is the projection ADR-0009 fixes. A member added to the projection
/// without amending the input set by a superseding ADR fails this suite, so the
/// rule lives in the repository rather than in a reviewer's memory.
const ALLOWED_MEMBERS: &[&str] = &[
    // Manifest envelope.
    "entry",
    "format",
    "job",
    "nodes",
    "transitions",
    // Every node.
    "id",
    "kind",
    // Step nodes.
    "listeners",
    "policy",
    "start",
    "step",
    "declaration",
    "name",
    "allow_start_if_complete",
    "start_limit",
    // Step declarations.
    "component",
    "components",
    "reader",
    "processor",
    "writer",
    "checkpoint",
    "context",
    "schema",
    "version",
    "size",
    "delivery_mode",
    "in_flight_policy",
    "transaction_boundary",
    // Fault policy identity.
    "backoff",
    "initial_ms",
    "maximum_ms",
    "multiplier",
    "classifier",
    "revision",
    "rules",
    "category",
    "phase",
    "retryable",
    "skip",
    "retry_limit",
    "retry_state_limit",
    "skip_limit",
    // Decision nodes.
    "decision",
    "input_version",
    // Split, join, and partitioned-step nodes.
    "branches",
    "failure_policy",
    "join",
    "aggregation",
    "partition_count",
    "partitioner",
    "step_name",
    "worker",
    // Transitions.
    "pattern",
    "source",
    "target",
    "node",
    "terminal",
];

fn time(offset: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(offset)
}

fn tasklet(id: &str) -> Result<StepNode, Box<dyn Error>> {
    Ok(StepNode::new(
        NodeId::new(id)?,
        StepName::new(id)?,
        StepComponents::Tasklet(ComponentRevision::new(format!("{id}-v1"))?),
    ))
}

fn chunk_contract() -> Result<ChunkRestartContract, Box<dyn Error>> {
    Ok(ChunkRestartContract::new(
        StateSchemaId::new("test.position")?,
        StateSchemaVersion::new(1)?,
        StateSchemaId::new("test.context")?,
        StateSchemaVersion::new(1)?,
        ChunkDeliveryMode::AtomicSameResource,
    ))
}

fn chunk_components(reader: &str) -> Result<ChunkComponentRevisions, Box<dyn Error>> {
    Ok(ChunkComponentRevisions::new(
        ComponentRevision::new(reader)?,
        ComponentRevision::new("processor-v1")?,
        ComponentRevision::new("writer-v1")?,
        ComponentRevision::new("checkpoint-v1")?,
        chunk_contract()?,
    ))
}

/// One knob per restart-relevant or excluded value the scenarios vary.
#[derive(Clone, Copy)]
struct Shape {
    job_name: &'static str,
    entry_step: &'static str,
    reader_revision: &'static str,
    chunk_size: u32,
    start_limit: u32,
    partition_count: u16,
    partitioner: &'static str,
    // Excluded by ADR-0009: throughput bounds only.
    parallel_branches: u8,
    split_pool_size: u32,
    partition_workers: u8,
    partition_pool_size: u32,
}

impl Default for Shape {
    fn default() -> Self {
        Self {
            job_name: "nightly_settlement",
            entry_step: "prepare",
            reader_revision: "reader-v1",
            chunk_size: 100,
            start_limit: 3,
            partition_count: 8,
            partitioner: "by-region-v1",
            parallel_branches: 2,
            split_pool_size: 3,
            partition_workers: 4,
            partition_pool_size: 5,
        }
    }
}

/// Compiles a maximal format-3 plan: chunk and tasklet steps, start controls,
/// a listener, a decision, a split with two branches, and a partitioned step.
fn plan(shape: Shape) -> Result<oxide_batch::CompiledExecutionPlan, Box<dyn Error>> {
    let entry = NodeId::new(shape.entry_step)?;
    let decide = NodeId::new("decide")?;
    let split = NodeId::new("import")?;
    let join = NodeId::new("import_join")?;
    let partition = NodeId::new("settle")?;

    Ok(FlowGraph::new(entry.clone())
        .with_node(FlowNode::step(
            StepNode::new(
                entry.clone(),
                StepName::new(shape.entry_step)?,
                StepComponents::Chunk {
                    size: ChunkSize::new(shape.chunk_size)?,
                    revisions: Box::new(chunk_components(shape.reader_revision)?),
                },
            )
            .with_start_controls(StartControls::new(
                StartLimit::new(shape.start_limit)?,
                false,
            ))
            .with_listener_revision(ComponentRevision::new("audit-listener-v1")?),
        ))
        .with_node(FlowNode::decision(DecisionNode::new(
            decide.clone(),
            DeciderRevision::new("weekday-v1")?,
            DecisionInputVersion::new(1)?,
        )))
        .with_node(FlowNode::split(SplitNode::new(
            split.clone(),
            vec![
                SplitBranch::new(vec![tasklet("import_accounts")?]),
                SplitBranch::new(vec![tasklet("import_orders")?]),
            ],
            join.clone(),
            SplitBudget::new(shape.parallel_branches, shape.split_pool_size)?,
        )))
        .with_node(FlowNode::join(JoinNode::new(join.clone())))
        .with_node(FlowNode::partitioned_step(PartitionedStepNode::new(
            partition.clone(),
            StepName::new("settle")?,
            tasklet("settle_worker")?,
            ComponentRevision::new(shape.partitioner)?,
            ComponentRevision::new("sum-counts-v1")?,
            PartitionCount::new(shape.partition_count)?,
            PartitionBudget::new(shape.partition_workers, shape.partition_pool_size)?,
        )))
        .with_sequence(entry, FlowTarget::Node(decide.clone()))?
        .with_transition(FlowTransition::new(
            decide.clone(),
            ExitPattern::new("SKIP_IMPORT")?,
            FlowTarget::Node(partition.clone()),
        ))
        .with_transition(FlowTransition::new(
            decide,
            ExitPattern::new("*")?,
            FlowTarget::Node(split),
        ))
        .with_sequence(join, FlowTarget::Node(partition.clone()))?
        .with_sequence(partition, FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(
            &JobName::new(shape.job_name)?,
            DefinitionRevision::new("2026-08-03")?,
        )?)
}

fn fingerprint(shape: Shape) -> Result<[u8; 32], Box<dyn Error>> {
    Ok(*plan(shape)?.fingerprint())
}

fn tasklet_identity(
    job: &str,
    step: &str,
    revision: &str,
    component: &str,
) -> Result<DefinitionIdentity, Box<dyn Error>> {
    Ok(DefinitionIdentity::tasklet(
        &JobName::new(job)?,
        &StepName::new(step)?,
        DefinitionRevision::new(revision)?,
        &ComponentRevision::new(component)?,
    )?)
}

#[test]
fn unchanged_definition_recompiles_to_the_same_fingerprint() -> Result<(), Box<dyn Error>> {
    let first = plan(Shape::default())?;
    let second = plan(Shape::default())?;

    assert_eq!(
        first.definition_identity().canonical_manifest(),
        second.definition_identity().canonical_manifest()
    );
    assert_eq!(first.fingerprint(), second.fingerprint());

    // Recompiling the same graph value repeatedly is also stable, so nothing in
    // compilation depends on allocation addresses, iteration order, or a clock.
    let graph_fingerprints: BTreeSet<[u8; 32]> = (0..8)
        .map(|_| fingerprint(Shape::default()))
        .collect::<Result<_, _>>()?;
    assert_eq!(graph_fingerprints.len(), 1);
    Ok(())
}

#[test]
fn restart_relevant_change_changes_the_fingerprint() -> Result<(), Box<dyn Error>> {
    let baseline = fingerprint(Shape::default())?;
    let mutations: [(&str, Shape); 7] = [
        (
            "job name",
            Shape {
                job_name: "nightly_settlement_v2",
                ..Shape::default()
            },
        ),
        (
            "logical step and node id",
            Shape {
                entry_step: "prepare_v2",
                ..Shape::default()
            },
        ),
        (
            "component revision",
            Shape {
                reader_revision: "reader-v2",
                ..Shape::default()
            },
        ),
        (
            "chunk size",
            Shape {
                chunk_size: 101,
                ..Shape::default()
            },
        ),
        (
            "start limit",
            Shape {
                start_limit: 4,
                ..Shape::default()
            },
        ),
        (
            "partition count",
            Shape {
                partition_count: 9,
                ..Shape::default()
            },
        ),
        (
            "partitioner identity",
            Shape {
                partitioner: "by-region-v2",
                ..Shape::default()
            },
        ),
    ];

    let mut seen = BTreeSet::new();
    seen.insert(baseline);
    for (value, shape) in mutations {
        let changed = fingerprint(shape)?;
        assert_ne!(changed, baseline, "{value} must change the fingerprint");
        assert!(
            seen.insert(changed),
            "{value} must not collide with another restart-relevant change"
        );
    }
    Ok(())
}

#[test]
fn restart_relevant_state_and_delivery_changes_change_the_fingerprint() -> Result<(), Box<dyn Error>>
{
    let step = |contract: ChunkRestartContract| -> Result<[u8; 32], Box<dyn Error>> {
        let id = NodeId::new("load")?;
        let plan = FlowGraph::new(id.clone())
            .with_node(FlowNode::step(StepNode::new(
                id.clone(),
                StepName::new("load")?,
                StepComponents::Chunk {
                    size: ChunkSize::new(10)?,
                    revisions: Box::new(ChunkComponentRevisions::new(
                        ComponentRevision::new("reader-v1")?,
                        ComponentRevision::new("processor-v1")?,
                        ComponentRevision::new("writer-v1")?,
                        ComponentRevision::new("checkpoint-v1")?,
                        contract,
                    )),
                },
            )))
            .with_sequence(id, FlowTarget::Terminal(TerminalKind::Complete))?
            .compile(&JobName::new("state")?, DefinitionRevision::new("v1")?)?;
        Ok(*plan.fingerprint())
    };

    let baseline = step(chunk_contract()?)?;
    let newer_checkpoint_schema = step(ChunkRestartContract::new(
        StateSchemaId::new("test.position")?,
        StateSchemaVersion::new(2)?,
        StateSchemaId::new("test.context")?,
        StateSchemaVersion::new(1)?,
        ChunkDeliveryMode::AtomicSameResource,
    ))?;
    let newer_context_schema = step(ChunkRestartContract::new(
        StateSchemaId::new("test.position")?,
        StateSchemaVersion::new(1)?,
        StateSchemaId::new("test.context")?,
        StateSchemaVersion::new(2)?,
        ChunkDeliveryMode::AtomicSameResource,
    ))?;
    let weaker_delivery = step(ChunkRestartContract::new(
        StateSchemaId::new("test.position")?,
        StateSchemaVersion::new(1)?,
        StateSchemaId::new("test.context")?,
        StateSchemaVersion::new(1)?,
        ChunkDeliveryMode::AtLeastOnce,
    ))?;
    let rollback_in_flight =
        step(chunk_contract()?.with_in_flight_policy(InFlightPolicy::RollbackChunk))?;

    for (value, changed) in [
        ("checkpoint schema version", newer_checkpoint_schema),
        ("context schema version", newer_context_schema),
        ("delivery mode", weaker_delivery),
        ("in-flight policy", rollback_in_flight),
    ] {
        assert_ne!(changed, baseline, "{value} must change the fingerprint");
    }
    Ok(())
}

#[test]
fn throughput_only_budget_change_does_not_change_the_fingerprint() -> Result<(), Box<dyn Error>> {
    let baseline = plan(Shape::default())?;
    let retuned = plan(Shape {
        parallel_branches: 1,
        split_pool_size: 8,
        partition_workers: 16,
        partition_pool_size: 32,
        ..Shape::default()
    })?;

    assert_eq!(
        baseline.definition_identity().canonical_manifest(),
        retuned.definition_identity().canonical_manifest(),
        "a retuned budget must not reach the canonical manifest"
    );
    assert_eq!(baseline.fingerprint(), retuned.fingerprint());

    // One knob at a time, so no two budget changes can cancel each other out.
    for shape in [
        Shape {
            parallel_branches: 1,
            ..Shape::default()
        },
        Shape {
            split_pool_size: 9,
            ..Shape::default()
        },
        Shape {
            partition_workers: 1,
            partition_pool_size: 2,
            ..Shape::default()
        },
        Shape {
            partition_pool_size: 64,
            ..Shape::default()
        },
    ] {
        assert_eq!(fingerprint(shape)?, *baseline.fingerprint());
    }
    Ok(())
}

#[tokio::test]
async fn display_name_or_storage_key_change_does_not_change_the_fingerprint()
-> Result<(), Box<dyn Error>> {
    // A definition is identified by its logical values. Storage placement,
    // adapter primary keys, runtime execution identifiers, and the clock that
    // stamps them are all excluded, so persisting the same definition into two
    // differently keyed repositories must produce one fingerprint.
    let compiled = plan(Shape::default())?;
    let mut digests = BTreeSet::new();
    for (seed, at) in [(1_u64, 100_u64), (9_999, 1_754_000_000)] {
        let clock = ManualClock::new(time(at));
        let ids = DeterministicIds::new(NonZeroU64::new(seed).ok_or("nonzero seed")?);
        let repository = InMemoryJobRepository::new(Arc::new(clock), Arc::new(ids));
        let key = JobInstanceKey::new(JobName::new("nightly_settlement")?, &JobParameters::new());

        let mut unit = repository.begin().await?;
        let instance = unit
            .select_or_create_job_instance(&key)
            .await?
            .instance()
            .clone();
        let execution = unit
            .create_job_execution_with_definition(instance.id(), compiled.definition_identity())
            .await?;
        unit.commit().await?;

        // The runtime identifiers differ across the two repositories; the
        // definition identity does not.
        digests.insert(*compiled.definition_identity().manifest_digest());
        assert_ne!(u64::from(execution.id()), 0);
    }
    assert_eq!(digests.len(), 1);

    // No runtime or storage token reaches the canonical bytes.
    let canonical = std::str::from_utf8(compiled.definition_identity().canonical_manifest())?;
    for forbidden in [
        "ob_job_execution",
        "ob_step_partition",
        "instance_id",
        "execution_id",
        "pool",
        "created_at",
        "telemetry",
        "postgres",
    ] {
        assert!(
            !canonical.contains(forbidden),
            "canonical manifest must not contain {forbidden}"
        );
    }
    Ok(())
}

#[test]
fn canonical_manifest_contains_only_allowlisted_members() -> Result<(), Box<dyn Error>> {
    let allowed: BTreeSet<&str> = ALLOWED_MEMBERS.iter().copied().collect();
    let mut found = BTreeSet::new();
    let document: serde_json::Value = serde_json::from_slice(
        plan(Shape::default())?
            .definition_identity()
            .canonical_manifest(),
    )?;
    collect_members(&document, &mut found);

    let unexpected: Vec<&String> = found
        .iter()
        .filter(|member| !allowed.contains(member.as_str()))
        .collect();
    assert!(
        unexpected.is_empty(),
        "a manifest member outside the ADR-0009 input set appeared: {unexpected:?}"
    );

    // The maximal plan must actually exercise the projection, so the allowlist
    // cannot pass by describing an empty manifest.
    for required in ["nodes", "transitions", "partition_count", "branches"] {
        assert!(found.contains(required), "{required} must be projected");
    }
    Ok(())
}

fn collect_members(value: &serde_json::Value, found: &mut BTreeSet<String>) {
    match value {
        serde_json::Value::Object(members) => {
            for (name, child) in members {
                found.insert(name.clone());
                collect_members(child, found);
            }
        }
        serde_json::Value::Array(values) => {
            for child in values {
                collect_members(child, found);
            }
        }
        _ => {}
    }
}

#[test]
fn newer_manifest_format_is_rejected() -> Result<(), Box<dyn Error>> {
    let canonical = plan(Shape::default())?
        .definition_identity()
        .canonical_manifest()
        .to_vec();
    let newer = String::from_utf8(canonical)?.replace("\"format\":3", "\"format\":4");

    assert_eq!(
        DefinitionManifest::read(newer.as_bytes()),
        Err(ManifestError::UnsupportedFormat {
            format: 4,
            supported: 3,
        })
    );
    // The digest is never consulted for a format the runtime cannot interpret,
    // so a newer manifest cannot be admitted by supplying a matching digest.
    let digest: [u8; 32] = <sha2::Sha256 as sha2::Digest>::digest(newer.as_bytes()).into();
    assert_eq!(
        DefinitionManifest::read_verified(newer.as_bytes(), &digest),
        Err(ManifestError::UnsupportedFormat {
            format: 4,
            supported: 3,
        })
    );
    Ok(())
}

#[tokio::test]
async fn fingerprint_mismatch_without_an_edge_rejects_restart_before_any_write()
-> Result<(), Box<dyn Error>> {
    let clock = ManualClock::new(time(100));
    let ids = DeterministicIds::new(NonZeroU64::MIN);
    let repository = InMemoryJobRepository::new(Arc::new(clock.clone()), Arc::new(ids));
    let key = JobInstanceKey::new(JobName::new("daily_import")?, &JobParameters::new());
    let checkpointing = tasklet_identity("daily_import", "import", "v1", "tasklet-v1")?;
    let proposed = tasklet_identity("daily_import", "import", "v2", "tasklet-v2")?;
    assert_ne!(
        checkpointing.manifest_digest(),
        proposed.manifest_digest(),
        "the scenario needs two distinct fingerprints"
    );

    let mut unit = repository.begin().await?;
    let instance = unit
        .select_or_create_job_instance(&key)
        .await?
        .instance()
        .clone();
    let first = unit
        .create_job_execution_with_definition(instance.id(), &checkpointing)
        .await?;
    unit.commit().await?;
    clock.set(time(101));
    let mut unit = repository.begin().await?;
    unit.transition_job_execution(
        first.id(),
        first.version(),
        LifecycleTransition::failed(
            time(101),
            FailureSummary::new(FailureCategory::UserComponent, FailureId::new(700)?),
        ),
    )
    .await?;
    unit.commit().await?;

    let before = {
        let mut unit = repository.begin().await?;
        let executions = unit.job_executions(instance.id()).await?;
        unit.rollback().await?;
        executions
    };

    let mut unit = repository.begin().await?;
    let rejected = unit
        .create_job_execution_with_definition(instance.id(), &proposed)
        .await;
    assert_eq!(
        rejected,
        Err(RepositoryError::IncompatibleDefinition {
            instance_id: instance.id(),
        })
    );
    unit.rollback().await?;

    let after = {
        let mut unit = repository.begin().await?;
        let executions = unit.job_executions(instance.id()).await?;
        unit.rollback().await?;
        executions
    };
    assert_eq!(
        before.len(),
        after.len(),
        "a rejected restart must not create an execution"
    );
    for (before, after) in before.iter().zip(after.iter()) {
        assert_eq!(before.id(), after.id());
        assert_eq!(before.version(), after.version());
        assert_eq!(before.metadata().status(), after.metadata().status());
    }
    Ok(())
}

#[tokio::test]
async fn revision_rebound_to_a_new_fingerprint_is_drift() -> Result<(), Box<dyn Error>> {
    let clock = ManualClock::new(time(100));
    let ids = DeterministicIds::new(NonZeroU64::MIN);
    let repository = InMemoryJobRepository::new(Arc::new(clock.clone()), Arc::new(ids));
    let key = JobInstanceKey::new(JobName::new("daily_import")?, &JobParameters::new());
    let recorded = tasklet_identity("daily_import", "import", "v1", "tasklet-v1")?;
    // The same application revision, rebound to different restart-relevant
    // values. ADR-0004 calls this drift and never reconciles it.
    let rebound = tasklet_identity("daily_import", "import", "v1", "tasklet-v1-rebuilt")?;
    assert_eq!(recorded.revision().as_str(), rebound.revision().as_str());
    assert_ne!(recorded.manifest_digest(), rebound.manifest_digest());

    let mut unit = repository.begin().await?;
    let instance = unit
        .select_or_create_job_instance(&key)
        .await?
        .instance()
        .clone();
    let first = unit
        .create_job_execution_with_definition(instance.id(), &recorded)
        .await?;
    unit.commit().await?;
    clock.set(time(101));
    let mut unit = repository.begin().await?;
    unit.transition_job_execution(
        first.id(),
        first.version(),
        LifecycleTransition::failed(
            time(101),
            FailureSummary::new(FailureCategory::UserComponent, FailureId::new(700)?),
        ),
    )
    .await?;
    unit.commit().await?;

    let mut unit = repository.begin().await?;
    let rejected = unit
        .create_job_execution_with_definition(instance.id(), &rebound)
        .await;
    assert!(
        matches!(rejected, Err(RepositoryError::DefinitionDrift { .. })),
        "a rebound revision must be drift rather than an incompatible definition"
    );
    unit.rollback().await?;

    let mut unit = repository.begin().await?;
    let executions = unit.job_executions(instance.id()).await?;
    unit.rollback().await?;
    assert_eq!(executions.len(), 1, "drift must not create an execution");
    Ok(())
}

#[test]
fn format1_and_format2_bytes_are_never_rewritten() -> Result<(), Box<dyn Error>> {
    // A format-1 identity is produced by its own constructor and is not
    // re-encoded by the format-2 and format-3 projections that landed later.
    let format1 = tasklet_identity("daily_import", "import", "v1", "tasklet-v1")?;
    assert_eq!(format1.manifest_format(), 1);
    assert_eq!(
        std::str::from_utf8(format1.canonical_manifest())?,
        concat!(
            r#"{"component":{"tasklet":"tasklet-v1"},"delivery_mode":"best_effort","format":1,"#,
            r#""job":"daily_import","kind":"tasklet","restart_state":"none","step":"import","#,
            r#""transaction_boundary":"tasklet_completion"}"#
        )
    );
    let reread =
        DefinitionManifest::read_verified(format1.canonical_manifest(), format1.manifest_digest())?;
    assert_eq!(reread.format(), 1);

    // A format-2 manifest keeps its own bytes when a format-3 node kind exists
    // in the same runtime: the projection is selected by the graph, not by the
    // newest format the build understands.
    let id = NodeId::new("load")?;
    let format2 = FlowGraph::new(id.clone())
        .with_node(FlowNode::step(tasklet("load")?))
        .with_sequence(id, FlowTarget::Terminal(TerminalKind::Complete))?
        .compile(
            &JobName::new("daily_import")?,
            DefinitionRevision::new("v1")?,
        )?;
    assert_eq!(format2.manifest_format(), 2);
    let canonical = std::str::from_utf8(format2.definition_identity().canonical_manifest())?;
    assert!(canonical.contains(r#""format":2"#));
    assert!(
        !canonical.contains("partition_count") && !canonical.contains("branches"),
        "a format-2 manifest must not gain format-3 members"
    );
    Ok(())
}
