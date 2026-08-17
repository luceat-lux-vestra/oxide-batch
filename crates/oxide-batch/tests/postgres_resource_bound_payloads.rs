//! Durable and in-memory payload ceilings at the boundary and one past it.
//!
//! This report covers the buffers and the retry cache: every framework-owned
//! resource whose bound is a size or a count that a caller supplies. Their
//! overload policy is uniformly fail-closed, so each is checked twice — at its
//! declared ceiling, which must be accepted, and one unit past it, which must
//! be refused.
//!
//! Both halves are load-bearing, and the accepted half is the one that is
//! usually left out. A test that only checks the refusal passes just as well
//! against a bound enforced one short, ten short, or at zero, which is a
//! different contract from the declared one and a worse one: it refuses work an
//! operator was told would be accepted. So every cell here names the value it
//! offered, and the retained evidence carries the offered value rather than
//! only the verdict.
//!
//! Four of the resources are durable, and for those a construction check is not
//! enough. A key the framework accepts and the schema truncates is a bound that
//! held in the wrong place, and it would not be visible until a restart matched
//! the wrong partition. So the boundary-sized partition key, partition context,
//! definition manifest, and instance key are written through the adapter and
//! read back, and the report requires the bytes to come back identical rather
//! than merely to have been accepted.
//!
//! The refusals of the durable ones are checked for the same absence the
//! worker report checks: a payload one byte too long must be refused before
//! anything about it reaches the database.
//!
//! The identifier-and-reference bounds are swept as a table rather than written
//! out one scenario each. There are fourteen of them, every one is a
//! caller-supplied string that becomes part of a durable record or an operator
//! request, and there is nothing true of any one of them that is not true of
//! all. A fifteenth is a line in the table, and the campaign's reconciliation
//! separately requires it to have been named in the denominator, so the cheap
//! shape here does not make it easy to add one nobody proves.

#![cfg(feature = "postgres")]

#[path = "resource_bounds/mod.rs"]
mod resource_bounds;

use std::error::Error;
use std::num::NonZeroU64;
use std::sync::Arc;

use oxide_batch::BoxFuture;
use oxide_batch::{
    ActorRef, CaCertificate, ClassifierRevision, ComponentRevision, DefinitionRevision,
    ExecutionContext, ExecutionVersion, ExitCode, ExitPattern, FailureCategory, FaultPhase,
    FaultStateEntry, FaultStateEnvelope, FlowGraph, FlowJob, FlowLauncher, FlowNode, FlowTarget,
    ItemListenerSet, JobInstanceKey, JobName, JobParameter, JobParameters, JobRepository,
    MAX_ACTOR_REF_BYTES, MAX_NODES, MAX_OPERATION_ID_BYTES, MAX_OUTGOING_TRANSITIONS,
    MAX_PARTITION_CONTEXT_BYTES, MAX_PARTITION_KEY_BYTES, MAX_PATTERN_BYTES, MAX_REASON_CODE_BYTES,
    MAX_TRANSITIONS, NodeId, OperationId, ParameterName, ParameterRole, ParameterValue,
    PartitionBudget, PartitionCount, PartitionKey, PartitionPlanEntry, PartitionPlanFactory,
    PartitionTaskletFactory, PartitionedStepNode, PostgresJobRepository, PostgresMigrator,
    ReadListener, ReasonCode, RecoveryRequest, RetryKey, RetryOrdinal, RetryStateLimit,
    SequentialIdGenerator, StateCodecError, StateLimits, StateSchemaId, StateSchemaUpgrade,
    StateSchemaVersion, StepComponents, StepName, StepNode, StopSource, Tasklet, TaskletContext,
    TaskletError, TaskletOutcome, TaskletStep, TerminalKind, VersionedStateCodec,
};
use serde_json::{Value, json};

use resource_bounds::{
    Failure, FixedClock, config, execution_manifest, major_version, migrator_url, remove_job,
    retain_observation, runtime_url, server_version,
};

/// The report identifier the runner reconciles this observation under.
const REPORT: &str = "bounded-payloads";

/// The job whose durable payloads are written at their ceiling.
const BOUNDARY_JOB: &str = "m5_resource_bound_payload_boundary";

/// The job whose durable payloads are one unit past their ceiling.
const REFUSED_JOB: &str = "m5_resource_bound_payload_refused";

/// The declared ceiling on any durable checkpoint or execution context.
const DURABLE_STATE_CEILING: usize = 1024 * 1024;

/// The declared ceiling on the raw input the instance key is digested from.
const INSTANCE_KEY_CEILING: usize = 1024 * 1024;

/// The declared ceiling on a certificate bundle the adapter will read.
const CA_CERTIFICATE_CEILING: usize = 1024 * 1024;

/// The declared ceiling on one durable definition manifest.
const MANIFEST_CEILING: usize = 64 * 1024;

/// The declared ceiling on one resolved state-upgrade chain.
const UPGRADE_CHAIN_CEILING: usize = 64;

#[test]
fn bounded_payloads_are_refused_one_byte_over_the_ceiling() -> Result<(), Box<dyn Error>> {
    let Some(runtime) = runtime_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set");
        return Ok(());
    };
    let Some(migrator) = migrator_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL is not set");
        return Ok(());
    };

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(report(runtime, migrator))
}

/// Runs every payload obligation and retains one observation.
async fn report(runtime: String, migrator: String) -> Result<(), Box<dyn Error>> {
    PostgresMigrator::migrate(&config(migrator.clone())?).await?;
    for job in [BOUNDARY_JOB, REFUSED_JOB] {
        remove_job(&migrator, job).await?;
    }

    let server = server_version(&runtime).await?;
    let mut cells = Vec::new();
    cells.extend(partition_cells());
    cells.extend(retry_cache_cells()?);
    let largest_chain = largest_manifest_chain();
    cells.extend(definition_cells(largest_chain)?);
    cells.extend(state_cells());
    cells.extend(upgrade_chain_cells());
    cells.extend(listener_cells());
    cells.extend(identifier_cells());

    let mut violations: Vec<String> = cells.iter().filter_map(Cell::violation).collect();

    let durable = round_trip_the_boundary(&runtime).await?;
    violations.extend(durable.violations.clone());

    let key = instance_key_bound(&runtime).await?;
    violations.extend(key.violations.clone());

    let document = json!({
        "report": REPORT,
        "scenario": "bounded_payloads_are_refused_one_byte_over_the_ceiling",
        "server_version": server,
        "postgres_major_version": major_version(&server),
        "resources": resource_rollup(&cells),
        "cells": cells.iter().map(Cell::evidence).collect::<Vec<_>>(),
        "definition_bound": json!({
            "node_ceiling": MAX_NODES,
            "transition_ceiling": MAX_TRANSITIONS,
            "manifest_ceiling": MANIFEST_CEILING,
            "largest_accepted_chain": largest_chain,
            "largest_accepted_manifest_bytes": manifest_bytes(largest_chain)?,
            "binding_bound": "definition-manifest",
            "note": "The node and transition ceilings are not independently \
                     reachable: the canonical manifest crosses its own ceiling \
                     first, so a graph is refused for its encoded size before it \
                     is refused for its node count. Both are still declared and \
                     both still refuse, and the campaign records which one an \
                     author actually meets.",
        }),
        "durable_round_trip": durable.evidence(),
        "instance_key": key.evidence(),
        "execution_manifest": execution_manifest()?,
        "violations": violations,
        "passed": violations.is_empty(),
    });
    retain_observation(REPORT, &document)?;

    for job in [BOUNDARY_JOB, REFUSED_JOB] {
        remove_job(&migrator, job).await?;
    }

    assert!(
        violations.is_empty(),
        "the bounded-payload report observed {violations:#?}",
    );
    Ok(())
}

/// Summarizes the cells per resource, as the runner reconciles them.
fn resource_rollup(cells: &[Cell]) -> Vec<Value> {
    let mut rollup: Vec<Value> = Vec::new();
    let mut seen: Vec<&str> = Vec::new();

    for cell in cells {
        if seen.contains(&cell.resource) {
            continue;
        }
        seen.push(cell.resource);
        let mine = cells.iter().filter(|other| other.resource == cell.resource);
        let accepted = mine
            .clone()
            .filter(|other| other.expected && other.accepted == other.expected)
            .count();
        let refused = mine
            .clone()
            .filter(|other| !other.expected && other.accepted == other.expected)
            .count();
        let ceiling = cell.ceiling;
        let offered = mine.clone().map(|other| other.value).max().unwrap_or(0);
        // The peak is the largest value the framework actually accepted, not
        // the ceiling it declares: for a resource whose only proof is a pair of
        // constructor calls, those two are the same number only when the
        // declared ceiling is reachable, and the definition graph is where they
        // are not.
        let peak = mine
            .clone()
            .filter(|other| other.expected && other.accepted)
            .map(|other| other.value)
            .max()
            .unwrap_or(0);
        let violations = mine.clone().filter_map(Cell::violation).collect::<Vec<_>>();

        rollup.push(json!({
            "resource": cell.resource,
            "overload_policy": "fail-closed",
            "configured_ceiling": ceiling,
            "offered_load": offered,
            "observed_peak_occupancy": peak,
            "accepted_at_boundary": accepted,
            "rejections": refused,
            "waits": 0,
            "drops": 0,
            "violations": violations,
            "passed": violations.is_empty(),
        }));
    }

    rollup
}

/// Reports the partition key and context at and past their ceilings.
fn partition_cells() -> Vec<Cell> {
    vec![
        Cell::new(
            "partition-key",
            MAX_PARTITION_KEY_BYTES as u64,
            "at the ceiling",
            MAX_PARTITION_KEY_BYTES as u64,
            PartitionKey::new("k".repeat(MAX_PARTITION_KEY_BYTES)).is_ok(),
            true,
        ),
        Cell::new(
            "partition-key",
            MAX_PARTITION_KEY_BYTES as u64,
            "one byte past the ceiling",
            MAX_PARTITION_KEY_BYTES as u64 + 1,
            PartitionKey::new("k".repeat(MAX_PARTITION_KEY_BYTES + 1)).is_ok(),
            false,
        ),
        Cell::new(
            "partition-key",
            MAX_PARTITION_KEY_BYTES as u64,
            "an empty key",
            0,
            PartitionKey::new("").is_ok(),
            false,
        ),
        // The ceiling belongs to the durable partition entry rather than to
        // the envelope: an execution context of any size inside the durable
        // state limits is a legal envelope, and what a partition may carry is
        // narrower than what the envelope may hold. Checking the envelope alone
        // would report the wrong bound holding.
        Cell::new(
            "partition-context",
            MAX_PARTITION_CONTEXT_BYTES as u64,
            "at the ceiling",
            MAX_PARTITION_CONTEXT_BYTES as u64,
            plan_entry(MAX_PARTITION_CONTEXT_BYTES).is_ok(),
            true,
        ),
        Cell::new(
            "partition-context",
            MAX_PARTITION_CONTEXT_BYTES as u64,
            "one byte past the ceiling",
            MAX_PARTITION_CONTEXT_BYTES as u64 + 1,
            plan_entry(MAX_PARTITION_CONTEXT_BYTES + 1).is_ok(),
            false,
        ),
    ]
}

/// Reports the retry cache at and past its entry, byte, and declared ceilings.
fn retry_cache_cells() -> Result<Vec<Cell>, Box<dyn Error>> {
    let full = envelope(FaultStateEnvelope::MAX_ENTRIES)?;
    let canonical = full.to_canonical_json()?.len();

    Ok(vec![
        Cell::new(
            "retry-cache-entries",
            FaultStateEnvelope::MAX_ENTRIES as u64,
            "at the ceiling",
            FaultStateEnvelope::MAX_ENTRIES as u64,
            envelope(FaultStateEnvelope::MAX_ENTRIES).is_ok(),
            true,
        ),
        Cell::new(
            "retry-cache-entries",
            FaultStateEnvelope::MAX_ENTRIES as u64,
            "one entry past the ceiling",
            FaultStateEnvelope::MAX_ENTRIES as u64 + 1,
            envelope(FaultStateEnvelope::MAX_ENTRIES + 1).is_ok(),
            false,
        ),
        // The entry ceiling only bounds memory if a full envelope also fits the
        // durable byte ceiling. If it did not, the two bounds would contradict
        // each other and a legal envelope would be unwritable.
        Cell::new(
            "retry-cache-bytes",
            FaultStateEnvelope::MAX_BYTES as u64,
            "the canonical bytes of a full envelope",
            canonical as u64,
            canonical <= FaultStateEnvelope::MAX_BYTES,
            true,
        ),
        Cell::new(
            "retry-cache-bytes",
            FaultStateEnvelope::MAX_BYTES as u64,
            "one byte past the ceiling, presented as durable bytes",
            FaultStateEnvelope::MAX_BYTES as u64 + 1,
            FaultStateEnvelope::from_canonical_json(
                FaultStateEnvelope::FORMAT_VERSION,
                FaultStateEnvelope::FORMAT,
                FaultStateEnvelope::SCHEMA_VERSION,
                &vec![b'{'; FaultStateEnvelope::MAX_BYTES + 1],
                &[0; 32],
            )
            .is_ok(),
            false,
        ),
        Cell::new(
            "declared-retry-state-capacity",
            FaultStateEnvelope::MAX_ENTRIES as u64,
            "at the ceiling",
            FaultStateEnvelope::MAX_ENTRIES as u64,
            RetryStateLimit::new(u32::try_from(FaultStateEnvelope::MAX_ENTRIES)?).is_ok(),
            true,
        ),
        Cell::new(
            "declared-retry-state-capacity",
            FaultStateEnvelope::MAX_ENTRIES as u64,
            "one past the ceiling",
            FaultStateEnvelope::MAX_ENTRIES as u64 + 1,
            RetryStateLimit::new(u32::try_from(FaultStateEnvelope::MAX_ENTRIES)? + 1).is_ok(),
            false,
        ),
        Cell::new(
            "declared-retry-state-capacity",
            FaultStateEnvelope::MAX_ENTRIES as u64,
            "zero retained keys",
            0,
            RetryStateLimit::new(0).is_ok(),
            false,
        ),
    ])
}

/// Reports the definition graph bounds at and past their ceilings.
///
/// The node ceiling and the manifest ceiling bound the same object from two
/// directions, and the report finds which one binds first rather than assuming
/// it: a graph is refused as soon as its canonical manifest is too large, which
/// happens well below `MAX_NODES` steps. So the accepted case here is the
/// largest chain the manifest admits, and the node ceiling is proved by its
/// refusal alone. `definition_bound` records where the two meet.
fn definition_cells(largest_chain: usize) -> Result<Vec<Cell>, Box<dyn Error>> {
    Ok(vec![
        Cell::new(
            "outgoing-transitions-per-node",
            MAX_OUTGOING_TRANSITIONS as u64,
            "at the ceiling",
            MAX_OUTGOING_TRANSITIONS as u64,
            fans_out(MAX_OUTGOING_TRANSITIONS),
            true,
        ),
        Cell::new(
            "outgoing-transitions-per-node",
            MAX_OUTGOING_TRANSITIONS as u64,
            "one past the ceiling",
            MAX_OUTGOING_TRANSITIONS as u64 + 1,
            fans_out(MAX_OUTGOING_TRANSITIONS + 1),
            false,
        ),
        Cell::new(
            "definition-nodes",
            MAX_NODES as u64,
            "the largest chain any bound admits",
            largest_chain as u64,
            chain_of(largest_chain).is_ok(),
            true,
        ),
        Cell::new(
            "definition-nodes",
            MAX_NODES as u64,
            "one node past the ceiling",
            MAX_NODES as u64 + 1,
            chain_of(MAX_NODES + 1).is_ok(),
            false,
        ),
        Cell::new(
            "definition-transitions",
            MAX_TRANSITIONS as u64,
            "the transitions of the largest chain any bound admits",
            (largest_chain * 2) as u64,
            largest_chain * 2 <= MAX_TRANSITIONS,
            true,
        ),
        Cell::new(
            "definition-manifest",
            MANIFEST_CEILING as u64,
            "the largest chain that fits the ceiling",
            manifest_bytes(largest_chain)? as u64,
            manifest_bytes(largest_chain)? <= MANIFEST_CEILING,
            true,
        ),
        Cell::new(
            "definition-manifest",
            MANIFEST_CEILING as u64,
            "one node past the largest chain that fits",
            // The refused graph has no manifest to measure, so the cell records
            // the chain length rather than a size that does not exist.
            largest_chain as u64 + 1,
            chain_of(largest_chain + 1).is_ok(),
            false,
        ),
    ])
}

/// Reports the durable-state envelope and upgrade-chain bounds.
fn state_cells() -> Vec<Cell> {
    vec![
        Cell::new(
            "durable-state-envelope",
            DURABLE_STATE_CEILING as u64,
            "at the ceiling",
            DURABLE_STATE_CEILING as u64,
            StateLimits::new(DURABLE_STATE_CEILING, 64).is_ok(),
            true,
        ),
        Cell::new(
            "durable-state-envelope",
            DURABLE_STATE_CEILING as u64,
            "one byte past the ceiling",
            DURABLE_STATE_CEILING as u64 + 1,
            StateLimits::new(DURABLE_STATE_CEILING + 1, 64).is_ok(),
            false,
        ),
        Cell::new(
            "durable-state-envelope",
            DURABLE_STATE_CEILING as u64,
            "one level past the depth ceiling",
            65,
            StateLimits::new(DURABLE_STATE_CEILING, 65).is_ok(),
            false,
        ),
        Cell::new(
            "ca-certificate",
            CA_CERTIFICATE_CEILING as u64,
            "at the ceiling",
            CA_CERTIFICATE_CEILING as u64,
            CaCertificate::new(vec![b'-'; CA_CERTIFICATE_CEILING]).is_ok(),
            true,
        ),
        Cell::new(
            "ca-certificate",
            CA_CERTIFICATE_CEILING as u64,
            "one byte past the ceiling",
            CA_CERTIFICATE_CEILING as u64 + 1,
            CaCertificate::new(vec![b'-'; CA_CERTIFICATE_CEILING + 1]).is_ok(),
            false,
        ),
    ]
}

/// Reports the bounded upgrade chain a codec may declare.
///
/// The chain is walked for real rather than counted: a payload recorded at the
/// oldest version is decoded through every declared edge, so the bound is the
/// number of upgrades the framework will actually apply rather than the number
/// a codec is allowed to list.
fn upgrade_chain_cells() -> Vec<Cell> {
    vec![
        Cell::new(
            "state-upgrade-chain",
            UPGRADE_CHAIN_CEILING as u64,
            "a chain of exactly the ceiling",
            UPGRADE_CHAIN_CEILING as u64,
            walks_chain(UPGRADE_CHAIN_CEILING),
            true,
        ),
        Cell::new(
            "state-upgrade-chain",
            UPGRADE_CHAIN_CEILING as u64,
            "one edge past the ceiling",
            UPGRADE_CHAIN_CEILING as u64 + 1,
            walks_chain(UPGRADE_CHAIN_CEILING + 1),
            false,
        ),
    ]
}

/// Reports the bounded item-listener registration.
fn listener_cells() -> Vec<Cell> {
    let ceiling = ItemListenerSet::<(), ()>::MAX_LISTENERS;
    vec![
        Cell::new(
            "item-listeners",
            ceiling as u64,
            "at the ceiling",
            ceiling as u64,
            registers_listeners(ceiling),
            true,
        ),
        Cell::new(
            "item-listeners",
            ceiling as u64,
            "one past the ceiling",
            ceiling as u64 + 1,
            registers_listeners(ceiling + 1),
            false,
        ),
    ]
}

/// Reports every bounded identifier and reference, as one table.
///
/// Twelve subjects, not the fourteen symbols the scope names: two of the
/// fourteen — the cursor-name column encoding and the CLI's interactive
/// confirmation read — are validated only by a function this campaign's
/// public-API surface cannot reach (an internal cursor encoder and a raw
/// `io::stdin()` byte loop respectively, neither a constructible type), and
/// the scope document argues them out of scope by name rather than silently
/// dropping them. Each of the twelve real subjects here carries its own
/// declared ceiling and the value actually offered, not a placeholder,
/// because a subject proved only by a boolean accept/refuse pair is not a
/// non-vacuous boundary proof.
///
/// Four of the twelve ceilings have no `pub` constant to import: the bound
/// declaration convention makes visibility irrelevant to what the campaign
/// is answerable for, so a private ceiling is still declared here as a
/// literal that mirrors it, commented with the constant it stands for.
fn identifier_cells() -> Vec<Cell> {
    let mut cells = Vec::new();
    for (subject, ceiling, at, over) in identifier_subjects() {
        let ceiling = ceiling as u64;
        cells.push(Cell::named(
            "bounded-identifier-text",
            subject,
            "at the ceiling",
            ceiling,
            ceiling,
            at,
            true,
        ));
        cells.push(Cell::named(
            "bounded-identifier-text",
            subject,
            "one byte past the ceiling",
            ceiling,
            ceiling + 1,
            over,
            false,
        ));
    }
    cells
}

/// One accept/refuse pair per bounded-identifier-text subject: its name, its
/// declared ceiling, whether a value exactly at the ceiling was accepted, and
/// whether one byte past it was refused.
///
/// Private ceilings this campaign cannot import are mirrored here as
/// literals: the bound declaration convention makes visibility irrelevant to
/// what the campaign is answerable for, so a private ceiling is still
/// declared, commented with the constant it stands for.
fn identifier_subjects() -> Vec<(&'static str, usize, bool, bool)> {
    const MAX_EXIT_CODE_BYTES: usize = 64;
    const MAX_PARAMETER_NAME_BYTES: usize = 128;
    const MAX_PARAMETER_STRING_BYTES: usize = 64 * 1024;
    const MAX_SCHEMA_ID_BYTES: usize = 128;
    const MAX_DOMAIN_NAME_BYTES: usize = 128;
    const MAX_TOKEN_BYTES: usize = 128;
    const MAX_RECOVERY_REASON_BYTES: usize = 64;
    const MAX_OPERATOR_REFERENCE_BYTES: usize = 128;

    let version = ExecutionVersion::INITIAL;
    let digest = [0_u8; 32];

    let mut subjects = vec![
        (
            "operation-id",
            MAX_OPERATION_ID_BYTES,
            OperationId::new("o".repeat(MAX_OPERATION_ID_BYTES)).is_ok(),
            OperationId::new("o".repeat(MAX_OPERATION_ID_BYTES + 1)).is_ok(),
        ),
        (
            "reason-code",
            MAX_REASON_CODE_BYTES,
            ReasonCode::new("R".repeat(MAX_REASON_CODE_BYTES)).is_ok(),
            ReasonCode::new("R".repeat(MAX_REASON_CODE_BYTES + 1)).is_ok(),
        ),
        (
            "actor-ref",
            MAX_ACTOR_REF_BYTES,
            ActorRef::new("a".repeat(MAX_ACTOR_REF_BYTES)).is_ok(),
            ActorRef::new("a".repeat(MAX_ACTOR_REF_BYTES + 1)).is_ok(),
        ),
        (
            "exit-pattern",
            MAX_PATTERN_BYTES,
            ExitPattern::new("P".repeat(MAX_PATTERN_BYTES)).is_ok(),
            ExitPattern::new("P".repeat(MAX_PATTERN_BYTES + 1)).is_ok(),
        ),
        (
            "exit-code",
            MAX_EXIT_CODE_BYTES,
            ExitCode::new("C".repeat(MAX_EXIT_CODE_BYTES)).is_ok(),
            ExitCode::new("C".repeat(MAX_EXIT_CODE_BYTES + 1)).is_ok(),
        ),
        (
            "parameter-name",
            MAX_PARAMETER_NAME_BYTES,
            ParameterName::new("p".repeat(MAX_PARAMETER_NAME_BYTES)).is_ok(),
            ParameterName::new("p".repeat(MAX_PARAMETER_NAME_BYTES + 1)).is_ok(),
        ),
        (
            "parameter-string",
            MAX_PARAMETER_STRING_BYTES,
            ParameterValue::string("v".repeat(MAX_PARAMETER_STRING_BYTES)).is_ok(),
            ParameterValue::string("v".repeat(MAX_PARAMETER_STRING_BYTES + 1)).is_ok(),
        ),
        (
            "schema-id",
            MAX_SCHEMA_ID_BYTES,
            StateSchemaId::new("s".repeat(MAX_SCHEMA_ID_BYTES)).is_ok(),
            StateSchemaId::new("s".repeat(MAX_SCHEMA_ID_BYTES + 1)).is_ok(),
        ),
        (
            "domain-name",
            MAX_DOMAIN_NAME_BYTES,
            JobName::new("j".repeat(MAX_DOMAIN_NAME_BYTES)).is_ok(),
            JobName::new("j".repeat(MAX_DOMAIN_NAME_BYTES + 1)).is_ok(),
        ),
        (
            "definition-token",
            MAX_TOKEN_BYTES,
            DefinitionRevision::new("t".repeat(MAX_TOKEN_BYTES)).is_ok(),
            DefinitionRevision::new("t".repeat(MAX_TOKEN_BYTES + 1)).is_ok(),
        ),
    ];
    subjects.extend(recovery_text_subjects(
        version,
        digest,
        MAX_RECOVERY_REASON_BYTES,
        MAX_OPERATOR_REFERENCE_BYTES,
    ));
    subjects
}

/// The two subjects `RecoveryRequest::abandon` validates: its reason code and
/// its operator reference. Each is tested at its own boundary while the other
/// field is held at a valid, unrelated value, since the constructor checks
/// the reason code first and a boundary value there would otherwise mask
/// whatever the operator-reference boundary was meant to prove.
fn recovery_text_subjects(
    version: ExecutionVersion,
    digest: [u8; 32],
    reason_ceiling: usize,
    reference_ceiling: usize,
) -> Vec<(&'static str, usize, bool, bool)> {
    vec![
        (
            "recovery-reason",
            reason_ceiling,
            RecoveryRequest::abandon(version, "r".repeat(reason_ceiling), "operator", digest)
                .is_ok(),
            RecoveryRequest::abandon(version, "r".repeat(reason_ceiling + 1), "operator", digest)
                .is_ok(),
        ),
        (
            "operator-reference",
            reference_ceiling,
            RecoveryRequest::abandon(version, "reason", "o".repeat(reference_ceiling), digest)
                .is_ok(),
            RecoveryRequest::abandon(version, "reason", "o".repeat(reference_ceiling + 1), digest)
                .is_ok(),
        ),
    ]
}

/// Writes the boundary-sized durable payloads and reads them back.
async fn round_trip_the_boundary(url: &str) -> Result<RoundTrip, Box<dyn Error>> {
    let key_text = "k".repeat(MAX_PARTITION_KEY_BYTES);
    let clock = FixedClock::default();
    let repository =
        PostgresJobRepository::connect(config(url.to_owned())?, Arc::new(clock)).await?;

    let name = JobName::new(BOUNDARY_JOB)?;
    let manager = NodeId::new("partitioned")?;
    let worker_name = StepName::new("worker")?;
    let plan = FlowGraph::new(manager.clone())
        .with_node(FlowNode::partitioned_step(PartitionedStepNode::new(
            manager.clone(),
            StepName::new("partitioned")?,
            StepNode::new(
                NodeId::new("worker")?,
                worker_name.clone(),
                StepComponents::Tasklet(ComponentRevision::new("worker-v1")?),
            ),
            ComponentRevision::new("partitioner-v1")?,
            ComponentRevision::new("canonical-v1")?,
            PartitionCount::new(1)?,
            PartitionBudget::new(1, 2)?,
        )))
        .with_sequence(
            manager.clone(),
            FlowTarget::Terminal(TerminalKind::Complete),
        )?
        .compile(&name, DefinitionRevision::new("v1")?)?;

    // The context is filled to the ceiling exactly, so what is written is the
    // largest durable partition state the framework accepts.
    let context = context_of(MAX_PARTITION_CONTEXT_BYTES)?;
    let offered_context = context.encoded_len();
    let entry = PartitionPlanEntry::new(PartitionKey::new(key_text.clone())?, context.clone())?;
    let partitioner = PartitionPlanFactory::new(move |_| Ok(vec![entry.clone()]));
    let factory_name = worker_name.clone();
    let factory = PartitionTaskletFactory::new(worker_name, move |_input| {
        TaskletStep::new(factory_name.clone(), Arc::new(CompleteTasklet))
    });
    let job = FlowJob::new(name, plan)?.with_partitioned_tasklet(manager, partitioner, factory)?;

    let ids = SequentialIdGenerator::new(NonZeroU64::MIN);
    let (_, stop) = StopSource::new();
    let launched = FlowLauncher::new(&repository, &clock, &ids)
        .launch(&job, &JobParameters::new(), &stop)
        .await?;

    let parent = launched
        .step_executions()
        .last()
        .ok_or_else(|| Failure::boxed("the boundary step produced no parent execution"))?;
    let mut unit = repository.begin().await?;
    let partitions = unit.step_partition_plan(parent.id()).await?;
    unit.rollback().await?;
    repository.close().await?;

    let mut violations = Vec::new();
    let stored = partitions
        .first()
        .ok_or_else(|| Failure::boxed("the boundary step recorded no partition"))?;

    // Acceptance is not the claim. What comes back has to be byte-identical:
    // a key the framework accepts and the column silently shortens is a bound
    // that held in the wrong place, and it would surface as a restart matching
    // the wrong partition rather than as an error.
    if stored.key().as_str() != key_text {
        violations.push(format!(
            "a partition key written at its {MAX_PARTITION_KEY_BYTES}-byte ceiling came back as \
             {} bytes",
            stored.key().as_str().len(),
        ));
    }
    if stored.context() != &context {
        violations.push(format!(
            "a partition context written at its {MAX_PARTITION_CONTEXT_BYTES}-byte ceiling came \
             back as {} encoded bytes",
            stored.context().encoded_len(),
        ));
    }

    Ok(RoundTrip {
        key_bytes: key_text.len() as u64,
        context_bytes: offered_context as u64,
        returned_key_bytes: stored.key().as_str().len() as u64,
        returned_context_bytes: stored.context().encoded_len() as u64,
        violations,
    })
}

/// Offers the instance key more raw input than the digest guard accepts.
async fn instance_key_bound(url: &str) -> Result<InstanceKey, Box<dyn Error>> {
    // The guard is on the serialized identifying-parameter document rather than
    // on any one value, so the input is built out of parameters that are each
    // inside their own declared bound.
    let value = "v".repeat(64 * 1024);
    let clock = FixedClock::default();
    let repository =
        PostgresJobRepository::connect(config(url.to_owned())?, Arc::new(clock)).await?;
    let name = JobName::new(REFUSED_JOB)?;

    let mut offered = 0_usize;
    let mut parameters = JobParameters::new();
    for slot in 0..20 {
        parameters.insert(
            ParameterName::new(format!("oversize-{slot:02}"))?,
            JobParameter::new(
                ParameterValue::string(value.clone())?,
                ParameterRole::Identifying,
            ),
        )?;
        offered += value.len();
    }

    let key = JobInstanceKey::new(name.clone(), &parameters);
    let mut unit = repository.begin().await?;
    let refused = unit.select_or_create_job_instance(&key).await.is_err();
    unit.rollback().await?;
    repository.close().await?;

    let mut violations = Vec::new();
    if !refused {
        violations.push(format!(
            "an instance key digested from {offered} bytes of identifying parameters was accepted \
             against a {INSTANCE_KEY_CEILING}-byte input ceiling",
        ));
    }

    Ok(InstanceKey {
        offered: offered as u64,
        ceiling: INSTANCE_KEY_CEILING as u64,
        refused,
        violations,
    })
}

/// Reports whether a payload survives a chain of `edges` declared upgrades.
fn walks_chain(edges: usize) -> bool {
    fn walk(edges: usize) -> Result<(), Box<dyn Error>> {
        let schema = StateSchemaId::new("m5.resource-bounds.chain")?;
        let mut upgrades = Vec::with_capacity(edges);
        for edge in 0..edges {
            let from = StateSchemaVersion::new(u32::try_from(edge)? + 1)?;
            let to = StateSchemaVersion::new(u32::try_from(edge)? + 2)?;
            upgrades.push(StateSchemaUpgrade::new(from, to, |payload| {
                Ok(payload.to_vec())
            })?);
        }
        let codec = ChainCodec {
            schema: schema.clone(),
            current: StateSchemaVersion::new(u32::try_from(edges)? + 1)?,
            upgrades,
        };

        // The payload is recorded at the oldest version the chain starts from,
        // so decoding it is what walks every edge.
        let recorded = ExecutionContext::from_json(
            b"{\"format\":\"oxide-batch.execution-context\",\"format_version\":1,\
              \"schema\":\"m5.resource-bounds.chain\",\"schema_version\":1,\
              \"payload\":{}}",
            StateLimits::new(DURABLE_STATE_CEILING, 16)?,
        )?;
        recorded.decode(&codec)?;
        Ok(())
    }

    walk(edges).is_ok()
}

/// Reports whether `count` read listeners may be registered on one step.
fn registers_listeners(count: usize) -> bool {
    let mut listeners = ItemListenerSet::<(), ()>::new();
    for _ in 0..count {
        listeners = match listeners.with_read_listener(Arc::new(SilentListener)) {
            Ok(next) => next,
            Err(_) => return false,
        };
    }
    true
}

/// A codec whose only interesting property is the length of its chain.
struct ChainCodec {
    schema: StateSchemaId,
    current: StateSchemaVersion,
    upgrades: Vec<StateSchemaUpgrade>,
}

impl VersionedStateCodec<()> for ChainCodec {
    fn schema_id(&self) -> &StateSchemaId {
        &self.schema
    }

    fn current_version(&self) -> StateSchemaVersion {
        self.current
    }

    fn upgrades(&self) -> &[StateSchemaUpgrade] {
        &self.upgrades
    }

    fn encode(&self, (): &()) -> Result<Vec<u8>, StateCodecError> {
        Ok(b"{}".to_vec())
    }

    fn decode(&self, _payload: &[u8]) -> Result<(), StateCodecError> {
        Ok(())
    }
}

/// A listener that observes nothing, so only its registration is measured.
struct SilentListener;

impl ReadListener<()> for SilentListener {}

/// Builds one durable partition entry whose context envelope is `bytes` long.
fn plan_entry(bytes: usize) -> Result<PartitionPlanEntry, Box<dyn Error>> {
    Ok(PartitionPlanEntry::new(
        PartitionKey::new("boundary")?,
        context_of(bytes)?,
    )?)
}

/// Builds one execution context whose encoded envelope is `bytes` long.
fn context_of(bytes: usize) -> Result<ExecutionContext, Box<dyn Error>> {
    const ENVELOPE: &str = "{\"format\":\"oxide-batch.execution-context\",\"format_version\":1,\
                            \"schema\":\"m5.resource-bounds\",\"schema_version\":1,\
                            \"payload\":{\"filler\":\"\"}}";
    let filler = bytes.saturating_sub(ENVELOPE.len());
    let document = format!(
        "{{\"format\":\"oxide-batch.execution-context\",\"format_version\":1,\
         \"schema\":\"m5.resource-bounds\",\"schema_version\":1,\
         \"payload\":{{\"filler\":\"{}\"}}}}",
        "f".repeat(filler),
    );
    debug_assert_eq!(document.len(), bytes.max(ENVELOPE.len()));
    Ok(ExecutionContext::from_json(
        document.as_bytes(),
        StateLimits::new(DURABLE_STATE_CEILING, 16)?,
    )?)
}

/// Builds one fault-state envelope holding `entries` unresolved keys.
fn envelope(entries: usize) -> Result<FaultStateEnvelope, Box<dyn Error>> {
    let revision = ClassifierRevision::new("m5_resource_bounds_v1")?;
    let mut retained = Vec::with_capacity(entries);
    for index in 0..entries {
        let mut digest = [0_u8; 32];
        digest[0] = u8::try_from(index % 256)?;
        digest[1] = u8::try_from(index / 256)?;
        retained.push(FaultStateEntry::new(
            RetryKey::from_bytes(digest),
            FaultPhase::Write,
            FailureCategory::Timeout,
            RetryOrdinal::new(1)?,
            revision.clone(),
        ));
    }
    Ok(FaultStateEnvelope::new([1; 32], retained)?)
}

/// Reports whether one node may carry `outgoing` transitions.
fn fans_out(outgoing: usize) -> bool {
    fn build(outgoing: usize) -> Result<(), Box<dyn Error>> {
        let name = JobName::new("m5-resource-bound-fanout")?;
        let entry = NodeId::new("entry")?;
        let mut graph = FlowGraph::new(entry.clone()).with_node(FlowNode::step(StepNode::new(
            entry.clone(),
            StepName::new("entry")?,
            StepComponents::Tasklet(ComponentRevision::new("fanout-v1")?),
        )));
        for index in 0..outgoing {
            graph = graph.with_transition(oxide_batch::FlowTransition::new(
                entry.clone(),
                ExitPattern::new(format!("C{index:04}"))?,
                FlowTarget::Terminal(TerminalKind::Complete),
            ));
        }
        graph.compile(&name, DefinitionRevision::new("v1")?)?;
        Ok(())
    }

    build(outgoing).is_ok()
}

/// Builds a linear graph of `nodes` steps.
fn chain_of(nodes: usize) -> Result<oxide_batch::CompiledExecutionPlan, Box<dyn Error>> {
    let name = JobName::new("m5-resource-bound-chain")?;
    let entry = NodeId::new("n000000")?;
    let mut graph = FlowGraph::new(entry);
    for index in 0..nodes {
        graph = graph.with_node(FlowNode::step(StepNode::new(
            NodeId::new(format!("n{index:06}"))?,
            StepName::new(format!("n{index:06}"))?,
            StepComponents::Tasklet(ComponentRevision::new("chain-v1")?),
        )));
    }
    for index in 0..nodes {
        let target = if index + 1 == nodes {
            FlowTarget::Terminal(TerminalKind::Complete)
        } else {
            FlowTarget::Node(NodeId::new(format!("n{:06}", index + 1))?)
        };
        graph = graph.with_sequence(NodeId::new(format!("n{index:06}"))?, target)?;
    }
    Ok(graph.compile(&name, DefinitionRevision::new("v1")?)?)
}

/// Returns the longest chain whose manifest stays inside the manifest ceiling.
///
/// The node ceiling and the manifest ceiling bound the same object from two
/// directions, and which one binds first is a property of the encoding rather
/// than a decision. The report finds it rather than assuming it.
fn largest_manifest_chain() -> usize {
    let mut best = 1;
    let mut low = 1;
    let mut high = MAX_NODES;
    while low <= high {
        let middle = low + (high - low) / 2;
        match manifest_bytes(middle) {
            Ok(bytes) if bytes <= MANIFEST_CEILING => {
                best = middle;
                low = middle + 1;
            }
            _ => {
                if middle == 0 {
                    break;
                }
                high = middle - 1;
            }
        }
    }
    best
}

/// Returns the canonical manifest size of a chain of `nodes` steps.
fn manifest_bytes(nodes: usize) -> Result<usize, Box<dyn Error>> {
    Ok(chain_of(nodes)?
        .definition_identity()
        .canonical_manifest()
        .len())
}

/// A worker whose only job is to let the boundary payload be written.
struct CompleteTasklet;

impl Tasklet for CompleteTasklet {
    fn execute<'a>(
        &'a self,
        _context: TaskletContext<'a>,
    ) -> BoxFuture<'a, Result<TaskletOutcome, TaskletError>> {
        Box::pin(async { Ok(TaskletOutcome::Completed) })
    }
}

/// What writing the boundary-sized durable payloads observed.
struct RoundTrip {
    key_bytes: u64,
    context_bytes: u64,
    returned_key_bytes: u64,
    returned_context_bytes: u64,
    violations: Vec<String>,
}

impl RoundTrip {
    /// Renders what the retained evidence records for the round trip.
    fn evidence(&self) -> Value {
        json!({
            "partition_key_bytes_written": self.key_bytes,
            "partition_key_bytes_returned": self.returned_key_bytes,
            "partition_context_bytes_written": self.context_bytes,
            "partition_context_bytes_returned": self.returned_context_bytes,
            "identical": self.violations.is_empty(),
            "violations": self.violations,
        })
    }
}

/// What offering the instance key an oversized input observed.
struct InstanceKey {
    offered: u64,
    ceiling: u64,
    refused: bool,
    violations: Vec<String>,
}

impl InstanceKey {
    /// Renders what the retained evidence records for the instance key.
    fn evidence(&self) -> Value {
        json!({
            "resource": "instance-key-input",
            "overload_policy": "fail-closed",
            "configured_ceiling": self.ceiling,
            "offered_load": self.offered,
            "refused": self.refused,
            "violations": self.violations,
            "passed": self.violations.is_empty(),
        })
    }
}

/// One payload the framework must accept or refuse.
struct Cell {
    resource: &'static str,
    subject: Option<&'static str>,
    case: &'static str,
    ceiling: u64,
    value: u64,
    accepted: bool,
    expected: bool,
}

impl Cell {
    /// Records one construction result for a resource with one subject.
    const fn new(
        resource: &'static str,
        ceiling: u64,
        case: &'static str,
        value: u64,
        accepted: bool,
        expected: bool,
    ) -> Self {
        Self {
            resource,
            subject: None,
            case,
            ceiling,
            value,
            accepted,
            expected,
        }
    }

    /// Records one construction result for one member of a swept table.
    ///
    /// `ceiling` and `value` are the subject's own real numbers, not a
    /// placeholder: a subject proved only by a boolean accept/refuse pair,
    /// with no numeric ceiling recorded, is not a non-vacuous boundary proof —
    /// nothing distinguishes it from a bound enforced anywhere else.
    #[allow(clippy::too_many_arguments)]
    const fn named(
        resource: &'static str,
        subject: &'static str,
        case: &'static str,
        ceiling: u64,
        value: u64,
        accepted: bool,
        expected: bool,
    ) -> Self {
        Self {
            resource,
            subject: Some(subject),
            case,
            ceiling,
            value,
            accepted,
            expected,
        }
    }

    /// Returns the violation this cell is, when it is one.
    fn violation(&self) -> Option<String> {
        (self.accepted != self.expected).then(|| {
            let subject = self.subject.unwrap_or(self.resource);
            if self.expected {
                format!(
                    "{subject} refused {}, which is inside its declared bound",
                    self.case,
                )
            } else {
                format!(
                    "{subject} accepted {}, which is outside its declared bound",
                    self.case,
                )
            }
        })
    }

    /// Renders what the retained evidence records for this cell.
    fn evidence(&self) -> Value {
        json!({
            "resource": self.resource,
            "subject": self.subject,
            "case": self.case,
            "declared_ceiling": self.ceiling,
            "value": self.value,
            "expected": if self.expected { "accepted" } else { "refused" },
            "observed": if self.accepted { "accepted" } else { "refused" },
        })
    }
}
