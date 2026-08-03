# ADR-0009: Definition Fingerprint Input Set

- **State:** Accepted
- **Date:** 2026-08-03
- **Owners:** runtime and API maintainers
- **Deciders:** project owner
- **Governing RFC:** [RFC-0004](../../rfcs/0004-compiled-execution-plan.md)
- **Extends:** [ADR-0004](0004-job-definition-restart-compatibility.md) and
  [ADR-0005](0005-compiled-execution-plan.md)

## Context

M5 stabilizes the compiled plan and definition fingerprint. The
[M5 stabilization slice](../execution-plan.md#m5-stabilization-slice) fixes the
canonical manifest as "exactly the values that select or reinterpret durable
state" and names an exclusion list that resource budgets which change only
throughput MUST NOT change a fingerprint. The
[design-gate record](../../project/m5-design-gate-evidence.md) requires the
scenario `throughput_only_budget_change_does_not_change_the_fingerprint` as
evidence for issue #98.

The delivered manifest projection does not satisfy that rule. Two classes of
value enter the canonical bytes without selecting or reinterpreting any durable
state.

**Framework capability bounds.** Formats 2 and 3 record a `bounds` object of
framework compile-time constants — `max_nodes`, `max_transitions`,
`max_outgoing_transitions`, `max_pattern_bytes`, and, for format 3,
`max_split_branches`, `max_branch_steps`, `max_partitions`, and
`max_partition_workers`. No reader consumes the member. `DefinitionManifest`
validates a graph against the constants the running build carries, not against
the recorded values. Raising any constant in a later release therefore changes
every fingerprint the framework has ever produced, turns every persisted
definition into definition drift, and fails every restart closed until each
application registers a compatibility edge that upgrades nothing.

**Throughput-only budgets.** Format 3 records `SplitBudget` and
`PartitionBudget` in full: `repository_pool_size`, `max_parallel_branches`, and
`max_partition_workers`. None of the three selects durable state. The
[local-scale contract](../local-scale.md) gives the partitioner the plan
fingerprint, the job instance identity, the logical step ID, and the configured
partition count — not the worker count — and aggregates partition results
in `partition_key` byte order and branch results in declared branch order. The
same contract's sequential fallback equivalence asserts that
`MaxParallelBranches = 1` and `MaxPartitionWorkers = 1` produce identical
normalized durable rows, counters, checkpoints, and callbacks; any divergence
is a defect in the concurrent path rather than an accepted difference. A pool
size is a connection budget re-validated at launch against
`InsufficientPoolCapacity`. Under the accepted contracts these are throughput
values, so an operator who retunes a pool or a worker count after a crash is
currently blocked from restarting by fail-closed drift.

The M5 slice also states that formats 1, 2, and 3 keep their existing golden
vectors and that M5 adds vectors rather than replacing them. That statement and
the exclusion list cannot both hold against the delivered projection, so the
conflict requires a decision rather than an implementation choice. No released
version has emitted these bytes: the workspace is `0.1.0-alpha.1`, every
changelog entry is `Unreleased`, and no crate has been published.

## Decision

The definition fingerprint input set is exactly the values that select or
reinterpret durable state. The canonical manifest projects that set and nothing
else.

**Included.** Manifest format, job name, entry node, node and component logical
IDs, graph edges with their stable ordering, terminal kinds, step names, step
kind and component revisions, chunk size, checkpoint and context schema
identity and version, delivery mode, in-flight policy, transaction boundary,
start controls, fault-policy identity, authoritative listener revisions,
decider revision and durable input version, split branch membership and order,
join ownership, local failure policy, partition count, and partitioner and
aggregation identity.

**Excluded.** Framework capability bounds, resource budgets that change only
throughput, display names, storage keys, adapter primary keys, runtime
execution identifiers, telemetry attributes, and diagnostic text. An excluded
value MUST NOT appear in the canonical manifest, so it cannot change a
fingerprint.

Three consequences of that rule are normative:

1. The `bounds` member leaves manifest formats 2 and 3. Graph bounds remain a
   read-time capability check: a runtime rejects a manifest whose node or
   transition count exceeds what that build accepts, and rejects a manifest
   format newer than it can read. The check belongs to the runtime, not to
   definition identity.
2. `repository_pool_size`, `max_parallel_branches`, and
   `max_partition_workers` leave manifest format 3. `SplitBudget` and
   `PartitionBudget` remain public API, remain validated at compilation, and
   remain re-validated at launch; a budget is still never silently reduced.
   `partition_count`, `failure_policy`, branch membership and order, and
   partitioner and aggregation identity stay in the fingerprint because they
   select assignment or change aggregate meaning.
3. Manifest format numbers do not change. Formats 2 and 3 keep their
   identifiers, their canonical JSON encoding rules, their bounds on size and
   depth, and their readers. Only the projected member set narrows. Format 1
   carries neither excluded class and is unchanged, byte for byte.

Persisted manifests are still never rewritten. A definition persisted by a
pre-acceptance build compares unequal to the same definition recompiled after
this ADR, which is definition drift and is rejected fail-closed before any
lifecycle write, exactly as accepted. The framework registers no compatibility
edge for the re-projection: no released version produced the old bytes, and a
framework-supplied edge would claim an equivalence that ADR-0004 reserves for
the application. A pre-release deployment that must resume across the change
registers its own directed edge under the existing contract.

Golden vectors for formats 2 and 3 are re-pinned exactly once, in the change
that implements this ADR, and the re-pin is recorded in that change's evidence
with both the old and the new digest. After it, the M5 rule stands without
exception: fingerprint vectors are added, never replaced, and any later change
to the input set requires a superseding ADR.

## Consequences

- An operator may retune a connection pool, a branch concurrency budget, or a
  partition worker count after a crash and still restart, which is the
  operability property the preview needs.
- A framework release may raise a graph bound without invalidating the restart
  identity of every persisted definition.
- The fingerprint stops depending on the framework build and depends only on
  the application's definition, which is what makes
  `unchanged_definition_recompiles_to_the_same_fingerprint` meaningful across
  releases rather than within one build.
- One re-pin of the format-2 and format-3 vectors is spent now; every later
  input-set change costs a superseding ADR.
- Pre-release deployments holding format-2 or format-3 manifests see drift on
  upgrade and must recompile or register an edge. The support matrix's preview
  bounds must say so before the preview ships.
- The M4 expectation that a worker-count change alters the fingerprint is
  withdrawn. Its test is replaced by the inverse invariant.
- Removing an unread member shrinks the manifest, which keeps the 64 KiB bound
  further from the M7 general compiled-plan population.

## Alternatives considered

- **Keep the bytes and reclassify the budgets as assignment identity.** This
  preserves the vectors but contradicts the local-scale contract's own
  partitioning inputs, aggregation order, and sequential fallback equivalence,
  and it would ship a preview in which routine resource tuning blocks recovery.
  It also leaves the framework bounds defect untouched.
- **Introduce manifest format 4 and a format-3-to-4 edge.** This preserves
  every existing vector, but it spends a durable format number and a
  compatibility edge on a projection that no released version has emitted, adds
  a second reader for bytes no deployment holds, and contradicts the M5 rule
  that M5 adds no manifest format.
- **Move `bounds` to an unhashed sidecar.** The member has no reader, so a
  sidecar preserves a value nothing consumes and adds a second durable
  artifact to version, migrate, and redact.
- **Defer the whole question to M7.** M5 is the milestone that stabilizes the
  fingerprint and promotes ledger rows to `Verified`. Promoting an identity
  that changes with a framework constant, and publishing a preview whose
  recovery path breaks under resource tuning, is the outcome the stabilization
  gate exists to prevent.

## Validation

Acceptance is validated by the plan-and-fingerprint scenarios the design gate
names for issue #98, specifically:

- `throughput_only_budget_change_does_not_change_the_fingerprint`, over pool
  size, branch concurrency, and worker count, for both split and partitioned
  nodes;
- `display_name_or_storage_key_change_does_not_change_the_fingerprint`, proving
  that no adapter key, execution identifier, or telemetry attribute reaches the
  canonical bytes;
- `restart_relevant_change_changes_the_fingerprint`, over every included value
  named above, so the narrowing removes nothing that selects durable state;
- `unchanged_definition_recompiles_to_the_same_fingerprint`, including across
  declaration order and repeated compilation;
- a manifest member allowlist assertion, so a later member added to the
  projection fails the suite unless the input set is amended by ADR;
- `format1_and_format2_bytes_are_never_rewritten` and
  `newer_manifest_format_is_rejected`, unchanged in meaning;
- re-pinned format-2 and format-3 golden vectors, recorded with their prior
  digests in the issue #98 evidence record.

Accepting this ADR requires documentation corrections in the
[execution-plan architecture](../execution-plan.md) M4 and M5 slices, the
[local-scale contract](../local-scale.md) manifest and schema impact, the
[M5 design-gate record](../../project/m5-design-gate-evidence.md) fingerprint
row, and the [support matrix](../../release/support-matrix.md) preview bounds.

## Revisit triggers

Revisit if a budget is shown to change a durable row, a counter, a checkpoint,
or an aggregate outcome, which would make it restart-relevant and would also
invalidate the sequential fallback equivalence claim; if the M7 general
compiled plan requires recording a framework bound to interpret a persisted
graph; or if a released version's manifest bytes must change, which requires a
migration edge and evidence rather than a re-pin.
