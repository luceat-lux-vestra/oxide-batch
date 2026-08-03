# Execution-Plan Architecture

**State:** Accepted

**Governing decisions:** [RFC-0004](../rfcs/0004-compiled-execution-plan.md)
and [ADR-0005](decisions/0005-compiled-execution-plan.md)

This document is the canonical target specification for job definitions,
compiled plans, definition identity, and plan evolution.
[ADR-0004](decisions/0004-job-definition-restart-compatibility.md) remains the
binding current restart contract during staged lowering.

## Model

`JobDefinition` is an immutable declaration containing:

- a stable `DefinitionId`, application-owned `DefinitionRevision`, and
  framework-computed `DefinitionFingerprint`;
- a versioned parameter schema;
- a directed `FlowGraph`;
- component logical IDs, capability declarations, and durable schema versions;
- default restart, transaction, delivery, and resource policies.

`StepDefinition` has a stable logical ID and one kind: tasklet, chunk,
partition, remote, nested job, or registered custom extension. Flow nodes are
step, decision, split, nested-job, end, fail, or stop nodes. Transition edges
match typed or bounded exit outcomes and have stable ordering.

Logical IDs survive harmless display-name and storage-key changes. Adapter
primary keys and runtime execution IDs are not definition IDs.

## Compilation

A builder produces a `JobDefinition`; it does not directly execute. Plan
compilation validates and normalizes that definition into an immutable
`CompiledExecutionPlan`.

Compilation MUST reject:

- missing entry or terminal nodes, unreachable nodes, duplicate logical IDs,
  undefined transitions, and invalid or non-terminating cycles;
- reader/processor/writer type incompatibility;
- non-`Send` components assigned to a cross-thread boundary, or shared
  components lacking the declared thread-safety capability;
- restartable steps whose stateful components cannot checkpoint;
- transaction or delivery requirements unsupported by their adapters;
- remote nodes whose definition, component, state, or artifact cannot cross
  the selected protocol boundary;
- resource budgets that are zero, contradictory, or unbounded.

Compilation emits stable, bounded diagnostics without parameters, contexts,
credentials, item values, or component-private data.

## Manifest, fingerprint, and registry

The neutral definition manifest is a bounded, canonical, versioned
representation of restart- and deployment-relevant structure. It records node
and component IDs, graph edges, parameter schema, checkpoint/context codec
versions, capabilities, delivery mode, and the resource policies that change
durable meaning. It does not contain executable Rust code, secrets, endpoints,
user data, framework capacity bounds, or throughput-only budgets.

Canonical bytes are hashed to produce the fingerprint. Exact encoding remains
the one accepted by ADR-0004 until a superseding ADR changes it. Equivalent
source builders MUST produce identical canonical bytes; a changed
restart-relevant value MUST change the fingerprint.

`DefinitionRegistry` resolves `(definition_id, revision)` to one immutable
manifest and executable assembly. Binding the same pair to a different
fingerprint is definition drift.

## Static and erased components

The manifest must describe logical components without serializing Rust values
and remain compatible with either the current boxed components or the contract
accepted by [RFC-0005](../rfcs/0005-static-and-erased-components.md) and
recorded as
[ADR-0008](decisions/0008-item-component-contract.md). Representation is not
restart-relevant: moving a component between the two forms does not change its
manifest entry or the definition fingerprint. The contract lands in M6; M5
manifests continue to describe boxed components.

Erasure does not remove validation. The registry MUST prove that the resolved
component matches the manifest before launch or assignment.

## Compatibility and evolution

The restart modes are:

- `Strict`: the checkpoint-producing and proposed fingerprints match;
- `Compatible`: one explicit directed upgrade maps every durable source node
  and state schema to the target;
- `Fork`: start a new instance lineage linked to the source execution or
  savepoint without claiming to resume it;

`Force` is not an accepted restart mode. Adding an exceptional audited override
requires a separate decision and cannot fabricate stronger guarantees.

Upgrade edges are bounded, deterministic, one-way, and non-transitive. They
cannot erase counters, change the selected instance, reinterpret a committed
checkpoint, or strengthen the recorded guarantee of past effects.

Plan manifest and fingerprint versions have explicit readers and migration
rules. A runtime rejects newer versions it cannot interpret.

## Current-to-target migration

Current `TaskletJob` and `TaskletStep` APIs remain compatibility wrappers. The
staged path is:

1. produce an equivalent one-step definition and plan;
2. compare lifecycle traces and repository writes;
3. route execution through the compiled plan without changing the facade;
4. add general flow node types;
5. deprecate wrappers only through the public compatibility policy.

## M3 bounded lowering and flow slice

M3 implements the acyclic step/decision/terminal subset, deterministic exit
patterns, durable decisions, and start controls specified by the
[basic-flow contract](basic-flow.md). Its fixed graph and manifest bounds are
implementation requirements, not defaults that a caller may disable.

Manifest format 2 is canonical JSON and captures the graph, fault policies,
authoritative listener/decider revisions, and start controls. Format-1
one-step wrappers remain readable and lower without changing their original
bytes, fingerprint, normalized repository writes, or callback trace. Moving a
persisted definition from format 1 to 2 requires a direct compatibility edge;
schema migration never rewrites manifest identity.

## M4 bounded local-scale slice

M4 adds exactly one split node kind and one partitioned step node kind to the
M3 acyclic subset, with the branch, partition, budget, ownership, aggregation,
and thread-safety rules fixed by the
[M4 bounded local-scale contract](local-scale.md). Compilation rejects nested
splits, partitioned steps inside a branch, decision nodes inside a branch, and
any zero, contradictory, or unbounded budget.

Manifest format 3 adds those node kinds and the partition count, partitioner,
and aggregation identity that select durable assignment or change aggregate
meaning, which therefore participate in the fingerprint. Concurrency and
connection budgets do not: they bound throughput, and the same contract's
sequential fallback equivalence requires them to leave every normalized durable
observation unchanged. The delivered M4 projection hashed them anyway;
[ADR-0009](decisions/0009-definition-fingerprint-input-set.md) removed them and
re-pinned the format-3 vector once. Formats 1 and 2 remain readable and their
bytes are never rewritten; moving a persisted definition to format 3 requires
one direct compatibility edge. M4 keeps the accepted ADR-0002 boxed component
boundary and does not implement the RFC-0005 static hot path.

## M5 stabilization slice

M5 stabilizes the delivered manifest and fingerprint path. It adds no node
kind, no manifest format, and no new restart mode.

**Canonical restart-relevant manifest.** The manifest records exactly the
values that select or reinterpret durable state: node and component logical
IDs, graph edges and their stable ordering, the parameter schema, checkpoint
and context codec versions, declared capabilities that change durable meaning,
delivery mode, start controls, fault-policy identity, and the partition count,
partitioner, and aggregation identity that select assignment or change
aggregate meaning. Display names, storage keys, adapter primary keys, runtime
execution IDs, telemetry attributes, framework capacity bounds, resource
budgets that change only throughput, and diagnostic text are excluded and MUST
NOT change a fingerprint.
[ADR-0009](decisions/0009-definition-fingerprint-input-set.md) is the normative
input set, and the projection carries an executable member allowlist so a new
member cannot enter it without a superseding decision.

**Fingerprint stability.** Canonical bytes are deterministic across processes,
architectures, framework releases, and repeated compilation of an unchanged
definition. Equivalent source builders produce identical bytes; any change to a
recorded restart-relevant value changes the fingerprint. Formats 1, 2, and 3
keep their format identifiers, canonical encoding rules, and readers. ADR-0009
re-pinned the format-2 and format-3 vectors exactly once when it removed the
excluded values from the projection; format 1 was unaffected. After that
re-pin, M5 adds vectors rather than replacing them, and a later change to the
input set requires a superseding ADR.

**Compatibility edges.** A persisted definition moves between manifest formats
only through one direct, bounded, deterministic, one-way edge, as already
accepted for format 1 to 2 and format 2 to 3. Edges are non-transitive and
never rewrite the bytes of a stored earlier-format manifest.

**Fail-closed drift detection.** Restart resolves `(definition_id, revision)`
to its recorded manifest and compares the proposed fingerprint before any
lifecycle write. A mismatch without an accepted compatibility edge rejects the
restart with a typed error and changes no durable state. Binding the same
`(definition_id, revision)` to a different fingerprint is definition drift and
is rejected rather than reconciled. A runtime rejects a manifest format newer
than it can read.

**Delivered subset boundary.** M5 stabilizes fingerprinting, drift detection,
and restart identity over the delivered M2-M4 subset. General compiled-plan
restart, the M7 `DefinitionRegistry` as a public service, schema-transforming
upgrade edges, and `Fork` lineage remain M7. M5 keeps the accepted ADR-0002
boxed component boundary and does not implement the RFC-0005 static hot path.

## Evidence

Production implementation requires:

- canonicalization and fingerprint golden vectors plus fuzzing;
- invalid-graph property and validation tests;
- current-wrapper versus plan trace-equivalence tests;
- compile-fail tests for typed component incompatibility;
- strict, compatible, fork, force-rejection, and definition-drift restart
  matrices;
- migration rollback and corrupted/newer-manifest tests;
- static and erased lowering evidence;
- local/distributed manifest and trace-equivalence tests before M11.
