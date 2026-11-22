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

[ADR-0017](decisions/0017-canonical-compiled-plan-manifest-v1-reset.md)
defines one canonical definition manifest projected from the normalized
`CompiledExecutionPlan`.

All supported job forms use that same projection. A one-step tasklet or chunk
is a one-node compiled plan; it does not own a separate durable format.

Canonical manifest v1 has the universally required root:

- `manifest_version = 1`;
- `job`;
- `entry`;
- `nodes`; and
- `transitions`.

It adds `restart_policy`, `parameter_incrementer`, `scoped_components`,
and `durable_effects` only when their restart-relevant semantics are
selected.

`manifest_version` is a grammar/parser version only. It never selects runtime
behavior, repository invariants, node kinds, plan capabilities, or builder
origin. Once a document is parsed into the normalized plan/definition model,
execution semantics come only from explicit typed plan state.

The manifest records restart-relevant, non-secret structure and identity. It
does not contain executable Rust values, credentials, endpoints, item/user
data, framework capacity ceilings, telemetry, diagnostics, or
throughput-only budgets.

Canonical bytes are hashed with SHA-256 to produce the definition fingerprint.
[ADR-0009](decisions/0009-definition-fingerprint-input-set.md) remains the
normative membership rule: only values that select or reinterpret durable
restart state enter the fingerprint.

Equivalent normalized plans MUST produce identical canonical bytes regardless
of which public builder API produced them. A changed restart-relevant semantic
MUST change the fingerprint.

`DefinitionRegistry` binds an immutable definition/revision to the canonical
manifest and fingerprint. Rebinding the same identity to another fingerprint is
definition drift.

Historical preview formats 1-4 are not current runtime compatibility targets.
The new reader may identify them only far enough to return an explicit
unsupported-preview-manifest error before lifecycle mutation. It does not
execute, migrate, or reinterpret those documents.

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

Current `TaskletJob`, `ChunkJob`, and wrapper APIs remain supported facade
entry points. Their target lowering path is:

1. construct the equivalent validated compiled-plan node(s);
2. normalize through the same `CompiledExecutionPlan` model used by flows;
3. project one canonical manifest v1 from that plan;
4. derive one definition fingerprint from those canonical bytes; and
5. execute through the same compiled-plan runtime path.

No wrapper retains a private manifest representation merely for historical
compatibility.

The one-node lowering is semantically complete. Standalone tasklet/chunk
behavior that was previously supplied by a format-specific runtime branch is
encoded in the normalized plan itself. A successful custom step exit remains a
successful standalone job through explicit transition/terminal semantics rather
than a one-step version check. The target representation should use ordinary
plan rules, for example exact failure/stop transitions plus a less-specific
completion transition, when that preserves the observable contract.

A standalone wrapper path does not become a durable flow merely because it is
represented as a one-node plan: it need not persist a `FlowDecision` where
the current standalone lifecycle has no such durable decision. This distinction
is explicit execution-path semantics, not manifest-version semantics.

The source cutover intentionally removes the old preview-format restart
boundary. Persisted preview definitions/executions using historical formats are
not migrated into manifest v1 and are not restartable by the new line.
Operational upgrade guidance is published with that source cutover.

## Historical delivery slices

M3, M4, M5, and M7 introduced the flow, local-scale, fingerprint, advanced
flow, scope, repeat, and definition-evolution semantics that the current
compiled plan still owns.

Their historical implementation used manifest formats 2, 3, and 4 (with
one-step format 1). Those format-specific records remain valid evidence for the
release lines they describe, but they are not current parser or compatibility
requirements after ADR-0017.

The semantic payload survives in canonical manifest v1:

- M3 step/decision/transition/start/fault semantics are represented by the
  normalized node and transition projection;
- M4 split/partition/aggregation identity remains restart-relevant where it
  changes durable assignment or aggregate meaning;
- M5's ADR-0009 input-set rule remains binding and excludes throughput-only
  budgets and framework capability ceilings;
- M7 advanced flow, nested structure, repeat, scope/late binding, restart
  policy, parameter incrementer, and durable decision identity remain
  represented by the normalized compiled plan.

Durable flow-decision sequencing is normalized as one explicit plan semantic.
Every transition source that can persist `FlowDecision` owns a deterministic
compiled sequence identity. Runtime and repository code validate that identity
uniformly; they do not select append-count versus compiled-sequence behavior
from `manifest_version` or any historical format number.

Historical format numbers and golden vectors are not copied into the new
manifest and do not select restart compatibility.

## Compatibility and preview reset

For canonical manifest v1:

- exact fingerprint restart remains compatible;
- a different fingerprint is incompatible by default;
- one explicit bounded direct compatibility edge may authorize a compatible
  definition evolution where the owning semantics permit it;
- edges are one-way and non-transitive;
- definition drift fails before lifecycle mutation; and
- a newer/unknown canonical manifest version fails closed.

Historical preview manifests have no compatibility edge into canonical v1.
They fail explicitly and require preview metadata reset/recreation before the
new line is used when old definition/execution rows exist.

Durable-effect definitions are stricter: a different-digest compatible restart
remains unavailable until explicit live-effect continuity/mapping is defined.

## Canonical v1 bounds

The canonical document remains bounded. Node/transition/nesting/identifier
ceilings are reader/compiler capabilities, not fingerprint members. Raising a
framework capability ceiling does not change an unchanged definition's bytes.

Closed-object parsing, canonical ordering, duplicate rejection, and bounded
collection validation apply to every manifest-v1 structure.

Repository adapters do not independently inspect `manifest_version` as a
semantic feature switch. Where repository-side correctness validation needs
definition evidence, it consumes the typed canonical manifest/plan semantics
and explicit node/transition/decision identity instead of re-deriving behavior
from version numbers.

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
