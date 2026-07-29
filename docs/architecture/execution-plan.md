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
versions, capabilities, delivery mode, and relevant resource policies. It
does not contain executable Rust code, secrets, endpoints, or user data.

Canonical bytes are hashed to produce the fingerprint. Exact encoding remains
the one accepted by ADR-0004 until a superseding ADR changes it. Equivalent
source builders MUST produce identical canonical bytes; a changed
restart-relevant value MUST change the fingerprint.

`DefinitionRegistry` resolves `(definition_id, revision)` to one immutable
manifest and executable assembly. Binding the same pair to a different
fingerprint is definition drift.

## Proposed static and erased components

The manifest must describe logical components without serializing Rust values
and remain compatible with either current boxed components or the dual-path
architecture proposed by [RFC-0005](../rfcs/0005-static-and-erased-components.md).
RFC-0005 must be accepted before statically lowered and erased component APIs
become production contracts.

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
