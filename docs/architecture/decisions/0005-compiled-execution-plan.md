# ADR-0005: Compiled Execution Plan

- **State:** Accepted
- **Date:** 2026-07-30
- **Owners:** runtime and API maintainers
- **Deciders:** project owner
- **Governing RFC:** [RFC-0004](../../rfcs/0004-compiled-execution-plan.md)
- **Extends:** [ADR-0004](0004-job-definition-restart-compatibility.md)

## Context

ADR-0004 establishes fail-closed definition revision, canonical manifest,
fingerprint, and directed restart upgrades. A one-step tasklet model cannot
represent the validated flow, capability, placement, and migration information
needed by advanced flow and distributed execution.

## Decision

Adopt immutable `JobDefinition` and `StepDefinition` values, a stable-ID flow
graph, and compilation into a normalized `CompiledExecutionPlan` as the target
execution model.

Compilation validates graph structure, component type/thread-safety,
restart/checkpoint support, transaction and delivery capabilities, resource
bounds, and remote-placement requirements before launch. Current
`TaskletJob`/`TaskletStep` APIs lower through the plan and remain compatibility
wrappers until the public deprecation policy permits otherwise.

The compiled-plan manifest extends ADR-0004; it does not replace or weaken its
canonical fingerprint, definition-drift, directed-upgrade, and atomic migration
rules. `Fork` creates new lineage and never claims restart. A forced override
remains disabled unless separately approved with audit semantics.

## Consequences

- structural and capability errors fail before execution;
- stable logical IDs and manifest versions become durable compatibility data;
- plan compilation and migration add complexity and bounded startup cost;
- current tasklet behavior needs trace/repository equivalence during lowering;
- the plan can become the shared semantic input for local and future
  distributed engines.

## Alternatives considered

- Interpreting mutable builders directly would defer errors and destabilize
  fingerprinting.
- Serializing executable Rust code is not portable or stable.
- Job/step names alone cannot prove restart compatibility.
- Separate local and distributed definitions would allow semantic drift.

## Validation

The decision is accepted, but dependent production implementation remains
gated by:

- one-step lowering with normalized trace and repository-write equivalence;
- canonicalization golden vectors, bounds, and fuzzing;
- invalid-graph and compile-fail component tests;
- strict, directed-upgrade, and fork restart matrices;
- reviewed manifest-version migration.

## Revisit triggers

Revisit if the manifest cannot represent required native components without
leaking implementation data, compilation cost violates an accepted budget, or
local/distributed equivalence requires incompatible definitions.
