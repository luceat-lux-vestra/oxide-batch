# RFC-0004: Compiled Execution Plan and Definition Fingerprinting

- **State:** Accepted
- **Created:** 2026-07-30
- **Owner:** runtime and API maintainers
- **Target milestone:** M5-M7
- **Related ADR:** [ADR-0004](../architecture/decisions/0004-job-definition-restart-compatibility.md)

## Summary

Generalize current tasklet definitions into immutable job/step graphs compiled
into a validated execution plan, extending ADR-0004 fingerprints and directed
upgrades without weakening them.

## Context and current accepted rule

ADR-0004 requires a bounded application revision, canonical definition
manifest, SHA-256 fingerprint, and explicit directed upgrade for restart.
Current execution is centered on a one-step `TaskletJob`/`TaskletStep`.

## Problem

Multi-step flow, splits, nested jobs, capabilities, distributed placement, and
resource validation need one stable graph/manifest. Executing builders directly
would defer structural errors and make definition evolution ambiguous.

## Proposal

- Add immutable `JobDefinition`, `StepDefinition`, typed node/edge IDs,
  parameter schema, and `FlowGraph`.
- Compile to a normalized `CompiledExecutionPlan` before launch.
- Reject graph, type, restart, thread-safety, capability, resource, and remote
  placement errors defined in the canonical execution-plan document.
- Extend the ADR-0004 manifest with normalized graph/capability information.
- Support `Strict`, directed `Compatible`, and lineage-preserving `Fork`.
  `Force` is outside this decision and would require a separate RFC.
- Lower current tasklet APIs through the plan as compatibility wrappers.

## Alternatives

1. Interpret mutable builders at runtime. Rejected because validation and
   fingerprinting become nondeterministic.
2. Serialize executable Rust closures. Not portable or stable.
3. Use job/step names only. Rejected by ADR-0004 because they cannot prove
   restart meaning.

## Consequences

Definitions gain a compile phase and stable logical IDs. Manifests and
registries become broader durable contracts. Invalid plans fail earlier.
Canonicalization and migration add engineering cost.

## Compatibility impact

Existing facade APIs remain wrappers during rollout. A fingerprint may change
when newly captured restart-relevant information is introduced; manifest
version migration and explicit compatibility edges prevent silent rejection or
acceptance.

## Metadata, restart, and transaction impact

Metadata stores manifest/plan versions, fingerprint, node IDs, compatibility
edge, and lineage/fork relation. Upgrade state and new execution creation are
atomic. Plan compilation validates delivery/transaction requirements but does
not replace adapter runtime negotiation.

## Migration and rollout

Implement one-step lowering, golden fingerprints, and trace equivalence first.
Add general nodes incrementally. Provide a manifest-version reader and an
explicit migration/compatibility path for definitions persisted before graph
fields exist. Deprecation of wrappers follows public policy.

## Validation and evidence plan

- canonicalization golden vectors and fuzzing;
- invalid-graph property tests and compile-fail component checks;
- current versus lowered trace/repository equivalence;
- strict/upgrade/fork/force-rejection restart matrix;
- crash during definition/context upgrade;
- local/distributed manifest equivalence before M11;
- plan compilation time and manifest-size bounds.

## Unresolved questions

- Exact neutral manifest encoding after the accepted canonical JSON version.
- Whether a future audited override is justified at all; it requires a separate
  RFC and is not part of this accepted decision.
- Savepoint/fork identity and input-snapshot requirements.

## Decision

**Accepted by the project owner on 2026-07-30.**

The immutable definition and compiled-plan direction is accepted as an
extension of ADR-0004. This acceptance does not authorize production lowering
or a manifest-format change until the one-step equivalence, canonicalization,
and migration evidence in this RFC passes. ADR-0004's fail-closed fingerprint
and directed-upgrade rules remain binding.
