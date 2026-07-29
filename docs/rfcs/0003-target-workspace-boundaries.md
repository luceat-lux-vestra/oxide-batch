# RFC-0003: Target Workspace Boundaries and Staged Extraction

- **State:** Accepted
- **Created:** 2026-07-30
- **Owner:** architecture maintainers
- **Target milestone:** M5-M8
- **Related ADR:** [ADR-0001](../architecture/decisions/0001-workspace-and-facade.md)

## Summary

Retain one workspace and curated facade while incrementally extracting core,
plan, engine, item, repository, adapter, observability, test, protocol, and CLI
boundaries when their dependency and support obligations become real.

## Context and current accepted rule

ADR-0001 approves one workspace, the `oxide-batch` facade, private internal
crates by default, and no placeholder publication. Most current code remains in
one implementation crate, which was appropriate for M1/M2.

## Problem

Continued growth in one crate/runtime unit risks dependency inversion,
infrastructure leakage, slow builds, untestable boundaries, and inability to
optimize the item hot path independently. Creating every forecast crate now
would be equally premature.

## Proposal

- Keep ADR-0001's monorepo, facade, and publish-by-obligation rules.
- Define target dependency boundaries shown in the architecture overview and
  strategy.
- Extract in stages: core values/state; repository ports/contracts; plan;
  engine; item; adapters/observability/test; distributed protocol and CLI as
  demanded.
- Preserve facade paths through re-exports and behavior/API tests.
- Keep implementation crates `publish = false` until a public integration
  boundary is separately approved.
- Prohibit cyclic dependencies and core dependencies on Tokio, SQLx, Clap,
  OpenTelemetry SDKs, brokers, or web frameworks.

## Alternatives

1. Keep a monolithic crate indefinitely. Rejected for long-term dependency and
   performance isolation.
2. Create and publish the full forecast immediately. Rejected as empty support
   commitments.
3. Split repositories. Rejected until release cadence/ownership makes atomic
   changes materially harmful.

## Consequences

More crates and explicit adapters increase workspace/release complexity.
Private boundaries can evolve; the facade remains the user contract. Extraction
must not become a rewrite.

## Compatibility impact

Facade behavior and supported imports remain stable within the applicable
pre-1.0/1.0 policy. Workspace internal paths are not compatibility promises.
Crate publication requires the existing publishing policy.

## Metadata, restart, and transaction impact

Extraction cannot change persisted bytes, transaction boundaries, lifecycle
writes, or restart selection. Repository/engine separation must preserve the
borrowed adapter-owned transaction path and ADR-0004 definition identity.

## Migration and rollout

For each extraction: freeze facade behavior with compile/API tests, generate a
module dependency graph, move the smallest coherent boundary, retain temporary
facade adapters, compare traces and repository writes, then remove obsolete
internal paths. Reversal restores the previous internal module without changing
the facade or metadata.

## Validation and evidence plan

- dependency graph and forbidden-dependency checks;
- all current unit/property/contract/conformance/crash tests;
- facade compile and rustdoc/API comparisons;
- package content/dry-run checks;
- build-time and binary-size measurements for material splits;
- no schema, fingerprint, or normalized trace changes.

## Unresolved questions

- Whether the Tokio engine needs its own crate or an internal module initially.
- When `oxide-batch-test` and adapter crates become public.
- Exact extraction order after M2 runtime hotspots are measured.

## Decision

**Accepted by the project owner on 2026-07-30.**

The target direction and staged, behavior-preserving extraction rules are
accepted. ADR-0001 remains valid because the monorepo, curated facade,
private-by-default crates, and no-placeholder-publication rules are unchanged.
Each extraction still requires its dependency, facade-equivalence, and
packaging evidence; public-crate approval is separate.
