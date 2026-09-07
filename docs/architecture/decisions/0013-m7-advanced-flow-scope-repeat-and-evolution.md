# ADR-0013: M7 Advanced Flow, Scope, Repeat, and Definition Evolution

- **State:** Accepted
- **Date:** 2026-09-07
- **Owners:** runtime, API, repository, and operator maintainers
- **Deciders:** project owner
- **Extends:** [ADR-0004](0004-job-definition-restart-compatibility.md),
  [ADR-0005](0005-compiled-execution-plan.md), and
  [ADR-0009](0009-definition-fingerprint-input-set.md)
- **Canonical contract:**
  [M7 advanced-flow, scope, repeat, and evolution contract](../m7-advanced-flow-scope-repeat-and-evolution.md)

## Context

M7 adds general compiled-flow composition beyond the delivered M3/M4 subset,
real job/step scope and late binding, repeat context/interceptors, a public
definition registry with compatible evolution and fork lineage, and the
operator/explorer surface required to control those definitions.

Those features cross public-API, durable-state, restart-selection, migration,
resource, and redaction boundaries. Their meaning must therefore be fixed
before issues #195-#199 implement production behavior.

At this decision baseline the newest delivered definition-manifest format is
`3`, while the PostgreSQL repository already reports schema version `4` because
M6 component-state durability migrated schema `3 -> 4`. M7 must preserve both
facts rather than reusing an occupied schema number.

## Decision

Adopt the M7 contract linked above as the binding semantic boundary.

1. **Advanced flow is a finite compiled graph.** Structural back edges are
   prohibited. Repetition is represented only by the explicit repeat contract.
   Nested flow, split, nested-job, and registered custom nodes use stable
   logical identity, bounded composition, deterministic durable decisions, and
   the existing runtime/repository authority.
2. **Registered custom nodes are leaves under the one framework engine.** A
   custom handler may use framework-provided lifecycle, transaction, state,
   cancellation, and fault ports, but may not own a second executor/repository
   lifecycle, mutate the compiled graph at runtime, or bypass public guards.
3. **Scope is execution-attempt scoped.** Job scope lives for one job execution
   attempt and step scope for one step execution attempt. Late binding reads
   only typed parameters, committed execution-context state, and a closed
   framework-metadata set. Ambient environment, time, randomness, credentials,
   arbitrary I/O, and user callbacks are not resolver sources.
4. **Repeat is one explicit durable control primitive over the M6
   completion/fault engine.** Retry/skip/rollback remain inside a logical repeat
   iteration. Interceptors wrap that iteration in deterministic nesting order
   without creating another retry, transaction, or lifecycle engine.
5. **Definition evolution remains fail closed.** `DefinitionRegistry` binds one
   immutable revision to one canonical manifest/fingerprint and bounded
   application-owned artifact identity. Strict restart is default; compatible
   restart requires one explicit direct one-way edge; `Fork` creates new
   lineage and never claims restart; `Force` remains unavailable.
6. **Operator/explorer extensions inherit M4 discipline.** New M7 mutations are
   optimistic-versioned, operation-ID idempotent, audited, bounded, and
   redacted. Parameter incrementers are deterministic pure functions over typed
   parameter state and definition identity.
7. **M7 introduces definition-manifest format 4 and PostgreSQL repository
   schema 5.** Manifest formats 1-3 remain immutable/readable. Existing schema
   4 remains the M6 baseline; M7 appends an ordered `4 -> 5` migration and
   earlier supported schemas reach 5 through the existing chain. Older
   runtimes reject unsupported newer schema/manifest versions before writes.
   Operational rollback after a successful schema migration is restore-based.
8. **ADR-0009 fingerprint membership is unchanged.** Only values that select or
   reinterpret durable meaning enter the fingerprint. Framework capability
   ceilings, throughput-only budgets, credentials, runtime locations,
   telemetry, and diagnostics remain excluded.
9. **M8-M12 remain out of scope.** This decision adds no repository portability,
   messaging/HTTP/object-store certification, M10 scheduling/performance model,
   remote worker protocol, Spring migration tooling, hosted control plane, or
   generic cross-resource exactly-once claim.

## Consequences

- Issues #195-#199 may add public and durable structures only inside the closed
  contract after #194 itself passes exact-head/post-main acceptance.
- Scope/repeat persist bounded restart-relevant metadata/state only; live
  component instances and sensitive values are never durable identity.
- Manifest format 4 and schema 5 require golden vectors, populated migration and
  restore fixtures, older-runtime rejection, and PostgreSQL 15/18 failure/
  restart evidence before M7 exit.
- Database migration never rewrites persisted manifest formats 1-3 or invents a
  compatibility edge. Restart under a changed definition still follows
  ADR-0004.
- A later implementation discovery that changes public behavior, durable
  meaning, restart selection, schema/manifest interpretation, security/redaction
  boundaries, or ownership requires a superseding decision.

## Alternatives considered

- **Arbitrary graph cycles with runtime termination predicates:** rejected
  because loop progress would create a second durable restart model.
- **Ambient late binding:** rejected because restart meaning could change
  without committed authority and secrets could leak into diagnostics/state.
- **Compatibility inferred from semver/names/deserialization:** rejected by
  ADR-0004 and remains unsafe for M7.
- **Reuse PostgreSQL schema 4 for M7:** rejected because main already uses schema
  4 for M6 component-state durability.
- **Rewrite earlier manifests to format 4 during DB migration:** rejected because
  persisted definition identity is immutable.
- **Force restart:** rejected; M7 has no safe semantic or evidence model for it.

## Validation

Dependent delivery issues must satisfy the adversarial scenario matrix in
[`m7-design-gate-evidence.md`](../../project/m7-design-gate-evidence.md),
including deterministic flow/restart, scope cleanup/resolution, repeat/fault
ordering, registry concurrency/evolution, operator idempotency, format/schema
rejection, schema-4-to-5 migration/restore, dual-PostgreSQL process-kill
recovery, resource bounds, and redaction.

## Revisit triggers

Revisit if a required capability cannot be represented without structural graph
cycles; if scope requires a durable secret model; if repeat cannot compose with
M6 fault semantics without duplicate accounting; if manifest format 4 cannot
represent the M7 graph inside the accepted bounds; or if repository schema 5
cannot be migrated/restored from every supported prior schema.
