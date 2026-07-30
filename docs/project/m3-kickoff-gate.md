# M3 Fault Tolerance and Flow Kickoff Gate

**State:** Active (2026-07-31)

**Umbrella:** GitHub issue
[#16](https://github.com/luceat-lux-vestra/oxide-batch/issues/16)

**Kickoff tracking:** GitHub issue
[#58](https://github.com/luceat-lux-vestra/oxide-batch/issues/58)

This record turns the accepted M3 roadmap outcome into definition-ready work.
M3 is active, but implementation may cross a named decision boundary only
after that boundary's gate below is closed.

## Satisfied prerequisites

- [x] M2 is complete, including separate-process pre-commit and post-commit
      crash/restart evidence on the release-blocking PostgreSQL 15 and 18 axes.
- [x] Enlisted PostgreSQL business writes, checkpoint, context, counters, and
      optimistic step version commit or roll back together.
- [x] Restart creates distinct attempts from the last committed state and
      fails closed on definition drift or an absent directed compatibility
      edge.
- [x] The current boxed reader, processor, writer, completion, listener,
      checkpoint, context, and chunk-transaction contracts are sufficient for
      the bounded M3 slice.
- [x] The test harness supplies injected clocks and identifiers, controlled
      backoff, bounded waits, reusable repository contracts, crash workers, and
      redaction sentinels.
- [x] RFC-0004 and ADR-0005 accept immutable definitions and compiled plans,
      while preserving explicit implementation evidence gates.

The M2 correctness-evidence blocker on issue #16 is therefore resolved.

## Impact classification

M3 changes observable retry, skip, rollback, listener, step-start, and flow
behavior. It adds public typed policies and basic flow definitions, extends
durable counters and decision state, requires a PostgreSQL schema migration,
and extends the canonical definition manifest. These are compatibility,
public-API, restart, transaction, data-migration, telemetry, and resource-bound
changes.

M3 does not change the distributed protocol, add a repository backend, promise
cross-resource exactly-once behavior, or authorize a project-wide release
claim. Parameters, contexts, item values, error text, credentials, policy
private state, and decider private state remain excluded from diagnostics and
low-cardinality telemetry.

## Decisions required before dependent implementation

| Gate | Owner | Required decision and evidence | Blocks |
| --- | --- | --- | --- |
| Fault policy and error taxonomy | Runtime/API owner | Typed bounded retry, backoff, skip, and rollback/no-rollback policies; classifier inputs; exhaustion and stop behavior; fingerprint impact | Policy contracts and execution |
| Durable policy state and schema | Repository/runtime owners | Commit boundary for retry/skip/rollback counters and policy state; restart reconstruction; schema-v2 migration, restore, corruption, and newer-version rejection | PostgreSQL fault-tolerance durability |
| Listener and interceptor slice | Runtime/compatibility owners | Item/retry/skip callback taxonomy, nesting, ordering, error/panic behavior, authoritative outcome rules, and safe diagnostics | Listener contracts and runtime integration |
| One-step compiled-plan lowering | Plan/runtime owners | Logical IDs, normalization, bounded validation, canonical manifest evolution, old-manifest reader, fingerprint rules, and wrapper trace/repository equivalence | Production plan execution and basic flow |
| Basic flow and durable decisions | Plan/repository owners | Sequential and conditional graph subset, exit matching, decider inputs/results, persisted traversal, restart behavior, start limits, and allow-start-if-complete | Multi-step flow execution |
| Backoff, cancellation, and telemetry | Runtime/operations owners | Injected monotonic timing, cancellation points, maximum retained attempts/state, event timing, cardinality, and bounded diagnostic fields | Retry runtime and operational evidence |

Issue
[#59](https://github.com/luceat-lux-vestra/oxide-batch/issues/59)
closes these gates in the canonical documents and fixtures. An accepted
contract change still requires a superseding RFC or ADR before dependent
implementation.

The decisions and dependency handoff are recorded in the
[M3 design-gate evidence](m3-design-gate-evidence.md).

## Governing architecture constraints

[RFC-0005](../rfcs/0005-static-and-erased-components.md) remains proposed.
M3 therefore uses the current ADR-0002 boxed component contract and does not
introduce the proposed native static hot path, expand the standard component
catalog, or stabilize per-item boxing as the long-term design.

RFC-0004 and ADR-0005 authorize the compiled-plan direction, but production
lowering remains gated by one-step trace/repository equivalence, canonical
manifest evidence, invalid-plan tests, and reviewed migration behavior.
Existing `TaskletJob`, `TaskletStep`, `ChunkJob`, and `ChunkStep` APIs remain
compatibility wrappers.

No new crate or feature flag is created solely to reserve a future M6/M7
boundary. Basic M3 flow must leave the accepted advanced-flow target possible
without implementing nested jobs, split flow, remote nodes, or the complete
item model early.

## Delivery workstreams and order

1. [#59](https://github.com/luceat-lux-vestra/oxide-batch/issues/59) closes
   fault-policy, persistence, listener, compiled-plan, flow-decision, backoff,
   telemetry, migration, and evidence gates.
2. [#60](https://github.com/luceat-lux-vestra/oxide-batch/issues/60) adds
   runtime-neutral typed fault-tolerance and listener contracts on the current
   accepted component boundary.
3. [#61](https://github.com/luceat-lux-vestra/oxide-batch/issues/61)
   integrates deterministic retry, skip, rollback/no-rollback, listeners,
   stop, and backoff into chunk execution.
4. [#62](https://github.com/luceat-lux-vestra/oxide-batch/issues/62)
   persists accepted policy state and counters atomically in PostgreSQL schema
   version 2 and reconstructs them across restart.
5. [#63](https://github.com/luceat-lux-vestra/oxide-batch/issues/63) lowers
   existing one-step jobs into compiled plans with manifest migration and
   normalized behavior equivalence.
6. [#64](https://github.com/luceat-lux-vestra/oxide-batch/issues/64) adds
   durable sequential/conditional flow, typed deciders, exit mappings, start
   limits, and allow-start-if-complete.
7. [#65](https://github.com/luceat-lux-vestra/oxide-batch/issues/65) runs
   the M3 conformance and crash/restart matrix, publishes operational
   documentation, and records exit evidence.

After #59 closes, contract work in #60 and one-step plan work in #63 may
proceed independently. Runtime fault tolerance follows #60; PostgreSQL policy
durability follows #61. Basic flow follows #63 and consumes the accepted
durability contract where fault-tolerant steps participate. Exit work follows
all implementation streams.

## Definition of done

M3 closes only when:

- `FT-RETRY-001`, `FT-BACKOFF-001`, `FT-SKIP-001`,
  `FT-ROLLBACK-001`, the M3 `LISTENER-ITEM-001` slice,
  `FLOW-SEQUENCE-001`, `FLOW-DECIDER-001`, and the start-control scenarios
  link named executable evidence;
- retry limits, skip decisions, rollback classification, backoff, stop, and
  listener behavior remain deterministic and bounded across restart;
- enlisted business writes, checkpoint, context, policy state, counters, and
  optimistic version commit or roll back at the documented boundary;
- one-step facade and compiled-plan execution have equivalent normalized
  lifecycle traces and repository writes;
- exit mappings and deciders select one durable path, and restart cannot
  silently choose a different path;
- schema and manifest migrations pass from every supported prior version,
  reject newer versions, and retain documented backup/restore rollback;
- PostgreSQL 15 and 18 integration and process-kill gates pass with validated
  TLS and least-privilege roles; intermediate supported majors retain their
  documented smoke coverage;
- public APIs and diagnostics expose no runtime, database, serializer,
  credential, item-value, context-value, or error-string implementation types;
- fault-tolerance, flow, migration, telemetry, and failure/restart
  documentation is executable and reviewed.

Rows remain `Implemented`, rather than released `Verified`, until a named
OxideBatch release satisfies the compatibility contract's full evidence
profile.

## Scope controls

M3 does not include the full standard item catalog, the RFC-0005 static hot
path, advanced repeat/composite policies, nested jobs, split/parallel flow,
definition fork/savepoint behavior whose contract remains open, additional
repository backends, operator CLI, local parallelism, remote execution, or
full Spring Batch parity. Those remain assigned to later roadmap gates.
