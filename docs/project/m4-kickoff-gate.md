# M4 Operations and Local Scale Kickoff Gate

**State:** Active (2026-08-01)

**Umbrella:** GitHub issue
[#10](https://github.com/luceat-lux-vestra/oxide-batch/issues/10)

**Kickoff tracking:** GitHub issue
[#74](https://github.com/luceat-lux-vestra/oxide-batch/issues/74)

This record turns the accepted M4 roadmap outcome into definition-ready work.
M4 is active, but implementation may cross a named decision boundary only
after that boundary's gate below is closed.

## Satisfied prerequisites

- [x] M3 is complete through issues #58–#65 and merged pull requests #66–#73,
      including its required quality, MSRV, security, PostgreSQL 15/18,
      process-kill, migration, and conformance gates.
- [x] M2/M3 PostgreSQL metadata, checkpoint, fault-policy state, counters,
      flow decisions, and optimistic versions preserve their accepted atomic
      and unknown-commit boundaries.
- [x] Finite compiled flow, stable logical IDs, deterministic transition
      decisions, start controls, and restart reconstruction provide the plan
      foundation required by bounded local work.
- [x] The runtime has cooperative stop, injected clocks and backoff, bounded
      blocking work, panic isolation, redaction sentinels, and reusable
      in-memory/PostgreSQL conformance and crash fixtures.
- [x] RFC-0006, RFC-0007, RFC-0008, and ADR-0005 through ADR-0007 accept the
      runtime, repository-service, control-plane, and compiled-plan target
      boundaries while retaining implementation evidence gates.

The M3 dependency on issue #10 is therefore resolved. M3 feature rows remain
unreleased `Implemented` or `Partial` evidence; completing M3 does not imply an
M4 capability or a production-readiness claim.

## Impact classification

M4 changes observable launch, query, stop, abandon, recover, shutdown,
stale-detection, retention, telemetry, local assignment, aggregation, and
restart behavior. It adds portable operator/explorer and CLI surfaces, may
extend durable metadata and plan manifests, distinguishes deployment
authorization from core lifecycle guards, and introduces explicit task,
queue, memory, connection, exporter, deadline, and output bounds.

These are public-API, compatibility, lifecycle/restart, transaction, durable
data/migration, destructive-operation, security, telemetry, and
resource/performance changes. M4 does not change cross-resource delivery
guarantees, authorize remote execution, create a hosted control plane or
scheduler, or permit a project-wide readiness claim. Parameters, contexts,
items, credentials, endpoints, SQL, user error text, retry keys, and private
policy or decider state remain excluded from public diagnostics and
low-cardinality telemetry.

## Decisions required before dependent implementation

| Gate | Owner | Required decision and evidence | Blocks |
| --- | --- | --- | --- |
| Operator and explorer services | Repository/operations owners | Bounded pagination and cursor consistency; redacted projections; idempotent request identity; launch/restart/stop/abandon/recover guards; optimistic conflict and unknown-outcome behavior; audit and deployment-authorization boundary | Portable services and CLI |
| CLI and configuration | CLI/operations owners | Command grammar, stable exit categories, human/machine output bounds, configuration precedence, secret handling, dry-run and confirmation rules, non-interactive safeguards, and broken-output behavior | Operator CLI and diagnostics |
| Shutdown and stale recovery | Runtime/repository owners | Intake stop, cancellation tree, in-flight commit/rollback policy, child joining, deadlines, durable terminal outcomes, stale evidence, clock rules, recovery proposal/application, and process-signal/kill matrix | Graceful shutdown and local-scale cancellation |
| Telemetry and diagnostics | Observability/security owners | Versioned event/metric/span catalog, commit-relative timing, safe fields, label-cardinality budget, bounded exporter queues, drop/flush behavior, overhead measurement, and incident-bundle contents | Exporter integration and exit evidence |
| Initial retention slice | Repository/operations owners | Eligibility, holds, target/version guards, bounded batches, audit, interruption/retry behavior, privilege separation, and the boundary before M8 archive/purge portability | M4 retention service and CLI |
| Bounded local scale | Plan/runtime/repository owners | Exact parallel-step/partition graph subset, deterministic aggregation, local assignment identity, durable restart state, manifest/schema evolution, thread-safety/capability validation, structured task ownership, finite resource budgets, and sequential-fallback equivalence | Parallel steps and local partitions |
| Evidence and support bounds | Quality/operations owners | Named conformance, crash/restart, destructive-action, security, cardinality, cancellation, load, soak/leak, telemetry-overhead, and PostgreSQL matrix fixtures with retained raw evidence | M4 exit claims |

Issue
[#75](https://github.com/luceat-lux-vestra/oxide-batch/issues/75)
closes these gates in canonical documents and executable fixtures. Any change
to an accepted contract still requires a superseding RFC or ADR before
dependent implementation.

## Governing architecture constraints

[RFC-0009](../rfcs/0009-transport-neutral-worker-protocol.md) remains
proposed. M4 local execution therefore cannot add remote envelopes, worker
registration, transport acknowledgements, distributed lease/fencing claims,
or cross-host coordination. Local assignment and aggregation may preserve a
future-compatible meaning, but their correctness cannot depend on the
proposed protocol.

[RFC-0005](../rfcs/0005-static-and-erased-components.md) also remains
proposed. M4 retains the current accepted boxed component boundary and does
not introduce the proposed native static hot path or use performance work to
preempt M6. Existing component thread-safety and placement constraints must be
validated explicitly at every concurrent boundary.

[RFC-0007](../rfcs/0007-repository-services-and-capabilities.md) and
[ADR-0006](../architecture/decisions/0006-repository-capability-model.md)
authorize separated operator, explorer, and retention responsibilities, but
production service changes remain gated by compatibility adapters, bounded
queries, preserved PostgreSQL behavior, and typed capability rejection.
Operator and destructive actions never bypass repository lifecycle, CAS,
definition, checkpoint, unknown-outcome, audit, or retention invariants.

[RFC-0008](../rfcs/0008-core-and-control-plane-boundary.md) and
[ADR-0007](../architecture/decisions/0007-control-plane-boundary.md) keep
portable correctness services and the minimal CLI in this repository. M4 does
not add hosted APIs, identity/RBAC, scheduling, Kubernetes, fleet management,
UI, or SaaS dependencies. Deployment authentication and authorization occur
outside core types while core actions still enforce their own guards.

The M4 local-scale slice must be narrower than complete M7 split/nested flow,
M8 retention portability, M10 multi-threaded/local-chunk performance, and M11
distributed execution. Issue #75 owns the exact boundary before code or
durable formats change. No new crate, feature flag, manifest field, schema
table, CLI command, or extension point is added merely to reserve later scope.

## Delivery workstreams and order

1. [#75](https://github.com/luceat-lux-vestra/oxide-batch/issues/75) closes
   operator/explorer, CLI, shutdown/recovery, telemetry, retention,
   configuration, local-scale, migration, security, and evidence gates.
2. [#76](https://github.com/luceat-lux-vestra/oxide-batch/issues/76) adds
   bounded runtime-neutral operator/explorer services and the accepted initial
   retention primitives, with PostgreSQL service contracts and compatibility
   adapters.
3. [#77](https://github.com/luceat-lux-vestra/oxide-batch/issues/77) adds the
   minimal guarded operator CLI, typed configuration precedence, stable exit
   categories, and redacted diagnostics over the portable services.
4. [#78](https://github.com/luceat-lux-vestra/oxide-batch/issues/78)
   implements graceful shutdown, owned-task cancellation and joining, durable
   final outcomes, stale detection, and evidence-based recovery.
5. [#79](https://github.com/luceat-lux-vestra/oxide-batch/issues/79) adds the
   versioned bounded telemetry/export mapping and redacted incident diagnostic
   bundles without making them correctness authorities.
6. [#80](https://github.com/luceat-lux-vestra/oxide-batch/issues/80)
   implements only the accepted bounded local parallel-step/partition subset,
   deterministic aggregation, durable restart behavior, sequential fallback,
   and finite resource budgets.
7. [#81](https://github.com/luceat-lux-vestra/oxide-batch/issues/81) runs the
   M4 conformance, process-kill, security, destructive-operation, load,
   cancellation, cardinality, soak/leak, and PostgreSQL evidence; publishes
   operational documentation; and records the exit gate.

After #75 closes, #76 and independent telemetry foundations in #79 may proceed
within the accepted contracts. CLI work follows portable services.
Shutdown/recovery follows the service and lifecycle gates. Local scale follows
its plan/durability gates and consumes the accepted shutdown semantics. Exit
work follows all implementation streams.

## Definition of done

M4 closes only when:

- `REPO-EXPLORE-001`, `REPO-OPERATOR-001`, the M4
  `LIFE-STOP-001`/`LIFE-RECOVER-001`/`LIFE-ABANDON-001` slices,
  `REPO-RETENTION-001`, `OBS-EXEC-001`, `OBS-METRICS-001`,
  `SCALE-PARSTEP-001`, and `SCALE-LOCALPART-001` link named executable
  evidence at the delivered boundary;
- launch, inspect, stop, restart, abandon, recover, and accepted retention
  actions are bounded, guarded, audited, and idempotent where specified;
- CLI configuration, output, exit categories, confirmations, and automation
  safeguards are deterministic, bounded, redacted, and executable;
- shutdown stops intake, propagates cancellation, joins owned children,
  applies the accepted in-flight policy, persists its outcome, and reports
  missed deadlines without guessing commit results;
- stale detection and recovery use durable evidence and cannot infer an
  ambiguous external effect or treat telemetry as authority;
- event, metric, trace, exporter, and diagnostic-bundle schemas satisfy their
  cardinality, redaction, failure-isolation, bounded-queue, flush-deadline, and
  overhead evidence;
- local parallel work owns and joins every child, uses finite queues and
  permits, aggregates deterministically, preserves completed durable work
  across restart, and matches the canonical sequential observations;
- any schema and manifest migrations pass from every supported prior version,
  reject newer or corrupt versions, and retain documented backup/restore
  rollback;
- PostgreSQL 15 and 18 integration and process-kill gates pass with validated
  TLS and least-privilege runtime/explorer/operator roles; intermediate
  supported majors retain their documented smoke coverage;
- bounded load, memory/connection/queue ceilings, cancellation latency,
  telemetry overhead, and soak/leak results retain reproducible environment,
  correctness, and raw-evidence records;
- public APIs and diagnostics expose no runtime, database, telemetry-SDK,
  credential, deployment-auth, sensitive payload, SQL, or user-error-text
  implementation types;
- CLI, configuration, telemetry, capacity, shutdown/recovery, retention,
  failure, and operator documentation is executable and reviewed.

Rows remain `Implemented`, `Partial`, or `Planned`, rather than released
`Verified`, until a named OxideBatch release satisfies the compatibility
contract's complete evidence profile.

## Scope controls

M4 does not include a hosted control plane, scheduler, UI, authentication/RBAC
implementation, Kubernetes or fleet management, cross-host coordination,
remote worker protocol, transport adapter, full definition registry, advanced
nested/split/job flow, complete retention portability, additional database
backends, multi-threaded item processing, local chunking, dynamic work
stealing, adaptive optimization, the RFC-0005 static hot path, or full Spring
Batch parity. Those remain assigned to later roadmap and decision gates.
