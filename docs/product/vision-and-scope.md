# Product Vision and Scope

**State:** Accepted

**Long-term direction:** Accepted by RFC-0001 and RFC-0002.

## Vision

OxideBatch is an idiomatic Rust framework for reliable, restartable,
observable batch workloads. It adopts the proven domain language and
observable behavior of Spring Batch without reproducing Java APIs or the
Spring container.

The long-term target is complete coverage of the documented Spring Batch 6.x
feature population and execution semantics, as accepted by
[RFC-0002](../rfcs/0002-full-spring-batch-feature-ledger-parity.md). The
[feature ledger](../compatibility/conformance-matrix.md) is the only permitted
denominator for future parity claims.

## Target users

- Rust teams running data import, export, reconciliation, settlement, and ETL;
- platform teams requiring durable metadata and guarded operator controls;
- organizations migrating selected Spring Batch workloads to Rust;
- library authors implementing reusable components, repositories, transports,
  or operational integrations.

## Product principles

1. Correct restart behavior is more important than raw throughput.
2. Transaction and delivery guarantees are explicit and resource-scoped.
3. Durable state transitions are auditable and concurrency-safe.
4. Stable core contracts remain independent of database, CLI, executor, and
   telemetry SDK types.
5. APIs and execution paths are Rust-native even when behavior is equivalent
   to Spring Batch.
6. Failure paths, bounded resources, and executable evidence are first-class
   design inputs.
7. A compatibility or readiness claim cannot exceed its recorded evidence.

## Compatibility meanings

| Dimension | Meaning |
| --- | --- |
| Semantic parity | Equivalent domain meaning for jobs, steps, identities, statuses, parameters, checkpoints, and restart |
| Behavioral parity | Equivalent externally observable result for named input/failure/stop/restart scenarios |
| Feature parity | An implemented equivalent or reviewed disposition for every row in the complete feature ledger |
| Operational parity | Equivalent launch, stop, restart, abandon, recover, explore, registry, and retention capability through any documented API |
| Migration parity | Versioned tools and reports for definition and metadata conversion |

An **exact behavioral equivalent** produces the same normalized observations
for the row's complete scenario set. A **native equivalent** solves the same
user and operator need through an idiomatic Rust mechanism while its reviewed
difference remains visible in the ledger.

Java source/binary/API compatibility, unchanged Spring dependency-injection
configuration, and shared live-schema compatibility are excluded from the
native core. They may not be inferred from feature or behavioral parity.

## Product layers

| Layer | Responsibility |
| --- | --- |
| Embedded core | definitions, execution, lifecycle, restart, item processing, repository ports, and local operation |
| Distributed execution | coordinator/worker protocol, remote step/chunk/partition, leases, fencing, and semantic equivalence |
| Integrations | database, file, object-store, messaging, HTTP, telemetry, and extension adapters |
| External control plane | hosted APIs, UI, identity/RBAC, scheduler/calendar, fleet, Kubernetes, alerts, and SaaS concerns |

The core owns operator and explorer semantics, recovery decisions, retention
primitives, telemetry schema, and worker protocol. A future control-plane
project hosts those contracts but does not become the correctness authority.

## Current near-term scope

The accepted M0-M4 program remains the active delivery commitment:

- job/step/instance/execution domain model and typed parameters;
- tasklet and chunk-oriented single-host execution;
- durable PostgreSQL metadata and versioned execution context;
- restart, stop, abandon, explicit recovery, and transaction boundaries;
- retry, skip, backoff, listeners, conditional flow, local bounded
  concurrency and partitioning;
- operator CLI, vendor-neutral telemetry, and failure/conformance evidence.

M2, M3, and M4 are complete implementation milestones; M5 is the active
near-term milestone under its compiled-plan/fingerprint, component-boundary,
crate-extraction, context-codec, transaction-capability, facade/API,
ledger-promotion, and evidence design gates. Historical M0-M4 gates remain
authoritative evidence for completed work, while release and
production-readiness claims remain gated by their separate evidence.

## Release interpretation

[RFC-0001](../rfcs/0001-m5-preview-and-project-wide-1-0.md) establishes:

- M5 becomes an Embedded Core Production Preview/stabilization boundary rather
  than project-wide 1.0;
- M6-M13 close item, flow, repository, integration, scale, distributed,
  migration, ecosystem, and certification gaps;
- M14 is the project-wide 1.0/GA evidence and support gate.

## Permanent non-goals

- Java source, binary, annotation, XML, or Spring Bean compatibility in the
  native core;
- concurrent Spring Batch and OxideBatch writes to one metadata schema;
- transparent exactly-once effects across arbitrary resources;
- automatic translation of arbitrary Java application code;
- a stable in-process Rust dynamic-library ABI;
- making the embedded engine itself a general scheduler, workflow service,
  Kubernetes control plane, or unbounded streaming engine.

Distributed execution, additional databases, messaging, and migration tooling
are planned future scope, not permanent non-goals and not current capability.

## Claim and evidence rules

- “Inspired by Spring Batch” needs no parity implication.
- A named semantic or behavioral claim requires matching `Verified` ledger
  rows for a released OxideBatch version.
- A category-level parity claim requires every row in that category to have a
  reviewed terminal disposition and all claimed rows to be `Verified`.
- “Complete documented feature parity” requires the entire pinned ledger to
  have no `Unknown`, `Deferred`, `Planned`, `Implemented`, `Partial`, or
  `Untested` row.
- “Migration compatible” requires released tooling and round-trip/reconciliation
  evidence for the named source/target versions.
- “Production preview” requires the accepted M5 evidence gate.
- “Enterprise-ready” and “project-wide 1.0/GA” require the M14 support,
  upgrade, security, compatibility, distributed, and reference-workload
  evidence. They must not be used earlier as project-wide claims.

## Success measures

- every execution is explainable from durable metadata and a versioned plan;
- crashes and restarts produce the documented outcome at every boundary;
- feature claims are traceable from a ledger row to executable evidence;
- resource use and cancellation are bounded and measurable;
- adapters preserve resource-native guarantees and disclose divergence;
- a new user can build, fail, inspect, restart, and migrate documented
  reference workloads using released guides and tools.
