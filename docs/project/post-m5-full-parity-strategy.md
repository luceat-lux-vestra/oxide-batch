# Post-M5 Full-Parity and Architecture Strategy

**State:** Active

**Document type:** Strategic umbrella

**Source date:** 2026-07-29

**Comparison baseline:** Spring Batch 6.0.4 and the Spring Batch 6.x
reference and API documentation

This document preserves the accepted long-term product and architecture
rationale for evolving OxideBatch beyond the original M0-M5 program. It is not
the sole implementation specification. Detailed normative authority belongs to
the documents in the [canonical document map](#canonical-document-map).
RFC-0005 was accepted on 2026-08-03 and is recorded as ADR-0008; RFC-0009
remains proposed. Every change beyond the accepted decisions still requires its
named evidence gate or a superseding ADR.

## Strategic conclusion

The accepted long-term goal has three parts:

1. cover every documented Spring Batch 6.x user capability and observable
   execution semantic;
2. provide an independent Rust-native API and engine that use Rust's type
   system, ownership, static dispatch, and structured concurrency rather than
   reproducing Java APIs or the Spring container;
3. provide stronger options for distributed execution, verifiable state,
   reproducibility, resource control, data efficiency, and operational
   transparency.

“Complete parity” would mean that every feature in the fixed Spring Batch 6.x
ledger has an exact observable equivalent, a reviewed Rust-native equivalent,
or a documented divergence or not-applicable rationale, with executable
evidence. It would not mean Java source or binary compatibility, unchanged
Spring Bean configuration, a shared live metadata schema, or automatic
translation of arbitrary Java code.

The compatibility dimensions are:

- **semantic parity:** equivalent meanings for jobs, steps, instances,
  executions, statuses, parameters, restart, and checkpoints;
- **behavioral parity:** equivalent external results for the same input,
  failure, stop, and restart scenarios;
- **feature parity:** an OxideBatch capability or reviewed disposition for
  every documented Spring Batch feature;
- **operational parity:** equivalent launch, stop, restart, abandon, recover,
  explore, and retention capabilities, even when APIs differ;
- **migration parity:** explicit tools for converting definitions and
  metadata, with reports for constructs that require manual porting.

The denominator for a future “complete” claim must come from the Spring Batch
reference, public API packages, metadata schemas, integration modules, and test
module. OxideBatch must not define a conveniently small supported subset and
call it complete.

## Canonical document map

| Topic | Canonical owner | Approval dependency |
| --- | --- | --- |
| Product target, parity meanings, scope layers, and claim rules | [Vision and scope](../product/vision-and-scope.md) | [RFC-0001](../rfcs/0001-m5-preview-and-project-wide-1-0.md), [RFC-0002](../rfcs/0002-full-spring-batch-feature-ledger-parity.md) |
| Market positioning and claim ladder | [Alternatives and positioning](../product/alternatives-and-positioning.md) | RFC-0001 and RFC-0002 |
| Milestone order and exit evidence | [Delivery roadmap](../roadmap.md) | RFC-0001 through RFC-0010 as named by each milestone |
| Compatibility baseline, row states, and claims | [Spring Batch compatibility contract](../compatibility/spring-batch.md) | RFC-0002 |
| Complete feature population and row ownership | [Spring Batch feature ledger](../compatibility/conformance-matrix.md) | RFC-0002 |
| Executable and differential evidence | [Conformance strategy](../compatibility/conformance-strategy.md) | RFC-0002 |
| Lifecycle, restart, transaction, delivery, lease, and fencing semantics | [Execution semantics](../compatibility/execution-semantics.md) | RFC-0004, RFC-0007, RFC-0009 |
| Target layers and dependency direction | [Architecture overview](../architecture/overview.md) | RFC-0003, RFC-0005, RFC-0006 |
| Definitions, plans, revisions, and fingerprints | [Execution-plan architecture](../architecture/execution-plan.md) | RFC-0004 |
| Item components and static/erased boundaries | [Item-processing model](../architecture/item-processing-model.md) | RFC-0005 |
| M3 fault policies and item/retry/skip listeners | [M3 fault-tolerance contract](../architecture/fault-tolerance.md) | Existing M3 scope and ADR-0002 |
| M3 basic flow, deciders, and start controls | [M3 basic-flow contract](../architecture/basic-flow.md) | RFC-0004 and ADR-0005 |
| Repository ports and transaction capabilities | [Repository and transaction model](../architecture/repository-and-transaction-model.md) | RFC-0007 |
| Integration categories and delivery capabilities | [Integration model](../architecture/integration-model.md) | RFC-0007 |
| Coordinator/worker semantics and protocol | [Distributed execution](../architecture/distributed-execution.md) | RFC-0009 |
| Core versus external operations boundary | [Control-plane boundary](../operations/control-plane-boundary.md) | RFC-0008 |
| Spring definition and metadata migration | [Spring Batch migration](../compatibility/spring-batch-migration.md) | RFC-0010 |
| Context codecs, schema evolution, external blobs, and corruption handling | [Persistence and migrations](../operations/persistence-and-migrations.md) | RFC-0007 and RFC-0010 where durable formats change |
| Wall-clock/deadline, logical/storage ID, and stable failure semantics | [Execution semantics](../compatibility/execution-semantics.md) | Subsystem RFC/ADR when an accepted public contract changes |
| Lifecycle hooks, interceptors, and component state | [Item-processing model](../architecture/item-processing-model.md) | RFC-0005 |
| Static, registered, out-of-process, and WASI extension modes | [Integration model](../architecture/integration-model.md) | RFC-0005 and RFC-0009 |
| Structured concurrency, cancellation, and backpressure invariants | [Execution semantics](../compatibility/execution-semantics.md) | RFC-0006 and RFC-0009 |
| Allocation, concurrency, and benchmark budgets | [Performance plan](../engineering/performance-plan.md) | RFC-0005 and milestone evidence |
| Schema lifecycle, adapters, export/import, and retention | [Persistence and migrations](../operations/persistence-and-migrations.md) | RFC-0007 and RFC-0010 |
| Documentation precedence and freshness | [Documentation strategy](../documentation/strategy.md) | None |
| Release channels, support, and stability | [Release and support policy](../release/support-policy.md) | RFC-0001 |

## Control-plane boundary

The core repository should continue to own the correctness-bearing operational
contract:

- `JobOperator`, `JobExplorer`, `JobRegistry`, and definition-resolution
  services;
- launch, stop, restart, abandon, recover, stale-detection, and recovery
  decisions;
- paginated or streaming execution queries;
- metadata retention and purge primitives;
- stable telemetry events and metrics;
- coordinator/worker protocol types and administrative DTOs;
- a minimal CLI, embedded mode, worker mode, conformance tests, and protocol
  compatibility tests.

A future `oxide-batch-ops` or `oxide-batch-control-plane` project should own:

- hosted REST/gRPC APIs and web UI;
- authentication, RBAC, tenants, and organizations;
- schedulers, calendars, Kubernetes controllers, and fleet management;
- alerts, notifications, dashboards, and audit search;
- secret-backend integration, deployment topology, high availability, quotas,
  billing, and other SaaS concerns.

Creating a separate repository immediately would multiply coordination while
protocols are unstable. The proposed sequence is to establish
`oxide-batch-protocol`, `oxide-batch-cli`, and a thin reference server in the
core workspace, then extract the control plane after operator semantics and
protocol compatibility gates are met.

Scheduling remains outside the engine. The core executes an explicit launch
request correctly; it does not decide when that request occurs. Operator APIs
must nevertheless support an idempotency key so scheduler retries can be
deduplicated.

## Strengths to preserve

Future work must preserve these established properties:

- the repository is authoritative for execution identity and lifecycle state;
- a committed checkpoint and enlisted business write are atomic;
- arbitrary external resources never receive a blanket exactly-once promise;
- unit-of-work changes are not published before commit;
- job parameters distinguish typed values and identifying roles;
- parameters and context are sensitive by default;
- clocks and ID sources are injectable for deterministic tests;
- lifecycle transitions use an explicit state machine;
- listener ordering and failure semantics are executable contracts;
- property, compile-fail, conformance, crash, and failure tests are design
  evidence rather than optional polish;
- core contracts do not expose SQLx, PostgreSQL, Tokio, or OpenTelemetry SDK
  types;
- `unsafe`, panics, and unchecked assumptions remain exceptional and reviewed.

Correct restart and failure semantics are a more important differentiator than
the number of features.

## Accepted architecture direction and open proposals

RFC-0001, RFC-0002, RFC-0003, RFC-0004, RFC-0006, RFC-0007, RFC-0008, and
RFC-0010 were accepted on 2026-07-30. Their implementation evidence gates still
apply. RFC-0005, the static/erased component architecture, was accepted on
2026-08-03 and is recorded as ADR-0008; its implementation is M6 scope. The
distributed worker protocol (RFC-0009) remains proposed.

### Reinterpret M5 and project-wide 1.0

The original M5 freezes public APIs and metadata for a project-wide 1.0 while
distributed execution, additional repositories, integrations, and migration
remain outside its scope. Full parity makes that freeze premature.

M5 is an Embedded Core Production Preview in the `0.x` line, and project-wide
1.0 is deferred to M14. The exact preview version is selected during release
planning. RFC-0001 owns this decision.

### Stage workspace extraction

The accepted facade rule remains useful, but domain, orchestration,
repository, item, observability, and test responsibilities should move behind
real workspace boundaries as dependencies require them. Extraction must be
incremental, retain facade re-exports, and pass behavior-equivalence tests.
RFC-0003 accepts the target boundary and extraction order.

The long-term namespace forecast is:

```text
crates/
  oxide-batch/
  oxide-batch-core/
  oxide-batch-plan/
  oxide-batch-engine/
  oxide-batch-engine-tokio/
  oxide-batch-item/
  oxide-batch-repository/
  oxide-batch-repository-memory/
  oxide-batch-repository-postgres/
  oxide-batch-repository-mysql/
  oxide-batch-repository-sqlite/
  oxide-batch-repository-sqlserver/
  oxide-batch-repository-oracle/
  oxide-batch-repository-db2/
  oxide-batch-transaction/
  oxide-batch-flow/
  oxide-batch-fault-tolerance/
  oxide-batch-partition/
  oxide-batch-distributed/
  oxide-batch-protocol/
  oxide-batch-observability/
  oxide-batch-test/
  oxide-batch-spring-compat/
  oxide-batch-macros/
  oxide-batch-cli/

integrations/
  oxide-batch-file/
  oxide-batch-csv/
  oxide-batch-json/
  oxide-batch-xml/
  oxide-batch-avro/
  oxide-batch-arrow/
  oxide-batch-parquet/
  oxide-batch-kafka/
  oxide-batch-amqp/
  oxide-batch-nats/
  oxide-batch-pulsar/
  oxide-batch-sqs/
  oxide-batch-redis/
  oxide-batch-object-store/
  oxide-batch-http/
```

This list does not authorize empty packages or publication. A crate is created
only when a dependency boundary and independent support obligation exist.

### Separate the static hot path from erased composition

Using `BoxFuture` and `dyn Trait` at a heterogeneous composition boundary can
be ergonomic, but using them for every item operation would force allocation
and dynamic dispatch into the hottest path.

RFC-0005, accepted and recorded as ADR-0008, specifies this dual path:

1. a generic, associated-type/native-async path in which reader, processor, and
   writer calls can be monomorphized and do not allocate a future per item;
2. erased adapters for dynamic registries, heterogeneous plans, the ergonomic
   facade, and out-of-process boundaries, with boxing limited to step or chunk
   boundaries.

The public surface may provide both an ergonomic erased builder and a
maximum-performance generic builder. RFC-0005 owns the change to the accepted
boxed-future rule and requires measurements of allocation, throughput, binary
size, compile time, object safety, and API ergonomics.

### Make the core runtime-neutral and the engine explicit

Tokio remains a suitable initial engine. RFC-0006 accepts that
`oxide-batch-core`, `oxide-batch-item`, and `oxide-batch-repository` do not
depend on it, while the default engine names and tests its Tokio dependency.
OxideBatch should define only narrow spawn, cancellation, deadline, and
blocking boundaries; it should not reimplement a generic async runtime.
Alternative executors should be added only when demand and conformance
evidence justify them. RFC-0006 owns this boundary.

### Compile immutable definitions into execution plans

The one-step `TaskletJob`/`TaskletStep` model should lower into a general,
immutable model:

```text
JobDefinition
  -> DefinitionId + Revision + Fingerprint
  -> ParameterSchema
  -> FlowGraph
       -> StepNode
       -> DecisionNode
       -> SplitNode
       -> JobNode
       -> End / Fail / Stop transition

StepDefinition
  -> Tasklet
  -> Chunk
  -> Partition
  -> Remote
  -> NestedJob
  -> Custom extension
```

Compilation must reject unreachable nodes, invalid or non-terminating cycles,
duplicate stable names, missing exit mappings, incompatible restart policies,
capability mismatches, non-thread-safe components placed in concurrent plans,
uncheckpointable readers in restartable steps, and components that cannot
cross a requested remote boundary. Existing tasklet types should remain
compatibility wrappers during staged lowering. RFC-0004 and ADR-0005 own this
decision.

### Persist definition identity and evolution

Accepted ADR-0004 already makes revision, canonical manifest, digest,
component IDs, parameter/context schemas, and directed upgrades durable. A
compiled plan should extend rather than replace that rule.

The target operator choices are:

- `Strict`: require the same fingerprint;
- `Compatible`: allow only a registered directed upgrade;
- `Fork`: create a new instance lineage instead of resuming.

`Force` is not accepted. A disabled-by-default audited override would require a
separate decision and could not fabricate stronger guarantees.

The canonical execution-plan document and ADR-0005 own the fork/lineage
extension. No implementation may weaken ADR-0004 during staged lowering.

### Separate repository services and capabilities

A single object-safe repository port should not accumulate every command,
query, chunk transaction, and operator action. The accepted model separates:

- `JobRepository` for metadata commands and lifecycle compare-and-swap;
- `JobExplorer` for bounded read-only queries;
- `JobOperator` for launch, restart, stop, abandon, and recover services;
- `DefinitionRegistry` for definition resolution;
- `ExecutionTransaction` and `ChunkTransaction`;
- `RepositoryCapabilities` for adapter features;
- `RetentionRepository` for archive and purge primitives.

Every list operation should be paginated or streamed. RFC-0007 owns the
service split, capability negotiation, and adapter certification.

### Model transactions and delivery honestly

The preferred PostgreSQL path enlists business writes and checkpoint metadata
in the same transaction. Other resources require explicit modes:

- `AtomicSameResource`;
- `TransactionalMessage`;
- `Outbox`;
- `InboxDedup`;
- `IdempotentExternalEffect`;
- `AtLeastOnce`;
- `BestEffort`.

A step declares its required delivery mode. Plan compilation rejects a
component/adapter combination that cannot supply it. No abstraction may imply
distributed ACID or generic exactly-once behavior. The repository/transaction
and integration documents own these accepted target capabilities under
RFC-0007 and ADR-0006.

## Supporting model improvements

### Execution context

Versioned JSON remains the initial accepted codec, not the permanent universal
representation. A future context model should support component namespaces,
typed keys and codecs, schema IDs and versions, maximum size and quota,
redaction classification, inline versus external blobs, compression
thresholds, deterministic migration, unknown-field policy, canonical encoding,
checksums, and corruption detection. Compact and application-defined codecs
may supplement JSON. Large state should use a bounded, content-addressed blob
adapter rather than an unbounded database row.

### Time and identifiers

Persisted timestamps use UTC wall-clock instants; durations, timeouts,
backoff, and lease deadlines use monotonic time. Distributed lease rules must
account for clock skew, and tests require a virtual clock.

Logical definition and instance identities are distinct from adapter-owned
storage keys. External correlation uses a sortable opaque identifier such as
UUIDv7 or an equivalent. Process-local counters are suitable only for the
in-memory adapter and never establish distributed uniqueness.

### Error taxonomy

The stable taxonomy should cover validation, optimistic conflict, transient
and permanent resources, timeout, cancellation/stop, serialization/version
mismatch, corruption/invariant violation, unsupported capability, user
component failure, operator rejection, and unknown commit outcome. Retry
classification uses categories and capabilities, never strings. Public
context is redacted; full source chains are diagnostic-only.

### Lifecycle hooks and events

Parity requires job, step, chunk, read, process, write, skip, retry,
partition, flow-decision, and recovery hooks. Ordered interceptors that may
change outcomes must be distinguished from non-authoritative telemetry events.
Failure and panic behavior for every hook belongs in the ledger and executable
conformance scenarios.

### Extensions

OxideBatch should not promise a stable Rust dynamic-library ABI. Preferred
extension modes are:

- Cargo features and static linking for high-performance components;
- traits plus facade registration for third-party Rust components;
- an out-of-process protocol for dynamic or language-neutral extensions;
- WASI components for sandboxed processors.

### Structured concurrency and backpressure

Job, step, partition, and chunk tasks should form an owned task tree. Detached
tasks are prohibited. Cancellation propagates to children, children are joined,
channels and queues are bounded, and memory, connections, in-flight chunks,
broker credits, and blocking threads have declared budgets. Graceful stop
ceases intake, applies the in-flight policy, checkpoints or rolls back, and
persists the outcome in that order.

## Rust-native API principles

OxideBatch should not translate Java builders, emulate Bean scopes with a
service locator, use reflection-like component lookup, copy exception class
hierarchies into strings, or hide every resource behind one transaction
manager.

Prefer immutable typed definitions, compile-time-compatible components,
explicit dependency injection and ownership, enum-based lifecycle state,
typed errors, explicit capabilities, static dispatch on the hot path, and
erasure only at composition boundaries.

The Rust-native equivalent of job/step scope should be a
`JobComponentFactory` or `StepComponentFactory` that receives typed execution
context and constructs an instance whose lifetime is tied to that execution.
Typed resolvers and closures are the default late-binding mechanism; an
expression DSL is optional.

Validation belongs at three boundaries:

- Rust compilation: item-type compatibility, required `Send`/`Sync`, and
  capability marker compatibility;
- plan compilation: graph completeness, parameters, resources, restart and
  checkpoint support, remoting support, and persisted-definition compatibility;
- launch/runtime: actual resources, negotiated database/broker capabilities,
  lease ownership, optimistic versions, and dynamic partition inputs.

## Complete feature population

The canonical ledger must include:

- core domain, identity, parameters, statuses, contexts, repository, explorer,
  operator, registry, launch, stop, restart, abandon, and recovery;
- tasklet, chunk, custom, nested-job, remote-step, start-limit,
  allow-start-if-complete, non-restartable, and graceful-stop step behavior;
- sequential/conditional flow, deciders, splits, end/fail/stop transitions,
  nested flows, and restart controls;
- readers, processors, writers, item streams, checkpoints, composites,
  classifiers, delegates, peek/aggregate/multi-resource forms, validators,
  filters, and synchronization wrappers;
- retry, backoff, retry context/cache, skip, rollback classification,
  completion policies, repeat operations, and durable counters;
- job/step-scoped component equivalents, late binding, runtime resource
  selection, and scoped cleanup;
- parallel steps, multi-threaded processing, local chunking, local and remote
  partitioning, remote chunking, remote step, and fabric-independent restart;
- flat/fixed-width, CSV, XML, JSON, Avro, database, ORM/repository-equivalent,
  MongoDB, Kafka, AMQP/JMS-equivalent, Redis, queue/channel, mail, LDAP, and
  other documented integrations, with demand tiers rather than omissions;
- full-job, single-step, scoped-component, fixture, cleanup, restart/failure,
  and transport-independent distributed testing utilities;
- schema, migration, locking, IDs, PostgreSQL, MySQL/MariaDB, SQLite,
  SQL Server, Oracle, DB2, HANA, and the disposition of non-relational
  metadata repositories.

Spring-specific features that are not meaningful in Rust still receive a
reviewed row explaining the equivalent or not-applicable rationale. The
full-parity gate has no `Unknown`, `Deferred`, or `Untested` rows.

## Continuous roadmap

The detailed objectives, dependencies, evidence, non-goals, and release impact
are canonical in the [roadmap](../roadmap.md). The intended sequence is:

- **M5 — Embedded Core Production Preview:** a correct PostgreSQL,
  single-host kernel with future-compatible definitions and boundaries;
- **M6 — Complete item-processing model, standard components, and user test
  kit;**
- **M7 — Advanced flow, repeat, scoped/late-bound components, and composition
  parity;**
- **M8 — Repository portability, relational databases, and capability model;**
- **M9 — Messaging and streaming integrations with explicit delivery
  semantics;**
- **M10 — Local scalability and high-performance execution model;**
- **M11 — Distributed partitioning, remote chunking, remote step, and worker
  protocol;**
- **M12 — Spring Batch feature-ledger closure and migration tooling;**
- **M13 — Ecosystem, extension SDK, reference workloads, compatibility
  certification, and evidence-backed differentiators;**
- **M14 — Project-wide 1.0/GA evidence and support commitment.**

The sequence deliberately keeps current M2 durable restart work moving while
preventing choices that would block the longer-term plan.

## Beyond-parity priorities

The first differentiation tier is:

1. compiled plans and definition fingerprints;
2. exact transaction capabilities and an effect journal;
3. deterministic execution traces and replay;
4. structured concurrency with declarative resource budgets;
5. one plan with equivalent local and distributed semantics.

The second tier is adaptive chunk sizing, dynamic partitioning and work
stealing, Arrow/Parquet columnar processing, savepoints/forks/lineage, and a
Spring differential harness.

The third tier is WASI components, a plan optimizer, bounded batch/stream
convergence, and configuration DSL or UI integration. These must not distract
from core correctness.

Candidate differentiators include:

- optional Arrow `RecordBatch` chunks, vectorized processing, schema evolution,
  zero-copy paths, and spill-to-disk;
- adaptive chunk sizing whose decisions are persisted so restart remains
  deterministic;
- first-class memory, concurrency, connection, broker-credit, blocking-thread,
  and deadline budgets;
- replayable traces of policy choices, chunk boundaries, partition assignments,
  and retry delays;
- named savepoints, forked execution lineages, backfills, and input
  fingerprints;
- lease-based dynamic partition queues, skew detection, work stealing, and
  deterministic ownership history;
- bounded WASI components with versioned interfaces;
- an effect journal that records idempotency key, attempt, response
  fingerprint, and unknown-outcome reconciliation;
- plan optimization based on capabilities and data locality, with a canonical
  unoptimized fallback;
- a shared finite-batch and bounded-replay-stream checkpoint model without
  becoming a general streaming engine.

Each differentiator needs benchmark, failure, or replay evidence. Optimization
must not change default semantics, and disabling it must recover the canonical
deterministic execution.

## Performance direction

The native hot path should avoid:

- one heap allocation or boxed future per item;
- one trait-object lookup per item;
- one JSON context encoding per item;
- unbounded queues, metadata lists, or caches;
- blocking I/O on async workers;
- high-cardinality metric labels;
- unnecessary item clones;
- a global lock serializing unrelated steps.

Candidate techniques include generic pipelines, reusable chunk buffers,
borrowed byte slices, batched database writes, keyset pagination, bounded
prefetch, ordered commit barriers, buffer pools, spill-to-disk, optional Arrow
record batches, and capability-specific fast paths.

The benchmark suite should cover no-op tasklets, per-item overhead, static
versus erased paths, CSV-to-PostgreSQL, PostgreSQL-to-Parquet, retry/skip-heavy
workloads, checkpoint-size scaling, 1/10/100 local and remote partitions,
remote duplicate delivery, metadata histories from thousands to hundreds of
millions of executions, recovery time, and memory ceilings. Reports record
workload, hardware, database/broker versions, and durability settings.

## Evidence direction

Each feature receives an evidence profile chosen from unit, state-machine
property, compile-fail/type, adapter contract, integration, crash injection,
migration, conformance, differential, and performance/resource-limit tests.
An omitted evidence class requires a rationale.

Concurrency evidence should cover duplicate launches, concurrent restart/stop,
stale-lease commits, listener failures, chunk commit versus cancellation, and
coordinator/worker ownership races, using model checking where appropriate.

Fuzz targets include parameter encoding, plan manifests/fingerprints, context
codecs/migrations, file parsers, protocol decoders, graph compilation, and
migration imports.

The crash matrix includes before/after read, process, and write; before
business commit; unknown commit result; before/after checkpoint; listener
boundaries; partition assignment; worker result transmission; and coordinator
state update. Expected replay, counters, context, status, and external effects
are recorded for each point.

## Delivery discipline

Every implementation slice should:

1. read current code and normative documents;
2. list the invariants and ledger rows it changes;
3. obtain an accepted RFC/ADR when required;
4. add compile-fail/API evidence before incompatible public API changes;
5. implement the smallest useful vertical slice;
6. add unit, property, contract, integration, conformance, crash, and
   performance evidence as applicable;
7. update the ledger, roadmap evidence, and user/operator documentation;
8. record before/after measurements for performance-sensitive work;
9. avoid unrelated cleanup.

Agents must not rewrite the runtime or repository without evidence, claim
parity by copying Spring class names, combine static and erased traits without
a boundary, hide resource differences behind a leaky transaction abstraction,
make blanket exactly-once claims, rely on telemetry or a broker for
correctness, introduce unbounded resources, change context formats without
versioning, stabilize APIs without evidence, expose infrastructure types from
core, or promise a stable Rust dynamic-library ABI.

The recommended first implementation sequence after approval is:

1. decide M5/1.0 semantics;
2. freeze the complete feature-ledger population;
3. validate staged crate extraction;
4. measure and decide the static/erased component boundary;
5. compile plans and connect definition fingerprints;
6. finish M2 durable chunk/restart;
7. finish the original M3-M5 correctness scope;
8. execute M6-M14 through their evidence gates.

The program delivers capabilities only when restart, transaction, failure,
migration, observability, and bounded-resource semantics are complete.

## Recommended implementation issue structure

Future implementation issues should preserve this information:

```markdown
## Objective
State one user-observable capability.

## Normative references
- Governing OxideBatch specification or accepted ADR/RFC
- Spring Batch feature-ledger row

## Existing invariants
- State, transaction, restart, and delivery rules that must remain true

## In scope
- Smallest useful vertical slice

## Out of scope
- Work deliberately assigned to a later issue

## API impact
- Public or internal boundary and compatibility/deprecation plan

## Failure model
- Retry, rollback, cancellation, crash, and unknown-commit behavior

## Verification
- Unit, property, contract, integration, conformance, crash, migration, and
  performance evidence as applicable

## Exit criteria
- Executable, reviewable completion conditions

## Evidence
- Test, benchmark, decision, and feature-ledger links
```
