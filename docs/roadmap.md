# Delivery Roadmap

**State:** Active

**Program decision:** M5-M14 is accepted by RFC-0001. RFC-0005 was accepted on
2026-08-03 and is recorded as ADR-0008. RFC-0009 remains an evidence gate for
its specific architecture.

**Last reviewed:** 2026-08-03

Milestones are capability and evidence gates, not calendar promises. M0-M4
record accepted history; M5 is the active gate. The continuous M5-M14 program is
accepted by [RFC-0001](rfcs/0001-m5-preview-and-project-wide-1-0.md) and the
topic decisions linked below. Historical M0 decisions remain preserved with
their supersession record.

The [feature ledger](compatibility/conformance-matrix.md) owns feature status.
This roadmap owns sequencing and milestone evidence, not detailed semantics.

## M0 — Foundation

**Status:** Complete (2026-07-29)

**Objective:** authorize implementation with accepted product, compatibility,
correctness, architecture, engineering, security, release, and governance
contracts.

**Delivered:** product scope; Spring Batch 6.0 baseline; lifecycle, restart,
transaction, API, dependency, persistence, release, support, and documentation
policies; architecture spikes; first-slice acceptance scenarios.

**Exit evidence:** [preparation master plan](project/preparation-master-plan.md)
and [runtime kickoff gate](project/kickoff-gate.md).

**Release implication:** foundation-only pre-release; no runtime capability
claim.

## M1 — Executable Kernel

**Status:** Complete (2026-07-29)

**Objective:** run a deterministic single-process, single-tasklet job against
the in-memory repository.

**Delivered:** core identities and statuses, typed parameters, validated state
transitions, tasklet execution, repository/clock/ID ports, listeners,
diagnostics, in-memory adapter, conformance harness, and first example.

**Ledger slice:** initial domain identity, job/step lifecycle, tasklet,
listener, inspection, and telemetry rows are `Implemented`, not released
`Verified` claims.

**Exit evidence:** [M1 exit evidence](project/m1-exit-evidence.md).

**Release implication:** pre-release executable kernel; no durable repository
claim.

## M2 — Durable Chunk and Restart

**Status:** Complete (2026-07-30)

**Objective:** resume a PostgreSQL-backed chunk job from the last committed
checkpoint after process failure without corrupting metadata.

**User-visible capabilities:** reader/processor/writer contracts, chunks,
versioned context and checkpoints, durable launch/restart/recovery, and
PostgreSQL metadata.

**Architecture deliverables:** accepted definition identity, PostgreSQL
physical model, adapter-owned unit of work, same-resource transaction
enlistment, and explicit unknown-commit outcome.

**Ledger categories:** durable identity/repository, basic chunk, stream state,
checkpoint, restart, and PostgreSQL reference semantics.

**Dependencies:** M1 plus the accepted M2 design, component, and repository
gates.

**Non-goals:** retry/skip breadth, conditional flow, automatic orphan takeover,
operator CLI, distributed execution, or arbitrary-resource exactly-once.

**Exit evidence:** atomic business/checkpoint commit, pre/post-commit crash
matrix, duplicate launch, optimistic conflict, version migrations,
PostgreSQL 15/18 release-blocking integration, TLS/role evidence, and durable
conformance rows.

**Documentation:** setup, transaction guarantees, migration, crash/restart,
recovery, and M2 exit record.

**Release implication:** durable PostgreSQL capability remains pre-1.0 and is
claimed only for supported matrix rows.

**Exit evidence:** [M2 durable chunk and restart exit
evidence](project/m2-exit-evidence.md).

See the [M2 kickoff gate](project/m2-kickoff-gate.md).

## M3 — Fault Tolerance and Flow

**Status:** Complete (2026-08-01)

The decision gates, dependency-ordered workstreams, architecture constraints,
and definition of done are recorded in the
[M3 kickoff gate](project/m3-kickoff-gate.md). The fault-tolerance, listener,
compiled-plan, flow, schema, migration, and evidence decisions are closed by
the [M3 design-gate record](project/m3-design-gate-evidence.md).

**Objective:** express deterministic multi-step control flow and bounded
retry/skip/rollback behavior.

**User-visible capabilities:** retry, backoff, skip, rollback classification,
sequential/conditional flow, deciders, exit mappings, start limits, and
allow-start-if-complete.

**Architecture deliverables:** typed policies, durable counters, flow
decisions, and listener/interceptor expansion without weakening checkpoint
atomicity.

**Ledger categories:** initial fault tolerance, listeners, conditional flow,
and restart controls.

**Dependencies:** durable M2 chunk and restart.

**Non-goals:** complete item catalog, advanced nested/split flow, remote
execution, or full Spring Batch parity.

**Exit evidence:** deterministic policy limits across restart, rollback and
checkpoint fixtures, persisted flow decisions, and mapped conformance rows.

The completed implementation boundary and remaining M6/M7 scope are recorded
in the [M3 exit evidence](project/m3-exit-evidence.md).

**Documentation:** fault-tolerance, flow, and failure/restart recipes.

**Release implication:** category claims remain limited to `Verified` rows.

## M4 — Operations and Local Scale

**Status:** Complete (2026-08-03)

The decision gates, dependency-ordered workstreams, architecture constraints,
and definition of done are recorded in the
[M4 kickoff gate](project/m4-kickoff-gate.md). The operator/explorer, CLI,
shutdown/recovery, telemetry, retention, local-scale, schema, manifest,
security, and evidence gates are closed by the
[M4 design-gate record](project/m4-design-gate-evidence.md), which authorizes
the dependency-ordered implementation workstreams without claiming any
implemented capability.

**Objective:** provide guarded local operation, observability, and bounded
single-host parallelism.

**User-visible capabilities:** CLI launch/inspect/stop/recover, graceful
shutdown, stale detection, local parallel steps and partitioning, structured
telemetry, and configuration diagnostics.

**Architecture deliverables:** operator/explorer application services,
resource bounds, local ownership/aggregation, and stable observability schema.

**Ledger categories:** initial operator/explorer, observability, parallel
steps, local partitioning, stop/recovery, and retention primitives.

**Dependencies:** M2 durability and M3 flow/policies where applicable.

**Non-goals:** cross-host coordination, hosted control plane, scheduler, or
unbounded concurrency.

**Exit evidence:** idempotent/guarded operator actions, shutdown and recovery
matrix, telemetry disclosure/cardinality tests, and bounded load/soak results.

The delivered boundary, measured bounded-resource evidence, and residual scope
are recorded in the [M4 exit evidence](project/m4-exit-evidence.md).

**Documentation:** CLI reference, runbooks, telemetry catalog, configuration,
and [capacity guidance](operations/capacity-and-resource-budgets.md).

**Release implication:** local operational claims only; no distributed or
project-wide readiness claim.

## Post-M4 program

The remaining milestones are accepted sequencing and capability gates. A
milestone does not authorize implementation across a proposed architecture
boundary, and it closes only when its evidence is attached to an exit record
and the ledger is updated.

## M5 — Embedded Core Production Preview

**Status:** Active (2026-08-03)

The decision gates, dependency-ordered workstreams, architecture constraints,
and definition of done are recorded in the
[M5 kickoff gate](project/m5-kickoff-gate.md), and the nine gates are closed
by the [M5 design-gate evidence](project/m5-design-gate-evidence.md). M5
stabilizes the delivered M0-M4 embedded scope; it is the first milestone that
may promote advertised embedded-kernel ledger rows to `Verified`, and only
against a named released version.

**Objective:** stabilize a correct, supportable, single-host PostgreSQL batch
kernel without freezing APIs that M6-M12 must extend.

**User-visible capabilities:**

- the complete accepted M0-M4 embedded scope;
- durable restart, bounded local concurrency, operator CLI, and telemetry;
- definitions that cannot silently drift across restart;
- clear preview support and upgrade expectations.

**Architecture deliverables:**

- approved path to `CompiledExecutionPlan` and definition fingerprinting;
- decided static/erased component boundary;
- validated staged crate extraction;
- approved context-codec and transaction-capability direction;
- no public facade decision that blocks the M6-M12 target.

**Ledger categories closed:** every M0-M4 row has a reviewed disposition;
advertised embedded-kernel rows are `Verified`. Deferred later-milestone rows
remain visible and prevent any full-parity claim.

**Dependencies:** M2-M4 completion; implementation evidence for accepted
[RFC-0001](rfcs/0001-m5-preview-and-project-wide-1-0.md),
[RFC-0003](rfcs/0003-target-workspace-boundaries.md),
[RFC-0004](rfcs/0004-compiled-execution-plan.md), and
[RFC-0007](rfcs/0007-repository-services-and-capabilities.md); and a recorded
decision on [RFC-0005](rfcs/0005-static-and-erased-components.md), satisfied on
2026-08-03 by the design gate's continued deferral. The RFC was accepted later
the same day on spike evidence; M5 is unaffected and still exits on the
ADR-0002 boxed boundary.

**Explicit non-goals:** full item/flow/integration/distributed parity,
additional Tier-1 databases, Spring metadata migration, project-wide API
stability, and enterprise/GA claims.

**Executable exit evidence:** full embedded conformance suite; PostgreSQL
crash, restore, upgrade, security, performance, soak, and resource-bound
campaigns; facade/API review; reference workload; no unresolved correctness
P0/P1.

**Documentation:** production-preview guide, limitations, support matrix,
operator/developer guides, upgrade/recovery runbooks, and M5 exit record.

**Release/compatibility implication:** a `0.x` Production Preview, with the
exact version selected during release planning. Public APIs and metadata remain
governed by pre-1.0 policy.

## M6 — Complete Item Processing and User Test Kit

**Objective:** complete the Rust-native item/chunk/stream component model,
standard local components, and application-facing test utilities.

**User-visible capabilities:**

- generic reader, processor, writer, and item-stream contracts;
- repeat/completion, retry, backoff, skip, no-rollback, and listener taxonomy;
- composites, classifiers, delegates, validators, filters, peek, aggregate,
  multi-resource, and thread-safety wrappers;
- iterator/list, CSV/delimited, fixed-width, JSON/JSONL, PostgreSQL
  cursor/paging, SQL batch, multi-resource, and object-store resource basics;
- full-job, single-step, scoped-component, restart, and failure-injection test
  utilities.

**Architecture deliverables:** the
[item-processing model](architecture/item-processing-model.md), native static
hot path, erased adapters, versioned component state, typed capabilities, and
`oxide-batch-test` boundary.

**Ledger categories closed:** item model, item stream/checkpoint, standard
components in scope, repeat/fault tolerance, listeners, and testing utilities
are fully populated and classified; shipped claims are `Verified`.

**Dependencies:** M5 boundary decisions and accepted
[RFC-0005](rfcs/0005-static-and-erased-components.md), satisfied on 2026-08-03
and recorded as
[ADR-0008](architecture/decisions/0008-item-component-contract.md).

**Explicit non-goals:** advanced nested/split/job flow, additional repository
backends, broker integrations, remote execution, or complete ledger closure.

**Executable exit evidence:** component contract suite; state migration;
typed/boxed trace equivalence; allocation evidence proving no per-item boxed
future on the typed path, and a decision with evidence on per-item item-listener
allocation, which ADR-0008 leaves open; malformed input, partial write, rollback, stop,
panic, and process-kill matrices; user test-kit examples.

**Documentation:** component reference, extension guide, restart/state guide,
test-kit tutorial, support tiers, and ledger evidence links.

**Release/compatibility implication:** pre-1.0 item APIs; category parity
claims only for closed and verified rows.

## M7 — Advanced Flow, Repeat, Scope, and Composition

**Objective:** provide Rust-native equivalents for advanced Spring Batch core
flow, scope/late binding, repeat, and composition semantics.

**User-visible capabilities:**

- nested flow, split/parallel flow, deciders, stop/fail/end transitions, and
  nested jobs;
- start limit, allow-start-if-complete, non-restartable jobs/steps, and restart
  controls;
- job/step component factories, typed parameter/context binding, scoped
  cleanup, and optional expression resolution;
- job parameter incrementer and complete registry/operator/explorer behavior;
- definition upgrade and fork workflow.

**Architecture deliverables:** general flow graph and compiled plan, typed
component factories, persisted decisions, lineage/fork model, and complete
operator service boundary.

**Ledger categories closed:** advanced job/step/flow, repeat/completion, scope,
late binding, registry, launcher/operator/explorer, and nested-job semantics.

**Dependencies:** M6 component model and accepted
[RFC-0004](rfcs/0004-compiled-execution-plan.md).

**Explicit non-goals:** database portability, broker adapters, remote
execution, or Java/Spring source compatibility.

**Executable exit evidence:** invalid graph tests, split/nested/stop/restart
matrix, definition upgrade/fork crash tests, scoped lifecycle tests,
differential reference scenarios, and replayable persisted decisions.

**Documentation:** execution-plan, flow, scope-equivalent, operator, and
definition-evolution guides plus ledger evidence.

**Release/compatibility implication:** may claim verified core semantic
categories, not complete Spring Batch parity.

## M8 — Repository Portability and Capability Model

**Objective:** move beyond a PostgreSQL-only durable deployment while
preserving explicit, adapter-specific semantics and fast paths.

**User-visible capabilities:**

- Tier 1 PostgreSQL, MySQL/MariaDB, SQLite, and SQL Server adapters;
- evaluated Tier 2 Oracle, DB2, and HANA adapters/certification;
- cursor/paging/keyset readers, batch/upsert writers, stored procedures;
- same-resource enlistment, outbox/inbox, effect journal, idempotency, and
  unknown-commit recovery;
- metadata archive, purge, export, and retention.

**Architecture deliverables:** separated repository/explorer/operator ports,
capability negotiation, dialect/migration SPI, adapter certification kit, and
transaction/delivery descriptors.

**Ledger categories closed:** metadata repositories, relational databases,
schema/migration/locking/ID behavior, database readers/writers, retention, and
transaction capabilities.

**Dependencies:** M7 plan requirements and accepted
[RFC-0007](rfcs/0007-repository-services-and-capabilities.md).

**Explicit non-goals:** universal distributed transactions, identical behavior
where databases differ, unverified MongoDB metadata, or degrading PostgreSQL to
the lowest common denominator.

**Executable exit evidence:** duplicate launch, optimistic conflict,
concurrent restart, stale recovery, all-version migration, backup/restore,
query-plan/index, same-resource atomicity, cross-resource delivery, and
capability-rejection suites for every supported adapter.

**Documentation:** adapter capability tables, certification reports,
migration/rollback/retention guides, and database-specific limitations.

**Release/compatibility implication:** adapters receive independent support
tiers; no database is “supported” without certification evidence.

## M9 — Messaging and Streaming Integrations

**Objective:** support messaging and bounded streaming resources as
first-class item sources/sinks with truthful delivery semantics.

**User-visible capabilities:**

- common bounded message envelope, offsets/checkpoints, acknowledgements, dead
  letters, and poison-message policy;
- Kafka, AMQP/RabbitMQ, NATS JetStream, Pulsar, SQS, Redis Streams, and generic
  channel/queue adapters according to approved support tiers;
- object storage, HTTP pagination/streaming, webhook/effect writers;
- broker transactions, outbox/inbox bridges, idempotency, and schema/version
  metadata.

**Architecture deliverables:** [integration model](architecture/integration-model.md),
capability negotiation, adapter-specific delivery descriptors, effect journal,
and strict separation between item integrations and worker transports.

**Ledger categories closed:** messaging readers/writers, offsets/acknowledgement,
object storage, HTTP, delivery/redelivery, and supported integration modules.

**Dependencies:** M8 transaction capabilities and certified repository support.

**Explicit non-goals:** pretending all brokers provide identical semantics,
general unbounded stream processing, distributed step execution, or
transport-authoritative correctness.

**Executable exit evidence:** restart, duplicate, redelivery, rebalance,
commit-ambiguity, schema evolution, backpressure, and in-flight budget
matrices for each supported broker/mode.

**Documentation:** per-adapter guarantee tables, checkpoint/ack diagrams,
idempotency/outbox recipes, support tiers, and operational runbooks.

**Release/compatibility implication:** messaging claims name broker/version and
delivery mode; no blanket exactly-once statement.

## M10 — Local Scalability and High-Performance Execution

**Objective:** deliver bounded, evidence-backed high performance on one host
without weakening canonical semantics.

**User-visible capabilities:**

- parallel steps, multi-threaded processing, local chunking, and dynamic local
  partitioning;
- declarative memory, CPU, connection, queue, broker-credit, blocking-thread,
  and deadline budgets;
- deterministic scheduling/commit traces, bounded prefetch, graceful drain;
- optional adaptive chunking and Arrow/Parquet columnar paths when approved by
  evidence.

**Architecture deliverables:** structured task tree, cancellation propagation,
ordered commit barrier, resource-budget planner, static fast paths, spill
policy, and canonical deterministic fallback.

**Ledger categories closed:** all local scalability forms, thread-safety
wrappers, local resource behavior, performance/resource evidence, and
observability of bounded concurrency.

**Dependencies:** M6 item model, M7 plan, M8 repositories, and M9 integration
backpressure.

**Explicit non-goals:** cross-host execution, optimization that changes
restart/ordering semantics, unbounded work stealing, or unsupported benchmark
marketing.

**Executable exit evidence:** 1/10/100 worker scaling, cancellation latency,
memory ceiling, queue pressure, boxed/static comparison, database round trips,
large-history queries, soak/leak, crash/restart under load, and deterministic
fallback equivalence.

**Documentation:** performance methodology and results, capacity formulas,
resource tuning, component thread-safety, and limitations.

**Release/compatibility implication:** performance claims name workload,
hardware, versions, durability, and variance; semantics remain release
blocking.

## M11 — Distributed Execution

**Objective:** execute the same compiled plan across hosts with embedded/local
equivalent lifecycle and restart behavior.

**User-visible capabilities:**

- remote step, remote partitioning, and remote chunking;
- worker registration and capability negotiation;
- durable assignments, leases, fencing, heartbeats, retry/redelivery,
  reassignment, and result aggregation;
- transport adapters for approved Kafka, NATS, AMQP, and direct gRPC paths;
- coordinator recovery/HA, rolling protocol upgrade, artifact distribution,
  cancellation, and graceful drain.

**Architecture deliverables:** versioned transport-neutral worker protocol,
coordinator/worker state machine, durable ownership, artifact verification,
bounded queues/credits, and security boundaries.

**Ledger categories closed:** remote step/chunk/partition, worker protocol,
fabric-independent restart, distributed operator semantics, and supported
transport profiles.

**Dependencies:** M7 compiled plans, M8 repository fencing/capabilities, M9
transports, M10 resource model, and accepted
[RFC-0009](rfcs/0009-transport-neutral-worker-protocol.md).

**Explicit non-goals:** making the broker authoritative, generic consensus as
a public abstraction, scheduler/UI/RBAC, or silent semantic differences
between local and remote modes.

**Executable exit evidence:** kill, partition, delayed/duplicate message,
stale worker commit, coordinator/worker restart, repository failover,
split-brain, artifact mismatch, rolling N/N-1 protocol, trace equivalence,
scale-out, and resource-limit campaigns.

**Documentation:** protocol reference, security/deployment guide, transport
profiles, failure/recovery runbook, compatibility policy, and chaos evidence.

**Release/compatibility implication:** distributed APIs/protocol remain
pre-1.0 until M14; claims name the certified topology and transport.

## M12 — Spring Batch Ledger Closure and Migration

**Objective:** give every documented Spring Batch 6.x ledger row a complete
reviewed disposition and provide evidence-backed migration tooling.

**User-visible capabilities:**

- Java reference harness and normalized differential comparison;
- versioned neutral job IR and Java-side definition extractor;
- mapping report, Rust porting stubs, compatibility profiles, and difference
  ledger;
- one-way parameter, status, exit, context, and metadata export/import for
  explicitly supported versions.

**Architecture deliverables:** frozen ledger population procedure, evidence
versioning, migration package schema, fingerprint mapping, import lineage, and
dry-run/reconciliation workflow.

**Ledger categories closed:** every category. `Unknown`, `Deferred`,
`Untested`, and unexplained `Partial` rows are zero. Rust-irrelevant Spring
constructs have reviewed equivalents or not-applicable rationales.

**Dependencies:** M6-M11 capabilities, accepted
[RFC-0002](rfcs/0002-full-spring-batch-feature-ledger-parity.md), and accepted
[RFC-0010](rfcs/0010-metadata-and-spring-migration.md).

**Explicit non-goals:** arbitrary Java source/bytecode translation,
bidirectional live metadata replication, unchanged Spring configuration, or
shared-schema operation.

**Executable exit evidence:** reference/differential suite for every claimed
behavior, migration fixtures for all supported source versions, corrupted and
partial-import recovery, count/context/fingerprint reconciliation, and at
least five representative migration workloads.

**Documentation:** complete ledger, differences, migration guide, mapping
catalog, tooling reference, limitations, and evidence index.

**Release/compatibility implication:** complete documented feature-parity
claims may begin only after released verification. This milestone may open,
but does not itself pass, a project-wide 1.0 release-candidate gate.

## M13 — Ecosystem, Extension SDK, and Certification

**Objective:** make the closed core usable and supportable across a certified
ecosystem while proving selected beyond-Spring differentiators.

**User-visible capabilities:**

- stable extension SDK and adapter certification kit;
- first-party and certified-third-party support tiers;
- production-grade reference workloads and cookbooks;
- deterministic replay, effect journal, savepoint/fork/lineage, and selected
  columnar/adaptive/dynamic-partition capabilities with evidence;
- optional out-of-process and WASI extension profiles where approved.

**Architecture deliverables:** extension compatibility contract, conformance
package, certification metadata, versioned trace/replay format, and
resource/security isolation profiles.

**Ledger categories closed:** ecosystem/integration support records,
extension/testing utilities, reference workloads, and performance/operational
evidence links remain release-current.

**Dependencies:** M12 ledger closure and stable M6-M11 contracts.

**Explicit non-goals:** unstable Rust dynamic ABI, uncertified compatibility
claims, optimizer behavior that changes semantics, or adopting every
differentiator before evidence exists.

**Executable exit evidence:** certification suites, reference applications,
replay/failure campaigns, benchmarks for each differentiator, extension
compatibility across supported releases, security isolation tests, and
fallback semantic equivalence.

**Documentation:** extension SDK, certification handbook, ecosystem support
tiers, reference architecture/cookbook, benchmark reports, and replay guide.

**Release/compatibility implication:** freezes the candidate ecosystem subset
that M14 may commit to support; experimental adapters remain outside 1.0.

## M14 — Project-Wide 1.0 / GA

**Objective:** make a durable support commitment for the selected core,
distributed, migration, and ecosystem surface only after complete evidence.

**User-visible capabilities:** stable public APIs, metadata, configuration,
protocols, support tiers, deprecation policy, upgrade paths, and maintained
reference workloads.

**Architecture deliverables:** final package/publication boundaries, schema and
protocol compatibility windows, release/security ownership, and documented
control-plane extraction decision.

**Ledger categories closed:** the full pinned Spring Batch population remains
closed and all 1.0 claims are `Verified` for the released version. Selected
beyond-parity capabilities have versioned evidence.

**Dependencies:** M5-M13 completion and all accepted governing RFCs/ADRs.

**Explicit non-goals:** unsupported adapters/topologies, automatic arbitrary
Java translation, shared live Spring schema, embedded scheduler/control plane,
universal exactly-once, or a general streaming engine.

**Executable exit evidence:**

- N/N-1/N-2 metadata and protocol upgrade/rollback evidence;
- long-running soak, distributed chaos, security and supply-chain campaign;
- at least five production-grade reference applications;
- at least three Tier-1 databases and two Tier-1 brokers;
- successful Spring reference-workload migration;
- complete docs, package, provenance, restore, and disaster-recovery rehearsal;
- no unresolved correctness P0/P1 and an accepted stable support window.

**Documentation:** complete tutorial/how-to/reference/explanation set,
architecture guide, API/protocol/schema references, support matrix,
deprecation/migration policies, operational runbooks, benchmark publication,
limitations, and M14 exit record.

**Release/compatibility implication:** project-wide `1.0.0`/GA is permitted
only after this gate. “Enterprise-ready,” “production-ready,” or complete
parity language remains prohibited before the evidence record is approved.
