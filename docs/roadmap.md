# Delivery Roadmap

**State:** Accepted

**Last reviewed:** 2026-07-29

The milestones are capability gates, not calendar promises. Work may move
between milestones when an architecture spike changes the risk, but a milestone
is complete only when all of its exit criteria are demonstrated.

## M0 — Foundation

**Outcome:** implementation can begin without unresolved product, correctness,
or ownership assumptions.

M0 is executed through ten readiness workstreams:

1. identity, ownership, and repository;
2. product charter and requirements;
3. compatibility and domain contract;
4. architecture and technology;
5. developer experience and coding system;
6. verification and quality engineering;
7. CI, security, and supply chain;
8. data, operations, and observability;
9. release, support, and documentation;
10. governance, planning, and risk.

The authoritative item-by-item status is maintained in the
[project preparation master plan](project/preparation-master-plan.md). Runtime
work starts only after the [kickoff gate](project/kickoff-gate.md) is signed
off.

Scope:

- approve product vision, target users, scope, and non-goals;
- approve the Spring Batch reference baseline and compatibility vocabulary;
- specify job, step, execution, status, restart, and transaction semantics;
- approve crate boundaries, dependency direction, and technical baseline;
- define coding, testing, security, release, migration, and support policies;
- specify the first end-to-end vertical slice and its failure tests.

Exit criteria:

- all proposed M0 decisions are accepted or explicitly deferred with an owner;
- no open P0/P1 design blocker remains;
- architecture spikes have recorded evidence and conclusions;
- the M1 vertical slice has executable acceptance-test scenarios.

No production runtime implementation belongs in M0.

## M1 — Executable Kernel

**Outcome:** a user can define and run a single-process, single-step job against
an in-memory repository with deterministic lifecycle events.

Scope:

- domain types for jobs, steps, parameters, instances, and executions;
- validated status transitions and distinct batch/exit statuses;
- tasklet-style step execution;
- repository and clock/ID abstractions;
- listener lifecycle and structured diagnostic context;
- in-memory reference repository;
- initial conformance harness and examples.

Exit criteria:

- duplicate job-instance creation and illegal state transitions are rejected;
- success, failure, stop, and listener-order scenarios are deterministic;
- public APIs contain no runtime or PostgreSQL implementation leakage;
- unit, property, documentation, and compile-fail tests pass.

## M2 — Durable Chunk and Restart

**Outcome:** a chunk-oriented PostgreSQL job can survive a process failure and
restart from its last committed checkpoint without losing metadata integrity.

Scope:

- reader, processor, writer, and chunk completion contracts;
- PostgreSQL metadata repository and versioned migrations;
- transaction/checkpoint boundary implementation;
- execution-context serialization and schema/version handling;
- launch, restart, stop, and abandoned-execution recovery;
- failure injection before, during, and after commit.

Exit criteria:

- committed chunks are not replayed after restart;
- uncommitted work follows the documented delivery guarantee;
- concurrent creation of the same job instance is serialized safely;
- repository migrations are tested from every supported schema version;
- PostgreSQL integration tests run in CI.

## M3 — Fault Tolerance and Flow

**Outcome:** production jobs can express multi-step control flow and bounded
fault-tolerance policies with auditable outcomes.

Scope:

- retry, backoff, skip, and no-rollback classification;
- retry/skip counters and listener semantics;
- sequential and conditional flows, deciders, and exit-code mapping;
- start limits, allow-start-if-complete, and restart flow rules;
- composite readers/processors/writers where contracts remain clear.

Exit criteria:

- retry and skip limits are deterministic across restart;
- rollback and checkpoint behavior is tested for every policy boundary;
- flow decisions are reproducible from persisted metadata;
- a compatibility matrix maps each supported behavior to conformance tests.

## M4 — Operations and Local Scale

**Outcome:** operators can launch, inspect, stop, recover, and observe jobs, and
jobs can use bounded parallelism on one host.

Scope:

- CLI for job operation and metadata inspection;
- graceful shutdown, stale-execution detection, and explicit recovery;
- structured logs, stable metric names, and trace/span conventions;
- local parallel steps and local partitioning with bounded concurrency;
- configuration layering and secret-redaction rules;
- operational runbooks and reference deployment examples.

Exit criteria:

- all operator actions are idempotent or explicitly guarded;
- shutdown and crash-recovery tests cover every lifecycle phase;
- telemetry cardinality and sensitive-data tests pass;
- load tests demonstrate documented resource and backpressure limits.

Remote chunking and cross-host scheduling are not implied by this milestone.

## M5 — Enterprise Readiness and 1.0

**Outcome:** OxideBatch can make a stable, supportable 1.0 compatibility
commitment.

Scope:

- public API, metadata-schema, and configuration stability review;
- upgrade, rollback, backup, restore, and disaster-recovery validation;
- performance baselines, soak tests, and chaos/failure campaigns;
- software supply-chain evidence and release provenance;
- complete operator/developer guides and migration examples;
- support matrix, deprecation policy, and release candidate program.

Exit criteria:

- the supported compatibility matrix has no unexplained gaps;
- a release candidate passes conformance, security, migration, recovery,
  performance, and documentation gates;
- at least one realistic reference workload completes an upgrade and
  crash/restart exercise;
- remaining limitations are public, testable, and accepted for 1.0.

## Beyond M5

Candidates include remote partitioning, remote chunking, additional database
implementations, scheduler integrations, and metadata import tooling. They are
not part of the initial 1.0 promise unless promoted through an RFC.
