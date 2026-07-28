# Product Vision and Scope

**State:** Proposed  
**Decision needed:** approve before M1

## Vision

OxideBatch is an idiomatic Rust framework for reliable, restartable,
observable batch workloads. It adopts the proven domain language and behavioral
expectations of Spring Batch without reproducing Java APIs or the Spring
container model.

## Target users

- Rust teams running data import, export, reconciliation, settlement, and ETL;
- platform teams that require durable execution metadata and operator controls;
- organizations migrating selected batch workloads from Java while preserving
  familiar job/step/restart concepts;
- library authors implementing reusable readers, writers, repositories, or
  operational integrations.

## Product principles

1. Correct restart behavior is more important than raw throughput.
2. Transaction and delivery guarantees must be explicit, never implied.
3. Durable state transitions must be auditable and concurrency-safe.
4. The core remains independent of a database, CLI, and telemetry vendor.
5. Rust APIs are idiomatic even when behavior is Spring Batch compatible.
6. Operational failure paths receive first-class tests and documentation.

## 1.0 scope

- job, step, job instance, job execution, and step execution domain model;
- identifying and non-identifying job parameters;
- tasklet and chunk-oriented steps;
- durable PostgreSQL metadata and execution context;
- restart, stop, abandon, and explicit recovery semantics;
- retry, skip, backoff, listeners, and conditional step flow;
- local bounded parallelism and partitioning;
- CLI operations and vendor-neutral telemetry;
- compatibility and failure-injection test suites.

## Non-goals for 1.0

- Java source, binary, annotation, XML, or Spring dependency-injection
  compatibility;
- sharing a live Spring Batch metadata schema between Java and Rust processes;
- transparent exactly-once delivery across arbitrary external systems;
- a general-purpose scheduler, workflow-as-a-service control plane, or UI;
- remote chunking, cross-host partitioning, or a distributed coordinator;
- first-party repositories for every database;
- automatic translation of arbitrary Spring Batch applications.

## Success measures

- restart correctness can be demonstrated under injected crashes;
- the compatibility matrix is backed by executable conformance scenarios;
- operators can explain every running or terminal execution from metadata;
- a new user can build, run, fail, inspect, and restart the reference job using
  only the published guide;
- upgrades preserve the documented public API and metadata guarantees.
