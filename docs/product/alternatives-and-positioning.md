# Alternatives and Positioning

**State:** Accepted

OxideBatch should be chosen for its execution semantics, not because every
scheduled or durable workload needs a new framework.

## Alternatives

| Alternative | Prefer it when | OxideBatch distinction |
| --- | --- | --- |
| Plain Rust loop plus SQL | One small job, simple failure handling, no shared operational contract | OxideBatch adds standardized metadata, restart, policies, and tooling |
| Spring Batch | JVM/Spring is acceptable and its mature ecosystem/integrations are valuable | OxideBatch targets idiomatic Rust rather than Java/Spring compatibility |
| Workflow orchestrator such as Airflow | Scheduling, DAG visibility, multi-team control plane, and heterogeneous tasks dominate | OxideBatch is an embedded item/chunk execution framework, not a scheduler/UI |
| Durable workflow engine | Long-lived event-driven workflows, timers, signals, and service orchestration dominate | OxideBatch focuses on bounded batch runs, checkpoints, chunks, and metadata |
| Kubernetes CronJob or external scheduler | Triggering and process placement are the main requirement | OxideBatch can run inside the scheduled process and handle job identity/restart |
| Data processing engine | Distributed data-parallel compute and connector ecosystem dominate | OxideBatch prioritizes application-embedded transactional batch semantics |
| Database-native procedure/task | Work is fully inside one database and operational portability is unnecessary | OxideBatch coordinates typed application logic and multiple step types |

These tools can be complementary. An external scheduler may launch an
OxideBatch application; the identifying parameters must make scheduler retries
safe.

Reference boundaries:

- [Spring Batch reference](https://docs.spring.io/spring-batch/reference/)
- [Apache Airflow architecture](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/overview.html)
- [Kubernetes CronJob behavior](https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/)

## Initial differentiation

- Rust-native ownership, errors, traits, and async integration;
- explicit Spring Batch-inspired domain and conformance vocabulary;
- PostgreSQL-backed durable metadata and restart semantics;
- transaction/delivery guarantees stated per resource boundary;
- failure injection and operator recovery treated as primary behavior;
- a curated facade with infrastructure adapters kept replaceable.

## Positioning constraints

- Do not market OxideBatch as a drop-in Spring Batch replacement.
- Do not imply a built-in scheduler, distributed control plane, or UI.
- Do not use “exactly once” without naming the resources and transaction.
- Do not claim enterprise readiness before M5 exit evidence.
- Use “inspired by Spring Batch” until the relevant conformance rows are
  Verified.

## Build-versus-adopt checkpoint

Re-evaluate the project at M2 and before 1.0:

- Do target users still require an embedded Rust framework?
- Are restart and transaction requirements better met by an existing tool?
- Does maintenance cost exceed the value of a stable common contract?
- Are scope exclusions holding, or has the project become an orchestrator?
- Is compatibility evidence sufficient to justify public positioning?
