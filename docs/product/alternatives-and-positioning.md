# Alternatives and Positioning

**State:** Accepted

**Long-term direction:** Accepted by RFC-0001 and RFC-0002.

OxideBatch should be chosen for its execution semantics and Rust-native
integration, not because every scheduled or durable workload needs a new
framework.

## Alternatives

| Alternative | Prefer it when | OxideBatch boundary |
| --- | --- | --- |
| Plain Rust loop plus SQL | The job is small, failure handling is simple, and no shared operational contract is needed | OxideBatch adds durable identity, restart, policies, evidence, and tooling |
| Spring Batch | JVM/Spring is acceptable; its mature integrations, operational knowledge, or exact Java behavior matters; a ledger row is not yet Verified in OxideBatch | OxideBatch is a Rust-native alternative, not a drop-in Java replacement |
| Scheduler/orchestrator such as Airflow | Calendars, DAG visibility, cross-system scheduling, and multi-team governance dominate | A scheduler can launch OxideBatch; the engine owns item/chunk execution and restart |
| Durable workflow engine | Long-lived signals, timers, human tasks, and service orchestration dominate | OxideBatch focuses on bounded batch runs, checkpoints, chunks, and execution metadata |
| Kubernetes CronJob/external scheduler | Triggering and process placement are the primary requirements | OxideBatch runs inside the process and makes scheduler retries safe through identity/idempotency |
| Data-processing engine | Large distributed relational/stream processing and an existing connector ecosystem dominate | OxideBatch prioritizes application-embedded transactional batch semantics and explicit effects |
| Database-native procedure/task | All work is inside one database and application portability is unnecessary | OxideBatch coordinates typed application logic, multiple resources, and restart evidence |
| Hosted control plane | UI, RBAC, tenancy, fleet operations, and scheduling are the product need | The future control plane hosts core operator contracts; it does not belong in the embedded engine |

These tools are complementary. Positioning must name the layer being compared:
embedded core, distributed execution, integrations, or external control plane.

## When Spring Batch remains the better choice

Prefer Spring Batch when a team depends on unchanged Java/Spring configuration,
Spring-specific scopes or extensions, a mature connector that OxideBatch has
not certified, existing operational expertise, shared Java libraries, or an
exact behavior whose OxideBatch ledger row is not `Verified`.

Migration cost matters. A working Spring Batch system should not be rewritten
solely for a theoretical Rust performance advantage.

## When OxideBatch is credible

OxideBatch becomes credible for a workload when:

- Rust is the application/runtime environment;
- the required feature rows and adapter capabilities are `Verified`;
- restart, transaction, failure, and migration differences are acceptable;
- bounded concurrency and resource ownership are valuable;
- the user prefers compiled plans, explicit delivery modes, replayable
  evidence, or consistent local/distributed semantics.

Current releases are pre-1.0 and must be evaluated against the exact support
matrix and ledger rather than the long-term roadmap.

## Compatibility, migration, and replacement

- **Compatibility** is a per-feature semantic, behavioral, operational, or
  data claim backed by ledger evidence.
- **Migration** is a versioned conversion workflow with mapping reports and
  validation; it may require manual Rust ports.
- **Drop-in replacement** would imply unchanged APIs, configuration, runtime,
  or schemas and is not an OxideBatch goal.

Calling OxideBatch a Rust-native alternative does not imply drop-in
replacement.

## Claim ladder

| Claim | Minimum evidence |
| --- | --- |
| Spring Batch-inspired | Shared concepts only; no equivalence implied |
| Equivalent for named semantics | Released `Verified` semantic rows |
| Equivalent for named scenarios | Released `Verified` behavioral rows and normalized evidence |
| Category parity | Complete reviewed category with every claimed row `Verified` |
| Migration-compatible for X to Y | Released converter, fixtures, reconciliation, and documented manual boundaries |
| Complete documented Spring Batch 6.x feature parity | Entire pinned ledger closed with no unknown/deferred/untested gap |
| Project-wide 1.0/GA | M14 evidence and accepted support commitment |

Claims name the baseline version, OxideBatch release, category or rows,
divergences, and evidence. “100% compatible,” “enterprise-ready,”
“production-ready,” and “1.0-ready” are prohibited unless their full gates are
met.

## Differentiators

Potential differentiators, each requiring evidence, are:

- deterministic compiled execution plans and definition fingerprints;
- explicit resource-scoped delivery and transaction capabilities;
- bounded structured concurrency and declarative resource budgets;
- deterministic decision traces, crash evidence, and replay;
- one plan with equivalent embedded, local, and distributed semantics;
- capability-aware adapters rather than lowest-common-denominator guarantees;
- effect journals, savepoints, forks, and execution lineage;
- optional columnar/Arrow processing and adaptive chunking that preserve
  restart semantics.

## Positioning boundaries

- OxideBatch is not a built-in scheduler or calendar service.
- The embedded core is not a workflow-as-a-service control plane or UI.
- It is not a general streaming or distributed query engine.
- A future control plane is a separate product boundary.
- No blanket exactly-once claim is permitted.
- Spring-specific source/API/schema incompatibility remains explicit even
  after feature-ledger closure.

## Build-versus-adopt checkpoints

Re-evaluate at every major capability gate:

- Does the target user still need an embedded Rust batch framework?
- Does an established tool satisfy the required semantics at lower total cost?
- Are ledger evidence and support tiers sufficient for the workload?
- Are distributed or control-plane concerns displacing core correctness?
- Does the maintenance and certification burden remain sustainable?

Reference boundaries:

- [Spring Batch reference](https://docs.spring.io/spring-batch/reference/)
- [Apache Airflow architecture](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/overview.html)
- [Kubernetes CronJob behavior](https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/)
