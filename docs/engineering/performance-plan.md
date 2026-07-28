# Performance and Capacity Plan

**State:** Accepted

Performance work begins with reproducible workloads and correctness checks. A
faster result that changes checkpoint, ordering, or delivery behavior is invalid.

## Workloads

| ID | Workload | Primary pressure |
| --- | --- | --- |
| P-001 | In-memory tasklet lifecycle | framework fixed overhead |
| P-002 | Small items, PostgreSQL writer | transaction/metadata overhead |
| P-003 | Large serialized items/context | allocation and size limits |
| P-004 | CPU-heavy processor | scheduler and blocking isolation |
| P-005 | Latency-heavy remote enrichment | concurrency/backpressure |
| P-006 | Concurrent job launches | repository contention/uniqueness |
| P-007 | Local partitioned backfill | bounded scaling and aggregation |
| P-008 | Crash and restart after many chunks | recovery/checkpoint lookup |
| P-009 | Large execution history | query/index and operator inspection |

## Measurements

- items and chunks per second;
- job/step/chunk framework latency;
- repository calls, latency, lock wait, and conflicts;
- allocations and peak/resident memory;
- task/thread/connection counts and queue depth;
- restart discovery and resume time;
- telemetry overhead on/off;
- CPU utilization and scheduler saturation.

Every report records source commit, optimized profile, Rust/LLVM, OS/kernel,
hardware, PostgreSQL/configuration, dataset, features, warmup, repetitions, and
variance. Raw machine-readable results are retained for material claims.

## Regression policy

- Establish baselines only after workload correctness assertions pass.
- PR benchmarks are informational until variance is understood.
- A stable release defines budgets for critical workloads and requires review
  for statistically meaningful regression.
- Optimize measured bottlenecks; do not introduce unsafe code solely for a
  synthetic score without an ADR and audit.

## Capacity model

Documentation expresses memory and connections as functions of:

- chunk size and maximum encoded item size;
- active chunks/partitions;
- retry buffer and in-flight external calls;
- repository pool and operator connections;
- telemetry queue and exporter delay;
- execution-context and metadata history limits.

No internal queue or retry history may grow without a configured bound.
