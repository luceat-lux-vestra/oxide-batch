# Performance and Capacity Plan

**State:** Accepted

Correctness, restart, delivery, ordering, and bounded resource assertions run
before performance measurements. A faster result that changes them is invalid.

## Measurement principles

- Separate framework overhead from user logic, database, broker, and storage.
- Record source commit, profile, Rust/LLVM, OS/kernel, hardware, database or
  broker version/configuration, durability settings, dataset, features, warmup,
  repetitions, variance, and correctness result.
- Retain raw machine-readable results for material claims.
- Compare like-for-like guarantees and persistence settings.
- Treat provisional budgets as hypotheses until repeated baselines justify a
  release gate; never present invented numbers as commitments.

## Architecture budgets

The proposed native hot path:

- MUST NOT allocate or box one future per item;
- MUST NOT require one dynamic dispatch per item;
- SHOULD reuse chunk buffers and batch repository/database operations;
- MAY erase or allocate at validated step/chunk/registry boundaries;
- MUST retain an unoptimized canonical path for semantic comparison.

Every implementation report measures allocations per item and chunk, bytes
copied, buffer reuse, dynamic dispatch count, future boxing, binary size,
compile time, and throughput where the static/erased decision is involved.

## Workloads

| ID | Workload | Primary pressure |
| --- | --- | --- |
| P-001 | In-memory no-op tasklet lifecycle | fixed framework overhead |
| P-002 | Static versus erased per-item processor | allocation/dispatch/API tradeoff |
| P-003 | CSV to PostgreSQL with enlisted writer | parser, batch write, transaction, metadata |
| P-004 | PostgreSQL to Parquet/Arrow | paging, columnar buffers, publication |
| P-005 | Retry/skip-heavy processing | policy and durable counter overhead |
| P-006 | Large context/checkpoint | serialization, checksum, migration, size bounds |
| P-007 | CPU-heavy and blocking components | scheduler and isolation |
| P-008 | Latency-heavy HTTP/message enrichment | bounded concurrency/backpressure |
| P-009 | Concurrent launches/restarts | locks, uniqueness, conflicts |
| P-010 | 1/10/100 local partitions/chunks | scaling, aggregation, memory |
| P-011 | Remote chunk/partition/step | protocol, duplicate rate, scale-out |
| P-012 | 10^3/10^6/10^8 execution history | query/index, retention, explorer pagination |
| P-013 | Crash/restart after many chunks | discovery and recovery time |
| P-014 | Stop/cancel/drain under load | cancellation latency and leaked work |
| P-015 | Long-running soak | memory, task, connection, handle growth |

## Required measurements

- items/chunks per second and end-to-end duration;
- fixed job/step/chunk and per-item overhead;
- allocations, copies, peak/resident memory, spill volume;
- repository calls, round trips, metadata writes, batch sizes, latency, lock
  wait, query plan, and conflicts;
- queue depth/capacity, in-flight items/chunks, backpressure delay;
- task/thread/connection/broker-credit counts;
- cancellation request-to-intake-stop and request-to-durable-terminal latency;
- recovery discovery/resume time;
- partition/worker scaling efficiency and skew;
- telemetry overhead on/off;
- CPU utilization and executor saturation.

## Chunk and metadata overhead

Reports express overhead per item, per chunk, and per job/step transition.
Chunk/batch size experiments include transaction latency, checkpoint bytes,
metadata write count, business batch efficiency, replay size, and memory.

Metadata-write reductions are accepted only when lifecycle and recovery
observations remain identical. Explorer and retention measurements use bounded
keyset/page queries; unbounded history loads are prohibited.

## Backpressure and capacity

Memory and connection guidance is expressed as a function of:

- maximum encoded item and chunk size;
- active steps, chunks, partitions, retries, and remote assignments;
- reader prefetch, writer buffering, and external-call concurrency;
- repository/operator pools and transaction duration;
- context/checkpoint/blob limits;
- telemetry queue/export delay and protocol buffers.

Every queue, retry cache, page, buffer pool, worker assignment, and result set
has a finite configured or derived bound. A stress test proves the process
stays within the declared ceiling and propagates backpressure.

## Cancellation and scale

Cancellation tests measure async, blocking, transaction, broker, and remote
worker phases separately. Forced loss of a worker is a crash/recovery result,
not low cancellation latency.

Local and distributed scaling reports compare 1/10/100 workers or the largest
practical bounded equivalent, record resource saturation and skew, and verify
checkpoint/ordering/duplicate semantics at every scale.

## Regression gates

- Baselines are established only after correctness assertions pass.
- PR benchmarks remain informational until variance and environment stability
  are understood.
- A release names provisional and binding budgets separately.
- Statistically meaningful regression against a binding budget requires review
  or release-blocking disposition.
- A performance improvement cannot bypass the relevant conformance, crash,
  migration, or resource-limit suite.
- `unsafe` requires a separate ADR, audit, and evidence that safe alternatives
  cannot meet an accepted budget.
