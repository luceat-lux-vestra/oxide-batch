# Capacity and Resource Budgets

**State:** Accepted

**Applies to:** the M4 embedded, single-host boundary

This document turns the accepted M4 resource bounds into operational sizing
guidance, and records the provisional budgets the
[M4 measurements](../engineering/measurements/m4/README.md) observed. The
[performance and capacity plan](../engineering/performance-plan.md) owns
measurement method; this document owns what an operator configures and expects.

Every number here is a **provisional budget**, not a release commitment. The
plan's regression gates require repeated baselines before a budget becomes
binding, and no measurement in this document was taken against a distributed
deployment, a broker, or a workload larger than the recorded scale points.

## Declared bounds

Every queue, page, buffer, worker set, and result set in the M4 boundary has a
finite configured or derived bound. Configuration that violates one of these
fails closed at construction or launch rather than degrading at runtime.

| Resource | Bound | Owner |
| --- | --- | --- |
| Split branches per node | `2..=8` | [Bounded local scale](../architecture/local-scale.md) |
| Steps per split branch | `1..=8` | Bounded local scale |
| Concurrent split branches | `1..=8` | Manifest `MaxParallelBranches` |
| Partitions per partitioned step | `1..=1024` | Manifest `PartitionCount` |
| Concurrent partition workers | `1..=64` | Manifest `PartitionBudget` |
| Partition key | `256` bytes | `MAX_PARTITION_KEY_BYTES` |
| Partition context | `4 KiB` | `MAX_PARTITION_CONTEXT_BYTES` |
| Explorer page size | `1..=500` rows | `MAX_PAGE_SIZE` |
| Explorer response | `256 KiB` | `MAX_RESPONSE_BYTES` |
| Explorer cursor | `256` bytes | `MAX_CURSOR_BYTES` |
| Purge batch | `1..=1000` candidates | `MAX_PURGE_BATCH` |
| Telemetry exporter queue | `64..=65536` records | `ExportQueueBound` |
| Retained incident events | `200` per execution | `MAX_RETAINED_EVENTS_PER_EXECUTION` |
| Metric series per family | `200` | `METRIC_CARDINALITY_BUDGET` |
| Diagnostic bundle | `4 MiB` | Bounded diagnostics |
| Shutdown deadline | `1 s..=1 h` | `ShutdownDeadline` |
| Telemetry flush deadline | `100 ms..=1 min` | `TelemetryFlushDeadline` |

## Deriving a connection pool

A bounded local step derives its required pool from its concurrency budget:

```text
required_connections = concurrent_children + 1
```

The extra connection belongs to the parent, which owns planning, per-child
result compare-and-swap, and the aggregate commit. `FlowLauncher::launch`
revalidates the running repository's `connection_capacity` against this derived
requirement **before creating a job execution**, so a pool that is one
connection short fails with `InsufficientPoolCapacity` and starts no child.
`p010_local_partition_scaling` asserts that closed failure directly.

Size the deployment pool for the widest concurrent step in the plan, plus any
operator, explorer, and retention connections the same process opens. Those
services are not accounted for by the launcher's derived requirement.

## Deriving a memory ceiling

The bounded local slice holds at most the following batch-owned state per
partitioned step:

```text
peak_child_state = concurrent_workers x (partition_context + partition_key)
peak_plan_state  = partition_count   x (partition_context + partition_key)
```

At the largest configured values (`1024` partitions of `4 KiB` each) the durable
plan for one step is approximately `4 MiB`, and `64` concurrent workers hold
approximately `256 KiB` of live child context. Add the bounded exporter queue
(`records x record size`), the retained incident buffer, and one bounded
explorer page (`256 KiB`) per concurrent inspection.

These are framework bounds only. Application readers, writers, and item buffers
are outside the M4 boundary and are budgeted by the workload.

## Provisional measured budgets

Measured on the environment recorded in each raw report: macOS on `aarch64`,
release profile, one Tokio runtime with four worker threads, in-memory
repository. Reproduce with the command in
[the measurement index](../engineering/measurements/m4/README.md).

| Observation | Value | Source |
| --- | --- | --- |
| Repository units of work per partition | `5.2`, identical at 1, 10, and 64 workers | [P-010](../engineering/measurements/m4/p-010.json) |
| Partition throughput, 64 partitions of `4 ms` await | `151/s` at 1 worker, `1099/s` at 10, `5774/s` at 64 | P-010 |
| Scaling efficiency against the one-worker baseline | `0.73` at 10 workers, `0.60` at 64 | P-010 |
| Aggregation cost after the last child | `0.33–0.41 ms` | P-010 |
| Explorer rows per page and cursor size | never above the requested size; `59` bytes | [P-012](../engineering/measurements/m4/p-012.json) |
| Purge plan and apply, `50`-candidate batches | plan `9–124 µs`, apply `94–228 µs` | [Retention](../engineering/measurements/m4/retention.json) |
| Launch latency interleaved with a purge campaign | `1.9 ms` median against a `3.0 ms` quiet baseline | Retention |
| Stop request to worker cancellation | `1 µs` | [P-014](../engineering/measurements/m4/p-014.json) |
| Stop request to durable terminal status | `137 µs` | P-014 |
| Twelve owned tasks, request to complete drain | `15 µs`, zero unjoined | P-014 |
| Telemetry export on a no-op lifecycle | `9 µs` quiet against `14 µs` exported, seven records per attempt | [Telemetry overhead](../engineering/measurements/m4/telemetry-overhead.json) |
| Resident growth across 24 shutdown/restart cycles | `112 KiB` | [P-015](../engineering/measurements/m4/p-015.json) |

Scaling efficiency falls from `0.73` at 10 workers to `0.60` at 64 because the
per-partition durable writes serialize behind the repository, not because the
worker budget is unmet: peak occupancy equalled the configured budget at every
scale point. Raising the worker budget beyond the repository's useful write
concurrency buys throughput that the metadata writes give back.

These are single-sample figures on a shared host, and they move materially
between runs: a second capture of the same commit produced `0.93` and `0.70`
for the same two points. Treat the direction — sublinear scaling that worsens
as workers grow — as the result, and the exact ratio as one observation. The
plan's regression gates require repeated baselines before any of these numbers
becomes a binding budget.

## Sizing checklist

1. Choose `concurrent_workers` from the workload's external concurrency, not
   from the host's core count: children are awaited, not spawned per core.
2. Set the repository pool to at least `concurrent_workers + 1` for the widest
   step, plus the process's operator, explorer, and retention connections.
3. Keep `partition_context` well under `4 KiB`; it is durable state written and
   read on every attempt, not a scratch buffer.
4. Size the exporter queue for the burst between flushes. Overflow drops the
   newest record and increments a counted, throttled drop observation; it never
   blocks or slows batch work.
5. Set the shutdown deadline above the longest expected in-flight commit. The
   coordinator never cancels an in-flight persistence future to meet the outer
   deadline; it reports the missed deadline instead.
6. Size explorer pages for the response bound, not for convenience: a `500`-row
   page of wide projections approaches the `256 KiB` response ceiling.

## Known limitations

- The in-memory repository is a deterministic fixture, not a deployment target.
  It clones its whole state per unit of work and per explorer query, so its
  latency grows with history and it serializes every writer behind one revision
  check. Latency figures above describe the framework's call counts and
  ordering, not a database's cost.
- Per-page cost, rows examined, index selection, and lock wait under real
  concurrency are PostgreSQL properties. The `10^6` and `10^8` history points
  of P-012 and the concurrent-purge lock-wait measurement require the
  PostgreSQL campaign and are not claimed here.
- No measurement in this document covers multi-threaded item processing, local
  chunking, remote execution, or an adaptive optimizer. Those belong to M10 and
  M11.
