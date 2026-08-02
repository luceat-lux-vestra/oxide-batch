# M4 Measurement Evidence

**State:** Active

These files are the retained raw evidence for the M4 section of the
[performance and capacity plan](../../performance-plan.md). Operational sizing
guidance derived from them lives in
[capacity and resource budgets](../../../operations/capacity-and-resource-budgets.md);
the M4 gate that consumes them is the
[M4 exit evidence](../../../project/m4-exit-evidence.md).

## Reports

| File | Workload | What it measures |
| --- | --- | --- |
| [`p-010.json`](p-010.json) | P-010 local partitions | Throughput, scaling efficiency, skew, aggregation cost, repository units, and worker/pool ceilings at 1, 10, and 64 workers |
| [`p-012.json`](p-012.json) | P-012 execution history | Keyset pagination bounds, cursor size, and per-page latency over 1 000, 5 000, and 20 000 instances |
| [`retention.json`](retention.json) | Bounded retention | Plan and apply cost per bounded batch and the latency of launches interleaved with a purge campaign |
| [`p-014.json`](p-014.json) | P-014 stop/cancel/drain | Request-to-cancellation and request-to-durable-terminal latency, clean drain cost, and reported unjoined tasks on escalation |
| [`telemetry-overhead.json`](telemetry-overhead.json) | Telemetry on/off | Export cost on P-001 and P-010, exporter queue depth, and counted drops |
| [`p-015.json`](p-015.json) | P-015 soak | Repository work, durable observation, and resident growth across 24 launch, restart, and drain cycles |

## Document shape

Every report carries the same envelope:

- `environment` — source commit, working-tree cleanliness, `rustc`, profile,
  OS/architecture, host parallelism, the pinned Tokio worker-thread count, and
  resident memory at capture;
- `points` — one object per measured scale point;
- `correctness` — the named assertions the measurement enforced while
  measuring. A report whose `correctness` entries are not all `true` is a
  failure, not a slower number;
- `notes` — the reviewed limitations that bound how the numbers may be read.

## Reproducing

```bash
OXIDEBATCH_MEASUREMENT_DIR=docs/engineering/measurements/m4 cargo test --release -p oxide-batch --test m4_exit_measurements -- --test-threads=1
```

The measurement suite is part of the ordinary workspace test run. Without
`OXIDEBATCH_MEASUREMENT_DIR` it writes to `target/m4-measurements`, so a normal
`cargo test` never rewrites the retained evidence. The committed files were
captured in the release profile; a debug run reports the same structure with
larger durations and records `"profile": "debug"`.

Durations depend on the host and on concurrent load. The assertions do not: the
suite compares structure, ordering, ceilings, and durable equivalence, never a
duration against a threshold.

## What these files do not establish

Per-page cost over `10^6` and `10^8` executions, rows examined, index
selection, and lock wait under real write concurrency are properties of the
PostgreSQL adapter. The in-memory repository used here clones its whole state
per unit of work and per query, so its latency grows with history by
construction. Those measurements belong to the PostgreSQL performance campaign
and are not claimed by this directory.
