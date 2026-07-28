# Test Strategy

**State:** Accepted

Correctness is demonstrated at several boundaries. Coverage percentage alone is
not an acceptance criterion.

| Layer | Purpose |
| --- | --- |
| Unit | Values, validation, classifications, counters, and pure policy logic |
| Property/state-machine | Legal transitions, retry/skip limits, identity invariants |
| Compile-fail | Public trait and type-system guarantees |
| Contract | Every repository/reader/writer implementation follows shared behavior |
| Integration | Transactions, locking, migrations, cancellation, and PostgreSQL |
| Conformance | Named Spring Batch-compatible behavioral scenarios |
| Failure injection | Crash and error at every lifecycle/commit boundary |
| Performance/soak | Throughput, memory, backpressure, leaks, and long-running stability |

## Determinism

Tests use injected clocks, deterministic IDs and backoff, bounded timeouts, and
seeded generators. Retrying a flaky test is not a fix. Tests that need real time
or scheduling races must state why and report enough evidence to reproduce a
failure.

## PostgreSQL matrix

CI runs the oldest and newest supported PostgreSQL major versions. Migration
tests start from every supported OxideBatch schema version. Repository contract
tests also exercise concurrent clients and forced disconnects.

## Failure matrix

For each chunk and tasklet lifecycle, inject failure:

- before an execution becomes started;
- before and after reading/processing/writing;
- immediately before commit;
- after business commit but before acknowledgement;
- after checkpoint commit;
- during listener callbacks;
- during stop and process termination.

Expected metadata, replay, counters, exit status, and operator action are
asserted for every point.

## Quality gates by milestone

- M1: unit, property, compile-fail, in-memory contract, documentation.
- M2: PostgreSQL contract, migration, crash/restart, transaction integration.
- M3: policy boundary and compatibility conformance.
- M4: concurrency, shutdown, telemetry, load, and soak.
- M5: full upgrade/recovery, security, performance, and release-candidate suite.
