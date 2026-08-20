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

## M1 harness and fixture layout

M1 test support lives under `crates/oxide-batch/tests/support/` and provides
manual clocks, deterministic ID sequences, seeded randomness, controllable
backoff, event capture, and repository contract helpers. Shared behavior is
organized by boundary:

- `tests/contract/` for repository and component contracts;
- `tests/conformance/` for scenarios named by the compatibility matrix;
- `tests/property/` for lifecycle and identity invariants;
- `tests/ui/` for compile-fail public API guarantees;
- `tests/fixtures/<scenario-id>/` for versioned, reviewable data.

Conformance reports include the matrix row ID, executable scenario name,
reproduction seed, and insertion-ordered normalized events. Repository
implementations use test-owned adapters to run the same contract cases without
making the harness part of the production API.

Fixture directories use the stable scenario ID, contain no secrets or copied
third-party implementation material, and include provenance when derived from
an external specification. Tests must not depend on wall-clock time, random
UUID generation, or registration order that is not part of the contract.

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

M2 pre/post-commit crash evidence runs the worker as a separate OS process and
terminates through `process::exit`, so Rust transaction and pool destructors do
not supply the rollback behavior under test. A new process inspects PostgreSQL,
records the recovery decision, and resumes from the durable checkpoint.

## M6 user-facing test-kit boundary

Closed by [M6 Gate G](../project/m6-design-gate-evidence.md#gate-g--oxide-batch-test-boundary).
`oxide-batch-test` is a dedicated public crate, not a module re-exported from
the `oxide-batch` facade, because the test kit needs an application-facing
test API, deterministic clock/ID sources, failure/panic/cooperative-stop
injection, repository fixtures, and a restart harness — a dependency and
resource boundary independent of the production runtime, on the same "real
dependency boundary" rule the
[staged crate-extraction contract](../architecture/crate-extraction.md)
already applies to `oxide-batch-core`, `oxide-batch-repository`, and
`oxide-batch-plan`.

The `tests/support/` harness described above is internal test support: it is
not published, and it may depend on anything convenient for the framework's
own suite. `oxide-batch-test` is a different thing — a published package
consumed by application test code — and the two must not be conflated:

- the production `oxide-batch` facade does not re-export `oxide-batch-test`;
- the production path does not depend on it;
- it consumes `oxide-batch`'s public contracts, never a private
  implementation type, even where that would be more convenient;
- it does not leak SQLx/Tokio/database-driver concrete types in its public
  API;
- its MSRV matches the project line; in M6 it shares `oxide-batch`'s release
  line/version cadence with no independent stability promise;
- the no-placeholder-crate rule applies: `crates/oxide-batch-test/` is
  created only when it ships a first usable utility with tests, in
  [#145](https://github.com/luceat-lux-vestra/oxide-batch/issues/145), not
  reserved ahead of that.

The public test-kit target boundary carries over the same determinism,
failure-injection, and process-restart principles the internal harness
already uses: full-job harness, single-step harness, scoped-component
harness, deterministic clock, deterministic ID source, failure injection,
panic injection, cooperative-stop injection, restart harness, and repository
fixture/cleanup support.

## Quality gates by milestone

- M1: unit, property, compile-fail, in-memory contract, documentation.
- M2: PostgreSQL contract, migration, crash/restart, transaction integration.
- M3: policy boundary and compatibility conformance.
- M4: concurrency, shutdown, telemetry, load, and soak.
- M5: embedded preview upgrade/recovery, security, performance, and soak suite.
- M6-M10: complete component/flow/adapter/integration/local-scale evidence.
- M11: protocol compatibility, distributed trace equivalence, and chaos.
- M12: complete ledger differential and migration evidence.
- M13: extension certification and reference workloads.
- M14: full upgrade/recovery, security, support, and 1.0 release-candidate
  suite.
