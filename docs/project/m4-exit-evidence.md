# M4 Operations and Local Scale Exit Evidence

**State:** Complete on merge

**Issues:** [#10](https://github.com/luceat-lux-vestra/oxide-batch/issues/10)
and [#81](https://github.com/luceat-lux-vestra/oxide-batch/issues/81)

**Date:** 2026-08-03

This record closes the M4 milestone defined by the
[kickoff gate](m4-kickoff-gate.md) and authorized by the
[design gate](m4-design-gate-evidence.md). It joins the operator/explorer,
CLI, shutdown/recovery, telemetry, and bounded local-scale workstreams with the
bounded-resource, cancellation-latency, retention, telemetry-overhead, and
soak measurements the M4 section of the
[performance plan](../engineering/performance-plan.md) requires.

M4 completion is an implementation milestone, not a release or
production-readiness claim. Every named ledger row remains `Implemented` or
`Partial`; none becomes released `Verified` here. The measured budgets in this
record are provisional hypotheses under the plan's regression gates, not
release commitments.

## Closed implementation boundaries

The delivered boundary of each workstream is recorded by its own evidence:

| Workstream | Record |
| --- | --- |
| Operator, explorer, and retention services | [M4 operator services](m4-operator-services-evidence.md) |
| Guarded CLI and configuration diagnostics | [M4 operator CLI](m4-operator-cli-evidence.md) |
| Graceful shutdown and stale recovery | [M4 shutdown and recovery](m4-shutdown-recovery-evidence.md) |
| Bounded telemetry and diagnostic bundles | [M4 telemetry](m4-telemetry-evidence.md) |
| Local-scale manifest | [M4 local-scale plan](m4-local-scale-plan-evidence.md) |
| Durable partition repository and aggregation | [M4 partition repository](m4-partition-repository-evidence.md) |
| Bounded parallel-split execution | [M4 parallel-split runtime](m4-parallel-split-evidence.md) |
| Bounded local partition execution | [M4 local-partition runtime](m4-local-partition-runtime-evidence.md) |

This gate adds the measurement layer those records deferred: bounded-resource,
scaling, cancellation-latency, retention-throughput, telemetry-overhead, and
soak evidence with retained raw results, plus the operational capacity guidance
derived from it.

## Exit-criterion map

| M4 exit criterion | Evidence |
| --- | --- |
| Guarded, bounded, audited, idempotent operator and retention actions | [Operator services](m4-operator-services-evidence.md) and the shared in-memory/PostgreSQL [service contract](../../crates/oxide-batch/tests/contract/services.rs) |
| Deterministic CLI configuration, output, exit categories, and safeguards | [Operator CLI evidence](m4-operator-cli-evidence.md), [`operator_cli.rs`](../../crates/oxide-batch-cli/tests/operator_cli.rs), and the [CLI reference](../operations/operator-cli-reference.md) |
| Shutdown stops intake, joins owned children, and persists its outcome | [Shutdown and recovery evidence](m4-shutdown-recovery-evidence.md) and [`p014_cancellation_and_shutdown_latency`](../../crates/oxide-batch/tests/m4_exit_measurements.rs) |
| Stale detection and recovery use durable evidence only | [Shutdown and recovery evidence](m4-shutdown-recovery-evidence.md) and `recovery_proposal_is_visible_and_the_current_digest_guards_apply` |
| Telemetry satisfies cardinality, redaction, isolation, queue, and deadline bounds | [Telemetry evidence](m4-telemetry-evidence.md) and [`telemetry_export_overhead`](../../crates/oxide-batch/tests/m4_exit_measurements.rs) |
| Local parallel work owns and joins children, aggregates deterministically, and matches the sequential canon | [Split](m4-parallel-split-evidence.md) and [partition](m4-local-partition-runtime-evidence.md) runtime evidence plus [`p010_local_partition_scaling`](../../crates/oxide-batch/tests/m4_exit_measurements.rs) |
| Schema and manifest migrations pass from every supported prior version | [Schema-3 migration guide](../operations/migrations/0003-operations-and-local-scale.md) and the PostgreSQL 15–18 design-gate job |
| PostgreSQL 15 and 18 integration and process-kill gates pass | `postgres_crash_recovery`, `postgres_fault_crash_recovery`, `postgres_flow_crash_recovery`, `postgres_local_split_crash_recovery`, and `postgres_local_partition_crash_recovery` in repository CI |
| Bounded load, ceilings, cancellation latency, telemetry overhead, and soak results retain reproducible raw evidence | [M4 measurement evidence](../engineering/measurements/m4/README.md) |
| Public APIs and diagnostics expose no runtime, database, or sensitive types | Facade compile-fail suite and the redaction assertions in the telemetry, CLI, and service records |
| CLI, configuration, telemetry, capacity, shutdown, retention, and failure documentation is reviewed | The operations documents listed in [`docs/README.md`](../README.md), including the new [capacity and resource budgets](../operations/capacity-and-resource-budgets.md) |

## Measured bounded-resource evidence

Each measurement asserts the properties that make its numbers meaningful and
then records them. Assertions are structural — ceilings, ordering, durable
equivalence, and counted drops — never a duration against a threshold, so the
suite is deterministic on a loaded host. The raw results and their environment
are retained in [`docs/engineering/measurements/m4`](../engineering/measurements/m4/README.md).

| Measurement | Asserted property | Recorded observation |
| --- | --- | --- |
| P-010 local partitions | Peak occupancy never exceeded the worker budget; 1, 10, and 64 workers produced the same durable partition observation; a pool one connection short of the derived budget failed closed before the first worker | `5.2` repository units per partition at every scale point; `0.73` scaling efficiency at 10 workers and `0.60` at 64; `0.33–0.41 ms` aggregation |
| P-012 explorer pagination | No page exceeded its requested size or the `256`-byte cursor bound; each traversal returned every row exactly once | `59`-byte cursors and full-size pages across 1 000, 5 000, and 20 000 instances |
| Bounded retention | Every plan stayed within its batch bound; the campaign purged exactly the eligible executions; every interleaved launch still committed | plan `9–124 µs`, apply `94–228 µs` per `50`-candidate batch; interleaved launch `1.9 ms` median against a `3.0 ms` quiet baseline |
| P-014 stop, cancel, and drain | Cancellation reached a worker before the durable terminal status; the stopped attempt persisted `STOPPED`; no worker outlived its parent; the clean drain joined every child; intake closed; escalation reported every task that remained | `1 µs` request-to-cancellation, `137 µs` request-to-durable-terminal, `15 µs` to drain twelve owned tasks with zero unjoined, and an escalated drain that named all three remaining |
| Telemetry overhead | The exporter queue never exceeded its bound; every rejected record was counted as dropped; export enabled and disabled produced identical durable observations | `9 µs` against `14 µs` per no-op lifecycle attempt for seven exported records; `1 088` counted drops at a deliberately saturated `64`-record queue |
| P-015 soak | Every cycle joined every owned task, performed the same repository work, and reached the same durable observation; every restart re-ran only the partition that failed | `68` repository units per cycle held constant and `112 KiB` resident growth across 24 launch, fail, restart, and drain cycles |

## Reviewed dispositions

- **Sequential-fallback equivalence is the concurrency gate.** A concurrency
  result that changes durable observations relative to the one-worker run is
  invalid regardless of its throughput. P-010 compares the complete durable
  partition observation across all three scale points, and P-015 repeats the
  comparison across cycles.
- **Falling scaling efficiency is a real result, not a defect.** Efficiency
  drops from `0.73` at 10 workers to `0.60` at 64 because per-partition durable
  writes serialize behind the repository while peak occupancy still equals the
  configured budget. The budget is met; the metadata write path is the limit.
  Widening it is M8 and M10 work, not an M4 correction. The ratios are one
  sample on a shared host and move between runs; the sublinear direction, not
  the exact value, is the result this gate claims.
- **Peak concurrent connections are bounded by contract, not sampled.** The
  launcher revalidates the derived `children + 1` requirement against the
  repository's declared capacity before creating an execution, and a pool one
  connection short fails closed. The measurement counts units of work per run
  and asserts that closed rejection rather than instrumenting a pool.
- **Telemetry overhead is reported as a ratio with its sample.** On the no-op
  lifecycle the export path is resolvable (`9 µs` to `14 µs` for seven
  records); on the partitioned workload it is inside run-to-run variance. Both
  are recorded as measured rather than reduced to a single claim.
- **The in-memory repository is a fixture, not a deployment target.** It clones
  its whole state per unit of work and per query and serializes writers behind
  one revision check. The measurements therefore assert framework call counts,
  ordering, ceilings, and equivalence, and explicitly assign per-page cost,
  rows examined, index selection, and lock wait to the PostgreSQL campaign.

## Compatibility disposition

M4 supplies executable evidence for `REPO-EXPLORE-001`, `REPO-OPERATOR-001`,
`REPO-RETENTION-001`, the M4 slices of `LIFE-STOP-001`, `LIFE-RECOVER-001`, and
`LIFE-ABANDON-001`, `OBS-EXEC-001`, `OBS-METRICS-001`, `SCALE-PARSTEP-001`, and
`SCALE-LOCALPART-001`.

Rows whose Spring population continues into M7, M8, or M10 remain `Partial`;
rows delivered whole at this boundary remain `Implemented`. This exit promotes
no row to released `Verified`, which requires the complete release evidence
profile named by the [compatibility contract](../compatibility/spring-batch.md).

## Reproduction

```console
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo check -p oxide-batch --no-default-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
cargo +1.95.0 check --workspace --all-targets --all-features --locked

OXIDEBATCH_MEASUREMENT_DIR=docs/engineering/measurements/m4 \
  cargo test --release -p oxide-batch --test m4_exit_measurements -- --test-threads=1

cargo test -p oxide-batch --features postgres \
  --test postgres_local_split_crash_recovery -- --nocapture --test-threads=1
cargo test -p oxide-batch --features postgres \
  --test postgres_local_partition_crash_recovery -- --nocapture --test-threads=1
```

The PostgreSQL targets require isolated
`OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL` and `OXIDEBATCH_POSTGRES_TEST_URL`
values and otherwise print a skip reason; PostgreSQL 15 and 18 repository CI is
their release-blocking gate.

## Residual scope

These items are explicitly **not** claimed by this gate:

- the PostgreSQL explorer campaign over `10^6` and `10^8` executions, including
  per-page latency, rows examined, index selection, and cursor cost on
  realistic history, and purge lock wait against a concurrently running launch;
- the schema-3 least-privilege grant fixture that `REPO-RETENTION-001` names,
  which requires a released migration fixture rather than a runtime test;
- chunk-step factories inside split branches, which no accepted M4 gate
  requires;
- multi-threaded item processing, local chunking, dynamic partitioning, work
  stealing, and adaptive optimization, which belong to M10;
- remote execution, worker protocol, transport adapters, leases, and fencing,
  which remain proposed under RFC-0009;
- the RFC-0005 static hot path, which remains proposed.

M5 owns the embedded production-preview gate that converts these implemented
rows into released verification.
