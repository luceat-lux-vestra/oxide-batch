# M4 Bounded Telemetry and Diagnostic-Bundle Evidence

**State:** Implemented (unreleased)

**Issue:** [#79](https://github.com/luceat-lux-vestra/oxide-batch/issues/79)

**Date:** 2026-08-02

This record implements the accepted M4 telemetry schema, bounded exporter and
metric behavior, and incident diagnostic bundle. It does not close M4, does
not make telemetry authoritative, and does not move either observability row
to released `Verified` status.

## Implemented boundary

- Telemetry schema version `1` publishes one closed event catalog with stable
  names, severity, component, and commit/read/evidence/runtime-relative timing.
  Existing lifecycle and flow events map explicitly into this catalog.
- `JobOperator`, `JobExplorer`, `RecoveryProposer`, `RetentionService`, and
  `ShutdownCoordinator` accept panic-isolated sinks. Operator and recovery
  application events are emitted only after their audit/effect commit returns;
  explorer pages emit only after a successful bounded read.
- Metric families publish names, units, and complete label-key sets. Framework
  values are typed; job and step names require allowlists of at most `50`; each
  family retains at most `200` distinct combinations and maps overflow to
  `__other__` with a counted cardinality drop.
- The schema-version-1 span catalog fixes the job, step, chunk, item,
  repository-commit, retry, and backoff hierarchy; complete reviewed field
  sets and adapter-neutral outcome classes exclude payload-bearing values.
- The exporter queue is `64..=65536` records, default `1024`, and drops the
  newest record without applying execution backpressure. Drop reports are
  throttled to `1 s..=1 h`, default `60 s`. Export adapter errors and panics are
  counted and isolated. The library spawns no exporter task; applications own
  the drain task and join it through their runtime shutdown tree.
- The process-local incident buffer is bounded to `200` returned events per
  execution and `4096` total records by default. It is diagnostic and may lose
  or duplicate observations across a crash.
- `diagnostics bundle --execution ... --out ...` stages files and writes a new
  directory while refusing overwrite. Deterministic files contain effective
  redacted configuration, schema/capability status, explorer projections,
  retained events, and a bounded host summary. `manifest.json` records file
  checksums and omissions. Total encoded size is at most `4 MiB`.

No OpenTelemetry SDK, exporter, transport, database driver, credential, or
runtime type crosses the facade telemetry API. `TelemetryExportSink` is the
optional adapter boundary; no OpenTelemetry dependency is enabled by default.

## Named evidence

| Scenario | Executable evidence |
| --- | --- |
| Catalog and schema | [`m4_events_match_the_published_catalog_and_schema_version`](../../crates/oxide-batch/tests/telemetry.rs) |
| Span hierarchy and safe fields | [`m4_spans_match_the_published_hierarchy_and_safe_fields`](../../crates/oxide-batch/tests/telemetry.rs) |
| Commit-relative operator/recovery events | [`operator_and_recovery_events_follow_their_durable_commit`](../../crates/oxide-batch/tests/telemetry.rs) |
| Bundle redaction | [`diagnostic_bundle_excludes_every_prohibited_value_class`](../../crates/oxide-batch-cli/tests/operator_cli.rs) |
| Bundle size and omissions | [`diagnostic_bundle_respects_its_size_bound_and_records_omissions`](../../crates/oxide-batch-cli/src/bundle.rs) |
| Per-family cardinality | [`metric_labels_stay_within_the_family_cardinality_budget`](../../crates/oxide-batch/tests/telemetry.rs) |
| Name allowlist | [`unallowlisted_names_map_to_other`](../../crates/oxide-batch/tests/telemetry.rs) |
| Drop-newest exporter queue | [`full_exporter_queue_drops_newest_and_counts`](../../crates/oxide-batch/tests/telemetry.rs) |
| Export failure isolation | [`exporter_failure_cannot_change_execution_state`](../../crates/oxide-batch/tests/telemetry.rs) |
| Independent flush deadline | [`telemetry_flush_deadline_is_separate_from_shutdown`](../../crates/oxide-batch/tests/telemetry.rs) |

On 2026-08-02, the repository, flow, and three separate-process crash/recovery
suites ran serially against local Homebrew PostgreSQL 18.4 on a freshly
migrated isolated schema-3 database: all `31` tests passed. This is local
integration evidence only; it does not replace the PostgreSQL 15/18 CI matrix,
least-privilege, TLS, upgrade, or restore gates.

## Compatibility and residual limits

- `OBS-EXEC-001` and `OBS-METRICS-001` are unreleased `Implemented`, not
  `Verified`. Repository counters and lifecycle records remain authoritative
  after a lost, duplicate, or reordered event.
- The M4 catalog includes split and partition event names accepted by the
  design gate, but issue #80 owns their runtime emission and durable local-scale
  evidence.
- This issue supplies deterministic bounded-resource and failure-isolation
  evidence plus one local PostgreSQL 18.4 integration run. Comparative exporter
  overhead, long soak/leak evidence, and the release PostgreSQL matrix remain
  issue #81 exit work.
- Hosted collectors, dashboards, alerting, trace propagation across remote
  workers, and RFC-0009 transport semantics remain outside M4.
