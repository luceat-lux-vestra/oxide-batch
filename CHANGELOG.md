# Changelog

All notable changes to OxideBatch will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Facade-owned M1 domain types for job and step names, opaque execution
  identifiers, typed identifying parameters, canonical job-instance keys,
  lifecycle metadata, counters, exit statuses, and redacted failure summaries.
- Deterministic M1 integration-test support for clocks, IDs, randomness,
  backoff, bounded waits, event diagnostics, fixture provenance, redaction
  sentinels, conformance IDs, and reusable repository contracts.
- Deterministic job and step lifecycle transitions with typed optimistic-version
  conflicts, terminal-state enforcement, separate exit-status enrichment, and
  fresh execution attempts for restart.
- Async-first single-step tasklet execution with persisted lifecycle outcomes,
  cooperative stopping, panic classification, and an explicitly bounded
  blocking adapter.
- Deterministic job and step listeners, commit-aligned lifecycle events,
  execution-attempt correlation, and value-redacted log, span, metric-label,
  and listener-failure diagnostics.
- A runnable first in-memory job, facade-boundary compile-fail tests, and M1
  executable-kernel conformance and exit evidence.
- An active M2 kickoff gate with dependency-ordered workstreams, PostgreSQL
  15–18 verification targets, and durable chunk/restart exit criteria.
- Accepted M2 definition-compatibility, physical metadata, pool/timeout, TLS,
  role, and migration-operation contracts with an executable PostgreSQL 15–18
  design-gate fixture.
- Runtime-neutral item reader, processor, writer, chunk-completion, enlisted
  business-transaction, checked chunk-count, and bounded versioned durable-state
  contracts for M2.
- An optional PostgreSQL metadata repository with facade-owned redacting
  configuration, validated Rustls transport, bounded pool/timeouts, immutable
  schema-v1 migrations, canonical instance-key hashing, database-authoritative
  launch serialization, typed optimistic conflicts, and unknown-commit
  classification.
- Deterministic chunk orchestration and PostgreSQL same-resource chunk
  transactions that atomically commit business writes, checkpoints, contexts,
  counters, and optimistic versions.
- Canonical definition manifests and SHA-256 identity, typed definition-drift
  and compatibility rejection, explicit directed step mappings, committed
  checkpoint inheritance into distinct restart attempts, and versioned
  append-only PostgreSQL recovery decisions.
- Separate-process PostgreSQL pre/post-commit crash injection, durable
  restart/conformance evidence, and executable setup, transaction-guarantee,
  crash/recovery, backup, and migration operations documentation for the M2
  exit gate.
- An active M3 kickoff gate with explicit fault-policy, persistence, listener,
  compiled-plan, flow-decision, migration, resource, and evidence boundaries
  plus dependency-ordered delivery workstreams.
- Accepted M3 contracts for typed bounded retry/skip/rollback, deterministic
  backoff, item/retry/skip listeners, acyclic conditional flow, durable
  deciders, start controls, manifest format 2, and the schema-2
  migration/restore boundary.
- Runtime-neutral M3 fault-tolerance values: bounded retry, retry-state, and
  aggregate skip limits, capped deterministic backoff with an injected
  cancellable sleeper, order-independent phase/category classification over
  extended stable failure categories, capability-scoped rollback dispositions,
  and ordered item/retry/skip listener families with panic-safe aggregation.
- Deterministic fault-tolerant chunk execution: a retryable fault rolls the
  attempt back, reserves its ordinal through a bounded `FaultStateStore`, runs
  the retry scope, waits the injected cancellable backoff, and replays the chunk
  from its buffered inputs without re-reading committed input. Read, process,
  and write skips are classified from framework evidence, counted per phase, and
  become authoritative only in the commit that accepts them; a commit-safe skip
  requires the declared delivery mode and an enlisted transaction. Item, retry,
  and skip callbacks run at their contracted boundaries, and the chunk report
  exposes per-phase retry and skip counts, rollback and no-rollback counts, and
  redacted listener failures.
- Post-decision `retry.reserved`, `retry.backoff_started`,
  `retry.backoff_cancelled`, `retry.exhausted`, `item.skipped`,
  `fault.rollback_committed`, and `fault.no_rollback_committed` lifecycle events
  carrying only fault phase, retry ordinal, backoff duration, stable category,
  and the existing opaque correlation.
- Durable PostgreSQL fault-tolerance state in immutable schema version 2:
  per-phase retry and skip counters, a no-rollback count, a bounded checksummed
  fault-state envelope holding at most 256 digest-sorted retry entries, backfilled
  step logical IDs, the extended failure-category constraint, and the
  append-only `ob_flow_decision` table the flow workstream will write.
- `PostgresFaultState`, a durable retry-reservation store whose compare-and-swap
  runs as one short metadata transaction after a known rollback and before
  backoff. It advances the phase retry count, the acknowledged rollback count,
  and the retained envelope under an optimistic version check, so a stale or
  concurrent writer loses instead of spending one ordinal twice, and a restart
  resumes the persisted ordinal instead of refilling the retry budget.
- Enlisted commit of the skips one chunk accepted: the per-phase deltas, the
  no-rollback delta, and the cleared fault state of the superseded checkpoint
  generation now commit or roll back with the business writes, checkpoint,
  context, counters, and optimistic version.
- Restart inheritance of committed fault-tolerance totals and retained retry
  state, so the shared skip limit and every retry budget span all attempts of one
  job instance.
- A schema-1 to schema-2 upgrade fixture with realistic completed,
  failed-with-active-restart, stopped, and unresolved `UNKNOWN` source history,
  byte-for-byte logical-ID backfill verification, published empty-state checksum
  verification, constraint and index verification, fail-closed bounds probes, and
  a reapplication guard.
- Immutable bounded M3 flow graphs and compiled execution plans with stable node
  and decider revisions, deterministic exit-pattern selection, start controls,
  structural validation, canonical manifest format 2, golden fingerprints, and
  fail-closed format-1/format-2 manifest reading.
- Compatibility lowering for existing one-step tasklet and chunk jobs that keeps
  their format-1 manifest bytes and fingerprint unchanged while routing terminal
  outcomes through the compiled plan, with eleven golden lifecycle, listener,
  repository-write, stop, panic, restart, and unknown-commit equivalence traces.
- Durable execution of the finite M3 flow slice with sequential and conditional
  tasklet/chunk steps, custom exit outcomes, typed panic-safe deciders,
  `Complete`/`Fail`/`Stop` terminals, instance-wide atomic start limits, and
  `allow_start_if_complete`. In-memory and PostgreSQL repositories now append
  manifest-validated transition decisions before target start, reconstruct
  logical-step history across attempts, and reuse matching completed-step and
  decider decisions during restart.
- M0 implementation-readiness plan, M0–M5 roadmap, decision records, and
  product, compatibility, architecture, engineering, security, operations, and
  release policy set.
- Dedicated MSRV and supply-chain CI checks.
- Repository `cargo xtask` commands for development checks and package
  verification.
- Changed-file pull-request labeling, bounded non-authoritative AI review,
  CodeQL workflow scanning, and owned scheduled supply-chain failure reporting.
- Protected-tag draft Release preparation with locked package verification,
  CycloneDX SBOM, SHA-256 checksums, and package provenance/SBOM attestations.

### Changed

- `ChunkTransaction::commit` takes the `ChunkFaultProgress` one chunk accepted,
  and `ChunkTransactionManager` gains `inherited_progress` with a
  no-inheritance default. `FaultStateStore` gains `bind`, with a process-local
  default, because a durable store cannot know its step execution until the
  attempt starts.
- `ChunkExecutionReport` retry, skip, and no-rollback counts are cumulative
  durable totals rather than per-attempt counts, because the aggregate skip
  limit is defined across every attempt of one job instance.
- The PostgreSQL runtime requires metadata schema version 2 and rejects
  version 3 or higher without guessing compatibility. Downgrade from schema 2
  is restore-only.
- `ChunkStep::execute` takes the `ExecutionCorrelation` for the run, because
  item, retry, and skip callbacks receive it. The repository-backed
  `JobLauncher::launch_chunk` path is unchanged.
- `ReaderError`, `ProcessorError`, `WriterError`, and `ChunkCompletionError`
  declare a stable `FailureCategory` at the adapter boundary and still drop the
  payload, display text, and source chain. `ReaderError` can prove forward
  checkpoint progress and `WriterError` can locate one known-rolled-back output,
  which the read and write skip contracts require.

### Fixed

- PostgreSQL design-gate readiness now probes the final TCP listener instead of
  mistaking the official image's socket-only initialization server for a ready
  database.

## [0.1.0-alpha.1] - 2026-07-29

### Added

- Initial project governance, repository policy, and CI foundation.
- Public `oxide-batch` facade crate metadata and pre-alpha documentation.

[Unreleased]: https://github.com/luceat-lux-vestra/oxide-batch/compare/v0.1.0-alpha.1...HEAD
[0.1.0-alpha.1]: https://github.com/luceat-lux-vestra/oxide-batch/releases/tag/v0.1.0-alpha.1
