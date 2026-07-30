# M2 Durable Chunk and Restart Exit Evidence

**State:** Complete on merge

**Issue:** [#45](https://github.com/luceat-lux-vestra/oxide-batch/issues/45)

**Date:** 2026-07-30

This record closes the M2 capability gate. It records implementation and
executable evidence, not a released `Verified` compatibility or
production-readiness claim.

## Exit criteria

| Criterion | Evidence |
| --- | --- |
| Deterministic crash worker | `tests/postgres_crash_recovery.rs` launches a separate copy of the test executable, commits the first two-item chunk, exits with no Rust destructor at the second chunk's pre-commit or post-commit boundary, and inspects from a fresh repository process. |
| Committed chunks are not replayed | `crash_after_commit_does_not_replay_chunk` observes checkpoint 4, four business rows, four read/process/write counts, and two commits after the worker exits immediately after the second commit. The restart performs no second-chunk write. |
| Uncommitted chunks replay from the last checkpoint | `crash_before_commit_replays_chunk` observes only checkpoint 2 and the first two business rows after process exit, audits the orphan, creates distinct restart attempts, writes items 3–4 once, and finishes at checkpoint 4. |
| Atomic business/progress transaction | `committed_chunk_advances_checkpoint` and `writer_failure_rolls_back_business_and_checkpoint` cover business rows, checkpoint, context, all counters, and step CAS on PostgreSQL. |
| Optimistic conflict | `optimistic_conflict_has_one_winner` proves one version/business-write winner and a fully rolled-back loser. |
| Commit ambiguity | `disconnect_during_commit_never_guesses_outcome`, `postgres_chunk_disconnect_is_known_not_committed_before_commit`, and `unknown_execution_requires_audited_postgres_recovery` distinguish known rollback from `UNKNOWN`, discard suspect connections, inspect through a healthy connection, and require a versioned recovery decision. |
| Definition-guarded restart | `durable_restart_requires_compatible_definition_and_inherits_checkpoint` rejects revision drift and absent edges, accepts one direct renamed-step edge, creates distinct attempts, and copies only committed state. |
| Duplicate launch | `concurrent_launch_creates_single_instance` runs eight contenders and observes one database-authoritative instance/execution. |
| Component/listener/stop boundaries | `reader_failure_preserves_checkpoint`, `processor_failure_rolls_back_chunk`, `writer_failure_rolls_back_business_and_checkpoint`, `listener_failure_preserves_committed_work`, and `stop_during_chunk_uses_commit_boundary` preserve committed-only counters and replay boundaries. |
| Durable inspection and redaction | The crash tests inspect status, attempts, versions, counters, checkpoint position, definition-compatible restart, and recovery audit while asserting the evidence digest remains redacted. Existing facade/debug tests exclude connection details, parameters, contexts, checkpoint payloads, SQL, bound values, records, and driver errors. |
| Migration and newer-version rejection | `migration_is_idempotent_when_migrator_fixture_is_available`, the immutable version-1 checksum, design-gate restore, and `newer_schema_is_rejected_without_guessing_compatibility` cover the complete M2 source matrix: uninitialized and version 1, with version 2 rejected. |
| PostgreSQL support matrix | CI runs full repository, transaction, disconnect, process-kill restart, migration, TLS, role, and restore evidence on 15 and 18. The 15–18 design gate runs validated TLS, role, migration, repository, and vertical-slice smoke coverage on every explicit major. |

## Conformance slice

| Acceptance ID | Executable evidence |
| --- | --- |
| `CHUNK-COMMIT-001` | `committed_chunk_advances_checkpoint`, `crash_after_commit_does_not_replay_chunk` |
| `CHUNK-ROLLBACK-001` | `crash_before_commit_replays_chunk` |
| `RESTART-001` | `crash_before_commit_replays_chunk`, `crash_after_commit_does_not_replay_chunk` |
| durable `JOB-EXEC-001` | both process-kill tests assert distinct original/restart job and step execution IDs |
| `JOB-CONCURRENCY-001` | `concurrent_launch_creates_single_instance` |
| durable `OBS-INSPECT-001` | both process-kill tests inspect durable lifecycle, counters, checkpoint, and recovery audit with payload redaction |

Rows remain `Implemented`, rather than released `Verified`, until a named
OxideBatch release satisfies the compatibility contract's complete evidence
profile.

## Operational material

- [PostgreSQL setup](../operations/postgres-setup.md)
- [Migration 0001](../operations/migrations/0001-initial-metadata.md)
- [Transaction guarantees](../operations/transaction-guarantees.md)
- [Crash/restart/recovery runbook](../operations/crash-restart-and-recovery.md)
- [Persistence, backup, restore, and migration policy](../operations/persistence-and-migrations.md)

## Reproduction

```console
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo check -p oxide-batch --no-default-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
cargo +1.95.0 check --workspace --all-targets --all-features --locked
./tests/fixtures/postgres/run-design-gate.sh 15
./tests/fixtures/postgres/run-design-gate.sh 18
```

The Docker-backed PostgreSQL commands are CI-required when no compatible local
daemon is available.

## Residual limits

- M2 provides the correctness-bearing recovery repository operation, not the
  M4/M7 operator service or CLI.
- Recovery authentication, authorization, and full evidence storage belong to
  the deployment; metadata keeps only an opaque operator reference and digest.
- M2 direct definition upgrades preserve checkpoint/context bytes and do not
  transform state schemas.
- Non-enlisted and external resources cannot use the same-resource atomicity
  evidence or claim generic exactly-once delivery.
- M2 does not include retry/skip breadth, conditional flow, automatic orphan
  takeover, distributed execution, retention/purge, or project-wide
  production readiness.
