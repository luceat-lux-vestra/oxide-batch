# M2 PostgreSQL Atomic Chunk Transaction Evidence

**State:** Complete on merge

**Issue:** [#43](https://github.com/luceat-lux-vestra/oxide-batch/issues/43)

**Date:** 2026-07-30

This record maps the fifth M2 workstream's exit criteria to the PostgreSQL
same-resource transaction path. It does not claim durable restart selection or
operator recovery; issue #44 owns those capabilities.

| Exit criterion | Evidence |
| --- | --- |
| Borrowed facade transaction | `PostgresChunkTransactionManager` overrides the launched `begin_for` path and lends writers only `BusinessTransaction`. SQLx connections, queries, rows, and errors remain private. Unbound standalone use is rejected rather than guessing an execution target. |
| Atomic business and progress commit | One adapter-owned connection encloses parameter-bound business writes and a step CAS update covering checkpoint, execution context, read/process/write/filter/commit counters, injected-clock update time, and optimistic version. `postgres_chunk_commit_and_rollback_are_atomic` commits two chunks and observes matching business rows, cumulative counters, and the advancing checkpoint. |
| Known rollback boundary | Writer failure rolls the open PostgreSQL transaction back. The integration case observes no business rows, no durable counters, and the prior checkpoint. State-provider failure or panic also remains pre-commit and rollback-eligible. |
| Post-commit acknowledgement | Completion failure occurs after the database commit. The integration case observes a failed enclosing execution while the committed business rows, checkpoint, context, and counters remain authoritative and therefore are not eligible for replay. |
| Optimistic conflict | Two transactions read the same step version. Exactly one CAS and its business write commit; the loser returns `NotCommitted` and its business write rolls back. |
| Unknown commit classification | Repository and chunk commits share the same `COMMIT` helper: any failed acknowledgement discards the connection and returns the public unknown-outcome classification. A disconnect before `COMMIT` is known not committed because no commit command was sent; the forced-disconnect case observes rollback through durable state. |
| Delivery boundary | The PostgreSQL manager always supplies an enlisted transaction. Managers that supply no transaction retain `WriteContext::non_transactional` and the documented at-least-once boundary; they cannot use this evidence to claim same-resource atomicity. |
| PostgreSQL support matrix | The PostgreSQL 15/18 CI jobs run atomic commit/rollback, CAS conflict, forced disconnect, migration, repository, TLS, and least-privilege fixtures. The disposable application table lives in a separate fixture schema and is not part of the OxideBatch metadata migration. |
| Durable inspection and redaction | `load_committed_state` reads checkpoint/context and the step snapshot through a healthy pool connection. Public diagnostics redact payloads, SQL, values, connection details, and driver errors. |

## Reproduction

```console
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
cargo +1.95.0 check --workspace --all-targets --all-features --locked
./tests/fixtures/postgres/run-design-gate.sh 15
./tests/fixtures/postgres/run-design-gate.sh 18
```

The two container commands require a running Docker-compatible daemon. The
repository CI matrix is the release-blocking execution environment when one is
not available locally.

## Boundary handed to implementation

Issue #44 may use `PostgresDurableStepState` to select the latest committed
checkpoint/context through a healthy connection, create a distinct compatible
restart attempt, and refuse automatic replay for active, orphaned, or
`UNKNOWN` executions. Recovery must remain audited and cannot reinterpret a
failed commit response from process memory.
