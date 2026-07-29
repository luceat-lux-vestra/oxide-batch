# M2 Deterministic Chunk Runtime Evidence

**State:** Complete on merge

**Issue:** [#42](https://github.com/luceat-lux-vestra/oxide-batch/issues/42)

**Date:** 2026-07-30

This record maps the fourth M2 workstream's exit criteria to the chunk
orchestrator, lifecycle integration, and deterministic in-memory evidence. It
does not claim that PostgreSQL business writes, checkpoint, context, counters,
and optimistic version commit atomically; issue #43 owns that adapter
implementation and integration evidence.

| Exit criterion | Evidence |
| --- | --- |
| Facade-owned chunk execution | `ChunkStep<I, O>` executes `ItemReader`, `ItemProcessor`, `ItemWriter`, `ChunkCompletion`, and `ChunkTransaction` ports without exposing Tokio, SQLx, or serializer types. `ChunkJob` and `JobLauncher::launch_chunk` reuse the accepted job/step repository lifecycle. |
| Deterministic checked counters | `ChunkProgress` advances read, processed, filtered, and written counts only at their typed boundaries. `ChunkExecutionReport` publishes aggregate counts from successful commit receipts only, plus checked committed and rolled-back chunk counts. |
| Typed redacted outcomes | `ChunkFailure` distinguishes reader, processor, writer, transaction, completion, listener, count, and panic phases. Component and listener payloads are discarded. A commit with unknowable outcome returns `ChunkExecutionOutcome::Unknown`, persists job/step `UNKNOWN`, and emits `chunk.unknown`, `step.unknown`, and `job.unknown`. |
| Stop and rollback boundary | Stop before commit rolls the open transaction back and publishes no partial counts. `StoppedAfterCommit` retains the committed chunk and stops before further intake. A known commit failure rolls back; an unknown commit is never rolled back or inferred. |
| Listener nesting | Chunk before-listeners run in registration order and after-listeners in reverse order. Before failure prevents the transaction body. After failure or panic retains any committed chunk and records the superseded outcome plus every redacted listener failure. Existing job/step listeners continue to nest outside the chunk adapter. |
| Committed lifecycle events | `chunk.started`, `chunk.committed`, `chunk.rolled_back`, and `chunk.unknown` carry the existing execution correlation plus a nonzero chunk sequence. Event sink failure or panic remains non-authoritative. |
| Deterministic test matrix | `tests/chunk_runtime.rs` covers empty input, partial final chunks, filtering, reader/processor/writer errors and panics, stop and rollback, post-commit stop, late completion failure, listener order/error/panic, unknown commit, persisted lifecycle state, and correlated chunk events. |
| Tasklet compatibility | `launch_chunk` adapts chunk work through the existing tasklet lifecycle rather than duplicating job/step transitions. The complete tasklet, listener, repository, PostgreSQL, facade-leakage, and documentation suites remain unchanged. |

## Reproduction

```console
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
cargo +1.95.0 check --workspace --all-targets --all-features --locked
```

## Boundary handed to implementation

Issue #43 may implement `ChunkTransactionManager` and `ChunkTransaction` over
the adapter-owned PostgreSQL connection. It must lend the existing
`BusinessTransaction` to the writer and return `ChunkCommitReceipt` only after
business writes, checkpoint, context, counters, and optimistic step version
commit together. A known failure rolls back; a commit-response ambiguity
returns `CommitOutcomeUnknown` and preserves the runtime's `UNKNOWN` path.

Issue #44 may then load the last durable receipt and select restart/recovery
behavior without depending on the non-authoritative in-memory report.
