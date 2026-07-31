# M3 Fault-Tolerance Runtime Evidence

**State:** Complete on merge

**Issue:** [#61](https://github.com/luceat-lux-vestra/oxide-batch/issues/61)

**Date:** 2026-07-31

This record maps the third M3 workstream's exit criteria to chunk-runtime
behavior and deterministic test evidence. It integrates the accepted
[fault-tolerance contract](../architecture/fault-tolerance.md) into chunk
execution on the current ADR-0002 boxed component boundary.

It does not claim durable retry reservation, schema-2 counters, restart
inheritance, or manifest fingerprint input. Those remain owned by issues
[#62](https://github.com/luceat-lux-vestra/oxide-batch/issues/62) and
[#63](https://github.com/luceat-lux-vestra/oxide-batch/issues/63), so the
crash, restart, and PostgreSQL scenarios named by the
[design gate](m3-design-gate-evidence.md) are still outstanding.

## Executable behavior

A retryable fault ends the chunk attempt. The runtime rolls the open
transaction back, reserves the next ordinal through `FaultStateStore`, runs
`before_retry`, waits the deterministic backoff, and then replays the chunk. The
reader is stateful and cannot rewind in process, so inputs already read stay
buffered across the replay and only components that have not yet succeeded are
re-invoked. This preserves "at most `N + 1` component calls for one retry key"
without re-reading committed input.

An accepted skip is provisional until the chunk commits. `RollbackDisposition::Rollback`
rolls the attempt back and replays it with the failed unit excluded;
`CommitSafeSkip` keeps the open transaction and commits the skip with the
remaining successful work. Either way the skip callback runs immediately before
the accepting commit, and the counters become authoritative only in that commit.

## Exit criteria

| Exit criterion | Evidence |
| --- | --- |
| Retry limits and backoff requests are deterministic, cancellable, and bounded | `retryable_failure_succeeds_within_limit`, `retry_exhaustion_uses_initial_plus_reserved_retries` (one initial call plus exactly `retry_limit` reserved retries), `backoff_uses_injected_monotonic_time` (delays come only from the fingerprinted policy and the ordinal through an injected `BackoffSleeper`), and `stop_during_backoff_consumes_reservation_without_reinvoke`. The runtime checks stop before reservation, before waiting, while waiting, and before re-invocation; no task or timer is detached. |
| Read/process/write skip and retry/rollback counts follow the accepted commit boundary | `skip_limit_is_shared_across_phases` keeps the three phase counts distinct under one aggregate limit, `next_skip_after_limit_fails` fails the step on the skip after the limit, and `skip_count_commits_with_chunk` shows an accepted skip whose commit failed is not counted. `read_skip_requires_forward_checkpoint_proof` and `write_skip_requires_located_known_rollback` show that unproven reader progress and an unlocated write cannot be skipped. `rollback_count` records the retry reservation and the terminal known rollback only. |
| No-rollback cannot advance past an effect incompatible with the declared delivery mode | `commit_safe_skip_requires_capability` rejects the policy at `FaultRuntime` construction when the declared `ChunkDeliveryMode` cannot commit a skip atomically, and fails closed with `ChunkFailure::UnsupportedCapability` before any user work when the open transaction enlists no business transaction. `ChunkJob::new` rejects a fault runtime whose delivery mode differs from the restart contract. `commit_safe_skip_counts_a_skip_without_rolling_back` shows the skip is still counted and still increments `no_rollback_count`. |
| Stop, listener error/panic, component error/panic, and exhausted-policy matrices preserve the prior committed checkpoint | `listener_failure_rolls_back_and_redacts` prevents an uncommitted chunk from committing and reports a redacted `ItemListenerFailure`. `retry_exhaustion_runs_its_callback_once` runs `after_retry` then `on_retry_exhausted` exactly once before the step fails. `unknown_commit_is_never_retried` and `unknown_commit_category_is_never_retried_or_skipped` produce `UNKNOWN` without rollback, retry, or skip. Every terminal path leaves `committed_chunks` and the committed counts unchanged. |
| Events remain non-authoritative and disclose only reviewed bounded fields | `fault_events_are_non_authoritative_and_bounded` observes `retry.reserved`, `fault.rollback_committed`, and `retry.backoff_started` through the existing `LifecycleEventSink`, asserts the span fields are the fault phase, retry ordinal, stable category, and opaque failure ID, and asserts no retry-key digest, item value, or error text appears. Sink failure and panic stay isolated by the existing launcher boundary. |

## Named scenarios satisfied by this workstream

`FT-RETRY-001` gains `retryable_failure_succeeds_within_limit` and
`retry_exhaustion_uses_initial_plus_reserved_retries`, plus
`stale_retry_reservation_loses_cas` against the reservation contract.
`FT-BACKOFF-001` gains `backoff_uses_injected_monotonic_time` and
`stop_during_backoff_consumes_reservation_without_reinvoke`. `FT-SKIP-001`
gains `next_skip_after_limit_fails` and `skip_count_commits_with_chunk`.
`FT-ROLLBACK-001` gains `retry_rolls_back_before_reinvoke` and
`unknown_commit_is_never_retried`. `LISTENER-ITEM-001` gains
`item_error_precedes_policy_decision`,
`skip_listener_effect_commits_once_with_skip`, and
`listener_failure_rolls_back_and_redacts`.

`crash_before_reservation_replays_initial_call`,
`retry_reservation_survives_restart`, `crash_before_commit_replays_chunk`, and
the PostgreSQL atomic skip/counter scenarios need durable state and a separate
process, so they stay outstanding for issue #62.

## Deliberate decisions recorded here

- A retry replays the whole chunk attempt from its in-memory buffer rather than
  re-invoking one component under an open transaction, because the contract
  requires a known rollback before every retry and the reader cannot rewind.
- `rollback_count` increments for a retry reservation and for a terminal known
  rollback, exactly as the contract names. A rollback taken to apply a
  `RollbackDisposition::Rollback` skip is observable through
  `rolled_back_chunks` instead, because it has no separate durable
  acknowledgement until the accepting commit.
- `ReaderError` and `WriterError` now carry the framework evidence the contract
  requires from a component: forward checkpoint progress for a read skip, and a
  located, known-rolled-back output index for a write skip. Every component
  error also declares a stable `FailureCategory` and still drops its payload,
  display text, and source chain.
- `InMemoryFaultState` is process-local. It makes the reservation ordering and
  its compare-and-swap executable without a database; a restart starts from an
  empty state, which the contract permits because a restart may invoke fewer
  retries than were reserved, never more.

## Reproduction

```console
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo check -p oxide-batch --no-default-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
cargo +1.95.0 check --workspace --all-targets --all-features --locked
```

## Boundary handed to the next workstreams

Issue #62 replaces `InMemoryFaultState` with the schema-2 checksummed envelope
and its compare-and-swap reservation, enlists `FaultStateStore::clear_resolved`
in the chunk transaction, and persists the per-phase retry, skip,
`rollback_count`, and `no_rollback_count` totals that
`ChunkExecutionReport` reports in memory today. Issue #63 adds the retry and
skip limits, retry-state capacity, backoff values, classifier rules and
revision, rollback dispositions, and listener revisions as manifest fingerprint
input.
