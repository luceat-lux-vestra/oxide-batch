# M3 Fault-Tolerance and Listener Contract Evidence

**State:** Complete on merge

**Issue:** [#60](https://github.com/luceat-lux-vestra/oxide-batch/issues/60)

**Date:** 2026-07-31

This record maps the second M3 workstream's exit criteria to facade contracts
and deterministic test evidence. It does not claim that retry, skip, rollback,
backoff, or listener behavior is integrated into chunk execution, persisted in
PostgreSQL, or included in a definition manifest. Those remain owned by issues
[#61](https://github.com/luceat-lux-vestra/oxide-batch/issues/61),
[#62](https://github.com/luceat-lux-vestra/oxide-batch/issues/62), and
[#63](https://github.com/luceat-lux-vestra/oxide-batch/issues/63).

| Exit criterion | Evidence |
| --- | --- |
| Unrepresentable or rejected unbounded policies | `RetryLimit` accepts `0..=65_535`, `RetryStateLimit` requires an explicit `1..=256` bound, and `BackoffPolicy` rejects delays above 24 hours, a zero multiplier, and a ceiling below its initial delay. `SkipCounts` keeps read, process, and write totals distinct and returns typed errors instead of wrapping. `FaultPolicy::new` rejects a retry rule that `RetryLimit::NONE` can never satisfy. |
| Stable categories rather than error strings | `FaultDescriptor` carries only a phase, a `FailureSummary`, the retry ordinal, committed skip counts, the open-transaction flag, and the declared delivery mode. `FailureCategory` gains `OptimisticConflict`, `Timeout`, `UnsupportedCapability`, and `UnknownCommit`; `FailureCategory::is_policy_eligible` and `FaultPhase::is_policy_eligible` fail closed for definition, lifecycle, cancellation, serialization, invariant, capability, unknown-commit, and listener faults. |
| Deterministic, order-independent classification | `FaultRule` addresses exactly one phase and category, so no outcome depends on registration order, and duplicate pairs are rejected. An unmatched fault decides `FailAndRollback`. `FaultPolicy::decide` is a pure function of the policy, the descriptor, and framework `FaultEvidence`, so a restart reproduces it. |
| Bounded, capped backoff arithmetic | `BackoffPolicy::delay_for` derives every delay from the policy and the retry ordinal only, uses saturating arithmetic, and caps at the configured maximum. `backoff_arithmetic_is_capped` and a seeded monotonicity property cover ordinals up to 65,535. |
| Capability-scoped no-rollback | `RollbackDisposition::CommitSafeSkip` is rejected at construction for write, transaction, checkpoint, listener, and backoff phases; `FaultPolicy::validate_capabilities` fails before user work when the resource cannot commit a skip atomically; and `decide` still requires located, known-rollback, and forward-checkpoint evidence. An accepted commit-safe skip still counts a skip. |
| Executable ordering, nesting, error, and panic rules | `ItemListenerSet` runs before callbacks in registration order, stops at the first failure, and reports how many listeners entered. Matching after, error, retry-completion, and exhaustion callbacks run only those listeners in reverse order and aggregate every failure. Skip callbacks run in registration order. A panic is classified as `ListenerFailureKind::Panic` exactly like a returned error. |
| Cancellable, injected backoff | `BackoffSleeper` exchanges a `Duration` and a `StopToken` for a typed `BackoffOutcome`, so the runtime and tests inject monotonic waiting without wall-clock time, detached timers, or executor types. |
| Facade isolation | The new contracts use only facade types, `BoxFuture`, and the standard library. `FaultDescriptor` diagnostics expose no runtime, database, serializer, item, or error-payload types, and existing `trybuild` fixtures continue to reject `tokio`, `sqlx`, and `serde_json` re-exports. |

## Named scenarios satisfied by this workstream

`FT-BACKOFF-001` gains `backoff_arithmetic_is_capped`. `FT-SKIP-001` gains
`skip_limit_is_shared_across_phases` and
`write_skip_requires_located_known_rollback`. `FT-ROLLBACK-001` gains
`commit_safe_skip_requires_capability`, recorded here as
`commit_safe_skip_requires_capability_and_evidence`. `LISTENER-ITEM-001` gains
`item_listeners_nest_and_reverse_after_order`.

The remaining design-gate scenarios require durable reservation, chunk
execution, or restart, so their ledger rows stay `Planned` until the dependent
workstreams supply that evidence.

## Reproduction

```console
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo check -p oxide-batch --no-default-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
cargo +1.95.0 check --workspace --all-targets --all-features --locked
```

## Boundary handed to implementation

Issue #61 may integrate `FaultPolicy::decide`, `ItemListenerSet`, and
`BackoffSleeper` into chunk execution, and owns retry-key derivation, retry
reservation ordering, stop points, and the authoritative outcome after a
listener failure. Issue #62 owns schema version 2, the checksummed fault-state
envelope, and the compare-and-swap reservation; the schema-2 constraint also
admits the four new failure-category names, which a schema-1 database still
rejects. Issue #63 owns manifest format 2, including retry and skip limits,
retry-state capacity, backoff values, classifier rules and revision, rollback
dispositions, and listener revisions as fingerprint input.
