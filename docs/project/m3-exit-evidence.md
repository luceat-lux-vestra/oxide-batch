# M3 Fault Tolerance and Flow Exit Evidence

**State:** Complete on merge

**Issue:** [#65](https://github.com/luceat-lux-vestra/oxide-batch/issues/65)

**Date:** 2026-08-01

This record closes the finite M3 fault-tolerance and flow milestone defined by
the [kickoff gate](m3-kickoff-gate.md) and the accepted
[design gate](m3-design-gate-evidence.md). It joins the prior contract,
runtime, PostgreSQL durability, compiled-plan, and durable-flow workstreams
with process-kill evidence at the remaining retry, skip, and flow boundaries.

M3 completion is an implementation milestone, not a release or production
readiness claim. Broad Spring Batch rows remain `Partial` where M6 or M7 owns
the remaining population, and no row becomes released `Verified` until the
release evidence required by the compatibility ledger exists.

## Closed implementation boundaries

- A terminal known chunk rollback increments `rollback_count` in the same
  repository transaction that commits the failed step lifecycle. This applies
  to both the format-1 chunk launcher and a chunk bound into a format-2 flow;
  checked count overflow fails closed.
- The PostgreSQL step lifecycle compare-and-swap writes that domain count in
  the same unit of work and requires exactly one matching row.
- `FlowLauncher` emits post-commit, value-redacted
  `flow.step_result_committed`, `flow.decision_committed`,
  `flow.completed_step_reused`, and `step.start_limit_exceeded` events through
  a non-authoritative panic-isolated sink. Durable step and decision rows remain
  the restart authority.
- Separate worker processes now stop before and after retry-reservation commit,
  during a skip callback before its accepting transaction commits, after a
  terminal step result but before its flow decision, and after a flow decision
  but before target start. A fresh process inspects the durable boundary,
  applies an audited recovery decision, and completes through a distinct
  restart attempt.

## Exit-criterion map

| M3 exit criterion | Evidence |
| --- | --- |
| Deterministic typed retry, skip, rollback, and listener behavior | [Fault contract](m3-fault-contract-evidence.md) and [runtime evidence](m3-fault-runtime-evidence.md), including bounded limits, deterministic backoff, capability-scoped no-rollback, callback ordering, redaction, and panic isolation |
| Durable policy state and counters remain restart-correct | [PostgreSQL durability evidence](m3-postgres-fault-durability-evidence.md), `terminal_known_rollback_commits_with_failed_step_lifecycle`, and `flow_launcher_persists_a_bound_chunk_terminal_rollback` |
| Retry and skip crash boundaries are executable | [`postgres_fault_crash_recovery.rs`](../../crates/oxide-batch/tests/postgres_fault_crash_recovery.rs) proves pre-reservation replay, post-reservation ordinal retention, and replay of an externally witnessed skip callback whose accepting commit did not occur |
| Legacy jobs retain canonical behavior while format-2 plans are deterministic | [Compiled-plan lowering evidence](m3-compiled-plan-evidence.md) pins format-1 bytes and eleven golden traces; the plan tests reject ambiguous or invalid graphs |
| Flow decisions, completed-step reuse, deciders, and start controls are durable | [Flow runtime evidence](m3-flow-runtime-evidence.md), [`postgres_flow.rs`](../../crates/oxide-batch/tests/postgres_flow.rs), and the repository manifest and lineage checks |
| Flow crash boundaries do not rerun committed work | [`postgres_flow_crash_recovery.rs`](../../crates/oxide-batch/tests/postgres_flow_crash_recovery.rs) proves completed-step reuse after a crash before decision append and committed-decision reuse after a crash before target start |
| Operator and telemetry contracts describe the implemented boundary | [Crash/restart runbook](../operations/crash-restart-and-recovery.md), [transaction guarantees](../operations/transaction-guarantees.md), [observability contract](../operations/observability-contract.md), and the [schema-2 migration guide](../operations/migrations/0002-fault-tolerance-and-flow.md) |
| Supported PostgreSQL and full Rust gates cover the milestone | PostgreSQL 15 and 18 repository CI runs the complete fault/flow process-kill matrix; PostgreSQL 15–18 design-gate CI retains migration, TLS, roles, restore, and newer-version rejection. The ordinary Rust, no-default-features, rustdoc, MSRV, and supply-chain gates remain required. |

## Named restart and crash scenarios

| Boundary | Durable observation after process exit | Restart observation |
| --- | --- | --- |
| Before retry reservation commit | Initial external call exists; retry, rollback, and retained-state counts remain zero | Initial call may replay and the retry budget is not silently spent |
| After retry reservation commit | Initial call exists; retry ordinal, rollback count, and retained retry key equal one | Restart inherits the reservation and cannot refill the budget |
| During skip callback before accepting commit | Callback witness exists; process-skip and no-rollback counts remain zero | Callback may replay; exactly one accepting commit records one process skip and one no-rollback count |
| After terminal step result, before flow decision | Source step is `COMPLETED`; no outgoing decision exists | Source body is not invoked again; restart appends `CompletedStepReuse` and starts the target |
| After flow decision commit, before target start | Source step and outgoing decision are durable | Source body and decision selection are not repeated; restart starts the recorded target |

These scenarios deliberately show at-least-once callback or external-component
invocation where no same-resource commit exists. They do not claim arbitrary
cross-resource exactly-once delivery.

## Compatibility disposition

M3 supplies executable evidence for `FT-RETRY-001`, `FT-BACKOFF-001`,
`FT-SKIP-001`, `FT-ROLLBACK-001`, the M3 portion of `LISTENER-ITEM-001`,
`FLOW-SEQUENCE-001`, `FLOW-DECIDER-001`, and `STEP-STARTLIMIT-001`.
`FT-BACKOFF-001` remains `Implemented`; the broader retry, skip, rollback,
listener, flow, decider, and start-control rows remain `Partial` because their
ledger notes assign additional Spring population to M6 or M7. This exit does
not promote any unreleased row to `Verified`.

## Reproduction

```console
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo check -p oxide-batch --no-default-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
cargo +1.95.0 check --workspace --all-targets --all-features --locked

cargo test -p oxide-batch --features postgres \
  --test postgres_repository -- --nocapture --test-threads=1
cargo test -p oxide-batch --features postgres \
  --test postgres_flow -- --nocapture --test-threads=1
cargo test -p oxide-batch --features postgres \
  --test postgres_crash_recovery -- --nocapture --test-threads=1
cargo test -p oxide-batch --features postgres \
  --test postgres_fault_crash_recovery -- --nocapture --test-threads=1
cargo test -p oxide-batch --features postgres \
  --test postgres_flow_crash_recovery -- --nocapture --test-threads=1
```

The PostgreSQL tests require isolated
`OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL` and
`OXIDEBATCH_POSTGRES_TEST_URL` values and otherwise print a skip reason. The
Docker-backed PostgreSQL 15–18 design gate remains CI evidence when a local
Docker-compatible daemon is unavailable.

## Residual scope

M4 owns operator applications and local-scale operations. M6 owns the complete
item/fault/listener population. M7 owns advanced and complete flow semantics,
including nested flows, split execution, and the remaining restart controls.
RFC-0005 and RFC-0009 remain proposed and are not implemented by this gate.
