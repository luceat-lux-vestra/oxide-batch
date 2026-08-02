# M4 Shutdown and Stale-Recovery Implementation Evidence

**State:** Complete on merge

**Issue:** [#78](https://github.com/luceat-lux-vestra/oxide-batch/issues/78)

**Date:** 2026-08-02

This record links the implemented M4 shutdown, execution ownership, stale
classification, and recovery boundary to executable evidence. It does not
close M4 and does not claim released verification. The cross-version
PostgreSQL process-signal, process-kill, cancellation-latency, and soak matrix
remains part of the M4 exit workstream in
[#81](https://github.com/luceat-lux-vestra/oxide-batch/issues/81).

## Delivered boundary

`ShutdownCoordinator` owns a Tokio-adapter task set inside an
application-owned runtime. The library installs no process signal handler and
owns no process-global runtime. The first `ShutdownSignal` request stops
intake and propagates cooperative cancellation; a second request ends join
waiting without aborting an in-flight transaction. The coordinator reports
every unjoined task by its bounded phase, then runs persistence, independently
bounded telemetry flush, and repository-close hooks in the accepted order.

`ChunkRestartContract` now includes `InFlightPolicy`. `FinishChunk` remains the
byte-identical default and masks a newly arriving process stop only for the
already-open attempt; the real token is observed immediately after commit.
`RollbackChunk` keeps the real token visible and preserves the previous
checkpoint. Unknown commit remains the existing `UNKNOWN` path.

`JobLauncher` and `FlowLauncher` can claim a newly created execution with a
16-byte process token, observe durable operator stop requests at a bounded
`StopPollInterval`, and consume an application-owned shutdown signal. A token
comparison is exact and grants no takeover authority. The repository moves an
owned active execution to `STOPPING` when it observes a durable request.

`RecoveryProposer` reads repository server time and closed redacted metadata
through `RecoveryRepository`. Active candidates require strict inactivity
beyond `StaleThreshold` and a missing or different owner. `UNKNOWN` remains an
explicit recovery candidate. Clock skew, negative inactivity, a too-wide
monotonic observation window, or backwards server time produces no proposal.
The proposal digest covers the observed execution version and durable decision
evidence, including `updated_at`. Advancing observation-time derivatives gate
proposal creation but do not make an otherwise unchanged proposal unusable by
a later stateless CLI invocation.

`execution show` returns the current proposal when one exists. `execution
recover` regenerates that proposal, compares both version and digest, then
passes the typed proposal to `JobOperator`. `MarkFailed` for an unknown commit
requires reason `UNKNOWN_EFFECT`; otherwise the action is rejected and
audited. Recovery still permits only `FAILED` or `ABANDONED` and changes no
checkpoint or counter.

## Named evidence

| Contract boundary | Executable evidence |
| --- | --- |
| Deadline bounds, first/second request, intake rejection, phase ordering, and unjoined phase counts | `shutdown::tests` in `crates/oxide-batch/src/shutdown.rs` |
| Application-owned shutdown cancellation and durable terminal state | `application_shutdown_signal_stops_work_and_rejects_new_intake` |
| Durable operator stop polling by the owning tasklet launcher | `durable_operator_stop_is_polled_by_the_owning_launcher` |
| Flow shutdown without selecting a later graph target | `process_shutdown_stops_a_flow_without_selecting_another_target` |
| Open-chunk finish/rollback policy and fingerprint participation | `declared_in_flight_policy_commits_or_rolls_back_the_open_chunk`, `rollback_chunk_policy_changes_identity_without_changing_the_default_manifest` |
| Exact owner comparison and durable stop observation | `owner_comparison_and_stop_observation_are_atomic` |
| Stale threshold, owner mismatch, redacted digest, monotonic observation, and backwards server time | `service::recovery::tests` |
| Unknown-effect recovery reason and audited result | `a_recover_request_carries_the_failure_its_disposition_requires` |
| CLI proposal visibility, current digest guard, and applied recovery | `recovery_proposal_is_visible_and_the_current_digest_guards_apply` |
| Existing commit ambiguity and restart boundaries remain unchanged | `unknown_chunk_commit_persists_unknown_lifecycle`, M2/M3 PostgreSQL crash suites |

## Durable and compatibility consequences

No new table or column is required. Schema 3 already owns `owner_token`, stop
request fields, and append-only operator/recovery audit. Definition formats 1
and 2 retain their existing canonical bytes for `FinishChunk`; only explicitly
selecting `RollbackChunk` adds the restart-relevant manifest member and changes
the fingerprint.

The runtime-neutral domain and service ports expose no Tokio, SQLx, signal,
credential, endpoint, SQL, parameter value, context value, or checkpoint
payload. Tokio remains inside the tasklet and shutdown adapters.

## Residual evidence boundary

This implementation workstream does not claim the full PostgreSQL 15/18
signal/kill matrix, blocking cancellation latency, exporter/pool failure
matrix, or repeated-shutdown leak/soak result. Issue #81 owns those complete M4
exit measurements and the released support claim. Until that record exists,
`LIFE-STOP-001` and `LIFE-RECOVER-001` remain `Partial`, not `Verified`.

## Reproduction

```console
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo check -p oxide-batch --no-default-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
```

The PostgreSQL suites require the isolated URLs documented in the
[crash/restart runbook](../operations/crash-restart-and-recovery.md). Their M4
15/18 matrix result is intentionally recorded only by issue #81.
