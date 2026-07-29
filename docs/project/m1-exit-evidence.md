# M1 Executable Kernel Exit Evidence

**State:** Complete (2026-07-29)

**Implementation revision:**
[`0ac48c5bf0b5113776c65c589cc321ac7f0a6aa4`](https://github.com/luceat-lux-vestra/oxide-batch/commit/0ac48c5bf0b5113776c65c589cc321ac7f0a6aa4)

This record maps the M1 roadmap and issue #25 criteria to executable evidence.
The compatibility rows remain `Implemented` until released behavior can be
recorded as `Verified` under the accepted conformance strategy.

## Capability evidence

| Criterion | Executable evidence |
| --- | --- |
| First launch creates a linked instance, job execution, and step execution | `first_launch_creates_execution_graph` in `tests/repository.rs` |
| Identifying parameters select the same logical instance | `job_instance_same_identifying_parameters` in `tests/domain.rs` |
| Restart creates distinct execution identities | `restart_creates_new_execution` in `tests/lifecycle_conformance/cases.rs` |
| A completed instance rejects another launch | `completed_instance_is_rejected_before_user_work_runs` in `tests/tasklet.rs` |
| Batch and exit status remain independent | `exit_status_does_not_forge_batch_status` in `tests/lifecycle_conformance/cases.rs` |
| Inspection excludes parameter values, records, and user error payloads | `inspection_redacts_record_contents` in `tests/listeners.rs` |
| Events correlate instance, execution, step, and attempt | `telemetry_correlates_execution` in `tests/listeners.rs` |
| Success persists the final execution graph | `successful_launch_borrows_context_and_persists_final_graph` in `tests/tasklet.rs` |
| Typed user failure is redacted and persisted | `typed_tasklet_failure_is_redacted_and_persisted` in `tests/tasklet.rs` |
| Cooperative stop is deterministic and persisted | `cooperative_stop_during_async_work_is_persisted` in `tests/tasklet.rs` |
| User panic is isolated and the runtime remains usable | `tasklet_panic_is_classified_and_runtime_remains_usable` in `tests/tasklet.rs` |
| Listener nesting and reverse after-order are deterministic | `listeners_nest_and_reverse_after_order` in `tests/listeners.rs` |
| Every status pair follows the accepted transition policy | `all_status_pairs_follow_the_accepted_transition_policy` in `tests/lifecycle_property/cases.rs` |
| Repository duplicate, stale, illegal, and rollback behavior is reusable | `run_repository_contract` in `tests/contract/mod.rs` |
| Executor and PostgreSQL concrete types do not leak through the facade | `public_facade_does_not_reexport_executor_or_postgres_driver_types` in `tests/ui.rs` |
| A supported public API launches one in-memory job | `examples/first_job.rs` |

All lifecycle-sensitive runtime scenarios inject a controlled clock and
deterministic identifier source. Async stop scenarios use explicit executor
coordination, while the blocking-adapter scenario bounds its deliberate
synchronous delay. Error, parameter, and event diagnostics are checked with a
sentinel secret.

## Gate evidence

The following commands passed from a clean tree at the implementation revision
on Rust 1.97.1 for `aarch64-apple-darwin`:

```text
cargo xtask check
cargo +1.95.0 check --workspace --all-targets --all-features --locked
cargo xtask package
cargo deny check
cargo run -p oxide-batch --example first_job
```

`cargo deny check` completed with the repository's accepted duplicate-version
warnings and reported advisories, bans, licenses, and sources as passing.
`cargo xtask package` verified the 50-file facade package and completed the
publish dry-run without upload.

The [required pull-request checks](https://github.com/luceat-lux-vestra/oxide-batch/pull/36/checks)
passed for quality, Rust 1.95 MSRV, dependency review, supply-chain policy, and
the real-PostgreSQL architecture spike. They provide the primary
`x86_64-unknown-linux-gnu` evidence for the completed milestone.

## Closure record

Pull request [#36](https://github.com/luceat-lux-vestra/oxide-batch/pull/36)
merged the exit evidence as revision `0ac48c5` after all required checks passed.
All seven M1 delivery issues are closed, and this record satisfies the closure
criteria in umbrella issue
[#9](https://github.com/luceat-lux-vestra/oxide-batch/issues/9).
