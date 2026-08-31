# M6 `oxide-batch-test` Application Test-Kit Evidence

**State:** Implementation and cross-consumer closure complete; published in
`0.6.0` (tag `v0.6.0`, 2026-08-31); ledger `Verified` promotion is a separate
governance decision, not yet made

**Issue:** [#145](https://github.com/luceat-lux-vestra/oxide-batch/issues/145)

This record maps issue #145's required scenarios to the new
`crates/oxide-batch-test/` crate and its evidence. It implements the
[M6 Gate G](m6-design-gate-evidence.md#gate-g--oxide-batch-test-boundary)
public test-kit boundary closed as design; it does not reopen Gate G, and it
implements no CSV/JSON/PostgreSQL standard component (owned by #146-#150).

Issue #145's own exit criteria require that at least one later M6 component
issue consume the kit as its own test harness before #145 is marked done.
Issue #146 consumed the kit in its component tests, so that cross-consumer
criterion is complete and #145 is closed. The boundary and its four `TEST-*`
facilities are published in `0.6.0` with retained campaign evidence; this
record does not itself promote their ledger rows to released `Verified` —
that promotion is a separate governance decision.

## Package boundary

| Requirement | Evidence |
| --- | --- |
| Dedicated public crate, not a facade module | `crates/oxide-batch-test/` (`Cargo.toml` `name = "oxide-batch-test"`), added to the workspace `Cargo.toml` |
| `oxide-batch` does not depend on it | `crates/oxide-batch/Cargo.toml` has no `oxide-batch-test` dependency; `cargo xtask deps` boundary check passes |
| Facade does not re-export it | No `oxide-batch-test` reference in `crates/oxide-batch/src/lib.rs` |
| Consumes only `oxide-batch`'s public contracts | `crates/oxide-batch-test/Cargo.toml` depends only on `oxide-batch` (plus `tokio`/`futures-executor` as dev-dependencies for doctests/examples); no `oxide-batch-core`/`oxide-batch-repository`/`oxide-batch-plan` path dependency |
| No `SQLx`/Tokio-runtime-handle/driver type in public API | `src/postgres.rs` exposes only `oxide_batch::PostgresJobRepository`/`PostgresChunkTransactionManager` and framework-owned `PostgresConfig`; no `sqlx::` type appears in any public signature (`cargo doc` surface inspected manually; no `sqlx` dependency in `[dependencies]` at all) |
| MSRV/release cadence matches `oxide-batch` | `Cargo.toml` uses `rust-version.workspace = true`, `version.workspace = true` |
| No-placeholder-crate rule | This PR creates the crate and its first usable utilities (clock/IDs, harnesses, injection, restart, process, repository fixtures) together, per Gate G |
| Package/publication dry run | `cargo xtask package` (workspace `cargo package --list` + `cargo publish --workspace --locked --dry-run`) succeeds with `oxide-batch-test` included at candidate `0.6.0`, resolving it against the co-verified local `oxide-batch` rather than a published release |

## Public contract

| Area | Evidence |
| --- | --- |
| Deterministic clock | `ManualClock` implements the framework's own `Clock` port (`src/clock.rs`); no wall-clock read |
| Deterministic ID source | `DeterministicIds` implements the framework's own `IdGenerator` port (`src/ids.rs`); ordered, one shared sequence, explicit `IdSequenceError::Exhausted`, no random-UUID fallback |
| Scoped-component fixture (`TEST-SCOPE-001`) | `ComponentFixture` (`src/scope.rs`) hands out real `ReadContext`/`ProcessContext`/`WriteContext`/`Stream*Context` values via their own public constructors; owns a `StopSource`/`StopToken` pair for controlled cancellation |
| Single-step harness (`TEST-STEP-001`) | `TestStep` (`src/step.rs`) wraps `oxide_batch::ChunkStep` and drives it through its real `execute` path; `StandaloneTransactions`/`NoCompletion` (`src/transactions.rs`) are the standalone-execution defaults the production API already documents for this case |
| Full-job harness (`TEST-JOB-001`) | `TestJob` (`src/job.rs`), generic over any `JobRepository`, wraps `oxide_batch::ChunkJob` and drives it through the real `JobLauncher::launch_chunk`; `EmbeddedRepository` (`src/repository.rs`) is the fast isolated default backing, `PostgresFixture::transaction_manager` the durable one |
| Failure injection | `oxide_batch_test::inject::{InjectedReader, InjectedProcessor, InjectedWriter, InjectedStream, InjectedTransactions}` (`src/inject.rs`) substitute a `ComponentAction`/`StreamAction`/`PreCommitAction` once a `Trigger` fires, and otherwise delegate; every firing is recorded in an `InjectionLog` under its own `InjectionId` *before* it takes effect, so a test can prove an observed failure was the one it injected, not a genuine defect |
| Panic injection | `ComponentAction::Panic`/`StreamAction::Panic` panic through the real call, letting the framework's own `catch_unwind` boundary (`chunk_runtime.rs`) convert it to `ChunkFailure::{ReaderPanic,ProcessorPanic,WriterPanic}` — this crate never catches the panic itself. Pre-commit injection has no `Panic` variant: `ChunkTransaction::commit` has no `catch_unwind` boundary in production (it is adapter-owned infrastructure, not a panic-isolated user component), so only `PreCommitAction::Fail` is offered there |
| Cooperative-stop injection | `ComponentAction::Stop(StopSource)` calls `request_stop()` on a real `StopSource` and returns the component's own `Stopped` outcome; no wall-clock race |
| Restart harness | `oxide_batch_test::restart::{RangeReader, RangeStream, range_reader}` (`src/restart.rs`): since the production `ItemReader` contract has no resume hook of its own, a resumable reader pairs itself with an `ItemStream` (the real #161 lifecycle) that restores position from the last committed envelope on `open`. The restart itself needs no special call: calling `TestJob::launch` again with the same identifying `JobParameters` against the same repository *is* the production restart path |
| Repository fixture, embedded | `EmbeddedRepository` (`src/repository.rs`) wraps a fresh, isolated `InMemoryJobRepository`; nothing durable to clean up beyond dropping the value |
| Repository fixture, `PostgreSQL` | `PostgresFixture` (`src/postgres.rs`), behind the `postgres` feature: isolation by job name; migration via `PostgresMigrator::migrate`; cleanup through the real, adapter-neutral `RetentionService` purge path (never hand-written `DELETE`s against an internal table), satisfying the production `MIN_PURGE_AGE` floor deterministically via the fixture's own injected clock rather than a real wall-clock wait |
| Process/repository failure fixture | `oxide_batch_test::process` (`src/process.rs`): `spawn_worker_test`/`announce`/`wait_for_file`/`park_until_killed`/`kill_and_wait`/`was_sigkilled` reuse the project's separate-OS-process crash-evidence principle in bounded form, not a general subprocess framework |

## Required scenarios

| Scenario | Evidence |
| --- | --- |
| `full_job_harness_launches_with_deterministic_clock_and_id` | `tests/gate_g_scenarios.rs::full_job_harness_launches_with_deterministic_clock_and_id` |
| `single_step_and_scoped_component_harness_construct_fixture_context` | `tests/gate_g_scenarios.rs::single_step_and_scoped_component_harness_construct_fixture_context` |
| `failure_panic_and_stop_injection_are_available_to_application_tests` | `tests/gate_g_scenarios.rs::failure_panic_and_stop_injection_are_available_to_application_tests` |
| `restart_harness_resumes_from_the_last_committed_checkpoint` | `tests/restart_harness.rs::restart_harness_resumes_from_the_last_committed_checkpoint` (requires `OXIDEBATCH_POSTGRES_TEST_URL` and the `postgres` feature; enforced in CI on release-blocking PostgreSQL 15 and 18 by the `postgres-item-components` matrix job, per #172) |
| `repository_fixture_cleans_up_isolated_metadata` | `tests/postgres_fixture.rs::repository_fixture_cleans_up_isolated_metadata` (same requirement and CI enforcement, per #172) |
| `package_dry_run_succeeds_for_oxide_batch_test` | `cargo xtask package`, per this repository's own convention that shelling out to `cargo` is `xtask`'s responsibility, not a `#[test]`'s |
| Process-kill fixture mechanics | `tests/process_fixture.rs::process_fixture_kills_and_reports_sigkill` (real `SIGKILL`, verified via `ExitStatusExt::signal`) |
| Application-facing usage (doctests) | `src/clock.rs`, `src/ids.rs`, `src/scope.rs`, `src/step.rs`, `src/job.rs`, `src/repository.rs` doctests, `cargo test --doc -p oxide-batch-test --all-features` |
| Application-facing usage (examples) | `examples/full_job.rs`, `examples/single_step.rs`, `examples/injected_failure.rs`, `examples/restart.rs` (the last requires `OXIDEBATCH_POSTGRES_TEST_URL` and the `postgres` feature; all four use only published public API) |

## Reproduction

```console
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --exclude oxide-batch-xtask -- -D warnings
cargo test --workspace --all-features
cargo test --doc --workspace --all-features
cargo check -p oxide-batch --no-default-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
cargo test -p oxide-batch-test --all-features
cargo test --doc -p oxide-batch-test --all-features
cargo build --examples -p oxide-batch-test --all-features
cargo xtask package
```

`tests/postgres_fixture.rs` and `tests/restart_harness.rs` require
`OXIDEBATCH_POSTGRES_TEST_URL` set to an isolated migrated database and are
skipped otherwise when run outside CI; in CI, the `postgres-item-components`
PG15/PG18 matrix job sets this variable and runs both against a real
PostgreSQL service container, per #172. `examples/restart.rs` requires the
same variable and the `postgres` feature, and remains local-only.

## Ledger disposition

`TEST-JOB-001`, `TEST-STEP-001`, `TEST-SCOPE-001`, and `TEST-REPO-001` move
from `Planned` to `Implemented`. None promotes to `Verified` on this branch:
promotion requires a named released `oxide-batch` version, per the ledger's
own promotion rule, which this issue does not itself cut. See
[`docs/compatibility/conformance-matrix.md`](../compatibility/conformance-matrix.md).

## Scope not implemented here

Per issue #145's own scope: no CSV/JSON/PostgreSQL standard component, no
composite/decorator catalog, and no change to any accepted M6 invariant
(definition identity, manifest fingerprint, checkpoint semantics,
component-state semantics, transaction boundaries, restart selection,
`ItemStream` ordering, panic conversion, or stop semantics — this test kit
consumes those unchanged). The internal `crates/oxide-batch/tests/support/`
harness is unaffected and remains a separate thing, per
[the test strategy](../testing/strategy.md#m6-user-facing-test-kit-boundary).

## Closure

- Implementation state: **complete**.
- Later-component-consumer closure criterion (issue #145's own exit
  criterion): **satisfied by #146**, the first standard-component consumer.
- Issue closed: **yes**, independently of this release-preparation PR.
