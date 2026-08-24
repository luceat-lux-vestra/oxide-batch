# M6 `#144` Item-Stream/Component-State PostgreSQL CI Coverage Correction

**Issue:** #174
**PR:** #175

This is a CI/evidence coverage correction, not a feature or a production
defect fix. It does not change, strengthen, or reinterpret any semantic claim
made by #144's `ItemStream`/component-state evidence (`docs/project/m6-item-stream-evidence.md`).

## Gap found

`cargo test --workspace --all-features` (the `quality` job) discovers every
`#![cfg(feature = "postgres")]` test binary, but that job runs with no
PostgreSQL service and no `OXIDEBATCH_POSTGRES_TEST_URL` /
`OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL`. Each affected test's `runtime_url()`/
`migrator_url()` guard returns `None`, the test prints
`skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set` (or the migrator
equivalent) to stderr, and returns `Ok(())`. `cargo test` reports this as a
pass. No job in `.github/workflows/` and no `campaign-scope.json` driving the
`m5-*.yml` campaign workflows ever set the PostgreSQL env vars and ran these
two binaries against a real server:

- `crates/oxide-batch/tests/postgres_retention_component_state.rs`
- `crates/oxide-batch/tests/postgres_item_stream_crash_recovery.rs`

Both were added by #161 (M6 `#144`'s `ItemStream` open/update/close and
component-state contract implementation, including its corrective review).
`postgres_item_stream_crash_recovery.rs` is the sole evidence citation for
four rows of `docs/project/m6-item-stream-evidence.md`'s required-scenarios
table (`committed_stream_state_survives_same_attempt_process_crash`,
`committed_stream_state_resumes_on_genuine_restart`,
`process_kill_before_commit_restores_previous_stream_state`,
`process_kill_after_commit_restores_new_stream_state`), plus
`postgres_preserves_non_canonical_json_bytes_exactly`. That doc already
disclosed the gap honestly ("is skipped otherwise") in its reproduction
section; this correction closes that disclosed gap, it does not correct an
overclaim.

This gap was identified during the repository-wide false-green audit
performed while closing #172 (`docs/project/m6-172-test-kit-postgres-ci-coverage.md`,
"Repository-wide false-green audit"), which explicitly tracked it as #174 and
excluded it from PR #173's scope as a different evidence area.

## Ownership decision

Both binaries belong to the `oxide-batch` crate (not `oxide-batch-test`),
are gated behind its `postgres` feature, and self-migrate via
`PostgresMigrator::migrate` inside each `#[test]` — the same pattern already
used by every other `-p oxide-batch --features postgres` test binary this
repository wires into CI. Both also `mod crash_restore;`, the shared fixture
module already used by three other `-p oxide-batch` binaries
(`postgres_commit_phase_process_kill.rs`, `postgres_restart_after_many_chunks.rs`,
`postgres_logical_backup_restore.rs`, driven separately by the M5
crash-restore campaign's own `cargo xtask crash-restore` and
`m5-crash-restore.yml` — a different, heavier orchestration scoped to a
frozen M5 phase/report set that `#144`'s evidence does not belong to and
must not be folded into).

`postgres-item-components` is not the right owner. That job's existing
steps are all concrete `ItemReader`/`ItemWriter`/component *implementations*
(cursor, keyset paging, batch/enlisted SQL writer, flat-file, JSON/JSONL) and
the `oxide-batch-test` Gate G test-kit fixtures (#172). Neither `#144` binary
exercises a concrete component implementation; both drive
`PostgresJobRepository`/`PostgresChunkTransactionManager` directly with a
minimal in-test codec, proving framework-level durability of the
`ob_component_state` table itself — the same altitude as `postgres-repository`'s
existing steps (`postgres_repository`, `postgres_flow`, and the "process-kill
crash and restart matrix": `postgres_crash_recovery`,
`postgres_fault_crash_recovery`, `postgres_flow_crash_recovery`,
`postgres_local_partition_crash_recovery`, `postgres_local_split_crash_recovery`).
`postgres_item_stream_crash_recovery.rs` in particular is architecturally the
same shape as that matrix's existing members — a process-kill/restart
scenario proving repository-level durable-state correctness across a real
`SIGKILL` — and `postgres_retention_component_state.rs` extends
`RetentionService`/`PurgePlanRequest` purge-path coverage, itself a
repository/service-level concern with no existing dedicated PostgreSQL
retention step anywhere in CI.

`postgres-repository`'s PG15/18 matrix, PostgreSQL service container, and
`OXIDEBATCH_POSTGRES_ADMIN_TEST_URL`/`_MIGRATOR_TEST_URL`/`_TEST_URL` env vars
are reused unchanged. No new PostgreSQL job, service container, or reusable
workflow was created, and the M5 crash-restore campaign's scope document
(`tests/fixtures/crash-restore/campaign-scope.json`) was not touched — these
two binaries are `#144` evidence, not M5 campaign scope, and adding them
there would blur that campaign's frozen phase/report accounting.

## Fix

Two new steps were added to the existing `postgres-repository` job in
`.github/workflows/ci.yml`, immediately after the existing "Run process-kill
crash and restart matrix" step, reusing that job's PG15/PG18 matrix, service
container, env vars, and migration step verbatim:

```yaml
- name: Run PostgreSQL M6 #144 item-stream component-state crash/restart evidence
  run: >-
    cargo test -p oxide-batch --features postgres
    --test postgres_item_stream_crash_recovery
    -- --nocapture --test-threads=1

- name: Run PostgreSQL M6 #144 retention component-state evidence
  run: >-
    cargo test -p oxide-batch --features postgres
    --test postgres_retention_component_state
    -- --nocapture --test-threads=1
```

## Named tests inventoried

`postgres_retention_component_state.rs` has one top-level scenario:

- `purge_deletes_component_state_before_the_step_execution_it_references`

`postgres_item_stream_crash_recovery.rs` has five top-level `#[test]` items;
`stream_crash_worker` is not an independent scenario — it is the re-exec'd
killable-worker mechanism the other four spawn via
`Command::new(std::env::current_exe()).arg("--exact").arg("stream_crash_worker")`,
and it returns `Ok(())` immediately when its phase environment variable is
unset (i.e. every time the binary itself is invoked as the top-level test
run, not as a spawned child):

- `process_kill_before_commit_restores_previous_stream_state`
- `process_kill_after_commit_restores_new_stream_state`
- `restart_with_new_step_execution_id_inherits_committed_stream_state`
- `postgres_preserves_non_canonical_json_bytes_exactly`
- `stream_crash_worker` (worker mechanism, not an independent scenario)

## Isolation audit

Every scenario across both binaries uses a distinct, hardcoded job name —
`m6_retention_component_state`, `m6_stream_kill_before_commit`,
`m6_stream_kill_after_commit`, `m6_stream_restart_inherits_state`,
`m6_stream_non_canonical_bytes` — none of which collides with any job name
used by `postgres-repository`'s pre-existing test/run steps (its process-kill
matrix alone uses `postgres_durable_restart`, `postgres_chunk_conflict`,
`postgres_writer_failure`, `postgres_explicit_recovery`,
`postgres_chunk_disconnect`, `postgres_fault_*`, `postgres_m3_flow_*`,
`postgres_m4_local_*`).

Both binaries' `prepare_fixture`/`remove_job` helpers (`crash_restore` module)
delete every durable row scoped to their own `job_name` before the test body
runs, and again at the end of a successful run. The shared
`oxide_batch_business` schema and its `m5_crash_restore_output` table
(`CREATE TABLE IF NOT EXISTS`) are the same schema/table three other
`-p oxide-batch` binaries already use via the same module; every
`postgres-repository` binary that touches `oxide_batch_business` uses its own
table name inside it (`postgres_crash_recovery.rs` →
`m2_crash_output`, `postgres_fault_crash_recovery.rs` →
`m3_fault_crash_call`, `postgres_repository.rs` → `chunk_output`), so there is
no cross-binary table collision.

The `stream_crash_worker` re-exec spawns a child process of the *same test
binary*, scoped to its own job name via `PHASE_ENV`/`HANDSHAKE_ENV`, and the
parent only ever kills that specific child `Command` handle — it cannot
reach the PostgreSQL service process or any other binary's process. Every
scenario asserts on rows scoped to its own `step_execution_id`, resolved
fresh from its own job's `latest_attempt` — no scenario reads or purges any
job name other than its own, and no scenario's assertions depend on another
scenario or another binary having already run.

All seven of `postgres-repository`'s pre-existing test/run steps and both
new steps were run locally, back to back, in the job's real step order,
against one shared, freshly created PostgreSQL 18 database, with no cleanup
between binaries — see "Local verification" below for the exact transcript.
Nothing "passes only when run alone" was relied on as isolation evidence.

## Shared-database isolation with `stream_crash_worker`

`stream_crash_worker` is itself one of the five `#[test]` functions
`cargo test --test postgres_item_stream_crash_recovery` discovers and runs
by default (alongside the four real scenarios) whenever the binary executes
as the top-level `cargo test` process — it is not `#[ignore]`d. Its guard
(`std::env::var(PHASE_ENV)` unset ⇒ immediate `Ok(())`) makes that a no-op
pass rather than a sixth scenario needing separate CI enforcement; it is
enforced only in the sense that the four real scenarios each spawn and kill
a real re-exec of it as their mechanism, and CI evidence below shows all
five entries in the binary's own `test result:` summary.

## Scope statement

In scope: `.github/workflows/ci.yml` and `#144` evidence documentation only.
Out of scope, and not touched: #150 multi-resource/object-store work, #151
fault/listener work, #152 configuration ergonomics, #153 M6 exit campaign,
any production source under `crates/*/src`, any public API, the M5
crash-restore campaign's scope document or workflow, and any compatibility
ledger promotion.

## Local verification

Fresh PostgreSQL 18 (Homebrew, `postgresql@18`, port 5432), fresh database
(`oxide_batch_174_scratch`), the `postgres-repository` job's real step order
reproduced end to end — all seven pre-existing test/run steps followed
immediately by the two new steps — against one shared database, with no
cleanup between binaries:

```
cargo test -p oxide-batch --features postgres --test postgres_repository \
  migration_is_idempotent_when_migrator_fixture_is_available -- --nocapture --test-threads=1
# 1 passed; 0 failed

cargo test -p oxide-batch --features postgres --test postgres_repository shared_repository_contract_passes_on_postgres -- --nocapture --test-threads=1
# 1 passed; 0 failed
cargo test -p oxide-batch --features postgres --test postgres_repository concurrent_launch_creates_single_instance -- --nocapture --test-threads=1
# 1 passed; 0 failed
cargo test -p oxide-batch --features postgres --test postgres_repository committed_chunk_advances_checkpoint -- --nocapture --test-threads=1
# 1 passed; 0 failed
cargo test -p oxide-batch --features postgres --test postgres_repository writer_failure_rolls_back_business_and_checkpoint -- --nocapture --test-threads=1
# 1 passed; 0 failed
cargo test -p oxide-batch --features postgres --test postgres_repository optimistic_conflict_has_one_winner -- --nocapture --test-threads=1
# 1 passed; 0 failed

cargo test -p oxide-batch --features postgres --test postgres_repository shared_service_contract_passes_on_postgres -- --nocapture --test-threads=1
# 1 passed; 0 failed

cargo test -p oxide-batch --features postgres --test postgres_repository retry_reservation_is_a_durable_compare_and_swap -- --nocapture --test-threads=1
# 1 passed; 0 failed
cargo test -p oxide-batch --features postgres --test postgres_repository skips_counters_and_fault_state_commit_with_the_chunk -- --nocapture --test-threads=1
# 1 passed; 0 failed
cargo test -p oxide-batch --features postgres --test postgres_repository corrupt_fault_state_fails_before_component_work -- --nocapture --test-threads=1
# 1 passed; 0 failed

cargo test -p oxide-batch --features postgres --test postgres_flow -- --nocapture --test-threads=1
# 5 passed; 0 failed

cargo test -p oxide-batch --features postgres --test postgres_repository disconnect_during_commit_never_guesses_outcome -- --nocapture --test-threads=1
# 1 passed; 0 failed
cargo test -p oxide-batch --features postgres --test postgres_repository postgres_chunk_disconnect_is_known_not_committed_before_commit -- --nocapture --test-threads=1
# 1 passed; 0 failed
cargo test -p oxide-batch --features postgres --test postgres_repository newer_schema_is_rejected_without_guessing_compatibility -- --nocapture --test-threads=1
# 1 passed; 0 failed

cargo test -p oxide-batch --features postgres --test postgres_crash_recovery -- --nocapture --test-threads=1
# 3 passed; 0 failed
cargo test -p oxide-batch --features postgres --test postgres_fault_crash_recovery -- --nocapture --test-threads=1
# 4 passed; 0 failed
cargo test -p oxide-batch --features postgres --test postgres_flow_crash_recovery -- --nocapture --test-threads=1
# 3 passed; 0 failed
cargo test -p oxide-batch --features postgres --test postgres_local_partition_crash_recovery -- --nocapture --test-threads=1
# 2 passed; 0 failed
cargo test -p oxide-batch --features postgres --test postgres_local_split_crash_recovery -- --nocapture --test-threads=1
# 2 passed; 0 failed

cargo test -p oxide-batch --features postgres --test postgres_item_stream_crash_recovery -- --nocapture --test-threads=1
# 5 passed; 0 failed
#   postgres_preserves_non_canonical_json_bytes_exactly ... ok
#   process_kill_after_commit_restores_new_stream_state ... ok
#   process_kill_before_commit_restores_previous_stream_state ... ok
#   restart_with_new_step_execution_id_inherits_committed_stream_state ... ok
#   stream_crash_worker ... ok

cargo test -p oxide-batch --features postgres --test postgres_retention_component_state -- --nocapture --test-threads=1
# 1 passed; 0 failed
#   purge_deletes_component_state_before_the_step_execution_it_references ... ok
```

All 21 `test result:` lines report `0 failed`. Grepping the full local
transcript for `skipped:` returns zero matches — neither new binary took its
missing-environment skip path. This reproduces the job's real step order
(21 `cargo test` invocations across its nine steps — seven pre-existing test/
run steps plus the two new ones) against one shared, freshly created
PostgreSQL 18 database, with no cleanup between binaries: no state leakage
or ordering dependency was observed. PostgreSQL 15 is not installed locally
(only `postgresql@18` via Homebrew); per this repository's PG15 evidence
convention, PG15 enforcement is proven by the PR's CI matrix run, not local
reproduction — see "CI verification" below.

## CI verification

### Producer commit

The workflow change was introduced in commit
`be4b9e4814605275205def865330633358d3b4a5` (PR #175). That commit's CI run
is the evidence that the two new steps actually execute real
PostgreSQL-backed tests rather than only existing in YAML:

- `postgres-15-repository`: PASS — [run log](https://github.com/luceat-lux-vestra/oxide-batch/actions/runs/32765830927/job/97554969885). The "Run PostgreSQL M6 #144 item-stream component-state crash/restart evidence" step group shows `test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out` for `postgres_item_stream_crash_recovery`, with all five named tests present and `ok`: `postgres_preserves_non_canonical_json_bytes_exactly`, `process_kill_after_commit_restores_new_stream_state`, `process_kill_before_commit_restores_previous_stream_state`, `restart_with_new_step_execution_id_inherits_committed_stream_state`, `stream_crash_worker`. The following "Run PostgreSQL M6 #144 retention component-state evidence" step group shows `test purge_deletes_component_state_before_the_step_execution_it_references ... ok` and `test result: ok. 1 passed; 0 failed`.
- `postgres-18-repository`: PASS — [run log](https://github.com/luceat-lux-vestra/oxide-batch/actions/runs/32765830927/job/97554970001). Identical named-test results: `postgres_item_stream_crash_recovery` → 5 passed, 0 failed (same five names, all `ok`); `postgres_retention_component_state` → 1 passed, 0 failed.
- Grepping each full job log for `skipped:` returns zero matches in both — neither test binary printed `skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set` (or the migrator equivalent) in either job; both executed against the job's real PostgreSQL service container.
- All other required checks on this PR passed at this same commit: `quality`, `msrv`, `packaging`, `postgres-spike`, the full PG15/16/17/18 `postgres-*-design-gate` matrix, `postgres-15/18-item-components`, the full M5 campaign matrix (cancellation, crash-restore, performance, resource-bounds, security, soak, upgrade, conformance) on both PG15 and PG18, `dependency-review`, `supply-chain`, `evidence-provenance` (Evidence workflow), CodeQL (`Analyze`), the AI pull request review, and the PR labeler.

### Exact-head merge gate

Per this repository's exact-head convention, the PR's actual merge gate is
whatever its final head SHA is at merge time, not the producer commit
specifically — see the PR's own Checks tab / description for that result
rather than a SHA hardcoded in this file, since any further edit to this
file necessarily advances the head again.

## Production behavior

Zero changes to `crates/oxide-batch/src`, `oxide-batch-core`,
`oxide-batch-repository`, `oxide-batch-plan`, or any other production crate.
No test assertions, scenarios, or matrix legs were weakened, skipped, or
removed to reach green.

## Compatibility ledger

`ITEM-STREAM-001` and `META-CONTEXT-001` remain **Implemented**. This
correction enforces existing `#144` evidence in CI; it does not promote
either row to **Verified** — that requires a named released `oxide-batch`
version, per the ledger's own promotion rule, which this issue does not
itself cut. See
[`docs/compatibility/conformance-matrix.md`](../compatibility/conformance-matrix.md).
