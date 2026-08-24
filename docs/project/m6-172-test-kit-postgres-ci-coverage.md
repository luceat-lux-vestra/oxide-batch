# M6 Gate G Test-Kit PostgreSQL CI Coverage Correction

**Issue:** #172
**PR:** #173

This is a CI/evidence coverage correction, not a feature or a production defect fix. It does not change, strengthen, or reinterpret any semantic claim made by #145's Gate G test-kit evidence.

## Gap found

`cargo test --workspace --all-features` (the `quality` job) discovers every `#![cfg(feature = "postgres")]` test binary, but that job runs with no PostgreSQL service and no `OXIDEBATCH_POSTGRES_TEST_URL`. Each affected test's `runtime_url()` guard returns `None`, the test prints `skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set` to stderr, and returns `Ok(())`. `cargo test` reports this as a pass. No job in `.github/workflows/` ever set the PostgreSQL env vars and ran these two binaries against a real server:

- `crates/oxide-batch-test/tests/restart_harness.rs::restart_harness_resumes_from_the_last_committed_checkpoint`
- `crates/oxide-batch-test/tests/postgres_fixture.rs::repository_fixture_cleans_up_isolated_metadata`

Both were added by #162. `repository_fixture_cleans_up_isolated_metadata` is also the sole evidence citation for the `TEST-REPO-001` conformance-matrix row (`Implemented`, M6/M8), so this false-green directly undermined that row's evidence. `docs/project/m6-test-kit-evidence.md` already disclosed honestly that both were "verified against a local PostgreSQL 18 instance" and "skipped otherwise" in CI — this PR closes that disclosed gap; it is not correcting an overclaim.

This gap, and its scope carve-out from #170, were identified in #170's own corrective-evidence record (`docs/project/m6-170-postgres-restart-ci-coverage.md`, "Also found, out of scope here").

## Ownership decision

Both binaries belong to the `oxide-batch-test` crate, gated behind its `postgres` feature, and self-migrate via `PostgresFixture::migrate` on each `#[tokio::test]` — the same pattern already used by every other `oxide-batch-test` postgres-gated test binary (`postgres_flat_file_restart`, `postgres_json_restart`, `postgres_item_components_restart`, `postgres_item_components_db_restart`).

`postgres-repository`'s PG15/18 matrix, PostgreSQL service container, and `OXIDEBATCH_POSTGRES_TEST_URL`/`_ADMIN_TEST_URL`/`_MIGRATOR_TEST_URL` env vars are technically sufficient to run these two binaries — nothing about that job makes it incapable of it. But that job has never run `-p oxide-batch-test` at all; it exclusively drives `-p oxide-batch`'s own `postgres_repository`/`postgres_flow`/crash-recovery test binaries. Adding these two there would split `oxide-batch-test`'s real-PostgreSQL evidence across two jobs for no functional reason.

`postgres-item-components` is the job that already owns every real-PostgreSQL `oxide-batch-test` binary, including the three added by #170's own fix for this exact class of gap. Reusing it — rather than `postgres-repository` — keeps all of `oxide-batch-test`'s PostgreSQL evidence under one job and matches the precedent #170 itself set when it added the sibling `oxide-batch-test` restart binaries here. Its PG15/18 matrix, service container, env vars, and migration step are reused unchanged. No new PostgreSQL job, service container, or reusable workflow was created.

## Fix

Two new steps were added to the existing `postgres-item-components` job in `.github/workflows/ci.yml`, immediately after the existing `postgres_item_components_restart` (#170) step, reusing that job's PG15/PG18 matrix, service container, env vars, and migration step verbatim:

```yaml
- name: Run PostgreSQL Gate G restart harness (#172)
  run: >-
    cargo test -p oxide-batch-test --features postgres
    --test restart_harness
    -- --nocapture --test-threads=1

- name: Run PostgreSQL Gate G repository fixture cleanup (#172)
  run: >-
    cargo test -p oxide-batch-test --features postgres
    --test postgres_fixture
    -- --nocapture --test-threads=1
```

## Isolation audit

### `restart_harness`

- `job_name` is `oxide_batch_test_restart_harness_{nonce}`, where `nonce` is a `SystemTime::now()` nanosecond duration since `UNIX_EPOCH` — unique per run, never collides with another test binary's durable rows in the shared `oxide_batch_item_components` database.
- Attempt A's injected pre-commit failure on chunk 3 genuinely rolls back at the real `ChunkTransaction::commit` boundary; the durable committed count after attempt A is asserted to be exactly 4 (two committed chunks), never 6 (the discarded in-memory candidate).
- Attempt B is a second `TestJob::launch` call against the *same* `ChunkJob`/`job_name`/repository — the real production restart path, not a manual shortcut — and is asserted to inherit read ordinal 4 (the last committed count) exactly once, and a real (non-zero-sentinel) checkpoint digest.
- The test does not depend on any other test binary's state, and nothing about its own state depends on execution order — it never reads or purges any job name other than its own nonce-suffixed one.

### `postgres_fixture`

- `job_name` is `oxide_batch_test_fixture_cleanup_{nonce}`, independently nonce-suffixed the same way — isolated from every other job name in the shared database, including `restart_harness`'s.
- Cleanup goes through `PostgresFixture::purge_job`, which is `oxide-batch-test`'s thin wrapper around the production `RetentionService` purge path (`PurgePlanRequest::new(job_name, ...)`), not a hand-written `DELETE`. The purge plan is scoped to the exact `job_name` passed in, so it can only ever delete rows for this test's own nonce-suffixed job — it cannot touch another binary's or another attempt's durable rows.
- `MIN_PURGE_AGE` is satisfied deterministically via the fixture's own `ManualClock::advance(Duration::from_mins(61))`, not a real wall-clock wait, so the test is not flaky under CI scheduling variance.
- The assertions (`job_executions() >= 1`, `job_instances() >= 1`) prove the purge actually removed this job's own durable rows, not merely that the call returned `Ok`.

Both binaries were run locally immediately after the `postgres-item-components` job's nine pre-existing steps, in the job's actual step order, against one shared, freshly created PostgreSQL 18 database mirroring one live service container across a whole job run, with no cleanup between binaries. See "Local verification" below for the exact command transcript and results.

## Repository-wide false-green audit

Beyond the two binaries in scope here, the repository was searched for every `#![cfg(feature = "postgres")]` test binary and cross-referenced against every `--test <name>` invocation in `.github/workflows/ci.yml` and every `target` entry in the M5 campaign `tests/fixtures/*/campaign-scope.json` manifests driving `.github/workflows/m5-*.yml`.

All `oxide-batch`-crate and `oxide-batch-test`-crate postgres-gated binaries have a real CI owner (`ci.yml`'s `postgres-repository`/`postgres-item-components` jobs or one of the seven `m5-*.yml` campaign jobs), **except two, found in `crates/oxide-batch/tests/`, that are out of scope for this issue**:

- `postgres_retention_component_state.rs` (M6 `#144`; not referenced by any workflow or `campaign-scope.json`)
- `postgres_item_stream_crash_recovery.rs` (M6 `#144`; documented in `docs/project/m6-item-stream-evidence.md` as "verified" only via local reproduction commands, with the same skip-on-missing-env pattern, and likewise not referenced by any workflow or `campaign-scope.json`)

These are the same *kind* of gap as #172 (a postgres-gated binary with no real-PostgreSQL CI owner), but a different evidence area — `#144`'s item-stream component-state evidence, not `#145`'s Gate G test-kit boundary — and neither belongs to a job this PR already touches. Per this issue's scope gate, they are not fixed here. **Tracked separately as #174** (`fix(m6): enforce #144 PostgreSQL component-state fixtures in CI`), following the same pattern as #170/#172; #174 owns both binaries above and remains out of scope for this PR.

## Local verification

Fresh PostgreSQL 18 (Homebrew), fresh database (`oxide_batch_172_scratch`), migrations applied via the same `migration_is_idempotent_when_migrator_fixture_is_available` step the job already runs, then the job's own step order reproduced end to end with no cleanup between binaries:

```
cargo test -p oxide-batch --features postgres --test postgres_repository \
  migration_is_idempotent_when_migrator_fixture_is_available -- --nocapture --test-threads=1
# 1 passed; 0 failed

cargo test -p oxide-batch --features postgres --test postgres_item_components_cursor -- --nocapture --test-threads=1
# 8 passed; 0 failed
cargo test -p oxide-batch --features postgres --test postgres_item_components_paging -- --nocapture --test-threads=1
# 8 passed; 0 failed
cargo test -p oxide-batch --features postgres --test postgres_item_components_batch_writer -- --nocapture --test-threads=1
# 7 passed; 0 failed
cargo test -p oxide-batch --features postgres --test postgres_item_components_crash_recovery -- --nocapture --test-threads=1
# 3 passed; 0 failed
cargo test -p oxide-batch --features postgres --test postgres_item_components_cursor_fault -- --nocapture --test-threads=1
# 1 passed; 0 failed
cargo test -p oxide-batch-test --features postgres --test postgres_item_components_db_restart -- --nocapture --test-threads=1
# 3 passed; 0 failed
cargo test -p oxide-batch-test --features postgres --test postgres_flat_file_restart -- --nocapture --test-threads=1
# 4 passed; 0 failed
cargo test -p oxide-batch-test --features postgres --test postgres_json_restart -- --nocapture --test-threads=1
# 6 passed; 0 failed
cargo test -p oxide-batch-test --features postgres --test postgres_item_components_restart -- --nocapture --test-threads=1
# 1 passed; 0 failed

cargo test -p oxide-batch-test --features postgres --test restart_harness -- --nocapture --test-threads=1
# 1 passed; 0 failed

cargo test -p oxide-batch-test --features postgres --test postgres_fixture -- --nocapture --test-threads=1
# 1 passed; 0 failed
```

The complete `postgres-item-components` test sequence was reproduced locally, in the job's real step order, against one shared, freshly created `oxide_batch_172_scratch` PostgreSQL 18 database: the job's nine pre-existing steps (ten `cargo test` invocations, one per distinct test binary — the "cursor and keyset paging" step alone issues two) followed immediately by the two new `restart_harness` and `postgres_fixture` steps. All twelve `cargo test` invocations above are the exact commands run, in that exact order, with no cleanup between them; every one passed with no state leakage.

## CI verification

### Producer commit

The workflow change was introduced in commit `d9b2de2ec412db33811a507e3bd71fd9b906e2ad` (PR #173) and has not been touched since. That commit's CI run is the evidence that the new steps actually execute real PostgreSQL-backed tests rather than only existing in YAML:

- `postgres-15-item-components`: PASS — [run log](https://github.com/luceat-lux-vestra/oxide-batch/actions/runs/32721028551/job/97412279026). The job log shows `test restart_harness_resumes_from_the_last_committed_checkpoint ... ok` (1 passed) and `test repository_fixture_cleans_up_isolated_metadata ... ok` (1 passed), each under its own `Run cargo test ... --test restart_harness` / `--test postgres_fixture` step group, after the nine pre-existing steps in the job's real order.
- `postgres-18-item-components`: PASS — [run log](https://github.com/luceat-lux-vestra/oxide-batch/actions/runs/32721028551/job/97412279185), same two tests, same pass counts, same order.
- Grepping each full job log for `skipped:` returns zero matches — neither test binary printed `skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set` in either job; both executed against the job's real PostgreSQL service container.
- All other required checks (quality, msrv, packaging, dependency-review, supply-chain, evidence-provenance, CodeQL, postgres-spike, the full PG15/16/17/18 design-gate matrix, postgres-15/18-repository, and the full M5 campaign matrix — cancellation, crash-restore, performance, resource-bounds, security, soak, upgrade, conformance) passed at this same commit.

### Exact-head merge gate

Per this repository's exact-head convention, the PR's actual merge gate is whatever its final head SHA is at merge time, not the producer commit specifically — see the PR's own Checks tab / description for that result rather than a SHA hardcoded in this file, since every edit to this file necessarily advances the head again.

## Production code

Zero changes to `crates/oxide-batch/src`, `oxide-batch-core`, `oxide-batch-repository`, `oxide-batch-plan`, or `oxide-batch-test/src`. No test assertions, scenarios, or matrix legs were weakened, skipped, or removed to reach green.

## Compatibility ledger

`TEST-REPO-001` (and the other `TEST-*` rows) remain **Implemented**. This correction enforces existing evidence in CI; it does not promote any row to **Verified** — that requires a named released `oxide-batch` version, per the ledger's own promotion rule, which this issue does not itself cut.
