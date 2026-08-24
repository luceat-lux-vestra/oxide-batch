# M6 PostgreSQL Restart Fixture CI Coverage Correction

**Issue:** #170
**PR:** #171

This is a CI/evidence coverage correction, not a feature or a production defect fix. It does not change, strengthen, or reinterpret any semantic claim made by #147, #148, or #149's evidence documents.

## Gap found

`cargo test --workspace --all-features` (the `quality` job) discovers every `#![cfg(feature = "postgres")]` test binary, but that job runs with no PostgreSQL service and no `OXIDEBATCH_POSTGRES_TEST_URL`. Each affected test's `runtime_url()` guard returns `None`, the test prints `skipped: OXIDEBATCH_POSTGRES_TEST_URL is not set` to stderr, and returns `Ok(())`. `cargo test` reports this as a pass. No job in `.github/workflows/` ever set the PostgreSQL env vars and ran these three binaries against a real server:

- `crates/oxide-batch-test/tests/postgres_flat_file_restart.rs` (added in #165)
- `crates/oxide-batch-test/tests/postgres_json_restart.rs` (added in #148)
- `crates/oxide-batch-test/tests/postgres_item_components_restart.rs` (added in #164)

The sibling binary `postgres_item_components_db_restart.rs`, from the same crate and added in the same #149 wave, *was* already wired into the `postgres-item-components` PG15/PG18 matrix job — these three were simply never added alongside it. That job was the correct existing owner; no new service job was created.

### Also found, out of scope here

`crates/oxide-batch-test/tests/postgres_fixture.rs` and `crates/oxide-batch-test/tests/restart_harness.rs` (both added by #162) have the identical execution gap. These are not M6 item-component evidence: they are #145's M6 Gate G test-kit boundary evidence — specifically the named Gate G scenarios `repository_fixture_cleans_up_isolated_metadata` and `restart_harness_resumes_from_the_last_committed_checkpoint` (`docs/project/m6-design-gate-evidence.md`, "Gate G test kit (#145)"). `repository_fixture_cleans_up_isolated_metadata` is also the sole evidence citation for the `TEST-REPO-001` conformance-matrix row (`Implemented`, M6/M8). `docs/project/m6-test-kit-evidence.md` already discloses honestly that both were only verified locally and are skipped in CI; this is a real, already-disclosed gap, not a doc that overclaimed. Tracked separately as #172, out of scope for this PR since it belongs to Gate G's test-kit evidence, not M6 item-component evidence.

## Fix

Three new steps were added to the existing `postgres-item-components` job in `.github/workflows/ci.yml`, immediately after the existing `postgres_item_components_db_restart` step, reusing that job's PG15/PG18 matrix, service container, env vars, and migration step verbatim:

- `postgres_flat_file_restart`
- `postgres_json_restart`
- `postgres_item_components_restart`

No new job, no new abstraction, no reusable workflow.

## Isolation verified

None of the three added binaries creates or touches a shared PostgreSQL business-data table (`sqlx`-driven `CREATE TABLE`/`DROP TABLE`/`TRUNCATE`) — each uses `PostgresFixture` only for durable job/step/checkpoint state under a per-run nonce-suffixed job name, and (where applicable) an in-process file/document fixture for business data. Locally, all three were run back-to-back with the job's other six steps against one shared, freshly created database (mirroring one live service container across a whole job run), in the job's step order, with no cleanup between binaries: all pass, order-independent, no state leakage.

## CI verification

### Producer commit

The workflow change itself — the three new steps in `.github/workflows/ci.yml` — was introduced in commit `ff3a3da4cf75d370efd07f18fb05fac43be6a8f5` and has not been touched since. That commit's CI run is the evidence that the new steps actually execute real PostgreSQL-backed tests rather than only existing in YAML:

- `postgres-15-item-components`: PASS — [run log](https://github.com/luceat-lux-vestra/oxide-batch/actions/runs/32695332457/job/97336248066) shows `postgres_flat_file_restart` (4 passed), `postgres_json_restart` (6 passed), and `postgres_item_components_restart` (1 passed) each actually executing, in that order, after the pre-existing steps.
- `postgres-18-item-components`: PASS — [run log](https://github.com/luceat-lux-vestra/oxide-batch/actions/runs/32695332457/job/97336248079), same three binaries, same counts, all passed.
- All other required checks (quality, msrv, packaging, dependency-review, supply-chain, evidence-provenance, CodeQL, and the full M5 campaign matrix) passed at this same commit.

This document and later documentation-only commits in this PR (this file, and #172's carve-out) necessarily land *after* `ff3a3da` — a doc commit that tried to claim "no commits followed" about itself would be self-contradictory the moment it was written. None of those later commits touches `.github/workflows/ci.yml`, `crates/oxide-batch-test/tests/`, or any other executable path, so they do not change what `ff3a3da`'s run demonstrated about the workflow's behavior.

### Exact-head merge gate

Per this repository's exact-head convention, the PR's actual merge gate is whatever its final head SHA is at merge time, not `ff3a3da` specifically — see the PR's own Checks tab / description for that result rather than a SHA hardcoded in this file, since every edit to this file necessarily advances the head again.

## Local verification

Fresh PostgreSQL 18 (Homebrew), fresh database, migrations applied via the same `migration_is_idempotent_when_migrator_fixture_is_available` step the job already runs:

```
cargo test -p oxide-batch-test --features postgres --test postgres_flat_file_restart -- --nocapture --test-threads=1
# 4 passed; 0 failed

cargo test -p oxide-batch-test --features postgres --test postgres_json_restart -- --nocapture --test-threads=1
# 6 passed; 0 failed

cargo test -p oxide-batch-test --features postgres --test postgres_item_components_restart -- --nocapture --test-threads=1
# 1 passed; 0 failed
```

## Production code

Zero changes to `crates/oxide-batch/src`, `oxide-batch-core`, `oxide-batch-plan`, or `oxide-batch-repository`. No test assertions, scenarios, or matrix legs were weakened, skipped, or removed to reach green.

## Compatibility ledger

`IO-FLAT-001` and `IO-STRUCTURED-001` remain **Implemented**. This correction enforces existing evidence in CI; it does not expand parity, promote either row to **Verified**, or alter #147/#148's semantic claims retroactively.
