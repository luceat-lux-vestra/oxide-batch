# M5 Exit Evidence

**State:** M5 Embedded Core Production Preview gate: PASSED

**Released:** OxideBatch `0.5.0` — M5 Embedded Core Production Preview

## Release identity

| Field | Value |
| --- | --- |
| Tag | `v0.5.0` (immutable; unmoved throughout closure) |
| Release commit | `7b830024d67148f5c3cc4c935cf8673d3ee54d6e` |
| GitHub Release | <https://github.com/luceat-lux-vestra/oxide-batch/releases/tag/v0.5.0> |
| Published | `2026-08-19T02:44:53Z` |
| Release recovery fix | PR [#135](https://github.com/luceat-lux-vestra/oxide-batch/pull/135) (`workflow_dispatch(tag)` recovery scaffold, squash merge `97c5dba214897a8fb8605aa73d1dcec86e95257d`) and PR [#136](https://github.com/luceat-lux-vestra/oxide-batch/pull/136) (explicit `User-Agent` on the crates.io checksum lookup, squash merge `fa89ec4a991d20d43f99f51567a26cbb2f0b476b`) |
| Release verification recovery | PR [#137](https://github.com/luceat-lux-vestra/oxide-batch/pull/137) — `fix(release): add verification recovery dispatch` (squash merge `575b618cd48c8d9a3a5b63c60c38586c10f7b4b0`); manual recovery run [`32223056964`](https://github.com/luceat-lux-vestra/oxide-batch/actions/runs/32223056964) against latest `main`, `succeeded` |

The `release.published` run tied to the original publication
(`32209663147`) failed its crates.io checksum comparison because it executed
the workflow definition that predates PR #136's `User-Agent` fix (crates.io
403s a request with no identifying `User-Agent`). PR #137 added a
`workflow_dispatch(tag)` recovery path that checks out the immutable release
tag with the *current* workflow definition, is structurally unable to reach
crates.io authentication or `cargo publish` (`Authenticate to crates.io` and
`Publish to crates.io` are both gated on `github.event_name == 'release'`),
and re-runs the same local/registry checksum comparison. The recovery run's
log recorded exact equality for every package:

```text
oxide-batch-core 0.5.0: local=3d59d211...b332b6 crates.io=3d59d211...b332b6
oxide-batch-repository 0.5.0: local=d0ac0fde...dcdfde4d crates.io=d0ac0fde...dcdfde4d
oxide-batch-plan 0.5.0: local=db4075b2...086b8fa4f3 crates.io=db4075b2...086b8fa4f3
oxide-batch 0.5.0: local=6c81c4da...b71e59ac5f crates.io=6c81c4da...b71e59ac5f
oxide-batch-cli 0.5.0: local=85906fd1...a267997787 crates.io=85906fd1...a267997787
```

`v0.5.0` was never republished, yanked, retagged, or force-pushed at any point
in this closure.

## crates.io publication

All five packages are published at `0.5.0`, independently verified against
the crates.io API (not by re-publishing):

| Package | Version | Yanked | Owner | Internal dependency pin |
| --- | --- | --- | --- | --- |
| `oxide-batch-core` | 0.5.0 | No | `luceat-lux-vestra` (sole owner) | — |
| `oxide-batch-repository` | 0.5.0 | No | `luceat-lux-vestra` (sole owner) | `oxide-batch-core =0.5.0` |
| `oxide-batch-plan` | 0.5.0 | No | `luceat-lux-vestra` (sole owner) | `oxide-batch-core =0.5.0` |
| `oxide-batch` | 0.5.0 | No | `luceat-lux-vestra` (sole owner) | `oxide-batch-core =0.5.0`, `oxide-batch-plan =0.5.0`, `oxide-batch-repository =0.5.0` |
| `oxide-batch-cli` | 0.5.0 | No | `luceat-lux-vestra` (sole owner) | `oxide-batch ^0.5.0` |

License, repository URL, and description metadata match the workspace
manifests for all five packages. No repository secret or environment secret
(the `release` GitHub Actions environment has zero secrets configured) stores
a long-lived crates.io token; the only publication path is OIDC Trusted
Publishing via `rust-lang/crates-io-auth-action`, gated to
`github.event_name == 'release'`.

## docs.rs

All five packages built successfully at `0.5.0` (`docs.rs` status API,
`doc_status: true`) with a reachable, HTTP-200 crate-root documentation page:

| Package | Build | Root page |
| --- | --- | --- |
| `oxide-batch-core` | Success | Reachable |
| `oxide-batch-repository` | Success | Reachable |
| `oxide-batch-plan` | Success | Reachable |
| `oxide-batch` | Success | Reachable |
| `oxide-batch-cli` | Success | Reachable |

This confirms build success and crate-root rendering; it is not an exhaustive
intra-doc-link crawl of every public item.

## crates.io-only external consumer

A consumer project entirely outside this repository (no path, git, or
workspace dependency) declared:

```toml
[dependencies]
oxide-batch = { version = "=0.5.0", features = ["postgres"] }
tokio = { version = "1", features = ["full"] }
```

`cargo generate-lockfile`, `cargo check --locked`, and `cargo build --locked`
all succeeded resolving every dependency — `oxide-batch` and its three
internal crates included — from the crates.io registry only (`cargo tree`
confirms `oxide-batch v0.5.0`, `oxide-batch-core v0.5.0`,
`oxide-batch-plan v0.5.0`, and `oxide-batch-repository v0.5.0`, none carrying
a path or git source).

Rather than a bare compile check, the consumer exercised the public facade
end to end against a real PostgreSQL 18.4 backend: `PostgresConfig` +
`PostgresMigrator::migrate`, job definition via `TaskletJob`/`TaskletStep`,
`PostgresJobRepository::connect`, `JobLauncher::launch`, and execution-status
query via `RepositoryUnitOfWork::get_job_execution`/`job_executions`. See
[PostgreSQL release smoke](#postgresql-release-smoke) below for the full
scenario and result.

## PostgreSQL release smoke

**Environment:** PostgreSQL 18.4 (Homebrew), an isolated scratch cluster
(not the host's default instance), TCP loopback with SCRAM authentication.
**Consumer:** the crates.io-only external consumer above, driven as two (and
for the kill/restart scenario, three) separate process invocations sharing
the same database, so "process restart" is a genuine new OS process with a
fresh connection, not merely a fresh `struct`.

**Normal release smoke — PASS.**

1. Fresh process: `PostgresMigrator::migrate` provisions schema, connects, and
   launches `m5_smoke_normal` (business_date `2026-08-19`) via `JobLauncher`;
   the execution reaches `BatchStatus::Completed` and is immediately
   re-queryable through the repository.
2. Process exits (`repository.close()`).
3. Fresh process (new PID, new connection) reconnects, finds the prior
   instance and its `Completed` execution by `JobInstanceKey` — confirming
   relaunching a `Completed` instance under the *same* identifying parameters
   is correctly rejected (`RepositoryError::CompletedInstance`), matching the
   ledger's `LIFE-COMPLETE-001` semantics.
4. The same fresh process launches a second, distinct instance
   (business_date `2026-08-20`) representing new post-restart work; it
   completes normally and is independently queryable, with the original
   instance unmodified.

**Kill/restart smoke — PASS.**

1. A process launches `m5_smoke_kill` with a tasklet that sleeps after the
   framework's `STARTED` transition commits (launch commits `STARTED` before
   user work runs). The process is sent `kill -9` mid-sleep.
2. Direct inspection confirms the execution is durably left in `STARTED`
   status in PostgreSQL — no partial or corrupted row.
3. After the configured 60-second staleness threshold elapses, a fresh
   process (new PID) reconnects, recognizes the single stale `Started`
   execution for the instance (no duplicate rows), and uses
   `RecoveryProposer` + `JobOperator::execute(OperatorRequest::recover(...))`
   to obtain and apply an audited `MarkFailed` recovery decision
   (`OperatorOutcomeClass::Applied`, `result_status: Failed`).
4. The same process then launches a fresh execution for the recovered
   instance; it completes normally (`BatchStatus::Completed`).
5. Final state for the instance: exactly two executions — the crashed one
   `Failed` via the audited recovery decision, the new one `Completed`. No
   duplicate or corrupt metadata.

This exercises OxideBatch's actual crash/restart contract: a `TaskletJob` has
no mid-step checkpoint to silently resume (that is `ChunkJob`'s scope, not
exercised end-to-end against a fresh crates.io-only consumer in this pass),
so recovery from a crash is the documented audited-recovery path
(`LIFE-RECOVER-001`) followed by a fresh, restart-eligible execution
(`LIFE-RESTART-001`) — not implicit silent resumption.

## GitHub Release artifacts

All eleven expected artifacts are present on the release: five `.crate`
archives, five CycloneDX `.cdx.json` SBOMs (one per crate), and one
`oxide-batch-0.5.0.sha256` checksum manifest. GitHub build attestations were
generated as part of the release-draft recovery in PR #135.

## Ledger disposition and promotion

The M5 evidence-campaign gate (issue [#102](https://github.com/luceat-lux-vestra/oxide-batch/issues/102))
closed on 2026-08-18 per [`m5-102-reconciliation.md`](m5-102-reconciliation.md):
every criterion `SATISFIED`, no unresolved correctness P0/P1 (re-confirmed in
this closure: `gh issue list --state open --label priority:p0` returns none;
the only open `priority:p1` issues are the milestone-tracking issues #12 and
#103 themselves).

With `v0.5.0` now released, this closure promotes `28` of the `29` advertised
embedded-kernel rows in
[`conformance-matrix.md`](../compatibility/conformance-matrix.md#m5-disposition-and-promotion-set)
from `Implemented` to `Verified`, each citing this record as its release
evidence. `META-CONTEXT-001` is the one advertised row that stays
`Implemented`: it still links an architecture spike rather than codec
migration tests, so it does not promote in this release and remains a named
M6+ gap. The `REPO-RETENTION-001`-adjacent least-privilege separation gate
that blocked `REPO-EXPLORE-001`/`REPO-OPERATOR-001` closed, because the M5
security campaign evidence is retained inside the `v0.5.0` release tree
itself.

Population after this promotion: `83` rows total — `28` `Verified`, `1`
`Implemented`, `13` `Partial`, `39` `Planned`, `2` `Unknown`. The `13`
`Partial` rows remain published preview limitations; the `39` `Planned` and
`2` `Unknown` rows remain visible and unreviewed/deferred. None of this
promotes or implies GA, stable-1.0, enterprise-ready, full Spring Batch
compatibility, or project-wide production readiness — the precise, accurate
positioning stays **OxideBatch 0.5.0, M5 Embedded Core Production Preview**.
`crates/oxide-batch/tests/m5_conformance_campaign.rs` and
`tests/fixtures/conformance/accepted-scope.json` were updated in the same
change so the ledger's machine-checked disposition and scope stay consistent
with this promotion (`accepted_scope_matches_the_ledger_disposition` and the
rest of that test file pass).

## Unresolved limitations and deferred M6+ capability

Unchanged by this closure, and explicitly not resolved by it:

- `LIFE-STOP-001`, `LIFE-RECOVER-001`, `STEP-STARTLIMIT-001`, `FT-RETRY-001`,
  `FT-SKIP-001`, `FT-ROLLBACK-001`, `LISTENER-ITEM-001`, `FLOW-SEQUENCE-001`,
  `FLOW-DECIDER-001`, `REPO-COMMAND-001`, `REPO-RETENTION-001`,
  `SCALE-PARSTEP-001`, and `SCALE-LOCALPART-001` stay `Partial` and expand in
  M6-M11.
- `META-CONTEXT-001` needs codec migration tests (not an architecture spike)
  before it can promote.
- No CSV/JSON production reader/writer, composite component, remote/
  distributed execution, additional Tier-1 database, or Spring metadata
  migration exists; these remain M6+ scope per the roadmap and the
  [M5 kickoff gate's scope controls](m5-kickoff-gate.md#scope-controls).
- This closure did not exercise `ChunkJob` mid-checkpoint crash/resume against
  a crates.io-only consumer (only the `TaskletJob` audited-recovery path);
  deeper crash/restart dogfooding across component kinds is recommended as
  part of M6's separate reference-consumer project.

## Release operations hygiene — complete

Independent of the M5 technical gate above, the `0.5.0` bootstrap publication
(the first-ever publish of `oxide-batch-core`, `oxide-batch-repository`,
`oxide-batch-plan`, and `oxide-batch-cli`) used a temporary crates.io API
token, since Trusted Publishing cannot create a crate that does not exist
yet. The maintainer subsequently confirmed completion of all bootstrap cleanup:

- crates.io Trusted Publisher is configured for all four newly created crates
  with owner `luceat-lux-vestra`, repository `oxide-batch`, workflow
  `release.yml`, and environment `release`;
- `cargo logout` was run on the bootstrap machine and the temporary registry
  credential was removed from local Cargo credentials;
- the temporary crates.io bootstrap API token was revoked.

Repository-side verification also confirmed that no GitHub repository or
`release`-environment secret stores a crates.io token. Every future
(non-bootstrap) release therefore uses OIDC Trusted Publishing exclusively
(`rust-lang/crates-io-auth-action`, gated to `github.event_name == 'release'`).
