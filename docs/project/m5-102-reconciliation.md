# M5 Issue #102 Evidence Campaign Reconciliation

**State:** Complete

**Issue:** [#102](https://github.com/luceat-lux-vestra/oxide-batch/issues/102)

**Date:** 2026-08-18

This is the authoritative, independently-derived reconciliation of every
[#102](https://github.com/luceat-lux-vestra/oxide-batch/issues/102) exit
criterion against the current repository, its CI workflows, and its retained
evidence. It does not restate the campaign narratives; those live in the
[M5 evidence campaign record](m5-campaign-evidence.md), which this document
cross-checks and, where it found stale forward-references, corrected. It
exists so a reviewer can answer, without access to the originating session,
whether #102's evidence is complete, reproducible, and provenance-bound.

`cargo xtask reconciliation` (folded into `cargo xtask check`) machine-checks
the objective parts of this document — the criterion table's shape, the
declared campaign/report counts, and that every evidence link it names
resolves on disk — against the repository on every run, so this document
cannot silently drift from what it claims without failing CI. The prose
conclusions (no open correctness P0/P1, no orphan evidence cell, denominator
independently re-derived) are recorded here because they are not the kind of
claim a parser should adjudicate; see [What is machine-checked](#what-is-machine-checked-and-what-is-not)
below.

## Evidence identity

| Field | Value |
| --- | --- |
| Audited baseline (`origin/main` at assignment) | `13ae527c0d945dca945d92281556fca79ebf6a6e` |
| PR #133 merge commit (evidence execution tree) | `58213c0fd14099d7d77930d9ea751659ac2bcc43` — the tree all 16 retained reports below record from their own checkout, per `evidence-provenance.json` |
| Declared campaigns | `8` (conformance, crash-and-restore, upgrade, security, resource-bounds, soak, cancellation, performance/reference-workload) |
| Required PostgreSQL matrix per campaign | `postgres-15`, `postgres-18` (release-blocking; majors 16-17 receive connection/migration/smoke coverage per the [support matrix](../release/support-matrix.md)) |
| Retained reports | `16` — independently recomputed as 8 declared campaigns × the 2-point required matrix, matching `docs/engineering/campaigns/m5/*.json` (excluding `evidence-provenance.json` itself) and `cargo xtask evidence`'s own count |
| Conformance denominator | `42` ledger rows (`29` `Implemented` + `13` `Partial`, independently recounted from `docs/compatibility/conformance-matrix.md`), proved by `133` scenario assignments (independently recounted from `tests/fixtures/conformance/accepted-scope.json`) |
| Unresolved M5-scope correctness P0/P1 | None. `gh issue list --state open --label priority:p0` returns no results; the only open `priority:p1` issues are the milestone-tracking issues #12, #102 (this issue), and #103 (docs/exit record, blocked on #102), none of which is a code defect. No `#[ignore]`d test and no `TODO`/`FIXME` marker exists under `crates/` or `xtask/`. |
| Repository checks | `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, `cargo test --workspace --all-features`, `cargo xtask check`, `cargo xtask evidence`, `cargo xtask reconciliation` all pass at the PR's final HEAD (see the PR description for the exact commands and CI run links) |
| Retention note | This PR's own `xtask/src/main.rs` change (new `reconciliation` subcommand, clippy-arg fix) is part of every M5 campaign's declared semantic closure, which invalidated the 16 reports retained before this PR under `cargo xtask evidence`. All 16 were re-run fresh against PR #133's merge commit (workflow run IDs and job IDs are recorded per-campaign in `evidence-provenance.json` and in each campaign's Results section of [the evidence record](m5-campaign-evidence.md)) and independently re-verified — digest, byte-for-byte extracted-report match, and git-blob identity — before retention. |

## Criterion reconciliation

Every row below carries exactly one disposition: `SATISFIED`, `BLOCKED`, or
`NOT APPLICABLE`. None is used to paper over missing evidence; a `BLOCKED` row
would keep this PR from closing #102, and none appears below.

| # | #102 criterion | Authoritative requirement | Campaign / workflow | Verifier / test | Supported matrix | Retained evidence | Reproduction | Result |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| A | Full embedded conformance suite passes across the accepted M0-M4 scope | [Performance plan](../engineering/performance-plan.md#m5-production-preview-campaigns), [design gate](m5-design-gate-evidence.md#named-campaign-scenarios); denominator = ledger's `Implemented`+`Partial` rows ([conformance-matrix.md](../compatibility/conformance-matrix.md#m5-disposition-and-promotion-set)) | `.github/workflows/m5-conformance.yml` | `cargo xtask conformance` (`xtask/src/conformance.rs`); denominator reconciled by `cargo test -p oxide-batch --test m5_conformance_campaign` | postgres-15, postgres-18 | [`conformance-campaign-postgres-15.json`](../engineering/campaigns/m5/conformance-campaign-postgres-15.json), [`-18.json`](../engineering/campaigns/m5/conformance-campaign-postgres-18.json) — 30 targets, 291 tests, 291 `ok`, both points | `./tests/fixtures/conformance/run-ci-campaign.sh 18` | **SATISFIED** |
| B | PostgreSQL crash and restore campaigns pass on the supported matrix | [Design gate](m5-design-gate-evidence.md), `process_kill_at_each_commit_phase_recovers_without_a_forged_status`, P-013 | `.github/workflows/m5-crash-restore.yml` | `cargo xtask crash-restore` (`xtask/src/crash_restore.rs`); reconciled by `m5_crash_restore_campaign.rs` | postgres-15, postgres-18 | [`crash-restore-campaign-postgres-15.json`](../engineering/campaigns/m5/crash-restore-campaign-postgres-15.json), [`-18.json`](../engineering/campaigns/m5/crash-restore-campaign-postgres-18.json) — 5 commit-phase kills + P-013 + logical restore, both points | `./tests/fixtures/crash-restore/run-ci-campaign.sh 18` | **SATISFIED** |
| C | Upgrade/migration/rollback: `schema1_and_schema2_upgrade_directly_to_schema3`, `historical_runtimes_reject_the_current_schema`, `every_source_backup_restores_its_prior_schema` (inherited from #100) | [Design gate](m5-design-gate-evidence.md#dependency-handoff), [support matrix](../release/support-matrix.md#m5-production-preview-support-bounds) | `.github/workflows/m5-upgrade.yml` | `cargo xtask upgrade` (`xtask/src/upgrade.rs`); reconciled by `m5_upgrade_campaign.rs`, including `the_historical_revision_is_bound_across_scope_runner_and_contract`, which pins the schema-2 runtime revision across the scope document, runner, and contract so future source drift cannot silently redefine "schema 2 runtime" | postgres-15, postgres-18 | [`upgrade-campaign-postgres-15.json`](../engineering/campaigns/m5/upgrade-campaign-postgres-15.json), [`-18.json`](../engineering/campaigns/m5/upgrade-campaign-postgres-18.json) — all 5 schema paths observed, both points | `./tests/fixtures/upgrade/run-ci-campaign.sh 18` | **SATISFIED** |
| D | Security campaigns pass with validated TLS and least-privilege roles | [Design gate](m5-design-gate-evidence.md), `verify_full_tls_is_required_in_the_supported_mode`, `least_privilege_role_cannot_exceed_its_class`, `redaction_sweep_finds_no_prohibited_value_class` | `.github/workflows/m5-security.yml` | `cargo xtask security` (`xtask/src/security.rs`); reconciled by `m5_security_campaign.rs` against the exact 40-identity role-matrix denominator in `tests/fixtures/security/role-matrix.json` | postgres-15, postgres-18 | [`security-campaign-postgres-15.json`](../engineering/campaigns/m5/security-campaign-postgres-15.json), [`-18.json`](../engineering/campaigns/m5/security-campaign-postgres-18.json) — `verify-full` TLS + 3 refusal classes, 40/40 role-matrix cells (10 allowed/30 forbidden), 0 prohibited redaction occurrences over 483 scanned strings, both points | `./tests/fixtures/security/run-ci-campaign.sh 18` | **SATISFIED** |
| E | Performance campaigns pass against the performance plan's regression gates | [Performance plan](../engineering/performance-plan.md#m5-production-preview-campaigns) (P-001, P-003, P-010) | `.github/workflows/m5-performance.yml` | `cargo xtask performance` (`xtask/src/performance.rs`); reconciled by `m5_performance_campaign.rs` | postgres-15, postgres-18 | [`performance-campaign-postgres-15.json`](../engineering/campaigns/m5/performance-campaign-postgres-15.json), [`-18.json`](../engineering/campaigns/m5/performance-campaign-postgres-18.json) | `./tests/fixtures/performance/run-ci-campaign.sh 18` | **SATISFIED** — observational only; no throughput/latency number is asserted as an SLA (see [claim boundary](#claim-boundary)) |
| F | Soak campaigns pass against the performance plan's regression gates | [Performance plan](../engineering/performance-plan.md#m5-production-preview-campaigns) (P-015), `soak_reports_no_task_connection_handle_or_memory_growth` | `.github/workflows/m5-soak.yml` | `cargo xtask soak` (`xtask/src/soak.rs`, `xtask/src/soak_evidence.rs`); reconciled by `m5_soak_campaign.rs` | postgres-15, postgres-18 | [`soak-campaign-postgres-15.json`](../engineering/campaigns/m5/soak-campaign-postgres-15.json), [`-18.json`](../engineering/campaigns/m5/soak-campaign-postgres-18.json) — 32 warmup + 600 measured cycles, all 15 correctness obligations held, tasks/connections/handles flat at every boundary, both points | `./tests/fixtures/soak/run-ci-campaign.sh 18` | **SATISFIED** |
| G | Cancellation evidence (accepted into the M5 campaign set; not separately named in #102's original text) | [Performance plan](../engineering/performance-plan.md#m5-production-preview-campaigns) (P-014); design gate records no separate named scenario for this campaign (recorded rather than repaired, see the campaign's own scope note) | `.github/workflows/m5-cancellation.yml` | `cargo xtask cancellation` (`xtask/src/cancellation.rs`); reconciled by `m5_cancellation_campaign.rs` | postgres-15, postgres-18 | [`cancellation-campaign-postgres-15.json`](../engineering/campaigns/m5/cancellation-campaign-postgres-15.json), [`-18.json`](../engineering/campaigns/m5/cancellation-campaign-postgres-18.json) — durable `STOPPED` terminal, phase-separated latency (async/blocking/transaction), unjoined counts at every declared deadline, restart re-runs no committed partition, both points | `./tests/fixtures/cancellation/run-ci-campaign.sh 18` | **SATISFIED** — cancellation semantics unchanged from the accepted campaign set; no new scenario added |
| H | Resource-bound campaigns pass against the performance plan's regression gates | [Performance plan](../engineering/performance-plan.md#m5-production-preview-campaigns) (declared ceiling proof); [`tests/fixtures/resource-bounds/campaign-scope.json`](../../tests/fixtures/resource-bounds/campaign-scope.json) | `.github/workflows/m5-resource-bounds.yml` | `cargo xtask resource-bounds` (`xtask/src/resource_bounds.rs`); reconciled by `m5_resource_bounds_campaign.rs`. Four corrective passes (F40-F42 and the fourth pass) closed exact-set gaps: every root/generic construction cell must match its declared bound and canonical unit, malformed or contradictory extra cells fail rather than disappearing, and `Cargo.lock` is bound into the semantic closure | postgres-15, postgres-18 | [`resource-bounds-campaign-postgres-15.json`](../engineering/campaigns/m5/resource-bounds-campaign-postgres-15.json), [`-18.json`](../engineering/campaigns/m5/resource-bounds-campaign-postgres-18.json) — 36/36 declared resources observed, 0 violations, both points | `./tests/fixtures/resource-bounds/run-ci-campaign.sh 18` | **SATISFIED** — no orphan evidence cell, no declared resource without evidence (exact-set checked by the campaign's own reconciliation test) |
| I | Reference workload runs and its results are recorded against the performance/correctness gates | [Performance plan](../engineering/performance-plan.md#m5-production-preview-campaigns) (P-003 as the reference workload) | `.github/workflows/m5-performance.yml` (same run produces the reference-workload report; one P-003 execution satisfies both rows) | `cargo xtask performance`; reconciled by `m5_performance_campaign.rs` | postgres-15, postgres-18 | Same [`performance-campaign-postgres-{15,18}.json`](../engineering/campaigns/m5/performance-campaign-postgres-15.json) reports — 10,000 rows, seed 102, source digest = written digest, `AtomicSameResource`, both points | `./tests/fixtures/performance/run-ci-campaign.sh 18` | **SATISFIED** — real end-to-end execution against the accepted M5 embedded boundary; P-003's reader/writer are explicitly documented as test-local evidence code, not the `IO-FLAT-001` CSV component (M6 scope); no CSV/JSON production-component parity is claimed |
| J | No unresolved correctness P0/P1 remains at the M5 triage bar | [M5 kickoff gate](m5-kickoff-gate.md#definition-of-done) | N/A — cross-repository search | `gh issue list --state open --label priority:p0/p1`; `grep -rn -e "#[ignore]" -e "TODO" -e "FIXME" crates/ xtask/` | N/A | See [evidence identity](#evidence-identity) above | `gh issue list --state open --label priority:p0` | **SATISFIED** — no open P0; the three open P1s are milestone-tracking issues (#12, #102, #103), not code defects |

## What is machine-checked, and what is not

`cargo xtask reconciliation` (`xtask/src/reconciliation.rs`, wired into
`cargo xtask check`) verifies, on every run:

- the `## Criterion reconciliation` table, parsed as a markdown table rather
  than scanned as free text, names the criterion set `{A, ..., J}`
  *exactly* — every required ID present once, no missing ID, no duplicate,
  and no unexpected extra ID (an `K` row would itself be a violation);
- every row's `Result` cell starts, after stripping markdown emphasis, with
  exactly one of `SATISFIED`, `BLOCKED`, or the two-word literal `NOT
  APPLICABLE` — a structural prefix-and-boundary check on the parsed column,
  not a substring search over the row, so a string like `NOT SATISFIED`
  cannot be mistaken for `SATISFIED`;
- the `## Evidence identity` table names exactly one `Declared campaigns`
  row and exactly one `Retained reports` row, each with a value that parses
  as an integer and matches what `evidence-provenance.json` and the
  retained-report directory actually contain — a missing row, a duplicated
  row, or an unparsable value is itself a violation, never a silently
  skipped comparison;
- every repository-relative evidence link this document names — every
  `docs/`, `tests/fixtures/`, `.github/workflows/`, and `xtask/src/` path
  cited in the criterion table — resolves to a real file, so this document
  cannot cite evidence that has moved or never existed.

Every one of the checks above has a negative/mutation unit test in
`xtask/src/reconciliation.rs`'s own test module — deleting a required row,
duplicating one, corrupting a count, adding an unexpected criterion,
removing or replacing a disposition, and citing a nonexistent evidence link
each have a dedicated test proving the checker actually rejects them, using
synthetic fixture text rather than the real document.

It deliberately does not attempt to parse or judge the prose disposition of
any row: whether a role-matrix cell count is the *right* count, whether a
GitHub issue is genuinely a correctness defect, and whether the campaign
narratives in [the evidence record](m5-campaign-evidence.md) are honestly
argued are human-review conclusions, recorded here and cross-checked against
the ledger and fixtures by hand during this reconciliation (see
[criterion reconciliation](#criterion-reconciliation)), not brittle parsers
run over free text. Each individual campaign's own denominator, exact-set,
and provenance checks are already machine-verified in depth by
`cargo xtask evidence` and each campaign's own `cargo test -p oxide-batch
--test m5_*_campaign` reconciliation; this check adds only the cross-cutting
layer specific to #102 closure that no single campaign's own verifier owns.

## Claim boundary

This reconciliation closes the M5 evidence-campaign gate only. It does not by
itself establish:

- project-wide production readiness, enterprise readiness, or Spring Batch
  parity — the compatibility ledger's `39` `Planned` and `2` `Unknown` rows
  remain visible and unresolved, and the `13` rows that stay `Partial` are
  published as limitations, exactly as the [design gate](m5-design-gate-evidence.md#ledger-disposition-review)
  requires;
- a `Verified` promotion for any ledger row. `Verified` requires a named
  released OxideBatch version per the [conformance matrix's row rules](../compatibility/conformance-matrix.md#row-and-claim-rules)
  and the [M5 kickoff gate](m5-kickoff-gate.md#definition-of-done); that
  promotion, and the M5 exit record, are [#103](https://github.com/luceat-lux-vestra/oxide-batch/issues/103)'s
  scope, not this one's;
- an SLA or performance commitment. Every number in the performance,
  reference-workload, and soak reports is an observation; the campaigns
  themselves say so, and nothing here promotes one to a budget;
- any M6+ capability. No item/component catalog, production CSV/JSON
  component, advanced flow, additional database, repository-portability,
  messaging, remote/distributed execution, scheduler, or Spring
  metadata-import capability was added or implied by this reconciliation.

## Residual limitations left to later milestones

These are recorded, not resolved, by this reconciliation:

- `LIFE-STOP-001`, `LIFE-RECOVER-001`, `STEP-STARTLIMIT-001`, `FT-RETRY-001`,
  `FT-SKIP-001`, `FT-ROLLBACK-001`, `LISTENER-ITEM-001`, `FLOW-SEQUENCE-001`,
  `FLOW-DECIDER-001`, `REPO-COMMAND-001`, `REPO-RETENTION-001`,
  `SCALE-PARSTEP-001`, and `SCALE-LOCALPART-001` stay `Partial` and expand in
  M6-M11 (see [conformance-matrix.md](../compatibility/conformance-matrix.md#m5-disposition-and-promotion-set)).
- `META-CONTEXT-001` currently links an architecture spike rather than codec
  migration tests, and the least-privilege separation `REPO-RETENTION-001`
  depends on now has schema-3 evidence (this campaign's security results)
  but still needs the release-fixture promotion #103 owns.
- P-002 (static-versus-erased measurement) is explicitly not an M5 campaign
  and remains M6 scope per the [performance plan](../engineering/performance-plan.md#m5-production-preview-campaigns).
- Remote/distributed execution, additional Tier-1 databases, and M6-M13
  scope named in the [M5 kickoff gate's scope controls](m5-kickoff-gate.md#scope-controls)
  remain out of bounds and untouched by this reconciliation.
