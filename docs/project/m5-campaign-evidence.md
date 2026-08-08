# M5 Evidence Campaign Record

**State:** In progress. The conformance campaign is delivered; the crash and
restore, upgrade, security, resource-bound, soak, cancellation, performance,
and reference-workload campaigns are not.

**Issue:** [#102](https://github.com/luceat-lux-vestra/oxide-batch/issues/102)

**Date:** 2026-08-08

This record is the evidence for the sixth M5 workstream: running the campaigns
the [design gate](m5-design-gate-evidence.md) named and retaining reproducible
raw evidence for each. The campaigns themselves are listed in the
[performance and capacity plan](../engineering/performance-plan.md#m5-production-preview-campaigns);
the ten scenarios they owe are listed in the design gate's
[named campaign scenarios](m5-design-gate-evidence.md#named-campaign-scenarios).

The campaigns change no observable behavior. They add scenarios, one runner,
and retained records. Nothing here promotes a ledger row: promotion needs a
named released version, and that stays with
[#103](https://github.com/luceat-lux-vestra/oxide-batch/issues/103).

## Campaign status

| Campaign | Named scenarios | State |
| --- | --- | --- |
| Conformance | `full_embedded_conformance_suite_passes_on_the_accepted_scope` | Delivered |
| Crash and restore | `process_kill_at_each_commit_phase_recovers_without_a_forged_status` | Not started |
| Upgrade | `schema1_and_schema2_upgrade_directly_to_schema3`, `schema2_runtime_rejects_schema3`, `schema3_backup_restores_the_prior_schema` | Not started |
| Security | `verify_full_tls_is_required_in_the_supported_mode`, `least_privilege_role_cannot_exceed_its_class`, `redaction_sweep_finds_no_prohibited_value_class` | Not started |
| Resource bounds | `declared_ceilings_hold_under_stress_with_backpressure` | Not started |
| Soak | `soak_reports_no_task_connection_handle_or_memory_growth` | Not started |
| Cancellation | P-014 report | Not started |
| Performance | P-001, P-003, P-010 reports | Not started |
| Reference workload | Published P-003 run | Not started |
| Extraction | Build, size, and dependency observations | Delivered by [#99](https://github.com/luceat-lux-vestra/oxide-batch/issues/99) and recorded in the [crate-extraction evidence](m5-crate-extraction-evidence.md) |

## Conformance campaign

### What the campaign runs

The accepted M0-M4 scope is the ledger's `29` `Implemented` and `13` `Partial`
rows. The campaign runs the whole workspace test suite and requires each of
those `42` rows to be proved by scenarios that ran and passed.

The assignment is committed as
[`tests/fixtures/conformance/accepted-scope.json`](../../tests/fixtures/conformance/accepted-scope.json).
Each row lists one or more scenarios, and each scenario records the package,
the test target, the test path libtest reports, the ledger evidence class it
contributes, and the fixture it needs. `133` scenario assignments cover the
`42` rows.

The document is data rather than prose for one reason: two consumers need the
same list, and a list stated twice drifts.

### Why it is a runner and a test

The scenario the design gate named has two halves, and they cannot run in the
same place.

**Which rows are owed, and what proves each one** is a reconciliation between
the ledger, the scope document, and the tests the workspace declares. It runs
in `crates/oxide-batch/tests/m5_conformance_campaign.rs` as two scenarios:

- `accepted_scope_matches_the_ledger_disposition` holds the campaign's
  denominator. It parses the ledger, requires the disposition to stay the one
  the design gate closed (`0` `Verified`, `29` `Implemented`, `13` `Partial`,
  `39` `Planned`, `2` `Unknown`), requires the ledger's advertised-set and
  limitations prose to name exactly the `Implemented` and `Partial` rows, and
  requires the scope document to cover that union and nothing else.
- `every_accepted_row_names_a_declared_conformance_scenario` holds the
  assignment. Every row needs at least one conformance-class scenario, every
  scenario name must resolve to a test the workspace declares, and no row may
  name the same scenario twice.

**Whether the suite passes** is `cargo xtask conformance`. A test process
cannot observe it, for two reasons that are both about not forging a pass:

- several scenario names exist in more than one target —
  `committed_chunk_advances_checkpoint`,
  `concurrent_launch_creates_single_instance`,
  `completed_instance_rejects_launch`,
  `concurrency_one_matches_parallel_durable_observations` and others — so a
  result is only attributable when the runner knows which target produced it;
- every PostgreSQL scenario returns success without a database. It prints a
  skip line to stderr and returns `Ok`. Under `cargo test` that is
  indistinguishable from evidence.

The runner therefore resolves the fixtures first and refuses to start without
them. On a development host with no PostgreSQL the campaign fails, by design,
with the variables it needs named. This mirrors the deviation the
[facade review](m5-facade-api-review-evidence.md) recorded for
`rustdoc_surface_contains_no_leaked_implementation_type`: the scenario is
delivered as a command because the evidence it needs is not available inside a
test process.

### How the runner works

1. It reads the scope document and resolves each declared fixture against the
   environment. A fixture some scenario needs and the environment lacks is a
   violation, and the run stops there with a report recording the absence.
2. It enumerates every test target from `cargo metadata` — every target whose
   `test` flag is set, as a `--lib`, `--bin`, or `--test` selector.
3. It runs each target through cargo, one at a time, with one test thread, and
   attributes every `test <path> ... <outcome>` line to that target. Running
   through cargo rather than executing the built binary matters: the
   compile-fail suite needs the environment cargo supplies, and executing its
   binary directly fails for a reason that has nothing to do with the facade.

   The prefix and the outcome are read separately, because they do not always
   arrive together. libtest writes `test <path> ... ` before the test runs and
   the outcome after; a test that re-executes its own binary — which is how
   every process-kill scenario works — lets the child's libtest header land
   between the two halves. Reading only whole lines lost all nine
   crash-recovery results as "did not run", which is how the split was found.
4. It runs the workspace documentation tests as one target.
5. It reconciles: every assigned scenario must have reported `ok`. A scenario
   that reported `ignored`, reported `FAILED`, or never ran is a violation, as
   is any target that exited unsuccessfully.
6. It writes the report to `OXIDEBATCH_CAMPAIGN_DIR`, or to
   `target/m5-campaigns` when that is unset, so an ordinary run never rewrites
   retained evidence.

### Where it runs

`postgres-15-conformance-campaign` and `postgres-18-conformance-campaign` in
`.github/workflows/ci.yml`, on the two ends of the supported PostgreSQL
`15`-`18` range, matching the existing `postgres-repository` matrix. Each job
retains its report as a build artifact, and the committed copies in
[`docs/engineering/campaigns/m5`](../engineering/campaigns/m5/README.md) come
from those jobs.

### Results

Both matrix points pass. Each run executed `66` test targets and `473` tests,
every one reporting `ok`, with the workspace documentation tests passing and
all three fixtures present. All `42` accepted rows are proved.

| Report | Matrix | Targets | Tests | Outcomes | Result |
| --- | --- | --- | --- | --- | --- |
| [`conformance-campaign-postgres-15.json`](../engineering/campaigns/m5/conformance-campaign-postgres-15.json) | PostgreSQL 15 | 66 | 473 | 473 `ok` | Passed |
| [`conformance-campaign-postgres-18.json`](../engineering/campaigns/m5/conformance-campaign-postgres-18.json) | PostgreSQL 18 | 66 | 473 | 473 `ok` | Passed |

Both were produced by commit `0f41aad`, which is the merge commit the workflow
checked out rather than a branch tip.

The `133` scenario assignments break down by ledger evidence class as `42`
conformance, `53` unit, `20` integration, `9` crash, `5` performance, and `4`
migration. Every row has at least one conformance-class scenario, which the
in-process reconciliation requires.

No correctness P0 or P1 is open against this campaign. The two findings below
are recorded and both are closed.

### What this campaign does not establish

- **Evidence-profile completeness.** The ledger gives each row a required
  `U/I/C/Cr/M/P` profile. This campaign assigns each scenario its class and
  records the classes in the report, but it does not require a row's profile to
  be fully covered. Several `M` and `P` cells are closed by the campaigns that
  follow, and profile completeness is a promotion condition that belongs to
  #103.
- **Anything outside the accepted scope.** The `39` `Planned` and `2` `Unknown`
  rows are untouched and stay visible.
- **A parity claim.** Passing the accepted scope is not parity with the ledger
  population, and the record's own denominator says so.

### Findings

**F1. The RFC-0005 spike's allocation counter measured other threads.**
The first campaign run that reached the whole suite failed on PostgreSQL 18
and passed on 15, with `the_typed_pipeline_allocates_nothing_per_item`
reporting `2` allocations of `144` bytes where every other run reported `0`.
The counting allocator in `spikes/m6-item-hot-path/src/allocation.rs` enabled
its window with a process-global flag, so it counted whatever the test
harness's own thread allocated while the measured run was in flight.

That makes the central [spike 0004](../architecture/spikes/0004-static-and-erased-item-path.md)
measurement depend on what an unrelated thread happened to do, which is not a
reproducible measurement. The window, the counters, and the allocations they
see now all belong to one thread; the assertions are unchanged and still
require exactly zero. The spike record's limitation note is corrected.

This is the campaign working: the flake existed before this issue and no gate
had run the whole suite in one place often enough to see it.

**F2. The harness's matrix identifiers were never ledger identifiers.**
`crates/oxide-batch/tests/conformance/mod.rs` carries `MATRIX_SCENARIOS`,
documented as "matrix rows known to the harness" with the note that "status
remains authoritative in the matrix". Its `20` identifiers are the acceptance
criteria of the [first vertical slice](../product/first-vertical-slice.md) —
`VS-LAUNCH-001`, `CHUNK-COMMIT-001`, `RESTART-001` and their siblings — plus
several that appear in no document at all, including `FLOW-001` and
`RECOVERY-001`. Not one of them is a row in the feature ledger.

Nothing was wrong with the scenarios; the naming implied a reconciliation with
the ledger that did not exist, and a reader checking ledger coverage against
that list would have been reading the wrong list. The campaign does not reuse
it: the accepted-scope document is keyed by ledger row identifier, and the two
lists stay separate because they answer different questions. The stale doc
comment is corrected in place.
