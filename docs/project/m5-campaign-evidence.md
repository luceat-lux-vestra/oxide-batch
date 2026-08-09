# M5 Evidence Campaign Record

**State:** In progress. The conformance, crash-and-restore, and upgrade
campaigns are delivered; the security, resource-bound, soak, cancellation,
performance, and reference-workload campaigns are not.

**Issue:** [#102](https://github.com/luceat-lux-vestra/oxide-batch/issues/102)

**Date:** 2026-08-09

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
| Crash and restore | `process_kill_at_each_commit_phase_recovers_without_a_forged_status`, plus the P-013 and logical-restore reports | Delivered |
| Upgrade | `schema1_and_schema2_upgrade_directly_to_schema3`, `schema2_runtime_rejects_schema3`, `schema3_backup_restores_the_prior_schema` | Delivered |
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

## Crash and restore campaign

### What the campaign owes

The [performance plan](../engineering/performance-plan.md#m5-production-preview-campaigns)
gives the crash and restore row three obligations: P-013 restart after many
chunks, process-kill at each commit phase, and a logical backup restore on
PostgreSQL 15 and 18. The [design gate](m5-design-gate-evidence.md#named-campaign-scenarios)
names one of them as a scenario,
`process_kill_at_each_commit_phase_recovers_without_a_forged_status`.

The denominator is committed as
[`tests/fixtures/crash-restore/campaign-scope.json`](../../tests/fixtures/crash-restore/campaign-scope.json).
It lists the three reports, the five commit-protocol phases a process must be
killed in, and the `11` M2-M4 scenarios the campaign reuses rather than
rewrites. `crates/oxide-batch/tests/m5_crash_restore_campaign.rs` reconciles it
against the plan and the gate in an ordinary `cargo test`, so a phase or a
report cannot quietly leave the campaign.

### The phases, and why two of them needed no adapter change

The M2-M4 crash targets already kill a process on either side of a durable
write. What they do not cover is the inside of one commit, and they leave the
process by calling `exit`, which is an orderly departure taken only where
application code chooses to take it. Every phase below is reached by a live
process that is then killed with `SIGKILL` from outside, and the campaign
requires the child to report termination by signal with no exit code.

| Phase | Where the process dies | Required durable outcome |
| --- | --- | --- |
| `business-written` | The enlisted rows are written and `commit` has not been called | Nothing durable; the chunk replays whole |
| `state-provided` | Inside `commit`, after the counters are checked and the durable state produced, before the progress bind | Nothing durable; the chunk replays whole |
| `progress-blocked` | The progress write is issued and blocked | Nothing durable; the chunk replays whole |
| `commit-in-flight` | `COMMIT` is in flight and the server completes it after the kill | The chunk is durable and must not replay |
| `commit-acknowledged` | `commit` returned successfully | The chunk is durable and must not replay |

Two of these are inside the adapter, where no application hook exists. They are
reached without changing a line of the adapter, by holding the lock the commit
is about to need: a row lock on the step execution row blocks the progress
write, and a deferred constraint trigger on the business table blocks `COMMIT`
until the campaign releases an advisory lock. A backend blocked on a lock stays
blocked, so the kill lands at the phase on every run rather than racing a fast
statement.

`commit-in-flight` is the unknown-outcome boundary and the most valuable of the
five. The killed process never learns whether its commit succeeded; the server
completes it afterwards. The campaign requires the restart to observe the chunk
as durable, to leave the business rows unique, and to reach the same terminal
observation as a run that was never interrupted.

### What every report asserts

A measurement is only worth recording if the recovery it times is correct, so
each report is a correctness scenario first. All three require the same thing
of the restart: the finished job must be **indistinguishable from an
uninterrupted run of the same work**, compared as the committed reader
position, the durable counters, the exact set of enlisted business rows, and
the final job and step status.

That comparison is what rules out both failure directions at once. A restart
that reprocessed a committed chunk would collide with the business table's
primary key; one that skipped a chunk would leave fewer rows. Neither can
report a pass.

Each report additionally requires that the crashed attempt is never forged into
a terminal success: it stays `STARTED` until an audited operator decision
resolves it, resolving it never rewrites what it durably committed, and the
restart is a new attempt with the crashed one still visible.

Restart discovery runs through the accepted path rather than a shortcut.
`RecoveryProposer` produces an evidence-bound proposal, the campaign requires
that proposal to agree with the durable metadata — status, optimistic version,
step identity, and the checkpoint schema the step declared — and `JobOperator`
applies the decision. Where the proposal reports an unknown commit, the
campaign uses the reason code the accepted contract reserves for it rather than
one that would be accepted either way.

### Why it is a runner as well as a test

Every scenario returns success without a database, because it prints a skip
line and returns; under `cargo test` that is indistinguishable from evidence.
`cargo xtask crash-restore` resolves the fixtures first and refuses to start
without them.

Passing tests are not sufficient either, so the runner requires more than a
green result. Each report writes a machine-readable observation into a
directory the runner creates empty and inspects afterwards, recording the child
process identifier, the signal it died from, the durable state the kill left,
the discovery result, the decision, and the restart outcome. A report with no
observation, a phase missing from one, a phase that did not end in `SIGKILL`,
or an observation carrying a violation fails the campaign.

The reused M2-M4 scenarios are run rather than cited. The campaign does not
rewrite the evidence those milestones delivered, and it does not take their
passing on trust.

### Where it runs

`postgres-15-crash-restore-campaign` and `postgres-18-crash-restore-campaign`
in `.github/workflows/ci.yml`, on the two ends of the supported PostgreSQL
`15`-`18` range. Each job installs the client tools matching its matrix point,
because the backup report takes a real `pg_dump` archive and loads it with a
real `pg_restore`; the runner image ships a different major version whose
wrapper would otherwise be selected silently. Each job retains its report as a
build artifact, and the committed copies in
[`docs/engineering/campaigns/m5`](../engineering/campaigns/m5/README.md) come
from those jobs.

### Results

Both matrix points pass. Every declared phase killed a live process with
`SIGKILL`, every report retained an observation, and all `11` reused M2-M4
scenarios ran and reported `ok`.

| Report | Matrix | Result |
| --- | --- | --- |
| [`crash-restore-campaign-postgres-15.json`](../engineering/campaigns/m5/crash-restore-campaign-postgres-15.json) | PostgreSQL 15 | Passed |
| [`crash-restore-campaign-postgres-18.json`](../engineering/campaigns/m5/crash-restore-campaign-postgres-18.json) | PostgreSQL 18 | Passed |

Both were produced by commit `8ec060b`, which is the merge commit the workflow
checked out rather than a branch tip, on `rustc 1.97.1` and Linux `x86_64`.

**Commit phases.** All five phases behaved as the accepted contract requires,
identically on both matrix points. The three phases before the commit record
left the durable checkpoint at `2` with exactly the two enlisted rows of the
first chunk; the two after it left it at `4` with four rows. Every restart
inherited exactly what its crashed attempt had committed and reached the same
terminal observation as the uninterrupted run — position `6`, counters
`6/6/6/0/3/0`, and the same six business rows — whichever phase the kill landed
in.

`commit-in-flight` is worth stating separately, because it is the case the
protocol exists for. The process was killed while `COMMIT` was blocked in a
deferred trigger; the server completed the commit afterwards, so the work
became durable and the process that did it never learned so. The restart
observed the chunk as committed, did not replay it, and the business table's
primary key would have rejected it if it had.

The recovery proposal reported `unknown_commit` as `false` in all five phases,
on both matrix points. That is correct and worth recording: the marker reports
durable ambiguity — an `UNKNOWN` status or an `UNKNOWN_COMMIT` failure category
a process recorded before it stopped — and a process killed outright records
nothing. The campaign resolves each attempt under the reason code the evidence
justifies rather than under one that would be accepted either way.

**P-013.** The workload is `200` chunks of `5` items, killed after `130`
committed chunks, with the remaining `70` resumed after restart. The durable
checkpoint named position `650` exactly, and the enlisted rows were exactly the
first `650`.

| Matrix | Uninterrupted run | Chunks before the kill | Discovery | Resume |
| --- | --- | --- | --- | --- |
| PostgreSQL 15 | `1083 ms` | `634 ms` | `68 ms` | `291 ms` |
| PostgreSQL 18 | `759 ms` | `443 ms` | `38 ms` | `266 ms` |

The two matrix points ran on different runners, so the gap between them is a
host difference rather than a database one and no comparison between the rows
is claimed.

Discovery is the durable reading plus the evidence-bound proposal. Resume is
the restart attempt plus the `70` remaining chunk commits. These are
observations from one CI runner, not budgets.

**Logical backup and restore.** The backup was taken with the client matching
each matrix point — `pg_dump` and `pg_restore` `15.18` against server `15.18`,
and `18.4` against server `18.4` — as a custom-format archive of the metadata
and business schemas, `78160` and `78716` bytes respectively. It was restored
into a database created for the run, and everything afterwards ran against the
restored copy.

The state backed up was a chunk job with two attempts, the first resolved
through the audited operator path and the second live at position `15` of `25`,
plus a bounded partitioned flow job run to completion. The restored database
reported the same job instance identity, attempts and explorer projections,
definition revisions and manifest fingerprints, optimistic versions, step
attempts, durable checkpoints and execution contexts, counters, recovery
decisions, flow decisions, partition metadata, and enlisted business rows —
compared as equality over one reading, not row by row. The job then restarted
on the restored database, resumed its remaining `2` chunks in `114 ms` and
`77 ms`, and reached the same terminal observation as the uninterrupted run.

No correctness P0 or P1 is open against this campaign. The two findings below
are recorded; the first is a defect and it is closed.

### What this campaign does not establish

- **Schema upgrade and rollback.** The logical restore proves that a schema-3
  backup restores schema-3 state and that a job restarts on the restored copy.
  It does not upgrade a prior schema or roll one back. That is the
  [upgrade campaign](#upgrade-campaign) below, which delivers
  `schema1_and_schema2_upgrade_directly_to_schema3`,
  `schema2_runtime_rejects_schema3`, and
  `schema3_backup_restores_the_prior_schema`.
- **Physical backup, replication, or point-in-time recovery.** The campaign
  covers the logical backup the support bounds name and nothing wider.
- **Crash behavior of the database itself.** Every kill in this campaign is a
  client process. A server crash, a disk failure, or a lost volume is not
  simulated, and no claim here covers them.
- **A latency budget.** P-013 records discovery and resume timings from one CI
  runner. They are an observation, not a provisional budget, and the
  performance plan's regression gates do not consume them.

### Findings

**F3. Evidence-bound recovery discovery never worked against PostgreSQL.**
The first campaign run reached the durable inspection on both matrix points and
then failed at discovery with `Repository(Unavailable)`, a redacted error that
says nothing about its cause.
`PostgresExplorer::recovery_snapshot` read the `attempt` column, which the
schema declares as `integer`, through an `i64` accessor. The decode failed on
every row, so every proposal failed before it observed anything.

That is not a campaign-only path. `oxide-batch-cli` wires `RecoveryProposer`
over `PostgresExplorer`, so the documented operator recovery workflow could not
produce a proposal on the only supported database. It is a correctness P1, and
it is fixed: the column is read the way the execution projection beside it
already reads it. No SQL, durable format, transaction boundary, or public API
changed.

Nothing caught it because no PostgreSQL test had ever called
`recovery_snapshot`. `RecoveryProposer`'s coverage runs against the in-memory
explorer, and the shared PostgreSQL service contract does not reach that port.
The campaign now exercises it on both matrix points in every report, which is
the regression evidence.

**F4. Durable metadata classes belong to the definitions that declare them.**
The backup fixture first tried to write a flow decision and a partition plan
directly against the chunk job, so that one job would carry every durable
class. The repository refused the flow decision with `FlowStateCorrupt`, and
correctly: a decision may only be appended for a manifest that declares the
node and the transition, and a chunk manifest declares neither.

This is the accepted contract working, not a defect, and it is recorded because
it changed the campaign. The backup now covers a bounded partitioned flow job
run to completion alongside the chunk job, so the flow decisions and partition
metadata the restore is compared over are ones the runtime actually wrote. The
report asserts both are present before comparing them, so the comparison cannot
pass vacuously.

## Upgrade campaign

### What the campaign owes

The [performance plan](../engineering/performance-plan.md#m5-production-preview-campaigns)
gives the upgrade row three obligations: a direct upgrade from schema 1 and
schema 2 to schema 3, newer-schema rejection, and restore-based rollback. The
[design gate](m5-design-gate-evidence.md#named-campaign-scenarios) names all
three as scenarios: `schema1_and_schema2_upgrade_directly_to_schema3`,
`schema2_runtime_rejects_schema3`, and
`schema3_backup_restores_the_prior_schema`.

The denominator is committed as
[`tests/fixtures/upgrade/campaign-scope.json`](../../tests/fixtures/upgrade/campaign-scope.json).
It lists the two prior schemas the campaign builds fixtures at, the three
reports, and the five schema paths those reports must be observed taking.
`crates/oxide-batch/tests/m5_upgrade_campaign.rs` reconciles it against the plan
and the gate in an ordinary `cargo test`, so a path or a report cannot quietly
leave the campaign.

| Path | Report | From | To | Required outcome |
| --- | --- | --- | --- | --- |
| `upgrade-from-1` | `schema-upgrade` | `1` | `3` | Migrates, preserves every prior value, and opens through the current repository |
| `upgrade-from-2` | `schema-upgrade` | `2` | `3` | The same |
| `reject-schema-3` | `schema-rejection` | `2` | `3` | A schema-2 runtime refuses the upgraded database and writes nothing |
| `rollback-to-1` | `upgrade-rollback` | `1` | `3` | The pre-upgrade backup restores a schema-1 database carrying the state it was taken from |
| `rollback-to-2` | `upgrade-rollback` | `2` | `3` | The same, at schema 2 |

### The prior schemas are the prior schemas

This is the part of an upgrade campaign that is easiest to fake and worth the
most. A fixture built by installing schema 3 and lowering the recorded version
would pass an upgrade test and would prove nothing, because the upgrade would
have nothing to do.

The campaign does not build one. This crate's migration set is immutable, and
`0001_initial_metadata.sql` and `0002_fault_tolerance_and_flow.sql` are the
migrations that installed schema 1 and schema 2 when each was the whole schema.
A fixture is that set run up to the version under test and stopped, through
sqlx's own migrator, so the tables, columns, constraints, indexes, and
applied-migration bookkeeping are the ones that version produced. The upgrade
afterwards is the real remaining chain rather than a replay of it.

Three checks keep it that way. Before every upgrade the report requires the
recorded version to be the source version, every table that schema declared to
be present, and every table and column a later schema introduced to be
**absent** — so a fixture that drifted into being schema 3 fails the report that
depends on it. The scope reconciliation requires each declared source schema to
be one a migration in that set actually installs, and refuses a seed script that
so much as mentions a later schema's column. And the seed scripts are applied by
the database, whose constraints reject a row that does not belong to the schema
holding it.

The seeded state is what an upgrade has to carry: two registered definitions and
the upgrade edge between them, a job instance, a resolved attempt and a live
one, the step execution of each with its durable checkpoint, execution context,
and counters, and the recovery decision that resolved the first attempt. The
schema-2 fixture adds the durable state schema 2 introduced — the logical step
identity, the retry and skip counters, and a recorded flow decision. The live
attempt is the one that matters most: an upgrade that lost it would lose a
running job's restart point.

### How durable state is compared across a schema change

A prior schema cannot be read through the repository port, because the current
runtime refuses to open one. The comparison is therefore a direct row reading,
but taken through the column list the **source** schema declared, captured from
`information_schema` before the upgrade. A column a later schema adds is outside
the projection and cannot mask a lost or rewritten value.

The comparison is exact rather than tolerant. The chain rewrites no value of a
column that already existed, so anything but equality is a defect. The one
transformation it does make is asserted rather than tolerated: schema 2 gives
every schema-1 step execution a logical identity equal to its step name, and the
report requires exactly that, because a migration that invented one instead
would leave the step unaddressable across the definition change the identity
exists for.

Row equality alone would not be enough — a migration can leave rows intact and
unreadable — so each upgraded database is then opened through
`PostgresJobRepository`, its instance looked up by the identity the domain
computes rather than by a stored key, every attempt and step decoded into its
typed form, and every attempt projected through the explorer. Finally the
migrator is run a second time and required to change the recorded version,
the durable rows, and the port reading not at all.

### The rejecting runtime is a real schema-2 runtime

`schema2_runtime_rejects_schema3` is about an old release meeting a database a
newer migrator has moved on. That runtime cannot be built from this working
tree: the supported schema version is a constant of the crate, and this tree's
is `3`.

The report builds it. `397a38bcada93d961dbb2ca3d9960311a3fb4395` is the last
revision before schema 3 was added; its `SUPPORTED_SCHEMA_VERSION` is `2` and its
migration set ends at `0002`. The report checks it out into a worktree, builds
it, and runs the campaign's committed probe
([`tests/fixtures/upgrade/schema-2-runtime/probe.rs`](../../tests/fixtures/upgrade/schema-2-runtime/probe.rs))
against a database this crate's migrator has just upgraded from schema 2 to
schema 3. The scope reconciliation reads the supported version out of that
revision rather than asserting something about it, so a pin that moved to a
runtime supporting something else fails in review.

Both of that runtime's entry points are asked, because an operator rolling a
deployment back runs both: the repository a running instance opens, and the
migrator a deployment step runs. A schema-2 migrator that treated a schema-3
database as having pending work would rewrite it downwards.

Refusing is only half the contract, so the report also requires that nothing was
written while refusing — no durable value, no recorded schema version, no
applied-migration row — and that the current runtime still opens and projects
the database afterwards. That is what makes the refusal non-destructive rather
than merely unsuccessful.

The existing
`postgres_repository::newer_schema_is_rejected_without_guessing_compatibility`
is kept as the lower-level regression test and is **not** treated as this
scenario's evidence. It moves the recorded version one past whatever the current
runtime supports and requires the typed rejection, which proves the comparison
is wired up. It does not prove that the runtime which shipped against schema 2
refuses the schema 3 this crate now installs. The campaign does not run it,
because it needs a database already migrated to the current schema — the crash
and restore campaign's fixture rather than this one's, since every database this
campaign touches it creates itself — and the reconciliation requires it to keep
existing.

### The rollback is a restore, and says so

No downgrade migration exists in this repository and none was written. The M5
contract is restore-based rollback, and the report performs the operational
sequence: a prior-schema database is built and seeded, `pg_dump` writes a
custom-format archive of the metadata schema, the migrator upgrades the database
to schema 3, and the archive is loaded by `pg_restore` into a separate database
created empty for it.

Between the upgrade and the restore the upgraded database is used, through a
path that exists only in schema 3: a retention hold is placed through
`RetentionService`, which writes a column and an audit table schema 2 did not
have. Without that the restored copy and the upgraded one would differ by a
version number alone, and the report could not say the restore brought back the
earlier state rather than the later one.

The restored database must be at the source schema, must not declare a structure
schema 3 introduced, and must carry the durable state the reading taken
immediately before the archive was written recorded. The current schema-3
runtime must then **refuse** it, with `MigrationRequired` naming the version it
found. That requirement is what keeps the report honest: a rollback that
produced something the schema-3 runtime accepted would not be a rollback. And
nothing here claims the schema-3 state was converted to the prior schema — the
upgraded database is checked afterwards and still records schema 3, still holds
the hold, and still reports exactly what it reported before.

### Why it is a runner as well as a test

Every scenario needs a real database and returns success without one, because it
prints a skip line and returns. That half is `cargo xtask upgrade`, which
resolves the fixtures before starting and refuses to run without them.

An upgrade campaign has a sharper version of the forged-pass problem than the
others: a report that covered one source schema and silently skipped the other
would be green and would have proved half of what it claims. So each report
retains a machine-readable observation into a directory the runner creates
empty, and the runner reconciles the five declared schema paths against what
those observations record — the source and target schema version, the migration
result, what opening the database afterwards did, whether durable state was
compared, the backup and restore result where the path has one, and the version
finally observed. A path with no matching observation, or one whose observation
disagrees with the committed scope, fails the campaign.

### Where it runs

`postgres-15-upgrade-campaign` and `postgres-18-upgrade-campaign` in
`.github/workflows/ci.yml`, on the two ends of the supported PostgreSQL `15`-`18`
range. Two things separate these jobs from the other campaign jobs.

Their checkout is not shallow. The rejection report builds a previous revision
of this crate, and that revision is not in a shallow clone; the report fails
rather than skipping without it.

Their service database is never migrated by anything but the campaign. It
supplies the server, the role, and the connection parameters, and every database
a report works on it creates itself — because a report that ran against a
database something else had already migrated would not be a report about an
upgrade.

Each job installs the client tools matching its matrix point, because the
rollback report takes a real `pg_dump` archive and loads it with a real
`pg_restore`, and retains its report as a build artifact.

### Results

Both matrix points pass. All five schema paths were observed, both reports that
cover two source schemas covered both, and no report skipped.

| Report | Matrix | Result |
| --- | --- | --- |
| [`upgrade-campaign-postgres-15.json`](../engineering/campaigns/m5/upgrade-campaign-postgres-15.json) | PostgreSQL 15 | Passed |
| [`upgrade-campaign-postgres-18.json`](../engineering/campaigns/m5/upgrade-campaign-postgres-18.json) | PostgreSQL 18 | Passed |

Both were produced by commit `ce5fe10`, which is the merge commit the workflow
checked out rather than a branch tip, on `rustc 1.97.1` and Linux `x86_64`,
against servers `15.18` and `18.4`. The command is
`cargo run --package oxide-batch-xtask -- upgrade`.

**Schema 1 and schema 2 to schema 3.** Both upgrades succeeded in one migrator
invocation and left the recorded version at `3`, identically on both matrix
points. The schema-1 fixture carried `9` rows across `6` tables and the schema-2
fixture `10` rows across `7`; every value of every column the source schema
declared compared equal afterwards, and both step executions kept `import` as
their logical identity, which is what schema 2's one transformation of an
existing column is required to write.

Each upgraded database then opened through `PostgresJobRepository`. The seeded
instance was found by the identity the domain computes for it rather than by a
stored key, both attempts decoded — the resolved `FAILED` one and the live
`STARTED` one — the recovery decision on the first attempt survived, both
attempts projected through the explorer, and the schema-2 fixture's recorded
flow decision was still there. Running the migrator a second time left the
version, the durable rows, and that whole reading unchanged.

**Newer-schema rejection.** The runtime built from
`397a38bcada93d961dbb2ca3d9960311a3fb4395` reported
`supported_schema_version: 2`, which is the fact no build of this tree can
produce. Pointed at a database this crate's migrator had just carried from
schema 2 to schema 3, it refused through both entry points and reported the same
thing through each: `NewerSchema`, `observed_schema_version: 3`,
`supported_schema_version: 2`. The refusal was clean — the `10` tables compared
before and after were identical, the recorded schema version was still `3`, and
the applied-migration bookkeeping was untouched — and the current runtime still
opened and projected the database afterwards.

**Restore-based rollback.** Both rollbacks used the client matching their matrix
point — `pg_dump` and `pg_restore` `15.18` against server `15.18`, and `18.4`
against server `18.4`. The pre-upgrade archives were `35889` and `46171` bytes on
PostgreSQL 15, and `36176` and `46518` on PostgreSQL 18, for the schema-1 and
schema-2 sources respectively.

Each restored database came up at its source schema, declared no structure schema
3 introduced, and carried exactly the durable state the reading taken immediately
before the archive was written recorded. The current runtime refused each one:
`MigrationRequired { current: 1, supported: 3 }` and
`MigrationRequired { current: 2, supported: 3 }`. The upgraded databases were
unaffected by any of it — still at schema 3, still holding the retention hold
placed after the upgrade, and still reporting exactly what they reported before
the restore ran.

No correctness P0 or P1 was found by this campaign, and none is open against it.

### What this campaign does not establish

- **That an old runtime is safe on a database it accepts.** The rejection report
  proves a schema-2 runtime refuses schema 3 and writes nothing while refusing.
  It says nothing about mixed-version operation that the runtime does not
  refuse, and the M5 support contract does not offer any.
- **A downgrade migration.** There is none, and the campaign does not imply one.
  Rolling back means restoring the backup taken before the upgrade, which loses
  everything written after it — by construction, and the report demonstrates
  exactly that by leaving the post-upgrade hold behind.
- **Rollback of an upgrade whose backup was not taken.** The contract is
  restore-based, so a backup is the precondition. The campaign proves the
  restore, not a recovery path for an operator who skipped it.
- **Upgrade under load.** Every upgrade here runs against a quiescent database.
  Migration behavior with concurrent runtime traffic, lock contention, or a
  partially applied chain interrupted mid-flight is not covered, and the
  advisory lock the migrator takes is not exercised against a competing
  migrator.
- **Physical backup, replication, or point-in-time recovery.** The campaign
  covers the logical backup the support bounds name and nothing wider.
- **A duration budget.** The campaign records outcomes, not timings. The
  fixtures are small on purpose, so nothing here says how long an upgrade of a
  production-sized database takes.

### Findings

No defect was found. Two observations changed the campaign and are recorded
because they are the reason it is shaped the way it is.

**F5. A schema-2 runtime cannot be built from a tree that installs schema 3.**
The first design of the rejection report intended to reuse the existing
newer-schema mechanism: set the recorded version above what the runtime
supports and require the typed failure. That is what
`newer_schema_is_rejected_without_guessing_compatibility` already does, and it
cannot discharge this scenario. With `SUPPORTED_SCHEMA_VERSION` at `3` the
comparison it exercises is `4` against `3`, not `3` against `2`, and the
database it exercises it against is a schema-3 database wearing a higher number
rather than a schema-3 database. The scenario the gate names is about a runtime
that shipped, so the report builds that runtime from the revision before schema
3 existed. The cost is that the campaign needs the repository's full history and
a build of a previous revision; the alternative was a report that named schema 2
without ever running one.

**F6. `installed_schema_version` postdates the schema-2 runtime.**
The probe was first written to report the version it found through
`PostgresMigrator::installed_schema_version`, which does not exist at the pinned
revision — it was added afterwards. The probe uses only the API that revision
exposed, and the version it found is read out of the typed failure itself, where
`NewerSchema` carries `current` alongside `supported`. That is a better reading
anyway: it is the runtime's own account of what it saw, rather than a second
observation taken beside it.
