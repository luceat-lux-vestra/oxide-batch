# M5 Evidence Campaign Record

**State:** In progress. The conformance, crash-and-restore, upgrade, security,
resource-bound, and soak campaigns are delivered; the cancellation,
performance, and reference-workload campaigns are not.

**Issue:** [#102](https://github.com/luceat-lux-vestra/oxide-batch/issues/102)

**Date:** 2026-08-10

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
| Security | `verify_full_tls_is_required_in_the_supported_mode`, `least_privilege_role_cannot_exceed_its_class`, `redaction_sweep_finds_no_prohibited_value_class` | Delivered |
| Resource bounds | `declared_ceilings_hold_under_stress_with_backpressure`, plus the bounded query-path, payload, and shedding reports | Delivered |
| Soak | `soak_reports_no_task_connection_handle_or_memory_growth` | Delivered |
| Cancellation | `operator_stop_reaches_a_durable_terminal_status`, `cancellation_latency_is_measured_separately_per_phase`, `drain_reports_unjoined_tasks_at_every_declared_deadline`, `restart_after_cancellation_resumes_without_rerunning_committed_work` | Delivered |
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

## Security campaign

### What the campaign owes

The design gate's security row promises three things about the M5 preview:
that `verify-full` TLS is the supported production transport, that privilege is
separated across migration, runtime, explorer, operator, and retention, and
that no prohibited value class reaches a diagnostic surface. Each is one named
scenario, and each is a claim about what does *not* happen, which is what
shapes the campaign.

| Scenario | What it must show |
| --- | --- |
| `verify_full_tls_is_required_in_the_supported_mode` | The supported configuration connects only when the certificate chain and the host name both validate, and refuses rather than continuing unencrypted when they do not. |
| `least_privilege_role_cannot_exceed_its_class` | Each of the five classes performs its own work through the path an operator uses, and is refused outside it by the database. |
| `redaction_sweep_finds_no_prohibited_value_class` | No canary injected as a password, a database endpoint, a certificate, or a payload appears in any artifact the milestone can put in front of an operator. |

The denominator is committed as
[`tests/fixtures/security/campaign-scope.json`](../../tests/fixtures/security/campaign-scope.json),
and the least-privilege policy is committed as SQL beside it, in
[`roles.sql`](../../tests/fixtures/security/roles.sql) and
[`grants.sql`](../../tests/fixtures/security/grants.sql), so what each class may
reach is reviewable on its own rather than assembled inside a test.

### Why the fixture is more than a database

Three of the campaign's claims cannot be checked against an ordinary server, so
[`provision.sh`](../../tests/fixtures/security/provision.sh) builds what a
connection string cannot carry.

A certificate refusal needs an authority that really signed nothing the server
presents, so a second authority is generated and signs nothing. A host-name
refusal needs a name that reaches the same server and is not in its
certificate, so the certificate carries `DNS:localhost` and no address, and the
mismatch attempt connects to `127.0.0.1` — the same server, the same trusted
chain, and the name as the only difference.

The third is the one that carries the claim. The assertion that the supported
mode does not fall back needs a reachable server offering no TLS at all, which
is a second container with `ssl=off`. Without it the campaign would show only
that a bad certificate is refused, and a client that quietly continued
unencrypted whenever TLS was unavailable would show exactly that too.

This is also why the CI job declares no `services:` block. A service container
starts before any step has run, and therefore before the certificate the server
must present exists.

### The refusals are classified, not merely counted

`PostgresJobRepository::connect` reports every connection failure as one
redacted `Unavailable`, by design. That is right for an operator and useless
for this report: an attempt built around an untrusted authority that actually
failed on the host name would be green and would prove nothing about
certificate validation.

So each refusal is corroborated at the transport layer and required to carry
the reason the attempt was built to provoke, and the transport probe and the
production path are required to agree on every attempt so the two cannot drift
apart. The probe's error text is never retained — it can carry the host, the
port, and the name a certificate was issued for — only the class it maps to.

### The matrix is real operations, on both sides

The allowed half of the privilege matrix is not `has_table_privilege` lookups.
The migrator migrates, the runtime builds an execution graph through
`JobRepository`, the explorer answers through `JobExplorer`, the operator
applies a guarded stop through `JobOperator`, and retention holds, releases, and
plans a purge through `RetentionService`, so a grant that is present but
unusable fails the report.

The forbidden half requires `42501` exactly rather than any failure. An `INSERT`
a class may not perform and an `INSERT` that violates a constraint both merely
fail, and a matrix that asked only whether the statement failed would pass with
the privilege wide open. No forbidden statement can change anything if it
unexpectedly succeeds: the destructive ones match no row and the inserting ones
select none, so a passing cell and a failing cell leave the database identical.

Two boundaries needed column granularity. The operator may move an execution's
status and ask it to stop but may not write `owner_token`, so it cannot claim an
execution a live runtime holds. Retention may write an instance's hold columns
and the runtime may write its identity columns, and neither may write the
other's.

### The sweep injects before it scans

The campaign's redaction evidence is a sweep rather than a set of per-value
assertions, because the per-value shape rots silently: an assertion that an
artifact does not contain a string nothing in the run ever produced passes
forever, including after the redaction is removed.

The sweep generates one canary per prohibited value class, each naming its own
class and carrying a per-run suffix, injects each through a place that really
accepts a value of that class, collects every artifact the milestone produces,
and scans all of them for all of the canaries. Structured artifacts are scanned
twice — over the serialized bytes and over every string reachable in the parsed
document — because a value escaped on the way out survives the first scan and a
value carried in framing survives the second.

It also requires what is safe to survive. Redaction that worked by deleting
diagnostics would pass every scan and leave operators worse off, so the sweep
requires the configuration to still list its keys and still mark the withheld
ones redacted, and the instance projection to still name the parameter the
payload arrived in and still report its type.

### Why it is a runner as well as a test

Two of the three scenarios need a real database and return green without one,
because they skip. `cargo xtask security` resolves the fixtures first and fails
before any target starts when one is absent.

Passing tests are not sufficient either, because everything the campaign proves
is negative: a report that connected once and attempted no refusal, a matrix
that filled in one class, or a sweep that injected nothing would all be green.
So each report retains an observation into a directory the runner creates empty,
and the runner requires the substance — the three refusals and their classes,
every class on both sides of its boundary and once through a service path, every
refusal carrying `42501`, and every surface and value class covered with nothing
found.

Each database report must also name the PostgreSQL major it ran against, because
a matrix point is invisible in a connection string and an observation from one
supported major would otherwise reconcile perfectly inside a run of another.

### Where it runs

`postgres-15-security-campaign` and `postgres-18-security-campaign` in
`.github/workflows/ci.yml`, on the two ends of the supported PostgreSQL `15`-`18`
range, each retaining its report as a build artifact on success and failure
alike.

The M2 `postgres-design-gate` axis is untouched and keeps running over `15`
through `18`. It is not this campaign renamed: it predates the adapter,
exercises a draft schema through `psql`, and separates four roles rather than
the five the M5 preview does.

### Results

Both matrix points pass. All three scenarios ran and none skipped.

| Report | Matrix | Result |
| --- | --- | --- |
| [`security-campaign-postgres-15.json`](../engineering/campaigns/m5/security-campaign-postgres-15.json) | PostgreSQL 15 | Passed |
| [`security-campaign-postgres-18.json`](../engineering/campaigns/m5/security-campaign-postgres-18.json) | PostgreSQL 18 | Passed |

Both were produced by commit `9a87a35`, which is the merge commit the workflow
checked out rather than a branch tip, on `rustc 1.97.1` and Linux `x86_64`,
against servers `15.18` and `18.4`. The command is
`./tests/fixtures/security/provision.sh <major>`, which provisions the fixture
and runs `cargo run --package oxide-batch-xtask -- security`.

**`verify-full` TLS.** The supported configuration — `PostgresConfig` with the
default TLS mode, no production-mode switch anywhere — connected once and was
refused three times, identically on both matrix points. The successful session
negotiated `TLSv1.3`, and that it was encrypted was read from the server through
a separate administrative connection rather than from the client's account of
itself: `pg_stat_ssl` reported the adapter's live backend as encrypted, and no
unencrypted backend existed on the adapter's behalf at any point in the report,
on either server.

Each refusal carried the reason it was built to provoke. The untrusted authority
failed on the issuer, the mismatched name failed on the name, and the server
offering no TLS failed because no TLS was offered rather than by continuing
without it. The configuration surface also refused `sslmode=disable`,
`sslmode=prefer`, and `sslrootcert` in the connection string, which is the only
other way a deployment could express a weaker transport.

**Least-privilege separation.** All five classes were provisioned from the
committed policy on schema 3, and the matrix recorded `40` cells: `10` allowed
and `30` forbidden, with every class appearing on both sides and once through
the path an operator uses. Every forbidden cell was refused under `42501` and no
other code.

Read back from `pg_roles` rather than assumed from the script that created them,
no class holds `SUPERUSER`, `CREATEDB`, `CREATEROLE`, `REPLICATION`, or
`BYPASSRLS`. `PUBLIC` holds no privilege in the metadata schema — the query that
looks for one returned nothing — and the migration bookkeeping is readable by
none of the four non-migrating classes.

**Redaction sweep.** Four value classes were injected — a password and a
database endpoint through the repository URL, a certificate through the CA
setting, and a payload as an identifying job parameter value — through both the
environment and a configuration file. `41` artifacts across the four surfaces
were collected and `483` strings scanned. Prohibited occurrences: `0`.

The diagnostics survived: the bundle still reported `9` configuration keys with
`2` marked redacted, and the instance projection still named `business_key` and
still reported its type while carrying no value.

No correctness P0 or P1 was found by this campaign, and none is open against it.

### What this campaign does not establish

- **That a deployment is configured this way.** The campaign proves the
  supported configuration validates certificates and host names and refuses to
  continue unencrypted. It cannot prove an operator did not set
  `TlsMode::Plaintext`, which the API still offers for an explicitly isolated
  environment and which the support bounds already describe as unsupported in
  production.
- **Client certificates or mutual TLS.** The campaign covers server
  authentication. `PostgresConfig` exposes no client certificate, and the M5
  support contract does not offer one.
- **Certificate expiry, revocation, or rotation.** The fixture's certificates
  live for a day and are never rotated. Nothing here says what happens when a
  chain expires mid-pool or when an authority publishes a revocation.
- **That the committed policy is the policy a deployment installs.** The
  campaign proves the policy separates the classes it describes. The operations
  documentation owns telling an operator to install it, and no mechanism forces
  the two to agree.
- **Row-level security or column-level reads.** The separation is table and
  column granular for writes. No class is prevented from reading a row of a
  table it may read at all.
- **That every diagnostic surface exists in the sweep.** The sweep proves a
  property of the artifacts it collects. A surface added later is covered only
  once it is collected, and the runner requires the four the gate names rather
  than proving the list is complete.
- **Redaction under an unbounded value.** The canaries are short strings. A
  value large enough to be truncated, chunked, or re-encoded on the way out is
  not exercised.

### Findings

No defect was found in the product. Three observations changed the campaign and
are recorded because they are the reason it is shaped the way it is.

**F7. The existing bundle redaction test asserts about values it never
produces.** `diagnostic_bundle_excludes_every_prohibited_value_class` requires a
bundle to contain neither `context-payload-sentinel` nor
`checkpoint-payload-sentinel` nor `SELECT sentinel SQL` nor
`user-error-text-sentinel`. No code in this repository produces any of those
strings, so those four assertions have been passing without exercising anything
and would keep passing if the redaction were removed. The test is kept — it does
cover the values it really injects — and the sweep exists because that failure
mode cannot be fixed by adding more assertions of the same shape. The scope
document records it as evidence the campaign keeps and does not stand in for.

**F8. A refused connection cannot say why it was refused, and should not.** The
adapter maps every connection failure to one redacted `Unavailable`, so the TLS
report cannot learn from the production path whether a refusal was about the
certificate, the name, or the absence of TLS. The alternative — classifying
failures at the facade — would put the host, the port, and the certificate's
subject into a diagnostic, which is the thing the redaction sweep exists to
prevent. The report corroborates at the transport layer instead, retains only
the class, and requires the probe and the production path to agree on every
attempt so the corroboration cannot drift into describing a different
connection.

**F9. Row locking is a privilege, and the runtime needs it on the instance
table.** The first policy gave the runtime `SELECT, INSERT` on
`ob_job_instance` and no `UPDATE`, on the reasoning that it writes no column of
an existing instance. Creating an attempt takes `SELECT ... FOR UPDATE` on the
instance row so that two launches cannot both decide they are the first, and
PostgreSQL requires `UPDATE` on at least one column to take that lock. The grant
is confined to the identity columns the runtime writes when it creates the
instance, which leaves the hold columns to retention and keeps the boundary the
matrix checks in both directions.

## Resource-bound campaign

### What the campaign owes

The performance plan's resource-bounds row promises a declared-ceiling proof
for every queue, retry cache, page, buffer, worker assignment, and result set
the framework owns, with backpressure propagation under stress. The design gate
names one scenario for it; the campaign delivers that one and three more,
because those six words name six different kinds of resource and no single
scenario can be about all of them.

| Report | Scenario | What it must show |
| --- | --- | --- |
| Worker assignment | `declared_ceilings_hold_under_stress_with_backpressure` | The worker and branch sets fill to their ceilings and no further under a load several times their size, a pool one connection short is refused before any child exists, and the stressed run leaves the sequential baseline's durable record. |
| Bounded query paths | `bounded_query_paths_stay_bounded_as_history_grows` | Pages, encoded responses, cursors, and purge batches stay bounded against a history several times larger than any of them, and a bounded traversal loses and repeats nothing. |
| Bounded payloads | `bounded_payloads_are_refused_one_byte_over_the_ceiling` | Every buffer and the retry cache accept their declared ceiling and refuse one unit past it, and the durable ones come back byte-identical. |
| Bounded shedding | `bounded_queues_shed_under_overload_without_blocking_batch_work` | Every queue keeps its bound under an offered overload, sheds under the rule it contracts for, and does not block batch work while doing it. |

### The denominator is most of the work

Every campaign on this page needs a denominator. This one needs a different
kind, and it is worth being explicit about why.

The other campaigns enumerate obligations that are written down somewhere:
ledger rows, commit phases, schema paths, privilege classes. A document can
list them and review can check the list against its source. The obligation here
is *every bounded resource the framework owns*, and that set is defined by the
code rather than by any document. A campaign that proved nine ceilings out of an
unstated number of them would look exactly like a complete one.

So the denominator is committed as
[`tests/fixtures/resource-bounds/campaign-scope.json`](../../tests/fixtures/resource-bounds/campaign-scope.json)
and reconciled in **both directions** by an ordinary `cargo test`, in
[`m5_resource_bounds_campaign.rs`](../../crates/oxide-batch/tests/m5_resource_bounds_campaign.rs).

From the code outward, the reconciliation *parses* every library crate and
requires each constant declared under the repository's
[bound declaration convention](../engineering/coding-conventions.md#declaring-a-resource-bound)
to be classified: as a resource with a proving report, or as out of scope with a
reason a reviewer can disagree with.

It parses rather than reads lines, and that is not a detail. A textual reader
recognizes the spellings its author happened to think of — it sees `pub const`
and misses `pub(super) const`, it sees a declaration that fits on one line and
misses the same one after a formatter wraps it — and both failures are silent,
and both would remove a resource from the denominator without removing it from
the product. So visibility is not consulted at all, layout cannot be, and
associated constants and constants inside inline modules are found, because
`FaultStateEnvelope::MAX_ENTRIES` is one and missing it would drop the retry
cache.

**What that does and does not guarantee** is worth stating exactly, because it
is easy to claim more. A constant declared under the convention cannot enter the
product without entering the campaign. A ceiling written as a bare literal at
the point it is enforced, or named outside the convention, or existing only
after macro expansion, is invisible to the scan — those are ruled out by the
convention being a documented rule that review applies, not by the scan itself.
The campaign does not claim to discover every bounded resource automatically,
and the convention exists so that the claim it does make has something to be
measured against.

From the operator's document inward, it requires the
[capacity budget](../operations/capacity-and-resource-budgets.md#declared-bounds)
table and the scope to say the same thing about the same resources, and the
scope's numbers to be the numbers the code holds. That table is what a
deployment is sized from; a number there the code does not hold is worse than
no number.

Both directions found something, recorded below as F10 and F11.

The current denominator is `36` resources across the six classes, plus five
explicit exclusions. The exclusions carry reasons rather than silence, because a
reader cannot otherwise tell an unexamined resource from an out-of-boundary one:
operation timeouts and staleness thresholds bound *when* something happens
rather than *how much* is held; retry and backoff limits bound a policy a
definition declares rather than a resource the framework holds; and application
readers, writers, and item buffers are outside the framework boundary, which the
capacity budget already says.

### Four overload policies, not one

The campaign does not force one shape onto every resource, and could not
without breaking a contract.

- **Fail-closed** refuses before work starts, and nothing may be partially
  launched behind it.
- **Bounded concurrency** admits the work and holds occupancy at the ceiling, so
  a backlog waits rather than growing the worker set.
- **Bounded shedding** keeps the bound and discards under a stated rule while
  never blocking batch progress.
- **Bounded truncation** returns less than was asked for and says so, through a
  continuation cursor or a recorded truncation.

The telemetry exporter queue is where the distinction is load-bearing.
Telemetry may not block batch work — that is the accepted contract — so a full
queue cannot apply backpressure the way a bounded worker set does. Making it do
so to keep the campaign uniform would break the contract rather than strengthen
the evidence.

The shedding rules differ from each other too, and the scope records which one
each queue contracts for, because dropping the wrong record would otherwise
reconcile. The exporter queue drops the **newest**, since it exists to shed a
burst and the records already in it are the ones about to be exported. The
incident buffer evicts the **oldest**, since it exists to be read after a
failure. The cardinality guard collapses an unseen combination into one reserved
series and counts it, so the series count stays finite while the observation is
still made.

### Reaching a ceiling is the evidence; respecting one is not

This is the failure mode the campaign is shaped around, and the other campaigns
do not have it.

A bound can be *reported* as holding by a run that never approached it. A worker
budget of `64` whose observed peak was `3`, a page bound checked against four
rows, a queue that was never filled and therefore dropped nothing — all three
are green, and none is evidence about a ceiling.

So every report records what it offered and what the framework held, and the
runner requires the two to be in the relation the resource's policy implies:
peak equal to ceiling for a live occupancy, offered greater than ceiling with a
non-zero shed count for a queue, and more available rows than the page or batch
bound admits for a bounded query.

Reaching a live ceiling deterministically needs a device. Each wave of workers
is held at a barrier sized to the ceiling, so a framework that admitted fewer
never completes a wave, times out, and fails on the peak it actually saw rather
than hanging; a framework that admitted more trips the gauge.

Two ceilings could not be saturated in the ordinary sense, and both are recorded
as what they are rather than papered over — F13 and F14.

### The stressed run is compared, not just counted

The performance plan holds that a concurrency result which changes a durable
observation is invalid regardless of its throughput. So the worker report runs
the same `128` partitions twice — once with a budget of `64` and once one child
at a time — and compares the durable record field by field. Thirteen fields:
the job and step statuses and exit statuses, the aggregate counters and the six
individual ones, the step-execution count, the partition key set, the
per-partition status, counters, and context, and the partition count.

Nothing here is timed, and no duration is compared against a threshold.
Throughput at these scale points is the P-010 measurement's and is not claimed.

### Why it is a runner as well as a test

Three of the four reports need a real database and return green without one,
because they skip. `cargo xtask resource-bounds` resolves the fixtures first
and fails before any target starts when one is absent.

Passing reports are not sufficient either, for the reason above, so each retains
an observation into a directory the runner creates empty and the runner requires
the substance: that every declared resource was observed by some report, that
the ceiling each report checked is the one the denominator declares, that every
live ceiling was reached and every shedding resource shed, that the fail-closed
rejection left no row behind, and that the durable comparison ran and agreed.

Writing those checks found five gaps in the reports as first drafted — two
resources proved by nothing, two comparison fields the denominator named and the
report did not compare, and a split run reported as saturated when it had
nothing to hold back. That is the argument for the runner in one sentence.

Each database report must also name the PostgreSQL major it ran against, because
a matrix point is invisible in a connection string and an observation from one
supported major would otherwise reconcile perfectly inside a run of another.

### Where it runs

`postgres-15-resource-bound-campaign` and `postgres-18-resource-bound-campaign`
in `.github/workflows/ci.yml`, on the two ends of the supported PostgreSQL
`15`-`18` range, each retaining its report as a build artifact on success and
failure alike.

### Results

Both matrix points pass. All four reports ran and none skipped. `36` declared
resources, `36` observed.

| Report | Matrix | Result |
| --- | --- | --- |
| [`resource-bounds-campaign-postgres-15.json`](../engineering/campaigns/m5/resource-bounds-campaign-postgres-15.json) | PostgreSQL 15 | Passed |
| [`resource-bounds-campaign-postgres-18.json`](../engineering/campaigns/m5/resource-bounds-campaign-postgres-18.json) | PostgreSQL 18 | Passed |

Both were produced by commit `3a4a962`, which is the merge commit the workflow
checked out rather than a branch tip, on `rustc 1.97.1` and Linux `x86_64`,
against servers `15.18` and `18.4`. The command is
`cargo run --package oxide-batch-xtask -- resource-bounds`.

Every structural figure below is identical on the two matrix points, which is
what the campaign claims: it asserts occupancy, counts, and refusals rather than
durations, so a supported server that changed one of them would be the finding.
The figures were also identical on the development host the campaign was written
against, PostgreSQL `18.4` on macOS `aarch64`.

**Worker and connection ceilings.** `128` partitions were offered against a
worker budget of `64` through a pool of exactly `65` connections. Peak
occupancy: `64`, in two full waves, with all `128` admitted — the ceiling held
by bounding concurrency rather than by dropping work — and zero workers still
holding when the step finished.

The branch set was run twice for the reason in F13: budgeted at `4` with `8`
branches offered, where the peak was `4`, and budgeted at the declared ceiling
of `8`, where the peak was `8`.

A pool one connection short of the derived `concurrent_children + 1` was refused
as `InsufficientPoolCapacity { required: 9, configured: 8 }`, and the refusal
was confirmed as an absence: zero rows in `ob_job_instance`, `ob_job_execution`,
and `ob_job_definition` afterwards. The same step against a pool of exactly `9`
completed.

The stressed run and the one-child-at-a-time baseline agreed on all thirteen
compared fields over all `128` partitions.

**Bounded query paths.** Against `1200` seeded instances, six pages returned all
`1200` rows with `0` duplicates. The largest page held `202` rows against a
`500`-row bound, because the encoded response bound is what stopped it: the largest
page encoded `261994` bytes against `262144`, and five of the six pages were
truncated by it and handed back a continuation cursor. Cursors were `59` bytes
against a `256`-byte bound at every point in the traversal. A purge planned
against `1200` eligible candidates with a batch bound of `100` took exactly
`100` and deleted exactly `100` instances and `100` executions.

**Bounded payloads.** Every buffer and every retry-cache bound accepted its
declared ceiling and refused one unit past it. The durable ones round-tripped:
a `128`-byte partition key and a `4096`-byte partition context were written
through the adapter and came back identical. An instance key digested from
`1310720` bytes of identifying parameters was refused against the `1048576`-byte
input ceiling. A `64`-edge upgrade chain was walked and a `65`-edge one refused.

**Bounded shedding.** `256` records offered to a `64`-record queue: peak depth
`64`, `192` dropped — the excess exactly — with the queue's own counter agreeing
and the drop report throttled rather than emitted per drop. `400` label
combinations offered to a `200`-series family budget: `200` series retained,
`201` collapsed into the reserved series and counted. `600` events emitted for
one execution against a `200`-event buffer: `200` returned. A `1072043`-byte
response was truncated to `261750` bytes and said so.

A launch then ran with its exporter queue full from the first record and left
the same durable record as one with room, which is what makes shedding
acceptable rather than data loss with a counter attached.

No correctness P0 or P1 was found by this campaign, and none is open against it.
Two P2 documentation defects were found and fixed, recorded as F10 and F11.

### What this campaign does not establish

- **Any throughput, latency, or memory number.** Nothing here is timed and no
  duration is compared against a threshold. Per-page latency, rows examined,
  index selection, scaling efficiency, and peak resident memory belong to the
  performance campaign and the P-010 and P-012 measurements.
- **That the bounds are the right bounds.** The campaign proves the framework
  holds the ceilings it declares. Whether `64` concurrent workers or a `4 KiB`
  partition context is the right ceiling for a deployment is a sizing question
  the capacity budget answers, provisionally.
- **Behaviour above the largest configured value.** Every ceiling is proved at
  its declared maximum. A deployment configured below one is bounded by its own
  configuration, which the campaign checks only at the values it runs.
- **That a bound holds under a fault.** The stressed runs complete. What a
  bounded worker set does when a child panics, a connection drops, or the
  process is killed mid-wave is the crash-and-restore campaign's, and the
  cancellation campaign owns the drain.
- **Resource behaviour over time.** No leak, growth, or accumulation claim is
  made. Twelve minutes of stress says nothing about twelve hours; that is the
  soak campaign's report.
- **The `4 MiB` diagnostic bundle ceiling as a truncation.** No M5 input
  produces a bundle anywhere near it, so it is proved as an upper limit that
  held rather than as a truncation that happened. See F14.
- **Skip and retry counter equivalence under concurrency.** The compared
  counters are the six an execution carries. Skip and retry counters live in the
  durable fault state, and this campaign's workers are tasklets that produce
  none, so a field comparing them would agree between two runs that both
  produced nothing.

### Findings

No defect was found in the product. Two documentation defects were found and
fixed, and four observations changed the campaign and are recorded because they
are the reason it is shaped the way it is.

**F10 (P2). The capacity budget declared a partition-key bound the code has
never held.** The declared-bounds table gave the partition key as `256` bytes.
`MAX_PARTITION_KEY_BYTES` has been `128` since the bound was introduced in
[#89](https://github.com/luceat-lux-vestra/oxide-batch/pull/89), so the number
an operator would have sized against was never the number the framework
enforces. The document is corrected. This is the drift the code-outward and
document-inward halves of the reconciliation exist to catch, and it is now
caught by an ordinary `cargo test` rather than by reading.

**F11 (P2). The capacity budget had no row for the retry cache.** The
performance plan names six resource classes that must have a finite bound, and
"retry cache" is one of them. The declared-bounds table had a row for none of
it. The durable fault state *is* that cache — a bounded envelope of unresolved
retry keys that commits with the chunk — with a `256`-entry and `64 KiB`
ceiling, and both are now declared where an operator will look for them, with a
note that it is a durable-format ceiling rather than a cache in front of a
store.

**F12. The declared node and transition ceilings are not independently
reachable.** `MAX_NODES` is `1024` and `MAX_TRANSITIONS` is `4096`, and a
definition author meets neither: the canonical manifest crosses its own `64 KiB`
ceiling first, at a chain of `158` steps whose manifest is `65248` bytes. All
three bounds are real and all three refuse, and none of them is wrong. But the
node count is not the capability it reads as, so the campaign finds the binding
bound by bisection rather than assuming it, and records which one an author
actually meets.

**F13. The split-branch ceiling cannot be saturated in the ordinary sense.** A
split may declare at most eight branches and its budget may admit at most eight,
so at the declared ceiling there is nothing left over to hold back. A run there
proves the branches all ran, not that the budget bounds anything. The report
therefore runs twice — budgeted at four with eight offered, which is a real
backlog, and budgeted at the ceiling, which shows the declared ceiling is
reachable — and the evidence carries both with the budgeted run as the
observation.

**F14. The diagnostic bundle's ceiling is not reachable through any M5 input.** A
bundle is built from a bounded configuration, a bounded execution projection,
and a bounded incident buffer, and all three together came to `4546` bytes
against a `4 MiB` ceiling. The campaign records the observed size beside the
ceiling and claims an upper limit that held rather than a truncation that
happened, because a report that claimed saturation here would be claiming
something it did not produce.

**F15. A telemetry record cannot be given an execution identifier from
outside.** The incident buffer's per-execution bound is defined in terms of the
identifier a record carries, and no public constructor attaches one — only the
services that already know an execution emit records that carry it. Fabricated
records would have filled the buffer with events belonging to no execution, and
`events_for` would have returned nothing for any identifier: a bound that looks
held because nothing was ever offered to it. The report drives `600` real
paginated reads instead. This is not a defect — a record's execution identity
should come from whatever observed the execution — but it is the reason that one
report is more expensive than it looks.

## Soak campaign

### What the campaign owes

The performance plan's soak row promises P-015 across repeated launch,
shutdown, restart, and recovery cycles, reporting task, connection, handle, and
memory growth over the declared duration. The design gate names one scenario for
it, and the campaign delivers that one.

| Report | Scenario | What it must show |
| --- | --- | --- |
| Soak | `soak_reports_no_task_connection_handle_or_memory_growth` | Over a declared window of repeated launch, fault, restart, recovery, and drain cycles against one PostgreSQL pool, every cycle leaves the first measured cycle's durable record, and framework-owned tasks, pooled connections, process handles, and resident memory do not accumulate under rules declared before the run. |

### The denominator is the period

Every campaign on this page needs a denominator, and this one needs a kind the
others do not.

The other campaigns enumerate obligations that exist independently of the run: a
ledger row, a commit phase, a schema path, a privilege class, a declared
ceiling. A report either covered one or it did not, and the report itself says
which. This campaign's obligation is *a period*. Nothing outside the campaign
says how long a soak should be, which means a soak that ran three cycles and a
soak that ran three hundred produce reports of exactly the same shape, both
green — and the shorter one produces the flatter series, and therefore the more
convincing result. A soak is the one campaign here whose evidence gets *better*
looking as it does less.

So the period is committed as
[`tests/fixtures/soak/campaign-scope.json`](../../tests/fixtures/soak/campaign-scope.json),
and all three consumers read it rather than restating it:

- **the report** takes its cycle counts, workload shape, correctness
  obligations, and growth rules from it, so there are no constants in the test
  that could disagree with the document;
- **the runner**, `cargo xtask soak`, reads it independently and requires the
  run to have matched it;
- **the reconciliation**,
  [`m5_soak_campaign.rs`](../../crates/oxide-batch/tests/m5_soak_campaign.rs),
  checks the document against the accepted plan and the design gate in an
  ordinary `cargo test`, so a shrinking denominator is caught in review.

The declared window is `32` warmup cycles and `600` measured cycles. Each cycle
launches a `16`-partition step with a worker budget of `4` through a pool of
exactly `5` connections, fails one partition, restarts, recovers, and drains
`4` owned tasks. That is `1264` launches, `632` restarts, `632` drains, and
`10744` partition executions per matrix point.

The reconciliation also holds the shape of the window rather than only its
existence: warmup and measurement must both be non-empty, the minimum sample
count must equal the declared measured window so a short run cannot pass, the
measured window must be at least as long as warmup, and exactly one termination
condition may be allowed — a soak with a second way to stop can stop early and
still be green.

### Why it is not the M4 measurement rerun

The M4 measurement `p015_shutdown_restart_soak` already runs this shape of
cycle, and it fixed the per-cycle semantics this campaign keeps: every owned
task joined, the same repository work each cycle, the same durable observation
each cycle, no re-run of a committed partition. It stays exactly where it is,
on the in-memory repository, and the reconciliation asserts that it does —
including that it still uses `InMemoryJobRepository`, because the cheapest way
to appear to deliver an M5 soak would be to move that measurement under a
database fixture and relabel the result.

What it cannot supply is production-preview evidence, and the reason is
structural rather than a matter of scale. It builds a fresh in-memory repository
every cycle, which resets the two observations a production-preview soak is
mostly about — pooled connections and process handles — at every cycle boundary,
and it takes its resident reading over a process that holds no pool at all. This
campaign opens one PostgreSQL pool before the first cycle and closes it after
the last, and every boundary sample is taken against that one pool.

### Correctness first, then resources

A soak is not a memory profiler. Resource flatness over a workload that stopped
doing the work is the easiest green in this document to produce by accident, so
every cycle's durable record is compared against the first measured cycle's
before any resource number is consulted, and a cycle that differs fails the
campaign whatever its trajectory was.

Fifteen obligations are declared and decided per cycle: the terminal job and
step statuses and exit statuses; the six execution counters individually as well
as in aggregate; the partition count, the partition key set, and every
partition's terminal state, exit status, and counters; the restart position; the
reuse of committed work; the absence of duplicate and of missing durable work;
that the failed attempt was recorded as failed rather than forged into a
success; that the restart followed the accepted recovery path; that no worker
outlived its parent; that the drain joined everything; and that every cycle
began the same number of repository transactions.

The declared set and the decided set are reconciled in both directions. A
declared obligation the report does not decide is a violation, and so is a
decision the report makes that the scope does not declare.

### The fault waits rather than fires

Making every cycle leave the *same* durable record is harder than it sounds, and
getting it wrong would have made the whole comparison above unusable.

A sibling stop in the partitioned runtime is cooperative, and it is consulted
only *before* a worker's tasklet is invoked. A fault that fired on a timer would
therefore stop a scheduling-dependent number of not-yet-started siblings: one
cycle would commit fourteen partitions and the next fifteen, and every durable
comparison in the campaign would fail for a reason that has nothing to do with
the framework.

So the injected worker waits until every sibling has returned, and only then
fails. The failing key is the last one and the budget is smaller than the
partition count, so that worker is the last to start and every sibling is
already in flight when it begins waiting — it cannot deadlock against the budget
whose slot it is holding. The wait is bounded, so a run that cannot reach the
fault fails on the record it produced rather than hanging past the CI timeout
and retaining nothing.

The result is exact and was exact in all `632` cycles of every run, on both
matrix points and on the development host:
`15` partitions committed on the first attempt, one failed, and exactly one
re-run by the restart.

### Four observations, from four different places

The distinction between them is most of what the campaign is.

**Tasks** are read from the Tokio runtime's own alive-task count, not from a
counter the framework keeps. A count the framework maintained would miss exactly
the task that escaped it, and adding one to the public surface for a campaign's
benefit is not something a campaign gets to ask for. The framework's own
`ShutdownCoordinator` accounting is read too, as each cycle's drain result, but
it answers the narrower question of whether the tasks it owns were joined.

**Connections** are read from the adapter's own pool, at every cycle boundary
and — by a sampler running throughout — while the cycles are in flight, because
a boundary sample cannot see a ceiling: by the time a cycle ends it holds
nothing. The database's own `pg_stat_activity` count is recorded beside them and
is never substituted for them. A pool that has returned a connection and a
server that has closed a backend are different events at different times, and
reporting one as the other would turn a campaign about the framework's
connection accounting into a campaign about PostgreSQL's.

**Handles** are counted as directory entries in `/proc/self/fd` on Linux and
`/dev/fd` on macOS, both of which are the process's own descriptor table and
neither of which needs a dependency. Database sockets are handles, so the number
is read as a trend across steady-state boundaries rather than as an absolute
with a meaning.

**Resident memory** comes from `/proc/self/statm` on Linux and `ps` elsewhere.

**Durable history** — instances, executions, step executions, and transactions —
is recorded at every sample and is deliberately under no growth rule. The
database is supposed to grow; every cycle commits an instance, two executions,
and a partition plan on purpose. It is there so that a reader can see it rising
while the process series do not, and so a flat process series cannot be
explained away as a workload that stopped working. The reconciliation enforces
the separation in both directions: no rule may be decided from a
durable-history metric, and the campaign may not record no durable history at
all.

### The growth rules are declared, not judged

No threshold was invented for this campaign, and no run is passed by reading a
trajectory and finding it acceptable. Eight rules are declared in the scope
document, decided by the report from the measured samples, and required by the
runner — which additionally requires that the rule the report applied is the
rule the scope declares, and that the verdict carries the series it was decided
from. A verdict nobody can check against its readings is the kind of green this
campaign exists to refuse.

| Metric | Rule |
| --- | --- |
| `alive_tasks` | No measured sample above the post-warmup baseline |
| `unjoined_tasks` | Every measured sample zero |
| `panicked_tasks` | Every measured sample zero |
| `pool_connections_in_use` | Every measured sample zero |
| `pool_connections` | No measured sample above the post-warmup baseline |
| `peak_connections_in_use` | No measured sample above the configured capacity |
| `open_handles` | No measured sample above the post-warmup baseline |
| `resident_kib` | The mean growth rate across the measured window is at most a quarter of the mean rate across warmup |

The first seven are exact and need no interpretation. Tasks and handles are
checked at *every* boundary rather than only at the end, which is the point: a
run that accumulated for thirty cycles and cleaned up on the last one would pass
an end-state check, and the boundary series is what makes that pattern visible.

The memory rule is the one that needed thought, and it took a failed CI run to
get right. Requiring the final resident reading to equal the first would be a
statement about the allocator rather than about the framework, and inventing a
kilobyte budget would publish a release commitment nobody accepted.

The rule holds resident memory to **convergence** rather than to a level: the
mean growth rate across the measured window must be at most a quarter of the
mean rate across warmup, each taken end to end over its window. It compares a
rate against a rate, so it carries no unit and no allowance in kilobytes, and it
is scale-free in both the size of the process and the page size of the host.

What makes it discriminate is that accumulation and settling differ in their
*derivative*, not their level. A leak adds the same amount every cycle, so its
rate is flat and the ratio of late rate to early rate is one — a leak of one
byte per cycle and a leak of one megabyte per cycle both fail. An allocator
reaching a steady state against an unchanging transient pattern has a rate that
decays toward zero. A series that is already flat passes with both rates at
zero; one that is flat early and rises later fails, because a rate of zero
admits nothing above it.

The blind spot is stated rather than left to be discovered: a leak small enough
to be dominated by settling that is still in progress is not resolved, because
the measured rate would be the settling's and the ratio would still decay.
Process resident memory does not support a finer reading than that at any window
this campaign can afford. **That is why the accumulation claim does not rest on
it.** Tasks, pooled connections, checkouts, and handles are integers required to
be flat at every boundary; resident memory is required only to converge. The two
kinds of evidence are not interchangeable and the campaign does not present them
as such.

Each rate is the window's last reading minus its first, divided by the number of
cycle intervals the window spans — one fewer than the number of samples it
holds. The denominator is intervals rather than samples because the two windows
are deliberately different lengths, and a sample-count denominator scales each
window's rate by its own length; that factor does not cancel in their ratio, so
a constant leak would report `0.97` instead of the `1.00` the rule is stated in
terms of. The report and the runner compute this separately and neither imports
the other's arithmetic, so both are held to the vectors in
[`rate-vectors.json`](../../tests/fixtures/soak/rate-vectors.json) instead. That
file exists because writing the two implementations separately is what stops one
from copying the other's bug, and is not what stops them from sharing a
misreading of the statistic — which is precisely what happened, and is recorded
as F23.

The limit is `0.25`, and it is set from a characterised null rather than from
taste. Across ten CI runs of the two supported majors the observed ratio is
`0.030` to `0.043` — a spread of `1.4×` — so the limit sits roughly six times
above the worst healthy run and four times below the `1.00` a straight line
gives. Those are the ten runs characterised for F22, restated for the interval
denominator rather than re-measured: at a fixed window the correction rescales
every ratio by exactly `(600/599)/(32/31)`, so what each run would now report is
arithmetic on what it did report. No single run moves the limit, and it was not
adjusted to fit the reruns that followed. The reconciliation additionally
requires any declared decay to be between `1` and `99` percent, since `100`
permits a straight line.

Getting to a statistic that stable took three attempts, and the reason is
recorded as F22: the resident series is bursty as well as quantised, so any
statistic computed *inside* a sub-window of the measurement is sensitive to
where a burst happens to land, while a whole-window rate is not.

Three rules were tried and rejected before this one; they are recorded as F20,
F21 and F22, because the reasons are worth more than the rules were.

Every rule's series, and the structural summary a reader would want beside it
(first, last, minimum, maximum, delta, consecutive new highs, the two half
statistics, and a least-squares slope) is retained in the report. The slope is
recorded and never asserted on.

### Why it is a runner as well as a test

The report needs a real database and returns green without one, because it
skips. `cargo xtask soak` resolves the fixture first and fails before the target
starts when it is absent.

A passing report is not sufficient either, for the reason at the top of this
section, so the runner reads the committed denominator and requires the
substance:

- the declared warmup and measured windows ran, with one retained sample per
  cycle and at least the declared minimum of measured samples, and with the
  warmup marked as declared rather than widened after the fact;
- the workload was the declared one, down to the partition count, the worker
  budget, and the pool size, because a soak of a smaller workload is a different
  campaign with the same name and nothing in a resource series says which
  workload produced it;
- the lifecycle actually happened, once per cycle: a fault injected, a restart,
  a recovery, and a completed drain. Without this a run that repeated a plain
  launch would satisfy every window and workload requirement above and produce
  the same flat series;
- the connection sampler took readings at all, since a sampler that died leaves
  every peak occupancy at a zero that means *nothing was measured* rather than
  *nothing was held*;
- every declared observation appears in every retained sample, because a metric
  that stopped being sampled leaves its rule deciding an absence;
- every declared correctness obligation and every declared growth rule was
  decided and holds;
- the final drain joined every owned task and closed the pool with nothing
  checked out.

Fifteen unit tests in `xtask/src/soak.rs` hold those requirements against
crafted observations, so what review checks is what the campaign enforces: a
shortened window, a widened warmup, a smaller workload, a run that repeated a
plain launch, a metric that stopped being sampled, a rule with no verdict, a
verdict decided from too few readings, a verdict that applied a different rule,
and a pool still checked out afterwards are each rejected by a named test.

The report must also name the PostgreSQL major it ran against, because a matrix
point is invisible in a connection string and an observation from one supported
major would otherwise reconcile perfectly inside a run of another.

### Where it runs

`postgres-15-soak-campaign` and `postgres-18-soak-campaign` in
`.github/workflows/ci.yml`, on the two ends of the supported PostgreSQL
`15`-`18` range, each retaining its report as a build artifact on success and
failure alike. Failure is the case that retention exists for: what a failed
soak needs is the resource trajectory that led to it, and a job that uploaded
only green reports would discard exactly the evidence worth reading. The job's
timeout is `60` minutes — higher than the other campaigns because the window is
minutes of work rather than seconds, and still a ceiling, because a soak that
stopped making progress must fail rather than occupy a runner.

### Results

Both matrix points pass with no violations.

| Report | Matrix | Result |
| --- | --- | --- |
| [`soak-campaign-postgres-15.json`](../engineering/campaigns/m5/soak-campaign-postgres-15.json) | PostgreSQL 15 | Passed |
| [`soak-campaign-postgres-18.json`](../engineering/campaigns/m5/soak-campaign-postgres-18.json) | PostgreSQL 18 | Passed |

The retained reports are immutable CI artifacts produced from the recorded
producer commit. The later evidence-retention commit only records those
artifacts and their provenance.

Both were produced by run `31418389250` of the `Rust` workflow — which concluded
successfully as a whole, not merely in its soak jobs — from execution tree
`e947dc0`, the merge commit the workflow actually checked out, off branch head
`e999f91`. The execution tree is the provenance root and the branch head is
recorded beside it as metadata; they are different commits and are never used
interchangeably. `rustc 1.97.1`, Linux `x86_64` on kernel `6.17.0-1020-azure`,
servers `15.18` and `18.4`, `4` Tokio worker threads. The command is
`./tests/fixtures/soak/run-ci-campaign.sh <major>`, which the workflow calls and
which runs `cargo xtask soak` — so the independent recomputation is what the
producing job itself executed. The runs took `246` and `224` seconds. Each
artifact's own sha256 digest was read from the GitHub API at promotion and
matched the downloaded bytes exactly before those bytes were retained.

This is the campaign's second producer run. The first, `31387759020`, was
discarded rather than reinterpreted: F23 corrected the memory statistic's
denominator and the workflow-closure corrections touched three closure files, so
those reports described a campaign this tree no longer runs. No figure below is
carried across from them, and none of their numbers was adjusted by hand to the
new rule.

The full provenance is
[`evidence-provenance.json`](../engineering/campaigns/m5/evidence-provenance.json),
and `cargo xtask evidence` checks it on every CI run rather than leaving it as
prose — including that the campaign these reports describe is still the campaign
this tree runs. See "How retained evidence is bound" below.

Every exact figure below is identical on the two matrix points. The resident
series differ, which is the one place this campaign expects them to: it is the
only observation that is not an integer the framework controls.

**The window.** `32` warmup and `600` measured cycles, `632` completed, one
sample per cycle, and `40906` and `37117` pool readings taken while the
cycles ran, none of which failed to read the gauge.

**Correctness.** All fifteen obligations held in all `600` measured cycles, on
both majors. Every cycle: `15` partitions committed on the failed attempt, the
injected partition failed, `Failed` recorded durably, a new job execution on the
same instance, exactly `partition-0015` re-run, `16` partitions `Completed`,
`108` repository transactions, and a drain that joined all `4` owned tasks with
no panic. `10744` partition executions per matrix point. No worker was still
holding when a step returned, and peak worker occupancy never exceeded the
budget of `4` — it reached `2` and `3` on the CI runners and `4` on the
development host, which is a property of the host's parallelism rather than of
the bound, and is why the campaign requires occupancy to stay within the budget
rather than to reach it. Reaching a ceiling is the resource-bound campaign's
obligation.

**Tasks.** `4` alive at the post-warmup baseline and `4` at all `600` measured
boundaries, on both majors. No drain left a task unjoined and none panicked, in
any cycle.

**Connections.** The pool held `5` connections at every boundary with `0`
checked out at every one of them, and `0` checked out in the authoritative
reading taken while the pool was still open at the end of the run. In-flight
occupancy reached `5` — the whole pool, which is `worker_budget + 1` — and never
exceeded it. The database reported `5` backends for the application throughout,
and `0` after the close, which the server reached within a millisecond.

**Handles.** `17` at the post-warmup baseline and `17` at all `600` measured
boundaries, on both majors.

**Memory.** Resident memory grew at `13.161` KiB per cycle across warmup and
`0.574` across the measured window on PostgreSQL 15 — a ratio of `0.044` — and
at `14.839` and `0.474` on PostgreSQL 18, a ratio of `0.032`. Each rate is the
window's rise over the cycle intervals it spans: `408` KiB over `31` warmup
intervals and `344` over `599` measured ones on 15, `460` and `284` on 18. The
rule allows `0.25` and a straight line gives exactly `1.00`, so the two sit
`5.7` and `7.8` times inside the limit.

One of them is worth stating rather than rounding past. The statistic was
characterised over ten runs whose ratios, restated for the interval denominator,
span `0.030` to `0.043`; PostgreSQL 15's `0.0436` here is marginally *above* the
top of that range. It is a new observation extending a ten-run sample, not a
regression — the margin to the limit is still `5.7×`, and PostgreSQL 18 sits
comfortably inside — but the characterised range in the scope document was not
widened to absorb it, because that document is inside the semantic closure and
editing it would invalidate the evidence being retained. The next campaign that
touches the closure for its own reasons should fold these two runs into the
characterisation. Recording the exceedance is the point: the F20/F21/F22
sequence is a record of statistics whose healthy range was discovered late.

What that establishes is convergence and not the absence of a leak. The rule
compares a rate against a rate, so a leak much smaller than the warmup settling
rate is a small addition to both and leaves the ratio small. The campaign's
non-accumulation claim rests on the exact counters above — tasks, pooled
connections, checkouts and handles are integers required to be flat at every
boundary — and resident memory carries only the weaker claim. The scenario
name the design gate fixes is broader than what this half of it proves, and is
kept unchanged for that reason rather than because it is exact.

The rates decay rather than hold, which is the shape the rule asks for, and two
further readings say why that is settling rather than a leak. The rate falls
about fourteenfold from warmup to measurement on both majors. And a `1032`-cycle
diagnostic run on the development host — a different allocator — is flat for its
last `800` samples at an overall slope of `0.014` KiB per cycle: a leak of even
a tenth of a kilobyte a cycle would have moved five pages across those `800`
cycles, and it moved none.

**Durable history, for contrast.** Instances rose from `33` to `632`, job
executions from `66` to `1264`, and step executions from `627` to `12008` across
the measured window, while every exact process counter stayed flat. That
contrast is the campaign's shape in one line: the database accumulated `11381`
step executions and the process accumulated no task, no connection, and no
handle.

No correctness P0 or P1 was found by this campaign, and none is open against it.
No product defect was found. Two of the findings below are defects in the
campaign itself, both caught by its own evidence; the rest are observations that
shaped it.

### How retained evidence is bound

The campaign produces two different kinds of thing and only one of them is
evidence. Its samples and its per-cycle journal are **observations** — what the
run saw. Its correctness results, growth verdicts and campaign totals are
**conclusions it drew about itself**, and a conclusion a program reaches about
its own run is not evidence for that run; it is the claim under examination.
The trust model follows from that distinction:

```text
actual CI execution tree
      ↓
raw immutable observations
      ↓
independent verifier
      ↓
derived verdicts
      ↓
retained artifact
      ↓
current-tree semantic compatibility
```

**The root is the tree that actually executed.** The run records the git object
identity of every path in the campaign's declared closure from inside its own
checkout, into the report, as it runs. That is the only way to name it
correctly: a pull-request job executes against an ephemeral merge commit no
later clone can resolve, and the branch head is a *different tree*, so using the
branch head as a stand-in means checking the evidence against something that
never ran. Both SHAs are recorded, in separate fields, and only the manifest is
authority.

**Nothing the report concluded is read as evidence.** `cargo xtask soak`
recomputes every verdict from the raw observations and requires the report's own
answers to match, in an order that is itself load-bearing:

1. **chronology** — the samples and the journal must be one contiguous, ordered,
   correctly phased run of the declared length, with each entry's recorded cycle
   index equal to its position. Serialization order is not authority. Nothing
   below is evaluated until this holds, because every later step indexes into
   these arrays: the correctness baseline is *the first measured cycle* and the
   memory verdict is *the ordered endpoints of the whole warmup and measured
   windows*, and a duplicated, missing or reordered entry moves both silently.
2. **lifecycle** — folded from the journal, never read from the summary, and
   required of each cycle individually: totals cannot distinguish six hundred
   launches with thirty restarts from six hundred of each.
3. **correctness** — all fifteen obligations recomputed per cycle from the
   journal, which is why the journal records exact partition-key sets and
   per-partition status, exit status and counters rather than counts. The
   comparison is on the *failing-cycle set*, so a report that holds an
   obligation while the journal shows it violated in cycle 417 is caught, and so
   is one that blames the wrong cycle. Declared, reported and recomputed
   identifier sets are reconciled as unique sets in all three directions, and a
   duplicated identifier is rejected outright rather than resolved last-wins.
4. **growth** — every rule recomputed from the sample series, with the series
   each verdict carries required to equal the measured samples element by
   element.

**The permanent verifier is offline.** `cargo xtask evidence` resolves no
commit, fetches nothing, and needs only the retained report, the declared
closure and the working tree. It checks the retained artifact's blob identity
and requires every identity in the execution manifest to still be the tree's
identity today. The one-time remote check — workflow run and producing job
identity and conclusion, artifact digest, and a byte-for-byte comparison of the
downloaded artifact against the retained file — happens once, at promotion, and
is recorded as machine-readable booleans the verifier requires rather than as a
sentence saying it was done.

**The closure is declared once**, in
[`tests/fixtures/soak/campaign-semantics.json`](../../tests/fixtures/soak/campaign-semantics.json),
and read by both the producer and the verifier. It covers framework source, the
repository adapter, migrations, cargo manifests, `Cargo.lock`, the toolchain and
build configuration, the campaign implementation and fixtures, the soak
execution contract, and the verifier itself. Two of those were learned the hard
way: an earlier closure was assembled from the code the campaign obviously
touches and omitted every input that reaches it indirectly.

The **soak execution contract** covers how CI runs the campaign — the matrix,
the database, the command, the environment — and lives in
[`execution-contract.json`](../../tests/fixtures/soak/execution-contract.json)
and `run-ci-campaign.sh` beside the campaign. `.github/workflows/ci.yml` is in
the closure with them. That is deliberately blunt, and the bluntness has a cost
worth stating plainly: an unrelated CI job now invalidates retained soak
evidence and forces a rerun. The alternative was to trust that the contract
file, the script and the workflow agree with each other, and agreement
maintained by review is not a mechanism. Extracting the soak job into its own
workflow, so that only that file sits in the closure, is the right fix and is
left to its own change.

A retained report cannot be produced by the commit that contains it — the
artifact records the tree it ran on and the commit storing it comes after — so
producer and retention differing is defined as the normal state, and the
difference is bounded by the semantic binding rather than by a rule that cannot
be satisfied.

### What this campaign does not establish

- **That no leak exists.** What a passing run establishes is bounded by the
  declared window and the declared workload, and the two kinds of observation
  carry different strengths. Over `632` cycles of *this* work, the exact-count
  resources — tasks, pooled connections, checkouts, and handles — did not
  accumulate; they are integers and every measured boundary held them at the
  post-warmup baseline. Resident-memory growth *converged* under the declared
  rate-decay rule, which is the weaker statement the rule actually supports: it
  does not exclude a sufficiently small leak masked by settling behaviour. Four
  minutes says nothing about four hours, and the campaign does not extrapolate.
  A longer window is a different campaign result, not a stronger reading of
  this one.
- **That an unobserved resource is bounded.** Four resource classes are observed
  because the plan names four. Anything else the process holds is unexamined
  rather than proved absent.
- **Anything about workloads this one does not run.** The cycle is a partitioned
  tasklet step. Chunk-oriented steps, splits, the operator and retention
  services, and the explorer are not in the loop, so nothing here says whether
  those accumulate. The same is true of concurrent launches: the campaign runs
  one job at a time.
- **Any throughput or latency number.** Cycle duration is recorded as context
  and never compared against a threshold, so a loaded host makes this campaign
  slower rather than red. P-001, P-003, and P-010 belong to the performance
  campaign.
- **A memory bound.** No number here says how much memory the framework may
  use. The rule compares a growth rate against a growth rate, so a run that
  passes has said nothing about its footprint — only that its resident series is
  converging. Its blind spot is stated above: a leak dominated by settling still
  in progress is not resolved, which is a limit of what process resident memory
  supports rather than a property of the framework. The accumulation claim rests
  on the exact counters, not on this.
- **That the handle count is only the framework's.** `/proc/self/fd` is the
  whole process, including the test's own journal and the observing connection.
  The campaign reads it as a trend at steady-state boundaries, which is what a
  whole-process count supports.
- **Cancellation or drain latency.** Every cycle drains and every drain must
  complete, but no drain is timed. Request-to-intake-stop and
  request-to-durable-terminal latency, and the unjoined count at each deadline,
  are the P-014 cancellation campaign's.
- **Anything about the published reference workload.** This workload is a
  lifecycle exerciser sized to make accumulation visible, not a representative
  deployment.

### Findings

**F16. A campaign that measures its own process must not retain its own
evidence in it.** The first implementation of the report collected each cycle's
evidence into a vector and rendered the whole thing at the end, which is the
obvious shape and is wrong here: it retained roughly `13` KiB per cycle inside
the very process whose resident memory is the result, and drew a straight line
through the measured window that had nothing to do with the framework. The
campaign's own memory rule caught it, on a run whose first-half maximum and
second-half minimum were equal by luck.

The repair matters as much as the defect. Loosening the rule until the report's
bookkeeping fit underneath it would have weakened it for real accumulation too,
and would have left the reported number partly measuring the report. Instead the
per-cycle evidence is written out of the process as it is produced, one JSON
document per line, and read back after the last sample. What stays resident is a
handful of integers per declared metric, in vectors reserved to their final
length before the measured window opens. Measured against the window in force at
the time, the growth fell from `432` KiB across `32` cycles to `16` KiB across
`120`.

**F17. A timed fault cannot produce a repeatable durable record.** Recorded in
full under "The fault waits rather than fires" above. It is a finding rather
than a design note because it is a property of the partitioned runtime that a
campaign author would reasonably not expect: a sibling stop is *cooperative* and
is consulted only before a worker's tasklet is invoked, so what a fault
interrupts is the set of siblings that had not yet started, and that set is a
scheduling outcome. Nothing is wrong with the runtime here — a cooperative stop
that never abandons work already invoked is the behaviour a batch framework
should have — but a soak that assumed otherwise would have had a different
durable record in most cycles and no usable comparison.

**F18. The adapter's pool occupancy is only published through `Debug`.**
`PostgresJobRepository` renders `pool_size` and `pool_idle` in its debug output
and exposes them nowhere else; `connection_capacity` returns the configured
ceiling, not the occupancy. The campaign therefore reads the occupancy out of
that rendering, and fails loudly rather than recording an absent number if it
ever stops carrying the fields — a connection observation that silently stopped
being taken is the one outcome a connection campaign must not be able to reach.

This is recorded rather than fixed. The alternative was to add a pool-metrics
accessor to the facade so that a test could call it, and putting a soak
instrument in the public API is a larger decision than a campaign gets to make
on its own. Whether a deployment should be able to observe its own pool is a
real question and an M6 one; it is not answered by this campaign needing to.

**F19. The heap high-water needs roughly fifty cycles to settle, and the warmup
is sized from that.** The residual growth after F16 is a general-purpose
allocator reaching a steady state against a per-cycle transient allocation
pattern that does not change, and almost all of it happens in the first few
cycles. With a short warmup the tail of that settling lands inside the measured
window, where the memory rule reads it as accumulation. The warmup is therefore
`32` cycles, fixed in the denominator and not adjusted to a run.

Being explicit about the risk: a longer warmup is the sort of thing that *could*
be used to hide accumulation, which is why the count is committed rather than
computed. It cannot actually hide one — a leak keeps rising after the settling
stops, and the measured window is four times the warmup — but the reason it is a
fixed number in a reviewed document rather than a threshold the report picks is
that the distinction should not depend on anyone's good intentions.

**F20. A rule whose verdict depends on where a page fault lands is not a
rule.** The memory rule first written for this campaign compared the smallest
reading of the measured window's second half against the largest of its first,
which reads well and asks the right question: did the process ever come back to
a level it had already held. It survived several development runs, always by
equality, and that was the tell. What it actually decides is *where the last
settling step falls*. The same trajectory — a couple of page-sized shifts and
then a long plateau — passes when the last shift lands before the midpoint and
fails when it lands after, and the development runs produced both. On a CI
runner it would have been an intermittent failure that no amount of reading the
report would have explained, and the temptation at that point is to widen the
rule until it stops firing, which is how a campaign quietly stops testing
anything.

It was replaced by the upward-shift count described above, which asks a question
whose answer does not depend on phase. The half statistics are still recorded on
every verdict, so a reader who wants the original comparison can make it.

The general lesson is worth keeping: a structural rule can be threshold-free and
still be wrong, if the structure it keys on is an accident of scheduling. The
check that caught this was not a test — it was noticing that a rule kept passing
by exactly zero margin.

**F21 (P2, campaign defect). The replacement rule was page-size dependent, and
CI rejected it on both supported majors.** The rule that replaced F20's counted
how often the resident series rose to a level it had not held before, against a
budget of `6`. The development host produced one or two shifts. PostgreSQL 15
and 18 CI produced `22` and `29`, and the campaign failed on its first run.

The product was not the problem and neither was the budget. A level shift is one
page, so the count is a function of the host's page size: the development host
has `16` KiB pages and the CI runner has `4` KiB, so the same drift is up to
four times as many shifts, and the coarser host had additionally hidden most of
it.
A rule that a supported platform fails for its page size is not measuring the
framework.

Raising the budget would have been the wrong repair — it would have tuned the
rule to the observed number on one platform and left it meaningless on both. The
diagnosis came from two runs instead. A `1032`-cycle run on the development host
is flat for its last `800` samples with an overall slope of `0.014` KiB per
cycle, which rules out a per-cycle leak: at even a tenth of a kilobyte a cycle,
`800` cycles would have moved five pages and moved none. The CI series meanwhile
rises about a kilobyte a cycle with a rate that decays roughly fourteenfold from
warmup. Same framework, two allocators, two shapes — so the shape that is common
to both is convergence, and that is what the rule now requires.

The lesson generalizes past this campaign: a metric that is quantised by the
platform cannot carry a rule stated in units of that quantum. The three
observations this campaign is most confident about — tasks, connections, handles
— are exact integers, and they were flat on both hosts from the first run.

**F22 (P2, campaign defect). A gate whose healthy range crosses its own limit
is a flaky gate, and this one crossed it on the tenth run.** The rule that
replaced F21's compared the least-squares growth rate of the measured window's
last third against its first third, with a limit of `0.50`. It passed nine CI
runs and then failed a healthy PostgreSQL 18 run at `0.585` — on a retention
commit whose diff was documentation only, so the same tree that had just
produced passing evidence failed on rerun.

The two series are almost the same run. Warmup rose `428` and `424` KiB;
measurement rose `260` and `300`; both decayed. What differed is *where*: the
failing run gained `48` KiB during cycles 400-499, and that burst falls inside
the last third, tilting the regression line inside that window. Per-hundred-cycle
slopes were `1.19, 0.49, 0.36, 0.23, 0.17, 0.15` for the passing run and
`1.00, 0.63, 0.50, 0.15, 0.47, 0.22` for the failing one — the same decay with
a bump in it.

Characterising the statistic across all ten runs is what settled it. The
last-third slope ranges `0.076` to `0.443`, a `5.8×` spread, and the ratio
ranges `0.084` to `0.585` — so a limit of `0.50` sits *inside* the healthy
distribution. That is the same mistake as F20 and F21 one level up: a statistic
whose run-to-run variance is comparable to the effect being measured, with a
bound set from too few observations.

Four candidates were then measured against the same ten runs rather than
reasoned about. Whole-window rate decay — measured mean rate over warmup mean
rate — came in at `0.031` to `0.044`, a `1.4×` spread, against `1.00` for a
straight line. It is robust for a structural reason rather than a lucky one: a
burst moves both endpoints of the window it lands in, so it changes the rate
that window reports without changing where the rate is measured from. The
campaign now gates on that, and records the rejected statistics alongside it so
the choice can be re-examined against future runs.

The general lesson, third time stated because it keeps recurring in a new
disguise: before gating on a statistic, characterise what it does on runs that
are known good. Every rule this campaign has discarded looked correct in the
abstract and was chosen without knowing its own null distribution.

**F23 (P2, campaign defect). Two implementations written separately from the
same ambiguous sentence made the same mistake, and each was the other's
check.** The convergence rule divides a window's rise by its length, and both
the report and the runner divided by the number of samples. A window of `n`
readings spans `n - 1` intervals, so every rate was understated by `(n - 1)/n`.

The factor does not cancel between the two windows, because they are
deliberately different lengths. At the declared `32` and `600` a constant leak
— the shape the rule exists to reject — came out at `0.97` rather than the
`1.00` the scope document states the rule in terms of. Nothing passed that
should have failed: `0.97` is still four times the limit, and the healthy runs
sat near `0.04`. The defect was in what the threshold *meant*, not in what it
decided, which is why ten CI runs and several reviews went past it.

The interesting half is why the recomputation did not catch it. `cargo xtask
soak` exists to disagree with the report, and it is kept separate from the
report for exactly that reason — no shared helper, no import, deliberately
different code. It still agreed, because both were written from the same prose,
and the prose said "over their window" without saying what the denominator of a
window is. Separate implementations of an ambiguous specification are not two
opinions; they are one opinion typed twice.

So the statistic is now declared outside both, as vectors with their expected
rates and verdicts in
[`rate-vectors.json`](../../tests/fixtures/soak/rate-vectors.json), and each
side is checked against the declaration rather than against the other. The
vectors carry the constant-leak property at unequal window lengths, both sides
of the inclusive boundary, and the degenerate windows; reverting either
implementation to the sample-count denominator fails them on that side alone.
A window spanning no interval now yields no rate rather than a rate of zero,
since a one-sample window read as flat would let a truncated campaign pass a
rule it could not decide.

The threshold did not move. The characterised null was restated by exact
arithmetic rather than re-measured — at a fixed window the correction rescales
every ratio by `(600/599)/(32/31)` — and the straight line the limit is
measured against is now exactly `1.00` rather than `0.97`.

The lesson is the F20/F21/F22 lesson displaced one level. Those were about
choosing a statistic without knowing its null distribution. This one is about
believing an agreement that was structurally incapable of being a
disagreement — the check was real, and it was checking the wrong thing.

## Cancellation campaign

### What the campaign owes

The performance plan's cancellation row promises P-014 request-to-intake-stop
and request-to-durable-terminal latency with the count of unjoined tasks at each
deadline, and its M4 operations section additionally requires those latencies
separated by phase. The campaign delivers four reports.

| Report | Scenario | What it must show |
| --- | --- | --- |
| Operator stop | `operator_stop_reaches_a_durable_terminal_status` | A cancellation requested through the accepted durable operator path stops intake and reaches a durable terminal status, both latencies are measured and correctly ordered, and work committed before the cancellation survives it. |
| Phase separation | `cancellation_latency_is_measured_separately_per_phase` | Request-to-durable-terminal measured separately for the async, blocking, and transaction phases, with the process intake path recorded beside them rather than averaged into them. |
| Deadlines | `drain_reports_unjoined_tasks_at_every_declared_deadline` | At every declared deadline and at escalation, the number of tasks the drain still owned, attributed by phase, equal to the number the report actually held. |
| Restart | `restart_after_cancellation_resumes_without_rerunning_committed_work` | A cancelled attempt restarts as a new execution of the same instance and re-runs only what it did not commit. |

This campaign is the first on this page whose scenario names the design gate
does not fix. Its named-scenario table lists ten IDs for the evidence campaigns
and none of them is a cancellation scenario; conformance, crash, upgrade,
security, resource bounds, and soak each have at least one, and cancellation,
performance, and reference workload have none. That gap is recorded rather than
repaired — adding an ID to a closed decision record so that a downstream
campaign's reconciliation can lean on it is the wrong direction of travel — and
`the_design_gate_still_names_no_cancellation_scenario` asserts the gap so that
the day one is added, the test fails and the omission is revisited deliberately.
It belongs to the M5 exit record in
[#103](https://github.com/luceat-lux-vestra/oxide-batch/issues/103), not to a
campaign PR. The practical consequence is that the reconciliation against the
plan carries the weight the gate would otherwise share, so it runs in both
directions: the campaign may not drop an obligation the plan states, and may not
claim one it does not.

### Denominator and scope

The denominator is
[`tests/fixtures/cancellation/campaign-scope.json`](../../tests/fixtures/cancellation/campaign-scope.json),
read by all three consumers rather than restated: the reports take their
deadline set, phase mapping, and workload shape from it; `cargo xtask
cancellation` reads it independently and requires the run to have matched it;
and
[`m5_cancellation_campaign.rs`](../../crates/oxide-batch/tests/m5_cancellation_campaign.rs)
checks it against the accepted plan in an ordinary `cargo test`.

The deadline set is the part that most needs committing before the run. P-014
owes the unjoined count *at each deadline*, and "each" is meaningless against a
set the run gets to choose afterwards: a campaign that quietly reduced its set to
one produces a report of exactly the same shape. Three points are declared, each
inside the accepted `1 s..=1 h` bound that `ShutdownDeadline` and
`TaskJoinDeadline` enforce, and each holding a *different* number of tasks —
`3`, `5`, and `7` — because a set whose points all held the same number cannot
distinguish a coordinator reporting what it owns from one reporting a constant.
The accepted default of `30 s` is one of the three, and it costs the campaign
thirty seconds of deliberate waiting per matrix point; omitting it and reporting
only cheap deadlines would measure the configuration the campaign chose rather
than the one the framework ships.

### Latency is observational, and that is a decision

**No accepted document states a cancellation budget.** The plan requires both
latencies to be *measured* and reported and states no value either must stay
under. The scope records this as `latency_status: observational`, and the rule
is enforced from both sides: nothing in the reports compares a duration against
a limit, and the runner checks only that each duration is present, structurally
possible, and correctly ordered. A fast run and a slow run reach the same
verdict.

Two tempting alternatives were rejected. Importing the M4 in-memory figures —
`1–2 µs` to cancellation, `135–148 µs` to durable terminal — would fail every
healthy PostgreSQL run, because there the durable terminal is a committed
transaction and the intake stop is bounded by a configured poll interval; the
difference is the storage engine, not the framework. Inventing new numbers from
these runs would publish a release commitment nobody accepted, against the
plan's own rule that provisional budgets stay hypotheses until repeated
baselines justify a release gate. This campaign supplies raw material for that
decision and does not pre-empt it.

The counts are a different matter and *are* asserted. The accepted contract
fixes them: a drain that reports fewer unjoined tasks than it still owns is a
defect at any speed, and it is checkable exactly because the report knows how
many tasks it held.

### Scenario methodology

Cancellation is requested through the accepted operator path and no other. An
operator knows a job name and its parameters, so the campaign finds the running
execution the way one would — `find_job_instance` then `job_executions` — rather
than reading an identifier out of a launch report no deployment has. It then
calls `request_execution_stop` under compare-and-swap and commits.

**The clock starts when that request is durable**, not when the campaign decided
to cancel and not when the call was made: a request that has not committed is one
no observer could act on, and starting earlier would charge the framework for the
campaign's own round trip. Request-to-intake-stop ends when the durable
`STOPPING` transition is first readable, which is the accepted meaning of intake
having stopped on this path; request-to-durable-terminal ends when `STOPPED` is
first readable.

Both endpoints are read back from the database by a watcher on its own
connection, outside the runtime. The alternative — a hook or telemetry event
reporting the framework's own timings — is cheaper and wrong twice over: it asks
the component under test to time itself, and it would measure the interval
between two points the runtime already knew about, skipping the commit at each
end, which is where an operator's latency actually goes. The cost is a sampling
floor of `5 ms`, recorded beside every duration it bounds rather than left to be
discovered, and two orders of magnitude below the framework's own stop poll
interval, which dominates this path.

The cancellation is requested only after a partition has durably committed and
while later workers are in flight, so there is always both committed work to
preserve and running work to cancel. Every duration is monotonic; the framework
clock is pinned to a fixed instant precisely so no durable timestamp can
accidentally become one of the campaign's measurements.

The drain report runs each deadline **twice**: once with tasks that finish
before it, where the drain must complete and report nothing unjoined, and once
with tasks held past it, where the reported count must equal the number held and
the per-phase attribution must sum to it. Either half alone is worthless — a
coordinator hard-coded to report zero passes every completing drain, and one
hard-coded to report the held count passes every expiring one — and across three
deadlines with three different held counts neither constant survives. Escalation
is measured beside them because it ends waiting the other way and owes the same
count.

### Why a runner is required

All four reports return success without a database, because they skip and
return, and under `cargo test` that is indistinguishable from evidence. `cargo
xtask cancellation` resolves the fixture first and fails before any target runs
when it is missing. This is verified rather than assumed: with the fixture
unset, the campaign exits non-zero naming the variables, while `cargo test`
reports four passes.

Passing tests would not be sufficient even with a database. This campaign's
headline numbers are durations, and a duration is the easiest observation in the
M5 set to produce without doing the work — a report that measured nothing can
retain a zero, and one that measured the wrong interval retains a plausible
number. So the runner requires the substance: every observation the denominator
declares was taken by the report that owes it; every declared deadline was run
both ways with the right count; the durations are present and correctly ordered;
and the accepted recovery contract held. An observation the denominator declares
and the runner does not know how to locate is a violation rather than a pass, so
the two cannot drift apart in either direction.

### Matrix and CI execution

`postgres-15-cancellation-campaign` and `postgres-18-cancellation-campaign` run
the two ends of the supported range, each provisioning its own server and
calling
[`run-ci-campaign.sh`](../../tests/fixtures/cancellation/run-ci-campaign.sh);
how the campaign is executed lives in
[`execution-contract.json`](../../tests/fixtures/cancellation/execution-contract.json)
beside the campaign rather than in the workflow. Reports are retained on failure
as well as success, because a failed cancellation campaign is mostly worth
reading for the latency and unjoined-count detail that led there.

Every report names the PostgreSQL major it ran against, and the runner requires
it to match the matrix point the run declares. A matrix point is invisible in a
connection string, so without it an observation from one supported major
reconciles perfectly inside a run of another.

### Results

Both matrix points passed with no violations. Every figure below is an
observation from workflow run
[31450855074](https://github.com/luceat-lux-vestra/oxide-batch/actions/runs/31450855074),
debug profile, on a shared GitHub-hosted runner. **Nothing is asserted against
any of them.**

| Observation | PostgreSQL 15 | PostgreSQL 18 |
| --- | --- | --- |
| Server | `15.18 (Debian 15.18-1.pgdg13+1)` | `18.4 (Debian 18.4-1.pgdg13+1)` |
| Request to intake stop | `15 197 µs` | `10 037 µs` |
| Request to durable terminal | `431 691 µs` | `483 574 µs` |
| Ordering holds | yes | yes |
| Durable terminal | `STOPPED` / exit `STOPPED` | `STOPPED` / exit `STOPPED` |
| Committed partitions preserved | `1` → `2` | `1` → `2` |
| Workers outliving the attempt | `0` | `0` |

Request-to-durable-terminal separated by phase, and the process intake path
beside it:

| Phase | Mechanism | Intake stop, 15 | Terminal, 15 | Intake stop, 18 | Terminal, 18 |
| --- | --- | --- | --- | --- | --- |
| async | tasklet awaiting the cooperative token | `17 117 µs` | `433 802 µs` | `15 737 µs` | `461 609 µs` |
| blocking | `BlockingTaskletAdapter` | `7 720 µs` | `550 664 µs` | `8 353 µs` | `548 011 µs` |
| transaction | tasklet holding an open repository unit | `8 463 µs` | `408 430 µs` | `10 661 µs` | `472 915 µs` |
| process intake | `request_shutdown` then `ensure_accepting` | `5 µs` | — | `2 µs` | — |

Unjoined counts. Every declared deadline was run both ways; the completing
column is the drain's join cost when its tasks finish, and the expiring column
is what the coordinator still owned when the deadline won.

| Deadline | Held | Completing (unjoined / cost, 15) | Expiring (unjoined, 15) | Completing (18) | Expiring (18) |
| --- | --- | --- | --- | --- | --- |
| `minimum`, `1 000 ms` | `3` | `0` / `89 560 µs` | `3` | `0` / `77 983 µs` | `3` |
| `intermediate`, `5 000 ms` | `5` | `0` / `61 492 µs` | `5` | `0` / `76 060 µs` | `5` |
| `default`, `30 000 ms` | `7` | `0` / `5 291 µs` | `7` | `0` / `5 730 µs` | `7` |
| escalation | `4` | — | `4`, in `41 µs` | — | `4`, in `45 µs` |

Every expiring count is attributed across the three observed phases and sums to
the total: `Tasklet`/`ChunkReadProcess`/`Transaction` of `1`/`1`/`1`, `2`/`2`/`1`,
and `3`/`2`/`2` respectively, identically on both majors.

Restart after cancellation, both majors: the restart was a new execution of the
same instance, re-ran `62` partitions, re-ran **none** of the partitions the
cancelled attempt had committed, and reached `COMPLETED`.

### Correctness assertions

Every one of these held on both matrix points:

- the cancelled attempt persisted the `STOPPED` batch status and the
  framework-owned `STOPPED` exit status;
- request-to-intake-stop was less than or equal to request-to-durable-terminal;
- no worker outlived its cancelled parent;
- every partition committed before the cancellation was still committed after
  it, and no interrupted partition was left recorded as running;
- at every declared deadline and at escalation, the reported unjoined total
  equalled the number of tasks held and the per-phase counts summed to it;
- no task was detached and no panic was swallowed;
- the restart was a new execution of the same instance and re-ran only what the
  cancelled attempt had not committed.

### Findings

**F23. The exit status of a cancelled attempt is `STOPPED`, not `CANCELLED`.**
This campaign asserted `CANCELLED` before reading the delivered contract, on the
reasonable-sounding grounds that the domain has an exit status by that name.
It does not: `CANCELLED` is the durable code of `FailureCategory::Cancelled`, a
*failure category* reached when cancellation surfaces as a failure, while a
cooperatively stopped attempt persists `ExitStatus::stopped()`, which is the
framework-owned code `STOPPED`. The two are different fields with different
meanings and one overlapping intuition.

The framework is right and the campaign was wrong, which is the point worth
recording. Had it not been checked, the campaign would have failed a correct
framework and the obvious repair — "fix" the runtime to persist `CANCELLED` —
would have changed an accepted durable contract to satisfy a test. This is
exactly the failure mode a campaign is most exposed to, because it writes its
assertions before it runs anything.

**F24. The blocking phase costs roughly a third again as much as the others to
reach a durable terminal, and a single averaged latency would have hidden it.**
On both majors the blocking phase reached its terminal at `551 ms` and `548 ms`
against `434`/`408` and `462`/`473` for the async and transaction phases. The
cause is not a defect: `BlockingTaskletAdapter`'s synchronous body runs to
completion once started and the adapter reports the stop afterwards, which is
the accepted late-stop limitation stated in its own documentation. The finding
is about *measurement*, and it is why the accepted plan asks for the phases
separately rather than as one number — a mean over the three would sit near
`480 ms` and would describe none of them, and the phase whose cancellation
behaviour a deployment most needs to understand is the one it would blur away.

**F25. Request-to-intake-stop is a phase offset, not a constant, and a single
run cannot be read as its typical value.** The configured stop poll interval
bounds how long the owning runtime may go without re-reading the durable stop
request; where inside that window an operator's request happens to land is
arbitrary. The campaign configures the accepted minimum of `100 ms`, and the
observed values are `15 ms` and `10 ms` on CI against `110 ms` on the
development machine — the same code, spread across the interval as the offset
predicts.

This is recorded because a reader given one number would reasonably take it for
a latency the framework achieves. It is not: it is one sample of a quantity
whose spread is the configured interval, and the honest summary is that
request-to-intake-stop on this path is bounded by the poll interval and
distributed within it. Both the configured interval and the accepted default of
`1 s` are recorded in every report so the figure can be read against either. A
campaign that wanted the *distribution* rather than a sample would need repeated
cancellations at one configuration, which is a larger measurement than P-014
asks for and is left to whatever eventually proposes a budget.

**F26. The two intake paths differ by three orders of magnitude and must not be
reported as one number.** Process intake stop — `request_shutdown` followed by
`ensure_accepting` — completed in `5 µs` and `2 µs`, against `15 197 µs` and
`10 037 µs` for the durable operator path. The first is an atomic state
transition in memory; the second is a committed transaction observed at a poll
interval. They are both truthfully called "request to intake stop", and a report
that averaged them, or quoted whichever suited, would be describing neither.
They are recorded as separate observations for that reason.

**F27. Adding this campaign invalidated the retained soak evidence, and that is
the closure mechanism working.** Registering `cargo xtask cancellation` in
`xtask/src/main.rs` and adding the two CI jobs to `.github/workflows/ci.yml`
changed two paths inside the *soak* campaign's declared semantic closure, so
`cargo xtask evidence` correctly refused the retained soak reports: they
describe a campaign this tree no longer runs. The soak was re-run in the same
workflow run and its evidence re-retained.

The cost is real and was accepted knowingly when the soak declared the workflow
file as one bound object. Its own closure document anticipates this case in as
many words, and names the fix — extracting each campaign into its own workflow
so that only that file is in the closure — as belonging to its own change rather
than to whichever campaign happens to trigger it next. This is the second
campaign to pay the cost, which is the argument for doing it; it is still not
this PR's change to make.

**F28. A campaign whose workload finishes before its cancellation lands measures
nothing, and reports it as a failure of the framework.** The first declared
workload was `16` partitions of `50 ms`, which is about `200 ms` of work against
a cancellation path that needs a durable commit, a poll interval, and a second
commit — roughly twice that. Every attempt reached `COMPLETED` before its stop
request was observed, and the report failed with "the cancelled attempt persisted
COMPLETED rather than STOPPED" and no `STOPPING` transition at all.

The distinction that matters is between a cancellation that was *fast* and one
that was never *observed*, and the failing report could not tell them apart. The
workload is now derived from what the campaign must be able to do rather than
chosen for convenience — `64` partitions of `500 ms`, about eight seconds of
work — and the derivation is recorded in the scope so that a later change that
shortens it has to argue against the reasoning rather than merely pass.

### What this campaign does not establish

- **No latency budget.** Every duration here is an observation. Nothing is
  asserted against any of them, and no number in this section is a commitment.
- **Forced worker loss.** The accepted plan puts it in the crash campaign
  explicitly: forced loss of a worker is a crash and recovery result, not low
  cancellation latency. No process is killed here.
- **Broker and remote worker phases.** The plan names them among the phases a
  cancellation test separates. M5 adds no broker and no remote or distributed
  semantics, so neither exists to measure. They are unexamined, not proved
  absent, and belong to the milestone that introduces them.
- **Three of the six `ShutdownTaskPhase` variants.** `ChunkWrite`,
  `RetryBackoff`, and `FlowDecision` are never occupied by an unjoined task
  here, and are reported as unexamined. Spawning a placeholder task into each to
  fill the table would report coverage the campaign does not have.
- **Cancellation at arbitrary scale or under an arbitrary workload.** One
  partitioned job at a declared partition count and worker budget. Cancellation
  at `1`, `10`, and `100` workers is a P-010 question.
- **Release-profile numbers.** Every M5 campaign runs debug so the matrix points
  are comparable with each other. The latencies below are debug-build latencies.

### Reproduction

```sh
# A supported PostgreSQL, schema 3, with both URLs pointing at it.
export OXIDEBATCH_POSTGRES_TEST_URL=postgres://postgres:postgres@127.0.0.1:5432/oxide_batch_cancellation
export OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL="$OXIDEBATCH_POSTGRES_TEST_URL"
export OXIDEBATCH_CAMPAIGN_DIR=target/m5-campaigns
export OXIDEBATCH_CAMPAIGN_MATRIX=postgres-18

cargo run --package oxide-batch-xtask -- cancellation
```

Or exactly as CI runs it, which is the same command through the committed
execution contract:

```sh
./tests/fixtures/cancellation/run-ci-campaign.sh 18
```

The report is written to `target/m5-campaigns/cancellation-campaign.json`. The
denominator reconciliation runs without a database:

```sh
cargo test --package oxide-batch --all-features --test m5_cancellation_campaign
```
