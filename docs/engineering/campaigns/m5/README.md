# M5 Campaign Evidence

**State:** Active

These files are the retained raw evidence for the M5 production-preview
campaigns named in the
[performance and capacity plan](../../performance-plan.md#m5-production-preview-campaigns).
The gate that consumes them is the
[M5 campaign evidence](../../../project/m5-campaign-evidence.md); the issue
that owns them is
[#102](https://github.com/luceat-lux-vestra/oxide-batch/issues/102).

Measurement reports live in
[`../../measurements/m5`](../../measurements/m5/README.md) instead. The split
is by what the file records rather than by which campaign produced it: a
measurement is a number that depends on the host, and a campaign record here is
a pass or failure that does not.

## Reports

| File | Campaign | What it records |
| --- | --- | --- |
| [`conformance-campaign-postgres-15.json`](conformance-campaign-postgres-15.json) | Conformance | Every accepted M0-M4 ledger row, the scenarios assigned to it, and the outcome each one reported, on PostgreSQL 15 |
| [`conformance-campaign-postgres-18.json`](conformance-campaign-postgres-18.json) | Conformance | The same, on PostgreSQL 18 |
| [`crash-restore-campaign-postgres-15.json`](crash-restore-campaign-postgres-15.json) | Crash and restore | Every commit phase a process was killed in, the three reports and their observations, and the reused M2-M4 crash scenarios, on PostgreSQL 15 |
| [`crash-restore-campaign-postgres-18.json`](crash-restore-campaign-postgres-18.json) | Crash and restore | The same, on PostgreSQL 18 |
| [`upgrade-campaign-postgres-15.json`](upgrade-campaign-postgres-15.json) | Upgrade | Every schema path from a prior schema to schema 3, the three reports and their observations, and the revision the rejecting runtime was built from, on PostgreSQL 15 |
| [`upgrade-campaign-postgres-18.json`](upgrade-campaign-postgres-18.json) | Upgrade | The same, on PostgreSQL 18 |
| [`security-campaign-postgres-15.json`](security-campaign-postgres-15.json) | Security | The TLS attempts and the reason each refusal carried, the privilege matrix and the code every refusal was refused under, and the surfaces and value classes the redaction sweep covered, on PostgreSQL 15 |
| [`security-campaign-postgres-18.json`](security-campaign-postgres-18.json) | Security | The same, on PostgreSQL 18 |
| [`resource-bounds-campaign-postgres-15.json`](resource-bounds-campaign-postgres-15.json) | Resource bounds | Every declared bounded resource, the ceiling each report checked, the load it was offered, the occupancy it reached, and what it shed or refused, on PostgreSQL 15 |
| [`resource-bounds-campaign-postgres-18.json`](resource-bounds-campaign-postgres-18.json) | Resource bounds | The same, on PostgreSQL 18 |
| [`soak-campaign-postgres-15.json`](soak-campaign-postgres-15.json) | Soak | The declared warmup and measured window, every cycle's fault, restart, recovery, drain, and durable record, and the per-cycle task, connection, handle, memory, and durable-history series each growth rule was decided from, on PostgreSQL 15 |
| [`soak-campaign-postgres-18.json`](soak-campaign-postgres-18.json) | Soak | The same, on PostgreSQL 18 |
| [`cancellation-campaign-postgres-15.json`](cancellation-campaign-postgres-15.json) | Cancellation | The declared deadline set, the request-to-intake-stop and request-to-durable-terminal latencies, those latencies separated by phase, the unjoined task count and its per-phase attribution at every deadline and at escalation, and the durable record a cancelled attempt left and a restart resumed from, on PostgreSQL 15 |
| [`cancellation-campaign-postgres-18.json`](cancellation-campaign-postgres-18.json) | Cancellation | The same, on PostgreSQL 18 |

A file carries the matrix point in its name because one run produces one
report: the runner always writes `conformance-campaign.json`, and the two jobs
that produce it differ only in the database behind the fixture. The point is
also inside the file, as `environment.matrix`, so a copied report still says
which run it came from.

## Provenance

[`evidence-provenance.json`](evidence-provenance.json) records where each
retained report came from; `cargo xtask evidence` checks it on every CI run.

The root of trust is the tree that actually executed. Each run records the git
object identity of every path in the campaign's declared closure from inside its
own checkout, into the report, as it runs — because that is the only way to name
that tree correctly. A pull-request job executes against an ephemeral merge
commit no later clone can resolve, and the branch head is a *different* tree, so
using the branch head as a stand-in would mean checking evidence against
something that never ran. Both SHAs are recorded in separate fields and only the
manifest is authority.

Each campaign declares its own closure, once, and the producer and the verifier
read the same document: the soak's is
[`soak/campaign-semantics.json`](../../../../tests/fixtures/soak/campaign-semantics.json),
the cancellation campaign's is
[`cancellation/campaign-semantics.json`](../../../../tests/fixtures/cancellation/campaign-semantics.json),
the performance campaign's is
[`performance/campaign-semantics.json`](../../../../tests/fixtures/performance/campaign-semantics.json),
the conformance campaign's is
[`conformance/campaign-semantics.json`](../../../../tests/fixtures/conformance/campaign-semantics.json),
the crash and restore campaign's is
[`crash-restore/campaign-semantics.json`](../../../../tests/fixtures/crash-restore/campaign-semantics.json),
the upgrade campaign's is
[`upgrade/campaign-semantics.json`](../../../../tests/fixtures/upgrade/campaign-semantics.json),
and the security campaign's is
[`security/campaign-semantics.json`](../../../../tests/fixtures/security/campaign-semantics.json).
All seven cover framework source, migrations, cargo manifests, `Cargo.lock`,
toolchain and build configuration, the campaign implementation and fixtures,
the execution contract, and the verifier. The security campaign's closure
additionally binds the committed least-privilege policy —
`tests/fixtures/security/roles.sql` and `tests/fixtures/security/grants.sql`
— as its own category, distinct from the campaign fixtures that declare which
reports and denominators the runner reads, because the policy is what the
matrix's allowed and forbidden cells are checked against rather than what
decides which cells exist. The upgrade campaign's closure
additionally binds a historical revision that is not itself a path any
closure can hash: the rejection report builds a real schema-2 runtime from
the commit before schema 3 was added, pinned by hash in
`crates/oxide-batch/tests/upgrade/mod.rs`'s `SCHEMA2_RUNTIME_REVISION`
constant, which the closure covers as part of the campaign implementation, and
cross-checked byte-for-byte against the same value in
`tests/fixtures/upgrade/campaign-scope.json` and
`tests/fixtures/upgrade/execution-contract.json` — both by
`verify-ci-contract.sh` on every CI run and by
`crates/oxide-batch/tests/m5_upgrade_campaign.rs` in ordinary review. The
conformance campaign's producer
distinguishes its row-proof denominator (the `42` accepted rows and their
`133` assigned scenarios) from its execution envelope (the `30` unique test
targets those assignments touch): it selects exactly those `30` targets, but
each one still runs in full, unfiltered, so a test inside a selected target
that no assigned scenario names still runs and can still fail the campaign —
only a target outside the envelope entirely cannot. `campaign implementation`
lists the specific `30` target files those assignments name rather than a
whole directory — the same granularity the narrower campaigns already used.
The producer used to select every workspace test target `cargo metadata`
reported, not only the `30` the assignments touch; that let a workspace
target the accepted scope never touched at all affect the campaign's
pass/fail gate, including other M5 campaigns' own reconciliation tests, and
is recorded as a corrected defect (F37) in
[`m5-campaign-evidence.md`](../../../project/m5-campaign-evidence.md). The one
obligation that does stay whole-workspace is the documentation tests, because
they execute only committed rustdoc examples and read nothing dynamic. If any
closure path differs from what a report recorded, that report describes a
campaign this tree no longer runs and may not be promoted — the campaign has
to be run again.

Which closure applies to which report is decided by the campaign the report
belongs to, recorded in `campaigns.declared` in the provenance document along
with the matrix points that campaign owes. Resolving this per campaign is not a
formality: two campaigns' closures share most of their paths, so checking one
campaign's evidence against the other's would pass while checking the wrong
thing, and a matrix point covered once by each of two campaigns is not a matrix
point covered twice.

How CI executes each campaign lives in its own `execution-contract.json`,
`run-ci-campaign.sh`, and contract-check script, inside that campaign's closure.
The dedicated workflow only provisions the runner and database, verifies the
contract, calls the script, and retains the report. Soak is owned by
`.github/workflows/m5-soak.yml`; cancellation is owned by
`.github/workflows/m5-cancellation.yml`; performance is owned by
`.github/workflows/m5-performance.yml`; conformance is owned by
`.github/workflows/m5-conformance.yml`; crash and restore is owned by
`.github/workflows/m5-crash-restore.yml`; upgrade is owned by
`.github/workflows/m5-upgrade.yml`; security is owned by
`.github/workflows/m5-security.yml`. An unrelated quality or build change in
`.github/workflows/ci.yml` therefore does not invalidate any campaign's
retained evidence, while a change to the relevant dedicated workflow does. The
security workflow is the one exception to "provisions the runner and
database": it provisions no `services:` database at all, because the campaign
needs a server whose certificate was signed by an authority generated for that
run and a second server that offers no TLS, neither of which a service
container started before any step runs could be. Its canonical runner,
`tests/fixtures/security/provision.sh`, builds both servers itself before
calling the verifier and the campaign, which is why it is the security
campaign's canonical runner in place of a `run-ci-campaign.sh`: it already does
the provisioning work that script would otherwise only wrap.

The contract-check scripts compare the important workflow values — triggers,
permissions, runner, PostgreSQL matrix and provisioning, timeout, command,
report path, artifact name, and failure-retention policy — against the JSON
contract and fail closed before the campaign runs. This keeps the workflow
boundary narrow without relaxing the evidence verifier.

The permanent verifier is offline: no commit resolution, no fetch, nothing but
the retained report, the closure and the working tree. The one-time remote check
— workflow run and producing job identity and conclusion, artifact digest, and a
byte-for-byte comparison of the downloaded artifact against the retained file —
runs once at promotion and is recorded as machine-readable booleans the verifier
requires, rather than as a sentence saying it happened. A report is never edited
to make a hash or a commit match; if the recorded identity and the file
disagree, the file is wrong.

## Reproducing

```bash
OXIDEBATCH_CAMPAIGN_DIR=docs/engineering/campaigns/m5 \
  cargo run --package oxide-batch-xtask -- conformance

OXIDEBATCH_CAMPAIGN_DIR=docs/engineering/campaigns/m5 \
  cargo run --package oxide-batch-xtask -- crash-restore

OXIDEBATCH_CAMPAIGN_DIR=docs/engineering/campaigns/m5 \
  cargo run --package oxide-batch-xtask -- upgrade

OXIDEBATCH_CAMPAIGN_DIR=docs/engineering/campaigns/m5 \
  cargo run --package oxide-batch-xtask -- resource-bounds

OXIDEBATCH_CAMPAIGN_DIR=docs/engineering/campaigns/m5 \
  cargo run --package oxide-batch-xtask -- soak
```

Without `OXIDEBATCH_CAMPAIGN_DIR` the runner writes to `target/m5-campaigns`,
so an ordinary run never rewrites the retained evidence.

The conformance campaign requires `OXIDEBATCH_POSTGRES_ADMIN_TEST_URL`,
`OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL`, and `OXIDEBATCH_POSTGRES_TEST_URL`,
and fails before running anything when any is absent. It therefore does not
run on a development host without PostgreSQL, and the committed files come
from the dedicated `.github/workflows/m5-conformance.yml` workflow's
`postgres-<version>-conformance-campaign` jobs, which run it on the supported
matrix. That is the only place these results are produced.

The crash and restore campaign requires those two variables and
`OXIDEBATCH_POSTGRES_BACKUP_TEST_URL`, which names a database whose role may
create and drop the database the archive is restored into. It also needs
`pg_dump` and `pg_restore` on `PATH`, because the backup report takes a real
archive rather than simulating one. Its committed files come from the
dedicated `.github/workflows/m5-crash-restore.yml` workflow's
`postgres-<version>-crash-restore-campaign` jobs, which run it on the
supported matrix. That is the only place these results are produced.

The upgrade campaign requires `OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL` and
`OXIDEBATCH_POSTGRES_BACKUP_TEST_URL`, and needs `pg_dump` and `pg_restore` for
the same reason. It never migrates the database the first names: it reads the
server, the role, and the connection parameters off it and creates every
database it reports on, because a report that ran against a database something
else had already migrated would not be a report about an upgrade. It also needs
the repository's full history and `git`, because the rejection report builds the
runtime that shipped against schema 2 from the revision before schema 3 was
added; a shallow clone fails the campaign rather than skipping that report. Its
committed files come from the dedicated `.github/workflows/m5-upgrade.yml`
workflow's `postgres-<version>-upgrade-campaign` jobs, which run it on the
supported matrix and check out full history for exactly that reason. That is
the only place these results are produced.

The resource-bound campaign requires `OXIDEBATCH_POSTGRES_TEST_URL` and
`OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL`, and fails before running anything when
either is absent. It needs a server that will grant it `65` connections at once,
because the worker report saturates a partition budget of `64` and the derived
requirement is one connection per child plus the parent's; the official image's
default of `100` is enough and the CI axes do not raise it. Its committed files
come from the `postgres-<version>-resource-bound-campaign` CI jobs.

The soak campaign requires `OXIDEBATCH_POSTGRES_TEST_URL` and
`OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL`, and fails before running anything when
either is absent. It needs a server that will grant it `5` connections for the
whole run, which is `worker_budget + 1` and is the pool it holds open from
before the first cycle until after the last. It runs `632` cycles and takes
minutes rather than seconds, which is the point of it; the declared window is
committed in
[`tests/fixtures/soak/campaign-scope.json`](../../../../tests/fixtures/soak/campaign-scope.json)
and the runner refuses a run that covered less of it. Its committed files come
from the `postgres-<version>-soak-campaign` CI jobs.

The security campaign needs more than a connection string, so it is reproduced
through the fixture script that builds what a URL cannot carry:

```bash
OXIDEBATCH_CAMPAIGN_DIR=docs/engineering/campaigns/m5 \
  ./tests/fixtures/security/provision.sh 18
```

The script generates a private certificate authority and a `localhost`
certificate, starts a PostgreSQL container with TLS configured against them,
starts a second container with TLS switched off, generates a second authority
that signs nothing, and then runs the security runner against all of it. It
needs `docker` and `openssl` on `PATH` and
accepts one supported major as its argument. An environment that already
supplies that material can invoke the runner directly instead:

```bash
OXIDEBATCH_CAMPAIGN_DIR=docs/engineering/campaigns/m5 \
  cargo run --package oxide-batch-xtask -- security
```

Then `OXIDEBATCH_POSTGRES_ADMIN_TEST_URL`,
`OXIDEBATCH_SECURITY_PLAINTEXT_TEST_URL`, `OXIDEBATCH_SECURITY_TLS_HOST`,
`OXIDEBATCH_SECURITY_TLS_MISMATCH_HOST`, `OXIDEBATCH_SECURITY_TLS_CA`, and
`OXIDEBATCH_SECURITY_TLS_UNTRUSTED_CA` must all be set; the runner names the
ones it is missing and fails before running anything. Its committed files come
from the `postgres-<version>-security-campaign` CI jobs, which are the only
place the release-blocking results are produced.

## A passing test suite is not a campaign result

This holds for every campaign here and is worth stating once, because the
security campaign is where it bites hardest.

A scenario that needs a database and does not have one prints a skip line and
returns success. Under `cargo test` that is indistinguishable from evidence, so
`cargo test --workspace --all-features` passing on a development host says
nothing about any campaign on this page.

The runners exist for that reason and do not accept it. Each one resolves its
declared fixtures first and fails before starting a target when one is absent,
rather than reporting on the subset it happened to receive, and each one then
requires every scenario it owes to have run and reported `ok` — a scenario that
is missing or `ignored` is a violation rather than a silence.

Reporting `ok` is not sufficient either, for every campaign whose claim is
about work done rather than about a suite passing. The crash-and-restore,
upgrade, and security runners additionally require the observation each
scenario retained to exist and to record that work, so a scenario that returned
green without doing anything fails. The security runner requires the substance
of a negative claim: the three transport refusals and the reason each carried,
every privilege class on both sides of its boundary with every refusal under
`42501`, and every swept surface and value class with nothing found.

The soak runner requires a further thing, because its campaign is the one that
can be weakened by doing less rather than by doing nothing. A soak that ran
three cycles and one that ran three hundred produce reports of identical shape,
both green, and the shorter one produces the flatter series and therefore the
more convincing result. So the runner reads the committed denominator itself and
requires the declared warmup and measured windows to have run with a sample
each, the workload to have been the declared one down to the partition count and
pool size, the lifecycle to have happened once per cycle — a fault, a restart, a
recovery, and a completed drain — and every declared growth rule to have been
decided, to have applied the rule the scope declares, and to carry the readings
it was decided from.

Each database report must also name the PostgreSQL major it ran against,
because a matrix point is invisible in a connection string and an observation
from one supported major would otherwise reconcile perfectly inside a run of
another.

The cancellation runner has the mirror-image problem, and it is worth stating
because it changes what the runner checks rather than how hard it checks. That
campaign's headline observations are *durations*, and a duration is the easiest
thing in this set to produce without doing the work: a report that measured
nothing retains a zero, one that measured the wrong interval retains a plausible
number, and both look exactly like evidence. There is also no accepted budget to
compare any of them against, so the runner cannot fall back on a threshold.

It therefore requires structure rather than magnitude. Every observation the
denominator declares must have been taken by the report that owes it; the two
latencies must be present and ordered so intake stopped no later than the
durable terminal; every declared deadline must have been run both with tasks
that finish and with tasks held past it, with the reported unjoined count equal
to the number held and its per-phase attribution summing to that; and the
accepted recovery contract must have held after the cancellation. No duration is
compared against a limit anywhere, so a fast run and a slow run reach the same
verdict.
