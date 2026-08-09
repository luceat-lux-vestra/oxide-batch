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
| `resource-bounds-campaign-postgres-15.json` | Resource bounds | Every declared bounded resource, the ceiling each report checked, the load it was offered, the occupancy it reached, and what it shed or refused, on PostgreSQL 15 |
| `resource-bounds-campaign-postgres-18.json` | Resource bounds | The same, on PostgreSQL 18 |

A file carries the matrix point in its name because one run produces one
report: the runner always writes `conformance-campaign.json`, and the two jobs
that produce it differ only in the database behind the fixture. The point is
also inside the file, as `environment.matrix`, so a copied report still says
which run it came from.

## Document shape

Every report carries the same envelope:

- `environment` — source commit, working-tree cleanliness, `rustc`, profile,
  OS/architecture, and the supported-matrix point. On a pull request the
  recorded commit is the merge commit the workflow checked out rather than the
  branch tip;
- `fixtures` — which declared fixture the run had, so a record produced without
  a database can never be read as one produced with it;
- the campaign's own body;
- `violations` and `passed` — the campaign result. A report whose `passed` is
  `false` is a failure record, retained deliberately: an absent report and a
  failed one must not look the same.

The crash and restore report additionally embeds the observation each scenario
retained while it ran, under `reports[].observation`. That is what separates a
scenario that passed from one that skipped: the observation records the child
process identifier, the signal it died from, the durable state the kill left
behind, the discovery result, and the restart outcome. The runner requires one
per report and requires every declared phase to appear inside it, so a scenario
that reported `ok` without doing the work fails the campaign.

The upgrade report embeds the same kind of observation for the same reason, and
its unit is a schema path rather than a commit phase. Each observation carries a
`paths` array, and each entry records the source and target schema version, the
migration result, what opening the database with the current runtime did, the
durable-state comparison, the backup and restore result where the path has one,
and the version finally observed. The runner requires every declared path to
appear in one and to agree with the committed denominator, so a report that
covered one source schema and skipped the other fails rather than passing half
proved. The report also names the revision the rejecting runtime was built from,
because that runtime is not this tree's.

The security report embeds an observation per report for the same reason, and
its units are the things a security claim is made of, because all three of them
are negative. The TLS observation carries every attempt, the authority it
trusted, what the supported configuration did, and — for a refusal — the
transport reason it carried, so an attempt built around an untrusted authority
that actually failed on the host name is a failure rather than a pass. The
privilege observation carries the whole `role × operation` matrix: the class,
the role, the operation, whether it was expected to be allowed or forbidden,
whether it reached the database through a service path or as a statement, and
the `SQLSTATE` the server answered with, which for a forbidden cell must be
`42501` and nothing else. The redaction observation carries the value classes
injected, the surfaces collected from, the artifact and string counts, and the
occurrence count. It records no canary, no credential, no connection string,
and no certificate: the classes appear by name and the roles by role name.

## The resource-bound campaign

The resource-bound report carries one row per declared bounded resource, under
`resource_ledger`, and each row says which report proved it, the ceiling that
report checked, the load it was offered, the occupancy it reached, and how much
it shed or refused. The row exists whether or not a report covered the
resource, because `covered` being `false` is the finding.

The report is shaped that way because of the failure mode this campaign has and
the others do not: a bound can be reported as holding by a run that never
approached it. A worker budget of `64` whose observed peak was `3` and a page
bound checked against four rows are both green and neither is evidence about a
ceiling. So `offered_load` and `observed_peak_occupancy` are recorded beside
`declared_ceiling` on every row, and the runner requires the three to be in the
relation the resource's overload policy implies.

`declared_ceiling` and `configured_ceiling` are separate fields and are
sometimes different numbers. The declared one is what the denominator says the
framework enforces; the configured one is what the run was set up with, which is
lower wherever running at the declared ceiling would leave nothing to hold back.
The report also carries `out_of_scope`, so a reader can tell a resource the
campaign examined and excluded from one it never looked at.

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
```

Without `OXIDEBATCH_CAMPAIGN_DIR` the runner writes to `target/m5-campaigns`,
so an ordinary run never rewrites the retained evidence.

The conformance campaign requires `OXIDEBATCH_POSTGRES_TEST_URL` and
`OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL`, and fails before running anything when
either is absent. It therefore does not run on a development host without
PostgreSQL, and the committed files come from the
`postgres-<version>-conformance-campaign` CI jobs, which run it on the
supported matrix. That is the only place these results are produced.

The crash and restore campaign requires those two variables and
`OXIDEBATCH_POSTGRES_BACKUP_TEST_URL`, which names a database whose role may
create and drop the database the archive is restored into. It also needs
`pg_dump` and `pg_restore` on `PATH`, because the backup report takes a real
archive rather than simulating one. Its committed files come from the
`postgres-<version>-crash-restore-campaign` CI jobs.

The upgrade campaign requires `OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL` and
`OXIDEBATCH_POSTGRES_BACKUP_TEST_URL`, and needs `pg_dump` and `pg_restore` for
the same reason. It never migrates the database the first names: it reads the
server, the role, and the connection parameters off it and creates every
database it reports on, because a report that ran against a database something
else had already migrated would not be a report about an upgrade. It also needs
the repository's full history and `git`, because the rejection report builds the
runtime that shipped against schema 2 from the revision before schema 3 was
added; a shallow clone fails the campaign rather than skipping that report. Its
committed files come from the `postgres-<version>-upgrade-campaign` CI jobs.

The resource-bound campaign requires `OXIDEBATCH_POSTGRES_TEST_URL` and
`OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL`, and fails before running anything when
either is absent. It needs a server that will grant it `65` connections at once,
because the worker report saturates a partition budget of `64` and the derived
requirement is one connection per child plus the parent's; the official image's
default of `100` is enough and the CI axes do not raise it. Its committed files
come from the `postgres-<version>-resource-bound-campaign` CI jobs.

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

Each database report must also name the PostgreSQL major it ran against,
because a matrix point is invisible in a connection string and an observation
from one supported major would otherwise reconcile perfectly inside a run of
another.
