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

## Reproducing

```bash
OXIDEBATCH_CAMPAIGN_DIR=docs/engineering/campaigns/m5 \
  cargo run --package oxide-batch-xtask -- conformance

OXIDEBATCH_CAMPAIGN_DIR=docs/engineering/campaigns/m5 \
  cargo run --package oxide-batch-xtask -- crash-restore

OXIDEBATCH_CAMPAIGN_DIR=docs/engineering/campaigns/m5 \
  cargo run --package oxide-batch-xtask -- upgrade
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
