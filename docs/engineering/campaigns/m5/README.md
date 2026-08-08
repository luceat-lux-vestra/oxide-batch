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

## Reproducing

```bash
OXIDEBATCH_CAMPAIGN_DIR=docs/engineering/campaigns/m5 \
  cargo run --package oxide-batch-xtask -- conformance
```

Without `OXIDEBATCH_CAMPAIGN_DIR` the runner writes to `target/m5-campaigns`,
so an ordinary run never rewrites the retained evidence.

The conformance campaign requires `OXIDEBATCH_POSTGRES_TEST_URL` and
`OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL`, and fails before running anything when
either is absent. It therefore does not run on a development host without
PostgreSQL, and the committed files come from the
`postgres-<version>-conformance-campaign` CI jobs, which run it on the
supported matrix. That is the only place these results are produced.
