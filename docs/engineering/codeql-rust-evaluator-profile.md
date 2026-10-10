# CodeQL Rust evaluator profiling — opt-in, offline, non-authoritative

**Status:** manually invoked experimental harness; **not** a CI authority, not a
production security-scan substitute. See oxide-batch [#433](https://github.com/luceat-lux-vestra/oxide-batch/issues/433)
for the measured 2026-10-10 baseline and constraints.

## Why this exists

Three Rust-positive runs on identical CodeQL CLI 2.27.2 and Rust queries 0.1.44
showed the CodeQL `database run-queries` stage at ~170.3 s (x64), ~115.7 s
(ARM64), and ~118.4 s (ARM64 + no optional database upload). Query evaluation
and interpretation, not the optional GitHub DB bundle, remains the dominant
compute stage. The CI status `357/357 scanned` **does not** imply that all 357
files extracted without errors: reports consistently say 355 without error and
2 with errors, possibly related to out-of-workspace historical upgrade probes.

This harness generates a **structured evaluator performance log** for further
analysis on an **isolated, pre-existing Rust CodeQL database**. It does not
create a database, publish a SARIF report, upload artifacts, change workflow
permissions or touch the required PR security checks. It **does** execute
queries and modify the database's `results`/cache, so always use a disposable
DB/copy separate from existing CI/security scanning.

## Setup and invocation

1. On a machine with CodeQL **2.27.2**, prepare an **isolated disposable** CodeQL
   Rust database from the **exact** source revision to be profiled, using the
   same `--build-mode=none`, query configuration and QL pack as the required
   CodeQL analysis. For example, from a separate checkout pinned to your chosen
   SHA:

   ```shell
   codeql database create /private/tmp/oxide-codeql-db \
     --language=rust --build-mode=none --source-root="$PWD"
   ```

   That example alone is **not** proof of query/config equivalence with GitHub
   CodeQL Actions; reproduce all applicable Code Scanning config options and
   independently check source SHA, QL pack resolution, threat models and 355/2
   extraction diagnostics. The harness checks the logged query identities but
   **cannot** cryptographically prove that the database came from the claimed
   source tree. The database may contain source code and must remain private.

2. Execute from that machine (output directory must **not already exist** and
   must be **outside** this repository and the CodeQL DB):

   ```shell
   python3 .github/scripts/profile_codeql_rust.py \
     --codeql /absolute/path/to/codeql \
     --database /private/tmp/oxide-codeql-db \
     --output-dir /private/tmp/oxide-profile-new
   ```

3. The script fails closed unless the CLI reports version 2.27.2 and verbose
   `database run-queries` logs contain **all 37 expected query file identities
   from the first-attempt #488 trusted authority**, in QL pack 0.1.44, with all
   37 evaluation ordinals. This is a **frozen historical manifest**, not permission
   to trim any query. A future change to default queries should trigger a
   deliberate manifest/semantic audit, not an automatic bypass.

4. Local-only artifacts (directory permissions 0700, summary metadata 0600):
   `evaluator-events.json` (raw, potentially sensitive),
   `overall-summary.json` (generated with `codeql generate log-summary
   --format=overall`), `run-queries.log`, `sanitized-hotspots.json`, and
   `metadata.json`. All stay private on the local host. The **new** sanitized
   report includes a maximum of five source-order entries from each of
   `mostExpensivePerQuery` and `mostExpensivePerStage` and up to eight explicitly
   allowlisted numeric performance fields per entry. Arbitrary names, strings,
   source/predicate paths, query arguments, unknown fields, and sensitive text
   are discarded. A query identity is retained **only** when it matches the
   frozen 37-query manifest exactly; otherwise the item is anonymous.
   List entries preserve CodeQL's order; object-map entries are explicitly
   **unranked**. Numeric field names/units are preserved verbatim; do not
   assume numbers represent seconds or that shared/parallel stages sum to wall
   time. The report includes the local-only privacy classification, schema
   version, and source record counts. `metadata.json` records the new report
   SHA-256, not its potentially sensitive contents.

   Both `sanitized-hotspots.json` and `metadata.json` have mode 0600 under an
   output directory of mode 0700. If the CLI changes the overall-summary JSON
   shape such that either rank group has no recognized metrics, the profile fails closed
   and records `INCOMPLETE_DO_NOT_USE`. **The installed CLI's exact nested
   overall JSON field names were not established by the prior ARM64 run**:
   this is a defensive extractor validated by mock tests, not a live-schema
   certification. Before sharing even sanitized output, review its contents
   for sensitive metrics or inadvertent identifiability. Do **not** publish raw
   evaluator logs, full overall summaries, CodeQL DBs, or source archives as
   GitHub Actions artifacts. Do not commit their output.

5. For actual performance comparison, repeat on the **same machine/architecture**,
   source SHA, QL pack and query set with warmup and multiple samples, measure
   provisioning/queue separately, and compare *independent security SARIF*
   scanning and extraction diagnostics. Profiling overhead means a profiled run
   cannot be directly equated with the unprofiled required CI's latency.

## Explicit scope and safeguards

- No new workflow, dispatch trigger, token permission, CodeQL Action, or required
  Merge Gate edit; no paid runners, query removal or altered build mode.
- Fail closed on mismatched CLI/version/QL pack/query manifest, non-Rust DB,
  nonzero command exit, missing evaluator log or missing overall summary.
- Do not run on the privileged required CodeQL authority job. This command is
  **not** security-validation evidence, and its completion never authorizes PR
  merging; maintain exact-final-HEAD first-attempt required CI independently.
- Unit tests are deterministic mocks (do not claim actual CodeQL profiling has
  been performed). Run `python3 .github/scripts/test_profile_codeql_rust.py`.

Reference: [CodeQL CLI database run-queries](https://docs.github.com/en/code-security/reference/code-scanning/codeql/codeql-cli-manual/database-run-queries) and [generate log-summary](https://docs.github.com/en/code-security/reference/code-scanning/codeql/codeql-cli-manual/generate-log-summary).
