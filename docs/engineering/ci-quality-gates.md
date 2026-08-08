# CI and Quality Gates

**State:** Accepted

Checks are layered so ordinary pull requests receive fast feedback while
expensive and probabilistic checks run on a schedule or release candidate.

## Required on every pull request

| Gate | Purpose | Activation |
| --- | --- | --- |
| Format and diff hygiene | Deterministic source and no whitespace defects | Now |
| Clippy, all targets/features | Static correctness and repository lint policy | Now |
| Unit and documentation tests | Behavior and examples | Now |
| Rustdoc warnings denied | Public documentation integrity | Now |
| Facade surface disclosure | No forbidden dependency class in the rendered public surface | Now |
| Dependency review | New vulnerability/license risk | Now |
| MSRV build/test | Enforce declared Rust support | Before M1 |
| Dependency/license/source policy | RustSec, license, bans, source control | Before M1 |
| CodeQL | GitHub Actions workflow security analysis | Now |
| Feature matrix | Default/minimal/all and approved combinations | Active: facade-only and `postgres`/all |
| Core platform matrix | Supported OS and architecture | Before first public runtime API |
| PostgreSQL contracts | Real transaction and migration semantics | Before M2 |
| Conformance campaign | Every accepted ledger row proved by a scenario that ran and passed | Active: M5 |
| SemVer API check | Detect public API breakage | After first public runtime API |

Required check names remain stable because rulesets bind to names. A workflow
refactor must not create a skipped required job that appears successful.

## Rust channels

- stable Rust 1.97.1 is the pinned development, normal CI, and release toolchain;
- stable Rust 1.95 is the MSRV required check;
- beta/nightly compatibility jobs are not run;
- nightly-only language features are not allowed in public or production code;
- a future nightly-only analysis tool requires a separate decision and is not
  implied by this plan.

## Scheduled/deep checks

- Loom or deterministic concurrency-model tests for synchronization primitives;
- full feature powerset and dependency-minimal/version checks where supported;
- ignored, slow, crash, migration, soak, and leak tests;
- advisory refresh independent of dependency-change pull requests.

Failures create or update an owned issue. Scheduled checks may be quarantined
only with an expiry and a release-impact statement.

The scheduled supply-chain workflow creates or updates one owned security issue
when its advisory, license, ban, or source gate fails. The issue is an
operational notification and never converts a failed gate into success.

GitHub CodeQL default setup analyzes Actions workflows. CodeQL does not support
Rust analysis for this repository; dependency review, `cargo deny`, Clippy,
tests, and the documented security review process remain the Rust gates.

The first optional adapter activates the concrete feature matrix. The ordinary
quality job checks the facade with no default features and the workspace with
all features. PostgreSQL 15 and 18 additionally build and run the `postgres`
feature against the released migration, atomic transaction suite, disconnect
classification, and separate-process pre/post-commit crash/restart matrix.
PostgreSQL 15–18 run TLS, role, migration, repository-contract, and
vertical-slice smoke evidence.

PostgreSQL 15 and 18 additionally run the M5 conformance campaign, which runs
the whole workspace suite one target at a time and requires every accepted
M0-M4 ledger row to be proved by a scenario that ran and reported `ok`. The
campaign resolves its database fixtures before running anything and fails when
one is absent, because a database-backed scenario skips silently and would
otherwise report a pass without a database. Each job retains its report as a
build artifact, and the retained copies live in
`docs/engineering/campaigns/m5`.

## Release gates

- clean source and reproducible package contents;
- all supported platform/database/MSRV checks;
- API SemVer and feature compatibility;
- schema migration from each supported source version;
- conformance matrix with no unexplained required gap;
- SBOM, provenance/attestation, changelog, license, and notices;
- install/build from the exact packaged crates;
- release-tag/version match and post-publish verification.

Pushing a protected `v<version>` tag prepares, but does not publish, a draft
GitHub Release. The tag workflow verifies the version, performs locked package
and publish dry-runs, generates a package-scoped CycloneDX SBOM and checksums,
and attests the exact `.crate` artifact. A maintainer reviews the draft and
publishes it before Trusted Publishing runs.

## Repository automation

- changed-file labels communicate affected areas but never assign priority,
  lifecycle status, or breaking-change disposition;
- label automation is skipped for changes larger than its reviewed file bound;
- AI review is advisory, non-required, and limited to trusted-contributor pull
  requests and a bounded textual diff;
- AI output cannot approve, merge, execute pull-request code, or override
  accepted documents and deterministic evidence;
- model quota or inference failure does not affect merge eligibility.

## Coverage

Use source-based coverage as diagnostic evidence, including branch coverage when
stable and practical. Do not make a repository-wide percentage the sole gate.
Critical lifecycle, transition, transaction, restart, and recovery modules
instead maintain named scenario coverage and may define local thresholds.

Coverage excludes generated code only through reviewed configuration. Reports
identify code that cannot be exercised and link the corresponding risk or test
plan.

## Flakiness

- A test that passes only after retry is still a failure signal.
- Retrying in CI may gather evidence but cannot turn a release gate green
  without marking the test flaky.
- Every quarantined test has an owner, reason, issue, and removal date.
- Timing assertions use eventual conditions and controlled clocks, not arbitrary
  sleeps.

## Tooling candidates

- `cargo-nextest` for CI profiles, slow-test visibility, JUnit, and partitioning;
- `cargo-llvm-cov` for source-based coverage;
- `cargo-semver-checks` for public API compatibility;
- `cargo-hack` for feature powersets;
- `cargo-deny` and RustSec advisories for dependency policy.

Tool adoption requires immutable CI pinning, license/MSRV review, and a local
equivalent command.
