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
| Dependency review | New vulnerability/license risk | Now |
| MSRV build/test | Enforce declared Rust support | Before M1 |
| Dependency/license/source policy | RustSec, license, bans, source control | Before M1 |
| Feature matrix | Default/minimal/all and approved combinations | When features appear |
| Core platform matrix | Supported OS and architecture | Before first public runtime API |
| PostgreSQL contracts | Real transaction and migration semantics | Before M2 |
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

## Release gates

- clean source and reproducible package contents;
- all supported platform/database/MSRV checks;
- API SemVer and feature compatibility;
- schema migration from each supported source version;
- conformance matrix with no unexplained required gap;
- SBOM, provenance/attestation, changelog, license, and notices;
- install/build from the exact packaged crates;
- release-tag/version match and post-publish verification.

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
