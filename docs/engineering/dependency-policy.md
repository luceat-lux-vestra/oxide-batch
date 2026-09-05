# Dependency and License Policy

**State:** Accepted

## Admission

Every production dependency change states:

- the problem it solves and why the standard library/current graph is
  insufficient;
- public API exposure and replacement cost;
- direct and important transitive dependencies;
- required/default features;
- MSRV and supported-platform compatibility;
- maintenance, advisory, and release posture;
- license and source;
- owner and removal/upgrade plan.

Dev dependencies receive a lighter review unless they execute in CI, parse
untrusted artifacts, access release credentials, or affect generated outputs.

## Risk tiers

| Tier | Examples | Approval |
| --- | --- | --- |
| Critical | database driver, async runtime, serialization, crypto, release action | ADR or explicit architecture review |
| High | telemetry SDK, CLI/config parser, migration or code-generation tool | dependency review issue |
| Standard | focused utility with private implementation use | pull-request review |
| Development | test assertion or local developer helper | pull-request review |

## Version and feature rules

- At introduction, evaluate the latest stable crate release compatible with the
  project MSRV. Do not select an older release without a recorded compatibility,
  regression, maintenance, or licensing reason.
- Use a concrete default/caret Cargo requirement such as `"1.53.1"` or
  `"0.9.0"`. This names the tested lower bound while allowing SemVer-compatible
  updates.
- Do not use `latest`, bare `*`, wildcard, or open-ended comparison
  requirements.
- Commit `Cargo.lock`; CI and release commands use `--locked` where dependency
  resolution must be reproducible.
- Use exact `=version` only for a documented reason, such as coordinated
  published workspace crates or a pre-release compatibility constraint.
- Avoid unpublished Git dependencies in releases.
- Disable dependency default features when they add unnecessary capability, not
  as a ritual that creates an untested configuration.
- Centralize shared workspace dependencies after the second real consumer.
- Avoid duplicate major versions of security- or format-sensitive crates unless
  an exception explains why.
- Test the lowest supported Rust toolchain; do not assume the pinned development
  toolchain proves MSRV.

`cargo add` is the default addition mechanism because it selects a current
MSRV-compatible release and writes a concrete requirement. The pull request
records the version/features actually evaluated. The lockfile is updated
deliberately through reviewed dependency pull requests.

## License and source rules

Initially allowed SPDX licenses:

- Apache-2.0;
- MIT;
- Apache-2.0 WITH LLVM-exception;
- BSD-2-Clause and BSD-3-Clause;
- ISC;
- Unicode-3.0;
- Zlib.

Other licenses require documented review. Copyleft, source-available,
non-commercial, custom, missing, or ambiguous licenses are denied until
explicitly approved. Registry sources are the default; alternate registries and
Git sources require an allowlisted exception.

Automated license detection is supporting evidence, not legal certainty. A
maintainer reviews unusual packaging or generated/native components.

## Advisories and exceptions

- Critical/high exploitable advisories block release.
- Moderate advisories require triage of reachability and deployment exposure.
- Unmaintained or yanked crates require replacement or a time-bounded exception.
- Ignoring an advisory without an expiry is prohibited.
- Every active cargo-deny policy **waiver** must have one matching entry in
  `.github/supply-chain-exceptions.json` with a non-empty `owner`, `rationale`,
  and ISO `expires` date. Expired entries fail CI.
- The registry covers `advisories.ignore`, crate-specific
  `licenses.exceptions`, duplicate-version waivers in `bans.skip` and
  `bans.skip-tree`, alternate registries, and Git source allowlisting. A
  registry entry with no corresponding active waiver also fails CI so stale
  exceptions cannot linger silently.
- Restrictive policy entries are not temporary waivers. For example,
  `bans.deny` tightens policy and therefore does not require an expiry. Changes
  to the global accepted license baseline or other permanent policy belong in
  this document and `deny.toml` together and receive normal policy review;
  they must not be disguised as expiring exceptions.
- Exception targets use the canonical representation emitted by
  `.github/scripts/validate_supply_chain_exceptions.py`; the implementing PR
  must show the exact target string it is registering.
- Exceptions are temporary risk decisions, not a substitute for changing the
  accepted baseline. A permanent policy change belongs in this document and
  `deny.toml` together and must not be disguised as an indefinitely renewed
  exception.

The current exception registry is empty. Introducing an exception therefore
requires both the `deny.toml` policy change and its structured registry entry in
the same reviewed pull request.

## Supply-chain control roles

The three dependency controls are intentionally different and none replaces the
others:

- **dependency-review** is PR-diff scoped. It examines dependency changes
  introduced by the pull request and blocks newly introduced dependency risk.
- **cargo-deny / supply-chain** is full-graph policy enforcement. It checks the
  resolved dependency graph for advisories, licenses, bans, sources, and the
  time-bounded exception registry on every PR/push and on its schedule.
- **Dependabot** is update automation. It proposes dependency/security updates;
  those pull requests still pass the normal review, dependency-review, and
  cargo-deny gates before merge.

## Update policy

- Security and correctness updates are prioritized independently of the weekly
  Dependabot batch.
- Routine updates are grouped when that preserves diagnostic clarity.
- Major updates receive an issue and compatibility review.
- Dependency-update pull requests are never merged solely because CI is green;
  behavior, MSRV, features, changelog, and supply-chain impact are reviewed.

Response objectives:

- actively exploited or Critical reachable advisory: contain/triage within 24
  hours where practical;
- High reachable advisory: remediation plan within 3 business days;
- Moderate advisory: reachability and exception/update decision within 7
  business days;
- routine updates: review in the weekly dependency cycle;
- every exception has an owner and expiry no longer than 90 days without
  renewed evidence.

## Release evidence

Stable releases include or link:

- dependency and license report;
- advisory status and approved exceptions;
- SBOM in the selected standard format;
- source revision and package checksum;
- build/release provenance.

## Release tooling record

`cargo-cyclonedx 0.5.9` is the release-only SBOM generator. It is installed
with its published lockfile and exact version in the protected tag workflow,
has Apache-2.0 licensing, declares Rust 1.85 as its MSRV, and does not enter the
runtime dependency graph. The workflow generates CycloneDX 1.5 JSON for the
public `oxide-batch` package with all features and target dependencies before
attesting the exact `.crate` artifact. Upgrading the tool receives the same
workflow, license, output-format, and package-content review as an Action
upgrade.

## M2 PostgreSQL dependency record

The optional `postgres` feature admits two production dependencies reviewed by
issue #41:

- `sqlx 0.9` is the critical database driver and migration engine accepted by
  ADR-0003. Default features are disabled; only PostgreSQL, JSON, migrations,
  macros, the application-owned Tokio runtime, and Rustls with native roots are
  enabled. All SQLx types remain private to the adapter.
- `sha2 0.10` is the focused implementation of the accepted SHA-256
  job-instance identity algorithm. Digest bytes are persisted, while the
  implementation type and crate remain private.

Both follow the workspace MSRV, registry-source, license, advisory, lockfile,
and replacement-review rules above. Disabling `postgres` removes both from the
facade crate's active dependency graph.
