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
- An exception records advisory ID, affected package, risk, compensating
  control, owner, expiry, and removal issue.
- Ignoring an advisory without an expiry is prohibited.

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
