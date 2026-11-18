# Release, Schema, and Support Policy

**State:** Accepted

## Versioning

OxideBatch follows Semantic Versioning for public Rust APIs and documented
behavior. During `0.x`, incompatible changes may occur in minor releases and
must be called out in the changelog. The 1.0 release begins the stable
compatibility commitment.

[RFC-0001](../rfcs/0001-m5-preview-and-project-wide-1-0.md) reserves
project-wide 1.0 for M14 and treats M5 as a `0.x` Embedded Core Production
Preview. No release is called stable or 1.0 before its named evidence gate
passes.

All public workspace crates use one coordinated version initially. Public
crates are published in dependency order and the facade is published last.

## Release channels

- alpha: architecture and API exploration; no compatibility promise;
- beta: feature-complete for the named milestone; migration may still change;
- production preview: supportable pre-1.0 capability set with explicit
  limitations, upgrade expectations, and no project-wide stability promise;
- release candidate: intended 1.0 contract; only release-blocking fixes;
- stable: supported public API and metadata contracts.

Releases use reviewed commits, protected `v<version>` tags, GitHub Releases, and
crates.io Trusted Publishing. A protected tag prepares a draft Release and its
package evidence; a maintainer publishes the reviewed draft to authorize
Trusted Publishing. A release is complete only after package content,
documentation, provenance, and installation are verified.

## Rust toolchain and compiler support

[ADR-0014](../architecture/decisions/0014-floating-stable-toolchain-no-msrv.md)
supersedes the historical D-008 Rust toolchain and MSRV baseline for future
development and releases after its separate implementation is merged:

- development and ordinary CI use rustup's floating `stable` channel; the
  active compiler is resolved at run time, not pinned to a version number;
- package manifests deliberately omit `rust-version`; no numeric minimum
  supported Rust version (MSRV) is declared or independently guaranteed for
  new versions built under this policy;
- beta/nightly are not part of the required compiler support matrix, and
  public crates must not use unstable Rust features;
- all required formatting, lint, build, feature, test, integration, security
  and release validations continue on the then-current stable compiler;
- release evidence records the resolved `rustc -Vv`, Cargo version, revision,
  and package artifacts so floating toolchains do not erase reproducibility.

No numeric `rust-version` declaration does **not** mean Cargo automatically
uses the most recent compiler as its minimum supported version. It means the
project no longer promises older-compiler compatibility. The latest stable
compiler must still be installed/upgraded locally with
`rustup update stable`; CI resolves the channel on execution.

**Release boundary.** Already-published release support statements (including
0.6.0's Rust 1.95 MSRV and pinned 1.97.1 build evidence) remain authoritative
for those historical artifacts and are not rewritten. Removing a previously
advertised compiler floor is a support-contract change: announce it in release
notes for the **next eligible pre-1.0 minor release**. It is not permitted to
silently change the compiler support promise of an existing patch release.

**Transition.** The superseding policy decision and its implementation are
separate, bounded PRs under #699. Until the implementation PR merges, current
`rust-toolchain.toml`, crate manifests and the required `msrv` CI check
still enforce the earlier numeric configuration. The implementation must
preserve required check-context identity while replacing obsolete MSRV
semantics with meaningful current-Stable verification; no gate may simply be
disabled or omitted.

## Metadata schema

- the schema has its own monotonically increasing version;
- migrations are forward-only artifacts and never edited after release;
- startup refuses unsupported newer schemas;
- every release states source versions that can upgrade directly;
- backup and restore instructions precede stable schema upgrades;
- rollback means restoring a compatible backup unless a tested downgrade is
  explicitly supplied;
- application data and OxideBatch metadata use separate logical ownership even
  when they share a PostgreSQL transaction.

## Support window

Before 1.0, only the latest release line is supported. The proposed stable
policy is:

- latest stable minor: bug and security fixes;
- previous stable minor: critical security and data-integrity fixes for six
  months after the next minor;
- older minors: upgrade guidance only;
- release candidates and prereleases: best effort.

The stable support window is finalized before the first M14 1.0 release
candidate. M5 preview support remains the pre-1.0 latest-line policy unless a
separate release decision says otherwise.

## Compatibility and readiness claims

Release channel names do not create compatibility evidence. Every release
states its verified feature-ledger rows, schemas, protocols, adapters,
platforms, and known divergences. “Enterprise-ready,” complete parity, and
project-wide production/1.0 claims require the
[M14 gate](../roadmap.md#m14-project-wide-10-ga).

## Deprecation

Stable public APIs are deprecated for at least one minor release before
removal, except when retaining them creates a critical security or correctness
risk. Behavioral and schema deprecations include migration guidance.
