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
crates.io Trusted Publishing. A release is complete only after package content,
documentation, provenance, and installation are verified.

## MSRV

The accepted toolchain baseline is:

- development, normal CI, and releases use pinned stable Rust 1.97.1;
- the minimum supported Rust version is stable Rust 1.95;
- beta/nightly compatibility CI is not part of the supported test matrix;
- public crates do not require unstable language or Cargo features.

Required CI tests the workspace on the MSRV. Raising MSRV requires a release
note and may occur:

- in any pre-1.0 minor release;
- in a stable minor release only with documented justification and notice;
- freely in a stable major release.

Patch releases must not raise MSRV.

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
