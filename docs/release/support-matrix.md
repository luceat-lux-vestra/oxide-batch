# Support Matrix

**State:** Template

The matrix becomes binding only for released runtime versions. “Builds” and
“supported” are not synonyms: supported combinations receive automated tests
and issue triage under the release support policy.

## Current foundation release

| OxideBatch | Rust | OS/architecture | PostgreSQL | Status |
| --- | --- | --- | --- | --- |
| 0.1.0-alpha.1 | MSRV declared 1.95; development 1.97.1 | No runtime claim | Not applicable | Facade/foundation only |

## M1 implementation matrix

M1 makes a core-library claim only; PostgreSQL and operator behavior begin at
later gates.

| Target | M1 status | Required evidence |
| --- | --- | --- |
| Linux x86_64 GNU | Primary, release-blocking | MSRV and current-stable format, Clippy, unit, property, compile-fail, contract, and documentation tests |
| macOS arm64/x86_64 | Development, best effort until CI is added | Local core checks; no release-blocking claim |
| Linux aarch64 | Candidate, not yet supported | Add CI before promoting |
| Windows x86_64 MSVC | Candidate, not yet supported | Resolve signal/filesystem differences and add CI before promoting |

The first public M1 runtime API cannot be released until the primary Linux
target has required CI. A target is not described as supported until its
required evidence runs in CI.

## M2 PostgreSQL implementation matrix

M2 targets PostgreSQL majors 15 through 18 on Linux x86_64 GNU. PostgreSQL 15
and 18 are release-blocking integration axes; majors 16 and 17 receive
connection, migration, and vertical-slice smoke coverage.

| Target | M2 status | Required evidence |
| --- | --- | --- |
| PostgreSQL 15 | Oldest, release-blocking | Repository contract, migrations, transactions, validated TLS, roles, crash/restart |
| PostgreSQL 16–17 | Supported intermediate majors | Connection, migration, and vertical-slice smoke tests |
| PostgreSQL 18 | Newest, release-blocking | Repository contract, migrations, transactions, validated TLS, roles, crash/restart |
| PostgreSQL 14 | Not in the M2 support promise | Upstream support ends in November 2026; reassess only from explicit user demand |

CI uses explicit major tags and reviews the matrix at M2 exit against the
[PostgreSQL versioning policy](https://www.postgresql.org/support/versioning/).
Certificate-validated Rustls connectivity is the supported production mode;
plaintext connectivity is limited to local and isolated test environments.
The pre-adapter
[design-gate fixture](../../tests/fixtures/postgres/design-gate/README.md)
runs schema, role, `verify-full` TLS, newer-schema rejection, and logical
backup/restore checks against every explicit `15`, `16`, `17`, and `18` image.
Adapter repository, transaction, and crash depth remains release-blocking on
15 and 18 as shown above.

## M5 production-preview support bounds

The M5 design gate closes the preview's supported configuration. A combination
absent from the supported column is not part of the preview promise, however
well it builds.

| Dimension | M5 preview decision | Required evidence |
| --- | --- | --- |
| Rust | MSRV 1.95 supported; pinned 1.97.1 for development, CI, and releases | Workspace builds and required suites on both, release-blocking |
| Linux x86_64 GNU | Supported runtime and PostgreSQL integration target | Every release-blocking suite and campaign |
| Linux aarch64 | Not in the preview promise; candidate | Add required CI before any promotion |
| macOS arm64/x86_64 | Development only, not supported | Local core checks; no release-blocking claim |
| Windows x86_64 MSVC | Not in the preview promise; candidate | Resolve signal/filesystem differences and add CI first |
| PostgreSQL 15 | Supported oldest major, release-blocking | Repository contract, migrations, transactions, TLS, roles, crash/restore/upgrade |
| PostgreSQL 16-17 | Supported intermediate majors | Connection, migration, and vertical-slice smoke |
| PostgreSQL 18 | Supported newest major, release-blocking | Repository contract, migrations, transactions, TLS, roles, crash/restore/upgrade |
| TLS | Certificate-validated Rustls (`verify-full`) is the supported production mode | Validated TLS fixtures on 15 and 18 |
| Plaintext PostgreSQL | Local and isolated test environments only | Not a supported production configuration |
| Metadata schema | Schema 3 | Direct upgrade from schemas 1 and 2; newer-schema rejection; restore-based rollback |
| Deployment shape | Single host, embedded in the application process | No remote, distributed, or multi-host claim |

**Version selection.** The preview is published as a `0.x` release. The exact
version is selected during release planning from the accepted pre-1.0 policy;
no preview release is described as stable, 1.0, GA, or enterprise-ready.

**Upgrade and downgrade expectations.** Supported upgrade sources are the
released schema versions named above, applied quiesced and transactionally.
Downgrade is restore of a compatible backup unless a tested downgrade is
explicitly supplied. A schema-2 runtime rejects schema 3 rather than operating
on it.

**Pre-release definition fingerprints.** No preview release has been published,
so the preview carries no fingerprint compatibility promise for bytes produced
before it.
[ADR-0009](../architecture/decisions/0009-definition-fingerprint-input-set.md)
narrowed the manifest projection during M5 stabilization, which changed the
format-2 and format-3 fingerprints of definitions compiled by earlier
pre-release builds. Definition drift is fail-closed, so such a definition is
rejected on restart until it is recompiled or the application registers its own
directed compatibility edge. Formats and schema versions are unchanged, and
format-1 bytes are unaffected. From the first preview release onward, a
fingerprint change requires a superseding decision and migration evidence.

**Support commitment.** Preview support follows the pre-1.0 latest-line rule in
the [support policy](support-policy.md): only the latest preview line receives
fixes, and the stable support window is finalized before the first M14 release
candidate.

**Limitations.** The preview publishes an explicit limitations record naming
every ledger row that is not advertised as verified embedded-kernel capability.
Rows that remain `Partial` or `Planned` are visible there and prevent any
full-parity claim.

Other exact platform versions and any change to the database range are reviewed
against upstream support windows, CI availability, and user needs.

This preview interpretation is accepted by
[RFC-0001](../rfcs/0001-m5-preview-and-project-wide-1-0.md). It is not a
project-wide 1.0 or enterprise-readiness matrix.

## Project-wide 1.0 dimensions

M14 support would additionally name:

- every public crate/API and configuration stability surface;
- metadata and distributed protocol N/N-1/N-2 compatibility;
- at least three certified Tier-1 database adapters;
- at least two certified Tier-1 broker adapters and their delivery modes;
- supported coordinator/worker topologies and transports;
- Spring Batch baseline, complete ledger disposition, migration source
  versions, and reference workloads;
- security, supply-chain, soak, chaos, backup/restore, and disaster-recovery
  evidence.

These are accepted gates, not current support. Exact versions are added only
after their automated or approved external certification evidence exists.

## Rules

- Every public feature builds on the MSRV.
- Core platform tests do not imply PostgreSQL operational support.
- Platform-specific signal/shutdown behavior is documented and tested.
- Dropping a supported platform, PostgreSQL major, or Rust version follows the
  release/deprecation policy.
- Best-effort combinations are labeled and do not block a release unless a
  regression affects a supported combination.
