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

## Proposed 1.0 dimensions

- Rust: declared MSRV and current stable;
- Linux x86_64: primary runtime and PostgreSQL integration target;
- Linux aarch64: production support candidate;
- macOS x86_64/aarch64: core development support candidate;
- Windows x86_64 MSVC: core support candidate;
- PostgreSQL: oldest and newest selected supported major versions;
- TLS: Rustls-backed PostgreSQL connectivity where supported by the adapter.

The M2 PostgreSQL range is selected above. Other exact platform versions and
any change to the database range are reviewed against upstream support windows,
CI availability, and user needs.

## Rules

- Every public feature builds on the MSRV.
- Core platform tests do not imply PostgreSQL operational support.
- Platform-specific signal/shutdown behavior is documented and tested.
- Dropping a supported platform, PostgreSQL major, or Rust version follows the
  release/deprecation policy.
- Best-effort combinations are labeled and do not block a release unless a
  regression affects a supported combination.
