# Support Matrix

**State:** Proposed template

The matrix becomes binding only for released runtime versions. “Builds” and
“supported” are not synonyms: supported combinations receive automated tests
and issue triage under the release support policy.

## Current foundation release

| OxideBatch | Rust | OS/architecture | PostgreSQL | Status |
| --- | --- | --- | --- | --- |
| 0.1.0-alpha.1 | MSRV declared 1.95; development 1.97.1 | No runtime claim | Not applicable | Facade/foundation only |

## Proposed 1.0 dimensions

- Rust: declared MSRV and current stable;
- Linux x86_64: primary runtime and PostgreSQL integration target;
- Linux aarch64: production support candidate;
- macOS x86_64/aarch64: core development support candidate;
- Windows x86_64 MSVC: core support candidate;
- PostgreSQL: oldest and newest selected supported major versions;
- TLS: Rustls-backed PostgreSQL connectivity where supported by the adapter.

Exact PostgreSQL and platform versions are selected before M2 implementation
based on upstream support windows, CI availability, and user needs.

## Rules

- Every public feature builds on the MSRV.
- Core platform tests do not imply PostgreSQL operational support.
- Platform-specific signal/shutdown behavior is documented and tested.
- Dropping a supported platform, PostgreSQL major, or Rust version follows the
  release/deprecation policy.
- Best-effort combinations are labeled and do not block a release unless a
  regression affects a supported combination.
