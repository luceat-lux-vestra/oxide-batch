# Crate Publishing Policy

## Workspace strategy

OxideBatch uses one Git repository and one Cargo workspace. The public
`oxide-batch` crate is the facade and supported entry point.

Potential future workspace crates include:

| Package | Intended role | Initial publication |
| --- | --- | --- |
| `oxide-batch` | Stable facade and curated re-exports | Public |
| `oxide-batch-core` | Domain model and execution contracts | Internal, published with the facade |
| `oxide-batch-plan` | Definition graph and compiled plan | Internal, published with the facade |
| `oxide-batch-engine` | Execution engine | Undecided |
| `oxide-batch-engine-tokio` | Explicit Tokio engine, if a separate boundary is justified | Undecided |
| `oxide-batch-item` | Item/chunk/stream contracts | Undecided |
| `oxide-batch-repository` | Persistence interfaces | Internal, published with the facade |
| `oxide-batch-repository-postgres` | PostgreSQL implementation | Likely public |
| `oxide-batch-protocol` | Versioned worker/admin protocol | Undecided |
| `oxide-batch-observability` | Telemetry integration | Undecided |
| `oxide-batch-test` | Conformance and test utilities | Likely public |
| `oxide-batch-cli` | Operational command-line interface | Likely public |

This list is a namespace and architecture forecast, not approval to publish
empty packages.

The forecast is governed by
[RFC-0003](../rfcs/0003-target-workspace-boundaries.md). An accepted workspace
boundary still does not authorize publication.

## Publication rules

- Publish only crates that have a documented user or integration boundary, or
  that a published crate requires as an internal dependency under
  [ADR-0010](../architecture/decisions/0010-extracted-crate-publication.md).
- Mark internal-only packages with `publish = false` unless a published crate
  depends on them. Cargo rewrites a published archive's path dependencies to
  registry dependencies, so a publishable crate cannot depend on an
  unpublished one.
- An internal published crate states in its rustdoc landing page and README
  that it is implementation detail with no stability promise and that
  `oxide-batch` is the supported entry point. It receives no ledger row,
  support window, or independent release cadence.
- Never publish secrets, private fixtures, generated credentials, or internal
  incident data.
- Run `cargo package --workspace --list` and `cargo publish --workspace
  --dry-run --locked` before publication. The workspace dry run resolves
  unpublished members through a temporary local registry, so it succeeds
  before the first upload.
- Publish workspace crates in dependency order: `oxide-batch-core`,
  `oxide-batch-repository`, `oxide-batch-plan`, `oxide-batch`,
  `oxide-batch-cli`.
- Use exact version requirements for workspace dependencies in published
  manifests while retaining local `path` dependencies.
- Create a reviewed changelog entry and immutable Git tag for every release.
- Published versions are permanent and must never be treated as disposable.
- Do not publish operating-system binary assets until an approved first-party
  user CLI or service binary exists. The current library release contains only
  the package and its release evidence; reassess binary matrices with the M4
  CLI boundary.

## Name allocation

crates.io allocates names on a first-come, first-served basis and has no
separate reservation operation. The facade name `oxide-batch` is the only name
that should be claimed during foundation work. Predictable subcrate names are
checked periodically but are not claimed with empty placeholder releases.

If a future subcrate name becomes unavailable, the implementation may remain
internal or use another name without changing the public `oxide-batch` facade.

## Initial release

The initial version is `0.1.0-alpha.1`. It clearly communicates that OxideBatch
is pre-release while providing real, buildable package metadata and
documentation. The version was published manually to establish crates.io
ownership.

All subsequent releases use crates.io Trusted Publishing. The trusted publisher
is bound to:

- GitHub owner: `luceat-lux-vestra`
- Repository: `oxide-batch`
- Workflow: `release.yml`
- Environment: `release`

The release workflow exchanges GitHub's OIDC identity for a short-lived
crates.io token. Long-lived crates.io tokens must not be stored in GitHub.

A protected version tag first creates a draft GitHub Release containing the
exact `.crate`, SHA-256 checksums, a package-scoped CycloneDX SBOM, and GitHub
artifact attestations. Publishing that reviewed draft triggers
`release.yml`; tag creation alone never publishes to crates.io.
