# Crate Publishing Policy

## Workspace strategy

OxideBatch uses one Git repository and one Cargo workspace. The public
`oxide-batch` crate is the facade and supported entry point.

Potential future workspace crates include:

| Package | Intended role | Initial publication |
| --- | --- | --- |
| `oxide-batch` | Stable facade and curated re-exports | Public |
| `oxide-batch-core` | Domain model and execution contracts | Undecided |
| `oxide-batch-plan` | Definition graph and compiled plan | Undecided |
| `oxide-batch-engine` | Execution engine | Undecided |
| `oxide-batch-engine-tokio` | Explicit Tokio engine, if a separate boundary is justified | Undecided |
| `oxide-batch-item` | Item/chunk/stream contracts | Undecided |
| `oxide-batch-repository` | Persistence interfaces | Undecided |
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

- Publish only crates that have a documented user or integration boundary.
- Mark internal-only packages with `publish = false`.
- Never publish secrets, private fixtures, generated credentials, or internal
  incident data.
- Run `cargo package --list` and `cargo publish --dry-run` before publication.
- Publish workspace crates in dependency order.
- Use exact version requirements for workspace dependencies in published
  manifests while retaining local `path` dependencies.
- Create a reviewed changelog entry and immutable Git tag for every release.
- Published versions are permanent and must never be treated as disposable.

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
