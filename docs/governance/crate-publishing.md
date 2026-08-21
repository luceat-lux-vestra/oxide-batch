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
| `oxide-batch-test` | Application-facing test kit (deterministic clock/ID, job/step/component harnesses, failure injection, restart harness) | Public, added to the accepted release set in [#145](https://github.com/luceat-lux-vestra/oxide-batch/issues/145) per the M6 [Gate G](../project/m6-design-gate-evidence.md#gate-g--oxide-batch-test-boundary) decision; released in lockstep with the facade starting from its first published version, which needs its own reviewed first-publication bootstrap (see [`m6-oxide-batch-test-bootstrap.md`](../release/m6-oxide-batch-test-bootstrap.md)) before that first release, since it did not exist at `0.5.0` |
| `oxide-batch-cli` | Operational command-line interface | Public, released in lockstep with the facade from `0.5.0` ([RFC-0011](../rfcs/0011-publication-of-extracted-implementation-crates.md)) |

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
separate reservation operation. The facade name `oxide-batch` was claimed by
the initial release. Predictable subcrate names are not claimed with empty
placeholder releases; when a real release first introduces one, that crate is
bootstrapped from the reviewed release tree under the first-publication rule
below.

If a future subcrate name becomes unavailable, the implementation may remain
internal or use another name without changing the public `oxide-batch` facade.

## Initial release and Trusted Publishing

The initial facade version is `0.1.0-alpha.1`. It was published manually to
establish crates.io ownership while providing real, buildable package metadata
and documentation.

Normal releases use crates.io Trusted Publishing. The trusted publisher is
bound to:

- GitHub owner: `luceat-lux-vestra`
- Repository: `oxide-batch`
- Workflow: `release.yml`
- Environment: `release`

The release workflow exchanges GitHub's OIDC identity for a short-lived
crates.io token. Long-lived crates.io tokens must not be stored in GitHub.

### First-publication exception

crates.io Trusted Publishing cannot create a crate name before that crate has a
first published version. A newly approved real crate therefore has one narrow
bootstrap exception:

- the first version is published manually from the exact reviewed, immutable
  release tag using a short-lived maintainer crates.io API token;
- the publish follows the accepted workspace dependency order and stops on the
  first failure;
- an already published version is never blindly retried after a partial
  operation;
- the crate's Trusted Publisher is configured immediately after its first
  version exists;
- the local token is removed after bootstrap and is never persisted in the
  repository or as a long-lived GitHub secret;
- every later version uses Trusted Publishing through `release.yml`.

The M5 `0.5.0` release is the first use of this exception because
`oxide-batch-core`, `oxide-batch-repository`, `oxide-batch-plan`, and
`oxide-batch-cli` are first published alongside the already-owned facade. The
exact procedure is [`m5-0.5.0-bootstrap.md`](../release/m5-0.5.0-bootstrap.md).
The `0.5.0` release workflow verifies the manually published registry archives
against packages rebuilt from the immutable tag instead of attempting to
publish the same versions twice.

A protected version tag first creates a draft GitHub Release containing the
exact `.crate`, SHA-256 checksums, a package-scoped CycloneDX SBOM, and GitHub
artifact attestations. Publishing that reviewed draft triggers `release.yml`;
tag creation alone never publishes to crates.io.
