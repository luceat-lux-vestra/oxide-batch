# ADR-0010: Extracted Implementation Crate Publication

- **State:** Accepted
- **Date:** 2026-08-03
- **Owners:** architecture and release maintainers
- **Deciders:** project owner
- **Governing RFC:**
  [RFC-0011](../../rfcs/0011-publication-of-extracted-implementation-crates.md)
- **Partially supersedes:** [ADR-0001](0001-workspace-and-facade.md)

## Context

ADR-0001 keeps implementation crates `publish = false` until a public
integration boundary is approved, and the M5
[staged crate-extraction contract](../crate-extraction.md) repeats that rule for
the three boundaries it authorizes. The `oxide-batch` facade is published, and
the M5 support bounds in the
[support matrix](../../release/support-matrix.md) commit the milestone to
publishing the production preview as a `0.x` release.

Cargo rewrites a published package's path dependencies to registry
dependencies when it verifies the archive, so a publishable crate cannot depend
on an unpublished one. Reproduced on cargo 1.97.1, `cargo publish --dry-run`
for a publishable crate with a `publish = false` path dependency fails with
`no matching package named ... found`. The first extraction stage would
therefore make the facade unpublishable and fail the design gate's own
`package_dry_run_succeeds_for_every_workspace_crate` scenario.

The conflict is between accepted documents and cannot be resolved by
implementation.

## Decision

Publish `oxide-batch-core`, `oxide-batch-repository`, and `oxide-batch-plan` as
internal crates:

- each sets `publish = ["crates-io"]` and inherits the workspace version;
- workspace dependents require them by path plus an exact `=` version;
- they are released only in the same release as the facade, in dependency
  order, through `cargo publish --workspace`;
- their rustdoc landing page and README state, as their first content, that
  they are implementation detail with no stability promise and that
  `oxide-batch` is the supported entry point;
- they carry no independent cadence, support window, ledger row, or
  supported-configuration entry.

ADR-0001's private-by-default rule is superseded for these three crates only.
It stands unchanged for every other boundary, and publication as an internal
crate is not the public-integration-boundary approval ADR-0001 reserves.

## Consequences

- the facade stays publishable through every extraction stage, and the M5
  preview release remains reachable;
- three crates.io names are claimed permanently by real code;
- the release becomes a five-crate ordered operation with more partial-failure
  surface;
- stage reversal is free only before the release that publishes the stage;
  afterwards the published version is permanent and reversal leaves an orphan
  version that nothing references;
- users may take unsupported direct dependencies on internal crates despite the
  disclosure;
- every extraction boundary the M5 gate does not authorize remains unextracted
  and unpublished.

## Alternatives considered

- Defer extraction past the preview release, preserving ADR-0001 exactly. It
  costs only sequencing and satisfies M5's definition of done, but moves
  structural repackaging to after the first public release, when
  facade-equivalence obligations are strongest.
- Keep the crates private and stop publishing the facade, which contradicts
  RFC-0001's published preview outcome.
- Publish with `--no-verify`, which produces an archive that resolves to
  nothing and does not build for users.
- Vendor extracted sources into the facade package, which cargo does not
  support and which is not an extraction.

## Validation

`cargo xtask package` runs `cargo package --workspace --list` and `cargo
publish --workspace --dry-run --locked`. The dry run resolves unpublished
workspace members through a temporary local registry and is the executable
evidence that the facade remains publishable after each stage. `cargo xtask
deps` enforces the forbidden-dependency and cycle rules that keep the internal
crates from acquiring runtime, driver, command-line, or telemetry-SDK
dependencies. The facade public-API snapshot, golden fingerprint vectors, and
normalized repository-write traces hold the equivalence and durable-invariance
obligations.

## Revisit triggers

Revisit if a crates.io name becomes unavailable, if users depend on an internal
crate widely enough to create a de facto support obligation, if an internal
crate acquires a documented user or integration boundary and becomes a
candidate for supported status, or if cargo gains a mechanism that lets a
published package carry unpublished workspace dependencies.
