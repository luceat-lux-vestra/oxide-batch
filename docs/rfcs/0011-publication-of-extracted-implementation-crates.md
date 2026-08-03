# RFC-0011: Publication of Extracted Implementation Crates

- **State:** Accepted
- **Created:** 2026-08-03
- **Owner:** architecture and release maintainers
- **Target milestone:** M5
- **Related issues/ADRs:** issue
  [#99](https://github.com/luceat-lux-vestra/oxide-batch/issues/99),
  [RFC-0003](0003-target-workspace-boundaries.md),
  [ADR-0001](../architecture/decisions/0001-workspace-and-facade.md),
  [ADR-0010](../architecture/decisions/0010-extracted-crate-publication.md)

## Summary

Publish the three implementation crates the M5
[staged crate-extraction contract](../architecture/crate-extraction.md)
authorizes — `oxide-batch-core`, `oxide-batch-repository`, and
`oxide-batch-plan` — as versioned crates released in lockstep with the
`oxide-batch` facade, each documented as internal implementation with no
independent API promise. Boundaries this gate does not authorize remain
unextracted and unpublished.

## Motivation

Two accepted rules cannot both hold once the first extraction stage lands.

**The extracted crates must be private.** ADR-0001 keeps implementation crates
`publish = false` until a public integration boundary is separately approved.
RFC-0003 repeats the rule, and the extraction contract states that every
extracted crate is `publish = false`.

**The facade must stay publishable.** `oxide-batch` is `publish =
["crates-io"]`, `0.1.0-alpha.1` is already released, and the M5 support bounds
in the [support matrix](../release/support-matrix.md) commit the milestone to
publishing the production preview as a `0.x` release.
The M5 design gate additionally requires the scenario
`package_dry_run_succeeds_for_every_workspace_crate` as extraction evidence.

Cargo does not permit both. A published package's path dependencies are
rewritten to registry dependencies when the `.crate` archive is verified, so a
publishable crate cannot depend on an unpublished one. Reproduced on cargo
1.97.1 with a two-crate workspace in which `b` is publishable and depends on a
`publish = false` path dependency `a`:

```text
$ cargo publish -p b --dry-run
   Packaging b v0.1.0
error: failed to prepare local package for uploading

Caused by:
  no matching package named `a` found
  location searched: crates.io index
  required by package `b v0.1.0`
```

The failure is unconditional and is not a lockfile, feature, or offline
artifact. Under the accepted rules, the first extraction stage would therefore
make the facade unpublishable, break the release workflow, and fail the design
gate's own packaging scenario. This is a conflict between accepted documents,
so it requires a decision rather than an implementation choice.

## Goals and non-goals

**Goals.** Keep `oxide-batch` publishable through every extraction stage; keep
the facade the only supported entry point; keep the three crates' internal
status explicit to users and reviewers; keep the release a single ordered
operation.

**Non-goals.** This RFC does not authorize any additional extraction boundary,
does not give the three crates an independent release cadence, support window,
or stability promise, does not make workspace-internal paths a compatibility
surface, and does not change the facade's public API.

## Terminology

An **internal published crate** is a crate whose artifacts exist on crates.io
because a published dependent requires them, and whose API carries no
compatibility, support, or documentation promise of its own. A **supported
entry point** is a crate whose API is governed by the pre-1.0 evolution policy
and the compatibility ledger. `oxide-batch` is the only supported entry point.

## Detailed design

**Manifest rules.** `oxide-batch-core`, `oxide-batch-repository`, and
`oxide-batch-plan` set `publish = ["crates-io"]` and inherit the workspace
version. Every dependent inside the workspace requires them by path plus an
exact `=` version, so a published archive can never resolve a different
version than the workspace built and tested.

**Version and release coupling.** The three crates carry the facade's version
and are released only in the same release as the facade, in dependency order:
core, repository, plan, `oxide-batch`, `oxide-batch-cli`. `cargo publish
--workspace` performs the ordering and verifies unpublished members against a
temporary local registry, so the dry run succeeds before any upload. No
version of an internal crate is published without the facade version that
consumes it.

**Disclosure.** Each internal crate's rustdoc landing page and README state, as
their first content, that the crate is OxideBatch implementation detail, that
its API may change in any release without a deprecation period, and that
`oxide-batch` is the supported entry point. The crates are excluded from the
supported-configuration matrix and from every ledger evidence link that names a
user-facing API.

**Unchanged rules.** The forbidden-dependency rules, facade and API
equivalence obligations, durable-invariance obligation, packaging checks,
measurements, and per-stage reversal in the extraction contract are unchanged.
Engine, item, adapter, observability, test-kit, distributed-protocol, and
integration boundaries remain deferred past M5 and unpublished. Publication of
an internal crate is not the "public integration boundary approval" ADR-0001
reserves; that approval remains a separate decision that would move a crate
from internal to supported.

## Compatibility

The facade's public API, supported import paths, persisted bytes, transaction
boundaries, lifecycle writes, restart selection, definition fingerprints, and
normalized traces are unaffected. This RFC changes packaging only.

Three crates.io names are claimed by real code rather than by placeholder
releases, which the
[crate publishing policy](../governance/crate-publishing.md) permits. If a
name is unavailable at release time, the implementation crate is renamed
without touching the facade, exactly as the existing name-allocation rule
provides.

Users can technically depend on an internal crate directly. Doing so is
unsupported: it is outside the compatibility ledger, receives no pre-1.0
evolution notice, and may break in any release.

## Security and privacy

Each additional published artifact enters the release evidence set and must
carry the same `.crate` checksum, package-scoped CycloneDX SBOM, and GitHub
artifact attestation the facade carries. The trusted-publishing configuration
is per repository and workflow rather than per crate, so no new long-lived
credential is introduced. Internal crates publish no fixtures, credentials, or
incident data; the existing packaging include rules apply unchanged.

## Failure and recovery

A partially successful multi-crate publish leaves some crates uploaded. The
[release checklist](../release/release-checklist.md) already forbids blind
retry and requires inspection followed by a corrected version; that rule now
covers five crates instead of two.

Reversal changes character after publication. Before the release that carries
it, reverting an extraction stage is free: the commit is reverted, the crate
directory disappears, and no artifact exists. After that release, the
published version is permanent: reverting the stage removes the dependency but
leaves an orphan version on crates.io that nothing references. A stage must be
reverted before the release that publishes it, or its reversal is recorded as a
new version that stops depending on the orphaned crate. Yanking withdraws a
version from new resolution but never deletes it.

## Alternatives

1. **Defer extraction past the preview release.** Publish the preview from the
   single implementation crate and resequence the three stages to a later
   milestone. This preserves ADR-0001 exactly and costs only sequencing, and
   M5's definition of done constrains "each completed crate extraction" rather
   than requiring any. Rejected because the structural repackaging is cheapest
   before the first preview release, when facade-equivalence obligations are
   weakest and no user has been asked to upgrade.
2. **Keep the crates private and stop publishing the facade.** Rejected: it
   contradicts [RFC-0001](0001-m5-preview-and-project-wide-1-0.md), which makes
   a published `0.x` preview the milestone's outcome.
3. **Publish the facade with `--no-verify`.** Rejected: the archive would still
   record unresolvable registry dependencies, so the published crate would not
   build for any user.
4. **Vendor the extracted sources into the facade package.** Rejected: cargo
   has no mechanism for it, and a package that carries its dependencies'
   sources is not an extraction.
5. **Status quo, one implementation crate.** Rejected by RFC-0003 for
   dependency and performance isolation.

## Test and evidence plan

- `cargo xtask deps` enforces the per-crate forbidden-dependency rules and the
  workspace cycle prohibition, and fails the build on violation.
- `cargo xtask package` runs `cargo package --workspace --list` and `cargo
  publish --workspace --dry-run --locked`, which is the executable form of
  `package_dry_run_succeeds_for_every_workspace_crate` and the direct evidence
  that this RFC's problem is solved.
- The facade public-API snapshot and import-resolution tests hold the supported
  surface byte-identical across every stage.
- Golden definition fingerprints and normalized repository-write traces hold
  the durable-invariance obligation.
- The existing unit, property, contract, conformance, crash, and PostgreSQL
  suites run unchanged per stage.

## Rollout and rollback

The decision lands before extraction stage 1. Each stage is one revertible
commit that adds its crate with the manifest rules above. The first release to
carry the crates is the M5 production preview; before it, every stage is
reversible with no external effect.

## Drawbacks and risks

Three permanent artifacts and namespace claims are created for a repackaging
with no user-visible benefit, which is the cost ADR-0001 was written to avoid.
Users may take direct dependencies despite the disclosure. The release becomes
a five-crate ordered operation with more partial-failure surface. Reversal
after publication is no longer free.

Risk R-008 is updated and risk R-019 is added to the
[risk register](../project/risk-register.md).

## Unresolved questions

None. The choice between this proposal and alternative 1 was the only open
question and is decided below.

## Decision

**Accepted on 2026-08-03 by the project owner.** The three M5-authorized
implementation crates are published as internal crates in lockstep with the
facade, on the condition that their internal status is disclosed in rustdoc and
README, that they gain no independent cadence or support promise, and that no
further boundary is extracted or published without its own decision. The
architecture decision is recorded as
[ADR-0010](../architecture/decisions/0010-extracted-crate-publication.md), which
partially supersedes ADR-0001 for these three crates only. Follow-up work is
issue [#99](https://github.com/luceat-lux-vestra/oxide-batch/issues/99).
