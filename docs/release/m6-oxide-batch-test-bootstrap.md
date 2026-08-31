# M6 `oxide-batch-test` Publication Bootstrap

**State:** Complete for `v0.6.0`

All phases below are done and independently re-verified; this record is kept
as the design rationale and as the completion evidence for the first
`oxide-batch-test` release, per
[Issue #190](https://github.com/luceat-lux-vestra/oxide-batch/issues/190).
A future newly-published crate needs its own separately reviewed bootstrap
record (see "Later releases" below) rather than reopening this one.

| Phase | State | Evidence |
| --- | --- | --- |
| Manual `oxide-batch-test` bootstrap | Done | version `0.6.0`, checksum `8bad1e49c191f276402c7092b441c2a38dc33ff57b49d5911c801eafa42cb49e`, not yanked, Rust `1.95`, published `2026-08-31T02:54:19Z`; PASS recorded in [issue comment 5473131482](https://github.com/luceat-lux-vestra/oxide-batch/issues/190#issuecomment-5473131482) |
| Trusted Publisher prerequisite | Configured (owner-confirmed) | owner `luceat-lux-vestra`, repository `oxide-batch`, workflow `release.yml`, environment `release`; crates.io exposes no public readback for this admin setting, so it is recorded as user-confirmed, not independently API-verified |
| Registered publication (five already-registered crates) | Done | run [`33348113358`](https://github.com/luceat-lux-vestra/oxide-batch/actions/runs/33348113358), dispatched from the reviewed `main` copy of `release.yml` (PR [#216](https://github.com/luceat-lux-vestra/oxide-batch/pull/216), squash `7281c5245d558cc9df10e9f88513383acce0e36c`), conclusion `success` |
| Public GitHub Release | Done | release id `379278864`, `draft:false`, published `2026-08-31T03:11:29Z`; pre-publication checkpoint in [issue comment 5473193240](https://github.com/luceat-lux-vestra/oxide-batch/issues/190#issuecomment-5473193240) |
| Release-event verification | Done | run [`33353036930`](https://github.com/luceat-lux-vestra/oxide-batch/actions/runs/33353036930) (event `release`, exact tag `v0.6.0`, exact commit `e9ce3891a9c37959ad1022a62dbf723c9edd2d65`), conclusion `success`; PASS recorded in [issue comment 5473297562](https://github.com/luceat-lux-vestra/oxide-batch/issues/190#issuecomment-5473297562) |
| Post-publish validation | Done | crates.io, docs.rs, clean external consumer, PostgreSQL release smoke, checksum manifest, SBOM, and attestation-subject verification, recorded in [`v0.6.0-post-publish.md`](evidence/v0.6.0-post-publish.md) |

[`m5-0.5.0-bootstrap.md`](m5-0.5.0-bootstrap.md) already states the rule this
record applies: "If a future milestone adds another newly published crate,
that crate requires a separately reviewed first-publication bootstrap
decision; do not generalize the `0.5.0` exception silently." M6's
[Gate G](../project/m6-design-gate-evidence.md#gate-g--oxide-batch-test-boundary)
and [#145](https://github.com/luceat-lux-vestra/oxide-batch/issues/145) added
`oxide-batch-test` to the accepted release set (see
[`crate-publishing.md`](../governance/crate-publishing.md)). crates.io
Trusted Publishing cannot create a crate name -- only publish a new version of
one that already exists. Before `v0.6.0`, `oxide-batch-test` had never been
published, so its first version needed the same one-time manual bootstrap the
original four newly published crates needed at `0.5.0`. That bootstrap is now
complete; see the phase table above.

## Why this cannot be silently automatic

`.github/workflows/release.yml` derives the six-crate order from the immutable
tag's Cargo metadata. Its normal release path fails loudly and by name before
OIDC if any accepted crate name is unregistered. The one explicit
`workflow_dispatch` mode used here is `publish-registered`, and it requires the
operator to type `PUBLISH_REGISTERED_ONLY`. That mode publishes only pending
versions of already-registered names, leaves the final unregistered crate for
this document's manual bootstrap, and never creates a crate name. The default
dispatch mode remains verification-only.

## Preconditions

All of the following were confirmed true before the `v0.6.0` bootstrap began,
and remain the checklist a future newly-published crate's own bootstrap must
satisfy:

- the release-preparation PR that first included `oxide-batch-test` was
  merged;
- the merge commit was the intended release tree;
- required CI, including the Evidence workflow, was green on that tree;
- `cargo xtask package` and `cargo xtask release-crates` passed;
- the protected release tag pointed at that reviewed commit;
- the tag-triggered draft GitHub Release was prepared successfully and its
  package, checksum, SBOM, and attestation assets were reviewed;
- a short-lived crates.io API token was available locally to the maintainer
  performing the bootstrap.

Do not store that token in the repository or as a long-lived GitHub secret.

## Executed sequence for `v0.6.0` (historical record)

This is the exact Phase B sequence completed for the `oxide-batch-test
0.6.0` bootstrap, performed separately from release preparation. A future
newly-published crate follows this same sequence as its own one-time
bootstrap (see "Later releases" below).

1. The release-preparation PR was independently reviewed.
2. The reviewed release-preparation PR was merged.
3. The merge commit was independently confirmed as the intended release tree.
4. The required exact-tree CI and evidence were confirmed green.
5. The protected immutable `v0.6.0` tag was created from that exact commit.
6. The tag workflow produced the draft release artifacts.
7. Every `.crate`, checksum, package-scoped SBOM, attestation, and
   provenance link was reviewed.
8. `release.yml` was dispatched from the reviewed, merged `refs/heads/main`
   — not from the immutable tag's own copy of the workflow file, which was
   frozen at whatever `release.yml` looked like when `v0.6.0` was tagged and
   could not carry any later-reviewed fix (`#215` fixed exactly this:
   dispatching the tag's stale copy could not even see the still-draft
   Release to verify it, because that copy predates the job-scoped
   `contents: write` fix). Run
   [`33348113358`](https://github.com/luceat-lux-vestra/oxide-batch/actions/runs/33348113358)
   used `mode: publish-registered`, `tag: v0.6.0`, and confirmation
   `PUBLISH_REGISTERED_ONLY`. `verify-release` itself still checked out the
   product from the immutable `v0.6.0` tag; only the workflow *definition*
   that ran came from `main`. This used OIDC Trusted Publishing to publish
   the five already-registered crates in derived dependency order and left
   `oxide-batch-test` unpublished.
9. From the same immutable tag, `oxide-batch-test 0.6.0` was manually
   published with a short-lived local crates.io token. The facade was
   already on crates.io, so this dependency order was valid.
10. Its Trusted Publisher was immediately configured: owner
    `luceat-lux-vestra`, repository `oxide-batch`, workflow `release.yml`,
    environment `release` (owner-confirmed; see the phase table above).
11. The local token and any temporary copy were removed/logged out/deleted.
12. The reviewed GitHub Release (`379278864`) was then published so normal
    OIDC Publishing handled the release set under the accepted recovery
    contract.
13. Post-publish crates.io, docs.rs, clean-consumer, checksum, SBOM,
    provenance, and PostgreSQL release-smoke verification was performed; see
    [`v0.6.0-post-publish.md`](evidence/v0.6.0-post-publish.md).

The manual bootstrap command used for step 9 above was:

```console
git checkout v0.6.0
cargo login
cargo publish -p oxide-batch-test --locked
cargo logout
```

A future one-time bootstrap must likewise never store its token in the
repository or as a long-lived GitHub secret.

## Later-release workflow behavior

`release.yml` checks the immutable tag tree, verifies the complete current
release order, rejects an unregistered crate on the normal release path, and
compares local archives with existing crates.io checksums. The explicit
`publish-registered` dispatch is restricted by its required confirmation and
publishes only registered names; it exists solely to put the five existing
names on the registry before the one-time test-kit bootstrap. Every crate is
rechecked immediately before a publish attempt. A partial publication is
therefore idempotent: an exact matching version is skipped, a checksum mismatch
or unexpected registry response fails closed, and only an exact 404 can lead to
an upload. The default `workflow_dispatch` mode remains verification-only and
does not authenticate or publish.

## Later releases

This is a one-time bootstrap for `oxide-batch-test` specifically. If a future
milestone adds another newly published crate, that crate requires its own
separately reviewed first-publication bootstrap decision; do not generalize
this exception silently.
