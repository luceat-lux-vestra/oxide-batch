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
and [#145](https://github.com/luceat-lux-vestra/oxide-batch/issues/145) add
`oxide-batch-test` to the accepted release set (see
[`crate-publishing.md`](../governance/crate-publishing.md)), but crates.io
Trusted Publishing cannot create a crate name -- only publish a new version of
one that already exists. `oxide-batch-test` has never been published, so its
first version needs the same one-time manual bootstrap the original four
newly published crates needed at `0.5.0`.

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

Do not begin until all of the following are true:

- the release-preparation PR that will first include `oxide-batch-test` is
  merged;
- the merge commit is the intended release tree;
- required CI, including the Evidence workflow, is green on that tree;
- `cargo xtask package` and `cargo xtask release-crates` pass;
- the protected release tag points at that reviewed commit;
- the tag-triggered draft GitHub Release was prepared successfully and its
  package, checksum, SBOM, and attestation assets were reviewed;
- a short-lived crates.io API token is available locally to the maintainer
  performing the bootstrap.

Do not store that token in the repository or as a long-lived GitHub secret.

## Required later sequence

This sequence is Phase B and must not be performed as part of release
preparation:

1. Independently review the release-preparation PR.
2. Merge the reviewed release-preparation PR.
3. Independently confirm that the merge commit is the intended release tree.
4. Confirm the required exact-tree CI and evidence are green.
5. Create the protected immutable `v0.6.0` tag from that exact commit.
6. Let the tag workflow produce the draft release artifacts.
7. Review every `.crate`, checksum, package-scoped SBOM, attestation, and
   provenance link.
8. Dispatch `release.yml` from the reviewed, merged `refs/heads/main` — not
   from the immutable tag's own copy of the workflow file, which is frozen at
   whatever `release.yml` looked like when `v0.6.0` was tagged and cannot
   carry any later-reviewed fix (`#215` fixed exactly this: dispatching the
   tag's stale copy could not even see the still-draft Release to verify it,
   because that copy predates the job-scoped `contents: write` fix). Use
   `mode: publish-registered`, `tag: v0.6.0`, and confirmation
   `PUBLISH_REGISTERED_ONLY`. `verify-release` itself still checks out the
   product from the immutable `v0.6.0` tag; only the workflow *definition*
   being run comes from `main`. This uses OIDC Trusted Publishing to publish
   only the already-registered crates in derived dependency order and leaves
   `oxide-batch-test` unpublished.
9. From the same immutable tag, manually publish only
   `oxide-batch-test 0.6.0` with a short-lived local crates.io token. The
   facade is already on crates.io, so this dependency order is valid.
10. Immediately configure its Trusted Publisher: owner
   `luceat-lux-vestra`, repository `oxide-batch`, workflow `release.yml`,
   environment `release`.
11. Remove/logout/delete the local token and any temporary copy.
12. Only then publish or re-run the reviewed GitHub Release so normal OIDC
    Publishing handles the release set under the accepted recovery contract.
13. Perform post-publish crates.io, docs.rs, clean-consumer, checksum, SBOM,
    provenance, and PostgreSQL release-smoke verification.

The manual bootstrap command, shown for the later phase only, is:

```console
git checkout v0.6.0
cargo login
cargo publish -p oxide-batch-test --locked
cargo logout
```

Never store the token in the repository or as a long-lived GitHub secret.

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
