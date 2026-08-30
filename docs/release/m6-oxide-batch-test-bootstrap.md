# M6 `oxide-batch-test` Publication Bootstrap

**State:** Pending -- required before the first release that includes
`oxide-batch-test`

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

`.github/workflows/release.yml`'s publication job uses OIDC Trusted Publishing
only after a verification job confirms that every crate name is registered and
that any already-published exact version has the matching checksum. The
verification job fails loudly and by name if any accepted crate is not yet
registered on crates.io -- `oxide-batch-test` included, until this bootstrap
runs. That failure is the intended, fail-closed signal to complete this
document's steps before retrying the release, not a defect to silence.

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
8. From the immutable tag, manually publish only `oxide-batch-test 0.6.0`
   with a short-lived local crates.io token.
9. Immediately configure its Trusted Publisher: owner
   `luceat-lux-vestra`, repository `oxide-batch`, workflow `release.yml`,
   environment `release`.
10. Remove/logout/delete the local token and any temporary copy.
11. Only then publish or re-run the reviewed GitHub Release so OIDC Trusted
    Publishing handles the release set under the accepted recovery contract.
12. Perform post-publish crates.io, docs.rs, clean-consumer, checksum, SBOM,
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
release order, rejects an unregistered crate before OIDC, and compares local
archives with existing crates.io checksums. A partial publication is resumed
only for exact versions still absent from crates.io; already-published
versions are never blindly retried. `workflow_dispatch` remains verification
only and never authenticates to or publishes on crates.io.

## Later releases

This is a one-time bootstrap for `oxide-batch-test` specifically. If a future
milestone adds another newly published crate, that crate requires its own
separately reviewed first-publication bootstrap decision; do not generalize
this exception silently.
