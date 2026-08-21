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

`.github/workflows/release.yml`'s "Publish to crates.io" step uses OIDC
Trusted Publishing and runs unconditionally for every non-bootstrap release
(every release after `0.5.0`). A "Verify every released crate already exists
on crates.io" pre-flight step fails that workflow, loudly and by name, if any
crate in the accepted release set is not yet registered on crates.io --
`oxide-batch-test` included, until this bootstrap runs. That failure is the
intended, fail-closed signal to complete this document's steps before
retrying the release, not a defect to silence.

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

## Bootstrap publish

Check out the immutable release tag and authenticate locally:

```console
git checkout <the release tag>
cargo login
```

Publish just the one new crate:

```console
cargo publish -p oxide-batch-test --locked
```

A crate version already present on crates.io must not be blindly retried;
crates.io versions are immutable.

## Configure Trusted Publishing

Immediately after `oxide-batch-test` exists on crates.io, configure its
Trusted Publisher to the same identity the other five released crates use:

- GitHub owner: `luceat-lux-vestra`
- repository: `oxide-batch`
- workflow: `release.yml`
- environment: `release`

After the crate is present and its publisher is configured, remove the local
API token with `cargo logout` and delete any temporary copy of the token.

## Publish the GitHub Release

Only after this manual bootstrap is complete, publish (or re-run) the
reviewed draft GitHub Release. `release.yml`'s pre-flight existence check
then passes, and every crate -- `oxide-batch-test` included -- publishes
through the normal OIDC `release.yml` path from this point forward.

## Later releases

This is a one-time bootstrap for `oxide-batch-test` specifically. If a future
milestone adds another newly published crate, that crate requires its own
separately reviewed first-publication bootstrap decision; do not generalize
this exception silently.
