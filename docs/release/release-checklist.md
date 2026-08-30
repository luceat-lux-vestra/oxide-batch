# Release Checklist

**State:** Accepted

This checklist supplements the automated release workflow. The accepted M6
release set is six crates; the first `oxide-batch-test` publication remains a
one-time manual bootstrap and is never generalized by the workflow.

## Prepare

- [ ] Confirm target version/channel and milestone exit criteria.
- [ ] For the M6 candidate, confirm package version `0.6.0` and prospective
      tag `v0.6.0`; neither exists until the later publication phase.
- [ ] Resolve release blockers and review open security/data-integrity issues.
- [ ] Update changelog, support matrix, compatibility matrix, and migration guide.
- [ ] Confirm every advertised compatibility claim names released `Verified`
      ledger rows, baseline version, and known divergences.
- [ ] Confirm preview, RC, stable, and readiness wording matches the approved
      milestone/release gate rather than a proposed roadmap label.
- [ ] Confirm public API and feature changes match SemVer.
- [ ] Confirm MSRV and supported database/platform matrix.
- [ ] Review dependency advisories, licenses, sources, and exceptions.
- [ ] Test schema upgrade from every supported source version.
- [ ] Complete required crash/restart, recovery, and rollback exercises.
- [ ] Verify all examples and documentation against packaged crates.

## Build evidence

- [ ] Run required, deep, and release-candidate CI suites.
- [ ] Record source commit, toolchain, package file list, and checksums.
- [ ] Generate SBOM, license report, and provenance/attestation.
- [ ] Run `cargo package --workspace --list` and inspect every package.
- [ ] Run `cargo publish --workspace --dry-run --locked`, which orders the
      dry run by dependency and resolves unpublished members locally.
- [ ] Install/test from generated package archives, not workspace paths.
- [ ] Verify no credential, private fixture, or unrelated file is packaged.
- [ ] Run `cargo xtask release-crates`; it must derive exactly the six
      publishable crates and their dependency order from Cargo metadata.

## Publish

- [ ] Merge the reviewed release PR.
- [ ] Create the protected, signed `v<version>` tag from the reviewed commit.
- [ ] Review the tag-generated draft Release, `.crate`, checksum, SBOM, and
      package attestations.
- [ ] Record the reviewed tag object/commit/tree and every published crate's
      exact `.crate`/SBOM SHA-256 into `docs/release/evidence/<tag>.json` and
      merge it to `main` before any `workflow_dispatch` recovery run against
      this tag. This manifest is the recovery workflow's independent trust
      anchor: the draft Release it recovers evidence into is mutable, so a
      recovery that only cross-checks the draft's own SBOM against the
      draft's own checksum manifest proves nothing if both were tampered
      with together. A merged, reviewed manifest on `main` is a separate
      trust domain from that mutable draft.
- [ ] If this release introduces a crate name that does not yet exist on
      crates.io, follow a reviewed first-publication bootstrap from the exact
      release tag. For M5 `0.5.0`, follow
      [`m5-0.5.0-bootstrap.md`](m5-0.5.0-bootstrap.md).
- [ ] For M6, first use the explicitly confirmed `release.yml`
      `publish-registered` dispatch to publish the five already-registered
      crates in derived dependency order. Then manually publish only
      `oxide-batch-test 0.6.0`, configure its Trusted Publisher immediately,
      remove the local token, and only then publish the reviewed GitHub
      Release. Follow
      [`m6-oxide-batch-test-bootstrap.md`](m6-oxide-batch-test-bootstrap.md).
- [ ] Publish the GitHub Release with notes and migration warnings.
- [ ] Let Trusted Publishing release crates in dependency order:
      `oxide-batch-core`, `oxide-batch-repository`, `oxide-batch-plan`,
      `oxide-batch`, `oxide-batch-cli`, `oxide-batch-test`, as derived by
      `cargo xtask release-order`. Internal crates carry the facade version and
      are never released on their own. A reviewed first-publication bootstrap
      is the only exception; the release workflow must verify rather than
      re-publish an already bootstrapped exact version.
- [ ] Do not retry a partially successful multi-crate publish blindly; the
      workflow must recheck each crate immediately before upload, skip only an
      exact matching checksum, and fail closed on mismatch or unexpected state.

## Verify

- [ ] Confirm crates.io ownership, metadata, dependency versions, and checksum.
- [ ] Confirm docs.rs builds and public links resolve.
- [ ] Build a clean consumer project from crates.io with documented features.
- [ ] Run the release smoke job against a supported PostgreSQL version.
- [ ] Confirm GitHub artifacts, SBOM, provenance, and release notes.
- [ ] Announce known limitations and support window.
- [ ] Promote candidate compatibility rows only after the named released
      version and post-publication evidence exist; campaign PASS alone is not
      release-backed `Verified` evidence.

## Close

- [ ] Close the milestone only after evidence links are attached.
- [ ] Open follow-up issues for accepted residual risks and deferred work.
- [ ] Verify release credentials remain short-lived and no local token persists.
- [ ] Record release retrospective for any manual step or failure.

## Emergency handling

Published crates.io versions are immutable. If a release is defective:

1. stop dependent publishing;
2. document affected versions and safe mitigation;
3. yank only when it protects users and does not break valid builds;
4. publish a corrected version rather than replacing artifacts;
5. issue a security advisory when confidentiality/integrity is involved;
6. preserve the incident and release audit trail.
