# Release Checklist

**State:** Accepted

This checklist supplements the automated release workflow. Multi-crate steps
apply only after additional public crates are approved.

## Prepare

- [ ] Confirm target version/channel and milestone exit criteria.
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

## Publish

- [ ] Merge the reviewed release PR.
- [ ] Create the protected, signed `v<version>` tag from the reviewed commit.
- [ ] Review the tag-generated draft Release, `.crate`, checksum, SBOM, and
      package attestations.
- [ ] Publish the GitHub Release with notes and migration warnings.
- [ ] Let Trusted Publishing release crates in dependency order:
      `oxide-batch-core`, `oxide-batch-repository`, `oxide-batch-plan`,
      `oxide-batch`, then `oxide-batch-cli`. Internal crates carry the facade
      version and are never released on their own.
- [ ] Do not retry a partially successful multi-crate publish blindly; inspect
      registry state and prepare compatible remaining versions.

## Verify

- [ ] Confirm crates.io ownership, metadata, dependency versions, and checksum.
- [ ] Confirm docs.rs builds and public links resolve.
- [ ] Build a clean consumer project from crates.io with documented features.
- [ ] Run the release smoke job against a supported PostgreSQL version.
- [ ] Confirm GitHub artifacts, SBOM, provenance, and release notes.
- [ ] Announce known limitations and support window.

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
