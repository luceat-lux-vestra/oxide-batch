# Maintainer Continuity and Access

**State:** Proposed

The project currently has one maintainer. Continuity controls reduce accidental
loss of the repository or release path without publishing secret recovery
material.

## Access inventory

The maintainer keeps a private inventory of:

- GitHub account recovery and strong multi-factor authentication;
- repository administration and ruleset bypass;
- crates.io ownership and trusted publisher binding;
- signing identity used for protected release tags;
- domain/documentation services if introduced;
- vulnerability-report notification route;
- backup location for non-public recovery instructions.

The repository records which systems exist and who owns them, never recovery
codes or private keys.

## Current controls

- branch and tag rules protect reviewed history;
- release publishing uses repository-bound OIDC rather than a stored token;
- workflows have least-privilege default permissions and pinned actions;
- crates.io and GitHub ownership are currently held by the same maintainer;
- private vulnerability reporting is enabled.

## Required before first runtime release

- choose and test signed release-tag procedure;
- test account recovery without exposing credentials;
- verify no long-lived crates.io token exists locally or in GitHub;
- export a private, encrypted recovery runbook;
- document emergency ruleset/tag bypass and subsequent audit;
- identify a trusted successor or explicitly accept the residual single-owner
  risk for the prerelease.

## Required before 1.0

- add at least one active maintainer or document why stable releases remain
  single-maintainer;
- define nomination, access grant, removal, inactivity, and conflict procedures;
- require independent review for release/security changes when maintainers
  permit;
- verify crates.io owner and GitHub organization/repository continuity;
- conduct a release-access recovery exercise.

## Change and offboarding

Access changes are least privilege and auditable. Removing a maintainer includes
repository roles, environment approvals, package ownership, signing/recovery
material, notification channels, and outstanding security reports. Public
governance changes do not disclose private credentials.
