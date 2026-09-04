# Security Policy

## Supported versions

OxideBatch is an early-alpha project with a published `0.6.0` release line.
Security fixes are developed on the latest `main` and, when a vulnerability
applies to a published release, the maintainer decides whether to issue a
patched `0.6.x` release based on severity, exploitability, and compatibility
risk. Older release lines are not maintained in parallel.

The published support matrix describes the runtime and database combinations
covered by the current release. Publication of a release does not imply a
production-readiness or full Spring Batch compatibility claim.

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability.

Use GitHub's private vulnerability reporting feature:

1. Open the repository's **Security** tab.
2. Select **Advisories**.
3. Select **Report a vulnerability**.

Include:

- Affected version or commit
- Impact and realistic attack scenario
- Reproduction steps or proof of concept
- Suggested mitigation, if known
- Whether the issue is already public

The maintainer will acknowledge a complete report as soon as practical,
coordinate remediation and disclosure, and credit the reporter if requested.

Never include production credentials, personal data, or third-party secrets in
a report.

## Conduct reports are separate

Security vulnerability reports and Code of Conduct reports are different
processes. The private security-advisory form above is only for vulnerabilities.
For abusive, harassing, or otherwise unacceptable community behavior, follow
the private reporting instructions in `CODE_OF_CONDUCT.md`; do not describe a
conduct complaint as a security vulnerability.
