# GitHub Actions static security gate

The repository's `actions-security` check is the fail-closed static audit for checked-in GitHub Actions workflows. It is produced by the reusable `.github/workflows/dependency-review.yml` job inside the pull-request DAG and is intentionally not path-filtered.

The check combines three independent controls:

- actionlint for workflow syntax and structural validation;
- zizmor in auditor mode at low severity, with only narrowly scoped checked-in suppressions;
- `.github/scripts/validate_actions_security.py` for repository-specific invariants that must remain deterministic even if scanner behavior changes.

The deterministic policy requires immutable full-SHA external `uses:` references, disabled checkout credential persistence unless explicitly justified, job-scoped write permissions, safe `pull_request_target` boundaries, no direct untrusted-context interpolation into program text, and digest-pinned service/container images where practical. Its negative fixtures live in `.github/scripts/test_validate_actions_security.py`.

`dependency-review` is the sibling dependency-diff check in the same reusable workflow. Both checks feed the single PR DAG in `.github/workflows/pr-ci.yml`. The final ruleset authority is `Direct Merge Gate`, which fails closed unless the trusted scope classification and every applicable Rust, dependency, evidence, supply-chain, and M5 conformance job completes with the expected result.

No separate `pull_request_target` merge authority, workflow-run polling layer, caller-run correlation protocol, or protected-workflow blob allowlist is part of the current design. Pull-request labels remain isolated in `.github/workflows/pr-labeler.yml` with job-scoped write permissions and do not participate in merge authorization.

Ordinary pushes to `main` do not repeat pull-request validation. The exact final pull-request HEAD is the validation surface; scheduled/manual security and campaign workflows retain their independent responsibilities.
