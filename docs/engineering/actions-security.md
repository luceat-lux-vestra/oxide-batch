# GitHub Actions static security gate

The repository's `actions-security` check is the fail-closed static audit for checked-in GitHub Actions workflows. It is a pull-request-time child authority produced by `.github/workflows/dependency-review.yml` and remains a canonical member of the active repository-level `merge-gate`. The workflow is intentionally not path-filtered.

The gate combines three independent controls:

- actionlint for workflow syntax and structural validation;
- zizmor in auditor mode at low severity, with only narrowly scoped checked-in suppressions;
- `.github/scripts/validate_actions_security.py` for repository-specific invariants that must remain deterministic even if scanner behavior changes.

The deterministic policy requires immutable full-SHA external `uses:` references, disabled checkout credential persistence unless explicitly justified, job-scoped write permissions, safe `pull_request_target` boundaries, no direct untrusted-context interpolation into program text, and digest-pinned service/container images where practical. Its negative fixtures live in `.github/scripts/test_validate_actions_security.py`.

`dependency-review` is the sibling pull-request-time dependency-diff authority in the same workflow. Both `dependency-review` and `actions-security` remain independently visible exact-HEAD checks and canonical `merge-gate` members, but neither is directly required by the live ruleset.

The repository-level aggregation cutover is complete: `.github/merge-gate-policy.json` records `repository_merge_gate.state = active`, `pending_ruleset_contexts` is empty, and the live `Protect main` ruleset directly requires only `merge-gate`. The protected-base gate fails closed unless every canonical child member's latest exact-HEAD execution succeeds.

Ordinary pushes to `main` do not repeat this pull-request validation. The current post-main policy has an empty ordinary-push workflow allowlist, so dependency review and Actions security evidence is proved on the exact final pull-request HEAD rather than rerun after squash merge.


## Base-trusted repository merge gate

The audited `.github/workflows/pr-labeler.yml` target-context path also hosts a
separate `merge-gate` job. This does not widen the label job's write authority:
workflow-level permissions remain empty, label writes stay job-local, and the
merge-gate job is read-only (`actions`, `contents`, and `pull-requests`).

The merge gate never checks out or executes repository code. It runs from the
protected-base workflow definition, validates live PR base/head identity, reads
its canonical member inventory from the exact base SHA, and inspects exact-head
GitHub Actions run/job metadata only. A PR that edits the workflow cannot use
that edited definition to approve itself.
