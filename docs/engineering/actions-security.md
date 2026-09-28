# GitHub Actions static security gate

The repository's `actions-security` check is the fail-closed static audit for checked-in GitHub Actions workflows. It runs on every pull request and on pushes to `main` through `.github/workflows/dependency-review.yml`; it is intentionally not path-filtered.

The gate combines three independent controls:

- actionlint for workflow syntax and structural validation;
- zizmor in auditor mode at low severity, with only narrowly scoped checked-in suppressions;
- `.github/scripts/validate_actions_security.py` for repository-specific invariants that must remain deterministic even if scanner behavior changes.

The deterministic policy requires immutable full-SHA external `uses:` references, disabled checkout credential persistence unless explicitly justified, job-scoped write permissions, safe `pull_request_target` boundaries, no direct untrusted-context interpolation into program text, and digest-pinned service/container images where practical. Its negative fixtures live in `.github/scripts/test_validate_actions_security.py`.

`dependency-review` remains a stable required context on both pull requests and `main`: pull requests run the real dependency diff review, while `main` pushes emit the same context without pretending that a dependency diff exists. Unsupported trigger types fail closed.

Ruleset promotion is staged. `actions-security` must first be observed successfully on both workflow-changing and non-workflow-changing pull requests and on `main`; only then may the live `Protect main` ruleset require it and the corresponding `pending_ruleset_contexts` entry be removed.


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
