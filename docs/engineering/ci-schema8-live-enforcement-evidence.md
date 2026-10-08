# Schema-v8 trusted-base merge-gate live enforcement evidence

**Evidence date:** 2026-10-09 (KST)
**Tracker:** [oxide-batch #433](https://github.com/luceat-lux-vestra/oxide-batch/issues/433)
**Cross-project tracker:** [R2A #95](https://github.com/luceat-lux-vestra/Research-to-Action/issues/95)

## Security objective and trusted anchor

The repository's active Ruleset `19905142` requires the GitHub Actions
`merge-gate` context from integration `15368`. The protected
`.github/workflows/pr-labeler.yml` runs the required gate from a
trusted base, rather than accepting self-reported, candidate-head
workflow checks. Policy schema **8** on `main`, introduced by
[PR #442](https://github.com/luceat-lux-vestra/oxide-batch/pull/442),
adds exactly six non-workflow control files to the staged Git-blob
admission model, while retaining the existing protected-workflow list.

The bootstrap's landed trusted main was
`e92f9f3cab47e22ae66c23f4bdd644aa4e4d3799`, with policy
blob `09fce80260b957112a872354ef74ec68e223c904`.
Its protected-file inventory is independent of PR-head assertions.
Git object identity is a *change-control mechanism*, not a substitute
for code review, behavioral tests, provenance, or security analysis.

## Live adversarial probe — rejected as designed

[PR #443](https://github.com/luceat-lux-vestra/oxide-batch/pull/443)
was explicitly marked **TEST ONLY / DO NOT MERGE**. Its exact HEAD
`fe27f778a6d8a4a62a4252078774d49198e3b895`
changed only `.github/scripts/validate_supply_chain_exceptions.py`:
one inert comment was appended, without pre-admitting the resulting
Git blob through the policy.

| Evidence | Observed result |
| --- | --- |
| Trusted base | `e92f9f3cab47e22ae66c23f4bdd644aa4e4d3799` (schema 8) |
| Proposed protected-file blob | `cd0f3f0665e179f34ce3982bd1827c32217fc236` |
| Required check | `merge-gate`, GitHub Actions integration `15368` |
| Run and job | [#37845003214](https://github.com/luceat-lux-vestra/oxide-batch/actions/runs/37845003214), job `113543562870` |
| First-attempt result | **FAILURE** at `Evaluate base-trusted repository merge authority` |
| Log reason | `changed protected file blob ... was not pre-admitted by trusted-base policy` |
| Resolution | PR closed **without merge**; no retry used to mask the failure |

This **negative test passed** because the authoritative required check
rejected an unstaged protected-file change. It does **not** establish
positive compatibility or independent approval of future protected
control-file modifications.

## Positive control and boundaries

The first subsequent ordinary, non-protected-file documentation PR
should be assessed against **the already-active schema-8 trusted base**
and must receive an independent successful required `merge-gate`
before squash merge. The exact candidate HEAD, job/run identities,
first-attempt outcomes and post-main Git tree equality are tracked
in [#433](https://github.com/luceat-lux-vestra/oxide-batch/issues/433);
this record alone is **not** positive merge authorization.

Until further verified migrations, the following remain unchanged:

- Eight protected workflow blob inventories and six schema-8 protected
  control-file identities retain staged change admission.
- `pr-proof`, PostgreSQL 15/18 conformance, exact-final-HEAD binding,
  source workflow identity and fail-closed absence/error handling remain
  mandatory where applicable.
- Non-required Shadow CI results are advisory only and cannot be
  promoted into independent merge authority.
- No workflow SHA pre-admission replacement, reduction in required
  checks, performance improvement or representative p95 claim follows
  from this probe.
- The ARM rollout remains parked under
  [R2A #94](https://github.com/luceat-lux-vestra/Research-to-Action/issues/94).

## Repeatability and acceptance criteria

For a repeat exercise, pin the **exact current trusted main SHA**,
verify the required Ruleset identity, and propose an intentionally
*unadmitted* non-executable change to one of the six protected
control files on a disposable **do-not-merge** PR. Require the
base-trusted `merge-gate` to fail specifically on the changed blob;
capture its **first-attempt** log and exact HEAD, then close the
probe without merging. An unexpected PASS is a security incident and
must block any simplification. A separate clean PR must prove the
positive path, with no bypass, impersonated contexts or rerun masking.
