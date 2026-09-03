# Merge gate policy

`.github/merge-gate-policy.json` is the canonical classification of status-producing pull-request jobs for `oxide-batch`.

The policy exists so merge authority is not inferred from workflow names or from the current GitHub ruleset alone. The verifier reconciles the accepted policy, checked-in workflow/job producers, aggregate membership, and the live `Protect main` ruleset.

## Classifications

- `required`: the job is part of merge authority and must emit a status on every applicable pull request. Required workflows may not use top-level path filters, and required jobs may not use conditions that can suppress their status.
- `advisory`: useful PR-time feedback that is intentionally outside direct ruleset authority.
- `optional`: explicitly path/scenario-scoped PR work that is allowed to disappear. Use this only when absence is intentional and documented by the policy.

A workflow-level default classifies every job in that workflow unless a `job_overrides` entry narrows one job. This keeps most deep M5/M6 campaign jobs advisory without duplicating every job id while allowing narrow merge-authoritative jobs such as the M5 PostgreSQL conformance campaign to remain explicitly required. Any new PR-triggered workflow or job that cannot obtain a classification fails closed.

## Required producer types

Most required contexts are produced by checked-in workflow jobs. `managed_required_contexts` covers GitHub-managed controls whose producer is not a repository workflow, currently CodeQL default setup's `Analyze (actions)` context.

Matrix jobs are expanded from their literal matrix axes and their checked-in `name`. A changed matrix therefore changes the required context set and must agree with the canonical policy and live topology.

## Stable aggregate gates

#223 adopts two stable native GitHub Actions aggregate contexts for the eleven PostgreSQL-specific required contexts:

- `postgresql` aggregates the nine PostgreSQL jobs emitted by `.github/workflows/ci.yml`.
- `postgresql-conformance` aggregates the two PostgreSQL conformance jobs emitted by `.github/workflows/m5-conformance.yml`.

The split is intentional. GitHub Actions `needs` is workflow-local, so keeping each aggregate inside the workflow that owns its member jobs preserves native dependency semantics without cross-workflow polling, commit-status publishing, elevated `statuses: write` permission, or a long-lived polling runner.

The aggregate jobs are ordinary pull-request jobs. GitHub therefore owns their lifecycle, cancellation, rerun, and current check state for the PR HEAD. Each aggregate uses `if: ${{ always() }}` so it still executes after a failed/cancelled/skipped dependency, then fails unless every canonical dependency job result is exactly `success`.

Aggregate membership lives only in `merge-gate-policy.json`. The verifier maps each member context back to its checked-in required producer and requires all members of an aggregate to belong to the aggregate producer's workflow. It also requires the aggregate job's `needs` set to match those member job ids exactly, its context name to match policy exactly, and its script to match the canonical fail-closed dependency-result check. A removed/renamed member, matrix drift, dependency omission, altered success criterion, duplicate context, or producer reclassification therefore fails closed in required `quality` CI.

The two aggregates share migration group `postgresql`. Migration-group state must move together; mixed `candidate`/`cutover`/`active` states are rejected.

The design deliberately does **not** aggregate `dependency-review`, `supply-chain`, `msrv`, `packaging`, `quality`, `evidence-provenance`, or CodeQL because those controls have distinct dependency, security, compatibility, release, repository-quality, evidence-integrity, or static-analysis authority.

## Aggregate lifecycle and atomic cutover

`pending_ruleset_contexts` is a temporary migration mechanism, not a weaker classification. A pending aggregate must still be backed by its canonical checked-in producer.

Aggregate states have these meanings:

- `candidate`: the native aggregate jobs exist and run, but the live ruleset must still use the legacy child-context topology. The aggregate contexts remain in `pending_ruleset_contexts`.
- `cutover`: the aggregate contexts remain pending in policy while the live ruleset may be exactly either the legacy topology or the full replacement topology for the migration group. Partial/hybrid replacement is rejected.
- `active`: the live ruleset must use the aggregate replacement topology and the aggregate contexts must no longer be pending.

The PostgreSQL migration is:

1. **Bootstrap PR:** merge the two native aggregate jobs and `candidate` policy while all eleven PostgreSQL child contexts remain independently required.
2. Open a migration PR that moves both aggregates together to `cutover`, leaving both in `pending_ruleset_contexts`.
3. On the exact migration PR HEAD, verify both `postgresql` and `postgresql-conformance` are green while all eleven legacy child contexts are still directly required.
4. Perform **one** GitHub Settings save that simultaneously removes all eleven PostgreSQL child contexts and adds both aggregate contexts.
5. Fresh-read the live ruleset and require the exact final topology. A hybrid topology is not accepted by the verifier.
6. On the same migration PR, move both aggregates to `active`, remove both from `pending_ruleset_contexts`, and rerun strict review/CI on that new exact HEAD.
7. Squash-merge only after all final required contexts are green and the live ruleset/policy topology matches exactly.

There is no policy-approved intermediate topology containing only one aggregate replacement. The single Settings save changes the migration group atomically from the legacy eleven-context surface to the two-context aggregate surface.

## Enforcement

The repository's required `quality` job runs `cargo test --workspace --all-features`. `xtask/tests/merge_gate_policy.rs` uses that established merge gate to run:

- negative contract tests for canonical producer/ruleset drift;
- aggregate membership, producer-name, `needs`, `always()`, canonical fail-closed script, collision, and classification tests;
- atomic migration-group tests covering legacy, final, and rejected partial/hybrid topologies; and
- a read-only live ruleset comparison on GitHub Actions.

Local `cargo test` does not perform the external GitHub API readback. This keeps ordinary local tests offline while preserving live drift enforcement in required CI.

`release-crates` remains owned by the existing `quality` job and is not duplicated here.

## Accepted stable topology

After #223 migration completes, the expected required contexts are exactly:

- `Analyze (actions)`
- `dependency-review`
- `msrv`
- `packaging`
- `quality`
- `supply-chain`
- `evidence-provenance`
- `postgresql`
- `postgresql-conformance`

The eleven PostgreSQL child jobs continue to run as aggregate members; only their direct ruleset surface is replaced by the two native aggregate contexts.

#233 may compose this verifier later for scheduled hardening drift auditing.
