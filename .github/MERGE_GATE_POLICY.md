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

#223 adopts one stable aggregate context, `postgresql`, for the eleven PostgreSQL implementation/version-specific required contexts. It deliberately does **not** aggregate `dependency-review`, `supply-chain`, `msrv`, `packaging`, `quality`, `evidence-provenance`, or CodeQL because those controls have distinct dependency, security, compatibility, release, repository-quality, evidence-integrity, or static-analysis authority.

Aggregate membership lives only in `merge-gate-policy.json`. The trusted evaluator does not maintain a second child list. The verifier requires every aggregate member to resolve to an actual policy-`required` producer, so a renamed/removed child or matrix drift fails closed before the topology can be accepted.

The `postgresql` status is published by `.github/workflows/postgres-merge-gate.yml`. That workflow uses `pull_request_target` and `workflow_run`, checks out the implementation from trusted `main`, has only `contents: read` plus `statuses: write`, and evaluates check runs for the exact PR head SHA. It accepts only GitHub Actions child checks. A child that fails, is cancelled, is skipped, completes without a success conclusion, or disappears after its source workflow completes makes the aggregate fail. In-progress or not-yet-produced children keep it pending.

This trusted-main design is intentional: an untrusted pull request must not be able to edit its own aggregate evaluator and manufacture a false-green merge status.

## Staged ruleset migration

`pending_ruleset_contexts` is a temporary migration mechanism, not a weaker classification. A pending context must still be a canonical producer/aggregate. Candidate aggregate gates must remain pending until their trusted producer has been deployed and proven.

The PostgreSQL migration is therefore two-phase:

1. **Bootstrap PR:** merge the trusted evaluator and candidate policy while all eleven existing PostgreSQL child contexts remain independently required.
2. On a subsequent PR, observe `postgresql` against the exact PR HEAD and verify it agrees with all eleven child checks.
3. Add `postgresql` to the live `Protect main` ruleset while the eleven children are still required.
4. Read the live ruleset back and verify the aggregate is required and green.
5. Remove the eleven child contexts from the live ruleset in one Settings edit, leaving `postgresql` required.
6. Read the live ruleset back again.
7. Change the policy aggregate state from `candidate` to `active`, remove `postgresql` from `pending_ruleset_contexts`, and rerun the exact final migration PR HEAD.

There is no interval in which a required status is impossible to produce. The bootstrap PR cannot itself prove the trusted aggregate because `pull_request_target` and `workflow_run` execute workflow definitions from the base/default branch; proof begins only after the evaluator exists on `main`.

## Enforcement

The repository's required `quality` job runs `cargo test --workspace --all-features`. `xtask/tests/merge_gate_policy.rs` uses that established merge gate to run:

- negative contract tests for canonical producer/ruleset drift;
- aggregate policy migration/inventory negative tests;
- aggregate runtime semantics tests for failure, cancellation, skip, missing child, rerun selection, and non-GitHub-Actions spoofing; and
- a read-only live ruleset comparison on GitHub Actions.

Local `cargo test` does not perform the external GitHub API readback. This keeps ordinary local tests offline while preserving live drift enforcement in required CI.

`release-crates` remains owned by the existing `quality` job and is not duplicated here.

## Accepted stable topology

After #223 migration completes, the expected required contexts are:

- `Analyze (actions)`
- `dependency-review`
- `msrv`
- `packaging`
- `quality`
- `supply-chain`
- `evidence-provenance`
- `postgresql`

The eleven PostgreSQL child checks continue to run and remain canonical aggregate members; only their direct ruleset surface is replaced by `postgresql`.

#233 may compose this verifier later for scheduled hardening drift auditing.
