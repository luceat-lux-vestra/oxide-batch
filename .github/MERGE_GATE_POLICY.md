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

## PostgreSQL aggregate decision

#223 evaluated all eleven current `postgres-*` required contexts rather than assuming that every PostgreSQL-looking check should be hidden behind one cosmetic status.

The accepted boundary is one native GitHub Actions aggregate context, `postgresql`, over the nine PostgreSQL jobs emitted by `.github/workflows/ci.yml`:

- four PostgreSQL design-gate matrix contexts;
- two item-component matrix contexts;
- two repository matrix contexts; and
- `postgres-spike`.

The two M5 conformance contexts remain independently required:

- `postgres-15-conformance-campaign`;
- `postgres-18-conformance-campaign`.

That is an intentional **decline** to aggregate the conformance campaign, not omitted evaluation. GitHub Actions `needs` is workflow-local, so a native conformance aggregate would have to modify `.github/workflows/m5-conformance.yml`. That workflow's exact Git object identity is part of the retained M5 conformance evidence provenance contract. Changing it solely to reduce the ruleset surface invalidates the currently retained campaign evidence and requires a new campaign/evidence promotion even though the conformance obligation itself did not change. The conformance checks therefore retain useful independent evidence authority and stay outside this aggregate.

Cross-workflow polling or custom commit-status publication was also evaluated and declined. It adds lifecycle/rerun races and elevated status-publishing machinery that a workflow-local native dependency graph does not need.

The design also deliberately leaves `dependency-review`, `supply-chain`, `msrv`, `packaging`, `quality`, `evidence-provenance`, and CodeQL independently required because those controls have distinct dependency, security, compatibility, release, repository-quality, evidence-integrity, or static-analysis authority.

## Native aggregate contract

`postgresql` is an ordinary pull-request job in `.github/workflows/ci.yml`. GitHub therefore owns its lifecycle, cancellation, rerun, and current check state for the PR HEAD.

The job uses `if: ${{ always() }}` so it still executes after a failed/cancelled/skipped dependency.

### Why raw `needs.<job>.result` is not selective-rerun-safe

The aggregate's four `needs` job ids each back a matrix (four PostgreSQL versions for the design gate, two each for item-components and repository). GitHub Actions' `needs.<job-id>.result` collapses an entire matrix job into a single result for the *current* workflow attempt. When a PR author uses "re-run failed jobs" to rerun only one failed matrix child (say, `postgres-15-design-gate`), GitHub bumps the run's `run_attempt` and re-executes only that child and its dependents (including the aggregate); a sibling matrix child that was never rerun (say, `postgres-18-design-gate`) keeps its result from the earlier, lower `run_attempt`. A workflow-level `needs.postgres-design-gate.result` check re-evaluated on the new attempt cannot see per-matrix-child history closely enough to distinguish "every canonical context's latest execution succeeded" from "the matrix job merely ran again" — it can read back a success even though an unrepaired sibling failure from an earlier attempt is still the last word for that context. A raw `needs.*.result == success` check is therefore not sufficient on its own to prove every one of the nine canonical PostgreSQL contexts is actually green.

### How the aggregate proves it instead

The final authority is `.github/scripts/evaluate-aggregate-run.rb`, invoked as the aggregate producer's only substantive step. It:

1. Reads the nine canonical member context names exclusively from `merge-gate-policy.json`'s `postgresql` aggregate entry — there is no second, manually duplicated list of the nine names anywhere in the workflow or scripts.
2. Calls the GitHub Actions Jobs API (`GET /repos/{owner}/{repo}/actions/runs/{run_id}/jobs?filter=all&per_page=100`, paginated to exhaustion) to read every job execution recorded for the current run, across **every** workflow attempt — `filter=all`, not `filter=latest`, because a `latest`-only read would miss exactly the un-rerun sibling's earlier execution.
3. For each canonical member context independently, matches Jobs API entries by exact job `name`, finds that member's own maximum `run_attempt`, and requires that one specific execution to be `status == "completed"` and `conclusion == "success"`.

Because the latest attempt is selected **per canonical member**, not globally by workflow attempt, a member that was never rerun keeps its own last execution as authoritative even while a sibling member has since moved to a higher attempt number. This is exactly what preserves an unrepaired sibling's failure: repairing `postgres-15-design-gate` in attempt 2 cannot launder a `postgres-18-design-gate` failure that is still sitting, unrepaired, at attempt 1 — the evaluator still reads that member's latest (and only) execution and fails closed on it. Selectively rerunning and repairing every failed member independently still passes, because each member's own latest execution is what is checked.

The evaluator fails closed on any HTTP, API, JSON, or schema error (non-2xx response, unparseable body, a job entry missing an integer `run_attempt`, and so on), on a missing canonical member, on an ambiguous/duplicate latest-attempt entry, and on any conclusion other than exactly `success` (`failure`, `cancelled`, `skipped`, `neutral`, `timed_out`, `action_required`, `stale`, `startup_failure`, or a non-`completed` status such as `queued`/`in_progress`). It never interprets absence or a non-success result optimistically.

### Least-privilege access and boundaries

The aggregate producer job declares only the job-level permissions the evaluator needs to call the Jobs API and check out the evaluator script itself:

```yaml
permissions:
  actions: read
  contents: read
```

No write permission is granted. The job authenticates to the Jobs API with `GITHUB_TOKEN: ${{ github.token }}` passed as an explicit step environment variable; it never publishes a custom commit status, never polls or waits on other workflows, and never uses `pull_request_target`. The aggregate's own pass/fail is still communicated exclusively through GitHub's native check-run status for the `postgresql` job, the same as before.

Aggregate membership lives only in `merge-gate-policy.json`. The verifier maps every aggregate member context back to its checked-in required producer and requires all members to belong to the aggregate producer's workflow. It also requires:

- the aggregate workflow to remain PR-triggered without path suppression;
- the aggregate job's `needs` set to match the member-producing job ids exactly;
- the emitted context name to match policy exactly;
- no matrix or `continue-on-error` on the aggregate producer;
- `if: ${{ always() }}`;
- the bounded runner/timeout shape;
- exact least-privilege `permissions: {actions: read, contents: read}`;
- a checkout step that reuses the same `actions/checkout` SHA already pinned elsewhere in the workflow (no second, independently-drifting pin); and
- the canonical evaluator invocation (`ruby .github/scripts/evaluate-aggregate-run.rb <context>`) with the `GITHUB_TOKEN` environment wired.

A removed/renamed member, matrix drift, dependency omission, weakened permissions, an unpinned or diverging checkout SHA, an altered/missing evaluator invocation, duplicate context, producer suppression, or producer reclassification therefore fails closed in required `quality` CI.

The two M5 PostgreSQL conformance contexts (`postgres-15-conformance-campaign`, `postgres-18-conformance-campaign`) are produced by a different workflow (`.github/workflows/m5-conformance.yml`) and are not members of this aggregate; they remain independently required, unchanged by this evaluator.

## Aggregate lifecycle and atomic cutover

`pending_ruleset_contexts` is a temporary migration mechanism, not a weaker classification. A pending aggregate must still be backed by its canonical checked-in producer.

Aggregate states have these meanings:

- `candidate`: the native aggregate job exists and runs, but the live ruleset must still use the legacy child-context topology. The aggregate context remains in `pending_ruleset_contexts`.
- `cutover`: the aggregate context remains pending in policy while the live ruleset may be exactly either the legacy topology or the full replacement topology. Partial/hybrid replacement is rejected.
- `active`: the live ruleset must use the aggregate replacement topology and the aggregate context must no longer be pending.

The PostgreSQL migration is:

1. **Bootstrap PR:** merge the native `postgresql` aggregate and `candidate` policy while all nine Rust PostgreSQL child contexts remain independently required. The two M5 conformance contexts remain required throughout and are not migration members.
2. Open a migration PR that moves `postgresql` to `cutover`, leaving it in `pending_ruleset_contexts`.
3. On the exact migration PR HEAD, verify `postgresql` is green while all nine legacy Rust PostgreSQL child contexts are still directly required.
4. Perform **one** GitHub Settings save that simultaneously removes the nine Rust PostgreSQL child contexts and adds `postgresql`. Do not change the two conformance required contexts.
5. Fresh-read the live ruleset and require the exact accepted topology. A hybrid topology is not accepted by the verifier.
6. On the same migration PR, move `postgresql` to `active`, remove it from `pending_ruleset_contexts`, and rerun strict review/CI on that new exact HEAD.
7. Squash-merge only after all final required contexts are green and the live ruleset/policy topology matches exactly.

## Enforcement

The repository's required `quality` job runs `cargo test --workspace --all-features`. `xtask/tests/merge_gate_policy.rs` uses that established merge gate to run:

- negative contract tests for canonical producer/ruleset drift;
- aggregate membership, producer-name, `needs`, `always()`, path-suppression, least-privilege permissions, pinned-checkout reuse, canonical evaluator invocation, collision, and classification tests;
- deterministic selective-rerun-safety tests for the aggregate evaluator's pure per-member latest-attempt reconciliation, covering repaired and unrepaired matrix reruns;
- cutover topology tests covering legacy, final, and rejected partial/hybrid states; and
- a read-only live ruleset comparison on GitHub Actions.

Local `cargo test` does not perform the external GitHub API readback. This keeps ordinary local tests offline while preserving live drift enforcement in required CI.

`release-crates` remains owned by the existing `quality` job and is not duplicated here.

## Accepted stable topology

After #223 migration completes, the expected required contexts are exactly:

- `Analyze (actions)`
- `dependency-review`
- `msrv`
- `packaging`
- `postgres-15-conformance-campaign`
- `postgres-18-conformance-campaign`
- `quality`
- `supply-chain`
- `evidence-provenance`
- `postgresql`

The nine Rust PostgreSQL child jobs continue to run as aggregate members; only their direct ruleset surface is replaced. The two conformance contexts continue to run and remain directly required as independent evidence authority.

#233 may compose this verifier later for scheduled hardening drift auditing.
