# Merge gate policy

`.github/merge-gate-policy.json` is the canonical classification of status-producing pull-request jobs for `oxide-batch`.

The policy exists so merge authority is not inferred from workflow names or from the current GitHub ruleset alone. The verifier reconciles the accepted policy, checked-in workflow/job producers, aggregate membership, and the live `Protect main` ruleset.

## Classifications

- `required`: the job is part of merge authority and must emit a status on every applicable pull request. Required workflows must use the unprivileged `pull_request` event, may not use `pull_request_target`, and may not use top-level path filters; required jobs may not use conditions that can suppress their status.
- `advisory`: useful PR-time feedback that is intentionally outside direct ruleset authority.
- `optional`: explicitly path/scenario-scoped PR work that is allowed to disappear. Use this only when absence is intentional and documented by the policy.

A workflow-level default classifies every job in that workflow unless a `job_overrides` entry narrows one job. This keeps most deep M5/M6 campaign jobs advisory without duplicating every job id while allowing narrow merge-authoritative jobs such as the M5 PostgreSQL conformance campaign to remain explicitly required. Any new PR-triggered workflow or job that cannot obtain a classification fails closed.

## Required producer types

Required contexts are produced by checked-in pull-request workflows. `managed_required_contexts` is now empty; GitHub-managed default CodeQL setup is not part of merge authority.

`.github/workflows/codeql.yml` is the single CodeQL authority. It emits two explicit jobs rather than a matrix so policy can represent their different authority precisely: `Analyze (actions)` is required and `Analyze (rust)` is advisory. Both run on pull requests and the weekly schedule, but the workflow has no `push` trigger. This preserves the required context name while eliminating post-merge duplicate analysis.

The repository settings policy requires CodeQL default setup to remain disabled while this checked-in advanced setup is active. Re-enabling default setup would create competing configurations and is policy drift.

Matrix jobs are expanded from their literal matrix axes and their checked-in `name`. A changed matrix therefore changes the required context set and must agree with the canonical policy and live topology.

## Post-main validation invariant

`main` is not a second validation stage. A squash merge may not trigger the
same build/test/security/dependency analysis already proven on the exact final
pull-request HEAD.

`merge-gate-policy.json.post_main` names the default branch and the explicit
allowlist for workflows that may target ordinary pushes to that branch. The
current allowlist is empty. The verifier scans every checked-in workflow and
fails closed if a non-allowlisted workflow can run on `push: main`.

This does not prohibit tag-only release workflows, feature-branch push Fast CI,
weekly schedules, `workflow_dispatch`, or issue/PR lifecycle automation. A
future deployment that genuinely requires a main-push event must be added to
the allowlist by an explicit policy change rather than silently reintroducing
post-merge validation.

## Trusted PR scope and campaign applicability

The next CI topology uses one fail-closed scope model rather than separate path lists for documentation and each M5/M6 campaign.

`.github/scripts/pr-scope.py` must run with its repository root checked out at the pull request's **exact trusted base SHA**. The classifier verifies `git rev-parse HEAD` against the supplied 40-character base SHA before classifying. The classifier, this policy, every discovered semantic-closure document, and the retained-evidence producer inventory are therefore read from one immutable base tree. A pull request may change any of those files, but PR-head changes do not get to decide whether their own validation is applicable. If the trusted tree cannot be established or changed-file metadata cannot be reconciled exactly, callers must fall back to full validation and all campaigns.

Documentation-only scope is intentionally narrow: only the explicitly listed root documentation files and Markdown under `docs/**` qualify. Rename/copy provenance is evaluated on both source and destination.

Campaign applicability does **not** duplicate M5/M6 path lists here. The classifier discovers `tests/fixtures/**/campaign-semantics.json`, validates every closure, derives its dedicated workflow from that closure, and requires the resulting workflow inventory to match `docs/engineering/retained-evidence-policy.json`'s artifact producers exactly. A changed path intersects a campaign when it equals a declared semantic path or is below a declared semantic directory.

`Cargo.lock` is intentionally conservative in this first model: any lockfile change marks every retained-evidence campaign applicable. The retained-evidence system already narrows dependency identity through campaign-scoped `dependency-closure.json` files; a future optimization may compare those derived closures before campaign execution, but absence of that proof is not treated as non-impact.

Retained evidence now has two explicit authorities. Required `evidence-provenance` continuously verifies retained bytes, producer/provenance identity, canonical verdict, matrix/inventory, execution-manifest structure, and the determinism of the current campaign dependency-closure metadata. `cargo xtask evidence-freshness` is the separate fail-closed current-HEAD authority that compares a retained report's recorded semantic objects with the checkout that would run now. Semantic-impact/deep/release routing may require that freshness proof; an ordinary source PR does not turn a trustworthy historical artifact into forged evidence merely because a bound source object moved. Missing or malformed provenance, manifests, closure metadata, or applicability evidence remains a failure rather than an implicit non-impact decision.

The trusted-classifier foundation is already on `main`. The current migration stage separates retained-evidence integrity from current-HEAD freshness without suppressing campaign execution or altering the live required-context topology. Follow-on migration PRs may consume the base classifier and freshness authority to consolidate advisory campaigns, make M5 Conformance semantic-impact-driven, perform one retained-evidence refresh after workflow/contract identities stabilize, and build the final native Merge Gate.

## PostgreSQL aggregate decision

#223 originally evaluated all eleven then-current `postgres-*` required contexts rather than assuming that every PostgreSQL-looking check should be hidden behind one cosmetic status. #323 later retired the completed M0 `postgres-spike` experiment from merge-time CI after its production invariants had moved to the repository/crash-recovery suites.

The accepted current boundary is one native GitHub Actions aggregate context, `postgresql`, over the eight production PostgreSQL jobs emitted by `.github/workflows/ci.yml`:

- four PostgreSQL design-gate matrix contexts;
- two item-component matrix contexts; and
- two repository matrix contexts.

The two M5 conformance contexts remain independently required:

- `postgres-15-conformance-campaign`;
- `postgres-18-conformance-campaign`.

That is an intentional **decline** to aggregate the conformance campaign, not omitted evaluation. GitHub Actions `needs` is workflow-local, so a native conformance aggregate would have to modify `.github/workflows/m5-conformance.yml`. That workflow's exact Git object identity is part of the retained M5 conformance evidence provenance contract. Changing it solely to reduce the ruleset surface invalidates the currently retained campaign evidence and requires a new campaign/evidence promotion even though the conformance obligation itself did not change. The conformance checks therefore retain useful independent evidence authority and stay outside this aggregate.

Cross-workflow polling or custom commit-status publication was also evaluated and declined. It adds lifecycle/rerun races and elevated status-publishing machinery that a workflow-local native dependency graph does not need.

The design also deliberately leaves `dependency-review`, `supply-chain`, `msrv`, `packaging`, `quality`, `evidence-provenance`, and required CodeQL Actions analysis independently required because those controls have distinct dependency, security, compatibility, release, repository-quality, evidence-integrity, or static-analysis authority. Advisory Rust CodeQL adds another static-security signal without replacing or weakening any of those controls.

## Native aggregate contract

`postgresql` is an ordinary pull-request job in `.github/workflows/ci.yml`. GitHub therefore owns its lifecycle, cancellation, rerun, and current check state for the PR HEAD.

The job uses `if: ${{ always() }}` so it still executes after a failed/cancelled/skipped dependency.

### Why raw `needs.<job>.result` is not selective-rerun-safe

The aggregate's four `needs` job ids each back a matrix (four PostgreSQL versions for the design gate, two each for item-components and repository). GitHub Actions' `needs.<job-id>.result` collapses an entire matrix job into a single result for the *current* workflow attempt. When a PR author uses "re-run failed jobs" to rerun only one failed matrix child (say, `postgres-15-design-gate`), GitHub bumps the run's `run_attempt` and re-executes only that child and its dependents (including the aggregate); a sibling matrix child that was never rerun (say, `postgres-18-design-gate`) keeps its result from the earlier, lower `run_attempt`. A workflow-level `needs.postgres-design-gate.result` check re-evaluated on the new attempt cannot see per-matrix-child history closely enough to distinguish "every canonical context's latest execution succeeded" from "the matrix job merely ran again" — it can read back a success even though an unrepaired sibling failure from an earlier attempt is still the last word for that context. A raw `needs.*.result == success` check is therefore not sufficient on its own to prove every one of the eight canonical PostgreSQL contexts is actually green.

### How the aggregate proves it instead

The final authority is `.github/scripts/evaluate-aggregate-run.rb`, invoked as the aggregate producer's only substantive step. It:

1. Reads the eight canonical member context names exclusively from `merge-gate-policy.json`'s `postgresql` aggregate entry — there is no second, manually duplicated list of the nine names anywhere in the workflow or scripts.
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

## Failure handling before remediation

As of #311, the repository-wide `Failure classification` reporter and mandatory
`failure-triage` PR declaration/check are retired. Failure investigation remains
a fail-closed engineering discipline governed by `AGENTS.md`: establish enough
root-cause evidence to justify the owning layer, never weaken valid evidence to
obtain green CI, and invalidate affected exact-HEAD evidence when remediation
changes one of its premises.

No PR-body schema or dedicated failure-triage status is part of merge authority.

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

The current expected required contexts are exactly:

- `Analyze (actions)`
- `actions-security`
- `dependency-review`
- `msrv`
- `packaging`
- `postgres-15-conformance-campaign`
- `postgres-18-conformance-campaign`
- `quality`
- `supply-chain`
- `evidence-provenance`
- `postgresql`

`Analyze (rust)` is intentionally not in this required list while #248 treats it
as advisory. Its absence from this list is not evidence that Rust is unsupported
or unscanned.

The eight production Rust PostgreSQL child jobs continue to run as aggregate members; only their direct ruleset surface is replaced. The historical M0 PostgreSQL spike remains reproducible source evidence but is no longer a merge-time aggregate member. The two conformance contexts continue to run and remain directly required as independent evidence authority.

#233 may compose this verifier for scheduled hardening drift auditing. Advisory Rust CodeQL remains outside direct merge authority until a separate explicit policy migration promotes it.

## Advisory campaign orchestration

Advisory retained-evidence campaigns are PR-triggered only through
`.github/workflows/campaign-orchestrator.yml`. Their individual producer
workflows remain manually dispatchable and reusable via `workflow_call`, but
must not independently subscribe to `pull_request`.

The orchestrator inventory is not a hand-maintained campaign list. The
merge-gate verifier derives the retained artifact producers from
`docs/engineering/retained-evidence-policy.json`, excludes producers that own
required jobs (currently M5 Conformance), and requires the orchestrator's local
reusable-workflow calls to match the remaining advisory producer set exactly.
Missing, duplicate, unknown, directly PR-triggered, or non-reusable advisory
producers fail closed.
