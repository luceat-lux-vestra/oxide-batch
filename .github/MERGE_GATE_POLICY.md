# Merge gate policy

`.github/merge-gate-policy.json` is the canonical classification of status-producing pull-request jobs for `oxide-batch`.

The policy exists so merge authority is not inferred from workflow names or from the current GitHub ruleset alone. The verifier reconciles three surfaces: the accepted policy, checked-in workflow/job producers, and the live `Protect main` ruleset.

## Classifications

- `required`: the job is part of merge authority and must emit a status on every applicable pull request. Required workflows may not use top-level path filters, and required jobs may not use conditions that can suppress their status.
- `advisory`: useful PR-time feedback that is intentionally outside merge authority.
- `optional`: explicitly path/scenario-scoped PR work that is allowed to disappear. Use this only when absence is intentional and documented by the policy.

A workflow-level default classifies every job in that workflow unless a `job_overrides` entry narrows one job. This keeps most deep M5/M6 campaign jobs advisory without duplicating every job id while allowing narrow existing merge-authoritative jobs such as the M5 PostgreSQL conformance campaign to remain explicitly required. Any new PR-triggered workflow or job that cannot obtain a classification fails closed.

## Required producer types

Most required contexts are produced by checked-in workflow jobs. `managed_required_contexts` covers GitHub-managed controls whose producer is not a repository workflow, currently CodeQL default setup's `Analyze (actions)` context.

Matrix jobs are expanded from their literal matrix axes and their checked-in `name`. A changed matrix therefore changes the required context set and must agree with the live ruleset.

## Staged ruleset migration

`pending_ruleset_contexts` is a temporary migration mechanism, not a weaker classification. A context listed there must still be produced by a policy-`required` job. It is excluded only from the live-ruleset equality check while the new producer is being proven on a pull request.

For #222, `evidence-provenance` is classified as required while its ruleset promotion is staged. The retained-evidence workflow remains separate from the Rust producer workflow intentionally: producer CI can succeed while retained evidence is temporarily stale, but a mergeable final PR HEAD must restore valid retained-evidence provenance.

The migration sequence is:

1. prove the required context is emitted successfully;
2. add it to the live `Protect main` ruleset;
3. read the live ruleset back;
4. remove it from `pending_ruleset_contexts`;
5. rerun the exact final PR HEAD.

A required context is never renamed or removed during this sequence.

## Enforcement

The repository's existing required `quality` job runs `cargo test --workspace --all-features`. `xtask/tests/merge_gate_policy.rs` uses that established merge gate to run:

- negative contract tests for the verifier; and
- a read-only live ruleset comparison on GitHub Actions.

Local `cargo test` does not perform the external GitHub API readback. This keeps ordinary local tests offline while preserving live drift enforcement in the required CI environment.

`release-crates` remains owned by the existing `quality` job and is not duplicated here.

## Related controls

- `dependency-review`: diff-scoped dependency risk gate.
- `supply-chain`: full dependency graph advisory/license/ban/source gate.
- `packaging` and `msrv`: packageability and minimum Rust support.
- PostgreSQL contexts: version/component/conformance correctness evidence.
- `evidence-provenance`: retained evidence integrity.
- `Analyze (actions)`: GitHub-managed CodeQL analysis.

#223 owns any future stable aggregate topology. This policy/verifier does not aggregate or weaken existing required contexts. #233 may compose this verifier later for scheduled hardening drift auditing.
