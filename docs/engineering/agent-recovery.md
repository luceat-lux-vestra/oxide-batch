# Crash-Recoverable Agent Work

This document defines repository-operation recovery after an agent session, connection, harness, or process interruption in `luceat-lux-vestra/oxide-batch`.

It does not replace this repository's architecture, review, CI, merge, release, security, research, or promotion policy. Existing repository-specific rules remain authoritative.

## Goal

A fresh agent session must be able to determine the exact safe continuation point from durable repository/GitHub state without depending on prior chat memory or a hand-written handoff.

```text
durable policy
  + compact checkpoint
  + authoritative GitHub state
  + reconciliation
  = safe resume
```

## Authority hierarchy

For execution recovery, use this order:

1. current GitHub execution state;
2. current repository policy/documentation;
3. the active Issue checkpoint;
4. conversation/session memory.

GitHub owns observable execution truth for Issues, refs, commit SHAs, pull requests, checks/workflows, reviews/threads, merge state, and the default branch. The checkpoint is a resume hint and never overrides current GitHub state.

## Active-work discovery

Before starting new non-trivial work:

1. fresh-read open production PRs;
2. recover and finish an existing production PR before creating another when repository policy requires one-at-a-time work;
3. identify the owning Issue and its checkpoint;
4. only then derive the next safe action.

A checkpoint never authorizes parallel work that repository policy forbids.

## Durable checkpoint

For recoverable work, keep exactly one mutable checkpoint comment on the owning Issue with this marker:

```text
<!-- agent-resume-checkpoint:v1 -->
```

Do not store the mutable checkpoint in the production branch. Updating such a file would move HEAD and can invalidate exact-HEAD evidence.

Use this compact shape:

```yaml
schema: agent-resume/v1
repository: luceat-lux-vestra/oxide-batch
work_item: <issue-number>
work_unit: <stable-logical-id>
phase: selected | implementation-durable | pr-open | validating | merge-ready | merged | housekeeping | done
branch: <branch-or-null>
pr: <pr-number-or-null>
expected:
  branch_head: <sha-or-null>
  pr_head: <sha-or-null>
  base: main
last_verified:
  at: <timestamp>
  authority: github
next_safe_action: <short-semantic-action>
last_mutation:
  kind: <operation-or-none>
  outcome: verified | unknown | none
stop_condition: <repository-policy stop condition>
```

The checkpoint records durable identity and phase, not volatile CI state. Do not store chat/model IDs, hidden reasoning, secrets, or private transcript content.

## Normal checkpoint boundaries

Update the checkpoint only after a durable postcondition is verified:

1. work item selected;
2. implementation reaches a durable candidate commit/ref;
3. PR existence is verified;
4. final candidate HEAD is established;
5. merge result is verified;
6. post-merge housekeeping is verified;
7. work unit is done.

Do not checkpoint every tool call.

In normal work, do **not** intentionally leave a checkpoint stale. Deliberately stale checkpoints were a fault-injection technique during validation. A stale checkpoint should now arise only from a real interruption, lost response, concurrent state change, or failed checkpoint write.

## Session-start reconciliation

Before mutating interrupted work:

```text
1. read AGENTS.md and this document
2. fresh-read open production PRs
3. identify the active Issue and checkpoint
4. fresh-read Issue / branch / PR / HEAD / base
5. read checks, reviews and threads when they affect the next decision
6. compare checkpoint expectation with current GitHub state
7. classify the relationship
8. derive exactly one safe next action
9. mutate only after required preconditions are proven
```

Use one classification:

- `MATCH` — checkpoint identity and GitHub agree.
- `CHECKPOINT_STALE_BUT_RECONCILABLE` — GitHub is ahead, and the newer state is uniquely attributable to the same work unit.
- `EXTERNAL_STATE_MISSING_RETRYABLE` — expected external state is proven absent and retry remains duplicate-safe.
- `CONFLICT` — current state exists but cannot be safely attributed to the work unit.
- `UNKNOWN` — required authority cannot be read or evidence is insufficient.

`UNKNOWN / UNVERIFIED / INSUFFICIENT EVIDENCE = FAIL` when the unresolved fact is material to the next mutation or merge decision.

## Mutation protocol

Every material external mutation follows:

```text
fresh precondition read
  -> bounded mutation
  -> authoritative read-back
  -> classify postcondition
  -> checkpoint only after verified postcondition
```

If the response/session is lost after a mutation, never repeat the operation solely because the checkpoint did not advance. Re-read GitHub first.

For create operations, use stable identities and search/read before retrying. For ref movement, re-read the branch and record the verified SHA. For merge, pin the expected reviewed HEAD when supported and re-read PR/default-branch state before retrying an unknown outcome.

## Exact-HEAD evidence

Current PR HEAD is authoritative.

Any HEAD movement invalidates prior HEAD-specific CI/review/runtime evidence unless existing repository policy explicitly permits reuse for the same exact SHA and producer. If branch-history cleanup or another rewrite changes HEAD, rerun the affected proof on the new exact final HEAD.

Green CI is necessary where required but is not by itself a correctness proof.

## Base advancement with unchanged HEAD

This section clarifies evidence provenance when the PR HEAD is unchanged but `main` advances. It does not weaken oxide-batch's exact-HEAD rule, live merge gate, or current-composition requirements.

Classify movement before deciding what prior evidence still proves:

1. **PR HEAD moved** — prior HEAD-specific CI/review/runtime evidence is invalid. Validate the new exact final HEAD.
2. **PR HEAD unchanged; base unchanged** — apply the ordinary exact-HEAD merge gate.
3. **PR HEAD unchanged; base advanced** — prior evidence may still support claims that depend only on the unchanged HEAD, but it does not by itself prove the newly composed base+HEAD result or authorize merge.
4. **Material authority or semantic interaction is unavailable or `UNKNOWN`** — fail closed and refresh/revalidate.

Before relying on same-HEAD evidence after base advancement, fresh-read the current PR HEAD, `main`, merge-base, live ruleset, required checks, review threads, and mergeability. Determine whether the evidence was produced for the unchanged HEAD itself or for an old synthetic merge/base+HEAD composition.

For oxide-batch specifically, the live `Protect main` ruleset requires the GitHub Actions `Direct Merge Gate` with strict required-status-check enforcement. `AGENTS.md` also requires exact-HEAD proof, fail-closed material unknowns, required CI, and repository-state revalidation before completion. Those stricter requirements remain authoritative. Same-HEAD evidence reuse therefore never authorizes an out-of-date or otherwise invalid current composition.

Evidence reuse is claim-scoped, not blanket merge authorization:

- exact-HEAD evidence may remain useful when its producer and claim are independent of the base movement;
- evidence tied to an old synthetic merge result or old base+HEAD tree is not proof for the newly composed tree;
- different filenames alone do not establish semantic independence;
- dependency/lockfile, feature-graph, durable schema/codec/migration, restart/recovery, workflow, generated-artifact, benchmark/evidence-producer, runtime/operational-contract, or other cross-cutting interactions require revalidation when material;
- conflicts, changed required checks, policy/ruleset drift, or unresolved material interaction require fresh validation rather than reuse.

If bringing the branch up to date, rebasing, conflict resolution, history cleanup, regeneration, or any other action changes the PR HEAD, the resulting SHA becomes a new exact-HEAD proof unit and all prior HEAD-specific PASS evidence is invalid.

Do not change CI workflows, `.github/aggregate-policy.json`, `.github/repository-settings-policy.json`, release policy, or live rulesets merely to make prior evidence reusable.

## Failure handling

- Read-only interruption: fresh-read and continue.
- Local-only/unpushed edits: not recoverable from GitHub; bound loss with logical durable commits where appropriate.
- Lost response after remote mutation: infer outcome from authority, not from the stale checkpoint.
- Stale checkpoint / HEAD movement: current branch/PR HEAD wins; establish ownership, then reconcile or fail closed.
- Concurrent actor: do not use last-write-wins reasoning.
- Partial multi-step work: recover existing objects and complete only missing steps.
- Asynchronous CI/review changes: read current evidence for the current exact HEAD.
- GitHub/required authority unavailable: fail closed for dependent decisions.
- Missing/corrupt checkpoint: reconstruct only when one active work unit is unambiguous.

## Post-merge recovery

A work unit is not done merely because a PR reports merged. Complete the repository's required merged-state verification and linked Issue/tracker/status housekeeping, then checkpoint `done` and stop according to repository policy.

## Visibility and privacy

Checkpoint content must be safe for the repository's visibility. Never store credentials, tokens, hidden reasoning, raw private prompts/transcripts, personal secrets, company-confidential data, or unrelated private-repository provenance.
