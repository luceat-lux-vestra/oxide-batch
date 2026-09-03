# Development and Decision Process

**State:** Accepted

## Work item flow

```text
needs-triage
    ├── needs-info / needs-reproduction
    ├── needs-design ──> RFC/ADR/spike
    └── ready ──> in progress PR ──> verified ──> closed

blocked returns to the preceding state when its dependency resolves.
```

GitHub does not currently have every state label in the diagram. Add labels
only when the transition is actively used; avoid process labels that never
drive a decision.

## Definition of ready

An implementation issue is ready when:

- user/operator problem and observable outcome are clear;
- scope and non-goals are explicit;
- acceptance and relevant failure criteria are testable;
- compatibility, API, data/restart, security, telemetry, and migration impact
  are classified;
- required ADR/RFC is accepted;
- dependencies and blockers are linked;
- milestone and owner are known;
- no unresolved design choice would fundamentally rewrite the solution.

Spikes have a question, alternatives, time/effort bound, reproducible
environment, expected evidence, and decision owner.

## Milestone activation invariant

A roadmap milestone title appearing in issue text (`M7`, `M8`, ...) is not by
itself milestone assignment. The native GitHub milestone is the authoritative
execution bucket. Before the first implementation PR of a product milestone,
all of the following must hold, and every fresh-main/task-start governance
audit must re-verify them:

- the native GitHub milestone exists and is `open`;
- the umbrella tracking issue is assigned to it;
- every open, milestone-owned child issue is assigned to it;
- cross-cutting/cross-program issues (roadmap-ledger reconciliation, repository
  hardening, performance/scalability, governance programs, and similar) are
  linked as prerequisites or context but are not assigned to the product
  milestone unless their scope is explicitly bounded to that one milestone;
- the umbrella issue's status/labels match its actual activation state
  (`planning`/`blocked` before the gate passes, `ready`/`in progress` after).

A missing native milestone, an unassigned milestone-owned issue, or a
misclassified cross-cutting issue is a governance FAIL and blocks
implementation start for that milestone, regardless of roadmap or umbrella
issue prose. Milestone shells for not-yet-active milestones may exist in
advance (see "Just-in-time milestone shells" below); shell existence alone is
never authorization to begin implementation.

### Just-in-time milestone shells

Native milestone shells for upcoming product milestones may be created ahead
of activation so umbrella trackers always have a milestone to attach to.
Creating a shell early does not authorize implementation: an umbrella tracker
assigned to a shell milestone still follows its own `planning`/`blocked`
status and entry rule before any child implementation issue opens. Detailed
child issues are assigned to the milestone only once they are actually
created.

## Milestone closure invariant

A native product milestone may be closed only after:

- its exit-gate issue is complete and post-merge validated;
- the milestone-owned open issue count is zero, or every exception (an issue
  explicitly deferred to a later milestone) carries a recorded rationale;
- roadmap, documentation, the compatibility ledger, and evidence records agree
  with the delivered state;
- the umbrella issue is closed as completed;
- only then is the native GitHub milestone closed, and the closure is
  confirmed with a fresh read of the milestone and umbrella issue state.

## Definition of done

A change is done when:

- acceptance and failure tests pass at the required layers;
- required CI and documentation checks pass;
- public API, compatibility matrix, changelog, migration, and runbooks are
  updated where applicable;
- telemetry and sensitive-data behavior is reviewed;
- no unexplained warning, flaky test, or advisory exception is introduced;
- operational rollout and rollback are understood;
- the issue records verification evidence and residual limitations.

Merge is not synonymous with milestone completion. Milestone exit criteria are
verified independently.

## RFC process

Use an RFC for user-visible semantics or a problem with multiple stakeholders
and credible designs. Flow:

1. open an RFC issue describing motivation and alternatives;
2. create a numbered RFC document from the template;
3. discuss unresolved questions and prototype risky assumptions;
4. record final disposition and rationale;
5. create or update ADRs for binding architecture decisions;
6. split implementation and conformance issues;
7. preserve rejected/superseded RFCs as decision history.

## ADR process

Use an ADR for one durable architecture choice. Accepted ADRs are immutable
except for status/link metadata. A changed choice receives a superseding ADR.
Implementation PRs link the governing ADR.

## Review order

Review high-risk changes in this order:

1. correctness and data/restart semantics;
2. security/privacy and destructive behavior;
3. compatibility and public API;
4. operations and observability;
5. tests and failure evidence;
6. maintainability and style.

## Triage and planning

- security and possible data loss/corruption are handled immediately;
- M0/M1 critical-path issues are reviewed at least weekly while active;
- milestones are capability gates without artificial due dates;
- issues are not closed for inactivity alone;
- scope changes update roadmap, risk register, and affected exit criteria.
