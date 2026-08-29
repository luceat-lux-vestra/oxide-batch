# M6 Exit Evidence

**State:** IN PROGRESS — this document reflects the candidate PR HEAD at the
time of writing, not a merged or closed state. #140 (the M6 umbrella) is
**not** closed by this document or by landing this PR; it closes only after
an independent strict-review pass on this PR's exact final HEAD SHA passes
and the PR merges, per #153's own governing instructions. Sections marked
**PENDING** below are real, tracked remaining work in this same PR, not
hidden gaps — see "Final M6 exit disposition."

## Source/base identity

- Canonical `main` HEAD at the start of #153: `e740790c9fd55b9f1f8095d30ee8897b3254c4c0`.
- Work branch: `m6/153-exit-gate`.
- #152 (the M6 issue immediately preceding #153) is `CLOSED (COMPLETED)`;
  PR #180 is `MERGED`.
- #153 is `OPEN`; #140 (M6 umbrella) is `OPEN`.

## M6 scope

M6 delivered the item-processing component model, PostgreSQL item
components, completion policies, item-level fault/listener taxonomy, and
pipeline-builder ergonomics across #143–#152. #153 does not add capability;
it proves the delivered capability set with the Gate B/Gate H campaigns
named by `docs/project/m6-design-gate-evidence.md`, closes any genuine,
narrow correctness defect that blocks proving an already-promised M6
semantic, documents the shipped surface, reconciles the compatibility
ledger, and records this exit disposition.

## #143–#152 delivery summary

| Issue | Delivered | Primary PR |
|---|---|---|
| #143 | ADR-0008 item contract + chunk runtime migration | #160 |
| #144 | `ItemStream` open/update/close + component-state contract | #161 (Postgres Gate G fixture fix in #173/#175) |
| #145 | `oxide-batch-test` application/component/restart harness (Gate G boundary) | #162 |
| #146 | Standard processors/delegates/classifiers/composites | #164 |
| #147 | Restartable delimited/CSV/fixed-width | #165 |
| #148 | Restartable JSON/JSONL | #166 |
| #149 | PostgreSQL cursor/paging/SQL batch + enlisted writer | #169 (checkpoint-coherence fix in #168) |
| #150 | Multi-resource + object-store basics | #176 (lifecycle/bound-violation fix in #178) |
| #151 | Item-level completion policies + listener-taxonomy evidence, Gate F allocation regression | #179 |
| #152 | Item pipeline configuration ergonomics | #180 |

## Gate B — transaction/restart equivalence protocol

Protocol: `docs/project/m6-design-gate-evidence.md#gate-b--transactionrestart-equivalence-protocol`.
All 8 required scenarios implemented, independently re-verified against a
real local PostgreSQL 18 by re-running from a clean build (not just trusted
from the implementing pass):

| Scenario | Result | File |
|---|---|---|
| B-01 `normal_enlisted_commit_is_representation_identical` | PASS | `crates/oxide-batch/tests/gate_b_01_normal_commit.rs` |
| B-02 `writer_failure_before_commit_rolls_back_identically` | PASS | `gate_b_02_writer_failure_rollback.rs` |
| B-03 `state_checkpoint_counter_share_one_atomic_boundary` | PASS | `gate_b_03_atomic_boundary.rs` |
| B-04 `unknown_commit_outcome_forces_recovery_not_inference` | PASS | `gate_b_04_unknown_outcome.rs` |
| B-05 `process_kill_before_commit_restart_is_identical` | PASS (real `SIGKILL`) | `gate_b_05_kill_before_commit.rs` |
| B-06 `process_kill_around_commit_acknowledgement_is_identical` | PASS (real `SIGKILL`) | `gate_b_06_kill_around_acknowledgement.rs` |
| B-07 `multi_chunk_restart_selects_identically` | PASS (two real crash/restart cycles) | `gate_b_07_multi_chunk_restart.rs` |
| B-08 `representation_does_not_change_definition_or_restart_identity` | PASS (fingerprint + cross-representation restart, both directions) | `gate_b_08_representation_transparent_identity.rs` |

**PostgreSQL 15 result: PENDING final retained run.** The release-blocking
15 leg is executed by the dedicated Gate B CI matrix and will be named here
with its final workflow/artifact identity after the candidate HEAD is fixed.

**PostgreSQL 18 result: PASS locally; PENDING final retained run.** All 8
scenarios, all 20 tests across the 9 Gate B files (including the shared-harness
foundation smoke test), passed locally against a real PostgreSQL 18 instance.
The final exit record will use the retained 15/18 reports from the same
candidate merge ref rather than treating the local run as provenance.

**Gate B PASS condition met** for the PostgreSQL 18 leg: every durable
observation compared (business rows, checkpoint, component state, counters,
lifecycle trace) was representation-independent across all 8 scenarios, via
structured `GateBObservation` comparison (`crates/oxide-batch/tests/support/gate_b.rs`),
never string-log eyeballing.

**Real defect found and fixed during this work** (harness-internal, not a
framework defect): the Gate B harness's `BusinessWriter` originally wrote
business rows through its own independent connection instead of the
chunk's enlisted transaction, silently defeating `AtomicSameResource`
regardless of the actual framework behavior — caught directly by B-03's
forced-failure assertion, fixed by writing through the enlisted
`BusinessTransaction`. See `docs/guides/restart-and-state.md#checkpoint-relationship-and-transaction-atomicity`
for the full account.

## Gate H — P-002 real-component performance protocol

Protocol: `docs/project/m6-design-gate-evidence.md#gate-h--p-002-real-component-performance-protocol`.
Reference workload: real, shipped `DelimitedReader`/`DelimitedWriter` (CSV
parsing/formatting, #147) around real, shipped `IdentityProcessor`, typed
vs `Boxed*`, chosen because both components do genuine per-item work,
unlike a synthetic pass-through fixture — see `gate_h_allocation.rs`'s
module doc for the full reasoning.

### Hard invariant 1 — typed per-item future allocation == 0

**PASS**, proved structurally (`crates/oxide-batch/tests/gate_h_dispatch.rs`):
`ChunkStep<I,O,R,P,W>` stores reader/processor/writer as bare `R`/`P`/`W`
fields (`crates/oxide-batch/src/chunk_runtime.rs:61`); for the typed
representation these are concrete structs (`DelimitedReader<Src>`,
`DelimitedWriter`, `IdentityProcessor`) with no `dyn` anywhere in their own
definitions, verified by reading them — so no per-item `Box::pin` is
possible in that call chain, because there is no erasure boundary to
cross. Corroborated by two automated checks: `BoxedReader`/`BoxedProcessor`/
`BoxedWriter` are exactly fat-pointer-sized (computed against the
platform's own `Box<dyn Trait>` size), typed components are not; and by the
pre-existing zero-allocation synthetic fixtures
(`chunk_allocation.rs`/`item_components_allocation.rs`), re-verified
passing, delta = 21 allocator calls across a 19,800-item span (constant,
not scaling).

### Hard invariant 2 — typed path requires no framework-controlled dynamic dispatch per item

**PASS**, same structural proof: no `dyn` in the typed instantiation's
reachable reader/processor/writer types means no vtable exists for any
per-item call to resolve through.

### Listener-enabled companion measurement

Built by #151 under Gate F (`crates/oxide-batch/tests/item_listener_allocation.rs`),
and now included as a separate target in the Gate H campaign: listener-enabled allocation
delta scales with item count (the opposite of the typed-path zero
guarantee), reported as a measurement fully separate from typed component
allocation, per Gate F's own decision to keep boxed per-item-per-phase
listener representation for M6 — not folded into or weakening the
listener-free hard guarantee above.

### Required metrics (disclosure, not gating)

- **Allocations/item, allocations/chunk**: `gate_h_allocation.rs` — the
  campaign retains structured raw allocator statistics for both
  representations, including calls/item and calls/chunk. The local release
  run measured typed 24.001 calls/item and boxed 28.001 calls/item on the
  real CSV workload; both are dominated by CSV parsing/formatting, not
  framework overhead. Typed never exceeds boxed per item (asserted and
  verified).
- **Throughput/latency**: `gate_h_throughput.rs` retains raw nanosecond
  samples, min/mean/max, and derived throughput for both representations.
  The local release run measured typed mean 47.689 ms / 1,048,465 items/s and
  boxed mean 48.548 ms / 1,029,908 items/s. Final retained CI values remain
  pending until the candidate merge ref is fixed.
- **Binary-size delta, compile-time delta**: the Gate H runner measures both
  release reference examples and retains raw bytes and wall-clock build
  seconds; final CI retention remains pending.
- **Dynamic dispatch count, future boxing count**: proved as an
  architecture invariant (== 0 for typed), not counted at runtime — no such
  counter exists in this codebase and building one would be new,
  unvalidated instrumentation; see `gate_h_dispatch.rs`'s own reasoning.
- **Environment metadata**: captured per run via
  `performance::measurement_environment` (source commit, Rust/toolchain
  version implicitly via the build, OS/kernel, CPU model, compiler
  profile) — printed by `gate_h_throughput.rs`; formal retained-evidence
  environment capture (LLVM version, feature set, full repetition/variance
  record) is part of the evidence-retention pass.

**No invented threshold.** No throughput/latency/allocation number above is
asserted as a pass/fail gate; the only hard PASS/FAIL criteria are the two
invariants above (both PASS) and Gate B's semantic equivalence (PASS locally
on PostgreSQL 18, PostgreSQL 15 pending final CI retention).

## Full M6 conformance / malformed / failure / rollback / stop / panic / crash matrix

See `docs/engineering/campaigns/m6/component-conformance-matrix.md` (this
campaign's own deliverable) for the complete, component-by-component
breakdown. Summary: every first-party M6 component has real evidence for
every scenario its own documented contract says applies, or an explicit
contract-based reason it does not (composition/decoration inherits a
delegate's restart evidence rather than needing its own; `InMemoryObjectStore`
has no durable backing store to restart against). One real, confirmed gap
was found and closed: `MultiResourceWriter` had a durable-checkpoint
contract but no restart/crash test, unlike its reader counterpart — closed
with `multi_resource_writer_restarts_across_a_resource_boundary_crash`
(`crates/oxide-batch-test/tests/postgres_multi_resource_restart.rs`),
verified against real PostgreSQL 18. No gap requiring new production API
was found.

A significant correction happened while building this matrix: the initial
gap inventory for #153 checked only `crates/oxide-batch/tests/` and wrongly
concluded several components (`aggregate`, `classify`, `composite`, `sync`)
were untested; they are extensively covered in
`crates/oxide-batch-test/tests/`, which the inventory had missed entirely.
Corrected directly in the matrix document. The new `m6-conformance` campaign
now executes this fixed component/failure/restart denominator on PostgreSQL
15 and 18 and retains per-target structured outcomes; final reports remain
pending CI retention.

## Component catalog and documentation

Four M6 documentation deliverables published, indexed in
`docs/documentation/strategy.md` and `docs/README.md`, every internal link
and cited API name verified against real source:

- `docs/guides/component-reference.md` — user-facing reference for every
  first-party M6 component.
- `docs/guides/restart-and-state.md` — distills and cross-references the
  existing canonical restart/state content into one operator/component-
  author guide.
- `docs/guides/extension-guide.md` — contracts a custom component
  implementation must honor.
- `docs/guides/test-kit-tutorial.md` — `oxide-batch-test`'s real public API,
  every example drawn from a real, currently-passing test.

Support tier: every M6 component is **First-party** per the existing
Integration Model tier system (`docs/architecture/integration-model.md`) —
no new, parallel tier scheme was introduced. The pre-release/candidate
limitation and release-promotion rule are also recorded in
`docs/release/support-matrix.md`.

## Ledger reconciliation result

Every M6-scoped row in `docs/compatibility/conformance-matrix.md` reviewed
(23 rows, including the four shared M2/M6 rows). Four shared rows received
wording reconciliation so their released `0.5.0` `Verified` status is not
misread as retroactive M6 verification; the M6-specific rows retain their
implementation disposition and candidate-HEAD evidence links. No row was
promoted to `Verified`: that
requires a named released version carrying the evidence, which M6 does not
yet have, per the ledger's own binding promotion rule — this campaign's
evidence completeness does not bypass that rule.

## Retained artifact/provenance references

**PENDING final retention.** The `xtask gate-b`, `xtask gate-h`, and
`xtask m6-conformance` campaign runners and their dedicated CI workflows now
exist, following the M5 campaign shape. The final section will name real CI
run/artifact identities for both PostgreSQL majors, the Gate H report, and
the refreshed M5 reports before this PR is marked ready for strict review.

`cargo xtask evidence` itself was generalized early in this campaign
(commit `958c762`) to check both `docs/engineering/campaigns/m5/` and a new
`docs/engineering/campaigns/m6/` directory, skipping the latter until it
exists — so `cargo xtask evidence` continues to pass checking only `m5/`
until the M6 provenance lands, at which point it becomes mandatory for both.

Editing the shared verifier files (`xtask/src/{evidence,main}.rs`) and
adding the M6 campaign wiring mechanically invalidates the *existing* M5
retained-evidence hash lineage
(every M5 campaign's `campaign-semantics.json` declares these shared files
in its closure) — this is a known, previously-executed pattern in this
repository (`631ae60 ci(m5): retain evidence for #152` did the same after
#152's changes), not a scope violation, and an M5 re-retention pass is
included in the evidence-retention work below.

## Unresolved P0/P1 review

Checked directly via `gh issue list --state open --label priority:p0`
(empty) and `--label priority:p1` (only #153 itself and the #140 umbrella,
both expected to remain open by design until this PR passes independent
strict review and merges). No correctness P0/P1 blocks M6 exit.

## Known non-blocking limitations / follow-ups

None identified requiring a new capability, API, or architecture follow-up
issue. Every gap found during this campaign (the `MultiResourceWriter`
restart gap, the flawed initial component-coverage assumption, the Gate B
harness's non-enlisted-writer bug) was small, contained, and closed
directly within #153's scope, per the work order's own scope-discipline
rule.

## Final M6 exit disposition

**NOT YET FINAL.** This document will be updated once the evidence-
retention pass below completes, immediately before this PR is marked ready
for strict review:

- [x] Gate B: 8/8 scenarios PASS on PostgreSQL 18 (local, independently
      re-verified).
- [ ] Gate B: PostgreSQL 15 leg (CI).
- [x] Gate H: both hard invariants PASS (structural proof + empirical
      corroboration).
- [x] Gate H: required disclosure metrics captured (debug-profile local
      run for throughput/latency).
- [ ] Gate H: release-profile retained throughput/latency run, binary-size
      delta, compile-time delta (CI).
- [x] Full M6 component conformance/failure matrix published, one real gap
      found and closed.
- [x] M6 documentation set published (all four deliverables).
- [x] Ledger reconciliation complete.
- [x] P0/P1 review clean.
- [ ] Gate B/Gate H campaign CI workflows + retained evidence/provenance.
- [ ] M5 evidence re-retention pass (required by editing shared xtask
      verifier files).
- [ ] `cargo xtask evidence` PASS on final HEAD, covering both `m5/` and
      `m6/`.
- [ ] Full verification command set (work order §12) run on final HEAD.

**#140 does not close with this document or this PR's merge trigger alone**
— it closes only after an independent strict-review pass on this PR's exact
final HEAD SHA passes and the PR merges.
