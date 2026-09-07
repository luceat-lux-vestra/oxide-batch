# M7 Advanced Flow, Scope, Repeat, and Composition Kickoff Gate

**State:** Active on merge

**Umbrella:** GitHub issue
[#192](https://github.com/luceat-lux-vestra/oxide-batch/issues/192)

**Kickoff tracking:** GitHub issue
[#193](https://github.com/luceat-lux-vestra/oxide-batch/issues/193)

**Design closure:** GitHub issue
[#194](https://github.com/luceat-lux-vestra/oxide-batch/issues/194)

**Authorization baseline:** `main`
`01c41ef31aa5214682ab6dc7a4b2ab781afb6f9b`

This record turns the accepted M7 roadmap outcome and the M7-M14 reconciliation
from issue #189 into definition-ready, dependency-ordered work. It freezes
capability ownership, prerequisite invariants, design gates, and evidence
obligations. It does not implement M7 behavior and does not promote any
compatibility-ledger row.

Closing #193 authorizes #194 to close the named design decisions. It does **not**
authorize production implementation in #195-#199. Those issues remain blocked
until #194 has closed every decision that affects their public API, durable
meaning, restart selection, resource/security boundary, or evidence protocol.

## Satisfied activation prerequisites

- [x] M6 delivery and exit are complete through #140 and #153. M7 consumes the
      released M0-M6 contracts as prerequisites rather than reopening them.
- [x] Issue #241 created native GitHub milestone #8, `M7: advanced flow, scope,
      repeat, and composition`, and fresh readback confirms #192-#200 are
      assigned to it.
- [x] Issue #189 is `completed`. PR #261 reconciled M7-M14 roadmap and ledger
      ownership and merged to `main` at
      `01c41ef31aa5214682ab6dc7a4b2ab781afb6f9b`; its exact post-merge main ran
      17/17 push workflows successfully.
- [x] The M7 slice below has no unresolved `Unknown` disposition. The two
      project-wide `Unknown` rows remain outside M7: `DB-MONGO-001` is owned by
      the M8/#201 decision gate and `IO-MAILLDAP-001` by the M13/#207 decision
      gate.

## Scope and impact classification

M7 extends the already accepted compiled-plan/repository model with advanced
flow composition, job/step scope and late binding, repeat/interceptor state,
definition registry/evolution, complete M7 restart controls, and the
operator/explorer surfaces required to operate those definitions.

M7 may add public API and restart-relevant manifest/state. Therefore no new
node, scope, repeat/interceptor, registry, upgrade, restart-control, or service
shape may land until #194 records the exact durable/public impact and the
migration/rejection rules that make it safe.

M7 explicitly does **not** authorize:

- M8 repository portability, additional database adapters, archive/export
  portability, or new cross-resource delivery mechanisms;
- M9 messaging, HTTP, object-store provider certification, or webhook adapters;
- M10 multi-threaded item processing, local chunking, dynamic partitioning,
  scheduler/performance rewrites, or Arrow/Parquet paths;
- M11 remote execution, worker registration, transport profiles, leases, or
  fencing semantics;
- M12 Spring migration tooling or project-wide ledger closure;
- any hosted scheduler, control plane, identity/RBAC system, or generic
  cross-resource exactly-once claim.

## M0-M6 invariants M7 must preserve

These are prerequisites, not design choices reopened by M7.

1. **Repository authority.** Durable repository state remains authoritative for
   instance uniqueness, lifecycle, definition identity, checkpoints, ownership,
   recovery, and committed decisions. Telemetry, process memory, and transport
   acknowledgement never override repository state.
2. **Restart from committed state only.** Restart creates new attempts and uses
   the last valid committed checkpoint/context/counters. In-memory or
   uncommitted work cannot become restart authority.
3. **Fail-closed definition identity.** Every execution binds immutable
   revision/manifest/fingerprint identity. Same revision with a different
   fingerprint is drift; restart rejects it before a lifecycle write unless one
   explicit directed compatibility edge applies.
4. **No inferred compatibility.** Names, revision ordering, semantic-version
   syntax, successful deserialization, or implementation representation never
   establish restart compatibility. `Force` remains unavailable.
5. **Representation transparency.** The M6 typed versus `Boxed*` component
   representation does not change logical component identity, manifest entry,
   definition fingerprint, checkpoint semantics, transaction ports, or restart
   selection.
6. **Transaction/checkpoint atomicity.** Same-resource business writes,
   checkpoint, context, counters, and optimistic version retain the accepted
   atomic transaction boundary. Ambiguous commit becomes `UNKNOWN`; no generic
   arbitrary-resource exactly-once guarantee is introduced.
7. **Deterministic flow authority.** A transition/decider result commits before
   its target starts. Restart reuses the matching committed decision and
   completed work; it cannot choose a different durable path because of process
   timing, registration order, or concurrency.
8. **Lifecycle and failure separation.** Batch lifecycle status and exit outcome
   remain distinct. Illegal transitions, stale versions, user errors, and panic
   stay typed and fail closed; panic is contained at the framework boundary.
9. **Bounded durable/user state.** Context, checkpoint, component state,
   manifest, diagnostic, and request values remain versioned, bounded,
   corruption-checked where specified, sensitivity-aware, and reject unsupported
   newer versions rather than defaulting or truncating them.
10. **Structured ownership and cancellation.** Framework-owned work has bounded
    task/resource ownership, propagates cancellation, joins owned children, and
    never fabricates terminal completion after an incomplete drain.
11. **M4 local-scale equivalence.** Existing local split/partition behavior
    remains deterministic and sequential-fallback equivalent. M7 may generalize
    graph composition; it may not use that work to claim M10 scheduling or
    performance semantics.
12. **Operator mutation discipline.** Mutating operator actions remain guarded,
    optimistic-versioned, idempotent by operation ID, auditable, and explicit
    about unknown commit outcomes.
13. **Explorer bounds and redaction.** Explorer queries remain closed-set,
    bounded, keyset-paginated over immutable ordering keys, timeout-bounded, and
    redacted by construction. M7 extensions may not introduce unbounded query
    or payload surfaces.
14. **Facade isolation.** Public core/service contracts do not expose SQLx,
    Tokio runtime ownership, credentials, deployment authorization internals,
    driver diagnostics, or other adapter/deployment implementation types.

Changing any invariant above requires the appropriate superseding RFC/ADR and
cannot be smuggled through a delivery issue.

## Existing semantics: prerequisite versus M7 completion

| Existing slice | Preserved prerequisite | M7 completion boundary |
| --- | --- | --- |
| M3 sequential/conditional flow | finite acyclic graph, deterministic transition specificity, committed decision before target start, restart reuse | #195 may add nested/general composition and M7 graph forms without changing the existing decision authority |
| M3 decider | side-effect-free typed decision input and committed result as restart authority | #195 composes deciders inside the accepted advanced-flow model |
| M3 start controls | atomic start-limit accounting and existing sequential `allow_start_if_complete` behavior | #199 completes M7 definition/operator semantics for restart controls |
| M4 local split | durable child identity, deterministic aggregation, structured ownership/cancellation, sequential fallback | #195 owns M7 split **graph and restart semantics**; M10/#203 retains local high-performance scheduling/execution ownership |
| M4 operator/explorer | idempotent audited mutations, lifecycle guards, bounded keyset queries, redaction | #199 extends the service surface to M7 definitions, lineage, scopes, and restart controls without weakening the M4 contract |
| M6 completion/fault/listener engine | bounded completion policies, retry/skip/rollback/listener ordering and durable accounting | #197 adds repeat context/interceptors and accepted flow-level composition on the same authoritative engine, not a second engine |
| M6 scoped test fixture | `TEST-SCOPE-001` constructs real public context values for component tests | #196 owns real M7 scope lifecycle; the fixture is evidence infrastructure, not an implementation of job/step scope |

## #194 design gates

All gates below are **open** at #193 closure. #194 is their canonical decision
owner and must close them before dependent production implementation starts.

| Gate | Decision that #194 must freeze | Dependent delivery |
| --- | --- | --- |
| A — advanced flow/composition | finite nested/split/nested-job topology, node identity, durable branch/join/child state, aggregation, custom-plan-node boundary, restart selection, failure/stop propagation, depth/fan-out bounds | #195 |
| B — scope/late binding | job/step scope lifetime, factory creation/reuse/cleanup, allowed late-bound sources, fingerprint versus durable-state participation, sensitivity/redaction, partial-construction failure, recursion/dependency bounds | #196 |
| C — repeat/interceptors | repeat context/state identity and bounds, interceptor lifecycle/order, nesting with completion/retry/skip/rollback/listeners/scopes/flow, panic pairing, durable attempt accounting and restart-relevant policy identity | #197 |
| D — registry/evolution | `DefinitionRegistry` lookup/version/artifact identity, exact restart default, directed upgrade edge and state transformation, fork lineage, manifest/schema versioning, drift/newer/corrupt rejection, concurrency/idempotency | #198 |
| E — application/operator completion | parameter incrementer identity, non-restartable/start controls at service boundaries, versioned/nested launch/restart/upgrade/fork actions, explorer projections, bounded query forms, audit/idempotency/redaction | #199 |
| F — cross-cutting evidence and evolution | manifest/schema migration and rollback, cancellation/resource bounds, public diagnostics/redaction, PG15/18 crash points, deterministic normalized observations, and evidence retention | #195-#200; #200 verifies the milestone aggregate |

If one decision cannot be closed cohesively inside these boundaries, #194 must
create a bounded M7 child, assign it to native milestone #8, and update #192
before any dependent implementation begins.

## Exact M7 ledger delivery/evidence ownership

The compatibility ledger remains authoritative for row status and the
`U/I/C/Cr/M/P` evidence profile. This kickoff copies those profiles verbatim;
it does not weaken them or promote a row. “Owner” below means the one M7 issue
responsible for delivering the M7 semantic tail and producing its row-level
evidence. #200 independently verifies and aggregates milestone exit evidence;
it is not a second row owner.

| Ledger row | Baseline status | Evidence U/I/C/Cr/M/P | Exact M7 owner | M7 boundary |
| --- | --- | --- | --- | --- |
| `DOM-JOB-001` | Verified | R/R/R/J/J/N | #195 | advanced compiled-flow/composition extension only |
| `DOM-EXIT-001` | Verified | R/R/R/R/J/N | #195 | flow-facing outcome mapping in advanced composition |
| `STEP-TASKLET-001` | Verified | R/R/R/R/J/R | #195 | general compiled-plan lowering tail; base tasklet behavior unchanged |
| `STEP-CUSTOM-001` | Planned | R/R/R/R/J/R | #195 | bounded registered custom plan-node/step under the one runtime |
| `STEP-JOB-001` | Planned | R/R/R/R/R/J | #195 | nested-job node, lineage, outcome/restart mapping |
| `FLOW-SEQUENCE-001` | Partial | R/R/R/R/R/J | #195 | complete M7 general-flow coverage over the M3 base |
| `FLOW-DECIDER-001` | Partial | R/R/R/R/R/J | #195 | decider composition without changing committed-decision authority |
| `FLOW-SPLIT-001` | Planned | R/R/R/R/R/R | #195 | M7 graph/restart semantics only; M10/#203 keeps local scale/performance tail |
| `FLOW-NESTED-001` | Planned | R/R/R/R/R/J | #195 | nested flow/job composition and restart |
| `SCOPE-JOB-001` | Planned | R/R/R/R/R/J | #196 | real job-scope lifecycle and late binding |
| `SCOPE-STEP-001` | Planned | R/R/R/R/R/J | #196 | real step-scope lifecycle and late binding |
| `TEST-SCOPE-001` | Implemented | R/R/R/J/R/N | #196 | evidence over the delivered M7 scope lifecycle; existing fixture remains the harness |
| `REPEAT-POLICY-001` | Partial | R/R/R/R/R/R | #197 | flow-level repeat/interceptor composition over the M6 policy base |
| `REPEAT-CONTEXT-001` | Planned | R/R/R/R/R/J | #197 | bounded repeat context/callback/interceptor state and restart |
| `LIFE-RESTART-001` | Verified | R/R/R/R/R/J | #198 | general compiled-plan restart/upgrade selection; service exposure is consumed by #199 |
| `LIFE-DEFINITION-001` | Verified | R/R/R/R/R/J | #198 | schema-transforming compatible upgrades and fork lineage |
| `REPO-REGISTRY-001` | Planned | R/R/R/R/R/R | #198 | `DefinitionRegistry`, revision/manifest/fingerprint/artifact resolution |
| `DOM-PARAM-002` | Planned | R/R/R/J/R/N | #199 | deterministic typed parameter incrementer and instance selection |
| `LIFE-NORESTART-001` | Planned | R/R/R/R/R/N | #199 | non-restartable job/step policy at launch/restart/operator boundary |
| `LIFE-STOP-001` | Partial | R/R/R/R/J/R | #199 | complete M7 operator stop behavior over the durable M4 base |
| `LIFE-ABANDON-001` | Verified | R/R/R/R/R/N | #199 | M7 operator/definition tail only; existing guarded abandon remains verified |
| `STEP-STARTLIMIT-001` | Partial | R/R/R/R/R/N | #199 | complete M7 start-limit/allow-start-if-complete service semantics |
| `REPO-EXPLORE-001` | Verified | R/R/R/J/R/R | #199 | bounded projections/query forms for M7 definitions/lineage/scope evidence |
| `REPO-OPERATOR-001` | Verified | R/R/R/R/R/R | #199 | complete registry/versioned-definition operator behavior |
| `OPS-CLI-001` | Verified | R/R/R/J/R/N | #199 | CLI exposure of delivered M7 operator actions without weakening automation safety |

There are 25 M7-scoped rows in this freeze. Each appears exactly once above.
Any later change to row ownership requires a reviewed update to this record and
#192 before the affected implementation proceeds.

## M7 evidence overlay

The ledger profile is the minimum per-row evidence obligation. M7 additionally
freezes these milestone-specific obligations because it introduces new durable
boundaries and composition across already-correct subsystems.

- **PostgreSQL 15 and 18:** every newly durable M7 decision/state boundary must
  have visible PG15 and PG18 restart/process-kill evidence. A row with `Cr=J`
  may omit a crash case only with an explicit reviewed N/A justification tied
  to a boundary that is genuinely non-durable; green-by-skip is not evidence.
- **Advanced flow (#195):** invalid topology, bounded depth/fan-out, deterministic
  transition/branch/join aggregation, child completion reuse, stop/failure/
  panic/`UNKNOWN` propagation, process kill before/after durable decision and
  child/join boundaries, and sequential-normalized equivalence where parallel
  execution is not semantically required.
- **Scope (#196):** creation/reuse/cleanup ordering over success/failure/panic/
  stop/cancel/restart and partial construction; allowed-source validation,
  missing/invalid/sensitive values, redacted diagnostics, recursion/dependency
  bounds, and restart resolution that either reproduces durable meaning or
  fails closed on incompatible definition/state.
- **Repeat (#197):** nested ordering across completion/retry/skip/rollback/
  listeners/scopes/flow, durable attempt accounting, callback panic/error
  pairing, bounded context/history/interceptor chains, policy fingerprint
  drift, and PG process-kill at newly durable repeat boundaries.
- **Definition evolution (#198):** exact, compatible-upgrade, incompatible,
  fork, stale/concurrent registry update, missing artifact, corrupt/newer
  manifest/state, deterministic migration, and process-kill before/after
  registry/upgrade durability boundaries. Drift must reject before an
  unauthorized lifecycle write.
- **Operator/explorer (#199):** duplicate/concurrent incrementer/launch cases,
  lifecycle guard/rejection/audit/idempotency, upgrade/fork actions, bounded
  keyset explorer traversal for new projections, stale-version and unknown
  outcomes, redaction, and facade implementation-type leakage checks.
- **Exit (#200):** independently verify the row matrix, run the complete M7
  conformance and dual-PG failure/restart campaigns, reconcile ledger/docs/
  support statements, and prove required campaigns executed rather than
  passing through skips.

No performance number changes a correctness or restart semantic. Any M7
resource measurement is evidence that a declared bound holds, not authority to
pull M10 optimization into M7.

## Delivery order and blockers

```text
#193 ownership/evidence freeze
  ↓
#194 design closure (all Gates A-F)
  ├── #195 advanced flow/custom node
  ├── #196 scope/late binding
  ├── #197 repeat/interceptors
  └── #198 registry/evolution
          ↓
        #199 operator/explorer/incrementer/restart controls
          ↓
#200 complete conformance + PG15/18 restart + docs/ledger exit
```

After #194 closes, #195-#198 may proceed independently only where their
contracts do not depend on an unfinished sibling implementation. #199 builds on
#198 and consumes the delivered definition forms through the accepted service
boundaries; it must not duplicate runtime-internal restart logic owned by #198.
#200 follows all M7 delivery work.

Throughout M7, native milestone #8 remains the execution bucket. Any bounded
M7 child created later must be assigned to milestone #8 before work begins.
Missing milestone assignment is a governance FAIL.

## Production-implementation authorization rule

At #193 closure:

- #194 may move to `status:ready` and perform design/evidence-protocol work.
- #192 remains planning-blocked until #194 closes.
- #195-#200 remain blocked.
- No production M7 implementation is authorized by this document.

Only after #194 has merged, passed exact-main verification, and closed its
semantic acceptance may the appropriate #195-#199 implementation issues be
unblocked according to the dependency graph above.

## #193 acceptance checklist

- [x] #189 reconciliation is consumed into one exact M7 slice.
- [x] Native milestone #8 exists and owns #192-#200.
- [x] M3/M4/M6 prerequisite semantics are separated from M7 completion work.
- [x] The existing M0-M6 correctness/restart/resource/service invariants M7
      must preserve are enumerated.
- [x] #194 owns every unresolved public-API/durable/restart/security design
      decision before implementation.
- [x] All 25 M7-scoped ledger rows have exactly one M7 delivery/evidence owner
      and their canonical ledger evidence profile is preserved verbatim.
- [x] PG15/18 restart/process-kill, deterministic flow, scope resolution,
      definition evolution, operator, resource, and redaction evidence
      obligations are frozen before implementation.
- [x] M8-M12 and M10/M11 execution optimizations/distribution remain outside
      M7 authorization.
- [x] #200 is an independent exit verifier/aggregator, not a duplicate row
      owner.
- [x] At #193 closure, #195-#199 production implementation was blocked on #194;
      after #194 semantic acceptance, fresh GitHub dependency state is the
      authorization authority for unblocking.