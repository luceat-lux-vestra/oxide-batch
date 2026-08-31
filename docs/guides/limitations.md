# M6 Release Limitations

**State:** Accepted

**Applies to:** the published OxideBatch `0.6.0` M6 release (tag `v0.6.0`,
2026-08-31)

This is the explicit limitations record the
[production preview guide](production-preview.md) and the
[M6 support matrix](../release/support-matrix.md#0.6.0-m6-release)
promise. It names every compatibility-ledger row that is not advertised as
verified capability in this release. The
[feature ledger](../compatibility/conformance-matrix.md) is the canonical,
machine-cross-checked source; this page is a readable summary of it and never
overrides it. No row is hidden: `Partial`, `Planned`, and `Unknown` rows are
all listed or counted below.

## Released M5 foundation and M6

The released M5 foundation has `28` of its `29` advertised rows `Verified`
against `0.5.0`; `META-CONTEXT-001` remains `Implemented`. M6 adds the item
component catalog, schema-4 component state, and public `oxide-batch-test`.
Those M6 rows have retained campaign evidence; `0.6.0` is now published with
post-publish verification passed, but promoting a row to release-backed
`Verified` evidence is a separate governance decision this document does not
make automatically upon publication. See the
[M6 disposition](../release/support-matrix.md#0.6.0-m6-release)
and [M6 exit record](../project/m6-exit-evidence.md). Everything below this
line is **not** advertised as fully verified by this release.

## Partial: implemented at a bounded M0-M5 boundary, expands in M7-M11

These `13` rows work within the boundary described, and that boundary is
narrower than full Spring Batch parity for the same feature. Each expands in a
later milestone; none is silently dropped.

| Row | What works today | What is bounded or deferred |
| --- | --- | --- |
| [`LIFE-STOP-001`](../compatibility/conformance-matrix.md#domain-identity-lifecycle-and-launch) | Application-owned cooperative stop, durable operator stop request, drain reporting, in-flight chunk policy, deadlines | Complete operator-driven stop across every execution shape is M7 |
| [`LIFE-RECOVER-001`](../compatibility/conformance-matrix.md#domain-identity-lifecycle-and-launch) | Evidence-based audited recovery: stale-clock guards, version/digest binding, `UNKNOWN`-effect reason, atomic audit | Stricter `UNKNOWN` handling than Spring; no automatic takeover |
| [`STEP-STARTLIMIT-001`](../compatibility/conformance-matrix.md#step-chunk-item-stream-and-standard-components) | Start limit and allow-start-if-complete for basic sequential flow | Complete M7 coverage across nested/split flow remains |
| [`FT-RETRY-001`](../compatibility/conformance-matrix.md#fault-tolerance-repeat-listeners-flow-and-scope) | Typed bounded retry policy, `65,535`-attempt and `256`-key caps | A crash can replay the pre-decision initial call or consume an uninvoked reservation |
| [`FT-SKIP-001`](../compatibility/conformance-matrix.md#fault-tolerance-repeat-listeners-flow-and-scope) | Classified, counted read/process/write skips; shared limit across attempts | A crash during a pre-commit skip callback may replay the callback |
| [`FT-ROLLBACK-001`](../compatibility/conformance-matrix.md#fault-tolerance-repeat-listeners-flow-and-scope) | Typed rollback-capability classifier | No-rollback is capability-scoped and still records a skip |
| [`LISTENER-ITEM-001`](../compatibility/conformance-matrix.md#fault-tolerance-repeat-listeners-flow-and-scope) | M3 callbacks plus the M6 component listener boundary | Complete flow/listener taxonomy remains later M7 scope |
| [`FLOW-SEQUENCE-001`](../compatibility/conformance-matrix.md#fault-tolerance-repeat-listeners-flow-and-scope) | Finite acyclic sequential/conditional flow with process-kill reuse | Nested/split flow and complete M7 coverage remain |
| [`FLOW-DECIDER-001`](../compatibility/conformance-matrix.md#fault-tolerance-repeat-listeners-flow-and-scope) | Basic decider node, restart reuse, decision-commit crash boundary | Complete M7 coverage remains |
| [`REPO-COMMAND-001`](../compatibility/conformance-matrix.md#repository-operator-testing-and-observability) | Authoritative identity/lifecycle writes over OxideBatch's own schema and ports | Service split and pagination remain accepted targets |
| [`REPO-RETENTION-001`](../compatibility/conformance-matrix.md#repository-operator-testing-and-observability) | Hold, eligibility, plan/apply digest guard, bounded batches, audit | Archive, export, scheduling, and portability remain M8 |
| [`SCALE-PARSTEP-001`](../compatibility/conformance-matrix.md#local-and-distributed-scalability) | Bounded local split: factories, cancellation, aggregation, both sibling policies | Chunk branches are not required at this boundary; complete scale is M10 |
| [`SCALE-LOCALPART-001`](../compatibility/conformance-matrix.md#local-and-distributed-scalability) | Bounded local partitioning: durable state, deterministic aggregation, restart carry-forward | No lease or fencing in the local slice; RFC-0009 remote semantics remain proposed |

## Planned and Unknown: not yet implemented

The remaining rows are `Planned` for a named later milestone (M7-M14), and `2` rows —
`DB-MONGO-001` and `IO-MAILLDAP-001` — are `Unknown` (reviewed disposition not
yet decided). None of these is implemented in this release. The complete list
with target milestones is the
[feature ledger](../compatibility/conformance-matrix.md); notable absences a
new user is likely to look for:

- M6 standard reusable item components, composites, decorators, multi-resource
  input/output, and test-kit rows are published in `0.6.0` with retained
  campaign and post-publish evidence; promoting them to ledger `Verified`
  status is a separate governance decision, not yet made;
- nested/split job flow, job/step scope and late binding, the definition
  registry (`FLOW-SPLIT-001`, `FLOW-NESTED-001`, `SCOPE-JOB-001`,
  `SCOPE-STEP-001`, `REPO-REGISTRY-001`) — M7;
- additional relational databases and messaging/streaming adapters
  (`DB-MYSQL-001`, `DB-SQLITE-001`, `DB-SQLSERVER-001`, `MSG-KAFKA-001`,
  `MSG-AMQP-001`, and related rows) — M8/M9;
- multi-threaded/local-chunk scaling beyond the bounded M4/M5 slice
  (`SCALE-MTSTEP-001`, `SCALE-LOCALCHUNK-001`) — M10;
- remote/distributed execution of any kind (`SCALE-REMOTEPART-001`,
  `SCALE-REMOTECHUNK-001`, `SCALE-REMOTESTEP-001`) — M11;
- Spring Batch definition/metadata migration tooling (`MIG-DEFINITION-001`,
  `MIG-METADATA-001`) — M12.

## What this means for a full-parity or readiness claim

The visibility of every row above prevents any full Spring Batch parity,
enterprise-readiness, or project-wide production-readiness claim for this
release. See [what M6 explicitly is not](production-preview.md#what-m6-explicitly-is-not).
