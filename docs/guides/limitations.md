# M5 Production Preview Limitations

**State:** Accepted

**Applies to:** OxideBatch `0.5.0`, the M5 Embedded Core Production Preview

This is the explicit limitations record the
[production preview guide](production-preview.md) and the
[M5 support matrix](../release/support-matrix.md#m5-production-preview-support-bounds)
promise. It names every compatibility-ledger row that is not advertised as
verified embedded-kernel capability in this release. The
[feature ledger](../compatibility/conformance-matrix.md) is the canonical,
machine-cross-checked source; this page is a readable summary of it and never
overrides it. No row is hidden: `Partial`, `Planned`, and `Unknown` rows are
all listed or counted below.

## Advertised capability

`29` ledger rows form the advertised embedded-kernel set this release
delivers, each with its evidence campaign already complete (see the
[#102 reconciliation](../project/m5-102-reconciliation.md)). With `0.5.0`
published and its release artifacts independently verified, `28` of the `29`
are `Verified`; `META-CONTEXT-001` remains `Implemented` pending codec
migration tests. See the
[ledger's M5 disposition set](../compatibility/conformance-matrix.md#m5-disposition-and-promotion-set)
for the exact list and the
[M5 exit record](../project/m5-exit-evidence.md) for the promoted disposition
and evidence trail. Everything below this line is **not** part of that set.

## Partial: implemented at a bounded M0-M4 boundary, expands in M6-M11

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
| [`LISTENER-ITEM-001`](../compatibility/conformance-matrix.md#fault-tolerance-repeat-listeners-flow-and-scope) | M3 read/process/write/retry/skip listener callbacks and crash scenarios | Complete listener taxonomy is M6 |
| [`FLOW-SEQUENCE-001`](../compatibility/conformance-matrix.md#fault-tolerance-repeat-listeners-flow-and-scope) | Finite acyclic sequential/conditional flow with process-kill reuse | Nested/split flow and complete M7 coverage remain |
| [`FLOW-DECIDER-001`](../compatibility/conformance-matrix.md#fault-tolerance-repeat-listeners-flow-and-scope) | Basic decider node, restart reuse, decision-commit crash boundary | Complete M7 coverage remains |
| [`REPO-COMMAND-001`](../compatibility/conformance-matrix.md#repository-operator-testing-and-observability) | Authoritative identity/lifecycle writes over OxideBatch's own schema and ports | Service split and pagination remain accepted targets |
| [`REPO-RETENTION-001`](../compatibility/conformance-matrix.md#repository-operator-testing-and-observability) | Hold, eligibility, plan/apply digest guard, bounded batches, audit | Archive, export, scheduling, and portability remain M8 |
| [`SCALE-PARSTEP-001`](../compatibility/conformance-matrix.md#local-and-distributed-scalability) | Bounded local split: factories, cancellation, aggregation, both sibling policies | Chunk branches are not required at this boundary; complete scale is M10 |
| [`SCALE-LOCALPART-001`](../compatibility/conformance-matrix.md#local-and-distributed-scalability) | Bounded local partitioning: durable state, deterministic aggregation, restart carry-forward | No lease or fencing in the local slice; RFC-0009 remote semantics remain proposed |

## Planned and Unknown: not yet implemented

`39` rows are `Planned` for a named later milestone (M6-M14), and `2` rows —
`DB-MONGO-001` and `IO-MAILLDAP-001` — are `Unknown` (reviewed disposition not
yet decided). None of these is implemented in this release. The complete list
with target milestones is the
[feature ledger](../compatibility/conformance-matrix.md); notable absences a
new user is likely to look for:

- standard reusable item components — CSV, fixed-width, JSON/JSONL, database
  cursor/paging readers and writers (`IO-FLAT-001`, `IO-STRUCTURED-001`,
  `IO-DB-001`) — M6;
- item composites, decorators, and multi-resource input/output
  (`ITEM-COMPOSITE-001`, `ITEM-DECORATOR-001`, `ITEM-MULTI-001`) — M6;
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
release. See [what M5 explicitly is not](production-preview.md#what-m5-explicitly-is-not).
