# M7-M14 Roadmap and Feature-Ledger Reconciliation

**State:** Active on merge of issue #189

**Audit baseline:** `main` commit
`819a7c1cb048373c932ec183be0a79b1030f4fb0`

**Feature-ledger baseline:** Spring Batch 6.0.4,
`docs/compatibility/conformance-matrix.md` blob
`082d1449913084e01354b5c2a7b51bc472d49af4`

**Issue:** [#189](https://github.com/luceat-lux-vestra/oxide-batch/issues/189)

This record reconciles the accepted M7-M14 roadmap and canonical target
architecture with the Spring Batch feature ledger. It is a traceability and
ownership record, not a product implementation authorization and not a second
feature-status authority.

`docs/compatibility/conformance-matrix.md` remains authoritative for Spring
feature population, row status, evidence profiles, and released compatibility
claims. This record answers the separate question that #189 must close:
for every accepted post-M6 capability, what stable ledger identity or explicit
disposition owner prevents the capability from being silently lost?

A later milestone may refine a tracker into bounded child issues, but it may
not remove a capability from this mapping without a reviewed roadmap,
compatibility, RFC/ADR, or support decision as appropriate.

## Reconciliation invariants

- Mapping is by observable semantic identity, not similar wording.
- A roadmap capability and a Spring ledger row are not required to be 1:1.
- A native-only architecture or integration capability is not given a fake
  Spring source row merely to make the tables symmetrical.
- Existing `Verified`, `Implemented`, and `Partial` evidence is not promoted by
  this audit.
- An earlier `Verified` row can have a later-milestone extension without
  weakening or retroactively broadening the released claim that earned
  `Verified`.
- `Unknown` is a failing implementation input. The two current `Unknown` rows
  remain visible only because this record gives each one a named decision gate
  and owner before its milestone may decompose implementation work.
- M11 production implementation remains blocked by RFC-0009. A proposed
  distributed architecture is an inventory source, not authorization.
- Performance or scalability work remains evidence-driven. M10 ownership does
  not authorize speculative optimization or a stronger performance claim.

## Current ledger inventory and stale-count correction

Reading all 83 rows at the audit baseline produces this current status
population:

| Status | Rows |
| --- | ---: |
| `Verified` | 28 |
| `Implemented` | 13 |
| `Partial` | 14 |
| `Planned` | 26 |
| `Unknown` | 2 |
| **Total** | **83** |

The M5 historical population in the feature ledger correctly records 39
`Planned` rows at that earlier gate. At the audit baseline, the later M6
disposition paragraph still said that “the `39` `Planned` rows keep their
accepted milestone”. That was stale after M6 moved 13 rows from the prior
population into `Implemented` or `Partial` states. #189 corrects the canonical
ledger itself to the current population above; the historical M5 count remains
unchanged and no row is promoted by this reconciliation.

The audit-baseline M6 paragraph saying the retained `Partial` rows “expand in
M7-M11” was also too broad when read as an implementation assignment. Some
partial rows encode reviewed divergence with no accepted M7-M11 implementation
gap. #189 corrects that canonical guidance and the ownership matrix below
records the reviewed future semantic/disposition owner; feature status and
evidence authority remain in the conformance matrix.

## M7-M14 capability-family traceability

| Milestone | Accepted capability family | Stable ledger coverage | Explicit owner/disposition |
| --- | --- | --- | --- |
| M7 | advanced sequential/conditional flow, splits, nested flow/job, custom plan nodes, deterministic durable decisions | `FLOW-SEQUENCE-001`, `FLOW-DECIDER-001`, `FLOW-SPLIT-001`, `FLOW-NESTED-001`, `STEP-CUSTOM-001`, `STEP-JOB-001`; released base also exists in `DOM-JOB-001`, `DOM-EXIT-001`, `STEP-TASKLET-001` | M7 umbrella [#192](https://github.com/luceat-lux-vestra/oxide-batch/issues/192); ownership freeze [#193](https://github.com/luceat-lux-vestra/oxide-batch/issues/193); design gate [#194](https://github.com/luceat-lux-vestra/oxide-batch/issues/194); flow delivery [#195](https://github.com/luceat-lux-vestra/oxide-batch/issues/195) |
| M7 | job/step scope, component factories, late binding, scoped cleanup | `SCOPE-JOB-001`, `SCOPE-STEP-001`; `TEST-SCOPE-001` supplies an existing test-kit base only | [#196](https://github.com/luceat-lux-vestra/oxide-batch/issues/196), with conformance/exit evidence in [#200](https://github.com/luceat-lux-vestra/oxide-batch/issues/200) |
| M7 | repeat context/interceptors and flow-level composition with fault semantics | `REPEAT-POLICY-001`, `REPEAT-CONTEXT-001`; existing fault/listener rows remain prerequisites rather than a second engine | [#197](https://github.com/luceat-lux-vestra/oxide-batch/issues/197) |
| M7 | definition registry, compatible upgrade edges, fork lineage, general compiled-plan restart | `REPO-REGISTRY-001`; released base in `LIFE-DEFINITION-001` and `LIFE-RESTART-001` | [#198](https://github.com/luceat-lux-vestra/oxide-batch/issues/198); exact cross-row split is frozen by #193/#194 |
| M7 | parameter incrementer, start/non-restart controls, launcher/operator/explorer completion | `DOM-PARAM-002`, `LIFE-NORESTART-001`, `LIFE-STOP-001`, `STEP-STARTLIMIT-001`; released base in `LIFE-ABANDON-001`, `REPO-EXPLORE-001`, `REPO-OPERATOR-001`, `OPS-CLI-001` | [#199](https://github.com/luceat-lux-vestra/oxide-batch/issues/199), with final evidence [#200](https://github.com/luceat-lux-vestra/oxide-batch/issues/200) |
| M8 | repository portability, capability negotiation/certification, cursor/paging/keyset/streaming, batch/upsert/stored procedure, same-resource enlistment | `REPO-COMMAND-001`, `IO-DB-001`, `DB-POSTGRES-001`, `DB-MYSQL-001`, `DB-SQLITE-001`, `DB-SQLSERVER-001`, `DB-ENTERPRISE-001`, `DB-MONGO-001`, `META-CONTEXT-001`, `TEST-REPO-001` | M8 tracker [#201](https://github.com/luceat-lux-vestra/oxide-batch/issues/201) |
| M8 | durable effect primitives: outbox, inbox/dedup, effect journal, idempotency, unknown-commit recovery | No distinct Spring row is required: these are native delivery primitives behind capability-specific adapters. Their Spring-facing consequences remain represented by repository/integration rows. | Native-extension workstream owned by [#201](https://github.com/luceat-lux-vestra/oxide-batch/issues/201); M9 adapters consume the same model through #202 rather than inventing a second delivery model |
| M8 | retention/archive/export portability | `REPO-RETENTION-001`, `META-RETENTION-001`; `META-UPGRADE-001` supplies the released schema-evolution base | [#201](https://github.com/luceat-lux-vestra/oxide-batch/issues/201); project-wide compatibility carry-forward is rechecked by M14 #208 |
| M9 | broker/stream adapters, envelope, offset/ack/redelivery, DLQ/poison, replay/backpressure | `MSG-KAFKA-001`, `MSG-AMQP-001`, `MSG-OTHER-001` | M9 tracker [#202](https://github.com/luceat-lux-vestra/oxide-batch/issues/202) |
| M9 | object-store readers/writers and provider certification | `IO-OBJECT-001` | [#202](https://github.com/luceat-lux-vestra/oxide-batch/issues/202); the existing M6 in-memory/provider-neutral slice is not cloud-provider certification |
| M9 | HTTP pagination/streaming readers | No Spring parity row. This is an accepted OxideBatch-native network/service adapter family. Pagination-token checkpoint ownership, resource bounds, cancellation, and diagnostics are governed by the integration/repository contracts. | [#202](https://github.com/luceat-lux-vestra/oxide-batch/issues/202). Intentional omission from the Spring population; do not create a parity row unless a future Spring baseline adds a source capability that requires one. |
| M9 | idempotent webhook/external-effect writers | No Spring parity row. This is an accepted OxideBatch-native integration family over M8 durable-effect primitives. | [#202](https://github.com/luceat-lux-vestra/oxide-batch/issues/202), dependent on the M8 #201 delivery/effect model; no generic cross-resource exactly-once claim |
| M10 | multi-threaded item/step processing, local chunking, local partition scaling, split scaling, ordered commit/aggregation | `SCALE-PARSTEP-001`, `SCALE-MTSTEP-001`, `SCALE-LOCALCHUNK-001`, `SCALE-LOCALPART-001`; M7 semantics for `FLOW-SPLIT-001` remain a prerequisite rather than a competing scheduler | M10 tracker [#203](https://github.com/luceat-lux-vestra/oxide-batch/issues/203) |
| M10 | resource planning, bounded prefetch, graceful drain, spill policy, deterministic fallback, adaptive controls | No separate Spring row for the native resource-control mechanisms. Observable Spring-equivalent concurrency behavior remains in the `SCALE-*` rows. | Native execution/resource workstream [#203](https://github.com/luceat-lux-vestra/oxide-batch/issues/203); changes require workload/benchmark evidence and correctness equivalence |
| M10 | Arrow/Parquet or other high-throughput columnar formats | No Spring parity row. Accepted only as an evidence-gated native high-throughput format family; the roadmap does not claim present support. | Evaluation/performance owner [#203](https://github.com/luceat-lux-vestra/oxide-batch/issues/203); residual format/extension certification, if selected, is M13 [#207](https://github.com/luceat-lux-vestra/oxide-batch/issues/207) |
| M10 | concurrency telemetry sufficient to explain scaling/resource behavior | released base in `OBS-METRICS-001` | [#203](https://github.com/luceat-lux-vestra/oxide-batch/issues/203); later instrumentation does not retroactively broaden the existing `Verified` claim |
| M11 | remote step, partition, and chunk execution plus fault-injected harness | `SCALE-REMOTESTEP-001`, `SCALE-REMOTEPART-001`, `SCALE-REMOTECHUNK-001`, `TEST-DIST-001` | M11 tracker [#204](https://github.com/luceat-lux-vestra/oxide-batch/issues/204), hard-gated by [#205](https://github.com/luceat-lux-vestra/oxide-batch/issues/205) / RFC-0009 |
| M11 | worker registration/capabilities, durable assignment, lease/fencing/heartbeat, protocol/version negotiation, coordinator HA, artifact trust, cancellation/drain | No additional Spring row is needed for the native protocol/HA mechanism itself; the observable remote execution capability remains in the `SCALE-REMOTE*` rows. | [#204](https://github.com/luceat-lux-vestra/oxide-batch/issues/204); protocol decisions are owned by [#205](https://github.com/luceat-lux-vestra/oxide-batch/issues/205). RFC-0009 is still proposed, so this mapping is not implementation authorization. |
| M12 | complete Spring differential/parity closure | Every Spring ledger row; M12 must leave zero `Unknown`, `Deferred`, `Planned`, `Implemented`, `Partial`, or untested rows | M12 tracker [#206](https://github.com/luceat-lux-vestra/oxide-batch/issues/206) |
| M12 | neutral definition IR/extractor, mapping/difference reports, Rust stubs, one-way metadata import/export | `MIG-DEFINITION-001`, `MIG-METADATA-001` | [#206](https://github.com/luceat-lux-vestra/oxide-batch/issues/206), under RFC-0010 and the accepted migration contract |
| M13 | XML/Avro and other residual structured formats | residual portion of `IO-STRUCTURED-001` | M13 tracker [#207](https://github.com/luceat-lux-vestra/oxide-batch/issues/207) |
| M13 | additional/later-tier database and broker/integration certification | residual portions of `DB-ENTERPRISE-001`, `MSG-OTHER-001`; `IO-MAILLDAP-001` receives its decision here | [#207](https://github.com/luceat-lux-vestra/oxide-batch/issues/207), after earlier M8/M9 capability decisions |
| M13 | extension SDK/certification kit, deterministic replay/trace, residual effect-journal/savepoint/fork-lineage ecosystem surfaces | No new Spring row merely for the SDK/certification mechanism. Existing Spring-facing behavior stays with its semantic row; native extension surfaces are explicit roadmap extensions. | [#207](https://github.com/luceat-lux-vestra/oxide-batch/issues/207) |
| M13 | out-of-process/WASI or other isolation profiles | No Spring parity row. Registered in-process custom-step semantics are already represented by `STEP-CUSTOM-001`; isolation is a separate native security/extension capability. | [#207](https://github.com/luceat-lux-vestra/oxide-batch/issues/207), only after an explicit architecture/security decision. M11 worker isolation remains distinct. |
| M13 | selected columnar/adaptive/dynamic-partition capabilities left by earlier milestones | Reuse the relevant prior `SCALE-*` row when the observable Spring capability is the same; Arrow/Parquet and extension-only mechanics stay native-only unless a Spring source identity is established. | [#207](https://github.com/luceat-lux-vestra/oxide-batch/issues/207), consuming reviewed M10 evidence rather than reopening M10 by default |
| M14 | stable API/schema/manifest/protocol/support boundaries, N/N-1/N-2 compatibility, certification matrix, upgrade/rollback, project-wide RC/GA evidence | No release-governance row is created merely for GA. Existing rows such as `META-UPGRADE-001` and `META-RETENTION-001` carry their feature semantics; every advertised capability must independently have a terminal/released disposition. | M14 tracker [#208](https://github.com/luceat-lux-vestra/oxide-batch/issues/208) |

## Non-terminal row ownership

This matrix covers every current `Planned`, `Partial`, and actionable `Unknown`
row. “Primary owner” means the issue that must either deliver the remaining
semantics or record the reviewed terminal disposition. A secondary owner is
listed only when the existing row deliberately spans two milestones.

### Planned rows

| Row | Current milestone field | Remaining semantic owner |
| --- | --- | --- |
| `DOM-PARAM-002` | M7 | #199 |
| `LIFE-NORESTART-001` | M7 | #199 |
| `STEP-CUSTOM-001` | M7 | #195, subject to the exact #193/#194 M7 ownership freeze |
| `STEP-JOB-001` | M7 | #195 |
| `REPEAT-CONTEXT-001` | M7 | #197 |
| `FLOW-SPLIT-001` | M7/M10 | #195 owns M7 graph semantics; #203 owns the later local-scale/performance completion |
| `FLOW-NESTED-001` | M7 | #195 |
| `SCOPE-JOB-001` | M7 | #196 |
| `SCOPE-STEP-001` | M7 | #196 |
| `REPO-REGISTRY-001` | M7 | #198 |
| `TEST-DIST-001` | M11 | #204, blocked on #205/RFC-0009 |
| `SCALE-MTSTEP-001` | M10 | #203 |
| `SCALE-LOCALCHUNK-001` | M10 | #203 |
| `SCALE-REMOTEPART-001` | M11 | #204, blocked on #205/RFC-0009 |
| `SCALE-REMOTECHUNK-001` | M11 | #204, blocked on #205/RFC-0009 |
| `SCALE-REMOTESTEP-001` | M11 | #204, blocked on #205/RFC-0009 |
| `DB-MYSQL-001` | M8 | #201 |
| `DB-SQLITE-001` | M8 | #201 |
| `DB-SQLSERVER-001` | M8 | #201 |
| `DB-ENTERPRISE-001` | M8/M13 | #201 owns per-database M8 evaluation/disposition; #207 owns any residual later-tier ecosystem certification |
| `MSG-KAFKA-001` | M9 | #202 |
| `MSG-AMQP-001` | M9 | #202 |
| `MSG-OTHER-001` | M9/M13 | #202 owns shared M9 integration/delivery semantics and initial support disposition; #207 owns residual extension-tier certification |
| `MIG-DEFINITION-001` | M12 | #206 |
| `MIG-METADATA-001` | M12 | #206 |
| `META-RETENTION-001` | M8/M14 | #201 owns archive/export portability; #208 owns final GA compatibility/readiness carry-forward |

### Partial rows

| Row | Current milestone field | Remaining semantic/disposition owner |
| --- | --- | --- |
| `LIFE-STOP-001` | M1/M4/M7 | #199 for complete M7 operator-stop behavior |
| `LIFE-RECOVER-001` | M2/M4 | #206 final parity/disposition owner. No new M7-M11 implementation is inferred from `Partial`; the current stronger `UNKNOWN` recovery model remains accepted unless a reviewed gate changes it. |
| `STEP-STARTLIMIT-001` | M3/M7 | #199 |
| `FT-RETRY-001` | M3/M6 | #206 final parity/disposition owner; #151 already found the remaining difference to be M3 engine-internal rather than an M6 component gap |
| `FT-SKIP-001` | M3/M6 | #206 final parity/disposition owner; do not invent M7 work from the commit-boundary replay characteristic |
| `FT-ROLLBACK-001` | M3/M6 | #206 final parity/disposition owner; #151 found no open item/component implementation gap |
| `REPEAT-POLICY-001` | M6/M7 | #197 for the remaining flow-level repeat/interceptor composition |
| `LISTENER-ITEM-001` | M2/M3/M6 | #206 final parity/disposition owner; #151 found the callback taxonomy complete and the native differences intentional/evidenced |
| `FLOW-SEQUENCE-001` | M3/M7 | #195 |
| `FLOW-DECIDER-001` | M3/M7 | #195 |
| `REPO-COMMAND-001` | M2/M8 | #201 |
| `REPO-RETENTION-001` | M4/M8 | #201 |
| `SCALE-PARSTEP-001` | M4/M10 | #203 |
| `SCALE-LOCALPART-001` | M4/M10 | #203 |

### Unknown rows and decision gates

| Row | Current status | Decision owner and mandatory gate |
| --- | --- | --- |
| `DB-MONGO-001` | `Unknown` | Keep `Unknown` until M8 tracker #201's entry inventory reviews the pinned Spring 6.0.4 source semantics and the accepted repository model, then records one of a bounded implementation plan or reviewed `Unsupported`/`NotApplicable` disposition. No MongoDB implementation issue may be generated before that decision. |
| `IO-MAILLDAP-001` | `Unknown` | Keep `Unknown` until M13 tracker #207's entry inventory reviews the pinned Spring source population, Rust relevance, demand/support tier, and extension model, then records first-party/certified-third-party work or reviewed `Unsupported`/`NotApplicable` disposition. It may not be silently dropped. |

Keeping these rows `Unknown` is not a compatibility pass. The named gates make
the uncertainty owned and block decomposition that would otherwise assume a
support decision.

## Implemented rows with post-M6 tails

`Implemented` means code exists but released evidence/promotion is incomplete.
Most M6-only `Implemented` rows need no new product implementation from this
audit; M12 #206 still owns terminal ledger closure if they remain non-terminal.
The following rows additionally contain an explicit M7+ product/evidence tail:

| Row | Later tail | Owner |
| --- | --- | --- |
| `IO-STRUCTURED-001` | XML/Avro residual | #207 (M13) |
| `IO-DB-001` | upsert/stored-procedure/other backends/portable DB forms | #201 (M8) |
| `TEST-SCOPE-001` | evidence over the real M7 scoped lifecycle | #200, consuming #196 |
| `TEST-REPO-001` | adapter-portable repository test/certification coverage | #201 (M8) |
| `IO-OBJECT-001` | S3/Azure/GCS or other approved real-provider certification | #202 (M9) |
| `META-CONTEXT-001` | portable codec/migration capability beyond the delivered JSON baseline | #201 (M8), with M12 terminal ledger closure if still non-terminal |

The M6-only `ITEM-STREAM-001`, `ITEM-COMPOSITE-001`, `ITEM-DECORATOR-001`,
`ITEM-MULTI-001`, `IO-FLAT-001`, `TEST-JOB-001`, and `TEST-STEP-001` receive no
speculative later feature work from #189. If still non-terminal at M12, #206
must produce their final reviewed disposition/evidence.

## Released rows with later milestone extensions

A `Verified` row's named-release claim remains bounded to the evidence that
promoted it. The following future work therefore extends, but does not
reinterpret, an existing released base:

- M7: `DOM-JOB-001`, `DOM-EXIT-001`, `LIFE-RESTART-001`,
  `LIFE-ABANDON-001`, `LIFE-DEFINITION-001`, `STEP-TASKLET-001`,
  `REPO-EXPLORE-001`, `REPO-OPERATOR-001`, and `OPS-CLI-001` feed #193/#194
  and then their assigned M7 delivery owner.
- M8: `DB-POSTGRES-001` and `META-UPGRADE-001` feed #201 without implying
  that additional adapters already share PostgreSQL's certification.
- M10: `OBS-METRICS-001` feeds #203 for concurrency/resource instrumentation;
  its existing released telemetry contract stays verified only at its named
  boundary.
- M14: `META-UPGRADE-001` is re-exercised under #208's N/N-1/N-2 and GA
  compatibility obligations rather than being treated as proof that those
  future release combinations already pass.

## Explicit native-only omission decisions

These accepted surfaces intentionally receive no new Spring feature row in
this reconciliation:

1. **HTTP pagination/streaming** — M9 #202 native integration family.
2. **Webhook/external-effect writers** — M9 #202 native integration family,
   using M8 #201 durable-effect semantics.
3. **Arrow/Parquet** — M10 #203 evidence-gated high-throughput format family;
   M13 #207 owns any residual extension/certification form.
4. **Outbox/inbox/effect-journal mechanism** — M8 #201 native delivery
   primitives; broker/network adapters consume them rather than duplicating
   delivery semantics.
5. **Worker protocol, lease/fencing, coordinator HA, artifact trust** — M11
   #204 native distributed correctness mechanisms, hard-gated by #205 and
   RFC-0009.
6. **Extension SDK/certification machinery and deterministic trace/replay
   format** — M13 #207 native ecosystem mechanisms.
7. **Out-of-process/WASI isolation** — M13 #207 native extension/security
   capability after an explicit decision; it is not the same semantic identity
   as `STEP-CUSTOM-001`.
8. **Project-wide GA mechanics** — M14 #208 release/support governance, not a
   feature row.

The omission rule is deliberate: the Spring ledger remains sourced from the
pinned Spring population. If a later Spring baseline establishes an official
source identity for one of these observable capabilities, the normal baseline
update procedure adds or maps the row before its disposition is decided.

## Milestone activation consequences

- #189 does not authorize M7 product code.
- #193 must consume this record and freeze every M7-scoped row to exactly one
  delivery/evidence owner. Where this record names a split or umbrella-level
  owner, #193/#194 must resolve the exact intra-M7 assignment before #195-#199
  production implementation starts.
- #194 closes the semantic/design gates after #193.
- #195-#199 remain blocked until those gates close.
- #200 is the M7 conformance/restart/documentation exit gate.
- M8-M14 trackers must consume the mapping above at their entry gates rather
  than rediscovering capability families from roadmap prose.
- #236 remains a cross-milestone performance/scalability evidence umbrella;
  it is not a delivery owner and does not preempt #203 or #204.

## Acceptance checklist for #189

- [x] M7-M14 roadmap and canonical integration/architecture capability
  families are inventoried by semantic identity.
- [x] Every accepted family maps to a stable Spring ledger row or an explicit
  native-only disposition owner.
- [x] HTTP pagination/streaming, webhook/external effects, Arrow/Parquet, and
  extension/isolation surfaces have explicit dispositions without speculative
  implementation claims.
- [x] `DB-MONGO-001` and `IO-MAILLDAP-001` have named decision owners and gates
  while remaining visibly `Unknown`.
- [x] All 26 `Planned`, all 14 `Partial`, and both `Unknown` rows have an
  explicit future semantic/disposition owner.
- [x] M6 `Implemented` rows with later milestone tails are mapped without
  promoting them to `Verified`.
- [x] Existing released `Verified` rows with later milestone extensions retain
  their bounded named-release meaning.
- [x] The stale post-M6 `39 Planned` count is reconciled to the current
  83-row population without rewriting historical M5 evidence.
- [x] No M7-M14 product implementation is introduced by this record.
