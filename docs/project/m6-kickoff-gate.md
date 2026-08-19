# M6 Complete Item Processing and User Test Kit Kickoff Gate

**State:** Active (2026-08-19)

**Umbrella:** GitHub issue
[#140](https://github.com/luceat-lux-vestra/oxide-batch/issues/140)

**Kickoff tracking:** GitHub issue
[#141](https://github.com/luceat-lux-vestra/oxide-batch/issues/141)

This record turns the accepted M6 roadmap outcome into definition-ready work.
M6 is active, but implementation may cross a named decision boundary only
after that boundary's gate below is closed. Unlike M5, M6 is not a
stabilization milestone over already-delivered scope — it completes the
Rust-native item/chunk/stream component model, standard local components, and
the application-facing test kit.

This gate does not implement anything. It authorizes tracked delivery work:
fixing governance documents that still described RFC-0005/ADR-0008 as
undecided, naming the gates that remaining M6 design questions must close
through, splitting M6 into dependency-ordered issues, and mapping every
M6-scoped compatibility-ledger row to exactly one owning issue.

## Satisfied prerequisites

- [x] M5 is complete and released as `oxide-batch` `0.5.0` (M5 Embedded Core
      Production Preview). The M5 gate PASSED; `28` of the `29` advertised
      embedded-kernel rows are `Verified` against the release; `META-CONTEXT-001`
      is the one advertised row that stays `Implemented` (it links an
      architecture spike rather than codec migration tests — Gate C below
      names its M6 evidence owner); PostgreSQL 18.4 external-consumer normal
      and kill/restart smoke passed; release integrity and crates.io checksum
      verification passed. Recorded in the
      [M5 exit evidence](m5-exit-evidence.md).
- [x] M6 does not re-prove any M5 evidence. The `v0.5.0` tag, its release
      commit, and every M5 evidence record are an immutable baseline.
- [x] [RFC-0005](../rfcs/0005-static-and-erased-components.md) — the
      static/erased item component architecture — is **accepted** (2026-08-03,
      on the evidence of
      [spike 0004](../architecture/spikes/0004-static-and-erased-item-path.md))
      and its decision is recorded as
      [ADR-0008](../architecture/decisions/0008-item-component-contract.md).
      M6 kickoff does not redesign that architecture; it is a prerequisite M6
      implements. ADR-0008 supersedes
      [ADR-0002](../architecture/decisions/0002-execution-model.md) **in
      part** — the item reader/processor/writer public representation only.
      ADR-0002 remains the accepted record for the execution model and every
      other extension point, including item listeners.
- [x] The accepted contract shape (one public generic trait per item
      component role; explicit `Boxed*` erasure handles over a sealed,
      private, dyn-compatible mirror trait; one generic chunk loop shared by
      the typed and erased pipelines; no per-item future allocation on the
      typed path for a pipeline with no item listeners; unaffected logical
      component identity, definition fingerprint, checkpoint semantics,
      transaction ports, and restart selection) is settled and is not
      reopened by this gate.
- [x] The production migration boundary this gate authorizes as the first
      implementation issue has not yet happened:
      `crates/oxide-batch/src/chunk.rs` still carries the ADR-0002-era boxed
      `ItemReader<I>`, `ItemProcessor<I,O>`, `ItemWriter<I>`; the accepted
      ADR-0008 shape exists only in `spikes/m6-item-hot-path/`.

## Impact classification

M6 adds new public item-component contracts and a standard component
catalog, rather than stabilizing existing scope the way M5 did. It changes
public API (the item reader/processor/writer traits move to the ADR-0008
shape and the ADR-0002 forms are removed), adds new item-listener,
`ItemStream`, and component-state contracts, and adds a new
first-party-component catalog and test-kit boundary. It does not change
job/step lifecycle, launch/restart/recover semantics at the job or step
level, the repository capability model, or the definition-fingerprint input
set — those stay owned by their existing accepted decisions (ADR-0004,
ADR-0005, ADR-0006, ADR-0009) and are not reopened here.

What is closed by this kickoff record: the architecture decision
(RFC-0005/ADR-0008) and the delivery-issue decomposition below. What remains
open, and is therefore gated rather than assumed, is named by Gates A-H. Gates
A/C/D/E/F/G are design decisions that must close before their dependent
implementation lands. Gates B/H are evidence gates: #142 freezes their
scenarios, protocol, and acceptance criteria, but they close only after #153
executes the transaction/restart-equivalence and P-002 campaigns successfully.

## Accepted architecture

[ADR-0008](../architecture/decisions/0008-item-component-contract.md) is the
binding decision for the item component contract. Its accepted shape:

- one public generic contract per item component role — `ItemReader<I>`,
  `ItemProcessor<I, O>`, `ItemWriter<I>` — each with an explicit call
  lifetime and an opaque `impl Future<Output = ..> + Send + 'a` return;
  implementors write a plain `async fn`;
- erasure is a concrete handle (`BoxedReader<I>`, `BoxedProcessor<I, O>`,
  `BoxedWriter<I>`), not a second public trait, over a sealed, private,
  dyn-compatible mirror that cannot be named, implemented, or depended on
  outside the crate;
- one generic chunk driver: the typed and dynamically dispatched pipelines
  are the same function with different type arguments, not two execution
  paths;
- no per-item future allocation on the typed path for a pipeline with no
  item listeners — item listeners are explicitly outside this decision and
  keep the ADR-0002 boxed form (reducing their per-item allocation is
  separate work with its own evidence, tracked as Gate F);
- representation is not restart-relevant: moving a component between the
  boxed form and the contract, or from a concrete type to a handle, does not
  change logical component identity, revision, state schema, checkpoint
  semantics, transaction ports, or the ADR-0004 definition fingerprint.

This decision is not reopened by M6 kickoff or by any gate below. What
remains is implementing it in production code and completing the item-model
scope ADR-0008 explicitly left outside its own decision (item listeners,
`ItemStream`, the standard component catalog, and the test kit).

## Decisions/evidence required before dependent implementation

| Gate | Owner | Required decision and evidence | Blocks |
| --- | --- | --- | --- |
| A — Item contract migration | Core/runtime owners | Exact production migration boundary: publish the ADR-0008 contract, sealed mirror, and `Boxed*` handles; make `chunk_runtime::ChunkStep` generic over the contract; port existing components/tests; remove the ADR-0002 item traits in the same change that removes their last use — with logical component identity, definition fingerprint, checkpoint, transaction, lifecycle, and restart selection held invariant | Any standard-component catalog work |
| B — Transaction/restart equivalence | Repository/runtime owners | Freeze named PostgreSQL-fixture scenarios and acceptance criteria in #142; #153 must then prove the typed and `Boxed*` paths semantically identical for enlisted transaction behavior, statement participation, checkpoint, state, counters, rollback, unknown commit outcome, process kill, and restart | M6 exit; closes only in #153 |
| C — `ItemStream`/component state | Core/repository owners | Namespace, schema ID/version, codec ID/version, bounded size/depth, checksum/corruption handling, sensitivity, migration, unknown-newer-version rejection, and restartability-declaration contract; explicit link to `META-CONTEXT-001`'s remaining evidence gap, with a named owner rather than an assumed promotion | Any component that persists state beyond a checkpoint position |
| D — Standard component semantics | Core owners | The documentation/evidence contract every first-party component must satisfy: input/output type, format/version, state schema, checkpoint ownership, ordering, restart, thread safety, transaction/delivery capability, resource bounds, cancellation, close behavior, sensitive-diagnostic handling, malformed-input behavior, and support tier | Every format-specific component issue (CSV, JSON, PostgreSQL, multi-resource) |
| E — Composition semantics | Core owners | Common rules for composite/delegate/classifier/validator/filter/peek/aggregate/multi-resource/thread-safety wrappers: a wrapper MUST NOT claim a stronger capability than its least-capable delegate, for ordering, checkpoint ownership, transaction participation, error classification, thread safety, restartability, and close ordering | Standard processors/composites, multi-resource issues |
| F — Item listener allocation | API/performance owners | A measured decision — keep the current boxed per-item-per-phase allocation, adopt an allocation-reducing structure, or explicitly defer to a later milestone — with evidence, not assumed by measuring nothing | Fault/listener ergonomics issue |
| G — `oxide-batch-test` boundary | Core/testing owners | The public test-kit surface: full-job, single-step, scoped-component, restart, deterministic-clock/ID, and failure/panic/stop-injection utilities; the crate's independent dependency/support boundary decided before it is created; no production internal exposed for test convenience alone; no placeholder crate | Test-kit foundation issue |
| H — Performance evidence | Performance owners | Freeze the P-002 real-component workload, measurement protocol, and acceptance criteria in #142; #153 must then measure allocations/item, allocations/chunk, throughput, latency, binary-size delta, compile-time delta, with the item-listener caveat from Gate F stated rather than hidden | M6 exit; closes only in #153 |

Issue
[#141](https://github.com/luceat-lux-vestra/oxide-batch/issues/141) records
this table and the delivery order. Issue
[#142](https://github.com/luceat-lux-vestra/oxide-batch/issues/142) closes the
design decisions in Gates A/C/D/E/F/G and freezes the executable protocols and
acceptance criteria for evidence Gates B/H before dependent implementation
lands. Gates B/H remain open until issue
[#153](https://github.com/luceat-lux-vestra/oxide-batch/issues/153) executes
those campaigns successfully. Any change to an accepted contract still
requires a superseding RFC or ADR before dependent implementation.

## Delivery workstreams and order

1. [#142](https://github.com/luceat-lux-vestra/oxide-batch/issues/142) — close
   design Gates A/C/D/E/F/G and freeze the Gate B/H evidence scenarios,
   protocols, and acceptance criteria, mirroring the M5 design-before-delivery
   discipline without pretending post-implementation evidence already exists.
2. [#143](https://github.com/luceat-lux-vestra/oxide-batch/issues/143) —
   item contract and chunk hot-path migration. This is the **first M6
   implementation issue**: it lands before any standard-component catalog
   work, because CSV/JSON/PostgreSQL readers and writers all have to be
   built on this component contract, and building them on the legacy boxed
   API first would require migrating the whole catalog a second time.
3. [#144](https://github.com/luceat-lux-vestra/oxide-batch/issues/144) —
   `ItemStream`/checkpoint/component-state contract, the state-migration and
   restart foundation every stateful component needs.
4. [#145](https://github.com/luceat-lux-vestra/oxide-batch/issues/145) —
   user-facing component and restart test-kit foundation. Designed to
   proceed alongside #143 once the contract shape is fixed, so every later
   component issue reuses one test harness instead of ad hoc fixtures.
5. [#146](https://github.com/luceat-lux-vestra/oxide-batch/issues/146) —
   standard processors, delegates, classifiers, and composites (the common
   composition catalog other than format-specific adapters).
6. [#147](https://github.com/luceat-lux-vestra/oxide-batch/issues/147) —
   restartable delimited/CSV and fixed-width components, including
   file-offset checkpoint and malformed-record semantics.
7. [#148](https://github.com/luceat-lux-vestra/oxide-batch/issues/148) —
   restartable JSON and JSONL components, prioritizing streaming and bounded
   memory over whole-file materialization.
8. [#149](https://github.com/luceat-lux-vestra/oxide-batch/issues/149) —
   PostgreSQL cursor, paging, and SQL batch components. M6 implements
   PostgreSQL capability only; a generic multi-database abstraction is M8
   repository-portability scope.
9. [#150](https://github.com/luceat-lux-vestra/oxide-batch/issues/150) —
   multi-resource components, object-store basics, and any remaining
   advanced local composition.
10. [#151](https://github.com/luceat-lux-vestra/oxide-batch/issues/151) —
    item-level fault and listener ergonomics over the existing M3
    fault-tolerance engine, applying the Gate F decision.
11. [#152](https://github.com/luceat-lux-vestra/oxide-batch/issues/152) —
    item pipeline configuration ergonomics usable over both the typed and
    `Boxed*` dynamic pipeline.
12. [#153](https://github.com/luceat-lux-vestra/oxide-batch/issues/153) —
    execute Gates B/H, complete conformance/performance evidence,
    documentation, ledger reconciliation, and the M6 exit gate. The final M6
    issue, following every implementation stream.

The approximate dependency graph:

```text
Kickoff (#141)
  ↓
Design closure + evidence protocol freeze (#142)
  ↓
Item contract + chunk hot-path migration (#143)
  ├── ItemStream/state (#144)
  └── Test-kit foundation (#145)
        ↓
Core components/composites (#146)
        ├── CSV/fixed-width (#147)
        ├── JSON/JSONL (#148)
        ├── PostgreSQL reader/writer (#149)
        └── multi-resource/wrappers (#150)
                ↓
      fault/listener ergonomics (#151)
      configuration ergonomics (#152)
                ↓
  execute B/H + conformance/docs/exit (#153)
```

Independent workstreams may parallelize, but no component-catalog issue
(#146-#150) may land before the contract migration (#143), and the exit
issue (#153) follows every implementation stream.

## Compatibility ledger ownership

Every M6-scoped row in the
[conformance matrix](../compatibility/conformance-matrix.md) is mapped below
to exactly one owning delivery issue. No row's status changes in this
record; a row promotes only against a named released version with its
required evidence, per the ledger's own promotion rule.

| Ledger row | Current status | Owning issue |
| --- | --- | --- |
| `STEP-CHUNK-001` | Verified (full component model M6) | [#143](https://github.com/luceat-lux-vestra/oxide-batch/issues/143) |
| `ITEM-READER-001` | Verified (full contract M6) | [#143](https://github.com/luceat-lux-vestra/oxide-batch/issues/143) |
| `ITEM-PROCESSOR-001` | Verified (native hot path M6) | [#143](https://github.com/luceat-lux-vestra/oxide-batch/issues/143) |
| `ITEM-WRITER-001` | Verified | [#143](https://github.com/luceat-lux-vestra/oxide-batch/issues/143) |
| `ITEM-STREAM-001` | Planned | [#144](https://github.com/luceat-lux-vestra/oxide-batch/issues/144) |
| `META-CONTEXT-001` | Implemented (M5's one non-`Verified` advertised row) | [#144](https://github.com/luceat-lux-vestra/oxide-batch/issues/144) |
| `TEST-JOB-001` | Planned | [#145](https://github.com/luceat-lux-vestra/oxide-batch/issues/145) |
| `TEST-STEP-001` | Planned | [#145](https://github.com/luceat-lux-vestra/oxide-batch/issues/145) |
| `TEST-SCOPE-001` | Planned | [#145](https://github.com/luceat-lux-vestra/oxide-batch/issues/145) |
| `TEST-REPO-001` | Planned | [#145](https://github.com/luceat-lux-vestra/oxide-batch/issues/145) |
| `ITEM-COMPOSITE-001` | Planned | [#146](https://github.com/luceat-lux-vestra/oxide-batch/issues/146) |
| `ITEM-DECORATOR-001` | Planned | [#146](https://github.com/luceat-lux-vestra/oxide-batch/issues/146) |
| `IO-FLAT-001` | Planned | [#147](https://github.com/luceat-lux-vestra/oxide-batch/issues/147) |
| `IO-STRUCTURED-001` | Planned (M6 slice: JSON/JSONL only; XML/Avro stay M13) | [#148](https://github.com/luceat-lux-vestra/oxide-batch/issues/148) |
| `IO-DB-001` | Planned (M6 slice: PostgreSQL only; other adapters M8) | [#149](https://github.com/luceat-lux-vestra/oxide-batch/issues/149) |
| `ITEM-MULTI-001` | Planned | [#150](https://github.com/luceat-lux-vestra/oxide-batch/issues/150) |
| `IO-OBJECT-001` | Planned (M6 slice: basics only; certification M9) | [#150](https://github.com/luceat-lux-vestra/oxide-batch/issues/150) |
| `FT-RETRY-001` | Partial | [#151](https://github.com/luceat-lux-vestra/oxide-batch/issues/151) |
| `FT-BACKOFF-001` | Verified | [#151](https://github.com/luceat-lux-vestra/oxide-batch/issues/151) |
| `FT-SKIP-001` | Partial | [#151](https://github.com/luceat-lux-vestra/oxide-batch/issues/151) |
| `FT-ROLLBACK-001` | Partial | [#151](https://github.com/luceat-lux-vestra/oxide-batch/issues/151) |
| `REPEAT-POLICY-001` | Planned | [#151](https://github.com/luceat-lux-vestra/oxide-batch/issues/151) |
| `LISTENER-ITEM-001` | Partial | [#151](https://github.com/luceat-lux-vestra/oxide-batch/issues/151) |

`FT-BACKOFF-001` is already `Verified` against `0.5.0`; its M6 owner tracks
the item-facing ergonomics work still named against it, not a re-verification
of the M3 evidence. No other row above changes status because of this table.

## Definition of done

M6 kickoff closes only when:

- the M6 umbrella
  ([#140](https://github.com/luceat-lux-vestra/oxide-batch/issues/140)) and
  kickoff-tracking
  ([#141](https://github.com/luceat-lux-vestra/oxide-batch/issues/141))
  issues exist, and the umbrella links every delivery issue in dependency
  order;
- RFC-0005, ADR-0008, and `item-processing-model.md` agree on what is
  accepted (the item component contract shape) and what remains open (the
  rest of the item-processing scope), with no document reading RFC-0005 as
  still `Proposed`;
- the roadmap, root status, and documentation index name M6 as active
  without claiming any M6 capability is implemented or released `Verified`;
- Gates A/C/D/E/F/G each have a named owner, required decision, and named
  dependent issue; Gates B/H each have a named owner, frozen executable
  protocol and acceptance criteria, and explicit closure owner #153;
- every M6-scoped conformance-matrix row has exactly one owning delivery
  issue, recorded in the table above;
- the first implementation issue is the item-contract/chunk-runtime
  migration ([#143](https://github.com/luceat-lux-vestra/oxide-batch/issues/143)),
  ordered before every standard-component catalog issue;
- no CSV/JSON/PostgreSQL reader/writer implementation, and no item hot-path
  runtime migration, has landed in this kickoff record — both are scoped to
  their own delivery issues;
- `v0.5.0` and every M5 evidence record are unchanged.

This is the kickoff's own definition of done, not M6's. M6 itself closes
against the exit criteria recorded in
[#140](https://github.com/luceat-lux-vestra/oxide-batch/issues/140) and the
M6 exit record that
[#153](https://github.com/luceat-lux-vestra/oxide-batch/issues/153) produces.

## Scope controls

M6 does not include advanced nested/split/job flow and the definition
registry (M7), repository backends or Tier-1 databases beyond PostgreSQL
(M8), broker/messaging integrations (M9), high-performance or
multi-threaded local execution (M10), remote/distributed execution (M11), or
complete Spring Batch ledger closure (M12). No new crate, feature flag,
manifest field, schema table, CLI command, or extension point is added
merely to reserve later scope. `v0.5.0`, its release tag, and every M5
evidence record remain an immutable baseline that no M6 gate or issue
reopens.