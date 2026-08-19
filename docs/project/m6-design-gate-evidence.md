# M6 Complete Item Processing and User Test Kit Design-Gate Evidence

**State:** Complete on merge

**Issue:** [#142](https://github.com/luceat-lux-vestra/oxide-batch/issues/142)

**Date:** 2026-08-20

This record closes the design gates that the
[M6 kickoff gate](m6-kickoff-gate.md) names as prerequisites for dependent M6
implementation. It closes Gates A, C, D, E, F, and G in canonical
documentation. It freezes the executable scenario set, measurement protocol,
and acceptance criteria for Gates B and H so that a later issue can prove them
against real code, but it does not execute either campaign and does not close
either gate. This document implements no production capability: it changes no
file under `crates/oxide-batch/src`, creates no crate, and moves no durable
format. No compatibility-ledger row promotes because of this record.

RFC-0005 and ADR-0008 are not reopened. ADR-0008's migration order, its
preserved-from-ADR-0002 list, and its definition-identity invariant are the
binding decision this record freezes as the M6 production migration boundary
— it states no new architecture.

## Closed design gates

| Gate | Decision | Canonical evidence |
| --- | --- | --- |
| A — Item contract migration | The ADR-0008 migration order (contract and handles, generic `ChunkStep`, component/test port, ADR-0002 trait removal) is the exact M6 production migration boundary, with logical component identity, definition fingerprint, checkpoint, transaction, lifecycle, and restart selection held invariant | [Gate A](#gate-a--item-contract-migration) below and [ADR-0008](../architecture/decisions/0008-item-component-contract.md) |
| C — `ItemStream`/component state | Namespace, schema ID/version, codec ID/version, bounded size/depth, checksum-before-decode, migration, unknown-newer-version rejection, restartability declaration, and large-state handling are closed as the state contract in the [item-processing model](../architecture/item-processing-model.md) | [Gate C](#gate-c--itemstream--component-state) below |
| D — Standard component semantics | Every first-party component must document a fixed set of properties and ship declared contract plus executable evidence together; prose alone is not completion | [Gate D](#gate-d--standard-component-semantics) below |
| E — Composition semantics | A wrapper's advertised capability is the intersection of its delegates' capabilities, for ordering, transaction participation, restartability, checkpoint/state namespace, thread safety, error classification, and close ordering | [Gate E](#gate-e--composition-semantics) below |
| F — Item-listener allocation | M6 KEEPS the ADR-0002 boxed item-listener representation; no allocation-reducing listener type system is introduced in M6 | [Gate F](#gate-f--item-listener-allocation) below |
| G — `oxide-batch-test` boundary | The public test kit is a dedicated `oxide-batch-test` crate, not a facade module, on the strength of its independent dependency/resource boundary; the crate itself is not created by this issue | [Gate G](#gate-g--oxide-batch-test-boundary) below |

## Frozen evidence gates

| Gate | Status | Canonical protocol | Closure owner |
| --- | --- | --- | --- |
| B — Transaction/restart equivalence | Protocol FROZEN, gate OPEN | [Gate B](#gate-b--transactionrestart-equivalence-protocol) below | [#153](https://github.com/luceat-lux-vestra/oxide-batch/issues/153) |
| H — P-002 real-component performance | Protocol FROZEN, gate OPEN | [Gate H](#gate-h--p-002-real-component-performance-protocol) below | [#153](https://github.com/luceat-lux-vestra/oxide-batch/issues/153) |

Gate B and Gate H are not closed by this document. Their scenarios, matrix,
metrics, and acceptance criteria are fixed here so that #153 executes against
a decided protocol rather than an invented one; #153 is the only issue that
may record either gate as passed or failed.

## Gate A — Item contract migration

**Result: CLOSED.** No new architecture decision is made here — ADR-0008
already fixes the migration order, and this gate freezes that order as the
M6 production migration boundary rather than restating it as a fresh design.

The binding order, unchanged from ADR-0008:

1. publish the generic `ItemReader<I>`/`ItemProcessor<I, O>`/`ItemWriter<O>`
   contract, the sealed dyn-compatible mirror trait, and the
   `BoxedReader`/`BoxedProcessor`/`BoxedWriter` handles;
2. make `chunk_runtime::ChunkStep` generic over the contract, with the
   handles as the instantiation used by name-resolved plan components;
3. port existing components and their tests onto the contract;
4. remove `oxide_batch::{ItemReader, ItemProcessor, ItemWriter}` (the
   ADR-0002 form) in the same change that removes their last use.

The following invariants bind the migration and are not renegotiable by the
implementation issue:

- logical component identity is unchanged;
- the ADR-0004 definition fingerprint is unchanged;
- checkpoint semantics are unchanged;
- the borrowed enlisted transaction boundary is unchanged;
- lifecycle observations (trace, counters, terminal outcome) are unchanged;
- restart selection is unchanged;
- representation — typed contract, sealed mirror, or `Boxed*` handle — is not
  itself restart-relevant state;
- the migration does not produce two execution loops: the typed and erased
  pipelines are one generic chunk driver instantiated with different type
  arguments;
- erasure is a representation boundary at the point a handle is constructed,
  not a second execution semantics.

**Implementation owner:** [#143](https://github.com/luceat-lux-vestra/oxide-batch/issues/143).
No other M6 implementation issue may land before #143, because every
standard-component issue (#146-#150) is built directly on the contract this
gate freezes.

## Gate B — Transaction/restart equivalence protocol

**Result: PROTOCOL FROZEN, GATE OPEN.** No campaign runs against this
protocol in #142; #153 is the only issue that may record Gate B as passed or
failed.

The obligation is that the typed contract path and the `Boxed*` erased path
produce identical durable, observable outcomes on the same PostgreSQL
workload. Representation must not change what a workload commits, rolls back,
checkpoints, or replays.

### Scenario set

| ID | Scenario | Required equivalence |
| --- | --- | --- |
| B-01 | `normal_enlisted_commit_is_representation_identical` | Same business statements, business rows, checkpoint, component state, counters, repository writes, and normalized lifecycle trace on typed and `Boxed*` |
| B-02 | `writer_failure_before_commit_rolls_back_identically` | A typed writer failure before commit produces identical rollback of business writes, no checkpoint advancement, no component-state advancement, and no committed-counter advancement on both paths |
| B-03 | `state_checkpoint_counter_share_one_atomic_boundary` | Business effects, checkpoint, component state, counters, and optimistic version share one commit/rollback boundary in a same-resource enlisted transaction, identically on both paths |
| B-04 | `unknown_commit_outcome_forces_recovery_not_inference` | An untrusted post-COMMIT acknowledgement drives both paths into the UNKNOWN recovery path with no inferred success/rollback and no automatic replay before a fresh connection confirms durable state |
| B-05 | `process_kill_before_commit_restart_is_identical` | A separate worker process killed before commit restarts from the same authoritative durable checkpoint, with identical replay range, business-effect duplication (or its absence), counters, and state on both paths |
| B-06 | `process_kill_around_commit_acknowledgement_is_identical` | A kill forced between the commit boundary and its acknowledgement produces identical restart/recovery results on both paths |
| B-07 | `multi_chunk_restart_selects_identically` | After several committed chunks and a forced kill, restart-selected checkpoint, resumed input position, final business output, component state, counters, and lifecycle trace are identical on both paths |
| B-08 | `representation_does_not_change_definition_or_restart_identity` | Switching a component between typed and `Boxed*` representation changes no definition fingerprint, requires no restart-compatibility edge, and changes no restart-selection result |

### Execution matrix

Release-blocking PostgreSQL axes only: PostgreSQL 15 (oldest, release-blocking)
and PostgreSQL 18 (newest, release-blocking), per the existing
[support matrix](../release/support-matrix.md) policy. Majors 16 and 17 are
not expanded into a full Gate B campaign requirement.

### Acceptance criterion

Every scenario's durable observation is identical between the typed and
`Boxed*` paths. A representation-caused difference in any durable observation
is a Gate B FAIL; a difference caused by anything else is out of this gate's
scope and is a correctness defect on its own terms.

**Execution and closure owner:** [#153](https://github.com/luceat-lux-vestra/oxide-batch/issues/153).
At the point this document merges, Gate B remains OPEN with its protocol
frozen; no campaign has been run.

## Gate C — `ItemStream` / component state

**Result: CLOSED.** The canonical specification is
[`item-processing-model.md`](../architecture/item-processing-model.md),
which this record turns from a state-contract skeleton into a closed
contract.

### State identity

Component durable state carries a stable namespace, a schema ID, a schema
version, a codec ID, and a codec version. The namespace is a logical
identifier scoped under the owning component's logical identity — never a
display name, a runtime object identity, or a process-local pointer/object
address. Delegate and composite state namespaces must not collide across
children; a wrapper is responsible for keeping each delegate's namespace
distinct (see [Gate E](#gate-e--composition-semantics)).

### Bounds

M6 reuses the M5 context-envelope bounds unchanged rather than inventing a
parallel bound:

- default encoded size `64 KiB`, default JSON/structured depth `16`;
- hard ceiling encoded size `1 MiB`, hard ceiling depth `64`;
- schema identifier bound `128` bytes.

No new arbitrary bound is introduced by this gate.

### Corruption and checksum

- a checksum is verified before any decode or migration step runs;
- a checksum mismatch is a typed corruption failure, not a decode error to be
  papered over;
- corrupt state is never replaced with empty or default state;
- corruption never advances a checkpoint;
- diagnostics never expose the raw state value.

If #144 is the first issue to implement the durable checksum encoding, the
canonical format includes an algorithm identity and an algorithm version
alongside it, so that a future checksum algorithm change is a versioned
migration rather than a silent reinterpretation of existing bytes.

### Migration

- an equal recorded version decodes directly;
- an older recorded version applies one bounded, deterministic, directed
  migration;
- a newer recorded version fails closed;
- an unknown schema fails closed;
- an unknown codec fails closed;
- a migration failure is a known, not-committed outcome;
- migration never changes component identity;
- migration never changes definition identity.

### Restartability

A stateful component that disables persistence must declare the resulting
non-restartable limitation explicitly. A step containing a component whose
required state transitions cannot be reconstructed cannot claim to be
restartable.

### Large state

Oversized state is never inlined into metadata as a workaround. Large state
is handled only through a bounded external blob capability addressed by
content identity.

### Relationship to META-CONTEXT-001

`META-CONTEXT-001` is the one M5-advertised ledger row that stayed
`Implemented` rather than `Verified` at `0.5.0`, because it links an
architecture spike ([spike 0003](../architecture/spikes/0003-execution-context-evolution.md))
rather than codec migration tests. This gate names the remaining evidence gap
and its owner without promoting the ledger row and without a `Verified`
claim: `META-CONTEXT-001` promotes only when
[#144](https://github.com/luceat-lux-vestra/oxide-batch/issues/144) lands
state-migration and rejection-fixture evidence and a release links it,
following the ledger's own promotion rule.

**Implementation/evidence owner:** [#144](https://github.com/luceat-lux-vestra/oxide-batch/issues/144),
which implements state migration and rejection fixtures and updates the
ledger disposition alongside its own release evidence.

## Gate D — Standard component semantics

**Result: CLOSED.** The common contract template every first-party component
must satisfy is canonicalized in
[`item-processing-model.md`](../architecture/item-processing-model.md#standard-component-requirements).

Every first-party component documents at minimum:

- input type;
- output type;
- format and format version;
- state schema;
- checkpoint ownership;
- ordering semantics;
- restartability;
- thread-safety;
- reentrancy, where relevant;
- transaction capability;
- delivery capability;
- maximum/bounded resource behavior;
- buffering behavior;
- backpressure behavior;
- cancellation behavior;
- close behavior;
- sensitive-data classification;
- diagnostic/redaction behavior;
- malformed-input behavior;
- failure classification;
- support tier;
- required contract evidence;
- crash/restart evidence, where stateful;
- performance/resource evidence, where applicable.

A prose claim of "supported" with no matching evidence does not satisfy this
gate. A component pull request ships its declared contract and its
executable evidence in the same change; a contract without evidence, or
evidence without a declared contract, is incomplete.

**Dependent owners:** [#146](https://github.com/luceat-lux-vestra/oxide-batch/issues/146)
(common components/composites), [#147](https://github.com/luceat-lux-vestra/oxide-batch/issues/147)
(CSV/delimited/fixed-width), [#148](https://github.com/luceat-lux-vestra/oxide-batch/issues/148)
(JSON/JSONL), [#149](https://github.com/luceat-lux-vestra/oxide-batch/issues/149)
(PostgreSQL), [#150](https://github.com/luceat-lux-vestra/oxide-batch/issues/150)
(multi-resource/object-store basics).

## Gate E — Composition semantics

**Result: CLOSED.** "A wrapper must not claim a stronger capability than its
least-capable delegate" is fixed as the following concrete composition
semantics, canonicalized alongside Gate D.

**Capability meet rule.** A composite or wrapper's advertised capability is,
by default, the intersection (meet) of its delegates' capabilities — never
their union, and never a capability none of them has.

**Ordering.** If any required delegate is order-sensitive, the composite
stays order-sensitive. A wrapper must not claim to remove an ordering
requirement one of its delegates still has.

**Transaction participation.** A composite must not claim a stronger
transaction or delivery mode than every required delegate supports.
`WriteContext`'s enlisted transaction reborrows sequentially into each
delegate in turn; two delegates simultaneously holding the same
`&mut BusinessTransaction` is forbidden.

**Restartability.** If any required delegate cannot reconstruct its state,
the composite as a whole cannot claim restartability.

**Checkpoint/state.** A wrapper must not hide a delegate's state and must
not let two delegates' state namespaces collide. A wrapper documents its own
checkpoint ownership and its delegates' component-state namespaces.

**Thread safety.** A wrapper must not advertise a thread-safety capability
that every required delegate does not itself satisfy. A synchronization
wrapper that genuinely serializes access may separately advertise the
narrower capability its own serialization provides.

**Error classification.** A wrapper must not arbitrarily strengthen, weaken,
or hide a delegate failure's provenance/classification. A filter or
validator may convert an outcome to a different classification only when
that conversion is the component's explicit, documented semantic purpose.

**Close ordering.** The default deterministic rule is: close delegates in
the reverse of their successful open order. A close failure on one delegate
does not skip the close attempt on any other already-opened delegate, and a
close failure never erases an earlier primary runtime failure.

**Classifier-selected delegates.** A wrapper that selects among several
delegates at runtime must not infer a stronger static capability from the
one delegate a single run happened to select. The static declaration must
hold for every delegate the wrapper could select.

**Dependent owners:** [#146](https://github.com/luceat-lux-vestra/oxide-batch/issues/146),
[#150](https://github.com/luceat-lux-vestra/oxide-batch/issues/150).

## Gate F — Item-listener allocation

**Result: CLOSED — KEEP the ADR-0002 boxed representation for M6.** This is
an explicit decision, not a deferral by omission.

**Rationale.** ADR-0008 already excludes item listeners from the component
hot-path decision. The current listener set is heterogeneous, registration-
ordered, and pays one boxed future per registered listener per phase. The
zero-allocation claim applies to a listener-free typed pipeline; listener
allocation cost is not the same thing as component hot-path allocation and
the two must not be conflated. Monomorphizing listeners or introducing a
type-level heterogeneous list in M6 would change public ergonomics, add
implementation complexity, and open a separate architecture question outside
ADR-0008's accepted scope.

**Consequences for M6:**

- [#151](https://github.com/luceat-lux-vestra/oxide-batch/issues/151)
  completes listener taxonomy and ergonomics; it does not change the
  allocation representation;
- listener-enabled path cost is measured and reported separately under
  [Gate H](#gate-h--p-002-real-component-performance-protocol), never merged
  into the listener-free hard guarantee.

**Revisit triggers.** A future RFC/ADR is warranted if a real-component
P-002/listener measurement violates a binding budget, or if a Rust
language/object-safety change makes allocation-free heterogeneous listener
dispatch a realistic public API.

**Implementation owner:** [#151](https://github.com/luceat-lux-vestra/oxide-batch/issues/151).

## Gate G — `oxide-batch-test` boundary

**Result: CLOSED — dedicated `oxide-batch-test` public crate boundary.**

**Decision.** The user-facing test kit ships as a dedicated public crate,
`oxide-batch-test`, not as a module inside the `oxide-batch` facade.

**Rationale.** The test kit needs application-facing test APIs,
deterministic clock/ID sources, failure/panic/cooperative-stop injection,
repository fixtures, and a restart harness — a dependency and resource
boundary genuinely independent of the production runtime. This matches the
repository's own rule that a crate is created only when a real dependency
boundary exists, the same rule the
[staged crate-extraction contract](../architecture/crate-extraction.md)
already applies to `oxide-batch-core`, `oxide-batch-repository`, and
`oxide-batch-plan`.

**Boundary rules for `oxide-batch-test`:**

- it is a public package consumed by application test code;
- the production `oxide-batch` facade does not re-export it;
- the production path does not depend on it;
- it consumes `oxide-batch`'s public contracts, not private implementation
  types, even for test convenience;
- it does not leak SQLx/Tokio/database-driver concrete types in its public
  API;
- its MSRV matches the project line;
- in M6 it shares `oxide-batch`'s release line/version cadence and makes no
  independent stability promise;
- the no-placeholder-crate rule applies: it is not created until it ships a
  first usable utility with tests.

**This gate does not create the crate.** `crates/oxide-batch-test/` is
created only by [#145](https://github.com/luceat-lux-vestra/oxide-batch/issues/145),
alongside its first usable utility and tests, per the no-placeholder rule.
#145 also runs `cargo package` and relevant dry-run/package checks to verify
the publication cycle and package structure at creation time.

**Public test-kit target boundary (minimum):** full-job harness, single-step
harness, scoped-component harness, deterministic clock, deterministic ID
source, failure injection, panic injection, cooperative-stop injection,
restart harness, and repository fixture/cleanup support. The existing
internal test strategy's deterministic clock/ID, failure injection, and
process-restart principles carry over into the public kit rather than being
reinvented.

**Implementation owner:** [#145](https://github.com/luceat-lux-vestra/oxide-batch/issues/145).

## Gate H — P-002 real-component performance protocol

**Result: PROTOCOL FROZEN, GATE OPEN.** No benchmark is run against this
protocol in #142; #153 is the only issue that may record Gate H as passed or
failed. This section extends the
[performance and capacity plan](../engineering/performance-plan.md) with the
M6 P-002 campaign it names as an M6 obligation.

### Required comparison

The same logical pipeline runs in two forms with identical dataset,
component logic, chunk size, transaction semantics, delivery semantics,
state/checkpoint behavior, feature set, and compiler profile:

1. a fully typed component pipeline;
2. the same components wrapped as `BoxedReader`/`BoxedProcessor`/`BoxedWriter`
   erased pipeline.

### Primary listener-free campaign

Hard acceptance criterion, checked as an architecture invariant rather than
an average or a threshold:

**Framework-controlled per-item future allocation on the typed path == 0.**

### Listener-enabled companion measurement

To avoid hiding the [Gate F](#gate-f--item-listener-allocation) cost, a
listener-enabled variant is measured separately. Listener allocation is
never folded into the typed component allocation figure, the listener-free
hard guarantee is never weakened by this measurement, and the
listener-enabled cost is reported as its own distinct result.

### Required metrics

- allocations per item;
- allocations per chunk;
- bytes allocated/copied where measurable;
- throughput;
- item latency / relevant latency distribution;
- binary-size delta;
- compile-time delta.

### Environment metadata

Recorded and retained alongside raw evidence: source commit, Rust version,
LLVM version, compiler profile, OS/kernel, CPU/hardware, feature set,
dataset, chunk sizes, warmup protocol, repetition count, and variance —
following the [performance-plan measurement principles](../engineering/performance-plan.md#measurement-principles).

### No invented performance threshold

This document sets no numeric release gate — no "N% faster," no invented
latency or throughput target. The only hard pass/fail criteria are:

1. correctness/restart/durable observations are identical between paths
   (Gate B's own criterion, run separately);
2. the typed path's framework-controlled per-item future allocation is `0`.

Throughput, latency, code-size delta, and compile-time delta are M6
measurement and disclosure evidence, not invented binding budgets.

**Execution and closure owner:** [#153](https://github.com/luceat-lux-vestra/oxide-batch/issues/153).

## Impact classification

| Area | M6 #142 decision |
| --- | --- |
| Observable compatibility | Unchanged. #142 adds no node kind, manifest format, restart mode, schema table, CLI command, capability, or extension point. |
| Public API | No API changes land with #142. Gate A freezes the exact migration boundary #143 will implement; Gate G freezes the `oxide-batch-test` boundary #145 will create. |
| Restart and transactions | Unchanged. Gate A and Gate B both hold logical component identity, definition fingerprint, checkpoint semantics, and the borrowed enlisted-transaction boundary invariant; Gate B's equivalence obligation is frozen as protocol, not yet proved. |
| Durable data | No format moves in #142. Gate C closes the state contract's identity, bounds, corruption, and migration rules without implementing them; implementation is #144's. |
| Packaging | No crate is created. Gate G closes the `oxide-batch-test` boundary decision; the crate itself is #145's to create. |
| Composition | Gate E closes the capability-meet, ordering, transaction-participation, restartability, checkpoint/state, thread-safety, error-classification, and close-ordering rules that #146-#150 implement against. |
| Performance | Gate H freezes the P-002 protocol and its listener-free hard invariant without running it; Gate F closes the M6 listener-allocation decision as KEEP. No numeric threshold is set. |
| Ledger claims | No row promotes because of this document. `META-CONTEXT-001` stays `Implemented`, named as [#144](https://github.com/luceat-lux-vestra/oxide-batch/issues/144)'s to close. |
| Design philosophy | Existing Rust-native, bounded-resource, static-hot-path, explicit-effects, evidence-driven principles already present across accepted documents and `AGENTS.md` are consolidated into `docs/engineering/standards.md` as their canonical owner. No new architecture decision, product scope, or M6 gate is introduced by this consolidation. |

No decision here changes ADR-0008's accepted contract shape, reopens
RFC-0005, or authorizes any CSV/JSON/PostgreSQL component, `ItemStream`
runtime, or component-state persistence implementation.

## Named evidence scenarios

| Workstream | Scenario IDs required by the dependent issue |
| --- | --- |
| Gate A migration (#143) | `contract_and_handles_compile_and_type_check`, `chunk_step_is_generic_over_the_contract`, `ported_components_pass_unchanged_conformance`, `adr0002_traits_removed_with_last_use`, `zero_allocation_per_item_for_listener_free_typed_pipeline`, `trace_state_counter_checkpoint_unchanged_by_migration` |
| Gate B equivalence (#153) | `normal_enlisted_commit_is_representation_identical`, `writer_failure_before_commit_rolls_back_identically`, `state_checkpoint_counter_share_one_atomic_boundary`, `unknown_commit_outcome_forces_recovery_not_inference`, `process_kill_before_commit_restart_is_identical`, `process_kill_around_commit_acknowledgement_is_identical`, `multi_chunk_restart_selects_identically`, `representation_does_not_change_definition_or_restart_identity` |
| Gate C state contract (#144) | `checksum_verified_before_decode_or_migration`, `corrupt_state_never_advances_checkpoint`, `unknown_newer_schema_or_codec_fails_closed`, `older_version_applies_one_bounded_directed_migration`, `oversized_or_over_deep_state_is_a_known_not_committed_outcome`, `non_restartable_declaration_required_when_persistence_disabled`, `large_state_uses_bounded_external_blob_capability_only` |
| Gate D/E component and composition (#146-#150) | `declared_contract_and_evidence_ship_together`, `composite_capability_is_the_meet_of_delegate_capabilities`, `order_sensitive_delegate_keeps_composite_order_sensitive`, `no_two_delegates_hold_the_same_enlisted_transaction_concurrently`, `composite_restartability_requires_every_required_delegate_restartable`, `delegate_state_namespaces_do_not_collide`, `close_runs_in_reverse_open_order_and_does_not_skip_on_failure`, `classifier_static_capability_holds_for_every_selectable_delegate` |
| Gate F listener decision (#151) | `listener_free_pipeline_measures_zero_allocation`, `registered_listener_cost_is_reported_separately_from_component_cost` |
| Gate G test kit (#145) | `full_job_harness_launches_with_deterministic_clock_and_id`, `single_step_and_scoped_component_harness_construct_fixture_context`, `failure_panic_and_stop_injection_are_available_to_application_tests`, `restart_harness_resumes_from_the_last_committed_checkpoint`, `repository_fixture_cleans_up_isolated_metadata`, `package_dry_run_succeeds_for_oxide_batch_test` |
| Gate H performance (#153) | `typed_path_framework_controlled_per_item_allocation_is_zero`, `listener_enabled_allocation_is_reported_separately_from_typed_path`, `binary_size_and_compile_time_delta_recorded_for_both_forms`, `throughput_and_latency_recorded_without_an_invented_threshold` |

Required evidence classes mirror the M5 precedent: unit/property, PostgreSQL
integration, named conformance, crash/failure injection, migration, and
performance tests as indicated by each ledger profile. These scenario names
are acceptance targets fixed by this record, not evidence links, until the
tests exist and pass under their owning issue.

## Dependency handoff

- Issue [#143](https://github.com/luceat-lux-vestra/oxide-batch/issues/143)
  may begin the production migration under the Gate A boundary frozen above.
  No other M6 implementation issue lands first.
- Issue [#144](https://github.com/luceat-lux-vestra/oxide-batch/issues/144)
  may implement the Gate C state contract, including the checksum algorithm
  identity/version if it is the first issue to encode it, and updates
  `META-CONTEXT-001`'s disposition alongside its own release evidence.
- Issue [#145](https://github.com/luceat-lux-vestra/oxide-batch/issues/145)
  may create `crates/oxide-batch-test/` under the Gate G boundary, with its
  first usable utility and tests landing in the same change, and may proceed
  alongside #143 once the contract shape is fixed.
- Issues [#146](https://github.com/luceat-lux-vestra/oxide-batch/issues/146)
  through [#150](https://github.com/luceat-lux-vestra/oxide-batch/issues/150)
  follow #143 and implement components and composites against the Gate D and
  Gate E contracts frozen above.
- Issue [#151](https://github.com/luceat-lux-vestra/oxide-batch/issues/151)
  applies the Gate F KEEP decision to listener taxonomy/ergonomics without
  changing the allocation representation.
- Issue [#153](https://github.com/luceat-lux-vestra/oxide-batch/issues/153)
  is the sole owner that may execute the Gate B and Gate H protocols frozen
  above and record either gate as closed. It follows every other M6
  implementation stream and owns the M6 exit record and ledger
  reconciliation.

Any implementation need that changes an invariant fixed above — logical
component identity, definition fingerprint, checkpoint semantics, transaction
boundary, state-contract bound, composition rule, listener representation,
test-kit boundary, or a Gate B/H scenario/acceptance criterion — requires a
documentation correction and, where it changes an accepted contract, a
superseding RFC or ADR before dependent implementation continues.
