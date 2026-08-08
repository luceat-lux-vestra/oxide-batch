# M5 Context-Codec and Transaction-Capability Evidence

**State:** Complete for the codec lifecycle and the launch-time repository
capability negotiation portion of issue #100. The migration and restore portion
was reassigned rather than delivered here.

**Issue #100 is closed.** It opened with two criteria this work could not
satisfy — migration from every supported prior version, and retained
reproducible PostgreSQL migration and restore evidence — and those are now
formally moved to issue
[#102](https://github.com/luceat-lux-vestra/oxide-batch/issues/102) in both
issue bodies, which is the condition this record set for closing #100. See
[Migration and rollback](#migration-and-rollback) for why this work owes no new
migration of its own, which is a different statement from those criteria being
met.

**Issue:** [#100](https://github.com/luceat-lux-vestra/oxide-batch/issues/100)

**Date:** 2026-08-06

This record is the evidence for the fourth M5 workstream: applying the accepted
[M5 codec and capability direction](../architecture/repository-and-transaction-model.md#m5-context-codec-and-transaction-capability-direction)
that the [design-gate evidence](m5-design-gate-evidence.md) closed. It covers
the two gaps the delivered code had against that direction, the two public API
changes closing them required, the eight named scenarios, and the boundaries
this work deliberately did not cross.

The direction stabilizes direction only. **No durable format moves.** The
framework envelope stays at format version `1` with the same members, `encode`
produces the same bytes, and formats 1, 2, and 3 keep their canonical manifests
and golden fingerprint vectors.

## What the delivered code was missing

The gate closed on a direction the code satisfied in part. Two clauses had no
implementation behind them.

**The codec declared no upgrades and the framework applied no chain.** The
direction requires that "an application codec declares its current version and
the directed upgrades it can apply" and that decoding "applies one bounded,
deterministic upgrade chain." `VersionedStateCodec::decode` instead received the
recorded version and left each codec to decide what an older payload meant. The
trait's own documentation said so: *decode must explicitly handle every older
supported schema version*. Nothing held that per-codec logic to being finite,
single-valued, or forward-moving, and nothing stopped a codec from reading a
version-1 payload as though it were version 3 minus some fields — which is
exactly the truncation the direction forbids.

**Capabilities were rejected but never declared.** The direction requires an
adapter to declare its capabilities in a versioned descriptor and a requirement
it does not declare to fail "at compilation or launch negotiation." There was no
descriptor. `RepositoryError::UnsupportedCapability` existed, but it came out of
a `RepositoryUnitOfWork` default method body, so it fired when the runtime
reached the call. A deployment that could not create durable step partitions
accepted a partitioned plan, wrote job and step lifecycle rows, and failed at
the first partition write. The rejection was typed; it was not negotiation, and
it was not before any durable write.

A first attempt at this closed only half the gap by deriving requirements from
the compiled plan alone. That misses `ExecutionOwnership`, which no plan
mentions because it is enabled by the launcher rather than declared by a
definition — the same definition needs it or does not depending on how it is
launched.

## The codec schema lifecycle

`StateSchemaUpgrade` is one declared directed edge with a deterministic payload
transform. Three constraints make a resolved chain bounded and single-valued,
and each is enforced rather than documented:

| Constraint | Enforced by | Failure |
| --- | --- | --- |
| An edge strictly increases the version | `StateSchemaUpgrade::new` | `StateError::NonIncreasingUpgrade` |
| At most one edge leaves any version | chain resolution | `StateError::AmbiguousUpgrade` |
| No edge passes the current version | chain resolution | `StateError::UpgradeOvershootsCurrent` |
| A recorded version must reach the current one | chain resolution | `StateError::NoUpgradePath` |
| The chain is finite | `MAX_UPGRADE_CHAIN` | `StateError::UpgradeChainTooLong` |

Because each edge strictly increases the version and at most one leaves any
version, a chain cannot loop and cannot exceed the declared edge count; the
explicit ceiling bounds a codec that declares an unreasonable number of edges.

`decode` now receives a payload already at the current version and loses its
version argument. That is the point rather than a side effect: a codec that
cannot see the recorded version cannot reinterpret it.

**Upgrade output is checked before it reaches the codec.** A transform is
application code running between two framework checks, so its result is held to
the envelope's JSON-object shape and to the durable hard ceilings — `1 MiB` and
depth `64`. The *configured* limits deliberately do not apply there. An
intermediate is never persisted; the bytes that are persisted come from
`encode`, which is already checked against the configured limits. Retaining the
configured limits on the envelope to check intermediates against them would have
grown `Checkpoint`, `ExecutionContext`, and therefore every `ChunkCommitReceipt`
on the chunk commit path, for no durable benefit.

## The capability descriptor

`RepositoryDescriptor` is the adapter's own claim about the deployment it is
connected to. It versions its own shape (`descriptor_version`) separately from
the durable metadata schema (`schema_version`), because a runtime can understand
a descriptor whose schema it refuses — the two answer different questions and a
single version number would conflate them.

Negotiation runs at flow launch, before any durable work:

- the launch's requirements are derived from the compiled plan and from the
  launcher configuration;
- each is required against the descriptor;
- an undeclared requirement is `FlowRuntimeError::UndeclaredCapability`, naming
  the capability and the descriptor version.

**A launch requirement has two sources, and both are negotiated.**

| Source | Requirement | Why it is not the other source |
| --- | --- | --- |
| The compiled plan | `StepPartitions`, from a partitioned step node | A definition declares it |
| The launcher configuration | `ExecutionOwnership`, from `FlowLauncher::with_execution_control` | No plan mentions it; the same definition needs it or not depending on how it is launched |

Reading only the plan would have missed `ExecutionOwnership` entirely. The two
sets are unioned into a `BTreeSet`, so a capability required by both is
negotiated once and the rejection order is deterministic.

Capabilities that only an operator action needs — stop requests, retention
purge, instance holds — stay negotiated where that action is applied, because a
plan that is never stopped or purged does not need the deployment to support
stopping or purging, and rejecting such a launch would be a false negative.

**The `descriptor` default declares nothing.** An adapter that has not been
reviewed against a capability is negotiated as not providing it. Failing closed
costs a rejected launch; failing open would cost a silently weaker guarantee,
which is the outcome the direction exists to prevent. Both delivered adapters
override it and declare the six capabilities they implement. The wrapping test
doubles delegate, because narrowing a connection budget says nothing about what
a deployment can do.

## Fingerprint participation

The direction names the durable-meaning capabilities: the declared transaction
and delivery mode, the checkpoint and context codec versions, and the enlistment
class. The first three were already manifest members. **The enlistment class
reaches the fingerprint through the declared delivery mode**, which already
carries it: `ChunkDeliveryMode::AtomicSameResource` *is* the declaration that
business writes and progress share one resource transaction, and dropping to
`AtLeastOnce` is the enlistment change.

No manifest member is added. That is required, not merely convenient: the
milestone's impact classification fixes that formats 1, 2, and 3 keep their bytes
and golden vectors, and
[ADR-0009](../architecture/decisions/0009-definition-fingerprint-input-set.md)
re-pinned the format-2 and format-3 vectors exactly once. A new member would
have re-pinned them a second time and turned every existing definition into
fail-closed drift.

Throughput settings do not participate, and the evidence for that is structural
rather than a passing assertion: pool size, connection capacity, and statement
timeout are adapter configuration that is not part of a definition at all, is
absent from the descriptor's capability set, and is absent from the canonical
manifest. There is no path by which they could reach a fingerprint.

## Named scenarios

All eight scenarios the design gate named for this workstream are delivered and
pass.

| Scenario | Location |
| --- | --- |
| `older_recorded_schema_version_upgrades_through_one_directed_chain` | [durable_state_codec.rs](../../crates/oxide-batch/tests/durable_state_codec.rs) |
| `newer_recorded_schema_version_is_rejected` | same |
| `oversized_or_over_deep_payload_is_a_known_not_committed_outcome` | same |
| `corrupt_payload_never_advances_a_checkpoint` | same |
| `undeclared_capability_requirement_is_rejected_with_a_typed_error` | [repository_capabilities.rs](../../crates/oxide-batch/tests/repository_capabilities.rs), via a real `FlowLauncher::launch` |
| `borrowed_transaction_preserves_atomic_checkpoint_and_unknown_outcome` | same |
| `durable_meaning_capability_change_changes_the_fingerprint` | same |
| `throughput_capability_change_does_not_change_the_fingerprint` | same |

Three supporting tests sit alongside them in the same file:
`undeclared_execution_ownership_is_rejected_before_any_repository_transaction`
(the launcher-carried requirement),
`declared_capabilities_pass_negotiation_and_reach_the_repository` (the positive
control for both sources), and `descriptor_declares_and_requires_each_capability`
(the descriptor unit test).

### Rejection before any durable write

`undeclared_capability_requirement_is_rejected_with_a_typed_error` and
`undeclared_execution_ownership_is_rejected_before_any_repository_transaction`
call the real `FlowLauncher::launch` rather than `RepositoryDescriptor::require`
directly. A descriptor-level assertion proves the descriptor works; it proves
nothing about *when* a launch consults it, which is the whole claim.

Both assert the typed `FlowRuntimeError::UndeclaredCapability` **and**
`repository.begin_count() == 0`, through a counting repository that wraps the
reference adapter. The counter is the load-bearing assertion: checking that no
metadata was stored would only show that nothing was committed, while checking
that `begin` was never called shows the launch was rejected before it opened a
repository transaction at all — so no instance, execution, or lifecycle row can
exist.

`descriptor_declares_and_requires_each_capability` remains as a descriptor unit
test. It is not the evidence for rejection ordering and is not cited as such.

`declared_capabilities_pass_negotiation_and_reach_the_repository` is the
positive control for both sources: the same partitioned plan and the same
execution-controlled launch complete when the descriptor declares what they
need.

The `ExecutionOwnership` case is worth stating precisely, because a mutation
check found it is stronger than it first appears. The in-memory adapter beneath
the test double *does* implement ownership claims. With the launcher requirement
removed, the job therefore runs to completion under a descriptor that never
promised ownership — it does not fail late inside `claim_execution_owner`, it
does not fail at all. The descriptor is the deployment's contract, and honouring
it cannot depend on what the adapter happens to implement.

### The borrowed adapter-owned transaction

`borrowed_transaction_preserves_atomic_checkpoint_and_unknown_outcome` drives a
real chunk through `ChunkStep::execute`. The runtime, not the test, calls
`business_transaction()` and builds the `WriteContext` the writer receives, so
the path exercised is the one production uses:

- **the adapter owns the concrete transaction.** One value implements both
  `ChunkTransaction` and `BusinessTransaction`; it decides when work is
  published and when it is discarded.
- **the writer is lent only a bounded port.** It receives
  `&mut dyn BusinessTransaction` for the duration of its write call. It holds
  no handle to the resource, so a business row cannot reach the resource by any
  route except that port — which is what makes the state assertions evidence
  rather than coincidence.
- **business writes are staged only through the port.** Each statement binds
  its value separately and is recorded by the port itself, together with a
  snapshot showing nothing committed while the transaction is open.
- **writes and checkpoint publish at one boundary.** On commit the staged rows
  and the checkpoint of that same commit become visible together, and every
  published row carries the checkpoint the receipt reports.
- **an ambiguous commit stays `UNKNOWN`.** The runtime reports
  `ChunkExecutionOutcome::Unknown` and returns no receipt. The test does not
  read the fixture's contents and conclude the transaction did not commit:
  under `UNKNOWN` the outcome is unknown until a healthy connection reads
  durable state, and in-memory fixture contents are not that state. A known
  rollback is asserted separately as the distinct, typed `NotCommitted` case.

The scenario is held to detecting five specific regressions, each confirmed to
fail the test:

| Mutation | Detected |
| --- | --- |
| `business_transaction()` returns `None` | Yes |
| The writer ignores `context.transaction()` | Yes |
| A business write is visible as committed before the commit | Yes |
| Writes publish with a checkpoint from another boundary | Yes |
| `CommitOutcomeUnknown` is downgraded to `NotCommitted` | Yes |

The second of these is the reason the writer holds no resource handle. An
earlier version of this test gave the writer one and staged rows directly from
the test body; it passed whether or not the borrowing path ran at all, so it
was evidence of nothing.

### Error contract

The two failure modes stay distinct and must not be collapsed:

| Situation | Result |
| --- | --- |
| The descriptor does not declare a launch requirement | `FlowRuntimeError::UndeclaredCapability`, before any transaction |
| The descriptor declares a capability the adapter does not implement | An adapter defect, surfaced as `RepositoryError::UnsupportedCapability` |

A missing declaration is never converted into a generic
`FlowRuntimeError::Repository`.

Two of the codec scenarios are runtime scenarios rather than value scenarios,
deliberately. A
bound only means something if breaching it stops the commit that would have made
the bad state authoritative, so
`oversized_or_over_deep_payload_is_a_known_not_committed_outcome` and
`corrupt_payload_never_advances_a_checkpoint` drive the chunk runtime through a
commit-boundary state provider shaped like the PostgreSQL adapter's, and assert
both the typed not-committed outcome and that the retained checkpoint generation
did not move. Asserting only that `Checkpoint::encode` returns an error would
have proved nothing about whether a checkpoint advances.

`durable_meaning_capability_change_changes_the_fingerprint` varies one value at
a time, so no two changes can cancel each other out.

## Public API changes

Two, both pre-1.0, both reviewed here and recorded in the changelog.

**`VersionedStateCodec` gains `upgrades` and `decode` loses its version
argument.** Breaking. The `upgrades` default is empty, which is correct for a
codec whose schema has only ever had one version, so a single-version codec
needs no change beyond the `decode` signature. A codec that handled an older
version inside `decode` must now declare that edge; if it does not, an older
recorded payload is `NoUpgradePath` rather than silently misread. No released
version exists, and the repository's one codec implementation was updated with
the change.

**`RepositoryDescriptor` and `JobRepository::descriptor` are added.** Additive:
`descriptor` has a default, so no existing implementation breaks. The facade
snapshot's only differences across this work are the two added items,
`StateSchemaUpgrade` and `RepositoryDescriptor`.

## Migration and rollback

The direction states that a durable-format move requires a direct compatibility
edge, migration evidence from every supported prior version, and a documented
restore-based rollback — and that M5 moves no durable format.

**No durable format moved here**, so no new migration is owed. The envelope
format version, its members, and every byte `encode` produces are unchanged; the
upgrade chain changes how recorded bytes are *read back*, not what is written.
The declared schema-1-to-3 upgrade path for the durable metadata schema, its
newer-version rejection, and its restore-based rollback are unchanged and are
exercised by the campaign scenarios
`schema1_and_schema2_upgrade_directly_to_schema3`,
`schema2_runtime_rejects_schema3`, and
`schema3_backup_restores_the_prior_schema`, which the design gate assigns to
issue [#102](https://github.com/luceat-lux-vestra/oxide-batch/issues/102) rather
than to this one. This record does not claim them. The two exit criteria that
depended on them were reassigned to #102 in both issue bodies, and #100 closed
on that reassignment rather than on their evidence.

The application-visible consequence recorded above — a codec that decoded an
older version inline must now declare the edge — is an application migration,
not a durable one. No stored byte changes and no backup becomes unreadable.

## Validation

Run and passing at the commit this record describes:

```shell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo check -p oxide-batch --no-default-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
cargo xtask deps
```

The golden fingerprint vectors and the normalized repository write traces are
unchanged. The facade snapshot was rewritten deliberately for the two added
items and for no other difference.

**Not run locally:** the PostgreSQL suites, which require a database this
environment does not have. They run in CI, and the migration and restore
campaign is issue #102's.

## Boundaries held

- No repository portability, no additional Tier-1 adapter, and no Spring
  metadata migration; those remain M8 and M12.
- No capability is added to the six the milestone defines, and no descriptor
  field is added to reserve a capability that is not delivered. The accepted
  target descriptor in the
  [repository and transaction model](../architecture/repository-and-transaction-model.md#capability-negotiation)
  lists isolation, locking, lease/fencing, pagination form, and backup support;
  none of those are declared here, because declaring a field no adapter varies
  and no caller reads would be reserving a future design.
- The borrowed adapter-owned transaction path is unchanged. The facade still
  lends a bounded OxideBatch-owned transaction port, driver types stay private,
  and atomic checkpoint and unknown-outcome semantics are exactly as accepted.
- No observable batch semantics, persisted byte, transaction boundary, lifecycle
  write, restart selection, definition fingerprint, or normalized trace changes.
