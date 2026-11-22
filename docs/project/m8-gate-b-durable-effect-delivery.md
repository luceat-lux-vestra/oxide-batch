# M8 Gate-B — Durable-Effect Persistence and Chunk Enlistment

**State:** Accepted M8 design, revised by
[ADR-0016](../architecture/decisions/0016-durable-effect-definition-identity-and-runtime-boundary.md).
The earlier remote semantic-contract model is retained only as superseded design
history in ADR-0015.
**Owner:** M8 tracker
[#201](https://github.com/luceat-lux-vestra/oxide-batch/issues/201)
**M9 consumer:** integration tracker
[#202](https://github.com/luceat-lux-vestra/oxide-batch/issues/202)

**Governing decisions:**
[RFC-0007](../rfcs/0007-repository-services-and-capabilities.md),
the [repository and transaction model](../architecture/repository-and-transaction-model.md),
the [execution semantics](../compatibility/execution-semantics.md),
[ADR-0004](../architecture/decisions/0004-job-definition-restart-compatibility.md),
[ADR-0009](../architecture/decisions/0009-definition-fingerprint-input-set.md),
and [ADR-0016](../architecture/decisions/0016-durable-effect-definition-identity-and-runtime-boundary.md).

Gate-B remains an internal M8 workstream name. The next-release public and
durable-wire vocabulary uses durable-effect, effect, Outbox, and Inbox terms
rather than the Gate-B codename.

This record defines the M8 portable correctness floor for durable effect intent,
Outbox, InboxDedup, and transaction-scoped enlistment. It deliberately does not
standardize provider/broker acknowledgement, remote replay, unknown-outcome, or
idempotency semantics before M9 has concrete adapters that can enforce them.

## Design principles

M8 follows six rules.

1. **Only enforceable semantics become definition contracts.** A manifest must
   not claim a guarantee that the executable runtime has no port to enforce.
2. **One transaction owns one atomicity claim.** EffectJournal intent, Outbox,
   or InboxDedup records that protect chunk work must stage in the same real
   `ChunkTransaction` as the business mutation and protected progress.
3. **Existing authority is reused.** `ChunkDeliveryMode::AtomicSameResource`
   remains the authority for same-resource business/progress atomicity; M8 does
   not create a duplicate local-atomicity declaration.
4. **Logical effect identity is restart-stable.** A logical `EffectId` is
   distinct from physical attempts, execution IDs, wall clock, connections, and
   payload equality.
5. **Capability absence fails closed.** A definition requiring a local durable
   effect capability does not silently degrade when the selected repository
   profile lacks it.
6. **Remote semantics are evidence-driven extensions.** M9 adapters document
   exact protocol behavior first. A shared portable remote contract is promoted
   only when multiple concrete consumers prove it is useful.

## M8 version-1 requirement model

The mandatory M8 durable-effect definition contains three composable local
persistence requirements.

| Requirement | Values | Portable meaning |
| --- | --- | --- |
| Effect journal | `not_required`, `required` | the open chunk transaction can durably stage immutable logical effect intent |
| Outbound obligation | `none`, `outbox` | the authorizing mutation, protected progress, and outbound obligation commit or roll back together |
| Inbound protection | `none`, `inbox_dedup` | the inbound identity guard, protected local mutation, and consumed progress commit or roll back together |

These requirements are orthogonal. A definition may require any valid
composition.

The M8 version-1 model does **not** include:

- a second local-atomicity field;
- external `best_effort`, `at_least_once`, or provider-idempotent replay
  modes;
- broker-native transaction contracts;
- portable remote semantic-profile IDs;
- acknowledgement-contract IDs;
- unknown-outcome-contract IDs;
- provider-key binding contracts; or
- provider idempotency validity floors.

Those are M9-or-later concerns until an executable integration owns them.

## Runtime applicability

The M8 durable-effect contract is initially chunk-runtime only.

Supported bindings are:

- a standalone `ChunkJob`; and
- a direct compiled `FlowNode::Step` with `StepComponents::Chunk` when the
  runtime binding supplies the transaction-scoped durable-effect enlistment
  path.

A mixed graph may contain tasklet and chunk nodes. Only explicitly bound chunk
nodes may own durable-effect sites.

The initial contract does not apply to:

- `TaskletJob` or `StepComponents::Tasklet`;
- custom-leaf bodies;
- deciders;
- nested-job boundaries;
- split managers;
- partition managers or workers that do not expose an explicit chunk effect
  binding; or
- any future node kind without the same transaction-scoped enforcement path.

Compilation or definition construction fails before manifest production when an
effect site targets an unsupported node. The runtime never treats an unsupported
site as a no-op.

## Same-resource authority

`ChunkDeliveryMode::AtomicSameResource` remains the existing declaration that
business writes and the framework progress they protect share one resource
transaction.

When durable-effect enlistment is also selected, an adapter may lend a
transaction-scoped durable-effect port from that same `ChunkTransaction`.
This does not create a second transaction manager or generic distributed
transaction abstraction.

A separate repository write before or after the chunk transaction does not
satisfy EffectJournal intent, Outbox, or InboxDedup atomicity when that record is
part of the protected chunk outcome.

An ambiguous local commit remains governed by the existing
`CommitOutcomeUnknown` contract. A fresh durable repository observation is
required before the framework infers whether the local transaction committed.

## Logical effect identity

`EffectId` is the opaque replay-stable identity of one logical effect
occurrence.

An EffectId:

- survives process retry and supported restart for the same logical
  occurrence;
- does not change because a physical attempt changes;
- is not a random retry ID, wall clock, connection ID, `JobExecutionId`,
  `StepExecutionId`, or payload hash alone; and
- differs for distinct logical occurrences even when their payload bytes are
  equal.

M8 version 1 uses one framework-owned effect-identity contract version. It
selects the complete derivation algorithm rather than exposing speculative
independent version axes.

The derivation combines:

- the stable definition/effect-site identity; and
- a restart-stable occurrence input.

The occurrence input must be reproducible from durable progress plus a stable
component-owned occurrence key, or from an explicit stable application
operation key. A transient chunk number or in-memory item ordinal alone is not
sufficient.

The implementation must not claim support for an identity-contract version
until the derivation implementation and restart evidence exist.

## Effect-site identity

M8 version 1 does not create an unrelated effect-scope namespace.

A static effect site is identified by:

- the existing compiled `NodeId` of the chunk step that owns the effect; and
- one bounded `EffectSlotId` inside that node.

The effect declaration is canonicalized by `NodeId`, then `EffectSlotId`.
Duplicate node declarations and duplicate slots within one node fail closed.

A standalone `ChunkJob` already compiles through a stable step/node identity.
The durable-effect contract consumes that identity rather than duplicating it.

Provider/destination identities used by a concrete runtime operation may still
be recorded as runtime effect-intent bytes. They are not a second static
definition namespace in M8 version 1.

## Transaction-scoped enlistment

The portable transaction-scoped port supports three operations.

### Inbox guard

An Inbox guard carries:

- a restart-stable inbound identity; and
- an opaque canonical request identity.

The transaction returns one of:

- first delivery;
- duplicate with the same protected request; or
- identity conflict.

The guard is provisional until the enclosing chunk transaction commits.

### Effect intent

An effect intent carries:

- `EffectId`;
- opaque request identity; and
- opaque destination/operation identity needed by the local durable record.

M8 version 1 does not require a provider-key binding field. Provider-specific
idempotency binding belongs to a later integration contract.

The intent operation is idempotent by immutable logical identity: a matching
existing intent succeeds, while incompatible content conflicts.

### Outbox obligation

An Outbox obligation carries:

- `EffectId`;
- opaque destination/operation identity; and
- a bounded encoded obligation.

The obligation is staged inside the same chunk transaction as the authorizing
local mutation and protected progress.

### Enlistment outcome

A successful enlistment operation means only that the record is staged in the
open transaction. It becomes authoritative only if the enclosing
`ChunkTransaction` commits.

Rollback discards the staged record. An unknown final commit remains unknown and
is resolved through the repository's existing local commit-ambiguity rules.

## EffectJournal persistence floor

For M8 version 1, EffectJournal persistence means the repository can durably
stage and preserve immutable logical effect intent under the transaction rules
above.

M8 does not yet standardize a provider/broker dispatch state machine. In
particular, version 1 does not make `ExposureGranted`, remote
`Acknowledged`, provider query results, or terminal remote dispositions part
of the mandatory portable contract.

M9 may extend the journal with exposure, attempt, acknowledgement, and
reconciliation state when concrete integrations define the corresponding
authority and failure semantics.

This narrower M8 floor prevents the repository layer from claiming remote truth
it cannot observe.

## Outbox

Outbox requires the authorizing local mutation, protected framework progress,
and outbound obligation to commit in one certified transaction.

A later dispatcher may deliver an Outbox record more than once unless its
integration profile proves a stronger remote property. M8 does not infer
provider idempotency from the existence of an Outbox row.

The Outbox record is a local durable obligation, not evidence that the remote
system accepted the effect.

## InboxDedup

InboxDedup requires the inbound marker, protected local business effect, and
consumed framework progress to commit in the same certified local transaction.

Concurrent deliveries serialize through durable uniqueness, compare-and-swap,
transactional conflict, or an equivalent certified mechanism.

Matching inbound identity with matching request identity returns the recorded
duplicate disposition. Reusing the inbound identity for a conflicting request
fails closed.

A definition may declare a positive minimum InboxDedup validity floor. The
deployment's effective safety window is a repository capability and must be at
least that floor. The effective value itself is not definition identity.

M8 does not define broker acknowledgement ordering beyond this local floor.
M9 consumer adapters must define when an acknowledgement/delete/offset change is
allowed relative to the committed Inbox authority.

## Repository capability declaration

EffectJournal, Outbox, and InboxDedup persistence are separate optional
repository capabilities even when one backend uses one physical table,
collection, or log for all three.

A deployment-owned durable-effect persistence descriptor may certify:

- the framework-owned durable-effect persistence contract version;
- EffectJournal persistence profile version;
- Outbox persistence profile version;
- InboxDedup persistence profile version;
- effective InboxDedup validity window;
- maximum effect records staged per chunk; and
- maximum encoded durable-effect bytes staged per chunk.

These values are deployment capability, not definition fingerprint material.

Runnable-core repository support requires none of these optional capabilities.
A definition that selects one fails closed at plan/launch/restart negotiation
when the deployment does not certify it.

## Definition identity and manifest format 5

[ADR-0016](../architecture/decisions/0016-durable-effect-definition-identity-and-runtime-boundary.md)
owns the durable definition projection.

A durable-effect definition uses format 5 with:

- exact legacy `base` projection;
- outer `format = 5`; and
- a closed `durable_effects` object.

The version-1 durable-effect projection contains only:

- EffectJournal requirement;
- Outbox requirement;
- InboxDedup requirement;
- one framework-owned effect-identity contract version;
- canonical compiled node/effect-slot declarations; and
- optional definition-level InboxDedup minimum validity.

Released format 1 through 4 bytes and digests remain immutable.

The internal Gate-B codename does not enter the durable wire format.

## Retention and cleanup

Generic Gate-C history purge must not delete durable-effect records whose local
correctness obligation is still live.

At minimum:

- pending Outbox obligations survive generic job-history deletion;
- live InboxDedup markers survive until their certified safety window and
  cleanup policy permit removal; and
- unresolved EffectJournal intents survive until an explicit effect-specific
  cleanup/disposition policy authorizes removal.

Effect/dedup cleanup is a separately authorized bounded operation. Generic
history retention does not silently decide remote application state.

## M9 handoff: concrete remote semantics first

M9 owns messaging, HTTP/webhook effect writers, and other remote integrations.

Every concrete adapter that claims non-trivial delivery behavior must document
and certify at least:

- stable message/effect identity source;
- exposure boundary;
- acknowledgement meaning;
- offset/cursor/visibility ownership;
- redelivery and duplicate behavior;
- native transaction scope, when any;
- known-not-applied, known-applied, and unknown-outcome mapping;
- idempotency/dedup key scope and validity, when claimed;
- checkpoint/progress ordering relative to acknowledgement or send;
- resource bounds and cancellation behavior; and
- exact remote product/protocol versions.

Those facts remain adapter-specific until evidence shows that two or more
integrations need the same provider-neutral contract.

A portable semantic-contract layer may be promoted later when:

1. multiple adapters require the same provider-neutral semantic identity;
2. compatible restart across adapter/provider replacement requires it;
3. third-party adapter compatibility cannot be represented by the existing
   capability model; or
4. durable reconciliation requires a finer stable semantic contract.

M8 does not reserve speculative public fields merely to avoid future manifest
evolution.

## Security and observability

Credentials, endpoints, raw provider keys, request/response bodies, business
records, SQL, and raw driver/provider errors do not enter public durable-effect
diagnostics.

`EffectId` may be used as bounded correlation evidence in logs or spans when
reviewed, but it must not become an unbounded metric label. Request identities
and opaque destination identities are correctness metadata, not automatically
public telemetry.

## M8 validation floor

Implementation and certification must prove:

- format-1/2/3/4 released bytes and digests remain unchanged;
- unsupported tasklet/non-chunk effect bindings are rejected before manifest
  production;
- standalone chunk and bound flow-chunk sites use the existing compiled
  `NodeId`;
- effect sites and slots canonicalize deterministically;
- EffectJournal intent, Outbox, and InboxDedup staging occurs in the same real
  `ChunkTransaction` as protected business/progress work;
- missing repository capability fails closed before lifecycle mutation;
- InboxDedup validity floors are enforced against deployment capability;
- rollback removes provisional effect records;
- local unknown commit is not guessed into committed or rolled back;
- no remote/provider/deployment value enters definition identity; and
- M8 makes no generic cross-resource exactly-once claim.

## Explicit non-goals

M8 version 1 does not:

- provide a universal transaction manager;
- add tasklet durable-effect enlistment;
- define a generic provider/broker semantic registry;
- standardize remote acknowledgement or unknown-outcome semantics;
- standardize provider idempotency-key binding;
- implement an Outbox dispatcher;
- implement broker/HTTP provider adapters;
- claim remote exactly-once delivery; or
- make M9 integration certification unnecessary.
