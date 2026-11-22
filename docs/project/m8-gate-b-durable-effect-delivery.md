# M8 Gate-B — Durable-Effect Persistence and Chunk Enlistment

**State:** Accepted M8 design. Local durable-effect semantics are governed by
[ADR-0016](../architecture/decisions/0016-durable-effect-definition-identity-and-runtime-boundary.md);
their manifest representation is governed by
[ADR-0017](../architecture/decisions/0017-canonical-compiled-plan-manifest-v1-reset.md).
The earlier remote semantic-contract model and format-5 wrapper remain only as
superseded design history.
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
[ADR-0016](../architecture/decisions/0016-durable-effect-definition-identity-and-runtime-boundary.md),
and [ADR-0017](../architecture/decisions/0017-canonical-compiled-plan-manifest-v1-reset.md).

Gate-B remains an internal M8 workstream name. The next-release public and
durable-wire vocabulary uses durable-effect, effect, Outbox, and Inbox terms
rather than the Gate-B codename.

This record defines the M8 portable correctness floor for durable effect intent,
Outbox, InboxDedup, and transaction-scoped enlistment. It deliberately does not
standardize provider/broker acknowledgement, remote replay, unknown-outcome, or
idempotency semantics before M9 has concrete adapters that can enforce them.

This record is a design contract, not implementation, adapter-certification, or
release-support evidence. A merged type or descriptor helper does not prove the
launcher, persistence adapter, restart path, or EffectId derivation satisfies
this record.

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
occurrence. It is required only when the definition uses EffectJournal intent
or Outbox obligations. Pure InboxDedup does not require EffectId.

M8 version 1 uses one framework-owned effect-identity contract version. Its
semantic input tuple contains at minimum:

- the exact owning definition identity/fingerprint;
- canonical logical job-instance identity derived from job name and identifying
  parameters, not a repository-assigned execution attempt;
- the compiled `NodeId`;
- the `EffectSlotId`; and
- restart-stable logical occurrence input.

The occurrence input is reproducible from durable progress plus a stable
component-owned occurrence key or is an explicit stable application operation
key.

A physical attempt, wall clock, connection identifier, random retry ID,
`JobExecutionId`, `StepExecutionId`, or payload hash alone is never logical
effect identity.

Version `1` is not considered implemented until exact domain separation, byte
framing, digest/derivation algorithm, canonical logical-instance encoding, and
golden restart vectors are frozen and executable.


## Effect-site identity

M8 version 1 does not create an unrelated effect-scope namespace.

A static effect site is identified by:

- the existing compiled `NodeId` of the chunk step that owns the effect; and
- one bounded `EffectSlotId` inside that node.

Effect identity is conditional:

- `effect_journal=required` or `outbound_obligation=outbox` requires an
  effect-identity declaration;
- a pure `inbound_protection=inbox_dedup` definition forbids effect identity
  but still requires at least one supported direct chunk step in the compiled
  plan; and
- an effect-bearing declaration has at least one node and at least one slot.

Canonical manifest-v1 production and reading both validate every declared
effect `node_id` against the normalized compiled plan:

- the node resolves exactly once;
- it is a direct executable `FlowNode::Step`;
- its `StepComponents` is `Chunk`; and
- tasklet, decision, nested-job, custom-leaf, split, join, and unsupported
  partition nodes cannot own M8 version-1 effect slots.

A standalone `ChunkJob` first lowers to its ordinary one-node compiled plan,
so its effect site uses that same compiled `NodeId` rather than a wrapper-only
identity.

Node and slot declarations are canonical, unique, and bounded. Effect nodes are
limited by the compiled plan and the framework `MAX_NODES` ceiling. Total
effect slots use one dedicated framework-owned durable-effect-slot reader
ceiling; the bound is centralized in code and is not serialized.

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
repository capabilities even when one backend stores them together.

A deployment-owned durable-effect descriptor may certify:

- EffectJournal persistence profile version;
- Outbox persistence profile version;
- InboxDedup persistence profile version;
- effective InboxDedup validity window;
- maximum effect records staged per chunk; and
- maximum encoded durable-effect bytes staged per chunk.

Version authorities are deliberately separate:

- canonical manifest `manifest_version` versions the definition grammar;
- `effect_identity.contract_version` versions EffectId derivation semantics;
- each local persistence profile version certifies one deployment capability;
- repository descriptor version only versions the descriptor envelope.

Equal integers across these families imply nothing.

Initial M8 local profile recognition is exact and framework-owned:

- EffectJournal requirement accepts EffectJournal persistence profile v1;
- Outbox requirement accepts Outbox persistence profile v1;
- InboxDedup requirement accepts InboxDedup persistence profile v1.

Unknown profile versions fail closed. Numeric ordering does not imply
compatibility. A later runtime accepts another profile version only through an
explicit governed mapping.

These deployment profile versions are not serialized into the definition
manifest.

The descriptor-wide Gate-B contract-version equality relation is not a valid
compatibility rule and must be removed from the implementation.

Definition/plan construction remains deployment-independent. It validates
static shape and chunk binding only.

The live repository descriptor is negotiated at launch/restart before lifecycle
mutation. Equivalent pre-lifecycle checking is required on both FlowLauncher
and standalone chunk JobLauncher paths. Descriptor types or direct helper tests
alone are not support evidence until those runtime paths are wired and
certified.


## Definition identity in canonical manifest v1

[ADR-0017](../architecture/decisions/0017-canonical-compiled-plan-manifest-v1-reset.md)
owns the manifest representation.

Durable effects do not create a new manifest format. They are an optional
`durable_effects` member of the one canonical compiled-plan manifest v1.

The M8 durable-effect projection contains only restart-relevant local semantics:

- `effect_journal`;
- `outbound_obligation`;
- `inbound_protection`;
- conditional effect-identity contract/version;
- canonical compiled `NodeId` / `EffectSlotId` declarations when an
  EffectJournal/Outbox logical effect identity is required; and
- optional InboxDedup minimum validity floor.

At least one durable-effect requirement must be selected when the member is
present.

Pure InboxDedup does not carry effect identity. It remains usable only from the
chunk runtime path where the transaction-scoped Inbox guard exists.

EffectJournal or Outbox requires effect identity and at least one valid chunk
effect site.

The internal Gate-B codename is not durable wire vocabulary.

EffectId derives from the canonical-v1 definition fingerprint plus the logical
job-instance identity, compiled `NodeId`, `EffectSlotId`, and a
restart-stable logical occurrence input. The existing
`JobInstanceKey::digest()` is the preferred logical-instance identity input
unless implementation review establishes a correctness reason otherwise.

Historical preview formats 1-4 are not embedded, wrapped, or cross-validated.
Proposed format 5 is never released.

### Restart boundary

M8 version 1 allows strict restart only for durable-effect definitions.

If either source or target selects `durable_effects` and the source/target
canonical manifest digests differ, compatible restart fails closed before
lifecycle mutation even when an ordinary ADR-0004 direct compatibility edge
exists.

A fork creates a new job instance and copies no EffectJournal, Outbox,
InboxDedup, or EffectId state by default. Existing durable-effect records remain
source facts and are never re-derived or retargeted by an upgrade.

A later ADR may authorize compatible durable-effect restart only after it
defines explicit bounded live-effect continuity and mapping semantics.

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

- canonical manifest-v1 durable-effect projection is deterministic and closed;
- historical preview formats are not embedded or used as durable-effect identity;
- all-default durable-effect requirements are rejected;
- pure InboxDedup works without effect identity and rejects an unnecessary
  effect-identity object;
- EffectJournal or Outbox requires non-empty effect nodes and slots;
- tasklet/non-chunk effect bindings are rejected by both producer and reader;
- standalone chunk and bound flow-chunk sites resolve existing compiled
  `NodeId`;
- node/slot order, uniqueness, identifier bounds, manifest-byte bound, and
  dedicated effect-slot bound are enforced;
- EffectId v1 domain separation, byte framing, algorithm, logical-instance
  identity, and golden restart vectors are frozen before support is claimed;
- manifest grammar version, EffectId version, repository descriptor version,
  and persistence-profile versions are never compared as one version family;
- FlowLauncher and standalone chunk JobLauncher reject missing required local
  persistence capability before lifecycle mutation;
- effective InboxDedup validity below the definition floor fails closed;
- EffectJournal intent, Outbox, and InboxDedup staging occurs in the same real
  `ChunkTransaction` as the protected business/progress work;
- rollback removes provisional effect records;
- local unknown commit is not guessed;
- exact canonical-v1 strict restart succeeds subject to normal guards;
- different-digest compatible restart involving durable effects fails closed;
- fork copies no durable-effect state by default; and
- no remote/provider/deployment value enters definition identity.

These are acceptance requirements, not claims that current main already
satisfies them.

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
