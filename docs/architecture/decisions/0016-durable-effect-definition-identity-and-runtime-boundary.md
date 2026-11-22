# ADR-0016: Durable-Effect Definition Identity and Runtime Boundary

- **State:** Accepted; partially superseded by [ADR-0017](0017-canonical-compiled-plan-manifest-v1-reset.md) for legacy-format immutability and additive format-5 representation only
- **Date:** 2026-11-22
- **Owners:** runtime, API, repository, and integration maintainers
- **Deciders:** project owner
- **Work item:** [#715](https://github.com/luceat-lux-vestra/oxide-batch/issues/715)
- **Supersedes:** [ADR-0015](0015-gate-b-definition-identity-and-manifest-fingerprint-projection.md)
- **Partially supersedes:** [ADR-0006](0006-repository-capability-model.md) for the
  M8 durable-effect delivery/profile scope only; ADR-0006 remains authoritative
  for repository-service separation and capability negotiation.
- **Specializes:** [RFC-0007](../../rfcs/0007-repository-services-and-capabilities.md)
  after acceptance by narrowing which delivery semantics are mandatory M8
  definition contracts.
- **Extends:** [ADR-0004](0004-job-definition-restart-compatibility.md),
  [ADR-0005](0005-compiled-execution-plan.md), and
  [ADR-0009](0009-definition-fingerprint-input-set.md)
- **Partial supersession scope:** ADR-0017 replaces the formats-1-4 preservation,
  format-5 wrapper, legacy-base grammar, and wrapper-specific base/effect
  cross-validation requirements. This ADR remains authoritative for the local
  durable-effect model, runtime boundary, identity semantics, capability
  negotiation, strict durable-effect restart, and M9 deferral.
- **Canonical M8 record:**
  [M8 Gate-B durable-effect delivery](../../project/m8-gate-b-durable-effect-delivery.md)

## Status and evidence boundary

This ADR is an accepted target architecture. It is **not** implementation,
adapter-certification, or release-support evidence.

The latest published OxideBatch release is v0.7.0. Its released manifest formats
1 through 4 are compatibility artifacts and remain immutable.

Gate-B API and format-5 work merged after v0.7.0 is unreleased. It may be
renamed, removed, or substantially replaced before the next release when that
is required by this ADR.

No current source path may claim complete ADR-0016 support until the required
runtime negotiation, effect-identity derivation, format-5 validation, adapter
persistence evidence, and restart evidence are implemented and verified.

## Context

ADR-0015 correctly identified that restart-relevant durable-effect semantics
need an additive manifest boundary rather than a silent change to formats 1
through 4.

Its first model generalized M8 beyond what the current runtime can enforce. It
made provider-neutral remote replay profiles, acknowledgement contracts,
unknown-outcome contracts, broker-native transaction contracts, provider-key
binding versions, and several independent identity-version axes mandatory parts
of the definition model before M9 had concrete adapters capable of enforcing
them.

The current M8 runtime does have a narrower enforceable boundary:

- one chunk transaction can expose transaction-scoped EffectJournal, Outbox,
  and InboxDedup enlistment;
- those local records can share the same physical correctness transaction as
  protected business work and progress;
- a repository descriptor can declare local persistence capabilities; and
- launch/restart can fail closed before lifecycle mutation when the selected
  deployment cannot satisfy the definition.

This ADR keeps that concrete M8 floor and defers remote/provider semantics until
M9 has executable integrations and evidence.

## Decision

### 1. M8 owns local durable-effect correctness

The mandatory M8 version-1 durable-effect definition has exactly three
composable local requirement dimensions.

| Requirement | Values | Meaning |
| --- | --- | --- |
| Effect journal | `not_required`, `required` | the open chunk transaction can durably stage immutable logical effect intent |
| Outbound obligation | `none`, `outbox` | authorizing local mutation, protected progress, and outbound obligation commit together |
| Inbound protection | `none`, `inbox_dedup` | inbound identity marker, protected local mutation, and consumed progress commit together |

These dimensions compose. They are not a strength ladder.

A durable-effect definition must select at least one non-default requirement.
A format-5 wrapper with all three defaults is invalid because it adds no
restart-relevant semantic information.

Existing `ChunkDeliveryMode::AtomicSameResource` remains the single definition
authority for same-resource business/progress atomicity. M8 does not add a
second local-atomicity field.

The following are not M8 version-1 definition requirements:

- external best-effort or at-least-once delivery profiles;
- provider-idempotent external replay;
- broker-native transaction contracts;
- acknowledgement semantics;
- unknown-outcome semantics;
- provider-key binding contracts; or
- provider idempotency validity floors.

Those remain valid integration concerns but are M9-or-later definition
extensions.

### 2. M8 durable-effect opt-in is initially chunk-runtime only

A definition may select ADR-0016 durable-effect semantics only for effect sites
that resolve to an executable chunk step with the transaction-scoped
durable-effect enlistment path.

Initially supported bindings are:

- a standalone `ChunkJob`; and
- a direct compiled `FlowNode::Step` whose `StepComponents` is `Chunk`
  and whose executable binding uses the chunk runtime.

A graph may contain tasklet and chunk nodes together. Only explicitly bound
chunk nodes may own effect slots.

The version-1 contract does not authorize effect sites on:

- `TaskletJob` or `StepComponents::Tasklet`;
- custom-leaf bodies;
- deciders;
- nested-job boundaries;
- split managers;
- partition managers or workers without an explicit chunk-effect runtime
  binding; or
- any node kind that cannot prove the same transaction-scoped enforcement
  boundary.

Definition/plan construction rejects an unsupported effect-site binding before
format-5 identity is produced.

### 3. Static effect sites reuse compiled node identity

M8 does not create an unrelated public effect-scope namespace.

Each static effect site is identified by:

1. the existing compiled `NodeId` that owns the executable chunk step; and
2. a bounded stable `EffectSlotId` within that node.

For a standalone chunk definition, the one-step compiler already derives the
compiled node from the stable step name. Format-5 uses that compiled node
identity rather than inventing a parallel scope ID.

Node declarations are canonically ordered by `NodeId`. Slot declarations
within a node are canonically ordered by `EffectSlotId`. Duplicate nodes or
duplicate slots within a node are invalid.

### 4. Effect identity is conditional, not universal

`EffectId` is the opaque replay-stable identity of one logical effect
occurrence.

An `EffectId` is required by local effect intent and outbound obligation
semantics. It is not required by pure inbound deduplication.

Therefore:

- if `effect_journal=required` or `outbound_obligation=outbox`,
  `effect_identity` is required;
- if both are absent and the definition is pure
  `inbound_protection=inbox_dedup`, `effect_identity` is forbidden;
- an effect-bearing definition has at least one effect node and at least one
  slot; and
- a pure InboxDedup definition may validly contain no effect node/slot
  declaration; and
- even without effect slots, a pure InboxDedup definition must contain at least
  one supported direct chunk step in the embedded base, because the Inbox guard
  is available only through the chunk transaction/runtime path.

This keeps inbound message identity separate from outbound/logical effect
identity. For mixed graphs, the requirement is definition-wide capability
selection; actual Inbox guard invocation is possible only from a chunk
`WriteContext`.

### 5. One framework-owned EffectId derivation contract

M8 version 1 has one framework-owned effect-identity contract version.

Its semantic input tuple contains, at minimum:

- the exact definition identity/fingerprint that owns the effect declaration;
- the canonical logical job-instance identity derived from the job name and
  identifying parameters, not a repository-assigned execution attempt ID;
- the owning compiled `NodeId`;
- the owning `EffectSlotId`; and
- one restart-stable logical occurrence input.

The occurrence input must be reproducible from durable progress plus a stable
component-owned occurrence key, or be supplied as an explicit stable
application operation key.

The derivation never uses a physical attempt number, wall clock, connection
identity, random retry token, `JobExecutionId`, `StepExecutionId`, or a
payload hash alone as logical identity.

Contract version 1 is not considered implemented merely because the manifest
contains the number `1`. Before production support, implementation work must
freeze and test:

- exact domain separation;
- exact byte framing for every semantic input;
- exact digest/derivation algorithm;
- canonical parameter/job-instance encoding; and
- golden vectors proving restart stability and collision-domain separation.

Until those details and vectors exist, callers cannot manufacture a supported
`EffectId` from transient values.

### 6. Manifest format 5 is the additive durable-effect boundary

A definition that selects ADR-0016 durable-effect semantics uses manifest
format 5. Definitions that do not opt in continue to emit their exact
historical format 1, 2, 3, or 4 bytes.

Format 5 has exactly three root members:

```json
{
  "base": { "...": "exact frozen legacy format-1/2/3/4 projection" },
  "durable_effects": { "...": "closed durable-effect contract" },
  "format": 5
}
```

The internal project codename `Gate-B` is not durable wire vocabulary.

The `base` object is the exact canonical historical projection the same
definition shape would emit without durable effects. Serializing `base`
alone must reproduce the frozen legacy bytes exactly.

Format-5 parsing is intentionally stricter than the historical standalone
readers. The embedded base is validated with a closed grammar appropriate to
its exact legacy format, but standalone format-1/2/3/4 readers are not
retroactively tightened.

The existing framework manifest byte ceiling remains authoritative for the
entire format-5 document.

### 7. Format-5 durable-effect grammar version 1

`durable_effects.contract_version` is the version of the durable-effect
definition/wire semantic grammar. Version 1 is the only recognized value.

The `durable_effects` object has:

- required `contract_version`;
- required `requirements`;
- conditionally present `effect_identity`; and
- conditionally present `inbox_dedup_minimum_validity_ms`.

No root or nested member accepts JSON `null`. Unknown members fail closed.

`requirements` has exactly three required members:

```json
{
  "effect_journal": "not_required",
  "inbound_protection": "inbox_dedup",
  "outbound_obligation": "none"
}
```

All three default values together are invalid.

When an effect identity is required, its exact version-1 shape is:

```json
{
  "contract_version": 1,
  "nodes": [
    {
      "node_id": "compiled-chunk-node-id",
      "slots": [
        { "slot_id": "stable-effect-slot-id" }
      ]
    }
  ]
}
```

Each node object has exactly `node_id` and `slots`. Each slot object has
exactly `slot_id`.

The effect node array is non-empty when present. Every node has at least one
slot.

Node declarations are bounded by both:

- the number of resolvable compiled nodes in the embedded base; and
- the existing framework `MAX_NODES` reader capability ceiling.

The total number of effect slots is bounded by one dedicated framework-owned
reader capability ceiling for durable-effect slots. That bound is centralized
in code, tested, and not serialized into the manifest. The implementation must
not repurpose an unrelated mutable environment value as this bound.

`inbox_dedup_minimum_validity_ms` is a positive definition floor. It is:

- allowed only when `inbound_protection=inbox_dedup`;
- omitted when no explicit floor is selected; and
- never a deployment effective-window value.

### 8. Reader and producer validate the same base-to-effect binding

The format-5 producer and strict reader enforce the same semantic checks.

For pure InboxDedup without `effect_identity`, the embedded base must still
contain at least one supported direct chunk step.

For every effect node declaration:

1. the `node_id` resolves to exactly one node in the embedded base;
2. that resolved node is a direct executable step node;
3. the step declaration is `StepComponents::Chunk` / the equivalent frozen
   one-step chunk projection;
4. the effect node is not a tasklet, decision, custom leaf, nested job, split,
   join, or unsupported partition node; and
5. slot declarations satisfy canonical ordering, uniqueness, identifier bounds,
   and the dedicated total-slot ceiling.

For an embedded format-1 one-step base:

- `kind=tasklet` forbids `effect_identity`;
- `kind=chunk` resolves the owning `NodeId` using the existing one-step
  compiled-node rule derived from the frozen `step` identity.

For embedded formats 2 through 4, the reader resolves `node_id` against the
exact compiled graph projection and verifies the node's declared component
kind.

A persisted manifest that could not have been produced from a valid supported
runtime binding is rejected. The reader never trusts the outer
`durable_effects` declaration without cross-validating the base.

### 9. Version families are independent authorities

The following version families are intentionally distinct:

| Version family | Authority |
| --- | --- |
| `durable_effects.contract_version` | manifest definition/wire semantic grammar |
| `effect_identity.contract_version` | EffectId derivation semantics |
| EffectJournal profile version | deployment certification of local EffectJournal persistence |
| Outbox profile version | deployment certification of local Outbox persistence |
| InboxDedup profile version | deployment certification of local InboxDedup persistence |
| Repository descriptor version | descriptor envelope/schema evolution |

Equal integer values across these families have no compatibility meaning.

In particular, a repository persistence descriptor must not be accepted merely
because a repository-wide numeric contract version equals
`durable_effects.contract_version`.

If a descriptor envelope has its own schema/contract version, that value
versions the descriptor representation only. Definition requirements are
matched to their selected local capability families and profile versions, not
to accidental numeric equality with the manifest grammar.

### 10. Capability negotiation is launch/restart authority, not compiler authority

Definition and plan construction are deployment-independent.

They validate:

- static requirement shape;
- format-5 grammar;
- effect-site binding to supported chunk nodes;
- canonical identity declarations; and
- definition-level invariants.

They do not inspect a live repository descriptor.

Before lifecycle mutation, launch and restart resolve the selected deployment
descriptor and fail closed when it cannot satisfy the definition's required
local durable-effect capabilities.

This pre-lifecycle negotiation must exist for both:

- `FlowLauncher` paths; and
- standalone `JobLauncher::launch_chunk` / compatible entry points.

Current source contains durable-effect descriptor data and direct descriptor
validation helpers, but that alone is not support evidence. ADR-0016 support is
not claimed until the actual launcher/restart paths are wired and tested
against certified adapters.

### 11. M8 version-1 compatible restart is strict for durable-effect definitions

ADR-0004 remains authoritative for ordinary definitions.

For any restart where either source or target selects ADR-0016
`durable_effects` and the source/target manifest digests differ, M8 version 1
fails closed.

Therefore:

- strict restart of the exact same format-5 manifest is allowed;
- a direct compatible-upgrade edge does not override this restriction for M8
  version 1 durable-effect definitions;
- legacy definitions that do not select `durable_effects` retain their
  existing ADR-0004 compatible-restart behavior; and
- a fork creates a new job instance and copies no EffectJournal, Outbox,
  InboxDedup, or EffectId state by default.

This restriction exists because the current `DefinitionUpgrade` state-family
model has no explicit durable-effect continuity/mapping contract.

Already durable EffectJournal/Outbox/Inbox records remain source facts. They are
never re-derived, retargeted, or rewritten merely because a target definition
has another fingerprint.

A future ADR may enable compatible restart for durable-effect definitions only
after it defines explicit bounded continuity for live durable-effect records,
effect sites, and identity semantics.

### 12. Remote semantic contracts are deferred, not prohibited

The following concepts are outside M8 manifest contract version 1:

- generic portable remote semantic-contract IDs and versions;
- broker-native transaction contract IDs;
- remote semantic-profile IDs;
- acknowledgement-contract IDs;
- unknown-outcome-contract IDs;
- provider-key binding contracts;
- provider idempotency validity floors; and
- generic remote-profile parser registries.

M9 adapters still document their exact acknowledgement, exposure, redelivery,
duplicate, native-transaction, idempotency-window, and unknown-outcome
behavior.

A provider-neutral contract becomes a candidate when concrete evidence shows
at least one of:

- two or more independent adapters need the same provider-neutral semantic
  contract;
- restart across adapter/provider replacement needs semantic identity
  independent of provider identity;
- a third-party extension cannot be represented truthfully by the existing
  capability model; or
- durable reconciliation requires a stable semantic identity finer than the
  M8 local contract.

### 13. Deployment values remain outside definition identity

The deployment-owned repository descriptor may certify:

- EffectJournal persistence profile/version;
- Outbox persistence profile/version;
- InboxDedup persistence profile/version;
- effective InboxDedup validity window; and
- bounded per-chunk effect record and encoded-byte limits.

A selected definition requirement that the deployment does not certify fails
closed before lifecycle mutation.

Deployment/provider values do not enter definition identity, including:

- endpoints;
- accounts;
- credentials;
- regions;
- connection/pool settings;
- concrete idempotency keys;
- physical attempts; and
- observed remote references/results.

### 14. Public vocabulary uses domain terms

`Gate-B` is an internal project workstream name, not a user-facing domain
abstraction.

Before the next release, surviving public API names use durable-effect, effect,
Outbox, or Inbox terminology. Existing merged Gate-B names are unreleased and
are not compatibility constraints.

Project issues and historical project records may continue to use Gate-B as a
workstream label.

## Consequences

- M8 keeps a useful enforceable durable-effect foundation without pretending to
  be a universal distributed-effect protocol.
- Format 5 remains additive and fail closed while formats 1 through 4 stay
  byte-for-byte immutable.
- Pure InboxDedup definitions do not pay for an unrelated EffectId contract.
- Effect-bearing definitions are tied to actual compiled chunk nodes.
- Manifest grammar version, EffectId derivation version, and repository profile
  versions cannot be accidentally conflated.
- Runtime support requires real pre-lifecycle deployment negotiation, not only
  descriptor helper methods.
- Durable-effect definitions use strict restart only in M8 version 1.
- Remote/provider semantics remain available for later evidence-driven
  extension.
- The current format-5 implementation branch may be substantially rewritten.

## Alternatives considered

### Keep ADR-0015 and only choose an open or closed remote registry

Rejected. The registry question was a symptom. The deeper issue was that M8
could serialize semantics the runtime could not enforce.

### Keep all RFC-0007 delivery vocabulary in the M8 manifest

Rejected. RFC-0007 remains valid as a broader M5-M9 target, but ADR-0016 narrows
which semantics are mandatory M8 definition contracts. Remote delivery
vocabulary remains M9 integration work.

### Require EffectId for InboxDedup-only definitions

Rejected. InboxDedup protects inbound identity and request identity and does not
need an outbound/logical effect slot.

### Reuse one version number for manifest and repository semantics

Rejected. Grammar evolution, EffectId derivation, and deployment capability
certification are independent compatibility axes.

### Permit compatible restart immediately

Rejected. Existing upgrade metadata has no durable-effect state family or live
effect-site continuity mapping. Strict restart is safer than inventing implicit
continuity.

### Add tasklet durable-effect enlistment now

Rejected for M8 version 1. Tasklet runtime does not expose the same transaction-
scoped durable-effect enlistment authority.

## Validation

Implementation work following this ADR must prove at least:

### Released compatibility

- exact v0.7.0 format-1/2/3/4 bytes and digests remain unchanged;
- unchanged definitions that do not opt into durable effects keep their exact
  historical format;
- older runtimes reject outer format 5.

### Closed grammar and bounds

- exact root allowlist `base`, `durable_effects`, `format`;
- exact nested allowlists with no unknown members or JSON `null`;
- existing total manifest byte ceiling;
- all three requirement members are required;
- all-default requirements are rejected;
- InboxDedup validity floor without InboxDedup is rejected;
- canonical node/slot order and duplicate rejection;
- effect node count respects both resolved-base nodes and `MAX_NODES`;
- total slots respect the dedicated durable-effect-slot ceiling.

### Conditional effect identity

- pure InboxDedup format 5 without `effect_identity` succeeds when the base
  contains at least one supported direct chunk step;
- pure InboxDedup with `effect_identity` is rejected;
- EffectJournal or Outbox without `effect_identity` is rejected;
- effect-bearing identity with no nodes/slots is rejected.

### Base-to-runtime binding

- format-1 tasklet base plus effect identity is rejected;
- format-1 chunk resolves its existing compiled `NodeId`;
- formats 2-4 accept slots only on direct chunk step nodes;
- tasklet, custom, nested, decision, split, join, and unsupported partition
  bindings are rejected;
- producer and reader enforce the same cross-field rules.

### Version-family isolation

- manifest grammar support does not compare numerically with repository profile
  versions;
- each local repository profile is checked against the correct capability
  family;
- changing one version family cannot silently satisfy another.

### Launcher negotiation

- FlowLauncher rejects a missing selected local persistence capability before
  lifecycle mutation;
- standalone chunk JobLauncher does the same;
- compatible entry points perform the same pre-lifecycle check;
- effective InboxDedup window below the definition floor fails closed;
- deployment values above/within accepted bounds do not change definition
  bytes.

### EffectId v1

- exact domain-separated byte framing and digest algorithm are frozen;
- golden vectors include canonical logical instance identity, NodeId, slot, and
  occurrence input;
- restart of the same logical occurrence reproduces the same EffectId;
- distinct effect site or occurrence changes EffectId;
- physical attempt/execution IDs and wall clock do not influence EffectId.

### Restart

- exact format-5 strict restart succeeds subject to ordinary restart guards and
  capability negotiation;
- any different-digest compatible restart involving durable effects fails
  before lifecycle mutation;
- legacy compatible restart behavior is unchanged;
- fork copies no durable-effect state by default.

### Transaction correctness

- EffectJournal intent, Outbox, and InboxDedup staging occurs inside the same
  real `ChunkTransaction` as the protected business/progress mutation;
- rollback removes provisional records;
- local unknown commit is not guessed into committed or rolled back.

## Revisit triggers

A replacement or additive ADR is justified when:

- tasklets gain an enforceable durable-effect runtime port;
- partitioned/nested execution gains explicit effect-site binding;
- M9 proves a provider-neutral remote semantic contract is useful;
- durable-effect compatible restart gains an explicit continuity/mapping model;
- effect-identity subcontracts demonstrably need independent version evolution;
  or
- released format-5 compatibility constrains a future extension.
