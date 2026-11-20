# ADR-0015: Gate-B Definition Identity and Manifest Fingerprint Projection

- **State:** Proposed
- **Date:** 2026-11-20
- **Owners:** runtime, API, repository, and integration maintainers
- **Deciders:** project owner
- **Work item:** [#707](https://github.com/luceat-lux-vestra/oxide-batch/issues/707)
- **Supersedes:** [ADR-0009](0009-definition-fingerprint-input-set.md) for new
  Gate-B definition-identity and fingerprint projection only; ADR-0009 remains
  authoritative for the general inclusion rule and released legacy formats
- **Extends:** [ADR-0004](0004-job-definition-restart-compatibility.md),
  [ADR-0005](0005-compiled-execution-plan.md), and
  [ADR-0013](0013-m7-advanced-flow-scope-repeat-and-evolution.md)
- **Canonical Gate-B contract:**
  [M8 Gate-B durable-effect delivery](../../project/m8-gate-b-durable-effect-delivery.md)

## Context

ADR-0009 fixes the definition fingerprint as exactly the values that select or
reinterpret durable state. Gate-B adds restart-relevant durable-effect
semantics that did not exist when ADR-0009 was accepted: composable delivery
requirements, replay-stable effect identity contracts, remote
acknowledgement/unknown-outcome semantics, and compatible-upgrade continuity for
live durable effects.

Those values cannot be represented safely by silently expanding the existing
legacy `ChunkDeliveryMode`. In particular, legacy
`ChunkDeliveryMode::AtLeastOnce` is the existing chunk writer/replay
duplicate-possible contract; it is not the new Gate-B remote
`AtLeastOnce` profile. Likewise, the historical tasklet
`delivery_mode=best_effort` does not select the new Gate-B remote
`BestEffort` semantics.

The release boundary also changed since ADR-0009. Version `v0.7.0` is a
published non-prerelease release and contains manifest formats 1 through 4.
Their emitted bytes and fingerprints are therefore durable compatibility
material. ADR-0009's earlier one-time format-2/3 re-pin was valid only because
no released version had emitted the replaced bytes; that exception cannot be
reused.

The released reader supports at most format 4 and rejects a newer outer format
as `UnsupportedFormat`. Inside a known format, however, historical reader
validation is not a recursive closed schema and can tolerate members it does
not interpret. Adding optional Gate-B members to formats 1-4 could therefore
let an older runtime ignore correctness-relevant semantics and execute a
definition under weaker legacy meaning.

Gate-B consequently needs a new manifest boundary that:

- leaves formats 1-4 byte-for-byte immutable;
- forces older runtimes to fail closed;
- carries the exact new restart semantics rather than an adapter capability
  superset;
- preserves ADR-0004's explicit application-owned compatible-restart authority;
- does not put per-effect runtime values or deployment bindings into definition
  identity.

## Decision

### 1. Structured Gate-B definitions use manifest format 5

Manifest format 5 is the definition-identity boundary for the new structured
Gate-B contract.

Any definition that explicitly selects the structured Gate-B contract emits
format 5. There is no heuristic that attempts to prove a structured Gate-B
definition "equivalent enough" to a legacy definition and then emits format
1-4.

Definitions using only the existing legacy APIs continue to emit their exact
historical formats and bytes:

- format 1 for one-step tasklet/chunk definitions;
- format 2 for basic compiled flows;
- format 3 for local-scale compiled flows;
- format 4 for advanced-flow definitions.

The framework does not automatically upgrade an unchanged legacy definition to
format 5, and it does not automatically downgrade a structured definition to a
legacy format.

### 2. Format 5 wraps an exact frozen legacy projection

Format 5 has exactly three root members:

```json
{
  "base": { "...": "exact legacy format-1/2/3/4 projection" },
  "format": 5,
  "gate_b": { "...": "closed Gate-B semantic projection" }
}
```

`base` is the exact canonical object that the same definition shape would
emit under the frozen historical format-1, -2, -3, or -4 producer, including
that object's own historical `format` member and legacy
`delivery_mode` semantics.

The following rules are normative:

- `base.format` is one of 1, 2, 3, or 4.
- One-step definitions retain the exact format-1 grammar inside `base`.
- Graph definitions retain the exact applicable format-2, -3, or -4 grammar
  inside `base`.
- The format-5 reader validates `base` against the frozen shape-specific
  grammar. Acceptance by the historical permissive `DefinitionManifest`
  reader alone is insufficient.
- Unknown `base` members fail closed in format 5 even if a legacy reader
  would have ignored them.
- Canonically serializing `base` by itself must reproduce the bytes that the
  frozen legacy producer would emit for that definition.
- The complete outer format-5 manifest remains subject to the existing
  64 KiB canonical-manifest ceiling.

This wrapper deliberately avoids re-specifying or translating every historical
restart member into a second top-level grammar. The legacy projection remains
one authority, while the outer format number gives older runtimes a fail-closed
boundary.

### 3. `gate_b` is a recursively closed, versioned semantic object

The `gate_b` object is not an open extension bag. Every recognized contract
version has a recursively closed member allowlist, exact types, bounded values,
and cross-field validation.

For Gate-B contract version 1 the canonical semantic content comprises:

1. the Gate-B contract version;
2. six orthogonal delivery-requirement dimensions;
3. static effect-identity contract declarations;
4. portable remote semantic-contract identity;
5. definition-level minimum validity/deduplication floors when selected.

The six requirement dimensions are:

| Dimension | Version-1 values |
| --- | --- |
| local atomicity | `none`, `atomic_same_resource` |
| effect journal | `not_required`, `required` |
| outbound obligation | `none`, `outbox` |
| inbound protection | `none`, `inbox_dedup` |
| ambiguity/replay | `none`, `best_effort`, `at_least_once`, `idempotent_external_effect` |
| native message transaction | `none`, or `required` plus a portable semantic contract ID/version |

These dimensions compose; they are not a single strength ladder. In particular,
`TransactionalMessage` is a broker-native transaction scope and remains
separate from ambiguity/replay behavior.

A native-message transaction requirement records only a bounded portable
semantic contract ID and non-zero contract version. It does not record a
concrete broker implementation, cluster, endpoint, region, account, credential,
or client object.

#### Contract-version-1 canonical member grammar

For `contract_version = 1`, the recursively closed `gate_b` projection uses the
following exact member grammar. This fixes serialization shape only; it does not
add a seventh requirement dimension or change the accepted Gate-B semantics.

The `gate_b` object has exactly these members:

- required `contract_version`;
- required `requirements`;
- required `effect_identity`;
- conditionally present `remote_contract`.

No member in this grammar accepts JSON `null`. A conditionally absent value is
omitted rather than serialized as `null`. Unknown members at any level fail
closed.

`contract_version` is the integer `1`. Contract and semantic-contract versions
below are unsigned non-zero 16-bit integers. Contract IDs and logical identifiers
use the existing definition-token bound: 1-128 UTF-8 bytes, no surrounding
whitespace, and no control characters.

`requirements` has exactly six members, all required:

~~~json
{
  "ambiguity_replay": "none",
  "effect_journal": "not_required",
  "inbound_protection": "none",
  "local_atomicity": "none",
  "native_message_transaction": {
    "mode": "none"
  },
  "outbound_obligation": "none"
}
~~~

Their exact version-1 spellings are:

- `local_atomicity`: `none` or `atomic_same_resource`;
- `effect_journal`: `not_required` or `required`;
- `outbound_obligation`: `none` or `outbox`;
- `inbound_protection`: `none` or `inbox_dedup`;
- `ambiguity_replay`: `none`, `best_effort`, `at_least_once`, or
  `idempotent_external_effect`;
- `native_message_transaction`: an object with either exactly
  `{"mode":"none"}`, or exactly `mode`, `semantic_contract_id`, and
  `semantic_contract_version` where `mode="required"`.

The `effect_identity` object has exactly five required members:

~~~json
{
  "derivation_contract_version": 1,
  "occurrence_key_contract_version": 1,
  "provider_key_binding_contract_version": 1,
  "request_identity_contract_version": 1,
  "scopes": []
}
~~~

`scopes` is an array canonically sorted by `id`. Each scope object has exactly
required `id` and required `slots`. Scope IDs are unique. `slots` is an array
canonically sorted by `id`; each slot object has required `id` and may have
`logical_destination_contract` only when destination identity is part of that
slot's logical obligation identity. Slot IDs are unique within a scope. When
present, `logical_destination_contract` is one bounded definition token. It is
omitted otherwise and is never `null`.

The number of scopes is at most the reader's current `MAX_NODES`; the total
number of slots across all scopes is at most its current `MAX_TRANSITIONS`.
Those ceilings are reader capability checks and are not serialized as
fingerprint members.

When present, `remote_contract` has exactly these required members and one
optional member:

~~~json
{
  "acknowledgement_contract_id": "portable-token",
  "acknowledgement_contract_version": 1,
  "semantic_profile_id": "portable-token",
  "semantic_profile_version": 1,
  "unknown_outcome_contract_id": "portable-token",
  "unknown_outcome_contract_version": 1,
  "minimum_validity_ms": 1
}
~~~

The first six members are required whenever `remote_contract` is present.
`minimum_validity_ms` is optional and, when present, is a positive unsigned
64-bit integer. It is omitted when no definition-level remote validity floor is
selected.

Canonical JSON map keys use the existing byte-sorted map encoding. Declaration
order therefore cannot change object bytes. Scope and slot arrays are
semantically unordered declarations and are normalized by their identifiers as
specified above; no other array is reordered by this ADR.

### 4. Static effect contract enters the fingerprint; dynamic effect values do not

Format 5 fingerprints the static contract needed to reconstruct and interpret
one logical effect obligation across supported restart.

The static Gate-B identity members are:

- Gate-B contract version;
- `EffectId` derivation-contract version;
- canonical `EffectScopeId` declarations;
- canonical `EffectSlotId` declarations within each scope;
- occurrence-key contract version;
- canonical request-identity contract version;
- provider-key binding/derivation contract version;
- logical destination contract identity when destination identity selects the
  obligation;
- portable remote semantic-profile version and its acknowledgement and
  unknown-outcome contract versions;
- definition-level minimum validity or deduplication floors that change replay
  or dedup interpretation.

`EffectScopeId` declarations are canonically ordered by identifier, and slots
within one scope are canonically ordered by `EffectSlotId`. Identifiers use
the existing definition-token discipline: 1-128 UTF-8 bytes, no surrounding
whitespace, and no control characters. A format-5 definition may declare at
most the runtime's current `MAX_NODES` effect scopes and at most
`MAX_TRANSITIONS` effect slots in total. Those constants are reader
capability ceilings, not bytes embedded as fingerprint members.

The following values are runtime/effect-record state and MUST NOT appear in the
definition manifest:

- a concrete occurrence-key value;
- a concrete `EffectId`;
- the concrete canonical request digest;
- a concrete provider idempotency key;
- `EffectAttempt`;
- a remote acknowledgement/reference value;
- `JobExecutionId` or `StepExecutionId`;
- process, connection, or physical-dispatch identity.

Already-durable effects are never re-derived merely because a compatible target
definition has a different fingerprint.

### 5. Remote semantics are portable contracts, not provider identity

When a selected Gate-B requirement needs remote semantics, format 5 records a
portable versioned remote contract. The contract identifies:

- the semantic profile;
- acknowledgement interpretation;
- unknown-outcome interpretation;
- any selected minimum remote-idempotency validity floor.

Each identity is a bounded definition token plus a non-zero version. These
values describe the semantic contract an adapter must satisfy; they do not name
the concrete provider implementation or deployment.

For contract version 1, `remote_contract` presence is deterministic:

- it is required when `ambiguity_replay` is not `none`;
- it is required when `native_message_transaction.mode` is `required`;
- it is omitted when both conditions are false;
- it is never emitted merely to record a provider, broker, endpoint, account,
  credential, or other deployment binding.

If both conditions require a remote contract, the one `remote_contract` object
must describe a compatible portable semantic profile for the selected
composition; two competing remote-contract authorities are invalid.

A definition-level remote validity floor is fingerprint material only when
changing that floor would change whether an existing durable effect may be
replayed or deduplicated. An effective provider or inbox window above a
definition-required floor is deployment capability and remains outside the
fingerprint.

### 6. Format-5 validation is recursively fail closed

A format-5 reader validates in this order:

1. parse JSON and reject malformed, duplicate-key, floating-point, or
   non-canonical encoding;
2. enforce the existing 64 KiB total bound;
3. require the exact root allowlist `base`, `format`, `gate_b`;
4. require outer `format = 5`;
5. validate `base` against the exact closed grammar for its declared legacy
   format;
6. validate the exact `gate_b` allowlists, types, required members, identifier
   bounds, and collection bounds;
7. reject unknown Gate-B contract/profile versions;
8. reject duplicate effect-scope or effect-slot identity;
9. validate requirement cross-field consistency;
10. only then accept the manifest as definition identity.

Cross-field and presence rules are normative:

- `idempotent_external_effect` requires `effect_journal=required`;
- `outbound_obligation=outbox` is the sole version-1 Outbox selector; there is
  no second Outbox contract member to infer or reconcile;
- `inbound_protection=inbox_dedup` is the sole version-1 InboxDedup selector;
  there is no second InboxDedup contract member to infer or reconcile;
- Outbox by itself does not imply EffectJournal, and InboxDedup by itself does
  not imply EffectJournal;
- `native_message_transaction.mode=none` forbids
  `semantic_contract_id` and `semantic_contract_version`;
- `native_message_transaction.mode=required` requires both of those members;
- `remote_contract` is required exactly under the selection predicates in
  section 5 and is forbidden otherwise;
- `minimum_validity_ms` is allowed only inside a present `remote_contract` and
  must be positive;
- duplicate scope IDs, duplicate slot IDs within one scope, or unsorted
  canonical scope/slot declarations are invalid;
- a slot whose compiled Gate-B identity contract makes logical destination part
  of the obligation must contain `logical_destination_contract`; a slot for
  which destination is not identity-bearing must omit it.

These rules do not strengthen one requirement into another. In particular,
`outbox`, `inbox_dedup`, a native message transaction, and an effect journal
remain independently selected dimensions except for the explicit
`idempotent_external_effect -> effect_journal=required` dependency frozen by
Gate-B.

A reader that recognizes outer format 5 MUST NOT ignore an unknown
correctness-relevant nested member. A later semantic extension requires a
governed recognized contract version with a corresponding closed parser, or a
later manifest format.

### 7. Legacy semantics stay separate

Format 5 does not redefine historical manifest values:

- legacy `ChunkDeliveryMode::AtomicSameResource` keeps its existing
  same-resource chunk business/progress atomicity meaning;
- legacy `ChunkDeliveryMode::AtLeastOnce` keeps its existing chunk
  writer/replay duplicate-possible meaning and does not imply the new Gate-B
  remote `AtLeastOnce` requirement;
- legacy tasklet `delivery_mode=best_effort` remains historical tasklet
  semantics and does not imply new Gate-B remote `BestEffort`.

The legacy value remains inside `base` when that historical definition shape
contains it. Structured Gate-B semantics live only in the `gate_b` object.
There is no second, implicit legacy-to-structured translation authority.

### 8. Restart compatibility remains application owned

ADR-0004 remains authoritative:

1. equal manifest digest is strict-compatible;
2. a different digest is incompatible by default;
3. a different definition is accepted only through one explicit, direct,
   application-owned compatibility edge;
4. compatibility is never inferred from matching names, revision ordering,
   additive fields, successful deserialization, or apparent semantic strength.

A legacy-format definition and a format-5 definition therefore do not restart
across each other automatically.

Gate-B adds one further condition. When an `IntentDurable`-or-later effect
exists, a compatible restart must preserve or explicitly map the source
`EffectScopeId`, `EffectSlotId`, logical destination/binding, and relevant
effect contract versions, or prove that no live source obligation requires that
continuity. Existing live effects retain their source `EffectId` and source
binding; the target definition never re-derives them under its own contract.

The concrete compatible-upgrade API/record shape for that continuity is an
implementation concern following this ADR. It must remain explicit,
deterministic, bounded, and application owned under ADR-0004.

### 9. Released legacy vectors are immutable; format-5 vectors are add-only

Formats 1-4 are release-backed compatibility artifacts.

- Format 2 retains the committed
  `LIFE-DEFINITION-001/format2-two-step.manifest.json` bytes and fingerprint
  `c0ea69669657cb8ec425801588a1f042608d8785333ad7d38d8a1f7ed5d8557f`.
- Format 3 retains fingerprint
  `f5ee7c2d6923411c8c068b6c2770b95575256833bddaed1be9c3893324c541a9`.
- Format 4 retains fingerprint
  `5588696b6bedc009b4da64689e4e9eb46adb10454c2ea2b91025850cdc68436f`.
- Format 1 has release-backed producer behavior but no dedicated pre-existing
  golden vector. Implementation of this ADR adds representative tasklet and
  chunk baselines generated from the exact `v0.7.0` producer and verifies
  current legacy emission byte-for-byte against those reference bytes.

Adding the format-1 reference baseline records already-released behavior; it
does not authorize changing the bytes to fit a new expectation.

No released format-1/2/3/4 vector may be replaced or re-pinned. Format 5
receives new vectors only, and subsequent format-5 vectors are add-only.

## Consequences

- Older runtimes fail closed on structured Gate-B definitions because the outer
  format is newer than they support.
- Existing legacy applications retain their exact released definition identity
  when they do not opt into structured Gate-B semantics.
- The format-5 wrapper duplicates the legacy projection structurally inside
  `base`, increasing manifest size, but avoids a second translation grammar
  and keeps compatibility evidence direct.
- Format-5 parsing is intentionally stricter than historical parsing. Future
  correctness-relevant fields cannot be introduced by "optional" JSON members
  that old format-5 readers ignore.
- Effect identity becomes restart-stable without turning per-occurrence,
  provider, or execution values into definition identity.
- Provider/broker adapters negotiate against portable semantic contracts rather
  than appearing in the fingerprint.
- Compatible restart involving live durable effects gains an additional
  explicit continuity proof; this prevents a definition upgrade from
  retroactively retargeting an existing obligation.
- Production manifest/API/schema work remains blocked until this ADR is
  accepted and reviewed separately.

## Alternatives considered

- **Add optional Gate-B members to formats 1-4:** rejected because a released
  reader can ignore some unknown members inside a known outer format, allowing
  semantic downgrade.
- **Rewrite or re-pin formats 1-4:** rejected because `v0.7.0` has released
  those encodings as durable compatibility material.
- **Expand `ChunkDeliveryMode` into one flat seven-value enum:** rejected
  because Gate-B requirements compose and do not form one strength ladder.
- **Reuse a legacy format when structured requirements appear equivalent:**
  rejected because equivalence would create an implicit normalization authority
  outside the manifest contract.
- **Re-spell all historical members directly at the format-5 root:** rejected
  because it creates a second copy of the legacy grammar and increases the risk
  of accidental reinterpretation or omitted format-4 semantics.
- **Hash concrete provider/broker/deployment identity:** rejected because those
  values are deployment bindings, not definition semantics, and would prevent
  legitimate deployment changes that still satisfy the same portable contract.
- **Put concrete `EffectId`, occurrence keys, or provider keys in the
  fingerprint:** rejected because those are runtime obligation state, not
  definition identity.
- **Framework-supplied legacy-to-format-5 compatibility:** rejected because
  ADR-0004 reserves compatibility claims to an explicit application-owned edge.

## Validation

Implementation and acceptance evidence for this decision must include all of
the following.

### Released legacy preservation

- add representative format-1 tasklet and chunk reference baselines generated
  from the exact `v0.7.0` producer and prove current legacy emission matches
  them byte-for-byte;
- keep the existing format-2 fixture byte-for-byte identical;
- keep the existing format-3 golden fingerprint exact;
- keep the existing format-4 golden fingerprint exact;
- prove unchanged legacy APIs continue to emit formats 1-4 rather than format
  5.

### Format-5 fail-closed boundary

- prove the `v0.7.0` reader rejects a format-5 manifest as
  `UnsupportedFormat`;
- prove the new reader continues to accept valid legacy formats 1-4;
- prove the new reader accepts a valid format-5 manifest;
- reject unknown root and nested semantic members;
- reject unknown contract/profile versions;
- reject missing required members and contradictory requirement combinations;
- reject duplicate scope/slot identity and out-of-bound declarations.

### Fingerprint semantics

- mutate every included static Gate-B semantic member independently and prove
  the format-5 digest changes;
- prove concrete occurrence values, `EffectId`, provider-key values,
  acknowledgement values, execution IDs, adapter/provider selection,
  credentials, compatible schema version, worker/pool capacity, pure timeouts,
  and telemetry do not enter canonical bytes;
- prove semantically unordered scope/slot declarations normalize to identical
  bytes;
- prove repeated compilation of an identical definition produces identical
  canonical bytes and digest.

### Compatibility and continuity

- prove legacy-to-format-5 restart without a direct compatibility edge fails
  before lifecycle mutation;
- prove a direct edge still obeys every ADR-0004 constraint;
- prove missing live-effect scope/slot/binding continuity fails closed;
- prove an existing live effect retains its source `EffectId` after a
  compatible upgrade and is never re-derived from the target definition.

### Vector governance

- never replace or re-pin a released legacy vector;
- add initial format-5 vectors after this ADR is accepted;
- keep later format-5 vectors add-only.

## Revisit triggers

Revisit this decision if:

- a valid structured Gate-B definition cannot fit within the existing 64 KiB
  manifest ceiling without removing restart-relevant meaning;
- a required Gate-B semantic contract cannot be expressed portably without
  embedding concrete provider/deployment identity;
- explicit live-effect continuity cannot compose with ADR-0004's bounded
  compatible-upgrade artifact;
- a released legacy format is shown not to reproduce its frozen producer
  projection as assumed here;
- future Gate-B semantics require an extension that cannot be represented by a
  new closed contract version without creating downgrade-by-ignore risk;
- the project adopts a different durable definition encoding whose migration
  semantics require a superseding compatibility decision.
