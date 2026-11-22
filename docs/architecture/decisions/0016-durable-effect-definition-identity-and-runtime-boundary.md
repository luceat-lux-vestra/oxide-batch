# ADR-0016: Durable-Effect Definition Identity and Runtime Boundary

- **State:** Accepted
- **Date:** 2026-11-22
- **Owners:** runtime, API, repository, and integration maintainers
- **Deciders:** project owner
- **Work item:** [#715](https://github.com/luceat-lux-vestra/oxide-batch/issues/715)
- **Supersedes:** [ADR-0015](0015-gate-b-definition-identity-and-manifest-fingerprint-projection.md)
- **Extends:** [ADR-0004](0004-job-definition-restart-compatibility.md),
  [ADR-0005](0005-compiled-execution-plan.md), and
  [ADR-0009](0009-definition-fingerprint-input-set.md)
- **Canonical M8 contract:**
  [M8 Gate-B durable-effect delivery](../../project/m8-gate-b-durable-effect-delivery.md)

## Context

ADR-0015 correctly identified that restart-relevant durable-effect semantics
need a new manifest boundary and must not be injected into released manifest
formats 1 through 4. Its first implementation, however, generalized the M8
contract beyond the semantics that the current runtime can enforce.

In particular, ADR-0015 made provider-neutral remote replay profiles,
acknowledgement contracts, unknown-outcome contracts, broker-native transaction
contracts, provider-key binding versions, and several independently versioned
identity subcontracts part of the mandatory definition model. M8 has no
provider or broker execution port that can enforce those declarations. Those
semantics belong to M9 integration adapters unless concrete cross-adapter
evidence later justifies a shared portable contract.

The current M8 runtime does have an enforceable local durable-effect boundary:
the chunk transaction can lend one transaction-scoped port that stages an
effect intent, Outbox obligation, or InboxDedup guard in the same physical
correctness transaction as business work and protected progress. The repository
descriptor can fail closed when the selected local persistence capability is not
certified.

The correction therefore keeps the concrete M8 safety floor and removes
speculative remote semantics from the initial durable definition contract.

The latest published release is v0.7.0. It emits and reads manifest formats 1
through 4. Their released bytes and digests remain immutable. Gate-B changes
merged after v0.7.0 are unreleased and may be renamed, removed, or replaced
before the next release.

## Decision

### 1. M8 owns local durable-effect correctness

The mandatory M8 durable-effect definition contract contains only semantics
that the current framework can bind to an executable chunk runtime and a
certified repository capability.

The version-1 requirement dimensions are:

| Requirement | Values | Meaning |
| --- | --- | --- |
| Effect journal | `not_required`, `required` | the chunk transaction can durably stage immutable logical effect intent |
| Outbound obligation | `none`, `outbox` | the authorizing local mutation, protected progress, and outbound obligation commit together |
| Inbound protection | `none`, `inbox_dedup` | the inbound identity marker, protected local mutation, and consumed progress commit together |

These dimensions compose. They are not a strength ladder.

Existing `ChunkDeliveryMode::AtomicSameResource` remains the single definition
authority for same-resource chunk business/progress atomicity. M8 does not add
a second `local_atomicity` field.

Remote replay modes such as best-effort, at-least-once external delivery,
provider-idempotent replay, broker-native transactions, acknowledgement
interpretation, and unknown-outcome interpretation are not part of the M8
version-1 definition contract. M9 may add them through a later versioned
extension when an executable adapter can enforce them.

### 2. Durable-effect opt-in is initially chunk-runtime only

A definition may select the M8 durable-effect contract only for effect sites
that resolve to an executable chunk step with transaction-scoped
durable-effect enlistment.

The initial supported runtime bindings are:

- a standalone `ChunkJob`; and
- a direct compiled `FlowNode::Step` whose `StepComponents` is `Chunk` and
  whose runtime binding supplies the same `ChunkTransaction` /
  `WriteContext` durable-effect enlistment path.

A graph may contain tasklet and chunk nodes together. Only explicitly bound
chunk nodes may own durable-effect slots.

The initial contract does not authorize durable-effect opt-in for:

- `TaskletJob` or `StepComponents::Tasklet`;
- custom-leaf bodies;
- deciders;
- nested-job boundaries;
- split managers;
- partition managers or workers without an explicit chunk-effect runtime
  binding; or
- any future node kind that cannot prove the same transaction-scoped
  enforcement boundary.

Compilation must reject an effect declaration that targets an unsupported node.
The framework must not serialize a durable-effect guarantee and then silently
ignore it at runtime.

### 3. Effect identity reuses compiled node identity

M8 does not introduce an unrelated public scope namespace for effect sites.

Each static effect site is identified by:

1. the existing compiled `NodeId` that owns the executable chunk step; and
2. a bounded, stable `EffectSlotId` within that node.

A standalone `ChunkJob` already compiles through a stable step/node identity;
the durable-effect contract consumes that identity rather than creating a
second scope identifier.

The effect-identity declaration is canonicalized by `NodeId`, then
`EffectSlotId`. Duplicate node declarations and duplicate slot declarations
within one node are invalid.

`EffectId` remains the opaque replay-stable runtime identity of one logical
effect occurrence. A physical attempt, wall clock, connection identifier,
random retry identifier, `JobExecutionId`, `StepExecutionId`, or payload hash
alone is never the logical effect identity.

### 4. One framework-owned effect-identity contract version

M8 version 1 uses one framework-owned effect-identity contract version.

The version selects the complete framework algorithm that combines the stable
definition/effect-site identity with a restart-stable occurrence input to
derive one logical `EffectId`. The occurrence input must be reproducible from
durable progress plus a stable component-owned occurrence key, or from an
explicit stable application operation key.

M8 does not expose separate definition versions for occurrence-key semantics,
canonical request identity, provider-key binding, or destination-contract
identity. Those axes may be introduced by a later ADR only when a concrete
consumer needs them to evolve independently.

The initial implementation must not claim version 1 support until the
derivation implementation and restart evidence exist.

### 5. Manifest format 5 remains the additive boundary

A definition that selects the M8 durable-effect contract uses manifest format
5. Definitions that do not opt in continue to emit their exact historical
format 1, 2, 3, or 4 bytes.

Format 5 has exactly three root members:

```json
{
  "base": { "...": "exact frozen legacy format-1/2/3/4 projection" },
  "durable_effects": { "...": "closed durable-effect contract" },
  "format": 5
}
```

The internal project name `Gate-B` is not a durable wire-format member.

The `base` object is the exact canonical historical projection that the same
definition shape would have emitted without durable effects. Serializing
`base` by itself must reproduce the frozen legacy bytes exactly. Format-5
validation uses a strict closed grammar for that embedded historical
projection; it does not retroactively tighten the standalone format-1/2/3/4
readers.

### 6. Format-5 durable-effect contract version 1

The `durable_effects` object has:

- required `contract_version`, exactly `1`;
- required `requirements`;
- required `effect_identity`; and
- conditionally present `inbox_dedup_minimum_validity_ms`.

No member accepts JSON `null`. Unknown members fail closed.

The exact version-1 shape is:

```json
{
  "contract_version": 1,
  "effect_identity": {
    "contract_version": 1,
    "nodes": [
      {
        "node_id": "stable-compiled-node-id",
        "slots": [
          { "slot_id": "stable-effect-slot-id" }
        ]
      }
    ]
  },
  "inbox_dedup_minimum_validity_ms": 60000,
  "requirements": {
    "effect_journal": "required",
    "inbound_protection": "inbox_dedup",
    "outbound_obligation": "outbox"
  }
}
```

`inbox_dedup_minimum_validity_ms` is allowed only when
`inbound_protection=inbox_dedup`. It is omitted when no explicit definition
floor is selected. The deployment's effective window remains outside the
definition fingerprint and is checked by capability negotiation.

The node/slot arrays are declarations, not user-significant ordering. The
producer sorts nodes by `node_id` and slots by `slot_id`; the strict reader
rejects duplicates and non-canonical persisted order.

### 7. Remote semantic contracts are deferred, not prohibited

The following concepts are deliberately outside M8 manifest contract version 1:

- generic portable semantic-contract IDs and versions;
- broker-native transaction contract IDs;
- remote semantic-profile IDs;
- acknowledgement-contract IDs;
- unknown-outcome-contract IDs;
- provider-key binding contracts;
- provider idempotency validity floors; and
- generic remote-profile parser registries.

M9 adapters must still document their exact acknowledgement, exposure,
redelivery, duplicate, native-transaction, idempotency-window, and
unknown-outcome behavior. They simply do not encode those semantics into the M8
version-1 definition contract before the runtime has a shared executable model.

A later manifest contract/version may promote a portable remote semantic layer
when concrete evidence shows at least one of:

- two or more independent adapters need the same provider-neutral semantic
  contract;
- compatible restart across adapter/provider replacement needs semantic
  identity independent of provider identity;
- a third-party extension contract cannot be represented truthfully by the
  existing capability model; or
- durable reconciliation requires a stable semantic identity finer than the M8
  local contract.

### 8. Deployment capability remains separate from definition identity

The repository descriptor remains deployment-owned and may certify:

- EffectJournal persistence profile/version;
- Outbox persistence profile/version;
- InboxDedup persistence profile/version;
- effective InboxDedup validity window; and
- bounded per-chunk effect record and encoded-byte limits.

A selected definition requirement that the deployment does not certify fails
closed before lifecycle mutation. Deployment capability values are not copied
into the definition fingerprint.

Provider endpoints, accounts, credentials, regions, concrete idempotency keys,
connection/pool settings, concrete effect attempts, and observed remote values
are never definition identity.

### 9. Public vocabulary uses domain names

`Gate-B` is a project workstream name, not a user-facing domain abstraction.
Before the next release, surviving public API names should use durable-effect,
effect, Outbox, or Inbox terminology. Existing merged Gate-B public names are
unreleased and are not compatibility constraints.

Project issues and internal project records may continue to use Gate-B as a
workstream label.

## Consequences

- M8 remains responsible for a useful, enforceable durable-effect foundation
  rather than a speculative distributed-effect protocol.
- Format 5 stays additive and fail-closed without changing released formats
  1-4.
- The current format-5 implementation branch can be substantially rewritten;
  its existing semantic shape is not an authority.
- Tasklet durable effects require a future runtime port and a later definition
  contract instead of receiving a manifest-only promise.
- M9 owns concrete remote delivery semantics first. Shared portable semantic
  contracts become evidence-driven extensions rather than prerequisites.
- The initial public surface becomes smaller and easier to support, but a later
  remote-semantic extension may require a new manifest contract version or
  format. That explicit evolution is preferred to reserving unproven fields
  now.

## Alternatives considered

### Keep ADR-0015 and only choose an open or closed profile registry

Rejected. The registry question was a symptom. The deeper issue is that the M8
runtime cannot enforce the remote semantic contracts that ADR-0015 required the
definition to declare.

### Delete all durable-effect work

Rejected. Transaction-scoped Outbox, InboxDedup, effect intent, stable
`EffectId`, and fail-closed repository capability negotiation solve concrete
batch restart/correctness problems.

### Preserve merged Gate-B API for source compatibility

Rejected. The API merged after v0.7.0 and has not been released. Preserving a
known-wrong unreleased abstraction would create permanent compatibility debt.

### Add durable-effect enlistment to tasklets now

Rejected for M8 version 1. That is a separate runtime design. The current
tasklet context owns no equivalent transaction/effect enlistment authority, so
adding only manifest declarations would be unsound.

## Validation

Implementation work following this ADR must prove at least:

- exact format-1/2/3/4 byte and digest immutability against released v0.7.0
  vectors;
- deterministic format-5 bytes and digest;
- strict closed format-5 root, base, and `durable_effects` grammars;
- canonical node/slot order and duplicate rejection;
- rejection of tasklet or otherwise unsupported effect-site bindings before
  manifest production;
- standalone chunk and directly bound flow-chunk production through the real
  compiled `NodeId`;
- local capability rejection before lifecycle mutation;
- transaction-scoped EffectJournal intent, Outbox, and InboxDedup staging
  remains in the same `ChunkTransaction` as protected business/progress work;
- no remote/provider/deployment values enter definition identity; and
- no current producer or reader accepts the superseded ADR-0015
  `gate_b.remote_contract` grammar as the new format-5 authority.

## Revisit triggers

A replacement or additive ADR is justified when:

- tasklets gain an enforceable durable-effect transaction/runtime port;
- partitioned or nested execution gains an explicit effect-site binding
  contract;
- M9 has concrete cross-adapter evidence for a portable remote semantic
  contract;
- effect-identity subcontracts demonstrably need independent version evolution;
  or
- released format-5 compatibility constrains a future extension.
