# ADR-0017: Canonical Compiled-Plan Manifest v1 and Pre-1.0 Preview Reset

- **State:** Accepted
- **Date:** 2026-11-22
- **Owners:** runtime, API, repository, and release maintainers
- **Deciders:** project owner
- **Work item:** [#716](https://github.com/luceat-lux-vestra/oxide-batch/issues/716)
- **Extends:** [ADR-0005](0005-compiled-execution-plan.md)
- **Partially supersedes:** [ADR-0004](0004-job-definition-restart-compatibility.md)
  for historical preview-manifest encoding/reader compatibility only.
- **Partially supersedes:** [ADR-0009](0009-definition-fingerprint-input-set.md)
  for formats 1-3 preservation/golden-vector requirements only.
- **Partially supersedes:** [ADR-0013](0013-m7-advanced-flow-scope-repeat-and-evolution.md)
  for format-4 and formats-1-3 readability/preservation requirements only.
- **Partially supersedes:** [ADR-0016](0016-durable-effect-definition-identity-and-runtime-boundary.md)
  for the immutable-formats-1-4 and additive-format-5 representation only.
- **Canonical architecture:** [Execution-plan architecture](../execution-plan.md)
- **Canonical correctness:** [Execution semantics](../../compatibility/execution-semantics.md)

## Context

OxideBatch accumulated multiple definition-manifest formats while the execution
model was still evolving:

- format 1 for one-step tasklet/chunk definitions;
- format 2 for compiled flow;
- format 3 for local-scale flow;
- format 4 for advanced flow/scope/repeat; and
- an unreleased format-5 design that wrapped an exact historical format-1/2/3/4
  object and added durable-effect semantics.

That representation history became an architectural constraint even though the
project is still pre-1.0 and has no external user or production definition
metadata that must remain restartable across the next preview line.

The released v0.7.0 line is a 0.x preview. The accepted support policy permits
an incompatible minor release when the break is explicit and migration guidance
is provided.

Preserving unused preview formats would require:

- multiple producer paths;
- multiple readers;
- recursive compatibility parsing;
- wrapper grammars for new semantics;
- migration/compatibility edges for data no user currently needs preserved; and
- long-term tests for representations that are no longer the target model.

That cost is not justified before adoption.

The architectural source of truth is now the normalized
`CompiledExecutionPlan`. Definition identity should therefore be projected
from one normalized plan model rather than from the historical path by which a
builder reached that plan.

## Decision

### 1. One canonical definition manifest

OxideBatch adopts one canonical compiled-plan definition manifest.

Every supported job form:

- `TaskletJob`;
- `ChunkJob`;
- ordinary flow;
- local-scale flow; and
- advanced flow

first lowers/compiles to the normalized `CompiledExecutionPlan`, then projects
definition identity from that same normalized plan model.

One-step jobs are one-node compiled plans. They do not own a separate durable
manifest grammar.

### 2. Canonical manifest version 1

The new grammar uses a single version member:

```json
{
  "manifest_version": 1,
  "job": "daily-import",
  "entry": "load",
  "nodes": [],
  "transitions": []
}
```

The root has exactly these universally required members:

- `manifest_version`;
- `job`;
- `entry`;
- `nodes`; and
- `transitions`.

The following members are present only when the corresponding
restart-relevant semantic is selected:

- `restart_policy`;
- `parameter_incrementer`;
- `scoped_components`; and
- `durable_effects`.

Other restart-relevant members may be introduced only through a governed
manifest change. Unknown members fail closed in the current version.

The grammar is canonical JSON and remains subject to the framework manifest
size/depth/collection bounds.

### 3. The manifest is projected from the normalized plan

The canonical projection represents all currently accepted restart-relevant
compiled-plan semantics, including:

- tasklet and chunk step declarations;
- component/checkpoint/context identity and versions;
- fault/retry/skip/repeat semantics;
- transition patterns and durable decision identity;
- split/join/partition structure;
- nested flow/job structure;
- custom leaf and decider identity;
- start/restart controls;
- parameter incrementer identity;
- scoped-component and late-binding identity; and
- optional M8 durable-effect semantics.

A builder-specific representation must not create a different fingerprint when
it lowers to the same normalized plan and restart semantics.

### 4. Fingerprint semantics are preserved

The definition fingerprint remains SHA-256 over the exact canonical manifest
bytes.

ADR-0009's core input rule remains authoritative:

> include values that select or reinterpret durable restart state; exclude
> values that affect only deployment, capacity, throughput, telemetry, or
> diagnostics.

Framework capacity ceilings are compile/read capability, not manifest
fingerprint material.

Equivalent normalized definitions produce identical bytes and fingerprint.
Changing a restart-relevant semantic changes the fingerprint.

### 5. Preview formats 1-4 are not a compatibility requirement

The next eligible pre-1.0 minor line does not read formats 1-4 as supported
definition manifests.

There is no format-1/2/3/4-to-v1 manifest migration reader.

There is no compatibility edge that transforms a historical preview manifest
into canonical manifest v1.

Definitions/executions persisted with historical preview manifests are not
restartable under the new line.

When old OxideBatch definition/execution metadata exists, upgrade guidance
requires resetting/recreating the OxideBatch-owned preview metadata before
running the new line.

This reset does not imply deletion of application/business data outside
OxideBatch metadata.

### 6. Legacy bytes fail explicitly

The new reader may recognize enough of a historical preview root such as the
old `format` member to return a deterministic legacy-preview-manifest
unsupported error.

That recognition is diagnostic only.

It must not:

- parse historical semantics for execution;
- project historical bytes into canonical v1;
- infer compatible restart; or
- silently reinterpret an old document as v1.

All rejection occurs before lifecycle mutation.

### 7. Proposed format 5 is abandoned

Manifest format 5 is never released.

The old design:

```json
{
  "base": { "...": "format 1/2/3/4" },
  "format": 5,
  "gate_b": {}
}
```

is not part of the new architecture.

The superseded #714 branch may be used only as reference for generic parsing or
test techniques. Its compatibility model is not incrementally patched into the
new implementation.

### 8. Durable effects are an optional canonical-v1 member

ADR-0016's local durable-effect semantics remain accepted.

`durable_effects` is an optional member of canonical manifest v1, not a
wrapper around legacy manifest bytes.

M8 version 1 continues to define only:

- EffectJournal intent;
- Outbox obligation;
- InboxDedup protection;
- conditional `EffectId`;
- compiled `NodeId` + `EffectSlotId` effect sites; and
- fail-closed local repository capability negotiation.

Remote replay, acknowledgement, provider-idempotency, broker-native
transaction, and unknown-outcome contracts remain M9-or-later concerns.

### 9. EffectId uses the new canonical definition identity

EffectId derivation uses the canonical-v1 definition fingerprint.

The semantic tuple contains:

- exact canonical definition fingerprint;
- canonical logical job-instance identity;
- compiled `NodeId`;
- `EffectSlotId`; and
- restart-stable logical occurrence input.

The existing versioned `JobInstanceKey::digest()` is the preferred
logical-instance identity input unless implementation review finds a concrete
correctness reason to introduce another encoding.

Physical attempts, execution IDs, wall clock, connection identity, random retry
IDs, and payload hash alone are not logical effect identity.

Exact EffectId domain separation, byte framing, digest/derivation algorithm,
and golden vectors must be frozen before support is claimed.

### 10. Restart semantics remain fail closed

ADR-0004's core restart model remains:

- exact fingerprint restart is compatible;
- different fingerprint is incompatible by default;
- compatibility requires one explicit governed direct edge;
- compatibility is not inferred from names, versions, or deserialization; and
- drift is rejected before lifecycle mutation.

The preview reset changes which historical manifests the runtime supports; it
does not weaken those rules for canonical v1.

M8 durable-effect definitions remain stricter: a different-digest compatible
restart involving durable-effect semantics fails closed until a later ADR
defines explicit live-effect continuity/mapping.

A fork creates a new lineage and does not copy EffectJournal, Outbox,
InboxDedup, or EffectId state by default.

### 11. Post-reset compatibility baseline

Canonical manifest v1 becomes the new baseline after implementation and release.

The runtime must never silently reinterpret persisted canonical-v1 bytes.

A future change must be one of:

1. an implementation change that preserves exact canonical bytes/meaning;
2. an explicit compatible definition-upgrade edge within the same grammar where
   restart semantics permit it; or
3. an explicitly governed breaking manifest/version reset.

Do not add wrapper grammars solely to preserve unused preview representations.

### 12. Repository profile-version compatibility is explicit

Manifest version, EffectId derivation version, repository descriptor version,
and local persistence-profile versions are independent namespaces.

Initial M8 recognition is:

- EffectJournal requirement accepts framework EffectJournal persistence profile
  v1;
- Outbox requirement accepts framework Outbox persistence profile v1;
- InboxDedup requirement accepts framework InboxDedup persistence profile v1.

Unknown local persistence profile versions fail closed.

Numeric ordering does not imply compatibility. A later runtime accepts an
additional profile version only through an explicit framework-owned
compatibility mapping.

These deployment profile versions are not serialized into the manifest.

The current descriptor-wide Gate-B contract-version equality relation is not a
valid compatibility rule and must be removed during implementation.

### 13. Metadata schema is a separate concern

This decision does not automatically bump the repository metadata schema.

If implementation review concludes that a schema bump is the cleanest way to
prevent mixed historical/new definition metadata, that is a separate bounded
decision with migration/restore evidence.

The manifest reset itself does not require an old-manifest migration.

### 14. Release and upgrade communication is mandatory

The source cutover and its user-facing release documentation land together.

The release/update documentation must state:

- this is an explicit pre-1.0 breaking definition-metadata change;
- old preview definition/execution restart is unsupported;
- old OxideBatch preview metadata must be reset/recreated before first use of
  the new line when such rows exist;
- application/business data is not implicitly deleted;
- no old-format-to-v1 manifest migration exists; and
- legacy preview manifests are rejected explicitly by the new runtime.

The accepted support policy itself does not need amendment because it already
permits an explicit incompatible 0.x minor release with migration guidance.

## Partial supersession details

### ADR-0004

ADR-0017 supersedes only:

- the historical preview manifest-format encoding details;
- the requirement that those preview formats remain readable by later preview
  runtimes; and
- the assumption that old preview manifests can participate in future restart.

ADR-0004 remains authoritative for definition identity, immutable persisted
meaning, SHA-256 fingerprinting, exact-digest restart, explicit compatibility
edges, and fail-closed drift.

### ADR-0009

ADR-0017 supersedes only:

- formats-1/2/3 preservation as future runtime support;
- old golden-vector immutability as a requirement on the new runtime; and
- the assumption that no later pre-1.0 manifest reset occurs.

ADR-0009's fingerprint input-set rule remains authoritative.

### ADR-0013

ADR-0017 supersedes only:

- format 4 as the current durable manifest family;
- formats 1-3 readability/immutability requirements for the new line; and
- historical no-replacement language for preview manifests.

ADR-0013's advanced flow, scope, repeat, definition-evolution, and ordered
repository-schema decisions remain authoritative.

### ADR-0016

ADR-0017 supersedes only:

- formats 1-4 as immutable future runtime compatibility artifacts;
- additive format-5 wrapping;
- the legacy `base` grammar; and
- base-to-effect cross-validation required only by that wrapper.

ADR-0016's local durable-effect semantics, runtime boundary, effect-site
identity, version-family separation, capability-negotiation boundary,
strict-restart rule for durable effects, and M9 deferral remain authoritative.

## Consequences

- Definition identity becomes simpler and aligned with the normalized execution
  model.
- One-step and graph definitions stop carrying separate manifest histories.
- The implementation may remove historical format constants/readers/golden
  vectors after the source cutover is complete.
- Existing preview metadata becomes intentionally non-restartable across the
  reset.
- The project takes a deliberate pre-adoption breaking change instead of
  carrying unused compatibility machinery toward 1.0.
- Release notes and upgrade guidance become mandatory for the cutover.
- M8 durable effects integrate directly into the canonical manifest rather than
  creating another format family.

## Alternatives considered

### Preserve formats 1-4 and add format 5

Rejected. It preserves data no current user needs while imposing recursive
legacy parsing and wrapper complexity on all future evolution.

### Keep old readers but emit only canonical v1

Rejected. That still creates indefinite support/testing obligations for unused
preview data and allows mixed metadata states the project does not need to
support.

### Migrate old manifests into v1 on startup

Rejected. There is no user requirement for that migration and it would require
semantic equivalence proofs across historical representations.

### Remove versioning entirely

Rejected. A versioned grammar is still required once real persisted v1 data
exists and future runtimes need deterministic interpretation/rejection.

### Reset all application data

Rejected. The break concerns OxideBatch-owned preview definition/execution
metadata, not arbitrary business/application data.

## Validation

Implementation following this ADR must prove at least:

- TaskletJob and ChunkJob lower to the same canonical plan projection used by
  graph jobs;
- all current restart-relevant advanced-flow semantics are represented;
- equivalent normalized definitions produce identical canonical bytes;
- throughput/capacity-only changes do not change fingerprint;
- restart-relevant changes do change fingerprint;
- canonical v1 round-trips through the new strict reader;
- unknown/current-invalid members fail closed;
- historical preview manifests fail with an explicit unsupported-preview
  result before lifecycle mutation;
- no historical compatibility parser/migration is used for execution;
- exact canonical-v1 restart remains valid;
- ordinary compatible edges remain explicit and direct;
- durable-effect different-digest restart remains fail closed;
- durable effects are an optional member rather than a wrapper;
- local persistence profile recognition follows the explicit per-family mapping;
  and
- source cutover includes changelog/support/upgrade documentation.

## Revisit triggers

Revisit when:

- canonical v1 cannot express an accepted execution semantic without ambiguity;
- production adoption creates a concrete support obligation for persisted v1
  data;
- a later grammar change cannot be represented safely by compatible
  definition evolution;
- metadata isolation requires an explicit schema-version boundary; or
- M14 stable compatibility policy requires an N/N-1/N-2 manifest support model.
