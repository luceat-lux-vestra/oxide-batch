# M8 Gate-B — Durable Effects, Idempotency, and Delivery Semantics

**State:** Proposed canonical record; P133-P139 semantics are frozen in Gate-B
[#644](https://github.com/luceat-lux-vestra/oxide-batch/issues/644), while
merge and final Gate-B closure remain separate work.
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
the [Gate-A repository portability record](m8-gate-a-repository-portability.md),
and the [Gate-C retention portability record](m8-gate-c-retention-portability.md).

This record consolidates Gate-B checkpoints P133-P139 into one portable
correctness contract. PostgreSQL may become the first implementation of some
primitives, but PostgreSQL tables, SQL, client behavior, and transaction
mechanics do not define the portable semantics.

It is a design contract, not implementation or support evidence. It adds no
public API, repository capability enum, schema/table, migration, manifest
member, fingerprint byte, broker adapter, workflow, or support claim.

## Design principles

Gate-B follows five rules.

1. **Unknown stays unknown.** A timeout, disconnect, missing acknowledgement,
   process crash, or absent local acknowledgement never proves that a remote
   effect did not happen.
2. **Local and remote authority are distinct.** Repository metadata proves local
   intent, fencing, acknowledgement evidence, and local disposition. Only the
   remote authority or its certified idempotency/query contract proves the
   corresponding remote state.
3. **No generic cross-resource exactly-once.** Stronger semantics exist only
   inside the exact resource and profile that certifies them.
4. **Guarantees compose explicitly.** Outbox, inbox/dedup, broker transactions,
   and idempotent external effects are semantic requirements/profiles, not a
   single strength ladder.
5. **Capability absence fails closed.** Compilation, launch, restart, or the
   feature operation rejects an unsatisfied requirement instead of silently
   selecting a weaker delivery guarantee.

## Same-resource versus cross-resource authority

### Atomic same resource

`AtomicSameResource` is the fast path where the business mutation and the
framework progress it protects participate in one adapter-owned transaction.
For a chunk this includes the checkpoint/context/counters/version and every
selected local obligation that correctness requires in that transaction.

The acknowledged commit of that resource is the authority. If commit
acknowledgement is ambiguous, the existing `CommitOutcomeUnknown` contract
applies and a fresh healthy repository observation is required. Gate-B does not
translate that ambiguity into a fictional remote-effect result.

A writer on another connection, an HTTP call, a broker send, or any operation
that cannot enlist in that same commit is cross-resource even when it uses the
same vendor or logical application.

### Cross-resource effects

Cross-resource effects have at least two authorities:

- the local durable authority, which records the logical obligation and its
  recovery state;
- the external resource, which owns the truth about whether the remote effect
  exists or was accepted when that resource exposes a reliable contract.

Neither side silently rewrites the other. Remote evidence is consumed only
after it is durably bound to the logical effect.

## Logical effect identity

`EffectId` is the conceptual replay-stable identity of one logical external
effect obligation. This record does not add a public Rust type.

An EffectId:

- is immutable once durable intent exists;
- survives process retry and supported restart for the same logical obligation;
- does not change merely because a physical dispatch attempt changes;
- is not a payload hash, wall clock, connection ID, random per-attempt request
  ID, `JobExecutionId`, or `StepExecutionId`;
- differs for distinct logical obligations even when their payload bytes match.

An `EffectAttempt` is physical provenance underneath one EffectId. Attempts
must be monotonically distinguishable but are never the deduplication identity.

For chunk writers, a transient chunk number or raw item ordinal is insufficient
by itself. The occurrence identity must be reconstructible from restart-stable
logical source/effect identity: durable progress plus a stable component-owned
occurrence key, or an explicit application operation key. A component that
cannot reproduce such an identity cannot claim a delivery profile whose
correctness depends on deduplication or idempotent replay.

## Durable effect state

Profiles that claim durable effect recovery use this semantic state model.

| State | Durable meaning |
| --- | --- |
| `NoRecord` | no durable effect obligation exists |
| `IntentDurable` | EffectId, destination/operation identity, request identity, and any required provider-key binding are durable |
| `ExposureGranted` | one physical attempt has been durably authorized to cross the local boundary; from this point the remote effect is conservatively treated as possibly applied |
| `Acknowledged` | bounded certified acknowledgement/remote evidence has been durably recorded |
| `Terminal` | the final local disposition allowed by the selected delivery requirements is durable |

`ExposureGranted` is an authorization fence, not proof that bytes reached the
network. A crash immediately after this transition therefore creates a
deliberate conservative false-positive ambiguity. That is safer than allowing a
remote effect with no durable local trace.

State is monotonic under compare-and-swap, idempotent append, or equivalent
serialization. A later attempt cannot erase an earlier exposure or
acknowledgement.

Two ambiguity classes remain separate:

- **repository outcome unknown:** the local state/progress transaction may or may
  not have committed; resolve it by a fresh repository read before any remote
  call;
- **external effect outcome unknown:** exposure is durable and the remote effect
  may or may not have happened; resolve it only through the selected delivery
  profile's safe replay/query contract or explicit reconciliation.

## Delivery requirements are composable

RFC-0007's delivery vocabulary names semantic profiles. It must not be
implemented as an assumption that every plan selects exactly one value from one
flat enum.

The current public `ChunkDeliveryMode` remains the compatibility surface it is
today (`AtomicSameResource` and `AtLeastOnce`) until a later implementation
decision changes it. Gate-B does not add enum variants.

| Requirement/profile | Portable guarantee and non-guarantee |
| --- | --- |
| `AtomicSameResource` | one resource commit owns business mutation plus protected framework progress; no cross-resource claim |
| `TransactionalMessage` | only the exact broker-native operations named by the adapter profile are atomic; unrelated repository work is not implied; an unknown broker commit requires the adapter's certified safe-retry rule or reconciliation rather than a generic resend |
| `Outbox` | the authorizing local mutation and outbound obligation commit together; later dispatch may duplicate |
| `InboxDedup` | the stable inbound identity, protected local mutation, and consumed progress commit together; duplicate suppression lasts only for the certified marker window |
| `IdempotentExternalEffect` | automatic replay is allowed only with the same certified remote key/request identity and within the provider contract's validity |
| `AtLeastOnce` | retry after ambiguity is permitted because duplicates are part of the contract |
| `BestEffort` | no default automatic retry after possible exposure; reconciliation/manual handling may be required |

Valid compositions include, for example, Outbox plus
IdempotentExternalEffect, and InboxDedup protecting local work that also
creates an Outbox obligation. An implementation should normalize the required
semantics into orthogonal requirement dimensions rather than hash or negotiate
an arbitrary adapter capability superset.

## Persistence and transaction ownership

Gate-B defines three optional persistence semantics. Exact trait names and
schema layout are deferred.

### Effect journal

An effect journal persists EffectId state, canonical request identity,
provider-key binding where required, attempt provenance, acknowledgement,
reconciliation evidence, and terminal disposition.

Durable intent exists before first external exposure. If a local business or
framework mutation creates an effect obligation whose correctness requires that
the obligation cannot be lost, the intent and that authorizing mutation must
commit in one certified local transaction, or the authorizing mutation/progress
must remain uncommitted and unadvanced until durable intent is proven. A
best-effort dual write to an independent journal cannot claim the stronger
no-lost-obligation contract.

`ExposureGranted` is committed before the remote call. If that commit is
unknown, no remote call may occur until a healthy repository read resolves the
local state.

The effect journal owns local evidence only; it is not proof that the remote
effect is present or absent.

### Outbox

Outbox correctness depends on the transaction that authorizes the outbound
obligation.

For chunk processing, the Outbox obligation must be able to join the same
`ChunkTransaction` boundary that commits the protected business writes and
checkpoint/progress. Placing an Outbox insert only in an independent
`JobRepository` unit of work before or after the business transaction does not
satisfy the Outbox profile.

The dispatcher consumes committed obligations. Multiple dispatchers use durable
claim/CAS/fencing or equivalent serialization for physical-attempt ownership,
but Outbox alone still permits duplicate remote delivery after ambiguity.

### Inbox/dedup

InboxDedup likewise requires the inbound marker, protected local business
effect, and consumed framework progress to commit in the same certified local
transaction. For a chunk-backed consumer, this requires integration with the
same `ChunkTransaction` boundary; a marker written independently before the
business effect can lose work, and a marker written independently afterward can
repeat work.

Concurrent deliveries serialize through durable uniqueness/CAS/transactional
conflict. Matching identity plus matching request identity returns the recorded
disposition. Matching identity with a conflicting request is rejected.

For a non-transactional broker acknowledgement, ack/delete/offset advancement
must not become durable before the Inbox marker, protected effect, and consumed
progress commit when that ordering could lose the message. If that local commit
is unknown, a fresh repository read resolves the Inbox authority before either
reprocessing or acknowledging the delivery.

### Capability declaration

Effect-journal, Outbox, and InboxDedup persistence are separate semantic
capabilities even if one backend stores them in one physical journal/table or
collection. Declaring a capability means certifying its atomicity, concurrency,
unknown-outcome behavior, migration/restore preservation, retention, and
resource bounds. Merely having a table with a familiar name is insufficient.

Runnable-core repository support requires none of the three. Plans that need
one fail closed on an adapter that does not declare and certify it.

The minimum semantic mapping is:

- `IdempotentExternalEffect` requires effect-journal persistence plus a
  certified remote idempotency contract;
- `Outbox` requires Outbox persistence; effect-journal persistence is added
  only when the selected composition also needs effect-level remote
  reconciliation;
- `InboxDedup` requires InboxDedup persistence plus a restart-stable inbound
  identity in the authenticated producer/stream/message namespace;
- `TransactionalMessage` relies on the named broker-native transaction
  contract and does not imply unrelated repository persistence;
- `AtLeastOnce` and `BestEffort` do not require an effect journal merely by
  name, although an application may still choose one for provenance.

## Idempotency-key lifecycle and request identity

The framework EffectId and a remote provider idempotency key are distinct.

A remote key is bound to exactly one EffectId and one immutable canonical
request identity inside a reviewed provider key namespace. That namespace
covers the logical application/tenant scope, provider/destination identity,
non-secret provider account or credential scope, operation/endpoint semantic
class, and environment/API version when the provider's uniqueness rules depend
on them. The binding is durable before exposure and cannot be rotated after
exposure to escape an unknown outcome.

A key may be application-supplied under a validated deterministic contract or
framework-derived from stable logical identity using a versioned,
domain-separated derivation. It must not be derived from wall clock, physical
attempt identity, or payload hash alone.

Canonical request identity is versioned and deterministic. It covers every
effect-relevant semantic field and excludes only fields proven effect-neutral,
such as a transport retry counter or trace identifier.

Reuse rules are fail-closed:

- same EffectId/key/request may replay only when the selected delivery profile
  permits it and the remote validity contract is still certified;
- same key with a different request identity is a conflict;
- a different EffectId must not deliberately reuse the same key namespace;
- same payload with a different EffectId is a distinct logical obligation;
- local commit ambiguity is reconciled before another exposure.

A provider key whose validity is expired, evicted, or unprovable is not
equivalent to "never applied." An exposed ambiguous effect is not blindly
resent with either the old key or a new key.

### Digest confidentiality

The full canonical request-identity digest is correctness metadata, not
automatically a public diagnostic field. Low-entropy request attributes can make
an ordinary digest useful for offline guessing.

Repository/internal conflict detection may retain the versioned identity digest
under the protected metadata boundary. Operator/telemetry surfaces expose only
a reviewed non-secret evidence identifier or fingerprint when needed. They do
not expose the canonical preimage, raw provider key, or an unreviewed digest.
A future keyed digest/HMAC design must separately define key rotation and
restore semantics before becoming durable format.

## Retention and minimal tombstones

There is no universal fixed TTL.

The effective safety window is bounded by the provider's conservative key/dedup
validity, client retry/restart horizon, broker redelivery horizon, in-flight
attempt/ack lag, reconciliation window, and cleanup safety margin.

Unresolved `IntentDurable`, `ExposureGranted`, or acknowledged-but-not-final
effects are not deleted merely because time elapsed. The minimum identity,
binding, ambiguity, and reconciliation evidence survives until resolution or an
explicit audited disposition.

After an explicit disposition and after every advertised replay/dedup window
ends, an implementation may compact a full effect record to a minimal tombstone
rather than retain unbounded payload/history. The tombstone must preserve enough
authority to prevent unsafe key/effect reuse and explain the remaining
ambiguity/disposition, including at least the logical effect identity, protected
request/key fingerprint or non-reuse fence, disposition class, and applicable
identity/profile version.

Generic Gate-C history purge has independent authority and must not cascade away
the only live Gate-B effect/outbox/inbox evidence. Gate-C owns generic
retention/archive operations; Gate-B owns the minimum effect/dedup survival
floor.

## Operator reconciliation and audit

Telemetry is never correctness authority. Logs, spans, metrics, and diagnostic
bundles may be lost or duplicated around a durable transition.

Bounded effect inspection may expose reviewed fields such as opaque EffectId,
delivery profile, durable state, attempt count/ordinal, bounded component and
adapter class, contract versions, timestamps, bounded outcome/reconciliation
class, and opaque failure identity. It excludes credentials, raw provider keys,
request/response bodies, endpoints, SQL, business records, arbitrary remote
error text, and unreviewed request digests.

Effect list/query surfaces follow the existing explorer rules: keyset
pagination, immutable ordering identity, statement/service deadline, and no
unbounded scans. The Gate-B diagnostic floor uses a default page size of 50, a
maximum page size of 500, and a complete encoded-response ceiling of 256 KiB.
Mutable-state filters are evaluated at page read time and do not imply a
cross-page snapshot.

EffectId may be used as a bounded correlation field in logs/spans, but EffectId,
provider-key fingerprints, canonical request digests, OperationId, ActorRef,
evidence digests, attempt identity, remote references, business identifiers,
and arbitrary reason/error text must not become metric labels. Existing
observability cardinality budgets remain authoritative.

A mutating reconciliation request conceptually reuses the existing operator
envelope: ActorRef, bounded ReasonCode, OperationId, RequestDigest,
ExpectedVersion, and an evidence/proposal digest. Deployment authentication and
authorization remain outside the core.

Accepted reconciliation appends immutable audit evidence and compare-and-swap
advances local effect state in one local transaction. Rejected mutation
attempts also append bounded audit evidence but do not change effect state.
The audit record remains value-redacted and contains only reviewed bounded
identity, version, disposition/outcome, reason, request/evidence digest, actor,
and timing classes.

The reconciliation transaction does not blindly call the remote system and it
does not silently rewrite job/execution lifecycle. A later retry/dispatch is a
separate action governed by the selected delivery requirements and the durable
post-reconciliation state.

Semantic dispositions may confirm applied/acknowledged, confirm not applied when
the provider contract positively proves absence, keep the effect unresolved, or
abandon automatic replay while retaining ambiguity. Timeout, 404 without a
certified absence contract, missing acknowledgement, or missing local evidence
are insufficient proof of non-application.

Execution recovery remains a separate audited lifecycle authority.

## Definition/plan identity and deployment negotiation

Gate-B follows ADR-0009: only values that select or reinterpret durable state
belong in the definition fingerprint.

Restart-relevant Gate-B semantics include, when selected:

- normalized delivery requirements;
- required effect-journal/outbox/inbox persistence semantics;
- EffectId occurrence/derivation contract version;
- canonical request-identity contract version;
- provider-key ownership/binding/derivation contract version;
- the required remote semantic profile and acknowledgement/unknown-outcome
  contract;
- any definition-level validity floor whose interpretation changes whether an
  existing effect may be replayed or deduplicated;
- stable logical destination/effect-slot identity when it selects the logical
  obligation.

Deployment/runtime-only values include concrete repository/provider
implementation, endpoints, region, account, credentials, concrete per-effect
key bytes, observed provider TTL, compatible repository schema version, pool or
worker counts, pure throughput/concurrency knobs, pure transport timeouts,
telemetry configuration, and runtime process identity.

Fingerprint exclusion is not permission to retarget existing effects.
`IntentDurable` and later states retain their original provider/destination/key
binding. A deployment change may service future unbound effects only when it
satisfies the same compiled semantic requirement.

Strict restart first checks normal definition compatibility and then negotiates
live capabilities. Compatible restart cannot retroactively strengthen or weaken
already-durable effects. Existing source effects retain their source contract;
a changed identity/canonicalization/key-binding contract needs explicit
continuity for every live obligation or restart fails closed.

### ADR-0009 implementation blocker

Current canonical manifests already hash the existing delivery mode, and
ADR-0009 requires a superseding ADR for any later change to the fingerprint
input set.

Therefore no implementation may add Gate-B requirement fields to manifest bytes
or silently re-pin golden fingerprints until a superseding/addendum decision
names the exact normalized member set, encoding, legacy-format behavior, and
compatibility/migration policy. This record intentionally does not choose a new
manifest format or public API shape.

## M9 integration handoff

M8 Gate-B owns delivery correctness. M9 owns transport/provider mechanics.

Every M9 effect/message adapter profile must declare the exact semantics it maps
to Gate-B, including:

- stable message/effect identity source and scope;
- acknowledgement meaning and exposure boundary;
- offset/cursor/visibility ownership;
- redelivery, duplicate, rebalance, ordering, and partition behavior;
- native transaction scope;
- known-not-applied, known-applied/acknowledged, and unknown-outcome mapping;
- idempotency/dedup key scope and validity when claimed;
- remote query/reconciliation support when claimed;
- required Gate-B persistence capabilities;
- checkpoint/progress ordering relative to acknowledgement or send;
- bounded buffering, concurrency, in-flight work, message size, timeout, and
  cancellation behavior;
- poison/dead-letter policy;
- redaction and supported remote product/protocol versions.

Broker-native "exactly once", publisher confirms, visibility timeouts, finite
dedup windows, or HTTP status codes are accepted only for the exact certified
scope. They never create a blanket cross-resource exactly-once claim.

Object-storage and HTTP readers do not require an effect journal merely because
they are integrations. They use the ordinary checkpoint/restart/resource
contract unless their operation actually performs duplicate-sensitive external
mutation.

M9 item integrations remain distinct from M11 distributed worker transports
even when both use the same broker product.

## Certification floor

Every claimed Gate-B persistence or M9 integration profile eventually requires
deterministic evidence for the semantics it advertises.

The shared suite includes:

- stable logical identity across restart/redelivery;
- conflicting identity/request reuse;
- crash before intent/local commit, after local commit, after exposure, after
  remote response, and before/after acknowledgement persistence;
- repository commit ambiguity and remote-effect ambiguity;
- lost acknowledgements, redelivery, duplicate send, and concurrent
  consumer/dispatcher behavior;
- idempotency-key replay within and outside the certified validity window;
- exact transaction scope and rollback;
- checkpoint/progress versus acknowledgement ordering;
- schema/profile migration and backup/restore preservation;
- bounded buffers, queues, in-flight work, retry, payload size, and
  cancellation;
- sentinel redaction across errors, projections, audit, logs, spans, metrics,
  and bundles.

M9 additionally runs provider/broker-specific destructive/crash/redelivery
fixtures against the exact remote versions it claims. Infrastructure absence,
mock-only tests, or generic read/write success cannot certify a delivery
profile. Evidence records exact adapter/source revision, client and remote
versions, profile descriptor, fault-fixture version, normalized result, resource
report, redaction report, and known limitations.

Support promotion remains versioned and capability-specific. Passing one
profile does not certify another profile or remote version.

## Implementation blockers and ordering

Gate-B semantics are sufficiently frozen for canonical documentation, but
implementation decomposition remains blocked on these decisions/artifacts:

1. accept this canonical record and close Gate-B only after cross-document
   review;
2. choose the implementation shape for composable delivery requirements without
   expanding the existing flat `ChunkDeliveryMode` by assumption;
3. freeze exact port placement so Outbox/Inbox atomic records can participate in
   the same `ChunkTransaction` that owns protected business/progress state;
4. accept the ADR-0009 follow-up before changing manifest/fingerprint inputs;
5. define schema/migrations and adapter declarations only after the portable
   port/capability shape is accepted;
6. add reusable deterministic certification suites before any capability or
   support promotion;
7. let M9 #202 close its shared envelope/dead-letter/profile gates before
   adapter-specific implementation issues.

No implementation issue may decide these semantics locally in a PR.

## Gate-B acceptance checklist

The consolidated design covers:

- durable logical effect identity and physical attempt identity;
- same-resource versus cross-resource authority;
- effect state and crash boundaries;
- precise delivery requirements and their composition;
- automatic retry versus reconciliation rules;
- idempotency key, request identity, conflict, collision, and validity;
- optional effect-journal/outbox/inbox persistence ownership;
- bounded/redacted diagnostics, operator reconciliation, and append-only audit;
- effect retention and Gate-C interaction;
- definition/plan identity versus deployment-only values;
- M9 handoff and certification floor.

This checklist is design coverage only. It does not claim implementation,
release-backed support, or ledger promotion.
