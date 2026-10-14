# M7 Gate-D — Explicit fork lineage contract (#552)

**Status:** implementation contract / evidence checklist; this document is not a claim that Gate-D has passed.
**Authoritative base:** `main@a6f98a3b8ae3d5a57c97170ce4fb770afa9b38af` (`#550` and `#551` are merged).
**Parent:** #198; **scope:** #552 only.

## Distinct operations

| Operation | Identity | Selection | Allowed state propagation |
| --- | --- | --- | --- |
| Exact restart | Existing `JobInstanceId` | Exact definition fingerprint/revision/artifact | Existing restart contract only |
| Compatible upgrade | Existing `JobInstanceId` | One registered direct edge with exact transformer revisions | Only explicitly reviewed upgrade mapping |
| Incompatible restart | Existing `JobInstanceId` | No compatible edge | Reject; no inferred compatibility |
| Fork | **New `JobInstanceId`** | Explicit fork request + exact registered target definition/artifact | **Nothing by default**; only an explicit fork transform |

A fork is a new launch, not a restart. It must not set `restart_of_execution_id`,
`upgrade_from_definition_id`, or the compatible-upgrade seed on the target execution.
Source historical definition identity and durable state are read-only.

## Proposed bounded public contract

- `ForkRequest`: caller-owned idempotency key (`OperationId`), exact source
  `JobExecutionId` and expected `ExecutionVersion`, exact target
  `DefinitionRegistration`, target `JobParameters`, and optional explicit
  `ForkTransformationExecution`.
- The target `JobInstanceKey` is derived using existing
  `JobInstanceKey::new(target_job_name, target_parameters)` semantics.
  Neither a synthetic timestamp nor a random suffix bypasses identity collisions.
- The source must be the latest committed execution in its instance and in a
  terminal, stable state. Reject stale versions, unavailable definitions or
  registry/artifact mismatches before any target mutation.
- A source-state identity contains the exact source execution ID/version and a
  domain-separated digest of an ordered, bounded projection of committed
  source step-execution IDs/versions, statuses, node IDs and validated step context envelopes. This is evidence of the source snapshot,
  **not** a token that authorizes unbounded state copying.
- The immutable `ForkLineage` stores operation ID, source execution/state
  identity, source and target definition IDs/fingerprints, target artifact ID,
  target instance ID, target execution ID, complete target-parameter digest,
  optional fork-transform contract/revisions and created-at timestamp.
  It contains no raw application values, credentials, opaque contexts or paths.
- Domain errors distinguish source missing/stale/corrupt, target registry or
  artifact absent, target instance collision, duplicate operation with
  divergent payload, transform mismatch, and newer durable schema.
  Sensitive values do not appear in `Debug`, `Display` or SQL diagnostics.

## Atomic transaction boundary

All the following happen under one repository `RepositoryUnitOfWork`:

1. Read exact `OperationId` lineage (idempotent replay or conflict).
2. Lock/validate source execution, latest-attempt identity, stable status,
   expected version, registered source definition and bounded step-state snapshot.
3. Resolve exact target registration and artifact. For a transform, validate
   all declared source/target schemas, node mappings and live revisions.
4. Select or create the target instance from the *ordinary* identifying key.
   **Existing** means collision, not permission to create a new attempt.
5. Create one first target execution, with explicit target parameters. No copied
   lifecycle, counters, ownership, operator request, context or restart marker.
6. Materialize only explicitly transformed target state (if requested).
7. Append one immutable lineage row (unique operation ID and target IDs).
8. Commit once. A dropped UoW or failed statement leaves no partial target.

This path must be explicit; do not reimplement ordinary job instance identity
or introduce compatibility inference in the fork service.

### Concurrency and unknown commit

- Concurrent identical fork requests must converge to **one** target lineage.
  Unique keys and transaction serialization arbitrate the winner.
- A request with the same operation ID but different source, version, target
  definition/artifact, target parameters or transform descriptor is rejected.
- After an ambiguous commit, **open a fresh unit of work** and read the exact
  operation ID. An immutable matching row is success; a divergent row is
  conflict; absence is unresolved and is **not** authorization for blind retry.
- A collision on target instance identity must never become a restart or
  alternate generated target key.
- Memory adapter's snapshot/CAS and PostgreSQL row locks/unique constraints
  must produce equivalent externally observable semantics.

## Transform boundary

Fork transformation is **not** `DefinitionUpgradeExecution`: the latter
authorizes a compatible restart *within the same* instance, including
restart-specific reuse of counters/state. A fork needs its own explicit
contract with bounded mapped state families and no identity/counter carryover.

Every mapped state item must be named, schema-ID/version explicit, versioned
by an exact process-local transformer revision, deterministic and side-effect
free. Reject undeclared state families, unknown source nodes, duplicate target
nodes, over-limit descriptors, missing live implementations and mismatched
revision sets. No implicit `JobExecutionContext`, `StepExecutionContext`,
checkpoint, repeat state, scope provenance, lifecycle status or attempt count.

## PostgreSQL storage and lifecycle

Introduce a schema-11 migration with a single append-only fork lineage table,
bound column sizes, FK protection for source/target identities, unique
`OperationId`, unique target instance/execution IDs, exact definition/artifact
identity and a source committed-state digest. Enforce shape and idempotency at
the database boundary. The migration must never reinterpret existing restart
or upgrade history as forks. Older runtimes must reject schema 11.

Retaining immutable lineage affects purge: test that retention cannot silently
orphan lineage records. Either preserve referenced executions under FK
restrictions, or add an explicit archival policy **outside** this operation;
do not cascade-delete history invisibly.

## Verification gates (must be evidenced before #198 closure)

| Evidence | Required assertions |
| --- | --- |
| Plain fork | New instance/execution, first attempt, exact target definition and artifact; no restart/upgrade association |
| Transformed fork | Only declared target state, exact schema/revisions and deterministic values |
| Target collision | Reject; no second attempt or hidden parameter rewrite |
| Source/registry failures | Stale/missing/corrupt source, missing definition/artifact, newer schema fail closed |
| No implicit copying | Lifecycle, attempt/start-limit, operator, ownership, opaque context and provenance absent |
| Atomicity | Injected failures after selection, execution, transform and lineage all roll back |
| Concurrency/recovery | Duplicate fork, divergent replay, process kill before/after commit, unknown commit exact readback |
| PostgreSQL | PostgreSQL 15 + 18, least privilege, populated upgrade, dump/restore, migration reapply, schema-11 older-runtime rejection, bounds/redaction |
| Final Gate-D | Exact restart, explicit compatible upgrade, incompatible rejection, explicit new-lineage fork in one matrix |

#198 and #552 remain OPEN until both adapter parity and the process-kill/
migration/security/rollback matrix pass on an exact candidate HEAD.
