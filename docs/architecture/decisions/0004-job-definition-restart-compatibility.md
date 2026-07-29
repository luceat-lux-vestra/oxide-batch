# ADR-0004: Job-definition Identity and Restart Compatibility

- **State:** Accepted
- **Date:** 2026-07-29
- **Owners:** runtime and API maintainers
- **Deciders:** project owner

## Context

A restart selects an existing job instance but executes application code loaded
by a new process. OxideBatch cannot inspect arbitrary Rust closures well enough
to prove that the new reader, processor, writer, checkpoint, and context
contracts mean the same thing as those that produced the durable state.

Accepting a restart because the job name still matches could reinterpret a
checkpoint, replay committed work, or skip required work. Rejecting every
application upgrade would be safe but would make planned evolution impossible.

## Decision

Every launch supplies a bounded, application-owned **definition revision** and
a framework-produced **definition manifest**. The manifest contains only
restart-relevant, non-secret metadata:

- manifest format version;
- job and step names in execution order;
- component revision tokens supplied by the application;
- checkpoint and execution-context schema identifiers and versions;
- transaction guarantee and chunk-boundary semantics;
- definition settings that can change durable behavior, including chunk size.

The canonical manifest encoding is versioned and hashed with SHA-256. The
repository persists the revision, the 32-byte digest, and the canonical
manifest. It rejects a `(job_name, definition_revision)` that is already bound
to a different digest as **definition drift**. The application revision is an
audit label; the manifest digest is the machine comparison value. OxideBatch
does not hash executable code or claim that equal component tokens prove equal
code.

Each job execution references the exact persisted definition used for that
attempt. Definition identity is not part of job-instance identity: identifying
parameters continue to select the instance, while definition compatibility
decides whether another execution may restart it.

### Restart decision

A completed or abandoned instance is rejected before definition compatibility
is considered. An apparently active, orphaned, or `UNKNOWN` execution requires
the recovery decision defined by the lifecycle contract before restart
selection.

For a failed or stopped instance:

1. the same manifest digest is compatible;
2. a different digest is incompatible by default;
3. a different digest is accepted only through one explicit, directed
   compatibility edge from the checkpoint-producing definition to the proposed
   definition;
4. every edge names its application-owned upgrade key, maps each durable step
   to exactly one target step, and supplies all required checkpoint and context
   schema upgrades;
5. the repository records the chosen edge on the new execution.

Compatibility is never inferred from revision ordering, semantic-version
syntax, matching step names, additive JSON fields, or a successful
deserialization. Edges are not transitive: upgrading from `v1` to `v3`
requires a direct registered edge even when `v1 -> v2` and `v2 -> v3` exist.
An edge is one-way unless its reverse is registered independently.

An upgrade must preserve the meaning of the last committed checkpoint and
cannot change the selected job instance, erase durable counters, claim stronger
transaction guarantees for already-produced effects, or map two source steps
to one target step. Upgrade code runs before user work, is bounded and
deterministic, and commits upgraded context plus the new execution creation in
one repository transaction. Failure leaves the prior execution and context
unchanged.

### Manifest and revision encoding

- format version is unsigned 16-bit and begins at `1`;
- job names, step names, schema identifiers, upgrade keys, component revision
  tokens, and definition revisions are UTF-8, limited to 128 bytes, reject
  surrounding whitespace and control characters, and are compared byte for
  byte;
- manifest maps are sorted by UTF-8 key bytes and integers use their canonical
  JSON decimal form; duplicate keys and floating-point values are forbidden;
- the canonical JSON byte stream has no insignificant whitespace and is limited
  to 64 KiB;
- the digest is exactly 32 raw bytes and is never accepted from a display
  string supplied by the application.

Names are not silently Unicode-normalized. Visually similar but byte-distinct
names remain distinct. This matches the facade's existing validated-name
contract and prevents database locale from changing identity.

## Rejection categories

The facade exposes stable, implementation-neutral categories:

- `DefinitionDrift`: one job name and revision produced different manifests;
- `IncompatibleDefinition`: no direct compatibility edge exists;
- `InvalidDefinitionUpgrade`: an edge is incomplete or violates an invariant;
- `DefinitionUpgradeFailed`: registered upgrade code failed before execution;
- `UnsupportedManifestVersion`: the runtime cannot interpret persisted identity.

Diagnostics may include job name, definition revision, manifest format, and
opaque digest prefixes. They never include component state, context payloads,
parameters, credentials, SQL, or serializer/driver errors.

## Consequences

- restart is fail-closed when application code or durable schemas change;
- applications must version opaque components honestly;
- deliberate evolution remains possible and auditable;
- definition records and compatibility edges become durable metadata;
- a new execution retains both the definition it runs and the edge used to
  interpret prior state;
- public APIs need definition/upgrade value types but no SQLx, TLS, serializer,
  or executor types.

## Alternatives considered

- Hashing Rust binaries is unstable across builds and does not express semantic
  compatibility.
- Treating semantic versions as compatibility would infer meaning from labels.
- Using job name plus step names misses reader position, context, and
  transaction changes.
- Automatically accepting additive JSON changes makes the serializer, rather
  than the application contract, the compatibility authority.

## Validation

M2 component tests must cover equal definitions, revision drift, absent and
wrong-direction edges, direct upgrades, incomplete step mappings, failed
context upgrades, and rollback of execution creation when an upgrade fails.
The PostgreSQL repository contract must prove that definition comparison and
new-execution creation observe one transactionally consistent state.

## Revisit triggers

Revisit this decision if a future declarative definition language can prove
more compatibility automatically, if manifests need a non-JSON canonical
format, or if multi-step flow upgrades require richer mapping than one
source step to one target step.
