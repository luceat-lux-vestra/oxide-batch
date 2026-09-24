# M7 #299 Repeat Definition Identity Evidence

**Issue:** #299
**Parent:** #197 / #192
**Proof boundary:** definition and restart identity only

## Delivered contract

- Repeat is metadata over one existing step execution, never a graph back edge.
- A repeat definition carries a stable repeat ID, policy kind/revision,
  bounded configuration identity, ordered interceptor registrations, and a
  bounded state-schema identity.
- One step may own a nested repeat wrapper chain up to depth 8.
- One repeat may own at most 32 ordered interceptors.
- Repeat IDs are unique across one compiled plan, including embedded linear
  split workers, partition workers, nested flows, and flow-branch scopes.
- Any repeat metadata forces canonical manifest format 4 and therefore enters
  the existing SHA-256 definition fingerprint.
- Runtime iteration, durable ordinal/state/decision persistence, interceptor
  callbacks, and flow-level fault orchestration remain explicitly deferred to
  #300 and #301.

## Custom completion-policy restart identity

The earlier M6 custom `CompletionPolicy::fingerprint()` default used the
concrete Rust type name. That could not distinguish two configurations of the
same custom policy. #299 closes that restart-identity hole without creating a
second policy path:

- the trait default now supplies no fingerprint;
- `completion_policy_revision` rejects the missing identity with
  `CompletionPolicyFingerprintMissing`;
- built-in policies continue to provide deterministic configuration identity;
- a composite propagates a missing nested member identity instead of hiding it
  behind the composite's own fingerprint.

This is fail-closed definition construction, not runtime repeat behavior.

## Verification obligations

The exact final candidate must prove:

- format-4 round-trip through `DefinitionManifest::read_verified`;
- policy configuration and interceptor order change the definition fingerprint;
- non-semantic flow-node declaration order remains canonical;
- 32 interceptors are accepted and 33 rejected;
- nesting depth 8 is accepted and 9 rejected;
- duplicate repeat IDs fail before plan construction;
- malformed/unknown repeat members fail closed;
- manifest format 5 remains rejected by a format-4 runtime;
- format-2 definitions with no repeat metadata remain readable;
- custom completion policies without restart identity are rejected;
- facade review/snapshot, Rust, evidence provenance, and affected retained
  campaigns pass on the exact final HEAD.

## Non-goals

No repository schema/table, durable repeat record, runtime loop, interceptor
callback, retry/skip/rollback path, graph cycle, service-locator behavior, or
new execution engine is introduced here.
