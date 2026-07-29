# RFC-0009: Transport-Neutral Distributed Worker Protocol

- **State:** Proposed
- **Created:** 2026-07-30
- **Owner:** distributed runtime maintainers
- **Target milestone:** M11
- **Related decision:** D-012 in the
  [M0 decision register](../product/open-decisions.md)

## Summary

Introduce a versioned coordinator/worker protocol for remote step,
partitioning, and chunking, with durable assignments, leases, fencing,
idempotency, capability negotiation, and transport-independent correctness.

## Context and current accepted rule

D-012 defers distributed work until after the original 1.0 unless promoted by
RFC. Current execution semantics make no distributed lease or ownership
promise.

## Problem

Full Spring Batch 6.x coverage includes remote execution forms. A
broker-specific or acknowledgement-authoritative design would make restart and
ownership depend on transport behavior and allow stale workers to commit.

## Proposal

- Add coordinator and worker roles as specified in the distributed execution
  document.
- Persist assignment, lease, fencing, command/result idempotency, capability,
  checkpoint, and aggregation state.
- Treat duplicate, delay, reordering, redelivery, crash, and partition as
  normal protocol conditions.
- Use a bounded versioned neutral envelope mapped by Kafka, NATS, AMQP, or
  direct gRPC adapters.
- Require the current fencing token for every authoritative commit.
- Preserve embedded/local/distributed normalized semantic equivalence.
- Support N/N-1 rolling protocol compatibility before production support.

## Alternatives

1. Let the broker guarantee exactly-once ownership. Rejected because transport
   state is not repository authority.
2. Build one protocol per transport. Rejected due to divergent correctness.
3. Use a distributed transaction coordinator. Rejected as broad and leaky.
4. Keep distributed execution permanently out of scope. Incompatible with the
   proposed feature-ledger target.

## Consequences

Protocol/schema/security/chaos testing and operational complexity grow
substantially. The design can reuse transports without erasing their delivery
differences. Distributed execution remains optional.

## Compatibility impact

Wire messages and durable coordination state become versioned compatibility
surfaces. Transport profiles name supported broker versions and delivery
modes. Local users retain the same plan semantics without transport overhead.

## Metadata, restart, and transaction impact

Forward migrations add durable ownership, lease, fencing, assignment,
idempotency, and result state. Stale tokens cannot update checkpoints or final
status. Reassignment increments fencing. Unknown business effects still follow
the selected transaction/delivery mode.

## Migration and rollout

Implement an in-memory/fault-injected protocol harness, then local worker
transport, then one external transport. Introduce schema behind disabled
features. Prove rolling N/N-1. Add other transports only through certification.
Rollback drains assignments, prevents new remote launches, and uses compatible
local execution only where the plan permits it.

## Validation and evidence plan

- protocol decoder fuzzing and size/depth bounds;
- duplicate/delayed/reordered message fixtures;
- worker/coordinator kill, partition, failover, split-brain, stale commit;
- stop/drain and resource backpressure;
- artifact and capability mismatch security tests;
- embedded/local/distributed trace equivalence;
- N/N-1 protocol/schema upgrade and rollback;
- scale-out and resource-limit reports.

## Unresolved questions

- Initial wire encoding and schema registry.
- Coordinator HA/storage topology.
- Artifact distribution and trusted execution profile.
- First supported transport and its minimum broker versions.

## Approval gate

Approval requires the fault-injected state-machine prototype, threat model,
schema/migration design, and local/distributed equivalence evidence. RFC-0001
already supersedes D-012's timing; the “local correctness first” guard remains.

## Current implementation constraint

RFC-0001 has moved distributed execution into the pre-1.0 M11 program, but
this protocol design remains `Proposed`. Do not implement a production worker
protocol, wire schema, lease store, or transport-specific correctness path
until this gate passes. Current plan and repository work must preserve stable
logical IDs, capability validation, and a future fencing boundary so it does
not make M11 impossible.
