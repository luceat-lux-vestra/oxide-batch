# Distributed Execution

**State:** Proposed

**Approval gate:** [RFC-0009](../rfcs/0009-transport-neutral-worker-protocol.md)

This document is the canonical proposed specification for remote step,
partition, and chunk execution. It does not authorize distributed production
code while RFC-0009 is pending.

## Invariants

- The repository/coordination state, not a transport, is authoritative.
- A worker must hold the current lease and fencing token to commit progress or
  a final result.
- Commands, acknowledgements, and results may be duplicated, delayed, or
  reordered.
- Completed partitions are not rerun on restart unless an explicit fork or
  replay policy says otherwise.
- Worker or coordinator failure cannot corrupt the definition, context, or
  checkpoint.
- The same compiled plan has equivalent lifecycle and restart meaning in
  embedded, local, and distributed modes.

## Responsibilities

The coordinator resolves and validates the plan, creates durable assignments,
selects compatible workers, issues fenced commands, observes heartbeats,
reassigns expired work, aggregates results, applies stop/recovery decisions,
and persists the authoritative execution trace.

A worker registers bounded capabilities, accepts only compatible protocol and
artifact versions, validates assignment and fencing, executes within declared
resource budgets, checkpoints through the authoritative repository path,
deduplicates commands/results, heartbeats, and drains cooperatively.

The transport delivers versioned envelopes and acknowledgements. It does not
decide ownership, completion, or restart.

## Execution models

- **Remote step:** a whole validated step executes on a compatible worker.
- **Remote partitioning:** the coordinator creates durable partition
  identities; workers execute full step instances for assigned partitions.
- **Remote chunking:** the manager reads and forms chunks while workers process
  or write them under explicit delivery and deduplication semantics.

Local partitioning and local chunking use the same assignment and aggregation
meaning where practical, without paying serialization or transport overhead.

## Durable protocol state

Durable records include execution and plan fingerprint, partition/chunk ID,
assignment attempt, worker capability selection, lease owner and expiry,
fencing token, command and result idempotency keys, acknowledgement state,
checkpoint/result fingerprint, retry/redelivery decision, and terminal
aggregation.

Leases use monotonic local deadlines plus repository/server-time evidence.
Renewal has a bounded interval and grace policy. Every reassignment increments
the fencing token. A stale worker can finish computation but cannot publish
authoritative state.

## Transport-neutral messages

Envelopes contain a protocol version, message kind, opaque IDs, plan/definition
fingerprint, capability requirements, bounded payload reference and checksum,
deadline, trace correlation, idempotency key, and fencing token. They exclude
credentials and unrestricted context payloads.

The wire schema has explicit size/depth limits, rejects unknown incompatible
major versions, preserves compatible unknown fields where specified, and is
fuzzed. Transport adapters map Kafka, NATS, AMQP, or direct gRPC semantics into
the envelope without pretending their acknowledgements are identical.

## Concurrency and backpressure

Coordinator queues, worker assignments, in-flight chunks, result buffers,
connections, and retries are bounded. Credits or permits propagate
backpressure. Stop prevents new assignment, chooses a documented in-flight
commit/rollback policy, waits to a deadline, persists remaining ownership, and
then reports whether drain was complete.

## Security boundary

Workers and coordinators mutually authenticate through deployment-provided
identity. Authorization limits accepted definitions, artifacts, resources, and
operator actions. Artifacts are content-addressed, verified, and allowlisted.
Protocol payloads are treated as untrusted and bounded. Native Rust components
are trusted code; sandboxing requires a separate WASI capability.

## Failure matrix

Evidence covers worker death before and after write/checkpoint, delayed or
duplicate commands/results, lost acknowledgement, expired lease, stale commit,
network partition, split brain, coordinator restart, repository failover,
transport rebalance, corrupted payload, artifact mismatch, stop during
assignment, and N/N-1 rolling upgrade.

For each case, expected owner, replay, checkpoint, counters, result, status,
and operator action are specified. Embedded/local/distributed normalized traces
must agree on semantic observations.
