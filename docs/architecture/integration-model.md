# Integration Model

**State:** Accepted

**Governing decision:**
[RFC-0007](../rfcs/0007-repository-services-and-capabilities.md)

This document is the canonical target specification for integration
categories, adapter capabilities, delivery guarantees, and support tiers.

## Adapter categories

| Category | Examples |
| --- | --- |
| Relational database | PostgreSQL, MySQL/MariaDB, SQLite, SQL Server, Oracle, DB2, HANA |
| Files and records | delimited/CSV, fixed-width, XML, JSON/JSONL, Avro |
| Columnar and analytical | Arrow, Parquet |
| Object storage | S3-compatible, Azure Blob, Google Cloud Storage |
| Messaging and streams | Kafka, NATS JetStream, AMQP/RabbitMQ, Pulsar, SQS, Redis Streams, JMS-equivalent |
| Network/service | HTTP pagination/streaming, webhook/effect writer |
| Custom | statically linked Rust component, registered erased component, out-of-process protocol, WASI component |

Item-source/sink contracts and distributed-worker transports remain distinct
even when the same broker has adapters for both.

## Capability descriptor

Each adapter declares:

- read/write direction, format and schema versions;
- restart and checkpoint ownership;
- ordering, partitioning, thread-safety, and reentrancy;
- transaction participation and required local durable-effect capabilities;
- acknowledgement, offset, redelivery, rebalance, and poison-message behavior;
- idempotency, deduplication, outbox/inbox, and effect-journal hooks;
- bounded buffering, concurrency, rate, timeout, and cancellation behavior;
- sensitive data, diagnostics, and telemetry;
- supported product versions and feature limitations.

Plan compilation rejects a requested guarantee or execution mode absent from
the descriptor. Runtime negotiation confirms actual server/broker features.

## Checkpoint and delivery rules

The component that owns a cursor, offset, resource index, or pagination token
defines its checkpoint schema. The adapter documents when acknowledgements and
external publication occur relative to metadata commit. Broker-native
semantics are preserved; they are not hidden behind a fictitious universal
exactly-once interface.

Duplicate and unknown outcomes are expected. Adapters expose or derive the
stable message/effect identity required by the selected profile; payload
equality is never a deduplication identity.

## M8 durable-effect handoff

The [M8 Gate-B record](../project/m8-gate-b-durable-effect-delivery.md) and
[ADR-0016](decisions/0016-durable-effect-definition-identity-and-runtime-boundary.md)
define the local foundation that M9 integrations may consume:

- replay-stable logical `EffectId`;
- chunk-scoped EffectJournal intent persistence;
- Outbox obligation persistence;
- InboxDedup protection; and
- fail-closed repository capability negotiation.

M8 does not require a generic provider-neutral remote semantic-contract
registry.

M9 adapters first describe and certify their concrete protocol semantics. Each
effect/message adapter profile names at least:

- stable message/effect identity source and scope;
- acknowledgement meaning and exposure boundary;
- offset/cursor/visibility owner;
- redelivery, duplicate, rebalance, ordering, and partition behavior;
- exact native transaction scope;
- known-not-applied, known-applied/acknowledged, and unknown-outcome mapping;
- idempotency/dedup key scope and validity when claimed;
- required local durable-effect persistence capabilities;
- checkpoint/progress ordering relative to acknowledgement or send;
- bounded buffering, concurrency, in-flight work, timeout, and cancellation;
- redaction policy; and
- exact supported remote product/protocol versions.

Broker-native "exactly once", publisher confirms, finite dedup windows,
visibility timeouts, or HTTP status codes are accepted only for their certified
scope. They never imply that unrelated repository progress committed atomically.

A provider-neutral semantic contract becomes a candidate only after concrete
evidence shows cross-adapter equivalence, adapter-replacement restart
continuity, third-party extension need, or reconciliation need that the local
M8 capability model cannot represent truthfully. Such a contract is introduced
through a later governed version/ADR rather than ad hoc inside one adapter.

Certification remains profile-specific. Generic read/write tests or mocks do
not certify delivery guarantees; claimed crash/redelivery/rebalance/ambiguity
behavior requires retained evidence against the named real remote version.
Object-storage and HTTP readers remain ordinary checkpoint/restart integrations
unless they actually perform duplicate-sensitive external mutation.

## Support tiers

- **First-party:** maintained in the OxideBatch organization and covered by
  the release support matrix.
- **Certified third-party:** maintained elsewhere but passes a versioned
  contract/conformance kit for named versions.
- **Experimental:** useful for evaluation, with no stable compatibility or
  production support promise.

Certification is capability-specific. Passing basic read/write tests does not
certify restart, transactions, distributed transport, or performance.

## Adapter evidence

Evidence includes component contracts, restart/checkpoint tests, crash points,
duplicate/redelivery/rebalance fixtures, schema/format evolution, bounded
backpressure, cancellation, security/redaction, compatibility rows, and
performance/resource reports. Every documented limitation is represented in
the feature ledger.
