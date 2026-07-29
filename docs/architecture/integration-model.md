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
- transaction participation and supported delivery modes;
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

Duplicate and unknown outcomes are expected. Adapters expose stable effect or
message IDs so applications can use idempotency, inbox/deduplication, outbox,
or reconciliation.

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
