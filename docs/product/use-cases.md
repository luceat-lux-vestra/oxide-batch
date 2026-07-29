# Representative Use Cases

**State:** Accepted

These use cases prevent the API from being optimized for one demonstration job.
Each milestone chooses a subset as acceptance workloads.

## UC-001 — Database import

Read records from a bounded file or stream, validate and transform them, then
write to PostgreSQL in chunks.

Required behavior:

- deterministic restart from committed input position;
- malformed records can fail or skip according to policy;
- business rows and checkpoint can share a transaction;
- progress is observable without logging record contents.

## UC-002 — Export and delivery

Read a stable database snapshot and produce one or more external files.

Required behavior:

- output publication is atomic or uses staging/rename semantics;
- restart does not silently duplicate a published file;
- snapshot/cursor consistency is documented;
- checksums and record counts support reconciliation.

## UC-003 — Reconciliation

Compare two systems or datasets and emit matched, mismatched, and unresolved
outcomes.

Required behavior:

- run identity includes the relevant business date/version;
- partial results are distinguishable from completed reconciliation;
- retrying a source does not change already committed classifications;
- counters and summaries remain auditable.

## UC-004 — Settlement or financial posting

Calculate and persist business-critical postings with strict duplicate
prevention.

Required behavior:

- idempotency keys and transaction ownership are explicit;
- no general exactly-once claim is made across unrelated resources;
- ambiguous outcomes stop for operator/application reconciliation;
- recovery is audited and guarded.

This is a high-integrity reference scenario, not a claim of regulatory
certification.

## UC-005 — Periodic cleanup

Select expired application records and delete/archive them in bounded chunks.

Required behavior:

- selection and deletion races have defined semantics;
- stop/restart does not broaden the deletion scope;
- dry-run and limit capabilities belong to application policy;
- destructive operations are visible and bounded.

## UC-006 — API enrichment

Read records and call a remote service before writing results.

Required behavior:

- concurrency, rate limit, timeout, retry, and circuit behavior are bounded;
- external effects are treated as at-least-once unless idempotency exists;
- backpressure prevents unbounded buffered items;
- cancellation interrupts backoff and pending framework work.

## UC-007 — Multi-step conditional job

Load input, validate aggregate quality, branch to publish or quarantine, then
write a summary.

Required behavior:

- flow decision is derived from persisted step outcome;
- completed steps follow documented restart rules;
- exit status and batch status remain distinct;
- listener/decision failures are durable and diagnosable.

## UC-008 — Local partitioned backfill

Partition a historical key range and run bounded workers on one host.

Required behavior:

- partitions are complete and non-overlapping;
- ownership and retry are durable;
- aggregate job status reflects every partition;
- worker concurrency and memory are bounded.

## Coverage by milestone

| Use case | First meaningful milestone |
| --- | --- |
| UC-001 | M2 |
| UC-002 | M3 |
| UC-003 | M3 |
| UC-004 | M5 preview validation; M14 GA reference |
| UC-005 | M2/M3 |
| UC-006 | M3 |
| UC-007 | M3 |
| UC-008 | M4 |
