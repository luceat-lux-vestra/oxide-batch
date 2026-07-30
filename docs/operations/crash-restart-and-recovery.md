# Crash, Restart, and Recovery Runbook

**State:** Implemented for M2

This runbook covers the embedded M2 PostgreSQL path. It does not provide the
future M4 operator CLI, automatic stale-worker takeover, or recovery
authorization. Deployment tooling must authenticate the operator and retain
the full evidence referenced by the bounded OxideBatch audit record.

## Expected crash state

A forced process exit can leave a job and step visibly `STARTING`, `STARTED`,
or `STOPPING`. Age and missing process liveness are evidence only; OxideBatch
does not automatically steal or rewrite the execution.

The last committed checkpoint, context, counters, business writes, and
optimistic version remain authoritative:

- an open transaction at exit rolls back;
- an acknowledged commit survives even when the process exits before
  completion callbacks or terminal lifecycle updates;
- an ambiguous commit response becomes `UNKNOWN`.

## Inspect

Use a fresh healthy connection and the operator-reader identity. Do not log
parameter values, context/checkpoint payloads, business records, SQL text, or
driver diagnostics.

Identify the instance and latest execution, then record:

- job and step execution IDs and attempts;
- status, timestamps, versions, exit status, and redacted failure category;
- six durable counters;
- checkpoint/context schema IDs, versions, and encoded sizes;
- the durable business-effect evidence required by the application;
- whether the observed state proves commit, proves rollback, or remains
  ambiguous.

For `UNKNOWN`, inspection must establish the external effect outcome. If it
cannot, keep the execution blocked and reconcile manually.

## Decide

`RecoveryRequest` requires:

- the exact observed execution version;
- a disposition of `FAILED` or `ABANDONED`;
- a stable bounded reason code;
- an opaque authenticated-operator correlation;
- a SHA-256 digest of evidence retained outside OxideBatch metadata;
- a redacted failure category and correlation ID.

The PostgreSQL repository rereads the execution under lock, appends the
recovery decision, and compare-and-swap updates the execution in one
transaction. A stale version publishes neither mutation.

Choose `FAILED` only when durable evidence permits restart from the retained
checkpoint. Choose `ABANDONED` when replay must remain permanently blocked.
Never use recovery to assert an external effect that cannot be established.

## Restart

A restart:

1. requires the same definition fingerprint or one registered direct
   `DefinitionUpgrade`;
2. creates distinct job and step execution IDs;
3. copies only the latest committed checkpoint, execution context, and six
   counters;
4. resumes input after that checkpoint;
5. leaves the prior attempt and recovery audit inspectable.

M2 definition upgrades are byte-preserving. A changed checkpoint or context
schema requires a future explicit transformation contract and is rejected.

Completed and abandoned instances are terminal. Active or `UNKNOWN` attempts
remain blocked until the audited recovery transaction commits.

## Validate

After restart, verify:

- no already committed business item was replayed;
- the formerly uncommitted chunk followed the documented delivery boundary;
- final business rows match the expected deterministic input;
- counters and checkpoint match the committed business result;
- the original and restart attempt IDs differ;
- the recovery record contains the expected prior/result status, version,
  reason code, operator correlation, and evidence digest;
- diagnostics contain no record, parameter, context, checkpoint, credential,
  endpoint, SQL, or bound-value payload.

The reproducible process-kill matrix is:

```console
cargo test -p oxide-batch --features postgres \
  --test postgres_crash_recovery \
  -- --nocapture --test-threads=1
```

Set both `OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL` and
`OXIDEBATCH_POSTGRES_TEST_URL` to an isolated migrated database. The test
creates and removes only its named metadata and
`oxide_batch_business.m2_crash_output` fixture rows.

## Escalation

Stop and preserve evidence when:

- the durable checkpoint and business effects disagree;
- the schema is newer than the runtime;
- the definition fingerprint or state schema changed without an approved edge;
- the recovery request loses its optimistic-version race;
- an external effect remains ambiguous;
- backup/restore verification fails.

Do not delete metadata, edit statuses directly, or reuse an old execution ID.
Retain database logs and application evidence under the deployment's sensitive
data policy.
