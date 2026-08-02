# Crash, Restart, and Recovery Runbook

**State:** Implemented through the M4 shutdown/recovery slice

This runbook covers the embedded M2/M3 PostgreSQL path and the M4
application-owned graceful-shutdown, stale-proposal, and operator-CLI path.
There is no automatic stale-worker takeover. Deployment tooling must
authenticate and authorize the operator and retain application evidence
outside the bounded OxideBatch audit record.

## Graceful process shutdown

Create one `ShutdownCoordinator` in the application runtime and install the
application's own `SIGINT`/`SIGTERM` adapter against its `ShutdownSignal`.
OxideBatch installs no handler. Give the same signal to each `JobLauncher` or
`FlowLauncher` and spawn owned runtime work through the coordinator.

On the first request, verify that intake rejects new work, cancellation reaches
every owned execution, and the execution progresses through `STOPPING` to a
durable result. `FinishChunk` commits the already-open chunk before stopping;
`RollbackChunk` preserves the previous checkpoint. A commit whose response is
ambiguous remains `UNKNOWN` under either policy.

A second request escalates waiting only. It does not terminate the process,
abort an in-flight commit, or create a terminal state. If the report is
`DrainResult::Incomplete`, retain its unjoined task count and phases, keep the
last durable execution state, and do not exit until the application has made
an explicit process-level decision. Telemetry failure and repository-close
failure are reported separately and cannot rewrite the durable result.

`SIGKILL`, host loss, and power loss are crashes, not graceful shutdown. Follow
the stale-evidence workflow below.

## Produce stale or unknown recovery evidence

Run `execution show --execution <id>` through an operator-reader connection.
For an eligible `UNKNOWN` execution or active execution older than the
configured strict stale threshold, the redacted `recovery_proposal` contains
the observed version and a 64-character evidence digest. A null proposal means
the current observation is not eligible or its clock evidence is unusable;
inspect application and repository diagnostics rather than guessing.

Active stale classification requires repository-server inactivity beyond the
threshold and an absent or different complete owner token. The token is not a
lease and authorizes no takeover. Local wall time, process liveness, and
telemetry are never recovery authority. Backwards repository time, negative
inactivity, excessive local/server skew, or an excessive monotonic observation
window produces no proposal.

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

`execution recover` and the underlying typed recovery request require:

- the exact observed execution version;
- a disposition of `FAILED` or `ABANDONED`;
- a stable bounded reason code;
- an opaque authenticated-operator correlation;
- the current framework-produced SHA-256 proposal digest, with supporting
  application evidence retained outside OxideBatch metadata;
- a redacted failure category and correlation ID.

The PostgreSQL repository rereads the execution under lock, appends the
recovery decision, and compare-and-swap updates the execution in one
transaction. A stale version publishes neither mutation.

Choose `FAILED` only when durable evidence permits restart from the retained
checkpoint. Choose `ABANDONED` when replay must remain permanently blocked.
Never use recovery to assert an external effect that cannot be established.
An unknown commit may become `FAILED` only with reason `UNKNOWN_EFFECT`, which
records that external-effect confirmation remains an application obligation;
otherwise choose `ABANDONED` or keep the execution unresolved.

Example guarded application after recording the proposal from `execution
show`:

```console
oxide-batch execution recover \
  --execution 42 \
  --expected-version 7 \
  --actor ops:incident-123 \
  --reason UNKNOWN_EFFECT \
  --directive mark-failed \
  --failure-category unknown_commit \
  --failure-id 900 \
  --evidence-digest <64-hex-digest> \
  --operation-id recover-incident-123 \
  --yes
```

The CLI regenerates the proposal immediately before application. A changed
version or digest rejects the command without lifecycle mutation. Every
service-level rejection or application is append-only audited.

## Restart

A restart:

1. requires the same definition fingerprint or one registered direct
   `DefinitionUpgrade`;
2. creates distinct job and step execution IDs;
3. copies the latest committed checkpoint, execution context, base execution
   counters, M3 fault counters, and retained retry state;
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
cargo test -p oxide-batch --features postgres \
  --test postgres_fault_crash_recovery \
  -- --nocapture --test-threads=1
cargo test -p oxide-batch --features postgres \
  --test postgres_flow_crash_recovery \
  -- --nocapture --test-threads=1
```

Set both `OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL` and
`OXIDEBATCH_POSTGRES_TEST_URL` to an isolated migrated database. The test
creates and removes only its named metadata and the
`oxide_batch_business.m2_crash_output`, `m3_fault_crash_call`, or
`m3_flow_crash_call` fixture rows.

## Fault-tolerance state after a crash

A retry reservation is durable before the runtime waits for backoff, so a
process that stops between reservation and re-invocation has still consumed that
ordinal. Restart resumes the persisted ordinal and never refills the retry
budget; it may invoke fewer retries than were reserved, never more. A crash
before the reservation commits consumes nothing, and the initial component call
may replay under its declared delivery mode.

Committed skip counts, `no_rollback_count`, and the retained retry state are
part of the chunk commit, so an uncommitted chunk leaves all of them unchanged.
Restart copies the committed totals and the retained state to the new step
attempt, which is why the shared skip limit spans every attempt of one job
instance.

A skip callback runs before the transaction that accepts its skip. A process
exit during that callback can therefore leave an external callback effect while
the durable skip and no-rollback counts remain unchanged. On restart the
callback may run again. Only callback work enlisted in the accepting transaction
has exactly one committed effect; external callback work must be idempotent or
reconciled.

A terminal known rollback that fails the step is committed with that terminal
step lifecycle and increments the attempt's `rollback_count`. A retry
reservation already accounts for its own known rollback, so the terminal path
does not count the same attempt twice.

## Flow state after a crash

The flow runtime commits a step terminal result before appending its selected
transition, then commits the transition before starting its target. Recovery
therefore follows the durable boundary:

- after a completed source step but before its decision, restart records a
  `CompletedStepReuse` decision and does not invoke the source body again;
- after a decision commit but before target start, restart reuses the recorded
  target and does not repeat source work or decision selection;
- an active or `UNKNOWN` job execution still requires the audited recovery
  procedure above before a new attempt can start.

Flow events can be lost or duplicated around a crash and are not recovery
authority. Inspect the step-execution and `ob_flow_decision` rows.

If durable fault state cannot be validated — an unsupported version, a checksum
mismatch, an unknown enumeration value, or state retained against a superseded
checkpoint — the step fails closed before any component runs. Treat that as
metadata corruption: inspect the step row, restore from the verified backup
named in [the schema-2 migration guide](migrations/0002-fault-tolerance-and-flow.md),
and do not edit the envelope by hand.

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
