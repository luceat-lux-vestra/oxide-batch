# Transaction Guarantees

**State:** Implemented for M2 and the M3 fault-tolerance slice

## Atomic same-resource PostgreSQL path

When metadata and business writes use the same PostgreSQL database through
`PostgresChunkTransactionManager`, one adapter-owned transaction contains:

1. parameter-bound business writes;
2. checkpoint and execution context;
3. cumulative read, process, write, filter, commit, and rollback counters;
4. the per-phase skip counts and no-rollback count this chunk accepted;
5. the cleared fault state of the superseded retry generation;
6. the step execution optimistic-version increment.

All six commit or roll back together. The adapter lends writers only the
facade-owned `BusinessTransaction`; SQLx pools, connections, transactions,
rows, SQL text, bound values, and driver errors remain private.

Skip and no-rollback counts are applied as deltas to the totals the adapter read
when the transaction began, so replaying an uncommitted chunk after a crash
cannot double-count a skip. The commit that advances the checkpoint supersedes
every retry key derived from the previous checkpoint, so it writes the empty
fault-state envelope rather than pruning keys individually.

A retry reservation is deliberately *not* part of this transaction. It is one
short metadata-only compare-and-swap that runs after a known rollback and before
backoff, so an ordinal stays consumed even when the process stops before
re-invoking the component. It advances one phase retry count and the
acknowledged `rollback_count` under `WHERE version = expected AND status =
'STARTED'`.

Checkpoint and counters become authoritative only after `COMMIT` is
acknowledged. Completion callbacks, chunk events, and other telemetry run
after commit and are not correctness authorities.

## Failure outcomes

| Boundary | Durable result | Restart consequence |
| --- | --- | --- |
| Reader/processor failure | Open chunk has no durable progress | Replay from the previous checkpoint |
| Writer or state-provider failure before commit | Business work and progress roll back | Replay the whole uncommitted chunk |
| Optimistic conflict | Exactly one transaction wins; losing business work rolls back | Loser resumes from the winning version |
| Process exit before `COMMIT` | PostgreSQL rolls back the open transaction | Replay from the previous checkpoint |
| Process exit after acknowledged `COMMIT` | Business work and progress remain durable | Do not replay the committed chunk |
| Commit response failure | Outcome is `UNKNOWN`; connection is discarded | Inspect durable state, then make an audited recovery decision |
| Completion/listener failure after commit | Earlier committed work remains authoritative | Restart after the retained checkpoint |
| Stop before commit | Open chunk rolls back | Restart may replay the chunk |
| Stop after commit | Committed chunk remains authoritative | Restart begins after that checkpoint |
| Process exit after an acknowledged reservation | The ordinal and its retry/rollback counts remain durable | Restart resumes the persisted ordinal and never refills the budget |
| Process exit before the reservation commits | No ordinal is consumed | Restart may replay the initial component call |
| Process exit during a skip callback before its accepting commit | Skip and no-rollback counts remain unchanged | Callback may replay; external callback effects require idempotency or reconciliation |
| Terminal known rollback | Failed step lifecycle and one rollback count commit together | Restart inherits only committed progress and does not undercount the terminal attempt |
| Process exit after step result but before flow decision | Completed source step is durable; no decision exists | Restart reuses the source step, records the reuse decision, and then starts the target |
| Process exit after flow decision but before target start | Source step and selected target are durable | Restart reuses the decision and starts the recorded target |
| Unusable durable fault state | Nothing is written | The step fails closed before any component runs |

A disconnect before the adapter sends `COMMIT` is known not committed. A
failure while receiving the `COMMIT` response is never guessed; it is
`CommitOutcomeUnknown`.

## Other resources

The M2 evidence does not create a generic exactly-once guarantee. A writer that
does not receive an enlisted `BusinessTransaction` remains outside the
same-resource atomic boundary and follows its documented delivery mode, which
may permit duplicates or require reconciliation.

Future transaction modes include transactional messages, outbox, inbox/dedup,
idempotent external effects, at-least-once, and best effort. Each adapter must
publish its acknowledgement, redelivery, ordering, and unknown-outcome
behavior before claiming one of those modes.

## Executable evidence

The release-blocking PostgreSQL 15 and 18 CI axes run:

- `committed_chunk_advances_checkpoint`;
- `writer_failure_rolls_back_business_and_checkpoint`;
- `optimistic_conflict_has_one_winner`;
- `disconnect_during_commit_never_guesses_outcome`;
- `crash_before_commit_replays_chunk`;
- `crash_after_commit_does_not_replay_chunk`;
- `retry_reservation_is_a_durable_compare_and_swap`;
- `skips_counters_and_fault_state_commit_with_the_chunk`;
- `corrupt_fault_state_fails_before_component_work`.
- `terminal_known_rollback_commits_with_failed_step_lifecycle`;
- `flow_launcher_persists_a_bound_chunk_terminal_rollback`;
- `crash_before_reservation_replays_initial_call`;
- `retry_reservation_survives_process_restart`;
- `crash_during_skip_callback_replays_before_atomic_skip_commit`;
- `crash_after_step_result_before_decision_reuses_durable_step`;
- `crash_after_decision_commit_reuses_durable_decision`.

The crash tests execute the worker as a separate OS process, terminate it
without running Rust destructors, inspect the database through a fresh
connection, apply an audited recovery decision, and resume only from retained
durable state.
