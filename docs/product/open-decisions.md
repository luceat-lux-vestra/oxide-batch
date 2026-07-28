# M0 Decision Register

**State:** Closed

**Owner:** project owner

The repository foundation is operational. Product and policy recommendations
were approved by the project owner on 2026-07-29. All M0 architecture choices
have the required spike evidence; later-gate decisions remain visible as
explicit deferrals rather than open M0 questions.

| ID | Decision | Recommendation | State / gate |
| --- | --- | --- | --- |
| D-001 | Workspace and facade strategy | One workspace, public `oxide-batch` facade, internal crates private by default | Accepted in ADR-0001 |
| D-002 | Compatibility baseline | Spring Batch 6.0 semantics; selected behavioral and operational compatibility | Accepted 2026-07-29 |
| D-003 | 1.0 scope | Single-host runtime, PostgreSQL, tasklet/chunk, restart, flow, fault tolerance, CLI/telemetry | Accepted 2026-07-29 |
| D-004 | Public execution model | Async-first Tokio, no hidden global runtime | Accepted in ADR-0002; spike 0001 passed 2026-07-29 |
| D-005 | Durable repository | PostgreSQL via SQLx; OxideBatch-owned versioned schema | Accepted in ADR-0003; spike 0002 passed 2026-07-29 |
| D-006 | Delivery guarantee | Atomic exactly-once checkpoint with enlisted PostgreSQL writes; otherwise explicit at-least-once | Accepted; transaction/crash matrix passed in spike 0002 |
| D-007 | Execution-context format | Bounded, versioned JSON initially | Accepted; backward-read fixtures passed in spike 0003 |
| D-008 | Rust baseline and MSRV | Stable 1.97.1 for development/releases; MSRV 1.95; no beta/nightly CI | Accepted 2026-07-29 |
| D-009 | Pre-1.0 support | Latest release line only | Accepted 2026-07-29 |
| D-010 | Stable support | Current minor plus critical fixes for previous minor for six months | Approved deferral; final decision at M5 |
| D-011 | Schema rollback | Backup/restore by default; downgrade only when explicitly shipped | Accepted 2026-07-29 |
| D-012 | Remote/distributed work | Post-1.0 unless promoted by RFC | Accepted 2026-07-29 |

## Approval rule

A decision is approved by merging the ADR or governing document with its state
changed to **Accepted**. A pull request that merely adds a proposal does not
silently approve it.

## Spike evidence required

- D-004: object safety, cancellation, panic isolation, blocking adapter, and
  transaction-scoped writer prototype;
- D-005/D-006: PostgreSQL locking, atomic checkpoint, crash matrix, and
  disconnect behavior;
- D-007: backward read and upgrade behavior for persisted contexts.

## M0 architecture resolution record

On 2026-07-29:

- [spike 0001](../architecture/spikes/0001-async-public-traits.md)
  resolved D-004 and supplied the execution evidence for ADR-0002;
- [spike 0002](../architecture/spikes/0002-postgres-transactions-and-recovery.md)
  resolved D-005 and D-006 and supplied the evidence for ADR-0003;
- [spike 0003](../architecture/spikes/0003-execution-context-evolution.md)
  resolved D-007.

The remaining entries in this register are accepted or explicitly deferred to
their named later gate.

M0 closed on 2026-07-29 through the runtime implementation kickoff gate. New
questions that could change an accepted public contract use the RFC/ADR process
and do not reopen this historical register.
