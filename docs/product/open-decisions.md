# M0 Decision Register

**State:** Active

**Owner:** project owner

The repository foundation is operational. Product and policy recommendations
were approved by the project owner on 2026-07-29. Architecture choices marked
as pending still require the named spike evidence before M0 can close.

| ID | Decision | Recommendation | State / gate |
| --- | --- | --- | --- |
| D-001 | Workspace and facade strategy | One workspace, public `oxide-batch` facade, internal crates private by default | Accepted in ADR-0001 |
| D-002 | Compatibility baseline | Spring Batch 6.0 semantics; selected behavioral and operational compatibility | Accepted 2026-07-29 |
| D-003 | 1.0 scope | Single-host runtime, PostgreSQL, tasklet/chunk, restart, flow, fault tolerance, CLI/telemetry | Accepted 2026-07-29 |
| D-004 | Public execution model | Async-first Tokio, no hidden global runtime | Spike direction approved; final decision pending evidence |
| D-005 | Durable repository | PostgreSQL via SQLx; OxideBatch-owned versioned schema | Spike direction approved; final decision pending evidence |
| D-006 | Delivery guarantee | Atomic exactly-once checkpoint with enlisted PostgreSQL writes; otherwise explicit at-least-once | Contract accepted; implementation validation pending |
| D-007 | Execution-context format | Bounded, versioned JSON initially | Spike direction approved; final decision pending evidence |
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
