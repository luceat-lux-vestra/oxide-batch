# M0 Decision Register

**State:** Proposed

**Owner:** project owner

The repository foundation is operational. The following product and
architecture choices still need approval or evidence before M0 can close.

| ID | Decision | Recommendation | State / gate |
| --- | --- | --- | --- |
| D-001 | Workspace and facade strategy | One workspace, public `oxide-batch` facade, internal crates private by default | Accepted in ADR-0001 |
| D-002 | Compatibility baseline | Spring Batch 6.0 semantics; selected behavioral and operational compatibility | Awaiting approval |
| D-003 | 1.0 scope | Single-host runtime, PostgreSQL, tasklet/chunk, restart, flow, fault tolerance, CLI/telemetry | Awaiting approval |
| D-004 | Public execution model | Async-first Tokio, no hidden global runtime | Awaiting spike and approval |
| D-005 | Durable repository | PostgreSQL via SQLx; OxideBatch-owned versioned schema | Awaiting spike and approval |
| D-006 | Delivery guarantee | Atomic exactly-once checkpoint with enlisted PostgreSQL writes; otherwise explicit at-least-once | Awaiting spike and approval |
| D-007 | Execution-context format | Bounded, versioned JSON initially | Awaiting evolution spike |
| D-008 | Rust baseline and MSRV | Stable 1.97.1 for development/releases; MSRV 1.95; no beta/nightly CI | Accepted 2026-07-29 |
| D-009 | Pre-1.0 support | Latest release line only | Awaiting approval |
| D-010 | Stable support | Current minor plus critical fixes for previous minor for six months | Defer final approval to M5 |
| D-011 | Schema rollback | Backup/restore by default; downgrade only when explicitly shipped | Awaiting approval |
| D-012 | Remote/distributed work | Post-1.0 unless promoted by RFC | Awaiting approval |

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
