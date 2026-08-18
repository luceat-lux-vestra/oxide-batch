# M5 Embedded Core Production Preview Guide

**State:** Accepted

**Applies to:** OxideBatch `0.5.0`, the M5 Embedded Core Production Preview

This is the entry point for the M5 preview. It orients a new user and links to
the canonical document that owns each topic in depth; it does not restate
normative detail that already has an owner. See
[documentation strategy](../documentation/strategy.md) for that ownership
rule.

## Who this is for

Rust teams embedding a durable, restartable, single-host PostgreSQL batch
kernel directly in their own application process: data import/export,
reconciliation, settlement, and ETL workloads that need explicit transaction
boundaries and auditable restart, not a hosted scheduler or a standalone job
runner. See [target users](../product/vision-and-scope.md#target-users) for
the complete audience statement.

## What M5 delivers

M5 stabilizes the M0-M4 embedded scope for a named release rather than adding
capability. It delivers:

- the complete accepted M0-M4 embedded kernel: typed job/step/chunk lifecycle,
  durable PostgreSQL metadata, restart from the last committed checkpoint,
  bounded retry/skip/rollback, finite sequential/conditional flow, bounded
  local split and partitioned steps, a guarded operator CLI, and structured
  telemetry;
- a definition fingerprint that fails closed on drift instead of silently
  restarting a changed definition
  ([restart semantics](#restart-and-definition-drift) below);
- a reviewed, curated public facade (`oxide-batch`) with a closed disclosure
  boundary ([M5 preview surface gate](../api/design-guidelines.md#m5-preview-surface-and-disclosure-gate));
- the `29`-row advertised embedded-kernel set with its evidence campaigns
  already complete (see [#102 reconciliation](../project/m5-102-reconciliation.md)),
  eligible for `Verified` promotion once this version is published and its
  release artifacts are verified — with every other ledger row left visibly
  `Implemented`, `Partial`, `Planned`, or `Unknown` — see
  [limitations](limitations.md), the
  [ledger's M5 disposition set](../compatibility/conformance-matrix.md#m5-disposition-and-promotion-set),
  and the [M5 exit record](../project/m5-exit-evidence.md) for the final
  promoted disposition.

## What M5 explicitly is not

- **Not `1.0` or GA.** `0.x` SemVer applies: an incompatible change may land
  in a minor release. Project-wide `1.0`/GA is M14 scope
  ([RFC-0001](../rfcs/0001-m5-preview-and-project-wide-1-0.md)).
- **Not a standalone job runner.** The `oxide-batch` CLI binary is a guarded
  metadata *operator*, not a Rust job-definition loader. Launching or
  restarting application work requires a host application that embeds
  `oxide-batch-cli` and registers its own compiled `DefinitionCatalog` — see
  the [operator guide](operator-guide.md#what-the-cli-cannot-do).
- **Not distributed.** Single host, embedded in the application process. No
  remote worker, lease, or fencing semantics exist yet (M11).
- **Not full Spring Batch parity.** The compatibility ledger's `39` `Planned`
  and `2` `Unknown` rows remain visible and unresolved; see
  [limitations](limitations.md).

## Installation

```toml
[dependencies]
oxide-batch = { version = "0.5.0", features = ["postgres"] }
```

Omit `features = ["postgres"]` to use only the in-memory reference repository
(development and testing). MSRV is `1.95`; development, CI, and this release
were built with pinned stable `1.97.1`. See the
[support matrix](../release/support-matrix.md#m5-production-preview-support-bounds)
for the complete supported-configuration table.

## Minimal embedded usage

The smallest complete job is the tested example at
[`crates/oxide-batch/examples/first_job.rs`](../../crates/oxide-batch/examples/first_job.rs),
runnable from the workspace root:

```console
cargo run -p oxide-batch --example first_job
```

It constructs an in-memory repository, a single-tasklet job, and a
[`JobLauncher`], launches it with typed identifying parameters, and asserts
the completed outcome. The facade's own crate documentation at
[`crates/oxide-batch/src/lib.rs`](../../crates/oxide-batch/src/lib.rs) carries
further runnable examples for parameters, definition manifests, multi-step
flow graphs, exit-pattern transitions, and fault policy, all under the single
supported import path `use oxide_batch::{...}`. Start there, then follow the
[developer guide](developer-guide.md) for the path from dependency
declaration to a PostgreSQL-backed chunk job.

## PostgreSQL setup

Schema `3` is the only schema this release runs against; a schema-2 runtime
refuses schema 3 on startup, and this release refuses a schema newer than 3.
Migration, role separation (migrator/runtime/operator-reader/operator-writer),
and TLS (`verify-full` only in production) are owned by
[PostgreSQL setup](../operations/postgres-setup.md). PostgreSQL `15` and `18`
are release-blocking; `16`-`17` receive smoke coverage — see the
[support matrix](../release/support-matrix.md#m5-production-preview-support-bounds).

## Runtime model: job, step, and chunk

A job is a named, versioned definition compiled ahead of time into an
immutable [`CompiledExecutionPlan`] with a canonical manifest and definition
fingerprint. A step is either a tasklet (one repeated unit of work) or a
chunk step (bounded read/process/write cycles that commit atomically with
their checkpoint, context, and counters). Multi-step definitions are a
[`FlowGraph`] of steps and deciders joined by exit-pattern transitions,
compiled once and executed many times. See the
[architecture overview](../architecture/overview.md) for the full layer
model and [execution-plan architecture](../architecture/execution-plan.md)
for the compiled-plan and fingerprint contract this release stabilizes.

The compiled plan and every registered component live in the **host
application's process** — OxideBatch does not load, discover, or dynamically
resolve job code. An application constructs its jobs, holds them (typically in
a `DefinitionCatalog` it owns), and calls into `JobLauncher` or `FlowLauncher`
directly.

## Restart and definition drift

Restart resolves the persisted `(definition_id, revision)` to its recorded
manifest and compares the proposed fingerprint **before any lifecycle
write**. An unchanged definition recompiles to the same fingerprint and
resumes from the last committed checkpoint; any restart-relevant change (not
a display name, storage key, or throughput-only budget) changes the
fingerprint and is rejected as drift unless an explicit, reviewed
compatibility edge exists. This is fail-closed by design: a silently
mismatched definition can never resume. See the
[M5 stabilization slice](../architecture/execution-plan.md#m5-stabilization-slice)
for the exact input set and
[crash, restart, and recovery](../operations/crash-restart-and-recovery.md)
for the operational restart procedure, including recovery from a crash or an
ambiguous commit.

## Operator CLI boundary

The `oxide-batch` binary inspects, stops, and recovers durable metadata and
partition state through a closed `noun verb` command grammar. It **cannot**
launch or restart application work on its own — those two commands require a
host-supplied `DefinitionCatalog`. See the [operator guide](operator-guide.md)
for the full walkthrough and the
[operator CLI contract](../operations/operator-cli.md) /
[CLI reference](../operations/operator-cli-reference.md) for the normative
command, configuration, output, and exit-code contract.

## Telemetry and diagnostics

Structured lifecycle, chunk, fault-tolerance, flow, and operations events are
emitted through a non-authoritative, value-redacted `LifecycleEventSink` with
a versioned schema (currently schema version `1`), a fixed span hierarchy,
and a bounded per-execution incident buffer feeding a redacted diagnostic
bundle. See the [observability contract](../operations/observability-contract.md).

## Local parallelism

Bounded local split (`2..=8` declared branches per split node, `1..=8`
running concurrently under the separate parallel-branch budget, `1..=8` steps
per branch) and local partitioning (`1..=1024` partitions, `1..=64` concurrent
workers) run within one process; there is no remote or cross-host execution.
Connection pool and memory sizing formulas, and the M4 provisional measured
budgets, are in
[capacity and resource budgets](../operations/capacity-and-resource-budgets.md).

## Supported environments

Linux x86_64 GNU is the only supported preview runtime target; macOS is
development-only, and Linux aarch64/Windows are not yet supported. The
complete dimension table — Rust, OS/architecture, PostgreSQL majors, TLS,
metadata schema, deployment shape — is the
[M5 support matrix](../release/support-matrix.md#m5-production-preview-support-bounds).

## Limitations

See [limitations](limitations.md) for the complete, ledger-derived list of
what this release does not yet cover and why.

## Upgrade and support expectations

See the [upgrade and rollback guide](upgrade-and-rollback.md) for schema
upgrade, backup, and restore-based rollback procedures, and the
[release and support policy](../release/support-policy.md) for the pre-1.0
latest-line support window this preview follows.

## Developer and operator guides

- [Developer guide](developer-guide.md) — dependency declaration through a
  working embedded job.
- [Operator guide](operator-guide.md) — CLI inspection, stop, recovery, and
  retention, and what the CLI cannot do.
- [Upgrade and rollback guide](upgrade-and-rollback.md) — schema upgrade,
  backup, and restore-based rollback.

[`JobLauncher`]: ../../crates/oxide-batch/src/runtime.rs
[`CompiledExecutionPlan`]: ../../crates/oxide-batch-plan/src/lib.rs
[`FlowGraph`]: ../../crates/oxide-batch-plan/src/lib.rs
