# M5 Operator Guide

**State:** Accepted

**Applies to:** OxideBatch `0.5.0`, the M5 Embedded Core Production Preview

A narrative walkthrough of the `oxide-batch` operator CLI for someone running
this release in production. The normative contract lives in
[the operator CLI contract](../operations/operator-cli.md) and the
[CLI reference](../operations/operator-cli-reference.md); this guide orders
that material for a first read and states plainly what the CLI cannot do. See
[documentation strategy](../documentation/strategy.md) for that ownership
split.

## What the CLI cannot do

Read this first. `oxide-batch` is a **guarded repository operator**, not a
Rust job-definition loader. It has no way to discover, compile, or load your
application's job code. `launch` and `execution restart` are guarded against
a job's canonical `DefinitionIdentity`, which only your application can
produce from its own live component revisions — accepting an identity from
configuration instead would let an operator assert one your application never
built. The shipped binary registers no definitions, so these two commands
always report a guard rejection unless you embed `oxide-batch-cli` in your own
application and register your compiled definitions in a `DefinitionCatalog`
(see the [developer guide](developer-guide.md#7-embed-the-operator-cli)).
Every other command — inspection, stop, recovery, retention, diagnostics —
works against the shipped binary with only a database connection.

## Connecting

Configuration resolves per value across four sources, highest priority first:
command-line option, `OXIDE_BATCH_`-namespaced environment variable,
configuration file, documented default. `config show` prints every effective
value with its source and redaction status — run it first against a new
environment. A configuration file that is group- or world-readable is
rejected outright on Unix-like platforms; secrets are never accepted as
command-line arguments, only by environment variable, configuration file, or
`__FILE`-suffixed file indirection.

Use a distinct database identity per operator concern: an **operator-reader**
identity is enough for every `Read`-class command; only **operator-writer**
can perform the `Destructive`- and `Lifecycle`-class commands. See
[roles and database ownership](../operations/postgres-setup.md#roles-and-database-ownership).

## Inspect

The `Read`-class commands (`job list`/`show`, `instance list`/`show`,
`execution list`/`show`/`steps`/`partitions`/`history`, `retention plan`,
`config show`, `schema status`) are safe to run at any time against an
operator-reader connection; none mutates. `execution history` returns exactly
one record family per call (flow, recovery, or operator records) because one
opaque cursor continues exactly one keyset traversal. Every projection is
redacted: no parameter value, execution context, checkpoint payload, business
record, SQL text, or credential is ever returned.

## Stop

`execution stop` requests a durable, cooperative stop. It does not kill the
process. The owning runtime observes the request, stops accepting new intake,
lets in-flight work reach its declared `InFlightPolicy` boundary (finish the
open chunk or roll it back to the previous checkpoint), and commits a durable
terminal result. A stop request against a process that is not currently
running the execution has nothing to signal; the durable request is still
recorded and observed the next time that execution's runtime checks it.

## Recover

An execution can be left `STARTING`, `STARTED`, `STOPPING`, or `UNKNOWN` after
a crash, `SIGKILL`, host loss, or power loss — none of which OxideBatch
distinguishes from an ordinary process exit until you decide. Recovery is
always an explicit, evidence-based, audited decision; there is no automatic
stale-worker takeover. Walk `execution show` (for the redacted
`recovery_proposal` and its evidence digest), then `execution recover` with
that digest, the observed version, a disposition (`FAILED` or `ABANDONED`),
and a bounded reason code. The full procedure, including what counts as valid
staleness evidence and how to handle an execution whose external effect is
genuinely ambiguous, is the
[crash, restart, and recovery runbook](../operations/crash-restart-and-recovery.md#produce-stale-or-unknown-recovery-evidence).

## Restart

`execution restart` (from an embedding host application only — see
[what the CLI cannot do](#what-the-cli-cannot-do)) starts a fresh attempt from
the last committed checkpoint, guarded by the definition fingerprint. See
[restart and definition drift](production-preview.md#restart-and-definition-drift).

## Retention

`retention plan` (read-only) produces a bounded purge plan and a digest;
`retention apply` (destructive) requires that exact digest, so a purge can
never be issued from arguments alone. `retention hold`/`release` protect an
instance's history from purge without blocking any lifecycle action on it.
Purge never targets a running, stopping, ambiguous, or held instance, deletes
in instance-owned order inside one transaction per bounded batch, and is
always audited. Purge has no reverse operation — the only way back is
restoring a verified backup. See
[retention primitives](../architecture/operator-and-explorer-services.md#retention-primitives)
for the eligibility and privilege-separation contract.

## Diagnostics

`diagnostics bundle` writes a bounded (`4 MiB`), redacted incident bundle for
one named execution, specified by the
[observability contract](../operations/observability-contract.md#diagnostic-bundles).
It never overwrites an existing target and reports a checksum.

## Destructive-action safeguards

Every `Destructive` command (`execution abandon`, `execution recover`,
`retention apply`, `retention hold`/`release`) requires explicit confirmation:
an interactive terminal prompts for it, and a non-interactive one requires
`--yes` or exits without mutating. Mutating commands additionally require an
explicit `--operation-id` when standard input is not a terminal, which is the
key you use to safely re-read or replay a request whose result you did not
see. `--dry-run` validates guards and prints the plan or evidence digest for
`launch`, `execution restart`, `execution recover`, and `retention apply`
without mutating, but it does not waive the confirmation a destructive command
still requires.

## Exit codes

Codes are stable and never reused for a different meaning. The one to
internalize is `6`, "outcome unknown" — it means the durable result could not
be determined, not that the operation failed, and the caller should replay
the same operation ID. The complete table is in
[the operator CLI contract](../operations/operator-cli.md#exit-categories).

## Authorization

The CLI performs no authentication or authorization of its own; it trusts the
database identity it is given. Deployment tooling is responsible for issuing,
rotating, and scoping the operator-reader/operator-writer credentials named
above, and for authenticating the human or system invoking the CLI before
that credential is ever used.

## Next

- [Upgrade and rollback guide](upgrade-and-rollback.md) before a schema
  upgrade.
- [Limitations](limitations.md) for what stop, recover, and retention do not
  yet cover.
