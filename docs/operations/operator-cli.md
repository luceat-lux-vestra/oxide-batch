# M4 Operator CLI and Configuration Contract

**State:** Accepted

**Governing decisions:**
[RFC-0008](../rfcs/0008-core-and-control-plane-boundary.md) and
[ADR-0007](../architecture/decisions/0007-control-plane-boundary.md)

This document is the canonical contract for the minimal first-party operator
CLI, its configuration precedence, output, exit categories, confirmation rules,
and secret handling. It fixes observable behavior; the user-facing command
reference is published by the workstream that implements it.

The CLI is a thin client over the portable services in the
[operator, explorer, and retention contract](../architecture/operator-and-explorer-services.md).
It owns no correctness rule of its own. It adds no hosted API, no identity
system, no scheduler, and no UI.

## Delivery boundary

The binary is `oxide-batch`, shipped by an optional workspace crate created by
the implementing workstream. The library crate does not gain a binary target, a
CLI dependency, or an argument-parsing dependency. Removing the CLI crate
removes no correctness capability.

## Command grammar

The grammar is `oxide-batch <noun> <verb> [options]`. Nouns and verbs are
closed sets; there is no plugin, alias, or dynamic command discovery.

| Command | Class | Purpose |
| --- | --- | --- |
| `job list` | `Read` | List registered job names |
| `job show` | `Read` | Show one job's definition identity |
| `instance list` | `Read` | List instances of a job |
| `instance show` | `Read` | Show one instance |
| `execution list` | `Read` | List executions of an instance |
| `execution show` | `Read` | Show one execution projection |
| `execution steps` | `Read` | List step executions |
| `execution partitions` | `Read` | List partitions of a partitioned step |
| `execution history` | `Read` | List flow, recovery, and operator records |
| `execution stop` | `Lifecycle` | Request a durable stop |
| `execution restart` | `Lifecycle` | Start a new attempt |
| `execution abandon` | `Destructive` | Make an execution permanently non-restartable |
| `execution recover` | `Destructive` | Propose and apply a recovery decision |
| `launch` | `Lifecycle` | Launch a registered job |
| `retention plan` | `Read` | Produce a bounded purge plan and digest |
| `retention apply` | `Destructive` | Apply a purge plan |
| `retention hold` | `Destructive` | Place a hold on an instance |
| `retention release` | `Destructive` | Release a hold |
| `config show` | `Read` | Print effective configuration with sources |
| `schema status` | `Read` | Report schema version and migration state |
| `diagnostics bundle` | `Read` | Write a bounded redacted incident bundle |

`stale list` is not a separate command; stale candidates are the age-bounded
form of `execution list`.

## Global options

| Option | Meaning |
| --- | --- |
| `--config <path>` | Configuration file path |
| `--output human\|json` | Output form, default `human` |
| `--page-size <n>` | `1..=500`, default `50` |
| `--cursor <token>` | Opaque continuation token from a prior page |
| `--operation-id <id>` | Idempotency key for a mutating command |
| `--actor <ref>` | Deployment-supplied opaque actor reference |
| `--reason <code>` | Bounded reason code from the closed set |
| `--expected-version <n>` | Observed optimistic version for a mutation |
| `--dry-run` | Validate and report without mutating |
| `--yes` | Confirm a destructive command non-interactively |
| `--timeout <duration>` | Client deadline, bounded `1 s..=1 h`, default `60 s` |
| `--no-color` | Disable styling |

Unknown options, unknown subcommands, and unknown configuration keys fail;
they are never ignored.

## Configuration precedence

The accepted precedence in the
[configuration model](../architecture/configuration-model.md) is binding for
the CLI, highest priority first:

1. explicit command-line option;
2. environment variable, namespaced `OXIDE_BATCH_`;
3. configuration file;
4. documented framework default.

Precedence is resolved per value, not per source, so a file may supply the
repository pool size while an option supplies the page size. `config show`
prints every effective value with its resolved source and redaction status, and
`config show --output json` is the machine-readable form of the same data.

Validation is strict and fail-closed:

- unknown keys and options are errors;
- durations, sizes, counts, and concurrency use bounded typed values and are
  rejected outside their documented bounds;
- contradictory values report every safe-to-display conflict in one pass;
- a configuration error exits before any repository connection is opened.

## Secret handling

- No secret is accepted as a command-line argument. Repository passwords and
  connection URLs are supplied by environment variable, by a configuration
  file, or by file indirection using the `__FILE` suffix on the owning key.
- A configuration file that is group- or world-readable is rejected on
  Unix-like platforms.
- Secrets use dedicated types whose `Debug` and `Display` redact the value.
  `config show` prints the source and a redaction marker, never the value.
- Errors, diagnostics, bundles, and telemetry never contain connection
  strings, hostnames, usernames, passwords, certificate contents or paths, SQL
  text, bound values, parameters, contexts, or checkpoints.
- The CLI writes no credential to disk, to a history file, or to a temporary
  file.

## Output

`--output human` is a stable but explicitly unversioned presentation. It is
not a machine interface and may be reformatted within a release series.

`--output json` emits exactly one JSON object per invocation:

- `schema_version`, the integer CLI output schema version, currently `1`;
- `command`, the canonical noun and verb;
- `outcome`, one of `success`, `rejected`, `conflict`, `unknown`, or `error`;
- `data`, the redacted projection of the command result;
- `page`, present for paginated commands, carrying `page_size`, `returned`,
  and `next_cursor`;
- `diagnostics`, a bounded array of redacted diagnostic records;
- `truncated`, `true` when a bound removed content.

The JSON form obeys the same redaction rules as the explorer. Encoded output is
bounded to `256 KiB`; exceeding the bound sets `truncated` and never silently
drops content without the flag. Output is written only after a mutating
command's durable effect is committed, so a display failure cannot lose an
effect.

A write failure on standard output, including a closed pipe, stops all further
output, performs no additional mutating call, and exits with the output-failure
category. The operation ID lets the caller re-read or replay the request
safely.

## Exit categories

Exit codes are stable, closed, and never reused for a different meaning.

| Code | Category |
| --- | --- |
| `0` | Success |
| `1` | Usage or argument error |
| `2` | Configuration invalid |
| `3` | Guard rejected the action |
| `4` | Target not found |
| `5` | Optimistic conflict |
| `6` | Outcome unknown |
| `7` | Repository or connectivity failure |
| `8` | Confirmation required or declined |
| `9` | Deadline exceeded |
| `10` | Output write failure |
| `70` | Internal error |

Code `6` means the durable outcome could not be determined and the caller must
replay the same operation ID. It never means failure. An internal error is a
defect and always emits a redacted diagnostic.

## Confirmation and non-interactive safeguards

- Every `Destructive` command requires confirmation. With an interactive
  terminal the CLI prints the exact target summary, the action class, and the
  operation ID, then requires an explicit affirmative response.
- Without an interactive terminal, `--yes` is required. Its absence exits `8`
  and mutates nothing.
- Mutating commands require an explicit `--operation-id` when standard input
  is not a terminal. In interactive use the CLI generates one, prints it before
  executing, and repeats it in the result.
- `--dry-run` is available for `launch`, `execution restart`,
  `execution recover`, and `retention apply`. It validates guards, prints the
  plan or evidence digest, and performs no mutation.
- The CLI never prompts when standard input is not a terminal and never treats
  an empty response as confirmation.
- `retention apply` additionally requires the plan digest from a prior
  `retention plan`, so a destructive purge cannot be issued from arguments
  alone.

## Diagnostics bundle

`diagnostics bundle` writes a bounded, redacted directory or archive for one
named execution. Its contents are specified by the
[observability contract](observability-contract.md). The CLI adds no data
beyond that contract, refuses to overwrite an existing target, and reports the
bundle manifest checksum.

## Evidence

Production implementation requires:

- deterministic tests for precedence resolution per value, unknown-key
  rejection, and bounds validation;
- golden tests for the JSON output schema, pagination fields, and truncation
  flag;
- exit-category tests covering every code in the table;
- confirmation tests for interactive, non-interactive, missing `--yes`,
  declined, and missing operation-ID paths;
- redaction tests proving no secret, parameter, context, checkpoint, SQL, or
  user error text reaches any output form or the process environment dump;
- broken-pipe and closed-stdout tests proving no repeated mutation and the
  correct exit category;
- dry-run tests proving no durable change;
- a file-permission test rejecting a world-readable configuration file.
