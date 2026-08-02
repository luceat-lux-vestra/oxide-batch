# Operator CLI Reference

**State:** Implemented (M4, issue
[#77](https://github.com/luceat-lux-vestra/oxide-batch/issues/77))

This is the user-facing reference for the `oxide-batch` operator command line.
The observable contract it implements — the closed grammar, configuration
precedence, output schema, exit categories, confirmation rules, and secret
handling — is fixed by the
[M4 operator CLI and configuration contract](operator-cli.md). Where this page
and that contract disagree, the contract governs.

The CLI is a thin client over the portable services in the
[operator, explorer, and retention contract](../architecture/operator-and-explorer-services.md).
It owns no correctness rule of its own, never writes metadata directly, and
adds no hosted API, identity system, scheduler, or user interface. Repository
state rather than CLI output is authoritative.

## Delivery and the definition catalog

The binary is `oxide-batch`, shipped by the optional `oxide-batch-cli` crate.
The library crate gains no binary target and no argument-parsing dependency;
removing the CLI crate removes no correctness capability.

`launch` and `execution restart` are guarded against the job's canonical
`DefinitionIdentity`, which is derived from the live component revisions of the
application that owns the job. A process that only reads metadata cannot
reconstruct one, and accepting a manifest digest from configuration would let an
operator assert an identity the application never produced.

The shipped binary therefore registers no definitions and serves every command
a repository alone can answer. An application that wants to launch or restart
from a command line embeds the crate and supplies a `DefinitionCatalog`:

```rust,ignore
use oxide_batch_cli::DefinitionCatalog;

let catalog = DefinitionCatalog::new().with(orders_identity)?;
let category = oxide_batch_cli::dispatch(
    &mut host, &mut plan, &services, &catalog, deadline,
).await;
```

Asking the shipped binary to launch an unregistered job is a guard rejection
(`JOB_NOT_REGISTERED`, exit `3`), never a silent no-op. The catalog resolves
nothing, persists nothing, and stores no component; it is the narrowest input
these two commands require and is not a definition registry.

## Command grammar

The grammar is `oxide-batch <noun> <verb> [options]`, with `launch` as the one
single-word command. Nouns and verbs are closed sets: there is no plugin, alias,
or dynamic discovery, and an unknown word always fails.

| Command | Class | Required target | Purpose |
| --- | --- | --- | --- |
| `job list` | `Read` | — | List registered job names |
| `job show` | `Read` | `--job` | Show one job's definition identity |
| `instance list` | `Read` | `--job` | List instances of a job |
| `instance show` | `Read` | `--instance` | Show one instance |
| `execution list` | `Read` | `--instance` or `--unresolved-age` | List executions, or stale candidates |
| `execution show` | `Read` | `--execution` | Show one execution and its current redacted recovery proposal when eligible |
| `execution steps` | `Read` | `--execution` | List step executions |
| `execution partitions` | `Read` | `--step` | List partitions of a partitioned step |
| `execution history` | `Read` | `--execution` | List flow, recovery, or operator records |
| `execution stop` | `Lifecycle` | `--execution`, `--expected-version` | Request a durable stop |
| `execution restart` | `Lifecycle` | `--instance`, `--job` | Start a new attempt |
| `execution abandon` | `Destructive` | `--execution`, `--expected-version`, `--reason` | Make an execution permanently non-restartable |
| `execution recover` | `Destructive` | `--execution`, `--expected-version`, `--reason`, `--directive`, `--evidence-digest` | Propose and apply a recovery decision |
| `launch` | `Lifecycle` | `--job` | Launch a registered job |
| `retention plan` | `Read` | `--job`, `--older-than` | Produce a bounded purge plan and digest |
| `retention apply` | `Destructive` | `--job`, `--older-than`, `--plan-digest`, `--reason` | Apply a purge plan |
| `retention hold` | `Destructive` | `--instance`, `--reason` | Place a hold on an instance |
| `retention release` | `Destructive` | `--instance`, `--reason` | Release a hold |
| `config show` | `Read` | — | Print effective configuration with sources |
| `schema status` | `Read` | — | Report schema version and migration state |
| `diagnostics bundle` | `Read` | `--execution` | Write a bounded redacted incident bundle |

Every mutating command additionally requires `--actor`, and `--operation-id`
when standard input is not a terminal.

`stale list` is not a separate command: stale candidates are the age-bounded
form of `execution list --unresolved-age`.

### Two realizations of the contract

Two places where this reference is more specific than the contract table, both
in the stricter direction:

- **`execution history --record operator|recovery|flow`** selects one record
  family per invocation, defaulting to `operator`. One opaque cursor continues
  exactly one keyset traversal, so merging three families into one page would
  make a continuation token ambiguous.
- **`--cursor` is accepted only by paginated commands.** It is listed as a
  global option, but a cursor names a traversal, and a command with no
  traversal would have to ignore it. Unknown and inapplicable options fail
  rather than being ignored. `--page-size` remains global, because
  `config show` reports it as an effective configuration value.

## Options

### Global

| Option | Meaning |
| --- | --- |
| `--config <path>` | Configuration file path |
| `--output human\|json` | Output form, default `human` |
| `--page-size <n>` | `1..=500`, default `50` |
| `--timeout <duration>` | Client deadline, `1s..=1h`, default `60s` |
| `--no-color` | Disable styling |

### Request envelope

| Option | Meaning |
| --- | --- |
| `--operation-id <id>` | Idempotency key for a mutating command |
| `--actor <ref>` | Deployment-supplied opaque actor reference |
| `--reason <code>` | Bounded uppercase reason code |
| `--expected-version <n>` | Observed optimistic version for a mutation |
| `--dry-run` | Validate and report without mutating |
| `--yes` | Confirm a destructive command non-interactively |

### Targets and filters

| Option | Meaning |
| --- | --- |
| `--job <name>` | Target job name |
| `--instance <id>` | Target logical instance |
| `--execution <id>` | Target execution attempt |
| `--step <id>` | Target step execution |
| `--cursor <token>` | Opaque continuation token from a prior page |
| `--unresolved-age <duration>` | Age bound selecting stale candidates |
| `--record operator\|recovery\|flow` | Record family for `execution history` |
| `--directive mark-failed\|abandon` | Recovery disposition |
| `--failure-category <name>`, `--failure-id <n>` | Stated failure of `mark-failed` |
| `--evidence-digest <hex>` | 64-character digest binding a recovery decision |
| `--older-than <duration>`, `--batch <n>`, `--status <STATUS>` | Purge selection |
| `--plan-digest <hex>` | Plan digest a `retention apply` must match |
| `--parameter <name>=<value>`, `--parameters-file <path>` | Launch parameters |
| `--out <path>` | Target path of a diagnostics bundle |

Options accept `--name value` and `--name=value`. `--status` and `--parameter`
may repeat; every other option may not. Unknown options, unknown subcommands,
options a command does not accept, and unknown configuration keys all fail.

Durations are an integer and an explicit unit: `ms`, `s`, `m`, `h`, or `d`. A
bare integer is rejected.

For recovery, first read `data.recovery_proposal` from `execution show`. Use
its `observed_version` and `evidence_digest` without modification in
`execution recover`. The command regenerates the proposal and rejects a stale
version or digest before applying the audited decision. `mark-failed` for an
unknown commit requires `--reason UNKNOWN_EFFECT`; this records an unresolved
external-effect obligation and does not assert whether the effect committed.

## Configuration

Precedence is resolved **per value**, highest priority first:

1. explicit command-line option;
2. environment variable, namespaced `OXIDE_BATCH_`;
3. configuration file;
4. documented framework default.

One source may supply some values while another supplies the rest. `config show`
prints every effective value with its resolved source and redaction status.

### Keys

| Key | Environment variable | Bounds | Default |
| --- | --- | --- | --- |
| `repository.url` | `OXIDE_BATCH_REPOSITORY_URL` | secret | — |
| `repository.ca_certificate` | `OXIDE_BATCH_REPOSITORY_CA_CERTIFICATE` | secret | system roots |
| `repository.tls_mode` | `OXIDE_BATCH_REPOSITORY_TLS_MODE` | `verify_full`, `plaintext` | `verify_full` |
| `repository.pool_size` | `OXIDE_BATCH_REPOSITORY_POOL_SIZE` | `1..=1024` | `10` |
| `repository.connect_timeout` | `OXIDE_BATCH_REPOSITORY_CONNECT_TIMEOUT` | `1ms..=5m` | `10s` |
| `repository.statement_timeout` | `OXIDE_BATCH_REPOSITORY_STATEMENT_TIMEOUT` | `1ms..=24h` | `30s` |
| `output.form` | `OXIDE_BATCH_OUTPUT_FORM` | `human`, `json` | `human` |
| `output.page_size` | `OXIDE_BATCH_OUTPUT_PAGE_SIZE` | `1..=500` | `50` |
| `client.timeout` | `OXIDE_BATCH_CLIENT_TIMEOUT` | `1s..=1h` | `60s` |

### File format

The file is JSON, must declare `config_version` `1`, is bounded to `256 KiB`
and four levels of nesting, and is rejected outright when it is group- or
world-readable on a Unix-like platform:

```json
{
  "config_version": 1,
  "repository": {
    "url__FILE": "/run/secrets/oxide-batch-url",
    "pool_size": 8,
    "statement_timeout": "15s"
  },
  "output": { "form": "json", "page_size": 100 }
}
```

Validation is strict and fail closed. Unknown keys are errors, bounded values
are rejected outside their documented bounds, every safe-to-display conflict is
reported in one pass, and a configuration error exits before any repository
connection is opened.

### Secrets

- No secret is accepted as a command-line argument. Repository passwords and
  connection URLs come from an environment variable, from the configuration
  file, or by file indirection using the `__FILE` suffix on the owning key
  (`repository.url__FILE`, `OXIDE_BATCH_REPOSITORY_URL__FILE`).
- Supplying both the inline value and its `__FILE` form for the same key is an
  error.
- Secrets use dedicated types whose `Debug` and `Display` redact the value.
  `config show` prints the source and `<redacted>`, never the value.
- Errors, diagnostics, and output never contain connection strings, host names,
  user names, passwords, certificate contents or paths, SQL text, bound values,
  parameters, contexts, or checkpoints.
- The CLI writes no credential to disk, to a history file, or to a temporary
  file.

## Output

`--output human` is a stable but explicitly unversioned presentation. It is not
a machine interface and may be reformatted within a release series.

`--output json` emits exactly one JSON object per invocation:

```json
{
  "schema_version": 1,
  "command": "execution list",
  "outcome": "success",
  "data": [],
  "page": { "page_size": 50, "returned": 0, "next_cursor": null },
  "diagnostics": [],
  "truncated": false
}
```

`outcome` is one of `success`, `rejected`, `conflict`, `unknown`, or `error`.
`page` is present only for paginated commands. Both forms render from the same
already-redacted projection, so a redaction rule cannot hold in one form and
leak in the other.

Encoded output is bounded to `256 KiB`. The bound covers the complete written
result, not only `data`, so a large page or diagnostic list cannot push the
envelope past it while still reporting `truncated: false`. Exceeding the bound
drops rows from the end of the page and sets `truncated`; content is never
removed without the flag, and the pagination and diagnostic fields survive
truncation because they are what a caller needs to continue.
Output is written only after a mutating command's durable effect is committed,
so a display failure cannot lose an effect.

A write failure on standard output, including a closed pipe, stops all further
output, performs no additional mutating call, and exits `10`. The operation
identifier lets the caller re-read or replay the request safely.

## Exit categories

| Code | Category | Meaning |
| --- | --- | --- |
| `0` | Success | The command completed; a durable effect, if any, is committed |
| `1` | Usage | The invocation could not be parsed against the closed grammar |
| `2` | Configuration invalid | A value was unknown, out of bounds, or contradictory |
| `3` | Guard rejected | A core guard refused the action; nothing was applied |
| `4` | Target not found | The named target does not exist |
| `5` | Optimistic conflict | The expected version lost its compare-and-swap |
| `6` | Outcome unknown | The durable outcome could not be determined |
| `7` | Repository unavailable | The repository or its infrastructure failed |
| `8` | Confirmation required | A destructive command lacked or was denied confirmation |
| `9` | Deadline exceeded | The client deadline elapsed |
| `10` | Output failure | Standard output could not be written |
| `70` | Internal | A defect; always emits a redacted diagnostic |

Code `6` is **not** a failure. It means the durable outcome is undetermined and
the caller must replay the same operation identifier, which either returns the
recorded outcome or re-attempts the effect exactly once. Code `9` carries the
same warning: the deadline says nothing about whether the effect committed.

Codes are stable and closed. A code is never reused for a different meaning.

## Confirmation and automation safeguards

- Every `Destructive` command requires confirmation. With an interactive
  terminal the CLI prints the target summary, the action class, and the
  operation identifier, then requires an explicit `yes`.
- Without an interactive terminal, `--yes` is required. Its absence exits `8`
  and mutates nothing.
- Confirmation is required even with `--dry-run`, because the contract requires
  it of every destructive command without exception.
- Mutating commands require an explicit `--operation-id` when standard input is
  not a terminal. In interactive use the CLI generates one and prints it before
  executing.
- The CLI never prompts when standard input is not a terminal, and never treats
  an empty response as confirmation.
- `retention apply` additionally requires the plan digest from a prior
  `retention plan`, so a destructive purge cannot be issued from arguments
  alone. A candidate that changed since the plan makes the digest stale and
  exits `5` without deleting anything.

## Examples

Inspect a job's recent attempts as JSON:

```shell
oxide-batch execution list --instance 42 --page-size 20 --output json
```

Continue that traversal:

```shell
oxide-batch execution list --instance 42 --page-size 20 --cursor "$NEXT_CURSOR"
```

Request a durable stop against an observed version:

```shell
oxide-batch execution stop --execution 913 --expected-version 7 --actor ops:alice --operation-id stop-913-1
```

Review a purge before applying it, then apply exactly that plan:

```shell
oxide-batch retention plan --job orders --older-than 90d --output json
```

```shell
oxide-batch retention apply --job orders --older-than 90d --plan-digest "$PLAN_DIGEST" --reason RETENTION_POLICY --actor ops:alice --operation-id purge-orders-7 --yes
```

Inspect effective configuration and its sources:

```shell
oxide-batch config show --output json
```

## Known limitations

- `diagnostics bundle` is part of the closed grammar but is not yet
  implemented. Its contents are owned by the
  [observability contract](observability-contract.md), whose telemetry catalog
  is issue
  [#79](https://github.com/luceat-lux-vestra/oxide-batch/issues/79). Until that
  lands the command exits `3` with `BUNDLE_UNAVAILABLE` rather than writing a
  partial bundle.
- `execution partitions` queries the durable partition rows, which are only
  written once bounded local partitioning lands in issue
  [#80](https://github.com/luceat-lux-vestra/oxide-batch/issues/80). The command
  is correct today and returns an empty page.
- `job show` reports the definition identity recorded on the newest attempt of
  the newest instance, because the repository records the identity it guarded.
  A job with no attempt yet reports exit `4`.
- `schema status` is available only against a `PostgreSQL` repository. An
  adapter with no durable schema reports `UNSUPPORTED_CAPABILITY`.
- The CLI is not a scheduler, a daemon, or a hosted control plane, and none of
  its commands imply one.
