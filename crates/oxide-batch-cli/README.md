# oxide-batch-cli

The minimal guarded operator command line for
[OxideBatch](https://github.com/luceat-lux-vestra/oxide-batch).

This crate ships the `oxide-batch` binary and the embeddable library behind it.
It is a thin client over the portable operator, explorer, and retention
services: it owns no correctness rule of its own, writes no metadata directly,
and adds no hosted API, identity system, scheduler, or user interface.
Repository state rather than CLI output is authoritative.

## Using the binary

```shell
oxide-batch execution list --instance 42 --output json
```

Configuration resolves per value across explicit options, `OXIDE_BATCH_`
environment variables, a JSON configuration file, and documented defaults.
Secrets are never accepted as command-line arguments.

```shell
export OXIDE_BATCH_REPOSITORY_URL__FILE=/run/secrets/oxide-batch-url
oxide-batch config show --output json
```

## Embedding the library

`launch` and `execution restart` are guarded against a job's canonical
`DefinitionIdentity`, which only the application that owns the job can build. A
host application registers its definitions and drives the same command surface:

```rust,ignore
use oxide_batch_cli::{DefinitionCatalog, ProcessHost};

let catalog = DefinitionCatalog::new().with(orders_identity)?;
let mut host = ProcessHost::new();
let mut plan = oxide_batch_cli::prepare(&mut host, &arguments)?;
let category =
    oxide_batch_cli::dispatch(&mut host, &mut plan, &services, &catalog, deadline).await;
std::process::exit(i32::from(category.code()));
```

The shipped binary registers no definitions, so it serves every command a
repository alone can answer and reports a guard rejection for those two. It is
not a standalone job-definition loader; it cannot discover a job from a crate,
manifest file, or database row.

```shell
oxide-batch --help
```

## Documentation

- [Operator CLI reference](https://github.com/luceat-lux-vestra/oxide-batch/blob/main/docs/operations/operator-cli-reference.md)
  — commands, options, configuration keys, output schema, and exit categories.
- [Operator CLI and configuration contract](https://github.com/luceat-lux-vestra/oxide-batch/blob/main/docs/operations/operator-cli.md)
  — the accepted observable contract this crate implements.

## Features

- `postgres` (default) — the `PostgreSQL` repository backend and the
  `oxide-batch` binary. Without it the crate is the runtime-neutral command
  surface only.

## License

Apache-2.0. See [LICENSE](https://github.com/luceat-lux-vestra/oxide-batch/blob/main/LICENSE).
