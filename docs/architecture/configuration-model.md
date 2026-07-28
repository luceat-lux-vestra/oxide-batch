# Configuration Model

**State:** Proposed

OxideBatch is primarily a library. Application code owns configuration assembly;
the framework provides typed configuration values and validation. A CLI may add
file/environment/argument loading without making that mechanism mandatory for
library users.

## Proposed precedence

For the first-party CLI, highest priority wins:

1. explicit command-line option;
2. environment variable;
3. configuration file;
4. documented framework default.

Application code may assemble typed configuration differently but receives the
same validation and effective-configuration diagnostics.

## Configuration classes

| Class | Examples | Change behavior |
| --- | --- | --- |
| Definition | step flow, chunk size, retry/skip policy | Version/restart compatibility impact |
| Runtime | concurrency, timeouts, shutdown deadline | May vary by execution within safe bounds |
| Repository | URL, pool, schema namespace | Deployment-controlled, secret-bearing |
| Telemetry | filters, exporter, sampling | Must not alter correctness |
| Operator | output format, confirmation, query limits | CLI-only |

Changing a job definition between failed execution and restart must be governed
by job-definition compatibility, not merely accepted because configuration
parses.

## Validation

- Unknown fields/options fail by default in first-party configuration.
- Durations, sizes, limits, and concurrency use bounded typed values.
- Conflicting options report all safe-to-display conflicts where practical.
- Secrets have dedicated types whose `Debug`/`Display` redact values.
- Effective configuration can be inspected with sources and redacted values.
- Environment-variable names are namespaced with `OXIDE_BATCH_`.

## Files and formats

No file format is selected in M0. If introduced, it must support:

- explicit schema/configuration version;
- strict parsing and useful source locations;
- deterministic merge semantics;
- secret indirection rather than requiring plaintext secrets;
- stable deprecation/migration diagnostics;
- size/depth limits for untrusted input.

## Defaults

Defaults prioritize data integrity and bounded resource use. There is no
unbounded retry, queue, concurrency, context, output, or shutdown wait. A
default that affects compatibility, data, or resource risk is documented in
the user reference and covered by a test.
