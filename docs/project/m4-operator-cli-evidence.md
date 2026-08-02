# M4 Operator CLI Evidence

**State:** Complete on merge

**Issue:** [#77](https://github.com/luceat-lux-vestra/oxide-batch/issues/77)

**Date:** 2026-08-02

This record links the executable evidence for the minimal guarded operator CLI
and its configuration diagnostics. It closes the CLI implementation stream of
M4. It does not close M4, and it moves no ledger row toward released `Verified`
status.

## Delivered boundary

The `oxide-batch-cli` crate ships the `oxide-batch` binary and the embeddable
library behind it. The `oxide-batch` library crate gained no binary target, no
argument-parsing dependency, and no CLI dependency; removing the CLI crate
removes no correctness capability.

Argument parsing is hand written rather than delegated to a parser library, so
the closed grammar, the rejection of every unknown word, and the stable exit
categories are observable properties of this repository rather than of a
dependency's defaults. The crate adds `futures-util` and `serde_json`, both
already in the workspace graph.

The CLI calls only the portable operator, explorer, and retention services. It
writes no metadata directly and owns no guard, compare-and-swap, idempotency
record, or audit row of its own.

## Named scenario evidence

`OPS-CLI-001` requires the following scenarios. Each is a test of the same name
in `crates/oxide-batch-cli/tests/operator_cli.rs`.

| Scenario | Evidence |
| --- | --- |
| `precedence_resolves_per_value` | One invocation resolves the page size from an option, the output form from the environment, the pool size from the file, and the client timeout from the default, and `config show` reports each source. |
| `unknown_option_or_configuration_key_fails` | An unknown option, an unknown subcommand, an option a command does not accept, and an unknown configuration key each fail; the configuration case writes no result and contacts no repository. |
| `every_exit_category_is_returned_by_its_named_case` | Each of the twelve published codes is produced by a named case, and the test asserts that the set of covered codes equals `ExitCategory::all()`, so a new category cannot be added without a case proving it. |
| `destructive_command_without_yes_exits_confirmation_required` | Non-interactive without `--yes`, interactive and declined, and interactive with no response all exit `8` and mutate nothing; the prompt names the target, the action class, and the operation identifier. |
| `dry_run_makes_no_durable_change` | A dry-run launch reports the action and request digest with `applied: false`, and the job still has no instance afterwards; a dry-run purge is refused by the stale-plan guard without deleting. |
| `broken_stdout_stops_output_and_repeats_no_mutation` | A closed pipe exits `10` after exactly one write attempt; a hold whose output fails is applied once, and replaying its operation identifier returns `REPLAYED` rather than applying a second hold. |
| `json_output_matches_the_published_schema_and_redaction_rules` | The envelope carries exactly the seven published fields at schema version `1`; a connection string, password, host name, and certificate are absent from both output forms and the affected rows report `<redacted>`. |

Supporting tests in the same file cover the unversioned and world-readable
configuration file, the non-interactive operation-identifier requirement, the
dry-run request digest, and the absence of driver text from diagnostics.
Module tests cover the exit-code table, the closed grammar, cursor and page
bounds, duration parsing, secret redaction, output truncation, and the failure
to exit-category mapping.

## Evidence classes satisfied

| Class | Result |
| --- | --- |
| Grammar and argument rejection | Unit and scenario tests; every unknown or inapplicable option fails |
| Configuration precedence and bounds | Per-value resolution, one-pass conflict reporting, and file-permission rejection |
| Golden output schema | Envelope field set, pagination fields, and truncation flag asserted against the published schema |
| Exit categories | Every code returned by a named case, with an equality assertion against the published set |
| Confirmation and automation | Interactive, declined, silent, and non-interactive paths |
| Redaction | Secret, host, and certificate absence proven in both output forms |
| Broken output | Single write attempt, correct category, and no repeated mutation |
| Dry run | No durable change observable after a dry run |

## Checks run

```shell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo check -p oxide-batch --no-default-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
```

All five passed. No PostgreSQL integration check was run for this change: the
CLI's repository behavior is the already-evidenced behavior of the M4 services,
and the one new library function, `PostgresMigrator::installed_schema_version`,
is exercised by the M4 PostgreSQL matrix in issue
[#81](https://github.com/luceat-lux-vestra/oxide-batch/issues/81) rather than
here. `schema status` therefore carries no PostgreSQL evidence yet.

## Decisions recorded

### The definition catalog

`launch` and `execution restart` are guarded against the job's canonical
`DefinitionIdentity`, which is derived from the live component revisions of the
owning application. A process that only reads metadata cannot reconstruct one,
and accepting a manifest digest from configuration would let an operator assert
an identity the application never produced.

The CLI therefore takes a host-supplied `DefinitionCatalog`. The shipped binary
registers none and serves every command a repository alone can answer;
launching an unregistered job is a `JOB_NOT_REGISTERED` guard rejection rather
than a silent no-op. The catalog resolves nothing, persists nothing, and stores
no component, so it is not the definition registry the M4 kickoff excludes.

### Two stricter realizations of the contract

- `execution history` selects one record family per invocation via `--record`,
  defaulting to `operator`. One opaque cursor continues exactly one keyset
  traversal; merging three families into one page would make a continuation
  token ambiguous.
- `--cursor` is accepted only by paginated commands. It is a global option, but
  a cursor names a traversal, and a command without one would have to ignore
  it, which the contract forbids. `--page-size` remains global because
  `config show` reports it as an effective value.

Both are recorded in the
[operator CLI reference](../operations/operator-cli-reference.md) and are
stricter than the contract rather than looser.

### Confirmation applies to dry runs

`--dry-run` does not waive confirmation for a destructive command, because the
contract requires confirmation of every destructive command without exception.

### Safeguards run before any connection

Argument parsing, configuration resolution, the operation-identifier
requirement, and the confirmation prompt all run in `prepare`, before a
repository connection is opened. A destructive command refused for want of
`--yes` therefore contacts no repository at all, and a configuration error is
always reported before any connection attempt.

## Residual risk and limitations

- At this issue #77 evidence boundary, `diagnostics bundle` was deliberately
  unavailable. Issue #79 subsequently implemented it; current evidence is the
  [M4 bounded telemetry record](m4-telemetry-evidence.md), while this historical
  CLI record continues to describe the earlier merge boundary.
- `execution partitions` is correct but returns an empty page until bounded
  local partitioning lands in issue #80.
- `schema status` has no PostgreSQL evidence yet, as recorded above.
- The human output form is deliberately unversioned and is not a machine
  interface.
- No load, soak, cardinality, or telemetry-overhead evidence is claimed for the
  CLI; those belong to issue #81.
- `oxide-batch-cli` declares `publish = ["crates-io"]` but cannot be packaged
  until `oxide-batch` is published, because it depends on the facade by path and
  version. Release automation still packages only `oxide-batch`; extending it to
  publish the two crates in order belongs to the M5 release gate.

## Ledger effect

`OPS-CLI-001` moves from `Planned` to `Implemented` with the scenario evidence
above. It does not become released `Verified`, which requires a named release
satisfying the compatibility contract's complete evidence profile.
