# M5 Staged Crate-Extraction Contract

**State:** Accepted

**Governing decisions:** [RFC-0003](../rfcs/0003-target-workspace-boundaries.md)
and [ADR-0001](decisions/0001-workspace-and-facade.md)

This document closes the extraction order, boundary set, forbidden-dependency
rules, equivalence obligations, packaging checks, measurements, and reversal
procedure that RFC-0003 left as an unresolved question. It authorizes the
stages named below and nothing else.

Extraction is behavior-preserving repackaging. It is not a rewrite, not an
optimization, and not a public-crate proposal.

## Current state

The workspace holds one implementation crate, `oxide-batch`, plus
`oxide-batch-cli`, the `m0-architecture` spike, and `xtask`. `oxide-batch` is
the curated facade and the entire library implementation.

## Authorized stages

Stages run in the order below. A stage begins only after the previous stage's
evidence passes.

| Stage | Boundary | Moved content | M5 status |
| --- | --- | --- | --- |
| 1 | `oxide-batch-core` | identities, parameters, statuses, lifecycle rules, execution-context values, definition types, and their validation | Authorized |
| 2 | `oxide-batch-repository` | repository, explorer, operator, retention, and transaction ports, capability descriptors, and their contract suite | Authorized |
| 3 | `oxide-batch-plan` | plan compilation, graph normalization, manifest encoding, and fingerprinting | Authorized only after the M5 plan and fingerprint stabilization slice lands |

Engine, item, adapter, observability, test-kit, distributed-protocol, and
integration boundaries are **deferred past M5**. They are named by RFC-0003 as
target direction and are not authorized by this gate.

Every extracted crate is `publish = false`. Public-crate approval remains a
separate decision under the
[crate publishing policy](../governance/crate-publishing.md).

## Forbidden dependencies

- `oxide-batch-core` MUST NOT depend on Tokio, SQLx, Clap, OpenTelemetry SDKs,
  brokers, HTTP or web frameworks, or any other extracted OxideBatch crate.
- `oxide-batch-repository` MUST NOT depend on SQLx, Clap, OpenTelemetry SDKs,
  or `oxide-batch-plan`. It may depend on `oxide-batch-core`.
- `oxide-batch-plan` MUST NOT depend on Tokio, SQLx, Clap, or OpenTelemetry
  SDKs. It may depend on `oxide-batch-core` and `oxide-batch-repository`.
- No cycle may exist between workspace crates.
- No extracted crate may re-export a driver, runtime, or telemetry-SDK type
  through a public signature.

A dependency check runs in CI and fails the build on any violation. The check
is authoritative; a passing manual review does not substitute for it.

## Facade and API equivalence

Each stage MUST satisfy all of the following before it is accepted:

- every supported `oxide-batch` import path resolves to the same item, through
  re-export where the item moved;
- the public API snapshot of `oxide-batch` is byte-identical to the snapshot
  taken immediately before the stage, or the difference is an accepted,
  separately reviewed API change;
- rustdoc for `oxide-batch` discloses no newly leaked implementation type;
- the full unit, property, contract, conformance, crash, and PostgreSQL suites
  pass unchanged;
- compile-fail tests for typed component incompatibility still fail to compile
  for the same reason.

## Durable-invariance obligation

A stage MUST change no persisted byte, transaction boundary, lifecycle write,
restart selection, definition fingerprint, or normalized trace. Golden
fingerprint vectors and normalized repository-write traces are compared before
and after each stage, and a difference fails the stage. There is no accepted
migration path for an extraction-induced durable change: the stage is reverted
instead.

## Packaging and measurement

Each stage records:

- `cargo xtask package` results, including publish dry-run and packaged file
  count for every workspace crate;
- clean and incremental build time for the workspace and for `oxide-batch`
  alone;
- release binary size of `oxide-batch-cli`;
- the module dependency graph before and after the move.

Measurements are recorded as provisional observations under the
[performance plan](../engineering/performance-plan.md). A build-time or
binary-size change is reported, not gated, unless it crosses a budget that a
later decision makes binding.

## Reversal

Each stage is one revertible commit. Reversal restores the previous internal
module layout and changes no facade path, persisted byte, or metadata value.
Because a stage may not alter durable state, reversal requires no migration and
no operator action. A stage that cannot be reverted this way is out of scope for
this contract and requires a superseding decision.

## Evidence

Stage acceptance requires:

- the CI forbidden-dependency and cycle check;
- the public API snapshot comparison and rustdoc leakage inspection;
- the complete existing test suite, including PostgreSQL integration and
  crash fixtures;
- golden fingerprint and normalized-trace comparison;
- `cargo xtask check` and `cargo xtask package`;
- the recorded build-time, binary-size, and dependency-graph measurements.

Documentation names are acceptance targets, not evidence links, until the
checks exist and pass.
