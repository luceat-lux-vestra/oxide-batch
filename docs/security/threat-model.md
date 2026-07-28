# Security and Supply-Chain Baseline

**State:** Proposed

**Scope:** framework, release pipeline, metadata repository, and operator tools

## Protected assets

- job parameters, execution context, and business record contents;
- database credentials and telemetry-export credentials;
- integrity of job definitions, checkpoints, and lifecycle status;
- release artifacts, source history, and crates.io publishing identity;
- availability of workers and the metadata repository.

## Trust boundaries and threats

| Boundary | Representative threats | Required controls |
| --- | --- | --- |
| User job ↔ framework | panic, blocking, resource exhaustion, malformed context | panic/error isolation, bounded resources, size limits, typed validation |
| Framework ↔ PostgreSQL | injection, credential exposure, races, partial commit | bound queries, least privilege, TLS guidance, locking/transaction tests |
| CLI/operator ↔ metadata | unauthorized stop/recover, accidental mutation | explicit confirmation/force modes, audit events, no secret output |
| Telemetry/export | parameter or context leakage, cardinality attack | deny-by-default fields, redaction, bounded labels |
| Build/release | dependency compromise, token theft, workflow mutation | lockfile, review, pinned actions, OIDC publishing, provenance |

## Data policy

Execution context and job parameters are untrusted, potentially sensitive input.
They are never emitted wholesale to logs, metrics, traces, or error messages.
The framework documents encryption as a deployment responsibility until an
approved encryption-at-rest design exists. Serialized contexts have size and
depth limits and an explicit format version.

## Dependency policy

- commit `Cargo.lock` for the workspace;
- minimize default features and review new transitive dependencies;
- allow only licenses approved by a checked-in `cargo-deny` policy;
- run RustSec advisory scanning and dependency/source/license checks in CI;
- pin third-party CI actions to immutable commit SHAs;
- do not use long-lived registry tokens in CI;
- record and time-bound advisory exceptions.

## Vulnerability handling

Private reporting follows `SECURITY.md`. A confirmed vulnerability receives
severity, affected versions, mitigation, owner, and coordinated disclosure
status. Security fixes may bypass normal release cadence but not artifact
integrity checks.

## Explicit limitations

OxideBatch is a library and does not itself authenticate application users,
manage database secrets, encrypt the database, or isolate hostile native code
inside the same process. Deployment guidance must make these responsibilities
clear.
