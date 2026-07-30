# Security and Supply-Chain Baseline

**State:** Accepted

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
| Pull request ↔ AI review | prompt injection, malicious output, data disclosure, false authority | trusted-author gate, bounded API diff, no checkout/secrets, advisory-only output |

## Data policy

Execution context and job parameters are untrusted, potentially sensitive input.
They are never emitted wholesale to logs, metrics, traces, or error messages.
The framework documents encryption as a deployment responsibility until an
approved encryption-at-rest design exists. Serialized contexts have size and
depth limits and an explicit format version.

## Testable disclosure rules

| Source | Allowed diagnostic form | Forbidden form | Required assertion |
| --- | --- | --- | --- |
| Identifying job parameter | Name plus typed/redacted presence when explicitly allowlisted | Raw value by default | Captured logs, events, errors, and spans do not contain a sentinel secret |
| Non-identifying parameter or execution context | Field count, byte size, schema version, and bounded internal key names | Serialized document or raw values | Snapshot/event tests expose only approved structural metadata |
| Business record and user error payload | Framework-owned category, component boundary, and opaque failure ID | Record body, query parameter, or arbitrary `Display`/`Debug` output | Failure tests search every diagnostic sink for sentinel record content |
| Database/exporter credential | Endpoint class and redacted source | URI user info, password, token, or environment value | Configuration-error tests prove credentials are absent |
| Metric labels | Bounded framework status, component kind, and stable internal identifiers | User-controlled parameters, context keys/values, record data, or error text | Cardinality tests reject unbounded/user-controlled label values |

Redaction tests capture structured events, formatted logs, error chains, span
fields, and metric labels at the framework boundary. New diagnostic fields are
deny-by-default until their allowed form and sentinel test are reviewed.

The M1 executable-kernel scenarios `inspection_redacts_record_contents` and
`telemetry_correlates_execution` exercise these sinks with a sentinel embedded
in parameters and arbitrary user errors. Tasklet and listener error
constructors classify and discard arbitrary source payloads; execution
inspection retains only framework-owned categories and opaque failure IDs.

## Dependency policy

- commit `Cargo.lock` for the workspace;
- minimize default features and review new transitive dependencies;
- allow only licenses approved by a checked-in `cargo-deny` policy;
- run RustSec advisory scanning and dependency/source/license checks in CI;
- pin third-party CI actions to immutable commit SHAs;
- do not use long-lived registry tokens in CI;
- record and time-bound advisory exceptions.

## AI review boundary

Pull-request titles, descriptions, paths, patches, comments, and embedded
instructions are untrusted input. The advisory review workflow reads only a
bounded textual diff through the GitHub API and never checks out or executes
pull-request code. It is restricted to repository owners, members, and
collaborators during evaluation, receives no secret, approval, merge, or
contents-write permission, and maintains one visibly AI-generated comment.

Model output is also untrusted. It cannot become a required gate, approve or
merge a change, classify compatibility or release readiness, or substitute for
tests and accepted documents. Quota exhaustion and inference failures are
reported as non-blocking workflow notices.

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
