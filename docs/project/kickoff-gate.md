# Runtime Implementation Kickoff Gate

**State:** Accepted

This is the final M0 sign-off. “Document exists” is not sufficient; the
decision, enforcement, and evidence columns must agree.

## Product and compatibility sign-off

- [x] Product vision, users, use cases, 1.0 scope, and non-goals are accepted.
- [x] Spring Batch reference line and compatibility levels are accepted.
- [x] Glossary, lifecycle, identity, restart, status, and transaction semantics
      are accepted.
- [x] Initial conformance rows and scenario names are reviewed.
- [x] Public compatibility wording has no unsupported claim.

## Architecture sign-off

- [x] System/deployment boundaries and dependency direction are accepted.
- [x] Async execution ADR has passing spike evidence.
- [x] PostgreSQL metadata ADR has passing transaction/locking evidence.
- [x] Execution-context evolution has passing fixtures.
- [x] Error, configuration, feature, cancellation, panic, and blocking contracts
      are decided for M1.
- [x] Deferred distributed/plugin/service work is outside the M1 API.

## Engineering sign-off

- [x] Development bootstrap and required local commands are reproducible.
- [x] Coding/API conventions and definition of done are accepted.
- [x] MSRV CI passes on the declared version.
- [x] Dependency/license/advisory CI and exception policy are active.
- [x] M1 test layers, naming, deterministic utility contracts, and fixture
      policy are defined.
- [x] Supported M1 platform matrix is explicit.

## Security and operations sign-off

- [x] Threat model and risk register have no unowned High-impact risk.
- [x] Parameter/context/error/telemetry redaction rules are testable.
- [x] Release access and maintainer recovery inventory is current.
- [x] PostgreSQL credentials/roles and test database safety are documented.
- [x] Recovery/destructive operations are out of M1 or have guarded semantics.

## First slice sign-off

- [x] Each vertical-slice criterion maps to a named test.
- [x] Failure points state expected metadata, replay, counters, and operator
      action.
- [x] M1 implements only the in-memory/kernel portion without weakening the M2
      durable contract.
- [x] Benchmark and diagnostic hooks needed to evaluate the architecture are
      identified.

## Approval record

| Field | Record |
| --- | --- |
| Decision owner and date | Project owner, 2026-07-29 |
| Foundation revision | `4139dff` (`docs(governance): define foundation and M0-M5 roadmap`) |
| Architecture revision | `ca6e36a` (merged PR #20 with accepted ADR-0002/0003 and spikes) |
| Spike evidence | Spikes 0001 async traits, 0002 PostgreSQL transactions/recovery, and 0003 execution-context evolution |
| Deferred ownership | Named by role and later gate in the preparation master plan |
| Residual risks | R-003 delivery assumptions, R-007 user-code isolation, R-010 telemetry disclosure, R-013 single-maintainer continuity, and R-017 public dependency leakage remain active with mitigations |
| First authorized M1 work | GitHub issue #9, to be split into definition-ready implementation issues |

This record closes M0 and authorizes M1 implementation. Deferred work does not
become part of M1 unless its named gate or an accepted RFC promotes it.
