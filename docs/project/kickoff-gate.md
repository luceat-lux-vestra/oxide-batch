# Runtime Implementation Kickoff Gate

**State:** Proposed

This is the final M0 sign-off. “Document exists” is not sufficient; the
decision, enforcement, and evidence columns must agree.

## Product and compatibility sign-off

- [ ] Product vision, users, use cases, 1.0 scope, and non-goals are accepted.
- [ ] Spring Batch reference line and compatibility levels are accepted.
- [ ] Glossary, lifecycle, identity, restart, status, and transaction semantics
      are accepted.
- [ ] Initial conformance rows and scenario names are reviewed.
- [ ] Public compatibility wording has no unsupported claim.

## Architecture sign-off

- [ ] System/deployment boundaries and dependency direction are accepted.
- [ ] Async execution ADR has passing spike evidence.
- [ ] PostgreSQL metadata ADR has passing transaction/locking evidence.
- [ ] Execution-context evolution has passing fixtures.
- [ ] Error, configuration, feature, cancellation, panic, and blocking contracts
      are decided for M1.
- [ ] Deferred distributed/plugin/service work is outside the M1 API.

## Engineering sign-off

- [ ] Development bootstrap and required local commands are reproducible.
- [ ] Coding/API conventions and definition of done are accepted.
- [ ] MSRV CI passes on the declared version.
- [ ] Dependency/license/advisory CI and exception policy are active.
- [ ] M1 test layers, naming, deterministic utilities, and fixture policy exist.
- [ ] Supported M1 platform matrix is explicit.

## Security and operations sign-off

- [ ] Threat model and risk register have no unowned High-impact risk.
- [ ] Parameter/context/error/telemetry redaction rules are testable.
- [ ] Release access and maintainer recovery inventory is current.
- [ ] PostgreSQL credentials/roles and test database safety are documented.
- [ ] Recovery/destructive operations are out of M1 or have guarded semantics.

## First slice sign-off

- [ ] Each vertical-slice criterion maps to a named test.
- [ ] Failure points state expected metadata, replay, counters, and operator
      action.
- [ ] M1 implements only the in-memory/kernel portion without weakening the M2
      durable contract.
- [ ] Benchmark and diagnostic hooks needed to evaluate the architecture are
      identified.

## Approval record

Record in the M0 tracking issue:

- decision owner and date;
- accepted document revisions/commit;
- spike evidence links;
- deferred items with milestone and owner;
- residual risks;
- first M1 issue authorized for implementation.
