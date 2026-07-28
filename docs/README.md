# OxideBatch Documentation

This directory contains the decisions and contracts that govern OxideBatch.
Documents marked **accepted** are binding. Documents marked **proposed** must be
approved before implementation depending on them begins.

## Start here

- [Roadmap](roadmap.md)
- [Product vision and scope](product/vision-and-scope.md)
- [M0 decision register](product/open-decisions.md)
- [Spring Batch compatibility contract](compatibility/spring-batch.md)
- [Execution, restart, and transaction semantics](compatibility/execution-semantics.md)
- [Architecture overview](architecture/overview.md)
- [Technical baseline](architecture/technical-baseline.md)
- [Architecture decision records](architecture/decisions/README.md)
- [Engineering standards](engineering/standards.md)
- [Test strategy](testing/strategy.md)
- [Security baseline](security/threat-model.md)
- [Release and support policy](release/support-policy.md)
- [First vertical slice](product/first-vertical-slice.md)

## Document states

| State | Meaning |
| --- | --- |
| Accepted | Approved and binding until superseded |
| Proposed | Concrete recommendation awaiting approval |
| Draft | Incomplete exploration, not an implementation contract |
| Superseded | Replaced by a newer named document or ADR |

Changes to compatibility guarantees, durable data, public APIs, or dependency
direction require an ADR or RFC and a pull request.
