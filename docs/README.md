# OxideBatch Documentation

This directory contains the decisions and contracts that govern OxideBatch.
Documents marked **accepted** are binding. Documents marked **proposed** must be
approved before implementation depending on them begins.

## Start here

1. [Project preparation master plan](project/preparation-master-plan.md)
2. [M0 runtime implementation kickoff gate](project/kickoff-gate.md)
3. [M0–M5 delivery roadmap](roadmap.md)
4. [M1 executable-kernel exit evidence](project/m1-exit-evidence.md)
5. [M2 durable chunk and restart kickoff gate](project/m2-kickoff-gate.md)
6. [M2 durable metadata design-gate evidence](project/m2-design-gate-evidence.md)
7. [M2 chunk component contract evidence](project/m2-component-contract-evidence.md)
8. [M0 decision register](product/open-decisions.md)

## Product

- [Vision and scope](product/vision-and-scope.md)
- [Representative use cases](product/use-cases.md)
- [Alternatives and positioning](product/alternatives-and-positioning.md)
- [Non-functional requirements](product/non-functional-requirements.md)
- [First vertical slice](product/first-vertical-slice.md)

## Compatibility and semantics

- [Spring Batch compatibility contract](compatibility/spring-batch.md)
- [Domain glossary](compatibility/glossary.md)
- [Execution, restart, and transaction semantics](compatibility/execution-semantics.md)
- [Compatibility and conformance matrix](compatibility/conformance-matrix.md)
- [Conformance strategy](compatibility/conformance-strategy.md)

## Architecture and API

- [System context and deployment boundaries](architecture/system-context.md)
- [Architecture overview](architecture/overview.md)
- [Technical baseline](architecture/technical-baseline.md)
- [Technology evaluation](architecture/technology-evaluation.md)
- [Configuration model](architecture/configuration-model.md)
- [PostgreSQL physical metadata model](architecture/postgres-physical-metadata-model.md)
- [Rust API design guidelines](api/design-guidelines.md)
- [Architecture spike template](architecture/spike-template.md)
- [Completed architecture spike evidence](architecture/spikes/README.md)
- [Architecture decision records](architecture/decisions/README.md)

## Engineering and quality

- [Engineering standards](engineering/standards.md)
- [Coding conventions](engineering/coding-conventions.md)
- [Development environment](engineering/development-environment.md)
- [CI and quality gates](engineering/ci-quality-gates.md)
- [Dependency and license policy](engineering/dependency-policy.md)
- [Test strategy](testing/strategy.md)
- [Performance and capacity plan](engineering/performance-plan.md)

## Security and operations

- [Threat model and supply-chain baseline](security/threat-model.md)
- [Severity and response objectives](security/severity-and-response.md)
- [Persistence and migration operations](operations/persistence-and-migrations.md)
- [Metadata migration-guide template](operations/migration-guide-template.md)
- [Observability contract](operations/observability-contract.md)

## Release, documentation, and governance

- [Release and support policy](release/support-policy.md)
- [Support matrix](release/support-matrix.md)
- [Release checklist](release/release-checklist.md)
- [Documentation strategy](documentation/strategy.md)
- [Development and decision process](project/development-process.md)
- [Risk register](project/risk-register.md)
- [RFC index and template](rfcs/README.md)
- [Repository policy](governance/repository-policy.md)
- [Crate publishing policy](governance/crate-publishing.md)
- [Maintainer continuity and access](governance/maintainer-continuity.md)

## Document states

| State | Meaning |
| --- | --- |
| Accepted | Approved and binding until superseded |
| Proposed | Concrete recommendation awaiting approval |
| Active | Accepted living register whose entries may have different states |
| Template | Approved structure with release- or issue-specific content pending |
| Draft | Incomplete exploration, not an implementation contract |
| Superseded | Replaced by a newer named document or ADR |

Changes to compatibility guarantees, durable data, public APIs, or dependency
direction require an ADR or RFC and a pull request.
