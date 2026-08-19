# OxideBatch Documentation

This directory contains the decisions, contracts, plans, and evidence that
govern OxideBatch. Documents marked **Accepted** are binding. **Proposed**
documents require approval before implementation depends on them.

## Reading paths

### Maintainers and coding agents

1. [Post-M5 full-parity strategy](project/post-m5-full-parity-strategy.md)
2. [Product vision and scope](product/vision-and-scope.md)
3. [Continuous roadmap](roadmap.md)
4. [Spring Batch compatibility contract](compatibility/spring-batch.md)
5. [Spring Batch feature ledger](compatibility/conformance-matrix.md)
6. [Architecture overview](architecture/overview.md)
7. [Documentation and decision precedence](documentation/strategy.md)
8. the focused subsystem document and relevant accepted RFCs/ADRs/gates

The strategy is an umbrella rationale. Focused canonical documents and accepted
decisions own the normative detail.

### Core engine contributors

1. [Execution semantics](compatibility/execution-semantics.md)
2. [Execution-plan architecture](architecture/execution-plan.md)
3. [Item-processing model](architecture/item-processing-model.md)
4. [M3 fault-tolerance contract](architecture/fault-tolerance.md)
5. [M3 basic-flow contract](architecture/basic-flow.md)
6. [M4 shutdown and stale-recovery contract](architecture/shutdown-and-recovery.md)
7. [M4 bounded local-scale contract](architecture/local-scale.md)
8. [Performance and capacity plan](engineering/performance-plan.md)
9. [Async execution ADR](architecture/decisions/0002-execution-model.md)
10. relevant roadmap and conformance rows

### Repository and integration implementers

1. [Repository and transaction model](architecture/repository-and-transaction-model.md)
2. [M4 operator, explorer, and retention contract](architecture/operator-and-explorer-services.md)
3. [Integration model](architecture/integration-model.md)
4. [Persistence and migrations](operations/persistence-and-migrations.md)
5. [PostgreSQL physical metadata model](architecture/postgres-physical-metadata-model.md)
6. [Repository ADR](architecture/decisions/0003-postgres-metadata.md)
7. adapter ledger rows and certification evidence

### Distributed execution contributors

1. [Distributed execution](architecture/distributed-execution.md)
2. [Execution-plan architecture](architecture/execution-plan.md)
3. [Repository and transaction model](architecture/repository-and-transaction-model.md)
4. [Control-plane boundary](operations/control-plane-boundary.md)
5. [RFC-0009](rfcs/0009-transport-neutral-worker-protocol.md)
6. M11 ledger rows and gate

### Compatibility reviewers

1. [Compatibility contract](compatibility/spring-batch.md)
2. [Feature ledger](compatibility/conformance-matrix.md)
3. [Conformance strategy](compatibility/conformance-strategy.md)
4. [Execution semantics](compatibility/execution-semantics.md)
5. [Spring Batch migration contract](compatibility/spring-batch-migration.md)
6. release/support evidence for the claim under review

## Current milestone evidence

- [Project preparation master plan](project/preparation-master-plan.md)
- [M0 runtime kickoff gate](project/kickoff-gate.md)
- [M1 executable-kernel exit evidence](project/m1-exit-evidence.md)
- [M2 durable chunk and restart kickoff gate](project/m2-kickoff-gate.md)
- [M2 durable metadata design-gate evidence](project/m2-design-gate-evidence.md)
- [M2 chunk component contract evidence](project/m2-component-contract-evidence.md)
- [M2 PostgreSQL repository evidence](project/m2-postgres-repository-evidence.md)
- [M2 deterministic chunk runtime evidence](project/m2-chunk-runtime-evidence.md)
- [M2 PostgreSQL atomic chunk transaction evidence](project/m2-postgres-chunk-transaction-evidence.md)
- [M2 durable restart and recovery evidence](project/m2-durable-restart-evidence.md)
- [M2 durable chunk and restart exit evidence](project/m2-exit-evidence.md)
- [M3 fault tolerance and flow kickoff gate](project/m3-kickoff-gate.md)
- [M3 fault tolerance and flow design-gate evidence](project/m3-design-gate-evidence.md)
- [M3 fault-tolerance and listener contract evidence](project/m3-fault-contract-evidence.md)
- [M3 fault-tolerance runtime evidence](project/m3-fault-runtime-evidence.md)
- [M3 PostgreSQL fault-durability evidence](project/m3-postgres-fault-durability-evidence.md)
- [M3 compiled-plan lowering evidence](project/m3-compiled-plan-evidence.md)
- [M3 durable flow runtime evidence](project/m3-flow-runtime-evidence.md)
- [M3 fault tolerance and flow exit evidence](project/m3-exit-evidence.md)
- [M4 operations and local scale kickoff gate](project/m4-kickoff-gate.md)
- [M4 operations and local-scale design-gate evidence](project/m4-design-gate-evidence.md)
- [M4 operator, explorer, and retention service evidence](project/m4-operator-services-evidence.md)
- [M4 operator CLI evidence](project/m4-operator-cli-evidence.md)
- [M4 shutdown and stale-recovery implementation evidence](project/m4-shutdown-recovery-evidence.md)
- [M4 bounded telemetry and diagnostic-bundle evidence](project/m4-telemetry-evidence.md)
- [M4 bounded local-scale plan evidence](project/m4-local-scale-plan-evidence.md)
- [M4 durable partition repository evidence](project/m4-partition-repository-evidence.md)
- [M4 bounded parallel-split runtime evidence](project/m4-parallel-split-evidence.md)
- [M4 bounded local-partition runtime evidence](project/m4-local-partition-runtime-evidence.md)
- [M4 operations and local scale exit evidence](project/m4-exit-evidence.md)
- [M5 embedded core production preview kickoff gate](project/m5-kickoff-gate.md)
- [M5 embedded core production preview design-gate evidence](project/m5-design-gate-evidence.md)
- [M5 plan and definition-fingerprint stabilization evidence](project/m5-plan-fingerprint-evidence.md)
- [M5 staged crate-extraction evidence](project/m5-crate-extraction-evidence.md)
- [M5 context-codec and transaction-capability evidence](project/m5-codec-and-capability-evidence.md)
- [M5 public facade and preview API review evidence](project/m5-facade-api-review-evidence.md)
- [M5 evidence campaign record](project/m5-campaign-evidence.md)
- [M5 issue #102 evidence campaign reconciliation](project/m5-102-reconciliation.md)
- [M5 exit evidence](project/m5-exit-evidence.md)
- [M6 complete item processing and user test kit kickoff gate](project/m6-kickoff-gate.md)
- [Historical M0 decision register](product/open-decisions.md)

Historical gates are preserved as records of their date. Later decisions link
to them and record supersession; they do not rewrite history.

## Guides

- [M5 Production Preview guide](guides/production-preview.md)
- [M5 production preview limitations](guides/limitations.md)
- [M5 developer guide](guides/developer-guide.md)
- [M5 operator guide](guides/operator-guide.md)
- [M5 upgrade and rollback guide](guides/upgrade-and-rollback.md)

## Product

- [Vision and scope](product/vision-and-scope.md)
- [Representative use cases](product/use-cases.md)
- [Alternatives and positioning](product/alternatives-and-positioning.md)
- [Non-functional requirements](product/non-functional-requirements.md)
- [First vertical slice](product/first-vertical-slice.md)

## Compatibility and semantics

- [Spring Batch compatibility contract](compatibility/spring-batch.md)
- [Spring Batch feature ledger](compatibility/conformance-matrix.md)
- [Conformance strategy](compatibility/conformance-strategy.md)
- [Execution, restart, and transaction semantics](compatibility/execution-semantics.md)
- [Spring Batch migration contract](compatibility/spring-batch-migration.md)
- [Domain glossary](compatibility/glossary.md)

## Architecture and API

- [System context and deployment boundaries](architecture/system-context.md)
- [Architecture overview](architecture/overview.md)
- [Execution-plan architecture](architecture/execution-plan.md)
- [Item-processing model](architecture/item-processing-model.md)
- [M3 fault-tolerance and listener contract](architecture/fault-tolerance.md)
- [M3 basic flow and start-control contract](architecture/basic-flow.md)
- [M4 shutdown and stale-recovery contract](architecture/shutdown-and-recovery.md)
- [M4 bounded local-scale contract](architecture/local-scale.md)
- [M5 staged crate-extraction contract](architecture/crate-extraction.md)
- [Repository and transaction model](architecture/repository-and-transaction-model.md)
- [M4 operator, explorer, and retention contract](architecture/operator-and-explorer-services.md)
- [Integration model](architecture/integration-model.md)
- [Distributed execution](architecture/distributed-execution.md)
- [Technical baseline](architecture/technical-baseline.md)
- [Technology evaluation](architecture/technology-evaluation.md)
- [Configuration model](architecture/configuration-model.md)
- [PostgreSQL physical metadata model](architecture/postgres-physical-metadata-model.md)
- [Rust API design guidelines](api/design-guidelines.md)
- [Architecture spikes](architecture/spikes/README.md)
- [Architecture decisions](architecture/decisions/README.md)

## Engineering and quality

- [Engineering standards](engineering/standards.md)
- [Coding conventions](engineering/coding-conventions.md)
- [Development environment](engineering/development-environment.md)
- [CI and quality gates](engineering/ci-quality-gates.md)
- [Dependency and license policy](engineering/dependency-policy.md)
- [Test strategy](testing/strategy.md)
- [Performance and capacity plan](engineering/performance-plan.md)
- [M4 measurement evidence](engineering/measurements/m4/README.md)
- [M5 measurement evidence](engineering/measurements/m5/README.md)
- [M5 campaign evidence](engineering/campaigns/m5/README.md)

## Security and operations

- [Threat model and supply-chain baseline](security/threat-model.md)
- [Severity and response objectives](security/severity-and-response.md)
- [Persistence and migration operations](operations/persistence-and-migrations.md)
- [PostgreSQL setup](operations/postgres-setup.md)
- [Transaction guarantees](operations/transaction-guarantees.md)
- [Crash, restart, and recovery runbook](operations/crash-restart-and-recovery.md)
- [Metadata migration-guide template](operations/migration-guide-template.md)
- [Schema-v1 initial metadata migration](operations/migrations/0001-initial-metadata.md)
- [Schema-v2 fault-tolerance and flow migration](operations/migrations/0002-fault-tolerance-and-flow.md)
- [Schema-v3 operations and local-scale migration](operations/migrations/0003-operations-and-local-scale.md)
- [Observability contract](operations/observability-contract.md)
- [Control-plane boundary](operations/control-plane-boundary.md)
- [M4 operator CLI and configuration contract](operations/operator-cli.md)
- [Operator CLI reference](operations/operator-cli-reference.md)
- [Capacity and resource budgets](operations/capacity-and-resource-budgets.md)

## Release, documentation, and governance

- [Delivery roadmap](roadmap.md)
- [Release and support policy](release/support-policy.md)
- [Support matrix](release/support-matrix.md)
- [Release checklist](release/release-checklist.md)
- [Documentation strategy](documentation/strategy.md)
- [Development and decision process](project/development-process.md)
- [Risk register](project/risk-register.md)
- [RFC index](rfcs/README.md)
- [Repository policy](governance/repository-policy.md)
- [Crate publishing policy](governance/crate-publishing.md)
- [Maintainer continuity and access](governance/maintainer-continuity.md)

## Document states

| State | Meaning |
| --- | --- |
| Accepted | Approved and binding until superseded |
| Proposed | Concrete recommendation awaiting approval |
| Active | Accepted living register/roadmap whose entries may have different states |
| Template | Approved structure with release-specific content pending |
| Draft | Incomplete exploration, not an implementation contract |
| Superseded | Replaced by a named document or decision |

Changes to accepted scope, compatibility, public APIs, durable data, dependency
direction, distributed protocol, or release policy require an RFC/ADR and pull
request.
