# Architecture Decision Records

ADRs record durable choices that affect compatibility, public APIs, persistent
data, correctness, security, or dependency direction.

## States

- Proposed
- Accepted
- Rejected
- Deprecated
- Superseded by ADR-NNNN
- Partially superseded by ADR-NNNN, when a later decision replaces a named part
  of a record and the rest stays in force. The superseding ADR must name the
  part; the superseded record keeps its state.

## Index

| ADR | Title | State |
| --- | --- | --- |
| [0001](0001-workspace-and-facade.md) | Workspace and public facade | Accepted; partially superseded by ADR-0010 |
| [0002](0002-execution-model.md) | Async execution model | Accepted; partially superseded by ADR-0008 |
| [0003](0003-postgres-metadata.md) | PostgreSQL metadata repository | Superseded by ADR-0006 |
| [0004](0004-job-definition-restart-compatibility.md) | Job-definition identity and restart compatibility | Accepted |
| [0005](0005-compiled-execution-plan.md) | Compiled execution plan | Accepted |
| [0006](0006-repository-capability-model.md) | Repository services and capability model | Accepted |
| [0007](0007-control-plane-boundary.md) | Core and control-plane boundary | Accepted |
| [0008](0008-item-component-contract.md) | Item component contract and erasure boundary | Accepted; partially supersedes ADR-0002 |
| [0009](0009-definition-fingerprint-input-set.md) | Definition fingerprint input set | Accepted |
| [0010](0010-extracted-crate-publication.md) | Extracted implementation crate publication | Accepted; partially supersedes ADR-0001 |
| [0011](0011-extraction-order-and-value-placement.md) | Durable value placement across extracted boundaries | Accepted |
| [0012](0012-json-item-representation-discloses-serde-json-value.md) | JSON item representation discloses `serde_json::Value` | Accepted |

Copy [template.md](template.md) for a new decision. ADRs are immutable after
acceptance except for status and links; changed decisions receive a new ADR.

## Pending proposals

[RFC-0009](../../rfcs/0009-transport-neutral-worker-protocol.md) may add the
distributed protocol decision after its state-machine, threat, migration, and
equivalence evidence passes. It is not accepted.

RFC-0005 is no longer pending: it was accepted on 2026-08-03 on the evidence of
[spike 0004](../spikes/0004-static-and-erased-item-path.md), and its decision
is recorded as [ADR-0008](0008-item-component-contract.md).
