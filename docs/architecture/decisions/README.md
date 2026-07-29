# Architecture Decision Records

ADRs record durable choices that affect compatibility, public APIs, persistent
data, correctness, security, or dependency direction.

## States

- Proposed
- Accepted
- Rejected
- Deprecated
- Superseded by ADR-NNNN

## Index

| ADR | Title | State |
| --- | --- | --- |
| [0001](0001-workspace-and-facade.md) | Workspace and public facade | Accepted |
| [0002](0002-execution-model.md) | Async execution model | Accepted |
| [0003](0003-postgres-metadata.md) | PostgreSQL metadata repository | Superseded by ADR-0006 |
| [0004](0004-job-definition-restart-compatibility.md) | Job-definition identity and restart compatibility | Accepted |
| [0005](0005-compiled-execution-plan.md) | Compiled execution plan | Accepted |
| [0006](0006-repository-capability-model.md) | Repository services and capability model | Accepted |
| [0007](0007-control-plane-boundary.md) | Core and control-plane boundary | Accepted |

Copy [template.md](template.md) for a new decision. ADRs are immutable after
acceptance except for status and links; changed decisions receive a new ADR.

## Pending proposals

[RFC-0005](../../rfcs/0005-static-and-erased-components.md) may change
ADR-0002's boxed-future allocation consequence after a performance/API spike.
[RFC-0009](../../rfcs/0009-transport-neutral-worker-protocol.md) may add the
distributed protocol decision after its state-machine, threat, migration, and
equivalence evidence passes. Neither proposal is accepted.
