# Control-Plane Boundary

**State:** Accepted

**Governing decisions:**
[RFC-0008](../rfcs/0008-core-and-control-plane-boundary.md) and
[ADR-0007](../architecture/decisions/0007-control-plane-boundary.md)

This document is the canonical boundary between this repository and a
future OxideBatch control-plane project.

## Core repository ownership

The core repository owns correctness-bearing semantics and portable contracts:

- `JobOperator`, `JobExplorer`, `JobRegistry`, and definition registry ports;
- launch, stop, restart, abandon, recover, and stale-execution decisions;
- idempotency keys for launch and operator requests;
- paginated/streaming execution queries;
- metadata archive, purge, retention, and hold primitives;
- stable telemetry schema;
- coordinator/worker and administrative protocol types;
- a minimal CLI, worker mode, and thin reference administration server;
- protocol, operator, recovery, and conformance test kits.

The core does not delegate correctness to a hosted service. Embedded
applications can use these contracts without deploying a control plane.

## External control-plane ownership

A separate `oxide-batch-ops` or `oxide-batch-control-plane` project should own:

- hosted REST/gRPC application assembly and web UI;
- authentication, RBAC, tenants, organizations, and audit-search experience;
- scheduler/calendar policy and trigger management;
- Kubernetes operator/controller and worker fleet management;
- alerts, notifications, dashboards, and log/trace navigation;
- secret-backend integrations;
- deployment topology, high availability, quotas, billing, and SaaS concerns.

The scheduler invokes core operator APIs. It does not become the authority for
instance identity, execution state, or checkpoint progress.

## Dependency and security rules

Core crates do not depend on web frameworks, identity providers, UI toolchains,
Kubernetes clients, or scheduler implementations. The control plane depends on
versioned core protocol/client crates.

Hosted endpoints authenticate and authorize requests before invoking operator
services. Core services still validate lifecycle, version, ownership,
idempotency, and destructive-operation guards. Credentials and tenant policy
do not enter definition manifests or execution contexts.

M4 makes this split executable. Every mutating core action declares one
authorization class of `Read`, `Lifecycle`, or `Destructive` that a deployment
can grant independently, and requires a deployment-supplied opaque actor
reference for audit. The core never authenticates a caller, never consults an
identity provider, and never treats a supplied actor reference as proof of
authorization. Removing deployment authorization removes no core guard. The
exact request envelope, guards, and audit records are owned by the
[operator, explorer, and retention contract](../architecture/operator-and-explorer-services.md),
and the minimal CLI over them by the
[M4 operator CLI contract](operator-cli.md).

## Extraction gate

A separate repository is created only after:

- operator and explorer semantics are accepted and have conformance tests;
- the administrative and worker protocols have a versioned compatibility
  policy plus N/N-1 evidence;
- embedded and hosted paths demonstrate the same lifecycle decisions;
- crate ownership, release cadence, vulnerability response, and CI handoff are
  assigned;
- cross-repository change and rollback procedures are exercised;
- the reference server proves the boundary without importing control-plane
  dependencies into the engine.

Until then, experimental hosting code remains thin, optional, and private in
the existing workspace. Extraction cannot remove the minimal CLI or portable
operator semantics from core.
