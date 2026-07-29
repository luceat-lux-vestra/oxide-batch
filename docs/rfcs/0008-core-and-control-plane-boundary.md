# RFC-0008: Core and External Control-Plane Boundary

- **State:** Accepted
- **Created:** 2026-07-30
- **Owner:** operations and architecture maintainers
- **Target milestone:** M7-M14
- **Related documents:** [system context](../architecture/system-context.md),
  [control-plane boundary](../operations/control-plane-boundary.md)

## Summary

Keep correctness-bearing operator, explorer, recovery, retention, telemetry,
and worker protocol contracts in core; place hosted APIs, UI, identity,
scheduling, fleet, Kubernetes, and SaaS concerns in a future external project.

## Context and current accepted rule

OxideBatch is an embedded framework with optional CLI. The accepted system
context excludes a hosted control plane, built-in scheduler, cross-host leader
election, and UI from current scope.

## Problem

Moving all operations out would make correctness depend on an external product.
Putting web hosting, auth, scheduling, and UI inside the engine would pollute
dependencies and scope. Premature repository extraction would make protocol
changes expensive.

## Proposal

- Core owns portable operator/explorer/registry services, recovery decisions,
  retention primitives, telemetry schema, worker/admin protocol types, minimal
  CLI, worker mode, and conformance kits.
- A future control-plane project owns hosting, REST/gRPC assembly, UI,
  authentication/RBAC/tenancy, scheduler/calendar, Kubernetes/fleet,
  notifications, dashboards, deployment topology, and SaaS concerns.
- Scheduler retries use core launch idempotency; scheduler state is not
  execution authority.
- Begin with a thin optional reference server in the existing workspace.
- Extract only after the protocol, ownership, release, and CI gates in the
  canonical boundary document pass.

## Alternatives

1. Put everything in core. Rejected for dependency and product-scope pollution.
2. Put all operations outside. Rejected because lifecycle correctness and
   recovery semantics belong to the engine contract.
3. Create a separate repository immediately. Rejected while protocols and
   ownership are unstable.

## Consequences

Two eventual products need coordinated versioning and security ownership. Core
remains usable without the hosted service. A reference server adds test surface
but does not become a production control plane by implication.

## Compatibility impact

Portable operator semantics and protocol DTOs become versioned compatibility
surfaces. Hosted APIs can evolve independently within their client/protocol
contract. The embedded facade does not acquire web/auth dependencies.

## Metadata, restart, and transaction impact

The control plane invokes core transactions and cannot write metadata directly
to bypass lifecycle, CAS, fencing, audit, or retention guards. Scheduler and UI
state cannot decide restart or commit outcome.

## Migration and rollout

Define core services, add a minimal CLI/reference host, stabilize protocol,
then rehearse cross-repository release and rollback before extraction. Existing
embedded users require no migration. Extraction preserves package/protocol
versions and can be reversed by restoring the reference host to the workspace.

## Validation and evidence plan

- embedded CLI and hosted reference server produce identical decisions;
- authorization cannot bypass core validation;
- protocol N/N-1 and client compatibility tests;
- scheduler duplicate/idempotency, recovery, and retention fixtures;
- dependency checks excluding control-plane frameworks from core;
- cross-repository release/security incident rehearsal before extraction.

## Unresolved questions

- Final repository/product name and maintainers.
- Whether the reference server is published or example-only.
- Protocol support window and authentication profiles.

## Decision

**Accepted by the project owner on 2026-07-30.**

The ownership boundary is accepted. This decision does not create or authorize
an external repository. Extraction still requires every protocol, ownership,
release, security, and CI criterion in the canonical boundary document plus an
explicit maintainer action.
