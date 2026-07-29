# ADR-0007: Core and Control-Plane Boundary

- **State:** Accepted
- **Date:** 2026-07-30
- **Owners:** operations and architecture maintainers
- **Deciders:** project owner
- **Governing RFC:** [RFC-0008](../../rfcs/0008-core-and-control-plane-boundary.md)

## Context

OxideBatch must preserve portable correctness-bearing operator semantics
without importing web hosting, authentication, scheduling, Kubernetes, UI, or
SaaS dependencies into the embedded engine. Moving all operations outside
would let an external service bypass lifecycle and recovery invariants.

## Decision

The core repository owns operator, explorer, registry, recovery, retention,
telemetry, and worker/admin protocol semantics, plus a minimal CLI, worker mode,
reference host, and conformance kits.

A future external control-plane project owns hosted APIs, UI,
authentication/RBAC/tenancy, scheduler/calendar, Kubernetes and fleet
management, notifications, dashboards, deployment topology, and SaaS concerns.

The control plane invokes versioned core services. It cannot mutate metadata
directly to bypass identity, lifecycle, compare-and-swap, fencing, recovery
audit, or retention guards. Scheduler state and telemetry are not correctness
authorities.

No external repository is created until the extraction criteria in the
[control-plane boundary](../../operations/control-plane-boundary.md) pass and a
maintainer explicitly authorizes extraction.

## Consequences

- embedded users retain complete correctness semantics without a hosted
  service;
- eventual cross-repository releases require protocol, security, and ownership
  coordination;
- core crates remain free of web, identity-provider, scheduler, UI, and
  Kubernetes dependencies;
- the reference host remains thin and does not imply production control-plane
  support.

## Alternatives considered

- Putting hosted concerns in core would pollute dependency and product scope.
- Moving all operator semantics outside would weaken correctness portability.
- Immediate repository extraction would multiply changes while protocols are
  unstable.

## Validation

Validate embedded CLI and reference-host decision equivalence, authorization
without invariant bypass, protocol compatibility, scheduler idempotency,
dependency isolation, and cross-repository release/rollback before extraction.

## Revisit triggers

Revisit if ownership/release cadence makes the monorepo materially harmful, or
if a required operator semantic cannot be expressed through the portable core
service boundary.
