# RFC-0001: M5 Production Preview and Project-Wide 1.0

- **State:** Accepted
- **Created:** 2026-07-30
- **Owner:** maintainers
- **Target milestone:** M5 and M14
- **Related decisions:** D-003, D-010, D-012 in the
  [M0 decision register](../product/open-decisions.md)

## Summary

Redefine M5 as an Embedded Core Production Preview/stabilization boundary and
reserve project-wide 1.0/GA for M14 after item, flow, repository, integration,
distributed, migration, ecosystem, and support evidence is complete.

## Context and current accepted rule

The accepted roadmap makes M5 “Enterprise Readiness and 1.0.” D-003 fixes a
single-host PostgreSQL 1.0 scope, and D-012 places distributed work after 1.0
unless an RFC promotes it.

## Problem

Freezing the facade, item contracts, metadata, and release commitment before
compiled plans, capability-aware integrations, distributed protocols, and the
complete Spring ledger are designed would either create long-lived
incompatibilities or make the later full-parity target misleading.

## Proposal

- M5 becomes `0.5` Embedded Core Production Preview.
- M5 requires complete M0-M4 evidence but does not claim full parity or
  project-wide stable APIs.
- M6-M13 close the roadmap categories defined in `docs/roadmap.md`.
- M12 may open a 1.0 RC readiness program after ledger closure.
- M14 alone authorizes `1.0.0`/GA, enterprise-ready, and project-wide
  production-ready language.
- Pre-1.0 SemVer and support rules continue through M13.

## Goals and non-goals

The goal is one coherent stability promise aligned with the long-term product.
This RFC does not accept the detailed designs in RFC-0002 through RFC-0010,
guarantee milestone completion, or reduce M5 correctness evidence.

## Alternatives

1. Keep M5 project-wide 1.0 and add features in compatible minors. Rejected
   because major public boundaries are not yet known.
2. Stabilize only `oxide-batch-core` at M5. Viable, but package-level 1.0 could
   still be confused with product readiness and complicate coordinated
   versioning.
3. Never publish a preview label. Rejected because users need an explicit,
   supportable embedded-core gate before full parity.

## Consequences

The project retains pre-1.0 freedom longer and must maintain a larger roadmap.
Users receive a truthful, narrower M5 claim. Historical M0 decisions remain
preserved and are superseded only if this RFC and a follow-up ADR/decision
update are accepted.

## Compatibility and release impact

No released stable API is broken. Documentation, milestones, release policy,
support matrix, and claims change. M5 metadata still follows immutable
migration rules but does not receive the final N/N-1/N-2 1.0 commitment.

## Metadata, restart, and transaction impact

M5 keeps all accepted correctness rules. It additionally blocks stability when
the public design would prevent definition fingerprints, plan evolution,
transaction capabilities, or future distributed fencing. No migration of
existing schema-v1 data is required merely to accept this RFC.

## Migration and rollout

After acceptance, record D-003/D-010/D-012 as superseded for release timing,
update milestone names in GitHub, release/support text, and user messaging, and
retain old gates as historical records. Rollback is a new RFC before any M5
preview promise; after a published preview, claims can be narrowed only under
the release policy.

## Validation and evidence plan

- review every public M0-M4 claim and support-matrix dimension;
- show that M5 evidence is executable and later ledgers remain visible;
- audit all repository text for premature 1.0/enterprise claims;
- rehearse a preview release and upgrade before M5 exit;
- require the full M14 evidence list before the first 1.0 RC.

## Unresolved questions

- Whether the release is exactly `0.5.0` or another prerelease identifier.
- Whether any internal crate should receive an independent stability label.
- The final stable support window, still decided before the first 1.0 RC.

## Decision

**Accepted by the project owner on 2026-07-30.**

M5 is the Embedded Core Production Preview boundary and M14 is the
project-wide 1.0/GA boundary. The decision register, roadmap, release policy,
and support matrix must use this interpretation. The exact M5 version label
and final stable support window remain release-planning decisions and do not
change this scope decision.
