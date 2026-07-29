# RFC-0002: Full Spring Batch Feature-Ledger Parity

- **State:** Accepted
- **Created:** 2026-07-30
- **Owner:** compatibility maintainers
- **Target milestone:** M12
- **Related decision:** D-002 in the
  [M0 decision register](../product/open-decisions.md)

## Summary

Adopt complete documented Spring Batch 6.x feature-ledger coverage as the
long-term compatibility target, while explicitly excluding Java/Spring
source, binary, container, and shared-live-schema compatibility.

## Context and current accepted rule

D-002 accepts Spring Batch 6.0 semantics and selected behavioral/operational
compatibility. The original 1.0 target does not require the complete documented
feature population.

## Problem

A subset-based matrix can reach “100%” by omitting difficult areas. It cannot
support an honest complete-parity claim, migration assessment, or stable
long-term prioritization.

## Proposal

- Pin Spring Batch 6.0.4 as the initial complete population baseline.
- Populate from reference docs, public API packages, schemas, integration and
  test modules, component appendices, release notes, and deprecations.
- Give every row a stable ID, source, semantics, native equivalent, status,
  milestone, divergence, evidence profile/links, owner, and dependencies.
- Permit exact equivalents, reviewed native equivalents, explicit unsupported
  differences, and not-applicable rationales.
- Require zero unknown/deferred/untested rows at M12 closure.
- Keep claim levels and source/schema/API non-goals in the compatibility
  contract.

## Alternatives

1. Continue a curated subset. Simpler but cannot justify full coverage.
2. Copy Spring public classes one-for-one. Rejected because names do not prove
   behavior and would produce non-idiomatic Rust.
3. Track only reference-guide chapters. Rejected because public APIs,
   integrations, schemas, and test utilities contain material capabilities.

## Consequences

The ledger and evidence burden grows substantially. Some rows will receive
approved divergence or not-applicable dispositions. Product claims become
more precise and auditable.

## Compatibility impact

This expands proposed long-term scope, not current support. No row becomes
supported until released and `Verified`. Baseline updates follow a reviewed
population diff and may require a new RFC when they alter commitments.

## Metadata, restart, and transaction impact

The ledger must include metadata, migration, contexts, transaction/delivery,
restart, recovery, distributed ownership, and retention—not only APIs.
Existing guarantees cannot be weakened to match a Spring behavior without an
explicit decision and migration plan.

## Migration and rollout

First validate the populated ledger, split compound rows before verification,
and connect current tests. Implement by milestone. If rejected, the expanded
ledger remains useful planning material but cannot be described as the accepted
complete target.

## Validation and evidence plan

- independent review against the full official source population;
- CI validation of required row fields and evidence links before `Verified`;
- differential and black-box fixtures under the conformance strategy;
- crash, adapter, migration, and performance evidence per row profile;
- release audit preventing claims above verified rows.

## Unresolved questions

- The cadence for updating from 6.0.4 to later 6.x baselines.
- Which compound integration rows must split before RFC acceptance versus
  before verification.
- How external certification evidence is retained immutably.

## Decision

**Accepted by the project owner on 2026-07-30.**

The complete Spring Batch 6.x feature ledger is the long-term compatibility
population. D-002 is superseded for target scope while its existing verified
semantics remain valid. Baseline updates and compound-row splitting follow the
compatibility contract and do not expand released claims automatically.
