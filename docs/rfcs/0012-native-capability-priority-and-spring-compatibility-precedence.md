# RFC-0012: Native Capability Priority and Spring Compatibility Precedence

- **State:** Accepted
- **Created:** 2026-11-15
- **Owner:** project owner / compatibility maintainers
- **Related:** #676, RFC-0002

## Summary

Keep the complete Spring Batch feature ledger as OxideBatch's mandatory
comparison and compatibility-accounting population, but make native product
capability, correctness, operational quality, and idiomatic Rust architecture
authoritative when Spring-shaped parity would conflict with them.

RFC-0002 remains accepted. This RFC clarifies its priority interpretation:
complete ledger coverage means every pinned Spring capability is reviewed and
given an evidence-backed disposition. It does **not** require OxideBatch to
copy Spring's API shape, configuration experience, implementation mechanism, or
to schedule a lower-value Spring-equivalent capability ahead of stronger
native functionality.

## Context

Spring Batch is a valuable reference because its domain model and operational
semantics capture mature batch-processing experience. It also provides a useful
population against which OxideBatch can detect omissions and make compatibility
claims honestly.

However, Java/Spring mechanisms are not a universal architecture target for a
Rust framework. A rule that treats similarity itself as the highest priority
can create abstractions that fit Spring rather than Rust, hide backend-native
capabilities, increase maintenance cost, or delay features that improve
correctness and production usefulness.

The SQL database adapter discussion in M8 is a representative example. Spring
Batch can rely on JDBC and application-provided `DataSource` implementations.
Rust has no equivalent standard database SPI. OxideBatch therefore keeps its
repository semantic ports stable while allowing concrete adapters to own their
database clients privately instead of inventing a JDBC-shaped portability
layer merely for experiential similarity.

## Decision

Design and delivery decisions use this precedence unless a narrower accepted
contract explicitly requires otherwise:

1. correctness, durability, restart/recovery, transaction/delivery, security,
   and data-integrity guarantees;
2. useful OxideBatch capability development and production/operational quality;
3. idiomatic Rust, bounded-resource behavior, maintainability, and
   resource/backend-native architecture;
4. Spring Batch semantic, behavioral, operational, and feature compatibility
   where it does not compromise the preceding priorities;
5. API, configuration, naming, or implementation-shape similarity as an
   ergonomic and adoption benefit.

A lower item must not force a design that materially weakens a higher item.

## Compatibility accounting remains mandatory

RFC-0002's complete ledger remains authoritative for comparison coverage.

For every affected Spring capability, OxideBatch must still record one of:

- verified exact or behavioral equivalence;
- a reviewed Rust-native equivalent;
- an explicit documented divergence;
- `Unsupported` with rationale;
- `NotApplicable` with rationale;
- a time-bounded `Deferred` state while work remains genuinely scheduled.

No difficult or inconvenient Spring capability may disappear from the ledger
merely because OxideBatch chooses a different product direction.

"Complete ledger coverage" therefore means complete population and reviewed
disposition. It does not by itself mean every Spring mechanism is implemented,
nor that Spring parity controls roadmap priority.

## Native capability development

OxideBatch may add capabilities with no Spring equivalent when they improve the
product's correctness, operability, performance, distributed execution,
resource control, reproducibility, integration model, or ecosystem.

Such native capabilities do not need a synthetic Spring parity row. They do
need the normal OxideBatch design, evidence, support, migration, and
compatibility treatment appropriate to their public contract.

Where Spring and OxideBatch solve the same user/operator problem differently,
the preferred outcome is the strongest justified native contract, with the
difference recorded rather than hidden.

## What this does not change

This RFC does not weaken an already advertised or released compatibility
guarantee. A capability claimed as Spring-compatible must continue to satisfy
the evidence and divergence rules in the compatibility contract.

This RFC also does not remove:

- Spring Batch 6.0.4 as the current pinned comparison baseline;
- the conformance matrix and reference evidence;
- named semantic or behavioral parity goals where explicitly owned;
- Spring metadata/definition migration work already accepted by RFC-0010;
- familiar Spring terminology where the semantics genuinely match.

## Rejected alternatives

### Make Spring parity the top-level product constraint

Rejected. It would allow compatibility shape to dictate architecture and
scheduling even when a Rust-native or stronger product capability is preferable.

### Drop the complete Spring ledger

Rejected. The ledger remains useful for omission detection, migration planning,
user expectations, and honest compatibility claims.

### Reproduce Spring infrastructure abstractions preemptively

Rejected. OxideBatch does not create JDBC-like, container-like, or other
framework-shaped abstractions solely to preserve familiarity or reserve a
future design. An abstraction requires a present semantic, capability, or
support need.

## Consequences

- Spring familiarity remains useful for adoption but is not an architecture
  veto.
- Roadmap ordering may prioritize native capability over an unimplemented
  Spring-equivalent row.
- Some Spring rows may reach terminal `Unsupported` or `NotApplicable`
  dispositions with explicit rationale rather than being implemented solely
  for parity.
- Product and architecture reviews must distinguish compatibility claims from
  product priority.
- Future implementations may preserve domain semantics while deliberately
  choosing different APIs, configuration, dependencies, or internal
  architecture.

## Validation and governance

Reviews of a material design choice must answer:

1. What user/operator capability or correctness property is being optimized?
2. Does Spring provide useful semantic evidence for this problem?
3. Would matching Spring's shape weaken a higher-priority product or
   engineering property?
4. If OxideBatch diverges, is the affected ledger disposition explicit?
5. Is any public compatibility claim still accurate after the decision?

## Decision

**Accepted by the project owner on 2026-11-15.**

RFC-0002 remains the compatibility-population decision. RFC-0012 is the
authoritative precedence rule when Spring compatibility and native product
design compete.
