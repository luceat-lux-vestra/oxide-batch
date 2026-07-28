# ADR-0001: Workspace and Public Facade

- **State:** Accepted
- **Date:** 2026-07-29
- **Owners:** maintainers
- **Deciders:** project owner

## Context

OxideBatch needs separate domain, runtime, persistence, operational, and test
boundaries, but publishing all predicted packages would create premature API
and support commitments.

## Decision

Use one Cargo workspace. Publish `oxide-batch` as the curated facade. Add
implementation crates only when a real dependency boundary exists, and keep
them private with `publish = false` until a public integration boundary is
approved.

## Consequences

- users have one stable default entry point;
- internal boundaries can evolve without immediately becoming public API;
- public adapter crates may be released independently when justified;
- workspace release tooling must publish public crates in dependency order;
- facade re-exports require deliberate stability review.

## Alternatives considered

- A monolithic crate would reduce initial structure but couple domain contracts
  to optional infrastructure.
- Publishing placeholder crates would reserve names but create unsupported
  permanent artifacts and was rejected.
- Multiple repositories would make atomic changes and conformance testing
  harder.

## Validation

The initial facade package has been published and the workspace publication
policy is documented.

## Revisit triggers

Revisit if independent release cadences or ownership boundaries make a
monorepo materially harmful.
