# Coding Conventions

**State:** Accepted

`rustfmt` is authoritative for layout. These conventions govern structure and
meaning that formatting cannot decide.

## Modules and visibility

- Prefer small modules organized by domain responsibility, not generic
  `utils`.
- Default to private; use `pub(crate)` before `pub`.
- Re-export public API deliberately from a module prelude/facade; avoid glob
  re-exports in public surfaces.
- Keep database rows, transport DTOs, and domain types distinct.
- Test-only helpers live in test modules or the eventual test-support crate.

## Naming

- Types and traits use domain nouns (`JobExecution`, `JobRepository`).
- Operations use verbs (`launch`, `restart`, `update_execution`).
- Boolean names read as predicates (`is_restartable`, `has_failure`).
- Units appear in type/name when a strong duration/size type is not available.
- Avoid abbreviations except established domain terms (`id`, `sql`, `utc`).
- Error variants state the condition, not the action that noticed it.

## Functions and control flow

- Validate at boundaries and keep domain internals operating on valid values.
- Prefer early returns for errors; avoid deeply nested happy paths.
- Keep mutation and I/O scopes narrow.
- Do not hold locks or database transactions across unrelated user callbacks.
- Every loop/retry/queue has a visible termination or bound.
- Cancellation-sensitive code documents its safe points.

## Error and panic handling

- Use `Result` for expected failures and typed categories for policy decisions.
- Add safe context at subsystem boundaries while preserving the source chain.
- Do not classify by parsing error strings.
- Do not ignore errors from persistence, flush, shutdown, or telemetry setup
  without an explicit policy.
- `panic!`, `unwrap`, and `expect` remain denied in production code; narrow
  test/example exceptions require lint scopes and obvious invariants.

## Comments and documentation

- Explain why an invariant or tradeoff exists; code should show mechanics.
- Link concurrency, transaction, unsafe, and compatibility assumptions to tests
  or ADRs.
- `TODO` includes an issue link and does not substitute for required behavior.
- Public docs use exact guarantees (`at-least-once within...`) rather than vague
  words (`safe`, `reliable`, `exactly-once`).

## Tests

- Test names describe observable behavior and condition.
- Arrange/act/assert separation is used when it improves failure diagnosis.
- Assertions include relevant IDs/statuses but never secret/item payloads.
- Shared fixtures expose builders with valid defaults and explicit mutations.
- Time-sensitive tests use injected clocks or eventual assertions with bounds.

## Dependencies and macros

- Import only used traits/items; avoid broad preludes inside implementation.
- Macros must preserve useful diagnostics and not hide transaction/control flow.
- Proc macros and code generation require extra compile-time, MSRV, and
  diagnostic review.
- Feature-gated code has a test for presence and absence of the feature.
