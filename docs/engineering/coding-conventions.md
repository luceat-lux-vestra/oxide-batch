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

## Declaring a resource bound

Every finite ceiling the framework owns — a queue, retry cache, page, buffer,
worker assignment, or result set — is declared as a **named constant**, and the
name follows this convention so that the
[resource-bound campaign](../project/m5-campaign-evidence.md#resource-bound-campaign)
can find it.

- The name is `SCREAMING_SNAKE_CASE` and contains `MAX_`, `MIN_`, `MAXIMUM`,
  `MINIMUM`, `_BUDGET`, `_BOUND`, or `_CAPACITY`.
- The ceiling is the constant's own value, not an anonymous literal inlined at
  the place it is enforced. `if value > MAX_PAGE_SIZE` is a bound; `if value >
  500` is a number nobody can find.
- The constant is an ordinary `const` item or an associated `const`. A ceiling
  that exists only after macro expansion is not declared for these purposes.
- A configurable limit still declares the hard ceiling it is validated against.
  `ExportQueueBound` is chosen by an operator and is checked against
  `MIN_EXPORT_QUEUE_RECORDS` and `MAX_EXPORT_QUEUE_RECORDS`, which are what the
  framework actually owns.
- Visibility is irrelevant to the convention. A private ceiling is still a
  ceiling, and the campaign's scan deliberately does not consult visibility.

The campaign's reconciliation parses every library crate and requires each
constant matching this convention to be classified — as a resource with a
proving report, or as an explicitly excluded bound with a stated reason. What
that guarantees is exactly this: **a bound declared under this convention cannot
enter the product without entering the campaign.** It does not, and cannot,
discover a ceiling written as a bare literal or named outside the convention.
That is the reason the convention is a documented rule rather than a habit, and
the reason review still owns the question of whether a new limit is a
framework-owned resource at all.

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
