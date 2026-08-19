# Engineering Standards

**State:** Accepted

## Design philosophy

This section is the canonical owner of OxideBatch's engineering design
philosophy. It consolidates principles already present across accepted
architecture documents and `AGENTS.md` into one normative statement; it
introduces no new architecture decision, and no other document restates it in
full — link here instead of copying it.

### Idiomatic Rust over framework translation

OxideBatch references Spring Batch's proven domain semantics and observable
behavior, but it does not translate Java/OOP architecture into Rust syntax.
Design choices favor ownership, borrowing, traits, generics, enums, validated
types/newtypes, explicit lifetimes where meaningful, deterministic
construction, typed errors, and structured concurrency over a service
locator, reflection-style lookup, hidden container state, exception-driven
policy, or an inheritance-heavy API. The standing question for any design
choice is: is this the most idiomatic Rust design available?

### Functional core, explicit effects

Domain rules, policy decisions, validation, plan normalization, transition
decisions, retry/skip decisions, and deterministic state transformations are
separated from I/O and runtime effects — database, filesystem, network, wall
clock, scheduler/runtime, external services, and telemetry — wherever that
separation is practical. The goal is deterministic reasoning, easier
property/state-machine testing, explicit mutation, narrow I/O scope, and
reproducibility, not demonstrating functional style for its own sake. This
does not license unnecessary cloning or immutable-data ceremony that Rust
does not require.

### Zero-cost by default, explicit cost at boundaries

Native hot paths prefer static dispatch, monomorphization, borrowing,
reusable buffers, and zero-copy or reduced-copy processing where practical.
Type erasure, boxing, dynamic dispatch, and heap allocation are permitted
only at explicit architecture/composition boundaries. A zero-cost claim is a
measurable claim, not a slogan; accidental per-item allocation or dynamic
dispatch outside a declared boundary is a defect.

### Bounded asynchronous composition

OxideBatch does not target "reactive framework" as a marketing category.
Asynchronous processing is bounded, cancellation-aware, backpressure-
preserving, has explicit resource ownership and structured task lifetime,
and creates no detached work and no hidden unbounded buffering. Stream,
future, and concurrency abstractions are used where they express these
properties more clearly, not because an API "should" be a `Stream`.

### Invalid states should be difficult or impossible to represent

Enums, validated constructors, newtypes, capability types, and structured
error categories are used to remove invalid states at the type level when
that improves real API quality. Where typestate or generic complexity would
significantly worsen ergonomics, diagnostics, compile times, evolution, or
maintainability, a simpler validated runtime boundary is chosen instead.

### Evidence over architectural fashion

Functional style, reactive style, async, zero-copy, lock-free design, static
dispatch, generics, macros, and the newest Rust features are never adopted
because they are fashionable. Adoption requires at least one of: improved
correctness, clearer semantics, improved ergonomics, improved maintainability,
or measurable efficiency improvement. A new Rust language or library feature
is judged by the same bar.

### Reference-quality engineering

OxideBatch's architecture and implementation aim to be a long-term reference
other Rust projects can learn from: understandable, explicit, testable,
measurable, reproducible, maintainable, failure-aware, and operationally
explainable. Working code is not sufficient on its own. Design and
implementation are expected to answer, at minimum:

1. Is this an idiomatic Rust design?
2. Is the abstraction cost zero-cost or explicitly visible?
3. Are effects and mutation explicit?
4. Are cancellation, backpressure, failure, and resource ownership explicit?
5. Are invalid states prevented where practical?
6. Can behavior be proven by executable evidence?
7. Would this code be suitable as a serious engineering reference?

Marketing adjectives such as "GOAT," "best-in-class," or "state-of-the-art"
do not appear in normative documentation. Reference quality is an engineering
bar to meet, not a claim to assert.

## Rust and API design

- Rust 2024 edition and the workspace MSRV are mandatory.
- `unsafe` is forbidden unless a dedicated ADR defines the invariant, audit,
  and tests.
- Production code does not use `unwrap`, `expect`, or `panic!` without a
  reviewed exception.
- Public types and errors use domain language consistently with the
  compatibility glossary.
- Public APIs avoid exposing optional implementation dependencies.
- All public items have rustdoc and runnable examples where practical.
- Time, IDs, randomness, retries, and cancellation are injectable or
  deterministic in tests.

## Formatting and linting

`rustfmt` is authoritative. Clippy runs for all targets and features with
warnings denied. Repository lint configuration is inherited by every workspace
crate. Exceptions are narrow, documented at the use site, and reviewed.

## Change design

An issue is required before implementation. Use an RFC or ADR for:

- public API or compatibility changes;
- status, restart, checkpoint, or transaction semantics;
- metadata schema changes;
- new production dependencies or cross-crate dependency direction;
- security boundaries and operator behavior.

## Review requirements

Every behavior change includes:

- tests at the lowest useful layer;
- failure-path and restart implications;
- compatibility-matrix impact;
- telemetry and sensitive-data impact;
- migration and release-note impact when applicable.

## Definition of done

- acceptance criteria and tests pass;
- formatting, lint, documentation, dependency, and license gates pass;
- public behavior and limitations are documented;
- no unresolved P0/P1 issue is introduced;
- ADR/RFC and compatibility links are updated;
- generated artifacts are reproducible and secrets are absent.
