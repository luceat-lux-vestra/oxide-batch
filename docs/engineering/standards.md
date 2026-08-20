# Engineering Standards

**State:** Accepted

## Design philosophy

This section is the canonical owner of OxideBatch's engineering design
philosophy. It consolidates principles already present across `AGENTS.md` and
other accepted documents into one normative statement; it introduces no new
architecture decision and strengthens no accepted contract. No other document
restates it in full — link here instead of copying it. Where a principle
below is a project-wide restatement of a rule some other document already
binds more specifically (dependency direction, hot-path allocation budgets),
that document remains the enforced source and is linked, not duplicated.

### Idiomatic Rust over framework translation

OxideBatch references Spring Batch's proven domain semantics and observable
behavior, but it does not translate Java/OOP architecture into Rust syntax.
Design choices favor ownership, borrowing, traits, generics, enums, validated
types/newtypes, explicit lifetimes where meaningful, deterministic
construction, typed errors, and structured concurrency over a service
locator, reflection-style lookup, hidden container state, exception-driven
policy, or an inheritance-heavy API. The standing question for any design
choice is: is this the most idiomatic Rust design available? This preference
does not weaken the accepted
[Spring Batch compatibility contract](../compatibility/spring-batch.md)'s
semantic-equivalence obligations — an idiomatic Rust design must still map or
document every pinned Spring capability, per
[AGENTS.md](../../AGENTS.md#compatibility-and-beyond-parity-discipline).

### Functional core, explicit effects (guideline)

Domain rules, policy decisions, validation, plan normalization, transition
decisions, retry/skip decisions, and deterministic state transformations
SHOULD be kept separate from I/O and runtime effects — database, filesystem,
network, wall clock, scheduler/runtime, external services, and telemetry —
where that separation is practical and does not fight Rust ownership and
borrowing. This is implementation guidance, not an architectural contract:
it does not forbid mutation, does not require unnecessary cloning or
persistent-data-structure ceremony, and does not mandate an effect-system or
functional-framework abstraction. The one MUST-level rule this guideline
supports is already accepted and enforced elsewhere: core domain, plan, item,
and repository contracts stay independent of Tokio, SQLx, database/broker
client, CLI/web framework, and telemetry-SDK types, with adapters depending
inward (see
[AGENTS.md](../../AGENTS.md#rust-and-architecture-bar)).

### Zero-cost by default, explicit cost at boundaries (guideline)

Native hot paths SHOULD prefer static dispatch, monomorphization, borrowing,
reusable buffers, and zero-copy or reduced-copy processing where practical,
and erasure/boxing/dynamic dispatch/heap allocation SHOULD stay at deliberate
composition boundaries rather than appearing silently on a hot path. This is
general guidance, not a blanket "every abstraction must be zero-cost" rule,
and it does not itself set or exceed a binding budget: the actual MUST-level
hot-path allocation and dispatch rules, and the specific boundaries where
erasure is permitted, are owned by the
[performance and capacity plan](performance-plan.md#architecture-budgets) and
by named gates (for example
[M6 Gate H](../project/m6-design-gate-evidence.md#gate-h--p-002-real-component-performance-protocol)).
A zero-cost claim is a measurable claim, not a slogan, but this section is
not itself the place that measures or binds it.

### Bounded asynchronous composition

OxideBatch does not target "reactive framework" as a marketing category or a
product identity. Asynchronous processing is bounded, cancellation-aware,
backpressure-preserving, has explicit resource ownership and structured task
lifetime, and creates no detached work and no hidden unbounded buffering, per
the bounded-resource rules already accepted in
[AGENTS.md](../../AGENTS.md#rust-and-architecture-bar) and the
[performance plan's backpressure and capacity](performance-plan.md#backpressure-and-capacity)
and [cancellation and scale](performance-plan.md#cancellation-and-scale)
sections. Stream, future, and concurrency abstractions are used where they
express these properties more clearly, not because an API "should" be a
`Stream` — not every API becomes a `Stream` under this principle.

### Invalid states should be difficult or impossible to represent

Enums, validated constructors, newtypes, capability types, and structured
error categories are used to remove invalid states at the type level when
that improves real API quality, consistent with the validated-types rule
already accepted in
[AGENTS.md](../../AGENTS.md#rust-and-architecture-bar) and the
[API design guidelines](../api/design-guidelines.md#naming-and-types). Where
typestate or generic complexity would significantly worsen ergonomics,
diagnostics, compile times, evolution, or maintainability, a simpler
validated runtime boundary is chosen instead; typestate/generic complexity is
never pursued as an end in itself.

### Evidence over architectural fashion

Functional style, reactive style, async, zero-copy, lock-free design, static
dispatch, generics, macros, and the newest Rust features are never adopted
because they are fashionable. Adoption requires at least one of: improved
correctness, clearer semantics, improved ergonomics, improved maintainability,
or measurable efficiency improvement, consistent with the justification
[AGENTS.md](../../AGENTS.md#rust-and-architecture-bar) already requires of
new dependencies, macros, and code generation, and the measurement discipline
the [performance plan](performance-plan.md#measurement-principles) already
requires of efficiency claims. A new Rust language or library feature is
judged by the same bar.

### Reference-quality engineering

OxideBatch's architecture and implementation aim to be a long-term reference
other Rust projects can learn from: understandable, explicit, testable,
measurable, reproducible, maintainable, failure-aware, and operationally
explainable. Working code is not sufficient on its own. This is the same
evidence-claim discipline
[AGENTS.md](../../AGENTS.md#mission-and-non-negotiable-direction) already
requires of "production-ready," "compatible," "high performance," and
"beyond Spring": reference quality is an engineering bar this project holds
itself to, not a product claim OxideBatch makes about itself. Design and
implementation are expected to answer, at minimum:

1. Is this an idiomatic Rust design?
2. Is the abstraction cost zero-cost or explicitly visible?
3. Are effects and mutation explicit?
4. Are cancellation, backpressure, failure, and resource ownership explicit?
5. Are invalid states prevented where practical?
6. Can behavior be proven by executable evidence?
7. Would this code be suitable as a serious engineering reference?

Marketing adjectives such as "GOAT," "best-in-class," or "state-of-the-art"
do not appear in normative documentation.

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
