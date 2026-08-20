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
policy, or an inheritance-heavy API. For each design, prefer idiomatic Rust
mechanisms over mechanical translation of Java/OOP structure while preserving
the accepted observable semantics. This preference does not weaken the accepted
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

### Zero-cost hot paths, explicit cost at boundaries (guideline)

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

### Evidence over architectural fashion (guideline)

Architecture and implementation choices SHOULD be justified by one or more of:
improved correctness, clearer semantics, improved ergonomics, improved
maintainability, or measurable efficiency, rather than novelty or fashion.
This applies equally when considering functional or reactive style, async,
zero-copy, lock-free design, static dispatch, generics, macros, or new Rust
language/library features. The binding requirements remain with the documents
that own them: [AGENTS.md](../../AGENTS.md#rust-and-architecture-bar) requires
cost justification for new dependencies, macros, and code generation, and the
[performance plan](performance-plan.md#measurement-principles) owns the
measurement discipline for efficiency claims.

### Engineering quality bar (guideline)

Architecture and implementation SHOULD be understandable, explicit, testable,
measurable, reproducible, maintainable, failure-aware, and operationally
explainable. Working code alone is not sufficient. This extends the same
evidence-claim discipline
[AGENTS.md](../../AGENTS.md#mission-and-non-negotiable-direction) already
requires of "production-ready," "compatible," "high performance," and
"beyond Spring" without creating a new product claim. Design and
implementation should be reviewable against concrete questions:

1. Does the design use idiomatic Rust mechanisms while preserving accepted semantics?
2. Is hot-path abstraction cost zero-cost or explicitly visible and justified?
3. Are effects and mutation explicit enough to reason about and test?
4. Are cancellation, backpressure, failure, and resource ownership explicit?
5. Are invalid states prevented where practical without disproportionate complexity?
6. Can behavior and material claims be supported by executable evidence?
7. Could an experienced maintainer understand and defend the design from repository evidence without access to the originating conversation?

Normative documentation avoids unverifiable superlatives and self-promotional
claims. Capability and quality claims require evidence appropriate to the
claim.

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
