# Spike 0004: Static and Erased Item Hot Path

- **State:** Complete
- **Owner:** API and performance maintainers
- **Issue:** none; run ahead of M6 kickoff for
  [RFC-0005](../../rfcs/0005-static-and-erased-components.md)
- **Date:** 2026-08-03
- **Decision/ADR:** closed
  [RFC-0005's approval gate](../../rfcs/0005-static-and-erased-components.md#approval-gate)
  with the measurements, the contract shape, and the ergonomics review; the
  decision is recorded as
  [ADR-0008](../decisions/0008-item-component-contract.md)

## Decision to unblock

[RFC-0005](../../rfcs/0005-static-and-erased-components.md) was `Proposed` when
this spike began, and its own approval gate required "a reproducible spike and
measurements for both paths" before it could be accepted. The
[M5 design gate](../../project/m5-design-gate-evidence.md) closed the RFC as
continued deferral for exactly that reason. M6 in turn depends on an *accepted*
RFC-0005, so the spike is the first thing that has to exist.

It also answers three questions the RFC left explicitly unresolved: the stable
trait form at MSRV 1.95, whether the facade can default to the native form, and
what monomorphization costs in code size and compile time.

## Hypotheses

1. A generic trait returning `impl Future<Output = ..> + Send` compiles at the
   supported MSRV and is not dyn compatible, so heterogeneous composition needs
   an explicit erased boundary.
2. A monomorphized chunk loop over such traits performs no heap allocation per
   item, while dynamic dispatch allocates exactly one future per call.
3. The two are observationally identical: same trace, counters, terminal
   outcome, durable fold, and panic behaviour.
4. Monomorphizing many pipelines costs measurably more code size and compile
   time than erasing them.

## Constraints

- Rust 1.97.1 development toolchain and Rust 1.95 MSRV;
- no Tokio, SQLx, or serializer type in the proposed signatures, and no such
  crate in the spike's non-dev dependencies;
- cooperative stop, the bounded blocking adapter, panic classification, and the
  borrowed enlisted transaction must survive unchanged;
- both dispatch forms must run the identical loop body.

The spike measures dispatch. It does not measure real I/O components, and it
does not exercise `PostgreSQL` transactions or restart.

## Toolchain findings that closed off alternatives

Checked directly rather than assumed:

| Feature | Stable 1.97.1 | Nightly 1.99.0 (2026-08-01) |
| --- | --- | --- |
| Return type notation (`R::read(..): Send`) | `E0658`, experimental | compiles behind `#![feature(return_type_notation)]` |
| `async fn` in a trait, used as `dyn` | not dyn compatible | `async_fn_in_dyn_trait` exists as a feature name; the trait is **still** not dyn compatible for `&dyn` or `Box<dyn>` |
| `#[diagnostic::on_unimplemented]` | works; replaces the `E0277` headline | — |

Waiting for the language is therefore not an option at MSRV 1.95, and erasure
must be built explicitly.

## The contract this spike settles on

One public trait per role, declared with an explicit call lifetime that mirrors
the accepted ADR-0002 signature:

```rust
pub trait ItemReader<I>: Send {
    fn read<'a>(&'a mut self, context: ReadContext<'a>)
        -> impl Future<Output = Result<ReadOutcome<I>, ReaderError>> + Send + 'a;
}
```

Implementors never write that form. They write `async fn`, and the `Send` bound
is still enforced — holding an `Rc` across an `await` is rejected at the
offending line with a note pointing back at the trait's bound.

Erasure is a **type**, not a second trait. `BoxedReader<I>` wraps a sealed,
private, dyn-compatible mirror and is itself an `ItemReader<I>`, the way
`Box<dyn Iterator>` is an `Iterator`. Consequently there is one chunk loop:
`driver::run`, generic over the contract. The typed pipeline and the
dynamically dispatched pipeline are that same function with different type
arguments.

### Alternatives rejected

- **Two parallel contracts** — keep the ADR-0002 boxed traits public and add
  generic ones beside them. Rejected: it doubles the component contract, the
  step type, and every piece of surrounding wiring, and it leaves the
  allocation win as opt-in, which is most of the reason to accept the RFC at
  all.
- **Blanket impl on `Box<dyn ItemReader<I>>`** — verified to work, and it lets
  today's handles satisfy a generic trait with no wrapper. Rejected anyway: it
  makes the old traits a permanent second public contract, requires one
  irrevocable blanket impl per container shape, and defers the "you are boxing
  here" failure to the registry boundary instead of the construction site.
- **Associated future types (GATs)** — implementors would have to name a future
  type that `async` blocks do not have. Pre-RPITIT ergonomics for no gain.
- **`async-trait` in the public contract** — still boxes, and forces a macro.
  Already rejected by ADR-0002.

## Experiment

Source:

- `spikes/m6-item-hot-path/src/contract.rs` — the contract, the sealed mirror,
  and the `Boxed*` handles;
- `spikes/m6-item-hot-path/src/driver.rs` — the one chunk loop;
- `spikes/m6-item-hot-path/src/workload.rs` — one allocation-free workload
  written the way an application would write it;
- `spikes/m6-item-hot-path/src/allocation.rs` — a counting global allocator;
- `spikes/m6-item-hot-path/src/sizes.rs` — const-generic pipelines for the
  code-size and compile-time comparison;
- `spikes/m6-item-hot-path/src/composite.rs` — the decorator and composite
  shapes M6 has to ship, written against the contract;
- `spikes/m6-item-hot-path/src/erased.rs` — adapters onto the ADR-0002 traits,
  retained as migration evidence.

Tests and measurement: `tests/equivalence.rs`, `tests/allocation.rs`,
`tests/dispatch_shape.rs`, `tests/contract_shape.rs`,
`tests/composite_shape.rs`, `tests/diagnostics.rs`, `tests/panic.rs`, the
`tests/ui` fixtures, `src/bin/measure.rs`, and
`src/bin/size-{typed,boxed}{,-1}.rs`.

Reproduce:

```console
./spikes/m6-item-hot-path/run-item-hot-path-spike.sh
```

The script runs every test, records compile time and binary size for one and
sixteen pipelines on each path, runs the throughput and allocation harness, and
writes the raw record to `target/rfc-0005-spike.json`.

## Acceptance and rejection criteria

The contract is viable only if it:

- compiles at the supported MSRV without `async-trait` and without boxing;
- allocates nothing per item in steady state;
- is observationally identical under both sets of type arguments across
  completion, filtering, stop, failure, and panic at every component;
- keeps a usable erased handle for heterogeneous composition;
- still borrows an enlisted transaction for the duration of a writer call;
- can still produce the ADR-0002 handles that existing code expects;
- imposes no code-size or compile-time cost needing a budget exception.

## Results

Host: Apple M1 Max, 10 cores, Darwin arm64, `rustc 1.97.1 (8bab26f4f
2026-07-14)`, release profile.

### Allocation

10,000 items, chunk size 100. Stop token, batch buffer, trace storage, and the
handles themselves are all built before the measurement window opens.

| Type arguments | Allocations | Bytes | Per item |
| --- | --- | --- | --- |
| Concrete | 0 | 0 | 0.0000 |
| `Boxed*` | 20,101 | 571,224 | 2.0101 |

The boxed figure is exactly `2N + 1 + chunks`: one boxed future per read
including the read that reports end of input, one per process, and one per
chunk write. Both rows are for a pipeline with **no item listeners**; see the
scope note below. `tests/allocation.rs` asserts that identity rather than a
threshold, so a change that adds a hidden allocation fails the test instead of
drifting.

### Throughput and latency

1,000,000 items, chunk size 1,000, tracing disabled, seven interleaved
repetitions after a warm-up of each, best-of reported.

| Type arguments | ns/item | items/s | best (ns) | mean (ns) |
| --- | --- | --- | --- | --- |
| Concrete | 4.82 | 207,370,271 | 4,822,292 | 5,001,738 |
| `Boxed*` | 66.10 | 15,127,678 | 66,104,000 | 77,932,279 |

About 61 ns per item, a factor of 13.7. The factor is an upper bound on the
benefit, not a forecast: these components do arithmetic only, so dispatch is
essentially the entire cost. What transfers to a real workload is the ~61 ns
and the two allocations per item. The boxed mean sits well above its best,
which is ordinary allocator noise; the concrete path's spread is much tighter.

### Code size and compile time

Sixteen distinct pipelines per binary, each with its own item type, reader,
processor, and writer; per-binary target directory; the shared library built
untimed first so the recorded time is the bin crate's own codegen.

| Binary | Pipelines | Bytes | Build (ms) |
| --- | --- | --- | --- |
| `size-typed-1` | 1 | 433,728 | 251 |
| `size-boxed-1` | 1 | 452,896 | 286 |
| `size-typed` | 16 | 450,240 | 695–728 |
| `size-boxed` | 16 | 524,320 | 652–752 |

Marginal cost of one more pipeline: **1,100 bytes concrete, 4,761 bytes boxed**.
Hypothesis 4 is refuted. Erasure is not the cheaper form here, because a boxed
pipeline still monomorphizes the driver and then adds a handle type, a sealed
vtable, boxed-future layout, and drop glue on top. Repeated compile-time
samples overlap, so at this scale there is **no measurable compile-time
difference** in either direction.

This is bounded by component body size. These bodies are a few instructions; a
monomorphized CSV or SQL component would grow the concrete marginal figure
while leaving the boxed one roughly flat, so there is a crossover this spike
cannot locate. What it does establish is that monomorphization needs no
code-size exception at the scale the RFC proposes.

### Equivalence, panics, dispatch shape, and migration

Twenty-five tests, all passing.

`tests/equivalence.rs` — 13 cases comparing the full ordered trace, all four
counters, the terminal outcome, and the writer's fold: whole chunks, a partial
final chunk, empty input, a chunk larger than the input, filtering, stop before
the first read, a stop outcome from each of the three components, a failure
from each of the three, and a mid-chunk failure that must leave the same
committed prefix. Under the single-contract design these runs are the same
function, so the suite is regression cover for the `Boxed*` handles rather than
the equivalence argument itself.

`tests/panic.rs` — a panic in the reader, the processor, or the writer unwinds
with an identical payload under both sets of type arguments, and a clean run
panics under neither.

`tests/dispatch_shape.rs` — the compiler comparator fails the contract's trait
form with the expected `not dyn compatible` diagnostic, which is why the sealed
mirror exists; the `Boxed*` handles are `Send` and nameable; and a
`Vec<BoxedProcessor<Record, Output>>` holds two different concrete processors
with no `dyn` in the type the application writes.

`tests/contract_shape.rs` — a writer borrows an enlisted transaction for the
duration of its call, concretely and through `BoxedWriter`, with identical
statement counts; and a contract component still produces a
`Box<dyn oxide_batch::ItemReader<Record>>`, which is what makes retiring the
ADR-0002 traits safe.

### Scope limit: item listeners

The measured pipeline registers no item listeners. Item listeners are also
per-item — `ItemListenerSet::before_read` and its siblings run around every
read, process, and write — and `forward_pass` boxes once per listener per
phase. An empty set costs nothing, because the loop runs zero times, so the
default configuration does get the zero-allocation result. Every registered
item listener adds one boxed future per item per phase it participates in.

That is not a defect in the contract; a listener set is a heterogeneous,
registration-ordered collection whose purpose is dynamic composition, and
monomorphizing it would mean type-level lists. It is a limit on what this
result claims, and M6 ships a listener taxonomy, so the claim has to be stated
with the qualifier attached rather than as "zero allocations per item" full
stop.

## Correctness and risk review

- There is one loop. Equivalence is a property of the same code run twice, not
  of two implementations kept in sync.
- Faults are positional, not time-based, so both runs see the identical
  sequence and results are deterministic.
- The counting allocator is process-global. The measuring test is the only test
  in its binary, and the timing phase runs with counting disabled.
- The harness uses a `block_on` that panics on `Pending` rather than an async
  runtime, so no scheduler work is attributed to either path. Valid only
  because no spike component yields.
- The spike relaxes the workspace `unsafe_code = "forbid"` to `deny` in one
  private crate so `src/allocation.rs` can carry a single `unsafe impl
  GlobalAlloc`. No published crate inherits the relaxation.
- **Retracted finding.** An earlier draft of this spike reported that erasure
  forces `I: 'static` on the item type and that a superseding ADR would have to
  state it. That was an artifact of eliding the call lifetime in the trait
  declaration, not a property of the design. With the explicit `'a` the bound
  disappears; `tests/contract_shape.rs` pins its absence.
- **Delegating-component cost, measured rather than asserted.** See the
  ergonomics review below.
- The throughput ratio will not survive contact with real components. Anything
  that reads a file or a socket dwarfs 61 ns. The allocation result is the
  durable one: per-item boxing is a fixed cost no amount of I/O removes.

## Ergonomics review

RFC-0005's gate asks for "reviewed public ergonomics". Reviewing it meant
writing the components M6 will actually ship and reading what the compiler
says, not judging the declaration on paper.

### Leaf components

A leaf component writes a plain `async fn` with no lifetime, no future type,
and no `Box::pin`. `src/workload.rs` is written that way throughout, and the
`Send` bound is still enforced against the body.

### Delegating components

M6's catalogue is mostly decorators and composites. `src/composite.rs` builds
three of them against the contract — an observer reader, a filtering processor,
and a fan-out writer — and `tests/composite_shape.rs` runs a pipeline made of
all three.

- A delegating component ties its lifetimes: `async fn write<'a>(&'a self,
  items: &'a [I], context: WriteContext<'a>)`. Still `async fn`, still no
  future type, but not lifetime-free.
- A *generic* composite states bounds a leaf never has to. The minimum,
  established by removing each until the compiler objected:

  | Composite | Needs |
  | --- | --- |
  | reader decorator | `I: 'static` |
  | processor decorator | `I: Sync`, `O: 'static` |
  | writer composite | `I: Sync` |

  The rule: a type in the *returned* value needs `'static`, because the opaque
  future must outlive the call lifetime for every choice of it; a type passed
  *by reference* needs `Sync`, because `&T: Send` requires it. The diagnostics
  say exactly this, so the cost is a `where` clause, not a debugging session.
- `WriteContext` is not `Copy`, because it carries
  `&mut dyn BusinessTransaction`. A fan-out writer reborrows the enlisted
  transaction per delegate, handing it to them one at a time. That is the
  correct semantics, and the test confirms both delegates land in the one
  transaction.
- **Decoration costs no dispatch.** A pipeline of observer reader, filtering
  processor, and fan-out writer over two sinks measures **zero allocations**
  across 5,000 items (no item listeners registered; see the scope limit above). Wrapping stays monomorphized, so the allocation result
  survives the shapes real applications build.

### Implementer-facing diagnostics

Each contract trait carries `#[diagnostic::on_unimplemented]`, and
`tests/diagnostics.rs` pins the two mistakes an implementer will actually make.
The assertions are on wording, so a regression fails here rather than in
someone's editor.

A missing or wrong impl reports:

```text
error[E0277]: `NotAReader` is not an OxideBatch item reader for `Invoice`
   |
   |     drive::<Invoice, _>(NotAReader);
   |                  ^ this component cannot read `Invoice`
   = note: implement `ItemReader<Invoice>` with `async fn read(&mut self, context: ReadContext<'_>)`
   = note: the returned future must be `Send`: do not hold a non-`Send` value across an await
```

A non-`Send` body is rejected at the offending value, with the await and the
originating trait bound both named.

The earlier concern that lifetime errors would replace readable trait-bound
errors did not materialize in any case exercised here.
`#[diagnostic::on_unimplemented]` still cannot annotate a lifetime error, so
the risk is reduced rather than eliminated.

### Outcome

The contract is accepted as reviewed. No change to the declaration was needed;
the bound rule above and the diagnostic attributes are the review's output.

## Conclusion

Hypotheses 1, 2, and 3 hold. Hypothesis 4 is refuted at this scale: the
concrete path is smaller, and compile time is a wash.

The trait form should be return-position `impl Future<Output = ..> + Send + 'a`
with an explicit call lifetime — not `async fn`, which cannot express the
`Send` bound, and not `async-trait`, which reintroduces the box. There should
be one public contract per role, not two, with erasure delivered as a concrete
handle over a sealed dyn-compatible mirror. The facade cannot default to the
contract trait as a trait object, so the handle is required rather than
optional, and it does deliver the heterogeneous composition the boxed contract
provides today.

Confidence is high for feasibility, equivalence, the allocation result, the
preserved transaction borrowing, and the ergonomics of both leaf and delegating
components. Confidence is moderate for the performance benefit at realistic
workloads, and low for the code-size crossover with large component bodies.

## Follow-up

RFC-0005's approval gate has three parts. The spike and the ergonomics review
close two; [ADR-0008](../decisions/0008-item-component-contract.md) drafts the
third. What remains is acceptance — ADR-0008 to `Accepted`, ADR-0002 to
`Superseded by ADR-0008`, RFC-0005 to `Accepted` — which governance requires
before dependent M6 implementation, not after.

Out of scope here:

- transaction and restart equivalence across the two paths, which needs the
  `PostgreSQL` fixtures and therefore CI;
- the P-002 measurement against the M6 reference workload with real components,
  which is where the code-size crossover and the realistic throughput benefit
  get located;
- compile-fail coverage for item-type and `Send`/`Sync` mistakes once the
  public form is named.
