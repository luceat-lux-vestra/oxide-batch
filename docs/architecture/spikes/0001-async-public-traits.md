# Spike 0001: Async Public Traits and Blocking Isolation

- **State:** Complete
- **Owner:** maintainers
- **Issue:** [#6](https://github.com/luceat-lux-vestra/oxide-batch/issues/6)
- **Date:** 2026-07-29
- **Decision/ADR:** [ADR-0002](../decisions/0002-execution-model.md)

## Decision to unblock

Select a public extension-trait representation that supports dynamic dispatch,
borrowed call scopes, cooperative cancellation, panic classification, and
explicitly bounded blocking work without putting Tokio types in the core
contract.

## Hypotheses

1. Native `async fn` in a trait is ergonomic for static dispatch but is not
   dyn-compatible on the supported toolchain.
2. An OxideBatch-owned `Pin<Box<dyn Future + Send + 'a>>` alias is
   dyn-compatible and can borrow the component, item, stop token, and a
   transaction-scoped port for the duration of a call.
3. Blocking work can be bounded with a semaphore and isolated with
   `spawn_blocking`; once a synchronous call starts, stopping cannot safely
   interrupt it.
4. Panics can be converted to a stable failure classification without exposing
   panic payloads or terminating the runtime.

## Constraints

- Rust 1.97.1 development toolchain and Rust 1.95 MSRV;
- Tokio 1.53.1;
- no hidden runtime construction;
- no Tokio, SQLx, or proc-macro type in the proposed trait signatures;
- blocking concurrency must have a nonzero finite limit.

The spike evaluates semantics and compile-time shape, not allocation
microbenchmarks or final M1 naming.

## Experiment

Source and tests:

- `spikes/m0-architecture/src/execution.rs`;
- `spikes/m0-architecture/tests/execution.rs`;
- `spikes/m0-architecture/tests/native_async_comparator.rs`;
- `spikes/m0-architecture/tests/ui/native_async_trait_dyn.rs`;
- the borrowed transaction port exercised by the PostgreSQL spike.

Reproduce:

```console
cargo test -p oxide-batch-m0-spikes \
  --test execution --test native_async_comparator
```

## Acceptance and rejection criteria

The selected form must:

- be callable through `dyn Trait`;
- keep call-scoped borrows inside the returned future lifetime;
- stop cancellable async work promptly;
- classify async and blocking panics;
- keep timers responsive while blocking work runs;
- enforce the configured blocking concurrency limit;
- define what happens when stop arrives during an already-running blocking call.

Native async traits are rejected as the sole public form if the compiler
comparator cannot construct a trait object. A synchronous core is rejected if
it requires asynchronous I/O or cancellation to be hidden behind blocking.

## Results

Observed output:

```text
running 6 tests
test boxed_future_trait_is_dyn_compatible_and_borrows_call_scope ... ok
test cooperative_cancellation_interrupts_async_user_work ... ok
test async_panic_is_classified_and_the_runtime_remains_usable ... ok
test blocking_work_is_bounded_and_does_not_starve_async_timers ... ok
test running_blocking_work_finishes_before_late_stop_is_reported ... ok
test blocking_panic_is_classified ... ok

test result: ok. 6 passed; 0 failed

running 1 test
test native_async_trait_is_not_dyn_compatible_on_the_supported_toolchain ... ok

test result: ok. 1 passed; 0 failed
```

The blocking test launched six 100 ms calls with a limit of two. Peak observed
blocking concurrency was exactly two, while a 20 ms async timer completed
before 80 ms. A stop requested during a running blocking call was reported only
after that call completed, so no detached side effect outlived the framework
boundary.

The compiler comparator failed the native form with the expected `not dyn
compatible` diagnostic. The boxed form succeeded with `Box<dyn
AsyncProcessor>` and borrowed input. The transaction test separately exercised
the same lifetime pattern through `&mut dyn BusinessTransaction`.

## Correctness and risk review

- Cancellation is cooperative. Async components can select on the owned stop
  token; blocking code is checked before queueing and starting but runs to
  completion once started.
- `catch_unwind` and blocking `JoinError` handling classify panics as
  framework-owned errors. The panic payload is not part of the error contract.
- The blocking semaphore permit is owned by the isolated call and released on
  every return or panic path.
- The boxed future adds one allocation per dynamic async call. This is accepted
  for the initial extension boundary and should be benchmarked before changing
  representation.
- The adapter requires an existing Tokio runtime; it never creates or stores a
  global runtime.

## Conclusion

Use an OxideBatch-owned boxed-future alias for dynamically dispatched M1
extension traits. Keep Tokio as the initial executor implementation but out of
core signatures. Do not require the `async-trait` macro in the public contract.

Use an explicit bounded blocking adapter. Stop before a blocking call starts;
after it starts, await completion, record that stop arrived, and stop before the
next unit of work.

Confidence is high for API feasibility and correctness shape. Allocation cost
and optimal default blocking limits remain workload-specific M1/M3 work.

## Follow-up

- implement facade-owned names and errors during the first M1 trait issue;
- add compile examples for third-party implementers;
- benchmark boxed dynamic dispatch against static dispatch when real chunk
  workloads exist;
- revisit only if dyn-compatible native async traits stabilize across the MSRV
  or allocation is shown to violate an accepted performance budget.
