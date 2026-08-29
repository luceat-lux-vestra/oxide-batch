# Test-Kit Tutorial

**State:** Accepted

This tutorial covers `oxide-batch-test`'s actual public API
(`crates/oxide-batch-test/src/lib.rs`) — the crate published for application
test code, never re-exported from the `oxide-batch` facade and never
depended on by it (a workspace dependency cycle `cargo xtask deps` rejects;
see [Restart and State](restart-and-state.md) if you're wondering why a
component's own test file can't just pull this crate in directly the way
application code does — `oxide-batch`'s own integration tests use local
support modules instead, for exactly this reason).

Every example below cites a real file:function in this repository's own
test suites that already compiles and passes — go read the cited test for
the full, worked, currently-passing version rather than trusting a
hand-copied snippet to still be accurate.

## Component test

`ComponentFixture` hands out real production call contexts
(`ReadContext`/`ProcessContext`/`WriteContext`) without constructing a full
job or any private/internal type:

```rust
let fixture = ComponentFixture::new();
let mut reader = MyReader::new();
assert_eq!(reader.read(fixture.read_context()).await?, ReadOutcome::Item(1));
```

Real example: `crates/oxide-batch-test/tests/gate_g_scenarios.rs::single_step_and_scoped_component_harness_construct_fixture_context`
exercises a reader, processor, and writer this way in one test.
`fixture.request_stop()` requests cooperative stop for every context the
fixture issues afterward, so a component's `Stopped` handling is testable
without a real deadline race; `fixture.clock()`/`fixture.ids()` give a
deterministic `ManualClock`/`DeterministicIds` if your component needs
either.

## Step test

`TestStep::new(name, size, reader, processor, writer)` drives a real
`ChunkStep` through its production `execute` path with a standalone
transaction manager and no-op completion — for a durable adapter under
test, `TestStep::with_transactions(.., transactions, completion)` takes an
explicit `Arc<dyn ChunkTransactionManager>`/`Arc<dyn ChunkCompletion>`
instead.

```rust
let mut step = TestStep::new(StepName::new("scope_step")?, ChunkSize::new(2)?, reader, processor, writer);
let report = step.run().await;
assert_eq!(report.outcome(), ChunkExecutionOutcome::Completed);
```

Real example: the same `gate_g_scenarios.rs` test above,
`report.committed_counts().read().get()` for asserting on exactly how much
was read/processed/written.

## Job test

`TestJob::embedded(chunk_job)` builds a full-job harness backed by a fresh,
isolated `EmbeddedRepository`, launching through the real `JobLauncher`
path — `TestJob::with_embedded(job, &embedded)` reuses an explicit
`EmbeddedRepository` across multiple jobs in one test.

```rust
let mut job = TestJob::embedded(chunk_job);
job.clock().advance(Duration::from_secs(5))?; // never moves on its own
let report = job.launch(&JobParameters::new()).await?;
assert_eq!(report.launch().instance().id().get(), 1); // deterministic IDs
```

Real example: `gate_g_scenarios.rs::full_job_harness_launches_with_deterministic_clock_and_id`.
A fresh embedded fixture's ID sequence is reproducible — the first launch's
instance ID and job-execution ID come out in a fixed, known order, which is
what makes assertions like the one above stable across runs.

## Failure, panic, and stop injection

`oxide_batch_test::inject`'s `InjectedReader`/`InjectedProcessor`/
`InjectedWriter`/`InjectedStream`/`InjectedTransactions` wrap a real
component and fire a configured `ComponentAction` (`Fail(FailureCategory)`,
`Panic`, or `Stop(StopSource)`) when a `Trigger` (e.g. `Trigger::immediately()`)
matches, logged to an `InjectionLog` under a caller-chosen `InjectionId` —
distinguishable from a genuine framework defect by that ID, which no real
failure ever produces.

```rust
let fail_log = InjectionLog::new();
let fail_id = InjectionId::new(1);
let mut step = TestStep::new(
    StepName::new("fail_step")?, ChunkSize::new(2)?,
    InjectedReader::new(real_reader, Trigger::immediately(),
        ComponentAction::Fail(FailureCategory::UserComponent), fail_id, fail_log.clone()),
    Identity, NoopWriter,
);
let report = step.run().await;
assert_eq!(report.outcome(), ChunkExecutionOutcome::Failed(ChunkFailure::Reader));
assert!(fail_log.fired(fail_id));
```

Real example (all three actions in one test, including the panic case —
which asserts the real panic-to-typed-failure boundary converts it, so the
test never unwinds, and the stop case, which uses `step.run_with_stop(&token)`
rather than a wall-clock race):
`crates/oxide-batch-test/tests/gate_g_scenarios.rs::failure_panic_and_stop_injection_are_available_to_application_tests`.
`InjectionPoint` (`Read`/`Process`/`Write`/`StreamOpen`/`StreamUpdate`/
`StreamClose`/`PreCommit`) is where else injection can target beyond the
reader shown above — see `crates/oxide-batch-test/src/inject.rs` for the
full enum and every `Injected*` wrapper's constructor.

## Restart testing

`oxide_batch_test::restart::range_reader(namespace, len)` builds a
restart-aware reader/stream/contract/position tuple over a deterministic
range; `ObservingTransactions<M>` wraps a real transaction manager `M` and
records what actually got committed (`observed_progress()`,
`observed_component_state()`), so a restart test asserts on structured
observations rather than re-deriving what "should" have happened.

Real, fully worked example: `crates/oxide-batch-test/tests/postgres_item_components_restart.rs::peek_decorated_reader_restarts_from_the_last_committed_checkpoint`
— runs an attempt partway, kills it, builds a *second* attempt with a fresh
`range_reader` over the same namespace, wraps its transaction manager in
`ObservingTransactions`, and asserts the observed progress matches exactly
what should have resumed from the durable checkpoint. This is the pattern
to follow for a custom stateful component's own restart evidence, per the
[extension guide](extension-guide.md#stateful-itemstream) — not Gate B's
PostgreSQL-fixture-specific internals in
`crates/oxide-batch/tests/support/gate_b.rs`, which are scoped to this
repository's own typed-vs-`Boxed*` campaign.

## Crash/process-kill testing

The `postgres` feature's `process` module gives a real, separate-OS-process
SIGKILL harness — not a simulated kill:

```rust
// in the test process:
let mut child = spawn_worker_test("worker_fn_name", &handshake)?;
wait_for_file(&handshake.join("reached"), Duration::from_secs(10))?;
let status = kill_and_wait(&mut child)?;
assert!(was_sigkilled(status));

// in the worker function (re-launched as a subprocess via --exact worker_fn_name):
if is_worker() {
    let handshake = handshake_dir().ok_or("no handshake dir")?;
    // .. do real work up to the point you want to kill it at ..
    announce(&handshake.join("reached"))?;
    park_until_killed(); // never returns; the test process kills this one
}
```

Real example: `crates/oxide-batch-test/tests/process_fixture.rs` — both the
worker body (`process_fixture_worker`) and the driving test
(`process_fixture_kills_and_reports_sigkill`) in one file, the smallest
complete round-trip of this pattern in the repository.

## PostgreSQL fixture

`postgres::PostgresFixture` is an isolated, self-cleaning `PostgreSQL`
repository fixture — required for a durable restart test, since only a real
`ChunkTransactionManager` backed by an actual database proves inherited
progress rather than an in-memory approximation of it.

```rust
PostgresFixture::migrate(url.clone()).await?;
let fixture = PostgresFixture::connect_with_clock(url, clock.clone()).await?;
```

Real example: `crates/oxide-batch-test/tests/postgres_fixture.rs::repository_fixture_cleans_up_isolated_metadata`.
