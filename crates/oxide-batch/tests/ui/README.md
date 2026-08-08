# Compile-fail tests

Compile-fail tests for public trait and type-system guarantees live here.
Fixtures must state the guarantee they protect and avoid incidental compiler
output outside the reviewed expectation.

`ui.rs` runs the fixtures with `trybuild`. The checked-in `.stderr` files are
reviewed compiler output; regenerate them deliberately with
`TRYBUILD=overwrite cargo test -p oxide-batch --test ui`.

The M2 facade boundary fixtures reject executor (`tokio`), database-driver
(`sqlx`), and serializer (`serde_json`) re-exports. This complements signature
review of the facade-owned boxed future, business transaction, and durable-state
codec contracts.

The M5 facade review added the telemetry-SDK fixture, which rejects
`opentelemetry` and `tracing-subscriber` re-exports. Each fixture covers one
prohibited disclosure class from the M5 preview surface gate in
`docs/api/design-guidelines.md`. A re-export is only one way to disclose a
class, so `cargo xtask surface` inspects the rendered surface for the rest.
