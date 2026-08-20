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

The M6 ADR-0008 fixtures pin the item component contract's compile-time
shape:

- `item_reader_natural_async.rs` and `item_reader_boxed_erasure.rs` must
  pass, proving a plain `async fn` impl satisfies `ItemReader` unassisted and
  that `BoxedReader::new` is a working, heterogeneous-registry-capable
  erasure boundary;
- `item_reader_non_static_item.rs` must pass, proving neither the contract
  nor `BoxedReader<I>` itself constrains the item type `I` to `'static` — a
  reader whose item type borrows from a buffer it owns satisfies
  `ItemReader`, and `BoxedReader<&'a str>` is a well-formed type for an
  arbitrary, non-`'static` `'a`; only `BoxedReader::new`'s own `R: 'static`
  bound constrains the component being erased;
- `item_reader_dyn_incompatible.rs` and `item_processor_missing_impl.rs` must
  fail, proving the contract traits cannot be named as `dyn Trait` and that a
  missing impl reports through the `#[diagnostic::on_unimplemented]` wording
  rather than a bare `E0277`;
- `item_reader_non_send_body.rs` must fail, proving the contract's `Send`
  bound is still enforced against a plain `async fn` body — holding a
  non-`Send` value (`Rc`) across an `.await` is rejected, with the compiler
  pointing at the offending value and the await point.
