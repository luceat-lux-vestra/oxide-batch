# ADR-0012: JSON Item Representation Discloses `serde_json::Value`

- **State:** Accepted
- **Date:** 2026-08-23
- **Owners:** API and performance maintainers
- **Deciders:** project owner

## Context

The M5 preview surface and disclosure gate
([`docs/api/design-guidelines.md`](../../api/design-guidelines.md)) requires an
accepted ADR before a foreign dependency type appears in the curated
`oxide-batch` facade's public signatures. `cargo xtask surface`'s `ACCEPTED`
list is explicit that it only *records* an exception an ADR already approved;
it is "not itself a way to approve one," and a disclosure with no accepted ADR
is removed rather than listed.

Issue #148 (M6 restartable JSON/JSONL item components,
`IO-STRUCTURED-001`'s M6 slice) needs a public item representation for a JSON
record (JSONL) or top-level array element. The issue's driving task locks this
choice directly: the public item representation is `serde_json::Value` (or a
restrained generic Serde representation), and it explicitly forbids
introducing a bespoke JSON AST solely to avoid exposing `serde_json::Value`.

`serde_json` is already a direct dependency of the `oxide-batch` facade crate
itself (not merely of an extracted implementation crate), used today for
internal component-state-codec payloads (`crates/oxide-batch/src/item_components/delimited.rs`
and `fixed_width.rs`'s `VersionedStateCodec` implementations, landed in #147).
#148 is the first work to place `serde_json::Value` in a *public*
reader/writer signature rather than only behind an internal codec.

A wrapper newtype around `Value` with a private field would satisfy the
rendered-surface scan syntactically, but it is indirection without a
behavioral gain: the item's actual shape (object, array, string, number,
bool, null) is already exactly what `serde_json::Value` models, so a wrapper's
accessors would need to hand back a `serde_json::Value` (or something
isomorphic to it) for the type to be usable at all -- the disclosure
reappears one level down, later, with an extra type every real integration
point (a downstream processor, a database driver's JSON column, an HTTP
client) has to convert through, when all of them already speak
`serde_json::Value` natively.

## Decision

`oxide_batch::item_components::{jsonl, json_array}`'s reader item type and
writer item bound is `serde_json::Value` directly:
`JsonLinesReader`/`JsonArrayReader` implement `ItemReader<I>` for
`I: From<Value>` (satisfied by `I = Value` through the reflexive blanket
`impl<T> From<T> for T`), and `JsonLinesWriter`/`JsonArrayWriter` implement
`ItemWriter<I>` for `I: Into<Value>`. No OxideBatch-owned JSON value wrapper
is introduced.

This is a bounded, named exception to the M5 disclosure gate's "serializer"
label, not a relaxation of the gate itself:

- it covers exactly `serde_json::Value` reachable from
  `item_components::jsonl`/`item_components::json_array`'s public
  reader/writer/constructor surface; no `serde_json::Error` or other
  `serde_json` type crosses a public signature (a parse failure is converted
  into the existing value-redacted `ReaderError`/`WriterError`, exactly like
  every other component's typed failure);
- it does not extend to any other serializer type, to a database driver, or
  to any other class the disclosure gate names;
- `serde_json::Value` is a stable, ecosystem-standard interchange type, not a
  database connection, a runtime handle, or a credential -- it is the data
  model this feature exists to carry, not an implementation detail a future
  refactor would want to hide. The M5 disclosure gate's own enumerated
  "Prohibited disclosure classes" in `docs/api/design-guidelines.md` names
  seven kinds (async-runtime, database driver, telemetry-SDK, credential,
  deployment-authorization, sensitive-payload, user-supplied-error-text);
  none of them is "serializer" or "record representation." The `"serializer"`
  label that `xtask/src/surface.rs`'s `CLASSES` table produces exists to make
  the violation message readable; it does not by itself widen the gate's
  named prohibitions.

`cargo xtask surface`'s `ACCEPTED` list records this exception with one entry
per disclosing item, citing this ADR, exactly as its own documentation
requires.

## Consequences

- `oxide_batch::item_components::jsonl`/`json_array` publicly disclose
  `serde_json::Value`; a caller composing a JSON reader/processor/writer
  pipeline works directly in `serde_json::Value`, with no OxideBatch-specific
  conversion step;
- this exception does not license disclosing any other `serde` ecosystem type
  (a format-specific `Serializer`/`Deserializer`, a derive-generated type, or
  a different format's value type) in a public signature; a future
  structured-format component (XML, Avro, M13) making an equivalent choice
  needs its own review against this ADR's scope, via an extension or a
  sibling ADR, not an assumed blanket coverage;
- the M6 item-surface boundary this disclosure touches is unaffected beyond
  the disclosure itself: `serde_json::Value` is `'static` and `Send + Sync`,
  so an `ItemReader<Value>`/`ItemWriter<Value>` pairing places no additional
  constraint on the M7 flow/registry, M8 portability, M9 integration, or
  M10/M11 concurrency surfaces beyond what any other concrete item type
  already would.

## Alternatives considered

- **A bespoke `JsonValue` AST duplicating `serde_json::Value`.** Rejected by
  the driving task directly, and on the merits: it does not remove
  `serde_json` as the parser, needs a full conversion surface to remain
  usable, and every real downstream integration point already speaks
  `serde_json::Value`.
- **A newtype wrapper (`pub struct JsonRecord(serde_json::Value)`, private
  field, accessor methods).** Rejected: it only avoids the rendered-surface
  link if no accessor returns or accepts `serde_json::Value` directly, which
  would leave the wrapper unable to expose or accept structured content --
  the same disclosure reappears the moment a caller needs the actual value,
  just later and with an extra type to learn.
- **A generic `Serialize + DeserializeOwned` bound instead of a concrete
  `Value`.** Left available as future work, not excluded by this ADR:
  `JsonLinesReader`/`JsonArrayReader`/`Writer`'s framing, restart, and
  bounded-memory design does not depend on `Value`'s concrete shape, so a
  later ADR could extend or supersede this one with a typed-schema surface
  if one is wanted. Not chosen now because #148's locked scope is the M6
  slice's `Value`-based contract.
- **Defer #148 until a broader serializer-disclosure policy exists.**
  Rejected: the driving task's scope is this M6 slice specifically, and the
  `"serializer"` label already present in `xtask/src/surface.rs`'s `CLASSES`
  table shows the tooling anticipated needing exactly this decision; this ADR
  is that decision, scoped narrowly to `serde_json::Value` in
  `item_components::jsonl`/`json_array`.

## Validation

`cargo xtask surface` passes with exactly the `item_components::jsonl`/
`item_components::json_array` `serde_json::Value` disclosures listed in
`ACCEPTED`, citing this ADR, and no other new disclosure.
`crates/oxide-batch-test/tests/item_components_jsonl.rs` and
`item_components_json_array.rs`'s typed/erased equivalence tests demonstrate
`serde_json::Value` working as an ordinary item type through the existing
`ItemReader`/`ItemProcessor`/`ItemWriter`/`BoxedReader`/`BoxedProcessor`/
`BoxedWriter` machinery, with no OxideBatch-specific conversion.

## Revisit triggers

Revisit if a future structured-format component needs a shared,
format-neutral item representation (motivating the generic
`Serialize`/`DeserializeOwned` alternative above), if a `serde_json`
major-version change alters `Value`'s own compatibility surface, or if a
future decision removes `serde_json` as a direct dependency of the facade
crate -- the last of these supersedes this ADR outright rather than merely
revisiting it.
