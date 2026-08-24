# M6 Multi-Resource and Object-Store Basics Evidence

**State:** Complete on merge

**Issue:** [#150](https://github.com/luceat-lux-vestra/oxide-batch/issues/150)

This record maps issue #150's required component families and evidence
requirements to production types and deterministic test evidence. It
implements multi-resource readers/writers and the M6 slice of object-store
capability basics, deliberately excluded from #146's scope, under the
composition rules closed by
[M6 Gate E](m6-design-gate-evidence.md#gate-e--composition-semantics) applied
to a resource set instead of a fixed, statically declared delegate list. It
does not reopen ADR-0008, Gate C/D/E, or the `ItemStream` contract (#144); it
does not implement S3/Azure/GCS SDK integrations, credential management,
multipart certification, or provider-specific retry/consistency
certification (all M9); and it does not touch #151 (listeners), #152
(configuration ergonomics), or #153 (M6 exit campaign).

## Audit performed before implementation

Per #150's own instruction not to re-derive or duplicate #146's catalog, the
following was confirmed present and reused rather than rebuilt:

| Requirement | Existing support (pre-#150) | Gap | Owner/type this PR adds |
| --- | --- | --- | --- |
| Multi-resource reader | `CompositeReader` has the right ordered-traversal shape but is documented in-memory-only, not restartable, over a fixed statically-registered delegate list | No restartable, runtime-sized resource set | `MultiResourceReader`/`MultiResourceReaderStream` |
| Multi-resource writer | `FanOutWriter` fans one chunk out to several delegates (not the same problem: partitioning output across resources) | No ordered output partitioning across resources | `MultiResourceWriter`/`MultiResourceWriterStream` |
| Resource ordering | None; nothing computed order from filesystem/listing order either (no prior art to avoid) | Caller-declared order with a fail-closed identity check | `ResourceSet`/`ResourceIdentity`/`ResourceSetRevision` |
| Restart position | `ComponentStateEnvelope`/`ComponentStreamIdentity`/`DefaultComponentCodec`/`VersionedStateCodec` (#144) fully reusable as-is | None; only a schema built on top was needed | `MultiResourceState` (nested-envelope schema, see below) |
| Versioning | `ComponentRevision`/schema-version/codec-version machinery (#144) fully reusable | None | Reused verbatim; no new versioning primitive |
| Failure semantics | Composition taxonomy's "wrapper must not claim stronger capability than delegate" rule (Gate E) already normative | None; only application to a resource set was needed | `MultiResourceOpenError` (redacted, ordinal-scoped) |
| Object-store basic read/write | None | Full gap | `ObjectStoreCapability`, `InMemoryObjectStore`, `ObjectStoreReaderOpener` |
| Peek/aggregate gap | `PeekReader`/`AggregatingReader` (#146) delegate restartability/ordering 100% to their inner component | No gap once multi-resource components exist as valid inner components | No production change; composition evidence only (see below) |
| Thread-safety edge cases | `SynchronizedWriter` (#146) establishes real mutual exclusion around its delegate | No gap once multi-resource components exist as valid delegates | No production change; composition evidence only (see below) |

No production defect was found in existing runtime/state/transaction code
during this audit or implementation; the stop condition in #150's own
instructions was never triggered.

## Component families delivered

| Family | Type(s) | Module |
| --- | --- | --- |
| Resource identity and ordered set | `ResourceIdentity`, `ResourceSet`, `ResourceSetRevision` | `item_components::multi_resource` |
| Multi-resource reader | `MultiResourceReader`, `MultiResourceReaderStream`, `MultiResourceReaderOpener`, `multi_resource_reader` | `item_components::multi_resource` |
| Multi-resource writer | `MultiResourceWriter`, `MultiResourceWriterStream`, `MultiResourceWriterOpener`, `RolloverPolicy` (`BatchCountRollover`, `NoRollover`), `multi_resource_writer` | `item_components::multi_resource` |
| Object-store capability | `ObjectStoreCapability`, `ObjectIdentity`, `ObjectVersionToken`, `ObjectMetadata`, `ObjectListPage`/`ObjectListContinuation` | `item_components::object_store` |
| Object-store fixture | `InMemoryObjectStore` (first-class contract evidence, not a toy) | `item_components::object_store` |
| Object-store to multi-resource bridge | `ObjectStoreReaderOpener`, `ObjectStoreWriterOpener` (implement `MultiResourceReaderOpener`/`MultiResourceWriterOpener`) | `item_components::object_store` |

Both new modules live under `oxide_batch::item_components`, matching #146's
placement precedent (a dedicated public module, not flattened into the
facade root).

## Why this is not `CompositeReader` plus a checkpoint

`CompositeReader`'s ordered-traversal shape is right, but its delegates are
pre-constructed by the caller and each delegate's own `ItemStream` (if any)
is registered independently under a statically declared
`ComponentStreamIdentity`. A multi-resource component cannot use that shape:
the resource count is often not known until runtime (a directory listing, an
object-store prefix), so it cannot register one `ComponentStreamIdentity`
per resource against `ChunkComponentRevisions`, which requires a fixed,
statically declared namespace set. `MultiResourceReader`/`MultiResourceWriter`
instead own exactly **one** namespace for the whole ordered resource set and
construct each resource's delegate on demand through a
`MultiResourceReaderOpener`/`MultiResourceWriterOpener` -- never more than
one resource open at a time.

## Durable position: nested envelope, not a second state mechanism

The durable envelope a `MultiResourceReaderStream`/`MultiResourceWriterStream`
produces carries exactly three things:

- **`ResourceSetRevision`**: a SHA-256 content fingerprint over the ordered
  resource-identity sequence. A restart whose caller-supplied resource set no
  longer matches this revision fails closed
  (`FailureCategory::UnsupportedCapability`) instead of silently
  reinterpreting a stored resource index against a different physical
  resource -- inserting or removing a resource ahead of the committed index
  is exactly the case this guards against, and it is the reason a bare
  `{ resource_index: usize, offset: u64 }` position (explicitly rejected by
  #150's own instructions) was never implemented.
- the current resource's ordinal index.
- the current resource's own delegate position, embedded verbatim.

That last point is what lets this module reuse the existing M6 component-state
contract exactly, rather than inventing a second state mechanism: every
durable column `ComponentStateEnvelope` carries (namespace, schema id/version,
codec id/version, checksum algorithm/value, and the bounded payload) is a
public accessor, and `ComponentStateEnvelope::from_durable` reconstructs an
envelope from exactly those columns. The delegate reader/writer opened for the
current resource has its own candidate envelope captured via its own
`ItemStream::update`, and that envelope's durable columns are embedded as
plain data inside the outer envelope. On restart, the reverse happens: the
embedded columns reconstruct the delegate's envelope via `from_durable`
(re-verifying its checksum), and the delegate's own `ItemStream::open`
restores its position from it, unaware it is nested inside another
component's state at all. No delegate state is hidden -- it is carried in
full, just not under a second, separately registered namespace, per the
composition taxonomy's "must not hide a delegate's state" rule.

Everything before the current resource is implicitly fully committed (this
module never revisits a resource once it advances past it, exactly like
`CompositeReader`); everything after has not started. So the durable envelope
only ever needs to describe the one current resource, never the whole set's
progress -- keeping the payload small regardless of resource-set size.

## Capability propagation (composition taxonomy, Gate E)

- **Ordering**: resources are traversed/filled in `ResourceSet`'s declared
  order; within a resource, delegate order is preserved.
- **Restartability**: the meet of the wrapper's own contribution (always
  restartable, since the durable envelope is self-contained) and the
  opener's declared restartability, supplied explicitly at construction
  (`multi_resource_reader`/`multi_resource_writer`'s `restartability`
  parameter) because a resource backend's stable-identity guarantee (e.g. an
  object store without version tokens) cannot be introspected generically.
- **Thread safety**: used exclusively (`&mut self`) like every reader;
  `MultiResourceWriter` is `Send + Sync`, with an async-aware lock guarding
  the active resource and rollover decision together as one atomic
  transition.
- **Transaction/delivery**: never claims a stronger mode than the current
  delegate writer supports; a rollover happens between writer calls, never
  inside one enlisted call (`WriteContext`'s reborrow technique, matching
  `FanOutWriter`'s established pattern).
- **Failure semantics**: a failure while opening or reading/writing resource
  N does not roll over to resource N+1 -- the same delegate/transition is
  retried at the same resource on the framework's own retry contract.
- **Close**: the currently active resource's delegate stream is closed
  through the paired stream's `close`; resources already advanced past were
  never left open.
- **Errors**: `MultiResourceOpenError`/`ObjectStoreError` carry only a
  resource ordinal (or nothing) and a stable `FailureCategory` -- never the
  underlying I/O error's payload, path, or message, per this crate's
  component-error redaction convention.

## Scenario -> test mapping

### Reader (`multi_resource.rs`, `#[cfg(test)]`)

| Scenario | Test |
| --- | --- |
| Ordered traversal across resources | `ordered_traversal_across_resources_reads_in_declared_order` |
| Zero-resource input | `empty_resource_set_reader_returns_end_of_input_immediately` |
| Commit after crossing a resource boundary mid-attempt; restart resumes inside the new resource, not its start | `restart_mid_resource_resumes_at_committed_position_not_resource_start` |
| Resource-set/order changed since the committed checkpoint -> reject | `resource_set_revision_mismatch_on_restart_is_rejected` |
| Resource open failure does not advance past the failure point; a retry re-hits the same transition | `resource_open_failure_does_not_advance_past_the_failure_point` |
| `ResourceIdentity` construction refuses one byte past its ceiling, empty input, and control characters | `resource_identity_rejects_one_byte_past_its_ceiling`, `resource_identity_rejects_empty_and_control_characters` |

### Writer (`multi_resource.rs`, `#[cfg(test)]`)

| Scenario | Test |
| --- | --- |
| Rollover writes successive batches to successive resources in order | `rollover_writes_batches_to_successive_resources_in_order` |
| Restart mid-resource resumes the delegate's committed position, not resource start, and does not roll over early | `writer_restart_mid_resource_resumes_committed_position` |
| Stale resource-set revision rejected on writer restart | `stale_resource_set_revision_is_rejected_on_writer_restart` |

### #146 residual composition audit (`multi_resource.rs`, `#[cfg(test)]`)

| Scenario | Test |
| --- | --- |
| `PeekReader` over `MultiResourceReader` crosses a resource boundary mid-peek without corrupting order | `peek_reader_over_multi_resource_reader_crosses_resource_boundary_without_corrupting_order` |
| `SynchronizedWriter` over `MultiResourceWriter` preserves rollover order under the added mutual exclusion | `synchronized_writer_over_multi_resource_writer_preserves_rollover_order` |

Both are audit evidence, not production-code gap fixes: `PeekReader` and
`SynchronizedWriter` already delegate 100% of restartability/ordering to
their inner component (`peek.rs`, `sync.rs`), so a resource-boundary
transition mid-peek or mid-rollover is exactly the existing single-resource
case from the inner component's point of view. No change to either decorator
was needed.

### Object store (`object_store.rs`, `#[cfg(test)]`)

| Scenario | Test |
| --- | --- |
| List pagination is deterministic and key-ordered, not insertion-ordered, across pages | `list_pagination_is_deterministic_and_orders_by_key` |
| Missing object | `missing_object_get_and_stat_fail_with_unsupported_capability` |
| `ObjectIdentity` construction refuses one byte past its ceiling, empty input, and control characters | `object_identity_rejects_one_byte_past_its_ceiling`, `object_identity_rejects_empty_and_control_characters` |
| Bounded write (oversized `put` rejected, not truncated) | `put_bounded_by_max_object_bytes` |
| Replacement object publishes a new version token; `get` returns the latest content and matching token | `put_get_roundtrip_returns_incrementing_version_tokens` |
| Reader restart resumes ordinal when object version is unchanged | `reader_restart_resumes_ordinal_when_object_version_unchanged` |
| Reader restart rejects a replaced object (version-token mismatch) rather than resuming against new content | `reader_restart_rejects_replaced_object` |
| Writer roundtrip through `MultiResourceWriter`: whole-object `PUT` accumulation across multiple `write` calls | `writer_roundtrip_through_multi_resource_writer_accumulates_and_puts` |

Cancellation is honored by construction (`ObjectItemWriter::write` and
`MultiResourceWriter::write` both check `context.stop_token()` before doing
any work, returning `WriteOutcome::Stopped`), the same pattern every other
first-party writer in this crate uses; a dedicated stop test was judged
redundant with the existing stop-propagation coverage already established
for that shared pattern in #146's evidence. Sensitive-metadata redaction is
by construction: `ObjectStoreError`/`MultiResourceOpenError` carry only a
`FailureCategory` (and, for the latter, a resource ordinal), never a
provider error payload or object content; there is no redaction logic to
separately test because there is no leak path to redact.

### Real PostgreSQL crash/restart evidence

`crates/oxide-batch-test/tests/postgres_multi_resource_restart.rs` exercises
the real production restart path (`ChunkJob`/`ChunkStep`/
`PostgresChunkStateProvider`), the same way
`postgres_item_components_restart.rs` does for a single-resource decorated
reader:

- `multi_resource_reader_restarts_across_a_resource_boundary_crash`: a stop
  is injected on the exact underlying read that would cross from a
  4-item first resource into a 6-item second resource, so the last durably
  committed envelope still names the first resource at its fully exhausted
  position, never having transitioned yet. A fresh reader/stream pair,
  relaunched through `TestJob`/`JobLauncher` against the same job name,
  transitions into the second resource and reads it from its own start --
  never replaying the first resource, never skipping the second resource's
  first item.
- `multi_resource_reader_completes_across_a_resource_boundary_with_no_crash`:
  the same ten-item, two-resource read completes in one attempt with no
  injected stop, using a chunk size (3) that does not align with the
  4-item resource boundary, proving normal (non-crash) traversal across a
  resource boundary through the real runtime, not only the in-memory unit
  fixtures above.

Requires `OXIDEBATCH_POSTGRES_TEST_URL`; skips (not fails) otherwise, per
this repository's PostgreSQL evidence convention. Wired into CI as "Run
PostgreSQL multi-resource restart evidence (#150)" in `.github/workflows/ci.yml`,
alongside every other `postgres_*` fixture in that job, so this evidence
cannot silently skip-green in CI the way #174 found for an earlier PR.

## Whole-object buffering is documented, not hidden (M6 basics)

Both object-store bridges buffer a whole object in memory:
`ObjectStoreReaderOpener` fetches and fully parses one object before serving
items from it; `ObjectStoreWriterOpener`'s accumulator holds everything
written to the current object so far, and reissues the full object on every
`ItemWriter::write` call (object-store `PUT` semantics are whole-object, not
append). This is bounded by a caller-supplied `max_object_bytes`, never
unbounded, but it is not streaming/multipart -- that stays M9, per #150's own
scope note.

`ObjectItemWriter`'s rustdoc documents its one known limitation directly: a
crash between a successful `put` and the runtime's own durable commit leaves
the object reflecting content whose corresponding chunk never committed. This
is the "duplicate and unknown outcomes are expected" case the integration
model requires adapters to model rather than hide -- on restart,
`ObjectItemWriterStream::open` refetches the object and compares its version
token against the last *committed* checkpoint's recorded token; an
uncommitted `put`'s version was never recorded there, so it never matches,
and the restart fails closed rather than silently accepting or discarding the
uncommitted write. No fake exactly-once guarantee is claimed anywhere in this
module.

## Allocation/performance

No per-item heap allocation was added to the item hot path: resource
transitions allocate a fresh delegate reader/writer once per resource (via
the opener), not once per item, matching #150's own allocation-avoidance
requirement. The reader's `pending_reader` handoff and `handle`/`active`
locks are held only across resource-transition boundaries, never across a
per-item `read`/`write` call in the steady state within one resource.

## Reproduction

```console
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings -A clippy::too_many_arguments -A clippy::too_many_lines
cargo test --workspace --all-features
cargo check -p oxide-batch --no-default-features
cargo check -p oxide-batch-cli --no-default-features --all-targets
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
cargo run --package oxide-batch-xtask -- deps
cargo run --package oxide-batch-xtask -- surface
cargo test -p oxide-batch-test --features postgres --test postgres_multi_resource_restart -- --nocapture --test-threads=1
```

The `postgres_multi_resource_restart` target requires
`OXIDEBATCH_POSTGRES_TEST_URL` set to an isolated migrated database and is
skipped otherwise; it was also run against a local `PostgreSQL` 18 instance
during development (both tests pass) in addition to CI.

## Ledger disposition

`ITEM-MULTI-001` moves from `Planned` to `Implemented`. `IO-OBJECT-001`'s M6
slice (provider-neutral `ObjectStoreCapability` basics) moves from `Planned`
to `Implemented`; its M9 slice (S3/Azure/GCS certification) remains
unimplemented and is not claimed here. Neither row promotes to `Verified` on
this branch: promotion requires a named released `oxide-batch` version, per
the ledger's own promotion rule, which #150 does not itself cut. See
[`docs/compatibility/conformance-matrix.md`](../compatibility/conformance-matrix.md).

## Known limitations / explicit out-of-scope

- No S3/Azure/GCS SDK integration, credential management framework,
  multipart upload certification, provider-specific retry policy, or
  provider-specific consistency certification (all M9).
- Object-store reads/writes are whole-object, not streaming/multipart.
- `ObjectStoreCapability`'s only implementation shipped in this crate is
  `InMemoryObjectStore`; real cloud adapters are M9 scope and would
  implement the same trait.
- A resource backend without a stable per-object version identity must be
  constructed with `RestartabilityDeclaration::NotRestartable`; this module
  does not (and cannot) infer that from the backend itself.
- #151 (listeners), #152 (configuration ergonomics), #153 (M6 exit
  campaign), M7 compiled-plan work, and M9 broker work are untouched.
