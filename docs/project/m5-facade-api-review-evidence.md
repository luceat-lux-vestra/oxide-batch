# M5 Public Facade and Preview API Review Evidence

**State:** Complete for the review this issue owns.

**Issue:** [#101](https://github.com/luceat-lux-vestra/oxide-batch/issues/101)

**Date:** 2026-08-08

This record is the evidence for the fifth M5 workstream: reviewing the
delivered public facade against the
[M5 preview surface and disclosure gate](../api/design-guidelines.md#m5-preview-surface-and-disclosure-gate)
that the [design-gate evidence](m5-design-gate-evidence.md) closed. It
enumerates the surface as delivered, audits every prohibited disclosure class,
records what the audit found, argues the M6-M12 boundaries one at a time, and
bounds what the preview may claim.

The review changes no observable behavior. It adds one inspection command, one
compile-fail fixture, and three scenarios, and renames one existing scenario to
the identifier the design gate assigned. It changes no persisted byte, no
transaction boundary, no lifecycle write, no restart selection, no definition
fingerprint, and no normalized trace.

It does change the public API in one place, because the review found a policy
violation there: the canonical-manifest seam between the core and plan crates
disclosed a serializer type, and removing it is finding F1's remedy. That is
the only public-API change, and the facade's exported names are unchanged.

## What the review inspected, and how

The gate requires the compatibility checklist, a rustdoc leakage inspection
over the complete public surface, the public API snapshot, and compile-fail
tests for each prohibited class expressible as a type error. It names four
scenarios. All four are delivered; two of them run somewhere other than
`cargo test`, for reasons recorded under [Deviations](#deviations).

| Named scenario | Delivered as |
| --- | --- |
| `facade_exposes_no_runtime_database_or_telemetry_sdk_type` | `tests/ui.rs`, over four `trybuild` fixtures |
| `debug_output_redacts_every_sensitive_payload_class` | `tests/facade_review.rs` |
| `rustdoc_surface_contains_no_leaked_implementation_type` | `cargo xtask surface` |
| `public_api_snapshot_matches_the_reviewed_preview_surface` | `tests/facade_review.rs` |

The rustdoc inspection is the load-bearing one, because it is the only check
that sees the surface as a consumer does. Reading `src/lib.rs` cannot: the
facade re-exports 225 of its 426 names from three implementation crates, so a
disclosure can arrive through an item this crate never mentions. Two mechanics
of that inspection are worth recording, because both changed the result:

**Dependencies are documented rather than skipped.** Under `--no-deps`, a
crate that declares no `html_root_url` has no address for rustdoc to link to,
so its types render as bare text. `tokio` is such a crate. A probe that
returned `tokio::runtime::Handle` from a public facade function rendered as an
unlinked `Handle` and was invisible to a link scan; documenting the
dependencies gave it a resolvable link and the scan found it. A leakage
inspection built on `--no-deps` would have reported a clean surface while an
async-runtime handle sat in a public signature.

**Blanket and synthetic implementation sections are excluded.** Rustdoc lists
what every other crate implements for every type, so `impl<T> IntoEither for T`
appears on page after page. Those describe the ecosystem rather than this
surface. They account for 15,700 foreign links across the rendered facade —
`tracing`, `either`, `tracing_core`, `hashbrown`, and `equivalent` lead the
count — and not one of them is something the facade declares. Excluding them
leaves exactly the four occurrences recorded as finding F1.

**Only rendered declarations are read.** The scan looks inside the item
signature block and the code headers of members and implementations, which is
where argument types, return types, public fields, associated types, and bounds
appear. Prose is deliberately out of scope: a documentation comment may link
anywhere, and a link in a sentence discloses nothing.

**A link that leaves for an unfamiliar host is reported, not skipped.** A
rendered declaration should reach this crate, the standard library, or a
dependency. Anything else is a destination the review has not seen, so it is
reported under that host's name rather than falling through as unrecognized.
Failing on it costs one reviewed entry; ignoring it would hide the next
disclosure that arrives in a form the scan was not written for.

The inspection is exact in both directions. An occurrence it does not carry as
an accepted exception fails the build, and an accepted exception that no longer
occurs also fails, so a repaired disclosure cannot leave a stale entry behind.
Both directions were confirmed by probe, and the accepted list is empty: the
gate currently permits nothing.

The list is not how an exception gets approved. The public-boundary rule
requires an accepted ADR before a forbidden class appears in a public
signature; an entry records where such an ADR already applies and names it. An
entry without one would be the exception approving itself, which is what F1
would have become had it been carried rather than removed.

## The delivered surface

The preview claims exactly one crate. `oxide-batch` exports **426 names**:
414 always, and 12 more under the optional `postgres` feature. The committed
snapshot at `crates/oxide-batch/tests/fixtures/facade/public-api.txt` is the
authoritative list; this table is the enumeration by the group each name is
delivered through, and
`public_api_snapshot_matches_the_reviewed_preview_surface` holds the two
against each other so the surface cannot move without this record moving with
it.

| Group | Names | What it delivers |
| --- | --- | --- |
| `oxide_batch_repository` | 110 | repository, explorer, operator, recovery, retention, and paging ports and their values |
| `oxide_batch_core` | 89 | durable domain values, definition identity, durable state, and fault-policy values |
| `telemetry` | 38 | the framework-owned event, metric, span, and export contracts |
| `chunk` | 29 | chunk component contracts, business transaction ports, and their outcomes |
| `oxide_batch_plan` | 26 | the flow graph, compiled plan, and bounded local-scale nodes |
| `flow` | 20 | multi-step runtime, deciders, and partition/split factories |
| `shutdown` | 20 | shutdown coordination, deadlines, and drain reporting |
| `runtime` | 18 | the launcher, tasklet contracts, and cooperative stop |
| `repository` | 14 | the in-memory adapters, plus 12 optional `postgres` names |
| `chunk_runtime` | 13 | chunk step execution, its report, and chunk listeners |
| `item_listener` | 12 | the read, process, write, retry, and skip listener families |
| `fault_state` | 11 | bounded retry-state storage and its envelope |
| `diagnostics` | 9 | lifecycle events, correlation, and metric labels |
| `listener` | 7 | job and step execution listeners |
| `service` | 7 | the explorer, operator, recovery, and retention services |
| `fault` | 2 | the injected backoff sleeper and its outcome |
| crate root | 1 | `VERSION` |

The three implementation crates are published in lockstep as internal crates
under [ADR-0010](../architecture/decisions/0010-extracted-crate-publication.md)
and remain outside the claim. Their paths are not compatibility promises even
though Cargo can resolve them.

## Two rules, audited separately

This record keeps two verdicts apart, because they are two rules with two
scopes and they were not in the same state when the review started.

**The M5 disclosure gate** prohibits seven named implementation classes from
the preview surface. All seven are clean, and the audit below is that verdict.

**The general public-boundary rule** in the
[API design guidelines](../api/design-guidelines.md#public-boundary) is wider:
it keeps Tokio, SQLx, Serde, tracing, and OpenTelemetry types out of core
public signatures unless an accepted ADR permits it. Serde is not one of the
seven classes, and the review found four public items that carried
`serde_json::Value` in violation of this rule. That is finding F1, and it was
removed rather than recorded — see [Findings](#findings).

Neither sentence below means "the public surface is fully clean" on its own.
The gate verdict is about the seven classes; the guideline verdict is about
what a signature may name at all.

## Disclosure audit

The gate prohibits seven classes from every public signature, public field,
public associated type, trait bound, error variant, `Debug` output, and
rustdoc example.

| Class | Verdict | Evidence |
| --- | --- | --- |
| Async-runtime type, handle, or executor | Clean | `tokio` appears in no rendered signature; the re-export fixture rejects `oxide_batch::tokio`. Public async contracts use the facade-owned `BoxFuture`. |
| Database driver, connection, pool, row, or SQL fragment | Clean | `sqlx` appears in no rendered signature; the re-export fixture rejects `oxide_batch::sqlx`. `PostgresConfig`, `TlsMode`, and `CaCertificate` are facade-owned, and the enlisted writer path lends a bounded `BusinessTransaction` rather than a driver handle. |
| Telemetry SDK, exporter, or tracing subscriber | Clean | No `opentelemetry` crate is in the graph at all, and the extraction boundary check forbids one in every extracted crate. `tracing` is in the graph — `sqlx` depends on it under the `postgres` feature — but it reaches the rendered surface only through blanket implementations such as `impl<T> Instrument for T`, which every crate in the ecosystem receives; it is in no signature the facade declares. The new fixture rejects `oxide_batch::opentelemetry` and `oxide_batch::tracing_subscriber`. |
| Credential, secret, token, certificate, or connection string | Clean | `PostgresConfig` withholds the connection string, `TlsMode` and `CaCertificate` withhold the bundle, and `OwnerToken` renders as `OwnerToken(<redacted>)`. Held by `debug_output_redacts_every_sensitive_payload_class` and by `configuration_bounds_and_diagnostics_are_safe`. |
| Deployment authorization or actor-identity implementation type | Clean | `ActorRef` is a bounded closed-charset value the core never authenticates and never treats as authorization, and `AuthorizationClass` is a framework-owned three-valued classification. No deployment identity, principal, or token type crosses the boundary. |
| Sensitive payload: parameter value, execution context, checkpoint payload, or item value | Clean | Every class is swept by `debug_output_redacts_every_sensitive_payload_class`, and `retained_payloads_never_reach_a_diagnostic` holds the durable-state envelopes at their own boundary. |
| User-supplied error text | Clean | The component errors take an arbitrary user error and drop it: `TaskletError::from_error` and the `component_error!` family retain no payload and no display text, and `Display` is a fixed string. |

The sweep distinguishes two ways a class withholds a value, because they are
not interchangeable. A type that retains a sensitive value renders
`<redacted>`, so an operator can tell suppression from absence. A container
that reports its contents by count, and an error that never retained the text,
have nothing to mark. Both are asserted, and a class that rendered nothing at
all fails, because an empty diagnostic proves nothing about redaction.

Two of these verdicts previously rested on nothing executable. The durable
state envelopes redacted their payload in `Debug` with no test behind it, and
the sensitive-payload classes were held one family at a time with no check that
the set was complete. Both gaps are closed above; the envelope redaction was
confirmed to fail its new scenario when the payload is rendered.

## Findings

Neither finding is against the seven prohibited classes. F1 is against the
general public-boundary rule and was removed; F2 is against the M8 extension
argument the gate requires and is tracked for that gate.

### F1 — the canonical-manifest seam disclosed the serializer (removed)

Four public items on facade-re-exported types carried `serde_json::Value`:

| Item | Direction |
| --- | --- |
| `ChunkComponentRevisions::manifest_value` | returned |
| `FlowTarget::manifest_value` | returned |
| `StartControls::manifest_value` | returned |
| `DefinitionIdentity::from_flow_manifest` | accepted |

**This was a real violation of an accepted rule.** The public-boundary rule
states that Serde types do not appear in core public signatures without an
accepted ADR, no such ADR exists, and the facade's own crate documentation
claims that codec signatures exchange JSON object bytes precisely to keep
serializer types out of the public contract. A compile-fail fixture has
rejected `oxide_batch::serde_json` since M2; the type reached the surface
through a signature instead of a re-export, which is why that fixture never saw
it.

**Cause.** Every one of the four was private before the staged crate
extraction. `ChunkComponentRevisions::manifest_value` was `pub(crate)`, and the
other three were module-private functions in the pre-extraction `plan.rs`.
Splitting core from plan turned an intra-crate call into a cross-crate one, and
Rust has no visibility between those two points, so the projection became
public along with the serializer type in its signature. The facade snapshot did
not catch it because the snapshot pins exported paths and their feature gate,
not item signatures, and these items are members of types that were already
exported.

**Remedy applied.** The canonical manifest is the plan crate's contract, so the
projection moved there and the core's contract now speaks durable values and
canonical bytes:

- `start_controls_manifest`, `flow_target_manifest`, and
  `chunk_declaration_manifest` are private functions in `oxide-batch-plan`,
  beside the `fault_manifest_value` projection that was already written that
  way. Nothing new is public for the first two: they read `start_limit`,
  `allow_start_if_complete`, `NodeId::as_str`, and `TerminalKind::as_str`,
  which were public already.
- `ChunkComponentRevisions` gains eight domain accessors — `reader`,
  `processor`, `writer`, `checkpoint`, `checkpoint_schema`,
  `checkpoint_schema_version`, `context_schema`, and `context_schema_version` —
  following the delegation `delivery_mode` and `in_flight_policy` already use.
  `ChunkDeliveryMode::manifest_name` becomes public for the same reason
  `TerminalKind::as_str` is: it is the durable name the mode is recorded under.
- `DefinitionIdentity::from_flow_manifest` takes canonical bytes instead of a
  parsed document. It validates the bound, that the bytes are a canonical JSON
  object, and that the declared format is a supported flow format, then hashes
  them. The caller's encoding responsibility is now explicit rather than
  implied, and the check that the bytes re-encode to themselves is one the
  value-taking form could not make.

Wrapping `serde_json::Value` in a new public type was rejected: the disclosure
would survive under another name the moment anything had to get the value back
out.

**Nothing durable moved.** The bytes are produced by the same
`serde_json::to_vec` call on the same composed document; it now runs in the
plan crate rather than one function later in the core. `format2_manifest_has_golden_bytes_and_fingerprint`,
`format1_and_format2_bytes_are_never_rewritten`,
`unchanged_definition_recompiles_to_the_same_fingerprint`, and the normalized
repository write traces all pass unchanged.

**The removal was proved to be the removal.** The hardened inspection was run
against the pre-remedy core and plan crates, with the accepted list already
empty, and reported exactly the same four occurrences. The clean result
therefore comes from the remedy and not from the narrowed scan.

**Public API impact.** Three public methods are removed, one changes its
parameter type, and nine are added. This is a pre-1.0 breaking change recorded
in the changelog. The facade snapshot is unchanged, because it pins exported
names and none was added or removed.

### F2 — the explorer port cannot gain a query without a break

`ExplorerRepository` declares 11 required methods and no provided one, so M8
cannot add a query without breaking every implementation. `JobRepository`
(1 required, 2 provided), `RepositoryUnitOfWork` (20 required, 22 provided),
### F2 — the explorer port cannot gain a query without a break

`ExplorerRepository` declares 11 required methods and no provided one, so M8
cannot add a query without breaking every implementation. `JobRepository`
(1 required, 2 provided), `RepositoryUnitOfWork` (20 required, 22 provided),
and `ChunkTransactionManager` (1 required, 2 provided) already use provided
methods for exactly this, and the capability model gives an unimplemented
operation a typed rejection rather than a compile error.

This does not block M8: the three implementations are all ours, no third-party
adapter exists, and pre-1.0 policy permits the break. It is recorded because
the asymmetry is unintended rather than designed, and M8 is the gate that
should decide whether the explorer port adopts the same default-and-reject
shape as the repository port. Tracked by issue
[#114](https://github.com/luceat-lux-vestra/oxide-batch/issues/114), which
carries the alternatives the decision should weigh.

## M6-M12 non-blocking argument

The gate requires this per target boundary, and treats a boundary the current
surface would block as a finding rather than a note. The structural facts
underneath the arguments below are that 104 of the 106 public enums are
`#[non_exhaustive]`, the manifest format is a bounded integer read fail-closed
rather than a closed type, and every capability is negotiated from a versioned
descriptor rather than inferred from a type.

**M6 — item and test-kit surface.** The three item traits each declare one
required method and no provided method, so the ADR-0008 item component contract
replaces rather than extends them. That break is already accepted by
[ADR-0008](../architecture/decisions/0008-item-component-contract.md), which
partially supersedes ADR-0002 for exactly these three traits, and pre-1.0
policy permits it in a minor release with a changelog entry. The user test kit
arrives as a new `oxide-batch-test` crate; the facade reserves no name for it
and blocks nothing. Not blocking, with an explicitly accepted break.

**M7 — flow, registry, and scope surface.** Nested flow, nested jobs, and
advanced transitions add node kinds, and `FlowNode` is `#[non_exhaustive]`, so
they extend the graph rather than replace it. `FlowTarget` is one of the two
closed enums, but it stays two-valued by construction: a nested flow compiles
to a node, so a transition still selects either a node or a terminal.
`CompiledExecutionPlan` owns its canonical manifest behind a bounded format
integer, and `DefinitionManifest::read_verified` already rejects a newer format
fail-closed, so a format 4 is additive. The definition registry is a service
the facade has not named. Not blocking.

**M8 — repository-portability surface.** `RepositoryCapability` is
`#[non_exhaustive]` and `RepositoryDescriptor` is versioned, so a new
capability is additive and an adapter that does not declare it is negotiated
away at launch rather than at the first write. `JobRepository::descriptor` and
`connection_capacity` are provided methods, so an adapter compiles without
knowing about either. No dialect, pool, or driver type is on the surface to
constrain a second adapter. F2 records the one port whose shape does not follow
this pattern. Not blocking.

**M9 — integration surface.** The facade names no message, envelope, offset,
acknowledgement, or broker type, and `ChunkDeliveryMode` is `#[non_exhaustive]`
so a broker-specific delivery mode is additive. The chunk contracts already
separate the reader position from the transaction boundary, which is the
distinction messaging adapters need. Not blocking.

**M10 and M11 — concurrency and distributed surfaces.** The framework creates
and retains no process-global runtime, and public async contracts use
`BoxFuture`, so a static hot path or a different executor changes no public
signature. `PartitionBudget` and `SplitBudget` are throughput bounds excluded
from the fingerprint by
[ADR-0009](../architecture/decisions/0009-definition-fingerprint-input-set.md),
so tuning them later is not a definition change. No remote envelope, worker
registration, lease, or fencing type is present, which is what
[RFC-0009](../rfcs/0009-transport-neutral-worker-protocol.md) needs to remain
free to specify. Not blocking.

**M12 — migration surface.** `DefinitionIdentity` separates the canonical
manifest, its digest, and its format, and retains a frozen legacy identity for
rows that predate manifest identity, so an imported definition is expressible
without changing what a native one means. `ParameterRole` is the second closed
enum; Spring Batch's identifying flag is boolean, so a third role is not a
migration requirement. F1's `from_flow_manifest` is on this path and its remedy
should land before M12 rather than after. Not blocking.

## Preview claim bounds

The preview claim is bounded by the accepted
[release, schema, and support policy](../release/support-policy.md) and adds
nothing to it. `oxide-batch` is a `0.x` production preview: an incompatible
change may occur in a minor release and must be called out in the changelog,
the preview creates no project-wide stability promise, and it does not shorten
the M14 gate. Release-channel names create no compatibility evidence, so no
statement in this record implies that any ledger row is `Verified`; promotion
requires a named released version with its evidence links, and that is issue
[#103](https://github.com/luceat-lux-vestra/oxide-batch/issues/103).

Two breaks are in scope of this policy. The F1 remedy is taken here, with its
changelog entry. The ADR-0008 item traits are taken in M6. Neither is a
deprecation-window obligation, because that obligation begins at 1.0.

## Deviations

The design gate named four scenarios without naming their runner. Two of them
cannot be ordinary tests, and both deviations are recorded here rather than
resolved silently.

`facade_exposes_no_runtime_database_or_telemetry_sdk_type` is the M1 scenario
`public_facade_does_not_reexport_executor_or_postgres_driver_types`, renamed to
the identifier the gate assigned now that a telemetry fixture joined the
executor, driver, and serializer ones. The [M1 exit evidence](m1-exit-evidence.md)
records the rename against its original row. A re-export that does not exist
can only be observed as a type error, so the scenario stays a `trybuild` set.

`rustdoc_surface_contains_no_leaked_implementation_type` is `cargo xtask
surface` rather than a `#[test]`, because it needs a complete documentation
build of the facade and its dependencies, which takes about three minutes. As a
test it would add that cost to every `cargo test --workspace` run and duplicate
it in CI, where it is now its own step next to the boundary check.

The scanner itself is unit-tested against both synthetic markup and real
rustdoc output. The synthetic cases hold the rules: attribution to the owning
member, the relative and absolute link forms, an unknown host being reported,
prose disclosing nothing, blanket-implementation exclusion, the standard
library and this crate not counting as foreign, an unlisted disclosure failing,
and a repaired exception failing as loudly as a new disclosure. The committed
`xtask/tests/fixtures/rustdoc` pages hold the rules against reality: they are
real output from the pinned toolchain for a crate that places a foreign type in
a public field, an argument, a return, an associated-type bound, and a bound on
a method's type parameter, and the test asserts each position is attributed to
the item that declares it. The same fixture links to `docs.rs` in its crate
prose and carries the ecosystem's blanket implementations, and the scan reports
neither.

## Validation

Run and passing at the commit this record describes:

```shell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo check -p oxide-batch --no-default-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
cargo xtask deps
cargo xtask surface
```

The facade snapshot is unchanged. The golden fingerprint vectors and the
normalized repository write traces are unchanged.

Four probes were run, and none left anything behind:

1. A public facade function returning `tokio::runtime::Handle` fails
   `cargo xtask surface`, attributed to the function that carried it. This is
   the probe that showed why the dependencies must be documented.
2. An accepted entry that no longer occurs fails as a stale exception, so a
   repaired disclosure cannot leave its permission behind.
3. An unknown absolute documentation host inside a declaration is reported
   rather than skipped, held by a unit test rather than a live probe because no
   such link exists in the rendered facade.
4. The inspection was run against the pre-remedy core and plan crates with an
   empty accepted list, and reported exactly the four F1 occurrences. This
   rules out the narrowed scan, rather than the remedy, being what produced the
   clean result.

**Not run locally:** the PostgreSQL suites, which require a database this
environment does not have. They run in CI. The credential classes in the
sensitive-payload sweep are feature-gated and were run locally under
`--all-features`; they construct configuration values and need no database.

## Boundaries held

- The public API changes only where F1 required it: three methods removed, one
  parameter type changed, nine methods added. The facade snapshot is
  byte-identical, because no exported name was added or removed.
- No observable batch semantics, persisted byte, transaction boundary,
  lifecycle write, restart selection, definition fingerprint, or normalized
  trace changes.
- No ledger row is promoted, and no statement here is a preview parity or
  readiness claim.
- No crate, feature flag, manifest field, schema table, CLI command, or
  extension point was added to reserve later scope. The one new command
  inspects the surface and changes nothing about it.
- The accepted-exception list is empty. No API-policy exception exists that an
  allowlist entry alone would be permitting.
