# Conformance Strategy

**State:** Accepted

Conformance evidence supports specific compatibility claims. It does not
reproduce Spring Batch internals or make documentation self-verifying.

## Sources

Use, in priority order:

1. official documentation for the pinned Spring Batch release;
2. public API documentation, schema documentation, release notes, and
   deprecation notices;
3. black-box behavior observed from the pinned public release;
4. independently authored minimal fixtures.

Record exact source links and versions. Do not copy Spring source, tests,
documentation prose, schemas, or fixtures unless license and attribution are
explicitly reviewed.

## Traceability from the ledger

Every executable scenario has:

- a stable ID matching one ledger row;
- source version and official references;
- plain-language preconditions and expected observations;
- the parity dimension and allowed differences;
- synthetic inputs, deterministic clock/IDs/randomness;
- launch, failure, stop, restart, concurrency, and upgrade phases as
  applicable;
- expected statuses, exit results, counts, contexts, durable relationships,
  external effects, telemetry, and operator decisions;
- a machine-readable normalized result and evidence version.

Generation from ledger rows is preferred. CI must fail when a row claims
`Verified` without all evidence links required by its profile.

## Reference and differential runner

A clean-room Java harness may run independently authored jobs against the
pinned Spring Batch release. It:

- lives outside published Rust crates and uses a separate database/schema;
- emits normalized observations rather than copied internal objects;
- pins Java, build tool, database, and dependencies;
- records dependency licenses and fixture provenance;
- cannot become a required consumer dependency.

The OxideBatch runner executes the corresponding native definition.
Differential comparison ignores IDs, timestamps, physical schema, APIs, and
formatting unless the row claims them. Every ignored field is declared; the
normalizer cannot erase a meaningful divergence.

When direct differential execution is legally or technically impractical, use
black-box fixtures derived from public behavior and record the limitation.

## Evidence matrix

Evidence is selected per ledger row:

- unit and property tests for values, policies, identity, and state machines;
- compile-fail/type tests for public component and dependency guarantees;
- shared adapter contracts for repositories, components, and transports;
- real integration tests for databases, brokers, filesystems, and object
  stores;
- black-box conformance/differential scenarios;
- crash/failure injection at every durable boundary;
- schema, context, definition, protocol, export/import, and round-trip
  migration tests;
- benchmark, soak, cancellation-latency, and resource-ceiling evidence.

Documentation, API similarity, a passing happy path, or one adapter test cannot
alone make a row `Verified`.

## Crash and restart matrix

At minimum, item steps inject before/after read, process, write, business
commit, commit acknowledgement, metadata checkpoint, and listener callbacks.
Distributed steps add assignment, acknowledgement, worker result, fencing,
lease expiry, coordinator update, and reassignment.

Every point records expected business effects, checkpoint, context, counters,
status, replay, idempotency/deduplication state, telemetry duplication, and
operator action. Unknown commit outcomes are tested rather than normalized
away.

## Repository adapter contracts

Every repository adapter runs common logical cases for identity, duplicate
launch, lifecycle compare-and-swap, checkpoint atomicity, queries, retention,
definition identity, recovery, and migration. Adapter-specific suites add
isolation, locking, query plans, TLS/authentication, time precision, leases,
fencing, backup/restore, and unknown commit behavior.

Passing one adapter cannot certify another. Certification names product
versions and capability descriptors.

## Messaging and distributed fixtures

Messaging fixtures cover acknowledgement/offset timing, duplicate delivery,
redelivery, rebalance, poison messages, dead-letter paths, outbox/inbox,
idempotency, and backpressure for each declared delivery mode.

Distributed protocol fixtures cover duplicate/delayed/reordered messages,
worker/coordinator crash, lost acknowledgement, stale fencing, network
partition, artifact mismatch, split brain, and N/N-1 protocol rolling upgrade.
The same plan must produce equivalent normalized lifecycle/restart traces in
embedded, local, and distributed modes.

## Migration evidence

Metadata and definition migration tests pin source/target versions and cover
dry-run, corrupted/oversized packages, unsupported constructs, context codec
mapping, fingerprint lineage, repeated import, partial failure, reconciliation,
backup/restore, and documented rollback. A migration claim requires a complete
source-to-target path, not merely a parser.

## Evidence versioning and release use

Evidence artifacts record:

- ledger schema/population revision;
- Spring Batch, OxideBatch, adapter, schema, protocol, and tool versions;
- source commit, fixture version/seed, environment, and normalized result
  schema;
- immutable CI/report link and checksum where retained externally.

Changing the normalizer or expected observations invalidates prior evidence
unless a reviewed migration proves equivalence.

A release cannot claim a compatibility level above its released `Verified`
rows. A regression of a verified observation blocks the claim and may block
the release.

## Fixture safety

Fixtures are synthetic, minimal, bounded, reproducible, and free of production
data or credentials. Binary fixtures include provenance, format version, and
regeneration command. Golden outputs are reviewed for semantic observations,
not incidental formatting.
