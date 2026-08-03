# Changelog

All notable changes to OxideBatch will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Facade-owned M1 domain types for job and step names, opaque execution
  identifiers, typed identifying parameters, canonical job-instance keys,
  lifecycle metadata, counters, exit statuses, and redacted failure summaries.
- Deterministic M1 integration-test support for clocks, IDs, randomness,
  backoff, bounded waits, event diagnostics, fixture provenance, redaction
  sentinels, conformance IDs, and reusable repository contracts.
- Deterministic job and step lifecycle transitions with typed optimistic-version
  conflicts, terminal-state enforcement, separate exit-status enrichment, and
  fresh execution attempts for restart.
- Async-first single-step tasklet execution with persisted lifecycle outcomes,
  cooperative stopping, panic classification, and an explicitly bounded
  blocking adapter.
- Deterministic job and step listeners, commit-aligned lifecycle events,
  execution-attempt correlation, and value-redacted log, span, metric-label,
  and listener-failure diagnostics.
- A runnable first in-memory job, facade-boundary compile-fail tests, and M1
  executable-kernel conformance and exit evidence.
- An active M2 kickoff gate with dependency-ordered workstreams, PostgreSQL
  15–18 verification targets, and durable chunk/restart exit criteria.
- Accepted M2 definition-compatibility, physical metadata, pool/timeout, TLS,
  role, and migration-operation contracts with an executable PostgreSQL 15–18
  design-gate fixture.
- Runtime-neutral item reader, processor, writer, chunk-completion, enlisted
  business-transaction, checked chunk-count, and bounded versioned durable-state
  contracts for M2.
- An optional PostgreSQL metadata repository with facade-owned redacting
  configuration, validated Rustls transport, bounded pool/timeouts, immutable
  schema-v1 migrations, canonical instance-key hashing, database-authoritative
  launch serialization, typed optimistic conflicts, and unknown-commit
  classification.
- Deterministic chunk orchestration and PostgreSQL same-resource chunk
  transactions that atomically commit business writes, checkpoints, contexts,
  counters, and optimistic versions.
- Canonical definition manifests and SHA-256 identity, typed definition-drift
  and compatibility rejection, explicit directed step mappings, committed
  checkpoint inheritance into distinct restart attempts, and versioned
  append-only PostgreSQL recovery decisions.
- Separate-process PostgreSQL pre/post-commit crash injection, durable
  restart/conformance evidence, and executable setup, transaction-guarantee,
  crash/recovery, backup, and migration operations documentation for the M2
  exit gate.
- An active M3 kickoff gate with explicit fault-policy, persistence, listener,
  compiled-plan, flow-decision, migration, resource, and evidence boundaries
  plus dependency-ordered delivery workstreams.
- An active M4 kickoff gate with explicit operator/explorer, CLI,
  shutdown/recovery, telemetry, retention, configuration, bounded local-scale,
  migration, security, resource, and evidence boundaries plus
  dependency-ordered delivery workstreams.
- Accepted M4 contracts for the bounded keyset-paginated `JobExplorer`, the
  idempotent guarded `JobOperator` with separately authorizable read,
  lifecycle, and destructive classes, and the initial hold plus two-phase
  purge retention slice.
- Accepted M4 contracts for the minimal `oxide-batch` operator CLI, per-value
  configuration precedence, versioned JSON output, stable exit categories,
  confirmation and non-interactive safeguards, and secret handling.
- Accepted M4 contracts for graceful shutdown ordering, in-flight chunk
  policy, deadlines and drain reporting, owner-token and server-time stale
  evidence, evidence-bound recovery, and the process-signal/kill matrix.
- Application-owned graceful shutdown with intake rejection, cooperative
  cancellation, structured child joining, escalation and deadline reports,
  separate telemetry flushing, and ordered repository close; definition-bound
  `FinishChunk`/`RollbackChunk` behavior; durable execution ownership and stop
  polling; and server-time, version/digest-bound stale recovery exposed by the
  operator CLI without payload-bearing evidence.
- Accepted M4 contracts for the bounded split and local partition subset,
  single-invocation partitioning, durable partition state, deterministic
  aggregation, finite resource budgets, manifest format 3, and
  sequential-fallback equivalence.
- Canonical manifest format 3 declarations for the exact M4 split, structural
  join, and local partitioned-step subset, with globally unique embedded child
  identities, typed finite concurrency/partition/pool budgets, stable golden
  fingerprints, and fail-closed format-3 runtime gating until durable local
  execution is implemented.
- Portable durable partition repository contracts with bounded byte-exact
  keys and contexts, atomic plan-before-worker persistence, deterministic
  key-ordered reads, unique local worker assignment, terminal-result
  compare-and-swap, restart-safe completed rows, checksum validation, and
  matching in-memory/PostgreSQL adapters.
- Deterministic partition aggregation with fixed status severity, key-ordered
  exit selection, checked counter sums, incomplete-child rejection, and an
  in-memory/PostgreSQL transaction that publishes the aggregate only with the
  parent step's terminal lifecycle update.
- Tasklet-only bounded parallel-split execution with launch-scoped component
  factories, finite owned child polling, cooperative sibling cancellation or
  draining, declared-order status/exit aggregation, durable
  `SPLIT_AGGREGATE` join decisions, `UNKNOWN` propagation, and completed-child
  reuse on restart.
- Bounded parallel-split evidence for both sibling failure policies, branch
  panic conversion, the branch-concurrency and repository-connection ceilings,
  parent stop, completion-order and sequential-fallback durable equivalence,
  repeated task-scope draining, and PostgreSQL 15/18 process-kill reuse of a
  branch committed before its join decision.
- Executable M4 bounded-resource measurements covering local partition scaling
  at 1, 10, and 64 workers, explorer pagination bounds over growing history,
  bounded retention batches beside interleaved launches, stop and drain latency
  by phase, telemetry export overhead with counted queue drops, and a
  launch/fail/restart/drain soak. Each measurement asserts ceilings, ordering,
  and durable equivalence rather than a duration threshold, and retains raw
  machine-readable results with their environment.
- Operational capacity and resource-budget guidance covering the declared M4
  bounds, the derived connection-pool and memory formulas, the provisional
  measured budgets, and the limitations of the in-memory fixture.
- The M4 exit record closing operations and local scale with its
  exit-criterion map, measured evidence, reviewed dispositions, and named
  residual PostgreSQL history, retention-grant, and M10/M11 scope.
- An active M5 kickoff gate with explicit compiled-plan/fingerprint,
  static-versus-erased component, staged crate-extraction, context-codec,
  transaction-capability, facade/API, ledger-promotion, preview-support, and
  evidence-campaign boundaries plus dependency-ordered delivery workstreams.
- Closed M5 design gates: the stabilized manifest, fingerprint-input, and
  fail-closed drift boundary; a staged crate-extraction contract with
  forbidden-dependency, facade-equivalence, durable-invariance, packaging, and
  reversal rules; the context-codec and transaction-capability direction with
  its fingerprint participation rule; the preview facade disclosure classes and
  M6-M12 non-blocking review requirement; the reviewed ledger disposition with
  its advertised embedded-kernel promotion set; the `0.x` preview support,
  upgrade, and rollback bounds; and the nine named evidence campaigns.
- The RFC-0005 static-versus-erased item hot path spike, supplying the
  measurement half of that RFC's approval gate ahead of M6 kickoff. It settles
  a contract shape as well: one public trait per role with an explicit call
  lifetime, implemented as a plain `async fn`, with erasure delivered as a
  concrete handle over a sealed dyn-compatible mirror rather than a second
  public trait, so one chunk loop serves both dispatch forms. Evidence covers
  trace, counter, outcome, fold, and panic equivalence across thirteen
  scenarios; a counting allocator showing zero allocations per item against
  exactly one boxed future per dispatched call; preserved enlisted-transaction
  borrowing on both forms; a retained path back to the ADR-0002 handles; a
  runtime-free throughput harness; and one- versus sixteen-pipeline code-size
  and compile-time comparisons. The ergonomics review is included: M6's
  decorator and composite shapes are built against the contract and still
  measure zero allocations per item, the extra bounds a generic composite needs
  are established by removal, and each contract trait carries
  `#[diagnostic::on_unimplemented]` with the implementer-facing wording pinned
  by compiler fixtures.
- Accepted RFC-0005 on 2026-08-03, on the evidence of spike 0004, closing the
  static-versus-erased architecture gate that M5 had deferred. M5 is unaffected
  and still exits on the ADR-0002 boxed boundary; the contract lands in M6.
- ADR-0008 replacing the public item component contract: one generic
  trait per role with an explicit call lifetime, implemented as `async fn`,
  with erasure delivered as a concrete handle over a sealed dyn-compatible
  mirror and one chunk loop serving both dispatch forms. It supersedes ADR-0002
  **in part** — the three item component traits only, with the execution model
  and the other twenty-one boxed extension points unchanged — and carries
  forward async-first execution, cooperative stop, the bounded blocking
  adapter, panic classification, and the borrowed enlisted transaction. Item
  listeners are explicitly out of scope and keep their per-item boxed future;
  the zero-allocation result is stated for pipelines without them.
- Tasklet-only bounded local partition execution with per-child factories,
  manager-owned finite worker scopes, pre-start and in-flight cancellation,
  panic isolation, durable completed-child restart reuse, explicit `UNKNOWN`
  blocking and inspection, manifest-to-repository pool validation, and
  PostgreSQL 15/18 process-kill recovery evidence.
- Versioned M4 telemetry: schema version 1, the operations, shutdown,
  recovery, retention, and local-scale event catalog, an enforced label
  cardinality budget, bounded exporter queues with counted drops, and the
  bounded redacted diagnostic bundle.
- Executable M4 telemetry with commit/read/evidence-relative events, typed
  metric names/units/labels, a fixed span hierarchy and reviewed safe fields,
  a per-family `200`-series budget, explicit name
  allowlists, an application-owned drop-newest exporter queue, panic-isolated
  export, bounded incident retention, and a staged non-overwriting `4 MiB`
  `diagnostics bundle` directory with checksums and recorded omissions.
- The accepted unreleased schema-3 design adding execution ownership and stop
  evidence, one instance hold, `ob_operator_request`, `ob_retention_action`,
  and `ob_step_partition`, with a backfill-free transactional upgrade and a
  restore-only rollback boundary.
- A portable bounded `JobExplorer` over a new `ExplorerRepository` port: the
  closed M4 query set, keyset-only pagination with a captured identity ceiling,
  an opaque checksummed cursor that separates a damaged token from one reused
  against another query, filter, or page size, redacted projections carrying
  names, opaque identifiers, ordinals, counters, versions, timestamps, digests,
  parameter descriptors, and durable-state envelope descriptions, and enforced
  page, response, and age bounds.
- A portable guarded `JobOperator` application service for launch, restart,
  stop, abandon, and recover. Every mutating action carries a validated
  `ActorRef`, `ReasonCode`, and `OperationId` envelope with a framework request
  digest, commits its append-only audit row in the transaction of its effect,
  replays by operation identifier, rejects a reused identifier that carries a
  different canonical request, reports optimistic conflicts and guard failures
  as audited rejections without an effect, and never guesses an ambiguous
  commit. An audit append that collides with a concurrently recorded operation
  identifier rolls its own effect back and returns the recorded outcome instead
  of surfacing the collision, and every abandoned unit of work is rolled back
  explicitly rather than dropped.
- A `RecoveryDirective` that pairs a recovery disposition with the evidence
  that disposition requires, so a `MarkFailed` decision without its stated
  failure is unrepresentable rather than a deferred validation error and an
  abandoning decision carries no failure for its request digest to cover.
- A portable `RetentionService` with instance holds and a bounded two-phase
  purge: eligibility that never targets a running, stopping, ambiguous, or held
  instance, a plan digest that rejects a stale apply without deleting, deletion
  in instance-owned order inside one transaction per batch, audited per-table
  counts, and safe re-planning after an interrupted run.
- PostgreSQL schema version 3 with immutable migration
  `0003_operations_and_local_scale.sql` and its bounded explorer, operator, and
  retention adapters. The schema-3 least-privilege grants required by purge are
  specified by the migration guide and remain unimplemented fixture work.
- An optional `oxide-batch-cli` crate shipping the minimal guarded `oxide-batch`
  operator binary and the embeddable library behind it. The closed
  noun/verb grammar is parsed without an argument-parsing dependency, so every
  unknown option, unknown subcommand, and inapplicable option fails rather than
  being ignored. Configuration resolves per value across option, namespaced
  environment variable, configuration file, and documented default, rejects
  unknown keys and out-of-bounds values in one pass before any connection is
  opened, reads secrets only by environment or `__FILE` indirection, and
  refuses a group- or world-readable configuration file. Output is a versioned
  `256 KiB`-bounded JSON envelope or an unversioned human form rendered from
  the same redacted projection, and both exclude credentials, endpoints,
  parameters, contexts, checkpoints, SQL, and user error text. Twelve stable
  exit categories, destructive-action confirmation with a non-interactive
  `--yes` requirement, mandatory operation identifiers for automated mutations,
  a plan-digest guard on purge application, dry runs that change nothing, and
  broken-output handling that repeats no mutation are each covered by a named
  `OPS-CLI-001` scenario.
- `PostgresMigrator::installed_schema_version`, a read-only counterpart to
  `migrate` that takes no advisory lock, applies no migration, and reports an
  uninitialized schema as an answer rather than a failure, so an unprivileged
  operator identity can report migration state.
- A shared in-memory and PostgreSQL service contract suite covering the named
  `REPO-EXPLORE-001`, `REPO-OPERATOR-001`, `REPO-RETENTION-001`, and M4
  `LIFE-ABANDON-001` scenarios, plus `JobInstanceKey::digest`, opaque
  recovery-decision, operator-request, retention-action, and step-partition
  identifiers, and `BatchStatus::as_str`/`ParameterValueKind::as_str` durable
  codes.
- Accepted M3 contracts for typed bounded retry/skip/rollback, deterministic
  backoff, item/retry/skip listeners, acyclic conditional flow, durable
  deciders, start controls, manifest format 2, and the schema-2
  migration/restore boundary.
- Runtime-neutral M3 fault-tolerance values: bounded retry, retry-state, and
  aggregate skip limits, capped deterministic backoff with an injected
  cancellable sleeper, order-independent phase/category classification over
  extended stable failure categories, capability-scoped rollback dispositions,
  and ordered item/retry/skip listener families with panic-safe aggregation.
- Deterministic fault-tolerant chunk execution: a retryable fault rolls the
  attempt back, reserves its ordinal through a bounded `FaultStateStore`, runs
  the retry scope, waits the injected cancellable backoff, and replays the chunk
  from its buffered inputs without re-reading committed input. Read, process,
  and write skips are classified from framework evidence, counted per phase, and
  become authoritative only in the commit that accepts them; a commit-safe skip
  requires the declared delivery mode and an enlisted transaction. Item, retry,
  and skip callbacks run at their contracted boundaries, and the chunk report
  exposes per-phase retry and skip counts, rollback and no-rollback counts, and
  redacted listener failures.
- Post-decision `retry.reserved`, `retry.backoff_started`,
  `retry.backoff_cancelled`, `retry.exhausted`, `item.skipped`,
  `fault.rollback_committed`, and `fault.no_rollback_committed` lifecycle events
  carrying only fault phase, retry ordinal, backoff duration, stable category,
  and the existing opaque correlation.
- Durable PostgreSQL fault-tolerance state in immutable schema version 2:
  per-phase retry and skip counters, a no-rollback count, a bounded checksummed
  fault-state envelope holding at most 256 digest-sorted retry entries, backfilled
  step logical IDs, the extended failure-category constraint, and the
  append-only `ob_flow_decision` table used by the finite-flow runtime.
- `PostgresFaultState`, a durable retry-reservation store whose compare-and-swap
  runs as one short metadata transaction after a known rollback and before
  backoff. It advances the phase retry count, the acknowledged rollback count,
  and the retained envelope under an optimistic version check, so a stale or
  concurrent writer loses instead of spending one ordinal twice, and a restart
  resumes the persisted ordinal instead of refilling the retry budget.
- Enlisted commit of the skips one chunk accepted: the per-phase deltas, the
  no-rollback delta, and the cleared fault state of the superseded checkpoint
  generation now commit or roll back with the business writes, checkpoint,
  context, counters, and optimistic version.
- Restart inheritance of committed fault-tolerance totals and retained retry
  state, so the shared skip limit and every retry budget span all attempts of one
  job instance.
- A schema-1 to schema-2 upgrade fixture with realistic completed,
  failed-with-active-restart, stopped, and unresolved `UNKNOWN` source history,
  byte-for-byte logical-ID backfill verification, published empty-state checksum
  verification, constraint and index verification, fail-closed bounds probes, and
  a reapplication guard.
- Immutable bounded M3 flow graphs and compiled execution plans with stable node
  and decider revisions, deterministic exit-pattern selection, start controls,
  structural validation, canonical manifest format 2, golden fingerprints, and
  fail-closed format-1/format-2 manifest reading.
- Compatibility lowering for existing one-step tasklet and chunk jobs that keeps
  their format-1 manifest bytes and fingerprint unchanged while routing terminal
  outcomes through the compiled plan, with eleven golden lifecycle, listener,
  repository-write, stop, panic, restart, and unknown-commit equivalence traces.
- Durable execution of the finite M3 flow slice with sequential and conditional
  tasklet/chunk steps, custom exit outcomes, typed panic-safe deciders,
  `Complete`/`Fail`/`Stop` terminals, instance-wide atomic start limits, and
  `allow_start_if_complete`. In-memory and PostgreSQL repositories now append
  manifest-validated transition decisions before target start, reconstruct
  logical-step history across attempts, and reuse matching completed-step and
  decider decisions during restart.
- Post-commit finite-flow events for committed step results and decisions,
  completed-step reuse, and start-limit rejection through a value-redacted,
  panic-isolated, non-authoritative `FlowEventSink`.
- Separate-process PostgreSQL retry-reservation, skip-callback, and flow-decision
  crash/restart matrices. Each inspects durable state through a fresh
  connection, applies audited recovery, and proves the accepted replay or reuse
  boundary in a distinct attempt.
- M3 fault-tolerance and finite-flow exit evidence mapping the implemented
  ledger slice, process-kill boundaries, PostgreSQL axes, and residual M6/M7
  population without promoting unreleased rows to `Verified`.
- M5 plan and definition-fingerprint stabilization: the canonical restart-relevant
  input set is fixed by ADR-0009 and enforced by an executable manifest member
  allowlist, and the design gate's named scenarios are executable — unchanged
  recompilation, restart-relevant change, excluded storage and runtime values,
  throughput-only budget invariance, drift and mismatch rejected before any
  lifecycle write, newer-format rejection, and untouched format-1 and format-2
  bytes.
- M0 implementation-readiness plan, M0–M5 roadmap, decision records, and
  product, compatibility, architecture, engineering, security, operations, and
  release policy set.
- Dedicated MSRV and supply-chain CI checks.
- Repository `cargo xtask` commands for development checks and package
  verification.
- Changed-file pull-request labeling, bounded non-authoritative AI review,
  CodeQL workflow scanning, and owned scheduled supply-chain failure reporting.
- Protected-tag draft Release preparation with locked package verification,
  CycloneDX SBOM, SHA-256 checksums, and package provenance/SBOM attestations.
- The `oxide-batch-core` implementation crate, holding domain identities, typed
  parameters, statuses, execution records, lifecycle rules, bounded versioned
  execution-context and checkpoint state, chunk sizing values, and
  restart-relevant definition identity. It is implementation detail with no
  stability promise; `oxide-batch` remains the only supported entry point and
  re-exports every supported item under its existing path.
- Staged crate-extraction evidence checks: `cargo xtask deps` fails the build on
  a forbidden dependency class or a workspace cycle, `cargo xtask package` runs
  the workspace publish dry run for every publishable crate, and a facade
  surface test holds all 424 exported paths against a committed snapshot while
  proving each path still resolves.

### Changed

- Extracted implementation crates are published in lockstep with the facade
  rather than kept `publish = false`. Cargo rewrites a published archive's path
  dependencies to registry dependencies, so a publishable facade cannot depend
  on an unpublished crate; the first extraction stage would have made
  `oxide-batch` unpublishable. RFC-0011 records the conflict and ADR-0010
  supersedes ADR-0001 for the three M5-authorized crates only. The release
  workflows now package, checksum, generate SBOMs for, attest, and publish
  those crates in dependency order.
- Crate-extraction stages 2 and 3 are on hold behind proposed ADR-0011. The
  repository port names `NodeId` and `StartLimit` in its signatures while the
  accepted contract forbids `oxide-batch-repository` from depending on
  `oxide-batch-plan`, so the named boundary content and the code disagree. The
  ADR proposes keeping the order and the accepted inward dependency rule, and
  placing each type in the lowest crate that every crate needing it can depend
  on.
- `MAX_NODES` and `MAX_TRANSITIONS` moved with the manifest reader that
  enforces them. Both facade paths resolve unchanged, and neither bound
  participates in a definition fingerprint.
- Facade code that matches `#[non_exhaustive]` domain enums now takes an
  explicit conservative arm, because exhaustive matching is not available
  outside the crate that declares an enum. An unknown status reports an unknown
  span outcome and the highest split severity, an unknown in-flight policy
  never masks a shutdown request, and an unknown parameter kind is rejected
  rather than written under a guessed durable tag.
- RFC-0005 stays `Proposed` through M5 by a recorded continued-deferral
  decision: its own approval gate requires a spike that has not run, and
  changing the item hot path underneath the M5 fingerprint and extraction work
  would invalidate their equivalence evidence. M5 keeps the ADR-0002 boxed
  boundary, the roadmap dependency is satisfied by the recorded decision, and
  the approval spike and P-002 measurement move to M6.
- The M5 preview support bounds replace the earlier dimension sketch with
  decided supported combinations; Linux aarch64, macOS, and Windows are named
  as outside the preview promise rather than as candidates within it.
- The PostgreSQL runtime now requires metadata schema version 3 and rejects
  version 4 or higher. Schema 2 to 3 is a quiesced, backfill-free, transactional
  upgrade; a schema-2 runtime sees schema 3 as newer and performs no work.
- The canonical instance-key digest moved from the PostgreSQL adapter to
  `JobInstanceKey::digest`, with a byte-identical version-1 encoding, so the
  durable adapter, redacted projections, and operator request digests share one
  identity.
- `RecoveryDecision` carries an opaque `RecoveryDecisionId`, because the bounded
  explorer orders decisions by an immutable identity column.
- The accepted M4 `launch` guard no longer rejects a held instance: a hold
  protects history from purge and never blocks a lifecycle action, which the
  same contract's retention section already required.
- The accepted `ob_operator_request` job-execution reference is now optional
  alongside its job-instance reference, because a launch rejected before its
  instance exists must still be audited without an effect.
- The accepted cursor encoding separates its integrity checksum from an 8-byte
  query binding, because one checksum over both cannot distinguish
  `CursorInvalid` from `CursorQueryMismatch`.
- `ChunkTransaction::commit` takes the `ChunkFaultProgress` one chunk accepted,
  and `ChunkTransactionManager` gains `inherited_progress` with a
  no-inheritance default. `FaultStateStore` gains `bind`, with a process-local
  default, because a durable store cannot know its step execution until the
  attempt starts.
- `ChunkExecutionReport` retry, skip, and no-rollback counts are cumulative
  durable totals rather than per-attempt counts, because the aggregate skip
  limit is defined across every attempt of one job instance.
- The PostgreSQL runtime required metadata schema version 2 and rejected
  version 3 or higher without guessing compatibility. Downgrade from a released
  schema version is restore-only.
- `ChunkStep::execute` takes the `ExecutionCorrelation` for the run, because
  item, retry, and skip callbacks receive it. The repository-backed
  `JobLauncher::launch_chunk` path is unchanged.
- `ReaderError`, `ProcessorError`, `WriterError`, and `ChunkCompletionError`
  declare a stable `FailureCategory` at the adapter boundary and still drop the
  payload, display text, and source chain. `ReaderError` can prove forward
  checkpoint progress and `WriterError` can locate one known-rolled-back output,
  which the read and write skip contracts require.

### Fixed

- The definition fingerprint no longer depends on the framework build or on
  resource tuning. Manifest formats 2 and 3 hashed the framework's own capacity
  constants, so raising one in a later release would have turned every persisted
  definition into fail-closed drift, and format 3 hashed the split and partition
  concurrency and connection budgets, so retuning a pool after a crash blocked
  restart. Neither class selects or reinterprets durable state, and both left the
  canonical projection under ADR-0009. Format numbers, encoding rules, readers,
  and format-1 bytes are unchanged; the format-2 and format-3 golden vectors are
  re-pinned once, which migrates nothing because no released version emitted the
  prior bytes. A pre-release definition compiled by an earlier build is rejected
  as drift until it is recompiled or given a directed edge.
- PostgreSQL design-gate readiness now probes the final TCP listener instead of
  mistaking the official image's socket-only initialization server for a ready
  database.
- A terminal known chunk rollback now increments `rollback_count` in the same
  repository transaction that commits the failed step lifecycle, including a
  chunk executed through the finite-flow launcher.
- The `SCALE-PARSTEP-001` and `SCALE-LOCALPART-001` ledger rows carried twelve
  cells against a thirteen-column header, so their notes occupied the canonical
  owner column and neither row had a reviewable owner. Both now record their
  owner and notes in the correct columns.

## [0.1.0-alpha.1] - 2026-07-29

### Added

- Initial project governance, repository policy, and CI foundation.
- Public `oxide-batch` facade crate metadata and pre-alpha documentation.

[Unreleased]: https://github.com/luceat-lux-vestra/oxide-batch/compare/v0.1.0-alpha.1...HEAD
[0.1.0-alpha.1]: https://github.com/luceat-lux-vestra/oxide-batch/releases/tag/v0.1.0-alpha.1
