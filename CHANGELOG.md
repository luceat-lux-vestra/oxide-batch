# Changelog

All notable changes to OxideBatch will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `cargo xtask soak`, the M5 PostgreSQL soak campaign. It runs a declared window
  of repeated launch, fault, restart, recovery, and drain cycles against one
  PostgreSQL pool and reports task, connection, handle, and memory growth over
  it — the P-015 obligation the performance plan states and the
  `soak_reports_no_task_connection_handle_or_memory_growth` scenario the design
  gate names. The M4 in-memory measurement it builds on is retained unchanged
  and is not cited as a PostgreSQL result; a test asserts that it still runs on
  the in-memory repository, because relabelling it would be the cheapest way to
  appear to deliver this. The campaign's denominator is a *period*, which is the
  failure mode it is shaped around: a soak that ran three cycles and one that ran
  three hundred produce reports of identical shape, and the shorter one produces
  the flatter series and the more convincing result. So the window — 32 warmup
  and 600 measured cycles, 16 partitions against a worker budget of 4 through a
  pool of exactly 5 — is committed as `tests/fixtures/soak/campaign-scope.json`
  and read by all three consumers: the report takes its cycle counts and rules
  from it, the runner requires the run to have matched it, and an ordinary test
  reconciles it against the accepted plan and design gate. Correctness comes
  before any resource number: fifteen durable obligations are decided every
  cycle against the first measured cycle, because flatness over a workload that
  stopped working is not a result. The fault is injected by a worker that waits
  for its siblings rather than firing on a timer, since a cooperative sibling
  stop is consulted only before a tasklet is invoked and a timed fault would give
  every cycle a different durable record. Eight growth rules are declared before
  the run, decided from the measured samples alone, and each verdict carries the
  series it was decided from, so no trajectory is passed by eye. No memory budget
  was invented. Tasks, connections, and handles are exact integers required to be
  flat at every cycle boundary, and the accumulation claim rests on those;
  resident memory is held only to convergence, by comparing the growth rate of
  the measured window's last third against its first, which fails a leak of any
  per-cycle size and says nothing about how much memory the framework may use. CI runs it on PostgreSQL 15 and 18 and
  retains each report on failure as well as success, because a failed soak's
  value is its trajectory. No production code changed.
- `cargo xtask resource-bounds`, the M5 resource-bound campaign. It proves that
  every queue, retry cache, page, buffer, worker assignment, and result set the
  framework owns has a finite ceiling, that the ceiling is enforced under the
  overload policy the resource contracts for, and that resource pressure changes
  no durable observation. The denominator is committed as
  `tests/fixtures/resource-bounds/campaign-scope.json` and reconciled in both
  directions by ordinary tests: from the code outward, every library crate is
  parsed and each constant declared under the repository's new bound declaration
  convention must be classified as a proved resource or an argued exclusion, so
  a bound written under that convention cannot ship without entering the
  campaign; from the operator's document inward, the capacity
  budget table and the scope must agree, and the scope's numbers must be the
  numbers the code holds. Four overload policies are kept distinct rather than
  collapsed — fail-closed, bounded concurrency, bounded shedding, and bounded
  truncation — because telemetry may not block batch work, and each queue
  records which shedding rule it contracts for. The campaign's own claim is that
  a ceiling was *reached*, not merely respected: a worker budget of 64 whose
  observed peak was 3 is evidence about a workload, so every report records what
  it offered beside what the framework held and the runner requires the two to
  be in the relation the policy implies. 128 partitions are offered against a
  64-worker budget through a pool of exactly 65 connections, a pool one
  connection short is refused before any row is written, and the stressed run is
  compared field by field against the same work run one child at a time. CI runs
  it on PostgreSQL 15 and 18 and retains each report. No production code changed.
- `cargo xtask upgrade`, the M5 PostgreSQL upgrade campaign. It proves the
  preview's upgrade contract: a schema-1 or schema-2 database upgrades directly
  to schema 3, a runtime that supports schema 2 refuses one that has been, and
  an upgrade is rolled back by restoring the backup taken before it. The prior
  schemas are not reconstructed — each is installed by running this crate's
  immutable migration set up to that version and stopping, and every report
  refuses to proceed against a fixture carrying a table or column a later schema
  introduced. Durable state is compared through the column list the source
  schema declared, so a column added later cannot mask a lost value, and the
  upgraded database is then opened through the repository and projected through
  the explorer. The rejecting runtime is built from the last revision before
  schema 3 was added, because no build of this tree can report a supported
  schema version of `2`; both its repository and its migrator must refuse with
  the typed newer-schema failure and write nothing. The rollback takes a real
  `pg_dump` archive before the upgrade and loads it with `pg_restore` into a
  separate database, which must come up at the prior schema with the state the
  backup was taken from. The runner resolves its fixtures before starting and
  requires every declared schema path to appear in a retained observation, so a
  report that covered one source schema and skipped the other fails rather than
  passing half proved. CI runs it on PostgreSQL 15 and 18 and retains each
  report. No production code changed.
- `cargo xtask crash-restore`, the M5 crash and restore campaign. It kills a
  live process with `SIGKILL` at every phase of the chunk commit protocol,
  reports P-013 restart after many chunks, and takes a real `pg_dump` archive
  that it restores into a separate database and restarts the job on. Two of the
  five commit phases are inside the adapter, where no application hook exists;
  they are reached without changing the adapter by holding the lock the commit
  is about to need, so the progress write blocks before it can commit and
  `COMMIT` blocks while the server finishes it after the process is gone. Every
  report requires the restarted run to be indistinguishable from an
  uninterrupted one, compared as the committed position, the durable counters,
  the exact set of enlisted rows, and the terminal statuses. The runner
  resolves its fixtures before starting, requires each report to retain an
  observation into a directory it creates empty, and fails on a phase that did
  not end in `SIGKILL`. It also runs the eleven M2-M4 crash scenarios it
  reuses, rather than citing them. CI runs it on PostgreSQL 15 and 18 and
  retains each report.
- `cargo xtask conformance`, the M5 conformance campaign. It runs the whole
  workspace suite one target at a time, attributes every result to the target
  that produced it, and requires each of the 42 accepted M0-M4 ledger rows to
  be proved by a scenario that ran and reported `ok`. The row-to-scenario
  assignment is committed as `tests/fixtures/conformance/accepted-scope.json`
  and reconciled against the ledger by ordinary tests, so a status change or a
  renamed scenario fails in review. The runner resolves its database fixtures
  before running anything and refuses to start without them, because a
  PostgreSQL scenario skips silently and would otherwise report a campaign pass
  on a host with no database. CI runs it on PostgreSQL 15 and 18 and retains
  each report.
- `cargo xtask surface`, the facade disclosure inspection the M5 preview
  surface gate requires. It renders the facade with every feature and every
  dependency documented, then reports any foreign crate a rendered declaration
  links to — argument and return types, public fields, associated types,
  bounds, and implementation headers. Prose is out of scope, and a link that
  leaves for a host the review has not seen is reported rather than skipped.
  Documenting the dependencies is what makes it sound: under `--no-deps` a
  crate that declares no `html_root_url` renders as unlinked text, and a
  `tokio::runtime::Handle` in a public signature was invisible until the
  dependencies were documented. The check fails both on an unlisted disclosure
  and on an accepted exception that no longer occurs, and the accepted list is
  empty. CI runs it next to the boundary check.
- Eight domain accessors on `ChunkComponentRevisions` — `reader`, `processor`,
  `writer`, `checkpoint`, `checkpoint_schema`, `checkpoint_schema_version`,
  `context_schema`, and `context_schema_version` — and a public
  `ChunkDeliveryMode::manifest_name`. They let the plan crate compose the
  canonical chunk declaration from typed values instead of receiving it
  pre-serialized. `manifest_name` returns the durable name the mode is recorded
  under, which is fixed for the life of the mode rather than a display string.

- `RepositoryDescriptor`, the versioned capability declaration an adapter
  publishes, plus `JobRepository::descriptor`. Flow launch negotiates the
  capabilities a compiled plan requires against it and rejects an undeclared
  requirement with `FlowRuntimeError::UndeclaredCapability` before any durable
  write, instead of failing part-way through an execution that already wrote
  lifecycle rows. The `descriptor` default declares nothing, so an adapter that
  has not been reviewed against a capability is negotiated as not providing it.
  Throughput settings — pool size, connection capacity, statement timeout — are
  deliberately absent from the descriptor and from the canonical manifest.
- `StateSchemaUpgrade`, one declared directed edge between two application
  schema versions. Edges must strictly increase the version and at most one may
  leave any version, so a resolved chain is deterministic and bounded. Upgrade
  output is held to the envelope's JSON-object shape and the durable hard
  ceilings before it reaches the codec, and `StateError` gains
  `NonIncreasingUpgrade`, `NoUpgradePath`, `AmbiguousUpgrade`,
  `UpgradeOvershootsCurrent`, `UpgradeChainTooLong`, and
  `UpgradeProducedInvalidJson` for the ways a declaration or a transform can
  fail.
- Public constructors and accessors the stage-2 crate boundary needs, landed
  ahead of the module move so the move itself changes no API:
  `OperatorRecordDraft::{applied, rejected}`,
  `RetentionRecordDraft::{instance_action, purge}`, `RecoveryEvidence::new`,
  `RecoveryProposal::new`, `OperatorRequest::job_instance_key`, and
  `RecoverySnapshot::{status, owner, updated_at, server_time}`. The draft and
  proposal constructors are purpose-named rather than one wide constructor per
  type, so an audit row cannot disagree with the request it audits and a
  proposal cannot carry a digest its evidence does not produce.
- `OperatorRejection::UnsupportedAction`, durable code `UNSUPPORTED_ACTION`, for
  an action a build cannot apply. `OperatorAction` is `#[non_exhaustive]`, so
  after stage 2 the operator service can no longer match it exhaustively; this
  is the conservative arm that audits the request and applies nothing. The arm
  that uses it lands with the module move, because a wildcard arm is
  unreachable inside the crate that declares the enum.
- `IdentifierKind::FlowDecisionSequence`.
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
- The `oxide-batch-repository` implementation crate, holding the metadata
  repository, unit-of-work, clock, identifier, explorer, operator, retention,
  and recovery ports, the durable partition, flow-decision, audit, retention,
  and recovery values those ports exchange, the bounded operator request
  envelope, and the keyset pagination vocabulary the explorer port pages with.
  It depends only on `oxide-batch-core`. It is implementation detail with no
  stability promise; `oxide-batch` remains the only supported entry point and
  re-exports every supported item under its existing path.
- The `oxide-batch-plan` implementation crate, holding the immutable flow graph
  of step, decision, split, join, and partitioned-step nodes with its
  exit-pattern transitions, the compiled execution plan those graphs lower
  into, graph normalization, the structural validation the accepted basic-flow
  contract names, and the canonical restart-relevant manifest projection the
  definition fingerprint digests. It depends only on `oxide-batch-core` and is
  an independent sibling of `oxide-batch-repository`. It is implementation
  detail with no stability promise; `oxide-batch` remains the only supported
  entry point and re-exports every supported item under its existing path.
- Staged crate-extraction evidence checks: `cargo xtask deps` fails the build on
  a forbidden dependency class or a workspace cycle, `cargo xtask package` runs
  the workspace publish dry run for every publishable crate, and a facade
  surface test holds all 424 exported paths against a committed snapshot while
  proving each path still resolves.

### Changed

- **Breaking (pre-1.0):** the canonical-manifest seam between the core and plan
  crates no longer exchanges `serde_json::Value`.
  `ChunkComponentRevisions::manifest_value`, `FlowTarget::manifest_value`, and
  `StartControls::manifest_value` are removed, and
  `DefinitionIdentity::from_flow_manifest` takes canonical manifest bytes
  instead of a parsed document. The projections move into `oxide-batch-plan`,
  which owns the canonical manifest, beside the fault-policy projection that
  was already written that way; the accessors listed above replace what the
  removed methods reached for. The M5 facade review found these four items in
  violation of the accepted rule that keeps Serde types out of core public
  signatures — every one of them was private before the staged crate extraction
  turned an intra-crate call into a cross-crate one. Taking bytes also lets the
  constructor check that they re-encode to themselves, which the value-taking
  form could not. No manifest byte and no definition fingerprint changes: the
  same `serde_json::to_vec` call runs on the same document, one crate earlier.
- The workspace version moves to `0.1.0-alpha.2`. `0.1.0-alpha.1` is published,
  so the API changes above cannot ship under it: `cargo publish --workspace
  --dry-run` resolves an already-published sibling from the registry rather than
  from the local archive, and the facade fails to verify against the older
  `oxide-batch-core` and `oxide-batch-repository`. Under the ADR-0010 lockstep
  rule every extracted crate moves with the facade. The support matrix is
  unchanged, because it binds only released versions and `alpha.2` is
  unreleased.
- **Breaking (pre-1.0):** `VersionedStateCodec` now declares the directed
  schema upgrades it can apply, and the framework applies them. `decode` loses
  its `StateSchemaVersion` argument and receives a payload already at
  `current_version`; a codec that has published an older schema returns the
  edges reaching the current one from the new `upgrades` method, whose default
  is empty. This applies the accepted M5 codec direction, which requires one
  bounded, deterministic upgrade chain rather than a codec that inspects a
  recorded version and guesses what an older field meant. A recorded version
  with no declared path to the current version is now rejected with
  `StateError::NoUpgradePath` instead of reaching the codec. No durable byte,
  envelope format version, or definition fingerprint changes.
- Extracted implementation crates are published in lockstep with the facade
  rather than kept `publish = false`. Cargo rewrites a published archive's path
  dependencies to registry dependencies, so a publishable facade cannot depend
  on an unpublished crate; the first extraction stage would have made
  `oxide-batch` unpublishable. RFC-0011 records the conflict and ADR-0010
  supersedes ADR-0001 for the three M5-authorized crates only. The release
  workflows now package, checksum, generate SBOMs for, attest, and publish
  those crates in dependency order.
- **Breaking (pre-1.0):** `StartLimit::new` returns `DefinitionError` instead of
  `PlanError`. The type is restart-relevant definition data that the repository
  port names, so ADR-0011 places it in the domain layer, and `PlanError` cannot
  follow it there because nineteen of its variants carry a `NodeId` and two
  carry an `ExitPattern`. `PlanError::ZeroStartLimit` is replaced by
  `DefinitionError::ZeroStartLimit`. No durable byte, fingerprint, or manifest
  member changes.
- **Breaking (pre-1.0):** `FlowDecisionSequence::new` returns `DomainError`
  instead of `FlowRuntimeError`. The sequence is a durable ordinal carried by
  every persisted flow decision, so it follows the decision record into the
  repository layer, and `FlowRuntimeError` cannot follow it there because it
  stays with the flow engine. It now rejects zero as
  `DomainError::ZeroIdentifier { kind: IdentifierKind::FlowDecisionSequence }`,
  exactly as the sibling `FlowDecisionId::new` already did. The engine maps the
  error back to `FlowRuntimeError::DecisionSequenceExhausted` at its one call
  site, so no caller of the flow runtime observes a different error. This is
  the second and last reviewed API change ADR-0011 predicts.
- Durable flow identities and fault-policy values moved into
  `oxide-batch-core`: `NodeId`, `FlowTarget`, `TerminalKind`, `StartControls`,
  `StartLimit`, `MAX_PARTITIONS`, and the seventeen runtime-free fault-policy
  values. Every `oxide-batch` path resolves unchanged. `BackoffSleeper` and
  `BackoffOutcome` stay with the runtime, and the plan crate keeps the compiler
  and the graph types only it constructs.
- Crate-extraction stage 3 landed, completing the M5 extraction: the flow
  graph, the compiled execution plan, graph normalization, and the canonical
  manifest projection moved from `oxide-batch` into `oxide-batch-plan`. The
  flow engine that executes a plan, the runtime, the metadata adapters, and the
  services stay in the facade. Every `oxide-batch` path resolves unchanged, the
  facade export snapshot is byte-identical, and the workspace test set is
  identical: only two doctest identities moved, because both examples relocated
  into the `oxide-batch` crate documentation so that they keep demonstrating
  the supported import path. `CompiledExecutionPlan::compatibility_one_step` is
  the single item the split forced open, and it is `#[doc(hidden)]`.
- Three `#[non_exhaustive]` matches now cross the stage-3 boundary and take a
  conservative wildcard arm: a flow-node kind the build cannot bind is reported
  unbound rather than passed as satisfied, a flow-node kind the runtime cannot
  dispatch is refused as an unsupported manifest exactly as a join node is, and
  a plan-selection failure the runtime cannot interpret stops the launch
  instead of following a guessed target. No durable byte, fingerprint,
  transaction boundary, or normalized trace changes.
- `cargo xtask deps` now enforces the accepted prohibition on
  `oxide-batch-plan` depending on `oxide-batch-repository`. The contract has
  named the two crates independent siblings since ADR-0011; the check carried
  the rule in only one direction until the plan crate existed.
- Crate-extraction stage 2 landed: the repository, explorer, operator,
  retention, and recovery ports, their capability descriptors, and the durable
  values they exchange moved from `oxide-batch` into `oxide-batch-repository`.
  The metadata adapters, the four services that drive the ports, the plan
  compiler, and the execution engines stay in the facade. Every `oxide-batch`
  path resolves unchanged, the facade export snapshot is byte-identical, and
  the documented public surface is unchanged: every item the split forced open
  is `#[doc(hidden)]` and is named in the crate-extraction evidence.
- Six `#[non_exhaustive]` matches now cross the stage-2 boundary and take a
  conservative wildcard arm: an unrecognized flow-transition kind matches no
  declared node, an operator action the build cannot apply is an audited
  `OperatorRejection::UnsupportedAction`, an unrecognized operator outcome class
  is reported to telemetry as non-accepting, an unrecognized retention action
  changes no hold state, and an explorer query neither metadata adapter knows
  reports `ExplorerError::UnsupportedCapability` instead of paging from a
  guessed ceiling. No durable byte, fingerprint, transaction boundary, or
  normalized trace changes.
- Crate-extraction stage 2 was attempted and not landed; the boundary is sound
  and the repository crate compiles clean, but splitting the service
  descriptors from their implementations needs public constructors and
  accessors that are their own reviewed API change. The findings are recorded
  in the crate-extraction evidence.
- Crate-extraction stages 2 and 3 are unblocked by accepted ADR-0011. The
  repository port names `NodeId` and `StartLimit` in its signatures while the
  accepted contract forbids `oxide-batch-repository` from depending on
  `oxide-batch-plan`, so the named boundary content and the code disagreed. The
  ADR places durable data at or below the layer that persists it and keeps
  compilers, runtimes, and engines above it, which makes the plan and
  repository crates independent siblings over core.
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

- The capacity budget's declared partition-key bound is the one the code holds.
  The table gave `256` bytes; `MAX_PARTITION_KEY_BYTES` has been `128` since the
  bound was introduced, so the number an operator would have sized a partition
  key against was never the number the framework enforces. Nothing about the
  enforcement changed — the ceiling has always been `128` and has always been
  refused above it — but the document a deployment is sized from disagreed with
  it, and no check related the two. The resource-bound campaign's reconciliation
  now requires the budget table, the campaign scope, and the constants the code
  declares to agree, so this class of drift fails an ordinary `cargo test`.
- The capacity budget declares the retry cache, which it had omitted entirely.
  The performance plan names six resource classes that must have a finite bound
  and the budget table had a row for five of them. The durable fault state is
  that cache: a bounded envelope of unresolved retry keys that commits with the
  chunk, with a `256`-entry and `64 KiB` ceiling. Both are now declared where an
  operator will look for them.

- Evidence-bound recovery discovery works against PostgreSQL. The recovery
  snapshot read the `attempt` column, which is an `integer`, as an `i64`, so
  every proposal failed with a redacted `Unavailable` before it observed
  anything, and the operator CLI's recovery workflow could not produce a
  proposal on the only supported database. No PostgreSQL test reached that
  port; the proposer's coverage ran against the in-memory explorer. The column
  is now read the way the execution projection already reads it, and the crash
  and restore campaign exercises it on both matrix points.
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
