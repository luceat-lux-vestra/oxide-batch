# Restart and State Guide

**State:** Accepted (distilled from existing canonical sources — see
"Canonical sources" below; this guide restates nothing those documents say
more precisely, it cross-references and orders it for a reader building or
operating a restartable step)

This guide is for two audiences: someone implementing a custom stateful
component (reader, processor, writer, or `ItemStream`) who needs to know
what restart requires of them, and someone operating a job who needs to know
what a restart actually does and does not guarantee. It does not introduce
new semantics — every claim here is owned, in more precise and binding form,
by one of the documents in "Canonical sources," and this guide links to the
exact section rather than restating it at length.

## Canonical sources

- [Execution, Restart, and Transaction Semantics](../compatibility/execution-semantics.md) —
  definition identity/fingerprint, lifecycle rules, transaction/delivery
  semantics, panic/crash handling. The binding source for "what a restart
  is."
- [Item-Processing Model](../architecture/item-processing-model.md) — chunk
  lifecycle, component state/checkpointing (namespace, schema/codec version,
  checksum, migration, sensitivity), composition taxonomy. The binding
  source for "what a stateful component owes the framework."
- [M6 design-gate evidence, Gate C](../project/m6-design-gate-evidence.md#gate-c--itemstream--component-state) —
  closes the `ItemStream`/component-state contract referenced above.
- [M6 design-gate evidence, Gate B](../project/m6-design-gate-evidence.md#gate-b--transactionrestart-equivalence-protocol) —
  the typed-vs-`Boxed*` representation-equivalence protocol. Evidence:
  `crates/oxide-batch/tests/gate_b_01_normal_commit.rs` through
  `gate_b_08_representation_transparent_identity.rs`.
- [Component conformance matrix](../engineering/campaigns/m6/component-conformance-matrix.md) —
  per-component restart/crash evidence, or the contract reason a component
  is N/A.

## Logical component identity, stream identity, and revision

A component's **logical identity** is a stable namespace scoped under the
owning definition's identity — never a display name, a runtime object
address, or anything that changes between processes
([item-processing-model.md § State and checkpointing](../architecture/item-processing-model.md#state-and-checkpointing)).
In code, this is `ComponentStreamIdentity::new(name)` for an `ItemStream`'s
own namespace, and `ComponentRevision::new(revision)` for a
reader/processor/writer/checkpoint-schema revision, both stable strings a
caller assigns explicitly at construction — never derived from a type name
or memory address (see `crates/oxide-batch/tests/support/gate_b.rs`'s
`ChunkComponentRevisions::new(reader_revision, processor_revision,
writer_revision, checkpoint_revision, restart_contract)` for a working
example of every identity a chunk step declares).

The **definition identity** — the job/step-level fingerprint restart
compatibility is checked against — is a separate, higher-level concept:
[ADR-0004](../architecture/decisions/0004-job-definition-restart-compatibility.md)
and [execution-semantics.md § Definition identity and restart](../compatibility/execution-semantics.md#definition-identity-and-restart)
are the binding source. `crates/oxide-batch/src/chunk_builder.rs`'s
`typed_and_boxed_pipelines_share_one_fingerprint` and Gate B's `gate_b_08`
are the evidence that this fingerprint — and everything restart selection
keys off — does not depend on whether a component is instantiated typed or
`Boxed*` (see "Typed vs Boxed representation irrelevance" below).

## Schema/codec version, migration, and rejection

Component durable state (an `ItemStream`'s checkpoint payload) carries a
schema ID and version, and a codec ID and version, independently of the
component's own logical identity. `StateSchemaId::new(..)` and
`StateSchemaVersion::new(..)` are the types; a `ChunkRestartContract`
(`crates/oxide-batch/src/chunk_builder.rs`, or `support/gate_b.rs`'s
`restart_contract()` for a minimal working example) declares the checkpoint
and context schema identity/version pair a step's durable state is written
under, plus its `ChunkDeliveryMode`.

Decode/migration rules
([item-processing-model.md § State and checkpointing](../architecture/item-processing-model.md#state-and-checkpointing)):
an equal version decodes directly; an older version applies one bounded,
deterministic, directed migration chain; a newer version, an unknown schema,
or an unknown codec all **fail closed**. A migration never changes component
or definition identity, and a migration failure is a known, not-committed
outcome — never a silent fallback to empty/default state.

## Checksum and corruption

A checksum is verified before any decode or migration step runs. A mismatch
is a typed corruption failure: corrupt state is never replaced with empty or
default state, never advances a checkpoint, and — per the sensitive-data
rule below — is never exposed as a raw value in diagnostics, including when
the corruption is what's being diagnosed.

## Checkpoint relationship and transaction atomicity

For an enlisted, same-resource transaction (`ChunkDeliveryMode::AtomicSameResource`),
business writes, the checkpoint, component state, and counters share **one**
atomic commit/rollback boundary — proved directly, not assumed, by Gate B's
`gate_b_03_atomic_boundary.rs`
(`state_checkpoint_counter_share_one_atomic_boundary`), which forces a
chunk's checkpoint-provider call to fail *after* its writer has already
succeeded and confirms the whole chunk — business rows included — rolls
back together rather than leaving a split commit. Building this evidence
surfaced a real bug worth knowing about if you are writing a custom writer
that participates in this boundary: a writer that persists through its own
independent connection instead of the chunk's enlisted transaction
(`WriteContext::transaction()`) commits its own rows immediately,
independently of the chunk's own commit or rollback — silently defeating
`AtomicSameResource` regardless of what the transaction manager does. See
`WriteContext::transaction()` (`crates/oxide-batch/src/chunk.rs`) and
`BusinessTransaction::execute` for the correct enlistment pattern; a custom
writer that needs this guarantee must use it, not its own connection.

An unknown commit outcome (the acknowledgement of a `COMMIT` the client
issued is lost, e.g. the process dies between issuing it and observing the
result) is never inferred as success or failure and never triggers automatic
replay before durable state is checked through a fresh connection — proved
by Gate B's `gate_b_04_unknown_outcome.rs`
(`unknown_commit_outcome_forces_recovery_not_inference`), and see
`ChunkAttemptOutcome::Unknown`/`RepositoryError::CommitOutcomeUnknown` in
`crates/oxide-batch/src/`. A crashed execution is left in a `Started` state;
restart requires an explicit, audited recovery decision
(`RecoveryRequest::mark_failed`) before a new attempt is allowed — it is
never a bare retry (see `gate_b.rs::mark_crashed_execution_failed`'s doc
comment for exactly why, discovered directly while building this evidence).

## Policy-owned state/revision semantics

A completion policy that carries its own persisted decision state (only
`AdaptiveCompletionPolicy` does, among first-party policies — see the
[component conformance matrix](../engineering/campaigns/m6/component-conformance-matrix.md#completion-policies))
owns that state's namespace and revision the same way a stateful
reader/writer does; `crates/oxide-batch/tests/postgres_completion_policy_restart.rs`
is its real-database restart evidence. A stateless policy
(`ItemCountCompletionPolicy`, `TimeCompletionPolicy`) or a pure composition
of member policies (`CompositeCompletionPolicy`) has no restart obligation
of its own — restart correctness is inherited from whatever it wraps, the
same inheritance rule as the composition taxonomy below.

## Composition and restartability inheritance

A decorator or composition component (`FilterProcessor`, `PeekReader`,
`AggregatingReader`, `ClassifyingProcessor`/`Writer`,
`CompositeReader`/`ChainProcessor`/`FanOutWriter`,
`SynchronizedProcessor`/`Writer`) that holds no durable state of its own has
its restartability determined entirely by its delegate(s) — this is stated
explicitly in each component's own doc comment (e.g. `AggregatingReader`:
"Restartability: exactly the delegate's") and is why the conformance matrix
marks these `N/A` for independent restart evidence rather than treating the
absence of a dedicated crash test as a gap. A component you compose from
first-party pieces inherits this same rule: if it adds no state of its own,
it adds no restart obligation of its own, and inherits whatever restart
guarantee its delegates already carry evidence for.

## Crash/restart expectations

- A killed process's in-progress, uncommitted work is not durable — proved
  by real `SIGKILL` evidence throughout Gate B (`gate_b_05`,
  `gate_b_06`, `gate_b_07`) and the M5-era
  `postgres_commit_phase_process_kill.rs`/`crash_restore` campaign.
- Restart selects the last valid **committed** checkpoint, never a
  process-local belief about what happened — durable state is always read
  through a fresh connection.
- A restart-selected checkpoint is itself a valid resume point for a further
  crash (`gate_b_07_multi_chunk_restart.rs` proves two independent
  kill-and-restart cycles land on the correct position each time, not only
  at final completion).
- A server-side resource with no crash-survival semantics of its own (a
  `PostgreSQL` cursor: "a fresh process has no cursor and no transaction")
  documents that as its own contract rather than pretending otherwise —
  restart re-derives the equivalent state (here, a fresh `DECLARE CURSOR`
  from the durable keyset position) rather than attempting to resume
  something that cannot survive a crash by construction. See
  `PostgresCursorReader`'s own doc comment and
  `postgres_item_components_crash_recovery.rs`.

## Typed vs Boxed representation irrelevance

Changing a component's representation between fully typed and
`BoxedReader`/`BoxedProcessor`/`BoxedWriter` must not change: definition
fingerprint, restart compatibility, restart selection, or logical component
identity. This is Gate B's entire subject
([design-gate-evidence.md § Gate B](../project/m6-design-gate-evidence.md#gate-b--transactionrestart-equivalence-protocol)),
and every one of `gate_b_01` through `gate_b_08` proves it for one scenario
each — normal commit, writer failure, atomic boundary, unknown outcome,
kill-before-commit, kill-around-acknowledgement, multi-chunk restart, and
(`gate_b_08_representation_transparent_identity.rs` specifically)
representation swapped *between* the killed attempt and the restart, in both
directions, on the same job instance. `JobInstanceKey` is job name and
parameters only — nothing in the repository lookup path could distinguish
representations even in principle — and `gate_b_08` is the evidence that
this structural fact holds in practice against a real crash and restart, not
just by inspection of the key type.

If you are choosing between a typed and a `Boxed*` representation for your
own component for reasons *other* than restart behavior (compile time,
binary size, or avoiding a generic-heavy call site — see the [performance
plan](../engineering/performance-plan.md)'s Gate H P-002 campaign for the
actual measured tradeoffs), that choice has no bearing on restart
correctness either way.
