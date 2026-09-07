# M7 Advanced Flow, Scope, Repeat, and Definition-Evolution Contract

**State:** Accepted by
[ADR-0013](decisions/0013-m7-advanced-flow-scope-repeat-and-evolution.md)

**Design gate:** [#194](https://github.com/luceat-lux-vestra/oxide-batch/issues/194)

This is the canonical M7 semantic contract for Gates A-F in the
[M7 kickoff gate](../project/m7-kickoff-gate.md). It extends the accepted M0-M6
lifecycle, repository, plan, transaction, operator, and redaction contracts; it
does not replace them.

## Binding prerequisites

- [ADR-0004](decisions/0004-job-definition-restart-compatibility.md): exact
  definition identity, fail-closed drift, direct one-way compatible edges, and
  atomic state upgrade.
- [ADR-0005](decisions/0005-compiled-execution-plan.md): immutable definitions,
  stable logical IDs, compiled validation, and one compiled-plan runtime.
- [ADR-0009](decisions/0009-definition-fingerprint-input-set.md): only values
  selecting/reinterpreting durable meaning enter the fingerprint.
- [M4 operator/explorer contract](operator-and-explorer-services.md): bounded
  keyset reads; optimistic, idempotent, audited mutations; payload redaction.
- M6 completion/fault/listener contracts: retry, skip, rollback, completion,
  listener ordering, panic containment, and durable accounting.

## Gate A — advanced flow and composition

### Graph and node model

M7 flow is finite and structurally acyclic. A compiler rejects any edge that
makes a node reach itself, directly or through composition. Repetition exists
only through Gate C; no runtime predicate legalizes a graph back edge.

Supported composition forms are nested flow, split with one declared join,
nested job, and a registered custom leaf node.

A nested-flow subgraph has one declared entry and one or more exits normalized
into its owning outer node. A split owns exactly one join; every branch is a
bounded subgraph whose successful/failed/stopped/unknown outcome reaches that
owned join, and no branch edge may escape to another branch, a foreign join, or
the outer graph. The join alone resumes the outer graph.

A custom leaf declares stable kind/revision/state-schema identity and executes
through framework-provided lifecycle, transaction, fault, cancellation, and
state ports. It may not:

- mutate graph topology after compilation;
- own an independent repository lifecycle or transaction authority;
- spawn detached framework work;
- bypass framework lifecycle/fault/listener/start-control hooks;
- expose adapter/runtime implementation types through the facade.

### Identity and capability bounds

Every materialized node has one stable `NodeId`. Nested diagnostic paths may be
hierarchical, but identity is the stable logical ID, not display text.

M7 retains the delivered global ceilings:

- `MAX_NODES = 1_024`;
- `MAX_TRANSITIONS = 4_096`;
- `MAX_OUTGOING_TRANSITIONS = 64`;
- `MAX_SPLIT_BRANCHES = 8`.

M7 adds `MAX_FLOW_COMPOSITION_DEPTH = 8`, including nested-job depth. A nested
flow, split branch, join, nested job, and custom leaf all count against global
node/transition ceilings. M4's `MAX_BRANCH_STEPS` remains the bound of the M4
linear-branch representation; M7 branches are bounded subgraphs and receive no
separate unbounded allowance. Capability ceilings are not fingerprint inputs.

### Nested-job binding and outcome

A nested-job node declares the child definition ID/revision and a bounded typed
parameter mapping. Its mapping sources use the same structured source model as
Gate B: parent job parameters, committed parent job/step context, and closed
framework metadata only. Mapping kind/revision, source selectors, target
parameter names/types, and default/coercion rules are definition identity when
they can change child instance selection or durable meaning.

The framework resolves and validates the complete child parameter set, computes
the child instance identity, and commits that parameter identity plus the
parent-node/child-definition/child-instance/child-execution link before child
user work starts. Once the link is committed, restart reuses it and does not
re-run the mapping against newer parent context.

The parent node mirrors the committed child terminal class: child `UNKNOWN`
keeps the parent unresolved and blocks restart until recovery; `FAILED` fails
the node; `STOPPED` stops it; `COMPLETED` exposes the child's stable exit status
to outer transition matching. Parent stop/cancellation propagates through the
owned child and never abandons it as detached work.

### Durable authority and restart

Before dependent work begins, the repository commits the selected transition or
decider result; split child identities/states and declared branch ordinals; the
join's normalized aggregate decision; nested-job parent/child linkage; and any
restart-relevant custom-leaf state declared by its versioned state schema.

Restart uses committed records only. Completed branches/children and committed
transition/join decisions are reused. Scheduling order, logs, task completion,
or mutable registration state cannot cause restart to select a different
committed path or nested child.

### Deterministic aggregation and failure

Split aggregation is independent of completion timing and retains the M4 total
order. Results normalize in declared branch ordinal. Terminal severity is
`UNKNOWN > FAILED > STOPPED > COMPLETED`; equal-severity primary outcome uses
the lowest branch ordinal. Secondary failure references remain bounded/redacted.

The accepted local failure policy controls sibling cancellation/drain behavior
but never normalized outcome. Framework-owned children are cooperatively
cancelled/drained and joined; an incomplete drain cannot fabricate terminal
completion.

## Gate B — job/step scope and late binding

### Lifetime and factories

Job scope belongs to one `JobExecution` attempt; step scope belongs to one
`StepExecution` attempt. Restart creates new live scope instances.

A job/step component factory creates at most one live component for one
`(scope, logical_component_id)` pair. Successful constructions are memoized
within the scope and cleanup runs in reverse successful-construction order. A
partial construction failure cleans every already-created dependency before the
failure escapes.

Live sockets, connections, process handles, credentials, or implementation
pointers are never persisted as scoped state.

### Structured late-bound source model

The canonical public semantic is a typed structured selector, not a free-form
expression language. A selector names exactly one source family plus a bounded
path and expected type:

1. immutable job parameters under their declared schema;
2. last committed job execution context visible at scope creation;
3. last committed step execution context visible at scope creation;
4. closed framework metadata: definition/revision/fingerprint, logical
   node/step identity, attempt ordinal, and opaque execution identifiers.

Environment variables, wall clock, randomness, filesystem/network reads,
credentials/secrets, telemetry state, arbitrary callbacks, user functions, and
user-defined I/O are prohibited sources.

M7 does not require or expose an evaluator with arithmetic, functions,
reflection, mutation, or ambient lookup. A future convenience string parser is
allowed only as syntax sugar that lowers bijectively to the same structured
selector; it may add no semantic source or operation and may be omitted entirely
without reducing M7 behavior.

Missing values, wrong types, unsupported coercions/defaults, unresolved paths,
cycles, and over-bound selectors fail with stable typed categories.

Deployment secrets may enter application factories only through deployment
resource handles outside the resolver. Credential values never enter manifests,
fingerprints, resolver records, explorer/audit output, or diagnostics.

### Restart meaning and bounds

Factory kind/revision, resolver kind/revision, structured source path/schema,
expected type, and coercion/default policy enter the fingerprint when they can
reinterpret durable state. Actual parameter/context values remain execution
data.

A restart-relevant resolution persists bounded provenance only: scope kind,
logical component ID, source kind/path and source schema/version, plus the
identity and committed version/checksum already owned by the authoritative
parameter/context record when such an identity exists. M7 does **not** persist
a new digest or copy of the resolved value: a low-entropy or otherwise
sensitive parameter must not gain a new offline-verification surface merely by
being late-bound. Restart re-resolves from the referenced committed source
snapshot and fails closed when that authoritative source identity/version or
resolver/schema contract no longer permits the prior meaning to be reused.

Bounds: at most 256 scoped components per scope, dependency depth 32, 64
late-bound inputs per component, and 1,024 UTF-8 bytes per selector path. These
capacity bounds are excluded from the fingerprint.

## Gate C — repeat context and interceptors

M7 repeat is one explicit control primitive over the M6 completion/fault engine.
It is not a graph cycle and adds no second retry/skip/rollback/transaction/
lifecycle engine.

A repeat definition has stable logical identity, policy kind/revision,
restart-relevant configuration, bounded state schema, and ordered interceptor
identity. Those values enter the fingerprint when they change durable meaning.

A logical iteration is identified by repeat ID plus zero-based durable ordinal
inside one step execution. The ordinal advances only at the accepted iteration
commit boundary. Rollback/process loss before commit replays the same ordinal.
The durable repeat record retains current committed ordinal, bounded repeat
state/policy state, and committed continue/complete decision; no unbounded
iteration history is stored.

The continue/complete decision commits before the next iteration begins.
Restart reuses that committed decision.

For interceptors `[A, B, C]`, `before` is `A -> B -> C`; the existing M6
operation/fault engine runs inside; `after` is `C -> B -> A` for interceptors
whose `before` completed. Retry/skip/rollback inside an iteration does not
re-enter repeat `before`. Step/job listener ordering remains the M6 contract.

Interceptor failures have one deterministic authority:

- if a `before` fails, the body does not run; the failing `before` is primary
  and previously entered interceptors unwind in reverse order;
- if the body succeeds and an `after` fails, the first `after` failure in the
  deterministic reverse-order unwind becomes the primary iteration failure;
- if the body already failed or panicked, any `after` failure is secondary and
  cannot replace that primary outcome;
- later unwind failures are secondary bounded diagnostics;
- interceptor failures are not item-level retry/skip candidates and therefore
  cannot recursively enter the M6 item fault loop.

A contained panic uses the same primary/secondary rule. Process kill fabricates
no `after` callback; restart reconstructs only from committed repeat state.

Bounds: at most 32 interceptors per repeat definition and repeat nesting depth 8.
Repeat state uses existing bounded versioned execution-context envelope rules.

## Gate D — `DefinitionRegistry`, compatible evolution, and fork lineage

### Immutable registration

`DefinitionRegistry` resolves `(DefinitionId, DefinitionRevision)` to exactly
one canonical manifest/fingerprint plus one bounded application-owned artifact
identity. Executable path/location and credentials are not durable identity.

Registration is compare-and-swap and idempotent: identical registration returns
the existing record; same definition/revision with a different fingerprint is
`DefinitionDrift`; a conflicting restart-relevant artifact identity requires a
new revision. There is no last-writer-wins mutation.

Before lifecycle creation, executable assembly resolution proves component
kind/revision and definition fingerprint against the registered manifest.
Missing/mismatched assembly rejects before lifecycle writes.

### Compatible restart

Strict restart requires exact checkpoint-producing fingerprint. Compatible
restart requires exactly one explicit direct one-way edge; edges are bounded,
deterministic, non-transitive, and never inferred from semver/names/successful
deserialization.

An edge names source/target fingerprints, bounded upgrade key/revision, an
injective mapping for every durable source node, and all checkpoint/context/
scope-resolution/repeat/other durable schema transforms required by mapped
nodes. Target-only nodes start with their declared initial state and no source
node may be silently dropped. The transformer is deterministic, bounded, and
side-effect free. Transformed durable state, selected edge, and new execution
creation commit atomically. Failure leaves source state unchanged and creates no
partial target.

### Fork

`Fork` creates a new job-instance lineage and never claims restart. The lineage
record names source execution/committed-state identity and target definition,
instance, and execution.

No lifecycle status, attempt/start-limit counter, operator request, or opaque
application context is copied by default. A declared deterministic bounded fork
transformer may derive target parameters/context/checkpoint from committed
source state under explicit schemas. Validation plus target instance/execution
and lineage creation commit atomically.

Target identifying parameters select the new instance normally. Collision with
an existing target instance follows ordinary launch guards.

### Rejection/concurrency

Newer/corrupt formats, missing transforms/artifacts, multiple ambiguous edges,
stale versions, or conflicting concurrent registration fail closed. Ambiguous
commit remains `UNKNOWN` and is resolved through idempotent replay/readback, not
blind duplicate writes. `Force` remains unavailable.

## Gate E — application, operator, and explorer completion

### Parameter incrementer and restart controls

A parameter incrementer is a deterministic pure application function over the
explicitly selected prior typed parameter set plus current target definition
identity. Kind and revision are restart-relevant metadata. It may not read
repository state, wall clock, randomness, environment, credentials, filesystem,
or network; the caller/operator supplies the prior set being incremented.

Returned parameters are validated against the target schema and identifying
instance identity is recomputed. Returning the same identifying parameters does
not trigger an internal retry loop or invented value.

Job/step restartability, start limit, and `allow_start_if_complete` are
definition metadata/fingerprint inputs. A non-restartable job cannot create a
restart execution after a committed attempt; a non-restartable step cannot be
re-entered on a later job-execution attempt once its committed state requires
restart. Nested flow/job/repeat does not reset or shadow start-limit counters.

### Operator/explorer

M7 operator mutations inherit existing `ActorRef`, `OperationId`,
`RequestDigest`, authorization class, `ExpectedVersion`, append-only audit,
optimistic-conflict, and `OperationOutcomeUnknown` semantics. Restart explicitly
selects strict or one named compatible edge; fork is recorded/projectioned as
fork, never restart. Authentication/RBAC remain deployment-owned.

M7 explorer may add only closed query families for definitions/revisions,
compatible edges, fork lineage, nested-job child links, branch/join decisions,
scope-resolution metadata, and repeat-state metadata. They inherit M4 keyset
pagination (1..=500, default 50), immutable ordering/identity ceiling, 256 KiB
response limit, timeout, and unsupported-capability behavior. No arbitrary
predicate, payload search, full-history count, or caller-selected ordering.

Projections expose bounded identities, versions, digests, sizes, statuses, and
framework categories only; parameter/context/checkpoint values, credentials,
executable locations, SQL/driver errors, and component-private state are
prohibited.

## Gate F — manifest/schema evolution, resource, and evidence

### Definition-manifest format 4

M7 writes canonical manifest format `4`; formats `1`, `2`, and `3` remain
readable and persisted bytes are never rewritten. Repository migration never
converts an older definition to format 4.

Format 4 may add only restart-relevant M7 structure: normalized nested/split/
join/nested-job/custom-leaf identity; nested child-definition and parameter-
mapping identity; scope factory/resolver/source-schema/policy identity; repeat
policy/interceptor/state-schema identity; registry artifact/upgrade/fork
interpretation identity; and restart-control/parameter-incrementer identity.

ADR-0009 exclusions remain binding. Resolved values, credentials, runtime
handles/paths, telemetry, diagnostics, capability ceilings, and throughput-only
budgets are excluded.

The existing maximum canonical-manifest size remains `64 KiB`. Format 4 gets no
larger side channel or spill format: compilation/reading fails closed when the
canonical bytes exceed that bound, even if node/transition ceilings have not
been reached. Format-4 readers also reject unsupported required members,
duplicate keys, invalid canonical encoding/order, newer format, over-bound
structure, and corrupt digest. Earlier readers reject format 4 as newer.

### PostgreSQL repository schema 5

The current M6 repository baseline is schema `4`, established by component-state
durability. M7 appends repository schema `5`; it does not reuse or reinterpret
schema 4.

Schema 5 adds bounded records/indexes required for immutable registry/direct
compatibility edges, fork lineage, nested-job links, generalized branch/join
state, scope-resolution metadata, repeat state/decisions, and M7 operator/
explorer projections.

The `4 -> 5` migration is ordered and transactional. It does not rewrite
existing manifests/fingerprints, component state, checkpoints, contexts,
lifecycle history, or prior audit rows. Supported earlier schemas reach 5 via
the existing chain ending in `4 -> 5`.

A pre-schema-5 runtime rejects schema 5 before repository/migrator writes. A
failed schema-5 migration must leave the source schema usable. Operational
rollback after successful migration is restore-based from a pre-migration
backup; no down-migration compatibility is claimed.

### Structured resource ownership

All M7 children remain framework-owned. No detached branch, nested-job, scope
cleanup, interceptor, custom-node, or migration worker exists. Cancellation
propagates to owned children and they are joined before a terminal status when
possible; incomplete drain remains explicit.

Capability ceilings in Gates A-C are compile/read-time bounds, not fingerprint
inputs. Throughput budgets remain excluded only when normalized durable
observations are unchanged.

### Mandatory PG15/18 evidence

Every newly durable M7 boundary receives visible PostgreSQL 15 and 18 restart/
process-kill evidence, including before/after transition/branch/join decisions,
nested-job parameter/link creation and child terminal observation, repeat
iteration/continue decisions, compatible transformed-state/new-execution
commit, fork lineage/new-instance commit, registry/edge mutations, and the
schema-5 migration transaction boundary.

Normalized observations compare durable rows, logical decisions, counters,
checkpoint/context digests, audit rows, and public outcomes independent of task
scheduling. A green skip does not count as evidence.

## Public impact summary

| Area | M7 decision |
| --- | --- |
| Public API | Typed composition/custom leaf, structured late-bound selector, scope factory, repeat/interceptor, registry/evolution, incrementer, and bounded operator/explorer families only; no adapter/runtime/credential leakage. |
| Durable meaning | Manifest format 4 (still <=64 KiB); repository schema 5 over current schema-4 M6 baseline. |
| Restart | Exact fingerprint default; one direct compatible edge across changed identity; fork is new lineage only; nested-job mapping/link is committed before child work. |
| Security | Resolver excludes credentials/ambient state; no derived resolved-value digest is persisted; diagnostics/explorer/audit remain payload-redacted; deployment owns auth/RBAC. |
| Resource | Existing graph/split/manifest ceilings plus composition depth 8, scope bounds, repeat interceptor/nesting bounds, structured child ownership. |
| Compatibility | Manifest 1-3 remain immutable/readable and pre-format-4 readers reject format 4; repository schema 1-4 use the ordered chain to 5 and pre-schema-5 runtimes reject schema 5 before writes. |
| Out of scope | M8 portability, M9 integrations, M10 scheduler/performance, M11 remote execution, M12 migration tooling, hosted control plane, generic cross-resource exactly-once. |
