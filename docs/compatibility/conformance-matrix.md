# Spring Batch Feature Ledger

**State:** Active

**Ledger baseline:** Spring Batch 6.0.4

This is the canonical population and status ledger for Spring Batch
compatibility. It is deliberately broader than the current OxideBatch
implementation. No claim denominator may exclude a row merely because the
feature is difficult, Spring-specific, deferred, or not yet understood.

[RFC-0002](../rfcs/0002-full-spring-batch-feature-ledger-parity.md) makes
complete ledger closure the accepted long-term product target. Planned rows
are accepted scope, not claims of current implementation or support.

## Row schema

Every row records:

- stable feature ID;
- Spring Batch source version and official reference/API source;
- category/subcategory;
- capability and observable semantics;
- OxideBatch native equivalent;
- parity type;
- current status and planned milestone;
- known divergence;
- required unit (`U`), integration (`I`), conformance/differential (`C`),
  crash/restart (`Cr`), migration (`M`), and performance/resource (`P`)
  evidence;
- evidence links;
- canonical owner and dependencies/notes.

Evidence profiles use `R` (required), `N` (not normally applicable), and `J`
(required justification if omitted), always in `U/I/C/Cr/M/P` order. A profile
does not mark evidence complete.

## Source population

The ledger population is derived from the pinned reference and public API, not
only from implemented scenarios:

- [6.0.4 API overview][api];
- [domain model][domain], [job configuration][job], [step configuration][step],
  and [scaling][scale];
- [item readers/writers and streams][item] plus the
  [standard component appendix][appendix];
- [retry][retry], [repeat][repeat], [testing][testing], and
  [advanced metadata][metadata];
- [metadata schema appendix][schema] and
  [Spring Batch Integration][integration].

An update of any source population follows the baseline-update procedure in
the [compatibility contract](spring-batch.md).

## Domain, identity, lifecycle, and launch

| ID | Source | Subcategory | Spring capability and observable semantics | OxideBatch equivalent | Parity | Status | Milestone | Known divergence | Evidence U/I/C/Cr/M/P | Evidence | Owner | Notes/dependencies |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| DOM-JOB-001 | 6.0.4 [domain] | Job/Step model | Job contains named steps and produces linked instance/execution records | Typed Rust domain and compiled plan | Semantic | Implemented | M1/M7 | Rust types/API | R/R/R/J/J/N | [M1 exit](../project/m1-exit-evidence.md) | [Execution semantics](execution-semantics.md) | General flow waits for M7 |
| DOM-INSTANCE-001 | 6.0.4 [domain] | JobInstance | Identifying parameters select one logical instance | Canonical typed instance key | Behavioral | Implemented | M1/M2 | Own encoding | R/R/R/R/R/J | [`job_instance_same_identifying_parameters`](../../crates/oxide-batch/tests/domain.rs), [M2 exit](../project/m2-exit-evidence.md) | [Execution semantics](execution-semantics.md) | Released verification remains pending |
| DOM-JOBEXEC-001 | 6.0.4 [domain] | JobExecution | Every launch/restart attempt has a distinct execution linked to the instance | Distinct job execution/attempt | Behavioral | Implemented | M1/M2 | IDs and API differ | R/R/R/R/R/N | [`restart_creates_new_execution`](../../crates/oxide-batch/tests/lifecycle_conformance/cases.rs), [M2 exit](../project/m2-exit-evidence.md) | [Execution semantics](execution-semantics.md) | Released verification remains pending |
| DOM-STEPEXEC-001 | 6.0.4 [domain] | StepExecution | Each step attempt records status, counts, context, and relationship | Typed step execution | Semantic | Implemented | M1/M2 | Metadata schema differs | R/R/R/R/R/N | [M1 exit](../project/m1-exit-evidence.md), [M2 exit](../project/m2-exit-evidence.md) | [Execution semantics](execution-semantics.md) | Released verification remains pending |
| DOM-PARAM-001 | 6.0.4 [job] | Parameters | Typed identifying/non-identifying parameters control instance identity | Rust enum values and identifying role | Behavioral | Implemented | M1/M2 | Supported value types differ | R/R/R/J/R/N | [`job_instance_same_identifying_parameters`](../../crates/oxide-batch/tests/domain.rs) | [Execution semantics](execution-semantics.md) | Ledger row needed per added type |
| DOM-PARAM-002 | 6.0.4 [job] | Incrementer | Operator can derive a next parameter set deterministically | Typed parameter incrementer service | Operational | Planned | M7 | No Spring factory/container API | R/R/R/J/R/N | — | [Execution plan](../architecture/execution-plan.md) | Depends on operator/registry |
| DOM-STATUS-001 | 6.0.4 [domain] | Status | Batch lifecycle status is distinct from exit status | `BatchStatus` and `ExitStatus` | Semantic | Implemented | M1 | Exact vocabulary differences ledgered | R/R/R/R/J/N | [`exit_status_does_not_forge_batch_status`](../../crates/oxide-batch/tests/lifecycle_conformance/cases.rs) | [Execution semantics](execution-semantics.md) | — |
| DOM-EXIT-001 | 6.0.4 [domain] | ExitStatus | Exit status can carry flow-facing result without forging lifecycle | Typed bounded exit outcome | Semantic | Implemented | M1/M7 | String mapping/API differ | R/R/R/R/J/N | [`exit_status_does_not_forge_batch_status`](../../crates/oxide-batch/tests/lifecycle_conformance/cases.rs) | [Execution semantics](execution-semantics.md) | Advanced mapping M7 |
| LIFE-LAUNCH-001 | 6.0.4 [job] | First launch | First launch creates the complete execution graph | Repository unit of work | Behavioral | Implemented | M1/M2 | Own repository API | R/R/R/R/R/J | [`first_launch_creates_execution_graph`](../../crates/oxide-batch/tests/repository.rs), [M2 exit](../project/m2-exit-evidence.md) | [Repository model](../architecture/repository-and-transaction-model.md) | Released verification remains pending |
| LIFE-COMPLETE-001 | 6.0.4 [job] | Completed instance | Completed instance rejects a repeat launch with same identity | Typed completed-instance rejection | Behavioral | Implemented | M1 | Error type differs | R/R/R/J/R/N | [`completed_instance_rejects_launch`](../../crates/oxide-batch/tests/lifecycle_conformance/cases.rs) | [Execution semantics](execution-semantics.md) | — |
| LIFE-RESTART-001 | 6.0.4 [job] | Restart | Failed/stopped work resumes under explicit compatibility rules | New attempt from committed checkpoint | Behavioral | Implemented | M2/M7 | Stronger definition-fingerprint guard | R/R/R/R/R/J | [M2 durable restart](../project/m2-durable-restart-evidence.md), [M2 exit](../project/m2-exit-evidence.md) | [Execution semantics](execution-semantics.md) | General compiled-plan restart remains M7 |
| LIFE-NORESTART-001 | 6.0.4 [job] | Non-restartable | Non-restartable job/step rejects restart | Typed definition policy | Behavioral | Planned | M7 | Rust plan declaration | R/R/R/R/R/N | — | [Execution plan](../architecture/execution-plan.md) | — |
| LIFE-STOP-001 | 6.0.4 [metadata] | Stop | Stop request is cooperative, durable, and observable | Stop token plus operator state transition | Operational | Partial | M1/M4/M7 | Blocking limitation explicit | R/R/R/R/J/R | [`cooperative_stop_during_async_work_is_persisted`](../../crates/oxide-batch/tests/tasklet.rs) | [Execution semantics](execution-semantics.md) | Full operator stop M4/M7 |
| LIFE-ABANDON-001 | 6.0.4 [metadata] | Abandon | Operator makes an execution permanently non-restartable | Audited guarded abandon | Operational | Planned | M4/M7 | API/authorization differ | R/R/R/R/R/N | — | [Execution semantics](execution-semantics.md) | — |
| LIFE-RECOVER-001 | 6.0.4 [metadata] | Recover | Ambiguous/orphaned work requires an explicit recovery decision | Evidence-based audited recover | Operational | Partial | M2/M4 | Stricter `UNKNOWN` handling | R/R/R/R/R/N | [M2 durable restart](../project/m2-durable-restart-evidence.md) | [Execution semantics](execution-semantics.md) | Audited repository path implemented; operator service/CLI remains M4/M7 |
| LIFE-DEFINITION-001 | 6.0.4 [job] | Definition evolution | Restart runs a compatible definition | Revision/manifest/fingerprint plus directed upgrade | Native equivalent | Implemented | M2/M7 | Stronger fail-closed model; M2 upgrades preserve state bytes | R/R/R/R/R/J | [M2 durable restart](../project/m2-durable-restart-evidence.md) | [Execution plan](../architecture/execution-plan.md) | Schema-transforming and compiled-plan upgrades remain M7 |

## Step, chunk, item stream, and standard components

| ID | Source | Subcategory | Spring capability and observable semantics | OxideBatch equivalent | Parity | Status | Milestone | Known divergence | Evidence U/I/C/Cr/M/P | Evidence | Owner | Notes/dependencies |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| STEP-TASKLET-001 | 6.0.4 [step] | Tasklet | Repeated tasklet contribution drives one step lifecycle | Async Rust tasklet | Behavioral | Implemented | M1 | Async-first component API | R/R/R/R/J/R | [`successful_launch_borrows_context_and_persists_final_graph`](../../crates/oxide-batch/tests/tasklet.rs) | [Item model](../architecture/item-processing-model.md) | General plan lowering M7 |
| STEP-TASKLET-PANIC-001 | 6.0.4 [step] | User failure | User panic/error becomes a classified step failure | Panic boundary and typed redacted error | Native equivalent | Implemented | M1 | Java exception behavior differs | R/R/R/R/N/N | [`tasklet_panic_is_classified_and_runtime_remains_usable`](../../crates/oxide-batch/tests/tasklet.rs) | [Execution semantics](execution-semantics.md) | — |
| STEP-CHUNK-001 | 6.0.4 [chunk] | Chunk lifecycle | Read/process/write repeats and commits at completion boundary | Typed bounded chunk engine | Behavioral | Implemented | M2/M6 | Rust component API | R/R/R/R/R/R | [Runtime](../project/m2-chunk-runtime-evidence.md), [PostgreSQL atomicity](../project/m2-postgres-chunk-transaction-evidence.md) | [Item model](../architecture/item-processing-model.md) | Full component model M6 |
| STEP-CUSTOM-001 | 6.0.4 [api] | Custom step | User can supply a step implementation under lifecycle rules | Registered custom plan node | Feature/native | Planned | M7 | No Java interface compatibility | R/R/R/R/J/R | — | [Execution plan](../architecture/execution-plan.md) | Static/erased boundary |
| STEP-JOB-001 | 6.0.4 [step] | Nested job/JobStep | A step launches a nested job and maps its result | Nested-job plan node and lineage | Behavioral | Planned | M7 | Rust operator/definition API | R/R/R/R/R/J | — | [Execution plan](../architecture/execution-plan.md) | — |
| STEP-STARTLIMIT-001 | 6.0.4 [step-restart] | Start controls | Start limit and allow-start-if-complete affect repeat execution | Typed step restart controls | Behavioral | Planned | M3/M7 | — | R/R/R/R/R/N | — | [Basic flow](../architecture/basic-flow.md) | Basic M3 scenarios mapped by [design gate](../project/m3-design-gate-evidence.md); complete M7 |
| ITEM-READER-001 | 6.0.4 [item] | Reader | Reader returns ordered items/end and may persist state | Generic `ItemReader<I>` | Feature/behavioral | Implemented | M2/M6 | Boxed current path; static proposed | R/R/R/R/R/R | [M2 component evidence](../project/m2-component-contract-evidence.md) | [Item model](../architecture/item-processing-model.md) | Full contract M6 |
| ITEM-PROCESSOR-001 | 6.0.4 [item] | Processor | Processor transforms or filters an item | Generic `ItemProcessor<I,O>` | Feature/behavioral | Implemented | M2/M6 | Rust types and errors | R/R/R/R/J/R | [M2 component evidence](../project/m2-component-contract-evidence.md) | [Item model](../architecture/item-processing-model.md) | Native hot path M6 |
| ITEM-WRITER-001 | 6.0.4 [item] | Writer | Writer consumes one bounded chunk under transaction rules | Generic `ItemWriter<O>` and write context | Feature/behavioral | Implemented | M2/M6 | Explicit delivery capability | R/R/R/R/R/R | [M2 component evidence](../project/m2-component-contract-evidence.md) | [Item model](../architecture/item-processing-model.md) | Enlistment M2 |
| ITEM-STREAM-001 | 6.0.4 [item] | ItemStream | Open/update/close state participates in restart lifecycle | Versioned namespaced state contract | Behavioral | Planned | M2/M6 | Codec/model differs | R/R/R/R/R/R | [M2 component evidence](../project/m2-component-contract-evidence.md) | [Item model](../architecture/item-processing-model.md) | Full ordering M6 |
| ITEM-CHECKPOINT-001 | 6.0.4 [chunk] | Checkpoint | Only committed state determines resume position | Atomic checkpoint/context/counters | Behavioral | Implemented | M2 | Own metadata schema | R/R/R/R/R/R | [PostgreSQL atomicity](../project/m2-postgres-chunk-transaction-evidence.md), [durable restart](../project/m2-durable-restart-evidence.md), [M2 exit](../project/m2-exit-evidence.md) | [Execution semantics](execution-semantics.md) | Released verification remains pending |
| ITEM-COMPOSITE-001 | 6.0.4 [appendix] | Composite/delegate | Composite, classifier, and delegating components preserve ordered semantics | Typed composites and capability intersection | Feature/behavioral | Planned | M6 | Rust generic composition | R/R/R/R/R/R | — | [Item model](../architecture/item-processing-model.md) | — |
| ITEM-DECORATOR-001 | 6.0.4 [appendix] | Decorators | Peek, aggregate, validator, filter, synchronized, and thread-safe wrappers | Native decorators with explicit capabilities | Feature/behavioral | Planned | M6 | Exact catalog may map to fewer generics | R/R/R/R/R/R | — | [Item model](../architecture/item-processing-model.md) | Row expansion during M6 |
| ITEM-MULTI-001 | 6.0.4 [item] | Multi-resource | Multiple resources form one ordered restartable logical input/output | Resource identity plus versioned position | Behavioral | Planned | M6 | Resource abstraction differs | R/R/R/R/R/R | — | [Item model](../architecture/item-processing-model.md) | Object-store capability |
| IO-FLAT-001 | 6.0.4 [item] | Flat/delimited/fixed | Restartable delimited and fixed-width file I/O | File/CSV/fixed-width adapters | Feature/behavioral | Planned | M6 | Rust parsers/config | R/R/R/R/R/R | — | [Integration model](../architecture/integration-model.md) | — |
| IO-STRUCTURED-001 | 6.0.4 [item] | XML/JSON/Avro | Restartable structured record readers/writers | XML, JSON/JSONL, Avro adapters | Feature/behavioral | Planned | M6/M13 | Library and schema differences | R/R/R/R/R/R | — | [Integration model](../architecture/integration-model.md) | Support tier per format |
| IO-DB-001 | 6.0.4 [appendix] | Database components | Cursor, paging, batch, upsert, stored-procedure, ORM/repository forms | Capability-aware SQL/native repository adapters | Native equivalent | Planned | M6/M8 | No Java ORM/API compatibility | R/R/R/R/R/R | — | [Integration model](../architecture/integration-model.md) | Backend-specific rows M8 |

## Fault tolerance, repeat, listeners, flow, and scope

| ID | Source | Subcategory | Spring capability and observable semantics | OxideBatch equivalent | Parity | Status | Milestone | Known divergence | Evidence U/I/C/Cr/M/P | Evidence | Owner | Notes/dependencies |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| FT-RETRY-001 | 6.0.4 [retry-config] | Retry | Typed policy bounds attempts and persists relevant counts | Typed retry policy/cache/backoff | Behavioral | Partial | M3/M6 | Stable categories, 65,535 retry/256-key caps; a crash can replay the pre-decision initial call or consume an uninvoked reservation | R/R/R/R/R/R | [M3 runtime evidence](../project/m3-fault-runtime-evidence.md), [M3 durability evidence](../project/m3-postgres-fault-durability-evidence.md) | [Fault tolerance](../architecture/fault-tolerance.md) | Scenarios mapped by [design gate](../project/m3-design-gate-evidence.md) |
| FT-BACKOFF-001 | 6.0.4 [retry-config] | Backoff | Backoff follows policy and can be interrupted | Injected monotonic clock and cancellable backoff | Behavioral | Implemented | M3/M6 | Native time API; no M3 jitter | R/R/R/R/J/R | [M3 runtime evidence](../project/m3-fault-runtime-evidence.md) | [Fault tolerance](../architecture/fault-tolerance.md) | Scenarios mapped by [design gate](../project/m3-design-gate-evidence.md) |
| FT-SKIP-001 | 6.0.4 [skip-config] | Skip | Read/process/write skips are classified, counted, and listened to | Typed skip classifier/listeners/counters | Behavioral | Partial | M3/M6 | Stable categories and write-location proof; the shared limit spans every attempt of one instance | R/R/R/R/R/R | [M3 runtime evidence](../project/m3-fault-runtime-evidence.md), [M3 durability evidence](../project/m3-postgres-fault-durability-evidence.md) | [Fault tolerance](../architecture/fault-tolerance.md) | Scenarios mapped by [design gate](../project/m3-design-gate-evidence.md) |
| FT-ROLLBACK-001 | 6.0.4 [fault-builder-api] | Rollback/no-rollback | Classifier controls rollback without corrupting checkpoint semantics | Typed rollback capability policy | Behavioral | Partial | M3/M6 | No-rollback is capability-scoped and still records a skip; a terminal known rollback is not yet counted durably | R/R/R/R/R/R | [M3 runtime evidence](../project/m3-fault-runtime-evidence.md), [M3 durability evidence](../project/m3-postgres-fault-durability-evidence.md) | [Fault tolerance](../architecture/fault-tolerance.md) | Scenarios mapped by [design gate](../project/m3-design-gate-evidence.md) |
| REPEAT-POLICY-001 | 6.0.4 [repeat] | Completion policy | Count/time/composite completion decides chunk/repeat end | Bounded typed completion policies | Behavioral | Planned | M6/M7 | Adaptive form may exceed Spring | R/R/R/R/R/R | — | [Item model](../architecture/item-processing-model.md) | — |
| REPEAT-CONTEXT-001 | 6.0.4 [repeat] | Repeat state | Repeat context/callback/interceptor preserves nested outcomes | Typed repeat state and interceptors | Native equivalent | Planned | M7 | No Java callback API | R/R/R/R/R/J | — | [Execution plan](../architecture/execution-plan.md) | — |
| LISTENER-JOBSTEP-001 | 6.0.4 [api] | Job/step listeners | Before/after listener order and failure semantics are deterministic | Ordered async listeners | Behavioral | Implemented | M1 | Failure aggregation differs | R/R/R/R/J/N | [`listeners_nest_and_reverse_after_order`](../../crates/oxide-batch/tests/listeners.rs) | [Execution semantics](execution-semantics.md) | Differences must remain explicit |
| LISTENER-ITEM-001 | 6.0.4 [step-listeners] | Chunk/item/skip/retry | Complete callback taxonomy observes or influences named boundaries | Typed interceptors plus non-authoritative events | Behavioral | Partial | M2/M3/M6 | Rust error/panic model, typed redaction, and explicit listener delivery mode | R/R/R/R/J/R | [M3 runtime evidence](../project/m3-fault-runtime-evidence.md) | [Fault tolerance](../architecture/fault-tolerance.md) | M3 scenarios mapped by [design gate](../project/m3-design-gate-evidence.md); complete taxonomy remains M6 |
| FLOW-SEQUENCE-001 | 6.0.4 [flow-control] | Sequential/conditional | Exit outcome selects a deterministic next step | Compiled transition graph | Behavioral | Planned | M3/M7 | Rust definition syntax; ambiguous equal-specificity patterns reject | R/R/R/R/R/J | — | [Basic flow](../architecture/basic-flow.md) | Basic M3 scenarios mapped by [design gate](../project/m3-design-gate-evidence.md); complete M7 |
| FLOW-DECIDER-001 | 6.0.4 [flow-control] | Decision | A decision node maps durable inputs to flow outcome | Typed decider node and persisted trace | Behavioral | Planned | M3/M7 | Native type/API; committed result is restart authority | R/R/R/R/R/J | — | [Basic flow](../architecture/basic-flow.md) | Scenarios mapped by [design gate](../project/m3-design-gate-evidence.md) |
| FLOW-SPLIT-001 | 6.0.4 [job] | Split/parallel | Branches execute concurrently and aggregate deterministically | Structured split node | Behavioral | Planned | M7/M10 | Bounded concurrency required | R/R/R/R/R/R | — | [Execution plan](../architecture/execution-plan.md) | Local execution M10 |
| FLOW-NESTED-001 | 6.0.4 [job] | Nested flow/job | Nested composition preserves restart and outcome mapping | Nested flow/job nodes with lineage | Behavioral | Planned | M7 | Rust plan model | R/R/R/R/R/J | — | [Execution plan](../architecture/execution-plan.md) | — |
| SCOPE-JOB-001 | 6.0.4 [step] | Job scope | Component instance and late binding live for a job execution | `JobComponentFactory` and typed resolver | Native equivalent | Planned | M7 | No proxy/DI container | R/R/R/R/R/J | — | [Execution plan](../architecture/execution-plan.md) | Explicit close |
| SCOPE-STEP-001 | 6.0.4 [step] | Step scope | Component instance and late binding live for a step execution | `StepComponentFactory` and typed resolver | Native equivalent | Planned | M7 | No proxy/SpEL requirement | R/R/R/R/R/J | — | [Execution plan](../architecture/execution-plan.md) | Optional expression DSL |

## Repository, operator, testing, and observability

| ID | Source | Subcategory | Spring capability and observable semantics | OxideBatch equivalent | Parity | Status | Milestone | Known divergence | Evidence U/I/C/Cr/M/P | Evidence | Owner | Notes/dependencies |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| REPO-COMMAND-001 | 6.0.4 [metadata] | JobRepository | Repository is authoritative for identity and lifecycle writes | `JobRepository` command port | Operational/semantic | Partial | M2/M8 | Own schema and ports | R/R/R/R/R/R | [PostgreSQL evidence](../project/m2-postgres-repository-evidence.md), [durable restart](../project/m2-durable-restart-evidence.md), [M2 exit](../project/m2-exit-evidence.md) | [Repository model](../architecture/repository-and-transaction-model.md) | Service split and pagination remain accepted targets |
| REPO-EXPLORE-001 | 6.0.4 [metadata] | JobExplorer | Read-only bounded inspection of executions and instances | Paginated/streaming `JobExplorer` | Operational | Partial | M1/M4/M7 | Query API differs | R/R/R/J/R/R | [`inspection_redacts_record_contents`](../../crates/oxide-batch/tests/listeners.rs) | [Repository model](../architecture/repository-and-transaction-model.md) | Durable pagination pending |
| REPO-OPERATOR-001 | 6.0.4 [metadata] | JobOperator | Launch/restart/stop/abandon/recover through guarded service | `JobOperator` application service | Operational | Planned | M4/M7 | Explicit recover and idempotency | R/R/R/R/R/R | — | [Repository model](../architecture/repository-and-transaction-model.md) | Control-plane portable |
| REPO-REGISTRY-001 | 6.0.4 [api] | JobRegistry | Register and resolve named/versioned definitions | `DefinitionRegistry` | Native equivalent | Planned | M7 | Revision/fingerprint required | R/R/R/R/R/R | — | [Execution plan](../architecture/execution-plan.md) | — |
| REPO-RETENTION-001 | 6.0.4 [metadata] | Retention | Bounded cleanup preserves running/held work and audit | `RetentionRepository` primitives | Operational | Planned | M4/M8 | Stronger hold/audit rules | R/R/R/R/R/R | — | [Persistence](../operations/persistence-and-migrations.md) | — |
| TEST-JOB-001 | 6.0.4 [testing] | Full job | Test utility launches a complete job with controlled inputs | `oxide-batch-test` job harness | Native equivalent | Planned | M6 | No Spring test context | R/R/R/J/R/J | — | [Conformance strategy](conformance-strategy.md) | — |
| TEST-STEP-001 | 6.0.4 [testing] | Single step | Test utility launches one named step with fixture context | Plan slice/step harness | Native equivalent | Planned | M6 | No Java utility API | R/R/R/J/R/J | — | [Conformance strategy](conformance-strategy.md) | — |
| TEST-SCOPE-001 | 6.0.4 [testing] | Scoped fixture | Test constructs job/step context for scoped components | Typed context/factory fixture | Native equivalent | Planned | M6/M7 | No Spring test listener | R/R/R/J/R/N | — | [Conformance strategy](conformance-strategy.md) | — |
| TEST-REPO-001 | 6.0.4 [testing] | Repository cleanup | Test utility creates/removes isolated metadata safely | Bounded fixture and cleanup kit | Operational | Planned | M6/M8 | Adapter-owned cleanup | R/R/R/R/R/J | — | [Repository model](../architecture/repository-and-transaction-model.md) | — |
| TEST-DIST-001 | 6.0.4 [scale] | Distributed harness | Remote behavior is testable independent of production fabric | In-memory/fault-injected protocol harness | Native equivalent | Planned | M11 | Stronger transport-neutral focus | R/R/R/R/R/R | — | [Distributed execution](../architecture/distributed-execution.md) | — |
| OBS-EXEC-001 | 6.0.4 [metadata] | Execution observation | Job/step status, counts, context, and failure are inspectable | Explorer plus stable events | Operational | Implemented | M1/M4 | Redaction stricter | R/R/R/R/J/R | [`telemetry_correlates_execution`](../../crates/oxide-batch/tests/listeners.rs), [M2 exit](../project/m2-exit-evidence.md) | [Observability contract](../operations/observability-contract.md) | Durable M2 inspection complete; exporter mapping M4 |
| OBS-METRICS-001 | 6.0.4 [api] | Metrics/traces | Lifecycle, item counts, duration, and failures are observable | Vendor-neutral bounded telemetry schema | Native equivalent | Planned | M4/M10 | Metric names/API differ | R/R/R/R/J/R | — | [Observability contract](../operations/observability-contract.md) | Telemetry never authoritative |

## Local and distributed scalability

| ID | Source | Subcategory | Spring capability and observable semantics | OxideBatch equivalent | Parity | Status | Milestone | Known divergence | Evidence U/I/C/Cr/M/P | Evidence | Owner | Notes/dependencies |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| SCALE-PARSTEP-001 | 6.0.4 [scale] | Parallel steps | Independent flow branches execute concurrently and aggregate | Structured bounded split execution | Behavioral | Planned | M4/M10 | Explicit task tree/budget | R/R/R/R/J/R | — | [Execution plan](../architecture/execution-plan.md) | — |
| SCALE-MTSTEP-001 | 6.0.4 [scale] | Multi-threaded step | Item processing uses concurrent workers under thread-safety rules | Typed concurrent processor path | Behavioral | Planned | M10 | Transaction/thread model differs | R/R/R/R/J/R | — | [Item model](../architecture/item-processing-model.md) | — |
| SCALE-LOCALCHUNK-001 | 6.0.4 [scale] | Local chunking | Chunks process concurrently on one host with ordered commit rules | Local worker queue and commit barrier | Behavioral | Planned | M10 | Rust structured concurrency | R/R/R/R/J/R | — | [Item model](../architecture/item-processing-model.md) | — |
| SCALE-LOCALPART-001 | 6.0.4 [scale] | Local partitioning | Durable partitions execute in bounded local workers | Local assignment/aggregation | Behavioral | Planned | M4/M10 | Capability/fencing model | R/R/R/R/R/R | — | [Distributed execution](../architecture/distributed-execution.md) | Same semantics as remote |
| SCALE-REMOTEPART-001 | 6.0.4 [scale] | Remote partitioning | Remote workers run partitions; restart does not depend on fabric | Fenced durable assignment protocol | Behavioral | Planned | M11 | Stronger lease/fencing rules | R/R/R/R/R/R | — | [Distributed execution](../architecture/distributed-execution.md) | RFC-0009 |
| SCALE-REMOTECHUNK-001 | 6.0.4 [integration] | Remote chunking | Manager forms work; remote workers process/write with durable delivery | Versioned chunk command/result protocol | Behavioral | Planned | M11 | Broker-neutral envelope | R/R/R/R/R/R | — | [Distributed execution](../architecture/distributed-execution.md) | Delivery profile required |
| SCALE-REMOTESTEP-001 | 6.0.4 [scale] | Remote step | Whole step executes on a remote compatible worker | Remote step plan node | Behavioral | Planned | M11 | Artifact/capability verification | R/R/R/R/R/R | — | [Distributed execution](../architecture/distributed-execution.md) | Spring 6 feature |

## Database, messaging, migration, and metadata lifecycle

| ID | Source | Subcategory | Spring capability and observable semantics | OxideBatch equivalent | Parity | Status | Milestone | Known divergence | Evidence U/I/C/Cr/M/P | Evidence | Owner | Notes/dependencies |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| DB-POSTGRES-001 | 6.0.4 [schema] | PostgreSQL | Durable metadata, locking, migrations, and supported queries | Reference PostgreSQL/SQLx adapter | Native equivalent | Implemented | M2/M8 | OxideBatch-owned schema | R/R/R/R/R/R | [Repository](../project/m2-postgres-repository-evidence.md), [chunk atomicity](../project/m2-postgres-chunk-transaction-evidence.md), [durable restart](../project/m2-durable-restart-evidence.md), [M2 exit](../project/m2-exit-evidence.md) | [Persistence](../operations/persistence-and-migrations.md) | Released verification remains pending; portability remains M8 |
| DB-MYSQL-001 | 6.0.4 [schema] | MySQL/MariaDB | Supported relational metadata behavior | Certified native adapter | Native equivalent | Planned | M8 | Schema/dialect differ | R/R/R/R/R/R | — | [Persistence](../operations/persistence-and-migrations.md) | Proposed Tier 1 |
| DB-SQLITE-001 | 6.0.4 [schema] | SQLite | Supported relational metadata behavior within concurrency limits | Certified native adapter/profile | Native equivalent | Planned | M8 | Explicit concurrency limitations | R/R/R/R/R/R | — | [Persistence](../operations/persistence-and-migrations.md) | Proposed Tier 1 |
| DB-SQLSERVER-001 | 6.0.4 [schema] | SQL Server | Supported relational metadata behavior | Certified native adapter | Native equivalent | Planned | M8 | Schema/dialect differ | R/R/R/R/R/R | — | [Persistence](../operations/persistence-and-migrations.md) | Proposed Tier 1 |
| DB-ENTERPRISE-001 | 6.0.4 [schema] | Oracle/DB2/HANA | Documented relational metadata support | Certified adapter or reviewed unsupported disposition per DB | Feature | Planned | M8/M13 | CI/licensing may require external evidence | R/R/R/R/R/R | — | [Persistence](../operations/persistence-and-migrations.md) | Split into one row/DB before verification |
| DB-MONGO-001 | 6.0.4 [api] | MongoDB metadata | Documented non-relational repository capability, if present in baseline | Research/RFC or reviewed disposition | Feature | Unknown | M8 | No accepted repository design | R/R/R/R/R/R | — | [Persistence](../operations/persistence-and-migrations.md) | Cannot be omitted |
| MSG-KAFKA-001 | 6.0.4 [integration] | Kafka | Items/offsets participate in restart with documented delivery | Kafka item adapter and delivery profile | Native equivalent | Planned | M9 | Broker-native semantics | R/R/R/R/R/R | — | [Integration model](../architecture/integration-model.md) | — |
| MSG-AMQP-001 | 6.0.4 [integration] | AMQP/JMS equivalent | Message ack/redelivery and item processing are explicit | AMQP adapter; JMS concepts mapped, not Java API | Native equivalent | Planned | M9 | No JMS API/source compatibility | R/R/R/R/R/R | — | [Integration model](../architecture/integration-model.md) | — |
| MSG-OTHER-001 | 6.0.4 [api] | NATS/Pulsar/SQS/Redis/channel | Demand-tier queue/stream integrations are classified | Capability-specific adapters | Feature/native | Planned | M9/M13 | Some exceed Spring catalog | R/R/R/R/R/R | — | [Integration model](../architecture/integration-model.md) | Split before certification |
| IO-OBJECT-001 | 6.0.4 [api] | Object storage | Restartable resource read/write and publication | S3/Azure/GCS capability adapters | Native equivalent | Planned | M6/M9 | Cloud API differs | R/R/R/R/R/R | — | [Integration model](../architecture/integration-model.md) | — |
| IO-MAILLDAP-001 | 6.0.4 [appendix] | Demand-tier integrations | Documented mail/LDAP/other components receive a disposition | Adapter, certified third party, or reviewed N/A | Feature | Unknown | M13 | Rust relevance and demand unreviewed | R/R/R/R/R/R | — | [Integration model](../architecture/integration-model.md) | Cannot be silently dropped |
| META-UPGRADE-001 | 6.0.4 [schema] | Schema lifecycle | Schema initialization and upgrade preserve supported metadata | Versioned adapter migrations and compatibility windows | Operational | Implemented | M2/M8/M14 | Own schema/version; released sources are 1 and 2 with restore-only rollback | R/R/R/R/R/R | [Schema 1](../operations/migrations/0001-initial-metadata.md), [Schema 2](../operations/migrations/0002-fault-tolerance-and-flow.md), [M3 durability evidence](../project/m3-postgres-fault-durability-evidence.md) | [Persistence](../operations/persistence-and-migrations.md) | Multi-version evidence grows |
| META-CONTEXT-001 | 6.0.4 [domain] | Context evolution | Durable context can be read/upgraded or fails explicitly | Bounded versioned codecs and migrations | Native equivalent | Implemented | M2/M8 | JSON initial; other codecs proposed | R/R/R/R/R/R | [Context spike](../architecture/spikes/0003-execution-context-evolution.md) | [Persistence](../operations/persistence-and-migrations.md) | — |
| MIG-DEFINITION-001 | 6.0.4 [api] | Definition migration | Spring job structure can be analyzed and mapped | Java extractor plus neutral IR/report | Migration | Planned | M12 | Custom Java code requires manual port | R/R/R/R/R/R | — | [Migration contract](spring-batch-migration.md) | RFC-0010 |
| MIG-METADATA-001 | 6.0.4 [schema] | Metadata migration | Supported history can be exported/imported and reconciled | One-way versioned package into own schema | Migration | Planned | M12 | No live shared schema | R/R/R/R/R/R | — | [Migration contract](spring-batch-migration.md) | RFC-0010 |
| META-RETENTION-001 | 6.0.4 [metadata] | Retention/upgrade | History can be archived/purged and survives supported upgrades | Guarded retention and export primitives | Operational | Planned | M8/M14 | Stronger audit/hold model | R/R/R/R/R/R | — | [Persistence](../operations/persistence-and-migrations.md) | — |

## Status vocabulary

- `Unknown`: population exists but its semantics/disposition are not reviewed.
- `Planned`: an accepted milestone names future work.
- `Implemented`: code exists but released evidence is incomplete.
- `Verified`: required evidence passes for a named released OxideBatch version.
- `Partial`: named observations differ or only part of the row is implemented.
- `Unsupported`: intentionally not provided, with an approved rationale.
- `Deferred`: reviewed but assigned outside the current milestone.
- `NotApplicable`: Spring-specific capability has no meaningful Rust use and
  an approved native-equivalent or no-equivalent rationale.

`Unsupported` and `NotApplicable` are reviewed terminal dispositions but do
not count as supported feature parity. A complete documented coverage claim
may include them only by naming the difference; a complete behavioral parity
claim may not.

## Row and claim rules

- `Verified` requires a released OxideBatch version and every required
  evidence link. Documentation alone cannot change a row to `Verified`.
- Exact official source sections and observed versions are recorded when a
  scenario is implemented.
- A known difference classifies semantic, behavioral, feature, operational,
  migration, schema, or API impact.
- New Spring Batch baseline content is added before its OxideBatch disposition
  is decided.
- A regression of a `Verified` row is a compatibility defect and potential
  release blocker.
- M12 ledger closure requires zero `Unknown`, `Deferred`, `Planned`,
  `Implemented`, `Partial`, and untested row.

[api]: https://docs.spring.io/spring-batch/reference/api/index.html
[domain]: https://docs.spring.io/spring-batch/reference/domain.html
[job]: https://docs.spring.io/spring-batch/reference/job.html
[step]: https://docs.spring.io/spring-batch/reference/step.html
[chunk]: https://docs.spring.io/spring-batch/reference/step/chunk-oriented-processing.html
[item]: https://docs.spring.io/spring-batch/reference/readersAndWriters.html
[appendix]: https://docs.spring.io/spring-batch/reference/appendix.html
[retry]: https://docs.spring.io/spring-batch/reference/retry.html
[retry-config]: https://docs.spring.io/spring-batch/reference/step/chunk-oriented-processing/retry-logic.html
[skip-config]: https://docs.spring.io/spring-batch/reference/step/chunk-oriented-processing/configuring-skip.html
[flow-control]: https://docs.spring.io/spring-batch/reference/step/controlling-flow.html
[step-restart]: https://docs.spring.io/spring-batch/reference/step/chunk-oriented-processing/restart.html
[step-listeners]: https://docs.spring.io/spring-batch/reference/step/chunk-oriented-processing/intercepting-execution.html
[fault-builder-api]: https://docs.spring.io/spring-batch/reference/api/org/springframework/batch/core/step/builder/FaultTolerantStepBuilder.html
[repeat]: https://docs.spring.io/spring-batch/reference/repeat.html
[testing]: https://docs.spring.io/spring-batch/reference/testing.html
[metadata]: https://docs.spring.io/spring-batch/reference/job/advanced-meta-data.html
[schema]: https://docs.spring.io/spring-batch/reference/schema-appendix.html
[scale]: https://docs.spring.io/spring-batch/reference/scalability.html
[integration]: https://docs.spring.io/spring-batch/reference/spring-batch-integration.html
