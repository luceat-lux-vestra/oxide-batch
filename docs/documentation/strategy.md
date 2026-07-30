# Documentation Strategy

**State:** Accepted

Documentation is a versioned product artifact, a release gate, and durable
decision memory. An implementation-critical decision cannot exist only in a
chat, prompt, issue, pull-request description, or report.

## Canonical ownership

| Topic | Canonical owner |
| --- | --- |
| Product scope, layers, parity meanings, and claim gates | [Vision and scope](../product/vision-and-scope.md) |
| Positioning and claim ladder | [Alternatives and positioning](../product/alternatives-and-positioning.md) |
| Milestone sequence and exit gates | [Roadmap](../roadmap.md) |
| Compatibility policy and baseline updates | [Spring Batch contract](../compatibility/spring-batch.md) |
| Feature population/status | [Feature ledger](../compatibility/conformance-matrix.md) |
| Evidence method | [Conformance strategy](../compatibility/conformance-strategy.md) |
| Lifecycle/restart/delivery semantics | [Execution semantics](../compatibility/execution-semantics.md) |
| Architecture layers and dependency direction | [Architecture overview](../architecture/overview.md) |
| Definitions and compiled plans | [Execution-plan architecture](../architecture/execution-plan.md) |
| Item components and chunk lifecycle | [Item-processing model](../architecture/item-processing-model.md) |
| M3 retry, skip, rollback, backoff, and item/retry/skip listeners | [M3 fault-tolerance contract](../architecture/fault-tolerance.md) |
| M3 sequential/conditional flow, deciders, and start controls | [M3 basic-flow contract](../architecture/basic-flow.md) |
| Repository/transaction ports and capabilities | [Repository and transaction model](../architecture/repository-and-transaction-model.md) |
| Integrations and support tiers | [Integration model](../architecture/integration-model.md) |
| Distributed execution/protocol semantics | [Distributed execution](../architecture/distributed-execution.md) |
| Core/control-plane boundary | [Control-plane boundary](../operations/control-plane-boundary.md) |
| Persistence, migration, retention operations | [Persistence and migrations](../operations/persistence-and-migrations.md) |
| Spring definition/metadata migration | [Spring migration contract](../compatibility/spring-batch-migration.md) |
| Context codec/schema lifecycle and external state blobs | [Persistence and migrations](../operations/persistence-and-migrations.md) |
| Time, ID, error, cancellation, and structured-concurrency semantics | [Execution semantics](../compatibility/execution-semantics.md) |
| Lifecycle hooks, interceptors, and component state | [Item-processing model](../architecture/item-processing-model.md) |
| Extension modes and adapter support tiers | [Integration model](../architecture/integration-model.md) |
| Performance/capacity evidence | [Performance plan](../engineering/performance-plan.md) |
| Release and support behavior | [Release/support policy](../release/support-policy.md) |

The [post-M5 strategy](../project/post-m5-full-parity-strategy.md) is an
umbrella rationale and map. It is not the sole normative owner of any detailed
topic.

## Authority and precedence

Use this order when documents disagree:

1. an accepted ADR for its exact architecture decision;
2. an accepted normative policy/specification for its owned topic;
3. accepted RFC decision and conditions;
4. active ledger/register status and approved milestone gate evidence;
5. roadmap sequencing;
6. code and executable tests as evidence of current implementation, not
   authority to contradict an accepted contract;
7. examples and user guides;
8. proposed RFCs/documents and strategic rationale;
9. issues, pull-request text, chat, and session instructions.

An accepted ADR is changed only by a superseding ADR. A proposed document
cannot override an accepted one. When equal-authority owners overlap, stop the
dependent implementation, identify the intended single owner, and submit an
RFC/ADR or documentation correction.

Historical gates describe what was accepted and evidenced at that time. A
later proposal links to them rather than rewriting their record.

## Document states

Use the repository vocabulary in [the index](../README.md): `Accepted`,
`Proposed`, `Active`, `Template`, `Draft`, `Superseded`, plus decision-process
terminal states where their index defines them. A mixed document must label
accepted current rules and proposed extensions explicitly.

No author or agent may infer acceptance from a merged proposal, roadmap entry,
implemented experiment, or passing test. Approval follows the RFC/ADR process.

## Change requirements

Every implementation pull request updates, when applicable:

- affected feature-ledger rows, status, milestone, divergence, and evidence
  links;
- canonical public API/reference and examples;
- restart, transaction, delivery, migration, security, and operator behavior;
- support matrix, release note, migration guide, or runbook;
- milestone exit evidence.

Changes to accepted scope, compatibility guarantees, public API, durable
metadata, dependency direction, distributed protocol, or release policy require
an RFC/ADR before dependent implementation. The final repository state must be
understandable without prior chat history.

## Information architecture

| Kind | User question |
| --- | --- |
| Tutorial | Can you help me learn by doing? |
| How-to | How do I complete a specific task safely? |
| Reference | What exactly is supported and guaranteed? |
| Explanation | Why does the architecture or behavior work this way? |
| Operations | How do I deploy, upgrade, recover, and inspect it? |
| Contributor | How do I change it and prove the change? |

Focused documents link to canonical owners instead of copying full contracts.
One topic has one normative owner.

## Review and freshness

Documentation review asks:

- Is state, audience, version, and prerequisite explicit?
- Does a normative statement live in the canonical owner?
- Are current, planned, proposed, deferred, and unsupported behavior distinct?
- Do claims match released ledger and support evidence?
- Are limits, failures, recovery, cleanup, and sensitive-data behavior explicit?
- Do internal links resolve and examples compile?
- Has the relevant source/baseline version changed?

Canonical product, compatibility, architecture, roadmap, support, and
operations documents are reviewed at every milestone transition and release
candidate. Feature-ledger sources are reviewed on every Spring baseline update.
An owner records the review date when freshness is material.

## Quality rules

- Public APIs have sufficient rustdoc for safe use.
- Examples are compiled/tested where practical and version-matched.
- Links and snippets are checked automatically.
- RFC 2119 terms are reserved for actual normative requirements.
- Restart, data-loss, delivery, security, and destructive-operation warnings
  are explicit.
- No real credentials, production data, or private incident details appear.
- Screenshots are avoided when searchable/versioned text is more maintainable.
