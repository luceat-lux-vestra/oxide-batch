# Project Preparation Master Plan

> This is the historical M0 preparation record. Its M0-M5 and original 1.0
> scope statements remain evidence of the 2026-07-29 gate, not the current
> post-M5 decision. See
> [RFC-0001](../rfcs/0001-m5-preview-and-project-wide-1-0.md).

**State:** Accepted

**M0 status:** Complete as of 2026-07-29

This is the authoritative checklist for work that must be considered before
OxideBatch runtime implementation starts. The delivery roadmap describes
capabilities; this plan describes project readiness.

Status vocabulary:

- **Done:** implemented and verified in the repository or GitHub.
- **Draft:** a concrete proposal exists but is not yet accepted.
- **Todo:** no approved artifact or enforcement exists.
- **Deferred:** intentionally outside M0 with a named later gate.

## M0.1 — Identity, ownership, and repository

| Deliverable | Status | Evidence or remaining action |
| --- | --- | --- |
| Product, repository, directory, package, and crate naming | Done | `OxideBatch`, `oxide-batch`, `oxide_batch` |
| Public GitHub repository and Apache-2.0 license | Done | `LICENSE`, `NOTICE`, repository settings |
| crates.io ownership and trusted publishing | Done | `oxide-batch` alpha release and OIDC workflow |
| Trunk-based branching and PR-only changes | Done | Repository policy and main ruleset |
| Required CI checks and squash merge | Done | `quality`, `dependency-review`, `msrv`, and `supply-chain` |
| CODEOWNERS and maintainer bypass rule | Done | `.github/CODEOWNERS`, repository policy |
| Issue forms, PR template, labels, and Discussions | Done | `.github`, GitHub settings |
| Secret scanning, push protection, private reporting | Done | GitHub security settings |
| Commit signing policy and release-tag signing enforcement | Deferred | Decide before first runtime release |
| Trademark/name-usage policy | Deferred | Required before third-party distribution or a project logo |
| Domain/website and social handles | Deferred | Reassess before M4 documentation launch |

## M0.2 — Product charter and requirements

| Deliverable | Status | Evidence or remaining action |
| --- | --- | --- |
| Vision, target users, and jobs-to-be-done | Done | Accepted product vision |
| 1.0 scope and explicit non-goals | Done | Accepted product vision and roadmap |
| Representative use-case catalog | Done | Eight accepted reference use cases |
| Functional capability map | Done | Accepted roadmap and use-case milestone coverage |
| Non-functional requirements | Done | Accepted NFR priorities; milestone-specific budgets require implementation evidence |
| Supported deployment archetypes | Done | One-shot, resident worker, and containerized job boundaries |
| Configuration and secret ownership boundaries | Done | Accepted configuration model and system context |
| Success metrics and adoption signals | Done | Initial measures accepted; numeric adoption targets are release-planning inputs |
| Competitive/alternative analysis | Done | Accepted build/use alternatives and positioning constraints |
| 1.0 exclusion and scope-change procedure | Done | Accepted RFC process and roadmap |

## M0.3 — Compatibility and domain contract

| Deliverable | Status | Evidence or remaining action |
| --- | --- | --- |
| Normative Spring Batch reference line | Done | Spring Batch 6.0 accepted |
| Compatibility vocabulary and claim rules | Done | Semantic, behavioral, operational, schema, and API rules accepted |
| Core glossary | Done | Accepted domain glossary |
| Job/step lifecycle state machines | Done | Accepted lifecycle and failure/listener transitions |
| Job-instance parameter identity rules | Done | Canonical typed-value identity contract accepted; encoding is an M1 implementation detail |
| Batch status versus exit status | Done | Accepted compatibility contract |
| Restart, stop, abandon, and recovery semantics | Done | Accepted execution semantics |
| Chunk/checkpoint transaction contract | Done | Accepted contract with PostgreSQL spike evidence |
| Retry, skip, rollback, and backoff contract | Done | Policy boundary accepted; implementation belongs to M3 |
| Listener ordering and failure behavior | Done | Accepted nesting, ordering, and typed failure rule |
| Job-definition versioning and restart compatibility | Deferred | Required before M2 |
| Compatibility matrix format and initial scenarios | Done | Reviewed M1–M4 conformance rows and stable scenario IDs |
| Spring Batch reference test harness strategy | Done | Accepted clean-room pinned reference runner |
| Metadata import/interoperability position | Done | No shared live schema; import is outside the 1.0 promise |

## M0.4 — Architecture and technology

| Deliverable | Status | Evidence or remaining action |
| --- | --- | --- |
| System context and deployment diagrams | Done | Accepted context and deployment archetypes |
| Crate/module boundaries and dependency direction | Done | Accepted architecture overview and ADR-0001 |
| Public facade and publication boundaries | Done | ADR-0001 and crate policy |
| Async/sync execution model | Done | Accepted ADR-0002 and spike 0001 |
| Blocking and CPU-bound work isolation | Done | Bounded adapter and late-stop contract in spike 0001 |
| Cancellation, panic, and shutdown model | Done | Accepted ADR-0002; M1 deadlines remain implementation policy |
| Repository port and unit-of-work boundary | Done | Borrowed port and atomic enlistment in spike 0002 |
| PostgreSQL/SQLx selection | Done | Accepted ADR-0003 and PostgreSQL 18.4 evidence |
| Metadata logical and physical model | Done | Accepted PostgreSQL physical model and immutable schema-v1 migration |
| Execution-context format and evolution | Done | Versioned JSON and backward-read fixtures in spike 0003 |
| Configuration model and precedence | Done | Typed library configuration and CLI precedence boundaries accepted |
| Error taxonomy and stability contract | Done | Accepted API design guidelines |
| Feature-flag policy | Done | Additive feature rules accepted; concrete matrix starts with the first feature |
| Telemetry abstraction and event vocabulary | Done | Accepted observability contract |
| Extension/plugin model | Deferred | Static Rust traits first; dynamic ABI post-1.0 |
| Distributed coordination model | Deferred | Post-1.0 unless promoted through RFC |

## M0.5 — Developer experience and coding system

| Deliverable | Status | Evidence or remaining action |
| --- | --- | --- |
| Toolchain, edition, and MSRV | Done | Stable 1.97.1; MSRV 1.95; both required CI |
| Local prerequisite and setup guide | Done | Reproducible development environment document |
| One-command bootstrap/check/test workflow | Done | `cargo xtask doctor/check/package` |
| Local PostgreSQL test environment | Done | Disposable container script plus PostgreSQL CI service |
| Editor defaults and line endings | Done | `.editorconfig`, `.gitattributes` |
| Formatting configuration | Done | `rustfmt.toml` plus editor defaults |
| Workspace lint policy | Done | Unsafe/panic/unwrap policies in root manifest |
| Detailed Rust API design rules | Done | Accepted API guidelines and coding conventions |
| Error-handling and panic policy | Done | Accepted classifications, panic boundary, and context rules |
| Logging and sensitive-data coding rules | Done | Accepted threat model and testable disclosure table |
| Dependency introduction/review process | Done | Accepted dependency and license policy |
| Feature matrix development commands | Done | Facade-only and all-feature checks plus PostgreSQL adapter matrix |
| Generated-code and migration conventions | Done | Contiguous immutable SQL migrations with checksums and upgrade evidence |
| Examples/fixtures naming and data policy | Done | Accepted conformance fixture layout and provenance policy |

## M0.6 — Verification and quality engineering

| Deliverable | Status | Evidence or remaining action |
| --- | --- | --- |
| Test pyramid and contract-test strategy | Done | Accepted test strategy |
| Deterministic time/ID/random/backoff strategy | Done | Accepted test strategy and M1 support layout |
| State-machine/property testing policy | Done | M1 gate and harness location accepted |
| PostgreSQL version test matrix | Done | Oldest/newest-major rule accepted; exact majors are selected before M2 implementation |
| OS/architecture support matrix | Done | Explicit M1 primary, development, and candidate targets |
| Feature-combination test matrix | Deferred | Introduce with first optional feature |
| Failure-injection/crash matrix | Done | Named scenarios with metadata, replay, counters, outcome, and operator action |
| Conformance suite structure | Done | Matrix IDs, harness layout, normalized runners, and evidence states |
| Coverage tooling and policy | Done | Source coverage plus scenario evidence policy accepted |
| Mutation testing policy | Deferred | Evaluate after stable core logic exists |
| Concurrency model checking | Done | Loom evaluation gate accepted |
| Fuzzing targets and corpus policy | Done | Initial target classes and corpus policy accepted |
| Performance workload definitions | Done | Nine named workloads accepted |
| Benchmark hardware/result protocol | Done | Reproducibility and variance requirements accepted |
| Soak and leak-test policy | Deferred | Required by M4 |
| Flaky-test quarantine and ownership | Done | Owner, issue, and expiry policy accepted |

## M0.7 — CI, security, and supply chain

| Deliverable | Status | Evidence or remaining action |
| --- | --- | --- |
| Format, Clippy, unit, doc CI | Done | Required `quality` job |
| Pull-request dependency review | Done | Severity and license gate |
| Explicit MSRV CI | Done | Dedicated Rust 1.95 check |
| Stable feature matrix CI | Done | No-default facade and all-feature workspace checks |
| Real PostgreSQL integration CI | Done | Dedicated PostgreSQL 18 spike job |
| Cross-platform CI | Deferred | Required before first public runtime API |
| RustSec and dependency policy CI | Done | Scheduled and PR `cargo-deny` checks |
| License policy file and exception process | Done | `deny.toml` and documented exception rules |
| Code coverage artifact and trend | Deferred | Add after executable code exists |
| SemVer API compatibility check | Deferred | Required once a public runtime API is released |
| Documentation link/example validation | Deferred | Add with first substantive user docs |
| Scheduled deep test workflow | Deferred | Stable-only checks when relevant; no beta/nightly CI |
| SBOM generation | Deferred | Select and add before first beta |
| Artifact attestations/provenance | Deferred | Required before first stable release |
| Reproducible package-content verification | Done | `cargo xtask package` dry-run and package file-list verification |
| CI permissions and action pinning | Done | Read-only defaults and immutable SHAs |
| Dependency update and exception SLA | Done | Severity-based response and 90-day exception expiry accepted |

## M0.8 — Data, operations, and observability

| Deliverable | Status | Evidence or remaining action |
| --- | --- | --- |
| Metadata retention and purge model | Done | Eligibility, audit, and safety rules accepted |
| Database roles and least-privilege model | Done | Migrator, runtime, and operator role boundaries accepted |
| Backup/restore and migration runbook | Done | Policy, template, and PostgreSQL 15–18 logical restore fixture |
| RPO/RTO vocabulary and responsibility | Done | Framework capability versus deployment promise accepted |
| Connection-pool and timeout policy | Done | Facade-owned bounded PostgreSQL configuration and adapter enforcement |
| Stale execution detection/recovery runbook | Deferred | Required by M4 |
| CLI command, exit-code, and confirmation conventions | Deferred | Required before M4 CLI implementation |
| Configuration precedence and validation | Done | Typed library configuration and CLI precedence accepted |
| Stable log event and field catalog | Done | Initial events, safe fields, and deny-by-default rule accepted |
| Metric name/unit/cardinality catalog | Done | Candidate families and forbidden labels accepted |
| Trace/span hierarchy and propagation | Done | Job, step, chunk, and retry hierarchy accepted |
| Health/readiness semantics | Deferred | Only if a resident worker/service is introduced |
| Capacity planning and backpressure guidance | Done | NFR and performance capacity model accepted |
| Incident diagnostic bundle policy | Deferred | Required by M4 |

## M0.9 — Release, support, and documentation

| Deliverable | Status | Evidence or remaining action |
| --- | --- | --- |
| SemVer, prerelease, and coordinated-crate policy | Done | Accepted release/support and crate-publishing policies |
| Changelog format and release-note ownership | Done | Accepted changelog format and release checklist |
| Release checklist and rollback procedure | Done | Accepted prepare/build/publish/verify/emergency steps |
| Metadata schema version and migration policy | Done | Accepted ADR-0003 and support policy |
| Public API deprecation policy | Done | Pre-1.0 policy accepted; stable details remain an M5 gate |
| Supported release/backport window | Deferred | Release owner; final stable commitment at M5 |
| Platform/PostgreSQL/Rust support matrix | Done | M1 matrix explicit; exact later-milestone versions remain gated |
| API documentation information architecture | Done | Tutorials, how-to, reference, and explanation structure accepted |
| Contributor/developer guide | Done | Bootstrap, process docs, and runnable `cargo xtask` tooling |
| User quickstart and examples plan | Deferred | Required with M1 user API |
| Operator guide and runbook plan | Deferred | Required by M4 |
| Migration guide format | Done | Accepted metadata migration-guide template |
| Documentation versioning and archival | Done | Crate/release-line policy accepted |
| Release announcement and communication channels | Deferred | Finalize before first beta |

## M0.10 — Governance, planning, and risk

| Deliverable | Status | Evidence or remaining action |
| --- | --- | --- |
| Governance, conduct, support, and security policies | Done | Root policy files |
| Maintainer roles and succession | Done | Single-maintainer reality and later gates accepted |
| RFC and ADR decision procedure | Done | Templates and lifecycle accepted |
| Definition of ready and definition of done | Done | Accepted development process |
| Issue lifecycle and triage cadence | Done | Transition and cadence rules accepted |
| Milestones M0–M5 and exit gates | Done | GitHub milestones and roadmap |
| Risk register with owners and triggers | Done | Eighteen accepted risks with role owners and triggers |
| Decision register and unresolved-question log | Done | All M0 decisions accepted or explicitly deferred |
| Architecture-spike template and evidence retention | Done | Reproducible template and three retained M0 spikes |
| Severity classification and response targets | Done | P0–P3 response objectives accepted |
| Bus-factor mitigation | Done | Access inventory and pre-release/1.0 gates accepted |
| Community roadmap and contribution ladder | Deferred | Establish before wider contributor recruitment |
| Funding/sponsorship/legal entity | Deferred | Reassess when maintenance burden warrants |

## M0 exit gate

Runtime implementation may begin when:

1. every M0 item marked **Todo** is either completed or explicitly deferred with
   rationale, milestone, and owner;
2. product, compatibility, execution semantics, and public architecture
   decisions are accepted;
3. the required architecture spikes have reproducible evidence;
4. CI enforces MSRV, dependencies/licenses, and the checks needed by M1;
5. the first vertical slice maps each acceptance criterion to a test;
6. no unresolved P0/P1 risk can invalidate the M1 public contract.

This gate does not require M2–M5 implementation details to be frozen. It does
require their capability boundaries, prerequisites, and exit criteria to remain
visible.

## Deferred ownership

Deferred items remain owned even when no individual assignee is named:

- **Release owner:** commit/tag signing, release windows, API compatibility,
  SBOM, provenance, announcements, and stable support decisions;
- **Repository owner:** metadata model, definition compatibility, migrations,
  pool/timeouts, backup/restore procedures, and recovery implementation;
- **Runtime/API owner:** optional-feature matrices, coverage/model-checking
  tooling, public API checks, and dynamic extension evaluation;
- **Operations owner:** CLI safeguards, stale-execution recovery, health,
  diagnostic bundles, operator guides, and soak/leak evidence;
- **Product/governance owner:** name usage, community ladder, continuity,
  funding, and post-1.0 distributed scope;
- **Documentation owner:** website, quickstart, migration guides, link/example
  validation, and versioned publication.

The milestone or release gate named in each row is the due point. If an item is
promoted earlier, the promoting RFC or issue must name its individual owner.
