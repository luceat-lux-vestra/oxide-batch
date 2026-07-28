# Project Preparation Master Plan

**State:** Proposed

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
| Required CI checks and squash merge | Done | `quality`, `dependency-review` |
| CODEOWNERS and maintainer bypass rule | Done | `.github/CODEOWNERS`, repository policy |
| Issue forms, PR template, labels, and Discussions | Done | `.github`, GitHub settings |
| Secret scanning, push protection, private reporting | Done | GitHub security settings |
| Commit signing policy and release-tag signing enforcement | Deferred | Decide before first runtime release |
| Trademark/name-usage policy | Deferred | Required before third-party distribution or a project logo |
| Domain/website and social handles | Deferred | Reassess before M4 documentation launch |

## M0.2 — Product charter and requirements

| Deliverable | Status | Evidence or remaining action |
| --- | --- | --- |
| Vision, target users, and jobs-to-be-done | Draft | Product vision document |
| 1.0 scope and explicit non-goals | Draft | Product vision and roadmap |
| Representative use-case catalog | Draft | Eight named reference use cases |
| Functional capability map | Draft | Roadmap and use-case milestone coverage |
| Non-functional requirements | Draft | NFR document; numeric budgets need approval/evidence |
| Supported deployment archetypes | Draft | One-shot, resident worker, containerized job |
| Configuration and secret ownership boundaries | Draft | Configuration model and system context |
| Success metrics and adoption signals | Draft | Initial success measures; numeric adoption targets later |
| Competitive/alternative analysis | Draft | Build/use alternatives and positioning constraints |
| 1.0 exclusion and scope-change procedure | Draft | RFC process and roadmap |

## M0.3 — Compatibility and domain contract

| Deliverable | Status | Evidence or remaining action |
| --- | --- | --- |
| Normative Spring Batch reference line | Draft | Proposed Spring Batch 6.0 |
| Compatibility vocabulary and claim rules | Draft | Semantic, behavioral, operational, schema, API |
| Core glossary | Draft | Standalone domain glossary |
| Job/step lifecycle state machines | Draft | Validate all failure/listener transitions |
| Job-instance parameter identity rules | Draft | Define canonical encoding and hashing |
| Batch status versus exit status | Draft | Compatibility contract |
| Restart, stop, abandon, and recovery semantics | Draft | Execution semantics |
| Chunk/checkpoint transaction contract | Draft | Validate through PostgreSQL spike |
| Retry, skip, rollback, and backoff contract | Draft | Complete before M3 |
| Listener ordering and failure behavior | Draft | Proposed nesting/order and failure rule |
| Job-definition versioning and restart compatibility | Deferred | Required before M2 |
| Compatibility matrix format and initial scenarios | Draft | Initial planned conformance rows |
| Spring Batch reference test harness strategy | Draft | Clean-room pinned reference runner |
| Metadata import/interoperability position | Draft | No shared live schema; import deferred |

## M0.4 — Architecture and technology

| Deliverable | Status | Evidence or remaining action |
| --- | --- | --- |
| System context and deployment diagrams | Draft | Context and initial deployment archetypes |
| Crate/module boundaries and dependency direction | Draft | Architecture overview and ADR-0001 |
| Public facade and publication boundaries | Done | ADR-0001 and crate policy |
| Async/sync execution model | Draft | ADR-0002 plus object-safety spike |
| Blocking and CPU-bound work isolation | Todo | Spike and resource policy |
| Cancellation, panic, and shutdown model | Draft | Specify runtime ownership and deadlines |
| Repository port and unit-of-work boundary | Todo | Transaction-enlistment spike |
| PostgreSQL/SQLx selection | Draft | ADR-0003 plus real-database evidence |
| Metadata logical and physical model | Deferred | Required before M2 implementation |
| Execution-context format and evolution | Draft | JSON proposal; compatibility spike |
| Configuration model and precedence | Draft | Typed library configuration and proposed CLI precedence |
| Error taxonomy and stability contract | Draft | API design guidelines |
| Feature-flag policy | Draft | Additive feature rules; concrete matrix later |
| Telemetry abstraction and event vocabulary | Draft | Observability contract |
| Extension/plugin model | Deferred | Static Rust traits first; dynamic ABI post-1.0 |
| Distributed coordination model | Deferred | Post-1.0 unless promoted through RFC |

## M0.5 — Developer experience and coding system

| Deliverable | Status | Evidence or remaining action |
| --- | --- | --- |
| Toolchain, edition, and MSRV | Done | Stable 1.97.1; MSRV 1.95; both required CI |
| Local prerequisite and setup guide | Draft | Development environment document |
| One-command bootstrap/check/test workflow | Done | `cargo xtask doctor/check/package` |
| Local PostgreSQL test environment | Deferred | Required before PostgreSQL spike/M2 |
| Editor defaults and line endings | Done | `.editorconfig`, `.gitattributes` |
| Formatting configuration | Done | `rustfmt.toml` plus editor defaults |
| Workspace lint policy | Done | Unsafe/panic/unwrap policies in root manifest |
| Detailed Rust API design rules | Draft | API guidelines and coding conventions |
| Error-handling and panic policy | Draft | Expand classifications and context rules |
| Logging and sensitive-data coding rules | Draft | Threat model; add examples and review checklist |
| Dependency introduction/review process | Draft | Dependency and license policy |
| Feature matrix development commands | Deferred | Required when first optional feature appears |
| Generated-code and migration conventions | Deferred | Required before M2 migrations |
| Examples/fixtures naming and data policy | Draft | Conformance fixture/data policy |

## M0.6 — Verification and quality engineering

| Deliverable | Status | Evidence or remaining action |
| --- | --- | --- |
| Test pyramid and contract-test strategy | Draft | Test strategy |
| Deterministic time/ID/random/backoff strategy | Draft | Test strategy |
| State-machine/property testing policy | Draft | Select concrete harness during M1 |
| PostgreSQL version test matrix | Draft | Dimensions defined; exact majors not selected |
| OS/architecture support matrix | Draft | Candidate matrix; exact support not selected |
| Feature-combination test matrix | Deferred | Introduce with first optional feature |
| Failure-injection/crash matrix | Draft | First vertical slice and test strategy |
| Conformance suite structure | Draft | Matrix IDs, normalized runners, evidence states |
| Coverage tooling and policy | Draft | Source coverage plus scenario evidence |
| Mutation testing policy | Deferred | Evaluate after stable core logic exists |
| Concurrency model checking | Draft | Loom evaluation gate documented |
| Fuzzing targets and corpus policy | Draft | Initial target classes documented |
| Performance workload definitions | Draft | Nine named workloads |
| Benchmark hardware/result protocol | Draft | Reproducibility and variance requirements |
| Soak and leak-test policy | Deferred | Required by M4 |
| Flaky-test quarantine and ownership | Draft | Owner/issue/expiry policy |

## M0.7 — CI, security, and supply chain

| Deliverable | Status | Evidence or remaining action |
| --- | --- | --- |
| Format, Clippy, unit, doc CI | Done | Required `quality` job |
| Pull-request dependency review | Done | Severity and license gate |
| Explicit MSRV CI | Done | Dedicated Rust 1.95 check |
| Stable feature matrix CI | Deferred | Add with first optional feature |
| Real PostgreSQL integration CI | Deferred | Add with repository spike/M2 |
| Cross-platform CI | Deferred | Required before first public runtime API |
| RustSec and dependency policy CI | Done | Scheduled and PR `cargo-deny` checks |
| License policy file and exception process | Done | `deny.toml` and documented exception rules |
| Code coverage artifact and trend | Deferred | Add after executable code exists |
| SemVer API compatibility check | Deferred | Required once a public runtime API is released |
| Documentation link/example validation | Deferred | Add with first substantive user docs |
| Scheduled deep test workflow | Deferred | Stable-only checks when relevant; no beta/nightly CI |
| SBOM generation | Deferred | Select and add before first beta |
| Artifact attestations/provenance | Deferred | Required before first stable release |
| Reproducible package-content verification | Draft | Dry run exists; record package manifest/checksum |
| CI permissions and action pinning | Done | Read-only defaults and immutable SHAs |
| Dependency update and exception SLA | Draft | Severity-based response and 90-day exception expiry |

## M0.8 — Data, operations, and observability

| Deliverable | Status | Evidence or remaining action |
| --- | --- | --- |
| Metadata retention and purge model | Draft | Eligibility, audit, and safety rules |
| Database roles and least-privilege model | Draft | Migrator/runtime/operator roles |
| Backup/restore and migration runbook | Draft | Policy exists; runnable procedure required by M2 |
| RPO/RTO vocabulary and responsibility | Draft | Framework capability versus deployment promise |
| Connection-pool and timeout policy | Deferred | Required before M2 |
| Stale execution detection/recovery runbook | Deferred | Required by M4 |
| CLI command, exit-code, and confirmation conventions | Deferred | Required before M4 CLI implementation |
| Configuration precedence and validation | Draft | Typed library config and CLI precedence |
| Stable log event and field catalog | Draft | Initial events and safe fields |
| Metric name/unit/cardinality catalog | Draft | Candidate families and forbidden labels |
| Trace/span hierarchy and propagation | Draft | Job/step/chunk/retry hierarchy |
| Health/readiness semantics | Deferred | Only if a resident worker/service is introduced |
| Capacity planning and backpressure guidance | Draft | NFR and performance capacity model |
| Incident diagnostic bundle policy | Deferred | Required by M4 |

## M0.9 — Release, support, and documentation

| Deliverable | Status | Evidence or remaining action |
| --- | --- | --- |
| SemVer, prerelease, and coordinated-crate policy | Draft | Release/support and crate-publishing policies |
| Changelog format and release-note ownership | Draft | `CHANGELOG.md`; checklist needed |
| Release checklist and rollback procedure | Draft | Prepare/build/publish/verify/emergency steps |
| Metadata schema version and migration policy | Draft | ADR-0003 and support policy |
| Public API deprecation policy | Draft | Finalize before 1.0 RC |
| Supported release/backport window | Draft | Final stable commitment deferred to M5 |
| Platform/PostgreSQL/Rust support matrix | Draft | Template and candidate 1.0 dimensions |
| API documentation information architecture | Draft | Tutorials/how-to/reference/explanation |
| Contributor/developer guide | Draft | Bootstrap and process docs; runnable tooling pending |
| User quickstart and examples plan | Deferred | Required with M1 user API |
| Operator guide and runbook plan | Deferred | Required by M4 |
| Migration guide format | Deferred | Required before first schema/API migration |
| Documentation versioning and archival | Draft | Match crate/release lines |
| Release announcement and communication channels | Deferred | Finalize before first beta |

## M0.10 — Governance, planning, and risk

| Deliverable | Status | Evidence or remaining action |
| --- | --- | --- |
| Governance, conduct, support, and security policies | Done | Root policy files |
| Maintainer roles and succession | Draft | Single-maintainer reality documented |
| RFC and ADR decision procedure | Draft | Templates and lifecycle documented |
| Definition of ready and definition of done | Draft | Development process |
| Issue lifecycle and triage cadence | Draft | Transition and cadence rules |
| Milestones M0–M5 and exit gates | Done | GitHub milestones and roadmap |
| Risk register with owners and triggers | Draft | Eighteen initial risks |
| Decision register and unresolved-question log | Draft | M0 decision register |
| Architecture-spike template and evidence retention | Draft | Reproducible spike template |
| Severity classification and response targets | Draft | P0–P3 response objectives |
| Bus-factor mitigation | Draft | Access inventory and pre-release/1.0 gates |
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
