# Project Risk Register

**State:** Accepted

Scale: likelihood and impact are Low, Medium, or High. Owners are roles until
additional maintainers join.

| ID | Risk | Likelihood | Impact | Owner | Mitigation and trigger |
| --- | --- | --- | --- | --- | --- |
| R-001 | “Spring Batch compatible” is interpreted as API/schema parity | High | High | Product | Publish compatibility levels; block unsupported claims in review |
| R-002 | Async trait choice makes transaction-scoped extensions unusable | Medium | High | Architecture | Complete object-safety/transaction spike before public traits |
| R-003 | Users assume exactly-once effects across external systems | High | High | Runtime | Name delivery guarantee per writer; require idempotency guidance |
| R-004 | Crash between business effects and checkpoint causes duplicates/loss | Medium | High | Repository | Atomic enlisted transaction plus exhaustive crash matrix |
| R-005 | Concurrent launches corrupt instance/execution identity | Medium | High | Repository | Database constraints, locking tests, optimistic versions |
| R-006 | Execution-context changes make jobs unrestartable | Medium | High | Core | Version format; size bounds; backward-read fixtures |
| R-007 | Blocking or panicking user code destabilizes Tokio workers | High | High | Runtime | Blocking adapter, panic boundary, resource limits, diagnostics |
| R-008 | Premature multi-crate/public API split slows design changes | Medium | Medium | Maintainer | Extract only gated boundaries; facade is the only supported contract |
| R-009 | Dependency/MSRV churn breaks enterprise builds | Medium | Medium | Release | MSRV CI, dependency review, controlled update policy |
| R-010 | Telemetry leaks job parameters or creates cardinality incidents | Medium | High | Observability | Deny-by-default fields, safe wrappers, cardinality tests |
| R-011 | PostgreSQL migrations cannot be rolled back safely | Medium | High | Repository | Forward-only policy, backup/restore drills, compatibility checks |
| R-012 | Distributed features expand scope before local correctness | High | High | Product | M11 remains gated behind local correctness and proposed RFC-0009; roadmap placement does not authorize early work |
| R-013 | One maintainer creates review and release bus-factor risk | High | High | Governance | Access inventory, recovery process, recruit before 1.0 |
| R-014 | Conformance tests accidentally copy incompatible material | Low | High | Compatibility | Use public behavior/specification; record provenance and licenses |
| R-015 | Benchmarks optimize happy path while restart degrades | Medium | Medium | Performance | Include failure/restart and metadata contention workloads |
| R-016 | Optional features produce unsupported combinations | Medium | Medium | API | Additive feature rules and feature-matrix CI |
| R-017 | Public error/API types expose replaceable dependencies | Medium | High | API | Facade-owned types and semver review |
| R-018 | Operator recovery mutates ambiguous state incorrectly | Medium | High | Operations | Guarded commands, audit events, runbooks, confirmation |
| R-019 | Internal published crates become a de facto support obligation | Medium | Medium | Release | ADR-0010 disclosure in rustdoc/README, no ledger row or support window, lockstep versions; revisit if direct dependents appear |

## Review rules

- Review this register at every milestone transition.
- A High-impact risk without an owner and mitigation blocks milestone closure.
- A triggered risk becomes an issue with severity, evidence, and containment.
- Closing a risk requires evidence or an explicit accepted residual risk; it is
  never removed merely because no incident has occurred.
