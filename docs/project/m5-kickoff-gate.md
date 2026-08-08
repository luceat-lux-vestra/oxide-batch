# M5 Embedded Core Production Preview Kickoff Gate

**State:** Active (2026-08-03)

**Umbrella:** GitHub issue
[#12](https://github.com/luceat-lux-vestra/oxide-batch/issues/12)

**Kickoff tracking:** GitHub issue
[#95](https://github.com/luceat-lux-vestra/oxide-batch/issues/95)

This record turns the accepted M5 roadmap outcome into definition-ready work.
M5 is active, but implementation may cross a named decision boundary only
after that boundary's gate below is closed.

M5 is a stabilization and evidence milestone over the delivered M0-M4 embedded
scope rather than a new-capability milestone. It is also the first milestone
that may promote advertised embedded-kernel ledger rows to `Verified`, which
the compatibility contract permits only against a named released OxideBatch
version with every required evidence link.

[RFC-0001](../rfcs/0001-m5-preview-and-project-wide-1-0.md) redefined M5 as
the Embedded Core Production Preview and reserved project-wide 1.0/GA for M14.
The GitHub umbrella and milestone carried the superseded "Enterprise Readiness
and 1.0" naming until 2026-08-03 and were retitled to match the roadmap.
Enterprise readiness and the 1.0/GA release are M14 scope and are excluded
here.

## Satisfied prerequisites

- [x] M4 is complete through issues #74-#81 and merged pull requests #82-#94,
      including its operator/explorer, CLI, shutdown/recovery, telemetry,
      retention, bounded local-scale, PostgreSQL 15/18, process-kill,
      migration, and conformance gates, recorded in the
      [M4 exit evidence](m4-exit-evidence.md).
- [x] M2 durability and M3 fault-tolerance/flow are complete through issues
      #38-#45 and #58-#65 and merged pull requests #57 and #73, recorded in
      the [M2 exit evidence](m2-exit-evidence.md) and
      [M3 exit evidence](m3-exit-evidence.md).
- [x] Durable metadata, checkpoints, fault-policy state, counters, flow
      decisions, optimistic versions, partition aggregation, and recovery
      evidence preserve their accepted atomic and unknown-commit boundaries
      across M2-M4.
- [x] RFC-0001, RFC-0003, RFC-0004, RFC-0007, and RFC-0010, with ADR-0001,
      ADR-0004, ADR-0005, ADR-0006, and ADR-0007, accept the preview
      boundary, target workspace boundaries, compiled-plan and fingerprinting
      direction, repository-service and capability model, and metadata
      migration direction.
- [x] Bounded telemetry, capacity budgets, cancellation-latency, and soak
      measurements exist as provisional hypotheses under the
      [performance plan](../engineering/performance-plan.md) regression
      gates.

The M4 dependency on issue #12 is therefore resolved. M0-M4 feature rows
remain unreleased `Implemented` or `Partial` evidence; completing M4 does not
imply an M5 preview claim, and no row is `Verified` at the time this gate
opens.

## Impact classification

M5 changes the supportability, stability, and disclosure boundary rather than
observable batch semantics. It stabilizes the compiled-plan and definition
fingerprint path, decides the static/erased component boundary, validates
staged crate extraction behind the facade, fixes the context-codec and
transaction-capability direction, reviews the public facade against the
M6-M12 target, and promotes advertised rows to released `Verified`.

These are public-API, compatibility, ledger-claim, packaging, support-policy,
and release changes, plus durable-format and restart-identity changes wherever
fingerprinting or context codecs are stabilized. M5 does not change
cross-resource delivery guarantees, add remote execution, add database
backends, or authorize a project-wide readiness claim.

Refactoring, extraction, and stabilization work must not change persisted
bytes, transaction boundaries, lifecycle writes, restart selection,
fingerprints, or normalized traces except through a gate that explicitly
accepts the change with migration evidence.

## Decisions required before dependent implementation

| Gate | Owner | Required decision and evidence | Blocks |
| --- | --- | --- | --- |
| Compiled plan and definition fingerprint | Plan/runtime owners | Canonical restart-relevant manifest, fingerprint input set and stability rules, revision/fingerprint compatibility edges, fail-closed drift detection, and the delivered subset boundary before M7 general compiled-plan restart | Plan stabilization and restart-identity evidence |
| Static and erased components | API/performance owners | Approval or continued deferral of [RFC-0005](../rfcs/0005-static-and-erased-components.md); if approved, the generic hot path, erased adapter boundary, object-compatibility rules, and the declared restart semantics that may not alter a fingerprint | Any native static hot path; otherwise nothing, and the accepted boxed boundary stands |
| Staged crate extraction | Architecture owners | Exact extraction order and boundary set, forbidden-dependency rules, facade re-export and compile/API equivalence, packaging and dry-run checks, build-time and binary-size measurement, and reversal procedure | Workspace boundary changes |
| Context codec and external state | Repository/metadata owners | Codec identity and versioning, schema lifecycle, oversized/corrupt payload behavior, upgrade and rollback rules, and the boundary before M8 portability and M12 Spring metadata migration | Durable context-format changes |
| Transaction capability direction | Repository owners | Capability declaration and typed rejection, the borrowed adapter-owned transaction path, preserved atomic checkpoint and unknown-outcome semantics, and which capabilities affect the definition fingerprint | Repository capability surface changes |
| Public facade and API review | API owners | Curated facade surface, disclosure rules for runtime/database/telemetry-SDK/credential types, pre-1.0 evolution policy, and evidence that no facade decision blocks the M6-M12 target | Preview API claims |
| Ledger disposition and promotion | Quality/compatibility owners | Reviewed disposition for every M0-M4 row, the explicit list of advertised embedded-kernel rows, the required evidence profile each needs for `Verified`, and the named release that carries them | Any `Verified` promotion or preview parity claim |
| Preview support, upgrade, and release bounds | Release/operations owners | `0.x` version selection inputs, supported-configuration matrix, upgrade and downgrade expectations, documented limitations, and preview support commitments | Preview publication |
| Evidence campaigns | Quality owners | Named full-embedded conformance, PostgreSQL crash/restore/upgrade, security, performance, soak, and resource-bound fixtures with retained reproducible raw evidence, plus the reference workload and correctness P0/P1 triage bar | M5 exit claims |

Issue
[#95](https://github.com/luceat-lux-vestra/oxide-batch/issues/95)
records this table and the delivery order. Issue
[#97](https://github.com/luceat-lux-vestra/oxide-batch/issues/97) closed every
gate above on 2026-08-03 in canonical documents, recorded by the
[M5 design-gate evidence](m5-design-gate-evidence.md). Any change to an
accepted contract still requires a superseding RFC or ADR before dependent
implementation.

## Governing architecture constraints

[RFC-0005](../rfcs/0005-static-and-erased-components.md) was **accepted** on
2026-08-03, after this gate closed as continued deferral and after its spike
subsequently ran. Its decision is recorded as
[ADR-0008](../architecture/decisions/0008-item-component-contract.md), which
partially supersedes ADR-0002 for the three item component traits.

Acceptance does not move M5. M5 retains the ADR-0002 boxed component boundary,
introduces no static hot path, and does not use stabilization work to preempt
M6 item-model scope; the new contract lands in M6 rather than underneath M5's
fingerprint and crate-extraction work. Component thread-safety and placement
constraints remain validated explicitly at every concurrent boundary. The
roadmap's M5 dependency was satisfied by the recorded deferral decision and is
unaffected by the later approval.

[RFC-0009](../rfcs/0009-transport-neutral-worker-protocol.md) remains
proposed. M5 cannot add remote envelopes, worker registration, transport
acknowledgements, distributed lease or fencing claims, or cross-host
coordination, and no preview claim may imply distributed readiness.

[RFC-0003](../rfcs/0003-target-workspace-boundaries.md) and
[ADR-0001](../architecture/decisions/0001-workspace-and-facade.md) keep one
workspace, the curated `oxide-batch` facade, private-by-default implementation
crates, and no placeholder publication. Extraction is staged and
behavior-preserving, must not become a rewrite, and each boundary carries its
own dependency-graph, facade-equivalence, and packaging evidence. Public-crate
approval remains separate from extraction.

[RFC-0004](../rfcs/0004-compiled-execution-plan.md) and
[ADR-0005](../architecture/decisions/0005-compiled-execution-plan.md)
authorize the compiled-plan and fingerprinting direction, but the M5 subset
must stay narrower than M7 general compiled-plan restart and the M7 definition
registry. [ADR-0004](../architecture/decisions/0004-job-definition-restart-compatibility.md)
definition identity and the accepted fail-closed restart model are preserved.

[RFC-0007](../rfcs/0007-repository-services-and-capabilities.md) and
[ADR-0006](../architecture/decisions/0006-repository-capability-model.md)
govern the capability surface; [RFC-0010](../rfcs/0010-metadata-and-spring-migration.md)
governs metadata and migration direction. M5 stabilizes direction only and does
not deliver M8 repository portability, additional Tier-1 databases, or M12
Spring metadata migration.

No new crate, feature flag, manifest field, schema table, CLI command, or
extension point is added merely to reserve later scope.

## Delivery workstreams and order

1. A design-gate issue closes the compiled-plan/fingerprint, static-vs-erased,
   crate-extraction, context-codec, transaction-capability, facade/API,
   ledger-promotion, support-bound, and evidence gates in canonical documents.
2. Plan and definition-fingerprint stabilization delivers the accepted subset,
   its drift detection, and its restart-identity evidence. Delivered by issue
   [#98](https://github.com/luceat-lux-vestra/oxide-batch/issues/98) and
   recorded in the
   [plan and fingerprint evidence](m5-plan-fingerprint-evidence.md), which
   applies [ADR-0009](../architecture/decisions/0009-definition-fingerprint-input-set.md).
3. Component-boundary and staged crate-extraction work applies the accepted
   decision behind an unchanged facade with dependency, equivalence, and
   packaging evidence. Delivered in part by issue
   [#99](https://github.com/luceat-lux-vestra/oxide-batch/issues/99) and
   recorded in the
   [crate-extraction evidence](m5-crate-extraction-evidence.md), which applies
   [ADR-0010](../architecture/decisions/0010-extracted-crate-publication.md).
   All three authorized stages are complete, including the ADR-0011 core
   placement that the boundary correction in that record required.
4. Context-codec and transaction-capability work applies the accepted
   direction with migration and rollback evidence where durable formats move.
   Delivered by issue
   [#100](https://github.com/luceat-lux-vestra/oxide-batch/issues/100) and
   recorded in the
   [codec and capability evidence](m5-codec-and-capability-evidence.md). No
   durable format moved, so no new migration is owed; the schema upgrade and
   restore campaign stays with issue #102.
5. Facade and public-API review records the preview surface, its disclosure
   rules, and its M6-M12 compatibility argument. Delivered by issue
   [#101](https://github.com/luceat-lux-vestra/oxide-batch/issues/101) and
   recorded in the
   [facade and API review evidence](m5-facade-api-review-evidence.md). The
   surface holds every prohibited disclosure class and blocks no M6-M12
   boundary. The review found one violation of the wider public-boundary rule
   and removed it, and recorded one M8 extension finding as issue
   [#114](https://github.com/luceat-lux-vestra/oxide-batch/issues/114).
6. The evidence campaigns run full embedded conformance and the PostgreSQL
   crash, restore, upgrade, security, performance, soak, and resource-bound
   matrices, plus the reference workload.
7. Preview documentation and the exit record publish the production-preview
   guide, limitations, support matrix, operator/developer guides,
   upgrade/recovery runbooks, the reviewed ledger dispositions, and the M5
   exit gate.

After the design gates close, plan stabilization and the independent evidence
foundations may proceed within the accepted contracts. Component and crate
boundary work follows its gates and the facade-equivalence rules. Codec and
capability work follows the durability gates. Facade review follows the
boundary decisions. Exit work follows all implementation streams, and ledger
promotion follows the named release.

## Definition of done

M5 closes only when:

- the full embedded conformance suite passes across the accepted M0-M4 scope
  with no unresolved correctness P0/P1;
- the delivered `CompiledExecutionPlan` and definition-fingerprint subset
  detects drift fail-closed, and definitions cannot silently change meaning
  across restart;
- the static/erased component boundary is decided by a recorded gate, and any
  approved hot path preserves object compatibility and declared restart
  semantics without altering a fingerprint;
- each completed crate extraction preserves facade behavior and supported
  imports, passes dependency and forbidden-dependency checks, and changes no
  persisted bytes, transaction boundary, lifecycle write, restart selection,
  fingerprint, or normalized trace;
- the context-codec and transaction-capability direction is approved, and any
  durable format move passes migration from every supported prior version,
  rejects newer or corrupt versions, and retains documented restore rollback;
- the public facade exposes no runtime, database, telemetry-SDK, credential,
  deployment-auth, sensitive payload, SQL, or user-error-text implementation
  types, and the review records that no facade decision blocks the M6-M12
  target;
- PostgreSQL crash, restore, upgrade, security, performance, soak, and
  resource-bound campaigns pass on the supported matrix with validated TLS and
  least-privilege roles and retained reproducible environment, correctness,
  and raw-evidence records;
- the reference workload runs and its results are recorded against the
  performance plan's regression gates;
- every M0-M4 ledger row has a reviewed disposition, the advertised
  embedded-kernel rows are `Verified` against the named released version with
  every required evidence link, and deferred later-milestone rows remain
  visible;
- preview support, limitations, upgrade expectations, operator/developer
  guides, and recovery runbooks are executable and reviewed.

Rows outside the advertised embedded-kernel set remain `Implemented`,
`Partial`, `Planned`, or `Deferred` rather than released `Verified`, and their
visibility prevents any full-parity or project-wide readiness claim.

## Scope controls

M5 does not include full item, flow, integration, or distributed parity, the
M6 user test kit, M7 advanced nested/split flow and the definition registry,
M8 repository portability or additional Tier-1 databases, M9 messaging and
streaming integrations, M10 high-performance execution, M11 distributed
execution, M12 Spring metadata migration and ledger closure, M13 ecosystem and
certification work, or the M14 project-wide 1.0/GA release. Those remain
assigned to later roadmap and decision gates.
