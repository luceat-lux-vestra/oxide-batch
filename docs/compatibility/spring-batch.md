# Spring Batch Compatibility Contract

**State:** Accepted

**Long-term target:** Complete feature-ledger parity is accepted by RFC-0002.

This is the normative compatibility and public-claim policy. The
[feature ledger](conformance-matrix.md) owns the population and per-row status;
the [conformance strategy](conformance-strategy.md) owns evidence requirements.

## Reference baseline

The pinned comparison baseline is **Spring Batch 6.0.4**:

- [reference documentation](https://docs.spring.io/spring-batch/reference/);
- [6.0.4 public API](https://docs.spring.io/spring-batch/reference/api/index.html);
- metadata schema appendix, integration modules, standard component appendix,
  test module, release notes, and deprecation notices linked by the ledger.

The baseline includes documented user-visible behavior and public capabilities,
not Spring implementation internals.

## Baseline update procedure

A new Spring Batch patch or minor release does not silently expand an existing
OxideBatch claim. Updating the baseline requires a compatibility review that:

1. records the old and proposed versions and official release/deprecation
   sources;
2. diffs the reference sections, public API packages, schemas, integrations,
   test module, and component catalog;
3. adds new/changed/removed ledger rows before assigning their disposition;
4. identifies semantic, metadata, protocol, migration, and support impact;
5. updates reference fixtures and evidence versions;
6. uses an RFC when the accepted target, public guarantee, or release gate
   changes.

Removed/deprecated Spring features remain historical rows for baselines that
claimed them. New baseline claims are not made until affected rows are
`Verified` or have an approved terminal divergence.

## Compatibility dimensions

| Dimension | Promise |
| --- | --- |
| Semantic | Same documented domain meaning and lifecycle outcome |
| Behavioral | Equivalent normalized observations for the complete named scenario set |
| Feature | An implemented capability exists for the documented Spring feature |
| Operational | Equivalent operator capability, possibly through a different API |
| Migration | Explicit definition or metadata conversion is versioned and tested |
| Schema | Both frameworks safely use the same live metadata schema |
| API/source | Java/Spring APIs or configuration run unchanged |

The native core explicitly does **not** target Java source/binary/API
compatibility, Spring container/Bean compatibility, or live shared-schema
compatibility.

## Exact and native equivalents

An **exact behavioral equivalent** produces the same normalized durable and
external observations under the row's input, failure, stop, restart, and
concurrency scenarios, excluding differences the row explicitly declares.

A **Rust-native equivalent** meets the same user/operator need through
idiomatic Rust types, explicit dependency injection, compiled plans,
capabilities, or factories. It is not automatically behaviorally equivalent;
the ledger records the parity type and every observable divergence.

Permitted divergences must:

- be explicit, stable, versioned, and linked to evidence;
- not weaken a claimed parity dimension;
- not hide data loss, replay, ordering, security, or operator consequences;
- have an approved rationale when the row becomes `Unsupported` or
  `NotApplicable`.

## Row states

- `Unknown`: population identified, disposition not reviewed;
- `Planned`: assigned to an accepted milestone;
- `Implemented`: code exists but release evidence is incomplete;
- `Verified`: all required evidence passes for a named released version;
- `Partial`: one or more named observations differ or are missing;
- `Unsupported`: deliberately not provided with approved rationale;
- `Deferred`: reviewed but scheduled outside the current milestone;
- `NotApplicable`: Spring-specific mechanism has no meaningful Rust
  application and an approved rationale/equivalent is recorded.

`Unsupported`, `Deferred`, and `NotApplicable` are never synonyms. A row cannot
become `Verified` through documentation, naming similarity, or compilation
alone.

## Claim levels

| Claim level | Required evidence |
| --- | --- |
| Inspired by | Shared concepts; no parity implication |
| Named semantic parity | Every cited semantic row is released and `Verified` |
| Named behavioral parity | Complete scenario observations are released and `Verified` |
| Category parity | The category population is complete and every claimed row is `Verified`; terminal divergences are named |
| Migration compatibility | Named source/target tooling, fixtures, reconciliation, and limitations are released |
| Complete documented feature coverage | Entire baseline ledger has a reviewed terminal disposition and no unknown/deferred/untested gap |
| Complete documented behavioral parity | Every behaviorally applicable row is `Verified`; no `Partial` or `Unsupported` behavior is hidden |

Public wording names the Spring baseline, OxideBatch version, ledger scope, and
known divergences. “100% compatible” is too ambiguous and must not be used.

Released claims remain limited to verified semantic, behavioral, feature,
operational, and migration rows. The accepted long-term target is complete
documented feature-ledger coverage under
[RFC-0002](../rfcs/0002-full-spring-batch-feature-ledger-parity.md).

## Evidence required for `Verified`

Each row supplies an evidence profile. As applicable, verification includes:

- unit/property/compile-fail evidence for values, policies, and type contracts;
- adapter contract and real integration evidence;
- black-box conformance and differential reference fixtures;
- crash, stop, restart, concurrency, and unknown-commit matrices;
- schema/context/definition/protocol migration evidence;
- performance, capacity, cancellation, and resource-limit evidence.

Evidence records source and OxideBatch versions, environment, fixture
provenance, normalized observations, and immutable links. An omitted evidence
class has a reviewed rationale.

## Metadata migration and schema policy

OxideBatch owns its metadata schema. Spring Batch and OxideBatch processes do
not concurrently mutate one schema. The accepted migration path is one-way,
versioned export/import with lineage, dry-run, validation, and reconciliation,
as specified by the [migration contract](spring-batch-migration.md).

Metadata migration compatibility does not imply definition compatibility,
automatic Java-code translation, or live schema compatibility.

## Claim safety

Documentation must use “inspired by” unless the named rows satisfy the claim
level above. Project-wide production, enterprise, complete-parity, and 1.0
language also requires the release gate in the
[vision](../product/vision-and-scope.md) and [roadmap](../roadmap.md).
