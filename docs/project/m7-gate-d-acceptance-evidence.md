# M7 Gate D: Definition evolution and fork acceptance (#198)

**Disposition:** implementation children #550, #551, and #552 merged; Gate-D exact-head acceptance evidence recorded 2026-10-14. This is **not** M7 (#192), M7 operator (#199), or release/GA acceptance.

## Authority and provenance

- Pre-merge source: PR [#555](https://github.com/luceat-lux-vestra/oxide-batch/pull/555), exact source HEAD `8ab7899b321cb2137b50fcb8fd58508eeafe71bb`; squash merged to `main@4702fbcb5546316e285ea36f0b47531ace7099f9`.
- Exact-source [trusted Rust authority](https://github.com/luceat-lux-vestra/oxide-batch/actions/runs/38553063607): **19/19 jobs passed**, including unit/integration/facade/surface/contracts, PostgreSQL 15/16/17/18 design gates, packaging and MSRV.
- Exact-source [M5 Conformance](https://github.com/luceat-lux-vestra/oxide-batch/actions/runs/38552991141): PostgreSQL 15 and 18 deep/conformance shards and aggregators passed.
- Exact-source [Fast branch CI](https://github.com/luceat-lux-vestra/oxide-batch/actions/runs/38552984848), [PR proof](https://github.com/luceat-lux-vestra/oxide-batch/actions/runs/38552991157), and [trusted merge gate](https://github.com/luceat-lux-vestra/oxide-batch/actions/runs/38553095925) passed. These are **pre-merge source** checks; no independent post-merge push-run claim is made here.

## Gate-D operation matrix

| Requested operation | Identity and selection | Allowed state semantics | Implementation/evidence |
| --- | --- | --- | --- |
| **Exact restart** | Existing instance; historical definition identity/fingerprint must match | Existing restart contract, without definition reinterpretation | M2/M3 strict restart guards and #550 exact registry/selection; quality and design-gate authority above |
| **Explicit compatible upgrade** | Existing instance; one reviewed direct, non-transitive registered compatibility edge | Only authorized schema/node mappings with exact transformer revision and atomic materialization; fail closed otherwise | #551 / PR #554; `postgres_compatible_upgrade_process_kill.rs`, quality and PostgreSQL design-gate authority |
| **Incompatible restart** | Historical instance with no valid exact/direct compatible edge | **Reject**, no silent definition rebinding, state guessing or fabricated compatibility | #550/#551 fail-closed registry/upgrade tests and strict restart guards |
| **Explicit fork** | New normal identifying-parameter `JobInstanceKey` and new execution, with immutable source-execution/state digest and `OperationId` lineage | **No state copy by default.** Optional single reviewed source step-context → target job-context transform; never an implicit restart or upgrade seed | #552 / PR #555; `fork_lineage.rs`, `fork_transform.rs`, `postgres_fork.rs` |

## Fork adversarial evidence

| Risk/condition | Asserted outcome and owning test/evidence |
| --- | --- |
| Target identity collision and divergent `OperationId` replay | Reject existing key or conflicting payload; no hidden generated key/second attempt. `fork_lineage.rs`; PostgreSQL fork integration |
| Missing target registration/artifact, stale source/version, or nonterminal source | Fail closed before target creation; historical source unchanged. `fork_lineage.rs`, `fork_transform.rs`, registry/definition tests |
| Unapproved schema/node/transformer or newer/corrupt state | Reject with explicit typed error; no inferred state family or cross-definition semantics. `fork_transform.rs`, schema guard and PostgreSQL design gate |
| Implicit copying | First target attempt is `STARTING`; no source restart marker, upgrade seed, steps, operator ownership, lifecycle/counters or opaque context carryover. `fork_lineage.rs`, `postgres_fork.rs` and FK/migration assertions |
| Same operation under concurrent PostgreSQL transactions | One durable target lineage/instance/execution after both requests complete; exact `OperationId` readback. `postgres_fork.rs` |
| Transaction rollback or client-unknown commit | All-or-nothing target/lineage removal on rollback; known durable operation readback on unknown commit, no blind retry. Memory `fork_lineage.rs` and PostgreSQL integration |
| Kill before source-lock release, during transformer, and while deferred commit blocks | SIGKILL leads to no partial target before commit; committed-but-unknown yields one complete lineage and replay without duplicate. `postgres_fork_process_kill.rs` with deterministic three-phase handshakes and PostgreSQL 15/18 design-gate logs |
| Populated DB migration, restore, privileges, version guard | Schema 10 → 11, retained legacy history, descriptor/payload restore, migration reapply rejected, newer schema rejected; runtime/migrator separation. `run-schema11-upgrade-gate.sh`, `verify-schema11-*.sql`, and 15/16/17/18 design gates |
| Bounded/redacted public contract | Typed `ForkRequest`/`ForkLineage`/`ForkContextTransform`/`JobFork`; no payload in lineage, no raw credential implementation types; facade review and API snapshot passed |

The PostgreSQL design-gate jobs on the exact source HEAD **explicitly executed** `postgres_fork.rs`, `postgres_fork_process_kill.rs`, and the populated 10→11 restore fixture. The conformance campaign is corroborating evidence, **not** a substitute for those targeted checks.

## Scope and handoff

- #198 owns Gate-D definition registry/evolution and explicit fork identity; #550–#552 are its serialized accepted implementation children.
- #199 now owns application-facing launcher/operator/explorer/incrementer integration and restart-control policy. No direct fork service implementation here proves those surfaces are finished.
- #197 repeat/interceptor semantics and #200 milestone-wide conformance/docs/exit are separate and remain unproven by Gate D.
- M8 repository portability, M9 integrations, M10 execution optimization, M11 remote execution/artifact distribution, and M12 migration closure remain outside this claim.

**Close #198 only on the evidence above; do not close #192 or #199.**
