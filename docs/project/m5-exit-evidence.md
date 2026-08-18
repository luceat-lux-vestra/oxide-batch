# M5 Exit Evidence

**State:** Pending release verification

**Target release:** OxideBatch `0.5.0` — M5 Embedded Core Production Preview

This document is intentionally present before the release so release-tagged
documentation has a stable, non-broken exit-record target. It is **not** an M5
exit decision and does not promote any compatibility-ledger row to `Verified`.

The authoritative M5 evidence campaign reconciliation is
[`m5-102-reconciliation.md`](m5-102-reconciliation.md). The release-preparation
state and user-facing support boundary are documented in the
[Production Preview guide](../guides/production-preview.md),
[limitations](../guides/limitations.md), and
[support matrix](../release/support-matrix.md).

## Pending post-release evidence

After the named `0.5.0` release is published, this record must be completed with
independently verified identities for:

- the final `v0.5.0` tag and release commit;
- all published crates and their crates.io checksums;
- docs.rs results for the published crates;
- the clean external-consumer build from crates.io;
- the supported PostgreSQL release smoke result;
- GitHub Release package archives, checksum manifest, SBOMs, and attestations;
- the final M0-M4 ledger disposition and the exact rows promoted to `Verified`;
- residual `Partial`, `Planned`, `Unknown`, and later-milestone limitations;
- closure of the live #103 exit criteria and the parent M5 milestone gate.

Until those checks are complete, the only valid status is **Pending release
verification**. Do not describe M5 as exited, passed, GA, stable,
enterprise-ready, fully Spring Batch compatible, or project-wide
production-ready based on this placeholder.

## Finalization rule

A post-release closure change replaces this pending record with the complete
release evidence and may state:

```text
M5 Embedded Core Production Preview gate: PASSED
```

only after every live M5 exit criterion is satisfied against the actual named
release.
