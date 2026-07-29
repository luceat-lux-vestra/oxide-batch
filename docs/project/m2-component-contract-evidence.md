# M2 Chunk Component and Durable-State Contract Evidence

**State:** Complete on merge

**Issue:** [#40](https://github.com/luceat-lux-vestra/oxide-batch/issues/40)

**Date:** 2026-07-29

This record maps the second M2 workstream's exit criteria to facade contracts
and deterministic test evidence. It does not claim that chunk orchestration or
the PostgreSQL adapter exists; those remain owned by issues #41–#43.

| Exit criterion | Evidence |
| --- | --- |
| Borrowed runtime-neutral component work | `ItemReader`, `ItemProcessor`, `ItemWriter`, and `ChunkCompletion` return the facade-owned `BoxFuture` and borrow component, input, stop, transaction, and committed-state scopes. Contract tests invoke processor, writer, and completion trait objects. |
| Distinct typed outcomes | `ReadOutcome`, `ProcessOutcome`, `WriteOutcome`, component-specific error types, and `ChunkCompletionOutcome` separate end of input, filtering, failure, cooperative stop, and commit acknowledgement. |
| Bounded versioned durable state | `Checkpoint` and `ExecutionContext` validate format, schema ID/version, JSON-object shape, byte size, and depth before codec use. Defaults are 64 KiB and depth 16; hard ceilings are 1 MiB and depth 64. Diagnostics redact schema IDs and payload values. |
| Checked size and count arithmetic | `ChunkSize` rejects zero. `ChunkCount`, `ChunkCounts`, and `ChunkProgress` reject overflow, processed/filter/read mismatches, written/processed mismatches, and reads beyond the configured chunk size. |
| PostgreSQL transaction enlistment | `WriteContext` lends an OxideBatch-owned `BusinessTransaction`; `BusinessStatement` and `BusinessValue` preserve bound-value separation and redact SQL/value diagnostics without exposing SQLx. |
| Reusable contract coverage | `tests/contract/components.rs` covers deterministic item/end/filter/write/ack success, reader/processor/writer failures, stop at every component boundary, enlisted writes, state size/depth limits, corruption redaction, newer-version rejection, and a v1-to-v2 context upgrade. |
| Facade isolation | `trybuild` fixtures reject `tokio`, `sqlx`, and `serde_json` re-exports. The public contracts use only facade and standard-library types. |

## Reproduction

```console
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
cargo +1.95.0 check --workspace --all-targets --all-features --locked
```

## Boundary handed to implementation

Issue #41 may implement `BusinessTransaction` over an adapter-internal SQLx
transaction and persist the public checkpoint/context parts without publishing
driver or serializer types. Issue #42 may orchestrate these typed outcomes and
checked counters at chunk boundaries. Issue #43 owns atomic enlistment of
business writes, checkpoint, context, counters, and optimistic version.
