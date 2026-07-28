# Conformance Strategy

**State:** Proposed

Conformance tests support specific documented compatibility claims. They do not
attempt to reproduce Spring Batch implementation internals.

## Sources

Use, in priority order:

1. official Spring Batch reference documentation;
2. behavior observed from a pinned public Spring Batch release;
3. public API documentation and release notes;
4. independently authored minimal examples.

Record exact source links and versions. Do not copy Spring Batch source, tests,
documentation prose, schemas, or fixtures into OxideBatch unless license and
attribution are explicitly reviewed.

## Structure

Each scenario has:

- stable ID matching a compatibility-matrix row;
- plain-language preconditions and expected observations;
- pinned Spring Batch version and reference source;
- OxideBatch capability/milestone;
- synthetic inputs and deterministic clock/IDs;
- launch, failure, stop, and restart sequence;
- expected statuses, exit statuses, counts, contexts, and durable relationships;
- allowed API/schema/operational differences;
- machine-readable result where practical.

## Reference runner

A clean-room reference harness may run minimal independently written Java jobs
against a pinned Spring Batch dependency. It:

- lives outside published Rust crates;
- uses a separate database/schema;
- emits normalized observations rather than Spring internal objects;
- never runs as a required consumer dependency;
- pins Java/build-tool/dependency versions for reproducibility;
- retains license notices for all reference dependencies.

The harness is approved through an issue before it is added.

## OxideBatch runner

The Rust runner executes the equivalent semantic scenario and normalizes output.
Comparison ignores IDs/timestamps/physical schema/API forms unless the row
claims compatibility for them.

## Status and release use

- Planned: scenario is specified but no implementation exists.
- Implemented: runner exists but evidence is incomplete.
- Verified: both required evidence and released OxideBatch behavior agree.
- Partial: named observations differ.
- Unsupported: intentionally not provided.
- Deferred: outside the current milestone.

A release cannot claim a compatibility level higher than its Verified rows.

## Fixtures and data

- All fixtures are synthetic and minimal.
- No production extracts, credentials, personal data, or proprietary schemas.
- Generated fixtures record generator/seed and are reproducible.
- Corrupt/oversized/untrusted fixtures are clearly isolated and bounded.
- Binary fixtures include provenance, format version, and regeneration command.
- Golden outputs are reviewed for stable semantics, not incidental formatting.
