# Spike 0003: Execution-Context Schema Evolution

- **State:** Complete
- **Owner:** maintainers
- **Issue:** [#6](https://github.com/luceat-lux-vestra/oxide-batch/issues/6)
- **Date:** 2026-07-29
- **Decision:** D-007 in the [M0 decision register](../../product/open-decisions.md)

## Decision to unblock

Select a durable execution-context envelope and application schema-evolution
rule that can restart from older data with bounded resource use and stable,
redacted diagnostics.

## Hypotheses

1. A versioned JSON envelope can separate framework format compatibility from
   application/job payload compatibility.
2. Explicit version decoders can upgrade a renamed required field while
   defaults handle an added optional field.
3. A flattened extension map can preserve additive unknown payload fields
   through read and rewrite.
4. Byte, depth, version, corruption, and type failures can be classified
   without placing raw context data in errors.

## Constraints

- Serde 1.0.229 and serde_json 1.0.151;
- framework format identifier and version are mandatory;
- application schema identifier and version are mandatory;
- default limit is 64 KiB and JSON nesting depth 16;
- a newer framework or application version is rejected;
- rolling deployment compatibility is not assumed.

The fixture schema models `next_index` in v1 becoming `cursor` in v2, with an
optional `source_checksum` added in v2.

## Experiment

Source and fixtures:

- `spikes/m0-architecture/src/context.rs`;
- `spikes/m0-architecture/tests/context.rs`;
- `spikes/m0-architecture/tests/fixtures/context`.

Reproduce:

```console
cargo test -p oxide-batch-m0-spikes --test context
```

## Acceptance and rejection criteria

Acceptance requires successful backward read and upgrade, missing optional-field
defaulting, additive unknown-field retention, current-version rewrite, and
typed failures for malformed, oversized, over-deep, newer-version, and
wrong-type values. Error text must not contain a fixture secret.

Opaque bytes are rejected as the default because the framework could not apply
these limits or compatibility checks. Rust-layout-dependent binary formats are
rejected if the fixture cannot be read independently of the original Rust type.

## Results

Observed output:

```text
running 6 tests
test corrupted_payload_has_a_stable_redacted_diagnostic ... ok
test size_depth_and_type_limits_fail_before_context_use ... ok
test missing_optional_field_uses_a_documented_default ... ok
test newer_framework_and_application_versions_are_rejected ... ok
test v1_fixture_upgrades_to_current_schema ... ok
test additive_unknown_fields_survive_read_and_rewrite ... ok

test result: ok. 6 passed; 0 failed
```

The checked-in v1 fixture upgraded `next_index: 41` to `cursor: 41`. Its
additive `partition` field survived the upgrade. The v2 fixture without
`source_checksum` defaulted to `None`. A future `future_hint` field survived a
decode/encode/decode round trip.

Malformed JSON produced only `execution context is malformed`; the embedded
fixture value was absent. Newer format and application versions produced
separate classifications.

## Correctness and risk review

- Size is checked before parsing; depth is checked before typed payload
  decoding.
- Framework and application versions evolve independently.
- Upgrades are explicit code paths. Renames and type changes never depend on
  Serde field aliases alone.
- Unknown payload fields are retained for additive compatibility. Framework
  envelope extensions are not promised across a rewrite in version 1.
- Errors contain categories, not payload values or parser excerpts.
- Context is still potentially sensitive. Inspectability does not authorize
  logging or telemetry export.
- The format does not define canonical JSON bytes or use JSON encoding as an
  identity hash.

## Conclusion

Accept bounded, versioned JSON via Serde as the initial durable execution
context. Use a framework-owned envelope with:

- `format` and `format_version`;
- application `schema` and `schema_version`;
- a JSON `payload`.

Each job definition owns explicit backward readers from every supported payload
version to its current typed context. Reject newer versions, preserve additive
payload extensions, and keep failure diagnostics data-redacted.

Confidence is high for the initial format. Canonicalization, context encryption,
and cross-release support windows require separate decisions if introduced.

## Follow-up

- turn the spike limits into typed M1/M2 configuration with conservative
  defaults;
- add each released context version as an immutable fixture;
- fuzz the envelope parser before accepting untrusted durable input in M2;
- document schema support windows with job-definition versioning before M2.
