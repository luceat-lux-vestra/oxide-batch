# `GATE-B-FORMAT5-001` manifest compatibility evidence

**Scenario:** Gate-B manifest format 5 defined by ADR-0015.

**Source:** independently authored synthetic fixtures. No production extract,
provider configuration, credential, account, endpoint, or third-party material
is present.

## Release-backed legacy baselines

`legacy-format1-tasklet.manifest.json` and
`legacy-format1-chunk.manifest.json` pin the exact format-1 bytes produced by
the v0.7.0 `DefinitionIdentity::tasklet` and `DefinitionIdentity::chunk`
producer paths. At the #714 implementation boundary the current
`crates/oxide-batch-core/src/definition.rs` blob is byte-identical to v0.7.0
for both producer functions.

Formats 2, 3, and 4 retain their pre-existing immutable evidence:

- format 2: `LIFE-DEFINITION-001/format2-two-step.manifest.json` and fingerprint
  `c0ea69669657cb8ec425801588a1f042608d8785333ad7d38d8a1f7ed5d8557f`;
- format 3 fingerprint
  `f5ee7c2d6923411c8c068b6c2770b95575256833bddaed1be9c3893324c541a9`;
- format 4 fingerprint
  `5588696b6bedc009b4da64689e4e9eb46adb10454c2ea2b91025850cdc68436f`.

Those legacy values are never regenerated or re-pinned by this scenario.

## Format-5 generation

The focused integration suite builds each legacy base through its real producer,
wraps it with a typed `GateBDefinitionContract`, and verifies that serializing
the nested `base` reproduces the original legacy bytes exactly.

Validation command:

```shell
cargo test -p oxide-batch --test gate_b_manifest
```

The format-5 vectors remain generated assertions in the focused test until the
implementation has passed executable validation. A later validation-only work
unit may materialize add-only format-5 byte fixtures from the validated
producer; it must not rewrite the release-backed legacy baselines.
