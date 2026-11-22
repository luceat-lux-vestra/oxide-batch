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

## Materialized format-5 vectors

The focused integration suite builds each legacy base through its real producer,
wraps it with the typed `GateBDefinitionContract`, and compares the complete
canonical outer bytes against add-only materialized vectors:

- `format5-base1-tasklet.manifest.json` — SHA-256
  `15f55b5f36b86251201ec12c92262bb37e1e57a43cb6b7f776d1bcce17da4ef6`;
- `format5-base1-chunk.manifest.json` — SHA-256
  `1d63b4960a3634a2a9471724bebd03a9b057f37fd1b87a306b8b20d41905fa5b`;
- `format5-base2.manifest.json` — SHA-256
  `d650b92159209c480b54687d4e9e2f49c3ea9266ea5330aa35aef23161ad9772`;
- `format5-base3.manifest.json` — SHA-256
  `9106d457a8bc9b99f475f28c1c0039a498fa803c99971ff84ba03f9fd70ff1e9`;
- `format5-base4.manifest.json` — SHA-256
  `4b393cf2955f180174c546c40f315b89499500baeef457514dd995efdb4033fd`.

The vectors contain the same non-trivial Gate-B contract used by
`gate_b_manifest.rs`: canonical scope/slot ordering, provider-key binding,
native-message semantics, remote acknowledgement/unknown-outcome identities,
and both definition-level validity floors.

Validation command:

```shell
cargo test -p oxide-batch --test gate_b_manifest
```

### Regeneration discipline

Regeneration is deliberately manual and add-only. Only a reviewed Gate-B
manifest-contract evolution may replace a format-5 vector. Reconstruct the
corresponding legacy definition through the named producer helper in
`gate_b_manifest.rs`, apply `with_gate_b(gate_b_contract())`, serialize
`canonical_manifest()` without a trailing newline, update the matching
format-5 fixture and recorded SHA-256, then run the focused validation command.

Never regenerate, replace, or re-pin the release-backed format-1/2/3/4
baselines as part of format-5 regeneration.
