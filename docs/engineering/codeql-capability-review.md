# CodeQL capability review: Rust

**Issue:** #248
**Review date:** 2026-09-06
**Status:** live Rust producer enabled; final PR/main acceptance pending

## Decision

Keep GitHub CodeQL **default setup** as the repository's single CodeQL
authority and add Rust to that managed configuration. Do not add a checked-in
advanced-setup CodeQL workflow while default setup is enabled.

The existing `Analyze (actions)` context remains required. The new `Analyze
(rust)` context starts as advisory and is not added to the `Protect main`
ruleset in this change. Promotion to required needs evidence that the managed
Rust producer is consistently present and reliable; #233 is the scheduled
hardening-drift review point for that decision.

## Capability drift

The previously accepted repository statement that CodeQL did not support Rust
is obsolete. GitHub made CodeQL analysis for Rust generally available and the
current CodeQL toolchain used by this repository contains the Rust extractor.
For Rust, the selected default-setup build mode is `none`, so the managed scan
does not duplicate the workspace build/test gate.

This is a platform-capability drift correction, not a replacement of existing
Rust controls. Clippy, tests, `cargo deny`/RustSec, dependency review,
supply-chain validation, and retained-evidence verification keep their existing
independent responsibilities.

## Pre-change live evidence

Immediately before #248 implementation, authoritative `main` was:

- commit: `2c9ad1106bda9a45dfdb6921c92f1c4d9ed2a27e`;
- tree: `71f35c9ea5df166b8201ffefe28f7ce86b6816b0`.

The GitHub-managed CodeQL push run for that commit was run `34024949621`, job
`101464159651`. Its live logs prove:

- workflow path `dynamic/github-code-scanning/codeql` and event `dynamic`;
- `CODE_SCANNING_IS_STEADY_STATE_DEFAULT_SETUP=true`;
- only `languages: actions` was selected by the repository configuration;
- `Analyze (actions)` completed successfully and uploaded SARIF to GitHub code
  scanning;
- CodeQL Action `4.37.9` used CodeQL CLI `2.26.4`;
- the same CLI installation resolved a Rust extractor and Rust extractor
  options, even though the live configuration selected only Actions.

Therefore the missing Rust result was a **configuration gap**, not an extractor
or runner-capability gap.

## Authority and duplication invariant

Exactly one CodeQL authority is allowed:

1. GitHub default setup owns both `Analyze (actions)` and `Analyze (rust)`.
2. No checked-in CodeQL advanced-setup workflow is added while default setup is
   enabled.
3. `Analyze (actions)` remains represented in
   `.github/merge-gate-policy.json` as the managed required context.
4. `Analyze (rust)` is intentionally advisory, so it is documented but is not
   inserted into the required-context policy or live ruleset yet.

This avoids duplicate CodeQL analyses and avoids a fake merge authority where a
required context can disappear because of path or workflow conditions.

## Acceptance evidence required on this change

Before #248 may close, all of the following must be observed rather than
assumed:

- the live default setup configuration includes Rust while remaining the single
  CodeQL authority;
- `Analyze (rust)` is emitted and succeeds for the exact final pull-request
  HEAD;
- the Rust analysis upload reaches GitHub code scanning for that exact HEAD;
- after squash merge, `Analyze (rust)` is emitted and succeeds again on the
  exact resulting `main` commit;
- `Analyze (actions)` remains present and successful throughout;
- the repository's normal proof-obligation merge gate is green on the exact
  final PR HEAD.

Exact PR/main run and job identities are ephemeral operational evidence. They
are recorded in the #248 closure evidence comment after post-merge verification;
this document records the stable authority, capability, and acceptance contract.

## Future drift review

#233 must treat GitHub CodeQL language/build-mode support and the repository's
managed language set as mutable external configuration. A future capability or
configuration change is not accepted merely because GitHub's UI or defaults
changed; the live producer set, documentation, and merge-authority decision must
be reconciled again.
