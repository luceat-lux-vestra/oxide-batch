# CodeQL capability review: Rust

**Issue:** #248
**Review date:** 2026-09-06
**Status:** capability accepted; default-setup authority superseded by #315

## Decision

#248 established that CodeQL can analyze both GitHub Actions and Rust in this
repository. Its original producer choice—GitHub-managed default setup—was
superseded by #315 after the repository adopted exact-final-HEAD PR validation
as the merge authority and removed duplicate validation on the resulting
`main` commit.

The current authority is checked-in advanced setup in
`.github/workflows/codeql.yml`. It preserves the existing context names:
`Analyze (actions)` remains required and `Analyze (rust)` remains advisory.
Both use `build-mode: none`. Default setup must remain disabled while the
advanced workflow is active.

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

1. Checked-in advanced setup owns both `Analyze (actions)` and `Analyze (rust)`.
2. The workflow has no `push: main` trigger; PR exact-final-HEAD analysis is merge-time authority.
3. Weekly schedule and explicit manual dispatch remain available for query/tool drift and bounded operational proof.
4. `Analyze (actions)` is represented as a checked-in required producer in merge-gate policy.
5. `Analyze (rust)` remains advisory.
6. GitHub default setup must remain `not-configured`; re-enabling it is policy drift.

## #315 live cutover readback

On 2026-09-28, the Administration-scoped Code Scanning API was used to disable
GitHub CodeQL default setup before final PR proof. Immediate readback returned:

- `state: not-configured`;
- prior managed language inventory: `actions`, `rust`;
- `query_suite: default`;
- `threat_model: remote`;
- `schedule: null` after disabling the managed configuration.

This readback proves only that managed default setup is no longer active. The
checked-in advanced workflow still requires fresh exact-HEAD PR evidence before
the migration is accepted.

## Migration evidence required by #315

Before the advanced-setup cutover is accepted:

- default setup is disabled through the Administration-scoped API and read back as `not-configured`;
- a fresh PR branch update after that cutover produces both advanced CodeQL contexts on the exact final HEAD;
- `Analyze (actions)` and `Analyze (rust)` both succeed from `.github/workflows/codeql.yml`;
- all live required contexts remain green on that same HEAD;
- after squash merge, the resulting `main` SHA does not start CodeQL, dependency-review, Actions-security, or hardening-drift validation merely because it was merged.

The historical #248 default-setup run identifiers below remain capability evidence only; they are no longer the current producer contract.

## PR-impact routing after #348

Rust CodeQL remains advisory; `Analyze (actions)` remains the repository Merge
Gate member. The PR execution model is now deliberately asymmetric:

- `Analyze (actions)` still runs on every ready pull request;
- a metadata-only `codeql-rust-impact` job classifies Rust impact;
- that job fetches `.github/scripts/codeql-rust-impact.py` from the exact PR
  base SHA and never trusts a classifier introduced by the PR being classified;
- the classifier re-reads live PR base/head identity and paginates/reconciles
  the complete changed-file list before allowing Rust analysis to be skipped;
- Rust source, Cargo manifest/lock, toolchain, Cargo configuration, and CodeQL
  routing-control changes all force `Analyze (rust)`;
- missing trusted classifier, API failure, malformed/incomplete metadata,
  unsupported file status, or route-job failure all force Rust analysis rather
  than suppress it;
- scheduled and manually dispatched CodeQL runs always perform full Rust
  analysis regardless of PR-impact routing.

The implementation PR is a bootstrap case: its protected base predates the
classifier, so the trusted-base lookup must fail closed to a full Rust analysis.
Only later non-Rust-impact PRs are eligible for the runtime skip.

The resource investigation did not introduce explicit `threads` or `ram`
settings. CodeQL already defaults to the hardware threads and available memory
of the selected GitHub-hosted runner, so setting those inputs without changing
runner capacity would not constitute a resource upgrade.

## Future drift review

#233 must treat GitHub CodeQL language/build-mode support and the repository's
managed language set as mutable external configuration. A future capability or
configuration change is not accepted merely because GitHub's UI or defaults
changed; the live producer set, documentation, and merge-authority decision must
be reconciled again.
