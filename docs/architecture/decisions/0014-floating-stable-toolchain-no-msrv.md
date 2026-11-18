# ADR-0014: Floating Rust Stable Toolchain Without a Declared MSRV

- **State:** Accepted
- **Date:** 2026-11-19
- **Owners:** repository and release maintainers
- **Deciders:** project owner
- **Work item:** [#699](https://github.com/luceat-lux-vestra/oxide-batch/issues/699)
- **Supersedes:** historical D-008 for future development/CI/compiler-support
  policy only; the original M0 decision and already-published release evidence
  remain unchanged
- **Canonical owner:**
  [release/support policy](../../release/support-policy.md)

## Context

M0 decision D-008 selected Rust 1.97.1 for development and releases, a numeric
MSRV of 1.95 for public crates, and no beta/nightly support. This creates an
ongoing dual-compiler maintenance contract. The project owner instead wants
every active Rust development and CI run to follow the newest released Stable
compiler automatically, without periodically editing numeric version strings.

Cargo accepts a numeric `package.rust-version` but cannot represent the floating
`stable` channel in that field. Omitting the field does not automatically
set an MSRV equal to latest Stable: it removes the explicit compiler minimum
and its published promise. The Rust 2024 edition retains its language feature
requirements, but no independent numeric MSRV is offered.

OxideBatch has already published `0.6.0`, with Rust 1.95 support and Rust
1.97.1 release-build evidence. Those point-in-time contracts are not erased
by changing the development branch.

## Decision

For future development and releases after the bounded implementation lands:

1. `rust-toolchain.toml` tracks `channel = "stable"`; development/CI/release
   checks use stable, never nightly, and fetch the current channel revision at
   invocation. Local rustup users run `rustup update stable` to upgrade.
2. Remove the workspace `rust-version` declaration and all member
   `rust-version.workspace = true` inheritance rather than creating a numeric
   MSRV update bot. Remove `clippy.toml`'s numeric `msrv`.
3. New package versions do not declare or promise a numeric minimum Rust
   compiler. Tests target latest Stable; older compilers may work, but
   compatibility is neither tested nor guaranteed. Do not describe this as an
   automatically floating numeric MSRV.
4. Preserve every required CI context and its substantive checks.
   Repurpose the existing `msrv`-named context to an independent Stable
   compilation/compatibility gate **if the context name is required by
   repository policy**, rather than removing it, bypassing it, or replacing it
   with a successful no-op. All other formatting, Clippy, tests, features,
   database integration, packaging, and security requirements remain.
5. Keep GitHub Action source versions pinned to reviewed commit SHAs. Rust
   compiler caches include the **actual resolved compiler identity** (from
   `rustc -Vv`, including release and commit) in their compatibility key;
   hashing the unchanging word `stable` alone is insufficient.
6. Record resolved `rustc -Vv`, Cargo identity, exact source/commit, and
   artifacts in future release/benchmark evidence so tests are reproducible
   even though the preferred toolchain channel floats.
7. Apply the removed compiler support floor at the next **pre-1.0 minor**
   release with an explicit release note. Do not rewrite 0.6.0's MSRV or
   silently change its existing patch-line contract.

The decision has no effect on Spring Batch feature semantics, API layouts,
storage/recovery contracts, database certification, dependency versions, or
unrelated milestone work.

## Consequences

- Simpler toolchain policy: one latest Stable line, no manual numeric MSRV
  bookkeeping and no nightly-only compiler features.
- New Stable releases may introduce warnings or compatibility regressions.
  Required `-D warnings` CI must fail closed until the owning code or
  dependency is corrected; CI must never suppress valid diagnostics simply
  to preserve the floating channel.
- Users requiring an older compiler cannot rely on new unreleased source or
  future artifacts to retain the previously declared 1.95 minimum. This
  contract change must be visible in the next eligible release notes.
- Old releases and evidence retain their historical support claims.
- A local checkout needs `rustup update stable`; an offline system may run
  its last installed Stable, so CI is authoritative for current support.

## Alternatives considered

- Keep both Rust 1.95 MSRV and pinned 1.97.1 development toolchain: rejected
  because it does not follow new Stable automatically.
- Use `rust-version = "stable"`: invalid Cargo manifest value.
- Keep numeric `rust-version` synchronized with the latest Stable through
  scheduled bot pull requests: rejected as needless version churn for this
  project's selected no-numeric-MSRV support contract.
- Use Nightly for development and Stable for release: rejected because
  Nightly-only features would complicate public crate compatibility.

## Validation and rollout

1. Merge this superseding decision and canonical support-policy change as a
   separate documentation-only PR under #699, after required CI/review.
2. In a later one-work-unit implementation PR, remove the numeric manifest
   and Clippy declarations; set `stable` in toolchain; update CI and
   version-sensitive cache/test contracts; align active engineering docs.
3. Validate static policy/manifest invariants, existing required CI gate
   graph, all-feature stable compilation, cache parity, and relevant test lanes
   on the exact implementation PR HEAD.
4. Merge only after all required checks succeed; verify protected main,
   record the next-minor release-note requirement, and close #699 only after
   the implementation is complete.

Rollback requires another reviewed support decision and an intentional
compiler/toolchain compatibility contract; editing a historical released
artifact or silently restoring 1.95 support does not constitute rollback.

## Revisit triggers

- A demonstrated user or downstream library need for older-compiler support;
- a supported Rust platform/toolchain ABI that cannot follow latest Stable;
- a change in Rust/Cargo manifest support for nonnumeric compiler policy;
- an M14 1.0 support commitment requiring a different, explicit compiler
  compatibility window.
