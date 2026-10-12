# Protected workflow: verified GitHub Actions patch pins

Tracking: [#545](https://github.com/luceat-lux-vestra/oxide-batch/issues/545). The trusted merge authority was activated by [#547](https://github.com/luceat-lux-vestra/oxide-batch/pull/547), following exact-blob pre-admission [#546](https://github.com/luceat-lux-vestra/oxide-batch/pull/546).

## Routine patch update

For a `uses:` GitHub Action already pinned to a full 40-character immutable Git commit SHA, one PR may update it **without a separate pre-admission PR** only when all of the following hold:

1. The protected workflow and the precise existing `uses:` line number are enumerated in the trusted-base `.github/scripts/action_pin_only.py` catalog. The catalog currently covers 46 audited sites in `pr-ci.yml`, `ci.yml`, `dependency-review.yml`, `codeql.yml`, `evidence.yml`, `supply-chain.yml`, and `m5-conformance.yml`.
2. The **only edits in that workflow** are audited `uses: repository/action@<40-character-sha> # vMAJOR.MINOR.PATCH` lines. The repository and action identity, indentation, all other workflow bytes, event triggers, permissions, scripts, environments, input parameters and artifact handling remain unchanged.
3. Each changed action repository is advanced consistently at every audited site used in the affected workflow. The version comment moves **forward within the same major and minor release line**, and the commit SHA changes.
4. The trusted verifier resolves the official upstream GitHub Release, its `vMAJOR.MINOR.PATCH` tag and that tag's actual commit. All three must match the proposed SHA. Network/API failures, missing releases and mismatched tags **fail closed**.
5. The standard exact-HEAD `pr-proof`, `merge-gate`, Rust checks, dependency review, CodeQL, M5 provenance and required conformance checks remain mandatory. Auxiliary dependency closures, tests and documentation may need updating in the **same PR**.

The exception is deliberately narrow. The elevated `pr-labeler.yml` merge authority is **never** eligible. Unlisted actions/sites/workflows, major or minor upgrades, non-SHA refs, repository substitutions, structural YAML edits, extra steps, permissions changes, changes to protected scripts and policy changes still follow the existing reviewed exact-blob pre-admission process.

## Trust boundaries and failure behavior

- The `pull_request_target` authority fetches the **exact protected base** code, checks the helper's Git blob identity and compares the old and new workflow bytes. It must not execute a PR-head helper or adopt a PR-head trust policy.
- The independent Ruby verifier also executes only the exact same pinned helper in its separately checked-out protected base. Both authorities enforce the same site inventory and upstream tag-to-SHA validation.
- The helper is pinned by blob `2ed10e141c7c11b1a88940256e3a68ca272467a4` in **both** trusted gate implementations. Updating the helper requires a new reviewed, staged trust-root migration; merely editing the unprotected helper path cannot authorize a change.
- After an approved patch-only PR merges, the resulting immutable workflow bytes form the next trusted **base**. Thus an unchanged, protected-base workflow does not need to appear in historical `accepted_blobs` entries. This baseline exception applies only to the audited workflow set while the base helper remains pinned.
- Official GitHub Release and tag provenance authenticates the upstream repository association, **not a cryptographic release signature**; for example, the CodeQL annotated `v4.38.3` tag was unsigned. Only specifically audited official repositories are included in the trust catalog.

## Tests and verification

The Python negative and replay suite runs in Fast branch CI:

```sh
python3 -m unittest discover -s .github/scripts -p 'test_action_pin_only.py' -v
```

Fast also syntax-checks the protected Ruby verifier and embedded protected Python Gate. The suite rejects action origin substitutions, edits outside audited `uses:` lines, partial updates within one action repository, short SHA references, version-only edits, downgrades, major/minor changes and forged release-to-commit associations. It replays a hypothetical forward patch against every currently audited real workflow.

The historic CodeQL `v4.38.2 → v4.38.3` and artifact upload/download `v7.0.1 → v7.0.2`/`v8.0.1 → v8.0.2` upgrades in [#540](https://github.com/luceat-lux-vestra/oxide-batch/pull/540) provide known upstream examples. They predate the new Gate and **do not count as an actual post-rollout single-PR acceptance test**. Do not deliberately downgrade `main` merely to manufacture such a test. Record that final acceptance evidence under #545 when the next supported upstream patch is published.

## Operator response to failure

When a patch-only update fails, do not waive `merge-gate` or modify the policy from the same PR. First confirm the exact base/head, the changed workflow paths and line numbers, the official upstream release SHA and the independent verifier's diagnostics. If the update is not in the audited scope, use ordinary explicit reviewed pre-admission. If source locations were structurally changed by an already approved redesign, update the trusted catalog through the same staged trust-root process.
