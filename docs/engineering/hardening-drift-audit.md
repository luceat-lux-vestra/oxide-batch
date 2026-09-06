# Repository hardening drift audit

Issue #233 turns the second-pass repository hardening controls into a recurring
drift detector. It does not create another merge authority and it does not
auto-remediate GitHub settings.

## Architecture

`.github/workflows/hardening-drift-audit.yml` runs weekly and after policy-relevant
changes land on `main`. The `detect` job has read-only repository contents
permission and emits one bounded machine-readable job output with exactly one
classification:

- `clean`
- `policy-drift`
- `infrastructure-failure`

`policy-drift` means an authoritative checked-in policy or live readback disagrees
with the accepted state. `infrastructure-failure` means the audit cannot produce
an authoritative verdict because API/tooling/readback failed. The two are never
collapsed.

The `report` job is a separate trust boundary. Only that job receives
`issues: write`, consumes the trusted detector job output, and owns exactly one
tracker issue identified by:

`<!-- oxide-batch:hardening-drift-audit -->`

Repeated non-clean runs update or reopen that issue. A later clean run records
recovery and closes it. Duplicate owned markers fail closed.

The detector/result handoff intentionally does **not** use `upload-artifact`.
The repository-wide retained-evidence policy treats every workflow using
`actions/upload-artifact` as a declared campaign-evidence producer. Audit-control
transport is not campaign evidence, so using an artifact here would create a
second meaning for that canonical inventory. The job output is bounded before
handoff so it remains below GitHub's output limit even when leaf diagnostics are
large.

## Canonical controls composed

The detector invokes existing sources of truth rather than copying their rules:

- `.github/merge-gate-policy.json` +
  `.github/scripts/verify-merge-gates.rb` for required producer/ruleset drift;
- `cargo xtask release-crates` and the existing release negative-contract test
  for publishable-crate and release binding invariants;
- `.github/scripts/validate_actions_security.py` for immutable action refs,
  least privilege, checkout credentials, and untrusted-context boundaries;
- `.github/scripts/validate_supply_chain_exceptions.py` for the owned,
  expiring supply-chain exception registry;
- the retained-evidence policy binary and `cargo xtask evidence` for retained
  evidence policy/provenance;
- `SECURITY.md` and `CODE_OF_CONDUCT.md` for the distinct `[SECURITY]` and
  `[CONDUCT]` reporting contracts.

`.github/repository-settings-policy.json` is the machine-readable record of the
live settings reconciled in #231. Controls readable with the scheduled
low-privilege token are checked automatically. Administration-only controls are
explicitly `manual-readback`; they remain canonical but are not falsely reported
as continuously monitored.

## Manual-readback boundary

The scheduled job deliberately carries no privileged PAT or administration
token. Administration-only controls include Dependabot/security-analysis
toggles, Actions allowlist/default-token/fork approval settings, merge-history
repository fields that may be omitted from low-privilege payloads, and CodeQL
default-setup configuration.

The #231 admin-scoped readback is the current evidence baseline for those
controls. Future hardening reviews must repeat that readback when the policy or
platform behavior changes.

## CodeQL Rust review

GitHub-managed default setup remains the single CodeQL authority with
`actions` and `rust` enabled. `Analyze (actions)` remains required.
`Analyze (rust)` remains advisory in this change.

That is deliberate: #248 established capability and one exact PR/main observation
pair, but that is not repeated independent reliability evidence sufficient to
promote a managed external producer into merge authority. The recurring
low-privilege audit also cannot authoritatively read the Administration-only
default-setup configuration. Future promotion therefore requires a separate
explicit ruleset/policy migration with fresh producer-presence and reliability
evidence.

## Verification

`.github/workflows/hardening-drift-audit-policy.yml` runs the audit-level
orchestration tests on every PR and `main` push. Those tests deliberately stub
leaf command outcomes: they prove that a rejection from each composed canonical
checker reaches the final `policy-drift` classification without copying the
leaf policy into the audit layer. Tooling failure reaches
`infrastructure-failure`; a live repository-setting mismatch is detected;
manual-readback inventory is explicit; and the owned issue lifecycle creates,
updates, reopens, recovers/closes, and rejects duplicate ownership.

The leaf rejection proofs remain owned by their canonical controls and run on the
same PR HEAD: merge-gate tests mutate required-context/ruleset fixtures; the
Actions-security and supply-chain validators have their own safe negative
fixtures; `release_negative_contract` drives the real release verifier against
mutated detached worktrees; and
`retained_evidence_policy_negative_contract` likewise mutates the canonical
retained-evidence policy in a detached worktree and requires the real verifier to
reject it. The audit acceptance proof is therefore the composition of real leaf
rejection plus audit-level failure propagation, not a second implementation of
those policies.

The workflow contract tests additionally prove that detection has no write
permission, reporting owns the only `issues: write` grant, and audit handoff does
not accidentally register itself as retained campaign evidence through
`upload-artifact`.
