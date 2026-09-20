## Summary

<!-- What changes and why? -->

## Related issue / decision

<!-- Link the owning issue and any governing ADR/RFC. -->

<!-- failure-triage:v1:start -->
## Failure remediation

Select exactly one. This declaration is required for human-authored PRs.

- [ ] Not remediation for an observed failure
- [ ] Remediation for an observed failure

If this PR is remediation, replace every placeholder below. If root cause is still
UNKNOWN / UNVERIFIED / INSUFFICIENT EVIDENCE, stop remediation and classify/investigate first.

Observed:
<!-- What failed, where, and on which exact revision/run? -->

Classification:
<!-- Exactly one: implementation defect | test defect | evidence defect | workflow-policy drift | environment failure -->

Basis:
<!-- Why is this responsibility layer proven? Which plausible alternatives were rejected or remain unresolved? -->

Root cause:
<!-- Established cause. UNKNOWN / UNVERIFIED / INSUFFICIENT EVIDENCE / TBD are not mergeable remediation states. -->

Remediation:
<!-- Which owning layer changes, and why is this the minimum justified change? -->

Proof:
<!-- What will prove the cause is resolved without weakening tests/evidence/policy? -->
<!-- failure-triage:v1:end -->

## Validation

<!-- Commands, tests, evidence, and exact revisions actually verified. -->

## Merge gate

- [ ] Exact final PR HEAD reviewed.
- [ ] Required CI is green on that exact HEAD.
- [ ] Any HEAD movement invalidates prior exact-HEAD evidence.
- [ ] No test, evidence obligation, conformance requirement, or policy gate was weakened merely to obtain green.
