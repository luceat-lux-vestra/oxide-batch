# Severity and Response Objectives

**State:** Accepted

These are maintainer response objectives, not a commercial SLA. A single
maintainer may not always meet them; missed objectives are reviewed rather than
hidden.

| Severity | Criteria | Initial response objective | Release impact |
| --- | --- | --- | --- |
| P0 Critical | Active vulnerability, credential/release compromise, proven data corruption/loss, unsafe recovery | As soon as practical, target 24 hours | Stop release; contain immediately |
| P1 High | Likely duplicate/lost processing, restart/checkpoint defect, exploitable high-impact security issue | Target 3 business days | Blocks affected milestone/release |
| P2 Medium | Significant incorrect behavior with workaround, moderate security or operational degradation | Target 7 business days | Triage into current/next milestone |
| P3 Low | Minor defect, diagnostics/docs issue, low-risk improvement | Best effort | Does not normally block release |

## Classification rules

- Severity describes impact and exploitability, not reporter urgency.
- Unknown integrity impact is treated as at least P1 until evidence reduces it.
- Security issues remain private under `SECURITY.md`.
- A compatibility difference is not automatically a bug unless the matrix
  promises the behavior.
- Performance is P1 only when it causes practical unavailability, resource
  exhaustion, or violates a documented budget.

## Response record

For P0/P1 record:

- affected versions/components;
- user-visible and data/restart impact;
- containment and safe workaround;
- reproduction/evidence and owner;
- disclosure, release, migration, or recovery plan;
- tests that prevent recurrence.

## Dependency advisories

- Critical/high and reachable: P0/P1 according to impact.
- Moderate: P2 until reachability/mitigation is documented.
- Unreachable or configuration-excluded: time-bounded exception with evidence.
- Unmaintained/yanked without vulnerability: dependency-health issue, normally
  P2/P3 depending on replacement and exposure.
