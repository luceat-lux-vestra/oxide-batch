# Documentation Strategy

**State:** Accepted

Documentation is a versioned product artifact and part of the release gate.

## Information architecture

| Kind | User question | Examples |
| --- | --- | --- |
| Tutorial | Can you help me learn by doing? | First job, fail and restart |
| How-to | How do I accomplish a task? | Configure retry, recover stale execution |
| Reference | What exactly is supported? | API, statuses, config, metrics, CLI |
| Explanation | Why does it work this way? | Transaction boundary, instance identity |
| Operations | How do I run it safely? | Upgrade, backup, restore, incident diagnosis |
| Contributor | How do I change it? | Architecture, tests, RFCs, releases |

## Required sets by milestone

- M0: charter, compatibility, architecture, standards, security, roadmap.
- M1: crate-level docs, API examples, domain glossary, first in-memory job.
- M2: PostgreSQL setup, migrations, transaction guarantees, crash/restart guide.
- M3: fault-tolerance and flow recipes plus compatibility matrix.
- M4: CLI reference, telemetry catalog, runbooks, capacity guidance.
- M5: complete tutorial/how-to/reference/explanation set, upgrade guide, support
  matrix, limitations, and 1.0 migration material.

## Source and versioning

- Rust API reference lives in rustdoc beside the public API.
- Design, policies, and guides live under `docs/`.
- Release notes live in `CHANGELOG.md` and GitHub Releases.
- Examples live in buildable workspace targets or tested documentation.
- Version-specific docs identify the OxideBatch release they describe.
- Superseded decisions remain available and link to replacements.

## Quality rules

- Every public API has sufficient rustdoc to use it safely.
- Examples compile under documented features and MSRV where practical.
- Links and code snippets are checked automatically.
- Terms match the glossary and compatibility vocabulary.
- Restart, transaction, data-loss, security, and destructive-operation warnings
  are explicit.
- Screenshots are avoided for information that changes frequently or needs
  accessibility/searchability.
- No documentation contains real credentials, production data, or private
  incident details.

## Review

Behavior changes update reference and examples in the same pull request.
Documentation review asks:

- Is the intended audience and prerequisite clear?
- Can the outcome be verified?
- Are defaults, limits, failure modes, and cleanup documented?
- Does the text promise more compatibility or durability than tests prove?
- Is the material discoverable from the index?
