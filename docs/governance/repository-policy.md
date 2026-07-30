# Repository Policy

## Naming

- Product and prose name: `OxideBatch`
- GitHub repository and local directory: `oxide-batch`
- Cargo package prefix: `oxide-batch`
- Rust crate import prefix: `oxide_batch`

## Branching model

OxideBatch uses trunk-based GitHub Flow.

- `main` is the only permanent development branch.
- Work happens on short-lived branches created from `main`.
- There is no `develop` branch.
- Release branches are introduced only when supported backports require them.

Allowed branch prefixes are `feat/`, `fix/`, `docs/`, `refactor/`, `test/`,
`chore/`, `spike/`, and `release/`.

## Pull requests

- Every post-bootstrap change to `main` must use a pull request.
- One approving review is required.
- Review conversations must be resolved.
- Required status checks must pass.
- Stale approvals are dismissed after new reviewable commits.
- The latest reviewable push must be approved.
- Only squash merge is enabled.

While the project has one maintainer, the repository owner has pull-request-only
bypass permission. This preserves a pull request and audit trail but permits the
owner to merge without a second person. Direct pushes remain outside the normal
workflow. The bypass will be removed when another active maintainer is added.

The initial repository commit is the sole bootstrap exception because the
ruleset cannot exist until the repository and its default branch exist.

## Commit history

Pull request titles follow Conventional Commit syntax and become squash commit
subjects. Merge commits and rebase merges are disabled.

## Protected tags

Tags matching `v*` are immutable. Release tags must point to a reviewed commit
on a protected branch. Signed release tags become mandatory before the first
public runtime release.

## Repository features

- Issues: enabled
- Discussions: enabled
- Projects: enabled
- Wiki: disabled; documentation is version-controlled
- Branch deletion after merge: automatic
- Auto-merge: enabled

## Automation

- Pull requests receive bounded changed-file area labels. Priority, lifecycle
  status, and breaking-change disposition remain maintainer decisions.
- CODEOWNERS remains the reviewer mechanism; automation does not fabricate an
  approving reviewer for the single-maintainer repository.
- AI review is optional advisory output for trusted contributors, is not a
  required check, and has no approval or merge authority.
- Scheduled supply-chain failures create or update one owned issue.
- Protected version tags prepare draft Releases. A maintainer publishes the
  reviewed draft; no tag workflow publishes a release automatically.

## Review cadence

Security and data-integrity issues take priority. Issues are not automatically
closed due to inactivity. Labels, milestones, and ownership are reviewed during
milestone planning.
