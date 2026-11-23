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
- Review conversations must be resolved.
- The required GitHub Actions `Direct Merge Gate` must pass.
- Exact-final-HEAD review/evidence remains mandatory under repository policy;
  a green gate is necessary but never sufficient.
- The live main ruleset currently requires **0** approving reviews and does not
  require latest-push approval. Do not fabricate a human approval requirement
  that the repository has not adopted.
- If reviews are present, stale reviews are dismissed after new reviewable
  commits under the live ruleset configuration.
- Only squash merge is enabled.
- The live main ruleset has **no bypass actors**. Do not assume owner/admin
  bypass authority; read live enforcement before a consequential merge action.

The checked-in `.github/repository-settings-policy.json` records these
repository controls and the live ruleset remains execution authority. If the
checked-in policy and GitHub diverge, fail closed and reconcile the drift rather
than choosing the more convenient state.

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

### Workflow lifecycle

Research, experiment, migration, and other temporary workflows should stay off
the protected default branch where practical. Run bounded research automation
from a non-default branch and remove the workflow source after the evidence is
recorded.

If an exceptional ephemeral workflow must land in `.github/workflows` on the
default branch, filenames beginning with `research-`, `temporary-`, `tmp-`,
or `experiment-` must declare non-empty `# lifecycle-owner:` and
`# lifecycle-retire:` comments in the first 20 lines. The owner identifies the
issue or campaign responsible for the workflow; the retirement field states the
concrete condition for deleting or consolidating it.

Historical Actions runs/artifacts are not automatically evidence. Preserve them when a concrete issue, PR, R2A campaign, security/release decision, benchmark, or migration record consumes them as immutable proof. Orphaned temporary/research/one-off history with no such consumer may be deleted after exact workflow-ID/path/run-count validation; never delete evidence-bearing history solely for sidebar cosmetics. Old run history is not a reason to keep or recreate obsolete workflow source.

## Review cadence

Security and data-integrity issues take priority. Issues are not automatically
closed due to inactivity. Labels, milestones, and ownership are reviewed during
milestone planning.
