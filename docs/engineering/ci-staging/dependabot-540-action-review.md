# Dependabot #540: exact action-patch admission

This prerequisite admits two future protected workflow blobs for
[PR #540](https://github.com/luceat-lux-vestra/oxide-batch/pull/540).
It does not activate either workflow. Existing accepted blobs and the complete
protected workflow/control inventory remain in place. Activation must follow
the prerequisite's manual squash merge into protected `main`; a candidate
cannot authorize its own changed protected bytes.

| Protected path | Current blob | Future blob | Retained candidate |
| --- | --- | --- | --- |
| `.github/workflows/codeql.yml` | `fef6dc22f11826543b3099c8e80e0adad928adf1` | `68878572050524d1f65e0aa7c14baca49074a953` | [CodeQL](dependabot-540-codeql-candidate.yml) |
| `.github/workflows/m5-conformance.yml` | `5e8c3cba0ae1c9f3d7c454c5a6004e71ad58c0ae` | `0d590fdd1130f462d31f353ed4394822b3c424fa` | [Conformance](dependabot-540-conformance-candidate.yml) |

The candidate CodeQL bytes change exactly four `init`/`analyze` action pins
from v4.38.2 (`2892aa5e19bbd11bc0cff5427e3b750a04d9e3c2`) to v4.38.3
(`24c54180a607b1449ed407dd24f251e4e9147c8d`). The
[official release](https://github.com/github/codeql-action/releases/tag/v4.38.3)
updates the default CLI to 2.27.2 and fixes custom query bundle handling.
The official annotated tag resolves to the pinned commit. No query suite,
analysis language, runner, permissions, caller validation, SARIF/server-side
processing requirement or database-upload setting changes.

The conformance candidate changes only upload-artifact v7.0.1 to
[v7.0.2](https://github.com/actions/upload-artifact/releases/tag/v7.0.2)
(`cf430e030ddbb5b0abf93d22962f4752f3646cd9`) and download-artifact v8.0.1 to
[v8.0.2](https://github.com/actions/download-artifact/releases/tag/v8.0.2)
(`9000827ccba6bdab643e8b6fd33ac0654aef8333`). Both official tags resolve to
these immutable commits. Both updates use `@actions/artifact` 6.3.1 and improve
HTTP 429 download retry handling, including valid Retry-After headers.
They retain Node 24, MIT licensing and the existing artifact input contract.
The caller's matrix, sharding, artifact names/paths, retention, merging,
execution deadlines and missing-artifact failure policy are unchanged.

## Activation obligations

PR #540 also updates artifact pins in the other ten campaign workflows.
Each of the eleven `execution-contract.json` files must bind its newly reviewed
workflow Git blob. Verification scripts, scenario inventories and retained
reports must remain unchanged; historical reports do not become current evidence
by editing their manifests.

The Actions security regression suite must bind the exact new active CodeQL
blob while preserving immutable earlier staging fixtures. Its comparison to
the historical caller-recheck stage must independently prove that reversing
exactly the four reviewed pins yields the old active blob. Negative cases must
reject runner, database-upload, caller-retry and incomplete-pin changes.
The actual active caller's mocked adversarial API tests remain required.

Before activation is merge-ready, verify the two candidate blobs, all eleven
execution contracts, deterministic Actions security, actionlint/zizmor,
protected-base admission and the full required exact-HEAD gate. A protected
workflow change remains blocked until the trusted base already contains its
admission. Historical rollback blobs remain admitted; any later rollback still
requires a reviewed PR and the matching execution contracts.

These tool patches make no Rust/public API, Spring capability, durable schema,
restart/transaction, delivery, telemetry or release-readiness change.
