#!/usr/bin/env python3
"""Negative fixtures for the GitHub Actions security policy validator."""

from __future__ import annotations

import importlib.util
import tempfile
import textwrap
from pathlib import Path


SCRIPT = Path(__file__).with_name("validate_actions_security.py")
SPEC = importlib.util.spec_from_file_location("validate_actions_security", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise SystemExit("could not load validate_actions_security.py")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def violations(workflow: str) -> list[str]:
    with tempfile.TemporaryDirectory(prefix="oxide-batch-actions-security-") as tmp:
        root = Path(tmp)
        workflows = root / ".github" / "workflows"
        workflows.mkdir(parents=True)
        (workflows / "fixture.yml").write_text(textwrap.dedent(workflow).lstrip())
        return MODULE.validate(root)


def require_rejection(name: str, workflow: str, needle: str) -> None:
    observed = violations(workflow)
    assert observed, f"{name}: broken fixture unexpectedly passed"
    assert any(needle in item for item in observed), (
        f"{name}: expected diagnostic containing {needle!r}; observed {observed!r}"
    )


def require_pass(name: str, workflow: str) -> None:
    observed = violations(workflow)
    assert not observed, f"{name}: valid fixture rejected: {observed!r}"


def codeql_routing_violations(workflow: str) -> list[str]:
    return MODULE.check_codeql_rust_routing_contract_text(textwrap.dedent(workflow).lstrip())


CODEQL_ROUTING_FIXTURE = """
name: CodeQL
on:
  pull_request:
    branches: [main]
  schedule:
    - cron: "47 18 * * 1"
  workflow_dispatch:
jobs:
  rust-impact:
    name: codeql-rust-impact
    if: ${{ github.event_name == 'pull_request' }}
    runs-on: ubuntu-latest
    permissions:
      contents: read
      pull-requests: read
    outputs:
      run_rust: ${{ steps.route.outputs.run_rust }}
    steps:
      - name: Resolve exact-base Rust CodeQL impact
        id: route
        run: |
          classifier_path = ".github/scripts/codeql-rust-impact.py"
          encoded_path = classifier_path
          base_sha = os.environ["BASE_SHA"]
          url = f"contents/{encoded_path}?ref={base_sha}"
          handle.write("run_rust=true\\n")
          handle.write("changed_files=unknown\\n")
          subprocess.run(["python3", classifier_path])
  analyze-rust:
    name: Analyze (rust)
    needs: rust-impact
    if: >-
      ${{ always() &&
          (github.event_name != 'pull_request' ||
           (github.event.pull_request.draft == false &&
            (needs.rust-impact.result != 'success' ||
             needs.rust-impact.outputs.run_rust != 'false'))) }}
    runs-on: ubuntu-latest
    steps:
      - uses: github/codeql-action/init@0000000000000000000000000000000000000001
        with:
          languages: rust
          build-mode: none
      - uses: github/codeql-action/analyze@0000000000000000000000000000000000000001
        with:
          category: /language:rust
"""


observed = codeql_routing_violations(CODEQL_ROUTING_FIXTURE)
assert not observed, f"valid CodeQL Rust route rejected: {observed!r}"

broken = CODEQL_ROUTING_FIXTURE.replace(
    'base_sha = os.environ["BASE_SHA"]',
    'base_sha = "pr-head"',
)
observed = codeql_routing_violations(broken)
assert any("base_sha" in item for item in observed), observed

broken = CODEQL_ROUTING_FIXTURE.replace("run_rust=true", "run_rust=false")
observed = codeql_routing_violations(broken)
assert any("must never suppress analysis" in item for item in observed), observed

broken = CODEQL_ROUTING_FIXTURE.replace(
    "needs.rust-impact.result != 'success'",
    "needs.rust-impact.result == 'success'",
)
observed = codeql_routing_violations(broken)
assert any("needs.rust-impact.result != 'success'" in item for item in observed), observed

broken = CODEQL_ROUTING_FIXTURE.replace(
    "    steps:\n      - name: Resolve exact-base Rust CodeQL impact",
    "    steps:\n      - uses: actions/checkout@0000000000000000000000000000000000000001\n"
    "      - name: Resolve exact-base Rust CodeQL impact",
)
observed = codeql_routing_violations(broken)
assert any("must not checkout PR-head" in item for item in observed), observed



# Stage M6: validate both future protected blobs before activation.
ROOT = Path(__file__).resolve().parents[2]
STAGED_VALIDATOR_PATH = ROOT / "docs/engineering/ci-staging/actions-security-cancel-aware-candidate.py"
STAGED_WORKFLOW_PATH = ROOT / "docs/engineering/ci-staging/codeql-cancel-aware-candidate.yml"
assert STAGED_VALIDATOR_PATH.is_file() and STAGED_WORKFLOW_PATH.is_file(), (
    "M6 protected candidate files must be present for pre-admission"
)
STAGED_SPEC = importlib.util.spec_from_file_location("actions_security_cancel_candidate", STAGED_VALIDATOR_PATH)
assert STAGED_SPEC is not None and STAGED_SPEC.loader is not None
STAGED_MODULE = importlib.util.module_from_spec(STAGED_SPEC)
STAGED_SPEC.loader.exec_module(STAGED_MODULE)

def staged_routing_violations(workflow: str) -> list[str]:
    return STAGED_MODULE.check_codeql_rust_routing_contract_text(textwrap.dedent(workflow).lstrip())

STAGED_WORKFLOW = STAGED_WORKFLOW_PATH.read_text(encoding="utf-8")
assert not staged_routing_violations(STAGED_WORKFLOW), "M6 future CodeQL workflow fails future security contract"
CANCELLABLE_FIXTURE = CODEQL_ROUTING_FIXTURE.replace("always()", "!cancelled()")
assert not staged_routing_violations(CANCELLABLE_FIXTURE), "M6 cancellable routing fixture rejected"

for label, unsafe_fixture, expected in (
    ("old always", CODEQL_ROUTING_FIXTURE, "!cancelled()"),
    ("always mixed with guard", CANCELLABLE_FIXTURE.replace(
        "!cancelled() &&", "always() || !cancelled() &&"), "must not use always()"),
    ("missing cancellation guard", CANCELLABLE_FIXTURE.replace(
        "!cancelled()", "success()"), "!cancelled()"),
    ("missing failed dependency fallback", CANCELLABLE_FIXTURE.replace(
        "needs.rust-impact.result != 'success'", "needs.rust-impact.result == 'success'"),
        "needs.rust-impact.result != 'success'"),
    ("draft PR accidentally scanned", CANCELLABLE_FIXTURE.replace(
        "github.event.pull_request.draft == false", "github.event.pull_request.draft == true"),
        "github.event.pull_request.draft == false"),
):
    errors = staged_routing_violations(unsafe_fixture)
    assert any(expected in violation for violation in errors), (
        f"M6 {label}: expected {expected!r} rejection, got {errors!r}"
    )

PINNED_CHECKOUT = "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1"
PG15 = "926f8799aef36e00001cfe15fba7abbd37d3c5224ea57e4c858e4bb670f10561"
PG18 = "4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280"
BAD_DIGEST = "0" * 64


require_pass(
    "baseline",
    f"""
    name: valid
    on: pull_request
    permissions:
      contents: read
    jobs:
      test:
        runs-on: ubuntu-latest
        timeout-minutes: 5
        steps:
          - uses: {PINNED_CHECKOUT}
            with:
              persist-credentials: false
          - uses: actions/dependency-review-action@a1d282b36b6f3519aa1f3fc636f609c47dddb294
    """,
)

require_rejection(
    "mutable action",
    """
    name: mutable
    on: pull_request
    permissions:
      contents: read
    jobs:
      test:
        runs-on: ubuntu-latest
        steps:
          - uses: actions/checkout@v7
            with:
              persist-credentials: false
    """,
    "full 40-hex commit SHA",
)

require_rejection(
    "checkout credential persistence",
    f"""
    name: credentials
    on: pull_request
    permissions:
      contents: read
    jobs:
      test:
        runs-on: ubuntu-latest
        steps:
          - uses: {PINNED_CHECKOUT}
    """,
    "persist-credentials: false",
)

require_rejection(
    "workflow top-level write",
    """
    name: broad
    on: issues
    permissions:
      contents: read
      issues: write
    jobs:
      classify:
        runs-on: ubuntu-latest
        steps:
          - run: echo safe
    """,
    "workflow-top-level write permission",
)

require_rejection(
    "untrusted PR title shell interpolation",
    """
    name: injection
    on: pull_request
    permissions:
      contents: read
    jobs:
      test:
        runs-on: ubuntu-latest
        steps:
          - run: |
              echo "${{ github.event.pull_request.title }}"
    """,
    "untrusted GitHub/input context",
)

require_rejection(
    "untrusted workflow input program interpolation",
    """
    name: injection
    on:
      workflow_dispatch:
        inputs:
          value:
            required: true
            type: string
    permissions:
      contents: read
    jobs:
      test:
        runs-on: ubuntu-latest
        steps:
          - uses: actions/github-script@3a2844b7e9c422d3c10d287c895573f7108da1b3
            with:
              script: |
                console.log("${{ inputs.value }}")
    """,
    "untrusted GitHub/input context",
)

require_rejection(
    "pull_request_target checkout",
    f"""
    name: target
    on: pull_request_target
    permissions:
      contents: read
    jobs:
      test:
        permissions:
          contents: read
          pull-requests: write
        runs-on: ubuntu-latest
        steps:
          - uses: {PINNED_CHECKOUT}
            with:
              persist-credentials: false
    """,
    "pull_request_target workflow must not checkout",
)

require_rejection(
    "mutable service image",
    """
    name: image
    on: pull_request
    permissions:
      contents: read
    jobs:
      test:
        runs-on: ubuntu-latest
        services:
          postgres:
            image: postgres:18
        steps:
          - run: echo safe
    """,
    "immutable sha256 digest",
)

require_pass(
    "digest-pinned service image",
    f"""
    name: image
    on: pull_request
    permissions:
      contents: read
    jobs:
      test:
        runs-on: ubuntu-latest
        services:
          postgres:
            image: postgres:18@sha256:{PG18}
        steps:
          - run: echo safe
    """,
)

require_pass(
    "checked-in postgres matrix digest mapping",
    f"""
    name: image-matrix
    on: pull_request
    permissions:
      contents: read
    jobs:
      test:
        runs-on: ubuntu-latest
        strategy:
          matrix:
            postgres: ["15", "18"]
        services:
          postgres:
            image: postgres:${{{{ matrix.postgres }}}}@sha256:${{{{ matrix.postgres == '15' && '{PG15}' || matrix.postgres == '18' && '{PG18}' || 'unsupported' }}}} # zizmor: ignore[unpinned-images]
        steps:
          - run: echo safe
    """,
)

require_rejection(
    "postgres matrix digest drift",
    f"""
    name: image-matrix
    on: pull_request
    permissions:
      contents: read
    jobs:
      test:
        runs-on: ubuntu-latest
        strategy:
          matrix:
            postgres: ["15", "18"]
        services:
          postgres:
            image: postgres:${{{{ matrix.postgres }}}}@sha256:${{{{ matrix.postgres == '15' && '{BAD_DIGEST}' || matrix.postgres == '18' && '{PG18}' || 'unsupported' }}}}
        steps:
          - run: echo safe
    """,
    "exact checked-in PostgreSQL 15/18 digest mapping",
)

require_rejection(
    "postgres matrix digest mapping without bounded matrix",
    f"""
    name: image-matrix
    on: pull_request
    permissions:
      contents: read
    jobs:
      test:
        runs-on: ubuntu-latest
        strategy:
          matrix:
            postgres: ["15", "16", "18"]
        services:
          postgres:
            image: postgres:${{{{ matrix.postgres }}}}@sha256:${{{{ matrix.postgres == '15' && '{PG15}' || matrix.postgres == '18' && '{PG18}' || 'unsupported' }}}}
        steps:
          - run: echo safe
    """,
    "exact checked-in PostgreSQL 15/18 digest mapping",
)

ISSUE_LABELER_CONTRACT = textwrap.dedent(
    """
    name: Issue labels
    on:
      workflow_dispatch:
        inputs:
          dry_run:
            default: true
          backfill:
            default: false
    permissions: {}
    jobs:
      classify:
        permissions:
          issues: write # trusted metadata mutation only
        steps:
          - uses: actions/github-script@3a2844b7e9c422d3c10d287c895573f7108da1b3
            env:
              DRY_RUN: true
              BACKFILL: true
            with:
              script: |
                const defaultBranchRef = `refs/heads/${context.payload.repository.default_branch}`;
                if (context.eventName === 'workflow_dispatch' && backfill && !dryRun && context.ref !== defaultBranchRef) {
                  console.log("Mutating backfill must run from");
                }
                if (!dryRun) {
                  github.rest.issues.updateLabel
                  github.rest.issues.createLabel
                }
                if (name !== explicitType && !dryRun) {
                  github.rest.issues.removeLabel
                }
                if (!dryRun && uniqueAdd.length) {
                  github.rest.issues.addLabels
                }
    """
).lstrip()

assert not MODULE.check_issue_labeler_contract_text(ISSUE_LABELER_CONTRACT), (
    "safe issue labeler contract fixture must pass"
)

missing_dry_run = ISSUE_LABELER_CONTRACT.replace("      dry_run:\n        default: true\n", "")
observed = MODULE.check_issue_labeler_contract_text(missing_dry_run)
assert any("dry_run" in item for item in observed), observed

unsafe_backfill_default = ISSUE_LABELER_CONTRACT.replace(
    "      backfill:\n        default: false\n",
    "      backfill:\n        default: true\n",
)
observed = MODULE.check_issue_labeler_contract_text(unsafe_backfill_default)
assert any("backfill must default to false" in item for item in observed), observed

broad_write = ISSUE_LABELER_CONTRACT.replace(
    "permissions: {}",
    "permissions:\n  issues: write",
)
observed = MODULE.check_issue_labeler_contract_text(broad_write)
assert any("workflow-level permissions" in item or "write authority" in item for item in observed), observed

extra_mutation = ISSUE_LABELER_CONTRACT.replace(
    "github.rest.issues.updateLabel",
    "github.rest.issues.updateLabel\n              github.rest.issues.updateLabel",
)
observed = MODULE.check_issue_labeler_contract_text(extra_mutation)
assert any("mutation surface drifted" in item for item in observed), observed

unguarded_remove = ISSUE_LABELER_CONTRACT.replace(
    "name !== explicitType && !dryRun",
    "name !== explicitType",
)
observed = MODULE.check_issue_labeler_contract_text(unguarded_remove)
assert any("safety contract missing" in item for item in observed), observed

missing_default_branch_guard = ISSUE_LABELER_CONTRACT.replace(
    "context.eventName === 'workflow_dispatch' && backfill && !dryRun && context.ref !== defaultBranchRef",
    "context.eventName === 'workflow_dispatch' && !dryRun",
)
observed = MODULE.check_issue_labeler_contract_text(missing_default_branch_guard)
assert any("issue reconciliation safety contract missing" in item for item in observed), observed

missing_default_branch_ref = ISSUE_LABELER_CONTRACT.replace(
    "const defaultBranchRef = `refs/heads/${context.payload.repository.default_branch}`;",
    "const defaultBranchRef = context.ref;",
)
observed = MODULE.check_issue_labeler_contract_text(missing_default_branch_ref)
assert any("issue reconciliation safety contract missing" in item for item in observed), observed

print("GitHub Actions security policy negative fixtures: PASS")
