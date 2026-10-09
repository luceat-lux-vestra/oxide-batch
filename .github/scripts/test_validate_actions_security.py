#!/usr/bin/env python3
"""Negative fixtures for the GitHub Actions security policy validator."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
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


LEGACY_CODEQL_ROUTING_FIXTURE = """
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
      ${{ !cancelled() &&
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
# The current activated validator requires independent dispatch caller evidence.
# Retain this historical fixture for separate M6 staged-validator contracts.
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
    if: ${{ github.event_name == 'pull_request' || github.event_name == 'workflow_dispatch' }}
    runs-on: ubuntu-latest
    permissions:
      actions: read
      contents: read
      pull-requests: read
    outputs:
      run_rust: ${{ steps.route.outputs.run_rust }}
    steps:
      - name: Resolve exact-base Rust CodeQL impact
        id: route
        run: |
          classifier_path = ".github/scripts/codeql-rust-impact.py"
          def trusted_dispatch_identity():
              if os.environ.get("GITHUB_SHA") != base_sha:
                  raise RuntimeError("wrong main")
              if os.environ.get("HEAD_REPO") != repository:
                  raise RuntimeError("wrong repo")
              if caller.get("path") != ".github/workflows/pr-ci.yml":
                  raise RuntimeError("caller path")
              if caller.get("run_attempt") != 1:
                  raise RuntimeError("first attempt required")
          trusted_dispatch_identity()
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
      ${{ !cancelled() &&
          (github.event_name != 'pull_request' || github.event.pull_request.draft == false) &&
          ((github.event_name != 'pull_request' && github.event_name != 'workflow_dispatch') ||
           needs.rust-impact.result != 'success' ||
           needs.rust-impact.outputs.run_rust != 'false') }}
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

# Stage M6 activated validator must reject the cancellation-resistant regression,
# without dropping the dependent-job failure/unknown fallback.
legacy_always = CODEQL_ROUTING_FIXTURE.replace("!cancelled()", "always()")
observed = codeql_routing_violations(legacy_always)
assert any("!cancelled()" in item for item in observed), observed
assert any("must not use always()" in item for item in observed), observed

for absent_guard in ("success()", "failure()"):
    observed = codeql_routing_violations(
        CODEQL_ROUTING_FIXTURE.replace("!cancelled()", absent_guard)
    )
    assert any("!cancelled()" in item for item in observed), observed

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
CANCELLABLE_FIXTURE = LEGACY_CODEQL_ROUTING_FIXTURE
assert not staged_routing_violations(CANCELLABLE_FIXTURE), "M6 cancellable routing fixture rejected"

for label, unsafe_fixture, expected in (
    ("old always", LEGACY_CODEQL_ROUTING_FIXTURE.replace("!cancelled()", "always()"), "!cancelled()"),
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


# M6 activated caller-readiness evidence: both protected production files must
# exactly match their pre-admitted staged Git blobs; no unapproved content drift.
import hashlib
import shutil

ROOT = Path(__file__).resolve().parents[2]
STAGED_WRITER = ROOT / "docs/engineering/ci-staging/orphan-codeql-caller-readiness-candidate.py"
STAGED_CODEQL = ROOT / "docs/engineering/ci-staging/codeql-caller-readiness-candidate.yml"
ACTIVE_WRITER = ROOT / ".github/scripts/orphan-codeql-cancel.py"
ACTIVE_CODEQL = ROOT / ".github/workflows/codeql.yml"
POLICY = ROOT / ".github/merge-gate-policy.json"

def m6_git_blob(path: Path) -> str:
    payload = path.read_bytes()
    return hashlib.sha1(b"blob " + str(len(payload)).encode("ascii") + b"\x00" + payload).hexdigest()

assert STAGED_WRITER.is_file() and STAGED_CODEQL.is_file(), "missing M6 pre-admitted candidates"
for path in (STAGED_WRITER, ACTIVE_WRITER):
    assert m6_git_blob(path) == "89fd82a519b43c5fad5d7300c9ab42e09392e9da", f"M6 active/staged writer drift: {path}"
assert ACTIVE_WRITER.read_bytes() == STAGED_WRITER.read_bytes(), "M6 writer differs from admitted source"
assert m6_git_blob(STAGED_CODEQL) == "2d6e80287d137c2d63da431ed9c085e24a920071", (
    "Historical M6 CodeQL stage changed without review"
)
assert m6_git_blob(ACTIVE_CODEQL) == "351638668c6e19dd171293f4f15d16409c5558e9", (
    "Active Rust CodeQL ARM64 must match its exact pre-admitted blob"
)
staged_wallclock = (
    ROOT / "docs/engineering/ci-staging/codeql-wallclock-rust-impact-candidate.yml"
).read_bytes()
rust_job_header = b"  analyze-rust:\n"
assert staged_wallclock.count(rust_job_header) == 1, "Ambiguous staged Rust job"
pre_rust, rust_job = staged_wallclock.split(rust_job_header, 1)
old_runner = b"    runs-on: ubuntu-latest\n"
assert rust_job.count(old_runner) == 1, "Staged Rust runner not unique"
expected_arm_codeql = (
    pre_rust + rust_job_header
    + rust_job.replace(old_runner, b"    runs-on: ubuntu-24.04-arm\n", 1)
)
assert ACTIVE_CODEQL.read_bytes() == expected_arm_codeql, (
    "Active ARM64 CodeQL differs from exact x64-stage plus runner-only substitution"
)

activated_codeql = ACTIVE_CODEQL.read_text(encoding="utf-8")
assert activated_codeql.count("89fd82a519b43c5fad5d7300c9ab42e09392e9da") == 2, "M6 CodeQL writer SHA must occur exactly twice"
assert "beb69de6b58d89b3814b23017e408775659b275f" not in activated_codeql, "M6 CodeQL retains stale writer SHA"
assert "!cancelled()" in activated_codeql, "M6 Rust Analyze cancellation guard regressed"
assert not codeql_routing_violations(activated_codeql), "active CodeQL must pass the activated trusted-dispatch security validator"

policy = json.loads(POLICY.read_text(encoding="utf-8"))
approved = next(
    item["accepted_blobs"]
    for item in policy["repository_merge_gate"]["protected_workflows"]
    if item["workflow"] == ".github/workflows/codeql.yml"
)
assert "2d6e80287d137c2d63da431ed9c085e24a920071" in approved, (
    "M6 active protected CodeQL blob not pre-admitted"
)
assert "025a05b98eb6dc2c5cfc6dbfddef905664a0d6c8" in approved, (
    "M6 prior protected CodeQL blob no longer accepted"
)
assert "351638668c6e19dd171293f4f15d16409c5558e9" in approved, (
    "ARM64 runner-only CodeQL blob is not pre-admitted"
)
assert "c150f2001a74e802180369fd16ee462b63e99232" in approved, (
    "x64 CodeQL rollback blob has been removed from trusted inventory"
)

# Future Rust CodeQL DB-upload optimization is staged but NOT yet active.
# Preserve all 37 security queries, scanning, SARIF upload, and server-side
# processing verification. Only the supplemental CodeQL database upload
# is toggled, with a one-line exact byte transition and pre-admitted blob.
STAGED_NO_DB_UPLOAD = (
    ROOT / "docs/engineering/ci-staging/codeql-rust-no-database-upload-candidate.yml"
)
assert STAGED_NO_DB_UPLOAD.is_file(), "missing CodeQL no-database-upload stage"
rust_category = b"          category: /language:rust\n"
active_codeql_bytes = ACTIVE_CODEQL.read_bytes()
assert active_codeql_bytes.count(rust_category) == 1, "ambiguous Rust CodeQL Analyze"
expected_no_db_upload = active_codeql_bytes.replace(
    rust_category,
    rust_category + b"          upload-database: false\n",
    1,
)
assert STAGED_NO_DB_UPLOAD.read_bytes() == expected_no_db_upload, (
    "CodeQL DB-upload stage must differ from active ARM64 solely in the Rust Analyze input"
)
assert m6_git_blob(STAGED_NO_DB_UPLOAD) == "894db7fb531721bbb961ce59231e08b9331b7444", (
    "CodeQL no-database-upload candidate Git blob drift"
)
assert "894db7fb531721bbb961ce59231e08b9331b7444" in approved, (
    "Future no-database-upload CodeQL blob must be pre-admitted before activation"
)
assert "          upload-database: false\n" not in activated_codeql, (
    "This PR may admit the candidate but must NOT activate the protected workflow"
)
assert not codeql_routing_violations(STAGED_NO_DB_UPLOAD.read_text(encoding="utf-8")), (
    "Future CodeQL DB-upload candidate must retain Rust routing security invariants"
)

# Exercise the now-active writer ONLY with mock HTTP and a trusted runtime copy.
# No actual token, Actions POST, or production cancellation is invoked.
with tempfile.TemporaryDirectory(prefix="oxide-m6-active-writer-") as td:
    root = Path(td)
    shutil.copyfile(ACTIVE_WRITER, root / "orphan-codeql-cancel.py")
    shutil.copyfile(ROOT / ".github/scripts/pr-authority-runtime.py",
                    root / "pr-authority-runtime.py")
    writer_contract = subprocess.run(
        [sys.executable, str(root / "orphan-codeql-cancel.py"), "self-test"],
        capture_output=True, text=True, timeout=30, check=False,
    )
    assert writer_contract.returncode == 0, (
        "M6 activated writer cancellation contract regression:\n"
        + writer_contract.stdout + writer_contract.stderr
    )

# CI wall-clock: preserve the historical x64 stage and validator evidence;
# active Rust CodeQL now differs ONLY by its exactly pre-admitted ARM64 runner.
WALLCLOCK_CODEQL = ROOT / "docs/engineering/ci-staging/codeql-wallclock-rust-impact-candidate.yml"
WALLCLOCK_VALIDATOR = ROOT / "docs/engineering/ci-staging/actions-security-wallclock-rust-impact-candidate.py"
assert WALLCLOCK_CODEQL.is_file() and WALLCLOCK_VALIDATOR.is_file()
assert m6_git_blob(ACTIVE_CODEQL) == "351638668c6e19dd171293f4f15d16409c5558e9"
assert m6_git_blob(ROOT / ".github/scripts/validate_actions_security.py") == (
    "29fe8e7a6778f433c6f0390f11aed10820764ee4"
)
assert m6_git_blob(WALLCLOCK_CODEQL) == "c150f2001a74e802180369fd16ee462b63e99232"
assert ACTIVE_CODEQL.read_bytes() == expected_arm_codeql
assert m6_git_blob(WALLCLOCK_VALIDATOR) == "29fe8e7a6778f433c6f0390f11aed10820764ee4"
assert (ROOT / ".github/scripts/validate_actions_security.py").read_bytes() == WALLCLOCK_VALIDATOR.read_bytes()

future_spec = importlib.util.spec_from_file_location("wallclock_validator", WALLCLOCK_VALIDATOR)
assert future_spec is not None and future_spec.loader is not None
future_module = importlib.util.module_from_spec(future_spec)
future_spec.loader.exec_module(future_module)
future_workflow = WALLCLOCK_CODEQL.read_text(encoding="utf-8")
route_errors = future_module.check_codeql_rust_routing_contract_text(future_workflow)
assert route_errors == [], ("future trusted-dispatch routing invalid", route_errors)

# Negative proofs must catch trust-source loss, actor/caller confusion,
# analysis suppression on uncertainty, and cancellation-resistant regressions.
for title, old, bad, diagnostic in (
    (
        "disable dispatch impact routing",
        "if: ${{ github.event_name == 'pull_request' || github.event_name == 'workflow_dispatch' }}",
        "if: ${{ github.event_name == 'pull_request' }}",
        "workflow_dispatch",
    ),
    (
        "caller Actions API permissions missing",
        "actions: read # Verify exact caller PR CI run and first-attempt provenance.",
        "actions: none # Verify exact caller PR CI run and first-attempt provenance.",
        "actions: read",
    ),
    (
        "wrong first-attempt caller",
        'caller.get("run_attempt") != 1',
        'caller.get("run_attempt") != 2',
        "run_attempt",
    ),
    (
        "missing exact trusted base source",
        'os.environ.get("GITHUB_SHA") != base_sha',
        'os.environ.get("GITHUB_SHA") == base_sha',
        "GITHUB_SHA",
    ),
    (
        "unsafe fail-open suppression",
        "run_rust=true",
        "run_rust=false",
        "must never suppress",
    ),
    (
        "ignore cancellation",
        "!cancelled()",
        "always()",
        "!cancelled()",
    ),
    (
        "suppress on uncertain classifier",
        "needs.rust-impact.result != 'success'",
        "needs.rust-impact.result == 'success'",
        "needs.rust-impact.result",
    ),
):
    assert old in future_workflow, ("fixture ineffective", title)
    bad_workflow = future_workflow.replace(old, bad)
    errors = future_module.check_codeql_rust_routing_contract_text(bad_workflow)
    assert any(diagnostic in error for error in errors), (title, errors)

candidate_policy = json.loads(POLICY.read_text(encoding="utf-8"))
for collection, key, path, admitted, prior in (
    ("protected_workflows", "workflow", ".github/workflows/codeql.yml",
     "c150f2001a74e802180369fd16ee462b63e99232", "2d6e80287d137c2d63da431ed9c085e24a920071"),
    ("protected_files", "path", ".github/scripts/validate_actions_security.py",
     "29fe8e7a6778f433c6f0390f11aed10820764ee4", "bbec03ce7e6f4696fc56c19c76f62e0cc7c3acc1"),
):
    entries = [
        row for row in candidate_policy["repository_merge_gate"][collection]
        if row[key] == path
    ]
    assert len(entries) == 1, ("protected artifact missing or duplicate", path)
    assert admitted in entries[0]["accepted_blobs"], ("future exact blob not pre-admitted", path)
    assert prior in entries[0]["accepted_blobs"], ("active prior blob de-admitted", path)

classifier_test = subprocess.run(
    [sys.executable, str(ROOT / ".github/scripts/codeql-rust-impact.py"), "--self-test"],
    capture_output=True, text=True, timeout=30, check=False,
)
assert classifier_test.returncode == 0, (
    "trusted-base classifier regression: " + classifier_test.stdout + classifier_test.stderr
)
print("CI wall-clock trusted dispatch routing candidate contract: PASS")
